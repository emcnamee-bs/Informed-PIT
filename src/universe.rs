//! Daily market universe: the (at most 300) markets the watcher polls every minute.
//!
//! [`select`] is a pure filter-and-rank over markets whose category is already known. [`refresh`]
//! does the I/O around it: list open markets closing in the window, apply the cheap filters first,
//! look up categories (one HTTP call per uncached series) only for the survivors, run [`select`], and record
//! the result in `universe_runs`, `markets` (`active` flag) and one `universe` event.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::db;
use crate::eventlog::EventLog;
use crate::http::Http;
use crate::kalshi::{Client, Market};

/// A market with its event's category (from `/events`) and whether it traded in the last 7 days.
#[derive(Debug, Clone)]
pub struct MarketWithCategory {
    pub market: Market,
    /// `None` when unknown (the markets endpoint has none; the lookup found none, or was skipped).
    pub category: Option<String>,
    /// Traded in the last 7 days: `Some(true)` (`volume_24h > 0`, or a trade found), `Some(false)`
    /// (checked, none found), `None` (not checked: the cap was already full of confirmed markets,
    /// so an unchecked market ranks after every confirmed one and falls to `cap`).
    pub recent_trade: Option<bool>,
    /// A category or trade lookup for this market failed this run, so it is unknown rather than
    /// rejected on merit. Never kept; rejected as `lookup_failed` and retried at the next refresh.
    pub lookup_failed: bool,
}

/// Owner-editable universe rules (`config/universe.toml`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniverseRules {
    /// Event categories to keep (case-insensitive match). Everything else is rejected as `category`.
    pub allow_categories: Vec<String>,
    /// Series-ticker prefixes that are always rejected (crypto and index price ranges).
    #[serde(default)]
    pub deny_series_prefixes: Vec<String>,
    #[serde(default = "default_max_markets")]
    pub max_markets: usize,
    #[serde(default = "default_max_close_days")]
    pub max_close_days: i64,
    #[serde(default = "default_min_close_secs")]
    pub min_close_secs: i64,
}

fn default_max_markets() -> usize {
    300
}
fn default_max_close_days() -> i64 {
    14
}
fn default_min_close_secs() -> i64 {
    3600
}

impl UniverseRules {
    pub fn load(path: &Path) -> Result<UniverseRules> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        text.parse()
            .with_context(|| format!("parse {}", path.display()))
    }
}

impl std::str::FromStr for UniverseRules {
    type Err = toml::de::Error;

    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        toml::from_str(text)
    }
}

/// Outcome of [`select`]. `kept` is ranked by `volume_24h` (highest first); every other market
/// appears once in `rejected` with the first rule that failed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    pub kept: Vec<String>,
    pub rejected: Vec<(String, &'static str)>,
}

impl Selection {
    /// Rejection counts by reason.
    pub fn reject_counts(&self) -> BTreeMap<&'static str, usize> {
        let mut counts = BTreeMap::new();
        for (_, reason) in &self.rejected {
            *counts.entry(*reason).or_insert(0) += 1;
        }
        counts
    }
}

fn denied(series_ticker: &str, rules: &UniverseRules) -> bool {
    rules
        .deny_series_prefixes
        .iter()
        .any(|p| series_ticker.starts_with(p.as_str()))
}

fn category_allowed(category: Option<&str>, rules: &UniverseRules) -> bool {
    category.is_some_and(|c| {
        rules
            .allow_categories
            .iter()
            .any(|a| a.eq_ignore_ascii_case(c))
    })
}

/// The first failing rule that needs only the listing itself (no category, no trade history), or
/// `None`. Order: deny prefix (reported as `category`), close window, quote, never traded (no
/// lifetime volume, so it cannot have a recent trade).
fn cheap_reject(m: &Market, rules: &UniverseRules, now: i64) -> Option<&'static str> {
    if denied(&m.series_ticker, rules) {
        return Some("category");
    }
    let secs_to_close = m.close_ts - now;
    if secs_to_close <= rules.min_close_secs {
        return Some("closes_too_soon");
    }
    if secs_to_close > rules.max_close_days * 86_400 {
        return Some("closes_too_late");
    }
    if m.yes_bid_c.is_none() || m.yes_ask_c.is_none() {
        return Some("no_two_sided_quote");
    }
    if m.volume == 0 && m.volume_24h == 0 {
        return Some("no_recent_trade");
    }
    None
}

/// Filter and rank. Rules apply in this order and a market gets the first reason that fails:
/// deny prefix (`category`), `closes_too_soon` (not more than `min_close_secs` away),
/// `closes_too_late` (more than `max_close_days` away), `no_two_sided_quote`, `no_recent_trade`
/// (never traded: no volume at all), `lookup_failed` (a lookup for it errored, so its category or
/// trade history is unknown), `category` (not on the allow list, or unknown),
/// `no_recent_trade` (checked and no trade in 7 days). Survivors beyond `max_markets` are rejected
/// as `cap`; they rank by `volume_24h`, with unchecked (`recent_trade == None`) ones last.
pub fn select(markets: &[MarketWithCategory], rules: &UniverseRules, now: i64) -> Selection {
    let mut rejected = Vec::new();
    let mut eligible: Vec<&MarketWithCategory> = Vec::new();
    for mc in markets {
        let reason = cheap_reject(&mc.market, rules, now)
            .or_else(|| mc.lookup_failed.then_some("lookup_failed"))
            .or_else(|| (!category_allowed(mc.category.as_deref(), rules)).then_some("category"))
            .or_else(|| (mc.recent_trade == Some(false)).then_some("no_recent_trade"));
        match reason {
            Some(r) => rejected.push((mc.market.ticker.clone(), r)),
            None => eligible.push(mc),
        }
    }
    // Highest 24 h volume first; lifetime volume then ticker keep the order deterministic.
    eligible.sort_by(|a, b| {
        a.recent_trade
            .is_none()
            .cmp(&b.recent_trade.is_none())
            .then(b.market.volume_24h.cmp(&a.market.volume_24h))
            .then(b.market.volume.cmp(&a.market.volume))
            .then(a.market.ticker.cmp(&b.market.ticker))
    });
    let kept = eligible
        .iter()
        .take(rules.max_markets)
        .map(|mc| mc.market.ticker.clone())
        .collect();
    rejected.extend(
        eligible
            .iter()
            .skip(rules.max_markets)
            .map(|mc| (mc.market.ticker.clone(), "cap")),
    );
    Selection { kept, rejected }
}

/// Category of `series`: from the `kv` cache, else looked up through one of its events.
async fn series_category(
    client: &Client<'_>,
    conn: &Connection,
    series: &str,
    event: &str,
) -> Result<Option<String>> {
    let key = format!("series_category:{series}");
    let cached: Option<String> = conn
        .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
        .optional()?;
    if cached.is_some() {
        return Ok(cached);
    }
    let category = client.get_event_category(conn, event).await?;
    if let Some(c) = &category {
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            [&key, c],
        )?;
    }
    Ok(category)
}

/// Everything a refresh learned, before anything is written.
pub struct Plan {
    /// Open markets the API returned.
    pub fetched: usize,
    /// Distinct series whose category was looked up (HTTP or cache).
    pub series_looked_up: usize,
    /// Lookups that failed (network or HTTP error): series categories plus per-market trade checks.
    /// Affected markets are rejected as `lookup_failed` for this run and retried at the next
    /// refresh; one bad lookup must not sink the whole universe.
    pub lookup_failures: usize,
    /// Markets that went through [`select`], with category and trade flag filled where it mattered.
    pub candidates: Vec<MarketWithCategory>,
    pub selection: Selection,
}

/// A market that traded within the last 7 days, as far as the listing can tell.
const RECENT_TRADE_SECS: i64 = 7 * 86_400;

/// Fetch, prefilter, look up categories and trade history, and select. Writes nothing except the
/// event-category cache (`kv`).
pub async fn plan(
    client: &Client<'_>,
    conn: &Connection,
    rules: &UniverseRules,
    now: i64,
) -> Result<Plan> {
    // The window is applied server-side to shrink paging; `cheap_reject` re-checks it below.
    let listed = client
        .list_open_markets_closing(
            Some(now + rules.min_close_secs),
            Some(now + rules.max_close_days * 86_400),
        )
        .await?;
    let fetched = listed.len();
    // An empty listing is an API fault, not an empty market: never replace the universe with it.
    anyhow::ensure!(
        fetched > 0,
        "kalshi returned no open markets in the close window; keeping the previous universe"
    );

    // Cheap filters first: only markets that pass them can need a category or trade lookup.
    let mut categories: HashMap<String, Option<String>> = HashMap::new();
    let mut candidates = Vec::with_capacity(fetched);
    let mut survivors: Vec<usize> = Vec::new();
    for market in listed {
        if cheap_reject(&market, rules, now).is_none() {
            survivors.push(candidates.len());
        }
        let recent_trade = if market.volume_24h > 0 {
            Some(true)
        } else {
            None
        };
        candidates.push(MarketWithCategory {
            recent_trade,
            category: None,
            lookup_failed: false,
            market,
        });
    }

    // Category per series: every event of a series shares its category, so one `/events` lookup
    // serves the whole series (about 1k series against 7k events in the window). The answer is
    // cached in `kv` across runs. Highest-volume first. A failed lookup leaves the category
    // unknown (rejected as `lookup_failed` this run, retried next run).
    survivors.sort_by_key(|&i| std::cmp::Reverse(candidates[i].market.volume_24h));
    let mut lookup_failures = 0;
    let mut failed_series: HashSet<String> = HashSet::new();
    for &i in &survivors {
        let series = candidates[i].market.series_ticker.clone();
        if !categories.contains_key(&series) {
            let event = candidates[i].market.event_ticker.clone();
            let category = match series_category(client, conn, &series, &event).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("universe: category of series {series} (event {event}): {e:#}");
                    lookup_failures += 1;
                    failed_series.insert(series.clone());
                    None
                }
            };
            categories.insert(series.clone(), category);
        }
        candidates[i].category = categories[&series].clone();
        candidates[i].lookup_failed = failed_series.contains(&series);
    }

    // Markets with no 24 h volume are checked for a trade in the last 7 days (one request each)
    // only while they could still make the cap: once confirmed markets fill it, the rest stay
    // unchecked (`None`) and are cut by `cap`.
    let mut on_list = survivors
        .iter()
        .filter(|&&i| {
            candidates[i].recent_trade == Some(true)
                && category_allowed(candidates[i].category.as_deref(), rules)
        })
        .count();
    let min_ts = now - RECENT_TRADE_SECS;
    for &i in &survivors {
        if on_list >= rules.max_markets {
            break;
        }
        let c = &candidates[i];
        if c.recent_trade.is_some() || !category_allowed(c.category.as_deref(), rules) {
            continue;
        }
        match client.has_trade_since(&c.market.ticker, min_ts).await {
            Ok(true) => {
                candidates[i].recent_trade = Some(true);
                on_list += 1;
            }
            Ok(false) => candidates[i].recent_trade = Some(false),
            Err(e) => {
                // Unconfirmed, so not kept this run (`lookup_failed`); retried at the next refresh.
                eprintln!("universe: trades of {}: {e:#}", c.market.ticker);
                candidates[i].lookup_failed = true;
                lookup_failures += 1;
            }
        }
    }

    let selection = select(&candidates, rules, now);
    Ok(Plan {
        fetched,
        series_looked_up: categories.len(),
        lookup_failures,
        candidates,
        selection,
    })
}

/// Refresh the universe against the production API and record it. Returns the `universe_runs` id.
pub async fn refresh(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    rules: &UniverseRules,
    now: i64,
) -> Result<i64> {
    refresh_with(&Client::new(http), conn, log, rules, now).await
}

/// [`refresh`] against an explicit client (tests point it at a mock server).
pub async fn refresh_with(
    client: &Client<'_>,
    conn: &Connection,
    log: &EventLog,
    rules: &UniverseRules,
    now: i64,
) -> Result<i64> {
    let plan = plan(client, conn, rules, now).await?;
    record(conn, log, rules, now, &plan)
}

/// Persist a plan: one `universe_runs` row, the kept markets flagged `active` (all others cleared),
/// and a `universe` event with the counts. The full rejection list goes to a gzip file.
pub fn record(
    conn: &Connection,
    log: &EventLog,
    rules: &UniverseRules,
    now: i64,
    plan: &Plan,
) -> Result<i64> {
    let kept: HashSet<&str> = plan.selection.kept.iter().map(String::as_str).collect();
    let counts = plan.selection.reject_counts();
    let params_json = serde_json::to_string(rules)?;
    let note = format!(
        "fetched {}, kept {}, rejected {:?}, lookup failures {}",
        plan.fetched,
        kept.len(),
        counts,
        plan.lookup_failures
    );

    let run_id = db::with_tx(conn, |tx| {
        tx.execute(
            "INSERT INTO universe_runs (ts, n_markets, params_json, note) VALUES (?1, ?2, ?3, ?4)",
            params![now, kept.len() as i64, params_json, note],
        )?;
        let run_id = tx.last_insert_rowid();
        tx.execute("UPDATE markets SET active = 0 WHERE active != 0", [])?;
        let mut upsert = tx.prepare(
            "INSERT INTO markets (ticker, event_ticker, series_ticker, title, category, status, close_ts, volume,
                                  universe_run_id, first_seen_ts, last_seen_ts, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, 1)
             ON CONFLICT(ticker) DO UPDATE SET
                 event_ticker = excluded.event_ticker, series_ticker = excluded.series_ticker,
                 title = excluded.title, category = excluded.category, status = excluded.status,
                 close_ts = excluded.close_ts, volume = excluded.volume,
                 universe_run_id = excluded.universe_run_id, last_seen_ts = excluded.last_seen_ts, active = 1",
        )?;
        for mc in plan
            .candidates
            .iter()
            .filter(|mc| kept.contains(mc.market.ticker.as_str()))
        {
            let m = &mc.market;
            upsert.execute(params![
                m.ticker,
                m.event_ticker,
                m.series_ticker,
                m.title,
                mc.category,
                m.status,
                m.close_ts,
                m.volume as f64,
                run_id,
                now,
            ])?;
        }
        Ok(run_id)
    })?;

    let rejections: String = plan
        .selection
        .rejected
        .iter()
        .map(|(t, r)| format!("{t}\t{r}\n"))
        .collect();
    let rejected_path = log.store_raw("universe-rejections", rejections.as_bytes());
    log.log(
        "universe",
        None,
        json!({
            "run_id": run_id,
            "fetched": plan.fetched,
            "series_looked_up": plan.series_looked_up,
            "lookup_failures": plan.lookup_failures,
            "kept": kept.len(),
            "rejected": plan.selection.rejected.len(),
            "rejected_by_reason": counts,
            "rejected_list": rejected_path,
            "rules": rules,
        }),
    );
    Ok(run_id)
}
