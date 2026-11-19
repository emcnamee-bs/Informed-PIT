//! Run lifecycle (spec section 9), the verdict, the kill switch and the long-running service loop.
//!
//! **Timeline.** `start --confirm` stores `run_start_ts` and `run_end_ts = start + 30 days` in `kv`. Before
//! that the service is [`Phase::NotStarted`]: it watches and scores (warm-up for the 24 h history) but creates
//! no candidates. During the 30 days it is [`Phase::Active`]. After the end it is [`Phase::SettlementOnly`]
//! while paper trades are open (watcher, scorer and investigator off; settlement and status on), then
//! [`Phase::Finished`].
//!
//! **Service design.** A rusqlite `Connection` is not `Send`, so the service runs on a current-thread runtime
//! inside one `LocalSet`, as three cooperative `spawn_local` tasks, each with its **own connection** to the
//! same WAL database (WAL lets them read while another writes; the 5 s busy timeout covers write overlap):
//!
//! - **minute task**: at every UTC minute boundary, watcher tick, then score every active market, then
//!   `on_score`; `expire_old` (in every phase); settlement every 10 minutes; `status.json` every minute. A tick that
//!   overruns skips the missed minute(s) (logged as `tick_overrun`; the watcher logs the gaps itself).
//! - **investigator task**: pulls the next queued candidate while the run is active, not paused and under the
//!   LLM caps, and runs it to its end. An investigation takes minutes; because it awaits network I/O, the
//!   minute task keeps ticking meanwhile.
//! - **daily task**: universe refresh at 06:00 UTC (retried every 15 minutes after a failure) and the 60-day
//!   prune. The first refresh runs before any task starts (cold category cache: ~10 minutes).
//!
//! The HTTP layer (and its per-service rate gates) is shared by all three tasks.

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::json;

use crate::config::Settings;
use crate::eventlog::EventLog;
use crate::http::Http;
use crate::investigate::{self, Deps, FinalStatus};
use crate::news::NewsEndpoints;
use crate::universe::UniverseRules;
use crate::{candidates, db, kalshi, llm, paper, score, status, universe, watcher};

pub const DAY_S: i64 = 86_400;
/// Length of the run (new candidates), in days.
pub const RUN_DAYS: i64 = 30;
/// Settled trades needed before the verdict can be KEEP or SHUT OFF.
pub const MIN_SETTLED: usize = 30;
/// Bootstrap resamples and seed for the verdict's confidence interval.
pub const BOOTSTRAP_RESAMPLES: usize = 2_000;
pub const BOOTSTRAP_SEED: u64 = 7;
/// Name of the kill-switch file in the data dir.
pub const PAUSE_FILE: &str = "PAUSE";

const KV_START: &str = "run_start_ts";
const KV_END: &str = "run_end_ts";
const KV_PAUSED: &str = "paused";
/// UTC day index (`ts / 86400`) on which an investigation ended `cap_skipped`; investigation stays off that day.
const KV_CAP_HIT_DAY: &str = "cap_hit_day";
/// Prefix of the per-UTC-date count of `dedup_suppressed` score outcomes (`dedup_suppressed:YYYY-MM-DD`).
pub const KV_DEDUP_PREFIX: &str = "dedup_suppressed:";
/// Prefix of the heartbeat keys: last successful run of a loop part (`hb:watcher`, `hb:scorer`, `hb:settlement`).
pub const KV_HB_PREFIX: &str = "hb:";

const SETTLE_EVERY_S: i64 = 10 * 60;
const UNIVERSE_HOUR_UTC: i64 = 6;
const UNIVERSE_RETRY_S: i64 = 15 * 60;
const INVESTIGATOR_IDLE: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------------------------------------
// kv helpers
// ---------------------------------------------------------------------------------------------------------

pub fn kv_get(conn: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    Ok(conn
        .query_row("SELECT value FROM kv WHERE key = ?1", [key], |r| {
            r.get::<_, Option<String>>(0)
        })
        .optional()?
        .flatten())
}

pub fn kv_i64(conn: &Connection, key: &str) -> rusqlite::Result<Option<i64>> {
    Ok(kv_get(conn, key)?.and_then(|v| v.trim().parse().ok()))
}

pub fn kv_set(conn: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}

/// `YYYY-MM-DD` of a UTC unix time.
pub fn utc_date(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

pub fn day_start(ts: i64) -> i64 {
    ts - ts.rem_euclid(DAY_S)
}

// ---------------------------------------------------------------------------------------------------------
// Phase
// ---------------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    NotStarted,
    /// `day` is 1 on the first 24 h of the run, 30 on the last.
    Active {
        day: i64,
    },
    SettlementOnly,
    Finished,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::NotStarted => "not_started",
            Phase::Active { .. } => "active",
            Phase::SettlementOnly => "settlement_only",
            Phase::Finished => "finished",
        }
    }

    /// Watcher and scorer run (NotStarted is the warm-up).
    pub fn watching(self) -> bool {
        matches!(self, Phase::NotStarted | Phase::Active { .. })
    }
}

/// `(run_start_ts, run_end_ts)` once the run has been started.
pub fn run_window(conn: &Connection) -> rusqlite::Result<Option<(i64, i64)>> {
    Ok(match (kv_i64(conn, KV_START)?, kv_i64(conn, KV_END)?) {
        (Some(s), Some(e)) => Some((s, e)),
        _ => None,
    })
}

pub fn open_trades(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM paper_trades WHERE status = 'open'",
        [],
        |r| r.get(0),
    )
}

fn investigating(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM candidates WHERE status = 'investigating'",
        [],
        |r| r.get(0),
    )
}

/// The phase at `now` (UTC unix seconds). Finished = after the end with no open paper trade.
pub fn phase(conn: &Connection, now: i64) -> rusqlite::Result<Phase> {
    let Some((start, end)) = run_window(conn)? else {
        return Ok(Phase::NotStarted);
    };
    Ok(if now < start {
        Phase::NotStarted
    } else if now < end {
        Phase::Active {
            day: (now - start) / DAY_S + 1,
        }
    } else if open_trades(conn)? > 0 || investigating(conn)? > 0 {
        Phase::SettlementOnly
    } else {
        Phase::Finished
    })
}

/// `start --confirm`: freeze the run window. Refuses when a run was already started.
pub fn start(conn: &Connection, log: &EventLog, now: i64) -> Result<(i64, i64)> {
    if let Some((s, e)) = run_window(conn)? {
        bail!("the run was already started at {s} (ends {e}); refusing to start again");
    }
    let end = now + RUN_DAYS * DAY_S;
    db::with_tx(conn, |tx| {
        kv_set(tx, KV_START, &now.to_string())?;
        kv_set(tx, KV_END, &end.to_string())?;
        tx.execute(
            "INSERT INTO events (ts, kind, payload) VALUES (?1, 'run_start', ?2)",
            params![
                now,
                json!({"run_start_ts": now, "run_end_ts": end}).to_string()
            ],
        )?;
        Ok(())
    })?;
    log.log(
        "run_start",
        None,
        json!({"run_start_ts": now, "run_end_ts": end}),
    );
    Ok((now, end))
}

// ---------------------------------------------------------------------------------------------------------
// Kill switch and caps
// ---------------------------------------------------------------------------------------------------------

pub fn pause_file(data_dir: &Path) -> PathBuf {
    data_dir.join(PAUSE_FILE)
}

/// Paused when the kv flag is set OR the `PAUSE` file exists in the data dir.
pub fn is_paused(conn: &Connection, data_dir: &Path) -> rusqlite::Result<bool> {
    Ok(pause_file(data_dir).exists() || kv_get(conn, KV_PAUSED)?.as_deref() == Some("1"))
}

/// `pause` / `resume`: set or clear both the kv flag and the `PAUSE` file, with an event row.
pub fn set_paused(
    conn: &Connection,
    log: &EventLog,
    data_dir: &Path,
    paused: bool,
    now: i64,
) -> Result<()> {
    let file = pause_file(data_dir);
    if paused {
        std::fs::create_dir_all(data_dir)?;
        std::fs::write(&file, format!("paused at {now}\n"))
            .with_context(|| format!("write {}", file.display()))?;
    } else if file.exists() {
        std::fs::remove_file(&file).with_context(|| format!("remove {}", file.display()))?;
    }
    let kind = if paused { "pause" } else { "resume" };
    db::with_tx(conn, |tx| {
        kv_set(tx, KV_PAUSED, if paused { "1" } else { "0" })?;
        tx.execute(
            "INSERT INTO events (ts, kind, payload) VALUES (?1, ?2, '{}')",
            params![now, kind],
        )?;
        Ok(())
    })?;
    log.log(kind, None, json!({}));
    Ok(())
}

/// LLM spend with `since <= ts < until`.
pub fn spend_between(conn: &Connection, since: i64, until: i64) -> rusqlite::Result<f64> {
    conn.query_row(
        "SELECT COALESCE(SUM(cost_usd), 0) FROM llm_calls WHERE ts >= ?1 AND ts < ?2",
        params![since, until],
        |r| r.get(0),
    )
}

/// LLM spend on the UTC day of `now`.
pub fn spend_today(conn: &Connection, now: i64) -> rusqlite::Result<f64> {
    spend_between(conn, day_start(now), day_start(now) + DAY_S)
}

/// LLM spend since the run start (all time before the run is started), up to `now` inclusive.
pub fn spend_run(conn: &Connection, now: i64) -> rusqlite::Result<f64> {
    let since = run_window(conn)?.map(|(s, _)| s).unwrap_or(0);
    spend_between(conn, since, now + 1)
}

/// The LLM cap is hit: day or run spend at its cap, or an investigation ended `cap_skipped` today (the
/// governor refuses a call whose estimate would cross the cap before spend reaches it).
pub fn cap_hit(conn: &Connection, settings: &Settings, now: i64) -> rusqlite::Result<bool> {
    Ok(spend_today(conn, now)? >= settings.daily_usd_cap
        || spend_run(conn, now)? >= settings.run_usd_cap
        || kv_i64(conn, KV_CAP_HIT_DAY)? == Some(now.div_euclid(DAY_S)))
}

fn mark_cap_hit_today(conn: &Connection, now: i64) -> rusqlite::Result<()> {
    kv_set(conn, KV_CAP_HIT_DAY, &now.div_euclid(DAY_S).to_string())
}

/// `reset-cap` (spec section 10 manual reset): clear today's cap-hit mark so investigations resume, with an
/// event row. Spend at or over a cap still counts as hit: this only lifts the mark set when the governor
/// refused a call.
pub fn reset_cap(conn: &Connection, log: &EventLog, now: i64) -> Result<()> {
    db::with_tx(conn, |tx| {
        tx.execute("DELETE FROM kv WHERE key = ?1", [KV_CAP_HIT_DAY])?;
        tx.execute(
            "INSERT INTO events (ts, kind, payload) VALUES (?1, 'cap_reset', '{}')",
            [now],
        )?;
        Ok(())
    })?;
    log.log("cap_reset", None, json!({}));
    Ok(())
}

/// Every queued candidate becomes `cap_skipped` (reason `cap_hit`), each with a `candidate_state` event.
/// Returns how many.
pub fn expire_queued_cap_skipped(conn: &Connection, log: &EventLog, now: i64) -> Result<usize> {
    let ids: Vec<i64> = db::with_tx(conn, |tx| {
        let ids: Vec<i64> = tx
            .prepare("SELECT id FROM candidates WHERE status = 'queued' ORDER BY id")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for &id in &ids {
            tx.execute(
                "UPDATE candidates SET status = 'cap_skipped', reason = 'cap_hit', updated_ts = ?2 WHERE id = ?1",
                params![id, now],
            )?;
            tx.execute(
                "INSERT INTO events (ts, kind, candidate_id, payload) VALUES (?1, 'candidate_state', ?2, ?3)",
                params![now, id, cap_payload().to_string()],
            )?;
        }
        Ok(ids)
    })?;
    for &id in &ids {
        log.log("candidate_state", Some(id), cap_payload());
    }
    Ok(ids.len())
}

fn cap_payload() -> serde_json::Value {
    json!({"from": "queued", "to": "cap_skipped", "reason": "cap_hit"})
}

/// The candidate the investigator should take now: `None` unless the run is active, not paused and under
/// the LLM caps; else [`candidates::next_queued`].
pub fn next_investigation(conn: &Connection, settings: &Settings, now: i64) -> Result<Option<i64>> {
    if !matches!(phase(conn, now)?, Phase::Active { .. })
        || is_paused(conn, &settings.data_dir)?
        || cap_hit(conn, settings, now)?
    {
        return Ok(None);
    }
    Ok(candidates::next_queued(conn, now)?)
}

// ---------------------------------------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------------------------------------

/// One settled paper trade as the verdict sees it (`pnl_c` straight from `settlements`).
#[derive(Debug, Clone, PartialEq)]
pub struct SettledTrade {
    pub event_ticker: String,
    pub contracts: i64,
    pub pnl_c: i64,
    /// The market resolved to neither yes nor no (cost refunded, `pnl_c` 0). Ruling R21: voids are left out
    /// of `n`, the bootstrap and the win rate, and counted separately.
    pub void: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Verdict {
    /// `KEEP`, `SHUT OFF` or `NOT ENOUGH TRADES`.
    pub label: String,
    /// Settled trades, voids excluded.
    pub n: usize,
    /// Voided settlements (not in `n`, the P&L or the range).
    pub voids: usize,
    /// Distinct events among them (the bootstrap's clusters).
    pub events: usize,
    /// Total settled P&L after fees, cents.
    pub pnl_c: i64,
    /// Total P&L / total contracts, cents per contract (`None` with no trades).
    pub mean_c_per_contract: Option<f64>,
    /// 95 % event-cluster bootstrap range of the per-contract P&L.
    pub ci_lo: Option<f64>,
    pub ci_hi: Option<f64>,
    /// True until the run is finished (last trade settled).
    pub provisional: bool,
}

/// SplitMix64: a tiny deterministic generator, enough for a fixed-seed bootstrap.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// The verdict on settled trades (pure). Voided settlements are counted in `voids` and otherwise left out.
/// Label: fewer than [`MIN_SETTLED`] trades is `NOT ENOUGH TRADES`,
/// else `KEEP` when total `pnl_c > 0`, else `SHUT OFF`. The range resamples whole events with replacement
/// ([`BOOTSTRAP_RESAMPLES`] times, seed [`BOOTSTRAP_SEED`]) and takes the 2.5th and 97.5th percentile of
/// total P&L / total contracts. `provisional` is set to true here; [`load_verdict`] clears it once the run
/// is finished.
pub fn verdict(all: &[SettledTrade]) -> Verdict {
    let settled: Vec<&SettledTrade> = all.iter().filter(|t| !t.void).collect();
    let voids = all.len() - settled.len();
    let n = settled.len();
    let pnl_c: i64 = settled.iter().map(|t| t.pnl_c).sum();
    let contracts: i64 = settled.iter().map(|t| t.contracts).sum();
    // Clusters in first-seen order so the resampling is deterministic.
    let mut clusters: Vec<(&str, f64, f64)> = Vec::new();
    for t in &settled {
        match clusters.iter_mut().find(|c| c.0 == t.event_ticker) {
            Some(c) => {
                c.1 += t.pnl_c as f64;
                c.2 += t.contracts as f64;
            }
            None => clusters.push((&t.event_ticker, t.pnl_c as f64, t.contracts as f64)),
        }
    }
    let mean = (contracts > 0).then(|| pnl_c as f64 / contracts as f64);
    let (ci_lo, ci_hi) = match mean {
        Some(_) => {
            let (lo, hi) = bootstrap_ci(&clusters);
            (Some(lo), Some(hi))
        }
        None => (None, None),
    };
    let label = if n < MIN_SETTLED {
        "NOT ENOUGH TRADES"
    } else if pnl_c > 0 {
        "KEEP"
    } else {
        "SHUT OFF"
    };
    Verdict {
        label: label.to_string(),
        n,
        voids,
        events: clusters.len(),
        pnl_c,
        mean_c_per_contract: mean,
        ci_lo,
        ci_hi,
        provisional: true,
    }
}

fn bootstrap_ci(clusters: &[(&str, f64, f64)]) -> (f64, f64) {
    let mut rng = SplitMix64(BOOTSTRAP_SEED);
    let m = clusters.len();
    let mut stats: Vec<f64> = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let (mut p, mut c) = (0.0, 0.0);
        for _ in 0..m {
            let k = &clusters[rng.below(m)];
            p += k.1;
            c += k.2;
        }
        if c > 0.0 {
            stats.push(p / c);
        }
    }
    stats.sort_by(|a, b| a.total_cmp(b));
    let at = |q: f64| stats[((q * stats.len() as f64) as usize).min(stats.len() - 1)];
    (at(0.025), at(0.975))
}

/// Settled trades with `settled_ts < until`, oldest first.
pub fn settled_trades(conn: &Connection, until: i64) -> rusqlite::Result<Vec<SettledTrade>> {
    let mut stmt = conn.prepare(
        "SELECT p.event_ticker, p.contracts, s.pnl_c, s.result NOT IN ('yes', 'no') FROM settlements s
         JOIN paper_trades p ON p.id = s.trade_id
         WHERE s.settled_ts < ?1 ORDER BY s.settled_ts, s.id",
    )?;
    let rows = stmt.query_map([until], |r| {
        Ok(SettledTrade {
            event_ticker: r.get(0)?,
            contracts: r.get(1)?,
            pnl_c: r.get(2)?,
            void: r.get(3)?,
        })
    })?;
    rows.collect()
}

/// The verdict on everything settled so far; final (not provisional) once the run is [`Phase::Finished`].
pub fn load_verdict(conn: &Connection, now: i64) -> rusqlite::Result<Verdict> {
    let mut v = verdict(&settled_trades(conn, i64::MAX)?);
    v.provisional = phase(conn, now)? != Phase::Finished;
    Ok(v)
}

// ---------------------------------------------------------------------------------------------------------
// Loop parts
// ---------------------------------------------------------------------------------------------------------

fn heartbeat(conn: &Connection, part: &str, now: i64) {
    let _ = kv_set(conn, &format!("{KV_HB_PREFIX}{part}"), &now.to_string());
}

/// Record an error in `errors` and the JSONL log. Never fails.
pub fn record_error(conn: &Connection, log: &EventLog, source: &str, msg: &str, now: i64) {
    let _ = conn.execute(
        "INSERT INTO errors (ts, source, message) VALUES (?1, ?2, ?3)",
        params![now, source, msg],
    );
    log.log("error", None, json!({"source": source, "error": msg}));
}

/// A loop step failed (usually a transient database error): an `errors` row, a `loop_error` event row and a
/// log line. The loop retries on its next tick. Never fails.
pub fn record_loop_error(
    conn: &Connection,
    log: &EventLog,
    source: &str,
    err: &anyhow::Error,
    now: i64,
) {
    let msg = format!("{err:#}");
    record_error(conn, log, source, &msg, now);
    let _ = conn.execute(
        "INSERT INTO events (ts, kind, payload) VALUES (?1, 'loop_error', ?2)",
        params![now, json!({"source": source, "error": msg}).to_string()],
    );
}

/// One settlement pass; the `settlement` heartbeat is written only when no step failed (failures are
/// `errors` rows with source `settlement`, so the light turns warn, then down).
pub async fn settlement_pass(
    client: &kalshi::Client<'_>,
    conn: &Connection,
    log: &EventLog,
    now: i64,
) -> paper::SettlePass {
    let pass = paper::settle_due_with(client, conn, log, now).await;
    if pass.failures == 0 {
        heartbeat(conn, "settlement", now);
    }
    if pass.settled > 0 || pass.failures > 0 {
        log.log(
            "settlement_pass",
            None,
            json!({"settled": pass.settled, "failures": pass.failures}),
        );
    }
    pass
}

/// What one scoring pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScoreReport {
    pub markets: usize,
    pub scored: usize,
    pub new_candidates: usize,
    pub dedup_suppressed: usize,
    pub errors: usize,
}

/// Score every active market at `minute_ts` and pass each score to [`candidates::on_score`]. `threshold`
/// is `f64::INFINITY` outside the active run, so scores are stored but no candidate is created. The day's
/// `dedup_suppressed` count is kept in `kv`. `daily_cap` is the most candidates created per UTC day.
pub fn score_all(
    conn: &Connection,
    log: &EventLog,
    minute_ts: i64,
    threshold: f64,
    daily_cap: u32,
) -> Result<ScoreReport> {
    let markets: Vec<(String, String)> = conn
        .prepare("SELECT ticker, event_ticker FROM markets WHERE active = 1 ORDER BY ticker")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut rep = ScoreReport {
        markets: markets.len(),
        ..Default::default()
    };
    for (ticker, event) in &markets {
        let result = score::load_bars(conn, ticker, minute_ts)
            .map_err(anyhow::Error::from)
            .and_then(|bars| match score::score_market(&bars, minute_ts) {
                Some(s) => candidates::on_score(
                    conn, log, ticker, event, &s, minute_ts, threshold, daily_cap,
                )
                .map(Some),
                None => Ok(None),
            });
        match result {
            Ok(Some(outcome)) => {
                rep.scored += 1;
                match outcome {
                    candidates::Outcome::NewCandidate(_) => rep.new_candidates += 1,
                    candidates::Outcome::DedupSuppressed(_) => rep.dedup_suppressed += 1,
                    _ => {}
                }
            }
            Ok(None) => {}
            Err(e) => {
                rep.errors += 1;
                record_error(conn, log, "scorer", &format!("{ticker}: {e:#}"), minute_ts);
            }
        }
    }
    if rep.dedup_suppressed > 0 {
        let key = format!("{KV_DEDUP_PREFIX}{}", utc_date(minute_ts));
        let before = kv_i64(conn, &key)?.unwrap_or(0);
        kv_set(
            conn,
            &key,
            &(before + rep.dedup_suppressed as i64).to_string(),
        )?;
    }
    Ok(rep)
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Sleep until wall-clock unix second `ts` (returns at once when it has passed).
async fn sleep_until_ts(ts: i64) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let wait = (ts * 1000 - now_ms).max(0) as u64;
    tokio::time::sleep(Duration::from_millis(wait)).await;
}

/// One watcher tick and scoring pass for `minute_ts`. Errors are recorded, never returned.
async fn minute_work(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    settings: &Settings,
    phase: Phase,
    minute_ts: i64,
    now_fn: &dyn Fn() -> i64,
) {
    match watcher::tick(http, conn, log, minute_ts).await {
        Ok(_) => heartbeat(conn, "watcher", now_fn()),
        Err(e) => record_error(conn, log, "watcher", &format!("{e:#}"), now_fn()),
    }
    let threshold = match phase {
        Phase::Active { .. } => settings.score_threshold,
        _ => f64::INFINITY,
    };
    match score_all(
        conn,
        log,
        minute_ts,
        threshold,
        settings.max_candidates_per_day,
    ) {
        Ok(rep) => {
            if rep.errors == 0 || rep.scored > 0 {
                heartbeat(conn, "scorer", now_fn());
            }
            log.log(
                "score_pass",
                None,
                json!({"minute_ts": minute_ts, "markets": rep.markets, "scored": rep.scored,
                       "new_candidates": rep.new_candidates, "dedup_suppressed": rep.dedup_suppressed,
                       "errors": rep.errors, "threshold": if threshold.is_finite() { json!(threshold) } else { json!(null) }}),
            );
        }
        Err(e) => record_error(conn, log, "scorer", &format!("{e:#}"), now_fn()),
    }
}

/// Everything the minute task does at `minute_ts` except the status write. `last_settle` is the time of the
/// last settlement pass and `now_fn` the clock. A database error (e.g. reading the phase) is returned; the loop records it and
/// tries again at the next minute.
pub async fn minute_step(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    settings: &Settings,
    minute_ts: i64,
    last_settle: &mut i64,
    now_fn: &dyn Fn() -> i64,
) -> Result<()> {
    let phase = phase(conn, now_fn())?;
    if phase.watching() {
        minute_work(http, conn, log, settings, phase, minute_ts, now_fn).await;
    }
    // Queued candidates time out in every phase, so none stays queued after the run window.
    if let Err(e) = candidates::expire_old(conn, log, now_fn()) {
        record_error(conn, log, "expire", &e.to_string(), now_fn());
    }
    if phase != Phase::Finished && now_fn() - *last_settle >= SETTLE_EVERY_S {
        settlement_pass(&kalshi::Client::new(http), conn, log, now_fn()).await;
        *last_settle = now_fn();
    }
    Ok(())
}

async fn minute_loop(
    http: Rc<Http>,
    conn: Connection,
    log: Rc<EventLog>,
    settings: Settings,
) -> Result<()> {
    let mut last_settle = 0;
    let mut minute_ts = minute_floor(now_ts()) + 60;
    loop {
        sleep_until_ts(minute_ts).await;
        if let Err(e) = minute_step(
            &http,
            &conn,
            &log,
            &settings,
            minute_ts,
            &mut last_settle,
            &now_ts,
        )
        .await
        {
            record_loop_error(&conn, &log, "minute_loop", &e, now_ts());
        }
        if let Err(e) = status::write(&conn, &settings, now_ts()) {
            record_error(&conn, &log, "status", &format!("{e:#}"), now_ts());
        }
        // R11: the next boundary after now; minutes the work overran are skipped, not caught up.
        let next = minute_floor(now_ts()) + 60;
        if next > minute_ts + 60 {
            log.log(
                "tick_overrun",
                None,
                json!({"minute_ts": minute_ts, "skipped_minutes": (next - minute_ts) / 60 - 1}),
            );
        }
        minute_ts = next;
    }
}

fn minute_floor(ts: i64) -> i64 {
    ts - ts.rem_euclid(60)
}

/// What one investigator step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvestigatorStep {
    /// Nothing to investigate (or not allowed now).
    Idle,
    /// One candidate was investigated (or its run failed and was recovered).
    Worked,
}

/// One investigator step: when the run is active and the LLM cap is hit, expire the queue as
/// `cap_skipped`; otherwise investigate the next candidate, if any. A `run_one` error (the candidate's state
/// could not be written) is recorded and the candidate is recovered (`failed`, reason `restart`) rather than
/// left `investigating`. Other database errors are returned for the loop to record and retry.
pub async fn investigator_step(
    deps: &Deps<'_>,
    now_fn: &dyn Fn() -> i64,
) -> Result<InvestigatorStep> {
    let (conn, log, settings) = (deps.conn, deps.log, deps.settings);
    let now = now_fn();
    if matches!(phase(conn, now)?, Phase::Active { .. }) && cap_hit(conn, settings, now)? {
        let n = expire_queued_cap_skipped(conn, log, now)?;
        if n > 0 {
            log.log("cap_skipped_queue", None, json!({"expired": n}));
        }
    }
    let Some(id) = next_investigation(conn, settings, now)? else {
        return Ok(InvestigatorStep::Idle);
    };
    match investigate::run_one(deps, id, now_fn).await {
        Ok(FinalStatus::CapSkipped) => {
            mark_cap_hit_today(conn, now_fn())?;
            expire_queued_cap_skipped(conn, log, now_fn())?;
        }
        Ok(_) => {}
        Err(e) => {
            record_error(
                conn,
                log,
                "investigate",
                &format!("candidate {id}: {e:#}"),
                now_fn(),
            );
            investigate::recover_on_boot(conn, log)?;
        }
    }
    Ok(InvestigatorStep::Worked)
}

async fn investigator_loop(
    http: Rc<Http>,
    conn: Connection,
    log: Rc<EventLog>,
    settings: Settings,
) -> Result<()> {
    let news = NewsEndpoints::default();
    let deps = Deps {
        http: &http,
        conn: &conn,
        log: &log,
        settings: &settings,
        news: &news,
        kalshi_base: kalshi::DEFAULT_BASE,
        anthropic_base: llm::ANTHROPIC_BASE,
    };
    loop {
        match investigator_step(&deps, &now_ts).await {
            Ok(InvestigatorStep::Worked) => {}
            Ok(InvestigatorStep::Idle) => tokio::time::sleep(INVESTIGATOR_IDLE).await,
            Err(e) => {
                record_loop_error(&conn, &log, "investigator", &e, now_ts());
                tokio::time::sleep(INVESTIGATOR_IDLE).await;
            }
        }
    }
}

/// Next 06:00 UTC strictly after `now`.
pub fn next_universe_refresh(now: i64) -> i64 {
    let at = day_start(now) + UNIVERSE_HOUR_UTC * 3600;
    if at > now {
        at
    } else {
        at + DAY_S
    }
}

/// One universe refresh, errors recorded. Returns whether it succeeded.
async fn refresh_universe(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    rules: &UniverseRules,
) -> bool {
    let now = now_ts();
    match universe::refresh(http, conn, log, rules, now).await {
        Ok(_) => true,
        Err(e) => {
            record_error(conn, log, "universe", &format!("{e:#}"), now_ts());
            false
        }
    }
}

async fn daily_loop(
    http: Rc<Http>,
    conn: Connection,
    log: Rc<EventLog>,
    rules: UniverseRules,
    first_ok: bool,
) -> Result<()> {
    let mut due = if first_ok {
        next_universe_refresh(now_ts())
    } else {
        now_ts() + UNIVERSE_RETRY_S
    };
    loop {
        sleep_until_ts(due).await;
        let watching = match phase(&conn, now_ts()) {
            Ok(p) => p.watching(),
            Err(e) => {
                record_loop_error(&conn, &log, "daily_loop", &e.into(), now_ts());
                due = now_ts() + 60;
                continue;
            }
        };
        if !watching {
            due = next_universe_refresh(now_ts());
            continue;
        }
        let ok = refresh_universe(&http, &conn, &log, &rules).await;
        match watcher::prune(&conn, now_ts()) {
            Ok((s, t)) => log.log("prune", None, json!({"snapshots": s, "trades": t})),
            Err(e) => record_error(&conn, &log, "prune", &format!("{e:#}"), now_ts()),
        }
        match log.prune_files(&conn, now_ts()) {
            Ok(p) => log.log(
                "prune_files",
                None,
                json!({"raw_files": p.raw_files, "log_files": p.log_files}),
            ),
            Err(e) => record_error(&conn, &log, "prune_files", &format!("{e:#}"), now_ts()),
        }
        due = if ok {
            next_universe_refresh(now_ts())
        } else {
            now_ts() + UNIVERSE_RETRY_S
        };
    }
}

/// The service (`informed-pit run`). Runs until a task fails; the systemd unit restarts it.
pub async fn serve(settings: Settings, rules: UniverseRules) -> Result<()> {
    let data = settings.data_dir.clone();
    let db_path = data.join("pit.db");
    let open = || db::open(&db_path).with_context(|| format!("open {}", db_path.display()));
    let conn = open()?;
    let log = Rc::new(EventLog::new(&data));
    let http = Rc::new(Http::new(EventLog::new(&data)));

    let recovered = investigate::recover_on_boot(&conn, &log)?;
    let ph = phase(&conn, now_ts())?;
    log.log(
        "service_start",
        None,
        json!({"phase": ph.as_str(), "recovered": recovered, "settings": format!("{settings:?}")}),
    );
    // Write a status before the (long) first refresh so the tile shows the service is up.
    if let Err(e) = status::write(&conn, &settings, now_ts()) {
        record_error(&conn, &log, "status", &format!("{e:#}"), now_ts());
    }
    // R9: the first refresh runs before the watcher starts.
    let first_ok = if ph.watching() {
        refresh_universe(&http, &conn, &log, &rules).await
    } else {
        true
    };

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let minute = tokio::task::spawn_local(minute_loop(
                http.clone(),
                conn,
                log.clone(),
                settings.clone(),
            ));
            let investigator = tokio::task::spawn_local(investigator_loop(
                http.clone(),
                open()?,
                log.clone(),
                settings.clone(),
            ));
            let daily =
                tokio::task::spawn_local(daily_loop(http, open()?, log.clone(), rules, first_ok));
            let (name, res) = tokio::select! {
                r = minute => ("minute", r),
                r = investigator => ("investigator", r),
                r = daily => ("daily", r),
            };
            let err = match res {
                Ok(Ok(())) => anyhow::anyhow!("{name} task ended"),
                Ok(Err(e)) => e.context(format!("{name} task failed")),
                Err(join) => anyhow::anyhow!("{name} task panicked: {join}"),
            };
            log.log("service_stop", None, json!({"error": format!("{err:#}")}));
            Err(err)
        })
        .await
}
