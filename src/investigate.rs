//! Per-candidate investigation state machine (spec sections 8 and 10).
//!
//! [`run_one`] takes one `queued` candidate through the whole pipeline:
//!
//! queued -> investigating -> phrases -> news search -> collect and date articles -> Haiku per article ->
//! branch -> Sonnet review -> decision -> one of `explained`, `no_buy`, `bought`, `stale`, `failed`,
//! `cap_skipped`, or back to `queued` for a retry.
//!
//! - **Staleness** is checked when the investigation starts, after the articles are collected (before the
//!   Haiku spend), before the Sonnet review and before buying: more than [`MAX_ELAPSED_S`] since the candidate
//!   was created, or the market's current mid back at or past the move's starting price (against the
//!   direction), ends it as `stale`.
//! - **Evidence quality** (ruling R23): on the UNEXPLAINED branch, a degraded search ([`evidence_degraded`])
//!   ends it as `no_buy` with reason `evidence_degraded`, before the Sonnet review.
//! - **Spend cap** hit anywhere (phrases, Haiku, Sonnet) ends it as `cap_skipped`; rows written before the hit
//!   stay. Too few valid phrases ends it as `failed` with reason `phrases`.
//! - **Other errors** re-queue the candidate with a backoff of 1, 4 and 16 minutes ([`RETRY_BACKOFF_S`];
//!   `candidates.retry_after_ts`, honoured by [`crate::candidates::next_queued`]); the fourth failure is
//!   `failed`. A retry reuses what earlier attempts paid for: stored phrases, stored Haiku verdicts per
//!   article and a stored Sonnet review.
//! - Every status change is a `candidate_state` row in `events` (and a line in the JSONL log).
//!
//! Single-threaded by design: the SQLite connection is not `Send`, and candidates are handled one at a time.

use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

use crate::config::Settings;
use crate::eventlog::EventLog;
use crate::http::Http;
use crate::kalshi::{Client, Market, Side};
use crate::llm::{Llm, LlmError};
use crate::news::article::{self, KeptArticle};
use crate::news::{self, NewsEndpoints};
use crate::paper::{self, OpenOutcome};
use crate::phrases::{self, MarketContext, PhraseError, MIN_VALID};
use crate::review::{
    self, branch_str, final_decision, side_str, Coverage, RelevantArticle, ReviewBar, ReviewInput,
    ReviewMarket, ReviewVerdict,
};
use crate::score;
use crate::verdict_haiku::{
    classify_judged, judge_all, Branch, HaikuVerdict, JudgedArticle, PointsTo,
};
use crate::watcher::mid_c;

/// A candidate older than this (from creation) is stale.
pub const MAX_ELAPSED_S: i64 = 20 * 60;
/// Wait before retry 1, 2 and 3 after a failed attempt; the failure after the third retry is final.
pub const RETRY_BACKOFF_S: [i64; 3] = [60, 4 * 60, 16 * 60];
/// Bars shown to Sonnet start this many minutes before the move started.
const BARS_LEAD_MIN: i64 = 15;

/// How [`run_one`] left the candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalStatus {
    Explained,
    NoBuy,
    Bought,
    Stale,
    Failed,
    CapSkipped,
    /// Back to `queued` after a failed attempt, held until `retry_after_ts`.
    Requeued,
}

impl FinalStatus {
    /// The `candidates.status` value.
    pub fn as_str(self) -> &'static str {
        match self {
            FinalStatus::Explained => "explained",
            FinalStatus::NoBuy => "no_buy",
            FinalStatus::Bought => "bought",
            FinalStatus::Stale => "stale",
            FinalStatus::Failed => "failed",
            FinalStatus::CapSkipped => "cap_skipped",
            FinalStatus::Requeued => "queued",
        }
    }
}

/// What an investigation needs. Base URLs are the production ones in the service and mock servers in tests.
pub struct Deps<'a> {
    pub http: &'a Http,
    pub conn: &'a Connection,
    pub log: &'a EventLog,
    pub settings: &'a Settings,
    pub news: &'a NewsEndpoints,
    pub kalshi_base: &'a str,
    pub anthropic_base: &'a str,
}

impl Deps<'_> {
    fn kalshi(&self) -> Client<'_> {
        Client::with_base(self.http, self.kalshi_base)
    }
}

struct Candidate {
    id: i64,
    status: String,
    direction: Side,
    move_start_ts: i64,
    created_ts: i64,
    attempts: i64,
}

/// One member market and its stored score.
#[derive(Clone)]
struct Member {
    ticker: String,
    total: f64,
    from_c: u32,
    to_c: u32,
    window_min: u32,
}

/// How the pipeline ended (before the status is written).
struct Ended {
    status: FinalStatus,
    reason: String,
    detail: Value,
}

impl Ended {
    fn new(status: FinalStatus, reason: impl Into<String>, detail: Value) -> Ended {
        Ended {
            status,
            reason: reason.into(),
            detail,
        }
    }
}

enum StageError {
    /// The spend governor refused a call (`day` or `run`).
    Cap(&'static str),
    /// Too few valid phrases.
    Phrases(String),
    /// Anything else: retried.
    Other(anyhow::Error),
}

impl From<anyhow::Error> for StageError {
    fn from(e: anyhow::Error) -> StageError {
        StageError::Other(e)
    }
}

impl From<rusqlite::Error> for StageError {
    fn from(e: rusqlite::Error) -> StageError {
        StageError::Other(e.into())
    }
}

fn from_llm(e: LlmError) -> StageError {
    match e {
        LlmError::CapHit { scope } => StageError::Cap(scope),
        e => StageError::Other(e.into()),
    }
}

// ---------------------------------------------------------------------------------------------------------
// State and events
// ---------------------------------------------------------------------------------------------------------

/// Write one `events` row and the matching JSONL line.
fn event(
    conn: &Connection,
    log: &EventLog,
    kind: &str,
    candidate_id: i64,
    payload: Value,
    now: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO events (ts, kind, candidate_id, payload) VALUES (?1, ?2, ?3, ?4)",
        params![now, kind, candidate_id, payload.to_string()],
    )?;
    log.log(kind, Some(candidate_id), payload);
    Ok(())
}

/// Move a candidate from `from` to `to` (refused if it is no longer in `from`), set its reason, attempt count
/// (when given) and `retry_after_ts` (cleared unless given), and record a `candidate_state` event.
#[allow(clippy::too_many_arguments)]
fn transition(
    conn: &Connection,
    log: &EventLog,
    id: i64,
    from: &str,
    to: &str,
    reason: &str,
    attempts: Option<i64>,
    retry_after_ts: Option<i64>,
    detail: Value,
    now: i64,
) -> Result<()> {
    let payload = json!({"from": from, "to": to, "reason": reason, "detail": detail});
    crate::db::with_tx(conn, |tx| {
        let n = tx.execute(
            "UPDATE candidates SET status = ?3, reason = ?4, updated_ts = ?5,
                 attempts = COALESCE(?6, attempts), retry_after_ts = ?7
             WHERE id = ?1 AND status = ?2",
            params![id, from, to, reason, now, attempts, retry_after_ts],
        )?;
        if n != 1 {
            bail!("candidate {id} is not {from}; cannot move it to {to}");
        }
        tx.execute(
            "INSERT INTO events (ts, kind, candidate_id, payload) VALUES (?1, 'candidate_state', ?2, ?3)",
            params![now, id, payload.to_string()],
        )?;
        Ok(())
    })?;
    log.log("candidate_state", Some(id), payload);
    Ok(())
}

/// After a restart nothing is investigating any more: every `investigating` candidate becomes `failed` with
/// reason `restart` (each with its event). Returns how many.
pub fn recover_on_boot(conn: &Connection, log: &EventLog) -> Result<usize> {
    let ids: Vec<i64> = conn
        .prepare("SELECT id FROM candidates WHERE status = 'investigating' ORDER BY id")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let now = chrono::Utc::now().timestamp();
    for &id in &ids {
        transition(
            conn,
            log,
            id,
            "investigating",
            "failed",
            "restart",
            None,
            None,
            json!({}),
            now,
        )?;
    }
    Ok(ids.len())
}

// ---------------------------------------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------------------------------------

fn load_candidate(conn: &Connection, id: i64) -> Result<Candidate> {
    let row = conn
        .query_row(
            "SELECT status, direction, move_start_ts, created_ts, attempts FROM candidates WHERE id = ?1",
            [id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| anyhow!("candidate {id} does not exist"))?;
    let direction = match row.1.as_str() {
        "yes" => Side::Yes,
        "no" => Side::No,
        other => bail!("candidate {id} has direction {other:?}"),
    };
    Ok(Candidate {
        id,
        status: row.0,
        direction,
        move_start_ts: row.2,
        created_ts: row.3,
        attempts: row.4,
    })
}

/// Member markets as stored now (late joiners included), best (highest score) first.
fn load_members(conn: &Connection, id: i64) -> Result<Vec<Member>> {
    let rows: Vec<(String, Option<String>)> = conn
        .prepare("SELECT ticker, score_json FROM candidate_markets WHERE candidate_id = ?1 ORDER BY ticker")?
        .query_map([id], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut out = Vec::new();
    for (ticker, raw) in rows {
        let v: Value = raw
            .as_deref()
            .and_then(|j| serde_json::from_str(j).ok())
            .ok_or_else(|| anyhow!("candidate {id} market {ticker} has no score"))?;
        let num = |k: &str| {
            v[k].as_f64()
                .ok_or_else(|| anyhow!("candidate {id} market {ticker}: score has no {k}"))
        };
        out.push(Member {
            total: num("total")?,
            from_c: num("from_c")? as u32,
            to_c: num("to_c")? as u32,
            window_min: num("window_min")? as u32,
            ticker,
        });
    }
    if out.is_empty() {
        bail!("candidate {id} has no markets");
    }
    out.sort_by(|a, b| b.total.total_cmp(&a.total));
    Ok(out)
}

/// Accepted phrases stored by an earlier attempt, in order.
fn stored_phrases(conn: &Connection, id: i64) -> rusqlite::Result<Vec<String>> {
    conn.prepare("SELECT phrase FROM phrases WHERE candidate_id = ?1 AND accepted = 1 ORDER BY id")?
        .query_map([id], |r| r.get(0))?
        .collect()
}

/// Haiku verdicts already stored for this candidate, newest per article.
fn stored_verdicts(conn: &Connection, id: i64) -> rusqlite::Result<HashMap<i64, JudgedArticle>> {
    let mut stmt = conn.prepare(
        "SELECT hv.id, hv.article_id, hv.status, hv.relevant, hv.points_to, hv.strength, hv.explains_move,
                hv.quote, hv.reason, hv.error, qc.found
         FROM haiku_verdicts hv LEFT JOIN quote_checks qc ON qc.haiku_verdict_id = hv.id
         WHERE hv.candidate_id = ?1 ORDER BY hv.id",
    )?;
    let rows = stmt.query_map([id], |r| {
        let status: String = r.get(2)?;
        let points_to: Option<String> = r.get(4)?;
        let verdict = if status == "ok" {
            let points = points_to
                .and_then(|p| serde_json::from_value::<PointsTo>(json!(p)).ok())
                .unwrap_or(PointsTo::Neither);
            Some(HaikuVerdict {
                relevant: r.get::<_, Option<bool>>(3)?.unwrap_or(false),
                points_to: points,
                strength: r.get::<_, Option<u8>>(5)?.unwrap_or(0),
                explains_move: r.get::<_, Option<bool>>(6)?.unwrap_or(false),
                quote: r.get::<_, Option<String>>(7)?.unwrap_or_default(),
                reason: r.get::<_, Option<String>>(8)?.unwrap_or_default(),
            })
        } else {
            None
        };
        let error: Option<String> = r.get(9)?;
        Ok(JudgedArticle {
            verdict_id: r.get(0)?,
            article_id: r.get(1)?,
            quote_ok: verdict.is_some() && r.get::<_, Option<bool>>(10)?.unwrap_or(false),
            error: if verdict.is_none() {
                Some(error.unwrap_or_else(|| "haiku_error".into()))
            } else {
                None
            },
            verdict,
        })
    })?;
    let mut out = HashMap::new();
    for j in rows {
        let j = j?;
        out.insert(j.article_id, j);
    }
    Ok(out)
}

/// Current markets by ticker.
async fn fetch_markets(deps: &Deps<'_>, tickers: &[String]) -> Result<HashMap<String, Market>> {
    Ok(deps
        .kalshi()
        .get_markets(tickers)
        .await?
        .into_iter()
        .map(|m| (m.ticker.clone(), m))
        .collect())
}

fn current_mid(m: &Market) -> Option<f64> {
    mid_c(m.yes_bid_c, m.yes_ask_c, m.last_c)
}

/// `Some(stale)` when the candidate is too old or `member`'s current mid is back at or past its `from_c`
/// against the direction. A market missing from `markets` is an error (retried).
fn stale_check(
    c: &Candidate,
    member: &Member,
    markets: &HashMap<String, Market>,
    stage: &str,
    now: i64,
) -> Result<Option<Ended>, StageError> {
    let elapsed = now - c.created_ts;
    if elapsed > MAX_ELAPSED_S {
        return Ok(Some(Ended::new(
            FinalStatus::Stale,
            "elapsed_20min",
            json!({"stage": stage, "elapsed_s": elapsed}),
        )));
    }
    let m = markets.get(&member.ticker).ok_or_else(|| {
        StageError::Other(anyhow!(
            "market {} missing from the kalshi reply",
            member.ticker
        ))
    })?;
    if let Some(mid) = current_mid(m) {
        let from = f64::from(member.from_c);
        let reversed = match c.direction {
            Side::Yes => mid <= from,
            Side::No => mid >= from,
        };
        if reversed {
            return Ok(Some(Ended::new(
                FinalStatus::Stale,
                "price_reversed",
                json!({"stage": stage, "ticker": member.ticker, "mid_c": mid, "from_c": member.from_c}),
            )));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------------------

/// Investigate queued candidate `candidate_id` to its end (or a re-queue). `now_fn` is the clock (UTC unix
/// seconds) used for staleness, the article window and the trade time. `Err` only when the candidate is not
/// queued or its state cannot be written.
pub async fn run_one(
    deps: &Deps<'_>,
    candidate_id: i64,
    now_fn: &dyn Fn() -> i64,
) -> Result<FinalStatus> {
    let (conn, log) = (deps.conn, deps.log);
    let cand = load_candidate(conn, candidate_id)?;
    if cand.status != "queued" {
        bail!("candidate {candidate_id} is {}, not queued", cand.status);
    }
    transition(
        conn,
        log,
        candidate_id,
        "queued",
        "investigating",
        "start",
        None,
        None,
        json!({"attempt": cand.attempts + 1}),
        now_fn(),
    )?;

    let llm = Llm::new(deps.http, conn, log, deps.settings, deps.anthropic_base);
    let result = pipeline(deps, &llm, &cand, now_fn).await;
    let now = now_fn();
    let ended = match result {
        Ok(e) => e,
        Err(StageError::Cap(scope)) => {
            Ended::new(FinalStatus::CapSkipped, "cap_hit", json!({"scope": scope}))
        }
        Err(StageError::Phrases(msg)) => {
            Ended::new(FinalStatus::Failed, "phrases", json!({"error": msg}))
        }
        Err(StageError::Other(e)) => {
            let msg = format!("{e:#}");
            conn.execute(
                "INSERT INTO errors (ts, source, candidate_id, message) VALUES (?1, 'investigate', ?2, ?3)",
                params![now, candidate_id, msg],
            )?;
            let attempts = cand.attempts + 1;
            return match RETRY_BACKOFF_S.get((attempts - 1) as usize) {
                Some(wait) => {
                    transition(
                        conn,
                        log,
                        candidate_id,
                        "investigating",
                        "queued",
                        "retry",
                        Some(attempts),
                        Some(now + wait),
                        json!({"error": msg, "attempt": attempts, "retry_after_s": wait}),
                        now,
                    )?;
                    Ok(FinalStatus::Requeued)
                }
                None => {
                    transition(
                        conn,
                        log,
                        candidate_id,
                        "investigating",
                        "failed",
                        "error",
                        Some(attempts),
                        None,
                        json!({"error": msg, "attempt": attempts}),
                        now,
                    )?;
                    Ok(FinalStatus::Failed)
                }
            };
        }
    };
    transition(
        conn,
        log,
        candidate_id,
        "investigating",
        ended.status.as_str(),
        &ended.reason,
        None,
        None,
        ended.detail,
        now,
    )?;
    Ok(ended.status)
}

async fn pipeline(
    deps: &Deps<'_>,
    llm: &Llm<'_>,
    cand: &Candidate,
    now_fn: &dyn Fn() -> i64,
) -> Result<Ended, StageError> {
    let (conn, log, id) = (deps.conn, deps.log, cand.id);
    let members = load_members(conn, id)?;
    let tickers: Vec<String> = members.iter().map(|m| m.ticker.clone()).collect();
    let best = members[0].clone();

    // Market text for the prompts, and the first staleness check.
    let markets = fetch_markets(deps, &tickers).await?;
    if let Some(stale) = stale_check(cand, &best, &markets, "start", now_fn())? {
        return Ok(stale);
    }
    let bm = &markets[&best.ticker];
    let ctx = MarketContext {
        title: bm.title.clone(),
        subtitle: bm.subtitle.clone(),
        rules: bm.rules_primary.clone(),
        direction: cand.direction,
        from_c: best.from_c,
        to_c: best.to_c,
        window_min: best.window_min,
    };

    // 1. Phrases (reused on a retry).
    let stored = stored_phrases(conn, id)?;
    let phrase_list = if stored.len() >= MIN_VALID {
        event(
            conn,
            log,
            "phrases_reused",
            id,
            json!({"count": stored.len()}),
            now_fn(),
        )?;
        stored
    } else {
        match phrases::generate(llm, conn, id, &ctx).await {
            Ok(p) => p,
            Err(PhraseError::Llm(e)) => return Err(from_llm(e)),
            Err(e @ PhraseError::TooFew { .. }) => return Err(StageError::Phrases(e.to_string())),
            Err(e) => return Err(StageError::Other(anyhow!(e.to_string()))),
        }
    };

    // 2-3. News search (RSS, NewsAPI.ai fallback); 4. fetch, date and window-test the articles.
    let search = news::search_all(
        deps.http,
        conn,
        log,
        deps.news,
        deps.settings.newsapi_key.as_deref(),
        &phrase_list,
        id,
    )
    .await;
    let kept = article::collect(
        deps.http,
        deps.news,
        conn,
        log,
        id,
        &search.hits,
        cand.move_start_ts,
        now_fn(),
    )
    .await;

    // Fetching and dating articles can take minutes: check staleness again before paying for Haiku.
    let markets = fetch_markets(deps, &tickers).await?;
    if let Some(stale) = stale_check(cand, &best, &markets, "after_collect", now_fn())? {
        return Ok(stale);
    }

    // 5-6. Haiku per article (stored verdicts are not judged again), 7. branch.
    let judged = judge_kept(llm, conn, id, &ctx, &kept).await?;
    let branch = classify_judged(&judged, cand.direction);
    let haiku_errors = judged.iter().filter(|j| j.error.is_some()).count();
    let quality = json!({
        "kept": kept.len(),
        "haiku_errors": haiku_errors,
        "rss_ok": search.rss_ok,
        "rss_failed": search.rss_failed,
        "fallback_used": search.fallback_used,
        "fallback_ok": search.fallback_ok,
    });
    let mut branch_payload = quality.clone();
    branch_payload["branch"] = json!(branch_str(branch));
    branch_payload["relevant"] = json!(judged
        .iter()
        .filter(|j| j.verdict.as_ref().is_some_and(|v| v.relevant))
        .count());
    event(conn, log, "haiku_branch", id, branch_payload, now_fn())?;

    // Evidence-quality gate (ruling R23): an outage must not look like "no news" and become a buy. Checked
    // before the Sonnet review so a degraded search does not pay for one.
    if branch == Branch::Unexplained {
        if let Some(why) = evidence_degraded(
            kept.len(),
            haiku_errors,
            search.rss_ok,
            search.rss_failed,
            search.fallback_ok,
        ) {
            let mut detail = quality;
            detail["branch"] = json!(branch_str(branch));
            detail["degraded"] = json!(why);
            return Ok(Ended::new(FinalStatus::NoBuy, "evidence_degraded", detail));
        }
    }

    // 8. Sonnet review, after a fresh staleness check.
    let markets = fetch_markets(deps, &tickers).await?;
    let now = now_fn();
    if let Some(stale) = stale_check(cand, &best, &markets, "before_review", now)? {
        return Ok(stale);
    }
    // A stored review is reused only if it was made on exactly this evidence: a retry can find new articles.
    let evidence: Vec<(i64, Option<bool>)> = judged
        .iter()
        .map(|j| (j.article_id, j.verdict.as_ref().map(|v| v.relevant)))
        .collect();
    let evidence_fp = review::evidence_fingerprint(branch, &evidence);
    let r = match review::load_latest(conn, id, &evidence_fp)? {
        Some(r) => {
            event(
                conn,
                log,
                "review_reused",
                id,
                json!({"evidence_fp": evidence_fp}),
                now,
            )?;
            r
        }
        None => {
            let input = build_input(
                conn,
                cand,
                branch,
                &members,
                &markets,
                &kept,
                &judged,
                &search,
                now,
                evidence_fp.clone(),
            )?;
            review::review(llm, conn, id, &input)
                .await
                .map_err(from_llm)?
        }
    };
    let d = final_decision(branch, &r, cand.direction, &tickers);
    review::record_final(conn, id, &evidence_fp, &d)?;
    let decision_detail = json!({
        "branch": branch_str(branch), "sonnet_decision": r.decision, "side": r.side, "ticker": r.ticker,
        "hallucination_found": r.hallucination_found, "explained_by_public_info": r.explained_by_public_info,
        "buy": d.buy, "reason": d.reason,
    });
    event(conn, log, "decision", id, decision_detail.clone(), now_fn())?;

    if branch == Branch::Explained {
        // Sonnet can confirm a buy on the UNEXPLAINED path, never create one here.
        let reason = if r.decision == "buy" {
            event(
                conn,
                log,
                "sonnet_disagrees",
                id,
                decision_detail.clone(),
                now_fn(),
            )?;
            "sonnet_disagrees"
        } else {
            d.reason
        };
        return Ok(Ended::new(FinalStatus::Explained, reason, decision_detail));
    }
    let Some(ticker) = d.ticker.clone().filter(|_| d.buy) else {
        return Ok(Ended::new(FinalStatus::NoBuy, d.reason, decision_detail));
    };

    // Buy, after one more staleness check on the chosen market.
    let chosen = members
        .iter()
        .find(|m| m.ticker == ticker)
        .cloned()
        .expect("final_decision only picks member tickers");
    let markets = fetch_markets(deps, std::slice::from_ref(&ticker)).await?;
    if let Some(stale) = stale_check(cand, &chosen, &markets, "before_buy", now_fn())? {
        return Ok(stale);
    }
    // No paper trade outside the run window (an investigation can start just before the run ends).
    if !matches!(
        crate::run::phase(conn, now_fn())?,
        crate::run::Phase::Active { .. }
    ) {
        return Ok(Ended::new(
            FinalStatus::NoBuy,
            "run_ended",
            json!({"ticker": ticker}),
        ));
    }
    let opened = paper::open_outcome(
        &deps.kalshi(),
        conn,
        log,
        id,
        &ticker,
        cand.direction,
        now_fn(),
    )
    .await?;
    Ok(match opened {
        OpenOutcome::Opened(trade_id) => Ended::new(
            FinalStatus::Bought,
            "ok",
            json!({"trade_id": trade_id, "ticker": ticker, "side": side_str(cand.direction)}),
        ),
        OpenOutcome::Refused(why) => Ended::new(
            FinalStatus::NoBuy,
            format!("paper_{why}"),
            json!({"ticker": ticker, "paper_refused": why}),
        ),
    })
}

/// Share of kept articles that may be Haiku errors before the evidence counts as degraded.
pub const MAX_HAIKU_ERROR_SHARE: f64 = 0.25;

/// Why the evidence is too degraded to buy on (ruling R23), or `None`: more than
/// [`MAX_HAIKU_ERROR_SHARE`] of the kept articles are Haiku errors (`haiku_errors`), or more RSS queries
/// failed than succeeded and no NewsAPI.ai fallback succeeded (`news_search`).
pub fn evidence_degraded(
    kept: usize,
    haiku_errors: usize,
    rss_ok: u32,
    rss_failed: u32,
    fallback_ok: bool,
) -> Option<&'static str> {
    if kept > 0 && haiku_errors as f64 > MAX_HAIKU_ERROR_SHARE * kept as f64 {
        return Some("haiku_errors");
    }
    if rss_failed > rss_ok && !fallback_ok {
        return Some("news_search");
    }
    None
}

/// Verdicts for `kept`, in order: stored ones reused, the rest judged now.
async fn judge_kept(
    llm: &Llm<'_>,
    conn: &Connection,
    id: i64,
    ctx: &MarketContext,
    kept: &[KeptArticle],
) -> Result<Vec<JudgedArticle>, StageError> {
    let mut stored = stored_verdicts(conn, id)?;
    let todo: Vec<KeptArticle> = kept
        .iter()
        .filter(|a| !stored.contains_key(&a.article_id))
        .cloned()
        .collect();
    let fresh = judge_all(llm, conn, id, ctx, &todo)
        .await
        .map_err(from_llm)?;
    for j in fresh {
        stored.insert(j.article_id, j);
    }
    Ok(kept
        .iter()
        .filter_map(|a| stored.remove(&a.article_id))
        .collect())
}

/// Everything Sonnet is shown.
#[allow(clippy::too_many_arguments)]
fn build_input(
    conn: &Connection,
    cand: &Candidate,
    branch: Branch,
    members: &[Member],
    markets: &HashMap<String, Market>,
    kept: &[KeptArticle],
    judged: &[JudgedArticle],
    search: &news::SearchOutcome,
    now: i64,
    evidence_fp: String,
) -> Result<ReviewInput, StageError> {
    let review_markets = members
        .iter()
        .map(|m| {
            let km = markets.get(&m.ticker);
            ReviewMarket {
                ticker: m.ticker.clone(),
                title: km.map(|k| k.title.clone()).unwrap_or_default(),
                subtitle: km.map(|k| k.subtitle.clone()).unwrap_or_default(),
                rules: km.map(|k| k.rules_primary.clone()).unwrap_or_default(),
                from_c: m.from_c,
                to_c: m.to_c,
                window_min: m.window_min,
                current_mid_c: km.and_then(current_mid),
            }
        })
        .collect();

    let best = &members[0];
    // Bars from 15 minutes before the move started up to now. `load_bars` reaches back 26 h from `now`, and a
    // candidate is stale 20 minutes after it was created, so the whole span is always loaded.
    let since = cand.move_start_ts - BARS_LEAD_MIN * 60;
    let bars = score::load_bars(conn, &best.ticker, now)?
        .into_iter()
        .filter(|b| b.ts >= since)
        .map(|b| ReviewBar {
            ts: b.ts,
            minute: (b.ts - cand.move_start_ts).div_euclid(60),
            mid_c: b.mid_c,
            volume: b.volume,
            taker_yes: b.taker_yes,
            taker_no: b.taker_no,
        })
        .collect();

    let by_id: HashMap<i64, &KeptArticle> = kept.iter().map(|a| (a.article_id, a)).collect();
    let mut verdicts = Vec::new();
    let mut relevant_articles = Vec::new();
    for j in judged {
        let Some(a) = by_id.get(&j.article_id) else {
            continue;
        };
        let v = j.verdict.as_ref();
        verdicts.push(ReviewVerdict {
            article_id: a.article_id,
            title: a.title.clone(),
            source: a.source.clone(),
            url: a.url.clone(),
            used_ts: a.used_ts,
            minutes_from_move_start: ((a.used_ts - cand.move_start_ts) as f64 / 6.0).round() / 10.0,
            time_source: a.time_source.as_str().to_string(),
            text_unavailable: a.text_unavailable,
            relevant: v.map(|v| v.relevant),
            points_to: v.map(|v| v.points_to.as_str().to_string()),
            strength: v.map(|v| v.strength),
            explains_move: v.map(|v| v.explains_move),
            quote: v.map(|v| v.quote.clone()),
            quote_ok: j.quote_ok,
            reason: v.map(|v| v.reason.clone()),
            haiku_error: j.error.clone(),
        });
        if v.is_some_and(|v| v.relevant) {
            relevant_articles.push(RelevantArticle {
                article_id: a.article_id,
                title: a.title.clone(),
                text: a.text.clone(),
            });
        }
    }

    let mut coverage = Coverage {
        rss_ok: search.rss_ok,
        rss_failed: search.rss_failed,
        fallback_used: search.fallback_used,
        articles_kept: kept.len() as u32,
        ..Coverage::default()
    };
    let mut stmt = conn.prepare(
        "SELECT COALESCE(decision, 'unknown'), COUNT(*) FROM articles
         WHERE candidate_id = ?1 AND COALESCE(decision, '') != 'kept' GROUP BY 1",
    )?;
    for row in stmt.query_map([cand.id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, u32>(1)?))
    })? {
        let (decision, n) = row?;
        coverage.articles_dropped.insert(decision, n);
    }

    Ok(ReviewInput {
        branch,
        direction: cand.direction,
        move_start_ts: cand.move_start_ts,
        markets: review_markets,
        bars_ticker: best.ticker.clone(),
        bars,
        verdicts,
        relevant_articles,
        coverage,
        evidence_fp,
    })
}
