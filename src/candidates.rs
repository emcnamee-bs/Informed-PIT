//! Candidate creation (spec section 10): scores at or above the threshold become candidates that are
//! investigated with paid LLM calls, so grouping, dedup and the daily cap here are cost guardrails.
//!
//! Identity of a candidate is Kalshi event + direction + move start (rounded to the minute). The scorer's
//! move start shifts every minute, so a repeat is recognised by the event's newest same-direction candidate
//! `C` and the price, not by the move start. Decision order for a score at or above the threshold:
//!
//! 1. **Join** (takes precedence over dedup): `C` was created less than [`JOIN_WINDOW_S`] ago. A market not
//!    yet in `C` is added to `candidate_markets`, and a market already in it whose score improved gets its
//!    stored score refreshed; both are [`Outcome::JoinedCandidate`]. A market already in `C` with no better
//!    score is a repeat: [`Outcome::DedupSuppressed`]. `C`'s score becomes the max of its members and its
//!    best `to_c` moves further along the direction (up for Yes, down for No).
//! 2. **Dedup** against `C` (any status), when `C` is at least 30 minutes old:
//!    - within [`COOLDOWN_S`] (6 h) of `C.created_ts`: suppressed unless `to_c` is at least [`FURTHER_POINTS`]
//!      (10) points beyond `C.best_to_c` in the move's direction, which creates a new candidate;
//!    - after 6 h: a new candidate only if `to_c != C.best_to_c` (the price has actually moved since); an
//!      unchanged price keeps being suppressed forever, so a move that sits unchanged for days yields exactly
//!      one candidate.
//! 3. **Daily cap**: `daily_cap` (`PIT_MAX_CANDIDATES_PER_DAY`, default [`DAILY_CAP`]) candidates created in
//!    the UTC calendar day of `now` is the limit ([`Outcome::DailyCapSkipped`], nothing inserted).
//! 4. Otherwise a `queued` candidate is inserted.
//!
//! Every score at or above [`STORE_MIN`] is also written to `scores` with all four parts.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::eventlog::EventLog;
use crate::kalshi::Side;
use crate::score::Score;

/// Scores below this are not stored at all.
pub const STORE_MIN: f64 = 40.0;
/// A market crossing the threshold within this long of a candidate's creation joins it.
pub const JOIN_WINDOW_S: i64 = 30 * 60;
/// After this long an event+direction candidate no longer blocks a NEW move (a different move start).
pub const COOLDOWN_S: i64 = 6 * 3600;
/// A market must move this many cents beyond a candidate's best price to override its suppression.
pub const FURTHER_POINTS: f64 = 10.0;
/// Default for the most candidates created per UTC day (`PIT_MAX_CANDIDATES_PER_DAY`).
pub const DAILY_CAP: u32 = 30;
/// A queued candidate older than this is marked stale.
pub const QUEUE_TIMEOUT_S: i64 = 20 * 60;

const DAY_S: i64 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Score in `[40, threshold)`: stored, not a candidate.
    Stored,
    /// Score below 40: nothing stored.
    Ignored,
    NewCandidate(i64),
    JoinedCandidate(i64),
    DedupSuppressed(i64),
    DailyCapSkipped,
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Yes => "yes",
        Side::No => "no",
    }
}

/// `true` when `price` is at least `points` cents beyond `best` in the move's direction.
fn beyond(side: Side, price: f64, best: f64, points: f64) -> bool {
    match side {
        Side::Yes => price >= best + points,
        Side::No => price <= best - points,
    }
}

/// The price further along the move's direction.
fn further(side: Side, a: f64, b: f64) -> f64 {
    match side {
        Side::Yes => a.max(b),
        Side::No => a.min(b),
    }
}

fn score_json(ticker: &str, s: &Score) -> Value {
    json!({
        "ticker": ticker,
        "total": s.total,
        "move_pts": s.move_pts,
        "volume_pts": s.volume_pts,
        "onesided_pts": s.onesided_pts,
        "quiet_pts": s.quiet_pts,
        "window_min": s.window_min,
        "move_start_ts": s.move_start_ts,
        "direction": side_str(s.direction),
        "from_c": s.from_c,
        "to_c": s.to_c,
        "move_z": s.move_z,
        "vol_ratio": s.vol_ratio,
        "onesided": s.onesided,
        "quiet_ratio": s.quiet_ratio,
        "window_trades": s.window_trades,
    })
}

fn store_score(
    tx: &Transaction<'_>,
    ticker: &str,
    event: &str,
    s: &Score,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO scores (ts, ticker, event_ticker, direction, move_start_ts, from_c, to_c, n_trades, score, parts_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            now,
            ticker,
            event,
            side_str(s.direction),
            s.move_start_ts,
            s.from_c,
            s.to_c,
            s.window_trades,
            s.total,
            score_json(ticker, s).to_string(),
        ],
    )?;
    Ok(())
}

struct Existing {
    id: i64,
    created_ts: i64,
    best_to_c: f64,
    score: f64,
}

/// The newest candidate (any status) for `event` + `direction`.
fn latest_candidate(
    tx: &Transaction<'_>,
    event: &str,
    direction: &str,
) -> rusqlite::Result<Option<Existing>> {
    tx.query_row(
        "SELECT id, created_ts, best_to_c, score FROM candidates
         WHERE event_ticker = ?1 AND direction = ?2
         ORDER BY created_ts DESC, id DESC LIMIT 1",
        params![event, direction],
        |r| {
            Ok(Existing {
                id: r.get(0)?,
                created_ts: r.get(1)?,
                best_to_c: r.get::<_, Option<f64>>(2)?.unwrap_or(f64::NAN),
                score: r.get(3)?,
            })
        },
    )
    .optional()
}

/// Add `ticker` to candidate `c` or refresh its stored score; also pull the candidate's best price and score
/// forward. Returns `true` when the market was new to the candidate or its score improved.
fn add_or_refresh_member(
    tx: &Transaction<'_>,
    c: &Existing,
    ticker: &str,
    s: &Score,
    now: i64,
) -> rusqlite::Result<bool> {
    let json = score_json(ticker, s).to_string();
    let old_total: Option<f64> = tx
        .query_row(
            "SELECT score_json FROM candidate_markets WHERE candidate_id = ?1 AND ticker = ?2",
            params![c.id, ticker],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?
        .map(|j| {
            j.and_then(|j| serde_json::from_str::<Value>(&j).ok())
                .and_then(|v| v["total"].as_f64())
                .unwrap_or(f64::NEG_INFINITY)
        });
    let improved = old_total.is_none_or(|t| s.total > t);
    if old_total.is_none() {
        tx.execute(
            "INSERT INTO candidate_markets (candidate_id, ticker, score_json) VALUES (?1, ?2, ?3)",
            params![c.id, ticker, json],
        )?;
    } else if improved {
        tx.execute(
            "UPDATE candidate_markets SET score_json = ?3 WHERE candidate_id = ?1 AND ticker = ?2",
            params![c.id, ticker, json],
        )?;
    }
    let best = if c.best_to_c.is_nan() {
        f64::from(s.to_c)
    } else {
        further(s.direction, c.best_to_c, f64::from(s.to_c))
    };
    tx.execute(
        "UPDATE candidates SET score = ?2, best_to_c = ?3, updated_ts = ?4 WHERE id = ?1",
        params![c.id, c.score.max(s.total), best, now],
    )?;
    Ok(improved)
}

fn created_today(tx: &Transaction<'_>, now: i64) -> rusqlite::Result<i64> {
    let day_start = now.div_euclid(DAY_S) * DAY_S;
    tx.query_row(
        "SELECT COUNT(*) FROM candidates WHERE created_ts >= ?1 AND created_ts < ?2",
        params![day_start, day_start + DAY_S],
        |r| r.get(0),
    )
}

/// Decide what a market's score means (see the module docs) and record it. `now` is UTC unix seconds.
/// The whole decision runs in one write transaction; the event log is written after it commits. `daily_cap` is
/// the most candidates created per UTC day (`Settings.max_candidates_per_day`).
#[allow(clippy::too_many_arguments)]
pub fn on_score(
    conn: &Connection,
    log: &EventLog,
    ticker: &str,
    event_ticker: &str,
    s: &Score,
    now: i64,
    threshold: f64,
    daily_cap: u32,
) -> anyhow::Result<Outcome> {
    if s.total < STORE_MIN {
        return Ok(Outcome::Ignored);
    }
    let direction = side_str(s.direction);
    let move_start_min = s.move_start_ts - s.move_start_ts.rem_euclid(60);
    let to_c = f64::from(s.to_c);

    // (outcome, event kind, reason) decided inside the transaction.
    let (outcome, reason) = crate::db::with_tx(conn, |tx| {
        store_score(tx, ticker, event_ticker, s, now)?;
        if s.total < threshold {
            return Ok((Outcome::Stored, ""));
        }
        let latest = latest_candidate(tx, event_ticker, direction)?;

        if let Some(c) = latest
            .as_ref()
            .filter(|c| now - c.created_ts < JOIN_WINDOW_S)
        {
            return Ok(if add_or_refresh_member(tx, c, ticker, s, now)? {
                (Outcome::JoinedCandidate(c.id), "joined_within_30min")
            } else {
                (Outcome::DedupSuppressed(c.id), "repeat_within_30min")
            });
        }

        if let Some(c) = &latest {
            let rearm = if now - c.created_ts < COOLDOWN_S {
                !c.best_to_c.is_nan() && beyond(s.direction, to_c, c.best_to_c, FURTHER_POINTS)
            } else {
                c.best_to_c.is_nan() || (to_c - c.best_to_c).abs() > 1e-9
            };
            if !rearm {
                let why = if now - c.created_ts < COOLDOWN_S {
                    "within_6h_cooldown"
                } else {
                    "price_unchanged_after_6h"
                };
                return Ok((Outcome::DedupSuppressed(c.id), why));
            }
        }

        if created_today(tx, now)? >= i64::from(daily_cap) {
            return Ok((Outcome::DailyCapSkipped, "daily_cap"));
        }
        tx.execute(
            "INSERT INTO candidates (event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts, best_to_c, from_c)
             VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?5, ?6, ?7)",
            params![event_ticker, direction, move_start_min, s.total, now, to_c, s.from_c],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO candidate_markets (candidate_id, ticker, score_json) VALUES (?1, ?2, ?3)",
            params![id, ticker, score_json(ticker, s).to_string()],
        )?;
        Ok((Outcome::NewCandidate(id), "new_candidate"))
    })?;

    let (kind, id) = match outcome {
        Outcome::NewCandidate(id) => ("candidate_new", Some(id)),
        Outcome::JoinedCandidate(id) => ("candidate_joined", Some(id)),
        Outcome::DedupSuppressed(id) => ("dedup_suppressed", Some(id)),
        Outcome::DailyCapSkipped => ("daily_cap_skipped", None),
        Outcome::Stored | Outcome::Ignored => return Ok(outcome),
    };
    let mut payload = score_json(ticker, s);
    payload["event_ticker"] = json!(event_ticker);
    payload["reason"] = json!(reason);
    payload["threshold"] = json!(threshold);
    log.log(kind, id, payload);
    Ok(outcome)
}

/// The queued candidate to investigate next: highest score first, then oldest. A candidate re-queued after a
/// failed attempt is skipped while its `retry_after_ts` is later than `now`.
pub fn next_queued(conn: &Connection, now: i64) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT id FROM candidates
         WHERE status = 'queued' AND (retry_after_ts IS NULL OR retry_after_ts <= ?1)
         ORDER BY score DESC, created_ts ASC, id ASC LIMIT 1",
        [now],
        |r| r.get(0),
    )
    .optional()
}

/// Mark queued candidates created more than [`QUEUE_TIMEOUT_S`] ago as `stale` (reason `queue_timeout`), each
/// with a `candidate_state` event (in `events` and the log). Returns how many were expired.
pub fn expire_old(conn: &Connection, log: &EventLog, now: i64) -> anyhow::Result<usize> {
    let expired: Vec<(i64, i64)> = crate::db::with_tx(conn, |tx| {
        let rows: Vec<(i64, i64)> = tx
            .prepare(
                "SELECT id, created_ts FROM candidates WHERE status = 'queued' AND created_ts < ?1 ORDER BY id",
            )?
            .query_map([now - QUEUE_TIMEOUT_S], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (id, created) in &rows {
            tx.execute(
                "UPDATE candidates SET status = 'stale', reason = 'queue_timeout', updated_ts = ?2
                 WHERE id = ?1 AND status = 'queued'",
                params![id, now],
            )?;
            tx.execute(
                "INSERT INTO events (ts, kind, candidate_id, payload) VALUES (?1, 'candidate_state', ?2, ?3)",
                params![now, id, expired_payload(now - created).to_string()],
            )?;
        }
        Ok(rows)
    })?;
    for (id, created) in &expired {
        log.log("candidate_state", Some(*id), expired_payload(now - created));
    }
    Ok(expired.len())
}

/// The `candidate_state` payload of a queue timeout (same shape as the investigator's transitions).
fn expired_payload(waited_s: i64) -> Value {
    json!({"from": "queued", "to": "stale", "reason": "queue_timeout", "detail": {"queued_s": waited_s}})
}
