//! `status.json` for wall tile 17 (spec section 12): built from the database, written atomically every minute.
//!
//! Definitions the tile relies on:
//! - **lights** (`ok` / `warn` / `down`, or `off` when the phase switches the part off): the watcher, scorer and
//!   settlement are periodic, judged on the age of their last success (heartbeat in `kv`) against their period
//!   (ok within 3 periods and no error in the last hour, warn within 10 periods, else down). RSS, NewsAPI,
//!   Haiku and Sonnet only run for candidates, so they are judged on the last hour: no failure is ok (idle
//!   included), failures next to successes warn, only failures is down. A missing key overrides that: no
//!   `ANTHROPIC_API_KEY` while the run is active puts Haiku and Sonnet at down (no investigation can work),
//!   and no `NEWSAPI_AI_KEY` puts NewsAPI at off (the fallback is not configured).
//! - **today**: candidates created on the current UTC day by status; `unexplained` counts those reviewed by
//!   Sonnet on the Unexplained branch (they end as `bought` or `no_buy`); `dedup_suppressed` counts suppressed
//!   repeat scores.
//! - **funnel** (run totals): `math_pit` candidates created in the run window, `to_sonnet` of those the ones
//!   with a Sonnet review on the Unexplained branch, `paper_buys` paper trades opened in the run window.
//! - **trades**: `settled` counts every settlement, `voids` those that resolved neither yes nor no (R21: left
//!   out of the verdict's `n`, its range and the win rate).
//! - **series.days**: one point per UTC day of the run. `open_mtm_c` is the unrealised P&L of positions open at
//!   the end of that day, marked at the last stored mid (`contracts x mid - cost - fee`). Spend is LLM spend
//!   (NewsAPI.ai is the free tier, so it costs nothing); `net_after_costs_cum_c = pnl_cum_c - spend in cents`.
//! - **score_hist**: scores stored in the last 24 h in 10-point buckets from 40 (empty when there are none).

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::config::Settings;
use crate::llm::Model;
use crate::news::newsapi;
use crate::run::{self, day_start, utc_date, Phase, DAY_S, KV_DEDUP_PREFIX, KV_HB_PREFIX};

const HOUR_S: i64 = 3600;

fn count(conn: &Connection, sql: &str, p: impl rusqlite::Params) -> rusqlite::Result<i64> {
    conn.query_row(sql, p, |r| r.get(0))
}

fn periodic_light(last: Option<i64>, errors_1h: i64, period: i64, now: i64) -> &'static str {
    match last {
        None => "down",
        Some(t) if now - t > 10 * period => "down",
        Some(t) if now - t > 3 * period || errors_1h > 0 => "warn",
        Some(_) => "ok",
    }
}

fn activity_light(ok_1h: i64, failed_1h: i64) -> &'static str {
    match (ok_1h, failed_1h) {
        (_, 0) => "ok",
        (0, _) => "down",
        _ => "warn",
    }
}

fn lights(
    conn: &Connection,
    settings: &Settings,
    phase: Phase,
    now: i64,
) -> rusqlite::Result<Value> {
    let hour = now - HOUR_S;
    let errors = |source: &str| {
        count(
            conn,
            "SELECT COUNT(*) FROM errors WHERE source = ?1 AND ts >= ?2",
            params![source, hour],
        )
    };
    let hb = |part: &str| run::kv_i64(conn, &format!("{KV_HB_PREFIX}{part}"));
    let searches = |source: &str| -> rusqlite::Result<&'static str> {
        let ok = count(
            conn,
            "SELECT COUNT(*) FROM searches WHERE source = ?1 AND ts >= ?2 AND ok = 1",
            params![source, hour],
        )?;
        let failed = count(
            conn,
            "SELECT COUNT(*) FROM searches WHERE source = ?1 AND ts >= ?2 AND ok = 0",
            params![source, hour],
        )?;
        Ok(activity_light(ok, failed))
    };
    let llm = |model: Model| -> rusqlite::Result<&'static str> {
        if settings.anthropic_key.is_none() && matches!(phase, Phase::Active { .. }) {
            return Ok("down");
        }
        let ok = count(
            conn,
            "SELECT COUNT(*) FROM llm_calls WHERE model_requested = ?1 AND ts >= ?2 AND status = 'ok'",
            params![model.id(), hour],
        )?;
        let failed = count(
            conn,
            "SELECT COUNT(*) FROM llm_calls WHERE model_requested = ?1 AND ts >= ?2 AND status != 'ok'",
            params![model.id(), hour],
        )?;
        Ok(activity_light(ok, failed))
    };
    let watching = phase.watching();
    let periodic = |part: &str, period: i64, on: bool| -> rusqlite::Result<&'static str> {
        Ok(if on {
            periodic_light(hb(part)?, errors(part)?, period, now)
        } else {
            "off"
        })
    };
    Ok(json!({
        "watcher": periodic("watcher", 60, watching)?,
        "scorer": periodic("scorer", 60, watching)?,
        "rss": searches("rss")?,
        "newsapi": if settings.newsapi_key.is_some() {
            searches("newsapi")?
        } else {
            "off"
        },
        "haiku": llm(Model::Haiku)?,
        "sonnet": llm(Model::Sonnet)?,
        "settlement": periodic("settlement", 600, phase != Phase::Finished)?,
    }))
}

fn today(conn: &Connection, now: i64) -> rusqlite::Result<Value> {
    let (from, to) = (day_start(now), day_start(now) + DAY_S);
    let by_status = |status: &str| {
        count(
            conn,
            "SELECT COUNT(*) FROM candidates WHERE created_ts >= ?1 AND created_ts < ?2 AND status = ?3",
            params![from, to, status],
        )
    };
    let dedup = run::kv_i64(conn, &format!("{KV_DEDUP_PREFIX}{}", utc_date(now)))?.unwrap_or(0);
    Ok(json!({
        "candidates": count(
            conn,
            "SELECT COUNT(*) FROM candidates WHERE created_ts >= ?1 AND created_ts < ?2",
            params![from, to],
        )?,
        "explained": by_status("explained")?,
        "unexplained": count(
            conn,
            "SELECT COUNT(DISTINCT c.id) FROM candidates c JOIN sonnet_reviews r ON r.candidate_id = c.id
             WHERE c.created_ts >= ?1 AND c.created_ts < ?2 AND r.branch = 'unexplained'",
            params![from, to],
        )?,
        "bought": by_status("bought")?,
        "no_buy": by_status("no_buy")?,
        "stale": by_status("stale")?,
        "failed": by_status("failed")?,
        "cap_skipped": by_status("cap_skipped")?,
        "dedup_suppressed": dedup,
    }))
}

/// Funnel counts for run-window candidates and trades whose event (creation, first Unexplained review,
/// trade opening) falls in `[from, to)`.
fn funnel(conn: &Connection, window: (i64, i64), from: i64, to: i64) -> rusqlite::Result<Value> {
    let (start, end) = window;
    Ok(json!({
        "math_pit": count(
            conn,
            "SELECT COUNT(*) FROM candidates WHERE created_ts >= ?1 AND created_ts < ?2
             AND created_ts >= ?3 AND created_ts < ?4",
            params![start, end, from, to],
        )?,
        "to_sonnet": count(
            conn,
            "SELECT COUNT(*) FROM (
                 SELECT r.candidate_id, MIN(r.ts) AS first_ts FROM sonnet_reviews r
                 JOIN candidates c ON c.id = r.candidate_id
                 WHERE r.branch = 'unexplained' AND c.created_ts >= ?1 AND c.created_ts < ?2
                 GROUP BY r.candidate_id)
             WHERE first_ts >= ?3 AND first_ts < ?4",
            params![start, end, from, to],
        )?,
        "paper_buys": count(
            conn,
            "SELECT COUNT(*) FROM paper_trades WHERE opened_ts >= ?1 AND opened_ts < ?2
             AND opened_ts >= ?3 AND opened_ts < ?4",
            params![start, end, from, to],
        )?,
    }))
}

/// Unrealised P&L (cents) of the positions open at `at`, marked at the last stored mid before `at`.
fn open_mtm_c(conn: &Connection, at: i64) -> rusqlite::Result<i64> {
    let mut stmt = conn.prepare(
        "SELECT p.side, p.contracts, p.cost_c, p.fee_c, p.avg_price_c,
                (SELECT s.mid_c FROM snapshots s WHERE s.ticker = p.ticker AND s.ts < ?1
                 AND s.mid_c IS NOT NULL ORDER BY s.ts DESC LIMIT 1)
         FROM paper_trades p LEFT JOIN settlements st ON st.trade_id = p.id
         WHERE p.opened_ts < ?1 AND (st.id IS NULL OR st.settled_ts >= ?1)",
    )?;
    let rows = stmt.query_map([at], |r| {
        let side: String = r.get(0)?;
        let contracts: i64 = r.get(1)?;
        let cost: i64 = r.get(2)?;
        let fee: i64 = r.get(3)?;
        let avg: f64 = r.get(4)?;
        let mid: Option<f64> = r.get(5)?;
        let price = match (mid, side.as_str()) {
            (Some(m), "no") => 100.0 - m,
            (Some(m), _) => m,
            (None, _) => avg,
        };
        Ok(contracts as f64 * price - cost as f64 - fee as f64)
    })?;
    let mut total = 0.0;
    for r in rows {
        total += r?;
    }
    Ok(total.round() as i64)
}

fn series(conn: &Connection, phase: Phase, now: i64) -> rusqlite::Result<Vec<Value>> {
    let Some((start, end)) = run::run_window(conn)? else {
        return Ok(Vec::new());
    };
    if now < start {
        return Ok(Vec::new());
    }
    let last = if phase == Phase::Finished {
        let last_settled: Option<i64> =
            conn.query_row("SELECT MAX(settled_ts) FROM settlements", [], |r| r.get(0))?;
        end.max(last_settled.unwrap_or(end))
    } else {
        now
    };
    let mut days = Vec::new();
    let mut d = day_start(start);
    while d <= last {
        let day_end = (d + DAY_S).min(last + 1);
        let f = funnel(conn, (start, end), d, d + DAY_S)?;
        let settled = run::settled_trades(conn, day_end)?;
        let v = run::verdict(&settled);
        // Voids are not wins or losses (ruling R21).
        let decided: Vec<_> = settled.iter().filter(|t| !t.void).collect();
        let wins = decided.iter().filter(|t| t.pnl_c > 0).count();
        let win_rate = (!decided.is_empty()).then(|| wins as f64 / decided.len() as f64);
        let spend = run::spend_between(conn, start, day_end)?;
        days.push(json!({
            "date": utc_date(d),
            "math_pit": f["math_pit"],
            "to_sonnet": f["to_sonnet"],
            "paper_buys": f["paper_buys"],
            "pnl_cum_c": v.pnl_c,
            "open_mtm_c": open_mtm_c(conn, day_end)?,
            "win_rate_cum": win_rate,
            "pnl_per_contract_cum_c": v.mean_c_per_contract,
            "ci_lo_c": v.ci_lo,
            "ci_hi_c": v.ci_hi,
            "spend_cum_usd": spend,
            "net_after_costs_cum_c": v.pnl_c - (spend * 100.0).round() as i64,
        }));
        d += DAY_S;
    }
    Ok(days)
}

fn score_hist(conn: &Connection, now: i64) -> rusqlite::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT MIN(CAST(score / 10 AS INTEGER), 9) * 10 AS b, COUNT(*) FROM scores
         WHERE ts > ?1 AND ts <= ?2 GROUP BY b ORDER BY b",
    )?;
    let found: Vec<(i64, i64)> = stmt
        .query_map(params![now - DAY_S, now], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    if found.is_empty() {
        return Ok(Vec::new());
    }
    Ok((4..10)
        .map(|b| {
            let n = found
                .iter()
                .find(|(k, _)| *k == b * 10)
                .map_or(0, |(_, n)| *n);
            let to = if b == 9 { 100 } else { b * 10 + 10 };
            json!({"from": b * 10, "to": to, "n": n})
        })
        .collect())
}

fn current_task(conn: &Connection, phase: Phase, paused: bool) -> rusqlite::Result<String> {
    let investigating: Option<(i64, String)> = conn
        .query_row(
            "SELECT id, event_ticker FROM candidates WHERE status = 'investigating' ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })?;
    if let Some((id, event)) = investigating {
        return Ok(format!("investigating candidate {id} ({event})"));
    }
    let markets = count(conn, "SELECT COUNT(*) FROM markets WHERE active = 1", [])?;
    let open = run::open_trades(conn)?;
    Ok(match phase {
        Phase::Active { .. } if paused => {
            format!("paused: watching {markets} markets, no new investigations")
        }
        Phase::NotStarted => format!("warm-up: watching {markets} markets (run not started)"),
        Phase::Active { .. } => format!("watching {markets} markets"),
        Phase::SettlementOnly => format!("settlement only: {open} open trades"),
        Phase::Finished => "finished".to_string(),
    })
}

/// Everything the tile shows, as of `now` (UTC unix seconds).
pub fn build(conn: &Connection, settings: &Settings, now: i64) -> Result<Value> {
    let phase = run::phase(conn, now)?;
    let window = run::run_window(conn)?;
    let paused = run::is_paused(conn, &settings.data_dir)?;
    let day = window
        .filter(|(s, _)| now >= *s)
        .map(|(s, _)| (now - s) / DAY_S + 1);
    let day_from = day_start(now);
    let funnel_total = match window {
        Some(w) => funnel(conn, w, i64::MIN, i64::MAX)?,
        None => json!({"math_pit": 0, "to_sonnet": 0, "paper_buys": 0}),
    };
    let verdict = run::load_verdict(conn, now)?;
    Ok(json!({
        "phase": phase.as_str(),
        "day": day,
        "run_start": window.map(|w| w.0),
        "run_end": window.map(|w| w.1),
        "paused": paused,
        "cap_hit": run::cap_hit(conn, settings, now)?,
        "lights": lights(conn, settings, phase, now)?,
        "today": today(conn, now)?,
        "trades": {
            "open": run::open_trades(conn)?,
            "settled": count(conn, "SELECT COUNT(*) FROM settlements", [])?,
            "voids": count(
                conn,
                "SELECT COUNT(*) FROM settlements WHERE result NOT IN ('yes', 'no')",
                [],
            )?,
            "pnl_c": count(conn, "SELECT COALESCE(SUM(pnl_c), 0) FROM settlements", [])?,
        },
        "verdict": serde_json::to_value(&verdict)?,
        "spend": {
            "today_usd": run::spend_today(conn, now)?,
            "run_usd": run::spend_run(conn, now)?,
            "day_cap": settings.daily_usd_cap,
            "run_cap": settings.run_usd_cap,
        },
        "newsapi": {
            "tokens_remaining": run::kv_i64(conn, "newsapi_tokens_remaining")?.unwrap_or(newsapi::START_TOKENS),
            "fallbacks_today": count(
                conn,
                "SELECT COUNT(*) FROM searches WHERE source = 'newsapi' AND ts >= ?1 AND ts < ?2",
                params![day_from, day_from + DAY_S],
            )?,
            "tokens_today": count(
                conn,
                "SELECT COALESCE(SUM(tokens_used), 0) FROM searches WHERE source = 'newsapi' AND ts >= ?1 AND ts < ?2",
                params![day_from, day_from + DAY_S],
            )?,
        },
        "funnel": funnel_total,
        "series": {"days": series(conn, phase, now)?},
        "score_hist": score_hist(conn, now)?,
        "current_task": current_task(conn, phase, paused)?,
        "updated_ts": now,
    }))
}

/// Build and write `<data_dir>/status.json` atomically (temp file, then rename).
pub fn write(conn: &Connection, settings: &Settings, now: i64) -> Result<()> {
    let value = build(conn, settings, now)?;
    write_to(&settings.data_dir, &value)
}

fn write_to(dir: &Path, value: &Value) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join("status.json.tmp");
    let path = dir.join("status.json");
    std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(())
}
