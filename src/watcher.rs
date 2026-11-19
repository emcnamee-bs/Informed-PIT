//! Minute watcher: every minute, one price snapshot per active universe market, plus the trades
//! of any market whose cumulative volume rose since its previous snapshot. The surprise score
//! reads this history.
//!
//! `snapshots.ts` is the start of the UTC minute (a multiple of 60). A market whose previous
//! snapshot is not exactly one minute earlier logs a `gap` event.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;

use crate::db;
use crate::eventlog::EventLog;
use crate::http::Http;
use crate::kalshi::{Client, Market, Side};

/// Snapshots and trades older than this are deleted by [`prune`].
const RETENTION_SECS: i64 = 60 * 86_400;

/// What one [`tick`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TickReport {
    /// Active universe markets polled.
    pub markets: usize,
    /// Snapshot rows written (one per market the API returned, minus any already stored for this minute).
    pub snapshots: usize,
    /// New `trades` rows (duplicates by `trade_id` are not counted).
    pub trade_rows: usize,
    /// Markets whose previous snapshot was not exactly one minute earlier.
    pub gaps: usize,
}

/// Mid price in cents: `(bid + ask) / 2` when both sides quote, else the last trade price,
/// clamped to 1..=99. `None` when there is neither a two-sided quote nor a last trade.
pub fn mid_c(bid: Option<u32>, ask: Option<u32>, last: Option<u32>) -> Option<f64> {
    let mid = match (bid, ask) {
        (Some(b), Some(a)) => (f64::from(b) + f64::from(a)) / 2.0,
        _ => f64::from(last?),
    };
    Some(mid.clamp(1.0, 99.0))
}

/// The market's snapshot just before this minute.
struct Previous {
    ts: i64,
    volume: Option<f64>,
}

/// Run one watcher tick against the production API.
pub async fn tick(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    minute_ts: i64,
) -> Result<TickReport> {
    tick_with(
        &Client::new(http).without_raw_bodies(),
        conn,
        log,
        minute_ts,
    )
    .await
}

/// [`tick`] against an explicit client (tests point it at a mock server).
///
/// A failed market listing fails the tick (nothing is written). A failed trade fetch for one
/// market is logged and skipped: its snapshot is kept, and because the next fetch starts after
/// the newest *stored* trade, the missed trades are picked up the next time its volume rises.
pub async fn tick_with(
    client: &Client<'_>,
    conn: &Connection,
    log: &EventLog,
    minute_ts: i64,
) -> Result<TickReport> {
    let tickers = active_tickers(conn)?;
    let mut report = TickReport {
        markets: tickers.len(),
        ..TickReport::default()
    };
    if tickers.is_empty() {
        return Ok(report);
    }
    let markets = client
        .get_markets(&tickers)
        .await
        .context("watcher: market listing")?;

    // Read every previous snapshot before this minute's rows are written.
    let mut rows: Vec<(Market, Option<Previous>)> = Vec::with_capacity(markets.len());
    for m in markets {
        let prev = previous_snapshot(conn, &m.ticker, minute_ts)?;
        rows.push((m, prev));
    }

    report.snapshots = db::with_tx(conn, |tx| {
        let mut insert = tx.prepare(
            "INSERT OR IGNORE INTO snapshots (ticker, ts, yes_bid_c, yes_ask_c, last_c, mid_c, volume, open_interest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        let mut written = 0;
        for (m, _) in &rows {
            written += insert.execute(params![
                m.ticker,
                minute_ts,
                m.yes_bid_c,
                m.yes_ask_c,
                m.last_c,
                mid_c(m.yes_bid_c, m.yes_ask_c, m.last_c),
                m.volume as f64,
                m.open_interest as f64,
            ])?;
        }
        Ok(written)
    })?;

    for (m, prev) in &rows {
        let Some(prev) = prev else { continue };
        if prev.ts != minute_ts - 60 {
            report.gaps += 1;
            log.log(
                "gap",
                None,
                json!({
                    "ticker": m.ticker,
                    "minute_ts": minute_ts,
                    "previous_ts": prev.ts,
                    "missing_minutes": (minute_ts - prev.ts) / 60 - 1,
                }),
            );
        }
        if !prev.volume.is_some_and(|v| (m.volume as f64) > v) {
            continue;
        }
        // Newest stored trade, else the previous snapshot. The bound is inclusive: a trade in the same
        // second as the newest stored one may not have been visible yet; INSERT OR IGNORE drops repeats.
        let last_trade_ts = last_trade_ts(conn, &m.ticker)?.unwrap_or(prev.ts);
        match client.get_trades(&m.ticker, last_trade_ts).await {
            Ok(trades) => report.trade_rows += insert_trades(conn, &trades)?,
            Err(e) => {
                let message = format!("trade fetch for {}: {e:#}", m.ticker);
                log.log(
                    "watcher_error",
                    None,
                    json!({"ticker": m.ticker, "message": message}),
                );
                conn.execute(
                    "INSERT INTO errors (ts, source, message) VALUES (?1, 'watcher', ?2)",
                    params![minute_ts, message],
                )?;
            }
        }
    }

    log.log(
        "watcher_tick",
        None,
        json!({
            "minute_ts": minute_ts,
            "markets": report.markets,
            "snapshots": report.snapshots,
            "trade_rows": report.trade_rows,
            "gaps": report.gaps,
        }),
    );
    Ok(report)
}

/// Delete snapshots and trades older than 60 days. Returns `(snapshots deleted, trades deleted)`.
pub fn prune(conn: &Connection, now: i64) -> Result<(usize, usize)> {
    let cutoff = now - RETENTION_SECS;
    db::with_tx(conn, |tx| {
        let snapshots = tx.execute("DELETE FROM snapshots WHERE ts < ?1", params![cutoff])?;
        let trades = tx.execute("DELETE FROM trades WHERE ts < ?1", params![cutoff])?;
        Ok((snapshots, trades))
    })
}

fn active_tickers(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT ticker FROM markets WHERE active = 1 ORDER BY ticker")?;
    let tickers = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(tickers)
}

fn previous_snapshot(conn: &Connection, ticker: &str, before_ts: i64) -> Result<Option<Previous>> {
    Ok(conn
        .query_row(
            "SELECT ts, volume FROM snapshots WHERE ticker = ?1 AND ts < ?2 ORDER BY ts DESC LIMIT 1",
            params![ticker, before_ts],
            |r| {
                Ok(Previous {
                    ts: r.get(0)?,
                    volume: r.get(1)?,
                })
            },
        )
        .optional()?)
}

fn last_trade_ts(conn: &Connection, ticker: &str) -> Result<Option<i64>> {
    Ok(conn.query_row(
        "SELECT MAX(ts) FROM trades WHERE ticker = ?1",
        params![ticker],
        |r| r.get(0),
    )?)
}

/// Insert trades, ignoring ones already stored. Returns how many were new.
fn insert_trades(conn: &Connection, trades: &[crate::kalshi::Trade]) -> Result<usize> {
    db::with_tx(conn, |tx| {
        let mut insert = tx.prepare(
            "INSERT OR IGNORE INTO trades (trade_id, ticker, ts, yes_price_c, count, taker_side)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        let mut new = 0;
        for t in trades {
            new += insert.execute(params![
                t.trade_id,
                t.ticker,
                t.created_ts,
                f64::from(t.yes_price_c),
                t.count as f64,
                match t.taker_side {
                    Side::Yes => "yes",
                    Side::No => "no",
                },
            ])?;
        }
        Ok(new)
    })
}
