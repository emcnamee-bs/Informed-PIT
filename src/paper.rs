//! Paper ledger (spec section 9): a confirmed buy becomes a paper trade filled by walking the real order
//! book, held to settlement. PAPER ONLY: this module only ever reads public market data. All money is
//! integer cents.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::db;
use crate::eventlog::EventLog;
use crate::http::Http;
use crate::kalshi::{taker_fee_cents, Client, OrderBook, Side};

/// Contracts a paper trade tries to buy.
pub const TARGET_CONTRACTS: i64 = 25;
/// Highest price (cents) a level may have to be filled.
pub const MAX_PRICE_C: u32 = 92;
/// No buy when the market closes in less than this many seconds.
pub const MIN_SECS_TO_CLOSE: i64 = 10 * 60;

/// The result of walking one side of the book.
#[derive(Debug, Clone, PartialEq)]
pub struct Fill {
    pub contracts: i64,
    pub cost_c: i64,
    /// `cost_c / contracts`; 0 when nothing filled.
    pub avg_price_c: f64,
    /// `(price in cents, contracts taken at that price)`, cheapest first.
    pub levels: Vec<(u32, i64)>,
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Yes => "yes",
        Side::No => "no",
    }
}

/// Buy `side` as a taker: yes is bought from no bids (ask = 100 - bid) and no from yes bids. Takes the
/// cheapest levels first until `target` contracts are filled or the next price exceeds `max_price_c`.
pub fn walk_book(book: &OrderBook, side: Side, target: i64, max_price_c: u32) -> Fill {
    let bids = match side {
        Side::Yes => &book.no,
        Side::No => &book.yes,
    };
    // Best bid (highest) is the cheapest ask. Sort a copy so a mis-ordered book cannot overpay.
    let mut bids: Vec<(u32, i64)> = bids
        .iter()
        .copied()
        .filter(|&(bid, qty)| (1..100).contains(&bid) && qty > 0)
        .collect();
    bids.sort_by_key(|&(bid, _)| std::cmp::Reverse(bid));

    let mut levels = Vec::new();
    let (mut contracts, mut cost_c) = (0i64, 0i64);
    for (bid, qty) in bids {
        let price = 100 - bid;
        let want = target - contracts;
        if want <= 0 || price > max_price_c {
            break;
        }
        let take = qty.min(want);
        levels.push((price, take));
        contracts += take;
        cost_c += price as i64 * take;
    }
    Fill {
        contracts,
        cost_c,
        avg_price_c: if contracts > 0 {
            cost_c as f64 / contracts as f64
        } else {
            0.0
        },
        levels,
    }
}

fn refuse(
    log: &EventLog,
    candidate_id: i64,
    ticker: &str,
    side: Side,
    reason: &str,
    detail: Value,
) {
    log.log(
        "paper_refused",
        Some(candidate_id),
        json!({"reason": reason, "ticker": ticker, "side": side_str(side), "detail": detail}),
    );
}

fn book_json(book: &OrderBook) -> Value {
    json!({"yes": book.yes, "no": book.no})
}

/// Open a paper trade for `candidate_id` on `ticker`/`side` against the production API.
pub async fn open(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    candidate_id: i64,
    ticker: &str,
    side: Side,
    now: i64,
) -> Result<Option<i64>> {
    open_with(
        &Client::new(http),
        conn,
        log,
        candidate_id,
        ticker,
        side,
        now,
    )
    .await
}

/// [`open`] against an explicit client (tests point it at a mock server).
///
/// Returns the new trade id, or `Ok(None)` when the buy is skipped; every skip is logged as a
/// `paper_refused` event with its reason: `market_unavailable`, `event_has_open_trade`, `closes_too_soon`,
/// `book_unavailable` or `no_liquidity` (nothing offered at or below the price limit).
pub async fn open_with(
    client: &Client<'_>,
    conn: &Connection,
    log: &EventLog,
    candidate_id: i64,
    ticker: &str,
    side: Side,
    now: i64,
) -> Result<Option<i64>> {
    Ok(
        match open_outcome(client, conn, log, candidate_id, ticker, side, now).await? {
            OpenOutcome::Opened(id) => Some(id),
            OpenOutcome::Refused(_) => None,
        },
    )
}

/// What [`open_outcome`] did: opened a trade, or refused with one of the reasons listed on [`open_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenOutcome {
    Opened(i64),
    Refused(&'static str),
}

/// [`open_with`], also returning the refusal reason (the investigator records it on the candidate).
pub async fn open_outcome(
    client: &Client<'_>,
    conn: &Connection,
    log: &EventLog,
    candidate_id: i64,
    ticker: &str,
    side: Side,
    now: i64,
) -> Result<OpenOutcome> {
    let market = match client.get_market(ticker).await {
        Ok(m) => m,
        Err(e) => {
            refuse(
                log,
                candidate_id,
                ticker,
                side,
                "market_unavailable",
                json!(format!("{e:#}")),
            );
            return Ok(OpenOutcome::Refused("market_unavailable"));
        }
    };
    let open_in_event: i64 = conn.query_row(
        "SELECT COUNT(*) FROM paper_trades WHERE event_ticker = ?1 AND status = 'open'",
        [&market.event_ticker],
        |r| r.get(0),
    )?;
    if open_in_event > 0 {
        refuse(
            log,
            candidate_id,
            ticker,
            side,
            "event_has_open_trade",
            json!({"event_ticker": market.event_ticker}),
        );
        return Ok(OpenOutcome::Refused("event_has_open_trade"));
    }
    if market.close_ts - now < MIN_SECS_TO_CLOSE {
        refuse(
            log,
            candidate_id,
            ticker,
            side,
            "closes_too_soon",
            json!({"close_ts": market.close_ts, "now": now}),
        );
        return Ok(OpenOutcome::Refused("closes_too_soon"));
    }
    let book = match client.get_orderbook(ticker).await {
        Ok(b) => b,
        Err(e) => {
            refuse(
                log,
                candidate_id,
                ticker,
                side,
                "book_unavailable",
                json!(format!("{e:#}")),
            );
            return Ok(OpenOutcome::Refused("book_unavailable"));
        }
    };
    let fill = walk_book(&book, side, TARGET_CONTRACTS, MAX_PRICE_C);
    if fill.contracts == 0 {
        refuse(
            log,
            candidate_id,
            ticker,
            side,
            "no_liquidity",
            json!({"max_price_c": MAX_PRICE_C}),
        );
        return Ok(OpenOutcome::Refused("no_liquidity"));
    }
    let fee_c = taker_fee_cents(fill.contracts, fill.avg_price_c.round() as u32);
    let book_path = log.store_raw(
        &format!("book-{ticker}"),
        book_json(&book).to_string().as_bytes(),
    );
    let levels_json = serde_json::to_string(&fill.levels)?;
    let id = conn
        .execute(
            "INSERT INTO paper_trades
                 (candidate_id, event_ticker, ticker, side, contracts, cost_c, fee_c, avg_price_c,
                  levels_json, book_raw_path, close_ts, opened_ts, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'open')",
            params![
                candidate_id,
                market.event_ticker,
                ticker,
                side_str(side),
                fill.contracts,
                fill.cost_c,
                fee_c,
                fill.avg_price_c,
                levels_json,
                book_path,
                market.close_ts,
                now
            ],
        )
        .map(|_| conn.last_insert_rowid())
        .context("insert paper trade")?;
    log.log(
        "paper_opened",
        Some(candidate_id),
        json!({
            "trade_id": id, "ticker": ticker, "side": side_str(side), "contracts": fill.contracts,
            "cost_c": fill.cost_c, "fee_c": fee_c, "avg_price_c": fill.avg_price_c,
            "levels": fill.levels, "book": book_path,
        }),
    );
    Ok(OpenOutcome::Opened(id))
}

/// What one settlement pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SettlePass {
    /// Trades settled in this pass.
    pub settled: u32,
    /// Failures: the open-trade query, a market fetch, or a settlement write. Each one is also an `errors`
    /// row with source `settlement`.
    pub failures: u32,
}

/// Record a settlement failure: an `errors` row (source `settlement`) and a `paper_settle_error` log line.
fn settle_error(
    conn: &Connection,
    log: &EventLog,
    candidate_id: Option<i64>,
    detail: Value,
    now: i64,
) {
    let _ = conn.execute(
        "INSERT INTO errors (ts, source, candidate_id, message) VALUES (?1, 'settlement', ?2, ?3)",
        params![now, candidate_id, detail.to_string()],
    );
    log.log("paper_settle_error", candidate_id, detail);
}

/// Settle every open trade whose market has closed and published a result, against the production API.
pub async fn settle_due(http: &Http, conn: &Connection, log: &EventLog, now: i64) -> SettlePass {
    settle_due_with(&Client::new(http), conn, log, now).await
}

struct OpenTrade {
    id: i64,
    candidate_id: i64,
    ticker: String,
    side: String,
    contracts: i64,
    cost_c: i64,
    fee_c: i64,
}

fn due_trades(conn: &Connection, now: i64) -> rusqlite::Result<Vec<OpenTrade>> {
    let mut stmt = conn.prepare(
        "SELECT id, candidate_id, ticker, side, contracts, cost_c, fee_c FROM paper_trades
         WHERE status = 'open' AND close_ts < ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map([now], |r| {
        Ok(OpenTrade {
            id: r.get(0)?,
            candidate_id: r.get(1)?,
            ticker: r.get(2)?,
            side: r.get(3)?,
            contracts: r.get(4)?,
            cost_c: r.get(5)?,
            fee_c: r.get(6)?,
        })
    })?;
    rows.collect()
}

/// [`settle_due`] against an explicit client (tests point it at a mock server). Returns how many trades
/// were settled in this pass and how many steps failed.
///
/// A result of `yes`/`no` pays `contracts * 100` cents when it equals the trade's side, else 0, and
/// `pnl_c = payout_c - cost_c - fee_c`. Any other result (void, scalar) is recorded as that result with the
/// cost refunded (`payout_c = cost_c`, `pnl_c = 0`; whether Kalshi also refunds the fee is unknown) and a
/// `paper_void` event so it is visible. A market with no result yet, or one that cannot be fetched, is left
/// open and retried on the next pass. Settling is idempotent: a trade settles once.
pub async fn settle_due_with(
    client: &Client<'_>,
    conn: &Connection,
    log: &EventLog,
    now: i64,
) -> SettlePass {
    let mut pass = SettlePass::default();
    let trades = match due_trades(conn, now) {
        Ok(t) => t,
        Err(e) => {
            settle_error(conn, log, None, json!({"error": e.to_string()}), now);
            pass.failures += 1;
            return pass;
        }
    };
    for t in trades {
        let market = match client.get_market(&t.ticker).await {
            Ok(m) => m,
            Err(e) => {
                settle_error(
                    conn,
                    log,
                    Some(t.candidate_id),
                    json!({"trade_id": t.id, "ticker": t.ticker, "error": format!("{e:#}")}),
                    now,
                );
                pass.failures += 1;
                continue;
            }
        };
        let Some(result) = market.result else {
            continue;
        };
        let (payout_c, pnl_c) = match result.as_str() {
            "yes" | "no" => {
                let payout = if result == t.side {
                    t.contracts * 100
                } else {
                    0
                };
                (payout, payout - t.cost_c - t.fee_c)
            }
            _ => {
                log.log(
                    "paper_void",
                    Some(t.candidate_id),
                    json!({"trade_id": t.id, "ticker": t.ticker, "result": result, "refunded_c": t.cost_c}),
                );
                (t.cost_c, 0)
            }
        };
        let written = db::with_tx(conn, |tx| {
            tx.execute(
                "INSERT INTO settlements (trade_id, result, payout_c, pnl_c, settled_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![t.id, result, payout_c, pnl_c, now],
            )?;
            tx.execute(
                "UPDATE paper_trades SET status = 'settled' WHERE id = ?1",
                [t.id],
            )?;
            Ok(())
        });
        match written {
            Ok(()) => {
                pass.settled += 1;
                log.log(
                    "paper_settled",
                    Some(t.candidate_id),
                    json!({
                        "trade_id": t.id, "ticker": t.ticker, "side": t.side, "result": result,
                        "payout_c": payout_c, "pnl_c": pnl_c,
                    }),
                );
            }
            Err(e) => {
                settle_error(
                    conn,
                    log,
                    Some(t.candidate_id),
                    json!({"trade_id": t.id, "error": format!("{e:#}")}),
                    now,
                );
                pass.failures += 1;
            }
        }
    }
    pass
}
