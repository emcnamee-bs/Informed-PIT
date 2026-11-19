//! Kalshi public market-data client. Read-only: no authentication and no order or portfolio
//! endpoints are ever called. Every request goes through [`Http::send`] (spacing, retry, logging).
//!
//! Live API shape (captured 2026-11-18, fixtures in `tests/fixtures/kalshi/`): prices are dollar
//! strings (`yes_bid_dollars: "0.4200"`), counts are fixed-point strings (`volume_fp: "10.00"`,
//! `count_fp`), timestamps are RFC 3339 strings. The parsers also accept the older integer-cent
//! fields (`yes_bid: 42`, `volume: 10`) so a shape change back does not break us.

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::Value;

use crate::http::{CallCtx, Http, Service};

/// Production API base (public endpoints, no key).
pub const DEFAULT_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// `GET /markets` accepts at most 1000 per page and at most 100 tickers in `tickers=`.
const PAGE_LIMIT: u32 = 1000;
const TICKER_BATCH: usize = 100;
/// Hard stop on cursor paging so a misbehaving cursor cannot loop forever.
const MAX_PAGES: usize = 500;
const MAX_ATTEMPTS: u32 = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct Market {
    pub ticker: String,
    pub event_ticker: String,
    /// Series the event belongs to. The markets endpoint does not return it, so it is the part of
    /// `event_ticker` before the first `-` (matches `series_ticker` on the events endpoint).
    pub series_ticker: String,
    pub title: String,
    /// The yes-side subtitle (`yes_sub_title`).
    pub subtitle: String,
    pub rules_primary: String,
    /// Not on the markets endpoint; filled by the caller from [`Client::get_event_category`].
    pub category: Option<String>,
    /// Unix seconds of `close_time` (may be earlier than expiration for early-closing markets).
    pub close_ts: i64,
    /// Integer cents. Bid and ask are `Some` only for 1..=99 (Kalshi reports no bid as 0 and no ask as 100); `last_c` is `None` for 0.
    pub yes_bid_c: Option<u32>,
    pub yes_ask_c: Option<u32>,
    pub last_c: Option<u32>,
    /// Contracts (fixed-point API values rounded to whole contracts).
    pub volume: i64,
    pub volume_24h: i64,
    pub open_interest: i64,
    /// Status string as the API returns it (`active`, `closed`, `finalized`, ...).
    pub status: String,
    /// `Some("yes")` / `Some("no")` once settled; `None` while the API returns an empty result.
    pub result: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub trade_id: String,
    pub ticker: String,
    /// Yes-side price in integer cents.
    pub yes_price_c: u32,
    pub count: i64,
    pub taker_side: Side,
    pub created_ts: i64,
}

/// Resting bids as `(price in cents, whole contracts)`, best (highest) price first.
/// A yes ask at `p` is a no bid at `100 - p` and vice versa.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderBook {
    pub yes: Vec<(u32, i64)>,
    pub no: Vec<(u32, i64)>,
}

/// Kalshi taker fee in cents: `ceil(7 * count * p * (100 - p) / 10000)` with `p` in cents, which is
/// `ceil(0.07 * C * P * (1 - P))` dollars expressed in cents. Standard multiplier 1 only; a few
/// series carry a different `fee_multiplier` and are not modelled.
// UNVERIFIED against Kalshi's own fee schedule: kalshi.com/docs/kalshi-fee-schedule.pdf answers
// HTTP 429 (Vercel checkpoint) to scripted fetches and the help.kalshi.com fees article only links
// to it. Several secondary sources quote this same formula (round up to the next cent, no
// settlement fee), e.g. https://www.rotowire.com/prediction-markets/trading-fees.
pub fn taker_fee_cents(count: i64, price_c: u32) -> i64 {
    if count <= 0 || price_c == 0 || price_c >= 100 {
        return 0;
    }
    let p = price_c as i64;
    let numerator = 7 * count * p * (100 - p);
    (numerator + 9_999) / 10_000
}

// ---------------------------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------------------------

/// A JSON number or numeric string as f64.
fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Price in cents from a dollar field (preferred) or a legacy integer-cent field, clamped to 0..=100.
fn price_cents(dollars: &Option<Value>, cents: &Option<Value>) -> Option<u32> {
    let c = match (
        dollars.as_ref().and_then(as_f64),
        cents.as_ref().and_then(as_f64),
    ) {
        (Some(d), _) => d * 100.0,
        (None, Some(c)) => c,
        (None, None) => return None,
    };
    Some(c.round().clamp(0.0, 100.0) as u32)
}

/// Bid or ask in cents. Kalshi reports a missing bid as 0 and a missing ask as 1.00 (no no-side
/// bids to cross with), so only 1..=99 is a real quote; anything else is `None`.
fn quote_cents(dollars: &Option<Value>, cents: &Option<Value>) -> Option<u32> {
    price_cents(dollars, cents).filter(|c| (1..=99).contains(c))
}

/// Last traded price: zero means "no trade yet".
fn last_cents(dollars: &Option<Value>, cents: &Option<Value>) -> Option<u32> {
    price_cents(dollars, cents).filter(|&c| c > 0)
}

/// Whole-contract count from a fixed-point field (preferred) or a legacy integer field.
fn count_of(fp: &Option<Value>, int: &Option<Value>) -> i64 {
    fp.as_ref()
        .and_then(as_f64)
        .or_else(|| int.as_ref().and_then(as_f64))
        .map_or(0, |c| c.round() as i64)
}

fn rfc3339_ts(s: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("bad timestamp {s:?}"))?
        .timestamp())
}

#[derive(Deserialize)]
struct RawMarket {
    ticker: String,
    #[serde(default)]
    event_ticker: String,
    series_ticker: Option<String>,
    #[serde(default)]
    title: String,
    yes_sub_title: Option<String>,
    subtitle: Option<String>,
    rules_primary: Option<String>,
    category: Option<String>,
    close_time: Option<String>,
    #[serde(default)]
    status: String,
    result: Option<String>,
    yes_bid_dollars: Option<Value>,
    yes_bid: Option<Value>,
    yes_ask_dollars: Option<Value>,
    yes_ask: Option<Value>,
    last_price_dollars: Option<Value>,
    last_price: Option<Value>,
    volume_fp: Option<Value>,
    volume: Option<Value>,
    volume_24h_fp: Option<Value>,
    volume_24h: Option<Value>,
    open_interest_fp: Option<Value>,
    open_interest: Option<Value>,
}

impl RawMarket {
    fn into_market(self) -> Result<Market> {
        let close_ts = match self.close_time.as_deref() {
            Some(s) if !s.is_empty() => {
                rfc3339_ts(s).with_context(|| format!("market {}", self.ticker))?
            }
            _ => bail!("market {} has no close_time", self.ticker),
        };
        let series_ticker = self
            .series_ticker
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                self.event_ticker
                    .split('-')
                    .next()
                    .unwrap_or("")
                    .to_string()
            });
        Ok(Market {
            yes_bid_c: quote_cents(&self.yes_bid_dollars, &self.yes_bid),
            yes_ask_c: quote_cents(&self.yes_ask_dollars, &self.yes_ask),
            last_c: last_cents(&self.last_price_dollars, &self.last_price),
            volume: count_of(&self.volume_fp, &self.volume),
            volume_24h: count_of(&self.volume_24h_fp, &self.volume_24h),
            open_interest: count_of(&self.open_interest_fp, &self.open_interest),
            series_ticker,
            subtitle: self.yes_sub_title.or(self.subtitle).unwrap_or_default(),
            rules_primary: self.rules_primary.unwrap_or_default(),
            category: self.category.filter(|c| !c.is_empty()),
            result: self.result.filter(|r| !r.is_empty()),
            ticker: self.ticker,
            event_ticker: self.event_ticker,
            title: self.title,
            close_ts,
            status: self.status,
        })
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<RawMarket>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct MarketEnvelope {
    market: RawMarket,
}

fn non_empty(cursor: Option<String>) -> Option<String> {
    cursor.filter(|c| !c.is_empty())
}

/// Parse a `GET /markets` body into markets plus the next-page cursor (`None` on the last page).
pub fn parse_markets_page(body: &[u8]) -> Result<(Vec<Market>, Option<String>)> {
    let page: MarketsPage = serde_json::from_slice(body).context("markets page")?;
    let markets = page
        .markets
        .into_iter()
        .map(RawMarket::into_market)
        .collect::<Result<Vec<_>>>()?;
    Ok((markets, non_empty(page.cursor)))
}

/// Parse a `GET /markets/{ticker}` body.
pub fn parse_market(body: &[u8]) -> Result<Market> {
    serde_json::from_slice::<MarketEnvelope>(body)
        .context("market")?
        .market
        .into_market()
}

#[derive(Deserialize)]
struct RawTrade {
    trade_id: String,
    ticker: String,
    created_time: String,
    taker_side: String,
    yes_price_dollars: Option<Value>,
    yes_price: Option<Value>,
    count_fp: Option<Value>,
    count: Option<Value>,
}

#[derive(Deserialize)]
struct TradesPage {
    #[serde(default)]
    trades: Vec<RawTrade>,
    cursor: Option<String>,
}

fn parse_side(s: &str) -> Result<Side> {
    match s {
        "yes" => Ok(Side::Yes),
        "no" => Ok(Side::No),
        other => Err(anyhow!("unknown taker_side {other:?}")),
    }
}

/// Parse a `GET /markets/trades` body into trades plus the next-page cursor.
pub fn parse_trades_page(body: &[u8]) -> Result<(Vec<Trade>, Option<String>)> {
    let page: TradesPage = serde_json::from_slice(body).context("trades page")?;
    let trades = page
        .trades
        .into_iter()
        .map(|t| {
            Ok(Trade {
                yes_price_c: price_cents(&t.yes_price_dollars, &t.yes_price)
                    .ok_or_else(|| anyhow!("trade {} has no yes price", t.trade_id))?,
                count: count_of(&t.count_fp, &t.count),
                taker_side: parse_side(&t.taker_side)
                    .with_context(|| format!("trade {}", t.trade_id))?,
                created_ts: rfc3339_ts(&t.created_time)
                    .with_context(|| format!("trade {}", t.trade_id))?,
                trade_id: t.trade_id,
                ticker: t.ticker,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((trades, non_empty(page.cursor)))
}

#[derive(Deserialize)]
struct RawBook {
    yes_dollars: Option<Vec<(Value, Value)>>,
    no_dollars: Option<Vec<(Value, Value)>>,
    yes: Option<Vec<(Value, Value)>>,
    no: Option<Vec<(Value, Value)>>,
}

#[derive(Deserialize)]
struct BookEnvelope {
    orderbook_fp: Option<RawBook>,
    orderbook: Option<RawBook>,
}

/// Levels as `(cents, whole contracts)`, best price first. Fractional contracts are floored (a
/// paper fill must be a real fill), levels outside 1..=99 cents or with no whole contract are
/// dropped, and levels that round to the same cent are merged.
fn book_side(
    dollars: Option<Vec<(Value, Value)>>,
    cents: Option<Vec<(Value, Value)>>,
) -> Result<Vec<(u32, i64)>> {
    let (levels, scale) = match (dollars, cents) {
        (Some(d), _) => (d, 100.0),
        (None, Some(c)) => (c, 1.0),
        (None, None) => return Ok(Vec::new()),
    };
    let mut out: Vec<(u32, i64)> = Vec::new();
    for (p, q) in levels {
        let price = as_f64(&p).ok_or_else(|| anyhow!("bad orderbook price {p}"))? * scale;
        let qty = as_f64(&q)
            .ok_or_else(|| anyhow!("bad orderbook quantity {q}"))?
            .floor() as i64;
        let price = price.round() as i64;
        if !(1..=99).contains(&price) || qty <= 0 {
            continue;
        }
        match out.iter_mut().find(|(c, _)| *c == price as u32) {
            Some(level) => level.1 += qty,
            None => out.push((price as u32, qty)),
        }
    }
    out.sort_by_key(|level| std::cmp::Reverse(level.0));
    Ok(out)
}

/// Parse a `GET /markets/{ticker}/orderbook` body. The API lists levels worst-to-best; the result
/// is sorted best-first.
pub fn parse_orderbook(body: &[u8]) -> Result<OrderBook> {
    let env: BookEnvelope = serde_json::from_slice(body).context("orderbook")?;
    let raw = env
        .orderbook_fp
        .or(env.orderbook)
        .ok_or_else(|| anyhow!("orderbook body has no orderbook"))?;
    Ok(OrderBook {
        yes: book_side(raw.yes_dollars, raw.yes)?,
        no: book_side(raw.no_dollars, raw.no)?,
    })
}

/// Category of the event in a `GET /events/{event_ticker}` body (`None` if absent or empty).
pub fn parse_event_category(body: &[u8]) -> Result<Option<String>> {
    let v: Value = serde_json::from_slice(body).context("event")?;
    let event = v
        .get("event")
        .ok_or_else(|| anyhow!("event body has no event"))?;
    Ok(event
        .get("category")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
        .map(str::to_string))
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

/// Kalshi client over the shared [`Http`] layer. The free functions below use [`DEFAULT_BASE`];
/// tests build a client with [`Client::with_base`] to point at a mock server.
pub struct Client<'a> {
    http: &'a Http,
    base: String,
    /// Keep response bodies under `raw/` (see [`Client::without_raw_bodies`]).
    store_raw: bool,
}

impl<'a> Client<'a> {
    pub fn new(http: &'a Http) -> Client<'a> {
        Client::with_base(http, DEFAULT_BASE)
    }

    pub fn with_base(http: &'a Http, base: &str) -> Client<'a> {
        Client {
            http,
            base: base.trim_end_matches('/').to_string(),
            store_raw: true,
        }
    }

    /// Do not store response bodies under `raw/` (the watcher's every-minute reads: their content goes to
    /// `snapshots` and `trades`, and the raw copies would fill the disk).
    pub fn without_raw_bodies(mut self) -> Client<'a> {
        self.store_raw = false;
        self
    }

    /// GET `path` with `params`. Returns the status and body; callers decide what a non-200 means.
    async fn get(
        &self,
        label: &'static str,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<(u16, bytes::Bytes)> {
        let req = self
            .http
            .client()
            .get(format!("{}{}", self.base, path))
            .query(params);
        let ctx = CallCtx {
            label,
            candidate_id: None,
            max_attempts: MAX_ATTEMPTS,
            store_raw: self.store_raw,
        };
        let res = self
            .http
            .send(Service::Kalshi, req, ctx)
            .await
            .with_context(|| format!("kalshi {label}"))?;
        Ok((res.status, res.body))
    }

    /// Like [`Client::get`] but any status other than 200 is an error.
    async fn get_ok(
        &self,
        label: &'static str,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<bytes::Bytes> {
        let (status, body) = self.get(label, path, params).await?;
        if status != 200 {
            let snippet: String = String::from_utf8_lossy(&body).chars().take(200).collect();
            bail!("kalshi {label}: HTTP {status}: {snippet}");
        }
        Ok(body)
    }

    /// All open markets, following the cursor. Multivariate (parlay) markets are excluded with
    /// `mve_filter=exclude`: they have no meaningful two-sided liquidity and dominate the listing.
    pub async fn list_open_markets(&self) -> Result<Vec<Market>> {
        self.list_open_markets_closing(None, None).await
    }

    /// [`Client::list_open_markets`] limited server-side to markets whose `close_time` is at or
    /// after `min_close_ts` and at or before `max_close_ts` (unix seconds). Verified live
    /// 2026-11-18 together with `status=open`: it returns the same markets a client-side filter
    /// on the full listing keeps (about 73k of 189k), in a third of the pages.
    pub async fn list_open_markets_closing(
        &self,
        min_close_ts: Option<i64>,
        max_close_ts: Option<i64>,
    ) -> Result<Vec<Market>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut params = vec![
                ("status", "open".to_string()),
                ("limit", PAGE_LIMIT.to_string()),
                ("mve_filter", "exclude".to_string()),
            ];
            if let Some(t) = min_close_ts {
                params.push(("min_close_ts", t.to_string()));
            }
            if let Some(t) = max_close_ts {
                params.push(("max_close_ts", t.to_string()));
            }
            if let Some(c) = &cursor {
                params.push(("cursor", c.clone()));
            }
            let body = self.get_ok("kalshi_markets", "/markets", &params).await?;
            let (mut markets, next) = parse_markets_page(&body)?;
            out.append(&mut markets);
            match next {
                Some(c) => cursor = Some(c),
                None => return Ok(out),
            }
        }
        bail!("kalshi markets: still paging after {MAX_PAGES} pages")
    }

    /// Markets by ticker, in batches of 100. Tickers the API does not know are simply missing.
    pub async fn get_markets(&self, tickers: &[String]) -> Result<Vec<Market>> {
        let mut out = Vec::new();
        for batch in tickers.chunks(TICKER_BATCH) {
            let params = [
                ("tickers", batch.join(",")),
                ("limit", TICKER_BATCH.to_string()),
            ];
            let body = self
                .get_ok("kalshi_markets_batch", "/markets", &params)
                .await?;
            out.append(&mut parse_markets_page(&body)?.0);
        }
        Ok(out)
    }

    /// One market. An unknown ticker (HTTP 404) is an error.
    pub async fn get_market(&self, ticker: &str) -> Result<Market> {
        let body = self
            .get_ok("kalshi_market", &format!("/markets/{ticker}"), &[])
            .await?;
        parse_market(&body)
    }

    /// Trades on `ticker` at or after `min_ts` (unix seconds), newest first as the API returns them.
    pub async fn get_trades(&self, ticker: &str, min_ts: i64) -> Result<Vec<Trade>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut params = vec![
                ("ticker", ticker.to_string()),
                ("limit", PAGE_LIMIT.to_string()),
                ("min_ts", min_ts.to_string()),
            ];
            if let Some(c) = &cursor {
                params.push(("cursor", c.clone()));
            }
            let body = self
                .get_ok("kalshi_trades", "/markets/trades", &params)
                .await?;
            let (mut trades, next) = parse_trades_page(&body)?;
            out.append(&mut trades);
            match next {
                Some(c) => cursor = Some(c),
                None => return Ok(out),
            }
        }
        bail!("kalshi trades {ticker}: still paging after {MAX_PAGES} pages")
    }

    /// Whether `ticker` has any trade at or after `min_ts`: one request for one trade.
    pub async fn has_trade_since(&self, ticker: &str, min_ts: i64) -> Result<bool> {
        let params = [
            ("ticker", ticker.to_string()),
            ("limit", "1".to_string()),
            ("min_ts", min_ts.to_string()),
        ];
        let body = self
            .get_ok("kalshi_trades", "/markets/trades", &params)
            .await?;
        Ok(!parse_trades_page(&body)?.0.is_empty())
    }

    pub async fn get_orderbook(&self, ticker: &str) -> Result<OrderBook> {
        let body = self
            .get_ok(
                "kalshi_orderbook",
                &format!("/markets/{ticker}/orderbook"),
                &[],
            )
            .await?;
        parse_orderbook(&body)
    }

    /// Category of an event, cached forever in `kv` under `event_category:<event_ticker>` (an
    /// event's category does not change). Unknown events (404) and events without a category
    /// return `None` and are not cached.
    pub async fn get_event_category(
        &self,
        conn: &Connection,
        event_ticker: &str,
    ) -> Result<Option<String>> {
        let key = format!("event_category:{event_ticker}");
        let cached: Option<String> = conn
            .query_row("SELECT value FROM kv WHERE key = ?1", [&key], |r| r.get(0))
            .optional()?;
        if cached.is_some() {
            return Ok(cached);
        }
        let (status, body) = self
            .get("kalshi_event", &format!("/events/{event_ticker}"), &[])
            .await?;
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            bail!("kalshi event {event_ticker}: HTTP {status}");
        }
        let category = parse_event_category(&body)?;
        if let Some(c) = &category {
            conn.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                [&key, c],
            )?;
        }
        Ok(category)
    }
}

// Production entry points (default base URL).

pub async fn list_open_markets(http: &Http) -> Result<Vec<Market>> {
    Client::new(http).list_open_markets().await
}

pub async fn get_markets(http: &Http, tickers: &[String]) -> Result<Vec<Market>> {
    Client::new(http).get_markets(tickers).await
}

pub async fn get_market(http: &Http, ticker: &str) -> Result<Market> {
    Client::new(http).get_market(ticker).await
}

pub async fn get_trades(http: &Http, ticker: &str, min_ts: i64) -> Result<Vec<Trade>> {
    Client::new(http).get_trades(ticker, min_ts).await
}

pub async fn get_orderbook(http: &Http, ticker: &str) -> Result<OrderBook> {
    Client::new(http).get_orderbook(ticker).await
}

pub async fn get_event_category(
    http: &Http,
    conn: &Connection,
    event_ticker: &str,
) -> Result<Option<String>> {
    Client::new(http)
        .get_event_category(conn, event_ticker)
        .await
}
