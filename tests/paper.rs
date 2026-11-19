use std::time::Duration;

use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::{taker_fee_cents, Client, OrderBook, Side};
use informed_pit::paper::{open_with, settle_due_with, walk_book};
use rusqlite::Connection;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// 2026-11-18 14:00:00 UTC.
const NOW: i64 = 1_795_010_400;

fn book(yes: &[(u32, i64)], no: &[(u32, i64)]) -> OrderBook {
    OrderBook {
        yes: yes.to_vec(),
        no: no.to_vec(),
    }
}

#[test]
fn walk_fills_25_across_levels() {
    // No bids 40/38/30 are yes asks 60/62/70.
    let b = book(&[], &[(40, 10), (38, 10), (30, 50)]);
    let f = walk_book(&b, Side::Yes, 25, 92);
    assert_eq!(f.contracts, 25);
    assert_eq!(f.levels, vec![(60, 10), (62, 10), (70, 5)]);
    assert_eq!(f.cost_c, 600 + 620 + 350);
    assert!((f.avg_price_c - 62.8).abs() < 1e-9);
}

#[test]
fn walk_stops_at_92c() {
    // Asks 90, 92, 93: the 93 level is above the limit.
    let b = book(&[], &[(10, 5), (8, 5), (7, 100)]);
    let f = walk_book(&b, Side::Yes, 25, 92);
    assert_eq!(f.contracts, 10);
    assert_eq!(f.levels, vec![(90, 5), (92, 5)]);
    assert_eq!(f.cost_c, 450 + 460);
}

#[test]
fn walk_partial_when_thin() {
    let b = book(&[], &[(20, 8), (5, 100)]);
    let f = walk_book(&b, Side::Yes, 25, 92);
    assert_eq!(f.contracts, 8);
    assert_eq!(f.cost_c, 640);
    assert!((f.avg_price_c - 80.0).abs() < 1e-9);
}

#[test]
fn walk_none_when_all_above_92() {
    let b = book(&[], &[(5, 10), (2, 10)]);
    let f = walk_book(&b, Side::Yes, 25, 92);
    assert_eq!(f.contracts, 0);
    assert_eq!(f.cost_c, 0);
    assert!(f.levels.is_empty());
}

#[test]
fn walk_sorts_unsorted_book_cheapest_first() {
    let b = book(&[], &[(30, 50), (40, 10)]);
    let f = walk_book(&b, Side::Yes, 5, 92);
    assert_eq!(f.levels, vec![(60, 5)]);
}

#[test]
fn buy_no_uses_yes_bids() {
    // Yes bids 60/55 are no asks 40/45.
    let b = book(&[(55, 20), (60, 10)], &[(1, 999)]);
    let f = walk_book(&b, Side::No, 25, 92);
    assert_eq!(f.contracts, 25);
    assert_eq!(f.levels, vec![(40, 10), (45, 15)]);
    assert_eq!(f.cost_c, 400 + 675);
}

// ---------------------------------------------------------------------------------------------
// open / settle_due against a mock Kalshi
// ---------------------------------------------------------------------------------------------

struct Fx {
    dir: TempDir,
    conn: Connection,
    http: Http,
    log: EventLog,
    server: MockServer,
}

async fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    for id in 1..=3 {
        conn.execute(
            "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
             VALUES (?1, 'EV', 'yes', ?2, 90, 'unexplained', 0, 0)",
            [id, NOW],
        )
        .unwrap();
    }
    let http = Http::new(EventLog::new(dir.path())).with_backoff_base(Duration::from_millis(1));
    let log = EventLog::new(dir.path());
    Fx {
        dir,
        conn,
        http,
        log,
        server: MockServer::start().await,
    }
}

fn iso(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

async fn mock_market(server: &MockServer, ticker: &str, event: &str, close_ts: i64, result: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/markets/{ticker}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"market": {
            "ticker": ticker, "event_ticker": event, "title": "t", "yes_sub_title": "s",
            "close_time": iso(close_ts), "status": if result.is_empty() { "active" } else { "finalized" },
            "result": result,
            "yes_bid_dollars": "0.4000", "yes_ask_dollars": "0.4500", "last_price_dollars": "0.4200",
            "volume_fp": "10.00", "volume_24h_fp": "5.00", "open_interest_fp": "7.00"
        }})))
        .mount(server)
        .await;
}

/// `no_bids` are `(cents, contracts)`; yes asks are 100 minus these.
async fn mock_book(server: &MockServer, ticker: &str, no_bids: &[(u32, i64)]) {
    let levels: Vec<serde_json::Value> = no_bids
        .iter()
        .map(|(p, q)| serde_json::json!([format!("{:.4}", *p as f64 / 100.0), format!("{q}.00")]))
        .collect();
    Mock::given(method("GET"))
        .and(path(format!("/markets/{ticker}/orderbook")))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"orderbook_fp": {"yes_dollars": [], "no_dollars": levels}}),
        ))
        .mount(server)
        .await;
}

async fn open_trade(
    fx: &Fx,
    candidate_id: i64,
    ticker: &str,
    side: Side,
) -> anyhow::Result<Option<i64>> {
    let client = Client::with_base(&fx.http, &fx.server.uri());
    open_with(&client, &fx.conn, &fx.log, candidate_id, ticker, side, NOW).await
}

fn log_text(fx: &Fx) -> String {
    let mut out = String::new();
    if let Ok(rd) = std::fs::read_dir(fx.dir.path().join("logs")) {
        for e in rd.flatten() {
            out.push_str(&std::fs::read_to_string(e.path()).unwrap());
        }
    }
    out
}

fn trade_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM paper_trades", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn open_records_fill_fee_and_book() {
    let fx = fx().await;
    mock_market(&fx.server, "M-1", "EV-A", NOW + 3600, "").await;
    mock_book(&fx.server, "M-1", &[(40, 10), (38, 10), (30, 50)]).await;
    let id = open_trade(&fx, 1, "M-1", Side::Yes)
        .await
        .unwrap()
        .expect("trade opened");
    let (event, side, n, cost, fee, avg, levels, raw, status, opened): (
        String,
        String,
        i64,
        i64,
        i64,
        f64,
        String,
        Option<String>,
        String,
        i64,
    ) = fx
        .conn
        .query_row(
            "SELECT event_ticker, side, contracts, cost_c, fee_c, avg_price_c, levels_json, book_raw_path, status, opened_ts
             FROM paper_trades WHERE id = ?1",
            [id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(event, "EV-A");
    assert_eq!(side, "yes");
    assert_eq!(n, 25);
    assert_eq!(cost, 1570);
    assert!((avg - 62.8).abs() < 1e-9);
    assert_eq!(fee, taker_fee_cents(25, 63));
    assert_eq!(levels, "[[60,10],[62,10],[70,5]]");
    assert_eq!(status, "open");
    assert_eq!(opened, NOW);
    let raw = raw.expect("book path stored");
    assert!(fx.dir.path().join(&raw).exists(), "book snapshot file");
}

#[tokio::test]
async fn second_trade_same_event_refused() {
    let fx = fx().await;
    mock_market(&fx.server, "M-1", "EV-A", NOW + 3600, "").await;
    mock_market(&fx.server, "M-2", "EV-A", NOW + 3600, "").await;
    mock_book(&fx.server, "M-1", &[(40, 30)]).await;
    mock_book(&fx.server, "M-2", &[(40, 30)]).await;
    assert!(open_trade(&fx, 1, "M-1", Side::Yes)
        .await
        .unwrap()
        .is_some());
    assert!(open_trade(&fx, 2, "M-2", Side::Yes)
        .await
        .unwrap()
        .is_none());
    assert_eq!(trade_count(&fx.conn), 1);
    assert!(log_text(&fx).contains("event_has_open_trade"));
}

#[tokio::test]
async fn refuses_when_closing_within_10_minutes() {
    let fx = fx().await;
    mock_market(&fx.server, "M-1", "EV-A", NOW + 599, "").await;
    mock_book(&fx.server, "M-1", &[(40, 30)]).await;
    assert!(open_trade(&fx, 1, "M-1", Side::Yes)
        .await
        .unwrap()
        .is_none());
    assert_eq!(trade_count(&fx.conn), 0);
    assert!(log_text(&fx).contains("closes_too_soon"));
}

#[tokio::test]
async fn refuses_when_book_unavailable() {
    let fx = fx().await;
    mock_market(&fx.server, "M-1", "EV-A", NOW + 3600, "").await;
    Mock::given(method("GET"))
        .and(path("/markets/M-1/orderbook"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&fx.server)
        .await;
    assert!(open_trade(&fx, 1, "M-1", Side::Yes)
        .await
        .unwrap()
        .is_none());
    assert_eq!(trade_count(&fx.conn), 0);
    assert!(log_text(&fx).contains("book_unavailable"));
}

#[tokio::test]
async fn refuses_when_no_liquidity_at_92c() {
    let fx = fx().await;
    mock_market(&fx.server, "M-1", "EV-A", NOW + 3600, "").await;
    mock_book(&fx.server, "M-1", &[(5, 100)]).await;
    assert!(open_trade(&fx, 1, "M-1", Side::Yes)
        .await
        .unwrap()
        .is_none());
    assert_eq!(trade_count(&fx.conn), 0);
    assert!(log_text(&fx).contains("no_liquidity"));
}

/// Open one trade per `(ticker, event, side)` at NOW, closing at NOW + 3600.
async fn open_all(fx: &Fx, trades: &[(&str, &str, Side)]) {
    for (i, (ticker, event, side)) in trades.iter().enumerate() {
        mock_market(&fx.server, ticker, event, NOW + 3600, "").await;
        // 40 bid on the opposite side => 60c ask for 25 contracts => cost 1500, fee 44.
        let book_body = serde_json::json!([["0.4000", "30.00"]]);
        let (yes, no) = match side {
            Side::Yes => (serde_json::json!([]), book_body),
            Side::No => (book_body, serde_json::json!([])),
        };
        Mock::given(method("GET"))
            .and(path(format!("/markets/{ticker}/orderbook")))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"orderbook_fp": {"yes_dollars": yes, "no_dollars": no}}),
            ))
            .mount(&fx.server)
            .await;
        let id = open_trade(fx, i as i64 + 1, ticker, *side)
            .await
            .unwrap()
            .expect("opened");
        assert_eq!(id, i as i64 + 1);
    }
}

async fn settle(fx: &Fx, server: &MockServer, now: i64) -> u32 {
    let client = Client::with_base(&fx.http, &server.uri());
    settle_due_with(&client, &fx.conn, &fx.log, now)
        .await
        .settled
}

fn settlement(conn: &Connection, trade_id: i64) -> (String, i64, i64, i64) {
    conn.query_row(
        "SELECT result, payout_c, pnl_c, settled_ts FROM settlements WHERE trade_id = ?1",
        [trade_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}

#[tokio::test]
async fn settle_win_and_loss_pnl() {
    let fx = fx().await;
    open_all(
        &fx,
        &[
            ("W-1", "EV-W", Side::Yes),
            ("L-1", "EV-L", Side::Yes),
            ("N-1", "EV-N", Side::No),
        ],
    )
    .await;
    let fee = taker_fee_cents(25, 60);
    assert_eq!(fee, 42);
    let settle_server = MockServer::start().await;
    let at = NOW + 3601;
    mock_market(&settle_server, "W-1", "EV-W", NOW + 3600, "yes").await;
    mock_market(&settle_server, "L-1", "EV-L", NOW + 3600, "no").await;
    mock_market(&settle_server, "N-1", "EV-N", NOW + 3600, "no").await;
    assert_eq!(settle(&fx, &settle_server, at).await, 3);
    // Cost 25 * 60 = 1500.
    assert_eq!(
        settlement(&fx.conn, 1),
        ("yes".to_string(), 2500, 2500 - 1500 - fee, at)
    );
    assert_eq!(
        settlement(&fx.conn, 2),
        ("no".to_string(), 0, -1500 - fee, at)
    );
    assert_eq!(
        settlement(&fx.conn, 3),
        ("no".to_string(), 2500, 2500 - 1500 - fee, at),
        "a no buy wins when the market resolves no"
    );
    let open: i64 = fx
        .conn
        .query_row(
            "SELECT COUNT(*) FROM paper_trades WHERE status = 'open'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(open, 0);
    // Idempotent: nothing left to settle, no second settlement rows.
    assert_eq!(settle(&fx, &settle_server, at + 60).await, 0);
    let n: i64 = fx
        .conn
        .query_row("SELECT COUNT(*) FROM settlements", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);
}

#[tokio::test]
async fn unsettled_market_left_open() {
    let fx = fx().await;
    open_all(&fx, &[("U-1", "EV-U", Side::Yes)]).await;
    // Not yet closed: no request is made at all (the mock server has no routes, so a request would 404).
    let empty = MockServer::start().await;
    assert_eq!(settle(&fx, &empty, NOW + 3599).await, 0);
    // Closed but Kalshi has not published a result yet.
    let server = MockServer::start().await;
    mock_market(&server, "U-1", "EV-U", NOW + 3600, "").await;
    assert_eq!(settle(&fx, &server, NOW + 3601).await, 0);
    let status: String = fx
        .conn
        .query_row("SELECT status FROM paper_trades WHERE id = 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "open");
    // The next pass picks it up once the result appears.
    let later = MockServer::start().await;
    mock_market(&later, "U-1", "EV-U", NOW + 3600, "yes").await;
    assert_eq!(settle(&fx, &later, NOW + 7200).await, 1);
}

#[tokio::test]
async fn void_result_refunds_cost_and_is_logged() {
    let fx = fx().await;
    open_all(&fx, &[("V-1", "EV-V", Side::Yes)]).await;
    let server = MockServer::start().await;
    mock_market(&server, "V-1", "EV-V", NOW + 3600, "void").await;
    assert_eq!(settle(&fx, &server, NOW + 3601).await, 1);
    assert_eq!(
        settlement(&fx.conn, 1),
        ("void".to_string(), 1500, 0, NOW + 3601)
    );
    assert!(log_text(&fx).contains("paper_void"));
}

#[tokio::test]
async fn failed_market_fetch_is_a_settlement_error() {
    let fx = fx().await;
    open_all(&fx, &[("F-1", "EV-F", Side::Yes)]).await;
    // Closed, but the market fetch fails (no route: 404).
    let broken = MockServer::start().await;
    let client = Client::with_base(&fx.http, &broken.uri());
    let pass = settle_due_with(&client, &fx.conn, &fx.log, NOW + 3601).await;
    assert_eq!((pass.settled, pass.failures), (0, 1));
    let errors: i64 = fx
        .conn
        .query_row(
            "SELECT COUNT(*) FROM errors WHERE source = 'settlement' AND candidate_id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(errors, 1);
}
