use std::path::Path;
use std::time::Duration;

use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::Client;
use informed_pit::watcher::{self, TickReport};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A UTC minute start (multiple of 60).
const T: i64 = 1_800_000_040 - 1_800_000_040 % 60;

fn fast_http(dir: &Path) -> Http {
    Http::with_spacing(EventLog::new(dir), Default::default())
        .with_backoff_base(Duration::from_millis(1))
}

fn rfc3339(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// A database with `tickers` flagged active.
fn db_with_active(dir: &Path, tickers: &[&str]) -> Connection {
    let conn = db::open(&dir.join("pit.db")).unwrap();
    for t in tickers {
        conn.execute(
            "INSERT INTO markets (ticker, event_ticker, first_seen_ts, last_seen_ts, active) VALUES (?1, 'EV', 0, 0, 1)",
            params![t],
        )
        .unwrap();
    }
    conn
}

fn market_json(
    ticker: &str,
    bid: Option<&str>,
    ask: Option<&str>,
    last: Option<&str>,
    volume: u32,
) -> Value {
    let mut m = json!({
        "ticker": ticker, "event_ticker": "EV", "title": "t", "yes_sub_title": "s",
        "close_time": rfc3339(T + 86_400), "status": "active", "result": "",
        "volume_fp": format!("{volume}.00"), "volume_24h_fp": "1.00", "open_interest_fp": "7.00"
    });
    for (k, v) in [
        ("yes_bid_dollars", bid),
        ("yes_ask_dollars", ask),
        ("last_price_dollars", last),
    ] {
        if let Some(v) = v {
            m[k] = json!(v);
        }
    }
    m
}

/// Serve `markets` once for the next `GET /markets` (earlier mounts are used up first).
async fn serve_markets_once(server: &MockServer, tickers: &str, markets: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("tickers", tickers))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"markets": markets, "cursor": ""})),
        )
        .up_to_n_times(1)
        .mount(server)
        .await;
}

fn trade_json(id: &str, ticker: &str, ts: i64, price: &str) -> Value {
    json!({"trade_id": id, "ticker": ticker, "created_time": rfc3339(ts), "taker_side": "yes",
           "yes_price_dollars": price, "count_fp": "3.00"})
}

/// Serve `trades` once for `GET /markets/trades` on `ticker` with the given `min_ts`.
async fn serve_trades_once(server: &MockServer, ticker: &str, min_ts: i64, trades: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path("/markets/trades"))
        .and(query_param("ticker", ticker))
        .and(query_param("min_ts", min_ts.to_string()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"trades": trades, "cursor": ""})),
        )
        .expect(1)
        .mount(server)
        .await;
}

async fn trade_request_count(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == "/markets/trades")
        .count()
}

fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

fn gap_events(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir.join("logs")).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        for line in text.lines() {
            let v: Value = serde_json::from_str(line).unwrap();
            if v["kind"] == "gap" {
                out.push(v);
            }
        }
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn writes_snapshot_per_market() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1", "B-1"]);
    // A third, inactive market must be ignored.
    conn.execute(
        "INSERT INTO markets (ticker, event_ticker, first_seen_ts, last_seen_ts, active) VALUES ('C-1', 'EV', 0, 0, 0)",
        [],
    )
    .unwrap();
    serve_markets_once(
        &server,
        "A-1,B-1",
        vec![
            market_json("A-1", Some("0.4000"), Some("0.4600"), Some("0.4300"), 10),
            market_json("B-1", Some("0.2000"), Some("0.2200"), None, 5),
        ],
    )
    .await;
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let log = EventLog::new(dir.path());

    let report = watcher::tick_with(&client, &conn, &log, T).await.unwrap();

    assert_eq!(
        report,
        TickReport {
            markets: 2,
            snapshots: 2,
            trade_rows: 0,
            gaps: 0
        }
    );
    let row = |ticker: &str| {
        conn.query_row(
            "SELECT ts, yes_bid_c, yes_ask_c, last_c, mid_c, volume, open_interest FROM snapshots WHERE ticker = ?1",
            params![ticker],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<f64>>(1)?,
                    r.get::<_, Option<f64>>(2)?,
                    r.get::<_, Option<f64>>(3)?,
                    r.get::<_, Option<f64>>(4)?,
                    r.get::<_, f64>(5)?,
                    r.get::<_, f64>(6)?,
                ))
            },
        )
        .unwrap()
    };
    assert_eq!(
        row("A-1"),
        (T, Some(40.0), Some(46.0), Some(43.0), Some(43.0), 10.0, 7.0)
    );
    assert_eq!(
        row("B-1"),
        (T, Some(20.0), Some(22.0), None, Some(21.0), 5.0, 7.0)
    );
    assert_eq!(count(&conn, "snapshots"), 2);
    // First sight of a market: no previous snapshot to compare, so no trade request.
    assert_eq!(trade_request_count(&server).await, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn fetches_trades_only_when_volume_rises() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1", "B-1"]);
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let log = EventLog::new(dir.path());
    let quote = |t: &str, v: u32| market_json(t, Some("0.4000"), Some("0.4400"), Some("0.4200"), v);

    // Minute 0: first snapshots, nothing to fetch.
    serve_markets_once(&server, "A-1,B-1", vec![quote("A-1", 10), quote("B-1", 10)]).await;
    watcher::tick_with(&client, &conn, &log, T).await.unwrap();
    assert_eq!(trade_request_count(&server).await, 0);

    // Minute 1: only A traded. No trade stored yet, so the fetch starts at the
    // previous snapshot's minute (inclusive).
    serve_markets_once(&server, "A-1,B-1", vec![quote("A-1", 15), quote("B-1", 10)]).await;
    serve_trades_once(
        &server,
        "A-1",
        T,
        vec![
            trade_json("t2", "A-1", T + 50, "0.4400"),
            trade_json("t1", "A-1", T + 30, "0.4200"),
        ],
    )
    .await;
    let report = watcher::tick_with(&client, &conn, &log, T + 60)
        .await
        .unwrap();
    assert_eq!(
        report,
        TickReport {
            markets: 2,
            snapshots: 2,
            trade_rows: 2,
            gaps: 0
        }
    );
    assert_eq!(trade_request_count(&server).await, 1);

    // Minute 2: volume unchanged everywhere, still no request.
    serve_markets_once(&server, "A-1,B-1", vec![quote("A-1", 15), quote("B-1", 10)]).await;
    let report = watcher::tick_with(&client, &conn, &log, T + 120)
        .await
        .unwrap();
    assert_eq!(report.trade_rows, 0);
    assert_eq!(trade_request_count(&server).await, 1);

    let (ticker, ts, price, n, side): (String, i64, f64, f64, String) = conn
        .query_row(
            "SELECT ticker, ts, yes_price_c, count, taker_side FROM trades WHERE trade_id = 't2'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        (ticker.as_str(), ts, price, n, side.as_str()),
        ("A-1", T + 50, 44.0, 3.0, "yes")
    );
    assert_eq!(count(&conn, "trades"), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn trade_dedup_by_id() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1"]);
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let log = EventLog::new(dir.path());
    let quote = |v: u32| market_json("A-1", Some("0.4000"), Some("0.4400"), Some("0.4200"), v);

    serve_markets_once(&server, "A-1", vec![quote(10)]).await;
    watcher::tick_with(&client, &conn, &log, T).await.unwrap();

    serve_markets_once(&server, "A-1", vec![quote(12)]).await;
    serve_trades_once(
        &server,
        "A-1",
        T,
        vec![trade_json("t1", "A-1", T + 40, "0.4200")],
    )
    .await;
    let r1 = watcher::tick_with(&client, &conn, &log, T + 60)
        .await
        .unwrap();
    assert_eq!(r1.trade_rows, 1);

    // The next fetch starts at the newest stored trade's second (T+40, inclusive). The API repeats
    // t1 and also returns t1b, which became visible later but carries the same second: t1 is not
    // duplicated, t1b (same-second late arrival) and t2 are captured.
    serve_markets_once(&server, "A-1", vec![quote(14)]).await;
    serve_trades_once(
        &server,
        "A-1",
        T + 40,
        vec![
            trade_json("t2", "A-1", T + 100, "0.4300"),
            trade_json("t1b", "A-1", T + 40, "0.4100"),
            trade_json("t1", "A-1", T + 40, "0.4200"),
        ],
    )
    .await;
    let r2 = watcher::tick_with(&client, &conn, &log, T + 120)
        .await
        .unwrap();
    assert_eq!(r2.trade_rows, 2);
    assert_eq!(count(&conn, "trades"), 3);
    let at_40: i64 = conn
        .query_row("SELECT COUNT(*) FROM trades WHERE ts = ?1", [T + 40], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(at_40, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn logs_gap_after_missing_minute() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1"]);
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let log = EventLog::new(dir.path());
    let quote = || market_json("A-1", Some("0.4000"), Some("0.4400"), Some("0.4200"), 10);

    serve_markets_once(&server, "A-1", vec![quote()]).await;
    let r0 = watcher::tick_with(&client, &conn, &log, T).await.unwrap();
    serve_markets_once(&server, "A-1", vec![quote()]).await;
    let r1 = watcher::tick_with(&client, &conn, &log, T + 60)
        .await
        .unwrap();
    assert_eq!((r0.gaps, r1.gaps), (0, 0));
    assert!(gap_events(dir.path()).is_empty());

    // Minutes T+120 and T+180 are missing.
    serve_markets_once(&server, "A-1", vec![quote()]).await;
    let r2 = watcher::tick_with(&client, &conn, &log, T + 240)
        .await
        .unwrap();
    assert_eq!(r2.gaps, 1);
    assert_eq!(r2.snapshots, 1);
    let events = gap_events(dir.path());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["payload"]["ticker"], "A-1");
    assert_eq!(events[0]["payload"]["missing_minutes"], 2);
    assert_eq!(events[0]["payload"]["previous_ts"], T + 60);
    assert_eq!(events[0]["payload"]["minute_ts"], T + 240);

    // Back on cadence: no further gap.
    serve_markets_once(&server, "A-1", vec![quote()]).await;
    let r3 = watcher::tick_with(&client, &conn, &log, T + 300)
        .await
        .unwrap();
    assert_eq!(r3.gaps, 0);
    assert_eq!(gap_events(dir.path()).len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn mid_falls_back_to_last_and_clamps() {
    // The pure function.
    assert_eq!(watcher::mid_c(Some(40), Some(45), Some(10)), Some(42.5));
    assert_eq!(watcher::mid_c(Some(40), None, Some(43)), Some(43.0));
    assert_eq!(watcher::mid_c(None, Some(45), Some(43)), Some(43.0));
    assert_eq!(watcher::mid_c(None, None, Some(100)), Some(99.0));
    assert_eq!(watcher::mid_c(None, None, Some(0)), Some(1.0));
    assert_eq!(watcher::mid_c(Some(40), None, None), None);
    assert_eq!(watcher::mid_c(None, None, None), None);

    // And as stored.
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1", "B-1", "C-1"]);
    serve_markets_once(
        &server,
        "A-1,B-1,C-1",
        vec![
            market_json("A-1", Some("0.4000"), Some("0.4500"), Some("0.1000"), 1),
            market_json("B-1", None, None, Some("1.0000"), 1),
            market_json("C-1", Some("0.3000"), None, None, 1),
        ],
    )
    .await;
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let log = EventLog::new(dir.path());
    watcher::tick_with(&client, &conn, &log, T).await.unwrap();

    let mid = |ticker: &str| -> Option<f64> {
        conn.query_row(
            "SELECT mid_c FROM snapshots WHERE ticker = ?1",
            params![ticker],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(mid("A-1"), Some(42.5));
    assert_eq!(mid("B-1"), Some(99.0));
    assert_eq!(mid("C-1"), None);
}

#[test]
fn prune_deletes_rows_older_than_60_days() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1"]);
    let now = T + 100 * 86_400;
    let cutoff = now - 60 * 86_400;
    for ts in [cutoff - 60, cutoff, cutoff + 60] {
        conn.execute(
            "INSERT INTO snapshots (ticker, ts, volume) VALUES ('A-1', ?1, 1)",
            params![ts],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO trades (trade_id, ticker, ts, yes_price_c, count) VALUES (?1, 'A-1', ?2, 40, 1)",
            params![format!("t{ts}"), ts],
        )
        .unwrap();
    }
    let (snaps, trades) = watcher::prune(&conn, now).unwrap();
    assert_eq!((snaps, trades), (1, 1));
    assert_eq!(count(&conn, "snapshots"), 2);
    assert_eq!(count(&conn, "trades"), 2);
    let oldest: i64 = conn
        .query_row("SELECT MIN(ts) FROM snapshots", [], |r| r.get(0))
        .unwrap();
    assert_eq!(oldest, cutoff);
}

#[tokio::test(flavor = "current_thread")]
async fn watcher_reads_store_no_raw_bodies() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let conn = db_with_active(dir.path(), &["A-1"]);
    serve_markets_once(
        &server,
        "A-1",
        vec![market_json(
            "A-1",
            Some("0.4000"),
            Some("0.4600"),
            Some("0.4300"),
            10,
        )],
    )
    .await;
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri()).without_raw_bodies();
    let log = EventLog::new(dir.path());

    let report = watcher::tick_with(&client, &conn, &log, T).await.unwrap();
    assert_eq!(report.snapshots, 1);
    assert!(
        !dir.path().join("raw").exists(),
        "no raw body stored for a watcher read"
    );
    // The request itself is still logged, with no raw path.
    let logs = std::fs::read_dir(dir.path().join("logs"))
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect::<String>();
    let http_ev: Value = logs
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .find(|v| v["kind"] == "http")
        .unwrap();
    assert!(http_ev["payload"]["raw_path"].is_null());
}
