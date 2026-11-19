use std::time::Duration;

use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::{self, taker_fee_cents, Client, Side};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/kalshi/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn fixture_str(name: &str) -> String {
    String::from_utf8(fixture(name)).unwrap()
}

fn fast_http(dir: &std::path::Path) -> Http {
    Http::with_spacing(EventLog::new(dir), Default::default())
        .with_backoff_base(Duration::from_millis(1))
}

fn market_json(ticker: &str) -> serde_json::Value {
    serde_json::json!({
        "ticker": ticker, "event_ticker": "EV-1", "title": "t", "yes_sub_title": "s",
        "close_time": "2026-12-01T00:00:00Z", "status": "active", "result": "",
        "yes_bid_dollars": "0.4200", "yes_ask_dollars": "0.4500", "last_price_dollars": "0.4300",
        "volume_fp": "10.00", "volume_24h_fp": "5.00", "open_interest_fp": "7.00"
    })
}

#[test]
fn parses_live_markets_fixture() {
    let (markets, _cursor) = kalshi::parse_markets_page(&fixture("markets.json")).unwrap();
    assert!(!markets.is_empty());
    for m in &markets {
        for p in [m.yes_bid_c, m.yes_ask_c, m.last_c].into_iter().flatten() {
            assert!(p <= 100, "{} price {p} out of range", m.ticker);
        }
        assert!(m.close_ts > 0, "{} close_ts", m.ticker);
        assert!(!m.ticker.is_empty() && !m.event_ticker.is_empty() && !m.series_ticker.is_empty());
    }
    let liquid = markets
        .iter()
        .find(|m| m.ticker == "KXBRASILEIROTOTAL-26NOV18CRCHA-7")
        .unwrap();
    assert_eq!(liquid.event_ticker, "KXBRASILEIROTOTAL-26NOV18CRCHA");
    assert_eq!(liquid.series_ticker, "KXBRASILEIROTOTAL");
    assert_eq!(liquid.yes_bid_c, Some(5));
    assert_eq!(liquid.yes_ask_c, Some(6));
    assert_eq!(liquid.last_c, Some(4));
    assert_eq!(liquid.volume, 3709);
    assert!(liquid.volume_24h > 0 && liquid.open_interest >= 0);
    assert_eq!(liquid.result, None);
    assert_eq!(liquid.status, "active");
    assert_eq!(
        liquid.category, None,
        "category only comes from the events endpoint"
    );
    // A market nobody has traded yet: a zero last price means "no last trade", not a 0c print.
    let untraded = markets.iter().find(|m| m.volume == 0).unwrap();
    assert_eq!(untraded.last_c, None);
}

#[test]
fn parses_trades_fixture_taker_side() {
    let (trades, cursor) = kalshi::parse_trades_page(&fixture("trades.json")).unwrap();
    assert!(trades.len() >= 5);
    assert!(cursor.is_some());
    for t in &trades {
        assert!((1..=99).contains(&t.yes_price_c));
        assert!(t.count > 0);
        assert!(t.created_ts > 1_700_000_000);
        assert_eq!(t.ticker, "KXBRASILEIROTOTAL-26NOV18CRCHA-7");
        assert!(!t.trade_id.is_empty());
    }
    // First fixture trade: taker bought NO at 95c, i.e. yes traded at 5c.
    assert_eq!(trades[0].taker_side, Side::No);
    assert_eq!(trades[0].yes_price_c, 5);
    assert_eq!(trades[0].count, 100);
}

#[test]
fn parses_orderbook_fixture_sorted_desc() {
    let book = kalshi::parse_orderbook(&fixture("orderbook.json")).unwrap();
    assert!(!book.yes.is_empty() && !book.no.is_empty());
    for side in [&book.yes, &book.no] {
        assert!(
            side.windows(2).all(|w| w[0].0 > w[1].0),
            "bids must be strictly descending by price"
        );
        assert!(side.iter().all(|&(p, q)| (1..=99).contains(&p) && q > 0));
    }
    // Fixture: best yes bid is 4c (levels 1c..4c), best no bid is 95c.
    assert_eq!(book.yes[0], (4, 2804));
    assert_eq!(book.no[0], (95, 920));
}

#[test]
fn parses_settled_result_yes_or_no() {
    let (markets, _) = kalshi::parse_markets_page(&fixture("settled_market.json")).unwrap();
    assert_eq!(markets.len(), 1);
    let r = markets[0].result.as_deref();
    assert!(r == Some("yes") || r == Some("no"), "result was {r:?}");
}

#[test]
fn parses_event_category() {
    assert_eq!(
        kalshi::parse_event_category(&fixture("events.json")).unwrap(),
        Some("Sports".to_string())
    );
}

#[test]
fn accepts_legacy_integer_cent_fields() {
    let body = br#"{"markets":[{"ticker":"T-1","event_ticker":"E-1","title":"t","close_time":"2026-12-01T00:00:00Z",
        "yes_bid":42,"yes_ask":45,"last_price":43,"volume":10,"volume_24h":5,"open_interest":7,"status":"open","result":""}]}"#;
    let (m, cursor) = kalshi::parse_markets_page(body).unwrap();
    assert_eq!(cursor, None);
    assert_eq!(
        (m[0].yes_bid_c, m[0].yes_ask_c, m[0].last_c),
        (Some(42), Some(45), Some(43))
    );
    assert_eq!(
        (m[0].volume, m[0].volume_24h, m[0].open_interest),
        (10, 5, 7)
    );
    assert_eq!(m[0].result, None);
}

#[test]
fn one_sided_book_has_no_ask() {
    // Live fixture: yes_bid "0.0100", yes_ask "1.0000" (no offer), volume 0.
    let (markets, _) = kalshi::parse_markets_page(&fixture("markets.json")).unwrap();
    let m = markets
        .iter()
        .find(|m| m.ticker == "KXNBASTOCKPROT-26NOV18BOSDEN-BOSPGEORGE13-4")
        .unwrap();
    assert_eq!(m.yes_bid_c, Some(1));
    assert_eq!(m.yes_ask_c, None);
}

#[test]
fn fee_table_cases() {
    // ceil(7 * C * p * (100 - p) / 10000)
    assert_eq!(taker_fee_cents(10, 30), 15); // 14.7c rounds up (the review note said 2; 0.07*10*0.3*0.7 = $0.147)
    assert_eq!(taker_fee_cents(100, 50), 175);
    assert_eq!(taker_fee_cents(100, 10), 63);
    assert_eq!(taker_fee_cents(100, 90), 63);
    assert_eq!(taker_fee_cents(1, 50), 2);
}

#[test]
fn fee_25_at_50c_is_44() {
    assert_eq!(taker_fee_cents(25, 50), 44);
}

#[test]
fn fee_1_at_92c_is_1() {
    assert_eq!(taker_fee_cents(1, 92), 1);
}

#[test]
fn fee_never_negative_at_edges() {
    for p in [1u32, 99] {
        assert!(taker_fee_cents(1, p) >= 0);
        assert!(taker_fee_cents(25, p) >= 0);
    }
    assert_eq!(taker_fee_cents(0, 50), 0);
    // 1 contract at 1c: 0.07 * 0.01 * 0.99 = 0.0007 dollars -> rounds up to 1 cent.
    assert_eq!(taker_fee_cents(1, 1), 1);
}

#[tokio::test]
async fn list_open_markets_follows_cursor() {
    let server = MockServer::start().await;
    let page1 = serde_json::json!({"cursor": "abc", "markets": [market_json("A-1")]});
    let page2 = serde_json::json!({"cursor": "", "markets": [market_json("B-1")]});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("status", "open"))
        .and(query_param("cursor", "abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page2))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("status", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let markets = client.list_open_markets().await.unwrap();
    let tickers: Vec<_> = markets.iter().map(|m| m.ticker.as_str()).collect();
    assert_eq!(tickers, ["A-1", "B-1"]);
}

#[tokio::test]
async fn get_markets_batches_of_100() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"cursor": "", "markets": [market_json("X-1")]})),
        )
        .expect(3)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let tickers: Vec<String> = (0..250).map(|i| format!("T-{i}")).collect();
    let markets = client.get_markets(&tickers).await.unwrap();
    assert_eq!(markets.len(), 3);
    let reqs = server.received_requests().await.unwrap();
    let first = reqs[0]
        .url
        .query_pairs()
        .find(|(k, _)| k == "tickers")
        .unwrap()
        .1
        .to_string();
    assert_eq!(first.split(',').count(), 100);
    server.verify().await;
}

#[tokio::test]
async fn get_event_category_is_cached_in_kv() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/KXBRASILEIROTOTAL-26NOV18CRCHA"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture_str("events.json")))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let conn = informed_pit::db::open(&dir.path().join("pit.db")).unwrap();
    let client = Client::with_base(&http, &server.uri());
    let a = client
        .get_event_category(&conn, "KXBRASILEIROTOTAL-26NOV18CRCHA")
        .await
        .unwrap();
    let b = client
        .get_event_category(&conn, "KXBRASILEIROTOTAL-26NOV18CRCHA")
        .await
        .unwrap();
    assert_eq!(a.as_deref(), Some("Sports"));
    assert_eq!(a, b);
    server.verify().await;
}

#[tokio::test]
async fn get_trades_sends_min_ts_and_follows_cursor() {
    let server = MockServer::start().await;
    let (_, cursor) = kalshi::parse_trades_page(&fixture("trades.json")).unwrap();
    // Second page (matched first, by cursor): empty and cursor-less, which ends paging.
    Mock::given(method("GET"))
        .and(path("/markets/trades"))
        .and(query_param("cursor", cursor.unwrap()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"cursor": "", "trades": []})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/markets/trades"))
        .and(query_param("ticker", "KXBRASILEIROTOTAL-26NOV18CRCHA-7"))
        .and(query_param("min_ts", "1700000000"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture_str("trades.json")))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let trades = client
        .get_trades("KXBRASILEIROTOTAL-26NOV18CRCHA-7", 1_700_000_000)
        .await
        .unwrap();
    assert!(trades.len() >= 5);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn get_market_unwraps_single_market_and_errors_on_404() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/markets/OK-1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"market": market_json("OK-1")})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/markets/NOPE-1"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    assert_eq!(client.get_market("OK-1").await.unwrap().ticker, "OK-1");
    assert!(client.get_market("NOPE-1").await.is_err());
}
