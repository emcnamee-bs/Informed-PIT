use std::time::Duration;

use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::{Client, Market};
use informed_pit::universe::{self, MarketWithCategory, UniverseRules};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;

fn rules() -> UniverseRules {
    UniverseRules::load(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/config/universe.toml"
    )))
    .unwrap()
}

fn market(ticker: &str, close_in_secs: i64) -> Market {
    Market {
        ticker: ticker.to_string(),
        event_ticker: format!("{}-EV", ticker.split('-').next().unwrap()),
        series_ticker: ticker.split('-').next().unwrap().to_string(),
        title: format!("title {ticker}"),
        subtitle: String::new(),
        rules_primary: String::new(),
        category: None,
        close_ts: NOW + close_in_secs,
        yes_bid_c: Some(40),
        yes_ask_c: Some(44),
        last_c: Some(42),
        volume: 1000,
        volume_24h: 100,
        open_interest: 50,
        status: "active".to_string(),
        result: None,
    }
}

fn with(market: Market, category: &str) -> MarketWithCategory {
    MarketWithCategory {
        market,
        category: Some(category.to_string()),
        recent_trade: Some(true),
        lookup_failed: false,
    }
}

/// A market that passes every rule.
fn good(ticker: &str) -> MarketWithCategory {
    with(market(ticker, 5 * DAY), "Politics")
}

fn only_reason(sel: &universe::Selection, ticker: &str) -> Option<&'static str> {
    sel.rejected
        .iter()
        .find(|(t, _)| t == ticker)
        .map(|(_, r)| *r)
}

#[test]
fn shipped_rules_match_the_brief() {
    let r = rules();
    assert_eq!(
        r.allow_categories,
        [
            "Politics",
            "Elections",
            "Economics",
            "Financials",
            "Companies",
            "World",
            "Science and Technology",
            "Health",
            "Transportation"
        ]
    );
    assert_eq!(
        r.deny_series_prefixes,
        ["KXBTC", "KXETH", "KXSOL", "KXINX", "KXNASDAQ", "KXDOGE", "KXXRP"]
    );
    assert_eq!(
        (r.max_markets, r.max_close_days, r.min_close_secs),
        (300, 14, 3600)
    );
}

#[test]
fn rejects_sports_category() {
    let sel = universe::select(
        &[
            with(market("KXGAME-1", 5 * DAY), "Sports"),
            good("KXGOOD-1"),
        ],
        &rules(),
        NOW,
    );
    assert_eq!(sel.kept, ["KXGOOD-1"]);
    assert_eq!(only_reason(&sel, "KXGAME-1"), Some("category"));
}

#[test]
fn rejects_unknown_category() {
    let mut m = good("KXMYSTERY-1");
    m.category = None;
    let sel = universe::select(&[m], &rules(), NOW);
    assert_eq!(only_reason(&sel, "KXMYSTERY-1"), Some("category"));
}

#[test]
fn rejects_close_in_30_min() {
    let sel = universe::select(
        &[with(market("KXSOON-1", 30 * 60), "Politics")],
        &rules(),
        NOW,
    );
    assert!(sel.kept.is_empty());
    assert_eq!(only_reason(&sel, "KXSOON-1"), Some("closes_too_soon"));
}

#[test]
fn rejects_close_in_15_days() {
    let sel = universe::select(
        &[with(market("KXLATE-1", 15 * DAY), "Politics")],
        &rules(),
        NOW,
    );
    assert!(sel.kept.is_empty());
    assert_eq!(only_reason(&sel, "KXLATE-1"), Some("closes_too_late"));
}

#[test]
fn keeps_close_in_13_days() {
    let sel = universe::select(
        &[with(market("KXOK-1", 13 * DAY), "Politics")],
        &rules(),
        NOW,
    );
    assert_eq!(sel.kept, ["KXOK-1"]);
    assert!(sel.rejected.is_empty());
}

#[test]
fn close_window_edges() {
    let r = rules();
    let edge = |secs| universe::select(&[with(market("KXEDGE-1", secs), "Politics")], &r, NOW);
    assert_eq!(
        only_reason(&edge(3600), "KXEDGE-1"),
        Some("closes_too_soon"),
        "exactly 1 h is not more than 1 h"
    );
    assert_eq!(edge(3601).kept, ["KXEDGE-1"]);
    assert_eq!(
        edge(14 * DAY).kept,
        ["KXEDGE-1"],
        "exactly 14 days is allowed"
    );
    assert_eq!(
        only_reason(&edge(14 * DAY + 1), "KXEDGE-1"),
        Some("closes_too_late")
    );
}

#[test]
fn rejects_one_sided_quote() {
    let mut no_ask = market("KXNOASK-1", 5 * DAY);
    no_ask.yes_ask_c = None;
    let mut no_bid = market("KXNOBID-1", 5 * DAY);
    no_bid.yes_bid_c = None;
    let sel = universe::select(
        &[with(no_ask, "Politics"), with(no_bid, "Politics")],
        &rules(),
        NOW,
    );
    assert!(sel.kept.is_empty());
    assert_eq!(only_reason(&sel, "KXNOASK-1"), Some("no_two_sided_quote"));
    assert_eq!(only_reason(&sel, "KXNOBID-1"), Some("no_two_sided_quote"));
}

#[test]
fn rejects_deny_prefix_btc() {
    // The category would pass; the series prefix is what excludes it.
    let sel = universe::select(
        &[with(market("KXBTCD-26NOV-T100000", 5 * DAY), "Financials")],
        &rules(),
        NOW,
    );
    assert!(sel.kept.is_empty());
    assert_eq!(only_reason(&sel, "KXBTCD-26NOV-T100000"), Some("category"));
}

#[test]
fn rejects_no_recent_trade() {
    let mut m = good("KXQUIET-1");
    m.recent_trade = Some(false);
    let sel = universe::select(&[m], &rules(), NOW);
    assert_eq!(only_reason(&sel, "KXQUIET-1"), Some("no_recent_trade"));
}

#[test]
fn rejects_never_traded() {
    let mut m = market("KXNEVER-1", 5 * DAY);
    m.volume = 0;
    m.volume_24h = 0;
    m.last_c = None;
    let sel = universe::select(&[with(m, "Politics")], &rules(), NOW);
    assert_eq!(only_reason(&sel, "KXNEVER-1"), Some("no_recent_trade"));
}

#[test]
fn unchecked_non_allowed_market_is_a_category_rejection() {
    let mut m = with(market("KXSPORT-1", 5 * DAY), "Sports");
    m.recent_trade = None;
    let sel = universe::select(&[m], &rules(), NOW);
    assert_eq!(only_reason(&sel, "KXSPORT-1"), Some("category"));
}

#[test]
fn unchecked_allowed_market_ranks_last_and_falls_to_cap() {
    let mut confirmed = good("KXCONF-1");
    confirmed.market.volume_24h = 0; // found via a trade lookup, so volume_24h is 0
    let mut unchecked = good("KXUNCHK-1");
    unchecked.market.volume_24h = 0;
    unchecked.market.volume = 5000; // more lifetime volume than the confirmed one
    unchecked.recent_trade = None;
    let mut r = rules();
    r.max_markets = 1;
    let sel = universe::select(&[unchecked, confirmed], &r, NOW);
    assert_eq!(sel.kept, ["KXCONF-1"]);
    assert_eq!(only_reason(&sel, "KXUNCHK-1"), Some("cap"));
}

#[test]
fn caps_at_max_by_volume_24h() {
    let markets: Vec<MarketWithCategory> = (0..305)
        .map(|i| {
            let mut m = good(&format!("KXM{i}-1"));
            m.market.volume_24h = i as i64 + 1; // 1..=305, all distinct
            m
        })
        .collect();
    let sel = universe::select(&markets, &rules(), NOW);
    assert_eq!(sel.kept.len(), 300);
    assert_eq!(sel.rejected.len(), 5);
    assert!(sel.rejected.iter().all(|(_, r)| *r == "cap"));
    // The five lowest volumes (1..=5) are the ones cut; the highest is first.
    let mut cut: Vec<&str> = sel.rejected.iter().map(|(t, _)| t.as_str()).collect();
    cut.sort_unstable();
    assert_eq!(cut, ["KXM0-1", "KXM1-1", "KXM2-1", "KXM3-1", "KXM4-1"]);
    assert_eq!(sel.kept[0], "KXM304-1");
}

// ---- refresh against a mock server ----

fn fast_http(dir: &std::path::Path) -> Http {
    Http::with_spacing(EventLog::new(dir), Default::default())
        .with_backoff_base(Duration::from_millis(1))
}

fn rfc3339(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn market_json(
    ticker: &str,
    event: &str,
    close_in_secs: i64,
    volume_24h: &str,
) -> serde_json::Value {
    serde_json::json!({
        "ticker": ticker, "event_ticker": event, "title": format!("title {ticker}"), "yes_sub_title": "s",
        "close_time": rfc3339(NOW + close_in_secs), "status": "active", "result": "",
        "yes_bid_dollars": "0.4200", "yes_ask_dollars": "0.4500", "last_price_dollars": "0.4300",
        "volume_fp": "100.00", "volume_24h_fp": volume_24h, "open_interest_fp": "7.00"
    })
}

fn event_body(category: &str) -> serde_json::Value {
    serde_json::json!({"event": {"event_ticker": "x", "category": category}, "markets": []})
}

async fn mount_event(server: &MockServer, event: &str, category: &str, expect: u64) {
    Mock::given(method("GET"))
        .and(path(format!("/events/{event}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(event_body(category)))
        .expect(expect)
        .mount(server)
        .await;
}

async fn mount_trades(server: &MockServer, ticker: &str, trade: bool, expect: u64) {
    let trades = if trade {
        serde_json::json!([{"trade_id": "t1", "ticker": ticker, "created_time": rfc3339(NOW - DAY),
            "taker_side": "yes", "yes_price_dollars": "0.4200", "count_fp": "3.00"}])
    } else {
        serde_json::json!([])
    };
    Mock::given(method("GET"))
        .and(path("/markets/trades"))
        .and(query_param("ticker", ticker))
        .and(query_param("limit", "1"))
        .and(query_param("min_ts", (NOW - 7 * DAY).to_string()))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"trades": trades, "cursor": ""})),
        )
        .expect(expect)
        .mount(server)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn refresh_records_run_markets_and_event() {
    let server = MockServer::start().await;
    let markets = serde_json::json!({"markets": [
        market_json("POL-A", "POL-EV", 5 * DAY, "10.00"),           // kept (traded today)
        market_json("POL-B", "POL-EV", 5 * DAY, "0.00"),            // quiet, but a trade 1 day ago: kept
        market_json("POL-C", "POL-EV", 5 * DAY + 60, "0.00"),       // quiet, no trade in 7 days
        market_json("SPT-A", "SPT-EV", 5 * DAY, "50.00"),           // sports
        market_json("FAR-A", "FAR-EV", 30 * DAY, "50.00"),          // closes too late: no category lookup
        market_json("KXBTC-A", "KXBTC-EV", 5 * DAY, "50.00"),       // deny prefix: no category lookup
    ], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .and(query_param("status", "open"))
        .and(query_param("min_close_ts", (NOW + 3600).to_string()))
        .and(query_param("max_close_ts", (NOW + 14 * DAY).to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(&server)
        .await;
    mount_event(&server, "POL-EV", "Politics", 1).await; // one lookup for three markets
    mount_event(&server, "SPT-EV", "Sports", 1).await;
    mount_event(&server, "FAR-EV", "Politics", 0).await;
    mount_event(&server, "KXBTC-EV", "Financials", 0).await;
    mount_trades(&server, "POL-B", true, 1).await;
    mount_trades(&server, "POL-C", false, 1).await;
    mount_trades(&server, "POL-A", true, 0).await; // already traded today: no trade lookup

    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let log = EventLog::new(dir.path());

    let id = universe::refresh_with(&client, &conn, &log, &rules(), NOW)
        .await
        .unwrap();

    let n: i64 = conn
        .query_row(
            "SELECT n_markets FROM universe_runs WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 2);
    let mut stmt = conn
        .prepare("SELECT ticker, category FROM markets WHERE active = 1 ORDER BY ticker")
        .unwrap();
    let active: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        active,
        [
            ("POL-A".to_string(), "Politics".to_string()),
            ("POL-B".to_string(), "Politics".to_string())
        ]
    );

    // The universe event is in today's log with the counts.
    let logs = std::fs::read_dir(dir.path().join("logs")).unwrap();
    let text: String = logs
        .map(|f| std::fs::read_to_string(f.unwrap().path()).unwrap())
        .collect();
    let line = text
        .lines()
        .find(|l| l.contains("\"kind\":\"universe\""))
        .expect("universe event logged");
    let ev: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(ev["payload"]["run_id"], id);
    assert_eq!(ev["payload"]["fetched"], 6);
    assert_eq!(ev["payload"]["kept"], 2);
    assert_eq!(ev["payload"]["rejected_by_reason"]["no_recent_trade"], 1);
    assert_eq!(ev["payload"]["rejected_by_reason"]["category"], 2); // sports + deny prefix
    assert_eq!(ev["payload"]["rejected_by_reason"]["closes_too_late"], 1);

    // A refresh that gets an empty listing is an error and leaves yesterday's universe alone.
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"markets": [], "cursor": ""})),
        )
        .mount(&server)
        .await;
    let err = universe::refresh_with(&client, &conn, &log, &rules(), NOW + 60)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no open markets"), "{err:#}");
    let active: i64 = conn
        .query_row("SELECT COUNT(*) FROM markets WHERE active = 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    let runs: i64 = conn
        .query_row("SELECT COUNT(*) FROM universe_runs", [], |r| r.get(0))
        .unwrap();
    assert_eq!((active, runs), (2, 1));
}

#[tokio::test(flavor = "current_thread")]
async fn trade_labels_reflect_what_was_checked() {
    let server = MockServer::start().await;
    let with_volume = |mut m: serde_json::Value, v: &str| {
        m["volume_fp"] = serde_json::json!(v);
        m
    };
    let markets = serde_json::json!({"markets": [
        market_json("POL-A", "POL-EV", 5 * DAY, "10.00"),                          // confirmed by 24 h volume
        with_volume(market_json("POL-B", "POL-EV", 5 * DAY, "0.00"), "100.00"),    // quiet, allowed: never checked
        with_volume(market_json("SPT-A", "SPT-EV", 5 * DAY, "0.00"), "100.00"),    // quiet, not allowed
        with_volume(market_json("POL-N", "POL-EV", 5 * DAY, "0.00"), "0.00"),      // never traded
    ], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(&server)
        .await;
    mount_event(&server, "POL-EV", "Politics", 1).await;
    mount_event(&server, "SPT-EV", "Sports", 1).await;
    // The cap (1) is full after POL-A, so no quiet market is trade-checked at all.
    for t in ["POL-A", "POL-B", "SPT-A", "POL-N"] {
        mount_trades(&server, t, true, 0).await;
    }

    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let mut r = rules();
    r.max_markets = 1;
    let plan = universe::plan(&client, &conn, &r, NOW).await.unwrap();
    assert_eq!(plan.selection.kept, ["POL-A"]);
    assert_eq!(only_reason(&plan.selection, "POL-B"), Some("cap"));
    assert_eq!(only_reason(&plan.selection, "SPT-A"), Some("category"));
    assert_eq!(
        only_reason(&plan.selection, "POL-N"),
        Some("no_recent_trade")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_category_lookup_rejects_that_series_only() {
    let server = MockServer::start().await;
    let markets = serde_json::json!({"markets": [
        market_json("BAD-A", "BAD-EV", 5 * DAY, "10.00"),
        market_json("POL-A", "POL-EV", 5 * DAY, "5.00"),
    ], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events/BAD-EV"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    mount_event(&server, "POL-EV", "Politics", 1).await;

    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let plan = universe::plan(&client, &conn, &rules(), NOW).await.unwrap();
    assert_eq!(plan.selection.kept, ["POL-A"]);
    assert_eq!(plan.lookup_failures, 1);
    assert_eq!(only_reason(&plan.selection, "BAD-A"), Some("lookup_failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn failed_trade_lookup_is_lookup_failed_and_not_kept() {
    let server = MockServer::start().await;
    let mut quiet = market_json("POL-Q", "POL-EV", 5 * DAY, "0.00");
    quiet["volume_fp"] = serde_json::json!("100.00");
    let markets = serde_json::json!({"markets": [market_json("POL-A", "POL-EV", 5 * DAY, "10.00"), quiet], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(&server)
        .await;
    mount_event(&server, "POL-EV", "Politics", 1).await;
    Mock::given(method("GET"))
        .and(path("/markets/trades"))
        .and(query_param("ticker", "POL-Q"))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let plan = universe::plan(&client, &conn, &rules(), NOW).await.unwrap();
    assert_eq!(plan.selection.kept, ["POL-A"]);
    assert_eq!(plan.lookup_failures, 1);
    assert_eq!(only_reason(&plan.selection, "POL-Q"), Some("lookup_failed"));
}

#[test]
fn lookup_failed_is_never_kept_even_with_room() {
    let mut m = good("KXFAIL-1");
    m.lookup_failed = true;
    let sel = universe::select(&[m], &rules(), NOW);
    assert!(sel.kept.is_empty());
    assert_eq!(only_reason(&sel, "KXFAIL-1"), Some("lookup_failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn one_category_lookup_per_series_and_cached_across_runs() {
    let server = MockServer::start().await;
    let markets = serde_json::json!({"markets": [
        market_json("POL-A", "POL-EV1", 5 * DAY, "10.00"),
        market_json("POL-B", "POL-EV2", 5 * DAY, "9.00"),
        market_json("POL-C", "POL-EV3", 5 * DAY, "8.00"),
    ], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(&server)
        .await;
    // Three events of one series: exactly one /events call in total, even over two runs.
    Mock::given(method("GET"))
        .and(path("/events/POL-EV1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(event_body("Politics")))
        .expect(1)
        .mount(&server)
        .await;
    for ev in ["POL-EV2", "POL-EV3"] {
        Mock::given(method("GET"))
            .and(path(format!("/events/{ev}")))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
    }

    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let client = Client::with_base(&http, &server.uri());
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    for _ in 0..2 {
        let plan = universe::plan(&client, &conn, &rules(), NOW).await.unwrap();
        assert_eq!(plan.selection.kept.len(), 3);
        assert_eq!(plan.series_looked_up, 1);
    }
}
