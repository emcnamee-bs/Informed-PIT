//! Investigation state machine end to end, every external service mocked: Bing RSS, the article page,
//! Anthropic (phrases, per-article Haiku, Sonnet) and Kalshi (markets, market, order book).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use informed_pit::candidates::next_queued;
use informed_pit::config::Settings;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::hostguard::HostGuard;
use informed_pit::http::Http;
use informed_pit::investigate::{recover_on_boot, run_one, Deps, FinalStatus};
use informed_pit::news::NewsEndpoints;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const OG: &str = include_str!("fixtures/articles/og_time.html");

/// Move started 2026-11-18 14:00 UTC.
const MOVE_START: i64 = 1_795_010_400;
/// The candidate was created at 14:30 (the article's own publish time).
const CREATED: i64 = 1_795_012_200;
/// Investigation clock: five minutes after creation.
const NOW: i64 = CREATED + 300;
const CAND: i64 = 1;
/// A quote that really is in og_time.html.
const REAL_QUOTE: &str = "left its benchmark interest rate unchanged";

struct Fx {
    _dir: TempDir,
    conn: Connection,
    http: Http,
    log: EventLog,
    settings: Settings,
    srv: MockServer,
    ep: NewsEndpoints,
    clock: Arc<AtomicI64>,
}

async fn fx_with(daily_cap: Option<&str>) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts, best_to_c, from_c)
         VALUES (?1, 'EV', 'yes', ?2, 92, 'queued', ?3, ?3, 62, 40)",
        params![CAND, MOVE_START, CREATED],
    )
    .unwrap();
    // The best market, and a late joiner with a lower score.
    for (ticker, total, from_c, to_c) in [("M-1", 92.0, 40, 62), ("M-2", 85.0, 30, 45)] {
        conn.execute(
            "INSERT INTO candidate_markets (candidate_id, ticker, score_json) VALUES (?1, ?2, ?3)",
            params![
                CAND,
                ticker,
                json!({"ticker": ticker, "total": total, "from_c": from_c, "to_c": to_c, "window_min": 30,
                       "direction": "yes", "move_start_ts": MOVE_START})
                .to_string()
            ],
        )
        .unwrap();
    }
    // The run is active (paper buys happen only inside the run window).
    conn.execute_batch(&format!(
        "INSERT INTO kv (key, value) VALUES ('run_start_ts', '{s}'), ('run_end_ts', '{e}');",
        s = MOVE_START - 3600,
        e = MOVE_START - 3600 + 30 * 86_400
    ))
    .unwrap();
    // A few minute snapshots of the move for the Sonnet bars.
    for (i, mid) in [(0i64, 40.0), (10, 50.0), (20, 58.0), (30, 62.0)] {
        conn.execute(
            "INSERT INTO snapshots (ticker, ts, yes_bid_c, yes_ask_c, last_c, volume, mid_c) VALUES ('M-1', ?1, ?2, ?2, ?2, ?3, ?2)",
            params![MOVE_START + i * 60, mid, 100.0 + i as f64 * 10.0],
        )
        .unwrap();
    }
    let http = Http::with_spacing(EventLog::new(dir.path()), HashMap::new())
        .with_backoff_base(Duration::from_millis(1))
        .with_article_guard(HostGuard::allowing_loopback());
    let srv = MockServer::start().await;
    let ep = NewsEndpoints {
        rss_base: srv.uri(),
        newsapi_base: srv.uri(),
        wayback_base: srv.uri(),
    };
    let data = dir.path().to_string_lossy().to_string();
    let cap = daily_cap.map(str::to_string);
    let settings = Settings::from_lookup(move |k| match k {
        "ANTHROPIC_API_KEY" => Some("sk-ant-test".to_string()),
        "PIT_DATA" => Some(data.clone()),
        "PIT_DAILY_USD_CAP" => cap.clone(),
        _ => None,
    });
    Fx {
        log: EventLog::new(dir.path()),
        _dir: dir,
        conn,
        http,
        settings,
        srv,
        ep,
        clock: Arc::new(AtomicI64::new(NOW)),
    }
}

async fn fx() -> Fx {
    fx_with(None).await
}

async fn run(fx: &Fx) -> FinalStatus {
    let base = fx.srv.uri();
    let deps = Deps {
        http: &fx.http,
        conn: &fx.conn,
        log: &fx.log,
        settings: &fx.settings,
        news: &fx.ep,
        kalshi_base: &base,
        anthropic_base: &base,
    };
    let clock = fx.clock.clone();
    let now_fn = move || clock.load(Ordering::SeqCst);
    run_one(&deps, CAND, &now_fn).await.unwrap()
}

// ---------------------------------------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------------------------------------

fn reply(model: &str, v: &Value) -> Value {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": model,
        "content": [{"type": "text", "text": v.to_string()}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 800, "output_tokens": 90}
    })
}

fn not_relevant() -> Value {
    json!({"relevant": false, "points_to": "neither", "strength": 0, "explains_move": false,
           "quote": "", "reason": "about something else"})
}

fn explains() -> Value {
    json!({"relevant": true, "points_to": "yes", "strength": 3, "explains_move": true,
           "quote": REAL_QUOTE, "reason": "the decision drove the move"})
}

fn sonnet(decision: &str, side: &str, ticker: Option<&str>, hallucination: bool) -> Value {
    json!({"haiku_errors": [], "hallucination_found": hallucination, "explained_by_public_info": false,
           "explaining_articles": [], "decision": decision, "side": side, "ticker": ticker,
           "reason": "no public news explains a move this fast"})
}

async fn mount_phrases(fx: &Fx) {
    let phrases: Vec<String> = (1..=50).map(|n| format!("fed signal {n}")).collect();
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("Write search phrases"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(reply("claude-haiku-5-5", &json!({"phrases": phrases}))),
        )
        .named("haiku phrases")
        .mount(&fx.srv)
        .await;
}

async fn mount_verdict(fx: &Fx, v: Value) {
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("You check whether one news article"))
        .respond_with(ResponseTemplate::new(200).set_body_json(reply("claude-haiku-5-5", &v)))
        .named("haiku verdict")
        .mount(&fx.srv)
        .await;
}

/// `None`: Sonnet must not be called at all.
async fn mount_sonnet(fx: &Fx, v: Option<Value>) {
    let m = Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("claude-sonnet-5-5"));
    let (response, calls) = match v {
        Some(v) => (
            ResponseTemplate::new(200).set_body_json(reply("claude-sonnet-5-5", &v)),
            1,
        ),
        None => (ResponseTemplate::new(500), 0),
    };
    m.respond_with(response)
        .expect(calls)
        .named("sonnet review")
        .mount(&fx.srv)
        .await;
}

fn iso(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn market(ticker: &str, bid: f64, ask: f64) -> Value {
    json!({
        "ticker": ticker, "event_ticker": "EV", "title": format!("Will the Fed cut ({ticker})?"),
        "yes_sub_title": "Dec 2026", "rules_primary": "Resolves Yes if the target rate is lowered.",
        "close_time": iso(NOW + 2 * 86_400), "status": "active", "result": "",
        "yes_bid_dollars": format!("{:.4}", bid / 100.0), "yes_ask_dollars": format!("{:.4}", ask / 100.0),
        "last_price_dollars": format!("{:.4}", (bid + ask) / 200.0),
        "volume_fp": "1000.00", "volume_24h_fp": "500.00", "open_interest_fp": "700.00"
    })
}

/// `GET /markets?tickers=...` with M-1 at `bid`/`ask`; `times` limits how often this answer is given.
async fn mount_markets(fx: &Fx, bid: f64, ask: f64, times: Option<u64>) {
    let body =
        json!({"cursor": "", "markets": [market("M-1", bid, ask), market("M-2", 44.0, 46.0)]});
    let m = Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body));
    match times {
        Some(n) => m.up_to_n_times(n).mount(&fx.srv).await,
        None => m.mount(&fx.srv).await,
    }
}

/// The paper ledger's market and book reads; `expect` is how many book reads the test expects.
async fn mount_paper(fx: &Fx, expect: u64) {
    mount_paper_book(
        fx,
        json!([["0.3800", "20.00"], ["0.4000", "10.00"]]),
        expect,
    )
    .await;
}

/// Like [`mount_paper`] with the book's NO bids (`[["dollars", "contracts"], ...]`; yes asks are 1 minus these).
async fn mount_paper_book(fx: &Fx, no_bids: Value, expect: u64) {
    Mock::given(method("GET"))
        .and(path("/markets/M-1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"market": market("M-1", 60.0, 64.0)})),
        )
        .mount(&fx.srv)
        .await;
    Mock::given(method("GET"))
        .and(path("/markets/M-1/orderbook"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"orderbook_fp": {"yes_dollars": [], "no_dollars": no_bids}})),
        )
        .expect(expect)
        .named("order book")
        .mount(&fx.srv)
        .await;
}

/// A second article, published 14:32 UTC (inside the window), that only some tests serve.
const A2: &str = "<!doctype html><html><head><title>Fed officials weigh December options</title>\
<meta property=\"article:published_time\" content=\"2026-11-18T14:32:00Z\"></head><body><article>\
<p>Several Federal Reserve officials said on Wednesday that they are weighing their options for December, \
with some pointing to softer hiring data and others to inflation that remains above target.</p>\
<p>No decision has been made, the officials said, and markets continue to price a range of outcomes.</p>\
</article></body></html>";

async fn mount_news(fx: &Fx) {
    mount_news_with(fx, false).await;
}

/// The RSS feed (with article a2 too when `with_a2`) and the article pages.
async fn mount_news_with(fx: &Fx, with_a2: bool) {
    let mut items = format!(
        "<item><title>Fed holds rates steady</title><link>{}/a1</link>\
         <description>The Federal Reserve left rates unchanged.</description></item>",
        fx.srv.uri()
    );
    if with_a2 {
        items.push_str(&format!(
            "<item><title>Fed officials weigh December options</title><link>{}/a2</link>\
             <description>Officials weigh options.</description></item>",
            fx.srv.uri()
        ));
        Mock::given(method("GET"))
            .and(path("/a2"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(A2, "text/html; charset=utf-8"))
            .mount(&fx.srv)
            .await;
    }
    let rss = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?><rss version=\"2.0\"><channel><title>t</title>{items}\
         </channel></rss>"
    );
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(rss, "application/rss+xml; charset=utf-8"),
        )
        .mount(&fx.srv)
        .await;
    Mock::given(method("GET"))
        .and(path("/a1"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(OG, "text/html; charset=utf-8"))
        .mount(&fx.srv)
        .await;
}

/// Everything for a run that reaches the Sonnet review.
async fn mount_all(fx: &Fx, verdict: Value, review: Option<Value>, book_reads: u64) {
    mount_phrases(fx).await;
    mount_verdict(fx, verdict).await;
    mount_sonnet(fx, review).await;
    mount_markets(fx, 60.0, 64.0, None).await;
    mount_paper(fx, book_reads).await;
    mount_news(fx).await;
}

/// Answers like a normal Haiku verdict, and moves the fake clock while doing so.
struct AdvanceClock {
    clock: Arc<AtomicI64>,
    to: i64,
    body: Value,
}

impl Respond for AdvanceClock {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        self.clock.store(self.to, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(&self.body)
    }
}

// ---------------------------------------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------------------------------------

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn status(conn: &Connection) -> (String, Option<String>) {
    conn.query_row(
        "SELECT status, reason FROM candidates WHERE id = ?1",
        [CAND],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .unwrap()
}

/// `(from, to, reason)` of every `candidate_state` event, in order.
fn transitions(conn: &Connection) -> Vec<(String, String, String)> {
    conn.prepare("SELECT payload FROM events WHERE kind = 'candidate_state' AND candidate_id = ?1 ORDER BY id")
        .unwrap()
        .query_map([CAND], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|p| {
            let v: Value = serde_json::from_str(&p.unwrap()).unwrap();
            (
                v["from"].as_str().unwrap().to_string(),
                v["to"].as_str().unwrap().to_string(),
                v["reason"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn t(from: &str, to: &str, reason: &str) -> (String, String, String) {
    (from.into(), to.into(), reason.into())
}

fn sonnet_request_last(conn: &Connection) -> String {
    conn.query_row(
        "SELECT request_json FROM llm_calls WHERE role = 'sonnet_review' ORDER BY id DESC LIMIT 1",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn sonnet_request(conn: &Connection) -> String {
    conn.query_row(
        "SELECT request_json FROM llm_calls WHERE role = 'sonnet_review'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

// ---------------------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn full_path_unexplained_buys() {
    let fx = fx().await;
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "yes", Some("M-1"), false)),
        1,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::Bought);
    assert_eq!(status(&fx.conn), ("bought".into(), Some("ok".into())));
    assert_eq!(
        transitions(&fx.conn),
        vec![
            t("queued", "investigating", "start"),
            t("investigating", "bought", "ok")
        ]
    );
    let (ticker, side, contracts): (String, String, i64) = fx
        .conn
        .query_row(
            "SELECT ticker, side, contracts FROM paper_trades WHERE candidate_id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (ticker.as_str(), side.as_str(), contracts),
        ("M-1", "yes", 25)
    );

    let (branch, decision, final_buy, final_reason, call): (String, String, bool, String, Option<i64>) = fx
        .conn
        .query_row(
            "SELECT branch, decision, final_buy, final_reason, llm_call_id FROM sonnet_reviews WHERE candidate_id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        (
            branch.as_str(),
            decision.as_str(),
            final_buy,
            final_reason.as_str()
        ),
        ("unexplained", "buy", true, "ok")
    );
    assert!(call.is_some());

    // Sonnet saw both markets (late joiner too), the bars and the verdict, but no text of an article
    // Haiku did not mark relevant.
    let req = sonnet_request(&fx.conn);
    assert!(req.contains("M-2"), "late joiner shown");
    assert!(req.contains("minute_bars") && req.contains("about something else"));
    assert!(
        !req.contains("Chair Jerome Powell"),
        "irrelevant article text withheld"
    );
    assert!(req.contains("search_coverage") && req.contains("rss_ok"));
    assert!(
        req.contains("\"max_tokens\":16000"),
        "review output cap (R20)"
    );
}

#[tokio::test]
async fn explained_path_no_buy_even_if_sonnet_disagrees() {
    let fx = fx().await;
    mount_all(
        &fx,
        explains(),
        Some(sonnet("buy", "yes", Some("M-1"), false)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::Explained);
    assert_eq!(
        status(&fx.conn),
        ("explained".into(), Some("sonnet_disagrees".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE kind = 'sonnet_disagrees'"
        ),
        1
    );
    let (branch, final_buy, final_reason): (String, bool, String) = fx
        .conn
        .query_row(
            "SELECT branch, final_buy, final_reason FROM sonnet_reviews",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (branch.as_str(), final_buy, final_reason.as_str()),
        ("explained", false, "explained_path")
    );
    // The relevant article's full text went to Sonnet.
    assert!(sonnet_request(&fx.conn).contains("Chair Jerome Powell"));
}

#[tokio::test]
async fn hallucination_blocks_buy() {
    let fx = fx().await;
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "yes", Some("M-1"), true)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("hallucination".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
    assert_eq!(
        count(&fx.conn, "SELECT hallucination_found FROM sonnet_reviews"),
        1
    );
}

#[tokio::test]
async fn side_mismatch_blocks_buy() {
    let fx = fx().await;
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "no", Some("M-1"), false)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("side_mismatch".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn stale_after_20_minutes() {
    let fx = fx().await;
    mount_phrases(&fx).await;
    // Judging the article takes the clock past 20 minutes since the candidate was created.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("You check whether one news article"))
        .respond_with(AdvanceClock {
            clock: fx.clock.clone(),
            to: CREATED + 21 * 60,
            body: reply("claude-haiku-5-5", &not_relevant()),
        })
        .mount(&fx.srv)
        .await;
    mount_sonnet(&fx, None).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 0).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::Stale);
    assert_eq!(
        status(&fx.conn),
        ("stale".into(), Some("elapsed_20min".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 1);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"), 0);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn stale_when_price_reverses_past_from() {
    let fx = fx().await;
    mount_phrases(&fx).await;
    mount_verdict(&fx, not_relevant()).await;
    mount_sonnet(&fx, None).await;
    // At the start and after collecting M-1 is at 62; by the review it is back at 39, below its move start of 40.
    mount_markets(&fx, 60.0, 64.0, Some(2)).await;
    mount_markets(&fx, 38.0, 40.0, None).await;
    mount_paper(&fx, 0).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::Stale);
    assert_eq!(
        status(&fx.conn),
        ("stale".into(), Some("price_reversed".into()))
    );
    let detail: String = fx
        .conn
        .query_row(
            "SELECT payload FROM events WHERE kind = 'candidate_state' ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let v: Value = serde_json::from_str(&detail).unwrap();
    assert_eq!(v["detail"]["stage"], "before_review");
    assert_eq!(v["detail"]["mid_c"], 39.0);
}

#[tokio::test]
async fn cap_hit_marks_cap_skipped() {
    // $0.01/day: the Haiku calls fit, the Sonnet reservation (16,000 output tokens at $10/MTok) does not.
    let fx = fx_with(Some("0.01")).await;
    mount_all(&fx, not_relevant(), None, 0).await;

    assert_eq!(run(&fx).await, FinalStatus::CapSkipped);
    assert_eq!(
        status(&fx.conn),
        ("cap_skipped".into(), Some("cap_hit".into()))
    );
    // What was paid for before the cap stays.
    assert_eq!(
        count(&fx.conn, "SELECT COUNT(*) FROM phrases WHERE accepted = 1"),
        50
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 1);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"), 0);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn restart_marks_investigating_failed() {
    let fx = fx().await;
    fx.conn
        .execute(
            "UPDATE candidates SET status = 'investigating' WHERE id = 1",
            [],
        )
        .unwrap();
    fx.conn
        .execute(
            "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
             VALUES (2, 'EV2', 'no', 0, 90, 'queued', 0, 0)",
            [],
        )
        .unwrap();

    assert_eq!(recover_on_boot(&fx.conn, &fx.log).unwrap(), 1);
    assert_eq!(status(&fx.conn), ("failed".into(), Some("restart".into())));
    assert_eq!(
        transitions(&fx.conn),
        vec![t("investigating", "failed", "restart")]
    );
    let other: String = fx
        .conn
        .query_row("SELECT status FROM candidates WHERE id = 2", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(other, "queued", "queued candidates are left alone");
    assert_eq!(recover_on_boot(&fx.conn, &fx.log).unwrap(), 0);
}

#[tokio::test]
async fn trail_reconstructable() {
    let fx = fx().await;
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "yes", Some("M-1"), false)),
        1,
    )
    .await;
    assert_eq!(run(&fx).await, FinalStatus::Bought);

    // One query from the candidate through every stage to the trade, each stage linked by its keys.
    let row: (i64, i64, i64, i64, i64, i64, i64, i64, i64) = fx
        .conn
        .query_row(
            "SELECT COUNT(DISTINCT p.id), COUNT(DISTINCT lp.id), COUNT(DISTINCT s.id), COUNT(DISTINCT a.id),
                    COUNT(DISTINCT hv.id), COUNT(DISTINCT lh.id), COUNT(DISTINCT r.id), COUNT(DISTINCT lr.id),
                    COUNT(DISTINCT t.id)
             FROM candidates c
             JOIN phrases p ON p.candidate_id = c.id AND p.accepted = 1
             JOIN llm_calls lp ON lp.id = p.llm_call_id AND lp.role = 'phrases'
             JOIN searches s ON s.candidate_id = c.id
             JOIN articles a ON a.candidate_id = c.id AND a.decision = 'kept'
             JOIN haiku_verdicts hv ON hv.article_id = a.id AND hv.candidate_id = c.id
             JOIN llm_calls lh ON lh.id = hv.llm_call_id AND lh.role = 'haiku_verdict'
             JOIN sonnet_reviews r ON r.candidate_id = c.id
             JOIN llm_calls lr ON lr.id = r.llm_call_id AND lr.role = 'sonnet_review'
             JOIN paper_trades t ON t.candidate_id = c.id
             WHERE c.id = ?1",
            [CAND],
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
                ))
            },
        )
        .unwrap();
    // 50 phrases from one call, 10 RSS queries, 1 kept article, 1 verdict, 1 review, 1 trade.
    assert_eq!(row, (50, 1, 10, 1, 1, 1, 1, 1, 1));
    assert!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE candidate_id = 1"
        ) >= 4
    );
}

#[tokio::test]
async fn transient_error_requeues_with_backoff_then_fails() {
    let fx = fx().await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&fx.srv)
        .await;

    assert_eq!(run(&fx).await, FinalStatus::Requeued);
    let (st, attempts, retry_after): (String, i64, Option<i64>) = fx
        .conn
        .query_row(
            "SELECT status, attempts, retry_after_ts FROM candidates WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (st.as_str(), attempts, retry_after),
        ("queued", 1, Some(NOW + 60))
    );
    assert_eq!(
        next_queued(&fx.conn, NOW + 59).unwrap(),
        None,
        "backing off"
    );
    assert_eq!(next_queued(&fx.conn, NOW + 60).unwrap(), Some(CAND));

    // Retries 2 and 3 wait 4 and 16 minutes; the failure after the third retry is final.
    assert_eq!(run(&fx).await, FinalStatus::Requeued);
    assert_eq!(
        count(&fx.conn, "SELECT retry_after_ts FROM candidates"),
        NOW + 240
    );
    assert_eq!(run(&fx).await, FinalStatus::Requeued);
    assert_eq!(
        count(&fx.conn, "SELECT retry_after_ts FROM candidates"),
        NOW + 960
    );
    assert_eq!(run(&fx).await, FinalStatus::Failed);
    assert_eq!(status(&fx.conn), ("failed".into(), Some("error".into())));
    assert_eq!(count(&fx.conn, "SELECT attempts FROM candidates"), 4);
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM errors WHERE source = 'investigate'"
        ),
        4
    );
}

#[tokio::test]
async fn retry_reuses_phrases_and_verdicts() {
    let fx = fx().await;
    // First attempt: everything works up to the review, then Kalshi fails (start and after-collect reads work).
    mount_phrases(&fx).await;
    mount_verdict(&fx, not_relevant()).await;
    mount_sonnet(&fx, None).await;
    mount_markets(&fx, 60.0, 64.0, Some(2)).await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&fx.srv)
        .await;
    mount_news(&fx).await;
    assert_eq!(run(&fx).await, FinalStatus::Requeued);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 1);

    // Second attempt: no new phrase or Haiku calls are allowed.
    fx.srv.reset().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("claude-haiku-5-5"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&fx.srv)
        .await;
    mount_sonnet(&fx, Some(sonnet("buy", "yes", Some("M-1"), false))).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 1).await;
    mount_news(&fx).await;
    assert_eq!(run(&fx).await, FinalStatus::Bought);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM phrases"), 50);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 1);
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE kind = 'phrases_reused'"
        ),
        1
    );
    assert_eq!(
        transitions(&fx.conn)
            .into_iter()
            .map(|(_, to, _)| to)
            .collect::<Vec<_>>(),
        vec!["investigating", "queued", "investigating", "bought"]
    );
}

/// First attempt: reviewed ("buy" on M-1, no relevant article), then Kalshi fails before the buy.
async fn attempt_reviewed_then_kalshi_fails(fx: &Fx) {
    mount_phrases(fx).await;
    mount_verdict(fx, not_relevant()).await;
    mount_sonnet(fx, Some(sonnet("buy", "yes", Some("M-1"), false))).await;
    // Start, after-collect and before-review reads work; the before-buy read fails.
    mount_markets(fx, 60.0, 64.0, Some(3)).await;
    Mock::given(method("GET"))
        .and(path("/markets"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&fx.srv)
        .await;
    mount_news(fx).await;
    assert_eq!(run(fx).await, FinalStatus::Requeued);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"), 1);
    fx.srv.reset().await;
}

#[tokio::test]
async fn retry_with_new_evidence_asks_sonnet_again() {
    let fx = fx().await;
    attempt_reviewed_then_kalshi_fails(&fx).await;

    // Second attempt finds a new relevant (not explaining) article: Haiku judges only it, and Sonnet must
    // see it (the first review is not reused).
    mount_verdict(
        &fx,
        json!({"relevant": true, "points_to": "yes", "strength": 1, "explains_move": false,
               "quote": "weighing their options for December", "reason": "background only"}),
    )
    .await;
    mount_sonnet(&fx, Some(sonnet("buy", "yes", Some("M-1"), false))).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 1).await;
    mount_news_with(&fx, true).await;

    assert_eq!(run(&fx).await, FinalStatus::Bought);
    assert_eq!(
        count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"),
        2,
        "Sonnet asked twice in all"
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(DISTINCT evidence_fp) FROM sonnet_reviews"
        ),
        2
    );
    assert_eq!(
        count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"),
        2,
        "a1 not judged again"
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE kind = 'review_reused'"
        ),
        0
    );
    // The decision is recorded on the review it came from (the newest).
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM sonnet_reviews WHERE final_reason = 'ok'
             AND id = (SELECT MAX(id) FROM sonnet_reviews)"
        ),
        1
    );
    assert!(sonnet_request_last(&fx.conn).contains("weighing their options for December"));
}

#[tokio::test]
async fn retry_with_unchanged_evidence_reuses_the_review() {
    let fx = fx().await;
    attempt_reviewed_then_kalshi_fails(&fx).await;

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .named("no LLM call at all")
        .mount(&fx.srv)
        .await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 1).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::Bought);
    assert_eq!(
        count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"),
        1,
        "Sonnet asked once in all"
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE kind = 'review_reused'"
        ),
        1
    );
    assert_eq!(count(&fx.conn, "SELECT final_buy FROM sonnet_reviews"), 1);
}

#[tokio::test]
async fn late_review_still_shows_the_move_start_bar() {
    let fx = fx().await;
    // Exactly 20 minutes after creation (not yet stale), 50 minutes after the move started.
    fx.clock.store(CREATED + 20 * 60, Ordering::SeqCst);
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("no_buy", "yes", None, false)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    let req = sonnet_request(&fx.conn);
    assert!(
        req.contains(&format!("\\\"ts\\\": {MOVE_START}")),
        "move-start bar shown"
    );
    assert!(req.contains("\\\"minute\\\": 0,"));
}

#[tokio::test]
async fn stale_when_no_move_price_reverses() {
    let fx = fx().await;
    // A NO move: M-1 fell from 60 to 40.
    fx.conn
        .execute(
            "UPDATE candidates SET direction = 'no', from_c = 60, best_to_c = 40",
            [],
        )
        .unwrap();
    fx.conn
        .execute(
            "UPDATE candidate_markets SET score_json = ?1 WHERE ticker = 'M-1'",
            [
                json!({"ticker": "M-1", "total": 92.0, "from_c": 60, "to_c": 40, "window_min": 30,
                    "direction": "no", "move_start_ts": MOVE_START})
                .to_string(),
            ],
        )
        .unwrap();
    mount_phrases(&fx).await;
    mount_verdict(&fx, not_relevant()).await;
    mount_sonnet(&fx, None).await;
    // Mid 40 at the start and after collecting, back up to 61 (at or past 60 against a NO move) by the review.
    mount_markets(&fx, 38.0, 42.0, Some(2)).await;
    mount_markets(&fx, 60.0, 62.0, None).await;
    mount_paper(&fx, 0).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::Stale);
    assert_eq!(
        status(&fx.conn),
        ("stale".into(), Some("price_reversed".into()))
    );
}

#[tokio::test]
async fn explained_path_with_sonnet_agreeing() {
    let fx = fx().await;
    mount_all(
        &fx,
        explains(),
        Some(sonnet("no_buy", "yes", None, false)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::Explained);
    assert_eq!(
        status(&fx.conn),
        ("explained".into(), Some("explained_path".into()))
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM events WHERE kind = 'sonnet_disagrees'"
        ),
        0
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn no_buy_after_the_run_ended() {
    let fx = fx().await;
    // The run window closed one second before the investigation reaches the buy.
    fx.conn
        .execute(
            "UPDATE kv SET value = ?1 WHERE key = 'run_end_ts'",
            [(NOW - 1).to_string()],
        )
        .unwrap();
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "yes", Some("M-1"), false)),
        0,
    )
    .await;
    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("run_ended".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn paper_refusal_ends_no_buy_with_its_reason() {
    let fx = fx().await;
    mount_phrases(&fx).await;
    mount_verdict(&fx, not_relevant()).await;
    mount_sonnet(&fx, Some(sonnet("buy", "yes", Some("M-1"), false))).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    // Only a no bid of 5 cents: a yes ask of 95, above the 92 cent limit.
    mount_paper_book(&fx, json!([["0.0500", "100.00"]]), 1).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("paper_no_liquidity".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn bad_ticker_blocks_buy() {
    let fx = fx().await;
    mount_all(
        &fx,
        not_relevant(),
        Some(sonnet("buy", "yes", Some("OTHER-1"), false)),
        0,
    )
    .await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("bad_ticker".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

/// Moves the fake clock to `to` while serving the article page.
struct SlowArticle {
    clock: Arc<AtomicI64>,
    to: i64,
}

impl Respond for SlowArticle {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        self.clock.store(self.to, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_raw(OG, "text/html; charset=utf-8")
    }
}

#[tokio::test]
async fn stale_after_collect_skips_the_haiku_spend() {
    let fx = fx().await;
    // Fetching the article takes the clock past 20 minutes since creation. This mock is mounted first, so
    // it answers /a1 instead of the plain one from mount_news.
    Mock::given(method("GET"))
        .and(path("/a1"))
        .respond_with(SlowArticle {
            clock: fx.clock.clone(),
            to: CREATED + 21 * 60,
        })
        .mount(&fx.srv)
        .await;
    mount_phrases(&fx).await;
    // Haiku and Sonnet must not be called.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("You check whether one news article"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&fx.srv)
        .await;
    mount_sonnet(&fx, None).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 0).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::Stale);
    assert_eq!(
        status(&fx.conn),
        ("stale".into(), Some("elapsed_20min".into()))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 0);
    assert_eq!(last_state(&fx.conn)["detail"]["stage"], "after_collect");
}

#[test]
fn evidence_degraded_rule() {
    use informed_pit::investigate::evidence_degraded;
    // Haiku errors: more than 25 % of kept articles.
    assert_eq!(evidence_degraded(4, 1, 5, 0, false), None, "exactly 25 %");
    assert_eq!(evidence_degraded(4, 2, 5, 0, false), Some("haiku_errors"));
    assert_eq!(evidence_degraded(0, 0, 5, 0, false), None, "nothing kept");
    // News search: more RSS failures than successes, and no NewsAPI.ai fallback succeeded.
    assert_eq!(
        evidence_degraded(1, 0, 2, 2, false),
        None,
        "a tie is not degraded"
    );
    assert_eq!(evidence_degraded(1, 0, 1, 3, false), Some("news_search"));
    assert_eq!(
        evidence_degraded(1, 0, 1, 3, true),
        None,
        "fallback rescued it"
    );
    assert_eq!(evidence_degraded(0, 0, 0, 4, false), Some("news_search"));
}

/// Payload of the candidate's last `candidate_state` event.
fn last_state(conn: &Connection) -> Value {
    let p: String = conn
        .query_row(
            "SELECT payload FROM events WHERE kind = 'candidate_state' ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    serde_json::from_str(&p).unwrap()
}

#[tokio::test]
async fn rss_outage_without_fallback_is_no_buy_and_skips_sonnet() {
    let fx = fx().await;
    mount_phrases(&fx).await;
    mount_sonnet(&fx, None).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 0).await;
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&fx.srv)
        .await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("evidence_degraded".into()))
    );
    let v = last_state(&fx.conn);
    assert_eq!(v["detail"]["degraded"], "news_search");
    assert_eq!(v["detail"]["rss_ok"], 0);
    assert!(v["detail"]["rss_failed"].as_u64().unwrap() > 0);
    assert_eq!(v["detail"]["fallback_ok"], false);
    // The coverage numbers are in the haiku_branch event too.
    let b: String = fx
        .conn
        .query_row(
            "SELECT payload FROM events WHERE kind = 'haiku_branch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let b: Value = serde_json::from_str(&b).unwrap();
    assert_eq!(
        (b["rss_ok"].clone(), b["fallback_ok"].clone()),
        (json!(0), json!(false))
    );
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"), 0);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM paper_trades"), 0);
}

#[tokio::test]
async fn haiku_errors_over_a_quarter_is_no_buy_and_skips_sonnet() {
    let fx = fx().await;
    mount_phrases(&fx).await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(body_string_contains("You check whether one news article"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            json!({"type":"error","error":{"type":"invalid_request_error","message":"nope"}}),
        ))
        .mount(&fx.srv)
        .await;
    mount_sonnet(&fx, None).await;
    mount_markets(&fx, 60.0, 64.0, None).await;
    mount_paper(&fx, 0).await;
    mount_news(&fx).await;

    assert_eq!(run(&fx).await, FinalStatus::NoBuy);
    assert_eq!(
        status(&fx.conn),
        ("no_buy".into(), Some("evidence_degraded".into()))
    );
    let v = last_state(&fx.conn);
    assert_eq!(v["detail"]["degraded"], "haiku_errors");
    assert_eq!(v["detail"]["kept"], 1);
    assert_eq!(v["detail"]["haiku_errors"], 1);
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM sonnet_reviews"), 0);
}
