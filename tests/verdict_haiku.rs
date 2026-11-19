use std::time::Duration;

use informed_pit::config::Settings;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::Side;
use informed_pit::llm::{Llm, LlmError};
use informed_pit::news::article::KeptArticle;
use informed_pit::news::pubtime::TimeSource;
use informed_pit::phrases::MarketContext;
use informed_pit::verdict_haiku::{
    classify, judge, judge_all, quote_found, Branch, HaikuVerdict, PointsTo, MAX_ARTICLE_CHARS,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// 2026-11-18 14:00:00 UTC.
const MOVE_START: i64 = 1_795_010_400;

struct Fx {
    _dir: TempDir,
    conn: Connection,
    http: Http,
    log: EventLog,
    settings: Settings,
}

fn fx_with(daily_cap: Option<&str>) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
         VALUES (7, 'EV', 'yes', ?1, 90, 'investigating', 0, 0)",
        [MOVE_START],
    )
    .unwrap();
    let http = Http::new(EventLog::new(dir.path())).with_backoff_base(Duration::from_millis(1));
    let log = EventLog::new(dir.path());
    let cap = daily_cap.map(str::to_string);
    let settings = Settings::from_lookup(move |k| match k {
        "ANTHROPIC_API_KEY" => Some("sk-ant-test".to_string()),
        "PIT_DATA" => Some("/unused".to_string()),
        "PIT_DAILY_USD_CAP" => cap.clone(),
        _ => None,
    });
    Fx {
        _dir: dir,
        conn,
        http,
        log,
        settings,
    }
}

fn fx() -> Fx {
    fx_with(None)
}

fn ctx() -> MarketContext {
    MarketContext {
        title: "Will the Fed cut rates in December?".into(),
        subtitle: "Dec 2026 FOMC".into(),
        rules: "Resolves Yes if the target rate is lowered.".into(),
        direction: Side::Yes,
        from_c: 40,
        to_c: 62,
        window_min: 30,
    }
}

/// Inserts an `articles` row and returns the matching [`KeptArticle`].
fn article(conn: &Connection, n: i64, text: &str, unavailable: bool) -> KeptArticle {
    let url = format!("https://news.example/a{n}");
    conn.execute(
        "INSERT INTO articles (id, candidate_id, url, title, source, publish_ts, time_source, decision)
         VALUES (?1, 7, ?2, 'Fed signals cut', 'Example News', ?3, 'metadata', 'kept')",
        rusqlite::params![n, url, MOVE_START + 300],
    )
    .unwrap();
    KeptArticle {
        article_id: n,
        url,
        title: "Fed signals cut".into(),
        source: "Example News".into(),
        used_ts: MOVE_START + 300,
        time_source: TimeSource::Metadata,
        text: text.into(),
        text_unavailable: unavailable,
    }
}

fn reply(v: &Value) -> Value {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
        "content": [{"type": "text", "text": v.to_string()}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 800, "output_tokens": 90}
    })
}

fn verdict_json(points_to: &str, strength: i64, explains: bool, quote: &str) -> Value {
    json!({"relevant": true, "points_to": points_to, "strength": strength,
           "explains_move": explains, "quote": quote, "reason": "because"})
}

async fn mount_text(server: &MockServer, text: &str, times: u64) {
    let body = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 800, "output_tokens": 90}
    });
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .up_to_n_times(times)
        .mount(server)
        .await;
}

async fn mount_verdict(server: &MockServer, v: &Value, times: u64) {
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(reply(v)))
        .up_to_n_times(times)
        .mount(server)
        .await;
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn hv(
    relevant: bool,
    points_to: PointsTo,
    strength: u8,
    explains_move: bool,
) -> (HaikuVerdict, bool) {
    (
        HaikuVerdict {
            relevant,
            points_to,
            strength,
            explains_move,
            quote: "q".into(),
            reason: "r".into(),
        },
        true,
    )
}

// ---------------------------------------------------------------------------------------------------------
// Pure
// ---------------------------------------------------------------------------------------------------------

#[test]
fn quote_found_normalises_whitespace_and_curly_quotes() {
    let text = "Powell said the committee\u{2019}s \u{201c}data-dependent\u{201d} stance\n  is over \u{2014} cuts are coming.";
    assert!(quote_found(
        "committee's \"data-dependent\" stance is over - cuts",
        text
    ));
    assert!(quote_found(
        "COMMITTEE\u{2019}S   \u{201c}Data-Dependent\u{201d}",
        text
    ));
    assert!(quote_found("over \u{2013} cuts are coming.", text));
    // A trailing ellipsis (either form) is dropped before searching; so is a leading one.
    assert!(quote_found(
        "the committee's \"data-dependent\" stance\u{2026}",
        text
    ));
    assert!(quote_found(
        "the committee's \"data-dependent\" stance...",
        text
    ));
    assert!(quote_found("...stance is over", text));
}

#[test]
fn quote_missing_detected() {
    let text = "The Fed held rates steady on Wednesday.";
    assert!(!quote_found("The Fed cut rates on Wednesday", text));
    assert!(!quote_found("", text));
    assert!(!quote_found("   \u{2026}", text));
    // Words in the right order but not contiguous do not count.
    assert!(!quote_found("Fed rates Wednesday", text));
}

#[test]
fn classify_explained_needs_strength_2_same_direction() {
    let yes = Side::Yes;
    assert_eq!(
        classify(&[hv(true, PointsTo::Yes, 2, true)], yes),
        Branch::Explained
    );
    assert_eq!(
        classify(&[hv(true, PointsTo::Yes, 3, true)], yes),
        Branch::Explained
    );
    // Strength 1 is too weak.
    assert_eq!(
        classify(&[hv(true, PointsTo::Yes, 1, true)], yes),
        Branch::Unexplained
    );
    // explains_move false, or not relevant, does not count.
    assert_eq!(
        classify(&[hv(true, PointsTo::Yes, 3, false)], yes),
        Branch::Unexplained
    );
    assert_eq!(
        classify(&[hv(false, PointsTo::Yes, 3, true)], yes),
        Branch::Unexplained
    );
    // One good verdict among weak ones is enough, whatever order or quote status.
    let mut good = hv(true, PointsTo::Yes, 2, true);
    good.1 = false;
    assert_eq!(
        classify(&[hv(true, PointsTo::Neither, 0, false), good], yes),
        Branch::Explained
    );
    assert_eq!(classify(&[], yes), Branch::Unexplained);
}

#[test]
fn classify_opposite_direction_is_unexplained() {
    assert_eq!(
        classify(&[hv(true, PointsTo::No, 3, true)], Side::Yes),
        Branch::Unexplained
    );
    assert_eq!(
        classify(&[hv(true, PointsTo::Neither, 3, true)], Side::Yes),
        Branch::Unexplained
    );
    assert_eq!(
        classify(&[hv(true, PointsTo::No, 2, true)], Side::No),
        Branch::Explained
    );
    assert_eq!(
        classify(&[hv(true, PointsTo::Yes, 3, true)], Side::No),
        Branch::Unexplained
    );
}

// ---------------------------------------------------------------------------------------------------------
// With the model mocked
// ---------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn judge_stores_verdict_and_quote_check_and_sends_the_right_prompt() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(
        &server,
        &verdict_json("yes", 3, true, "the Fed will cut rates in December"),
        1,
    )
    .await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let art = article(
        &f.conn,
        1,
        "Officials said the Fed will cut rates in December.",
        false,
    );

    let j = judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    let v = j.verdict.clone().expect("verdict");
    assert_eq!(
        (v.relevant, v.points_to, v.strength, v.explains_move),
        (true, PointsTo::Yes, 3, true)
    );
    assert!(j.quote_ok);
    assert!(j.error.is_none());

    type Row = (
        i64,
        String,
        i64,
        String,
        i64,
        i64,
        String,
        Option<i64>,
        String,
    );
    let row: Row = f
        .conn
        .query_row(
            "SELECT article_id, status, relevant, points_to, strength, explains_move, quote, llm_call_id, verdict
             FROM haiku_verdicts WHERE candidate_id = 7",
            [],
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
    assert_eq!(row.0, 1);
    assert_eq!(row.1, "ok");
    assert_eq!((row.2, row.3.as_str(), row.4, row.5), (1, "yes", 3, 1));
    assert_eq!(row.6, "the Fed will cut rates in December");
    assert!(row.7.is_some());
    assert_eq!(row.8, "relevant");
    let (found, vid): (i64, i64) = f
        .conn
        .query_row(
            "SELECT found, haiku_verdict_id FROM quote_checks",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(found, 1);
    assert_eq!(
        vid,
        count(
            &f.conn,
            "SELECT id FROM haiku_verdicts WHERE candidate_id = 7"
        )
    );

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body: Value = reqs[0].body_json().unwrap();
    assert_eq!(body["model"], "claude-haiku-5-5");
    let user = body["messages"][0]["content"].as_str().unwrap();
    assert!(
        user.contains("Will the Fed cut rates in December?"),
        "{user}"
    );
    assert!(user.contains("from 40\u{a2} to 62\u{a2}"), "{user}");
    assert!(user.contains("30 minutes"), "{user}");
    assert!(user.contains("YES"), "{user}");
    // Published 5 minutes after the move started, UTC.
    assert!(user.contains("2026-11-18 14:05 UTC"), "{user}");
    assert!(user.contains("5 minutes after the move started"), "{user}");
    assert!(user.contains("Officials said the Fed will cut"), "{user}");
    // Strict schema: points_to enum and a 0..3 strength.
    let schema = &body["output_config"]["format"]["schema"];
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(
        schema["properties"]["points_to"]["enum"],
        json!(["yes", "no", "neither"])
    );
    assert_eq!(
        schema["properties"]["strength"]["enum"],
        json!([0, 1, 2, 3])
    );
}

#[tokio::test]
async fn hallucinated_quote_is_recorded_as_not_found() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(
        &server,
        &verdict_json("yes", 3, true, "a sentence the article never contained"),
        1,
    )
    .await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let art = article(
        &f.conn,
        1,
        "Officials said the Fed will cut rates in December.",
        false,
    );
    let j = judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    assert!(j.verdict.is_some());
    assert!(!j.quote_ok);
    let (found, detail): (i64, String) = f
        .conn
        .query_row("SELECT found, detail FROM quote_checks", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(found, 0);
    assert_eq!(detail, "quote_not_found");
}

#[tokio::test]
async fn quote_copied_from_escaped_text_is_found() {
    // The prompt shows `&`, `<`, `>` as entities; a quote copied from there still matches the page.
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(
        &server,
        &verdict_json("yes", 3, true, "AT&amp;T said the &lt;merger&gt; is off"),
        1,
    )
    .await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let art = article(
        &f.conn,
        1,
        "Late on Tuesday AT&T said the <merger> is off.",
        false,
    );
    let j = judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    assert!(j.quote_ok);
    let sent = server.received_requests().await.unwrap();
    let body = String::from_utf8_lossy(&sent[0].body).to_string();
    assert!(body.contains("AT&amp;T said the &lt;merger&gt; is off."));
    // The quote is kept and passed on as plain text (what Sonnet sees), not as entities.
    let plain = "AT&T said the <merger> is off";
    assert_eq!(j.verdict.as_ref().unwrap().quote, plain);
    let (stored, checked): (String, String) = f
        .conn
        .query_row(
            "SELECT hv.quote, qc.quote FROM haiku_verdicts hv JOIN quote_checks qc ON qc.haiku_verdict_id = hv.id",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((stored.as_str(), checked.as_str()), (plain, plain));
}

#[tokio::test]
async fn relevant_with_empty_quote_fails_the_check_but_irrelevant_does_not() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(&server, &verdict_json("yes", 2, true, ""), 1).await;
    mount_verdict(
        &server,
        &json!({"relevant": false, "points_to": "neither", "strength": 0, "explains_move": false,
                "quote": "", "reason": "off topic"}),
        1,
    )
    .await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let a1 = article(&f.conn, 1, "text one", false);
    let a2 = article(&f.conn, 2, "text two", false);
    assert!(!judge(&llm, &f.conn, 7, &ctx(), &a1).await.unwrap().quote_ok);
    let j2 = judge(&llm, &f.conn, 7, &ctx(), &a2).await.unwrap();
    assert!(j2.quote_ok);
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM quote_checks"), 2);
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM haiku_verdicts WHERE verdict = 'not_relevant'"
        ),
        1
    );
}

#[tokio::test]
async fn article_text_is_cut_at_12000_chars_on_a_char_boundary() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(&server, &verdict_json("neither", 0, false, ""), 1).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    // 2-byte characters: a byte cut at 12,000 would still be fine, 12,001 chars must cut mid-string.
    let text = format!("{}{}", "\u{e9}".repeat(MAX_ARTICLE_CHARS), "TAILMARK");
    let art = article(&f.conn, 1, &text, false);
    judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    let body: Value = server.received_requests().await.unwrap()[0]
        .body_json()
        .unwrap();
    let user = body["messages"][0]["content"].as_str().unwrap();
    assert_eq!(MAX_ARTICLE_CHARS, 12_000);
    assert_eq!(user.matches('\u{e9}').count(), 12_000);
    assert!(!user.contains("TAILMARK"));
}

#[tokio::test]
async fn unavailable_text_is_sent_as_title_and_snippet_and_says_so() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(&server, &verdict_json("neither", 0, false, ""), 1).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let art = article(&f.conn, 1, "Snippet only: Fed meets today.", true);
    judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    let body: Value = server.received_requests().await.unwrap()[0]
        .body_json()
        .unwrap();
    let user = body["messages"][0]["content"].as_str().unwrap();
    assert!(user.contains("Fed signals cut"), "{user}");
    assert!(user.contains("Snippet only: Fed meets today."), "{user}");
    assert!(user.contains("could not be fetched"), "{user}");
}

#[tokio::test]
async fn haiku_error_stored_after_bad_json_twice() {
    let f = fx();
    let server = MockServer::start().await;
    mount_text(&server, "this is not json", 2).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let art = article(&f.conn, 1, "text", false);

    let j = judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
    assert!(j.verdict.is_none());
    assert!(!j.quote_ok);
    assert!(
        j.error.as_deref().unwrap().contains("not valid JSON"),
        "{:?}",
        j.error
    );
    // One retry inside the client: two calls, one stored error, no quote check.
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    let (status, error, verdict): (String, String, String) = f
        .conn
        .query_row(
            "SELECT status, error, verdict FROM haiku_verdicts WHERE article_id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "haiku_error");
    assert_eq!(verdict, "haiku_error");
    assert!(error.contains("not valid JSON"), "{error}");
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM quote_checks"), 0);
}

#[tokio::test]
async fn out_of_range_or_missing_fields_are_haiku_errors() {
    let f = fx();
    let server = MockServer::start().await;
    mount_verdict(&server, &verdict_json("yes", 4, true, "x"), 1).await;
    mount_verdict(&server, &verdict_json("maybe", 2, true, "x"), 1).await;
    mount_verdict(&server, &json!({"relevant": true}), 1).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    for n in 1..=3 {
        let art = article(&f.conn, n, "text x", false);
        let j = judge(&llm, &f.conn, 7, &ctx(), &art).await.unwrap();
        assert!(j.verdict.is_none(), "article {n}");
        assert!(j.error.is_some());
    }
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM haiku_verdicts WHERE status = 'haiku_error' AND llm_call_id IS NOT NULL"
        ),
        3
    );
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM quote_checks"), 0);
}

#[tokio::test]
async fn judge_all_records_errors_and_carries_on() {
    let f = fx();
    let server = MockServer::start().await;
    // Article 1: API error (400, not retried). Article 2: good verdict.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({"error": {"message": "bad"}})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    mount_verdict(&server, &verdict_json("yes", 2, true, "text two"), 1).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let arts = vec![
        article(&f.conn, 1, "text one", false),
        article(&f.conn, 2, "text two", false),
    ];
    let out = judge_all(&llm, &f.conn, 7, &ctx(), &arts).await.unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].article_id, 1);
    assert!(out[0].verdict.is_none());
    assert!(
        out[0].error.as_deref().unwrap().contains("400"),
        "{:?}",
        out[0].error
    );
    assert!(out[1].verdict.is_some() && out[1].quote_ok);
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM haiku_verdicts WHERE status = 'haiku_error'"
        ),
        1
    );
}

#[tokio::test]
async fn judge_all_stops_on_cap_hit_and_returns_err() {
    // Cap of 1 millionth of a dollar: the very first reservation is refused, before any HTTP.
    let f = fx_with(Some("0.000001"));
    let server = MockServer::start().await;
    mount_verdict(&server, &verdict_json("yes", 2, true, "text"), 5).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());
    let arts = vec![
        article(&f.conn, 1, "text one", false),
        article(&f.conn, 2, "text two", false),
    ];
    let err = judge_all(&llm, &f.conn, 7, &ctx(), &arts)
        .await
        .unwrap_err();
    assert!(matches!(err, LlmError::CapHit { .. }), "{err:?}");
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM haiku_verdicts"), 0);
}
