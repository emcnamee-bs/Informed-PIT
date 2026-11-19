use std::time::Duration;

use informed_pit::config::Settings;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::Side;
use informed_pit::llm::{Llm, LlmError};
use informed_pit::phrases::{generate, validate, MarketContext, PhraseError};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Fx {
    _dir: TempDir,
    conn: Connection,
    http: Http,
    log: EventLog,
    settings: Settings,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
         VALUES (7, 'EV', 'yes', 0, 90, 'investigating', 0, 0)",
        [],
    )
    .unwrap();
    let http = Http::new(EventLog::new(dir.path())).with_backoff_base(Duration::from_millis(1));
    let log = EventLog::new(dir.path());
    let settings = Settings::from_lookup(|k| match k {
        "ANTHROPIC_API_KEY" => Some("sk-ant-test".to_string()),
        "PIT_DATA" => Some("/unused".to_string()),
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

/// `n` distinct valid phrases, `tag` keeps batches apart.
fn phrases(tag: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{tag} fed cut {i}")).collect()
}

fn reply(list: &[String]) -> Value {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-haiku-5-5",
        "content": [{"type": "text", "text": json!({"phrases": list}).to_string()}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 500, "output_tokens": 600}
    })
}

async fn mount_once(server: &MockServer, list: &[String]) {
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(reply(list)))
        .up_to_n_times(1)
        .mount(server)
        .await;
}

async fn mount_api_error(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({"error": {"message": "bad"}})),
        )
        .mount(server)
        .await;
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

#[test]
fn validate_rejects_one_word_and_five_words() {
    let (ok, bad) = validate(s(&[
        "fed",
        "fed rate cut",
        "the fed cuts rates today",
        "fed cut",
        "  powell speech now  ",
        "",
    ]));
    assert_eq!(ok, s(&["fed rate cut", "fed cut", "powell speech now"]));
    assert_eq!(
        bad,
        vec![
            ("fed".to_string(), "word_count"),
            ("the fed cuts rates today".to_string(), "word_count"),
            (String::new(), "word_count"),
        ]
    );
}

#[test]
fn validate_rejects_long_and_quoted() {
    let long = format!("{} {}", "a".repeat(40), "b".repeat(25));
    let (ok, bad) = validate(vec![
        long,
        "\"fed cut\"".into(),
        "fed \u{201c}cut\u{201d} today".into(),
        "powell's speech".into(),
        "'fed cut'".into(),
    ]);
    assert_eq!(ok, s(&["powell's speech"]));
    assert_eq!(
        bad.iter().map(|(_, r)| *r).collect::<Vec<_>>(),
        vec!["too_long", "quote", "quote", "quote"]
    );
}

#[test]
fn validate_dedupes_case_insensitively() {
    let (ok, bad) = validate(s(&[
        "Fed Rate Cut",
        "fed rate cut",
        " FED RATE CUT ",
        "fed cut",
    ]));
    assert_eq!(ok, s(&["Fed Rate Cut", "fed cut"]));
    assert_eq!(
        bad,
        vec![
            ("fed rate cut".to_string(), "duplicate"),
            ("FED RATE CUT".to_string(), "duplicate"),
        ]
    );
}

#[tokio::test]
async fn retry_requests_missing_count() {
    let f = fx();
    let server = MockServer::start().await;
    mount_once(&server, &phrases("first", 40)).await;
    mount_once(&server, &phrases("second", 10)).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let got = generate(&llm, &f.conn, 7, &ctx()).await.unwrap();
    assert_eq!(got.len(), 50);
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM llm_calls"), 2);
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM phrases WHERE candidate_id = 7 AND accepted = 1"
        ),
        50
    );
    assert_eq!(
        count(&f.conn, "SELECT COUNT(*) FROM phrases WHERE attempt = 2"),
        10
    );
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM phrases WHERE llm_call_id IS NULL"
        ),
        0
    );

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2);
    let bodies: Vec<Value> = reqs.iter().map(|r| r.body_json().unwrap()).collect();
    let user = |b: &Value| b["messages"][0]["content"].as_str().unwrap().to_string();
    assert!(
        user(&bodies[0]).contains("exactly 50"),
        "{}",
        user(&bodies[0])
    );
    assert!(user(&bodies[0]).contains("from 40"), "{}", user(&bodies[0]));
    assert!(
        user(&bodies[1]).contains("exactly 10"),
        "{}",
        user(&bodies[1])
    );
    // The retry lists what is already accepted so the model does not repeat it.
    assert!(user(&bodies[1]).contains("first fed cut 0"));
    assert_eq!(bodies[0]["max_tokens"], 1500);
}

#[tokio::test]
async fn full_first_reply_makes_one_call_and_extras_are_capped() {
    let f = fx();
    let server = MockServer::start().await;
    mount_once(&server, &phrases("big", 55)).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let got = generate(&llm, &f.conn, 7, &ctx()).await.unwrap();
    assert_eq!(got.len(), 50);
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM llm_calls"), 1);
    assert_eq!(
        count(
            &f.conn,
            "SELECT COUNT(*) FROM phrases WHERE accepted = 0 AND reject_reason = 'excess'"
        ),
        5
    );
}

#[tokio::test]
async fn fails_below_10_valid() {
    let f = fx();
    let server = MockServer::start().await;
    let mut first = phrases("ok", 5);
    first.extend(s(&["one", "way too many words in this one"]));
    mount_once(&server, &first).await;
    mount_once(&server, &phrases("more", 3)).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let err = generate(&llm, &f.conn, 7, &ctx()).await.unwrap_err();
    match err {
        PhraseError::TooFew { valid, required } => assert_eq!((valid, required), (8, 10)),
        other => panic!("wrong error: {other:?}"),
    }
    // Everything is stored, accepted and rejected, even though the candidate failed.
    assert_eq!(
        count(&f.conn, "SELECT COUNT(*) FROM phrases WHERE accepted = 1"),
        8
    );
    let reasons: Vec<String> = f
        .conn
        .prepare("SELECT reject_reason FROM phrases WHERE accepted = 0 ORDER BY id")
        .unwrap()
        .query_map(params![], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(reasons, vec!["word_count", "word_count"]);
}

#[tokio::test]
async fn retry_failure_keeps_the_valid_phrases_when_enough() {
    let f = fx();
    let server = MockServer::start().await;
    mount_once(&server, &phrases("ok", 30)).await;
    mount_api_error(&server).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let got = generate(&llm, &f.conn, 7, &ctx()).await.unwrap();
    assert_eq!(got.len(), 30);
}

#[tokio::test]
async fn retry_failure_below_10_returns_the_llm_error() {
    let f = fx();
    let server = MockServer::start().await;
    mount_once(&server, &phrases("ok", 4)).await;
    mount_api_error(&server).await;
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let err = generate(&llm, &f.conn, 7, &ctx()).await.unwrap_err();
    assert!(
        matches!(err, PhraseError::Llm(LlmError::Api { status: 400, .. })),
        "{err:?}"
    );
}

#[tokio::test]
async fn cap_hit_propagates_distinguishably() {
    let f = fx();
    let server = MockServer::start().await;
    f.conn
        .execute(
            "INSERT INTO llm_calls (ts, role, model_requested, cost_usd, status)
             VALUES (?1, 'seed', 'x', 1000, 'ok')",
            [chrono::Utc::now().timestamp()],
        )
        .unwrap();
    let llm = Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri());

    let err = generate(&llm, &f.conn, 7, &ctx()).await.unwrap_err();
    assert!(
        matches!(err, PhraseError::Llm(LlmError::CapHit { .. })),
        "{err:?}"
    );
    assert_eq!(count(&f.conn, "SELECT COUNT(*) FROM phrases"), 0);
    assert!(server.received_requests().await.unwrap().is_empty());
}
