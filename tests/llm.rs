use std::time::Duration;

use informed_pit::config::Settings;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::llm::{Llm, LlmError, Model};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "sk-ant-test-secret-1234";

struct Fx {
    dir: TempDir,
    conn: Connection,
    http: Http,
    log: EventLog,
    settings: Settings,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let http = Http::new(EventLog::new(dir.path())).with_backoff_base(Duration::from_millis(1));
    let log = EventLog::new(dir.path());
    let settings = Settings::from_lookup(|k| match k {
        "ANTHROPIC_API_KEY" => Some(KEY.to_string()),
        "PIT_DATA" => Some("/unused".to_string()),
        _ => None,
    });
    Fx {
        dir,
        conn,
        http,
        log,
        settings,
    }
}

fn llm<'a>(f: &'a Fx, server: &MockServer) -> Llm<'a> {
    Llm::new(&f.http, &f.conn, &f.log, &f.settings, &server.uri())
}

fn schema() -> Value {
    json!({"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],
           "additionalProperties":false})
}

fn reply(model: &str, text: &str, stop: &str, i: u64, o: u64) -> Value {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": model,
        "content": [{"type": "text", "text": text}],
        "stop_reason": stop,
        "usage": {"input_tokens": i, "output_tokens": o}
    })
}

async fn mount(server: &MockServer, body: Value) {
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn call(
    l: &Llm<'_>,
    model: Model,
    max_tokens: u32,
) -> Result<(Value, informed_pit::llm::CallMeta), LlmError> {
    l.json_call(
        model,
        "test",
        Some(5),
        "sys prompt",
        "user prompt",
        schema(),
        max_tokens,
    )
    .await
}

fn seed_spend(conn: &Connection, ts: i64, usd: f64) {
    conn.execute(
        "INSERT INTO llm_calls (ts, role, model_requested, cost_usd, status) VALUES (?1, 'seed', 'x', ?2, 'ok')",
        params![ts, usd],
    )
    .unwrap();
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn rows(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM llm_calls WHERE role != 'seed'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

fn log_text(dir: &std::path::Path) -> String {
    let mut out = String::new();
    for e in std::fs::read_dir(dir.join("logs")).unwrap() {
        out += &std::fs::read_to_string(e.unwrap().path()).unwrap();
    }
    out
}

async fn only_request(server: &MockServer) -> wiremock::Request {
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    reqs.into_iter().next().unwrap()
}

#[tokio::test]
async fn haiku_body_has_thinking_disabled_and_schema() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-haiku-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    let (v, meta) = call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
    assert_eq!(v, json!({"answer":"hi"}));
    assert_eq!(meta.model_used, "claude-haiku-5-5");

    let req = only_request(&server).await;
    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["model"], "claude-haiku-5-5");
    assert_eq!(body["max_tokens"], 300);
    assert_eq!(body["system"], "sys prompt");
    assert_eq!(
        body["messages"],
        json!([{"role":"user","content":"user prompt"}])
    );
    assert_eq!(body["thinking"], json!({"type":"disabled"}));
    assert_eq!(
        body["output_config"],
        json!({"format":{"type":"json_schema","schema":schema()}})
    );
    assert!(body.get("fallbacks").is_none());
    let h = |n: &str| req.headers.get(n).map(|v| v.to_str().unwrap().to_string());
    assert_eq!(h("x-api-key").as_deref(), Some(KEY));
    assert_eq!(h("anthropic-version").as_deref(), Some("2026-10-21"));
    assert!(h("content-type").unwrap().starts_with("application/json"));
    assert!(h("anthropic-beta").is_none());
}

#[tokio::test]
async fn sonnet_body_has_adaptive_medium_and_fallback_header() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-sonnet-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    call(&llm(&f, &server), Model::Sonnet, 300).await.unwrap();

    let req = only_request(&server).await;
    let body: Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["model"], "claude-sonnet-5-5");
    assert_eq!(body["thinking"], json!({"type":"adaptive"}));
    assert_eq!(
        body["output_config"],
        json!({"format":{"type":"json_schema","schema":schema()},"effort":"medium"})
    );
    assert_eq!(body["fallbacks"], "default");
    assert_eq!(
        req.headers.get("anthropic-beta").unwrap().to_str().unwrap(),
        "server-side-fallback-2026-07-01"
    );
    assert_eq!(
        req.headers
            .get("anthropic-version")
            .unwrap()
            .to_str()
            .unwrap(),
        "2026-10-21"
    );
}

#[tokio::test]
async fn records_cost_from_usage() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply(
            "claude-haiku-5-5",
            r#"{"answer":"hi"}"#,
            "end_turn",
            1_000,
            200,
        ),
    )
    .await;
    let f = fx();
    let (_, meta) = call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
    assert!((meta.cost_usd - 0.0002).abs() < 1e-9, "{}", meta.cost_usd);

    let row: String = f
        .conn
        .query_row(
            "SELECT json_object('id', id, 'cand', candidate_id, 'role', role, 'req', model_requested,
                    'used', model_used, 'in', input_tokens, 'out', output_tokens, 'cost', cost_usd,
                    'status', status, 'error', error, 'request', request_json, 'response', response_json)
             FROM llm_calls",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let row: Value = serde_json::from_str(&row).unwrap();
    assert_eq!(row["id"], meta.call_id);
    assert_eq!(row["cand"], 5);
    assert_eq!(row["role"], "test");
    assert_eq!(row["req"], "claude-haiku-5-5");
    assert_eq!(row["used"], "claude-haiku-5-5");
    assert_eq!(
        (&row["in"], &row["out"], &row["status"]),
        (&json!(1_000), &json!(200), &json!("ok"))
    );
    assert!(row["error"].is_null());
    assert!((row["cost"].as_f64().unwrap() - 0.0002).abs() < 1e-9);
    assert!(row["request"].as_str().unwrap().contains("user prompt"));
    assert!(row["response"].as_str().unwrap().contains("end_turn"));
    assert!((informed_pit::llm::spend_today(&f.conn) - 0.0002).abs() < 1e-9);
    assert!((informed_pit::llm::spend_run(&f.conn) - 0.0002).abs() < 1e-9);
}

#[tokio::test]
async fn api_key_never_stored_or_logged() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-haiku-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
    let stored: String = f
        .conn
        .query_row(
            "SELECT request_json || response_json || COALESCE(error,'') FROM llm_calls",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!stored.contains(KEY));
    assert!(!log_text(f.dir.path()).contains(KEY));
}

#[tokio::test]
async fn refusal_maps_to_error_and_is_logged() {
    let server = MockServer::start().await;
    let mut body = reply("claude-haiku-5-5", "", "refusal", 50, 0);
    body["content"] = json!([]);
    body["stop_details"] = json!({"category": "cyber"});
    mount(&server, body).await;
    let f = fx();
    let err = call(&llm(&f, &server), Model::Haiku, 300)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, LlmError::Refusal { category } if category == "cyber"),
        "{err:?}"
    );
    // Not retried; billed input is still recorded.
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    let (status, error, cost): (String, String, f64) = f
        .conn
        .query_row("SELECT status, error, cost_usd FROM llm_calls", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!((status.as_str(), error.as_str()), ("refusal", "cyber"));
    assert!((cost - 50.0 * 0.10 / 1e6).abs() < 1e-12);
}

#[tokio::test]
async fn truncated_maps_to_error() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply(
            "claude-haiku-5-5",
            r#"{"answer":"cut"#,
            "max_tokens",
            10,
            300,
        ),
    )
    .await;
    let f = fx();
    let err = call(&llm(&f, &server), Model::Haiku, 300)
        .await
        .unwrap_err();
    assert!(matches!(err, LlmError::Truncated), "{err:?}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    let status: String = f
        .conn
        .query_row("SELECT status FROM llm_calls", [], |r| r.get(0))
        .unwrap();
    assert_eq!(status, "truncated");
}

#[tokio::test]
async fn bad_json_retried_once() {
    // First reply is not JSON, second is: success after exactly two calls, both recorded.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(reply(
            "claude-haiku-5-5",
            "not json",
            "end_turn",
            10,
            5,
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(reply(
            "claude-haiku-5-5",
            r#"{"answer":"ok"}"#,
            "end_turn",
            10,
            5,
        )))
        .mount(&server)
        .await;
    let f = fx();
    let (v, _) = call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
    assert_eq!(v, json!({"answer":"ok"}));
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    let statuses: Vec<String> = f
        .conn
        .prepare("SELECT status FROM llm_calls ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(statuses, ["bad_json", "ok"]);
}

#[tokio::test]
async fn bad_json_twice_gives_up_after_one_retry() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-haiku-5-5", "still not json", "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    let err = call(&llm(&f, &server), Model::Haiku, 300)
        .await
        .unwrap_err();
    assert!(matches!(err, LlmError::BadJson(_)), "{err:?}");
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(rows(&f.conn), 2);
}

#[tokio::test]
async fn day_cap_blocks_call_before_http() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-sonnet-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    seed_spend(&f.conn, now(), 2.99);
    // Sonnet reserve with max_tokens 2000 is about $0.02, which passes $3.
    let err = call(&llm(&f, &server), Model::Sonnet, 2_000)
        .await
        .unwrap_err();
    assert!(matches!(err, LlmError::CapHit { scope: "day" }), "{err:?}");
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
    assert_eq!(rows(&f.conn), 0);
    assert!(log_text(f.dir.path()).contains("llm_cap_hit"));
}

#[tokio::test]
async fn yesterdays_spend_does_not_count_against_the_day_cap() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-haiku-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    let today_start = now() - now().rem_euclid(86_400);
    seed_spend(&f.conn, today_start - 1, 2.99);
    call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
}

#[tokio::test]
async fn run_cap_counts_only_since_run_start() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply("claude-haiku-5-5", r#"{"answer":"hi"}"#, "end_turn", 10, 5),
    )
    .await;
    let f = fx();
    let t = now();
    // $59.99 spent before the run started does not count; the same amount after it does.
    // (The day cap is raised so only the run cap is in play.)
    let settings = Settings::from_lookup(|k| match k {
        "ANTHROPIC_API_KEY" => Some(KEY.to_string()),
        "PIT_DAILY_USD_CAP" => Some("1000".to_string()),
        _ => None,
    });
    let l = Llm::new(&f.http, &f.conn, &f.log, &settings, &server.uri());
    seed_spend(&f.conn, t - 10_000, 59.99);
    f.conn
        .execute(
            "INSERT INTO kv (key, value) VALUES ('run_start_ts', ?1)",
            [(t - 5_000).to_string()],
        )
        .unwrap();
    call(&l, Model::Sonnet, 2_000).await.unwrap();
    seed_spend(&f.conn, t - 1, 59.99);
    let err = call(&l, Model::Sonnet, 2_000).await.unwrap_err();
    assert!(matches!(err, LlmError::CapHit { scope: "run" }), "{err:?}");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn thinking_block_skipped_and_fallback_model_priced() {
    // A fallback served the call from Haiku: price at the model that answered, not the one requested.
    let server = MockServer::start().await;
    let mut body = reply(
        "claude-haiku-5-5",
        r#"{"answer":"fb"}"#,
        "end_turn",
        1_000_000,
        0,
    );
    body["content"] = json!([
        {"type": "thinking", "thinking": "hmm", "signature": "s"},
        {"type": "text", "text": r#"{"answer":"fb"}"#}
    ]);
    mount(&server, body).await;
    let f = fx();
    let (v, meta) = call(&llm(&f, &server), Model::Sonnet, 300).await.unwrap();
    assert_eq!(v, json!({"answer":"fb"}));
    assert_eq!(meta.model_used, "claude-haiku-5-5");
    // 1M tokens is above the 100K Haiku tier: $0.50, not Sonnet's $2.
    assert!((meta.cost_usd - 0.50).abs() < 1e-9, "{}", meta.cost_usd);
    let (req_m, used_m): (String, String) = f
        .conn
        .query_row(
            "SELECT model_requested, model_used FROM llm_calls",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (req_m.as_str(), used_m.as_str()),
        ("claude-sonnet-5-5", "claude-haiku-5-5")
    );
}

#[tokio::test]
async fn unknown_model_priced_at_sonnet_and_logged() {
    let server = MockServer::start().await;
    mount(
        &server,
        reply(
            "claude-mystery-1",
            r#"{"answer":"x"}"#,
            "end_turn",
            1_000_000,
            0,
        ),
    )
    .await;
    let f = fx();
    let (_, meta) = call(&llm(&f, &server), Model::Haiku, 300).await.unwrap();
    assert!((meta.cost_usd - 2.0).abs() < 1e-9, "{}", meta.cost_usd);
    assert!(log_text(f.dir.path()).contains("llm_unknown_model"));
}

#[tokio::test]
async fn api_4xx_is_recorded_with_message_and_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            json!({"type":"error","error":{"type":"invalid_request_error","message":"bad field"}}),
        ))
        .mount(&server)
        .await;
    let f = fx();
    let err = call(&llm(&f, &server), Model::Haiku, 300)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, LlmError::Api { status: 400, message } if message == "bad field"),
        "{err:?}"
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    let (status, cost): (String, f64) = f
        .conn
        .query_row("SELECT status, cost_usd FROM llm_calls", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!((status.as_str(), cost), ("api_error", 0.0));
}

#[tokio::test]
async fn missing_key_fails_before_http() {
    let server = MockServer::start().await;
    let f = fx();
    let settings = Settings::from_lookup(|_| None);
    let l = Llm::new(&f.http, &f.conn, &f.log, &settings, &server.uri());
    let err = call(&l, Model::Haiku, 300).await.unwrap_err();
    assert!(matches!(err, LlmError::MissingKey));
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn network_failure_is_not_retried_and_records_the_estimate() {
    // Ruling R24: nothing listens on port 1; the call is sent once and its reservation estimate is recorded
    // as spent (fail closed).
    let f = fx();
    let l = Llm::new(&f.http, &f.conn, &f.log, &f.settings, "http://127.0.0.1:1");
    let err = call(&l, Model::Haiku, 300).await.unwrap_err();
    assert!(matches!(err, LlmError::Http(_)), "{err:?}");
    let (status, cost): (String, f64) = f
        .conn
        .query_row("SELECT status, cost_usd FROM llm_calls", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(status, "http_error");
    // "sys prompt" + "user prompt" = 21 chars / 3.5 = 6 input tokens at $0.10/MTok, 300 output at $0.50/MTok.
    let estimate = (6.0 * 0.10 + 300.0 * 0.50) / 1e6;
    assert!((cost - estimate).abs() < 1e-12, "{cost}");
    let attempts = log_text(f.dir.path())
        .lines()
        .filter(|l| l.contains("\"kind\":\"http\""))
        .count();
    assert_eq!(attempts, 1);
}
