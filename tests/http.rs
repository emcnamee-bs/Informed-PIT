use std::collections::HashMap;
use std::time::{Duration, Instant};

use informed_pit::eventlog::EventLog;
use informed_pit::hostguard::HostGuard;
use informed_pit::http::{CallCtx, Http, HttpError, Service};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ctx(attempts: u32) -> CallCtx {
    CallCtx {
        label: "test",
        candidate_id: Some(7),
        max_attempts: attempts,
        store_raw: true,
    }
}

fn fast_http(dir: &std::path::Path) -> Http {
    let mut spacing = HashMap::new();
    spacing.insert(Service::Rss, Duration::from_millis(100));
    // Mock servers are on 127.0.0.1, which the production article guard refuses.
    Http::with_spacing(EventLog::new(dir), spacing)
        .with_backoff_base(Duration::from_millis(1))
        .with_article_guard(HostGuard::allowing_loopback())
}

/// All `http` events logged so far, as parsed JSON.
fn http_events(dir: &std::path::Path) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir.join("logs")).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        for line in text.lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if v["kind"] == "http" {
                out.push(v);
            }
        }
    }
    out
}

#[tokio::test]
async fn spacing_enforced_per_service() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let url = format!("{}/feed", server.uri());

    let start = Instant::now();
    for _ in 0..3 {
        let r = http
            .send(Service::Rss, reqwest::Client::new().get(&url), ctx(1))
            .await
            .unwrap();
        assert_eq!(r.status, 200);
    }
    assert!(
        start.elapsed() >= Duration::from_millis(200),
        "took {:?}",
        start.elapsed()
    );

    // A service with no spacing configured is not delayed by the Rss gate.
    let start = Instant::now();
    for _ in 0..3 {
        http.send(Service::Anthropic, reqwest::Client::new().get(&url), ctx(1))
            .await
            .unwrap();
    }
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn retries_429_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/m"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/m"))
        .respond_with(ResponseTemplate::new(200).set_body_string("hello"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());

    let r = http
        .send(
            Service::Kalshi,
            reqwest::Client::new().get(format!("{}/m", server.uri())),
            ctx(4),
        )
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(&r.body[..], b"hello");
    assert!(r.final_url.ends_with("/m"));
    assert!(
        dir.path().join(&r.raw_path).exists(),
        "raw body stored at {}",
        r.raw_path
    );

    let ev = http_events(dir.path());
    assert_eq!(ev.len(), 2);
    assert_eq!(ev[0]["payload"]["status"], 429);
    assert_eq!(ev[0]["payload"]["attempt"], 1);
    assert_eq!(ev[1]["payload"]["status"], 200);
    assert_eq!(ev[1]["payload"]["attempt"], 2);
    assert_eq!(ev[1]["payload"]["service"], "kalshi");
    assert_eq!(ev[1]["candidate_id"], 7);
}

#[tokio::test]
async fn gives_up_after_max_attempts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());

    let err = http
        .send(
            Service::Wayback,
            reqwest::Client::new().get(server.uri()),
            ctx(3),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::Exhausted { attempts: 3 }),
        "{err:?}"
    );
    assert_eq!(http_events(dir.path()).len(), 3);
}

#[tokio::test]
async fn non_retryable_status_is_returned_not_retried() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());

    let r = http
        .send(
            Service::Article,
            reqwest::Client::new().get(server.uri()),
            ctx(3),
        )
        .await
        .unwrap();
    assert_eq!(r.status, 404);
}

#[tokio::test]
async fn network_error_is_retried_then_exhausted() {
    // Port 1 on localhost refuses connections.
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let err = http
        .send(
            Service::Kalshi,
            reqwest::Client::new().get("http://127.0.0.1:1/x"),
            ctx(2),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::Exhausted { attempts: 2 }),
        "{err:?}"
    );
    let ev = http_events(dir.path());
    assert_eq!(ev.len(), 2);
    assert!(ev[0]["payload"]["status"].is_null());
    assert!(ev[0]["payload"]["error"].is_string());
}

#[tokio::test]
async fn redacts_api_key_in_log() {
    let secret = "SUPERSECRETKEY12345";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("x-api-key", secret))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());

    let req = reqwest::Client::new()
        .get(format!("{}/q?apiKey={secret}&q=bitcoin", server.uri()))
        .header("x-api-key", secret);
    let r = http.send(Service::NewsApi, req, ctx(1)).await.unwrap();
    assert_eq!(r.status, 200);
    assert!(!r.final_url.contains(secret));

    let mut all = String::new();
    for entry in std::fs::read_dir(dir.path().join("logs")).unwrap() {
        all.push_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap());
    }
    assert!(all.contains("q=bitcoin"), "non-secret query kept: {all}");
    assert!(all.contains("REDACTED"), "{all}");
    assert!(!all.contains(secret), "secret leaked: {all}");
}

#[tokio::test]
async fn anthropic_network_error_is_not_retried() {
    // Ruling R24: a timed-out or dropped Anthropic call may have been billed, so it is not sent again.
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let err = http
        .send(
            Service::Anthropic,
            reqwest::Client::new().post("http://127.0.0.1:1/v1/messages"),
            ctx(3),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, HttpError::Exhausted { attempts: 1 }),
        "{err:?}"
    );
    assert_eq!(http_events(dir.path()).len(), 1);
}

#[tokio::test]
async fn anthropic_429_and_529_are_still_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(529))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let http = fast_http(dir.path());
    let r = http
        .send(
            Service::Anthropic,
            reqwest::Client::new().post(format!("{}/v1/messages", server.uri())),
            ctx(3),
        )
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}
