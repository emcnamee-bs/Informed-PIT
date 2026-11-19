use std::collections::HashMap;
use std::time::Duration;

use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::{Http, Service};
use informed_pit::news::newsapi::{self, CapReason, NewsApiError};
use informed_pit::news::{
    bing_publisher_url, build_queries, canonical_link, rss, search_all, NewsEndpoints, Via,
    MAX_QUERY_ENCODED,
};
use rusqlite::{params, Connection};
use serde_json::Value;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const RSS_OK: &str = include_str!("fixtures/news/rss_ok.xml");
const RSS_EMPTY: &str = include_str!("fixtures/news/rss_empty.xml");
const RSS_CONSENT: &str = include_str!("fixtures/news/rss_consent.html");
const NEWSAPI_OK: &str = include_str!("fixtures/news/newsapi_ok.json");
const KEY: &str = "test-key-do-not-leak-123";

struct Fx {
    dir: TempDir,
    conn: Connection,
    log: EventLog,
    http: Http,
    server: MockServer,
    ep: NewsEndpoints,
}

async fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let mut spacing = HashMap::new();
    spacing.insert(Service::Rss, Duration::from_millis(1));
    spacing.insert(Service::NewsApi, Duration::from_millis(1));
    let http = Http::with_spacing(EventLog::new(dir.path()), spacing)
        .with_backoff_base(Duration::from_millis(1));
    let server = MockServer::start().await;
    let ep = NewsEndpoints {
        rss_base: server.uri(),
        newsapi_base: server.uri(),
        wayback_base: server.uri(),
    };
    Fx {
        log: EventLog::new(dir.path()),
        dir,
        conn,
        http,
        server,
        ep,
    }
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn phrases(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("topic{i} alpha beta")).collect()
}

fn xml(body: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body.to_string(), "application/rss+xml; charset=utf-8")
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn kv(conn: &Connection, key: &str) -> Option<String> {
    conn.query_row("SELECT value FROM kv WHERE key = ?1", [key], |r| r.get(0))
        .ok()
}

fn set_kv(conn: &Connection, key: &str, value: &str) {
    conn.execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
        [key, value],
    )
    .unwrap();
}

/// All events of `kind` in the JSONL logs.
fn events(dir: &std::path::Path, kind: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir.join("logs")).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        for line in text.lines() {
            let v: Value = serde_json::from_str(line).unwrap();
            if v["kind"] == kind {
                out.push(v);
            }
        }
    }
    out
}

fn insert_newsapi_search(conn: &Connection, candidate: i64, ts: i64, tokens: i64) {
    conn.execute(
        "INSERT INTO searches (candidate_id, ts, source, ok, tokens_used) VALUES (?1, ?2, 'newsapi', 1, ?3)",
        params![candidate, ts, tokens],
    )
    .unwrap();
}

async fn mount_newsapi_ok(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v1/article/getArticles"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string(NEWSAPI_OK),
        )
        .mount(server)
        .await;
}

async fn newsapi_calls(server: &MockServer) -> Vec<wiremock::Request> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/api/v1/article/getArticles")
        .collect()
}

/// Six phrases make two queries: five phrases, then one.
fn six() -> Vec<String> {
    phrases(6)
}

/// The `q` the RSS search sends for the one query built from `group`.
fn q_of(group: &[String]) -> String {
    build_queries(group).remove(0)
}

async fn mount_rss_for(server: &MockServer, group: &[String], resp: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", q_of(group)))
        .respond_with(resp)
        .mount(server)
        .await;
}

// ---- build_queries ----

fn encoded_q_len(q: &str) -> usize {
    let url = reqwest::Url::parse_with_params("https://x.test/news/search", [("q", q)]).unwrap();
    url.query().unwrap().len() - "q=".len()
}

#[test]
fn build_queries_respects_length_and_covers_all_phrases() {
    // 50 ordinary phrases: about 10 queries, every phrase quoted in exactly one of them.
    let ps = phrases(50);
    let qs = build_queries(&ps);
    assert_eq!(qs.len(), 10, "{qs:?}");
    for p in &ps {
        let quoted = format!("\"{p}\"");
        assert_eq!(qs.iter().filter(|q| q.contains(&quoted)).count(), 1, "{p}");
    }
    assert!(qs[0].starts_with('"') && qs[0].contains("\" OR \""));

    // Phrases that inflate when URL-encoded (6 chars per accented letter) force more, shorter queries.
    let wide: Vec<String> = (0..50)
        .map(|i| {
            let w = "\u{e9}".repeat(13);
            format!("{w}{i:02} {w} {w} {w}")
        })
        .collect();
    let qs = build_queries(&wide);
    assert!(qs.len() > 10, "{}", qs.len());
    for q in &qs {
        let len = encoded_q_len(q);
        assert!(len <= MAX_QUERY_ENCODED, "{len} > {MAX_QUERY_ENCODED}");
    }
    for p in &wide {
        let quoted = format!("\"{p}\"");
        assert_eq!(qs.iter().filter(|q| q.contains(&quoted)).count(), 1);
    }

    // Quotes inside a phrase are dropped, blanks and repeats skipped.
    let qs = build_queries(&s(&["say \"hi\" there", "  ", "Say hi there"]));
    assert_eq!(qs, vec!["\"say hi there\"".to_string()]);
    assert!(build_queries(&[]).is_empty());
}

// ---- rss ----

#[tokio::test]
async fn rss_parses_items_and_pubdate() {
    let fx = fx().await;
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", "\"federal reserve\""))
        .and(query_param("format", "rss"))
        .and(query_param("qft", "interval=\"7\""))
        .respond_with(xml(RSS_OK))
        .expect(1)
        .mount(&fx.server)
        .await;

    let hits = rss::search(&fx.http, &fx.ep, "\"federal reserve\"", 7)
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);

    let h = &hits[0];
    assert_eq!(h.title, "Fed holds rates steady, signals caution");
    assert!(h
        .link
        .starts_with("http://www.bing.com/news/apiclick.aspx?"));
    assert_eq!(h.source, "Reuters");
    // Bing says "14:30:00 GMT" but means Pacific wall time: 2026-11-18 22:30:00 UTC.
    assert_eq!(h.rss_pub_ts, Some(1_795_041_000));
    assert_eq!(h.via, Via::Rss);
    assert_eq!(h.newsapi_pub_ts, None);
    assert_eq!(
        h.snippet,
        "The Federal Reserve left rates unchanged on Wednesday and signalled caution about cuts."
    );

    // XML entities are decoded in titles; the double-escaped snippet loses its markup.
    assert_eq!(hits[1].title, "Markets react to Powell & the rate decision");
    assert_eq!(
        hits[1].snippet,
        "Stocks & bonds moved after Powell's remarks."
    );
    // No pubDate and no <source>: kept, with None / empty; a CDATA description is read.
    assert_eq!(hits[2].rss_pub_ts, None);
    assert_eq!(hits[2].source, "");
    assert_eq!(hits[2].snippet, "What the minutes say about the next move.");
}

#[tokio::test]
async fn rss_empty_channel_is_ok_empty() {
    let fx = fx().await;
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(xml(RSS_EMPTY))
        .mount(&fx.server)
        .await;
    let hits = rss::search(&fx.http, &fx.ep, "\"nothing here\"", 7)
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[tokio::test]
async fn rss_html_consent_page_is_error() {
    let fx = fx().await;
    // 200 with an HTML content type.
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", "\"consent\""))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(RSS_CONSENT, "text/html; charset=utf-8"),
        )
        .mount(&fx.server)
        .await;
    let err = rss::search(&fx.http, &fx.ep, "\"consent\"", 7)
        .await
        .unwrap_err();
    assert!(matches!(err.kind, rss::RssErrorKind::Html), "{err}");
    assert_eq!(err.http_status, Some(200));
    assert!(err.raw_path.is_some(), "the offending body is kept");

    // HTML served with an XML content type: no <channel>, so still an error.
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", "\"disguised\""))
        .respond_with(xml(RSS_CONSENT))
        .mount(&fx.server)
        .await;
    assert!(rss::search(&fx.http, &fx.ep, "\"disguised\"", 7)
        .await
        .is_err());

    // Truncated XML and a 404 are errors too.
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", "\"cut\""))
        .respond_with(xml(&RSS_OK[..RSS_OK.len() / 2]))
        .mount(&fx.server)
        .await;
    assert!(rss::search(&fx.http, &fx.ep, "\"cut\"", 7).await.is_err());
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .and(query_param("q", "\"gone\""))
        .respond_with(ResponseTemplate::new(404))
        .mount(&fx.server)
        .await;
    let err = rss::search(&fx.http, &fx.ep, "\"gone\"", 7)
        .await
        .unwrap_err();
    assert_eq!(err.http_status, Some(404));
}

// ---- fallback ----

#[tokio::test]
async fn fallback_runs_only_when_an_rss_query_failed() {
    let fx = fx().await;
    let ps = six();
    assert_eq!(build_queries(&ps).len(), 2);

    // Both RSS queries work (one with 3 items, one empty but valid): NewsAPI.ai is never called.
    mount_rss_for(&fx.server, &ps[..5], xml(RSS_OK)).await;
    mount_rss_for(&fx.server, &ps[5..], xml(RSS_EMPTY)).await;
    mount_newsapi_ok(&fx.server).await;
    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 7).await;
    assert_eq!((out.rss_ok, out.rss_failed), (2, 0));
    assert!(!out.fallback_used);
    assert_eq!(out.tokens_spent, 0);
    assert_eq!(out.hits.len(), 3);
    assert!(newsapi_calls(&fx.server).await.is_empty());
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM searches WHERE source='rss' AND ok=1"
        ),
        2
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM searches WHERE source='newsapi'"
        ),
        0
    );

    // Another candidate: the one-phrase query errors (HTTP 500) -> exactly one NewsAPI.ai call for its phrase.
    fx.server.reset().await;
    mount_rss_for(&fx.server, &ps[..5], xml(RSS_OK)).await;
    mount_rss_for(&fx.server, &ps[5..], ResponseTemplate::new(500)).await;
    mount_newsapi_ok(&fx.server).await;
    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 8).await;
    assert_eq!((out.rss_ok, out.rss_failed), (1, 1));
    assert!(out.fallback_used);
    assert_eq!(out.tokens_spent, 1);
    assert_eq!(out.hits.len(), 5); // 3 RSS + 2 NewsAPI.ai
    assert_eq!(out.hits.iter().filter(|h| h.via == Via::NewsApi).count(), 2);

    let calls = newsapi_calls(&fx.server).await;
    assert_eq!(calls.len(), 1);
    let body: Value = serde_json::from_slice(&calls[0].body).unwrap();
    assert_eq!(body["keyword"], serde_json::json!([ps[5].clone()]));
    assert_eq!(body["keywordOper"], "or");
    assert_eq!(body["articlesSortBy"], "date");
    assert_eq!(body["articlesCount"], 100);
    assert_eq!(body["resultType"], "articles");
    assert_eq!(body["dataType"], serde_json::json!(["news"]));
    assert_eq!(body["apiKey"], KEY);
    assert!(body["dateStart"].as_str().unwrap().len() == 10);

    // No remaining-tokens field in the response: the ledger counts 1 per page.
    assert_eq!(kv(&fx.conn, "newsapi_tokens_used").as_deref(), Some("1"));
    assert_eq!(
        kv(&fx.conn, "newsapi_tokens_remaining").as_deref(),
        Some("1999")
    );
}

#[tokio::test]
async fn fallback_blocked_at_200_remaining() {
    let fx = fx().await;
    let ps = six();
    set_kv(&fx.conn, "newsapi_tokens_remaining", "200");
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&fx.server)
        .await;
    mount_newsapi_ok(&fx.server).await;

    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 7).await;
    assert_eq!((out.rss_ok, out.rss_failed), (0, 2));
    assert!(!out.fallback_used);
    assert_eq!(out.tokens_spent, 0);
    assert!(out.hits.is_empty());
    assert!(newsapi_calls(&fx.server).await.is_empty());
    let blocked = events(fx.dir.path(), "newsapi_blocked");
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0]["payload"]["reason"], "remaining");

    // Called directly, the module refuses as well; one token above the reserve is allowed.
    let err = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 7, "x")
        .await
        .unwrap_err();
    assert!(
        matches!(err, NewsApiError::Cap(CapReason::Remaining)),
        "{err}"
    );
    set_kv(&fx.conn, "newsapi_tokens_remaining", "201");
    let r = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 7, "x")
        .await
        .unwrap();
    assert_eq!(r.tokens_spent, 1);
    assert_eq!(
        kv(&fx.conn, "newsapi_tokens_remaining").as_deref(),
        Some("200")
    );
}

#[tokio::test]
async fn fallback_blocked_after_100_today() {
    let fx = fx().await;
    let ps = six();
    mount_newsapi_ok(&fx.server).await;

    // 100 tokens spent yesterday do not count against today.
    for c in 0..10 {
        insert_newsapi_search(&fx.conn, 100 + c, now() - 2 * 86_400, 10);
    }
    newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 1, "x")
        .await
        .unwrap();

    // 100 tokens today across ten other candidates (the one just made counts too): refused.
    for c in 0..10 {
        insert_newsapi_search(&fx.conn, 200 + c, now(), 10);
    }
    let before = newsapi_calls(&fx.server).await.len();
    let err = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 2, "x")
        .await
        .unwrap_err();
    assert!(matches!(err, NewsApiError::Cap(CapReason::Daily)), "{err}");
    assert_eq!(newsapi_calls(&fx.server).await.len(), before);

    // Through search_all the refusal is logged and the candidate just gets no fallback hits.
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&fx.server)
        .await;
    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 3).await;
    assert!(!out.fallback_used && out.hits.is_empty());
    let blocked = events(fx.dir.path(), "newsapi_blocked");
    assert_eq!(blocked.last().unwrap()["payload"]["reason"], "daily");
}

#[tokio::test]
async fn fallback_blocked_after_10_for_one_candidate() {
    let fx = fx().await;
    let ps = six();
    mount_newsapi_ok(&fx.server).await;
    insert_newsapi_search(&fx.conn, 5, now(), 10);
    let err = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 5, "x")
        .await
        .unwrap_err();
    assert!(
        matches!(err, NewsApiError::Cap(CapReason::Candidate)),
        "{err}"
    );
    assert!(newsapi_calls(&fx.server).await.is_empty());
    // Another candidate is unaffected.
    newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &ps, 6, "x")
        .await
        .unwrap();
}

#[tokio::test]
async fn fallback_logs_trigger_error_and_tokens() {
    let fx = fx().await;
    let ps = six();
    mount_rss_for(&fx.server, &ps[..5], xml(RSS_OK)).await;
    mount_rss_for(&fx.server, &ps[5..], ResponseTemplate::new(500)).await;
    Mock::given(method("POST"))
        .and(path("/api/v1/article/getArticles"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .insert_header("x-ratelimit-remaining", "1234")
                .set_body_string(NEWSAPI_OK),
        )
        .mount(&fx.server)
        .await;

    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 9).await;
    assert!(out.fallback_used);
    assert_eq!(out.tokens_spent, 1);

    // The failed RSS query and the successful ones are rows; the fallback row has tokens and the trigger.
    let (ok, err, status): (i64, String, i64) = fx
        .conn
        .query_row(
            "SELECT ok, error, http_status FROM searches WHERE source='rss' AND ok=0",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                ))
            },
        )
        .unwrap();
    assert_eq!(ok, 0);
    assert!(!err.is_empty());
    let _ = status;
    let (tokens, n, trigger, query, raw, url): (i64, i64, String, String, String, String) = fx
        .conn
        .query_row(
            "SELECT tokens_used, n_results, trigger_error, query, raw_path, url FROM searches WHERE source='newsapi'",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(tokens, 1);
    assert_eq!(n, 2);
    assert!(
        trigger.contains("gave up") || trigger.contains("500"),
        "{trigger}"
    );
    assert!(
        trigger.contains(&q_of(&ps[5..])) || trigger.contains("topic5"),
        "{trigger}"
    );
    assert!(query.contains("topic5 alpha beta"));
    assert!(raw.starts_with("raw/"));
    assert!(!url.contains(KEY));

    // Our own count is kept; the API's remaining header is only reported in the event.
    assert_eq!(kv(&fx.conn, "newsapi_tokens_used").as_deref(), Some("1"));
    let expected = (newsapi::START_TOKENS - 1).to_string();
    assert_eq!(
        kv(&fx.conn, "newsapi_tokens_remaining").as_deref(),
        Some(expected.as_str())
    );

    // The fallback event says why, how many tokens, how many articles.
    let ev = events(fx.dir.path(), "newsapi_fallback");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0]["candidate_id"], 9);
    assert_eq!(ev[0]["payload"]["tokens"], 1);
    assert_eq!(ev[0]["payload"]["articles"], 2);
    assert_eq!(ev[0]["payload"]["header_remaining"], 1234);
    assert_eq!(ev[0]["payload"]["remaining"], newsapi::START_TOKENS - 1);
    assert!(ev[0]["payload"]["trigger_errors"][0]
        .as_str()
        .unwrap()
        .contains("topic5"));

    // The API key is in the request body only: nowhere on disk (logs, raw bodies, database).
    let mut stack = vec![fx.dir.path().to_path_buf()];
    let mut files = 0;
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                files += 1;
                let bytes = std::fs::read(&p).unwrap();
                assert!(
                    !bytes.windows(KEY.len()).any(|w| w == KEY.as_bytes()),
                    "{} contains the key",
                    p.display()
                );
            }
        }
    }
    assert!(files > 3);
}

#[tokio::test]
async fn no_key_skips_the_fallback_and_logs_it() {
    let fx = fx().await;
    let ps = six();
    Mock::given(method("GET"))
        .and(path("/news/search"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&fx.server)
        .await;
    mount_newsapi_ok(&fx.server).await;
    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, None, &ps, 4).await;
    assert_eq!(out.rss_failed, 2);
    assert!(!out.fallback_used && out.hits.is_empty());
    assert!(newsapi_calls(&fx.server).await.is_empty());
    assert_eq!(events(fx.dir.path(), "newsapi_no_key").len(), 1);
}

// ---- NewsAPI.ai parsing and errors ----

#[tokio::test]
async fn newsapi_parses_articles_and_dates() {
    let fx = fx().await;
    mount_newsapi_ok(&fx.server).await;
    let r = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &six(), 7, "x")
        .await
        .unwrap();
    assert_eq!(r.hits.len(), 2);
    let a = &r.hits[0];
    assert_eq!(a.via, Via::NewsApi);
    assert_eq!(a.title, "Fed holds rates steady");
    assert_eq!(a.link, "https://www.example.com/fed-holds?utm_source=feed");
    assert_eq!(a.source, "Example News");
    assert_eq!(a.newsapi_pub_ts, Some(1_795_011_600)); // dateTimePub 14:20:00Z
    assert_eq!(a.rss_pub_ts, None);
    assert!(a.snippet.starts_with("The Federal Reserve held"));
    // No dateTimePub: dateTime (15:01:00Z) is used.
    assert_eq!(r.hits[1].newsapi_pub_ts, Some(1_795_014_060));
}

#[tokio::test]
async fn newsapi_errors_are_not_charged() {
    let fx = fx().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/article/getArticles"))
        .respond_with(ResponseTemplate::new(401).set_body_string("The apiKey is not valid"))
        .mount(&fx.server)
        .await;
    let err = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &six(), 7, "x")
        .await
        .unwrap_err();
    assert!(
        matches!(err, NewsApiError::Status { status: 401, .. }),
        "{err}"
    );
    assert_eq!(kv(&fx.conn, "newsapi_tokens_used"), None);
    // The failed attempt is still logged, with no tokens.
    let (ok, tokens): (i64, i64) = fx
        .conn
        .query_row(
            "SELECT ok, COALESCE(tokens_used, 0) FROM searches WHERE source='newsapi'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((ok, tokens), (0, 0));

    // HTTP 200 carrying an "error" object is an error too.
    fx.server.reset().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/article/getArticles"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string(r#"{"error":"out of tokens"}"#),
        )
        .mount(&fx.server)
        .await;
    let err = newsapi::search(&fx.http, &fx.conn, &fx.ep, KEY, &six(), 7, "x")
        .await
        .unwrap_err();
    assert!(matches!(err, NewsApiError::Parse { .. }), "{err}");
    assert_eq!(kv(&fx.conn, "newsapi_tokens_used"), None);
}

// ---- dedupe ----

#[test]
fn canonical_link_lowercases_host_drops_fragment_and_utm() {
    assert_eq!(
        canonical_link("HTTPS://WWW.Example.COM/Path?id=5&utm_source=x&UTM_Medium=y&b=2#frag"),
        "https://www.example.com/Path?id=5&b=2"
    );
    assert_eq!(
        canonical_link("https://example.com/a?utm_campaign=z"),
        "https://example.com/a"
    );
    // A Bing click link stands for its publisher URL; the click link's own parameters do not matter.
    assert_eq!(
        canonical_link(
            "http://WWW.bing.com/news/apiclick.aspx?ref=FexRss&tid=1&url=https%3a%2f%2fExample.com%2fa%3futm_source%3dz%26id%3d5&c=9"
        ),
        "https://example.com/a?id=5"
    );
    // Other Bing and Google URLs are left as they are.
    assert_eq!(
        canonical_link("https://news.google.com/rss/articles/CBMi?oc=5"),
        "https://news.google.com/rss/articles/CBMi?oc=5"
    );
    assert_eq!(canonical_link("not a url"), "not a url");
}

#[tokio::test]
async fn search_all_dedupes_by_canonical_link() {
    let fx = fx().await;
    let ps = six();
    // The second query returns the same articles with another click id and an upper-case Bing host.
    let tracked = RSS_OK
        .replace("tid=aaaa1111", "tid=bbbb2222")
        .replace("http://www.bing.com/news", "http://WWW.Bing.com/news");
    mount_rss_for(&fx.server, &ps[..5], xml(RSS_OK)).await;
    mount_rss_for(&fx.server, &ps[5..], xml(&tracked)).await;
    let out = search_all(&fx.http, &fx.conn, &fx.log, &fx.ep, Some(KEY), &ps, 7).await;
    assert_eq!(out.rss_ok, 2);
    assert_eq!(out.hits.len(), 3);
    // Both requests are logged with their item counts.
    assert_eq!(
        count(
            &fx.conn,
            "SELECT SUM(n_results) FROM searches WHERE source='rss'"
        ),
        6
    );
}

// ---- Bing click links ----

#[test]
fn bing_publisher_url_is_the_plain_url_parameter() {
    let u = |s: &str| reqwest::Url::parse(s).unwrap();
    let click = |target: &str| {
        format!("http://www.bing.com/news/apiclick.aspx?ref=FexRss&aid=&tid=t&url={target}&c=1&mkt=en-us")
    };
    assert_eq!(
        bing_publisher_url(&u(&click(
            "https%3a%2f%2fapnews.com%2farticle%2fabc%3fx%3d1"
        ))),
        Some("https://apnews.com/article/abc?x=1".to_string())
    );
    assert_eq!(
        bing_publisher_url(&u(&click("http%3a%2f%2fexample.com%2fa"))),
        Some("http://example.com/a".to_string())
    );
    // Refused: not http(s), a Bing target, a target that is not a URL, no url parameter, another path or host.
    for bad in [
        "ftp%3a%2f%2fexample.com%2fa",
        "javascript%3aalert(1)",
        "https%3a%2f%2fwww.bing.com%2fnews%2fx",
        "not+a+url",
    ] {
        assert_eq!(bing_publisher_url(&u(&click(bad))), None, "{bad}");
    }
    assert_eq!(
        bing_publisher_url(&u("http://www.bing.com/news/apiclick.aspx?ref=FexRss")),
        None
    );
    assert_eq!(
        bing_publisher_url(&u(
            "http://www.bing.com/search?url=https%3a%2f%2fexample.com"
        )),
        None
    );
    assert_eq!(
        bing_publisher_url(&u(
            "http://example.com/news/apiclick.aspx?url=https%3a%2f%2fexample.com"
        )),
        None
    );
}

#[test]
fn bing_format_sample_parses_and_every_link_resolves() {
    // Synthetic sample (fake titles and URLs) in the structure of a live Bing response, checked 2026-11-19.
    let real = include_str!("fixtures/news/bing_sample.xml");
    let hits = rss::parse_feed(real).unwrap();
    assert_eq!(
        hits.len(),
        4,
        "the channel's own <link> and <image> are not items"
    );
    for h in &hits {
        assert!(
            h.link
                .starts_with("http://www.bing.com/news/apiclick.aspx?"),
            "{}",
            h.link
        );
        assert!(h.rss_pub_ts.is_some(), "{}", h.title);
        assert!(!h.source.is_empty() && !h.snippet.is_empty() && !h.title.is_empty());
        assert_eq!(h.via, Via::Rss);
        let url = reqwest::Url::parse(&h.link).unwrap();
        let publisher = bing_publisher_url(&url).expect("publisher url");
        assert!(publisher.starts_with("https://"), "{publisher}");
        assert!(!publisher.contains("bing.com"));
    }
    // "Thu, 19 Nov 2026 09:00:00 GMT" is 09:00 Pacific = 17:00 UTC.
    assert_eq!(hits[0].rss_pub_ts, Some(1_795_107_600));
    assert_eq!(hits[0].source, "Sample Wire");
}

#[test]
fn bing_pub_date_is_pacific_wall_time_labelled_gmt() {
    let ts = rss::parse_pub_date;
    // Standard time (+8): the live-checked case.
    assert_eq!(ts("Thu, 19 Nov 2026 09:00:00 GMT"), Some(1_795_107_600));
    // Daylight time (+7) in summer: 12:00 Pacific is 19:00 UTC.
    assert_eq!(ts("Mon, 15 Jun 2026 12:00:00 GMT"), Some(1_781_550_000));
    // 2027: daylight time starts Sunday 14 March at 02:00 and ends Sunday 7 November at 02:00 (Pacific).
    assert_eq!(ts("Sun, 14 Mar 2027 01:59:00 GMT"), Some(1_805_018_340)); // 09:59 UTC (+8)
    assert_eq!(ts("Sun, 14 Mar 2027 03:00:00 GMT"), Some(1_805_018_400)); // 10:00 UTC (+7)
    assert_eq!(ts("Sun, 07 Nov 2027 01:59:00 GMT"), Some(1_825_577_940)); // 08:59 UTC (+7)
    assert_eq!(ts("Sun, 07 Nov 2027 02:00:00 GMT"), Some(1_825_581_600)); // 10:00 UTC (+8)
    assert_eq!(ts("not a date"), None);
    // A zero offset in any spelling gets the Pacific reading; another stated offset is trusted as written.
    assert_eq!(ts("Thu, 19 Nov 2026 09:00:00 +0000"), Some(1_795_107_600));
    assert_eq!(ts("Thu, 19 Nov 2026 09:00:00 UT"), Some(1_795_107_600));
    assert_eq!(ts("Thu, 19 Nov 2026 09:00:00 -0500"), Some(1_795_096_800)); // 14:00 UTC
    assert_eq!(ts("Thu, 19 Nov 2026 09:00:00 +0100"), Some(1_795_075_200)); // 08:00 UTC
}
