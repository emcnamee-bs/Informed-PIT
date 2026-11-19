use std::cell::Cell;
use std::collections::HashMap;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::hostguard::HostGuard;
use informed_pit::http::{Http, HttpError, Service};
use informed_pit::news::article::{self, collect, resolve_link, KeptArticle, MAX_TEXT_CHARS};
use informed_pit::news::pubtime::{
    date_article, extract_pub_ts, in_window, is_date_only, parse_time, parse_time_detail, Dated,
    TimeSource,
};
use informed_pit::news::wayback;
use informed_pit::news::{Hit, NewsEndpoints, Via};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const OG: &str = include_str!("fixtures/articles/og_time.html");
const JSONLD: &str = include_str!("fixtures/articles/jsonld.html");
const TIME_TAG: &str = include_str!("fixtures/articles/time_tag.html");
const NO_TIME: &str = include_str!("fixtures/articles/no_time.html");

/// 2026-11-18 14:30:00 UTC, the `article:published_time` of og_time.html.
const OG_TS: i64 = 1_795_012_200;
/// 2026-11-18 13:05:10 UTC, the JSON-LD `datePublished`.
const JSONLD_TS: i64 = 1_795_007_110;
/// 2026-11-18 12:00:00 UTC, the `<time datetime>` (no offset, so UTC).
const TIME_TAG_TS: i64 = 1_795_003_200;
/// Move started 14:00, investigation at 15:00; the window opens at 13:45.
const MOVE_START: i64 = 1_795_010_400;
const NOW: i64 = 1_795_014_000;
const CAND: i64 = 1;

// ----------------------------------------------------------------------------------------------------------
// Pure tests
// ----------------------------------------------------------------------------------------------------------

#[test]
fn og_time_extracted() {
    // `og:updated_time` (16:00) is later in the order and must not win.
    assert_eq!(
        extract_pub_ts(OG),
        Some((OG_TS, "meta:article:published_time"))
    );
}

#[test]
fn jsonld_date_extracted() {
    // `dateModified` is ignored; the date sits inside an @graph array.
    assert_eq!(
        extract_pub_ts(JSONLD),
        Some((JSONLD_TS, "jsonld:datePublished"))
    );
    let plain = r#"<script type="application/ld+json">{"@type":"NewsArticle","datePublished":"2026-11-18T12:00:00+00:00"}</script>"#;
    assert_eq!(
        extract_pub_ts(plain),
        Some((TIME_TAG_TS, "jsonld:datePublished"))
    );
    let list = r#"<script type="application/ld+json">[{"@type":"Org"},{"datePublished":"2026-11-18T12:00:00Z"}]</script>"#;
    assert_eq!(
        extract_pub_ts(list),
        Some((TIME_TAG_TS, "jsonld:datePublished"))
    );
    let broken = r#"<script type="application/ld+json">{not json</script><time datetime="2026-11-18T12:00:00Z">x</time>"#;
    assert_eq!(extract_pub_ts(broken), Some((TIME_TAG_TS, "time:datetime")));
}

#[test]
fn date_only_values_are_tagged_and_lose_to_any_full_time() {
    // A bare date comes back (midnight UTC) only when nothing has a time of day, tagged `:date_only`.
    let only = r#"<script type="application/ld+json">{"datePublished":"2026-11-18"}</script>"#;
    let (ts, tag) = extract_pub_ts(only).unwrap();
    assert_eq!((ts, tag), (1_794_960_000, "jsonld:datePublished:date_only"));
    assert!(is_date_only(tag));
    let meta = r#"<meta property="article:published_time" content="2026-11-18">"#;
    assert_eq!(
        extract_pub_ts(meta),
        Some((1_794_960_000, "meta:article:published_time:date_only"))
    );
    // A full time from a lower-priority source beats a date from a higher one.
    let both = r#"<meta property="article:published_time" content="2026-11-18"><time datetime="2026-11-18T12:00:00Z">x</time>"#;
    assert_eq!(extract_pub_ts(both), Some((TIME_TAG_TS, "time:datetime")));
    assert!(!is_date_only("time:datetime"));
    assert_eq!(parse_time_detail("2026-11-18"), Some((1_794_960_000, true)));
    assert_eq!(
        parse_time_detail("2026-11-18T00:00:00Z"),
        Some((1_794_960_000, false))
    );
}

#[test]
fn time_tag_extracted() {
    assert_eq!(
        extract_pub_ts(TIME_TAG),
        Some((TIME_TAG_TS, "time:datetime"))
    );
}

#[test]
fn no_time_returns_none() {
    assert_eq!(extract_pub_ts(NO_TIME), None);
    // An unparseable value is no time either.
    let junk = r#"<meta property="article:published_time" content="yesterday">"#;
    assert_eq!(extract_pub_ts(junk), None);
}

#[test]
fn other_meta_tags_come_last() {
    let h = r#"<meta name="date" content="2026-11-18T12:00:00Z"><meta property="og:updated_time" content="2026-11-18T13:00:00Z">"#;
    assert_eq!(
        extract_pub_ts(h),
        Some((1_795_006_800, "meta:og:updated_time"))
    );
    let h = r#"<meta name="pubdate" content="2026-11-18T12:00:00Z">"#;
    assert_eq!(extract_pub_ts(h), Some((TIME_TAG_TS, "meta:pubdate")));
}

#[test]
fn time_formats() {
    assert_eq!(parse_time("2026-11-18T14:30:00Z"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18T09:30:00-05:00"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18T14:30:00+0000"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18T14:30:00"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18 14:30:00"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18T14:30:00.250Z"), Some(OG_TS));
    assert_eq!(parse_time("Wed, 18 Nov 2026 14:30:00 GMT"), Some(OG_TS));
    assert_eq!(parse_time("2026-11-18"), Some(1_794_960_000));
    assert_eq!(parse_time(""), None);
    assert_eq!(parse_time("soon"), None);
}

#[test]
fn metadata_is_primary() {
    let called = Cell::new(false);
    let d = date_article(Some(1000), Some(1000 + 3600), Some(5), 2000, || {
        called.set(true);
        Some(7)
    });
    assert_eq!(
        d,
        Dated {
            ts: Some(1000),
            source: TimeSource::Metadata
        }
    );
    assert!(
        !called.get(),
        "wayback must not run when metadata is usable"
    );
    // No RSS pubDate to disagree with, and 5 minutes of future slack.
    let d = date_article(Some(2000 + 300), None, None, 2000, || None);
    assert_eq!(d.source, TimeSource::Metadata);
    // Exactly 6 h from RSS still agrees.
    let d = date_article(Some(1000), Some(1000 + 6 * 3600), None, 5000, || None);
    assert_eq!(d.source, TimeSource::Metadata);
}

#[test]
fn future_meta_falls_back_to_wayback() {
    let d = date_article(Some(2000 + 301), None, None, 2000, || Some(1500));
    assert_eq!(
        d,
        Dated {
            ts: Some(1500),
            source: TimeSource::Wayback
        }
    );
}

#[test]
fn meta_far_from_rss_falls_back_to_wayback() {
    let d = date_article(Some(1000), Some(1000 + 6 * 3600 + 1), None, 100_000, || {
        Some(900)
    });
    assert_eq!(
        d,
        Dated {
            ts: Some(900),
            source: TimeSource::Wayback
        }
    );
}

#[test]
fn newsapi_time_comes_before_wayback() {
    let called = Cell::new(false);
    let d = date_article(None, Some(1), Some(1234), 5000, || {
        called.set(true);
        Some(1)
    });
    assert_eq!(
        d,
        Dated {
            ts: Some(1234),
            source: TimeSource::NewsApiPub
        }
    );
    assert!(!called.get());
    // Missing metadata falls through the same way.
    assert_eq!(
        date_article(None, None, None, 5000, || Some(9)).source,
        TimeSource::Wayback
    );
}

#[test]
fn no_time_gives_none() {
    let d = date_article(None, Some(1000), None, 5000, || None);
    assert_eq!(
        d,
        Dated {
            ts: None,
            source: TimeSource::None
        }
    );
}

#[test]
fn window_boundaries() {
    let (start, inv) = (MOVE_START, NOW);
    assert!(in_window(start - 900, start, inv));
    assert!(!in_window(start - 901, start, inv));
    assert!(in_window(inv, start, inv));
    assert!(!in_window(inv + 1, start, inv));
}

#[test]
fn text_extraction_drops_boilerplate() {
    let page = article::extract_text(OG);
    assert!(page.0.starts_with("Fed holds rates steady"));
    assert!(page
        .1
        .contains("left its benchmark interest rate unchanged"));
    assert!(!page.1.contains("Home | Markets"), "nav dropped");
    assert!(!page.1.contains("Copyright"), "footer dropped");
    assert!(!page.1.contains("tracking"), "script dropped");
    assert_eq!(page.1.matches("\n\n").count(), 2, "three paragraphs");
}

// ----------------------------------------------------------------------------------------------------------
// Fixtures for the network tests
// ----------------------------------------------------------------------------------------------------------

struct Fx {
    dir: TempDir,
    conn: Connection,
    log: EventLog,
    http: Http,
    /// Stands for the news search host (`rss_base`); nothing is ever requested from it by `collect`.
    google: MockServer,
    /// Plays the publishers and the Wayback CDX API.
    site: MockServer,
    ep: NewsEndpoints,
}

async fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts) \
         VALUES (1, 'EV', 'yes', ?1, 90, 'investigating', ?1, ?1)",
        [MOVE_START],
    )
    .unwrap();
    let mut spacing = HashMap::new();
    spacing.insert(Service::Wayback, Duration::from_millis(1));
    // The mock servers are on 127.0.0.1, which the production guard refuses; everything else stays blocked.
    let http = Http::with_spacing(EventLog::new(dir.path()), spacing)
        .with_backoff_base(Duration::from_millis(1))
        .with_article_guard(HostGuard::allowing_loopback());
    let google = MockServer::start().await;
    let site = MockServer::start().await;
    let ep = NewsEndpoints {
        rss_base: google.uri(),
        newsapi_base: google.uri(),
        wayback_base: site.uri(),
    };
    Fx {
        log: EventLog::new(dir.path()),
        dir,
        conn,
        http,
        google,
        site,
        ep,
    }
}

fn html(body: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body.to_string(), "text/html; charset=utf-8")
}

fn cdx(rows: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(rows.to_string(), "application/json")
}

fn hit(link: String) -> Hit {
    Hit {
        title: "Fed holds rates steady - Example".to_string(),
        link,
        source: "Example".to_string(),
        rss_pub_ts: None,
        snippet: "Fed holds rates steady Example".to_string(),
        via: Via::Rss,
        newsapi_pub_ts: None,
    }
}

/// A Bing News click link to `publisher_url`, as the RSS search returns them.
fn click_link(publisher_url: &str) -> String {
    let target = reqwest::Url::parse_with_params("http://x.test/", [("u", publisher_url)])
        .unwrap()
        .query()
        .unwrap()
        .trim_start_matches("u=")
        .to_string();
    format!(
        "http://www.bing.com/news/apiclick.aspx?ref=FexRss&aid=&tid=t1&url={target}&c=1&mkt=en-us"
    )
}

/// An old-style Google News link: the publisher URL sits in the decoded path token.
fn embedded_google_link(publisher_url: &str) -> String {
    let mut bytes = vec![0x08, 0x13, 0x22, publisher_url.len() as u8];
    bytes.extend_from_slice(publisher_url.as_bytes());
    bytes.extend_from_slice(&[0xd2, 0x01, 0x00]);
    format!(
        "https://news.google.com/rss/articles/{}?oc=5",
        URL_SAFE_NO_PAD.encode(bytes)
    )
}

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

struct ArticleRow {
    decision: String,
    reason: Option<String>,
    time_source: Option<String>,
    publish_ts: Option<i64>,
    meta_pub_ts: Option<i64>,
    meta_source: Option<String>,
    wayback_ts: Option<i64>,
    text_unavailable: bool,
    resolved_url: Option<String>,
    final_url: Option<String>,
}

fn row(conn: &Connection, url: &str) -> ArticleRow {
    conn.query_row(
        "SELECT decision, reason, time_source, publish_ts, meta_pub_ts, meta_source, wayback_ts, \
         text_unavailable, resolved_url, final_url FROM articles WHERE candidate_id = 1 AND url = ?1",
        [url],
        |r| {
            Ok(ArticleRow {
                decision: r.get(0)?,
                reason: r.get(1)?,
                time_source: r.get(2)?,
                publish_ts: r.get(3)?,
                meta_pub_ts: r.get(4)?,
                meta_source: r.get(5)?,
                wayback_ts: r.get(6)?,
                text_unavailable: r.get(7)?,
                resolved_url: r.get(8)?,
                final_url: r.get(9)?,
            })
        },
    )
    .unwrap_or_else(|e| panic!("no articles row for {url}: {e}"))
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

async fn run_collect(fx: &Fx, hits: &[Hit]) -> Vec<KeptArticle> {
    collect(
        &fx.http, &fx.ep, &fx.conn, &fx.log, CAND, hits, MOVE_START, NOW,
    )
    .await
}

// ----------------------------------------------------------------------------------------------------------
// Link resolution
// ----------------------------------------------------------------------------------------------------------

#[test]
fn publisher_link_is_returned_unchanged() {
    let u = resolve_link("https://example.com/a?x=1").unwrap();
    assert_eq!(u, "https://example.com/a?x=1");
}

#[test]
fn bing_click_link_resolves_to_its_url_parameter() {
    let link = "http://www.bing.com/news/apiclick.aspx?ref=FexRss&aid=&tid=6aff&url=https%3a%2f%2fapnews.com%2farticle%2fabc-123&c=8922&mkt=en-us";
    assert_eq!(
        resolve_link(link).unwrap(),
        "https://apnews.com/article/abc-123"
    );
    assert_eq!(
        resolve_link(&click_link("https://www.reuters.com/a/b?c=d&e=f")).unwrap(),
        "https://www.reuters.com/a/b?c=d&e=f"
    );
    // A Bing link without a usable publisher URL is an error, never fetched.
    for bad in [
        "http://www.bing.com/news/apiclick.aspx?ref=FexRss",
        "http://www.bing.com/news/apiclick.aspx?url=ftp%3a%2f%2fx.test%2fa",
        "http://www.bing.com/news/apiclick.aspx?url=https%3a%2f%2fwww.bing.com%2fnews",
        "https://www.bing.com/search?q=x",
    ] {
        assert!(resolve_link(bad).is_err(), "{bad}");
    }
}

#[test]
fn google_link_resolves_only_from_an_embedded_url() {
    let link = embedded_google_link("https://www.example.com/fed-holds");
    assert_eq!(
        resolve_link(&link).unwrap(),
        "https://www.example.com/fed-holds"
    );
    // The token format of the saved Google RSS fixture decodes the same way.
    let fixture =
        "https://news.google.com/rss/articles/CBMiOGh0dHBzOi8vZXhhbXBsZS5jb20vZmVkLWhvbGRz?oc=5";
    assert_eq!(
        resolve_link(fixture).unwrap(),
        "https://example.com/fed-holds"
    );
    // The opaque token every live link carried (2026-11-18) is an error.
    let opaque = "https://news.google.com/rss/articles/CBMigwFBVV95cUxPblE2b1lhT0VscmhqWHlpOWZJendmRmlQZFFPVUZFc0JENk1SODRoclpqc0QtWk14dXB4TkZNaEpPaF83VVBsb2VvekhOQjl6X0IyNWpxdDVscm05OUxJZHRkYzFUa2s0UG1vLVhZVl9kRHBBVHlTOENfSUxIdlBpQ1hfdw?oc=5";
    let err = resolve_link(opaque).unwrap_err();
    assert!(err.to_string().contains("did not resolve"), "{err}");
}

// ----------------------------------------------------------------------------------------------------------
// Wayback
// ----------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn wayback_earliest_capture_request_and_parse() {
    let fx = fx().await;
    Mock::given(method("GET"))
        .and(path("/cdx/search/cdx"))
        .and(query_param("url", "https://example.com/a"))
        .and(query_param("output", "json"))
        .and(query_param("limit", "1"))
        .and(query_param("fl", "timestamp"))
        .respond_with(cdx(r#"[["timestamp"],["20261118141500"]]"#))
        .expect(1)
        .mount(&fx.site)
        .await;
    let ts = wayback::earliest_capture(&fx.http, &fx.ep, "https://example.com/a", CAND)
        .await
        .unwrap();
    assert_eq!(ts, Some(1_795_011_300)); // 14:15:00 UTC
                                         // No lower date limit: the earliest capture ever is what counts (ruling R16).
    let reqs = fx.site.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        reqs[0].url.query_pairs().all(|(k, _)| k != "from"),
        "{}",
        reqs[0].url
    );
}

#[tokio::test]
async fn wayback_no_capture_and_errors() {
    let fx = fx().await;
    Mock::given(path("/cdx/search/cdx"))
        .and(query_param("url", "https://none.test/"))
        .respond_with(cdx("[]"))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .and(query_param("url", "https://empty.test/"))
        .respond_with(cdx(""))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .and(query_param("url", "https://bad.test/"))
        .respond_with(cdx("<html>nope</html>"))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .and(query_param("url", "https://forbidden.test/"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&fx.site)
        .await;
    let ask = |u: &'static str| {
        let fx = &fx;
        async move { wayback::earliest_capture(&fx.http, &fx.ep, u, CAND).await }
    };
    assert_eq!(ask("https://none.test/").await.unwrap(), None);
    assert_eq!(ask("https://empty.test/").await.unwrap(), None);
    assert!(ask("https://bad.test/").await.is_err());
    assert!(ask("https://forbidden.test/").await.is_err());
}

// ----------------------------------------------------------------------------------------------------------
// collect
// ----------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn metadata_time_keeps_article_and_skips_wayback() {
    let fx = fx().await;
    Mock::given(path("/story/og"))
        .respond_with(html(OG))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .expect(0)
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/og", fx.site.uri()));
    let mut h = hit(link.clone());
    h.rss_pub_ts = Some(OG_TS - 600); // agrees with the page
    let kept = run_collect(&fx, &[h]).await;

    assert_eq!(kept.len(), 1);
    let via: String = fx
        .conn
        .query_row("SELECT via FROM articles", [], |r| r.get(0))
        .unwrap();
    assert_eq!(via, "rss");
    let k = &kept[0];
    assert_eq!(k.used_ts, OG_TS);
    assert_eq!(k.time_source, TimeSource::Metadata);
    assert!(!k.text_unavailable);
    assert!(k
        .text
        .contains("left its benchmark interest rate unchanged"));
    assert!(k.text.chars().count() <= MAX_TEXT_CHARS);
    assert_eq!(k.title, "Fed holds rates steady");
    assert_eq!(k.url, format!("{}/story/og", fx.site.uri()));
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "kept");
    assert_eq!(r.time_source.as_deref(), Some("metadata"));
    assert_eq!(r.publish_ts, Some(OG_TS));
    assert_eq!(r.meta_pub_ts, Some(OG_TS));
    assert_eq!(
        r.meta_source.as_deref(),
        Some("meta:article:published_time")
    );
    assert_eq!(r.wayback_ts, None);
    assert!(r.resolved_url.is_some() && r.final_url.is_some());
    assert_eq!(count(&fx.conn, "SELECT COUNT(*) FROM wayback_checks"), 0);
}

#[tokio::test]
async fn article_before_the_window_is_out_of_window() {
    let fx = fx().await;
    Mock::given(path("/story/old"))
        .respond_with(html(JSONLD)) // 13:05, window opens 13:45
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/old", fx.site.uri()));
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert!(kept.is_empty());
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "out_of_window");
    assert_eq!(r.publish_ts, Some(JSONLD_TS));
    assert_eq!(r.time_source.as_deref(), Some("metadata"));
    assert_eq!(r.meta_source.as_deref(), Some("jsonld:datePublished"));
}

#[tokio::test]
async fn article_after_the_investigation_is_out_of_window() {
    let fx = fx().await;
    let late = format!(
        r#"<meta property="article:published_time" content="2026-11-18T15:00:01Z"><p>{}</p>"#,
        "x".repeat(300)
    );
    Mock::given(path("/story/late"))
        .respond_with(html(&late))
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/late", fx.site.uri()));
    // 15:00:01 is one second past NOW, and within the +5 min allowance for metadata, so it is dated and
    // then rejected by the window test.
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert!(kept.is_empty());
    assert_eq!(row(&fx.conn, &link).decision, "out_of_window");
}

#[tokio::test]
async fn no_time_anywhere_is_excluded_and_logged() {
    let fx = fx().await;
    Mock::given(path("/story/none"))
        .respond_with(html(NO_TIME))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .expect(1)
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/none", fx.site.uri()));
    let mut h = hit(link.clone());
    h.rss_pub_ts = Some(OG_TS); // RSS alone never dates an article
    let kept = run_collect(&fx, &[h]).await;

    assert!(kept.is_empty());
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "no_time");
    assert_eq!(r.time_source.as_deref(), Some("none"));
    assert_eq!(r.publish_ts, None);
    assert!(r.reason.unwrap().contains("no publish time"));
    let (ok, cap): (bool, Option<i64>) = fx
        .conn
        .query_row("SELECT ok, capture_ts FROM wayback_checks", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert!(ok);
    assert_eq!(cap, None);
    let ev = events(fx.dir.path(), "article_dated");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0]["payload"]["decision"], "no_time");
    assert_eq!(ev[0]["payload"]["time_source"], "none");
    assert_eq!(ev[0]["payload"]["rss_pub_ts"], OG_TS);
}

#[tokio::test]
async fn wayback_backs_up_missing_metadata() {
    let fx = fx().await;
    Mock::given(path("/story/wb"))
        .respond_with(html(NO_TIME))
        .mount(&fx.site)
        .await;
    let target = format!("{}/story/wb", fx.site.uri());
    Mock::given(path("/cdx/search/cdx"))
        .and(query_param("url", target.as_str()))
        .respond_with(cdx(r#"[["timestamp"],["20261118141500"]]"#))
        .expect(1)
        .mount(&fx.site)
        .await;
    let link = click_link(&target);
    let kept = run_collect(&fx, &[hit(link.clone())]).await;

    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].time_source, TimeSource::Wayback);
    assert_eq!(kept[0].used_ts, 1_795_011_300);
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "kept");
    assert_eq!(r.time_source.as_deref(), Some("wayback"));
    assert_eq!(r.wayback_ts, Some(1_795_011_300));
    assert_eq!(r.meta_pub_ts, None);
    let cap: Option<i64> = fx
        .conn
        .query_row("SELECT capture_ts FROM wayback_checks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cap, Some(1_795_011_300));
}

#[tokio::test]
async fn future_metadata_uses_wayback() {
    let fx = fx().await;
    let page = format!(
        r#"<meta property="article:published_time" content="2026-11-19T09:00:00Z"><p>{}</p>"#,
        "y".repeat(300)
    );
    Mock::given(path("/story/future"))
        .respond_with(html(&page))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx(r#"[["timestamp"],["20261118140000"]]"#))
        .expect(1)
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/future", fx.site.uri()));
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].time_source, TimeSource::Wayback);
    let r = row(&fx.conn, &link);
    assert_eq!(r.meta_pub_ts, Some(1_795_078_800)); // recorded even though not used
    assert_eq!(r.wayback_ts, Some(MOVE_START));
}

#[tokio::test]
async fn newsapi_time_is_used_before_wayback() {
    let fx = fx().await;
    Mock::given(path("/story/na"))
        .respond_with(html(NO_TIME))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .expect(0)
        .mount(&fx.site)
        .await;
    let link = format!("{}/story/na", fx.site.uri()); // not a Google link: used as is
    let mut h = hit(link.clone());
    h.via = Via::NewsApi;
    h.newsapi_pub_ts = Some(MOVE_START + 60);
    let kept = run_collect(&fx, &[h]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].time_source, TimeSource::NewsApiPub);
    assert_eq!(kept[0].used_ts, MOVE_START + 60);
    // The row records which search found it (spec 8.3: measure the fallback).
    let via: String = fx
        .conn
        .query_row("SELECT via FROM articles", [], |r| r.get(0))
        .unwrap();
    assert_eq!(via, "newsapi");
}

#[tokio::test]
async fn paywalled_page_kept_with_snippet_flagged() {
    let fx = fx().await;
    Mock::given(path("/story/blocked"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&fx.site)
        .await;
    let stub = r#"<meta property="article:published_time" content="2026-11-18T14:00:00Z"><p>Subscribe to keep reading.</p>"#;
    Mock::given(path("/story/stub"))
        .respond_with(html(stub))
        .mount(&fx.site)
        .await;
    let mut blocked = hit(format!("{}/story/blocked", fx.site.uri()));
    blocked.via = Via::NewsApi;
    blocked.newsapi_pub_ts = Some(MOVE_START + 120);
    blocked.snippet = "Fed holds rates, sources say".to_string();
    let stubbed = hit(click_link(&format!("{}/story/stub", fx.site.uri())));

    let kept = run_collect(&fx, &[blocked.clone(), stubbed.clone()]).await;
    assert_eq!(kept.len(), 2);
    for k in &kept {
        assert!(k.text_unavailable);
    }
    let b = kept
        .iter()
        .find(|k| k.url.ends_with("/story/blocked"))
        .unwrap();
    assert_eq!(b.text, "Fed holds rates, sources say");
    let s = kept
        .iter()
        .find(|k| k.url.ends_with("/story/stub"))
        .unwrap();
    assert_eq!(s.text, "Fed holds rates steady Example");
    assert_eq!(s.time_source, TimeSource::Metadata);
    let r = row(&fx.conn, &blocked.link);
    assert_eq!(r.decision, "kept");
    assert!(r.text_unavailable);
    assert!(r.reason.unwrap().contains("403"));
    assert!(row(&fx.conn, &stubbed.link).text_unavailable);
}

#[tokio::test]
async fn failed_fetch_with_no_time_is_fetch_failed() {
    let fx = fx().await;
    Mock::given(path("/story/gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/gone", fx.site.uri()));
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert!(kept.is_empty());
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "fetch_failed");
    assert!(r.reason.unwrap().contains("404"));
}

#[tokio::test]
async fn unresolved_google_link_is_excluded_logged_and_never_fetched() {
    let fx = fx().await;
    let link = "https://news.google.com/rss/articles/AU_yqLOPAQUE?oc=5".to_string();
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert!(kept.is_empty());
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "unresolved");
    assert!(r.resolved_url.is_none());
    assert!(r.reason.unwrap().contains("did not resolve"));
    let ev = events(fx.dir.path(), "article_dated");
    assert_eq!(ev[0]["payload"]["decision"], "unresolved");
    // No request of any kind went out (nothing is mounted anywhere; the http log would show one).
    assert!(fx.site.received_requests().await.unwrap().is_empty());
    assert!(fx.google.received_requests().await.unwrap().is_empty());
    assert!(!fx.dir.path().join("raw").exists(), "no body was fetched");
}

#[tokio::test]
async fn old_style_google_link_is_still_collected() {
    let fx = fx().await;
    Mock::given(path("/story/og"))
        .respond_with(html(OG))
        .mount(&fx.site)
        .await;
    let link = embedded_google_link(&format!("{}/story/og", fx.site.uri()));
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(row(&fx.conn, &link).decision, "kept");
}

#[tokio::test]
async fn two_links_to_the_same_page_are_a_dup() {
    let fx = fx().await;
    Mock::given(path("/story/og"))
        .respond_with(html(OG))
        .expect(1)
        .mount(&fx.site)
        .await;
    let target = format!("{}/story/og", fx.site.uri());
    let a = hit(click_link(&target));
    let b = hit(click_link(&format!("{target}?utm_source=x")));
    let kept = run_collect(&fx, &[a.clone(), b.clone()]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(row(&fx.conn, &a.link).decision, "kept");
    assert_eq!(row(&fx.conn, &b.link).decision, "dup");
}

#[tokio::test]
async fn cap_40_keeps_newest() {
    let fx = fx().await;
    Mock::given(path_regex("^/a/"))
        .respond_with(html(NO_TIME))
        .mount(&fx.site)
        .await;
    let hits: Vec<Hit> = (0..45)
        .map(|i| {
            let mut h = hit(format!("{}/a/{i}", fx.site.uri()));
            h.via = Via::NewsApi;
            // Article i is published i minutes after the window opens; all are inside it.
            h.newsapi_pub_ts = Some(MOVE_START - 900 + i * 60);
            h
        })
        .collect();
    let kept = run_collect(&fx, &hits).await;

    assert_eq!(kept.len(), 40);
    // Newest first: articles 44 down to 5.
    assert_eq!(kept[0].used_ts, MOVE_START - 900 + 44 * 60);
    assert_eq!(kept[39].used_ts, MOVE_START - 900 + 5 * 60);
    assert!(kept.windows(2).all(|w| w[0].used_ts >= w[1].used_ts));
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM articles WHERE decision = 'over_cap'"
        ),
        5
    );
    assert_eq!(
        count(
            &fx.conn,
            "SELECT COUNT(*) FROM articles WHERE decision = 'kept'"
        ),
        40
    );
    // The dropped ones are the five oldest.
    let oldest_kept = MOVE_START - 900 + 5 * 60;
    assert_eq!(
        count(
            &fx.conn,
            &format!("SELECT COUNT(*) FROM articles WHERE decision = 'over_cap' AND publish_ts < {oldest_kept}")
        ),
        5
    );
}

#[tokio::test]
async fn text_is_cut_to_20000_chars() {
    let fx = fx().await;
    let long = format!(
        r#"<meta property="article:published_time" content="2026-11-18T14:00:00Z"><p>{}</p>"#,
        "word ".repeat(10_000)
    );
    Mock::given(path("/story/long"))
        .respond_with(html(&long))
        .mount(&fx.site)
        .await;
    let page = article::fetch(&fx.http, &format!("{}/story/long", fx.site.uri()), CAND)
        .await
        .unwrap();
    assert_eq!(page.text.chars().count(), MAX_TEXT_CHARS);
}

/// A server that answers every request with a chunked 200 of `total` bytes and no `Content-Length`.
/// Returns its base URL.
async fn chunked_server(total: usize) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let head = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
                if sock.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                let chunk = vec![b'z'; 64 * 1024];
                let mut sent = 0;
                while sent < total {
                    let n = chunk.len().min(total - sent);
                    let frame = [format!("{n:x}\r\n").as_bytes(), &chunk[..n], b"\r\n"].concat();
                    if sock.write_all(&frame).await.is_err() {
                        return; // the client stopped reading at its limit
                    }
                    sent += n;
                }
                let _ = sock.write_all(b"0\r\n\r\n").await;
            });
        }
    });
    format!("http://{addr}")
}

fn raw_len(dir: &std::path::Path, raw_path: &str) -> usize {
    use std::io::Read;
    let file = std::fs::File::open(dir.join(raw_path)).unwrap();
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(file)
        .read_to_end(&mut out)
        .unwrap();
    out.len()
}

#[tokio::test]
async fn body_is_cut_at_2mb_in_the_returned_page_and_the_stored_file() {
    let fx = fx().await;
    let base = chunked_server(3 * 1024 * 1024).await;
    let page = article::fetch(&fx.http, &format!("{base}/big"), CAND)
        .await
        .unwrap();
    assert_eq!(page.html.len(), article::MAX_BODY_BYTES);
    assert_eq!(
        raw_len(fx.dir.path(), &page.raw_path),
        article::MAX_BODY_BYTES,
        "the stored raw body is the cut body, not the 3 MB the server sent"
    );
}

#[tokio::test]
async fn declared_length_over_2mb_is_refused_and_nothing_is_stored() {
    let fx = fx().await;
    let huge = format!("<p>start</p>{}", "z".repeat(3 * 1024 * 1024));
    Mock::given(path("/story/huge"))
        .respond_with(html(&huge))
        .mount(&fx.site)
        .await;
    let err = article::fetch(&fx.http, &format!("{}/story/huge", fx.site.uri()), CAND)
        .await
        .unwrap_err();
    assert!(
        err.chain().any(|c| matches!(
            c.downcast_ref::<HttpError>(),
            Some(HttpError::TooLarge { limit }) if *limit == article::MAX_BODY_BYTES
        )),
        "{err:#}"
    );
    assert!(!fx.dir.path().join("raw").exists(), "no body was stored");
}

#[tokio::test]
async fn fetch_rejects_non_html() {
    let fx = fx().await;
    Mock::given(path("/doc.pdf"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("%PDF-1.4", "application/pdf"))
        .mount(&fx.site)
        .await;
    let err = article::fetch(&fx.http, &format!("{}/doc.pdf", fx.site.uri()), CAND)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not html"), "{err}");
}

// ----------------------------------------------------------------------------------------------------------
// Dating rules: Wayback without a lower limit, date-only metadata
// ----------------------------------------------------------------------------------------------------------

#[tokio::test]
async fn old_page_recaptured_recently_is_dated_by_its_earliest_capture() {
    // Ruling R16: the CDX lookup has no `from`, so the earliest capture is the true one (a month ago), not
    // a recent re-capture, and the article is out of the window rather than kept as fresh news.
    let fx = fx().await;
    Mock::given(path("/story/old"))
        .respond_with(html(NO_TIME))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx(r#"[["timestamp"],["20261019120000"]]"#))
        .expect(1)
        .mount(&fx.site)
        .await;
    let link = click_link(&format!("{}/story/old", fx.site.uri()));
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert!(kept.is_empty());
    let r = row(&fx.conn, &link);
    assert_eq!(r.decision, "out_of_window");
    assert_eq!(r.time_source.as_deref(), Some("wayback"));
    assert_eq!(r.wayback_ts, Some(1_792_411_200)); // 2026-10-19 12:00 UTC
    let reqs = fx.site.received_requests().await.unwrap();
    let cdx_req = reqs
        .iter()
        .find(|q| q.url.path() == "/cdx/search/cdx")
        .unwrap();
    assert!(cdx_req.url.query_pairs().all(|(k, _)| k != "from"));
}

fn date_only_page() -> String {
    format!(
        r#"<meta property="article:published_time" content="2026-11-18"><p>{}</p>"#,
        "body text ".repeat(60)
    )
}

#[tokio::test]
async fn date_only_metadata_falls_through_to_newsapi() {
    let fx = fx().await;
    Mock::given(path("/story/d1"))
        .respond_with(html(&date_only_page()))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .expect(0)
        .mount(&fx.site)
        .await;
    let link = format!("{}/story/d1", fx.site.uri());
    let mut h = hit(link.clone());
    h.via = Via::NewsApi;
    h.newsapi_pub_ts = Some(MOVE_START + 300);
    let kept = run_collect(&fx, &[h]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].time_source, TimeSource::NewsApiPub);
    assert_eq!(kept[0].used_ts, MOVE_START + 300);
    let r = row(&fx.conn, &link);
    // The date is recorded, with the note, but not used.
    assert_eq!(r.meta_pub_ts, Some(1_794_960_000));
    assert_eq!(
        r.meta_source.as_deref(),
        Some("meta:article:published_time:date_only")
    );
    assert_eq!(r.time_source.as_deref(), Some("newsapi_pub"));
}

#[tokio::test]
async fn date_only_metadata_falls_through_to_wayback_then_to_excluded() {
    let fx = fx().await;
    Mock::given(path("/story/d2"))
        .respond_with(html(&date_only_page()))
        .mount(&fx.site)
        .await;
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx(r#"[["timestamp"],["20261118141500"]]"#))
        .up_to_n_times(1)
        .mount(&fx.site)
        .await;
    let link = format!("{}/story/d2", fx.site.uri());
    let kept = run_collect(&fx, &[hit(link.clone())]).await;
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].time_source, TimeSource::Wayback);
    assert_eq!(row(&fx.conn, &link).decision, "kept");

    // No other time at all: excluded, not dated at midnight.
    Mock::given(path("/cdx/search/cdx"))
        .respond_with(cdx("[]"))
        .mount(&fx.site)
        .await;
    let link2 = format!("{}/story/d2?again", fx.site.uri());
    let kept = run_collect(&fx, &[hit(link2.clone())]).await;
    assert!(kept.is_empty());
    assert_eq!(row(&fx.conn, &link2).decision, "no_time");
}

// ----------------------------------------------------------------------------------------------------------
// Host guard (SSRF)
// ----------------------------------------------------------------------------------------------------------

fn is_blocked(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| matches!(c.downcast_ref::<HttpError>(), Some(HttpError::Blocked(_))))
}

fn strict_http(dir: &std::path::Path) -> Http {
    Http::with_spacing(EventLog::new(dir), HashMap::new())
        .with_backoff_base(Duration::from_millis(1))
}

#[test]
fn guard_ip_classification() {
    use std::net::IpAddr;
    let strict = HostGuard::strict();
    let ip = |s: &str| s.parse::<IpAddr>().unwrap();
    for blocked in [
        "127.0.0.1",
        "127.9.9.9",
        "0.0.0.0",
        "0.1.2.3",
        "10.0.0.1",
        "10.255.255.255",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.8.3",
        "192.168.1.4",
        "169.254.1.1",
        "100.64.0.1",
        "100.96.77.115",
        "100.127.255.255",
        "224.0.0.1",
        "255.255.255.255",
        "240.0.0.1",
        "::1",
        "::",
        "fe80::1",
        "febf::1",
        "fc00::1",
        "fd12:3456::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:10.1.2.3",
        "::ffff:192.168.8.3",
    ] {
        assert!(!strict.ip_allowed(ip(blocked)), "{blocked} must be blocked");
    }
    for public in [
        "93.184.216.34",
        "8.8.8.8",
        "172.15.255.255",
        "172.32.0.1",
        "100.63.255.255",
        "100.128.0.1",
        "169.253.1.1",
        "2606:4700:4700::1111",
        "::ffff:93.184.216.34",
    ] {
        assert!(strict.ip_allowed(ip(public)), "{public} must be allowed");
    }
    // Test mode lets loopback through and nothing else.
    let lo = HostGuard::allowing_loopback();
    assert!(lo.ip_allowed(ip("127.0.0.1")) && lo.ip_allowed(ip("::1")));
    assert!(!lo.ip_allowed(ip("192.168.8.3")) && !lo.ip_allowed(ip("100.96.77.115")));
}

#[test]
fn guard_host_names() {
    let strict = HostGuard::strict();
    for blocked in [
        "localhost",
        "LOCALHOST",
        "foo.localhost",
        "mini-mac",
        "grafana",
        "mini-mac.local",
        "printer.LOCAL",
        "mini-mac.tail8b6e7c.ts.net",
        "svc.internal",
        "localhost.",
        "",
    ] {
        assert!(!strict.name_allowed(blocked), "{blocked:?}");
    }
    for ok in ["example.com", "www.apnews.com", "news.sample-wire.test"] {
        assert!(strict.name_allowed(ok), "{ok}");
    }
    assert!(HostGuard::allowing_loopback().name_allowed("localhost"));
    assert!(!HostGuard::allowing_loopback().name_allowed("mini-mac"));
}

#[tokio::test]
async fn guard_check_url() {
    let strict = HostGuard::strict();
    let ok = |s: &str| {
        let url = reqwest::Url::parse(s).unwrap();
        let strict = strict.clone();
        async move { strict.check_url(&url).await }
    };
    // Public-looking IP literals need no DNS and pass.
    assert!(ok("https://93.184.216.34/story").await.is_ok());
    assert!(ok("http://[2606:4700:4700::1111]/story").await.is_ok());
    for bad in [
        "http://127.0.0.1/x",
        "http://127.0.0.1:3000/x",
        "http://[::1]/x",
        "http://192.168.8.3:9090/",
        "http://10.0.0.5/",
        "http://169.254.169.254/latest/meta-data",
        "http://100.96.77.115/",
        "http://0.0.0.0/",
        "http://[::ffff:127.0.0.1]/",
        "http://localhost:8088/",
        "http://mini-mac:3000/",
        "http://mini-mac.tail8b6e7c.ts.net/",
        "http://box.local/",
        "ftp://93.184.216.34/file",
        "file:///etc/passwd",
    ] {
        assert!(ok(bad).await.is_err(), "{bad} must be refused");
    }
}

#[tokio::test]
async fn article_url_on_a_local_host_is_blocked_before_any_request() {
    let dir = tempfile::tempdir().unwrap();
    let http = strict_http(dir.path());
    let site = MockServer::start().await; // listens on 127.0.0.1
    Mock::given(path("/secret"))
        .respond_with(html("<p>internal</p>"))
        .mount(&site)
        .await;
    let err = article::fetch(&http, &format!("{}/secret", site.uri()), CAND)
        .await
        .unwrap_err();
    assert!(is_blocked(&err), "{err:#}");
    assert!(site.received_requests().await.unwrap().is_empty());
    assert_eq!(events(dir.path(), "http_blocked").len(), 1);
}

#[tokio::test]
async fn redirect_to_a_private_address_is_blocked_at_that_hop() {
    let fx = fx().await; // loopback allowed, so the first hop is fine
    Mock::given(path("/start"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("location", "http://192.168.8.3:3000/admin"),
        )
        .mount(&fx.site)
        .await;
    let err = article::fetch(&fx.http, &format!("{}/start", fx.site.uri()), CAND)
        .await
        .unwrap_err();
    assert!(is_blocked(&err), "{err:#}");
    assert!(
        err.chain().any(|c| c.to_string().contains("192.168.8.3")),
        "{err:#}"
    );
    assert_eq!(fx.site.received_requests().await.unwrap().len(), 1);

    // Names are refused too, without a lookup: tailnet, .local and single-label hosts.
    for target in [
        "http://mini-mac.tail8b6e7c.ts.net/",
        "http://printer.local/",
        "http://grafana:3000/",
        "http://100.96.77.115:9090/",
    ] {
        fx.site.reset().await;
        Mock::given(path("/start"))
            .respond_with(ResponseTemplate::new(301).insert_header("location", target))
            .mount(&fx.site)
            .await;
        let err = article::fetch(&fx.http, &format!("{}/start", fx.site.uri()), CAND)
            .await
            .unwrap_err();
        assert!(is_blocked(&err), "{target}: {err:#}");
    }
}

#[tokio::test]
async fn redirects_to_allowed_hosts_are_followed_and_limited() {
    let fx = fx().await;
    Mock::given(path("/a"))
        .respond_with(ResponseTemplate::new(301).insert_header("location", "/b"))
        .mount(&fx.site)
        .await;
    Mock::given(path("/b"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/story/final"))
        .mount(&fx.site)
        .await;
    Mock::given(path("/story/final"))
        .respond_with(html(&date_only_page()))
        .mount(&fx.site)
        .await;
    let page = article::fetch(&fx.http, &format!("{}/a", fx.site.uri()), CAND)
        .await
        .unwrap();
    assert!(
        page.final_url.ends_with("/story/final"),
        "{}",
        page.final_url
    );
    assert!(page.text.contains("body text"));

    Mock::given(path("/loop"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/loop"))
        .mount(&fx.site)
        .await;
    let err = article::fetch(&fx.http, &format!("{}/loop", fx.site.uri()), CAND)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("redirected more than"), "{err:#}");
}

#[tokio::test]
async fn blocked_host_article_is_excluded_logged_and_never_dated() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts) \
         VALUES (1, 'EV', 'yes', ?1, 90, 'investigating', ?1, ?1)",
        [MOVE_START],
    )
    .unwrap();
    let log = EventLog::new(dir.path());
    let http = strict_http(dir.path());
    let site = MockServer::start().await;
    let ep = NewsEndpoints {
        rss_base: site.uri(),
        newsapi_base: site.uri(),
        wayback_base: site.uri(),
    };
    // A NewsAPI-style direct link to an internal address, with a perfectly good publish time.
    let mut h = hit(format!("{}/story/internal", site.uri()));
    h.via = Via::NewsApi;
    h.newsapi_pub_ts = Some(MOVE_START + 60);
    let kept = collect(&http, &ep, &conn, &log, CAND, &[h.clone()], MOVE_START, NOW).await;
    assert!(kept.is_empty());
    let r = row(&conn, &h.link);
    assert_eq!(r.decision, "blocked_host");
    assert!(r.reason.unwrap().contains("blocked host"));
    assert_eq!(r.publish_ts, None);
    assert!(
        site.received_requests().await.unwrap().is_empty(),
        "no fetch, no Wayback call"
    );
    let ev = events(dir.path(), "article_dated");
    assert_eq!(ev[0]["payload"]["decision"], "blocked_host");
}
