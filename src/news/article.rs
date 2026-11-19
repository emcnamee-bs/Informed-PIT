//! Article step of the investigation (spec §8 step 4): resolve news links to publisher URLs, fetch
//! each page, date it (page metadata first, Wayback only as a backup), apply the window test and the
//! 40-article cap. Every article gets one `articles` row with its decision and reason.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use base64::Engine;
use rusqlite::{params, Connection};
use scraper::{Html, Node, Selector};
use serde_json::json;

use super::pubtime::{
    date_article, extract_pub_ts, in_window, is_date_only, TimeSource, WINDOW_LEAD_SECS,
};
use super::wayback;
use super::{bing_publisher_url, canonical_link, Hit, NewsEndpoints, Via};
use crate::eventlog::EventLog;
use crate::http::{CallCtx, Http, HttpError, Service};

/// Per-request timeout for an article fetch or a link resolution.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// Bytes of a page body kept (a longer body is cut here).
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Characters of article text kept for Haiku.
pub const MAX_TEXT_CHARS: usize = 20_000;
/// A fetched page with less text than this is treated as a paywall or empty stub: kept on title and
/// snippet only, flagged `text_unavailable`.
pub const MIN_TEXT_CHARS: usize = 200;
/// Kept articles per candidate, newest first.
pub const MAX_KEPT: usize = 40;
const MAX_ATTEMPTS: u32 = 2;

/// A fetched, parsed page.
#[derive(Debug, Clone)]
pub struct Page {
    /// URL after redirects.
    pub final_url: String,
    /// Body as text, cut at [`MAX_BODY_BYTES`].
    pub html: String,
    /// Paragraph text (script, style, nav and footer content dropped), at most [`MAX_TEXT_CHARS`] chars.
    pub text: String,
    pub title: String,
    /// Stored raw body, relative to the data dir.
    pub raw_path: String,
}

/// An article that passed the window test, as handed to the per-article Haiku step (Task 12).
#[derive(Debug, Clone)]
pub struct KeptArticle {
    pub article_id: i64,
    pub url: String,
    pub title: String,
    pub source: String,
    /// The publish time used for the window test (UTC unix seconds).
    pub used_ts: i64,
    pub time_source: TimeSource,
    /// At most [`MAX_TEXT_CHARS`] characters; the RSS snippet (or the title) when `text_unavailable`.
    pub text: String,
    pub text_unavailable: bool,
}

// ---------------------------------------------------------------------------------------------------------
// Link resolution
// ---------------------------------------------------------------------------------------------------------

fn is_google_host(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|h| {
        let h = h.to_ascii_lowercase();
        h == "google.com" || h.ends_with(".google.com")
    })
}

/// The publisher URL embedded in a Google News link's own path segment. Older Google News links are
/// `.../articles/<base64url>` where the decoded bytes hold the publisher URL in plain text. Newer links hold
/// an opaque token and yield `None`. This only parses the URL we were given.
pub(crate) fn decode_embedded_url(link: &reqwest::Url) -> Option<String> {
    let token = link.path_segments()?.rfind(|s| !s.is_empty())?;
    let engine = GeneralPurpose::new(
        &alphabet::URL_SAFE,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true),
    );
    let bytes = engine.decode(token.trim_end_matches('=')).ok()?;
    let start = bytes
        .windows(8)
        .position(|w| w == b"https://")
        .or_else(|| bytes.windows(7).position(|w| w == b"http://"))?;
    let end = bytes[start..]
        .iter()
        .position(|b| !(0x21..=0x7e).contains(b))
        .map_or(bytes.len(), |n| start + n);
    let candidate = std::str::from_utf8(&bytes[start..end]).ok()?;
    let url = reqwest::Url::parse(candidate).ok()?;
    (!is_google_host(&url)).then(|| url.to_string())
}

/// Publisher URL for a search hit's `link`; pure URL parsing, no request.
///
/// - Bing News click links (`bing.com/news/apiclick.aspx?...&url=<publisher>`): the plain `url` parameter
///   ([`bing_publisher_url`]), the primary RSS source's format.
/// - Google News links (the old RSS source): only the publisher URL embedded in the link's own path
///   segment. Live check 2026-11-18, 40 links from a real Google News RSS search: all 40 carried an opaque
///   `AU_yqL...` token, and a plain GET of the link also gave no publisher URL (HTTP 200 on the same URL, no
///   redirect), so an opaque Google link is an error and is never fetched.
/// - Anything else is returned unchanged (NewsAPI.ai gives direct publisher URLs).
///
/// An error makes the article `unresolved` (logged, excluded).
pub fn resolve_link(link: &str) -> Result<String> {
    let url = reqwest::Url::parse(link.trim()).with_context(|| format!("bad link {link:?}"))?;
    let host = url.host_str().unwrap_or("").to_ascii_lowercase();
    if host == "bing.com" || host.ends_with(".bing.com") {
        return bing_publisher_url(&url)
            .ok_or_else(|| anyhow!("bing link has no usable publisher url parameter"));
    }
    if is_google_host(&url) {
        return decode_embedded_url(&url).ok_or_else(|| {
            anyhow!("google link did not resolve: opaque token, no publisher url in the link")
        });
    }
    Ok(url.to_string())
}

// ---------------------------------------------------------------------------------------------------------
// Fetch and text extraction
// ---------------------------------------------------------------------------------------------------------

fn sel(css: &str) -> Selector {
    Selector::parse(css).expect("static selector")
}

/// Text of a node's descendants, leaving out `script`, `style` and `noscript` content.
fn visible_text(el: scraper::ElementRef<'_>) -> String {
    let mut out = String::new();
    for node in el.descendants() {
        if let Node::Text(t) = node.value() {
            let hidden = node
                .parent()
                .and_then(|p| p.value().as_element())
                .is_some_and(|p| matches!(p.name(), "script" | "style" | "noscript"));
            if !hidden {
                out.push_str(t);
            }
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Title (`og:title`, else `<title>`) and paragraph text of a page. `<p>` elements inside `nav` or `footer`
/// are dropped; the rest are joined with blank lines and cut to [`MAX_TEXT_CHARS`].
pub fn extract_text(html: &str) -> (String, String) {
    let doc = Html::parse_document(html);
    let og_title = doc.select(&sel("meta")).find_map(|m| {
        let v = m.value();
        (v.attr("property") == Some("og:title"))
            .then(|| v.attr("content").unwrap_or("").trim().to_string())
            .filter(|t| !t.is_empty())
    });
    let title = og_title
        .or_else(|| {
            doc.select(&sel("title"))
                .next()
                .map(visible_text)
                .filter(|t| !t.is_empty())
        })
        .unwrap_or_default();
    let paragraphs: Vec<String> = doc
        .select(&sel("p"))
        .filter(|p| {
            !p.ancestors().any(|a| {
                a.value()
                    .as_element()
                    .is_some_and(|e| matches!(e.name(), "nav" | "footer"))
            })
        })
        .map(visible_text)
        .filter(|t| !t.is_empty())
        .collect();
    let text: String = paragraphs
        .join("\n\n")
        .chars()
        .take(MAX_TEXT_CHARS)
        .collect();
    (title, text)
}

/// Redirect hops [`fetch`] follows.
const MAX_REDIRECTS: usize = 5;

/// GET `url` (polite user agent, 10 s timeout) and parse it. Redirects are followed here, one request per
/// hop, so the host guard checks every hop (up to [`MAX_REDIRECTS`]). Errors: a blocked host (the error's
/// chain holds [`HttpError::Blocked`]), transport failure after retries, a declared body over
/// [`MAX_BODY_BYTES`], a non-2xx status (a paywall or block usually shows as 401, 402 or 403), or a
/// non-HTML content type. A longer body without a declared length is cut at [`MAX_BODY_BYTES`] while
/// reading, and only the cut body is stored.
pub async fn fetch(http: &Http, url: &str, candidate_id: i64) -> Result<Page> {
    let mut current = reqwest::Url::parse(url).with_context(|| format!("bad url {url:?}"))?;
    let mut hops = 0;
    let res = loop {
        let res = http
            .send_capped(
                Service::Article,
                http.client().get(current.clone()).timeout(FETCH_TIMEOUT),
                CallCtx {
                    label: "article",
                    candidate_id: Some(candidate_id),
                    max_attempts: MAX_ATTEMPTS,
                    store_raw: true,
                },
                Some(MAX_BODY_BYTES),
            )
            .await
            .map_err(|e| anyhow::Error::new(e).context("article request failed"))?;
        let location = matches!(res.status, 301 | 302 | 303 | 307 | 308)
            .then(|| res.headers.get("location"))
            .flatten();
        let Some(location) = location else {
            break res;
        };
        hops += 1;
        if hops > MAX_REDIRECTS {
            return Err(anyhow!(
                "article redirected more than {MAX_REDIRECTS} times"
            ));
        }
        current = current
            .join(location)
            .with_context(|| format!("bad redirect location {location:?}"))?;
    };
    if !(200..300).contains(&res.status) {
        return Err(anyhow!("article http status {}", res.status));
    }
    if let Some(ct) = res.content_type() {
        if !(ct.contains("html") || ct.contains("xml")) {
            return Err(anyhow!("article content type {ct} is not html"));
        }
    }
    let body = &res.body; // already cut at MAX_BODY_BYTES by the HTTP layer
    let html = String::from_utf8_lossy(body).into_owned();
    let (title, text) = extract_text(&html);
    Ok(Page {
        final_url: res.final_url,
        html,
        text,
        title,
        raw_path: res.raw_path,
    })
}

// ---------------------------------------------------------------------------------------------------------
// Collect
// ---------------------------------------------------------------------------------------------------------

/// One `articles` row as written by [`collect`].
#[derive(Default)]
struct Row {
    url: String,
    resolved_url: Option<String>,
    final_url: Option<String>,
    title: String,
    source: String,
    rss_pub_ts: Option<i64>,
    meta_pub_ts: Option<i64>,
    meta_source: Option<&'static str>,
    wayback_ts: Option<i64>,
    publish_ts: Option<i64>,
    time_source: TimeSource,
    in_window: Option<bool>,
    decision: &'static str,
    reason: Option<String>,
    raw_path: Option<String>,
    text_unavailable: bool,
    /// Which search found it: `rss` or `newsapi` (`None` only in [`Row::default`]).
    via: Option<&'static str>,
}

/// What a Wayback lookup for one article left behind, for `wayback_checks`.
struct WaybackLog {
    url: String,
    capture_ts: Option<i64>,
    ok: bool,
    error: Option<String>,
    raw_path: Option<String>,
}

/// Fetch, date and window-test every hit; write one `articles` row per hit; return the kept articles
/// (newest first, at most [`MAX_KEPT`]). `move_start` is the move's first minute and `now` the investigation
/// time. Articles are handled one at a time. Nothing here fails the candidate: a problem with one article is
/// its row's decision and reason (`unresolved`, `fetch_failed`, `no_time`, `out_of_window`, `dup`,
/// `over_cap`, `blocked_host`, or `kept`) plus an `article_dated` event.
#[allow(clippy::too_many_arguments)]
pub async fn collect(
    http: &Http,
    ep: &NewsEndpoints,
    conn: &Connection,
    log: &EventLog,
    candidate_id: i64,
    hits: &[Hit],
    move_start: i64,
    now: i64,
) -> Vec<KeptArticle> {
    let mut seen_links = HashSet::new();
    let mut seen_resolved = HashSet::new();
    let mut kept: Vec<KeptArticle> = Vec::new();

    for hit in hits {
        if !seen_links.insert(hit.link.clone()) {
            log.log(
                "article_dated",
                Some(candidate_id),
                json!({"url": hit.link, "decision": "dup", "reason": "same link repeated in the hit list"}),
            );
            continue;
        }
        let (row, wb, text) = examine(
            http,
            ep,
            log,
            candidate_id,
            hit,
            move_start,
            now,
            &mut seen_resolved,
        )
        .await;
        let article_id = match save(conn, candidate_id, &row) {
            Ok(id) => id,
            Err(e) => {
                log.log(
                    "article_save_failed",
                    Some(candidate_id),
                    json!({"url": row.url, "error": e.to_string()}),
                );
                continue;
            }
        };
        if let Some(wb) = wb {
            if let Err(e) = save_wayback(conn, article_id, &wb) {
                log.log(
                    "article_save_failed",
                    Some(candidate_id),
                    json!({"url": row.url, "error": e.to_string(), "table": "wayback_checks"}),
                );
            }
        }
        log.log(
            "article_dated",
            Some(candidate_id),
            json!({
                "article_id": article_id,
                "url": row.url,
                "resolved_url": row.resolved_url,
                "decision": row.decision,
                "reason": row.reason,
                "rss_pub_ts": row.rss_pub_ts,
                "meta_pub_ts": row.meta_pub_ts,
                "meta_source": row.meta_source,
                "wayback_ts": row.wayback_ts,
                "used_ts": row.publish_ts,
                "time_source": row.time_source.as_str(),
                "in_window": row.in_window,
                "text_unavailable": row.text_unavailable,
            }),
        );
        if row.decision == "kept" {
            kept.push(KeptArticle {
                article_id,
                url: row
                    .final_url
                    .clone()
                    .or_else(|| row.resolved_url.clone())
                    .unwrap_or_else(|| row.url.clone()),
                title: row.title.clone(),
                source: row.source.clone(),
                used_ts: row.publish_ts.expect("kept articles have a time"),
                time_source: row.time_source,
                text: text.unwrap_or_default(),
                text_unavailable: row.text_unavailable,
            });
        }
    }

    // Newest first; everything past the cap is marked over_cap.
    kept.sort_by_key(|a| std::cmp::Reverse(a.used_ts));
    if kept.len() > MAX_KEPT {
        for dropped in kept.split_off(MAX_KEPT) {
            let res = conn.execute(
                "UPDATE articles SET decision = 'over_cap', reason = ?2 WHERE id = ?1",
                params![
                    dropped.article_id,
                    format!("more than {MAX_KEPT} kept articles; this one is older than the newest {MAX_KEPT}")
                ],
            );
            if let Err(e) = res {
                log.log(
                    "article_save_failed",
                    Some(candidate_id),
                    json!({"article_id": dropped.article_id, "error": e.to_string()}),
                );
            }
            log.log(
                "article_dated",
                Some(candidate_id),
                json!({"article_id": dropped.article_id, "url": dropped.url, "decision": "over_cap"}),
            );
        }
    }
    kept
}

/// Resolve, fetch, date and judge one hit. Returns its row, the Wayback lookup (if one ran) and, for a kept
/// article, the text to hand on.
#[allow(clippy::too_many_arguments)]
async fn examine(
    http: &Http,
    ep: &NewsEndpoints,
    log: &EventLog,
    candidate_id: i64,
    hit: &Hit,
    move_start: i64,
    now: i64,
    seen_resolved: &mut HashSet<String>,
) -> (Row, Option<WaybackLog>, Option<String>) {
    let mut row = Row {
        url: hit.link.clone(),
        title: hit.title.clone(),
        source: hit.source.clone(),
        rss_pub_ts: hit.rss_pub_ts,
        via: Some(match hit.via {
            Via::Rss => "rss",
            Via::NewsApi => "newsapi",
        }),
        ..Row::default()
    };

    let resolved = match resolve_link(&hit.link) {
        Ok(u) => u,
        Err(e) => {
            row.decision = "unresolved";
            row.reason = Some(e.to_string());
            return (row, None, None);
        }
    };
    row.resolved_url = Some(resolved.clone());
    if !seen_resolved.insert(canonical_link(&resolved)) {
        row.decision = "dup";
        row.reason = Some("another hit resolved to the same publisher url".to_string());
        return (row, None, None);
    }

    let (page, fetch_error) = match fetch(http, &resolved, candidate_id).await {
        Ok(p) => (Some(p), None),
        Err(e) => {
            let blocked = e
                .chain()
                .any(|c| matches!(c.downcast_ref::<HttpError>(), Some(HttpError::Blocked(_))));
            if blocked {
                // Never fetched, and not dated by other means either: a hostile or internal URL.
                row.decision = "blocked_host";
                row.reason = Some(format!("{e:#}"));
                return (row, None, None);
            }
            (None, Some(format!("{e:#}")))
        }
    };
    let mut meta = None;
    if let Some(p) = &page {
        row.final_url = Some(p.final_url.clone());
        row.raw_path = Some(p.raw_path.clone());
        if !p.title.is_empty() {
            row.title = p.title.clone();
        }
        if let Some((ts, src)) = extract_pub_ts(&p.html) {
            row.meta_pub_ts = Some(ts);
            row.meta_source = Some(src);
            // A bare date is recorded but is not an exact publish time: treated as missing.
            meta = (!is_date_only(src)).then_some(ts);
        }
    }

    // Metadata, then NewsAPI.ai; Wayback only when neither gives a usable time.
    let mut dated = date_article(meta, hit.rss_pub_ts, hit.newsapi_pub_ts, now, || None);
    let mut wb_log = None;
    if dated.source == TimeSource::None {
        let wb_url = row.final_url.clone().unwrap_or_else(|| resolved.clone());
        let lookup = wayback::lookup(http, ep, &wb_url, candidate_id).await;
        let wb_ts = match lookup {
            Ok(cap) => {
                wb_log = Some(WaybackLog {
                    url: wb_url,
                    capture_ts: cap.ts,
                    ok: true,
                    error: None,
                    raw_path: Some(cap.raw_path),
                });
                cap.ts
            }
            Err(e) => {
                log.log(
                    "wayback_failed",
                    Some(candidate_id),
                    json!({"url": wb_url, "error": e.to_string()}),
                );
                wb_log = Some(WaybackLog {
                    url: wb_url,
                    capture_ts: None,
                    ok: false,
                    error: Some(e.to_string()),
                    raw_path: None,
                });
                None
            }
        };
        row.wayback_ts = wb_ts;
        dated = date_article(meta, hit.rss_pub_ts, hit.newsapi_pub_ts, now, || wb_ts);
    }
    row.time_source = dated.source;
    row.publish_ts = dated.ts;

    let Some(ts) = dated.ts else {
        // Excluded: no time anywhere. A failed fetch is named as the cause when there was one.
        row.decision = if fetch_error.is_some() {
            "fetch_failed"
        } else {
            "no_time"
        };
        row.reason = Some(match &fetch_error {
            Some(e) => format!("{e}; no publish time from rss, newsapi or wayback"),
            None => "no publish time in page metadata, rss, newsapi or wayback".to_string(),
        });
        return (row, wb_log, None);
    };
    let inside = in_window(ts, move_start, now);
    row.in_window = Some(inside);
    if !inside {
        row.decision = "out_of_window";
        row.reason = Some(format!(
            "published {ts} ({}), window is {} to {now}",
            dated.source,
            move_start - WINDOW_LEAD_SECS
        ));
        return (row, wb_log, None);
    }

    row.decision = "kept";
    let body = page
        .as_ref()
        .map(|p| p.text.clone())
        .filter(|t| t.chars().count() >= MIN_TEXT_CHARS);
    match body {
        Some(text) => (row, wb_log, Some(text)),
        None => {
            row.text_unavailable = true;
            row.reason = Some(match fetch_error {
                Some(e) => format!("text unavailable: {e}"),
                None => "text unavailable: page has almost no readable text (paywall or stub)"
                    .to_string(),
            });
            let snippet = if hit.snippet.trim().is_empty() {
                hit.title.clone()
            } else {
                hit.snippet.clone()
            };
            (row, wb_log, Some(snippet))
        }
    }
}

/// Insert (or, when the candidate is collected again, replace) the row for `row.url`; returns its id.
fn save(conn: &Connection, candidate_id: i64, row: &Row) -> rusqlite::Result<i64> {
    conn.query_row(
        "INSERT INTO articles (candidate_id, url, resolved_url, final_url, title, source, publish_ts, \
         time_source, in_window, decision, reason, raw_path, fetched_ts, rss_pub_ts, meta_pub_ts, \
         meta_source, wayback_ts, text_unavailable, via) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19) \
         ON CONFLICT(candidate_id, url) DO UPDATE SET \
           resolved_url = excluded.resolved_url, final_url = excluded.final_url, title = excluded.title, \
           source = excluded.source, publish_ts = excluded.publish_ts, time_source = excluded.time_source, \
           in_window = excluded.in_window, decision = excluded.decision, reason = excluded.reason, \
           raw_path = excluded.raw_path, fetched_ts = excluded.fetched_ts, rss_pub_ts = excluded.rss_pub_ts, \
           meta_pub_ts = excluded.meta_pub_ts, meta_source = excluded.meta_source, \
           wayback_ts = excluded.wayback_ts, text_unavailable = excluded.text_unavailable, via = excluded.via \
         RETURNING id",
        params![
            candidate_id,
            row.url,
            row.resolved_url,
            row.final_url,
            row.title,
            row.source,
            row.publish_ts,
            row.time_source.as_str(),
            row.in_window,
            row.decision,
            row.reason,
            row.raw_path,
            chrono::Utc::now().timestamp(),
            row.rss_pub_ts,
            row.meta_pub_ts,
            row.meta_source,
            row.wayback_ts,
            row.text_unavailable,
            row.via,
        ],
        |r| r.get(0),
    )
}

fn save_wayback(conn: &Connection, article_id: i64, wb: &WaybackLog) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO wayback_checks (article_id, ts, url, capture_ts, ok, error, raw_path) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            article_id,
            chrono::Utc::now().timestamp(),
            wb.url,
            wb.capture_ts,
            wb.ok,
            wb.error,
            wb.raw_path
        ],
    )?;
    Ok(())
}
