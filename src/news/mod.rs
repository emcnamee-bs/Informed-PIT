//! News search (spec: investigation steps 2 and 3).
//!
//! Haiku's phrases are OR-combined into about ten Bing News RSS queries ([`build_queries`], [`rss`]).
//! NewsAPI.ai ([`newsapi`]) is the fallback and runs only when an RSS query failed (transport or HTTP error,
//! HTML or unparseable body); it is capped per candidate, per day and by the account's remaining tokens.
//! [`search_all`] ties the two together: every request and its outcome is a row in `searches`, every fallback
//! (or refused fallback) is an event naming the RSS error that triggered it.

pub mod article;
pub mod newsapi;
pub mod pubtime;
pub mod rss;
pub mod wayback;

use std::collections::HashSet;

use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::json;

use crate::eventlog::EventLog;
use crate::http::Http;
use newsapi::NewsApiError;

/// Longest allowed URL-encoded `q`.
pub const MAX_QUERY_ENCODED: usize = 1500;
/// Phrases per query: 50 phrases make about 10 queries.
pub const MAX_PHRASES_PER_QUERY: usize = 5;

/// Which service produced a [`Hit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Rss,
    NewsApi,
}

/// One article found by a search. `link` is as the service gave it (Bing News links are `apiclick.aspx` redirect URLs carrying the publisher URL; see [`bing_publisher_url`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub title: String,
    pub link: String,
    /// Publisher name; empty when the service gave none.
    pub source: String,
    /// RSS `pubDate` (UTC unix seconds), for RSS hits.
    pub rss_pub_ts: Option<i64>,
    pub snippet: String,
    pub via: Via,
    /// NewsAPI.ai `dateTimePub` (else `dateTime`), for NewsAPI.ai hits.
    pub newsapi_pub_ts: Option<i64>,
}

/// Base URLs of the two services; tests point them at a mock server.
#[derive(Debug, Clone)]
pub struct NewsEndpoints {
    pub rss_base: String,
    pub newsapi_base: String,
    /// Wayback Machine CDX host (plain http, as the CDX API documents).
    pub wayback_base: String,
}

impl Default for NewsEndpoints {
    fn default() -> NewsEndpoints {
        NewsEndpoints {
            rss_base: "https://www.bing.com".to_string(),
            newsapi_base: "https://eventregistry.org".to_string(),
            wayback_base: "http://web.archive.org".to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct SearchOutcome {
    /// Deduplicated by canonical link, RSS hits first.
    pub hits: Vec<Hit>,
    pub rss_ok: u32,
    pub rss_failed: u32,
    /// A NewsAPI.ai request was sent (whether or not it succeeded).
    pub fallback_used: bool,
    /// The NewsAPI.ai fallback answered successfully.
    pub fallback_ok: bool,
    pub tokens_spent: u32,
}

/// Length of `s` as a form-urlencoded query value (what `reqwest` sends): letters, digits and `*-._` stay,
/// a space becomes `+`, every other byte becomes `%XX`.
fn encoded_len(s: &str) -> usize {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '*' | '-' | '.' | '_' | ' ') {
                1
            } else {
                c.len_utf8() * 3
            }
        })
        .sum()
}

fn clean_phrase(p: &str) -> String {
    let no_quotes: String = p
        .chars()
        .filter(|c| !matches!(c, '"' | '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}'))
        .collect();
    no_quotes.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Queries with the (cleaned) phrases each one covers. Phrases are quoted and joined with ` OR `; a query
/// holds at most [`MAX_PHRASES_PER_QUERY`] phrases and its URL-encoded form stays within
/// [`MAX_QUERY_ENCODED`]. Blank and repeated (case-insensitive) phrases are skipped.
pub(crate) fn build_query_groups(phrases: &[String]) -> Vec<(String, Vec<String>)> {
    let mut seen = HashSet::new();
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    let mut cur: Vec<String> = Vec::new();
    let mut cur_q = String::new();
    for p in phrases.iter().map(|p| clean_phrase(p)) {
        if p.is_empty() || !seen.insert(p.to_lowercase()) {
            continue;
        }
        let token = format!("\"{p}\"");
        if encoded_len(&token) > MAX_QUERY_ENCODED {
            continue; // cannot fit even alone
        }
        let joined = if cur.is_empty() {
            token.clone()
        } else {
            format!("{cur_q} OR {token}")
        };
        if !cur.is_empty()
            && (cur.len() >= MAX_PHRASES_PER_QUERY || encoded_len(&joined) > MAX_QUERY_ENCODED)
        {
            groups.push((std::mem::take(&mut cur_q), std::mem::take(&mut cur)));
            cur_q = token;
            cur = vec![p];
        } else {
            cur_q = joined;
            cur.push(p);
        }
    }
    if !cur.is_empty() {
        groups.push((cur_q, cur));
    }
    groups
}

/// OR-join quoted phrases into queries whose URL-encoded `q` is at most [`MAX_QUERY_ENCODED`] characters.
pub fn build_queries(phrases: &[String]) -> Vec<String> {
    build_query_groups(phrases)
        .into_iter()
        .map(|(q, _)| q)
        .collect()
}

/// The publisher URL inside a Bing News click link (`bing.com/news/apiclick.aspx?...&url=<publisher>&...`):
/// the plain `url` query parameter, accepted only when it is an http(s) URL that is not itself on bing.com.
/// `None` for any other link.
pub fn bing_publisher_url(link: &reqwest::Url) -> Option<String> {
    let host = link.host_str()?.to_ascii_lowercase();
    if !(host == "bing.com" || host.ends_with(".bing.com"))
        || !link.path().eq_ignore_ascii_case("/news/apiclick.aspx")
    {
        return None;
    }
    let (_, target) = link.query_pairs().find(|(k, _)| k == "url")?;
    let target = reqwest::Url::parse(&target).ok()?;
    let target_host = target.host_str()?.to_ascii_lowercase();
    let on_bing = target_host == "bing.com" || target_host.ends_with(".bing.com");
    (matches!(target.scheme(), "http" | "https") && !on_bing).then(|| target.to_string())
}

/// Link used for deduplication: a Bing click link stands for its publisher URL (the click link's other
/// parameters differ per request); otherwise scheme and host lowercased. Fragment and `utm_*` parameters
/// are dropped.
pub fn canonical_link(link: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(link.trim()) else {
        return link.trim().to_string();
    };
    if let Some(publisher) = bing_publisher_url(&url) {
        if let Ok(p) = reqwest::Url::parse(&publisher) {
            url = p;
        }
    }
    url.set_fragment(None);
    if let Some(query) = url.query().map(str::to_string) {
        let kept: Vec<&str> = query
            .split('&')
            .filter(|kv| {
                let key = kv.split('=').next().unwrap_or("").to_ascii_lowercase();
                !kv.is_empty() && !key.starts_with("utm_")
            })
            .collect();
        if kept.is_empty() {
            url.set_query(None);
        } else {
            url.set_query(Some(&kept.join("&")));
        }
    }
    url.to_string()
}

/// One row of `searches`.
pub(crate) struct SearchRow<'a> {
    pub candidate_id: i64,
    pub source: &'static str,
    pub query: &'a str,
    pub url: &'a str,
    pub http_status: Option<u16>,
    pub n_results: Option<usize>,
    pub ok: bool,
    pub error: Option<&'a str>,
    pub raw_path: Option<&'a str>,
    pub latency_ms: Option<u64>,
    pub tokens_used: Option<u32>,
    pub trigger_error: Option<&'a str>,
}

pub(crate) fn insert_search(conn: &Connection, row: &SearchRow) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO searches (candidate_id, ts, source, query, url, http_status, n_results, ok, error, \
         raw_path, latency_ms, tokens_used, trigger_error) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            row.candidate_id,
            Utc::now().timestamp(),
            row.source,
            row.query,
            row.url,
            row.http_status,
            row.n_results.map(|n| n as i64),
            row.ok,
            row.error,
            row.raw_path,
            row.latency_ms.map(|n| n as i64),
            row.tokens_used,
            row.trigger_error,
        ],
    )?;
    Ok(())
}

/// Run every RSS query for `phrases`; if any failed, make one NewsAPI.ai call covering the phrases of the
/// failed queries (when a key is configured and the caps allow). Never fails: each problem is a `searches`
/// row and an event. `newsapi_key` None means the owner has not registered yet.
pub async fn search_all(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    ep: &NewsEndpoints,
    newsapi_key: Option<&str>,
    phrases: &[String],
    candidate_id: i64,
) -> SearchOutcome {
    let mut out = SearchOutcome::default();
    let mut all_hits: Vec<Hit> = Vec::new();
    let mut failed_phrases: Vec<String> = Vec::new();
    let mut trigger_errors: Vec<String> = Vec::new();

    for (query, group) in build_query_groups(phrases) {
        let url = rss::request_url(ep, &query);
        let url = url.as_str();
        match rss::search_detailed(http, ep, &query, candidate_id).await {
            Ok(res) => {
                out.rss_ok += 1;
                record(
                    conn,
                    log,
                    &SearchRow {
                        candidate_id,
                        source: "rss",
                        query: &query,
                        url,
                        http_status: Some(res.http_status),
                        n_results: Some(res.hits.len()),
                        ok: true,
                        error: None,
                        raw_path: Some(&res.raw_path),
                        latency_ms: Some(res.elapsed_ms),
                        tokens_used: None,
                        trigger_error: None,
                    },
                );
                all_hits.extend(res.hits);
            }
            Err(e) => {
                out.rss_failed += 1;
                let msg = e.to_string();
                record(
                    conn,
                    log,
                    &SearchRow {
                        candidate_id,
                        source: "rss",
                        query: &query,
                        url,
                        http_status: e.http_status,
                        n_results: None,
                        ok: false,
                        error: Some(&msg),
                        raw_path: e.raw_path.as_deref(),
                        latency_ms: e.elapsed_ms,
                        tokens_used: None,
                        trigger_error: None,
                    },
                );
                log.log(
                    "rss_query_failed",
                    Some(candidate_id),
                    json!({"query": query, "error": msg}),
                );
                trigger_errors.push(format!("{query}: {msg}"));
                failed_phrases.extend(group);
            }
        }
    }

    if out.rss_failed > 0 {
        run_fallback(
            http,
            conn,
            log,
            ep,
            newsapi_key,
            &failed_phrases,
            &trigger_errors,
            candidate_id,
            &mut out,
            &mut all_hits,
        )
        .await;
    }

    let mut seen = HashSet::new();
    out.hits = all_hits
        .into_iter()
        .filter(|h| seen.insert(canonical_link(&h.link)))
        .collect();
    log.log(
        "news_search_done",
        Some(candidate_id),
        json!({
            "rss_ok": out.rss_ok,
            "rss_failed": out.rss_failed,
            "fallback_used": out.fallback_used,
            "fallback_ok": out.fallback_ok,
            "tokens_spent": out.tokens_spent,
            "hits": out.hits.len(),
        }),
    );
    out
}

#[allow(clippy::too_many_arguments)]
async fn run_fallback(
    http: &Http,
    conn: &Connection,
    log: &EventLog,
    ep: &NewsEndpoints,
    key: Option<&str>,
    phrases: &[String],
    trigger_errors: &[String],
    candidate_id: i64,
    out: &mut SearchOutcome,
    all_hits: &mut Vec<Hit>,
) {
    let Some(key) = key else {
        log.log(
            "newsapi_no_key",
            Some(candidate_id),
            json!({"trigger_errors": trigger_errors}),
        );
        return;
    };
    let trigger = trigger_errors.join(" | ");
    match newsapi::search(http, conn, ep, key, phrases, candidate_id, &trigger).await {
        Ok(res) => {
            out.fallback_used = true;
            out.fallback_ok = true;
            out.tokens_spent += res.tokens_spent;
            log.log(
                "newsapi_fallback",
                Some(candidate_id),
                json!({
                    "trigger_errors": trigger_errors,
                    "tokens": res.tokens_spent,
                    "articles": res.hits.len(),
                    "remaining": res.remaining,
                    "header_remaining": res.header_remaining,
                }),
            );
            all_hits.extend(res.hits);
        }
        Err(NewsApiError::Cap(reason)) => {
            log.log(
                "newsapi_blocked",
                Some(candidate_id),
                json!({"reason": reason.as_str(), "trigger_errors": trigger_errors}),
            );
        }
        Err(e) => {
            // The request was attempted (and logged in `searches` by newsapi::search).
            out.fallback_used = !matches!(e, NewsApiError::Db(_));
            log.log(
                "newsapi_failed",
                Some(candidate_id),
                json!({"error": e.to_string(), "trigger_errors": trigger_errors}),
            );
        }
    }
}

/// Insert a `searches` row; a database failure is logged, never fatal.
fn record(conn: &Connection, log: &EventLog, row: &SearchRow) {
    if let Err(e) = insert_search(conn, row) {
        log.log(
            "search_log_failed",
            Some(row.candidate_id),
            json!({"source": row.source, "error": e.to_string()}),
        );
    }
}
