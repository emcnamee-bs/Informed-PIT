//! NewsAPI.ai (Event Registry) fallback search with a token ledger and caps.
//!
//! One call: `POST <base>/api/v1/article/getArticles` with a JSON body holding the API key (never in the URL
//! and never stored: only the response body is kept, by the HTTP layer). A request that returns articles costs
//! one token (one page of up to 100 articles). Ledger in `kv`: `newsapi_tokens_used` (+1 per page) and
//! `newsapi_tokens_remaining` (starts at [`START_TOKENS`], -1 per page; our own count only: the API's
//! `x-ratelimit-remaining` header, when sent, is reported in the `newsapi_fallback` event but never overwrites
//! the ledger, since its meaning is not documented as plan tokens). Per-candidate and per-day spend is summed
//! from the `searches` log.

use std::fmt;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use super::{insert_search, Hit, NewsEndpoints, SearchRow, Via};
use crate::db::with_tx;
use crate::http::{CallCtx, Http, HttpError, Service};

/// Tokens a free account starts with.
pub const START_TOKENS: i64 = 2000;
/// At most this many tokens per candidate.
pub const MAX_PER_CANDIDATE: i64 = 10;
/// At most this many tokens per UTC day.
pub const MAX_PER_DAY: i64 = 100;
/// Stop when the account has this many tokens left or fewer (reserved for a deliberate later trial).
pub const RESERVE: i64 = 200;
/// Search window, days.
const WINDOW_DAYS: i64 = 30;
const SNIPPET_CHARS: usize = 500;
const MAX_ATTEMPTS: u32 = 2;

const KV_USED: &str = "newsapi_tokens_used";
const KV_REMAINING: &str = "newsapi_tokens_remaining";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapReason {
    /// This candidate already used [`MAX_PER_CANDIDATE`].
    Candidate,
    /// Today already used [`MAX_PER_DAY`].
    Daily,
    /// The account is at or below [`RESERVE`] tokens.
    Remaining,
}

impl CapReason {
    pub fn as_str(self) -> &'static str {
        match self {
            CapReason::Candidate => "candidate",
            CapReason::Daily => "daily",
            CapReason::Remaining => "remaining",
        }
    }
}

#[derive(Debug)]
pub enum NewsApiError {
    /// Refused before any request; nothing was spent.
    Cap(CapReason),
    /// Network error or 429/5xx on every attempt.
    Http(HttpError),
    /// A non-200 status (401 bad key, 402/403 out of tokens, ...).
    Status {
        status: u16,
        raw_path: String,
    },
    /// HTTP 200 whose body is not an article result (invalid JSON, or an `error` field).
    Parse {
        message: String,
        raw_path: String,
    },
    Db(String),
}

impl fmt::Display for NewsApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NewsApiError::Cap(r) => write!(f, "newsapi cap reached: {}", r.as_str()),
            NewsApiError::Http(e) => write!(f, "newsapi request failed: {e}"),
            NewsApiError::Status { status, .. } => write!(f, "newsapi http status {status}"),
            NewsApiError::Parse { message, .. } => write!(f, "newsapi bad response: {message}"),
            NewsApiError::Db(m) => write!(f, "newsapi ledger database error: {m}"),
        }
    }
}

impl std::error::Error for NewsApiError {}

impl From<rusqlite::Error> for NewsApiError {
    fn from(e: rusqlite::Error) -> NewsApiError {
        NewsApiError::Db(e.to_string())
    }
}

#[derive(Debug)]
pub struct NewsApiSearch {
    pub hits: Vec<Hit>,
    pub tokens_spent: u32,
    /// Tokens left after this call, per our ledger.
    pub remaining: i64,
    /// The API's `x-ratelimit-remaining` header, when sent (logged only).
    pub header_remaining: Option<i64>,
}

fn kv_i64(conn: &Connection, key: &str, default: i64) -> rusqlite::Result<i64> {
    let v: Option<String> = conn
        .query_row("SELECT value FROM kv WHERE key = ?1", [key], |r| r.get(0))
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })?;
    Ok(v.and_then(|s| s.trim().parse().ok()).unwrap_or(default))
}

fn set_kv(conn: &Connection, key: &str, value: i64) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
        params![key, value.to_string()],
    )?;
    Ok(())
}

/// Tokens spent per the `searches` log for this candidate, and since UTC midnight.
fn spent(conn: &Connection, candidate_id: i64, now: i64) -> rusqlite::Result<(i64, i64)> {
    let day_start = now - now.rem_euclid(86_400);
    let candidate = conn.query_row(
        "SELECT COALESCE(SUM(tokens_used), 0) FROM searches WHERE source = 'newsapi' AND candidate_id = ?1",
        [candidate_id],
        |r| r.get(0),
    )?;
    let today = conn.query_row(
        "SELECT COALESCE(SUM(tokens_used), 0) FROM searches WHERE source = 'newsapi' AND ts >= ?1",
        [day_start],
        |r| r.get(0),
    )?;
    Ok((candidate, today))
}

/// `Ok` when one more token may be spent.
fn check_caps(conn: &Connection, candidate_id: i64, now: i64) -> Result<(), NewsApiError> {
    let (candidate, today) = spent(conn, candidate_id, now)?;
    if candidate >= MAX_PER_CANDIDATE {
        return Err(NewsApiError::Cap(CapReason::Candidate));
    }
    if today >= MAX_PER_DAY {
        return Err(NewsApiError::Cap(CapReason::Daily));
    }
    if kv_i64(conn, KV_REMAINING, START_TOKENS)? <= RESERVE {
        return Err(NewsApiError::Cap(CapReason::Remaining));
    }
    Ok(())
}

fn parse_time(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.timestamp())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map(|d| d.and_utc().timestamp())
        })
        .ok()
}

fn snippet(body: &str) -> String {
    let flat = body.split_whitespace().collect::<Vec<_>>().join(" ");
    flat.chars().take(SNIPPET_CHARS).collect()
}

/// `articles.results[]` as hits; `None` when the body has no such array.
fn parse_articles(body: &Value) -> Option<Vec<Hit>> {
    let results = body.get("articles")?.get("results")?.as_array()?;
    Some(
        results
            .iter()
            .filter_map(|a| {
                let link = a.get("url")?.as_str()?.trim().to_string();
                let text = |k: &str| a.get(k).and_then(Value::as_str).unwrap_or("");
                let published = a
                    .get("dateTimePub")
                    .and_then(Value::as_str)
                    .and_then(parse_time)
                    .or_else(|| {
                        a.get("dateTime")
                            .and_then(Value::as_str)
                            .and_then(parse_time)
                    });
                Some(Hit {
                    title: text("title").trim().to_string(),
                    link,
                    source: a
                        .get("source")
                        .and_then(|s| s.get("title"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    rss_pub_ts: None,
                    snippet: snippet(text("body")),
                    via: Via::NewsApi,
                    newsapi_pub_ts: published,
                })
            })
            .collect(),
    )
}

/// One OR-combined article search over the last 30 days, newest first, for `phrases`. Refuses with
/// [`NewsApiError::Cap`] (nothing sent, nothing logged) when a cap is reached. Otherwise the attempt gets a
/// `searches` row (`trigger_error` = the RSS failure that made us call), and a successful page costs one token.
pub async fn search(
    http: &Http,
    conn: &Connection,
    ep: &NewsEndpoints,
    key: &str,
    phrases: &[String],
    candidate_id: i64,
    trigger_error: &str,
) -> Result<NewsApiSearch, NewsApiError> {
    let now = Utc::now();
    check_caps(conn, candidate_id, now.timestamp())?;

    let url = format!(
        "{}/api/v1/article/getArticles",
        ep.newsapi_base.trim_end_matches('/')
    );
    let body = json!({
        "apiKey": key,
        "keyword": phrases,
        "keywordOper": "or",
        "articlesSortBy": "date",
        "articlesCount": 100,
        "articlesPage": 1,
        "resultType": "articles",
        "dataType": ["news"],
        "dateStart": (now - Duration::days(WINDOW_DAYS)).format("%Y-%m-%d").to_string(),
    });
    let query = phrases.join(" | ");
    let log_row = |r: SearchRow| insert_search(conn, &r);
    let trigger = Some(trigger_error).filter(|t| !t.is_empty());

    let res = match http
        .send(
            Service::NewsApi,
            http.client().post(&url).json(&body),
            CallCtx {
                label: "newsapi",
                candidate_id: Some(candidate_id),
                max_attempts: MAX_ATTEMPTS,
                store_raw: true,
            },
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            log_row(SearchRow {
                candidate_id,
                source: "newsapi",
                query: &query,
                url: &url,
                http_status: None,
                n_results: None,
                ok: false,
                error: Some(&msg),
                raw_path: None,
                latency_ms: None,
                tokens_used: Some(0),
                trigger_error: trigger,
            })?;
            return Err(NewsApiError::Http(e));
        }
    };

    let failure = |error: NewsApiError, msg: String| -> Result<NewsApiSearch, NewsApiError> {
        log_row(SearchRow {
            candidate_id,
            source: "newsapi",
            query: &query,
            url: &url,
            http_status: Some(res.status),
            n_results: None,
            ok: false,
            error: Some(&msg),
            raw_path: Some(&res.raw_path),
            latency_ms: Some(res.elapsed_ms),
            tokens_used: Some(0),
            trigger_error: trigger,
        })?;
        Err(error)
    };

    if res.status != 200 {
        return failure(
            NewsApiError::Status {
                status: res.status,
                raw_path: res.raw_path.clone(),
            },
            format!("http status {}", res.status),
        );
    }
    let parsed: Result<Value, _> = serde_json::from_slice(&res.body);
    let hits = match &parsed {
        Ok(v) => match parse_articles(v) {
            Some(h) => h,
            None => {
                let message = v
                    .get("error")
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no articles.results in response".to_string());
                return failure(
                    NewsApiError::Parse {
                        message: message.clone(),
                        raw_path: res.raw_path.clone(),
                    },
                    message,
                );
            }
        },
        Err(e) => {
            let message = format!("invalid json: {e}");
            return failure(
                NewsApiError::Parse {
                    message: message.clone(),
                    raw_path: res.raw_path.clone(),
                },
                message,
            );
        }
    };

    // One page: one token, counted by us. The API's remaining header is only reported.
    let header_remaining = res
        .headers
        .get("x-ratelimit-remaining")
        .and_then(|v| v.trim().parse::<i64>().ok());
    let remaining = with_tx(conn, |tx| {
        let used = kv_i64(tx, KV_USED, 0)? + 1;
        let remaining = kv_i64(tx, KV_REMAINING, START_TOKENS)? - 1;
        set_kv(tx, KV_USED, used)?;
        set_kv(tx, KV_REMAINING, remaining)?;
        insert_search(
            tx,
            &SearchRow {
                candidate_id,
                source: "newsapi",
                query: &query,
                url: &url,
                http_status: Some(res.status),
                n_results: Some(hits.len()),
                ok: true,
                error: None,
                raw_path: Some(&res.raw_path),
                latency_ms: Some(res.elapsed_ms),
                tokens_used: Some(1),
                trigger_error: trigger,
            },
        )?;
        Ok(remaining)
    })
    .map_err(|e| NewsApiError::Db(e.to_string()))?;

    Ok(NewsApiSearch {
        hits,
        tokens_spent: 1,
        remaining,
        header_remaining,
    })
}
