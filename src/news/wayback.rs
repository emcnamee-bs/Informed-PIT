//! Wayback Machine CDX lookup: backup source for an article's publish time (spec §8 step 4). The earliest
//! capture the archive holds for a URL is an upper bound on when the page was published (a page cannot be
//! captured before it exists). The request has no lower date limit on purpose: an old page that was captured
//! again recently must show its true earliest capture, not the recent one, or it would be mistaken for fresh
//! news.

use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use serde_json::Value;

use super::NewsEndpoints;
use crate::http::{CallCtx, Http, Service};

/// Attempts for transient failures (429, 5xx, network).
const MAX_ATTEMPTS: u32 = 2;

/// One CDX lookup: the earliest capture (if any) and the stored raw response.
#[derive(Debug)]
pub struct Capture {
    pub ts: Option<i64>,
    pub raw_path: String,
}

/// CDX request: `<base>/cdx/search/cdx?url=<url>&output=json&limit=1&fl=timestamp`. Results come oldest
/// first, so `limit=1` is the earliest capture ever.
pub(crate) fn request_url(ep: &NewsEndpoints, url: &str) -> reqwest::Url {
    reqwest::Url::parse_with_params(
        &format!("{}/cdx/search/cdx", ep.wayback_base.trim_end_matches('/')),
        [
            ("url", url),
            ("output", "json"),
            ("limit", "1"),
            ("fl", "timestamp"),
        ],
    )
    .expect("wayback base url is valid")
}

/// Look up the earliest capture of `url`, returning it with the raw response path (for the log).
/// Errors: transport failure after retries, a non-200 status, or a body that is not the CDX JSON.
pub async fn lookup(
    http: &Http,
    ep: &NewsEndpoints,
    url: &str,
    candidate_id: i64,
) -> Result<Capture> {
    let res = http
        .send(
            Service::Wayback,
            http.client().get(request_url(ep, url)),
            CallCtx {
                label: "wayback",
                candidate_id: Some(candidate_id),
                max_attempts: MAX_ATTEMPTS,
                store_raw: true,
            },
        )
        .await
        .map_err(|e| anyhow!("wayback request failed: {e}"))?;
    if res.status != 200 {
        return Err(anyhow!("wayback http status {}", res.status));
    }
    let ts = parse_cdx(&String::from_utf8_lossy(&res.body))?;
    Ok(Capture {
        ts,
        raw_path: res.raw_path,
    })
}

/// Earliest capture time (UTC unix seconds) of `url`; `None` when the archive has no capture.
pub async fn earliest_capture(
    http: &Http,
    ep: &NewsEndpoints,
    url: &str,
    candidate_id: i64,
) -> Result<Option<i64>> {
    Ok(lookup(http, ep, url, candidate_id).await?.ts)
}

/// CDX JSON is `[["timestamp"],["20261118143000"]]` (a header row, then captures), or `[]` / an empty body
/// when there is none.
pub(crate) fn parse_cdx(body: &str) -> Result<Option<i64>> {
    if body.trim().is_empty() {
        return Ok(None);
    }
    let rows: Vec<Vec<Value>> =
        serde_json::from_str(body).map_err(|e| anyhow!("wayback body is not CDX json: {e}"))?;
    let first = rows
        .iter()
        .filter_map(|r| r.first().and_then(Value::as_str))
        .find(|s| *s != "timestamp");
    let Some(stamp) = first else {
        return Ok(None);
    };
    let ts = NaiveDateTime::parse_from_str(stamp, "%Y%m%d%H%M%S")
        .map_err(|e| anyhow!("wayback timestamp {stamp:?}: {e}"))?;
    Ok(Some(ts.and_utc().timestamp()))
}
