//! Article publish time (spec §8 step 4). The page's own metadata is the primary source; RSS `pubDate` is
//! only a cross-check; NewsAPI.ai's `dateTimePub` and the Wayback Machine's earliest capture are backups.
//!
//! Everything here is pure: [`extract_pub_ts`] reads a page, [`date_article`] picks the time (the Wayback
//! lookup arrives as a closure so it only runs when needed), [`in_window`] is the window test.

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use scraper::{Html, Selector};
use serde_json::Value;

/// Metadata later than `now` by more than this is not trusted (clock skew allowance).
pub const FUTURE_SLACK_SECS: i64 = 300;
/// Metadata further than this from the RSS `pubDate` is not trusted.
pub const RSS_AGREEMENT_SECS: i64 = 6 * 3600;
/// An article may be published this long before the move started and still count.
pub const WINDOW_LEAD_SECS: i64 = 900;

/// Where the time used for an article came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeSource {
    /// The page's own metadata (the primary source).
    Metadata,
    /// NewsAPI.ai `dateTimePub` (backup 1).
    NewsApiPub,
    /// Earliest Wayback capture ever made, an upper bound on publication (backup 2).
    Wayback,
    /// No time anywhere: the article is excluded.
    #[default]
    None,
}

impl TimeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            TimeSource::Metadata => "metadata",
            TimeSource::NewsApiPub => "newsapi_pub",
            TimeSource::Wayback => "wayback",
            TimeSource::None => "none",
        }
    }
}

impl fmt::Display for TimeSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The chosen publish time and where it came from (`ts` is `None` exactly when `source` is `None`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dated {
    pub ts: Option<i64>,
    pub source: TimeSource,
}

/// Parse a page timestamp to UTC unix seconds. Accepts RFC 3339 / ISO 8601 with or without an offset (no
/// offset means UTC), RFC 2822, and a bare `YYYY-MM-DD` (midnight UTC) as the last resort.
pub fn parse_time(raw: &str) -> Option<i64> {
    parse_time_detail(raw).map(|(ts, _)| ts)
}

/// Like [`parse_time`], also saying whether the value was a bare date with no time of day. A date-only value
/// is only a day, not a publish time: see [`extract_pub_ts`].
pub fn parse_time_detail(raw: &str) -> Option<(i64, bool)> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(d) = DateTime::parse_from_rfc3339(s) {
        return Some((d.timestamp(), false));
    }
    if let Ok(d) = DateTime::parse_from_rfc2822(s) {
        return Some((d.timestamp(), false));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M%z",
    ] {
        if let Ok(d) = DateTime::parse_from_str(s, fmt) {
            return Some((d.timestamp(), false));
        }
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(d) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some((d.and_utc().timestamp(), false));
        }
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| (d.and_utc().timestamp(), true))
}

fn sel(css: &str) -> Selector {
    Selector::parse(css).expect("static selector")
}

/// First parseable `content` of a `<meta>` whose `property` or `name` equals `key` (case-insensitive).
fn meta_time(doc: &Html, key: &str) -> Option<(i64, bool)> {
    doc.select(&sel("meta")).find_map(|m| {
        let v = m.value();
        let matches = ["property", "name", "itemprop"]
            .iter()
            .filter_map(|a| v.attr(a))
            .any(|a| a.eq_ignore_ascii_case(key));
        if matches {
            v.attr("content").and_then(parse_time_detail)
        } else {
            None
        }
    })
}

/// First parseable `datePublished` anywhere in a JSON-LD value (arrays, objects and `@graph` are walked).
fn json_ld_date(v: &Value) -> Option<(i64, bool)> {
    match v {
        Value::Object(map) => {
            if let Some(t) = map
                .get("datePublished")
                .and_then(Value::as_str)
                .and_then(parse_time_detail)
            {
                return Some(t);
            }
            map.values().find_map(json_ld_date)
        }
        Value::Array(items) => items.iter().find_map(json_ld_date),
        _ => None,
    }
}

/// Suffix of the source tag of a value that was only a date (no time of day).
pub const DATE_ONLY_SUFFIX: &str = ":date_only";

/// Whether an [`extract_pub_ts`] source tag says the value was a bare date.
pub fn is_date_only(tag: &str) -> bool {
    tag.ends_with(DATE_ONLY_SUFFIX)
}

fn date_only_tag(tag: &'static str) -> &'static str {
    match tag {
        "meta:article:published_time" => "meta:article:published_time:date_only",
        "jsonld:datePublished" => "jsonld:datePublished:date_only",
        "time:datetime" => "time:datetime:date_only",
        "meta:og:updated_time" => "meta:og:updated_time:date_only",
        "meta:date" => "meta:date:date_only",
        _ => "meta:pubdate:date_only",
    }
}

/// The page's publish time and the tag of the place it was read from. Order: `article:published_time` meta,
/// JSON-LD `datePublished`, `<time datetime>`, then `og:updated_time` / `date` / `pubdate` meta. The first
/// value that has a time of day wins. A bare date (`2026-11-18`) is not a publish time; it is returned (as
/// midnight UTC, with a tag ending in [`DATE_ONLY_SUFFIX`]) only when no source has a full time, so it can be
/// recorded; callers must not use it as an exact time (see [`is_date_only`]).
pub fn extract_pub_ts(html: &str) -> Option<(i64, &'static str)> {
    let doc = Html::parse_document(html);
    let mut found: Vec<(i64, bool, &'static str)> = Vec::new();
    let mut add = |t: Option<(i64, bool)>, tag: &'static str| {
        if let Some((ts, date_only)) = t {
            found.push((ts, date_only, tag));
        }
    };
    add(
        meta_time(&doc, "article:published_time"),
        "meta:article:published_time",
    );
    let ld = sel("script[type=\"application/ld+json\"]");
    let ld_time = doc.select(&ld).find_map(|script| {
        let text: String = script.text().collect();
        serde_json::from_str::<Value>(text.trim())
            .ok()
            .as_ref()
            .and_then(json_ld_date)
    });
    add(ld_time, "jsonld:datePublished");
    add(
        doc.select(&sel("time[datetime]"))
            .find_map(|t| t.value().attr("datetime").and_then(parse_time_detail)),
        "time:datetime",
    );
    for (key, tag) in [
        ("og:updated_time", "meta:og:updated_time"),
        ("date", "meta:date"),
        ("pubdate", "meta:pubdate"),
    ] {
        add(meta_time(&doc, key), tag);
    }
    found
        .iter()
        .find(|(_, date_only, _)| !date_only)
        .map(|(ts, _, tag)| (*ts, *tag))
        .or_else(|| found.first().map(|(ts, _, tag)| (*ts, date_only_tag(tag))))
}

/// Pick the time used for the window test.
///
/// 1. Page metadata, when it is not in the future (more than 5 min after `now`) and, if the RSS item has a
///    `pubDate`, within 6 h of it.
/// 2. Otherwise NewsAPI.ai `dateTimePub`.
/// 3. Otherwise the earliest Wayback capture (`wayback` runs only here).
/// 4. Otherwise [`TimeSource::None`]: the article is excluded.
pub fn date_article(
    meta: Option<i64>,
    rss: Option<i64>,
    newsapi: Option<i64>,
    now: i64,
    wayback: impl FnOnce() -> Option<i64>,
) -> Dated {
    if let Some(m) = meta {
        let in_future = m > now + FUTURE_SLACK_SECS;
        let disagrees = rss.is_some_and(|r| (m - r).abs() > RSS_AGREEMENT_SECS);
        if !in_future && !disagrees {
            return Dated {
                ts: Some(m),
                source: TimeSource::Metadata,
            };
        }
    }
    if let Some(n) = newsapi {
        return Dated {
            ts: Some(n),
            source: TimeSource::NewsApiPub,
        };
    }
    match wayback() {
        Some(w) => Dated {
            ts: Some(w),
            source: TimeSource::Wayback,
        },
        None => Dated {
            ts: None,
            source: TimeSource::None,
        },
    }
}

/// Window test: `move_start − 15 min ≤ ts ≤ investigation_ts`.
pub fn in_window(ts: i64, move_start: i64, investigation_ts: i64) -> bool {
    ts >= move_start - WINDOW_LEAD_SECS && ts <= investigation_ts
}
