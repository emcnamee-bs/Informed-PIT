//! Bing News RSS search: one request per query, parsed into [`Hit`]s.
//!
//! A search fails (so the caller may fall back to NewsAPI.ai) when the request fails after retries, the
//! status is not 200, the response is HTML (a consent or block page), or the body is not RSS XML with a
//! `<channel>`. A valid channel with no items is a successful, empty search.

use std::fmt;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Weekday};
use quick_xml::events::Event;
use quick_xml::Reader;

use super::{Hit, NewsEndpoints, Via};
use crate::http::{CallCtx, Http, HttpError, Service};

/// Attempts per query for transient failures (429, 5xx, network).
const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug)]
pub enum RssErrorKind {
    /// Network error, 429 or 5xx on every attempt.
    Transport(String),
    /// Any other non-200 status.
    Status(u16),
    /// The response is an HTML page (typically a consent wall), not a feed.
    Html,
    /// The body is not well-formed XML.
    NotRss(String),
    /// Well-formed, but no `<channel>`.
    NoChannel,
}

#[derive(Debug)]
pub struct RssError {
    pub kind: RssErrorKind,
    pub http_status: Option<u16>,
    /// Stored raw body, when a response arrived.
    pub raw_path: Option<String>,
    pub elapsed_ms: Option<u64>,
    /// Request URL.
    pub url: String,
}

impl fmt::Display for RssError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            RssErrorKind::Transport(m) => write!(f, "rss request failed: {m}"),
            RssErrorKind::Status(s) => write!(f, "rss http status {s}"),
            RssErrorKind::Html => write!(f, "rss response is an html page, not a feed"),
            RssErrorKind::NotRss(m) => write!(f, "rss body does not parse: {m}"),
            RssErrorKind::NoChannel => write!(f, "rss body has no channel"),
        }
    }
}

impl std::error::Error for RssError {}

/// A successful search with what the `searches` log needs.
#[derive(Debug)]
pub struct RssSearch {
    pub hits: Vec<Hit>,
    pub http_status: u16,
    pub raw_path: String,
    pub elapsed_ms: u64,
}

/// Bing's `qft` value limiting results to the past 24 hours (checked live 2026-11-19: `interval="4"` is
/// the past hour, `"7"` the past 24 hours, `"8"` the past 7 days).
pub(crate) const PAST_DAY_FILTER: &str = "interval=\"7\"";

/// URL requested for query `q`: `<base>/news/search?q=<q>&format=rss&qft=interval%3d%227%22`.
pub(crate) fn request_url(ep: &NewsEndpoints, q: &str) -> reqwest::Url {
    reqwest::Url::parse_with_params(
        &format!("{}/news/search", ep.rss_base.trim_end_matches('/')),
        [("q", q), ("format", "rss"), ("qft", PAST_DAY_FILTER)],
    )
    .expect("rss base url is valid")
}

pub async fn search(
    http: &Http,
    ep: &NewsEndpoints,
    q: &str,
    candidate_id: i64,
) -> Result<Vec<Hit>, RssError> {
    search_detailed(http, ep, q, candidate_id)
        .await
        .map(|r| r.hits)
}

pub async fn search_detailed(
    http: &Http,
    ep: &NewsEndpoints,
    q: &str,
    candidate_id: i64,
) -> Result<RssSearch, RssError> {
    let url = request_url(ep, q);
    let url_text = url.to_string();
    let fail = |kind, http_status, raw_path, elapsed_ms| RssError {
        kind,
        http_status,
        raw_path,
        elapsed_ms,
        url: url_text.clone(),
    };
    let res = http
        .send(
            Service::Rss,
            http.client().get(url),
            CallCtx {
                label: "rss",
                candidate_id: Some(candidate_id),
                max_attempts: MAX_ATTEMPTS,
                store_raw: true,
            },
        )
        .await
        .map_err(|e| match e {
            HttpError::Exhausted { .. }
            | HttpError::BadRequest(_)
            | HttpError::Blocked(_)
            | HttpError::TooLarge { .. } => {
                fail(RssErrorKind::Transport(e.to_string()), None, None, None)
            }
        })?;
    let with_body = |kind| {
        fail(
            kind,
            Some(res.status),
            Some(res.raw_path.clone()),
            Some(res.elapsed_ms),
        )
    };
    if res.status != 200 {
        return Err(with_body(RssErrorKind::Status(res.status)));
    }
    if res.content_type().is_some_and(|c| c.contains("html")) {
        return Err(with_body(RssErrorKind::Html));
    }
    match parse_feed(&String::from_utf8_lossy(&res.body)) {
        Ok(hits) => Ok(RssSearch {
            hits,
            http_status: res.status,
            raw_path: res.raw_path.clone(),
            elapsed_ms: res.elapsed_ms,
        }),
        Err(kind) => Err(with_body(kind)),
    }
}

/// Unix seconds of a Bing `<pubDate>`. Bing labels the value `GMT` but it is US Pacific wall-clock time:
/// checked live 2026-11-19 against the publisher pages' own metadata, the page time was the RSS value plus
/// exactly 8.00 h on every article not updated since (about three quarters of those checked). So the label is
/// ignored and the wall time is read as Pacific, with US daylight saving (second Sunday of March 02:00 to
/// first Sunday of November 02:00). The daylight-saving half is an unverified assumption (only November was
/// seen); a wrong guess is off by one hour, well inside the 6 h agreement check against page metadata.
/// This applies only when the stated offset is zero (`GMT`, `UT`, `+0000`); a value that states another
/// offset (`-0800`, `+0100`) is trusted as written.
pub fn parse_pub_date(raw: &str) -> Option<i64> {
    let parsed = DateTime::parse_from_rfc2822(raw.trim()).ok()?;
    if parsed.offset().local_minus_utc() != 0 {
        return Some(parsed.timestamp());
    }
    let wall = parsed.naive_utc();
    Some(wall.and_utc().timestamp() + pacific_utc_offset_hours(wall) * 3600)
}

/// Hours to add to a US Pacific wall time to get UTC: 7 in daylight time, 8 in standard time.
fn pacific_utc_offset_hours(wall: NaiveDateTime) -> i64 {
    let year = wall.year();
    let start = NaiveDate::from_weekday_of_month_opt(year, 3, Weekday::Sun, 2)
        .and_then(|d| d.and_hms_opt(2, 0, 0));
    let end = NaiveDate::from_weekday_of_month_opt(year, 11, Weekday::Sun, 1)
        .and_then(|d| d.and_hms_opt(2, 0, 0));
    match (start, end) {
        (Some(s), Some(e)) if wall >= s && wall < e => 7,
        _ => 8,
    }
}

/// Fields of an `<item>` we read.
#[derive(Clone, Copy, PartialEq)]
enum Field {
    Title,
    Link,
    PubDate,
    Description,
    Source,
}

#[derive(Default)]
struct Item {
    title: String,
    link: String,
    pub_date: String,
    description: String,
    source: String,
}

impl Item {
    fn field(&mut self, f: Field) -> &mut String {
        match f {
            Field::Title => &mut self.title,
            Field::Link => &mut self.link,
            Field::PubDate => &mut self.pub_date,
            Field::Description => &mut self.description,
            Field::Source => &mut self.source,
        }
    }

    fn into_hit(self) -> Hit {
        Hit {
            title: self.title.trim().to_string(),
            link: self.link.trim().to_string(),
            source: self.source.trim().to_string(),
            rss_pub_ts: parse_pub_date(&self.pub_date),
            snippet: strip_html(&self.description),
            via: Via::Rss,
            newsapi_pub_ts: None,
        }
    }
}

fn field_of(name: &[u8]) -> Option<Field> {
    match name {
        b"title" => Some(Field::Title),
        b"link" => Some(Field::Link),
        b"pubDate" => Some(Field::PubDate),
        b"description" => Some(Field::Description),
        // Bing names the publisher `<News:Source>`; plain `<source>` is the RSS 2.0 spelling.
        b"News:Source" | b"source" => Some(Field::Source),
        _ => None,
    }
}

/// Parse an RSS 2.0 document into hits. Items with no link are skipped.
pub fn parse_feed(text: &str) -> Result<Vec<Hit>, RssErrorKind> {
    let mut reader = Reader::from_str(text);
    let mut saw_channel = false;
    let mut depth: i64 = 0;
    let mut item: Option<Item> = None;
    let mut field: Option<Field> = None;
    let mut hits = Vec::new();
    loop {
        let ev = reader
            .read_event()
            .map_err(|e| RssErrorKind::NotRss(e.to_string()))?;
        match ev {
            Event::Start(e) => {
                depth += 1;
                let name = e.name();
                match name.as_ref() {
                    b"channel" => saw_channel = true,
                    b"item" if saw_channel => item = Some(Item::default()),
                    n if item.is_some() => field = field_of(n),
                    _ => {}
                }
            }
            Event::Empty(e) if e.name().as_ref() == b"channel" => saw_channel = true,
            Event::End(e) => {
                depth -= 1;
                if e.name().as_ref() == b"item" {
                    if let Some(it) = item.take() {
                        let hit = it.into_hit();
                        if !hit.link.is_empty() {
                            hits.push(hit);
                        }
                    }
                }
                field = None;
            }
            Event::Text(t) => {
                if let (Some(it), Some(f)) = (item.as_mut(), field) {
                    let s = t
                        .unescape()
                        .map_err(|e| RssErrorKind::NotRss(e.to_string()))?;
                    it.field(f).push_str(&s);
                }
            }
            Event::CData(c) => {
                if let (Some(it), Some(f)) = (item.as_mut(), field) {
                    it.field(f).push_str(&String::from_utf8_lossy(&c));
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if depth != 0 {
        return Err(RssErrorKind::NotRss(
            "document ends inside an element".into(),
        ));
    }
    if !saw_channel {
        return Err(RssErrorKind::NoChannel);
    }
    Ok(hits)
}

/// Plain text of an HTML fragment: tags dropped, common entities decoded, whitespace collapsed.
fn strip_html(html: &str) -> String {
    let mut text = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' if in_tag => {
                in_tag = false;
                text.push(' ');
            }
            _ if !in_tag => text.push(c),
            _ => {}
        }
    }
    let text = text
        .replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&");
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
