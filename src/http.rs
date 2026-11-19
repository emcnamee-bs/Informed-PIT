//! Shared HTTP layer. Every external call (Kalshi, RSS, NewsAPI.ai, Wayback, articles, Anthropic)
//! goes through [`Http::send`], which enforces per-service spacing, retries transient failures
//! with exponential backoff, logs every attempt and stores the final body.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Instant;

use crate::eventlog::EventLog;
pub use crate::hostguard::HostGuard;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Service {
    Kalshi,
    Rss,
    NewsApi,
    Wayback,
    Article,
    Anthropic,
}

impl Service {
    pub fn as_str(self) -> &'static str {
        match self {
            Service::Kalshi => "kalshi",
            Service::Rss => "rss",
            Service::NewsApi => "newsapi",
            Service::Wayback => "wayback",
            Service::Article => "article",
            Service::Anthropic => "anthropic",
        }
    }

    /// Production minimum start-to-start spacing between requests to this service.
    fn default_spacing(self) -> Duration {
        match self {
            Service::Kalshi => Duration::from_millis(500),
            Service::Rss => Duration::from_millis(3000),
            Service::Wayback => Duration::from_millis(1000),
            Service::NewsApi => Duration::from_millis(1000),
            Service::Anthropic | Service::Article => Duration::ZERO,
        }
    }
}

const ALL_SERVICES: [Service; 6] = [
    Service::Kalshi,
    Service::Rss,
    Service::NewsApi,
    Service::Wayback,
    Service::Article,
    Service::Anthropic,
];

/// Max concurrent article fetches.
const ARTICLE_CONCURRENCY: usize = 2;
/// Cap on a single backoff wait, including a server-supplied `Retry-After`.
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// Query parameter names whose values must never reach a log (compared lowercase).
const SECRET_PARAMS: [&str; 4] = ["apikey", "x-api-key", "api_key", "api-key"];

#[derive(Debug, Clone, Copy)]
pub struct CallCtx {
    /// Short name used in the stored raw file name and the log.
    pub label: &'static str,
    pub candidate_id: Option<i64>,
    pub max_attempts: u32,
    /// Keep the final body under `raw/`. Off for the watcher's every-minute Kalshi reads, whose content is
    /// stored in `snapshots` and `trades` anyway.
    pub store_raw: bool,
}

#[derive(Debug, Clone)]
pub struct HttpResult {
    pub status: u16,
    pub body: Bytes,
    /// Final URL after redirects, with secret query values redacted.
    pub final_url: String,
    pub elapsed_ms: u64,
    /// Path of the gzipped raw body, relative to the data dir; empty when the call asked not to store it.
    pub raw_path: String,
    /// Response headers of the final attempt: lowercase names, values that are valid text.
    pub headers: HashMap<String, String>,
}

impl HttpResult {
    /// Value of the `content-type` header, lowercased.
    pub fn content_type(&self) -> Option<String> {
        self.headers.get("content-type").map(|v| v.to_lowercase())
    }
}

#[derive(Debug)]
pub enum HttpError {
    /// Every attempt failed with 429, 5xx or a network error. Details are in the `http` events.
    Exhausted { attempts: u32 },
    /// The request could not be built or cannot be replayed for a retry.
    BadRequest(String),
    /// The host guard refused the URL (an article fetch aimed at a local, private or tailnet host).
    Blocked(String),
    /// The response declared a body larger than the byte limit of [`Http::send_capped`].
    TooLarge { limit: usize },
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Exhausted { attempts } => write!(f, "gave up after {attempts} attempts"),
            HttpError::BadRequest(m) => write!(f, "bad request: {m}"),
            HttpError::Blocked(m) => write!(f, "blocked host: {m}"),
            HttpError::TooLarge { limit } => write!(f, "response body larger than {limit} bytes"),
        }
    }
}

impl std::error::Error for HttpError {}

/// Enforces a minimum start-to-start interval between calls to one service.
struct Gate {
    spacing: Duration,
    next_start: Mutex<Instant>,
}

impl Gate {
    fn new(spacing: Duration) -> Gate {
        Gate {
            spacing,
            next_start: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self) {
        if self.spacing.is_zero() {
            return;
        }
        // Holding the lock while sleeping queues concurrent callers in order.
        let mut next = self.next_start.lock().await;
        let now = Instant::now();
        if *next > now {
            tokio::time::sleep_until(*next).await;
        }
        *next = Instant::now() + self.spacing;
    }
}

pub struct Http {
    client: reqwest::Client,
    /// Client for [`Service::Article`]: follows no redirects (the caller re-checks every hop) and resolves
    /// names through the host guard.
    article_client: reqwest::Client,
    guard: HostGuard,
    log: EventLog,
    gates: HashMap<Service, Gate>,
    article_slots: Arc<Semaphore>,
    backoff_base: Duration,
}

impl Http {
    /// Production spacings: Kalshi 500 ms, Rss 3 s, Wayback 1 s, NewsApi 1 s, Anthropic none,
    /// Article none but at most 2 in flight.
    pub fn new(log: EventLog) -> Http {
        let spacing = ALL_SERVICES
            .iter()
            .map(|s| (*s, s.default_spacing()))
            .collect();
        Http::with_spacing(log, spacing)
    }

    /// Test constructor: explicit spacing per service (missing services get none).
    pub fn with_spacing(log: EventLog, spacing: HashMap<Service, Duration>) -> Http {
        let gates = ALL_SERVICES
            .iter()
            .map(|s| {
                (
                    *s,
                    Gate::new(spacing.get(s).copied().unwrap_or(Duration::ZERO)),
                )
            })
            .collect();
        let guard = HostGuard::strict();
        Http {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .user_agent(USER_AGENT)
                .build()
                .expect("reqwest client"),
            article_client: article_client(&guard),
            guard,
            log,
            gates,
            article_slots: Arc::new(Semaphore::new(ARTICLE_CONCURRENCY)),
            backoff_base: Duration::from_secs(1),
        }
    }

    /// Replace the host guard for article fetches (default: [`HostGuard::strict`]). Tests use
    /// [`HostGuard::allowing_loopback`] to reach a mock server.
    pub fn with_article_guard(mut self, guard: HostGuard) -> Http {
        self.article_client = article_client(&guard);
        self.guard = guard;
        self
    }

    /// Override the backoff base (default 1 s, so waits are 2 s, 4 s, 8 s ...). For tests.
    pub fn with_backoff_base(mut self, base: Duration) -> Http {
        self.backoff_base = base;
        self
    }

    /// Client for building requests to pass to [`Http::send`].
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Send `req`, retrying 429, 5xx and network errors up to `ctx.max_attempts` times (network errors and
    /// timeouts are not retried for [`Service::Anthropic`]). Other statuses (including 4xx) are returned as
    /// `Ok` for the caller to interpret.
    pub async fn send(
        &self,
        svc: Service,
        req: reqwest::RequestBuilder,
        ctx: CallCtx,
    ) -> Result<HttpResult, HttpError> {
        self.send_capped(svc, req, ctx, None).await
    }

    /// [`Http::send`] with a limit on the response body. With `Some(limit)`, a response whose
    /// `Content-Length` is over `limit` is refused ([`HttpError::TooLarge`], nothing stored); otherwise the
    /// body is read as a stream and cut at `limit` bytes (after decompression), and only that part is
    /// returned and stored.
    ///
    /// [`Service::Article`] requests are checked by the host guard first ([`HttpError::Blocked`]) and are
    /// sent by a client that does not follow redirects: a 3xx comes back as an ordinary result and the caller
    /// sends the next hop through here, so every hop is checked.
    pub async fn send_capped(
        &self,
        svc: Service,
        req: reqwest::RequestBuilder,
        ctx: CallCtx,
        max_bytes: Option<usize>,
    ) -> Result<HttpResult, HttpError> {
        let request = req
            .build()
            .map_err(|e| HttpError::BadRequest(e.without_url().to_string()))?;
        let max_attempts = ctx.max_attempts.max(1);
        let url_for_log = redact_url(request.url());
        if svc == Service::Article {
            if let Err(reason) = self.guard.check_url(request.url()).await {
                self.log.log(
                    "http_blocked",
                    ctx.candidate_id,
                    json!({"service": svc.as_str(), "label": ctx.label, "url": url_for_log, "reason": reason}),
                );
                return Err(HttpError::Blocked(reason));
            }
        }

        for attempt in 1..=max_attempts {
            let attempt_req = request
                .try_clone()
                .ok_or_else(|| HttpError::BadRequest("request body cannot be replayed".into()))?;

            let _permit = if svc == Service::Article {
                Some(
                    self.article_slots
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore never closed"),
                )
            } else {
                None
            };
            self.gates[&svc].wait().await;

            let started = Instant::now();
            let outcome = self.attempt(svc, attempt_req, max_bytes).await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let is_last = attempt == max_attempts;

            let (status, retry_after, retryable) = match &outcome {
                Outcome::Response {
                    status,
                    retry_after,
                    ..
                } => (
                    Some(*status),
                    *retry_after,
                    *status == 429 || *status >= 500,
                ),
                // An Anthropic request that failed on the network or timed out may still have been run (and
                // billed): it is not sent again (ruling R24). 429 and 5xx answers are retried as usual.
                Outcome::Network(_) => (None, None, svc != Service::Anthropic),
                Outcome::TooLarge { status } => (Some(*status), None, false),
            };

            let mut payload = json!({
                "service": svc.as_str(),
                "label": ctx.label,
                "url": url_for_log,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "attempt": attempt,
                "max_attempts": max_attempts,
            });
            if let Outcome::Network(msg) = &outcome {
                payload["error"] = json!(msg);
            }

            if retryable && !is_last {
                self.log.log("http", ctx.candidate_id, payload);
                tokio::time::sleep(backoff_delay(attempt, self.backoff_base, retry_after)).await;
                continue;
            }

            // Final attempt: store the body (when there is one) and log its path.
            match outcome {
                Outcome::Response {
                    status,
                    body,
                    final_url,
                    headers,
                    ..
                } => {
                    let raw_path = if ctx.store_raw {
                        self.log.store_raw(ctx.label, &body)
                    } else {
                        String::new()
                    };
                    payload["raw_path"] = if ctx.store_raw {
                        json!(raw_path)
                    } else {
                        Value::Null
                    };
                    self.log.log("http", ctx.candidate_id, payload);
                    if retryable {
                        return Err(HttpError::Exhausted { attempts: attempt });
                    }
                    return Ok(HttpResult {
                        status,
                        body,
                        final_url,
                        elapsed_ms,
                        raw_path,
                        headers,
                    });
                }
                Outcome::Network(_) => {
                    self.log.log("http", ctx.candidate_id, payload);
                    return Err(HttpError::Exhausted { attempts: attempt });
                }
                Outcome::TooLarge { .. } => {
                    let limit = max_bytes.unwrap_or_default();
                    payload["error"] = json!(format!("content-length over the {limit} byte limit"));
                    self.log.log("http", ctx.candidate_id, payload);
                    return Err(HttpError::TooLarge { limit });
                }
            }
        }
        unreachable!("loop returns on the last attempt")
    }

    async fn attempt(
        &self,
        svc: Service,
        req: reqwest::Request,
        max_bytes: Option<usize>,
    ) -> Outcome {
        let client = if svc == Service::Article {
            &self.article_client
        } else {
            &self.client
        };
        let mut resp = match client.execute(req).await {
            Ok(r) => r,
            Err(e) => return Outcome::Network(redact_error(&e)),
        };
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let final_url = redact_url(resp.url());
        let headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_lowercase(), v.to_str().ok()?.to_string())))
            .collect();
        let body = match max_bytes {
            None => match resp.bytes().await {
                Ok(b) => b,
                Err(e) => return Outcome::Network(redact_error(&e)),
            },
            Some(limit) => {
                if resp.content_length().is_some_and(|n| n > limit as u64) {
                    return Outcome::TooLarge { status };
                }
                let mut buf: Vec<u8> = Vec::new();
                loop {
                    match resp.chunk().await {
                        Ok(Some(chunk)) => {
                            let room = limit - buf.len();
                            if chunk.len() >= room {
                                buf.extend_from_slice(&chunk[..room]);
                                break;
                            }
                            buf.extend_from_slice(&chunk);
                        }
                        Ok(None) => break,
                        Err(e) => return Outcome::Network(redact_error(&e)),
                    }
                }
                Bytes::from(buf)
            }
        };
        Outcome::Response {
            status,
            retry_after,
            final_url,
            headers,
            body,
        }
    }
}

const USER_AGENT: &str = "informed-pit/0.1 (paper-trading research)";

/// The article client: no automatic redirects, names resolved through `guard`.
fn article_client(guard: &HostGuard) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(USER_AGENT)
        .redirect(reqwest::redirect::Policy::none())
        // A proxy would resolve the name itself and bypass the host guard.
        .no_proxy()
        .dns_resolver(Arc::new(guard.resolver()))
        .build()
        .expect("reqwest client")
}

enum Outcome {
    /// The declared `Content-Length` is over the limit; nothing was read.
    TooLarge {
        status: u16,
    },
    Response {
        status: u16,
        retry_after: Option<Duration>,
        final_url: String,
        headers: HashMap<String, String>,
        body: Bytes,
    },
    Network(String),
}

/// Wait before retry number `attempt` (1-based) has failed: `base * 2^attempt` (2 s, 4 s, ... with
/// the default 1 s base), or the server's `Retry-After` when given, never more than 60 s.
fn backoff_delay(attempt: u32, base: Duration, retry_after: Option<Duration>) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt.min(16)));
    retry_after.unwrap_or(exp).min(MAX_BACKOFF)
}

/// URL for logs: credentials dropped, values of secret query parameters replaced by `REDACTED`.
fn redact_url(url: &reqwest::Url) -> String {
    let mut u = url.clone();
    let _ = u.set_username("");
    let _ = u.set_password(None);
    if u.query().is_some() {
        let pairs: Vec<(String, String)> = u
            .query_pairs()
            .map(|(k, v)| {
                let secret = SECRET_PARAMS.contains(&k.to_ascii_lowercase().as_str());
                (
                    k.into_owned(),
                    if secret {
                        "REDACTED".to_string()
                    } else {
                        v.into_owned()
                    },
                )
            })
            .collect();
        u.query_pairs_mut().clear().extend_pairs(pairs);
    }
    u.to_string()
}

/// reqwest error text with its URL (which may carry a secret query value) removed.
fn redact_error(e: &reqwest::Error) -> String {
    let mut parts = vec![e.to_string()];
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        parts.push(s.to_string());
        src = s.source();
    }
    let text = parts.join(": ");
    match e.url() {
        Some(url) => text.replace(url.as_str(), &redact_url(url)),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_capped_and_honours_retry_after() {
        let b = Duration::from_secs(1);
        assert_eq!(backoff_delay(1, b, None), Duration::from_secs(2));
        assert_eq!(backoff_delay(3, b, None), Duration::from_secs(8));
        assert_eq!(backoff_delay(10, b, None), Duration::from_secs(60));
        assert_eq!(
            backoff_delay(1, b, Some(Duration::from_secs(5))),
            Duration::from_secs(5)
        );
        assert_eq!(
            backoff_delay(1, b, Some(Duration::from_secs(900))),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn redact_url_hides_secret_values_only() {
        let u = reqwest::Url::parse("https://u:p@x.test/a?apiKey=S&q=a%20b&X-Api-Key=T").unwrap();
        let r = redact_url(&u);
        assert!(r.contains("apiKey=REDACTED"), "{r}");
        assert!(r.contains("X-Api-Key=REDACTED"), "{r}");
        assert!(r.contains("q=a+b") || r.contains("q=a%20b"));
        assert!(!r.contains("u:p@"));
    }
}
