//! Anthropic Messages client for every Haiku and Sonnet call (phrases, per-article verdicts, Sonnet review).
//!
//! All calls ask for JSON through `output_config.format`, go through [`Http::send`] (retries, request log) and
//! through the spend governor: before each call an estimate is reserved and the call is refused when the UTC-day
//! spend or the run spend plus that estimate would pass its cap; after the call the real cost from `usage` and
//! the model that actually served it is written to `llm_calls`. The API key is only ever a request header; the
//! stored `request_json` is the body alone.

use std::cell::Cell;
use std::fmt;
use std::time::Duration;

use chrono::Utc;
use reqwest::header::HeaderValue;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::config::Settings;
use crate::eventlog::EventLog;
use crate::http::{CallCtx, Http, Service};

pub const ANTHROPIC_BASE: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2026-10-21";
const SONNET_FALLBACK_BETA: &str = "server-side-fallback-2026-07-01";
/// Sonnet with adaptive thinking can take a while; the shared client default is 60 s. A timed-out call is
/// not retried (it may have been billed), so the limit is generous (ruling R24).
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const HTTP_ATTEMPTS: u32 = 3;
/// Haiku 5.5 prompts above this many input tokens are billed at the higher tier.
const HAIKU_LONG_PROMPT_TOKENS: u64 = 100_000;
/// Rough characters per token used to size the pre-call reservation.
const CHARS_PER_TOKEN: f64 = 3.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Model {
    Haiku,
    Sonnet,
}

impl Model {
    pub fn id(self) -> &'static str {
        match self {
            Model::Haiku => "claude-haiku-5-5",
            Model::Sonnet => "claude-sonnet-5-5",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Model::Haiku => "llm-haiku",
            Model::Sonnet => "llm-sonnet",
        }
    }
}

/// USD per million tokens.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prices {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
}

/// Rates for `model` given the prompt size (`input_tokens`). Cache writes are billed at the input rate.
pub fn prices(model: Model, input_tokens: u64) -> Prices {
    match model {
        Model::Haiku if input_tokens > HAIKU_LONG_PROMPT_TOKENS => Prices {
            input: 0.50,
            output: 2.50,
            // Not in the pricing brief: assumed 10% of the input rate, as for Sonnet's published ratio.
            cache_read: 0.05,
        },
        Model::Haiku => Prices {
            input: 0.10,
            output: 0.50,
            cache_read: 0.01,
        },
        Model::Sonnet => Prices {
            input: 2.0,
            output: 10.0,
            cache_read: 0.10,
        },
    }
}

/// Which price list a model id from a response belongs to: `(model, known)`. A fallback can serve a call from a
/// different model; anything not recognised is priced at Sonnet rates (`known == false`, the caller logs it).
fn model_for_pricing(model_id: &str) -> (Model, bool) {
    if model_id.starts_with(Model::Haiku.id()) {
        (Model::Haiku, true)
    } else if model_id.starts_with(Model::Sonnet.id()) {
        (Model::Sonnet, true)
    } else {
        (Model::Sonnet, false)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

/// Cost in USD of one response. The Haiku long-prompt tier is chosen by `input_tokens`.
pub fn cost_usd(model: Model, usage: &Usage) -> f64 {
    let p = prices(model, usage.input_tokens);
    (usage.input_tokens + usage.cache_creation_input_tokens) as f64 * p.input / 1e6
        + usage.cache_read_input_tokens as f64 * p.cache_read / 1e6
        + usage.output_tokens as f64 * p.output / 1e6
}

#[derive(Debug)]
pub enum LlmError {
    /// `ANTHROPIC_API_KEY` is not set.
    MissingKey,
    /// The governor refused the call before any request was sent. `scope` is `"day"` or `"run"`.
    CapHit { scope: &'static str },
    /// The model refused (`stop_reason: refusal`).
    Refusal { category: String },
    /// `stop_reason: max_tokens`: the JSON is cut off.
    Truncated,
    /// The reply is not the JSON asked for (after the one automatic retry).
    BadJson(String),
    /// Non-200 answer that is not retryable (4xx other than 429) or an unreadable 200.
    Api { status: u16, message: String },
    /// Every attempt failed with 429, 5xx or a network error.
    Http(String),
    /// Reading or writing `llm_calls` failed.
    Db(String),
}

impl fmt::Display for LlmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LlmError::MissingKey => write!(f, "ANTHROPIC_API_KEY is not set"),
            LlmError::CapHit { scope } => write!(f, "spend cap hit ({scope})"),
            LlmError::Refusal { category } => write!(f, "model refused ({category})"),
            LlmError::Truncated => write!(f, "reply truncated at max_tokens"),
            LlmError::BadJson(m) => write!(f, "reply is not valid JSON: {m}"),
            LlmError::Api { status, message } => {
                write!(f, "anthropic api error {status}: {message}")
            }
            LlmError::Http(m) => write!(f, "anthropic request failed: {m}"),
            LlmError::Db(m) => write!(f, "llm_calls database error: {m}"),
        }
    }
}

impl std::error::Error for LlmError {}

impl From<rusqlite::Error> for LlmError {
    fn from(e: rusqlite::Error) -> LlmError {
        LlmError::Db(e.to_string())
    }
}

/// What a successful call cost and which `llm_calls` row holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct CallMeta {
    pub call_id: i64,
    pub model_used: String,
    pub usage: Usage,
    pub cost_usd: f64,
    pub latency_ms: u64,
}

/// Start of the current UTC day as unix seconds.
fn utc_day_start(now: i64) -> i64 {
    now - now.rem_euclid(86_400)
}

fn sum_cost_since(conn: &Connection, since: i64) -> rusqlite::Result<f64> {
    conn.query_row(
        "SELECT COALESCE(SUM(cost_usd), 0) FROM llm_calls WHERE ts >= ?1",
        [since],
        |r| r.get(0),
    )
}

fn try_spend_today(conn: &Connection) -> rusqlite::Result<f64> {
    sum_cost_since(conn, utc_day_start(Utc::now().timestamp()))
}

fn try_spend_run(conn: &Connection) -> rusqlite::Result<f64> {
    let start: Option<i64> = conn
        .query_row("SELECT value FROM kv WHERE key = 'run_start_ts'", [], |r| {
            r.get::<_, Option<String>>(0)
        })
        .optional()?
        .flatten()
        .and_then(|v| v.trim().parse().ok());
    sum_cost_since(conn, start.unwrap_or(0))
}

/// Spend so far on the current UTC calendar day. A database error reads as infinite spend (fail closed).
pub fn spend_today(conn: &Connection) -> f64 {
    try_spend_today(conn).unwrap_or(f64::INFINITY)
}

/// Spend since kv `run_start_ts` (all-time while the run has not started). Fails closed like [`spend_today`].
pub fn spend_run(conn: &Connection) -> f64 {
    try_spend_run(conn).unwrap_or(f64::INFINITY)
}

/// Request body for one JSON call (no headers, so it is safe to store).
fn build_body(model: Model, system: &str, user: &str, schema: &Value, max_tokens: u32) -> Value {
    let mut output_config = json!({"format": {"type": "json_schema", "schema": schema}});
    let mut body = json!({
        "model": model.id(),
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    });
    match model {
        Model::Haiku => {
            body["thinking"] = json!({"type": "disabled"});
        }
        Model::Sonnet => {
            body["thinking"] = json!({"type": "adaptive"});
            output_config["effort"] = json!("medium");
            body["fallbacks"] = json!("default");
        }
    }
    body["output_config"] = output_config;
    body
}

/// Holds an estimate against the caps while a call is in flight; released on drop.
struct Reservation<'a> {
    held: &'a Cell<f64>,
    amount: f64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.held.set((self.held.get() - self.amount).max(0.0));
    }
}

/// Everything `llm_calls` stores about one call.
struct CallRecord<'a> {
    candidate_id: Option<i64>,
    role: &'a str,
    model_requested: &'a str,
    model_used: Option<&'a str>,
    request_json: &'a str,
    response_json: Option<&'a str>,
    usage: Option<Usage>,
    cost_usd: f64,
    latency_ms: u64,
    status: &'a str,
    error: Option<&'a str>,
}

pub struct Llm<'a> {
    http: &'a Http,
    db: &'a Connection,
    log: &'a EventLog,
    settings: &'a Settings,
    base_url: String,
    /// Estimates of calls currently in flight (counted against the caps).
    held: Cell<f64>,
}

/// The reply of a 200 response, reduced to what the caller needs.
enum Reply {
    Json(Value),
    Refusal(String),
    Truncated,
    BadJson(String),
}

impl<'a> Llm<'a> {
    /// `base_url` is `https://api.anthropic.com` in production ([`ANTHROPIC_BASE`]) or a mock server in tests.
    pub fn new(
        http: &'a Http,
        db: &'a Connection,
        log: &'a EventLog,
        settings: &'a Settings,
        base_url: &str,
    ) -> Llm<'a> {
        Llm {
            http,
            db,
            log,
            settings,
            base_url: base_url.trim_end_matches('/').to_string(),
            held: Cell::new(0.0),
        }
    }

    /// One JSON-schema call. A reply that is not valid JSON is retried once (a fresh call, governed and
    /// recorded like the first); refusals and truncation are not retried.
    #[allow(clippy::too_many_arguments)]
    pub async fn json_call(
        &self,
        model: Model,
        role: &'static str,
        candidate_id: Option<i64>,
        system: &str,
        user: &str,
        schema: Value,
        max_tokens: u32,
    ) -> Result<(Value, CallMeta), LlmError> {
        let first = self
            .call_once(model, role, candidate_id, system, user, &schema, max_tokens)
            .await;
        match first {
            Err(LlmError::BadJson(why)) => {
                self.log.log(
                    "llm_retry",
                    candidate_id,
                    json!({"role": role, "model": model.id(), "reason": why}),
                );
                self.call_once(model, role, candidate_id, system, user, &schema, max_tokens)
                    .await
            }
            other => other,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn call_once(
        &self,
        model: Model,
        role: &'static str,
        candidate_id: Option<i64>,
        system: &str,
        user: &str,
        schema: &Value,
        max_tokens: u32,
    ) -> Result<(Value, CallMeta), LlmError> {
        let key = self
            .settings
            .anthropic_key
            .as_deref()
            .ok_or(LlmError::MissingKey)?;
        let mut key_header = HeaderValue::from_str(key)
            .map_err(|_| LlmError::Http("ANTHROPIC_API_KEY is not a valid header value".into()))?;
        key_header.set_sensitive(true);

        let body = build_body(model, system, user, schema, max_tokens);
        let request_json = body.to_string();
        let reservation = self.reserve(model, role, candidate_id, system, user, max_tokens)?;

        let mut req = self
            .http
            .client()
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", key_header)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .timeout(REQUEST_TIMEOUT)
            .json(&body);
        if model == Model::Sonnet {
            req = req.header("anthropic-beta", SONNET_FALLBACK_BETA);
        }
        let ctx = CallCtx {
            label: model.label(),
            candidate_id,
            max_attempts: HTTP_ATTEMPTS,
            store_raw: true,
        };

        let started = Instant::now();
        let sent = self.http.send(Service::Anthropic, req, ctx).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        let mut rec = CallRecord {
            candidate_id,
            role,
            model_requested: model.id(),
            model_used: None,
            request_json: &request_json,
            response_json: None,
            usage: None,
            cost_usd: 0.0,
            latency_ms,
            status: "",
            error: None,
        };

        let res = match sent {
            Ok(r) => r,
            Err(e) => {
                // The request may have run (a timeout or a dropped connection after the server took it):
                // count the reservation estimate as spent (fail closed, ruling R24).
                let msg = format!("{e}; cost recorded is the pre-call estimate");
                rec.status = "http_error";
                rec.error = Some(&msg);
                rec.cost_usd = reservation.amount;
                self.insert(&rec)?;
                self.log.log(
                    "llm_call",
                    candidate_id,
                    json!({"role": role, "model_requested": model.id(), "status": "http_error",
                           "cost_usd": reservation.amount, "estimated": true, "latency_ms": latency_ms}),
                );
                return Err(LlmError::Http(msg));
            }
        };
        let text = String::from_utf8_lossy(&res.body).into_owned();
        rec.response_json = Some(&text);

        if res.status != 200 {
            let msg = api_error_message(&text);
            rec.status = "api_error";
            rec.error = Some(&msg);
            self.insert(&rec)?;
            return Err(LlmError::Api {
                status: res.status,
                message: msg,
            });
        }
        let parsed: ApiResponse = match serde_json::from_str(&text) {
            Ok(p) => p,
            Err(e) => {
                let msg = format!("unreadable 200 response: {e}");
                rec.status = "api_error";
                rec.error = Some(&msg);
                self.insert(&rec)?;
                return Err(LlmError::Api {
                    status: 200,
                    message: msg,
                });
            }
        };

        // From here the call has been billed: price it from the model that actually served it.
        let model_used = parsed.model.clone().unwrap_or_else(|| model.id().into());
        let (price_model, known) = model_for_pricing(&model_used);
        if !known {
            self.log.log(
                "llm_unknown_model",
                candidate_id,
                json!({"role": role, "requested": model.id(), "served": model_used,
                       "priced_as": price_model.id()}),
            );
        }
        let cost = cost_usd(price_model, &parsed.usage);
        rec.model_used = Some(&model_used);
        rec.usage = Some(parsed.usage);
        rec.cost_usd = cost;

        let reply = interpret(&parsed);
        let (status, error): (&str, Option<String>) = match &reply {
            Reply::Json(_) => ("ok", None),
            Reply::Refusal(cat) => ("refusal", Some(cat.clone())),
            Reply::Truncated => ("truncated", Some("stop_reason max_tokens".into())),
            Reply::BadJson(why) => ("bad_json", Some(why.clone())),
        };
        rec.status = status;
        rec.error = error.as_deref();
        let call_id = self.insert(&rec)?;
        self.log.log(
            "llm_call",
            candidate_id,
            json!({"call_id": call_id, "role": role, "model_requested": model.id(),
                   "model_used": model_used, "status": status, "cost_usd": cost,
                   "input_tokens": parsed.usage.input_tokens,
                   "output_tokens": parsed.usage.output_tokens, "latency_ms": latency_ms}),
        );

        match reply {
            Reply::Json(v) => Ok((
                v,
                CallMeta {
                    call_id,
                    model_used,
                    usage: parsed.usage,
                    cost_usd: cost,
                    latency_ms,
                },
            )),
            Reply::Refusal(category) => Err(LlmError::Refusal { category }),
            Reply::Truncated => Err(LlmError::Truncated),
            Reply::BadJson(why) => Err(LlmError::BadJson(why)),
        }
    }

    /// Governor: estimate the call (input chars / 3.5 tokens plus `max_tokens` output, at the model's prices),
    /// refuse it when day or run spend plus in-flight estimates plus this one would pass a cap, else hold it.
    fn reserve(
        &self,
        model: Model,
        role: &str,
        candidate_id: Option<i64>,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<Reservation<'_>, LlmError> {
        let chars = system.chars().count() + user.chars().count();
        let in_tokens = (chars as f64 / CHARS_PER_TOKEN).ceil();
        let p = prices(model, in_tokens as u64);
        let estimate = (in_tokens * p.input + max_tokens as f64 * p.output) / 1e6;
        let in_flight = self.held.get();

        let checks = [
            (
                "day",
                try_spend_today(self.db)?,
                self.settings.daily_usd_cap,
            ),
            ("run", try_spend_run(self.db)?, self.settings.run_usd_cap),
        ];
        for (scope, spent, cap) in checks {
            if spent + in_flight + estimate > cap {
                self.log.log(
                    "llm_cap_hit",
                    candidate_id,
                    json!({"scope": scope, "role": role, "model": model.id(), "spent": spent,
                           "in_flight": in_flight, "estimate": estimate, "cap": cap}),
                );
                return Err(LlmError::CapHit { scope });
            }
        }
        self.held.set(in_flight + estimate);
        Ok(Reservation {
            held: &self.held,
            amount: estimate,
        })
    }

    fn insert(&self, r: &CallRecord<'_>) -> Result<i64, LlmError> {
        let u = r.usage.as_ref();
        self.db.execute(
            "INSERT INTO llm_calls (candidate_id, ts, role, model_requested, model_used, request_json,
                response_json, input_tokens, output_tokens, cost_usd, latency_ms, status, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                r.candidate_id,
                Utc::now().timestamp(),
                r.role,
                r.model_requested,
                r.model_used,
                r.request_json,
                r.response_json,
                u.map(|u| u.input_tokens as i64),
                u.map(|u| u.output_tokens as i64),
                r.cost_usd,
                r.latency_ms as i64,
                r.status,
                r.error,
            ],
        )?;
        Ok(self.db.last_insert_rowid())
    }
}

#[derive(Deserialize)]
struct ApiResponse {
    #[serde(default)]
    content: Vec<Value>,
    stop_reason: Option<String>,
    stop_details: Option<Value>,
    #[serde(default)]
    usage: Usage,
    model: Option<String>,
}

/// Turn a 200 response into the JSON asked for, or the reason it is not usable.
fn interpret(r: &ApiResponse) -> Reply {
    match r.stop_reason.as_deref() {
        Some("refusal") => {
            let category = r
                .stop_details
                .as_ref()
                .and_then(|d| d.get("category"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            return Reply::Refusal(category.to_string());
        }
        Some("max_tokens") => return Reply::Truncated,
        _ => {}
    }
    // Thinking blocks can precede the answer: take the first text block.
    let text = r
        .content
        .iter()
        .find(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|b| b.get("text"))
        .and_then(Value::as_str);
    match text {
        None => Reply::BadJson("no text block in reply".into()),
        Some(t) => match serde_json::from_str::<Value>(t) {
            Ok(v) => Reply::Json(v),
            Err(e) => Reply::BadJson(e.to_string()),
        },
    }
}

/// `error.message` from an Anthropic error body, else a short prefix of the body.
fn api_error_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v["error"]["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| body.chars().take(300).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(i: u64, o: u64) -> Usage {
        Usage {
            input_tokens: i,
            output_tokens: o,
            ..Default::default()
        }
    }

    #[test]
    fn haiku_cost_and_long_prompt_tier() {
        assert!((cost_usd(Model::Haiku, &usage(1_000, 200)) - 0.0002).abs() < 1e-12);
        // 100K is still the cheap tier, 100K + 1 is not.
        let at = cost_usd(Model::Haiku, &usage(100_000, 0));
        let over = cost_usd(Model::Haiku, &usage(100_001, 0));
        assert!((at - 0.01).abs() < 1e-12, "{at}");
        assert!((over - 100_001.0 * 0.50 / 1e6).abs() < 1e-12, "{over}");
    }

    #[test]
    fn sonnet_cost_with_cache() {
        let u = Usage {
            input_tokens: 1_000_000,
            output_tokens: 1_000_000,
            cache_creation_input_tokens: 1_000_000,
            cache_read_input_tokens: 1_000_000,
        };
        // 2 + 2 (cache write at input rate) + 0.10 + 10
        assert!((cost_usd(Model::Sonnet, &u) - 14.10).abs() < 1e-9);
    }

    #[test]
    fn model_ids_map_to_price_lists() {
        assert_eq!(model_for_pricing("claude-haiku-5-5"), (Model::Haiku, true));
        assert_eq!(
            model_for_pricing("claude-sonnet-5-5-20261101"),
            (Model::Sonnet, true)
        );
        assert_eq!(model_for_pricing("claude-opus-9"), (Model::Sonnet, false));
    }

    #[test]
    fn request_timeout_is_ten_minutes() {
        assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(600));
    }

    #[test]
    fn day_start_is_utc_midnight() {
        let t = 1_795_000_000 - 1_795_000_000 % 86_400;
        assert_eq!(utc_day_start(t + 86_399), t);
        assert_eq!(utc_day_start(t), t);
    }
}
