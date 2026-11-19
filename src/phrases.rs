//! Haiku news-search phrase generation (spec: investigation step 1).
//!
//! For one candidate move Haiku writes [`WANTED`] short phrases that a headline or article about news that
//! moved the market would contain; the news search (Task 10) uses them as queries. Replies are validated
//! ([`validate`]); when fewer than [`WANTED`] pass, one retry asks for the missing count. At least
//! [`MIN_VALID`] valid phrases are required, otherwise [`generate`] returns [`PhraseError::TooFew`] and the
//! investigator marks the candidate `failed` with reason `phrases`. This module never touches the
//! `candidates` row. Every phrase, accepted or rejected, is stored in `phrases` (`attempt` 1 or 2).

use std::collections::HashSet;
use std::fmt;

use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::kalshi::Side;
use crate::llm::{Llm, LlmError, Model};

/// Phrases asked for per candidate.
pub const WANTED: usize = 50;
/// Fewer valid phrases than this fails the candidate.
pub const MIN_VALID: usize = 10;
const MIN_WORDS: usize = 2;
const MAX_WORDS: usize = 4;
const MAX_CHARS: usize = 60;
const MAX_TOKENS: u32 = 1500;
const ROLE: &str = "phrases";
/// Market rules text shown to Haiku is cut at this many characters.
const MAX_RULES_CHARS: usize = 1_000;

const SYSTEM_PROMPT: &str = "\
You help a news search find out why a prediction market moved. You are given one market and a sudden move in \
its price. Write search phrases: short strings that a journalist's headline or article about NEWS THAT WOULD \
MOVE THIS MARKET would contain.

Rules:
- Each phrase is 2 to 4 words, English, plain text with no quotation marks.
- Mix entity names (people, organisations, places, products), event terms (what could have happened) and \
synonyms or alternative wordings of the same story.
- Most phrases should describe a development that would make the outcome the user names (YES or NO) more \
likely: that is the news that would have moved the price.
- Do not repeat a phrase, even with different capitalisation. Do not number the phrases.
- Return exactly the number of phrases the user asks for, as JSON: {\"phrases\": [\"...\", ...]}.";

/// What the investigator knows about the market; shared with the later pipeline stages.
#[derive(Debug, Clone)]
pub struct MarketContext {
    pub title: String,
    pub subtitle: String,
    pub rules: String,
    pub direction: Side,
    /// YES price in cents when the move started.
    pub from_c: u32,
    /// YES price in cents now.
    pub to_c: u32,
    pub window_min: u32,
}

#[derive(Debug)]
pub enum PhraseError {
    /// The LLM call failed (cap hit, refusal, bad reply, API or network error). Not retried here.
    Llm(LlmError),
    /// Fewer than `required` valid phrases after the retry: the candidate fails with reason `phrases`.
    TooFew {
        valid: usize,
        required: usize,
    },
    Db(String),
}

impl fmt::Display for PhraseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhraseError::Llm(e) => write!(f, "phrase generation: {e}"),
            PhraseError::TooFew { valid, required } => {
                write!(f, "only {valid} valid phrases, {required} required")
            }
            PhraseError::Db(m) => write!(f, "phrases database error: {m}"),
        }
    }
}

impl std::error::Error for PhraseError {}

impl From<LlmError> for PhraseError {
    fn from(e: LlmError) -> PhraseError {
        PhraseError::Llm(e)
    }
}

impl From<rusqlite::Error> for PhraseError {
    fn from(e: rusqlite::Error) -> PhraseError {
        PhraseError::Db(e.to_string())
    }
}

fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {"phrases": {"type": "array", "items": {"type": "string"}}},
        "required": ["phrases"],
        "additionalProperties": false
    })
}

fn is_quote(c: char) -> bool {
    matches!(c, '"' | '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}')
}

fn is_single_quote(c: char) -> bool {
    matches!(c, '\'' | '\u{2018}' | '\u{2019}')
}

/// Why `phrase` (already trimmed) is unusable, if it is. An apostrophe inside a word ("powell's") is fine; a
/// double quote anywhere, or a single quote opening or closing the phrase, is not.
fn reject_reason(phrase: &str) -> Option<&'static str> {
    let words = phrase.split_whitespace().count();
    if !(MIN_WORDS..=MAX_WORDS).contains(&words) {
        return Some("word_count");
    }
    if phrase.chars().count() > MAX_CHARS {
        return Some("too_long");
    }
    if phrase.chars().any(is_quote)
        || phrase.chars().next().is_some_and(is_single_quote)
        || phrase.chars().last().is_some_and(is_single_quote)
    {
        return Some("quote");
    }
    None
}

/// Like [`validate`], with `seen` (lowercased phrases accepted earlier) carried across batches.
fn validate_with(
    raw: Vec<String>,
    seen: &mut HashSet<String>,
) -> (Vec<String>, Vec<(String, &'static str)>) {
    let mut valid = Vec::new();
    let mut rejected = Vec::new();
    for p in raw {
        let p = p.trim().to_string();
        match reject_reason(&p) {
            Some(why) => rejected.push((p, why)),
            None if !seen.insert(p.to_lowercase()) => rejected.push((p, "duplicate")),
            None => valid.push(p),
        }
    }
    (valid, rejected)
}

/// Split model output into usable phrases and rejects with the reason. A phrase is trimmed, 2 to 4 words, at
/// most 60 characters, free of quotation marks, and not a case-insensitive repeat of an earlier one. Reasons:
/// `word_count`, `too_long`, `quote`, `duplicate`. Order is preserved.
pub fn validate(raw: Vec<String>) -> (Vec<String>, Vec<(String, &'static str)>) {
    validate_with(raw, &mut HashSet::new())
}

/// The user message. `now` (UTC unix seconds) gives the date Haiku is told.
fn user_prompt(ctx: &MarketContext, want: usize, already: &[String], now: i64) -> String {
    let (side, outcome) = match ctx.direction {
        Side::Yes => ("YES (buyers pushed it up)", "YES"),
        Side::No => ("NO (sellers pushed it down)", "NO"),
    };
    let today = chrono::DateTime::from_timestamp(now, 0)
        .unwrap_or_default()
        .format("%Y-%m-%d");
    let rules: String = ctx.rules.chars().take(MAX_RULES_CHARS).collect();
    let mut s = format!(
        "Today is {today} (UTC).\n\nMarket title: {}\nSubtitle: {}\nRules: {rules}\n\nThe YES price moved \
         from {}\u{a2} to {}\u{a2} over {} minutes, in the direction of {side}. The move was sudden: the news \
         that explains it, if there is any, was published within roughly the last hour, before or during the \
         move.\n\nWrite exactly {want} search phrases. Most phrases should describe a development that would \
         make the {outcome} outcome more likely.",
        ctx.title, ctx.subtitle, ctx.from_c, ctx.to_c, ctx.window_min
    );
    if !already.is_empty() {
        s.push_str(&format!(
            " You already wrote these, so do not repeat them or reword them closely:\n{}",
            already.join("\n")
        ));
    }
    s
}

/// One call; returns the raw strings and the `llm_calls` id.
async fn ask(llm: &Llm<'_>, candidate_id: i64, user: &str) -> Result<(Vec<String>, i64), LlmError> {
    let (value, meta) = llm
        .json_call(
            Model::Haiku,
            ROLE,
            Some(candidate_id),
            SYSTEM_PROMPT,
            user,
            schema(),
            MAX_TOKENS,
        )
        .await?;
    let raw = value
        .get("phrases")
        .and_then(Value::as_array)
        .ok_or_else(|| LlmError::BadJson("reply has no `phrases` array".into()))?
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    Ok((raw, meta.call_id))
}

struct Attempt {
    candidate_id: i64,
    attempt: i64,
    llm_call_id: i64,
}

fn store(
    conn: &Connection,
    at: &Attempt,
    accepted: &[String],
    rejected: &[(String, &'static str)],
) -> Result<(), PhraseError> {
    let ts = Utc::now().timestamp();
    crate::db::with_tx(conn, |tx| {
        let mut ins = tx.prepare(
            "INSERT INTO phrases (candidate_id, ts, phrase, llm_call_id, accepted, reject_reason, attempt)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for p in accepted {
            ins.execute(params![
                at.candidate_id,
                ts,
                p,
                at.llm_call_id,
                1,
                None::<&str>,
                at.attempt
            ])?;
        }
        for (p, why) in rejected {
            ins.execute(params![
                at.candidate_id,
                ts,
                p,
                at.llm_call_id,
                0,
                why,
                at.attempt
            ])?;
        }
        Ok(())
    })
    .map_err(|e| PhraseError::Db(e.to_string()))
}

/// Generate and store the search phrases for `candidate_id`: up to [`WANTED`] valid, deduplicated phrases.
///
/// If the first reply leaves fewer than [`WANTED`] valid phrases, one more call asks for the missing count
/// (listing what is already accepted). A failure of that second call is not fatal when at least
/// [`MIN_VALID`] phrases are already valid; otherwise its error is returned. Valid phrases beyond
/// [`WANTED`] are stored as rejected (`excess`). Errors from the first call, including
/// [`LlmError::CapHit`], come back as [`PhraseError::Llm`].
pub async fn generate(
    llm: &Llm<'_>,
    conn: &Connection,
    candidate_id: i64,
    ctx: &MarketContext,
) -> Result<Vec<String>, PhraseError> {
    let mut seen = HashSet::new();
    let mut accepted: Vec<String> = Vec::new();

    for attempt in 1..=2i64 {
        let want = WANTED - accepted.len();
        let user = user_prompt(ctx, want, &accepted, Utc::now().timestamp());
        let (raw, llm_call_id) = match ask(llm, candidate_id, &user).await {
            Ok(r) => r,
            // The failed call is already recorded in `llm_calls`.
            Err(_) if attempt == 2 && accepted.len() >= MIN_VALID => break,
            Err(e) => return Err(e.into()),
        };
        let (mut valid, mut rejected) = validate_with(raw, &mut seen);
        for extra in valid.split_off(want.min(valid.len())) {
            rejected.push((extra, "excess"));
        }
        store(
            conn,
            &Attempt {
                candidate_id,
                attempt,
                llm_call_id,
            },
            &valid,
            &rejected,
        )?;
        accepted.extend(valid);
        if accepted.len() >= WANTED {
            break;
        }
    }

    if accepted.len() < MIN_VALID {
        return Err(PhraseError::TooFew {
            valid: accepted.len(),
            required: MIN_VALID,
        });
    }
    Ok(accepted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_prompt_states_the_move_and_count() {
        let ctx = MarketContext {
            title: "T".into(),
            subtitle: "S".into(),
            rules: "R".into(),
            direction: Side::No,
            from_c: 70,
            to_c: 41,
            window_min: 15,
        };
        // 2026-11-18 14:00 UTC.
        let now = 1_795_010_400;
        let p = user_prompt(&ctx, 50, &[], now);
        assert!(
            p.contains("from 70\u{a2} to 41\u{a2} over 15 minutes"),
            "{p}"
        );
        assert!(p.contains("exactly 50"));
        assert!(!p.contains("already wrote"));
        assert!(p.starts_with("Today is 2026-11-18 (UTC)."), "{p}");
        assert!(
            p.contains(
                "The move was sudden: the news that explains it, if there is any, was published within \
                 roughly the last hour, before or during the move."
            ),
            "{p}"
        );
        assert!(
            p.contains(
                "Most phrases should describe a development that would make the NO outcome more likely."
            ),
            "{p}"
        );
        assert!(user_prompt(&ctx, 3, &["a b".into()], now).contains("a b"));
        assert!(!SYSTEM_PROMPT.contains("Cover both"));
    }

    #[test]
    fn rules_text_is_cut_at_1000_chars() {
        let ctx = MarketContext {
            title: "T".into(),
            subtitle: "S".into(),
            rules: format!("{}\u{e9}TAIL", "r".repeat(999)),
            direction: Side::Yes,
            from_c: 40,
            to_c: 60,
            window_min: 10,
        };
        let p = user_prompt(&ctx, 50, &[], 0);
        let kept = format!("Rules: {}\u{e9}\n", "r".repeat(999));
        assert!(p.contains(&kept), "{p}");
        assert!(!p.contains("TAIL"));
        assert!(p.contains("make the YES outcome more likely"));
    }
}
