//! `informed-pit report <candidate_id>`: the whole trail of one candidate, as JSON or readable text.
//!
//! Sections in pipeline order: candidate, member markets, phrases, searches, articles (with window decision),
//! Haiku verdicts, quote checks, Sonnet review, LLM calls (metering only, not the prompt bodies, which stay in
//! `llm_calls`), paper trade, settlement, events and errors.

use anyhow::{bail, Result};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use serde_json::{Map, Value};

/// Longest value printed in text mode (JSON mode prints everything).
const TEXT_MAX_CHARS: usize = 400;

const SECTIONS: &[(&str, &str)] = &[
    ("candidate", "SELECT * FROM candidates WHERE id = ?1"),
    (
        "members",
        "SELECT * FROM candidate_markets WHERE candidate_id = ?1 ORDER BY ticker",
    ),
    (
        "phrases",
        "SELECT id, ts, attempt, accepted, reject_reason, phrase, llm_call_id FROM phrases
         WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "searches",
        "SELECT * FROM searches WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "articles",
        "SELECT * FROM articles WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "wayback_checks",
        "SELECT w.* FROM wayback_checks w JOIN articles a ON a.id = w.article_id
         WHERE a.candidate_id = ?1 ORDER BY w.id",
    ),
    (
        "haiku_verdicts",
        "SELECT * FROM haiku_verdicts WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "quote_checks",
        "SELECT q.* FROM quote_checks q JOIN haiku_verdicts h ON h.id = q.haiku_verdict_id
         WHERE h.candidate_id = ?1 ORDER BY q.id",
    ),
    (
        "sonnet_reviews",
        "SELECT * FROM sonnet_reviews WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "llm_calls",
        "SELECT id, ts, role, model_requested, model_used, status, error, input_tokens, output_tokens,
                cost_usd, latency_ms FROM llm_calls WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "paper_trades",
        "SELECT * FROM paper_trades WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "settlements",
        "SELECT s.* FROM settlements s JOIN paper_trades p ON p.id = s.trade_id
         WHERE p.candidate_id = ?1 ORDER BY s.id",
    ),
    (
        "events",
        "SELECT * FROM events WHERE candidate_id = ?1 ORDER BY id",
    ),
    (
        "errors",
        "SELECT * FROM errors WHERE candidate_id = ?1 ORDER BY id",
    ),
];

fn rows(conn: &Connection, sql: &str, id: i64) -> rusqlite::Result<Vec<Value>> {
    let mut stmt = conn.prepare(sql)?;
    let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut out = Vec::new();
    let mut q = stmt.query([id])?;
    while let Some(r) = q.next()? {
        let mut obj = Map::new();
        for (i, name) in names.iter().enumerate() {
            let v = match r.get_ref(i)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(n) => Value::from(n),
                ValueRef::Real(f) => Value::from(f),
                ValueRef::Text(t) => Value::from(String::from_utf8_lossy(t).into_owned()),
                ValueRef::Blob(b) => Value::from(format!("<{} bytes>", b.len())),
            };
            obj.insert(name.clone(), v);
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// The trail as one JSON object (one key per section; `candidate` is an object, the rest arrays).
pub fn build(conn: &Connection, candidate_id: i64) -> Result<Value> {
    let mut out = Map::new();
    for (name, sql) in SECTIONS {
        let mut r = rows(conn, sql, candidate_id)?;
        if *name == "candidate" {
            if r.is_empty() {
                bail!("no candidate {candidate_id}");
            }
            out.insert((*name).to_string(), r.remove(0));
        } else {
            out.insert((*name).to_string(), Value::Array(r));
        }
    }
    Ok(Value::Object(out))
}

fn short(v: &Value) -> String {
    let s = match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let s = s.replace('\n', " ");
    if s.chars().count() > TEXT_MAX_CHARS {
        format!(
            "{}... ({} chars)",
            s.chars().take(TEXT_MAX_CHARS).collect::<String>(),
            s.chars().count()
        )
    } else {
        s
    }
}

fn row_text(row: &Value) -> String {
    row.as_object()
        .map(|o| {
            o.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| format!("{k}={}", short(v)))
                .collect::<Vec<_>>()
                .join("  ")
        })
        .unwrap_or_default()
}

/// The trail as readable text: one header per section, one line per row (`column=value`, nulls left out,
/// long values cut at 400 characters).
pub fn text(trail: &Value) -> String {
    let mut out = String::new();
    for (name, _) in SECTIONS {
        let v = &trail[*name];
        match v {
            Value::Array(rows) => {
                out.push_str(&format!("== {name} ({})\n", rows.len()));
                for r in rows {
                    out.push_str(&format!("  {}\n", row_text(r)));
                }
            }
            other => out.push_str(&format!("== {name}\n  {}\n", row_text(other))),
        }
    }
    out
}
