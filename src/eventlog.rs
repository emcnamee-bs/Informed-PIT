//! Append-only JSONL event log and gzip store for raw HTTP bodies, both under the data dir.
//!
//! [`EventLog::prune_files`] (run daily) deletes raw bodies after [`RAW_KEEP_DAYS`] unless a database row
//! points at them (articles, searches, Wayback checks, paper-trade order books: the evidence behind a decision
//! is kept for good), and log files after [`LOG_KEEP_DAYS`].

use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{write::GzEncoder, Compression};
use rusqlite::Connection;
use serde_json::{json, Value};

/// Raw bodies no database row points at are deleted once their day is older than this.
pub const RAW_KEEP_DAYS: i64 = 7;
/// Log files are deleted once their day is older than this (a whole 30-day run plus margin is kept for
/// debugging; everything a decision rests on is also in the database).
pub const LOG_KEEP_DAYS: i64 = 45;

/// Raw paths referenced by the database (kept by [`EventLog::prune_files`]).
const REFERENCED_RAW_SQL: &str = "
SELECT raw_path FROM articles WHERE raw_path IS NOT NULL
UNION SELECT raw_path FROM searches WHERE raw_path IS NOT NULL
UNION SELECT raw_path FROM wayback_checks WHERE raw_path IS NOT NULL
UNION SELECT book_raw_path FROM paper_trades WHERE book_raw_path IS NOT NULL";

/// What [`EventLog::prune_files`] deleted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FilesPruned {
    pub raw_files: usize,
    pub log_files: usize,
}

/// `YYYY-MM-DD` of the UTC day `days` before `now`.
fn day_name(now: i64, days: i64) -> String {
    DateTime::from_timestamp(now - days * 86_400, 0)
        .unwrap_or_default()
        .format("%Y-%m-%d")
        .to_string()
}

fn is_day(name: &str) -> bool {
    name.len() == 10 && NaiveDate::parse_from_str(name, "%Y-%m-%d").is_ok()
}

pub struct EventLog {
    data_dir: PathBuf,
}

impl EventLog {
    pub fn new(data_dir: impl Into<PathBuf>) -> EventLog {
        EventLog {
            data_dir: data_dir.into(),
        }
    }

    /// Append one JSON line `{ts, kind, candidate_id, payload}` to `logs/YYYY-MM-DD.jsonl`.
    /// Logging must never take the bot down, so failures go to stderr only.
    pub fn log(&self, kind: &str, candidate_id: Option<i64>, payload: Value) {
        if let Err(e) = self.try_log(kind, candidate_id, payload) {
            eprintln!("eventlog: cannot write {kind}: {e}");
        }
    }

    fn try_log(
        &self,
        kind: &str,
        candidate_id: Option<i64>,
        payload: Value,
    ) -> std::io::Result<()> {
        let now = Utc::now();
        let dir = self.data_dir.join("logs");
        fs::create_dir_all(&dir)?;
        let mut line = json!({
            "ts": now.timestamp(),
            "kind": kind,
            "candidate_id": candidate_id,
            "payload": payload,
        })
        .to_string();
        line.push('\n');
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{}.jsonl", now.format("%Y-%m-%d"))))?;
        // One write call per line keeps concurrent appends from interleaving.
        f.write_all(line.as_bytes())
    }

    /// Gzip `body` to `raw/YYYY-MM-DD/<uuid>-<label>.gz`; returns the path relative to the data dir
    /// (forward slashes). On failure the error is logged and the intended path is still returned.
    pub fn store_raw(&self, label: &str, body: &[u8]) -> String {
        let label: String = label
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .take(60)
            .collect();
        let rel = format!(
            "raw/{}/{}-{}.gz",
            Utc::now().format("%Y-%m-%d"),
            uuid::Uuid::new_v4(),
            label
        );
        if let Err(e) = self.write_gz(&rel, body) {
            eprintln!("eventlog: cannot store raw body {rel}: {e}");
        }
        rel
    }

    /// Delete `raw/<day>/` files older than [`RAW_KEEP_DAYS`] that no database row references (and the day
    /// directory once empty), and `logs/<day>.jsonl` older than [`LOG_KEEP_DAYS`]. `now` is UTC unix seconds.
    pub fn prune_files(&self, conn: &Connection, now: i64) -> anyhow::Result<FilesPruned> {
        let referenced: HashSet<String> = conn
            .prepare(REFERENCED_RAW_SQL)?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        let mut out = FilesPruned::default();

        let raw_cut = day_name(now, RAW_KEEP_DAYS);
        let raw = self.data_dir.join("raw");
        if raw.is_dir() {
            for day in fs::read_dir(&raw)? {
                let day = day?;
                let name = day.file_name().to_string_lossy().into_owned();
                if !is_day(&name) || name >= raw_cut || !day.path().is_dir() {
                    continue;
                }
                for f in fs::read_dir(day.path())? {
                    let f = f?;
                    let rel = format!("raw/{name}/{}", f.file_name().to_string_lossy());
                    if !referenced.contains(&rel) {
                        fs::remove_file(f.path())?;
                        out.raw_files += 1;
                    }
                }
                // Succeeds only when nothing referenced is left in it.
                let _ = fs::remove_dir(day.path());
            }
        }

        let log_cut = day_name(now, LOG_KEEP_DAYS);
        let logs = self.data_dir.join("logs");
        if logs.is_dir() {
            for f in fs::read_dir(&logs)? {
                let f = f?;
                let name = f.file_name().to_string_lossy().into_owned();
                let Some(day) = name.strip_suffix(".jsonl") else {
                    continue;
                };
                if is_day(day) && day < log_cut.as_str() {
                    fs::remove_file(f.path())?;
                    out.log_files += 1;
                }
            }
        }
        Ok(out)
    }

    fn write_gz(&self, rel: &str, body: &[u8]) -> std::io::Result<()> {
        let path = self.data_dir.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut enc = GzEncoder::new(fs::File::create(path)?, Compression::default());
        enc.write_all(body)?;
        enc.finish()?.flush()
    }
}
