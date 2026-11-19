use std::io::Read;

use informed_pit::db;
use informed_pit::eventlog::EventLog;

const TABLES: [&str; 20] = [
    "universe_runs",
    "markets",
    "snapshots",
    "trades",
    "scores",
    "candidates",
    "candidate_markets",
    "phrases",
    "searches",
    "articles",
    "wayback_checks",
    "llm_calls",
    "haiku_verdicts",
    "quote_checks",
    "sonnet_reviews",
    "paper_trades",
    "settlements",
    "errors",
    "events",
    "kv",
];

fn table_names(conn: &rusqlite::Connection) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn expected_tables() -> Vec<String> {
    let mut v: Vec<String> = TABLES.iter().map(|s| s.to_string()).collect();
    v.sort();
    v
}

#[test]
fn migrations_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pit.db");
    {
        let conn = db::open(&path).unwrap();
        assert_eq!(table_names(&conn), expected_tables());
        conn.execute("INSERT INTO kv(key, value) VALUES ('k', 'v')", [])
            .unwrap();
    }
    let conn = db::open(&path).unwrap();
    assert_eq!(table_names(&conn), expected_tables());
    let v: String = conn
        .query_row("SELECT value FROM kv WHERE key='k'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "v", "reopening must not wipe data");
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
    let fk: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert_eq!(fk, 1);
    let busy: i64 = conn
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
        .unwrap();
    assert_eq!(busy, 5000);
}

#[test]
fn prune_files_keeps_referenced_raw_and_recent_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let conn = db::open(&d.join("pit.db")).unwrap();
    let log = EventLog::new(d);
    // "Now" is 2026-03-01 12:00 UTC.
    let now = chrono::NaiveDate::from_ymd_opt(2026, 3, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();
    let touch = |rel: &str| {
        let p = d.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, b"x").unwrap();
    };
    touch("raw/2026-02-01/old-unreferenced.gz");
    touch("raw/2026-02-01/old-article.gz");
    touch("raw/2026-02-10/old-only.gz");
    touch("raw/2026-02-23/recent.gz"); // 6 days old: kept
    touch("logs/2026-01-01.jsonl"); // 59 days: deleted
    touch("logs/2026-02-01.jsonl"); // 28 days: kept
    touch("logs/notes.txt");
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
         VALUES (1, 'EV', 'yes', 0, 90, 'queued', 0, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles (candidate_id, url, raw_path) VALUES (1, 'u', 'raw/2026-02-01/old-article.gz')",
        [],
    )
    .unwrap();

    let p = log.prune_files(&conn, now).unwrap();
    assert_eq!((p.raw_files, p.log_files), (2, 1));
    assert!(!d.join("raw/2026-02-01/old-unreferenced.gz").exists());
    assert!(d.join("raw/2026-02-01/old-article.gz").exists());
    assert!(!d.join("raw/2026-02-10").exists(), "empty day dir removed");
    assert!(d.join("raw/2026-02-23/recent.gz").exists());
    assert!(!d.join("logs/2026-01-01.jsonl").exists());
    assert!(d.join("logs/2026-02-01.jsonl").exists());
    assert!(d.join("logs/notes.txt").exists());
}

#[test]
fn scores_ts_index_and_articles_via_exist() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let idx: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'scores_ts' AND tbl_name = 'scores'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(idx, 1);
    let plan: String = conn
        .query_row(
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM scores WHERE ts >= 0",
            [],
            |r| r.get(3),
        )
        .unwrap();
    assert!(plan.contains("scores_ts"), "{plan}");
    // `via` takes rss or newsapi only.
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
         VALUES (1, 'EV', 'yes', 0, 90, 'queued', 0, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles (candidate_id, url, via) VALUES (1, 'u1', 'newsapi')",
        [],
    )
    .unwrap();
    assert!(conn
        .execute(
            "INSERT INTO articles (candidate_id, url, via) VALUES (1, 'u2', 'bing')",
            [],
        )
        .is_err());
}

#[test]
fn with_tx_rolls_back_on_error() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let res: anyhow::Result<()> = db::with_tx(&conn, |tx| {
        tx.execute("INSERT INTO kv(key, value) VALUES ('a', '1')", [])?;
        anyhow::bail!("boom")
    });
    assert!(res.is_err());
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);

    // and it commits on success, returning the closure's value
    let got = db::with_tx(&conn, |tx| {
        tx.execute("INSERT INTO kv(key, value) VALUES ('a', '1')", [])?;
        Ok(42)
    })
    .unwrap();
    assert_eq!(got, 42);
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}

#[test]
fn candidate_status_check_rejects_unknown_values() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let ins = |status: &str| {
        conn.execute(
            "INSERT INTO candidates(event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts) \
             VALUES ('EV', 'up', 0, 90.0, ?1, 0, 0)",
            [status],
        )
    };
    for ok in [
        "queued",
        "investigating",
        "explained",
        "unexplained",
        "bought",
        "no_buy",
        "stale",
        "failed",
        "cap_skipped",
    ] {
        conn.execute("DELETE FROM candidates", []).unwrap();
        ins(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    assert!(ins("bogus").is_err());
}

#[test]
fn eventlog_appends_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let log = EventLog::new(dir.path());
    log.log("one", None, serde_json::json!({"a": 1}));
    log.log("two", Some(7), serde_json::json!({"b": "x"}));
    let logs = dir.path().join("logs");
    let files: Vec<_> = std::fs::read_dir(&logs)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    assert!(files[0].extension().unwrap() == "jsonl");
    let text = std::fs::read_to_string(&files[0]).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2);
    let vals: Vec<serde_json::Value> = lines
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    for v in &vals {
        assert!(v["kind"].is_string());
        assert!(v["ts"].as_i64().unwrap() > 1_700_000_000);
    }
    assert_eq!(vals[0]["kind"], "one");
    assert!(vals[0]["candidate_id"].is_null());
    assert_eq!(vals[1]["candidate_id"], 7);
    assert_eq!(vals[1]["payload"]["b"], "x");
}

#[test]
fn store_raw_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let log = EventLog::new(dir.path());
    let body = b"<rss>hello \xff\x00 world</rss>".repeat(50);
    let rel = log.store_raw("rss/google news?", &body);
    assert!(rel.starts_with("raw/") && rel.ends_with(".gz"), "{rel}");
    assert!(
        !rel.contains('\\') && !rel.contains('?') && rel.matches('/').count() == 2,
        "{rel}"
    );
    let f = std::fs::File::open(dir.path().join(&rel)).unwrap();
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(f)
        .read_to_end(&mut out)
        .unwrap();
    assert_eq!(out, body);
    // two stores never collide
    assert_ne!(rel, log.store_raw("rss/google news?", &body));
}
