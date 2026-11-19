//! SQLite storage: connection setup, versioned idempotent migrations, and a rollback-on-error transaction helper.
//!
//! All timestamps are UTC unix seconds (INTEGER). JSON blobs are TEXT. Prices are cents as REAL (Kalshi quotes
//! can be sub-cent). The schema version lives in `PRAGMA user_version`: `MIGRATIONS[i]` brings the database
//! from version `i` to `i + 1`. Later tasks append a new entry; never edit an applied one.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, Transaction, TransactionBehavior};

/// Allowed values of `candidates.status`.
pub const CANDIDATE_STATUSES: [&str; 9] = [
    "queued",
    "investigating",
    "explained",
    "unexplained",
    "bought",
    "no_buy",
    "stale",
    "failed",
    "cap_skipped",
];

const MIGRATIONS: &[&str] = &[
    SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5, SCHEMA_V6, SCHEMA_V7, SCHEMA_V8,
    SCHEMA_V9, SCHEMA_V10, SCHEMA_V11, SCHEMA_V12, SCHEMA_V13,
];

/// v13: `scores_ts` serves time-range reads of `scores` across all tickers; `articles.via` records which search
/// found the article (`rss` or `newsapi`) so the NewsAPI.ai fallback's contribution can be measured (spec 8.3).
const SCHEMA_V13: &str = "
CREATE INDEX scores_ts ON scores(ts);
ALTER TABLE articles ADD COLUMN via TEXT CHECK (via IN ('rss','newsapi'));
";

/// v12: `sonnet_reviews.evidence_fp` fingerprints the evidence a review was made on (branch, judged articles
/// and their relevance); a retried investigation reuses a stored review only when it matches.
const SCHEMA_V12: &str = "
ALTER TABLE sonnet_reviews ADD COLUMN evidence_fp TEXT;
";

/// v11: investigation state machine. `candidates.attempts` counts failed investigation attempts and
/// `retry_after_ts` holds a re-queued candidate back until then. `sonnet_reviews` keeps every field of the
/// review (`reasoning` is Sonnet's reason, `detail_json` its raw reply), the Haiku `branch` (`explained` or
/// `unexplained`) and the final decision (`final_buy`, `final_reason`).
const SCHEMA_V11: &str = "
ALTER TABLE candidates ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE candidates ADD COLUMN retry_after_ts INTEGER;
ALTER TABLE sonnet_reviews ADD COLUMN branch TEXT CHECK (branch IN ('explained','unexplained'));
ALTER TABLE sonnet_reviews ADD COLUMN haiku_errors_json TEXT;
ALTER TABLE sonnet_reviews ADD COLUMN hallucination_found INTEGER;
ALTER TABLE sonnet_reviews ADD COLUMN explained_by_public_info INTEGER;
ALTER TABLE sonnet_reviews ADD COLUMN explaining_articles_json TEXT;
ALTER TABLE sonnet_reviews ADD COLUMN side TEXT;
ALTER TABLE sonnet_reviews ADD COLUMN ticker TEXT;
ALTER TABLE sonnet_reviews ADD COLUMN final_buy INTEGER;
ALTER TABLE sonnet_reviews ADD COLUMN final_reason TEXT;
CREATE INDEX sonnet_reviews_candidate ON sonnet_reviews(candidate_id);
CREATE INDEX events_candidate ON events(candidate_id);
";

/// v9: per-article Haiku verdicts. `haiku_verdicts.verdict` is `relevant`, `not_relevant` or `haiku_error`;
/// `status` is `ok` or `haiku_error` (with `error` the reason and the verdict fields NULL). `detail_json`
/// keeps the raw reply. Each real verdict has one `quote_checks` row (`found`, `detail` says why).
const SCHEMA_V9: &str = "
ALTER TABLE haiku_verdicts ADD COLUMN relevant INTEGER;
ALTER TABLE haiku_verdicts ADD COLUMN points_to TEXT;
ALTER TABLE haiku_verdicts ADD COLUMN strength INTEGER;
ALTER TABLE haiku_verdicts ADD COLUMN explains_move INTEGER;
ALTER TABLE haiku_verdicts ADD COLUMN reason TEXT;
ALTER TABLE haiku_verdicts ADD COLUMN status TEXT NOT NULL DEFAULT 'ok';
ALTER TABLE haiku_verdicts ADD COLUMN error TEXT;
CREATE INDEX haiku_verdicts_candidate ON haiku_verdicts(candidate_id);
";

/// v8: article dating detail. `articles.url` is the link the search gave, `resolved_url` the publisher URL it
/// resolved to, `final_url` the page actually fetched (after redirects); `publish_ts` is the time used for the
/// window test (`time_source` says where it came from) next to every candidate time (`rss_pub_ts`,
/// `meta_pub_ts` read via `meta_source`, `wayback_ts`). `text_unavailable` marks pages kept on title and
/// snippet only.
const SCHEMA_V8: &str = "
ALTER TABLE articles ADD COLUMN final_url TEXT;
ALTER TABLE articles ADD COLUMN rss_pub_ts INTEGER;
ALTER TABLE articles ADD COLUMN meta_pub_ts INTEGER;
ALTER TABLE articles ADD COLUMN meta_source TEXT;
ALTER TABLE articles ADD COLUMN wayback_ts INTEGER;
ALTER TABLE articles ADD COLUMN text_unavailable INTEGER NOT NULL DEFAULT 0;
CREATE INDEX articles_candidate ON articles(candidate_id, decision);
";

/// v7: a NewsAPI.ai fallback row in `searches` records the RSS error(s) that triggered it (`trigger_error`).
const SCHEMA_V7: &str = "
ALTER TABLE searches ADD COLUMN trigger_error TEXT;
CREATE INDEX searches_candidate ON searches(candidate_id, source);
";

/// v6: `phrases` keeps rejected phrases too (`accepted` 0 with a `reject_reason`) and which of the up to two
/// generation calls produced each one (`attempt`).
const SCHEMA_V6: &str = "
ALTER TABLE phrases ADD COLUMN accepted INTEGER NOT NULL DEFAULT 1;
ALTER TABLE phrases ADD COLUMN reject_reason TEXT;
ALTER TABLE phrases ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;
";

/// v5: `llm_calls` records the model asked for and the model that served the call (server-side fallbacks can
/// differ), plus the exact request body (never headers) and raw response body.
const SCHEMA_V5: &str = "
ALTER TABLE llm_calls RENAME COLUMN model TO model_requested;
ALTER TABLE llm_calls RENAME COLUMN prompt TO request_json;
ALTER TABLE llm_calls RENAME COLUMN response TO response_json;
ALTER TABLE llm_calls ADD COLUMN model_used TEXT;
";

/// v4: candidate dedup looks up the newest candidate per event+direction by creation time, and the daily cap
/// counts candidates by creation time.
const SCHEMA_V4: &str = "
CREATE INDEX candidates_event_created ON candidates(event_ticker, direction, created_ts);
CREATE INDEX candidates_created ON candidates(created_ts);
";

/// v3: the watcher stores the mid price with each minute snapshot (`snapshots.ts` is the UTC minute start).
const SCHEMA_V3: &str = "
ALTER TABLE snapshots ADD COLUMN mid_c REAL;
";

/// v2: the universe refresh flags the markets it keeps (the watcher polls `active = 1` rows).
const SCHEMA_V2: &str = "
ALTER TABLE markets ADD COLUMN active INTEGER NOT NULL DEFAULT 0;
CREATE INDEX markets_active ON markets(active);
";

const SCHEMA_V1: &str = "
CREATE TABLE universe_runs (
    id            INTEGER PRIMARY KEY,
    ts            INTEGER NOT NULL,
    n_markets     INTEGER NOT NULL DEFAULT 0,
    params_json   TEXT,
    note          TEXT
);

CREATE TABLE markets (
    ticker          TEXT PRIMARY KEY,
    event_ticker    TEXT NOT NULL,
    series_ticker   TEXT,
    title           TEXT,
    category        TEXT,
    status          TEXT,
    open_ts         INTEGER,
    close_ts        INTEGER,
    volume          REAL,
    universe_run_id INTEGER REFERENCES universe_runs(id),
    first_seen_ts   INTEGER NOT NULL,
    last_seen_ts    INTEGER NOT NULL,
    raw_json        TEXT
);
CREATE INDEX markets_event ON markets(event_ticker);

CREATE TABLE snapshots (
    id            INTEGER PRIMARY KEY,
    ticker        TEXT NOT NULL,
    ts            INTEGER NOT NULL,
    yes_bid_c     REAL,
    yes_ask_c     REAL,
    last_c        REAL,
    volume        REAL,
    open_interest REAL,
    UNIQUE(ticker, ts)
);

CREATE TABLE trades (
    id          INTEGER PRIMARY KEY,
    trade_id    TEXT UNIQUE,
    ticker      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    yes_price_c REAL NOT NULL,
    count       REAL NOT NULL,
    taker_side  TEXT
);
CREATE INDEX trades_ticker_ts ON trades(ticker, ts);

CREATE TABLE scores (
    id            INTEGER PRIMARY KEY,
    ts            INTEGER NOT NULL,
    ticker        TEXT NOT NULL,
    event_ticker  TEXT NOT NULL,
    direction     TEXT,
    move_start_ts INTEGER,
    from_c        REAL,
    to_c          REAL,
    n_trades      INTEGER,
    score         REAL NOT NULL,
    parts_json    TEXT NOT NULL
);
CREATE INDEX scores_ticker_ts ON scores(ticker, ts);

CREATE TABLE candidates (
    id            INTEGER PRIMARY KEY,
    event_ticker  TEXT NOT NULL,
    direction     TEXT NOT NULL,
    move_start_ts INTEGER NOT NULL,
    score         REAL NOT NULL,
    status        TEXT NOT NULL CHECK (status IN (
        'queued','investigating','explained','unexplained','bought','no_buy','stale','failed','cap_skipped')),
    created_ts    INTEGER NOT NULL,
    updated_ts    INTEGER NOT NULL,
    best_to_c     REAL,
    from_c        REAL,
    reason        TEXT
);
CREATE INDEX candidates_event ON candidates(event_ticker, direction, move_start_ts);
CREATE INDEX candidates_status ON candidates(status);

CREATE TABLE candidate_markets (
    candidate_id INTEGER NOT NULL REFERENCES candidates(id),
    ticker       TEXT NOT NULL,
    score_json   TEXT,
    PRIMARY KEY (candidate_id, ticker)
);

CREATE TABLE phrases (
    id           INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(id),
    ts           INTEGER NOT NULL,
    phrase       TEXT NOT NULL,
    llm_call_id  INTEGER
);

CREATE TABLE searches (
    id           INTEGER PRIMARY KEY,
    candidate_id INTEGER,
    ts           INTEGER NOT NULL,
    source       TEXT NOT NULL,
    query        TEXT,
    url          TEXT,
    http_status  INTEGER,
    n_results    INTEGER,
    ok           INTEGER NOT NULL DEFAULT 1,
    error        TEXT,
    raw_path     TEXT,
    latency_ms   INTEGER,
    tokens_used  INTEGER
);
CREATE INDEX searches_ts ON searches(ts);

CREATE TABLE articles (
    id            INTEGER PRIMARY KEY,
    candidate_id  INTEGER NOT NULL REFERENCES candidates(id),
    url           TEXT NOT NULL,
    resolved_url  TEXT,
    title         TEXT,
    source        TEXT,
    publish_ts    INTEGER,
    time_source   TEXT,
    in_window     INTEGER,
    decision      TEXT,
    reason        TEXT,
    raw_path      TEXT,
    fetched_ts    INTEGER,
    UNIQUE(candidate_id, url)
);

CREATE TABLE wayback_checks (
    id         INTEGER PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES articles(id),
    ts         INTEGER NOT NULL,
    url        TEXT,
    capture_ts INTEGER,
    ok         INTEGER NOT NULL DEFAULT 1,
    error      TEXT,
    raw_path   TEXT
);

CREATE TABLE llm_calls (
    id            INTEGER PRIMARY KEY,
    candidate_id  INTEGER,
    ts            INTEGER NOT NULL,
    role          TEXT NOT NULL,
    model         TEXT NOT NULL,
    prompt        TEXT,
    response      TEXT,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    cost_usd      REAL NOT NULL DEFAULT 0,
    latency_ms    INTEGER,
    status        TEXT NOT NULL,
    error         TEXT
);
CREATE INDEX llm_calls_ts ON llm_calls(ts);

CREATE TABLE haiku_verdicts (
    id           INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(id),
    article_id   INTEGER NOT NULL REFERENCES articles(id),
    llm_call_id  INTEGER,
    ts           INTEGER NOT NULL,
    verdict      TEXT NOT NULL,
    quote        TEXT,
    detail_json  TEXT
);

CREATE TABLE quote_checks (
    id                INTEGER PRIMARY KEY,
    haiku_verdict_id  INTEGER NOT NULL REFERENCES haiku_verdicts(id),
    ts                INTEGER NOT NULL,
    quote             TEXT,
    found             INTEGER NOT NULL,
    detail            TEXT
);

CREATE TABLE sonnet_reviews (
    id           INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES candidates(id),
    llm_call_id  INTEGER,
    ts           INTEGER NOT NULL,
    decision     TEXT NOT NULL,
    reasoning    TEXT,
    detail_json  TEXT
);

CREATE TABLE paper_trades (
    id            INTEGER PRIMARY KEY,
    candidate_id  INTEGER NOT NULL REFERENCES candidates(id),
    event_ticker  TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    side          TEXT NOT NULL,
    ts            INTEGER NOT NULL,
    target_count  REAL NOT NULL,
    filled_count  REAL NOT NULL,
    avg_price_c   REAL,
    fee_usd       REAL NOT NULL DEFAULT 0,
    cost_usd      REAL,
    status        TEXT NOT NULL DEFAULT 'open',
    book_json     TEXT
);
CREATE INDEX paper_trades_event ON paper_trades(event_ticker);

CREATE TABLE settlements (
    id             INTEGER PRIMARY KEY,
    paper_trade_id INTEGER NOT NULL UNIQUE REFERENCES paper_trades(id),
    ts             INTEGER NOT NULL,
    result         TEXT NOT NULL,
    payout_usd     REAL NOT NULL,
    pnl_usd        REAL NOT NULL
);

CREATE TABLE errors (
    id           INTEGER PRIMARY KEY,
    ts           INTEGER NOT NULL,
    source       TEXT NOT NULL,
    candidate_id INTEGER,
    message      TEXT NOT NULL
);

CREATE TABLE events (
    id           INTEGER PRIMARY KEY,
    ts           INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    candidate_id INTEGER,
    payload      TEXT
);
CREATE INDEX events_kind_ts ON events(kind, ts);

CREATE TABLE kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);
";

/// v10: paper ledger in integer cents. The v1 `paper_trades`/`settlements` tables (USD REAL columns) were never
/// written to, so they are replaced. One open trade per event is also enforced by a partial unique index.
const SCHEMA_V10: &str = "
DROP TABLE settlements;
DROP TABLE paper_trades;

CREATE TABLE paper_trades (
    id            INTEGER PRIMARY KEY,
    candidate_id  INTEGER NOT NULL REFERENCES candidates(id),
    event_ticker  TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('yes','no')),
    contracts     INTEGER NOT NULL,
    cost_c        INTEGER NOT NULL,
    fee_c         INTEGER NOT NULL,
    avg_price_c   REAL NOT NULL,
    levels_json   TEXT NOT NULL,
    book_raw_path TEXT,
    close_ts      INTEGER NOT NULL,
    opened_ts     INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','settled'))
);
CREATE INDEX paper_trades_event ON paper_trades(event_ticker);
CREATE UNIQUE INDEX paper_trades_one_open_per_event ON paper_trades(event_ticker) WHERE status = 'open';

CREATE TABLE settlements (
    id         INTEGER PRIMARY KEY,
    trade_id   INTEGER NOT NULL UNIQUE REFERENCES paper_trades(id),
    result     TEXT NOT NULL,
    payout_c   INTEGER NOT NULL,
    pnl_c      INTEGER NOT NULL,
    settled_ts INTEGER NOT NULL
);
";

/// Open (creating if needed) the database at `path`: WAL, 5 s busy timeout, foreign keys on, migrated to latest.
pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        // A missing data dir is created here; real I/O errors resurface from Connection::open below.
        let _ = std::fs::create_dir_all(parent);
    }
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", true)?;
    // journal_mode returns a row, so it cannot go through execute().
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))?;
    migrate(&conn)?;
    Ok(conn)
}

fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    // IMMEDIATE so two processes opening at once serialize on the version check.
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    let current: usize = tx.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))? as usize;
    for (i, sql) in MIGRATIONS.iter().enumerate().skip(current) {
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", (i + 1) as i64)?;
    }
    tx.commit()
}

/// Run `f` inside one write transaction. Commits on `Ok`; on `Err` (or panic) the transaction is rolled back.
pub fn with_tx<T>(
    conn: &Connection,
    f: impl FnOnce(&Transaction<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    match f(&tx) {
        Ok(v) => {
            tx.commit()?;
            Ok(v)
        }
        Err(e) => {
            // Drop would roll back too; do it explicitly so a rollback failure is not silent.
            if let Err(re) = tx.rollback() {
                return Err(e.context(format!("rollback also failed: {re}")));
            }
            Err(e)
        }
    }
}
