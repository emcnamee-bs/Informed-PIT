# Informed-PIT

Paper-trading Kalshi bot (no orders, no Kalshi auth): detects surprise price moves, checks whether the news
explains them, and paper-trades the unexplained ones. One tokio binary, `informed-pit`.

Design: `docs/superpowers/specs/2026-11-18-informed-pit-design.md`. Build plan: `docs/superpowers/plans/`.

Build and test on mini-mac (no local Rust toolchain on ermac): `bash scripts/remote-cargo.sh test`.

## Runbook

### Commands

| Command | What it does |
|---|---|
| `informed-pit run` | The service (systemd runs it): universe refresh at start and daily at 06:00 UTC, watcher + scorer every minute, investigator, settlement every 10 min, `status.json` every minute. `--rules <file>` picks the universe rules (default `config/universe.toml`, also found next to the repo the binary was built in). |
| `informed-pit start --confirm` | Starts the 30-day run now (stores `run_start_ts` / `run_end_ts` in the database). Refuses if a run was already started. Before this the service only watches and scores (warm-up). |
| `informed-pit pause` / `resume` | Kill switch: no new investigations (watching, scoring and settlement continue). Sets a flag in the database and creates/removes the file `$PIT_DATA/PAUSE`; either one pauses. `touch $PIT_DATA/PAUSE` works without the binary. |
| `informed-pit reset-cap` | Manual reset after the daily LLM cap stopped investigations: clears today's cap mark so they resume (spend at or over a cap still stops them). |
| `informed-pit status` | Prints the status JSON (same as `$PIT_DATA/status.json`, built fresh). |
| `informed-pit report <candidate_id> [--json]` | The whole trail of one candidate: members, phrases, searches, articles and their window decisions, Haiku verdicts, quote checks, Sonnet review, LLM calls (metering), paper trade, settlement, events, errors. |
| `informed-pit verdict` | KEEP / SHUT OFF / NOT ENOUGH TRADES on the settled paper trades, with the 95 % event-clustered range; provisional until the last trade settles. |
| `informed-pit universe [--dry-run]` | Builds the market universe once (dry run: prints what would be kept, touches no data). |
| `informed-pit llm-smoke` | One tiny Haiku and one tiny Sonnet call (needs `ANTHROPIC_API_KEY`). |
| `informed-pit news-smoke <query>` | One live Bing News RSS search (no key). |

### Lifecycle

`not_started` (watch and score only; scores need 24 h of minute history) → `start --confirm` → `active` for 30
days (candidates, investigations, paper buys) → `settlement_only` while paper trades are open (watcher, scorer and
investigator off) → `finished`. Caps: Anthropic $3/day and $60/run, 30 candidates/day; when the daily cap is hit the
queued candidates end as `cap_skipped` and investigation stops until the next UTC day (or `reset-cap`). Voided
settlements (market resolved neither yes nor no) are counted separately and left out of the verdict.

**A trade whose market never returns a result keeps the service in `settlement_only`.** Settlement retries every
10 minutes, forever, and the phase cannot reach `finished` (nor the verdict stop being provisional) while any paper
trade is open. To find such trades: `informed-pit status` (`trades.open`) and

```sh
sqlite3 "$PIT_DATA/pit.db" "SELECT id, candidate_id, ticker, side, close_ts, datetime(close_ts,'unixepoch')
  FROM paper_trades WHERE status = 'open' ORDER BY close_ts"
```

then look at the market (`curl -s https://api.elections.kalshi.com/trade-api/v2/markets/<ticker>`: `status`,
`result`) and at the trade's settlement errors (`SELECT * FROM errors WHERE source = 'settlement' ORDER BY ts DESC`).
If Kalshi has finalised the market but `result` stays empty, or the market is gone, settle it by hand as a void
(cost refunded, left out of the verdict, same as the code does), with the service stopped:

```sh
systemctl --user stop informed-pit
sqlite3 "$PIT_DATA/pit.db" "BEGIN;
  INSERT INTO settlements (trade_id, result, payout_c, pnl_c, settled_ts)
    SELECT id, 'manual_void', cost_c, 0, strftime('%s','now') FROM paper_trades WHERE id = <trade_id> AND status = 'open';
  UPDATE paper_trades SET status = 'settled' WHERE id = <trade_id>;
  INSERT INTO events (ts, kind, candidate_id, payload)
    SELECT strftime('%s','now'), 'manual_settlement', candidate_id,
      json_object('trade_id', <trade_id>, 'why', '<reason>')
    FROM paper_trades WHERE id = <trade_id>;
COMMIT;"
systemctl --user start informed-pit
```

If the market did resolve yes or no, use that `result` with `payout_c = contracts * 100` when it equals the trade's
`side` (else 0) and `pnl_c = payout_c - cost_c - fee_c`.

### Configuration

Environment (the systemd unit loads `~/.config/informed-pit/env`, mode 600, never in git):
`ANTHROPIC_API_KEY`, `NEWSAPI_AI_KEY` (optional fallback), `PIT_DATA` (default `~/informed-pit-data`),
`PIT_SCORE_THRESHOLD` (80), `PIT_DAILY_USD_CAP` (3), `PIT_RUN_USD_CAP` (60), `PIT_MAX_CANDIDATES_PER_DAY` (30).
Universe rules: `config/universe.toml`.

### Data layout (`$PIT_DATA`)

| Path | What |
|---|---|
| `pit.db` | SQLite (WAL): markets, minute snapshots and trades (60-day retention), scores, candidates and their whole trail, LLM calls, paper trades, settlements, errors, events, `kv` (run window, pause flag, NewsAPI ledger, heartbeats). |
| `logs/YYYY-MM-DD.jsonl` | One JSON line per event or error (UTC dates). Deleted after 45 days by the daily prune. |
| `raw/YYYY-MM-DD/` | Gzipped raw HTTP bodies. The watcher's every-minute Kalshi reads are not stored (their content is in `snapshots`/`trades`). The daily prune deletes files older than 7 days unless a row in `articles`, `searches`, `wayback_checks` or `paper_trades` points at them (those are kept for good). |
| `status.json` | Wall tile 17 feed, rewritten atomically every minute. |
| `PAUSE` | Present = paused. |

### Deploy

mini-mac: bare repo `~/Informed-PIT.git`, checkout `~/Informed-PIT` on branch `deploy`,
`cargo build --release`, systemd user unit `informed-pit.service` (see `deploy/` once added, plan Task 16).
Logs: `journalctl --user -u informed-pit` and `$PIT_DATA/logs/`.
