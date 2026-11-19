# Informed-PIT Build Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and deploy the Informed-PIT paper-trading bot (Rust, one process on mini-mac) and wall tile 17, as specified in the design.

**Architecture:** One tokio binary `informed-pit` with internal modules: universe → watcher → scorer → candidate queue → investigator (Haiku phrases, RSS/NewsAPI search, article dating, Haiku verdicts, quote check, Sonnet review) → paper ledger → settlement; one SQLite DB, daily JSONL logs, raw HTTP bodies on disk, `status.json` for the wall. Every external call goes through one shared HTTP layer that rate-limits, retries and logs.

**Tech Stack:** Rust stable (rustup, user install), tokio, reqwest (rustls), rusqlite (bundled), serde/serde_json, clap, chrono, quick-xml, scraper, tracing, flate2, wiremock (tests), tempfile (tests).

**Spec:** `docs/superpowers/specs/2026-11-18-informed-pit-design.md` (read it first; section numbers below refer to it).

## Global Constraints

- Paper only: no Kalshi auth, no order endpoints, ever.
- Kalshi base `https://api.elections.kalshi.com/trade-api/v2`; ≤ 2 req/s average for this process.
- Universe: allowed categories from `config/universe.toml`; close in > 1 h and ≤ 14 days; traded in last 7 days; two-sided quote; cap 300 by recent volume; refresh daily 06:00 UTC and on start.
- Score threshold default 80; store every score ≥ 40 with its four parts; abstain without 24 h minute history or < 5 trades in window.
- Article window: `move_start − 15 min ≤ publish_time ≤ investigation_time`.
- RSS: ≥ 3 s between requests. NewsAPI.ai: ≤ 10 tokens/candidate, ≤ 100/day, stop at 200 tokens remaining. Wayback ≤ 1 req/s. Article fetch ≤ 2 concurrent, 10 s timeout, 2 MB cap.
- Kept articles ≤ 40 per candidate (newest first).
- Models: `claude-haiku-5-5` (thinking `{type:"disabled"}`, effort default), `claude-sonnet-5-5` (thinking `{type:"adaptive"}`, `output_config.effort:"medium"`, beta `server-side-fallback-2026-07-01` + `fallbacks:"default"`). Header `anthropic-version: 2026-10-21`. JSON via `output_config.format` (`{type:"json_schema", schema}`). Prices per MTok: Haiku 5.5 $0.10 in / $0.50 out (≤100K prompt), Sonnet 5.5 $2 in / $10 out.
- Spend caps: Anthropic $3/day, $60/run; ≤ 30 candidates/day.
- Paper trade: target 25 contracts, walk book at ≤ 92¢, smaller fill if thin, none → no trade; taker fee; one open position per event; hold to settlement.
- Candidate identity: event + direction + move start (minute); 30 min join window; 6 h cooldown unless ≥ 10 more points; stale after 20 min or price back past move start.
- Run: 30 days of new candidates, then settlement-only until last trade settles; verdict KEEP if settled P&L after fees > 0 with ≥ 30 settled trades, else SHUT OFF, or NOT ENOUGH TRADES if < 30; stats clustered by event.
- Data under `$PIT_DATA` (default `~/informed-pit-data`): `pit.db`, `logs/YYYY-MM-DD.jsonl`, `raw/YYYY-MM-DD/`, `status.json`. Nothing of this in git; repo is public.
- Secrets only from env file `~/.config/informed-pit/env` (`ANTHROPIC_API_KEY`, `NEWSAPI_AI_KEY`); never logged.
- All timestamps UTC unix seconds in the DB.

## Review Focus

1. **Kalshi field drift** (prices as dollars strings like `yes_bid_dollars`, counts as `count_fp`): parsing must accept the live shape captured in fixtures, not a remembered one. Pinned in Task 3 by fixture tests from a live capture.
2. **Article with no publish time anywhere** (no metadata, no RSS date, no Wayback capture): must be logged and excluded, never kept with a guessed time. Pinned in Task 11.
3. **Same move scored every minute for hours**: exactly one candidate and bounded LLM calls. Pinned in Task 7 (steady-state test).
4. **Process restart mid-investigation**: candidate in `investigating` on boot is marked `failed:restart`, not resumed blindly or lost. Pinned in Task 13.
5. **RSS returns 200 with an HTML consent/captcha page**: counts as an RSS error and triggers fallback (not "0 results"). Pinned in Task 10.

---

## File structure

```
Cargo.toml
config/universe.toml            category allow-list + exclusions (editable without rebuild)
src/main.rs                     clap CLI, wiring
src/config.rs                   Settings (env + defaults)
src/db.rs                       open(), migrations, small helpers
src/eventlog.rs                 JSONL event log + raw body store
src/http.rs                     shared client: per-service limiter, retry/backoff, request logging
src/kalshi.rs                   Kalshi client + types + fee
src/universe.rs                 daily market selection
src/watcher.rs                  minute snapshots + trades
src/score.rs                    surprise score (pure)
src/candidates.rs               threshold, event grouping, dedup, priority queue
src/llm.rs                      Anthropic Messages client, spend governor, metering
src/phrases.rs                  Haiku phrase generation
src/news/mod.rs                 search orchestration (RSS then fallback)
src/news/rss.rs                 Google News RSS
src/news/newsapi.rs             NewsAPI.ai fallback + token ledger
src/news/article.rs             fetch, redirect resolve, text extract
src/news/pubtime.rs             publish-time extraction + window test
src/news/wayback.rs             Wayback CDX backup
src/verdict_haiku.rs            per-article Haiku verdict + quote check
src/review.rs                   Sonnet review + decision
src/investigate.rs              per-candidate state machine
src/paper.rs                    order-book walk, ledger, settlement
src/run.rs                      run lifecycle, final verdict, pause flag
src/status.rs                   status.json writer
deploy/informed-pit.service     systemd user unit
deploy/install.sh               mini-mac setup script
tests/fixtures/...              captured live API bodies
README.md                       runbook
```

---

### Task 1: Scaffold, config, database, event log

**Files:** Create `Cargo.toml`, `src/main.rs`, `src/config.rs`, `src/db.rs`, `src/eventlog.rs`, `.gitignore`, `README.md` (stub). Test: `tests/db_eventlog.rs`.

**Interfaces:**
- Produces: `Settings::from_env() -> Settings` with fields `data_dir: PathBuf, anthropic_key: Option<String>, newsapi_key: Option<String>, score_threshold: f64 (80.0), daily_usd_cap: f64 (3.0), run_usd_cap: f64 (60.0), max_candidates_per_day: u32 (30)`.
- Produces: `db::open(path: &Path) -> rusqlite::Result<Connection>` (WAL, `busy_timeout` 5 s, foreign keys on, runs migrations idempotently). Tables exactly as spec §11 (`universe_runs, markets, snapshots, trades, scores, candidates, candidate_markets, phrases, searches, articles, wayback_checks, llm_calls, haiku_verdicts, quote_checks, sonnet_reviews, paper_trades, settlements, errors, events, kv`). `candidates.status` ∈ `queued|investigating|explained|unexplained|bought|no_buy|stale|failed|cap_skipped`.
- Produces: `EventLog::new(data_dir) -> EventLog`; `log(&self, kind: &str, candidate_id: Option<i64>, payload: serde_json::Value)` appends one JSON line to `logs/YYYY-MM-DD.jsonl` with `ts`; `store_raw(&self, label: &str, body: &[u8]) -> String` gzip-writes to `raw/YYYY-MM-DD/<uuid>-<label>.gz` and returns the relative path.
- Produces: `db::with_tx<T>(conn, f) -> Result<T>` that always rolls back on error.

- [ ] **Step 1: Write failing tests** `migrations_idempotent` (open twice, table list equals the 20 names), `with_tx_rolls_back_on_error` (insert then return Err → row count 0), `eventlog_appends_jsonl` (two `log` calls → file has 2 lines, each parses, has `kind` and `ts`), `store_raw_roundtrip` (gunzip equals input).
- [ ] **Step 2: Run** `cargo test --test db_eventlog` → fails to compile (modules missing).
- [ ] **Step 3: Implement** the four modules; `.gitignore` covers `target/`, `*.db`, `data/`, `.env*`.
- [ ] **Step 4: Run** `cargo test --test db_eventlog` → 4 passed.
- [ ] **Step 5: Commit** `feat: scaffold, settings, sqlite schema, event log`.

### Task 2: Shared HTTP layer

**Files:** Create `src/http.rs`. Test: `tests/http.rs` (wiremock).

**Interfaces:**
- Produces: `enum Service { Kalshi, Rss, NewsApi, Wayback, Article, Anthropic }`; `Http::new(log: EventLog, db: Arc<Mutex<Connection>>) -> Http` with per-service spacing (Kalshi 500 ms, Rss 3000 ms, Wayback 1000 ms, NewsApi 1000 ms, Anthropic 0, Article: semaphore 2).
- Produces: `Http::send(&self, svc: Service, req: reqwest::RequestBuilder, ctx: CallCtx) -> Result<HttpResult, HttpError>` where `CallCtx { label: &'static str, candidate_id: Option<i64>, max_attempts: u32 }`, `HttpResult { status: u16, body: Bytes, final_url: String, elapsed_ms: u64, raw_path: String }`. Retries 429/5xx/network with exponential backoff 2^n s (cap 60 s, honour `Retry-After`), up to `max_attempts`. Every attempt writes an `http` event (service, url without query secrets, status, elapsed, attempt) and the final body via `store_raw`.
- Header/query values named `x-api-key`, `apiKey` are redacted in logs.

- [ ] **Step 1: Failing tests** `spacing_enforced_per_service` (3 Rss calls take ≥ 6 s with spacing overridden to 3 s… use a test constructor `Http::with_spacing(map)` and 100 ms to keep tests fast: assert ≥ 200 ms), `retries_429_then_succeeds` (mock 429 once then 200 → status 200, 2 attempts logged), `gives_up_after_max_attempts` (always 503, max 3 → `HttpError::Exhausted{attempts:3}`), `redacts_api_key_in_log` (log file has no key string).
- [ ] **Step 2: Run** `cargo test --test http` → fail.
- [ ] **Step 3: Implement** `src/http.rs`.
- [ ] **Step 4: Run** → 4 passed.
- [ ] **Step 5: Commit** `feat: shared rate-limited, logged HTTP layer`.

### Task 3: Kalshi client, types, fee

**Files:** Create `src/kalshi.rs`, `tests/fixtures/kalshi/{markets.json,trades.json,orderbook.json,events.json,settled_market.json}`. Test: `tests/kalshi.rs`.

**Interfaces:**
- Produces types: `Market { ticker, event_ticker, series_ticker, title, subtitle, rules_primary, category: Option<String>, close_ts: i64, yes_bid_c: Option<u32>, yes_ask_c: Option<u32>, last_c: Option<u32>, volume: i64, volume_24h: i64, open_interest: i64, status: String, result: Option<String> }` (prices in integer cents 0–100), `Trade { trade_id, ticker, yes_price_c: u32, count: i64, taker_side: Side, created_ts: i64 }`, `enum Side { Yes, No }`, `OrderBook { yes: Vec<(u32,i64)>, no: Vec<(u32,i64)> }` (bids, cents, qty).
- Produces client fns: `list_open_markets(&Http) -> Vec<Market>` (paginated, cursor), `get_markets(&Http, tickers: &[String]) -> Vec<Market>` (batches of 100), `get_trades(&Http, ticker, min_ts) -> Vec<Trade>`, `get_orderbook(&Http, ticker) -> OrderBook`, `get_event_category(&Http, event_ticker) -> Option<String>` (cached in `kv`), `get_market(&Http, ticker) -> Market`.
- Produces: `taker_fee_cents(count: i64, price_c: u32) -> i64` = ceil(7 × count × p × (100 − p) / 10000) where p = price in cents (i.e. ceil(0.07·C·P·(1−P)) dollars in cents).

- [ ] **Step 1: Capture fixtures from the live API** (public, no key): `curl -s "$BASE/markets?status=open&limit=5" > tests/fixtures/kalshi/markets.json`, likewise `/markets/trades?ticker=<one from it>&limit=20`, `/markets/<ticker>/orderbook`, `/events/<event_ticker>`, and one settled market (`/markets?status=settled&limit=1`). Read them; note which price/count fields exist (dollar strings vs integer cents, `count` vs `count_fp`).
- [ ] **Step 2: Verify the fee formula** against Kalshi's current fee schedule page (help/docs "fees"); if different, update `taker_fee_cents` spec and its test values here and note the source URL in a code comment.
- [ ] **Step 3: Failing tests** `parses_live_markets_fixture` (≥ 1 market, every price in 0..=100 or None, close_ts > 0), `parses_trades_fixture_taker_side`, `parses_orderbook_fixture_sorted_desc`, `parses_settled_result_yes_or_no`, `fee_25_at_50c_is_44` (`taker_fee_cents(25,50)==44`), `fee_1_at_92c_is_1`, `fee_never_negative_at_edges` (prices 1 and 99).
- [ ] **Step 4: Run** `cargo test --test kalshi` → fail.
- [ ] **Step 5: Implement** with serde structs mirroring the fixture shape and a conversion to the types above (dollars strings × 100 rounded to cents).
- [ ] **Step 6: Run** → 7 passed.
- [ ] **Step 7: Commit** `feat: kalshi public client, types, taker fee (fixtures from live API)`.

### Task 4: Market universe

**Files:** Create `src/universe.rs`, `config/universe.toml`. Test: `tests/universe.rs`.

**Interfaces:**
- Consumes: `kalshi::list_open_markets`, `get_event_category`, `get_trades`.
- Produces: `UniverseRules::load(path) -> UniverseRules { allow_categories: Vec<String>, deny_series_prefixes: Vec<String>, max_markets: usize (300), max_close_days: i64 (14), min_close_secs: i64 (3600) }`; pure `select(markets: &[MarketWithCategory], rules: &UniverseRules, now: i64) -> Selection { kept: Vec<String>, rejected: Vec<(String, &'static str)> }` with reasons `category|closes_too_soon|closes_too_late|no_two_sided_quote|no_recent_trade|cap`; `refresh(&Http, &Connection, &EventLog, &rules, now) -> Result<i64 /*universe_run id*/>` writes `universe_runs`, `markets` (active flag) and one `universe` event with counts.
- `config/universe.toml` initial allow list, exactly: `Politics, Elections, Economics, Financials, Companies, World, Science and Technology, Health, Transportation` (weather, sports, entertainment excluded by omission); deny prefixes for crypto/index ranges: `KXBTC, KXETH, KXSOL, KXINX, KXNASDAQ, KXDOGE, KXXRP`. Builder confirms category strings against the `events.json` fixture and the live `/events` list and adjusts names (not intent).
- "Recent trade" = `volume_24h > 0` or last trade within 7 days (one `get_trades(limit=1)` only for markets with `volume_24h == 0`).

- [ ] **Step 1: Failing tests** (pure `select`): `rejects_sports_category`, `rejects_close_in_30_min`, `rejects_close_in_15_days`, `keeps_close_in_13_days`, `rejects_one_sided_quote`, `rejects_deny_prefix_btc`, `caps_at_max_by_volume_24h` (305 eligible → 300 kept, 5 rejected `cap`, highest volumes kept).
- [ ] **Step 2: Run** → fail. **Step 3: Implement.** **Step 4: Run** → 7 passed.
- [ ] **Step 5: Live dry run** `cargo run -- universe --dry-run` prints kept count and top rejection reasons; expect 50–300 kept. Paste the summary into the commit message body.
- [ ] **Step 6: Commit** `feat: daily market universe (≤14-day close, category allow-list)`.

### Task 5: Watcher (minute snapshots + trades)

**Files:** Create `src/watcher.rs`. Test: `tests/watcher.rs` (wiremock).

**Interfaces:**
- Consumes: `kalshi::get_markets`, `get_trades`, `db`.
- Produces: `tick(&Http, &Connection, &EventLog, minute_ts: i64) -> TickReport { markets: usize, snapshots: usize, trade_rows: usize, gaps: usize }`. Inserts one `snapshots` row per active market `(ticker, minute_ts, yes_bid_c, yes_ask_c, last_c, mid_c, volume, open_interest)`; fetches trades only where `volume` rose since the previous snapshot, from `last_trade_ts+1`; inserts `trades` with `INSERT OR IGNORE` on `trade_id`. A market whose previous snapshot is not `minute_ts − 60` logs a `gap` event (count of missing minutes).
- `mid_c` = (bid+ask)/2 when both present, else last, clamped 1..=99.
- Retention: `prune(&Connection, now)` deletes snapshots/trades older than 60 days.

- [ ] **Step 1: Failing tests** `writes_snapshot_per_market`, `fetches_trades_only_when_volume_rises`, `trade_dedup_by_id`, `logs_gap_after_missing_minute`, `mid_falls_back_to_last_and_clamps`.
- [ ] **Step 2–4:** run fail → implement → 5 passed.
- [ ] **Step 5: Commit** `feat: minute watcher with trades-on-volume-change and gap logging`.

### Task 6: Surprise score (pure)

**Files:** Create `src/score.rs`. Test: `tests/score.rs`.

**Interfaces:**
- Produces: `struct MinuteBar { ts: i64, mid_c: u32, volume: i64, taker_yes: i64, taker_no: i64 }` (volume and taker counts are per-minute deltas);
  `score_market(history: &[MinuteBar], now_ts: i64) -> Option<Score>` where `Score { total: f64, move_pts: f64, volume_pts: f64, onesided_pts: f64, quiet_pts: f64, window_min: u32, move_start_ts: i64, direction: Side, from_c: u32, to_c: u32, move_z: f64, vol_ratio: f64, onesided: f64, quiet_ratio: f64 }`; returns `None` (abstain) when fewer than 1,200 bars in the trailing 24 h or fewer than 5 trades in the chosen window.
- Algorithm (spec §7, values fixed here):
  - `lo(c) = ln(p/(1−p))`, p = c/100 clamped [0.01, 0.99].
  - `sigma24` = std dev of 1-min Δlo over the trailing 24 h (excluding the candidate window); floor 1e-4.
  - For each window w ∈ {5,10,15,20,30,45,60} min ending at `now_ts`: `z_w = |lo(end) − lo(start)| / (sigma24·√w)`. Choose the w with max `z_w`; direction = sign.
  - `lin(x, a, b) = 25·clamp((x−a)/(b−a), 0, 1)`.
  - move_pts = lin(z, 2, 8); volume_pts = lin(window_vol / max(1, median of the 24 h's non-overlapping same-length window volumes), 1, 10); onesided = (dir_taker − other_taker)/total within window, onesided_pts = lin(onesided, 0.3, 0.9); quiet_ratio = std of Δlo in the 60 min before the window / sigma24, quiet_pts = 25 − lin(quiet_ratio, 0.3, 1.0).
  - total = sum (0–100).

- [ ] **Step 1: Failing tests** `abstains_with_short_history`, `flat_market_scores_below_20`, `crawl_50_to_85_in_30min_on_one_sided_volume_scores_at_least_80` (24 h of 50±1 noise, low volume; then 30 min ramp 50→85, volume 10×, 90 % taker yes → total ≥ 80, direction Yes, window_min 30, move_start_ts = ramp start), `same_move_two_sided_volume_scores_lower` (taker 50/50 → onesided_pts 0), `noisy_market_same_move_scores_lower` (24 h noise ±8 → total < 80), `down_move_direction_no`, `deterministic` (same input twice → identical Score).
- [ ] **Step 2–4:** fail → implement → 7 passed.
- [ ] **Step 5: Commit** `feat: deterministic surprise score`.

### Task 7: Candidates: threshold, event grouping, dedup, queue

**Files:** Create `src/candidates.rs`. Test: `tests/candidates.rs`.

**Interfaces:**
- Consumes: `score::Score`, `db`.
- Produces: `on_score(&Connection, &EventLog, ticker, event_ticker, &Score, now, threshold) -> Outcome` with `enum Outcome { Stored /*≥40 <thr*/, Ignored /*<40*/, NewCandidate(i64), JoinedCandidate(i64), DedupSuppressed(i64), DailyCapSkipped }`.
  Rules: identity (event, direction, move_start minute); a score ≥ threshold for an event that has a candidate in the same direction created < 30 min ago joins it (`candidate_markets` row); otherwise if an event+direction candidate exists < 6 h old and this market's `to_c` is not ≥ 10 points beyond that candidate's best `to_c` → `DedupSuppressed`; ≥ 30 candidates created today (UTC) → `DailyCapSkipped`; else insert `candidates(status='queued', score, created_ts)`.
- Produces: `next_queued(&Connection) -> Option<i64>` (highest score first, then oldest); `expire_old(&Connection, now)` marks queued > 20 min as `stale`.

- [ ] **Step 1: Failing tests** `below_40_ignored`, `between_40_and_threshold_stored_not_candidate`, `first_crossing_creates_candidate`, `second_market_same_event_within_30min_joins`, `repeat_within_6h_suppressed`, `repeat_10_points_further_creates_new`, `daily_cap_30`, `priority_highest_score_first`, `queued_expires_after_20min`, **`steady_state_same_move_for_3_days_creates_one_candidate`** (feed the same ≥ threshold score every minute for 4,320 minutes → exactly 1 `NewCandidate`, rest `DedupSuppressed` except re-arm after 6 h only if price moved: none here → total candidates 1).
- [ ] **Step 2–4:** fail → implement → 10 passed.
- [ ] **Step 5: Commit** `feat: candidate creation with per-event dedup, cap, priority queue`.

### Task 8: Anthropic client, spend governor, metering

**Files:** Create `src/llm.rs`. Test: `tests/llm.rs` (wiremock for `api.anthropic.com`).

**Interfaces:**
- Produces: `enum Model { Haiku, Sonnet }` → ids `claude-haiku-5-5`, `claude-sonnet-5-5`; `Llm::new(http, db, log, settings, base_url)`;
  `Llm::json_call(&self, model: Model, role: &'static str, candidate_id: Option<i64>, system: &str, user: &str, schema: serde_json::Value, max_tokens: u32) -> Result<(serde_json::Value, CallMeta), LlmError>`.
  Request body: `{model, max_tokens, system, messages:[{role:"user",content:user}], output_config:{format:{type:"json_schema", schema}}}` plus for Haiku `thinking:{type:"disabled"}`; for Sonnet `thinking:{type:"adaptive"}`, `output_config.effort:"medium"`, `fallbacks:"default"` and header `anthropic-beta: server-side-fallback-2026-07-01`. Headers `x-api-key`, `anthropic-version: 2026-10-21`.
  Checks `stop_reason`: `refusal` → `LlmError::Refusal{category}`; `max_tokens` → `LlmError::Truncated`; parses the first text block as JSON (`LlmError::BadJson` with one automatic retry).
- Governor: before each call reserve an estimate (input chars/3.5 tokens + max_tokens at the model's prices); refuse with `LlmError::CapHit{scope:"day"|"run"}` if day spend + reserve > $3 or run spend + reserve > $60; after the call record actual cost from `usage` (input, output, cache read/write) using the prices in Global Constraints and the actual `model` in the response (fallbacks may change it; unknown model → priced at Sonnet rates and logged).
- Every call writes `llm_calls (candidate_id, role, model_requested, model_used, request_json, response_json, input_tokens, output_tokens, cost_usd, latency_ms, status, error)`.
- Produces: `spend_today(&Connection) -> f64`, `spend_run(&Connection) -> f64`.

- [ ] **Step 1: Failing tests** `haiku_body_has_thinking_disabled_and_schema`, `sonnet_body_has_adaptive_medium_and_fallback_header`, `records_cost_from_usage` (1,000 in/200 out Haiku → $0.0002), `refusal_maps_to_error_and_is_logged`, `bad_json_retried_once`, `day_cap_blocks_call_before_http` (seed $2.99 spend → `CapHit`, mock received 0 requests).
- [ ] **Step 2–4:** fail → implement → 6 passed.
- [ ] **Step 5: Live smoke (costs < $0.01, needs key on the build machine or run on mini-mac later):** `cargo run -- llm-smoke` sends one tiny Haiku and one tiny Sonnet JSON call and prints both results and costs. If `output_config.format` or the fallback beta is rejected, fix the body from the live docs (`shared/live-sources.md` of the claude-api skill) and adjust the tests.
- [ ] **Step 6: Commit** `feat: anthropic json client with spend governor and metering`.

### Task 9: Haiku phrase generation

**Files:** Create `src/phrases.rs`. Test: `tests/phrases.rs`.

**Interfaces:**
- Consumes: `Llm::json_call`.
- Produces: `generate(&Llm, &Connection, candidate_id, ctx: &MarketContext) -> Result<Vec<String>>`, `MarketContext { title, subtitle, rules, direction: Side, from_c, to_c, window_min }`. Schema `{phrases: string[50]}`. Validation (pure `validate(raw: Vec<String>) -> (Vec<String>, Vec<(String,&'static str)>)`): trim, lowercase dedupe, 2–4 words, ≤ 60 chars, no quotes. If < 50 valid, one retry asking for the missing count; proceed with whatever is valid (≥ 10 required, else candidate `failed:phrases`). All phrases and rejects stored in `phrases`.

- [ ] **Step 1: Failing tests** `validate_rejects_one_word_and_five_words`, `validate_dedupes_case_insensitively`, `retry_requests_missing_count` (mock returns 40 then 10 → 50 stored, 2 llm_calls), `fails_below_10_valid`.
- [ ] **Step 2–4:** fail → implement → 4 passed.
- [ ] **Step 5: Commit** `feat: haiku 50-phrase generation with validation`.

### Task 10: News search (RSS primary, NewsAPI.ai fallback)

**Files:** Create `src/news/mod.rs`, `src/news/rss.rs`, `src/news/newsapi.rs`, `tests/fixtures/news/{rss_ok.xml,rss_consent.html,newsapi_ok.json}`. Test: `tests/news_search.rs`.

**Interfaces:**
- Produces: `build_queries(phrases: &[String]) -> Vec<String>` — OR-joins quoted phrases into queries whose URL-encoded `q` is ≤ 1,500 chars (≈ 10 queries for 50 phrases).
- Produces: `rss::search(&Http, q, candidate_id) -> Result<Vec<Hit>, RssError>` using `https://news.google.com/rss/search?q=<q>+when:1d&hl=en-US&gl=US&ceid=US:en`; `Hit { title, link, source, rss_pub_ts: Option<i64>, snippet, via: Via::Rss|Via::NewsApi, newsapi_pub_ts: Option<i64> }`. `RssError` when: transport/HTTP error after retries, body does not parse as RSS XML, content-type is HTML, or body contains no `<channel>`. A valid channel with 0 items is `Ok(vec![])`.
- Produces: `newsapi::search(&Http, &Connection, key, phrases: &[String], candidate_id) -> Result<Vec<Hit>, NewsApiError>`: POST `https://eventregistry.org/api/v1/article/getArticles` (confirm the endpoint in the NewsAPI.ai docs during this task) with `keyword` = phrases, `keywordOper:"or"`, `articlesSortBy:"date"`, `articlesCount:100`, `resultType:"articles"`, `dataType:["news"]`; reads `dateTimePub`/`dateTime`. Token ledger in `kv`: `newsapi_tokens_used` (+1 per page), `newsapi_tokens_remaining` (starts 2000; refreshed from the API's remaining-tokens field if the response has one). Refuses (`NewsApiError::Cap`) when this candidate already used 10, today used 100, or remaining ≤ 200.
- Produces: `search_all(...) -> SearchOutcome { hits: Vec<Hit>, rss_ok: u32, rss_failed: u32, fallback_used: bool, tokens_spent: u32 }`: runs every RSS query; if any failed, one NewsAPI.ai call covering the phrases of the failed queries; every request and its outcome in `searches`; dedupe hits by canonical link.

- [ ] **Step 1: Failing tests** `build_queries_respects_length_and_covers_all_phrases`, `rss_parses_items_and_pubdate`, `rss_empty_channel_is_ok_empty`, **`rss_html_consent_page_is_error`**, `fallback_runs_only_when_an_rss_query_failed`, `fallback_blocked_at_200_remaining`, `fallback_blocked_after_100_today`, `fallback_logs_trigger_error_and_tokens`.
- [ ] **Step 2–4:** fail → implement → 8 passed.
- [ ] **Step 5: Live check** `cargo run -- news-smoke "federal reserve rate decision"`: prints hit count, first 3 links, and whether links are Google redirect URLs. Record the result in the commit body.
- [ ] **Step 6: Commit** `feat: google news rss search with capped newsapi.ai fallback`.

### Task 11: Article fetch, publish time, Wayback, window test

**Files:** Create `src/news/article.rs`, `src/news/pubtime.rs`, `src/news/wayback.rs`, `tests/fixtures/articles/{og_time.html,jsonld.html,time_tag.html,no_time.html}`. Test: `tests/articles.rs`.

**Interfaces:**
- Produces: `resolve_link(&Http, link) -> Result<String>`: for `news.google.com` links, follow redirects; if the final page is still Google, decode the publisher URL from the page (method found in Task 10's live check; document it in a comment). Unresolvable → article logged `unresolved`, excluded.
- Produces: `fetch(&Http, url, candidate_id) -> Result<Page { final_url, html, text, title }>` (10 s, 2 MB cap; text via `scraper`: drop script/style/nav/footer, join `<p>` text; ≤ 20,000 chars kept for Haiku).
- Produces: pure `extract_pub_ts(html: &str) -> Option<(i64, &'static str)>` in the order `article:published_time` meta → JSON-LD `datePublished` → `<time datetime>` → `og:updated_time`/`date`/`pubdate` meta; returns source tag.
- Produces: `wayback::earliest_capture(&Http, url) -> Result<Option<i64>>` via `http://web.archive.org/cdx/search/cdx?url=<url>&output=json&limit=1&fl=timestamp&from=<yyyymmdd of window start − 1 day>`.
- Produces: `date_article(meta: Option<i64>, rss: Option<i64>, newsapi: Option<i64>, now: i64, wayback: impl FnOnce() -> Option<i64>) -> Dated { ts: Option<i64>, source: TimeSource }`, `TimeSource ∈ {Metadata, NewsApiPub, Wayback, None}`. Uses metadata when present, not in the future (> now + 5 min) and within 6 h of RSS pubDate when RSS has one; otherwise NewsAPI `dateTimePub`; otherwise Wayback earliest capture; otherwise `None` (excluded).
- Produces: pure `in_window(ts, move_start, investigation_ts) -> bool` = `move_start − 900 ≤ ts ≤ investigation_ts`.
- Produces: `collect(&Http, &Connection, &EventLog, candidate_id, hits, move_start, now) -> Vec<KeptArticle>` writing one `articles` row per hit with `{url, final_url, title, rss_pub_ts, meta_pub_ts, meta_source, wayback_ts, used_ts, time_source, decision: kept|out_of_window|no_time|fetch_failed|unresolved|dup|over_cap, text_unavailable}`; keeps ≤ 40 newest.

- [ ] **Step 1: Failing tests** `og_time_extracted`, `jsonld_date_extracted`, `time_tag_extracted`, `no_time_returns_none`, `future_meta_falls_back_to_wayback`, `meta_far_from_rss_falls_back_to_wayback`, **`no_time_anywhere_is_excluded_and_logged`**, `window_boundaries` (start − 900 in, start − 901 out, investigation_ts in, +1 out), `paywalled_page_kept_with_snippet_flagged`, `cap_40_keeps_newest`.
- [ ] **Step 2–4:** fail → implement → 10 passed.
- [ ] **Step 5: Commit** `feat: article fetch, metadata publish time, wayback backup, window filter`.

### Task 12: Haiku per-article verdict + quote check

**Files:** Create `src/verdict_haiku.rs`. Test: `tests/verdict_haiku.rs`.

**Interfaces:**
- Produces: `judge(&Llm, &Connection, candidate_id, ctx: &MarketContext, art: &KeptArticle) -> Result<HaikuVerdict>`; schema/struct `HaikuVerdict { relevant: bool, points_to: "yes"|"no"|"neither", strength: 0..=3, explains_move: bool, quote: String, reason: String }`; article text sent ≤ 12,000 chars; one call per article; failure after retry stored as `haiku_error`.
- Produces: pure `quote_found(quote: &str, text: &str) -> bool` (lowercase, collapse whitespace, normalise curly quotes/dashes; empty quote → false when `relevant`).
- Produces: pure `classify(verdicts: &[(HaikuVerdict, bool /*quote ok*/)], direction: Side) -> Branch` where `Branch::Explained` if any verdict has `relevant && explains_move && points_to == direction && strength ≥ 2`, else `Branch::Unexplained`.

- [ ] **Step 1: Failing tests** `quote_found_normalises_whitespace_and_curly_quotes`, `quote_missing_detected`, `classify_explained_needs_strength_2_same_direction`, `classify_opposite_direction_is_unexplained`, `haiku_error_stored_after_bad_json_twice`.
- [ ] **Step 2–4:** fail → implement → 5 passed.
- [ ] **Step 5: Commit** `feat: per-article haiku verdict, quote check, branch classification`.

### Task 13: Sonnet review + investigation state machine

**Files:** Create `src/review.rs`, `src/investigate.rs`. Test: `tests/investigate.rs` (wiremock for every external service, fixtures from Tasks 3/10/11).

**Interfaces:**
- Produces: `review::review(&Llm, &Connection, candidate_id, input: &ReviewInput) -> Result<SonnetReview>`; `ReviewInput { markets (all joined), minute bars of the move, all Haiku verdicts, full text of relevant articles only, quote-check results, article timing details, search coverage (rss_ok, rss_failed, fallback_used) }`; `SonnetReview { haiku_errors: Vec<String>, hallucination_found: bool, explained_by_public_info: bool, explaining_articles: Vec<String>, decision: "buy"|"no_buy", side: "yes"|"no", ticker: Option<String>, reason: String }`.
- Produces: pure `final_decision(branch: Branch, r: &SonnetReview, move_dir: Side) -> Decision { buy: bool, ticker: Option<String>, reason: &'static str }`: buy only if `branch == Unexplained && r.decision == "buy" && !r.hallucination_found && !r.explained_by_public_info && r.side == move_dir && r.ticker` is one of the candidate's markets. Reasons: `explained_path|sonnet_no_buy|hallucination|sonnet_says_explained|side_mismatch|bad_ticker|ok`. On the Explained path, a Sonnet "buy" is logged as `sonnet_disagrees` and is not a buy.
- Produces: `investigate::run_one(deps, candidate_id, now_fn) -> Result<FinalStatus>`: queued → investigating → (phrases → search → collect → judge each → classify → review → decision) → explained | no_buy | bought (calls `paper::open`, Task 14) | stale | failed | cap_skipped. Checks before review and before buying: elapsed > 20 min → stale; current mid has gone back past `from_c` (against the direction) → stale. `CapHit` → `cap_skipped`. Any other error → retry the candidate up to 3 times with backoff 1/4/16 min, then `failed`. Every transition writes an `events` row.
- Produces: `investigate::recover_on_boot(&Connection, &EventLog)` marks `investigating` → `failed:restart`.

- [ ] **Step 1: Failing tests** `full_path_unexplained_buys` (all mocks: no relevant article, Sonnet buy → status bought, one paper trade), `explained_path_no_buy_even_if_sonnet_disagrees`, `hallucination_blocks_buy`, `side_mismatch_blocks_buy`, `stale_after_20_minutes` (fake clock), `stale_when_price_reverses_past_from`, `cap_hit_marks_cap_skipped`, **`restart_marks_investigating_failed`**, `trail_reconstructable` (after a full run, a single query joining candidates→phrases→searches→articles→llm_calls→sonnet_reviews→paper_trades returns rows at every stage).
- [ ] **Step 2–4:** fail → implement → 9 passed.
- [ ] **Step 5: Commit** `feat: sonnet review and per-candidate investigation state machine`.

### Task 14: Paper ledger and settlement

**Files:** Create `src/paper.rs`. Test: `tests/paper.rs`.

**Interfaces:**
- Consumes: `kalshi::get_orderbook`, `get_market`, `taker_fee_cents`.
- Produces: pure `walk_book(book: &OrderBook, side: Side, target: i64, max_price_c: u32) -> Fill { contracts: i64, cost_c: i64, avg_price_c: f64, levels: Vec<(u32,i64)> }`. Buying YES consumes NO bids as YES asks at price `100 − no_bid`, cheapest first; buying NO consumes YES bids at `100 − yes_bid`. Stops at `target` or when the next price > `max_price_c`.
- Produces: `open(&Http, &Connection, &EventLog, candidate_id, ticker, side, now) -> Result<Option<i64 /*trade id*/>>`: refuses (logged reason) when the event already has an open paper trade, market closes in < 10 min, or the book fetch failed; fill 0 → no trade (`no_liquidity`); fee = `taker_fee_cents(contracts, round(avg_price_c))`; stores book snapshot path, fill levels, cost, fee.
- Produces: `settle_due(&Http, &Connection, &EventLog, now) -> u32`: for open trades whose market `close_ts < now`, fetch the market; when `result` is `yes`/`no`, write `settlements` with `payout_c = contracts·100` if result == side else 0, `pnl_c = payout_c − cost_c − fee_c`; otherwise leave open (retry next pass).

- [ ] **Step 1: Failing tests** `walk_fills_25_across_levels`, `walk_stops_at_92c`, `walk_partial_when_thin` (only 8 offered ≤ 92 → 8), `walk_none_when_all_above_92`, `buy_no_uses_yes_bids`, `second_trade_same_event_refused`, `settle_win_and_loss_pnl`, `unsettled_market_left_open`.
- [ ] **Step 2–4:** fail → implement → 8 passed.
- [ ] **Step 5: Commit** `feat: paper ledger with order-book walk, per-event limit, settlement`.

### Task 15: Run lifecycle, verdict, status file, CLI, main loop

**Files:** Create `src/run.rs`, `src/status.rs`; finish `src/main.rs`. Test: `tests/run.rs`.

**Interfaces:**
- Produces CLI (clap): `run` (service loop), `start --confirm` (stores `run_start_ts`, `run_end_ts = start + 30·86400` in `kv`; refuses if already started), `pause` / `resume` (kv flag + `$PIT_DATA/PAUSE` file), `status` (prints status JSON), `report <candidate_id>` (prints the full trail), `verdict`, `universe --dry-run`, `llm-smoke`, `news-smoke <q>`.
- Produces: `run::phase(&Connection, now) -> Phase { NotStarted, Active{day}, SettlementOnly, Finished }` (Finished when after end and no open trades).
- Produces: pure `run::verdict(settled: &[SettledTrade]) -> Verdict { label: "KEEP"|"SHUT OFF"|"NOT ENOUGH TRADES", n, events, pnl_c, mean_c_per_contract, ci_lo, ci_hi, provisional: bool }` with a 95 % event-cluster bootstrap (2,000 resamples, fixed seed 7) on per-contract P&L; label from total `pnl_c > 0` and `n ≥ 30`.
- Main loop (`run`): `recover_on_boot`; every 60 s at the minute boundary: watcher tick → score every active market → `on_score` → `expire_old`; universe refresh at 06:00 UTC; investigator task pulls `next_queued` when phase is Active and not paused; settlement every 10 min; prune daily; `status::write` every 60 s. Watching and scoring run even when NotStarted (warm-up for the 24 h history); candidates are only created in Active.
- Produces: `status::build(&Connection, now) -> serde_json::Value` with keys: `phase, day, run_start, run_end, paused, cap_hit, lights{watcher,scorer,rss,newsapi,haiku,sonnet,settlement}` (ok/warn/down from last-success ages and error counts in the last hour), `today{candidates, explained, unexplained, bought, no_buy, stale, failed, cap_skipped, dedup_suppressed}`, `trades{open, settled, pnl_c}`, `verdict{...}`, `spend{today_usd, run_usd, day_cap, run_cap}`, `newsapi{tokens_remaining, fallbacks_today, tokens_today}`, `funnel{math_pit, to_sonnet, paper_buys}` (run totals: candidates created in Active phase; candidates whose `sonnet_reviews` row is on the Unexplained branch; paper trades opened), `series{days[]}` where each day is `{date, math_pit, to_sonnet, paper_buys, pnl_cum_c, open_mtm_c, win_rate_cum, pnl_per_contract_cum_c, ci_lo_c, ci_hi_c, spend_cum_usd, net_after_costs_cum_c}`, `score_hist[]`, `current_task`, `updated_ts`. Written atomically (tmp + rename).

- [ ] **Step 1: Failing tests** `funnel_counts` (seed 5 candidates: 2 explained, 3 unexplained reviewed by Sonnet, 1 bought → `{math_pit:5, to_sonnet:3, paper_buys:1}`), `series_net_after_costs` (pnl_cum 500c, spend $2.00 → net 300c), `phase_transitions`, `verdict_not_enough_under_30`, `verdict_keep_positive_with_30`, `verdict_shut_off_negative`, `verdict_bootstrap_deterministic`, `status_has_all_keys` (snapshot of key set), `pause_file_stops_new_investigations`.
- [ ] **Step 2–4:** fail → implement → 7 passed.
- [ ] **Step 5: Full suite** `cargo test` → all pass; `cargo clippy -- -D warnings` clean.
- [ ] **Step 6: Local soak (ermac or mini-mac, no key needed):** `PIT_DATA=$TMP/pit cargo run --release -- run` for 10 min; confirm snapshots grow each minute, `status.json` updates, no errors in the JSONL log.
- [ ] **Step 7: Commit** `feat: run lifecycle, verdict, status.json, CLI and main loop`; write `README.md` runbook (commands, data layout, pause, report, deploy).

### Task 16: Deploy to mini-mac

**Files:** Create `deploy/informed-pit.service`, `deploy/install.sh`.

- [ ] **Step 1: Install Rust on mini-mac (user account, no sudo):** `ssh emac@mini-mac 'curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal'`; check `~/.cargo/bin/cargo --version`. If the build needs system packages that are missing (e.g. `pkg-config`), they are not needed with `rustls` + `rusqlite/bundled`; if a C compiler is missing, report it (sudo is passwordless on mini-mac per FLEET.md: `sudo apt-get install -y build-essential`).
- [ ] **Step 2: Bare repo + checkout:** on mini-mac `git init --bare ~/Informed-PIT.git && git clone ~/Informed-PIT.git ~/Informed-PIT`; from ermac `git push ssh://emac@mini-mac/home/emac/Informed-PIT.git main:refs/heads/deploy`; on mini-mac `cd ~/Informed-PIT && git checkout deploy && ~/.cargo/bin/cargo build --release`.
- [ ] **Step 3: Unit + env:** `deploy/informed-pit.service` (user unit: `ExecStart=%h/Informed-PIT/target/release/informed-pit run`, `EnvironmentFile=%h/.config/informed-pit/env`, `Environment=PIT_DATA=%h/informed-pit-data`, `Restart=on-failure`, `RestartSec=30`); `install.sh` copies it, creates the env file with mode 600 if absent (placeholders only), `systemctl --user daemon-reload && systemctl --user enable --now informed-pit`. Copy `ANTHROPIC_API_KEY` from the server's existing Informed Favorites env (read in-process into the new file, never printed).
- [ ] **Step 4: NewsAPI.ai key (owner):** the owner registers the free account; hand the key over with the Windows secret handoff (claudmin `topics/windows-secret-handoff.md`) into mini-mac's env file. Until then the bot runs with fallback disabled and logs `newsapi_no_key` when it would have used it.
- [ ] **Step 5: Verify live:** `informed-pit llm-smoke` and `news-smoke` on mini-mac; `systemctl --user status informed-pit`; after 5 min `informed-pit status` shows phase NotStarted, watcher light ok, snapshots ≈ markets × minutes.
- [ ] **Step 6: Commit** `chore: systemd unit and mini-mac install script`; update claudmin `FLEET.md` with an `Informed-PIT` row (path, deploy pattern, data dir, slot 17) via a claudmin PR.

### Task 17: Wall tile 17

- [ ] **Step 1:** Ensure the ermac clone `~/Grafana-Dashboard-Skill-Creator` fast-forwards (`git -C ... pull --ff-only`; if not fast-forwardable, stop and report the divergence to the owner instead of resetting).
- [ ] **Step 2:** Invoke `/grafana-wall` for project `informed-pit`, slot 17, status read with one read-only `ssh emac@mini-mac cat ~/informed-pit-data/status.json`. Top bar: phase + day N/30, lights from `lights`, current task, and the three owner funnel chips from `funnel`: **Math PIT**, **To Sonnet**, **Paper buys**. KPI charts over time from `series.days` (spec §12): (1) cumulative P&L after fees (`pnl_cum_c`, `open_mtm_c`) with chips `P&L`, verdict; (2) daily funnel (`math_pit`, `to_sonnet`, `paper_buys`); (3) win rate + P&L per contract with CI band (`win_rate_cum`, `pnl_per_contract_cum_c`, `ci_lo_c`, `ci_hi_c`) with chip `settled n`; (4) net after API costs (`net_after_costs_cum_c`) with chips `spend $/60`. NewsAPI chip text: `NewsAPI free <tokens_remaining> left · <fallbacks_today> fallbacks today` (keep chip values ≤ 7 chars where the template requires; e.g. value `1940/3`).
- [ ] **Step 3:** Confirm the tile renders on the wall (screenshot via the wall Grafana at `http://mini-mac:3000`), then release nothing else.

### Task 18: Start the 30-day run

- [ ] **Step 1:** Wait until the service has ≥ 24 h of minute history for the universe (warm-up; scores otherwise abstain). Check `informed-pit status` → scorer light ok and `score_hist` non-empty.
- [ ] **Step 2:** Show the owner the first day of `scores` ≥ 40 (count and top 10) so they can confirm the 80 threshold before starting.
- [ ] **Step 3:** With the owner's go, `informed-pit start --confirm`; record run start/end in the repo README and in a claudmin memory note.
