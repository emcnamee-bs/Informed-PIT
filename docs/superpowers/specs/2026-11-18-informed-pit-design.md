# Informed-PIT: design

**Date:** 2026-11-18 · **Status:** draft for owner approval · **Repo:** github.com/emcnamee-bs/Informed-PIT (public)

This design replaces everything previously in this repo (the July v0.2 TypeScript proposal and its Phase 1 plan were
removed at the owner's request: outdated, not to influence this build). Owner decisions are marked **(owner)**;
choices the author made and the owner can still overturn are marked **(proposal)**; leftovers for the builder are in §14.

## 1. Intent

Find out, with paper money over 30 days, whether following "potential insider trading" (PIT) on Kalshi makes money.
A PIT is a sharp, one-sided price move (example: a market climbing from 50% to 85% over 30 minutes) that no public
news explains. The bot spots such moves with math, searches the news for an explanation, has Haiku read each article
and Sonnet check Haiku's work, and when no public explanation justifies the move, places a **paper** buy in the
direction of the move. Every step is logged so the owner can analyse failures and successes afterwards.

**Success (owner):** keep the bot running if paper P&L after fees is positive at the end of the 30-day run.
**(proposal)** the verdict needs at least 30 settled paper trades; with fewer it reads "not enough trades" instead of
keep/shut off. The tile also shows the plausible range of the result so a lucky positive is visible as such.

## 2. Goals, non-goals, anti-goals

Goals
- Rust service on mini-mac **(owner)**; one program **(owner, option 1)**.
- Focused set of Kalshi markets, prices checked every minute **(owner, option a)**.
- Surprise score computed by plain Rust code, no LLM **(owner, option 2)**.
- News search: Google News RSS primary; NewsAPI.ai only to cover RSS errors/gaps **(owner)**.
- Article publish time from the page's own metadata; Wayback Machine as backup only **(owner)**.
- Article window: from 15 min before the move began through to the moment of investigation **(owner)**.
- One Haiku call per candidate article, JSON answer **(owner)**; Sonnet verifies and decides **(owner)**.
- Paper trades only **(owner)**. 30-day run **(owner)**.
- Log everything: agent decisions, raw script/query results, errors, anything of later use **(owner)**.
- Grafana wall tile 17 **(owner)**, with a NewsAPI chip.

Non-goals
- No real orders, no Kalshi account key, no exchange writes of any kind.
- No GDELT (dropped in favour of RSS + NewsAPI.ai fallback).
- No retrospective backtest; the 30-day forward paper run is the test.
- No trained/learned scoring model in this build.

Anti-goals (would be a failure even if it "works")
- Spending past a cap, or an agent invoked repeatedly for the same unchanged move.
- A paper trade whose decision trail cannot be reconstructed from the logs.
- Trusting a Haiku quote that is not actually in the article.
- Breaching a news source's rate limit and getting the bot blocked.
- Secrets, raw article text or logs committed to the (public) repo.

## 3. Constraints

- **Host:** mini-mac (Ubuntu, 4 cores, ~10 GB free RAM, ~346 GB free disk). No Rust there yet: install rustup in the
  `emac` user account (no sudo). Runs as a systemd **user** unit like the other bots.
- **Kalshi API:** public market-data endpoints need no key. The Signal Lab on the same host shares Kalshi's ~20 req/s
  budget; this bot stays at **≤ 2 req/s average** (proposal).
- **Anthropic:** models `claude-haiku-5-5` and `claude-sonnet-5-5`. Server already has an `ANTHROPIC_API_KEY`
  (used by Informed Favorites); this bot reads it from its own env file. Haiku 5.5 spends thinking tokens by default:
  send `thinking: {type: "disabled"}` on every short JSON call (verified in an earlier project).
- **NewsAPI.ai (verified 2026-11-18 on newsapi.ai/plans and docs):** free plan = 2,000 tokens, one time, never
  expire; last 30 days only; 1 token per article search over the last 30 days, up to 100 articles per page, each extra
  page = 1 more token; keywords can be OR-combined in one search. Article fields include `dateTimePub` (from the
  article's metadata/RSS) and `dateTime` (when NewsAPI.ai found it). Paid 5K plan: $90/month, 5,000 tokens.
- **Repo is public:** code and docs only. Data, logs, prompts with article text, and env files live on mini-mac only.
- **Guardrails** for autonomous agents (claudmin `topics/agent-guardrails.md`) are mandatory: see §10.

## 4. Approaches considered

1. **One Rust program (chosen, owner).** Single service with internal parts (watcher, scorer, investigator, paper
   ledger) passing work over in-process channels; one SQLite DB + daily log files; writes a status file for the tile.
   One deploy, one place to enforce rate limits and spend caps, one query to follow a candidate end to end.
2. Several Rust services joined by a queue. Better crash isolation, but more units and shared rate limits across
   processes; overkill for a few hundred markets and a handful of candidates a day.
3. Rust watcher/scorer + existing Python Haiku/news code. Less new code, but two languages and two deploys, and not
   the Rust bot asked for.

## 5. Architecture

```
          Kalshi public API                     Google News RSS ──(error)──> NewsAPI.ai (fallback)
                 │                                      │
   ┌─────────────▼──────────────┐       ┌───────────────▼────────────────┐
   │ universe (daily)           │       │ investigator (per candidate)   │
   │ watcher (every 60 s)       │──────>│  1 Haiku: 50 phrases           │
   │ scorer  (every 60 s, math) │ cand. │  2 search RSS / fallback       │
   └────────────────────────────┘       │  3 fetch pages, publish time,  │
                                        │    Wayback backup, window test │
                                        │  4 Haiku per article (JSON)    │
                                        │  5 quote check (script)        │
                                        │  6 Sonnet review + decision    │
                                        └───────────────┬────────────────┘
                                                        │ buy / no buy
                                        ┌───────────────▼────────────────┐
                                        │ paper ledger + settlement      │
                                        └───────────────┬────────────────┘
   SQLite pit.db + logs/YYYY-MM-DD.jsonl + raw/  ◄──────┴──── everything      status.json ──> wall tile 17
```

Tokio async runtime; each part is a task. One process-wide rate limiter per external service and one spend governor
(§10) shared by all tasks.

## 6. Market universe (proposal)

- Daily at 06:00 UTC (and on start), a script pulls open Kalshi markets and keeps those in categories where private
  knowledge plausibly exists: politics/elections, economics/Fed/government data, companies/financial events,
  legal/regulatory, world events, science & tech/awards. Excluded: sports, weather, crypto/index price ranges,
  entertainment box-office style markets with public tracking.
- Filters: closes in more than 1 hour and **at most 14 days (owner)**, so every trade opened during the 30-day run
  settles by about day 44; traded in the last 7 days; a two-sided quote exists. The universe is refreshed daily, so
  markets enter as they come within 14 days of closing.
- Ranked by recent volume; cap **300 markets**. The list, the rule version and every inclusion/exclusion reason are
  logged. The category mapping lives in a config file so the owner can edit it without a rebuild.

## 7. Watcher and surprise score

**Watcher (every 60 s):** `GET /markets?tickers=…` in batches of 100 (3 calls) gives bid, ask, last price, cumulative
volume, open interest. For markets whose volume changed since the last minute, `GET /markets/trades?ticker=…&min_ts=…`
pulls new trades (with `taker_side`). A minute snapshot row per market is stored. Missed minutes are logged as gaps,
not filled.

**Surprise score (0–100, plain Rust, deterministic, recomputed every minute for every market).** Four parts,
25 points each **(proposal for weights and formulas; builder may refine, owner tunes the threshold)**:

| Part | Measure | 0 → 25 points |
|---|---|---|
| Move size | Change in log-odds over the best window ending now (window 5 to 60 min), divided by the market's typical 1-min log-odds swing over the past 24 h × √window | z = 2 → 0, z ≥ 8 → 25, linear |
| Volume burst | Contracts traded in the window ÷ median volume of same-length windows over the past 24 h | ratio 1 → 0, ≥ 10 → 25 |
| One-sided buying | \|taker-yes − taker-no\| ÷ total contracts in the window, counted in the move's direction | 0.3 → 0, ≥ 0.9 → 25 |
| Quiet before | Typical 1-min swing in the 60 min before the window vs the 24 h typical swing | ratio ≥ 1 → 0, ≤ 0.3 → 25 |

- Log-odds (`ln(p/(1−p))`) so a 5-point move near 95% counts more than near 50%.
- A market with less than 24 h of minute history, or fewer than N trades in the window, **abstains** (no score), so new
  thin markets don't create noise.
- **Move start** = first minute of the winning window; **move direction** = up (YES) or down (NO).
- **Threshold** starts strict at **80** (proposal), configurable; every score ≥ 40 is stored with its four parts so
  the threshold can be studied later, and all ≥ threshold events become candidates (subject to §10 dedup).

## 8. Investigation pipeline (per candidate)

All steps write rows keyed by `candidate_id`. Candidates are processed one at a time so rate limits are simple; when
several wait, the **highest score goes first** (priority queue) (author's call).

**Staleness (author's call):** a candidate whose investigation passes 20 minutes, or whose price falls back past the
move start before the decision, ends as `stale` (no trade, fully logged).

1. **Phrases (Haiku).** Input: market title, rules text, subtitle, move summary. Output JSON: exactly 50 phrases of
   2–4 words. Script validates count/word length/dedup; one retry; then proceed with what is valid (logged).
2. **News search: Google News RSS.** `https://news.google.com/rss/search?q=…&hl=en-US&gl=US&ceid=US:en`. Phrases are
   OR-combined into about 10 queries (quoted phrases, URL length kept safe). Spacing **≥ 3 s between RSS requests**,
   exponential backoff on 429/503 (proposal). Using `when:1d` in the query to limit age is a builder check (unverified).
   Each request logs URL, status, latency, item count and the raw body (to `raw/`).
3. **Fallback: NewsAPI.ai** runs for a query only when that RSS query fails: network error, HTTP error, rate limit,
   unparseable XML, or a broken/empty body where items were expected (an empty but valid result is not an error). It
   runs the same phrases as one OR-combined article search over the last 30 days, sorted by date.
   Caps **(proposal)**: ≤ 10 tokens per candidate, ≤ 100 per day, and stop using it when the account has 200 tokens
   left (reserve for a deliberate later trial). Every fallback logs the triggering RSS error, tokens used, articles
   returned, and later whether any of those articles affected the verdict.
4. **Fetch and date each article.** Dedupe by URL. Google News links are redirect URLs: resolve to the publisher URL
   (exact method is a builder task, unverified). Fetch the page (polite UA, 10 s timeout, size cap), extract text
   (readability-style), and read the publish time from page metadata in this order: `article:published_time`,
   JSON-LD `datePublished`, `<time datetime>`, other common meta tags. RSS `pubDate` is recorded alongside.
   **Wayback (backup only):** query the Wayback CDX API for the earliest capture when metadata is missing,
   unparseable, in the future, or disagrees with RSS `pubDate` by more than 6 h. If used, the earliest capture time is
   an upper bound on publication and is logged as `time_source = wayback`.
   **Window test:** keep the article if `move_start − 15 min ≤ publish_time ≤ investigation_time`. Every article,
   kept or dropped, is logged with its times, time source and the reason.
   Paywalled/unfetchable pages: keep with title + RSS snippet only, flagged `text_unavailable`.
   Cap: at most **40 kept articles** per candidate (newest first) (proposal).
5. **Haiku per article (one call each).** Input: market question + rules, move summary, the article text (truncated
   to a fixed size). Output JSON:
   `{relevant: bool, points_to: "yes"|"no"|"neither", strength: 0-3, explains_move: bool, quote: string, reason: string}`.
   Invalid JSON: one retry, then recorded as `haiku_error` (counted, not silently dropped).
6. **Quote check (script).** Each quote must appear in the article text (whitespace/quote-normalised substring).
   Mismatches are flagged `quote_not_found` and handed to Sonnet.
7. **Branch.**
   - If any article is relevant, published in the window, and Haiku says it **explains the move** (points the same way
     with strength ≥ 2), the candidate is **EXPLAINED**: no buy. Sonnet is still called as an audit. If Sonnet
     disagrees (thinks the move is unexplained) the disagreement is logged but there is still **no buy**: Sonnet can
     confirm a buy on the UNEXPLAINED path, never create one from the EXPLAINED path (author's call).
   - Otherwise the candidate is **UNEXPLAINED (PIT)** and goes to Sonnet.
8. **Sonnet review and decision.** Input: market, full move (prices/volume per minute), every Haiku verdict (all
   articles), the full text of only the articles Haiku marked relevant (keeps cost bounded; author's call),
   quote-check results, window/time-source details, search coverage (how many queries failed, fallback
   used). Sonnet checks for Haiku hallucinations (quotes, claims not in the text), checks Haiku's relevance/direction
   calls, and judges whether any public information justifies the size of the move in the time it took.
   Output JSON: `{haiku_errors: [...], hallucination_found: bool, explained_by_public_info: bool,
   explaining_articles: [...], decision: "buy"|"no_buy", side: "yes"|"no", reason: string}`.
   **Decision rule (owner):** buy only if Sonnet agrees no public explanation justifies the move. If Sonnet finds
   Haiku hallucinated, do not buy **(owner)**. Proposal: Sonnet's `side` must equal the move direction or it's a
   no-buy (logged as a disagreement).

## 9. Paper trading (proposal unless marked)

- Paper only **(owner)**. Target **25 contracts** per trade. **Fill model (author's call):** fetch the order book at
  decision time and walk it on the chosen side, filling only the contracts actually offered at ≤ **92¢**; if fewer
  than 25 are offered, the paper trade is that smaller size; if none, no trade. Entry cost = the walked prices plus
  Kalshi's taker fee (current schedule, verified at build time). The book snapshot is stored with the trade.
- No buy if the book is unavailable or the market closes in under 10 min. Skipped buys are logged with the reason.
- **One open paper position per Kalshi event** (author's call): linked markets in one event (e.g. brackets of one Fed
  decision) move on the same news, so counting them separately would inflate the evidence.
- Held to settlement; settlement read from Kalshi's market `result` when it settles. Settled P&L = payout − cost − fee.
- **Timeline:** `start --confirm` freezes start/end. New candidates stop at day 30. Because the universe only holds
  markets closing within 14 days, every trade settles by about day 44; the service keeps running in
  settlement-only mode until then (watcher/scorer/investigator off, settlement + tile on). The **final verdict**
  (keep if settled paper P&L after fees > 0 with ≥ 30 settled trades) is read when the last trade settles; the tile
  shows a provisional verdict before that. Statistics cluster by event.

## 10. Guardrails (mandatory)

- **One investigation per move, per event (author's call).** Candidate identity = Kalshi event + direction + move
  start (rounded). Markets in the same event that cross the threshold within the same 30 minutes join the existing
  candidate (one search, one set of Haiku calls; Sonnet picks which market, if any, to buy). The same event cannot
  create a new candidate for 6 h unless a market moves at least another 10 points in the same direction beyond the
  last candidate's price; repeat scores are logged as `dedup_suppressed`, never re-investigated.
- **Spend caps (proposal):** Anthropic $3/day and $60 per 30-day run; at most 30 candidates/day; NewsAPI caps in §8.
  When the daily cap is reached, investigation stops for the day; queued candidates expire as `cap_skipped` (logged).
  Every LLM call goes through one governor that reserves estimated cost before the call and records actual tokens and
  cost after. On a cap breach the bot stops investigating (watching and scoring continue), logs it, and turns the tile
  red until the next day or a manual reset.
- **Meter everything:** model, input/output tokens, cost, latency, status for every call, visible on the tile.
- **Failures:** each external call gets bounded retries with exponential backoff; a candidate that fails 3 times is
  marked `failed` (terminal, no automatic retry).
- **Rate limits are global** per service (Kalshi ≤ 2 req/s avg, RSS ≥ 3 s spacing, NewsAPI token caps, Wayback
  ≤ 1 req/s, article fetches ≤ 2 concurrent).
- **Steady-state test:** simulate the same move repeating for days and assert a bounded number of candidates and LLM
  calls.
- **Kill switch:** `informed-pit pause` (and a file flag) stops new investigations immediately.

## 11. Logging and storage

On mini-mac under `~/informed-pit-data/` (proposal):
- `pit.db` (SQLite, WAL, short write transactions, rollback on any error; lessons from the Signal Lab's lock wedges).
  Tables: `universe_runs`, `markets`, `snapshots` (minute), `trades`, `scores` (≥ 40), `candidates`, `phrases`,
  `searches` (every RSS/NewsAPI request), `articles` (URLs, times, time source, window decision + reason),
  `wayback_checks`, `llm_calls` (prompt, response, tokens, cost, latency, status), `haiku_verdicts`,
  `quote_checks`, `sonnet_reviews`, `paper_trades`, `settlements`, `errors`, `events` (state changes, caps, pauses).
- `logs/YYYY-MM-DD.jsonl`: structured log of every event and error (one line each, with `candidate_id` where relevant).
- `raw/YYYY-MM-DD/…`: compressed raw HTTP bodies (RSS XML, NewsAPI JSON, article HTML) referenced from the DB.
- Retention (proposal): snapshots/trades 60 days, everything about candidates kept for the life of the project.
- `informed-pit report <candidate_id>` prints the whole trail of one candidate end to end.

## 12. Wall tile 17

Built with the `/grafana-wall` skill (one fixed template), fed by `status.json` written every minute (read over ssh by
the wall's refresh, like the other tiles). Slot 17 is free (BTC-Predictability released 2026-11-16).
- Top bar: status (RUNNING / PAUSED / CAP HIT / DOWN), day N of 30, health lights (watcher, scorer, RSS, Haiku,
  Sonnet, settlement), current task.
- **Funnel chips (owner), run totals:** (1) **Math PIT**: moves that crossed the surprise-score threshold and became
  candidates; (2) **To Sonnet**: candidates on the UNEXPLAINED path sent to Sonnet for verification (Sonnet's audits
  of EXPLAINED candidates are not counted here); (3) **Paper buys**: Sonnet-confirmed buys that became paper trades.
- Other chips: P&L after fees, verdict (provisional/final), LLM spend run/cap, **NewsAPI chip** e.g.
  "NewsAPI free 1,940 left · 3 fallbacks today".
- **Charts (owner): KPIs over time that show whether the project is succeeding** (author's pick of four, daily points):
  1. Cumulative paper P&L after fees (settled), with the open-position mark-to-mid as a second line.
  2. Daily funnel: Math PIT → To Sonnet → Paper buys (three series).
  3. Settled win rate and P&L per contract, cumulative, with the 95 % event-clustered range as a band.
  4. Net result after API costs: cumulative paper P&L minus cumulative LLM + NewsAPI spend.

## 13. Deploy and run

- Bare repo `~/Informed-PIT.git` on mini-mac + checkout `~/Informed-PIT` (branch `deploy`), same pattern as the
  other bots; `cargo build --release` on mini-mac.
- systemd user units: `informed-pit.service` (the bot, restart on failure), `informed-pit-status.timer` if the
  status file is not written by the service itself.
- Env file `~/.config/informed-pit/env` (mode 600): Anthropic key, NewsAPI.ai key (owner registers the free account),
  paths. Never in the repo.
- CLI: `run`, `start --confirm`, `pause`/`resume`, `status`, `report <id>`, `verdict`, `universe --dry-run`.

## 14. Left to the builder

- Crate choices (tokio, reqwest, rusqlite, quick-xml/rss, a readability crate, serde, tracing).
- How to resolve Google News redirect links to publisher URLs; whether `when:` works in the RSS query.
- Kalshi current fee formula and exact market/trade field names (verify against live API).
- Category mapping for the universe (config file).
- Prompt wording for the three LLM roles; truncation sizes; the score's minimum-trade floor.
- Exact Wayback CDX query and a polite user agent.
