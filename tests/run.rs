//! Run lifecycle, verdict, kill switch and status.json.

use std::collections::BTreeSet;

use std::collections::HashMap;
use std::time::Duration;

use informed_pit::config::Settings;
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::investigate::Deps;
use informed_pit::kalshi::Client;
use informed_pit::news::NewsEndpoints;
use informed_pit::run::{self, Phase, SettledTrade};
use informed_pit::{report, status};
use rusqlite::{params, Connection};
use serde_json::Value;
use tempfile::TempDir;
use wiremock::MockServer;

/// 2026-11-18 14:00:00 UTC: the run starts here in these tests.
const START: i64 = 1_795_010_400;
const DAY: i64 = 86_400;

struct Fx {
    dir: TempDir,
    conn: Connection,
    log: EventLog,
    settings: Settings,
}

fn fx() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let log = EventLog::new(dir.path());
    let data = dir.path().to_string_lossy().into_owned();
    let settings = Settings::from_lookup(|k| (k == "PIT_DATA").then(|| data.clone()));
    Fx {
        dir,
        conn,
        log,
        settings,
    }
}

fn started() -> Fx {
    let f = fx();
    run::start(&f.conn, &f.log, START).unwrap();
    f
}

fn candidate(conn: &Connection, id: i64, event: &str, status: &str, created: i64) {
    conn.execute(
        "INSERT INTO candidates (id, event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts, best_to_c, from_c)
         VALUES (?1, ?2, 'yes', ?3, 90, ?4, ?3, ?3, 60, 40)",
        params![id, event, created, status],
    )
    .unwrap();
}

fn review(conn: &Connection, candidate_id: i64, branch: &str, ts: i64) {
    conn.execute(
        "INSERT INTO sonnet_reviews (candidate_id, ts, decision, branch) VALUES (?1, ?2, 'x', ?3)",
        params![candidate_id, ts, branch],
    )
    .unwrap();
}

/// An open paper trade; returns its id.
fn trade(conn: &Connection, candidate_id: i64, event: &str, contracts: i64, opened: i64) -> i64 {
    conn.execute(
        "INSERT INTO paper_trades (candidate_id, event_ticker, ticker, side, contracts, cost_c, fee_c, avg_price_c,
                                   levels_json, close_ts, opened_ts)
         VALUES (?1, ?2, ?2 || '-M', 'yes', ?3, ?3 * 60, 0, 60, '[]', ?4 + 86400, ?4)",
        params![candidate_id, event, contracts, opened],
    )
    .unwrap();
    conn.last_insert_rowid()
}

fn settle(conn: &Connection, trade_id: i64, pnl_c: i64, ts: i64) {
    settle_as(conn, trade_id, "yes", pnl_c, ts);
}

fn settle_as(conn: &Connection, trade_id: i64, result: &str, pnl_c: i64, ts: i64) {
    conn.execute(
        "INSERT INTO settlements (trade_id, result, payout_c, pnl_c, settled_ts) VALUES (?1, ?2, 0, ?3, ?4)",
        params![trade_id, result, pnl_c, ts],
    )
    .unwrap();
    conn.execute(
        "UPDATE paper_trades SET status = 'settled' WHERE id = ?1",
        [trade_id],
    )
    .unwrap();
}

fn llm_spend(conn: &Connection, usd: f64, ts: i64) {
    conn.execute(
        "INSERT INTO llm_calls (ts, role, model_requested, cost_usd, status) VALUES (?1, 'test', 'claude-haiku-5-5', ?2, 'ok')",
        params![ts, usd],
    )
    .unwrap();
}

fn st(trades: &[(&str, i64, i64)]) -> Vec<SettledTrade> {
    trades
        .iter()
        .map(|(e, contracts, pnl_c)| SettledTrade {
            event_ticker: e.to_string(),
            contracts: *contracts,
            pnl_c: *pnl_c,
            void: false,
        })
        .collect()
}

#[test]
fn funnel_counts() {
    let f = started();
    let c = &f.conn;
    // Before the run: not counted.
    candidate(c, 99, "EV0", "explained", START - DAY);
    review(c, 99, "unexplained", START - DAY + 100);
    // 2 explained (Sonnet audits them: not "to Sonnet"), 3 unexplained reviewed by Sonnet, 1 of them bought.
    candidate(c, 1, "EV1", "explained", START + 100);
    candidate(c, 2, "EV2", "explained", START + 200);
    review(c, 1, "explained", START + 150);
    candidate(c, 3, "EV3", "bought", START + 300);
    candidate(c, 4, "EV4", "no_buy", START + 400);
    candidate(c, 5, "EV5", "no_buy", START + 500);
    for id in [3, 4, 5] {
        review(c, id, "unexplained", START + 100 * id + 50);
    }
    // A retried candidate reviewed twice still counts once.
    review(c, 4, "unexplained", START + 900);
    trade(c, 3, "EV3", 25, START + 360);

    let s = status::build(c, &f.settings, START + 3600).unwrap();
    assert_eq!(
        s["funnel"],
        serde_json::json!({"math_pit": 5, "to_sonnet": 3, "paper_buys": 1})
    );
    let day = &s["series"]["days"][0];
    assert_eq!(
        (&day["math_pit"], &day["to_sonnet"], &day["paper_buys"]),
        (&Value::from(5), &Value::from(3), &Value::from(1))
    );
    assert_eq!(s["today"]["unexplained"], 3);
    assert_eq!(s["today"]["explained"], 2);
    assert_eq!(s["today"]["bought"], 1);
}

#[test]
fn series_net_after_costs() {
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "bought", START + 100);
    let t = trade(c, 1, "EV1", 25, START + 200);
    settle(c, t, 500, START + 3600);
    llm_spend(c, 1.25, START + 50);
    llm_spend(c, 0.75, START + 60);
    llm_spend(c, 9.0, START - 50); // before the run: not run spend

    let s = status::build(c, &f.settings, START + 2 * DAY).unwrap();
    let days = s["series"]["days"].as_array().unwrap();
    assert_eq!(days.len(), 3);
    let last = days.last().unwrap();
    assert_eq!(last["pnl_cum_c"], 500);
    assert!((last["spend_cum_usd"].as_f64().unwrap() - 2.0).abs() < 1e-9);
    assert_eq!(last["net_after_costs_cum_c"], 300);
    assert_eq!(last["win_rate_cum"], 1.0);
    assert!((s["spend"]["run_usd"].as_f64().unwrap() - 2.0).abs() < 1e-9);
}

#[test]
fn open_position_marked_to_mid() {
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "bought", START + 100);
    // 25 yes at 60c (cost 1500c); the market's mid is 70 later that day.
    trade(c, 1, "EV1", 25, START + 200);
    c.execute(
        "INSERT INTO snapshots (ticker, ts, mid_c) VALUES ('EV1-M', ?1, 70)",
        [START + 600],
    )
    .unwrap();
    let s = status::build(c, &f.settings, START + 3600).unwrap();
    assert_eq!(s["series"]["days"][0]["open_mtm_c"], 250);
    assert_eq!(s["trades"]["open"], 1);
}

#[test]
fn phase_transitions() {
    let f = fx();
    let c = &f.conn;
    assert_eq!(run::phase(c, START).unwrap(), Phase::NotStarted);
    run::start(c, &f.log, START).unwrap();
    assert!(
        run::start(c, &f.log, START + 10).is_err(),
        "second start refused"
    );
    assert_eq!(run::run_window(c).unwrap(), Some((START, START + 30 * DAY)));
    assert_eq!(run::phase(c, START - 1).unwrap(), Phase::NotStarted);
    assert_eq!(run::phase(c, START).unwrap(), Phase::Active { day: 1 });
    assert_eq!(
        run::phase(c, START + 2 * DAY + 5).unwrap(),
        Phase::Active { day: 3 }
    );
    assert_eq!(
        run::phase(c, START + 30 * DAY - 1).unwrap(),
        Phase::Active { day: 30 }
    );
    candidate(c, 1, "EV1", "bought", START + 100);
    let t = trade(c, 1, "EV1", 10, START + 200);
    assert_eq!(
        run::phase(c, START + 30 * DAY).unwrap(),
        Phase::SettlementOnly
    );
    settle(c, t, -100, START + 31 * DAY);
    assert_eq!(run::phase(c, START + 31 * DAY).unwrap(), Phase::Finished);
    // The verdict is final only once finished.
    assert!(run::load_verdict(c, START + 29 * DAY).unwrap().provisional);
    assert!(!run::load_verdict(c, START + 32 * DAY).unwrap().provisional);
}

#[test]
fn verdict_not_enough_under_30() {
    let trades: Vec<_> = (0..29).map(|i| (format!("E{i}"), 25, 100)).collect();
    let t: Vec<_> = trades
        .iter()
        .map(|(e, c, p)| (e.as_str(), *c, *p))
        .collect();
    let v = run::verdict(&st(&t));
    assert_eq!(v.label, "NOT ENOUGH TRADES");
    assert_eq!(v.n, 29);
    assert_eq!(v.pnl_c, 2900);
    let empty = run::verdict(&[]);
    assert_eq!(empty.label, "NOT ENOUGH TRADES");
    assert_eq!(
        (empty.mean_c_per_contract, empty.ci_lo, empty.ci_hi),
        (None, None, None)
    );
}

#[test]
fn verdict_keep_positive_with_30() {
    // 30 trades over 10 events: wins of +500 and losses of -300 per 25 contracts, 18 wins.
    let trades: Vec<(String, i64, i64)> = (0..30)
        .map(|i| (format!("E{}", i % 10), 25, if i < 18 { 500 } else { -300 }))
        .collect();
    let t: Vec<_> = trades
        .iter()
        .map(|(e, c, p)| (e.as_str(), *c, *p))
        .collect();
    let v = run::verdict(&st(&t));
    assert_eq!(v.label, "KEEP");
    assert_eq!((v.n, v.events), (30, 10));
    assert_eq!(v.pnl_c, 18 * 500 - 12 * 300);
    let mean = v.mean_c_per_contract.unwrap();
    assert!((mean - 5400.0 / 750.0).abs() < 1e-9);
    let (lo, hi) = (v.ci_lo.unwrap(), v.ci_hi.unwrap());
    assert!(lo < mean && mean < hi, "{lo} < {mean} < {hi}");
}

#[test]
fn verdict_shut_off_negative() {
    let trades: Vec<(String, i64, i64)> = (0..30)
        .map(|i| (format!("E{i}"), 10, if i < 10 { 200 } else { -150 }))
        .collect();
    let t: Vec<_> = trades
        .iter()
        .map(|(e, c, p)| (e.as_str(), *c, *p))
        .collect();
    let v = run::verdict(&st(&t));
    assert_eq!(v.label, "SHUT OFF");
    assert!(v.pnl_c < 0);
    // Exactly zero is not "> 0": shut off too.
    let zero: Vec<(String, i64, i64)> = (0..30)
        .map(|i| (format!("E{i}"), 10, if i % 2 == 0 { 100 } else { -100 }))
        .collect();
    let t: Vec<_> = zero.iter().map(|(e, c, p)| (e.as_str(), *c, *p)).collect();
    assert_eq!(run::verdict(&st(&t)).label, "SHUT OFF");
}

#[test]
fn verdict_bootstrap_deterministic() {
    let trades: Vec<(String, i64, i64)> = (0..40)
        .map(|i| (format!("E{}", i % 13), 5 + i % 7, (i * 37 % 101) - 45))
        .collect();
    let t: Vec<_> = trades
        .iter()
        .map(|(e, c, p)| (e.as_str(), *c, *p))
        .collect();
    let a = run::verdict(&st(&t));
    let b = run::verdict(&st(&t));
    assert_eq!(a, b);
    assert!(a.ci_lo.unwrap() < a.ci_hi.unwrap());
    // Pinned: any change to the RNG, seed, resample count or percentile rule moves these.
    assert!((a.ci_lo.unwrap() - (-0.10299003322259136)).abs() < 1e-12);
    assert!((a.ci_hi.unwrap() - 1.3154761904761905).abs() < 1e-12);
    // One event: every resample is the same cluster, so the range collapses to the mean.
    let one = run::verdict(&st(&[("E", 10, 50), ("E", 15, -20)]));
    assert_eq!(one.events, 1);
    assert_eq!(one.ci_lo, one.mean_c_per_contract);
    assert_eq!(one.ci_hi, one.mean_c_per_contract);
    // Clustering: one event with many trades counts as one draw, so the range is wider than if each trade
    // were its own event.
    let clustered: Vec<(String, i64, i64)> = (0..30)
        .map(|i| (format!("E{}", i / 10), 10, if i < 10 { 300 } else { -50 }))
        .collect();
    let unclustered: Vec<(String, i64, i64)> = clustered
        .iter()
        .enumerate()
        .map(|(i, (_, c, p))| (format!("U{i}"), *c, *p))
        .collect();
    let width = |x: &[(String, i64, i64)]| {
        let t: Vec<_> = x.iter().map(|(e, c, p)| (e.as_str(), *c, *p)).collect();
        let v = run::verdict(&st(&t));
        v.ci_hi.unwrap() - v.ci_lo.unwrap()
    };
    assert!(width(&clustered) > width(&unclustered));
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn status_has_all_keys() {
    let f = started();
    let s = status::build(&f.conn, &f.settings, START + 600).unwrap();
    assert_eq!(
        keys(&s),
        set(&[
            "phase",
            "day",
            "run_start",
            "run_end",
            "paused",
            "cap_hit",
            "lights",
            "today",
            "trades",
            "verdict",
            "spend",
            "newsapi",
            "funnel",
            "series",
            "score_hist",
            "current_task",
            "updated_ts"
        ])
    );
    assert_eq!(
        keys(&s["lights"]),
        set(&[
            "watcher",
            "scorer",
            "rss",
            "newsapi",
            "haiku",
            "sonnet",
            "settlement"
        ])
    );
    assert_eq!(
        keys(&s["today"]),
        set(&[
            "candidates",
            "explained",
            "unexplained",
            "bought",
            "no_buy",
            "stale",
            "failed",
            "cap_skipped",
            "dedup_suppressed"
        ])
    );
    assert_eq!(
        keys(&s["trades"]),
        set(&["open", "settled", "voids", "pnl_c"])
    );
    assert_eq!(
        keys(&s["verdict"]),
        set(&[
            "label",
            "n",
            "voids",
            "events",
            "pnl_c",
            "mean_c_per_contract",
            "ci_lo",
            "ci_hi",
            "provisional"
        ])
    );
    assert_eq!(
        keys(&s["spend"]),
        set(&["today_usd", "run_usd", "day_cap", "run_cap"])
    );
    assert_eq!(
        keys(&s["newsapi"]),
        set(&["tokens_remaining", "fallbacks_today", "tokens_today"])
    );
    assert_eq!(
        keys(&s["funnel"]),
        set(&["math_pit", "to_sonnet", "paper_buys"])
    );
    assert_eq!(keys(&s["series"]), set(&["days"]));
    assert_eq!(
        keys(&s["series"]["days"][0]),
        set(&[
            "date",
            "math_pit",
            "to_sonnet",
            "paper_buys",
            "pnl_cum_c",
            "open_mtm_c",
            "win_rate_cum",
            "pnl_per_contract_cum_c",
            "ci_lo_c",
            "ci_hi_c",
            "spend_cum_usd",
            "net_after_costs_cum_c"
        ])
    );
    assert_eq!(s["phase"], "active");
    assert_eq!(s["day"], 1);
    assert_eq!(s["newsapi"]["tokens_remaining"], 2000);
    assert_eq!(s["updated_ts"], START + 600);
}

#[test]
fn missing_keys_show_on_the_lights_while_active() {
    let f = started();
    let now = START + 600;
    let l = status::build(&f.conn, &f.settings, now).unwrap()["lights"].clone();
    assert_eq!(s3(&l), ("down", "down", "off"), "no keys, run active");

    let data = f.dir.path().to_string_lossy().into_owned();
    let keyed = Settings::from_lookup(|k| match k {
        "PIT_DATA" => Some(data.clone()),
        "ANTHROPIC_API_KEY" => Some("sk-ant-x".into()),
        "NEWSAPI_AI_KEY" => Some("nk".into()),
        _ => None,
    });
    let l = status::build(&f.conn, &keyed, now).unwrap()["lights"].clone();
    assert_eq!(s3(&l), ("ok", "ok", "ok"), "keys set, idle");
}

/// `(haiku, sonnet, newsapi)` lights.
fn s3(l: &Value) -> (&str, &str, &str) {
    (
        l["haiku"].as_str().unwrap(),
        l["sonnet"].as_str().unwrap(),
        l["newsapi"].as_str().unwrap(),
    )
}

#[test]
fn status_lights_and_hist() {
    let f = fx();
    let c = &f.conn;
    let now = START;
    // Watcher beat 30 s ago, scorer 5 minutes ago, settlement never.
    run::kv_set(c, "hb:watcher", &(now - 30).to_string()).unwrap();
    run::kv_set(c, "hb:scorer", &(now - 300).to_string()).unwrap();
    // RSS: one ok and one failure in the last hour -> warn; Sonnet: only failures -> down.
    for ok in [1, 0] {
        c.execute(
            "INSERT INTO searches (ts, source, ok) VALUES (?1, 'rss', ?2)",
            params![now - 60, ok],
        )
        .unwrap();
    }
    c.execute(
        "INSERT INTO llm_calls (ts, role, model_requested, status) VALUES (?1, 'r', 'claude-sonnet-5-5', 'http_error')",
        [now - 60],
    )
    .unwrap();
    for (score, ts) in [
        (45.0, now - 60),
        (47.0, now - 120),
        (99.5, now - 10),
        (85.0, now - 2 * DAY),
    ] {
        c.execute(
            "INSERT INTO scores (ts, ticker, event_ticker, score, parts_json) VALUES (?1, 'T', 'E', ?2, '{}')",
            params![ts, score],
        )
        .unwrap();
    }
    let s = status::build(c, &f.settings, now).unwrap();
    let l = &s["lights"];
    assert_eq!(
        (&l["watcher"], &l["scorer"], &l["settlement"]),
        (
            &Value::from("ok"),
            &Value::from("warn"),
            &Value::from("down")
        )
    );
    // No NewsAPI.ai key: its light is off. No Anthropic key but the run is not active: judged on activity.
    assert_eq!(
        (&l["rss"], &l["newsapi"], &l["haiku"], &l["sonnet"]),
        (
            &Value::from("warn"),
            &Value::from("off"),
            &Value::from("ok"),
            &Value::from("down")
        )
    );
    let hist = s["score_hist"].as_array().unwrap();
    assert_eq!(hist.len(), 6);
    assert_eq!(hist[0], serde_json::json!({"from": 40, "to": 50, "n": 2}));
    assert_eq!(hist[5], serde_json::json!({"from": 90, "to": 100, "n": 1}));
    assert_eq!(s["phase"], "not_started");
    assert_eq!(s["series"]["days"], serde_json::json!([]));
    assert_eq!(s["funnel"]["math_pit"], 0);
}

#[test]
fn pause_file_stops_new_investigations() {
    let f = started();
    let c = &f.conn;
    let now = START + 600;
    candidate(c, 1, "EV1", "queued", START + 300);
    assert_eq!(
        run::next_investigation(c, &f.settings, now).unwrap(),
        Some(1)
    );

    let pause = f.dir.path().join("PAUSE");
    std::fs::write(&pause, "").unwrap();
    assert_eq!(run::next_investigation(c, &f.settings, now).unwrap(), None);
    assert_eq!(status::build(c, &f.settings, now).unwrap()["paused"], true);
    std::fs::remove_file(&pause).unwrap();
    assert_eq!(
        run::next_investigation(c, &f.settings, now).unwrap(),
        Some(1)
    );

    // `pause` sets the kv flag and creates the file; `resume` clears both.
    run::set_paused(c, &f.log, f.dir.path(), true, now).unwrap();
    assert!(pause.exists());
    std::fs::remove_file(&pause).unwrap();
    assert_eq!(
        run::next_investigation(c, &f.settings, now).unwrap(),
        None,
        "kv flag alone pauses"
    );
    run::set_paused(c, &f.log, f.dir.path(), false, now).unwrap();
    assert!(!pause.exists());
    assert_eq!(
        run::next_investigation(c, &f.settings, now).unwrap(),
        Some(1)
    );
}

#[test]
fn no_investigation_outside_active_run() {
    let f = fx();
    let c = &f.conn;
    candidate(c, 1, "EV1", "queued", START);
    assert_eq!(
        run::next_investigation(c, &f.settings, START + 60).unwrap(),
        None
    );
}

#[test]
fn cap_hit_expires_queue_as_cap_skipped() {
    let f = started();
    let c = &f.conn;
    let now = START + 3600;
    candidate(c, 1, "EV1", "queued", now - 60);
    candidate(c, 2, "EV2", "queued", now - 30);
    llm_spend(c, 3.0, now - 100);
    assert!(run::cap_hit(c, &f.settings, now).unwrap());
    assert_eq!(run::next_investigation(c, &f.settings, now).unwrap(), None);
    assert_eq!(run::expire_queued_cap_skipped(c, &f.log, now).unwrap(), 2);
    let n: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM candidates WHERE status = 'cap_skipped' AND reason = 'cap_hit'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 2);
    let events: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'candidate_state'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(events, 2);
    // The next UTC day the daily cap no longer applies.
    assert!(!run::cap_hit(c, &f.settings, now + DAY).unwrap());
}

#[test]
fn status_write_is_atomic_file() {
    let f = started();
    status::write(&f.conn, &f.settings, START + 60).unwrap();
    let body = std::fs::read_to_string(f.dir.path().join("status.json")).unwrap();
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["updated_ts"], START + 60);
    assert!(!f.dir.path().join("status.json.tmp").exists());
}

#[test]
fn report_has_every_section() {
    let f = started();
    let c = &f.conn;
    candidate(c, 7, "EV7", "bought", START + 100);
    review(c, 7, "unexplained", START + 200);
    let t = trade(c, 7, "EV7", 25, START + 300);
    settle(c, t, 1000, START + DAY);
    let trail = report::build(c, 7).unwrap();
    assert_eq!(trail["candidate"]["event_ticker"], "EV7");
    assert_eq!(trail["sonnet_reviews"].as_array().unwrap().len(), 1);
    assert_eq!(trail["settlements"][0]["pnl_c"], 1000);
    let text = report::text(&trail);
    for section in [
        "candidate",
        "members",
        "phrases",
        "searches",
        "articles",
        "haiku_verdicts",
        "quote_checks",
        "sonnet_reviews",
        "paper_trades",
        "settlements",
        "events",
    ] {
        assert!(text.contains(&format!("== {section}")), "{section}");
    }
    assert!(report::build(c, 999).is_err());
}

fn http(dir: &std::path::Path) -> Http {
    Http::with_spacing(EventLog::new(dir), HashMap::new())
        .with_backoff_base(Duration::from_millis(1))
}

fn kv_i64(conn: &Connection, key: &str) -> Option<i64> {
    run::kv_i64(conn, key).unwrap()
}

#[tokio::test]
async fn failed_settlement_pass_turns_the_light_warn_then_down() {
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "bought", START + 100);
    // Closed by START + DAY + 200 (trade close_ts = opened + 1 day).
    trade(c, 1, "EV1", 25, START + 200);
    let now = START + DAY + 600;
    run::kv_set(c, "hb:settlement", &(now - 700).to_string()).unwrap();
    assert_eq!(
        status::build(c, &f.settings, now).unwrap()["lights"]["settlement"],
        "ok"
    );
    let h = http(f.dir.path());
    let broken = MockServer::start().await; // no routes: the market fetch fails
    let pass = run::settlement_pass(&Client::with_base(&h, &broken.uri()), c, &f.log, now).await;
    assert_eq!((pass.settled, pass.failures), (0, 1));
    assert_eq!(
        kv_i64(c, "hb:settlement"),
        Some(now - 700),
        "no heartbeat on failure"
    );
    assert_eq!(
        status::build(c, &f.settings, now).unwrap()["lights"]["settlement"],
        "warn"
    );
    // Still failing more than 10 periods (100 min) after the last good pass: down.
    assert_eq!(
        status::build(c, &f.settings, now + 6000).unwrap()["lights"]["settlement"],
        "down"
    );
    // A pass with nothing due beats again.
    let idle = run::settlement_pass(
        &Client::with_base(&h, &broken.uri()),
        c,
        &f.log,
        START + 300,
    )
    .await;
    assert_eq!(idle.failures, 0);
    assert_eq!(kv_i64(c, "hb:settlement"), Some(START + 300));
}

#[test]
fn paper_buys_bounded_by_run_window() {
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "bought", START + 100);
    trade(c, 1, "EV1", 25, START + 200);
    candidate(c, 2, "EV2", "bought", START + 30 * DAY - 60);
    // Opened after the window closed: not a run buy.
    trade(c, 2, "EV2", 25, START + 30 * DAY + 5);
    let s = status::build(c, &f.settings, START + 31 * DAY).unwrap();
    assert_eq!(s["funnel"]["paper_buys"], 1);
}

#[test]
fn finished_waits_for_investigations() {
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "investigating", START + 30 * DAY - 60);
    assert_eq!(
        run::phase(c, START + 30 * DAY + 10).unwrap(),
        Phase::SettlementOnly
    );
    c.execute("UPDATE candidates SET status = 'no_buy' WHERE id = 1", [])
        .unwrap();
    assert_eq!(
        run::phase(c, START + 30 * DAY + 10).unwrap(),
        Phase::Finished
    );
}

#[test]
fn voids_excluded_from_verdict_and_win_rate() {
    // 30 decided trades (positive) plus 5 voids: n = 30, voids = 5, label from the 30.
    let mut v: Vec<SettledTrade> = (0..30)
        .map(|i| SettledTrade {
            event_ticker: format!("E{i}"),
            contracts: 10,
            pnl_c: if i < 20 { 100 } else { -50 },
            void: false,
        })
        .collect();
    let decided = run::verdict(&v);
    for i in 0..5 {
        v.push(SettledTrade {
            event_ticker: format!("V{i}"),
            contracts: 25,
            pnl_c: 0,
            void: true,
        });
    }
    let with_voids = run::verdict(&v);
    assert_eq!((with_voids.n, with_voids.voids), (30, 5));
    assert_eq!(with_voids.label, "KEEP");
    assert_eq!(with_voids.events, 30);
    assert_eq!(with_voids.mean_c_per_contract, decided.mean_c_per_contract);
    assert_eq!(
        (with_voids.ci_lo, with_voids.ci_hi),
        (decided.ci_lo, decided.ci_hi)
    );
    // 29 decided + 1 void is still not enough trades.
    let few: Vec<SettledTrade> = v.iter().skip(1).take(30).cloned().collect();
    assert_eq!(run::verdict(&few).label, "NOT ENOUGH TRADES");

    // Status: the void is counted apart and is not a loss in the win rate.
    let f = started();
    let c = &f.conn;
    candidate(c, 1, "EV1", "bought", START + 100);
    candidate(c, 2, "EV2", "bought", START + 100);
    let t1 = trade(c, 1, "EV1", 25, START + 200);
    let t2 = trade(c, 2, "EV2", 25, START + 200);
    settle(c, t1, 400, START + 3600);
    settle_as(c, t2, "void", 0, START + 3600);
    let s = status::build(c, &f.settings, START + 7200).unwrap();
    assert_eq!(s["trades"]["voids"], 1);
    assert_eq!(s["verdict"]["voids"], 1);
    assert_eq!(s["verdict"]["n"], 1);
    assert_eq!(s["series"]["days"][0]["win_rate_cum"], 1.0);
}

#[test]
fn reset_cap_lets_investigations_resume() {
    let f = started();
    let c = &f.conn;
    let now = START + 3600;
    candidate(c, 1, "EV1", "queued", now - 60);
    // The governor refused a call today (mark set), spend itself is under the cap.
    run::kv_set(c, "cap_hit_day", &(now / DAY).to_string()).unwrap();
    assert_eq!(status::build(c, &f.settings, now).unwrap()["cap_hit"], true);
    assert_eq!(run::next_investigation(c, &f.settings, now).unwrap(), None);
    run::reset_cap(c, &f.log, now).unwrap();
    assert_eq!(
        status::build(c, &f.settings, now).unwrap()["cap_hit"],
        false
    );
    assert_eq!(
        run::next_investigation(c, &f.settings, now).unwrap(),
        Some(1)
    );
    let ev: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'cap_reset'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ev, 1);
}

#[tokio::test]
async fn expire_old_runs_after_the_run_window() {
    let f = started();
    let c = &f.conn;
    let after = START + 30 * DAY + 3600;
    // An open trade keeps the run in settlement-only; its market closes later, so no settlement request.
    candidate(c, 1, "EV1", "bought", START + 100);
    trade(c, 1, "EV1", 25, after);
    candidate(c, 2, "EV2", "queued", START + 30 * DAY - 120);
    assert_eq!(run::phase(c, after).unwrap(), Phase::SettlementOnly);
    let h = http(f.dir.path());
    let mut last_settle = i64::MAX / 2; // no settlement pass in this step
    run::minute_step(&h, c, &f.log, &f.settings, after, &mut last_settle, &|| {
        after
    })
    .await
    .unwrap();
    let status: String = c
        .query_row("SELECT status FROM candidates WHERE id = 2", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(status, "stale");
}

#[tokio::test]
async fn db_error_in_a_loop_step_is_returned_and_recorded() {
    let f = started();
    let c = &f.conn;
    let h = http(f.dir.path());
    let news = NewsEndpoints::default();
    let deps = Deps {
        http: &h,
        conn: c,
        log: &f.log,
        settings: &f.settings,
        news: &news,
        kalshi_base: "http://127.0.0.1:9",
        anthropic_base: "http://127.0.0.1:9",
    };
    // Simulated database failure: the kv table (run window) is gone.
    c.execute_batch("ALTER TABLE kv RENAME TO kv_gone").unwrap();
    let now_fn = || START + 60;
    let err = run::investigator_step(&deps, &now_fn).await.unwrap_err();
    run::record_loop_error(c, &f.log, "investigator", &err, START + 60);
    let mut last_settle = 0;
    let err2 = run::minute_step(
        &h,
        c,
        &f.log,
        &f.settings,
        START + 60,
        &mut last_settle,
        &|| START + 60,
    )
    .await
    .unwrap_err();
    run::record_loop_error(c, &f.log, "minute_loop", &err2, START + 60);
    let (errors, events): (i64, i64) = c
        .query_row(
            "SELECT (SELECT COUNT(*) FROM errors WHERE source IN ('investigator', 'minute_loop')),
                    (SELECT COUNT(*) FROM events WHERE kind = 'loop_error')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((errors, events), (2, 2));
    // Once the database is back, the next step works again.
    c.execute_batch("ALTER TABLE kv_gone RENAME TO kv").unwrap();
    assert_eq!(
        run::investigator_step(&deps, &now_fn).await.unwrap(),
        run::InvestigatorStep::Idle
    );
}
