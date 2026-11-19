use informed_pit::candidates::{expire_old, next_queued, on_score, Outcome, DAILY_CAP};
use informed_pit::db;
use informed_pit::eventlog::EventLog;
use informed_pit::kalshi::Side;
use informed_pit::score::Score;
use rusqlite::{params, Connection};
use tempfile::TempDir;

const T0: i64 = 1_800_000_000 - 1_800_000_000 % 86_400 + 3_600; // 01:00 UTC on some day
const THR: f64 = 80.0;
const H: i64 = 3_600;

fn setup() -> (TempDir, Connection, EventLog) {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let log = EventLog::new(dir.path());
    (dir, conn, log)
}

fn score(total: f64, side: Side, from_c: u32, to_c: u32, now: i64) -> Score {
    Score {
        total,
        move_pts: total / 4.0,
        volume_pts: total / 4.0,
        onesided_pts: total / 4.0,
        quiet_pts: total / 4.0,
        window_min: 10,
        move_start_ts: now - 600,
        direction: side,
        from_c,
        to_c,
        move_z: 5.0,
        vol_ratio: 4.0,
        onesided: 0.7,
        quiet_ratio: 0.4,
        window_trades: 12,
    }
}

fn yes(total: f64, to_c: u32, now: i64) -> Score {
    score(total, Side::Yes, 40, to_c, now)
}

fn feed(c: &Connection, l: &EventLog, ticker: &str, event: &str, s: &Score, now: i64) -> Outcome {
    on_score(c, l, ticker, event, s, now, THR, DAILY_CAP).unwrap()
}

fn count(c: &Connection, sql: &str) -> i64 {
    c.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn below_40_ignored() {
    let (_d, c, l) = setup();
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(39.9, 60, T0), T0),
        Outcome::Ignored
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM scores"), 0);
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 0);
}

#[test]
fn between_40_and_threshold_stored_not_candidate() {
    let (_d, c, l) = setup();
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(79.9, 60, T0), T0),
        Outcome::Stored
    );
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(40.0, 60, T0 + 60), T0 + 60),
        Outcome::Stored
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM scores"), 2);
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 0);
    let (parts, n, dir, ts): (String, i64, String, i64) = c
        .query_row(
            "SELECT parts_json, n_trades, direction, ts FROM scores ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&parts).unwrap();
    for k in ["move_pts", "volume_pts", "onesided_pts", "quiet_pts"] {
        assert!(v[k].is_number(), "{k} missing");
    }
    assert_eq!((n, dir.as_str(), ts), (12, "yes", T0));
}

#[test]
fn first_crossing_creates_candidate() {
    let (_d, c, l) = setup();
    let s = yes(85.0, 60, T0);
    let Outcome::NewCandidate(id) = feed(&c, &l, "M1", "EV", &s, T0) else {
        panic!("expected a new candidate");
    };
    let (status, score, best, from, created, ev, dir, start): (String, f64, f64, f64, i64, String, String, i64) = c
        .query_row(
            "SELECT status, score, best_to_c, from_c, created_ts, event_ticker, direction, move_start_ts FROM candidates WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)),
        )
        .unwrap();
    assert_eq!(
        (
            status.as_str(),
            score,
            best,
            from,
            created,
            ev.as_str(),
            dir.as_str()
        ),
        ("queued", 85.0, 60.0, 40.0, T0, "EV", "yes")
    );
    assert_eq!(start, (T0 - 600) - (T0 - 600) % 60);
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidate_markets"), 1);
    // The score that crossed the threshold is stored too.
    assert_eq!(count(&c, "SELECT COUNT(*) FROM scores"), 1);
}

#[test]
fn second_market_same_event_within_30min_joins() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(id) = feed(&c, &l, "M1", "EV", &yes(82.0, 60, T0), T0) else {
        panic!()
    };
    let now = T0 + 20 * 60;
    assert_eq!(
        feed(&c, &l, "M2", "EV", &yes(90.0, 66, now), now),
        Outcome::JoinedCandidate(id)
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 1);
    assert_eq!(
        count(
            &c,
            &format!("SELECT COUNT(*) FROM candidate_markets WHERE candidate_id = {id}")
        ),
        2
    );
    // Joining raises the candidate to the max score and the further price.
    let (score, best): (f64, f64) = c
        .query_row(
            "SELECT score, best_to_c FROM candidates WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((score, best), (90.0, 66.0));
    // A lower-scoring, less far market joins but does not lower either.
    let now = T0 + 25 * 60;
    assert_eq!(
        feed(&c, &l, "M3", "EV", &yes(81.0, 61, now), now),
        Outcome::JoinedCandidate(id)
    );
    let (score, best): (f64, f64) = c
        .query_row(
            "SELECT score, best_to_c FROM candidates WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((score, best), (90.0, 66.0));
}

#[test]
fn join_window_member_repeat_is_suppressed_unless_score_improves() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(id) = feed(&c, &l, "M1", "EV", &yes(82.0, 60, T0), T0) else {
        panic!()
    };
    let now = T0 + 60;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(82.0, 60, now), now),
        Outcome::DedupSuppressed(id)
    );
    let now = T0 + 120;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(95.0, 60, now), now),
        Outcome::JoinedCandidate(id)
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidate_markets"), 1);
    let stored: String = c
        .query_row("SELECT score_json FROM candidate_markets", [], |r| r.get(0))
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(v["total"], 95.0);
}

#[test]
fn opposite_direction_is_a_separate_candidate() {
    let (_d, c, l) = setup();
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0),
        Outcome::NewCandidate(_)
    ));
    let s = score(85.0, Side::No, 60, 40, T0);
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &s, T0),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn repeat_within_6h_suppressed() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(id) = feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0) else {
        panic!()
    };
    // 1 h later, to_c 9 points further: still suppressed (and not a join: older than 30 min).
    let now = T0 + H;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 69, now), now),
        Outcome::DedupSuppressed(id)
    );
    // Another market of the event, 5 h later, price not further: suppressed too.
    let now = T0 + 5 * H;
    assert_eq!(
        feed(&c, &l, "M2", "EV", &yes(88.0, 55, now), now),
        Outcome::DedupSuppressed(id)
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 1);
}

#[test]
fn repeat_10_points_further_creates_new() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(a) = feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0) else {
        panic!()
    };
    let now = T0 + H;
    let Outcome::NewCandidate(b) = feed(&c, &l, "M1", "EV", &yes(85.0, 70, now), now) else {
        panic!("10 points beyond must start a new candidate");
    };
    assert_ne!(a, b);
    // The new candidate is now the reference: 9 further than 70 is suppressed, 10 is new.
    let now = T0 + 2 * H;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 79, now), now),
        Outcome::DedupSuppressed(b)
    );
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 80, now), now),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn no_direction_further_means_lower_price() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(a) = feed(&c, &l, "M1", "EV", &score(85.0, Side::No, 60, 40, T0), T0)
    else {
        panic!()
    };
    let now = T0 + H;
    // Higher price is the wrong way for a No move: suppressed even 20 points away.
    assert_eq!(
        feed(&c, &l, "M1", "EV", &score(85.0, Side::No, 60, 60, now), now),
        Outcome::DedupSuppressed(a)
    );
    assert_eq!(
        feed(&c, &l, "M1", "EV", &score(85.0, Side::No, 60, 31, now), now),
        Outcome::DedupSuppressed(a)
    );
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &score(85.0, Side::No, 60, 30, now), now),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn rule_r13_within_6h_9_suppressed_10_new() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(a) = feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0) else {
        panic!()
    };
    let now = T0 + 3 * H;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 69, now), now),
        Outcome::DedupSuppressed(a)
    );
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 70, now), now),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn rule_r13_after_6h_price_moved_3_points_creates_new_unchanged_does_not() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(a) = feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0) else {
        panic!()
    };
    let now = T0 + 6 * H;
    assert_eq!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 60, now), now),
        Outcome::DedupSuppressed(a),
        "unchanged price after 6 h stays suppressed"
    );
    let now = T0 + 6 * H + 60;
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 63, now), now),
        Outcome::NewCandidate(_)
    ));
    // Moving back down 3 points after another 6 h is a moved price too.
    let now = T0 + 12 * H + 120;
    assert!(matches!(
        feed(&c, &l, "M1", "EV", &yes(85.0, 60, now), now),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn daily_cap_30() {
    let (_d, c, l) = setup();
    for i in 0..30 {
        let ev = format!("EV{i}");
        let now = T0 + i;
        assert!(
            matches!(
                feed(&c, &l, "M", &ev, &yes(85.0, 60, now), now),
                Outcome::NewCandidate(_)
            ),
            "candidate {i}"
        );
    }
    let now = T0 + 100;
    assert_eq!(
        feed(&c, &l, "M", "EV30", &yes(99.0, 60, now), now),
        Outcome::DailyCapSkipped
    );
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 30);
    // A join is not a new candidate, so it is not capped.
    assert!(matches!(
        feed(&c, &l, "M9", "EV0", &yes(85.0, 61, now), now),
        Outcome::JoinedCandidate(_)
    ));
    // Next UTC day the cap resets.
    let next_day = T0 - H + 86_400 + 60;
    assert!(matches!(
        feed(&c, &l, "M", "EV31", &yes(85.0, 60, next_day), next_day),
        Outcome::NewCandidate(_)
    ));
}

#[test]
fn priority_highest_score_first() {
    let (_d, c, l) = setup();
    assert_eq!(next_queued(&c, T0).unwrap(), None);
    let mk = |ev: &str, total: f64, now: i64| {
        let Outcome::NewCandidate(id) = feed(&c, &l, "M", ev, &yes(total, 60, now), now) else {
            panic!()
        };
        id
    };
    let a = mk("A", 82.0, T0);
    let b = mk("B", 91.0, T0 + 10);
    let d = mk("D", 91.0, T0 + 20);
    let e = mk("E", 85.0, T0 + 30);
    assert_eq!(next_queued(&c, T0).unwrap(), Some(b), "highest score");
    c.execute(
        "UPDATE candidates SET status='investigating' WHERE id=?1",
        [b],
    )
    .unwrap();
    assert_eq!(next_queued(&c, T0).unwrap(), Some(d), "tie: oldest first");
    c.execute(
        "UPDATE candidates SET status='investigating' WHERE id=?1",
        [d],
    )
    .unwrap();
    assert_eq!(next_queued(&c, T0).unwrap(), Some(e));
    c.execute(
        "UPDATE candidates SET status='investigating' WHERE id=?1",
        [e],
    )
    .unwrap();
    assert_eq!(next_queued(&c, T0).unwrap(), Some(a));
    // A re-queued candidate waits out its backoff, then takes its place by score again.
    c.execute(
        "UPDATE candidates SET status='queued', retry_after_ts=?2 WHERE id=?1",
        params![b, T0 + 240],
    )
    .unwrap();
    assert_eq!(
        next_queued(&c, T0 + 239).unwrap(),
        Some(a),
        "b is backing off"
    );
    assert_eq!(next_queued(&c, T0 + 240).unwrap(), Some(b), "backoff over");
}

#[test]
fn queued_expires_after_20min() {
    let (_d, c, l) = setup();
    let Outcome::NewCandidate(id) = feed(&c, &l, "M1", "EV", &yes(85.0, 60, T0), T0) else {
        panic!()
    };
    assert_eq!(
        expire_old(&c, &l, T0 + 20 * 60).unwrap(),
        0,
        "exactly 20 min is not older"
    );
    assert_eq!(next_queued(&c, T0).unwrap(), Some(id));
    assert_eq!(expire_old(&c, &l, T0 + 20 * 60 + 1).unwrap(), 1);
    assert_eq!(next_queued(&c, T0).unwrap(), None);
    let (status, reason, upd): (String, String, i64) = c
        .query_row(
            "SELECT status, reason, updated_ts FROM candidates WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (status.as_str(), reason.as_str(), upd),
        ("stale", "queue_timeout", T0 + 20 * 60 + 1)
    );
    // Only queued candidates expire.
    c.execute(
        "INSERT INTO candidates (event_ticker, direction, move_start_ts, score, status, created_ts, updated_ts)
         VALUES ('X', 'yes', 0, 90, 'bought', ?1, ?1)",
        params![T0],
    )
    .unwrap();
    assert_eq!(expire_old(&c, &l, T0 + 10 * H).unwrap(), 0);
    // Exactly one candidate_state event, for the one expiry.
    let payload: String = c
        .query_row(
            "SELECT payload FROM events WHERE kind = 'candidate_state' AND candidate_id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        (&v["from"], &v["to"], &v["reason"]),
        (
            &serde_json::json!("queued"),
            &serde_json::json!("stale"),
            &serde_json::json!("queue_timeout")
        )
    );
    assert_eq!(
        count(
            &c,
            "SELECT COUNT(*) FROM events WHERE kind = 'candidate_state'"
        ),
        1
    );
}

#[test]
fn daily_cap_comes_from_the_setting() {
    let (_d, c, l) = setup();
    for i in 0..2 {
        let now = T0 + i;
        let ev = format!("EV{i}");
        assert!(matches!(
            on_score(&c, &l, "M", &ev, &yes(85.0, 60, now), now, THR, 2).unwrap(),
            Outcome::NewCandidate(_)
        ));
    }
    let now = T0 + 10;
    assert_eq!(
        on_score(&c, &l, "M", "EV2", &yes(85.0, 60, now), now, THR, 2).unwrap(),
        Outcome::DailyCapSkipped
    );
}

#[test]
fn steady_state_same_move_for_3_days_creates_one_candidate() {
    let (_d, c, l) = setup();
    let mut new = 0;
    let mut dedup = 0;
    let mut other = 0;
    // The scorer's move start rolls forward every minute; the repeating score does the same here.
    for i in 0..4_320 {
        let now = T0 + i * 60;
        match feed(&c, &l, "M1", "EV", &yes(85.0, 60, now), now) {
            Outcome::NewCandidate(_) => new += 1,
            Outcome::DedupSuppressed(_) => dedup += 1,
            _ => other += 1,
        }
    }
    assert_eq!((new, dedup, other), (1, 4_319, 0));
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidates"), 1);
    assert_eq!(count(&c, "SELECT COUNT(*) FROM candidate_markets"), 1);
}
