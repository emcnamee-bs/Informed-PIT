use informed_pit::db;
use informed_pit::kalshi::Side;
use informed_pit::score::{load_bars, score_market, MinuteBar};
use rusqlite::params;

/// A UTC minute start.
const NOW: i64 = 1_800_000_000; // a multiple of 60

/// Deterministic noise: a fixed-seed LCG, so no rand crate and identical runs.
struct Lcg(u64);

impl Lcg {
    /// Uniform integer in `lo..=hi`.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        lo + ((self.0 >> 33) % (hi - lo + 1) as u64) as i64
    }
}

/// Minute `m` (<= 0) relative to NOW; the ramp, when there is one, starts at m = -30 and ends at m = 0.
fn ts(m: i64) -> i64 {
    NOW + m * 60
}

struct Scenario {
    /// Half-width of the uniform price noise (cents) around 50 before the ramp.
    noise: i64,
    /// Flat price for the 60 minutes before the ramp starts.
    quiet_hour: bool,
    /// Price at NOW after a linear 30-minute ramp from 50.
    ramp_to: i64,
    /// Ramp minutes trade 10x the baseline volume.
    burst: bool,
    /// Percent of ramp taker contracts on the move's side (of 100 per minute).
    taker_pct: i64,
    /// Minutes of history before NOW.
    minutes: i64,
}

impl Scenario {
    fn crawl() -> Self {
        Scenario {
            noise: 1,
            quiet_hour: true,
            ramp_to: 85,
            burst: true,
            taker_pct: 90,
            minutes: 1500,
        }
    }

    fn bars(&self) -> Vec<MinuteBar> {
        let mut rng = Lcg(42);
        let up = self.ramp_to > 50;
        (-self.minutes..=0)
            .map(|m| {
                let ramping = m > -30;
                let mid = if ramping {
                    let frac = (m + 30) as f64 / 30.0;
                    (50.0 + (self.ramp_to - 50) as f64 * frac).round() as i64
                } else if self.quiet_hour && m >= -90 {
                    50
                } else {
                    50 + rng.range(-self.noise, self.noise)
                };
                let base_vol = rng.range(5, 15);
                let (volume, dir, other) = if ramping {
                    (
                        if self.burst { 100 } else { base_vol },
                        self.taker_pct,
                        100 - self.taker_pct,
                    )
                } else {
                    (base_vol, 5, 5)
                };
                let (taker_yes, taker_no) = if up { (dir, other) } else { (other, dir) };
                MinuteBar {
                    ts: ts(m),
                    mid_c: mid as u32,
                    volume,
                    taker_yes,
                    taker_no,
                }
            })
            .collect()
    }
}

#[test]
fn abstains_with_short_history() {
    let s = Scenario {
        minutes: 600,
        ..Scenario::crawl()
    };
    assert_eq!(score_market(&s.bars(), NOW), None);
}

#[test]
fn abstains_with_fewer_than_five_trades_in_window() {
    let mut bars = Scenario::crawl().bars();
    for b in bars.iter_mut().filter(|b| b.ts > ts(-30)) {
        b.taker_yes = 0;
        b.taker_no = 0;
    }
    bars.last_mut().unwrap().taker_yes = 4;
    assert_eq!(score_market(&bars, NOW), None);
    bars.last_mut().unwrap().taker_yes = 5;
    assert!(score_market(&bars, NOW).is_some());
}

#[test]
fn flat_market_scores_below_20() {
    let s = Scenario {
        quiet_hour: false,
        ramp_to: 50,
        burst: false,
        taker_pct: 50,
        ..Scenario::crawl()
    };
    let score = score_market(&s.bars(), NOW).expect("enough history and trades");
    assert!(score.total < 20.0, "flat market scored {score:?}");
}

#[test]
fn crawl_50_to_85_in_30min_on_one_sided_volume_scores_at_least_80() {
    let score = score_market(&Scenario::crawl().bars(), NOW).expect("scored");
    assert!(score.total >= 80.0, "crawl scored {score:?}");
    assert_eq!(score.direction, Side::Yes);
    assert_eq!(score.window_min, 30);
    assert_eq!(score.move_start_ts, ts(-30));
    assert_eq!((score.from_c, score.to_c), (50, 85));
    assert_eq!(score.window_trades, 30 * 100);
    let parts = score.move_pts + score.volume_pts + score.onesided_pts + score.quiet_pts;
    assert!((parts - score.total).abs() < 1e-9);
}

#[test]
fn same_move_two_sided_volume_scores_lower() {
    let one_sided = score_market(&Scenario::crawl().bars(), NOW).unwrap();
    let two_sided = score_market(
        &Scenario {
            taker_pct: 50,
            ..Scenario::crawl()
        }
        .bars(),
        NOW,
    )
    .unwrap();
    assert_eq!(two_sided.onesided_pts, 0.0);
    assert!(two_sided.total < one_sided.total);
    assert!(two_sided.total < 80.0, "{two_sided:?}");
}

#[test]
fn noisy_market_same_move_scores_lower() {
    let noisy = Scenario {
        noise: 8,
        quiet_hour: false,
        ..Scenario::crawl()
    };
    let score = score_market(&noisy.bars(), NOW).unwrap();
    assert!(score.total < 80.0, "noisy market scored {score:?}");
    assert!(score.total < score_market(&Scenario::crawl().bars(), NOW).unwrap().total);
}

#[test]
fn down_move_direction_no() {
    let s = Scenario {
        ramp_to: 15,
        ..Scenario::crawl()
    };
    let score = score_market(&s.bars(), NOW).unwrap();
    assert_eq!(score.direction, Side::No);
    assert_eq!((score.from_c, score.to_c), (50, 15));
    assert!(
        score.onesided > 0.7,
        "taker flow counted in the NO direction: {score:?}"
    );
    assert!(score.total >= 80.0, "{score:?}");
}

#[test]
fn deterministic() {
    let bars = Scenario::crawl().bars();
    assert_eq!(score_market(&bars, NOW), score_market(&bars, NOW));
}

#[test]
fn history_of_1199_bars_abstains_and_1200_scores() {
    let short = Scenario {
        minutes: 1199,
        ..Scenario::crawl()
    };
    assert_eq!(score_market(&short.bars(), NOW), None);
    let enough = Scenario {
        minutes: 1200,
        ..Scenario::crawl()
    };
    assert!(score_market(&enough.bars(), NOW).is_some());
}

#[test]
fn stale_market_single_tick_scores_below_threshold() {
    // 24 h perfectly flat at 50, then one 1-cent tick with 10 contracts, all taker-yes.
    let bars: Vec<MinuteBar> = (-1500..=0)
        .map(|m| MinuteBar {
            ts: ts(m),
            mid_c: if m == 0 { 51 } else { 50 },
            volume: if m == 0 { 10 } else { 0 },
            taker_yes: if m == 0 { 10 } else { 0 },
            taker_no: 0,
        })
        .collect();
    let score = score_market(&bars, NOW).expect("scored");
    assert_eq!(
        score.move_pts, 0.0,
        "a 1-cent move earns no move points: {score:?}"
    );
    assert!(score.total < 80.0, "stale market scored {score:?}");
}

#[test]
fn crawl_still_scores_at_least_80() {
    let score = score_market(&Scenario::crawl().bars(), NOW).unwrap();
    assert!(score.total >= 80.0, "{score:?}");
    assert!(score.move_pts > 0.0);
}

fn lo(c: f64) -> f64 {
    let p = c / 100.0;
    (p / (1.0 - p)).ln()
}

/// A hand-built market where all four parts are known in closed form.
///
/// - Baseline (m = -1440..=-90) alternates 50, 51, so every 1-minute log-odds change is +-d with
///   d = lo(51) - lo(50); sigma24 is therefore d (also the one-cent floor at 50).
/// - The quiet hour (m = -90..=-30) holds 50 except 12 blips to 51 (24 of its 60 changes are +-d), so
///   the quiet ratio is sqrt(24/60).
/// - At m = -29 the price steps to 70 and stays; the 30-minute window (start price 50) wins, z = (lo(70) -
///   lo(50)) / (d * sqrt(30)).
/// - Baseline volume is 10 a minute (every 30-minute chunk = 300); the window trades 55 a minute (1650),
///   ratio 5.5. With `spikes`, five of the 47 baseline chunks carry 3000 instead of 300, so mean != median.
/// - Window taker flow is 80 yes / 20 no a minute: one-sidedness 0.6.
fn parts_fixture(spikes: bool) -> Vec<MinuteBar> {
    (-1500..=0)
        .map(|m| {
            let mid = if m >= -29 {
                70
            } else if (-1440..=-90).contains(&m) {
                if (m + 1440) % 2 == 0 {
                    50
                } else {
                    51
                }
            } else if (-89..=-67).contains(&m) && (m + 89) % 2 == 0 {
                51
            } else {
                50
            };
            let in_window = m >= -29;
            let mut volume = if in_window { 55 } else { 10 };
            if spikes && [-59, -89, -119, -149, -179].contains(&m) {
                volume += 2700;
            }
            let (taker_yes, taker_no) = if in_window { (80, 20) } else { (5, 5) };
            MinuteBar {
                ts: ts(m),
                mid_c: mid,
                volume,
                taker_yes,
                taker_no,
            }
        })
        .collect()
}

fn assert_parts_exact(spikes: bool) {
    let score = score_market(&parts_fixture(spikes), NOW).expect("scored");
    let d = lo(51.0) - lo(50.0);
    let z = (lo(70.0) - lo(50.0)) / (d * 30f64.sqrt());
    let quiet_ratio = (24.0f64 / 60.0).sqrt();
    let near = |got: f64, want: f64, what: &str| {
        assert!(
            (got - want).abs() < 1e-6,
            "{what}: got {got}, want {want} ({score:?})"
        );
    };
    assert_eq!(score.window_min, 30);
    assert_eq!(score.direction, Side::Yes);
    assert_eq!((score.from_c, score.to_c), (50, 70));
    near(score.move_z, z, "move_z");
    near(score.move_pts, 25.0 * (z - 2.0) / 6.0, "move_pts");
    near(score.vol_ratio, 5.5, "vol_ratio (median, not mean)");
    near(score.volume_pts, 12.5, "volume_pts");
    near(score.onesided, 0.6, "onesided");
    near(score.onesided_pts, 12.5, "onesided_pts");
    near(score.quiet_ratio, quiet_ratio, "quiet_ratio");
    near(
        score.quiet_pts,
        25.0 - 25.0 * (quiet_ratio - 0.3) / 0.7,
        "quiet_pts",
    );
    near(
        score.total,
        score.move_pts + 12.5 + 12.5 + score.quiet_pts,
        "total",
    );
    assert_eq!(score.window_trades, 3000);
}

#[test]
fn each_part_matches_its_formula_on_a_hand_built_market() {
    assert_parts_exact(false);
}

#[test]
fn volume_baseline_uses_the_median_not_the_mean() {
    // With the spikes the mean chunk is about 590 (ratio 2.8); the median stays 300 (ratio 5.5).
    assert_parts_exact(true);
}

#[test]
fn load_bars_builds_deltas_and_taker_sums() {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::open(&dir.path().join("pit.db")).unwrap();
    let t0 = NOW - 600;
    // (ts, mid_c, cumulative volume): a NULL mid, a cumulative-volume decrease, one row past `now`.
    let snaps: [(i64, Option<f64>, f64); 6] = [
        (t0, Some(50.0), 100.0),
        (t0 + 60, Some(51.4), 130.0),
        (t0 + 120, Some(49.6), 120.0),
        (t0 + 180, None, 200.0),
        (t0 + 240, Some(52.0), 260.0),
        (NOW + 60, Some(60.0), 999.0),
    ];
    for (ts, mid, vol) in snaps {
        conn.execute(
            "INSERT INTO snapshots (ticker, ts, mid_c, volume) VALUES ('T', ?1, ?2, ?3)",
            params![ts, mid, vol],
        )
        .unwrap();
    }
    // A snapshot older than 24 h + 120 min is out of range; so is another ticker's.
    conn.execute(
        "INSERT INTO snapshots (ticker, ts, mid_c, volume) VALUES ('T', ?1, 10.0, 1.0)",
        params![NOW - 27 * 3600],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO snapshots (ticker, ts, mid_c, volume) VALUES ('OTHER', ?1, 10.0, 1.0)",
        params![t0],
    )
    .unwrap();
    let trades: [(&str, &str, i64, f64, Option<&str>); 7] = [
        // A bar at minute M sums the trades in [M - 60, M): the interval its volume delta covers.
        ("a", "T", t0 + 59, 3.0, Some("yes")), // M-1: counted in bar t0 + 60
        ("b", "T", t0 + 60, 2.0, Some("no")),  // exactly M: counted in bar t0 + 120, not t0 + 60
        ("c", "T", t0 + 119, 4.0, Some("yes")),
        ("d", "T", t0 + 239, 7.0, Some("no")),
        ("e", "T", t0 + 240, 9.0, Some("yes")), // would be bar t0 + 300, which has no snapshot
        ("f", "OTHER", t0 + 59, 50.0, Some("yes")),
        ("g", "T", t0 + 59, 8.0, None), // unknown taker side
    ];
    for (id, ticker, ts, count, side) in trades {
        conn.execute(
            "INSERT INTO trades (trade_id, ticker, ts, yes_price_c, count, taker_side) VALUES (?1, ?2, ?3, 50, ?4, ?5)",
            params![id, ticker, ts, count, side],
        )
        .unwrap();
    }

    let bars = load_bars(&conn, "T", NOW).unwrap();
    let bar = |ts, mid_c, volume, taker_yes, taker_no| MinuteBar {
        ts,
        mid_c,
        volume,
        taker_yes,
        taker_no,
    };
    assert_eq!(
        bars,
        vec![
            bar(t0, 50, 0, 0, 0),       // first in range: no previous
            bar(t0 + 60, 51, 30, 3, 0), // 130 - 100; 51.4 rounds to 51
            bar(t0 + 120, 50, 0, 4, 2), // volume went down: 0
            // t0 + 180 has no mid and is skipped, but its volume is the next delta's baseline
            bar(t0 + 240, 52, 60, 0, 7), // 260 - 200
        ]
    );
}
