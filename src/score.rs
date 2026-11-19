//! Deterministic surprise score (spec section 7): a 0-100 number per market per minute built from four
//! 25-point parts (move size, volume burst, one-sided taker flow, quiet before). Pure maths over
//! [`MinuteBar`]s; [`load_bars`] is the only function that touches the database.

use std::collections::HashMap;

use rusqlite::{params, Connection};

use crate::kalshi::Side;

/// Candidate move windows, in minutes, all ending at `now_ts`.
pub const WINDOWS_MIN: [u32; 7] = [5, 10, 15, 20, 30, 45, 60];
/// Fewer bars than this in the trailing 24 h and the market abstains.
pub const MIN_BARS_24H: usize = 1200;
/// Fewer taker contracts than this in the chosen window and the market abstains.
pub const MIN_TRADES: i64 = 5;

const DAY_S: i64 = 86_400;
const QUIET_S: i64 = 3_600;
const SIGMA_FLOOR: f64 = 1e-4;
/// A window whose price moved less than this many cents earns no move points (it can still be chosen).
const MIN_MOVE_C: u32 = 5;
/// A boundary price (window start, window end) may come from a bar at most this many seconds before it.
const EDGE_TOL_S: i64 = 120;
/// `load_bars` reaches back 24 h (baseline) + 120 min (longest window 60 + quiet period 60).
const LOAD_SPAN_S: i64 = DAY_S + 120 * 60;

/// One minute of one market. `volume` and the taker counts are per-minute deltas, not cumulative.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinuteBar {
    pub ts: i64,
    pub mid_c: u32,
    pub volume: i64,
    pub taker_yes: i64,
    pub taker_no: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Score {
    pub total: f64,
    pub move_pts: f64,
    pub volume_pts: f64,
    pub onesided_pts: f64,
    pub quiet_pts: f64,
    pub window_min: u32,
    /// Start-price anchor of the winning window: `now - window_min * 60`.
    pub move_start_ts: i64,
    pub direction: Side,
    pub from_c: u32,
    pub to_c: u32,
    pub move_z: f64,
    pub vol_ratio: f64,
    pub onesided: f64,
    pub quiet_ratio: f64,
    /// Taker contracts (yes + no) in the chosen window.
    pub window_trades: i64,
}

/// Log-odds of a price in cents (probability clamped to [0.01, 0.99]).
fn lo(c: u32) -> f64 {
    let p = (f64::from(c) / 100.0).clamp(0.01, 0.99);
    (p / (1.0 - p)).ln()
}

/// 0 at `a`, 25 at `b`, linear between, clamped.
fn lin(x: f64, a: f64, b: f64) -> f64 {
    25.0 * ((x - a) / (b - a)).clamp(0.0, 1.0)
}

/// The quiet-before part: 25 at ratio <= 0.3, falling linearly to 0 at ratio >= 1.
fn quiet_points(ratio: f64) -> f64 {
    25.0 - lin(ratio, 0.3, 1.0)
}

/// One cent of price in log-odds at `c`: the smallest swing a market can show, used to floor sigma so a
/// perfectly flat 24 h does not turn a single tick into an enormous z.
fn tick(c: u32) -> f64 {
    let c = c.clamp(1, 99);
    if c < 99 {
        (lo(c + 1) - lo(c)).abs()
    } else {
        (lo(c) - lo(c - 1)).abs()
    }
}

/// The latest bar with `ts <= at`, if it is no more than [`EDGE_TOL_S`] older than `at`.
fn bar_at_or_before(history: &[MinuteBar], at: i64) -> Option<&MinuteBar> {
    let idx = history.partition_point(|b| b.ts <= at);
    let bar = history.get(idx.checked_sub(1)?)?;
    (at - bar.ts <= EDGE_TOL_S).then_some(bar)
}

/// Bars with `lo_ts <= ts <= hi_ts` (history is sorted ascending).
fn between(history: &[MinuteBar], lo_ts: i64, hi_ts: i64) -> &[MinuteBar] {
    let a = history.partition_point(|b| b.ts < lo_ts);
    let z = history.partition_point(|b| b.ts <= hi_ts).max(a);
    &history[a..z]
}

/// Population std dev of the 1-minute log-odds changes between adjacent-minute bars (gaps are skipped, not
/// bridged). `None` with fewer than two changes.
fn delta_std(bars: &[MinuteBar]) -> Option<f64> {
    let (mut n, mut mean, mut m2) = (0.0_f64, 0.0_f64, 0.0_f64);
    for pair in bars.windows(2) {
        if pair[1].ts - pair[0].ts != 60 {
            continue;
        }
        let d = lo(pair[1].mid_c) - lo(pair[0].mid_c);
        n += 1.0;
        let delta = d - mean;
        mean += delta / n;
        m2 += delta * (d - mean);
    }
    (n >= 2.0).then(|| (m2 / n).sqrt())
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let mid = v.len() / 2;
    if v.len() % 2 == 1 {
        v[mid]
    } else {
        (v[mid - 1] + v[mid]) / 2.0
    }
}

struct Best {
    w: u32,
    start_ts: i64,
    sigma: f64,
    z: f64,
    from_c: u32,
    dlo: f64,
}

/// Score one market at `now_ts` (the end of the current minute window; the newest bar is at or just before
/// it). `history` must be sorted by `ts` ascending and cover the last 24 h plus the longest window and the
/// quiet hour. Returns `None` (abstain) when the trailing 24 h `[now-24h, now)` holds fewer than
/// [`MIN_BARS_24H`] bars, when no recent end price or no window start price exists, or when the chosen
/// window saw fewer than [`MIN_TRADES`] taker contracts.
///
/// The window of `w` minutes is `(now - w, now]`: its start price is the nearest bar at or before `now - w`
/// (within two minutes), its volume and taker counts are the bars with `ts` in that interval. Baselines
/// (24 h sigma, same-length window volumes, quiet hour) use only bars at or before the window start, so the
/// candidate window never contaminates its own baseline; sigma is therefore recomputed per window length.
/// Sigma is floored at one cent of log-odds at the window's start price (and at 1e-4), and a window that
/// moved less than 5 cents earns no move points.
pub fn score_market(history: &[MinuteBar], now_ts: i64) -> Option<Score> {
    if between(history, now_ts - DAY_S, now_ts - 1).len() < MIN_BARS_24H {
        return None;
    }
    let to_c = bar_at_or_before(history, now_ts)?.mid_c;

    // Pick the window with the largest z (ties go to the shorter window).
    let mut best: Option<Best> = None;
    for w in WINDOWS_MIN {
        let start_ts = now_ts - i64::from(w) * 60;
        let Some(start_bar) = bar_at_or_before(history, start_ts) else {
            continue;
        };
        let base = between(history, now_ts - DAY_S, start_ts);
        let sigma = delta_std(base)
            .unwrap_or(0.0)
            .max(SIGMA_FLOOR)
            .max(tick(start_bar.mid_c));
        let dlo = lo(to_c) - lo(start_bar.mid_c);
        let z = dlo.abs() / (sigma * f64::from(w).sqrt());
        if best.as_ref().is_none_or(|b| z > b.z) {
            best = Some(Best {
                w,
                start_ts,
                sigma,
                z,
                from_c: start_bar.mid_c,
                dlo,
            });
        }
    }
    let Best {
        w,
        start_ts,
        sigma,
        z,
        from_c,
        dlo,
    } = best?;
    let direction = if dlo >= 0.0 { Side::Yes } else { Side::No };

    let window = between(history, start_ts + 1, now_ts);
    let (taker_yes, taker_no) = window
        .iter()
        .fold((0, 0), |(y, n), b| (y + b.taker_yes, n + b.taker_no));
    let total_taker = taker_yes + taker_no;
    if total_taker < MIN_TRADES {
        return None;
    }

    // Volume burst: window volume over the median of the same-length windows before it, back to now-24h.
    let w_s = i64::from(w) * 60;
    let window_vol: i64 = window.iter().map(|b| b.volume).sum();
    let chunks = (start_ts - (now_ts - DAY_S)) / w_s;
    let past: Vec<f64> = (1..=chunks)
        .map(|k| {
            let hi = start_ts - (k - 1) * w_s;
            between(history, hi - w_s + 1, hi)
                .iter()
                .map(|b| b.volume as f64)
                .sum()
        })
        .collect();
    let vol_ratio = window_vol as f64 / median(past).max(1.0);

    let (dir_taker, other_taker) = match direction {
        Side::Yes => (taker_yes, taker_no),
        Side::No => (taker_no, taker_yes),
    };
    let onesided = (dir_taker - other_taker) as f64 / total_taker as f64;

    // Quiet before: the hour up to the window start; no credit when that hour is too gappy to measure.
    let quiet_ratio =
        delta_std(between(history, start_ts - QUIET_S, start_ts)).map_or(1.0, |s| s / sigma);

    let move_pts = if to_c.abs_diff(from_c) < MIN_MOVE_C {
        0.0
    } else {
        lin(z, 2.0, 8.0)
    };
    let volume_pts = lin(vol_ratio, 1.0, 10.0);
    let onesided_pts = lin(onesided, 0.3, 0.9);
    let quiet_pts = quiet_points(quiet_ratio);
    Some(Score {
        total: move_pts + volume_pts + onesided_pts + quiet_pts,
        move_pts,
        volume_pts,
        onesided_pts,
        quiet_pts,
        window_min: w,
        move_start_ts: start_ts,
        direction,
        from_c,
        to_c,
        move_z: z,
        vol_ratio,
        onesided,
        quiet_ratio,
        window_trades: total_taker,
    })
}

/// Build the scoring history for `ticker` from the database: one bar per stored snapshot with
/// `now - 24 h - 120 min <= ts <= now_ts`, ascending by `ts`. Snapshots with no mid price are skipped.
/// `volume` is this snapshot's cumulative volume minus the previous snapshot's (0 for the first snapshot in
/// range or when it would be negative); `taker_yes`/`taker_no` sum the trade counts with `ts` in
/// `[bar.ts - 60, bar.ts)` by taker side, the same interval that snapshot's volume delta covers. Snapshot
/// `ts` is a UTC minute start.
pub fn load_bars(conn: &Connection, ticker: &str, now_ts: i64) -> rusqlite::Result<Vec<MinuteBar>> {
    let from_ts = now_ts - LOAD_SPAN_S;

    // (minute start, taker side) -> contracts; bar M reads the bucket starting at M - 60.
    let mut taker: HashMap<(i64, String), i64> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT ts - ts % 60, taker_side, SUM(count) FROM trades
         WHERE ticker = ?1 AND ts >= ?2 AND ts < ?3 AND taker_side IN ('yes', 'no')
         GROUP BY ts - ts % 60, taker_side",
    )?;
    let rows = stmt.query_map(params![ticker, from_ts - 60, now_ts], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, f64>(2)?,
        ))
    })?;
    for row in rows {
        let (minute, side, count) = row?;
        taker.insert((minute, side), count.round() as i64);
    }

    let mut stmt = conn.prepare(
        "SELECT ts, mid_c, volume FROM snapshots
         WHERE ticker = ?1 AND ts >= ?2 AND ts <= ?3 ORDER BY ts",
    )?;
    let rows = stmt.query_map(params![ticker, from_ts, now_ts], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Option<f64>>(1)?,
            r.get::<_, Option<f64>>(2)?,
        ))
    })?;
    let mut bars = Vec::new();
    let mut prev_volume: Option<f64> = None;
    for row in rows {
        let (ts, mid, volume) = row?;
        let delta = match (prev_volume, volume) {
            (Some(p), Some(v)) => (v - p).max(0.0).round() as i64,
            _ => 0,
        };
        if volume.is_some() {
            prev_volume = volume;
        }
        let Some(mid) = mid else { continue };
        let minute = ts - ts.rem_euclid(60) - 60;
        let get = |side: &str| taker.get(&(minute, side.to_string())).copied().unwrap_or(0);
        bars.push(MinuteBar {
            ts,
            mid_c: mid.round().clamp(1.0, 99.0) as u32,
            volume: delta,
            taker_yes: get("yes"),
            taker_no: get("no"),
        });
    }
    Ok(bars)
}

#[cfg(test)]
mod tests {
    use super::{lin, quiet_points};

    fn check(f: impl Fn(f64) -> f64, cases: &[(f64, f64)]) {
        for &(x, want) in cases {
            let got = f(x);
            assert!((got - want).abs() < 1e-9, "x = {x}: got {got}, want {want}");
        }
    }

    #[test]
    fn move_part_is_linear_from_z_2_to_8() {
        check(
            |x| lin(x, 2.0, 8.0),
            &[
                (1.0, 0.0),
                (2.0, 0.0),
                (5.0, 12.5),
                (8.0, 25.0),
                (20.0, 25.0),
            ],
        );
    }

    #[test]
    fn volume_part_is_linear_from_ratio_1_to_10() {
        check(
            |x| lin(x, 1.0, 10.0),
            &[
                (0.5, 0.0),
                (1.0, 0.0),
                (5.5, 12.5),
                (10.0, 25.0),
                (30.0, 25.0),
            ],
        );
    }

    #[test]
    fn onesided_part_is_linear_from_0_3_to_0_9() {
        check(
            |x| lin(x, 0.3, 0.9),
            &[
                (-0.5, 0.0),
                (0.3, 0.0),
                (0.6, 12.5),
                (0.9, 25.0),
                (1.0, 25.0),
            ],
        );
    }

    #[test]
    fn quiet_part_falls_from_25_at_0_3_to_0_at_1() {
        check(
            quiet_points,
            &[
                (0.0, 25.0),
                (0.3, 25.0),
                (0.65, 12.5),
                (1.0, 0.0),
                (2.5, 0.0),
            ],
        );
    }
}
