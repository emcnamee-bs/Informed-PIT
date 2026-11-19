//! Settings read from the environment (the systemd unit loads `~/.config/informed-pit/env`).

use std::fmt;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Settings {
    pub data_dir: PathBuf,
    pub anthropic_key: Option<String>,
    pub newsapi_key: Option<String>,
    pub score_threshold: f64,
    pub daily_usd_cap: f64,
    pub run_usd_cap: f64,
    pub max_candidates_per_day: u32,
}

// Debug is manual so the API keys can never end up in a log line.
impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settings")
            .field("data_dir", &self.data_dir)
            .field(
                "anthropic_key",
                &self.anthropic_key.as_ref().map(|_| "<set>"),
            )
            .field("newsapi_key", &self.newsapi_key.as_ref().map(|_| "<set>"))
            .field("score_threshold", &self.score_threshold)
            .field("daily_usd_cap", &self.daily_usd_cap)
            .field("run_usd_cap", &self.run_usd_cap)
            .field("max_candidates_per_day", &self.max_candidates_per_day)
            .finish()
    }
}

impl Settings {
    pub fn from_env() -> Settings {
        Settings::from_lookup(|k| std::env::var(k).ok())
    }

    /// Same as `from_env` with an injectable variable lookup (keeps tests off the process environment).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Settings {
        let non_empty = |k: &str| {
            get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let num = |k: &str, default: f64| {
            non_empty(k)
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(default)
        };
        let data_dir = non_empty("PIT_DATA").map(PathBuf::from).unwrap_or_else(|| {
            let home = non_empty("HOME")
                .or_else(|| non_empty("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join("informed-pit-data")
        });
        Settings {
            data_dir,
            anthropic_key: non_empty("ANTHROPIC_API_KEY"),
            newsapi_key: non_empty("NEWSAPI_AI_KEY"),
            score_threshold: num("PIT_SCORE_THRESHOLD", 80.0),
            daily_usd_cap: num("PIT_DAILY_USD_CAP", 3.0),
            run_usd_cap: num("PIT_RUN_USD_CAP", 60.0),
            max_candidates_per_day: non_empty("PIT_MAX_CANDIDATES_PER_DAY")
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    #[test]
    fn defaults() {
        let s = Settings::from_lookup(lookup(&[("HOME", "/home/x")]));
        assert_eq!(s.data_dir, PathBuf::from("/home/x/informed-pit-data"));
        assert!(s.anthropic_key.is_none() && s.newsapi_key.is_none());
        assert_eq!(s.score_threshold, 80.0);
        assert_eq!(s.daily_usd_cap, 3.0);
        assert_eq!(s.run_usd_cap, 60.0);
        assert_eq!(s.max_candidates_per_day, 30);
    }

    #[test]
    fn overrides_and_bad_values_fall_back() {
        let s = Settings::from_lookup(lookup(&[
            ("PIT_DATA", "/d"),
            ("ANTHROPIC_API_KEY", "sk-secret"),
            ("PIT_SCORE_THRESHOLD", "70.5"),
            ("PIT_DAILY_USD_CAP", "nope"),
            ("PIT_RUN_USD_CAP", "-5"),
            ("PIT_MAX_CANDIDATES_PER_DAY", "12"),
        ]));
        assert_eq!(s.data_dir, PathBuf::from("/d"));
        assert_eq!(s.anthropic_key.as_deref(), Some("sk-secret"));
        assert_eq!(s.score_threshold, 70.5);
        assert_eq!(s.daily_usd_cap, 3.0);
        assert_eq!(s.run_usd_cap, 60.0);
        assert_eq!(s.max_candidates_per_day, 12);
    }

    #[test]
    fn debug_never_shows_keys() {
        let s = Settings::from_lookup(lookup(&[
            ("ANTHROPIC_API_KEY", "sk-secret"),
            ("NEWSAPI_AI_KEY", "news-secret"),
        ]));
        let d = format!("{s:?}");
        assert!(!d.contains("sk-secret") && !d.contains("news-secret"));
    }
}
