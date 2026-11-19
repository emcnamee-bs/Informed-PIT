use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use informed_pit::eventlog::EventLog;
use informed_pit::http::Http;
use informed_pit::kalshi::Client;
use informed_pit::llm::{Llm, Model, ANTHROPIC_BASE};
use informed_pit::{config, db, report, run, status, universe};

#[derive(Parser)]
#[command(
    name = "informed-pit",
    version,
    about = "Paper-trading Kalshi bot: surprise-move detector with news check"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// The service: watcher, scorer, investigator, settlement and status.json (systemd runs this).
    Run {
        /// Universe rules file (a relative path not found in the working directory is also looked up next to
        /// the repo the binary was built in).
        #[arg(long, default_value = "config/universe.toml")]
        rules: PathBuf,
    },
    /// Start the 30-day run now (new candidates from now on). Refuses if a run was already started.
    Start {
        /// Required: starting freezes the run window and cannot be undone.
        #[arg(long)]
        confirm: bool,
    },
    /// Kill switch: no new investigations (kv flag and the PAUSE file in the data dir).
    Pause,
    /// Clear the kill switch (kv flag and PAUSE file).
    Resume,
    /// Manual reset of the LLM cap mark (spec section 10): investigations resume today. Spend at or over a
    /// cap still stops them.
    ResetCap,
    /// Print the status JSON (what status.json holds).
    Status,
    /// Print the whole trail of one candidate.
    Report {
        candidate_id: i64,
        /// JSON instead of text.
        #[arg(long)]
        json: bool,
    },
    /// Print the verdict on the settled paper trades (provisional until the last trade settles).
    Verdict,
    /// Build the market universe (the markets the watcher polls).
    Universe {
        /// Only print what would be kept; touch no data dir (uses a throw-away in-memory database).
        #[arg(long)]
        dry_run: bool,
        /// Universe rules file.
        #[arg(long, default_value = "config/universe.toml")]
        rules: PathBuf,
    },
    /// One tiny Haiku and one tiny Sonnet JSON call (needs ANTHROPIC_API_KEY; costs well under one cent).
    LlmSmoke,
    /// One live Bing News RSS search for QUERY (no key needed; never calls NewsAPI.ai). Prints the hit count, how
    /// many links resolve to publisher URLs, and for the first five whether the page's metadata gives a publish
    /// time (fetches those five pages).
    NewsSmoke {
        /// Search text, used as the RSS `q` as given.
        query: String,
    },
}

// Futures that hold a rusqlite connection are not Send, so the service runs on one thread.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Run { rules } => run_cmd(&rules).await,
        Command::Start { confirm } => start_cmd(confirm),
        Command::Pause => pause_cmd(true),
        Command::Resume => pause_cmd(false),
        Command::Status => status_cmd(),
        Command::Report { candidate_id, json } => report_cmd(candidate_id, json),
        Command::Verdict => verdict_cmd(),
        Command::ResetCap => reset_cap_cmd(),
        Command::Universe { dry_run, rules } => universe_cmd(dry_run, &rules).await,
        Command::LlmSmoke => llm_smoke_cmd().await,
        Command::NewsSmoke { query } => news_smoke_cmd(&query).await,
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn open_db(settings: &config::Settings) -> Result<rusqlite::Connection> {
    let path = settings.data_dir.join("pit.db");
    db::open(&path).with_context(|| format!("open {}", path.display()))
}

/// `path` as given when it exists or is absolute, else the same relative path under the source tree the
/// binary sits in (`<repo>/target/release/informed-pit` -> `<repo>/<path>`), so the unit's working directory
/// does not matter.
fn resolve_rules(path: &std::path::Path) -> PathBuf {
    if path.exists() || path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent()?.parent()?.parent().map(|repo| repo.join(path)))
        .filter(|p| p.exists())
        .unwrap_or_else(|| path.to_path_buf())
}

async fn run_cmd(rules_path: &std::path::Path) -> Result<()> {
    let rules_path = resolve_rules(rules_path);
    let rules = universe::UniverseRules::load(&rules_path)
        .with_context(|| format!("load {}", rules_path.display()))?;
    run::serve(config::Settings::from_env(), rules).await
}

fn start_cmd(confirm: bool) -> Result<()> {
    anyhow::ensure!(
        confirm,
        "starting the 30-day run cannot be undone; re-run as `informed-pit start --confirm`"
    );
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    let (start, end) = run::start(&conn, &EventLog::new(&settings.data_dir), now())?;
    println!(
        "run started {} UTC (ts {start}); new candidates until {} UTC (ts {end})",
        chrono::DateTime::from_timestamp(start, 0).unwrap_or_default(),
        chrono::DateTime::from_timestamp(end, 0).unwrap_or_default()
    );
    Ok(())
}

fn pause_cmd(paused: bool) -> Result<()> {
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    let log = EventLog::new(&settings.data_dir);
    run::set_paused(&conn, &log, &settings.data_dir, paused, now())?;
    println!(
        "{}",
        if paused {
            "paused: no new investigations (watching, scoring and settlement continue)"
        } else {
            "resumed"
        }
    );
    Ok(())
}

fn reset_cap_cmd() -> Result<()> {
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    run::reset_cap(&conn, &EventLog::new(&settings.data_dir), now())?;
    let still = run::cap_hit(&conn, &settings, now())?;
    println!(
        "cap mark cleared{}",
        if still {
            "; spend is still at or over a cap, so investigations stay off"
        } else {
            "; investigations resume"
        }
    );
    Ok(())
}

fn status_cmd() -> Result<()> {
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    let value = status::build(&conn, &settings, now())?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn report_cmd(candidate_id: i64, json: bool) -> Result<()> {
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    let trail = report::build(&conn, candidate_id)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&trail)?);
    } else {
        print!("{}", report::text(&trail));
    }
    Ok(())
}

fn verdict_cmd() -> Result<()> {
    let settings = config::Settings::from_env();
    let conn = open_db(&settings)?;
    let v = run::load_verdict(&conn, now())?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}

async fn universe_cmd(dry_run: bool, rules_path: &std::path::Path) -> Result<()> {
    let rules = universe::UniverseRules::load(rules_path)?;
    let now = chrono::Utc::now().timestamp();
    if dry_run {
        let tmp = tempdir_for_dry_run()?;
        let http = Http::new(EventLog::new(&tmp));
        let conn = rusqlite::Connection::open_in_memory()?;
        // Same schema as the real database, so the category cache (kv) works.
        conn.execute_batch("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);")?;
        let plan = universe::plan(&Client::new(&http), &conn, &rules, now).await?;
        print_summary(&plan);
        let _ = std::fs::remove_dir_all(&tmp);
        return Ok(());
    }
    let settings = config::Settings::from_env();
    let conn = db::open(&settings.data_dir.join("pit.db")).context("open pit.db")?;
    let log = EventLog::new(&settings.data_dir);
    let http = Http::new(EventLog::new(&settings.data_dir));
    let id = universe::refresh(&http, &conn, &log, &rules, now).await?;
    println!("universe run {id} recorded");
    Ok(())
}

async fn llm_smoke_cmd() -> Result<()> {
    let settings = config::Settings::from_env();
    anyhow::ensure!(
        settings.anthropic_key.is_some(),
        "ANTHROPIC_API_KEY is not set (put it in ~/.config/informed-pit/env)"
    );
    let conn = db::open(&settings.data_dir.join("pit.db")).context("open pit.db")?;
    let log = EventLog::new(&settings.data_dir);
    let http = Http::new(EventLog::new(&settings.data_dir));
    let llm = Llm::new(&http, &conn, &log, &settings, ANTHROPIC_BASE);
    let schema = serde_json::json!({
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": false
    });
    for model in [Model::Haiku, Model::Sonnet] {
        let (value, meta) = llm
            .json_call(
                model,
                "smoke",
                None,
                "Reply with the JSON object asked for, nothing else.",
                "Put the word pong in the answer field.",
                schema.clone(),
                300,
            )
            .await
            .with_context(|| format!("{} call", model.id()))?;
        println!(
            "{}: served by {}, {} in / {} out tokens, ${:.6}, {} ms -> {}",
            model.id(),
            meta.model_used,
            meta.usage.input_tokens,
            meta.usage.output_tokens,
            meta.cost_usd,
            meta.latency_ms,
            value
        );
    }
    println!(
        "spend today ${:.6}, this run ${:.6}",
        informed_pit::llm::spend_today(&conn),
        informed_pit::llm::spend_run(&conn)
    );
    Ok(())
}

async fn news_smoke_cmd(query: &str) -> Result<()> {
    let tmp = tempdir_for_dry_run()?;
    let http = Http::new(EventLog::new(&tmp));
    let ep = informed_pit::news::NewsEndpoints::default();
    let result = informed_pit::news::rss::search_detailed(&http, &ep, query, 0).await;
    let res = match result {
        Ok(r) => r,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&tmp);
            return Err(anyhow::anyhow!("{e} (url {})", e.url));
        }
    };
    println!(
        "{} hits (http {}, {} ms)",
        res.hits.len(),
        res.http_status,
        res.elapsed_ms
    );
    let now = chrono::Utc::now().timestamp();
    let ages: Vec<i64> = res
        .hits
        .iter()
        .filter_map(|h| h.rss_pub_ts)
        .map(|t| now - t)
        .collect();
    if let (Some(min), Some(max)) = (ages.iter().min(), ages.iter().max()) {
        println!(
            "article age: newest {:.1} h, oldest {:.1} h ({} of {} hits dated)",
            *min as f64 / 3600.0,
            *max as f64 / 3600.0,
            ages.len(),
            res.hits.len()
        );
    }
    let resolved: Vec<(&informed_pit::news::Hit, Option<String>)> = res
        .hits
        .iter()
        .map(|h| (h, informed_pit::news::article::resolve_link(&h.link).ok()))
        .collect();
    let n_resolved = resolved.iter().filter(|(_, u)| u.is_some()).count();
    println!(
        "{n_resolved} of {} links resolve to publisher URLs",
        res.hits.len()
    );
    let mut with_time = 0;
    let mut fetched = 0;
    for (h, url) in resolved.iter().filter(|(_, u)| u.is_some()).take(5) {
        let url = url.as_deref().unwrap_or_default();
        match informed_pit::news::article::fetch(&http, url, 0).await {
            Ok(page) => {
                fetched += 1;
                let meta = informed_pit::news::pubtime::extract_pub_ts(&page.html);
                with_time += usize::from(meta.is_some());
                let delta_h = match (meta, h.rss_pub_ts) {
                    (Some((m, _)), Some(r)) => format!("{:+.2} h", (m - r) as f64 / 3600.0),
                    _ => "n/a".to_string(),
                };
                println!(
                    "  {} | rss pub {:?} | page time {:?} | page minus rss {delta_h} | text {} chars | {url}",
                    h.source,
                    h.rss_pub_ts,
                    meta,
                    page.text.chars().count()
                );
            }
            Err(e) => println!("  {} | fetch failed: {e:#} | {url}", h.source),
        }
    }
    println!("of the first 5 resolved links: {fetched} fetched, {with_time} with a metadata publish time");
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(())
}

fn tempdir_for_dry_run() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("informed-pit-dry-run-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn print_summary(plan: &universe::Plan) {
    let sel = &plan.selection;
    println!(
        "fetched {} open markets, looked up {} series ({} lookup failures)",
        plan.fetched, plan.series_looked_up, plan.lookup_failures
    );
    println!("kept {}", sel.kept.len());
    let mut reasons: Vec<_> = sel.reject_counts().into_iter().collect();
    reasons.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    for (reason, n) in reasons {
        println!("  rejected {n:>6}  {reason}");
    }
    let mut by_category: std::collections::BTreeMap<String, usize> = Default::default();
    let kept: std::collections::HashSet<&str> = sel.kept.iter().map(String::as_str).collect();
    for mc in plan
        .candidates
        .iter()
        .filter(|mc| kept.contains(mc.market.ticker.as_str()))
    {
        *by_category
            .entry(mc.category.clone().unwrap_or_default())
            .or_insert(0) += 1;
    }
    println!("kept by category: {by_category:?}");
    let mut seen: std::collections::BTreeMap<String, usize> = Default::default();
    for mc in &plan.candidates {
        if let Some(c) = &mc.category {
            *seen.entry(c.clone()).or_insert(0) += 1;
        }
    }
    println!("categories seen (markets that reached the category lookup): {seen:?}");
}
