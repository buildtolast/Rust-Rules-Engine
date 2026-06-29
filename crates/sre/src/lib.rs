pub mod analysis;
pub mod dashboard;
pub mod docker;
pub mod probes;
pub mod store;
pub mod trace_analysis;

use analysis::{AnalysisClient, Finding, WeakestLinkDecision};
use chrono::{DateTime, Utc};
use clickhouse::Client;
use docker::ContainerInfo;
use probes::{ProbeConfig, SystemProbeResult};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use store::{SreObservation, SreStore};
use tokio::sync::{broadcast, RwLock};
use tracing::{error, info, warn};

// ── Config ─────────────────────────────────────────────────────────────────

pub struct SreConfig {
    pub clickhouse_url: String,
    pub clickhouse_db: String,
    pub clickhouse_user: String,
    pub clickhouse_pass: String,
    pub llm_base_url: String,
    pub llm_model: String,
    pub llm_api_key: Option<String>,
    pub llm_timeout_secs: u64,
    pub scan_interval: Duration,
    pub log_tail_lines: usize,
    pub dashboard_port: u16,
    pub auto_restart: bool,
    pub restart_cooldown_secs: u64,
    pub probe: ProbeConfig,
}

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
pub struct ContainerStatus {
    pub name: String,
    pub id: String,
    pub running: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub health: docker::HealthSummary,
    pub last_checked_at: DateTime<Utc>,
    pub last_severity: Option<String>,
    pub cpu_percent: f64,
    pub mem_used_bytes: u64,
    pub mem_limit_bytes: u64,
}

#[derive(serde::Serialize)]
pub struct SreState {
    pub containers: Vec<ContainerStatus>,
    pub findings: VecDeque<Finding>,
    pub last_scan_at: Option<DateTime<Utc>>,
    pub llm_available: bool,
    pub last_probe: Option<SystemProbeResult>,
    pub weakest_link: Option<WeakestLinkDecision>,
    pub last_weakest_link_at: Option<DateTime<Utc>>,
}

impl SreState {
    fn new() -> Self {
        Self {
            containers: Vec::new(),
            findings: VecDeque::new(),
            last_scan_at: None,
            llm_available: false,
            last_probe: None,
            weakest_link: None,
            last_weakest_link_at: None,
        }
    }

    fn push_finding(&mut self, f: Finding) {
        if self.findings.len() >= 100 {
            self.findings.pop_front();
        }
        self.findings.push_back(f);
    }
}

// ── ClickHouse client ───────────────────────────────────────────────────────

fn ch_client(cfg: &SreConfig) -> Client {
    Client::default()
        .with_url(&cfg.clickhouse_url)
        .with_database(&cfg.clickhouse_db)
        .with_user(&cfg.clickhouse_user)
        .with_password(&cfg.clickhouse_pass)
}

const MIGRATION_SRE: &str =
    include_str!("../../../migrations/clickhouse/0002_sre_observations.sql");
const MIGRATION_OUTAGES: &str =
    include_str!("../../../migrations/clickhouse/0004_sre_outages.sql");

async fn run_migration(client: &Client) -> anyhow::Result<()> {
    client.query(MIGRATION_SRE).execute().await?;
    client.query(MIGRATION_OUTAGES).execute().await?;
    Ok(())
}

// ── Analysis loop ───────────────────────────────────────────────────────────

fn is_one_shot_by_name(name: &str) -> bool {
    let bare = name.trim_start_matches("rre-");
    bare.ends_with("-init") || bare.ends_with("-patch") || bare.ends_with("-migration")
}

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

async fn scan_once(
    docker: &bollard::Docker,
    llm: &AnalysisClient,
    store: &mut SreStore,
    state: &Arc<RwLock<SreState>>,
    tx: &broadcast::Sender<Finding>,
    cfg: &SreConfig,
    ch: &Client,
) {
    // Probe LLM once per scan so the badge reflects real availability even when
    // all containers are healthy and no analysis calls are made.
    state.write().await.llm_available = llm.probe().await;

    let containers = match docker::list_containers(docker).await {
        Ok(v) => v,
        Err(e) => {
            error!("docker list_containers error: {e}");
            return;
        }
    };

    // Fetch stats for all running containers concurrently (one_shot=false blocks ~1s each).
    {
        let active: Vec<_> = containers
            .iter()
            .filter(|c| !is_one_shot_by_name(&c.name))
            .collect();

        let stats_futs = active.iter().map(|c| {
            let docker = docker.clone();
            let id = c.id.clone();
            async move {
                if c.running {
                    docker::fetch_stats(&docker, &id).await.unwrap_or((0.0, 0, 0))
                } else {
                    (0.0, 0, 0)
                }
            }
        });
        let all_stats: Vec<_> = futures_util::future::join_all(stats_futs).await;

        let statuses = active
            .iter()
            .zip(all_stats)
            .map(|(c, (cpu_percent, mem_used_bytes, mem_limit_bytes))| ContainerStatus {
                name: c.name.clone(),
                id: c.id.clone(),
                running: c.running,
                started_at: c.started_at,
                health: c.health.clone(),
                last_checked_at: Utc::now(),
                last_severity: None,
                cpu_percent,
                mem_used_bytes,
                mem_limit_bytes,
            })
            .collect();

        let mut st = state.write().await;
        st.containers = statuses;
        st.last_scan_at = Some(Utc::now());
    }

    // Run independent service probes concurrently with container scanning setup.
    let probe_result = probes::probe_all(&cfg.probe).await;

    // Store probe result in shared state.
    {
        let mut st = state.write().await;
        st.last_probe = Some(probe_result.clone());
    }

    // Emit metric-driven findings for memory pressure — no LLM needed.
    {
        let snapshot = state.read().await.containers.clone();
        for cs in &snapshot {
            if cs.mem_limit_bytes == 0 {
                continue;
            }
            let pct = cs.mem_used_bytes as f64 / cs.mem_limit_bytes as f64 * 100.0;
            if pct < 60.0 {
                continue;
            }
            let severity = if pct >= 80.0 { "CRITICAL" } else { "WARN" };
            let used_mb = cs.mem_used_bytes / 1_048_576;
            let limit_mb = cs.mem_limit_bytes / 1_048_576;
            let suggested_mb = (cs.mem_used_bytes as f64 / 0.4) as u64 / 1_048_576;
            let finding = analysis::Finding {
                severity: severity.to_string(),
                category: "resource_pressure".to_string(),
                finding: format!(
                    "Memory usage is {:.1}% ({} MB / {} MB) — above the 60% advisory threshold.",
                    pct, used_mb, limit_mb
                ),
                proposed_fix: format!(
                    "Increase the memory limit for {} to ~{} MB so current usage stays below 40%. \
                     Update the relevant *_MEM_LIMIT env var in docker-compose.yml and run \
                     `docker compose up -d --no-deps {}`.",
                    cs.name, suggested_mb, cs.name.trim_start_matches("rre-")
                ),
                container_name: cs.name.clone(),
                observed_at: Some(Utc::now()),
            };
            let hash = sha256_hex(&format!("{}:mem:{:.0}", cs.name, pct / 5.0 * 5.0));
            if !store.is_unchanged(&cs.name, &hash) {
                store.record_hash(&cs.name, &hash);
                store.record_severity(&cs.name, severity);
                let _ = tx.send(finding.clone());
                let mut st = state.write().await;
                st.findings.push_front(finding);
                st.findings.truncate(200);
            }
        }
    }

    // Analyze containers, skipping one-time init/patch/migration jobs.
    for c in &containers {
        if is_one_shot_by_name(&c.name) {
            continue;
        }
        analyze_container(c, docker, llm, store, state, tx, cfg).await;
        // Give the single-process inference server breathing room between requests.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // Fetch lag info from ClickHouse.
    let (total_lag, lag_trend, ch_backlog_batches) = store::fetch_pipeline_lag(ch).await;

    // Call weakest-link at most once per 60 s — it is LLM-heavy.
    let any_service_down = !probe_result.all_ok;
    let weakest_link_due = {
        let st = state.read().await;
        st.last_weakest_link_at
            .map(|t| Utc::now().signed_duration_since(t).num_seconds() >= 60)
            .unwrap_or(true)
    };
    if (total_lag > 0 || any_service_down) && weakest_link_due {
        let recent_findings: Vec<Finding> = {
            let st = state.read().await;
            st.findings.iter().cloned().collect()
        };
        let decision = llm
            .decide_weakest_link(
                &probe_result,
                total_lag,
                &lag_trend,
                ch_backlog_batches,
                &recent_findings,
            )
            .await;

        let mut st = state.write().await;
        st.weakest_link = decision;
        st.last_weakest_link_at = Some(Utc::now());
    }
}

/// Keep only WARN/ERROR/CRITICAL log lines — INFO and DEBUG are noise for SRE analysis.
fn filter_noisy_logs(raw: &str) -> String {
    raw.lines()
        .filter(|l| {
            let u = l.to_ascii_uppercase();
            u.contains("WARN")
                || u.contains("ERROR")
                || u.contains("CRITICAL")
                || u.contains("FATAL")
                || u.contains("PANIC")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn analyze_container(
    c: &ContainerInfo,
    docker: &bollard::Docker,
    llm: &AnalysisClient,
    store: &mut SreStore,
    state: &Arc<RwLock<SreState>>,
    tx: &broadcast::Sender<Finding>,
    cfg: &SreConfig,
) {
    if c.one_shot && c.exit_code == Some(0) {
        return;
    }

    // Track running-state transitions — persists outage/restoration events in ClickHouse.
    // `auto_restarted` is true when the container just came back AND we were the ones
    // who restarted it (i.e. it was down in prev scan and we acted on it).
    {
        let auto_restarted = c.running && store.was_auto_restarted(&c.name);
        if let Some(ev) = store.update_running_state(&c.name, c.running, auto_restarted) {
            if let Err(e) = store.write_outage_event(&ev).await {
                error!("outage event write error: {e}");
            }
        }
    }

    // Container is stopped/exited — emit CRITICAL immediately, no LLM needed.
    if !c.running {
        let hash = sha256_hex(&format!("{}:stopped", c.name));
        if !store.is_unchanged(&c.name, &hash) {
            store.reset_suppression(&c.name);
            store.record_hash(&c.name, &hash);
            store.record_severity(&c.name, "CRITICAL");

            // Auto-restart if enabled and not on cooldown.
            let restarted = if cfg.auto_restart {
                if store.is_restart_on_cooldown(&c.name, cfg.restart_cooldown_secs) {
                    info!(
                        "auto-restart suppressed for {} (cooldown {}s)",
                        c.name, cfg.restart_cooldown_secs
                    );
                    false
                } else {
                    match docker::restart_container(docker, &c.id).await {
                        Ok(()) => {
                            info!("auto-restarted container {}", c.name);
                            store.record_restart(&c.name);
                            true
                        }
                        Err(e) => {
                            error!("failed to restart {}: {e}", c.name);
                            false
                        }
                    }
                }
            } else {
                false
            };

            let fix = if restarted {
                format!("Container {} was automatically restarted by the SRE agent.", c.name)
            } else {
                format!("Restart with: docker start {}", c.name)
            };

            let finding = Finding {
                severity: "CRITICAL".into(),
                category: "crash_loop".into(),
                finding: format!(
                    "Container {} is not running (exited/stopped). Service is down.",
                    c.name
                ),
                proposed_fix: fix,
                container_name: c.name.clone(),
                observed_at: Some(Utc::now()),
            };

            let obs = store::SreObservation {
                observed_at: Utc::now(),
                container_name: c.name.clone(),
                severity: finding.severity.clone(),
                category: finding.category.clone(),
                finding: finding.finding.clone(),
                proposed_fix: finding.proposed_fix.clone(),
                log_window_hash: hash,
                log_snippet: String::new(),
            };
            if let Err(e) = store.write(&obs).await {
                error!("SreStore write error: {e}");
            }

            {
                let mut st = state.write().await;
                if let Some(cs) = st.containers.iter_mut().find(|cs| cs.name == c.name) {
                    cs.last_severity = Some("CRITICAL".into());
                }
                st.push_finding(finding.clone());
            }
            let _ = tx.send(finding);
        }
        return;
    }

    let raw = match docker::tail_logs(docker, &c.name, cfg.log_tail_lines).await {
        Ok(l) => l,
        Err(e) => {
            warn!("tail_logs {}: {e}", c.name);
            return;
        }
    };

    // Only look at lines that signal a problem.
    let logs = filter_noisy_logs(&raw);

    if logs.is_empty() {
        // No warnings or errors — nothing to analyse.
        return;
    }

    let hash = sha256_hex(&logs);
    let snippet: String = logs.chars().take(500).collect();

    // Same WARN/ERROR pattern as last scan — no new information.
    if store.is_unchanged(&c.name, &hash) {
        return;
    }

    // New hash means new WARN/ERROR lines appeared — reset quiet suppression.
    store.reset_suppression(&c.name);

    // LLM has already confirmed "nothing unusual" several times for this pattern.
    if store.is_suppressed(&c.name) {
        store.record_hash(&c.name, &hash);
        return;
    }

    let mut finding = match llm.analyze(&c.name, &logs).await {
        Ok(f) => {
            state.write().await.llm_available = true;
            f
        }
        Err(e) => {
            warn!("LLM unavailable for {}: {e}", c.name);
            state.write().await.llm_available = false;
            return;
        }
    };
    finding.container_name = c.name.clone();
    finding.observed_at = Some(Utc::now());

    store.record_hash(&c.name, &hash);
    store.record_severity(&c.name, &finding.severity);

    let obs = SreObservation {
        observed_at: Utc::now(),
        container_name: c.name.clone(),
        severity: finding.severity.clone(),
        category: finding.category.clone(),
        finding: finding.finding.clone(),
        proposed_fix: finding.proposed_fix.clone(),
        log_window_hash: hash,
        log_snippet: snippet,
    };

    if let Err(e) = store.write(&obs).await {
        error!("SreStore write error: {e}");
    }

    {
        let mut st = state.write().await;
        if let Some(cs) = st.containers.iter_mut().find(|cs| cs.name == c.name) {
            cs.last_severity = Some(finding.severity.clone());
        }
        st.push_finding(finding.clone());
    }

    let _ = tx.send(finding);
}

async fn analysis_loop(
    docker: bollard::Docker,
    llm: AnalysisClient,
    state: Arc<RwLock<SreState>>,
    tx: broadcast::Sender<Finding>,
    cfg: Arc<SreConfig>,
) {
    let ch = ch_client(&cfg);
    let mut store = SreStore::new(&ch);

    loop {
        scan_once(&docker, &llm, &mut store, &state, &tx, &cfg, &ch).await;
        tokio::time::sleep(cfg.scan_interval).await;
    }
}

// ── Entry point ─────────────────────────────────────────────────────────────

pub async fn run(cfg: SreConfig) -> anyhow::Result<()> {
    let cfg = Arc::new(cfg);

    // Apply ClickHouse migration
    let ch = ch_client(&cfg);
    run_migration(&ch).await?;
    info!("ClickHouse migration applied");

    // Connect to Docker
    let docker = bollard::Docker::connect_with_local_defaults()?;
    info!("Connected to Docker socket");

    // Shared state + SSE broadcast channel
    let state: Arc<RwLock<SreState>> = Arc::new(RwLock::new(SreState::new()));
    let (tx, _rx) = broadcast::channel::<Finding>(256);

    // Build analysis client
    let llm = AnalysisClient::new(
        &cfg.llm_base_url,
        &cfg.llm_model,
        cfg.llm_api_key.clone(),
        cfg.llm_timeout_secs,
    );

    // Spawn analysis loop
    let dash_llm = llm.clone();
    let loop_handle = {
        let docker = docker.clone();
        let state = state.clone();
        let tx = tx.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            analysis_loop(docker, llm, state, tx, cfg).await;
        })
    };

    // Spawn dashboard
    let dash_handle = {
        let state = state.clone();
        let tx = tx.clone();
        let port = cfg.dashboard_port;
        let dash_ch = ch_client(&cfg);
        tokio::spawn(async move {
            dashboard::serve(state, tx, dash_ch, dash_llm, port).await;
        })
    };

    tokio::select! {
        res = loop_handle  => { res?; }
        res = dash_handle  => { res?; }
    }

    Ok(())
}
