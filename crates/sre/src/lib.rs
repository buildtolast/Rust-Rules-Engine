pub mod analysis;
pub mod dashboard;
pub mod docker;
pub mod store;

use analysis::{AnalysisClient, Finding};
use chrono::{DateTime, Utc};
use clickhouse::Client;
use docker::ContainerInfo;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use store::{SreObservation, SreStore};
use tokio::sync::{broadcast, RwLock};
use tracing::{error, info, warn};

// ── Config ─────────────────────────────────────────────────────────────────

pub struct SreConfig {
    pub clickhouse_url:    String,
    pub clickhouse_db:     String,
    pub clickhouse_user:   String,
    pub clickhouse_pass:   String,
    pub llm_base_url:      String,
    pub llm_model:         String,
    pub llm_api_key:       Option<String>,
    pub scan_interval:     Duration,
    pub log_tail_lines:    usize,
    pub dashboard_port:    u16,
}

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
pub struct ContainerStatus {
    pub name:            String,
    pub id:              String,
    pub running:         bool,
    pub started_at:      Option<DateTime<Utc>>,
    pub health:          docker::HealthSummary,
    pub last_checked_at: DateTime<Utc>,
    pub last_severity:   Option<String>,
}

#[derive(serde::Serialize)]
pub struct SreState {
    pub containers:    Vec<ContainerStatus>,
    pub findings:      VecDeque<Finding>,
    pub last_scan_at:  Option<DateTime<Utc>>,
    pub llm_available: bool,
}

impl SreState {
    fn new() -> Self {
        Self {
            containers:    Vec::new(),
            findings:      VecDeque::new(),
            last_scan_at:  None,
            llm_available: false,
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

async fn run_migration(client: &Client) -> anyhow::Result<()> {
    client.query(MIGRATION_SRE).execute().await?;
    Ok(())
}

// ── Analysis loop ───────────────────────────────────────────────────────────

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

async fn scan_once(
    docker:  &bollard::Docker,
    llm:     &AnalysisClient,
    store:   &mut SreStore,
    state:   &Arc<RwLock<SreState>>,
    tx:      &broadcast::Sender<Finding>,
    cfg:     &SreConfig,
) {
    let containers = match docker::list_containers(docker).await {
        Ok(v)  => v,
        Err(e) => { error!("docker list_containers error: {e}"); return; }
    };

    // Update container statuses in shared state
    {
        let mut st = state.write().await;
        st.containers = containers.iter().map(|c| ContainerStatus {
            name:            c.name.clone(),
            id:              c.id.clone(),
            running:         c.running,
            started_at:      c.started_at,
            health:          c.health.clone(),
            last_checked_at: Utc::now(),
            last_severity:   None,
        }).collect();
        st.last_scan_at = Some(Utc::now());
    }

    for c in &containers {
        analyze_container(c, docker, llm, store, state, tx, cfg).await;
    }
}

async fn analyze_container(
    c:      &ContainerInfo,
    docker: &bollard::Docker,
    llm:    &AnalysisClient,
    store:  &mut SreStore,
    state:  &Arc<RwLock<SreState>>,
    tx:     &broadcast::Sender<Finding>,
    cfg:    &SreConfig,
) {
    let logs = match docker::tail_logs(docker, &c.name, cfg.log_tail_lines).await {
        Ok(l)  => l,
        Err(e) => { warn!("tail_logs {}: {e}", c.name); return; }
    };

    let hash = sha256_hex(&logs);
    let snippet: String = logs.chars().take(500).collect();

    let finding = match llm.analyze(&c.name, &logs).await {
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

    let obs = SreObservation {
        observed_at:     Utc::now(),
        container_name:  c.name.clone(),
        severity:        finding.severity.clone(),
        category:        finding.category.clone(),
        finding:         finding.finding.clone(),
        proposed_fix:    finding.proposed_fix.clone(),
        log_window_hash: hash,
        log_snippet:     snippet,
    };

    if let Err(e) = store.write(&obs).await {
        error!("SreStore write error: {e}");
    }

    // Update last_severity in state and push to ring buffer
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
    llm:    AnalysisClient,
    state:  Arc<RwLock<SreState>>,
    tx:     broadcast::Sender<Finding>,
    cfg:    Arc<SreConfig>,
) {
    let ch = ch_client(&cfg);
    let mut store = SreStore::new(&ch);

    loop {
        scan_once(&docker, &llm, &mut store, &state, &tx, &cfg).await;
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
    let llm = AnalysisClient::new(&cfg.llm_base_url, &cfg.llm_model, cfg.llm_api_key.clone());

    // Spawn analysis loop
    let loop_handle = {
        let docker = docker.clone();
        let state  = state.clone();
        let tx     = tx.clone();
        let cfg    = cfg.clone();
        tokio::spawn(async move {
            analysis_loop(docker, llm, state, tx, cfg).await;
        })
    };

    // Spawn dashboard
    let dash_handle = {
        let state = state.clone();
        let tx    = tx.clone();
        let port  = cfg.dashboard_port;
        tokio::spawn(async move {
            dashboard::serve(state, tx, port).await;
        })
    };

    tokio::select! {
        res = loop_handle  => { res?; }
        res = dash_handle  => { res?; }
    }

    Ok(())
}
