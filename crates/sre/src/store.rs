use chrono::{DateTime, Utc};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(clickhouse::Row, Serialize, Deserialize, Debug, Clone)]
pub struct SreOutageEvent {
    pub container_name: String,
    pub event_type: u8, // 1 = down, 2 = restored (matches ClickHouse Enum8)
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub occurred_at: DateTime<Utc>,
    pub auto_restarted: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Incident {
    pub container: String,
    pub down_at: DateTime<Utc>,
    pub restored_at: Option<DateTime<Utc>>,
    pub auto_restarted: bool,
    pub duration_secs: Option<i64>,
    pub active: bool,
}

#[derive(Debug, Error)]
pub enum SreStoreError {
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
}

#[derive(clickhouse::Row, Serialize, Deserialize, Debug, Clone)]
pub struct SreObservation {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at: DateTime<Utc>,
    pub container_name: String,
    pub severity: String,
    pub category: String,
    pub finding: String,
    pub proposed_fix: String,
    pub log_window_hash: String,
    pub log_snippet: String,
}

/// After this many consecutive "nothing unusual" findings, suppress LLM calls
/// for that container until new WARN/ERROR lines appear.
const QUIET_SUPPRESS_AFTER: u32 = 3;

pub struct SreStore {
    client: Client,
    last_hashes: std::collections::HashMap<String, String>,
    quiet_streaks: std::collections::HashMap<String, u32>,
    last_restart: std::collections::HashMap<String, std::time::Instant>,
    // None = first scan (don't emit events for pre-existing state)
    prev_running: std::collections::HashMap<String, bool>,
}

impl SreStore {
    pub fn new(client: &Client) -> Self {
        Self { client: client.clone(),
               last_hashes: Default::default(),
               quiet_streaks: Default::default(),
               last_restart: Default::default(),
               prev_running: Default::default() }
    }

    /// Call once per container per scan. Returns Some(event) only when the
    /// running state transitions (true→false or false→true). The first scan
    /// is used to establish baseline — no events emitted.
    pub fn update_running_state(&mut self,
                                container: &str,
                                running: bool,
                                auto_restarted: bool)
                                -> Option<SreOutageEvent> {
        let prev = self.prev_running.get(container).copied();
        self.prev_running.insert(container.to_string(), running);

        match (prev, running) {
            (Some(true), false) => Some(SreOutageEvent { container_name: container.to_string(),
                                                         event_type: 1, // down
                                                         occurred_at: Utc::now(),
                                                         auto_restarted: false }),
            (Some(false), true) => Some(SreOutageEvent { container_name: container.to_string(),
                                                         event_type: 2, // restored
                                                         occurred_at: Utc::now(),
                                                         auto_restarted }),
            _ => None, // first scan or no change
        }
    }

    pub async fn write_outage_event(&mut self, ev: &SreOutageEvent) -> Result<(), SreStoreError> {
        let mut insert = self.client.insert::<SreOutageEvent>("sre_outages").await?;
        insert.write(ev).await?;
        insert.end().await?;
        Ok(())
    }

    /// Returns all incidents (paired down/restored events), most recent first.
    pub async fn read_incidents(client: &Client) -> Result<Vec<Incident>, SreStoreError> {
        let events = client.query(
                                  "SELECT container_name, event_type, occurred_at, auto_restarted
                 FROM sre_outages
                 ORDER BY container_name, occurred_at ASC",
        )
                           .fetch_all::<SreOutageEvent>()
                           .await?;

        // Pair down (1) with the next restored (2) per container.
        let mut open: std::collections::HashMap<String, SreOutageEvent> = Default::default();
        let mut incidents: Vec<Incident> = Vec::new();

        for ev in events {
            match ev.event_type {
                1 => {
                    open.insert(ev.container_name.clone(), ev);
                }
                2 => {
                    if let Some(down) = open.remove(&ev.container_name) {
                        let duration_secs = (ev.occurred_at - down.occurred_at).num_seconds();
                        incidents.push(Incident { container: down.container_name,
                                                  down_at: down.occurred_at,
                                                  restored_at: Some(ev.occurred_at),
                                                  auto_restarted: ev.auto_restarted,
                                                  duration_secs: Some(duration_secs),
                                                  active: false });
                    }
                }
                _ => {}
            }
        }

        // Any remaining open events are still-active outages.
        for (_, down) in open {
            incidents.push(Incident { container: down.container_name,
                                      down_at: down.occurred_at,
                                      restored_at: None,
                                      auto_restarted: false,
                                      duration_secs: None,
                                      active: true });
        }

        incidents.sort_by(|a, b| b.down_at.cmp(&a.down_at));
        Ok(incidents)
    }

    /// True if the container was restarted less than `cooldown_secs` ago.
    pub fn is_restart_on_cooldown(&self, container: &str, cooldown_secs: u64) -> bool {
        self.last_restart
            .get(container)
            .map(|t| t.elapsed().as_secs() < cooldown_secs)
            .unwrap_or(false)
    }

    pub fn record_restart(&mut self, container: &str) {
        self.last_restart
            .insert(container.to_string(), std::time::Instant::now());
    }

    /// True if we issued a restart for this container and it was previously down.
    pub fn was_auto_restarted(&self, container: &str) -> bool {
        let was_down = self.prev_running.get(container).copied() == Some(false);
        let restarted_recently = self.last_restart
                                     .get(container)
                                     .map(|t| t.elapsed().as_secs() < 120)
                                     .unwrap_or(false);
        was_down && restarted_recently
    }

    /// True if the filtered log window is identical to the previous scan.
    pub fn is_unchanged(&self, container: &str, hash: &str) -> bool {
        self.last_hashes
            .get(container)
            .map(|h| h == hash)
            .unwrap_or(false)
    }

    /// True if the LLM has confirmed "nothing unusual" enough times that we
    /// should suppress further calls until new WARN/ERROR lines appear.
    pub fn is_suppressed(&self, container: &str) -> bool {
        self.quiet_streaks.get(container).copied().unwrap_or(0) >= QUIET_SUPPRESS_AFTER
    }

    pub fn record_hash(&mut self, container: &str, hash: &str) {
        self.last_hashes
            .insert(container.to_string(), hash.to_string());
    }

    /// Call after each LLM result. Increments quiet streak for INFO/normal,
    /// resets it for anything that warrants attention.
    pub fn record_severity(&mut self, container: &str, severity: &str) {
        let streak = self.quiet_streaks.entry(container.to_string()).or_insert(0);
        if severity == "INFO" {
            *streak += 1;
        } else {
            *streak = 0;
        }
    }

    /// Call when a new WARN/ERROR hash is seen — resets suppression so the
    /// LLM gets a fresh look.
    pub fn reset_suppression(&mut self, container: &str) {
        self.quiet_streaks.insert(container.to_string(), 0);
    }

    /// Returns the most recent findings, deduplicated by (container, hash) so
    /// two replicas scanning the same container don't produce double entries.
    pub async fn read_recent(client: &Client,
                             limit: u64)
                             -> Result<Vec<SreObservation>, SreStoreError> {
        let rows = client.query(
                                "SELECT
                    max(observed_at) AS observed_at,
                    container_name,
                    argMax(severity,     observed_at) AS severity,
                    argMax(category,     observed_at) AS category,
                    argMax(finding,      observed_at) AS finding,
                    argMax(proposed_fix, observed_at) AS proposed_fix,
                    log_window_hash,
                    argMax(log_snippet,  observed_at) AS log_snippet
                 FROM sre_observations
                 GROUP BY container_name, log_window_hash
                 ORDER BY observed_at DESC
                 LIMIT ?",
        )
                         .bind(limit)
                         .fetch_all::<SreObservation>()
                         .await?;
        Ok(rows)
    }

    pub async fn write(&mut self, obs: &SreObservation) -> Result<(), SreStoreError> {
        let mut insert = self.client
                             .insert::<SreObservation>("sre_observations")
                             .await?;
        insert.write(obs).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn end(self) -> Result<(), SreStoreError> {
        Ok(())
    }
}

/// Query the latest two rows from `pipeline_lag` for group `rules-engine`.
/// Returns `(total_lag, lag_trend, ch_backlog_batches)`.
/// Falls back to `(0, "stable", 0)` when the table is absent or empty.
pub async fn fetch_pipeline_lag(ch: &Client) -> (i64, String, i32) {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LagRow {
        total_lag: i64,
        ch_backlog_batches: i32,
    }

    let rows: Vec<LagRow> = match ch.query(
                                           "SELECT total_lag, ch_backlog_batches \
             FROM pipeline_lag \
             WHERE consumer_group = 'rules-engine' \
             ORDER BY recorded_at DESC \
             LIMIT 2",
    )
                                    .fetch_all()
                                    .await
    {
        Ok(r) => r,
        Err(_) => return (0, "stable".into(), 0),
    };

    match rows.as_slice() {
        [] => (0, "stable".into(), 0),
        [latest] => (latest.total_lag, "stable".into(), latest.ch_backlog_batches),
        [latest, prev, ..] => {
            let trend = if latest.total_lag > prev.total_lag {
                "growing"
            } else if latest.total_lag < prev.total_lag {
                "draining"
            } else {
                "stable"
            };
            (latest.total_lag, trend.into(), latest.ch_backlog_batches)
        }
    }
}
