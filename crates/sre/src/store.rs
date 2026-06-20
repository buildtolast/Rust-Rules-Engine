use chrono::{DateTime, Utc};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
}

impl SreStore {
    pub fn new(client: &Client) -> Self {
        Self {
            client: client.clone(),
            last_hashes: Default::default(),
            quiet_streaks: Default::default(),
        }
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
    pub async fn read_recent(
        client: &Client,
        limit: u64,
    ) -> Result<Vec<SreObservation>, SreStoreError> {
        let rows = client
            .query(
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
        let mut insert = self
            .client
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
