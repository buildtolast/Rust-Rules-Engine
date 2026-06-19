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
    pub observed_at:     DateTime<Utc>,
    pub container_name:  String,
    pub severity:        String,
    pub category:        String,
    pub finding:         String,
    pub proposed_fix:    String,
    pub log_window_hash: String,
    pub log_snippet:     String,
}

pub struct SreStore {
    inserter: clickhouse::inserter::Inserter<SreObservation>,
}

impl SreStore {
    pub fn new(client: &Client) -> Self {
        let mut inserter = client.inserter::<SreObservation>("ruleaudit.sre_observations");
        inserter = inserter.with_max_rows(100);
        inserter = inserter.with_period(Some(std::time::Duration::from_secs(5)));
        Self { inserter }
    }

    pub async fn write(&mut self, obs: &SreObservation) -> Result<(), SreStoreError> {
        self.inserter.write(obs).await?;
        self.inserter.commit().await?;
        Ok(())
    }

    pub async fn end(self) -> Result<(), SreStoreError> {
        self.inserter.end().await?;
        Ok(())
    }
}
