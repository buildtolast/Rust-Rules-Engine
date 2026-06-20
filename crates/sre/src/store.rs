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

pub struct SreStore {
    client: Client,
}

impl SreStore {
    pub fn new(client: &Client) -> Self {
        Self {
            client: client.clone(),
        }
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
