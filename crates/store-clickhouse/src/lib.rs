//! store-clickhouse — batched audit writer, migrations (S3), analytics queries (S4)

pub mod analytics;
pub use analytics::{
    query_analytics, query_top_audits, AnalyticsStats, AuditQueryRow, RuleStat, TimeSeriesPoint,
};
pub use clickhouse;
pub use clickhouse::Client as ClickHouseClient;

use chrono::{DateTime, Utc};
use clickhouse::Client;
use rules_core::{AuditRecord, AuditType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
}

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub batch_max_rows: u64,
    pub batch_period_ms: u64,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "ruleaudit".into(),
            user: "rules".into(),
            password: "rules".into(),
            batch_max_rows: 500,
            batch_period_ms: 200,
        }
    }
}

pub fn client(cfg: &ClickHouseConfig) -> Client {
    Client::default()
        .with_url(&cfg.url)
        .with_database(&cfg.database)
        .with_user(&cfg.user)
        .with_password(&cfg.password)
}

pub async fn ping(client: &Client) -> bool {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct One {
        #[serde(rename = "1")]
        _v: u8,
    }
    client.query("SELECT 1").fetch_one::<One>().await.is_ok()
}

pub const MIGRATION_AUDITS: &str = include_str!("../../../migrations/clickhouse/0001_audits.sql");
pub const MIGRATION_SRE: &str =
    include_str!("../../../migrations/clickhouse/0002_sre_observations.sql");
pub const MIGRATION_ANALYTICS: &str =
    include_str!("../../../migrations/clickhouse/0003_analytics.sql");
pub const MIGRATION_PIPELINE_LAG: &str =
    include_str!("../../../migrations/clickhouse/0005_pipeline_lag.sql");

pub async fn run_migrations(client: &Client) -> Result<(), Error> {
    client.query(MIGRATION_AUDITS).execute().await?;
    client.query(MIGRATION_SRE).execute().await?;
    client.query(MIGRATION_PIPELINE_LAG).execute().await?;
    for stmt in MIGRATION_ANALYTICS.split(";\n\n") {
        let stmt = stmt.trim();
        if !stmt.is_empty() {
            client.query(stmt).execute().await?;
        }
    }
    Ok(())
}

fn audit_type_str(t: AuditType) -> &'static str {
    match t {
        AuditType::Matched => "MATCHED",
        AuditType::Unmatched => "UNMATCHED",
        AuditType::Errored => "ERRORED",
    }
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct AuditRow {
    pub audit_id: String,
    pub rule_id: String,
    pub schema_version: u32,
    pub audit_type: String,
    pub reason: String,
    pub source_event: String,
    pub routed_event: String,
    pub source_topic: String,
    pub partition: i32,
    pub offset: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub parse_time_nano: u64,
    pub eval_time_nano: u64,
    pub total_time_nano: u64,
}

impl AuditRow {
    pub fn from_record(rec: &AuditRecord) -> Self {
        Self {
            audit_id: rec.audit_id.clone(),
            rule_id: rec.rule_id.clone(),
            schema_version: rec.schema_version,
            audit_type: audit_type_str(rec.audit_type).to_string(),
            reason: rec.reason.clone().unwrap_or_default(),
            source_event: rec.source_event.clone(),
            routed_event: rec.routed_event.clone().unwrap_or_default(),
            source_topic: rec.source_topic.clone(),
            partition: rec.partition,
            offset: rec.offset,
            timestamp: rec.timestamp,
            parse_time_nano: rec.parse_time_nano,
            eval_time_nano: rec.eval_time_nano,
            total_time_nano: rec.total_time_nano,
        }
    }
}

pub struct AuditWriter {
    client: Client,
    buffer: Vec<AuditRow>,
    batch_max: usize,
}

impl AuditWriter {
    pub fn new(client: &Client, cfg: &ClickHouseConfig) -> Self {
        Self {
            client: client.clone(),
            buffer: Vec::with_capacity(cfg.batch_max_rows as usize),
            batch_max: cfg.batch_max_rows as usize,
        }
    }

    /// Buffer a single record; flushes automatically when the buffer is full.
    pub async fn write(&mut self, rec: &AuditRecord) -> Result<(), Error> {
        self.write_batch(std::slice::from_ref(rec)).await
    }

    /// Buffer a batch of records; flushes automatically when the buffer is full.
    #[tracing::instrument(level = "debug", skip(self, recs), fields(row_count = recs.len()))]
    pub async fn write_batch(&mut self, recs: &[AuditRecord]) -> Result<(), Error> {
        for rec in recs {
            self.buffer.push(AuditRow::from_record(rec));
        }
        if self.buffer.len() >= self.batch_max {
            self.flush().await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(row_count = self.buffer.len()))]
    pub async fn flush(&mut self) -> Result<(), Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<AuditRow>("audits").await?;
        for row in &self.buffer {
            insert.write(row).await?;
        }
        insert.end().await?;
        self.buffer.clear();
        Ok(())
    }

    pub async fn end(mut self) -> Result<(), Error> {
        self.flush().await
    }
}
