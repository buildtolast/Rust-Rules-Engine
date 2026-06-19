//! store-clickhouse — batched audit writer, migrations (S3), analytics queries (S4)

pub mod analytics;
pub use analytics::{query_analytics, AnalyticsStats, RuleStat, TimeSeriesPoint};

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

pub const MIGRATION_AUDITS: &str = include_str!("../../../migrations/clickhouse/0001_audits.sql");

pub async fn run_migrations(client: &Client) -> Result<(), Error> {
    client.query(MIGRATION_AUDITS).execute().await?;
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
    inserter: clickhouse::inserter::Inserter<AuditRow>,
}

impl AuditWriter {
    pub fn new(client: &Client, cfg: &ClickHouseConfig) -> Self {
        let mut inserter = client.inserter::<AuditRow>("audits");
        inserter = inserter.with_max_rows(cfg.batch_max_rows);
        inserter =
            inserter.with_period(Some(std::time::Duration::from_millis(cfg.batch_period_ms)));
        Self { inserter }
    }

    pub async fn write(&mut self, rec: &AuditRecord) -> Result<(), Error> {
        let row = AuditRow::from_record(rec);
        self.inserter.write(&row).await?;
        self.inserter.commit().await?;
        Ok(())
    }

    pub async fn end(self) -> Result<(), Error> {
        self.inserter.end().await?;
        Ok(())
    }
}
