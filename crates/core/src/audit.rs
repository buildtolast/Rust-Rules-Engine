//! Audit model — one record per (event × rule). Mirrors Java
//! `audit/AuditRecord.java` and `audit/AuditType.java`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Categorization of an evaluation result. Serializes to the Java enum names
/// (`MATCHED` / `UNMATCHED` / `ERRORED`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditType {
    /// The rule matched the event payload.
    Matched,
    /// The rule did not match the event payload.
    Unmatched,
    /// Parsing or evaluation failed.
    Errored,
}

/// Deterministic audit identifier. Must equal the Java format
/// `{topic}:{partition}:{offset}:{ruleId}` — it is the dedup key.
pub fn audit_id(topic: &str, partition: i32, offset: i64, rule_id: &str) -> String {
    format!("{topic}:{partition}:{offset}:{rule_id}")
}

/// One audit record per evaluation of a single rule against a single event.
/// Serializes with camelCase keys to match the Java wire contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRecord {
    /// `{topic}:{partition}:{offset}:{ruleId}` — the dedup key.
    pub audit_id: String,
    pub rule_id: String,
    pub schema_version: u32,
    pub audit_type: AuditType,
    /// Explanation for UNMATCHED / ERRORED; absent (null) otherwise.
    #[serde(default)]
    pub reason: Option<String>,
    /// Original raw JSON input.
    pub source_event: String,
    /// Payload routed to the target topic; absent (null) when not matched.
    #[serde(default)]
    pub routed_event: Option<String>,
    pub source_topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: DateTime<Utc>,
    pub parse_time_nano: u64,
    pub eval_time_nano: u64,
    pub total_time_nano: u64,
}
