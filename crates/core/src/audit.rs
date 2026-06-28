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
#[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_id_correct_format() {
        let id = audit_id("my-topic", 3, 99, "rule-abc");
        assert_eq!(id, "my-topic:3:99:rule-abc");
    }

    #[test]
    fn audit_id_uniqueness_by_all_components() {
        let a = audit_id("topic", 0, 0, "rule-1");
        let b = audit_id("topic", 0, 0, "rule-2");
        let c = audit_id("topic", 0, 1, "rule-1");
        let d = audit_id("topic", 1, 0, "rule-1");
        let e = audit_id("other", 0, 0, "rule-1");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_ne!(a, e);
    }

    #[test]
    fn audit_type_serializes_matched() {
        let s = serde_json::to_string(&AuditType::Matched).unwrap();
        assert_eq!(s, r#""MATCHED""#);
    }

    #[test]
    fn audit_type_serializes_unmatched() {
        let s = serde_json::to_string(&AuditType::Unmatched).unwrap();
        assert_eq!(s, r#""UNMATCHED""#);
    }

    #[test]
    fn audit_type_serializes_errored() {
        let s = serde_json::to_string(&AuditType::Errored).unwrap();
        assert_eq!(s, r#""ERRORED""#);
    }

    #[test]
    fn audit_type_deserializes_from_screaming_snake_case() {
        let matched: AuditType = serde_json::from_str(r#""MATCHED""#).unwrap();
        let unmatched: AuditType = serde_json::from_str(r#""UNMATCHED""#).unwrap();
        let errored: AuditType = serde_json::from_str(r#""ERRORED""#).unwrap();
        assert_eq!(matched, AuditType::Matched);
        assert_eq!(unmatched, AuditType::Unmatched);
        assert_eq!(errored, AuditType::Errored);
    }
}
