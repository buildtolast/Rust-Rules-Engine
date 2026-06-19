//! Inbound event from the source topic. Carries the byte-identical raw JSON
//! (for `AuditRecord.source_event`) plus the parsed payload (for evaluation).

use serde_json::Value;

/// A record consumed from `source-events`, with its Kafka coordinates.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceEvent {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Kafka record timestamp, epoch milliseconds.
    pub timestamp_ms: i64,
    /// Original payload bytes as a string — preserved verbatim for the audit record.
    pub raw: String,
    /// Parsed payload, bound as the evaluation activation (see S2).
    pub payload: Value,
}

impl SourceEvent {
    /// Build from a consumed Kafka record, parsing the raw JSON payload once.
    pub fn from_kafka(
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp_ms: i64,
        raw: &str,
    ) -> Result<Self, serde_json::Error> {
        let payload = serde_json::from_str(raw)?;
        Ok(Self {
            topic: topic.to_owned(),
            partition,
            offset,
            timestamp_ms,
            raw: raw.to_owned(),
            payload,
        })
    }
}
