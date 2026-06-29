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
    pub fn from_kafka(topic: &str,
                      partition: i32,
                      offset: i64,
                      timestamp_ms: i64,
                      raw: &str)
                      -> Result<Self, serde_json::Error> {
        let payload = serde_json::from_str(raw)?;
        Ok(Self { topic: topic.to_owned(),
                  partition,
                  offset,
                  timestamp_ms,
                  raw: raw.to_owned(),
                  payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_json_parses_all_fields() {
        let raw = r#"{"key": "value", "num": 42}"#;
        let event = SourceEvent::from_kafka("test-topic", 2, 100, 1700000000000, raw).unwrap();
        assert_eq!(event.topic, "test-topic");
        assert_eq!(event.partition, 2);
        assert_eq!(event.offset, 100);
        assert_eq!(event.timestamp_ms, 1700000000000);
        assert_eq!(event.raw, raw);
        assert_eq!(event.payload["key"], "value");
        assert_eq!(event.payload["num"], 42);
    }

    #[test]
    fn invalid_json_returns_err() {
        let result = SourceEvent::from_kafka("topic", 0, 0, 0, "{not valid json}");
        assert!(result.is_err());
    }

    #[test]
    fn empty_string_returns_err() {
        let result = SourceEvent::from_kafka("topic", 0, 0, 0, "");
        assert!(result.is_err());
    }
}
