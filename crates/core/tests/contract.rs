//! Contract tests: serde shapes and audit_id must match the Java
//! `Spring-Kafka-Stream-Rules` system. See plans/rust-rules-engine-rebuild.md (S1).

use chrono::{TimeZone, Utc};
use rules_core::{
    audit_id, AuditRecord, AuditType, EvaluationResult, Rule, RuleResult, SourceEvent,
};
use serde_json::json;

#[test]
fn audit_id_matches_java_format() {
    // Java: {topic}:{partition}:{offset}:{ruleId}
    assert_eq!(
        audit_id("source-events", 3, 42, "rule-7"),
        "source-events:3:42:rule-7"
    );
}

#[test]
fn audit_type_serializes_screaming_snake() {
    assert_eq!(
        serde_json::to_string(&AuditType::Matched).unwrap(),
        "\"MATCHED\""
    );
    assert_eq!(
        serde_json::to_string(&AuditType::Unmatched).unwrap(),
        "\"UNMATCHED\""
    );
    assert_eq!(
        serde_json::to_string(&AuditType::Errored).unwrap(),
        "\"ERRORED\""
    );
    assert_eq!(
        serde_json::from_str::<AuditType>("\"ERRORED\"").unwrap(),
        AuditType::Errored
    );
}

#[test]
fn audit_record_uses_camel_case_keys() {
    let rec = AuditRecord {
        audit_id: audit_id("source-events", 0, 5, "r1"),
        rule_id: "r1".into(),
        schema_version: 1,
        audit_type: AuditType::Matched,
        reason: None,
        source_event: "{\"amount\":1500}".into(),
        routed_event: Some("{\"amount\":1500}".into()),
        source_topic: "source-events".into(),
        partition: 0,
        offset: 5,
        timestamp: Utc.with_ymd_and_hms(2026, 6, 17, 1, 2, 3).unwrap(),
        parse_time_nano: 100,
        eval_time_nano: 200,
        total_time_nano: 300,
    };
    let v: serde_json::Value = serde_json::to_value(&rec).unwrap();
    for key in [
        "auditId",
        "ruleId",
        "schemaVersion",
        "auditType",
        "sourceEvent",
        "routedEvent",
        "sourceTopic",
        "partition",
        "offset",
        "timestamp",
        "parseTimeNano",
        "evalTimeNano",
        "totalTimeNano",
    ] {
        assert!(v.get(key).is_some(), "missing camelCase key: {key}");
    }
    assert_eq!(v["auditId"], "source-events:0:5:r1");
    assert_eq!(v["auditType"], "MATCHED");

    // round-trips
    let back: AuditRecord = serde_json::from_value(v).unwrap();
    assert_eq!(back, rec);
}

#[test]
fn source_event_parses_demo_payload() {
    // A representative DemoMessages.generate() payload from the Java repo.
    let raw = r#"{"type":"order_event","amount":1500,"region":"US","tier":"premium","flagged":true,"metadata":{"source":"web","priority":1,"tax_rate":0.05},"order":{"items":[{"id":"it-0","price":500,"tags":["high-value"]}],"total_items":1},"timestamp":"2026-06-17T00:00:00Z"}"#;
    let ev = SourceEvent::from_kafka("source-events", 1, 99, 1_750_000_000_000, raw).unwrap();
    assert_eq!(ev.topic, "source-events");
    assert_eq!(ev.partition, 1);
    assert_eq!(ev.offset, 99);
    assert_eq!(ev.raw, raw); // byte-identical raw preserved for audit.sourceEvent
    assert_eq!(ev.payload["amount"], 1500);
    assert_eq!(ev.payload["metadata"]["source"], "web");
}

#[test]
fn rule_round_trips() {
    let rule = Rule {
        id: "r1".into(),
        description: "high value orders".into(),
        expression: "event.amount > 1000".into(),
        target_topic: "target-events".into(),
        enabled: true,
        version: 3,
        updated_at: Utc.with_ymd_and_hms(2026, 6, 17, 0, 0, 0).unwrap(),
    };
    let s = serde_json::to_string(&rule).unwrap();
    assert_eq!(serde_json::from_str::<Rule>(&s).unwrap(), rule);
    // camelCase target field
    assert!(serde_json::to_value(&rule)
        .unwrap()
        .get("targetTopic")
        .is_some());
}

#[test]
fn evaluation_result_verdict_precedence() {
    let r = |t| RuleResult {
        rule_id: "x".into(),
        audit_type: t,
        reason: None,
    };

    // any MATCHED -> MATCHED, regardless of errors
    let res = EvaluationResult::new(vec![r(AuditType::Errored), r(AuditType::Matched)]);
    assert!(res.matched());
    assert_eq!(res.verdict(), AuditType::Matched);

    // no match, any ERRORED -> ERRORED
    let res = EvaluationResult::new(vec![r(AuditType::Unmatched), r(AuditType::Errored)]);
    assert_eq!(res.verdict(), AuditType::Errored);

    // all unmatched -> UNMATCHED
    let res = EvaluationResult::new(vec![r(AuditType::Unmatched)]);
    assert_eq!(res.verdict(), AuditType::Unmatched);

    // empty -> UNMATCHED
    assert_eq!(
        EvaluationResult::new(vec![]).verdict(),
        AuditType::Unmatched
    );
}

#[test]
fn json_value_passthrough() {
    // sanity: serde_json round-trips a nested value used as event payload
    let v = json!({"a":[1,2,3],"b":{"c":true}});
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&v.to_string()).unwrap(),
        v
    );
}
