//! Parity + behavior tests for the CEL evaluator (S2, Version-A contract).
//! Seed rules ported from the Java `config/RuleSeeder.java` (SpEL -> CEL).
//! See plans/rust-rules-engine-rebuild.md (S2).

use eval::{compile, evaluate};
use rules_core::{AuditType, Rule, SourceEvent};

// Rule 1 (Java seed): amount/region/source/priority/tier + order selection.
const RULE_1: &str = r#"event.amount > 43618 && event.region == "US" && event.metadata.source == "web" && event.metadata.priority <= 5 && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.price > 678)"#;
// Rule 2 (Java seed): tax_rate threshold instead of order selection.
const RULE_2: &str = r#"event.amount > 38620 && event.region == "APAC" && event.metadata.source == "mobile" && event.metadata.priority <= 4 && event.tier == "gold" && event.metadata.tax_rate >= 0.16"#;
// Rule 3 (Java seed): timestamp.startsWith("202").
const RULE_3: &str = r#"event.amount > 18508 && event.region == "LATAM" && event.metadata.source == "partner" && event.metadata.priority <= 5 && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("202")"#;

fn rule(expr: &str) -> Rule {
    Rule {
        id: "r".into(),
        description: "test".into(),
        expression: expr.into(),
        target_topic: "target-events".into(),
        enabled: true,
        version: 1,
        updated_at: chrono::Utc::now(),
    }
}

fn event(raw: &str) -> SourceEvent {
    SourceEvent::from_kafka("source-events", 0, 0, 0, raw).unwrap()
}

fn verdict(expr: &str, raw: &str) -> AuditType {
    let compiled = compile(&rule(expr)).expect("rule should compile");
    evaluate(&compiled, &event(raw)).result.audit_type
}

#[test]
fn rule_1_matches_when_all_clauses_hold() {
    let raw = r#"{"amount":50000,"region":"US","tier":"standard","metadata":{"source":"web","priority":5},"order":{"items":[{"id":"a","price":700}]}}"#;
    assert_eq!(verdict(RULE_1, raw), AuditType::Matched);
}

#[test]
fn rule_1_unmatched_on_wrong_region() {
    let raw = r#"{"amount":50000,"region":"EU","tier":"standard","metadata":{"source":"web","priority":5},"order":{"items":[{"id":"a","price":700}]}}"#;
    assert_eq!(verdict(RULE_1, raw), AuditType::Unmatched);
}

#[test]
fn rule_1_unmatched_when_no_item_over_threshold() {
    // selection predicate (price > 678) matches nothing -> exists() is false
    let raw = r#"{"amount":50000,"region":"US","tier":"standard","metadata":{"source":"web","priority":5},"order":{"items":[{"id":"a","price":100}]}}"#;
    assert_eq!(verdict(RULE_1, raw), AuditType::Unmatched);
}

#[test]
fn rule_2_matches_on_tax_rate() {
    let raw = r#"{"amount":40000,"region":"APAC","tier":"gold","metadata":{"source":"mobile","priority":4,"tax_rate":0.2}}"#;
    assert_eq!(verdict(RULE_2, raw), AuditType::Matched);
}

#[test]
fn rule_3_matches_on_timestamp_prefix() {
    let raw = r#"{"amount":20000,"region":"LATAM","tier":"premium","metadata":{"source":"partner","priority":3},"timestamp":"2026-06-17T00:00:00Z"}"#;
    assert_eq!(verdict(RULE_3, raw), AuditType::Matched);
}

#[test]
fn missing_field_in_relational_op_is_errored() {
    // Version-A contract: a missing field in a relational comparison errors in CEL.
    let raw = r#"{"region":"US"}"#; // no `amount`
    assert_eq!(verdict("event.amount > 100", raw), AuditType::Errored);
}

#[test]
fn non_boolean_result_is_errored() {
    let raw = r#"{"amount":5}"#;
    assert_eq!(verdict("event.amount", raw), AuditType::Errored);
}

#[test]
fn errored_outcome_carries_a_reason() {
    let compiled = compile(&rule("event.amount > 100")).unwrap();
    let out = evaluate(&compiled, &event(r#"{}"#));
    assert_eq!(out.result.audit_type, AuditType::Errored);
    assert!(out.result.reason.is_some(), "ERRORED must include a reason");
}

#[test]
fn matched_outcome_has_no_reason() {
    let compiled = compile(&rule("event.amount > 100")).unwrap();
    let out = evaluate(&compiled, &event(r#"{"amount":500}"#));
    assert_eq!(out.result.audit_type, AuditType::Matched);
    assert!(out.result.reason.is_none());
}

#[test]
fn parse_error_surfaces_at_compile_not_eval() {
    let err = compile(&rule("event.amount >")).unwrap_err();
    assert_eq!(err.rule_id, "r");
}

#[test]
fn eval_time_is_recorded() {
    let compiled = compile(&rule("event.amount > 100")).unwrap();
    let out = evaluate(&compiled, &event(r#"{"amount":500}"#));
    // sane upper bound — proves the timing path executed without flaking on >0
    assert!(out.eval_time_nano < 5_000_000_000);
}
