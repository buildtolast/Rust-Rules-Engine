//! Evaluate a compiled rule against an event. Errors (binding, runtime, or a
//! non-boolean result) become an ERRORED outcome with a reason — mirroring the
//! Java evaluator, which catches RuntimeException and records ERRORED.

use std::time::Instant;

use cel_interpreter::{Context, Value};
use rules_core::{AuditType, RuleResult, SourceEvent};

use crate::compiled::CompiledRule;

/// The result of evaluating one rule against one event, plus the wall-clock
/// evaluation time in nanoseconds (feeds `AuditRecord.eval_time_nano`).
pub struct RuleOutcome {
    pub result: RuleResult,
    pub eval_time_nano: u64,
}

/// Evaluate `compiled` against `event`. Never returns an error: failures are
/// reported as an ERRORED [`RuleResult`].
pub fn evaluate(compiled: &CompiledRule, event: &SourceEvent) -> RuleOutcome {
    let start = Instant::now();
    let audit_type_and_reason = run(compiled, event);
    let eval_time_nano = start.elapsed().as_nanos() as u64;

    let (audit_type, reason) = audit_type_and_reason;
    RuleOutcome {
        result: RuleResult {
            rule_id: compiled.id.clone(),
            audit_type,
            reason,
        },
        eval_time_nano,
    }
}

fn run(compiled: &CompiledRule, event: &SourceEvent) -> (AuditType, Option<String>) {
    let mut context = Context::default();
    if let Err(e) = context.add_variable("event", &event.payload) {
        return (
            AuditType::Errored,
            Some(format!("failed to bind event payload: {e}")),
        );
    }

    match compiled.program.execute(&context) {
        Ok(Value::Bool(true)) => (AuditType::Matched, None),
        Ok(Value::Bool(false)) => (
            AuditType::Unmatched,
            Some(format!("condition not met: {}", compiled.expression)),
        ),
        Ok(_) => (
            AuditType::Errored,
            Some(format!(
                "expression did not return a boolean: {}",
                compiled.expression
            )),
        ),
        Err(e) => (AuditType::Errored, Some(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled::compile;
    use rules_core::{Rule, SourceEvent};

    fn make_rule(id: &str, expression: &str) -> Rule {
        Rule {
            id: id.to_owned(),
            description: String::new(),
            expression: expression.to_owned(),
            target_topic: "out".to_owned(),
            enabled: true,
            version: 1,
            updated_at: chrono::Utc::now(),
        }
    }

    fn make_event(raw: &str) -> SourceEvent {
        SourceEvent::from_kafka("topic", 0, 0, 0, raw).unwrap()
    }

    fn compiled(expression: &str) -> crate::compiled::CompiledRule {
        let rule = make_rule("test-rule", expression);
        compile(&rule).unwrap()
    }

    #[test]
    fn true_expression_returns_matched() {
        let outcome = evaluate(&compiled("true"), &make_event("{}"));
        assert_eq!(outcome.result.audit_type, AuditType::Matched);
        assert!(outcome.result.reason.is_none());
    }

    #[test]
    fn false_expression_returns_unmatched_with_reason() {
        let outcome = evaluate(&compiled("false"), &make_event("{}"));
        assert_eq!(outcome.result.audit_type, AuditType::Unmatched);
        let reason = outcome.result.reason.unwrap();
        assert!(reason.contains("condition not met"), "got: {reason}");
    }

    #[test]
    fn non_boolean_expression_returns_errored_with_reason() {
        let outcome = evaluate(&compiled("1 + 1"), &make_event("{}"));
        assert_eq!(outcome.result.audit_type, AuditType::Errored);
        let reason = outcome.result.reason.unwrap();
        assert!(reason.contains("did not return a boolean"), "got: {reason}");
    }

    #[test]
    fn field_match_returns_matched() {
        let outcome = evaluate(&compiled("event.x == 42"), &make_event(r#"{"x": 42}"#));
        assert_eq!(outcome.result.audit_type, AuditType::Matched);
        assert!(outcome.result.reason.is_none());
    }

    #[test]
    fn field_mismatch_returns_unmatched() {
        let outcome = evaluate(&compiled("event.x == 42"), &make_event(r#"{"x": 99}"#));
        assert_eq!(outcome.result.audit_type, AuditType::Unmatched);
        assert!(outcome.result.reason.is_some());
    }

    #[test]
    fn eval_time_nano_is_nonzero() {
        let outcome = evaluate(&compiled("true"), &make_event("{}"));
        assert!(outcome.eval_time_nano > 0);
    }

    #[test]
    fn rule_id_propagated_to_result() {
        let rule = make_rule("my-rule-id", "true");
        let compiled = compile(&rule).unwrap();
        let outcome = evaluate(&compiled, &make_event("{}"));
        assert_eq!(outcome.result.rule_id, "my-rule-id");
    }
}
