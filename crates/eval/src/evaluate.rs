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
