//! Compile a [`Rule`] into a reusable CEL [`Program`]. Parsing happens once at
//! compile time; evaluation reuses the compiled program on the hot path.

use std::panic::{catch_unwind, AssertUnwindSafe};

use cel_interpreter::Program;
use rules_core::Rule;
use thiserror::Error;

/// A rule whose CEL expression has been parsed once into an executable program.
/// Carries the routing target so the pipeline (S6) can route matched events.
#[derive(Debug)]
pub struct CompiledRule {
    pub id: String,
    pub target_topic: String,
    pub expression: String,
    pub(crate) program: Program,
}

/// A rule expression that failed to compile. Surfaced at load time, never
/// per-event. `message` captures either a clean parse error or a parser panic
/// (the CEL parser can `unreachable!()` on some malformed input).
#[derive(Debug, Error)]
#[error("failed to compile rule '{rule_id}': {message}")]
pub struct CompileError {
    pub rule_id: String,
    pub message: String,
}

/// Parse and compile a rule's CEL expression. Total function: invalid input —
/// whether a clean parse error or a parser panic — returns [`CompileError`]
/// rather than propagating, so a bad rule can never crash the loader (S7).
pub fn compile(rule: &Rule) -> Result<CompiledRule, CompileError> {
    let err = |message: String| CompileError {
        rule_id: rule.id.clone(),
        message,
    };

    // The CEL parser (antlr4rust) can panic on some malformed expressions
    // instead of returning Err; rule text is user input, so guard the boundary.
    let program = match catch_unwind(AssertUnwindSafe(|| Program::compile(&rule.expression))) {
        Ok(Ok(program)) => program,
        Ok(Err(e)) => return Err(err(e.to_string())),
        Err(_) => return Err(err("parser panicked on malformed expression".to_string())),
    };

    Ok(CompiledRule {
        id: rule.id.clone(),
        target_topic: rule.target_topic.clone(),
        expression: rule.expression.clone(),
        program,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rules_core::Rule;

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

    #[test]
    fn compile_valid_expression_succeeds() {
        let rule = make_rule("r1", "true");
        let result = compile(&rule);
        assert!(result.is_ok());
        let compiled = result.unwrap();
        assert_eq!(compiled.id, "r1");
        assert_eq!(compiled.target_topic, "out");
        assert_eq!(compiled.expression, "true");
    }

    #[test]
    fn compile_invalid_expression_returns_error_with_rule_id() {
        let rule = make_rule("bad-rule", "!!! @@ invalid CEL expression @@@ !!!");
        let result = compile(&rule);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.rule_id, "bad-rule");
        assert!(!err.message.is_empty());
    }

    #[test]
    fn compile_error_display_contains_rule_id_and_message() {
        let rule = make_rule("r-err", "??not valid");
        let err = compile(&rule).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("r-err"), "got: {msg}");
    }
}
