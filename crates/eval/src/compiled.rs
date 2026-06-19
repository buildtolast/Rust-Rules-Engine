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
