//! eval crate — compile and evaluate CEL rules against event payloads.
//!
//! Replaces the Java SpEL evaluator (`eval/RuleEvaluator.java`). The event JSON
//! is bound as the CEL activation variable `event`; a rule expression must
//! evaluate to a boolean. Outcomes mirror the Java taxonomy: MATCHED / UNMATCHED
//! / ERRORED. Translation follows the **Version-A contract** (see README): a
//! missing field in a relational op errors (ERRORED), matching CEL semantics
//! rather than SpEL's null handling.
//!
//! Zero I/O dependencies. See plans/rust-rules-engine-rebuild.md (S2).

mod compiled;
mod evaluate;

pub use compiled::{compile, CompileError, CompiledRule};
pub use evaluate::{evaluate, RuleOutcome};
