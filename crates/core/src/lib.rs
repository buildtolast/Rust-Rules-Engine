//! core crate — domain types shared across the engine. Zero I/O dependencies
//! (no kafka/clickhouse/postgres/axum). Mirrors the Java
//! `Spring-Kafka-Stream-Rules` contract. See plans/rust-rules-engine-rebuild.md (S1).

mod audit;
mod eval_result;
mod event;
mod rule;

pub use audit::{audit_id, AuditRecord, AuditType};
pub use eval_result::{EvaluationResult, RuleResult};
pub use event::SourceEvent;
pub use rule::Rule;
