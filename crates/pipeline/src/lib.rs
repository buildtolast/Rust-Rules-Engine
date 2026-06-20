//! pipeline crate — consume→eval→route+audit, EOS. See plans/rust-rules-engine-rebuild.md (S6, S7).

pub mod consumer;
pub mod metrics;
pub mod rule_cache;
pub use consumer::{run, PipelineConfig, PipelineError};
pub use metrics::PipelineCounters;
pub use rule_cache::{CacheError, RuleCache, watch_and_reload};
