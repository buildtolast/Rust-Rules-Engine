//! pipeline crate — consume→eval→route+audit, EOS. See plans/rust-rules-engine-rebuild.md (S6, S7).

pub mod rule_cache;
pub use rule_cache::{CacheError, RuleCache, watch_and_reload};
