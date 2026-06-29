use std::sync::Arc;

use eval::compile;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("postgres error: {0}")]
    Postgres(#[from] store_postgres::Error),
}

/// Lock-free cache of compiled rules, hot-swappable on Postgres NOTIFY.
///
/// Cheap to clone — all clones share the same live ArcSwap pointer.
#[derive(Clone)]
pub struct RuleCache {
    inner: Arc<arc_swap::ArcSwap<Vec<eval::CompiledRule>>>,
}

impl RuleCache {
    /// Load all enabled rules from Postgres, compile them, and return a populated cache.
    /// Rules that fail to compile are skipped with a warning; they do not abort the load.
    pub async fn load(repo: &store_postgres::RuleRepository) -> Result<Self, CacheError> {
        let compiled = compile_enabled(repo).await?;
        Ok(Self { inner: Arc::new(arc_swap::ArcSwap::from_pointee(compiled)) })
    }

    /// Construct a cache pre-loaded with the given compiled rules.
    /// Intended for tests and tooling — production code should use [`RuleCache::load`].
    pub fn new(rules: Vec<eval::CompiledRule>) -> Self {
        Self { inner: Arc::new(arc_swap::ArcSwap::from_pointee(rules)) }
    }

    /// Atomically replace the rule set. The next call to [`RuleCache::get`] will
    /// see the new rules. Intended for tests and the hot-reload path.
    pub fn swap(&self, rules: Vec<eval::CompiledRule>) {
        self.inner.store(Arc::new(rules));
    }

    /// Return a snapshot of the current compiled rule set.
    /// Lock-free atomic load; callers hold the Arc for one event batch then drop it.
    pub fn get(&self) -> Arc<Vec<eval::CompiledRule>> {
        self.inner.load_full()
    }
}

/// Background task: waits for `rules_changed` NOTIFY, reloads + swaps the cache.
/// Runs forever; spawn with `tokio::spawn`. Returns `Err` only if the Postgres
/// connection is lost.
pub async fn watch_and_reload(cache: RuleCache,
                              repo: store_postgres::RuleRepository,
                              mut listener: store_postgres::RuleChangeListener)
                              -> Result<(), CacheError> {
    loop {
        listener.recv().await?;
        tracing::info!("rules_changed NOTIFY received, reloading cache");

        let compiled = compile_enabled(&repo).await?;
        let count = compiled.len();
        cache.inner.store(Arc::new(compiled));
        tracing::info!("rule cache reloaded: {} active rules", count);
    }
}

/// Fetch all enabled rules from Postgres and compile each one.
/// Compile errors are warnings, not failures.
async fn compile_enabled(repo: &store_postgres::RuleRepository)
                         -> Result<Vec<eval::CompiledRule>, CacheError> {
    let rules = repo.list().await?;
    let compiled = rules.into_iter()
                        .filter(|r| r.enabled)
                        .filter_map(|r| match compile(&r) {
                            Ok(c) => Some(c),
                            Err(e) => {
                                tracing::warn!("skipping rule {}: {}", r.id, e);
                                None
                            }
                        })
                        .collect();
    Ok(compiled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rules_core::Rule;

    fn make_rule(id: &str, expression: &str) -> eval::CompiledRule {
        let rule = Rule { id: id.to_string(),
                          description: String::new(),
                          expression: expression.to_string(),
                          target_topic: "target-events".to_string(),
                          enabled: true,
                          version: 1,
                          updated_at: chrono::Utc::now() };
        eval::compile(&rule).expect("test rule should compile")
    }

    #[test]
    fn test_cache_get_returns_snapshot() {
        let cache = RuleCache::new(vec![]);
        let snapshot = cache.get();
        assert_eq!(snapshot.len(),
                   0,
                   "empty cache should return empty snapshot");
    }

    #[test]
    fn test_cache_atomic_swap() {
        let cache = RuleCache::new(vec![]);
        assert_eq!(cache.get().len(), 0);

        let rule = make_rule("rule-1", "true");
        cache.swap(vec![rule]);

        assert_eq!(cache.get().len(),
                   1,
                   "after swap, cache should reflect new rule set");
        assert_eq!(cache.get()[0].id, "rule-1");
    }

    #[test]
    fn test_cache_clone_shares_state() {
        let original = RuleCache::new(vec![]);
        let cloned = original.clone();

        assert_eq!(original.get().len(), 0);
        assert_eq!(cloned.get().len(), 0);

        // Swap on original — clone must see the same updated state.
        let rule = make_rule("rule-shared", "1 == 1");
        original.swap(vec![rule]);

        assert_eq!(cloned.get().len(),
                   1,
                   "clone should share ArcSwap pointer and see swapped rules");
        assert_eq!(cloned.get()[0].id, "rule-shared");
    }

    #[test]
    fn test_cache_swap_replaces_entire_rule_set() {
        let r1 = make_rule("r1", "true");
        let cache = RuleCache::new(vec![r1]);
        assert_eq!(cache.get().len(), 1);

        // Replace with two rules.
        let r2 = make_rule("r2", "true");
        let r3 = make_rule("r3", "false");
        cache.swap(vec![r2, r3]);

        let snapshot = cache.get();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].id, "r2");
        assert_eq!(snapshot[1].id, "r3");
    }
}
