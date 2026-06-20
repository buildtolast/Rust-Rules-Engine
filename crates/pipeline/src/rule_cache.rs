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
        Ok(Self {
            inner: Arc::new(arc_swap::ArcSwap::from_pointee(compiled)),
        })
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
pub async fn watch_and_reload(
    cache: RuleCache,
    repo: store_postgres::RuleRepository,
    mut listener: store_postgres::RuleChangeListener,
) -> Result<(), CacheError> {
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
async fn compile_enabled(
    repo: &store_postgres::RuleRepository,
) -> Result<Vec<eval::CompiledRule>, CacheError> {
    let rules = repo.list().await?;
    let compiled = rules
        .into_iter()
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
