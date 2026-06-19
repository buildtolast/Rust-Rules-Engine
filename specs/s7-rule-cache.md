# Spec: S7 — RuleCache (crates/pipeline/src/rule_cache.rs)

Generate the complete contents of `crates/pipeline/src/rule_cache.rs`.

Output EXACTLY ONE fenced Rust code block. No prose before or after it.

---

## Context

This file implements the rule cache with lock-free hot-reload for the Rust Rules Engine.

### Existing types you MUST NOT redefine — reference only

From crate `eval` (imported as `eval`):
- `eval::CompiledRule` — struct with fields `id: String`, `target_topic: String`, `expression: String`, and private `program`. Already defined in `crates/eval/src/compiled.rs`.
- `eval::compile(rule: &rules_core::Rule) -> Result<eval::CompiledRule, eval::CompileError>` — total function, never panics.

From crate `store_postgres` (imported as `store_postgres`):
- `store_postgres::RuleRepository` — has `async fn list(&self) -> Result<Vec<rules_core::Rule>, store_postgres::Error>`. Returns ALL rules (enabled and disabled).
- `store_postgres::RuleChangeListener` — has `async fn recv(&mut self) -> Result<String, store_postgres::Error>`. Blocks until a NOTIFY arrives on channel `rules_changed`. The payload is the changed rule's id (not needed for full reload).

From crate `rules_core` (imported as `rules_core`):
- `rules_core::Rule` — struct with field `enabled: bool` among others.

---

## What to generate

### Error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("postgres error: {0}")]
    Postgres(#[from] store_postgres::Error),
}
```

### RuleCache struct

```rust
#[derive(Clone)]
pub struct RuleCache {
    inner: Arc<arc_swap::ArcSwap<Arc<Vec<eval::CompiledRule>>>>,
}
```

The `ArcSwap` holds an `Arc<Vec<CompiledRule>>`. Cloning `RuleCache` is cheap (Arc clone). All clones share the same live pointer.

### RuleCache::load

```rust
pub async fn load(repo: &store_postgres::RuleRepository) -> Result<Self, CacheError>
```

- Call `repo.list().await?` to get all rules.
- Filter to only rules where `rule.enabled == true`.
- For each enabled rule, call `eval::compile(&rule)`.
  - On `Ok(compiled)` → keep it.
  - On `Err(e)` → log a warning with `tracing::warn!("skipping rule {}: {}", rule.id, e)` and skip it. Do NOT return an error — a bad rule must never prevent the cache from loading.
- Collect the compiled rules into a `Vec<eval::CompiledRule>`.
- Return `Ok(RuleCache { inner: Arc::new(arc_swap::ArcSwap::from_pointee(compiled_rules)) })`.

### RuleCache::get

```rust
pub fn get(&self) -> Arc<Vec<eval::CompiledRule>>
```

- Return `self.inner.load_full()`. This is a lock-free atomic load returning an `Arc<Vec<CompiledRule>>` snapshot. Callers hold the snapshot for the duration of one event evaluation and then drop it.

### watch_and_reload

```rust
pub async fn watch_and_reload(
    cache: RuleCache,
    repo: store_postgres::RuleRepository,
    mut listener: store_postgres::RuleChangeListener,
) -> Result<(), CacheError>
```

Runs forever. Intended to be spawned as a `tokio::spawn` task by the caller.

Loop body:
1. `listener.recv().await?` — blocks until a NOTIFY arrives (payload is a rule id, ignore the value).
2. Log the reload with `tracing::info!("rules_changed NOTIFY received, reloading cache")`.
3. Call `repo.list().await?` to fetch all rules.
4. Filter to `enabled == true`, compile each (same skip-on-error logic as `load`).
5. Wrap compiled vec in `Arc::new(...)`.
6. Call `cache.inner.store(new_arc)` to atomically swap the snapshot.
7. Log `tracing::info!("rule cache reloaded: {} active rules", count)`.
8. Loop back to step 1.

If `listener.recv()` returns an error, propagate it as `CacheError::Postgres(e)` — this signals the Postgres connection was lost and the caller should restart.

---

## Required imports

```rust
use std::sync::Arc;
use arc_swap::ArcSwap;
use tracing;
```

Do NOT import `eval::CompileError` by name — it is only used in the `Err(e)` match arm, where `e` suffices.

---

## File structure

The output file must have:
1. The `use` imports
2. `CacheError` enum
3. `RuleCache` struct + `impl RuleCache` block (`load`, `get`)
4. `watch_and_reload` free function

No other top-level items. No `mod` declarations. No `main`. No test module.
