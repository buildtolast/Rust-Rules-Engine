# Spec: crates/sre/src/store.rs

Generate EXACTLY ONE fenced Rust code block. No commentary outside it.
Output path: crates/sre/src/store.rs

## Context

This file is part of the `sre` crate (crates/sre). The crate uses:
- `clickhouse = { version = "0.15.1", features = ["inserter", "chrono"] }`
- `chrono` with serde feature
- `thiserror = "1"`
- `serde` with derive feature

## What to implement

A `SreStore` type that writes `SreObservation` rows to ClickHouse via an `Inserter`.

### SreObservation struct

```rust
#[derive(clickhouse::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SreObservation {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub observed_at:      chrono::DateTime<chrono::Utc>,
    pub container_name:   String,
    pub severity:         String,
    pub category:         String,
    pub finding:          String,
    pub proposed_fix:     String,
    pub log_window_hash:  String,
    pub log_snippet:      String,
}
```

### SreStore struct

```rust
pub struct SreStore {
    inserter: clickhouse::inserter::Inserter<SreObservation>,
}
```

### Public API

```rust
impl SreStore {
    /// Create a new SreStore wrapping an Inserter with max_rows=100 and period=5s.
    pub fn new(client: &clickhouse::Client) -> Result<Self, SreStoreError>

    /// Buffer one observation for insertion.
    pub async fn write(&mut self, obs: &SreObservation) -> Result<(), SreStoreError>

    /// Force-flush pending rows to ClickHouse.
    pub async fn commit(&mut self) -> Result<(), SreStoreError>
}
```

### Error type

```rust
#[derive(thiserror::Error, Debug)]
pub enum SreStoreError {
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] clickhouse::error::Error),
}
```

## Critical constraints

- DO NOT use `clickhouse::error::Error` as a local name — use fully-qualified `clickhouse::error::Error` in the `From` impl if needed, or use `#[from]` in thiserror (which handles it correctly).
- `Inserter::new(client, "ruleaudit.sre_observations")` creates the inserter.
  Then call `.with_max_rows(100).with_period(Some(std::time::Duration::from_secs(5)))`.
- `write` calls `self.inserter.write(obs).await` then `self.inserter.commit().await` (returns `Quantities`; ignore the value with `let _ = ...`). Note: `Inserter::write` takes `&T` not `T`.
- `commit` calls `self.inserter.end().await` (returns `Quantities`; ignore with `let _ = ...`).
- Use `#[allow(dead_code)]` on any fields that the compiler warns about.
- No main function. No mod declarations. Just the structs/impls above.
