Generate the COMPLETE contents of `crates/store-clickhouse/src/lib.rs` for a Rust
library that writes audit records to ClickHouse in batches.

OUTPUT RULES (critical):
- Output EXACTLY ONE fenced ```rust code block and nothing else. No prose.
- It must be the entire file. It must compile with `cargo build -p store-clickhouse`.
- Do NOT redefine `rules_core::AuditRecord` or `rules_core::AuditType` — import and use them.
- Match the AuditRow field order to the SQL column order EXACTLY (see below).

DEPENDENCIES (already in Cargo.toml — use these exact APIs, do not invent others):
- clickhouse 0.15: `clickhouse::Client`, `clickhouse::Row` (derive macro),
  `clickhouse::error::Error`, `clickhouse::inserter::Inserter<T>`.
  - `Client::default().with_url(s).with_database(s).with_user(s).with_password(s)` (builder, all take `impl Into<String>`).
  - `client.query(sql).execute().await -> Result<(), clickhouse::error::Error>`.
  - `client.inserter::<AuditRow>("audits") -> Inserter<AuditRow>` (SYNC, infallible).
  - `Inserter::with_max_rows(self, u64) -> Self`, `Inserter::with_period(self, Option<std::time::Duration>) -> Self`.
  - `inserter.write(&row).await -> Result<(), Error>` (takes `&AuditRow`).
  - `inserter.commit().await -> Result<_, Error>`, `inserter.end().await -> Result<_, Error>` (ignore the returned Quantities).
- `rules_core::{AuditRecord, AuditType}`. AuditType variants: `Matched`, `Unmatched`, `Errored`.
  AuditRecord fields: audit_id:String, rule_id:String, schema_version:u32, audit_type:AuditType,
  reason:Option<String>, source_event:String, routed_event:Option<String>, source_topic:String,
  partition:i32, offset:i64, timestamp:chrono::DateTime<chrono::Utc>, parse_time_nano:u64,
  eval_time_nano:u64, total_time_nano:u64.
- serde `Serialize`/`Deserialize` derive, `thiserror`, `chrono::{DateTime, Utc}`.

NAME-COLLISION RULES (the build fails otherwise):
- Do NOT `use clickhouse::error::Error`. Refer to ClickHouse's error type ONLY as the
  fully-qualified `clickhouse::error::Error`. The ONLY `Error` you import is the derive
  macro `thiserror::Error`, and the ONLY `Error` TYPE you define is your own `pub enum Error`.
- `audit_type_str` takes `AuditType` BY VALUE (AuditType is Copy). Call it `audit_type_str(rec.audit_type)`, no `&`.

PRODUCE these public items exactly:

1. `pub enum Error` (derive Debug, thiserror::Error) with one variant
   `ClickHouse(#[from] clickhouse::error::Error)`, message "clickhouse error: {0}".

2. `pub struct ClickHouseConfig` (derive Debug, Clone) with pub fields:
   url:String, database:String, user:String, password:String, batch_max_rows:u64, batch_period_ms:u64.
   `impl Default`: url "http://localhost:8123", database "ruleaudit", user "rules",
   password "rules", batch_max_rows 500, batch_period_ms 200.

3. `pub fn client(cfg: &ClickHouseConfig) -> clickhouse::Client` building from the config fields.

4. `pub const MIGRATION_AUDITS: &str = include_str!("../../../migrations/clickhouse/0001_audits.sql");`
   and `pub async fn run_migrations(client: &Client) -> Result<(), Error>` that executes it
   (idempotent: the SQL is CREATE TABLE IF NOT EXISTS).

5. `#[derive(Debug, Clone, Row, Serialize, Deserialize)] pub struct AuditRow` with fields IN THIS
   ORDER (matching the SQL columns): audit_id:String, rule_id:String, schema_version:u32,
   audit_type:String, reason:String, source_event:String, routed_event:String, source_topic:String,
   partition:i32, offset:i64, then
   `#[serde(with = "clickhouse::serde::chrono::datetime64::millis")] timestamp: DateTime<Utc>`,
   then parse_time_nano:u64, eval_time_nano:u64, total_time_nano:u64.
   - `impl AuditRow { pub fn from_record(rec: &AuditRecord) -> Self }`: clone the String fields;
     `audit_type` = a `&str` from a private `fn audit_type_str(t: AuditType) -> &'static str`
     (Matched->"MATCHED", Unmatched->"UNMATCHED", Errored->"ERRORED") then `.to_string()`;
     `reason` and `routed_event` = `rec.reason.clone().unwrap_or_default()` /
     `rec.routed_event.clone().unwrap_or_default()`.

6. `pub struct AuditWriter { inserter: clickhouse::inserter::Inserter<AuditRow> }` with:
   - `pub fn new(client: &Client, cfg: &ClickHouseConfig) -> Self` building the inserter on table
     "audits" with `.with_max_rows(cfg.batch_max_rows)` and
     `.with_period(Some(std::time::Duration::from_millis(cfg.batch_period_ms)))`.
   - `pub async fn write(&mut self, rec: &AuditRecord) -> Result<(), Error>`: build an AuditRow,
     `self.inserter.write(&row).await?; self.inserter.commit().await?; Ok(())`.
   - `pub async fn end(self) -> Result<(), Error>`: `self.inserter.end().await?; Ok(())`.

Keep it under ~120 lines. Module doc comment: "store-clickhouse — batched audit writer + migrations (S3)".
