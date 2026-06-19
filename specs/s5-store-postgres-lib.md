Generate the COMPLETE contents of `crates/store-postgres/src/lib.rs`: a Rust library
for a transactional Postgres rule store with LISTEN/NOTIFY hot-reload.

OUTPUT RULES (critical):
- Output EXACTLY ONE fenced ```rust code block and nothing else. No prose.
- Must compile with `cargo build -p store-postgres`.
- Do NOT redefine `rules_core::Rule` — import and use it.
- Do NOT use the compile-time macros `sqlx::query!` / `sqlx::query_as!` (they need a live
  DB at build time and would break CI). Use ONLY the runtime functions `sqlx::query`,
  `sqlx::query_as`, `sqlx::raw_sql` with `.bind(..)`.

DEPENDENCIES (use these exact APIs; do not invent others):
- sqlx 0.9 (postgres): `sqlx::PgPool`, `sqlx::postgres::PgPoolOptions`,
  `sqlx::postgres::PgListener`, `sqlx::FromRow`, `sqlx::query`, `sqlx::query_as`, `sqlx::raw_sql`.
  - `PgPoolOptions::new().connect(url).await -> Result<PgPool, sqlx::Error>`.
  - `sqlx::raw_sql(sql).execute(&pool).await` runs multi-statement SQL.
  - `sqlx::query_as::<_, RuleRow>("... $1 ...").bind(x).fetch_all(&pool).await -> Result<Vec<RuleRow>, sqlx::Error>`;
    also `.fetch_one(&pool)` and `.fetch_optional(&pool)`.
  - `sqlx::query("DELETE ... $1").bind(x).execute(&pool).await -> Result<PgQueryResult, _>`; `.rows_affected() -> u64`.
  - `PgListener::connect_with(&pool).await -> Result<PgListener, _>`; `listener.listen("rules_changed").await`;
    `listener.recv().await -> Result<PgNotification, _>`; `PgNotification::payload() -> &str`.
- `rules_core::Rule` fields: id:String, description:String, expression:String, target_topic:String,
  enabled:bool, version:i64, updated_at:chrono::DateTime<chrono::Utc>.
- `thiserror`, `chrono::{DateTime, Utc}`.

PRODUCE these public items exactly:

1. `pub enum Error` (derive Debug, thiserror::Error), one variant
   `Db(#[from] sqlx::Error)`, message "postgres error: {0}". Define `type Result<T> = std::result::Result<T, Error>;` (private alias is fine).

2. `pub async fn connect(url: &str) -> Result<PgPool>` via PgPoolOptions.
   `pub const MIGRATION_RULES: &str = include_str!("../../../migrations/postgres/0001_rules.sql");`
   `pub async fn run_migrations(pool: &PgPool) -> Result<()>` running MIGRATION_RULES via raw_sql.

3. `#[derive(Debug, Clone, FromRow)] struct RuleRow` with fields matching columns: id:String,
   description:String, expression:String, target_topic:String, enabled:bool, version:i64,
   updated_at:DateTime<Utc>. Add `impl From<RuleRow> for rules_core::Rule` (move each field).
   Use a shared const `const RULE_COLS: &str = "id, description, expression, target_topic, enabled, version, updated_at";`

4. `#[derive(Debug, Clone)] pub struct RuleInput { pub description: String, pub expression: String,
   pub target_topic: String, pub enabled: bool }`.

5. `#[derive(Clone)] pub struct RuleRepository { pool: PgPool }` with:
   - `pub fn new(pool: PgPool) -> Self`.
   - `pub async fn list(&self) -> Result<Vec<Rule>>`: `SELECT {RULE_COLS} FROM rules ORDER BY id`, map RuleRow->Rule.
   - `pub async fn get(&self, id: &str) -> Result<Option<Rule>>`: WHERE id = $1, fetch_optional.
   - `pub async fn create(&self, input: &RuleInput) -> Result<Rule>`:
     `INSERT INTO rules (description, expression, target_topic, enabled) VALUES ($1,$2,$3,$4) RETURNING {RULE_COLS}`, fetch_one.
   - `pub async fn update(&self, id: &str, input: &RuleInput) -> Result<Option<Rule>>`:
     `UPDATE rules SET description=$2, expression=$3, target_topic=$4, enabled=$5, version=version+1, updated_at=now() WHERE id=$1 RETURNING {RULE_COLS}`, fetch_optional.
   - `pub async fn delete(&self, id: &str) -> Result<bool>`: DELETE WHERE id=$1, return rows_affected()>0.
   (Build SELECT/RETURNING strings with `format!("... {RULE_COLS} ...")`. Bind id/inputs in order.)

6. `pub struct RuleChangeListener { inner: PgListener }` with:
   - `pub async fn connect(pool: &PgPool) -> Result<Self>`: connect_with, then `listen("rules_changed")`.
   - `pub async fn recv(&mut self) -> Result<String>`: `Ok(self.inner.recv().await?.payload().to_string())`.

7. `pub async fn seed_default_rules(repo: &RuleRepository) -> Result<usize>`: if `repo.list().await?` is
   non-empty, return Ok(0). Otherwise create these 3 rules (all target_topic "target-events", enabled true,
   description as given) and return the count (3):
   - "high value US standard order": `event.amount > 43618 && event.region == "US" && event.metadata.source == "web" && event.metadata.priority <= 5 && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.price > 678)`
   - "APAC gold mobile high tax": `event.amount > 38620 && event.region == "APAC" && event.metadata.source == "mobile" && event.metadata.priority <= 4 && event.tier == "gold" && event.metadata.tax_rate >= 0.16`
   - "LATAM premium partner 202x": `event.amount > 18508 && event.region == "LATAM" && event.metadata.source == "partner" && event.metadata.priority <= 5 && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("202")`

Import `rules_core::Rule`. Keep it focused. Module doc: "store-postgres — rule CRUD + LISTEN/NOTIFY hot-reload (S5)".
