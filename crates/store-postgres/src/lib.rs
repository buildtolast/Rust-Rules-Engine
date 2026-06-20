//! store-postgres — rule CRUD + LISTEN/NOTIFY hot-reload (S5)

use chrono::{DateTime, Utc};
use rules_core::Rule;
use sqlx::{
    postgres::{PgListener, PgPool, PgPoolOptions},
    query,
    query_builder::QueryBuilder,
    raw_sql, FromRow,
};
use thiserror::Error;

/// Errors returned by the store-postgres crate.
#[derive(Debug, Error)]
pub enum Error {
    #[error("postgres error: {0}")]
    Db(#[from] sqlx::Error),
}

type Result<T> = std::result::Result<T, Error>;

/// Connect to the Postgres database and return a connection pool.
pub async fn connect(url: &str) -> Result<PgPool> {
    PgPoolOptions::new().connect(url).await.map_err(Error::from)
}

/// SQL migration script for the rules table.
pub const MIGRATION_RULES: &str = include_str!("../../../migrations/postgres/0001_rules.sql");

/// Run the migration script against the provided pool.
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    raw_sql(MIGRATION_RULES).execute(pool).await?;
    Ok(())
}

/// Column names for the rules table, used to construct dynamic SQL.
const RULE_COLS: &str = "id, description, expression, target_topic, enabled, version, updated_at";

/// Database representation of a rule.
#[derive(Debug, Clone, FromRow)]
struct RuleRow {
    id: String,
    description: String,
    expression: String,
    target_topic: String,
    enabled: bool,
    version: i64,
    updated_at: DateTime<Utc>,
}

/// Convert a database row into the domain Rule.
impl From<RuleRow> for Rule {
    fn from(row: RuleRow) -> Self {
        Rule {
            id: row.id,
            description: row.description,
            expression: row.expression,
            target_topic: row.target_topic,
            enabled: row.enabled,
            version: row.version,
            updated_at: row.updated_at,
        }
    }
}

/// Input DTO for creating a new rule.
#[derive(Debug, Clone)]
pub struct RuleInput {
    pub description: String,
    pub expression: String,
    pub target_topic: String,
    pub enabled: bool,
}

/// Repository for performing CRUD operations on rules.
#[derive(Clone)]
pub struct RuleRepository {
    pool: PgPool,
}

impl RuleRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn ping(&self) -> bool {
        sqlx::query("SELECT 1").execute(&self.pool).await.is_ok()
    }

    pub async fn list(&self) -> Result<Vec<Rule>> {
        let mut qb = QueryBuilder::new("SELECT ");
        qb.push(RULE_COLS);
        qb.push(" FROM rules ORDER BY id");
        let rows = qb.build_query_as::<RuleRow>().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Rule::from).collect())
    }

    pub async fn get(&self, id: &str) -> Result<Option<Rule>> {
        let mut qb = QueryBuilder::new("SELECT ");
        qb.push(RULE_COLS);
        qb.push(" FROM rules WHERE id = $1");
        let row = qb
            .build_query_as::<RuleRow>()
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(Rule::from))
    }

    pub async fn create(&self, input: &RuleInput) -> Result<Rule> {
        let mut qb = QueryBuilder::new("INSERT INTO rules (description, expression, target_topic, enabled) VALUES ($1, $2, $3, $4) RETURNING ");
        qb.push(RULE_COLS);
        let row = qb
            .build_query_as::<RuleRow>()
            .bind(&input.description)
            .bind(&input.expression)
            .bind(&input.target_topic)
            .bind(input.enabled)
            .fetch_one(&self.pool)
            .await?;
        Ok(Rule::from(row))
    }

    pub async fn update(&self, id: &str, input: &RuleInput) -> Result<Option<Rule>> {
        let mut qb = QueryBuilder::new("UPDATE rules SET description=$2, expression=$3, target_topic=$4, enabled=$5, version=version+1, updated_at=now() WHERE id=$1 RETURNING ");
        qb.push(RULE_COLS);
        let row = qb
            .build_query_as::<RuleRow>()
            .bind(id)
            .bind(&input.description)
            .bind(&input.expression)
            .bind(&input.target_topic)
            .bind(input.enabled)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(Rule::from))
    }

    pub async fn delete(&self, id: &str) -> Result<bool> {
        let result = query("DELETE FROM rules WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

/// Listener for Postgres LISTEN/NOTIFY events.
pub struct RuleChangeListener {
    inner: PgListener,
}

impl RuleChangeListener {
    pub async fn connect(pool: &PgPool) -> Result<Self> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen("rules_changed").await?;
        Ok(Self { inner: listener })
    }

    pub async fn recv(&mut self) -> Result<String> {
        Ok(self.inner.recv().await?.payload().to_string())
    }
}

/// Seed the database with 100 complex default rules if it is empty.
pub async fn seed_default_rules(repo: &RuleRepository) -> Result<usize> {
    if !repo.list().await?.is_empty() {
        return Ok(0);
    }

    let rules: Vec<RuleInput> = vec![
        // ── US region rules ──────────────────────────────────────────────────
        RuleInput { description: "US web standard high-value with items".into(), expression: r#"event.amount > 43618 && event.region == "US" && event.metadata.source == "web" && event.metadata.priority <= 5 && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.price > 678)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US mobile gold >50k urgent".into(), expression: r#"event.amount > 50000 && event.region == "US" && event.metadata.source == "mobile" && event.tier == "gold" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US partner premium low tax".into(), expression: r#"event.amount > 30000 && event.region == "US" && event.metadata.source == "partner" && event.tier == "premium" && event.metadata.tax_rate < 0.05"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US web any tier >75k".into(), expression: r#"event.amount > 75000 && event.region == "US" && event.metadata.source == "web""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US API gold multi-item bulk order".into(), expression: r#"event.amount > 20000 && event.region == "US" && event.metadata.source == "api" && event.tier == "gold" && has(event.order) && event.order.items.size() >= 5"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US standard priority-2 tax band".into(), expression: r#"event.amount > 25000 && event.region == "US" && event.tier == "standard" && event.metadata.priority == 2 && event.metadata.tax_rate >= 0.08 && event.metadata.tax_rate <= 0.12"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US premium >100k any source".into(), expression: r#"event.amount > 100000 && event.region == "US" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US web standard items qty threshold".into(), expression: r#"event.amount > 15000 && event.region == "US" && event.metadata.source == "web" && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.quantity > 10)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US mobile standard low priority".into(), expression: r#"event.amount > 5000 && event.region == "US" && event.metadata.source == "mobile" && event.tier == "standard" && event.metadata.priority >= 4"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US partner gold mid-range".into(), expression: r#"event.amount >= 20000 && event.amount <= 50000 && event.region == "US" && event.metadata.source == "partner" && event.tier == "gold""#.into(), target_topic: "target-events".into(), enabled: true },

        // ── APAC region rules ────────────────────────────────────────────────
        RuleInput { description: "APAC gold mobile high-tax".into(), expression: r#"event.amount > 38620 && event.region == "APAC" && event.metadata.source == "mobile" && event.metadata.priority <= 4 && event.tier == "gold" && event.metadata.tax_rate >= 0.16"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC web premium >60k".into(), expression: r#"event.amount > 60000 && event.region == "APAC" && event.metadata.source == "web" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC partner standard large order".into(), expression: r#"event.amount > 22000 && event.region == "APAC" && event.metadata.source == "partner" && event.tier == "standard" && has(event.order) && event.order.items.size() >= 3"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC api gold priority-1".into(), expression: r#"event.amount > 15000 && event.region == "APAC" && event.metadata.source == "api" && event.tier == "gold" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC mobile premium 202x timestamp".into(), expression: r#"event.amount > 30000 && event.region == "APAC" && event.metadata.source == "mobile" && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC web gold tax band mid".into(), expression: r#"event.amount > 25000 && event.region == "APAC" && event.metadata.source == "web" && event.tier == "gold" && event.metadata.tax_rate >= 0.10 && event.metadata.tax_rate <= 0.18"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC standard >45k with item price".into(), expression: r#"event.amount > 45000 && event.region == "APAC" && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.price > 1000)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC partner premium urgent".into(), expression: r#"event.amount > 10000 && event.region == "APAC" && event.metadata.source == "partner" && event.tier == "premium" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC api standard bulk".into(), expression: r#"event.amount > 8000 && event.region == "APAC" && event.metadata.source == "api" && event.tier == "standard" && has(event.order) && event.order.items.size() >= 8"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC >80k any tier web".into(), expression: r#"event.amount > 80000 && event.region == "APAC" && event.metadata.source == "web""#.into(), target_topic: "target-events".into(), enabled: true },

        // ── LATAM region rules ───────────────────────────────────────────────
        RuleInput { description: "LATAM premium partner 202x".into(), expression: r#"event.amount > 18508 && event.region == "LATAM" && event.metadata.source == "partner" && event.metadata.priority <= 5 && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM web gold >35k".into(), expression: r#"event.amount > 35000 && event.region == "LATAM" && event.metadata.source == "web" && event.tier == "gold""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM mobile standard priority-2 items".into(), expression: r#"event.amount > 12000 && event.region == "LATAM" && event.metadata.source == "mobile" && event.tier == "standard" && event.metadata.priority == 2 && has(event.order)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM api premium low tax".into(), expression: r#"event.amount > 25000 && event.region == "LATAM" && event.metadata.source == "api" && event.tier == "premium" && event.metadata.tax_rate < 0.07"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM partner gold high item count".into(), expression: r#"event.amount > 18000 && event.region == "LATAM" && event.metadata.source == "partner" && event.tier == "gold" && has(event.order) && event.order.items.size() >= 6"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM web premium >55k".into(), expression: r#"event.amount > 55000 && event.region == "LATAM" && event.metadata.source == "web" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM mobile gold tax band".into(), expression: r#"event.amount > 20000 && event.region == "LATAM" && event.metadata.source == "mobile" && event.tier == "gold" && event.metadata.tax_rate >= 0.12"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM api standard high quantity".into(), expression: r#"event.amount > 9000 && event.region == "LATAM" && event.metadata.source == "api" && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.quantity > 15)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM partner standard priority-1 large".into(), expression: r#"event.amount > 40000 && event.region == "LATAM" && event.metadata.source == "partner" && event.tier == "standard" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM >70k any source gold".into(), expression: r#"event.amount > 70000 && event.region == "LATAM" && event.tier == "gold""#.into(), target_topic: "target-events".into(), enabled: true },

        // ── EU region rules ──────────────────────────────────────────────────
        RuleInput { description: "EU web premium >50k VAT band".into(), expression: r#"event.amount > 50000 && event.region == "EU" && event.metadata.source == "web" && event.tier == "premium" && event.metadata.tax_rate >= 0.20"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU mobile gold urgent multi-item".into(), expression: r#"event.amount > 30000 && event.region == "EU" && event.metadata.source == "mobile" && event.tier == "gold" && event.metadata.priority == 1 && has(event.order) && event.order.items.size() >= 4"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU partner standard >20k".into(), expression: r#"event.amount > 20000 && event.region == "EU" && event.metadata.source == "partner" && event.tier == "standard""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU api premium priority-2".into(), expression: r#"event.amount > 35000 && event.region == "EU" && event.metadata.source == "api" && event.tier == "premium" && event.metadata.priority <= 2"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU web gold high-value item".into(), expression: r#"event.amount > 45000 && event.region == "EU" && event.metadata.source == "web" && event.tier == "gold" && has(event.order) && event.order.items.exists(i, i.price > 2000)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU mobile standard low-priority bulk".into(), expression: r#"event.amount > 5000 && event.region == "EU" && event.metadata.source == "mobile" && event.tier == "standard" && event.metadata.priority >= 3 && has(event.order) && event.order.items.size() >= 10"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU partner gold 202x timestamp".into(), expression: r#"event.amount > 28000 && event.region == "EU" && event.metadata.source == "partner" && event.tier == "gold" && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU api gold >65k".into(), expression: r#"event.amount > 65000 && event.region == "EU" && event.metadata.source == "api" && event.tier == "gold""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU web standard high-tax threshold".into(), expression: r#"event.amount > 12000 && event.region == "EU" && event.metadata.source == "web" && event.tier == "standard" && event.metadata.tax_rate >= 0.19"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU >90k any tier premium".into(), expression: r#"event.amount > 90000 && event.region == "EU" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },

        // ── MEA region rules ─────────────────────────────────────────────────
        RuleInput { description: "MEA web gold zero-tax high value".into(), expression: r#"event.amount > 60000 && event.region == "MEA" && event.metadata.source == "web" && event.tier == "gold" && event.metadata.tax_rate == 0"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA mobile premium priority-1".into(), expression: r#"event.amount > 40000 && event.region == "MEA" && event.metadata.source == "mobile" && event.tier == "premium" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA partner standard >15k items".into(), expression: r#"event.amount > 15000 && event.region == "MEA" && event.metadata.source == "partner" && event.tier == "standard" && has(event.order) && event.order.items.size() >= 3"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA api premium >50k".into(), expression: r#"event.amount > 50000 && event.region == "MEA" && event.metadata.source == "api" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA web standard tax exempt bulk".into(), expression: r#"event.amount > 10000 && event.region == "MEA" && event.metadata.source == "web" && event.tier == "standard" && event.metadata.tax_rate < 0.03 && has(event.order) && event.order.items.size() >= 5"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA partner gold 202x large".into(), expression: r#"event.amount > 35000 && event.region == "MEA" && event.metadata.source == "partner" && event.tier == "gold" && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA mobile gold priority band".into(), expression: r#"event.amount > 22000 && event.region == "MEA" && event.metadata.source == "mobile" && event.tier == "gold" && event.metadata.priority <= 3"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA api standard high item price".into(), expression: r#"event.amount > 18000 && event.region == "MEA" && event.metadata.source == "api" && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.price > 500)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA web premium >80k".into(), expression: r#"event.amount > 80000 && event.region == "MEA" && event.metadata.source == "web" && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA partner premium zero-tax priority-1".into(), expression: r#"event.amount > 45000 && event.region == "MEA" && event.metadata.source == "partner" && event.tier == "premium" && event.metadata.tax_rate == 0 && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Cross-region / tier-based rules ──────────────────────────────────
        RuleInput { description: "any region premium >120k".into(), expression: r#"event.amount > 120000 && event.tier == "premium""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "any region gold urgent API >50k".into(), expression: r#"event.amount > 50000 && event.tier == "gold" && event.metadata.source == "api" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "web channel >200k any tier".into(), expression: r#"event.amount > 200000 && event.metadata.source == "web""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "partner channel premium 202x high value".into(), expression: r#"event.amount > 80000 && event.metadata.source == "partner" && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "mobile gold >40k with bulk items".into(), expression: r#"event.amount > 40000 && event.metadata.source == "mobile" && event.tier == "gold" && has(event.order) && event.order.items.size() >= 7"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "api standard >10k high quantity item".into(), expression: r#"event.amount > 10000 && event.metadata.source == "api" && event.tier == "standard" && has(event.order) && event.order.items.exists(i, i.quantity > 20)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "any source premium priority-1 >30k".into(), expression: r#"event.amount > 30000 && event.tier == "premium" && event.metadata.priority == 1"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "web gold tax>=0.15 >25k".into(), expression: r#"event.amount > 25000 && event.metadata.source == "web" && event.tier == "gold" && event.metadata.tax_rate >= 0.15"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "partner any tier >100k".into(), expression: r#"event.amount > 100000 && event.metadata.source == "partner""#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "mobile premium priority-2 large item".into(), expression: r#"event.amount > 35000 && event.metadata.source == "mobile" && event.tier == "premium" && event.metadata.priority == 2 && has(event.order) && event.order.items.exists(i, i.price > 3000)"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Multi-condition complex rules ─────────────────────────────────────
        RuleInput { description: "US web gold priority-1 high-tax >30k multi-item".into(), expression: r#"event.amount > 30000 && event.region == "US" && event.metadata.source == "web" && event.tier == "gold" && event.metadata.priority == 1 && event.metadata.tax_rate >= 0.10 && has(event.order) && event.order.items.size() >= 3"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU mobile premium priority-1 202x high-tax".into(), expression: r#"event.amount > 45000 && event.region == "EU" && event.metadata.source == "mobile" && event.tier == "premium" && event.metadata.priority == 1 && event.metadata.tax_rate >= 0.20 && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC partner standard bulk high-qty low-tax".into(), expression: r#"event.amount > 12000 && event.region == "APAC" && event.metadata.source == "partner" && event.tier == "standard" && event.metadata.tax_rate < 0.05 && has(event.order) && event.order.items.size() >= 10 && event.order.items.exists(i, i.quantity > 5)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM api gold priority band tax mid".into(), expression: r#"event.amount > 28000 && event.region == "LATAM" && event.metadata.source == "api" && event.tier == "gold" && event.metadata.priority <= 3 && event.metadata.tax_rate >= 0.08 && event.metadata.tax_rate <= 0.18"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA web standard 202x zero-tax large items".into(), expression: r#"event.amount > 20000 && event.region == "MEA" && event.metadata.source == "web" && event.tier == "standard" && event.metadata.tax_rate == 0 && has(event.timestamp) && event.timestamp.startsWith("202") && has(event.order) && event.order.items.size() >= 4"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US partner premium priority-2 high-item-price".into(), expression: r#"event.amount > 55000 && event.region == "US" && event.metadata.source == "partner" && event.tier == "premium" && event.metadata.priority == 2 && has(event.order) && event.order.items.exists(i, i.price > 5000)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC mobile standard priority-3 bulk order".into(), expression: r#"event.amount > 7000 && event.region == "APAC" && event.metadata.source == "mobile" && event.tier == "standard" && event.metadata.priority == 3 && has(event.order) && event.order.items.size() >= 12"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU web gold priority-1 202x low-tax".into(), expression: r#"event.amount > 42000 && event.region == "EU" && event.metadata.source == "web" && event.tier == "gold" && event.metadata.priority == 1 && event.metadata.tax_rate < 0.10 && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM mobile premium priority-1 high-item-count".into(), expression: r#"event.amount > 33000 && event.region == "LATAM" && event.metadata.source == "mobile" && event.tier == "premium" && event.metadata.priority == 1 && has(event.order) && event.order.items.size() >= 8 && event.order.items.exists(i, i.price > 300)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA api gold 202x zero-tax bulk".into(), expression: r#"event.amount > 38000 && event.region == "MEA" && event.metadata.source == "api" && event.tier == "gold" && event.metadata.tax_rate == 0 && has(event.timestamp) && event.timestamp.startsWith("202") && has(event.order) && event.order.items.size() >= 6"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Fraud / anomaly detection rules ──────────────────────────────────
        RuleInput { description: "US mobile >150k priority-1 fraud-watch".into(), expression: r#"event.amount > 150000 && event.region == "US" && event.metadata.source == "mobile" && event.metadata.priority == 1"#.into(), target_topic: "alerts".into(), enabled: true },
        RuleInput { description: "any region api >200k tax zero".into(), expression: r#"event.amount > 200000 && event.metadata.source == "api" && event.metadata.tax_rate == 0"#.into(), target_topic: "alerts".into(), enabled: true },
        RuleInput { description: "LATAM web standard >80k single item".into(), expression: r#"event.amount > 80000 && event.region == "LATAM" && event.metadata.source == "web" && event.tier == "standard" && has(event.order) && event.order.items.size() == 1 && event.order.items.exists(i, i.price > 70000)"#.into(), target_topic: "alerts".into(), enabled: true },
        RuleInput { description: "EU partner gold >90k zero-tax".into(), expression: r#"event.amount > 90000 && event.region == "EU" && event.metadata.source == "partner" && event.tier == "gold" && event.metadata.tax_rate == 0"#.into(), target_topic: "alerts".into(), enabled: true },
        RuleInput { description: "APAC mobile premium >110k priority-1".into(), expression: r#"event.amount > 110000 && event.region == "APAC" && event.metadata.source == "mobile" && event.tier == "premium" && event.metadata.priority == 1"#.into(), target_topic: "alerts".into(), enabled: true },

        // ── Operational / routing rules ───────────────────────────────────────
        RuleInput { description: "all standard orders any region".into(), expression: r#"event.tier == "standard" && event.amount > 0"#.into(), target_topic: "standard-events".into(), enabled: false },
        RuleInput { description: "web orders above minimum".into(), expression: r#"event.metadata.source == "web" && event.amount >= 1000"#.into(), target_topic: "web-events".into(), enabled: false },
        RuleInput { description: "mobile orders above minimum".into(), expression: r#"event.metadata.source == "mobile" && event.amount >= 500"#.into(), target_topic: "mobile-events".into(), enabled: false },
        RuleInput { description: "partner channel all orders".into(), expression: r#"event.metadata.source == "partner" && event.amount > 0"#.into(), target_topic: "partner-events".into(), enabled: false },

        // ── Latency / time sensitive rules ────────────────────────────────────
        RuleInput { description: "US gold 202x web priority-1 fast-lane".into(), expression: r#"event.amount > 25000 && event.region == "US" && event.tier == "gold" && event.metadata.source == "web" && event.metadata.priority == 1 && has(event.timestamp) && event.timestamp.startsWith("202")"#.into(), target_topic: "fast-lane".into(), enabled: true },
        RuleInput { description: "EU premium priority-1 any >40k fast-lane".into(), expression: r#"event.amount > 40000 && event.region == "EU" && event.tier == "premium" && event.metadata.priority == 1"#.into(), target_topic: "fast-lane".into(), enabled: true },
        RuleInput { description: "APAC gold partner priority-1 >35k".into(), expression: r#"event.amount > 35000 && event.region == "APAC" && event.tier == "gold" && event.metadata.source == "partner" && event.metadata.priority == 1"#.into(), target_topic: "fast-lane".into(), enabled: true },

        // ── Value band rules ─────────────────────────────────────────────────
        RuleInput { description: "micro order <500 any region".into(), expression: r#"event.amount > 0 && event.amount < 500"#.into(), target_topic: "micro-events".into(), enabled: false },
        RuleInput { description: "small order 500-5000".into(), expression: r#"event.amount >= 500 && event.amount < 5000"#.into(), target_topic: "small-events".into(), enabled: false },
        RuleInput { description: "mid order 5000-25000".into(), expression: r#"event.amount >= 5000 && event.amount < 25000"#.into(), target_topic: "mid-events".into(), enabled: false },
        RuleInput { description: "large order 25000-100000".into(), expression: r#"event.amount >= 25000 && event.amount < 100000"#.into(), target_topic: "large-events".into(), enabled: false },
        RuleInput { description: "enterprise order >=100000".into(), expression: r#"event.amount >= 100000"#.into(), target_topic: "enterprise-events".into(), enabled: false },

        // ── Timestamp / audit rules ───────────────────────────────────────────
        RuleInput { description: "US web gold 2025 timestamp >20k".into(), expression: r#"event.amount > 20000 && event.region == "US" && event.metadata.source == "web" && event.tier == "gold" && has(event.timestamp) && event.timestamp.startsWith("2025")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "EU partner premium 2026 timestamp >30k".into(), expression: r#"event.amount > 30000 && event.region == "EU" && event.metadata.source == "partner" && event.tier == "premium" && has(event.timestamp) && event.timestamp.startsWith("2026")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC api gold 2025 >25k".into(), expression: r#"event.amount > 25000 && event.region == "APAC" && event.metadata.source == "api" && event.tier == "gold" && has(event.timestamp) && event.timestamp.startsWith("2025")"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM mobile standard 2026 bulk".into(), expression: r#"event.amount > 8000 && event.region == "LATAM" && event.metadata.source == "mobile" && event.tier == "standard" && has(event.timestamp) && event.timestamp.startsWith("2026") && has(event.order) && event.order.items.size() >= 5"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "MEA partner gold 2025 >40k".into(), expression: r#"event.amount > 40000 && event.region == "MEA" && event.metadata.source == "partner" && event.tier == "gold" && has(event.timestamp) && event.timestamp.startsWith("2025")"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Item-level detail rules ───────────────────────────────────────────
        RuleInput { description: "any region gold high avg item price".into(), expression: r#"event.amount > 30000 && event.tier == "gold" && has(event.order) && event.order.items.exists(i, i.price > 2500)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "US premium single item ultra-high price".into(), expression: r#"event.amount > 50000 && event.region == "US" && event.tier == "premium" && has(event.order) && event.order.items.size() == 1 && event.order.items.exists(i, i.price > 40000)"#.into(), target_topic: "alerts".into(), enabled: true },
        RuleInput { description: "EU standard multi-item high qty web".into(), expression: r#"event.amount > 15000 && event.region == "EU" && event.tier == "standard" && event.metadata.source == "web" && has(event.order) && event.order.items.exists(i, i.quantity > 25)"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "APAC gold many small items".into(), expression: r#"event.amount > 10000 && event.region == "APAC" && event.tier == "gold" && has(event.order) && event.order.items.size() >= 15"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "LATAM premium expensive items bulk".into(), expression: r#"event.amount > 25000 && event.region == "LATAM" && event.tier == "premium" && has(event.order) && event.order.items.size() >= 5 && event.order.items.exists(i, i.price > 1500)"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Priority-specific rules ───────────────────────────────────────────
        RuleInput { description: "priority-1 any region gold >20k".into(), expression: r#"event.metadata.priority == 1 && event.tier == "gold" && event.amount > 20000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "priority-1 premium >15k fast-track".into(), expression: r#"event.metadata.priority == 1 && event.tier == "premium" && event.amount > 15000"#.into(), target_topic: "fast-lane".into(), enabled: true },
        RuleInput { description: "priority-5 standard bulk cheapest".into(), expression: r#"event.metadata.priority >= 5 && event.tier == "standard" && event.amount < 3000 && has(event.order) && event.order.items.size() >= 10"#.into(), target_topic: "bulk-events".into(), enabled: true },
        RuleInput { description: "priority-2 gold EU web >35k".into(), expression: r#"event.metadata.priority == 2 && event.tier == "gold" && event.region == "EU" && event.metadata.source == "web" && event.amount > 35000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "priority-3 standard US mobile item qty".into(), expression: r#"event.metadata.priority == 3 && event.tier == "standard" && event.region == "US" && event.metadata.source == "mobile" && has(event.order) && event.order.items.exists(i, i.quantity >= 10)"#.into(), target_topic: "target-events".into(), enabled: true },

        // ── Tax-bracket rules ─────────────────────────────────────────────────
        RuleInput { description: "high-tax bracket EU web >20k".into(), expression: r#"event.metadata.tax_rate >= 0.25 && event.region == "EU" && event.metadata.source == "web" && event.amount > 20000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "zero-tax MEA partner premium".into(), expression: r#"event.metadata.tax_rate == 0 && event.region == "MEA" && event.metadata.source == "partner" && event.tier == "premium" && event.amount > 25000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "low-tax APAC api gold".into(), expression: r#"event.metadata.tax_rate < 0.05 && event.region == "APAC" && event.metadata.source == "api" && event.tier == "gold" && event.amount > 30000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "mid-tax US mobile standard".into(), expression: r#"event.metadata.tax_rate >= 0.08 && event.metadata.tax_rate <= 0.14 && event.region == "US" && event.metadata.source == "mobile" && event.tier == "standard" && event.amount > 8000"#.into(), target_topic: "target-events".into(), enabled: true },
        RuleInput { description: "VAT threshold EU premium partner".into(), expression: r#"event.metadata.tax_rate >= 0.20 && event.metadata.tax_rate <= 0.25 && event.region == "EU" && event.metadata.source == "partner" && event.tier == "premium" && event.amount > 40000"#.into(), target_topic: "target-events".into(), enabled: true },
    ];

    let count = rules.len();
    for input in rules {
        repo.create(&input).await?;
    }

    Ok(count)
}
