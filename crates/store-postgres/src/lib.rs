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

/// Seed the database with default rules if it is empty.
pub async fn seed_default_rules(repo: &RuleRepository) -> Result<usize> {
    if !repo.list().await?.is_empty() {
        return Ok(0);
    }

    let rules = vec![
        RuleInput {
            description: "high value US standard order".to_string(),
            expression: "event.amount > 43618 && event.region == \"US\" && event.metadata.source == \"web\" && event.metadata.priority <= 5 && event.tier == \"standard\" && has(event.order) && event.order.items.exists(i, i.price > 678)".to_string(),
            target_topic: "target-events".to_string(),
            enabled: true,
        },
        RuleInput {
            description: "APAC gold mobile high tax".to_string(),
            expression: "event.amount > 38620 && event.region == \"APAC\" && event.metadata.source == \"mobile\" && event.metadata.priority <= 4 && event.tier == \"gold\" && event.metadata.tax_rate >= 0.16".to_string(),
            target_topic: "target-events".to_string(),
            enabled: true,
        },
        RuleInput {
            description: "LATAM premium partner 202x".to_string(),
            expression: "event.amount > 18508 && event.region == \"LATAM\" && event.metadata.source == \"partner\" && event.metadata.priority <= 5 && event.tier == \"premium\" && has(event.timestamp) && event.timestamp.startsWith(\"202\")".to_string(),
            target_topic: "target-events".to_string(),
            enabled: true,
        },
    ];

    for input in rules {
        repo.create(&input).await?;
    }

    Ok(3)
}
