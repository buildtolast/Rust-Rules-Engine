use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::atomic::Ordering::Relaxed;

use crate::AppState;

#[derive(Serialize)]
pub struct PipelineMetrics {
    pub messages_total: u64,
    pub batches_total: u64,
    pub messages_per_sec: f64,
    pub avg_eval_ms: u64,
    pub avg_txn_ms: u64,
    pub consumer_lag: i64,
    pub rules_cached: usize,
}

#[derive(Serialize)]
pub struct KafkaMetrics {
    pub healthy: bool,
    pub partitions: usize,
    pub source_topic: String,
}

#[derive(Serialize)]
pub struct ClickHouseMetrics {
    pub audit_rows: u64,
    pub sre_observations: u64,
    pub latency_ms: u64,
}

#[derive(Serialize)]
pub struct PostgresMetrics {
    pub rules_total: u64,
    pub rules_enabled: u64,
    pub latency_ms: u64,
}

#[derive(Serialize)]
pub struct ServiceMetrics {
    pub pipeline: PipelineMetrics,
    pub kafka: KafkaMetrics,
    pub clickhouse: ClickHouseMetrics,
    pub postgres: PostgresMetrics,
}

pub async fn metrics(State(s): State<AppState>) -> Json<ServiceMetrics> {
    let (ch_metrics, pg_metrics, kafka_healthy) = tokio::join!(
        query_clickhouse(&s),
        query_postgres(&s),
        check_kafka_health(&s),
    );

    let c = &s.counters;
    Json(ServiceMetrics {
        pipeline: PipelineMetrics {
            messages_total: c.messages_total.load(Relaxed),
            batches_total: c.batches_total.load(Relaxed),
            messages_per_sec: (c.messages_per_sec() * 10.0).round() / 10.0,
            avg_eval_ms: c.avg_eval_ms(),
            avg_txn_ms: c.avg_txn_ms(),
            consumer_lag: c.consumer_lag.load(Relaxed),
            rules_cached: s.rule_cache.get().len(),
        },
        kafka: KafkaMetrics {
            healthy: kafka_healthy,
            partitions: 12,
            source_topic: s.source_topic.clone(),
        },
        clickhouse: ch_metrics,
        postgres: pg_metrics,
    })
}

async fn query_clickhouse(s: &AppState) -> ClickHouseMetrics {
    let start = std::time::Instant::now();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        #[serde(rename = "n")]
        n: u64,
    }

    let (audits, sre) = tokio::join!(
        s.ch_client
            .query("SELECT count() AS n FROM audits")
            .fetch_one::<Row>(),
        s.ch_client
            .query("SELECT count() AS n FROM sre_observations")
            .fetch_one::<Row>(),
    );

    let latency_ms = start.elapsed().as_millis() as u64;
    ClickHouseMetrics {
        audit_rows: audits.map(|r| r.n).unwrap_or(0),
        sre_observations: sre.map(|r| r.n).unwrap_or(0),
        latency_ms,
    }
}

async fn query_postgres(s: &AppState) -> PostgresMetrics {
    let start = std::time::Instant::now();

    #[derive(sqlx::FromRow)]
    struct Counts {
        total: i64,
        enabled: i64,
    }

    let result = sqlx::query_as::<_, Counts>(
        "SELECT COUNT(*) as total, SUM(CASE WHEN enabled THEN 1 ELSE 0 END) as enabled FROM rules",
    )
    .fetch_one(s.rules.pool())
    .await;

    let latency_ms = start.elapsed().as_millis() as u64;
    match result {
        Ok(r) => PostgresMetrics {
            rules_total: r.total as u64,
            rules_enabled: r.enabled as u64,
            latency_ms,
        },
        Err(_) => PostgresMetrics {
            rules_total: 0,
            rules_enabled: 0,
            latency_ms,
        },
    }
}

async fn check_kafka_health(s: &AppState) -> bool {
    use tokio::net::TcpStream;
    let broker = s.kafka_brokers.split(',').next().unwrap_or("").to_string();
    // Replace internal Docker hostname with localhost for inter-container check
    let addr = broker;
    tokio::time::timeout(std::time::Duration::from_secs(2), TcpStream::connect(&addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}
