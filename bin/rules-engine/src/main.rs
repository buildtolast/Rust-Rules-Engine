use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ── Config from environment ──────────────────────────────────────────────
    let pg_url     = env_require("DATABASE_URL");
    let ch_cfg     = store_clickhouse::ClickHouseConfig {
        url:              std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into()),
        database:         std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "ruleaudit".into()),
        user:             std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "rules".into()),
        password:         std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "rules".into()),
        batch_max_rows:   500,
        batch_period_ms:  200,
    };
    let brokers        = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());
    let source_topic   = std::env::var("SOURCE_TOPIC").unwrap_or_else(|_| "source-events".into());
    let target_topic   = std::env::var("TARGET_TOPIC").unwrap_or_else(|_| "target-events".into());
    let consumer_group = std::env::var("CONSUMER_GROUP").unwrap_or_else(|_| "rules-engine".into());
    let txn_id         = std::env::var("TRANSACTIONAL_ID").unwrap_or_else(|_| "rules-engine-txn".into());
    let http_port: u16 = std::env::var("HTTP_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(8080);

    // ── Store connections ─────────────────────────────────────────────────────
    tracing::info!("connecting to postgres");
    let pool = store_postgres::connect(&pg_url).await.context("postgres connect")?;
    store_postgres::run_migrations(&pool).await.context("postgres migrations")?;

    tracing::info!("connecting to clickhouse");
    let ch_client = store_clickhouse::client(&ch_cfg);
    store_clickhouse::run_migrations(&ch_client).await.context("clickhouse migrations")?;

    // ── Rule cache + hot-reload ───────────────────────────────────────────────
    let repo = store_postgres::RuleRepository::new(pool.clone());
    let cache = pipeline::RuleCache::load(&repo).await.context("rule cache load")?;
    tracing::info!("rule cache loaded");

    let listener = store_postgres::RuleChangeListener::connect(&pool).await.context("pg listener")?;
    let cache_bg = cache.clone();
    let repo_bg  = repo.clone();
    tokio::spawn(async move {
        if let Err(e) = pipeline::watch_and_reload(cache_bg, repo_bg, listener).await {
            tracing::error!("hot-reload error: {e}");
        }
    });

    // ── Kafka producer (for simulation endpoint) ──────────────────────────────
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .context("kafka producer create")?;

    // ── HTTP API ─────────────────────────────────────────────────────────────
    let state = web::AppState {
        rules: repo,
        ch_client,
        producer: Arc::new(producer),
        source_topic: source_topic.clone(),
    };
    let app = web::router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], http_port));
    tracing::info!("HTTP API listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;

    // ── Pipeline ─────────────────────────────────────────────────────────────
    let pipeline_cfg = pipeline::PipelineConfig {
        brokers: brokers.clone(),
        source_topic,
        target_topic,
        consumer_group,
        transactional_id: txn_id,
        schema_version: 1,
    };
    let cache_pipeline = cache.clone();
    let ch_cfg_pipeline = ch_cfg.clone();

    tokio::select! {
        r = axum::serve(listener, app) => {
            r.context("axum serve")?;
        }
        r = pipeline::run(pipeline_cfg, cache_pipeline, ch_cfg_pipeline) => {
            r.context("pipeline")?;
        }
    }

    Ok(())
}

fn env_require(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| {
        eprintln!("ERROR: required environment variable {key} is not set");
        std::process::exit(1);
    })
}
