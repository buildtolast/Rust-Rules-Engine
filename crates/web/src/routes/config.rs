use axum::{extract::State, Json};
use serde::Serialize;

use crate::AppState;

#[derive(Serialize)]
pub struct RuntimeConfig {
    pub simulation_senders: usize,
    // Kafka / Pipeline
    pub source_topic: String,
    pub target_topic: String,
    pub consumer_group: String,
    pub transactional_id: String,
    pub kafka_brokers: String,
    // App
    pub http_port: u16,
    pub rust_log: String,
}

pub async fn get_config(State(s): State<AppState>) -> Json<RuntimeConfig> {
    Json(RuntimeConfig {
        simulation_senders: env_usize("SIMULATION_SENDERS", 8),
        source_topic: s.source_topic.clone(),
        target_topic: env_str("TARGET_TOPIC", "target-events"),
        consumer_group: env_str("CONSUMER_GROUP", "rules-engine"),
        transactional_id: env_str("TRANSACTIONAL_ID", "rules-engine-txn"),
        kafka_brokers: s.kafka_brokers.clone(),
        http_port: env_usize("HTTP_PORT", 8080) as u16,
        rust_log: env_str("RUST_LOG", "info"),
    })
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
