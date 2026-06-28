//! web crate — axum REST API (S8).
//!
//! Routes:
//!   GET    /health                  → liveness probe
//!   GET    /health/ready            → readiness probe (checks PG, CH, Kafka)
//!   GET    /api/rules               → list all rules
//!   POST   /api/rules               → create rule
//!   GET    /api/rules/:id           → get rule by id
//!   PUT    /api/rules/:id           → update rule
//!   DELETE /api/rules/:id           → delete rule
//!   GET    /api/analytics/stats     → analytics (from, to query params)
//!   GET    /api/metrics             → live per-service processing metrics
//!   GET    /api/config               → runtime config & feature flags
//!   POST   /api/simulation/push     → publish N synthetic events to Kafka
//!   GET    /api/integration/status  → probe service reachability (PG, CH, Kafka)

mod error;
mod routes;
#[cfg(test)]
mod tests;

pub use error::ApiError;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use rdkafka::producer::FutureProducer;
use tower_http::cors::{Any, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

/// Shared application state injected into every handler via axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub rules: store_postgres::RuleRepository,
    pub ch_client: store_clickhouse::ClickHouseClient,
    pub producer: Arc<FutureProducer>,
    pub source_topic: String,
    pub kafka_brokers: String,
    pub counters: Arc<pipeline::PipelineCounters>,
    pub rule_cache: pipeline::RuleCache,
}

/// Build the axum router with CORS enabled. Attach `AppState` before serving.
pub fn router(state: AppState, allowed_origins: Vec<axum::http::HeaderValue>) -> Router {
    let cors = if allowed_origins.is_empty() {
        tracing::warn!("ALLOWED_ORIGINS not set — CORS is permissive (all origins allowed)");
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allowed_origins))
            .allow_methods(Any)
            .allow_headers(Any)
    };

    Router::new()
        .route("/health", get(routes::health::health))
        .route("/health/ready", get(routes::health::ready))
        .route(
            "/api/rules",
            get(routes::rules::list).post(routes::rules::create),
        )
        .route(
            "/api/rules/:id",
            get(routes::rules::get_one)
                .put(routes::rules::update)
                .delete(routes::rules::delete_one),
        )
        .route("/api/config", get(routes::config::get_config))
        .route("/api/analytics/stats", get(routes::analytics::stats))
        .route("/api/reports/top", get(routes::reports::top))
        .route("/api/reports/export", get(routes::reports::export))
        .route("/api/metrics", get(routes::metrics::metrics))
        .route("/api/simulation/push", post(routes::simulation::push))
        .route("/api/integration/status", get(routes::integration::status))
        .route("/api/integration/run", post(routes::integration::run))
        .route("/tests", get(routes::integration::page))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
