//! web crate — axum REST API (S8).
//!
//! Routes:
//!   GET    /health                  → liveness probe
//!   GET    /api/rules               → list all rules
//!   POST   /api/rules               → create rule
//!   GET    /api/rules/:id           → get rule by id
//!   PUT    /api/rules/:id           → update rule
//!   DELETE /api/rules/:id           → delete rule
//!   GET    /api/analytics/stats     → analytics (from, to query params)
//!   POST   /api/simulation/push     → publish N synthetic events to Kafka

mod error;
mod routes;

pub use error::ApiError;

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use rdkafka::producer::FutureProducer;
use tower_http::cors::{Any, CorsLayer};

/// Shared application state injected into every handler via axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    pub rules: store_postgres::RuleRepository,
    pub ch_client: store_clickhouse::ClickHouseClient,
    pub producer: Arc<FutureProducer>,
    pub source_topic: String,
}

/// Build the axum router with CORS enabled. Attach `AppState` before serving.
pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(routes::health::health))
        .route("/api/rules", get(routes::rules::list).post(routes::rules::create))
        .route(
            "/api/rules/:id",
            get(routes::rules::get_one)
                .put(routes::rules::update)
                .delete(routes::rules::delete_one),
        )
        .route("/api/analytics/stats", get(routes::analytics::stats))
        .route("/api/simulation/push", post(routes::simulation::push))
        .layer(cors)
        .with_state(state)
}
