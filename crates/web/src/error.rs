use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Unified API error that converts into an axum response.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("postgres error: {0}")]
    Postgres(#[from] store_postgres::Error),
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] store_clickhouse::Error),
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::Postgres(_) | ApiError::ClickHouse(_) | ApiError::Kafka(_) => {
                tracing::error!("internal error: {self}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".into())
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
