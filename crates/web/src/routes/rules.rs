use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::{ApiError, AppState};

#[derive(Deserialize)]
pub struct RuleBody {
    pub description: String,
    pub expression: String,
    pub target_topic: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl From<RuleBody> for store_postgres::RuleInput {
    fn from(b: RuleBody) -> Self {
        store_postgres::RuleInput {
            description: b.description,
            expression: b.expression,
            target_topic: b.target_topic,
            enabled: b.enabled,
        }
    }
}

pub async fn list(State(s): State<AppState>) -> Result<Json<Vec<rules_core::Rule>>, ApiError> {
    Ok(Json(s.rules.list().await?))
}

pub async fn get_one(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<rules_core::Rule>, ApiError> {
    s.rules
        .get(&id)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

pub async fn create(
    State(s): State<AppState>,
    Json(body): Json<RuleBody>,
) -> Result<(StatusCode, Json<rules_core::Rule>), ApiError> {
    let rule = s.rules.create(&body.into()).await?;
    Ok((StatusCode::CREATED, Json(rule)))
}

pub async fn update(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RuleBody>,
) -> Result<Json<rules_core::Rule>, ApiError> {
    s.rules
        .update(&id, &body.into())
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

pub async fn delete_one(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if s.rules.delete(&id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound)
    }
}
