use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use store_clickhouse::analytics::AnalyticsStats;

use crate::{ApiError, AppState};

#[derive(Deserialize)]
pub struct StatsQuery {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
}

pub async fn stats(
    State(s): State<AppState>,
    Query(q): Query<StatsQuery>,
) -> Result<Json<AnalyticsStats>, ApiError> {
    let to = q.to.unwrap_or_else(Utc::now);
    let from = q.from.unwrap_or_else(|| to - Duration::hours(24));
    let result = store_clickhouse::query_analytics(&s.ch_client, from, to).await?;
    Ok(Json(result))
}
