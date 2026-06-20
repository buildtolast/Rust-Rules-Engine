use axum::{
    extract::{Query, State},
    Json,
};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{ApiError, AppState};

#[derive(Deserialize)]
pub struct PushQuery {
    #[serde(default = "default_count")]
    count: usize,
}

fn default_count() -> usize {
    10
}

#[derive(Serialize)]
pub struct PushResult {
    count: usize,
    message: String,
}

pub async fn push(
    State(s): State<AppState>,
    Query(q): Query<PushQuery>,
) -> Result<Json<PushResult>, ApiError> {
    let count = q.count.min(1000); // cap at 1000 per call
    let producer = s.producer.clone();
    let topic = s.source_topic.clone();

    tokio::spawn(async move {
        for i in 0..count {
            let event = generate_event(i);
            let payload = serde_json::to_string(&event).expect("serialize event");
            let record = FutureRecord::to(&topic).payload(&payload).key("");
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                tracing::warn!("simulation send error: {e}");
            }
        }
        tracing::info!("simulation: published {count} events to {topic}");
    });

    Ok(Json(PushResult {
        count,
        message: format!(
            "publishing {count} events to {} in background",
            s.source_topic
        ),
    }))
}

fn generate_event(i: usize) -> serde_json::Value {
    let types = ["ORDER", "PAYMENT", "REFUND", "TRANSFER"];
    let event_type = types[i % types.len()];
    let amount = 10.0 + (i as f64 * 7.3) % 990.0;
    let user_id = format!("user-{}", i % 50 + 1);

    serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "type": event_type,
        "amount": (amount * 100.0).round() / 100.0,
        "userId": user_id,
        "currency": "USD",
        "seq": i
    })
}
