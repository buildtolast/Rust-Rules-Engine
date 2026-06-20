use axum::{
    extract::{Query, State},
    Json,
};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{ApiError, AppState};

fn max_simulation_count() -> usize {
    std::env::var("MAX_SIMULATION_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

fn simulation_senders() -> usize {
    std::env::var("SIMULATION_SENDERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

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
    requested: usize,
    publishing: usize,
    message: String,
}

pub async fn push(
    State(s): State<AppState>,
    Query(q): Query<PushQuery>,
) -> Result<Json<PushResult>, ApiError> {
    let requested = q.count;
    let max_count = max_simulation_count();
    let senders = simulation_senders();
    let count = q.count.min(max_count);
    let producer = s.producer.clone();
    let topic = s.source_topic.clone();

    tokio::spawn(async move {
        let per_task = count.div_ceil(senders);
        let mut handles = Vec::with_capacity(senders);

        for t in 0..senders {
            let start = t * per_task;
            if start >= count {
                break;
            }
            let end = ((t + 1) * per_task).min(count);
            let producer = producer.clone();
            let topic = topic.clone();

            handles.push(tokio::spawn(async move {
                let mut ok = 0usize;
                for i in start..end {
                    let event = generate_event(i);
                    let payload = serde_json::to_string(&event).expect("serialize event");
                    let key = (i % 12).to_string();
                    let record = FutureRecord::to(&topic).payload(&payload).key(&key);
                    match producer.send(record, Duration::from_secs(5)).await {
                        Ok(_) => ok += 1,
                        Err((e, _)) => tracing::warn!("simulation send error: {e}"),
                    }
                }
                ok
            }));
        }

        let mut published = 0usize;
        for h in handles {
            published += h.await.unwrap_or(0);
        }
        tracing::info!("simulation: published {published}/{count} events to {topic}");
    });

    Ok(Json(PushResult {
        requested,
        publishing: count,
        message: format!(
            "publishing {count} events to {} in background ({senders} concurrent senders)",
            s.source_topic
        ),
    }))
}

/// Generate an event. 75 % (i % 4 != 3) are crafted to match at least one
/// seeded rule; 25 % (i % 4 == 3) are deliberately unmatched.
fn generate_event(i: usize) -> serde_json::Value {
    match i % 4 {
        0 => gen_premium_large(i),   // hits "any region premium >120k"
        1 => gen_gold_api_urgent(i), // hits "any region gold urgent API >50k"
        2 => gen_regional(i),        // hits various region+tier+source rules
        _ => gen_non_matching(i),    // no enabled rule matches
    }
}

/// Matches: "any region premium >120k" and many regional premium rules.
fn gen_premium_large(i: usize) -> serde_json::Value {
    const REGIONS: &[&str] = &["US", "APAC", "EU", "LATAM", "MEA"];
    const SOURCES: &[&str] = &["web", "mobile", "partner"];
    let region = REGIONS[(i / 4) % REGIONS.len()];
    let source = SOURCES[(i / 4) % SOURCES.len()];
    let amount = 130_000.0 + (i as f64 % 70.0) * 1_000.0;
    let tax = 0.10 + (i as f64 % 15.0) * 0.01;

    serde_json::json!({
        "id":        uuid::Uuid::new_v4().to_string(),
        "type":      "ORDER",
        "amount":    amount,
        "region":    region,
        "tier":      "premium",
        "timestamp": format!("2026-06-{:02}T12:00:00Z", (i % 28) + 1),
        "metadata":  { "source": source, "priority": 1, "tax_rate": tax },
        "order": {
            "orderId": format!("ord-{i}"),
            "items":   [{ "sku": format!("sku-{}", i % 100), "price": 50_000.0, "quantity": 1 }]
        },
        "seq": i
    })
}

/// Matches: "any region gold urgent API >50k" and EU/US/APAC gold API rules.
fn gen_gold_api_urgent(i: usize) -> serde_json::Value {
    const REGIONS: &[&str] = &["US", "APAC", "EU", "LATAM", "MEA"];
    let region = REGIONS[(i / 4) % REGIONS.len()];
    let amount = 55_000.0 + (i as f64 % 50.0) * 1_000.0;
    let tax = 0.08 + (i as f64 % 12.0) * 0.01;

    serde_json::json!({
        "id":        uuid::Uuid::new_v4().to_string(),
        "type":      "ORDER",
        "amount":    amount,
        "region":    region,
        "tier":      "gold",
        "timestamp": format!("2026-06-{:02}T08:00:00Z", (i % 28) + 1),
        "metadata":  { "source": "api", "priority": 1, "tax_rate": tax },
        "order": {
            "orderId": format!("ord-{i}"),
            "items":   [{ "sku": format!("sku-{}", i % 100), "price": 10_000.0, "quantity": 2 }]
        },
        "seq": i
    })
}

/// Cycles through 5 region+tier+source archetypes, each targeting specific seeded rules.
fn gen_regional(i: usize) -> serde_json::Value {
    // (region, source, tier, base_amount, priority, tax_rate, item_price)
    const ARCHETYPES: &[(&str, &str, &str, f64, u64, f64, f64)] = &[
        ("US", "web", "standard", 44_000.0, 2, 0.09, 1_000.0), // US web standard
        ("EU", "web", "premium", 55_000.0, 1, 0.21, 3_000.0),  // EU web premium VAT
        ("APAC", "mobile", "gold", 40_000.0, 2, 0.16, 2_000.0), // APAC gold mobile
        ("LATAM", "partner", "premium", 22_000.0, 3, 0.05, 800.0), // LATAM premium partner
        ("MEA", "web", "gold", 65_000.0, 1, 0.00, 1_500.0),    // MEA web gold zero-tax
    ];
    let (region, source, tier, base, priority, tax, item_price) =
        ARCHETYPES[(i / 4) % ARCHETYPES.len()];
    let amount = base + (i as f64 % 30.0) * 1_000.0;

    serde_json::json!({
        "id":        uuid::Uuid::new_v4().to_string(),
        "type":      "ORDER",
        "amount":    amount,
        "region":    region,
        "tier":      tier,
        "timestamp": format!("2026-06-{:02}T15:00:00Z", (i % 28) + 1),
        "metadata":  { "source": source, "priority": priority, "tax_rate": tax },
        "order": {
            "orderId": format!("ord-{i}"),
            "items": [
                { "sku": format!("sku-{}", i % 100),       "price": item_price,       "quantity": 5 },
                { "sku": format!("sku-{}", (i + 1) % 100), "price": item_price * 0.4, "quantity": 3 }
            ]
        },
        "seq": i
    })
}

/// Deliberately misses all enabled rules: amount < 500, unknown tier "bronze".
fn gen_non_matching(i: usize) -> serde_json::Value {
    let amount = 50.0 + (i as f64 % 400.0);

    serde_json::json!({
        "id":       uuid::Uuid::new_v4().to_string(),
        "type":     "ORDER",
        "amount":   amount,
        "region":   "US",
        "tier":     "bronze",
        "metadata": { "source": "web", "priority": 5, "tax_rate": 0.05 },
        "seq": i
    })
}
