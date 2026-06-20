use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::time::Instant;
use tokio::time::{timeout, Duration};

use crate::AppState;

/// Liveness probe — Docker healthcheck calls this every 15s.
/// Must be fast: no I/O, just proves the process is alive.
pub async fn health() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// Readiness probe — checks all downstream dependencies.
/// Returns 200 when all services are reachable, 503 when any are degraded.
pub async fn ready(State(s): State<AppState>) -> (StatusCode, Json<Value>) {
    let (pg, ch, kafka) = tokio::join!(check_postgres(&s), check_clickhouse(&s), check_kafka(&s),);

    let all_ok = pg.ok && ch.ok && kafka.ok;
    let code = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        code,
        Json(json!({
            "status": if all_ok { "ready" } else { "degraded" },
            "services": {
                "postgres":   { "status": if pg.ok { "ok" } else { "error" },    "latency_ms": pg.latency_ms,    "error": pg.error },
                "clickhouse": { "status": if ch.ok { "ok" } else { "error" },    "latency_ms": ch.latency_ms,    "error": ch.error },
                "kafka":      { "status": if kafka.ok { "ok" } else { "error" }, "latency_ms": kafka.latency_ms, "error": kafka.error },
            }
        })),
    )
}

struct Check {
    ok: bool,
    latency_ms: u64,
    error: Option<String>,
}

async fn check_postgres(s: &AppState) -> Check {
    let t = Instant::now();
    match timeout(Duration::from_secs(2), s.rules.ping()).await {
        Ok(true) => Check {
            ok: true,
            latency_ms: t.elapsed().as_millis() as u64,
            error: None,
        },
        Ok(false) => Check {
            ok: false,
            latency_ms: t.elapsed().as_millis() as u64,
            error: Some("ping query failed".into()),
        },
        Err(_) => Check {
            ok: false,
            latency_ms: 2000,
            error: Some("timeout after 2s".into()),
        },
    }
}

async fn check_clickhouse(s: &AppState) -> Check {
    let t = Instant::now();
    match timeout(Duration::from_secs(2), store_clickhouse::ping(&s.ch_client)).await {
        Ok(true) => Check {
            ok: true,
            latency_ms: t.elapsed().as_millis() as u64,
            error: None,
        },
        Ok(false) => Check {
            ok: false,
            latency_ms: t.elapsed().as_millis() as u64,
            error: Some("ping query failed".into()),
        },
        Err(_) => Check {
            ok: false,
            latency_ms: 2000,
            error: Some("timeout after 2s".into()),
        },
    }
}

async fn check_kafka(s: &AppState) -> Check {
    let t = Instant::now();
    // TCP connect to the first listed broker — fast, no Kafka protocol overhead.
    let broker = s
        .kafka_brokers
        .split(',')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    match timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&broker),
    )
    .await
    {
        Ok(Ok(_)) => Check {
            ok: true,
            latency_ms: t.elapsed().as_millis() as u64,
            error: None,
        },
        Ok(Err(e)) => Check {
            ok: false,
            latency_ms: t.elapsed().as_millis() as u64,
            error: Some(e.to_string()),
        },
        Err(_) => Check {
            ok: false,
            latency_ms: 2000,
            error: Some("timeout after 2s".into()),
        },
    }
}
