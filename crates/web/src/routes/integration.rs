use axum::Json;
use serde::Serialize;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Serialize)]
pub struct IntegrationStatus {
    pub ready: bool,
    pub services: Services,
}

#[derive(Serialize)]
pub struct Services {
    pub postgres: ServiceCheck,
    pub clickhouse: ServiceCheck,
    pub kafka: ServiceCheck,
}

#[derive(Serialize)]
pub struct ServiceCheck {
    pub ok: bool,
    pub addr: String,
}

async fn tcp_ok(addr: &str) -> bool {
    timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

pub async fn status() -> Json<IntegrationStatus> {
    let pg_addr = std::env::var("TEST_POSTGRES_ADDR")
        .unwrap_or_else(|_| "localhost:5432".into());
    let ch_addr = std::env::var("TEST_CLICKHOUSE_ADDR")
        .unwrap_or_else(|_| "localhost:8123".into());
    let kf_addr = std::env::var("TEST_KAFKA_ADDR")
        .unwrap_or_else(|_| "localhost:19092".into());

    let (pg_ok, ch_ok, kf_ok) = tokio::join!(
        tcp_ok(&pg_addr),
        tcp_ok(&ch_addr),
        tcp_ok(&kf_addr),
    );

    Json(IntegrationStatus {
        ready: pg_ok && ch_ok && kf_ok,
        services: Services {
            postgres: ServiceCheck { ok: pg_ok, addr: pg_addr },
            clickhouse: ServiceCheck { ok: ch_ok, addr: ch_addr },
            kafka: ServiceCheck { ok: kf_ok, addr: kf_addr },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_returns_valid_json_shape() {
        let Json(s) = status().await;
        assert!(!s.services.postgres.addr.is_empty());
        assert!(!s.services.clickhouse.addr.is_empty());
        assert!(!s.services.kafka.addr.is_empty());
        assert_eq!(s.ready, s.services.postgres.ok && s.services.clickhouse.ok && s.services.kafka.ok);
    }
}
