use axum::{
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use serde::Serialize;
use std::convert::Infallible;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::timeout;
use tokio_stream::{wrappers::LinesStream, StreamExt};

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

pub async fn run() -> Response {
    if std::env::var("ENABLE_TEST_RUNNER").unwrap_or_default().is_empty() {
        return (
            StatusCode::FORBIDDEN,
            "Set ENABLE_TEST_RUNNER=1 to enable this endpoint",
        )
            .into_response();
    }

    let mut child = match Command::new("cargo")
        .args(["test", "--workspace", "--include-ignored", "--color", "never"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_lines = LinesStream::new(BufReader::new(stdout).lines());
    let stderr_lines = LinesStream::new(BufReader::new(stderr).lines());
    let merged = tokio_stream::StreamExt::merge(stdout_lines, stderr_lines);

    let stream = async_stream::stream! {
        tokio::pin!(merged);
        while let Some(line) = merged.next().await {
            let text = line.unwrap_or_else(|e| format!("[read error: {e}]"));
            yield Ok::<Event, Infallible>(Event::default().data(text));
        }
        let exit = child.wait().await.ok();
        let code = exit.and_then(|s| s.code()).unwrap_or(-1);
        yield Ok::<Event, Infallible>(
            Event::default()
                .event("done")
                .data(format!(r#"{{"exit_code":{code}}}"#))
        );
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
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

    #[tokio::test]
    async fn run_returns_403_without_env_var() {
        std::env::remove_var("ENABLE_TEST_RUNNER");
        let resp = run().await;
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }
}
