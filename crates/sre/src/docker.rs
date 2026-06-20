use bollard::container::{ListContainersOptions, LogOutput, LogsOptions};
use bollard::models::{Health, HealthStatusEnum};
use bollard::Docker;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DockerError {
    #[error("bollard error: {0}")]
    Bollard(#[from] bollard::errors::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum HealthSummary {
    Healthy,
    Unhealthy,
    None,
}

#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub name: String,
    pub id: String,
    pub running: bool,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub health: HealthSummary,
}

fn health_from(h: Option<Health>) -> HealthSummary {
    match h.and_then(|h| h.status) {
        Some(HealthStatusEnum::HEALTHY) => HealthSummary::Healthy,
        Some(HealthStatusEnum::UNHEALTHY) => HealthSummary::Unhealthy,
        _ => HealthSummary::None,
    }
}

pub async fn list_containers(docker: &Docker) -> Result<Vec<ContainerInfo>, DockerError> {
    let mut filters = HashMap::new();
    filters.insert("status", vec!["running"]);

    let summaries = docker
        .list_containers(Some(ListContainersOptions {
            all: false,
            filters,
            ..Default::default()
        }))
        .await?;

    let mut result = Vec::with_capacity(summaries.len());
    for s in summaries {
        let id = s.id.unwrap_or_default();
        let name = s
            .names
            .as_ref()
            .and_then(|v| v.first())
            .map(|n| n.trim_start_matches('/').to_string())
            .unwrap_or_default();

        let inspect = docker.inspect_container(&id, None).await?;
        let state = inspect.state.unwrap_or_default();

        let running = state.running.unwrap_or(false);
        let started_at = state.started_at.as_deref().and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
        let health = health_from(state.health);

        result.push(ContainerInfo {
            name,
            id,
            running,
            started_at,
            health,
        });
    }
    Ok(result)
}

pub async fn tail_logs(docker: &Docker, name: &str, lines: usize) -> Result<String, DockerError> {
    let opts = LogsOptions::<String> {
        stdout: true,
        stderr: true,
        tail: lines.to_string(),
        follow: false,
        ..Default::default()
    };

    let mut stream = docker.logs(name, Some(opts));
    let mut collected = Vec::new();

    while let Some(item) = stream.next().await {
        match item? {
            LogOutput::StdOut { message } | LogOutput::StdErr { message } => {
                collected.push(String::from_utf8(message.to_vec())?);
            }
            _ => {}
        }
    }

    Ok(collected.join(""))
}
