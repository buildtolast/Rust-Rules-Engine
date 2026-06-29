use bollard::container::{ListContainersOptions, LogOutput, LogsOptions, StatsOptions};
use bollard::models::{Health, HealthStatusEnum};
use bollard::Docker;
use futures_util::stream::StreamExt;
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
    pub one_shot: bool,
    pub exit_code: Option<i64>,
}

fn health_from(h: Option<Health>) -> HealthSummary {
    match h.and_then(|h| h.status) {
        Some(HealthStatusEnum::HEALTHY) => HealthSummary::Healthy,
        Some(HealthStatusEnum::UNHEALTHY) => HealthSummary::Unhealthy,
        _ => HealthSummary::None,
    }
}

pub async fn list_containers(docker: &Docker) -> Result<Vec<ContainerInfo>, DockerError> {
    // Include stopped/exited containers so the SRE detects containers that went down.
    let summaries = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: true,
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
        let exit_code = state.exit_code;
        let one_shot = inspect
            .host_config
            .as_ref()
            .and_then(|hc| hc.restart_policy.as_ref())
            .and_then(|rp| rp.name.as_ref())
            .map(|n| matches!(n, bollard::models::RestartPolicyNameEnum::EMPTY | bollard::models::RestartPolicyNameEnum::NO))
            .unwrap_or(true);
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
            one_shot,
            exit_code,
        });
    }
    Ok(result)
}

/// Returns `(cpu_percent, mem_used_bytes, mem_limit_bytes)` for a running container.
/// Uses `one_shot=true` so the API returns immediately with a single sample.
/// Returns `None` for stopped containers or on any API error.
pub async fn fetch_stats(docker: &Docker, id: &str) -> Option<(f64, u64, u64)> {
    let opts = StatsOptions { stream: false, one_shot: true };
    let stats = docker
        .stats(id, Some(opts))
        .next()
        .await?
        .ok()?;

    let cpu_delta = stats
        .cpu_stats
        .cpu_usage
        .total_usage
        .saturating_sub(stats.precpu_stats.cpu_usage.total_usage);
    let system_delta = stats
        .cpu_stats
        .system_cpu_usage
        .unwrap_or(0)
        .saturating_sub(stats.precpu_stats.system_cpu_usage.unwrap_or(0));
    let num_cpus = stats.cpu_stats.online_cpus.unwrap_or(1) as f64;

    let cpu_pct = if system_delta > 0 {
        (cpu_delta as f64 / system_delta as f64) * num_cpus * 100.0
    } else {
        0.0
    };

    let mem_used = stats.memory_stats.usage.unwrap_or(0);
    let mem_limit = stats.memory_stats.limit.unwrap_or(0);

    Some((cpu_pct, mem_used, mem_limit))
}

pub async fn restart_container(docker: &Docker, id: &str) -> Result<(), DockerError> {
    docker
        .restart_container(id, None)
        .await
        .map_err(DockerError::Bollard)
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
