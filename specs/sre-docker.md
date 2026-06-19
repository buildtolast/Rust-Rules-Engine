# Spec: crates/sre/src/docker.rs

Generate EXACTLY ONE fenced Rust code block. No commentary outside it.
Output path: crates/sre/src/docker.rs

## Context

This file is part of the `sre` crate (crates/sre). Dependencies available:
- `bollard = "0.17"`
- `chrono` with serde feature
- `thiserror = "1"`
- `tokio` with full features

## What to implement

Functions that query the Docker Engine API via bollard.

### Structs

```rust
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub name:       String,      // e.g. "rre-postgres" (without leading slash)
    pub id:         String,
    pub running:    bool,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub health:     HealthSummary,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthSummary {
    Healthy,
    Unhealthy,
    None,
}
```

### Error type

```rust
#[derive(thiserror::Error, Debug)]
pub enum DockerError {
    #[error("bollard error: {0}")]
    Bollard(#[from] bollard::errors::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
```

### Functions

```rust
/// List all running containers (filters: status=running).
pub async fn list_containers(docker: &bollard::Docker) -> Result<Vec<ContainerInfo>, DockerError>

/// Collect the last `lines` log lines (stdout+stderr) from a container into a single String.
/// Lines are joined with '\n', newest last.
pub async fn tail_logs(docker: &bollard::Docker, name: &str, lines: usize) -> Result<String, DockerError>
```

## Critical constraints

### list_containers
- Use `bollard::container::ListContainersOptions::<String>` with `filters` = `{"status": ["running"]}`.
- For each container summary, call `docker.inspect_container(id, None)` to get `State` and `Health`.
- `name`: take the first element of `Names` vec, strip the leading `/` with `.trim_start_matches('/')`.
- `running`: `state.running.unwrap_or(false)`.
- `started_at`: parse `state.started_at` (Option<String>) via `chrono::DateTime::parse_from_rfc3339`, convert to Utc. If None or parse error → None.
- `health`: from `state.health.map(|h| h.status)`. Map `"healthy"` → `HealthSummary::Healthy`, `"unhealthy"` → `HealthSummary::Unhealthy`, anything else → `HealthSummary::None`.

### tail_logs
- Use `bollard::container::LogsOptions::<String>` with `stdout: true, stderr: true, tail: lines.to_string(), follow: false, timestamps: false`.
- Collect the stream: `use futures_util::StreamExt; let mut stream = docker.logs(name, Some(opts)); let mut lines_vec = Vec::new(); while let Some(item) = stream.next().await { ... }`.
- `bollard::container::LogOutput` has variants `StdOut { message }` and `StdErr { message }` — both are `Bytes`. Convert with `String::from_utf8(message.to_vec())?`.
- Join collected strings with `\n` and return.
- Import: `use bollard::container::{ListContainersOptions, LogsOptions, LogOutput};`
  and `use bollard::models::ContainerStateStatusEnum;` if needed.
- `futures_util` is a transitive dep of bollard — use `use futures_util::StreamExt;`.
- No main function. No mod declarations. Only the items specified.
