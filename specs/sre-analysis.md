# Spec: crates/sre/src/analysis.rs

Generate EXACTLY ONE fenced Rust code block. No commentary outside it.
Output path: crates/sre/src/analysis.rs

## Context

This file is part of the `sre` crate (crates/sre). Dependencies available:
- `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }`
- `serde` with derive feature
- `serde_json = "1"`
- `thiserror = "1"`
- `tokio` with full features

## What to implement

An HTTP client that calls a local LLM (OpenAI-compatible) and returns a structured finding.

### Finding struct

```rust
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Finding {
    pub severity:     String,    // INFO | WARN | ERROR | CRITICAL
    pub category:     String,    // crash_loop | connection_refused | oom | latency | config_error | normal | other
    pub finding:      String,    // plain English summary
    pub proposed_fix: String,    // plain English action or "No action required"
}
```

### AnalysisClient struct

```rust
pub struct AnalysisClient {
    base_url: String,
    model:    String,
    http:     reqwest::Client,
}
```

### Error type

```rust
#[derive(thiserror::Error, Debug)]
pub enum AnalysisError {
    #[error("LLM unavailable: {0}")]
    Unavailable(String),
    #[error("parse error: {0}")]
    ParseError(String),
}
```

### Public API

```rust
impl AnalysisClient {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self

    pub async fn analyze(
        &self,
        container: &str,
        log_window: &str,
    ) -> Result<Finding, AnalysisError>
}
```

## analyze implementation details

Build a request body like this (use serde_json::json! macro):

```json
{
  "model": "<self.model>",
  "messages": [
    {
      "role": "system",
      "content": "You are an SRE agent reviewing container logs for the Rust Rules Engine service stack (Redpanda, ClickHouse, Postgres, rules-engine, sre-agent). Respond ONLY with a JSON object with these exact keys: severity (one of INFO|WARN|ERROR|CRITICAL), category (one of crash_loop|connection_refused|oom|latency|config_error|normal|other), finding (plain English summary under 300 words), proposed_fix (plain English proposed action under 200 words, or \"No action required\" for INFO)."
    },
    {
      "role": "user",
      "content": "<container_name_and_log_window>"
    }
  ],
  "temperature": 0.2,
  "max_tokens": 512
}
```

The user content string should be:
```
Container: {container}
Log window (last 60 seconds, 200 lines):
{log_window}
```

POST to `{self.base_url}/v1/chat/completions`. Configure the reqwest::Client with a 30-second timeout.

Extract the response content:
```rust
let body: serde_json::Value = response.json().await.map_err(|e| AnalysisError::Unavailable(e.to_string()))?;
let content = body["choices"][0]["message"]["content"]
    .as_str()
    .ok_or_else(|| AnalysisError::ParseError("missing content".into()))?;
```

Parse `content` as JSON into `Finding` via `serde_json::from_str`. If parse fails, return `AnalysisError::ParseError`.

If the HTTP request itself fails (network error, timeout, non-2xx status), return `AnalysisError::Unavailable(...)`. Check status with `response.error_for_status()` before reading the body.

## Critical constraints

- The reqwest Client must be built with `.timeout(std::time::Duration::from_secs(30))`.
- NEVER panic on LLM failure — always return `Err(AnalysisError::...)`.
- `AnalysisClient::new` builds the reqwest::Client. If it fails (which it shouldn't), call `.expect("failed to build reqwest client")` — it's a programmer error.
- No main function. No mod declarations. Only the items specified.
