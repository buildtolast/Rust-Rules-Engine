use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("LLM unavailable: {0}")]
    Unavailable(String),
    #[error("parse error: {0}")]
    ParseError(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Finding {
    pub severity:     String,
    pub category:     String,
    pub finding:      String,
    pub proposed_fix: String,
}

pub struct AnalysisClient {
    base_url: String,
    model:    String,
    http:     reqwest::Client,
}

impl AnalysisClient {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");
        Self { base_url: base_url.into(), model: model.into(), http }
    }

    pub async fn analyze(
        &self,
        container: &str,
        log_window: &str,
    ) -> Result<Finding, AnalysisError> {
        let system_prompt = "You are an SRE agent reviewing container logs for the Rust Rules Engine \
            service stack (Redpanda, ClickHouse, Postgres, rules-engine, sre-agent). \
            Respond ONLY with a JSON object with these exact keys: \
            severity (one of INFO|WARN|ERROR|CRITICAL), \
            category (one of crash_loop|connection_refused|oom|latency|config_error|normal|other), \
            finding (plain English summary under 300 words), \
            proposed_fix (plain English proposed action under 200 words, or \"No action required\" for INFO).";

        let user_content = format!(
            "Container: {}\nLog window (last 60 seconds, 200 lines):\n{}",
            container, log_window
        );

        let body = serde_json::json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user",   "content": user_content  }
            ],
            "temperature": 0.2,
            "max_tokens":  512
        });

        let response = self
            .http
            .post(format!("{}/v1/chat/completions", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| AnalysisError::Unavailable(e.to_string()))?
            .error_for_status()
            .map_err(|e| AnalysisError::Unavailable(e.to_string()))?;

        let payload: serde_json::Value = response
            .json()
            .await
            .map_err(|e| AnalysisError::Unavailable(e.to_string()))?;

        let content = payload["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| AnalysisError::ParseError("missing content field".into()))?;

        serde_json::from_str::<Finding>(content)
            .map_err(|e| AnalysisError::ParseError(e.to_string()))
    }
}
