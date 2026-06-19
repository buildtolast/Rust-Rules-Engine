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
    api_key:  Option<String>,
    http:     reqwest::Client,
}

impl AnalysisClient {
    pub fn new(
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: Option<String>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build reqwest client");
        Self { base_url: base_url.into(), model: model.into(), api_key, http }
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

        // Assistant prefill with "{" forces the model to complete a JSON object
        // rather than reasoning in prose (works with most OpenAI-compatible endpoints).
        let body = serde_json::json!({
            "model": self.model,
            "messages": [
                { "role": "system",    "content": system_prompt },
                { "role": "user",      "content": user_content  },
                { "role": "assistant", "content": "{"            }
            ],
            "temperature": 0.1,
            "max_tokens":  300
        });

        let mut req = self
            .http
            .post(format!("{}/v1/chat/completions", self.base_url))
            .json(&body);

        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }

        let response = req
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

        // The assistant prefill "{" is not included in the response content — prepend it.
        let full = format!("{{{content}");
        extract_json(&full)
    }
}

/// Extract the first complete `{...}` JSON object from a string.
/// The LLM often wraps JSON in prose or markdown — this strips the wrapper.
fn extract_json(content: &str) -> Result<Finding, AnalysisError> {
    let start = content
        .find('{')
        .ok_or_else(|| AnalysisError::ParseError("no JSON object in LLM response".into()))?;
    let end = content
        .rfind('}')
        .ok_or_else(|| AnalysisError::ParseError("unclosed JSON object in LLM response".into()))?;

    let json_slice = &content[start..=end];
    serde_json::from_str::<Finding>(json_slice)
        .map_err(|e| AnalysisError::ParseError(format!("{e}: {json_slice}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finding_deserializes_from_valid_json() {
        let json = r#"{
            "severity": "WARN",
            "category": "latency",
            "finding": "High latency detected on port 9092",
            "proposed_fix": "Check broker backpressure"
        }"#;
        let f: Finding = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(f.severity, "WARN");
        assert_eq!(f.category, "latency");
    }

    #[test]
    fn finding_parse_error_on_missing_field() {
        let json = r#"{"severity": "INFO"}"#;
        let result = serde_json::from_str::<Finding>(json);
        assert!(result.is_err(), "missing fields should fail to deserialize");
    }

    #[test]
    fn analysis_error_unavailable_formats_message() {
        let e = AnalysisError::Unavailable("connection refused".into());
        assert!(e.to_string().contains("connection refused"));
    }

    #[test]
    fn analysis_error_parse_error_formats_message() {
        let e = AnalysisError::ParseError("unexpected token".into());
        assert!(e.to_string().contains("unexpected token"));
    }
}
