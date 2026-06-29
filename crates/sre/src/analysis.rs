use crate::probes::SystemProbeResult;
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
    pub severity: String,
    pub category: String,
    pub finding: String,
    pub proposed_fix: String,
    /// Populated after LLM response — not parsed from LLM JSON.
    #[serde(default)]
    pub container_name: String,
    #[serde(default)]
    pub observed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WeakestLinkDecision {
    pub weakest_link: String,
    pub reasoning: String,
    pub recommended_action: String,
    pub severity: String,
}

#[derive(Clone)]
pub struct AnalysisClient {
    base_url: String,
    model: String,
    api_key: Option<String>,
    http: reqwest::Client,
}

impl AnalysisClient {
    pub fn new(
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: Option<String>,
        timeout_secs: u64,
    ) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .expect("failed to build reqwest client");
        Self {
            base_url: base_url.into(),
            model: model.into(),
            api_key,
            http,
        }
    }

    pub async fn analyze(
        &self,
        container: &str,
        log_window: &str,
    ) -> Result<Finding, AnalysisError> {
        let system_prompt = "You are an SRE agent reviewing container logs for the Rust Rules Engine \
            service stack (Redpanda, ClickHouse, Postgres, rules-engine, sre-agent). \
            Respond ONLY with a valid JSON object. All keys MUST be double-quoted strings. \
            Example: {\"severity\":\"WARN\",\"category\":\"latency\",\"finding\":\"...\",\"proposed_fix\":\"...\"} \
            Required keys: \
            \"severity\" (one of INFO|WARN|ERROR|CRITICAL), \
            \"category\" (one of crash_loop|connection_refused|oom|latency|config_error|normal|other), \
            \"finding\" (plain English summary under 300 words), \
            \"proposed_fix\" (plain English proposed action under 200 words, or \"No action required\" for INFO). \
            Do not use unquoted keys. Do not add markdown or prose outside the JSON object.";

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
            "temperature": 0.1,
            "max_tokens":  1500
        });

        // Retry once on connection errors (server closed idle keep-alive between calls).
        // Do NOT retry on timeouts — the model is busy and re-sending doubles the wait.
        let response = {
            let mut last_err = None;
            let mut result = None;
            for attempt in 0..2u8 {
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                let mut req = self
                    .http
                    .post(format!("{}/v1/chat/completions", self.base_url))
                    .json(&body);
                if let Some(key) = &self.api_key {
                    req = req.bearer_auth(key);
                }
                match req.send().await {
                    Err(e) if e.is_timeout() => {
                        // Timeout: model is saturated, bail immediately.
                        return Err(AnalysisError::Unavailable(e.to_string()));
                    }
                    Err(e) => {
                        last_err = Some(e);
                    }
                    Ok(r) if !r.status().is_success() => {
                        let status = r.status();
                        let body = r.text().await.unwrap_or_default();
                        return Err(AnalysisError::Unavailable(format!("HTTP {status}: {body}")));
                    }
                    Ok(r) => {
                        result = Some(r);
                        break;
                    }
                }
            }
            match result {
                Some(r) => r,
                None => return Err(AnalysisError::Unavailable(last_err.unwrap().to_string())),
            }
        };

        let payload: serde_json::Value = response
            .json()
            .await
            .map_err(|e| AnalysisError::Unavailable(e.to_string()))?;

        let content = payload["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| AnalysisError::ParseError("missing content field".into()))?;

        extract_json(content)
    }

    pub async fn decide_weakest_link(
        &self,
        probe: &SystemProbeResult,
        total_lag: i64,
        lag_trend: &str,
        ch_backlog_batches: i32,
        recent_findings: &[Finding],
    ) -> Option<WeakestLinkDecision> {
        let system_prompt = "You are an SRE agent deciding which service is the weakest link in a \
            distributed system. Respond ONLY with valid JSON: \
            {\"weakest_link\":\"...\",\"reasoning\":\"...\",\"recommended_action\":\"...\",\"severity\":\"...\"} \
            weakest_link must be one of: postgres, clickhouse, kafka, app, none. \
            severity must be one of: INFO, WARN, ERROR, CRITICAL.";

        let service_lines: String = probe
            .services
            .iter()
            .map(|s| {
                if s.ok {
                    format!("  {}: ok ({}ms)", s.name, s.latency_ms)
                } else {
                    format!(
                        "  {}: ERROR: {}",
                        s.name,
                        s.error.as_deref().unwrap_or("unknown")
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let findings_lines: String = recent_findings
            .iter()
            .rev()
            .take(5)
            .map(|f| format!("  {}: [{}] {}", f.container_name, f.severity, f.finding))
            .collect::<Vec<_>>()
            .join("\n");

        let user_content = format!(
            "Consumer lag: {total_lag} messages ({lag_trend})\n\
             ClickHouse backlog: {ch_backlog_batches} buffered audit batches\n\
             Service health:\n{service_lines}\n\
             Recent findings (last 5):\n{}",
            if findings_lines.is_empty() {
                "  (none)".to_string()
            } else {
                findings_lines
            }
        );

        let body = serde_json::json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user",   "content": user_content  }
            ],
            "temperature": 0.1,
            "max_tokens": 512
        });

        let mut last_err: Option<String> = None;
        let mut result_text: Option<String> = None;

        for attempt in 0..2u8 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            let mut req = self
                .http
                .post(format!("{}/v1/chat/completions", self.base_url))
                .json(&body);
            if let Some(key) = &self.api_key {
                req = req.bearer_auth(key);
            }
            match req.send().await {
                Err(e) if e.is_timeout() => {
                    tracing::warn!("weakest-link LLM timeout");
                    return None;
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                }
                Ok(r) if !r.status().is_success() => {
                    last_err = Some(format!("HTTP {}", r.status()));
                }
                Ok(r) => match r.json::<serde_json::Value>().await {
                    Ok(payload) => {
                        if let Some(content) = payload["choices"][0]["message"]["content"].as_str()
                        {
                            result_text = Some(content.to_string());
                            break;
                        }
                        last_err = Some("missing content field".into());
                    }
                    Err(e) => {
                        last_err = Some(e.to_string());
                    }
                },
            }
        }

        let text = match result_text {
            Some(t) => t,
            None => {
                tracing::warn!(
                    "weakest-link LLM unavailable: {}",
                    last_err.as_deref().unwrap_or("unknown")
                );
                return None;
            }
        };

        // Strip <think> blocks
        let buf;
        let text_ref = if let (Some(s), Some(e)) = (text.find("<think>"), text.find("</think>")) {
            buf = format!("{}{}", &text[..s], &text[e + "</think>".len()..]);
            buf.as_str()
        } else {
            text.as_str()
        };

        let start = text_ref.find('{')?;
        let end = text_ref.rfind('}')?;
        let raw = &text_ref[start..=end];

        serde_json::from_str::<WeakestLinkDecision>(raw)
            .or_else(|_| {
                let fixed = quote_bare_keys(raw);
                serde_json::from_str::<WeakestLinkDecision>(&fixed)
            })
            .ok()
    }

    /// Lightweight reachability check — returns true if the LLM responds to a
    /// minimal completion request, false on any error or non-2xx status.
    pub async fn probe(&self) -> bool {
        let body = serde_json::json!({
            "model": self.model,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1
        });
        let mut req = self
            .http
            .post(format!("{}/v1/chat/completions", self.base_url))
            .json(&body);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        matches!(req.send().await, Ok(r) if r.status().is_success())
    }

    /// Send a raw prompt and return the raw LLM text response.
    pub async fn raw_complete(&self, prompt: &str) -> anyhow::Result<String> {
        #[derive(serde::Serialize)]
        struct Req<'a> {
            model: &'a str,
            messages: Vec<Msg<'a>>,
            temperature: f64,
            max_tokens: u32,
        }
        #[derive(serde::Serialize)]
        struct Msg<'a> {
            role: &'a str,
            content: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            choices: Vec<Choice>,
        }
        #[derive(serde::Deserialize)]
        struct Choice {
            message: MsgOut,
        }
        #[derive(serde::Deserialize)]
        struct MsgOut {
            content: String,
        }

        let req = Req {
            model: &self.model,
            messages: vec![Msg {
                role: "user",
                content: prompt,
            }],
            temperature: 0.2,
            max_tokens: 1024,
        };

        let mut request = self
            .http
            .post(format!("{}/v1/chat/completions", self.base_url))
            .json(&req);
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(key);
        }

        let resp: Resp = request.send().await?.error_for_status()?.json().await?;

        Ok(resp
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .unwrap_or_default())
    }
}

/// Quote any bare (unquoted) object keys so that JavaScript-style output
/// from models that ignore the JSON spec can still be parsed.
/// Only touches keys — values that are already quoted strings are left alone.
fn quote_bare_keys(s: &str) -> String {
    // Match an unquoted identifier used as a key: after { or , and before :
    // Unquoted key: starts at a word boundary, contains only [a-zA-Z0-9_].
    let mut out = String::with_capacity(s.len() + 16);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // After { or , skip whitespace then check for a bare key.
        if bytes[i] == b'{' || bytes[i] == b',' {
            out.push(bytes[i] as char);
            i += 1;
            // Consume whitespace
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                out.push(bytes[i] as char);
                i += 1;
            }
            // If next char is not '"', try to read an identifier
            if i < bytes.len()
                && bytes[i] != b'"'
                && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
            {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                // Only treat as bare key if followed (after whitespace) by ':'
                let end = i;
                let mut j = i;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b':' {
                    out.push('"');
                    out.push_str(&s[start..end]);
                    out.push('"');
                } else {
                    out.push_str(&s[start..end]);
                }
                continue;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Extract the first complete `{...}` JSON object from a string.
/// Strips `<think>…</think>` reasoning blocks (emitted by GLM and similar
/// models) before searching, so the reasoning draft never shadows the answer.
fn extract_json(content: &str) -> Result<Finding, AnalysisError> {
    // Remove <think>...</think> if present; the final answer follows it.
    let buf;
    let content = if let (Some(s), Some(e)) = (content.find("<think>"), content.find("</think>")) {
        buf = format!("{}{}", &content[..s], &content[e + "</think>".len()..]);
        buf.as_str()
    } else {
        content
    };

    let start = content
        .find('{')
        .ok_or_else(|| AnalysisError::ParseError("no JSON object in LLM response".into()))?;
    let end = content
        .rfind('}')
        .ok_or_else(|| AnalysisError::ParseError("unclosed JSON object in LLM response".into()))?;

    let raw = &content[start..=end];
    // Try strict parse first; fall back to quoting bare keys (some models output JS-style objects).
    serde_json::from_str::<Finding>(raw).or_else(|_| {
        let fixed = quote_bare_keys(raw);
        serde_json::from_str::<Finding>(&fixed)
            .map_err(|e| AnalysisError::ParseError(format!("{e}: {raw}")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_handles_bare_keys() {
        let js_style = r#"{severity: "WARN", category: "config_error", finding: "bad config", proposed_fix: "fix it"}"#;
        let f = extract_json(js_style).expect("should parse bare-key JS object");
        assert_eq!(f.severity, "WARN");
        assert_eq!(f.category, "config_error");
    }

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
