use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = sre::SreConfig {
        clickhouse_url: env_require("CLICKHOUSE_URL"),
        clickhouse_db: env_require("CLICKHOUSE_DB"),
        clickhouse_user: env_require("CLICKHOUSE_USER"),
        clickhouse_pass: env_require("CLICKHOUSE_PASSWORD"),
        llm_base_url: env_require("LLM_BASE_URL"),
        llm_model: std::env::var("LLM_MODEL").unwrap_or_else(|_| "unsloth".into()),
        llm_api_key: std::env::var("LLM_API_KEY").ok(),
        scan_interval: Duration::from_secs(
            std::env::var("SCAN_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        ),
        log_tail_lines: std::env::var("LOG_TAIL_LINES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200),
        dashboard_port: std::env::var("DASHBOARD_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8088),
    };

    if let Err(e) = sre::run(cfg).await {
        tracing::error!("sre-agent fatal error: {e:#}");
        std::process::exit(1);
    }
}

fn env_require(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| {
        eprintln!("ERROR: required environment variable {key} is not set");
        std::process::exit(1);
    })
}
