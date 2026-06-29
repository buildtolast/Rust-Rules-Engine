use std::time::Duration;

#[tokio::main]
async fn main() {
    let _telemetry = telemetry::init("sre-agent");

    let cfg = sre::SreConfig {
        clickhouse_url: env_require("CLICKHOUSE_URL"),
        clickhouse_db: env_require("CLICKHOUSE_DB"),
        clickhouse_user: env_require("CLICKHOUSE_USER"),
        clickhouse_pass: env_require("CLICKHOUSE_PASSWORD"),
        llm_base_url: env_require("LLM_BASE_URL"),
        llm_model: std::env::var("LLM_MODEL").unwrap_or_else(|_| "unsloth".into()),
        llm_api_key: std::env::var("LLM_API_KEY").ok(),
        llm_timeout_secs: std::env::var("LLM_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120),
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
        auto_restart: std::env::var("AUTO_RESTART")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false),
        restart_cooldown_secs: std::env::var("RESTART_COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
        auto_tune: std::env::var("AUTO_TUNE")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false),
        auto_tune_compose_file: std::env::var("AUTO_TUNE_COMPOSE_FILE")
            .unwrap_or_else(|_| "/deploy/docker-compose.yml".into()),
        auto_tune_env_file: std::env::var("AUTO_TUNE_ENV_FILE")
            .unwrap_or_else(|_| "/deploy/.env".into()),
        auto_tune_cooldown_secs: std::env::var("AUTO_TUNE_COOLDOWN_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(600),
        probe: sre::probes::ProbeConfig {
            kafka_brokers: std::env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092".into()),
            clickhouse_url: std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://clickhouse:8123".into()),
            postgres_host: std::env::var("POSTGRES_HOST")
                .unwrap_or_else(|_| "postgres".into()),
            postgres_port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5432),
            app_url: std::env::var("APP_URL")
                .unwrap_or_else(|_| "http://app:8080".into()),
        },
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
