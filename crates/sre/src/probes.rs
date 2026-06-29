use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::debug;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceCheck {
    pub name: String,
    pub ok: bool,
    pub latency_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SystemProbeResult {
    pub all_ok: bool,
    pub services: Vec<ServiceCheck>,
    pub probed_at: DateTime<Utc>,
}

pub struct ProbeConfig {
    /// Comma-separated broker addresses, e.g. "redpanda-0:9092,redpanda-1:9092,redpanda-2:9092"
    pub kafka_brokers: String,
    pub clickhouse_url: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub app_url: String,
}

pub async fn probe_all(cfg: &ProbeConfig) -> SystemProbeResult {
    let app_health_url = format!("{}/health", cfg.app_url);
    let (kafka, clickhouse, postgres, app) =
        tokio::join!(probe_kafka(&cfg.kafka_brokers),
                     probe_clickhouse(&cfg.clickhouse_url),
                     probe_tcp("postgres", &cfg.postgres_host, cfg.postgres_port),
                     probe_http("app", &app_health_url),);

    let services = vec![kafka, clickhouse, postgres, app];
    let all_ok = services.iter().all(|s| s.ok);

    SystemProbeResult { all_ok,
                        services,
                        probed_at: Utc::now() }
}

async fn probe_kafka(brokers: &str) -> ServiceCheck {
    let addrs: Vec<&str> = brokers.split(',').map(str::trim).collect();
    if addrs.is_empty() {
        return ServiceCheck { name: "kafka".into(),
                              ok: false,
                              latency_ms: 0,
                              error: Some("no brokers configured".into()) };
    }

    let start = std::time::Instant::now();
    let mut reachable = 0usize;
    let mut last_error: Option<String> = None;

    for addr in &addrs {
        match connect_tcp(addr).await {
            Ok(()) => reachable += 1,
            Err(e) => {
                debug!("kafka probe {addr} failed: {e}");
                last_error = Some(e);
            }
        }
    }

    let latency_ms = start.elapsed().as_millis() as u64;
    let majority = (addrs.len() / 2) + 1;
    let ok = reachable >= majority;

    ServiceCheck { name: "kafka".into(),
                   ok,
                   latency_ms,
                   error: if ok {
                       None
                   } else {
                       Some(last_error.unwrap_or_else(|| {
                                          format!("only {reachable}/{} brokers reachable",
                                                  addrs.len())
                                      }))
                   } }
}

async fn probe_clickhouse(url: &str) -> ServiceCheck {
    let ping_url = format!("{}/ping", url.trim_end_matches('/'));
    probe_http("clickhouse", &ping_url).await
}

async fn probe_http(name: &str, url: &str) -> ServiceCheck {
    let start = std::time::Instant::now();
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT)
                                                 .connect_timeout(PROBE_TIMEOUT)
                                                 .build()
    {
        Ok(c) => c,
        Err(e) => {
            return ServiceCheck { name: name.into(),
                                  ok: false,
                                  latency_ms: 0,
                                  error: Some(format!("failed to build http client: {e}")) };
        }
    };

    match client.get(url).send().await {
        Ok(resp) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            let ok = resp.status().is_success();
            ServiceCheck { name: name.into(),
                           ok,
                           latency_ms,
                           error: if ok {
                               None
                           } else {
                               Some(format!("HTTP {}", resp.status()))
                           } }
        }
        Err(e) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            let msg = if e.is_timeout() {
                format!("timeout after {}s", PROBE_TIMEOUT.as_secs())
            } else {
                e.to_string()
            };
            ServiceCheck { name: name.into(),
                           ok: false,
                           latency_ms,
                           error: Some(msg) }
        }
    }
}

async fn probe_tcp(name: &str, host: &str, port: u16) -> ServiceCheck {
    let addr = format!("{host}:{port}");
    let start = std::time::Instant::now();
    match connect_tcp(&addr).await {
        Ok(()) => ServiceCheck { name: name.into(),
                                 ok: true,
                                 latency_ms: start.elapsed().as_millis() as u64,
                                 error: None },
        Err(e) => ServiceCheck { name: name.into(),
                                 ok: false,
                                 latency_ms: start.elapsed().as_millis() as u64,
                                 error: Some(e) },
    }
}

async fn connect_tcp(addr: &str) -> Result<(), String> {
    tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("timeout after {}s", PROBE_TIMEOUT.as_secs()))?
        .map(|_| ())
        .map_err(|e| e.to_string())
}
