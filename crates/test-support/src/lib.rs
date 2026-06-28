//! Test helpers shared across integration test crates.
//! Only compiled under `#[cfg(test)]` in downstream crates — keep it lean.

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Returns true if a TCP connection to `addr` succeeds within 1 second.
pub async fn probe_tcp(addr: &str) -> bool {
    timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

pub async fn probe_postgres() -> bool {
    let host = std::env::var("TEST_POSTGRES_ADDR")
        .unwrap_or_else(|_| "localhost:5432".into());
    probe_tcp(&host).await
}

pub async fn probe_clickhouse() -> bool {
    let host = std::env::var("TEST_CLICKHOUSE_ADDR")
        .unwrap_or_else(|_| "localhost:8123".into());
    probe_tcp(&host).await
}

pub async fn probe_kafka() -> bool {
    let host = std::env::var("TEST_KAFKA_ADDR")
        .unwrap_or_else(|_| "localhost:19092".into());
    probe_tcp(&host).await
}

/// Use at the top of an `#[ignore]` integration test.
/// If the probe returns false, prints a skip message and returns from the
/// calling function (test passes vacuously = "skipped").
///
/// Usage:
/// ```rust
/// #[tokio::test]
/// #[ignore = "requires live Postgres"]
/// async fn my_test() {
///     skip_if_unavailable!(probe_postgres(), "Postgres at localhost:5432");
///     // ... rest of test
/// }
/// ```
#[macro_export]
macro_rules! skip_if_unavailable {
    ($probe:expr, $label:expr) => {
        if !$probe.await {
            eprintln!("[SKIP] {} is not reachable — skipping integration test", $label);
            return;
        }
    };
}
