// Requires a live ClickHouse instance — run with:
//   cargo test -p sre --test store_integration -- --ignored
use chrono::Utc;
use clickhouse::Client;
use sre::store::{SreObservation, SreStore};

fn test_client() -> Client {
    Client::default()
        .with_url("http://localhost:8123")
        .with_database("ruleaudit")
        .with_user("rules")
        .with_password("rules")
}

#[tokio::test]
#[ignore]
async fn dedup_same_hash_keeps_one_row() {
    let client = test_client();
    let mut store = SreStore::new(&client);

    let obs = SreObservation {
        observed_at: Utc::now(),
        container_name: "test-container".into(),
        severity: "INFO".into(),
        category: "normal".into(),
        finding: "all good".into(),
        proposed_fix: "No action required".into(),
        log_window_hash: "dedup-test-hash-12345".into(),
        log_snippet: "log line 1\nlog line 2".into(),
    };

    // Write the same observation twice — ReplacingMergeTree should keep one row
    store.write(&obs).await.expect("first write");
    store.write(&obs).await.expect("second write");
    store.end().await.expect("flush");

    // FINAL keyword forces dedup merge before reading
    let count: u64 = client
        .query(
            "SELECT count() FROM ruleaudit.sre_observations FINAL \
             WHERE log_window_hash = 'dedup-test-hash-12345'",
        )
        .fetch_one()
        .await
        .expect("count query");

    assert_eq!(count, 1, "ReplacingMergeTree should deduplicate same hash");
}
