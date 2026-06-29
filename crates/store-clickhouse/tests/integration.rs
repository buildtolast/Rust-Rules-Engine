//! Integration tests against a live ClickHouse (start with `deploy/run.sh`).
//! Run with: `cargo test -p store-clickhouse -- --ignored`.
//! Excluded from default/CI runs because they need infra.

use chrono::Utc;
use rules_core::{audit_id, AuditRecord, AuditType};
use store_clickhouse::{client, run_migrations, AuditWriter, ClickHouseConfig};

fn rec(topic: &str, partition: i32, offset: i64, rule_id: &str) -> AuditRecord {
    AuditRecord {
        audit_id: audit_id(topic, partition, offset, rule_id),
        rule_id: rule_id.into(),
        schema_version: 1,
        audit_type: AuditType::Matched,
        reason: None,
        source_event: "{\"amount\":1}".into(),
        routed_event: Some("{\"amount\":1}".into()),
        source_topic: topic.into(),
        partition,
        offset,
        timestamp: Utc::now(),
        parse_time_nano: 1,
        eval_time_nano: 2,
        total_time_nano: 3,
    }
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    c: u64,
}

#[tokio::test]
#[ignore = "requires live ClickHouse (deploy/run.sh)"]
async fn audits_persist_and_dedup_by_audit_id() {
    test_support::skip_if_unavailable!(
        test_support::probe_clickhouse(),
        "ClickHouse at localhost:8123"
    );
    let cfg = ClickHouseConfig::default();
    let client = client(&cfg);
    run_migrations(&client).await.expect("migrations");

    // Unique topic isolates this run from prior data sharing the audits table.
    let topic = format!("test-{}", Utc::now().timestamp_nanos_opt().unwrap());

    let mut writer = AuditWriter::new(&client, &cfg);
    writer.write(&rec(&topic, 0, 1, "r1")).await.unwrap();
    writer.write(&rec(&topic, 0, 2, "r1")).await.unwrap();
    writer.write(&rec(&topic, 0, 3, "r2")).await.unwrap();
    // Duplicate of the first row (identical auditId) — must collapse on dedup.
    writer.write(&rec(&topic, 0, 1, "r1")).await.unwrap();
    writer.end().await.unwrap();

    // FINAL forces ReplacingMergeTree dedup at read time.
    let row: CountRow = client
        .query("SELECT count() AS c FROM audits FINAL WHERE source_topic = ?")
        .bind(&topic)
        .fetch_one()
        .await
        .expect("count query");

    assert_eq!(row.c, 3, "duplicate auditId must collapse to one row");
}
