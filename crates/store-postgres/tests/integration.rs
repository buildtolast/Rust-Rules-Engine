//! Integration tests against a live Postgres (start with `deploy/run.sh`).
//! Run with: `cargo test -p store-postgres -- --ignored`.
//! Excluded from default/CI runs because they need infra.

use std::time::Duration;

use store_postgres::{
    connect, run_migrations, seed_default_rules, RuleChangeListener, RuleInput, RuleRepository,
};

const DSN: &str = "postgres://rules:rules@localhost:5432/ruleaudit";

fn input(expr: &str, enabled: bool) -> RuleInput {
    RuleInput {
        description: "test rule".into(),
        expression: expr.into(),
        target_topic: "target-events".into(),
        enabled,
    }
}

// One test controls table state end-to-end to avoid cross-test interference on
// the shared `rules` table.
#[tokio::test]
#[ignore = "requires live Postgres (deploy/run.sh)"]
async fn crud_seed_and_notify() {
    test_support::skip_if_unavailable!(
        test_support::probe_postgres(),
        "Postgres at localhost:5432"
    );
    let pool = connect(DSN).await.expect("connect");
    run_migrations(&pool).await.expect("migrations");

    // Clean slate. TRUNCATE does not fire the row-level NOTIFY trigger.
    sqlx::query("TRUNCATE rules")
        .execute(&pool)
        .await
        .expect("truncate");

    let repo = RuleRepository::new(pool.clone());

    // Seed is idempotent: 3 on empty, 0 when already populated.
    assert_eq!(seed_default_rules(&repo).await.unwrap(), 3);
    assert_eq!(seed_default_rules(&repo).await.unwrap(), 0);
    assert_eq!(repo.list().await.unwrap().len(), 3);

    // Listen only catches NOTIFYs emitted after LISTEN is issued.
    let mut listener = RuleChangeListener::connect(&pool).await.expect("listener");

    // create -> version 1, server-generated id, and a NOTIFY with that id.
    let created = repo.create(&input("event.x > 1", true)).await.unwrap();
    assert_eq!(created.version, 1);
    assert!(!created.id.is_empty());

    let payload = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("NOTIFY within 5s")
        .expect("recv ok");
    assert_eq!(payload, created.id, "NOTIFY payload is the changed rule id");

    // get
    let got = repo.get(&created.id).await.unwrap().expect("rule exists");
    assert_eq!(got.expression, "event.x > 1");

    // update bumps version and persists changes
    let updated = repo
        .update(&created.id, &input("event.x > 2", false))
        .await
        .unwrap()
        .expect("updated");
    assert_eq!(updated.version, 2);
    assert!(!updated.enabled);
    assert_eq!(updated.expression, "event.x > 2");

    // delete
    assert!(repo.delete(&created.id).await.unwrap());
    assert!(repo.get(&created.id).await.unwrap().is_none());
}
