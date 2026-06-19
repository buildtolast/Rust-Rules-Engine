//! Integration tests for RuleCache — require live Postgres (deploy/run.sh).
//! Run with: `cargo test -p pipeline -- --ignored`

use std::time::Duration;

use pipeline::{watch_and_reload, RuleCache};
use store_postgres::{connect, run_migrations, seed_default_rules, RuleChangeListener, RuleInput, RuleRepository};

const DSN: &str = "postgres://rules:rules@localhost:5432/ruleaudit";

#[tokio::test]
#[ignore = "requires live Postgres (deploy/run.sh)"]
async fn load_returns_only_enabled_rules() {
    let pool = connect(DSN).await.expect("connect");
    run_migrations(&pool).await.expect("migrations");
    let _: sqlx::postgres::PgQueryResult =
        sqlx::query("TRUNCATE rules").execute(&pool).await.expect("truncate");

    let repo = RuleRepository::new(pool.clone());

    // 2 enabled, 1 disabled
    repo.create(&RuleInput {
        description: "enabled 1".into(),
        expression: "event.x > 1".into(),
        target_topic: "t".into(),
        enabled: true,
    }).await.unwrap();
    repo.create(&RuleInput {
        description: "enabled 2".into(),
        expression: "event.y == \"a\"".into(),
        target_topic: "t".into(),
        enabled: true,
    }).await.unwrap();
    repo.create(&RuleInput {
        description: "disabled".into(),
        expression: "event.z < 0".into(),
        target_topic: "t".into(),
        enabled: false,
    }).await.unwrap();

    let cache = RuleCache::load(&repo).await.expect("load");
    let rules = cache.get();
    assert_eq!(rules.len(), 2, "only enabled rules should be compiled");
}

#[tokio::test]
#[ignore = "requires live Postgres (deploy/run.sh)"]
async fn hot_reload_swaps_on_notify() {
    let pool = connect(DSN).await.expect("connect");
    run_migrations(&pool).await.expect("migrations");
    let _: sqlx::postgres::PgQueryResult =
        sqlx::query("TRUNCATE rules").execute(&pool).await.expect("truncate");

    let repo = RuleRepository::new(pool.clone());
    seed_default_rules(&repo).await.expect("seed");

    let cache = RuleCache::load(&repo).await.expect("initial load");
    assert_eq!(cache.get().len(), 3);

    // Start listener BEFORE the update so we don't miss the NOTIFY.
    let listener = RuleChangeListener::connect(&pool).await.expect("listener");

    // Spawn the watch task.
    let cache_clone = cache.clone();
    let repo_clone = repo.clone();
    tokio::spawn(async move {
        watch_and_reload(cache_clone, repo_clone, listener).await.ok();
    });

    // Disable one rule — triggers NOTIFY via the Postgres trigger.
    let all = repo.list().await.unwrap();
    let first_id = &all[0].id.clone();
    repo.update(first_id, &RuleInput {
        description: "disabled".into(),
        expression: all[0].expression.clone(),
        target_topic: all[0].target_topic.clone(),
        enabled: false,
    }).await.unwrap();

    // Wait up to 5 s for the watch task to swap the cache.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cache.get().len() == 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("cache was not reloaded within 5 s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(cache.get().len(), 2, "cache should reflect the disabled rule");
}

#[tokio::test]
#[ignore = "requires live Postgres (deploy/run.sh)"]
async fn bad_rule_expression_is_skipped_not_fatal() {
    let pool = connect(DSN).await.expect("connect");
    run_migrations(&pool).await.expect("migrations");
    let _: sqlx::postgres::PgQueryResult =
        sqlx::query("TRUNCATE rules").execute(&pool).await.expect("truncate");

    let repo = RuleRepository::new(pool.clone());

    repo.create(&RuleInput {
        description: "good rule".into(),
        expression: "event.x > 1".into(),
        target_topic: "t".into(),
        enabled: true,
    }).await.unwrap();
    // Intentionally malformed CEL — compile() returns CompileError, should be skipped.
    repo.create(&RuleInput {
        description: "bad rule".into(),
        expression: "event.amount >".into(),
        target_topic: "t".into(),
        enabled: true,
    }).await.unwrap();

    let cache = RuleCache::load(&repo).await.expect("load should not fail for bad rules");
    assert_eq!(cache.get().len(), 1, "bad rule skipped, good rule retained");
}
