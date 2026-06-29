//! HTTP handler tests for the web crate.
//!
//! Tests are grouped into two tiers:
//!   1. Unit tests that run without any external services (fast, always run).
//!   2. Integration tests that need Postgres + ClickHouse + Kafka — these are
//!      marked `#[ignore]` and only run when `INTEGRATION=1` is set.
//!
//! Run unit tests:
//!   cargo test -p web
//!
//! Run integration tests (requires running docker-compose stack):
//!   INTEGRATION=1 cargo test -p web -- --include-ignored

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use http_body_util::BodyExt;

    use tower_service::Service;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Read the full body of an axum `Response<Body>` into a `Vec<u8>`.
    async fn collect_body(body: Body) -> Vec<u8> {
        body.collect()
            .await
            .expect("collect body")
            .to_bytes()
            .to_vec()
    }

    /// Drive a `Router` like `oneshot` does: poll_ready then call.
    ///
    /// We avoid importing `tower::ServiceExt` because the workspace pins
    /// `tower = "0.4"` while axum 0.7 uses `tower 0.5` internally, creating
    /// two rlib candidates. Instead we use `tower_service::Service` directly —
    /// `Router` is always ready so skipping `poll_ready` is safe here.
    async fn call(mut router: Router, req: Request<Body>) -> axum::response::Response {
        router.call(req).await.expect("router call failed")
    }

    // ── /health (no external services required) ───────────────────────────────

    /// Build a minimal router that only wires the liveness probe.
    /// This avoids constructing `AppState` (which needs PG/CH/Kafka) for a
    /// handler that doesn't use state at all.
    fn health_only_router() -> Router {
        Router::new().route("/health", get(crate::routes::health::health))
    }

    #[tokio::test]
    async fn test_health_returns_200() {
        let response = call(
            health_only_router(),
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_body_has_status_ok() {
        let response = call(
            health_only_router(),
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let bytes = collect_body(response.into_body()).await;
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).expect("response is valid JSON");
        assert_eq!(
            json["status"], "ok",
            r#"body should contain {{"status":"ok"}}"#
        );
    }

    #[tokio::test]
    async fn test_health_content_type_is_json() {
        let response = call(
            health_only_router(),
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("application/json"),
            "expected application/json content-type, got: {ct}"
        );
    }

    #[tokio::test]
    async fn test_unknown_route_returns_404() {
        let response = call(
            health_only_router(),
            Request::builder()
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── integration tests (require INTEGRATION=1 + running stack) ────────────

    /// Build AppState from environment variables the same way `bin/server.rs` would.
    /// Returns `None` if any required env var is missing — the caller should skip.
    async fn try_build_app_state() -> Option<crate::AppState> {
        let pg_url = std::env::var("DATABASE_URL").ok()?;
        let ch_url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
        let kafka_brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
        let source_topic =
            std::env::var("SOURCE_TOPIC").unwrap_or_else(|_| "source-events".to_string());

        // Postgres
        let pool = store_postgres::connect(&pg_url).await.ok()?;
        let rules = store_postgres::RuleRepository::new(pool);

        // ClickHouse — `Client` uses the builder pattern; `Default` points to localhost:8123.
        let ch_client = store_clickhouse::ClickHouseClient::default().with_url(&ch_url);

        // Kafka producer
        use rdkafka::config::ClientConfig;
        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .ok()?;

        // Pipeline counters + rule cache (loads enabled rules from PG)
        let counters = std::sync::Arc::new(pipeline::PipelineCounters::new());
        let rule_cache = pipeline::RuleCache::load(&rules).await.ok()?;

        Some(crate::AppState {
            rules,
            ch_client,
            producer: std::sync::Arc::new(producer),
            source_topic,
            kafka_brokers,
            counters,
            rule_cache,
        })
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_rules_list_returns_200_with_array() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let response = call(
            crate::router(state, vec![]),
            Request::builder()
                .uri("/api/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = collect_body(response.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(json.is_array(), "GET /api/rules should return a JSON array");
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_create_rule_returns_201_and_retrieve_returns_200() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let app = crate::router(state, vec![]);

        // POST /api/rules
        let body = serde_json::json!({
            "description": "integration test rule",
            "expression":  "event.amount > 0",
            "targetTopic": "test-output",
            "enabled":     true
        });
        let create_response = call(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri("/api/rules")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await;

        assert_eq!(create_response.status(), StatusCode::CREATED);
        let bytes = collect_body(create_response.into_body()).await;
        let created: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        let id = created["id"]
            .as_str()
            .expect("response has 'id' field")
            .to_owned();

        // GET /api/rules/:id
        let get_response = call(
            app,
            Request::builder()
                .uri(format!("/api/rules/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(get_response.status(), StatusCode::OK);
        let bytes = collect_body(get_response.into_body()).await;
        let fetched: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(fetched["id"].as_str(), Some(id.as_str()));
        assert_eq!(
            fetched["description"].as_str(),
            Some("integration test rule")
        );
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_get_nonexistent_rule_returns_404() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let response = call(
            crate::router(state, vec![]),
            Request::builder()
                .uri("/api/rules/00000000-0000-0000-0000-000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_analytics_stats_returns_200_with_object_body() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let response = call(
            crate::router(state, vec![]),
            Request::builder()
                .uri("/api/analytics/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = collect_body(response.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(
            json.is_object(),
            "GET /api/analytics/stats should return a JSON object, got: {json}"
        );
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_readiness_probe_returns_valid_shape() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let response = call(
            crate::router(state, vec![]),
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        // 200 (all up) or 503 (some degraded) — both are valid response shapes
        let status = response.status();
        assert!(
            status == StatusCode::OK || status == StatusCode::SERVICE_UNAVAILABLE,
            "unexpected status: {status}"
        );
        let bytes = collect_body(response.into_body()).await;
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert!(
            json["status"].is_string(),
            "readiness body should have a 'status' string field"
        );
        assert!(
            json["services"].is_object(),
            "readiness body should have a 'services' object"
        );
    }

    #[tokio::test]
    #[ignore = "requires INTEGRATION=1 and a running Postgres+ClickHouse+Kafka stack"]
    async fn test_export_rejects_unknown_audit_type() {
        test_support::skip_if_unavailable!(
            async {
                test_support::probe_postgres().await
                    && test_support::probe_clickhouse().await
                    && test_support::probe_kafka().await
            },
            "full stack (Postgres + ClickHouse + Kafka)"
        );
        let Some(state) = try_build_app_state().await else {
            eprintln!("skipping: could not build AppState (check DATABASE_URL)");
            return;
        };
        let response = call(
            crate::router(state, vec![]),
            Request::builder()
                .uri("/api/reports/export?type=BOGUS")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
