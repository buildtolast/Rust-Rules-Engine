# OTEL Tracing + LLM Insights Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add OpenTelemetry tracing to the rules engine, feed trace/timing data to the local LLM for analysis, and display actionable performance insights in a new "Tracing" tab in the frontend.

**Architecture:** OTEL is the abstraction layer — the Rust code exports spans via OTLP gRPC to a configurable endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT`). SigNoz runs as an optional Docker Compose overlay for visual trace exploration. For LLM analysis, the SRE agent (which already owns ClickHouse + LLM access) queries the existing `audits` table for per-rule timing data, feeds it to the local LLM, and exposes `/api/sre/traces/insights` — which nginx already proxies to the SRE agent at port 8088.

**Tech Stack:**
- `opentelemetry 0.27`, `opentelemetry_sdk 0.27`, `opentelemetry-otlp 0.27` (OTLP gRPC via tonic)
- `tracing-opentelemetry 0.28` (bridge between `tracing` crate and OTEL)
- `tower-http TraceLayer` for HTTP request spans (zero-touch)
- SigNoz via `deploy/docker-compose.observability.yml` (separate overlay, no ClickHouse conflict)
- ClickHouse `audits` table (already has `eval_time_nano`, `parse_time_nano`) as LLM data source
- Local LLM via existing `AnalysisClient` in the `sre` crate

**Parallelism:** Tasks 1+2 must run first (foundation). Tasks 3, 4, 5, 6 can run in parallel after Task 2. Tasks 7+8 can start after Task 2. Task 9 can start after Task 8. Task 10 needs Task 9.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `crates/telemetry/Cargo.toml` | OTEL deps for the new crate |
| Create | `crates/telemetry/src/lib.rs` | `init(service_name)` — sets up OTEL + tracing-subscriber, returns `ShutdownGuard` |
| Modify | `Cargo.toml` | Add `telemetry` to workspace members; add OTEL deps to `[workspace.dependencies]` |
| Modify | `bin/rules-engine/Cargo.toml` | Add `telemetry` dep |
| Modify | `bin/rules-engine/src/main.rs` | Replace `tracing_subscriber::fmt().init()` with `telemetry::init("rules-engine")` |
| Modify | `bin/sre-agent/Cargo.toml` | Add `telemetry` dep |
| Modify | `bin/sre-agent/src/main.rs` | Replace `tracing_subscriber::fmt().init()` with `telemetry::init("sre-agent")` |
| Modify | `crates/pipeline/src/consumer.rs` | Add `info_span!("pipeline.batch")` around batch loop; record eval_ms/txn_ms/lag as span attrs |
| Modify | `crates/web/Cargo.toml` | Add `trace` feature to `tower-http` |
| Modify | `crates/web/src/lib.rs` | Add `TraceLayer::new_for_http()` to axum router |
| Modify | `crates/store-clickhouse/src/lib.rs` | `#[instrument]` on `write_batch` and `flush` |
| Modify | `crates/store-postgres/src/lib.rs` | `#[instrument]` on `RuleRepository` CRUD methods |
| Create | `deploy/docker-compose.observability.yml` | SigNoz stack (separate overlay) |
| Create | `deploy/otel-collector-config.yaml` | OTEL collector pipeline → SigNoz |
| Modify | `deploy/docker-compose.yml` | Add `OTEL_EXPORTER_OTLP_ENDPOINT` env var to `app` and `sre-agent` services |
| Create | `crates/sre/src/trace_analysis.rs` | Query `audits` ClickHouse table → format LLM prompt → parse response → `TraceInsights` struct |
| Modify | `crates/sre/src/lib.rs` | `pub mod trace_analysis;` |
| Modify | `crates/sre/src/dashboard.rs` | Add `GET /api/sre/traces/insights` route; call `trace_analysis::fetch_insights` |
| Modify | `frontend/src/types.ts` | Add `RulePerf`, `TraceInsights` types |
| Create | `frontend/src/TracingInsightsTab.tsx` | Polls `/api/sre/traces/insights`; shows perf table + LLM insights panel |
| Modify | `frontend/src/App.tsx` | Add `'tracing'` to tab union; add tab button; render `TracingInsightsTab` |

---

## Task 1: OTEL workspace dependencies + telemetry crate scaffold

**Files:**
- Create: `crates/telemetry/Cargo.toml`
- Create: `crates/telemetry/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add OTEL crates to workspace `[workspace.dependencies]`**

In `Cargo.toml`, add inside `[workspace.dependencies]`:

```toml
opentelemetry          = { version = "0.27", features = ["trace"] }
opentelemetry_sdk      = { version = "0.27", features = ["rt-tokio", "trace"] }
opentelemetry-otlp     = { version = "0.27", features = ["tonic", "trace"] }
tracing-opentelemetry  = "0.28"
```

Also add `telemetry` to `[workspace] members`:

```toml
members = [
    "crates/core",
    "crates/eval",
    "crates/telemetry",       # ← add this line
    "crates/store-clickhouse",
    ...
]
```

- [ ] **Step 2: Create `crates/telemetry/Cargo.toml`**

```toml
[package]
name = "telemetry"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
publish = false

[dependencies]
opentelemetry         = { workspace = true }
opentelemetry_sdk     = { workspace = true }
opentelemetry-otlp    = { workspace = true }
tracing-opentelemetry = { workspace = true }
tracing               = { workspace = true }
tracing-subscriber    = { workspace = true }
tokio                 = { workspace = true }
```

- [ ] **Step 3: Create `crates/telemetry/src/lib.rs`**

```rust
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::Resource,
    runtime,
    trace::{self as sdktrace, Sampler},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Returned by `init` — shuts down the OTEL tracer provider on drop.
pub struct ShutdownGuard;

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        global::shutdown_tracer_provider();
    }
}

/// Initialise tracing + OTEL for a binary. Call once at the top of `main`.
///
/// Backend is controlled entirely by the `OTEL_EXPORTER_OTLP_ENDPOINT` env var
/// (default: `http://localhost:4317`). Swap the endpoint to switch between
/// SigNoz, Jaeger, Datadog, or any OTLP-compatible backend — the Rust code
/// does not change.
///
/// Sampling rate is controlled by `OTEL_SAMPLE_RATE` (default: 0.1 = 10%).
/// Set to 1.0 in development for full sampling.
pub fn init(service_name: &'static str) -> ShutdownGuard {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".into());

    let sample_rate: f64 = std::env::var("OTEL_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.1);

    global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name),
        opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .with_trace_config(
            sdktrace::Config::default()
                .with_resource(resource)
                .with_sampler(Sampler::ParentBased(Box::new(
                    Sampler::TraceIdRatioBased(sample_rate),
                ))),
        )
        .install_batch(runtime::Tokio)
        .expect("OTLP tracer install failed");

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    ShutdownGuard
}
```

- [ ] **Step 4: Verify it compiles in isolation**

```bash
cd /Users/chiya/GIT/Rust-Rules-Engine
cargo check -p telemetry 2>&1 | tail -20
```

Expected: no errors (warnings about unused imports are OK at this stage).

- [ ] **Step 5: Commit**

```bash
git add crates/telemetry/ Cargo.toml Cargo.lock
git commit -m "feat(telemetry): add OTEL crate with backend-agnostic init via OTLP"
```

---

## Task 2: Wire telemetry into both binaries

**Files:**
- Modify: `bin/rules-engine/Cargo.toml`
- Modify: `bin/rules-engine/src/main.rs`
- Modify: `bin/sre-agent/Cargo.toml`
- Modify: `bin/sre-agent/src/main.rs`

- [ ] **Step 1: Add telemetry dep to rules-engine**

In `bin/rules-engine/Cargo.toml`, add to `[dependencies]`:

```toml
telemetry = { path = "../../crates/telemetry" }
```

- [ ] **Step 2: Replace tracing init in rules-engine main.rs**

In `bin/rules-engine/src/main.rs`, replace:

```rust
use tracing_subscriber::EnvFilter;

// and in main():
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
```

With:

```rust
// in main():
    let _telemetry = telemetry::init("rules-engine");
```

Remove the `use tracing_subscriber::EnvFilter;` import (it's now internal to the telemetry crate).

- [ ] **Step 3: Add telemetry dep to sre-agent**

In `bin/sre-agent/Cargo.toml`, add to `[dependencies]`:

```toml
telemetry = { path = "../../crates/telemetry" }
```

- [ ] **Step 4: Replace tracing init in sre-agent main.rs**

In `bin/sre-agent/src/main.rs`, replace:

```rust
use tracing_subscriber::EnvFilter;

// and in main():
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
```

With:

```rust
// in main():
    let _telemetry = telemetry::init("sre-agent");
```

- [ ] **Step 5: Verify both binaries compile**

```bash
cargo check -p rules-engine -p sre-agent 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add bin/rules-engine/ bin/sre-agent/
git commit -m "feat: wire OTEL telemetry init into rules-engine and sre-agent binaries"
```

---

## Task 3: Instrument pipeline consumer with batch spans

**Files:**
- Modify: `crates/pipeline/src/consumer.rs`

The pipeline's hot path runs in a `spawn_blocking` thread (rayon-based). We add one `info_span!` per batch — NOT per message or per rule evaluation, which would generate millions of spans. Batch-level spans with aggregate attributes (eval_ms, txn_ms, lag) give OTEL the signal without span overhead.

- [ ] **Step 1: Add span around the batch processing loop**

In `crates/pipeline/src/consumer.rs`, inside the `loop` block, immediately after the `if batch.is_empty() { continue; }` check, wrap the batch processing phases in a span:

```rust
// ── Phase 1: parallel parse + eval (rayon) ────────────────────
let batch_span = tracing::info_span!(
    "pipeline.batch",
    "batch.size"    = batch.len(),
    "batch.eval_ms" = tracing::field::Empty,
    "batch.txn_ms"  = tracing::field::Empty,
    "kafka.lag"     = tracing::field::Empty,
    "audit.count"   = tracing::field::Empty,
);
let _batch_enter = batch_span.enter();
```

Place `let _batch_enter = batch_span.enter();` before Phase 1 starts and ensure it's in scope until after the `counters.record_batch(...)` call.

- [ ] **Step 2: Record span attributes after computing metrics**

After `let txn_ms = txn_start.elapsed().as_millis();` and before the `counters.record_batch(...)` call, add:

```rust
batch_span.record("batch.eval_ms", eval_ms as i64);
batch_span.record("batch.txn_ms", txn_ms as i64);
batch_span.record("kafka.lag", lag);
batch_span.record("audit.count", audit_count as i64);
```

- [ ] **Step 3: Add parse span inside the rayon closure**

Inside the `batch.par_iter().map(|msg| { ... })` closure, wrap the event parsing:

```rust
let _parse_span = tracing::debug_span!(
    "event.parse",
    "kafka.topic"     = %msg.topic,
    "kafka.partition" = msg.partition,
    "kafka.offset"    = msg.offset,
)
.entered();
```

Place this before `rules_core::SourceEvent::from_kafka(...)`. Note: `debug_span!` means these only appear when `RUST_LOG` includes `debug` — they are no-ops in production `info` level.

- [ ] **Step 4: Verify compilation**

```bash
cargo check -p pipeline 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add crates/pipeline/src/consumer.rs
git commit -m "feat(pipeline): add OTEL batch spans with eval/txn latency attributes"
```

---

## Task 4: Instrument web routes with TraceLayer

**Files:**
- Modify: `crates/web/Cargo.toml`
- Modify: `crates/web/src/lib.rs`

`TraceLayer::new_for_http()` automatically creates an OTEL-compatible span per HTTP request with method, URI, status code, and latency. Zero per-handler changes needed.

- [ ] **Step 1: Enable `trace` feature on tower-http in web crate**

In `crates/web/Cargo.toml`, change:

```toml
tower-http = { workspace = true }
```

To:

```toml
tower-http = { workspace = true, features = ["trace"] }
```

Also ensure the workspace `tower-http` dep has the trace feature. In `Cargo.toml` (workspace root), change:

```toml
tower-http = { version = "0.6", features = ["cors"] }
```

To:

```toml
tower-http = { version = "0.6", features = ["cors", "trace"] }
```

- [ ] **Step 2: Add TraceLayer to the axum router**

In `crates/web/src/lib.rs`, add the import at the top:

```rust
use tower_http::trace::TraceLayer;
```

In the `router` function, add `TraceLayer` **before** the cors layer (layers apply bottom-up in axum, so this order means TraceLayer wraps everything):

```rust
pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(routes::health::health))
        // ... all other routes unchanged ...
        .route("/api/simulation/push", post(routes::simulation::push))
        .layer(TraceLayer::new_for_http())   // ← add this line
        .layer(cors)
        .with_state(state)
}
```

- [ ] **Step 3: Verify compilation**

```bash
cargo check -p web 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add crates/web/Cargo.toml crates/web/src/lib.rs Cargo.toml
git commit -m "feat(web): add tower-http TraceLayer for automatic HTTP request spans"
```

---

## Task 5: Instrument store layers

**Files:**
- Modify: `crates/store-clickhouse/src/lib.rs`
- Modify: `crates/store-postgres/src/lib.rs`

- [ ] **Step 1: Instrument ClickHouse `flush` (the actual write path)**

In `crates/store-clickhouse/src/lib.rs`, add `#[tracing::instrument]` to `flush`:

```rust
#[tracing::instrument(level = "info", skip(self), fields(row_count = self.buffer.len()))]
pub async fn flush(&mut self) -> Result<(), Error> {
    if self.buffer.is_empty() {
        return Ok(());
    }
    let mut insert = self.client.insert::<AuditRow>("audits").await?;
    for row in &self.buffer {
        insert.write(row).await?;
    }
    insert.end().await?;
    self.buffer.clear();
    Ok(())
}
```

Also instrument `write_batch`:

```rust
#[tracing::instrument(level = "debug", skip(self, recs), fields(row_count = recs.len()))]
pub async fn write_batch(&mut self, recs: &[AuditRecord]) -> Result<(), Error> {
```

- [ ] **Step 2: Read store-postgres lib.rs to find CRUD methods**

```bash
grep -n "pub async fn\|pub fn" /Users/chiya/GIT/Rust-Rules-Engine/crates/store-postgres/src/lib.rs | head -30
```

- [ ] **Step 3: Instrument Postgres RuleRepository methods**

For each public method on `RuleRepository` (e.g. `list`, `get`, `create`, `update`, `delete`), add `#[tracing::instrument(level = "debug", skip(self))]`. Example:

```rust
#[tracing::instrument(level = "debug", skip(self))]
pub async fn list(&self) -> Result<Vec<Rule>, sqlx::Error> {
```

Apply the same pattern to all `pub async fn` methods on `RuleRepository`.

- [ ] **Step 4: Verify compilation**

```bash
cargo check -p store-clickhouse -p store-postgres 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add crates/store-clickhouse/src/lib.rs crates/store-postgres/src/lib.rs
git commit -m "feat(stores): instrument ClickHouse flush and Postgres repo with tracing spans"
```

---

## Task 6: SigNoz docker-compose overlay + OTEL env vars

**Files:**
- Create: `deploy/docker-compose.observability.yml`
- Create: `deploy/otel-collector-config.yaml`
- Modify: `deploy/docker-compose.yml`

SigNoz runs its own ClickHouse instance (separate from the audit ClickHouse). The overlay adds SigNoz + its ClickHouse + the OTEL collector. The main compose adds only the `OTEL_EXPORTER_OTLP_ENDPOINT` env var pointing at the collector.

- [ ] **Step 1: Create `deploy/otel-collector-config.yaml`**

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024

exporters:
  otlp:
    endpoint: signoz-otelcollector:4317
    tls:
      insecure: true
  debug:
    verbosity: basic

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp]
```

- [ ] **Step 2: Create `deploy/docker-compose.observability.yml`**

```yaml
# Run alongside the main compose to enable SigNoz observability:
#   docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.observability.yml up -d
#
# SigNoz UI: http://localhost:3301
# To switch to Jaeger: set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 and
# replace the signoz-* services with jaegertracing/all-in-one:1.57

name: rre-obs

services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.108.0
    container_name: rre-otel-collector
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml:ro
    ports:
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
    depends_on:
      - signoz-otelcollector
    networks:
      - rre_default

  signoz-clickhouse:
    image: clickhouse/clickhouse-server:24.1.2-alpine
    container_name: rre-signoz-clickhouse
    environment:
      CLICKHOUSE_DB: signoz_metrics
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: 27ff0399-0d3a-4bd8-919d-17c2181e6fb9
    volumes:
      - rre-signoz-ch-data:/var/lib/clickhouse
    deploy:
      resources:
        limits:
          memory: 2G
    networks:
      - rre_default

  signoz-otelcollector:
    image: signoz/signoz-otel-collector:0.111.25
    container_name: rre-signoz-otelcollector
    environment:
      CLICKHOUSE_URL: tcp://signoz-clickhouse:9000
    depends_on:
      signoz-clickhouse:
        condition: service_started
    networks:
      - rre_default

  signoz:
    image: signoz/signoz:0.55.0
    container_name: rre-signoz
    environment:
      STORAGE: clickhouse
      ClickHouseUrl: http://admin:27ff0399-0d3a-4bd8-919d-17c2181e6fb9@signoz-clickhouse:8123
      SIGNOZ_LOCAL_DB_PATH: /var/lib/signoz/signoz.db
    ports:
      - "3301:3301"   # SigNoz UI
    volumes:
      - rre-signoz-data:/var/lib/signoz
    depends_on:
      - signoz-clickhouse
      - signoz-otelcollector
    networks:
      - rre_default

volumes:
  rre-signoz-ch-data:
  rre-signoz-data:

networks:
  rre_default:
    external: true
    name: rre_default
```

- [ ] **Step 3: Add OTEL env vars to app services in `deploy/docker-compose.yml`**

In the `app` service `environment` block, add:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: ${OTEL_ENDPOINT:-http://otel-collector:4317}
OTEL_SAMPLE_RATE: ${OTEL_SAMPLE_RATE:-0.1}
```

In the `sre-agent` service `environment` block, add:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: ${OTEL_ENDPOINT:-http://otel-collector:4317}
OTEL_SAMPLE_RATE: ${OTEL_SAMPLE_RATE:-1.0}
```

(SRE agent gets 100% sampling — it's low-volume enough that full traces are useful.)

- [ ] **Step 4: Commit**

```bash
git add deploy/docker-compose.observability.yml deploy/otel-collector-config.yaml deploy/docker-compose.yml
git commit -m "feat(infra): add SigNoz observability overlay + OTEL collector config"
```

---

## Task 7: SRE trace analysis module (LLM-powered)

**Files:**
- Create: `crates/sre/src/trace_analysis.rs`
- Modify: `crates/sre/src/lib.rs`

The SRE agent already has `AnalysisClient` (LLM) and ClickHouse access. This task adds a module that queries the existing `audits` table for per-rule timing data, formats a structured LLM prompt, and parses the JSON response.

- [ ] **Step 1: Add the module declaration in `crates/sre/src/lib.rs`**

After the existing `pub mod store;` line, add:

```rust
pub mod trace_analysis;
```

And add the public re-export at the bottom of the module (or wherever appropriate):

```rust
pub use trace_analysis::{TraceInsights, fetch_insights};
```

- [ ] **Step 2: Create `crates/sre/src/trace_analysis.rs`**

```rust
//! Query the `audits` ClickHouse table for per-rule timing data, feed it to
//! the local LLM, and return structured performance insights.

use crate::analysis::AnalysisClient;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct RulePerf {
    pub rule_id: String,
    pub eval_count: u64,
    pub avg_eval_ms: f64,
    pub p95_eval_ms: f64,
    pub p99_eval_ms: f64,
    pub avg_parse_ms: f64,
    pub error_rate_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceInsights {
    pub generated_at: DateTime<Utc>,
    pub window_minutes: u32,
    pub rule_perf: Vec<RulePerf>,
    pub llm_insights: Vec<String>,
    pub llm_bottlenecks: Vec<String>,
    pub llm_recommendations: Vec<String>,
    pub llm_available: bool,
    pub total_evals: u64,
    pub avg_eval_ms_overall: f64,
}

#[derive(Deserialize)]
struct LlmResponse {
    #[serde(default)]
    insights: Vec<String>,
    #[serde(default)]
    top_bottlenecks: Vec<String>,
    #[serde(default)]
    recommendations: Vec<String>,
}

// ── ClickHouse query ─────────────────────────────────────────────────────────

const WINDOW_MINUTES: u32 = 10;

async fn query_rule_perf(ch: &Client) -> anyhow::Result<Vec<RulePerf>> {
    let sql = format!(
        r#"
        SELECT
            rule_id,
            count()                                          AS eval_count,
            round(avg(eval_time_nano) / 1e6, 3)             AS avg_eval_ms,
            round(quantile(0.95)(eval_time_nano) / 1e6, 3)  AS p95_eval_ms,
            round(quantile(0.99)(eval_time_nano) / 1e6, 3)  AS p99_eval_ms,
            round(avg(parse_time_nano) / 1e6, 3)            AS avg_parse_ms,
            round(countIf(audit_type = 2) * 100.0 / count(), 2) AS error_rate_pct
        FROM audits
        WHERE timestamp > now() - INTERVAL {WINDOW_MINUTES} MINUTE
        GROUP BY rule_id
        ORDER BY p95_eval_ms DESC
        LIMIT 20
        "#
    );

    let rows = ch
        .query(&sql)
        .fetch_all::<RulePerf>()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse query error: {e}"))?;

    Ok(rows)
}

// ── LLM prompt builder ───────────────────────────────────────────────────────

fn build_prompt(rule_perf: &[RulePerf]) -> String {
    let total_evals: u64 = rule_perf.iter().map(|r| r.eval_count).sum();
    let overall_avg = if rule_perf.is_empty() {
        0.0
    } else {
        rule_perf.iter().map(|r| r.avg_eval_ms * r.eval_count as f64).sum::<f64>()
            / total_evals.max(1) as f64
    };

    let table = rule_perf
        .iter()
        .map(|r| {
            format!(
                "  {:<36} | evals={:>8} | avg={:>7.3}ms | p95={:>7.3}ms | p99={:>7.3}ms | parse={:>6.3}ms | err={:.1}%",
                r.rule_id, r.eval_count, r.avg_eval_ms, r.p95_eval_ms, r.p99_eval_ms, r.avg_parse_ms, r.error_rate_pct
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"You are an SRE expert analysing a Rust-based CEL rules engine.

## Per-rule performance (last {WINDOW_MINUTES} minutes, sorted by p95 eval latency)
  rule_id                              | evals    | avg       | p95       | p99       | parse     | err%
{table}

## Overall
- Total evaluations in window: {total_evals}
- Weighted avg eval latency: {overall_avg:.3} ms

## Task
Identify performance problems and actionable optimisations.

Return ONLY a JSON object (no markdown fences, no preamble) with this exact shape:
{{
  "insights":         ["one-line observation 1", "..."],
  "top_bottlenecks":  ["rule_id or component causing the most pain 1", "..."],
  "recommendations":  ["specific actionable fix 1", "..."]
}}

Limit each array to 5 items max. Be specific — name rule IDs, thresholds, and techniques."#
    )
}

// ── Public API ───────────────────────────────────────────────────────────────

pub async fn fetch_insights(ch: &Client, llm: &AnalysisClient) -> TraceInsights {
    let rule_perf = match query_rule_perf(ch).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!("trace_analysis: ClickHouse query failed: {e}");
            return TraceInsights {
                generated_at: Utc::now(),
                window_minutes: WINDOW_MINUTES,
                rule_perf: vec![],
                llm_insights: vec!["ClickHouse query unavailable".into()],
                llm_bottlenecks: vec![],
                llm_recommendations: vec![],
                llm_available: false,
                total_evals: 0,
                avg_eval_ms_overall: 0.0,
            };
        }
    };

    let total_evals: u64 = rule_perf.iter().map(|r| r.eval_count).sum();
    let avg_eval_ms_overall = if rule_perf.is_empty() {
        0.0
    } else {
        rule_perf.iter().map(|r| r.avg_eval_ms * r.eval_count as f64).sum::<f64>()
            / total_evals.max(1) as f64
    };

    if rule_perf.is_empty() {
        return TraceInsights {
            generated_at: Utc::now(),
            window_minutes: WINDOW_MINUTES,
            rule_perf,
            llm_insights: vec!["No audit data in the last 10 minutes.".into()],
            llm_bottlenecks: vec![],
            llm_recommendations: vec!["Run the simulation to generate events.".into()],
            llm_available: true,
            total_evals,
            avg_eval_ms_overall,
        };
    }

    let prompt = build_prompt(&rule_perf);
    info!("trace_analysis: calling LLM with {} rules", rule_perf.len());

    let (llm_insights, llm_bottlenecks, llm_recommendations, llm_available) =
        match llm.raw_complete(&prompt).await {
            Ok(raw) => {
                match serde_json::from_str::<LlmResponse>(&raw) {
                    Ok(resp) => (resp.insights, resp.top_bottlenecks, resp.recommendations, true),
                    Err(e) => {
                        warn!("trace_analysis: LLM JSON parse failed: {e}. Raw: {raw:.200}");
                        (
                            vec![format!("LLM returned unparseable response: {}", &raw[..raw.len().min(100)])],
                            vec![],
                            vec![],
                            true,
                        )
                    }
                }
            }
            Err(e) => {
                warn!("trace_analysis: LLM call failed: {e}");
                (vec!["Local LLM unavailable".into()], vec![], vec![], false)
            }
        };

    TraceInsights {
        generated_at: Utc::now(),
        window_minutes: WINDOW_MINUTES,
        rule_perf,
        llm_insights,
        llm_bottlenecks,
        llm_recommendations,
        llm_available,
        total_evals,
        avg_eval_ms_overall,
    }
}
```

- [ ] **Step 3: Add `raw_complete` method to `AnalysisClient`**

`trace_analysis.rs` calls `llm.raw_complete(&prompt)` — this is a simpler version of the existing `analyze` method that sends a raw prompt and returns the raw text.

Open `crates/sre/src/analysis.rs` and add this method to `impl AnalysisClient`:

```rust
/// Send a raw prompt and return the raw text response from the LLM.
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
        messages: vec![Msg { role: "user", content: prompt }],
        temperature: 0.2,
        max_tokens: 1024,
    };

    let resp: Resp = self
        .client
        .post(format!("{}/v1/chat/completions", self.base_url))
        .json(&req)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(resp
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .unwrap_or_default())
}
```

Note: look at the existing `analyze` method in `analysis.rs` to confirm the field names (`base_url`, `model`, `client`) match. If they differ, align with the existing field names.

- [ ] **Step 4: Verify compilation**

```bash
cargo check -p sre 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add crates/sre/src/trace_analysis.rs crates/sre/src/lib.rs crates/sre/src/analysis.rs
git commit -m "feat(sre): add trace_analysis module — LLM-powered rule perf insights from ClickHouse"
```

---

## Task 8: SRE dashboard — `/api/sre/traces/insights` endpoint

**Files:**
- Modify: `crates/sre/src/dashboard.rs`

The SRE dashboard already serves `/api/sre/status`, `/api/sre/findings`, etc. Add one more route that nginx already proxies (the `/api/sre/` prefix rule covers it automatically).

The insights are computed on-demand (not cached) since ClickHouse queries are fast and the LLM call is the bottleneck. For production, add a TTL cache wrapping `Arc<RwLock<Option<TraceInsights>>>`.

- [ ] **Step 1: Add `llm` and `cfg` fields to the dashboard `AppState`**

In `crates/sre/src/dashboard.rs`, the local `AppState` struct currently has:

```rust
#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<SreState>>,
    tx: broadcast::Sender<Finding>,
    ch: Client,
}
```

Change it to:

```rust
#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<SreState>>,
    tx: broadcast::Sender<Finding>,
    ch: Client,
    llm: crate::analysis::AnalysisClient,
}
```

- [ ] **Step 2: Add the insights handler**

Add this function in `dashboard.rs`:

```rust
async fn trace_insights(
    State(app): State<AppState>,
) -> Json<crate::trace_analysis::TraceInsights> {
    let insights = crate::trace_analysis::fetch_insights(&app.ch, &app.llm).await;
    Json(insights)
}
```

- [ ] **Step 3: Add the route**

In the `serve` function where routes are defined, add:

```rust
.route("/api/sre/traces/insights", get(trace_insights))
```

- [ ] **Step 4: Update the `serve` function signature**

The `serve` function currently takes `state`, `tx`, `ch`, `port`. Add the `llm` parameter:

```rust
pub async fn serve(
    state: Arc<RwLock<SreState>>,
    tx: broadcast::Sender<Finding>,
    ch: Client,
    llm: crate::analysis::AnalysisClient,
    port: u16,
) {
```

Update the `AppState` construction inside `serve`:

```rust
let app_state = AppState { state, tx, ch, llm };
```

- [ ] **Step 5: Update the `serve` call in `lib.rs`**

In `crates/sre/src/lib.rs`, find the `dash_handle` block:

```rust
let dash_handle = {
    let state = state.clone();
    let tx = tx.clone();
    let port = cfg.dashboard_port;
    let dash_ch = ch_client(&cfg);
    tokio::spawn(async move {
        dashboard::serve(state, tx, dash_ch, port).await;
    })
};
```

Change it to:

```rust
let dash_handle = {
    let state = state.clone();
    let tx = tx.clone();
    let port = cfg.dashboard_port;
    let dash_ch = ch_client(&cfg);
    let dash_llm = AnalysisClient::new(
        &cfg.llm_base_url,
        &cfg.llm_model,
        cfg.llm_api_key.clone(),
        cfg.llm_timeout_secs,
    );
    tokio::spawn(async move {
        dashboard::serve(state, tx, dash_ch, dash_llm, port).await;
    })
};
```

- [ ] **Step 6: Verify compilation**

```bash
cargo check -p sre 2>&1 | tail -20
```

Expected: no errors.

- [ ] **Step 7: Manual smoke test (if services are running)**

```bash
curl -s http://localhost:8088/api/sre/traces/insights | jq '{total_evals, avg_eval_ms_overall, llm_available}'
```

Expected: JSON with `total_evals` (may be 0 if no events processed), `llm_available: true/false`.

- [ ] **Step 8: Commit**

```bash
git add crates/sre/src/dashboard.rs crates/sre/src/lib.rs
git commit -m "feat(sre): expose /api/sre/traces/insights endpoint with LLM-powered rule analysis"
```

---

## Task 9: Frontend `TracingInsightsTab` component

**Files:**
- Modify: `frontend/src/types.ts`
- Create: `frontend/src/TracingInsightsTab.tsx`

- [ ] **Step 1: Add types to `frontend/src/types.ts`**

Append at the end of `types.ts`:

```typescript
export interface RulePerf {
  rule_id: string;
  eval_count: number;
  avg_eval_ms: number;
  p95_eval_ms: number;
  p99_eval_ms: number;
  avg_parse_ms: number;
  error_rate_pct: number;
}

export interface TraceInsights {
  generated_at: string;
  window_minutes: number;
  rule_perf: RulePerf[];
  llm_insights: string[];
  llm_bottlenecks: string[];
  llm_recommendations: string[];
  llm_available: boolean;
  total_evals: number;
  avg_eval_ms_overall: number;
}
```

- [ ] **Step 2: Create `frontend/src/TracingInsightsTab.tsx`**

```tsx
import React, { useState, useEffect, useCallback } from 'react';
import { Activity, AlertTriangle, Lightbulb, TrendingUp, RefreshCw, Cpu } from 'lucide-react';
import type { TraceInsights, RulePerf } from './types';

const SRE_BASE = '/api/sre';
const POLL_INTERVAL_MS = 60_000;

function severityColor(p95: number): string {
  if (p95 > 10) return 'text-red-400';
  if (p95 > 2) return 'text-yellow-400';
  return 'text-green-400';
}

function PerfTable({ rows }: { rows: RulePerf[] }) {
  if (rows.length === 0) {
    return (
      <div className="text-center text-slate-500 py-8 text-sm">
        No evaluation data in the current window. Run the simulation to generate events.
      </div>
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-slate-500 uppercase tracking-wider">
            <th className="text-left py-2 pr-4">Rule ID</th>
            <th className="text-right pr-4">Evals</th>
            <th className="text-right pr-4">Avg (ms)</th>
            <th className="text-right pr-4">P95 (ms)</th>
            <th className="text-right pr-4">P99 (ms)</th>
            <th className="text-right pr-4">Parse (ms)</th>
            <th className="text-right">Err %</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.rule_id} className="border-t border-slate-700/50 hover:bg-slate-700/20">
              <td className="py-2 pr-4 font-mono text-slate-300 max-w-[200px] truncate" title={r.rule_id}>
                {r.rule_id}
              </td>
              <td className="text-right pr-4 text-slate-400">{r.eval_count.toLocaleString()}</td>
              <td className={`text-right pr-4 ${severityColor(r.avg_eval_ms)}`}>
                {r.avg_eval_ms.toFixed(3)}
              </td>
              <td className={`text-right pr-4 font-semibold ${severityColor(r.p95_eval_ms)}`}>
                {r.p95_eval_ms.toFixed(3)}
              </td>
              <td className={`text-right pr-4 ${severityColor(r.p99_eval_ms)}`}>
                {r.p99_eval_ms.toFixed(3)}
              </td>
              <td className="text-right pr-4 text-slate-500">{r.avg_parse_ms.toFixed(3)}</td>
              <td className={`text-right ${r.error_rate_pct > 5 ? 'text-red-400' : 'text-slate-400'}`}>
                {r.error_rate_pct.toFixed(1)}%
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function InsightsList({ title, items, icon, color }: {
  title: string;
  items: string[];
  icon: React.ReactNode;
  color: string;
}) {
  if (items.length === 0) return null;
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
      <div className={`flex items-center gap-2 mb-3 font-semibold text-sm ${color}`}>
        {icon}
        {title}
      </div>
      <ul className="space-y-2">
        {items.map((item, i) => (
          <li key={i} className="text-slate-300 text-sm flex gap-2">
            <span className="text-slate-600 shrink-0">{i + 1}.</span>
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function TracingInsightsTab() {
  const [data, setData] = useState<TraceInsights | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastFetch, setLastFetch] = useState<Date | null>(null);

  const fetchInsights = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(`${SRE_BASE}/traces/insights`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json: TraceInsights = await resp.json();
      setData(json);
      setLastFetch(new Date());
    } catch (e: any) {
      setError(e.message ?? 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchInsights();
    const id = setInterval(fetchInsights, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [fetchInsights]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-100 flex items-center gap-2">
            <Cpu className="w-5 h-5 text-violet-400" />
            Tracing Insights
          </h2>
          <p className="text-xs text-slate-500 mt-0.5">
            Rule evaluation performance · LLM analysis · {data?.window_minutes ?? 10}-minute window
          </p>
        </div>
        <div className="flex items-center gap-3">
          {data && (
            <div className="text-xs text-slate-500">
              {lastFetch ? `Updated ${lastFetch.toLocaleTimeString()}` : ''}
              {data.llm_available ? (
                <span className="ml-2 text-green-500">● LLM online</span>
              ) : (
                <span className="ml-2 text-red-500">● LLM offline</span>
              )}
            </div>
          )}
          <button
            onClick={fetchInsights}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-md disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-900/30 border border-red-700 rounded-lg p-3 text-red-300 text-sm flex gap-2">
          <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
          {error}
        </div>
      )}

      {/* Summary stats */}
      {data && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: 'Total Evals', value: data.total_evals.toLocaleString(), icon: <Activity className="w-4 h-4" /> },
            { label: 'Avg Eval', value: `${data.avg_eval_ms_overall.toFixed(3)} ms`, icon: <TrendingUp className="w-4 h-4" /> },
            { label: 'Rules Tracked', value: data.rule_perf.length.toString(), icon: <Cpu className="w-4 h-4" /> },
            { label: 'Window', value: `${data.window_minutes} min`, icon: <Activity className="w-4 h-4" /> },
          ].map(({ label, value, icon }) => (
            <div key={label} className="bg-slate-800 border border-slate-700 rounded-lg p-3">
              <div className="text-slate-500 text-xs flex items-center gap-1.5 mb-1">{icon}{label}</div>
              <div className="text-slate-100 font-semibold text-lg">{value}</div>
            </div>
          ))}
        </div>
      )}

      {/* LLM insights */}
      {data && (
        <div className="grid md:grid-cols-3 gap-4">
          <InsightsList
            title="Observations"
            items={data.llm_insights}
            icon={<Lightbulb className="w-4 h-4" />}
            color="text-blue-400"
          />
          <InsightsList
            title="Top Bottlenecks"
            items={data.llm_bottlenecks}
            icon={<AlertTriangle className="w-4 h-4" />}
            color="text-yellow-400"
          />
          <InsightsList
            title="Recommendations"
            items={data.llm_recommendations}
            icon={<TrendingUp className="w-4 h-4" />}
            color="text-green-400"
          />
        </div>
      )}

      {/* Performance table */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
        <h3 className="text-sm font-semibold text-slate-300 mb-4 flex items-center gap-2">
          <Activity className="w-4 h-4 text-violet-400" />
          Per-Rule Performance (sorted by P95 latency)
        </h3>
        {loading && !data ? (
          <div className="text-center text-slate-500 text-sm py-8">Loading...</div>
        ) : (
          <PerfTable rows={data?.rule_perf ?? []} />
        )}
      </div>

      {/* SigNoz link */}
      <div className="text-xs text-slate-600 text-center">
        For distributed trace waterfalls, open{' '}
        <a
          href="http://localhost:3301"
          target="_blank"
          rel="noopener noreferrer"
          className="text-violet-400 hover:underline"
        >
          SigNoz UI
        </a>{' '}
        (requires <code>docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.observability.yml up</code>)
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/TracingInsightsTab.tsx frontend/src/types.ts
git commit -m "feat(frontend): add TracingInsightsTab component with LLM insights + perf table"
```

---

## Task 10: Wire TracingInsightsTab into App.tsx

**Files:**
- Modify: `frontend/src/App.tsx`

- [ ] **Step 1: Add the import**

At the top of `frontend/src/App.tsx`, add:

```tsx
import { TracingInsightsTab } from './TracingInsightsTab';
```

- [ ] **Step 2: Extend the tab union type**

Find the line:

```tsx
const [activeTab, setActiveTab] = useState<'management' | 'analytics' | 'simulator' | 'reports' | 'sre' | 'metrics' | 'config'>('management');
```

Change to:

```tsx
const [activeTab, setActiveTab] = useState<'management' | 'analytics' | 'simulator' | 'reports' | 'sre' | 'metrics' | 'config' | 'tracing'>('management');
```

- [ ] **Step 3: Add the tab button**

Find the tab navigation buttons (the section with the other tab buttons like `'metrics'`, `'config'`, etc.). Add a new button in the same style — look at how the existing buttons are rendered and follow the exact same pattern. Add after the `'sre'` tab button:

```tsx
<button
  onClick={() => setActiveTab('tracing')}
  className={`flex items-center gap-1.5 px-3 py-2 text-sm rounded-md transition-colors ${
    activeTab === 'tracing'
      ? 'bg-slate-700 text-slate-100'
      : 'text-slate-400 hover:text-slate-300 hover:bg-slate-700/50'
  }`}
>
  <Cpu className="w-4 h-4" />
  Tracing
</button>
```

Add `Cpu` to the existing lucide-react import at the top of `App.tsx` if not already present.

- [ ] **Step 4: Add the tab render case**

Find the section in the JSX where tabs are conditionally rendered (the `activeTab === 'sre'` block). Add after it:

```tsx
{activeTab === 'tracing' && (
  <TracingInsightsTab />
)}
```

- [ ] **Step 5: Verify TypeScript compiles**

```bash
cd /Users/chiya/GIT/Rust-Rules-Engine/frontend && npm run build 2>&1 | tail -30
```

Expected: build succeeds with no type errors.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.tsx
git commit -m "feat(frontend): add Tracing tab wired to TracingInsightsTab"
```

---

## Self-Review

**Spec coverage check:**
- ✅ OTEL tracing injected into Rust code — Tasks 1–5
- ✅ Backend-agnostic via `OTEL_EXPORTER_OTLP_ENDPOINT` env var — Task 1 (`telemetry::init`)
- ✅ SigNoz as the default visual backend — Task 6
- ✅ Tracing data fed to local LLM for insights — Task 7 (`trace_analysis::fetch_insights`)
- ✅ SRE agent exposes the analysis — Task 8 (`/api/sre/traces/insights`)
- ✅ Frontend new tab shows data — Tasks 9+10
- ✅ Sub-agent executable — each task is self-contained with exact file paths and complete code

**Placeholder scan:** No TODOs, no "implement later", no references to undefined types. `raw_complete` method defined in Task 7 Step 3; used in `trace_analysis.rs` in the same task. `TraceInsights` defined in Task 7; used in Task 8 and Task 9 with matching field names.

**Type consistency:**
- `TraceInsights` Rust struct (Task 7) → `TraceInsights` TS type (Task 9): field names aligned (`total_evals`, `avg_eval_ms_overall`, `rule_perf`, `llm_insights`, `llm_bottlenecks`, `llm_recommendations`, `llm_available`, `window_minutes`, `generated_at`)
- `RulePerf` Rust struct and TS type: all fields match
- `fetch_insights(ch, llm)` defined in Task 7, called in Task 8 — signature consistent
- `dashboard::serve(state, tx, ch, llm, port)` updated in Task 8 Step 4 and the call site in Step 5 — consistent

**Parallel execution note for orchestrator:** After Task 2 completes, dispatch Tasks 3, 4, 5, 6, and 7 simultaneously to four sub-agents. Task 8 waits on Task 7. Task 9 can be dispatched in parallel with Task 8 (it has no Rust dependency). Task 10 waits on Task 9.
