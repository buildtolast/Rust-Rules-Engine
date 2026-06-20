# Rust Rules Engine

A lightweight Rust rebuild of a Kafka-Streams rule-routing + audit pipeline:
**consume `source-events` → evaluate rules per event → route matches to
`target-events` → persist one audit record per evaluation → expose analytics +
rules CRUD.**

Replaces a JVM + Kafka + Mongo + Redis stack with a far lighter one:

| Concern | Choice |
|---|---|
| Runtime | Rust + Tokio |
| Messaging | Redpanda (Kafka API) via `rdkafka`, exactly-once transactions |
| Rule eval | CEL (`cel-interpreter`) |
| Audit + analytics | ClickHouse (MergeTree + materialized views) |
| Rule store | Postgres (`sqlx`) with LISTEN/NOTIFY hot-reload |
| HTTP API | axum |

## Status

Bootstrap (S0): Cargo workspace, local infra, and CI are in place. See
[`plans/`](plans/) for the full construction blueprint and per-step detail.

## Quick start (infra)

```bash
./deploy/run.sh              # bring up Redpanda + ClickHouse + Postgres + rules-engine + frontend + SRE agent
./deploy/run.sh --rebuild    # clean rebuild of all Docker images (slow; use after Rust changes)
./deploy/run.sh --fast       # skip build, just restart containers
./deploy/run.sh --obs        # also start SigNoz tracing UI (OTEL collector + SigNoz on :3301)
./deploy/run.sh --down       # tear down all containers
```

Ports auto-increment if defaults are already in use. All flags can be combined:
`./deploy/run.sh --rebuild --obs --logs`

## Observability (optional)

The `--obs` flag starts a SigNoz overlay (`deploy/docker-compose.observability.yml`):

| Endpoint | Purpose |
|---|---|
| `http://localhost:3301` | SigNoz UI (traces, metrics, logs) |
| `localhost:4317` (gRPC) | OTEL collector — send spans here |
| `localhost:4318` (HTTP) | OTEL collector HTTP endpoint |

The SRE agent feeds LLM-generated trace insights into the **Tracing Insights** tab of the
frontend. No additional configuration is required; the app exports spans to the OTEL
collector automatically when it is running.

## Verification gate

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace
```
