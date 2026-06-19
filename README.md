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
./deploy/run.sh          # bring up Redpanda + ClickHouse + Postgres
./deploy/run.sh --down   # tear down
```

## Verification gate

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace
```
