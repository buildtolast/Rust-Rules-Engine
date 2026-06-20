# Rust Rules Engine — Developer Guide

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Project Layout](#2-project-layout)
3. [Running the Stack](#3-running-the-stack)
4. [Environment Variables](#4-environment-variables)
5. [Day-to-Day Operations](#5-day-to-day-operations)
6. [Adding and Changing Rules](#6-adding-and-changing-rules)
7. [Adding a New Pipeline Feature](#7-adding-a-new-pipeline-feature)
8. [Adding Analytics Queries](#8-adding-analytics-queries)
9. [Database Operations](#9-database-operations)
10. [Testing](#10-testing)
11. [Build System](#11-build-system)
12. [Debugging Common Issues](#12-debugging-common-issues)
13. [Capacity and Scaling](#13-capacity-and-scaling)
14. [Upgrading Dependencies](#14-upgrading-dependencies)

---

## 1. Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Rust | 1.94+ | Workspace builds (pinned in `rust-toolchain.toml`) |
| Docker + Docker Compose | 24+ | Full stack |
| `rpk` | Redpanda CLI | Topic management (inside rre-redpanda container) |
| Node.js | 20+ | Frontend dev only |
| curl / jq | any | Manual API calls |

The `UNSLOTH_API_KEY` environment variable must be set in `~/.zshrc` if the SRE agent's LLM
integration is active. The key is loaded at runtime via `source ~/.zshrc` in `run.sh`; it is
never stored on disk by the application.

---

## 2. Project Layout

```
Rust-Rules-Engine/
├── bin/
│   ├── rules-engine/src/main.rs   # Wires all crates; starts HTTP + pipeline
│   └── sre-agent/src/main.rs      # Wires sre crate; starts analysis + dashboard
├── crates/
│   ├── core/                      # Domain types (Rule, SourceEvent, AuditRecord)
│   ├── eval/                      # CEL compilation + evaluation
│   ├── pipeline/                  # Consumer loop, rule cache, metrics
│   ├── store-postgres/            # Rule CRUD + LISTEN/NOTIFY
│   ├── store-clickhouse/          # Audit batch writer + analytics queries
│   ├── web/                       # axum HTTP API (routes, AppState)
│   └── sre/                       # SRE agent: Docker, LLM, ClickHouse writes
├── migrations/
│   ├── clickhouse/
│   │   ├── 0001_audits.sql        # audits table + MVs + backfill
│   │   └── 0002_sre_observations.sql
│   └── postgres/
│       └── 0001_rules.sql         # rules table + NOTIFY trigger
├── frontend/                      # React SPA (Vite + Tailwind)
│   └── src/
│       ├── App.tsx
│       ├── AnalyticsDashboard.tsx
│       ├── MetricsTab.tsx         # Live service metrics
│       ├── SreTab.tsx
│       └── ...
├── deploy/
│   ├── docker-compose.yml
│   ├── run.sh                     # One-command stack launcher
│   ├── Dockerfile.app             # Multi-stage Rust build + debian-slim runtime
│   ├── Dockerfile.sre
│   └── clickhouse/config.xml      # Server-level CH settings only
├── Cargo.toml                     # Workspace manifest (pinned deps)
└── docs/
    ├── architecture.md            # This document's sibling
    ├── developer-guide.md         # This document
    └── scaling-decisions.md
```

---

## 3. Running the Stack

### Full stack (normal)

```bash
./deploy/run.sh
```

This does:
1. Tears down any running containers
2. Auto-increments port numbers if defaults are in use
3. Loads `UNSLOTH_API_KEY` from `~/.zshrc`
4. Builds Docker images (uses cache by default)
5. Starts all 6 containers
6. Creates `source-events` and `target-events` with 12 partitions each (idempotent; skips if already exists)
7. Waits for all containers to reach "healthy" status

### Variants

```bash
# Clean rebuild (necessary after Rust code changes if you haven't changed any docker layer)
./deploy/run.sh --rebuild

# Skip build entirely — just restart containers (useful after config-only changes)
./deploy/run.sh --fast

# Start and immediately follow logs
./deploy/run.sh --logs

# Tear down everything
./deploy/run.sh --down

# Also start SigNoz observability overlay (OTEL collector + SigNoz UI on :3301)
./deploy/run.sh --obs

# Flags can be combined freely
./deploy/run.sh --rebuild --obs --logs
```

### Port overrides

```bash
# Run on non-default ports (e.g., running two instances simultaneously)
FRONTEND_PORT=3001 APP_PORT=8081 ./deploy/run.sh
```

### Rebuilding individual images

```bash
# From the deploy/ directory:
docker compose build app        # Rust binary only
docker compose build frontend   # React SPA only
docker compose build sre-agent  # SRE agent binary only

# Redeploy after build:
docker compose up -d --force-recreate app frontend
```

### Frontend dev mode (hot reload)

```bash
cd frontend
npm install
npm run dev   # Vite on localhost:5173, proxies /api/* to localhost:8080
```

---

## 4. Environment Variables

### rules-engine (app container)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | required | Postgres DSN: `postgres://rules:rules@postgres:5432/ruleaudit` |
| `CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DB` | `ruleaudit` | Database name |
| `CLICKHOUSE_USER` | `rules` | Username |
| `CLICKHOUSE_PASSWORD` | `rules` | Password |
| `KAFKA_BROKERS` | `localhost:19092` | Comma-separated broker list |
| `SOURCE_TOPIC` | `source-events` | Input topic |
| `TARGET_TOPIC` | `target-events` | Output topic for matched events |
| `CONSUMER_GROUP` | `rules-engine` | Consumer group ID |
| `TRANSACTIONAL_ID` | `rules-engine-txn` | EOS transactional producer ID (unique per instance) |
| `HTTP_PORT` | `8080` | Bind port for axum |
| `RUST_LOG` | `info` | Log filter (e.g., `pipeline=debug,info`) |

### sre-agent container

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_URL` | required | Same CH as main app |
| `LLM_BASE_URL` | `http://host.docker.internal:8888` | OpenAI-compatible LLM base URL |
| `LLM_MODEL` | `unsloth` | Model name sent in `model` field of chat completion |
| `LLM_API_KEY` | empty | Bearer token for LLM. If empty, no Authorization header is sent. |
| `SCAN_INTERVAL_SECS` | `60` | Seconds between full container scans |
| `LOG_TAIL_LINES` | `200` | Lines to tail from each container |
| `RUST_LOG` | `info` | Log filter |

---

## 5. Day-to-Day Operations

### Check stack health

```bash
curl -s http://localhost:8080/health/ready | jq
# {"status":"ready","services":{"postgres":{"status":"ok",...},...}}

curl -s http://localhost:8080/api/metrics | jq
# Live pipeline throughput, consumer lag, CH/PG stats
```

### Check consumer lag

```bash
docker exec rre-redpanda rpk group describe rules-engine --brokers localhost:9092
# TOTAL-LAG shows how far behind the pipeline consumer is
```

### View pipeline logs

```bash
docker compose -f deploy/docker-compose.yml logs -f app
# Set RUST_LOG=pipeline=debug for per-batch timing
```

### Simulate traffic

```bash
# Push 5000 events (responds immediately, processes in background)
curl -X POST "http://localhost:8080/api/simulation/push?count=5000"

# Push 10000 events (max cap)
curl -X POST "http://localhost:8080/api/simulation/push?count=10000"
```

### Recreate Kafka topics with new partition count

When changing partition count, existing topics must be deleted and recreated.
**Warning: this deletes all unconsumed messages.**

```bash
docker exec rre-redpanda rpk topic delete source-events target-events --brokers localhost:9092
docker exec rre-redpanda rpk topic create source-events --brokers localhost:9092 --partitions 24
docker exec rre-redpanda rpk topic create target-events --brokers localhost:9092 --partitions 24
```

### Query ClickHouse directly

```bash
# Row counts
docker exec rre-clickhouse clickhouse-client \
  --query "SELECT count() FROM ruleaudit.audits" \
  --user=rules --password=rules

# Recent observations
docker exec rre-clickhouse clickhouse-client \
  --query "SELECT container_name, severity, finding FROM ruleaudit.sre_observations ORDER BY observed_at DESC LIMIT 10" \
  --user=rules --password=rules

# Consumer group hourly match rate
docker exec rre-clickhouse clickhouse-client \
  --query "SELECT hour, rule_id, matched, unmatched FROM ruleaudit.agg_rule_hourly ORDER BY hour DESC LIMIT 20" \
  --user=rules --password=rules
```

### Kill SRE agent LLM if it hangs

```bash
# Find the Unsloth process on the host
lsof -i :8888
kill <PID>
# Restart Unsloth Studio from its UI/CLI
```

---

## 6. Adding and Changing Rules

### Via the UI

Navigate to `http://localhost:3000` → Rule Management tab → "Add New Rule".

- **Description**: plain English label
- **Expression**: CEL expression. The event payload is bound as `event`. Example:
  ```
  event.amount > 1000 && event.region == "US"
  ```
- **Active**: enables the rule immediately on save

The Postgres trigger fires `pg_notify('rules_changed')` on every write. The running pipeline
picks up the new rule set within milliseconds without restarting.

### Via the API

```bash
# Create
curl -X POST http://localhost:8080/api/rules \
  -H "Content-Type: application/json" \
  -d '{
    "description": "High-value EU order",
    "expression": "event.amount > 5000 && event.region == \"EU\"",
    "targetTopic": "target-events",
    "enabled": true
  }'

# Update (triggers hot-reload)
curl -X PUT http://localhost:8080/api/rules/<id> \
  -H "Content-Type: application/json" \
  -d '{"description": "...", "expression": "...", "targetTopic": "target-events", "enabled": true}'

# Disable without deleting
curl -X PUT http://localhost:8080/api/rules/<id> \
  -H "Content-Type: application/json" \
  -d '{"enabled": false, ...}'
```

### CEL expression reference

The variable `event` holds the deserialized JSON payload. Supported CEL operations:

```
# Field access
event.amount
event.region
event.metadata.source

# Comparison
event.amount > 1000
event.region == "US"

# Boolean operators
event.amount > 1000 && event.region == "US"
event.tier == "gold" || event.tier == "premium"

# Has-field check
has(event.order)

# List membership / exists macro
event.order.items.exists(i, i.price > 500)

# String starts-with
event.timestamp.startsWith("2024")

# Numeric range
event.metadata.priority <= 5
event.metadata.tax_rate >= 0.16
```

**Bad expression handling:** if the CEL expression fails to compile (parse error or parser
panic), the rule is skipped at cache load time with a WARN log. It does not crash the
pipeline. Fix the expression via PUT and the next NOTIFY will retry compilation.

---

## 7. Adding a New Pipeline Feature

### Pattern: add a new metric counter

1. Add the field to `PipelineCounters` in `crates/pipeline/src/metrics.rs`
2. Update `record_batch()` (or add a new method)
3. Read it in `crates/web/src/routes/metrics.rs` and add it to the response struct

### Pattern: add a new HTTP route

1. Add a handler function in `crates/web/src/routes/<feature>.rs`
2. Declare the module in `crates/web/src/routes/mod.rs`
3. Register the route in `crates/web/src/lib.rs` (`Router::new().route(...)`)

### Pattern: add a new ClickHouse analytics query

1. Add a Row struct + query function to `crates/store-clickhouse/src/analytics.rs`
2. Export from `crates/store-clickhouse/src/lib.rs`
3. Call from an HTTP handler in `crates/web/src/routes/analytics.rs`

### Pattern: add a new ClickHouse table

1. Create `migrations/clickhouse/000N_<name>.sql`
2. Load it via `include_str!()` in the appropriate store crate
3. Call `client.query(SQL).execute().await` at startup

ClickHouse migrations run every startup (`CREATE TABLE IF NOT EXISTS` is idempotent).

### Pattern: add a new Postgres column

1. Alter `migrations/postgres/0001_rules.sql` — add a `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`
   statement to the existing migration file. sqlx's `raw_sql()` runs the whole file;
   `IF NOT EXISTS` makes it idempotent.
2. Add the field to `Rule` in `crates/core/src/rule.rs`
3. Add the field to `RuleRow` and update the CRUD queries in `crates/store-postgres/src/lib.rs`
4. Add to request/response DTOs in the web crate

---

## 8. Adding Analytics Queries

### New time-series metric (add to existing MV)

If the new metric can be derived from audit rows at INSERT time:

1. Alter `agg_rule_hourly` to add a new column: `my_metric UInt64`
2. Alter `mv_rule_hourly` to compute it in the `SELECT`
3. Query `SUM(my_metric)` in analytics functions

Since the MV only captures future inserts, run a backfill manually:
```sql
-- Connect to ClickHouse and run:
INSERT INTO agg_rule_hourly SELECT ..., my_metric_expr AS my_metric FROM ruleaudit.audits GROUP BY hour, rule_id;
```

### New table for a different aggregation shape

1. Define a new `SummingMergeTree` or `AggregatingMergeTree` target table
2. Define a `MATERIALIZED VIEW ... TO <target> AS SELECT ...`
3. Add a backfill `INSERT INTO <target> SELECT ...` to the migration

### Querying raw audits for reports

Use `audits FINAL` to force merge (applies dedup at query time rather than waiting for
background merge):

```sql
SELECT audit_id, audit_type, source_event
FROM ruleaudit.audits FINAL
WHERE timestamp >= toDateTime(?) AND timestamp <= toDateTime(?)
  AND audit_type = ?
ORDER BY timestamp DESC
LIMIT 100
```

`FINAL` is slower than querying aggregated MVs. Use it only for audit report views where
record-level accuracy matters.

---

## 9. Database Operations

### Postgres: manual inspection

```bash
docker exec -it rre-postgres psql -U rules -d ruleaudit

-- Check current rules
SELECT id, description, enabled, version, updated_at FROM rules ORDER BY updated_at DESC;

-- Check active LISTEN connections (confirms hot-reload listener is alive)
SELECT pid, state, query FROM pg_stat_activity WHERE query LIKE '%LISTEN%';
```

### ClickHouse: check MV health

```bash
docker exec rre-clickhouse clickhouse-client --user=rules --password=rules

-- Are MVs populated?
SELECT count() FROM ruleaudit.agg_rule_hourly;
SELECT count() FROM ruleaudit.agg_hour_messages;

-- Dedup check: rows before/after FINAL
SELECT count() FROM ruleaudit.audits;
SELECT count() FROM ruleaudit.audits FINAL;

-- Force merge (triggers dedup; expensive, not needed in normal operation)
OPTIMIZE TABLE ruleaudit.audits FINAL;
```

### ClickHouse: config constraints

`deploy/clickhouse/config.xml` contains **server-level settings only**. Do not add
query-level settings (e.g., `max_bytes_before_external_group_by`) to this file. With
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` enabled, ClickHouse 24.8 crashes at startup (OOM/
segfault) when query-level settings appear in `config.d/`. Use `SET` in queries or
`users.xml` profiles instead.

Valid example of `config.xml`:
```xml
<clickhouse>
    <max_concurrent_queries>200</max_concurrent_queries>
</clickhouse>
```

### Redpanda: consumer group management

```bash
# Full lag status (run inside rre-redpanda container)
docker exec rre-redpanda rpk group describe rules-engine --brokers localhost:9092

# Reset consumer offsets to beginning (causes full reprocessing)
docker exec rre-redpanda rpk group seek rules-engine --to start --brokers localhost:9092

# List all topics and partition counts
docker exec rre-redpanda rpk topic list --brokers localhost:9092

# Inspect a topic's data
docker exec rre-redpanda rpk topic consume source-events --brokers localhost:9092 -n 5
```

---

## 10. Testing

### Unit/integration tests

```bash
# Full workspace (runs in-process unit tests; no Docker needed)
cargo test

# Specific crate
cargo test -p eval
cargo test -p rules-core

# With output (useful for debugging)
cargo test -p pipeline -- --nocapture
```

### Integration tests that need running services

The tests in `crates/store-clickhouse/tests/` and `crates/store-postgres/tests/` require
running databases. Start the stack first, then run with the appropriate env vars:

```bash
# These env vars must match the running containers
DATABASE_URL=postgres://rules:rules@localhost:5432/ruleaudit \
CLICKHOUSE_URL=http://localhost:8123 \
  cargo test -p store-clickhouse
```

### Test coverage (no coverage tooling installed by default)

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --html
open target/llvm-cov/html/index.html
```

### Linting

```bash
cargo clippy -- -D warnings
cargo fmt --check    # formatting check only (no modifications)
cargo fmt            # apply formatting
```

---

## 11. Build System

### Local build

```bash
# Debug build (fast compile, slow binary)
cargo build

# Release build (slow compile, fast binary — what Docker uses)
cargo build --release -p rules-engine
cargo build --release -p sre-agent
```

### Docker multi-stage build

`deploy/Dockerfile.app` stages:
1. `rust:1.94` builder — full Rust toolchain, copies workspace, runs `cargo build --release -p rules-engine`
2. `debian:trixie-slim` runtime — copies only the binary + migration files

The runtime image is ~50MB. The Rust binary is statically linked against musl where possible;
the debian base provides glibc for rdkafka's librdkafka dependency (which requires a C runtime).

**ClickHouse migrations** are embedded via `include_str!()` at compile time and also copied
into the runtime image at `/migrations/` for manual reference.

### Build caching in Docker

Each `COPY` in the Dockerfile is a layer. The layer `COPY Cargo.toml Cargo.lock ./` is
separate from `COPY crates/ crates/` — so a change to a Rust source file does not invalidate
the dependency download layer. Run `--rebuild` only when `Cargo.lock` or the Docker base
images change.

---

## 12. Debugging Common Issues

### Pipeline consuming at 0 msg/s

Symptom: `messages_per_sec = 0` in `/api/metrics`, consumer lag is 0 or not growing.

1. Check that `source-events` exists: `docker exec rre-redpanda rpk topic list --brokers localhost:9092`
2. Check that the pipeline started: `docker compose logs app | grep "pipeline started"`
3. Check that the consumer group is stable: `docker exec rre-redpanda rpk group describe rules-engine`
4. Push a small batch: `curl -X POST "http://localhost:8080/api/simulation/push?count=10"`

### Consumer lag growing, not draining

Symptom: `consumer_lag` in `/api/metrics` keeps increasing, never reaches 0.

1. How many partitions? `docker exec rre-redpanda rpk topic describe source-events`
2. One consumer instance can drain at most one partition at a time. If partitions = 12 and
   the pipeline is single-threaded through the EOS commit, throughput = single-core batch rate.
   Increase `BATCH_SIZE` or add more pipeline instances (each needs a unique `TRANSACTIONAL_ID`).
3. Check if the CH writer is blocking: `docker compose logs app | grep "clickhouse write"`
4. Check `avg_txn_ms` — if >500ms, the Kafka broker commit is slow; check Redpanda memory.

### ClickHouse OOM / exit 137

1. Check memory limits in `docker-compose.yml` (`CLICKHOUSE_MEM_LIMIT` defaults to 8G).
2. Do NOT put query-level settings in `config.xml` — this causes a startup crash with access
   management enabled. See [§9](#9-database-operations) for the constraint.
3. Try raising the Docker memory limit: `docker update --memory=12g rre-clickhouse`

### SRE agent showing "LLM Unavailable"

1. Verify Unsloth Studio is running on the host: `lsof -i :8888`
2. Verify the process is healthy: `curl http://localhost:8888/v1/models`
3. Check if the process is stuck (accepting TCP but not responding to HTTP): kill the process
   (`kill <PID>`) and restart Unsloth Studio.
4. Check the container can reach the host: `docker exec rre-sre-agent curl -sf http://host.docker.internal:8888/v1/models`
5. Verify `UNSLOTH_API_KEY` is set: `docker inspect rre-sre-agent | jq '.[].Config.Env'`

### Hot-reload not firing

Symptom: edited a rule, but the pipeline doesn't reflect the change.

1. Check the NOTIFY listener is alive: `docker exec rre-postgres psql -U rules -d ruleaudit -c "SELECT pid, query FROM pg_stat_activity WHERE query LIKE '%LISTEN%';"`
2. Check the hot-reload log: `docker compose logs app | grep "rules_changed\|rule cache reloaded"`
3. If the listener crashed, restart the app container: `docker compose restart app`

### Frontend showing stale data

The frontend makes API calls directly to port 8080 (proxied through nginx). If the app is
slow or restarting, requests will fail. Check: `curl http://localhost:8080/health` and
`curl http://localhost:3000/health`.

---

## 13. Capacity and Scaling

### Current single-instance limits

With the current configuration (BATCH_SIZE=2000, 12 partitions):

- **Kafka consumer throughput**: ~44,000 messages/sec under load (rayon parallel eval,
  measured with 101 rules per event on 8 cores)
- **ClickHouse write throughput**: limited by batch size and CH insert speed; 5000-row
  batches over HTTP can sustain ~500K rows/sec depending on CH resources
- **Rule count**: scales O(rules × messages/batch) in rayon — adding more rules increases
  eval time but stays proportional; 100 rules adds ~7ms per 2000-message batch on test hardware

### Scaling beyond one instance

To run multiple pipeline instances (for > 12 partitions / > 1 consumer):

1. Each instance **must** have a unique `TRANSACTIONAL_ID`. Without this, the EOS producer
   will fence the previous instance's transactions, causing a crash.
2. Kafka partitions are the parallelism ceiling: N instances can each own ⌊partitions/N⌋
   partitions. There is no benefit to running more instances than partitions.
3. The `RuleCache` is per-instance (in-process). Each instance receives the NOTIFY
   independently and reloads its own cache. This is correct behavior.

```yaml
# Example: second pipeline instance
app2:
  image: rre/rules-engine:latest
  environment:
    TRANSACTIONAL_ID: rules-engine-txn-2   # MUST be unique
    CONSUMER_GROUP: rules-engine           # Same group — Kafka distributes partitions
    HTTP_PORT: 8081
```

### Autoscale design (theoretical)

The autoscale signal is `consumer_lag` from `/api/metrics`:
- **Scale up**: lag > 10,000 for 30 seconds → add one pipeline instance
- **Scale down**: lag < 1,000 for 2 minutes → remove one pipeline instance
- **Hard ceiling**: `instances ≤ partition_count` (Kafka constraint)

In Kubernetes, this maps to an HPA on a custom metric exported from `/api/metrics`. In
Docker Swarm, it requires an external orchestrator polling the endpoint.

### Increasing partition count

Partitions can only be added, not removed, in Kafka/Redpanda. Increasing from 12 to 24:

```bash
# Redpanda supports in-place partition increase (unlike Kafka which requires recreation)
docker exec rre-redpanda rpk topic alter-config source-events --set num_partitions=24 --brokers localhost:9092
# OR delete + recreate:
docker exec rre-redpanda rpk topic delete source-events target-events
docker exec rre-redpanda rpk topic create source-events target-events --partitions 24
```

**Important:** if you recreate topics, unconsumed messages are lost. The consumer group
offset is also lost, meaning the pipeline will start from the earliest available offset
(which is the beginning, since the topic is new and empty).

---

## 14. Upgrading Dependencies

### Rust crates

```bash
# Update all workspace dependencies
cargo update

# Update a specific crate
cargo update -p rdkafka

# Audit for security advisories
cargo audit
```

After `cargo update`, rebuild Docker images with `./deploy/run.sh --rebuild`.

### rdkafka / librdkafka

rdkafka bundles librdkafka as a C dependency compiled by the Rust build. Upgrading rdkafka
will also upgrade librdkafka. Check the rdkafka changelog for breaking changes to EOS
configuration (particularly `transactional_id`, `enable.idempotence`, `acks` semantics).

### SigNoz images (`--obs` mode)

The observability overlay (`deploy/docker-compose.observability.yml`) pins two SigNoz images:

| Image | Current tag | Notes |
|---|---|---|
| `signoz/signoz` | `v0.129.0` | SigNoz UI + query service; tags use `v` prefix |
| `signoz/signoz-otel-collector` | `v0.144.5` | SigNoz-flavoured OTEL collector |

When upgrading, bump both together — the collector and the UI must be compatible.
Check [SigNoz releases](https://github.com/SigNoz/signoz/releases) for the matched pair.
The `0.x.x` (no `v` prefix) tag format does not exist on Docker Hub; always include the `v`.

### ClickHouse image

Test against the new version in a scratch environment before upgrading, specifically:
- Migration idempotency (CREATE TABLE IF NOT EXISTS)
- Access management settings (they affected config loading in 24.8)
- MV behavior on INSERT after version changes

### Redpanda image

Check compatibility with the rdkafka EOS flow. Redpanda occasionally has edge cases in
`send_offsets_to_transaction` that differ from Apache Kafka. Run the integration test suite
against the new version before shipping.
