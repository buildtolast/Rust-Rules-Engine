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

## Application features

### Rules engine
- **CEL expression evaluation** — rules are stored in Postgres and hot-reloaded via LISTEN/NOTIFY with no restart required.
- **Exactly-once Kafka pipeline** — consumes `source-events`, evaluates each event against all active rules, routes matches to `target-events`, and writes one audit record per evaluation to ClickHouse.
- **Rules CRUD API** — create, update, enable/disable, and delete rules via the HTTP API or the frontend UI.
- **WAL-backed audit buffer** — audit records are written to a local WAL file before ClickHouse. On restart any uncommitted batches are replayed automatically, so no audit records are lost during transient ClickHouse outages.
- **CORS control** — set `ALLOWED_ORIGINS` (comma-separated) to restrict cross-origin access. Defaults to `http://localhost:3000`.

### Analytics & tracing
- **Real-time analytics** — ClickHouse materialized views track matched/unmatched/errored counts and per-rule latency (avg, p95, p99) with no additional aggregation jobs.
- **Tracing Insights** — the SRE agent calls an LLM to summarise recent OTEL trace data and surfaces bottlenecks and recommendations in the frontend **Tracing Insights** tab.

### Frontend dashboard
- **Container health grid** — live view of every running container with status, healthcheck result, CPU %, and memory usage bar (colour-coded: green <60%, amber 60–80%, red ≥80%). One-shot init/migration containers are excluded.
- **Outage banner & incidents** — active outages are surfaced at the top of the page; the incidents timeline shows restores and auto-restart events.
- **SRE findings feed** — LLM-generated findings with severity, category, and proposed fix appear in real time via SSE.
- **LLM availability badge** — reflects the real state of the local inference server, probed every scan cycle.

## SRE agent

The SRE agent runs as two replicas alongside the application and continuously monitors the stack.

### Capabilities

| Capability | Detail |
|---|---|
| **Container scanning** | Polls Docker every `SCAN_INTERVAL_SECS` (default 5s) for running state, healthcheck, CPU, and memory |
| **Log analysis** | Tails the last `LOG_TAIL_LINES` lines per container and sends WARN/ERROR lines to the LLM for root-cause analysis |
| **Auto-restart** | Restarts stopped containers via the Docker API with a configurable cooldown (`RESTART_COOLDOWN_SECS`, default 300s) |
| **Memory pressure detection** | Emits WARN at ≥60% and CRITICAL at ≥80% of the container memory limit — no LLM call needed |
| **Auto-tune** | On CRITICAL memory, calculates a new limit (`used / 0.4`), writes it to `.env`, and runs `docker compose up -d --no-deps <service>` — zero manual intervention |
| **Service probes** | Actively probes Kafka, ClickHouse, Postgres, and the app HTTP endpoint each cycle |
| **Trace insights** | Periodically queries ClickHouse for rule-latency data and sends it to the LLM for bottleneck analysis |
| **LLM probe** | Sends a lightweight ping to the inference server each cycle so the availability badge is always current |

### Auto-tune configuration

Auto-tune is opt-in. Enable it in `deploy/.env`:

```bash
AUTO_TUNE=true                  # enable automatic memory limit adjustment
AUTO_TUNE_COOLDOWN_SECS=600     # minimum seconds between tunes for the same service (default 600)
```

When triggered the agent:
1. Calculates the new limit: `ceil(mem_used / 0.4)` MB
2. Writes `<SERVICE>_MEM_LIMIT=<new>M` into `deploy/.env`
3. Runs `docker compose up -d --no-deps <service>` — the container is recreated with the new limit

The following services are tunable: `app`, `clickhouse`, `postgres`, `postgres-replica`, `frontend`, `redpanda-0/1/2`. The SRE agent itself is excluded to avoid self-restart loops.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `SCAN_INTERVAL_SECS` | `5` | Seconds between full scan cycles |
| `LOG_TAIL_LINES` | `200` | Log lines tailed per container per cycle |
| `AUTO_RESTART` | `true` | Restart stopped containers automatically |
| `RESTART_COOLDOWN_SECS` | `300` | Min seconds between restarts of the same container |
| `AUTO_TUNE` | `false` | Enable automatic memory limit tuning |
| `AUTO_TUNE_COOLDOWN_SECS` | `600` | Min seconds between tunes of the same service |
| `AUTO_TUNE_COMPOSE_FILE` | `/deploy/docker-compose.yml` | Path to compose file inside the container |
| `AUTO_TUNE_ENV_FILE` | `/deploy/.env` | Path to env file inside the container |
| `LLM_BASE_URL` | — | Base URL of the OpenAI-compatible inference server |
| `LLM_MODEL` | `unsloth` | Model name passed to the inference API |
| `LLM_API_KEY` | — | Bearer token for the inference server (optional) |

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
