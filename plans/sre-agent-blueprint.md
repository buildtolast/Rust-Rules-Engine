# Blueprint — SRE Agent Service

> Companion service to the Rust Rules Engine. A containerized, advisory-only SRE
> agent that streams Docker container logs, sends log windows to the local LLM for
> analysis, stores structured findings in ClickHouse, and serves a live dashboard
> on port 8088. It takes **no automated remediation action** — all findings are
> advisory summaries that a human acts on.

---

## Objective

Monitor the health of every container in the `rre-*` docker-compose stack at runtime.
Surface errors, anomalies, and service health status through:

1. A rolling log-analysis loop (bollard → local LLM → ClickHouse)
2. An axum HTTP dashboard at `localhost:8088` with per-service cards, uptime gauge,
   and a findings feed
3. A JSON API at `/api/sre/*` so external tooling can query the same data

The SRE agent must not restart, scale, or modify any container. It reports; it
does not act.

---

## Locked architecture decisions

| Concern | Decision | Rationale |
|---|---|---|
| Language/runtime | **Rust + Tokio** | Consistent with the rest of the workspace |
| Docker API | **`bollard`** crate | Safe async Rust client for the Docker Engine API; supports log streaming and container inspection |
| HTTP / dashboard | **`axum`** + Tera templates | Lightweight; SSE for live dashboard updates without a JS build step |
| Log analysis | **local LLM** via `reqwest` to `http://localhost:8888` (Unsloth Studio) | Same model used for codegen; advisory findings generated from log windows |
| Findings store | **ClickHouse** (`sre_observations` table) | Columnar; reuses the existing `rre-clickhouse` instance; long-term trend queries are cheap |
| Dashboard port | **8088** | Avoids conflicts with ClickHouse (8123), Redpanda (9644), Postgres (5432) |
| Authority | **Advisory only** | No container restart, no config mutation, no alert routing |
| Deployment | New service in `deploy/docker-compose.yml`, socket-mounted `/var/run/docker.sock` | Co-located with infra; one `./deploy/run.sh` brings everything up |

---

## New additions to the workspace

```
Rust-Rules-Engine/
├── crates/
│   └── sre/                   # NEW — SRE agent library crate
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          # pub use …
│           ├── docker.rs       # bollard: log streaming, health inspection
│           ├── analysis.rs     # LLM client: send log window, parse finding
│           ├── store.rs        # ClickHouse writer for sre_observations
│           └── dashboard.rs    # axum router: dashboard HTML + /api/sre/*
├── bin/
│   └── sre-agent/             # NEW — binary
│       ├── Cargo.toml
│       └── src/
│           └── main.rs         # config, wires crate, spawns tasks
├── migrations/
│   └── clickhouse/
│       └── 0002_sre_observations.sql   # NEW
├── templates/                 # NEW — Tera HTML templates for the dashboard
│   ├── dashboard.html
│   └── partials/
│       ├── service_card.html
│       └── findings_feed.html
└── deploy/
    └── docker-compose.yml     # AMENDED — add sre-agent service
```

The workspace `Cargo.toml` gains two new members:
- `crates/sre`
- `bin/sre-agent`

---

## Data model

### ClickHouse table: `sre_observations`

```sql
CREATE TABLE IF NOT EXISTS sre_observations (
    observed_at     DateTime64(3),              -- wall clock when LLM returned
    container_name  LowCardinality(String),     -- e.g. "rre-redpanda"
    severity        LowCardinality(String),     -- INFO | WARN | ERROR | CRITICAL
    category        LowCardinality(String),     -- e.g. "crash_loop" | "connection_refused" | "oom" | "latency"
    finding         String,                     -- LLM free-text summary (max ~2000 chars)
    proposed_fix    String,                     -- LLM proposed action (advisory; never applied)
    log_window_hash String,                     -- sha256(log_window) — dedup key
    log_snippet     String                      -- first 500 chars of the triggering log window
) ENGINE = ReplacingMergeTree(observed_at)
  ORDER BY (container_name, log_window_hash)
  PARTITION BY toYYYYMM(observed_at)
  TTL toDateTime(observed_at) + INTERVAL 30 DAY;
```

Dedup key is `(container_name, log_window_hash)` — the same repeated log burst
does not create duplicate findings. `ReplacingMergeTree(observed_at)` keeps the
most-recent observation for that (container, burst) pair.

### In-memory state (Arc<Mutex<SreState>>)

```rust
pub struct ContainerStatus {
    pub name:            String,
    pub id:              String,
    pub running:         bool,
    pub started_at:      Option<DateTime<Utc>>,
    pub health:          HealthSummary,   // Healthy | Unhealthy | None
    pub last_checked_at: DateTime<Utc>,
    pub error_rate_1m:   f64,            // errors seen in last 60 s of log tail
}

pub struct SreState {
    pub containers:    Vec<ContainerStatus>,
    pub findings:      VecDeque<Finding>,   // ring buffer, last 100
    pub last_scan_at:  Option<DateTime<Utc>>,
    pub llm_available: bool,
}
```

---

## Analysis loop design

```
every 60 s per container:
  1. bollard::DockerApi::logs()  → tail last 200 log lines  (docker.rs)
  2. sha256(log_window)  → skip if hash already in sre_observations in last 5 min
  3. POST /v1/chat/completions to localhost:8888  (analysis.rs)
       prompt = [system: "You are an SRE agent…", user: "<log_window>"]
       → parse JSON { severity, category, finding, proposed_fix }
  4. Write SreObservation row to ClickHouse  (store.rs)
  5. Push Finding to SreState ring buffer  (broadcast via SSE to dashboard)
```

LLM prompt contract (analysis.rs):

```
System: You are an SRE agent reviewing container logs for the Rust Rules Engine
        service stack (Redpanda, ClickHouse, Postgres, rules-engine, sre-agent).
        Respond ONLY with a JSON object with these exact keys:
          severity   : one of INFO | WARN | ERROR | CRITICAL
          category   : one of crash_loop | connection_refused | oom |
                       latency | config_error | normal | other
          finding    : plain English summary under 300 words
          proposed_fix: plain English proposed action under 200 words, or
                        "No action required" for INFO

User: Container: rre-redpanda
      Log window (last 60 seconds, 200 lines):
      <log lines here>
```

If the LLM is unreachable, the loop logs a warning and skips the LLM step —
the container health poll (bollard inspect) still runs and updates `SreState`.

---

## Dashboard routes

| Path | Method | Description |
|---|---|---|
| `/` | GET | Full dashboard HTML (Tera render of `dashboard.html`) |
| `/api/sre/status` | GET | JSON: all ContainerStatus + llm_available + last_scan_at |
| `/api/sre/findings` | GET | JSON: last 100 findings from ClickHouse |
| `/api/sre/findings/stream` | GET | SSE stream: new Finding pushed on each loop tick |
| `/health` | GET | `{"ok":true}` — for docker-compose healthcheck |

Dashboard shows:
- Top bar: overall health (all-green / degraded / critical) + LLM availability badge
- Service cards (one per container): name, running status, uptime, health check
  status, last error rate (1 min), last finding severity chip
- Findings feed: scrollable list — timestamp, container, severity badge, finding
  text, proposed fix (collapsed by default, expand on click)
- Auto-refreshes via SSE; no polling JS needed beyond an `EventSource` connection

---

## docker-compose changes

```yaml
  sre-agent:
    image: rre/sre-agent:latest          # built from Dockerfile.sre
    container_name: rre-sre-agent
    build:
      context: ..
      dockerfile: deploy/Dockerfile.sre
    ports:
      - "${SRE_PORT:-8088}:8088"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      CLICKHOUSE_DB: ruleaudit
      CLICKHOUSE_USER: rules
      CLICKHOUSE_PASSWORD: rules
      LLM_BASE_URL: http://host.docker.internal:8888
      LLM_MODEL: ${LLM_MODEL:-unsloth}
      SCAN_INTERVAL_SECS: ${SCAN_INTERVAL_SECS:-60}
      LOG_TAIL_LINES: ${LOG_TAIL_LINES:-200}
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      clickhouse:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8088/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 6
    deploy:
      resources:
        limits:
          memory: ${SRE_MEM_LIMIT:-256M}
```

The Docker socket is mounted **read-only** (`:ro`) — the agent can only query,
not issue commands. The LLM runs on the host; `host.docker.internal` resolves via
the `extra_hosts` gateway entry (same as Redpanda's existing config).

---

## Implementation steps

### Step 0 — ClickHouse migration (pre-requisite, no code)

**Context:** The `rre-clickhouse` container is already running (from S3). This step
adds the `sre_observations` table.

**Tasks:**
1. Write `migrations/clickhouse/0002_sre_observations.sql` with the DDL above.
2. Apply: `docker compose -f deploy/docker-compose.yml exec clickhouse clickhouse-client --query "$(cat migrations/clickhouse/0002_sre_observations.sql)"`

**Verify:** `SELECT count() FROM ruleaudit.sre_observations` returns 0 without error.

**Exit criteria:** Table exists; `cargo test -p sre` can compile (no infra tests yet).

---

### Step 1 — Workspace scaffold + Cargo.toml

**Context:** Add two new workspace members. The `crates/sre` crate holds all
library logic; `bin/sre-agent` is the thin binary.

**Key dependencies for `crates/sre`:**
```toml
bollard   = "0.17"
reqwest   = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
axum      = { version = "0.8", features = ["tokio"] }
tera      = "1"
tower-http = { version = "0.6", features = ["fs"] }
sha2      = "0.10"
hex       = "0.4"
tokio     = { workspace = true }
serde     = { workspace = true }
serde_json = { workspace = true }
chrono    = { workspace = true }
thiserror = { workspace = true }
clickhouse = { version = "0.15.1", features = ["inserter", "chrono"] }
tracing   = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

**Tasks:**
1. Add `crates/sre` and `bin/sre-agent` to workspace `Cargo.toml`.
2. Write `crates/sre/Cargo.toml` with the dependency list above.
3. Write stub `crates/sre/src/lib.rs` that compiles (empty pub mods).
4. Write `bin/sre-agent/Cargo.toml` depending on `sre = { path = "../../crates/sre" }` + tokio.
5. Write `bin/sre-agent/src/main.rs` stub that prints "sre-agent starting" and exits.

**Verify:** `cargo build -p sre && cargo build -p sre-agent` both succeed with no errors.

**Exit criteria:** CI stays green; workspace compiles with two new empty members.

---

### Step 2 — ClickHouse store (`crates/sre/src/store.rs`)

**Context:** Reuses the `clickhouse` 0.15.1 crate (same as `store-clickhouse`).
The `SreObservation` struct maps 1:1 to `sre_observations` columns. Write via
`Inserter<SreObservation>` with `with_max_rows(100).with_period(Some(5s))`.

**Spec for local-LLM loop:**
- Input: `SreObservation` struct (all fields, see schema above)
- `#[derive(clickhouse::Row, Serialize, Deserialize)]` — same pattern as `AuditRow`
- `observed_at` uses `#[serde(with = "clickhouse::serde::chrono::datetime64::millis")]`
- `container_name`, `severity`, `category` are `String` (LowCardinality handled by CH)
- Public API: `SreStore::new(client) -> Self`, `SreStore::write(&mut self, obs: &SreObservation) -> Result<(), Error>`, `SreStore::commit(&mut self) -> Result<(), Error>`
- Do NOT `use clickhouse::error::Error` — name collision; use fully-qualified `clickhouse::error::Error`

**Tasks:**
1. Author spec at `specs/sre-store.md`.
2. Run local-LLM loop: `cat specs/sre-store.md | python3 tools/llm-loop.py crates/sre/src/store.rs "cargo build -p sre" --max-iters 8`
3. Review generated output against spec; fix any issues in the spec and re-run (do not hand-edit generated file).
4. Write integration test `crates/sre/tests/store_integration.rs` (`#[ignore]`): insert 2 observations including 1 duplicate hash, assert `COUNT() FINAL` = 1.

**Verify:** `cargo build -p sre` + `cargo test -p sre --lib` both green.

---

### Step 3 — Docker polling (`crates/sre/src/docker.rs`)

**Context:** `bollard` connects to `/var/run/docker.sock` by default when the env
var `DOCKER_HOST` is unset (which is the case inside the sre-agent container). For
local dev (outside docker), it also works if the socket exists. Key bollard APIs:
- `Docker::connect_with_local_defaults()` — connects to socket or env
- `docker.list_containers(Some(ListContainersOptions { all: true, .. }))` — enumerate
- `docker.inspect_container(name, None)` — status, startedAt, health
- `docker.logs(name, Some(LogsOptions { stdout: true, stderr: true, tail: "200", follow: false, .. }))` — returns a `Stream<Item = Result<LogOutput, Error>>`

**Spec for local-LLM loop (`specs/sre-docker.md`):**
- `pub struct ContainerInfo { name, id, running, started_at, health }` — populated from bollard inspect
- `pub async fn list_containers(docker: &Docker) -> Result<Vec<ContainerInfo>, Error>`
- `pub async fn tail_logs(docker: &Docker, name: &str, lines: usize) -> Result<String, Error>` — collect stdout+stderr log lines into a single String, newest last
- `pub struct DockerError` via `thiserror`; wrap `bollard::errors::Error`
- bollard import: `use bollard::Docker; use bollard::container::{ListContainersOptions, LogsOptions, InspectContainerOptions};`

**Tasks:**
1. Author `specs/sre-docker.md`.
2. Run LLM loop: `cat specs/sre-docker.md | python3 tools/llm-loop.py crates/sre/src/docker.rs "cargo build -p sre" --max-iters 8`
3. Review: check that log stream is collected with `.collect::<Vec<_>>()` and mapped to strings properly, no blocking calls, correct bollard import paths.
4. Write a unit test (no Docker required): `tail_logs` with a mock is not easy — write a smoke compile test (`#[cfg(test)] mod tests { use super::*; }`) and mark heavier tests `#[ignore]`.

**Verify:** `cargo build -p sre` green, `cargo clippy -p sre -- -D warnings` clean.

---

### Step 4 — LLM analysis client (`crates/sre/src/analysis.rs`)

**Context:** Calls the local LLM (Unsloth Studio) at `http://localhost:8888` (or
`http://host.docker.internal:8888` inside Docker). The OpenAI-compatible endpoint
is `POST /v1/chat/completions`. Response is standard `choices[0].message.content`.

**Spec for local-LLM loop (`specs/sre-analysis.md`):**
- `pub struct Finding { severity, category, finding, proposed_fix }` — parsed from LLM JSON response
- `pub struct AnalysisClient { base_url: String, model: String, http: reqwest::Client }`
- `pub async fn analyze(client: &AnalysisClient, container: &str, log_window: &str) -> Result<Finding, AnalysisError>`
  - Build the system+user prompt (template in spec below)
  - POST to `{base_url}/v1/chat/completions` with `model`, `messages`, `temperature: 0.2`, `max_tokens: 512`
  - Parse response content as `serde_json::Value`, extract `severity`, `category`, `finding`, `proposed_fix`
  - If parse fails or HTTP error: return `AnalysisError::LlmUnavailable(msg)` — caller skips the LLM step and logs a warning
- `pub enum AnalysisError` via thiserror: `Unavailable(String)`, `ParseError(String)`
- Do NOT panic on LLM failure — return Err so the analysis loop degrades gracefully

**Tasks:**
1. Author `specs/sre-analysis.md` with the exact prompt template.
2. Run LLM loop: `cat specs/sre-analysis.md | python3 tools/llm-loop.py crates/sre/src/analysis.rs "cargo build -p sre" --max-iters 8`
3. Review: verify error handling, that `reqwest` is configured with a 30s timeout, and that the prompt string matches the contract above exactly.
4. Unit tests (no real LLM): test `Finding` deserialization from a hard-coded JSON string; test that a 500 response maps to `AnalysisError::Unavailable`.

**Verify:** `cargo test -p sre --lib` green (mocked HTTP tests).

---

### Step 5 — Analysis loop task (`crates/sre/src/lib.rs`)

**Context:** The main loop is a `tokio::task` that wakes every `SCAN_INTERVAL_SECS`
and processes all containers in parallel (via `tokio::join_all`). It holds
`Arc<RwLock<SreState>>` for sharing with the dashboard. The loop is the primary
orchestrator — it calls docker.rs, analysis.rs, store.rs in sequence and pushes
findings into the shared state ring buffer.

**Spec (Opus-authored directly — no LLM loop; control-flow logic, not mechanical codegen):**

```rust
pub struct SreConfig {
    pub clickhouse_url:    String,
    pub clickhouse_db:     String,
    pub clickhouse_user:   String,
    pub clickhouse_pass:   String,
    pub llm_base_url:      String,
    pub llm_model:         String,
    pub scan_interval:     Duration,
    pub log_tail_lines:    usize,
    pub dashboard_port:    u16,
}

pub async fn run(cfg: SreConfig) -> anyhow::Result<()> {
    // 1. Connect to ClickHouse, run migration 0002
    // 2. Connect to Docker socket
    // 3. Build shared SreState behind Arc<RwLock<_>>
    // 4. Spawn analysis_loop task (passes Arc clone)
    // 5. Spawn dashboard task (passes Arc clone + SreConfig)
    // 6. tokio::select! on both tasks; propagate first error
}
```

**Tasks:**
1. Write `crates/sre/src/lib.rs` (Opus-authored, not LLM loop — this is control
   flow wiring, not mechanical code gen).
2. Define `SreState`, `ContainerStatus` (matching the data model section above),
   and `Finding` structs if not already in analysis.rs.
3. Implement `analysis_loop`: interval timer, `list_containers`, per-container
   `tail_logs` → sha256 dedup → `analyze` → `SreStore::write`.
4. Implement state update: push to `findings` ring buffer (VecDeque, max 100),
   update `ContainerStatus` entries in `containers` vec.

**Verify:** `cargo build -p sre` green; clippy clean.

---

### Step 6 — Dashboard (`crates/sre/src/dashboard.rs`)

**Context:** axum router serving the Tera-rendered HTML dashboard and JSON API.
SSE endpoint streams `Finding` events as newline-delimited JSON using
`axum::response::sse::Sse` + `tokio::sync::broadcast`.

**Spec for local-LLM loop (`specs/sre-dashboard.md`):**
- `pub fn router(state: Arc<RwLock<SreState>>, tx: broadcast::Sender<Finding>) -> axum::Router`
- Routes: `GET /` → `dashboard_html`, `GET /api/sre/status` → `status_json`,
  `GET /api/sre/findings` → `findings_json`, `GET /api/sre/findings/stream` → `findings_sse`, `GET /health` → `health`
- `dashboard_html`: load `templates/dashboard.html` with Tera, inject serialized SreState; return `Html<String>`
- `status_json`: serialize `SreState.containers` + `llm_available` + `last_scan_at` as JSON
- `findings_json`: serialize `SreState.findings` as JSON array
- `findings_sse`: subscribe to `tx.subscribe()`, stream `Event::default().data(json_str)` per Finding
- `health`: return `Json(json!({"ok": true}))`
- State shared via `axum::extract::State<Arc<RwLock<SreState>>>` — no global statics

**Tasks:**
1. Author `specs/sre-dashboard.md`.
2. Run LLM loop: `cat specs/sre-dashboard.md | python3 tools/llm-loop.py crates/sre/src/dashboard.rs "cargo build -p sre" --max-iters 10`
3. Write Tera templates `templates/dashboard.html`, `templates/partials/service_card.html`, `templates/partials/findings_feed.html` (Opus-authored — HTML markup is not suitable for the LLM loop).
4. Wire the broadcast `Sender` into `analysis_loop` so findings are pushed to SSE subscribers.

**Verify:** `cargo build -p sre` green; `curl localhost:8088/health` returns `{"ok":true}` when run locally.

---

### Step 7 — Binary wiring (`bin/sre-agent/src/main.rs`)

**Context:** Reads config from env vars, initializes tracing, calls `sre::run(cfg).await`.
This is thin — all logic is in the `sre` crate.

**Tasks (Opus-authored directly — minimal boilerplate):**
1. Read all `SreConfig` fields from env vars using `std::env::var(...)`, fail fast
   with a descriptive error if required vars are missing.
2. Initialize `tracing_subscriber` with `EnvFilter` from `RUST_LOG` env (default
   `info`).
3. `#[tokio::main] async fn main()` → `sre::run(cfg).await.unwrap_or_else(|e| { ... })`

**Verify:** `cargo build -p sre-agent` green; binary produces structured log output.

---

### Step 8 — Dockerfile + docker-compose integration

**Context:** Multi-stage Dockerfile mirrors the existing `Dockerfile` for
`rules-engine`. The sre-agent container needs `libssl` for rustls TLS (reqwest).

**Tasks:**
1. Write `deploy/Dockerfile.sre` (multi-stage: builder → runtime):
   ```dockerfile
   FROM rust:1.94-slim AS builder
   WORKDIR /build
   COPY . .
   RUN cargo build --release -p sre-agent

   FROM debian:bookworm-slim
   RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*
   COPY --from=builder /build/target/release/sre-agent /usr/local/bin/
   COPY --from=builder /build/templates /templates
   CMD ["sre-agent"]
   ```
2. Add the `sre-agent` service to `deploy/docker-compose.yml` (see docker-compose
   changes section above).
3. Update `deploy/run.sh` health-wait logic to include `rre-sre-agent` alongside
   the existing services.

**Verify:** `docker compose -f deploy/docker-compose.yml build sre-agent` succeeds;
`docker compose -f deploy/docker-compose.yml up -d sre-agent` starts and `/health`
returns 200 within 30s.

---

### Step 9 — CI update

**Context:** The existing `.github/workflows/ci.yml` runs `cargo test --workspace`.
After this step, the workspace includes `sre` and `sre-agent`. Integration tests
(bollard, ClickHouse) remain `#[ignore]` and do not run in CI.

**Tasks:**
1. Verify `cargo test --workspace` in CI does not attempt to connect to Docker
   socket (all infra-dependent tests must be `#[ignore]`).
2. Add a `check_sre` step if desired: `cargo clippy -p sre -p sre-agent -- -D warnings`.
3. No change needed if all integration tests are properly `#[ignore]`-d.

**Verify:** CI passes with the two new crates in the workspace.

---

### Step 10 — End-to-end smoke test

**Context:** Manual smoke test with the full stack running (`./deploy/run.sh`). This
is not an automated test — it validates the full integration.

**Tasks:**
1. Run `./deploy/run.sh` to bring up all services including sre-agent.
2. Open `http://localhost:8088` — confirm dashboard renders with service cards for
   `rre-redpanda`, `rre-clickhouse`, `rre-postgres`.
3. Confirm `http://localhost:8088/api/sre/status` returns JSON with all containers.
4. Wait 60s for first analysis loop — confirm a finding appears in
   `http://localhost:8088/api/sre/findings`.
5. Verify ClickHouse: `SELECT count() FROM ruleaudit.sre_observations` > 0.
6. Simulate a container issue: `docker pause rre-postgres`, wait 60s, confirm
   dashboard shows `rre-postgres` as NOT running.
7. `docker unpause rre-postgres` — confirm recovery reflected on next scan.

**Exit criteria:** Dashboard loads; findings appear; container status reflects
actual Docker state.

---

## Dependency graph

```
Step 0 (CH migration)  ──────────────────────┐
Step 1 (scaffold)      ──────────────────────┤
                                              ▼
Step 2 (store.rs)      depends on: 0, 1      │
Step 3 (docker.rs)     depends on: 1          │
Step 4 (analysis.rs)   depends on: 1          │
                                              │
Step 5 (lib.rs loop)   depends on: 2, 3, 4   │
Step 6 (dashboard.rs)  depends on: 5          │
Step 7 (main.rs)       depends on: 5, 6       │
Step 8 (Docker/compose) depends on: 7         │
Step 9 (CI)            depends on: 1          │
Step 10 (smoke test)   depends on: 0–9       ◄┘
```

**Parallelism:** Steps 2, 3, and 4 can be developed in parallel after Step 1
(scaffold) is done. Steps 0 and 1 can also be done in parallel.

---

## Rollback

- Steps 0–9 add entirely new files or amend `docker-compose.yml` with a new service
  block. Rolling back is `git revert` on the SRE commits + removing the new
  service block from `docker-compose.yml`.
- No existing services are modified until Step 8 (docker-compose amendment).
  Steps 0–7 are fully non-destructive to the existing stack.

---

## Security notes

- Docker socket is mounted **read-only** (`:ro`). The agent cannot restart, stop,
  or modify any container via the API.
- `LLM_BASE_URL` and `CLICKHOUSE_PASSWORD` come from env vars. The sre-agent
  binary must fail fast (not silently degrade) if required env vars are absent.
- The dashboard has no authentication in this version — it is expected to run
  on localhost only. If exposed externally, add an axum middleware for IP
  allowlisting or basic auth.
- The log window sent to the LLM must be treated as sensitive; it may contain
  error messages with internal hostnames. The LLM is local (Unsloth Studio) —
  no data leaves the machine.

---

## Changelog

| Date | Author | Note |
|---|---|---|
| 2026-06-19 | arnab | Initial blueprint — runtime service, advisory only, Rust+axum+bollard |
