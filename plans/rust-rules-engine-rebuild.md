# Blueprint — Rust Rules Engine Rebuild

> Ground-up rebuild of the Java/Spring `Spring-Kafka-Stream-Rules` system as a
> lightweight Rust stack. Every step below is **cold-start executable**: a fresh
> agent can run any step using only that step's context brief plus this overview.

---

## Objective

Reproduce the existing pipeline — **consume `source-events` → evaluate rules per
event → route matched events to `target-events` → persist one audit record per
evaluation → expose analytics + rules CRUD** — on a stack that is dramatically
lighter on memory/CPU than the 10× JVM + 3-node Kafka + 3-node Mongo deployment.

**Source of truth for behavior:** the Java repo at
`/Users/chiya/GIT/Spring-Kafka-Stream-Rules`. When in doubt about a rule of
behavior (audit fields, topic names, match semantics, EOS guarantees), read the
corresponding Java class. Key references:
- Topology / routing: `topology/RoutingProcessor.java`
- Rule eval (SpEL): `eval/RuleEvaluator.java`, `rules/CompiledRule.java`
- Audit model: `audit/AuditRecord.java`, `audit/AuditConsumer.java`
- Rules CRUD + cache + hot reload: `rules/*`, `web/RuleController.java`
- Analytics + rollups: `web/AnalyticsService.java`, `web/RollupService.java`
- Simulation publish (fire-and-forget, paced): `web/SimulationController.java`
- Config: `src/main/resources/application.yml`

---

## Locked architecture decisions

These were settled in design discussion; do not relitigate without updating this
file's changelog.

| Concern | Decision | Rationale |
|---|---|---|
| Language/runtime | **Rust + Tokio** (async) | ~10–20× lower memory than JVM, no GC pauses |
| Messaging | **Redpanda** (Kafka-API compatible), client via **`rdkafka`** (librdkafka) | Drop the JVM broker; keep Kafka protocol + EOS |
| Rule evaluation | **CEL** via `cel-interpreter` crate (fallback: `rhai`) | Safe, fast expression eval over JSON; closest fit to SpEL predicates |
| Audit store + analytics | **ClickHouse** (`audits` MergeTree + **materialized views** for rollups) | Columnar, compressed, fast aggregation; MVs replace the scheduled rollup job + Redis lock entirely |
| Rule store (CRUD) | **Postgres** via `sqlx`; **LISTEN/NOTIFY** for hot-reload | Transactional rule edits; NOTIFY removes the need for Redis pub/sub |
| Redis | **Removed** | Hot-reload → Postgres NOTIFY; rollup lock → not needed (MVs) |
| HTTP API | **`axum`** (Tokio) | Lightweight; serves rules CRUD, analytics, health, simulation |
| Frontend | **Reuse the existing React SPA** from the Java repo (`frontend/`), repointed at the new API | API contract is compatible; no UI rewrite |
| Exactly-once | **`rdkafka` transactions** (read-process-write) | Preserve the EOS guarantee of the Kafka Streams version |
| Workspace | Cargo **workspace** of focused crates | High cohesion, testable units |

### Topic + data contract (must match Java)
- Topics: `source-events`, `target-events`, `audit-events` (+ `audit-events.DLT`).
- `AuditRecord` fields: `auditId` (= `{topic}:{partition}:{offset}:{ruleId}`),
  `ruleId`, `schemaVersion`, `auditType` (MATCHED|UNMATCHED|ERRORED), `reason`,
  `sourceEvent`, `routedEvent`, `sourceTopic`, `partition`, `offset`, `timestamp`,
  `parseTimeNano`, `evalTimeNano`, `totalTimeNano`.
- One audit record per (event × rule). `auditId` is the dedup key.

---

## Workspace layout (target)

```
Rust-Rules-Engine/
├── Cargo.toml                 # workspace
├── crates/
│   ├── core/                  # domain types: SourceEvent, Rule, CompiledRule, AuditRecord, AuditType
│   ├── eval/                  # CEL compile + evaluate; EvaluationResult + timings
│   ├── store-clickhouse/      # audits table, batch writer, analytics queries
│   ├── store-postgres/        # rules table, CRUD repo, LISTEN/NOTIFY
│   ├── pipeline/              # rdkafka consumer→eval→produce(routed+audit); EOS txns; audit→CH sink
│   └── web/                   # axum API: rules CRUD, analytics, health, simulation
├── bin/rules-engine/          # binary: wires config + crates + starts pipeline & web
├── migrations/                # sqlx (postgres) + clickhouse DDL
├── frontend/                  # ported React SPA (from Java repo)
├── deploy/                    # Dockerfile, docker-compose.yml, run.sh
└── plans/                     # this file
```

---

## Dependency graph & parallelism

```
S0 bootstrap
├─> S1 core domain
│   ├─> S2 eval (CEL)            ┐
│   ├─> S3 clickhouse store       ├ parallel after S1
│   └─> S5 postgres rule store    ┘
├─ S3 ─> S4 analytics (materialized views)
├─ S2,S5 ─> S7 rule cache + hot reload
├─ S2,S3,S5 ─> S6 pipeline (EOS)         [hardest; strongest model]
├─ S4,S5,S7 ─> S8 web API (axum)
├─ S8 ─> S9 frontend integration
├─ S6,S8 ─> S10 packaging + compose
└─ S10 ─> S11 load test + parity validation
```

**Parallelizable:** {S2, S3, S5} after S1; {S4} alongside {S5,S2}; {S9} alongside {S10 prep}.
**Critical path:** S0 → S1 → S3 → S6 → S10 → S11.

---

## Conventions (apply to every step)

- **Workflow:** branch `step-NN-slug` off `main`, implement, open a PR, merge after green CI. (Bootstrap creates the GitHub remote; until then, direct commits to `main`.)
- **TDD where logic is non-trivial** (eval, routing, analytics queries): write tests first, watch them fail, implement to green.
- **Verification gate (run before marking any step done):**
  `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace`
  plus the step's own verification commands.
- **Invariants checked after every step:** workspace compiles; `clippy` clean; no crate in `core`/`eval` depends on I/O crates (kafka/clickhouse/postgres/axum); topic names and audit field names match the Java contract.
- **Rollback:** revert the step's PR; schema steps ship a down-migration.
- **Model tier:** `strongest` for S2, S3, S4, S6 (design/correctness-heavy); `default` otherwise.

---

## Steps

### S0 — Bootstrap: workspace, infra, CI  ·  model: default  ·  deps: none

**Context brief.** Empty git repo (`master`, no remote). Establish the Cargo
workspace, empty crate skeletons, the local infra (Redpanda + ClickHouse +
Postgres) via docker-compose, CI, and the GitHub remote. No business logic yet.

**Tasks.**
1. `cargo new --vcs none` style: create root `Cargo.toml` workspace listing all crates + bin; create each crate/bin with a stub `lib.rs`/`main.rs` that compiles.
2. Rename default branch `master → main`.
3. `deploy/docker-compose.yml` with: `redpanda` (`redpandadata/redpanda`, single node, KRaft, listeners for host + in-network), `clickhouse` (`clickhouse/clickhouse-server`, volume), `postgres` (`postgres:16`, volume). No app service yet. Add memory limits (modest; single-node dev).
4. `deploy/run.sh` to bring infra up and wait for health (mirror the ergonomics of the Java `run.sh`: port-probe/auto-increment, clean teardown first).
5. CI: GitHub Actions running the verification gate on PRs.
6. Create the GitHub repo + remote (`gh repo create`), push `main`. **Confirm with the user before creating the remote.**
7. Pin toolchain: `rust-toolchain.toml` (1.94+), `rustfmt.toml`, `clippy` config.

**Verification.** `cargo build --workspace` succeeds; `docker compose -f deploy/docker-compose.yml up -d` brings redpanda/clickhouse/postgres healthy; `rpk cluster info`, `clickhouse-client --query "SELECT 1"`, `psql -c "select 1"` all succeed.

**Exit criteria.** Workspace compiles, infra runs locally, CI green on an initial PR, remote exists.

---

### S1 — Core domain types  ·  model: default  ·  deps: S0

**Context brief.** Pure-Rust domain crate, zero I/O dependencies. Mirrors Java
`audit/AuditRecord.java`, `rules/Rule.java`, `eval/EvaluationResult.java`.

**Tasks.**
1. `crates/core`: `SourceEvent` (raw JSON + Kafka coords topic/partition/offset/timestamp), `Rule` (id, expression, target metadata, enabled, version), `AuditType` enum, `AuditRecord` (all fields from the contract), `EvaluationResult`.
2. Deterministic `audit_id(topic, partition, offset, rule_id)` helper (must equal Java's `{topic}:{partition}:{offset}:{ruleId}`).
3. `serde` derives; JSON (de)serialization tests round-trip against sample payloads copied from `demo/DemoMessages.java`.

**Verification.** `cargo test -p core`; a round-trip test proves `audit_id` matches a hardcoded expected string from the Java format.

**Exit criteria.** Types compile, serde round-trips, `core` has no I/O deps.

---

### S2 — Rule evaluation engine (CEL)  ·  model: strongest  ·  deps: S1

**Context brief.** Replace Java SpEL (`eval/RuleEvaluator.java`) with CEL via
`cel-interpreter`. A rule's expression is evaluated against the event JSON bound
as a context variable (Java binds the parsed JSON `Map` as the SpEL root). Output:
MATCHED / UNMATCHED / ERRORED + `parseTimeNano`, `evalTimeNano`. **TDD.**

**Tasks.**
1. Define the binding contract: event JSON → CEL activation (e.g., variable `event`). Document the SpEL→CEL expression translation rules in the crate README (this is the riskiest semantic gap — capture it).
2. `compile(rule) -> CompiledRule` (parse/typecheck once, cache the Program); `evaluate(compiled, event) -> EvaluationResult` with timings.
3. Error taxonomy: parse error vs eval error vs non-boolean result → ERRORED with `reason`.
4. Tests first: a table of (expression, event, expected outcome) covering MATCHED, UNMATCHED, ERRORED, and at least 5 representative rules ported from the Java demo/seed rules (`config/RuleSeeder.java`).

**Verification.** `cargo test -p eval`; parity spot-check: pick 5 seed rules + sample events, confirm Rust outcomes match what the Java engine produces.

**Exit criteria.** Eval is deterministic, timed, and matches Java outcomes on the parity set; SpEL→CEL translation notes documented.

---

### S3 — ClickHouse audit store + schema  ·  model: strongest  ·  deps: S0, S1

**Context brief.** Audits live in a ClickHouse `audits` table (MergeTree), replacing
Mongo. High-throughput **batch** inserts (CH dislikes tiny inserts). Dedup by
`auditId` via `ReplacingMergeTree`. Retention via TTL.

**Tasks.**
1. `migrations/clickhouse/0001_audits.sql`: `audits` table — typed columns (`ruleId LowCardinality(String)`, `auditType Enum8`, `timestamp DateTime64(3)`, nanos `UInt64`, payloads `String`), `ENGINE = ReplacingMergeTree ORDER BY (timestamp, ruleId) PARTITION BY toYYYYMM(timestamp) TTL ...`.
2. `crates/store-clickhouse`: connection pool (`clickhouse` crate), `AuditWriter` that buffers and flushes in batches (size/interval configurable — mirror Java `max.poll.records: 500`, `fetch.max.wait.ms: 200`).
3. Migration runner (apply DDL on startup, idempotent).
4. Integration test against the docker-compose ClickHouse: write N audits, read them back, assert dedup on duplicate `auditId`.

**Verification.** `cargo test -p store-clickhouse -- --ignored` (integration, needs CH up); batch insert of 10k rows under target latency.

**Exit criteria.** Batched writes land in CH; duplicates collapse; schema migration is idempotent.

---

### S4 — Analytics via materialized views  ·  model: strongest  ·  deps: S3

**Context brief.** Reproduce the dashboard stats (`web/AnalyticsService.java`):
per-rule matched/unmatched/errored, hourly time series, total messages, total
evaluations, latency averages — but as **ClickHouse materialized views**, so they
update incrementally on insert. **This deletes the need for `RollupService` + the
Redis leader lock** from the Java design.

**Tasks.**
1. `migrations/clickhouse/0002_rollups.sql`: MV(s) into `AggregatingMergeTree`/`SummingMergeTree` target tables keyed by `(toStartOfHour(timestamp), ruleId)` for rule rollups and by hour for distinct-message counts.
2. Analytics query layer in `store-clickhouse`: functions returning the dashboard DTOs for an arbitrary `[from,to]` range (flexible ranges — sum the hourly buckets).
3. Tests: insert a known fixture, assert rollup tables + range queries match hand-computed expectations (port the Java verification: 12-rule fixture, top-N by matched, latency weighted average).

**Verification.** `cargo test -p store-clickhouse analytics`; `EXPLAIN`/`SELECT` confirm queries hit the MV target tables, not raw `audits`.

**Exit criteria.** Analytics served from MVs; flexible time ranges correct; no scheduled rollup job exists.

---

### S5 — Postgres rule store + hot-reload signal  ·  model: default  ·  deps: S0, S1

**Context brief.** Rules are transactional (CRUD) — Postgres via `sqlx`. Replace
the Java Mongo+Redis rule store and the Redis pub/sub hot-reload with Postgres
**LISTEN/NOTIFY** (a rule write emits a NOTIFY that all service instances hear).

**Tasks.**
1. `migrations/postgres/0001_rules.sql`: `rules` table (id, expression, target_topic, enabled, version, timestamps).
2. `crates/store-postgres`: `RuleRepository` (list/get/create/update/delete) with `sqlx`; on write, `NOTIFY rules_changed`.
3. `RuleChangeStream`: a `LISTEN rules_changed` subscription exposing a stream/broadcast for consumers (used by S7).
4. Seed loader porting `config/RuleSeeder.java` defaults.
5. Tests against the docker-compose Postgres: CRUD round-trip; NOTIFY received by a listener.

**Verification.** `cargo test -p store-postgres -- --ignored`; a listener observes a NOTIFY after an update.

**Exit criteria.** Rule CRUD durable; change notifications fire; seed rules load.

---

### S6 — Pipeline: consume → evaluate → route + audit (EOS)  ·  model: strongest  ·  deps: S2, S3, S5

**Context brief.** The heart of the system and the **highest-risk step** — it
replaces Kafka Streams' exactly-once topology with hand-built `rdkafka`
transactions. Read `source-events`, evaluate every active rule against each event,
produce matched events to `target-events`, produce one audit per evaluation to
`audit-events`, all within a read-process-write transaction so offsets + outputs
commit atomically (EOS). A second consumer batches `audit-events` into ClickHouse
(S3 writer).

**Tasks.**
1. Transactional producer + consumer config (`rdkafka`): `transactional.id`, `isolation.level=read_committed`, manual offset commit inside the transaction (`send_offsets_to_transaction`).
2. Process loop: poll batch → for each record, evaluate against the current rule set (from S7 cache; for this step a static set is acceptable, wired fully in S7) → build routed + audit messages → produce → commit transaction. On error, abort transaction; poison messages → `audit-events.DLT`.
3. Audit sink: a consumer group reading `audit-events`, batching into the S3 ClickHouse writer with at-least-once + dedup (ReplacingMergeTree handles dups).
4. Partitioning/scaling notes: documented; horizontal scaling = more consumer instances in the group (no fixed replica count).
5. Tests: an integration test (Redpanda + CH up) publishes events, asserts target + audit topics receive the right messages and audits land in CH; an EOS test asserts no duplicate side effects across a simulated rebalance/abort.

**Verification.** `cargo test -p pipeline -- --ignored`; end-to-end: publish 1k events → correct routed + audit counts in CH; kill/restart an instance mid-run → no duplicate audits (dedup) and no lost commits.

**Exit criteria.** EOS pipeline processes events correctly and idempotently; DLT handling works.

---

### S7 — Rule cache + hot reload  ·  model: default  ·  deps: S2, S5

**Context brief.** Mirror Java `rules/RuleCache.java` + `RuleChangeListener.java`:
an in-memory `ArcSwap`/`RwLock` of compiled rules, loaded from Postgres (S5) and
atomically swapped when a LISTEN/NOTIFY fires. The pipeline (S6) reads from this
cache on the hot path (no per-event DB hit).

**Tasks.**
1. `RuleCache` holding `Arc<Vec<CompiledRule>>` (compile via S2 on load).
2. Background task: initial load + subscribe to S5's change stream → reload + recompile → atomic swap.
3. Wire the pipeline (S6) to read the live cache.
4. Tests: update a rule in Postgres → cache reflects it within bound; concurrent reads see a consistent snapshot.

**Verification.** `cargo test -p pipeline cache` (or wherever the cache lives); a rule edit propagates to evaluation without restart.

**Exit criteria.** Rules hot-reload across instances via Postgres NOTIFY; hot path is lock-light.

---

### S8 — HTTP API (axum)  ·  model: default  ·  deps: S4, S5, S7

**Context brief.** Reproduce the Java REST surface so the existing React SPA works
unchanged. Endpoints (match `web/*Controller.java` paths): `/api/rules` CRUD,
`/api/analytics/stats?from&to`, `/api/health/status`, `/api/simulation/push?count`.

**Tasks.**
1. `crates/web` (axum): rules CRUD → S5; analytics → S4; health → ping CH/PG/Redpanda; simulation push → **fire-and-forget, paced** publish to `source-events` (port `SimulationController.java`: returns immediately, paced inter-message delay, background task).
2. JSON DTOs matching the Java response shapes exactly (the SPA depends on field names — verify against `frontend/src/types.ts`).
3. Tests: handler tests with a mocked store layer; a contract test asserting JSON shapes match the SPA's expected types.

**Verification.** `cargo test -p web`; `curl` each endpoint against a running stack returns the expected shape; `/api/simulation/push` returns immediately.

**Exit criteria.** API parity with the Java endpoints the SPA consumes.

---

### S9 — Frontend integration  ·  model: default  ·  deps: S8

**Context brief.** Reuse the React SPA from the Java repo (`frontend/`). It already
talks to `/api/*` with compatible shapes (S8). Port it, build it, and serve it.

**Tasks.**
1. Copy `frontend/` from the Java repo; adjust any base URL/proxy config.
2. Serve static build from axum (or an nginx sidecar mirroring the Java `frontend/Dockerfile`).
3. Smoke test: dashboard loads, rules CRUD works, analytics renders, simulate button triggers a background publish.

**Verification.** `npm run build` succeeds; loading the served UI shows live data after a simulation run.

**Exit criteria.** UI works end-to-end against the Rust backend.

---

### S10 — Packaging + compose  ·  model: default  ·  deps: S6, S8

**Context brief.** Produce a small static binary and a full-stack compose mirroring
the Java `run.sh`/`docker-compose.yml` ergonomics, sized for the lighter stack
(single Redpanda, single ClickHouse, single Postgres, N app instances).

**Tasks.**
1. Multi-stage Dockerfile → static `musl` binary (tiny image).
2. `deploy/docker-compose.yml`: add the `app` service (scalable via `APP_REPLICAS`), redpanda, clickhouse, postgres; resource limits; env wiring.
3. `deploy/run.sh`: build + up + health-wait + RS/migration init (CH + PG migrations on startup).
4. Document the connection matrix (Redpanda bootstrap, CH HTTP/native, PG DSN).

**Verification.** `./deploy/run.sh` brings the whole stack healthy; image size recorded (expect tens of MB vs the JVM image).

**Exit criteria.** One-command full stack; app image is small; migrations auto-apply.

---

### S11 — Load test + parity validation  ·  model: default  ·  deps: S10

**Context brief.** Prove the rebuild matches the Java system's behavior and is
lighter. Reuse the Java load scenario (100k paced publish) and compare outputs.

**Tasks.**
1. Publish 100k events via `/api/simulation/push`; confirm audit count = events × active-rules and analytics parity (totals, top-N, latency) against the Java baseline numbers.
2. Capture footprint: per-container memory/CPU under load vs the Java cluster (the headline win); confirm no OOM/restarts.
3. EOS soak: restart an app instance mid-load → no duplicate/lost audits.
4. Write `BENCHMARKS.md` comparing resource usage Java vs Rust.

**Verification.** Audit/analytics parity within tolerance; documented memory reduction; zero OOM/restarts.

**Exit criteria.** Behavioral parity proven; resource win quantified.

---

## Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| **SpEL → CEL semantic gaps** (functions, type coercion, null handling) | High | S2 documents translation rules + parity tests; keep `rhai` as a fallback engine if CEL can't express some rules |
| **Hand-rolled EOS correctness** (rdkafka transactions) | High | S6 dedicated step, strongest model, explicit abort/rebalance tests; ReplacingMergeTree dedup as a safety net on the audit sink |
| **ClickHouse small-insert pressure** | Medium | Batch inserts (S3) sized like the Java audit-writer; or use CH Kafka engine as an alternative ingestion path |
| **Eventual dedup vs Mongo's upsert-by-_id** | Medium | ReplacingMergeTree + `FINAL`/dedup-in-query where exactness matters; document the semantic difference |
| **Frontend contract drift** | Low | S8 contract tests against `frontend/src/types.ts` |
| **`librdkafka` build/link friction (musl)** | Medium | Pin `rdkafka` with `cmake-build`/`ssl-vendored` features; CI builds the static target early (S0/S10) |

---

## Plan mutation protocol

If a step proves too large, **split** it (`S6 → S6a/S6b`) and record the split here
with a one-line reason. If a decision changes (e.g., CEL → rhai), update the
**Locked architecture decisions** table and add a dated note to the changelog
below. Never silently diverge.

## Changelog

- 2026-06-19 — Initial blueprint drafted from the Java `Spring-Kafka-Stream-Rules`
  system and the agreed lightweight target stack (Rust + Redpanda + ClickHouse +
  Postgres, Redis removed).
