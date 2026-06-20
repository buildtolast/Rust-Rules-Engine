# Rust Rules Engine — Architecture Deep-Dive

## Table of Contents

1. [System Purpose](#1-system-purpose)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Crate Dependency Graph](#3-crate-dependency-graph)
4. [Data Flow: Message Lifecycle](#4-data-flow-message-lifecycle)
5. [Crate Internals](#5-crate-internals)
   - 5.1 [rules-core](#51-rules-core)
   - 5.2 [eval](#52-eval)
   - 5.3 [store-postgres](#53-store-postgres)
   - 5.4 [store-clickhouse](#54-store-clickhouse)
   - 5.5 [pipeline](#55-pipeline)
   - 5.6 [web](#56-web)
   - 5.7 [sre](#57-sre)
6. [Database Schemas](#6-database-schemas)
7. [SRE Agent Design](#7-sre-agent-design)
8. [Network Topology](#8-network-topology)
9. [Key Invariants](#9-key-invariants)

---

## 1. System Purpose

The Rust Rules Engine is a high-throughput event-processing pipeline that:

1. Reads structured JSON events from a Kafka topic (`source-events`)
2. Evaluates every active business rule (CEL expressions stored in Postgres) against each event
3. Routes matched events to a second Kafka topic (`target-events`) under Exactly-Once Semantics (EOS)
4. Writes one `AuditRecord` per (event × rule) pair to ClickHouse for compliance/analytics
5. Exposes a REST API for rule management and analytics queries
6. Runs a sidecar SRE agent that monitors container logs with a local LLM

The system is a Rust re-implementation of a Java/Spring-Kafka-Stream-Rules service, replacing
MongoDB + Redis with Postgres + ClickHouse and replacing SpEL with CEL.

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              Docker Compose Network                               │
│                                                                                  │
│  ┌─────────────┐    ┌──────────────────────────────────────────────────────┐    │
│  │  Frontend   │    │                  rules-engine (app)                   │    │
│  │  nginx:80   │───▶│                                                      │    │
│  │  React SPA  │    │  ┌────────────┐  ┌────────────────────────────────┐ │    │
│  └─────────────┘    │  │  HTTP API  │  │  Pipeline (spawn_blocking)     │ │    │
│                      │  │ axum:8080  │  │                                │ │    │
│  ┌─────────────┐    │  │            │  │  Phase 1: rayon par_iter()     │ │    │
│  │  SRE Agent  │    │  │  /api/*    │  │  ├─ parse JSON → SourceEvent   │ │    │
│  │  port 8088  │    │  │  /health/* │  │  └─ eval N rules per msg       │ │    │
│  │             │    │  │  /metrics  │  │                                │ │    │
│  │  bollard    │    │  └─────┬──────┘  │  Phase 2: EOS Kafka txn        │ │    │
│  │  Docker API │    │        │         │  ├─ begin_transaction           │ │    │
│  │  local LLM  │    │  AppState (Arc) │  │  route matched → target-events│ │    │
│  └──────┬──────┘    │  ├─ RuleRepo    │  │  send_offsets_to_transaction  │ │    │
│         │           │  ├─ CHClient    │  │  └─ commit_transaction         │ │    │
│         │           │  ├─ Producer    │  │                                │ │    │
│         │           │  ├─ Counters    │  │  Phase 3: CH batch channel     │ │    │
│         │           │  └─ RuleCache   │  │  └─ blocking_send(Vec<Audit>)  │ │    │
│         │           │                  │  └────────────────────────────────┘ │    │
│         │           │                  │                                      │    │
│         │           │  ┌──────────────────────────────────────────────────┐  │    │
│         │           │  │  CH Writer (tokio task)                          │  │    │
│         │           │  │  mpsc::Receiver<Vec<AuditRecord>>                │  │    │
│         │           │  │  AuditWriter: 5000-row buffer → single HTTP POST │  │    │
│         │           │  └──────────────────────────────────────────────────┘  │    │
│         │           └──────────────────────────────────────────────────────────┘    │
│         │                    │                    │                   │             │
│         │                    ▼                    ▼                   ▼             │
│  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │  ClickHouse │  │   Redpanda   │  │   ClickHouse     │  │    PostgreSQL    │   │
│  │  port 8123  │  │  port 19092  │  │   port 8123      │  │    port 5432     │   │
│  │             │◀─│  source-     │  │                  │  │                  │   │
│  │  sre_obs    │  │  events      │  │  audits          │  │  rules table     │   │
│  │  table      │  │  target-     │  │  (RMT + MVs)     │  │  NOTIFY trigger  │   │
│  └─────────────┘  │  events      │  └──────────────────┘  └──────────────────┘   │
│                    └──────────────┘                                               │
└──────────────────────────────────────────────────────────────────────────────────┘

    ▲ Host machine
    │
    └─ host.docker.internal:8888  (Unsloth Studio — local LLM, not containerized)
```

---

## 3. Crate Dependency Graph

The workspace uses a strict layered dependency model. No lower crate knows about a higher one.

```
                     bin/rules-engine
                     bin/sre-agent
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
           web crate                 sre crate
              │                         │
     ┌────────┴────────┐               │
     ▼                 ▼               ▼
  pipeline          store-       clickhouse (external)
  crate          clickhouse       bollard (external)
     │              crate
     │                │
     ▼                │
  eval              rules-core
  crate    ◀─────────┘
     │
     ▼
 cel-interpreter (external)

store-postgres
     │
     ▼
   sqlx (external)
   rules-core
```

**Dependency rules enforced by the workspace:**
- `rules-core` has zero I/O dependencies. It is pure data types + serialization.
- `eval` knows about `rules-core` and `cel-interpreter` only.
- `pipeline` knows about `eval`, `store-postgres`, `store-clickhouse`, and `rdkafka`.
- `web` knows about `pipeline`, `store-postgres`, `store-clickhouse`, and `axum`.
- `sre` knows about `clickhouse` and `bollard` directly; it does not depend on `rules-core`.
- Binaries glue everything together but hold no logic of their own.

---

## 4. Data Flow: Message Lifecycle

### 4.1 Normal processing path (hot path)

```
Producer (simulator or external)
        │
        │  JSON payload published to source-events
        ▼
  Redpanda (12 partitions)
        │
        │  BaseConsumer.poll()  ←─── BATCH_TIMEOUT_MS = 100ms
        ▼
  Batch collection (up to BATCH_SIZE = 2000 messages)
        │
        │  Phase 1: rayon par_iter()  — runs on all CPU cores
        ├──────────────────────────────────────────────────────────┐
        │  per message:                                            │
        │    SourceEvent::from_kafka()  →  serde_json::from_str() │
        │    rules: Arc<Vec<CompiledRule>>  (lock-free ArcSwap)    │
        │    for each CompiledRule:                                │
        │      cel_interpreter::Program::execute(&context)        │
        │      build AuditRecord { audit_id, audit_type, ... }    │
        └──────────────────────────────────────────────────────────┘
        │
        │  Phase 2: serial EOS transaction
        ├─ producer.begin_transaction()
        │  for each matched event:
        │    producer.send(BaseRecord::to(target_topic))
        │  send_offsets_to_transaction(tpl, &group_metadata)
        └─ producer.commit_transaction()
        │
        │  Phase 3: ClickHouse write
        └─ ch_tx.blocking_send(Vec<AuditRecord>)
              │
              │  (async tokio task)
              ▼
           AuditWriter.write_batch()
              │
              │  buffer until 5000 rows
              ▼
           clickhouse::Inserter::end()  →  single HTTP POST to CH
              │
              ▼
           ClickHouse persists to audits table
           mv_rule_hourly and mv_hour_messages fire on INSERT
```

### 4.2 Rule hot-reload path

```
  Admin action: POST /api/rules or PUT /api/rules/:id
        │
        │  RuleRepository::create() / update()
        ▼
  Postgres rules table (ACID write)
        │
        │  DB trigger: notify_rules_changed()
        ▼
  pg_notify('rules_changed', rule_id)
        │
        │  PgListener::recv()  ←─── background tokio task
        ▼
  compile_enabled():
    SELECT all enabled rules
    for each: eval::compile(rule)  →  Program::compile(expression)
        │
        │  ArcSwap::store(Arc::new(compiled_rules))
        ▼
  RuleCache updated atomically — zero downtime, no lock on hot path

  Next batch:
    cache.get()  →  Arc::load_full()  →  reads new rule set
```

### 4.3 Analytics read path

```
  Frontend: GET /api/analytics/stats?from=...&to=...
        │
        ▼
  web::routes::analytics::stats()
        │
  tokio::join!(
    query agg_hour_messages  →  uniqMerge(msg_hll)
    query agg_rule_hourly    →  SUM(matched, unmatched, errored, latencies)
    query agg_rule_hourly GROUP BY rule_id
    query agg_rule_hourly GROUP BY hour, rule_id
  )
        │
        ▼
  AnalyticsStats { total_messages, total_evaluations, rule_stats, time_series, avg_*_nano }
        │
        ▼
  JSON response to frontend
```

---

## 5. Crate Internals

### 5.1 rules-core

**Purpose:** Shared domain types. Zero I/O. No Kafka, no DB, no HTTP.

**Key types:**

| Type | Description |
|---|---|
| `Rule` | Persisted business rule. Fields: `id`, `description`, `expression` (CEL), `target_topic`, `enabled`, `version`, `updated_at`. |
| `SourceEvent` | One Kafka record. Contains `raw: String` (original bytes) and `payload: serde_json::Value` (parsed, bound to CEL context as `event`). |
| `AuditRecord` | One row in ClickHouse. One per (event × rule) evaluation. Contains Kafka coordinates, timing, `audit_type`, and a copy of the raw source event. |
| `AuditType` | Enum: `Matched` / `Unmatched` / `Errored`. Serializes to Java enum names `MATCHED` / `UNMATCHED` / `ERRORED`. |

**Dedup key:** `audit_id = "{topic}:{partition}:{offset}:{rule_id}"`. This is the `ORDER BY` key in ClickHouse's `ReplacingMergeTree`, which collapses duplicate inserts on merge. The identity components (topic/partition/offset/rule_id) are stable across reprocessing; timestamp is not used in the key because reprocessing would change the timestamp but not the identity.

### 5.2 eval

**Purpose:** CEL compilation and evaluation. No I/O.

**`CompiledRule`** holds a pre-parsed `cel_interpreter::Program`. Compilation happens once at cache load time. Evaluation reuses the `Program` across every message in every batch.

**`compile(rule: &Rule) -> Result<CompiledRule, CompileError>`** wraps `Program::compile()` in `std::panic::catch_unwind` because the underlying ANTLR4-Rust parser can `unreachable!()` on some malformed expressions. User-supplied rule text is treated as untrusted input; a panic from a bad expression must not crash the pipeline process.

**`evaluate(compiled: &CompiledRule, event: &SourceEvent) -> RuleOutcome`** builds a CEL `Context`, binds the event's `payload` as the variable `event`, and calls `Program::execute()`. The three possible outcomes:
- `Value::Bool(true)` → `AuditType::Matched`
- `Value::Bool(false)` → `AuditType::Unmatched` with the expression as the reason
- `Ok(_)` non-bool or `Err(_)` → `AuditType::Errored` with the error as the reason

Evaluation time is measured in nanoseconds with `std::time::Instant` and stored in `AuditRecord.eval_time_nano`.

### 5.3 store-postgres

**Purpose:** Rule CRUD + LISTEN/NOTIFY infrastructure.

**Connection:** `sqlx::PgPoolOptions::new().connect(url)` — connection pool, async.

**Migration:** The `0001_rules.sql` migration is embedded at compile time via `include_str!()` and executed idempotently at startup with `CREATE TABLE IF NOT EXISTS`.

**`RuleRepository`** wraps `PgPool` and exposes `list()`, `get()`, `create()`, `update()`, `delete()` — all using parameterized queries (no string interpolation).

**`RuleChangeListener`** wraps `PgListener` (sqlx's `LISTEN` implementation). The background task in `main.rs` calls `listener.recv().await` in a loop; each notification triggers a full reload + ArcSwap. The Postgres trigger on the `rules` table fires `pg_notify('rules_changed', rule_id)` on every `INSERT`, `UPDATE`, and `DELETE`.

This eliminates the need for Redis pub/sub that the original Java system used for distributing rule changes.

### 5.4 store-clickhouse

**Purpose:** Batched audit writes + analytics queries.

**`AuditWriter`** maintains an internal `Vec<AuditRow>` buffer (capacity = `batch_max_rows = 5000`). `write_batch()` appends to the buffer and flushes when it reaches capacity. `end()` flushes the remainder. A single flush = one `clickhouse::Inserter::end()` call = one HTTP POST to ClickHouse's HTTP interface at port 8123.

**Before batching (S6 initial):** every `AuditRecord` was one HTTP POST → catastrophic at scale. At 100 rules per event, a batch of 2000 events = 200,000 individual HTTP requests. The CH client would serialize one insert at a time.

**After batching:** 200,000 rows per 2000-event batch → ~40 HTTP POSTs (each carrying 5000 rows). The ClickHouse HTTP interface is optimized for large row batches.

**Analytics queries** hit pre-aggregated materialized views (`agg_rule_hourly`, `agg_hour_messages`), never the raw `audits` table. This makes analytics queries O(hours_in_range × rules) rather than O(total_audit_rows).

### 5.5 pipeline

**Three sub-modules:**

**`rule_cache.rs` — `RuleCache`**

Uses `arc_swap::ArcSwap<Vec<CompiledRule>>` for a lock-free, atomically swappable rule set. All clones of `RuleCache` share the same `ArcSwap` pointer through an outer `Arc`. Reading the cache on the hot path is a single atomic load (`load_full()`), which returns an `Arc<Vec<CompiledRule>>` that the batch can hold while processing. The swap (`store()`) on reload is also atomic — consumers mid-batch complete with the old rule set, and the next `cache.get()` sees the new one.

**`consumer.rs` — Three-phase pipeline loop**

Runs in `tokio::task::spawn_blocking` (executes on a dedicated OS thread, freeing the tokio runtime for async I/O).

```
┌────────────────────────────────────────────────────────────┐
│  Phase 0: Batch collection                                  │
│  Deadline: now() + BATCH_TIMEOUT_MS (100ms)                 │
│  Limit:    BATCH_SIZE (2000 messages)                       │
│  consumer.poll(remaining_timeout)                           │
│  Build Vec<OwnedMsg> — owns Kafka payload strings           │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Phase 1: Parallel eval (rayon)                             │
│  batch.par_iter()  — distributes across CPU cores           │
│  per message:                                               │
│    SourceEvent::from_kafka()  ← single serde_json::parse   │
│    rules.iter().map(|r| evaluate(r, &event))               │
│    collect into Vec<(bool, AuditRecord)> per rule           │
│  Output: Vec<MsgEval>                                       │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Phase 2: EOS Kafka transaction (serial — Kafka requirement)│
│  producer.begin_transaction()                               │
│  for each matched (bool=true) event:                        │
│    producer.send(BaseRecord::to(target_topic))              │
│  build TopicPartitionList from last offsets                 │
│  producer.send_offsets_to_transaction(tpl, group_metadata) │
│  producer.commit_transaction()                              │
└────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Phase 3: Async CH channel                                  │
│  ch_tx.blocking_send(Vec<AuditRecord>)                      │
│  (tokio mpsc, capacity 32 — backpressure if CH is slow)    │
└────────────────────────────────────────────────────────────┘
```

`OwnedMsg` is a locally owned copy of the Kafka message payload (required because `rdkafka::Message` borrows from a buffer that gets invalidated on the next `poll()`). `MsgEval` carries the results from Phase 1 into Phase 2 without re-parsing.

**`metrics.rs` — `PipelineCounters`**

Lock-free atomic counters updated per batch. Shared between the pipeline loop (write) and the HTTP metrics handler (read) via `Arc<PipelineCounters>`. Uses `Ordering::Relaxed` throughout because the counters are approximate monitoring data — strict ordering constraints are unnecessary.

Fields: `messages_total`, `batches_total`, `eval_ms_total`, `txn_ms_total`, `consumer_lag`, `started_at`.

### 5.6 web

**`AppState`** is cheaply cloned (all heavy state is behind `Arc` or `Clone`-on-pointer):
- `rules: RuleRepository` — `PgPool` inside, clone shares the pool
- `ch_client: ClickHouseClient` — HTTP client, clone shares it
- `producer: Arc<FutureProducer>` — for simulation endpoint
- `counters: Arc<PipelineCounters>` — shared with pipeline loop
- `rule_cache: RuleCache` — shares the `ArcSwap` via `Arc`

**Routes:**

| Method | Path | Handler | Notes |
|---|---|---|---|
| GET | `/health` | `health()` | Liveness. Returns 200 `{"status":"ok"}` instantly. No I/O. |
| GET | `/health/ready` | `ready()` | Readiness. Pings PG, CH, Kafka in parallel. 503 if any fail. |
| GET | `/api/rules` | `list()` | `SELECT * FROM rules ORDER BY id` |
| POST | `/api/rules` | `create()` | `INSERT INTO rules ... RETURNING` + NOTIFY trigger fires |
| GET | `/api/rules/:id` | `get_one()` | `SELECT ... WHERE id = $1` |
| PUT | `/api/rules/:id` | `update()` | `UPDATE rules SET ... version=version+1 ... RETURNING` |
| DELETE | `/api/rules/:id` | `delete_one()` | `DELETE FROM rules WHERE id = $1` |
| GET | `/api/analytics/stats` | `stats()` | Queries agg MVs, returns `AnalyticsStats` |
| GET | `/api/reports/top` | `top()` | Raw audit rows from `audits` table for compliance view |
| GET | `/api/metrics` | `metrics()` | Live counters + live CH/PG queries + Kafka health |
| POST | `/api/simulation/push` | `push()` | Publishes N synthetic JSON events to `source-events` |

**Simulation endpoint** cap: `count.min(10_000)`. Events are published in a `tokio::spawn` background task so the response is immediate.

### 5.7 sre

**Purpose:** Autonomous SRE monitoring of the Docker container stack using a local LLM.

**Design contract:** the SRE agent is a separate binary (`bin/sre-agent`) with its own HTTP server on port 8088. It shares ClickHouse with the main app (writes to `sre_observations`), but does not share any in-process state.

**Analysis loop (runs every `SCAN_INTERVAL_SECS = 60s`):**

```
for each container:
  1. tail_logs(docker, &c.name, 200 lines)     ← bollard Docker API
  2. filter_noisy_logs(raw)                    ← keep WARN/ERROR/FATAL/PANIC only
  3. if filtered is empty → skip (no problems)
  4. sha256_hex(filtered) → hash
  5. if hash == last_hash[container] → skip (unchanged)
  6. reset_suppression(container)
  7. if quiet_streak[container] >= 3 → record_hash, skip (suppressed)
  8. llm.analyze(container, filtered)          ← POST to local LLM
  9. record_hash, record_severity
  10. write SreObservation to ClickHouse
  tokio::time::sleep(2s)                       ← avoid piling requests on LLM
```

**Suppression logic prevents LLM saturation:**
- Every "INFO" severity result increments `quiet_streak[container]`
- Any non-INFO severity resets `quiet_streak[container]` to 0
- Once `quiet_streak >= QUIET_SUPPRESS_AFTER (3)`, stop calling LLM until the log hash changes
- A new WARN/ERROR hash resets suppression, triggering a fresh LLM call

**LLM protocol:**
- OpenAI-compatible `/v1/chat/completions` endpoint
- 3-message conversation: system prompt → user (container + filtered logs) → assistant prefill `"{"`
- The assistant prefill forces the model to respond with a JSON object directly (no prose wrapper)
- Response is parsed as `Finding { severity, category, finding, proposed_fix }`
- HTTP timeout: 10 seconds per request

**SRE Dashboard** (`/api/state`) serves the in-memory `SreState` as JSON. `/api/events` streams findings as Server-Sent Events. The frontend `SreTab` polls `/api/state` every 30 seconds.

---

## 6. Database Schemas

### 6.1 PostgreSQL

```sql
CREATE TABLE rules (
    id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    description  TEXT        NOT NULL DEFAULT '',
    expression   TEXT        NOT NULL,          -- CEL expression
    target_topic TEXT        NOT NULL,
    enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    version      BIGINT      NOT NULL DEFAULT 1, -- incremented on update
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Trigger fires pg_notify('rules_changed', rule_id) on every write
```

### 6.2 ClickHouse — audits (raw)

```sql
CREATE TABLE audits (
    audit_id         String,                    -- "{topic}:{partition}:{offset}:{rule_id}"
    rule_id          LowCardinality(String),
    schema_version   UInt32,
    audit_type       LowCardinality(String),    -- MATCHED | UNMATCHED | ERRORED
    reason           String,                    -- empty string if absent
    source_event     String,                    -- original JSON payload
    routed_event     String,                    -- copy of source if matched, else ''
    source_topic     LowCardinality(String),
    partition        Int32,
    offset           Int64,
    timestamp        DateTime64(3),             -- millisecond precision
    parse_time_nano  UInt64,
    eval_time_nano   UInt64,
    total_time_nano  UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (source_topic, partition, offset, rule_id)  -- identity-based dedup key
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;
```

`ReplacingMergeTree` choice: dedup key must be the natural audit identity
(`topic:partition:offset:rule_id`), not `(timestamp, rule_id)`. If the same event is
reprocessed (crash recovery), it produces the same audit_id and the MergeTree collapses
the duplicate on merge. Using timestamp in the key would cause the same logical audit to
appear twice (different wall-clock times).

### 6.3 ClickHouse — analytics aggregates

```sql
-- Per-rule hourly rollup (SummingMergeTree automatically sums on merge)
CREATE TABLE agg_rule_hourly (
    hour       DateTime,
    rule_id    LowCardinality(String),
    matched    UInt64,
    unmatched  UInt64,
    errored    UInt64,
    eval_count UInt64,
    parse_sum  UInt64,    -- sum of parse_time_nano (divide by eval_count for avg)
    eval_sum   UInt64,
    total_sum  UInt64
) ENGINE = SummingMergeTree(matched, unmatched, errored, eval_count, parse_sum, eval_sum, total_sum)
  ORDER BY (hour, rule_id);

-- Distinct message count per hour (HyperLogLog approximate)
CREATE TABLE agg_hour_messages (
    hour     DateTime,
    msg_hll  AggregateFunction(uniq, String)   -- HLL state, not a plain count
) ENGINE = AggregatingMergeTree()
  ORDER BY hour;
```

The materialized views `mv_rule_hourly` and `mv_hour_messages` fire automatically on every
INSERT into `audits`. This replaces the Java system's Redis-locked scheduled aggregation
job (`RollupService`). Analytics are always current to the last inserted batch.

`msg_hll` uses `uniqState()`/`uniqMerge()` (HyperLogLog) rather than `countDistinct()` to
avoid counting the same source event multiple times when it was evaluated against 101 rules
(producing 101 audit rows, all with the same source offset).

### 6.4 ClickHouse — sre_observations

```sql
CREATE TABLE sre_observations (
    observed_at     DateTime64(3),
    container_name  LowCardinality(String),
    severity        LowCardinality(String),    -- INFO | WARN | ERROR | CRITICAL
    category        LowCardinality(String),
    finding         String,
    proposed_fix    String,
    log_window_hash String,                    -- SHA256 of filtered log lines
    log_snippet     String                     -- first 500 chars of filtered logs
) ENGINE = ReplacingMergeTree(observed_at)
  ORDER BY (container_name, log_window_hash)
  TTL toDateTime(observed_at) + INTERVAL 30 DAY;
```

---

## 7. SRE Agent Design

```
┌──────────────────────────────────────────────────────────────┐
│  sre-agent container                                          │
│                                                              │
│  ┌────────────────────┐    ┌───────────────────────────┐    │
│  │  analysis_loop()   │    │  dashboard::serve()       │    │
│  │  tokio::spawn      │    │  axum, port 8088          │    │
│  │                    │    │                           │    │
│  │  every 60s:        │    │  GET /health              │    │
│  │  scan_once()       │    │  GET /api/state  (JSON)   │    │
│  │  ├─ list_containers│    │  GET /api/events (SSE)    │    │
│  │  └─ analyze_each   │    └───────────────────────────┘    │
│  │                    │                                      │
│  │  SreStore (memory) │    Arc<RwLock<SreState>>            │
│  │  ├─ last_hashes    │◀───├─ containers: Vec<Status>       │
│  │  └─ quiet_streaks  │    ├─ findings:   VecDeque<Finding> │
│  └────────────────────┘    └─ llm_available: bool           │
│                                                              │
│  /var/run/docker.sock (mounted read-only)                    │
└──────────────────────────────────────────────────────────────┘
           │                              │
           │ bollard (Docker API)         │ reqwest HTTP
           ▼                              ▼
     Docker containers         host.docker.internal:8888
     (tail logs)               (Unsloth Studio local LLM)
```

**Key design choices:**

- **Hash-based dedup**: SHA256 of the filtered log window prevents sending identical log
  patterns to the LLM repeatedly. Only new WARN/ERROR lines trigger a call.
- **Quiet suppression**: After 3 consecutive INFO results on the same log hash, the container
  is suppressed. LLM is expensive; if logs are stable and LLM keeps saying "nothing unusual",
  stop asking.
- **In-memory state, persistent history**: `SreStore` is in-memory (fast). Findings that
  warrant recording are written to ClickHouse `sre_observations` (durable).
- **2s inter-container delay**: The local LLM is single-process inference. Sending 6
  container log sets simultaneously would queue them all on the LLM, burning through the
  10s timeout. Staggering them gives the LLM time to respond before the next request arrives.

---

## 8. Network Topology

```
Host machine (macOS)
├── localhost:3000     → rre-frontend (nginx serving React SPA)
├── localhost:8080     → rre-app (rules-engine API + pipeline)
├── localhost:8088     → rre-sre-agent (SRE dashboard)
├── localhost:8123     → rre-clickhouse (HTTP interface)
├── localhost:9000     → rre-clickhouse (native TCP)
├── localhost:5432     → rre-postgres
├── localhost:19092    → rre-redpanda (Kafka external listener)
├── localhost:9644     → rre-redpanda (admin HTTP API)
└── localhost:8888     → Unsloth Studio (NOT containerized — runs on host)

Docker compose network (bridge):
├── redpanda:9092      (internal Kafka listener for containers)
├── clickhouse:8123    (HTTP for containers)
├── postgres:5432
├── app:8080
└── sre-agent:8088

nginx (frontend) proxies:
  /api/*    →  http://app:8080
  /health*  →  http://app:8080
  /         →  /usr/share/nginx/html (static SPA)
  (SRE tab polls http://localhost:8088 directly from browser)
```

---

## 9. Key Invariants

| Invariant | Where enforced |
|---|---|
| Every event is evaluated against every enabled rule | `consumer.rs` Phase 1: `rules.iter().map(evaluate)` |
| At-most-once matched event delivery to target topic | EOS `commit_transaction` (idempotent producer + offset commit atomic) |
| Audit record exists for every (event × rule) pair | AuditRecord built regardless of `audit_type`; all sent to CH |
| No audit row duplication on crash recovery | `audit_id = "{topic}:{partition}:{offset}:{rule_id}"` dedup key in `ReplacingMergeTree` |
| Rule compilation errors don't crash the pipeline | `compile()` wraps parser in `catch_unwind`; bad rules are skipped with a warning |
| Rule changes propagate without restart | `LISTEN/NOTIFY` + `ArcSwap::store()` — atomic, lock-free, no connection drops |
| Analytics always reflect current data | MVs fire on every CH INSERT; no batch job needed |
| HTTP health check is always fast | `/health` returns 200 instantly (no I/O); `/health/ready` has 2s per-service timeout |
| SRE LLM calls don't accumulate | 10s HTTP timeout + 2s inter-container gap + hash dedup + quiet suppression |
