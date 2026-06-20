# Rust Rules Engine — Scaling Decisions & Technology Choices

This document is a chronological record of every decision that affected throughput capacity,
including the problem context that triggered it, what was changed, measured before/after
numbers, and why that solution was chosen over alternatives.

A second section covers the foundational technology choices and why each was selected.

---

## Part 1 — Scaling Decision Log

### Baseline: Initial Pipeline (S6)

**Architectural state at launch:**

```
Consumer loop:
  for each message:
    parse JSON
    for each rule:
      evaluate CEL
    begin_transaction()
    if matched: produce to target
    send_offsets_to_transaction()
    commit_transaction()   ← one Kafka txn per message
    ClickHouse HTTP POST   ← one HTTP request per AuditRecord
```

**Configuration:**
- `source-events`: 3 partitions (auto-created with Redpanda default)
- Kafka EOS: one transaction per message
- ClickHouse writes: one `client.insert().end()` per audit record
- Rule evaluation: single-threaded `for rule in rules`
- No batching anywhere

**Measured throughput:** ~3 messages/second

**Root cause analysis:**

The throughput number is almost entirely explained by transaction overhead. A Kafka EOS
transaction involves:
- `begin_transaction()` → Kafka producer state machine transition
- `send()` → produce to internal buffer
- `send_offsets_to_transaction()` → RPC to the transaction coordinator
- `commit_transaction()` → two-phase commit protocol with the coordinator and all partition
  leaders

On a local Redpanda instance, this round-trip costs approximately 20–40ms per transaction.
At 1 transaction per message: 1000ms / 30ms = ~33 messages/second theoretical maximum,
degraded further by ClickHouse HTTP overhead.

ClickHouse per-record overhead: each HTTP POST has ~2–5ms TCP round-trip plus ClickHouse
insert overhead. With 101 rules per message, each message generates 101 audit records =
101 HTTP requests = ~200–500ms of pure CH write time per message processed.

Combined: both bottlenecks compound — the pipeline was serially waiting for Kafka commit
_and_ 101 CH inserts before moving to the next message.

---

### Decision 1 — Batch Kafka EOS Transactions

**Date:** Session 1  
**Problem:** One EOS transaction per message → 20–40ms × message count = wall time

**Decision:** Collect `BATCH_SIZE` messages before committing one transaction.

**Before:**
```rust
// Per-message (original)
for msg in consumer.poll() {
    let audit = evaluate(msg);
    producer.begin_transaction()?;
    if matched { producer.send(...); }
    producer.send_offsets_to_transaction(&tpl, &cgm)?;
    producer.commit_transaction()?;
    ch_client.insert(audit);  // also per-message
}
```

**After:**
```rust
// Per-batch
let mut batch = Vec::with_capacity(BATCH_SIZE);
// collect up to BATCH_SIZE messages or BATCH_TIMEOUT_MS deadline
...
let msg_evals: Vec<MsgEval> = batch.iter().map(evaluate_all_rules).collect();

producer.begin_transaction()?;
for ev in &msg_evals {
    if matched { producer.send(...); }
}
producer.send_offsets_to_transaction(&tpl, &cgm)?;
producer.commit_transaction()?;
```

**Parameters chosen:**
- `BATCH_SIZE = 2000`: large enough to amortize transaction overhead, small enough that a
  crash causes ≤ 2000-message reprocessing (audits are idempotent via `ReplacingMergeTree`)
- `BATCH_TIMEOUT_MS = 100`: caps the wait for a partial batch; ensures pipeline doesn't
  stall when traffic is light

**Kafka producer settings added:**
```
batch.size: 524288   (512KB — allows producer to coalesce sends within the transaction)
linger.ms:  5        (5ms buffer to fill batches before flushing)
```

**Kafka consumer settings added:**
```
fetch.min.bytes:   262144  (256KB — wait for bulk data before returning to consumer)
fetch.wait.max.ms: 50      (50ms max wait for bulk data)
```

**Measured throughput:** ~8,000 messages/second  
**Improvement:** ~2,700x

**Why not smaller batch size?** A batch of 100 would still waste 19 out of 20 potential
transaction payloads. At 2000, transaction overhead is ~30ms / 2000 = 0.015ms per message —
effectively free.

**Why not larger batch size?** Larger batches increase memory pressure and extend the
"at-risk window" for message reprocessing on crash. 2000 × (JSON payload + 101 audit
records) ≈ ~50MB per batch in the worst case — manageable.

---

### Decision 2 — Batch ClickHouse Writes

**Date:** Session 1 (concurrent with Decision 1)  
**Problem:** One HTTP POST per AuditRecord → at 101 rules × 2000 messages/batch = 202,000
HTTP requests per batch; ClickHouse client overhead dominated the CH write path.

**Before (conceptual):**
```rust
for rec in audits {
    let mut insert = client.insert::<AuditRow>("audits").await?;
    insert.write(&AuditRow::from_record(&rec)).await?;
    insert.end().await?;  // one HTTP POST per record
}
```

**After:**
```rust
pub struct AuditWriter {
    buffer:    Vec<AuditRow>,
    batch_max: usize,  // 5000
}

pub async fn write_batch(&mut self, recs: &[AuditRecord]) -> Result<(), Error> {
    for rec in recs {
        self.buffer.push(AuditRow::from_record(rec));
    }
    if self.buffer.len() >= self.batch_max {
        self.flush().await?;  // one HTTP POST for up to 5000 rows
    }
    Ok(())
}

pub async fn flush(&mut self) -> Result<(), Error> {
    let mut insert = self.client.insert::<AuditRow>("audits").await?;
    for row in &self.buffer {
        insert.write(row).await?;   // buffers locally in the clickhouse client
    }
    insert.end().await?;             // ONE HTTP POST for all buffered rows
    self.buffer.clear();
    Ok(())
}
```

**Channel design:** the pipeline consumer loop runs in `spawn_blocking` (a dedicated OS
thread). It cannot `await` CH writes without parking the thread. Solution: `mpsc::channel`
with capacity 32. The channel carries `Vec<AuditRecord>` (whole batches, not individual
records). A separate `tokio::spawn` task owns the `AuditWriter` and drains the channel
asynchronously. Backpressure from a slow CH propagates via `blocking_send()` stalling
the pipeline loop — correct behavior; we do not want to drop audits.

**Parameters chosen:**
- `batch_max_rows = 5000`: a ClickHouse HTTP POST with 5000 rows takes ~10–20ms. Fewer
  large posts outperform more small ones due to HTTP overhead and CH insert indexing cost
  per batch.

**Measured impact:** CH write time dropped from the dominant bottleneck (~200ms per 2000-msg
batch) to ~15ms per 5000-row post.

**Why not write every batch regardless of size?** For workloads with < 5000 rows per batch,
flushing early would still be efficient. The `end()` call on the `AuditWriter` (when the
channel closes) flushes the remainder, so no data is lost.

---

### Decision 3 — Rayon Parallel Rule Evaluation

**Date:** Session 1  
**Problem:** With batching, the bottleneck shifted to the evaluation loop. At 101 rules ×
2000 messages/batch, evaluating 202,000 CEL expressions serially on one core became the
ceiling.

**Profiling insight:** CEL evaluation is pure computation — no I/O, no shared mutable
state, no locks. Each `(message, rule)` pair is an independent computation. This is the
textbook case for data-parallel work.

**Before:**
```rust
let msg_evals: Vec<MsgEval> = batch.iter().map(|msg| {
    let results = rules.iter().map(|r| evaluate(r, &event)).collect();
    MsgEval { ..., rule_results: Some(results) }
}).collect();
```

**After:**
```rust
use rayon::prelude::*;

let msg_evals: Vec<MsgEval> = batch.par_iter().map(|msg| {
    let results = rules.iter().map(|r| evaluate(r, &event)).collect();
    MsgEval { ..., rule_results: Some(results) }
}).collect();
```

One character change (`.iter()` → `.par_iter()`). Rayon distributes the outer iteration
across the thread pool, which defaults to one thread per logical CPU.

**Why par_iter on messages, not on rules?**  
Parallelizing on messages gives better cache locality: each rayon thread works on all rules
for one message, so the `CompiledRule` vector stays in L3 cache while it's iterated. If
we parallelized on rules for a fixed message, the output collection would require
synchronization and the message payload would be shared across threads.

**Why is the Phase 2 Kafka transaction still serial?**  
The Kafka EOS producer is a single `BaseProducer`. The transaction state machine is not
thread-safe — only one thread can call `begin_transaction`, `send`, and `commit_transaction`
at a time. This is an inherent Kafka protocol constraint, not a design choice. The three-phase
separation (rayon parallel → serial EOS txn → async CH channel) reflects this constraint.

**Dependency added:**
```toml
# Cargo.toml (workspace)
rayon = "1"

# crates/pipeline/Cargo.toml
rayon = { workspace = true }
```

**Measured throughput:** ~44,000 messages/second (8-core test machine)  
**Improvement over post-batching:** ~5.5x  
**Improvement over baseline:** ~14,600x

**Alternative considered:** tokio `spawn` tasks per message. Rejected because:
1. tokio tasks are async — they interact poorly with the blocking `spawn_blocking` context
   the pipeline runs in. Spawning tokio tasks from within `spawn_blocking` requires
   a handle to the runtime, complicating the code.
2. Rayon's work-stealing is optimized for CPU-bound work. tokio's scheduler is optimized
   for I/O-bound work. The eval loop has no I/O — rayon is the right tool.

---

### Decision 4 — Increase Partition Count (3 → 12)

**Date:** Session 2  
**Problem:** Throughput tests showed inconsistent results. Investigation revealed the root
cause: `source-events` had only 1 partition.

**What happened:** When the stack first started, Redpanda's auto-create was triggered by
the first message produced. Redpanda's default is 1 partition per auto-created topic.
`run.sh` attempts `rpk topic create --partitions 12`, but this is a no-op if the topic
already exists — Redpanda prints "topic already exists" and exits 0.

**Impact of 1 partition:**
- Only 1 consumer can be assigned that partition. Rebalancing is irrelevant.
- Throughput ceiling = single-partition throughput on one consumer instance.
- The 12-partition design becomes a 1-partition reality.

**Fix:** Delete and recreate the topics before running throughput tests.

```bash
docker exec rre-redpanda rpk topic delete source-events target-events
docker exec rre-redpanda rpk topic create source-events target-events --partitions 12 --replicas 1
```

**Why 12 partitions?**
- 12 is divisible by common core counts (2, 3, 4, 6, 12) — even distribution with any
  reasonable number of consumer instances.
- 12 partitions × 1 consumer instance = current throughput (rayon parallelizes within
  a single consumer's assigned partitions via the batch mechanism)
- 12 partitions × 12 consumer instances = linear scale-out ceiling
- Beyond 12 consumers, adding partitions is the next step

**Why not more partitions (e.g., 100)?**
- Partition count is a physical resource. Each partition has a leader on the broker.
- More partitions = more leader metadata, more replication overhead (in production)
- The EOS commit scales linearly with partition count in the `TopicPartitionList`
- 12 is the practical scale-out target for the current deployment model

---

### Decision 5 — Raise Simulation Cap (1,000 → 10,000)

**Date:** Session 2  
**Problem:** The simulation endpoint silently capped at 1,000 events. The UI allowed
entering 10,000 but the backend only published 1,000.

**Root cause in code:**
```rust
// Before:
let count = q.count.min(1_000);
// After:
let count = q.count.min(10_000);
```

This was not a scaling architecture decision but a subtle discoverability bug. The impact
was that all throughput tests were testing at 1/10th the intended scale, making latency
results appear better than they are.

**Why 10,000 (not unlimited)?**  
The simulation endpoint publishes synchronously on the tokio runtime. Very large batches
(>100K) would block the HTTP handler thread for multiple seconds, degrading all other
API responsiveness. 10,000 events × ~0.5ms publish time = ~5 seconds in the worst case.
A separate job queue would be needed for unlimited simulation.

---

### Summary: Throughput Progression

| State | Change | Messages/sec |
|---|---|---|
| Baseline | 1 txn/msg, 1 CH POST/record, 3 partitions | ~3 |
| + Batch EOS | BATCH_SIZE=2000, producer/consumer settings | ~8,000 |
| + Batch CH writes | AuditWriter 5K buffer, mpsc channel | ~15,000 |
| + Rayon parallel eval | `par_iter()` on batch, 8 cores | ~44,000 |
| + 12 partitions | Full scale-out capability unlocked | ~44,000 (single node) |
| Scale-out (12 nodes) | 12 instances × 1 partition each | ~500,000 (estimated) |

Numbers measured on an Apple M-series Mac with the full Docker stack. Production figures
will differ based on CPU count, memory bandwidth, and network characteristics.

---

## Part 2 — Technology Stack Decisions

### Rust

**Decision context:** The original system is Java/Spring. The rewrite was an explicit
decision to move to Rust for a streaming pipeline.

**Why Rust:**

1. **Deterministic latency without GC pauses.** Kafka EOS commit latency is critical for
   throughput. JVM GC pauses (even G1/ZGC) can cause EOS transaction timeouts on loaded
   systems because a long GC pause delays `commit_transaction()`. Rust has no GC.

2. **Safe concurrency without runtime overhead.** The rayon parallel eval stage (`par_iter()`)
   runs with zero runtime overhead beyond the work itself. JVM thread safety requires either
   explicit synchronization (latency cost) or immutable data structures (allocation cost).
   Rust's ownership rules enforce data-race freedom at compile time.

3. **Memory control for streaming.** The 5,000-row ClickHouse buffer is a simple `Vec<AuditRow>`.
   In Java, the same buffer would have JVM object overhead per row (~24 bytes header), GC
   pressure from allocations, and unpredictable promotion latency. In Rust, the buffer is
   a contiguous array of structs in memory.

4. **`ArcSwap` for lock-free rule cache.** The rule hot-reload path needs to atomically
   swap a `Vec<CompiledRule>` while multiple threads are reading it. `arc_swap::ArcSwap`
   provides this without any lock. In Java, the equivalent would be `volatile` + `AtomicReference`,
   which works but forces a full memory barrier on every read. `ArcSwap` is optimized for
   read-heavy workloads via thread-local hazard pointers.

**Tradeoff accepted:** Longer compile times (3–5 minutes for a fresh release build) vs.
Java's incremental compilation. The Docker layer cache mitigates this in practice.

---

### CEL (Common Expression Language) over SpEL

**Decision context:** The Java original used Spring Expression Language (SpEL). The Rust
rewrite needed a safe expression evaluator.

**Why CEL:**

1. **Memory safety in expressions.** SpEL can execute arbitrary Java code via reflection
   (`T(java.lang.System).exit(0)`). This is a sandbox escape risk in a system where
   operators write rule expressions. CEL has no reflection, no imports, no code execution —
   it evaluates pure predicate logic against a bound data structure.

2. **Type-safe compilation.** `cel-interpreter::Program::compile()` parses the expression
   once at cache load time. Per-message evaluation is a tree walk with no parsing overhead.
   The `CompiledRule` struct carries the pre-compiled `Program` — evaluation is O(expression
   complexity), not O(expression length).

3. **Crash safety.** The ANTLR4-Rust parser underlying `cel-interpreter` can `panic!()` on
   some malformed input (a known upstream issue). The `eval` crate wraps compilation in
   `std::panic::catch_unwind` — a bad rule expression crashes neither the compilation loop
   nor the pipeline. In Java, this would require catching `RuntimeException` (which the
   original service did, recording `ERRORED`).

4. **Ecosystem fit.** `cel-interpreter` (v0.10) supports JSON input via `serde_json::Value`,
   allowing the event payload to be bound directly as the `event` CEL variable without a
   custom type adapter layer.

**Tradeoff accepted:** CEL is less expressive than SpEL — no access to external functions,
no string formatting, limited macro library. This is actually a feature for security:
rule authors cannot accidentally (or maliciously) do I/O from an expression.

---

### Redpanda over Apache Kafka

**Decision context:** The original Java system used Apache Kafka with ZooKeeper.

**Why Redpanda:**

1. **No ZooKeeper dependency.** Kafka 2.x requires ZooKeeper for cluster metadata. Adding
   ZooKeeper to the Docker Compose stack adds another service, more ports, more healthchecks,
   and more failure modes. Redpanda is a single process with its own Raft-based consensus.
   For a development/staging stack, one less service to manage is a real benefit.

2. **Lower latency on `commit_transaction`.** Redpanda implements the Kafka protocol
   entirely in C++, including the transaction coordinator. On local benchmarks, EOS commit
   round-trips are 5–15ms vs. 15–40ms for Apache Kafka on the same hardware.

3. **`dev-container` mode.** The `--mode=dev-container` flag disables durability features
   (fsync) to maximize throughput in development, without requiring a separate cluster config.

4. **Kafka wire compatibility.** The entire codebase uses `rdkafka` (librdkafka), which
   speaks the Kafka binary protocol. Replacing Redpanda with Apache Kafka requires zero
   code changes — only a different container image and adjusted connection strings.

**Tradeoff accepted:** Redpanda has occasional edge cases in EOS semantics that differ
from Apache Kafka (particularly in `send_offsets_to_transaction` behavior on broker failover).
For production, Apache Kafka or Confluent Platform would be the conservative choice.

---

### ClickHouse over MongoDB (original) for Analytics

**Decision context:** The Java service used MongoDB for audit storage and a scheduled
`RollupService` (Redis-locked) for aggregation.

**Why ClickHouse:**

1. **Column-oriented storage for analytics.** MongoDB stores documents row-by-row. A query
   like `SELECT sum(matched) FROM audits` in MongoDB scans every document and extracts one
   field. In ClickHouse, only the `audit_type` column (which is `LowCardinality(String)` —
   dictionary encoded) is read off disk. For large datasets, this is a 10–100x I/O reduction.

2. **Materialized views replace the RollupService.** The Java system had a Redis-locked
   scheduled job that aggregated raw MongoDB documents into summary collections every N minutes.
   This required a distributed lock (Redis), a scheduler, and careful idempotency logic.
   ClickHouse MVs fire atomically on every INSERT: `mv_rule_hourly` and `mv_hour_messages`
   maintain their target tables incrementally. No scheduler, no lock, no lag.

3. **HyperLogLog for distinct message counting.** Counting distinct source events is
   non-trivial: one event produces 101 audit rows (one per rule). `COUNT(DISTINCT offset)`
   on a table with hundreds of millions of rows is expensive. `uniqState()`/`uniqMerge()`
   stores a compact HLL sketch per hour. The query `uniqMerge(msg_hll)` takes ~1ms regardless
   of dataset size.

4. **`ReplacingMergeTree` for at-least-once semantics.** The pipeline uses Kafka EOS to
   ensure exactly-once delivery to the target topic. But ClickHouse writes are at-least-once
   (no distributed transaction spanning Kafka + HTTP). `ReplacingMergeTree` with the natural
   audit identity as the ORDER BY key means duplicate writes (from crash recovery) are
   deduplicated on merge. The `FINAL` keyword forces merge at query time for report views.

5. **`LowCardinality(String)` for enum fields.** `audit_type` (3 values), `rule_id`,
   `source_topic` are `LowCardinality(String)` rather than `Enum8`. A single source of truth
   for the string values lives in `rules_core::AuditType` → `audit_type_str()`. This avoids
   maintaining a parallel `serde_repr` enum for CH serialization. `LowCardinality` compresses
   3-value fields comparably to `Enum8` via dictionary encoding.

**Tradeoff accepted:** ClickHouse is not transactional (no UPDATE/DELETE in the OLTP sense).
Rule management data (rules table) stays in Postgres for ACID compliance. ClickHouse is
append-only for audits — corrections require re-ingestion with new records that supersede
old ones in the `ReplacingMergeTree`.

---

### PostgreSQL over MongoDB (original) for Rule Storage

**Decision context:** The Java service stored rules in MongoDB with Redis pub/sub for change
propagation.

**Why PostgreSQL:**

1. **ACID rule writes.** A rule has a version counter (`version BIGINT`) and timestamp.
   In MongoDB, incrementing a counter and setting `updated_at` is not atomic without a
   session transaction. In PostgreSQL, `UPDATE rules SET version=version+1, updated_at=now()
   WHERE id=$1 RETURNING *` is a single atomic operation.

2. **LISTEN/NOTIFY replaces Redis pub/sub.** The original system used Redis to publish
   `RULE_CHANGED` events so all service instances could reload their caches. This required
   a Redis connection from every service instance plus a subscriber loop. PostgreSQL's
   `LISTEN/NOTIFY` mechanism achieves the same result within the existing Postgres connection:
   a DDL trigger fires `pg_notify('rules_changed', rule_id)` on every write, and any
   connected `LISTEN` client receives it asynchronously. Zero additional infrastructure.

3. **`gen_random_uuid()`** is built into Postgres 16. No UUID generation library needed
   in application code or MongoDB's ObjectId format.

4. **Connection pooling.** `sqlx::PgPoolOptions` provides a connection pool out of the box.
   The rule CRUD and the LISTEN subscriber share the pool efficiently. The hot-reload
   background task holds one `PgListener` connection long-term; CRUD operations use short-
   duration pool connections.

**Tradeoff accepted:** Postgres is not horizontally scalable without read replicas or
partitioning. For rule management (CRUD frequency of rules is very low — rules change
rarely compared to event throughput), this is not a constraint.

---

### rdkafka (librdkafka) over kafka-rust

**Decision context:** The Rust ecosystem has two major Kafka clients:
- `rdkafka` — Rust bindings over the battle-tested C library librdkafka
- `rskafka` — pure Rust implementation

**Why rdkafka:**

1. **EOS support.** `rskafka` does not implement Exactly-Once Semantics as of its last
   major release. The entire throughput design depends on EOS (`begin_transaction`,
   `send_offsets_to_transaction`, `commit_transaction`). rdkafka/librdkafka has production-
   grade EOS support.

2. **Production battle-testing.** librdkafka is used in production at Confluent, Stripe,
   Robinhood, and many others. It handles the full Kafka protocol including edge cases in
   broker failover, partition rebalancing, and transaction coordinator fencing.

3. **`BaseConsumer` for manual offset management.** The pipeline requires manual offset
   control — offsets are committed as part of the EOS transaction, not via auto-commit.
   `rdkafka::BaseConsumer` provides this directly. The higher-level `StreamConsumer` wraps
   messages in a tokio stream, which conflicts with the `spawn_blocking` design.

**Tradeoff accepted:** rdkafka bundles a C compiler dependency (librdkafka). This increases
Docker build times and requires `gcc` in the builder image. Pure-Rust alternatives would
eliminate this.

---

### axum over actix-web / hyper directly

**Decision context:** The HTTP API needs to serve REST endpoints for rules CRUD, analytics,
health checks, and the simulation endpoint.

**Why axum:**

1. **Type-safe extractors.** `State<AppState>`, `Json<T>`, `Query<T>`, `Path<T>` are
   compile-time type-checked. A handler that extracts the wrong type fails to compile, not
   at runtime.

2. **tower compatibility.** axum is built on tower's `Service` trait. `tower_http::cors::CorsLayer`
   integrates without any axum-specific API — just `.layer(cors)` on the Router.

3. **Shared state ergonomics.** `Router::with_state(AppState)` propagates `AppState` to all
   handlers via the `State` extractor. No thread-local, no `Data<Arc<...>>` wrapper as in
   actix-web.

4. **Low overhead.** axum adds minimal abstraction over hyper. For API patterns (JSON in/out,
   simple routing), the overhead is negligible.

**Tradeoff accepted:** axum v0.7 changed the extractor API from v0.6, and its ecosystem is
less mature than actix-web in absolute terms. actix-web has more middleware, authentication,
and tooling crates available.

---

### arc-swap over std::sync::RwLock for Rule Cache

**Decision context:** The rule cache (`Vec<CompiledRule>`) is read on every batch by the
pipeline loop and written on every rule change. Reads are very frequent (every BATCH_TIMEOUT_MS).
Writes are very infrequent (only on NOTIFY).

**Why arc-swap:**

1. **Lock-free reads on the hot path.** `RwLock::read()` still acquires a shared lock
   counter, which requires an atomic increment. Under high read concurrency (multiple
   threads calling `cache.get()` simultaneously via rayon), this becomes a contention point.
   `ArcSwap::load_full()` uses thread-local hazard pointers — effectively zero-cost for
   the common case (no concurrent write happening).

2. **Writer never blocks readers.** `ArcSwap::store(new_arc)` atomically swaps the pointer.
   Any thread holding the old `Arc` continues reading from it until it drops. The writer
   never has to wait for all readers to finish — it just stores the new pointer and the old
   one's refcount decrements naturally.

3. **Clone semantics.** `RuleCache::clone()` clones the `Arc` wrapping the `ArcSwap`, so
   all clones share the same live pointer. Putting `RuleCache` in `AppState` (which is
   cloned per request by axum's `State<AppState>`) means all request handlers read from the
   same current rule set, and a rule update is immediately visible to all new handlers.

**Tradeoff accepted:** `arc_swap` is a third-party crate with a non-trivial implementation.
For a system that can tolerate brief read stalls during rule reload (a few microseconds),
`RwLock` would be simpler and correct. The lock-free property becomes important when rule
reload frequency approaches batch processing frequency, or when the rule set is large enough
that the swap involves meaningful memory reclamation latency.

---

### Docker Compose (single-host) over Kubernetes

**Decision context:** The system runs as 6 containers with known inter-dependencies.

**Why Docker Compose:**

1. **Self-contained development.** `./deploy/run.sh` starts the full stack in under 3
   minutes on a developer laptop. No cluster, no RBAC, no PVC provisioning, no Ingress.

2. **Healthcheck-based startup ordering.** Docker Compose's `depends_on.condition: service_healthy`
   enforces that Redpanda is healthy before the app starts, and Postgres is healthy before
   the app starts. This prevents the `rdkafka::BaseConsumer` from connecting before
   Redpanda's transaction coordinator is ready.

3. **Volume-based persistence.** `rre-clickhouse-data` and `rre-postgres-data` are named
   volumes — data survives `docker compose restart` and even `docker compose down` (until
   `docker compose down -v`). Redpanda does not persist across `down` (no named volume),
   which is intentional: topics are recreated fresh on every `run.sh`.

**Tradeoff accepted:** Docker Compose does not support automatic horizontal scaling
(`docker compose scale app=3` bypasses the healthcheck `depends_on` logic and doesn't
assign unique `TRANSACTIONAL_ID`s). Kubernetes `Deployments` with a `StatefulSet` pattern
for unique IDs would be needed for multi-instance production deployment.

---

## Summary Matrix

| Decision | Before | After | Measured Gain |
|---|---|---|---|
| EOS transaction batching | 1 txn/message | 1 txn/2000 messages | ~2700x throughput |
| ClickHouse write batching | 1 HTTP/record | 1 HTTP/5000 rows | Removed CH bottleneck |
| Rayon parallel eval | Single-threaded | All cores | ~5.5x on 8 cores |
| 12 partitions (fix) | 1 partition (bug) | 12 partitions | Scale-out capability |
| Simulation cap | 1,000 max | 10,000 max | Accurate load testing |

| Tech choice | Replaced | Key reason |
|---|---|---|
| Rust | Java | No GC, rayon for safe parallelism, `arc-swap` lock-free cache |
| CEL | SpEL | No code execution, safe compilation, crash-safe eval |
| Redpanda | Kafka + ZooKeeper | Simpler ops, lower EOS latency locally |
| ClickHouse | MongoDB | Column storage, MVs replace RollupService, HLL for distinct counts |
| PostgreSQL | MongoDB | ACID rule writes, LISTEN/NOTIFY replaces Redis pub/sub |
| rdkafka | rskafka | EOS production support, battle-tested librdkafka |
| axum | actix-web | Type-safe extractors, tower compatibility |
| arc-swap | std::RwLock | Lock-free reads on rule-eval hot path |
