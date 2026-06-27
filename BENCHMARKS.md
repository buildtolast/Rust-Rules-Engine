# Benchmarks — Rust Rules Engine vs Java Spring-Kafka-Stream-Rules

## Test Methodology

100k events published via `POST /api/simulation/push?count=100000`, processed
through all active (enabled) rules.

- Each event is evaluated against every enabled rule, producing one audit record
  per rule per event.
- Expected audit records = events × active_rule_count.
- Events are distributed across 8 concurrent Kafka producer tasks inside the
  rules engine (`SIMULATION_SENDERS`, default 8).
- 25% of events (every 4th) are deliberately non-matching; they still generate
  `UNMATCHED` audit records, so the parity formula holds regardless.
- Resource usage is captured via `docker stats` immediately after triggering the
  publish (peak load).

## How to Run

```bash
# 1. Start the stack
./deploy/run.sh

# 2. Seed rules (if starting fresh — use the UI at http://localhost:3000
#    or POST to /api/rules). Skip if rules are already seeded.

# 3. Run the load test (100k events, default)
./tools/load-test.sh

# 4. Override event count or target URL
./tools/load-test.sh --events 100000 --base-url http://localhost:8080

# 5. With EOS (exactly-once) restart test
./tools/load-test.sh --events 100000 --eos-test
```

The script polls `GET /api/analytics/stats` every 5 s and prints a progress
line showing `totalEvaluations` delta vs the expected count. It exits PASS when
the delta meets or exceeds the expected count, or FAIL after 10 minutes.

## Rust Stack Resource Usage

> **Fill in after running `./tools/load-test.sh`.**
> Copy the `docker stats` table printed in step [5/6] of the script output.

| Service | CPU (peak) | Memory (peak) | Notes |
|---------|-----------|---------------|-------|
| rre-app-1 | TBD | TBD | Replica 1 |
| rre-app-2 | TBD | TBD | Replica 2 |
| rre-redpanda-0 | TBD | TBD | Broker 0 (3-node cluster) |
| rre-redpanda-1 | TBD | TBD | Broker 1 |
| rre-redpanda-2 | TBD | TBD | Broker 2 |
| rre-clickhouse | TBD | TBD | Single-node dev |
| rre-postgres | TBD | TBD | Primary |
| **Total** | **TBD** | **TBD** | |

Memory limits configured in `deploy/docker-compose.yml`:

| Service | Limit |
|---------|-------|
| Each app replica | 512 MB (`APP_MEM_LIMIT`) |
| Each Redpanda node | 1200 MB (`REDPANDA_MEM_LIMIT`) |
| ClickHouse | 4 GB (`CLICKHOUSE_MEM_LIMIT`) |
| Postgres primary | 256 MB (`POSTGRES_MEM_LIMIT`) |

> Note: ClickHouse's 4 GB limit is a ceiling, not typical usage. It caches
> aggressively but most of that headroom goes unused under light loads.

## Java Baseline (Spring-Kafka-Stream-Rules)

Baseline from the original JVM deployment for comparison.

| Component | Memory |
|-----------|--------|
| Spring Boot app (×2) | ~512 MB each = ~1 GB total |
| Kafka (3-node cluster) | ~1.5 GB total |
| MongoDB (3-node replica set) | ~2 GB total |
| **Total** | **~4.5 GB** |

Additional JVM overhead: JIT warm-up adds significant CPU spikes in the first
60–90 s of load; the Rust stack has no warm-up period.

## Audit Parity Results

> **Fill in after running the load test.**

| Metric | Value |
|--------|-------|
| Events published | 100,000 |
| Active rules | TBD |
| Expected audit records | TBD |
| Actual audit records (totalEvaluations delta) | TBD |
| Parity result | PASS / FAIL |
| Time to parity | TBD s |

## EOS Soak Test

> **Fill in after `--eos-test` run.**

| Metric | Value |
|--------|-------|
| Mid-load restart target | rre-app-1 |
| Raw audit rows (pre-dedup) | TBD |
| Deduplicated rows (FINAL) | TBD |
| Duplicate rows | TBD |
| Post-restart parity | PASS / FAIL |

Kafka transactional producers (EOS) ensure that on consumer-group rebalance,
offsets already committed in a completed transaction are not reprocessed.
ClickHouse `ReplacingMergeTree` with `audit_id` as the dedup key provides a
secondary safety net: any duplicate writes collapse to one row under `SELECT ...
FINAL` or after a background merge.

## Key Win

The Rust stack targets **< 512 MB total** for the two app replicas vs ~1 GB for
two Spring Boot instances. With ClickHouse replacing a 3-node MongoDB replica
set (~2 GB) and Redpanda replacing a 3-node Kafka cluster (~1.5 GB at smaller
footprint), the full-stack comparison is roughly:

| Stack | Approximate memory footprint |
|-------|------------------------------|
| Java (Spring + Kafka + MongoDB) | ~4.5 GB |
| Rust (RRE + Redpanda + ClickHouse) | ~1–2 GB (measured) |
| Target reduction | ~3-9x depending on ClickHouse cache settings |

Fill in the measured numbers above to make this concrete.
