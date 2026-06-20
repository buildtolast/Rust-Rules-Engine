# Infrastructure Reference

Two views of the same system: how it runs locally under Docker Compose, and how each
component maps to AWS managed services for production.

---

## Docker Compose (local)

```
┌─────────────────────────── rre_default network ──────────────────────────────┐
│                                                                               │
│  ┌─────────────────────────────────────────────────┐                         │
│  │  Redpanda cluster  RF=3                          │                         │
│  │  redpanda-0 :19092/:9644  (exposed to host)      │                         │
│  │  redpanda-1 :9092  (internal only)               │                         │
│  │  redpanda-2 :9092  (internal only)               │                         │
│  │                                                  │                         │
│  │  Topics (created by redpanda-init, one-shot):    │                         │
│  │    source-events   6 partitions  RF=3            │                         │
│  │    target-events   6 partitions  RF=3            │                         │
│  └─────────────────────────────────────────────────┘                         │
│           ↑ consume (EOS)     ↓ produce target-events                        │
│  ┌──────────────────────────────────────────┐                                │
│  │  rules-engine  ×2 replicas               │                                │
│  │    axum HTTP :8080                        │                                │
│  │    pipeline: rayon CEL eval → EOS txn    │                                │
│  │    CH batch writer (5000-row channel)    │                                │
│  │    LISTEN/NOTIFY hot-reload from PG      │                                │
│  │    OTEL spans → otel-collector:4317      │                                │
│  └──────────────────────────────────────────┘                                │
│      ↓ audit writes         ↓ rule reads                                     │
│  ┌──────────────────┐  ┌──────────────────────────────────────┐             │
│  │  ClickHouse      │  │  PostgreSQL                           │             │
│  │  :8123 / :9000   │  │  primary :5432  +  streaming replica │             │
│  │  audits (RMT)    │  │  rules table  +  NOTIFY trigger       │             │
│  │  agg MVs         │  │  manual failover: pg_promote()        │             │
│  │  sre_observations│  └──────────────────────────────────────┘             │
│  │  sre_outages     │                                                        │
│  └──────────────────┘                                                        │
│           ↑ write findings                                                   │
│  ┌──────────────────────────────────────────┐                                │
│  │  sre-agent  ×2 replicas  :8088           │                                │
│  │    bollard → /var/run/docker.sock        │                                │
│  │    tail container logs → local LLM       │──→ host.docker.internal:8888  │
│  │    write sre_observations to CH          │    (Unsloth Studio, not       │
│  │    OTEL spans → otel-collector:4317      │     containerised)             │
│  └──────────────────────────────────────────┘                                │
│                                                                               │
│  ┌──────────────────────────────────────────┐                                │
│  │  frontend  (nginx :80)                   │                                │
│  │    React SPA                             │                                │
│  │    /api/* → app:8080                     │                                │
│  │    /sre/* → sre-agent:8088               │                                │
│  │    host :3000                            │                                │
│  └──────────────────────────────────────────┘                                │
│                                                                               │
│  ── --obs overlay (optional) ────────────────────────────────────────────── │
│  │  otel-collector   :4317 gRPC / :4318 HTTP                                │
│  │    → signoz-otelcollector  → signoz-clickhouse                           │
│  │  signoz UI  :3301  (host-exposed)                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Container inventory

| Container | Image | Host ports | Role |
|---|---|---|---|
| rre-redpanda-0 | redpandadata/redpanda:v24.2.7 | 19092, 9644 | Kafka broker 0 (seed) |
| rre-redpanda-1 | redpandadata/redpanda:v24.2.7 | — | Kafka broker 1 |
| rre-redpanda-2 | redpandadata/redpanda:v24.2.7 | — | Kafka broker 2 |
| rre-redpanda-init | redpandadata/redpanda:v24.2.7 | — | One-shot topic creator |
| rre-clickhouse | clickhouse/clickhouse-server:24.8 | 8123, 9000 | Audit store + analytics |
| rre-postgres | postgres:16 | 5432 | Rules store (primary) |
| rre-postgres-replica | postgres:16 | — | Streaming hot standby |
| rre-app-1, rre-app-2 | rre/rules-engine:latest | — | Rules Engine (behind nginx) |
| rre-sre-agent-1, rre-sre-agent-2 | rre/sre-agent:latest | — | SRE monitor + LLM |
| rre-frontend | rre/frontend:latest | 3000 | React SPA via nginx |
| rre-otel-collector | otel/opentelemetry-collector-contrib:0.108.0 | 4317, 4318 | OTEL receiver (`--obs`) |
| rre-signoz-otelcollector | signoz/signoz-otel-collector:v0.144.5 | — | SigNoz collector (`--obs`) |
| rre-signoz-clickhouse | clickhouse/clickhouse-server:24.1.2-alpine | — | SigNoz storage (`--obs`) |
| rre-signoz | signoz/signoz:v0.129.0 | 3301 | SigNoz UI (`--obs`) |

### Memory limits (defaults)

| Component | Default limit |
|---|---|
| Each Redpanda broker | 1200 MB |
| ClickHouse | 4 GB |
| PostgreSQL primary + replica | 256 MB each |
| rules-engine (per replica) | 512 MB |
| sre-agent (per replica) | 256 MB |
| frontend | 64 MB |

Override with env vars: `REDPANDA_MEM_LIMIT`, `CLICKHOUSE_MEM_LIMIT`, `POSTGRES_MEM_LIMIT`,
`APP_MEM_LIMIT`, `SRE_MEM_LIMIT`, `FRONTEND_MEM_LIMIT`.

---

## AWS Deployment Mapping

Each local component maps directly to an AWS managed service. The application code
requires no changes — only environment variables and IAM wiring.

```
┌──────────────────────────── AWS VPC ─────────────────────────────────────────┐
│                                                                               │
│  AZ-a                                     AZ-b                               │
│  ┌─────────────────────────────┐  ┌────────────────────────────────────┐     │
│  │ Amazon MSK (or Redpanda     │  │ MSK broker replicas                │     │
│  │ Cloud)  broker-a :9096      │◀─│ RF=3 across AZs — managed          │     │
│  │ source-events/target-events │  │                                    │     │
│  └─────────────────────────────┘  └────────────────────────────────────┘     │
│           ↑ consume (EOS)  ↓ produce                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  ECS Fargate  rules-engine service                                   │    │
│  │    task-1  task-2 … task-N  (each gets unique TRANSACTIONAL_ID)     │    │
│  │    auto-scales on consumer lag (CloudWatch metric → Application ASG) │    │
│  │    max tasks = partition count (Kafka ceiling)                       │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│      ↓ audit writes                  ↓ rule reads / LISTEN                  │
│  ┌────────────────────────┐  ┌──────────────────────────────────────────┐   │
│  │ ClickHouse Cloud       │  │ RDS PostgreSQL  Multi-AZ                  │   │
│  │ (or EC2 self-managed)  │  │   primary AZ-a → standby AZ-b            │   │
│  │ private HTTPS :8443    │  │   synchronous replica, auto-promoted      │   │
│  │ audits / MVs / SRE     │  │   rules table + NOTIFY trigger            │   │
│  │ read replica in AZ-b   │  │   :5432  private subnet                   │   │
│  └────────────────────────┘  └──────────────────────────────────────────┘   │
│           ↑ write findings                                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  ECS Fargate  sre-agent service                                      │    │
│  │    replaces bollard with: ECS list-tasks API + CloudWatch Logs       │    │
│  │    replaces Unsloth with: Amazon Bedrock (or SageMaker endpoint)     │    │
│  │    writes sre_observations → ClickHouse (same as local)              │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│                                                                               │
│  ┌──────────────────────┐    ┌─────────────────────────────────────────┐     │
│  │  ALB  (internal)     │    │  CloudFront + S3                         │     │
│  │  /api/* → rules-eng  │    │  React SPA (static, CDN-served)          │     │
│  │  /sre/* → sre-agent  │    │  or ECS Fargate nginx (same as local)    │     │
│  └──────────────────────┘    └─────────────────────────────────────────┘     │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Observability                                                        │    │
│  │    AWS X-Ray  — replace SigNoz for distributed traces                │    │
│  │    CloudWatch — metrics, log groups, consumer-lag alarm → ASG        │    │
│  │    or: self-host SigNoz on ECS (same compose overlay, different env) │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────────┘
```

### Local → AWS component map

| Local | AWS equivalent | Notes |
|---|---|---|
| Redpanda 3-node | Amazon MSK or Redpanda Cloud | Same Kafka API; EOS config unchanged |
| ClickHouse (single) | ClickHouse Cloud | Or `clickhouse/clickhouse-server` on EC2 behind EBS |
| Postgres + streaming replica | RDS PostgreSQL Multi-AZ | Managed failover; remove `replica-entrypoint.sh` |
| rules-engine ×2 | ECS Fargate service, task-count=2+ | One ECS task = one Docker container; `TRANSACTIONAL_ID` must be unique per task |
| sre-agent (bollard) | ECS Fargate + ECS API + CW Logs | `bollard` uses the Docker socket; on ECS replace with `aws ecs list-tasks` + `aws logs get-log-events` |
| Unsloth Studio (:8888) | Amazon Bedrock or SageMaker | SRE agent only needs an OpenAI-compat endpoint; set `LLM_BASE_URL` |
| nginx frontend | CloudFront + S3 (static) | `npm run build` → S3 bucket → CloudFront distribution |
| SigNoz (`--obs`) | AWS X-Ray + CloudWatch | Change `OTEL_EXPORTER_OTLP_ENDPOINT` to the X-Ray OTEL endpoint; or keep SigNoz on a Fargate task |
| Docker volumes | EBS volumes (ECS task storage) | Redpanda and ClickHouse need persistent EBS; Postgres uses RDS managed storage |

### SRE agent adaptation for AWS

The local SRE agent uses `bollard` to read the Docker daemon socket directly.
On AWS there is no Docker daemon to connect to. Two changes are needed:

1. **Container discovery:** replace `bollard::Docker::connect_with_local_defaults()` with
   `aws_sdk_ecs::Client::list_tasks()` + `describe_tasks()`.
2. **Log tailing:** replace `docker logs --tail N` with
   `aws_sdk_cloudwatchlogs::Client::get_log_events()` using the task's log group/stream.

The rest of the SRE agent (LLM call, ClickHouse write, axum API) is unchanged.

### Scaling rules

The pipeline's parallelism ceiling is `partition_count`. The ECS service target tracking
should cap `maxCapacity` at the number of partitions (currently 6 per topic) to avoid
idle tasks that hold no Kafka partitions.

```
CloudWatch Alarm:
  Metric:      custom/rules-engine/consumer_lag
  Source:      /api/metrics endpoint, scraped by a Lambda or CW agent
  Threshold:   lag > 10000 for 60s → scale out (+1 task)
               lag < 1000  for 120s → scale in  (-1 task)
  Hard ceiling: desired_count ≤ partition_count (= 6)
```
