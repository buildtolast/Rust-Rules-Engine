# Blueprint — S0 Bootstrap (detailed implementation)

> Expansion of **S0** from [`rust-rules-engine-rebuild.md`](./rust-rules-engine-rebuild.md).
> This is the **first implementation step**: stand up the Cargo workspace, local
> infra (Redpanda + ClickHouse + Postgres), CI, toolchain pins, and the GitHub
> remote. **No business logic.** Every sub-step below is cold-start executable.
>
> **Parent step:** S0 · model: default · deps: none · on critical path
> (S0 → S1 → S3 → S6 → S10 → S11).

---

## Environment as verified (2026-06-19)

| Fact | Value | Consequence for this step |
|---|---|---|
| Repo | `/Users/chiya/GIT/Rust-Rules-Engine`, branch `master`, **0 commits** | First commit goes straight to `master`; rename to `main` after. |
| Rust | `cargo 1.94.0`, `rustc 1.94.0` (Homebrew) | Meets the 1.94+ pin. |
| Docker | `29.4.0`, Compose `v5.1.2` (`docker compose`, V2) | Use `docker compose` (space), not `docker-compose`. |
| `gh` | authed as **`buildtolast`**; git user is **arnab / arnab@codrite.com** | Remote = **`buildtolast/Rust-Rules-Engine`, public** (resolved). |
| Host CLIs | `rpk`, `clickhouse-client`, `psql` **NOT installed**; `nc` present | **All DB/broker checks run via `docker compose exec`**, not host clients. This overrides the parent plan's verification commands. |

---

## Sub-step dependency order

```
S0.1 workspace skeleton ─┐
S0.7 toolchain pins ─────┤ (independent; do early so fmt/clippy gate works)
                         ├─> S0.2 rename master→main
S0.3 docker-compose ─────┤
S0.4 run.sh (needs S0.3) ┘
S0.5 CI (needs S0.1+S0.7) ─> S0.6 remote + first PR (needs all; user-gated)
```

Serial-enough that one agent does it top to bottom. The only human gate is **S0.6**.

---

## S0.1 — Cargo workspace skeleton

**Context brief.** Empty repo. Create a virtual workspace whose members are the
six crates + one binary named in the parent plan's layout. Each member must
compile as an empty stub so the verification gate is meaningful from day one.

**Files to create.**

Root `Cargo.toml` (virtual manifest — no `[package]`):
```toml
[workspace]
resolver = "2"
members = [
    "crates/core",
    "crates/eval",
    "crates/store-clickhouse",
    "crates/store-postgres",
    "crates/pipeline",
    "crates/web",
    "bin/rules-engine",
]

[workspace.package]
edition = "2021"
rust-version = "1.94"
license = "MIT"

[workspace.dependencies]
# Populated by later steps (serde, tokio, axum, rdkafka, sqlx, clickhouse, cel-interpreter…).
# Kept central so versions are pinned once. Empty is fine at S0.
```

Each library crate — `crates/<name>/Cargo.toml`:
```toml
[package]
name = "<name>"            # core, eval, store-clickhouse, store-postgres, pipeline, web
edition.workspace = true
rust-version.workspace = true
version = "0.0.0"
publish = false

[dependencies]
```
with `crates/<name>/src/lib.rs`:
```rust
//! <name> crate — see plans/rust-rules-engine-rebuild.md.
```

Binary — `bin/rules-engine/Cargo.toml`:
```toml
[package]
name = "rules-engine"
edition.workspace = true
rust-version.workspace = true
version = "0.0.0"
publish = false

[dependencies]
```
with `bin/rules-engine/src/main.rs`:
```rust
fn main() {
    println!("rules-engine: bootstrap stub");
}
```

`.gitignore`:
```
/target
**/*.rs.bk
.env
```

**Crate names:** `store-clickhouse`/`store-postgres` are the package names; their
import path becomes `store_clickhouse` / `store_postgres` (Cargo normalizes `-`→`_`).

**Verification.** `cargo build --workspace` succeeds; `cargo metadata --format-version 1 | python3 -c "import sys,json;print(len(json.load(sys.stdin)['workspace_members']))"` prints `7`.

**Exit.** Seven members compile from clean.

---

## S0.7 — Toolchain & lint pins

**Context brief.** Pin the toolchain and lint config so CI and every dev see the
same `fmt`/`clippy` behavior. Do this early — S0.5 CI and the parent's
verification gate depend on it.

**Files.**

`rust-toolchain.toml`:
```toml
[toolchain]
channel = "1.94"
components = ["rustfmt", "clippy"]
profile = "minimal"
```
> Note: host Rust is Homebrew 1.94.0, which is **not** rustup-managed. If `rustup`
> is absent this file is inert locally (Homebrew toolchain is used as-is) but still
> governs the CI runner. Do not block on installing rustup; record it as a known gap.

`rustfmt.toml`:
```toml
edition = "2021"
max_width = 100
```

Clippy: enforced via the gate flag `-D warnings` (parent convention). Optionally add
a `[workspace.lints]` block later; not required at S0.

**Verification.** `cargo fmt --check` exits 0; `cargo clippy --all-targets -- -D warnings` exits 0 on the stubs.

**Exit.** Formatting and lint gates pass on the skeleton.

---

## S0.2 — Rename default branch master → main

**Context brief.** Repo is on `master` with no commits. Parent plan & repo
convention target `main`. Rename before the first push so the remote's default is
`main`.

**Tasks.**
1. After S0.1/S0.7 files exist, make the first commit on `master`, then
   `git branch -m master main`. (Renaming an unborn branch with `-m` also works
   pre-commit, but committing first gives a clean initial history.)

**Commands.**
```bash
git add -A
git commit -m "chore: bootstrap cargo workspace, infra, CI"
git branch -m master main
```

**Verification.** `git branch --show-current` → `main`.

**Exit.** Local default branch is `main`.

---

## S0.3 — Local infra via docker-compose

**Context brief.** Replace the Java stack's `kafka + mongo + redis` with the
lightweight set: **Redpanda** (Kafka API), **ClickHouse**, **Postgres**. Single
node each, modest memory limits, host-reachable, healthchecked. **No app service
yet** (added in S10). Mirror the Java compose's env-driven port pattern so `run.sh`
can auto-increment. Auto-create-topics stays **off** (Java set
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`); topic creation is explicit in S0.4.

**File:** `deploy/docker-compose.yml`
```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.2.7
    container_name: rre-redpanda
    command:
      - redpanda start
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:${REDPANDA_PORT:-19092}
      - --advertise-kafka-addr=internal://redpanda:9092,external://host.docker.internal:${REDPANDA_PORT:-19092}
      - --rpc-addr=0.0.0.0:33145
      - --advertise-rpc-addr=redpanda:33145
      - --mode=dev-container        # single-node, KRaft, no replication asserts
      - --smp=1
      - --memory=1G
      - --reserve-memory=0M
      - --overprovisioned
    ports:
      - "${REDPANDA_PORT:-19092}:${REDPANDA_PORT:-19092}"   # Kafka API (host)
      - "${REDPANDA_ADMIN_PORT:-9644}:9644"                  # admin
    extra_hosts:
      - "host.docker.internal:host-gateway"
    deploy:
      resources:
        limits:
          memory: ${REDPANDA_MEM_LIMIT:-1500M}
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster info --brokers localhost:9092 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 12

  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    container_name: rre-clickhouse
    ports:
      - "${CLICKHOUSE_HTTP_PORT:-8123}:8123"   # HTTP
      - "${CLICKHOUSE_TCP_PORT:-9000}:9000"    # native
    ulimits:
      nofile: { soft: 262144, hard: 262144 }
    environment:
      CLICKHOUSE_DB: ruleaudit
      CLICKHOUSE_USER: rules
      CLICKHOUSE_PASSWORD: rules
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - rre-clickhouse-data:/var/lib/clickhouse
    deploy:
      resources:
        limits:
          memory: ${CLICKHOUSE_MEM_LIMIT:-2G}
    healthcheck:
      test: ["CMD-SHELL", "clickhouse-client --query 'SELECT 1' || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 12

  postgres:
    image: postgres:16
    container_name: rre-postgres
    command: ["postgres", "-p", "${POSTGRES_PORT:-5432}"]
    ports:
      - "${POSTGRES_PORT:-5432}:${POSTGRES_PORT:-5432}"
    environment:
      POSTGRES_DB: ruleaudit
      POSTGRES_USER: rules
      POSTGRES_PASSWORD: rules
    volumes:
      - rre-postgres-data:/var/lib/postgresql/data
    deploy:
      resources:
        limits:
          memory: ${POSTGRES_MEM_LIMIT:-512M}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -p ${POSTGRES_PORT:-5432} -U rules || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 12

volumes:
  rre-clickhouse-data:
  rre-postgres-data:
```

**Design notes / rationale.**
- **Redpanda over apache/kafka:** the parent's locked decision; drops the JVM
  broker. `mode=dev-container` skips multi-node assertions for a 1-node dev box.
- **External Kafka port `19092`, not `9092`:** Redpanda's *internal* listener uses
  `9092` and ClickHouse's *native* port is also `9092` on its own container — but
  to avoid host-side confusion the Kafka API is published on `19092`. (ClickHouse
  native `9000` and Kafka `19092` don't collide on the host.)
- **Credentials `rules/rules`, db `ruleaudit`:** dev-only, match the Java db name
  `ruleaudit`. Real secrets via env in S10; never hardcode beyond dev.
- **No app/ui service:** added in S10; S0 is infra-only.

**Verification (host-CLI-free — runs through the containers):**
```bash
docker compose -f deploy/docker-compose.yml up -d
# wait for healthy, then:
docker compose -f deploy/docker-compose.yml exec -T redpanda rpk cluster info
docker compose -f deploy/docker-compose.yml exec -T clickhouse clickhouse-client --query "SELECT 1"
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U rules -d ruleaudit -p ${POSTGRES_PORT:-5432} -c "select 1"
docker compose -f deploy/docker-compose.yml down
```

**Exit.** All three services reach `healthy`; the three exec probes succeed.

---

## S0.4 — `deploy/run.sh`

**Context brief.** Port the Java `run.sh` ergonomics (port auto-increment,
teardown-first, health-wait) to the 3-service infra stack. Because the app binary
doesn't exist yet, S0's `run.sh` brings up **infra only** and waits on container
health (not an `/api/health` endpoint — that arrives in S8/S10). Keep the same
flag surface (`-h`, `-l`, clean-teardown-first) so muscle memory transfers.

**Behavior spec.**
1. Verify Docker is running (`docker info`); detect `docker compose` V2 (it is here).
2. **Teardown first:** `docker compose -f deploy/docker-compose.yml down --remove-orphans`
   *before* probing ports — same rationale as the Java script (don't misdetect our
   own stale containers as external).
3. Port resolution via a `find_free_port` helper (lift the Java
   `is_port_open`/`port_taken`/`find_free_port` functions verbatim — they're
   self-contained) for: `REDPANDA_PORT` (19092), `REDPANDA_ADMIN_PORT` (9644),
   `CLICKHOUSE_HTTP_PORT` (8123), `CLICKHOUSE_TCP_PORT` (9000),
   `POSTGRES_PORT` (5432). Export each resolved value.
4. `docker compose -f deploy/docker-compose.yml up -d`.
5. Health-wait: poll `docker compose ... ps --format json` (or
   `docker inspect --format '{{.State.Health.Status}}'` per container) until all
   three report `healthy` or a 60-attempt/2s timeout elapses.
6. Print the connection matrix (Kafka bootstrap `localhost:$REDPANDA_PORT`,
   CH HTTP `localhost:$CLICKHOUSE_HTTP_PORT`, PG DSN
   `postgres://rules:rules@localhost:$POSTGRES_PORT/ruleaudit`).
7. Flags: `-h/--help`, `-l/--logs` (follow logs after start), `-d/--down`
   (teardown and exit). Default action = up + health-wait.

**Anti-patterns to avoid (carried from the Java script's hard-won lessons).**
- Do **not** probe ports before teardown.
- Do **not** `set -e` around the health-poll loop (a failed `curl`/`inspect` is
  expected mid-wait); scope `set -e` to setup only or guard the loop.
- Make it idempotent: re-running cleanly tears down and re-creates.

**Verification.** `bash deploy/run.sh` brings all three healthy and prints the
matrix; `bash deploy/run.sh --down` tears down cleanly; `shellcheck deploy/run.sh`
is clean (if `shellcheck` available — otherwise note as skipped).

**Exit.** One command stands up infra and reports healthy; teardown is clean.

---

## S0.5 — CI (GitHub Actions)

**Context brief.** Run the parent's verification gate on every PR. Rust-only at
S0 (infra integration tests are `--ignored` and land in S3+). Cache cargo for
speed.

**File:** `.github/workflows/ci.yml`
```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.94
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test --workspace
```

**Notes.**
- `--ignored` integration tests (needing live infra) are excluded by default — CI
  stays fast and hermetic until a later step adds a service-container job.
- `dtolnay/rust-toolchain@1.94` matches `rust-toolchain.toml`.

**Verification.** Workflow file is valid YAML; the three gate commands pass locally
(they will on the CI runner too).

**Exit.** CI defined; green on the bootstrap PR.

---

## S0.6 — GitHub remote + first PR  ·  **USER-GATED**

**Context brief.** Create the remote and open the bootstrap PR. **Confirm with the
user before creating the remote** (parent convention) — and resolve the account
ambiguity first.

**Decision — RESOLVED (2026-06-19, by user).**
- **Owner:** `buildtolast`. **Visibility:** **public**.
- Note: git committer is `arnab/arnab@codrite.com` while the repo owner is
  `buildtolast` — commits will attribute to arnab under the buildtolast repo.
  Left as-is unless the user later asks to align the committer identity.
- **Public-repo caution:** confirm nothing secret is committed before the first
  push. The dev creds in `docker-compose.yml` are throwaway local values
  (`rules/rules`), acceptable to publish; do not add real secrets.

**Tasks.**
1. `gh repo create buildtolast/Rust-Rules-Engine --public --source=. --remote=origin --push` — pushes `main`.
2. Branch the actual bootstrap work so CI has a PR to run against:
   `git switch -c step-00-bootstrap` (if not already committed there), open a PR
   with `gh pr create`, let CI run, merge after green.
   - If the bootstrap commit already sits on `main`, instead push a trivial
     follow-up PR (e.g., add a `README.md`) so CI demonstrably runs on a PR, per
     the parent's "CI green on an initial PR" exit criterion.
3. Set `main` as the default branch (it already is locally; confirm on remote).

**Verification.** `gh repo view` shows the remote; `gh pr checks` on the bootstrap
PR is green; `git remote -v` shows `origin`.

**Exit.** Remote exists, default branch `main`, CI green on a PR.

---

## Step-level rollback

- **Pre-remote (S0.1–S0.5, S0.7):** all changes are local files + one commit on a
  fresh repo. Rollback = `git reset` / delete files; no external state except
  Docker volumes (`docker compose -f deploy/docker-compose.yml down -v`).
- **Post-remote (S0.6):** revert the bootstrap PR; the remote can be deleted with
  `gh repo delete` if created in error (irreversible — confirm with user).

## Invariants (parent plan, checked at S0 exit)

- [ ] `cargo build --workspace` succeeds; 7 members.
- [ ] `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` clean.
- [ ] `core`/`eval` have **no** I/O deps (trivially true — empty stubs).
- [ ] Infra: redpanda + clickhouse + postgres all `healthy`; exec probes pass.
- [ ] No topic/audit-field contract introduced yet (deferred to S1/S3) — N/A at S0.

## Open items handed to later steps

- **Topic creation** (`source-events`, `target-events`, `audit-events`,
  `audit-events.DLT`): not created in S0 (`auto-create` is off). Add an explicit
  `rpk topic create` block to `run.sh` or a startup migration in **S6/S10**.
- **`/api/health/status`-based health-wait** in `run.sh`: app endpoint arrives in
  **S8**; S0 waits on container health instead.
- **rustup absence**: `rust-toolchain.toml` is inert against the Homebrew
  toolchain locally; governs CI only. Revisit if a contributor needs channel
  switching.

## Status

- **S0 COMPLETE (2026-06-19).** Workspace (7 members) builds; fmt/clippy/test gate
  green; infra (Redpanda + ClickHouse + Postgres) verified healthy with exec
  probes; CI green on PR #1; remote live at
  `https://github.com/buildtolast/Rust-Rules-Engine` (public), default branch
  `main`. Next: **S1 — core domain types**.

## Changelog

- 2026-06-19 — S0 expanded from the parent blueprint. Adjustments vs parent:
  (1) verification routed through `docker compose exec` because host lacks
  `rpk`/`clickhouse-client`/`psql`; (2) Kafka API published on `19092` to avoid
  host confusion with ClickHouse native `9000`; (3) `run.sh` waits on container
  health (no app endpoint yet); (4) flagged `gh` account (`buildtolast`) vs git
  user (`arnab`) mismatch as a pre-remote decision.
