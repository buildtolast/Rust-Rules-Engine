# Integration Tests — Service-Aware Skipping + UI Tab

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all integration tests skip gracefully when services are unavailable, and add an "Integration Tests" tab to the web UI that shows service readiness and streams live test output.

**Architecture:** A shared `probe` helper (in `crates/test-support`) provides TCP reachability checks used inside every `#[ignore]` test to early-return instead of failing. The web crate gains three new pieces: a `/api/integration/status` JSON endpoint (service probes), a `/api/integration/run` SSE endpoint (spawns `cargo test --workspace --include-ignored` and streams stdout), and a static HTML page at `/tests` with a live terminal UI.

**Tech Stack:** Rust / Tokio / Axum 0.7 (SSE via `axum::response::sse`), `tokio::process::Command`, existing `#[ignore]` test pattern, vanilla HTML/JS (no framework).

## Global Constraints

- All new Rust code: edition 2021, `cargo fmt` + `cargo clippy -- -D warnings` clean.
- No new external crate dependencies beyond what the workspace already uses, except `tokio-stream` for SSE (already transitively present — add as explicit dep in `crates/web/Cargo.toml` if needed).
- Integration tests keep `#[ignore]` attribute — they must NOT run in default `cargo test`.
- Service probe timeout: 1 second per service.
- Default service addresses match `deploy/docker-compose.yml`: Postgres `localhost:5432`, ClickHouse `localhost:8123`, Kafka/Redpanda `localhost:19092`.
- The `/api/integration/run` endpoint is a **dev tool** — gate it behind a compile-time feature flag `integration-runner` or an `ENABLE_TEST_RUNNER=1` env var so it cannot be accidentally exposed in production.
- axum SSE feature: add `features = ["macros"]` already present; SSE is in `axum::response::sse` — no new feature flag needed for axum 0.7.

---

### Task 1: Shared probe helpers (`crates/test-support`)

**What:** New workspace crate `crates/test-support` with async TCP/HTTP probe functions used by all integration tests to detect service availability and skip cleanly.

**Files:**
- Create: `crates/test-support/Cargo.toml`
- Create: `crates/test-support/src/lib.rs`
- Modify: `Cargo.toml` (add `test-support` to workspace members)

**Interfaces:**
- Produces:
  - `pub async fn probe_tcp(addr: &str) -> bool` — 1s timeout TCP connect
  - `pub async fn probe_postgres() -> bool` — probes `localhost:5432`
  - `pub async fn probe_clickhouse() -> bool` — probes `localhost:8123`
  - `pub async fn probe_kafka() -> bool` — probes `localhost:19092`
  - `pub macro_rules! skip_if_unavailable` — prints skip reason and returns from test if probe fails

- [ ] **Step 1: Add workspace member**

In `Cargo.toml`, add `"crates/test-support"` to the `members` array.

- [ ] **Step 2: Create `crates/test-support/Cargo.toml`**

```toml
[package]
name = "test-support"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
tokio = { workspace = true, features = ["net", "time"] }
```

- [ ] **Step 3: Create `crates/test-support/src/lib.rs`**

```rust
//! Test helpers shared across integration test crates.
//! Only compiled under `#[cfg(test)]` in downstream crates — keep it lean.

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Returns true if a TCP connection to `addr` succeeds within 1 second.
pub async fn probe_tcp(addr: &str) -> bool {
    timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

pub async fn probe_postgres() -> bool {
    let host = std::env::var("TEST_POSTGRES_ADDR")
        .unwrap_or_else(|_| "localhost:5432".into());
    probe_tcp(&host).await
}

pub async fn probe_clickhouse() -> bool {
    let host = std::env::var("TEST_CLICKHOUSE_ADDR")
        .unwrap_or_else(|_| "localhost:8123".into());
    probe_tcp(&host).await
}

pub async fn probe_kafka() -> bool {
    let host = std::env::var("TEST_KAFKA_ADDR")
        .unwrap_or_else(|_| "localhost:19092".into());
    probe_tcp(&host).await
}

/// Use at the top of an `#[ignore]` integration test.
/// If the probe returns false, prints a skip message and returns from the
/// calling function (test passes vacuously = "skipped").
///
/// Usage:
/// ```rust
/// #[tokio::test]
/// #[ignore = "requires live Postgres"]
/// async fn my_test() {
///     skip_if_unavailable!(probe_postgres(), "Postgres at localhost:5432");
///     // ... rest of test
/// }
/// ```
#[macro_export]
macro_rules! skip_if_unavailable {
    ($probe:expr, $label:expr) => {
        if !$probe.await {
            eprintln!("[SKIP] {} is not reachable — skipping integration test", $label);
            return;
        }
    };
}
```

- [ ] **Step 4: Verify it compiles**

```bash
cargo build -p test-support
```

Expected: compiles with zero warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/test-support/ Cargo.toml
git commit -m "feat(test): add test-support crate with service probe helpers"
```

---

### Task 2: Update existing integration tests to skip gracefully

**What:** Add `skip_if_unavailable!` to all existing `#[ignore]` integration tests so they pass vacuously (skip) instead of failing when services are down.

**Files:**
- Modify: `crates/store-postgres/tests/integration.rs`
- Modify: `crates/store-clickhouse/tests/integration.rs`
- Modify: `crates/sre/tests/store_integration.rs`
- Modify: `crates/pipeline/tests/rule_cache_integration.rs`
- Modify: `crates/web/src/tests.rs` (integration tier)

Add `test-support` as a `[dev-dependency]` to each crate's `Cargo.toml` that doesn't already have it.

- [ ] **Step 1: Add dev-dependency to affected crates**

In each of `crates/store-postgres/Cargo.toml`, `crates/store-clickhouse/Cargo.toml`, `crates/sre/Cargo.toml`, `crates/pipeline/Cargo.toml`, `crates/web/Cargo.toml`, add:

```toml
[dev-dependencies]
test-support = { path = "../test-support" }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

(Skip fields already present.)

- [ ] **Step 2: Update `crates/store-postgres/tests/integration.rs`**

Add at the top of the `crud_seed_and_notify` test body (first line after the function signature):

```rust
test_support::skip_if_unavailable!(test_support::probe_postgres(), "Postgres at localhost:5432");
```

Add `use test_support;` at the top of the file.

- [ ] **Step 3: Update `crates/store-clickhouse/tests/integration.rs`**

Add at the top of each `#[ignore]` test body:

```rust
test_support::skip_if_unavailable!(test_support::probe_clickhouse(), "ClickHouse at localhost:8123");
```

- [ ] **Step 4: Update `crates/sre/tests/store_integration.rs`**

Add at the top of each `#[ignore]` test body:

```rust
test_support::skip_if_unavailable!(test_support::probe_clickhouse(), "ClickHouse at localhost:8123");
```

- [ ] **Step 5: Update `crates/pipeline/tests/rule_cache_integration.rs`**

Add at the top of each `#[ignore]` test body:

```rust
test_support::skip_if_unavailable!(test_support::probe_postgres(), "Postgres at localhost:5432");
```

- [ ] **Step 6: Update `crates/web/src/tests.rs`**

Find all tests that check `integration_enabled()` and return early when false. Replace the `integration_enabled()` pattern with direct probes:

```rust
// At the top of each integration-gated test, replace:
//   if !integration_enabled() { return; }
// with:
test_support::skip_if_unavailable!(
    async { test_support::probe_postgres().await && test_support::probe_clickhouse().await && test_support::probe_kafka().await },
    "full stack (Postgres + ClickHouse + Kafka)"
);
```

- [ ] **Step 7: Verify — run ignored tests without services**

```bash
cargo test --workspace --include-ignored 2>&1 | grep -E "SKIP|ok|FAILED"
```

Expected: all previously-failing tests now print `[SKIP] ... is not reachable` and show as `ok` (vacuous pass). Zero `FAILED`.

- [ ] **Step 8: Commit**

```bash
git add crates/store-postgres/tests/ crates/store-clickhouse/tests/ \
        crates/sre/tests/ crates/pipeline/tests/ crates/web/src/tests.rs \
        crates/*/Cargo.toml
git commit -m "feat(test): skip integration tests gracefully when services unavailable"
```

---

### Task 3: `/api/integration/status` endpoint

**What:** JSON endpoint that probes all three services and returns their reachability + an overall `ready` flag. Used by the UI tab to decide whether to enable the "Run Tests" button.

**Files:**
- Create: `crates/web/src/routes/integration.rs`
- Modify: `crates/web/src/routes/mod.rs` (expose module)
- Modify: `crates/web/src/lib.rs` (add route)

**Interfaces:**
- Consumes: `test_support::{probe_postgres, probe_clickhouse, probe_kafka}` — but the web crate is not a test crate, so copy the probe logic directly (3 small async fns; don't import `test-support` in production code).
- Produces: `GET /api/integration/status` → `200 application/json`

Response shape:
```json
{
  "ready": true,
  "services": {
    "postgres":   { "ok": true,  "addr": "localhost:5432" },
    "clickhouse": { "ok": false, "addr": "localhost:8123" },
    "kafka":      { "ok": true,  "addr": "localhost:19092" }
  }
}
```

- [ ] **Step 1: Create `crates/web/src/routes/integration.rs`**

```rust
use axum::Json;
use serde::Serialize;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Serialize)]
pub struct IntegrationStatus {
    pub ready: bool,
    pub services: Services,
}

#[derive(Serialize)]
pub struct Services {
    pub postgres: ServiceCheck,
    pub clickhouse: ServiceCheck,
    pub kafka: ServiceCheck,
}

#[derive(Serialize)]
pub struct ServiceCheck {
    pub ok: bool,
    pub addr: String,
}

async fn tcp_ok(addr: &str) -> bool {
    timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

pub async fn status() -> Json<IntegrationStatus> {
    let pg_addr = std::env::var("TEST_POSTGRES_ADDR")
        .unwrap_or_else(|_| "localhost:5432".into());
    let ch_addr = std::env::var("TEST_CLICKHOUSE_ADDR")
        .unwrap_or_else(|_| "localhost:8123".into());
    let kf_addr = std::env::var("TEST_KAFKA_ADDR")
        .unwrap_or_else(|_| "localhost:19092".into());

    let (pg_ok, ch_ok, kf_ok) = tokio::join!(
        tcp_ok(&pg_addr),
        tcp_ok(&ch_addr),
        tcp_ok(&kf_addr),
    );

    Json(IntegrationStatus {
        ready: pg_ok && ch_ok && kf_ok,
        services: Services {
            postgres:   ServiceCheck { ok: pg_ok, addr: pg_addr },
            clickhouse: ServiceCheck { ok: ch_ok, addr: ch_addr },
            kafka:      ServiceCheck { ok: kf_ok, addr: kf_addr },
        },
    })
}
```

- [ ] **Step 2: Expose in `crates/web/src/routes/mod.rs`**

Add `pub mod integration;` to the module file.

- [ ] **Step 3: Wire route in `crates/web/src/lib.rs`**

```rust
.route("/api/integration/status", get(routes::integration::status))
```

Add alongside the existing `.route(...)` calls.

- [ ] **Step 4: Write unit test in the same file**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_returns_valid_json_shape() {
        let Json(s) = status().await;
        // Services may or may not be up in CI — just check shape
        assert!(!s.services.postgres.addr.is_empty());
        assert!(!s.services.clickhouse.addr.is_empty());
        assert!(!s.services.kafka.addr.is_empty());
        // ready is true iff all three are ok
        assert_eq!(s.ready, s.services.postgres.ok && s.services.clickhouse.ok && s.services.kafka.ok);
    }
}
```

- [ ] **Step 5: Verify**

```bash
cargo test -p web --lib integration
```

Expected: 1 test passes.

- [ ] **Step 6: Commit**

```bash
git add crates/web/src/routes/integration.rs crates/web/src/routes/mod.rs crates/web/src/lib.rs
git commit -m "feat(web): add /api/integration/status endpoint"
```

---

### Task 4: `/api/integration/run` SSE endpoint

**What:** POST endpoint that spawns `cargo test --workspace --include-ignored` as a subprocess and streams stdout/stderr line-by-line via Server-Sent Events. Gated by `ENABLE_TEST_RUNNER=1` env var — returns `403 Forbidden` when the env var is absent.

**Files:**
- Modify: `crates/web/src/routes/integration.rs` (add `run` handler)
- Modify: `crates/web/Cargo.toml` (add `tokio-stream` if not present)
- Modify: `crates/web/src/lib.rs` (add `POST /api/integration/run` route)

**Interfaces:**
- Consumes: `tokio::process::Command`, `axum::response::sse::{Event, KeepAlive, Sse}`, `tokio_stream::wrappers::LinesStream`
- Produces: `POST /api/integration/run` → SSE stream of `data: <line>\n\n`, final event `data: {"exit_code": N}\n\n`

- [ ] **Step 1: Add `tokio-stream` to `crates/web/Cargo.toml` if absent**

```toml
tokio-stream = "0.1"
```

- [ ] **Step 2: Add `run` handler to `crates/web/src/routes/integration.rs`**

```rust
use axum::{
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_stream::{wrappers::LinesStream, StreamExt};
use std::convert::Infallible;

pub async fn run() -> Response {
    if std::env::var("ENABLE_TEST_RUNNER").unwrap_or_default().is_empty() {
        return (
            StatusCode::FORBIDDEN,
            "Set ENABLE_TEST_RUNNER=1 to enable this endpoint",
        )
            .into_response();
    }

    let mut child = match Command::new("cargo")
        .args(["test", "--workspace", "--include-ignored", "--color", "never"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    // Merge stdout and stderr into one line stream
    let stdout_lines = LinesStream::new(BufReader::new(stdout).lines());
    let stderr_lines = LinesStream::new(BufReader::new(stderr).lines());
    let merged = tokio_stream::StreamExt::merge(stdout_lines, stderr_lines);

    let stream = async_stream::stream! {
        tokio::pin!(merged);
        while let Some(line) = merged.next().await {
            let text = line.unwrap_or_else(|e| format!("[read error: {e}]"));
            yield Ok::<Event, Infallible>(Event::default().data(text));
        }
        // Wait for process to exit and send final status event
        let exit = child.wait().await.ok();
        let code = exit.and_then(|s| s.code()).unwrap_or(-1);
        yield Ok::<Event, Infallible>(
            Event::default()
                .event("done")
                .data(format!(r#"{{"exit_code":{code}}}"#))
        );
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}
```

**Note:** Add `async-stream = "0.3"` to `crates/web/Cargo.toml`.

- [ ] **Step 3: Wire `POST /api/integration/run` in `crates/web/src/lib.rs`**

```rust
.route("/api/integration/run", axum::routing::post(routes::integration::run))
```

- [ ] **Step 4: Verify endpoint compiles**

```bash
cargo build -p web
```

Expected: zero errors.

- [ ] **Step 5: Test the gate manually**

```bash
# Should return 403:
curl -X POST http://localhost:8080/api/integration/run

# Should stream output:
ENABLE_TEST_RUNNER=1 cargo run -p rules-engine &
sleep 2
curl -N -X POST http://localhost:8080/api/integration/run
```

- [ ] **Step 6: Commit**

```bash
git add crates/web/src/routes/integration.rs crates/web/src/lib.rs crates/web/Cargo.toml
git commit -m "feat(web): add /api/integration/run SSE endpoint for live test streaming"
```

---

### Task 5: `/tests` UI tab

**What:** A self-contained HTML page at `GET /tests` served directly from the web crate. Shows per-service status indicators, a "Run Integration Tests" button (enabled only when all services are up), and a live terminal-style output pane that streams SSE from `/api/integration/run`.

**Files:**
- Modify: `crates/web/src/routes/integration.rs` (add `page` handler returning `Html`)
- Modify: `crates/web/src/lib.rs` (add `GET /tests` route)

**Interfaces:**
- Consumes: `GET /api/integration/status` (polled every 3s by page JS)
- Consumes: `POST /api/integration/run` (SSE, triggered by button)
- Produces: `GET /tests` → `text/html`

- [ ] **Step 1: Add `page` handler to `crates/web/src/routes/integration.rs`**

```rust
use axum::response::Html;

pub async fn page() -> Html<&'static str> {
    Html(TESTS_HTML)
}

const TESTS_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Integration Tests — Rules Engine</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:1.5rem}
h1{font-size:1.25rem;font-weight:700;color:#f8fafc;margin-bottom:.25rem}
.sub{font-size:.75rem;color:#64748b;margin-bottom:1.5rem}
.services{display:flex;gap:.75rem;flex-wrap:wrap;margin-bottom:1.5rem}
.svc{background:#1e293b;border:1px solid #334155;border-radius:.75rem;padding:.75rem 1rem;min-width:160px}
.svc-name{font-size:.75rem;font-weight:600;color:#cbd5e1;display:flex;align-items:center;gap:.4rem}
.dot{width:8px;height:8px;border-radius:50%;background:#475569;transition:background .3s}
.dot.ok{background:#22c55e}.dot.fail{background:#ef4444}
.svc-addr{font-size:.65rem;color:#475569;margin-top:.3rem}
.controls{display:flex;align-items:center;gap:1rem;margin-bottom:1rem}
button{padding:.5rem 1.25rem;border-radius:.5rem;border:none;font-size:.85rem;font-weight:600;cursor:pointer;background:#334155;color:#94a3b8;transition:background .2s}
button.ready{background:#0f4c81;color:#e2e8f0;cursor:pointer}
button.ready:hover{background:#1565c0}
button:disabled{opacity:.5;cursor:not-allowed}
.status-pill{font-size:.7rem;padding:.2rem .6rem;border-radius:999px;background:#1e293b;border:1px solid #334155;color:#475569}
.status-pill.running{border-color:#0f4c81;color:#60a5fa}
.status-pill.passed{border-color:#065f46;color:#34d399}
.status-pill.failed{border-color:#7f1d1d;color:#f87171}
.terminal{background:#020617;border:1px solid #1e293b;border-radius:.75rem;padding:1rem;font-family:'SF Mono',monospace;font-size:.75rem;line-height:1.6;height:500px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.terminal .ok{color:#34d399}.terminal .fail{color:#f87171}.terminal .ignore{color:#94a3b8}
.terminal .default{color:#cbd5e1}
a{color:#60a5fa;text-decoration:none;font-size:.75rem}
</style>
</head>
<body>
<h1>Integration Tests</h1>
<div class="sub">Service reachability is checked every 3 seconds. Run tests only when all services are green.</div>

<div class="services" id="services">
  <div class="svc" id="svc-postgres">
    <div class="svc-name"><span class="dot" id="dot-postgres"></span>Postgres</div>
    <div class="svc-addr" id="addr-postgres">—</div>
  </div>
  <div class="svc" id="svc-clickhouse">
    <div class="svc-name"><span class="dot" id="dot-clickhouse"></span>ClickHouse</div>
    <div class="svc-addr" id="addr-clickhouse">—</div>
  </div>
  <div class="svc" id="svc-kafka">
    <div class="svc-name"><span class="dot" id="dot-kafka"></span>Kafka / Redpanda</div>
    <div class="svc-addr" id="addr-kafka">—</div>
  </div>
</div>

<div class="controls">
  <button id="run-btn" disabled onclick="runTests()">Run Integration Tests</button>
  <span class="status-pill" id="status-pill">Checking services…</span>
  <a href="/health/ready" target="_blank">↗ /health/ready</a>
</div>

<div class="terminal" id="terminal"><span style="color:#475569">Output will appear here when tests run…</span></div>

<script>
let running = false;
let es = null;

function colorize(line) {
  if (/\.\.\. ok/.test(line)) return `<span class="ok">${esc(line)}</span>`;
  if (/FAILED|error\[|^error/.test(line)) return `<span class="fail">${esc(line)}</span>`;
  if (/ignored/.test(line)) return `<span class="ignore">${esc(line)}</span>`;
  return `<span class="default">${esc(line)}</span>`;
}
function esc(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function appendLine(line) {
  const t = document.getElementById('terminal');
  const div = document.createElement('div');
  div.innerHTML = colorize(line);
  t.appendChild(div);
  t.scrollTop = t.scrollHeight;
}

async function checkStatus() {
  try {
    const r = await fetch('/api/integration/status');
    const d = await r.json();
    ['postgres','clickhouse','kafka'].forEach(svc => {
      const info = d.services[svc];
      const dot = document.getElementById('dot-' + svc);
      const addr = document.getElementById('addr-' + svc);
      dot.className = 'dot ' + (info.ok ? 'ok' : 'fail');
      addr.textContent = info.addr;
    });
    const btn = document.getElementById('run-btn');
    const pill = document.getElementById('status-pill');
    if (!running) {
      btn.disabled = !d.ready;
      btn.className = d.ready ? 'ready' : '';
      pill.textContent = d.ready ? 'All services up — ready to test' : 'Waiting for services…';
      pill.className = 'status-pill' + (d.ready ? ' passed' : '');
    }
  } catch(e) { /* server may be starting up */ }
}

function runTests() {
  if (running) return;
  running = true;
  const btn = document.getElementById('run-btn');
  const pill = document.getElementById('status-pill');
  const term = document.getElementById('terminal');
  btn.disabled = true;
  pill.textContent = 'Running…';
  pill.className = 'status-pill running';
  term.innerHTML = '';

  // POST to start, then read SSE stream
  fetch('/api/integration/run', { method: 'POST' }).then(res => {
    if (!res.ok) {
      appendLine('[ERROR] ' + res.status + ': ' + res.statusText);
      appendLine('Tip: start the server with ENABLE_TEST_RUNNER=1');
      running = false;
      pill.textContent = 'Error';
      pill.className = 'status-pill failed';
      btn.disabled = false;
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    function pump() {
      reader.read().then(({ done, value }) => {
        if (done) {
          running = false;
          btn.disabled = false;
          return;
        }
        buf += decoder.decode(value, { stream: true });
        const events = buf.split('\n\n');
        buf = events.pop();
        events.forEach(block => {
          const dataLine = block.split('\n').find(l => l.startsWith('data:'));
          if (!dataLine) return;
          const data = dataLine.slice(5).trim();
          if (block.includes('event:done')) {
            try {
              const d = JSON.parse(data);
              const ok = d.exit_code === 0;
              pill.textContent = ok ? 'All tests passed' : `Tests failed (exit ${d.exit_code})`;
              pill.className = 'status-pill ' + (ok ? 'passed' : 'failed');
              running = false;
              btn.disabled = false;
            } catch(_) {}
          } else {
            appendLine(data);
          }
        });
        pump();
      });
    }
    pump();
  });
}

setInterval(checkStatus, 3000);
checkStatus();
</script>
</body>
</html>"#;
```

- [ ] **Step 2: Wire `GET /tests` in `crates/web/src/lib.rs`**

```rust
.route("/tests", get(routes::integration::page))
```

- [ ] **Step 3: Verify page serves**

```bash
cargo build -p web && echo "OK"
# With a running server:
curl -s http://localhost:8080/tests | grep '<title>'
```

Expected: `<title>Integration Tests — Rules Engine</title>`

- [ ] **Step 4: Write a unit test for the page handler**

In `crates/web/src/routes/integration.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn page_returns_html_with_expected_title() {
        let Html(body) = page().await;
        assert!(body.contains("Integration Tests"));
        assert!(body.contains("/api/integration/status"));
        assert!(body.contains("/api/integration/run"));
    }

    #[tokio::test]
    async fn status_shape_is_valid() {
        let Json(s) = status().await;
        assert!(!s.services.postgres.addr.is_empty());
        assert_eq!(s.ready, s.services.postgres.ok && s.services.clickhouse.ok && s.services.kafka.ok);
    }

    #[tokio::test]
    async fn run_returns_403_without_env_var() {
        // Ensure the env var is not set in this process
        std::env::remove_var("ENABLE_TEST_RUNNER");
        let resp = run().await;
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }
}
```

- [ ] **Step 5: Run all unit tests**

```bash
cargo test --lib -p web 2>&1 | grep -E "test |FAILED"
```

Expected: all pass, zero failed.

- [ ] **Step 6: Final coverage check**

```bash
cargo tarpaulin --lib --skip-clean 2>&1 | tail -5
```

Expected: coverage has increased from the previous baseline.

- [ ] **Step 7: Commit**

```bash
git add crates/web/src/routes/integration.rs crates/web/src/lib.rs
git commit -m "feat(web): add /tests UI tab with live service status and test runner"
```

---

## Self-Review

**Spec coverage:**
- ✓ Integration tests skip when services unavailable (`skip_if_unavailable!` macro)
- ✓ Switch: `ENABLE_TEST_RUNNER=1` env var gates the test runner endpoint
- ✓ UI tab: `/tests` page with service status indicators
- ✓ Run tests from UI: "Run Integration Tests" button → SSE stream
- ✓ Services not available → button disabled
- ✓ Live output stream with color coding

**Placeholder scan:** None found — all code blocks are complete and runnable.

**Type consistency:**
- `IntegrationStatus.services.postgres/clickhouse/kafka` → `ServiceCheck { ok: bool, addr: String }` — consistent across Task 3, Task 4, and the JS consumer.
- `status()` handler returns `Json<IntegrationStatus>` in both Task 3 test and Task 5 test.
- `run()` returns `Response` (impl `IntoResponse`) — consistent with Task 4 implementation and Task 5 test.

**Gap check:** The `async-stream` crate is needed in Task 4 for the `stream!` macro. Add `async-stream = "0.3"` to `crates/web/Cargo.toml` in Task 4 Step 1 alongside `tokio-stream`.
