# Spec: crates/sre/src/dashboard.rs

Generate EXACTLY ONE fenced Rust code block. No commentary outside it.
Output path: crates/sre/src/dashboard.rs

## Context

This file is part of the `sre` crate. The following items ALREADY EXIST in the crate —
DO NOT redefine them, only import them:

From `crate::` (lib.rs):
- `pub struct SreState { containers: Vec<ContainerStatus>, findings: VecDeque<Finding>, last_scan_at: Option<DateTime<Utc>>, llm_available: bool }`
- `pub struct ContainerStatus { name, id, running, started_at, health, last_checked_at, last_severity }`
- `pub struct Finding { severity: String, category: String, finding: String, proposed_fix: String }` (from analysis module)

Available dependencies (in Cargo.toml):
- `axum = { version = "0.8", features = ["tokio"] }`
- `tokio = { workspace = true }` with full features
- `serde_json = "1"`
- `tracing = "0.1"`

## What to implement

Replace the placeholder `serve` function with a real axum server.

### Public API (must match exactly — this signature exists in lib.rs):

```rust
pub async fn serve(
    state: Arc<RwLock<SreState>>,
    tx:    broadcast::Sender<Finding>,
    port:  u16,
)
```

### Routes:

| Path | Handler |
|------|---------|
| `GET /` | Returns simple HTML with a meta-refresh and JSON dump — no Tera needed |
| `GET /api/sre/status` | JSON: `{ containers: [...], llm_available: bool, last_scan_at: Option<String> }` |
| `GET /api/sre/findings` | JSON array of last 100 findings |
| `GET /api/sre/findings/stream` | SSE stream of new findings |
| `GET /health` | `{"ok": true}` |

### SSE handler:

```rust
async fn findings_sse(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>>
```

Use `axum::response::sse::{Event, Sse}` and `tokio_stream::wrappers::BroadcastStream`.
Map each `Finding` to `Event::default().data(serde_json::to_string(&f).unwrap_or_default())`.
Map `BroadcastStream` errors to `None` via `.filter_map`.

### AppState:

```rust
#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<SreState>>,
    tx:    broadcast::Sender<Finding>,
}
```

Use `axum::extract::State<AppState>` — no global statics.

## Critical constraints

- Import `use tokio_stream::wrappers::BroadcastStream;` — `tokio-stream` is a transitive dep via axum/tokio, but add it to Cargo.toml if needed. Actually use `tokio::sync::broadcast::Receiver` and `async_stream::stream!` is NOT available. Instead create the stream manually:
  ```rust
  let rx = state.tx.subscribe();
  let stream = BroadcastStream::new(rx)
      .filter_map(|r| async move { r.ok() })
      .map(|f| Ok::<_, Infallible>(Event::default().data(serde_json::to_string(&f).unwrap_or_default())));
  ```
  For `BroadcastStream` and `filter_map`/`map` on streams use `use futures_util::StreamExt;` and `use tokio_stream::wrappers::BroadcastStream;`.
  Add `tokio-stream = { version = "0.1", features = ["sync"] }` to Cargo.toml if not present (it's a transitive dep but must be declared directly).

- The `/` route returns a simple `Html<String>` with an inline HTML page — do NOT use Tera or file templates in the stub.

- Use `axum::Router::new()` with `.route(...)` for each path, `.with_state(app_state)`.

- Bind with `tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.expect(...)` then `axum::serve(listener, router).await.expect(...)`.

- The `serve` function is `pub async fn` — no `#[tokio::main]`, it runs inside an existing tokio runtime.

- Imports needed:
  ```rust
  use crate::{Finding, SreState};
  use axum::{
      extract::State,
      response::{Html, sse::{Event, Sse}},
      routing::get,
      Json, Router,
  };
  use futures_util::StreamExt;
  use std::{convert::Infallible, sync::Arc};
  use tokio::sync::{broadcast, RwLock};
  use tokio_stream::wrappers::BroadcastStream;
  ```

- No main function. No mod declarations. Only the items specified above.
