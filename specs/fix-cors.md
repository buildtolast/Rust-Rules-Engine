# Spec: harden CORS — read allowed origins from env, not Any

## Overview
Two files must change together. The `router()` function in `crates/web/src/lib.rs` currently
uses `CorsLayer::new().allow_origin(Any)`. It must be changed to accept an explicit list of
allowed origins read from the `ALLOWED_ORIGINS` environment variable.

## File 1: crates/web/src/lib.rs

### Changes
1. Remove the `Any` import from `tower_http::cors`.
2. Change `router(state: AppState)` to `router(state: AppState, allowed_origins: Vec<axum::http::HeaderValue>)`.
3. Build the CorsLayer using the provided list:
   - If `allowed_origins` is empty, fall back to `CorsLayer::permissive()` and log a warning with `tracing::warn!`.
   - Otherwise use `.allow_origin(tower_http::cors::AllowOrigin::list(allowed_origins))`.
4. Keep `.allow_methods(Any)` and `.allow_headers(Any)`.
5. Add the `Any` import only for methods/headers: `use tower_http::cors::{Any, AllowOrigin, CorsLayer};`

### Exact signature change
```rust
pub fn router(state: AppState, allowed_origins: Vec<axum::http::HeaderValue>) -> Router {
```

Output EXACTLY ONE fenced ```rust block with the complete file content.

---

## File 2: bin/rules-engine/src/main.rs

### Changes
In `main()`, before building `AppState`, add:
```rust
// ── CORS allowed origins ──────────────────────────────────────────────────
let allowed_origins: Vec<axum::http::HeaderValue> = std::env::var("ALLOWED_ORIGINS")
    .unwrap_or_default()
    .split(',')
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .filter_map(|s| s.parse().ok())
    .collect();
if allowed_origins.is_empty() {
    tracing::warn!("ALLOWED_ORIGINS not set — CORS is permissive (all origins allowed)");
}
```

Then change `web::router(state)` to `web::router(state, allowed_origins)`.

Output EXACTLY ONE fenced ```rust block with the complete file content.

## Constraints
- Output the two files as TWO separate fenced ```rust blocks, each starting with a line comment `// FILE: <path>`.
- Do NOT change any other logic.
- Do NOT remove the `axum` import in web/src/lib.rs — it is already used.
- `AllowOrigin` comes from `tower_http::cors::AllowOrigin`.
