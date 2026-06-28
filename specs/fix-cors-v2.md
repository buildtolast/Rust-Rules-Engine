# Spec: harden CORS in two files

You MUST apply ALL changes described below. Do not output the original files unchanged.

---

## File 1: crates/web/src/lib.rs

Apply these EXACT changes to the file content shown below:

CHANGE 1 — replace the import line:
  OLD: `use tower_http::cors::{Any, CorsLayer};`
  NEW: `use tower_http::cors::{AllowOrigin, Any, CorsLayer};`

CHANGE 2 — replace the function signature:
  OLD: `pub fn router(state: AppState) -> Router {`
  NEW: `pub fn router(state: AppState, allowed_origins: Vec<axum::http::HeaderValue>) -> Router {`

CHANGE 3 — replace the CORS layer construction:
  OLD:
    ```
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    ```
  NEW:
    ```
    let cors = if allowed_origins.is_empty() {
        tracing::warn!("ALLOWED_ORIGINS not set — CORS is permissive (all origins allowed)");
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allowed_origins))
            .allow_methods(Any)
            .allow_headers(Any)
    };
    ```

Output EXACTLY ONE fenced ```rust block with the complete modified file.

---

## File 2: bin/rules-engine/src/main.rs

Apply these EXACT changes:

CHANGE 1 — insert BEFORE the line `let state = web::AppState {`, add:
    ```
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

CHANGE 2 — replace:
  OLD: `let app = web::router(state);`
  NEW: `let app = web::router(state, allowed_origins);`

Output EXACTLY ONE fenced ```rust block with the complete modified file.

---

## Current file contents
