use crate::{Finding, SreState};
use axum::{
    extract::State,
    response::{
        sse::{Event, Sse},
        Html,
    },
    routing::get,
    Json, Router,
};
use futures_util::{Stream, StreamExt};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<SreState>>,
    tx:    broadcast::Sender<Finding>,
}

async fn index(State(app): State<AppState>) -> Html<String> {
    let st = app.state.read().await;
    let json_dump = serde_json::to_string_pretty(&*st).unwrap_or_default();
    drop(st);
    Html(format!(
        r#"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="30">
  <title>SRE Dashboard</title>
  <style>body{{font-family:monospace;padding:1rem}} pre{{background:#111;color:#0f0;padding:1rem;overflow:auto}}</style>
</head>
<body>
  <h1>SRE Dashboard</h1>
  <pre id="state">{json_dump}</pre>
  <script>
    const es = new EventSource('/api/sre/findings/stream');
    es.onmessage = e => document.getElementById('state').textContent = e.data;
  </script>
</body>
</html>"#
    ))
}

async fn status(State(app): State<AppState>) -> Json<serde_json::Value> {
    let st = app.state.read().await;
    Json(serde_json::json!({
        "containers":    st.containers,
        "llm_available": st.llm_available,
        "last_scan_at":  st.last_scan_at,
    }))
}

async fn findings(State(app): State<AppState>) -> Json<Vec<Finding>> {
    let st = app.state.read().await;
    Json(st.findings.iter().cloned().collect())
}

async fn findings_sse(
    State(app): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx     = app.tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(|r| async move { r.ok() })
        .map(|f| {
            Ok::<_, Infallible>(
                Event::default().data(serde_json::to_string(&f).unwrap_or_default()),
            )
        });
    Sse::new(stream)
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

pub async fn serve(
    state: Arc<RwLock<SreState>>,
    tx:    broadcast::Sender<Finding>,
    port:  u16,
) {
    let app_state = AppState { state, tx };

    let router = Router::new()
        .route("/", get(index))
        .route("/api/sre/status", get(status))
        .route("/api/sre/findings", get(findings))
        .route("/api/sre/findings/stream", get(findings_sse))
        .route("/health", get(health_check))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind dashboard port");

    tracing::info!("SRE dashboard listening on http://0.0.0.0:{port}");
    axum::serve(listener, router).await.expect("dashboard server error");
}
