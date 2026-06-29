use crate::{
    analysis::AnalysisClient,
    store::{Incident, SreStore},
    trace_analysis, Finding, SreState,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html,
    },
    routing::get,
    Json, Router,
};
use clickhouse::Client;
use futures_util::{Stream, StreamExt};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone)]
struct AppState {
    state: Arc<RwLock<SreState>>,
    tx: broadcast::Sender<Finding>,
    ch: Client,
    llm: AnalysisClient,
}

async fn index(State(app): State<AppState>) -> Html<&'static str> {
    let _ = app; // state is loaded by the page via /api/sre/* endpoints
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SRE Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:1.5rem}
h1{font-size:1.25rem;font-weight:700;color:#f8fafc;margin-bottom:0.25rem}
.subtitle{font-size:0.75rem;color:#64748b;margin-bottom:1.5rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:0.75rem;margin-bottom:1.5rem}
.card{background:#1e293b;border:1px solid #334155;border-radius:0.75rem;padding:0.875rem}
.card-name{font-size:0.8rem;font-weight:600;color:#cbd5e1;display:flex;align-items:center;gap:0.4rem;margin-bottom:0.5rem}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.dot-green{background:#22c55e}.dot-red{background:#ef4444}.dot-gray{background:#475569}
.badge{display:inline-block;font-size:0.65rem;font-weight:700;padding:0.15rem 0.5rem;border-radius:999px;margin-top:0.25rem}
.b-info{background:#022c22;color:#34d399;border:1px solid #065f46}
.b-warn{background:#422006;color:#fb923c;border:1px solid #7c2d12}
.b-error{background:#450a0a;color:#f87171;border:1px solid #7f1d1d}
.b-critical{background:#3b0764;color:#e879f9;border:1px solid #581c87}
.b-none{background:#1e293b;color:#475569;border:1px solid #334155}
.health{font-size:0.65rem;color:#64748b;margin-top:0.3rem}
.findings{display:flex;flex-direction:column;gap:0.5rem}
.finding{background:#1e293b;border:1px solid #334155;border-radius:0.75rem;padding:0.875rem;border-left:3px solid #334155}
.finding.CRITICAL{border-left-color:#a855f7}
.finding.ERROR{border-left-color:#ef4444}
.finding.WARN{border-left-color:#f97316}
.finding.INFO{border-left-color:#22c55e}
.finding-header{display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.4rem}
.finding-text{font-size:0.8rem;color:#cbd5e1;line-height:1.5}
.finding-fix{font-size:0.75rem;color:#64748b;margin-top:0.4rem;padding-top:0.4rem;border-top:1px solid #334155}
.tag{font-size:0.65rem;padding:0.15rem 0.5rem;border-radius:999px;background:#0f172a;color:#475569;border:1px solid #334155}
.ts{font-size:0.65rem;color:#475569;margin-left:auto}
.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:1.5rem}
.pill{display:flex;align-items:center;gap:0.4rem;font-size:0.75rem;padding:0.3rem 0.75rem;border-radius:999px;background:#1e293b;border:1px solid #334155}
.live-dot{width:7px;height:7px;border-radius:50%;background:#22c55e;animation:ping 1.5s infinite}
@keyframes ping{0%,100%{opacity:1}50%{opacity:0.3}}
.section-title{font-size:0.7rem;font-weight:700;text-transform:uppercase;letter-spacing:0.08em;color:#475569;margin-bottom:0.75rem}
.empty{text-align:center;color:#334155;padding:2rem;font-size:0.85rem}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>SRE Dashboard</h1>
    <div class="subtitle" id="scan-time">Waiting for first scan…</div>
  </div>
  <div class="pill">
    <span class="live-dot" id="live-dot" style="background:#475569"></span>
    <span id="live-label">Connecting</span>
  </div>
</div>

<div class="section-title">Container Health</div>
<div class="grid" id="containers"><div class="card"><div class="empty">Loading…</div></div></div>

<div class="section-title">Findings <span id="finding-count" style="color:#64748b;font-weight:400"></span></div>
<div class="findings" id="findings"><div class="empty">No findings yet</div></div>

<script>
const SEV_BADGE = {INFO:'b-info',WARN:'b-warn',ERROR:'b-error',CRITICAL:'b-critical'};
const SEV_DOT = {INFO:'dot-green',WARN:'dot-gray',ERROR:'dot-red',CRITICAL:'dot-red'};

function relTime(iso){
  if(!iso) return '';
  const s = Math.floor((Date.now()-new Date(iso))/1000);
  if(s<60) return s+'s ago';
  if(s<3600) return Math.floor(s/60)+'m ago';
  return Math.floor(s/3600)+'h ago';
}

function renderContainers(cs){
  const el = document.getElementById('containers');
  if(!cs.length){el.innerHTML='<div class="card"><div class="empty">No containers</div></div>';return;}
  el.innerHTML = cs.map(c=>{
    const dotCls = c.running ? 'dot-green' : 'dot-red';
    const sev = c.last_severity || '';
    const badge = sev ? `<span class="badge ${SEV_BADGE[sev]||'b-none'}">${sev}</span>` : '';
    const health = c.health && c.health !== 'None' ? `<div class="health">${c.health.toLowerCase()}</div>` : '';
    return `<div class="card">
      <div class="card-name"><span class="dot ${dotCls}"></span>${c.name.replace('rre-','')}</div>
      ${badge}${health}
    </div>`;
  }).join('');
}

function renderFindings(fs){
  const el = document.getElementById('findings');
  document.getElementById('finding-count').textContent = fs.length ? `(${fs.length})` : '';
  if(!fs.length){el.innerHTML='<div class="empty">No findings yet — all clear</div>';return;}
  el.innerHTML = [...fs].reverse().slice(0,50).map(f=>{
    const fix = f.severity!=='INFO' && f.proposed_fix && f.proposed_fix!=='No action required'
      ? `<div class="finding-fix">Fix: ${f.proposed_fix}</div>` : '';
    return `<div class="finding ${f.severity}">
      <div class="finding-header">
        <span class="badge ${SEV_BADGE[f.severity]||'b-none'}">${f.severity}</span>
        <span class="tag">${f.category}</span>
        <span class="tag" style="color:#94a3b8">${(f.container_name||'').replace('rre-','')}</span>
        <span class="ts">${relTime(f.observed_at)}</span>
      </div>
      <div class="finding-text">${f.finding}</div>
      ${fix}
    </div>`;
  }).join('');
}

async function fetchStatus(){
  try{
    const [sr,fr] = await Promise.all([fetch('/api/sre/status'),fetch('/api/sre/findings')]);
    if(sr.ok){
      const s = await sr.json();
      renderContainers(s.containers||[]);
      if(s.last_scan_at) document.getElementById('scan-time').textContent =
        'Last scan: '+relTime(s.last_scan_at) + (s.llm_available?' · LLM active':' · LLM unavailable');
    }
    if(fr.ok) renderFindings(await fr.json());
  }catch(e){}
}

fetchStatus();
setInterval(fetchStatus, 30000);

const es = new EventSource('/api/sre/findings/stream');
es.onopen = ()=>{
  document.getElementById('live-dot').style.background='#22c55e';
  document.getElementById('live-label').textContent='Live';
};
es.onerror = ()=>{
  document.getElementById('live-dot').style.background='#ef4444';
  document.getElementById('live-label').textContent='Disconnected';
};
es.onmessage = e=>{
  try{
    const f = JSON.parse(e.data);
    const el = document.getElementById('findings');
    if(el.querySelector('.empty')) el.innerHTML='';
    const div = document.createElement('div');
    div.className = `finding ${f.severity}`;
    const fix = f.severity!=='INFO' && f.proposed_fix && f.proposed_fix!=='No action required'
      ? `<div class="finding-fix">Fix: ${f.proposed_fix}</div>` : '';
    div.innerHTML = `<div class="finding-header">
      <span class="badge ${SEV_BADGE[f.severity]||'b-none'}">${f.severity}</span>
      <span class="tag">${f.category}</span>
      <span class="tag" style="color:#94a3b8">${(f.container_name||'').replace('rre-','')}</span>
      <span class="ts">just now</span>
    </div><div class="finding-text">${f.finding}</div>${fix}`;
    el.prepend(div);
    // refresh containers so severity dots update
    fetchStatus();
  }catch(e){}
};
</script>
</body>
</html>"#;

async fn status(State(app): State<AppState>) -> Json<serde_json::Value> {
    let st = app.state.read().await;
    Json(serde_json::json!({
             "containers":    st.containers,
             "llm_available": st.llm_available,
             "last_scan_at":  st.last_scan_at,
         }))
}

async fn findings(State(app): State<AppState>) -> Json<Vec<Finding>> {
    // Serve from ClickHouse so all replicas return consistent data.
    // Falls back to in-memory state if ClickHouse is unavailable.
    match SreStore::read_recent(&app.ch, 100).await {
        Ok(rows) => Json(rows.into_iter()
                             .map(|obs| Finding { severity: obs.severity,
                                                  category: obs.category,
                                                  finding: obs.finding,
                                                  proposed_fix: obs.proposed_fix,
                                                  container_name: obs.container_name,
                                                  observed_at: Some(obs.observed_at) })
                             .collect()),
        Err(_) => {
            let st = app.state.read().await;
            Json(st.findings.iter().cloned().collect())
        }
    }
}

async fn findings_sse(State(app): State<AppState>)
                      -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = app.tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| async move { r.ok() })
                                         .map(|f| {
                                             Ok::<_, Infallible>(
                Event::default().data(serde_json::to_string(&f).unwrap_or_default()),
            )
                                         });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(25))
                                                .text("ping"))
}

async fn outages(State(app): State<AppState>) -> Json<Vec<Incident>> {
    match SreStore::read_incidents(&app.ch).await {
        Ok(incidents) => Json(incidents),
        Err(_) => Json(vec![]),
    }
}

async fn traces_insights(State(s): State<AppState>) -> Json<trace_analysis::TraceInsights> {
    let insights = trace_analysis::fetch_insights(&s.ch, &s.llm).await;
    Json(insights)
}

async fn system_ready(State(app): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let st = app.state.read().await;
    let (total_lag, lag_trend, ch_backlog_batches) =
        crate::store::fetch_pipeline_lag(&app.ch).await;

    // Build service map from last probe result.
    let services_map: serde_json::Value = match &st.last_probe {
        Some(probe) => {
            let mut map = serde_json::Map::new();
            for svc in &probe.services {
                map.insert(svc.name.clone(),
                           serde_json::json!({
                               "ok": svc.ok,
                               "latency_ms": svc.latency_ms,
                               "error": svc.error,
                           }));
            }
            serde_json::Value::Object(map)
        }
        None => serde_json::json!({}),
    };

    let all_services_ok = st.last_probe.as_ref().map(|p| p.all_ok).unwrap_or(false);

    let ready = all_services_ok && total_lag == 0;
    let degraded = !ready;

    // HTTP 503 only when both postgres AND kafka are unreachable.
    let postgres_ok = st.last_probe
                        .as_ref()
                        .and_then(|p| p.services.iter().find(|s| s.name == "postgres"))
                        .map(|s| s.ok)
                        .unwrap_or(true);
    let kafka_ok = st.last_probe
                     .as_ref()
                     .and_then(|p| p.services.iter().find(|s| s.name == "kafka"))
                     .map(|s| s.ok)
                     .unwrap_or(true);
    let status_code = if !postgres_ok && !kafka_ok {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    let probed_at = st.last_probe
                      .as_ref()
                      .map(|p| p.probed_at.to_rfc3339())
                      .unwrap_or_default();

    let backlog = match &st.weakest_link {
        Some(wl) => serde_json::json!({
            "consumer_lag_total": total_lag,
            "lag_trend": lag_trend,
            "ch_backlog_batches": ch_backlog_batches,
            "weakest_link": wl.weakest_link,
            "weakest_link_reasoning": wl.reasoning,
            "recommended_action": wl.recommended_action,
            "severity": wl.severity,
        }),
        None => serde_json::json!({
            "consumer_lag_total": total_lag,
            "lag_trend": lag_trend,
            "ch_backlog_batches": ch_backlog_batches,
            "weakest_link": null,
            "weakest_link_reasoning": null,
            "recommended_action": null,
            "severity": null,
        }),
    };

    (status_code,
     Json(serde_json::json!({
              "ready": ready,
              "degraded": degraded,
              "services": services_map,
              "backlog": backlog,
              "probed_at": probed_at,
          })))
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({"ok": true}))
}

pub async fn serve(state: Arc<RwLock<SreState>>,
                   tx: broadcast::Sender<Finding>,
                   ch: Client,
                   llm: AnalysisClient,
                   port: u16) {
    let app_state = AppState { state, tx, ch, llm };

    let router = Router::new().route("/", get(index))
                              .route("/api/sre/status", get(status))
                              .route("/api/sre/findings", get(findings))
                              .route("/api/sre/findings/stream", get(findings_sse))
                              .route("/api/sre/outages", get(outages))
                              .route("/api/sre/traces/insights", get(traces_insights))
                              .route("/api/system/ready", get(system_ready))
                              .route("/health", get(health_check))
                              .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind dashboard port");

    tracing::info!("SRE dashboard listening on http://0.0.0.0:{port}");
    axum::serve(listener, router).await
                                 .expect("dashboard server error");
}
