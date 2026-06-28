use axum::{
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    Json,
};
use serde::Serialize;
use std::convert::Infallible;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::timeout;
use tokio_stream::{wrappers::LinesStream, StreamExt};

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
            postgres: ServiceCheck { ok: pg_ok, addr: pg_addr },
            clickhouse: ServiceCheck { ok: ch_ok, addr: ch_addr },
            kafka: ServiceCheck { ok: kf_ok, addr: kf_addr },
        },
    })
}

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

    let stdout_lines = LinesStream::new(BufReader::new(stdout).lines());
    let stderr_lines = LinesStream::new(BufReader::new(stderr).lines());
    let merged = tokio_stream::StreamExt::merge(stdout_lines, stderr_lines);

    let stream = async_stream::stream! {
        tokio::pin!(merged);
        while let Some(line) = merged.next().await {
            let text = line.unwrap_or_else(|e| format!("[read error: {e}]"));
            yield Ok::<Event, Infallible>(Event::default().data(text));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_returns_valid_json_shape() {
        let Json(s) = status().await;
        assert!(!s.services.postgres.addr.is_empty());
        assert!(!s.services.clickhouse.addr.is_empty());
        assert!(!s.services.kafka.addr.is_empty());
        assert_eq!(s.ready, s.services.postgres.ok && s.services.clickhouse.ok && s.services.kafka.ok);
    }

    #[tokio::test]
    async fn page_returns_html_with_expected_title() {
        let Html(body) = page().await;
        assert!(body.contains("Integration Tests"));
        assert!(body.contains("/api/integration/status"));
        assert!(body.contains("/api/integration/run"));
    }

    #[tokio::test]
    async fn run_returns_403_without_env_var() {
        std::env::remove_var("ENABLE_TEST_RUNNER");
        let resp = run().await;
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }
}
