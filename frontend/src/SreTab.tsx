import { useState, useEffect, useRef } from 'react';
import { Activity, CheckCircle, Cpu, Clock, Wifi, WifiOff, ChevronDown, ChevronRight, AlertTriangle, RotateCcw, GitPullRequest, X } from 'lucide-react';
import type { SreFinding, SreStatus, SreContainerStatus, Incident } from './types';

const SEVERITY_STYLES: Record<string, { badge: string; dot: string }> = {
  INFO:     { badge: 'bg-emerald-50 text-emerald-700 border-emerald-100', dot: 'bg-emerald-500' },
  WARN:     { badge: 'bg-amber-50 text-amber-700 border-amber-100',       dot: 'bg-amber-400' },
  ERROR:    { badge: 'bg-red-50 text-red-700 border-red-100',             dot: 'bg-red-500' },
  CRITICAL: { badge: 'bg-red-100 text-red-800 border-red-200 font-black', dot: 'bg-red-700' },
};

const CATEGORY_LABELS: Record<string, string> = {
  crash_loop:        'Crash Loop',
  connection_refused:'Connection Refused',
  oom:               'Out of Memory',
  latency:           'Latency',
  config_error:      'Config Error',
  normal:            'Normal',
  other:             'Other',
};

function severityStyle(sev: string) {
  return SEVERITY_STYLES[sev] ?? SEVERITY_STYLES.INFO;
}

function relativeTime(iso: string | null): string {
  if (!iso) return '—';
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function ContainerCard({ c }: { c: SreContainerStatus }) {
  const sev = c.last_severity ?? 'INFO';
  const { badge } = severityStyle(sev);
  const healthStatus = c.health?.status ?? 'none';

  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5 flex flex-col gap-3 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${c.running ? 'bg-emerald-500' : 'bg-red-500'}`} />
          <span className="font-semibold text-gray-800 text-sm truncate">{c.name.replace('rre-', '')}</span>
        </div>
        {c.last_severity && (
          <span className={`text-xs px-2 py-0.5 rounded-full border font-bold flex-shrink-0 ${badge}`}>
            {sev}
          </span>
        )}
      </div>
      <div className="flex items-center gap-3 text-xs text-gray-500">
        <span className={`px-2 py-0.5 rounded-lg font-medium ${
          healthStatus === 'healthy' ? 'bg-emerald-50 text-emerald-700' :
          healthStatus === 'unhealthy' ? 'bg-red-50 text-red-700' :
          'bg-gray-50 text-gray-500'
        }`}>
          {healthStatus === 'none' ? 'no healthcheck' : healthStatus}
        </span>
      </div>
      <div className="text-xs text-gray-400 flex items-center gap-1">
        <Clock size={11} />
        {relativeTime(c.last_checked_at)}
      </div>
    </div>
  );
}

function FindingCard({ f }: { f: SreFinding }) {
  const [expanded, setExpanded] = useState(false);
  const { badge, dot } = severityStyle(f.severity);
  const isInfo = f.severity === 'INFO';
  const isActionable = !isInfo && f.proposed_fix && f.proposed_fix !== 'No action required';

  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm overflow-hidden">
      <div className="p-5">
        <div className="flex items-start gap-3">
          <span className={`w-2 h-2 rounded-full mt-1.5 flex-shrink-0 ${dot}`} />
          <div className="flex-1 min-w-0">
            <div className="flex flex-wrap items-center gap-2 mb-2">
              <span className={`text-xs px-2.5 py-0.5 rounded-full border font-bold ${badge}`}>
                {f.severity}
              </span>
              <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 font-medium">
                {CATEGORY_LABELS[f.category] ?? f.category}
              </span>
              {f.container_name && (
                <span className="text-xs text-gray-400 font-mono">{f.container_name.replace('rre-', '')}</span>
              )}
              <span className="text-xs text-gray-400 ml-auto">{relativeTime(f.observed_at)}</span>
            </div>
            <p className="text-sm text-gray-700 leading-relaxed">{f.finding}</p>
          </div>
        </div>
      </div>

      {!isInfo && f.proposed_fix && (
        <button
          onClick={() => setExpanded(e => !e)}
          className={`w-full flex items-center gap-2 px-5 py-3 text-xs font-semibold border-t transition-colors ${
            isActionable
              ? 'text-indigo-600 bg-indigo-50/50 hover:bg-indigo-50 border-indigo-100'
              : 'text-gray-400 bg-gray-50/50 hover:bg-gray-50 border-gray-100'
          }`}
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          {isActionable ? 'Proposed fix' : 'No action required'}
        </button>
      )}

      {expanded && f.proposed_fix && (
        <div className="px-5 py-4 bg-gray-50 border-t border-gray-100">
          <p className="text-sm text-gray-600 leading-relaxed">{f.proposed_fix}</p>
        </div>
      )}
    </div>
  );
}

function formatDuration(secs: number): string {
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString([], { month: 'short', day: 'numeric' });
}

function IncidentRow({ incident }: { incident: Incident }) {
  const downDate = formatDate(incident.down_at);
  const downTime = formatTime(incident.down_at);

  return (
    <div className={`bg-white rounded-2xl border shadow-sm p-5 ${incident.active ? 'border-red-200 shadow-red-50' : 'border-gray-100'}`}>
      <div className="flex flex-wrap items-start gap-3 justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${incident.active ? 'bg-red-500 animate-pulse' : 'bg-emerald-500'}`} />
          <div>
            <span className="font-semibold text-gray-800 text-sm">{incident.container.replace('rre-', '')}</span>
            {incident.active && (
              <span className="ml-2 text-xs font-bold text-red-600 bg-red-50 border border-red-100 px-2 py-0.5 rounded-full">
                ACTIVE OUTAGE
              </span>
            )}
          </div>
        </div>
        {incident.auto_restarted && (
          <span className="flex items-center gap-1 text-xs font-semibold text-indigo-600 bg-indigo-50 border border-indigo-100 px-2.5 py-1 rounded-full">
            <RotateCcw size={11} />
            Auto-restarted
          </span>
        )}
      </div>

      <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
        <div>
          <p className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-1">Went Down</p>
          <p className="font-semibold text-gray-800">{downTime}</p>
          <p className="text-xs text-gray-400">{downDate}</p>
        </div>
        <div>
          <p className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-1">Restored</p>
          {incident.restored_at ? (
            <>
              <p className="font-semibold text-emerald-700">{formatTime(incident.restored_at)}</p>
              <p className="text-xs text-gray-400">{formatDate(incident.restored_at)}</p>
            </>
          ) : (
            <p className="font-semibold text-red-500">Still down</p>
          )}
        </div>
        <div>
          <p className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-1">Duration</p>
          <p className="font-semibold text-gray-800">
            {incident.duration_secs != null
              ? formatDuration(incident.duration_secs)
              : <span className="text-red-500">Ongoing</span>}
          </p>
        </div>
        <div>
          <p className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-1">Recovery</p>
          <p className={`font-semibold ${incident.auto_restarted ? 'text-indigo-600' : 'text-gray-500'}`}>
            {incident.auto_restarted ? 'Automatic' : 'Manual'}
          </p>
        </div>
      </div>
    </div>
  );
}

export function SreTab() {
  const [subTab, setSubTab] = useState<'findings' | 'incidents'>('findings');
  const [status, setStatus] = useState<SreStatus | null>(null);
  const [findings, setFindings] = useState<SreFinding[]>([]);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [sseConnected, setSseConnected] = useState(false);
  const esRef = useRef<EventSource | null>(null);

  // Remediation state
  const [remediating, setRemediating] = useState(false);
  const [remediateLog, setRemediateLog] = useState<string[]>([]);
  const [remediateOpen, setRemediateOpen] = useState(false);
  const [prUrl, setPrUrl] = useState<string | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  const fetchAll = async () => {
    try {
      const [statusRes, findingsRes, outagesRes] = await Promise.all([
        fetch('/api/sre/status'),
        fetch('/api/sre/findings'),
        fetch('/api/sre/outages'),
      ]);
      if (statusRes.ok) setStatus(await statusRes.json());
      if (findingsRes.ok) {
        const data: SreFinding[] = await findingsRes.json();
        setFindings(data.slice().reverse());
      }
      if (outagesRes.ok) setIncidents(await outagesRes.json());
    } catch {
      // backend may be unavailable
    }
  };

  useEffect(() => {
    fetchAll();
    const poll = setInterval(fetchAll, 30_000);

    const es = new EventSource('/api/sre/findings/stream');
    esRef.current = es;
    es.onopen = () => setSseConnected(true);
    es.onerror = () => setSseConnected(false);
    es.onmessage = (e) => {
      try {
        const f: SreFinding = JSON.parse(e.data);
        setFindings(prev => [f, ...prev].slice(0, 100));
      } catch { /* ignore parse errors */ }
    };

    return () => {
      clearInterval(poll);
      es.close();
    };
  }, []);

  const startRemediation = async () => {
    setRemediating(true);
    setRemediateLog([]);
    setPrUrl(null);
    setRemediateOpen(true);
    try {
      const res = await fetch('/api/remediate', { method: 'POST' });
      if (!res.ok || !res.body) {
        setRemediateLog(['Error: remediation server unreachable. Run: python3 tools/sre-remediate.py --serve']);
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const parts = buf.split('\n\n');
        buf = parts.pop() ?? '';
        for (const part of parts) {
          const dataLine = part.split('\n').find(l => l.startsWith('data: '));
          if (!dataLine) continue;
          try {
            const payload = JSON.parse(dataLine.slice(6));
            if (payload.line !== undefined) {
              const line: string = payload.line;
              setRemediateLog(prev => [...prev, line]);
              const m = line.match(/https?:\/\/github\.com\/\S+/);
              if (m) setPrUrl(m[0]);
            }
          } catch { /* ignore malformed SSE */ }
        }
      }
    } catch {
      setRemediateLog(prev => [...prev, 'Connection lost.']);
    } finally {
      setRemediating(false);
    }
  };

  // Auto-scroll log to bottom
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [remediateLog]);

  const llmAvailable = status?.llm_available ?? false;
  const containers = status?.containers ?? [];
  const lastScan = status?.last_scan_at ?? null;
  const activeOutages = incidents.filter(i => i.active);

  return (
    <div className="space-y-6">
      {/* Header strip */}
      <div className="flex flex-wrap items-center justify-between gap-4 bg-white rounded-2xl border border-gray-100 shadow-sm px-6 py-4">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2">
            {llmAvailable ? (
              <Wifi size={16} className="text-emerald-500" />
            ) : (
              <WifiOff size={16} className="text-amber-400" />
            )}
            <span className={`text-sm font-semibold ${llmAvailable ? 'text-emerald-700' : 'text-amber-600'}`}>
              LLM {llmAvailable ? 'Active' : 'Unavailable'}
            </span>
          </div>
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <Clock size={14} />
            Last scan: {relativeTime(lastScan)}
          </div>
          <div className="flex items-center gap-1.5 text-sm">
            <span className={`w-2 h-2 rounded-full ${sseConnected ? 'bg-emerald-500 animate-pulse' : 'bg-gray-300'}`} />
            <span className={sseConnected ? 'text-emerald-600 font-medium' : 'text-gray-400'}>
              {sseConnected ? 'Live' : 'Polling'}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={fetchAll}
            className="text-xs font-semibold text-indigo-600 hover:text-indigo-800 hover:bg-indigo-50 px-3 py-1.5 rounded-xl transition-colors"
          >
            Refresh
          </button>
          <button
            onClick={startRemediation}
            disabled={remediating}
            className={`flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-xl transition-colors ${
              remediating
                ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                : 'bg-indigo-600 text-white hover:bg-indigo-700'
            }`}
          >
            <GitPullRequest size={13} />
            {remediating ? 'Remediating…' : 'Remediate'}
          </button>
        </div>
      </div>

      {/* Remediation output panel */}
      {remediateOpen && (
        <div className="bg-gray-950 rounded-2xl border border-gray-800 overflow-hidden">
          <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-800">
            <div className="flex items-center gap-2">
              <GitPullRequest size={14} className="text-indigo-400" />
              <span className="text-sm font-semibold text-gray-200">Remediation Pipeline</span>
              {remediating && (
                <span className="w-2 h-2 rounded-full bg-indigo-400 animate-pulse" />
              )}
            </div>
            <div className="flex items-center gap-3">
              {prUrl && (
                <a
                  href={prUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs font-semibold text-indigo-400 hover:text-indigo-300 underline"
                >
                  View PR
                </a>
              )}
              <button
                onClick={() => setRemediateOpen(false)}
                className="text-gray-500 hover:text-gray-300 transition-colors"
              >
                <X size={14} />
              </button>
            </div>
          </div>
          <div className="h-64 overflow-y-auto p-4 font-mono text-xs leading-relaxed">
            {remediateLog.length === 0 && remediating && (
              <span className="text-gray-500">Starting pipeline…</span>
            )}
            {remediateLog.map((line, i) => (
              <div key={i} className={`${
                line.includes('✅') || line.includes('PR created') ? 'text-emerald-400' :
                line.includes('Error') || line.includes('error') ? 'text-red-400' :
                line.startsWith('    ✓') ? 'text-emerald-500' :
                line.startsWith('    ~') ? 'text-gray-500' :
                line.startsWith('[') ? 'text-indigo-300' :
                'text-gray-300'
              }`}>
                {line || ' '}
              </div>
            ))}
            {!remediating && remediateLog.length > 0 && (
              <div className="mt-2 text-gray-600">— done —</div>
            )}
            <div ref={logEndRef} />
          </div>
        </div>
      )}

      {/* Container grid */}
      <section>
        <h2 className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-4 flex items-center gap-2">
          <Cpu size={14} />
          Container Health
        </h2>
        {containers.length === 0 ? (
          <div className="bg-white rounded-2xl border border-gray-100 p-10 text-center text-gray-400 text-sm">
            Waiting for first scan…
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            {containers.map(c => <ContainerCard key={c.name} c={c} />)}
          </div>
        )}
      </section>

      {/* Sub-tab nav */}
      <div className="flex gap-1 bg-gray-100 rounded-xl p-1 w-fit">
        <button
          onClick={() => setSubTab('findings')}
          className={`px-4 py-2 text-sm font-semibold rounded-lg transition-all ${
            subTab === 'findings'
              ? 'bg-white text-gray-900 shadow-sm'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <span className="flex items-center gap-2">
            <Activity size={14} />
            Findings
            {findings.length > 0 && (
              <span className="px-1.5 py-0.5 bg-gray-200 text-gray-600 text-xs rounded-full font-medium">
                {findings.length}
              </span>
            )}
          </span>
        </button>
        <button
          onClick={() => setSubTab('incidents')}
          className={`px-4 py-2 text-sm font-semibold rounded-lg transition-all ${
            subTab === 'incidents'
              ? 'bg-white text-gray-900 shadow-sm'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <span className="flex items-center gap-2">
            <AlertTriangle size={14} />
            Incidents
            {activeOutages.length > 0 && (
              <span className="px-1.5 py-0.5 bg-red-100 text-red-600 text-xs rounded-full font-bold animate-pulse">
                {activeOutages.length}
              </span>
            )}
          </span>
        </button>
      </div>

      {/* Findings panel */}
      {subTab === 'findings' && (
        <section>
          {findings.length === 0 ? (
            <div className="bg-white rounded-2xl border border-gray-100 p-10 text-center">
              <div className="flex flex-col items-center gap-3 text-gray-400">
                <CheckCircle size={32} className="text-gray-200" />
                <p className="text-sm">
                  {llmAvailable
                    ? 'No findings yet — first scan in progress.'
                    : 'LLM unavailable. Findings will appear once the LLM is reachable.'}
                </p>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              {findings.map((f, i) => <FindingCard key={`${f.container_name}-${f.observed_at}-${i}`} f={f} />)}
            </div>
          )}
        </section>
      )}

      {/* Incidents panel */}
      {subTab === 'incidents' && (
        <section>
          {incidents.length === 0 ? (
            <div className="bg-white rounded-2xl border border-gray-100 p-10 text-center">
              <div className="flex flex-col items-center gap-3 text-gray-400">
                <CheckCircle size={32} className="text-gray-200" />
                <p className="text-sm">No service incidents recorded yet.</p>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              {incidents.map((inc, i) => (
                <IncidentRow key={`${inc.container}-${inc.down_at}-${i}`} incident={inc} />
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  );
}

export function useActiveOutages(): Incident[] {
  const [active, setActive] = useState<Incident[]>([]);

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/sre/outages')
        .then(r => r.ok ? r.json() : [])
        .then((data: Incident[]) => {
          const next = data.filter(i => i.active);
          // Only update state if the set of active containers changed.
          setActive(prev => {
            const prevKeys = prev.map(i => i.container).sort().join(',');
            const nextKeys = next.map(i => i.container).sort().join(',');
            return prevKeys === nextKeys ? prev : next;
          });
        })
        .catch(() => {});

    fetch_();
    const t = setInterval(fetch_, 30_000);
    return () => clearInterval(t);
  }, []);

  return active;
}
