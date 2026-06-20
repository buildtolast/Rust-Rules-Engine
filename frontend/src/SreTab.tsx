import { useState, useEffect, useRef } from 'react';
import { Activity, CheckCircle, Cpu, Clock, Wifi, WifiOff, ChevronDown, ChevronRight } from 'lucide-react';
import type { SreFinding, SreStatus, SreContainerStatus } from './types';

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

export function SreTab() {
  const [status, setStatus] = useState<SreStatus | null>(null);
  const [findings, setFindings] = useState<SreFinding[]>([]);
  const [sseConnected, setSseConnected] = useState(false);
  const esRef = useRef<EventSource | null>(null);

  const fetchStatus = async () => {
    try {
      const [statusRes, findingsRes] = await Promise.all([
        fetch('/api/sre/status'),
        fetch('/api/sre/findings'),
      ]);
      if (statusRes.ok) setStatus(await statusRes.json());
      if (findingsRes.ok) {
        const data: SreFinding[] = await findingsRes.json();
        setFindings(data.slice().reverse());
      }
    } catch {
      // backend may be unavailable
    }
  };

  useEffect(() => {
    fetchStatus();
    const poll = setInterval(fetchStatus, 30_000);

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

  const llmAvailable = status?.llm_available ?? false;
  const containers = status?.containers ?? [];
  const lastScan = status?.last_scan_at ?? null;

  return (
    <div className="space-y-8">
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
        <button
          onClick={fetchStatus}
          className="text-xs font-semibold text-indigo-600 hover:text-indigo-800 hover:bg-indigo-50 px-3 py-1.5 rounded-xl transition-colors"
        >
          Refresh
        </button>
      </div>

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

      {/* Findings feed */}
      <section>
        <h2 className="text-sm font-bold text-gray-500 uppercase tracking-widest mb-4 flex items-center gap-2">
          <Activity size={14} />
          Findings
          <span className="ml-1 px-2 py-0.5 bg-gray-100 text-gray-600 text-xs rounded-full font-medium">
            {findings.length}
          </span>
        </h2>
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
    </div>
  );
}
