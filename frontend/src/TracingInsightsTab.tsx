import { useState, useEffect } from 'react';
import { Cpu, Zap, AlertTriangle, CheckCircle, WifiOff, RefreshCw } from 'lucide-react';
import type { ReactNode } from 'react';
import type { TraceInsights, RulePerf } from './types';

const POLL_INTERVAL_MS = 60_000;

function fmtMs(ms: number): string {
  return ms < 1 ? `${(ms * 1000).toFixed(0)}μs` : `${ms.toFixed(2)}ms`;
}

function PerfBadge({ ms }: { ms: number }) {
  const color = ms < 2 ? 'text-emerald-600' : ms < 10 ? 'text-amber-600' : 'text-red-600';
  return <span className={`font-mono text-xs font-semibold ${color}`}>{fmtMs(ms)}</span>;
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5 flex flex-col gap-1">
      <span className="text-xs text-gray-500 font-medium uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-gray-800">{value}</span>
      {sub && <span className="text-xs text-gray-400">{sub}</span>}
    </div>
  );
}

function InsightList({ title, items, icon }: { title: string; items: string[]; icon: ReactNode }) {
  if (items.length === 0) return null;
  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5 flex flex-col gap-3">
      <div className="flex items-center gap-2">
        {icon}
        <span className="font-semibold text-gray-700 text-sm">{title}</span>
      </div>
      <ul className="flex flex-col gap-2">
        {items.map((item, i) => (
          <li key={i} className="text-sm text-gray-600 flex gap-2">
            <span className="text-gray-300 select-none">·</span>
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function RulePerfRow({ r }: { r: RulePerf }) {
  const shortId = r.rule_id.length > 12 ? `…${r.rule_id.slice(-12)}` : r.rule_id;
  return (
    <tr className="border-t border-gray-50 hover:bg-gray-50 transition-colors">
      <td className="py-2 px-3 font-mono text-xs text-gray-600 max-w-[140px] truncate" title={r.rule_id}>
        {shortId}
      </td>
      <td className="py-2 px-3 text-xs text-gray-500 text-right">{r.eval_count.toLocaleString()}</td>
      <td className="py-2 px-3 text-right"><PerfBadge ms={r.avg_eval_ms} /></td>
      <td className="py-2 px-3 text-right"><PerfBadge ms={r.p95_eval_ms} /></td>
      <td className="py-2 px-3 text-right"><PerfBadge ms={r.p99_eval_ms} /></td>
      <td className="py-2 px-3 text-xs text-gray-400 text-right font-mono">{fmtMs(r.avg_parse_ms)}</td>
      <td className="py-2 px-3 text-xs text-right">
        <span className={r.error_rate_pct > 1 ? 'text-red-500 font-semibold' : 'text-gray-400'}>
          {r.error_rate_pct.toFixed(1)}%
        </span>
      </td>
    </tr>
  );
}

export function TracingInsightsTab() {
  const [insights, setInsights] = useState<TraceInsights | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastFetch, setLastFetch] = useState<Date | null>(null);

  async function fetchInsights() {
    try {
      const res = await fetch('/api/sre/traces/insights');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: TraceInsights = await res.json();
      setInsights(data);
      setError(null);
      setLastFetch(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchInsights();
    const id = setInterval(fetchInsights, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 gap-3 text-gray-400">
        <RefreshCw className="w-5 h-5 animate-spin" />
        <span>Loading trace insights…</span>
      </div>
    );
  }

  if (error || !insights) {
    return (
      <div className="flex items-center justify-center h-48 gap-3 text-red-400">
        <AlertTriangle className="w-5 h-5" />
        <span>{error ?? 'No data'}</span>
      </div>
    );
  }

  const totalEvals = insights.total_evals.toLocaleString();
  const genAt = lastFetch ? lastFetch.toLocaleTimeString() : '—';

  return (
    <div className="flex flex-col gap-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Cpu className="w-5 h-5 text-indigo-500" />
          <h2 className="text-lg font-semibold text-gray-800">Rule Engine Performance</h2>
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
            last {insights.window_minutes}m
          </span>
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-400">
          {insights.llm_available
            ? <CheckCircle className="w-4 h-4 text-emerald-400" />
            : <WifiOff className="w-4 h-4 text-amber-400" />}
          <span>LLM {insights.llm_available ? 'online' : 'offline'}</span>
          <span className="text-gray-200">·</span>
          <span>updated {genAt}</span>
          <button
            onClick={fetchInsights}
            className="ml-2 p-1 rounded-lg hover:bg-gray-100 transition-colors"
            title="Refresh"
          >
            <RefreshCw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <StatCard
          label="Total Evaluations"
          value={totalEvals}
          sub={`last ${insights.window_minutes} minutes`}
        />
        <StatCard
          label="Avg Eval Latency"
          value={fmtMs(insights.avg_eval_ms_overall)}
          sub="weighted across all rules"
        />
        <StatCard
          label="Rules Profiled"
          value={String(insights.rule_perf.length)}
          sub="sorted by p95 latency"
        />
      </div>

      {/* LLM insight columns */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <InsightList
          title="Insights"
          items={insights.llm_insights}
          icon={<Zap className="w-4 h-4 text-indigo-400" />}
        />
        <InsightList
          title="Top Bottlenecks"
          items={insights.llm_bottlenecks}
          icon={<AlertTriangle className="w-4 h-4 text-amber-400" />}
        />
        <InsightList
          title="Recommendations"
          items={insights.llm_recommendations}
          icon={<CheckCircle className="w-4 h-4 text-emerald-400" />}
        />
      </div>

      {/* Per-rule performance table */}
      {insights.rule_perf.length > 0 && (
        <div className="bg-white rounded-2xl border border-gray-100 shadow-sm overflow-hidden">
          <div className="px-5 py-3 border-b border-gray-50 flex items-center gap-2">
            <Cpu className="w-4 h-4 text-gray-400" />
            <span className="font-semibold text-sm text-gray-700">Per-Rule Breakdown</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="bg-gray-50 text-xs text-gray-400 uppercase tracking-wide">
                  <th className="py-2 px-3 font-medium">Rule ID</th>
                  <th className="py-2 px-3 font-medium text-right">Evals</th>
                  <th className="py-2 px-3 font-medium text-right">Avg</th>
                  <th className="py-2 px-3 font-medium text-right">p95</th>
                  <th className="py-2 px-3 font-medium text-right">p99</th>
                  <th className="py-2 px-3 font-medium text-right">Parse</th>
                  <th className="py-2 px-3 font-medium text-right">Err%</th>
                </tr>
              </thead>
              <tbody>
                {insights.rule_perf.map(r => (
                  <RulePerfRow key={r.rule_id} r={r} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
