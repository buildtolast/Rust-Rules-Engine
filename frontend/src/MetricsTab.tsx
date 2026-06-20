import React, { useState, useEffect, useCallback } from 'react';
import { Activity, Database, MessageSquare, Server, RefreshCw, Cpu, Clock, Layers } from 'lucide-react';

interface PipelineMetrics {
  messages_total: number;
  batches_total: number;
  messages_per_sec: number;
  avg_eval_ms: number;
  avg_txn_ms: number;
  consumer_lag: number;
  rules_cached: number;
}

interface KafkaMetrics {
  healthy: boolean;
  partitions: number;
  source_topic: string;
}

interface ClickHouseMetrics {
  audit_rows: number;
  sre_observations: number;
  latency_ms: number;
}

interface PostgresMetrics {
  rules_total: number;
  rules_enabled: number;
  latency_ms: number;
}

interface ServiceMetrics {
  pipeline: PipelineMetrics;
  kafka: KafkaMetrics;
  clickhouse: ClickHouseMetrics;
  postgres: PostgresMetrics;
}

function StatRow({ label, value, unit }: { label: string; value: string | number; unit?: string }) {
  return (
    <div className="flex items-center justify-between py-2.5 border-b border-gray-50 last:border-0">
      <span className="text-sm text-gray-500">{label}</span>
      <span className="text-sm font-bold text-gray-900 tabular-nums">
        {value}{unit ? <span className="text-gray-400 font-normal ml-1">{unit}</span> : null}
      </span>
    </div>
  );
}

function ServiceCard({
  title,
  icon,
  color,
  status,
  children,
}: {
  title: string;
  icon: React.ReactNode;
  color: string;
  status: 'ok' | 'warn' | 'idle';
  children: React.ReactNode;
}) {
  const statusColor = status === 'ok' ? 'bg-emerald-400' : status === 'warn' ? 'bg-amber-400' : 'bg-gray-300';
  return (
    <div className="bg-white rounded-3xl shadow-sm border border-gray-100 p-6 hover:shadow-md transition-shadow">
      <div className="flex items-center gap-3 mb-4">
        <div className={`w-11 h-11 ${color} rounded-2xl flex items-center justify-center`}>{icon}</div>
        <div className="flex-1">
          <h3 className="font-bold text-gray-900">{title}</h3>
        </div>
        <div className="flex items-center gap-1.5">
          <div className={`w-2 h-2 rounded-full ${statusColor}`} />
          <span className="text-xs font-bold text-gray-400 uppercase">
            {status === 'ok' ? 'live' : status === 'warn' ? 'slow' : 'idle'}
          </span>
        </div>
      </div>
      <div>{children}</div>
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toLocaleString();
}

export function MetricsTab() {
  const [metrics, setMetrics] = useState<ServiceMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const fetch_ = useCallback(async () => {
    try {
      const res = await fetch('/api/metrics');
      if (res.ok) {
        setMetrics(await res.json());
        setLastUpdated(new Date());
      }
    } catch {
      /* silently retry */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetch_();
    const id = setInterval(fetch_, 10_000);
    return () => clearInterval(id);
  }, [fetch_]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-32">
        <div className="w-10 h-10 border-4 border-indigo-100 border-t-indigo-600 rounded-full animate-spin" />
      </div>
    );
  }

  if (!metrics) {
    return (
      <div className="text-center py-32 text-gray-400">
        <Server size={48} className="mx-auto mb-4 opacity-30" />
        <p className="font-medium">Unable to fetch service metrics</p>
      </div>
    );
  }

  const p = metrics.pipeline;
  const k = metrics.kafka;
  const ch = metrics.clickhouse;
  const pg = metrics.postgres;

  const pipelineStatus = p.messages_total === 0 ? 'idle' : p.consumer_lag > 10_000 ? 'warn' : 'ok';

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Service Metrics</h2>
          <p className="text-gray-500 text-sm mt-1">Live processing stats across all services · refreshes every 10s</p>
        </div>
        <div className="flex items-center gap-3">
          {lastUpdated && (
            <span className="text-xs text-gray-400">
              Updated {lastUpdated.toLocaleTimeString()}
            </span>
          )}
          <button
            onClick={fetch_}
            className="p-2 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all"
            title="Refresh"
          >
            <RefreshCw size={18} />
          </button>
        </div>
      </div>

      {/* Summary bar */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {[
          { label: 'Messages Processed', value: fmt(p.messages_total), icon: <Activity size={20} />, color: 'text-indigo-600 bg-indigo-50' },
          { label: 'Audit Rows (CH)', value: fmt(ch.audit_rows), icon: <Database size={20} />, color: 'text-emerald-600 bg-emerald-50' },
          { label: 'Consumer Lag', value: fmt(p.consumer_lag), icon: <MessageSquare size={20} />, color: p.consumer_lag > 1000 ? 'text-amber-600 bg-amber-50' : 'text-gray-600 bg-gray-50' },
          { label: 'Rules Active', value: `${pg.rules_enabled}/${pg.rules_total}`, icon: <Layers size={20} />, color: 'text-purple-600 bg-purple-50' },
        ].map(s => (
          <div key={s.label} className="bg-white rounded-2xl border border-gray-100 p-4 flex items-center gap-3 shadow-sm">
            <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${s.color}`}>{s.icon}</div>
            <div>
              <p className="text-xs font-bold text-gray-400 uppercase tracking-wide">{s.label}</p>
              <p className="text-xl font-black text-gray-900 tabular-nums">{s.value}</p>
            </div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Pipeline */}
        <ServiceCard
          title="Pipeline (Kafka Consumer)"
          icon={<Cpu size={22} className="text-indigo-600" />}
          color="bg-indigo-50"
          status={pipelineStatus}
        >
          <StatRow label="Messages total" value={fmt(p.messages_total)} />
          <StatRow label="Batches processed" value={fmt(p.batches_total)} />
          <StatRow label="Throughput" value={p.messages_per_sec.toFixed(1)} unit="msg/s" />
          <StatRow label="Avg eval time" value={p.avg_eval_ms} unit="ms/batch" />
          <StatRow label="Avg txn commit" value={p.avg_txn_ms} unit="ms/batch" />
          <StatRow label="Consumer lag" value={fmt(p.consumer_lag)} unit="msgs" />
          <StatRow label="Rules cached" value={p.rules_cached} />
        </ServiceCard>

        {/* Kafka / Redpanda */}
        <ServiceCard
          title="Redpanda (Kafka)"
          icon={<MessageSquare size={22} className="text-orange-600" />}
          color="bg-orange-50"
          status={k.healthy ? 'ok' : 'warn'}
        >
          <StatRow label="Broker health" value={k.healthy ? 'Healthy' : 'Unreachable'} />
          <StatRow label="Source topic" value={k.source_topic} />
          <StatRow label="Partitions" value={k.partitions} />
          <StatRow label="Consumer group" value="rules-engine" />
          <StatRow label="Isolation" value="read_committed" />
          <StatRow label="EOS mode" value="Transactional" />
        </ServiceCard>

        {/* ClickHouse */}
        <ServiceCard
          title="ClickHouse (Analytics)"
          icon={<Database size={22} className="text-emerald-600" />}
          color="bg-emerald-50"
          status={ch.latency_ms < 500 ? 'ok' : 'warn'}
        >
          <StatRow label="Audit rows" value={fmt(ch.audit_rows)} />
          <StatRow label="SRE observations" value={fmt(ch.sre_observations)} />
          <StatRow label="Query latency" value={ch.latency_ms} unit="ms" />
          <StatRow label="Engine" value="ReplacingMergeTree" />
          <StatRow label="Write mode" value="5K batch HTTP insert" />
        </ServiceCard>

        {/* Postgres */}
        <ServiceCard
          title="PostgreSQL (Rule Store)"
          icon={<Server size={22} className="text-blue-600" />}
          color="bg-blue-50"
          status={pg.latency_ms < 200 ? 'ok' : 'warn'}
        >
          <StatRow label="Rules total" value={pg.rules_total} />
          <StatRow label="Rules enabled" value={pg.rules_enabled} />
          <StatRow label="Rules disabled" value={pg.rules_total - pg.rules_enabled} />
          <StatRow label="Query latency" value={pg.latency_ms} unit="ms" />
          <StatRow label="Hot-reload" value="LISTEN/NOTIFY" />
        </ServiceCard>
      </div>

      {/* Timing breakdown */}
      {p.batches_total > 0 && (
        <div className="bg-white rounded-3xl border border-gray-100 shadow-sm p-6">
          <h3 className="font-bold text-gray-900 mb-4 flex items-center gap-2">
            <Clock size={18} className="text-indigo-500" />
            Per-Batch Latency Breakdown
          </h3>
          <div className="space-y-3">
            {[
              { label: 'Parallel eval (rayon)', ms: p.avg_eval_ms, color: 'bg-indigo-500' },
              { label: 'EOS transaction commit', ms: p.avg_txn_ms, color: 'bg-orange-500' },
            ].map(item => {
              const total = p.avg_eval_ms + p.avg_txn_ms;
              const pct = total === 0 ? 0 : Math.round((item.ms / total) * 100);
              return (
                <div key={item.label}>
                  <div className="flex justify-between text-sm mb-1.5">
                    <span className="text-gray-600">{item.label}</span>
                    <span className="font-bold text-gray-900 tabular-nums">{item.ms}ms ({pct}%)</span>
                  </div>
                  <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full ${item.color} rounded-full transition-all duration-700`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
