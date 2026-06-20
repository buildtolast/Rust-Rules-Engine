import React, { useState, useEffect, useCallback } from 'react';
import { RefreshCw, Zap, MessageSquare, Database, Server, HardDrive, Info } from 'lucide-react';

interface RuntimeConfig {
  max_simulation_count: number;
  simulation_senders: number;
  source_topic: string;
  target_topic: string;
  consumer_group: string;
  transactional_id: string;
  kafka_brokers: string;
  http_port: number;
  rust_log: string;
}

interface ParamRowProps {
  envVar: string;
  value: string | number;
  description: string;
  increaseEffect: string;
  decreaseEffect: string;
  unit?: string;
}

function ParamRow({ envVar, value, description, increaseEffect, decreaseEffect, unit }: ParamRowProps) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div
      className="border border-gray-100 rounded-2xl overflow-hidden cursor-pointer hover:border-indigo-200 transition-colors"
      onClick={() => setExpanded(e => !e)}
    >
      <div className="flex items-center justify-between p-4">
        <div className="flex items-center gap-3 min-w-0">
          <code className="text-xs font-mono bg-gray-100 text-gray-600 px-2 py-1 rounded-lg whitespace-nowrap shrink-0">
            {envVar}
          </code>
          <span className="text-sm text-gray-500 truncate hidden sm:block">{description}</span>
        </div>
        <div className="flex items-center gap-2 shrink-0 ml-3">
          <span className="font-bold text-gray-900 tabular-nums text-sm">
            {typeof value === 'number' ? value.toLocaleString() : value}
            {unit && <span className="text-gray-400 font-normal ml-1 text-xs">{unit}</span>}
          </span>
          <Info size={14} className={`transition-transform text-gray-400 ${expanded ? 'rotate-180' : ''}`} />
        </div>
      </div>
      {expanded && (
        <div className="border-t border-gray-50 px-4 pb-4 pt-3 bg-gray-50/50 space-y-3">
          <p className="text-sm text-gray-600">{description}</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div className="bg-emerald-50 border border-emerald-100 rounded-xl p-3">
              <p className="text-xs font-bold text-emerald-700 uppercase tracking-wide mb-1">Increasing</p>
              <p className="text-sm text-emerald-800">{increaseEffect}</p>
            </div>
            <div className="bg-amber-50 border border-amber-100 rounded-xl p-3">
              <p className="text-xs font-bold text-amber-700 uppercase tracking-wide mb-1">Decreasing</p>
              <p className="text-sm text-amber-800">{decreaseEffect}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

interface StaticParamRowProps {
  name: string;
  value: string;
  description: string;
  increaseEffect: string;
  decreaseEffect: string;
  note?: string;
}

function StaticParamRow({ name, value, description, increaseEffect, decreaseEffect, note }: StaticParamRowProps) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div
      className="border border-gray-100 rounded-2xl overflow-hidden cursor-pointer hover:border-indigo-200 transition-colors"
      onClick={() => setExpanded(e => !e)}
    >
      <div className="flex items-center justify-between p-4">
        <div className="flex items-center gap-3 min-w-0">
          <code className="text-xs font-mono bg-gray-100 text-gray-600 px-2 py-1 rounded-lg whitespace-nowrap shrink-0">
            {name}
          </code>
          <span className="text-sm text-gray-500 truncate hidden sm:block">{description}</span>
        </div>
        <div className="flex items-center gap-2 shrink-0 ml-3">
          <span className="font-bold text-gray-900 tabular-nums text-sm">{value}</span>
          <Info size={14} className={`transition-transform text-gray-400 ${expanded ? 'rotate-180' : ''}`} />
        </div>
      </div>
      {expanded && (
        <div className="border-t border-gray-50 px-4 pb-4 pt-3 bg-gray-50/50 space-y-3">
          <p className="text-sm text-gray-600">{description}</p>
          {note && (
            <p className="text-xs text-indigo-600 bg-indigo-50 border border-indigo-100 rounded-xl px-3 py-2">
              {note}
            </p>
          )}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div className="bg-emerald-50 border border-emerald-100 rounded-xl p-3">
              <p className="text-xs font-bold text-emerald-700 uppercase tracking-wide mb-1">Increasing</p>
              <p className="text-sm text-emerald-800">{increaseEffect}</p>
            </div>
            <div className="bg-amber-50 border border-amber-100 rounded-xl p-3">
              <p className="text-xs font-bold text-amber-700 uppercase tracking-wide mb-1">Decreasing</p>
              <p className="text-sm text-amber-800">{decreaseEffect}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

interface SectionProps {
  title: string;
  subtitle: string;
  icon: React.ReactNode;
  color: string;
  children: React.ReactNode;
  badge?: string;
}

function Section({ title, subtitle, icon, color, children, badge }: SectionProps) {
  return (
    <div className="bg-white rounded-3xl border border-gray-100 shadow-sm overflow-hidden">
      <div className={`px-6 py-4 border-b border-gray-50 flex items-center gap-3 ${color}`}>
        <div className="opacity-80">{icon}</div>
        <div className="flex-1">
          <h3 className="font-bold text-gray-900">{title}</h3>
          <p className="text-xs text-gray-500 mt-0.5">{subtitle}</p>
        </div>
        {badge && (
          <span className="text-xs font-bold bg-white/60 border border-white/40 text-gray-600 px-2 py-1 rounded-lg">
            {badge}
          </span>
        )}
      </div>
      <div className="p-4 space-y-2">{children}</div>
    </div>
  );
}

export function ConfigTab() {
  const [config, setConfig] = useState<RuntimeConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const fetchConfig = useCallback(async () => {
    try {
      const res = await fetch('/api/config');
      if (res.ok) {
        setConfig(await res.json());
        setLastUpdated(new Date());
      }
    } catch {
      /* ignore */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-32">
        <div className="w-10 h-10 border-4 border-indigo-100 border-t-indigo-600 rounded-full animate-spin" />
      </div>
    );
  }

  if (!config) {
    return (
      <div className="text-center py-32 text-gray-400">
        <Server size={48} className="mx-auto mb-4 opacity-30" />
        <p className="font-medium">Unable to fetch configuration</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Configuration & Feature Flags</h2>
          <p className="text-gray-500 text-sm mt-1">
            Runtime parameters across all services · click any row for impact details
          </p>
        </div>
        <div className="flex items-center gap-3">
          {lastUpdated && (
            <span className="text-xs text-gray-400">Updated {lastUpdated.toLocaleTimeString()}</span>
          )}
          <button
            onClick={fetchConfig}
            className="p-2 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-xl transition-all"
            title="Refresh"
          >
            <RefreshCw size={18} />
          </button>
        </div>
      </div>

      {/* Simulator Feature Flags */}
      <Section
        title="Simulator Feature Flags"
        subtitle="Control the Traffic Simulator tab behaviour · set via env vars, restart app to apply"
        icon={<Zap size={20} className="text-indigo-600" />}
        color="bg-indigo-50/60"
        badge="env var"
      >
        <ParamRow
          envVar="MAX_SIMULATION_COUNT"
          value={config.max_simulation_count}
          description="Maximum number of synthetic Kafka events that can be pushed in a single simulation run."
          increaseEffect="Allows larger load tests in one shot. Higher values stress the pipeline and ClickHouse insert path more aggressively."
          decreaseEffect="Caps accidental runaway pushes. Useful in dev/staging environments to avoid flooding Redpanda."
        />
        <ParamRow
          envVar="SIMULATION_SENDERS"
          value={config.simulation_senders}
          description="Number of concurrent Tokio tasks used to publish simulation events to Kafka in parallel."
          increaseEffect="Higher publish throughput — saturates Redpanda faster, exercises rdkafka batching under parallel load."
          decreaseEffect="Lower throughput, events published more sequentially. Easier to profile individual latency."
        />
      </Section>

      {/* Kafka / Pipeline */}
      <Section
        title="Kafka / Pipeline"
        subtitle="Topic routing, consumer group identity, and broker addresses · changes require app restart"
        icon={<MessageSquare size={20} className="text-orange-600" />}
        color="bg-orange-50/60"
        badge="env var"
      >
        <ParamRow
          envVar="SOURCE_TOPIC"
          value={config.source_topic}
          description="Kafka topic the pipeline consumer reads raw audit events from."
          increaseEffect="N/A — topic name is not a numeric scale. Changing it redirects the consumer to a different event stream."
          decreaseEffect="N/A — topic name is not a numeric scale."
        />
        <ParamRow
          envVar="TARGET_TOPIC"
          value={config.target_topic}
          description="Kafka topic the pipeline publishes evaluated (matched/unmatched) audit results to."
          increaseEffect="N/A — changing redirects results to a different downstream consumer."
          decreaseEffect="N/A — changing redirects results to a different downstream consumer."
        />
        <ParamRow
          envVar="CONSUMER_GROUP"
          value={config.consumer_group}
          description="Kafka consumer group ID. Kafka tracks per-group offset progress independently."
          increaseEffect="N/A — changing creates a new independent offset cursor, causing all events to be replayed from the beginning of the topic's retention window."
          decreaseEffect="N/A — same as above."
        />
        <ParamRow
          envVar="TRANSACTIONAL_ID"
          value={config.transactional_id}
          description="Kafka exactly-once semantics (EOS) producer transaction ID. Must be unique per producer instance."
          increaseEffect="N/A — must match the identity configured when the producer was registered. Changing causes EOS state reset."
          decreaseEffect="N/A — same as above."
        />
        <ParamRow
          envVar="KAFKA_BROKERS"
          value={config.kafka_brokers}
          description="Comma-separated list of Kafka/Redpanda broker addresses the app connects to."
          increaseEffect="Adding more brokers improves availability and throughput capacity for high-volume workloads."
          decreaseEffect="Fewer brokers reduces redundancy. Single-broker setups have no failover."
        />
      </Section>

      {/* App */}
      <Section
        title="Application"
        subtitle="HTTP server port and log verbosity"
        icon={<Server size={20} className="text-blue-600" />}
        color="bg-blue-50/60"
        badge="env var"
      >
        <ParamRow
          envVar="HTTP_PORT"
          value={config.http_port}
          description="Port the Axum REST API listens on inside the container."
          increaseEffect="N/A — port number doesn't scale performance. Use FRONTEND_PORT / APP_PORT in docker-compose for host-side mapping."
          decreaseEffect="N/A — same as above."
        />
        <ParamRow
          envVar="RUST_LOG"
          value={config.rust_log}
          description="Log verbosity filter. Supports level directives like 'info', 'debug', 'warn', or per-module overrides like 'pipeline=debug,web=info'."
          increaseEffect="More verbose (debug/trace) provides detailed event traces but increases CPU overhead and log volume — not suitable for production under load."
          decreaseEffect="Less verbose (warn/error) reduces overhead and noise. Use 'error' only in high-throughput production runs."
        />
      </Section>

      {/* Resource Limits — static */}
      <Section
        title="Resource Limits (Memory)"
        subtitle="Docker container memory caps set in deploy/docker-compose.yml · requires stack restart to change"
        icon={<HardDrive size={20} className="text-purple-600" />}
        color="bg-purple-50/60"
        badge="docker-compose"
      >
        <StaticParamRow
          name="REDPANDA_MEM_LIMIT"
          value="600M"
          description="Docker memory ceiling for the Redpanda (Kafka) broker container. Redpanda's internal --memory flag is set to 512M, leaving 88M headroom for its JVM/OS overhead."
          increaseEffect="Allows Redpanda to buffer more partitions in memory, improving throughput under bursty load or many partitions."
          decreaseEffect="Redpanda may OOM-kill during burst ingest or large partition count. Minimum safe is ~512M for single-partition dev setups."
          note="Set via env var REDPANDA_MEM_LIMIT in docker-compose.yml. The internal --memory=512M flag must also be updated to match."
        />
        <StaticParamRow
          name="CLICKHOUSE_MEM_LIMIT"
          value="2G"
          description="Docker memory ceiling for the ClickHouse analytics store. ClickHouse uses RAM for query working sets — aggregations, GROUP BY, and materialized view merges."
          increaseEffect="Larger working memory enables faster queries over more data. Needed for 100M+ row analytical queries without spilling to disk."
          decreaseEffect="ClickHouse may OOM on heavy GROUP BY or large INSERT batches. At 1G it still works for normal eval load; below 512M risks crashes."
          note="Set via env var CLICKHOUSE_MEM_LIMIT. Also update max_memory_usage in deploy/clickhouse/config.xml to match (currently 4G — should be lowered to match this limit)."
        />
        <StaticParamRow
          name="POSTGRES_MEM_LIMIT"
          value="256M"
          description="Docker memory ceiling for PostgreSQL, which stores the rules table. Postgres memory scales with shared_buffers and active connections, not message count."
          increaseEffect="More shared_buffers cache — marginally faster rule queries for very large rule sets (1000+ rules)."
          decreaseEffect="Below 128M Postgres may struggle under concurrent connection load. Current 256M is safe for dev and moderate production."
          note="Set via env var POSTGRES_MEM_LIMIT."
        />
        <StaticParamRow
          name="APP_MEM_LIMIT"
          value="512M"
          description="Docker memory ceiling for the Rust rules-engine binary (pipeline + web API). The Rust binary is very lean; this limit is generous for the current load."
          increaseEffect="Headroom for larger rule caches (1000+ rules in memory) or higher batch sizes without GC pressure."
          decreaseEffect="The Rust allocator is frugal — actual usage is well under 100M at normal load. Safe to drop to 256M for dev."
          note="Set via env var APP_MEM_LIMIT."
        />
        <StaticParamRow
          name="SRE_MEM_LIMIT"
          value="256M"
          description="Docker memory ceiling for the SRE monitoring agent. Scans Docker logs and writes observations to ClickHouse on a periodic interval."
          increaseEffect="Allows more log history to be buffered in memory before processing."
          decreaseEffect="Below 128M the agent may crash when processing very large log tails (LOG_TAIL_LINES=1000+)."
          note="Set via env var SRE_MEM_LIMIT."
        />
        <StaticParamRow
          name="FRONTEND_MEM_LIMIT"
          value="64M"
          description="Docker memory ceiling for the nginx frontend container that serves the React app and proxies /api/* and /api/sre/* requests."
          increaseEffect="No measurable effect — nginx for static file serving uses very little memory regardless of user count."
          decreaseEffect="Below 32M nginx may fail to start. 64M is already minimal."
          note="Set via env var FRONTEND_MEM_LIMIT."
        />
      </Section>

      {/* ClickHouse memory config note */}
      <div className="bg-amber-50 border border-amber-200 rounded-2xl p-4 flex items-start gap-3">
        <Database size={18} className="text-amber-600 mt-0.5 shrink-0" />
        <div>
          <p className="text-sm font-bold text-amber-800">ClickHouse internal memory config</p>
          <p className="text-sm text-amber-700 mt-1">
            ClickHouse has a separate in-process memory cap in <code className="bg-amber-100 px-1 rounded text-xs">deploy/clickhouse/config.xml</code>{' '}
            (<code className="bg-amber-100 px-1 rounded text-xs">max_memory_usage</code>, currently 4G). This should be kept below
            the Docker container limit (<code className="bg-amber-100 px-1 rounded text-xs">CLICKHOUSE_MEM_LIMIT=2G</code>).
            Update <code className="bg-amber-100 px-1 rounded text-xs">config.xml</code> to match when you change the container limit.
          </p>
        </div>
      </div>
    </div>
  );
}
