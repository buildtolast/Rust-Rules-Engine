import { useState, useEffect, useCallback } from 'react';

interface ServiceStatus {
  ok: boolean;
  latency_ms: number;
  error: string | null;
}

interface BacklogInfo {
  consumer_lag_total: number;
  lag_trend: 'growing' | 'stable' | 'draining';
  ch_backlog_batches: number;
  weakest_link: string | null;
  weakest_link_reasoning: string | null;
  recommended_action: string | null;
  severity: string;
}

interface SystemReadyResponse {
  ready: boolean;
  degraded: boolean;
  services: Record<string, ServiceStatus>;
  backlog: BacklogInfo | null;
  probed_at: string;
}

const POLL_INTERVAL_MS = 5_000;

const SERVICE_LABELS: Record<string, string> = {
  postgres:   'Postgres',
  clickhouse: 'ClickHouse',
  kafka:      'Kafka',
  app:        'App',
};

const LAG_TREND_COLORS: Record<string, string> = {
  growing:  'text-red-400',
  stable:   'text-yellow-400',
  draining: 'text-emerald-400',
};

function ServiceRow({ name, svc }: { name: string; svc: ServiceStatus }) {
  return (
    <div className="flex items-center gap-3 py-2 border-b border-gray-700 last:border-0">
      <span
        className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${
          svc.ok ? 'bg-emerald-500' : 'bg-red-500 animate-pulse'
        }`}
      />
      <span className="w-28 font-semibold text-gray-200 flex-shrink-0">
        {SERVICE_LABELS[name] ?? name}
      </span>
      <span className="text-gray-400 text-xs w-20 flex-shrink-0">
        {svc.latency_ms} ms
      </span>
      {svc.error && (
        <span className="text-red-400 text-xs truncate" title={svc.error}>
          {svc.error}
        </span>
      )}
      {!svc.error && svc.ok && (
        <span className="text-emerald-500 text-xs">operational</span>
      )}
    </div>
  );
}

function BlockingOverlay({
  data,
  fetchError,
  lastChecked,
}: {
  data: SystemReadyResponse | null;
  fetchError: boolean;
  lastChecked: Date | null;
}) {
  const services = data ? Object.entries(data.services) : [];

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-950/95 backdrop-blur-sm flex items-center justify-center p-6">
      <div className="bg-gray-900 border border-red-800/60 rounded-2xl shadow-2xl w-full max-w-lg p-8">
        <div className="flex items-center gap-3 mb-6">
          <span className="w-3 h-3 rounded-full bg-red-500 animate-pulse flex-shrink-0" />
          <h2 className="text-2xl font-black text-red-400 tracking-tight">
            System Unavailable
          </h2>
        </div>

        {fetchError ? (
          <p className="text-gray-400 text-sm mb-6">
            Cannot reach SRE agent. Check network connectivity.
          </p>
        ) : (
          <>
            <p className="text-gray-400 text-sm mb-6">
              One or more critical services are unreachable. The system will
              resume automatically when all services recover.
            </p>

            <div className="bg-gray-800 rounded-xl px-4 py-3 mb-6">
              {services.map(([name, svc]) => (
                <ServiceRow key={name} name={name} svc={svc} />
              ))}
            </div>

            {data?.backlog?.weakest_link && (
              <div className="bg-red-950/50 border border-red-800/40 rounded-xl p-4 mb-6">
                <p className="text-xs font-bold text-red-400 uppercase tracking-widest mb-1">
                  Bottleneck
                </p>
                <p className="text-sm font-semibold text-gray-200 mb-1">
                  {SERVICE_LABELS[data.backlog.weakest_link] ?? data.backlog.weakest_link}
                </p>
                {data.backlog.weakest_link_reasoning && (
                  <p className="text-xs text-gray-400 leading-relaxed">
                    {data.backlog.weakest_link_reasoning}
                  </p>
                )}
              </div>
            )}
          </>
        )}

        {lastChecked && (
          <p className="text-xs text-gray-600 text-right">
            Last checked {lastChecked.toLocaleTimeString([], {
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
            })}
          </p>
        )}
      </div>
    </div>
  );
}

function DegradedBanner({ data }: { data: SystemReadyResponse }) {
  const degradedServices = Object.entries(data.services).filter(
    ([, svc]) => !svc.ok,
  );
  const backlog = data.backlog;

  return (
    <div className="sticky top-0 z-50 bg-amber-900/90 border-b border-amber-700/60 text-amber-100 px-5 py-3 text-sm shadow-lg backdrop-blur-sm">
      <div className="max-w-6xl mx-auto flex flex-col gap-1.5">
        <div className="flex items-start gap-3">
          <span className="w-2 h-2 rounded-full bg-amber-400 animate-pulse mt-1.5 flex-shrink-0" />
          <div className="flex-1 min-w-0">
            <span className="font-bold text-amber-300">Degraded — </span>
            {degradedServices.map(([name, svc], i) => (
              <span key={name}>
                {i > 0 && ', '}
                <span className="font-semibold">
                  {SERVICE_LABELS[name] ?? name}
                </span>
                {svc.error && (
                  <span className="text-amber-300/80 ml-1">({svc.error})</span>
                )}
              </span>
            ))}
          </div>
        </div>

        {backlog && (
          <div className="ml-5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-amber-200/80">
            <span>
              {backlog.consumer_lag_total.toLocaleString()} messages queued,{' '}
              {backlog.ch_backlog_batches} audit batches buffered
            </span>
            <span>
              Lag:{' '}
              <span
                className={`font-semibold ${LAG_TREND_COLORS[backlog.lag_trend] ?? 'text-amber-300'}`}
              >
                {backlog.lag_trend}
              </span>
            </span>
            {backlog.weakest_link && (
              <span>
                Bottleneck:{' '}
                <span className="font-semibold text-amber-300">
                  {SERVICE_LABELS[backlog.weakest_link] ?? backlog.weakest_link}
                </span>
                {backlog.weakest_link_reasoning && (
                  <span className="text-amber-200/60 ml-1">
                    — {backlog.weakest_link_reasoning}
                  </span>
                )}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function isBlocking(data: SystemReadyResponse): boolean {
  const s = data.services;
  return (s.postgres && !s.postgres.ok) || (s.kafka && !s.kafka.ok);
}

export function SystemHealthOverlay() {
  const [data, setData] = useState<SystemReadyResponse | null>(null);
  const [fetchError, setFetchError] = useState(false);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const poll = useCallback(async () => {
    try {
      const res = await fetch('/api/system/ready');
      if (!res.ok) {
        setFetchError(true);
        return;
      }
      const json: SystemReadyResponse = await res.json();
      setData(json);
      setFetchError(false);
      setLastChecked(new Date());
    } catch {
      setFetchError(true);
      setLastChecked(new Date());
    }
  }, []);

  useEffect(() => {
    poll();
    const t = setInterval(poll, POLL_INTERVAL_MS);
    return () => clearInterval(t);
  }, [poll]);

  // Show blocking overlay when fetch fails or critical services are down
  if (fetchError || (data && isBlocking(data))) {
    return (
      <BlockingOverlay
        data={data}
        fetchError={fetchError}
        lastChecked={lastChecked}
      />
    );
  }

  // Show degraded banner when degraded but not blocking
  if (data && data.degraded && !isBlocking(data)) {
    return <DegradedBanner data={data} />;
  }

  // Healthy — render nothing
  return null;
}
