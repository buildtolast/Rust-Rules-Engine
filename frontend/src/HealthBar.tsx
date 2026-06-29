import { useState, useEffect } from "react";

interface ServiceStatus {
  status: "ok" | "error";
  latency_ms: number;
  error: string | null;
}

interface ReadyResponse {
  status: "ready" | "degraded";
  services: {
    postgres: ServiceStatus;
    clickhouse: ServiceStatus;
    kafka: ServiceStatus;
  };
}

const SERVICE_LABELS: Record<string, string> = {
  postgres: "Postgres",
  clickhouse: "ClickHouse",
  kafka: "Kafka",
};

export function HealthBar() {
  const [data, setData] = useState<ReadyResponse | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);

  const check = async () => {
    try {
      const res = await fetch("/health/ready");
      const json: ReadyResponse = await res.json();
      setData(json);
      setLastChecked(new Date());
    } catch {
      setData(null);
    }
  };

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    check();
    const t = setInterval(check, 30_000);
    return () => clearInterval(t);
  }, []);

  const services = data?.services ? Object.entries(data.services) : [];

  return (
    <div className="flex items-center gap-4 px-6 py-2 bg-gray-50 border-b border-gray-100 text-xs">
      <span className="font-bold text-gray-400 uppercase tracking-widest">Services</span>
      {services.length === 0 ? (
        <span className="text-gray-400">checking…</span>
      ) : (
        services.map(([name, svc]) => (
          <div
            key={name}
            className="flex items-center gap-1.5"
            title={svc.error ?? `${svc.latency_ms}ms`}
          >
            <span
              className={`w-2 h-2 rounded-full flex-shrink-0 ${svc.status === "ok" ? "bg-emerald-500" : "bg-red-500 animate-pulse"}`}
            />
            <span
              className={`font-medium ${svc.status === "ok" ? "text-gray-600" : "text-red-600"}`}
            >
              {SERVICE_LABELS[name] ?? name}
            </span>
            <span className="text-gray-400">{svc.latency_ms}ms</span>
          </div>
        ))
      )}
      {lastChecked && (
        <span className="ml-auto text-gray-300">
          checked{" "}
          {lastChecked.toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          })}
        </span>
      )}
    </div>
  );
}
