import React, { useState, useEffect, useRef } from "react";
import axios from "axios";
import {
  Activity,
  BarChart3,
  RefreshCw,
  AlertCircle,
  TrendingUp,
  Hash,
  CheckCircle2,
  Table as TableIcon,
  LineChart as LineChartIcon,
  ArrowUp,
  ArrowDown,
} from "lucide-react";
import type { AnalyticsStats, TimeSeriesPoint, Rule } from "./types";

interface Props {
  rules: Rule[];
}

const AnalyticsDashboard: React.FC<Props> = ({ rules }) => {
  const [stats, setStats] = useState<AnalyticsStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [range, setRange] = useState<string>("24h");
  const [subTab, setSubTab] = useState<"table" | "graph">("table");
  const [sortConfig, setSortConfig] = useState<{
    key: "matched" | "unmatched" | "total";
    direction: "asc" | "desc";
  }>({ key: "matched", direction: "desc" });
  const [selectedRule, setSelectedRule] = useState<string>("all");
  const [currentPage, setCurrentPage] = useState(1);
  const PAGE_SIZE = 10;

  const ruleNameMap = React.useMemo(() => {
    const map: Record<string, string> = {};
    rules.forEach((r) => {
      if (r.id) map[r.id] = r.description;
    });
    return map;
  }, [rules]);

  const ruleName = (id: string) => ruleNameMap[id] ?? id.slice(0, 8) + "…";

  const inFlightRef = useRef(false);

  const fetchStats = async () => {
    // Skip if a request is still running so slow queries don't stack up and
    // keep Mongo permanently busy (the "stuck in a loop" behaviour).
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    setLoading(true);
    try {
      let from: string | null = null;
      const now = new Date();
      if (range === "1h") from = new Date(now.getTime() - 3600000).toISOString();
      else if (range === "6h") from = new Date(now.getTime() - 6 * 3600000).toISOString();
      else if (range === "24h") from = new Date(now.getTime() - 24 * 3600000).toISOString();

      const response = await axios.get<AnalyticsStats>("/api/analytics/stats", {
        params: { from },
      });
      setStats(response.data);
      setError(null);
    } catch (err: unknown) {
      const axiosErr = err as { response?: { data?: { message?: string } }; message?: string };
      const msg = axiosErr.response?.data?.message || axiosErr.message || "Unknown error";
      setError(`Failed to fetch analytics: ${msg}`);
      console.error(err);
    } finally {
      setLoading(false);
      inFlightRef.current = false;
    }
  };

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setCurrentPage(1);
    fetchStats();
    const interval = setInterval(fetchStats, 30000); // Auto refresh every 30s
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range]);

  const sortedRuleStats = React.useMemo(() => {
    if (!stats) return [];

    const sorted = [...stats.ruleStats];
    sorted.sort((a, b) => {
      const aTotal = a.matched + a.unmatched;
      const bTotal = b.matched + b.unmatched;

      let aVal = 0;
      let bVal = 0;

      if (sortConfig.key === "total") {
        aVal = aTotal;
        bVal = bTotal;
      } else if (sortConfig.key === "matched") {
        aVal = a.matched;
        bVal = b.matched;
      } else if (sortConfig.key === "unmatched") {
        aVal = a.unmatched;
        bVal = b.unmatched;
      }

      if (sortConfig.direction === "asc") {
        return aVal - bVal;
      } else {
        return bVal - aVal;
      }
    });
    return sorted;
  }, [stats, sortConfig]);

  // Client-side pagination: 10 rows per page over the sorted list.
  const totalPages = Math.max(1, Math.ceil(sortedRuleStats.length / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const pagedRuleStats = sortedRuleStats.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  const requestSort = (key: "matched" | "unmatched" | "total") => {
    let direction: "asc" | "desc" = "desc";
    if (sortConfig.key === key && sortConfig.direction === "desc") {
      direction = "asc";
    }
    setSortConfig({ key, direction });
    setCurrentPage(1);
  };

  if (loading && !stats) {
    return (
      <div className="flex flex-col items-center justify-center py-24">
        <div className="w-12 h-12 border-4 border-indigo-100 border-t-indigo-600 rounded-full animate-spin mb-4"></div>
        <span className="text-lg font-medium text-gray-500">Calculating statistics...</span>
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      {/* Analytics Header */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <TrendingUp size={24} className="text-indigo-600" />
            System Performance
          </h2>
          <p className="text-gray-500">Real-time evaluation metrics and rule performance.</p>
        </div>

        <div className="flex items-center gap-3">
          <div className="bg-white p-1 rounded-2xl border border-gray-200 flex shadow-sm">
            {(["1h", "6h", "24h"] as const).map((r) => (
              <button
                key={r}
                onClick={() => setRange(r)}
                className={`px-4 py-2 rounded-xl text-xs font-bold transition-all ${range === r ? "bg-indigo-600 text-white shadow-md" : "text-gray-500 hover:bg-gray-50"}`}
              >
                Last {r}
              </button>
            ))}
          </div>
          <button
            onClick={fetchStats}
            className="p-2.5 text-gray-500 hover:text-indigo-600 hover:bg-white hover:shadow-sm border border-transparent hover:border-gray-200 rounded-xl transition-all"
          >
            <RefreshCw size={20} className={loading ? "animate-spin" : ""} />
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-2xl p-4 flex items-start gap-3">
          <AlertCircle className="text-red-500 mt-0.5" size={20} />
          <p className="text-red-700 font-medium">{error}</p>
        </div>
      )}

      {/* High-level Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          icon={<Activity className="text-blue-600" size={24} />}
          label="Processed Messages"
          value={stats?.totalMessages || 0}
          bgColor="bg-blue-50"
        />
        <StatCard
          icon={<Hash className="text-purple-600" size={24} />}
          label="Total Evaluations"
          value={stats?.totalEvaluations || 0}
          bgColor="bg-purple-50"
        />
        <StatCard
          icon={<CheckCircle2 className="text-emerald-600" size={24} />}
          label="Matches"
          value={(stats?.ruleStats || []).reduce((acc, curr) => acc + curr.matched, 0)}
          bgColor="bg-emerald-50"
        />
        <StatCard
          icon={<AlertCircle className="text-red-600" size={24} />}
          label="Errors"
          value={(stats?.ruleStats || []).reduce((acc, curr) => acc + curr.errored, 0)}
          bgColor="bg-red-50"
        />
      </div>

      {/* Rule Breakdown */}
      <div className="bg-white rounded-[2rem] shadow-xl shadow-gray-200/50 border border-gray-100 overflow-hidden">
        <div className="px-8 py-6 border-b border-gray-50 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <h3 className="text-xl font-bold text-gray-900 flex items-center gap-2">
            <BarChart3 size={20} className="text-indigo-600" />
            Rule Performance Breakdown
          </h3>

          <div className="bg-gray-100 p-1 rounded-xl flex">
            <button
              onClick={() => setSubTab("table")}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold transition-all ${subTab === "table" ? "bg-white text-indigo-600 shadow-sm" : "text-gray-500 hover:text-gray-700"}`}
            >
              <TableIcon size={14} />
              Table
            </button>
            <button
              onClick={() => setSubTab("graph")}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-bold transition-all ${subTab === "graph" ? "bg-white text-indigo-600 shadow-sm" : "text-gray-500 hover:text-gray-700"}`}
            >
              <LineChartIcon size={14} />
              Timeseries
            </button>
          </div>
        </div>

        {subTab === "table" ? (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-gray-50/50 border-b border-gray-100">
                  <th className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">
                    Rule Identifier
                  </th>
                  <th
                    className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest cursor-pointer hover:text-indigo-600 transition-colors"
                    onClick={() => requestSort("total")}
                  >
                    <div className="flex items-center gap-1">
                      Total
                      {sortConfig.key === "total" &&
                        (sortConfig.direction === "asc" ? (
                          <ArrowUp size={12} />
                        ) : (
                          <ArrowDown size={12} />
                        ))}
                    </div>
                  </th>
                  <th
                    className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest cursor-pointer hover:text-indigo-600 transition-colors"
                    onClick={() => requestSort("matched")}
                  >
                    <div className="flex items-center gap-1">
                      Matched
                      {sortConfig.key === "matched" &&
                        (sortConfig.direction === "asc" ? (
                          <ArrowUp size={12} />
                        ) : (
                          <ArrowDown size={12} />
                        ))}
                    </div>
                  </th>
                  <th
                    className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest cursor-pointer hover:text-indigo-600 transition-colors"
                    onClick={() => requestSort("unmatched")}
                  >
                    <div className="flex items-center gap-1">
                      Unmatched
                      {sortConfig.key === "unmatched" &&
                        (sortConfig.direction === "asc" ? (
                          <ArrowUp size={12} />
                        ) : (
                          <ArrowDown size={12} />
                        ))}
                    </div>
                  </th>
                  <th className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">
                    Errored
                  </th>
                  <th className="px-8 py-4 text-xs font-bold text-gray-400 uppercase tracking-widest">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {sortedRuleStats.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-8 py-12 text-center text-gray-500 font-medium">
                      No evaluations recorded for this time range.
                    </td>
                  </tr>
                ) : (
                  pagedRuleStats.map((rs) => {
                    const total = rs.matched + rs.unmatched + rs.errored;
                    const matchRate = total > 0 ? ((rs.matched / total) * 100).toFixed(1) : "0";

                    return (
                      <tr
                        key={rs.ruleId}
                        className={`hover:bg-gray-50/50 transition-colors ${selectedRule === rs.ruleId ? "bg-indigo-50/50" : ""}`}
                      >
                        <td className="px-8 py-5 text-sm font-bold text-gray-700 max-w-[220px]">
                          <span className="block truncate" title={rs.ruleId}>
                            {ruleName(rs.ruleId)}
                          </span>
                        </td>
                        <td className="px-8 py-5">
                          <div className="w-full bg-gray-100 rounded-full h-2.5 max-w-[120px]">
                            <div
                              className="bg-indigo-600 h-2.5 rounded-full"
                              style={{ width: `${matchRate}%` }}
                            ></div>
                          </div>
                          <span className="text-[10px] font-black text-indigo-600 mt-1 block uppercase tracking-tighter">
                            {matchRate}% match rate
                          </span>
                          <span className="text-[10px] font-bold text-gray-400 block uppercase tracking-tighter">
                            ({rs.matched + rs.unmatched} total)
                          </span>
                        </td>
                        <td className="px-8 py-5">
                          <span className="text-emerald-600 font-black text-lg">{rs.matched}</span>
                        </td>
                        <td className="px-8 py-5">
                          <span className="text-gray-400 font-bold">{rs.unmatched}</span>
                        </td>
                        <td className="px-8 py-5">
                          <span
                            className={`font-bold ${rs.errored > 0 ? "text-red-500" : "text-gray-300"}`}
                          >
                            {rs.errored}
                          </span>
                        </td>
                        <td className="px-8 py-5">
                          <button
                            onClick={() => {
                              setSelectedRule(rs.ruleId);
                              setSubTab("graph");
                            }}
                            className="text-xs font-bold text-indigo-600 hover:text-indigo-800 flex items-center gap-1"
                          >
                            <LineChartIcon size={14} />
                            View Trend
                          </button>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
            {sortedRuleStats.length > 0 && (
              <div className="flex items-center justify-between px-8 py-4 border-t border-gray-100">
                <span className="text-xs font-bold text-gray-400 uppercase tracking-widest">
                  Page {safePage} of {totalPages} · {sortedRuleStats.length} rules
                </span>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                    disabled={safePage <= 1}
                    className="px-3 py-1.5 text-xs font-bold rounded-lg border border-gray-200 text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    Previous
                  </button>
                  <button
                    onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
                    disabled={safePage >= totalPages}
                    className="px-3 py-1.5 text-xs font-bold rounded-lg border border-gray-200 text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </div>
        ) : (
          <div className="p-8">
            <div className="flex justify-between items-center mb-6">
              <div className="flex items-center gap-3">
                <span className="text-sm font-bold text-gray-500 uppercase">Filter Rule:</span>
                <select
                  value={selectedRule}
                  onChange={(e) => setSelectedRule(e.target.value)}
                  className="bg-gray-50 border border-gray-200 rounded-xl px-4 py-2 text-sm font-medium focus:ring-2 focus:ring-indigo-500 outline-none"
                >
                  <option value="all">All Rules (Aggregated)</option>
                  {(stats?.ruleStats || []).map((rs) => (
                    <option key={rs.ruleId} value={rs.ruleId}>
                      {ruleName(rs.ruleId)}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <PerformanceChart data={stats?.timeSeries || []} selectedRule={selectedRule} />
          </div>
        )}
      </div>
    </div>
  );
};

interface StatCardProps {
  icon: React.ReactNode;
  label: string;
  value: number;
  bgColor: string;
}

const StatCard: React.FC<StatCardProps> = ({ icon, label, value, bgColor }) => {
  const formatted = value.toLocaleString();
  const sizeClass =
    formatted.length > 11 ? "text-xl" : formatted.length > 8 ? "text-2xl" : "text-3xl";
  return (
    <div className="bg-white p-6 rounded-3xl shadow-sm border border-gray-100 flex items-center gap-5 min-w-0">
      <div
        className={`w-14 h-14 flex-shrink-0 ${bgColor} rounded-2xl flex items-center justify-center shadow-inner`}
      >
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-sm font-bold text-gray-500 uppercase tracking-wider">{label}</p>
        <h3 className={`${sizeClass} font-black text-gray-900 tabular-nums`}>{formatted}</h3>
      </div>
    </div>
  );
};

interface PerformanceChartProps {
  data: TimeSeriesPoint[];
  selectedRule: string;
}

const PerformanceChart: React.FC<PerformanceChartProps> = ({ data, selectedRule }) => {
  const [hoveredPoint, setHoveredPoint] = useState<number | null>(null);
  const [visibleSeries, setVisibleSeries] = useState({
    matched: true,
    unmatched: false,
    errored: false,
  });

  const toggleSeries = (series: keyof typeof visibleSeries) => {
    setVisibleSeries((prev) => {
      const next = { ...prev, [series]: !prev[series] };
      // Ensure at least one is always selected, or allow none?
      // User said "matched vs unmatched or both", so maybe allow toggling freely.
      return next;
    });
  };

  const filteredData = React.useMemo(() => {
    if (selectedRule === "all") {
      // Group by timestamp and sum values
      const grouped = data.reduce(
        (acc, curr) => {
          const time = curr.timestamp;
          if (!acc[time]) {
            acc[time] = { ...curr, ruleId: "all", matched: 0, unmatched: 0, errored: 0 };
          }
          acc[time].matched += curr.matched;
          acc[time].unmatched += curr.unmatched;
          acc[time].errored += curr.errored;
          return acc;
        },
        {} as Record<string, TimeSeriesPoint>
      );
      return Object.values(grouped).sort(
        (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
      );
    }
    return data.filter((d) => d.ruleId === selectedRule);
  }, [data, selectedRule]);

  if (filteredData.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-gray-400 font-medium bg-gray-50 rounded-2xl border-2 border-dashed border-gray-100">
        No timeseries data available for this range.
      </div>
    );
  }

  const maxVal = Math.max(
    ...filteredData.map((d) => {
      let sum = 0;
      if (visibleSeries.matched) sum = Math.max(sum, d.matched);
      if (visibleSeries.unmatched) sum = Math.max(sum, d.unmatched);
      if (visibleSeries.errored) sum = Math.max(sum, d.errored);
      return sum;
    }),
    1
  );
  const width = 800;
  const height = 300;
  const padding = 40;

  const points = filteredData.map((d, i) => ({
    x: padding + (i * (width - 2 * padding)) / (filteredData.length - 1 || 1),
    yMatched: height - padding - (d.matched * (height - 2 * padding)) / maxVal,
    yUnmatched: height - padding - (d.unmatched * (height - 2 * padding)) / maxVal,
    yErrored: height - padding - (d.errored * (height - 2 * padding)) / maxVal,
    yVal:
      height -
      padding -
      ((visibleSeries.matched ? d.matched : visibleSeries.unmatched ? d.unmatched : d.errored) *
        (height - 2 * padding)) /
        maxVal,
    date: new Date(d.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
    fullDate: new Date(d.timestamp).toLocaleString(),
    data: d,
  }));

  const getPath = (key: "yMatched" | "yUnmatched" | "yErrored") => {
    if (points.length < 2) return "";
    return points.reduce(
      (acc, p, i) => acc + (i === 0 ? `M ${p.x} ${p[key]}` : ` L ${p.x} ${p[key]}`),
      ""
    );
  };

  const getAreaPath = (key: "yMatched" | "yUnmatched" | "yErrored") => {
    if (points.length < 2) return "";
    const path = getPath(key);
    return `${path} L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;
  };

  return (
    <div className="w-full">
      <div className="flex justify-between items-center mb-6">
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-black text-gray-400 uppercase tracking-widest">
            Toggle Series:
          </span>
          <div className="flex gap-2">
            <button
              onClick={() => toggleSeries("matched")}
              className={`px-3 py-1 rounded-full text-[10px] font-bold transition-all border ${visibleSeries.matched ? "bg-emerald-500 text-white border-emerald-500 shadow-sm" : "bg-white text-gray-400 border-gray-100"}`}
            >
              Matched
            </button>
            <button
              onClick={() => toggleSeries("unmatched")}
              className={`px-3 py-1 rounded-full text-[10px] font-bold transition-all border ${visibleSeries.unmatched ? "bg-indigo-500 text-white border-indigo-500 shadow-sm" : "bg-white text-gray-400 border-gray-100"}`}
            >
              Unmatched
            </button>
            <button
              onClick={() => toggleSeries("errored")}
              className={`px-3 py-1 rounded-full text-[10px] font-bold transition-all border ${visibleSeries.errored ? "bg-red-500 text-white border-red-500 shadow-sm" : "bg-white text-gray-400 border-gray-100"}`}
            >
              Errored
            </button>
          </div>
        </div>

        <div className="flex gap-4">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-emerald-500"></div>
            <span className="text-[10px] font-black text-gray-500 uppercase">Matched</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-indigo-500"></div>
            <span className="text-[10px] font-black text-gray-500 uppercase">Unmatched</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-red-500"></div>
            <span className="text-[10px] font-black text-gray-500 uppercase">Errored</span>
          </div>
        </div>
      </div>

      <div className="relative">
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-auto overflow-visible">
          {/* Grid Lines */}
          {[0, 0.25, 0.5, 0.75, 1].map((p) => {
            const y = height - padding - p * (height - 2 * padding);
            return (
              <g key={p}>
                <line
                  x1={padding}
                  y1={y}
                  x2={width - padding}
                  y2={y}
                  stroke="#f1f5f9"
                  strokeWidth="1"
                />
                <text
                  x={padding - 10}
                  y={y}
                  textAnchor="end"
                  alignmentBaseline="middle"
                  className="text-[10px] fill-gray-400 font-bold"
                >
                  {Math.round(p * maxVal)}
                </text>
              </g>
            );
          })}

          {/* Areas */}
          {visibleSeries.unmatched && (
            <path
              d={getAreaPath("yUnmatched")}
              className="fill-indigo-50 opacity-30 transition-all duration-500"
            />
          )}
          {visibleSeries.matched && (
            <path
              d={getAreaPath("yMatched")}
              className="fill-emerald-50 opacity-30 transition-all duration-500"
            />
          )}

          {/* Lines */}
          {visibleSeries.unmatched && (
            <path
              d={getPath("yUnmatched")}
              fill="none"
              stroke="#6366f1"
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="opacity-30 transition-all duration-500"
            />
          )}
          {visibleSeries.matched && (
            <path
              d={getPath("yMatched")}
              fill="none"
              stroke="#10b981"
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="transition-all duration-500"
            />
          )}
          {visibleSeries.errored && (
            <path
              d={getPath("yErrored")}
              fill="none"
              stroke="#ef4444"
              strokeWidth="2"
              strokeDasharray="4 4"
              className="transition-all duration-500"
            />
          )}

          {/* Vertical Line on Hover */}
          {hoveredPoint !== null && (
            <line
              x1={points[hoveredPoint].x}
              y1={padding}
              x2={points[hoveredPoint].x}
              y2={height - padding}
              stroke="#cbd5e1"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
          )}

          {/* Interaction Circles */}
          {points.map((p, i) => (
            <g
              key={i}
              onMouseEnter={() => setHoveredPoint(i)}
              onMouseLeave={() => setHoveredPoint(null)}
            >
              {visibleSeries.matched && (
                <circle
                  cx={p.x}
                  cy={p.yMatched}
                  r={hoveredPoint === i ? "6" : "4"}
                  className="fill-emerald-500 stroke-white stroke-2 transition-all cursor-pointer"
                />
              )}
              {visibleSeries.unmatched && (
                <circle
                  cx={p.x}
                  cy={p.yUnmatched}
                  r={hoveredPoint === i ? "6" : "4"}
                  className="fill-indigo-500 stroke-white stroke-2 opacity-50 transition-all cursor-pointer"
                />
              )}
              {visibleSeries.errored && (
                <circle
                  cx={p.x}
                  cy={p.yErrored}
                  r={hoveredPoint === i ? "6" : "4"}
                  className="fill-red-500 stroke-white stroke-2 transition-all cursor-pointer"
                />
              )}

              {/* X Axis Labels */}
              {(i === 0 ||
                i === points.length - 1 ||
                (points.length > 10 && i % Math.floor(points.length / 5) === 0)) && (
                <text
                  x={p.x}
                  y={height - padding + 20}
                  textAnchor="middle"
                  className="text-[10px] fill-gray-400 font-bold"
                >
                  {p.date}
                </text>
              )}

              {/* Transparent hit area for hover */}
              <rect
                x={p.x - (width - 2 * padding) / (2 * (points.length - 1 || 1))}
                y={padding}
                width={(width - 2 * padding) / (points.length - 1 || 1)}
                height={height - 2 * padding}
                fill="transparent"
                className="cursor-crosshair"
              />
            </g>
          ))}
        </svg>

        {/* Custom Tooltip */}
        {hoveredPoint !== null && (
          <div
            className="absolute z-10 bg-white shadow-xl border border-gray-100 rounded-2xl p-4 pointer-events-none min-w-[200px] animate-in fade-in zoom-in duration-200"
            style={{
              left: `${(points[hoveredPoint].x / width) * 100}%`,
              top: `${(points[hoveredPoint].yVal / height) * 100}%`,
              transform: "translate(-50%, -120%)",
            }}
          >
            <p className="text-[10px] font-black text-gray-400 uppercase tracking-widest mb-2 pb-2 border-b border-gray-50">
              {points[hoveredPoint].fullDate}
            </p>
            <div className="space-y-2">
              {visibleSeries.matched && (
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
                    <span className="text-xs font-bold text-gray-600">Matched</span>
                  </div>
                  <span className="text-sm font-black text-emerald-600">
                    {points[hoveredPoint].data.matched}
                  </span>
                </div>
              )}
              {visibleSeries.unmatched && (
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-indigo-500"></div>
                    <span className="text-xs font-bold text-gray-600">Unmatched</span>
                  </div>
                  <span className="text-sm font-black text-indigo-600">
                    {points[hoveredPoint].data.unmatched}
                  </span>
                </div>
              )}
              {visibleSeries.errored && (
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-red-500"></div>
                    <span className="text-xs font-bold text-gray-600">Errored</span>
                  </div>
                  <span className="text-sm font-black text-red-600">
                    {points[hoveredPoint].data.errored}
                  </span>
                </div>
              )}
              {selectedRule === "all" && (
                <div className="mt-2 pt-2 border-t border-gray-50">
                  <p className="text-[10px] font-bold text-gray-400 italic">
                    Aggregated for all rules
                  </p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AnalyticsDashboard;
