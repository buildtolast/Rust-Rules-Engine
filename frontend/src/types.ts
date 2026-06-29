export interface Rule {
  id?: string;
  description: string;
  expression: string;
  targetTopic: string;
  enabled: boolean;
  version?: number;
  updatedAt?: string;
}

export interface RuleStats {
  ruleId: string;
  matched: number;
  unmatched: number;
  errored: number;
}

export interface TimeSeriesPoint {
  ruleId: string;
  timestamp: string;
  matched: number;
  unmatched: number;
  errored: number;
}

export interface AnalyticsStats {
  totalMessages: number;
  totalEvaluations: number;
  ruleStats: RuleStats[];
  timeSeries: TimeSeriesPoint[];
  avgParseTimeNano: number;
  avgEvalTimeNano: number;
  avgTotalTimeNano: number;
}

export interface SreFinding {
  container_name: string;
  observed_at: string | null;
  severity: "INFO" | "WARN" | "ERROR" | "CRITICAL";
  category: string;
  finding: string;
  proposed_fix: string;
}

export interface SreContainerStatus {
  name: string;
  id: string;
  running: boolean;
  started_at: string | null;
  health: { status: string };
  last_checked_at: string;
  last_severity: string | null;
  cpu_percent: number;
  mem_used_bytes: number;
  mem_limit_bytes: number;
}

export interface SreStatus {
  containers: SreContainerStatus[];
  llm_available: boolean;
  last_scan_at: string | null;
}

export interface Incident {
  container: string;
  down_at: string;
  restored_at: string | null;
  auto_restarted: boolean;
  duration_secs: number | null;
  active: boolean;
}

export interface AuditRecord {
  auditId: string;
  ruleId: string;
  auditType: "MATCHED" | "UNMATCHED" | "ERRORED";
  reason?: string;
  sourceEvent: string;
  routedEvent?: string;
  sourceTopic: string;
  partition: number;
  offset: number;
  timestamp: string;
  parseTimeNano: number;
  evalTimeNano: number;
  totalTimeNano: number;
}

export interface RulePerf {
  rule_id: string;
  eval_count: number;
  avg_eval_ms: number;
  p95_eval_ms: number;
  p99_eval_ms: number;
  avg_parse_ms: number;
  error_rate_pct: number;
}

export interface TraceInsights {
  generated_at: string;
  window_minutes: number;
  rule_perf: RulePerf[];
  llm_insights: string[];
  llm_bottlenecks: string[];
  llm_recommendations: string[];
  llm_available: boolean;
  total_evals: number;
  avg_eval_ms_overall: number;
}
