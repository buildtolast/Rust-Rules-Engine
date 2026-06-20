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
  severity: 'INFO' | 'WARN' | 'ERROR' | 'CRITICAL';
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
}

export interface SreStatus {
  containers: SreContainerStatus[];
  llm_available: boolean;
  last_scan_at: string | null;
}

export interface AuditRecord {
  auditId: string;
  ruleId: string;
  auditType: 'Matched' | 'Unmatched' | 'Error';
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
