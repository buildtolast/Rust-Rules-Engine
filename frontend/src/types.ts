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
