CREATE TABLE IF NOT EXISTS ruleaudit.sre_observations (
    observed_at     DateTime64(3),
    container_name  LowCardinality(String),
    severity        LowCardinality(String),
    category        LowCardinality(String),
    finding         String,
    proposed_fix    String,
    log_window_hash String,
    log_snippet     String
) ENGINE = ReplacingMergeTree(observed_at)
  ORDER BY (container_name, log_window_hash)
  PARTITION BY toYYYYMM(observed_at)
  TTL toDateTime(observed_at) + INTERVAL 30 DAY;
