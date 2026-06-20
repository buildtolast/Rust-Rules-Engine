CREATE TABLE IF NOT EXISTS sre_outages
(
    container_name  LowCardinality(String),
    event_type      UInt8,
    occurred_at     DateTime64(3, 'UTC'),
    auto_restarted  Bool
)
ENGINE = MergeTree()
ORDER BY (container_name, occurred_at);
