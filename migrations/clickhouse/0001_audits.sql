-- S3: audits table. One row per (event x rule) evaluation.
--
-- Engine: ReplacingMergeTree. Dedup collapses rows sharing the full ORDER BY
-- key. The audit identity is auditId = {topic}:{partition}:{offset}:{ruleId},
-- so the ORDER BY is exactly those identity components — NOT (timestamp, ruleId),
-- because `timestamp` is processing wall-clock and a reprocessed event keeps the
-- same identity but a different timestamp, which would defeat dedup.
--
-- Analytics is served by S4 materialized views, not by scanning this raw table,
-- so the raw table is optimized for dedup correctness rather than time-range reads.
--
-- auditType is LowCardinality(String) (values MATCHED/UNMATCHED/ERRORED) rather
-- than Enum8: a single source of truth (rules_core::AuditType -> &str) avoids a
-- parallel serde_repr enum; LowCardinality compresses comparably for 3 values.
--
-- reason/routedEvent are NOT NULL columns; absent values are stored as ''.

CREATE TABLE IF NOT EXISTS audits
(
    audit_id         String,
    rule_id          LowCardinality(String),
    schema_version   UInt32,
    audit_type       LowCardinality(String),
    reason           String,
    source_event     String,
    routed_event     String,
    source_topic     LowCardinality(String),
    partition        Int32,
    offset           Int64,
    timestamp        DateTime64(3),
    parse_time_nano  UInt64,
    eval_time_nano   UInt64,
    total_time_nano  UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (source_topic, partition, offset, rule_id)
PARTITION BY toYYYYMM(timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
