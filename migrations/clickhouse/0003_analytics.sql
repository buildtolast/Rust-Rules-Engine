-- S4: analytics materialized views.
--
-- Replaces Java's RollupService (scheduled MongoDB aggregation + Redis lock) with
-- two ClickHouse target tables that are maintained incrementally by MVs on insert.
--
-- agg_rule_hourly  — per (hour, rule_id) counters + latency sums
--                    feeds: ruleStats, timeSeries, totalEvaluations, avg latency
-- agg_hour_messages — distinct source-event count per hour (HLL approximate)
--                    feeds: totalMessages
--
-- MVs trigger on every INSERT into audits; no scheduled job or lock needed.
-- Historical rows already in audits are backfilled by the INSERT…SELECT below.

-- ── Target table 1: per-rule hourly rollup ───────────────────────────────────

CREATE TABLE IF NOT EXISTS agg_rule_hourly
(
    hour          DateTime,
    rule_id       LowCardinality(String),
    matched       UInt64,
    unmatched     UInt64,
    errored       UInt64,
    eval_count    UInt64,
    parse_sum     UInt64,
    eval_sum      UInt64,
    total_sum     UInt64
)
ENGINE = SummingMergeTree((matched, unmatched, errored, eval_count, parse_sum, eval_sum, total_sum))
ORDER BY (hour, rule_id)
PARTITION BY toYYYYMM(hour)
TTL toDateTime(hour) + INTERVAL 90 DAY;

-- ── Target table 2: distinct messages per hour (HLL) ────────────────────────

CREATE TABLE IF NOT EXISTS agg_hour_messages
(
    hour      DateTime,
    msg_hll   AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree()
ORDER BY hour
PARTITION BY toYYYYMM(hour)
TTL toDateTime(hour) + INTERVAL 90 DAY;

-- ── Materialized view 1: feeds agg_rule_hourly ──────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rule_hourly
TO agg_rule_hourly
AS
SELECT
    toStartOfHour(timestamp)              AS hour,
    rule_id,
    countIf(audit_type = 'MATCHED')       AS matched,
    countIf(audit_type = 'UNMATCHED')     AS unmatched,
    countIf(audit_type = 'ERRORED')       AS errored,
    count()                               AS eval_count,
    sum(parse_time_nano)                  AS parse_sum,
    sum(eval_time_nano)                   AS eval_sum,
    sum(total_time_nano)                  AS total_sum
FROM ruleaudit.audits
GROUP BY hour, rule_id;

-- ── Materialized view 2: feeds agg_hour_messages ────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hour_messages
TO agg_hour_messages
AS
SELECT
    toStartOfHour(timestamp)                                        AS hour,
    uniqState(concat(source_topic, ':', toString(partition), ':', toString(offset))) AS msg_hll
FROM ruleaudit.audits
GROUP BY hour;

-- ── Backfill from existing audits rows ──────────────────────────────────────
-- MVs only capture future inserts; backfill brings historical data into the
-- aggregate tables so analytics queries return correct totals from day one.

INSERT INTO agg_rule_hourly
SELECT
    toStartOfHour(timestamp)          AS hour,
    rule_id,
    countIf(audit_type = 'MATCHED')   AS matched,
    countIf(audit_type = 'UNMATCHED') AS unmatched,
    countIf(audit_type = 'ERRORED')   AS errored,
    count()                           AS eval_count,
    sum(parse_time_nano)              AS parse_sum,
    sum(eval_time_nano)               AS eval_sum,
    sum(total_time_nano)              AS total_sum
FROM ruleaudit.audits
GROUP BY hour, rule_id;

INSERT INTO agg_hour_messages
SELECT
    toStartOfHour(timestamp)                                                         AS hour,
    uniqState(concat(source_topic, ':', toString(partition), ':', toString(offset))) AS msg_hll
FROM ruleaudit.audits
GROUP BY hour;
