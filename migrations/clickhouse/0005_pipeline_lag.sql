CREATE TABLE IF NOT EXISTS ruleaudit.pipeline_lag
(
    ts              DateTime64(3, 'UTC'),
    consumer_group  LowCardinality(String),
    total_lag       Int64,
    ch_backlog_batches Int32,
    batch_size      Int32,
    eval_ms         Int64,
    txn_ms          Int64
)
ENGINE = MergeTree()
ORDER BY (consumer_group, ts)
TTL ts + INTERVAL 7 DAY;
