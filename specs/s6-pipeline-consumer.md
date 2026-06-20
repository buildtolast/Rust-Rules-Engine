# Spec: S6 — EOS pipeline consumer (crates/pipeline/src/consumer.rs)

Generate the complete contents of `crates/pipeline/src/consumer.rs`.

Output EXACTLY ONE fenced Rust code block. No prose before or after it.

---

## Role

This module implements the pipeline:
  source-events topic → evaluate rules → produce matched events to target-events → write all audits to ClickHouse

EOS (exactly-once semantics): each Kafka transaction covers one source message and
produces to the target topic + commits the consumer offset atomically. ClickHouse
writes are at-least-once (idempotent via ReplacingMergeTree dedup on audit_id).

---

## Dependencies available in pipeline/Cargo.toml

```
rules_core::{AuditRecord, AuditType, SourceEvent, audit_id}
eval::{evaluate, CompiledRule}               ← evaluate(compiled, event) -> RuleOutcome
crate::rule_cache::RuleCache                 ← cache.get() -> Arc<Vec<CompiledRule>>
store_clickhouse::{AuditWriter, ClickHouseConfig, client as ch_client}
rdkafka (workspace, version 0.36, features = ["tokio"])
tokio (workspace)
thiserror (workspace)
tracing (workspace)
chrono::{DateTime, Utc}
```

---

## Types to define

### PipelineConfig

```rust
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub brokers:          String,
    pub source_topic:     String,
    pub target_topic:     String,
    pub consumer_group:   String,
    pub transactional_id: String,
    pub schema_version:   u32,
}
```

### PipelineError

```rust
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] store_clickhouse::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
```

---

## Main function signature

```rust
pub async fn run(
    config: PipelineConfig,
    cache: RuleCache,
    ch_cfg: store_clickhouse::ClickHouseConfig,
) -> Result<(), PipelineError>
```

---

## Implementation — two concurrent tasks

### Task A: ClickHouse writer (async, tokio::spawn)

```rust
let (ch_tx, mut ch_rx) = tokio::sync::mpsc::channel::<rules_core::AuditRecord>(2000);

let ch_handle = tokio::spawn(async move {
    let client = store_clickhouse::client(&ch_cfg);
    let mut writer = store_clickhouse::AuditWriter::new(&client, &ch_cfg);
    while let Some(rec) = ch_rx.recv().await {
        if let Err(e) = writer.write(&rec).await {
            tracing::warn!("clickhouse write error: {e}");
            // continue — at-least-once; CH deduplicates on audit_id
        }
    }
    writer.end().await.map_err(PipelineError::ClickHouse)
});
```

When `ch_tx` is dropped (pipeline ends or errors), `ch_rx.recv()` returns `None`
and the writer ends cleanly.

### Task B: Consumer-producer loop (tokio::task::spawn_blocking)

All rdkafka calls are blocking. Run in a dedicated blocking thread.
Communicate with Task A via `ch_tx.blocking_send(audit)`.

```rust
let pipeline_handle = tokio::task::spawn_blocking(move || -> Result<(), PipelineError> {
    // ── 1. Build consumer ────────────────────────────────────────────────────
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{BaseProducer, BaseRecord};
    use rdkafka::{Message, TopicPartitionList};
    use std::time::Duration;

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.consumer_group)
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[config.source_topic.as_str()])?;

    // ── 2. Build transactional producer ─────────────────────────────────────
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("enable.idempotence", "true")
        .set("transactional.id", &config.transactional_id)
        .set("acks", "all")
        .create()?;

    producer.init_transactions(Duration::from_secs(30))?;

    tracing::info!(
        brokers = %config.brokers,
        source = %config.source_topic,
        target = %config.target_topic,
        group = %config.consumer_group,
        "pipeline started"
    );

    // ── 3. Poll loop ─────────────────────────────────────────────────────────
    loop {
        let msg = match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::warn!("consumer poll error: {e}");
                continue;
            }
            None => continue,   // timeout, no messages available
        };

        let topic     = msg.topic().to_string();
        let partition = msg.partition();
        let offset    = msg.offset();
        let ts_ms     = msg.timestamp()
                           .to_millis()
                           .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        let raw = match msg.payload_view::<str>() {
            Some(Ok(s)) => s.to_string(),
            _ => {
                tracing::warn!(topic, partition, offset, "non-UTF-8 payload, skipping");
                // still commit the offset so we don't re-read this bad message
                producer.begin_transaction()?;
                let mut offsets = TopicPartitionList::new();
                offsets.add_partition_offset(&topic, partition, offset + 1)
                    .expect("add_partition_offset");
                let cgm = consumer.group_metadata().expect("group_metadata");
                producer.send_offsets_to_transaction(&offsets, &cgm, Duration::from_secs(10))?;
                producer.commit_transaction(Duration::from_secs(10))?;
                continue;
            }
        };

        // ── parse ─────────────────────────────────────────────────────────────
        let parse_start = std::time::Instant::now();
        let event = match rules_core::SourceEvent::from_kafka(&topic, partition, offset, ts_ms, &raw) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(topic, partition, offset, "parse error: {e}, skipping");
                continue;
            }
        };
        let parse_ns = parse_start.elapsed().as_nanos() as u64;

        // ── evaluate all rules ────────────────────────────────────────────────
        let rules = cache.get();
        let process_start = std::time::Instant::now();

        producer.begin_transaction()?;

        for compiled in rules.iter() {
            let outcome = eval::evaluate(compiled, &event);
            let eval_ns = outcome.eval_time_nano;
            let total_ns = parse_ns + eval_ns;

            let routed_event = if outcome.result.audit_type == rules_core::AuditType::Matched {
                Some(event.raw.clone())
            } else {
                None
            };

            // produce matched event to target topic (inside EOS transaction)
            if let Some(ref routed) = routed_event {
                let rec: BaseRecord<str, str> = BaseRecord::to(&config.target_topic)
                    .payload(routed.as_str());
                if let Err((e, _)) = producer.send(rec) {
                    tracing::warn!("producer send error: {e}");
                }
            }

            let audit = rules_core::AuditRecord {
                audit_id:       rules_core::audit_id(&topic, partition, offset, &compiled.id),
                rule_id:        compiled.id.clone(),
                schema_version: config.schema_version,
                audit_type:     outcome.result.audit_type,
                reason:         outcome.result.reason,
                source_event:   event.raw.clone(),
                routed_event:   routed_event.clone(),
                source_topic:   topic.clone(),
                partition,
                offset,
                timestamp:      chrono::DateTime::from_timestamp_millis(ts_ms)
                                    .unwrap_or_else(chrono::Utc::now),
                parse_time_nano: parse_ns,
                eval_time_nano:  eval_ns,
                total_time_nano: total_ns,
            };

            // send audit record to the async ClickHouse writer task via channel
            if ch_tx.blocking_send(audit).is_err() {
                tracing::warn!("ch channel closed, dropping audit");
            }
        }

        // ── flush + commit EOS ────────────────────────────────────────────────
        producer.flush(Duration::from_secs(5))?;

        let mut offsets = TopicPartitionList::new();
        offsets.add_partition_offset(&topic, partition, offset + 1)
            .expect("add_partition_offset");

        let cgm = consumer.group_metadata().expect("group_metadata");
        producer.send_offsets_to_transaction(&offsets, &cgm, Duration::from_secs(10))?;
        producer.commit_transaction(Duration::from_secs(10))?;

        let total_ms = process_start.elapsed().as_millis();
        tracing::debug!(
            topic, partition, offset,
            rules = rules.len(),
            total_ms,
            "message processed"
        );
    }
});
```

### Join both tasks

```rust
tokio::select! {
    r = pipeline_handle => r??,
    r = ch_handle       => { r??; Ok(()) },
}
```

---

## File structure

1. `use` imports at the top (only what is needed — no unused imports)
2. `PipelineConfig` struct
3. `PipelineError` enum
4. `pub async fn run(...)` implementing everything above

No `mod` declarations. No `main`. No `#[cfg(test)]` block. No other public items.

---

## Critical correctness notes the LLM must follow exactly

- `ch_tx` must be moved into the `spawn_blocking` closure so it is dropped when the pipeline ends.
- `consumer.group_metadata()` returns `KafkaResult<ConsumerGroupMetadata>` — call with `.expect()` inside the blocking closure.
- `offsets.add_partition_offset(topic, partition, offset + 1)` — committed offset is NEXT to consume, so `offset + 1`.
- `BaseRecord::<str, str>::to(topic).payload(payload)` — explicit type params avoid inference failures.
- `chrono::DateTime::from_timestamp_millis(ts_ms).unwrap_or_else(chrono::Utc::now)` — note `Utc::now` without `()` — it is passed as a function pointer, not called.
- Do NOT call `consumer.commit(...)` — offsets are committed atomically via `send_offsets_to_transaction`.
- Do NOT use `StreamConsumer` — use `BaseConsumer` inside `spawn_blocking`.
- All `?` inside the `spawn_blocking` closure propagate `PipelineError` — make sure From impls cover it.
- The `ch_tx` type is `tokio::sync::mpsc::Sender<rules_core::AuditRecord>` — use `blocking_send` inside the blocking closure.
