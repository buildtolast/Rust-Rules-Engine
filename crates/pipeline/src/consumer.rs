//! S6 — EOS pipeline: consume source-events → evaluate rules → route matched
//! events to target-events + write all audits to ClickHouse.
//!
//! EOS covers Kafka-to-Kafka: one transaction per message, consumer offsets
//! committed atomically with the produce. ClickHouse writes are at-least-once
//! (ReplacingMergeTree deduplicates on audit_id).

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::sync::mpsc;

use crate::rule_cache::RuleCache;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub brokers: String,
    pub source_topic: String,
    pub target_topic: String,
    pub consumer_group: String,
    pub transactional_id: String,
    pub schema_version: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] store_clickhouse::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub async fn run(
    config: PipelineConfig,
    cache: RuleCache,
    ch_cfg: store_clickhouse::ClickHouseConfig,
) -> Result<(), PipelineError> {
    let (ch_tx, mut ch_rx) = mpsc::channel::<rules_core::AuditRecord>(2000);

    // Task A: drain the audit channel and write to ClickHouse (async, at-least-once).
    let ch_handle = tokio::spawn(async move {
        let client = store_clickhouse::client(&ch_cfg);
        let mut writer = store_clickhouse::AuditWriter::new(&client, &ch_cfg);
        while let Some(rec) = ch_rx.recv().await {
            if let Err(e) = writer.write(&rec).await {
                tracing::warn!("clickhouse write error: {e}");
            }
        }
        writer.end().await.map_err(PipelineError::ClickHouse)
    });

    // Task B: rdkafka consumer-producer EOS loop (all blocking calls).
    // ch_tx is moved here so it drops when this task ends, closing the channel.
    let pipeline_handle = tokio::task::spawn_blocking(move || -> Result<(), PipelineError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(&[config.source_topic.as_str()])?;

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

        loop {
            let msg = match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    tracing::warn!("consumer poll error: {e}");
                    continue;
                }
                None => continue,
            };

            let topic = msg.topic().to_string();
            let partition = msg.partition();
            let offset = msg.offset();
            let ts_ms = msg
                .timestamp()
                .to_millis()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

            let raw = match msg.payload_view::<str>() {
                Some(Ok(s)) => s.to_string(),
                _ => {
                    tracing::warn!(topic, partition, offset, "non-UTF-8 payload, skipping");
                    // Commit the offset so we don't re-read this bad message.
                    producer.begin_transaction()?;
                    let mut offsets = TopicPartitionList::new();
                    offsets
                        .add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
                        .expect("add_partition_offset");
                    let cgm = consumer.group_metadata().expect("group_metadata");
                    producer.send_offsets_to_transaction(&offsets, &cgm, Duration::from_secs(10))?;
                    producer.commit_transaction(Duration::from_secs(10))?;
                    continue;
                }
            };

            let parse_start = Instant::now();
            let event =
                match rules_core::SourceEvent::from_kafka(&topic, partition, offset, ts_ms, &raw) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(topic, partition, offset, "JSON parse error: {e}, skipping");
                        continue;
                    }
                };
            let parse_ns = parse_start.elapsed().as_nanos() as u64;

            let rules = cache.get();
            let process_start = Instant::now();

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

                // Produce matched event to target topic inside the EOS transaction.
                if let Some(ref routed) = routed_event {
                    let rec: BaseRecord<str, str> =
                        BaseRecord::to(&config.target_topic).payload(routed.as_str());
                    if let Err((e, _)) = producer.send(rec) {
                        tracing::warn!("producer send error: {e}");
                    }
                }

                let audit = rules_core::AuditRecord {
                    audit_id: rules_core::audit_id(&topic, partition, offset, &compiled.id),
                    rule_id: compiled.id.clone(),
                    schema_version: config.schema_version,
                    audit_type: outcome.result.audit_type,
                    reason: outcome.result.reason,
                    source_event: event.raw.clone(),
                    routed_event,
                    source_topic: topic.clone(),
                    partition,
                    offset,
                    timestamp: chrono::DateTime::from_timestamp_millis(ts_ms)
                        .unwrap_or_else(chrono::Utc::now),
                    parse_time_nano: parse_ns,
                    eval_time_nano: eval_ns,
                    total_time_nano: total_ns,
                };

                if ch_tx.blocking_send(audit).is_err() {
                    tracing::warn!("ch channel closed, dropping audit");
                }
            }

            producer.flush(Duration::from_secs(5))?;

            let mut offsets = TopicPartitionList::new();
            offsets
                .add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
                .expect("add_partition_offset");

            let cgm = consumer.group_metadata().expect("group_metadata");
            producer.send_offsets_to_transaction(&offsets, &cgm, Duration::from_secs(10))?;
            producer.commit_transaction(Duration::from_secs(10))?;

            let total_ms = process_start.elapsed().as_millis();
            tracing::debug!(
                topic,
                partition,
                offset,
                rules = rules.len(),
                total_ms,
                "message processed"
            );
        }
    });

    tokio::select! {
        r = pipeline_handle => r?,
        r = ch_handle => r?,
    }
}
