//! S6 — EOS pipeline: consume source-events → evaluate rules → route matched
//! events to target-events + write all audits to ClickHouse.
//!
//! Two-phase design for throughput:
//!   Phase 1 (parallel, rayon): parse JSON + evaluate all rules concurrently
//!             across all messages in the batch.
//!   Phase 2 (serial): one EOS Kafka transaction commits offsets for the batch;
//!             the ClickHouse writer drains the audit Vec asynchronously.
//!
//! Throughput knobs:
//!   BATCH_SIZE       — messages per Kafka transaction (default 2000)
//!   BATCH_TIMEOUT_MS — max wait to fill a partial batch  (default 100 ms)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use tokio::sync::mpsc;

use crate::metrics::PipelineCounters;
use crate::rule_cache::RuleCache;

const BATCH_SIZE: usize = 2_000;
const BATCH_TIMEOUT_MS: u64 = 100;

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

/// Owned copy of a Kafka message — safe to hand to rayon across thread boundaries.
struct OwnedMsg {
    topic: String,
    partition: i32,
    offset: i64,
    ts_ms: i64,
    raw: String,
}

/// Output of the parallel eval phase for one message.
struct MsgEval {
    topic: String,
    partition: i32,
    offset: i64,
    /// One entry per rule; None if the message failed to parse.
    rule_results: Option<Vec<(bool, rules_core::AuditRecord)>>,
}

pub async fn run(
    config: PipelineConfig,
    cache: RuleCache,
    ch_cfg: store_clickhouse::ClickHouseConfig,
    counters: Arc<PipelineCounters>,
) -> Result<(), PipelineError> {
    // Channel carries Vec<AuditRecord> per batch so the async writer always
    // receives whole batches and flushes them in one ClickHouse HTTP POST.
    let (ch_tx, mut ch_rx) = mpsc::channel::<Vec<rules_core::AuditRecord>>(32);

    // Task A: drain audit batches → ClickHouse.
    let ch_handle = tokio::spawn(async move {
        let client = store_clickhouse::client(&ch_cfg);
        let mut writer = store_clickhouse::AuditWriter::new(&client, &ch_cfg);
        while let Some(batch) = ch_rx.recv().await {
            if let Err(e) = writer.write_batch(&batch).await {
                tracing::warn!("clickhouse write error: {e}");
            }
        }
        writer.end().await.map_err(PipelineError::ClickHouse)
    });

    // Task B: rdkafka EOS loop.
    let pipeline_handle = tokio::task::spawn_blocking(move || -> Result<(), PipelineError> {
        let counters = counters;
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "earliest")
            .set("fetch.min.bytes", "262144")
            .set("fetch.wait.max.ms", "50")
            .set("max.poll.interval.ms", "300000")
            .create()?;

        consumer.subscribe(&[config.source_topic.as_str()])?;

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("enable.idempotence", "true")
            .set("transactional.id", &config.transactional_id)
            .set("acks", "all")
            .set("batch.size", "524288")
            .set("linger.ms", "5")
            .create()?;

        producer.init_transactions(Duration::from_secs(30))?;

        tracing::info!(
            brokers = %config.brokers,
            source  = %config.source_topic,
            target  = %config.target_topic,
            group   = %config.consumer_group,
            batch   = BATCH_SIZE,
            "pipeline started"
        );

        loop {
            // ── Phase 0: collect a batch of raw messages ──────────────────
            let mut batch: Vec<OwnedMsg> = Vec::with_capacity(BATCH_SIZE);
            let deadline = Instant::now() + Duration::from_millis(BATCH_TIMEOUT_MS);

            while batch.len() < BATCH_SIZE {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }

                match consumer.poll(remaining.min(Duration::from_millis(10))) {
                    Some(Ok(m)) => {
                        let ts_ms = m
                            .timestamp()
                            .to_millis()
                            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                        match m.payload_view::<str>() {
                            Some(Ok(s)) => batch.push(OwnedMsg {
                                topic: m.topic().to_string(),
                                partition: m.partition(),
                                offset: m.offset(),
                                ts_ms,
                                raw: s.to_string(),
                            }),
                            _ => tracing::warn!(
                                topic = m.topic(),
                                partition = m.partition(),
                                offset = m.offset(),
                                "non-UTF-8 payload, skipping"
                            ),
                        }
                    }
                    Some(Err(e)) => tracing::warn!("consumer poll error: {e}"),
                    None => {}
                }
            }

            if batch.is_empty() {
                continue;
            }

            let batch_span = tracing::info_span!(
                "pipeline.batch",
                "batch.size" = batch.len() as i64,
                "batch.eval_ms" = tracing::field::Empty,
                "batch.txn_ms" = tracing::field::Empty,
                "kafka.lag" = tracing::field::Empty,
                "audit.count" = tracing::field::Empty,
            );
            let _batch_enter = batch_span.enter();

            // ── Phase 1: parallel parse + eval (rayon) ────────────────────
            let rules: Arc<Vec<eval::CompiledRule>> = cache.get();
            let schema_version = config.schema_version;

            let eval_start = Instant::now();
            let msg_evals: Vec<MsgEval> = batch
                .par_iter()
                .map(|msg| {
                    let _parse_span = tracing::debug_span!("event.parse", "kafka.offset" = msg.offset).entered();
                    let parse_start = Instant::now();
                    let event = match rules_core::SourceEvent::from_kafka(
                        &msg.topic,
                        msg.partition,
                        msg.offset,
                        msg.ts_ms,
                        &msg.raw,
                    ) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(
                                topic = %msg.topic, partition = msg.partition,
                                offset = msg.offset, "parse error: {e}"
                            );
                            return MsgEval {
                                topic: msg.topic.clone(),
                                partition: msg.partition,
                                offset: msg.offset,
                                rule_results: None,
                            };
                        }
                    };
                    let parse_ns = parse_start.elapsed().as_nanos() as u64;

                    let rule_results = rules
                        .iter()
                        .map(|compiled| {
                            let outcome = eval::evaluate(compiled, &event);
                            let matched =
                                outcome.result.audit_type == rules_core::AuditType::Matched;
                            let routed = if matched {
                                Some(event.raw.clone())
                            } else {
                                None
                            };
                            let eval_ns = outcome.eval_time_nano;
                            let audit = rules_core::AuditRecord {
                                audit_id: rules_core::audit_id(
                                    &msg.topic,
                                    msg.partition,
                                    msg.offset,
                                    &compiled.id,
                                ),
                                rule_id: compiled.id.clone(),
                                schema_version,
                                audit_type: outcome.result.audit_type,
                                reason: outcome.result.reason,
                                source_event: event.raw.clone(),
                                routed_event: routed,
                                source_topic: msg.topic.clone(),
                                partition: msg.partition,
                                offset: msg.offset,
                                timestamp: chrono::DateTime::from_timestamp_millis(msg.ts_ms)
                                    .unwrap_or_else(chrono::Utc::now),
                                parse_time_nano: parse_ns,
                                eval_time_nano: eval_ns,
                                total_time_nano: parse_ns + eval_ns,
                            };
                            (matched, audit)
                        })
                        .collect();

                    MsgEval {
                        topic: msg.topic.clone(),
                        partition: msg.partition,
                        offset: msg.offset,
                        rule_results: Some(rule_results),
                    }
                })
                .collect();

            let eval_ms = eval_start.elapsed().as_millis();
            batch_span.record("batch.eval_ms", eval_ms as i64);

            // ── Phase 2: EOS transaction (serial) ─────────────────────────
            let txn_start = Instant::now();
            producer.begin_transaction()?;

            let mut last_offsets: HashMap<(String, i32), i64> = HashMap::new();
            for ev in &msg_evals {
                last_offsets.insert((ev.topic.clone(), ev.partition), ev.offset);
                if let Some(ref results) = ev.rule_results {
                    for (matched, audit) in results {
                        if *matched {
                            let rec: BaseRecord<str, str> = BaseRecord::to(&config.target_topic)
                                .payload(audit.source_event.as_str());
                            if let Err((e, _)) = producer.send(rec) {
                                tracing::warn!("producer send error: {e}");
                            }
                        }
                    }
                }
            }

            let mut tpl = TopicPartitionList::new();
            for ((topic, partition), offset) in &last_offsets {
                tpl.add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
                    .expect("add_partition_offset");
            }
            let cgm = consumer.group_metadata().expect("group_metadata");
            producer.send_offsets_to_transaction(&tpl, &cgm, Duration::from_secs(10))?;
            producer.commit_transaction(Duration::from_secs(10))?;

            let txn_ms = txn_start.elapsed().as_millis();
            batch_span.record("batch.txn_ms", txn_ms as i64);

            // ── Phase 3: ship audits to ClickHouse ────────────────────────
            let audits: Vec<rules_core::AuditRecord> = msg_evals
                .into_iter()
                .filter_map(|ev| ev.rule_results)
                .flatten()
                .map(|(_, audit)| audit)
                .collect();

            let audit_count = audits.len();
            batch_span.record("audit.count", audit_count as i64);
            if ch_tx.blocking_send(audits).is_err() {
                tracing::warn!("ch channel closed, dropping audit batch");
            }

            // Compute total consumer lag across all assigned partitions.
            let lag: i64 = last_offsets
                .iter()
                .map(|((topic, partition), committed)| {
                    consumer
                        .fetch_watermarks(topic, *partition, Duration::from_millis(200))
                        .map(|(_low, high)| (high - committed).max(0))
                        .unwrap_or(0)
                })
                .sum();
            batch_span.record("kafka.lag", lag);

            counters.record_batch(batch.len() as u64, eval_ms as u64, txn_ms as u64, lag);

            tracing::debug!(
                messages = batch.len(),
                audits = audit_count,
                eval_ms,
                txn_ms,
                lag,
                "batch processed"
            );
        }
    });

    tokio::select! {
        r = pipeline_handle => r?,
        r = ch_handle => r?,
    }
}
