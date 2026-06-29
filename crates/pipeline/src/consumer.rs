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
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
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
const HEALTH_CHECK_INTERVAL_SECS: u64 = 15;
const HEALTH_CHECK_TIMEOUT_SECS: u64 = 2;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub brokers: String,
    pub source_topic: String,
    pub target_topic: String,
    pub consumer_group: String,
    pub transactional_id: String,
    pub schema_version: u32,
    pub database_url: String,
    pub clickhouse_url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] store_clickhouse::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("consumer group metadata unavailable")]
    NoGroupMetadata,
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

/// Row written to `ruleaudit.pipeline_lag`.
#[derive(Debug, Clone, clickhouse::Row, serde::Serialize)]
struct LagRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    ts: chrono::DateTime<chrono::Utc>,
    consumer_group: String,
    total_lag: i64,
    ch_backlog_batches: i32,
    batch_size: i32,
    eval_ms: i64,
    txn_ms: i64,
}

pub async fn run(config: PipelineConfig,
                 cache: RuleCache,
                 ch_cfg: store_clickhouse::ClickHouseConfig,
                 counters: Arc<PipelineCounters>)
                 -> Result<(), PipelineError> {
    // ── Shared health flags ──────────────────────────────────────────────────
    let postgres_healthy = Arc::new(AtomicBool::new(true));
    let clickhouse_healthy = Arc::new(AtomicBool::new(true));

    // ── Shared backlog counter (written by WAL writer, read by loop) ─────────
    let backlog_arc: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));

    // ── Task A: WAL-backed ClickHouse audit writer ───────────────────────────
    let (ch_tx, ch_rx) = mpsc::channel::<Vec<rules_core::AuditRecord>>(32);
    let backlog_writer = Arc::clone(&backlog_arc);

    let ch_handle = {
        let cfg = ch_cfg.clone();
        tokio::spawn(async move {
            crate::wal::run_writer(ch_rx, cfg, backlog_writer).await;
            Ok::<(), PipelineError>(())
        })
    };

    // ── Task B: per-dependency health checker ────────────────────────────────
    let pg_flag = Arc::clone(&postgres_healthy);
    let ch_flag = Arc::clone(&clickhouse_healthy);
    let database_url = config.database_url.clone();
    let clickhouse_url_health = config.clickhouse_url.clone();

    let health_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(HEALTH_CHECK_INTERVAL_SECS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            let pg_ok = check_postgres_tcp(&database_url).await;
            if pg_ok != pg_flag.load(Ordering::Relaxed) {
                tracing::info!(healthy = pg_ok, "postgres health changed");
                pg_flag.store(pg_ok, Ordering::Relaxed);
            }

            let ch_ok = check_clickhouse_ping(&clickhouse_url_health).await;
            if ch_ok != ch_flag.load(Ordering::Relaxed) {
                tracing::info!(healthy = ch_ok, "clickhouse health changed");
                ch_flag.store(ch_ok, Ordering::Relaxed);
            }
        }
    });

    // ── Task C: fire-and-forget lag writer ───────────────────────────────────
    let (lag_tx, mut lag_rx) = mpsc::channel::<LagRow>(64);
    let lag_ch_cfg = ch_cfg.clone();
    let lag_handle = tokio::spawn(async move {
        let client = store_clickhouse::client(&lag_ch_cfg);
        while let Some(row) = lag_rx.recv().await {
            let result = async {
                let mut insert = client.insert::<LagRow>("pipeline_lag").await?;
                insert.write(&row).await?;
                insert.end().await
            };
            if let Err(e) = result.await {
                tracing::debug!("lag row write failed (non-critical): {e}");
            }
        }
        Ok::<(), PipelineError>(())
    });

    // ── Task D: rdkafka EOS loop ─────────────────────────────────────────────
    let pg_flag_loop = Arc::clone(&postgres_healthy);
    let backlog_loop = Arc::clone(&backlog_arc);
    let pipeline_handle = tokio::task::spawn_blocking(move || -> Result<(), PipelineError> {
        let counters = counters;
        let consumer: BaseConsumer = ClientConfig::new().set("bootstrap.servers", &config.brokers)
                                                        .set("group.id", &config.consumer_group)
                                                        .set("enable.auto.commit", "false")
                                                        .set("isolation.level", "read_committed")
                                                        .set("auto.offset.reset", "earliest")
                                                        .set("fetch.min.bytes", "262144")
                                                        .set("fetch.wait.max.ms", "50")
                                                        .set("max.poll.interval.ms", "300000")
                                                        .create()?;

        consumer.subscribe(&[config.source_topic.as_str()])?;

        let producer: BaseProducer =
            ClientConfig::new().set("bootstrap.servers", &config.brokers)
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

        let mut was_paused = false;

        loop {
            // ── Postgres health gate ──────────────────────────────────────────
            let pg_ok = pg_flag_loop.load(Ordering::Relaxed);
            if !pg_ok && !was_paused {
                let all_partitions = consumer_all_partitions(&consumer, &config.source_topic);
                if let Err(e) = consumer.pause(&all_partitions) {
                    tracing::warn!("failed to pause consumer: {e}");
                }
                tracing::warn!("postgres unhealthy — consumer paused");
                was_paused = true;
            } else if pg_ok && was_paused {
                let all_partitions = consumer_all_partitions(&consumer, &config.source_topic);
                if let Err(e) = consumer.resume(&all_partitions) {
                    tracing::warn!("failed to resume consumer: {e}");
                }
                tracing::info!("postgres healthy — consumer resumed");
                was_paused = false;
            }

            if was_paused {
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }

            // ── Phase 0: collect a batch of raw messages ──────────────────────
            let mut batch: Vec<OwnedMsg> = Vec::with_capacity(BATCH_SIZE);
            let deadline = Instant::now() + Duration::from_millis(BATCH_TIMEOUT_MS);

            while batch.len() < BATCH_SIZE {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }

                match consumer.poll(remaining.min(Duration::from_millis(10))) {
                    Some(Ok(m)) => {
                        let ts_ms = m.timestamp()
                                     .to_millis()
                                     .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                        match m.payload_view::<str>() {
                            Some(Ok(s)) => batch.push(OwnedMsg { topic: m.topic().to_string(),
                                                                 partition: m.partition(),
                                                                 offset: m.offset(),
                                                                 ts_ms,
                                                                 raw: s.to_string() }),
                            _ => tracing::warn!(topic = m.topic(),
                                                partition = m.partition(),
                                                offset = m.offset(),
                                                "non-UTF-8 payload, skipping"),
                        }
                    }
                    Some(Err(e)) => tracing::warn!("consumer poll error: {e}"),
                    None => {}
                }
            }

            if batch.is_empty() {
                continue;
            }

            let batch_span = tracing::info_span!("pipeline.batch",
                                                 "batch.size" = batch.len() as i64,
                                                 "batch.eval_ms" = tracing::field::Empty,
                                                 "batch.txn_ms" = tracing::field::Empty,
                                                 "kafka.lag" = tracing::field::Empty,
                                                 "audit.count" = tracing::field::Empty,);
            let _batch_enter = batch_span.enter();

            // ── Phase 1: parallel parse + eval (rayon) ────────────────────────
            let rules: Arc<Vec<eval::CompiledRule>> = cache.get();
            let schema_version = config.schema_version;

            let eval_start = Instant::now();
            let msg_evals: Vec<MsgEval> =
                batch.par_iter()
                     .map(|msg| {
                         let _parse_span =
                        tracing::debug_span!("event.parse", "kafka.offset" = msg.offset).entered();
                         let parse_start = Instant::now();
                         let event = match rules_core::SourceEvent::from_kafka(&msg.topic,
                                                                               msg.partition,
                                                                               msg.offset,
                                                                               msg.ts_ms,
                                                                               &msg.raw)
                         {
                             Ok(e) => e,
                             Err(e) => {
                                 tracing::warn!(
                                     topic = %msg.topic, partition = msg.partition,
                                     offset = msg.offset, "parse error: {e}"
                                 );
                                 return MsgEval { topic: msg.topic.clone(),
                                                  partition: msg.partition,
                                                  offset: msg.offset,
                                                  rule_results: None };
                             }
                         };
                         let parse_ns = parse_start.elapsed().as_nanos() as u64;

                         let rule_results = rules.iter()
                                                 .map(|compiled| {
                                                     let outcome = eval::evaluate(compiled, &event);
                                                     let matched =
                                                         outcome.result.audit_type
                                                         == rules_core::AuditType::Matched;
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

                         MsgEval { topic: msg.topic.clone(),
                                   partition: msg.partition,
                                   offset: msg.offset,
                                   rule_results: Some(rule_results) }
                     })
                     .collect();

            let eval_ms = eval_start.elapsed().as_millis();
            batch_span.record("batch.eval_ms", eval_ms as i64);

            // ── Phase 2: EOS transaction (serial) ─────────────────────────────
            let txn_start = Instant::now();
            producer.begin_transaction()?;

            let mut last_offsets: HashMap<(String, i32), i64> = HashMap::new();
            for ev in &msg_evals {
                last_offsets.insert((ev.topic.clone(), ev.partition), ev.offset);
                if let Some(ref results) = ev.rule_results {
                    for (matched, audit) in results {
                        if *matched {
                            let rec: BaseRecord<str, str> =
                                BaseRecord::to(&config.target_topic).payload(audit.source_event
                                                                                  .as_str());
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
                   .map_err(PipelineError::Kafka)?;
            }
            let cgm = consumer.group_metadata()
                              .ok_or(PipelineError::NoGroupMetadata)?;
            producer.send_offsets_to_transaction(&tpl, &cgm, Duration::from_secs(10))?;
            producer.commit_transaction(Duration::from_secs(10))?;

            let txn_ms = txn_start.elapsed().as_millis();
            batch_span.record("batch.txn_ms", txn_ms as i64);

            // ── Phase 3: ship audits to ClickHouse (via WAL) ──────────────────
            let audits: Vec<rules_core::AuditRecord> = msg_evals.into_iter()
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
            let lag: i64 = last_offsets.iter()
                                       .map(|((topic, partition), committed)| {
                                           consumer.fetch_watermarks(topic,
                                                                     *partition,
                                                                     Duration::from_millis(200))
                                                   .map(|(_low, high)| (high - committed).max(0))
                                                   .unwrap_or(0)
                                       })
                                       .sum();
            batch_span.record("kafka.lag", lag);

            counters.record_batch(batch.len() as u64, eval_ms as u64, txn_ms as u64, lag);

            // ── Phase 4: fire-and-forget lag row to ClickHouse ────────────────
            let ch_backlog = backlog_loop.load(Ordering::Relaxed);
            counters.ch_backlog_batches
                    .store(ch_backlog, Ordering::Relaxed);
            let lag_row = LagRow { ts: chrono::Utc::now(),
                                   consumer_group: config.consumer_group.clone(),
                                   total_lag: lag,
                                   ch_backlog_batches: ch_backlog,
                                   batch_size: batch.len() as i32,
                                   eval_ms: eval_ms as i64,
                                   txn_ms: txn_ms as i64 };
            let _ = lag_tx.try_send(lag_row); // non-blocking; loss is acceptable

            tracing::debug!(messages = batch.len(),
                            audits = audit_count,
                            eval_ms,
                            txn_ms,
                            lag,
                            "batch processed");
        }
    });

    tokio::select! {
        r = pipeline_handle => r?,
        r = ch_handle => r?,
        r = lag_handle => r?,
        _ = health_handle => Ok(()),
    }
}

/// Build a TopicPartitionList covering all partitions of `topic` visible in metadata.
fn consumer_all_partitions(consumer: &BaseConsumer, topic: &str) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    if let Ok(meta) = consumer.fetch_metadata(Some(topic), Duration::from_secs(2)) {
        for t in meta.topics() {
            for p in t.partitions() {
                tpl.add_partition(t.name(), p.id());
            }
        }
    }
    tpl
}

/// TCP-connect check for Postgres: parse first host:port from DATABASE_URL.
/// DATABASE_URL format: postgres://user:pass@host:port/db
async fn check_postgres_tcp(database_url: &str) -> bool {
    let host_port = match database_url.split("://")
                                      .nth(1)
                                      .and_then(|rest| rest.split('@').nth(1))
                                      .and_then(|host_db| host_db.split('/').next())
    {
        Some(hp) => hp,
        None => return false,
    };

    let addrs: Vec<_> = match host_port.to_socket_addrs() {
        Ok(a) => a.collect(),
        Err(_) => return false,
    };
    for addr in addrs {
        let result = tokio::time::timeout(Duration::from_secs(HEALTH_CHECK_TIMEOUT_SECS),
                                          tokio::net::TcpStream::connect(addr)).await;
        if matches!(result, Ok(Ok(_))) {
            return true;
        }
    }
    false
}

/// HTTP GET {clickhouse_url}/ping with 2s timeout.
async fn check_clickhouse_ping(clickhouse_url: &str) -> bool {
    let url = format!("{clickhouse_url}/ping");
    let result = tokio::time::timeout(Duration::from_secs(HEALTH_CHECK_TIMEOUT_SECS),
                                      reqwest::get(&url)).await;
    matches!(result, Ok(Ok(r)) if r.status().is_success())
}

#[cfg(test)]
mod tests {
    use rules_core::audit_id;

    // ── Test 4: audit_id construction ────────────────────────────────────────

    #[test]
    fn test_audit_id_format() {
        let id = audit_id("source-events", 0, 42, "rule-1");
        assert_eq!(id, "source-events:0:42:rule-1");
    }

    #[test]
    fn test_audit_id_negative_partition_and_large_offset() {
        let id = audit_id("my-topic", 3, 1_000_000, "rule-abc");
        assert_eq!(id, "my-topic:3:1000000:rule-abc");
    }

    #[test]
    fn test_audit_id_uniqueness_by_rule() {
        let id_a = audit_id("events", 0, 10, "rule-A");
        let id_b = audit_id("events", 0, 10, "rule-B");
        assert_ne!(id_a, id_b,
                   "different rules on the same event must yield distinct audit IDs");
    }

    #[test]
    fn test_audit_id_uniqueness_by_offset() {
        let id_a = audit_id("events", 0, 1, "rule-X");
        let id_b = audit_id("events", 0, 2, "rule-X");
        assert_ne!(id_a, id_b,
                   "same rule on different offsets must yield distinct audit IDs");
    }

    #[test]
    fn test_audit_id_uniqueness_by_partition() {
        let id_a = audit_id("events", 0, 5, "rule-X");
        let id_b = audit_id("events", 1, 5, "rule-X");
        assert_ne!(id_a, id_b,
                   "same rule/offset on different partitions must yield distinct IDs");
    }
}
