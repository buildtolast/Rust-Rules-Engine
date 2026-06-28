use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// Atomic counters updated by the pipeline consumer loop.
/// Cheap to read from the HTTP handler thread — all fields are lock-free.
pub struct PipelineCounters {
    pub messages_total: AtomicU64,
    pub batches_total: AtomicU64,
    pub eval_ms_total: AtomicU64,
    pub txn_ms_total: AtomicU64,
    pub consumer_lag: AtomicI64,
    /// Number of audit batches written to the WAL but not yet confirmed written
    /// to ClickHouse. Driven by `wal::run_writer`; exposed here for health/metrics.
    pub ch_backlog_batches: AtomicI32,
    pub started_at: Instant,
}

impl Default for PipelineCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineCounters {
    pub fn new() -> Self {
        Self {
            messages_total: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            eval_ms_total: AtomicU64::new(0),
            txn_ms_total: AtomicU64::new(0),
            consumer_lag: AtomicI64::new(0),
            ch_backlog_batches: AtomicI32::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn record_batch(&self, messages: u64, eval_ms: u64, txn_ms: u64, lag: i64) {
        self.messages_total.fetch_add(messages, Relaxed);
        self.batches_total.fetch_add(1, Relaxed);
        self.eval_ms_total.fetch_add(eval_ms, Relaxed);
        self.txn_ms_total.fetch_add(txn_ms, Relaxed);
        self.consumer_lag.store(lag, Relaxed);
    }

    pub fn messages_per_sec(&self) -> f64 {
        let secs = self.started_at.elapsed().as_secs_f64();
        if secs < 1.0 {
            return 0.0;
        }
        self.messages_total.load(Relaxed) as f64 / secs
    }

    pub fn avg_eval_ms(&self) -> u64 {
        let batches = self.batches_total.load(Relaxed);
        if batches == 0 {
            return 0;
        }
        self.eval_ms_total.load(Relaxed) / batches
    }

    pub fn avg_txn_ms(&self) -> u64 {
        let batches = self.batches_total.load(Relaxed);
        if batches == 0 {
            return 0;
        }
        self.txn_ms_total.load(Relaxed) / batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    #[test]
    fn new_starts_with_all_zeros() {
        let c = PipelineCounters::new();
        assert_eq!(c.messages_total.load(Relaxed), 0);
        assert_eq!(c.batches_total.load(Relaxed), 0);
        assert_eq!(c.eval_ms_total.load(Relaxed), 0);
        assert_eq!(c.txn_ms_total.load(Relaxed), 0);
        assert_eq!(c.consumer_lag.load(Relaxed), 0);
        assert_eq!(c.ch_backlog_batches.load(Relaxed), 0);
    }

    #[test]
    fn record_batch_increments_messages_and_batches() {
        let c = PipelineCounters::new();
        c.record_batch(10, 5, 3, 0);
        c.record_batch(20, 5, 3, 0);
        assert_eq!(c.messages_total.load(Relaxed), 30);
        assert_eq!(c.batches_total.load(Relaxed), 2);
    }

    #[test]
    fn avg_eval_ms_zero_when_no_batches() {
        assert_eq!(PipelineCounters::new().avg_eval_ms(), 0);
    }

    #[test]
    fn avg_txn_ms_zero_when_no_batches() {
        assert_eq!(PipelineCounters::new().avg_txn_ms(), 0);
    }

    #[test]
    fn avg_eval_ms_correct_after_multiple_batches() {
        let c = PipelineCounters::new();
        c.record_batch(1, 10, 0, 0);
        c.record_batch(1, 20, 0, 0);
        c.record_batch(1, 30, 0, 0);
        assert_eq!(c.avg_eval_ms(), 20);
    }

    #[test]
    fn avg_txn_ms_correct_after_multiple_batches() {
        let c = PipelineCounters::new();
        c.record_batch(1, 0, 6, 0);
        c.record_batch(1, 0, 12, 0);
        assert_eq!(c.avg_txn_ms(), 9);
    }

    #[test]
    fn consumer_lag_stores_latest_not_accumulated() {
        let c = PipelineCounters::new();
        c.record_batch(1, 0, 0, 100);
        c.record_batch(1, 0, 0, 42);
        assert_eq!(c.consumer_lag.load(Relaxed), 42);
    }

    #[test]
    fn default_same_as_new() {
        let d = PipelineCounters::default();
        assert_eq!(d.messages_total.load(Relaxed), 0);
        assert_eq!(d.batches_total.load(Relaxed), 0);
    }

    #[test]
    fn messages_per_sec_near_zero_immediately_after_creation() {
        // elapsed < 1s so returns 0.0 regardless of message count
        assert_eq!(PipelineCounters::new().messages_per_sec(), 0.0);
    }
}
