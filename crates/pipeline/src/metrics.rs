use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// Atomic counters updated by the pipeline consumer loop.
/// Cheap to read from the HTTP handler thread — all fields are lock-free.
pub struct PipelineCounters {
    pub messages_total: AtomicU64,
    pub batches_total: AtomicU64,
    pub eval_ms_total: AtomicU64,
    pub txn_ms_total: AtomicU64,
    pub consumer_lag: AtomicI64,
    pub started_at: Instant,
}

impl PipelineCounters {
    pub fn new() -> Self {
        Self {
            messages_total: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            eval_ms_total: AtomicU64::new(0),
            txn_ms_total: AtomicU64::new(0),
            consumer_lag: AtomicI64::new(0),
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
