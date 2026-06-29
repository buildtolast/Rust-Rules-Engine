//! WAL-backed ClickHouse audit buffer.
//!
//! Each audit batch is appended to a file as newline-delimited JSON lines
//! followed by a sentinel `---BATCH_END---\n`. On startup any complete
//! uncommitted batches are replayed to ClickHouse, then the file is truncated.
//! After every successful ClickHouse write the file is fsynced and truncated.
//!
//! If the WAL file cannot be opened the module falls back to in-memory only
//! (audit records are still sent to ClickHouse, just without crash-recovery).

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use rules_core::AuditRecord;
use store_clickhouse::{AuditWriter, ClickHouseConfig};

const SENTINEL: &str = "---BATCH_END---";
const WAL_PATH_ENV: &str = "CH_WAL_PATH";
const WAL_PATH_DEFAULT: &str = "/data/ch-audit.wal";

fn wal_path() -> PathBuf {
    std::env::var(WAL_PATH_ENV)
        .unwrap_or_else(|_| WAL_PATH_DEFAULT.to_owned())
        .into()
}

/// Open or create the WAL file for append + read.
/// Returns `None` if the path cannot be created (logs error, does not panic).
fn open_wal(path: &PathBuf) -> Option<File> {
    match OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
    {
        Ok(f) => Some(f),
        Err(e) => {
            tracing::error!(
                path = %path.display(),
                "cannot open WAL file: {e} — falling back to in-memory only"
            );
            None
        }
    }
}

/// Read all complete batches from the WAL file (seeks to start).
fn read_batches(file: &mut File) -> Vec<Vec<AuditRecord>> {
    if let Err(e) = file.seek(SeekFrom::Start(0)) {
        tracing::warn!("WAL seek error during replay: {e}");
        return vec![];
    }
    let reader = BufReader::new(&*file);
    let mut batches: Vec<Vec<AuditRecord>> = Vec::new();
    let mut current: Vec<AuditRecord> = Vec::new();

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("WAL read error: {e}");
                break;
            }
        };
        if line == SENTINEL {
            if !current.is_empty() {
                batches.push(std::mem::take(&mut current));
            }
        } else if !line.is_empty() {
            match serde_json::from_str::<AuditRecord>(&line) {
                Ok(r) => current.push(r),
                Err(e) => tracing::warn!("WAL JSON parse error (skipping line): {e}"),
            }
        }
    }
    // Incomplete final batch (no sentinel) is dropped — not confirmed written.
    batches
}

/// Truncate the WAL to zero length (all prior batches confirmed written).
fn truncate_wal(file: &mut File) {
    if let Err(e) = file.seek(SeekFrom::Start(0)).and_then(|_| file.set_len(0)) {
        tracing::warn!("WAL truncate failed: {e}");
    } else if let Err(e) = file.sync_all() {
        tracing::warn!("WAL fsync after truncate failed: {e}");
    }
}

/// Append a batch to the WAL (newline-delimited JSON + sentinel).
fn append_batch(file: &mut File, batch: &[AuditRecord]) {
    let mut buf = String::new();
    for rec in batch {
        match serde_json::to_string(rec) {
            Ok(line) => {
                buf.push_str(&line);
                buf.push('\n');
            }
            Err(e) => tracing::warn!("WAL serialise error: {e}"),
        }
    }
    buf.push_str(SENTINEL);
    buf.push('\n');
    if let Err(e) = file.write_all(buf.as_bytes()) {
        tracing::warn!("WAL write error: {e}");
    }
}

/// Receive audit batches from the pipeline, WAL-back them, and write to ClickHouse
/// with exponential-backoff retries. Runs indefinitely until the sender is dropped.
pub async fn run_writer(
    mut ch_rx: tokio::sync::mpsc::Receiver<Vec<AuditRecord>>,
    ch_cfg: ClickHouseConfig,
    backlog: Arc<AtomicI32>,
) {
    let path = wal_path();
    let mut wal_file: Option<File> = open_wal(&path);

    let client = store_clickhouse::client(&ch_cfg);

    // Replay any uncommitted batches from a previous run.
    if let Some(ref mut f) = wal_file {
        let batches = read_batches(f);
        if !batches.is_empty() {
            tracing::info!(
                count = batches.len(),
                "replaying WAL batches from previous run"
            );
            for batch in batches {
                backlog.fetch_add(1, Relaxed);
                write_with_backoff(&client, &ch_cfg, &batch).await;
                backlog.fetch_sub(1, Relaxed);
            }
            truncate_wal(f);
        }
    }

    while let Some(batch) = ch_rx.recv().await {
        if let Some(ref mut f) = wal_file {
            append_batch(f, &batch);
        }
        backlog.fetch_add(1, Relaxed);

        write_with_backoff(&client, &ch_cfg, &batch).await;

        backlog.fetch_sub(1, Relaxed);

        if let Some(ref mut f) = wal_file {
            truncate_wal(f);
        }
    }

    // Channel closed; nothing further to flush (write_with_backoff ensures each
    // batch is fully written before the loop advances).
}

async fn write_with_backoff(
    client: &store_clickhouse::ClickHouseClient,
    ch_cfg: &ClickHouseConfig,
    batch: &[AuditRecord],
) {
    let mut delay_secs: u64 = 1;
    loop {
        let mut writer = AuditWriter::new(client, ch_cfg);
        let result = async {
            writer.write_batch(batch).await?;
            writer.end().await
        };
        match result.await {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(
                    delay_secs,
                    "ClickHouse write failed: {e} — retrying in {delay_secs}s"
                );
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                delay_secs = (delay_secs * 2).min(60);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rules_core::{AuditRecord, AuditType};
    use std::fs;
    use std::path::PathBuf;

    fn make_record(id: &str) -> AuditRecord {
        AuditRecord {
            audit_id: id.to_owned(),
            rule_id: "r1".to_owned(),
            schema_version: 1,
            audit_type: AuditType::Matched,
            reason: None,
            source_event: "{}".to_owned(),
            routed_event: None,
            source_topic: "src".to_owned(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
            parse_time_nano: 0,
            eval_time_nano: 0,
            total_time_nano: 0,
        }
    }

    fn tmp_path(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("wal-test-{}-{}", std::process::id(), suffix))
    }

    fn open_rw(path: &PathBuf) -> std::fs::File {
        std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .unwrap()
    }

    #[test]
    fn append_and_read_single_batch() {
        let path = tmp_path("single");
        let records = vec![make_record("a1"), make_record("a2")];
        let mut file = open_rw(&path);
        append_batch(&mut file, &records);
        let batches = read_batches(&mut file);
        fs::remove_file(&path).ok();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[0][0].audit_id, "a1");
        assert_eq!(batches[0][1].audit_id, "a2");
    }

    #[test]
    fn read_incomplete_batch_dropped() {
        let path = tmp_path("incomplete");
        let mut file = open_rw(&path);
        use std::io::Write;
        let line = serde_json::to_string(&make_record("b1")).unwrap();
        writeln!(file, "{}", line).unwrap();
        let batches = read_batches(&mut file);
        fs::remove_file(&path).ok();
        assert!(batches.is_empty());
    }

    #[test]
    fn truncate_empties_file() {
        let path = tmp_path("truncate");
        let mut file = open_rw(&path);
        append_batch(&mut file, &[make_record("c1")]);
        truncate_wal(&mut file);
        let batches = read_batches(&mut file);
        fs::remove_file(&path).ok();
        assert!(batches.is_empty());
    }

    #[test]
    fn multiple_complete_batches_all_read() {
        let path = tmp_path("multi");
        let mut file = open_rw(&path);
        append_batch(&mut file, &[make_record("d1"), make_record("d2")]);
        append_batch(&mut file, &[make_record("d3")]);
        let batches = read_batches(&mut file);
        fs::remove_file(&path).ok();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 2);
        assert_eq!(batches[1].len(), 1);
        assert_eq!(batches[1][0].audit_id, "d3");
    }

    #[test]
    fn open_wal_creates_file() {
        let path = tmp_path("open");
        fs::remove_file(&path).ok();
        let result = open_wal(&path);
        let exists = path.exists();
        fs::remove_file(&path).ok();
        assert!(result.is_some());
        assert!(exists);
    }
}
