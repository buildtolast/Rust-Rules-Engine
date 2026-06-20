use axum::{
    body::Body,
    extract::{Query, State},
    response::Response,
    Json,
};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use store_clickhouse::AuditQueryRow;

use crate::{ApiError, AppState};

// ── /api/reports/top ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct TopQuery {
    #[serde(rename = "type")]
    audit_type: String,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    10
}

#[derive(Serialize)]
pub struct AuditRecordOut {
    #[serde(rename = "auditId")]
    pub audit_id: String,
    #[serde(rename = "ruleId")]
    pub rule_id: String,
    #[serde(rename = "auditType")]
    pub audit_type: String,
    pub reason: String,
    #[serde(rename = "sourceEvent")]
    pub source_event: String,
    #[serde(rename = "routedEvent")]
    pub routed_event: String,
    #[serde(rename = "sourceTopic")]
    pub source_topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: String,
    #[serde(rename = "parseTimeNano")]
    pub parse_time_nano: u64,
    #[serde(rename = "evalTimeNano")]
    pub eval_time_nano: u64,
    #[serde(rename = "totalTimeNano")]
    pub total_time_nano: u64,
}

impl From<AuditQueryRow> for AuditRecordOut {
    fn from(r: AuditQueryRow) -> Self {
        let ts = chrono::DateTime::from_timestamp(r.timestamp_secs as i64, 0)
            .unwrap_or_default()
            .to_rfc3339();
        Self {
            audit_id: r.audit_id,
            rule_id: r.rule_id,
            audit_type: r.audit_type,
            reason: r.reason,
            source_event: r.source_event,
            routed_event: r.routed_event,
            source_topic: r.source_topic,
            partition: r.partition,
            offset: r.offset,
            timestamp: ts,
            parse_time_nano: r.parse_time_nano,
            eval_time_nano: r.eval_time_nano,
            total_time_nano: r.total_time_nano,
        }
    }
}

pub async fn top(
    State(s): State<AppState>,
    Query(q): Query<TopQuery>,
) -> Result<Json<Vec<AuditRecordOut>>, ApiError> {
    let to = q.to.unwrap_or_else(Utc::now);
    let from = q.from.unwrap_or_else(|| to - Duration::hours(24));
    let limit = q.limit.min(100);
    let ch_type = q.audit_type.to_uppercase();
    let ch_type = ch_type.as_str();
    let rows = store_clickhouse::query_top_audits(&s.ch_client, ch_type, from, to, limit).await?;
    Ok(Json(rows.into_iter().map(AuditRecordOut::from).collect()))
}

// ── /api/reports/export ──────────────────────────────────────────────────────

/// Metadata-only export row (default; no JSON payload columns).
#[derive(clickhouse::Row, serde::Deserialize)]
struct ExportRowMeta {
    audit_id: String,
    rule_id: String,
    audit_type: String,
    reason: String,
    source_topic: String,
    partition: i32,
    offset: i64,
    timestamp_secs: u32,
    parse_time_nano: u64,
    eval_time_nano: u64,
    total_time_nano: u64,
}

/// Full export row including JSON event payloads (opt-in via include_events=true).
#[derive(clickhouse::Row, serde::Deserialize)]
struct ExportRowFull {
    audit_id: String,
    rule_id: String,
    audit_type: String,
    reason: String,
    source_topic: String,
    partition: i32,
    offset: i64,
    timestamp_secs: u32,
    parse_time_nano: u64,
    eval_time_nano: u64,
    total_time_nano: u64,
    source_event: String,
    routed_event: String,
}

#[derive(Deserialize)]
pub struct ExportQuery {
    #[serde(rename = "type")]
    audit_type: String,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    rule_id: Option<String>,
    /// Include source_event and routed_event JSON payloads (default: false — large blobs)
    #[serde(default)]
    include_events: bool,
    /// Row limit (default: 1_000_000; max: 10_000_000)
    #[serde(default = "default_export_limit")]
    limit: u64,
}

fn default_export_limit() -> u64 {
    1_000_000
}

const CSV_HEADER_META: &str =
    "audit_id,rule_id,audit_type,reason,source_topic,partition,offset,\
     timestamp,parse_time_nano,eval_time_nano,total_time_nano\n";

const CSV_HEADER_FULL: &str =
    "audit_id,rule_id,audit_type,reason,source_topic,partition,offset,\
     timestamp,parse_time_nano,eval_time_nano,total_time_nano,source_event,routed_event\n";

pub async fn export(
    State(s): State<AppState>,
    Query(q): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    // Validate audit_type against the known enum values.
    let ch_type = match q.audit_type.to_uppercase().as_str() {
        "MATCHED" => "MATCHED",
        "UNMATCHED" => "UNMATCHED",
        "ERRORED" => "ERRORED",
        other => {
            return Err(ApiError::BadRequest(format!(
                "unknown audit type '{other}'; expected MATCHED, UNMATCHED, or ERRORED"
            )))
        }
    };

    // Validate rule_id as a UUID to prevent injection before interpolation.
    if let Some(ref rid) = q.rule_id {
        uuid::Uuid::parse_str(rid)
            .map_err(|_| ApiError::BadRequest("rule_id must be a valid UUID".into()))?;
    }

    let to = q.to.unwrap_or_else(Utc::now);
    let from = q.from.unwrap_or_else(|| to - Duration::hours(24));
    let from_ts = from.timestamp() as u32;
    let to_ts = to.timestamp() as u32;

    let rule_clause = q
        .rule_id
        .as_deref()
        .map(|rid| format!("AND rule_id = '{rid}'"))
        .unwrap_or_default();

    let limit = q.limit.min(10_000_000);
    let include_events = q.include_events;

    let event_cols = if include_events {
        ", source_event, routed_event"
    } else {
        ""
    };

    // No ORDER BY — avoids a full sort of potentially 50M+ rows before LIMIT is applied.
    // Rows come out in the MergeTree primary-key order (source_topic, partition, offset, rule_id),
    // which is a consistent ordering suitable for a CSV download.
    // max_memory_usage caps per-query RAM so a large export cannot OOM the server.
    let sql = format!(
        "SELECT audit_id, rule_id, audit_type, reason, source_topic, \
         partition, offset, toUnixTimestamp(timestamp) AS timestamp_secs, \
         parse_time_nano, eval_time_nano, total_time_nano{event_cols} \
         FROM audits \
         WHERE audit_type = '{ch_type}' \
           AND timestamp >= toDateTime({from_ts}) \
           AND timestamp <= toDateTime({to_ts}) \
           {rule_clause} \
         LIMIT {limit} \
         SETTINGS max_memory_usage = 2000000000"
    );

    // Two separate fetch paths because ClickHouse binary protocol is positional:
    // the struct field count must match the SELECT column count exactly.
    let body: Body = if include_events {
        let header = futures::stream::once(std::future::ready(Ok::<Bytes, std::io::Error>(
            Bytes::from(CSV_HEADER_FULL),
        )));
        let cursor = s
            .ch_client
            .query(&sql)
            .fetch::<ExportRowFull>()
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        let rows = futures::stream::unfold(cursor, |mut cursor| async move {
            match cursor.next().await {
                Ok(Some(row)) => {
                    let ts = DateTime::from_timestamp(row.timestamp_secs as i64, 0)
                        .unwrap_or_default()
                        .to_rfc3339();
                    let line = format!(
                        "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                        row.audit_id, row.rule_id, row.audit_type,
                        csv_escape(&row.reason), row.source_topic,
                        row.partition, row.offset, ts,
                        row.parse_time_nano, row.eval_time_nano, row.total_time_nano,
                        csv_escape(&row.source_event), csv_escape(&row.routed_event),
                    );
                    Some((Ok::<Bytes, std::io::Error>(Bytes::from(line)), cursor))
                }
                Ok(None) => None,
                Err(e) => { tracing::error!("export stream error: {e}"); None }
            }
        });
        Body::from_stream(header.chain(rows))
    } else {
        let header = futures::stream::once(std::future::ready(Ok::<Bytes, std::io::Error>(
            Bytes::from(CSV_HEADER_META),
        )));
        let cursor = s
            .ch_client
            .query(&sql)
            .fetch::<ExportRowMeta>()
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        let rows = futures::stream::unfold(cursor, |mut cursor| async move {
            match cursor.next().await {
                Ok(Some(row)) => {
                    let ts = DateTime::from_timestamp(row.timestamp_secs as i64, 0)
                        .unwrap_or_default()
                        .to_rfc3339();
                    let line = format!(
                        "{},{},{},{},{},{},{},{},{},{},{}\n",
                        row.audit_id, row.rule_id, row.audit_type,
                        csv_escape(&row.reason), row.source_topic,
                        row.partition, row.offset, ts,
                        row.parse_time_nano, row.eval_time_nano, row.total_time_nano,
                    );
                    Some((Ok::<Bytes, std::io::Error>(Bytes::from(line)), cursor))
                }
                Ok(None) => None,
                Err(e) => { tracing::error!("export stream error: {e}"); None }
            }
        });
        Body::from_stream(header.chain(rows))
    };

    let filename = format!(
        "audit-{}-{}-{}.csv",
        ch_type.to_lowercase(),
        from.format("%Y%m%dT%H%M%S"),
        to.format("%Y%m%dT%H%M%S"),
    );

    Ok(Response::builder()
        .header("content-type", "text/csv; charset=utf-8")
        .header(
            "content-disposition",
            format!("attachment; filename=\"{filename}\""),
        )
        .body(body)
        .unwrap())
}

// RFC 4180 CSV escaping: wrap in quotes if the value contains commas, quotes, or newlines.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
