use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use store_clickhouse::AuditQueryRow;

use crate::{ApiError, AppState};

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
