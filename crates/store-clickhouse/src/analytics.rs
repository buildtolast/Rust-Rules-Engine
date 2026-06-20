use crate::Error;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct MsgCount {
    total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct EvalSums {
    total_evaluations: u64,
    parse_sum: u64,
    eval_sum: u64,
    total_sum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct RuleStatRow {
    rule_id: String,
    matched: u64,
    unmatched: u64,
    errored: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
struct TsRow {
    hour_ts: u32,
    rule_id: String,
    matched: u64,
    unmatched: u64,
    errored: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleStat {
    pub rule_id: String,
    pub matched: u64,
    pub unmatched: u64,
    pub errored: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub rule_id: String,
    pub matched: u64,
    pub unmatched: u64,
    pub errored: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsStats {
    pub total_messages: u64,
    pub total_evaluations: u64,
    pub rule_stats: Vec<RuleStat>,
    pub time_series: Vec<TimeSeriesPoint>,
    pub avg_parse_time_nano: u64,
    pub avg_eval_time_nano: u64,
    pub avg_total_time_nano: u64,
}

/// A single row from the raw audits table, for the Reports tab.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct AuditQueryRow {
    pub audit_id: String,
    pub rule_id: String,
    pub audit_type: String,
    pub reason: String,
    pub source_event: String,
    pub routed_event: String,
    pub source_topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_secs: u32,
    pub parse_time_nano: u64,
    pub eval_time_nano: u64,
    pub total_time_nano: u64,
}

pub async fn query_top_audits(
    client: &Client,
    audit_type: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<AuditQueryRow>, Error> {
    let from_ts = from.timestamp() as u32;
    let to_ts = to.timestamp() as u32;
    let rows = client
        .query(
            "SELECT audit_id, rule_id, toString(audit_type) AS audit_type, \
             ifNull(reason, '') AS reason, source_event, \
             ifNull(routed_event, '') AS routed_event, source_topic, \
             partition, offset, toUnixTimestamp(timestamp) AS timestamp_secs, \
             parse_time_nano, eval_time_nano, total_time_nano \
             FROM ruleaudit.audits \
             WHERE audit_type = ? AND timestamp >= toDateTime(?) AND timestamp <= toDateTime(?) \
             ORDER BY timestamp DESC LIMIT ?",
        )
        .bind(audit_type)
        .bind(from_ts)
        .bind(to_ts)
        .bind(limit)
        .fetch_all::<AuditQueryRow>()
        .await?;
    Ok(rows)
}

pub async fn query_analytics(
    client: &Client,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<AnalyticsStats, Error> {
    // Truncate to the start of the hour so we always include the current/earliest
    // aggregation bucket, regardless of where in the hour the query starts.
    let from_ts = (from.timestamp() / 3600 * 3600) as u32;
    let to_ts = to.timestamp() as u32;

    // Query 1: total_messages
    let msg_count = match client
        .query("SELECT uniqMerge(msg_hll) AS total FROM ruleaudit.agg_hour_messages WHERE hour >= toDateTime(?) AND hour <= toDateTime(?)")
        .bind(from_ts)
        .bind(to_ts)
        .fetch_one::<MsgCount>()
        .await
    {
        Ok(r) => r,
        Err(clickhouse::error::Error::RowNotFound) => MsgCount { total: 0 },
        Err(e) => return Err(Error::ClickHouse(e)),
    };

    // Query 2: total_evaluations + latency
    let eval_sums = match client
        .query("SELECT sum(eval_count) AS total_evaluations, sum(parse_sum) AS parse_sum, sum(eval_sum) AS eval_sum, sum(total_sum) AS total_sum FROM ruleaudit.agg_rule_hourly WHERE hour >= toDateTime(?) AND hour <= toDateTime(?)")
        .bind(from_ts)
        .bind(to_ts)
        .fetch_one::<EvalSums>()
        .await
    {
        Ok(r) => r,
        Err(clickhouse::error::Error::RowNotFound) => EvalSums { total_evaluations: 0, parse_sum: 0, eval_sum: 0, total_sum: 0 },
        Err(e) => return Err(Error::ClickHouse(e)),
    };

    let avg_parse = if eval_sums.total_evaluations > 0 {
        eval_sums.parse_sum / eval_sums.total_evaluations
    } else {
        0
    };
    let avg_eval = if eval_sums.total_evaluations > 0 {
        eval_sums.eval_sum / eval_sums.total_evaluations
    } else {
        0
    };
    let avg_total = if eval_sums.total_evaluations > 0 {
        eval_sums.total_sum / eval_sums.total_evaluations
    } else {
        0
    };

    // Query 3: rule_stats
    let rows: Vec<RuleStatRow> = client
        .query("SELECT rule_id, sum(matched) AS matched, sum(unmatched) AS unmatched, sum(errored) AS errored FROM ruleaudit.agg_rule_hourly WHERE hour >= toDateTime(?) AND hour <= toDateTime(?) GROUP BY rule_id ORDER BY matched DESC")
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all::<RuleStatRow>()
        .await?;

    let rule_stats: Vec<RuleStat> = rows
        .into_iter()
        .map(|r| RuleStat {
            rule_id: r.rule_id,
            matched: r.matched,
            unmatched: r.unmatched,
            errored: r.errored,
        })
        .collect();

    // Query 4: time_series
    let ts_rows: Vec<TsRow> = client
        .query("SELECT toUnixTimestamp(hour) AS hour_ts, rule_id, sum(matched) AS matched, sum(unmatched) AS unmatched, sum(errored) AS errored FROM ruleaudit.agg_rule_hourly WHERE hour >= toDateTime(?) AND hour <= toDateTime(?) GROUP BY hour, rule_id ORDER BY hour ASC")
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all::<TsRow>()
        .await?;

    let time_series: Vec<TimeSeriesPoint> = ts_rows
        .into_iter()
        .map(|r| TimeSeriesPoint {
            timestamp: DateTime::from_timestamp(r.hour_ts as i64, 0).unwrap_or_default(),
            rule_id: r.rule_id,
            matched: r.matched,
            unmatched: r.unmatched,
            errored: r.errored,
        })
        .collect();

    Ok(AnalyticsStats {
        total_messages: msg_count.total,
        total_evaluations: eval_sums.total_evaluations,
        rule_stats,
        time_series,
        avg_parse_time_nano: avg_parse,
        avg_eval_time_nano: avg_eval,
        avg_total_time_nano: avg_total,
    })
}
