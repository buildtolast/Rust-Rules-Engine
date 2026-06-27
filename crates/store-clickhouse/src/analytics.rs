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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;

    // ── Unit tests (no DB) ────────────────────────────────────────────────────

    #[test]
    fn test_analytics_stats_dto_serializes_to_camel_case() {
        let stats = AnalyticsStats {
            total_messages: 100,
            total_evaluations: 50,
            rule_stats: vec![],
            time_series: vec![],
            avg_parse_time_nano: 1_000,
            avg_eval_time_nano: 2_000,
            avg_total_time_nano: 3_000,
        };
        let v: Value = serde_json::to_value(&stats).expect("serialization failed");
        assert!(v.get("totalMessages").is_some(), "expected totalMessages");
        assert!(v.get("totalEvaluations").is_some(), "expected totalEvaluations");
        assert!(v.get("ruleStats").is_some(), "expected ruleStats");
        assert!(v.get("timeSeries").is_some(), "expected timeSeries");
        assert!(v.get("avgParseTimeNano").is_some(), "expected avgParseTimeNano");
        assert!(v.get("avgEvalTimeNano").is_some(), "expected avgEvalTimeNano");
        assert!(v.get("avgTotalTimeNano").is_some(), "expected avgTotalTimeNano");

        // Snake-case keys must NOT appear
        assert!(v.get("total_messages").is_none());
        assert!(v.get("rule_stats").is_none());

        // Values round-trip correctly
        assert_eq!(v["totalMessages"], 100u64);
        assert_eq!(v["totalEvaluations"], 50u64);
        assert_eq!(v["avgParseTimeNano"], 1_000u64);
    }

    #[test]
    fn test_rule_stat_serializes_camel_case() {
        let stat = RuleStat {
            rule_id: "r1".into(),
            matched: 10,
            unmatched: 2,
            errored: 1,
        };
        let v: Value = serde_json::to_value(&stat).expect("serialization failed");
        assert!(v.get("ruleId").is_some(), "expected ruleId");
        assert!(v.get("matched").is_some(), "expected matched");
        assert!(v.get("unmatched").is_some(), "expected unmatched");
        assert!(v.get("errored").is_some(), "expected errored");

        assert!(v.get("rule_id").is_none(), "snake_case key must not appear");

        assert_eq!(v["ruleId"], "r1");
        assert_eq!(v["matched"], 10u64);
        assert_eq!(v["unmatched"], 2u64);
        assert_eq!(v["errored"], 1u64);
    }

    #[test]
    fn test_time_series_point_timestamp_serializes() {
        let ts = Utc.timestamp_opt(0, 0).single().expect("valid timestamp");
        let point = TimeSeriesPoint {
            timestamp: ts,
            rule_id: "rule-42".into(),
            matched: 5,
            unmatched: 1,
            errored: 0,
        };
        let v: Value = serde_json::to_value(&point).expect("serialization failed");
        assert!(v.get("timestamp").is_some(), "expected timestamp field");
        // chrono with serde serializes DateTime<Utc> as RFC 3339 string
        let ts_str = v["timestamp"].as_str().expect("timestamp should be a string");
        assert!(
            ts_str.contains("1970"),
            "epoch timestamp should contain 1970, got: {ts_str}"
        );
        assert!(v.get("ruleId").is_some(), "expected ruleId (camelCase)");
        assert!(v.get("rule_id").is_none(), "snake_case must not appear");
    }

    #[test]
    fn test_audit_query_row_fields_match_schema() {
        // AuditQueryRow has no #[serde(rename_all)] so serde keys stay snake_case.
        let row = AuditQueryRow {
            audit_id: "topic:0:1:rule-1".into(),
            rule_id: "rule-1".into(),
            audit_type: "MATCHED".into(),
            reason: "".into(),
            source_event: r#"{"key":"val"}"#.into(),
            routed_event: r#"{"key":"val"}"#.into(),
            source_topic: "events".into(),
            partition: 0,
            offset: 1,
            timestamp_secs: 1_700_000_000,
            parse_time_nano: 500,
            eval_time_nano: 800,
            total_time_nano: 1_300,
        };
        let v: Value = serde_json::to_value(&row).expect("serialization failed");

        // No rename_all — all keys are snake_case
        assert!(v.get("audit_id").is_some(), "expected audit_id");
        assert!(v.get("rule_id").is_some(), "expected rule_id");
        assert!(v.get("audit_type").is_some(), "expected audit_type");
        assert!(v.get("reason").is_some(), "expected reason");
        assert!(v.get("source_event").is_some(), "expected source_event");
        assert!(v.get("routed_event").is_some(), "expected routed_event");
        assert!(v.get("source_topic").is_some(), "expected source_topic");
        assert!(v.get("partition").is_some(), "expected partition");
        assert!(v.get("offset").is_some(), "expected offset");
        assert!(v.get("timestamp_secs").is_some(), "expected timestamp_secs");
        assert!(v.get("parse_time_nano").is_some(), "expected parse_time_nano");
        assert!(v.get("eval_time_nano").is_some(), "expected eval_time_nano");
        assert!(v.get("total_time_nano").is_some(), "expected total_time_nano");

        // CamelCase keys must NOT appear (no rename_all on this struct)
        assert!(v.get("auditId").is_none());
        assert!(v.get("ruleId").is_none());

        assert_eq!(v["audit_type"], "MATCHED");
        assert_eq!(v["partition"], 0i32);
        assert_eq!(v["offset"], 1i64);
    }

    // ── Integration tests (require live ClickHouse + INTEGRATION env var) ─────

    #[tokio::test]
    #[ignore]
    async fn test_query_analytics_integration() {
        if std::env::var("INTEGRATION").is_err() {
            return;
        }

        use crate::{client, AuditWriter, ClickHouseConfig};
        use rules_core::{AuditRecord, AuditType};

        let cfg = ClickHouseConfig {
            url: std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://localhost:8123".into()),
            ..ClickHouseConfig::default()
        };
        let ch = client(&cfg);
        let mut writer = AuditWriter::new(&ch, &cfg);

        let now = Utc::now();
        for i in 0u64..3 {
            let rec = AuditRecord {
                audit_id: format!("test-topic:0:{i}:rule-integration"),
                rule_id: "rule-integration".into(),
                schema_version: 1,
                audit_type: AuditType::Matched,
                reason: None,
                source_event: r#"{"x":1}"#.into(),
                routed_event: Some(r#"{"x":1}"#.into()),
                source_topic: "test-topic".into(),
                partition: 0,
                offset: i as i64,
                timestamp: now,
                parse_time_nano: 100,
                eval_time_nano: 200,
                total_time_nano: 300,
            };
            writer.write(&rec).await.expect("write failed");
        }
        writer.end().await.expect("flush failed");

        // Give ClickHouse a moment for the MV to process
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let from = now - chrono::Duration::hours(1);
        let to = now + chrono::Duration::hours(1);
        let stats = query_analytics(&ch, from, to)
            .await
            .expect("query_analytics failed");

        assert!(
            stats.total_evaluations >= 3,
            "expected at least 3 evaluations, got {}",
            stats.total_evaluations
        );
        assert!(
            !stats.rule_stats.is_empty(),
            "expected non-empty rule_stats"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_query_top_audits_integration() {
        if std::env::var("INTEGRATION").is_err() {
            return;
        }

        use crate::{client, ClickHouseConfig};

        let cfg = ClickHouseConfig {
            url: std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://localhost:8123".into()),
            ..ClickHouseConfig::default()
        };
        let ch = client(&cfg);

        let now = Utc::now();
        let from = now - chrono::Duration::hours(1);
        let to = now + chrono::Duration::hours(1);

        let result = query_top_audits(&ch, "MATCHED", from, to, 10).await;
        assert!(result.is_ok(), "query_top_audits returned Err: {:?}", result);
        // Length may be 0 if no MATCHED rows exist — that is a valid state.
        let rows = result.unwrap();
        assert!(
            rows.len() <= 10,
            "limit=10 but got {} rows",
            rows.len()
        );
    }
}
