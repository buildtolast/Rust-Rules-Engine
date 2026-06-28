//! Query the `audits` ClickHouse table for per-rule timing data, feed it to
//! the local LLM, and return structured performance insights.

use crate::analysis::AnalysisClient;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct RulePerf {
    pub rule_id: String,
    pub eval_count: u64,
    pub avg_eval_ms: f64,
    pub p95_eval_ms: f64,
    pub p99_eval_ms: f64,
    pub avg_parse_ms: f64,
    pub error_rate_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceInsights {
    pub generated_at: DateTime<Utc>,
    pub window_minutes: u32,
    pub rule_perf: Vec<RulePerf>,
    pub llm_insights: Vec<String>,
    pub llm_bottlenecks: Vec<String>,
    pub llm_recommendations: Vec<String>,
    pub llm_available: bool,
    pub total_evals: u64,
    pub avg_eval_ms_overall: f64,
}

#[derive(Deserialize)]
struct LlmResponse {
    #[serde(default)]
    insights: Vec<String>,
    #[serde(default)]
    top_bottlenecks: Vec<String>,
    #[serde(default)]
    recommendations: Vec<String>,
}

// ── ClickHouse query ──────────────────────────────────────────────────────────

const WINDOW_MINUTES: u32 = 10;

async fn query_rule_perf(ch: &Client) -> anyhow::Result<Vec<RulePerf>> {
    let sql = format!(
        r"SELECT
            rule_id,
            count()                                                  AS eval_count,
            round(avg(eval_time_nano) / 1e6, 3)                     AS avg_eval_ms,
            round(quantile(0.95)(eval_time_nano) / 1e6, 3)          AS p95_eval_ms,
            round(quantile(0.99)(eval_time_nano) / 1e6, 3)          AS p99_eval_ms,
            round(avg(parse_time_nano) / 1e6, 3)                    AS avg_parse_ms,
            round(countIf(audit_type = 'ERRORED') * 100.0 / count(), 2) AS error_rate_pct
        FROM audits
        WHERE timestamp > now() - INTERVAL {WINDOW_MINUTES} MINUTE
        GROUP BY rule_id
        ORDER BY p95_eval_ms DESC
        LIMIT 20"
    );

    ch.query(&sql)
        .fetch_all::<RulePerf>()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse query error: {e}"))
}

// ── LLM prompt ────────────────────────────────────────────────────────────────

fn build_prompt(rule_perf: &[RulePerf]) -> String {
    let total_evals: u64 = rule_perf.iter().map(|r| r.eval_count).sum();
    let overall_avg = if total_evals == 0 {
        0.0
    } else {
        rule_perf
            .iter()
            .map(|r| r.avg_eval_ms * r.eval_count as f64)
            .sum::<f64>()
            / total_evals as f64
    };

    let table = rule_perf
        .iter()
        .map(|r| {
            format!(
                "  {:<36} | evals={:>8} | avg={:>7.3}ms | p95={:>7.3}ms | p99={:>7.3}ms | parse={:>6.3}ms | err={:.1}%",
                r.rule_id,
                r.eval_count,
                r.avg_eval_ms,
                r.p95_eval_ms,
                r.p99_eval_ms,
                r.avg_parse_ms,
                r.error_rate_pct
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"You are an SRE expert analysing a Rust-based CEL rules engine.

## Per-rule performance (last {WINDOW_MINUTES} minutes, sorted by p95 eval latency)
  rule_id                              | evals    | avg       | p95       | p99       | parse     | err%
{table}

## Overall
- Total evaluations: {total_evals}
- Weighted avg eval latency: {overall_avg:.3} ms

Identify performance problems and actionable optimisations.

Return ONLY a JSON object (no markdown fences) with this exact shape:
{{"insights": ["..."], "top_bottlenecks": ["..."], "recommendations": ["..."]}}
Limit each array to 5 items. Be specific — name rule IDs and techniques."#
    )
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn strip_fences(s: &str) -> &str {
    let s = s.trim();
    let s = s.strip_prefix("```json").or_else(|| s.strip_prefix("```")).unwrap_or(s);
    let s = s.strip_suffix("```").unwrap_or(s);
    s.trim()
}

// ── Public API ────────────────────────────────────────────────────────────────

pub async fn fetch_insights(ch: &Client, llm: &AnalysisClient) -> TraceInsights {
    let rule_perf = match query_rule_perf(ch).await {
        Ok(rows) => rows,
        Err(e) => {
            warn!("trace_analysis: ClickHouse query failed: {e}");
            return TraceInsights {
                generated_at: Utc::now(),
                window_minutes: WINDOW_MINUTES,
                rule_perf: vec![],
                llm_insights: vec!["ClickHouse query unavailable".into()],
                llm_bottlenecks: vec![],
                llm_recommendations: vec![],
                llm_available: false,
                total_evals: 0,
                avg_eval_ms_overall: 0.0,
            };
        }
    };

    let total_evals: u64 = rule_perf.iter().map(|r| r.eval_count).sum();
    let avg_eval_ms_overall = if total_evals == 0 {
        0.0
    } else {
        rule_perf
            .iter()
            .map(|r| r.avg_eval_ms * r.eval_count as f64)
            .sum::<f64>()
            / total_evals as f64
    };

    if rule_perf.is_empty() {
        return TraceInsights {
            generated_at: Utc::now(),
            window_minutes: WINDOW_MINUTES,
            rule_perf,
            llm_insights: vec!["No audit data in the last 10 minutes.".into()],
            llm_bottlenecks: vec![],
            llm_recommendations: vec!["Run the simulation to generate events.".into()],
            llm_available: true,
            total_evals,
            avg_eval_ms_overall,
        };
    }

    let prompt = build_prompt(&rule_perf);
    info!("trace_analysis: calling LLM with {} rules", rule_perf.len());

    let (llm_insights, llm_bottlenecks, llm_recommendations, llm_available) =
        match llm.raw_complete(&prompt).await {
            Ok(raw) => {
                // Strip markdown fences that local LLMs often emit despite instructions.
                let json = strip_fences(&raw);
                match serde_json::from_str::<LlmResponse>(json) {
                    Ok(resp) => (resp.insights, resp.top_bottlenecks, resp.recommendations, true),
                    Err(e) => {
                        let preview: String = raw.chars().take(100).collect();
                        warn!("trace_analysis: LLM JSON parse failed: {e}. Raw: {preview}");
                        (vec!["LLM response unparseable".into()], vec![], vec![], true)
                    }
                }
            }
            Err(e) => {
                warn!("trace_analysis: LLM call failed: {e}");
                (vec!["Local LLM unavailable".into()], vec![], vec![], false)
            }
        };

    TraceInsights {
        generated_at: Utc::now(),
        window_minutes: WINDOW_MINUTES,
        rule_perf,
        llm_insights,
        llm_bottlenecks,
        llm_recommendations,
        llm_available,
        total_evals,
        avg_eval_ms_overall,
    }
}
