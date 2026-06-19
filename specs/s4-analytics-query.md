# Spec: S4 — analytics query module (crates/store-clickhouse/src/analytics.rs)

Generate the complete contents of `crates/store-clickhouse/src/analytics.rs`.

Output EXACTLY ONE fenced Rust code block. No prose before or after it.

---

## Context

The ClickHouse database `ruleaudit` has two aggregate tables populated by materialized views:

### `agg_rule_hourly` (SummingMergeTree, ORDER BY (hour, rule_id))
Columns:
- `hour`        DateTime
- `rule_id`     LowCardinality(String)
- `matched`     UInt64
- `unmatched`   UInt64
- `errored`     UInt64
- `eval_count`  UInt64
- `parse_sum`   UInt64
- `eval_sum`    UInt64
- `total_sum`   UInt64

### `agg_hour_messages` (AggregatingMergeTree, ORDER BY hour)
Columns:
- `hour`     DateTime
- `msg_hll`  AggregateFunction(uniq, String)   ← HLL state, NOT a plain count

Query hint: use `uniqMerge(msg_hll)` to read it.

---

## Imports available

From the `clickhouse` crate (0.15.1):
- `clickhouse::Client`
- `clickhouse::Row` derive macro
- `clickhouse::error::Error as ChError` (use fully-qualified path, do NOT import as `Error` — name collision with crate's own `Error`)

From workspace:
- `chrono::{DateTime, Utc}`
- `serde::{Deserialize, Serialize}`

The crate's own error type (ALREADY EXISTS in lib.rs, do NOT redefine):
```rust
pub enum Error {
    ClickHouse(#[from] clickhouse::error::Error),
}
```
Import it as `use crate::Error;`

---

## What to generate

### Output structs

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleStat {
    pub rule_id:   String,
    pub matched:   u64,
    pub unmatched: u64,
    pub errored:   u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp:  DateTime<Utc>,
    pub rule_id:    String,
    pub matched:    u64,
    pub unmatched:  u64,
    pub errored:    u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsStats {
    pub total_messages:      u64,
    pub total_evaluations:   u64,
    pub rule_stats:          Vec<RuleStat>,
    pub time_series:         Vec<TimeSeriesPoint>,
    pub avg_parse_time_nano: u64,
    pub avg_eval_time_nano:  u64,
    pub avg_total_time_nano: u64,
}
```

### Main function

```rust
pub async fn query_analytics(
    client: &Client,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<AnalyticsStats, Error>
```

Implement by running FOUR separate ClickHouse queries and assembling the result:

#### Query 1 — total_messages
```sql
SELECT uniqMerge(msg_hll) AS total
FROM ruleaudit.agg_hour_messages
WHERE hour >= toDateTime({from_ts:UInt32}) AND hour <= toDateTime({to_ts:UInt32})
```
- `from_ts` = `from.timestamp() as u32`
- `to_ts`   = `to.timestamp() as u32`
- Fetch one row with a helper struct `struct MsgCount { total: u64 }` (derive `Row`, `Deserialize`)
- If no rows, default to 0

#### Query 2 — total_evaluations + latency
```sql
SELECT
    sum(eval_count)  AS total_evaluations,
    sum(parse_sum)   AS parse_sum,
    sum(eval_sum)    AS eval_sum,
    sum(total_sum)   AS total_sum
FROM ruleaudit.agg_rule_hourly
WHERE hour >= toDateTime({from_ts:UInt32}) AND hour <= toDateTime({to_ts:UInt32})
```
- Helper struct `struct EvalSums { total_evaluations: u64, parse_sum: u64, eval_sum: u64, total_sum: u64 }` (derive `Row`, `Deserialize`)
- Compute weighted averages: if `total_evaluations > 0` then `parse_sum / total_evaluations` else `0` (integer division, result is u64 nanoseconds)
- If no rows, all fields default to 0

#### Query 3 — rule_stats
```sql
SELECT
    rule_id,
    sum(matched)   AS matched,
    sum(unmatched) AS unmatched,
    sum(errored)   AS errored
FROM ruleaudit.agg_rule_hourly
WHERE hour >= toDateTime({from_ts:UInt32}) AND hour <= toDateTime({to_ts:UInt32})
GROUP BY rule_id
ORDER BY matched DESC
```
- Fetch into `Vec<RuleStatRow>` where `RuleStatRow` derives `Row` + `Deserialize` and has exactly these 4 fields.
- Map to `Vec<RuleStat>` (same fields, different struct used for Row derive isolation)

#### Query 4 — time_series
```sql
SELECT
    toUnixTimestamp(hour) AS hour_ts,
    rule_id,
    sum(matched)   AS matched,
    sum(unmatched) AS unmatched,
    sum(errored)   AS errored
FROM ruleaudit.agg_rule_hourly
WHERE hour >= toDateTime({from_ts:UInt32}) AND hour <= toDateTime({to_ts:UInt32})
GROUP BY hour, rule_id
ORDER BY hour ASC
```
- Helper struct `struct TsRow { hour_ts: u32, rule_id: String, matched: u64, unmatched: u64, errored: u64 }` (derive `Row`, `Deserialize`)
- Convert `hour_ts: u32` to `DateTime<Utc>`: `DateTime::from_timestamp(hour_ts as i64, 0).unwrap_or_default()`
- Map to `Vec<TimeSeriesPoint>`

### Binding pattern for all queries

Use `.bind("from_ts", from.timestamp() as u32)` and `.bind("to_ts", to.timestamp() as u32)`.

Query pattern:
```rust
let rows: Vec<T> = client
    .query("SELECT ...")
    .bind("from_ts", from.timestamp() as u32)
    .bind("to_ts", to.timestamp() as u32)
    .fetch_all::<T>()
    .await?;
```

For single-row queries (Q1, Q2), use `.fetch_one::<T>().await` wrapped in a match — on `Err(clickhouse::error::Error::RowNotFound)` return the default struct. Use `clickhouse::error::Error::RowNotFound` fully-qualified, NOT imported.

---

## File structure

1. `use` imports
2. Private helper structs (`MsgCount`, `EvalSums`, `RuleStatRow`, `TsRow`) — do NOT `pub`
3. Public output structs (`RuleStat`, `TimeSeriesPoint`, `AnalyticsStats`)
4. `pub async fn query_analytics(...)`

No `mod` declarations. No `main`. No test module. No other top-level items.
