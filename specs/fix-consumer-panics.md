# Spec: fix panicking expects and URL parsing in crates/pipeline/src/consumer.rs

## Context
File: crates/pipeline/src/consumer.rs
Full file content is provided below — output the COMPLETE modified file.

## Changes required

### 1. Add `NoGroupMetadata` variant to `PipelineError`
In the `PipelineError` enum (around line 47), add a new variant:
```rust
#[error("consumer group metadata unavailable")]
NoGroupMetadata,
```

### 2. Fix the two panicking `.expect()` calls (lines 381-384)
Replace:
```rust
tpl.add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
    .expect("add_partition_offset");
let cgm = consumer.group_metadata().expect("group_metadata");
```
With:
```rust
tpl.add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
    .map_err(PipelineError::Kafka)?;
let cgm = consumer
    .group_metadata()
    .ok_or(PipelineError::NoGroupMetadata)?;
```

### 3. Fix the fragile URL parsing in `check_postgres_tcp` (around line 467)
Replace the silent fallback to `"localhost:5432"` when parsing fails.
Instead of `.unwrap_or("localhost:5432")`, return `false` immediately if parsing yields nothing:
```rust
let host_port = match database_url
    .split("://")
    .nth(1)
    .and_then(|rest| rest.split('@').nth(1))
    .and_then(|host_db| host_db.split('/').next())
{
    Some(hp) => hp,
    None => return false,
};
```

## Constraints
- Output EXACTLY ONE fenced ```rust code block containing the complete file.
- Do NOT add, remove, or reorder any imports beyond what is needed.
- Do NOT redefine any types that already exist in the file.
- Do NOT change any logic outside the three targeted areas.
- Preserve all existing comments exactly.
