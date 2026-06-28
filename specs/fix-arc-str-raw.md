# Spec: use Arc<str> for OwnedMsg::raw to avoid per-rule string clones

## File: crates/pipeline/src/consumer.rs

## Problem
`OwnedMsg::raw` is a `String`. In the parallel eval phase, for each matched rule the
code clones `event.raw` (which came from `msg.raw`) twice — once for `source_event`
and once for `routed_event`. With N rules per event, that is N allocations per event.

## Fix
Change `OwnedMsg::raw` from `String` to `Arc<str>`. Then:
- Where `OwnedMsg` is constructed from `m.payload_view::<str>()`, use `Arc::from(s)` instead of `s.to_string()`.
- `SourceEvent::raw` is still a `String` (it's in the `core` crate — do not change it).
- After `SourceEvent::from_kafka(...)` is called with `&msg.raw`, the `SourceEvent` already owns its own `String` copy (that's fine — one allocation per event, not per rule).
- For the audit record fields `source_event` and `routed_event`, these come from `event.raw.clone()` (the `SourceEvent`'s raw, which is a `String`). To avoid per-rule clones of `event.raw`, wrap the SourceEvent's raw in an `Arc<str>` locally in the closure and use `Arc::clone` for each rule:

Inside the `par_iter().map(|msg| { ... })` closure, after `SourceEvent::from_kafka(...)` succeeds, add:
```rust
let raw_arc: Arc<str> = Arc::from(event.raw.as_str());
```
Then replace the two occurrences of `event.raw.clone()` in the audit record construction with `raw_arc.to_string()` — wait, that still allocates. Instead, keep `source_event: event.raw.clone()` as-is for now (one `String` clone per rule is unavoidable without changing `AuditRecord`), BUT eliminate the redundant `routed_event` clone by reusing the same `Arc`:

Actually, the simplest correct fix that avoids N allocations is:
- Wrap `event.raw` in `Arc<str>` once per event (not per rule).
- For `source_event` (always set) use `Arc::clone(&raw_arc).to_string()` — no, that still clones.

The correct minimal fix: change only `OwnedMsg::raw` from `String` to `Arc<str>`, so that when `msg.raw` is accessed in two parallel rayon threads for two different rules, it is a reference-counted pointer bump rather than a string copy. The `event.raw.clone()` inside the rule loop remains a `String` clone (one per rule, which is the existing behaviour).

## Actual changes required
1. Change `raw: String` to `raw: Arc<str>` in `OwnedMsg`.
2. Add `use std::sync::Arc;` — it is already imported.
3. Where `OwnedMsg` is pushed onto `batch`, change `raw: s.to_string()` to `raw: Arc::from(s)`.
4. Where `SourceEvent::from_kafka` is called with `&msg.raw`, change to `&msg.raw` — since `Arc<str>` derefs to `str`, use `msg.raw.as_ref()` or `&*msg.raw`.
5. Where `tracing::warn!` logs `msg.raw` does not exist (the raw is only accessed via `payload_view`), no change needed there.
6. All other uses of `msg.raw` (the tracing warn for non-UTF-8 payloads does not access `msg.raw`; the parse error path does not either) remain unchanged.

## Constraints
- Output EXACTLY ONE fenced ```rust code block with the complete file.
- `Arc` is already imported via `use std::sync::Arc;` — do not add a duplicate import.
- Do NOT change `SourceEvent`, `AuditRecord`, or any other type outside `consumer.rs`.
- Do NOT change any logic beyond the three targeted sites (OwnedMsg struct, push to batch, from_kafka call).
- Preserve all existing comments exactly.
