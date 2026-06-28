Apply two small patches to the Rust source below. Output EXACTLY ONE fenced ```rust block with the complete updated file. Nothing outside the block.

PATCH 1 — one-shot early return in `analyze_container`:
Insert as the FIRST line inside the function body (before everything else):
```rust
if c.one_shot && c.exit_code == Some(0) {
    return;
}
```

PATCH 2 — rate-limit `decide_weakest_link` in `SreState` + `scan_once`:

2a. Add field to `SreState`:
```rust
pub last_weakest_link_at: Option<DateTime<Utc>>,
```

2b. Initialize it in `SreState::new()`:
```rust
last_weakest_link_at: None,
```

2c. In `scan_once`, replace the block:
```rust
    // Call weakest-link decision if there's something to analyze.
    let any_service_down = !probe_result.all_ok;
    if total_lag > 0 || any_service_down {
```
with:
```rust
    // Call weakest-link at most once per 60 s — it is LLM-heavy.
    let any_service_down = !probe_result.all_ok;
    let weakest_link_due = {
        let st = state.read().await;
        st.last_weakest_link_at
            .map(|t| Utc::now().signed_duration_since(t).num_seconds() >= 60)
            .unwrap_or(true)
    };
    if (total_lag > 0 || any_service_down) && weakest_link_due {
```

2d. After `st.weakest_link = decision;` inside that block, add:
```rust
        st.last_weakest_link_at = Some(Utc::now());
```

CURRENT FILE:
