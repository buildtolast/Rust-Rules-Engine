# Spec: fix misc clippy warnings across multiple files

## File 1: crates/sre/src/analysis.rs — redundant else (line ~242)
Find the pattern:
```rust
} else {
    last_err = Some("missing content field".into());
}
```
that follows a `continue` or `break` inside the preceding `if` block.
Remove the `else` keyword and dedent its contents by one level.

## File 2: crates/sre/src/trace_analysis.rs — needless raw string hashes (line ~52)
Find:
```rust
r#"SELECT
    ...
    LIMIT 20"#
```
Replace `r#"..."#` with `r"..."` (no hash needed since the string contains no `"`).

## File 3: crates/web/src/tests.rs — manual_let_else (lines ~316 and ~355)
Find two occurrences of:
```rust
let state = match try_build_app_state().await {
    Some(s) => s,
    None => {
        eprintln!("skipping: could not build AppState (check DATABASE_URL)");
        return;
    }
};
```
Replace each with:
```rust
let Some(state) = try_build_app_state().await else {
    eprintln!("skipping: could not build AppState (check DATABASE_URL)");
    return;
};
```

## File 4: crates/core/src/audit.rs — add #[must_use]
Add `#[must_use]` attribute to the `audit_id` function.

## File 5: crates/core/src/eval_result.rs — add #[must_use]
Add `#[must_use]` to the `matched()` and `verdict()` methods on `EvaluationResult`.

## File 6: crates/telemetry/src/lib.rs — use map_or instead of match
Find:
```rust
let sample_rate: f64 = match std::env::var("OTEL_SAMPLE_RATE") {
    Ok(v) => v.parse().unwrap_or_else(|_| {
        eprintln!("WARN: OTEL_SAMPLE_RATE={v:?} is not a valid f64, using default 0.1");
        0.1
    }),
    Err(_) => 0.1,
};
```
Replace with:
```rust
let sample_rate: f64 = std::env::var("OTEL_SAMPLE_RATE").map_or(0.1, |v| {
    v.parse().unwrap_or_else(|_| {
        eprintln!("WARN: OTEL_SAMPLE_RATE={v:?} is not a valid f64, using default 0.1");
        0.1
    })
});
```

## Output format
For EACH file, output a separate fenced ```rust code block with a comment header on the first line:
// FILE: <path>
Then the complete file content.

Output all 6 files in sequence, each in its own fenced block.
