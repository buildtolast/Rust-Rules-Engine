# Spec: SRE lib.rs — skip one-shot containers that exited cleanly

## Context

`crates/sre/src/lib.rs` calls `analyze_container` for every container returned by
`docker::list_containers`. After the change to `docker.rs`, `ContainerInfo` now has:
- `one_shot: bool`
- `exit_code: Option<i64>`

## Required change

In `analyze_container`, immediately after the opening brace (before the
running-state transition tracking block), add an early-return guard:

```rust
// One-shot init containers (restart=no) that exited cleanly are not service crashes.
if c.one_shot && c.exit_code == Some(0) {
    return;
}
```

This must be the very first thing in the function body so the state-transition
tracker and LLM are never invoked for completed init jobs.

## Output: exactly ONE fenced Rust block containing the COMPLETE `analyze_container`
function only (from `async fn analyze_container` through its closing `}`).
No other functions. No commentary outside the fenced block.

## Output path: `crates/sre/src/lib.rs` (patch target: `analyze_container` function)
