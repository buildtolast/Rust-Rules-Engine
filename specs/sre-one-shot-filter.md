# Spec: SRE one-shot container filter — docker.rs

## Context

`crates/sre/src/docker.rs` lists all containers (including exited ones) so the SRE
agent can detect services that crashed. But one-shot init containers (e.g.
`redpanda-init`, `restart: "no"`) exit with code 0 on success — the agent must not
treat them as crashed services.

## Task

Modify `ContainerInfo` and `list_containers` to expose restart policy and exit code.

### Existing struct (do NOT remove any existing fields):
```rust
#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub name: String,
    pub id: String,
    pub running: bool,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub health: HealthSummary,
}
```

### Required changes:
1. Add two fields to `ContainerInfo`:
   - `pub one_shot: bool` — true when restart policy name is `"no"`, `""`, or absent
   - `pub exit_code: Option<i64>` — last exit code; `None` if still running

2. In `list_containers`, after `let running = state.running.unwrap_or(false);`, read:
   - `exit_code`: `state.exit_code` (already `Option<i64>` in bollard)
   - `one_shot`: from `inspect.host_config.as_ref()?.restart_policy?.name`; treat
     policy name `""`, `"no"`, and `None` as one_shot=true; anything else
     (`"always"`, `"unless-stopped"`, `"on-failure"`) as one_shot=false

3. Add both new fields to the `result.push(ContainerInfo { ... })` call.

### Output: exactly ONE fenced Rust block containing the COMPLETE file.
### No commentary outside the code block.
