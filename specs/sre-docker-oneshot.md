Modify the Rust file below. Output EXACTLY ONE fenced ```rust block containing the complete updated file. Nothing outside the block.

CHANGES REQUIRED:
1. Add two fields to ContainerInfo:
   - `pub one_shot: bool`
   - `pub exit_code: Option<i64>`

2. In `list_containers`, after `let running = state.running.unwrap_or(false);`, add:
   ```
   let exit_code = state.exit_code;
   let one_shot = inspect
       .host_config
       .as_ref()
       .and_then(|hc| hc.restart_policy.as_ref())
       .and_then(|rp| rp.name.as_ref())
       .map(|n| matches!(n, bollard::models::RestartPolicyNameEnum::EMPTY | bollard::models::RestartPolicyNameEnum::NO))
       .unwrap_or(true);
   ```

3. Add `one_shot` and `exit_code` to the `result.push(ContainerInfo { ... })` call.

CURRENT FILE:
