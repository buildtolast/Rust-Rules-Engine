# Spec: add restart loop for watch_and_reload in bin/rules-engine/src/main.rs

## Context
File: bin/rules-engine/src/main.rs
Full file content is provided below — output the COMPLETE modified file.

## Change required

Replace this block:
```rust
let listener = store_postgres::RuleChangeListener::connect(&pool)
    .await
    .context("pg listener")?;
let cache_bg = cache.clone();
let repo_bg = repo.clone();
tokio::spawn(async move {
    if let Err(e) = pipeline::watch_and_reload(cache_bg, repo_bg, listener).await {
        tracing::error!("hot-reload error: {e}");
    }
});
```

With this (note: `listener` is consumed by `watch_and_reload` each call, so use a `current_listener` variable that gets replaced on reconnect):
```rust
let listener = store_postgres::RuleChangeListener::connect(&pool)
    .await
    .context("pg listener")?;
let pool_bg = pool.clone();
let cache_bg = cache.clone();
let repo_bg = repo.clone();
tokio::spawn(async move {
    let mut current_listener = listener;
    let mut backoff = std::time::Duration::from_secs(1);
    loop {
        match pipeline::watch_and_reload(cache_bg.clone(), repo_bg.clone(), current_listener).await {
            Ok(()) => break,
            Err(e) => {
                tracing::error!("hot-reload error: {e} — reconnecting in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                match store_postgres::RuleChangeListener::connect(&pool_bg).await {
                    Ok(new_listener) => {
                        current_listener = new_listener;
                        backoff = std::time::Duration::from_secs(1);
                    }
                    Err(re) => {
                        tracing::error!("failed to reconnect pg listener: {re}");
                        return;
                    }
                }
            }
        }
    }
});
```

## Constraints
- Output EXACTLY ONE fenced ```rust code block containing the complete file.
- Do NOT add `mut` to `cache_bg` or `repo_bg` — they are not mutated.
- Do NOT change any other logic in the file.
- Preserve all comments exactly.
- There must be NO `mut cache_bg` or `mut repo_bg` declarations — only `mut current_listener` and `mut backoff`.
