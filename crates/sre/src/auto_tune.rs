use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct AutoTuner {
    cooldowns: Mutex<HashMap<String, Instant>>,
    cooldown: Duration,
    compose_file: String,
    env_file: String,
    pub enabled: bool,
}

impl AutoTuner {
    pub fn new(enabled: bool, compose_file: String, env_file: String, cooldown_secs: u64) -> Self {
        Self {
            cooldowns: Mutex::new(HashMap::new()),
            cooldown: Duration::from_secs(cooldown_secs),
            compose_file,
            env_file,
            enabled,
        }
    }

    /// Called when a CRITICAL memory finding fires. Returns a human-readable
    /// description of the action taken, or `None` if skipped (disabled,
    /// cooldown, unknown service, or command failed).
    pub async fn maybe_tune(&self, container_name: &str, mem_used_bytes: u64) -> Option<String> {
        if !self.enabled {
            return None;
        }

        let (service, env_var) = service_mapping(container_name)?;

        {
            let cooldowns = self.cooldowns.lock().unwrap();
            if let Some(last) = cooldowns.get(service) {
                if last.elapsed() < self.cooldown {
                    tracing::debug!(service, "auto-tune: still in cooldown, skipping");
                    return None;
                }
            }
        }

        let new_limit_mb = (mem_used_bytes as f64 / 0.4 / 1_048_576.0).ceil() as u64;

        tracing::info!(
            container = container_name,
            service,
            env_var,
            new_limit_mb,
            "auto-tune: applying memory limit increase"
        );

        if let Err(e) = update_env_file(&self.env_file, env_var, new_limit_mb) {
            tracing::warn!("auto-tune: failed to write .env ({e}), aborting");
            return None;
        }

        let result = tokio::process::Command::new("docker")
            .args([
                "compose",
                "-f",
                &self.compose_file,
                "--env-file",
                &self.env_file,
                "up",
                "-d",
                "--no-deps",
                service,
            ])
            .output()
            .await;

        match result {
            Ok(o) if o.status.success() => {
                let mut cooldowns = self.cooldowns.lock().unwrap();
                cooldowns.insert(service.to_string(), Instant::now());
                let msg = format!(
                    "auto-tuned {env_var}={new_limit_mb}M for service '{service}' — container restarting"
                );
                tracing::info!("{msg}");
                Some(msg)
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::warn!("auto-tune: docker compose failed: {stderr}");
                None
            }
            Err(e) => {
                tracing::warn!("auto-tune: could not exec docker CLI: {e}");
                None
            }
        }
    }
}

/// Maps a container name (e.g. `rre-app-1`) to (compose service, env var).
/// Returns `None` for unknown or excluded services (e.g. sre-agent tunes itself
/// only if you know what you're doing — excluded here to avoid restart loops).
fn service_mapping(container_name: &str) -> Option<(&'static str, &'static str)> {
    let bare = container_name.trim_start_matches("rre-");
    let (service, env_var) = if bare.starts_with("app-") || bare == "app" {
        ("app", "APP_MEM_LIMIT")
    } else if bare == "clickhouse" {
        ("clickhouse", "CLICKHOUSE_MEM_LIMIT")
    } else if bare == "postgres" {
        ("postgres", "POSTGRES_MEM_LIMIT")
    } else if bare == "postgres-replica" {
        ("postgres-replica", "POSTGRES_MEM_LIMIT")
    } else if bare == "frontend" {
        ("frontend", "FRONTEND_MEM_LIMIT")
    } else if bare == "redpanda-0" {
        ("redpanda-0", "REDPANDA_MEM_LIMIT")
    } else if bare == "redpanda-1" {
        ("redpanda-1", "REDPANDA_MEM_LIMIT")
    } else if bare == "redpanda-2" {
        ("redpanda-2", "REDPANDA_MEM_LIMIT")
    } else {
        // sre-agent, signoz-*, otel-* — excluded
        return None;
    };
    Some((service, env_var))
}

/// Update or append `KEY=<value>M` in a simple `.env` file (key=value, one per line).
fn update_env_file(path: &str, key: &str, value_mb: u64) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let prefix = format!("{key}=");
    let new_line = format!("{key}={value_mb}M");
    let mut found = false;

    let mut lines: Vec<String> = content
        .lines()
        .map(|l| {
            if l.starts_with(&prefix) {
                found = true;
                new_line.clone()
            } else {
                l.to_string()
            }
        })
        .collect();

    if !found {
        lines.push(new_line);
    }

    std::fs::write(path, lines.join("\n") + "\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_env(content: &str) -> (tempfile::NamedTempFile, String) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{content}").unwrap();
        let path = f.path().to_str().unwrap().to_string();
        (f, path)
    }

    #[test]
    fn updates_existing_key() {
        let (_f, path) = tmp_env("FOO=512M\nBAR=1G\n");
        update_env_file(&path, "FOO", 1024).unwrap();
        let result = std::fs::read_to_string(&path).unwrap();
        assert!(result.contains("FOO=1024M"), "got: {result}");
        assert!(result.contains("BAR=1G"), "got: {result}");
    }

    #[test]
    fn appends_missing_key() {
        let (_f, path) = tmp_env("BAR=1G\n");
        update_env_file(&path, "NEW_LIMIT", 2048).unwrap();
        let result = std::fs::read_to_string(&path).unwrap();
        assert!(result.contains("NEW_LIMIT=2048M"), "got: {result}");
    }

    #[test]
    fn service_mapping_app() {
        assert_eq!(service_mapping("rre-app-1"), Some(("app", "APP_MEM_LIMIT")));
        assert_eq!(service_mapping("rre-app-2"), Some(("app", "APP_MEM_LIMIT")));
    }

    #[test]
    fn service_mapping_redpanda() {
        assert_eq!(
            service_mapping("rre-redpanda-0"),
            Some(("redpanda-0", "REDPANDA_MEM_LIMIT"))
        );
    }

    #[test]
    fn service_mapping_excludes_sre() {
        assert!(service_mapping("rre-sre-agent-1").is_none());
    }
}
