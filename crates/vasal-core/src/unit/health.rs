//! Periodic unit health checking via IPC or shell commands.

use std::path::Path;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use vasal_protocol::sidecar::{HealthResponse, HealthStatus};

use crate::config::RuntimeConfig;
use crate::state::StateStore;

/// Probe a sidecar's health with retries until `timeout_sec` elapses.
pub async fn probe_sidecar(socket_dir: &Path, unit_name: &str, timeout_sec: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_sec);
    let mut attempt = 0u32;

    loop {
        if tokio::time::Instant::now() >= deadline {
            warn!(unit = %unit_name, "health probe timed out");
            return false;
        }

        let (health, _error) = check_sidecar(socket_dir, unit_name).await;
        if health == "ok" || health == "degraded" {
            debug!(unit = %unit_name, attempt, "health probe passed");
            return true;
        }

        attempt += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run the health check loop for all managed units.
pub async fn run(
    store: StateStore,
    socket_dir: std::path::PathBuf,
    runtime_rx: watch::Receiver<RuntimeConfig>,
    shutdown: CancellationToken,
) {
    info!("unit health checker started");

    loop {
        let interval_sec = runtime_rx.borrow().health_check_interval_sec;
        let interval = Duration::from_secs(interval_sec);

        tokio::select! {
            () = shutdown.cancelled() => {
                info!("unit health checker stopped");
                return;
            }
            () = tokio::time::sleep(interval) => {}
        }

        let s = store.clone();
        let units = match tokio::task::spawn_blocking(move || s.list_units()).await {
            Ok(Ok(u)) => u,
            Ok(Err(e)) => {
                warn!(error = %e, "failed to list units for health check");
                continue;
            }
            Err(e) => {
                warn!(error = %e, "spawn_blocking failed");
                continue;
            }
        };

        for unit in &units {
            if unit.state != "running" && unit.state != "installed" {
                continue;
            }

            let (health, health_error) = match unit.kind.as_str() {
                "sidecar" => check_sidecar(&socket_dir, &unit.name).await,
                "package" => check_package(unit).await,
                _ => continue,
            };

            let changed = unit.health.as_deref() != Some(health)
                || unit.health_error.as_deref() != health_error.as_deref();

            if changed {
                debug!(
                    unit = %unit.name,
                    old_health = ?unit.health,
                    new_health = %health,
                    "health status changed",
                );

                let mut updated = unit.clone();
                updated.health = Some(health.to_owned());
                updated.health_error = health_error;
                updated.updated_at = crate::state::now_ms();

                let s = store.clone();
                let _ =
                    tokio::task::spawn_blocking(move || s.upsert_unit(&updated)).await;
            }
        }
    }
}

async fn check_sidecar(socket_dir: &Path, unit_name: &str) -> (&'static str, Option<String>) {
    let socket_path = super::socket_path_for(socket_dir, unit_name);

    match crate::task::sidecar::call_raw(&socket_path, "health", None).await {
        Ok(resp) => {
            if let Some(result) = resp.result {
                if let Ok(hr) = serde_json::from_value::<HealthResponse>(result) {
                    return match hr.status {
                        HealthStatus::Ok => ("ok", None),
                        HealthStatus::Degraded => ("degraded", hr.error),
                        HealthStatus::Unhealthy => ("unhealthy", hr.error),
                    };
                }
            }
            if let Some(err) = resp.error {
                return ("unhealthy", Some(err.message));
            }
            ("ok", None)
        }
        Err(e) => ("unhealthy", Some(e.to_string())),
    }
}

async fn check_package(unit: &crate::state::UnitRow) -> (&'static str, Option<String>) {
    let config_json = match &unit.config_json {
        Some(c) => c,
        None => return ("ok", None),
    };

    let config: serde_json::Value = match serde_json::from_str(config_json) {
        Ok(v) => v,
        Err(_) => return ("ok", None),
    };

    let command = match config
        .get("health_check")
        .and_then(|hc| hc.get("command"))
        .and_then(|c| c.as_str())
    {
        Some(c) => c,
        None => return ("ok", None),
    };

    let timeout_ms = config
        .get("health_check")
        .and_then(|hc| hc.get("timeout_ms"))
        .and_then(|t| t.as_u64())
        .unwrap_or(5000);

    let result = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(command)
            .output(),
    )
    .await;

    match result {
        Ok(Ok(output)) if output.status.success() => ("ok", None),
        Ok(Ok(output)) => {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            (
                "unhealthy",
                Some(format!(
                    "exit code {:?}: {}",
                    output.status.code(),
                    stderr.trim()
                )),
            )
        }
        Ok(Err(e)) => ("unhealthy", Some(e.to_string())),
        Err(_) => ("unhealthy", Some("health check timed out".into())),
    }
}
