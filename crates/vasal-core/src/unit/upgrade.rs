//! Unit upgrade with rollback (DD-04).
//!
//! 1. Download new artifact, verify SHA-256.
//! 2. Stop current version.
//! 3. Install new version.
//! 4. Start, health check.
//! 5. On health failure → rollback (stop new, install old, start old).

use std::path::Path;

use sha2::{Digest, Sha256};
use tracing::{info, warn};
use vasal_protocol::task::RollbackSpec;

use crate::state::StateStore;

/// Upgrade a managed unit.
#[allow(clippy::too_many_arguments)]
pub async fn upgrade(
    unit_name: &str,
    target_version: &str,
    artifact_url: &str,
    artifact_sha256: &str,
    rollback: Option<&RollbackSpec>,
    artifact_cache_dir: &Path,
    socket_dir: &Path,
    store: &StateStore,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(unit = %unit_name, target_version, "upgrading unit");

    // 1. Download and verify new artifact.
    let resp = http_client.get(artifact_url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Unit(format!(
            "artifact download for {} returned HTTP {}",
            unit_name,
            resp.status(),
        )));
    }
    let bytes = resp.bytes().await?;

    let actual_sha256 = hex::encode(Sha256::digest(&bytes));
    if actual_sha256 != artifact_sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: artifact_sha256.to_owned(),
            actual: actual_sha256,
        });
    }

    // 2. Look up current unit info.
    let store_clone = store.clone();
    let name = unit_name.to_owned();
    let current = tokio::task::spawn_blocking(move || store_clone.get_unit(&name)).await??;

    let current = current
        .ok_or_else(|| crate::Error::Unit(format!("unit {unit_name} not found in state store")))?;

    // 3. Stop current version (if sidecar with PID).
    if let Some(pid) = current.pid {
        info!(unit = %unit_name, pid, "stopping current version");
        // Send SIGTERM via kill command.
        let _ = tokio::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await;
        // Give it a moment to stop.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // 4. Install new version.
    let install_dir = artifact_cache_dir.join(unit_name);
    std::fs::create_dir_all(&install_dir)?;
    let artifact_path = install_dir.join(format!("{unit_name}-{target_version}.tar.gz"));
    std::fs::write(&artifact_path, &bytes)?;

    let status = tokio::process::Command::new("tar")
        .args([
            "xzf",
            &artifact_path.to_string_lossy(),
            "-C",
            &install_dir.to_string_lossy(),
        ])
        .status()
        .await?;

    if !status.success() {
        if let Some(rb) = rollback {
            warn!(unit = %unit_name, "extraction failed — attempting rollback to {}", rb.version);
            perform_rollback(
                unit_name,
                rb,
                artifact_cache_dir,
                socket_dir,
                store,
                http_client,
            )
            .await?;
        }
        return Err(crate::Error::Unit(format!(
            "failed to extract artifact for {unit_name}",
        )));
    }

    // 5. Start new version (if sidecar).
    let mut updated_row = current.clone();
    updated_row.version = target_version.to_owned();
    updated_row.updated_at = crate::state::now_ms();

    if current.kind == "sidecar" {
        let binary_path = install_dir.join(unit_name);
        let socket = super::socket_path_for(socket_dir, unit_name);

        if binary_path.exists() {
            let child = tokio::process::Command::new(&binary_path)
                .arg("--socket")
                .arg(&socket)
                .spawn()?;

            updated_row.pid = Some(child.id().unwrap_or(0));
            updated_row.state = "running".into();
            info!(unit = %unit_name, version = %target_version, "new version started");

            // 5a. Health check the new version — give it up to 10 seconds.
            let healthy = super::health::probe_sidecar(socket_dir, unit_name, 10).await;
            if !healthy {
                warn!(unit = %unit_name, "new version failed health check — rolling back");
                // Kill the new version.
                let _ = tokio::process::Command::new("kill")
                    .arg("-TERM")
                    .arg(updated_row.pid.unwrap_or(0).to_string())
                    .status()
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                if let Some(rb) = rollback {
                    perform_rollback(
                        unit_name,
                        rb,
                        artifact_cache_dir,
                        socket_dir,
                        store,
                        http_client,
                    )
                    .await?;
                }
                return Err(crate::Error::Unit(format!(
                    "new version of {unit_name} failed health check after upgrade",
                )));
            }
        }
    }

    // 6. Persist updated state.
    let store_clone = store.clone();
    tokio::task::spawn_blocking(move || store_clone.upsert_unit(&updated_row)).await??;

    info!(unit = %unit_name, version = %target_version, "upgrade completed");
    Ok(())
}

/// Download, verify, extract, and start the rollback version of a unit.
async fn perform_rollback(
    unit_name: &str,
    rollback: &RollbackSpec,
    artifact_cache_dir: &Path,
    socket_dir: &Path,
    store: &StateStore,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(
        unit = %unit_name,
        rollback_version = %rollback.version,
        "downloading rollback artifact",
    );

    // 1. Download rollback artifact.
    let resp = http_client.get(&rollback.artifact.url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Unit(format!(
            "rollback artifact download for {} returned HTTP {}",
            unit_name,
            resp.status(),
        )));
    }
    let rb_bytes = resp.bytes().await?;

    // 2. Verify SHA-256.
    let actual_sha256 = hex::encode(Sha256::digest(&rb_bytes));
    if actual_sha256 != rollback.artifact.sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: rollback.artifact.sha256.clone(),
            actual: actual_sha256,
        });
    }

    // 3. Extract rollback artifact.
    let install_dir = artifact_cache_dir.join(unit_name);
    std::fs::create_dir_all(&install_dir)?;
    let rb_path = install_dir.join(format!("{unit_name}-{}.tar.gz", rollback.version));
    std::fs::write(&rb_path, &rb_bytes)?;

    let status = tokio::process::Command::new("tar")
        .args([
            "xzf",
            &rb_path.to_string_lossy(),
            "-C",
            &install_dir.to_string_lossy(),
        ])
        .status()
        .await?;

    if !status.success() {
        return Err(crate::Error::Unit(format!(
            "failed to extract rollback artifact for {unit_name}",
        )));
    }

    // 4. Start rollback version (if sidecar).
    let store_clone = store.clone();
    let name = unit_name.to_owned();
    let current = tokio::task::spawn_blocking(move || store_clone.get_unit(&name)).await??;

    if let Some(mut row) = current {
        row.version = rollback.version.clone();
        row.updated_at = crate::state::now_ms();

        if row.kind == "sidecar" {
            let binary_path = install_dir.join(unit_name);
            let socket = super::socket_path_for(socket_dir, unit_name);

            if binary_path.exists() {
                let child = tokio::process::Command::new(&binary_path)
                    .arg("--socket")
                    .arg(&socket)
                    .spawn()?;

                row.pid = Some(child.id().unwrap_or(0));
                row.state = "running".into();
                info!(
                    unit = %unit_name,
                    version = %rollback.version,
                    "rollback version started",
                );
            }
        }

        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || store_clone.upsert_unit(&row)).await??;
    }

    info!(unit = %unit_name, version = %rollback.version, "rollback completed");
    Ok(())
}
