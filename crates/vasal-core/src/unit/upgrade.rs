//! Unit upgrade with rollback.

use std::path::Path;

use sha2::{Digest, Sha256};
use tracing::{info, warn};
use vasal_protocol::task::RollbackSpec;

use crate::state::StateStore;

/// Upgrade a managed unit, rolling back on failure.
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

    let s = store.clone();
    let name = unit_name.to_owned();
    let current = tokio::task::spawn_blocking(move || s.get_unit(&name)).await??;

    let current =
        current.ok_or_else(|| crate::Error::Unit(format!("unit not found: {unit_name}")))?;

    if let Some(pid) = current.pid {
        info!(unit = %unit_name, pid, "stopping current version");
        let _ = tokio::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

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
            warn!(unit = %unit_name, "extraction failed — rolling back to {}", rb.version);
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

            let healthy = super::health::probe_sidecar(socket_dir, unit_name, 10).await;
            if !healthy {
                warn!(unit = %unit_name, "health check failed — rolling back");
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
                    "health check failed after upgrade: {unit_name}",
                )));
            }
        }
    }

    let s = store.clone();
    tokio::task::spawn_blocking(move || s.upsert_unit(&updated_row)).await??;

    info!(unit = %unit_name, version = %target_version, "upgrade completed");
    Ok(())
}

/// Roll back to a previous version.
async fn perform_rollback(
    unit_name: &str,
    rollback: &RollbackSpec,
    artifact_cache_dir: &Path,
    socket_dir: &Path,
    store: &StateStore,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(unit = %unit_name, rollback_version = %rollback.version, "downloading rollback artifact");

    let resp = http_client.get(&rollback.artifact.url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Unit(format!(
            "rollback artifact download for {} returned HTTP {}",
            unit_name,
            resp.status(),
        )));
    }
    let rb_bytes = resp.bytes().await?;

    let actual_sha256 = hex::encode(Sha256::digest(&rb_bytes));
    if actual_sha256 != rollback.artifact.sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: rollback.artifact.sha256.clone(),
            actual: actual_sha256,
        });
    }

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

    let s = store.clone();
    let name = unit_name.to_owned();
    let current = tokio::task::spawn_blocking(move || s.get_unit(&name)).await??;

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
                info!(unit = %unit_name, version = %rollback.version, "rollback version started");
            }
        }

        let s = store.clone();
        tokio::task::spawn_blocking(move || s.upsert_unit(&row)).await??;
    }

    info!(unit = %unit_name, version = %rollback.version, "rollback completed");
    Ok(())
}
