//! Unit installation — download, verify, extract, start (DD-04).
//!
//! Handles the `install` task type. Downloads an artifact, verifies its
//! SHA-256 digest, installs it (extract tarball or run package manager),
//! starts the unit, and persists it to the state store.

use std::path::Path;

use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use vasal_protocol::unit::{ManagedUnit, UnitKind};

use super::unit_to_row;
use crate::state::StateStore;

/// Install a managed unit.
///
/// 1. Download the artifact from `unit.artifact.url`.
/// 2. Verify SHA-256 digest.
/// 3. Extract/install to the artifact cache directory.
/// 4. For sidecars: start the process, wait for health check.
/// 5. Persist to state store.
pub async fn install(
    unit: &ManagedUnit,
    artifact_cache_dir: &Path,
    socket_dir: &Path,
    store: &StateStore,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(unit = %unit.name, version = %unit.version, "installing unit");

    // 1. Download artifact.
    let resp = http_client.get(&unit.artifact.url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Unit(format!(
            "artifact download for {} returned HTTP {}",
            unit.name,
            resp.status(),
        )));
    }
    let bytes = resp.bytes().await?;

    // 2. Verify SHA-256.
    let actual_sha256 = hex::encode(Sha256::digest(&bytes));
    if actual_sha256 != unit.artifact.sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: unit.artifact.sha256.clone(),
            actual: actual_sha256,
        });
    }
    debug!(unit = %unit.name, "SHA-256 verified");

    // 3. Extract to artifact cache.
    let install_dir = artifact_cache_dir.join(&unit.name);
    std::fs::create_dir_all(&install_dir)?;
    let artifact_path = install_dir.join(format!("{}-{}.tar.gz", unit.name, unit.version));
    std::fs::write(&artifact_path, &bytes)?;

    // TODO: detect artifact type and extract accordingly.
    // For now, assume tarball and extract with tar.
    let status = tokio::process::Command::new("tar")
        .args(["xzf", &artifact_path.to_string_lossy(), "-C", &install_dir.to_string_lossy()])
        .status()
        .await?;

    if !status.success() {
        return Err(crate::Error::Unit(format!(
            "failed to extract artifact for {}: tar exited with {:?}",
            unit.name,
            status.code(),
        )));
    }

    // 4. For sidecars: start the process.
    let mut row = unit_to_row(unit);
    if unit.kind == UnitKind::Sidecar {
        let binary_path = install_dir.join(&unit.name);
        let socket = super::socket_path_for(socket_dir, &unit.name);

        if binary_path.exists() {
            let child = tokio::process::Command::new(&binary_path)
                .arg("--socket")
                .arg(&socket)
                .spawn()?;

            row.pid = Some(child.id().unwrap_or(0));
            row.socket_path = Some(socket.to_string_lossy().into_owned());
            row.state = "running".into();

            info!(unit = %unit.name, pid = row.pid, "sidecar started");
        } else {
            warn!(unit = %unit.name, "binary not found after extraction");
            row.state = "installed".into();
        }
    } else {
        row.state = "installed".into();
    }

    // 5. Persist to state store.
    let store_clone = store.clone();
    tokio::task::spawn_blocking(move || store_clone.upsert_unit(&row)).await??;

    info!(unit = %unit.name, "unit installed successfully");
    Ok(())
}
