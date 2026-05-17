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

    // Detect artifact type by URL extension and extract accordingly.
    let url_lower = unit.artifact.url.to_lowercase();
    extract_artifact(&url_lower, &bytes, &install_dir, &unit.name, &unit.version).await?;

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

/// Detect artifact type by URL extension and extract to `install_dir`.
///
/// Supported formats:
/// - `.tar.gz` / `.tgz` — gzip-compressed tarball
/// - `.tar.xz` / `.txz` — xz-compressed tarball
/// - `.tar.bz2` / `.tbz2` — bzip2-compressed tarball
/// - `.deb` — Debian package (extracted via `dpkg-deb`)
/// - `.zip` — ZIP archive
/// - Anything else — treated as a raw binary, written as `<unit_name>`
async fn extract_artifact(
    url_lower: &str,
    bytes: &[u8],
    install_dir: &Path,
    unit_name: &str,
    version: &str,
) -> crate::Result<()> {
    if url_lower.ends_with(".tar.gz") || url_lower.ends_with(".tgz") {
        let artifact_path = install_dir.join(format!("{unit_name}-{version}.tar.gz"));
        std::fs::write(&artifact_path, bytes)?;
        run_extract(
            "tar",
            &[
                "xzf",
                &artifact_path.to_string_lossy(),
                "-C",
                &install_dir.to_string_lossy(),
            ],
            unit_name,
        )
        .await
    } else if url_lower.ends_with(".tar.xz") || url_lower.ends_with(".txz") {
        let artifact_path = install_dir.join(format!("{unit_name}-{version}.tar.xz"));
        std::fs::write(&artifact_path, bytes)?;
        run_extract(
            "tar",
            &[
                "xJf",
                &artifact_path.to_string_lossy(),
                "-C",
                &install_dir.to_string_lossy(),
            ],
            unit_name,
        )
        .await
    } else if url_lower.ends_with(".tar.bz2") || url_lower.ends_with(".tbz2") {
        let artifact_path = install_dir.join(format!("{unit_name}-{version}.tar.bz2"));
        std::fs::write(&artifact_path, bytes)?;
        run_extract(
            "tar",
            &[
                "xjf",
                &artifact_path.to_string_lossy(),
                "-C",
                &install_dir.to_string_lossy(),
            ],
            unit_name,
        )
        .await
    } else if url_lower.ends_with(".deb") {
        let artifact_path = install_dir.join(format!("{unit_name}-{version}.deb"));
        std::fs::write(&artifact_path, bytes)?;
        // Extract .deb contents (data.tar) into install_dir.
        run_extract(
            "dpkg-deb",
            &[
                "--extract",
                &artifact_path.to_string_lossy(),
                &install_dir.to_string_lossy(),
            ],
            unit_name,
        )
        .await
    } else if url_lower.ends_with(".zip") {
        let artifact_path = install_dir.join(format!("{unit_name}-{version}.zip"));
        std::fs::write(&artifact_path, bytes)?;
        run_extract(
            "unzip",
            &[
                "-o",
                &artifact_path.to_string_lossy(),
                "-d",
                &install_dir.to_string_lossy(),
            ],
            unit_name,
        )
        .await
    } else {
        // Raw binary — write directly as the unit name and make executable.
        let binary_path = install_dir.join(unit_name);
        std::fs::write(&binary_path, bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&binary_path, std::fs::Permissions::from_mode(0o755))?;
        }
        debug!(unit = %unit_name, "installed as raw binary");
        Ok(())
    }
}

/// Run an extraction command and return an error if it fails.
async fn run_extract(cmd: &str, args: &[&str], unit_name: &str) -> crate::Result<()> {
    let status = tokio::process::Command::new(cmd)
        .args(args)
        .status()
        .await?;

    if !status.success() {
        return Err(crate::Error::Unit(format!(
            "failed to extract artifact for {unit_name}: {cmd} exited with {:?}",
            status.code(),
        )));
    }
    Ok(())
}
