//! Unit installation — download, verify, extract, start, persist.

use std::path::Path;

use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use vasal_protocol::unit::{ManagedUnit, UnitKind};

use super::unit_to_row;
use crate::state::StateStore;

/// Install a managed unit end-to-end.
pub async fn install(
    unit: &ManagedUnit,
    artifact_cache_dir: &Path,
    socket_dir: &Path,
    store: &StateStore,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(unit = %unit.name, version = %unit.version, "installing unit");

    let resp = http_client.get(&unit.artifact.url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Unit(format!(
            "artifact download for {} returned HTTP {}",
            unit.name,
            resp.status(),
        )));
    }
    let bytes = resp.bytes().await?;

    let actual_sha256 = hex::encode(Sha256::digest(&bytes));
    if actual_sha256 != unit.artifact.sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: unit.artifact.sha256.clone(),
            actual: actual_sha256,
        });
    }
    debug!(unit = %unit.name, "SHA-256 verified");

    let install_dir = artifact_cache_dir.join(&unit.name);
    std::fs::create_dir_all(&install_dir)?;

    let url_lower = unit.artifact.url.to_lowercase();
    extract_artifact(&url_lower, &bytes, &install_dir, &unit.name, &unit.version).await?;

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

    let s = store.clone();
    tokio::task::spawn_blocking(move || s.upsert_unit(&row)).await??;

    info!(unit = %unit.name, "unit installed successfully");
    Ok(())
}

/// Extract artifact based on URL extension.
async fn extract_artifact(
    url_lower: &str,
    bytes: &[u8],
    install_dir: &Path,
    unit_name: &str,
    version: &str,
) -> crate::Result<()> {
    const FORMATS: &[(&[&str], &str, &[&str])] = &[
        (&[".tar.gz", ".tgz"], "tar.gz", &["tar", "xzf"]),
        (&[".tar.xz", ".txz"], "tar.xz", &["tar", "xJf"]),
        (&[".tar.bz2", ".tbz2"], "tar.bz2", &["tar", "xjf"]),
        (&[".deb"], "deb", &["dpkg-deb", "--extract"]),
        (&[".zip"], "zip", &["unzip", "-o"]),
    ];

    let install_dir_str = install_dir.to_string_lossy();

    for (extensions, file_ext, cmd_parts) in FORMATS {
        if extensions.iter().any(|ext| url_lower.ends_with(ext)) {
            let artifact_path = install_dir.join(format!("{unit_name}-{version}.{file_ext}"));
            std::fs::write(&artifact_path, bytes)?;
            let artifact_str = artifact_path.to_string_lossy();

            let (cmd, args) = if cmd_parts[0] == "tar" {
                (cmd_parts[0], vec![cmd_parts[1], &artifact_str, "-C", &install_dir_str])
            } else if cmd_parts[0] == "unzip" {
                (cmd_parts[0], vec![cmd_parts[1], &artifact_str, "-d", &install_dir_str])
            } else {
                (cmd_parts[0], vec![cmd_parts[1], &artifact_str, &install_dir_str])
            };

            return run_extract(cmd, &args, unit_name).await;
        }
    }

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

/// Run an extraction command, returning an error on failure.
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
