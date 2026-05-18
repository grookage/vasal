//! Agent self-upgrade with rollback.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingUpgrade {
    pub target_version: String,
    pub previous_version: String,
    pub rollback_path: PathBuf,
    pub initiated_at: i64,
}

const STATE_FILE: &str = "pending-upgrade.json";

/// Check if there's a pending upgrade from a prior restart, consuming the state file.
pub fn check_pending(data_dir: &Path) -> Option<PendingUpgrade> {
    let path = data_dir.join(STATE_FILE);
    if !path.exists() {
        return None;
    }

    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to read pending-upgrade state file");
            let _ = std::fs::remove_file(&path);
            return None;
        }
    };

    match serde_json::from_str::<PendingUpgrade>(&content) {
        Ok(state) => {
            info!(
                target_version = %state.target_version,
                previous_version = %state.previous_version,
                "found pending upgrade from prior restart",
            );
            let _ = std::fs::remove_file(&path);
            Some(state)
        }
        Err(e) => {
            error!(error = %e, "failed to parse pending-upgrade state file");
            let _ = std::fs::remove_file(&path);
            None
        }
    }
}

/// Download, verify, and replace the current binary. Caller must restart after success.
pub async fn execute(
    artifact_url: &str,
    expected_sha256: &str,
    target_version: &str,
    current_version: &str,
    data_dir: &Path,
    http_client: &reqwest::Client,
) -> crate::Result<()> {
    info!(
        target_version = %target_version,
        artifact_url = %artifact_url,
        "starting self-upgrade",
    );

    let resp = http_client.get(artifact_url).send().await?;
    if !resp.status().is_success() {
        return Err(crate::Error::Transport(format!(
            "artifact download returned HTTP {}",
            resp.status(),
        )));
    }
    let bytes = resp.bytes().await?;

    let actual_sha256 = hex::encode(Sha256::digest(&bytes));
    if actual_sha256 != expected_sha256 {
        return Err(crate::Error::Sha256Mismatch {
            expected: expected_sha256.to_owned(),
            actual: actual_sha256,
        });
    }
    info!("SHA-256 verified");

    let current_exe = std::env::current_exe().map_err(|e| {
        crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("cannot determine current executable path: {e}"),
        ))
    })?;
    let backup_path = data_dir.join(format!("vasal.{current_version}.bak"));

    std::fs::copy(&current_exe, &backup_path)?;
    info!(backup = %backup_path.display(), "backed up current binary");

    let state = PendingUpgrade {
        target_version: target_version.to_owned(),
        previous_version: current_version.to_owned(),
        rollback_path: backup_path,
        initiated_at: crate::state::now_ms(),
    };
    let state_path = data_dir.join(STATE_FILE);
    let state_json = serde_json::to_string_pretty(&state)?;
    std::fs::write(&state_path, state_json)?;

    let new_path = data_dir.join("vasal.new");
    std::fs::write(&new_path, &bytes)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(&new_path, perms)?;
    }

    std::fs::rename(&new_path, &current_exe)?;
    info!("binary replaced — restart required to complete upgrade");

    Ok(())
}

/// Roll back a failed upgrade by restoring the backup binary.
pub fn rollback(pending: &PendingUpgrade) -> crate::Result<()> {
    let current_exe = std::env::current_exe().map_err(|e| {
        crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("cannot determine current executable path: {e}"),
        ))
    })?;

    if !pending.rollback_path.exists() {
        warn!(
            path = %pending.rollback_path.display(),
            "rollback binary not found — cannot roll back",
        );
        return Err(crate::Error::Unit("rollback binary not found".into()));
    }

    std::fs::rename(&pending.rollback_path, &current_exe)?;
    info!(
        restored_version = %pending.previous_version,
        "rolled back agent binary",
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn check_pending_no_file() {
        let dir = TempDir::new().unwrap();
        assert!(check_pending(dir.path()).is_none());
    }

    #[test]
    fn check_pending_valid_file() {
        let dir = TempDir::new().unwrap();
        let state = PendingUpgrade {
            target_version: "0.2.0".into(),
            previous_version: "0.1.0".into(),
            rollback_path: dir.path().join("vasal.0.1.0.bak"),
            initiated_at: 1_700_000_000_000,
        };
        let path = dir.path().join(STATE_FILE);
        std::fs::write(&path, serde_json::to_string(&state).unwrap()).unwrap();

        let result = check_pending(dir.path()).unwrap();
        assert_eq!(result.target_version, "0.2.0");
        assert_eq!(result.previous_version, "0.1.0");
        assert!(!path.exists());
    }

    #[test]
    fn check_pending_corrupt_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(STATE_FILE);
        std::fs::write(&path, "not json").unwrap();

        assert!(check_pending(dir.path()).is_none());
        assert!(!path.exists());
    }
}
