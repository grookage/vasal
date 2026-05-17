//! Managed unit definitions.
//!
//! A **managed unit** is any software artifact whose lifecycle the agent
//! controls. Units come in two kinds:
//!
//! - **Sidecar** — speaks the agent's JSON-RPC 2.0 IPC protocol over a Unix
//!   domain socket. The agent can call `submit`, `status`, `cancel`, `health`.
//! - **Package** — a plain binary or system package managed via shell commands.
//!   No IPC channel.
//!
//! Both kinds share identical lifecycle operations: download, verify, install,
//! start, stop, health-check, upgrade, rollback, remove.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ── ManagedUnit ────────────────────────────────────────────────────────────

/// A software artifact managed by the agent.
///
/// The control plane declares which managed units should exist on a host.
/// The agent ensures they do (via task dispatch, not autonomous reconciliation).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManagedUnit {
    /// Unique name (e.g., `"mysql-ctrl"`, `"mariadb-server"`).
    pub name: String,
    /// Whether this unit speaks the sidecar IPC protocol.
    pub kind: UnitKind,
    /// Installed version string (semver recommended, not enforced).
    pub version: String,
    /// Download location and integrity hash.
    pub artifact: Artifact,
    /// Current lifecycle state.
    #[serde(default)]
    pub state: UnitState,
    /// Health check for package units. Sidecars use protocol-mandated
    /// `health()` IPC automatically; this field is only meaningful for
    /// packages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheck>,
    /// Unit-specific configuration (opaque to the agent, forwarded as-is).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    /// IPC socket path. Present only when `kind` is [`UnitKind::Sidecar`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_path: Option<PathBuf>,
}

// ── UnitKind ───────────────────────────────────────────────────────────────

/// Discriminator for managed unit types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitKind {
    /// Speaks the sidecar IPC protocol (JSON-RPC 2.0 over Unix socket).
    Sidecar,
    /// Managed via shell commands only — no IPC channel.
    Package,
}

// ── UnitState ──────────────────────────────────────────────────────────────

/// Lifecycle state of a managed unit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitState {
    /// Binary or package is installed but not running.
    Installed,
    /// Process is running (sidecars only).
    Running,
    /// Explicitly stopped.
    Stopped,
    /// Not present on the host.
    #[default]
    Absent,
}

// ── Artifact ───────────────────────────────────────────────────────────────

/// A downloadable artifact with SHA-256 integrity verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    /// Download URL.
    pub url: String,
    /// Expected SHA-256 hex digest (64 lowercase hex characters).
    pub sha256: String,
}

// ── HealthCheck ────────────────────────────────────────────────────────────

/// Health check configuration for a **package** unit.
///
/// Sidecars use the protocol-mandated `health()` IPC call automatically;
/// this struct is only needed for packages that don't speak IPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Shell command to execute. Exit code 0 = healthy.
    pub command: String,
    /// Timeout for the health check command in milliseconds.
    #[serde(default = "default_health_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_health_timeout_ms() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn managed_unit_sidecar_roundtrip() {
        let unit = ManagedUnit {
            name: "mysql-ctrl".into(),
            kind: UnitKind::Sidecar,
            version: "1.2.0".into(),
            artifact: Artifact {
                url: "https://artifacts.internal/mysql-ctrl-1.2.0.tar.gz".into(),
                sha256: "abcdef1234567890".into(),
            },
            state: UnitState::Running,
            health_check: None,
            config: Some(json!({"port": 3306})),
            socket_path: Some("/run/vasal/mysql-ctrl.sock".into()),
        };
        let json = serde_json::to_string_pretty(&unit).unwrap();
        let parsed: ManagedUnit = serde_json::from_str(&json).unwrap();
        assert_eq!(unit, parsed);
    }

    #[test]
    fn managed_unit_package_roundtrip() {
        let unit = ManagedUnit {
            name: "mariadb-server".into(),
            kind: UnitKind::Package,
            version: "10.6.12".into(),
            artifact: Artifact {
                url: "https://artifacts.internal/mariadb-10.6.12.deb".into(),
                sha256: "deadbeef".into(),
            },
            state: UnitState::Installed,
            health_check: Some(HealthCheck {
                command: "systemctl is-active mariadb".into(),
                timeout_ms: 3000,
            }),
            config: None,
            socket_path: None,
        };
        let json = serde_json::to_string(&unit).unwrap();
        let parsed: ManagedUnit = serde_json::from_str(&json).unwrap();
        assert_eq!(unit, parsed);
    }

    #[test]
    fn unit_state_default_is_absent() {
        assert_eq!(UnitState::default(), UnitState::Absent);
    }

    #[test]
    fn health_check_default_timeout() {
        let json = r#"{"command":"true"}"#;
        let hc: HealthCheck = serde_json::from_str(json).unwrap();
        assert_eq!(hc.timeout_ms, 5_000);
    }

    #[test]
    fn unit_kind_serialization() {
        assert_eq!(
            serde_json::to_string(&UnitKind::Sidecar).unwrap(),
            r#""sidecar""#
        );
        assert_eq!(
            serde_json::to_string(&UnitKind::Package).unwrap(),
            r#""package""#
        );
    }
}
