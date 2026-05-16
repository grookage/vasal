//! Heartbeat payload types.
//!
//! The agent sends a heartbeat to the control plane at a configured interval
//! (DD-17). The heartbeat carries:
//!
//! - Agent identity and version
//! - Status of all managed units (the CP diffs this against its desired state)
//! - Summary counts of active tasks

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sidecar::HealthStatus;
use crate::unit::UnitKind;

// ── Heartbeat ──────────────────────────────────────────────────────────────

/// Periodic heartbeat payload sent from the agent to the control plane.
///
/// ```json
/// {
///   "agent_id": "550e8400-e29b-41d4-a716-446655440000",
///   "agent_version": "0.1.0",
///   "uptime_sec": 3600,
///   "timestamp": 1700000000000,
///   "units": [ ... ],
///   "active_tasks": { "oneshot": 2, "continuous": 3, "total": 5 }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Heartbeat {
    /// Agent identifier (assigned at registration, persisted in config).
    pub agent_id: Uuid,
    /// Running agent version string.
    pub agent_version: String,
    /// Seconds since the agent process started.
    pub uptime_sec: u64,
    /// Unix epoch timestamp in milliseconds.
    pub timestamp: u64,
    /// Status snapshot of every managed unit.
    #[serde(default)]
    pub units: Vec<UnitReport>,
    /// Summary of currently active tasks.
    #[serde(default)]
    pub active_tasks: ActiveTaskCounts,
}

// ── UnitReport ─────────────────────────────────────────────────────────────

/// Status snapshot of a single managed unit, included in every heartbeat.
///
/// The control plane diffs these reports against its internal desired state
/// and dispatches lifecycle tasks as needed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnitReport {
    /// Unit name (e.g., `"mysql-ctrl"`).
    pub name: String,
    /// Sidecar or package.
    pub kind: UnitKind,
    /// Currently installed version.
    pub version: String,
    /// Current lifecycle state (e.g., `"running"`, `"installed"`, `"stopped"`).
    pub state: String,
    /// Health status from the most recent health check (if available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<HealthStatus>,
    /// Process ID (if the unit is running).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Error detail when health is [`HealthStatus::Degraded`] or
    /// [`HealthStatus::Unhealthy`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_error: Option<String>,
}

// ── ActiveTaskCounts ───────────────────────────────────────────────────────

/// Summary counts of active tasks, included in every heartbeat.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTaskCounts {
    /// Number of currently running one-shot tasks.
    pub oneshot: u32,
    /// Number of currently running continuous tasks.
    pub continuous: u32,
    /// Total active tasks (`oneshot + continuous`).
    pub total: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_roundtrip() {
        let hb = Heartbeat {
            agent_id: Uuid::nil(),
            agent_version: "0.1.0".into(),
            uptime_sec: 3600,
            timestamp: 1_700_000_000_000,
            units: vec![
                UnitReport {
                    name: "mysql-ctrl".into(),
                    kind: UnitKind::Sidecar,
                    version: "1.2.0".into(),
                    state: "running".into(),
                    health: Some(HealthStatus::Ok),
                    pid: Some(4521),
                    health_error: None,
                },
                UnitReport {
                    name: "mariadb-server".into(),
                    kind: UnitKind::Package,
                    version: "10.6.12".into(),
                    state: "installed".into(),
                    health: None,
                    pid: None,
                    health_error: None,
                },
            ],
            active_tasks: ActiveTaskCounts {
                oneshot: 2,
                continuous: 3,
                total: 5,
            },
        };
        let json = serde_json::to_string_pretty(&hb).unwrap();
        let parsed: Heartbeat = serde_json::from_str(&json).unwrap();
        assert_eq!(hb, parsed);
    }

    #[test]
    fn active_task_counts_default() {
        let counts = ActiveTaskCounts::default();
        assert_eq!(counts.oneshot, 0);
        assert_eq!(counts.continuous, 0);
        assert_eq!(counts.total, 0);
    }

    #[test]
    fn unit_report_optional_fields_absent_when_none() {
        let report = UnitReport {
            name: "test".into(),
            kind: UnitKind::Package,
            version: "1.0.0".into(),
            state: "installed".into(),
            health: None,
            pid: None,
            health_error: None,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("health"));
        assert!(!json.contains("pid"));
        assert!(!json.contains("health_error"));
    }
}
