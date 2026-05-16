//! Task dispatch types.
//!
//! A [`Task`] is the fundamental unit of work dispatched by a control plane to
//! the Vasal agent. Tasks are discriminated by an explicit `type` field on the
//! wire (DD-07b):
//!
//! | Type | Purpose |
//! |---|---|
//! | `exec` | Execute a command (shell or sidecar) |
//! | `cancel` | Cancel a running task |
//! | `install` | Install a managed unit |
//! | `upgrade` | Upgrade a managed unit |
//! | `remove` | Remove a managed unit |
//! | `self_upgrade` | Upgrade the agent binary |
//!
//! This module also defines [`TaskChain`] for sequential multi-step execution
//! with rollback, and [`TaskResult`] for reporting outcomes to the CP.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::credential::CredentialRef;
use crate::unit::{Artifact, ManagedUnit};

// ── Priority ───────────────────────────────────────────────────────────────

/// Task execution priority, from highest to lowest urgency.
///
/// The agent may use priority to order its execution queue. `Critical` tasks
/// are expected to preempt lower-priority work where possible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Critical,
    High,
    #[default]
    Normal,
    Low,
}

// ── Executor ───────────────────────────────────────────────────────────────

/// Which executor handles an exec task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Executor {
    /// The built-in shell executor (the *only* built-in executor — DD-01).
    Shell,
    /// Dispatch to a named sidecar over Unix socket IPC.
    Sidecar,
}

// ── ExecKind ───────────────────────────────────────────────────────────────

/// Execution lifecycle model for exec tasks (DD-07).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecKind {
    /// Execute once, capture output, report result.
    Oneshot,
    /// Execute repeatedly at a defined interval until the CP cancels.
    Continuous,
}

// ── Task ───────────────────────────────────────────────────────────────────

/// A task dispatched by the control plane.
///
/// On the wire, the `type` field discriminates the variant:
///
/// ```json
/// { "type": "exec", "id": "...", "priority": "normal", "kind": "oneshot", ... }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Task {
    /// Execute a command via shell or sidecar.
    Exec(ExecTask),
    /// Cancel a running task.
    Cancel(CancelTask),
    /// Install a managed unit (sidecar or package).
    Install(InstallTask),
    /// Upgrade a managed unit to a new version.
    Upgrade(UpgradeTask),
    /// Remove a managed unit.
    Remove(RemoveTask),
    /// Upgrade the agent binary itself.
    SelfUpgrade(SelfUpgradeTask),
}

impl Task {
    /// Returns the task's unique identifier.
    pub fn id(&self) -> Uuid {
        match self {
            Self::Exec(t) => t.id,
            Self::Cancel(t) => t.id,
            Self::Install(t) => t.id,
            Self::Upgrade(t) => t.id,
            Self::Remove(t) => t.id,
            Self::SelfUpgrade(t) => t.id,
        }
    }

    /// Returns the task's execution priority.
    pub fn priority(&self) -> Priority {
        match self {
            Self::Exec(t) => t.priority,
            Self::Cancel(t) => t.priority,
            Self::Install(t) => t.priority,
            Self::Upgrade(t) => t.priority,
            Self::Remove(t) => t.priority,
            Self::SelfUpgrade(t) => t.priority,
        }
    }

    /// Returns a reference to the task's opaque tag map.
    pub fn tags(&self) -> &HashMap<String, String> {
        match self {
            Self::Exec(t) => &t.tags,
            Self::Cancel(t) => &t.tags,
            Self::Install(t) => &t.tags,
            Self::Upgrade(t) => &t.tags,
            Self::Remove(t) => &t.tags,
            Self::SelfUpgrade(t) => &t.tags,
        }
    }
}

// ── ExecTask ───────────────────────────────────────────────────────────────

/// Execute a command — one-shot or continuous.
///
/// - **Oneshot**: execute once, capture output, report result.
/// - **Continuous**: execute at `interval_ms`, report on every tick until
///   the CP sends a cancel (DD-07).
///
/// The `executor` field selects between the built-in shell and sidecar IPC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecTask {
    /// Unique task identifier.
    pub id: Uuid,
    /// Execution priority.
    #[serde(default)]
    pub priority: Priority,
    /// Opaque CP metadata, passed through in reports.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// One-shot or continuous execution model.
    pub kind: ExecKind,
    /// Shell or sidecar.
    pub executor: Executor,
    /// Target sidecar name. **Required** when `executor` is `Sidecar`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// JSON-RPC method to call on the sidecar. **Required** when `executor`
    /// is `Sidecar`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Payload forwarded to the executor — script text for shell, arbitrary
    /// JSON for sidecars.
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Tick interval in milliseconds. **Required** when `kind` is `Continuous`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
    /// Per-execution timeout in milliseconds.
    pub timeout_ms: u64,
    /// Credentials to resolve before (or during) execution.
    #[serde(default)]
    pub credentials: Vec<CredentialRef>,
}

// ── CancelTask ─────────────────────────────────────────────────────────────

/// Cancel a currently running task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelTask {
    /// Unique identifier for this cancel task.
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// The identifier of the task to cancel.
    pub target_task_id: Uuid,
}

// ── InstallTask ────────────────────────────────────────────────────────────

/// Install a managed unit (sidecar or package).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Full specification of the unit to install.
    pub unit: ManagedUnit,
}

// ── UpgradeTask ────────────────────────────────────────────────────────────

/// Upgrade a managed unit to a new version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpgradeTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Name of the unit to upgrade.
    pub unit_name: String,
    /// Target version string.
    pub target_version: String,
    /// Artifact for the new version.
    pub artifact: Artifact,
    /// Rollback specification in case the upgrade fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSpec>,
}

/// Rollback specification for a failed upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RollbackSpec {
    /// The version to restore.
    pub version: String,
    /// The artifact to reinstall.
    pub artifact: Artifact,
}

// ── RemoveTask ─────────────────────────────────────────────────────────────

/// Remove a managed unit from the host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoveTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Name of the unit to remove.
    pub unit_name: String,
    /// If `true`, also remove configuration and data (not just the binary).
    #[serde(default)]
    pub purge: bool,
}

// ── SelfUpgradeTask ────────────────────────────────────────────────────────

/// Upgrade the agent binary itself (DD-08).
///
/// The agent downloads the new binary, verifies its SHA-256, performs an
/// atomic rename, and restarts. If the new binary fails its health check
/// within the configured timeout, it rolls back.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelfUpgradeTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Target agent version.
    pub target_version: String,
    /// New agent binary artifact.
    pub artifact: Artifact,
    /// Rollback specification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSpec>,
}

// ── TaskChain ──────────────────────────────────────────────────────────────

/// A sequential chain of exec tasks with rollback support (DD-07a).
///
/// Steps execute strictly in order. On failure, the agent rolls back
/// according to [`on_failure`](TaskChain::on_failure). This reduces
/// CP-to-agent round-trips at scale — one dispatch, one result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskChain {
    /// Unique chain identifier.
    pub id: Uuid,
    /// Ordered steps to execute.
    pub steps: Vec<ChainStep>,
    /// Rollback strategy on step failure.
    #[serde(default)]
    pub on_failure: RollbackStrategy,
    /// Opaque CP metadata.
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// A single step in a [`TaskChain`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChainStep {
    /// The action to perform.
    pub task: ExecTask,
    /// Optional rollback action, executed if this step (or a later step) fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<ExecTask>,
}

/// Strategy for handling chain step failures.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackStrategy {
    /// Rollback only the failed step, then abort the chain.
    RollbackFailed,
    /// Rollback the failed step, then all prior steps in reverse order.
    #[default]
    RollbackAll,
}

// ── TaskResult ─────────────────────────────────────────────────────────────

/// Result of a task execution, reported from the agent to the control plane.
///
/// For continuous tasks, one `TaskResult` is sent per tick with the same
/// `task_id`. For chain steps, `chain_id` and `step_index` identify the
/// position within the chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskResult {
    /// The task that produced this result.
    pub task_id: Uuid,
    /// Chain identifier, if this result is from a chain step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<Uuid>,
    /// Zero-indexed step within the chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<u32>,
    /// Outcome of the execution.
    pub status: TaskResultStatus,
    /// Shell exit code (if applicable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Captured stdout.
    #[serde(default)]
    pub stdout: String,
    /// Captured stderr.
    #[serde(default)]
    pub stderr: String,
    /// Wall-clock execution duration in milliseconds.
    pub duration_ms: u64,
    /// Unix epoch timestamp in milliseconds when this result was produced.
    pub timestamp: u64,
    /// Human-readable error description on failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Outcome of a task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultStatus {
    /// Task completed successfully.
    Success,
    /// Task failed.
    Failed,
    /// Task was cancelled by the CP.
    Cancelled,
    /// Task exceeded its timeout.
    Timeout,
    /// Task was rolled back (chain failure).
    RolledBack,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn nil_uuid() -> Uuid {
        Uuid::nil()
    }

    // ── Task discriminator ─────────────────────────────────────────────

    #[test]
    fn exec_task_serialization_has_type_field() {
        let task = Task::Exec(ExecTask {
            id: nil_uuid(),
            priority: Priority::Normal,
            tags: HashMap::new(),
            kind: ExecKind::Oneshot,
            executor: Executor::Shell,
            target: None,
            method: None,
            payload: json!({"script": "echo hello"}),
            interval_ms: None,
            timeout_ms: 30_000,
            credentials: vec![],
        });
        let json_str = serde_json::to_string(&task).unwrap();
        assert!(json_str.contains(r#""type":"exec""#));
    }

    #[test]
    fn exec_task_roundtrip() {
        let task = Task::Exec(ExecTask {
            id: nil_uuid(),
            priority: Priority::High,
            tags: [("env".into(), "prod".into())].into(),
            kind: ExecKind::Oneshot,
            executor: Executor::Shell,
            target: None,
            method: None,
            payload: json!({"script": "uptime"}),
            interval_ms: None,
            timeout_ms: 5_000,
            credentials: vec![],
        });
        let json_str = serde_json::to_string(&task).unwrap();
        let parsed: Task = serde_json::from_str(&json_str).unwrap();
        assert_eq!(task, parsed);
    }

    #[test]
    fn continuous_exec_task_roundtrip() {
        let task = Task::Exec(ExecTask {
            id: nil_uuid(),
            priority: Priority::Normal,
            tags: HashMap::new(),
            kind: ExecKind::Continuous,
            executor: Executor::Sidecar,
            target: Some("mysql-ctrl".into()),
            method: Some("submit".into()),
            payload: json!({"action": "discover"}),
            interval_ms: Some(30_000),
            timeout_ms: 5_000,
            credentials: vec![],
        });
        let json_str = serde_json::to_string(&task).unwrap();
        let parsed: Task = serde_json::from_str(&json_str).unwrap();
        assert_eq!(task, parsed);
    }

    #[test]
    fn cancel_task_roundtrip() {
        let task = Task::Cancel(CancelTask {
            id: nil_uuid(),
            priority: Priority::Critical,
            tags: HashMap::new(),
            target_task_id: Uuid::from_u128(1),
        });
        let json_str = serde_json::to_string(&task).unwrap();
        assert!(json_str.contains(r#""type":"cancel""#));
        let parsed: Task = serde_json::from_str(&json_str).unwrap();
        assert_eq!(task, parsed);
    }

    #[test]
    fn install_task_roundtrip() {
        let task = Task::Install(InstallTask {
            id: nil_uuid(),
            priority: Priority::Normal,
            tags: HashMap::new(),
            unit: crate::unit::ManagedUnit {
                name: "echo-ctrl".into(),
                kind: crate::unit::UnitKind::Sidecar,
                version: "0.1.0".into(),
                artifact: Artifact {
                    url: "https://example.com/echo-ctrl.tar.gz".into(),
                    sha256: "abc123".into(),
                },
                state: Default::default(),
                health_check: None,
                config: None,
                socket_path: Some("/run/vasal/echo-ctrl.sock".into()),
            },
        });
        let json_str = serde_json::to_string(&task).unwrap();
        let parsed: Task = serde_json::from_str(&json_str).unwrap();
        assert_eq!(task, parsed);
    }

    #[test]
    fn self_upgrade_task_roundtrip() {
        let task = Task::SelfUpgrade(SelfUpgradeTask {
            id: nil_uuid(),
            priority: Priority::Critical,
            tags: HashMap::new(),
            target_version: "0.2.0".into(),
            artifact: Artifact {
                url: "https://example.com/vasal-0.2.0".into(),
                sha256: "deadbeef".into(),
            },
            rollback: Some(RollbackSpec {
                version: "0.1.0".into(),
                artifact: Artifact {
                    url: "https://example.com/vasal-0.1.0".into(),
                    sha256: "cafebabe".into(),
                },
            }),
        });
        let json_str = serde_json::to_string(&task).unwrap();
        let parsed: Task = serde_json::from_str(&json_str).unwrap();
        assert_eq!(task, parsed);
    }

    // ── Task accessor methods ──────────────────────────────────────────

    #[test]
    fn task_accessors() {
        let id = Uuid::from_u128(42);
        let task = Task::Remove(RemoveTask {
            id,
            priority: Priority::Low,
            tags: [("key".into(), "val".into())].into(),
            unit_name: "old-sidecar".into(),
            purge: true,
        });
        assert_eq!(task.id(), id);
        assert_eq!(task.priority(), Priority::Low);
        assert_eq!(task.tags().get("key").unwrap(), "val");
    }

    // ── TaskChain ──────────────────────────────────────────────────────

    #[test]
    fn task_chain_roundtrip() {
        let chain = TaskChain {
            id: nil_uuid(),
            steps: vec![
                ChainStep {
                    task: ExecTask {
                        id: Uuid::from_u128(1),
                        priority: Priority::Normal,
                        tags: HashMap::new(),
                        kind: ExecKind::Oneshot,
                        executor: Executor::Shell,
                        target: None,
                        method: None,
                        payload: json!({"script": "step1"}),
                        interval_ms: None,
                        timeout_ms: 5_000,
                        credentials: vec![],
                    },
                    rollback: Some(ExecTask {
                        id: Uuid::from_u128(10),
                        priority: Priority::Normal,
                        tags: HashMap::new(),
                        kind: ExecKind::Oneshot,
                        executor: Executor::Shell,
                        target: None,
                        method: None,
                        payload: json!({"script": "undo step1"}),
                        interval_ms: None,
                        timeout_ms: 5_000,
                        credentials: vec![],
                    }),
                },
                ChainStep {
                    task: ExecTask {
                        id: Uuid::from_u128(2),
                        priority: Priority::Normal,
                        tags: HashMap::new(),
                        kind: ExecKind::Oneshot,
                        executor: Executor::Shell,
                        target: None,
                        method: None,
                        payload: json!({"script": "step2"}),
                        interval_ms: None,
                        timeout_ms: 5_000,
                        credentials: vec![],
                    },
                    rollback: None,
                },
            ],
            on_failure: RollbackStrategy::RollbackAll,
            tags: HashMap::new(),
        };
        let json_str = serde_json::to_string_pretty(&chain).unwrap();
        let parsed: TaskChain = serde_json::from_str(&json_str).unwrap();
        assert_eq!(chain, parsed);
    }

    #[test]
    fn rollback_strategy_default_is_rollback_all() {
        assert_eq!(RollbackStrategy::default(), RollbackStrategy::RollbackAll);
    }

    // ── TaskResult ─────────────────────────────────────────────────────

    #[test]
    fn task_result_roundtrip() {
        let result = TaskResult {
            task_id: nil_uuid(),
            chain_id: None,
            step_index: None,
            status: TaskResultStatus::Success,
            exit_code: Some(0),
            stdout: "hello\n".into(),
            stderr: String::new(),
            duration_ms: 42,
            timestamp: 1_700_000_000_000,
            error: None,
        };
        let json_str = serde_json::to_string(&result).unwrap();
        let parsed: TaskResult = serde_json::from_str(&json_str).unwrap();
        assert_eq!(result, parsed);
    }

    #[test]
    fn task_result_chain_step() {
        let result = TaskResult {
            task_id: Uuid::from_u128(1),
            chain_id: Some(Uuid::from_u128(100)),
            step_index: Some(2),
            status: TaskResultStatus::RolledBack,
            exit_code: None,
            stdout: String::new(),
            stderr: "step failed".into(),
            duration_ms: 150,
            timestamp: 1_700_000_000_000,
            error: Some("connection refused".into()),
        };
        let json_str = serde_json::to_string(&result).unwrap();
        assert!(json_str.contains(r#""chain_id""#));
        assert!(json_str.contains(r#""step_index":2"#));
    }

    // ── Priority default ───────────────────────────────────────────────

    #[test]
    fn priority_default_is_normal() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    // ── Deserialization from raw JSON ──────────────────────────────────

    #[test]
    fn deserialize_exec_from_raw_json() {
        let raw = r#"{
            "type": "exec",
            "id": "00000000-0000-0000-0000-000000000001",
            "kind": "oneshot",
            "executor": "shell",
            "payload": { "script": "echo hello" },
            "timeout_ms": 30000
        }"#;
        let task: Task = serde_json::from_str(raw).unwrap();
        assert!(matches!(task, Task::Exec(ref t) if t.executor == Executor::Shell));
        assert_eq!(task.priority(), Priority::Normal); // default
    }

    #[test]
    fn deserialize_remove_from_raw_json() {
        let raw = r#"{
            "type": "remove",
            "id": "00000000-0000-0000-0000-000000000002",
            "priority": "high",
            "unit_name": "old-ctrl",
            "purge": true
        }"#;
        let task: Task = serde_json::from_str(raw).unwrap();
        assert!(matches!(task, Task::Remove(ref t) if t.purge));
    }
}
