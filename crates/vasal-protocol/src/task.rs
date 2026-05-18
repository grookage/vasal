//! Task dispatch types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::credential::CredentialRef;
use crate::unit::{Artifact, ManagedUnit};

/// Task execution priority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Critical,
    High,
    #[default]
    Normal,
    Low,
}

/// Which executor handles an exec task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Executor {
    Shell,
    Sidecar,
}

/// Execution lifecycle model for exec tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecKind {
    Oneshot,
    Continuous,
}

/// A task dispatched by the control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Task {
    Exec(ExecTask),
    Cancel(CancelTask),
    Install(InstallTask),
    Upgrade(UpgradeTask),
    Remove(RemoveTask),
    SelfUpgrade(SelfUpgradeTask),
}

macro_rules! task_accessor {
    ($name:ident -> $ret:ty) => {
        pub fn $name(&self) -> $ret {
            match self {
                Self::Exec(t) => t.$name,
                Self::Cancel(t) => t.$name,
                Self::Install(t) => t.$name,
                Self::Upgrade(t) => t.$name,
                Self::Remove(t) => t.$name,
                Self::SelfUpgrade(t) => t.$name,
            }
        }
    };
}

impl Task {
    task_accessor!(id -> Uuid);
    task_accessor!(priority -> Priority);

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

/// Execute a command — one-shot or continuous.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub kind: ExecKind,
    pub executor: Executor,
    /// Required when `executor` is `Sidecar`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Required when `executor` is `Sidecar`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Script text for shell, arbitrary JSON for sidecars.
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Required when `kind` is `Continuous`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
    pub timeout_ms: u64,
    #[serde(default)]
    pub credentials: Vec<CredentialRef>,
}

/// Cancel a currently running task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub target_task_id: Uuid,
}

/// Install a managed unit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InstallTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub unit: ManagedUnit,
}

/// Upgrade a managed unit to a new version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpgradeTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub unit_name: String,
    pub target_version: String,
    pub artifact: Artifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSpec>,
}

/// Rollback specification for a failed upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RollbackSpec {
    pub version: String,
    pub artifact: Artifact,
}

/// Remove a managed unit from the host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoveTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub unit_name: String,
    #[serde(default)]
    pub purge: bool,
}

/// Upgrade the agent binary itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelfUpgradeTask {
    pub id: Uuid,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub target_version: String,
    pub artifact: Artifact,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackSpec>,
}

/// A sequential chain of exec tasks with rollback support.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskChain {
    pub id: Uuid,
    pub steps: Vec<ChainStep>,
    #[serde(default)]
    pub on_failure: RollbackStrategy,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// A single step in a [`TaskChain`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChainStep {
    pub task: ExecTask,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<ExecTask>,
}

/// Strategy for handling chain step failures.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackStrategy {
    RollbackFailed,
    #[default]
    RollbackAll,
}

/// Result of a task execution, reported from the agent to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskResult {
    pub task_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<u32>,
    pub status: TaskResultStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    pub duration_ms: u64,
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Outcome of a task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultStatus {
    Success,
    Failed,
    Cancelled,
    Timeout,
    RolledBack,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn nil_uuid() -> Uuid {
        Uuid::nil()
    }

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

    #[test]
    fn priority_default_is_normal() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

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
