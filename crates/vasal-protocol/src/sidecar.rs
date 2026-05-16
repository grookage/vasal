//! Sidecar IPC protocol types.
//!
//! These types define the response structures for the four sidecar protocol
//! methods: `submit`, `status`, `cancel`, and `health` (DD-14, DD-15).
//!
//! Request parameters for `status` and `cancel` are also defined here.
//! `submit` takes opaque JSON params (forwarded from the task payload) and
//! `health` takes no parameters.
//!
//! # Wire Format
//!
//! Messages are serialized as JSON inside a JSON-RPC 2.0 envelope
//! (see [`crate::jsonrpc`]) with 4-byte big-endian length-prefixed framing
//! over Unix domain sockets.

use serde::{Deserialize, Serialize};

// ── Submit ─────────────────────────────────────────────────────────────────

/// Response from a `submit` call.
///
/// The sidecar decides the execution mode:
///
/// - **Synchronous** (common case): return [`Completed`](SubmitResponse::Completed)
///   or [`Failed`](SubmitResponse::Failed) immediately. No state stored.
/// - **Asynchronous** (long-running): return [`Accepted`](SubmitResponse::Accepted).
///   The agent polls via [`StatusResponse`] until a terminal state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SubmitResponse {
    /// Work completed synchronously — result available immediately.
    Completed {
        /// Primary output.
        #[serde(default)]
        stdout: String,
        /// Diagnostic output.
        #[serde(default)]
        stderr: String,
        /// Whether output was truncated due to the 4 MB message limit.
        #[serde(default)]
        truncated: bool,
    },
    /// Work failed synchronously.
    Failed {
        /// Error description.
        error: String,
        /// Diagnostic output.
        #[serde(default)]
        stderr: String,
    },
    /// Work accepted for asynchronous execution.
    Accepted {
        /// Identifier for subsequent `status` and `cancel` calls.
        task_id: String,
    },
}

impl SubmitResponse {
    /// Returns `true` if this response represents a terminal state
    /// (no further polling required).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Accepted { .. })
    }
}

// ── Status ─────────────────────────────────────────────────────────────────

/// Parameters for a `status` poll (used after `submit` returns `Accepted`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatusParams {
    /// The `task_id` returned by a prior `submit` call.
    pub task_id: String,
}

/// Response from a `status` poll.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StatusResponse {
    /// Work is still in progress.
    Running,
    /// Work completed successfully.
    Completed {
        #[serde(default)]
        stdout: String,
        #[serde(default)]
        stderr: String,
        #[serde(default)]
        truncated: bool,
    },
    /// Work failed.
    Failed {
        error: String,
        #[serde(default)]
        stderr: String,
    },
    /// Work was cancelled.
    Cancelled,
}

impl StatusResponse {
    /// Returns `true` if this status is terminal (no further polling needed).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

// ── Cancel ─────────────────────────────────────────────────────────────────

/// Parameters for a `cancel` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelParams {
    /// The `task_id` to cancel.
    pub task_id: String,
}

/// Response from a `cancel` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelResponse {
    /// Whether the cancellation was acknowledged by the sidecar.
    pub cancelled: bool,
}

// ── Health ──────────────────────────────────────────────────────────────────

/// Response from a `health` call.
///
/// Every sidecar must respond to `health`. This is used by the agent's unit
/// manager for periodic liveness checks, independent of task execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthResponse {
    /// Current health status.
    pub status: HealthStatus,
    /// Sidecar version string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Error detail when status is not [`HealthStatus::Ok`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Arbitrary metadata the sidecar wants to surface (e.g., attached probes,
    /// connection count, uptime).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Sidecar health status, reported via `health()` and surfaced in heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Fully operational.
    Ok,
    /// Operational but with issues (e.g., disk space low).
    Degraded,
    /// Not operational.
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_completed_roundtrip() {
        let resp = SubmitResponse::Completed {
            stdout: "hello world".into(),
            stderr: String::new(),
            truncated: false,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""status":"completed""#));
        let parsed: SubmitResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
        assert!(resp.is_terminal());
    }

    #[test]
    fn submit_accepted_roundtrip() {
        let resp = SubmitResponse::Accepted {
            task_id: "task-001".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""status":"accepted""#));
        let parsed: SubmitResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
        assert!(!resp.is_terminal());
    }

    #[test]
    fn submit_failed_roundtrip() {
        let resp = SubmitResponse::Failed {
            error: "connection refused".into(),
            stderr: "dial tcp: timeout".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: SubmitResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
        assert!(resp.is_terminal());
    }

    #[test]
    fn status_terminal_states() {
        assert!(!StatusResponse::Running.is_terminal());
        assert!(StatusResponse::Cancelled.is_terminal());
        assert!((StatusResponse::Completed {
            stdout: String::new(),
            stderr: String::new(),
            truncated: false,
        })
        .is_terminal());
        assert!((StatusResponse::Failed {
            error: "err".into(),
            stderr: String::new(),
        })
        .is_terminal());
    }

    #[test]
    fn health_response_minimal() {
        let resp = HealthResponse {
            status: HealthStatus::Ok,
            version: None,
            error: None,
            metadata: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        // Optional fields should be absent, not null.
        assert!(!json.contains("version"));
        assert!(!json.contains("error"));
        assert!(!json.contains("metadata"));
    }

    #[test]
    fn health_status_values() {
        assert_eq!(serde_json::to_string(&HealthStatus::Ok).unwrap(), r#""ok""#);
        assert_eq!(
            serde_json::to_string(&HealthStatus::Degraded).unwrap(),
            r#""degraded""#
        );
        assert_eq!(
            serde_json::to_string(&HealthStatus::Unhealthy).unwrap(),
            r#""unhealthy""#
        );
    }

    #[test]
    fn cancel_response_roundtrip() {
        let resp = CancelResponse { cancelled: true };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: CancelResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(resp, parsed);
    }
}
