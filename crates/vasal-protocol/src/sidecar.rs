//! Sidecar IPC protocol types.

use serde::{Deserialize, Serialize};

/// Response from a `submit` call.
///
/// The sidecar may complete synchronously or return `Accepted` for async polling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SubmitResponse {
    Completed {
        #[serde(default)]
        stdout: String,
        #[serde(default)]
        stderr: String,
        #[serde(default)]
        truncated: bool,
    },
    Failed {
        error: String,
        #[serde(default)]
        stderr: String,
    },
    Accepted {
        task_id: String,
    },
}

impl SubmitResponse {
    /// Returns `true` if no further polling is required.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Accepted { .. })
    }
}

/// Parameters for a `status` poll.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatusParams {
    pub task_id: String,
}

/// Response from a `status` poll.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StatusResponse {
    Running,
    Completed {
        #[serde(default)]
        stdout: String,
        #[serde(default)]
        stderr: String,
        #[serde(default)]
        truncated: bool,
    },
    Failed {
        error: String,
        #[serde(default)]
        stderr: String,
    },
    Cancelled,
}

impl StatusResponse {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Parameters for a `cancel` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelParams {
    pub task_id: String,
}

/// Response from a `cancel` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelResponse {
    pub cancelled: bool,
}

/// Response from a `health` call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthResponse {
    pub status: HealthStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Sidecar health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ok,
    Degraded,
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
