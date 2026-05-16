//! Protocol error codes and the [`ProtocolError`] type.
//!
//! Error codes follow JSON-RPC 2.0 conventions:
//!
//! - **Standard codes** (`-327xx`): defined by the JSON-RPC 2.0 spec.
//! - **Application codes** (`-320xx`): Vasal-specific semantics.
//!
//! [`ProtocolError`] converts losslessly to/from [`crate::jsonrpc::ErrorObject`]
//! for wire serialization.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::jsonrpc::ErrorObject;

// ── Standard JSON-RPC 2.0 error codes ─────────────────────────────────────

/// Malformed JSON was received by the server.
pub const PARSE_ERROR: i32 = -32700;

/// The JSON sent is not a valid request object.
pub const INVALID_REQUEST: i32 = -32600;

/// The method does not exist or is not available.
pub const METHOD_NOT_FOUND: i32 = -32601;

/// Invalid method parameter(s).
pub const INVALID_PARAMS: i32 = -32602;

/// Internal JSON-RPC error.
pub const INTERNAL_ERROR: i32 = -32603;

// ── Vasal application error codes ──────────────────────────────────────────

/// Task execution exceeded its configured timeout.
pub const EXECUTION_TIMEOUT: i32 = -32000;

/// The referenced `task_id` does not exist.
pub const TASK_NOT_FOUND: i32 = -32001;

/// A cancel was requested on an already-cancelled task.
pub const TASK_ALREADY_CANCELLED: i32 = -32002;

/// Credential resolution failed (missing, rejected, or expired).
pub const CREDENTIAL_ERROR: i32 = -32003;

/// The sidecar cannot reach the target host or service.
pub const TARGET_UNREACHABLE: i32 = -32004;

/// The sidecar is overloaded and cannot accept more work.
pub const CAPACITY_EXCEEDED: i32 = -32005;

// ── ProtocolError ──────────────────────────────────────────────────────────

/// A protocol-level error carrying a JSON-RPC 2.0 error code.
///
/// Implements [`std::error::Error`] for idiomatic Rust error handling and
/// converts to/from [`ErrorObject`] for wire serialization.
///
/// # Examples
///
/// ```
/// use vasal_protocol::ProtocolError;
///
/// let err = ProtocolError::method_not_found("frobnicate");
/// assert_eq!(err.code, -32601);
/// assert!(err.to_string().contains("frobnicate"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolError {
    /// JSON-RPC error code.
    pub code: i32,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured data providing additional context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl ProtocolError {
    /// Create an error with the given code and message.
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attach arbitrary structured data to this error.
    #[must_use]
    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    // ── Standard code constructors ─────────────────────────────────────

    /// Malformed JSON received.
    pub fn parse_error(detail: impl Into<String>) -> Self {
        Self::new(PARSE_ERROR, detail)
    }

    /// Request is structurally invalid.
    pub fn invalid_request(detail: impl Into<String>) -> Self {
        Self::new(INVALID_REQUEST, detail)
    }

    /// The requested method does not exist on this sidecar.
    pub fn method_not_found(method: &str) -> Self {
        Self::new(METHOD_NOT_FOUND, format!("method not found: {method}"))
    }

    /// Method parameters are invalid.
    pub fn invalid_params(detail: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, detail)
    }

    /// Unexpected internal error.
    pub fn internal_error(detail: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR, detail)
    }

    // ── Application code constructors ──────────────────────────────────

    /// Execution exceeded the configured timeout.
    pub fn execution_timeout() -> Self {
        Self::new(EXECUTION_TIMEOUT, "execution timeout")
    }

    /// The referenced task ID does not exist.
    pub fn task_not_found(task_id: &str) -> Self {
        Self::new(TASK_NOT_FOUND, format!("task not found: {task_id}"))
    }

    /// Cancel requested on an already-cancelled task.
    pub fn task_already_cancelled(task_id: &str) -> Self {
        Self::new(
            TASK_ALREADY_CANCELLED,
            format!("task already cancelled: {task_id}"),
        )
    }

    /// Credential resolution failed.
    pub fn credential_error(detail: impl Into<String>) -> Self {
        Self::new(CREDENTIAL_ERROR, detail)
    }

    /// Cannot reach the target host or service.
    pub fn target_unreachable(detail: impl Into<String>) -> Self {
        Self::new(TARGET_UNREACHABLE, detail)
    }

    /// Sidecar is overloaded.
    pub fn capacity_exceeded(detail: impl Into<String>) -> Self {
        Self::new(CAPACITY_EXCEEDED, detail)
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for ProtocolError {}

impl From<ProtocolError> for ErrorObject {
    fn from(e: ProtocolError) -> Self {
        ErrorObject {
            code: e.code,
            message: e.message,
            data: e.data,
        }
    }
}

impl From<ErrorObject> for ProtocolError {
    fn from(e: ErrorObject) -> Self {
        Self {
            code: e.code,
            message: e.message,
            data: e.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_format() {
        let err = ProtocolError::method_not_found("frobnicate");
        assert_eq!(err.to_string(), "[-32601] method not found: frobnicate");
    }

    #[test]
    fn with_data() {
        let err = ProtocolError::internal_error("boom")
            .with_data(serde_json::json!({"detail": "stack trace"}));
        assert!(err.data.is_some());
    }

    #[test]
    fn roundtrip_through_error_object() {
        let original = ProtocolError::credential_error("expired token");
        let obj: ErrorObject = original.clone().into();
        let recovered: ProtocolError = obj.into();
        assert_eq!(original.code, recovered.code);
        assert_eq!(original.message, recovered.message);
    }

    #[test]
    fn error_code_values() {
        assert_eq!(PARSE_ERROR, -32700);
        assert_eq!(INVALID_REQUEST, -32600);
        assert_eq!(METHOD_NOT_FOUND, -32601);
        assert_eq!(INVALID_PARAMS, -32602);
        assert_eq!(INTERNAL_ERROR, -32603);
        assert_eq!(EXECUTION_TIMEOUT, -32000);
        assert_eq!(TASK_NOT_FOUND, -32001);
        assert_eq!(TASK_ALREADY_CANCELLED, -32002);
        assert_eq!(CREDENTIAL_ERROR, -32003);
        assert_eq!(TARGET_UNREACHABLE, -32004);
        assert_eq!(CAPACITY_EXCEEDED, -32005);
    }
}
