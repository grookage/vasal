//! The [`SidecarHandler`] trait that sidecar authors implement.

use async_trait::async_trait;
use vasal_protocol::sidecar::{CancelResponse, HealthResponse, StatusResponse, SubmitResponse};
use vasal_protocol::ProtocolError;

/// Handler trait for a Vasal sidecar.
///
/// Synchronous sidecars implement `health` and `submit` only — the default
/// `status`/`cancel` implementations reject with "method not found".
/// Asynchronous sidecars additionally override `status` and `cancel` to
/// support polling and cancellation of long-running tasks.
#[async_trait]
pub trait SidecarHandler: Send + Sync + 'static {
    /// Human-readable name used in log output.
    fn name(&self) -> &str;

    /// Handle a `health` check.
    async fn health(&self) -> HealthResponse;

    /// Handle a `submit` request.
    ///
    /// `params` is the opaque JSON payload forwarded from the task.
    async fn submit(&self, params: serde_json::Value) -> Result<SubmitResponse, ProtocolError>;

    /// Poll the status of an asynchronous task. Default returns "method not found".
    async fn status(&self, _task_id: &str) -> Result<StatusResponse, ProtocolError> {
        Err(ProtocolError::method_not_found("status"))
    }

    /// Cancel an asynchronous task. Default returns "method not found".
    async fn cancel(&self, _task_id: &str) -> Result<CancelResponse, ProtocolError> {
        Err(ProtocolError::method_not_found("cancel"))
    }
}
