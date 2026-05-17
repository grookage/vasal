//! The [`SidecarHandler`] trait — the primary extension point for sidecar authors.
//!
//! Implement this trait to define how your sidecar responds to agent requests.
//! Only [`health`](SidecarHandler::health) and [`submit`](SidecarHandler::submit)
//! are required; the defaults for [`status`](SidecarHandler::status) and
//! [`cancel`](SidecarHandler::cancel) return "method not found", which is
//! correct for synchronous-only sidecars.

use async_trait::async_trait;
use vasal_protocol::sidecar::{CancelResponse, HealthResponse, StatusResponse, SubmitResponse};
use vasal_protocol::ProtocolError;

/// Handler trait for a Vasal sidecar.
///
/// # Synchronous vs. Asynchronous Sidecars
///
/// **Synchronous** (most sidecars): implement only `health` and `submit`.
/// Return [`SubmitResponse::Completed`] or [`SubmitResponse::Failed`] from
/// `submit`. The default `status` and `cancel` implementations correctly
/// reject calls with "method not found".
///
/// **Asynchronous** (long-running operations): additionally override `status`
/// and `cancel`. Return [`SubmitResponse::Accepted`] from `submit` with a
/// `task_id`, then handle poll and cancellation requests.
#[async_trait]
pub trait SidecarHandler: Send + Sync + 'static {
    /// The sidecar's human-readable name, used in log output.
    fn name(&self) -> &str;

    /// Handle a `health` check.
    ///
    /// Called periodically by the agent's unit manager, independent of task
    /// execution. Implementations should return quickly (sub-millisecond
    /// is ideal — no network calls).
    async fn health(&self) -> HealthResponse;

    /// Handle a `submit` request.
    ///
    /// `params` is the opaque JSON payload forwarded from the task. The
    /// sidecar interprets its structure (e.g., `{"action": "query", "sql": "..."}`).
    ///
    /// Return:
    /// - [`SubmitResponse::Completed`] / [`SubmitResponse::Failed`] for
    ///   synchronous work.
    /// - [`SubmitResponse::Accepted`] for async work the agent will poll.
    async fn submit(&self, params: serde_json::Value) -> Result<SubmitResponse, ProtocolError>;

    /// Poll the status of an asynchronous task.
    ///
    /// Only called after `submit` returned [`SubmitResponse::Accepted`].
    /// The default implementation returns "method not found".
    async fn status(&self, _task_id: &str) -> Result<StatusResponse, ProtocolError> {
        Err(ProtocolError::method_not_found("status"))
    }

    /// Cancel an asynchronous task.
    ///
    /// Only called for tasks that returned [`SubmitResponse::Accepted`].
    /// The default implementation returns "method not found".
    async fn cancel(&self, _task_id: &str) -> Result<CancelResponse, ProtocolError> {
        Err(ProtocolError::method_not_found("cancel"))
    }
}
