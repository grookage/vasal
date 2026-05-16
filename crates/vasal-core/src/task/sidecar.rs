//! Sidecar dispatcher — IPC client for sidecar JSON-RPC calls.
//!
//! Connects to a sidecar's Unix domain socket, sends a length-prefixed
//! JSON-RPC 2.0 request, reads the response, and handles sync/async modes
//! with poll backoff (DD-14, DD-15).

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;
use vasal_protocol::credential::CredentialRef;
use vasal_protocol::jsonrpc::{Request, Response};
use vasal_protocol::sidecar::{StatusParams, SubmitResponse};
use vasal_protocol::task::{TaskResult, TaskResultStatus};
use vasal_sidecar_sdk::codec::LengthPrefixCodec;

use super::router::make_result;
use crate::credential::{self, ResolvedCredentials};

/// Poll backoff schedule per DD-15.
const POLL_DELAYS_MS: &[u64] = &[0, 100, 200, 500, 1000];

/// Execute a sidecar task.
///
/// 1. Connect to sidecar socket.
/// 2. Build JSON-RPC `submit` request with payload + resolved credentials.
/// 3. Send request, read response.
/// 4. If synchronous (Completed/Failed) → return immediately.
/// 5. If asynchronous (Accepted) → poll `status` with backoff until terminal.
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    task_id: Uuid,
    target: &str,
    method: &str,
    payload: &serde_json::Value,
    cred_refs: &[CredentialRef],
    resolved_creds: &ResolvedCredentials,
    timeout_ms: u64,
    socket_dir: &Path,
    cancel: CancellationToken,
) -> TaskResult {
    let start = Instant::now();
    let socket_path = socket_dir.join(format!("{target}.sock"));

    // Build params: merge payload with resolved credentials and lazy cred refs.
    let mut params = payload.clone();
    if let Some(obj) = params.as_object_mut() {
        // Inject resolved (eager) credentials.
        if !resolved_creds.is_empty() {
            obj.insert(
                "_credentials".into(),
                serde_json::to_value(resolved_creds).unwrap_or_default(),
            );
        }
        // Attach lazy credential refs for sidecar self-resolution.
        let lazy = credential::lazy_credentials_as_json(cred_refs);
        if !lazy.is_null() {
            obj.insert("_lazy_credentials".into(), lazy);
        }
    }

    // Submit.
    let submit_resp = match call_raw(&socket_path, method, Some(params)).await {
        Ok(resp) => resp,
        Err(e) => {
            return make_result(
                task_id,
                TaskResultStatus::Failed,
                None,
                String::new(),
                e.to_string(),
                start.elapsed(),
                Some(format!("sidecar {target}: {e}")),
            );
        }
    };

    // Parse submit response.
    if let Some(err) = submit_resp.error {
        return make_result(
            task_id,
            TaskResultStatus::Failed,
            None,
            String::new(),
            format!("[{}] {}", err.code, err.message),
            start.elapsed(),
            Some(err.message),
        );
    }

    let result_value = submit_resp.result.unwrap_or(serde_json::Value::Null);
    let submit: SubmitResponse = match serde_json::from_value(result_value.clone()) {
        Ok(s) => s,
        Err(e) => {
            return make_result(
                task_id,
                TaskResultStatus::Failed,
                None,
                result_value.to_string(),
                e.to_string(),
                start.elapsed(),
                Some(format!("failed to parse sidecar response: {e}")),
            );
        }
    };

    match submit {
        SubmitResponse::Completed { stdout, stderr, .. } => {
            make_result(task_id, TaskResultStatus::Success, None, stdout, stderr, start.elapsed(), None)
        }
        SubmitResponse::Failed { error, stderr } => {
            make_result(
                task_id, TaskResultStatus::Failed, None,
                String::new(), stderr, start.elapsed(), Some(error),
            )
        }
        SubmitResponse::Accepted { task_id: sidecar_task_id } => {
            // Async mode — poll with backoff.
            poll_until_complete(
                task_id,
                &sidecar_task_id,
                &socket_path,
                timeout_ms,
                start,
                cancel,
            )
            .await
        }
    }
}

/// Poll a sidecar's `status` endpoint until a terminal state or timeout.
async fn poll_until_complete(
    agent_task_id: Uuid,
    sidecar_task_id: &str,
    socket_path: &Path,
    timeout_ms: u64,
    start: Instant,
    cancel: CancellationToken,
) -> TaskResult {
    let timeout = Duration::from_millis(timeout_ms);
    let mut poll_index: usize = 0;

    loop {
        // Check timeout.
        if start.elapsed() >= timeout {
            // Best-effort cancel on the sidecar.
            let _ = call_raw(
                socket_path,
                "cancel",
                Some(serde_json::json!({"task_id": sidecar_task_id})),
            )
            .await;

            return make_result(
                agent_task_id,
                TaskResultStatus::Timeout,
                None,
                String::new(),
                String::new(),
                start.elapsed(),
                Some(format!("timeout after {timeout_ms}ms")),
            );
        }

        // Backoff delay.
        let delay_ms = POLL_DELAYS_MS.get(poll_index).copied()
            .unwrap_or(*POLL_DELAYS_MS.last().unwrap());
        if delay_ms > 0 {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    let _ = call_raw(
                        socket_path,
                        "cancel",
                        Some(serde_json::json!({"task_id": sidecar_task_id})),
                    ).await;
                    return make_result(
                        agent_task_id,
                        TaskResultStatus::Cancelled,
                        None, String::new(), String::new(),
                        start.elapsed(), None,
                    );
                }
                () = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
            }
        }

        // Poll status.
        let status_params = serde_json::to_value(StatusParams {
            task_id: sidecar_task_id.to_owned(),
        })
        .unwrap();

        match call_raw(socket_path, "status", Some(status_params)).await {
            Ok(resp) => {
                if let Some(err) = resp.error {
                    return make_result(
                        agent_task_id,
                        TaskResultStatus::Failed,
                        None,
                        String::new(),
                        format!("[{}] {}", err.code, err.message),
                        start.elapsed(),
                        Some(err.message),
                    );
                }

                let value = resp.result.unwrap_or_default();
                if let Ok(status) = serde_json::from_value::<vasal_protocol::sidecar::StatusResponse>(value) {
                    match status {
                        vasal_protocol::sidecar::StatusResponse::Running => {
                            debug!(task_id = %sidecar_task_id, "sidecar task still running");
                        }
                        vasal_protocol::sidecar::StatusResponse::Completed { stdout, stderr, .. } => {
                            return make_result(
                                agent_task_id, TaskResultStatus::Success, None,
                                stdout, stderr, start.elapsed(), None,
                            );
                        }
                        vasal_protocol::sidecar::StatusResponse::Failed { error, stderr } => {
                            return make_result(
                                agent_task_id, TaskResultStatus::Failed, None,
                                String::new(), stderr, start.elapsed(), Some(error),
                            );
                        }
                        vasal_protocol::sidecar::StatusResponse::Cancelled => {
                            return make_result(
                                agent_task_id, TaskResultStatus::Cancelled, None,
                                String::new(), String::new(), start.elapsed(), None,
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "status poll failed — will retry");
            }
        }

        poll_index += 1;
    }
}

/// Low-level: send a single JSON-RPC request to a sidecar socket and read the response.
///
/// This is a per-request connection (connect, send, recv, close). Unix socket
/// connect is ~50us so no persistent connections are needed.
pub async fn call_raw(
    socket_path: &Path,
    method: &str,
    params: Option<serde_json::Value>,
) -> crate::Result<Response> {
    let stream = UnixStream::connect(socket_path).await.map_err(|e| {
        crate::Error::Transport(format!(
            "failed to connect to sidecar at {}: {e}",
            socket_path.display(),
        ))
    })?;

    let mut framed = Framed::new(stream, LengthPrefixCodec::new());

    // Build and send request.
    let req = Request::new(method, params, 1i64);
    let payload = serde_json::to_vec(&req)?;
    framed.send(Bytes::from(payload)).await.map_err(|e| {
        crate::Error::Transport(format!("failed to send to sidecar: {e}"))
    })?;

    // Read response.
    let frame = framed.next().await.ok_or_else(|| {
        crate::Error::Transport("sidecar closed connection without responding".into())
    })?.map_err(|e| {
        crate::Error::Transport(format!("failed to read sidecar response: {e}"))
    })?;

    let response: Response = serde_json::from_slice(&frame)?;
    Ok(response)
}
