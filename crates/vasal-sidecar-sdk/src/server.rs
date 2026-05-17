//! Unix domain socket server for sidecar IPC.
//!
//! [`SidecarServer`] binds to a Unix socket, accepts connections from the
//! Vasal agent, and dispatches JSON-RPC 2.0 requests to a [`SidecarHandler`].
//!
//! # Connection Model
//!
//! The agent connects per-request — each connection carries exactly one
//! request/response pair. Unix socket connect is ~50 us, so there is no
//! need for persistent connections or multiplexing.
//!
//! # Lifecycle
//!
//! 1. Remove any stale socket file from a prior run.
//! 2. Bind and listen.
//! 3. For each connection: read one frame, parse JSON-RPC, dispatch, respond.
//! 4. On shutdown signal: stop accepting, finish in-flight requests, remove
//!    the socket file.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use vasal_protocol::jsonrpc::{self, ErrorObject, Request, RequestId, Response};
use vasal_protocol::ProtocolError;

use crate::codec::LengthPrefixCodec;
use crate::handler::SidecarHandler;

/// A Unix domain socket server that dispatches JSON-RPC 2.0 requests to a
/// [`SidecarHandler`] implementation.
///
/// # Example
///
/// ```rust,no_run
/// # use vasal_sidecar_sdk::{SidecarServer, SidecarHandler};
/// # async fn example(handler: impl SidecarHandler) {
/// let server = SidecarServer::new(handler, "/run/vasal/my-sidecar.sock");
/// let shutdown = async { tokio::signal::ctrl_c().await.ok(); };
/// server.run(shutdown).await.unwrap();
/// # }
/// ```
pub struct SidecarServer<H> {
    handler: Arc<H>,
    socket_path: PathBuf,
}

impl<H: SidecarHandler> SidecarServer<H> {
    /// Create a new server bound to the given socket path.
    ///
    /// The handler is wrapped in an `Arc` internally and shared across
    /// all connection tasks.
    pub fn new(handler: H, socket_path: impl Into<PathBuf>) -> Self {
        Self {
            handler: Arc::new(handler),
            socket_path: socket_path.into(),
        }
    }

    /// Run the server until the `shutdown` future resolves.
    ///
    /// - Removes any stale socket file before binding.
    /// - Cleans up the socket file on shutdown.
    /// - In-flight connections are allowed to finish (they hold an `Arc` to
    ///   the handler).
    pub async fn run(
        &self,
        shutdown: impl std::future::Future<Output = ()> + Send,
    ) -> std::io::Result<()> {
        cleanup_socket(&self.socket_path);

        let listener = UnixListener::bind(&self.socket_path)?;
        info!(
            sidecar = self.handler.name(),
            path = %self.socket_path.display(),
            "listening",
        );

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;

                () = &mut shutdown => {
                    info!(sidecar = self.handler.name(), "shutting down");
                    break;
                }

                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _addr)) => {
                            let handler = Arc::clone(&self.handler);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(handler, stream).await {
                                    warn!(error = %e, "connection handler error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept connection");
                        }
                    }
                }
            }
        }

        cleanup_socket(&self.socket_path);
        Ok(())
    }
}

// ── Connection handler ─────────────────────────────────────────────────────

/// Handle a single agent connection: read one request, dispatch, write one
/// response, close.
async fn handle_connection<H: SidecarHandler>(
    handler: Arc<H>,
    stream: UnixStream,
) -> std::io::Result<()> {
    let mut framed = Framed::new(stream, LengthPrefixCodec::new());

    // Read exactly one frame.
    let frame = match framed.next().await {
        Some(Ok(f)) => f,
        Some(Err(e)) => return Err(e),
        None => {
            debug!("client disconnected before sending a request");
            return Ok(());
        }
    };

    // Parse JSON-RPC envelope.
    let request: Request = match serde_json::from_slice(&frame) {
        Ok(r) => r,
        Err(e) => {
            let resp = Response::error(
                RequestId::Integer(0),
                ProtocolError::parse_error(e.to_string()).into(),
            );
            return send_response(&mut framed, &resp).await;
        }
    };

    // Validate protocol version.
    if request.jsonrpc != jsonrpc::JSONRPC_VERSION {
        let resp = Response::error(
            request.id.clone(),
            ProtocolError::invalid_request(format!(
                "expected jsonrpc \"{}\", got {:?}",
                jsonrpc::JSONRPC_VERSION,
                request.jsonrpc,
            ))
            .into(),
        );
        return send_response(&mut framed, &resp).await;
    }

    debug!(
        sidecar = handler.name(),
        method = %request.method,
        id = %request.id,
        "dispatching request",
    );

    let response = dispatch(&*handler, &request).await;
    send_response(&mut framed, &response).await
}

// ── Dispatch ───────────────────────────────────────────────────────────────

/// Route a parsed JSON-RPC request to the appropriate [`SidecarHandler`] method.
async fn dispatch<H: SidecarHandler>(handler: &H, request: &Request) -> Response {
    let id = request.id.clone();
    let params = request.params.clone().unwrap_or(serde_json::Value::Null);

    match request.method.as_str() {
        // ── health ─────────────────────────────────────────────────────
        "health" => {
            let result = handler.health().await;
            to_success_or_internal_error(id, &result)
        }

        // ── submit ─────────────────────────────────────────────────────
        "submit" => match handler.submit(params).await {
            Ok(result) => to_success_or_internal_error(id, &result),
            Err(e) => Response::error(id, e.into()),
        },

        // ── status ─────────────────────────────────────────────────────
        "status" => {
            let task_id = match extract_task_id(&params) {
                Ok(tid) => tid,
                Err(resp) => return resp.with_id(id),
            };
            match handler.status(&task_id).await {
                Ok(result) => to_success_or_internal_error(id, &result),
                Err(e) => Response::error(id, e.into()),
            }
        }

        // ── cancel ─────────────────────────────────────────────────────
        "cancel" => {
            let task_id = match extract_task_id(&params) {
                Ok(tid) => tid,
                Err(resp) => return resp.with_id(id),
            };
            match handler.cancel(&task_id).await {
                Ok(result) => to_success_or_internal_error(id, &result),
                Err(e) => Response::error(id, e.into()),
            }
        }

        // ── unknown method ─────────────────────────────────────────────
        other => Response::error(id, ProtocolError::method_not_found(other).into()),
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Serialize a result value into a success response, falling back to an
/// internal error if serialization fails.
fn to_success_or_internal_error<T: serde::Serialize>(id: RequestId, value: &T) -> Response {
    match serde_json::to_value(value) {
        Ok(v) => Response::success(id, v),
        Err(e) => Response::error(
            id,
            ProtocolError::internal_error(format!("response serialization failed: {e}")).into(),
        ),
    }
}

/// Extract the `task_id` string from a `status` or `cancel` params object.
fn extract_task_id(params: &serde_json::Value) -> Result<String, ErrorResponse> {
    // Accept either StatusParams or CancelParams — both have `task_id`.
    #[derive(serde::Deserialize)]
    struct TaskIdParam {
        task_id: String,
    }
    match serde_json::from_value::<TaskIdParam>(params.clone()) {
        Ok(p) => Ok(p.task_id),
        Err(e) => Err(ErrorResponse(
            ProtocolError::invalid_params(e.to_string()).into(),
        )),
    }
}

/// A deferred error response that needs a [`RequestId`] attached.
struct ErrorResponse(ErrorObject);

impl ErrorResponse {
    fn with_id(self, id: RequestId) -> Response {
        Response::error(id, self.0)
    }
}

/// Serialize and send a JSON-RPC response over the framed connection.
async fn send_response(
    framed: &mut Framed<UnixStream, LengthPrefixCodec>,
    response: &Response,
) -> std::io::Result<()> {
    let payload = serde_json::to_vec(response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    framed.send(Bytes::from(payload)).await
}

/// Remove a socket file if it exists.
fn cleanup_socket(path: &Path) {
    if path.exists() {
        if let Err(e) = std::fs::remove_file(path) {
            warn!(path = %path.display(), error = %e, "failed to remove stale socket");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SidecarHandler;
    use async_trait::async_trait;
    use vasal_protocol::sidecar::{HealthResponse, HealthStatus, SubmitResponse};

    /// Minimal test handler that echoes submit params.
    struct TestHandler;

    #[async_trait]
    impl SidecarHandler for TestHandler {
        fn name(&self) -> &str {
            "test-handler"
        }

        async fn health(&self) -> HealthResponse {
            HealthResponse {
                status: HealthStatus::Ok,
                version: Some("0.0.1".into()),
                error: None,
                metadata: None,
            }
        }

        async fn submit(&self, params: serde_json::Value) -> Result<SubmitResponse, ProtocolError> {
            Ok(SubmitResponse::Completed {
                stdout: serde_json::to_string_pretty(&params)
                    .unwrap_or_else(|_| params.to_string()),
                stderr: String::new(),
                truncated: false,
            })
        }
    }

    /// Spin up a server on a temp socket, send one request, return the response.
    async fn roundtrip(request: &Request) -> Response {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");

        let server = SidecarServer::new(TestHandler, &sock);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_handle = tokio::spawn({
            async move {
                let _ = server
                    .run(async {
                        shutdown_rx.await.ok();
                    })
                    .await;
            }
        });

        // Give the server a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect and send request.
        let stream = UnixStream::connect(&sock).await.unwrap();
        let mut framed = Framed::new(stream, LengthPrefixCodec::new());

        let payload = serde_json::to_vec(request).unwrap();
        framed.send(Bytes::from(payload)).await.unwrap();

        let resp_frame = framed.next().await.unwrap().unwrap();
        let response: Response = serde_json::from_slice(&resp_frame).unwrap();

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;

        response
    }

    #[tokio::test]
    async fn health_request() {
        let req = Request::new("health", None, 1i64);
        let resp = roundtrip(&req).await;
        assert!(resp.is_success());

        let result: HealthResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(result.status, HealthStatus::Ok);
        assert_eq!(result.version.as_deref(), Some("0.0.1"));
    }

    #[tokio::test]
    async fn submit_echoes_params() {
        let params = serde_json::json!({"greeting": "hello"});
        let req = Request::new("submit", Some(params.clone()), 2i64);
        let resp = roundtrip(&req).await;
        assert!(resp.is_success());

        let result: SubmitResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
        match result {
            SubmitResponse::Completed { stdout, .. } => {
                let echoed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
                assert_eq!(echoed, params);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let req = Request::new("frobnicate", None, 3i64);
        let resp = roundtrip(&req).await;
        assert!(!resp.is_success());
        let err = resp.error.unwrap();
        assert_eq!(err.code, vasal_protocol::error::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn status_on_sync_sidecar_returns_method_not_found() {
        let req = Request::new("status", Some(serde_json::json!({"task_id": "abc"})), 4i64);
        let resp = roundtrip(&req).await;
        assert!(!resp.is_success());
        assert_eq!(
            resp.error.unwrap().code,
            vasal_protocol::error::METHOD_NOT_FOUND,
        );
    }

    #[tokio::test]
    async fn malformed_json_returns_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("test.sock");

        let server = SidecarServer::new(TestHandler, &sock);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let _ = server
                .run(async {
                    shutdown_rx.await.ok();
                })
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let stream = UnixStream::connect(&sock).await.unwrap();
        let mut framed = Framed::new(stream, LengthPrefixCodec::new());

        // Send garbage bytes.
        framed
            .send(Bytes::from_static(b"this is not json"))
            .await
            .unwrap();

        let resp_frame = framed.next().await.unwrap().unwrap();
        let resp: Response = serde_json::from_slice(&resp_frame).unwrap();

        assert!(!resp.is_success());
        assert_eq!(resp.error.unwrap().code, vasal_protocol::error::PARSE_ERROR,);

        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn invalid_jsonrpc_version() {
        let req = Request {
            jsonrpc: "1.0".into(),
            method: "health".into(),
            params: None,
            id: RequestId::Integer(5),
        };
        let resp = roundtrip(&req).await;
        assert!(!resp.is_success());
        assert_eq!(
            resp.error.unwrap().code,
            vasal_protocol::error::INVALID_REQUEST,
        );
    }
}
