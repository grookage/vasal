//! gRPC bidirectional stream transport for task dispatch.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};
use vasal_protocol::task::{Task, TaskChain, TaskResult};

use super::{ReceivedWork, Transport};

pub mod proto {
    tonic::include_proto!("vasal.v1");
}

use proto::agent_dispatch_client::AgentDispatchClient;
use proto::{agent_message, control_plane_message, AgentHello, AgentMessage, ControlPlaneMessage};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

struct GrpcInner {
    outbound_tx: Option<mpsc::Sender<AgentMessage>>,
    inbound_stream: Option<tonic::Streaming<ControlPlaneMessage>>,
    backoff: Duration,
}

/// gRPC bidirectional streaming transport with automatic reconnection.
pub struct GrpcTransport {
    endpoint: String,
    agent_id: String,
    agent_version: String,
    inner: tokio::sync::Mutex<GrpcInner>,
}

impl GrpcTransport {
    pub fn new(endpoint: String, agent_id: String, agent_version: String) -> Self {
        Self {
            endpoint,
            agent_id,
            agent_version,
            inner: tokio::sync::Mutex::new(GrpcInner {
                outbound_tx: None,
                inbound_stream: None,
                backoff: INITIAL_BACKOFF,
            }),
        }
    }

    async fn connect(
        endpoint: &str,
        agent_id: &str,
        agent_version: &str,
    ) -> crate::Result<(
        tonic::Streaming<ControlPlaneMessage>,
        mpsc::Sender<AgentMessage>,
    )> {
        info!(endpoint = %endpoint, "connecting to CP via gRPC");

        let mut client = AgentDispatchClient::connect(endpoint.to_owned())
            .await
            .map_err(|e| crate::Error::Transport(format!("gRPC connect failed: {e}")))?;

        let (tx, rx) = mpsc::channel::<AgentMessage>(64);

        let hello = AgentMessage {
            payload: Some(agent_message::Payload::Hello(AgentHello {
                agent_id: agent_id.to_owned(),
                agent_version: agent_version.to_owned(),
            })),
        };
        tx.send(hello)
            .await
            .map_err(|e| crate::Error::Transport(format!("failed to enqueue hello: {e}")))?;

        let response = client
            .task_stream(ReceiverStream::new(rx))
            .await
            .map_err(|e| crate::Error::Transport(format!("gRPC stream open failed: {e}")))?;

        let inbound_stream = response.into_inner();
        info!("gRPC stream established");

        Ok((inbound_stream, tx))
    }

    fn decode_work(msg: &ControlPlaneMessage) -> Option<ReceivedWork> {
        match &msg.payload {
            Some(control_plane_message::Payload::Task(bytes)) => {
                match serde_json::from_slice::<Task>(bytes) {
                    Ok(task) => Some(ReceivedWork::Single(task)),
                    Err(e) => {
                        warn!(error = %e, "failed to decode task from gRPC");
                        None
                    }
                }
            }
            Some(control_plane_message::Payload::TaskChain(bytes)) => {
                match serde_json::from_slice::<TaskChain>(bytes) {
                    Ok(chain) => Some(ReceivedWork::Chain(chain)),
                    Err(e) => {
                        warn!(error = %e, "failed to decode task chain from gRPC");
                        None
                    }
                }
            }
            Some(control_plane_message::Payload::Ack(ack)) => {
                if ack.accepted {
                    info!(message = %ack.message, "CP accepted agent hello");
                } else {
                    error!(message = %ack.message, "CP rejected agent hello");
                }
                None
            }
            None => None,
        }
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn recv_tasks(&self) -> crate::Result<Vec<ReceivedWork>> {
        let mut inner = self.inner.lock().await;

        if inner.inbound_stream.is_none() || inner.outbound_tx.is_none() {
            match Self::connect(&self.endpoint, &self.agent_id, &self.agent_version).await {
                Ok((stream, tx)) => {
                    inner.inbound_stream = Some(stream);
                    inner.outbound_tx = Some(tx);
                    inner.backoff = INITIAL_BACKOFF;
                }
                Err(e) => {
                    let backoff = inner.backoff;
                    warn!(
                        error = %e,
                        backoff_sec = backoff.as_secs(),
                        "gRPC connection failed — will retry",
                    );
                    inner.backoff = (backoff * 2).min(MAX_BACKOFF);
                    drop(inner);
                    tokio::time::sleep(backoff).await;
                    return Ok(vec![]);
                }
            }
        }

        let stream = inner.inbound_stream.as_mut().unwrap();
        match stream.message().await {
            Ok(Some(msg)) => {
                if let Some(work) = Self::decode_work(&msg) {
                    debug!("received work from gRPC stream");
                    Ok(vec![work])
                } else {
                    Ok(vec![])
                }
            }
            Ok(None) => {
                info!("gRPC stream ended — will reconnect");
                inner.inbound_stream = None;
                inner.outbound_tx = None;
                Ok(vec![])
            }
            Err(e) => {
                warn!(error = %e, "gRPC stream error — will reconnect");
                inner.inbound_stream = None;
                inner.outbound_tx = None;
                Err(crate::Error::Transport(format!("gRPC stream error: {e}")))
            }
        }
    }

    async fn send_result(&self, result: &TaskResult) -> crate::Result<()> {
        let inner = self.inner.lock().await;
        let tx = inner
            .outbound_tx
            .as_ref()
            .ok_or_else(|| crate::Error::Transport("gRPC not connected".into()))?
            .clone();
        drop(inner);

        let json_bytes = serde_json::to_vec(result)?;
        let msg = AgentMessage {
            payload: Some(agent_message::Payload::TaskResult(json_bytes)),
        };

        tx.send(msg)
            .await
            .map_err(|e| crate::Error::Transport(format!("failed to send result via gRPC: {e}")))?;

        debug!(task_id = %result.task_id, "result sent via gRPC");
        Ok(())
    }
}
