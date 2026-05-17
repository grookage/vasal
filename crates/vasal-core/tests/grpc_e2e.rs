//! End-to-end gRPC transport test with a control plane stub.
//!
//! Spins up a minimal gRPC server that implements the `AgentDispatch` service,
//! pushes a task to the agent's transport layer, and verifies the result comes
//! back through the stream.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

// The generated server code is available because build.rs sets build_server(true).
use vasal_core::transport::grpc::proto::{
    agent_dispatch_server::{AgentDispatch, AgentDispatchServer},
    agent_message, control_plane_message, AgentMessage, ControlPlaneMessage, ServerAck,
};

use vasal_protocol::task::*;

/// Minimal CP stub that sends one task and collects one result.
struct CpStub {
    /// Task to push to the agent.
    task_json: Vec<u8>,
    /// Collected results from the agent.
    results: Arc<Mutex<Vec<Vec<u8>>>>,
}

type BoxStream = Pin<Box<dyn Stream<Item = Result<ControlPlaneMessage, Status>> + Send>>;

#[tonic::async_trait]
impl AgentDispatch for CpStub {
    type TaskStreamStream = BoxStream;

    async fn task_stream(
        &self,
        request: Request<Streaming<AgentMessage>>,
    ) -> Result<Response<Self::TaskStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let task_bytes = self.task_json.clone();
        let results = Arc::clone(&self.results);

        // Spawn a reader for agent messages (hello, results).
        let (tx, rx) = mpsc::channel::<Result<ControlPlaneMessage, Status>>(16);

        tokio::spawn(async move {
            // Wait for the AgentHello.
            if let Some(msg) = inbound.message().await.ok().flatten().as_ref() {
                if let Some(agent_message::Payload::Hello(hello)) = &msg.payload {
                    // Send ack.
                    let ack = ControlPlaneMessage {
                        payload: Some(control_plane_message::Payload::Ack(ServerAck {
                            accepted: true,
                            message: format!("welcome agent {}", hello.agent_id),
                        })),
                    };
                    let _ = tx.send(Ok(ack)).await;

                    // Send the task.
                    let task_msg = ControlPlaneMessage {
                        payload: Some(control_plane_message::Payload::Task(task_bytes)),
                    };
                    let _ = tx.send(Ok(task_msg)).await;
                }
            }

            // Collect results.
            while let Ok(Some(msg)) = inbound.message().await {
                if let Some(agent_message::Payload::TaskResult(bytes)) = msg.payload {
                    results.lock().await.push(bytes);
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as BoxStream))
    }
}

#[tokio::test]
async fn grpc_roundtrip_task_and_result() {
    // Build a task.
    let task = Task::Exec(ExecTask {
        id: uuid::Uuid::new_v4(),
        priority: Priority::Normal,
        tags: std::collections::HashMap::new(),
        kind: ExecKind::Oneshot,
        executor: Executor::Shell,
        target: None,
        method: None,
        payload: serde_json::json!({"script": "echo grpc_e2e_test"}),
        interval_ms: None,
        timeout_ms: 5000,
        credentials: vec![],
    });
    let task_id = task.id();
    let task_json = serde_json::to_vec(&task).unwrap();

    let results = Arc::new(Mutex::new(Vec::new()));

    // Start the CP stub server on a random port.
    let stub = CpStub {
        task_json,
        results: Arc::clone(&results),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AgentDispatchServer::new(stub))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a GrpcTransport pointing at our stub.
    let transport = vasal_core::transport::grpc::GrpcTransport::new(
        format!("http://{addr}"),
        "test-agent".into(),
        "0.1.0".into(),
    );

    use vasal_core::transport::Transport;

    // Receive the task — may take a few recv rounds (ack arrives first).
    let mut work = vec![];
    for _ in 0..10 {
        match transport.recv_tasks().await {
            Ok(w) if !w.is_empty() => {
                work = w;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    assert_eq!(work.len(), 1);
    match &work[0] {
        vasal_core::transport::ReceivedWork::Single(t) => {
            assert_eq!(t.id(), task_id);
        }
        _ => panic!("expected Single task"),
    }

    // Send a result back.
    let result = TaskResult {
        task_id,
        chain_id: None,
        step_index: None,
        status: TaskResultStatus::Success,
        exit_code: Some(0),
        stdout: "grpc_e2e_test".into(),
        stderr: String::new(),
        duration_ms: 42,
        timestamp: 1234567890,
        error: None,
    };
    transport.send_result(&result).await.unwrap();

    // Give the CP stub time to receive the result.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the CP received the result.
    let collected = results.lock().await;
    assert_eq!(collected.len(), 1);
    let received_result: TaskResult = serde_json::from_slice(&collected[0]).unwrap();
    assert_eq!(received_result.task_id, task_id);
    assert_eq!(received_result.status, TaskResultStatus::Success);
    assert_eq!(received_result.stdout, "grpc_e2e_test");

    server_handle.abort();
}
