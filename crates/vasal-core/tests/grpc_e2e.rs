//! End-to-end gRPC transport test with a control plane stub.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use vasal_core::transport::grpc::proto::{
    agent_dispatch_server::{AgentDispatch, AgentDispatchServer},
    agent_message, control_plane_message, AgentMessage, ControlPlaneMessage, ServerAck,
};

use vasal_protocol::task::*;

/// Minimal CP stub that sends one task and collects one result.
struct CpStub {
    task_json: Vec<u8>,
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

        let (tx, rx) = mpsc::channel::<Result<ControlPlaneMessage, Status>>(16);

        tokio::spawn(async move {
            // Wait for AgentHello, then send ack + task
            if let Some(msg) = inbound.message().await.ok().flatten().as_ref() {
                if let Some(agent_message::Payload::Hello(hello)) = &msg.payload {
                    let ack = ControlPlaneMessage {
                        payload: Some(control_plane_message::Payload::Ack(ServerAck {
                            accepted: true,
                            message: format!("welcome agent {}", hello.agent_id),
                        })),
                    };
                    let _ = tx.send(Ok(ack)).await;

                    let task_msg = ControlPlaneMessage {
                        payload: Some(control_plane_message::Payload::Task(task_bytes)),
                    };
                    let _ = tx.send(Ok(task_msg)).await;
                }
            }

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

    tokio::time::sleep(Duration::from_millis(100)).await;

    let transport = vasal_core::transport::grpc::GrpcTransport::new(
        format!("http://{addr}"),
        "test-agent".into(),
        "0.1.0".into(),
    );

    use vasal_core::transport::Transport;

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

    tokio::time::sleep(Duration::from_millis(500)).await;

    let collected = results.lock().await;
    assert_eq!(collected.len(), 1);
    let received_result: TaskResult = serde_json::from_slice(&collected[0]).unwrap();
    assert_eq!(received_result.task_id, task_id);
    assert_eq!(received_result.status, TaskResultStatus::Success);
    assert_eq!(received_result.stdout, "grpc_e2e_test");

    server_handle.abort();
}
