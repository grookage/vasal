//! Transport layer — configurable task dispatch and result reporting (DD-02).
//!
//! The transport module abstracts how the agent receives tasks from and reports
//! results to the control plane. Two modes are supported:
//!
//! - **Poll** (HTTP): agent GETs pending tasks on an interval, POSTs results.
//! - **gRPC stream**: bidirectional streaming, CP pushes tasks.
//!
//! Both modes feed into the same [`TaskManager`](crate::task::TaskManager).

pub mod grpc;
pub mod poll;

use async_trait::async_trait;
use vasal_protocol::task::{Task, TaskChain, TaskResult};

/// Trait abstracting the transport layer.
///
/// Implementations handle the wire protocol for receiving tasks and sending
/// results. The agent's main loop calls `recv_tasks` and `send_result`
/// without knowing the underlying transport.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Receive pending tasks from the control plane.
    ///
    /// For poll mode, this issues an HTTP GET. For gRPC mode, this reads
    /// from the bidirectional stream.
    async fn recv_tasks(&self) -> crate::Result<Vec<ReceivedWork>>;

    /// Report a task result to the control plane.
    async fn send_result(&self, result: &TaskResult) -> crate::Result<()>;
}

/// Work item received from the control plane.
///
/// Can be either a single task or a task chain.
#[derive(Debug, Clone)]
pub enum ReceivedWork {
    /// A single task.
    Single(Task),
    /// A task chain (sequential steps with rollback).
    Chain(TaskChain),
}
