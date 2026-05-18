pub mod chain;
pub mod continuous;
pub mod router;
pub mod shell;
pub mod sidecar;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, watch, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use vasal_protocol::heartbeat::ActiveTaskCounts;
use vasal_protocol::task::{ExecKind, ExecTask, Task, TaskChain, TaskResult};

use crate::config::RuntimeConfig;
use crate::state::StateStore;

/// Manages task lifecycle: routing, execution, concurrency, and cancellation.
pub struct TaskManager {
    semaphore: Arc<Semaphore>,
    active_tasks: Arc<Mutex<HashMap<Uuid, ActiveTask>>>,
    counts_tx: watch::Sender<ActiveTaskCounts>,
    http_client: reqwest::Client,
    socket_dir: PathBuf,
    store: StateStore,
    #[allow(dead_code)]
    runtime_rx: watch::Receiver<RuntimeConfig>,
    result_tx: Option<mpsc::Sender<TaskResult>>,
    shutdown: CancellationToken,
}

struct ActiveTask {
    cancel_token: CancellationToken,
    kind: TaskKindTag,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskKindTag {
    Oneshot,
    Continuous,
}

impl TaskManager {
    pub fn new(
        store: StateStore,
        http_client: reqwest::Client,
        socket_dir: PathBuf,
        runtime_rx: watch::Receiver<RuntimeConfig>,
        counts_tx: watch::Sender<ActiveTaskCounts>,
        result_tx: Option<mpsc::Sender<TaskResult>>,
        shutdown: CancellationToken,
    ) -> Self {
        let max_concurrent = runtime_rx.borrow().max_concurrent;
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            counts_tx,
            http_client,
            socket_dir,
            store,
            runtime_rx,
            result_tx,
            shutdown,
        }
    }

    /// Routes the task by type and spawns it onto the Tokio runtime.
    pub async fn submit(&self, task: Task) -> crate::Result<()> {
        let task_id = task.id();
        info!(task_id = %task_id, "task received");

        crate::audit::record(
            &self.store,
            crate::audit::event::TASK_RECEIVED,
            Some(&task_id.to_string()),
            serde_json::json!({"type": format!("{:?}", task)}),
        );

        match task {
            Task::Cancel(cancel) => {
                self.handle_cancel(cancel.target_task_id).await;
            }
            Task::Exec(exec) => {
                self.spawn_exec(exec).await?;
            }
            Task::Install(_) | Task::Upgrade(_) | Task::Remove(_) | Task::SelfUpgrade(_) => {
                self.spawn_routed(task).await?;
            }
        }

        Ok(())
    }

    async fn spawn_exec(&self, exec: ExecTask) -> crate::Result<()> {
        let task_id = exec.id;
        let cancel_token = CancellationToken::new();
        let kind_tag = match exec.kind {
            ExecKind::Oneshot => TaskKindTag::Oneshot,
            ExecKind::Continuous => TaskKindTag::Continuous,
        };

        {
            let mut active = self.active_tasks.lock().await;
            active.insert(
                task_id,
                ActiveTask {
                    cancel_token: cancel_token.clone(),
                    kind: kind_tag,
                },
            );
        }
        self.update_counts().await;

        let semaphore = Arc::clone(&self.semaphore);
        let active_tasks = Arc::clone(&self.active_tasks);
        let counts_tx = self.counts_tx.clone();
        let store = self.store.clone();
        let http_client = self.http_client.clone();
        let socket_dir = self.socket_dir.clone();
        let shutdown = self.shutdown.clone();
        let result_tx = self.result_tx.clone();

        tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    error!(task_id = %task_id, "semaphore closed");
                    return;
                }
            };

            let result = router::execute_exec(
                &exec,
                &http_client,
                &socket_dir,
                &store,
                cancel_token.clone(),
                shutdown,
            )
            .await;

            let journal_row = result_to_journal_row(&result);
            let s = store.clone();
            let _ = tokio::task::spawn_blocking(move || s.record_task_result(&journal_row)).await;

            let event_type = match result.status {
                vasal_protocol::task::TaskResultStatus::Success => {
                    crate::audit::event::TASK_COMPLETED
                }
                vasal_protocol::task::TaskResultStatus::Failed => crate::audit::event::TASK_FAILED,
                vasal_protocol::task::TaskResultStatus::Cancelled => {
                    crate::audit::event::TASK_CANCELLED
                }
                vasal_protocol::task::TaskResultStatus::Timeout => {
                    crate::audit::event::TASK_TIMEOUT
                }
                vasal_protocol::task::TaskResultStatus::RolledBack => {
                    crate::audit::event::TASK_FAILED
                }
            };
            crate::audit::record(
                &store,
                event_type,
                Some(&task_id.to_string()),
                serde_json::to_value(&result).unwrap_or_default(),
            );

            if let Some(tx) = &result_tx {
                let _ = tx.send(result.clone()).await;
            }

            {
                let mut active = active_tasks.lock().await;
                active.remove(&task_id);
            }
            update_counts_static(&active_tasks, &counts_tx).await;

            debug!(task_id = %task_id, status = ?result.status, "task completed");
        });

        Ok(())
    }

    async fn spawn_routed(&self, task: Task) -> crate::Result<()> {
        let task_id = task.id();
        let cancel_token = CancellationToken::new();

        {
            let mut active = self.active_tasks.lock().await;
            active.insert(
                task_id,
                ActiveTask {
                    cancel_token: cancel_token.clone(),
                    kind: TaskKindTag::Oneshot,
                },
            );
        }
        self.update_counts().await;

        let active_tasks = Arc::clone(&self.active_tasks);
        let counts_tx = self.counts_tx.clone();
        let store = self.store.clone();
        let http_client = self.http_client.clone();
        let socket_dir = self.socket_dir.clone();
        let result_tx = self.result_tx.clone();

        tokio::spawn(async move {
            let result =
                router::route_task(&task, &http_client, &socket_dir, &store, cancel_token).await;

            let journal_row = result_to_journal_row(&result);
            let s = store.clone();
            let _ = tokio::task::spawn_blocking(move || s.record_task_result(&journal_row)).await;

            if let Some(tx) = &result_tx {
                let _ = tx.send(result).await;
            }

            {
                let mut active = active_tasks.lock().await;
                active.remove(&task_id);
            }
            update_counts_static(&active_tasks, &counts_tx).await;
        });

        Ok(())
    }

    async fn handle_cancel(&self, target_task_id: Uuid) {
        let active = self.active_tasks.lock().await;
        if let Some(task) = active.get(&target_task_id) {
            info!(task_id = %target_task_id, "cancelling task");
            task.cancel_token.cancel();
        } else {
            warn!(task_id = %target_task_id, "cancel target not found");
        }
    }

    pub fn counts_rx(&self) -> watch::Receiver<ActiveTaskCounts> {
        self.counts_tx.subscribe()
    }

    /// Spawns the chain executor. Each step runs sequentially; on failure the
    /// configured rollback strategy is applied.
    pub async fn submit_chain(&self, chain: TaskChain) -> crate::Result<()> {
        let chain_id = chain.id;
        info!(chain_id = %chain_id, steps = chain.steps.len(), "chain received");

        crate::audit::record(
            &self.store,
            crate::audit::event::TASK_RECEIVED,
            Some(&chain_id.to_string()),
            serde_json::json!({"type": "chain", "steps": chain.steps.len()}),
        );

        let cancel_token = CancellationToken::new();

        {
            let mut active = self.active_tasks.lock().await;
            active.insert(
                chain_id,
                ActiveTask {
                    cancel_token: cancel_token.clone(),
                    kind: TaskKindTag::Oneshot,
                },
            );
        }
        self.update_counts().await;

        let semaphore = Arc::clone(&self.semaphore);
        let active_tasks = Arc::clone(&self.active_tasks);
        let counts_tx = self.counts_tx.clone();
        let store = self.store.clone();
        let http_client = self.http_client.clone();
        let socket_dir = self.socket_dir.clone();
        let result_tx = self.result_tx.clone();

        tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    error!(chain_id = %chain_id, "semaphore closed");
                    return;
                }
            };

            let results =
                chain::execute(&chain, &http_client, &socket_dir, &store, cancel_token).await;

            for result in &results {
                let journal_row = result_to_journal_row(result);
                let s = store.clone();
                let _ =
                    tokio::task::spawn_blocking(move || s.record_task_result(&journal_row)).await;

                if let Some(tx) = &result_tx {
                    let _ = tx.send(result.clone()).await;
                }
            }

            {
                let mut active = active_tasks.lock().await;
                active.remove(&chain_id);
            }
            update_counts_static(&active_tasks, &counts_tx).await;

            debug!(
                chain_id = %chain_id,
                step_results = results.len(),
                "chain execution finished",
            );
        });

        Ok(())
    }

    async fn update_counts(&self) {
        update_counts_static(&self.active_tasks, &self.counts_tx).await;
    }
}

async fn update_counts_static(
    active: &Mutex<HashMap<Uuid, ActiveTask>>,
    tx: &watch::Sender<ActiveTaskCounts>,
) {
    let guard = active.lock().await;
    let oneshot = guard
        .values()
        .filter(|t| t.kind == TaskKindTag::Oneshot)
        .count() as u32;
    let continuous = guard
        .values()
        .filter(|t| t.kind == TaskKindTag::Continuous)
        .count() as u32;
    let _ = tx.send(ActiveTaskCounts {
        oneshot,
        continuous,
        total: oneshot + continuous,
    });
}

fn result_to_journal_row(result: &TaskResult) -> crate::state::TaskResultRow {
    crate::state::TaskResultRow {
        task_id: result.task_id.to_string(),
        chain_id: result.chain_id.map(|id| id.to_string()),
        step_index: result.step_index.map(|i| i as i32),
        status: format!("{:?}", result.status),
        exit_code: result.exit_code,
        stdout: result.stdout.clone(),
        stderr: result.stderr.clone(),
        duration_ms: result.duration_ms as i64,
        created_at: result.timestamp as i64,
    }
}
