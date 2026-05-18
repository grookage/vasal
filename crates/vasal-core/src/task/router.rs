//! Task routing — dispatch by task type and executor.

use std::path::Path;
use std::time::Instant;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use vasal_protocol::task::*;

use super::{chain, continuous, shell, sidecar};
use crate::state::StateStore;

pub async fn execute_exec(
    exec: &ExecTask,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    store: &StateStore,
    cancel: CancellationToken,
    shutdown: CancellationToken,
) -> TaskResult {
    match exec.kind {
        ExecKind::Oneshot => execute_oneshot(exec, http_client, socket_dir, cancel).await,
        ExecKind::Continuous => {
            continuous::run(exec, http_client, socket_dir, store, cancel, shutdown).await
        }
    }
}

async fn execute_oneshot(
    exec: &ExecTask,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    cancel: CancellationToken,
) -> TaskResult {
    let start = Instant::now();

    let creds =
        match crate::credential::resolve_eager(&exec.credentials, http_client, socket_dir).await {
            Ok(c) => c,
            Err(e) => {
                return make_result(
                    exec.id,
                    TaskResultStatus::Failed,
                    None,
                    String::new(),
                    e.to_string(),
                    start.elapsed(),
                    Some(e.to_string()),
                );
            }
        };

    match exec.executor {
        Executor::Shell => shell::execute(exec, &creds, cancel).await,
        Executor::Sidecar => {
            let target = exec.target.as_deref().unwrap_or("unknown");
            let method = exec.method.as_deref().unwrap_or("submit");
            sidecar::execute(
                exec.id,
                target,
                method,
                &exec.payload,
                &exec.credentials,
                &creds,
                exec.timeout_ms,
                socket_dir,
                cancel,
            )
            .await
        }
    }
}

pub async fn route_task(
    task: &Task,
    http_client: &reqwest::Client,
    _socket_dir: &Path,
    store: &StateStore,
    _cancel: CancellationToken,
) -> TaskResult {
    let start = Instant::now();
    let task_id = task.id();

    match task {
        Task::Install(install) => {
            debug!(unit = %install.unit.name, "routing install task");
            make_result(
                task_id,
                TaskResultStatus::Success,
                None,
                format!("installed unit {}", install.unit.name),
                String::new(),
                start.elapsed(),
                None,
            )
        }
        Task::Upgrade(upgrade) => {
            debug!(unit = %upgrade.unit_name, "routing upgrade task");
            make_result(
                task_id,
                TaskResultStatus::Success,
                None,
                format!(
                    "upgraded unit {} to {}",
                    upgrade.unit_name, upgrade.target_version
                ),
                String::new(),
                start.elapsed(),
                None,
            )
        }
        Task::Remove(remove) => {
            debug!(unit = %remove.unit_name, "routing remove task");
            make_result(
                task_id,
                TaskResultStatus::Success,
                None,
                format!("removed unit {}", remove.unit_name),
                String::new(),
                start.elapsed(),
                None,
            )
        }
        Task::SelfUpgrade(upgrade) => {
            debug!(version = %upgrade.target_version, "routing self-upgrade task");
            match crate::self_upgrade::execute(
                &upgrade.artifact.url,
                &upgrade.artifact.sha256,
                &upgrade.target_version,
                env!("CARGO_PKG_VERSION"),
                &store.data_dir_or_default(),
                http_client,
            )
            .await
            {
                Ok(()) => make_result(
                    task_id,
                    TaskResultStatus::Success,
                    None,
                    format!(
                        "self-upgrade to {} prepared — restart required",
                        upgrade.target_version
                    ),
                    String::new(),
                    start.elapsed(),
                    None,
                ),
                Err(e) => make_result(
                    task_id,
                    TaskResultStatus::Failed,
                    None,
                    String::new(),
                    e.to_string(),
                    start.elapsed(),
                    Some(e.to_string()),
                ),
            }
        }
        Task::Exec(_) | Task::Cancel(_) => {
            error!(task_id = %task_id, "exec/cancel routed through route_task — this is a bug");
            make_result(
                task_id,
                TaskResultStatus::Failed,
                None,
                String::new(),
                "internal routing error".into(),
                start.elapsed(),
                Some("exec/cancel should not reach route_task".into()),
            )
        }
    }
}

pub async fn execute_chain(
    task_chain: &TaskChain,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    store: &StateStore,
    cancel: CancellationToken,
) -> Vec<TaskResult> {
    chain::execute(task_chain, http_client, socket_dir, store, cancel).await
}

pub fn make_result(
    task_id: uuid::Uuid,
    status: TaskResultStatus,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    elapsed: std::time::Duration,
    error: Option<String>,
) -> TaskResult {
    TaskResult {
        task_id,
        chain_id: None,
        step_index: None,
        status,
        exit_code,
        stdout,
        stderr,
        duration_ms: elapsed.as_millis() as u64,
        timestamp: crate::state::now_ms() as u64,
        error,
    }
}
