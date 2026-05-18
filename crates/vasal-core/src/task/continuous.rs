//! Continuous task executor — repeating work at a fixed interval until cancelled.

use std::path::Path;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use vasal_protocol::task::*;

use super::router::make_result;
use crate::credential::ResolvedCredentials;
use crate::state::StateStore;

pub async fn run(
    exec: &ExecTask,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    store: &StateStore,
    cancel: CancellationToken,
    shutdown: CancellationToken,
) -> TaskResult {
    let task_id = exec.id;
    let interval_ms = exec.interval_ms.unwrap_or(30_000);
    let interval = Duration::from_millis(interval_ms);

    info!(task_id = %task_id, interval_ms, "continuous task started");

    let mut last_result = make_result(
        task_id,
        TaskResultStatus::Success,
        None,
        String::new(),
        String::new(),
        Duration::ZERO,
        None,
    );

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!(task_id = %task_id, "continuous task cancelled");
                return make_result(
                    task_id,
                    TaskResultStatus::Cancelled,
                    None,
                    String::new(),
                    String::new(),
                    Duration::ZERO,
                    None,
                );
            }
            () = shutdown.cancelled() => {
                info!(task_id = %task_id, "continuous task stopping (shutdown)");
                return last_result;
            }
            () = tokio::time::sleep(interval) => {}
        }

        let tick_start = Instant::now();

        let creds = match crate::credential::resolve_eager(
            &exec.credentials,
            http_client,
            socket_dir,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                warn!(task_id = %task_id, error = %e, "credential resolution failed on tick");
                last_result = make_result(
                    task_id,
                    TaskResultStatus::Failed,
                    None,
                    String::new(),
                    e.to_string(),
                    tick_start.elapsed(),
                    Some(e.to_string()),
                );
                record_tick(store, &last_result);
                continue;
            }
        };

        let tick_cancel = cancel.child_token();
        let result = execute_tick(exec, &creds, socket_dir, tick_cancel).await;

        debug!(
            task_id = %task_id,
            status = ?result.status,
            duration_ms = result.duration_ms,
            "continuous tick completed",
        );

        record_tick(store, &result);
        last_result = result;
    }
}

async fn execute_tick(
    exec: &ExecTask,
    creds: &ResolvedCredentials,
    socket_dir: &Path,
    cancel: CancellationToken,
) -> TaskResult {
    match exec.executor {
        Executor::Shell => super::shell::execute(exec, creds, cancel).await,
        Executor::Sidecar => {
            let target = exec.target.as_deref().unwrap_or("unknown");
            let method = exec.method.as_deref().unwrap_or("submit");
            super::sidecar::execute(
                exec.id,
                target,
                method,
                &exec.payload,
                &exec.credentials,
                creds,
                exec.timeout_ms,
                socket_dir,
                cancel,
            )
            .await
        }
    }
}

fn record_tick(store: &StateStore, result: &TaskResult) {
    let row = crate::state::TaskResultRow {
        task_id: result.task_id.to_string(),
        chain_id: None,
        step_index: None,
        status: format!("{:?}", result.status),
        exit_code: result.exit_code,
        stdout: result.stdout.clone(),
        stderr: result.stderr.clone(),
        duration_ms: result.duration_ms as i64,
        created_at: result.timestamp as i64,
    };
    if let Err(e) = store.record_task_result(&row) {
        warn!(error = %e, "failed to record continuous tick result");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn cancellation_stops_continuous_task() {
        let exec = ExecTask {
            id: Uuid::new_v4(),
            priority: Priority::Normal,
            tags: HashMap::new(),
            kind: ExecKind::Continuous,
            executor: Executor::Shell,
            target: None,
            method: None,
            payload: json!({"script": "echo tick"}),
            interval_ms: Some(50),
            timeout_ms: 5000,
            credentials: vec![],
        };

        let store = crate::state::StateStore::open_in_memory().unwrap();
        let cancel = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let client = reqwest::Client::new();
        let dir = tempfile::tempdir().unwrap();

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(180)).await;
            cancel_clone.cancel();
        });

        let result = run(&exec, &client, dir.path(), &store, cancel, shutdown).await;
        assert_eq!(result.status, TaskResultStatus::Cancelled);
    }
}
