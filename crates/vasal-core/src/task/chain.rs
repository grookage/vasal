//! Task chain executor — sequential steps with rollback.

use std::path::Path;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use vasal_protocol::task::*;

use super::router;
use crate::state::StateStore;

pub async fn execute(
    chain: &TaskChain,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    store: &StateStore,
    cancel: CancellationToken,
) -> Vec<TaskResult> {
    let mut results = Vec::new();
    let chain_id = chain.id;

    info!(chain_id = %chain_id, steps = chain.steps.len(), "executing task chain");

    crate::audit::record(
        store,
        crate::audit::event::CHAIN_STARTED,
        Some(&chain_id.to_string()),
        serde_json::json!({"steps": chain.steps.len()}),
    );

    for (idx, step) in chain.steps.iter().enumerate() {
        if cancel.is_cancelled() {
            info!(chain_id = %chain_id, step = idx, "chain cancelled");
            break;
        }

        debug!(chain_id = %chain_id, step = idx, "executing chain step");

        let creds =
            match crate::credential::resolve_eager(&step.task.credentials, http_client, socket_dir)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    let mut result = router::make_result(
                        step.task.id,
                        TaskResultStatus::Failed,
                        None,
                        String::new(),
                        e.to_string(),
                        std::time::Duration::ZERO,
                        Some(e.to_string()),
                    );
                    result.chain_id = Some(chain_id);
                    result.step_index = Some(idx as u32);
                    results.push(result);

                    run_rollbacks(
                        chain,
                        &results,
                        idx,
                        http_client,
                        socket_dir,
                        store,
                        &cancel,
                    )
                    .await;
                    return results;
                }
            };

        let step_cancel = cancel.child_token();
        let mut result = match step.task.executor {
            Executor::Shell => super::shell::execute(&step.task, &creds, step_cancel).await,
            Executor::Sidecar => {
                let target = step.task.target.as_deref().unwrap_or("unknown");
                let method = step.task.method.as_deref().unwrap_or("submit");
                super::sidecar::execute(
                    step.task.id,
                    target,
                    method,
                    &step.task.payload,
                    &step.task.credentials,
                    &creds,
                    step.task.timeout_ms,
                    socket_dir,
                    step_cancel,
                )
                .await
            }
        };

        result.chain_id = Some(chain_id);
        result.step_index = Some(idx as u32);

        let failed = result.status != TaskResultStatus::Success;
        results.push(result);

        if failed {
            info!(chain_id = %chain_id, step = idx, "chain step failed — initiating rollback");
            run_rollbacks(
                chain,
                &results,
                idx,
                http_client,
                socket_dir,
                store,
                &cancel,
            )
            .await;
            return results;
        }
    }

    crate::audit::record(
        store,
        crate::audit::event::CHAIN_COMPLETED,
        Some(&chain_id.to_string()),
        serde_json::json!({"steps_completed": results.len()}),
    );

    info!(chain_id = %chain_id, "chain completed successfully");
    results
}

async fn run_rollbacks(
    chain: &TaskChain,
    _results: &[TaskResult],
    failed_step_idx: usize,
    http_client: &reqwest::Client,
    socket_dir: &Path,
    store: &StateStore,
    cancel: &CancellationToken,
) {
    crate::audit::record(
        store,
        crate::audit::event::CHAIN_ROLLBACK,
        Some(&chain.id.to_string()),
        serde_json::json!({
            "strategy": format!("{:?}", chain.on_failure),
            "failed_step": failed_step_idx,
        }),
    );

    let rollback_range: Vec<usize> = match chain.on_failure {
        RollbackStrategy::RollbackFailed => {
            vec![failed_step_idx]
        }
        RollbackStrategy::RollbackAll => {
            (0..=failed_step_idx).rev().collect()
        }
    };

    for idx in rollback_range {
        if let Some(rollback_task) = &chain.steps[idx].rollback {
            debug!(chain_id = %chain.id, step = idx, "executing rollback");

            let creds = match crate::credential::resolve_eager(
                &rollback_task.credentials,
                http_client,
                socket_dir,
            )
            .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!(chain_id = %chain.id, step = idx, error = %e, "rollback credential resolution failed");
                    continue;
                }
            };

            let rb_cancel = cancel.child_token();
            let result = match rollback_task.executor {
                Executor::Shell => super::shell::execute(rollback_task, &creds, rb_cancel).await,
                Executor::Sidecar => {
                    let target = rollback_task.target.as_deref().unwrap_or("unknown");
                    let method = rollback_task.method.as_deref().unwrap_or("submit");
                    super::sidecar::execute(
                        rollback_task.id,
                        target,
                        method,
                        &rollback_task.payload,
                        &rollback_task.credentials,
                        &creds,
                        rollback_task.timeout_ms,
                        socket_dir,
                        rb_cancel,
                    )
                    .await
                }
            };

            if result.status == TaskResultStatus::Success {
                debug!(chain_id = %chain.id, step = idx, "rollback succeeded");
            } else {
                warn!(
                    chain_id = %chain.id, step = idx,
                    status = ?result.status,
                    "rollback failed — continuing with remaining rollbacks",
                );
            }
        } else {
            debug!(chain_id = %chain.id, step = idx, "no rollback action defined — skipping");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn make_step(script: &str, rollback_script: Option<&str>) -> ChainStep {
        ChainStep {
            task: ExecTask {
                id: Uuid::new_v4(),
                priority: Priority::Normal,
                tags: HashMap::new(),
                kind: ExecKind::Oneshot,
                executor: Executor::Shell,
                target: None,
                method: None,
                payload: json!({"script": script}),
                interval_ms: None,
                timeout_ms: 5000,
                credentials: vec![],
            },
            rollback: rollback_script.map(|s| ExecTask {
                id: Uuid::new_v4(),
                priority: Priority::Normal,
                tags: HashMap::new(),
                kind: ExecKind::Oneshot,
                executor: Executor::Shell,
                target: None,
                method: None,
                payload: json!({"script": s}),
                interval_ms: None,
                timeout_ms: 5000,
                credentials: vec![],
            }),
        }
    }

    #[tokio::test]
    async fn all_steps_succeed() {
        let chain = TaskChain {
            id: Uuid::new_v4(),
            steps: vec![
                make_step("echo step1", None),
                make_step("echo step2", None),
                make_step("echo step3", None),
            ],
            on_failure: RollbackStrategy::RollbackAll,
            tags: HashMap::new(),
        };

        let store = crate::state::StateStore::open_in_memory().unwrap();
        let cancel = CancellationToken::new();
        let client = reqwest::Client::new();
        let dir = tempfile::tempdir().unwrap();

        let results = execute(&chain, &client, dir.path(), &store, cancel).await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.status, TaskResultStatus::Success);
            assert_eq!(r.chain_id, Some(chain.id));
        }
        assert_eq!(results[0].step_index, Some(0));
        assert_eq!(results[1].step_index, Some(1));
        assert_eq!(results[2].step_index, Some(2));
    }

    #[tokio::test]
    async fn step_fails_aborts_chain() {
        let chain = TaskChain {
            id: Uuid::new_v4(),
            steps: vec![
                make_step("echo step1", Some("echo rollback1")),
                make_step("exit 1", Some("echo rollback2")),
                make_step("echo step3", None),
            ],
            on_failure: RollbackStrategy::RollbackFailed,
            tags: HashMap::new(),
        };

        let store = crate::state::StateStore::open_in_memory().unwrap();
        let cancel = CancellationToken::new();
        let client = reqwest::Client::new();
        let dir = tempfile::tempdir().unwrap();

        let results = execute(&chain, &client, dir.path(), &store, cancel).await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, TaskResultStatus::Success);
        assert_eq!(results[1].status, TaskResultStatus::Failed);
    }
}
