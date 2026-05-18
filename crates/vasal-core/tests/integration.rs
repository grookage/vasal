//! Integration tests for vasal-core.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use vasal_core::config::RuntimeConfig;
use vasal_core::state::StateStore;
use vasal_core::task::TaskManager;
use vasal_protocol::heartbeat::ActiveTaskCounts;
use vasal_protocol::task::*;

fn default_runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        log_level: "info".into(),
        max_concurrent: 4,
        heartbeat_interval_sec: 10,
        health_check_interval_sec: 30,
        audit_batch_size: 50,
        audit_flush_interval_sec: 5,
    }
}

fn make_exec(script: &str) -> ExecTask {
    ExecTask {
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
    }
}

fn make_chain_step(script: &str, rollback_script: Option<&str>) -> ChainStep {
    ChainStep {
        task: make_exec(script),
        rollback: rollback_script.map(make_exec),
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned()
}

fn echo_ctrl_binary() -> PathBuf {
    workspace_root().join("target/debug/echo-ctrl")
}

async fn spawn_echo_ctrl(socket_path: &Path) -> tokio::process::Child {
    if !echo_ctrl_binary().exists() {
        let status = tokio::process::Command::new("cargo")
            .args(["build", "-p", "echo-ctrl"])
            .current_dir(workspace_root())
            .status()
            .await
            .expect("failed to run cargo build");
        assert!(status.success(), "echo-ctrl build failed");
    }

    let child = tokio::process::Command::new(echo_ctrl_binary())
        .arg(socket_path.to_str().unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "failed to spawn echo-ctrl at {}: {e}",
                echo_ctrl_binary().display()
            )
        });

    for _ in 0..100 {
        if socket_path.exists() {
            return child;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "echo-ctrl socket never appeared at {}",
        socket_path.display()
    );
}

#[tokio::test]
async fn shell_task_success() {
    let exec = make_exec("echo integration_test");
    let creds = HashMap::new();
    let cancel = CancellationToken::new();

    let result = vasal_core::task::shell::execute(&exec, &creds, cancel).await;

    assert_eq!(result.status, TaskResultStatus::Success);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.stdout.trim(), "integration_test");
    assert_eq!(result.task_id, exec.id);

    // Also verify multiline output handling while we're here
    let exec2 = make_exec("printf 'line1\\nline2\\nline3'");
    let result2 =
        vasal_core::task::shell::execute(&exec2, &HashMap::new(), CancellationToken::new()).await;
    assert_eq!(result2.status, TaskResultStatus::Success);
    let lines: Vec<&str> = result2.stdout.lines().collect();
    assert_eq!(lines, vec!["line1", "line2", "line3"]);
}

#[tokio::test]
async fn shell_task_nonzero_exit() {
    let exec = make_exec("exit 42");
    let result =
        vasal_core::task::shell::execute(&exec, &HashMap::new(), CancellationToken::new()).await;

    assert_eq!(result.status, TaskResultStatus::Failed);
    assert_eq!(result.exit_code, Some(42));
}

#[tokio::test]
async fn shell_task_timeout() {
    let mut exec = make_exec("sleep 60");
    exec.timeout_ms = 200;

    let result =
        vasal_core::task::shell::execute(&exec, &HashMap::new(), CancellationToken::new()).await;

    assert_eq!(result.status, TaskResultStatus::Timeout);
    assert!(
        result.error.is_some(),
        "timeout should populate error field"
    );
}

#[tokio::test]
async fn shell_task_cancellation() {
    let exec = make_exec("sleep 60");
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_clone.cancel();
    });

    let result = vasal_core::task::shell::execute(&exec, &HashMap::new(), cancel).await;
    assert_eq!(result.status, TaskResultStatus::Cancelled);
}

// regression: used to panic on empty PATH
#[tokio::test]
async fn shell_task_credential_injection() {
    let exec = make_exec("echo $SECRET_KEY");
    let mut creds = HashMap::new();
    creds.insert("SECRET_KEY".into(), "hunter2".into());

    let result = vasal_core::task::shell::execute(&exec, &creds, CancellationToken::new()).await;

    assert_eq!(result.status, TaskResultStatus::Success);
    assert_eq!(result.stdout.trim(), "hunter2");
}

#[tokio::test]
async fn shell_task_stderr_captured() {
    let exec = make_exec("echo err_output >&2");
    let result =
        vasal_core::task::shell::execute(&exec, &HashMap::new(), CancellationToken::new()).await;

    assert_eq!(result.status, TaskResultStatus::Success);
    assert_eq!(result.stderr.trim(), "err_output");
}

#[tokio::test]
async fn chain_all_steps_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let client = reqwest::Client::new();

    let chain = TaskChain {
        id: Uuid::new_v4(),
        steps: vec![
            make_chain_step("echo step1", None),
            make_chain_step("echo step2", None),
            make_chain_step("echo step3", None),
        ],
        on_failure: RollbackStrategy::RollbackAll,
        tags: HashMap::new(),
    };

    let results =
        vasal_core::task::chain::execute(&chain, &client, socket_dir.path(), &store, cancel).await;

    assert_eq!(results.len(), 3);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(
            r.status,
            TaskResultStatus::Success,
            "step {i} should succeed"
        );
        assert_eq!(r.chain_id, Some(chain.id));
        assert_eq!(r.step_index, Some(i as u32));
    }
    assert_eq!(results[0].stdout.trim(), "step1");
    assert_eq!(results[2].stdout.trim(), "step3");
}

#[tokio::test]
async fn chain_rollback_all_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let client = reqwest::Client::new();

    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("rollback_step0_ran");

    let chain = TaskChain {
        id: Uuid::new_v4(),
        steps: vec![
            make_chain_step("echo step1", Some(&format!("touch {}", marker.display()))),
            make_chain_step("exit 1", Some("echo rollback2")),
            make_chain_step("echo step3_never", None),
        ],
        on_failure: RollbackStrategy::RollbackAll,
        tags: HashMap::new(),
    };

    let results =
        vasal_core::task::chain::execute(&chain, &client, socket_dir.path(), &store, cancel).await;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status, TaskResultStatus::Success);
    assert_eq!(results[1].status, TaskResultStatus::Failed);

    assert!(
        marker.exists(),
        "rollback_all should have run step 0's rollback (marker file)"
    );
}

#[tokio::test]
async fn chain_rollback_failed_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let client = reqwest::Client::new();

    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("should_not_exist");

    let chain = TaskChain {
        id: Uuid::new_v4(),
        steps: vec![
            make_chain_step("echo step1", Some(&format!("touch {}", marker.display()))),
            make_chain_step("exit 1", Some("echo only_this_rollback")),
            make_chain_step("echo step3_never", None),
        ],
        on_failure: RollbackStrategy::RollbackFailed,
        tags: HashMap::new(),
    };

    let results =
        vasal_core::task::chain::execute(&chain, &client, socket_dir.path(), &store, cancel).await;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status, TaskResultStatus::Success);
    assert_eq!(results[1].status, TaskResultStatus::Failed);
    // Only the failed step's rollback should run, not step 0's
    assert!(!marker.exists());
}

#[tokio::test]
async fn sidecar_health_check() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("echo-ctrl.sock");
    let mut child = spawn_echo_ctrl(&socket_path).await;

    let response = vasal_core::task::sidecar::call_raw(&socket_path, "health", None)
        .await
        .expect("health call failed");

    assert!(response.error.is_none());
    let result = response.result.unwrap();
    assert_eq!(result["status"], "ok");
    assert!(result["version"].is_string());

    child.kill().await.ok();
}

#[tokio::test]
async fn sidecar_submit_echo() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("echo-ctrl.sock");
    let mut child = spawn_echo_ctrl(&socket_path).await;

    let params = json!({"message": "hello from integration test", "count": 42});
    let response = vasal_core::task::sidecar::call_raw(&socket_path, "submit", Some(params))
        .await
        .expect("submit call failed");

    assert!(response.error.is_none());
    let result = response.result.unwrap();
    assert_eq!(result["status"], "completed");

    let stdout = result["stdout"].as_str().unwrap();
    let echoed: serde_json::Value = serde_json::from_str(stdout).unwrap();
    assert_eq!(echoed["message"], "hello from integration test");
    assert_eq!(echoed["count"], 42);

    child.kill().await.ok();
}

#[tokio::test]
async fn sidecar_execute_full_flow() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("echo-ctrl.sock");
    let mut child = spawn_echo_ctrl(&socket_path).await;

    let task_id = Uuid::new_v4();
    let payload = json!({"action": "discover", "target": "db-01"});
    let cancel = CancellationToken::new();

    let result = vasal_core::task::sidecar::execute(
        task_id,
        "echo-ctrl",
        "submit",
        &payload,
        &[],
        &HashMap::new(),
        5000,
        socket_dir.path(),
        cancel,
    )
    .await;

    assert_eq!(result.status, TaskResultStatus::Success);
    assert_eq!(result.task_id, task_id);

    let echoed: serde_json::Value =
        serde_json::from_str(&result.stdout).expect("stdout should be valid JSON");
    assert_eq!(echoed["action"], "discover");
    assert_eq!(echoed["target"], "db-01");
    assert!(result.duration_ms < 5000);

    child.kill().await.ok();
}

#[tokio::test]
async fn sidecar_unknown_method_returns_error() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("echo-ctrl.sock");
    let mut child = spawn_echo_ctrl(&socket_path).await;

    let response =
        vasal_core::task::sidecar::call_raw(&socket_path, "nonexistent_method", None).await;

    let resp = response.expect("should still get a response");
    assert!(
        resp.error.is_some(),
        "unknown method should return an error"
    );

    child.kill().await.ok();
}

#[tokio::test]
async fn task_manager_shell_submit_and_journal() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let shutdown = CancellationToken::new();

    let rt_config = default_runtime_config();
    let (_runtime_tx, runtime_rx) = watch::channel(rt_config);
    let (counts_tx, _counts_rx) = watch::channel(ActiveTaskCounts::default());

    let manager = TaskManager::new(
        store.clone(),
        reqwest::Client::new(),
        socket_dir.path().to_owned(),
        runtime_rx,
        counts_tx,
        None,
        shutdown,
    );

    let exec = make_exec("echo task_manager_test");
    let task_id = exec.id;
    manager.submit(Task::Exec(exec)).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let events = store.pending_audit_events(100).unwrap();
    let task_events: Vec<_> = events
        .iter()
        .filter(|e| e.task_id.as_deref() == Some(&task_id.to_string()))
        .collect();
    assert!(!task_events.is_empty());

    let has_received = task_events.iter().any(|e| e.event_type == "task.received");
    assert!(has_received, "should have a task.received audit event");

    let has_terminal = task_events
        .iter()
        .any(|e| e.event_type == "task.completed" || e.event_type == "task.failed");
    assert!(has_terminal);
}

#[tokio::test]
async fn task_manager_cancel_running_task() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let shutdown = CancellationToken::new();

    let rt_config = default_runtime_config();
    let (_runtime_tx, runtime_rx) = watch::channel(rt_config);
    let (counts_tx, _counts_rx) = watch::channel(ActiveTaskCounts::default());

    let manager = TaskManager::new(
        store.clone(),
        reqwest::Client::new(),
        socket_dir.path().to_owned(),
        runtime_rx,
        counts_tx,
        None,
        shutdown,
    );

    let mut exec = make_exec("sleep 60");
    let task_id = exec.id;
    exec.timeout_ms = 60_000;
    manager.submit(Task::Exec(exec)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let cancel_task = Task::Cancel(CancelTask {
        id: Uuid::new_v4(),
        priority: Priority::Normal,
        tags: HashMap::new(),
        target_task_id: task_id,
    });
    manager.submit(cancel_task).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let events = store.pending_audit_events(100).unwrap();
    let cancelled = events.iter().any(|e| {
        e.event_type == "task.cancelled" && e.task_id.as_deref() == Some(&task_id.to_string())
    });
    assert!(cancelled, "should have a task.cancelled audit event");
}

#[tokio::test]
async fn task_manager_sidecar_submit() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("echo-ctrl.sock");
    let mut child = spawn_echo_ctrl(&socket_path).await;

    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();
    let shutdown = CancellationToken::new();

    let rt_config = default_runtime_config();
    let (_runtime_tx, runtime_rx) = watch::channel(rt_config);
    let (counts_tx, _counts_rx) = watch::channel(ActiveTaskCounts::default());

    let manager = TaskManager::new(
        store.clone(),
        reqwest::Client::new(),
        socket_dir.path().to_owned(),
        runtime_rx,
        counts_tx,
        None,
        shutdown,
    );

    let exec = ExecTask {
        id: Uuid::new_v4(),
        priority: Priority::Normal,
        tags: HashMap::new(),
        kind: ExecKind::Oneshot,
        executor: Executor::Sidecar,
        target: Some("echo-ctrl".into()),
        method: Some("submit".into()),
        payload: json!({"action": "ping"}),
        interval_ms: None,
        timeout_ms: 5000,
        credentials: vec![],
    };
    let task_id = exec.id;

    manager.submit(Task::Exec(exec)).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let events = store.pending_audit_events(100).unwrap();
    let has_completed = events.iter().any(|e| {
        e.event_type == "task.completed" && e.task_id.as_deref() == Some(&task_id.to_string())
    });
    assert!(has_completed);

    child.kill().await.ok();
}

#[test]
fn state_store_persists_across_reopens() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = StateStore::open(dir.path()).unwrap();
        store
            .upsert_unit(&vasal_core::state::UnitRow {
                name: "persist-test".into(),
                kind: "sidecar".into(),
                version: "2.0.0".into(),
                state: "running".into(),
                health: Some("ok".into()),
                health_error: None,
                pid: Some(9999),
                socket_path: Some("/tmp/persist.sock".into()),
                config_json: Some(r#"{"key":"value"}"#.into()),
                updated_at: 1234567890,
            })
            .unwrap();

        store
            .record_task_result(&vasal_core::state::TaskResultRow {
                task_id: "persist-task-1".into(),
                chain_id: None,
                step_index: None,
                status: "Success".into(),
                exit_code: Some(0),
                stdout: "persisted output".into(),
                stderr: String::new(),
                duration_ms: 50,
                created_at: 1234567890,
            })
            .unwrap();
    }

    {
        let store = StateStore::open(dir.path()).unwrap();
        let unit = store
            .get_unit("persist-test")
            .unwrap()
            .expect("unit should persist across reopens");
        assert_eq!(unit.version, "2.0.0");
        assert_eq!(unit.pid, Some(9999));
        assert_eq!(unit.state, "running");
        assert_eq!(unit.config_json.as_deref(), Some(r#"{"key":"value"}"#));

        assert_eq!(store.data_dir_or_default(), dir.path());
    }
}

#[test]
fn state_store_unit_list_and_remove() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();

    for name in &["alpha", "beta", "gamma"] {
        store
            .upsert_unit(&vasal_core::state::UnitRow {
                name: name.to_string(),
                kind: "sidecar".into(),
                version: "1.0.0".into(),
                state: "running".into(),
                health: None,
                health_error: None,
                pid: None,
                socket_path: None,
                config_json: None,
                updated_at: 0,
            })
            .unwrap();
    }

    let units = store.list_units().unwrap();
    assert_eq!(units.len(), 3);
    assert_eq!(units[0].name, "alpha");
    assert_eq!(units[2].name, "gamma");

    store.remove_unit("beta").unwrap();
    let units = store.list_units().unwrap();
    assert_eq!(units.len(), 2);
    assert!(store.get_unit("beta").unwrap().is_none());
}

#[test]
fn config_load_from_toml_file() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");

    std::fs::write(
        &config_path,
        r#"
[agent]
log_level = "debug"

[transport]
mode = "poll"

[transport.poll]
endpoint = "https://cp.example.com/api/v1"
interval_sec = 5

[heartbeat]
interval_sec = 15
endpoint = "https://cp.example.com/api/v1/heartbeat"

[audit]
endpoint = "https://cp.example.com/api/v1/audit"
batch_size = 100

[auth]
provider = "https://auth.example.com/v1/token"

[shell]
max_concurrent = 8
default_timeout_ms = 60000

[units]
health_check_interval_sec = 60
"#,
    )
    .unwrap();

    let config = vasal_core::config::Config::load(&config_path).unwrap();
    assert_eq!(config.agent.log_level, "debug");
    assert_eq!(config.shell.max_concurrent, 8);
    assert_eq!(config.shell.default_timeout_ms, 60_000);
    assert_eq!(config.heartbeat.interval_sec, 15);

    let rt = config.runtime();
    assert_eq!(rt.log_level, "debug");
    assert_eq!(rt.max_concurrent, 8);
    assert_eq!(rt.heartbeat_interval_sec, 15);
    assert_eq!(rt.health_check_interval_sec, 60);
    assert_eq!(rt.audit_batch_size, 100);
}

#[test]
fn config_load_invalid_toml_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "this is not valid toml [[[").unwrap();

    assert!(vasal_core::config::Config::load(&path).is_err());
}

#[test]
fn config_load_missing_file_returns_error() {
    let path = PathBuf::from("/tmp/vasal-nonexistent-config.toml");
    assert!(vasal_core::config::Config::load(&path).is_err());
}

#[test]
fn audit_record_and_retrieve() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();

    vasal_core::audit::record(
        &store,
        "task.started",
        Some("audit-test-1"),
        json!({"script": "echo hello"}),
    );
    vasal_core::audit::record(
        &store,
        "task.completed",
        Some("audit-test-1"),
        json!({"exit_code": 0}),
    );
    vasal_core::audit::record(&store, "agent.started", None, json!({"version": "0.1.0"}));

    let events = store.pending_audit_events(100).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].event_type, "task.started");
    assert_eq!(events[0].task_id.as_deref(), Some("audit-test-1"));
    assert_eq!(events[1].event_type, "task.completed");
    assert_eq!(events[2].event_type, "agent.started");
    assert!(events[2].task_id.is_none());
}

#[test]
fn audit_mark_forwarded() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();

    for i in 0..5 {
        vasal_core::audit::record(
            &store,
            "task.completed",
            Some(&format!("task-{i}")),
            json!({}),
        );
    }

    let events = store.pending_audit_events(100).unwrap();
    assert_eq!(events.len(), 5);

    let ids: Vec<i64> = events.iter().take(3).filter_map(|e| e.id).collect();
    store.mark_forwarded(&ids).unwrap();

    let remaining = store.pending_audit_events(100).unwrap();
    assert_eq!(remaining.len(), 2);
}

#[test]
fn audit_journal_prune() {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(dir.path()).unwrap();

    for i in 0..20 {
        store
            .record_task_result(&vasal_core::state::TaskResultRow {
                task_id: format!("prune-task-{i}"),
                chain_id: None,
                step_index: None,
                status: "Success".into(),
                exit_code: Some(0),
                stdout: format!("output-{i}"),
                stderr: String::new(),
                duration_ms: 10,
                created_at: i as i64,
            })
            .unwrap();
    }

    let deleted = store.prune_journal(5).unwrap();
    assert_eq!(deleted, 15);
}
