//! Shell executor — the only built-in executor (DD-01).
//!
//! Spawns a shell process via `tokio::process::Command`, captures stdout and
//! stderr, enforces timeouts, and injects resolved credentials as environment
//! variables.

use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use vasal_protocol::task::{ExecTask, TaskResult, TaskResultStatus};

use super::router::make_result;
use crate::credential::ResolvedCredentials;

/// Maximum captured output size per stream (1 MB).
const MAX_OUTPUT_SIZE: usize = 1024 * 1024;

/// Execute a shell task.
///
/// The `payload.script` field is expected to contain the shell command.
/// Credentials are injected as environment variables — NEVER as command
/// arguments (security: args are visible in `/proc` and `ps`).
pub async fn execute(
    exec: &ExecTask,
    creds: &ResolvedCredentials,
    cancel: CancellationToken,
) -> TaskResult {
    let start = Instant::now();
    let task_id = exec.id;

    // Extract script from payload.
    let script = match exec.payload.get("script").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => {
            return make_result(
                task_id,
                TaskResultStatus::Failed,
                None,
                String::new(),
                "missing 'script' field in payload".into(),
                start.elapsed(),
                Some("missing 'script' field in payload".into()),
            );
        }
    };

    debug!(task_id = %task_id, script_len = script.len(), "executing shell task");

    // Build command.
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(script);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.stdin(Stdio::null());

    // Inject credentials as environment variables.
    for (key, value) in creds {
        cmd.env(key, value);
    }

    // Set working dir from payload if specified, otherwise use default.
    if let Some(dir) = exec.payload.get("working_dir").and_then(|v| v.as_str()) {
        cmd.current_dir(dir);
    }

    // Spawn.
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return make_result(
                task_id,
                TaskResultStatus::Failed,
                None,
                String::new(),
                e.to_string(),
                start.elapsed(),
                Some(format!("failed to spawn shell: {e}")),
            );
        }
    };

    let timeout = Duration::from_millis(exec.timeout_ms);

    // Take stdout/stderr handles before the select (to avoid ownership issues
    // with `wait_with_output` which consumes `child`).
    let mut stdout_pipe = child.stdout.take().unwrap();
    let mut stderr_pipe = child.stderr.take().unwrap();

    // Spawn readers for stdout and stderr.
    let stdout_handle = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut stdout_pipe, &mut buf).await;
        buf
    });
    let stderr_handle = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr_pipe, &mut buf).await;
        buf
    });

    // Wait for completion, timeout, or cancellation.
    tokio::select! {
        biased;

        () = cancel.cancelled() => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            make_result(
                task_id,
                TaskResultStatus::Cancelled,
                None,
                String::new(),
                String::new(),
                start.elapsed(),
                None,
            )
        }

        () = tokio::time::sleep(timeout) => {
            warn!(task_id = %task_id, timeout_ms = exec.timeout_ms, "shell task timed out");
            let _ = child.start_kill();
            let _ = child.wait().await;
            make_result(
                task_id,
                TaskResultStatus::Timeout,
                None,
                String::new(),
                String::new(),
                start.elapsed(),
                Some(format!("timeout after {}ms", exec.timeout_ms)),
            )
        }

        status = child.wait() => {
            match status {
                Ok(exit_status) => {
                    let exit_code = exit_status.code().unwrap_or(-1);
                    let stdout_bytes = stdout_handle.await.unwrap_or_default();
                    let stderr_bytes = stderr_handle.await.unwrap_or_default();
                    let stdout = truncate_output(&stdout_bytes);
                    let stderr = truncate_output(&stderr_bytes);

                    let status = if exit_status.success() {
                        TaskResultStatus::Success
                    } else {
                        TaskResultStatus::Failed
                    };

                    make_result(
                        task_id,
                        status,
                        Some(exit_code),
                        stdout,
                        stderr,
                        start.elapsed(),
                        if exit_status.success() {
                            None
                        } else {
                            Some(format!("exit code {exit_code}"))
                        },
                    )
                }
                Err(e) => {
                    make_result(
                        task_id,
                        TaskResultStatus::Failed,
                        None,
                        String::new(),
                        e.to_string(),
                        start.elapsed(),
                        Some(format!("failed to wait for shell: {e}")),
                    )
                }
            }
        }
    }
}

/// Truncate output to MAX_OUTPUT_SIZE and convert to a lossy UTF-8 string.
fn truncate_output(bytes: &[u8]) -> String {
    if bytes.len() <= MAX_OUTPUT_SIZE {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        let mut s = String::from_utf8_lossy(&bytes[..MAX_OUTPUT_SIZE]).into_owned();
        s.push_str("\n... [truncated]");
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use serde_json::json;
    use uuid::Uuid;
    use vasal_protocol::task::{ExecKind, Executor, Priority};

    fn make_exec(script: &str, timeout_ms: u64) -> ExecTask {
        ExecTask {
            id: Uuid::new_v4(),
            priority: Priority::Normal,
            tags: Default::default(),
            kind: ExecKind::Oneshot,
            executor: Executor::Shell,
            target: None,
            method: None,
            payload: json!({"script": script}),
            interval_ms: None,
            timeout_ms,
            credentials: vec![],
        }
    }

    #[tokio::test]
    async fn echo_hello() {
        let exec = make_exec("echo hello", 5000);
        let cancel = CancellationToken::new();
        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Success);
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout.trim(), "hello");
    }

    #[tokio::test]
    async fn exit_code_propagated() {
        let exec = make_exec("exit 42", 5000);
        let cancel = CancellationToken::new();
        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Failed);
        assert_eq!(result.exit_code, Some(42));
    }

    #[tokio::test]
    async fn timeout_kills_process() {
        let exec = make_exec("sleep 60", 100);
        let cancel = CancellationToken::new();
        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Timeout);
    }

    #[tokio::test]
    async fn cancellation() {
        let exec = make_exec("sleep 60", 60_000);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Cancel after a short delay.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Cancelled);
    }

    #[tokio::test]
    async fn credentials_as_env_vars() {
        let exec = make_exec("echo $DB_PASSWORD", 5000);
        let mut creds = HashMap::new();
        creds.insert("DB_PASSWORD".into(), "secret123".into());
        let cancel = CancellationToken::new();
        let result = execute(&exec, &creds, cancel).await;
        assert_eq!(result.status, TaskResultStatus::Success);
        assert_eq!(result.stdout.trim(), "secret123");
    }

    #[tokio::test]
    async fn missing_script_field() {
        let exec = ExecTask {
            id: Uuid::new_v4(),
            priority: Priority::Normal,
            tags: Default::default(),
            kind: ExecKind::Oneshot,
            executor: Executor::Shell,
            target: None,
            method: None,
            payload: json!({"command": "echo hi"}), // Wrong field name.
            interval_ms: None,
            timeout_ms: 5000,
            credentials: vec![],
        };
        let cancel = CancellationToken::new();
        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Failed);
        assert!(result.error.unwrap().contains("missing 'script'"));
    }

    #[tokio::test]
    async fn stderr_captured() {
        let exec = make_exec("echo error >&2", 5000);
        let cancel = CancellationToken::new();
        let result = execute(&exec, &HashMap::new(), cancel).await;
        assert_eq!(result.status, TaskResultStatus::Success);
        assert_eq!(result.stderr.trim(), "error");
    }
}
