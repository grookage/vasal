//! Structured audit trail with local persistence and batched forwarding.

use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::RuntimeConfig;
use crate::state::{AuditRow, StateStore};

pub mod event {
    pub const TASK_RECEIVED: &str = "task.received";
    pub const TASK_STARTED: &str = "task.started";
    pub const TASK_COMPLETED: &str = "task.completed";
    pub const TASK_FAILED: &str = "task.failed";
    pub const TASK_CANCELLED: &str = "task.cancelled";
    pub const TASK_TIMEOUT: &str = "task.timeout";
    pub const CHAIN_STARTED: &str = "chain.started";
    pub const CHAIN_COMPLETED: &str = "chain.completed";
    pub const CHAIN_ROLLBACK: &str = "chain.rollback";
    pub const UNIT_INSTALLED: &str = "unit.installed";
    pub const UNIT_UPGRADED: &str = "unit.upgraded";
    pub const UNIT_REMOVED: &str = "unit.removed";
    pub const UNIT_HEALTH_CHANGED: &str = "unit.health_changed";
    pub const CREDENTIAL_FETCHED: &str = "credential.fetched";
    pub const SELF_UPGRADE_STARTED: &str = "self_upgrade.started";
    pub const SELF_UPGRADE_COMPLETED: &str = "self_upgrade.completed";
    pub const CONFIG_RELOADED: &str = "config.reloaded";
    pub const AGENT_STARTED: &str = "agent.started";
    pub const AGENT_SHUTDOWN: &str = "agent.shutdown";
}

pub fn record(
    store: &StateStore,
    event_type: &str,
    task_id: Option<&str>,
    detail: serde_json::Value,
) {
    let row = AuditRow {
        id: None,
        timestamp: crate::state::now_ms(),
        event_type: event_type.to_owned(),
        task_id: task_id.map(str::to_owned),
        detail_json: detail.to_string(),
    };
    if let Err(e) = store.append_audit(&row) {
        error!(event_type = %event_type, error = %e, "failed to record audit event");
    }
}

pub async fn run_forwarder(
    store: StateStore,
    endpoint: String,
    http_client: reqwest::Client,
    runtime_rx: watch::Receiver<RuntimeConfig>,
    shutdown: CancellationToken,
) {
    info!(endpoint = %endpoint, "audit forwarder started");

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        let rt = runtime_rx.borrow().clone();
        let flush_interval = Duration::from_secs(rt.audit_flush_interval_sec);
        let batch_size = rt.audit_batch_size;

        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                let _ = flush_once(&store, &endpoint, &http_client, batch_size).await;
                info!("audit forwarder stopped");
                return;
            }
            () = tokio::time::sleep(flush_interval) => {}
        }

        match flush_once(&store, &endpoint, &http_client, batch_size).await {
            Ok(count) => {
                if count > 0 {
                    debug!(count, "forwarded audit events");
                }
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                warn!(error = %e, backoff_sec = backoff.as_secs(), "audit forwarding failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

async fn flush_once(
    store: &StateStore,
    endpoint: &str,
    client: &reqwest::Client,
    batch_size: usize,
) -> crate::Result<usize> {
    let s = store.clone();
    let events = tokio::task::spawn_blocking(move || s.pending_audit_events(batch_size)).await??;

    if events.is_empty() {
        return Ok(0);
    }

    let count = events.len();
    let ids: Vec<i64> = events.iter().filter_map(|e| e.id).collect();

    let payload: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "timestamp": e.timestamp,
                "event_type": e.event_type,
                "task_id": e.task_id,
                "detail": serde_json::from_str::<serde_json::Value>(&e.detail_json)
                    .unwrap_or(serde_json::Value::Null),
            })
        })
        .collect();

    let resp = client.post(endpoint).json(&payload).send().await?;

    if !resp.status().is_success() {
        return Err(crate::Error::Transport(format!(
            "audit endpoint returned HTTP {}",
            resp.status(),
        )));
    }

    let s = store.clone();
    tokio::task::spawn_blocking(move || s.mark_forwarded(&ids)).await??;

    Ok(count)
}
