//! Periodic heartbeat sender to the control plane.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;
use vasal_protocol::heartbeat::{ActiveTaskCounts, Heartbeat, UnitReport};
use vasal_protocol::sidecar::HealthStatus;
use vasal_protocol::unit::UnitKind;

use crate::config::RuntimeConfig;
use crate::state::StateStore;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    agent_id: Uuid,
    agent_version: String,
    endpoint: String,
    store: StateStore,
    http_client: reqwest::Client,
    runtime_rx: watch::Receiver<RuntimeConfig>,
    active_tasks_rx: watch::Receiver<ActiveTaskCounts>,
    shutdown: CancellationToken,
) {
    info!(endpoint = %endpoint, "heartbeat sender started");
    let started_at = Instant::now();

    loop {
        let interval_sec = runtime_rx.borrow().heartbeat_interval_sec;
        let interval = Duration::from_secs(interval_sec);

        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("heartbeat sender stopped");
                return;
            }
            () = tokio::time::sleep(interval) => {}
        }

        let uptime_sec = started_at.elapsed().as_secs();
        let active_tasks = *active_tasks_rx.borrow();

        let s = store.clone();
        let units = match tokio::task::spawn_blocking(move || s.list_units()).await {
            Ok(Ok(rows)) => rows
                .into_iter()
                .map(|r| UnitReport {
                    name: r.name,
                    kind: match r.kind.as_str() {
                        "sidecar" => UnitKind::Sidecar,
                        _ => UnitKind::Package,
                    },
                    version: r.version,
                    state: r.state,
                    health: r.health.as_deref().map(|h| match h {
                        "ok" => HealthStatus::Ok,
                        "degraded" => HealthStatus::Degraded,
                        _ => HealthStatus::Unhealthy,
                    }),
                    pid: r.pid,
                    health_error: r.health_error,
                })
                .collect(),
            Ok(Err(e)) => {
                warn!(error = %e, "failed to read units for heartbeat");
                vec![]
            }
            Err(e) => {
                warn!(error = %e, "spawn_blocking failed for heartbeat");
                vec![]
            }
        };

        let hb = Heartbeat {
            agent_id,
            agent_version: agent_version.clone(),
            uptime_sec,
            timestamp: crate::state::now_ms() as u64,
            units,
            active_tasks,
        };

        match http_client.post(&endpoint).json(&hb).send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!("heartbeat sent");
            }
            Ok(resp) => {
                warn!(status = %resp.status(), "heartbeat rejected by CP");
            }
            Err(e) => {
                warn!(error = %e, "heartbeat failed");
            }
        }
    }
}
