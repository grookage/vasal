//! ebpf-observer — reference sidecar for kernel-level observation (DD-16).
//!
//! This sidecar uses eBPF (via the `aya` crate on Linux) to load programs
//! that attach to kernel tracepoints, kprobes, and XDP hooks. It exposes
//! kernel-level metrics through the standard sidecar protocol.
//!
//! # Supported Probes
//!
//! | Probe | Hook | Detects |
//! |---|---|---|
//! | tcp_retransmit | kprobe | Network issues |
//! | blk_io_latency | tracepoint | Storage degradation |
//! | oom_kill | tracepoint | OOM events |
//! | connection_rate | kprobe/XDP | DDoS or thundering herd |
//!
//! # Platform Requirements
//!
//! - Linux kernel >= 5.8
//! - `CAP_BPF` + `CAP_PERFMON` capabilities
//! - On non-Linux platforms, this sidecar runs as a stub that reports
//!   "unsupported platform" for all probes.

use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;
use vasal_protocol::sidecar::{HealthResponse, HealthStatus, SubmitResponse};
use vasal_protocol::ProtocolError;
use vasal_sidecar_sdk::{SidecarHandler, SidecarServer};

struct EbpfObserver;

#[derive(Debug, Deserialize)]
struct ProbeParams {
    action: String,
    #[serde(default)]
    probes: Vec<String>,
}

#[async_trait]
impl SidecarHandler for EbpfObserver {
    fn name(&self) -> &str {
        "ebpf-observer"
    }

    async fn health(&self) -> HealthResponse {
        let (status, error) = if cfg!(target_os = "linux") {
            (HealthStatus::Ok, None)
        } else {
            (
                HealthStatus::Degraded,
                Some("eBPF requires Linux — running as stub".into()),
            )
        };

        HealthResponse {
            status,
            version: Some(env!("CARGO_PKG_VERSION").into()),
            error,
            metadata: Some(serde_json::json!({
                "platform": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
            })),
        }
    }

    async fn submit(
        &self,
        params: serde_json::Value,
    ) -> Result<SubmitResponse, ProtocolError> {
        let p: ProbeParams = serde_json::from_value(params)
            .map_err(|e| ProtocolError::invalid_params(e.to_string()))?;

        match p.action.as_str() {
            "snapshot" => {
                // On non-Linux: return stub data.
                #[cfg(not(target_os = "linux"))]
                {
                    return Ok(SubmitResponse::Completed {
                        stdout: serde_json::json!({
                            "status": "stub",
                            "message": "eBPF not available on this platform",
                            "requested_probes": p.probes,
                        })
                        .to_string(),
                        stderr: String::new(),
                        truncated: false,
                    });
                }

                // On Linux: read eBPF map values.
                #[cfg(target_os = "linux")]
                {
                    // TODO: Implement actual eBPF map reads via aya.
                    Ok(SubmitResponse::Completed {
                        stdout: serde_json::json!({
                            "status": "ok",
                            "probes": p.probes,
                            "message": "eBPF probe snapshot not yet implemented",
                        })
                        .to_string(),
                        stderr: String::new(),
                        truncated: false,
                    })
                }
            }
            "attach" => {
                if !cfg!(target_os = "linux") {
                    return Err(ProtocolError::new(
                        -32603,
                        "eBPF probe attachment requires Linux",
                    ));
                }

                // TODO: Attach probes (async, return Accepted).
                Ok(SubmitResponse::Completed {
                    stdout: serde_json::json!({
                        "status": "ok",
                        "message": "eBPF probe attachment not yet implemented",
                    })
                    .to_string(),
                    stderr: String::new(),
                    truncated: false,
                })
            }
            other => Err(ProtocolError::invalid_params(format!(
                "unknown action: {other} (expected 'snapshot' or 'attach')",
            ))),
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .init();

    let socket_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "/run/vasal/ebpf-observer.sock".into());

    info!(socket = %socket_path, "ebpf-observer starting");

    let server = SidecarServer::new(EbpfObserver, &socket_path);
    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
    };
    server.run(shutdown).await
}
