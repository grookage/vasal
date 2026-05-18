//! `ebpf-observer` — kernel-level observation sidecar using procfs.
//!
//! Probes: `tcp_retransmit`, `blk_io_latency`, `oom_kill`, `connection_rate`.
//! On non-Linux platforms, runs as a stub reporting "unsupported platform".

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
            if std::path::Path::new("/proc/vmstat").exists() {
                (HealthStatus::Ok, None)
            } else {
                (
                    HealthStatus::Degraded,
                    Some("/proc/vmstat not accessible — metrics may be incomplete".into()),
                )
            }
        } else {
            (
                HealthStatus::Degraded,
                Some("procfs metrics require Linux — running as stub".into()),
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

    async fn submit(&self, params: serde_json::Value) -> Result<SubmitResponse, ProtocolError> {
        let p: ProbeParams = serde_json::from_value(params)
            .map_err(|e| ProtocolError::invalid_params(e.to_string()))?;

        match p.action.as_str() {
            "snapshot" => handle_snapshot(&p.probes),
            "attach" => handle_attach(&p.probes),
            other => Err(ProtocolError::invalid_params(format!(
                "unknown action: {other} (expected 'snapshot' or 'attach')",
            ))),
        }
    }
}

fn handle_snapshot(probes: &[String]) -> Result<SubmitResponse, ProtocolError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = probes;
        Ok(SubmitResponse::Completed {
            stdout: serde_json::json!({
                "status": "stub",
                "message": "procfs metrics not available on this platform",
                "platform": std::env::consts::OS,
            })
            .to_string(),
            stderr: String::new(),
            truncated: false,
        })
    }

    #[cfg(target_os = "linux")]
    {
        let all_probes = probes.is_empty();
        let mut metrics = serde_json::Map::new();

        if all_probes || probes.iter().any(|p| p == "tcp_retransmit") {
            metrics.insert("tcp_retransmit".into(), read_tcp_retransmits());
        }
        if all_probes || probes.iter().any(|p| p == "blk_io_latency") {
            metrics.insert("blk_io_latency".into(), read_diskstats());
        }
        if all_probes || probes.iter().any(|p| p == "oom_kill") {
            metrics.insert("oom_kill".into(), read_oom_kills());
        }
        if all_probes || probes.iter().any(|p| p == "connection_rate") {
            metrics.insert("connection_rate".into(), read_tcp_connections());
        }

        Ok(SubmitResponse::Completed {
            stdout: serde_json::Value::Object(metrics).to_string(),
            stderr: String::new(),
            truncated: false,
        })
    }
}

fn handle_attach(probes: &[String]) -> Result<SubmitResponse, ProtocolError> {
    if !cfg!(target_os = "linux") {
        return Err(ProtocolError::new(
            -32603,
            "probe attachment requires Linux",
        ));
    }

    Ok(SubmitResponse::Completed {
        stdout: serde_json::json!({
            "status": "attached",
            "probes": probes,
            "message": "procfs-based probes are always active — snapshot to read values",
        })
        .to_string(),
        stderr: String::new(),
        truncated: false,
    })
}

#[cfg(target_os = "linux")]
fn read_tcp_retransmits() -> serde_json::Value {
    match std::fs::read_to_string("/proc/net/snmp") {
        Ok(content) => {
            let mut header_fields = Vec::new();
            let mut value_fields = Vec::new();
            for line in content.lines() {
                if line.starts_with("Tcp:") {
                    if header_fields.is_empty() {
                        header_fields = line.split_whitespace().collect();
                    } else {
                        value_fields = line.split_whitespace().collect();
                    }
                }
            }
            let retrans_idx = header_fields.iter().position(|&f| f == "RetransSegs");
            let retrans = retrans_idx
                .and_then(|i| value_fields.get(i))
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            let out_segs_idx = header_fields.iter().position(|&f| f == "OutSegs");
            let out_segs = out_segs_idx
                .and_then(|i| value_fields.get(i))
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);

            serde_json::json!({
                "retransmit_segments": retrans,
                "out_segments": out_segs,
                "retransmit_rate": if out_segs > 0 {
                    retrans as f64 / out_segs as f64
                } else {
                    0.0
                },
            })
        }
        Err(e) => serde_json::json!({"error": e.to_string()}),
    }
}

#[cfg(target_os = "linux")]
fn read_diskstats() -> serde_json::Value {
    match std::fs::read_to_string("/proc/diskstats") {
        Ok(content) => {
            let mut disks = serde_json::Map::new();
            for line in content.lines() {
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() < 14 {
                    continue;
                }
                let name = fields[2];
                if name.starts_with("loop") || name.starts_with("ram") {
                    continue;
                }
                let reads: u64 = fields[3].parse().unwrap_or(0);
                let read_ms: u64 = fields[6].parse().unwrap_or(0);
                let writes: u64 = fields[7].parse().unwrap_or(0);
                let write_ms: u64 = fields[10].parse().unwrap_or(0);
                let io_in_progress: u64 = fields[11].parse().unwrap_or(0);
                let io_ms: u64 = fields[12].parse().unwrap_or(0);

                disks.insert(
                    name.to_string(),
                    serde_json::json!({
                        "reads": reads,
                        "read_ms": read_ms,
                        "writes": writes,
                        "write_ms": write_ms,
                        "io_in_progress": io_in_progress,
                        "io_ms": io_ms,
                        "avg_read_latency_ms": if reads > 0 { read_ms as f64 / reads as f64 } else { 0.0 },
                        "avg_write_latency_ms": if writes > 0 { write_ms as f64 / writes as f64 } else { 0.0 },
                    }),
                );
            }
            serde_json::Value::Object(disks)
        }
        Err(e) => serde_json::json!({"error": e.to_string()}),
    }
}

#[cfg(target_os = "linux")]
fn read_oom_kills() -> serde_json::Value {
    match std::fs::read_to_string("/proc/vmstat") {
        Ok(content) => {
            let mut oom_kill = 0u64;
            let mut oom_kill_found = false;
            let mut pgmajfault = 0u64;
            for line in content.lines() {
                if let Some(val) = line.strip_prefix("oom_kill ") {
                    oom_kill = val.trim().parse().unwrap_or(0);
                    oom_kill_found = true;
                } else if let Some(val) = line.strip_prefix("pgmajfault ") {
                    pgmajfault = val.trim().parse().unwrap_or(0);
                }
            }
            serde_json::json!({
                "oom_kill_count": oom_kill,
                "oom_kill_available": oom_kill_found,
                "major_page_faults": pgmajfault,
            })
        }
        Err(e) => serde_json::json!({"error": e.to_string()}),
    }
}

#[cfg(target_os = "linux")]
fn read_tcp_connections() -> serde_json::Value {
    match std::fs::read_to_string("/proc/net/tcp") {
        Ok(content) => {
            let mut established = 0u64;
            let mut listen = 0u64;
            let mut time_wait = 0u64;
            let mut close_wait = 0u64;
            let mut total = 0u64;

            for line in content.lines().skip(1) {
                let fields: Vec<&str> = line.split_whitespace().collect();
                if fields.len() < 4 {
                    continue;
                }
                total += 1;
                match fields[3] {
                    "01" => established += 1,
                    "0A" => listen += 1,
                    "06" => time_wait += 1,
                    "08" => close_wait += 1,
                    _ => {}
                }
            }

            if let Ok(content6) = std::fs::read_to_string("/proc/net/tcp6") {
                for line in content6.lines().skip(1) {
                    let fields: Vec<&str> = line.split_whitespace().collect();
                    if fields.len() < 4 {
                        continue;
                    }
                    total += 1;
                    match fields[3] {
                        "01" => established += 1,
                        "0A" => listen += 1,
                        "06" => time_wait += 1,
                        "08" => close_wait += 1,
                        _ => {}
                    }
                }
            }

            serde_json::json!({
                "total": total,
                "established": established,
                "listen": listen,
                "time_wait": time_wait,
                "close_wait": close_wait,
            })
        }
        Err(e) => serde_json::json!({"error": e.to_string()}),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().with_target(true).init();

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
