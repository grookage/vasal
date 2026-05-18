//! `echo-ctrl` — test sidecar that echoes back submitted payloads.

use async_trait::async_trait;
use tracing::info;
use vasal_protocol::sidecar::{HealthResponse, HealthStatus, SubmitResponse};
use vasal_protocol::ProtocolError;
use vasal_sidecar_sdk::{SidecarHandler, SidecarServer};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_SOCKET: &str = "/run/vasal/echo-ctrl.sock";

struct EchoHandler;

#[async_trait]
impl SidecarHandler for EchoHandler {
    fn name(&self) -> &str {
        "echo-ctrl"
    }

    async fn health(&self) -> HealthResponse {
        HealthResponse {
            status: HealthStatus::Ok,
            version: Some(VERSION.to_owned()),
            error: None,
            metadata: None,
        }
    }

    async fn submit(&self, params: serde_json::Value) -> Result<SubmitResponse, ProtocolError> {
        let stdout = serde_json::to_string_pretty(&params).unwrap_or_else(|_| params.to_string());

        Ok(SubmitResponse::Completed {
            stdout,
            stderr: String::new(),
            truncated: false,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SOCKET.to_owned());

    info!(version = VERSION, socket = %socket_path, "starting echo-ctrl");

    let server = SidecarServer::new(EchoHandler, &socket_path);

    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        }
    };

    server.run(shutdown).await?;
    Ok(())
}
