//! Vasal agent entry point.
//!
//! Handles CLI argument parsing, logging setup, config loading, state store
//! initialization, signal handling (SIGTERM for graceful shutdown, SIGHUP for
//! config reload), and the main run loop that ties together transport, task
//! execution, heartbeat, audit forwarding, and unit health checking.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use vasal_protocol::heartbeat::ActiveTaskCounts;

use vasal_core::config::{self, Config, RuntimeConfig, TransportMode};
use vasal_core::state::StateStore;
use vasal_core::task::TaskManager;
use vasal_core::transport::grpc::GrpcTransport;
use vasal_core::transport::poll::PollTransport;
use vasal_core::transport::{ReceivedWork, Transport};
use vasal_core::{audit, heartbeat, unit};

/// Agent version, injected at compile time.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Holds OTel shutdown guard (if feature-enabled and configured).
struct TracingGuard {
    #[cfg(feature = "otel")]
    _otel: Option<vasal_core::telemetry::TelemetryGuard>,
}

/// Vasal — a lightweight, protocol-first, general-purpose host agent.
#[derive(Parser)]
#[command(name = "vasal", version, about)]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "/etc/vasal/config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let cfg = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
    };

    let _tracing = init_tracing(&cfg.agent.log_level, &cfg);
    info!(
        version = VERSION,
        config = %cli.config.display(),
        "vasal starting",
    );

    let state = match StateStore::open(&cfg.agent.data_dir) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to open state store");
            std::process::exit(1);
        }
    };

    let initial_runtime = cfg.runtime();
    let (runtime_tx, runtime_rx) = watch::channel(initial_runtime);

    let config_path = cli.config.clone();
    let full_config = Arc::new(std::sync::Mutex::new(cfg.clone()));

    let shutdown = CancellationToken::new();

    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("failed to register SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
        shutdown_clone.cancel();
    });

    {
        let full_config = Arc::clone(&full_config);
        let runtime_tx = runtime_tx.clone();
        let config_path = config_path.clone();
        tokio::spawn(async move {
            let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("failed to register SIGHUP handler");

            loop {
                sighup.recv().await;
                info!("received SIGHUP — reloading config");

                match Config::load(&config_path) {
                    Ok(new_cfg) => {
                        let mut old_guard = full_config.lock().unwrap();
                        let old_runtime = old_guard.runtime();
                        let new_runtime = new_cfg.runtime();

                        config::warn_restart_required(&old_guard, &new_cfg);

                        if old_runtime != new_runtime {
                            config::log_config_diff(&old_runtime, &new_runtime);
                            let _ = runtime_tx.send(new_runtime);
                            info!("runtime config updated");
                        } else {
                            info!("no hot-reloadable fields changed");
                        }

                        *old_guard = new_cfg;
                    }
                    Err(e) => {
                        warn!(error = %e, "config reload failed — keeping current config");
                    }
                }
            }
        });
    }

    if let Err(e) = run(cfg, state, runtime_rx, shutdown).await {
        error!(error = %e, "agent exited with error");
        std::process::exit(1);
    }

    info!("graceful shutdown complete");
}

/// Core agent run loop — transport, task execution, heartbeat, audit, health.
async fn run(
    cfg: Config,
    state: StateStore,
    runtime_rx: watch::Receiver<RuntimeConfig>,
    shutdown: CancellationToken,
) -> vasal_core::Result<()> {
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent(format!("vasal/{VERSION}"))
        .build()
        .expect("failed to build HTTP client");

    match vasal_core::auth::AuthManager::init(
        &cfg.auth.token_file,
        &cfg.auth.provider,
        &http_client,
    )
    .await
    {
        Ok(auth) => {
            if auth.access_token().is_some() {
                info!("authenticated with control plane");
            } else {
                warn!("running unauthenticated — some CP endpoints may reject requests");
            }
        }
        Err(e) => {
            warn!(error = %e, "auth initialization failed — continuing unauthenticated");
        }
    }

    let agent_id = cfg.agent.id.unwrap_or_else(|| {
        let id = Uuid::new_v4();
        warn!(agent_id = %id, "no agent.id configured — using random UUID (assign one in config)");
        id
    });
    info!(agent_id = %agent_id, "agent identity resolved");

    let (result_tx, mut result_rx) = mpsc::channel(256);

    let (counts_tx, counts_rx) = watch::channel(ActiveTaskCounts::default());

    let task_manager = TaskManager::new(
        state.clone(),
        http_client.clone(),
        cfg.agent.socket_dir.clone(),
        runtime_rx.clone(),
        counts_tx,
        Some(result_tx),
        shutdown.clone(),
    );

    let transport: Box<dyn Transport> = match cfg.transport.mode {
        TransportMode::Poll => {
            let poll_cfg = cfg.transport.poll.as_ref().ok_or_else(|| {
                vasal_core::Error::Config(
                    "transport.mode = \"poll\" but [transport.poll] section is missing".into(),
                )
            })?;
            info!(
                endpoint = %poll_cfg.endpoint,
                interval_sec = poll_cfg.interval_sec,
                "using poll transport",
            );
            Box::new(PollTransport::new(
                poll_cfg.endpoint.clone(),
                http_client.clone(),
                poll_cfg.interval_sec,
            ))
        }
        TransportMode::Grpc => {
            let grpc_cfg = cfg.transport.grpc.as_ref().ok_or_else(|| {
                vasal_core::Error::Config(
                    "transport.mode = \"grpc\" but [transport.grpc] section is missing".into(),
                )
            })?;
            info!(
                endpoint = %grpc_cfg.endpoint,
                "using gRPC transport",
            );
            Box::new(GrpcTransport::new(
                grpc_cfg.endpoint.clone(),
                agent_id.to_string(),
                VERSION.to_owned(),
            ))
        }
    };

    let hb_handle = tokio::spawn(heartbeat::run(
        agent_id,
        VERSION.to_owned(),
        cfg.heartbeat.endpoint.clone(),
        state.clone(),
        http_client.clone(),
        runtime_rx.clone(),
        counts_rx,
        shutdown.clone(),
    ));

    let audit_handle = tokio::spawn(audit::run_forwarder(
        state.clone(),
        cfg.audit.endpoint.clone(),
        http_client.clone(),
        runtime_rx.clone(),
        shutdown.clone(),
    ));

    let health_handle = tokio::spawn(unit::health::run(
        state.clone(),
        cfg.agent.socket_dir.clone(),
        runtime_rx.clone(),
        shutdown.clone(),
    ));

    audit::record(
        &state,
        audit::event::AGENT_STARTED,
        None,
        serde_json::json!({
            "agent_id": agent_id.to_string(),
            "version": VERSION,
            "transport": format!("{:?}", cfg.transport.mode),
        }),
    );

    info!("agent ready — entering main loop");

    loop {
        tokio::select! {
            biased;

            () = shutdown.cancelled() => {
                info!("shutdown signal received — exiting main loop");
                break;
            }

            Some(result) = result_rx.recv() => {
                debug!(task_id = %result.task_id, "forwarding result to CP");
                if let Err(e) = transport.send_result(&result).await {
                    warn!(
                        error = %e,
                        task_id = %result.task_id,
                        "failed to report result — result preserved in local journal",
                    );
                }
            }

            work = transport.recv_tasks() => {
                match work {
                    Ok(items) if items.is_empty() => {}
                    Ok(items) => {
                        debug!(count = items.len(), "received work from transport");
                        for item in items {
                            match item {
                                ReceivedWork::Single(task) => {
                                    let task_id = task.id();
                                    if let Err(e) = task_manager.submit(task).await {
                                        error!(error = %e, task_id = %task_id, "failed to submit task");
                                    }
                                }
                                ReceivedWork::Chain(chain) => {
                                    let chain_id = chain.id;
                                    if let Err(e) = task_manager.submit_chain(chain).await {
                                        error!(error = %e, chain_id = %chain_id, "failed to submit chain");
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "transport recv failed — will retry");
                    }
                }
            }
        }
    }

    let mut drained = 0u32;
    while let Ok(result) = result_rx.try_recv() {
        if let Err(e) = transport.send_result(&result).await {
            warn!(error = %e, task_id = %result.task_id, "failed to flush result during shutdown");
        }
        drained += 1;
    }
    if drained > 0 {
        info!(count = drained, "flushed remaining results during shutdown");
    }

    audit::record(
        &state,
        audit::event::AGENT_SHUTDOWN,
        None,
        serde_json::json!({
            "agent_id": agent_id.to_string(),
        }),
    );

    info!("waiting for background tasks to stop");
    let _ = tokio::join!(hb_handle, audit_handle, health_handle);

    Ok(())
}

/// Initialize tracing with optional OpenTelemetry export.
fn init_tracing(level: &str, cfg: &Config) -> TracingGuard {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| {
        eprintln!("warning: invalid log level {level:?}, falling back to \"info\"");
        EnvFilter::new("info")
    });

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    #[cfg(feature = "otel")]
    {
        let (guard, otel_layer) = if cfg.telemetry.enabled {
            match vasal_core::telemetry::init(
                &cfg.telemetry.otlp_endpoint,
                &cfg.telemetry.service_name,
            ) {
                Ok((guard, tracer)) => {
                    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
                    eprintln!(
                        "info: OpenTelemetry export enabled → {}",
                        cfg.telemetry.otlp_endpoint,
                    );
                    (Some(guard), Some(layer))
                }
                Err(e) => {
                    eprintln!("warning: failed to initialise OpenTelemetry: {e}");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();

        TracingGuard { _otel: guard }
    }

    #[cfg(not(feature = "otel"))]
    {
        if cfg.telemetry.enabled {
            eprintln!(
                "warning: telemetry.enabled = true but vasal was compiled without the `otel` feature \
                 — rebuild with `cargo build --features otel` to enable OpenTelemetry export"
            );
        }

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();

        TracingGuard {}
    }
}
