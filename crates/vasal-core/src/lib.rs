//! Vasal agent core library.
//!
//! This crate contains all agent logic: configuration, state management,
//! task execution, transport, unit lifecycle, authentication, and audit.

pub mod audit;
pub mod auth;
pub mod config;
pub mod credential;
pub mod heartbeat;
pub mod metrics;
pub mod self_upgrade;
pub mod state;
pub mod task;
#[cfg(feature = "otel")]
pub mod telemetry;
pub mod transport;
pub mod unit;

use thiserror::Error;

/// Crate-level error type.
#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration: {0}")]
    Config(String),

    #[error("state store: {0}")]
    State(#[from] rusqlite::Error),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("transport: {0}")]
    Transport(String),

    #[error("authentication: {0}")]
    Auth(String),

    #[error("protocol: {0}")]
    Protocol(#[from] vasal_protocol::ProtocolError),

    #[error("HTTP: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("task: {0}")]
    Task(String),

    #[error("unit management: {0}")]
    Unit(String),

    #[error("SHA-256 mismatch: expected {expected}, got {actual}")]
    Sha256Mismatch { expected: String, actual: String },

    #[error("join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;
