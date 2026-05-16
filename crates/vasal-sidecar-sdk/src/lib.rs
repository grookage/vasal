//! # vasal-sidecar-sdk
//!
//! A Rust SDK for building Vasal sidecars.
//!
//! This crate provides the runtime scaffolding for a sidecar process:
//!
//! - Unix domain socket listener with graceful shutdown
//! - 4-byte big-endian length-prefixed framing ([`codec`])
//! - JSON-RPC 2.0 request dispatch ([`server`])
//! - The [`SidecarHandler`] trait that sidecar authors implement ([`handler`])
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use vasal_protocol::sidecar::*;
//! use vasal_protocol::ProtocolError;
//! use vasal_sidecar_sdk::{SidecarHandler, SidecarServer};
//!
//! struct MySidecar;
//!
//! #[async_trait]
//! impl SidecarHandler for MySidecar {
//!     fn name(&self) -> &str { "my-sidecar" }
//!
//!     async fn health(&self) -> HealthResponse {
//!         HealthResponse {
//!             status: HealthStatus::Ok,
//!             version: Some(env!("CARGO_PKG_VERSION").into()),
//!             error: None,
//!             metadata: None,
//!         }
//!     }
//!
//!     async fn submit(
//!         &self,
//!         params: serde_json::Value,
//!     ) -> Result<SubmitResponse, ProtocolError> {
//!         Ok(SubmitResponse::Completed {
//!             stdout: format!("processed: {params}"),
//!             stderr: String::new(),
//!             truncated: false,
//!         })
//!     }
//! }
//! ```

pub mod codec;
pub mod handler;
pub mod server;

pub use handler::SidecarHandler;
pub use server::SidecarServer;
