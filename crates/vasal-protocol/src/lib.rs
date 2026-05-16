//! # vasal-protocol
//!
//! Canonical type definitions for the Vasal agent protocol.
//!
//! This crate defines the shared contract between the Vasal agent, its control
//! plane, and any sidecar that speaks the Vasal IPC protocol. It contains no
//! runtime logic — only types, serialization, and validation.
//!
//! # Modules
//!
//! | Module | Purpose |
//! |---|---|
//! | [`task`] | Task dispatch types — exec, cancel, install, upgrade, remove, self-upgrade |
//! | [`sidecar`] | Sidecar IPC response types — submit, status, cancel, health |
//! | [`heartbeat`] | Heartbeat payload sent periodically to the control plane |
//! | [`unit`] | Managed unit definitions — sidecars and packages |
//! | [`credential`] | Per-task credential resolution descriptors |
//! | [`jsonrpc`] | JSON-RPC 2.0 wire format types |
//! | [`error`] | Protocol error codes and the [`ProtocolError`] type |

pub mod credential;
pub mod error;
pub mod heartbeat;
pub mod jsonrpc;
pub mod sidecar;
pub mod task;
pub mod unit;

pub use error::ProtocolError;
