//! Canonical type definitions for the Vasal agent protocol.

pub mod credential;
pub mod error;
pub mod heartbeat;
pub mod jsonrpc;
pub mod sidecar;
pub mod task;
pub mod unit;

pub use error::ProtocolError;
