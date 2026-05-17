//! OpenTelemetry OTLP trace export (feature-gated behind `otel`).
//!
//! When enabled, this module initialises an OTLP span exporter that ships
//! traces to a collector over HTTP/protobuf.  All existing `tracing`
//! instrumentation is automatically bridged via `tracing-opentelemetry` —
//! no code changes are required in other modules.
//!
//! # Usage
//!
//! ```toml
//! # config.toml
//! [telemetry]
//! enabled = true
//! otlp_endpoint = "http://localhost:4318"   # OTLP HTTP receiver
//! service_name  = "vasal"
//! ```
//!
//! Compile with `cargo build --features otel` to include OpenTelemetry support.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use opentelemetry_sdk::Resource;

/// Guard that flushes and shuts down the OTel pipeline on drop.
///
/// Must be held for the lifetime of the application.  Dropping it triggers
/// a final flush of pending spans to the collector.
pub struct TelemetryGuard {
    provider: SdkTracerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("warning: OpenTelemetry shutdown error: {e}");
        }
    }
}

/// Initialise the OTLP trace pipeline.
///
/// Returns a [`TelemetryGuard`] (keep alive for the application lifetime)
/// and a [`Tracer`] to pass to `tracing_opentelemetry::layer()`.
///
/// Uses HTTP/protobuf transport so the agent does **not** need a gRPC
/// dependency beyond what it already has — avoiding version conflicts
/// with the gRPC transport layer.
pub fn init(endpoint: &str, service_name: &str) -> crate::Result<(TelemetryGuard, Tracer)> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| crate::Error::Config(format!("OTel span exporter: {e}")))?;

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", service_name.to_owned()))
        .build();

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer("vasal");
    let guard = TelemetryGuard { provider };

    Ok((guard, tracer))
}
