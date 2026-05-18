//! OpenTelemetry OTLP trace export (feature-gated behind `otel`).
//!
//! ```toml
//! [telemetry]
//! enabled = true
//! otlp_endpoint = "http://localhost:4318"
//! service_name  = "vasal"
//! ```

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use opentelemetry_sdk::Resource;

/// Guard that flushes and shuts down the OTel pipeline on drop.
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

/// Initialise the OTLP trace pipeline. Keep the returned guard alive for the application lifetime.
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
