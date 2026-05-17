//! Lightweight agent metrics — atomic counters and Prometheus textfile export.
//!
//! The agent tracks internal counters and gauges using lock-free atomics.
//! Metrics are included in heartbeat payloads and optionally written to a
//! Prometheus-compatible textfile for node_exporter scraping.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Agent-wide metrics singleton.
///
/// All fields are atomic — safe to read and increment from any thread
/// without locking. Clone is cheap (Arc internals via static).
pub struct Metrics {
    /// Total tasks received since agent start.
    pub tasks_received: AtomicU64,
    /// Total tasks completed successfully.
    pub tasks_succeeded: AtomicU64,
    /// Total tasks failed.
    pub tasks_failed: AtomicU64,
    /// Total tasks cancelled.
    pub tasks_cancelled: AtomicU64,
    /// Total tasks timed out.
    pub tasks_timed_out: AtomicU64,
    /// Total sidecar IPC calls made.
    pub sidecar_calls: AtomicU64,
    /// Total sidecar IPC call failures.
    pub sidecar_call_failures: AtomicU64,
    /// Total heartbeats sent.
    pub heartbeats_sent: AtomicU64,
    /// Total heartbeat send failures.
    pub heartbeat_failures: AtomicU64,
    /// Total audit events recorded.
    pub audit_events_recorded: AtomicU64,
    /// Total audit events forwarded to CP.
    pub audit_events_forwarded: AtomicU64,
    /// Total credential resolutions performed.
    pub credential_resolutions: AtomicU64,
    /// Total credential resolution failures.
    pub credential_failures: AtomicU64,
    /// Total config reloads via SIGHUP.
    pub config_reloads: AtomicU64,
    /// Current number of active tasks (gauge).
    pub active_tasks: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics instance with all counters at zero.
    pub const fn new() -> Self {
        Self {
            tasks_received: AtomicU64::new(0),
            tasks_succeeded: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            tasks_cancelled: AtomicU64::new(0),
            tasks_timed_out: AtomicU64::new(0),
            sidecar_calls: AtomicU64::new(0),
            sidecar_call_failures: AtomicU64::new(0),
            heartbeats_sent: AtomicU64::new(0),
            heartbeat_failures: AtomicU64::new(0),
            audit_events_recorded: AtomicU64::new(0),
            audit_events_forwarded: AtomicU64::new(0),
            credential_resolutions: AtomicU64::new(0),
            credential_failures: AtomicU64::new(0),
            config_reloads: AtomicU64::new(0),
            active_tasks: AtomicU64::new(0),
        }
    }

    /// Export metrics as a Prometheus-compatible textfile string.
    ///
    /// Format follows the [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        write_counter(
            &mut out,
            "vasal_tasks_received_total",
            "Total tasks received",
            self.tasks_received.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_tasks_succeeded_total",
            "Total tasks completed successfully",
            self.tasks_succeeded.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_tasks_failed_total",
            "Total tasks failed",
            self.tasks_failed.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_tasks_cancelled_total",
            "Total tasks cancelled",
            self.tasks_cancelled.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_tasks_timed_out_total",
            "Total tasks timed out",
            self.tasks_timed_out.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_sidecar_calls_total",
            "Total sidecar IPC calls",
            self.sidecar_calls.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_sidecar_call_failures_total",
            "Total sidecar IPC failures",
            self.sidecar_call_failures.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_heartbeats_sent_total",
            "Total heartbeats sent",
            self.heartbeats_sent.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_heartbeat_failures_total",
            "Total heartbeat failures",
            self.heartbeat_failures.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_audit_events_recorded_total",
            "Total audit events recorded",
            self.audit_events_recorded.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_audit_events_forwarded_total",
            "Total audit events forwarded",
            self.audit_events_forwarded.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_credential_resolutions_total",
            "Total credential resolutions",
            self.credential_resolutions.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_credential_failures_total",
            "Total credential resolution failures",
            self.credential_failures.load(Ordering::Relaxed),
        );
        write_counter(
            &mut out,
            "vasal_config_reloads_total",
            "Total config reloads via SIGHUP",
            self.config_reloads.load(Ordering::Relaxed),
        );
        write_gauge(
            &mut out,
            "vasal_active_tasks",
            "Number of currently active tasks",
            self.active_tasks.load(Ordering::Relaxed),
        );
        out
    }

    /// Write the Prometheus textfile to the given directory.
    ///
    /// Writes to `<dir>/vasal.prom` atomically (write tmp, rename).
    /// Intended for the Prometheus node_exporter textfile collector.
    pub fn write_textfile(&self, dir: &Path) -> std::io::Result<()> {
        let content = self.to_prometheus();
        let target = dir.join("vasal.prom");
        let tmp = dir.join("vasal.prom.tmp");
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, &target)?;
        Ok(())
    }

    /// Return a JSON summary of current metrics for inclusion in heartbeats.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "tasks_received": self.tasks_received.load(Ordering::Relaxed),
            "tasks_succeeded": self.tasks_succeeded.load(Ordering::Relaxed),
            "tasks_failed": self.tasks_failed.load(Ordering::Relaxed),
            "tasks_cancelled": self.tasks_cancelled.load(Ordering::Relaxed),
            "tasks_timed_out": self.tasks_timed_out.load(Ordering::Relaxed),
            "sidecar_calls": self.sidecar_calls.load(Ordering::Relaxed),
            "heartbeats_sent": self.heartbeats_sent.load(Ordering::Relaxed),
            "audit_events_forwarded": self.audit_events_forwarded.load(Ordering::Relaxed),
            "active_tasks": self.active_tasks.load(Ordering::Relaxed),
        })
    }
}

/// Global metrics instance.
pub static METRICS: Metrics = Metrics::new();

/// Write a Prometheus counter metric entry.
fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

/// Write a Prometheus gauge metric entry.
fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_increment_and_export() {
        let m = Metrics::new();
        m.tasks_received.fetch_add(5, Ordering::Relaxed);
        m.tasks_succeeded.fetch_add(3, Ordering::Relaxed);
        m.tasks_failed.fetch_add(2, Ordering::Relaxed);
        m.active_tasks.store(1, Ordering::Relaxed);

        let prom = m.to_prometheus();
        assert!(prom.contains("vasal_tasks_received_total 5"));
        assert!(prom.contains("vasal_tasks_succeeded_total 3"));
        assert!(prom.contains("vasal_tasks_failed_total 2"));
        assert!(prom.contains("vasal_active_tasks 1"));
        assert!(prom.contains("# TYPE vasal_tasks_received_total counter"));
        assert!(prom.contains("# TYPE vasal_active_tasks gauge"));
    }

    #[test]
    fn metrics_to_json() {
        let m = Metrics::new();
        m.tasks_received.fetch_add(10, Ordering::Relaxed);
        let json = m.to_json();
        assert_eq!(json["tasks_received"], 10);
    }

    #[test]
    fn metrics_textfile_write() {
        let dir = tempfile::tempdir().unwrap();
        let m = Metrics::new();
        m.tasks_received.fetch_add(1, Ordering::Relaxed);
        m.write_textfile(dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("vasal.prom")).unwrap();
        assert!(content.contains("vasal_tasks_received_total 1"));
    }
}
