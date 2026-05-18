//! Atomic counters and Prometheus textfile export.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    pub tasks_received: AtomicU64,
    pub tasks_succeeded: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_cancelled: AtomicU64,
    pub tasks_timed_out: AtomicU64,
    pub sidecar_calls: AtomicU64,
    pub sidecar_call_failures: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub heartbeat_failures: AtomicU64,
    pub audit_events_recorded: AtomicU64,
    pub audit_events_forwarded: AtomicU64,
    pub credential_resolutions: AtomicU64,
    pub credential_failures: AtomicU64,
    pub config_reloads: AtomicU64,
    pub active_tasks: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
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

    pub fn to_prometheus(&self) -> String {
        use std::fmt::Write;

        const COUNTERS: &[(&str, &str)] = &[
            ("vasal_tasks_received_total", "Total tasks received"),
            (
                "vasal_tasks_succeeded_total",
                "Total tasks completed successfully",
            ),
            ("vasal_tasks_failed_total", "Total tasks failed"),
            ("vasal_tasks_cancelled_total", "Total tasks cancelled"),
            ("vasal_tasks_timed_out_total", "Total tasks timed out"),
            ("vasal_sidecar_calls_total", "Total sidecar IPC calls"),
            (
                "vasal_sidecar_call_failures_total",
                "Total sidecar IPC failures",
            ),
            ("vasal_heartbeats_sent_total", "Total heartbeats sent"),
            ("vasal_heartbeat_failures_total", "Total heartbeat failures"),
            (
                "vasal_audit_events_recorded_total",
                "Total audit events recorded",
            ),
            (
                "vasal_audit_events_forwarded_total",
                "Total audit events forwarded",
            ),
            (
                "vasal_credential_resolutions_total",
                "Total credential resolutions",
            ),
            (
                "vasal_credential_failures_total",
                "Total credential resolution failures",
            ),
            (
                "vasal_config_reloads_total",
                "Total config reloads via SIGHUP",
            ),
        ];

        let counter_fields: &[&AtomicU64] = &[
            &self.tasks_received,
            &self.tasks_succeeded,
            &self.tasks_failed,
            &self.tasks_cancelled,
            &self.tasks_timed_out,
            &self.sidecar_calls,
            &self.sidecar_call_failures,
            &self.heartbeats_sent,
            &self.heartbeat_failures,
            &self.audit_events_recorded,
            &self.audit_events_forwarded,
            &self.credential_resolutions,
            &self.credential_failures,
            &self.config_reloads,
        ];

        let mut out = String::with_capacity(2048);

        for (i, (name, help)) in COUNTERS.iter().enumerate() {
            let value = counter_fields[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }

        let active = self.active_tasks.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "# HELP vasal_active_tasks Number of currently active tasks"
        );
        let _ = writeln!(out, "# TYPE vasal_active_tasks gauge");
        let _ = writeln!(out, "vasal_active_tasks {active}");

        out
    }

    /// Write Prometheus textfile to `<dir>/vasal.prom` atomically.
    pub fn write_textfile(&self, dir: &Path) -> std::io::Result<()> {
        let content = self.to_prometheus();
        let target = dir.join("vasal.prom");
        let tmp = dir.join("vasal.prom.tmp");
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, &target)?;
        Ok(())
    }

    /// Return a JSON summary for inclusion in heartbeats.
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

pub static METRICS: Metrics = Metrics::new();

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
