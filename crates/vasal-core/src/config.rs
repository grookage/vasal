//! Agent configuration — TOML parsing, validation, and hot-reload (DD-18).
//!
//! The agent reads `/etc/vasal/config.toml` (or a path given via `--config`).
//! On `SIGHUP`, hot-reloadable fields are re-applied without restarting.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tracing::{info, warn};

// ── Top-level config ───────────────────────────────────────────────────────

/// Complete agent configuration, deserialized from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub agent: AgentConfig,
    pub transport: TransportConfig,
    pub heartbeat: HeartbeatConfig,
    pub audit: AuditConfig,
    pub auth: AuthConfig,
    pub shell: ShellConfig,
    pub units: UnitsConfig,
    /// OpenTelemetry configuration (requires the `otel` compile-time feature).
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

impl Config {
    /// Load and parse configuration from a TOML file.
    pub fn load(path: &Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::Error::Config(format!("failed to read {}: {e}", path.display())))?;
        let config: Self = toml::from_str(&content).map_err(|e| {
            crate::Error::Config(format!("failed to parse {}: {e}", path.display()))
        })?;
        Ok(config)
    }

    /// Extract the hot-reloadable subset of this configuration.
    pub fn runtime(&self) -> RuntimeConfig {
        RuntimeConfig {
            log_level: self.agent.log_level.clone(),
            max_concurrent: self.shell.max_concurrent,
            heartbeat_interval_sec: self.heartbeat.interval_sec,
            health_check_interval_sec: self.units.health_check_interval_sec,
            audit_batch_size: self.audit.batch_size,
            audit_flush_interval_sec: self.audit.flush_interval_sec,
        }
    }

    /// Validate configuration for common misconfigurations.
    ///
    /// Returns a list of warnings and errors. Errors indicate the agent
    /// will likely fail to start; warnings indicate sub-optimal but
    /// functional configurations.
    pub fn lint(&self) -> Vec<ConfigLint> {
        let mut results = Vec::new();

        // Auth disabled
        if self.auth.provider.is_empty() || self.auth.provider == "none" {
            results.push(ConfigLint {
                severity: LintSeverity::Warning,
                message: "auth.provider is disabled — the agent will run unauthenticated. \
                          Set auth.provider to an auth endpoint for production deployments."
                    .into(),
            });
        }

        // Transport config mismatch
        match self.transport.mode {
            TransportMode::Poll if self.transport.poll.is_none() => {
                results.push(ConfigLint {
                    severity: LintSeverity::Error,
                    message: "transport.mode = \"poll\" but [transport.poll] section is missing"
                        .into(),
                });
            }
            TransportMode::Grpc if self.transport.grpc.is_none() => {
                results.push(ConfigLint {
                    severity: LintSeverity::Error,
                    message: "transport.mode = \"grpc\" but [transport.grpc] section is missing"
                        .into(),
                });
            }
            _ => {}
        }

        // Zero concurrency
        if self.shell.max_concurrent == 0 {
            results.push(ConfigLint {
                severity: LintSeverity::Error,
                message: "shell.max_concurrent is 0 — no tasks can execute".into(),
            });
        }

        // Data dir existence
        if !self.agent.data_dir.exists() {
            results.push(ConfigLint {
                severity: LintSeverity::Warning,
                message: format!(
                    "agent.data_dir {:?} does not exist — it will be created on startup",
                    self.agent.data_dir,
                ),
            });
        }

        // Heartbeat endpoint validation
        if self.heartbeat.endpoint.is_empty() {
            results.push(ConfigLint {
                severity: LintSeverity::Warning,
                message: "heartbeat.endpoint is empty — heartbeats will fail".into(),
            });
        }

        results
    }
}

/// Configuration lint result.
#[derive(Debug, Clone)]
pub struct ConfigLint {
    /// Severity of the lint finding.
    pub severity: LintSeverity,
    /// Human-readable description of the issue.
    pub message: String,
}

/// Severity level for configuration lint findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LintSeverity {
    /// The agent will likely fail to start or operate correctly.
    Error,
    /// Sub-optimal but functional configuration.
    Warning,
}

// ── Runtime (hot-reloadable) config ────────────────────────────────────────

/// Subset of configuration that can be changed via `SIGHUP` without restart.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    pub log_level: String,
    pub max_concurrent: usize,
    pub heartbeat_interval_sec: u64,
    pub health_check_interval_sec: u64,
    pub audit_batch_size: usize,
    pub audit_flush_interval_sec: u64,
}

/// Diff two runtime configs and log what changed.
pub fn log_config_diff(old: &RuntimeConfig, new: &RuntimeConfig) {
    if old.log_level != new.log_level {
        info!(old = %old.log_level, new = %new.log_level, "log_level changed");
    }
    if old.max_concurrent != new.max_concurrent {
        info!(
            old = old.max_concurrent,
            new = new.max_concurrent,
            "max_concurrent changed"
        );
    }
    if old.heartbeat_interval_sec != new.heartbeat_interval_sec {
        info!(
            old = old.heartbeat_interval_sec,
            new = new.heartbeat_interval_sec,
            "heartbeat_interval_sec changed",
        );
    }
    if old.health_check_interval_sec != new.health_check_interval_sec {
        info!(
            old = old.health_check_interval_sec,
            new = new.health_check_interval_sec,
            "health_check_interval_sec changed",
        );
    }
    if old.audit_batch_size != new.audit_batch_size {
        info!(
            old = old.audit_batch_size,
            new = new.audit_batch_size,
            "audit_batch_size changed"
        );
    }
    if old.audit_flush_interval_sec != new.audit_flush_interval_sec {
        info!(
            old = old.audit_flush_interval_sec,
            new = new.audit_flush_interval_sec,
            "audit_flush_interval_sec changed",
        );
    }
}

/// Warn about fields that require a restart to take effect.
pub fn warn_restart_required(old: &Config, new: &Config) {
    if old.transport.mode != new.transport.mode {
        warn!("transport.mode changed — restart required");
    }
    if old.agent.data_dir != new.agent.data_dir {
        warn!("agent.data_dir changed — restart required");
    }
    if old.agent.socket_dir != new.agent.socket_dir {
        warn!("agent.socket_dir changed — restart required");
    }
    if old.auth.provider != new.auth.provider {
        warn!("auth.provider changed — restart required");
    }
}

// ── Section configs ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    /// Agent UUID, assigned after first registration.
    #[serde(default)]
    pub id: Option<uuid::Uuid>,
    /// Human-readable hostname.
    #[serde(default)]
    pub name: Option<String>,
    /// Directory for SQLite state, task journal, audit log.
    #[serde(default = "defaults::data_dir")]
    pub data_dir: PathBuf,
    /// Directory for sidecar Unix sockets.
    #[serde(default = "defaults::socket_dir")]
    pub socket_dir: PathBuf,
    /// Log level filter string.
    #[serde(default = "defaults::log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TransportConfig {
    #[serde(default)]
    pub mode: TransportMode,
    pub poll: Option<PollConfig>,
    pub grpc: Option<GrpcConfig>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    #[default]
    Poll,
    Grpc,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PollConfig {
    pub endpoint: String,
    #[serde(default = "defaults::poll_interval_sec")]
    pub interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct GrpcConfig {
    pub endpoint: String,
    #[serde(default = "defaults::reconnect_interval_sec")]
    pub reconnect_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatConfig {
    #[serde(default = "defaults::heartbeat_interval_sec")]
    pub interval_sec: u64,
    pub endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditConfig {
    pub endpoint: String,
    #[serde(default = "defaults::audit_batch_size")]
    pub batch_size: usize,
    #[serde(default = "defaults::audit_flush_interval_sec")]
    pub flush_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AuthConfig {
    pub provider: String,
    #[serde(default = "defaults::token_file")]
    pub token_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShellConfig {
    #[serde(default = "defaults::shell_timeout_ms")]
    pub default_timeout_ms: u64,
    #[serde(default = "defaults::max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default = "defaults::working_dir")]
    pub working_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UnitsConfig {
    #[serde(default = "defaults::artifact_cache_dir")]
    pub artifact_cache_dir: PathBuf,
    #[serde(default = "defaults::health_check_interval_sec")]
    pub health_check_interval_sec: u64,
}

/// OpenTelemetry export configuration.
///
/// Requires the `otel` compile-time feature to have any effect.  When the
/// feature is absent, the section is parsed but ignored (with a warning).
#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    /// Whether OTel export is enabled (default: `false`).
    #[serde(default)]
    pub enabled: bool,
    /// OTLP collector endpoint (HTTP/protobuf).
    #[serde(default = "defaults::otlp_endpoint")]
    pub otlp_endpoint: String,
    /// Service name reported to the collector.
    #[serde(default = "defaults::service_name")]
    pub service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: defaults::otlp_endpoint(),
            service_name: defaults::service_name(),
        }
    }
}

// ── Defaults ───────────────────────────────────────────────────────────────

mod defaults {
    use std::path::PathBuf;

    pub fn data_dir() -> PathBuf {
        PathBuf::from("/var/lib/vasal")
    }
    pub fn socket_dir() -> PathBuf {
        PathBuf::from("/run/vasal")
    }
    pub fn log_level() -> String {
        "info".into()
    }
    pub fn poll_interval_sec() -> u64 {
        10
    }
    pub fn reconnect_interval_sec() -> u64 {
        5
    }
    pub fn heartbeat_interval_sec() -> u64 {
        10
    }
    pub fn audit_batch_size() -> usize {
        50
    }
    pub fn audit_flush_interval_sec() -> u64 {
        5
    }
    pub fn token_file() -> PathBuf {
        PathBuf::from("/var/lib/vasal/token.json")
    }
    pub fn shell_timeout_ms() -> u64 {
        300_000
    }
    pub fn max_concurrent() -> usize {
        4
    }
    pub fn working_dir() -> PathBuf {
        PathBuf::from("/tmp/vasal")
    }
    pub fn artifact_cache_dir() -> PathBuf {
        PathBuf::from("/var/cache/vasal")
    }
    pub fn health_check_interval_sec() -> u64 {
        30
    }
    pub fn otlp_endpoint() -> String {
        "http://localhost:4318".into()
    }
    pub fn service_name() -> String {
        "vasal".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_CONFIG: &str = r#"
[agent]

[transport]
mode = "poll"

[transport.poll]
endpoint = "https://cp.internal/api/v1"

[heartbeat]
endpoint = "https://cp.internal/api/v1/heartbeat"

[audit]
endpoint = "https://cp.internal/api/v1/audit"

[auth]
provider = "https://auth.internal/v1/token"

[shell]

[units]
"#;

    #[test]
    fn parse_minimal_config() {
        let config: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(config.transport.mode, TransportMode::Poll);
        assert_eq!(config.shell.max_concurrent, 4);
        assert_eq!(config.shell.default_timeout_ms, 300_000);
        assert_eq!(config.agent.log_level, "info");
    }

    #[test]
    fn runtime_config_extraction() {
        let config: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        let rt = config.runtime();
        assert_eq!(rt.log_level, "info");
        assert_eq!(rt.max_concurrent, 4);
        assert_eq!(rt.heartbeat_interval_sec, 10);
    }

    #[test]
    fn transport_mode_default() {
        assert_eq!(TransportMode::default(), TransportMode::Poll);
    }

    #[test]
    fn config_lint_catches_disabled_auth() {
        let mut config: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        config.auth.provider = "none".into();
        let lints = config.lint();
        assert!(lints
            .iter()
            .any(|l| l.severity == LintSeverity::Warning && l.message.contains("unauthenticated")));
    }

    #[test]
    fn config_lint_catches_zero_concurrency() {
        let mut config: Config = toml::from_str(MINIMAL_CONFIG).unwrap();
        config.shell.max_concurrent = 0;
        let lints = config.lint();
        assert!(lints
            .iter()
            .any(|l| l.severity == LintSeverity::Error && l.message.contains("max_concurrent")));
    }
}
