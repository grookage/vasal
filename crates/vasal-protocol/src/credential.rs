//! Per-task credential resolution types.
//!
//! Credentials are never stored in the agent (DD-06). The control plane
//! specifies, per task, *how* and *where* to resolve each credential:
//!
//! - **Eager**: the agent fetches the credential before execution and injects
//!   it into the execution context (e.g., as an environment variable).
//! - **Lazy**: the agent forwards the [`CredentialRef`] to the sidecar, which
//!   fetches the credential itself (useful when the sidecar has direct network
//!   access to the credential provider).

use serde::{Deserialize, Serialize};

// ── CredentialRef ──────────────────────────────────────────────────────────

/// A reference to a credential that must be resolved for task execution.
///
/// The control plane includes zero or more of these in each task. The agent
/// resolves them according to [`resolve`](CredentialRef::resolve) mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CredentialRef {
    /// Logical name — injected as a key into the execution context.
    ///
    /// For shell tasks, this becomes an environment variable name.
    /// For sidecar tasks with eager resolution, it's included in the
    /// forwarded params.
    pub name: String,
    /// When to resolve this credential relative to task execution.
    pub resolve: ResolveMode,
    /// Where to fetch the credential from.
    pub provider: CredentialProvider,
    /// Additional parameters for the credential provider (e.g., secret path,
    /// key name). Provider-specific and opaque to the agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

// ── ResolveMode ────────────────────────────────────────────────────────────

/// When the credential is resolved relative to task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveMode {
    /// Agent fetches the credential *before* execution begins and injects
    /// it into the execution context.
    Eager,
    /// Agent passes the [`CredentialRef`] through to the sidecar, which
    /// fetches the credential itself during execution.
    Lazy,
}

// ── CredentialProvider ─────────────────────────────────────────────────────

/// Source from which to fetch a credential.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CredentialProvider {
    /// Fetch via HTTP call to the given endpoint.
    Http {
        /// URL of the credential endpoint (e.g., a Vault HTTP API).
        endpoint: String,
    },
    /// Fetch via IPC call to a credential-provider sidecar.
    Sidecar {
        /// Name of the credential-provider sidecar (e.g., `"vault-ctrl"`).
        endpoint: String,
        /// JSON-RPC method to invoke on the sidecar.
        method: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eager_http_roundtrip() {
        let cred = CredentialRef {
            name: "DB_PASSWORD".into(),
            resolve: ResolveMode::Eager,
            provider: CredentialProvider::Http {
                endpoint: "https://vault.internal/v1/secret/db".into(),
            },
            params: Some(json!({"key": "password"})),
        };
        let json_str = serde_json::to_string(&cred).unwrap();
        let parsed: CredentialRef = serde_json::from_str(&json_str).unwrap();
        assert_eq!(cred, parsed);
    }

    #[test]
    fn lazy_sidecar_roundtrip() {
        let cred = CredentialRef {
            name: "TLS_CERT".into(),
            resolve: ResolveMode::Lazy,
            provider: CredentialProvider::Sidecar {
                endpoint: "vault-ctrl".into(),
                method: "fetch_cert".into(),
            },
            params: None,
        };
        let json_str = serde_json::to_string(&cred).unwrap();
        let parsed: CredentialRef = serde_json::from_str(&json_str).unwrap();
        assert_eq!(cred, parsed);
    }

    #[test]
    fn provider_type_discriminator() {
        let json = r#"{"type":"http","endpoint":"https://vault.internal"}"#;
        let provider: CredentialProvider = serde_json::from_str(json).unwrap();
        assert!(matches!(provider, CredentialProvider::Http { .. }));

        let json = r#"{"type":"sidecar","endpoint":"vault-ctrl","method":"get"}"#;
        let provider: CredentialProvider = serde_json::from_str(json).unwrap();
        assert!(matches!(provider, CredentialProvider::Sidecar { .. }));
    }
}
