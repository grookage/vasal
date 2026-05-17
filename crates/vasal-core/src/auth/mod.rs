//! Authentication — bootstrap and token management (DD-12).
//!
//! The auth module handles:
//!
//! - **Bootstrap**: read the one-time key from `onetimeauth.toml`, present it
//!   to the auth provider, receive a token pair (access + refresh).
//! - **Ongoing**: persist tokens, auto-refresh access token before expiry,
//!   inject `Authorization: Bearer <token>` into all CP-bound HTTP requests.

pub mod token;

use std::path::Path;

use serde::Deserialize;
use tracing::{info, warn};

use token::TokenStore;

/// One-time bootstrap key, read from `/etc/vasal/onetimeauth.toml`.
#[derive(Debug, Deserialize)]
struct BootstrapConfig {
    bootstrap: BootstrapKey,
}

#[derive(Debug, Deserialize)]
struct BootstrapKey {
    key: String,
    #[allow(dead_code)]
    auth_endpoint: String,
}

/// Authentication manager providing token injection for HTTP requests.
#[derive(Clone)]
pub struct AuthManager {
    token_store: TokenStore,
}

impl AuthManager {
    /// Create an auth manager from a persisted token file.
    ///
    /// If a valid token file exists, loads it. Otherwise, attempts bootstrap
    /// from the one-time key file.
    pub async fn init(
        token_file: &Path,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<Self> {
        // Try loading existing tokens first.
        if let Some(store) = TokenStore::load(token_file) {
            info!("loaded existing auth tokens");
            return Ok(Self { token_store: store });
        }

        // Attempt bootstrap from one-time key.
        let bootstrap_path = token_file
            .parent()
            .unwrap_or(Path::new("/etc/vasal"))
            .join("onetimeauth.toml");

        if bootstrap_path.exists() {
            info!(path = %bootstrap_path.display(), "bootstrapping from one-time key");
            let store = Self::bootstrap(&bootstrap_path, auth_provider_url, http_client).await?;
            store.save(token_file)?;
            // Best-effort cleanup of one-time key file.
            if let Err(e) = std::fs::remove_file(&bootstrap_path) {
                warn!(error = %e, "failed to remove one-time key file");
            }
            return Ok(Self { token_store: store });
        }

        // No tokens and no bootstrap key — start unauthenticated.
        warn!("no auth tokens or bootstrap key found — running unauthenticated");
        Ok(Self {
            token_store: TokenStore::empty(),
        })
    }

    /// Bootstrap: exchange one-time key for a token pair.
    async fn bootstrap(
        bootstrap_path: &Path,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<TokenStore> {
        let content = std::fs::read_to_string(bootstrap_path).map_err(|e| {
            crate::Error::Auth(format!(
                "failed to read bootstrap config {}: {e}",
                bootstrap_path.display(),
            ))
        })?;
        let config: BootstrapConfig = toml::from_str(&content)
            .map_err(|e| crate::Error::Auth(format!("failed to parse bootstrap config: {e}",)))?;

        let resp = http_client
            .post(auth_provider_url)
            .json(&serde_json::json!({
                "grant_type": "one_time_key",
                "key": config.bootstrap.key,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(crate::Error::Auth(format!(
                "bootstrap auth returned HTTP {}",
                resp.status(),
            )));
        }

        let body: token::TokenResponse = resp.json().await?;
        info!("bootstrap successful — tokens received");

        Ok(TokenStore::from_response(body))
    }

    /// Apply authentication to an outgoing HTTP request builder.
    ///
    /// Injects the `Authorization: Bearer <token>` header if a valid access
    /// token is available.
    pub fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.token_store.access_token() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }

    /// Get the current access token, if available.
    pub fn access_token(&self) -> Option<String> {
        self.token_store.access_token()
    }

    /// Refresh the access token using the stored refresh token.
    pub async fn refresh(
        &self,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<()> {
        self.token_store
            .refresh(auth_provider_url, http_client)
            .await
    }
}
