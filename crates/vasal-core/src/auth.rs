//! Authentication — bootstrap and token management.

use std::path::Path;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

// ─── Token types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub token_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTokens {
    access_token: String,
    refresh_token: Option<String>,
    expires_at: Option<u64>,
}

// ─── TokenStore ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct TokenStore {
    inner: Arc<RwLock<Option<PersistedTokens>>>,
}

impl TokenStore {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    pub fn from_response(resp: TokenResponse) -> Self {
        let expires_at = resp.expires_in.map(|secs| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + secs
        });

        Self {
            inner: Arc::new(RwLock::new(Some(PersistedTokens {
                access_token: resp.access_token,
                refresh_token: resp.refresh_token,
                expires_at,
            }))),
        }
    }

    pub fn load(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let tokens: PersistedTokens = serde_json::from_str(&content).ok()?;
        Some(Self {
            inner: Arc::new(RwLock::new(Some(tokens))),
        })
    }

    pub fn save(&self, path: &Path) -> crate::Result<()> {
        let guard = self.inner.read().unwrap();
        if let Some(tokens) = guard.as_ref() {
            let json = serde_json::to_string_pretty(tokens)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, json)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                if let Err(e) = std::fs::set_permissions(path, perms) {
                    tracing::warn!(error = %e, "failed to set token file permissions to 0600");
                }
            }
        }
        Ok(())
    }

    /// Get the current access token if available and not expired.
    pub fn access_token(&self) -> Option<String> {
        let guard = self.inner.read().unwrap();
        let tokens = guard.as_ref()?;

        if let Some(expires_at) = tokens.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now + 30 >= expires_at {
                return None;
            }
        }

        Some(tokens.access_token.clone())
    }

    pub async fn refresh(
        &self,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<()> {
        let refresh_token = {
            let guard = self.inner.read().unwrap();
            guard.as_ref().and_then(|t| t.refresh_token.clone())
        };

        let refresh_token =
            refresh_token.ok_or_else(|| crate::Error::Auth("no refresh token available".into()))?;

        let resp = http_client
            .post(auth_provider_url)
            .json(&serde_json::json!({
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(crate::Error::Auth(format!(
                "token refresh returned HTTP {}",
                resp.status(),
            )));
        }

        let body: TokenResponse = resp.json().await?;
        info!("access token refreshed");

        let expires_at = body.expires_in.map(|secs| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + secs
        });

        let mut guard = self.inner.write().unwrap();
        *guard = Some(PersistedTokens {
            access_token: body.access_token,
            refresh_token: body.refresh_token.or(Some(refresh_token)),
            expires_at,
        });

        Ok(())
    }
}

// ─── AuthManager ────────────────────────────────────────────────────────────

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

#[derive(Clone)]
pub struct AuthManager {
    token_store: TokenStore,
}

impl AuthManager {
    /// Load existing tokens or bootstrap from one-time key.
    pub async fn init(
        token_file: &Path,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<Self> {
        if let Some(store) = TokenStore::load(token_file) {
            info!("loaded existing auth tokens");
            return Ok(Self { token_store: store });
        }

        let bootstrap_path = token_file
            .parent()
            .unwrap_or(Path::new("/etc/vasal"))
            .join("onetimeauth.toml");

        if bootstrap_path.exists() {
            info!(path = %bootstrap_path.display(), "bootstrapping from one-time key");
            let store = Self::bootstrap(&bootstrap_path, auth_provider_url, http_client).await?;
            store.save(token_file)?;
            if let Err(e) = std::fs::remove_file(&bootstrap_path) {
                warn!(error = %e, "failed to remove one-time key file");
            }
            return Ok(Self { token_store: store });
        }

        warn!("no auth tokens or bootstrap key found — running unauthenticated");
        Ok(Self {
            token_store: TokenStore::empty(),
        })
    }

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

        let body: TokenResponse = resp.json().await?;
        info!("bootstrap successful — tokens received");

        Ok(TokenStore::from_response(body))
    }

    /// Inject `Authorization: Bearer` header if a valid token is available.
    pub fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.token_store.access_token() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }

    pub fn access_token(&self) -> Option<String> {
        self.token_store.access_token()
    }

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

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn empty_store_has_no_token() {
        let store = TokenStore::empty();
        assert!(store.access_token().is_none());
    }

    #[test]
    fn from_response_and_access() {
        let store = TokenStore::from_response(TokenResponse {
            access_token: "abc123".into(),
            refresh_token: Some("refresh456".into()),
            expires_in: Some(3600),
            token_type: Some("Bearer".into()),
        });
        assert_eq!(store.access_token().unwrap(), "abc123");
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("token.json");

        let store = TokenStore::from_response(TokenResponse {
            access_token: "tok".into(),
            refresh_token: Some("ref".into()),
            expires_in: Some(7200),
            token_type: None,
        });
        store.save(&path).unwrap();

        let loaded = TokenStore::load(&path).unwrap();
        assert_eq!(loaded.access_token().unwrap(), "tok");
    }

    #[test]
    fn expired_token_returns_none() {
        let store = TokenStore {
            inner: Arc::new(RwLock::new(Some(PersistedTokens {
                access_token: "expired".into(),
                refresh_token: None,
                expires_at: Some(0),
            }))),
        };
        assert!(store.access_token().is_none());
    }
}
