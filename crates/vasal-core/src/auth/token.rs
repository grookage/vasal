//! Token persistence and refresh logic.
//!
//! The token store holds an access token and a refresh token. The access token
//! is short-lived; the refresh token is long-lived and used to obtain new
//! access tokens.

use std::path::Path;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::info;

/// Response from the auth provider's token endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Token lifetime in seconds.
    #[serde(default)]
    pub expires_in: Option<u64>,
    /// Token type (e.g., "Bearer").
    #[serde(default)]
    pub token_type: Option<String>,
}

/// Persisted token pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTokens {
    access_token: String,
    refresh_token: Option<String>,
    /// Unix epoch in seconds when the access token expires.
    expires_at: Option<u64>,
}

/// Thread-safe token store with interior mutability.
#[derive(Clone)]
pub struct TokenStore {
    inner: Arc<RwLock<Option<PersistedTokens>>>,
}

impl TokenStore {
    /// Create an empty token store (no authentication).
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a token store from an auth provider response.
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

    /// Load tokens from a file.
    pub fn load(path: &Path) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        let tokens: PersistedTokens = serde_json::from_str(&content).ok()?;
        Some(Self {
            inner: Arc::new(RwLock::new(Some(tokens))),
        })
    }

    /// Persist tokens to a file.
    pub fn save(&self, path: &Path) -> crate::Result<()> {
        let guard = self.inner.read().unwrap();
        if let Some(tokens) = guard.as_ref() {
            let json = serde_json::to_string_pretty(tokens)?;
            // Ensure parent directory exists.
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, json)?;
        }
        Ok(())
    }

    /// Get the current access token, if available and not obviously expired.
    pub fn access_token(&self) -> Option<String> {
        let guard = self.inner.read().unwrap();
        let tokens = guard.as_ref()?;

        // Check rough expiry (with 30s buffer).
        if let Some(expires_at) = tokens.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if now + 30 >= expires_at {
                return None; // Expired or about to expire.
            }
        }

        Some(tokens.access_token.clone())
    }

    /// Refresh the access token using the stored refresh token.
    pub async fn refresh(
        &self,
        auth_provider_url: &str,
        http_client: &reqwest::Client,
    ) -> crate::Result<()> {
        let refresh_token = {
            let guard = self.inner.read().unwrap();
            guard
                .as_ref()
                .and_then(|t| t.refresh_token.clone())
        };

        let refresh_token = refresh_token.ok_or_else(|| {
            crate::Error::Auth("no refresh token available".into())
        })?;

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
                expires_at: Some(0), // Expired in 1970.
            }))),
        };
        assert!(store.access_token().is_none());
    }
}
