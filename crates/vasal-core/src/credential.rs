//! Per-task credential resolution (eager and lazy modes).

use std::collections::HashMap;
use std::path::Path;

use tracing::{debug, warn};
use vasal_protocol::credential::{CredentialProvider, CredentialRef, ResolveMode};

use crate::task::sidecar as sidecar_client;

pub type ResolvedCredentials = HashMap<String, String>;

/// Resolve all eager credentials for a task, skipping lazy ones.
pub async fn resolve_eager(
    refs: &[CredentialRef],
    http_client: &reqwest::Client,
    socket_dir: &Path,
) -> crate::Result<ResolvedCredentials> {
    let mut creds = HashMap::new();

    for cred_ref in refs {
        if cred_ref.resolve == ResolveMode::Lazy {
            debug!(name = %cred_ref.name, "skipping lazy credential");
            continue;
        }

        let value = resolve_one(cred_ref, http_client, socket_dir).await?;
        creds.insert(cred_ref.name.clone(), value);
    }

    Ok(creds)
}

async fn resolve_one(
    cred_ref: &CredentialRef,
    http_client: &reqwest::Client,
    socket_dir: &Path,
) -> crate::Result<String> {
    match &cred_ref.provider {
        CredentialProvider::Http { endpoint } => {
            resolve_http(endpoint, cred_ref.params.as_ref(), http_client).await
        }
        CredentialProvider::Sidecar { endpoint, method } => {
            resolve_sidecar(endpoint, method, cred_ref.params.as_ref(), socket_dir).await
        }
    }
}

async fn resolve_http(
    endpoint: &str,
    params: Option<&serde_json::Value>,
    client: &reqwest::Client,
) -> crate::Result<String> {
    debug!(endpoint = %endpoint, "resolving credential via HTTP");

    let mut req = client.post(endpoint);
    if let Some(p) = params {
        req = req.json(p);
    }

    let resp = req.send().await?;

    if !resp.status().is_success() {
        return Err(crate::Error::Auth(format!(
            "credential provider returned HTTP {}",
            resp.status(),
        )));
    }

    let body = resp.text().await?;

    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
        if let Some(val) = parsed.get("value").and_then(|v| v.as_str()) {
            return Ok(val.to_owned());
        }
    }

    Ok(body)
}

async fn resolve_sidecar(
    sidecar_name: &str,
    method: &str,
    params: Option<&serde_json::Value>,
    socket_dir: &Path,
) -> crate::Result<String> {
    debug!(
        sidecar = %sidecar_name,
        method = %method,
        "resolving credential via sidecar",
    );

    let socket_path = socket_dir.join(format!("{sidecar_name}.sock"));
    let rpc_params = params.cloned().unwrap_or(serde_json::Value::Null);

    let response = sidecar_client::call_raw(&socket_path, method, Some(rpc_params)).await?;

    if let Some(result) = response.result {
        if let Some(val) = result.get("value").and_then(|v| v.as_str()) {
            return Ok(val.to_owned());
        }
        return Ok(result.to_string());
    }

    if let Some(err) = response.error {
        return Err(crate::Error::Auth(format!(
            "credential sidecar error: [{}] {}",
            err.code, err.message,
        )));
    }

    Err(crate::Error::Auth(
        "credential sidecar returned empty response".into(),
    ))
}

/// Collect lazy credential refs as JSON for forwarding to a sidecar.
pub fn lazy_credentials_as_json(refs: &[CredentialRef]) -> serde_json::Value {
    let lazy: Vec<&CredentialRef> = refs
        .iter()
        .filter(|c| c.resolve == ResolveMode::Lazy)
        .collect();
    if lazy.is_empty() {
        return serde_json::Value::Null;
    }
    serde_json::to_value(&lazy).unwrap_or_else(|e| {
        warn!(error = %e, "failed to serialize lazy credentials");
        serde_json::Value::Null
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vasal_protocol::credential::{CredentialProvider, CredentialRef, ResolveMode};

    #[test]
    fn filter_lazy_credentials() {
        let refs = [
            CredentialRef {
                name: "DB_PASS".into(),
                resolve: ResolveMode::Eager,
                provider: CredentialProvider::Http {
                    endpoint: "https://vault/v1/secret".into(),
                },
                params: None,
            },
            CredentialRef {
                name: "TLS_CERT".into(),
                resolve: ResolveMode::Lazy,
                provider: CredentialProvider::Sidecar {
                    endpoint: "vault-ctrl".into(),
                    method: "fetch_cert".into(),
                },
                params: None,
            },
        ];

        let lazy: Vec<&CredentialRef> = refs
            .iter()
            .filter(|c| c.resolve == ResolveMode::Lazy)
            .collect();
        assert_eq!(lazy.len(), 1);
        assert_eq!(lazy[0].name, "TLS_CERT");
    }

    #[test]
    fn lazy_credentials_json_empty() {
        let result = lazy_credentials_as_json(&[]);
        assert!(result.is_null());
    }
}
