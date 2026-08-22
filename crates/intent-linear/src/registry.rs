//! Engine selection from settings.
//!
//! [`LinearRegistry::from_settings`] resolves the Linear API key (inline,
//! secrets store, or `LINEAR_API_KEY`) and builds a [`LinearEngine`]. A missing key
//! yields a typed [`Error::NotConfigured`] so the daemon stays up (graceful,
//! mirroring `intent-sourcecontrol`).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::client::LinearClient;
use crate::engine::{LinearEngine, LinearEngineImpl};
use crate::error::{Error, Result};
use crate::token::{self, TokenSource};

/// Linear settings (`linear.*`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinearSettings {
    /// Inline API key (already resolved, e.g. read from the secrets store by
    /// the caller). When present and non-empty it takes precedence over
    /// [`Self::token_source`]. SECRET — never logged.
    #[serde(default)]
    pub token: Option<String>,
    /// How to resolve the key when [`Self::token`] is absent.
    #[serde(default)]
    pub token_source: TokenSource,
    /// Override for the Linear GraphQL endpoint (defaults to the public API).
    #[serde(default)]
    pub api_base_url: Option<String>,
}

/// Builds the active [`LinearEngine`] from settings.
pub struct LinearRegistry;

impl LinearRegistry {
    /// Construct the engine, or a typed [`Error::NotConfigured`] when no key is
    /// available. Async because the secrets-store lookup runs on the blocking pool
    /// with a bounded timeout (see [`token::resolve`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotConfigured`] when no API key is available; propagates client-construction failures.
    pub async fn from_settings(settings: &LinearSettings) -> Result<Arc<dyn LinearEngine>> {
        let key = resolve_token(settings).await?;
        let client = LinearClient::new(&key, settings.api_base_url.as_deref())?;
        Ok(Arc::new(LinearEngineImpl::new(client)))
    }
}

/// Resolve the API key from inline settings or the configured source.
async fn resolve_token(settings: &LinearSettings) -> Result<String> {
    if let Some(token) = settings.token.as_deref() {
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    token::resolve(&settings.token_source).await.ok_or_else(|| {
        Error::NotConfigured(
            "linear: no API key found (set linear.token or LINEAR_API_KEY)".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inline_token_builds_engine() {
        let settings = LinearSettings {
            token: Some("lin_api_test".to_string()),
            ..LinearSettings::default()
        };
        let engine = LinearRegistry::from_settings(&settings).await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn blank_inline_token_falls_through_to_not_configured() {
        let result = resolve_token(&LinearSettings {
            token: Some("   ".to_string()),
            token_source: TokenSource::Env,
            api_base_url: None,
        })
        .await;
        assert!(matches!(result, Err(Error::NotConfigured(_))));
    }
}
