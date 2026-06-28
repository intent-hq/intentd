//! Engine selection from settings.
//!
//! [`LinearRegistry::from_settings`] resolves the Linear API key (inline,
//! keychain, or `LINEAR_API_KEY`) and builds a [`LinearEngine`]. A missing key
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
    /// Inline API key (already resolved, e.g. read from the keychain by the
    /// caller). When present and non-empty it takes precedence over
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
    /// available.
    pub fn from_settings(settings: &LinearSettings) -> Result<Arc<dyn LinearEngine>> {
        let key = resolve_token(settings)?;
        let client = LinearClient::new(&key, settings.api_base_url.as_deref())?;
        Ok(Arc::new(LinearEngineImpl::new(client)))
    }
}

/// Resolve the API key from inline settings or the configured source.
fn resolve_token(settings: &LinearSettings) -> Result<String> {
    if let Some(token) = settings.token.as_deref() {
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    token::resolve(&settings.token_source).ok_or_else(|| {
        Error::NotConfigured(
            "linear: no API key found (set linear.token or LINEAR_API_KEY)".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_token_builds_engine() {
        let settings = LinearSettings {
            token: Some("lin_api_test".to_string()),
            ..LinearSettings::default()
        };
        let engine = LinearRegistry::from_settings(&settings);
        assert!(engine.is_ok());
    }

    #[test]
    fn missing_token_is_not_configured() {
        // `Explicit` reads the keychain only; no `intentd` entry exists in CI,
        // so resolution yields `None` regardless of ambient env vars.
        let settings = LinearSettings {
            token: None,
            token_source: TokenSource::Explicit,
            api_base_url: None,
        };
        let result = LinearRegistry::from_settings(&settings);
        assert!(matches!(result, Err(Error::NotConfigured(_))));
    }

    #[test]
    fn blank_inline_token_falls_through_to_not_configured() {
        let result = resolve_token(&LinearSettings {
            token: Some("   ".to_string()),
            token_source: TokenSource::Explicit,
            api_base_url: None,
        });
        assert!(matches!(result, Err(Error::NotConfigured(_))));
    }
}
