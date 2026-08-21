//! Engine selection from settings.
//!
//! [`SentryRegistry::from_settings`] resolves the Sentry credential pair
//! (inline org/token, secrets store, or env) and builds a [`SentryEngine`]. A
//! missing pair yields a typed [`Error::NotConfigured`] so the daemon stays
//! up (graceful, mirroring `intent-linear`).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::client::SentryClient;
use crate::engine::{SentryEngine, SentryEngineImpl};
use crate::error::{Error, Result};
use crate::token::{self, Credentials, TokenSource};

/// Sentry settings (`sentry.*`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentrySettings {
    /// Inline organization slug (already resolved). When present together
    /// with [`Self::token`] it takes precedence over [`Self::token_source`].
    #[serde(default)]
    pub organization: Option<String>,
    /// Inline API token (already resolved, e.g. read from the secrets store
    /// by the caller). SECRET — never logged.
    #[serde(default)]
    pub token: Option<String>,
    /// How to resolve the credential pair when inline values are absent.
    #[serde(default)]
    pub token_source: TokenSource,
    /// Override for the Sentry REST endpoint (defaults to the public API).
    #[serde(default)]
    pub api_base_url: Option<String>,
}

/// Builds the active [`SentryEngine`] from settings.
pub struct SentryRegistry;

impl SentryRegistry {
    /// Construct the engine, or a typed [`Error::NotConfigured`] when no
    /// credential pair is available. Async because the secrets-store lookup
    /// runs on the blocking pool with a bounded timeout (see [`token::resolve`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotConfigured`] when no credential pair is available; propagates client-construction failures.
    pub async fn from_settings(settings: &SentrySettings) -> Result<Arc<dyn SentryEngine>> {
        let creds = resolve_credentials(settings).await?;
        let client = SentryClient::new(
            &creds.token,
            &creds.organization,
            settings.api_base_url.as_deref(),
        )?;
        Ok(Arc::new(SentryEngineImpl::new(client)))
    }
}

/// Resolve credentials from inline settings or the configured source.
async fn resolve_credentials(settings: &SentrySettings) -> Result<Credentials> {
    let inline_token = settings
        .token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let inline_org = settings
        .organization
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let (Some(token), Some(organization)) = (inline_token, inline_org) {
        return Ok(Credentials {
            token: token.to_string(),
            organization: organization.to_string(),
        });
    }
    token::resolve(&settings.token_source).await.ok_or_else(|| {
        Error::NotConfigured(
            "sentry: no credentials found (set sentry.organization + sentry.token, or \
             SENTRY_ORG + SENTRY_API_TOKEN)"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inline_pair_builds_engine() {
        let settings = SentrySettings {
            organization: Some("acme".into()),
            token: Some("sntrys_test".into()),
            ..SentrySettings::default()
        };
        let engine = SentryRegistry::from_settings(&settings).await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn inline_half_only_falls_through_to_not_configured() {
        // Use the `Env` source so the tests do not touch the secrets store; a
        // partial inline pair must still yield the same `NotConfigured`
        // outcome the wire relies on.
        let result = resolve_credentials(&SentrySettings {
            organization: Some("acme".into()),
            token: Some("   ".into()),
            token_source: TokenSource::Env,
            api_base_url: None,
        })
        .await;
        assert!(matches!(result, Err(Error::NotConfigured(_))));

        let result = resolve_credentials(&SentrySettings {
            organization: None,
            token: Some("sntrys_test".into()),
            token_source: TokenSource::Env,
            api_base_url: None,
        })
        .await;
        assert!(matches!(result, Err(Error::NotConfigured(_))));
    }
}
