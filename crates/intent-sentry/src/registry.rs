//! Engine selection from settings.
//!
//! [`SentryRegistry::from_settings`] resolves the Sentry credential pair
//! (inline org/token, each missing half from env) and builds a
//! [`SentryEngine`]. A missing pair yields a typed [`Error::NotConfigured`] so the daemon stays
//! up (graceful, mirroring `intent-linear`).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::client::SentryClient;
use crate::engine::{SentryEngine, SentryEngineImpl};
use crate::error::{Error, Result};
use crate::token::{self, CredentialParts, Credentials};

/// Sentry settings (`sentry.*`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentrySettings {
    /// Inline organization slug (the saved account). Takes precedence over
    /// `SENTRY_ORG`, which only fills a missing half.
    #[serde(default)]
    pub organization: Option<String>,
    /// Inline API token (the saved account, read from the secrets store by
    /// the caller). Takes precedence over `SENTRY_API_TOKEN`. SECRET — never
    /// logged.
    #[serde(default)]
    pub token: Option<String>,
    /// Override for the Sentry REST endpoint (defaults to the public API).
    #[serde(default)]
    pub api_base_url: Option<String>,
}

/// Builds the active [`SentryEngine`] from settings.
pub struct SentryRegistry;

impl SentryRegistry {
    /// Construct the engine, or a typed [`Error::NotConfigured`] when no
    /// credential pair is available.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotConfigured`] when no credential pair is available; propagates client-construction failures.
    pub fn from_settings(settings: &SentrySettings) -> Result<Arc<dyn SentryEngine>> {
        let creds = resolve_credentials(settings)?;
        let client = SentryClient::new(
            &creds.token,
            &creds.organization,
            settings.api_base_url.as_deref(),
        )?;
        Ok(Arc::new(SentryEngineImpl::new(client)))
    }
}

/// Resolve credentials from inline settings, filling a missing half from env.
fn resolve_credentials(settings: &SentrySettings) -> Result<Credentials> {
    let inline = CredentialParts {
        token: settings.token.as_deref().and_then(token::non_empty),
        organization: settings.organization.as_deref().and_then(token::non_empty),
    };
    let parts = if inline.token.is_some() && inline.organization.is_some() {
        inline
    } else {
        inline.or(token::env_parts())
    };
    parts.complete().ok_or_else(|| {
        Error::NotConfigured(
            "sentry: no credentials found (set accounts.sentry.organization + \
             accounts.sentry.token, or SENTRY_ORG + SENTRY_API_TOKEN)"
                .to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_pair_builds_engine() {
        let settings = SentrySettings {
            organization: Some("acme".into()),
            token: Some("sntrys_test".into()),
            ..SentrySettings::default()
        };
        assert!(SentryRegistry::from_settings(&settings).is_ok());
    }

    #[test]
    fn inline_half_only_falls_through_to_not_configured() {
        if std::env::var_os("SENTRY_API_TOKEN").is_some()
            || std::env::var_os("SENTRY_ORG").is_some()
        {
            return;
        }
        let result = resolve_credentials(&SentrySettings {
            organization: Some("acme".into()),
            token: Some("   ".into()),
            api_base_url: None,
        });
        assert!(matches!(result, Err(Error::NotConfigured(_))));

        let result = resolve_credentials(&SentrySettings {
            organization: None,
            token: Some("sntrys_test".into()),
            api_base_url: None,
        });
        assert!(matches!(result, Err(Error::NotConfigured(_))));
    }
}
