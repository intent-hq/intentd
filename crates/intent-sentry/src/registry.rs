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
        Self::from_settings_with_host_env(settings, true)
    }

    /// [`Self::from_settings`], filling a missing half from `SENTRY_API_TOKEN`
    /// / `SENTRY_ORG` only when `host_env` is set — an orchestrator tenant
    /// passes `false` so it never borrows the host's Sentry account.
    ///
    /// # Errors
    ///
    /// As [`Self::from_settings`].
    pub fn from_settings_with_host_env(
        settings: &SentrySettings,
        host_env: bool,
    ) -> Result<Arc<dyn SentryEngine>> {
        let env = if host_env {
            token::env_parts()
        } else {
            CredentialParts::default()
        };
        let creds = resolve_credentials(settings, env)?;
        let client = SentryClient::new(
            &creds.token,
            &creds.organization,
            settings.api_base_url.as_deref(),
        )?;
        Ok(Arc::new(SentryEngineImpl::new(client)))
    }
}

/// Resolve credentials from inline settings, filling a missing half from `env`.
fn resolve_credentials(settings: &SentrySettings, env: CredentialParts) -> Result<Credentials> {
    let inline = CredentialParts {
        token: settings.token.as_deref().and_then(token::non_empty),
        organization: settings.organization.as_deref().and_then(token::non_empty),
    };
    inline.or(env).complete().ok_or_else(|| {
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

    fn settings(organization: Option<&str>, token: Option<&str>) -> SentrySettings {
        SentrySettings {
            organization: organization.map(str::to_string),
            token: token.map(str::to_string),
            api_base_url: None,
        }
    }

    fn env(organization: Option<&str>, token: Option<&str>) -> CredentialParts {
        CredentialParts {
            token: token.map(str::to_string),
            organization: organization.map(str::to_string),
        }
    }

    fn pair(c: &Credentials) -> (&str, &str) {
        (c.organization.as_str(), c.token.as_str())
    }

    #[test]
    fn saved_pair_wins_over_env() {
        let c = resolve_credentials(
            &settings(Some("saved-org"), Some("saved-tok")),
            env(Some("env-org"), Some("env-tok")),
        )
        .unwrap();
        assert_eq!(pair(&c), ("saved-org", "saved-tok"));
    }

    #[test]
    fn each_missing_saved_half_comes_from_env() {
        let c = resolve_credentials(
            &settings(Some("saved-org"), Some("   ")),
            env(Some("env-org"), Some("env-tok")),
        )
        .unwrap();
        assert_eq!(pair(&c), ("saved-org", "env-tok"));

        let c = resolve_credentials(
            &settings(None, Some("saved-tok")),
            env(Some("env-org"), None),
        )
        .unwrap();
        assert_eq!(pair(&c), ("env-org", "saved-tok"));
    }

    #[test]
    fn a_missing_half_everywhere_is_not_configured() {
        for (saved, fallback) in [
            (settings(Some("acme"), Some("   ")), env(None, None)),
            (
                settings(None, Some("sntrys_test")),
                env(None, Some("env-tok")),
            ),
            (settings(None, None), env(Some("env-org"), None)),
        ] {
            assert!(matches!(
                resolve_credentials(&saved, fallback),
                Err(Error::NotConfigured(_))
            ));
        }
    }
}
