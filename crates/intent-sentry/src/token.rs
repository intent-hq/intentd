//! Sentry credential resolution.
//!
//! Sentry needs **both** an organization slug and an API token — they are
//! resolved together as [`Credentials`]. Resolution honours `sentry.tokenSource`:
//!
//! 1. `explicit` — read the OS keychain (`sentry.token` + `sentry.org`).
//! 2. `env` — read `SENTRY_API_TOKEN` + `SENTRY_ORG`.
//!
//! `auto` (the default) tries keychain then env and uses the first hit that
//! yields **both** values. A missing pair is *not* an error here —
//! [`resolve`] returns `None`, and the registry turns that into a graceful
//! `NotConfigured`.
//!
//! GUARDRAIL: the token is a secret. It is only ever read and handed to the
//! HTTP client — never logged, echoed, or returned across the wire.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// Keychain service name used for `intentd` secrets.
const KEYRING_SERVICE: &str = "intentd";
/// Keychain account/key for the Sentry API token.
const KEYRING_TOKEN_ACCOUNT: &str = "sentry.token";
/// Keychain account/key for the Sentry organization slug.
const KEYRING_ORG_ACCOUNT: &str = "sentry.org";
/// Bounded wait for a keychain read before treating the entry as absent. A
/// stuck OS keychain (e.g. a pending macOS auth prompt) would otherwise block
/// the caller — and, historically, an entire tokio worker — indefinitely.
/// Mirrors the read budget used by `intent-services::AsyncSecretStore`.
const KEYCHAIN_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Strategy used to resolve the Sentry credential pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TokenSource {
    /// Try keychain, then env (the default).
    #[default]
    Auto,
    /// Read from the OS keychain only.
    Explicit,
    /// Read from `SENTRY_API_TOKEN` + `SENTRY_ORG` only.
    Env,
}

/// A resolved Sentry credential pair. The `token` field is secret and is
/// only handed to the HTTP client.
#[derive(Clone)]
pub struct Credentials {
    pub token: String,
    pub organization: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("organization", &self.organization)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Resolve a credential pair for the given strategy, or `None` if either half
/// is missing. Keychain reads run on the blocking pool with a bounded timeout
/// so a wedged OS keychain never blocks the async runtime.
pub async fn resolve(source: &TokenSource) -> Option<Credentials> {
    match source {
        TokenSource::Explicit => keyring_credentials().await,
        TokenSource::Env => env_credentials(),
        TokenSource::Auto => match keyring_credentials().await {
            Some(c) => Some(c),
            None => env_credentials(),
        },
    }
}

/// Read both halves of the credential pair from the OS keychain. Both entries
/// are loaded off the async runtime on the blocking pool with a bounded
/// timeout so a hung keychain (e.g. a pending macOS auth prompt) cannot
/// wedge a tokio worker.
async fn keyring_credentials() -> Option<Credentials> {
    let handle = tokio::task::spawn_blocking(|| {
        let token = keyring::Entry::new(KEYRING_SERVICE, KEYRING_TOKEN_ACCOUNT)
            .ok()?
            .get_password()
            .ok()?;
        let organization = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ORG_ACCOUNT)
            .ok()?
            .get_password()
            .ok()?;
        Some((token, organization))
    });
    let pair = match timeout(KEYCHAIN_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Some(pair))) => pair,
        Ok(Ok(None)) | Ok(Err(_)) => return None,
        Err(_) => {
            tracing::warn!("keychain load timed out for sentry credentials");
            return None;
        }
    };
    let token = non_empty(pair.0)?;
    let organization = non_empty(pair.1)?;
    Some(Credentials {
        token,
        organization,
    })
}

/// Read both halves of the credential pair from the environment.
fn env_credentials() -> Option<Credentials> {
    pick_env_credentials(
        std::env::var("SENTRY_API_TOKEN").ok(),
        std::env::var("SENTRY_ORG").ok(),
    )
}

/// Pure selection of env credentials (testable). Both halves must be
/// non-empty after trimming.
pub(crate) fn pick_env_credentials(
    token: Option<String>,
    organization: Option<String>,
) -> Option<Credentials> {
    let token = token.and_then(non_empty)?;
    let organization = organization.and_then(non_empty)?;
    Some(Credentials {
        token,
        organization,
    })
}

/// `Some(s)` only when `s` is non-empty after trimming.
fn non_empty(s: String) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_non_empty_env_pair() {
        let c = pick_env_credentials(Some("tok".into()), Some("acme".into())).unwrap();
        assert_eq!(c.token, "tok");
        assert_eq!(c.organization, "acme");
    }

    #[test]
    fn missing_half_yields_none() {
        assert!(pick_env_credentials(Some("tok".into()), None).is_none());
        assert!(pick_env_credentials(None, Some("acme".into())).is_none());
        assert!(pick_env_credentials(Some("   ".into()), Some("acme".into())).is_none());
        assert!(pick_env_credentials(Some("tok".into()), Some("".into())).is_none());
    }

    #[test]
    fn token_source_deserializes_kebab_case() {
        let s: TokenSource = serde_json::from_str("\"explicit\"").unwrap();
        assert_eq!(s, TokenSource::Explicit);
        assert_eq!(TokenSource::default(), TokenSource::Auto);
    }

    #[test]
    fn debug_redacts_token() {
        let c = Credentials {
            token: "supersecret".into(),
            organization: "acme".into(),
        };
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
        assert!(dbg.contains("acme"));
    }
}
