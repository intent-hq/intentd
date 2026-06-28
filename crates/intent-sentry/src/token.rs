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

use serde::{Deserialize, Serialize};

/// Keychain service name used for `intentd` secrets.
const KEYRING_SERVICE: &str = "intentd";
/// Keychain account/key for the Sentry API token.
const KEYRING_TOKEN_ACCOUNT: &str = "sentry.token";
/// Keychain account/key for the Sentry organization slug.
const KEYRING_ORG_ACCOUNT: &str = "sentry.org";

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
/// is missing.
pub fn resolve(source: &TokenSource) -> Option<Credentials> {
    match source {
        TokenSource::Explicit => keyring_credentials(),
        TokenSource::Env => env_credentials(),
        TokenSource::Auto => keyring_credentials().or_else(env_credentials),
    }
}

/// Read both halves of the credential pair from the OS keychain.
fn keyring_credentials() -> Option<Credentials> {
    let token = keyring::Entry::new(KEYRING_SERVICE, KEYRING_TOKEN_ACCOUNT)
        .ok()?
        .get_password()
        .ok()
        .and_then(non_empty)?;
    let organization = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ORG_ACCOUNT)
        .ok()?
        .get_password()
        .ok()
        .and_then(non_empty)?;
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
