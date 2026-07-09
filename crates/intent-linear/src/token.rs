//! Linear API-key resolution.
//!
//! The key is resolved per `linear.tokenSource`:
//!
//! 1. `explicit` — stored in the OS keychain via the `keyring` crate (never in
//!    plaintext config or logs).
//! 2. `env` — `LINEAR_API_KEY`.
//!
//! `auto` (the default) tries keychain then env and uses the first hit. A
//! missing key is *not* an error here — [`resolve`] returns `None`, and the
//! registry turns that into a graceful `NotConfigured`.
//!
//! GUARDRAIL: the key is a secret. It is only ever read and handed to the
//! HTTP client — never logged, echoed, or returned across the wire.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// Keychain service name used for `intentd` secrets.
const KEYRING_SERVICE: &str = "intentd";
/// Keychain account/key for the Linear API key (`linear.token`).
const KEYRING_ACCOUNT: &str = "linear.token";
/// Bounded wait for a keychain read before treating the entry as absent. A
/// stuck OS keychain (e.g. a pending macOS auth prompt) would otherwise block
/// the caller — and, historically, an entire tokio worker — indefinitely.
/// Mirrors the read budget used by `intent-services::AsyncSecretStore`.
const KEYCHAIN_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Strategy used to resolve the Linear API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TokenSource {
    /// Try keychain, then env (the default).
    #[default]
    Auto,
    /// Read from the OS keychain only.
    Explicit,
    /// Read from `LINEAR_API_KEY` only.
    Env,
}

/// Resolve a key for the given strategy, or `None` if none is available.
/// Keychain reads run on the blocking pool with a bounded timeout so a wedged
/// OS keychain never blocks the async runtime.
pub async fn resolve(source: &TokenSource) -> Option<String> {
    match source {
        TokenSource::Explicit => keyring_token().await,
        TokenSource::Env => env_token(),
        TokenSource::Auto => match keyring_token().await {
            Some(v) => Some(v),
            None => env_token(),
        },
    }
}

/// Read the key from the OS keychain. Any keychain error (missing entry,
/// unavailable backend) resolves to `None` so resolution can fall through.
/// Runs on the blocking pool with a bounded timeout so a hung keychain
/// (e.g. a pending macOS auth prompt) cannot wedge a tokio worker.
async fn keyring_token() -> Option<String> {
    let handle = tokio::task::spawn_blocking(|| {
        let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT).ok()?;
        entry.get_password().ok()
    });
    match timeout(KEYCHAIN_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Some(v))) => non_empty(v),
        Ok(Ok(None)) | Ok(Err(_)) => None,
        Err(_) => {
            tracing::warn!(
                account = %KEYRING_ACCOUNT,
                "keychain load timed out for linear token"
            );
            None
        }
    }
}

/// Read `LINEAR_API_KEY` from the environment.
fn env_token() -> Option<String> {
    pick_env_token(std::env::var("LINEAR_API_KEY").ok())
}

/// Pure selection of the env key (testable), ignoring empty values.
pub(crate) fn pick_env_token(linear: Option<String>) -> Option<String> {
    linear.and_then(non_empty)
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
    fn picks_non_empty_env_key() {
        assert_eq!(
            pick_env_token(Some("lin_api_abc".into())).as_deref(),
            Some("lin_api_abc")
        );
    }

    #[test]
    fn ignores_empty_values() {
        assert_eq!(pick_env_token(Some("   ".into())), None);
        assert_eq!(pick_env_token(None), None);
    }

    #[test]
    fn token_source_deserializes_kebab_case() {
        let s: TokenSource = serde_json::from_str("\"explicit\"").unwrap();
        assert_eq!(s, TokenSource::Explicit);
        assert_eq!(TokenSource::default(), TokenSource::Auto);
    }
}
