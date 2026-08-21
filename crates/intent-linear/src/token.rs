//! Linear API-key resolution.
//!
//! The key is resolved per `linear.tokenSource`:
//!
//! 1. `explicit` — stored in the file-backed secrets store
//!    ([`intent_core::FileSecretStore`], `~/intent/secrets.json`) under
//!    account `linear.token` (never in plaintext config or logs).
//! 2. `env` — `LINEAR_API_KEY`.
//!
//! `auto` (the default) tries the secrets store then env and uses the first hit. A
//! missing key is *not* an error here — [`resolve`] returns `None`, and the
//! registry turns that into a graceful `NotConfigured`.
//!
//! GUARDRAIL: the key is a secret. It is only ever read and handed to the
//! HTTP client — never logged, echoed, or returned across the wire.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// Secrets-store account/key for the Linear API key (`linear.token`).
const SECRET_ACCOUNT: &str = "linear.token";
/// Bounded wait for a secrets-store read before treating the entry as absent.
/// A stalled backing store (e.g. a wedged filesystem) would otherwise block
/// the caller indefinitely. Mirrors the read budget used by
/// `intent-services::AsyncSecretStore`.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Strategy used to resolve the Linear API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TokenSource {
    /// Try the secrets store, then env (the default).
    #[default]
    Auto,
    /// Read from the file-backed secrets store only.
    Explicit,
    /// Read from `LINEAR_API_KEY` only.
    Env,
}

/// Resolve a key for the given strategy, or `None` if none is available.
/// Secrets-store reads run on the blocking pool with a bounded timeout so a
/// stalled backing store never blocks the async runtime.
pub async fn resolve(source: &TokenSource) -> Option<String> {
    match source {
        TokenSource::Explicit => file_store_token().await,
        TokenSource::Env => env_token(),
        TokenSource::Auto => match file_store_token().await {
            Some(v) => Some(v),
            None => env_token(),
        },
    }
}

/// Read the key from the file-backed secrets store
/// ([`intent_core::FileSecretStore`]). A missing or unreadable entry resolves
/// to `None` so resolution can fall through. Runs on the blocking pool with a
/// bounded timeout so a stalled backing store cannot wedge a tokio worker.
async fn file_store_token() -> Option<String> {
    let handle =
        tokio::task::spawn_blocking(|| intent_core::FileSecretStore::new().load(SECRET_ACCOUNT));
    match timeout(SECRET_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Ok(Some(v)))) => non_empty(&v),
        Ok(Ok(Ok(None))) => None,
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                error = %e,
                "secrets-store load failed for linear token (corrupt/unreadable file)"
            );
            None
        }
        Ok(Err(_)) => None,
        Err(_) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                "secrets-store load timed out for linear token"
            );
            None
        }
    }
}

/// Read `LINEAR_API_KEY` from the environment.
fn env_token() -> Option<String> {
    pick_env_token(std::env::var("LINEAR_API_KEY").ok().as_deref())
}

/// Pure selection of the env key (testable), ignoring empty values.
pub(crate) fn pick_env_token(linear: Option<&str>) -> Option<String> {
    linear.and_then(non_empty)
}

/// `Some(s)` only when `s` is non-empty after trimming.
fn non_empty(s: &str) -> Option<String> {
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
            pick_env_token(Some("lin_api_abc")).as_deref(),
            Some("lin_api_abc")
        );
    }

    #[test]
    fn ignores_empty_values() {
        assert_eq!(pick_env_token(Some("   ")), None);
        assert_eq!(pick_env_token(None), None);
    }

    #[test]
    fn token_source_deserializes_kebab_case() {
        let s: TokenSource = serde_json::from_str("\"explicit\"").unwrap();
        assert_eq!(s, TokenSource::Explicit);
        assert_eq!(TokenSource::default(), TokenSource::Auto);
    }
}
