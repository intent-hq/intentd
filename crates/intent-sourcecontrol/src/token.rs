//! GitHub token resolution (§7.3).
//!
//! Tokens are resolved per `sourceControl.github.tokenSource`:
//!
//! 1. `explicit` — stored in the file-backed secrets store
//!    ([`intent_core::FileSecretStore`], `~/intent/secrets.json`) under
//!    account `sourceControl.github.token` (never in plaintext config or logs).
//! 2. `env` — `GITHUB_TOKEN` / `GH_TOKEN`.
//! 3. `gh-cli` — `gh auth token` (shell out to the GitHub CLI).
//!
//! `auto` (the default) tries the three in order and uses the first hit. A
//! missing token is *not* an error here — [`resolve`] returns `None`, and the
//! registry turns that into a graceful `NotConfigured` (§7.4).

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

/// Secrets-store account/key for the GitHub token (`sourceControl.github.token`).
/// Shared with [`crate::device_flow`], which writes/deletes this exact entry.
pub(crate) const SECRET_ACCOUNT: &str = "sourceControl.github.token";
/// Bounded wait for a secrets-store read before treating the entry as absent.
/// A stalled backing store (e.g. a wedged filesystem) would otherwise block
/// the caller indefinitely. Mirrors the read budget used by
/// `intent-services::AsyncSecretStore`.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Bounded wait for the `gh auth token` subprocess. Shelling out to `gh` can
/// stall on flaky network / OS state, so cap it so the async runtime is never
/// blocked waiting on the child.
const GH_CLI_TIMEOUT: Duration = Duration::from_secs(3);

/// Strategy used to resolve the GitHub token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TokenSource {
    /// Try the secrets store, then env, then `gh` CLI (the default).
    #[default]
    Auto,
    /// Read from the file-backed secrets store only.
    Explicit,
    /// Read from `GITHUB_TOKEN` / `GH_TOKEN` only.
    Env,
    /// Shell out to `gh auth token` only.
    GhCli,
}

/// Resolve a token for the given strategy, or `None` if none is available.
/// Secrets-store and `gh` subprocess reads run on the blocking pool with
/// bounded timeouts so a stalled backing store or hung child never blocks the
/// async runtime.
pub async fn resolve(source: &TokenSource) -> Option<String> {
    match source {
        TokenSource::Explicit => file_store_token().await,
        TokenSource::Env => env_token(),
        TokenSource::GhCli => gh_cli_token().await,
        TokenSource::Auto => {
            if let Some(v) = file_store_token().await {
                return Some(v);
            }
            if let Some(v) = env_token() {
                return Some(v);
            }
            gh_cli_token().await
        }
    }
}

/// Read the token from the file-backed secrets store
/// ([`intent_core::FileSecretStore`]). A missing or unreadable entry resolves
/// to `None` so resolution can fall through. Runs on the blocking pool with a
/// bounded timeout so a stalled backing store cannot wedge a tokio worker.
async fn file_store_token() -> Option<String> {
    let handle =
        tokio::task::spawn_blocking(|| intent_core::FileSecretStore::new().load(SECRET_ACCOUNT));
    match timeout(SECRET_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Ok(Some(v)))) => non_empty(v),
        Ok(Ok(Ok(None))) => None,
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                error = %e,
                "secrets-store load failed for github token (corrupt/unreadable file)"
            );
            None
        }
        Ok(Err(_)) => None,
        Err(_) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                "secrets-store load timed out for github token"
            );
            None
        }
    }
}

/// Read `GITHUB_TOKEN`, falling back to `GH_TOKEN`.
fn env_token() -> Option<String> {
    pick_env_token(
        std::env::var("GITHUB_TOKEN").ok(),
        std::env::var("GH_TOKEN").ok(),
    )
}

/// Pure selection of the env token (testable): prefer `GITHUB_TOKEN`, then
/// `GH_TOKEN`, ignoring empty values.
pub(crate) fn pick_env_token(github: Option<String>, gh: Option<String>) -> Option<String> {
    github
        .and_then(non_empty)
        .or_else(|| gh.and_then(non_empty))
}

/// Shell out to `gh auth token`. A non-zero exit or missing CLI yields `None`.
/// Runs on the blocking pool with a bounded timeout so a wedged child can't
/// block a tokio worker.
async fn gh_cli_token() -> Option<String> {
    let handle = tokio::task::spawn_blocking(|| {
        let output = std::process::Command::new("gh")
            .args(["auth", "token"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    });
    match timeout(GH_CLI_TIMEOUT, handle).await {
        Ok(Ok(Some(v))) => non_empty(v),
        Ok(Ok(None)) | Ok(Err(_)) => None,
        Err(_) => {
            tracing::warn!("`gh auth token` timed out");
            None
        }
    }
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
    fn prefers_github_token_over_gh_token() {
        let picked = pick_env_token(Some("gho_primary".into()), Some("gho_fallback".into()));
        assert_eq!(picked.as_deref(), Some("gho_primary"));
    }

    #[test]
    fn falls_back_to_gh_token() {
        let picked = pick_env_token(None, Some("gho_fallback".into()));
        assert_eq!(picked.as_deref(), Some("gho_fallback"));
    }

    #[test]
    fn ignores_empty_values() {
        assert_eq!(
            pick_env_token(Some("   ".into()), Some(String::new())),
            None
        );
        assert_eq!(pick_env_token(None, None), None);
    }

    #[test]
    fn token_source_deserializes_kebab_case() {
        let s: TokenSource = serde_json::from_str("\"gh-cli\"").unwrap();
        assert_eq!(s, TokenSource::GhCli);
        assert_eq!(TokenSource::default(), TokenSource::Auto);
    }
}
