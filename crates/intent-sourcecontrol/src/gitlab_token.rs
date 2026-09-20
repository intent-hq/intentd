//! GitLab token resolution — the `gitlab` sibling of [`crate::token`].
//!
//! The chain is deliberately shorter than GitHub's: the file-backed secrets
//! store ([`intent_core::FileSecretStore`]) under account
//! `sourceControl.gitlab.token`, then the `GITLAB_TOKEN` environment variable.
//! There is **no CLI fallback** — nothing in the GitLab path may require
//! `gh` or `glab` to be installed. `auto` (the default) tries both in order.
//!
//! A missing token is *not* an error here: [`resolve_gitlab_token`] returns
//! `None`, and [`resolve_gitlab_token_detailed`] reports why each attempted
//! source yielded nothing so the caller can build an actionable
//! `NotConfigured` error. The GitHub chain in [`crate::token`] is untouched.

use std::future::Future;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

pub use crate::token::TokenResolution;

/// Secrets-store account/key for the GitLab access token
/// (`sourceControl.gitlab.token`). Shared with [`crate::gitlab_auth`], which
/// writes/deletes this exact entry.
pub const SECRET_ACCOUNT: &str = "sourceControl.gitlab.token";
/// Secrets-store account/key for the OAuth refresh token the device grant
/// returns next to the access token (`sourceControl.gitlab.refreshToken`).
/// Persisted by [`crate::gitlab_auth`] so a later refresh exchange
/// ([`crate::gitlab_auth::refresh_access_token`]) can renew the (two-hour)
/// access token; never read by the resolution chain. Its presence is also
/// what marks the stored credential as device-grant issued (a PAT has none).
pub const REFRESH_SECRET_ACCOUNT: &str = "sourceControl.gitlab.refreshToken";
/// Secrets-store account/key for the access token's expiry
/// (`sourceControl.gitlab.tokenExpiresAt`, unix seconds as a decimal string).
/// Written next to the refresh token by the device grant / refresh exchange
/// so a caller can refresh proactively; absent for a PAT (no expiry).
pub const EXPIRES_AT_SECRET_ACCOUNT: &str = "sourceControl.gitlab.tokenExpiresAt";
/// Bounded wait for a secrets-store read before treating the entry as absent
/// (same budget as the GitHub chain).
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Strategy used to resolve the GitLab token (`sourceControl.gitlab.*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GitlabTokenSource {
    /// Try the secrets store, then env (the default).
    #[default]
    Auto,
    /// Read from the file-backed secrets store only.
    Explicit,
    /// Read from `GITLAB_TOKEN` only.
    Env,
}

/// One source's attempt: the token, or a reason it yielded nothing.
type SourceResult = std::result::Result<String, String>;

/// Resolve a GitLab token for the given strategy, or `None` if none is
/// available. The secrets-store read runs on the blocking pool with a bounded
/// timeout so a stalled backing store never blocks the async runtime.
pub async fn resolve_gitlab_token(source: GitlabTokenSource) -> Option<String> {
    resolve_gitlab_token_detailed(source).await.token
}

/// Like [`resolve_gitlab_token`], but reports why each attempted source
/// yielded nothing (reasons never carry token material).
pub async fn resolve_gitlab_token_detailed(source: GitlabTokenSource) -> TokenResolution {
    resolve_with(source, file_store_token, env_token).await
}

/// The resolution order over injectable source probes (test seam: the mocks
/// never touch the real secrets store or the process environment). Sources
/// are attempted lazily — a hit short-circuits the rest.
async fn resolve_with<SFut>(
    source: GitlabTokenSource,
    secrets: impl FnOnce() -> SFut,
    env: impl FnOnce() -> SourceResult,
) -> TokenResolution
where
    SFut: Future<Output = SourceResult>,
{
    let mut skipped = Vec::new();
    let attempt = |result: SourceResult, skipped: &mut Vec<String>| match result {
        Ok(token) => Some(token),
        Err(reason) => {
            skipped.push(reason);
            None
        }
    };
    let token = match source {
        GitlabTokenSource::Explicit => attempt(secrets().await, &mut skipped),
        GitlabTokenSource::Env => attempt(env(), &mut skipped),
        GitlabTokenSource::Auto => {
            let mut token = attempt(secrets().await, &mut skipped);
            if token.is_none() {
                token = attempt(env(), &mut skipped);
            }
            token
        }
    };
    TokenResolution { token, skipped }
}

/// Read the token from the file-backed secrets store. A missing or unreadable
/// entry resolves to a skip reason so resolution can fall through.
async fn file_store_token() -> SourceResult {
    let handle =
        tokio::task::spawn_blocking(|| intent_core::FileSecretStore::new().load(SECRET_ACCOUNT));
    match timeout(SECRET_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Ok(Some(v)))) => {
            non_empty(&v).ok_or_else(|| format!("secrets store: `{SECRET_ACCOUNT}` entry is empty"))
        }
        Ok(Ok(Ok(None))) => Err(format!("secrets store: no `{SECRET_ACCOUNT}` entry")),
        Ok(Err(_)) => Err("secrets store: lookup task failed".to_string()),
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                error = %e,
                "secrets-store load failed for gitlab token (corrupt/unreadable file)"
            );
            Err("secrets store: load failed (corrupt/unreadable file)".to_string())
        }
        Err(_) => {
            tracing::warn!(
                account = %SECRET_ACCOUNT,
                "secrets-store load timed out for gitlab token"
            );
            Err("secrets store: load timed out".to_string())
        }
    }
}

/// Read `GITLAB_TOKEN` (the only env source; there is no `GL_TOKEN` alias).
fn env_token() -> SourceResult {
    pick_env_token(std::env::var("GITLAB_TOKEN").ok().as_deref())
        .ok_or_else(|| "env: GITLAB_TOKEN unset or empty".to_string())
}

/// Pure selection of the env token (testable): `GITLAB_TOKEN`, ignoring an
/// empty or whitespace-only value.
fn pick_env_token(gitlab: Option<&str>) -> Option<String> {
    gitlab.and_then(non_empty)
}

fn non_empty(s: &str) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::unnecessary_wraps, reason = "mock probe result shape")]
    fn hit(token: &str) -> SourceResult {
        Ok(token.to_string())
    }

    fn miss(reason: &str) -> SourceResult {
        Err(reason.to_string())
    }

    #[test]
    fn env_token_ignores_blank_values() {
        assert_eq!(pick_env_token(Some("glpat-x")).as_deref(), Some("glpat-x"));
        assert_eq!(
            pick_env_token(Some("  glpat-y ")).as_deref(),
            Some("glpat-y")
        );
        assert_eq!(pick_env_token(Some("   ")), None);
        assert_eq!(pick_env_token(None), None);
    }

    #[tokio::test]
    async fn auto_prefers_the_secrets_store() {
        let r = resolve_with(
            GitlabTokenSource::Auto,
            || async { hit("stored") },
            || panic!("env must not be consulted after a store hit"),
        )
        .await;
        assert_eq!(r.token.as_deref(), Some("stored"));
        assert!(r.skipped.is_empty());
    }

    #[tokio::test]
    async fn auto_falls_through_to_env_and_records_the_skip() {
        let r = resolve_with(
            GitlabTokenSource::Auto,
            || async { miss("secrets store: no entry") },
            || hit("from-env"),
        )
        .await;
        assert_eq!(r.token.as_deref(), Some("from-env"));
        assert_eq!(r.skipped, vec!["secrets store: no entry".to_string()]);
    }

    #[tokio::test]
    async fn auto_reports_every_miss_in_order_without_a_cli_fallback() {
        let r = resolve_with(
            GitlabTokenSource::Auto,
            || async { miss("secrets store: no entry") },
            || miss("env: GITLAB_TOKEN unset or empty"),
        )
        .await;
        assert_eq!(r.token, None);
        assert_eq!(
            r.skipped,
            vec![
                "secrets store: no entry".to_string(),
                "env: GITLAB_TOKEN unset or empty".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn explicit_and_env_consult_only_their_own_source() {
        let r = resolve_with(
            GitlabTokenSource::Explicit,
            || async { miss("secrets store: no entry") },
            || panic!("env must not be consulted for explicit"),
        )
        .await;
        assert_eq!(r.token, None);
        assert_eq!(r.skipped.len(), 1);

        let r = resolve_with(
            GitlabTokenSource::Env,
            || async { panic!("store must not be consulted for env") },
            || hit("from-env"),
        )
        .await;
        assert_eq!(r.token.as_deref(), Some("from-env"));
    }

    #[test]
    fn token_source_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_value(GitlabTokenSource::Explicit).unwrap(),
            "explicit"
        );
        assert_eq!(
            serde_json::from_value::<GitlabTokenSource>(serde_json::json!("env")).unwrap(),
            GitlabTokenSource::Env
        );
        assert_eq!(GitlabTokenSource::default(), GitlabTokenSource::Auto);
    }

    #[test]
    fn secret_accounts_are_provider_scoped() {
        assert_eq!(SECRET_ACCOUNT, "sourceControl.gitlab.token");
        assert_eq!(REFRESH_SECRET_ACCOUNT, "sourceControl.gitlab.refreshToken");
        assert_eq!(
            EXPIRES_AT_SECRET_ACCOUNT,
            "sourceControl.gitlab.tokenExpiresAt"
        );
        assert_ne!(SECRET_ACCOUNT, crate::token::SECRET_ACCOUNT);
    }
}
