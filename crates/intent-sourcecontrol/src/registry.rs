//! Provider selection (§7.4).
//!
//! [`SourceControlRegistry::from_settings`] builds the active
//! [`SourceControl`] from `sourceControl.activeProvider` plus that provider's
//! settings. v1 registers only `github`; selecting any other provider yields a
//! typed [`Error::Config`]. A missing token yields a typed
//! [`Error::NotConfigured`] so the daemon stays up (graceful per §8.3).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::github::GitHubSourceControl;
use crate::token::{self, TokenSource};
use crate::SourceControl;

/// GitHub-specific settings (`sourceControl.github.*`, §9.8).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GithubSettings {
    /// Inline token (already resolved, e.g. read from the secrets store by
    /// the caller). When present and non-empty it takes precedence over
    /// [`Self::token_source`].
    #[serde(default)]
    pub token: Option<String>,
    /// How to resolve the token when [`Self::token`] is absent.
    #[serde(default)]
    pub token_source: TokenSource,
    /// GitHub Enterprise API base URL (`octocrab` `.base_uri(...)`).
    #[serde(default)]
    pub api_base_url: Option<String>,
}

/// Top-level source-control settings (`sourceControl.*`, §9.8).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceControlSettings {
    /// Active provider id (v1 supports only `"github"`).
    pub active_provider: String,
    /// GitHub provider settings.
    #[serde(default)]
    pub github: GithubSettings,
}

impl Default for SourceControlSettings {
    fn default() -> Self {
        Self {
            active_provider: "github".to_string(),
            github: GithubSettings::default(),
        }
    }
}

/// Builds the active [`SourceControl`] implementation from settings.
pub struct SourceControlRegistry;

impl SourceControlRegistry {
    /// Construct the active provider, or a typed error when the provider is
    /// unknown ([`Error::Config`]) or no token is available
    /// ([`Error::NotConfigured`]). Async because the secrets-store / `gh` lookups
    /// run on the blocking pool with bounded timeouts (see [`token::resolve`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] for an unknown provider; [`Error::NotConfigured`] when no token is available; propagates client-construction failures.
    pub async fn from_settings(settings: &SourceControlSettings) -> Result<Arc<dyn SourceControl>> {
        match settings.active_provider.as_str() {
            "github" => {
                let gh = GitHubSourceControl::new(
                    &resolve_github_token(&settings.github).await?,
                    settings.github.api_base_url.as_deref(),
                )?;
                Ok(Arc::new(gh))
            }
            other => Err(Error::Config(format!(
                "unknown source-control provider {other:?} (v1 supports only \"github\")"
            ))),
        }
    }
}

/// Resolve the GitHub token from inline settings or the configured source.
async fn resolve_github_token(github: &GithubSettings) -> Result<String> {
    if let Some(token) = github.token.as_deref() {
        if !token.trim().is_empty() {
            return Ok(token.trim().to_string());
        }
    }
    let resolution = token::resolve_detailed(&github.token_source).await;
    resolution.token.ok_or_else(|| {
        let mut msg = String::from(
            "github: no token found (set sourceControl.github.token, GITHUB_TOKEN/GH_TOKEN, \
             or authenticate with `gh auth login`)",
        );
        if !resolution.skipped.is_empty() {
            msg.push_str("; sources tried: ");
            msg.push_str(&resolution.skipped.join("; "));
        }
        Error::NotConfigured(msg)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_provider_is_config_error() {
        let settings = SourceControlSettings {
            active_provider: "gitlab".to_string(),
            github: GithubSettings::default(),
        };
        let result = SourceControlRegistry::from_settings(&settings).await;
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn inline_token_builds_github() {
        let settings = SourceControlSettings {
            active_provider: "github".to_string(),
            github: GithubSettings {
                token: Some("ghp_test_token".to_string()),
                ..GithubSettings::default()
            },
        };
        let sc = SourceControlRegistry::from_settings(&settings)
            .await
            .expect("should build");
        assert_eq!(sc.provider_id(), "github");
        assert!(sc.capabilities().check_runs);
    }

    #[tokio::test]
    async fn blank_inline_token_falls_through_to_not_configured() {
        // Use the `Env` source so the test does not touch the secrets store or
        // shell out to `gh`; a blank inline token must still yield the same
        // `NotConfigured` outcome the wire relies on.
        let token = resolve_github_token(&GithubSettings {
            token: Some("   ".to_string()),
            token_source: TokenSource::Env,
            api_base_url: None,
        })
        .await;
        // The error names the sources tried and why each yielded nothing
        // (monorepo#3321). GITHUB_TOKEN/GH_TOKEN may legitimately be set in
        // dev/CI shells, in which case resolution succeeds instead.
        match token {
            Err(Error::NotConfigured(msg)) => {
                assert!(msg.contains("sources tried"), "{msg}");
                assert!(msg.contains("GITHUB_TOKEN/GH_TOKEN"), "{msg}");
            }
            Ok(_) => assert!(
                std::env::var("GITHUB_TOKEN").is_ok() || std::env::var("GH_TOKEN").is_ok(),
                "resolution succeeded without an env token"
            ),
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
}
