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
use crate::gitlab_auth::{GitlabHost, GITLAB_COM_HOST};
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

/// GitLab-specific settings (`sourceControl.gitlab.*`): one instance per
/// daemon. Consumed by [`crate::gitlab_auth`] (device grant / PAT) and
/// [`crate::gitlab_token`]; the registry does not build a GitLab provider yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitlabSettings {
    /// Instance host or URL (`gitlab.com`, `https://gitlab.acme.internal`);
    /// normalized by [`GitlabHost::parse`].
    #[serde(default = "default_gitlab_host")]
    pub host: String,
    /// OAuth application client id for the device grant (public, not a
    /// secret). Empty falls back to the compiled gitlab.com default on
    /// gitlab.com only — see [`crate::gitlab_auth::resolve_client_id`].
    #[serde(default)]
    pub oauth_client_id: String,
    /// Instance root override (test seam / non-standard deployments). When
    /// set it replaces [`Self::host`] for every `/oauth/*` and `/api/v4/*`
    /// request; same normalization rules as the host.
    #[serde(default)]
    pub api_base_url: Option<String>,
}

fn default_gitlab_host() -> String {
    GITLAB_COM_HOST.to_string()
}

impl Default for GitlabSettings {
    fn default() -> Self {
        Self {
            host: default_gitlab_host(),
            oauth_client_id: String::new(),
            api_base_url: None,
        }
    }
}

impl GitlabSettings {
    /// The instance every request targets: [`Self::api_base_url`] when set
    /// (non-blank), else [`Self::host`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when the chosen value is not a valid GitLab
    /// instance reference (see [`GitlabHost::parse`]).
    pub fn resolved_host(&self) -> Result<GitlabHost> {
        match self
            .api_base_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(base) => GitlabHost::parse(base),
            None => GitlabHost::parse(&self.host),
        }
    }
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
    /// GitLab provider settings (additive; not yet selectable as the active
    /// provider).
    #[serde(default)]
    pub gitlab: GitlabSettings,
}

impl Default for SourceControlSettings {
    fn default() -> Self {
        Self {
            active_provider: "github".to_string(),
            github: GithubSettings::default(),
            gitlab: GitlabSettings::default(),
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
            ..SourceControlSettings::default()
        };
        let result = SourceControlRegistry::from_settings(&settings).await;
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn gitlab_settings_default_to_gitlab_com_and_deserialize_additively() {
        let defaults = SourceControlSettings::default();
        assert_eq!(defaults.gitlab, GitlabSettings::default());
        assert_eq!(defaults.gitlab.host, "gitlab.com");
        assert!(defaults.gitlab.oauth_client_id.is_empty());
        assert_eq!(defaults.gitlab.api_base_url, None);
        assert_eq!(
            defaults.gitlab.resolved_host().unwrap().host(),
            "gitlab.com"
        );

        // Existing settings payloads without a `gitlab` block keep working.
        let parsed: SourceControlSettings =
            serde_json::from_value(serde_json::json!({ "activeProvider": "github" })).unwrap();
        assert_eq!(parsed.gitlab, GitlabSettings::default());

        let parsed: SourceControlSettings = serde_json::from_value(serde_json::json!({
            "activeProvider": "github",
            "gitlab": { "host": "https://GitLab.Acme.internal/", "oauthClientId": "abc" }
        }))
        .unwrap();
        assert_eq!(parsed.gitlab.oauth_client_id, "abc");
        let host = parsed.gitlab.resolved_host().unwrap();
        assert_eq!(host.host(), "gitlab.acme.internal");
        assert_eq!(host.base_url(), "https://gitlab.acme.internal");
        let v = serde_json::to_value(&parsed.gitlab).unwrap();
        assert_eq!(v["host"], "https://GitLab.Acme.internal/");
        assert_eq!(v["oauthClientId"], "abc");
    }

    #[test]
    fn gitlab_api_base_url_overrides_the_host_when_non_blank() {
        let settings = GitlabSettings {
            host: "gitlab.com".to_string(),
            api_base_url: Some("http://127.0.0.1:4321".to_string()),
            ..GitlabSettings::default()
        };
        assert_eq!(settings.resolved_host().unwrap().host(), "127.0.0.1:4321");
        let blank = GitlabSettings {
            api_base_url: Some("   ".to_string()),
            ..GitlabSettings::default()
        };
        assert_eq!(blank.resolved_host().unwrap().host(), "gitlab.com");
        let bad = GitlabSettings {
            host: "http://gitlab.acme.internal".to_string(),
            ..GitlabSettings::default()
        };
        assert!(matches!(bad.resolved_host(), Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn inline_token_builds_github() {
        let settings = SourceControlSettings {
            active_provider: "github".to_string(),
            github: GithubSettings {
                token: Some("ghp_test_token".to_string()),
                ..GithubSettings::default()
            },
            ..SourceControlSettings::default()
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
