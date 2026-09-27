//! Shared execution reads and sanitized invalidation, separate from host setup.

use intent_core::events::{
    HOST_EXECUTION_CONTEXT_CHANGED, SETTINGS_CHANGED, SOURCE_CONTROL_AUTH_CHANGED,
};
use intent_core::execution::{GitCredentialPolicy, HostExecutionContext, RepositoryConnection};
use intent_core::settings_file::GithubTokenSource;
use intent_core::{now_iso, Result, WorkspaceId};
use intent_sourcecontrol::token::{self, TokenSource};
use intent_store::NewEvent;
use serde_json::Value;

use crate::{events::SubscriptionFilter, publish_event, system_actor, Services};

impl Services {
    /// Presence only, with no forge request or AI provider spawn. The CLI
    /// credential fallback uses the existing bounded token cache. Never read
    /// collaboration-purpose credential slots here.
    pub(crate) async fn execution_context_snapshot(&self) -> Result<HostExecutionContext> {
        let settings = self.effective_settings();
        let mut enabled_provider_ids: Vec<_> = intent_providers::all_provider_ids()
            .into_iter()
            .filter(|id| {
                !crate::agent_ops::provider_is_disabled(id, settings.providers.enabled.as_ref())
            })
            .map(str::to_owned)
            .collect();
        enabled_provider_ids.sort_unstable();
        enabled_provider_ids.dedup();
        let source = settings.source_control.github.token_source;
        let stored = if matches!(
            source,
            GithubTokenSource::Auto | GithubTokenSource::Explicit
        ) {
            self.secrets
                .load("sourceControl.github.token")
                .await?
                .is_some_and(|s| !s.trim().is_empty())
        } else {
            false
        };
        let github_configured = self.source_control.is_some()
            || stored
            || match source {
                GithubTokenSource::Explicit => false,
                GithubTokenSource::Auto => {
                    token::resolve(&TokenSource::Env).await.is_some()
                        || token::resolve(&TokenSource::GhCli).await.is_some()
                }
                GithubTokenSource::Env => token::resolve(&TokenSource::Env).await.is_some(),
                GithubTokenSource::GhCli => token::resolve(&TokenSource::GhCli).await.is_some(),
            };
        let mut repository_connections = vec![RepositoryConnection {
            provider: "github".into(),
            host: github_execution_host(&settings.source_control.github.api_base_url)
                .unwrap_or_else(|| "github.com".into()),
            configured: github_configured,
        }];
        if let Ok(host) =
            crate::source_control_auth_ops::parse_gitlab_host(&settings.source_control.gitlab.host)
        {
            repository_connections.push(RepositoryConnection {
                provider: "gitlab".into(),
                host: host.host().into(),
                configured: self.own_gitlab_token(&host).await.is_some(),
            });
        }
        Ok(HostExecutionContext {
            default_provider_id: settings.model.default_provider,
            default_model_id: settings.model.default,
            enabled_provider_ids,
            repository_connections,
            git_credential_policy: GitCredentialPolicy::github_https(
                crate::terminal_ops::expose_git_credential(self.settings_registry.as_deref()),
            ),
        })
    }

    pub(crate) async fn publish_execution_context_changed(&self) {
        let Some(bus) = self.event_bus.as_ref() else {
            return;
        };
        if let Ok(snapshot) = self.execution_context_snapshot().await {
            publish_event(
                Some(bus),
                NewEvent {
                    workspace_id: WorkspaceId::from(""),
                    timestamp: now_iso(),
                    event_type: HOST_EXECUTION_CONTEXT_CHANGED.into(),
                    actor: system_actor(),
                    session_id: None,
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: serde_json::to_value(snapshot).expect("execution context serializes"),
                },
            )
            .await;
        } else {
            tracing::warn!("safe execution context unavailable during invalidation");
        }
    }

    /// Observe committed setup changes. Subscribe before returning, so an
    /// immediate mutation cannot race registration. The daemon owns lifetime;
    /// a member disconnect never stops shared execution or these updates.
    #[must_use]
    pub fn spawn_execution_context_loop(&self) -> tokio::task::JoinHandle<()> {
        let mut sub = self.event_bus.as_ref().map(|bus| {
            bus.subscribe(SubscriptionFilter {
                event_types: vec![SETTINGS_CHANGED.into(), SOURCE_CONTROL_AUTH_CHANGED.into()],
                ..Default::default()
            })
        });
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let Some(ref mut sub) = sub else { return };
            loop {
                tokio::select! {
                    events = sub.recv() => {
                        let Some(events) = events else { break };
                        if events.iter().any(|event| event.event_type == SOURCE_CONTROL_AUTH_CHANGED
                            || execution_settings_changed(&event.data)) {
                            services.publish_execution_context_changed().await;
                        }
                    }
                    () = services.execution_invalidation.notified() => {
                        services.publish_execution_context_changed().await;
                    }
                }
            }
        })
    }
}

fn execution_settings_changed(data: &Value) -> bool {
    data.get("changes")
        .and_then(Value::as_array)
        .is_some_and(|changes| {
            changes.iter().any(|change| {
                change
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| {
                        path.starts_with("model.")
                            || path.starts_with("providers.")
                            || path.starts_with("sourceControl.")
                            || path == "context.auggiePath"
                    })
            })
        })
}

use intent_core::execution::{
    ExecutionAuthorizationFailure, ExecutionAuthorizationReason, ExecutionResource,
    GIT_HELPER_SETTING,
};
use intent_core::{BoxFuture, Caller, Error, HostRole};
use std::future::Future;

tokio::task_local! {
    static EXECUTION_CALL: ExecutionCall;
}

#[derive(Clone)]
struct ExecutionCall {
    services: Services,
    member: bool,
}

impl Services {
    /// Preserve the authenticated caller while sharing host execution state.
    /// Only classified failures get diagnostics; command text is never parsed.
    pub(crate) fn execution_call<'a, T: Send + 'a>(
        &'a self,
        future: impl Future<Output = Result<T>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T>> {
        Box::pin(async move {
            let member = match intent_core::current_caller() {
                Some(Caller::Wire {
                    principal_id,
                    host_role: HostRole::Member | HostRole::Guest,
                }) => self
                    .store
                    .get_host_role(&principal_id)
                    .await
                    .is_ok_and(|r| r == HostRole::Member),
                _ => false,
            };
            EXECUTION_CALL
                .scope(
                    ExecutionCall {
                        services: self.clone(),
                        member,
                    },
                    future,
                )
                .await
        })
    }
}

/// Called at the provider's typed error boundary, before its response text is
/// flattened into the legacy Internal error. Bodies never enter diagnostics.
pub(crate) fn forge_error(error: intent_sourcecontrol::Error) -> Error {
    use intent_sourcecontrol::Error as ScError;
    let reason = match &error {
        ScError::NotConfigured(_) => Some(ExecutionAuthorizationReason::Missing),
        ScError::Auth(_) if error.is_insufficient_scope() => {
            Some(ExecutionAuthorizationReason::InsufficientScope)
        }
        ScError::Auth(_) => Some(ExecutionAuthorizationReason::Rejected),
        _ => None,
    };
    let legacy = match error {
        ScError::Unsupported(msg) => Error::Internal(format!("unsupported by provider: {msg}")),
        ScError::RateLimited(msg) => Error::RateLimited(msg),
        other => Error::Internal(other.to_string()),
    };
    let Some(reason) = reason else { return legacy };
    let provider = EXECUTION_CALL
        .try_with(|c| {
            c.services
                .source_control
                .as_ref()
                .map_or("github", |s| s.provider_id())
                .to_string()
        })
        .ok();
    let (host, github_https) = EXECUTION_CALL
        .try_with(|c| {
            let settings = c.services.effective_settings();
            match provider.as_deref() {
                Some("github") => {
                    let base = &settings.source_control.github.api_base_url;
                    let host = github_execution_host(base);
                    let https = host.as_deref() == Some("github.com")
                        && reqwest::Url::parse(base).is_ok_and(|u| u.scheme() == "https");
                    (host, https)
                }
                Some("gitlab") => (
                    crate::source_control_auth_ops::parse_gitlab_host(
                        &settings.source_control.gitlab.host,
                    )
                    .ok()
                    .map(|h| h.host().to_owned()),
                    false,
                ),
                _ => (None, false),
            }
        })
        .unwrap_or((None, false));
    authorization_error(
        legacy,
        ExecutionAuthorizationFailure::new(ExecutionResource::Git, reason, provider, host),
        github_https,
    )
}

fn authorization_error(
    legacy: Error,
    mut authorization: ExecutionAuthorizationFailure,
    github_https: bool,
) -> Error {
    let context = EXECUTION_CALL.try_with(Clone::clone).ok();
    let Some(context) = context else {
        return legacy;
    };
    context.services.execution_invalidation.notify_one();
    if !context.member {
        return legacy;
    }
    if github_https
        && !crate::terminal_ops::expose_git_credential(
            context.services.settings_registry.as_deref(),
        )
    {
        authorization.recovery.setting = Some(GIT_HELPER_SETTING.into());
    }
    Error::ExecutionAuthorization {
        source: Box::new(legacy),
        authorization: Box::new(authorization),
    }
}

pub(crate) fn ai_authorization_error(
    legacy: Error,
    provider: &str,
    reason: ExecutionAuthorizationReason,
) -> Error {
    authorization_error(
        legacy,
        ExecutionAuthorizationFailure::new(
            ExecutionResource::Ai,
            reason,
            Some(provider.into()),
            None,
        ),
        false,
    )
}

/// Only typed libgit2 or the daemon's existing clone-auth classification.
/// Generic process output, network and missing-repository errors stay unchanged.
pub(crate) fn git_error(error: Error, origin: Option<&str>) -> Error {
    if !matches!(
        error,
        Error::GitAuthorization(_)
            | Error::CloneFailed {
                category: intent_core::CloneErrorCategory::AuthRequired,
                ..
            }
    ) {
        return error;
    }
    let host = origin
        .and_then(intent_core::GitRemoteUrl::parse)
        .map(|u| u.host().to_ascii_lowercase());
    let provider = host.as_deref().and_then(|h| match h {
        "github.com" => Some("github".into()),
        "gitlab.com" => Some("gitlab".into()),
        _ => None,
    });
    let github_https = host.as_deref() == Some("github.com")
        && origin.is_some_and(|url| {
            url.get(..8)
                .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
        });
    authorization_error(
        error,
        ExecutionAuthorizationFailure::new(
            ExecutionResource::Git,
            ExecutionAuthorizationReason::Rejected,
            provider,
            host,
        ),
        github_https,
    )
}

/// Keep the diagnostic audience across a detached worker, without substituting
/// execution identity for human authorship or changing the daemon caller. The
/// metadata principal was stamped by the service; queued work uses that stamp.
pub(crate) fn background_execution<T: Send + 'static>(
    services: Services,
    principal: Option<intent_core::PrincipalId>,
    future: impl Future<Output = T> + Send + 'static,
) -> impl Future<Output = T> + Send + 'static {
    let captured = EXECUTION_CALL.try_with(Clone::clone).ok();
    async move {
        let context = if let Some(context) = captured {
            context
        } else {
            let member = match principal {
                Some(id) => services
                    .store
                    .get_host_role(&id)
                    .await
                    .is_ok_and(|r| r == HostRole::Member),
                None => false,
            };
            ExecutionCall { services, member }
        };
        EXECUTION_CALL.scope(context, future).await
    }
}

fn github_execution_host(api_base_url: &str) -> Option<String> {
    let url = reqwest::Url::parse(api_base_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    Some(if host == "api.github.com" {
        "github.com".into()
    } else {
        host
    })
}
