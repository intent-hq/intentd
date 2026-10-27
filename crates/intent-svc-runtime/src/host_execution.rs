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

use intent_services::feature_api::{
    events::SubscriptionFilter, publish_event, system_actor, Services,
};

/// The host execution context snapshot and its change loop, on [`Services`].
pub(crate) trait HostExecutionExt {
    async fn execution_context_snapshot(&self) -> Result<HostExecutionContext>;
    async fn publish_execution_context_changed(&self);
    #[must_use]
    fn spawn_execution_context_loop(&self) -> tokio::task::JoinHandle<()>;
}

impl HostExecutionExt for Services {
    /// Presence only, with no forge request or AI provider spawn. The CLI
    /// credential fallback uses the existing bounded token cache. Never read
    /// collaboration-purpose credential slots here.
    async fn execution_context_snapshot(&self) -> Result<HostExecutionContext> {
        let settings = self.effective_settings();
        let agents = self.feature_agent_wake();
        let mut enabled_provider_ids: Vec<_> = intent_providers::all_provider_ids()
            .into_iter()
            .filter(|id| !agents.provider_is_disabled(id, settings.providers.enabled.as_ref()))
            .map(str::to_owned)
            .collect();
        enabled_provider_ids.sort_unstable();
        enabled_provider_ids.dedup();
        let source = settings.source_control.github.token_source;
        let stored = if matches!(
            source,
            GithubTokenSource::Auto | GithubTokenSource::Explicit
        ) {
            self.feature_secrets()
                .load("sourceControl.github.token")
                .await?
                .is_some_and(|s| !s.trim().is_empty())
        } else {
            false
        };
        let github_configured = self.feature_source_control().is_some()
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
            host: intent_services::feature_api::execution_call::github_execution_host(
                &settings.source_control.github.api_base_url,
            )
            .unwrap_or_else(|| "github.com".into()),
            configured: github_configured,
        }];
        if let Some(host) = self.bound_gitlab_host() {
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
                intent_services::feature_api::execution_call::expose_git_credential(
                    self.feature_settings_registry(),
                ),
            ),
        })
    }

    async fn publish_execution_context_changed(&self) {
        let Some(bus) = self.feature_event_bus() else {
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
    fn spawn_execution_context_loop(&self) -> tokio::task::JoinHandle<()> {
        let mut sub = self.feature_event_bus().map(|bus| {
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
                    () = services.feature_execution_invalidation().notified() => {
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
