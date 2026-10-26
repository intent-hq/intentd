//! The host-execution call context shared by runtime and agents: the
//! task-local caller scope (`execution_call`, `background_execution`) and the
//! classified authorization errors built inside it (`forge_error`,
//! `ai_authorization_error`, `git_error`). Moved out of `host_execution`
//! unchanged so callers outside runtime do not reach into it.

use crate::{settings_registry::SettingsRegistry, Services};
use intent_core::execution::{
    ExecutionAuthorizationFailure, ExecutionAuthorizationReason, ExecutionResource,
    GIT_HELPER_SETTING,
};
use intent_core::Result;
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
    if github_https && !expose_git_credential(context.services.settings_registry.as_deref()) {
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
    // Boxed here, in this synchronous frame: `TaskLocalFuture::poll` keeps an
    // `Option<F>`-sized temporary in its debug poll frame, so an unboxed
    // worker future (the agent turn path) would be duplicated on the stack of
    // every poll. See `crate::stack_frame`.
    let future = Box::pin(future);
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

pub(crate) fn github_execution_host(api_base_url: &str) -> Option<String> {
    let url = reqwest::Url::parse(api_base_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    Some(if host == "api.github.com" {
        "github.com".into()
    } else {
        host
    })
}

/// The `exposeGitCredentialToChildren` gate. `None` (registry not wired —
/// minimal/test compositions) reads as **off** so bare spawns never trigger
/// token resolution; the production composition root always wires the
/// registry, where the schema default (`true`) applies. Shared with the
/// `system.gitCredential` UDS RPC (see [`crate::github_git_credential`]).
pub(crate) fn expose_git_credential(settings: Option<&SettingsRegistry>) -> bool {
    if intent_core::process_policy::ProcessPolicy::current().private_test_profile() {
        return false;
    }
    settings.is_some_and(|r| {
        r.snapshot()
            .effective
            .source_control
            .github
            .expose_git_credential_to_children
    })
}
