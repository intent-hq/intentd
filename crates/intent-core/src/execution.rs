//! Non-secret projection and recovery for execution on a shared host.

use serde::{Deserialize, Serialize};

pub const GIT_HELPER_SETTING: &str = "sourceControl.github.exposeGitCredentialToChildren";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCredentialPolicy {
    pub provider: String,
    pub protocol: String,
    pub host: String,
    pub managed_helper_enabled: bool,
    pub setting: String,
}

impl GitCredentialPolicy {
    #[must_use]
    pub fn github_https(enabled: bool) -> Self {
        Self {
            provider: "github".into(),
            protocol: "https".into(),
            host: "github.com".into(),
            managed_helper_enabled: enabled,
            setting: GIT_HELPER_SETTING.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryConnection {
    pub provider: String,
    pub host: String,
    pub configured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostExecutionContext {
    pub default_provider_id: Option<String>,
    pub default_model_id: Option<String>,
    /// Canonical execution-policy IDs, independent of installation or readiness.
    pub enabled_provider_ids: Vec<String>,
    pub repository_connections: Vec<RepositoryConnection>,
    pub git_credential_policy: GitCredentialPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionResource {
    Git,
    Ai,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionAuthorizationReason {
    Missing,
    Rejected,
    InsufficientScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRecovery {
    pub actor: String,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setting: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionAuthorizationFailure {
    pub resource: ExecutionResource,
    pub reason: ExecutionAuthorizationReason,
    pub provider_id: Option<String>,
    pub host: Option<String>,
    pub recovery: ExecutionRecovery,
}

impl ExecutionAuthorizationFailure {
    #[must_use]
    pub fn message(&self) -> String {
        let kind = match self.resource {
            ExecutionResource::Git => "Git",
            ExecutionResource::Ai => "AI",
        };
        let mut message = format!("The connected host needs {kind} authorization. Ask its owner to check the execution account and access, then retry.");
        if self.recovery.setting.is_some() {
            message.push_str(" This host's owner disabled Intent's managed GitHub credential helper with sourceControl.github.exposeGitCredentialToChildren. Ask the owner to review that setting or the host's other Git authorization.");
        }
        message
    }

    #[must_use]
    pub fn new(
        resource: ExecutionResource,
        reason: ExecutionAuthorizationReason,
        provider_id: Option<String>,
        host: Option<String>,
    ) -> Self {
        Self {
            resource,
            reason,
            provider_id,
            host,
            recovery: ExecutionRecovery {
                actor: "host-owner".into(),
                action: match resource {
                    ExecutionResource::Git => "check-git-authorization",
                    ExecutionResource::Ai => "check-ai-authorization",
                }
                .into(),
                setting: None,
            },
        }
    }
}
