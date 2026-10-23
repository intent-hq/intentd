//! Explicit, bounded repository reads for workspace and pre-workspace consumers.
//!
//! Context and resource identities are request inputs, never permission grants.
//! The original connection owner admits them before provider work and again at
//! delivery. Legacy workspace-only resource methods retain their existing shape.

use serde::{Deserialize, Serialize};

use crate::repository_request::{
    resource_lifetime, resource_target, RepositoryResourceCapture, RepositoryResourceFailure,
    RepositoryResourceQuota, RepositoryResourceResult,
};
use crate::{ExecutionScope, RepositoryContextRevision, RepositoryProvider, RepositoryTarget};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RepositoryReadContext {
    /// Requires the original caller's durable workspace authority.
    Workspace {
        #[serde(rename = "workspaceId")]
        workspace_id: crate::WorkspaceId,
    },
    /// Requires the original physical connection's durable host owner.
    Host {},
}

impl RepositoryReadContext {
    #[must_use]
    pub fn workspace_id(&self) -> Option<&crate::WorkspaceId> {
        match self {
            Self::Workspace { workspace_id } => Some(workspace_id),
            Self::Host {} => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryScopedCaptureQuery {
    pub context: RepositoryReadContext,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryScopedBoundQuery {
    pub context: RepositoryReadContext,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryScopedDetailQuery {
    pub context: RepositoryReadContext,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
    #[serde(deserialize_with = "resource_target")]
    pub target: crate::ReviewTarget,
    #[serde(default)]
    pub refresh: bool,
}

/// An explicit local identity read, never a workspace or remote permission grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryKnownQuery {
    pub context: RepositoryReadContext,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
    #[serde(deserialize_with = "known_path")]
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryKnownSelection {
    pub context: RepositoryReadContext,
    pub read_lifetime_id: String,
    pub scope: ExecutionScope,
    pub revision: RepositoryContextRevision,
    pub path: String,
    pub selection: crate::ReviewSelectionResolution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryListKind {
    MergeRequest,
    Issue,
}

impl RepositoryListKind {
    #[must_use]
    pub const fn resource_kind(self) -> crate::RepositoryResourceKind {
        match self {
            Self::MergeRequest => crate::RepositoryResourceKind::MergeRequest,
            Self::Issue => crate::RepositoryResourceKind::Issue,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryListState {
    #[default]
    All,
    Open,
    /// Merge-request lists include both closed-without-merge and merged.
    Closed,
    Merged,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryListFilter {
    #[default]
    All,
    Created,
    Assigned,
    ReviewRequested,
    /// Accepted as a query; providers without exact semantics report unsupported.
    Involves,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryListQuery {
    pub state: RepositoryListState,
    pub filter: RepositoryListFilter,
    #[serde(deserialize_with = "search")]
    pub search: String,
    #[serde(deserialize_with = "limit")]
    pub limit: u8,
    /// Opaque original-lifetime continuation, never a provider page parameter.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "cursor"
    )]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryResourceListQuery {
    pub context: RepositoryReadContext,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
    #[serde(deserialize_with = "repository")]
    pub repository: RepositoryTarget,
    pub kind: RepositoryListKind,
    pub query: RepositoryListQuery,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryPageQuery {
    #[serde(deserialize_with = "limit")]
    pub limit: u8,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "cursor"
    )]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepositoryDiscussionsQuery {
    pub context: RepositoryReadContext,
    #[serde(deserialize_with = "resource_lifetime")]
    pub read_lifetime_id: String,
    #[serde(deserialize_with = "discussion_target")]
    pub target: crate::ReviewTarget,
    pub page: RepositoryPageQuery,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryScopedCapture {
    pub context: RepositoryReadContext,
    #[serde(flatten)]
    pub capture: RepositoryResourceCapture,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryScopedDetail {
    pub context: RepositoryReadContext,
    #[serde(flatten)]
    pub detail: RepositoryResourceResult,
}

/// Additive page errors do not extend the strict legacy detail error union.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepositoryPageFailure {
    Authentication,
    ProjectDenied,
    ResourceDenied,
    OptionalRestricted,
    OptionalUnavailable,
    RateLimited,
    Transient,
    Unavailable,
    Unsupported,
    Unknown,
}

impl From<RepositoryResourceFailure> for RepositoryPageFailure {
    fn from(value: RepositoryResourceFailure) -> Self {
        match value {
            RepositoryResourceFailure::Authentication => Self::Authentication,
            RepositoryResourceFailure::ProjectDenied => Self::ProjectDenied,
            RepositoryResourceFailure::ResourceDenied => Self::ResourceDenied,
            RepositoryResourceFailure::OptionalRestricted => Self::OptionalRestricted,
            RepositoryResourceFailure::OptionalUnavailable => Self::OptionalUnavailable,
            RepositoryResourceFailure::RateLimited => Self::RateLimited,
            RepositoryResourceFailure::Transient => Self::Transient,
            RepositoryResourceFailure::Unavailable => Self::Unavailable,
            RepositoryResourceFailure::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RepositoryPageOutcome {
    Page {
        /// Original provider DTOs, independently qualified to the request target.
        items: Vec<serde_json::Value>,
        /// Empty items with a continuation is not the end of the collection.
        #[serde(rename = "nextCursor")]
        next_cursor: Option<String>,
        /// Rows withheld because their original observation is no longer valid.
        omitted: usize,
    },
    Failure {
        code: RepositoryPageFailure,
        status: Option<u16>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceList {
    pub context: RepositoryReadContext,
    pub read_lifetime_id: String,
    pub scope: ExecutionScope,
    pub revision: RepositoryContextRevision,
    pub repository: RepositoryTarget,
    pub kind: RepositoryListKind,
    pub query: RepositoryListQuery,
    pub outcome: RepositoryPageOutcome,
    pub quota: RepositoryResourceQuota,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryDiscussions {
    pub context: RepositoryReadContext,
    pub read_lifetime_id: String,
    pub scope: ExecutionScope,
    pub revision: RepositoryContextRevision,
    pub target: crate::ReviewTarget,
    pub page: RepositoryPageQuery,
    pub outcome: RepositoryPageOutcome,
    pub quota: RepositoryResourceQuota,
}

/// Same original socket only; no permission grant or private provider payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryResourceInvalidated {
    pub read_lifetime_ids: Vec<String>,
    pub sequence: String,
    pub repository: RepositoryTarget,
    pub coverage: RepositoryInvalidationCoverage,
}

/// A denied list endpoint cannot invalidate independently readable details.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RepositoryInvalidationCoverage {
    Project {},
    Resource {
        target: crate::ReviewTarget,
    },
    List {
        #[serde(rename = "resourceKind")]
        resource_kind: RepositoryListKind,
    },
}

pub enum RepositoryResourceEvent {
    Retired(crate::repository_request::RepositoryResourceRetired),
    Invalidated(RepositoryResourceInvalidated),
}

fn known_path<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let value = String::deserialize(d)?;
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(serde::de::Error::custom("invalid known repository path"));
    }
    Ok(value)
}

fn limit<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u8, D::Error> {
    let value = u8::deserialize(d)?;
    if !(1..=100).contains(&value) {
        return Err(serde::de::Error::custom("invalid repository page size"));
    }
    Ok(value)
}

fn search<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let value = String::deserialize(d)?;
    if value.len() > 512 || value.chars().any(char::is_control) {
        return Err(serde::de::Error::custom("invalid repository search"));
    }
    Ok(value)
}

fn cursor<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let value = String::deserialize(d)?;
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(serde::de::Error::custom("invalid repository cursor"));
    }
    Ok(Some(value))
}

fn repository<'de, D: serde::Deserializer<'de>>(d: D) -> Result<RepositoryTarget, D::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Target {
        provider: RepositoryProvider,
        instance_base_url: String,
        project_path: String,
    }
    let target = Target::deserialize(d)?;
    if target.provider != RepositoryProvider::Gitlab
        || target.instance_base_url.is_empty()
        || target.instance_base_url.len() > 2048
        || target.project_path.is_empty()
        || target.project_path.len() > 1024
    {
        return Err(serde::de::Error::custom("invalid explicit GitLab project"));
    }
    Ok(RepositoryTarget {
        provider: target.provider,
        instance_base_url: target.instance_base_url,
        project_path: target.project_path,
    })
}

fn discussion_target<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<crate::ReviewTarget, D::Error> {
    let target = resource_target(d)?;
    if target.kind != crate::RepositoryResourceKind::MergeRequest {
        return Err(serde::de::Error::custom(
            "discussions require a merge request",
        ));
    }
    Ok(target)
}

#[cfg(test)]
mod tests;
