//! Planned additive review-monitor contract. No runtime capability is enabled here.
//!
//! Reuses `ReviewTarget` and the existing monitor lifecycle. These declarations
//! do not resolve remotes, canonicalize provider identities, authorize callers,
//! register watches, or turn deserialized binding evidence into a read lease.
//! Signal availability and checklists keep the existing qualified snapshot
//! projection (`intent-sourcecontrol::ReviewAvailability` in the producer).

use serde::{Deserialize, Serialize};

use crate::{
    AgentId, ExecutionScope, PrMonitorId, PrMonitorState, RepositoryConnectionScope,
    RepositoryProvider, RepositoryResourceKind, ReviewTarget, WorkspaceGitRootId, WorkspaceId,
};

/// Optional `client.hello` server capability. Absence/zero means unsupported.
/// Do not emit version 1 until routing, polling, restoration and private egress
/// all support this contract. The agent feature toggle remains an independent gate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMonitoringCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_monitoring: Option<u32>,
}

impl ReviewMonitoringCapabilities {
    #[must_use]
    pub fn supports_review_monitoring(&self) -> bool {
        self.review_monitoring.is_some_and(|version| version >= 1)
    }
}

/// Planned flattened MCP arguments, shared by both namespace spellings.
/// `repo` is the legacy GitHub-only override. The three addressing options are
/// mutually exclusive; omission resolves the current authorized primary root.
/// Account, daemon, caller and ownership are never accepted as input fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "ReviewMonitorRequestWire")]
pub struct ReviewMonitorRequest {
    pub pr_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_root_id: Option<WorkspaceGitRootId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<ReviewTarget>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReviewMonitorRequestWire {
    pr_number: u64,
    // Legacy null means omitted. New selectors reject null so a malformed
    // explicit address cannot quietly select the current repository instead.
    repo: Option<String>,
    #[serde(default, deserialize_with = "present")]
    git_root_id: Option<WorkspaceGitRootId>,
    #[serde(default, deserialize_with = "present")]
    target: Option<ReviewTarget>,
}

fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl TryFrom<ReviewMonitorRequestWire> for ReviewMonitorRequest {
    type Error = &'static str;

    fn try_from(value: ReviewMonitorRequestWire) -> Result<Self, Self::Error> {
        let request = Self {
            pr_number: value.pr_number,
            repo: value.repo,
            git_root_id: value.git_root_id,
            target: value.target,
        };
        request.address()?;
        Ok(request)
    }
}

/// Validated addressing intent, not a resolved or admitted repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewMonitorAddress {
    LegacyGithub {
        repo: String,
    },
    SelectedRoot {
        git_root_id: Option<WorkspaceGitRootId>,
    },
    Explicit {
        target: ReviewTarget,
    },
}

impl ReviewMonitorRequest {
    /// Validate again at a service boundary even for directly constructed values.
    /// Canonical URL/project validation and root ownership belong to the provider
    /// and original service admission, not this structural argument contract.
    ///
    /// # Errors
    /// Rejects invalid numbers, competing addresses and non-review targets.
    pub fn address(&self) -> Result<ReviewMonitorAddress, &'static str> {
        if self.pr_number == 0 || self.pr_number > i64::MAX.cast_unsigned() {
            return Err("prNumber must be a positive signed 64-bit integer");
        }
        let addresses = usize::from(self.repo.is_some())
            + usize::from(self.git_root_id.is_some())
            + usize::from(self.target.is_some());
        if addresses > 1 {
            return Err("use only one of repo, gitRootId or target");
        }
        if let Some(repo) = &self.repo {
            return Ok(ReviewMonitorAddress::LegacyGithub { repo: repo.clone() });
        }
        if (self.git_root_id.is_some() || self.target.is_some())
            && self.pr_number > 9_007_199_254_740_991
        {
            return Err("addressed prNumber must be a JavaScript-safe positive integer");
        }
        if let Some(root) = &self.git_root_id {
            if root.as_str().trim().is_empty() || root.as_str().chars().any(char::is_control) {
                return Err("gitRootId must identify a registered root");
            }
        }
        if let Some(target) = &self.target {
            let compatible = matches!(
                (target.repository.provider, target.kind),
                (
                    RepositoryProvider::Github,
                    RepositoryResourceKind::PullRequest
                ) | (
                    RepositoryProvider::Gitlab,
                    RepositoryResourceKind::MergeRequest
                )
            );
            let repository = &target.repository;
            if !compatible
                || target.number != self.pr_number
                || repository.instance_base_url.trim().is_empty()
                || repository.instance_base_url.len() > 2048
                || repository.instance_base_url.chars().any(char::is_control)
                || repository.project_path.trim().is_empty()
                || repository.project_path.len() > 1024
                || repository.project_path.chars().any(char::is_control)
            {
                return Err("target must identify the requested pull request or merge request");
            }
            return Ok(ReviewMonitorAddress::Explicit {
                target: target.clone(),
            });
        }
        Ok(ReviewMonitorAddress::SelectedRoot {
            git_root_id: self.git_root_id.clone(),
        })
    }
}

/// Private durable binding evidence, stored beside the existing snapshot envelope.
/// Never a public event field or an authorization parameter. Services capture it
/// from the original host/caller and settled connection; restart must re-admit it.
/// Missing evidence remains unresolved, never filled from a changed default.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMonitorBinding {
    pub scope: ExecutionScope,
    pub connection: RepositoryConnectionScope,
    pub resource: ReviewTarget,
}

/// Monitoring health, independent of both lifecycle and merge readiness.
/// Unknown future state words fail closed to Unknown. Existing row.lastError
/// supplies sanitized explanation; this does not require an additional UI row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "kebab-case",
    rename_all_fields = "camelCase"
)]
pub enum ReviewMonitorAvailability {
    Available,
    Pending,
    ReconnectRequired,
    Backoff {
        retry_at: String,
    },
    Restricted,
    Disabled,
    Unsupported,
    Unavailable,
    Unresolved {
        reason: ReviewMonitorUnresolvedReason,
    },
    #[serde(other)]
    Unknown,
}

/// Wire words corresponding to the existing Store qualification outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewMonitorUnresolvedReason {
    MissingProvenance,
    UnverifiedImport,
    InvalidTarget,
    /// Durable target exists but the original host/account admission is missing.
    MissingAuthority,
    #[serde(other)]
    Unknown,
}

/// Optional public additions shared by list rows, lifecycle events and wakes.
/// No resource is asserted for unresolved provenance. Legacy fields remain in
/// their existing envelope; canonical resource.number equals its prNumber.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMonitorExtension {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<ReviewTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitor_availability: Option<ReviewMonitorAvailability>,
}

/// Existing prMonitor:* event payload plus optional qualified identity/health.
/// Snapshot/title/private authority never travel in this event DTO. Consumers
/// refetch the same list after health changes; no second MR event family exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMonitorEventData {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub monitor_id: PrMonitorId,
    pub repo: String,
    pub pr_number: u64,
    pub state: PrMonitorState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adopted_from: Option<AgentId>,
    #[serde(flatten)]
    pub extension: ReviewMonitorExtension,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReviewMonitorWakeType {
    #[serde(rename = "pr_monitor_wake")]
    PrMonitorWake,
}

/// Existing queued/persisted wake attribution. Both aliases retain this type
/// and prNumber; MR terminology belongs to presentation, not a renamed wire key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewMonitorWakeMetadata {
    pub r#type: ReviewMonitorWakeType,
    pub monitor_id: PrMonitorId,
    pub repo: String,
    pub pr_number: u64,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adopted_by: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_until: Option<String>,
    #[serde(flatten)]
    pub extension: ReviewMonitorExtension,
}

/// Per-recipient support, independent of host support and authorization.
/// Construct from the current connection hello or trusted agent harness receipt,
/// never an agent-authored request, another connection, or a persisted wake.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReviewMonitorRecipientSupport {
    qualified_v1: bool,
}

impl ReviewMonitorRecipientSupport {
    /// Forwarders must not substitute their support for an old downstream reader.
    #[must_use]
    pub const fn intersect(self, downstream: Self) -> Self {
        Self {
            qualified_v1: self.qualified_v1 && downstream.qualified_v1,
        }
    }

    /// Parse only the negotiated capability object. Missing/malformed versions
    /// fail closed; future integer versions promise the version-1 subset.
    #[must_use]
    pub fn from_capabilities(capabilities: &serde_json::Value) -> Self {
        Self {
            qualified_v1: capabilities
                .get("reviewMonitoring")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|version| version >= 1),
        }
    }
}

/// Internal producer classification, not a wire field or caller-controlled tag.
/// `LegacyGithub` means output from the UNCHANGED legacy GitHub producer, not a
/// qualified GitHub observation stripped of resource/availability. Unresolved
/// provenance, nullable qualified facts and every GitLab observation are Qualified.
/// T is the whole output envelope, including body/URL/metadata on queued wakes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewMonitorOutput<T> {
    LegacyGithub(T),
    Qualified(T),
}

impl<T> ReviewMonitorOutput<T> {
    /// Pure planned egress policy. No runtime route calls this declaration yet.
    /// Apply after fresh authority admission, to every list entry, event, waiting
    /// entry, direct response and entire wake/replay envelope. None means withhold,
    /// never acknowledge delivery or erase durable ownership/pending changes.
    /// Direct operations instead return the generic unsupported error before any
    /// side effect; list/event projections omit withheld items. See protocol docs.
    #[must_use]
    pub fn project(
        self,
        host: &ReviewMonitoringCapabilities,
        recipient: ReviewMonitorRecipientSupport,
    ) -> Option<T> {
        match self {
            Self::LegacyGithub(payload) => Some(payload),
            Self::Qualified(payload)
                if host.supports_review_monitoring() && recipient.qualified_v1 =>
            {
                Some(payload)
            }
            Self::Qualified(_) => None,
        }
    }
}
