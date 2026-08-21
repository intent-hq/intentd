//! Host-agnostic data models for the forge API (§7.2).
//!
//! Timestamps are kept as RFC-3339 strings (parity with the TS ground truth,
//! which passes ISO strings straight through) rather than a datetime type, so
//! the crate stays free of a chrono/time coupling. Field names serialize as
//! `camelCase` to match the existing FE wire shapes.

use serde::{Deserialize, Serialize};

/// Identifies a repository on a forge (host-agnostic).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoRef {
    pub owner: String,
    pub name: String,
}

impl RepoRef {
    /// Convenience constructor.
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }
}

/// Repository metadata (parity with the FE `GithubRepo`). Backs the
/// `github.repos.list/search/get` browse surface. `url` carries GitHub's
/// `html_url`; absent optionals are omitted from the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Repo {
    pub owner: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// A remote branch (`GET /repos/{owner}/{name}/branches` item).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Branch {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default)]
    pub protected: bool,
}

/// Pagination input for the forge list reads (§5.5 uniform pagination).
///
/// `limit` is the server-clamped page size (`1..=200`; the default is applied by
/// the services layer). `cursor` is an **engine-native** continuation token — a
/// REST page number (`"2"`) or a GraphQL end-cursor — not the opaque base64
/// `nextToken` exposed on the wire; the services layer wraps/unwraps that.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageParams {
    pub limit: u8,
    pub cursor: Option<String>,
}

impl PageParams {
    /// A first-page request of `limit` items (no continuation cursor).
    #[must_use]
    pub fn first(limit: u8) -> Self {
        Self {
            limit,
            cursor: None,
        }
    }
}

/// A page of forge items plus the engine-native cursor for the next page
/// (`None` on the last page). The services layer projects `items` onto the wire
/// DTOs and wraps `next_cursor` into the opaque base64 `nextToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Normalized pull/merge/change-request state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrState {
    Open,
    Closed,
    Merged,
}

/// Merge strategy requested for a pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MergeMethod {
    Merge,
    Squash,
    Rebase,
}

/// Verdict carried by a submitted review. Wire values are kebab-case
/// (`approve` / `request-changes` / `comment`, §5.18 `Review`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReviewVerdict {
    Approve,
    RequestChanges,
    Comment,
}

/// The forge's authoritative review-requirement verdict for a pull request
/// (GitHub GraphQL `reviewDecision`). `None` at the trait level means the
/// forge reports no review requirement (unprotected base) or the host does
/// not support the signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecision {
    Approved,
    ChangesRequested,
    ReviewRequired,
}

/// Normalized CI check-run state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckState {
    Pending,
    Success,
    Failure,
    Neutral,
    Cancelled,
}

/// A pull/merge/change request, normalized across hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequest {
    pub number: u64,
    pub url: String,
    pub title: String,
    pub body: Option<String>,
    pub state: PrState,
    pub draft: bool,
    pub source_branch: String,
    pub target_branch: String,
    pub author: String,
    pub mergeable: Option<bool>,
    /// Raw forge mergeability state (e.g. GitHub `mergeable_state`:
    /// `clean`/`dirty`/`blocked`/`unstable`/`behind`/`unknown`). Powers the
    /// `pr.status` summary parity; `None` when the forge does not expose it.
    pub mergeable_state: Option<String>,
    /// SHA of the head commit (race protection; default `pr.listCheckRuns` ref).
    pub head_sha: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Input for opening a pull request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewPullRequest {
    pub title: String,
    pub body: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    #[serde(default)]
    pub draft: bool,
}

/// Partial update applied to a pull request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrPatch {
    pub title: Option<String>,
    pub body: Option<String>,
    pub target_branch: Option<String>,
    pub draft: Option<bool>,
    pub state: Option<PrState>,
}

/// Involvement of the authenticated user (`@me`) for PR search. Parity with the
/// FE `searchGitHubPullRequests` `filter` values; the FE `all` filter carries no
/// involvement constraint and maps to `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PrInvolvement {
    /// `author:@me`.
    Created,
    /// `assignee:@me`.
    Assigned,
    /// `review-requested:@me`.
    ReviewRequested,
    /// `involves:@me`.
    Involves,
}

/// Filter for listing pull requests.
///
/// When `involvement` or `search` is set, listing routes through
/// `GET /search/issues` (`is:pr repo:o/r is:<state> [<filter>:@me] [<text>]`)
/// so callers can express author/assignee/review-requested/involves @me and
/// free text; otherwise the plain `GET /repos/{o}/{r}/pulls` path is used with
/// client-side `author` filtering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrQuery {
    pub state: Option<PrState>,
    pub base: Option<String>,
    pub head: Option<String>,
    pub author: Option<String>,
    pub involvement: Option<PrInvolvement>,
    /// Free-text search term appended to the `/search/issues` query; a
    /// non-blank value routes the listing through `/search/issues` even
    /// without `involvement`. Blank/absent leaves listing behavior unchanged.
    pub search: Option<String>,
    pub limit: Option<u8>,
    /// Engine-native continuation cursor (a REST page number); `None` is the
    /// first page. The opaque wire `nextToken` is owned by the services layer.
    pub cursor: Option<String>,
}

/// Options carried by a merge request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeOptions {
    pub commit_title: Option<String>,
    pub commit_message: Option<String>,
}

/// Outcome of a merge operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeOutcome {
    pub merged: bool,
    pub message: String,
    pub sha: Option<String>,
}

/// Mergeability detail for a pull request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mergeability {
    pub mergeable: Option<bool>,
    pub conflicts: bool,
    pub required_checks_passed: bool,
}

/// A submitted review on a pull request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Review {
    pub author: String,
    pub verdict: ReviewVerdict,
    pub body: Option<String>,
    pub submitted_at: String,
}

/// A conversation (issue/PR) comment, optionally line-anchored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub id: String,
    pub author: String,
    pub body: String,
    pub path: Option<String>,
    pub line: Option<u64>,
    pub created_at: String,
    /// Forge web URL of the comment (GitHub `html_url`); `None` when the host
    /// does not surface one. Powers the `pr.postComment` `htmlUrl` reply.
    pub url: Option<String>,
}

/// Where a review comment is anchored in the diff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentAnchor {
    pub path: String,
    pub line: u64,
    pub side: Option<String>,
}

/// A line-anchored review comment (REST `pulls/{n}/comments`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewComment {
    pub id: u64,
    pub body: String,
    pub path: String,
    pub line: Option<u64>,
    pub author: String,
    pub created_at: String,
    pub updated_at: String,
    pub in_reply_to_id: Option<u64>,
    pub url: String,
}

/// A single comment within a review thread (GraphQL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewThreadComment {
    pub id: String,
    pub body: String,
    pub author: String,
    pub path: String,
    pub line: Option<u64>,
    pub created_at: String,
}

/// A review thread with its resolution state (GraphQL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewThread {
    pub id: String,
    pub is_resolved: bool,
    pub comments: Vec<ReviewThreadComment>,
}

/// A CI check-run for a git ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckRun {
    pub name: String,
    pub state: CheckState,
    pub url: Option<String>,
}

/// An issue (PRs excluded; gated by capabilities).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    pub number: u64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub url: String,
}

/// Filter for listing issues.
///
/// When `search` is set, listing routes through `GET /search/issues`
/// (`is:issue repo:o/r [state:<state>] <text>`) so callers can express free
/// text; otherwise the plain `GET /repos/{o}/{r}/issues` listing is used.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueQuery {
    pub state: Option<String>,
    pub labels: Option<String>,
    /// Free-text search term; a non-blank value routes the listing through
    /// `/search/issues`. Blank/absent leaves listing behavior unchanged.
    pub search: Option<String>,
    pub limit: Option<u8>,
    /// Engine-native continuation cursor (a REST page number); `None` is the
    /// first page. The opaque wire `nextToken` is owned by the services layer.
    pub cursor: Option<String>,
}

/// Authenticated GitHub identity (`GET /user`). Only non-sensitive identity
/// fields are surfaced — the credential/token is never included. `avatarUrl` /
/// `htmlUrl` mirror GitHub's `avatar_url` / `html_url`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserIdentity {
    pub login: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub html_url: Option<String>,
}

/// Auth / connectivity probe result (`settings`/`doctor`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthStatus {
    pub authenticated: bool,
    pub login: Option<String>,
    pub scopes: Vec<String>,
}

/// Capabilities a concrete host may or may not support (FE gates UI on these).
// One bool per independent capability; the flat shape IS the wire contract.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScCapabilities {
    pub draft_prs: bool,
    pub squash_merge: bool,
    pub rebase_merge: bool,
    pub review_required_changes: bool,
    pub check_runs: bool,
    pub issues: bool,
}

/// One entry of the forge's status-check rollup for a pull request, carrying
/// the per-check "is this required to merge?" flag GitHub only exposes through
/// GraphQL (`statusCheckRollup.contexts` → `isRequired(pullRequestNumber:)`).
/// Both check-runs and legacy commit statuses collapse onto this shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupCheck {
    pub name: String,
    pub state: CheckState,
    /// Whether the host reports this check as required for merging. `false`
    /// when the host says so *and* when the signal is unavailable — callers
    /// that need the distinction consult [`MergeRequirementSignals`].
    pub is_required: bool,
    pub url: Option<String>,
}

/// Merge-relevant branch rules for a pull request's base branch (GitHub
/// `GET /repos/{owner}/{repo}/rules/branches/{branch}`). Every field is
/// optional/empty when the host does not report that rule; an unreadable rules
/// endpoint yields no [`BranchRules`] at all rather than an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchRules {
    /// Approvals the base branch requires before merging.
    pub required_approving_review_count: Option<u32>,
    /// Whether every review thread must be resolved before merging.
    pub required_conversation_resolution: Option<bool>,
    /// Names of the status checks the base branch requires.
    pub required_status_checks: Vec<String>,
}

/// Per-PR merge-requirement signals a host can report beyond the plain
/// [`PullRequest`] snapshot. Hosts without the signals return
/// [`crate::Error::Unsupported`]; individual fields degrade to `None`/empty
/// when a sub-read is unavailable rather than failing the whole probe.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeRequirementSignals {
    /// Raw forge merge-state status (GitHub GraphQL `mergeStateStatus`:
    /// `CLEAN`/`BLOCKED`/`BEHIND`/`DIRTY`/`UNSTABLE`/`DRAFT`/`HAS_HOOKS`/
    /// `UNKNOWN`). Finer-grained than REST `mergeable_state`.
    pub merge_state_status: Option<String>,
    /// The forge's authoritative review verdict, same value the standalone
    /// [`crate::SourceControl::review_decision`] read returns.
    pub review_decision: Option<ReviewDecision>,
    /// The PR head's status-check rollup, with per-check `isRequired`.
    pub checks: Vec<RollupCheck>,
    /// Whether the rollup's `isRequired` flags are trustworthy — `false` when
    /// the host did not report the rollup at all.
    pub checks_known: bool,
    /// Base-branch rules, or `None` when they are unreadable (missing scope,
    /// unsupported endpoint) — a degraded but non-fatal probe.
    pub branch_rules: Option<BranchRules>,
}
