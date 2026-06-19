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

/// Verdict carried by a submitted review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReviewVerdict {
    Approve,
    RequestChanges,
    Comment,
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

/// Filter for listing pull requests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrQuery {
    pub state: Option<PrState>,
    pub base: Option<String>,
    pub head: Option<String>,
    pub author: Option<String>,
    pub limit: Option<u8>,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IssueQuery {
    pub state: Option<String>,
    pub labels: Option<String>,
    pub limit: Option<u8>,
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
