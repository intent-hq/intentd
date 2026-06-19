//! intent-sourcecontrol — provider-agnostic forge abstraction (§3.1, §7).
//!
//! Depends on `intent-core` only among workspace crates (§3.2). It defines the
//! host-agnostic [`SourceControl`] trait (the remote *forge* API — pull/merge
//! requests, reviews, comments, check-runs, mergeability, issues), the v1
//! [`GitHubSourceControl`] implementation backed by `octocrab`, and a
//! [`SourceControlRegistry`] that builds the active implementation from
//! settings (§7.4). Local git operations stay in `intent-git` (§9.5).
//!
//! No `pr.*` wire methods or routing live here — those map onto this trait in a
//! later milestone (§7.5).

pub mod error;
pub mod github;
pub mod model;
pub mod registry;
pub mod token;

use async_trait::async_trait;

pub use error::{Error, Result};
pub use github::GitHubSourceControl;
pub use model::{
    AuthStatus, CheckRun, CheckState, Comment, CommentAnchor, Issue, IssueQuery, MergeMethod,
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, PrPatch, PrQuery, PrState,
    PullRequest, RepoRef, Review, ReviewComment, ReviewThread, ReviewThreadComment, ReviewVerdict,
    ScCapabilities,
};
pub use registry::{GithubSettings, SourceControlRegistry, SourceControlSettings};
pub use token::TokenSource;

/// The provider-agnostic forge API (§7.2).
///
/// `services` consumes only `Arc<dyn SourceControl>`, never `octocrab`
/// directly, so a new host is added by writing one implementation. Hosts
/// advertise [`ScCapabilities`] so the FE can gate UI on what the active
/// provider supports; operations a host cannot perform return
/// [`Error::Unsupported`]. "PR" is host-agnostic (PR / MR / change-request).
#[async_trait]
pub trait SourceControl: Send + Sync {
    /// Stable id of the provider, e.g. `"github"`.
    fn provider_id(&self) -> &'static str;

    /// Capabilities the active host supports (FE can gate UI on these).
    fn capabilities(&self) -> ScCapabilities;

    /// Auth / connectivity probe (used by `settings`/`doctor`).
    async fn check_auth(&self) -> Result<AuthStatus>;

    // --- Pull/merge requests ---

    /// Open a new pull/merge request.
    async fn create_pr(&self, repo: &RepoRef, input: NewPullRequest) -> Result<PullRequest>;

    /// Fetch a single pull request by number.
    async fn get_pr(&self, repo: &RepoRef, number: u64) -> Result<PullRequest>;

    /// List pull requests matching `query`.
    async fn list_prs(&self, repo: &RepoRef, query: PrQuery) -> Result<Vec<PullRequest>>;

    /// Apply a partial update to a pull request.
    async fn update_pr(&self, repo: &RepoRef, number: u64, patch: PrPatch) -> Result<PullRequest>;

    /// Merge a pull request using `method` and optional commit overrides.
    async fn merge_pr(
        &self,
        repo: &RepoRef,
        number: u64,
        method: MergeMethod,
        options: MergeOptions,
    ) -> Result<MergeOutcome>;

    /// Mergeability detail for a pull request.
    async fn mergeability(&self, repo: &RepoRef, number: u64) -> Result<Mergeability>;

    /// Update the PR branch from its base (`pr.updateBranch`).
    async fn update_branch(&self, repo: &RepoRef, number: u64) -> Result<()>;

    // --- Reviews & comments ---

    /// Submit a review (approve / request-changes / comment).
    async fn submit_review(
        &self,
        repo: &RepoRef,
        number: u64,
        verdict: ReviewVerdict,
        body: Option<String>,
    ) -> Result<Review>;

    /// List submitted reviews for a pull request.
    async fn list_reviews(&self, repo: &RepoRef, number: u64) -> Result<Vec<Review>>;

    /// List issue/PR (conversation) comments.
    async fn list_comments(&self, repo: &RepoRef, number: u64) -> Result<Vec<Comment>>;

    /// Add a conversation comment, or a line-anchored review comment when an
    /// `anchor` is supplied.
    async fn add_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        body: &str,
        anchor: Option<CommentAnchor>,
    ) -> Result<Comment>;

    /// List line-anchored review comments on a pull request.
    async fn list_review_comments(&self, repo: &RepoRef, number: u64)
        -> Result<Vec<ReviewComment>>;

    /// Reply to an existing review comment.
    async fn reply_to_review_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        comment_id: u64,
        body: &str,
    ) -> Result<ReviewComment>;

    /// Fetch review threads (GraphQL on GitHub).
    async fn get_review_threads(&self, repo: &RepoRef, number: u64) -> Result<Vec<ReviewThread>>;

    /// Resolve a review thread; returns the resulting resolved state.
    async fn resolve_thread(&self, thread_id: &str) -> Result<bool>;

    /// Unresolve a review thread; returns the resulting resolved state.
    async fn unresolve_thread(&self, thread_id: &str) -> Result<bool>;

    // --- CI / checks ---

    /// Check-runs for a git ref/sha.
    async fn check_runs(&self, repo: &RepoRef, git_ref: &str) -> Result<Vec<CheckRun>>;

    // --- Issues (optional; gated by capabilities) ---

    /// Create an issue.
    async fn create_issue(&self, repo: &RepoRef, title: &str, body: Option<&str>) -> Result<Issue>;

    /// Fetch a single issue by number.
    async fn get_issue(&self, repo: &RepoRef, number: u64) -> Result<Issue>;

    /// List issues matching `query` (PRs excluded).
    async fn list_issues(&self, repo: &RepoRef, query: IssueQuery) -> Result<Vec<Issue>>;
}
