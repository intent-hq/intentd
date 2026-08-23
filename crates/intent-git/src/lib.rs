//! intent-git — git2 (libgit2) wrappers + worktree locking (§3.1, §9.5).
//!
//! Depends on `intent-core` only (§3.2). Provides the read/stage git operations
//! exposed over the wire (`git.status`, `git.stage`, `git.getBranches`) plus the
//! internal diff and worktree-lock helpers Cycle C consumes. Local git
//! operations live here, never in `intent-sourcecontrol` (the forge trait, §7).

use intent_core::Error;

pub use intent_core::{
    FileStatus, GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult, GitFileStatus,
    GitMergeConflicts, GitPullResult, GitStatus, Result,
};

pub use cow::{cow_clone, cow_probe, CowSupport, TEST_COW_CLONE_UNSUPPORTED_PATH_ENV};

pub mod auth;
pub mod branches;
pub mod commit;
pub mod conflicts;
pub mod cow;
pub mod cow_checkout;
pub mod diff;
pub mod fetch;
pub mod fs_remove;
pub mod history;
pub mod ls_remote;
pub mod pull;
pub mod push;
pub mod rebase;
pub mod redact;
pub mod refs;
pub mod remote;
pub mod repo_cache;
pub mod reset;
pub mod show;
pub mod squash;
pub mod stage;
pub mod stash;
pub mod status;
pub mod submodule;
pub mod worktree;

#[cfg(test)]
mod testutil;

/// Threshold above which a single git read operation logs one warn-level line
/// so slow git work on large repos is visible in production (#963).
pub(crate) const SLOW_GIT_WARN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(5);

/// Map a libgit2 error into the domain [`Error::Internal`] (`-32603`).
// By-value so it slots point-free into `map_err(map_git_err)` everywhere.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn map_git_err(e: git2::Error) -> Error {
    Error::Internal(e.message().to_string())
}

/// Whether `path` points at a git repository (a repo root or a linked
/// worktree). Read-only probe backing the `repoPath` validation of the
/// path-based branch reads (`git.getBranches`, `git.branchStatus`) in
/// `intent-services`.
#[must_use]
pub fn is_repository(path: &std::path::Path) -> bool {
    git2::Repository::open(path).is_ok()
}

/// Whether a libgit2 error represents a merge/checkout conflict, mirroring the TS
/// `message.includes('conflict')` classification (used to tell a conflicting
/// rebase/stash-pop apart from an unrelated failure). Checks the structured error
/// code first, then falls back to a case-insensitive message probe.
pub(crate) fn is_conflict_error(e: &git2::Error) -> bool {
    use git2::ErrorCode;
    matches!(e.code(), ErrorCode::Conflict | ErrorCode::MergeConflict)
        || e.message().to_lowercase().contains("conflict")
}
