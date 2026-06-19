//! intent-git — git2 (libgit2) wrappers + worktree locking (§3.1, §9.5).
//!
//! Depends on `intent-core` only (§3.2). Provides the read/stage git operations
//! exposed over the wire (`git.status`, `git.stage`, `git.getBranches`) plus the
//! internal diff and worktree-lock helpers Cycle C consumes. Local git
//! operations live here, never in `intent-sourcecontrol` (the forge trait, §7).

use intent_core::Error;

pub use intent_core::{
    FileStatus, GitAgentCommitResult, GitBranches, GitCommitResult, GitFileStatus,
    GitMergeConflicts, GitStatus, Result,
};

pub mod branches;
pub mod commit;
pub mod conflicts;
pub mod diff;
pub mod stage;
pub mod status;
pub mod worktree;

#[cfg(test)]
mod testutil;

/// Map a libgit2 error into the domain [`Error::Internal`] (`-32603`).
pub(crate) fn map_git_err(e: git2::Error) -> Error {
    Error::Internal(e.message().to_string())
}
