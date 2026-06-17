//! intent-sourcecontrol — forge abstraction (§3.1, §7).
//!
//! Depends on `intent-core` only (§3.2). Stub only — octocrab is pulled when
//! the GitHub implementation lands in Wave 3.

pub use intent_core::Result;

/// Forge abstraction over PRs/issues/reviews/check-runs/mergeability.
/// Implemented by `github::GitHubSourceControl` in Wave 3 (§7).
pub trait SourceControl: Send + Sync {}

pub mod github {
    //! octocrab-backed `GitHubSourceControl` implementation — stub.
}
