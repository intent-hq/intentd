//! intent-git — libgit2/gix wrappers + worktree locking (§3.1).
//!
//! Depends on `intent-core` only (§3.2). Stub only — git2/gix is pulled when
//! git operations land in Wave 3.

pub use intent_core::Result;

pub mod status {
    //! Working-tree status — stub.
}

pub mod commit {
    //! Stage + commit operations — stub.
}

pub mod branches {
    //! Branch listing / management — stub.
}

pub mod worktree {
    //! Worktree create + lock — stub.
}
