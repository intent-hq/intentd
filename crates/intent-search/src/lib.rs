//! intent-search — BE-owned `search.*` namespace (§14).
//!
//! Depends on `intent-core` (errors) and `intent-store` (§3.2), plus the
//! ripgrep libraries (`grep`/`ignore`/`globset`). This slice implements the
//! file-based methods (`search.inFiles`, `search.fileNames`) over a
//! gitignore-aware worktree walk, plus the per-request cancellation registry
//! that backs `search.cancel`. Store-backed adapters (sessions/events/
//! memories/notes/codebase) land later.

pub use intent_core::Result;

mod glob;
mod util;

pub mod cancel;
pub mod content;
pub mod paths;

pub mod adapters {
    //! Adapters over persisted sessions/events/memories/notes/codebase — stub.
}

pub use cancel::{mint_request_id, CancelRegistry, CancelToken};
pub use content::{search_in_files, ContentSearchResult, SearchMatch, SearchOpts};
pub use paths::{search_file_names, FileNameResult};

#[cfg(test)]
mod tests;
