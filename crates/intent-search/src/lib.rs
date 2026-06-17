//! intent-search — BE-owned `search.*` namespace (§14).
//!
//! Depends on `intent-core` and `intent-store` (§3.2). Stub only — the ripgrep
//! libraries (grep/ignore/globset) are pulled when search lands in Wave 3.

pub use intent_core::Result;

pub mod content {
    //! ripgrep-equivalent content search (grep + ignore + globset) — stub.
}

pub mod paths {
    //! Path / glob search (`search.fileNames`) — stub.
}

pub mod adapters {
    //! Adapters over persisted sessions/events/memories/notes/codebase — stub.
}

pub mod cancel {
    //! Per-request cancellation keyed by `requestId` — stub.
}
