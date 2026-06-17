//! intent-store — SQLite + file-tree persistence (§3.1).
//!
//! Depends on `intent-core` only (§3.2). Stub only — no database or file I/O
//! yet; heavy deps (sqlx/rusqlite) are pulled in Wave 3.

pub use intent_core::{Error, Result};

pub mod pool {
    //! Async connection pool + runtime handle (sqlx/rusqlite) — stub.
}

pub mod migrations {
    //! Embedded schema migrations (§9.4) — stub.
}

pub mod repositories {
    //! Per-entity repository types (notes/tasks/agents/…) — stub.
}

pub mod layout {
    //! On-disk workspace file layout — stub.
}

pub mod locking {
    //! Worktree / row locking primitives — stub.
}
