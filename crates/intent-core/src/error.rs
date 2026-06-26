//! Typed domain errors that map to JSON-RPC error codes (§11.1, PROTOCOL §9).
//!
//! Kept small but principled: `InvalidParams` and `NotFound` both surface as
//! `-32602` (per PROTOCOL §9 "not found" lookups are invalid-params), while
//! `Internal` is `-32603`. Higher layers translate these into JSON-RPC error
//! objects via [`Error::code`].

/// Domain error type for intentd.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A required parameter was missing or malformed.
    #[error("invalid params: {0}")]
    InvalidParams(String),

    /// A requested entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// An unexpected internal failure (I/O, persistence, serialization).
    #[error("internal error: {0}")]
    Internal(String),

    /// A conditional write lost an optimistic-concurrency check: the entity's
    /// current `rev` did not match the supplied `expectedVersion`. Carries the
    /// current entity so the client can reconcile (PROTOCOL §4, §5.6 — surfaced
    /// as `-32005` with `error.data.current`).
    #[error("conflict: version mismatch")]
    Conflict { current: serde_json::Value },
}

impl Error {
    /// JSON-RPC 2.0 numeric error code for this error (PROTOCOL §9).
    pub fn code(&self) -> i32 {
        match self {
            Error::InvalidParams(_) | Error::NotFound(_) => -32602,
            Error::Internal(_) => -32603,
            Error::Conflict { .. } => -32005,
        }
    }
}

/// Convenience result alias used across the workspace.
pub type Result<T> = std::result::Result<T, Error>;
