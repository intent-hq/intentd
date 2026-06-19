//! Typed errors for the source-control layer (§7, §11.5 `GithubError`).
//!
//! These are the crate-local domain errors for the forge abstraction. The
//! transport/wire mapping onto JSON-RPC error objects (PROTOCOL §9) is added by
//! a later milestone when the `pr.*` methods land — this crate stays free of
//! any wire concern (§3.2).

/// Errors surfaced by [`crate::SourceControl`] implementations and the
/// [`crate::SourceControlRegistry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No usable credential/configuration for the active provider. The daemon
    /// keeps running and source-control features report this (graceful per
    /// §8.3 / §7.3).
    #[error("source control not configured: {0}")]
    NotConfigured(String),

    /// The active host cannot perform this operation (gated by
    /// [`crate::ScCapabilities`]); surfaced to the FE so it can hide the UI.
    #[error("operation unsupported by provider: {0}")]
    Unsupported(String),

    /// Invalid or unknown provider selection / settings (e.g. an unregistered
    /// `activeProvider`, a malformed `apiBaseUrl`).
    #[error("source control configuration error: {0}")]
    Config(String),

    /// Authentication/authorization failed against the forge.
    #[error("source control auth error: {0}")]
    Auth(String),

    /// The forge rejected the request as a conflict (e.g. not mergeable).
    #[error("source control conflict: {0}")]
    Conflict(String),

    /// A requested entity was not found on the forge.
    #[error("not found: {0}")]
    NotFound(String),

    /// The forge rate-limited the request.
    #[error("source control rate limited: {0}")]
    RateLimited(String),

    /// A generic error returned by the forge API/transport.
    #[error("source control api error: {0}")]
    Api(String),

    /// Response (de)serialization failure.
    #[error("source control decode error: {0}")]
    Decode(String),
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

impl From<octocrab::Error> for Error {
    fn from(err: octocrab::Error) -> Self {
        // octocrab's error enum is large and version-sensitive; categorize on
        // the `GitHub` variant's HTTP status where present, otherwise fall back
        // to a generic API error. (Status-precise mapping can be refined when
        // the wire layer needs it.)
        if let octocrab::Error::GitHub { source, .. } = &err {
            let status = source.status_code.as_u16();
            let msg = source.message.clone();
            return match status {
                401 | 403 => Error::Auth(msg),
                404 => Error::NotFound(msg),
                409 | 422 => Error::Conflict(msg),
                429 => Error::RateLimited(msg),
                _ => Error::Api(format!("{status}: {msg}")),
            };
        }
        Error::Api(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Decode(err.to_string())
    }
}
