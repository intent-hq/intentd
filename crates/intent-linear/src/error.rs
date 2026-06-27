//! Typed errors for the Linear engine.
//!
//! Crate-local domain errors mirroring `intent-sourcecontrol`'s `error.rs`.
//! This crate stays free of any wire concern — the transport/wire mapping onto
//! JSON-RPC error objects is added by the `linear.*` wire milestone.

/// Errors surfaced by [`crate::LinearEngine`] implementations and the
/// [`crate::LinearRegistry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No usable credential/configuration for Linear. The daemon keeps running
    /// and Linear features report this (graceful, mirroring source-control).
    #[error("linear not configured: {0}")]
    NotConfigured(String),

    /// Invalid or malformed settings (e.g. a bad `apiBaseUrl`).
    #[error("linear configuration error: {0}")]
    Config(String),

    /// Authentication/authorization failed against Linear.
    #[error("linear auth error: {0}")]
    Auth(String),

    /// A requested entity was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Linear rate-limited the request.
    #[error("linear rate limited: {0}")]
    RateLimited(String),

    /// A generic error returned by the Linear API/transport.
    #[error("linear api error: {0}")]
    Api(String),

    /// Response (de)serialization failure.
    #[error("linear decode error: {0}")]
    Decode(String),
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if let Some(status) = err.status() {
            let code = status.as_u16();
            return match code {
                401 | 403 => Error::Auth(status.to_string()),
                404 => Error::NotFound(status.to_string()),
                429 => Error::RateLimited(status.to_string()),
                _ => Error::Api(format!("{code}: {status}")),
            };
        }
        if err.is_decode() {
            return Error::Decode(err.to_string());
        }
        Error::Api(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Decode(err.to_string())
    }
}
