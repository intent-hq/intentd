//! Typed errors for the Sentry engine.
//!
//! Crate-local domain errors mirroring `intent-linear`'s `error.rs`. This
//! crate stays free of any wire concern — the transport/wire mapping onto
//! JSON-RPC error objects is added by the `sentry.*` wire milestone, where
//! both `NotConfigured` and REST failures surface as a generic `-32603`.

/// Errors surfaced by [`crate::SentryEngine`] implementations and the
/// [`crate::SentryRegistry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No usable credential pair (org + token) for Sentry. The daemon keeps
    /// running and Sentry features report this (graceful, mirroring
    /// `intent-linear`).
    #[error("sentry not configured: {0}")]
    NotConfigured(String),

    /// Invalid or malformed settings (e.g. a bad `apiBaseUrl`).
    #[error("sentry configuration error: {0}")]
    Config(String),

    /// Authentication/authorization failed against Sentry.
    #[error("sentry auth error: {0}")]
    Auth(String),

    /// A requested entity was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Sentry rate-limited the request.
    #[error("sentry rate limited: {0}")]
    RateLimited(String),

    /// A generic error returned by the Sentry API/transport.
    #[error("sentry api error: {0}")]
    Api(String),

    /// Response (de)serialization failure.
    #[error("sentry decode error: {0}")]
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
