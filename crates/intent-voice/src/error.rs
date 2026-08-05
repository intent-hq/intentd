//! Typed errors for the voice engine.
//!
//! Crate-local domain errors mirroring `intent-linear`'s `error.rs`. This
//! crate stays free of any wire concern — the transport/wire mapping onto
//! JSON-RPC error objects lives in `intent-services::voice_ops`.

/// Errors surfaced by [`crate::VoiceEngine`] implementations and the
/// [`crate::VoiceRegistry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No usable API key for the selected provider. The daemon keeps running
    /// and voice features report this (graceful, mirroring intent-linear).
    /// This variant is exclusively the missing-key case — it drives the
    /// structured `voice-no-api-key` wire code (monorepo#1448) — so provider
    /// failures must not reuse it.
    #[error("voice not configured: {0}")]
    NotConfigured(String),

    /// The requested transcription model is unavailable on this account
    /// (provider returned 404). Distinct from [`Error::NotConfigured`] so a
    /// model-unavailable failure is never mislabeled as a missing API key.
    #[error("voice model unavailable: {0}")]
    ModelUnavailable(String),

    /// Invalid or malformed settings (e.g. a bad `apiBaseUrl`).
    #[error("voice configuration error: {0}")]
    Config(String),

    /// Authentication/authorization failed against the provider.
    #[error("voice auth error: {0}")]
    Auth(String),

    /// The provider rate-limited the request.
    #[error("voice rate limited: {0}")]
    RateLimited(String),

    /// A generic error returned by the provider API/transport.
    #[error("voice api error: {0}")]
    Api(String),

    /// Response (de)serialization failure.
    #[error("voice decode error: {0}")]
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
