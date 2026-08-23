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

/// Classify a GitHub REST error by status + message onto the crate's
/// [`Error`] categories. Extracted from the [`From<octocrab::Error>`] impl so
/// the 403 rate-limit-vs-auth discrimination is unit-testable without
/// constructing octocrab error values.
///
/// GitHub answers **403** (not only 429) for both primary quota exhaustion
/// ("API rate limit exceeded for user ID ...") and secondary limits ("You
/// have exceeded a secondary rate limit ..."). octocrab's typed `GitHubError`
/// carries no response headers, so the header-level signals
/// (`x-ratelimit-remaining: 0`, `Retry-After`) are unavailable here — the
/// documented message text is the discriminator (monorepo#2961). A 403 whose
/// body does not name the rate limit stays an auth error.
fn classify_github_status(status: u16, msg: String) -> Error {
    match status {
        403 if msg.to_ascii_lowercase().contains("rate limit") => Error::RateLimited(msg),
        401 | 403 => Error::Auth(msg),
        404 => Error::NotFound(msg),
        409 | 422 => Error::Conflict(msg),
        429 => Error::RateLimited(msg),
        _ => Error::Api(format!("{status}: {msg}")),
    }
}

impl From<octocrab::Error> for Error {
    fn from(err: octocrab::Error) -> Self {
        // octocrab's error enum is large and version-sensitive; categorize on
        // the `GitHub` variant's HTTP status where present, otherwise fall back
        // to a generic API error. (Status-precise mapping can be refined when
        // the wire layer needs it.)
        if let octocrab::Error::GitHub { source, .. } = &err {
            return classify_github_status(source.status_code.as_u16(), source.message.clone());
        }
        Error::Api(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Decode(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_403_rate_limit_messages_as_rate_limited() {
        // Primary quota exhaustion (the monorepo#2961 log shape) and
        // secondary limits both answer 403 with "rate limit" in the body.
        for msg in [
            "API rate limit exceeded for user ID 526899.",
            "You have exceeded a secondary rate limit. Please wait a few minutes.",
        ] {
            let err = classify_github_status(403, msg.to_string());
            assert!(matches!(err, Error::RateLimited(m) if m == msg), "{msg}");
        }
    }

    #[test]
    fn classifies_plain_403_and_401_as_auth() {
        let err = classify_github_status(403, "Resource not accessible by integration".into());
        assert!(matches!(err, Error::Auth(_)));
        let err = classify_github_status(401, "Bad credentials".into());
        assert!(matches!(err, Error::Auth(_)));
    }

    #[test]
    fn classifies_429_as_rate_limited() {
        let err = classify_github_status(429, "too many requests".into());
        assert!(matches!(err, Error::RateLimited(_)));
    }
}
