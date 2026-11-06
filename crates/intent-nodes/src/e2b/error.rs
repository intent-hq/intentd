use std::time::Duration;

/// Errors from the E2B API and envd. No variant holds a key or token.
#[derive(Debug, thiserror::Error)]
pub enum E2bError {
    #[error("invalid E2B request: {0}")]
    InvalidArgument(String),
    #[error("the E2B API key is not configured")]
    NotConfigured,
    #[error("could not read the E2B API key: {0}")]
    SecretStore(String),
    /// The retry budget ran out while E2B kept answering 429.
    #[error("E2B rate limit reached")]
    RateLimited { retry_after: Option<Duration> },
    /// E2B kept no outcome for this idempotency key and did not run the create
    /// again. A sandbox may exist: look it up by metadata before a new create.
    #[error(
        "E2B does not know the outcome of the earlier create; look the sandbox up by metadata"
    )]
    IdempotencyOutcomeUnknown,
    #[error("the E2B idempotency key was already used for a different request")]
    IdempotencyMismatch,
    #[error("an E2B create with this idempotency key is still in progress")]
    IdempotencyInProgress,
    /// 503 on pause: the sandbox's node is busy with snapshot work. Retry later.
    #[error("E2B refused the pause: {0}")]
    PauseRefused(String),
    #[error("E2B sandbox or template not found: {0}")]
    NotFound(String),
    /// The sandbox is changing state (for example pausing or resuming).
    #[error("E2B sandbox conflict: {0}")]
    Conflict(String),
    #[error("E2B rejected the credentials (HTTP {status})")]
    Unauthorized { status: u16 },
    #[error("E2B API error (HTTP {status}): {message}")]
    Api {
        status: u16,
        error_code: Option<String>,
        message: String,
        should_retry: bool,
    },
    #[error("E2B request failed: {0}")]
    Transport(String),
    #[error("unexpected E2B response: {0}")]
    Protocol(String),
    /// The envd stream ended with a Connect error.
    #[error("E2B process failed ({code}): {message}")]
    Process { code: String, message: String },
}

impl E2bError {
    /// Whether the same call may succeed later without a change.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RateLimited { .. }
            | Self::IdempotencyInProgress
            | Self::PauseRefused(_)
            | Self::Conflict(_)
            | Self::Transport(_) => true,
            Self::Api {
                status,
                should_retry,
                ..
            } => *should_retry || matches!(status, 502..=504),
            _ => false,
        }
    }
}
