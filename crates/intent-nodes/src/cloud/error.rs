use std::time::Duration;

/// Which provider limit refused a call (`CloudQuotaData.limit`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaLimit {
    Concurrency,
    Rate,
    Plan,
}

impl QuotaLimit {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Concurrency => "concurrency",
            Self::Rate => "rate",
            Self::Plan => "plan",
        }
    }
}

/// What a failed provider call means to the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudErrorKind {
    InvalidArgument,
    NotConfigured,
    Quota(QuotaLimit),
    /// The machine or image is gone.
    NotFound,
    /// The machine is changing state.
    Conflict,
    /// The provider rejected the key, or forbade the call.
    Unauthorized,
    /// The key is valid but lacks a permission; [`CloudError::action`]
    /// names the refused call.
    PermissionDenied,
    /// The project the entry names does not exist, or the key cannot see it.
    ProjectMissing,
    /// Any other HTTP refusal; [`CloudError::status`] says which.
    Api,
    Other,
}

/// A failed provider call. The message is the provider's, scrubbed of
/// keys and tokens.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CloudError {
    kind: CloudErrorKind,
    status: Option<u16>,
    retry_after: Option<Duration>,
    retryable: bool,
    action: Option<String>,
    message: String,
}

impl CloudError {
    #[must_use]
    pub fn new(kind: CloudErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            status: None,
            retry_after: None,
            retryable: false,
            action: None,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    #[must_use]
    pub fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }

    #[must_use]
    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// The refused call of a [`CloudErrorKind::PermissionDenied`], for
    /// example `create-service`.
    #[must_use]
    pub fn with_action(mut self, action: impl Into<String>) -> Self {
        self.action = Some(action.into());
        self
    }

    #[must_use]
    pub fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    #[must_use]
    pub fn kind(&self) -> CloudErrorKind {
        self.kind
    }

    /// The HTTP status of a refusal, when the provider gave one.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        self.status
    }

    /// How long the provider asked to wait before the next call.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Whether the same call may succeed later without a change.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub fn quota_limit(&self) -> Option<QuotaLimit> {
        match self.kind {
            CloudErrorKind::Quota(limit) => Some(limit),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.kind == CloudErrorKind::NotFound
    }

    #[must_use]
    pub fn is_conflict(&self) -> bool {
        self.kind == CloudErrorKind::Conflict
    }
}
