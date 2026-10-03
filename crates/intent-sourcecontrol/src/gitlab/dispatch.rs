//! One-use HTTP ownership transfer. Only the provider constructs or executes
//! requests; an adapter can authenticate and admit the exact prepared request.
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::{header, Request};

use super::{Error, ExposeSecret, GitlabCredentialRequest, HeaderMap, Result, SecretString};

/// Exact method, URL, query, body and private provider provenance. Neither this
/// request nor its authenticated/admitted forms expose Debug, Clone or Serde.
pub struct GitlabPreparedRequest<'a> {
    request: Request,
    context: GitlabCredentialRequest<'a>,
}

impl<'a> GitlabPreparedRequest<'a> {
    pub(super) fn new(request: Request, context: GitlabCredentialRequest<'a>) -> Self {
        Self { request, context }
    }

    /// Metadata borrowed from the same immutable request prepared by the provider.
    #[must_use]
    pub fn credential_request(&self) -> GitlabCredentialRequest<'a> {
        self.context
    }

    /// Attach a request-local token before the caller's final admission fence.
    ///
    /// # Errors
    /// Rejects absent credentials and invalid header characters without sending.
    pub fn authenticate(mut self, token: SecretString) -> Result<GitlabAuthenticatedRequest<'a>> {
        if token.expose_secret().trim().is_empty() {
            return Err(Error::NotConfigured("GitLab credential is absent".into()));
        }
        let mut value = header::HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
            .map_err(|_| {
                Error::Config("GitLab credential contains invalid header characters".into())
            })?;
        value.set_sensitive(true);
        // Consume the owned snapshot here; only the request-local header remains.
        drop(token);
        self.request
            .headers_mut()
            .insert(header::AUTHORIZATION, value);
        Ok(GitlabAuthenticatedRequest { prepared: self })
    }
}

/// Authenticated but not yet admitted. Consuming this inside the original fence
/// transfers ownership only: no network work, await or task spawn takes place.
pub struct GitlabAuthenticatedRequest<'a> {
    prepared: GitlabPreparedRequest<'a>,
}
impl GitlabAuthenticatedRequest<'_> {
    /// Admit exactly once. Any response is attributed to this request's receipt.
    #[must_use]
    pub fn admit(self, receipt: Option<Box<dyn GitlabResponseReceipt>>) -> GitlabAdmittedRequest {
        GitlabAdmittedRequest {
            request: self.prepared.request,
            receipt,
        }
    }
}

/// An admitted effect may outlive retirement. Only the provider can consume it
/// for HTTP; no caller can clone, retarget or extract its authenticated request.
pub struct GitlabAdmittedRequest {
    request: Request,
    receipt: Option<Box<dyn GitlabResponseReceipt>>,
}
impl GitlabAdmittedRequest {
    pub(super) fn into_parts(self) -> (Request, Option<Box<dyn GitlabResponseReceipt>>) {
        (self.request, self.receipt)
    }
}

/// Metadata from actual response headers, before optional-field degradation.
#[derive(Debug, Clone, Copy)]
pub struct GitlabResponseObservation {
    pub status: u16,
    /// Monotonic deadline derived from the request's quota headers, if present.
    pub backoff_until: Option<Instant>,
}
impl GitlabResponseObservation {
    pub(super) fn from_headers(status: u16, headers: &HeaderMap) -> Self {
        let number = |name: &str| headers.get(name)?.to_str().ok()?.parse::<u64>().ok();
        let throttled = status == 429;
        let exhausted = throttled || number("ratelimit-remaining") == Some(0);
        let reset_after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|now| {
                number("ratelimit-reset").map(|at| Duration::from_secs(at).saturating_sub(now))
            });
        let retry_after = throttled
            .then(|| number("retry-after"))
            .flatten()
            .map(Duration::from_secs);
        let backoff_until = if exhausted {
            reset_after
                .max(retry_after)
                .and_then(|delay| Instant::now().checked_add(delay))
        } else {
            None
        };
        Self {
            status,
            backoff_until,
        }
    }
}

/// Request-local attribution only. Observing cannot change a known HTTP outcome
/// or authorize another request; stale receipt rejection remains owner-defined.
pub trait GitlabResponseReceipt: Send + Sync {
    fn observe(&self, observation: GitlabResponseObservation);
}
