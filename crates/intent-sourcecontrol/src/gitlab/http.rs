//! Request boundary: injected credentials per request, no redirects or body-bearing errors.
use crate::error::AdmissionUnavailable;

use super::{
    project, Error, GitLabSourceControl, HeaderMap, Method, ProviderAvailability, ProviderFailure,
    ProviderFailureKind, RepoRef, RequestProvenance, Result, Value, MAX_RESPONSE_BYTES,
};

#[derive(Clone, Copy)]
pub(super) enum Purpose {
    Primary,
    /// Only the addressed project resource, not a branch or MR within it.
    Project,
    Optional,
}

#[derive(Clone, Copy)]
pub(super) struct RequestScope<'a> {
    pub purpose: Purpose,
    pub provenance: RequestProvenance<'a>,
}
impl From<Purpose> for RequestScope<'_> {
    fn from(purpose: Purpose) -> Self {
        Self {
            purpose,
            provenance: RequestProvenance::Direct,
        }
    }
}

pub(super) fn failure(kind: ProviderFailureKind, status: Option<u16>) -> Error {
    ProviderFailure { kind, status }.into()
}

impl GitLabSourceControl {
    pub(super) async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<Value>,
    ) -> Result<(Value, HeaderMap)> {
        self.request_for(method, path, query, body, Purpose::Primary)
            .await
    }

    pub(super) async fn request_for(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<Value>,
        purpose: Purpose,
    ) -> Result<(Value, HeaderMap)> {
        self.request_scoped(method, path, query, body, purpose.into())
            .await
    }

    pub(super) async fn request_scoped(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<Value>,
        scope: RequestScope<'_>,
    ) -> Result<(Value, HeaderMap)> {
        let url = self
            .api
            .join(path)
            .map_err(|_| Error::Config("invalid GitLab endpoint".into()))?;
        if url.origin() != self.api.origin()
            || !url.path().starts_with(self.api.path())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Config(
                "GitLab endpoint escaped configured instance".into(),
            ));
        }
        let writing = method != Method::GET;
        let uncertain = || {
            failure(
                if writing {
                    ProviderFailureKind::WriteUncertain
                } else {
                    ProviderFailureKind::Transient
                },
                None,
            )
        };
        let mut request = self.client.request(method, url).query(query);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let prepared = super::GitlabPreparedRequest::new(
            request
                .build()
                .map_err(|_| Error::Config("cannot prepare GitLab request".into()))?,
            super::GitlabCredentialRequest {
                descriptor: &self.descriptor,
                path,
                writing,
                provenance: scope.provenance,
            },
        );
        let admitted = self.credentials.admit_http_request(prepared).await?;
        // No application queue, await or spawn between admission and execution.
        // The pinned transport may continue an already admitted request (including
        // a recovered unstarted pooled request); retirement cannot recall it.
        let (request, receipt) = admitted.into_parts();
        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(|_| uncertain())?;
        let status = response.status();
        let headers = response.headers().clone();
        self.observe_rate_limit(&headers, status.as_u16() == 429);
        if let Some(receipt) = receipt {
            receipt.observe(super::GitlabResponseObservation::from_headers(
                status.as_u16(),
                &headers,
            ));
        }
        if !status.is_success() {
            let code = status.as_u16();
            let kind = match (code, scope.purpose) {
                (429, _) => {
                    return Err(Error::RateLimited("GitLab request quota exhausted".into()))
                }
                (401, _) => ProviderFailureKind::CredentialRejected,
                (403 | 404, Purpose::Project) => ProviderFailureKind::ProjectDenied,
                (403 | 404, Purpose::Primary) => ProviderFailureKind::ResourceDenied,
                (403, Purpose::Optional) => ProviderFailureKind::OptionalRestricted,
                (404 | 405 | 501, Purpose::Optional) => ProviderFailureKind::OptionalUnavailable,
                (409 | 422 | 400, _) => return Err(Error::Conflict(format!("GitLab HTTP {code}"))),
                (408 | 500..=599, _) if writing => ProviderFailureKind::WriteUncertain,
                (408 | 500..=599, _) => ProviderFailureKind::Transient,
                _ => ProviderFailureKind::Unknown,
            };
            return Err(failure(kind, Some(code)));
        }
        let invalid = || {
            failure(
                if writing {
                    ProviderFailureKind::WriteUncertain
                } else {
                    ProviderFailureKind::Unknown
                },
                Some(status.as_u16()),
            )
        };
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| uncertain())? {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).map_err(|_| invalid())?
        };
        Ok((value, headers))
    }

    pub(super) async fn get(&self, path: &str) -> Result<Value> {
        self.request(Method::GET, path, &[], None)
            .await
            .map(|r| r.0)
    }

    pub(super) async fn get_project(&self, repo: &RepoRef) -> Result<Value> {
        self.request_for(Method::GET, &project(repo), &[], None, Purpose::Project)
            .await
            .map(|r| r.0)
    }

    pub(super) async fn optional_get(
        &self,
        path: &str,
    ) -> Result<(Option<Value>, ProviderAvailability)> {
        self.optional_get_for(path, Purpose::Optional).await
    }

    pub(super) async fn optional_get_for(
        &self,
        path: &str,
        purpose: Purpose,
    ) -> Result<(Option<Value>, ProviderAvailability)> {
        match self
            .request_for(Method::GET, path, &[], None, purpose)
            .await
        {
            Ok((v, _)) => Ok((Some(v), ProviderAvailability::Available)),
            Err(e) if optional_failure(&e) => Ok((None, availability(&e))),
            Err(e) => Err(e),
        }
    }
}

pub(super) fn availability(error: &Error) -> ProviderAvailability {
    match error {
        Error::Provider(ProviderFailure {
            kind: ProviderFailureKind::OptionalRestricted,
            ..
        }) => ProviderAvailability::Restricted,
        Error::Provider(ProviderFailure {
            kind: ProviderFailureKind::OptionalUnavailable,
            ..
        })
        | Error::Unsupported(_) => ProviderAvailability::Unavailable,
        Error::Provider(ProviderFailure {
            kind: ProviderFailureKind::Transient,
            ..
        }) => ProviderAvailability::Transient,
        Error::RateLimited(_) | Error::AdmissionUnavailable(AdmissionUnavailable::Backoff) => {
            ProviderAvailability::RateLimited
        }
        _ => ProviderAvailability::Unknown,
    }
}

pub(super) fn optional_failure(error: &Error) -> bool {
    matches!(
        error,
        Error::RateLimited(_)
            | Error::AdmissionUnavailable(AdmissionUnavailable::Backoff)
            | Error::Decode(_)
            | Error::Unsupported(_)
            | Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::OptionalRestricted
                    | ProviderFailureKind::OptionalUnavailable
                    | ProviderFailureKind::Transient
                    | ProviderFailureKind::Unknown,
                ..
            })
    )
}
