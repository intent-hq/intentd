//! Active-stage adapter for the credential owner's sole private authority trait.
//! Real writer retirement and request entry adapters remain uninstalled.

use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthorityFence, RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    RepositoryAuthority, RepositoryCredentialError, RepositoryCredentialUse,
};
use intent_core::RepositoryProvider;

use super::*;

fn request_stage(request: &RepositoryAuthorityRequest) -> Option<NativeReviewStage> {
    match request.use_kind {
        RepositoryCredentialUse::NativePush => Some(NativeReviewStage::Push),
        RepositoryCredentialUse::NativeReviewCreate => Some(NativeReviewStage::CreatePr),
        RepositoryCredentialUse::NativeRead | RepositoryCredentialUse::ChildGit => None,
    }
}

pub(super) fn valid_requests(facts: &RepositoryOperationFacts) -> bool {
    facts
        .credential_requests
        .iter()
        .enumerate()
        .all(|(i, request)| {
            if request.execution != facts.preparation.scope
                || facts.credential_requests[..i]
                    .iter()
                    .any(|previous| previous.use_kind == request.use_kind)
            {
                return false;
            }
            let branch = match (&request.allowed_transport, request.use_kind) {
                (
                    RepositoryCredentialTransport::GitHttps(destinations),
                    RepositoryCredentialUse::NativePush,
                ) if !destinations.is_empty() && *destinations == facts.push_destinations => {
                    &facts.preparation.source
                }
                (
                    RepositoryCredentialTransport::GitlabApi(descriptor),
                    RepositoryCredentialUse::NativeReviewCreate,
                ) if request.target.provider == RepositoryProvider::Gitlab
                    && descriptor.instance().as_str() == request.target.instance_base_url =>
                {
                    &facts.preparation.target
                }
                _ => return false,
            };
            request.target == branch.repository
                && branch.connection.as_ref() == Some(&request.connection)
        })
}

fn local_error(error: AdmissionError) -> RepositoryCredentialError {
    match error {
        AdmissionError::Denied => RepositoryCredentialError::AuthorityDenied,
        AdmissionError::Unavailable => RepositoryCredentialError::AuthorityUnavailable,
        AdmissionError::Retired | AdmissionError::BindingChanged | AdmissionError::StageOrder => {
            RepositoryCredentialError::Retired
        }
        AdmissionError::InvalidPlan | AdmissionError::InvalidCompletion => {
            RepositoryCredentialError::BoundaryMismatch
        }
    }
}

fn active(inner: &OperationInner, stage: NativeReviewStage, index: usize) -> AdmissionResult<()> {
    let progress = inner.progress.lock().map_err(|_| AdmissionError::Retired)?;
    check_active(&progress, inner, stage, index)
}

fn check_active(
    progress: &OperationState,
    inner: &OperationInner,
    stage: NativeReviewStage,
    index: usize,
) -> AdmissionResult<()> {
    if progress.terminal
        || !progress.active
        || progress.next != index
        || inner.stages.get(index) != Some(&stage)
    {
        return Err(AdmissionError::StageOrder);
    }
    Ok(())
}

struct ActiveAuthority {
    inner: Arc<OperationInner>,
    stage: NativeReviewStage,
    index: usize,
    request: RepositoryAuthorityRequest,
}

impl RepositoryDispatchStamp {
    /// Capture from the active private stage, never from a public preparation.
    /// Commit cannot mint forge credentials. Reads made as create prerequisites
    /// retain the create purpose, rather than upgrading a read-only handle.
    pub(crate) fn credential_authority(
        &self,
    ) -> AdmissionResult<(RepositoryAuthorityRequest, Arc<dyn RepositoryAuthority>)> {
        self.inner.retirement.check_current()?;
        active(&self.inner, self.stage, self.index)?;
        let request = self
            .inner
            .facts
            .credential_requests
            .iter()
            .find(|request| request_stage(request) == Some(self.stage))
            .ok_or(AdmissionError::Denied)?
            .clone();
        Ok((
            request.clone(),
            Arc::new(ActiveAuthority {
                inner: self.inner.clone(),
                stage: self.stage,
                index: self.index,
                request,
            }),
        ))
    }
}

impl RepositoryAuthority for ActiveAuthority {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if request != &self.request {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            self.inner.retirement.check_current().map_err(local_error)?;
            active(&self.inner, self.stage, self.index).map_err(local_error)?;
            let (observed, legacy) = observe_authority(&self.inner, self.stage)
                .await
                .map_err(local_error)?;
            let checked = self.inner.retirement.dispatch(|| {
                let mut progress = self
                    .inner
                    .progress
                    .lock()
                    .map_err(|_| AdmissionError::Retired)?;
                check_active(&progress, &self.inner, self.stage, self.index)?;
                if !self.inner.facts.matches(&observed, &progress) {
                    return Err(AdmissionError::BindingChanged);
                }
                progress.last_revision = observed.preparation.context_revision.clone();
                Ok(progress.last_revision.clone())
            });
            if checked == Err(AdmissionError::BindingChanged) {
                self.inner.retirement.retire();
            }
            Ok(Box::new(ActiveFence {
                inner: self.inner.clone(),
                stage: self.stage,
                index: self.index,
                revision: checked.map_err(local_error)?,
                _legacy: legacy,
            }) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

struct ActiveFence {
    inner: Arc<OperationInner>,
    stage: NativeReviewStage,
    index: usize,
    revision: RepositoryContextRevision,
    _legacy: Option<CredentialLease>,
}

impl RepositoryAuthorityFence for ActiveFence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> crate::repository_credentials::Result<()> + Send),
    ) -> crate::repository_credentials::Result<()> {
        // R lifetime -> active operation -> P directory. Action is the sole
        // synchronous token release, never an HTTP/Git call or another R read.
        let result = self.inner.retirement.dispatch(|| {
            let progress = self
                .inner
                .progress
                .lock()
                .map_err(|_| AdmissionError::Retired)?;
            check_active(&progress, &self.inner, self.stage, self.index)?;
            if progress.last_revision != self.revision {
                return Err(AdmissionError::Retired);
            }
            Ok(action())
        });
        // The original legacy lease protects only this release, not later I/O.
        drop(self);
        result.map_err(local_error)?
    }
}
