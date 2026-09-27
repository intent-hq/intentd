use std::future::Future;
use std::pin::Pin;

use intent_sourcecontrol::{
    error::AdmissionUnavailable, gitlab::GitlabCredentialRequest, Error, GitLabSourceControl,
    GitlabInstance, GitlabRequestCredentials,
};

use super::authority::RepositoryCredentialTransport;
use super::*;

impl RepositoryConnectionDirectory {
    pub(crate) fn admit(
        &self,
        binding: &RepositoryConnectionBinding,
        request: RepositoryAuthorityRequest,
        authority: Arc<dyn RepositoryAuthority>,
    ) -> Result<RepositoryCredentialAdmission> {
        let state = self.lock()?;
        let published = state.ready()?;
        if &published.binding != binding || state.generation != binding.scope.connection_generation
        {
            return Err(RepositoryCredentialError::Retired);
        }
        if request.execution.daemon_id != self.daemon_id
            || request.connection != binding.scope
            || request.target.provider != binding.account.provider
            || request.target.instance_base_url != binding.account.instance_base_url
            || !valid_project(&request.target.project_path)
        {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        match (&request.allowed_transport, request.use_kind) {
            (
                RepositoryCredentialTransport::GitlabApi(descriptor),
                RepositoryCredentialUse::NativeRead | RepositoryCredentialUse::NativeReviewCreate,
            ) if descriptor == &published.verified.descriptor => {}
            (
                RepositoryCredentialTransport::GitHttps(destinations),
                RepositoryCredentialUse::NativeRead
                | RepositoryCredentialUse::NativePush
                | RepositoryCredentialUse::ChildGit,
            ) if !destinations.is_empty()
                && destinations
                    .iter()
                    .all(|url| git_destination(url, &request.target)) => {}
            _ => return Err(RepositoryCredentialError::BoundaryMismatch),
        }
        let child_revision = if request.use_kind == RepositoryCredentialUse::ChildGit {
            if !state.child_enabled {
                return Err(RepositoryCredentialError::ChildDisabled);
            }
            Some(state.child_revision)
        } else {
            None
        };
        Ok(RepositoryCredentialAdmission {
            epoch: self.epoch,
            binding: binding.clone(),
            descriptor: published.verified.descriptor.clone(),
            request,
            authority,
            child_revision,
        })
    }

    pub(crate) fn check_current(&self, admission: &RepositoryCredentialAdmission) -> Result<()> {
        let state = self.lock()?;
        self.check_locked(&state, admission).map(|_| ())
    }

    fn check_locked<'a>(
        &self,
        state: &'a State,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<&'a Published> {
        if admission.epoch != self.epoch
            || admission.binding.daemon_id != self.daemon_id
            || state.status == RepositoryConnectionState::Retired
            || state.generation != admission.binding.scope.connection_generation
            || state
                .published
                .as_ref()
                .is_none_or(|p| p.binding != admission.binding)
        {
            return Err(RepositoryCredentialError::Retired);
        }
        let published = state.ready()?;
        if admission
            .child_revision
            .is_some_and(|v| v != state.child_revision)
        {
            return Err(RepositoryCredentialError::Retired);
        }
        if admission.child_revision.is_some() && !state.child_enabled {
            return Err(RepositoryCredentialError::ChildDisabled);
        }
        if state.backoff_until.is_some_and(|v| v > Instant::now()) {
            return Err(RepositoryCredentialError::Backoff);
        }
        Ok(published)
    }

    pub(crate) async fn acquire_exact(
        &self,
        admission: &RepositoryCredentialAdmission,
        reader: &dyn RepositorySecretReader,
        budget: Duration,
    ) -> Result<RepositoryCredentialTicket> {
        tokio::time::timeout(budget, async {
            let expected = {
                let state = self.lock()?;
                let published = self.check_locked(&state, admission)?;
                RepositorySecretRequest {
                    binding: published.binding.clone(),
                    secret_revision: state.secret_revision,
                    source: published.verified.source,
                }
            };
            let snapshot = reader.load(&expected).await?;
            if snapshot.request != expected {
                return Err(RepositoryCredentialError::SecretMismatch);
            }
            // Revalidate durable original-caller authority AFTER the potentially
            // blocking secret read; dispatch then checks both leaf fences at once.
            let fence = admission.authority.revalidate(&admission.request).await?;
            let mut token = Some(snapshot.token);
            let mut released = None;
            fence.dispatch(&mut || {
                let state = self.lock()?;
                self.check_locked(&state, admission)?;
                if state.secret_revision != expected.secret_revision {
                    return Err(RepositoryCredentialError::SecretMismatch);
                }
                let token = token
                    .take()
                    .ok_or(RepositoryCredentialError::AuthorityDenied)?;
                released = Some(RepositoryCredentialTicket {
                    token,
                    stamp: RepositoryDispatchStamp {
                        epoch: self.epoch,
                        binding: admission.binding.clone(),
                        secret_revision: state.secret_revision,
                        child_revision: admission.child_revision,
                        use_kind: admission.request.use_kind,
                    },
                });
                Ok(())
            })?;
            released.ok_or(RepositoryCredentialError::AuthorityDenied)
        })
        .await
        .map_err(|_| RepositoryCredentialError::TimedOut)?
    }
}

fn valid_project(path: &str) -> bool {
    path.is_ascii()
        && path.split('/').count() >= 2
        && path.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        })
}

fn git_destination(url: &str, target: &RepositoryTarget) -> bool {
    let expected = format!("{}/{}", target.instance_base_url, target.project_path);
    // Explicit HTTPS transport only in this inactive increment. Resolver-owned
    // aliases/SSH mappings cannot grant HTTPS credentials or erase the prefix.
    url == expected || url == format!("{expected}.git")
}

/// Private construction fixes account, project, use and approved transport.
/// `token_for` alone is intentionally unusable: actual request context is required.
pub(crate) struct BoundGitlabRequestCredentials {
    directory: Arc<RepositoryConnectionDirectory>,
    admission: RepositoryCredentialAdmission,
    reader: Arc<dyn RepositorySecretReader>,
    budget: Duration,
}
impl BoundGitlabRequestCredentials {
    pub(crate) fn new(
        directory: Arc<RepositoryConnectionDirectory>,
        admission: RepositoryCredentialAdmission,
        reader: Arc<dyn RepositorySecretReader>,
        budget: Duration,
    ) -> Result<Self> {
        if !matches!(
            admission.request.allowed_transport,
            RepositoryCredentialTransport::GitlabApi(_)
        ) || !matches!(
            admission.request.use_kind,
            RepositoryCredentialUse::NativeRead | RepositoryCredentialUse::NativeReviewCreate
        ) {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        directory.check_current(&admission)?;
        Ok(Self {
            directory,
            admission,
            reader,
            budget,
        })
    }

    pub(crate) fn into_provider(self) -> intent_sourcecontrol::Result<GitLabSourceControl> {
        GitLabSourceControl::new(self.admission.descriptor.clone(), Arc::new(self))
    }

    async fn for_request(
        &self,
        instance: &GitlabInstance,
        request: GitlabCredentialRequest<'_>,
    ) -> intent_sourcecontrol::Result<SecretString> {
        if instance.as_str() != self.admission.binding.account.instance_base_url
            || request.descriptor != &self.admission.descriptor
            || !request.is_for_project(&self.admission.request.target.project_path)
            || request.writing
                && (self.admission.request.use_kind != RepositoryCredentialUse::NativeReviewCreate
                    || !request.is_review_create(&self.admission.request.target.project_path))
        {
            return Err(RepositoryCredentialError::BoundaryMismatch.into());
        }
        let ticket = self
            .directory
            .acquire_exact(&self.admission, self.reader.as_ref(), self.budget)
            .await?;
        Ok(ticket.token)
    }
}

// Match the existing async-trait ABI without adding a service dependency on the
// macro. No token is cached in this callback or its HTTP pool.
impl GitlabRequestCredentials for BoundGitlabRequestCredentials {
    fn token_for<'s, 'i, 'f>(
        &'s self,
        _instance: &'i GitlabInstance,
    ) -> Pin<Box<dyn Future<Output = intent_sourcecontrol::Result<SecretString>> + Send + 'f>>
    where
        's: 'f,
        'i: 'f,
        Self: 'f,
    {
        Box::pin(async { Err(RepositoryCredentialError::BoundaryMismatch.into()) })
    }

    fn token_for_request<'s, 'i, 'r, 'f>(
        &'s self,
        instance: &'i GitlabInstance,
        request: GitlabCredentialRequest<'r>,
    ) -> Pin<Box<dyn Future<Output = intent_sourcecontrol::Result<SecretString>> + Send + 'f>>
    where
        's: 'f,
        'i: 'f,
        'r: 'f,
        Self: 'f,
    {
        Box::pin(self.for_request(instance, request))
    }
}

impl From<RepositoryCredentialError> for Error {
    fn from(error: RepositoryCredentialError) -> Self {
        use RepositoryCredentialError as Local;
        Self::AdmissionUnavailable(match error {
            Local::Retired | Local::StaleMutation | Local::CounterExhausted => {
                return Self::AdmissionRetired
            }
            Local::Missing => AdmissionUnavailable::Missing,
            Local::Unverified => AdmissionUnavailable::Unverified,
            Local::Disconnected => AdmissionUnavailable::Disconnected,
            Local::ChildDisabled => AdmissionUnavailable::ChildDisabled,
            Local::Mutating => AdmissionUnavailable::Mutating,
            Local::Indeterminate => AdmissionUnavailable::Indeterminate,
            Local::AuthorityDenied => AdmissionUnavailable::AuthorityDenied,
            Local::AuthorityUnavailable => AdmissionUnavailable::AuthorityUnavailable,
            Local::BoundaryMismatch => AdmissionUnavailable::BoundaryMismatch,
            Local::SecretMismatch => AdmissionUnavailable::SecretChanged,
            Local::TimedOut => AdmissionUnavailable::TimedOut,
            Local::Backoff => AdmissionUnavailable::Backoff,
        })
    }
}
