use std::future::Future;
use std::pin::Pin;

use intent_sourcecontrol::{
    error::AdmissionUnavailable,
    gitlab::{
        GitlabAdmittedRequest, GitlabAuthenticatedRequest, GitlabCredentialRequest,
        GitlabPreparedRequest, GitlabResponseObservation, GitlabResponseReceipt,
    },
    Error, GitLabSourceControl, GitlabInstance, GitlabRequestCredentials,
};

use super::authority::RepositoryCredentialTransport;
use super::read::ReadReceiptSink;
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

    // The token-release fence is already consumed. Revalidate the SAME captured
    // authority and perform only a one-use ownership transfer under its new fence.
    async fn admit_http_exact(
        self: &Arc<Self>,
        admission: &RepositoryCredentialAdmission,
        stamp: RepositoryDispatchStamp,
        prepared: GitlabAuthenticatedRequest<'_>,
        read_sink: Option<Arc<ReadReceiptSink>>,
    ) -> Result<GitlabAdmittedRequest> {
        let fence = admission.authority.revalidate(&admission.request).await?;
        let mut pending = Some((
            prepared,
            Box::new(DispatchReceipt {
                directory: self.clone(),
                stamp,
                read_sink,
            }),
        ));
        let mut admitted = None;
        fence.dispatch(&mut || {
            let state = self.lock()?;
            self.check_locked(&state, admission)?;
            let stamp = &pending
                .as_ref()
                .ok_or(RepositoryCredentialError::AuthorityDenied)?
                .1
                .stamp;
            if stamp.epoch != self.epoch
                || stamp.binding != admission.binding
                || stamp.child_revision != admission.child_revision
                || stamp.use_kind != admission.request.use_kind
            {
                return Err(RepositoryCredentialError::Retired);
            }
            if stamp.secret_revision != state.secret_revision {
                return Err(RepositoryCredentialError::SecretMismatch);
            }
            let (request, receipt) = pending
                .take()
                .ok_or(RepositoryCredentialError::AuthorityDenied)?;
            admitted = Some(request.admit(Some(receipt)));
            Ok(())
        })?;
        admitted.ok_or(RepositoryCredentialError::AuthorityDenied)
    }
}

struct DispatchReceipt {
    directory: Arc<RepositoryConnectionDirectory>,
    stamp: RepositoryDispatchStamp,
    read_sink: Option<Arc<ReadReceiptSink>>,
}
impl GitlabResponseReceipt for DispatchReceipt {
    fn observe(&self, observation: GitlabResponseObservation) {
        if let Some(until) = observation.backoff_until {
            let _ = self.directory.record_backoff(&self.stamp, until);
        }
        let rejection = (observation.status == 401)
            .then(|| self.directory.reject_current_credential(&self.stamp));
        if let Some(sink) = &self.read_sink {
            sink.observe(&self.stamp, observation.status, rejection);
        }
        // These existing predicates ignore obsolete receipts. A bookkeeping
        // failure must not erase the actual dispatched HTTP outcome, and a
        // poisoned directory already refuses subsequent acquisition.
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
    read_sink: Option<Arc<ReadReceiptSink>>,
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
            read_sink: None,
        })
    }

    pub(super) fn for_read(
        directory: Arc<RepositoryConnectionDirectory>,
        admission: RepositoryCredentialAdmission,
        reader: Arc<dyn RepositorySecretReader>,
        budget: Duration,
        read_sink: Arc<ReadReceiptSink>,
    ) -> Result<Self> {
        let mut bound = Self::new(directory, admission, reader, budget)?;
        bound.read_sink = Some(read_sink);
        Ok(bound)
    }

    pub(crate) fn into_provider(self) -> intent_sourcecontrol::Result<GitLabSourceControl> {
        GitLabSourceControl::new(self.admission.descriptor.clone(), Arc::new(self))
    }

    fn validate_request(
        &self,
        instance: &GitlabInstance,
        request: GitlabCredentialRequest<'_>,
    ) -> intent_sourcecontrol::Result<()> {
        if instance.as_str() != self.admission.binding.account.instance_base_url
            || request.descriptor != &self.admission.descriptor
            || !request.is_for_project(&self.admission.request.target.project_path)
            || request.writing
                && (self.admission.request.use_kind != RepositoryCredentialUse::NativeReviewCreate
                    || !request.is_review_create(&self.admission.request.target.project_path))
        {
            return Err(RepositoryCredentialError::BoundaryMismatch.into());
        }
        Ok(())
    }

    async fn for_request(
        &self,
        instance: &GitlabInstance,
        request: GitlabCredentialRequest<'_>,
    ) -> intent_sourcecontrol::Result<SecretString> {
        self.validate_request(instance, request)?;
        let ticket = self
            .directory
            .acquire_exact(&self.admission, self.reader.as_ref(), self.budget)
            .await?;
        Ok(ticket.token)
    }

    async fn for_http_request(
        &self,
        prepared: GitlabPreparedRequest<'_>,
    ) -> intent_sourcecontrol::Result<GitlabAdmittedRequest> {
        let context = prepared.credential_request();
        self.validate_request(context.descriptor.instance(), context)?;
        tokio::time::timeout(self.budget, async {
            let ticket = self
                .directory
                .acquire_exact(&self.admission, self.reader.as_ref(), self.budget)
                .await?;
            let authenticated = prepared.authenticate(ticket.token)?;
            self.directory
                .admit_http_exact(
                    &self.admission,
                    ticket.stamp,
                    authenticated,
                    self.read_sink.clone(),
                )
                .await
                .map_err(Error::from)
        })
        .await
        .map_err(|_| Error::from(RepositoryCredentialError::TimedOut))?
    }
}

// Match the existing async-trait ABI without adding a service dependency on the
// macro. No token is cached in this callback or its HTTP pool.
impl GitlabRequestCredentials for BoundGitlabRequestCredentials {
    fn admit_http_request<'s, 'r, 'f>(
        &'s self,
        prepared: GitlabPreparedRequest<'r>,
    ) -> Pin<
        Box<dyn Future<Output = intent_sourcecontrol::Result<GitlabAdmittedRequest>> + Send + 'f>,
    >
    where
        's: 'f,
        'r: 'f,
        Self: 'f,
    {
        Box::pin(self.for_http_request(prepared))
    }

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

impl RepositoryConnectionDirectory {
    /// Consume a prepared native HTTPS action on the actual owning blocking
    /// worker. Revalidate the original active R stage AFTER secret acquisition,
    /// compare P under that fence, then immediately start Git without requeueing.
    pub(crate) async fn native_push(
        &self,
        admission: &RepositoryCredentialAdmission,
        reader: &dyn RepositorySecretReader,
        prepared: intent_git::native_push::PreparedNativePush,
        observed: impl FnMut(&str) + Send,
    ) -> intent_core::Result<String> {
        use intent_sourcecontrol::ExposeSecret;
        let unavailable = |_| intent_core::Error::Forbidden("Repository review unavailable".into());
        if admission.request.use_kind != RepositoryCredentialUse::NativePush {
            return Err(unavailable(RepositoryCredentialError::AuthorityDenied));
        }
        let ticket = self
            .acquire_exact(admission, reader, Duration::from_secs(5))
            .await
            .map_err(unavailable)?;
        let fence = admission
            .authority
            .revalidate(&admission.request)
            .await
            .map_err(unavailable)?;
        let mut pending = Some((prepared, ticket));
        let mut admitted = None;
        fence
            .dispatch(&mut || {
                let state = self.lock()?;
                self.check_locked(&state, admission)?;
                let (_, ticket) = pending
                    .as_ref()
                    .ok_or(RepositoryCredentialError::AuthorityDenied)?;
                if ticket.stamp.epoch != self.epoch
                    || ticket.stamp.binding != admission.binding
                    || ticket.stamp.secret_revision != state.secret_revision
                    || ticket.stamp.use_kind != RepositoryCredentialUse::NativePush
                {
                    return Err(RepositoryCredentialError::Retired);
                }
                admitted = pending.take();
                Ok(())
            })
            .map_err(unavailable)?;
        let (prepared, ticket) =
            admitted.ok_or_else(|| unavailable(RepositoryCredentialError::AuthorityDenied))?;
        prepared.execute(
            || git2::Cred::userpass_plaintext("oauth2", ticket.token.expose_secret()),
            observed,
        )
    }
}
