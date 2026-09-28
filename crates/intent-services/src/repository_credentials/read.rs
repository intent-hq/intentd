//! One qualified read and its actual response evidence. None of these metadata
//! objects grants caller authority or replaces the original secret reader.

use intent_core::{RepositoryResourceKind, ReviewTarget};
use intent_sourcecontrol::{
    error::ProviderFailureKind, GitLabSourceControl, Issue, RateLimitStatus, RepoRef,
    ReviewDetails, ReviewObservation, SourceControl,
};

use super::acquire::BoundGitlabRequestCredentials;
use super::authority::RepositoryCredentialTransport;
use super::*;

/// Captured from an actual admission; equality includes the original authority
/// allocation, not just serializable scope identifiers.
pub(crate) struct RepositoryReadScope {
    directory: Arc<RepositoryConnectionDirectory>,
    admission: RepositoryCredentialAdmission,
}

/// Lock planning only: every original scope remains separate. Allocation order
/// prevents opposite-order batches from recursively locking a shared directory.
pub(crate) struct RepositoryReadBatch<'a> {
    directories: Vec<Arc<RepositoryConnectionDirectory>>,
    members: Vec<(&'a RepositoryReadScope, usize)>,
}

impl RepositoryReadBatch<'_> {
    pub(crate) fn with_current(
        self,
        action: impl FnOnce(&[RepositorySecretRequest]) -> Result<()> + Send,
    ) -> Result<()> {
        let states = self
            .directories
            .iter()
            .map(|directory| directory.lock())
            .collect::<Result<Vec<_>>>()?;
        let selected = self
            .members
            .iter()
            .map(|(scope, index)| {
                scope
                    .directory
                    .read_metadata(&states[*index], &scope.admission)
            })
            .collect::<Result<Vec<_>>>()?;
        action(&selected)
    }
}

/// Separate output planner: only additional, optional-only locks may be skipped.
/// It neither permits an empty required read nor certifies an ordinary invocation.
pub(crate) struct RepositoryOutputBatch<'a> {
    directories: Vec<Arc<RepositoryConnectionDirectory>>,
    required: Vec<bool>,
    members: Vec<(&'a RepositoryReadScope, usize)>,
    optional: Option<usize>,
}

impl RepositoryOutputBatch<'_> {
    pub(crate) fn with_current(
        self,
        action: impl FnOnce(
                &[RepositorySecretRequest],
                Option<RepositoryConnectionMetadata<'_>>,
            ) -> Result<()>
            + Send,
    ) -> Result<()> {
        let states = self
            .directories
            .iter()
            .zip(&self.required)
            .map(|(directory, required)| {
                if *required {
                    directory.lock().map(Some)
                } else {
                    // Neither contention nor poison on an optional-only lock
                    // may delay or refuse otherwise valid required output.
                    Ok(directory.state.try_lock().ok())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let selected = self
            .members
            .iter()
            .map(|(scope, index)| {
                scope.directory.read_metadata(
                    states[*index]
                        .as_deref()
                        .expect("required directory locked"),
                    &scope.admission,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let optional = self.optional.and_then(|index| {
            states[index]
                .as_deref()
                .map(|state| self.directories[index].connection_metadata(state))
        });
        action(&selected, optional)
    }
}

impl RepositoryReadScope {
    /// Allocation planning only. The two owner-facing entry points separately
    /// enforce required-nonempty versus a future proven ordinary output branch.
    pub(crate) fn prepare_output<'a>(
        originals: &[&'a Self],
        optional: Option<&Arc<RepositoryConnectionDirectory>>,
    ) -> RepositoryOutputBatch<'a> {
        let mut directories = originals
            .iter()
            .map(|scope| scope.directory.clone())
            .chain(optional.cloned())
            .collect::<Vec<_>>();
        directories.sort_unstable_by_key(Arc::as_ptr);
        directories.dedup_by(|a, b| Arc::ptr_eq(a, b));
        let index = |directory: &Arc<RepositoryConnectionDirectory>| {
            directories
                .binary_search_by_key(&Arc::as_ptr(directory), Arc::as_ptr)
                .expect("original directory retained")
        };
        let members = originals
            .iter()
            .map(|scope| (*scope, index(&scope.directory)))
            .collect::<Vec<_>>();
        let optional = optional.map(index);
        let required = directories
            .iter()
            .map(|directory| {
                originals
                    .iter()
                    .any(|scope| Arc::ptr_eq(directory, &scope.directory))
            })
            .collect();
        RepositoryOutputBatch {
            directories,
            required,
            members,
            optional,
        }
    }
}

impl RepositoryReadScope {
    /// Gather retained allocations before the owner takes any metadata locks.
    pub(crate) fn prepare_all_current<'a>(
        originals: &[&'a Self],
    ) -> Result<RepositoryReadBatch<'a>> {
        if originals.is_empty() {
            return Err(RepositoryCredentialError::Unverified);
        }
        let mut directories = originals
            .iter()
            .map(|scope| scope.directory.clone())
            .collect::<Vec<_>>();
        directories.sort_unstable_by_key(Arc::as_ptr);
        directories.dedup_by(|left, right| Arc::ptr_eq(left, right));
        let members = originals
            .iter()
            .map(|scope| {
                let index = directories
                    .binary_search_by_key(&Arc::as_ptr(&scope.directory), Arc::as_ptr)
                    .expect("captured directory remains retained");
                (*scope, index)
            })
            .collect();
        Ok(RepositoryReadBatch {
            directories,
            members,
        })
    }

    pub(crate) fn capture(
        directory: Arc<RepositoryConnectionDirectory>,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<Self> {
        if admission.epoch != directory.epoch
            || admission.binding.daemon_id != directory.daemon_id
            || admission.request.use_kind != RepositoryCredentialUse::NativeRead
            || admission.child_revision.is_some()
            || admission.request.target.provider != RepositoryProvider::Gitlab
            || admission.request.allowed_transport
                != RepositoryCredentialTransport::GitlabApi(admission.descriptor.clone())
        {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        Ok(Self {
            directory,
            admission: RepositoryCredentialAdmission {
                epoch: admission.epoch,
                binding: admission.binding.clone(),
                descriptor: admission.descriptor.clone(),
                request: admission.request.clone(),
                authority: admission.authority.clone(),
                child_revision: None,
            },
        })
    }

    pub(crate) fn descriptor(&self) -> &GitlabDescriptor {
        &self.admission.descriptor
    }

    pub(crate) fn with_current(
        &self,
        action: &mut (dyn FnMut(&RepositorySecretRequest) -> Result<()> + Send),
    ) -> Result<()> {
        let state = self.directory.lock()?;
        let selected = self.directory.read_metadata(&state, &self.admission)?;
        action(&selected)
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.directory, &other.directory)
            && Arc::ptr_eq(&self.admission.authority, &other.admission.authority)
            && self.admission.epoch == other.admission.epoch
            && self.admission.binding == other.admission.binding
            && self.admission.descriptor == other.admission.descriptor
            && self.admission.request == other.admission.request
    }

    /// The owner holds config/descriptor before this directory lock and checks
    /// its settled source while the lock is still held. No authority or I/O here.
    pub(crate) fn with_response(
        &self,
        attribution: &RepositoryResponseAttribution,
        target: &ReviewTarget,
        action: &mut (dyn FnMut(
            RepositoryResponseDisposition,
            Option<&RepositorySecretRequest>,
        ) -> Result<()>
                  + Send),
    ) -> Result<()> {
        if !self.matches(&attribution.scope) || target != &attribution.target {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        let state = self.directory.lock()?;
        let (disposition, stamp) = match &attribution.evidence {
            ResponseEvidence::NoDenial => (RepositoryResponseDisposition::NoDenial, None),
            ResponseEvidence::Unattributed => (RepositoryResponseDisposition::Unattributed, None),
            ResponseEvidence::Indeterminate => (RepositoryResponseDisposition::Indeterminate, None),
            ResponseEvidence::Credential(accepted) => (
                if *accepted {
                    RepositoryResponseDisposition::AcceptedCredentialRejection
                } else {
                    RepositoryResponseDisposition::NotApplied
                },
                None,
            ),
            ResponseEvidence::Denial(kind, stamp) => (
                if *kind == ProviderFailureKind::ProjectDenied {
                    RepositoryResponseDisposition::CurrentProjectDenial
                } else {
                    RepositoryResponseDisposition::CurrentResourceDenial
                },
                Some(stamp),
            ),
        };
        let Some(stamp) = stamp else {
            // Accepted rejection is an event on the captured connection. Its
            // own disconnect must not erase it; it grants no current eligibility.
            return action(disposition, None);
        };
        let selected = match self.directory.read_metadata(&state, &self.admission) {
            Ok(selected) => selected,
            Err(RepositoryCredentialError::Retired | RepositoryCredentialError::Disconnected) => {
                return action(RepositoryResponseDisposition::NotApplied, None);
            }
            Err(error) => return Err(error),
        };
        if stamp.epoch != self.admission.epoch
            || stamp.binding != selected.binding
            || stamp.secret_revision != selected.secret_revision
            || stamp.use_kind != RepositoryCredentialUse::NativeRead
            || stamp.child_revision.is_some()
        {
            return action(RepositoryResponseDisposition::NotApplied, None);
        }
        action(disposition, Some(&selected))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryResponseDisposition {
    NoDenial,
    AcceptedCredentialRejection,
    CurrentProjectDenial,
    CurrentResourceDenial,
    NotApplied,
    Unattributed,
    Indeterminate,
}

enum ResponseEvidence {
    NoDenial,
    Credential(bool),
    Denial(ProviderFailureKind, RepositoryDispatchStamp),
    Unattributed,
    Indeterminate,
}

pub(crate) struct RepositoryResponseAttribution {
    scope: RepositoryReadScope,
    target: ReviewTarget,
    evidence: ResponseEvidence,
}

struct Response {
    stamp: RepositoryDispatchStamp,
    status: u16,
    rejection: Option<Result<bool>>,
}

/// A fixed typed provider call is sequential and exclusively owns its callback.
/// Retain only its last actual HTTP response, never a history or a token cache.
#[derive(Default)]
pub(super) struct ReadReceiptSink(Mutex<Option<Response>>);

impl ReadReceiptSink {
    pub(super) fn observe(
        &self,
        stamp: &RepositoryDispatchStamp,
        status: u16,
        rejection: Option<Result<bool>>,
    ) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some(Response {
                stamp: stamp.clone(),
                status,
                rejection,
            });
        }
    }

    fn seal<T>(
        &self,
        scope: RepositoryReadScope,
        target: ReviewTarget,
        result: &intent_sourcecontrol::Result<T>,
    ) -> RepositoryResponseAttribution {
        let evidence = match result {
            Err(intent_sourcecontrol::Error::Provider(failure))
                if matches!(
                    failure.kind,
                    ProviderFailureKind::CredentialRejected
                        | ProviderFailureKind::ProjectDenied
                        | ProviderFailureKind::ResourceDenied
                ) =>
            {
                match self.0.lock() {
                    Err(_) => ResponseEvidence::Indeterminate,
                    Ok(slot) => match slot.as_ref().filter(|r| Some(r.status) == failure.status) {
                        Some(response)
                            if failure.kind == ProviderFailureKind::CredentialRejected
                                && response.status == 401 =>
                        {
                            match response.rejection {
                                Some(Ok(accepted)) => ResponseEvidence::Credential(accepted),
                                Some(Err(_)) => ResponseEvidence::Indeterminate,
                                None => ResponseEvidence::Unattributed,
                            }
                        }
                        Some(response)
                            if matches!(response.status, 403 | 404)
                                && failure.kind != ProviderFailureKind::CredentialRejected =>
                        {
                            ResponseEvidence::Denial(failure.kind, response.stamp.clone())
                        }
                        _ => ResponseEvidence::Unattributed,
                    },
                }
            }
            _ => ResponseEvidence::NoDenial,
        };
        RepositoryResponseAttribution {
            scope,
            target,
            evidence,
        }
    }
}

pub(crate) struct RepositoryProviderRead<T> {
    result: intent_sourcecontrol::Result<T>,
    quota: RateLimitStatus,
    attribution: RepositoryResponseAttribution,
}

impl<T> RepositoryProviderRead<T> {
    pub(crate) fn into_parts(
        self,
    ) -> (
        intent_sourcecontrol::Result<T>,
        RateLimitStatus,
        RepositoryResponseAttribution,
    ) {
        (self.result, self.quota, self.attribution)
    }
}

/// Lazy and consuming: separate primary/full reads have separate response sinks.
/// Construction does not consult dispatch backoff or release a credential.
pub(crate) struct RepositoryReadOperation {
    scope: RepositoryReadScope,
    admission: RepositoryCredentialAdmission,
    reader: Arc<dyn RepositorySecretReader>,
    budget: Duration,
    target: ReviewTarget,
}

type ProviderFuture<'a, T> = std::pin::Pin<
    Box<dyn std::future::Future<Output = intent_sourcecontrol::Result<T>> + Send + 'a>,
>;

impl RepositoryReadOperation {
    pub(crate) fn new(
        directory: Arc<RepositoryConnectionDirectory>,
        admission: RepositoryCredentialAdmission,
        reader: Arc<dyn RepositorySecretReader>,
        budget: Duration,
        target: ReviewTarget,
    ) -> Result<Self> {
        let scope = RepositoryReadScope::capture(directory, &admission)?;
        if target.repository != admission.request.target
            || target.number == 0
            || !matches!(
                target.kind,
                RepositoryResourceKind::Issue | RepositoryResourceKind::MergeRequest
            )
        {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        Ok(Self {
            scope,
            admission,
            reader,
            budget,
            target,
        })
    }

    pub(crate) async fn read_issue(self) -> RepositoryProviderRead<Issue> {
        self.perform(RepositoryResourceKind::Issue, |p, r, n| {
            Box::pin(p.get_issue(r, n))
        })
        .await
    }

    pub(crate) async fn review_details(self) -> RepositoryProviderRead<ReviewDetails> {
        self.perform(RepositoryResourceKind::MergeRequest, |p, r, n| {
            Box::pin(p.review_details(r, n))
        })
        .await
    }

    pub(crate) async fn review_observation(self) -> RepositoryProviderRead<ReviewObservation> {
        self.perform(RepositoryResourceKind::MergeRequest, |p, r, n| {
            Box::pin(p.review_observation(r, n))
        })
        .await
    }

    async fn perform<T>(
        self,
        kind: RepositoryResourceKind,
        call: impl for<'a> FnOnce(&'a GitLabSourceControl, &'a RepoRef, u64) -> ProviderFuture<'a, T>,
    ) -> RepositoryProviderRead<T> {
        let sink = Arc::new(ReadReceiptSink::default());
        let provider = if self.target.kind == kind {
            BoundGitlabRequestCredentials::for_read(
                self.scope.directory.clone(),
                self.admission,
                self.reader,
                self.budget,
                sink.clone(),
            )
            .map_err(intent_sourcecontrol::Error::from)
            .and_then(BoundGitlabRequestCredentials::into_provider)
        } else {
            Err(RepositoryCredentialError::BoundaryMismatch.into())
        };
        let (result, quota) = match provider {
            Ok(provider) => {
                // The admitted provider-canonical project already has this shape.
                let (owner, name) = self
                    .target
                    .repository
                    .project_path
                    .rsplit_once('/')
                    .expect("admitted project");
                let repo = RepoRef {
                    owner: owner.into(),
                    name: name.into(),
                };
                let result = call(&provider, &repo, self.target.number).await;
                // GitLab's implementation reads its response metadata only.
                let quota = provider.rate_limit_status().await.unwrap_or_default();
                (result, quota)
            }
            Err(error) => (Err(error), RateLimitStatus::default()),
        };
        let attribution = sink.seal(self.scope, self.target, &result);
        RepositoryProviderRead {
            result,
            quota,
            attribution,
        }
    }
}

/// Borrowed local metadata. The owning projection validates config/proof while
/// this directory guard is held; these fields cannot authorize a request.
pub(crate) struct RepositoryConnectionMetadata<'a> {
    pub(crate) lifecycle: RepositoryConnectionState,
    pub(crate) mutation: Option<RepositoryMutationKind>,
    pub(crate) reserved: bool,
    pub(crate) ready: Result<(&'a GitlabDescriptor, RepositorySecretRequest)>,
    pub(crate) backoff_until: Option<Instant>,
    pub(crate) child_enabled: bool,
    pub(crate) child_revision: u64,
    pub(crate) child_pending: Option<bool>,
}

impl RepositoryConnectionDirectory {
    /// Status projection only. Does not change any admission/state predicate.
    pub(crate) fn with_connection_metadata<T>(
        &self,
        inspect: impl FnOnce(RepositoryConnectionMetadata<'_>) -> Result<T>,
    ) -> Result<T> {
        let state = self.lock()?;
        inspect(self.connection_metadata(&state))
    }

    fn connection_metadata<'a>(&self, state: &'a State) -> RepositoryConnectionMetadata<'a> {
        let ready = state.ready().and_then(|published| {
            if published.binding.daemon_id != self.daemon_id
                || published.binding.scope.connection_generation != state.generation
            {
                return Err(RepositoryCredentialError::Retired);
            }
            Ok((
                &published.verified.descriptor,
                RepositorySecretRequest {
                    binding: published.binding.clone(),
                    secret_revision: state.secret_revision,
                    source: published.verified.source,
                },
            ))
        });
        RepositoryConnectionMetadata {
            lifecycle: state.status,
            mutation: state.active.as_ref().map(|active| active.kind),
            reserved: state.reservation.is_some(),
            ready,
            backoff_until: state.backoff_until,
            child_enabled: state.child_enabled,
            child_revision: state.child_revision,
            child_pending: state
                .child_active
                .as_ref()
                .map(|active| active.indeterminate),
        }
    }
}

impl RepositoryConnectionDirectory {
    /// Original-owner metadata construction only, before any read admission.
    /// The owner already holds config/descriptor and checks proof while this
    /// state guard remains held. No caller, output, secret or I/O callback.
    pub(crate) fn with_settled_metadata<T>(
        &self,
        original: Option<&RepositoryConnectionBinding>,
        inspect: impl FnOnce(&GitlabDescriptor, &RepositorySecretRequest) -> Result<T>,
    ) -> Result<T> {
        let state = self.lock()?;
        if state.status == RepositoryConnectionState::Retired
            || original.is_some_and(|binding| {
                binding.daemon_id != self.daemon_id
                    || state.generation != binding.scope.connection_generation
                    || state
                        .published
                        .as_ref()
                        .is_none_or(|p| p.binding != *binding)
            })
        {
            return Err(RepositoryCredentialError::Retired);
        }
        let published = state.ready()?;
        if published.binding.daemon_id != self.daemon_id
            || published.binding.scope.connection_generation != state.generation
        {
            return Err(RepositoryCredentialError::Retired);
        }
        inspect(
            &published.verified.descriptor,
            &RepositorySecretRequest {
                binding: published.binding.clone(),
                secret_revision: state.secret_revision,
                source: published.verified.source,
            },
        )
    }

    /// Eligibility metadata only. This deliberately omits ONLY the dispatch
    /// deadline; acquisition/final HTTP admission still use `check_locked`.
    fn read_metadata(
        &self,
        state: &State,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<RepositorySecretRequest> {
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
        if published.verified.descriptor != admission.descriptor {
            return Err(RepositoryCredentialError::BoundaryMismatch);
        }
        Ok(RepositorySecretRequest {
            binding: published.binding.clone(),
            secret_revision: state.secret_revision,
            source: published.verified.source,
        })
    }
}
