//! Private, inactive repository operation admission and receipt engine.
//!
//! Entry/store/Git adapters and mutation-writer coverage are injected, not
//! installed. Public preparations are correlation projections, never grants.
//! Final dispatch is a local admission point: it cannot recall an effect already
//! admitted. Retiring a request prevents later stages; completed effects survive.

#[path = "repository_admission/authority.rs"]
mod authority;
#[path = "repository_admission/credential_bridge.rs"]
mod credential_bridge;
#[path = "repository_admission/lifecycle.rs"]
pub(crate) mod lifecycle;
#[path = "repository_admission/read_request.rs"]
pub(crate) mod read_request;
#[path = "repository_admission/request_context.rs"]
pub(crate) mod request_context;

pub(crate) use authority::{
    OriginalRepositoryCaller, RepositoryAgentIdentity, RepositoryAuthorityFacts,
    RepositoryAuthorityProvenance, RepositoryAuthoritySource, RepositoryEntry,
    RepositoryRetirement,
};

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::repository_credentials::RepositoryAuthorityRequest;
use intent_core::caller::CredentialLease;
use intent_core::{
    BoxFuture, NativeReviewDetails, NativeReviewExecution, NativeReviewGitReceipt,
    NativeReviewOutcome, NativeReviewPreparation, NativeReviewPublication, NativeReviewStage,
    RepositoryContextRevision,
};

/// Static local failures. These never classify a provider denial or log out a
/// forge account. No raw URL, token/hash, path or durable-store error is exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Denied,
    Unavailable,
    Retired,
    BindingChanged,
    InvalidPlan,
    StageOrder,
    InvalidCompletion,
}

pub(crate) type AdmissionResult<T> = Result<T, AdmissionError>;

/// Trusted operation facts. The effective original destinations stay private;
/// only the caller's already-sanitized preparation may be projected outward.
/// A matching path or DTO alone supplies no permission.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RepositoryOperationFacts {
    pub preparation: NativeReviewPreparation,
    pub worktree_path: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub source_ref: String,
    pub staging_fingerprint: Option<String>,
    pub fetch_destinations: Vec<String>,
    pub push_destinations: Vec<String>,
    /// Exact server-admitted requests, including the approved API transport.
    /// These are private input facts, never reconstructed from display URLs.
    pub credential_requests: Vec<RepositoryAuthorityRequest>,
}

impl RepositoryOperationFacts {
    fn valid(&self) -> bool {
        !self.preparation.operation_id.is_empty()
            && !self.preparation.worktree_id.is_empty()
            && self.worktree_path.is_absolute()
            && self.git_dir.is_absolute()
            && self.common_dir.is_absolute()
            && !self.source_ref.is_empty()
            && credential_bridge::valid_requests(self)
    }

    fn matches(&self, observed: &Self, progress: &OperationState) -> bool {
        if !matches!(
            observed.preparation.context_revision.compare_in_scopes(
                &observed.preparation.scope,
                &progress.last_revision,
                &self.preparation.scope,
            ),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        ) {
            return false;
        }
        let mut expected = self.clone();
        expected
            .preparation
            .local_head_sha
            .clone_from(&progress.expected_head);
        expected
            .staging_fingerprint
            .clone_from(&progress.expected_staging);
        // Other roots can advance the inventory sequence. Exact scope/epoch and
        // this operation's entire binding must still match, including all URLs.
        expected.preparation.context_revision = observed.preparation.context_revision.clone();
        expected == *observed
    }
}

/// A future adapter must read the captured root under the actual worktree lock,
/// then bounded durable authority. Tests inject these reads; no watcher, store
/// transaction, lock acquisition or remote request is implemented here.
pub(crate) trait RepositoryOperationSource: RepositoryAuthoritySource {
    fn observe<'a>(
        &'a self,
        original: &'a RepositoryOperationFacts,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryOperationFacts>>;
}

struct OperationState {
    next: usize,
    active: bool,
    terminal: bool,
    expected_head: Option<String>,
    expected_staging: Option<String>,
    last_revision: RepositoryContextRevision,
    receipts: Vec<NativeReviewGitReceipt>,
    outcome: NativeReviewOutcome,
    publication: NativeReviewPublication,
}

struct OperationInner {
    original: OriginalRepositoryCaller,
    source: Arc<dyn RepositoryOperationSource>,
    retirement: RepositoryRetirement,
    facts: RepositoryOperationFacts,
    authority: authority::AuthorityIdentity,
    request_id: String,
    stages: Vec<NativeReviewStage>,
    progress: Mutex<OperationState>,
}

/// Private construction only through capture after authentic caller validation.
/// Clones refer to this one operation; they never become a replacement request.
#[derive(Clone)]
pub(crate) struct RepositoryOperationAdmission {
    inner: Arc<OperationInner>,
}

fn stage_rank(stage: NativeReviewStage) -> u8 {
    match stage {
        NativeReviewStage::Commit => 0,
        NativeReviewStage::Push => 1,
        NativeReviewStage::CreatePr => 2,
    }
}

pub(crate) async fn capture_repository_operation(
    original: OriginalRepositoryCaller,
    request_id: String,
    facts: RepositoryOperationFacts,
    stages: Vec<NativeReviewStage>,
    source: Arc<dyn RepositoryOperationSource>,
    retirement: RepositoryRetirement,
) -> AdmissionResult<RepositoryOperationAdmission> {
    if request_id.is_empty()
        || !facts.valid()
        || stages.is_empty()
        || !stages
            .windows(2)
            .all(|s| stage_rank(s[0]) < stage_rank(s[1]))
    {
        return Err(AdmissionError::InvalidPlan);
    }
    retirement.check_current()?;
    let observed = source.observe(&facts).await?;
    if facts != observed {
        return Err(AdmissionError::BindingChanged);
    }
    let _legacy = original.legacy_lease().await?;
    let authority = source
        .read(&original, &facts.preparation.root.workspace_id)
        .await?;
    original.verify(&authority, &facts.preparation.root.workspace_id)?;
    if stages.iter().any(|stage| !authority.permits(*stage)) {
        return Err(AdmissionError::Denied);
    }
    retirement.check_current()?;
    let progress = OperationState {
        next: 0,
        active: false,
        terminal: false,
        expected_head: facts.preparation.local_head_sha.clone(),
        expected_staging: facts.staging_fingerprint.clone(),
        last_revision: facts.preparation.context_revision.clone(),
        receipts: Vec::new(),
        outcome: NativeReviewOutcome::NotAttempted,
        publication: NativeReviewPublication::Unknown {
            local_head_sha: facts.preparation.local_head_sha.clone(),
            remote_source_sha: None,
        },
    };
    Ok(RepositoryOperationAdmission {
        inner: Arc::new(OperationInner {
            original,
            source,
            retirement,
            facts,
            authority: authority.identity(),
            request_id,
            stages,
            progress: Mutex::new(progress),
        }),
    })
}

/// One revalidation, retaining the original legacy lease only until dispatch.
/// This cannot be serialized, cloned or constructed from a preparation.
pub(crate) struct CheckedRepositoryStage {
    inner: Arc<OperationInner>,
    stage: NativeReviewStage,
    index: usize,
    _legacy: Option<CredentialLease>,
}

fn check_stage(
    progress: &OperationState,
    inner: &OperationInner,
    stage: NativeReviewStage,
) -> AdmissionResult<()> {
    if progress.terminal || progress.active || inner.stages.get(progress.next) != Some(&stage) {
        return Err(AdmissionError::StageOrder);
    }
    Ok(())
}

pub(crate) async fn revalidate_repository_stage(
    operation: &RepositoryOperationAdmission,
    stage: NativeReviewStage,
) -> AdmissionResult<CheckedRepositoryStage> {
    let inner = &operation.inner;
    inner.retirement.check_current()?;
    {
        let progress = inner.progress.lock().map_err(|_| AdmissionError::Retired)?;
        check_stage(&progress, inner, stage)?;
    }
    // Queued work calls here after entering its actual worker/worktree lock,
    // not before a potentially unbounded queue wait.
    let (observed, legacy) = observe_authority(inner, stage).await?;
    let index = inner.retirement.dispatch(|| {
        let mut progress = inner.progress.lock().map_err(|_| AdmissionError::Retired)?;
        check_stage(&progress, inner, stage)?;
        if !inner.facts.matches(&observed, &progress) {
            return Err(AdmissionError::BindingChanged);
        }
        progress.last_revision = observed.preparation.context_revision.clone();
        Ok(progress.next)
    });
    if index == Err(AdmissionError::BindingChanged) {
        inner.retirement.retire();
    }
    Ok(CheckedRepositoryStage {
        inner: inner.clone(),
        stage,
        index: index?,
        _legacy: legacy,
    })
}

/// Shared original-caller and root read for stage admission and every credential
/// release within that stage. No state lock crosses the async reads; the original
/// legacy admission is retained through the later dispatch fence.
async fn observe_authority(
    inner: &OperationInner,
    stage: NativeReviewStage,
) -> AdmissionResult<(RepositoryOperationFacts, Option<CredentialLease>)> {
    let observed = inner.source.observe(&inner.facts).await?;
    let legacy = retire_denied(inner, inner.original.legacy_lease().await)?;
    let facts = retire_denied(
        inner,
        inner
            .source
            .read(&inner.original, &inner.facts.preparation.root.workspace_id)
            .await,
    )?;
    if inner
        .original
        .verify(&facts, &inner.facts.preparation.root.workspace_id)
        .is_err()
        || facts.identity() != inner.authority
        || !facts.permits(stage)
    {
        inner.retirement.retire();
        return Err(AdmissionError::Denied);
    }
    Ok((observed, legacy))
}

fn retire_denied<T>(inner: &OperationInner, result: AdmissionResult<T>) -> AdmissionResult<T> {
    if matches!(
        &result,
        Err(AdmissionError::Denied | AdmissionError::Retired)
    ) {
        inner.retirement.retire();
    }
    result
}

/// One admitted effect attempt. Drop without classification records uncertainty
/// and stops the plan; missing completion evidence never licenses a retry.
pub(crate) struct RepositoryDispatchStamp {
    inner: Arc<OperationInner>,
    stage: NativeReviewStage,
    index: usize,
    classified: bool,
}

/// Call at the actual worker's dispatch edge, with no intervening queue/await
/// before starting the stage. A future delayed worker must revalidate there.
/// The fence guards only the admission decision; it is dropped before I/O.
pub(crate) fn begin_repository_stage(
    checked: CheckedRepositoryStage,
) -> AdmissionResult<RepositoryDispatchStamp> {
    let result = checked.inner.retirement.dispatch(|| {
        let mut progress = checked
            .inner
            .progress
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        check_stage(&progress, &checked.inner, checked.stage)?;
        if progress.next != checked.index {
            return Err(AdmissionError::StageOrder);
        }
        progress.active = true;
        Ok(RepositoryDispatchStamp {
            inner: checked.inner.clone(),
            stage: checked.stage,
            index: checked.index,
            classified: false,
        })
    });
    // Consume the check and release its original credential lease before I/O.
    drop(checked);
    result
}

impl Drop for RepositoryDispatchStamp {
    fn drop(&mut self) {
        if !self.classified {
            if let Ok(mut progress) = self.inner.progress.lock() {
                if progress.active && progress.next == self.index {
                    progress.active = false;
                    progress.terminal = true;
                    progress.outcome = NativeReviewOutcome::Uncertain {
                        stage: self.stage,
                        message: "Stage completion was not observed".to_owned(),
                    };
                }
            }
        }
    }
}

/// Trusted executor/provider observations, not client-supplied command results.
pub(crate) enum RepositoryCompletion {
    Committed {
        hash: String,
        staging_after: Option<String>,
    },
    Pushed {
        sha: String,
    },
    Created(Box<NativeReviewDetails>),
    Reused(Box<NativeReviewDetails>),
    Failed {
        code: Option<String>,
        message: String,
    },
    Uncertain {
        message: String,
    },
}

pub(crate) fn classify_repository_completion(
    mut stamp: RepositoryDispatchStamp,
    completion: RepositoryCompletion,
) -> AdmissionResult<NativeReviewExecution> {
    {
        let mut progress = stamp
            .inner
            .progress
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if !progress.active || progress.next != stamp.index {
            return Err(AdmissionError::StageOrder);
        }
        match completion {
            RepositoryCompletion::Committed {
                hash,
                staging_after,
            } if stamp.stage == NativeReviewStage::Commit && !hash.is_empty() => {
                progress.receipts.push(NativeReviewGitReceipt::Commit {
                    commit_hash: hash.clone(),
                });
                progress.expected_head = Some(hash.clone());
                progress.expected_staging = staging_after;
                progress.publication = NativeReviewPublication::Unknown {
                    local_head_sha: Some(hash),
                    remote_source_sha: None,
                };
            }
            RepositoryCompletion::Pushed { sha }
                if stamp.stage == NativeReviewStage::Push && !sha.is_empty() =>
            {
                progress
                    .receipts
                    .push(NativeReviewGitReceipt::Push { pushed_sha: sha });
            }
            RepositoryCompletion::Created(review) if valid_review(&stamp, &review) => {
                progress.outcome = NativeReviewOutcome::Created { review };
            }
            RepositoryCompletion::Reused(review) if valid_review(&stamp, &review) => {
                progress.outcome = NativeReviewOutcome::Reused { review };
            }
            RepositoryCompletion::Failed { code, message } => {
                progress.outcome = NativeReviewOutcome::Failed {
                    stage: stamp.stage,
                    code,
                    message,
                };
                progress.terminal = true;
            }
            RepositoryCompletion::Uncertain { message } => {
                progress.outcome = NativeReviewOutcome::Uncertain {
                    stage: stamp.stage,
                    message,
                };
                progress.terminal = true;
            }
            _ => return Err(AdmissionError::InvalidCompletion),
        }
        progress.active = false;
        progress.next += 1;
        progress.terminal |= progress.next == stamp.inner.stages.len();
    }
    stamp.classified = true;
    execution(&stamp.inner)
}

fn valid_review(stamp: &RepositoryDispatchStamp, review: &NativeReviewDetails) -> bool {
    use intent_core::{RepositoryProvider, RepositoryResourceKind};
    stamp.stage == NativeReviewStage::CreatePr
        && review.resource.repository == stamp.inner.facts.preparation.target.repository
        && review.resource.kind
            == match review.resource.repository.provider {
                RepositoryProvider::Github => RepositoryResourceKind::PullRequest,
                RepositoryProvider::Gitlab => RepositoryResourceKind::MergeRequest,
            }
}

fn execution(inner: &OperationInner) -> AdmissionResult<NativeReviewExecution> {
    let progress = inner.progress.lock().map_err(|_| AdmissionError::Retired)?;
    Ok(NativeReviewExecution {
        request_id: inner.request_id.clone(),
        preparation: inner.facts.preparation.clone(),
        git_receipts: progress.receipts.clone(),
        outcome: progress.outcome.clone(),
        publication: progress.publication.clone(),
    })
}

impl RepositoryOperationAdmission {
    /// Preserve the old operation's receipts; caller disclosure authorization is
    /// still required by the eventual response adapter, not supplied by this API.
    pub(crate) fn execution(&self) -> AdmissionResult<NativeReviewExecution> {
        execution(&self.inner)
    }

    /// Pre-dispatch failure only. If a stage was already admitted, its stamp owns
    /// classification and may still report completion or uncertainty after retire.
    pub(crate) fn fail_before_dispatch(
        &self,
        stage: NativeReviewStage,
        error: AdmissionError,
    ) -> AdmissionResult<NativeReviewExecution> {
        {
            let mut progress = self
                .inner
                .progress
                .lock()
                .map_err(|_| AdmissionError::Retired)?;
            check_stage(&progress, &self.inner, stage)?;
            progress.outcome = NativeReviewOutcome::Failed {
                stage,
                code: Some(
                    match error {
                        AdmissionError::Denied => "repository-authority-denied",
                        AdmissionError::Unavailable => "repository-authority-unavailable",
                        AdmissionError::Retired => "repository-admission-retired",
                        AdmissionError::BindingChanged => "repository-binding-changed",
                        _ => "repository-stage-invalid",
                    }
                    .to_owned(),
                ),
                message: "Repository operation could not continue".to_owned(),
            };
            progress.terminal = true;
        }
        self.execution()
    }

    /// Supplied only by a trusted Git observation/ancestry adapter. No create,
    /// push receipt or unequal SHA comparison is treated as publication proof.
    pub(crate) fn record_publication(
        &self,
        evidence: NativeReviewPublication,
    ) -> AdmissionResult<()> {
        let mut progress = self
            .inner
            .progress
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        let local_head = match &evidence {
            NativeReviewPublication::Included { local_head_sha, .. }
            | NativeReviewPublication::LocalAhead { local_head_sha, .. }
            | NativeReviewPublication::Diverged { local_head_sha, .. } => Some(local_head_sha),
            NativeReviewPublication::RemoteBranchMissing { local_head_sha }
            | NativeReviewPublication::Unknown { local_head_sha, .. } => local_head_sha.as_ref(),
        };
        if local_head != progress.expected_head.as_ref() {
            return Err(AdmissionError::BindingChanged);
        }
        progress.publication = evidence;
        Ok(())
    }
}

#[cfg(test)]
#[path = "repository_admission/tests.rs"]
mod tests;

// Services keeps its native adapter outside this Services-free engine. These
// crate-private entries preserve the same original retirement implementation.
impl RepositoryRetirement {
    pub(crate) fn native_dispatch<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        self.dispatch(action)
    }
    pub(crate) async fn native_cancelled(&self) {
        self.cancelled().await;
    }
}
