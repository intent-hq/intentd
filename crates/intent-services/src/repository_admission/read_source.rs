//! Original Agent/MCP reads. Local observations never supply permission; the
//! existing member gate, retained request and actual credential owner all apply.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::mcp_server::private_results::{McpHostCall, McpReadReservation};
use intent_core::caller::{current_caller, Caller};
use intent_core::{
    resolve_review_selection, ExecutionScope, RepositoryProvider, RepositoryResourceKind,
    RepositoryRootId, RepositoryRootKind, ReviewSelectionOutcome, ReviewTarget,
    SavedReviewSelection, WorkspaceId,
};
use intent_sourcecontrol::remote_project::{CanonicalRemoteResolver, RemoteInstance};
use intent_sourcecontrol::{RepoRef, ReviewObservation};
use intent_store::{RepositoryLifecycleKey, RepositoryWorkspaceAuthoritySnapshot};

use crate::pr_monitor::qualified_cache::{
    read_managed_review, CacheAdmissionGuard, CacheFailure, CacheRead, CacheRequest,
    ManagedCacheRequest,
};
use crate::pr_monitor::PrReadPolicy;
use crate::repository_admission::read_request::{RepositoryReadChild, RepositoryReadRequest};
use crate::repository_admission::{AdmissionError, AdmissionResult, RepositoryAgentIdentity};
use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};
use crate::repository_admission_sources::read_agent_identity;
use crate::repository_context_reader::{RepositoryChangeInputs, RepositoryObservedRoot};
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthorityFence, RepositoryCredentialTransport,
};
use crate::repository_credentials::read::RepositoryReadOperation;
use crate::repository_credentials::{
    self as credentials, RepositoryAuthority, RepositoryAuthorityRequest,
    RepositoryCredentialError, RepositoryCredentialUse,
};
use crate::repository_read_policy::ReadHost;
use crate::source_control_auth_ops::repository_owner::{
    RepositoryReadEligibility, RepositorySettledConnection,
};
use crate::Services;

pub(crate) const REFUSAL: &str = "Private repository read unavailable";

pub(crate) fn refused() -> intent_core::Error {
    intent_core::Error::Forbidden(REFUSAL.into())
}

fn local(error: &intent_core::Error) -> AdmissionError {
    match error {
        intent_core::Error::Forbidden(_) | intent_core::Error::NotFound(_) => {
            AdmissionError::Denied
        }
        _ => AdmissionError::Unavailable,
    }
}

fn credential(error: AdmissionError) -> RepositoryCredentialError {
    match error {
        AdmissionError::Retired => RepositoryCredentialError::Retired,
        AdmissionError::Unavailable => RepositoryCredentialError::AuthorityUnavailable,
        _ => RepositoryCredentialError::AuthorityDenied,
    }
}

#[derive(Clone, PartialEq, Eq)]
struct MemberFacts {
    workspace: RepositoryWorkspaceAuthoritySnapshot,
    agent: RepositoryAgentIdentity,
}

async fn member(
    services: &Services,
    request: &RepositoryReadRequest,
    workspace: &WorkspaceId,
) -> AdmissionResult<MemberFacts> {
    request.check_current()?;
    let caller = current_caller().ok_or(AdmissionError::Denied)?;
    if !matches!(caller, Caller::Agent { .. }) {
        return Err(AdmissionError::Denied);
    }
    let before = services
        .store
        .repository_workspace_authority_snapshot(workspace)
        .await
        .map_err(|e| local(&e))?;
    let agent = read_agent_identity(services, &caller)
        .await?
        .ok_or(AdmissionError::Denied)?;
    if agent.workspace_id != *workspace
        || before.workspace.value.is_none()
        || before.workspace.revision.is_none()
    {
        return Err(AdmissionError::Denied);
    }
    services
        .require_member(workspace)
        .await
        .map_err(|e| local(&e))?;
    let after = services
        .store
        .repository_workspace_authority_snapshot(workspace)
        .await
        .map_err(|e| local(&e))?;
    let current = read_agent_identity(services, &caller).await?;
    request.check_current()?;
    if before != after || current.as_ref() != Some(&agent) {
        return Err(AdmissionError::BindingChanged);
    }
    Ok(MemberFacts {
        workspace: before,
        agent,
    })
}

fn subscribe(child: &mut RepositoryReadChild, workspace: &WorkspaceId) -> AdmissionResult<()> {
    let Some(Caller::Agent { agent_id }) = current_caller() else {
        return Err(AdmissionError::Denied);
    };
    child.subscribe(&[
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::Workspace(workspace.clone()),
        RepositoryLifecycleKey::Agent(agent_id),
    ])
}

fn resolver(
    settled: Option<&RepositorySettledConnection>,
) -> AdmissionResult<CanonicalRemoteResolver> {
    let mut instances = vec![RemoteInstance::github_com()];
    if let Some(settled) = settled {
        instances.push(RemoteInstance::gitlab(
            settled.descriptor().instance().clone(),
        ));
    }
    // There is no installed GitLab alias mapping in this producer. In particular
    // the HTTP loopback fixture and SSH host spelling are never logical aliases.
    CanonicalRemoteResolver::new(instances, Vec::new()).map_err(|_| AdmissionError::Unavailable)
}

fn target(observed: &RepositoryObservedRoot) -> AdmissionResult<intent_core::RepositoryTarget> {
    match resolve_review_selection(&SavedReviewSelection::Automatic, &observed.remotes, None)
        .outcome
    {
        ReviewSelectionOutcome::Resolved { target, .. } => Ok(target),
        _ => Err(AdmissionError::Unavailable),
    }
}

/// Kept private, without Debug/Serde. Equality checks every original local fact.
#[derive(PartialEq, Eq)]
struct GitFacts {
    root: RepositoryRootId,
    branch: Option<String>,
    head: Option<String>,
    changes: RepositoryChangeInputs,
    source_ref: Option<String>,
    transports: Vec<(String, Vec<String>, Vec<String>)>,
}

impl GitFacts {
    fn from(observed: RepositoryObservedRoot) -> Self {
        Self {
            root: observed.root,
            branch: observed.branch,
            head: observed.head_sha,
            changes: observed.change_inputs,
            source_ref: observed.private_root.source_ref,
            transports: observed
                .private_root
                .remotes
                .into_iter()
                .map(|r| (r.name, r.fetch, r.push))
                .collect(),
        }
    }
}

pub(crate) enum ReadOutcome {
    Github(RepoRef),
    Managed {
        target: ReviewTarget,
        result: Box<Result<CacheRead<ReviewObservation>, CacheFailure>>,
    },
}

/// Captured synchronously by `pr_state`, before `execution_call` or permission I/O.
/// Failed qualified capture does not impose a new gate on ordinary GitHub.
pub(crate) struct CapturedReview {
    host: AdmissionResult<ReadHost>,
    child: AdmissionResult<RepositoryReadChild>,
    settled: credentials::Result<RepositorySettledConnection>,
    workspace: WorkspaceId,
    number: u64,
}

impl CapturedReview {
    pub(crate) fn capture(services: &Services, workspace: WorkspaceId, number: u64) -> Self {
        let host = crate::repository_read_policy::capture(services);
        let child = host.as_ref().map_err(|e| *e).and_then(|host| {
            let mut child = host.request().child()?;
            subscribe(&mut child, &workspace)?;
            Ok(child)
        });
        Self {
            host,
            child,
            settled: services.gitlab_repository_settled_connection(),
            workspace,
            number,
        }
    }

    pub(crate) async fn read(mut self, services: &Services) -> AdmissionResult<ReadOutcome> {
        services
            .require_member(&self.workspace)
            .await
            .map_err(|e| local(&e))?;
        let root_id = RepositoryRootId {
            workspace_id: self.workspace.clone(),
            kind: RepositoryRootKind::Primary,
        };
        let root = RootRecord::read(&services.store, &root_id).await?;
        if let Ok(child) = self.child.as_mut() {
            if let Err(error) =
                child.subscribe(&[RepositoryLifecycleKey::Database, root.lifecycle_key()])
            {
                self.child = Err(error);
            }
        }
        let observed = root
            .observe_local(
                &services.store,
                &services.worktree_locks,
                resolver(self.settled.as_ref().ok())?,
            )
            .await?;
        services
            .require_member(&self.workspace)
            .await
            .map_err(|e| local(&e))?;
        let selected = target(&observed)?;
        if selected.provider == RepositoryProvider::Github {
            let (owner, name) = selected
                .project_path
                .rsplit_once('/')
                .ok_or(AdmissionError::Unavailable)?;
            return Ok(ReadOutcome::Github(RepoRef::new(owner, name)));
        }
        let host = self.host?;
        host.check_current()?;
        read_review(
            &host,
            self.workspace,
            self.number,
            self.child?,
            self.settled,
            root,
            GitFacts::from(observed),
        )
        .await
    }
}

/// A persistent obligation, never a reusable acquisition fence.
pub(crate) struct ReadRecord {
    pub(crate) request: Arc<RepositoryReadRequest>,
    facts: Arc<ReadFacts>,
    pub(crate) operation: Arc<Mutex<ReadState>>,
    pub(crate) eligibility: Arc<RepositoryReadEligibility>,
    #[cfg(test)]
    acquisition: Arc<ReadAuthority>,
}

#[derive(PartialEq, Eq)]
pub(crate) enum ReadState {
    Acquiring,
    Finished,
    Abandoned,
}

struct ReadFacts {
    services: Arc<Services>,
    root: RootRecord,
    member: MemberFacts,
    git: GitFacts,
    settled: RepositorySettledConnection,
    request: RepositoryAuthorityRequest,
}

struct FinishRead(Arc<Mutex<ReadState>>);

impl FinishRead {
    fn finish(&self) -> AdmissionResult<()> {
        *self.0.lock().map_err(|_| AdmissionError::Retired)? = ReadState::Finished;
        Ok(())
    }
}

impl Drop for FinishRead {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lock() {
            if *state == ReadState::Acquiring {
                *state = ReadState::Abandoned;
            }
        }
    }
}

impl ReadFacts {
    async fn validate_local(
        &self,
        request: &RepositoryReadRequest,
        git: &RepositoryGitSource,
    ) -> AdmissionResult<()> {
        request.check_current()?;
        let authority = member(&self.services, request, &self.git.root.workspace_id).await?;
        let observed = git.observe_root(resolver(Some(&self.settled))?).await?;
        if authority != self.member
            || target(&observed)? != self.request.target
            || GitFacts::from(observed) != self.git
        {
            return Err(AdmissionError::BindingChanged);
        }
        request.check_current()
    }

    async fn validate(
        &self,
        request: &RepositoryReadRequest,
        git: &RepositoryGitSource,
    ) -> AdmissionResult<()> {
        let settled = self
            .settled
            .reobserve()
            .map_err(|_| AdmissionError::Unavailable)?;
        self.validate_local(request, git).await?;
        let after = self
            .settled
            .reobserve()
            .map_err(|_| AdmissionError::Unavailable)?;
        if after.selected() != settled.selected() || after.descriptor() != settled.descriptor() {
            return Err(AdmissionError::Unavailable);
        }
        request.check_current()
    }
}

struct ReadAuthority {
    request: Arc<RepositoryReadRequest>,
    child: Arc<RepositoryReadChild>,
    facts: Arc<ReadFacts>,
    git: Arc<RepositoryGitSource>,
    operation: Arc<Mutex<ReadState>>,
}

impl ReadAuthority {
    async fn finish_response(&self) {
        // This runs before cache admission and outside cache/metadata locks.
        // Preserve the original opaque response, including its own disconnect
        // and raw quota/error evidence; only the original source lifetime is fenced.
        if let Err(error) = self.facts.validate_local(&self.request, &self.git).await {
            if matches!(
                error,
                AdmissionError::Denied | AdmissionError::BindingChanged | AdmissionError::Retired
            ) {
                self.child.retirement().retire();
            } else {
                self.child.retirement().end_scope();
            }
        }
    }

    fn with_current<T>(
        &self,
        action: impl FnOnce() -> credentials::Result<T>,
    ) -> credentials::Result<T> {
        self.child
            .transfer(|| {
                let state = self.operation.lock().map_err(|_| AdmissionError::Retired)?;
                if *state != ReadState::Acquiring {
                    return Err(AdmissionError::Retired);
                }
                // Return the exact credential/provider-local action result unchanged.
                Ok(action())
            })
            .map_err(credential)?
    }
}

struct ReadFence(Arc<ReadAuthority>);

impl RepositoryAuthorityFence for ReadFence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> credentials::Result<()> + Send),
    ) -> credentials::Result<()> {
        self.0.with_current(action)
    }
}

// This implementation is on the retained allocation so returned fences can only
// own this exact source, never reconstruct a later authority from its fields.
impl RepositoryAuthority for Arc<ReadAuthority> {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if request != &self.facts.request {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            self.child
                .retirement()
                .check_current()
                .map_err(credential)?;
            self.facts
                .validate(&self.request, &self.git)
                .await
                .inspect_err(|error| {
                    if matches!(
                        error,
                        AdmissionError::Denied
                            | AdmissionError::BindingChanged
                            | AdmissionError::Retired
                    ) {
                        self.child.retirement().retire();
                    }
                })
                .map_err(credential)?;
            self.with_current(|| Ok(()))?;
            Ok(Box::new(ReadFence(self.clone())) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

impl ReadRecord {
    fn bind(&self, slot: McpReadReservation) -> AdmissionResult<()> {
        slot.bind(Arc::new(Self {
            request: self.request.clone(),
            facts: self.facts.clone(),
            operation: self.operation.clone(),
            eligibility: self.eligibility.clone(),
            #[cfg(test)]
            acquisition: self.acquisition.clone(),
        }))
        .map_err(|_| AdmissionError::Retired)
    }
}

/// Original local routing may precede reservation. Every subsequent qualified
/// access, including a cached clone, is inside its original reserved obligation.
async fn read_review(
    host: &ReadHost,
    workspace: WorkspaceId,
    number: u64,
    mut child: RepositoryReadChild,
    settled: credentials::Result<RepositorySettledConnection>,
    original_root: RootRecord,
    original_git: GitFacts,
) -> AdmissionResult<ReadOutcome> {
    host.check_current()?;
    if number == 0 {
        return Err(AdmissionError::Denied);
    }
    let member_facts = member(host.services(), host.request(), &workspace).await?;
    let root_id = RepositoryRootId {
        workspace_id: workspace,
        kind: RepositoryRootKind::Primary,
    };
    let root = RootRecord::read(&host.services().store, &root_id).await?;
    if root != original_root {
        return Err(AdmissionError::BindingChanged);
    }
    child.subscribe(&[RepositoryLifecycleKey::Database, root.lifecycle_key()])?;
    let child = Arc::new(child);
    let resolve = resolver(settled.as_ref().ok())?;
    let retirement = child.retirement();
    RepositoryGitSource::with_locked(
        &host.services().store,
        &host.services().worktree_locks,
        root.clone(),
        child.retirement(),
        |git| async move {
            let observed = git.observe_root(resolve).await?;
            let selected_target = target(&observed)?;
            let git_facts = GitFacts::from(observed);
            if git_facts != original_git || selected_target.provider != RepositoryProvider::Gitlab {
                return Err(AdmissionError::BindingChanged);
            }
            if member(host.services(), host.request(), &root_id.workspace_id).await? != member_facts
            {
                return Err(AdmissionError::BindingChanged);
            }
            host.check_current()?;
            let settled = settled.map_err(|_| AdmissionError::Unavailable)?;
            let fresh = settled
                .reobserve()
                .map_err(|_| AdmissionError::Unavailable)?;
            if fresh.selected() != settled.selected() || fresh.descriptor() != settled.descriptor()
            {
                return Err(AdmissionError::Unavailable);
            }
            let slot = host.reserve()?;
            let execution = ExecutionScope {
                daemon_id: host.services().daemon_boot_id.clone(),
                authority_scope_id: host.request().correlation().to_owned(),
                authority_generation: member_facts
                    .workspace
                    .workspace
                    .revision
                    .ok_or(AdmissionError::Unavailable)?
                    .get(),
            };
            let request = RepositoryAuthorityRequest {
                execution: execution.clone(),
                target: selected_target.clone(),
                connection: settled.selected().binding.scope.clone(),
                use_kind: RepositoryCredentialUse::NativeRead,
                allowed_transport: RepositoryCredentialTransport::GitlabApi(
                    settled.descriptor().clone(),
                ),
            };
            let facts = Arc::new(ReadFacts {
                services: host.services().clone(),
                root,
                member: member_facts,
                git: git_facts,
                settled,
                request,
            });
            let operation = Arc::new(Mutex::new(ReadState::Acquiring));
            let finish = FinishRead(operation.clone());
            let authority = Arc::new(ReadAuthority {
                request: host.request().clone(),
                child,
                facts: facts.clone(),
                git,
                operation: operation.clone(),
            });
            let bridge: Arc<dyn RepositoryAuthority> = Arc::new(authority.clone());
            let directory = host.services().repository_connection_directory();
            let admit = || {
                directory.admit(
                    &facts.settled.selected().binding,
                    facts.request.clone(),
                    bridge.clone(),
                )
            };
            let primary_admission = admit().map_err(|_| AdmissionError::Unavailable)?;
            let eligibility = Arc::new(
                host.services()
                    .gitlab_repository_read_eligibility(&primary_admission)
                    .map_err(|_| AdmissionError::Unavailable)?,
            );
            let record = ReadRecord {
                request: host.request().clone(),
                facts: facts.clone(),
                operation,
                eligibility,
                #[cfg(test)]
                acquisition: authority.clone(),
            };
            record.bind(slot)?;
            let reader = host
                .services()
                .gitlab_repository_secret_reader()
                .map_err(|_| AdmissionError::Unavailable)?;
            let review = ReviewTarget {
                repository: selected_target,
                kind: RepositoryResourceKind::MergeRequest,
                number,
            };
            let primary = RepositoryReadOperation::new(
                directory.clone(),
                primary_admission,
                reader.clone(),
                Duration::from_secs(30),
                review.clone(),
            )
            .map_err(|_| AdmissionError::Unavailable)?;
            let full_admission = admit().map_err(|_| AdmissionError::Unavailable)?;
            let full = RepositoryReadOperation::new(
                directory,
                full_admission,
                reader,
                Duration::from_secs(30),
                review.clone(),
            )
            .map_err(|_| AdmissionError::Unavailable)?;
            let connection = host.connection(execution, facts.request.connection.clone())?;
            let revalidate = || host.check_current().map_err(|_| refused());
            let guard: &CacheAdmissionGuard<'_> = &|action| authority.with_current(action);
            let cache_request = ManagedCacheRequest {
                request: CacheRequest {
                    connection: &connection,
                    target: &review,
                    revalidate: &revalidate,
                },
                eligibility: &record.eligibility,
                with_authority: guard,
            };
            // The cache's provider callback is intentionally infallible/opaque.
            // A failed lazy reservation cancels this same cache future; it never
            // fabricates a provider envelope or starts the forbidden subread.
            let (refuse, refusal) = tokio::sync::oneshot::channel();
            let monitored = HashSet::new();
            let full_read = || async {
                if reserve_subread(host.call(), &record).is_err() {
                    let _ = refuse.send(());
                    return std::future::pending().await;
                }
                let response = full.review_observation().await;
                authority.finish_response().await;
                response
            };
            let read = read_managed_review(
                &host.services().pr_cache,
                &cache_request,
                PrReadPolicy::Serve {
                    max_age: host.services().pr_cache_max_age(),
                },
                &monitored,
                || async {
                    let response = primary.review_details().await;
                    authority.finish_response().await;
                    response
                },
                full_read,
            );
            tokio::pin!(read);
            let result = tokio::select! {
                biased;
                Ok(()) = refusal => return Err(AdmissionError::Retired),
                result = &mut read => result,
            };
            finish.finish()?;
            Ok(ReadOutcome::Managed {
                target: review.clone(),
                result: Box::new(result),
            })
        },
    )
    .await
    .inspect_err(|error| {
        if matches!(
            error,
            AdmissionError::Denied | AdmissionError::BindingChanged | AdmissionError::Retired
        ) {
            retirement.retire();
        }
    })
}

fn reserve_subread(call: &McpHostCall, record: &ReadRecord) -> AdmissionResult<()> {
    record.bind(call.reserve().map_err(|_| AdmissionError::Retired)?)
}

/// Fresh simultaneous validation for one original boundary. All source locks
/// remain live until the synchronous callback finishes; no checked fence escapes.
pub(crate) async fn with_records<T: Send>(
    request: &Arc<RepositoryReadRequest>,
    services: &Arc<Services>,
    records: &[&ReadRecord],
    transfer: impl FnOnce() -> T + Send,
) -> AdmissionResult<T> {
    request.check_current()?;
    let current = crate::repository_admission::request_context::current_read_request()?;
    if !Arc::ptr_eq(request, &current) {
        return Err(AdmissionError::Denied);
    }
    if records.is_empty()
        || records
            .iter()
            .any(|r| !Arc::ptr_eq(&r.request, request) || !Arc::ptr_eq(&r.facts.services, services))
    {
        return Err(AdmissionError::Denied);
    }
    let mut child = request.child()?;
    let mut roots = Vec::with_capacity(records.len());
    for record in records {
        subscribe(&mut child, &record.facts.git.root.workspace_id)?;
        child.subscribe(&[
            RepositoryLifecycleKey::Database,
            record.facts.root.lifecycle_key(),
        ])?;
        roots.push(record.facts.root.clone());
    }
    RepositoryGitSource::with_group(
        &services.store,
        &services.worktree_locks,
        roots,
        child.retirement(),
        |sources| async move {
            for (record, source) in records.iter().zip(sources) {
                record
                    .facts
                    .validate(request, &source)
                    .await
                    .inspect_err(|error| {
                        if matches!(
                            error,
                            AdmissionError::Denied
                                | AdmissionError::BindingChanged
                                | AdmissionError::Retired
                        ) {
                            child.retirement().retire();
                        }
                    })?;
            }
            let mut operations = records
                .iter()
                .map(|r| r.operation.clone())
                .collect::<Vec<_>>();
            operations.sort_unstable_by_key(Arc::as_ptr);
            operations.dedup_by(|a, b| Arc::ptr_eq(a, b));
            let eligibility = records
                .iter()
                .map(|r| r.eligibility.as_ref())
                .collect::<Vec<_>>();
            child.transfer(|| {
                let states = operations
                    .iter()
                    .map(|op| op.lock().map_err(|_| AdmissionError::Retired))
                    .collect::<AdmissionResult<Vec<_>>>()?;
                if states.iter().any(|state| **state != ReadState::Finished) {
                    return Err(AdmissionError::Retired);
                }
                let mut output = None;
                RepositoryReadEligibility::with_all_current(&eligibility, || {
                    output = Some(transfer());
                    Ok(())
                })
                .map_err(|_| AdmissionError::Unavailable)?;
                output.ok_or(AdmissionError::Retired)
            })
        },
    )
    .await
}

#[cfg(test)]
#[path = "read_source/tests.rs"]
pub(crate) mod tests;
