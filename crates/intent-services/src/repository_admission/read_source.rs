//! Original Agent/MCP reads. Local observations never supply permission; the
//! existing member gate, retained request and actual credential owner all apply.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
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
    read_managed_review, CacheAdmissionGuard, CacheDelivery, CacheFailure, CacheRead, CacheRequest,
    ManagedCacheRequest,
};
use crate::pr_monitor::PrReadPolicy;
use crate::repository_admission::read_request::{RepositoryReadChild, RepositoryReadRequest};
use crate::repository_admission::{AdmissionError, AdmissionResult, RepositoryAgentIdentity};
use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};
use crate::repository_admission_sources::read_agent_identity;
use crate::repository_context_live::SelectionFacts;
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

pub(crate) fn unsupported() -> intent_core::Error {
    intent_core::Error::Unsupported("Addressed review monitoring unavailable".into())
}

pub(crate) fn capture_monitor_owner(
    services: &Services,
) -> AdmissionResult<Arc<RepositoryReadRequest>> {
    let host = crate::repository_read_policy::capture(services)?;
    if !host.request().supports_review_monitoring() {
        return Err(AdmissionError::Unavailable);
    }
    host.request().retain_monitor()
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

pub(crate) async fn context_member(
    services: &Services,
    request: &RepositoryReadRequest,
    workspace: &WorkspaceId,
) -> AdmissionResult<(
    RepositoryWorkspaceAuthoritySnapshot,
    RepositoryAgentIdentity,
)> {
    request.check_optional_current()?;
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
    if before != after || read_agent_identity(services, &caller).await?.as_ref() != Some(&agent) {
        return Err(AdmissionError::BindingChanged);
    }
    request.check_optional_current()?;
    Ok((before, agent))
}

fn subscribe(child: &mut RepositoryReadChild, root: &RepositoryRootId) -> AdmissionResult<()> {
    let Some(Caller::Agent { agent_id }) = current_caller() else {
        return Err(AdmissionError::Denied);
    };
    child.subscribe(&[
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::Workspace(root.workspace_id.clone()),
        RepositoryLifecycleKey::Agent(agent_id),
        RepositoryLifecycleKey::Selection {
            workspace_id: root.workspace_id.clone(),
            git_root_id: match &root.kind {
                RepositoryRootKind::Primary => None,
                RepositoryRootKind::Registered { git_root_id } => Some(git_root_id.clone()),
            },
        },
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

fn target(
    observed: &RepositoryObservedRoot,
    saved: &SavedReviewSelection,
) -> AdmissionResult<intent_core::RepositoryTarget> {
    match resolve_review_selection(saved, &observed.remotes, None).outcome {
        ReviewSelectionOutcome::Resolved { target, .. } => Ok(target),
        _ => Err(AdmissionError::Unavailable),
    }
}

/// Implicit (repo-less, non-addressed) reads only. A workspace inserted with
/// repository metadata has an unresolved historical primary selection
/// (intent-hq/intent#7449), which would otherwise refuse every repo-less read.
/// This is a deliberate services-level exception to the intent-core rule that
/// current remotes never recover historical intent: the selection is treated
/// as Automatic, and the result is accepted only when it is a GitHub target.
/// Anything else (GitLab, ambiguous, unmapped, no remote) keeps the strict
/// refusal, so managed reads, their revalidation and addressed reads, which
/// all use `selected_target`, are unchanged.
fn implicit_target(
    observed: &RepositoryObservedRoot,
    selection: &SelectionFacts,
) -> AdmissionResult<intent_core::RepositoryTarget> {
    let saved = selection.saved()?;
    if matches!(saved, SavedReviewSelection::UnresolvedHistorical { .. }) {
        if let Ok(target) = target(observed, &SavedReviewSelection::Automatic) {
            if target.provider == RepositoryProvider::Github {
                return Ok(target);
            }
        }
    }
    target(observed, &saved)
}

fn selected_target(
    observed: &RepositoryObservedRoot,
    selection: &SelectionFacts,
    explicit: Option<&intent_core::RepositoryTarget>,
    settled: Option<&RepositorySettledConnection>,
) -> AdmissionResult<intent_core::RepositoryTarget> {
    let Some(explicit) = explicit else {
        return target(observed, &selection.saved()?);
    };
    let resolved = resolver(settled)?
        .resolve(&format!(
            "{}/{}",
            explicit.instance_base_url.trim_end_matches('/'),
            explicit.project_path
        ))
        .map_err(|_| AdmissionError::Denied)?;
    let provider = match resolved.provider {
        intent_sourcecontrol::remote_project::RemoteProvider::Github => RepositoryProvider::Github,
        intent_sourcecontrol::remote_project::RemoteProvider::Gitlab => RepositoryProvider::Gitlab,
    };
    if provider != explicit.provider
        || resolved.instance_base_url != explicit.instance_base_url
        || resolved.project_path != explicit.project_path
    {
        return Err(AdmissionError::Denied);
    }
    Ok(explicit.clone())
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
    Unsupported,
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
    root_kind: RepositoryRootKind,
    explicit: Option<intent_core::RepositoryTarget>,
    addressed: bool,
}

impl CapturedReview {
    pub(crate) fn capture(services: &Services, workspace: WorkspaceId, number: u64) -> Self {
        Self::capture_root(services, workspace, number, RepositoryRootKind::Primary)
    }

    fn capture_root(
        services: &Services,
        workspace: WorkspaceId,
        number: u64,
        root_kind: RepositoryRootKind,
    ) -> Self {
        let host = crate::repository_read_policy::capture(services);
        let child = host.as_ref().map_err(|e| *e).and_then(|host| {
            let mut child = host.request().child()?;
            subscribe(
                &mut child,
                &RepositoryRootId {
                    workspace_id: workspace.clone(),
                    kind: root_kind.clone(),
                },
            )?;
            Ok(child)
        });
        Self {
            host,
            child,
            settled: services.gitlab_repository_settled_connection(),
            workspace,
            number,
            root_kind,
            explicit: None,
            addressed: false,
        }
    }

    pub(crate) fn capture_addressed(
        services: &Services,
        workspace: WorkspaceId,
        request: &intent_core::review_monitor::ReviewMonitorRequest,
    ) -> intent_core::Result<Self> {
        use intent_core::review_monitor::ReviewMonitorAddress;
        let address = request
            .address()
            .map_err(|e| intent_core::Error::InvalidParams(e.into()))?;
        let root_kind = match &address {
            ReviewMonitorAddress::SelectedRoot {
                git_root_id: Some(git_root_id),
            } => RepositoryRootKind::Registered {
                git_root_id: git_root_id.clone(),
            },
            _ => RepositoryRootKind::Primary,
        };
        let mut captured = Self::capture_root(services, workspace, request.pr_number, root_kind);
        captured.addressed = true;
        match address {
            ReviewMonitorAddress::SelectedRoot { .. } => {}
            ReviewMonitorAddress::Explicit { target } => {
                captured.explicit = Some(target.repository);
            }
            ReviewMonitorAddress::LegacyGithub { .. } => {
                return Err(intent_core::Error::InvalidParams(
                    "legacy repo uses the legacy observation seam".into(),
                ))
            }
        }
        Ok(captured)
    }

    pub(crate) async fn resolve(mut self, services: &Services) -> AdmissionResult<ResolvedReview> {
        if self.addressed
            && !self
                .host
                .as_ref()
                .is_ok_and(|host| host.request().supports_review_monitoring())
        {
            return Ok(ResolvedReview::Unsupported);
        }
        services
            .require_member(&self.workspace)
            .await
            .map_err(|e| local(&e))?;
        let root_id = RepositoryRootId {
            workspace_id: self.workspace.clone(),
            kind: self.root_kind.clone(),
        };
        let root = RootRecord::read(&services.store, &root_id).await?;
        if let Ok(child) = self.child.as_mut() {
            if let Err(error) =
                child.subscribe(&[RepositoryLifecycleKey::Database, root.lifecycle_key()])
            {
                self.child = Err(error);
            }
        }
        let selection = SelectionFacts::read(services, &root_id).await?;
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
        if SelectionFacts::read(services, &root_id).await? != selection {
            return Err(AdmissionError::BindingChanged);
        }
        // Only `capture_addressed` sets `explicit`, so an implicit read never
        // carries one and `selected_target` would reduce to `target`.
        let selected = if self.addressed {
            selected_target(
                &observed,
                &selection,
                self.explicit.as_ref(),
                self.settled.as_ref().ok(),
            )?
        } else {
            implicit_target(&observed, &selection)?
        };
        if selected.provider == RepositoryProvider::Github {
            let (owner, name) = selected
                .project_path
                .rsplit_once('/')
                .ok_or(AdmissionError::Unavailable)?;
            return Ok(ResolvedReview::Github(RepoRef::new(owner, name)));
        }
        if !self
            .host
            .as_ref()
            .is_ok_and(|host| host.request().supports_review_monitoring())
        {
            return Ok(ResolvedReview::Unsupported);
        }
        let host = self.host?;
        host.check_current()?;
        Ok(ResolvedReview::Managed(Box::new(PreparedReview {
            host,
            workspace: self.workspace,
            target: ReviewTarget {
                repository: selected,
                kind: RepositoryResourceKind::MergeRequest,
                number: self.number,
            },
            child: self.child?,
            settled: self.settled,
            original: (root, GitFacts::from(observed), selection, self.explicit),
        })))
    }

    pub(crate) async fn read(self, services: &Services) -> AdmissionResult<ReadOutcome> {
        match self.resolve(services).await? {
            ResolvedReview::Unsupported => Ok(ReadOutcome::Unsupported),
            ResolvedReview::Github(repo) => Ok(ReadOutcome::Github(repo)),
            ResolvedReview::Managed(prepared) => prepared.read().await,
        }
    }
}

pub(crate) enum ResolvedReview {
    Unsupported,
    Github(RepoRef),
    Managed(Box<PreparedReview>),
}

pub(crate) struct PreparedReview {
    host: ReadHost,
    workspace: WorkspaceId,
    pub(crate) target: ReviewTarget,
    child: RepositoryReadChild,
    settled: credentials::Result<RepositorySettledConnection>,
    original: (
        RootRecord,
        GitFacts,
        SelectionFacts,
        Option<intent_core::RepositoryTarget>,
    ),
}

impl PreparedReview {
    pub(crate) async fn monitor_authority(
        &self,
        retained: Arc<RepositoryReadRequest>,
    ) -> AdmissionResult<Arc<MonitorAuthority>> {
        self.host.check_current()?;
        let original = self
            .settled
            .as_ref()
            .map_err(|_| AdmissionError::Unavailable)?;
        let settled = original
            .reobserve()
            .map_err(|_| AdmissionError::Unavailable)?;
        if settled.selected() != original.selected()
            || settled.descriptor() != original.descriptor()
        {
            return Err(AdmissionError::BindingChanged);
        }
        MonitorAuthority::admit_original(
            self.host.services(),
            retained,
            &self.workspace,
            self.target.clone(),
            settled,
            None,
        )
        .await
    }

    pub(crate) async fn read(self) -> AdmissionResult<ReadOutcome> {
        read_review(
            &self.host,
            self.workspace,
            self.target.number,
            self.child,
            self.settled,
            self.original,
        )
        .await
    }
}

/// Original session/account lifetime retained independently from the MCP reply.
/// Root/selection subscriptions are deliberately short-lived: the resource in
/// this binding never changes after registration. The binding alone is evidence.
pub(crate) struct MonitorAuthority {
    pub(crate) binding: intent_core::review_monitor::ReviewMonitorBinding,
    request: Arc<RepositoryReadRequest>,
    member: MemberFacts,
    settled: RepositorySettledConnection,
    lifetime: RepositoryReadChild,
    eligibility: OnceLock<RepositoryReadEligibility>,
    source: OnceLock<Arc<dyn RepositoryAuthority>>,
    services: std::sync::Weak<Services>,
    observations: crate::observation_adapter::ConnectionObservations,
    restored_from: Option<intent_core::review_monitor::ReviewMonitorBinding>,
    explicit_recovery: std::sync::atomic::AtomicBool,
}

#[path = "read_source/monitor_poll.rs"]
mod monitor_poll;
pub(crate) use monitor_poll::{
    MonitorDeliveries, MonitorDeliveryCompletion, MonitorPromptAdmission,
};

impl MonitorAuthority {
    async fn admit_original(
        services: &Arc<Services>,
        retained: Arc<RepositoryReadRequest>,
        workspace: &WorkspaceId,
        target: ReviewTarget,
        settled: RepositorySettledConnection,
        restored_from: Option<intent_core::review_monitor::ReviewMonitorBinding>,
    ) -> AdmissionResult<Arc<Self>> {
        if !retained.retains(services.as_ref()) || !retained.supports_review_monitoring() {
            return Err(AdmissionError::Denied);
        }
        retained.check_current()?;
        let member = member(services, &retained, workspace).await?;
        let binding = intent_core::review_monitor::ReviewMonitorBinding {
            scope: ExecutionScope {
                daemon_id: services.daemon_boot_id.clone(),
                authority_scope_id: retained.correlation().into(),
                authority_generation: member
                    .workspace
                    .workspace
                    .revision
                    .ok_or(AdmissionError::Unavailable)?
                    .get(),
            },
            connection: settled.selected().binding.scope.clone(),
            resource: target,
        };
        let lifetime = retained.child()?;
        lifetime.subscribe(&[
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::Workspace(workspace.clone()),
            RepositoryLifecycleKey::Agent(member.agent.id.clone()),
        ])?;
        let authority = Arc::new(MonitorAuthority {
            observations: crate::observation_adapter::ConnectionObservations::new(
                binding.scope.clone(),
                binding.connection.clone(),
            ),
            binding,
            request: retained,
            member,
            settled,
            lifetime,
            eligibility: OnceLock::new(),
            source: OnceLock::new(),
            services: Arc::downgrade(services),
            explicit_recovery: std::sync::atomic::AtomicBool::new(restored_from.is_some()),
            restored_from,
        });
        let source: Arc<dyn RepositoryAuthority> =
            Arc::new(MonitorAuthoritySource(Arc::downgrade(&authority)));
        authority
            .source
            .set(source.clone())
            .map_err(|_| AdmissionError::Unavailable)?;
        let admission = services
            .repository_connection_directory()
            .admit(
                &authority.settled.selected().binding,
                authority.credential_request(),
                source,
            )
            .map_err(|_| AdmissionError::Unavailable)?;
        let eligibility = services
            .gitlab_repository_read_eligibility(&admission)
            .map_err(|_| AdmissionError::Unavailable)?;
        authority
            .eligibility
            .set(eligibility)
            .map_err(|_| AdmissionError::Unavailable)?;
        Ok(authority)
    }

    /// A real manager prompt capture, not an ID lookup, supplies this request.
    /// The saved row constrains fresh admission and never supplies a credential.
    pub(crate) async fn recover(
        services: &Arc<Services>,
        retained: Arc<RepositoryReadRequest>,
        row: &intent_store::QualifiedPrMonitor,
        admitted_connection: &intent_core::RepositoryConnectionScope,
    ) -> AdmissionResult<Arc<Self>> {
        let intent_store::PersistedMonitorTarget::Resolved {
            target,
            provenance: intent_store::MonitorTargetProvenance::CapturedRequest,
        } = row.target()
        else {
            return Err(AdmissionError::Denied);
        };
        let envelope: serde_json::Value = serde_json::from_str(
            row.monitor()
                .last_snapshot
                .as_deref()
                .ok_or(AdmissionError::Unavailable)?,
        )
        .map_err(|_| AdmissionError::Unavailable)?;
        let original: intent_core::review_monitor::ReviewMonitorBinding =
            serde_json::from_value(envelope["monitorBinding"].clone())
                .map_err(|_| AdmissionError::Unavailable)?;
        if original.resource != *target
            || original.connection.account_id != admitted_connection.account_id
            || row.monitor().state != intent_core::PrMonitorState::Active
        {
            return Err(AdmissionError::BindingChanged);
        }
        let settled = services
            .gitlab_repository_settled_connection()
            .map_err(|_| AdmissionError::Unavailable)?;
        if &settled.selected().binding.scope != admitted_connection {
            return Err(AdmissionError::BindingChanged);
        }
        let authority = Self::admit_original(
            services,
            retained,
            &row.monitor().workspace_id,
            target.clone(),
            settled,
            Some(original),
        )
        .await?;
        if authority.owner() != &row.monitor().agent_id {
            return Err(AdmissionError::Denied);
        }
        authority.validate(services).await?;
        Ok(authority)
    }

    pub(crate) fn requires_explicit_recovery(&self) -> bool {
        self.explicit_recovery
            .load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) fn matches_saved_binding(
        &self,
        saved: &intent_core::review_monitor::ReviewMonitorBinding,
    ) -> bool {
        (saved.resource == self.binding.resource
            && saved.connection == self.binding.connection
            && saved.scope.daemon_id == self.binding.scope.daemon_id)
            || self.restored_from.as_ref() == Some(saved)
    }
    fn credential_request(&self) -> RepositoryAuthorityRequest {
        RepositoryAuthorityRequest {
            execution: self.binding.scope.clone(),
            target: self.binding.resource.repository.clone(),
            connection: self.binding.connection.clone(),
            use_kind: RepositoryCredentialUse::NativeRead,
            allowed_transport: RepositoryCredentialTransport::GitlabApi(
                self.settled.descriptor().clone(),
            ),
        }
    }

    pub(crate) fn reserve_output(self: &Arc<Self>, services: &Services) -> AdmissionResult<()> {
        let host = crate::repository_read_policy::capture(services)?;
        if !host.request().supports_review_monitoring() {
            return Err(AdmissionError::Denied);
        }
        host.reserve()?
            .bind(Arc::new(MonitorRecord {
                request: host.request().clone(),
                authority: self.clone(),
            }))
            .map_err(|_| AdmissionError::Retired)
    }

    pub(crate) async fn validate(&self, services: &Services) -> AdmissionResult<()> {
        if !self.request.retains(services) || !self.request.supports_review_monitoring() {
            return Err(AdmissionError::Denied);
        }
        self.lifetime.retirement().check_current()?;
        if member(services, &self.request, &self.member.agent.workspace_id).await? != self.member {
            return Err(AdmissionError::BindingChanged);
        }
        let fresh = self
            .settled
            .reobserve()
            .map_err(|_| AdmissionError::Unavailable)?;
        if fresh.selected() != self.settled.selected()
            || fresh.descriptor() != self.settled.descriptor()
        {
            return Err(AdmissionError::BindingChanged);
        }
        self.request.check_current()
    }

    /// Reserve the database writer before entering this boundary. A blocking
    /// worker retains both original physical lifetime and credential/source
    /// fences through the actual SQL statement and COMMIT. Cancellation of the
    /// awaiting caller drops only its `JoinHandle`, not an admitted SQL worker.
    pub(crate) async fn commit(
        self: &Arc<Self>,
        services: &Services,
        write: intent_store::PreparedMonitorWrite,
    ) -> intent_core::Result<bool> {
        self.validate(services).await.map_err(|_| refused())?;
        let authority = self.clone();
        tokio::task::spawn_blocking(move || {
            // Context/caller checks ran in the original async call. This worker
            // can use only its retained original leaf, never ambient authority.
            authority
                .lifetime
                .commit_monitor_write(authority.eligibility.get().ok_or_else(refused)?, write)
                .map_err(|_| refused())?
        })
        .await
        .map_err(|_| refused())?
    }

    pub(crate) fn owner(&self) -> &intent_core::AgentId {
        &self.member.agent.id
    }
}

struct MonitorFence(Arc<MonitorAuthority>);
impl RepositoryAuthorityFence for MonitorFence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> credentials::Result<()> + Send),
    ) -> credentials::Result<()> {
        self.0
            .lifetime
            .transfer(|| Ok(action()))
            .map_err(credential)?
    }
}
struct MonitorAuthoritySource(std::sync::Weak<MonitorAuthority>);
impl RepositoryAuthority for MonitorAuthoritySource {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            let authority = self.0.upgrade().ok_or(RepositoryCredentialError::Retired)?;
            if request != &authority.credential_request() {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            let services = authority
                .services
                .upgrade()
                .ok_or(RepositoryCredentialError::Retired)?;
            authority.validate(&services).await.map_err(credential)?;
            Ok(Box::new(MonitorFence(authority)) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

pub(crate) struct MonitorRecord {
    pub(crate) request: Arc<RepositoryReadRequest>,
    pub(crate) authority: Arc<MonitorAuthority>,
}

/// A persistent obligation, never a reusable acquisition fence.
pub(crate) struct ReadRecord {
    pub(crate) request: Arc<RepositoryReadRequest>,
    facts: Arc<ReadFacts>,
    pub(crate) operation: Arc<Mutex<ReadState>>,
    pub(crate) eligibility: Arc<RepositoryReadEligibility>,
    delivery: Arc<OnceLock<CacheDelivery>>,
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
    selection: SelectionFacts,
    explicit: Option<intent_core::RepositoryTarget>,
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
            || SelectionFacts::read(&self.services, &self.git.root).await? != self.selection
            || selected_target(
                &observed,
                &self.selection,
                self.explicit.as_ref(),
                Some(&self.settled),
            )? != self.request.target
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
            delivery: self.delivery.clone(),
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
    child: RepositoryReadChild,
    settled: credentials::Result<RepositorySettledConnection>,
    original: (
        RootRecord,
        GitFacts,
        SelectionFacts,
        Option<intent_core::RepositoryTarget>,
    ),
) -> AdmissionResult<ReadOutcome> {
    let (original_root, original_git, selection, explicit) = original;
    host.check_current()?;
    if number == 0 {
        return Err(AdmissionError::Denied);
    }
    let member_facts = member(host.services(), host.request(), &workspace).await?;
    let root_id = original_git.root.clone();
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
            if SelectionFacts::read(host.services(), &root_id).await? != selection {
                return Err(AdmissionError::BindingChanged);
            }
            let selected_target = selected_target(
                &observed,
                &selection,
                explicit.as_ref(),
                settled.as_ref().ok(),
            )?;
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
                selection,
                explicit,
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
                delivery: Arc::new(OnceLock::new()),
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
            if let Ok(read) = &result {
                record
                    .delivery
                    .set(read.delivery.clone())
                    .map_err(|_| AdmissionError::Retired)?;
            }
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

fn with_deliveries<T: Send>(
    records: &[&ReadRecord],
    transfer: impl FnOnce() -> AdmissionResult<T> + Send,
) -> AdmissionResult<T> {
    let mut deliveries = Vec::new();
    for record in records {
        // Finish is published only after the successful receipt is installed.
        // An acquiring/abandoned record cannot be mistaken for a completed Err
        // (which has no private payload and therefore no delivery receipt).
        if *record
            .operation
            .lock()
            .map_err(|_| AdmissionError::Retired)?
            != ReadState::Finished
        {
            return Err(AdmissionError::Retired);
        }
        deliveries.extend(record.delivery.get());
    }
    let mut output = None;
    let mut transfer = Some(transfer);
    CacheDelivery::with_all_current(&deliveries, &mut || {
        let transfer = transfer.take().ok_or(RepositoryCredentialError::Retired)?;
        output = Some(transfer());
        Ok(())
    })
    .map_err(|_| AdmissionError::Retired)?;
    output.ok_or(AdmissionError::Retired)?
}

/// Fresh simultaneous validation for one original boundary. All source locks
/// remain live until the synchronous callback finishes; no checked fence escapes.
pub(crate) async fn with_records<T: Send>(
    request: &Arc<RepositoryReadRequest>,
    services: &Arc<Services>,
    records: &[&ReadRecord],
    transfer: impl FnOnce() -> T + Send,
) -> AdmissionResult<T> {
    with_records_and_monitors(request, services, records, &[], transfer).await
}

pub(crate) async fn with_records_and_monitors<T: Send>(
    request: &Arc<RepositoryReadRequest>,
    services: &Arc<Services>,
    records: &[&ReadRecord],
    monitors: &[&MonitorRecord],
    transfer: impl FnOnce() -> T + Send,
) -> AdmissionResult<T> {
    request.check_current()?;
    let current = crate::repository_admission::request_context::current_read_request()?;
    if !Arc::ptr_eq(request, &current) {
        return Err(AdmissionError::Denied);
    }
    if (records.is_empty() && monitors.is_empty())
        || records
            .iter()
            .any(|r| !Arc::ptr_eq(&r.request, request) || !Arc::ptr_eq(&r.facts.services, services))
    {
        return Err(AdmissionError::Denied);
    }
    for record in monitors {
        if !Arc::ptr_eq(&record.request, request) || !request.supports_review_monitoring() {
            return Err(AdmissionError::Denied);
        }
        record.authority.validate(services).await?;
    }
    let mut child = request.child()?;
    let mut roots = Vec::with_capacity(records.len());
    for record in records {
        subscribe(&mut child, &record.facts.git.root)?;
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
            let mut eligibility = records
                .iter()
                .map(|r| r.eligibility.as_ref())
                .collect::<Vec<_>>();
            for record in monitors {
                eligibility.push(
                    record
                        .authority
                        .eligibility
                        .get()
                        .ok_or(AdmissionError::Unavailable)?,
                );
            }
            let mut authorities = monitors.iter().map(|r| &r.authority).collect::<Vec<_>>();
            authorities.sort_unstable_by_key(|a| Arc::as_ptr(a));
            authorities.dedup_by(|a, b| Arc::ptr_eq(a, b));
            with_monitor_fences(&authorities, || {
                with_deliveries(records, || {
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
                })
            })
        },
    )
    .await
}

fn with_monitor_fences<T>(
    authorities: &[&Arc<MonitorAuthority>],
    action: impl FnOnce() -> AdmissionResult<T>,
) -> AdmissionResult<T> {
    if let Some((first, rest)) = authorities.split_first() {
        first
            .lifetime
            .transfer(|| with_monitor_fences(rest, action))
    } else {
        action()
    }
}

/// INACTIVE: checks optional LOCAL lifetime only, never optional Git/settings/P
/// facts. Do not expose context from this primitive before those producers exist.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "joint optional admission is deliberately not activated"
    )
)]
pub(crate) async fn with_joint_local_records<T: Send>(
    request: &Arc<RepositoryReadRequest>,
    services: &Arc<Services>,
    records: &[&ReadRecord],
    optional: Option<&crate::repository_admission::read_request::RepositoryOptionalMetadata>,
    transfer: impl FnOnce(bool) -> T + Send,
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
        subscribe(&mut child, &record.facts.git.root)?;
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
            with_deliveries(records, || {
                child.transfer_with_optional(optional, |include| {
                    let states = operations
                        .iter()
                        .map(|op| op.lock().map_err(|_| AdmissionError::Retired))
                        .collect::<AdmissionResult<Vec<_>>>()?;
                    if states.iter().any(|state| **state != ReadState::Finished) {
                        return Err(AdmissionError::Retired);
                    }
                    let mut output = None;
                    RepositoryReadEligibility::with_all_current(&eligibility, || {
                        output = Some(transfer(include));
                        Ok(())
                    })
                    .map_err(|_| AdmissionError::Unavailable)?;
                    output.ok_or(AdmissionError::Retired)
                })
            })
        },
    )
    .await
}

/// Complete original optional evidence, retained separately from all packet
/// bytes. The metadata and facts must belong to the same physical request.
pub(crate) struct ContextOptional {
    pub(crate) prepared:
        Arc<crate::repository_admission::read_request::PreparedRepositoryOptional<()>>,
    pub(crate) facts: Arc<crate::repository_context_live::PreparedContextFacts>,
}
impl ContextOptional {
    fn belongs_to(&self, request: &Arc<RepositoryReadRequest>, services: &Arc<Services>) -> bool {
        Arc::ptr_eq(self.prepared.metadata().request(), request)
            && Arc::ptr_eq(&self.facts.request, request)
            && Arc::ptr_eq(&self.facts.owner.services, services)
    }
}

/// This token closes the worker's actual lock interval. Its drop signals the
/// worker; blocking jobs are still owned and counted until their real exit.
struct ContextLocks(
    #[expect(dead_code, reason = "sender drop releases the tracked source worker")]
    tokio::sync::oneshot::Sender<()>,
);
struct RequiredObservation {
    facts: Arc<ReadFacts>,
    operation: Arc<Mutex<ReadState>>,
    eligibility: Arc<RepositoryReadEligibility>,
}

pub(crate) async fn with_context_records<T: Send>(
    request: &Arc<RepositoryReadRequest>,
    services: &Arc<Services>,
    records: &[&ReadRecord],
    optional: &ContextOptional,
    transfer: impl FnOnce(bool) -> T + Send,
) -> AdmissionResult<T> {
    request.check_current()?;
    let current = crate::repository_admission::request_context::current_read_request()?;
    if !Arc::ptr_eq(request, &current)
        || records.is_empty()
        || records
            .iter()
            .any(|r| !Arc::ptr_eq(&r.request, request) || !Arc::ptr_eq(&r.facts.services, services))
    {
        return Err(AdmissionError::Denied);
    }
    if !optional.belongs_to(request, services) {
        return with_records(request, services, records, || transfer(false)).await;
    }
    let mut child = request.child()?;
    let mut roots = Vec::new();
    for record in records {
        subscribe(&mut child, &record.facts.git.root)?;
        child.subscribe(&[
            RepositoryLifecycleKey::Database,
            record.facts.root.lifecycle_key(),
        ])?;
        roots.push(record.facts.root.clone());
    }
    let child = Arc::new(child);
    let originals = records
        .iter()
        .map(|r| RequiredObservation {
            facts: r.facts.clone(),
            operation: r.operation.clone(),
            eligibility: r.eligibility.clone(),
        })
        .collect::<Vec<_>>();
    let mut operations = originals
        .iter()
        .map(|r| r.operation.clone())
        .collect::<Vec<_>>();
    operations.sort_unstable_by_key(Arc::as_ptr);
    operations.dedup_by(|a, b| Arc::ptr_eq(a, b));
    let eligibility = originals
        .iter()
        .map(|r| r.eligibility.clone())
        .collect::<Vec<_>>();
    let facts = optional.facts.clone();
    let worker_facts = facts.clone();
    let worker_services = services.clone();
    let worker_request = request.clone();
    let worker_child = child.clone();
    let caller = current_caller().ok_or(AdmissionError::Denied)?;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let _locks = ContextLocks(release_tx);
    let worker = facts.owner.start_job(intent_core::caller::with_caller(
        caller,
        intent_core::caller::with_wire_credential(None, async move {
            RepositoryGitSource::with_mixed_group(
                &worker_services.store,
                &worker_services.worktree_locks,
                roots,
                worker_facts.roots(),
                worker_child.retirement(),
                |sources, optional_sources| async move {
                    for (record, source) in originals.iter().zip(&sources) {
                        record.facts.validate(&worker_request, source).await?;
                    }
                    let include = match optional_sources {
                        Some(sources) => worker_facts.revalidate(&sources).await.is_ok(),
                        None => false,
                    };
                    if ready_tx.send(include).is_ok() {
                        let _ = release_rx.await;
                    }
                    Ok(())
                },
            )
            .await
        }),
    ));
    let Ok(include) = ready_rx.await else {
        return Err(worker
            .await
            .ok()
            .and_then(Result::err)
            .unwrap_or(AdmissionError::Unavailable));
    };
    let eligibility = eligibility.iter().map(Arc::as_ref).collect::<Vec<_>>();
    with_deliveries(records, || {
        child.transfer_with_optional(Some(optional.prepared.metadata()), |local_current| {
        facts.with_revision(|revision_current| {
            let states = operations.iter().map(|op| op.lock().map_err(|_| AdmissionError::Retired)).collect::<AdmissionResult<Vec<_>>>()?;
            if states.iter().any(|state| **state != ReadState::Finished) { return Err(AdmissionError::Retired); }
            let mut output = None;
            let mut transfer = Some(transfer);
            let mut batch = |provider: Option<&crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts>| {
                crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility::with_all_current_and_facts(
                    &eligibility, provider, |include| {
                        output = Some(transfer.take().expect("one context transfer")(include));
                        Ok(())
                    })
            };
            if include && local_current && revision_current { facts.with_settings(&mut batch) } else { batch(None) }
                .map_err(|_| AdmissionError::Unavailable)?;
            output.ok_or(AdmissionError::Retired)
        })
    })
    })
}

/// Only ACP can construct `McpSealedReads`. The prompt arm owns a distinct real
/// request, so neither an empty vector nor ordinary output chooses this branch.
pub(crate) enum OptionalContextOrigin<'a> {
    Mcp(intent_acp::mcp_server::private_results::McpSealedReads<'a>),
    Prompt(&'a crate::repository_admission::request_context::RepositoryPromptRequest),
}

pub(crate) async fn with_context_only<T: Send>(
    origin: OptionalContextOrigin<'_>,
    optional: &ContextOptional,
    transfer: impl FnOnce(bool) -> T + Send,
) -> AdmissionResult<T> {
    let facts = &optional.facts;
    let prompt = match origin {
        OptionalContextOrigin::Mcp(sealed) if sealed.is_empty() => false,
        OptionalContextOrigin::Prompt(owner) if Arc::ptr_eq(owner.read(), &facts.request) => true,
        _ => return Err(AdmissionError::Denied),
    };
    if !optional.belongs_to(&facts.request, &facts.owner.services) {
        return Ok(transfer(false));
    }
    if !optional.prepared.metadata().transfer_optional(|live| live) {
        return Ok(transfer(false));
    }
    let caller = current_caller().ok_or(AdmissionError::Denied)?;
    let worker_facts = facts.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let _locks = ContextLocks(release_tx);
    let worker = facts.owner.start_job(intent_core::caller::with_caller(
        caller,
        intent_core::caller::with_wire_credential(None, async move {
            let services = worker_facts.owner.services.clone();
            RepositoryGitSource::with_mixed_group(
                &services.store,
                &services.worktree_locks,
                Vec::new(),
                worker_facts.roots(),
                crate::repository_admission::RepositoryRetirement::default(),
                |_, sources| async move {
                    let include = match sources {
                        Some(sources) => worker_facts.revalidate(&sources).await.is_ok(),
                        None => false,
                    };
                    if ready_tx.send(include).is_ok() {
                        let _ = release_rx.await;
                    }
                    Ok(())
                },
            )
            .await
        }),
    ));
    let include = ready_rx.await.unwrap_or(false);
    // The tracked worker owns real locks until release/exit, even after cancellation.
    drop(worker);
    optional.prepared.metadata().transfer_optional(|local_current| {
        let mut output = None;
        let mut transfer = Some(transfer);
        let mut batch = |provider: Option<&crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts>| {
            let consume = |include| { output = Some(transfer.take().expect("one optional transfer")(include)); Ok(()) };
            if prompt {
                crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts::with_prompt_current(provider, consume)
            } else {
                crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts::with_optional_current(provider, consume)
            }
        };
        if include && local_current { facts.with_current(&mut batch) } else { batch(None) }.map_err(|_| AdmissionError::Unavailable)?;
        output.ok_or(AdmissionError::Unavailable)
    })
}

#[cfg(test)]
#[path = "read_source/tests.rs"]
pub(crate) mod tests;

#[cfg(test)]
#[path = "read_source/monitor_native_tests.rs"]
mod monitor_native_tests;

#[cfg(test)]
#[path = "read_source/native_runtime_tests.rs"]
mod native_runtime_tests;
