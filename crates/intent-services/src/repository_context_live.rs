//! Original, token-free repository observations. This private owner is bound
//! once to a confirmed physical allocation; display never supplies permission.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use intent_core::caller::{
    current_caller, current_wire_credential, with_caller, with_wire_credential, Caller,
};
use intent_core::{
    ExecutionScope, RepositoryAvailability, RepositoryCapability, RepositoryCapabilityState,
    RepositoryContext, RepositoryContextRevision, RepositoryOperation, RepositoryProvider,
    RepositoryRootContext, RepositoryRootId, RepositoryRootKind, RepositoryTarget,
    RepositoryTargetContext, SavedReviewSelection, WorkspaceId,
};
use intent_sourcecontrol::remote_project::{CanonicalRemoteResolver, RemoteInstance};
use intent_sourcecontrol::GitlabDescriptor;
use intent_store::{
    RepositoryLifecycleKey, RepositorySelectionSnapshot, RepositoryStoredSelection,
    RepositoryWorkspaceAuthoritySnapshot,
};
use tokio::sync::{oneshot, Notify};

use crate::repository_admission::lifecycle::physical_owner::RepositoryPhysicalOwner;
use crate::repository_admission::read_request::{
    RepositoryOptionalMetadata, RepositoryReadOwner, RepositoryReadRequest,
};
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::repository_admission::{
    AdmissionError, AdmissionResult, RepositoryAgentIdentity, RepositoryRetirement,
};
use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};
use crate::repository_context_reader::{
    read_context_root_with_resolver, GitConfigEnvironment, RepositoryObservedRoot,
};
use crate::repository_credentials::{
    RepositoryConnectionState, RepositoryCredentialError, RepositoryMutationKind,
    RepositorySecretRequest,
};
use crate::settings_registry::SettingsSnapshot;
use crate::source_control_auth_ops::repository_owner::{
    RepositoryAttachmentState, RepositoryChildPolicyState, RepositoryConnectionFacts,
    RepositoryDescriptorState,
};
use crate::{Services, SettingsRegistry};

/// The snapshot owns its actual Store domain. Public counters are compared only
/// after the original Services/Store/request allocation has been checked.
pub(crate) struct SelectionFacts(Arc<RepositorySelectionSnapshot>);
impl SelectionFacts {
    pub(crate) async fn read(
        services: &Services,
        root: &RepositoryRootId,
    ) -> AdmissionResult<Self> {
        let value = services
            .store
            .repository_selection_snapshot(root)
            .await
            .map_err(local)?;
        if value.binding().is_none()
            || value.root_incarnation().is_none()
            || value.selection_revision().is_none()
        {
            return Err(AdmissionError::Unavailable);
        }
        Ok(Self(Arc::new(value)))
    }
    pub(crate) fn saved(&self) -> AdmissionResult<SavedReviewSelection> {
        match self.0.selection() {
            Some(RepositoryStoredSelection::NeverSaved | RepositoryStoredSelection::Reset) => {
                Ok(SavedReviewSelection::Automatic)
            }
            Some(RepositoryStoredSelection::Saved(choice)) => Ok(choice.clone()),
            None => Err(AdmissionError::Unavailable),
        }
    }
    pub(crate) fn key(root: &RepositoryRootId) -> RepositoryLifecycleKey {
        RepositoryLifecycleKey::Selection {
            workspace_id: root.workspace_id.clone(),
            git_root_id: match &root.kind {
                RepositoryRootKind::Primary => None,
                RepositoryRootKind::Registered { git_root_id } => Some(git_root_id.clone()),
            },
        }
    }
}
impl Clone for SelectionFacts {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl PartialEq for SelectionFacts {
    fn eq(&self, other: &Self) -> bool {
        self.0.root() == other.0.root()
            && self.0.binding() == other.0.binding()
            && self.0.root_incarnation() == other.0.root_incarnation()
            && self.0.selection_revision() == other.0.selection_revision()
            && self.0.selection() == other.0.selection()
    }
}
impl Eq for SelectionFacts {}

pub(crate) fn local(_: intent_core::Error) -> AdmissionError {
    AdmissionError::Unavailable
}

#[derive(Clone, PartialEq, Eq)]
struct ProviderStamp {
    attachment: RepositoryAttachmentState,
    approval: RepositoryDescriptorState,
    descriptor: Option<GitlabDescriptor>,
    lifecycle: RepositoryConnectionState,
    mutation: Option<RepositoryMutationKind>,
    preflight: bool,
    selected: Option<RepositorySecretRequest>,
    unavailable: Option<RepositoryCredentialError>,
    deadline: Option<Instant>,
    child: Option<(RepositoryChildPolicyState, u64)>,
}
impl ProviderStamp {
    fn from(facts: &RepositoryConnectionFacts) -> Self {
        Self {
            attachment: facts.attachment(),
            approval: facts.approval(),
            descriptor: facts.descriptor().cloned(),
            lifecycle: facts.lifecycle(),
            mutation: facts.mutation(),
            preflight: facts.preflight_pending(),
            selected: facts.settled().map(|s| s.selected().clone()),
            unavailable: facts.unavailable_reason(),
            deadline: facts.backoff_until(),
            child: facts.child_policy(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ContextMember {
    workspace: RepositoryWorkspaceAuthoritySnapshot,
    agent: RepositoryAgentIdentity,
}

#[derive(Clone, PartialEq, Eq)]
struct RootFacts {
    record: RootRecord,
    selection: SelectionFacts,
    context: RepositoryRootContext,
    observed: RepositoryObservedRoot,
}

#[derive(PartialEq)]
struct Observation {
    member: ContextMember,
    roots: Vec<RootFacts>,
    settings: Vec<(
        String,
        Option<serde_json::Value>,
        Option<crate::SettingOrigin>,
    )>,
    provider: Option<ProviderStamp>,
}
struct RevisionState {
    attempt: u64,
    sequence: u64,
    published: Option<Arc<Observation>>,
    unavailable: bool,
}
struct PreparationAttempt {
    owner: Arc<RepositoryContextOwner>,
    attempt: u64,
    completed: bool,
}
impl Drop for PreparationAttempt {
    fn drop(&mut self) {
        if !self.completed {
            if let Ok(mut state) = self.owner.revision.lock() {
                if state.attempt == self.attempt {
                    state.published = None;
                }
            }
        }
    }
}
#[derive(Default)]
struct Jobs {
    active: AtomicUsize,
    done: Notify,
}
struct RunningJob(Arc<Jobs>);
impl Drop for RunningJob {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.done.notify_waiters();
    }
}

/// No strong physical owner is retained. The callback contains the original
/// weak physical origin and the once-captured read-anchor success or failure.
pub(crate) struct RepositoryContextOwner {
    pub(crate) services: Arc<Services>,
    pub(crate) callback: Arc<RepositoryCallbackContext>,
    scope: String,
    epoch: String,
    lane: Arc<tokio::sync::Mutex<()>>,
    revision: Mutex<RevisionState>,
    jobs: Arc<Jobs>,
    #[cfg(test)]
    git_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl RepositoryContextOwner {
    pub(crate) fn bind(
        services: Arc<Services>,
        original: AdmissionResult<Arc<RepositoryReadOwner>>,
        physical: &RepositoryPhysicalOwner,
    ) -> AdmissionResult<Arc<Self>> {
        let retained = original.as_ref().map_err(|e| *e)?;
        if !retained.retains(services.as_ref()) {
            return Err(AdmissionError::Denied);
        }
        let callback = Arc::new(physical.callback().with_read_owner(original));
        // Validate the original Store/observer pairing before exposing the owner.
        // This temporary request is retired normally; no metadata prolongs it.
        let (scope, read) = callback.capture_owned();
        let read = read?;
        if !read.retains(services.as_ref()) {
            return Err(AdmissionError::Denied);
        }
        drop(scope);
        Ok(Arc::new(Self {
            services,
            callback,
            scope: uuid::Uuid::new_v4().to_string(),
            epoch: uuid::Uuid::new_v4().to_string(),
            lane: Arc::new(tokio::sync::Mutex::new(())),
            revision: Mutex::new(RevisionState {
                attempt: 0,
                sequence: 0,
                published: None,
                unavailable: false,
            }),
            jobs: Arc::new(Jobs::default()),
            #[cfg(test)]
            git_probe: Mutex::new(None),
        }))
    }

    /// Own the actual asynchronous worker and every nested blocking Git job.
    /// Dropping the waiter closes publication, but the worker keeps its locks
    /// until it exits. `drain_jobs` observes that real completion.
    pub(crate) fn start_job<T, F>(
        self: &Arc<Self>,
        future: F,
    ) -> oneshot::Receiver<AdmissionResult<T>>
    where
        T: Send + 'static,
        F: std::future::Future<Output = AdmissionResult<T>> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.jobs.active.fetch_add(1, Ordering::AcqRel);
        let running = RunningJob(self.jobs.clone());
        tokio::spawn(async move {
            let _running = running;
            let result = future.await;
            let _ = tx.send(result);
        });
        rx
    }

    pub(crate) async fn drain_jobs(&self) {
        loop {
            let notified = self.jobs.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.jobs.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// An explicit resume/invalidation on THIS allocation invalidates old
    /// observations. No current `AgentId` lookup or new epoch repairs them.
    pub(crate) fn invalidate(&self) {
        if let Ok(mut state) = self.revision.lock() {
            state.published = None;
            if let Some(next) = state.attempt.checked_add(1) {
                state.attempt = next;
            } else {
                state.unavailable = true;
            }
        }
    }

    pub(crate) async fn prepare(
        self: &Arc<Self>,
        metadata: RepositoryOptionalMetadata,
    ) -> AdmissionResult<Arc<PreparedContextFacts>> {
        metadata.check_current()?;
        if !metadata.request().retains(self.services.as_ref()) {
            return Err(AdmissionError::Denied);
        }
        let caller = current_caller().ok_or(AdmissionError::Denied)?;
        if current_wire_credential().is_some() {
            return Err(AdmissionError::Denied);
        }
        let attempt = {
            let mut state = self.revision.lock().map_err(|_| AdmissionError::Retired)?;
            if state.unavailable {
                return Err(AdmissionError::Unavailable);
            }
            let Some(next) = state.attempt.checked_add(1) else {
                state.unavailable = true;
                return Err(AdmissionError::Unavailable);
            };
            state.attempt = next;
            next
        };
        let mut publication = PreparationAttempt {
            owner: self.clone(),
            attempt,
            completed: false,
        };
        let _lane = self.lane.lock().await;
        let owner = self.clone();
        let local = metadata.clone();
        let request = metadata.request().clone();
        let receiver = self.start_job(with_caller(
            caller,
            with_wire_credential(None, async move { owner.observe(local).await }),
        ));
        let raw = receiver.await.map_err(|_| AdmissionError::Unavailable)??;
        metadata.check_current()?;
        let generation = raw
            .observation
            .member
            .workspace
            .workspace
            .revision
            .ok_or(AdmissionError::Unavailable)?
            .get();
        let scope = ExecutionScope {
            daemon_id: self.services.daemon_boot_id.clone(),
            authority_scope_id: self.scope.clone(),
            authority_generation: generation,
        };
        let mut state = self.revision.lock().map_err(|_| AdmissionError::Retired)?;
        if state.unavailable || state.attempt != attempt {
            return Err(AdmissionError::BindingChanged);
        }
        let observed = if let Some(old) = state
            .published
            .as_ref()
            .filter(|old| old.as_ref() == &raw.observation)
        {
            old.clone()
        } else {
            let Some(next) = state.sequence.checked_add(1).filter(|next| *next > 0) else {
                state.unavailable = true;
                state.published = None;
                return Err(AdmissionError::Unavailable);
            };
            state.sequence = next;
            let value = Arc::new(raw.observation);
            state.published = Some(value.clone());
            value
        };
        let revision = RepositoryContextRevision::new(&self.epoch, state.sequence);
        publication.completed = true;
        Ok(Arc::new(PreparedContextFacts {
            owner: self.clone(),
            request,
            context: RepositoryContext {
                revision,
                scope,
                roots: observed
                    .roots
                    .iter()
                    .map(|root| root.context.clone())
                    .collect(),
            },
            observed,
            settings: raw.settings,
            registry: raw.registry,
            provider: raw.provider,
        }))
    }

    async fn observe(
        self: &Arc<Self>,
        metadata: RepositoryOptionalMetadata,
    ) -> AdmissionResult<RawObservation> {
        let services = &self.services;
        let Some(Caller::Agent { agent_id }) = current_caller() else {
            return Err(AdmissionError::Denied);
        };
        let session = services
            .store
            .get_agent_session(&agent_id)
            .await
            .map_err(local)?;
        let workspace = session.workspace_id;
        metadata.subscribe_metadata(&[
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::Workspace(workspace.clone()),
            RepositoryLifecycleKey::Agent(agent_id),
            RepositoryLifecycleKey::RootInventory(workspace.clone()),
        ])?;
        let member = context_member(services, metadata.request(), &workspace).await?;
        let mut roots = vec![RepositoryRootId {
            workspace_id: workspace.clone(),
            kind: RepositoryRootKind::Primary,
        }];
        for root in services
            .store
            .list_workspace_git_roots(&workspace)
            .await
            .map_err(local)?
        {
            roots.push(RepositoryRootId {
                workspace_id: workspace.clone(),
                kind: RepositoryRootKind::Registered {
                    git_root_id: root.id,
                },
            });
        }
        roots[1..].sort_by(|a, b| match (&a.kind, &b.kind) {
            (
                RepositoryRootKind::Registered { git_root_id: a },
                RepositoryRootKind::Registered { git_root_id: b },
            ) => a.as_str().cmp(b.as_str()),
            _ => std::cmp::Ordering::Equal,
        });
        let mut records = Vec::new();
        let mut selections = Vec::new();
        for root in &roots {
            let mut keys = vec![RepositoryLifecycleKey::Database, SelectionFacts::key(root)];
            if let RepositoryRootKind::Registered { git_root_id } = &root.kind {
                keys.push(RepositoryLifecycleKey::GitRoot(git_root_id.clone()));
            }
            metadata.subscribe_metadata(&keys)?;
            let record = RootRecord::read(&services.store, root).await?;
            metadata
                .subscribe_metadata(&[RepositoryLifecycleKey::Database, record.lifecycle_key()])?;
            selections.push(SelectionFacts::read(services, root).await?);
            records.push(record);
        }
        let registry = services
            .settings_registry
            .clone()
            .ok_or(AdmissionError::Unavailable)?;
        let owner_services = services.clone();
        let registry_copy = registry.clone();
        let (settings, provider) = tokio::task::spawn_blocking(move || {
            (
                registry_copy.snapshot(),
                owner_services
                    .gitlab_repository_connection_facts()
                    .ok()
                    .map(Arc::new),
            )
        })
        .await
        .map_err(|_| AdmissionError::Unavailable)?;
        let resolver = resolver(provider.as_deref())?;
        let optional = records.clone();
        RepositoryGitSource::with_mixed_group(
            &services.store,
            &services.worktree_locks,
            Vec::new(),
            optional,
            RepositoryRetirement::default(),
            |_, sources| async move {
                let sources = sources.ok_or(AdmissionError::Unavailable)?;
                let mut root_facts = Vec::new();
                for ((record, selection), source) in
                    records.into_iter().zip(selections).zip(sources)
                {
                    metadata.check_current()?;
                    source.check_root().await?;
                    let saved = selection.saved()?;
                    let id = record.root().clone();
                    let path = record.path().to_owned();
                    let resolve = resolver.clone();
                    let facts = provider.clone();
                    #[cfg(test)]
                    let probe = self.git_probe.lock().unwrap().take();
                    let (context, observed) = tokio::task::spawn_blocking(move || {
                        #[cfg(test)]
                        if let Some(probe) = probe {
                            probe();
                        }
                        read_context_root_with_resolver(
                            &id,
                            &path,
                            &saved,
                            None,
                            &resolve,
                            &GitConfigEnvironment::default(),
                            |target| target_context(target, facts.as_deref()),
                        )
                    })
                    .await
                    .map_err(|_| AdmissionError::Unavailable)?
                    .map_err(local)?;
                    source.check_root().await?;
                    if SelectionFacts::read(services, record.root()).await? != selection {
                        return Err(AdmissionError::BindingChanged);
                    }
                    root_facts.push(RootFacts {
                        record,
                        selection,
                        context,
                        observed,
                    });
                }
                if context_member(services, metadata.request(), &workspace).await? != member {
                    return Err(AdmissionError::BindingChanged);
                }
                metadata.check_current()?;
                let settings_facts = crate::KNOWN_PATHS
                    .iter()
                    .map(|key| ((*key).to_owned(), settings.get(key), settings.origin(key)))
                    .collect();
                Ok(RawObservation {
                    observation: Observation {
                        member,
                        roots: root_facts,
                        settings: settings_facts,
                        provider: provider.as_deref().map(ProviderStamp::from),
                    },
                    settings,
                    registry,
                    provider,
                })
            },
        )
        .await
    }
}

struct RawObservation {
    observation: Observation,
    settings: Arc<SettingsSnapshot>,
    registry: Arc<SettingsRegistry>,
    provider: Option<Arc<RepositoryConnectionFacts>>,
}

/// Private source and allocation evidence; no fence is stored in the DTO.
pub(crate) struct PreparedContextFacts {
    pub(crate) owner: Arc<RepositoryContextOwner>,
    pub(crate) request: Arc<RepositoryReadRequest>,
    pub(crate) context: RepositoryContext,
    observed: Arc<Observation>,
    settings: Arc<SettingsSnapshot>,
    registry: Arc<SettingsRegistry>,
    provider: Option<Arc<RepositoryConnectionFacts>>,
}
impl PreparedContextFacts {
    pub(crate) fn roots(&self) -> Vec<RootRecord> {
        self.observed
            .roots
            .iter()
            .map(|root| root.record.clone())
            .collect()
    }

    pub(crate) async fn revalidate(
        &self,
        sources: &[Arc<RepositoryGitSource>],
    ) -> AdmissionResult<()> {
        if sources.len() != self.observed.roots.len() {
            return Err(AdmissionError::BindingChanged);
        }
        let workspace = &self.observed.member.workspace.workspace_id;
        if context_member(&self.owner.services, &self.request, workspace).await?
            != self.observed.member
        {
            return Err(AdmissionError::BindingChanged);
        }
        for (root, source) in self.observed.roots.iter().zip(sources) {
            if SelectionFacts::read(&self.owner.services, root.record.root()).await?
                != root.selection
            {
                return Err(AdmissionError::BindingChanged);
            }
            if source
                .observe_root(resolver(self.provider.as_deref())?)
                .await?
                != root.observed
            {
                return Err(AdmissionError::BindingChanged);
            }
        }
        Ok(())
    }

    pub(crate) fn with_revision<T>(&self, action: impl FnOnce(bool) -> T) -> T {
        let state = self.owner.revision.try_lock();
        let current = state.as_ref().is_ok_and(|state| {
            !state.unavailable
                && state
                    .published
                    .as_ref()
                    .is_some_and(|value| Arc::ptr_eq(value, &self.observed))
        });
        action(current)
    }
    pub(crate) fn with_settings<T>(
        &self,
        action: impl FnOnce(Option<&RepositoryConnectionFacts>) -> T,
    ) -> T {
        self.registry
            .with_original_snapshot(&self.settings, |current| {
                action(if current {
                    self.provider.as_deref()
                } else {
                    None
                })
            })
    }

    /// Called only inside the original parent/local fences. No authority I/O,
    /// async gate, recursive getter or serialization occurs in this closure.
    pub(crate) fn with_current<T>(
        &self,
        action: impl FnOnce(Option<&RepositoryConnectionFacts>) -> T,
    ) -> T {
        let state = self.owner.revision.try_lock();
        let current = state.as_ref().is_ok_and(|state| {
            !state.unavailable
                && state
                    .published
                    .as_ref()
                    .is_some_and(|value| Arc::ptr_eq(value, &self.observed))
        });
        if !current {
            return action(None);
        }
        self.registry
            .with_original_snapshot(&self.settings, |current| {
                action(if current {
                    self.provider.as_deref()
                } else {
                    None
                })
            })
    }
}

pub(crate) async fn context_member(
    services: &Services,
    request: &RepositoryReadRequest,
    workspace: &WorkspaceId,
) -> AdmissionResult<ContextMember> {
    let (workspace, agent) =
        crate::repository_read_source::context_member(services, request, workspace).await?;
    Ok(ContextMember { workspace, agent })
}

fn resolver(facts: Option<&RepositoryConnectionFacts>) -> AdmissionResult<CanonicalRemoteResolver> {
    let mut instances = vec![RemoteInstance::github_com()];
    if let Some(facts) =
        facts.filter(|facts| facts.approval() == RepositoryDescriptorState::Approved)
    {
        if let Some(descriptor) = facts.descriptor() {
            instances.push(RemoteInstance::gitlab(descriptor.instance().clone()));
        }
    }
    CanonicalRemoteResolver::new(instances, Vec::new()).map_err(|_| AdmissionError::Unavailable)
}

fn target_context(
    target: &RepositoryTarget,
    facts: Option<&RepositoryConnectionFacts>,
) -> RepositoryTargetContext {
    let facts = facts.filter(|facts| {
        target.provider == RepositoryProvider::Gitlab
            && facts.descriptor().is_some_and(|descriptor| {
                descriptor.instance().as_str() == target.instance_base_url
            })
    });
    let settled = facts.and_then(RepositoryConnectionFacts::settled);
    let availability = if settled.is_some() {
        RepositoryAvailability::Connected
    } else if facts
        .is_some_and(|facts| facts.lifecycle() == RepositoryConnectionState::Disconnected)
    {
        RepositoryAvailability::Disconnected
    } else {
        RepositoryAvailability::Unknown
    };
    let capabilities = [
        RepositoryOperation::ReadReview,
        RepositoryOperation::ReadIssue,
        RepositoryOperation::CreateReview,
        RepositoryOperation::Clone,
        RepositoryOperation::Fetch,
        RepositoryOperation::Push,
    ]
    .into_iter()
    .map(|operation| RepositoryCapability {
        operation,
        state: if matches!(
            operation,
            RepositoryOperation::Clone | RepositoryOperation::Fetch | RepositoryOperation::Push
        ) && facts
            .and_then(RepositoryConnectionFacts::child_policy)
            .is_some_and(|(state, _)| state == RepositoryChildPolicyState::Disabled)
        {
            RepositoryCapabilityState::Unavailable
        } else {
            RepositoryCapabilityState::Unknown
        },
    })
    .collect();
    RepositoryTargetContext {
        target: target.clone(),
        provider_project_id: None,
        connection: settled.map(|settled| settled.selected().binding.scope.clone()),
        availability,
        capabilities,
    }
}

#[cfg(test)]
pub(crate) mod tests;
