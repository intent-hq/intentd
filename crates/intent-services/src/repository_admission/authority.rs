//! Original entry provenance and injected durable authority for the inactive engine.
//!
//! None of these values deserialize. A future trusted entry adapter must capture
//! before spawning; a future store adapter must read the original credential and
//! durable roles. No such adapter or auth-writer hook is installed by this module.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_core::caller::{
    current_caller, current_wire_credential, Caller, CredentialLease, WireCredential,
};
use intent_core::{
    AgentId, BoxFuture, HostRole, NativeReviewStage, PrincipalCredential, PrincipalId, WorkspaceId,
    WorkspaceRole,
};

use super::{AdmissionError, AdmissionResult};

/// The trusted transport/agent/daemon adapter supplies this, never request JSON.
#[derive(Clone, Copy)]
pub(crate) enum RepositoryEntry {
    Bearer,
    AdmittedLocal,
    AgentCallback,
    DaemonTask,
}

/// Authentic task-local provenance, retained even when the request changes tasks.
/// Deliberately neither Debug nor Serde: a personal credential includes its hash.
pub(crate) struct OriginalRepositoryCaller {
    caller: Caller,
    credential: Option<WireCredential>,
    entry: RepositoryEntry,
}

impl OriginalRepositoryCaller {
    pub(crate) fn capture(entry: RepositoryEntry) -> AdmissionResult<Self> {
        let caller = current_caller().ok_or(AdmissionError::Denied)?;
        let credential = current_wire_credential();
        let valid = match (&caller, &credential, entry) {
            (Caller::Wire { principal_id, .. }, Some(wire), RepositoryEntry::Bearer) => {
                principal_id == wire.principal_id()
            }
            (Caller::Wire { .. }, None, RepositoryEntry::AdmittedLocal)
            | (Caller::Agent { .. }, None, RepositoryEntry::AgentCallback)
            | (Caller::Daemon, None, RepositoryEntry::DaemonTask) => true,
            _ => false,
        };
        if !valid {
            return Err(AdmissionError::Denied);
        }
        Ok(Self {
            caller,
            credential,
            entry,
        })
    }

    pub(crate) fn caller(&self) -> &Caller {
        &self.caller
    }

    pub(crate) fn wire_credential(&self) -> Option<&WireCredential> {
        self.credential.as_ref()
    }

    pub(crate) async fn legacy_lease(&self) -> AdmissionResult<Option<CredentialLease>> {
        match self.credential.as_ref() {
            Some(WireCredential::Legacy { authority, .. }) => authority
                .authorize()
                .await
                .map(Some)
                .map_err(|error| match error {
                    intent_core::Error::Forbidden(_) | intent_core::Error::NotFound(_) => {
                        AdmissionError::Denied
                    }
                    _ => AdmissionError::Unavailable,
                }),
            _ => Ok(None),
        }
    }

    pub(crate) fn verify(
        &self,
        facts: &RepositoryAuthorityFacts,
        workspace: &WorkspaceId,
    ) -> AdmissionResult<()> {
        if self.caller != facts.caller || !facts.workspace_exists || workspace != &facts.workspace {
            return Err(AdmissionError::Denied);
        }
        match (&self.caller, &self.credential, self.entry) {
            (
                Caller::Wire { principal_id, .. },
                Some(WireCredential::Principal { token_hash, .. }),
                RepositoryEntry::Bearer,
            ) => {
                let credential = facts.credential.as_ref().ok_or(AdmissionError::Denied)?;
                if credential.principal_id != *principal_id
                    || credential.token_hash != *token_hash
                    || !credential.is_active()
                {
                    return Err(AdmissionError::Denied);
                }
            }
            (
                Caller::Wire { principal_id, .. },
                Some(WireCredential::Legacy { .. }),
                RepositoryEntry::Bearer,
            )
            | (Caller::Wire { principal_id, .. }, None, RepositoryEntry::AdmittedLocal) => {
                if facts.primary_principal_id.as_ref() != Some(principal_id) {
                    return Err(AdmissionError::Denied);
                }
            }
            (Caller::Agent { .. }, None, RepositoryEntry::AgentCallback)
            | (Caller::Daemon, None, RepositoryEntry::DaemonTask) => {}
            _ => return Err(AdmissionError::Denied),
        }
        if let Caller::Wire {
            principal_id,
            host_role: HostRole::Owner,
        } = &self.caller
        {
            if facts.primary_principal_id.as_ref() != Some(principal_id) {
                return Err(AdmissionError::Denied);
            }
        }
        Ok(())
    }
}

/// Private continuity evidence. Store revisions remain independent and retain
/// tombstones; no process-local scalar, timestamp or hash substitutes for them.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum RepositoryAuthorityProvenance {
    Store(Box<intent_store::RepositoryAuthoritySnapshot>),
    Internal {
        workspace: Box<intent_store::RepositoryWorkspaceAuthoritySnapshot>,
        agent: Option<RepositoryAgentIdentity>,
    },
    #[cfg(test)]
    Injected(u64),
}

/// Actual session identity, not a generation or permission grant. Agent writer
/// hooks must retire the separately supplied lifetime even for an ABA change.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RepositoryAgentIdentity {
    pub id: AgentId,
    pub workspace_id: WorkspaceId,
    pub parent_agent_id: Option<AgentId>,
    pub backend_session_id: Option<AgentId>,
    pub acp_session_id: Option<String>,
}

/// Fresh bounded authority facts; current roles alone cannot prove continuity.
#[derive(Clone)]
pub(crate) struct RepositoryAuthorityFacts {
    pub caller: Caller,
    pub workspace: WorkspaceId,
    pub workspace_exists: bool,
    pub primary_principal_id: Option<PrincipalId>,
    pub workspace_role: Option<WorkspaceRole>,
    pub credential: Option<PrincipalCredential>,
    pub provenance: RepositoryAuthorityProvenance,
    /// Explicit internal permissions, never borrowed from a human Owner caller.
    pub internal_stages: Vec<NativeReviewStage>,
}

impl RepositoryAuthorityFacts {
    pub(super) fn permits(&self, stage: NativeReviewStage) -> bool {
        match &self.caller {
            Caller::Wire { host_role, .. } => match host_role {
                HostRole::Owner => true,
                HostRole::Member => !self.workspace.is_chief(),
                HostRole::Guest => {
                    self.workspace_role.is_some() && stage != NativeReviewStage::CreatePr
                }
            },
            Caller::Agent { .. } | Caller::Daemon => self.internal_stages.contains(&stage),
        }
    }

    pub(super) fn identity(&self) -> AuthorityIdentity {
        AuthorityIdentity {
            caller: self.caller.clone(),
            workspace: self.workspace.clone(),
            primary_principal_id: self.primary_principal_id.clone(),
            workspace_role: self.workspace_role,
            provenance: self.provenance.clone(),
            internal_stages: self.internal_stages.clone(),
        }
    }
}

#[derive(PartialEq, Eq)]
pub(super) struct AuthorityIdentity {
    caller: Caller,
    workspace: WorkspaceId,
    primary_principal_id: Option<PrincipalId>,
    workspace_role: Option<WorkspaceRole>,
    provenance: RepositoryAuthorityProvenance,
    internal_stages: Vec<NativeReviewStage>,
}

pub(crate) trait RepositoryAuthoritySource: Send + Sync {
    /// Re-read the original credential and durable actor/workspace, including
    /// originally Owner callers. Never substitute the current primary bearer.
    fn read<'a>(
        &'a self,
        original: &'a OriginalRepositoryCaller,
        workspace: &'a WorkspaceId,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryAuthorityFacts>>;
}

/// One server-owned lifetime. Real writers must retire it before mutation; a
/// replacement gets a different allocation. Retirement is permanent (no ABA).
/// This module supplies the mechanism, not the still-missing writer coverage.
#[derive(Clone, Default)]
pub(crate) struct RepositoryRetirement {
    state: Arc<RetirementNode>,
    ancestors: Vec<Arc<RetirementNode>>,
}

/// Optional children have no ancestor chain. The original parent retains only
/// weak links, and every closer snapshots the same live links before joining.
#[derive(Default)]
struct RetirementNode {
    fence: Mutex<bool>,
    closed: AtomicBool,
    optional: Mutex<Vec<Weak<RetirementNode>>>,
    cancelled: tokio::sync::Notify,
}

impl RetirementNode {
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancelled.notify_waiters();
        // Join admitted synchronous actions, never an async preparation task.
        *self
            .fence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        let children = {
            let mut links = self
                .optional
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            links.retain(|link| link.strong_count() != 0);
            links.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        // Neither the parent fence nor the dependent-list lock crosses a wait.
        for child in children {
            child.close();
        }
    }
}

impl RepositoryRetirement {
    #[cfg(test)]
    pub(crate) fn with_deletion_test_dispatch<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        self.dispatch(action)
    }

    /// A lock session may end without ending its retained request. Dispatch
    /// still consumes every original ancestor fence before this local leaf.
    pub(super) fn source_child(&self) -> Self {
        let mut ancestors = self.ancestors.clone();
        ancestors.push(self.state.clone());
        Self {
            state: Arc::new(RetirementNode::default()),
            ancestors,
        }
    }

    pub(crate) fn retire(&self) {
        // A permanent denial observed by one source also retires its original
        // request. Poison already excludes dispatch. No child lock precedes a
        // parent lock, including while retirement waits for an admitted start.
        for parent in &self.ancestors {
            parent.close();
        }
        self.end_scope();
    }

    /// Normal lock/subscription cleanup ends only its local operation scope.
    /// Cancellation and permanent authority changes must use `retire` instead.
    pub(crate) fn end_scope(&self) {
        self.state.close();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub(super) fn link_optional(&self, local: &Self) -> AdmissionResult<()> {
        if !self.ancestors.is_empty()
            || !local.ancestors.is_empty()
            || Arc::ptr_eq(&self.state, &local.state)
        {
            return Err(AdmissionError::Denied);
        }
        let parent = self
            .state
            .fence
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if *parent || self.is_closed() || local.is_closed() {
            return Err(AdmissionError::Retired);
        }
        let mut links = self
            .state
            .optional
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        links.retain(|link| link.strong_count() != 0);
        links.push(Arc::downgrade(&local.state));
        Ok(())
    }

    pub(super) fn unlink_optional(&self, local: &Self) {
        let mut links = self
            .state
            .optional
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        links.retain(|link| {
            link.strong_count() != 0 && !Weak::ptr_eq(link, &Arc::downgrade(&local.state))
        });
    }

    pub(super) async fn cancelled(&self) {
        loop {
            let notified = self.state.cancelled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    /// The caller already holds the common required parent/source fences.
    /// This leaf is independent: contention or poison may only omit it.
    pub(super) fn with_optional<T>(&self, action: impl FnOnce(bool) -> T) -> T {
        let guard = self.state.fence.try_lock();
        let include = self.ancestors.is_empty()
            && !self.is_closed()
            && guard.as_ref().is_ok_and(|retired| !**retired);
        action(include)
    }

    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        self.dispatch(|| Ok(()))
    }

    pub(super) fn dispatch<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        let ancestors = self
            .ancestors
            .iter()
            .map(|parent| parent.fence.lock().map_err(|_| AdmissionError::Retired))
            .collect::<AdmissionResult<Vec<_>>>()?;
        if ancestors.iter().any(|retired| **retired)
            || self
                .ancestors
                .iter()
                .any(|parent| parent.closed.load(Ordering::Acquire))
        {
            return Err(AdmissionError::Retired);
        }
        let retired = self
            .state
            .fence
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if *retired || self.is_closed() {
            return Err(AdmissionError::Retired);
        }
        // Original request -> lock-session child -> operation -> P directory.
        // Never reverse that order or await/read durable state/do I/O here.
        action()
    }
}
