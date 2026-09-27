//! Original entry provenance and injected durable authority for the inactive engine.
//!
//! None of these values deserialize. A future trusted entry adapter must capture
//! before spawning; a future store adapter must read the original credential and
//! durable roles. No such adapter or auth-writer hook is installed by this module.

use std::sync::{Arc, Mutex};

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
    state: Arc<Mutex<bool>>,
}

impl RepositoryRetirement {
    pub(crate) fn retire(&self) {
        // Poison is already fail-closed in dispatch/check_current.
        if let Ok(mut retired) = self.state.lock() {
            *retired = true;
        }
    }

    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        self.dispatch(|| Ok(()))
    }

    pub(super) fn dispatch<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        let retired = self.state.lock().map_err(|_| AdmissionError::Retired)?;
        if *retired {
            return Err(AdmissionError::Retired);
        }
        // Leaf only. Actions may acquire the P directory fence after this one,
        // never the reverse. No await, durable read, Git or network under here.
        action()
    }
}
