//! Concrete sources for the inactive native admission engine.
//!
//! The original caller, actual service gates and Store continuity are checked
//! before waiting for the real worktree lock and again inside its lifetime.
//! Provider bindings remain trusted inputs, not discoveries from Git URLs.
//! No entrypoint, mutation writer, context feed or native effect is registered.

use std::future::Future;
use std::sync::Arc;

use intent_core::caller::{Caller, WireCredential};
use intent_core::{BoxFuture, NativeReviewStage, WorkspaceId};
use intent_sourcecontrol::remote_project::CanonicalRemoteResolver;

use crate::repository_admission::{
    capture_repository_operation, AdmissionError, AdmissionResult, OriginalRepositoryCaller,
    RepositoryAgentIdentity, RepositoryAuthorityFacts, RepositoryAuthorityProvenance,
    RepositoryAuthoritySource, RepositoryOperationAdmission, RepositoryOperationFacts,
    RepositoryOperationSource, RepositoryRetirement,
};
use crate::repository_admission_durable_source::check_stage_gates;
use crate::repository_context_reader::{
    AdmittedRepositoryRoot, GitConfigEnvironment, RepositoryContextInput,
};
use crate::Services;

use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};

/// Exact server-admitted inputs. These do not deserialize, and no constructor
/// fills accounts, authority or transport from a displayed repository context.
pub(crate) struct RepositorySourceInput {
    pub facts: RepositoryOperationFacts,
    pub context: RepositoryContextInput,
    pub resolver: CanonicalRemoteResolver,
    pub environment: GitConfigEnvironment,
    #[cfg(test)]
    pub before_lock: Option<Arc<tokio::sync::Notify>>,
}

struct RetireOnDrop(RepositoryRetirement);

impl Drop for RetireOnDrop {
    fn drop(&mut self) {
        self.0.retire();
    }
}

fn local_error(error: &intent_core::Error) -> AdmissionError {
    match error {
        intent_core::Error::NotFound(_) | intent_core::Error::Forbidden(_) => {
            AdmissionError::Denied
        }
        _ => AdmissionError::Unavailable,
    }
}

/// A non-human entry retains its own identity. A missing human never takes the
/// internal branch and neither an agent nor the daemon borrows a principal.
async fn read_current_authority(
    services: &Services,
    original: &OriginalRepositoryCaller,
    workspace: &WorkspaceId,
    stages: &[NativeReviewStage],
    retirement: &RepositoryRetirement,
) -> AdmissionResult<RepositoryAuthorityFacts> {
    retirement.check_current()?;
    let Caller::Wire {
        principal_id,
        host_role,
    } = original.caller()
    else {
        return read_internal_authority(services, original, workspace, stages, retirement).await;
    };
    let hash = match original.wire_credential() {
        Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
        _ => None,
    };
    let before = services
        .store
        .repository_authority_snapshot(workspace, principal_id, hash)
        .await
        .map_err(|error| local_error(&error))?;
    if before.workspace.value.is_none() || before.principal.value.is_none() {
        return Err(AdmissionError::Denied);
    }
    // Existing policy owns permission. Its reads are surrounded by the atomic
    // continuity snapshots, so even a remove/re-add cannot look like no change.
    check_stage_gates(services, original, workspace, stages).await?;
    let current_role = services
        .store
        .get_host_role(principal_id)
        .await
        .map_err(|error| local_error(&error))?;
    if &current_role != host_role {
        return Err(AdmissionError::Denied);
    }
    let credential = if let Some(hash) = hash {
        services
            .store
            .lookup_principal_credential(hash)
            .await
            .map_err(|error| local_error(&error))?
    } else {
        None
    };
    let after = services
        .store
        .repository_authority_snapshot(workspace, principal_id, hash)
        .await
        .map_err(|error| local_error(&error))?;
    retirement.check_current()?;
    if before != after {
        return Err(AdmissionError::Retired);
    }
    let facts = RepositoryAuthorityFacts {
        caller: original.caller().clone(),
        workspace: workspace.clone(),
        workspace_exists: before.workspace.value.is_some(),
        primary_principal_id: before
            .primary_principal
            .value
            .as_ref()
            .map(|row| row.id.clone()),
        workspace_role: before.workspace_grant.value,
        credential,
        provenance: RepositoryAuthorityProvenance::Store(Box::new(before)),
        internal_stages: Vec::new(),
    };
    original.verify(&facts, workspace)?;
    Ok(facts)
}

async fn read_agent_identity(
    services: &Services,
    caller: &Caller,
) -> AdmissionResult<Option<RepositoryAgentIdentity>> {
    let Caller::Agent { agent_id } = caller else {
        return if matches!(caller, Caller::Daemon) {
            Ok(None)
        } else {
            Err(AdmissionError::Denied)
        };
    };
    let session = services
        .store
        .get_agent_session_summary(agent_id)
        .await
        .map_err(|error| local_error(&error))?;
    if session.retired_at.is_some()
        || session.status == intent_core::AgentStatus::Deleted
        || services
            .pending_agent_deletes
            .deadline(agent_id.as_str())
            .is_some()
    {
        return Err(AdmissionError::Denied);
    }
    Ok(Some(RepositoryAgentIdentity {
        id: session.id,
        workspace_id: session.workspace_id,
        parent_agent_id: session.parent_agent_id,
        backend_session_id: session.backend_session_id,
        acp_session_id: session.acp_session_id,
    }))
}

async fn read_internal_authority(
    services: &Services,
    original: &OriginalRepositoryCaller,
    workspace: &WorkspaceId,
    stages: &[NativeReviewStage],
    retirement: &RepositoryRetirement,
) -> AdmissionResult<RepositoryAuthorityFacts> {
    if original.wire_credential().is_some() || matches!(original.caller(), Caller::Wire { .. }) {
        return Err(AdmissionError::Denied);
    }
    let before = services
        .store
        .repository_workspace_authority_snapshot(workspace)
        .await
        .map_err(|error| local_error(&error))?;
    if before.workspace.value.is_none() {
        return Err(AdmissionError::Denied);
    }
    let agent = read_agent_identity(services, original.caller()).await?;
    check_stage_gates(services, original, workspace, stages).await?;
    let after = services
        .store
        .repository_workspace_authority_snapshot(workspace)
        .await
        .map_err(|error| local_error(&error))?;
    let current_agent = read_agent_identity(services, original.caller()).await?;
    retirement.check_current()?;
    if before != after || agent != current_agent {
        return Err(AdmissionError::Retired);
    }
    // Existing gates authorize these exact stages under the captured internal
    // caller. Session rows and workspace revisions do not authorize on their own.
    let facts = RepositoryAuthorityFacts {
        caller: original.caller().clone(),
        workspace: workspace.clone(),
        workspace_exists: true,
        primary_principal_id: None,
        workspace_role: None,
        credential: None,
        provenance: RepositoryAuthorityProvenance::Internal {
            workspace: Box::new(before),
            agent,
        },
        internal_stages: stages.to_vec(),
    };
    original.verify(&facts, workspace)?;
    Ok(facts)
}

fn same_wire(left: Option<&WireCredential>, right: Option<&WireCredential>) -> bool {
    match (left, right) {
        (None, None) => true,
        (
            Some(WireCredential::Principal {
                principal_id: a,
                token_hash: x,
            }),
            Some(WireCredential::Principal {
                principal_id: b,
                token_hash: y,
            }),
        ) => a == b && x == y,
        (
            Some(WireCredential::Legacy {
                principal_id: a,
                authority: x,
            }),
            Some(WireCredential::Legacy {
                principal_id: b,
                authority: y,
            }),
        ) => a == b && Arc::ptr_eq(x, y),
        _ => false,
    }
}

/// Only constructed by the lock-scoped session below. Arc clones cannot keep
/// the worktree lock or its authority alive after that session exits.
struct RepositorySource {
    services: Services,
    git: Arc<RepositoryGitSource>,
    retirement: RepositoryRetirement,
    caller: Caller,
    wire: Option<WireCredential>,
    workspace: WorkspaceId,
    stages: Vec<NativeReviewStage>,
    provenance: RepositoryAuthorityProvenance,
    context: RepositoryContextInput,
    resolver: CanonicalRemoteResolver,
    environment: GitConfigEnvironment,
}

impl RepositorySource {
    fn check_workspace_lifetime(&self) -> AdmissionResult<()> {
        self.retirement.check_current()?;
        // Pending deletion is a Services-owned observation; Store rows do not
        // contain the projected deadline. Cancellation never revives this leaf.
        if self
            .services
            .pending_workspace_deletes
            .deadline(self.workspace.as_str())
            .is_some()
        {
            self.retirement.retire();
            return Err(AdmissionError::Denied);
        }
        Ok(())
    }
}

impl RepositoryAuthoritySource for RepositorySource {
    fn read<'a>(
        &'a self,
        original: &'a OriginalRepositoryCaller,
        workspace: &'a WorkspaceId,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryAuthorityFacts>> {
        Box::pin(async move {
            self.check_workspace_lifetime()?;
            if original.caller() != &self.caller
                || workspace != &self.workspace
                || !same_wire(original.wire_credential(), self.wire.as_ref())
            {
                return Err(AdmissionError::Denied);
            }
            self.git.check_root().await?;
            let facts = read_current_authority(
                &self.services,
                original,
                workspace,
                &self.stages,
                &self.retirement,
            )
            .await
            .inspect_err(|&error| {
                if matches!(error, AdmissionError::Denied | AdmissionError::Retired) {
                    self.retirement.retire();
                }
            })?;
            if facts.provenance != self.provenance {
                self.retirement.retire();
                return Err(AdmissionError::Retired);
            }
            self.git.check_root().await?;
            self.check_workspace_lifetime()?;
            Ok(facts)
        })
    }
}

impl RepositoryOperationSource for RepositorySource {
    fn observe<'a>(
        &'a self,
        original: &'a RepositoryOperationFacts,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryOperationFacts>> {
        Box::pin(async move {
            self.check_workspace_lifetime()?;
            let context = RepositoryContextInput {
                scope: self.context.scope.clone(),
                revision: self.context.revision.clone(),
                roots: self
                    .context
                    .roots
                    .iter()
                    .map(|root| AdmittedRepositoryRoot {
                        root: root.root.clone(),
                        path: root.path.clone(),
                        saved_selection: root.saved_selection.clone(),
                        explicit_target: root.explicit_target.clone(),
                        targets: root.targets.clone(),
                    })
                    .collect(),
            };
            let environment = GitConfigEnvironment {
                global_config: self.environment.global_config.clone(),
                system_config: self.environment.system_config.clone(),
                extra_config_paths: self.environment.extra_config_paths.clone(),
            };
            let observed = self
                .git
                .observe_operation(original, context, self.resolver.clone(), environment)
                .await
                .inspect_err(|error| {
                    // Early typed errors never reach the engine's full-facts
                    // comparison. Preserve its permanent-binding semantics here.
                    if matches!(
                        error,
                        AdmissionError::Denied
                            | AdmissionError::Retired
                            | AdmissionError::BindingChanged
                    ) {
                        self.retirement.retire();
                    }
                })?;
            self.check_workspace_lifetime()?;
            Ok(observed)
        })
    }
}

/// Admission remains confined to the Services-owned lock and one request.
/// The caller supplies the original entry before any spawn/queue. This function
/// neither dispatches an effect nor upgrades a read/child request into a write.
pub(crate) async fn with_repository_source<T, F, Fut>(
    services: &Services,
    original: OriginalRepositoryCaller,
    request_id: String,
    stages: Vec<NativeReviewStage>,
    input: RepositorySourceInput,
    retirement: RepositoryRetirement,
    action: F,
) -> AdmissionResult<T>
where
    F: FnOnce(RepositoryOperationAdmission) -> Fut,
    Fut: Future<Output = AdmissionResult<T>>,
{
    let _pending = RetireOnDrop(retirement.clone());
    let workspace = &input.facts.preparation.root.workspace_id;
    let initial = {
        // The initial read precedes the lock wait. Engine revalidation already
        // holds this lease while calling Source::read; never reacquire there.
        let _legacy = original.legacy_lease().await?;
        read_current_authority(services, &original, workspace, &stages, &retirement).await?
    };
    let record = RootRecord::read(&services.store, &input.facts.preparation.root).await?;
    #[cfg(test)]
    if let Some(ready) = &input.before_lock {
        ready.notify_one();
    }
    RepositoryGitSource::with_locked(
        &services.store,
        &services.worktree_locks,
        record,
        retirement.clone(),
        |git| async move {
            let facts = input.facts;
            let source = Arc::new(RepositorySource {
                services: services.clone(),
                git,
                retirement: retirement.clone(),
                caller: original.caller().clone(),
                wire: original.wire_credential().cloned(),
                workspace: facts.preparation.root.workspace_id.clone(),
                stages: stages.clone(),
                provenance: initial.provenance,
                context: input.context,
                resolver: input.resolver,
                environment: input.environment,
            });
            let admission = capture_repository_operation(
                original, request_id, facts, stages, source, retirement,
            )
            .await?;
            action(admission).await
        },
    )
    .await
}

#[cfg(test)]
#[path = "source_tests/authority.rs"]
mod tests;
