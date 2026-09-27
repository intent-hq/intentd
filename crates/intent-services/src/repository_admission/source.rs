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
use intent_store::RepositoryLifecycleKey;

use crate::repository_admission::lifecycle::RepositorySourceLifetime;

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
        self.0.end_scope();
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
async fn with_repository_source<T, F, Fut>(
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

/// The callable source boundary requires the actual installed Store observer
/// and an original physical-owner handle before ANY awaited authority/root read.
/// Neither the keys nor that handle replaces the existing original-caller gates.
pub(crate) async fn with_captured_repository_source<T, F, Fut>(
    services: &Services,
    original: OriginalRepositoryCaller,
    request_id: String,
    stages: Vec<NativeReviewStage>,
    input: RepositorySourceInput,
    action: F,
) -> AdmissionResult<T>
where
    F: FnOnce(RepositoryOperationAdmission) -> Fut,
    Fut: Future<Output = AdmissionResult<T>>,
{
    let lifetime = crate::repository_admission::request_context::current_source_lifetime()?;
    with_repository_lifecycle_source(
        services, original, request_id, stages, input, lifetime, action,
    )
    .await
    .inspect_err(|error| {
        crate::repository_admission::request_context::retire_current_request_on_denial(*error);
    })
}

/// The original producer or captured scope supplies this lifetime; serialized
/// request fields and current session lookups cannot manufacture it.
pub(crate) async fn with_repository_lifecycle_source<T, F, Fut>(
    services: &Services,
    original: OriginalRepositoryCaller,
    request_id: String,
    stages: Vec<NativeReviewStage>,
    input: RepositorySourceInput,
    lifetime: RepositorySourceLifetime,
    action: F,
) -> AdmissionResult<T>
where
    F: FnOnce(RepositoryOperationAdmission) -> Fut,
    Fut: Future<Output = AdmissionResult<T>>,
{
    let retirement = lifetime.retirement();
    let _pending = RetireOnDrop(retirement.clone());
    // This path is an original producer-qualified input, not canonicalized or
    // reconstructed from a public DTO here. The actual source validates it.
    if !input.facts.worktree_path.is_absolute() {
        return Err(AdmissionError::Unavailable);
    }
    let root = &input.facts.preparation.root;
    let mut keys = vec![
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::Workspace(root.workspace_id.clone()),
        RepositoryLifecycleKey::Worktree(input.facts.worktree_path.clone()),
    ];
    if let intent_core::RepositoryRootKind::Registered { git_root_id } = &root.kind {
        keys.push(RepositoryLifecycleKey::GitRoot(git_root_id.clone()));
    }
    if let Caller::Agent { agent_id } = original.caller() {
        keys.push(RepositoryLifecycleKey::Agent(agent_id.clone()));
    }
    let _subscription = lifetime.subscribe(&services.store, original.caller(), &keys)?;
    with_repository_source(
        services, original, request_id, stages, input, retirement, action,
    )
    .await
}

#[cfg(test)]
#[path = "source_tests/authority.rs"]
mod tests;

#[cfg(test)]
#[path = "lifecycle/source_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "source_tests/reader.rs"]
mod reader_tests;

#[cfg(test)]
mod captured_scope_tests {
    use intent_acp::mcp_server::request_context::McpRequestContext;
    use intent_core::caller::with_caller;

    use super::*;
    use crate::repository_admission::lifecycle::{FixtureOriginOwner, RepositoryLifecycleRegistry};
    use crate::repository_admission::request_context::RepositoryCallbackContext;
    use crate::repository_admission::{begin_repository_stage, revalidate_repository_stage};
    use crate::repository_admission_source_tests::fixtures::Fixture;

    #[tokio::test]
    async fn original_acp_scope_enters_real_sources_and_store_retirement_reaches_checked_stage() {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let agent_id = tests::agent(f).await;
        let caller = Caller::Agent {
            agent_id: agent_id.clone(),
        };
        let services = Services::new(f.store.clone());
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&f.store).await.unwrap();
        // The transport scope is real; physical creation remains an explicit
        // fixture until the original Store initialization proof is composed.
        let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
        let scope = McpRequestContext::capture(&callback);
        let absent = with_captured_repository_source(
            &services,
            tests::internal(caller.clone()).await,
            "absent".into(),
            vec![NativeReviewStage::Commit],
            tests::input(f),
            |_| async { panic!("missing original scope entered") },
        )
        .await;
        assert!(matches!(absent, Err::<(), _>(AdmissionError::Unavailable)));
        let mut entered = false;
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                let entered = &mut entered;
                let agent_id = &agent_id;
                with_captured_repository_source(
                    &services,
                    tests::internal(caller).await,
                    "original".into(),
                    vec![NativeReviewStage::Commit],
                    tests::input(f),
                    |admission| async move {
                        *entered = true;
                        let checked =
                            revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                                .await
                                .unwrap();
                        f.store
                            .replace_acp_session_id(
                                &f.workspace.id,
                                agent_id,
                                "original-acp",
                                "replacement-acp",
                            )
                            .await
                            .unwrap();
                        assert!(matches!(
                            begin_repository_stage(checked),
                            Err(AdmissionError::Retired)
                        ));
                        Ok(())
                    },
                )
                .await
                .unwrap();
            })),
        )
        .await;
        assert!(entered);
    }

    #[tokio::test]
    async fn normal_source_completion_keeps_original_scope_for_preparation_but_retires_escaped_admission(
    ) {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let caller = Caller::Agent {
            agent_id: tests::agent(f).await,
        };
        let services = Services::new(f.store.clone());
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&f.store).await.unwrap();
        let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
        let scope = McpRequestContext::capture(&callback);
        let mut escaped = None;
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                escaped = Some(
                    with_captured_repository_source(
                        &services,
                        tests::internal(caller.clone()).await,
                        "operation".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |admission| async move {
                            let checked =
                                revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                                    .await?;
                            Ok((admission, checked))
                        },
                    )
                    .await
                    .unwrap(),
                );
            })),
        )
        .await;
        let (admission, checked) = escaped.unwrap();
        assert!(matches!(
            begin_repository_stage(checked),
            Err(AdmissionError::Retired)
        ));
        assert!(matches!(
            revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
            Err(AdmissionError::Retired)
        ));
        let mut prepared = false;
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                with_captured_repository_source(
                    &services,
                    tests::internal(caller.clone()).await,
                    "preparation".into(),
                    vec![NativeReviewStage::Commit],
                    tests::input(f),
                    |admission| async move {
                        revalidate_repository_stage(&admission, NativeReviewStage::Commit).await?;
                        Ok(())
                    },
                )
                .await
                .expect(
                    "normal source return must preserve the SAME original request for preparation",
                );
                prepared = true;
            })),
        )
        .await;
        assert!(prepared);
    }

    #[tokio::test]
    async fn observed_source_denial_retires_parent_even_if_action_handles_error_but_unavailable_recovers(
    ) {
        for change in 0..3 {
            let fixture = Fixture::new().await;
            let f = &fixture;
            let caller = Caller::Agent {
                agent_id: tests::agent(f).await,
            };
            let services = Services::new(f.store.clone());
            let service = &services;
            let registry = Arc::new(RepositoryLifecycleRegistry::default());
            registry.install(&f.store).await.unwrap();
            let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
            let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
            let scope = McpRequestContext::capture(&callback);
            with_caller(
                caller.clone(),
                scope.scope(Box::pin(async {
                    with_captured_repository_source(
                        service,
                        tests::internal(caller.clone()).await,
                        "observe".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |admission| async move {
                            let config = f.path.join(".git/config");
                            let prior = std::fs::read(&config).unwrap();
                            match change {
                                0 => {
                                    service.pending_workspace_deletes.schedule(
                                        f.workspace.id.to_string(),
                                        "2026-09-27T01:00:00Z".into(),
                                        |_| tokio::spawn(async {}),
                                    );
                                }
                                1 => {
                                    f.git(&f.path, &["checkout", "--detach", "main"]);
                                }
                                _ => std::fs::write(&config, "[malformed\n").unwrap(),
                            }
                            let observed =
                                revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                                    .await;
                            match change {
                                0 => {
                                    service
                                        .pending_workspace_deletes
                                        .cancel(f.workspace.id.as_str());
                                    assert!(matches!(observed, Err(AdmissionError::Denied)));
                                }
                                1 => {
                                    f.git(&f.path, &["checkout", "main"]);
                                    assert!(matches!(
                                        observed,
                                        Err(AdmissionError::BindingChanged)
                                    ));
                                }
                                _ => {
                                    std::fs::write(&config, prior).unwrap();
                                    assert!(matches!(observed, Err(AdmissionError::Unavailable)));
                                }
                            }
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
                })),
            )
            .await;
            with_caller(
                caller.clone(),
                scope.scope(Box::pin(async {
                    let later = with_captured_repository_source(
                        service,
                        tests::internal(caller.clone()).await,
                        "same-after-restore".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |_| async { Ok(()) },
                    )
                    .await;
                    if change == 2 {
                        assert_eq!(later, Ok(()));
                    } else {
                        assert_eq!(later, Err(AdmissionError::Retired));
                    }
                })),
            )
            .await;
            let fresh = McpRequestContext::capture(&callback);
            with_caller(
                caller.clone(),
                fresh.scope(Box::pin(async {
                    with_captured_repository_source(
                        service,
                        tests::internal(caller.clone()).await,
                        "fresh-after-restore".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |_| async { Ok(()) },
                    )
                    .await
                    .unwrap();
                })),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn root_writer_after_normal_source_return_still_retires_original_preparation() {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let caller = Caller::Agent {
            agent_id: tests::agent(f).await,
        };
        let services = Services::new(f.store.clone());
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&f.store).await.unwrap();
        let owner = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        let callback = RepositoryCallbackContext::new(&registry, Some(owner.origin()));
        let scope = McpRequestContext::capture(&callback);
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                with_captured_repository_source(
                    &services,
                    tests::internal(caller.clone()).await,
                    "before-writer".into(),
                    vec![NativeReviewStage::Commit],
                    tests::input(f),
                    |_| async { Ok(()) },
                )
                .await
                .unwrap();
            })),
        )
        .await;
        f.store
            .archive_workspace_detaching_guests(&f.workspace.id, "2026-09-27T01:00:00Z")
            .await
            .unwrap();
        f.store
            .unarchive_workspace_if_archived(&f.workspace.id, "2026-09-27T01:01:00Z")
            .await
            .unwrap();
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                assert_eq!(
                    with_captured_repository_source(
                        &services,
                        tests::internal(caller.clone()).await,
                        "after-writer".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |_| async { Ok(()) },
                    )
                    .await,
                    Err(AdmissionError::Retired)
                );
            })),
        )
        .await;
    }

    #[tokio::test]
    async fn original_initialized_owner_composes_actual_store_git_and_source_lock_lifetime() {
        use crate::repository_admission::lifecycle::physical_owner::{
            RepositoryCreationIntent, RepositoryCreationOwner,
        };
        let fixture = Fixture::new().await;
        let f = &fixture;
        let agent_id = tests::agent(f).await;
        let caller = Caller::Agent {
            agent_id: agent_id.clone(),
        };
        let services = Services::new(f.store.clone());
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        registry.install(&f.store).await.unwrap();
        let creator = RepositoryCreationOwner::allocate(
            &registry,
            &f.store,
            f.workspace.id.clone(),
            agent_id,
            RepositoryCreationIntent::Loaded {
                session_id: "original-acp".into(),
            },
        )
        .unwrap();
        let pending = creator.callback().capture();
        // This completed producer future is a fixture, not manager integration.
        // Store confirmation, R original ownership and subsequent source are real.
        let owner = creator
            .initialize(&f.store, || async { Ok("original-acp".into()) })
            .await
            .unwrap();
        let callback = owner.callback();
        let scope = McpRequestContext::capture(&callback);
        let mut escaped = None;
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                assert!(matches!(
                    pending.source_lifetime(),
                    Err(AdmissionError::Unavailable)
                ));
                escaped = Some(
                    with_captured_repository_source(
                        &services,
                        tests::internal(caller.clone()).await,
                        "initialized-owner".into(),
                        vec![NativeReviewStage::Commit],
                        tests::input(f),
                        |admission| async move {
                            revalidate_repository_stage(&admission, NativeReviewStage::Commit).await
                        },
                    )
                    .await
                    .unwrap(),
                );
            })),
        )
        .await;
        assert!(matches!(
            begin_repository_stage(escaped.unwrap()),
            Err(AdmissionError::Retired)
        ));
        with_caller(
            caller.clone(),
            scope.scope(Box::pin(async {
                with_captured_repository_source(
                    &services,
                    tests::internal(caller.clone()).await,
                    "initialized-preparation".into(),
                    vec![NativeReviewStage::Commit],
                    tests::input(f),
                    |_| async { Ok(()) },
                )
                .await
                .unwrap();
            })),
        )
        .await;
        let request = callback.capture();
        drop(owner);
        with_caller(caller, async {
            assert!(matches!(
                request.source_lifetime(),
                Err(AdmissionError::Retired)
            ));
        })
        .await;
    }
}
