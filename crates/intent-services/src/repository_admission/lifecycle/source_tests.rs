//! Real Store writers with the admission registry and request leaf. Physical
//! origin owners are explicit fixtures; no actual process-entry proof is claimed.

use intent_core::{
    NativeReviewGitReceipt, NativeReviewOutcome, NativeReviewPublication, RepositoryRootKind,
    WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource,
};
use intent_store::{RepositoryLifecycleObserver, Store};
use tokio::sync::{oneshot, Notify};

use super::tests::{agent, input, internal, owner};
use super::*;
use crate::repository_admission::lifecycle::{FixtureOriginOwner, RepositoryLifecycleRegistry};
use crate::repository_admission::{
    begin_repository_stage, classify_repository_completion, revalidate_repository_stage,
    RepositoryCompletion,
};
use crate::repository_admission_source_tests::fixtures::Fixture;

async fn registry(f: &Fixture) -> Arc<RepositoryLifecycleRegistry> {
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    registry.install(&f.store).await.unwrap();
    registry
}

fn lifetime(
    registry: &Arc<RepositoryLifecycleRegistry>,
    physical: &FixtureOriginOwner,
) -> RepositorySourceLifetime {
    RepositorySourceLifetime::new(
        registry.clone(),
        Some(physical.origin()),
        RepositoryRetirement::default(),
    )
}

#[tokio::test]
async fn no_observer_or_missing_physical_origin_never_enters_the_source() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = Arc::new(RepositoryLifecycleRegistry::default());
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    for installed in [false, true] {
        if installed {
            registry.install(&f.store).await.unwrap();
        }
        let binding = RepositorySourceLifetime::new(
            registry.clone(),
            if installed {
                None
            } else {
                Some(physical.origin())
            },
            RepositoryRetirement::default(),
        );
        let result = with_repository_lifecycle_source(
            &services,
            owner(f, false).await,
            "unavailable".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            binding,
            |_| async { panic!("unavailable source entered") },
        )
        .await;
        assert!(matches!(result, Err::<(), _>(AdmissionError::Unavailable)));
    }
}

#[tokio::test]
async fn real_workspace_aba_from_clone_or_independent_store_retires_checked_stage() {
    for mode in 0..4 {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let services = Services::new(f.store.clone());
        let registry = registry(f).await;
        let independent = Store::open(&f.dir.path().join("store.db")).await.unwrap();
        let original = owner(f, false).await;
        let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
        let writer = if mode % 2 == 0 {
            f.store.clone()
        } else {
            independent
        };
        with_repository_lifecycle_source(
            &services,
            original,
            "workspace-aba".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |admission| async move {
                let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                    .await
                    .unwrap();
                match mode {
                    0 => {
                        let mut changed = f.workspace.clone();
                        changed.repository_path =
                            Some(f.dir.path().join("other").to_str().unwrap().into());
                        writer.update_workspace(&changed).await.unwrap();
                        writer.update_workspace(&f.workspace).await.unwrap();
                    }
                    1 => {
                        writer
                            .update_workspace_with_branch(&f.workspace, Some("other"))
                            .await
                            .unwrap();
                        writer
                            .update_workspace_with_branch(&f.workspace, Some("main"))
                            .await
                            .unwrap();
                    }
                    2 => {
                        writer
                            .archive_workspace_detaching_guests(
                                &f.workspace.id,
                                "2026-09-27T01:00:00Z",
                            )
                            .await
                            .unwrap();
                        writer
                            .unarchive_workspace_if_archived(
                                &f.workspace.id,
                                "2026-09-27T01:01:00Z",
                            )
                            .await
                            .unwrap();
                    }
                    _ => {
                        writer.delete_workspace(&f.workspace.id).await.unwrap();
                        writer.insert_workspace(&f.workspace).await.unwrap();
                    }
                }
                assert!(matches!(
                    begin_repository_stage(checked),
                    Err(AdmissionError::Retired)
                ));
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                    Err(AdmissionError::Retired)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
        with_repository_lifecycle_source(
            &services,
            owner(f, false).await,
            "fresh".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |_| async { Ok(()) },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn real_agent_aba_never_allows_an_old_physical_origin_to_recapture() {
    for mode in 0..3 {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let id = agent(f).await;
        let services = Services::new(f.store.clone());
        let registry = registry(f).await;
        let caller = Caller::Agent {
            agent_id: id.clone(),
        };
        let physical = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        let session = f.store.get_agent_session(&id).await.unwrap();
        with_repository_lifecycle_source(
            &services,
            internal(caller.clone()).await,
            "agent-aba".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |admission| async move {
                let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                    .await
                    .unwrap();
                match mode {
                    0 => {
                        let mut changed = session.clone();
                        changed.model = Some("different-model".into());
                        f.store
                            .update_agent_session(&f.workspace.id, &changed)
                            .await
                            .unwrap();
                        f.store
                            .update_agent_session(&f.workspace.id, &session)
                            .await
                            .unwrap();
                    }
                    1 => {
                        f.store
                            .set_agent_session_retired_at(
                                &f.workspace.id,
                                &id,
                                Some("2026-09-27T01:00:00Z"),
                                "2026-09-27T01:00:00Z",
                            )
                            .await
                            .unwrap();
                        f.store
                            .set_agent_session_retired_at(
                                &f.workspace.id,
                                &id,
                                None,
                                "2026-09-27T01:01:00Z",
                            )
                            .await
                            .unwrap();
                    }
                    _ => {
                        f.store
                            .delete_agent_session(&f.workspace.id, &id)
                            .await
                            .unwrap();
                        f.store.insert_agent_session(&session).await.unwrap();
                    }
                }
                assert!(matches!(
                    begin_repository_stage(checked),
                    Err(AdmissionError::Retired)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            with_repository_lifecycle_source(
                &services,
                internal(caller.clone()).await,
                "old-origin".into(),
                vec![NativeReviewStage::Commit],
                input(f),
                lifetime(&registry, &physical),
                |_| async { Ok(()) }
            )
            .await,
            Err(AdmissionError::Retired)
        ));
        // Explicit NEW fixture owner, never a lookup/substitution for old origin.
        let replacement = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        with_repository_lifecycle_source(
            &services,
            internal(caller).await,
            "new-origin".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &replacement),
            |_| async { Ok(()) },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn registered_root_delete_recreate_retires_old_request() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let root = WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: f.workspace.id.clone(),
        path: f.path.to_str().unwrap().into(),
        source: WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: "2026-09-27T00:00:00Z".into(),
        updated_at: "2026-09-27T00:00:00Z".into(),
    };
    f.store.upsert_workspace_git_root(&root).await.unwrap();
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    let mut parameters = input(f);
    parameters.facts.preparation.root.kind = RepositoryRootKind::Registered {
        git_root_id: root.id.clone(),
    };
    parameters.context.roots[0].root = parameters.facts.preparation.root.clone();
    with_repository_lifecycle_source(
        &services,
        original,
        "registered".into(),
        vec![NativeReviewStage::Commit],
        parameters,
        lifetime(&registry, &physical),
        |admission| async move {
            let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                .await
                .unwrap();
            f.store.delete_workspace_git_root(&root.id).await.unwrap();
            f.store.upsert_workspace_git_root(&root).await.unwrap();
            assert!(matches!(
                begin_repository_stage(checked),
                Err(AdmissionError::Retired)
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn subscriptions_exist_before_the_real_worktree_wait() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    let locks = services.worktree_locks.clone();
    let path = f.path.clone();
    let (started, ready) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let blocker = tokio::spawn(async move {
        locks
            .with_lock(&path, || async {
                started.send(()).unwrap();
                released.await.unwrap();
            })
            .await;
    });
    ready.await.unwrap();
    let reached = Arc::new(Notify::new());
    let mut parameters = input(f);
    parameters.before_lock = Some(reached.clone());
    let future = with_repository_lifecycle_source(
        &services,
        original,
        "queued".into(),
        vec![NativeReviewStage::Commit],
        parameters,
        lifetime(&registry, &physical),
        |_| async { panic!("retired queued work ran") },
    );
    tokio::pin!(future);
    tokio::select! { ()=reached.notified()=>{}, _=&mut future=>panic!("did not reach lock wait") }
    f.store
        .update_workspace_with_branch(&f.workspace, Some("other"))
        .await
        .unwrap();
    f.store
        .update_workspace_with_branch(&f.workspace, Some("main"))
        .await
        .unwrap();
    release.send(()).unwrap();
    blocker.await.unwrap();
    assert!(matches!(
        future.await,
        Err::<(), _>(AdmissionError::Retired)
    ));
}

#[tokio::test]
async fn no_op_metadata_and_failed_precondition_preserve_current_request() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    with_repository_lifecycle_source(
        &services,
        original,
        "metadata".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        lifetime(&registry, &physical),
        |admission| async move {
            let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                .await
                .unwrap();
            let mut changed = f.workspace.clone();
            changed.title = "new display name".into();
            f.store.update_workspace(&changed).await.unwrap();
            assert!(!f
                .store
                .unarchive_workspace_if_archived(&f.workspace.id, "2026-09-27T00:00:00Z")
                .await
                .unwrap());
            assert!(f
                .store
                .delete_workspace_git_root(&WorkspaceGitRootId::new())
                .await
                .is_err());
            let stamp = begin_repository_stage(checked).unwrap();
            drop(stamp);
            assert!(matches!(
                admission.execution().unwrap().outcome,
                NativeReviewOutcome::Uncertain { .. }
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_write_after_barrier_blocks_fresh_capture_without_reviving_old_request() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    with_repository_lifecycle_source(
        &services,
        original,
        "failed-insert".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        lifetime(&registry, &physical),
        |admission| async move {
            let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                .await
                .unwrap();
            assert!(f.store.insert_workspace(&f.workspace).await.is_err());
            assert!(matches!(
                begin_repository_stage(checked),
                Err(AdmissionError::Retired)
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
    // The Store writer did not confirm the error as no-effect. Conservative
    // blocking is intentional until that original owner can reconcile it.
    assert!(matches!(
        with_repository_lifecycle_source(
            &services,
            owner(f, false).await,
            "pending".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |_| async { Ok(()) }
        )
        .await,
        Err(AdmissionError::Unavailable)
    ));
}

#[tokio::test]
async fn known_commit_and_uncertain_started_stage_survive_actual_writer_retirement() {
    for completed in [false, true] {
        let fixture = Fixture::new().await;
        let f = &fixture;
        let services = Services::new(f.store.clone());
        let registry = registry(f).await;
        let original = owner(f, false).await;
        let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
        with_repository_lifecycle_source(
            &services,
            original,
            "receipts".into(),
            vec![NativeReviewStage::Commit, NativeReviewStage::Push],
            input(f),
            lifetime(&registry, &physical),
            |admission| async move {
                let stamp = begin_repository_stage(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                        .await
                        .unwrap(),
                )
                .unwrap();
                f.store
                    .archive_workspace_detaching_guests(&f.workspace.id, "2026-09-27T01:00:00Z")
                    .await
                    .unwrap();
                if completed {
                    // The already-admitted disposable effect reports actual data.
                    f.git(
                        &f.path,
                        &[
                            "-c",
                            "user.name=Fixture",
                            "-c",
                            "user.email=fixture@example.invalid",
                            "commit",
                            "--allow-empty",
                            "-m",
                            "fixture",
                        ],
                    );
                    let hash = f.git(&f.path, &["rev-parse", "HEAD"]).trim().to_owned();
                    let receipt = classify_repository_completion(
                        stamp,
                        RepositoryCompletion::Committed {
                            hash: hash.clone(),
                            staging_after: None,
                        },
                    )
                    .unwrap();
                    assert_eq!(
                        receipt.git_receipts,
                        vec![NativeReviewGitReceipt::Commit { commit_hash: hash }]
                    );
                } else {
                    drop(stamp);
                    assert!(matches!(
                        admission.execution().unwrap().outcome,
                        NativeReviewOutcome::Uncertain { .. }
                    ));
                }
                assert!(matches!(
                    admission.execution().unwrap().publication,
                    NativeReviewPublication::Unknown { .. }
                ));
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Push).await,
                    Err(AdmissionError::Retired)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cancelling_lock_wait_and_unknown_owner_ticket_cannot_leak_authority() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    let locks = services.worktree_locks.clone();
    let path = f.path.clone();
    let (started, ready) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let blocker = tokio::spawn(async move {
        locks
            .with_lock(&path, || async {
                started.send(()).unwrap();
                released.await.unwrap();
            })
            .await;
    });
    ready.await.unwrap();
    let reached = Arc::new(Notify::new());
    let mut parameters = input(f);
    parameters.before_lock = Some(reached.clone());
    let binding = lifetime(&registry, &physical);
    let leaf = binding.retirement();
    let mut future = Box::pin(with_repository_lifecycle_source(
        &services,
        original,
        "cancel".into(),
        vec![NativeReviewStage::Commit],
        parameters,
        binding,
        |_| async { Ok(()) },
    ));
    tokio::select! { ()=reached.notified()=>{}, _=&mut future=>panic!("did not wait") }
    drop(future);
    release.send(()).unwrap();
    blocker.await.unwrap();
    assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
    let pending = registry
        .begin_mutation(&[RepositoryLifecycleKey::Workspace(f.workspace.id.clone())])
        .unwrap();
    drop(pending);
    assert!(matches!(
        with_repository_lifecycle_source(
            &services,
            owner(f, false).await,
            "unknown-owner".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |_| async { Ok(()) }
        )
        .await,
        Err(AdmissionError::Unavailable)
    ));
}

#[tokio::test]
async fn managed_reopen_retires_checked_request_before_followup_write() {
    let fixture = Fixture::new().await;
    let f = &fixture;
    let services = Services::new(f.store.clone());
    let registry = registry(f).await;
    let original = owner(f, false).await;
    let physical = FixtureOriginOwner::new(&registry, original.caller().clone()).unwrap();
    with_repository_lifecycle_source(
        &services,
        original,
        "before-reopen".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        lifetime(&registry, &physical),
        |admission| async move {
            let checked = revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                .await
                .unwrap();
            let reopened = Store::open(&f.dir.path().join("store.db")).await.unwrap();
            assert!(matches!(
                begin_repository_stage(checked),
                Err(AdmissionError::Retired)
            ));
            assert!(matches!(
                revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                Err(AdmissionError::Retired)
            ));
            drop(reopened);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        with_repository_lifecycle_source(
            &services,
            owner(f, false).await,
            "old-origin-after-reopen".into(),
            vec![NativeReviewStage::Commit],
            input(f),
            lifetime(&registry, &physical),
            |_| async { panic!("retired physical origin entered") }
        )
        .await,
        Err::<(), _>(AdmissionError::Retired)
    ));
    let fresh = owner(f, false).await;
    let replacement = FixtureOriginOwner::new(&registry, fresh.caller().clone()).unwrap();
    with_repository_lifecycle_source(
        &services,
        fresh,
        "fresh-origin-after-reopen".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        lifetime(&registry, &replacement),
        |_| async { Ok(()) },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn unknown_domain_survives_last_managed_handle_drop() {
    for installed in [false, true] {
        let Fixture {
            dir,
            store,
            workspace,
            ..
        } = Fixture::new().await;
        let registry = Arc::new(RepositoryLifecycleRegistry::default());
        let caller = Caller::Daemon;
        let physical = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
        let keys = [
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::Workspace(workspace.id.clone()),
        ];
        let binding = lifetime(&registry, &physical);
        let leaf = binding.retirement();
        let subscription = if installed {
            registry.install(&store).await.unwrap();
            Some(binding.subscribe(&store, &caller, &keys).unwrap())
        } else {
            None
        };
        // This actual SQL error occurs after the Store mutation barrier. The
        // owner does not assert confirmed settlement merely because it errored.
        assert!(store.insert_workspace(&workspace).await.is_err());
        if installed {
            assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
        }
        drop(store);
        let reopened = Store::open(&dir.path().join("store.db")).await.unwrap();
        if installed {
            let observer: Arc<dyn RepositoryLifecycleObserver> = registry.clone();
            assert!(reopened.has_repository_lifecycle_observer(&observer));
            registry.install(&reopened).await.unwrap();
            let different = Arc::new(RepositoryLifecycleRegistry::default());
            assert_eq!(
                different.install(&reopened).await,
                Err(AdmissionError::Unavailable)
            );
            let replacement = FixtureOriginOwner::new(&registry, caller.clone()).unwrap();
            assert!(matches!(
                lifetime(&registry, &replacement).subscribe(&reopened, &caller, &keys),
                Err(AdmissionError::Unavailable)
            ));
            assert_eq!(leaf.check_current(), Err(AdmissionError::Retired));
        } else {
            assert_eq!(
                registry.install(&reopened).await,
                Err(AdmissionError::Unavailable)
            );
        }
        drop(subscription);
    }
}

mod precise_registered_tests {
    use super::*;
    use crate::repository_admission::lifecycle::physical_owner::{
        RepositoryCreationIntent, RepositoryCreationOwner, RepositoryPhysicalOwner,
    };
    use crate::repository_admission::read_request::{
        PreparedRepositoryOptional, RepositoryReadOwner, RepositoryReadRequest,
    };
    use crate::repository_admission::request_context::{
        current_read_request, RepositoryCallbackContext,
    };
    use intent_acp::mcp_server::request_context::McpRequestContext;
    use intent_core::caller::with_caller;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

    struct RegisteredFixture {
        f: Fixture,
        services: Arc<Services>,
        registry: Arc<RepositoryLifecycleRegistry>,
        physical: RepositoryPhysicalOwner,
        context: RepositoryCallbackContext,
        caller: Caller,
        optional: WorkspaceGitRoot,
        required: WorkspaceGitRoot,
    }

    fn row(f: &Fixture, id: &str, path: &std::path::Path) -> WorkspaceGitRoot {
        serde_json::from_value(serde_json::json!({
            "id":id,"workspaceId":f.workspace.id,"path":path,"source":"agent",
            "createdAt":"same-time","updatedAt":"same-time"
        }))
        .unwrap()
    }

    impl RegisteredFixture {
        async fn new() -> Self {
            let f = Fixture::new().await;
            let optional = row(&f, "optional-root", &f.dir.path().join("optional"));
            let required = row(&f, "required-root", &f.path);
            f.store.upsert_workspace_git_root(&optional).await.unwrap();
            f.store.upsert_workspace_git_root(&required).await.unwrap();
            let agent = intent_core::AgentId::new();
            let session:intent_core::AgentSession=serde_json::from_value(serde_json::json!({
                "id":agent,"workspaceId":f.workspace.id,"name":"registered source","status":"active",
                "createdAt":"same-time","updatedAt":"same-time"
            })).unwrap();
            f.store.insert_agent_session(&session).await.unwrap();
            let services = Arc::new(Services::new(f.store.clone()));
            let registry = services.repository_lifecycle_registry().await.unwrap();
            // Real Store one-use confirmation and physical owner; ACP completion
            // is explicitly scripted. No production NativeRead route is added.
            let physical = RepositoryCreationOwner::allocate(
                &registry,
                &f.store,
                f.workspace.id.clone(),
                agent.clone(),
                RepositoryCreationIntent::FirstSet,
            )
            .unwrap()
            .initialize(&f.store, || async {
                Ok("scripted original ACP completion".into())
            })
            .await
            .unwrap();
            let context = physical
                .callback()
                .with_read_owner(RepositoryReadOwner::capture(services.clone()));
            Self {
                f,
                services,
                registry,
                physical,
                context,
                caller: Caller::Agent { agent_id: agent },
                optional,
                required,
            }
        }

        async fn required_source(&self, registered: Option<&WorkspaceGitRoot>) {
            let mut parameters = input(&self.f);
            if let Some(root) = registered {
                parameters.facts.preparation.root.kind = RepositoryRootKind::Registered {
                    git_root_id: root.id.clone(),
                };
                parameters.context.roots[0].root = parameters.facts.preparation.root.clone();
            }
            with_captured_repository_source(
                &self.services,
                internal(self.caller.clone()).await,
                "required original source".into(),
                vec![NativeReviewStage::Commit],
                parameters,
                |admission| async move {
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await?;
                    Ok(())
                },
            )
            .await
            .unwrap();
            // Source cleanup closes its operation child. The original request
            // retains its actual root subscriptions for later consumption.
        }

        async fn prepare_optional(
            &self,
            read: &Arc<RepositoryReadRequest>,
        ) -> PreparedRepositoryOptional<intent_store::RepositorySelectionSnapshot> {
            let scope = read.capture_optional().unwrap();
            scope
                .run_optional(|metadata| async move {
                    metadata.subscribe_metadata(&[
                        RepositoryLifecycleKey::Database,
                        RepositoryLifecycleKey::RootInventory(self.f.workspace.id.clone()),
                    ])?;
                    let roots = self
                        .f
                        .store
                        .list_workspace_git_roots(&self.f.workspace.id)
                        .await
                        .map_err(|_| AdmissionError::Unavailable)?;
                    let selected = roots
                        .into_iter()
                        .find(|r| r.id == self.optional.id)
                        .ok_or(AdmissionError::Unavailable)?;
                    metadata.subscribe_metadata(&[
                        RepositoryLifecycleKey::Database,
                        RepositoryLifecycleKey::GitRoot(selected.id.clone()),
                        RepositoryLifecycleKey::Selection {
                            workspace_id: selected.workspace_id.clone(),
                            git_root_id: Some(selected.id.clone()),
                        },
                    ])?;
                    let root = intent_core::RepositoryRootId {
                        workspace_id: selected.workspace_id,
                        kind: RepositoryRootKind::Registered {
                            git_root_id: selected.id,
                        },
                    };
                    self.f
                        .store
                        .repository_selection_snapshot(&root)
                        .await
                        .map_err(|_| AdmissionError::Unavailable)
                })
                .unwrap()
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn precise_registered_optional_delete_omits_only_local_evidence() {
        let f = RegisteredFixture::new().await;
        let scope = McpRequestContext::capture(&f.context);
        let sibling = McpRequestContext::capture(&f.context);
        with_caller(
            f.caller.clone(),
            scope.scope(Box::pin(async {
                let read = current_read_request().unwrap();
                assert!(read.retains(f.services.as_ref()));
                f.required_source(None).await;
                f.required_source(Some(&f.required)).await;
                let required = read.child().unwrap();
                let ready = f.prepare_optional(&read).await;
                assert!(ready.value().binding().is_some());
                assert_eq!(
                    required.transfer_with_optional(Some(ready.metadata()), Ok),
                    Ok(true)
                );
                f.f.store
                    .delete_workspace_git_root(&f.optional.id)
                    .await
                    .unwrap();
                let calls = AtomicUsize::new(0);
                let output = required
                    .transfer_with_optional(Some(ready.metadata()), |include| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(if include {
                            "base plus original metadata"
                        } else {
                            "base"
                        })
                    })
                    .unwrap();
                assert_eq!(output, "base");
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert!(read.check_current().is_ok());
                f.f.store
                    .upsert_workspace_git_root(&f.optional)
                    .await
                    .unwrap();
                assert_eq!(
                    required.transfer_with_optional(Some(ready.metadata()), Ok),
                    Ok(false)
                );
            })),
        )
        .await;
        with_caller(
            f.caller.clone(),
            sibling.scope(Box::pin(async {
                f.required_source(None).await;
                f.required_source(Some(&f.required)).await;
                assert_eq!(
                    current_read_request()
                        .unwrap()
                        .child()
                        .unwrap()
                        .transfer(|| Ok(37)),
                    Ok(37)
                );
            })),
        )
        .await;
    }

    #[tokio::test]
    async fn precise_registered_required_member_refuses_the_whole_parent() {
        let f = RegisteredFixture::new().await;
        let scope = McpRequestContext::capture(&f.context);
        let sibling = McpRequestContext::capture(&f.context);
        with_caller(
            f.caller.clone(),
            scope.scope(Box::pin(async {
                let read = current_read_request().unwrap();
                f.required_source(None).await;
                f.required_source(Some(&f.required)).await;
                let required = read.child().unwrap();
                let ready = f.prepare_optional(&read).await;
                assert_eq!(
                    required.transfer_with_optional(Some(ready.metadata()), Ok),
                    Ok(true)
                );
                f.f.store
                    .delete_workspace_git_root(&f.required.id)
                    .await
                    .unwrap();
                let calls = AtomicUsize::new(0);
                assert_eq!(
                    required.transfer_with_optional(Some(ready.metadata()), |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }),
                    Err(AdmissionError::Retired)
                );
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                f.f.store
                    .upsert_workspace_git_root(&f.required)
                    .await
                    .unwrap();
                assert_eq!(read.check_current(), Err(AdmissionError::Retired));
            })),
        )
        .await;
        // A distinct already-captured primary request under the SAME physical
        // owner remains valid. It never repairs the retired required aggregate.
        with_caller(
            f.caller.clone(),
            sibling.scope(Box::pin(async {
                f.required_source(None).await;
                assert_eq!(
                    current_read_request()
                        .unwrap()
                        .child()
                        .unwrap()
                        .transfer(|| Ok(41)),
                    Ok(41)
                );
            })),
        )
        .await;
    }

    async fn wait_blocked(read: &Arc<RepositoryReadRequest>, key: RepositoryLifecycleKey) {
        tokio::time::timeout(BUDGET, async {
            loop {
                let optional = read.capture_optional().unwrap();
                match optional
                    .metadata()
                    .subscribe_metadata(&[RepositoryLifecycleKey::Database, key.clone()])
                {
                    Err(AdmissionError::Unavailable) => break,
                    Ok(()) => {}
                    Err(error) => panic!("unexpected probe outcome: {error:?}"),
                }
                drop(optional);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn held_mutation(required_root: bool) {
        let f = RegisteredFixture::new().await;
        let scope = McpRequestContext::capture(&f.context);
        let probe_scope = McpRequestContext::capture(&f.context);
        let mut other_workspace = f.f.workspace.clone();
        other_workspace.id = intent_core::WorkspaceId::new();
        f.f.store.insert_workspace(&other_workspace).await.unwrap();
        with_caller(
            f.caller.clone(),
            scope.scope(Box::pin(async {
                let read = current_read_request().unwrap();
                f.required_source(None).await;
                f.required_source(Some(&f.required)).await;
                let required = Arc::new(read.child().unwrap());
                let ready = f.prepare_optional(&read).await;
                let (entered, entry) = oneshot::channel();
                let (release, blocked) = std::sync::mpsc::sync_channel(1);
                let consumer = required.clone();
                let metadata = ready.metadata().clone();
                let caller = f.caller.clone();
                let original_scope = scope.clone();
                let runtime = tokio::runtime::Handle::current();
                let action = std::thread::spawn(move || {
                    let (result_tx, result_rx) = std::sync::mpsc::channel();
                    runtime.block_on(with_caller(
                        caller,
                        original_scope.scope(Box::pin(async move {
                            let result =
                                consumer.transfer_with_optional(Some(&metadata), |include| {
                                    assert!(include);
                                    entered.send(()).unwrap();
                                    blocked.recv_timeout(BUDGET).unwrap();
                                    Ok(include)
                                });
                            result_tx.send(result).unwrap();
                        })),
                    ));
                    result_rx.recv().unwrap()
                });
                tokio::time::timeout(BUDGET, entry).await.unwrap().unwrap();
                let changed = if required_root {
                    f.required.clone()
                } else {
                    f.optional.clone()
                };
                let store = f.f.store.clone();
                let id = changed.id.clone();
                let writer =
                    tokio::spawn(async move { store.delete_workspace_git_root(&id).await });
                let sentinel = RepositoryLifecycleKey::RootInventory(other_workspace.id.clone());
                let mut second_retire = None;
                let mut second_done = None;
                probe_scope
                    .scope(Box::pin(async {
                        let probe = current_read_request().unwrap();
                        wait_blocked(
                            &probe,
                            RepositoryLifecycleKey::RootInventory(f.f.workspace.id.clone()),
                        )
                        .await;
                        assert!(
                            f.f.store.get_workspace_git_root(&changed.id).await.is_ok(),
                            "no DML before the consuming action ends"
                        );
                        let registry = f.registry.clone();
                        let root_key = RepositoryLifecycleKey::GitRoot(changed.id.clone());
                        let sentinel_key = sentinel.clone();
                        let (done_tx, done_rx) = std::sync::mpsc::channel();
                        let second = std::thread::spawn(move || {
                            let ticket =
                                registry.begin_mutation(&[root_key, sentinel_key]).unwrap();
                            done_tx.send(()).unwrap();
                            ticket.settle_confirmed();
                        });
                        wait_blocked(&probe, sentinel).await;
                        assert!(
                            done_rx.try_recv().is_err(),
                            "every concurrent retirer joins the same held action"
                        );
                        assert!(!writer.is_finished());
                        // A real unrelated workspace read and unrelated registry owner
                        // progress while the original mutation waits outside map locks.
                        tokio::time::timeout(BUDGET, f.f.store.get_workspace(&other_workspace.id))
                            .await
                            .unwrap()
                            .unwrap();
                        f.registry
                            .begin_mutation(&[RepositoryLifecycleKey::GitRoot(
                                WorkspaceGitRootId::new(),
                            )])
                            .unwrap()
                            .settle_confirmed();
                        second_done = Some(done_rx);
                        second_retire = Some(second);
                    }))
                    .await;
                release.send(()).unwrap();
                assert_eq!(action.join().unwrap(), Ok(true));
                tokio::time::timeout(BUDGET, writer)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                second_done.unwrap().recv_timeout(BUDGET).unwrap();
                second_retire.unwrap().join().unwrap();
                let result = required.transfer_with_optional(Some(ready.metadata()), Ok);
                if required_root {
                    assert_eq!(result, Err(AdmissionError::Retired));
                } else {
                    assert_eq!(result, Ok(false));
                }
            })),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn precise_registered_optional_writer_and_concurrent_retire_join_consumption() {
        held_mutation(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn precise_registered_required_writer_and_concurrent_retire_join_consumption() {
        held_mutation(true).await;
    }

    #[tokio::test]
    async fn precise_registered_broad_retirement_still_closes_required_and_optional() {
        for mode in 0..6 {
            let f = RegisteredFixture::new().await;
            let scope = McpRequestContext::capture(&f.context);
            with_caller(
                f.caller.clone(),
                scope.scope(Box::pin(async {
                    let read = current_read_request().unwrap();
                    f.required_source(None).await;
                    let required = read.child().unwrap();
                    let ready = f.prepare_optional(&read).await;
                    match mode {
                        0 => f.physical.retirement().retire(),
                        1 => {
                            f.f.store
                                .update_workspace_with_branch(&f.f.workspace, Some("changed"))
                                .await
                                .unwrap();
                        }
                        2 => {
                            f.f.store.delete_workspace(&f.f.workspace.id).await.unwrap();
                        }
                        3 => {
                            let Caller::Agent { agent_id } = &f.caller else {
                                unreachable!()
                            };
                            f.f.store
                                .set_agent_session_model(
                                    &f.f.workspace.id,
                                    agent_id,
                                    "changed",
                                    None,
                                    "same-time",
                                )
                                .await
                                .unwrap();
                        }
                        4 => {
                            f.f.store
                                .begin_repository_pending_delete(&[
                                    RepositoryLifecycleKey::Workspace(f.f.workspace.id.clone()),
                                ])
                                .await
                                .unwrap()
                                .settle_confirmed();
                        }
                        _ => {
                            let _independent =
                                Store::open(&f.f.dir.path().join("store.db")).await.unwrap();
                        }
                    }
                    let calls = AtomicUsize::new(0);
                    assert_eq!(
                        required.transfer_with_optional(Some(ready.metadata()), |_| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }),
                        Err(AdmissionError::Retired),
                        "mode {mode}"
                    );
                    assert_eq!(
                        ready.metadata().check_current(),
                        Err(AdmissionError::Retired)
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                })),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn precise_registered_subscription_precedes_actual_worktree_wait() {
        let f = RegisteredFixture::new().await;
        let scope = McpRequestContext::capture(&f.context);
        let locks = f.services.worktree_locks.clone();
        let path = f.f.path.clone();
        let (entered, entry) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let blocker = tokio::spawn(async move {
            locks
                .with_lock(&path, || async {
                    entered.send(()).unwrap();
                    released.await.unwrap();
                })
                .await;
        });
        entry.await.unwrap();
        with_caller(f.caller.clone(),scope.scope(Box::pin(async {
            let reached=Arc::new(Notify::new());let mut parameters=input(&f.f);
            parameters.facts.preparation.root.kind=RepositoryRootKind::Registered{git_root_id:f.required.id.clone()};
            parameters.context.roots[0].root=parameters.facts.preparation.root.clone();parameters.before_lock=Some(reached.clone());
            let calls=AtomicUsize::new(0);
            let future=with_captured_repository_source(&f.services,internal(f.caller.clone()).await,"queued registered source".into(),vec![NativeReviewStage::Commit],parameters,|_|async{calls.fetch_add(1,Ordering::SeqCst);Ok(())});
            tokio::pin!(future);
            tokio::select! {()=reached.notified()=>{},_=&mut future=>panic!("did not reach the real lock wait")}
            f.f.store.delete_workspace_git_root(&f.required.id).await.unwrap();
            f.f.store.upsert_workspace_git_root(&f.required).await.unwrap();
            release.send(()).unwrap();blocker.await.unwrap();
            assert_eq!(future.await,Err(AdmissionError::Retired));
            assert_eq!(calls.load(Ordering::SeqCst),0);
        }))).await;
    }
}
