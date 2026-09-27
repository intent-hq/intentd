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
