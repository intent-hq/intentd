use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    RepositoryRootKind, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource, WorkspaceId,
};
use intent_git::worktree::WorktreeLocks;
use tokio::sync::{oneshot, Notify};

use super::fixtures::{init, resolver, Fixture};
use super::git_source::{RepositoryGitSource, RootRecord};
use super::*;

#[tokio::test]
async fn operation_observation_keeps_exact_transports_head_ref_and_staging_without_credentials() {
    let owned = Fixture::new().await;
    let f = &owned;
    f.git(&f.path, &["tag", "main"]);
    f.git(
        &f.path,
        &["config", "remote.origin.url", "short:team/a.git"],
    );
    f.git(
        &f.path,
        &["config", "url.https://github.com/.insteadOf", "short:"],
    );
    f.git(
        &f.path,
        &[
            "config",
            "remote.origin.pushurl",
            "https://git.example:8443/gitlab/group/sub/app.git",
        ],
    );
    f.git(
        &f.path,
        &[
            "config",
            "--add",
            "remote.origin.pushurl",
            "ssh://git@unknown:2222/group/sub/app.git",
        ],
    );
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../intent-core/tests/fixtures/native_review_v1.json"
    ))
    .unwrap();
    let mut preparation: intent_core::NativeReviewPreparation =
        serde_json::from_value(fixture["prepare"]["reviewPreparation"].clone()).unwrap();
    preparation.root = f.root();
    preparation.scope = Fixture::input(f.root(), &f.path).scope;
    preparation.source.repository = Fixture::input(f.root(), &f.path).roots[0].targets[0]
        .target
        .clone();
    preparation.source.connection = None;
    preparation.target = preparation.source.clone();
    preparation.transport.as_mut().unwrap().remote_name = "origin".into();
    let original = RepositoryOperationFacts {
        preparation,
        worktree_path: f.path.clone(),
        git_dir: f.path.join(".git"),
        common_dir: f.path.join(".git"),
        source_ref: "refs/heads/main".into(),
        staging_fingerprint: Some("capture-request".into()),
        fetch_destinations: Vec::new(),
        push_destinations: Vec::new(),
        credential_requests: Vec::new(),
    };
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    RepositoryGitSource::with_locked(
        &f.store,
        &WorktreeLocks::new(),
        record,
        RepositoryRetirement::default(),
        |source| async move {
            let initial = source
                .observe_operation(
                    &original,
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            assert_eq!(
                initial.fetch_destinations,
                ["https://github.com/team/a.git"]
            );
            assert_eq!(
                initial.push_destinations,
                [
                    "https://git.example:8443/gitlab/group/sub/app.git",
                    "ssh://git@unknown:2222/group/sub/app.git"
                ]
            );
            assert_eq!(initial.source_ref, "refs/heads/main");
            assert_eq!(initial.preparation.source.branch, "main");
            assert!(initial.preparation.local_head_sha.is_some());
            assert!(initial.credential_requests.is_empty());
            std::fs::write(f.path.join("staged"), "change").unwrap();
            f.git(&f.path, &["add", "staged"]);
            let staged = source
                .observe_operation(
                    &initial,
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            assert_ne!(initial.staging_fingerprint, staged.staging_fingerprint);
            assert_eq!(
                initial.preparation.local_head_sha,
                staged.preparation.local_head_sha
            );
            f.git(&f.path, &["checkout", "-b", "next"]);
            let moved = source
                .observe_operation(
                    &staged,
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            assert_eq!(moved.source_ref, "refs/heads/next");
            f.git(&f.path, &["remote", "rename", "origin", "renamed"]);
            assert_eq!(
                source
                    .observe_operation(
                        &moved,
                        Fixture::input(f.root(), &f.path),
                        resolver(),
                        f.environment()
                    )
                    .await
                    .err(),
                Some(AdmissionError::BindingChanged)
            );
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_store_root_and_reader_preserve_local_identity_without_forge_login() {
    let owned = Fixture::new().await;
    let f = &owned;
    f.git(
        &f.path,
        &["remote", "add", "origin", "https://github.com/team/a.git"],
    );
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    let retirement = RepositoryRetirement::default();
    let escaped = RepositoryGitSource::with_locked(
        &f.store,
        &WorktreeLocks::new(),
        record,
        retirement.clone(),
        |source| async move {
            let output = source
                .read_context(
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            assert_eq!(output.context.roots[0].branch.as_deref(), Some("main"));
            assert!(output.context.roots[0].head_sha.is_some());
            assert!(output.context.roots[0].targets[0].connection.is_none());
            assert_eq!(output.context.scope.daemon_id, "host-A");
            Ok(source)
        },
    )
    .await
    .unwrap();
    assert_eq!(retirement.check_current(), Err(AdmissionError::Retired));
    assert_eq!(
        escaped
            .read_context(
                Fixture::input(f.root(), &f.path),
                resolver(),
                f.environment()
            )
            .await
            .err(),
        Some(AdmissionError::Retired)
    );
}

#[tokio::test]
async fn queued_path_replacement_is_rejected_after_the_actual_lock_wait() {
    let owned = Fixture::new().await;
    let f = &owned;
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    let locks = WorktreeLocks::new();
    let entered = Arc::new(Notify::new());
    let (release, released) = oneshot::channel();
    let holder = tokio::spawn({
        let locks = locks.clone();
        let path = f.path.clone();
        let entered = entered.clone();
        async move {
            locks
                .with_lock(&path, || async {
                    entered.notify_one();
                    released.await.unwrap();
                })
                .await;
        }
    });
    entered.notified().await;
    let store = f.store.clone();
    let queued = tokio::spawn(async move {
        RepositoryGitSource::with_locked(
            &store,
            &locks,
            record,
            RepositoryRetirement::default(),
            |_| async { Ok(()) },
        )
        .await
    });
    let other = f.dir.path().join("other");
    init(&other);
    let mut workspace = f.workspace.clone();
    workspace.worktree_path = Some(other.to_str().unwrap().into());
    f.store.update_workspace(&workspace).await.unwrap();
    release.send(()).unwrap();
    holder.await.unwrap();
    assert_eq!(queued.await.unwrap(), Err(AdmissionError::BindingChanged));
}

#[tokio::test]
async fn actual_git_edits_are_refreshed_and_store_changes_stop_the_same_session() {
    let owned = Fixture::new().await;
    let f = &owned;
    f.git(
        &f.path,
        &[
            "config",
            "remote.origin.url",
            "https://github.com/team/a.git",
        ],
    );
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    RepositoryGitSource::with_locked(
        &f.store,
        &WorktreeLocks::new(),
        record,
        RepositoryRetirement::default(),
        |source| async move {
            let before = source
                .read_context(
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            f.git(
                &f.path,
                &[
                    "config",
                    "remote.origin.url",
                    "https://github.com/team/b.git",
                ],
            );
            f.git(&f.path, &["checkout", "-b", "changed"]);
            let after = source
                .read_context(
                    Fixture::input(f.root(), &f.path),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            assert_ne!(
                before.change_inputs[0].fingerprint,
                after.change_inputs[0].fingerprint
            );
            assert_eq!(after.context.roots[0].branch.as_deref(), Some("changed"));
            assert_eq!(
                after.context.roots[0].targets[0].target.project_path,
                "team/b"
            );
            f.store
                .update_workspace_with_branch(&f.workspace, Some("changed"))
                .await
                .unwrap();
            assert_eq!(
                source
                    .read_context(
                        Fixture::input(f.root(), &f.path),
                        resolver(),
                        f.environment()
                    )
                    .await
                    .err(),
                Some(AdmissionError::BindingChanged)
            );
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn linked_worktree_uses_the_actual_worktree_and_common_git_dirs() {
    let owned = Fixture::new().await;
    let f = &owned;
    let linked = f.dir.path().join("linked");
    f.git(
        &f.path,
        &["worktree", "add", "-b", "linked", linked.to_str().unwrap()],
    );
    let mut workspace = f.workspace.clone();
    workspace.worktree_path = Some(linked.to_str().unwrap().into());
    f.store
        .update_workspace_with_branch(&workspace, Some("linked"))
        .await
        .unwrap();
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    RepositoryGitSource::with_locked(
        &f.store,
        &WorktreeLocks::new(),
        record,
        RepositoryRetirement::default(),
        |source| async move {
            let output = source
                .read_context(
                    Fixture::input(f.root(), &linked),
                    resolver(),
                    f.environment(),
                )
                .await
                .unwrap();
            let changes = &output.change_inputs[0];
            assert_ne!(changes.git_dir, changes.common_dir);
            assert_eq!(changes.common_dir, f.path.join(".git"));
            assert_eq!(output.context.roots[0].branch.as_deref(), Some("linked"));
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn registered_root_cannot_be_substituted_by_a_foreign_workspace_or_primary() {
    let owned = Fixture::new().await;
    let f = &owned;
    let path = f.dir.path().join("registered");
    init(&path);
    let registered = WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: f.workspace.id.clone(),
        path: path.to_str().unwrap().into(),
        source: WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: Vec::new(),
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: "2026-09-27T00:00:00Z".into(),
        updated_at: "2026-09-27T00:00:00Z".into(),
    };
    f.store
        .upsert_workspace_git_root(&registered)
        .await
        .unwrap();
    let mut root = f.root();
    root.kind = RepositoryRootKind::Registered {
        git_root_id: registered.id.clone(),
    };
    let record = RootRecord::read(&f.store, &root).await.unwrap();
    RepositoryGitSource::with_locked(
        &f.store,
        &WorktreeLocks::new(),
        record,
        RepositoryRetirement::default(),
        |source| {
            let root = root.clone();
            let path = path.clone();
            let registered_id = registered.id.clone();
            async move {
                assert!(source
                    .read_context(
                        Fixture::input(root.clone(), &path),
                        resolver(),
                        f.environment()
                    )
                    .await
                    .is_ok());
                assert_eq!(
                    source
                        .read_context(
                            Fixture::input(root.clone(), &f.path),
                            resolver(),
                            f.environment()
                        )
                        .await
                        .err(),
                    Some(AdmissionError::BindingChanged)
                );
                f.store
                    .delete_workspace_git_root(&registered_id)
                    .await
                    .unwrap();
                assert_eq!(
                    source
                        .read_context(
                            Fixture::input(root.clone(), &path),
                            resolver(),
                            f.environment()
                        )
                        .await
                        .err(),
                    Some(AdmissionError::Denied)
                );
                Ok(())
            }
        },
    )
    .await
    .unwrap();
    let mut other = f.workspace.clone();
    other.id = WorkspaceId::new();
    f.store.insert_workspace(&other).await.unwrap();
    f.store
        .upsert_workspace_git_root(&registered)
        .await
        .unwrap();
    root.workspace_id = other.id;
    assert_eq!(
        RootRecord::read(&f.store, &root).await.err(),
        Some(AdmissionError::Denied)
    );
}

#[tokio::test]
async fn cancellation_retires_escaped_sources_before_releasing_the_worktree_lock() {
    let owned = Fixture::new().await;
    let f = &owned;
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    let locks = WorktreeLocks::new();
    let retirement = RepositoryRetirement::default();
    let (sent, received) = oneshot::channel();
    let task = tokio::spawn({
        let store = f.store.clone();
        let locks = locks.clone();
        let retirement = retirement.clone();
        async move {
            RepositoryGitSource::with_locked(
                &store,
                &locks,
                record,
                retirement,
                |source| async move {
                    sent.send(source).ok().unwrap();
                    std::future::pending::<AdmissionResult<()>>().await
                },
            )
            .await
        }
    });
    let escaped = received.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(retirement.check_current(), Err(AdmissionError::Retired));
    tokio::time::timeout(
        Duration::from_secs(2),
        locks.with_lock(&f.path, || async {
            assert_eq!(
                escaped
                    .read_context(
                        Fixture::input(f.root(), &f.path),
                        resolver(),
                        f.environment()
                    )
                    .await
                    .err(),
                Some(AdmissionError::Retired)
            );
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn errors_release_the_lock_and_retire_the_session() {
    let owned = Fixture::new().await;
    let f = &owned;
    let record = RootRecord::read(&f.store, &f.root()).await.unwrap();
    let locks = WorktreeLocks::new();
    let retirement = RepositoryRetirement::default();
    let result =
        RepositoryGitSource::with_locked(&f.store, &locks, record, retirement.clone(), |_| async {
            Err::<(), _>(AdmissionError::Unavailable)
        })
        .await;
    assert_eq!(result, Err(AdmissionError::Unavailable));
    assert_eq!(retirement.check_current(), Err(AdmissionError::Retired));
    tokio::time::timeout(
        Duration::from_secs(2),
        locks.with_lock(&f.path, || async {}),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_unavailable_and_empty_worktree_records_never_fall_back_to_metadata_paths() {
    let owned = Fixture::new().await;
    let f = &owned;
    let mut workspace = f.workspace.clone();
    workspace.is_remote = true;
    f.store.update_workspace(&workspace).await.unwrap();
    assert_eq!(
        RootRecord::read(&f.store, &f.root()).await.err(),
        Some(AdmissionError::Denied)
    );
    workspace.is_remote = false;
    workspace.worktree_path = Some(String::new());
    f.store.update_workspace(&workspace).await.unwrap();
    assert_eq!(
        RootRecord::read(&f.store, &f.root()).await.err(),
        Some(AdmissionError::Unavailable)
    );
    f.store.close().await;
    assert_eq!(
        RootRecord::read(&f.store, &f.root()).await.err(),
        Some(AdmissionError::Unavailable)
    );
}
