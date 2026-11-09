//! `workspace.retire` (PROTOCOL §5.1): the Retired lifecycle state at the
//! service layer. Retire deletes the checkout folder, keeps every DB row and
//! the git branch, hides the workspace from every list unless the caller
//! opts in with `includeRetired`, and refuses disk-backed mutations with
//! `workspace-retired`.

use std::time::Duration;

use intent_core::{Error, WorkspaceApi, WorkspaceId, WorkspaceListFilter, WorkspaceStatus};

use crate::test_support::test_tempdir;
use crate::Services;

use crate::test_support::retire_workspace::{
    create_workspace, days_from_now, fixture, flip_retired, Fixture,
};

async fn wait_gone(path: &std::path::Path) {
    for _ in 0..100 {
        if !path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{} still exists", path.display());
}

fn ids(list: &[intent_core::Workspace]) -> Vec<String> {
    let mut v: Vec<String> = list.iter().map(|w| w.id.0.clone()).collect();
    v.sort();
    v
}

/// Retiring an archived workspace deletes its checkout folder and git
/// worktree registration, keeps the git branch and every row (notes, agent
/// sessions), and stamps `Retired` + `retiredAt` while staying archived.
#[intent_test_macros::daemon_test]
async fn retire_archived_workspace_removes_folder_and_keeps_rows_and_branch() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    let notes_before = f.store.list_notes(&ws.id).await.unwrap().len();
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let retired = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert_eq!(retired.status, WorkspaceStatus::Retired);
    assert!(retired.retired_at.is_some());
    assert!(retired.archived);
    assert!(retired.archived_at.is_some());

    assert!(
        !wt.exists(),
        "checkout is gone from its path when retire returns"
    );
    wait_gone(&f.root.join(ws.id.as_str())).await;

    let repo = git2::Repository::open(&f.repo).unwrap();
    assert!(
        repo.find_branch(&ws.branch, git2::BranchType::Local)
            .is_ok(),
        "the workspace branch is kept"
    );
    let names = repo.worktrees().unwrap();
    let mut registered = Vec::new();
    for i in 0..names.len() {
        if let Ok(Some(name)) = names.get(i) {
            registered.push(name.to_string());
        }
    }
    assert!(
        registered.iter().all(|name| {
            repo.find_worktree(name)
                .map_or(true, |w| w.path() != wt.as_path())
        }),
        "worktree registration pruned: {registered:?}"
    );

    assert_eq!(
        f.store.list_notes(&ws.id).await.unwrap().len(),
        notes_before,
        "notes kept"
    );
    assert_eq!(
        f.store
            .list_agent_session_summaries(&ws.id)
            .await
            .unwrap()
            .len(),
        1,
        "agent sessions kept"
    );
    let row = f
        .svc
        .get_workspace(ws.id.clone())
        .await
        .expect("still readable");
    assert_eq!(row.status, WorkspaceStatus::Retired);

    // Idempotent: retiring again returns the retired row.
    let again = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire again");
    assert_eq!(again.retired_at, retired.retired_at);
}

/// Retiring an Active workspace runs the archive teardown first (its
/// archived delta), then publishes the retired delta, and deletes the folder.
#[intent_test_macros::daemon_test]
async fn retire_active_workspace_archives_first_then_retires() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    let mut sub = f.bus.subscribe(crate::SubscriptionFilter {
        event_types: vec!["workspace:updated".to_string()],
        workspace_id: Some(ws.id.0.clone()),
        ..Default::default()
    });

    let retired = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert_eq!(retired.status, WorkspaceStatus::Retired);

    let mut changes = Vec::new();
    while changes.len() < 2 {
        let batch = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(2)),
            sub.recv(),
        )
        .await
        .expect("event delivered")
        .expect("subscription open");
        for ev in batch {
            changes.push(serde_json::to_value(&ev).unwrap()["data"]["changes"].clone());
        }
    }
    assert_eq!(changes[0]["status"], "Archived", "{changes:?}");
    assert_eq!(changes[1]["status"], "Retired", "{changes:?}");
    assert_eq!(
        changes[1]["retiredAt"],
        serde_json::json!(retired.retired_at)
    );
    assert_eq!(changes[1]["archived"], true);
    assert!(!wt.exists());
}

/// Every service list hides retired workspaces unless `include_retired` is
/// set — `include_archived` alone never includes them.
#[intent_test_macros::daemon_test]
async fn service_lists_hide_retired_unless_opted_in() {
    let f = fixture().await;
    let live = create_workspace(&f).await;
    let archived = create_workspace(&f).await;
    let retired = create_workspace(&f).await;
    f.svc
        .archive_workspace(archived.id.clone(), None)
        .await
        .unwrap();
    f.svc
        .retire_workspace(retired.id.clone(), None)
        .await
        .unwrap();

    let mut live_and_archived = vec![live.id.0.clone(), archived.id.0.clone()];
    live_and_archived.sort();
    let mut all = vec![
        live.id.0.clone(),
        archived.id.0.clone(),
        retired.id.0.clone(),
    ];
    all.sort();

    for lite in [false, true] {
        let active = list(&f.svc, lite, WorkspaceListFilter::ACTIVE).await;
        assert_eq!(ids(&active), vec![live.id.0.clone()], "lite={lite}");
        let with_archived = list(&f.svc, lite, WorkspaceListFilter::WITH_ARCHIVED).await;
        assert_eq!(
            ids(&with_archived),
            live_and_archived,
            "lite={lite}: includeArchived alone must not include retired"
        );
        let opted_in = list(&f.svc, lite, WorkspaceListFilter::ALL).await;
        assert_eq!(ids(&opted_in), all, "lite={lite}");
        let row = opted_in.iter().find(|w| w.id == retired.id).unwrap();
        assert_eq!(row.status, WorkspaceStatus::Retired);
        assert!(row.retired_at.is_some());
    }
}

async fn list(
    svc: &Services,
    lite: bool,
    filter: WorkspaceListFilter,
) -> Vec<intent_core::Workspace> {
    if lite {
        svc.list_workspaces_lite(filter).await.unwrap()
    } else {
        svc.list_workspaces(filter).await.unwrap()
    }
}

/// The virtual Chief workspace cannot be retired.
#[intent_test_macros::daemon_test]
async fn retire_refuses_chief() {
    let f = fixture().await;
    let err = f
        .svc
        .retire_workspace(WorkspaceId::chief(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
}

/// Retire only admits daemon-owned checkouts: a `cow` / `direct` row whose
/// repository is not its own folder (`repositoryPath != worktreePath`),
/// `skipIsolation`, remote and checkout-less workspaces are refused with
/// `retire-unsupported` and left untouched.
#[intent_test_macros::daemon_test]
async fn retire_refuses_non_worktree_checkouts() {
    let f = fixture().await;
    let base = |name: &str| {
        let mut ws = super::workspace(&WorkspaceId::from(format!("ws-{name}")));
        ws.repository_path = Some(f.repo.to_string_lossy().to_string());
        ws.worktree_path = Some(
            f.root
                .join(ws.id.as_str())
                .join("repo")
                .to_string_lossy()
                .to_string(),
        );
        ws.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
        ws
    };
    let mut cow = base("cow");
    cow.checkout_mode = Some(intent_core::CheckoutMode::Cow);
    let mut direct = base("direct");
    direct.checkout_mode = Some(intent_core::CheckoutMode::Direct);
    let mut skip = base("skip");
    skip.skip_worktree = true;
    let mut remote = base("remote");
    remote.is_remote = true;
    let mut bare = base("bare");
    bare.worktree_path = None;
    // A worktree-mode row whose persisted path the daemon cannot prove it
    // created (outside every workspaces root): never deleted.
    let mut foreign = base("foreign");
    foreign.worktree_path = Some(
        f.repo
            .join("ws-foreign")
            .join("repo")
            .to_string_lossy()
            .to_string(),
    );
    let cases = [
        (cow, Some(intent_core::CheckoutMode::Cow)),
        (direct, Some(intent_core::CheckoutMode::Direct)),
        (skip, None),
        (remote, None),
        (bare, None),
        (foreign, Some(intent_core::CheckoutMode::Worktree)),
    ];
    for (ws, mode) in cases {
        f.store.insert_workspace(&ws).await.expect("insert");
        let err = f
            .svc
            .retire_workspace(ws.id.clone(), None)
            .await
            .unwrap_err();
        match &err {
            Error::RetireUnsupported { checkout_mode, .. } => {
                assert_eq!(*checkout_mode, mode, "{}", ws.id.0);
            }
            other => panic!("{}: expected RetireUnsupported, got {other:?}", ws.id.0),
        }
        let row = f.store.get_workspace(&ws.id).await.unwrap();
        assert_eq!(row.status, WorkspaceStatus::Active, "{} untouched", ws.id.0);
        assert!(!row.archived, "{} not archived", ws.id.0);
    }
}

/// A daemon-root path that exists but is not a linked worktree registered
/// in the source repository is never deleted: retire is refused and the
/// folder survives untouched.
#[intent_test_macros::daemon_test]
async fn retire_refuses_unregistered_folder_and_keeps_it() {
    let f = fixture().await;
    let id = WorkspaceId::from("ws-unregistered");
    let folder = f.root.join(id.as_str()).join("repo");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("keep.txt"), "user data").unwrap();
    let mut ws = super::workspace(&id);
    ws.repository_path = Some(f.repo.to_string_lossy().to_string());
    ws.worktree_path = Some(folder.to_string_lossy().to_string());
    ws.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
    f.store.insert_workspace(&ws).await.expect("insert");

    let err = f.svc.retire_workspace(id.clone(), None).await.unwrap_err();
    assert!(matches!(err, Error::RetireUnsupported { .. }), "{err:?}");
    assert!(folder.join("keep.txt").is_file(), "folder untouched");
    let row = f.store.get_workspace(&id).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Active);
}

// ---------------------------------------------------------------------------
// Revive (PROTOCOL §5.1 "Revive"): a retired workspace comes back as a fresh
// blank checkout of the default branch on a NEW branch, at the same path.
// ---------------------------------------------------------------------------

pub(crate) fn subscribe_updates(f: &Fixture, ws: &WorkspaceId) -> crate::Subscription {
    f.bus.subscribe(crate::SubscriptionFilter {
        event_types: vec!["workspace:updated".to_string()],
        workspace_id: Some(ws.0.clone()),
        ..Default::default()
    })
}

/// Collect the `changes` of every `workspace:updated` delta that arrives
/// within a short quiet window.
pub(crate) async fn drain_changes(sub: &mut crate::Subscription) -> Vec<serde_json::Value> {
    let mut changes = Vec::new();
    while let Ok(Some(batch)) = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(500)),
        sub.recv(),
    )
    .await
    {
        for ev in batch {
            changes.push(serde_json::to_value(&ev).unwrap()["data"]["changes"].clone());
        }
    }
    changes
}

fn head_branch(worktree: &std::path::Path) -> String {
    let repo = git2::Repository::open(worktree).expect("open revived checkout");
    let head = repo.head().expect("head");
    head.shorthand().expect("branch name").to_string()
}

/// Retire `ws` after leaving an untracked file in its checkout, so a revive
/// can prove nothing on disk came back.
async fn retire_with_scratch(f: &Fixture, ws: &intent_core::Workspace) -> std::path::PathBuf {
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    std::fs::write(wt.join("scratch.txt"), "uncommitted work").unwrap();
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert!(!wt.exists());
    wt
}

/// `workspace.unarchive` on a retired workspace is the manual revive: a fresh
/// blank checkout at the same path on a new branch cut from the default
/// branch (the old branch is kept), the row flips back to Active with
/// `archivedAt` / `retiredAt` cleared, and ONE delta carries
/// `revived: { reason: "manual" }` plus the new `branch` — never
/// `autoUnarchive`. Rows are kept and no agent transcript notice is written.
#[intent_test_macros::daemon_test]
async fn manual_unarchive_revives_retired_workspace_as_fresh_checkout() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let notes_before = f.store.list_notes(&ws.id).await.unwrap().len();
    let wt = retire_with_scratch(&f, &ws).await;
    let mut sub = subscribe_updates(&f, &ws.id);

    let revived = f
        .svc
        .unarchive_workspace(ws.id.clone())
        .await
        .expect("unarchive revives");
    assert_eq!(revived.status, WorkspaceStatus::Active);
    assert!(!revived.archived);
    assert!(revived.archived_at.is_none());
    assert!(revived.retired_at.is_none());
    assert_ne!(revived.branch, ws.branch, "revive uses a new branch");
    assert_eq!(
        revived.worktree_path, ws.worktree_path,
        "revive provisions at the same path"
    );

    assert!(
        wt.join("README.md").is_file(),
        "fresh checkout of the default branch"
    );
    assert!(
        !wt.join("scratch.txt").exists(),
        "nothing from the old folder comes back"
    );
    assert_eq!(head_branch(&wt), revived.branch);
    let repo = git2::Repository::open(&f.repo).unwrap();
    assert!(
        repo.find_branch(&ws.branch, git2::BranchType::Local)
            .is_ok(),
        "the old branch is left untouched"
    );
    assert!(
        intent_git::worktree::is_registered_worktree(&f.repo, &wt),
        "the new checkout is a registered linked worktree"
    );

    let changes = drain_changes(&mut sub).await;
    assert_eq!(changes.len(), 1, "exactly one delta: {changes:?}");
    assert_eq!(
        changes[0],
        serde_json::json!({
            "archived": false,
            "status": "Active",
            "archivedAt": null,
            "retiredAt": null,
            "branch": revived.branch,
            "baseCommitSha": revived.base_commit_sha,
            "revived": { "reason": "manual" },
            "statusMessage": null,
        })
    );
    assert!(
        revived.status_message.is_none(),
        "the unchanged purge status message is cleared on revive"
    );

    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Active);
    assert_eq!(row.branch, revived.branch);
    assert!(row.retired_at.is_none());
    assert_eq!(
        f.store.list_notes(&ws.id).await.unwrap().len(),
        notes_before,
        "notes kept"
    );
    let agent = intent_core::AgentId::from(format!("agent-{}", ws.id.0));
    assert!(
        f.store
            .get_agent_messages(&agent, None)
            .await
            .unwrap()
            .is_empty(),
        "a manual revive writes no agent transcript notice"
    );

    // Writable again: the retired read-only gate no longer applies.
    f.svc
        .file_write(ws.id.clone(), "new.txt".into(), "x".into(), None)
        .await
        .expect("revived workspace accepts writes");
}

/// `workspace.restore` is the same manual revive.
#[intent_test_macros::daemon_test]
async fn restore_revives_retired_workspace() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = retire_with_scratch(&f, &ws).await;
    let revived = f
        .svc
        .restore_workspace(ws.id.clone())
        .await
        .expect("restore revives");
    assert_eq!(revived.status, WorkspaceStatus::Active);
    assert!(revived.retired_at.is_none());
    assert!(wt.join("README.md").is_file());
    assert!(!wt.join("scratch.txt").exists());
}

/// A revive racing the retire's background folder sweep must keep the new
/// checkout: the sweep of the old `<root>/<id>/` directory never deletes a
/// folder provisioned by a revive.
#[intent_test_macros::daemon_test]
async fn revive_right_after_retire_survives_the_folder_sweep() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    f.svc
        .unarchive_workspace(ws.id.clone())
        .await
        .expect("revive");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        wt.join("README.md").is_file(),
        "the revived checkout survives the retire sweep"
    );
}

/// The turn-start trigger revives a retired workspace and reports it as a
/// revive (not a plain unarchive), with a delta stamped
/// `revived: { reason: "agent_activity", agentId, agentName }`.
#[intent_test_macros::daemon_test]
async fn turn_start_revives_retired_workspace() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = retire_with_scratch(&f, &ws).await;
    let agent = intent_core::AgentId::from(format!("agent-{}", ws.id.0));
    let mut sub = subscribe_updates(&f, &ws.id);

    let outcome = f.svc.auto_unarchive_on_turn_start(&ws.id, &agent).await;
    let Some(crate::UnarchiveOutcome::Revived {
        previous_branch,
        branch,
    }) = outcome
    else {
        panic!("expected a revive, got {outcome:?}");
    };
    assert_eq!(previous_branch, ws.branch);
    assert_ne!(branch, ws.branch);
    assert!(wt.join("README.md").is_file());
    assert!(!wt.join("scratch.txt").exists());

    let changes = drain_changes(&mut sub).await;
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(
        changes[0]["revived"],
        serde_json::json!({
            "reason": "agent_activity",
            "agentId": agent.0,
            "agentName": format!("test-{}", agent.0),
        })
    );
    assert!(changes[0].get("autoUnarchive").is_none(), "{changes:?}");
    assert_eq!(changes[0]["branch"], serde_json::json!(branch));
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Active);

    // A second turn start is a no-op: the workspace is Active now.
    assert!(f
        .svc
        .auto_unarchive_on_turn_start(&ws.id, &agent)
        .await
        .is_none());
}

async fn begin_upload(f: &Fixture, ws: &WorkspaceId, payload: &[u8]) -> String {
    use base64::Engine as _;
    let r = f
        .svc
        .file_attachment_upload_begin(
            ws.clone(),
            "report.bin".to_string(),
            payload.len() as u64,
            crate::attachment_upload::sha256_hex(payload),
            None,
            None,
        )
        .await
        .expect("begin");
    let upload = r["uploadId"].as_str().expect("uploadId").to_string();
    f.svc
        .file_attachment_upload_chunk(
            upload.clone(),
            0,
            base64::engine::general_purpose::STANDARD.encode(payload),
        )
        .await
        .expect("chunk before retire");
    upload
}

/// An upload begun before the workspace became retired cannot keep writing:
/// `chunk` and `commit` resolve the upload's workspace and are refused with
/// `workspace-retired` (the RPC gate cannot see an `uploadId`-only call).
#[intent_test_macros::daemon_test]
async fn upload_continuations_refused_once_workspace_is_retired() {
    use base64::Engine as _;
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let upload = begin_upload(&f, &ws.id, b"payload").await;
    // The row flip alone (the window before retire's upload sweep).
    f.store
        .archive_workspace_detaching_guests(&ws.id, &intent_core::now_iso())
        .await
        .unwrap();
    assert!(f
        .store
        .retire_workspace_if_archived(&ws.id, &intent_core::now_iso())
        .await
        .unwrap());
    let is_retired = |e: &Error| matches!(e, Error::WorkspaceRetired { workspace_id } if *workspace_id == ws.id.0);

    let err = f
        .svc
        .file_attachment_upload_chunk(
            upload.clone(),
            0,
            base64::engine::general_purpose::STANDARD.encode(b"payload"),
        )
        .await
        .unwrap_err();
    assert!(is_retired(&err), "chunk: {err:?}");
    let err = f
        .svc
        .file_attachment_upload_commit(upload.clone())
        .await
        .unwrap_err();
    assert!(is_retired(&err), "commit: {err:?}");
    assert!(
        f.store.list_attachments(&ws.id).await.unwrap().is_empty(),
        "nothing placed"
    );
}

/// `workspace.retire` aborts the workspace's in-flight uploads: their
/// sessions are dropped and staging deleted, so a late chunk / commit has
/// nothing to write into.
#[intent_test_macros::daemon_test]
async fn retire_aborts_in_flight_uploads() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let upload = begin_upload(&f, &ws.id, b"payload").await;
    let staging = f
        .svc
        .attachment_uploads
        .lock()
        .unwrap()
        .get(&upload)
        .map(|s| s.staging_dir.clone())
        .expect("session");
    assert!(staging.is_dir());
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert!(
        !f.svc
            .attachment_uploads
            .lock()
            .unwrap()
            .contains_key(&upload),
        "session dropped on retire"
    );
    assert!(!staging.exists(), "staging deleted on retire");
    assert!(f.svc.file_attachment_upload_commit(upload).await.is_err());
}

/// A revive racing an in-flight retire waits for the whole retire tail
/// (teardown and folder sweep run under the archive fence): whichever order
/// the two land in, an Active result always has a live checkout, and a
/// Retired result has none.
#[intent_test_macros::daemon_test]
async fn retire_and_revive_racing_leave_a_consistent_workspace() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    // Both sides run concurrently on this task (it carries the bound
    // caller the retire capability gate requires).
    let retire = f.svc.retire_workspace(ws.id.clone(), None);
    // Revive as soon as the row reads Retired (inside the retire tail).
    let revive = async {
        loop {
            if f.store.get_workspace(&ws.id).await.unwrap().is_retired() {
                return Some(f.svc.unarchive_workspace(ws.id.clone()).await);
            }
            tokio::task::yield_now().await;
        }
    };
    let (retired, revived) = tokio::join!(retire, revive);
    retired.expect("retire");
    let revived = revived.expect("observed the retired row").expect("revive");
    assert_eq!(revived.status, WorkspaceStatus::Active);
    // Let any background sweep settle, then the revived folder must stand.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Active);
    assert!(wt.join("README.md").is_file(), "revived checkout survives");
    assert_eq!(head_branch(&wt), row.branch);
}

// ---------------------------------------------------------------------------
// Automatic purge (PROTOCOL §5.1 "Automatic purge", §5.12
// `workspace.purgeArchivedAfterDays`, formerly
// `workspace.autoRetireArchivedAfterDays`): a periodic sweep retires
// workspaces archived longer than the configured number of days.
// ---------------------------------------------------------------------------

/// Off by default: with no `workspace.purgeArchivedAfterDays`, the
/// sweep retires nothing, however long a workspace has been archived.
#[intent_test_macros::daemon_test]
async fn auto_retire_is_off_by_default() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let report = f.svc.sweep_auto_retire_at(days_from_now(3650)).await;
    assert_eq!(report, crate::AutoRetireReport::default());
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert!(!row.is_retired());
    assert!(row.archived);
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
}

/// With 7 days: a workspace archived 8 days ago is retired (folder removed,
/// one retire delta), one archived 6 days ago is not, and an Active
/// workspace is never touched. A second sweep is a no-op (idempotent).
#[intent_test_macros::daemon_test]
async fn auto_retire_retires_only_workspaces_archived_past_the_threshold() {
    let f = fixture().await;
    let old = create_workspace(&f).await;
    let active = create_workspace(&f).await;
    f.svc
        .archive_workspace(old.id.clone(), None)
        .await
        .expect("archive");

    // Archived "6 days ago": not yet due.
    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(6))
        .await;
    assert_eq!(report.retired, 0, "{report:?}");
    assert!(!f.store.get_workspace(&old.id).await.unwrap().is_retired());

    // Archived "8 days ago": retired through the workspace.retire path.
    let mut sub = subscribe_updates(&f, &old.id);
    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.retired, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    let row = f.store.get_workspace(&old.id).await.unwrap();
    assert!(row.is_retired());
    assert!(row.retired_at.is_some());
    wait_gone(std::path::Path::new(old.worktree_path.as_deref().unwrap())).await;
    let changes = drain_changes(&mut sub).await;
    assert!(
        changes
            .iter()
            .any(|c| c["status"] == "Retired" && c["archived"] == true),
        "retire delta published: {changes:?}"
    );

    let untouched = f.store.get_workspace(&active.id).await.unwrap();
    assert_eq!(untouched.status, WorkspaceStatus::Active);
    assert!(!untouched.archived);
    assert!(std::path::Path::new(active.worktree_path.as_deref().unwrap()).is_dir());

    // Idempotent: the retired workspace is not swept again.
    let again = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(30))
        .await;
    assert_eq!(again, crate::AutoRetireReport::default());
}

/// Fail-soft per workspace: a due workspace whose retire is refused (a
/// folder that is not a registered worktree) is counted as skipped — not a
/// failure, so it does not warn every sweep — and left intact, and the sweep
/// still retires the other due workspace.
#[intent_test_macros::daemon_test]
async fn auto_retire_is_fail_soft_per_workspace() {
    let f = fixture().await;
    let id = WorkspaceId::from("ws-auto-unregistered");
    let folder = f.root.join(id.as_str()).join("repo");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("keep.txt"), "user data").unwrap();
    let mut bad = super::workspace(&id);
    bad.repository_path = Some(f.repo.to_string_lossy().to_string());
    bad.worktree_path = Some(folder.to_string_lossy().to_string());
    bad.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
    bad.archived = true;
    bad.archived_at = Some(crate::now_iso());
    f.store.insert_workspace(&bad).await.expect("insert");
    let good = create_workspace(&f).await;
    f.svc
        .archive_workspace(good.id.clone(), None)
        .await
        .expect("archive");

    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.retired, 1, "{report:?}");
    assert_eq!(report.skipped, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert!(f.store.get_workspace(&good.id).await.unwrap().is_retired());
    let kept = f.store.get_workspace(&id).await.unwrap();
    assert!(!kept.is_retired());
    assert!(
        folder.join("keep.txt").is_file(),
        "refused folder untouched"
    );
}

/// Remote and retire-unsupported workspaces (here a `direct` row whose
/// folder is not a repository of its own) are skipped without an attempt,
/// and an archived row without `archivedAt` is never treated as overdue.
#[intent_test_macros::daemon_test]
async fn auto_retire_skips_remote_unsupported_and_undated_workspaces() {
    let f = fixture().await;
    let mut inserted = Vec::new();
    for (name, remote, mode, dated) in [
        (
            "ws-auto-remote",
            true,
            intent_core::CheckoutMode::Worktree,
            true,
        ),
        (
            "ws-auto-direct",
            false,
            intent_core::CheckoutMode::Direct,
            true,
        ),
        (
            "ws-auto-undated",
            false,
            intent_core::CheckoutMode::Worktree,
            false,
        ),
    ] {
        let id = WorkspaceId::from(name);
        let folder = f.root.join(name).join("repo");
        std::fs::create_dir_all(&folder).unwrap();
        let mut ws = super::workspace(&id);
        ws.repository_path = Some(f.repo.to_string_lossy().to_string());
        ws.worktree_path = Some(folder.to_string_lossy().to_string());
        ws.checkout_mode = Some(mode);
        ws.is_remote = remote;
        ws.archived = true;
        ws.archived_at = dated.then(crate::now_iso);
        f.store.insert_workspace(&ws).await.expect("insert");
        inserted.push((id, folder));
    }

    let report = f
        .svc
        .sweep_auto_retire_after(Some(1), days_from_now(30))
        .await;
    assert_eq!(report.retired, 0, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert_eq!(report.skipped, 3, "{report:?}");
    for (id, folder) in inserted {
        assert!(
            !f.store.get_workspace(&id).await.unwrap().is_retired(),
            "{id}"
        );
        assert!(folder.is_dir(), "{id} folder untouched");
    }
}

/// The sweep reads the setting live from the settings registry: once
/// `workspace.autoRetireArchivedAfterDays = 7` is set, an overdue archived
/// workspace is retired on the next sweep without a restart.
#[intent_test_macros::daemon_test]
async fn auto_retire_reads_the_live_setting() {
    let f = fixture().await;
    let cfg = f.root.join("auto-retire.config.toml");
    let registry = std::sync::Arc::new(crate::SettingsRegistry::load(cfg).expect("registry"));
    let svc = f.svc.clone().with_settings_registry(registry.clone());
    let ws = create_workspace(&f).await;
    svc.archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    assert_eq!(svc.sweep_auto_retire_at(days_from_now(8)).await.retired, 0);
    registry
        .apply(&[(
            "workspace.autoRetireArchivedAfterDays".to_string(),
            serde_json::json!(7),
        )])
        .expect("set");
    assert_eq!(svc.sweep_auto_retire_at(days_from_now(8)).await.retired, 1);
    assert!(f.store.get_workspace(&ws.id).await.unwrap().is_retired());
}

/// Review regression (intentd#2605): the sweep lists workspaces, then
/// retires them later, so the row can be stale. A workspace unarchived after
/// it was listed must be left alone: the automatic op never archives an
/// Active workspace and never deletes its checkout.
#[intent_test_macros::daemon_test]
async fn auto_retire_op_leaves_a_workspace_unarchived_after_listing() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");
    f.svc
        .unarchive_workspace(ws.id.clone())
        .await
        .expect("unarchive");

    let out = f
        .svc
        .auto_retire_workspace_op(ws.id.clone(), days_from_now(8))
        .await
        .expect("no error");
    assert!(out.is_none(), "not due any more: {out:?}");
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Active);
    assert!(!row.archived, "must not be re-archived");
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
}

/// Review regression: a workspace re-archived after the sweep listed it
/// carries a fresh `archivedAt` after the cutoff and is not retired.
#[intent_test_macros::daemon_test]
async fn auto_retire_op_skips_a_workspace_archived_after_the_cutoff() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let out = f
        .svc
        .auto_retire_workspace_op(ws.id.clone(), days_from_now(-1))
        .await
        .expect("no error");
    assert!(out.is_none(), "{out:?}");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().is_retired());
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());

    // The same op retires it once the cutoff is past its archivedAt.
    let out = f
        .svc
        .auto_retire_workspace_op(ws.id.clone(), days_from_now(1))
        .await
        .expect("retire");
    assert!(out.is_some_and(|w| w.is_retired()));
}

/// Review regression: the setting accepts any positive u32, and a window
/// longer than the representable date range must not panic the sweep loop —
/// nothing can be that old, so the sweep is a no-op.
#[intent_test_macros::daemon_test]
async fn auto_retire_with_a_huge_window_is_a_no_op() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let report = f
        .svc
        .sweep_auto_retire_after(Some(u32::MAX), days_from_now(0))
        .await;
    assert_eq!(report, crate::AutoRetireReport::default());
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().is_retired());
}

/// Review regression (intentd#2605): an unarchive that commits while the
/// automatic retire waits for the archive fence must win BEFORE any
/// teardown. The re-check runs under the fence ahead of the script stop, so
/// the resumed workspace's scripts are never closed. With the script stop
/// ahead of the fence, `close_workspace` would enter the park and the op
/// would never finish.
#[intent_test_macros::daemon_test]
async fn auto_retire_op_does_not_stop_scripts_of_a_workspace_unarchived_while_waiting() {
    let f = fixture().await;
    let park = std::sync::Arc::new(crate::runtime_config::SupervisePark::default());
    let svc = f
        .svc
        .clone()
        .with_script_workspace_closed_park(park.clone());
    let ws = create_workspace(&f).await;
    svc.archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    // Hold the fence so the op stops at it, after its first (stale) read.
    let guard = svc.archive_fence.acquire(&ws.id).await;
    let op = intent_core::spawn_daemon({
        let svc = svc.clone();
        let id = ws.id.clone();
        async move { svc.auto_retire_workspace_op(id, days_from_now(8)).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!op.is_finished(), "the op waits on the held fence");
    // An unarchive commits while the op is blocked (as `workspace.unarchive`
    // would under the fence).
    assert!(f
        .store
        .unarchive_workspace_if_archived(&ws.id, &crate::now_iso())
        .await
        .expect("unarchive flip"));
    drop(guard);

    let out = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        op,
    )
    .await
    .expect("op must not park in the script stop")
    .expect("join")
    .expect("no error");
    assert!(out.is_none(), "no longer due: {out:?}");
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(100)),
            park.entered.notified(),
        )
        .await
        .is_err(),
        "scripts must not be closed for a workspace that is no longer due"
    );
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert!(!row.archived && !row.is_retired());
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
}

/// Review regression (intentd#2605): a manual `workspace.retire` that waits
/// on the archive fence while an unarchive commits must refuse BEFORE any
/// teardown. Its resumed scripts are never closed (`close_workspace` would
/// enter the park and the call would never return), and the workspace stays
/// Active with its checkout.
#[intent_test_macros::daemon_test]
async fn manual_retire_does_not_stop_scripts_of_a_workspace_unarchived_while_waiting() {
    let f = fixture().await;
    let park = std::sync::Arc::new(crate::runtime_config::SupervisePark::default());
    let svc = f
        .svc
        .clone()
        .with_script_workspace_closed_park(park.clone());
    let ws = create_workspace(&f).await;
    svc.archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let guard = svc.archive_fence.acquire(&ws.id).await;
    let op = intent_core::spawn_daemon({
        let svc = svc.clone();
        let id = ws.id.clone();
        async move { svc.retire_workspace(id, None).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!op.is_finished(), "retire waits on the held fence");
    assert!(f
        .store
        .unarchive_workspace_if_archived(&ws.id, &crate::now_iso())
        .await
        .expect("unarchive flip"));
    drop(guard);

    let err = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        op,
    )
    .await
    .expect("retire must not park in the script stop")
    .expect("join")
    .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(100)),
            park.entered.notified(),
        )
        .await
        .is_err(),
        "scripts must not be closed for a workspace that was unarchived"
    );
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert!(!row.archived && !row.is_retired());
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
}

// ---------------------------------------------------------------------------
// Ownership: `Retired` is entered only through workspace.retire, and revive
// never detaches a folder the daemon cannot prove it owns.
// ---------------------------------------------------------------------------

/// A generic `workspace.update { status: "Retired" }` is refused on an
/// Active and on an Archived workspace: the row, its `retiredAt` and its
/// checkout are untouched (before the fix the store wrote `Retired` with
/// `archived = false` and no `retiredAt`, skipping all teardown).
#[intent_test_macros::daemon_test]
async fn workspace_update_refuses_retired_status() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    for archive_first in [false, true] {
        if archive_first {
            f.svc
                .archive_workspace(ws.id.clone(), None)
                .await
                .expect("archive");
        }
        let err = f
            .svc
            .update_workspace(
                ws.id.clone(),
                intent_core::WorkspaceUpdate {
                    status: Some(WorkspaceStatus::Retired),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
        let row = f.store.get_workspace(&ws.id).await.unwrap();
        assert!(!row.is_retired(), "archived={archive_first}: {row:?}");
        assert!(row.retired_at.is_none());
        assert_eq!(row.archived, archive_first);
        assert!(wt.is_dir(), "checkout untouched");
    }
}

/// A retired row (status, `archived`, `retiredAt`) pointing at `folder`.
async fn insert_retired_row(f: &Fixture, id: &WorkspaceId, folder: &std::path::Path) {
    let mut ws = super::workspace(id);
    ws.repository_path = Some(f.repo.to_string_lossy().to_string());
    ws.worktree_path = Some(folder.to_string_lossy().to_string());
    ws.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
    ws.status = WorkspaceStatus::Retired;
    ws.archived = true;
    ws.archived_at = Some(crate::now_iso());
    ws.retired_at = Some(crate::now_iso());
    f.store.insert_workspace(&ws).await.expect("insert");
    assert!(f.store.get_workspace(id).await.unwrap().is_retired());
}

/// Revive of a retired row whose `worktreePath` is a user folder outside
/// every daemon root is refused, and the folder is not renamed or deleted.
#[intent_test_macros::daemon_test]
async fn revive_refuses_a_path_outside_the_daemon_roots_and_keeps_it() {
    let f = fixture().await;
    let user = test_tempdir("intentd-retire-user-");
    let folder = user.path().join("ws-foreign").join("repo");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("keep.txt"), "user data").unwrap();
    let id = WorkspaceId::from("ws-foreign");
    insert_retired_row(&f, &id, &folder).await;

    assert!(f.svc.unarchive_workspace(id.clone()).await.is_err());
    assert!(folder.join("keep.txt").is_file(), "user folder untouched");
    assert!(f.store.get_workspace(&id).await.unwrap().is_retired());
}

/// Revive never detaches a folder at a daemon-root path that is not a
/// linked worktree registered in the source repository.
#[intent_test_macros::daemon_test]
async fn revive_refuses_an_unregistered_folder_and_keeps_it() {
    let f = fixture().await;
    let id = WorkspaceId::from("ws-unregistered-revive");
    let folder = f.root.join(id.as_str()).join("repo");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("keep.txt"), "user data").unwrap();
    insert_retired_row(&f, &id, &folder).await;

    assert!(f.svc.unarchive_workspace(id.clone()).await.is_err());
    assert!(folder.join("keep.txt").is_file(), "folder untouched");
    assert!(f.store.get_workspace(&id).await.unwrap().is_retired());
}

/// A linked worktree registered in the fixture repo at `<outside>/<id>/repo`,
/// with a marker file — a real checkout the daemon does not own.
fn outside_linked_worktree(
    f: &Fixture,
    outside: &std::path::Path,
    id: &WorkspaceId,
) -> std::path::PathBuf {
    let parent = outside.join(id.as_str());
    std::fs::create_dir_all(&parent).unwrap();
    let wt = parent.join("repo");
    git2::Repository::open(&f.repo)
        .unwrap()
        .worktree(&format!("outside-{}", id.as_str()), &wt, None)
        .expect("add linked worktree");
    std::fs::write(wt.join("keep.txt"), "user data").unwrap();
    wt
}

/// An Active row whose persisted `worktreePath` is `path`.
async fn insert_active_row(f: &Fixture, id: &WorkspaceId, path: &std::path::Path) {
    let mut ws = super::workspace(id);
    ws.repository_path = Some(f.repo.to_string_lossy().to_string());
    ws.worktree_path = Some(path.to_string_lossy().to_string());
    ws.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
    f.store.insert_workspace(&ws).await.expect("insert");
}

/// Both retire and revive refuse `path`, and the outside checkout keeps its
/// files and its git worktree registration.
async fn assert_escape_refused(
    f: &Fixture,
    id: &WorkspaceId,
    path: &std::path::Path,
    real: &std::path::Path,
) {
    insert_active_row(f, id, path).await;
    let err = f.svc.retire_workspace(id.clone(), None).await.unwrap_err();
    assert!(matches!(err, Error::RetireUnsupported { .. }), "{err:?}");
    assert!(!f.store.get_workspace(id).await.unwrap().is_retired());
    f.store.delete_workspace(id).await.expect("drop active row");

    insert_retired_row(f, id, path).await;
    assert!(f.svc.unarchive_workspace(id.clone()).await.is_err());
    assert!(f.store.get_workspace(id).await.unwrap().is_retired());

    // Give a (wrongly) spawned sweep time to run before checking.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        real.join("keep.txt").is_file(),
        "outside checkout untouched"
    );
    assert!(
        intent_git::worktree::is_registered_worktree(&f.repo, real),
        "outside worktree still registered"
    );
}

/// `<root>/../<outside>/<id>/repo` passes a lexical `starts_with(root)` but
/// resolves outside the managed root: retire and revive both refuse it.
#[intent_test_macros::daemon_test]
async fn retire_and_revive_refuse_a_dotdot_escape() {
    let f = fixture().await;
    let outside = test_tempdir("intentd-retire-outside-");
    let id = WorkspaceId::from("ws-dotdot");
    let real = outside_linked_worktree(&f, outside.path(), &id);
    let sibling = outside.path().file_name().unwrap();
    assert_eq!(
        outside.path().parent(),
        f.root.parent(),
        "sibling temp dirs"
    );
    let escaping = f
        .root
        .join("..")
        .join(sibling)
        .join(id.as_str())
        .join("repo");
    assert!(escaping.starts_with(&f.root), "lexically inside the root");
    assert_escape_refused(&f, &id, &escaping, &real).await;
}

/// `<root>/<id>` is a symlink to an outside folder holding a registered
/// checkout: the path is lexically owned, canonically outside — refused.
/// Unix-only: it builds the escape with `std::os::unix::fs::symlink`.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn retire_and_revive_refuse_a_symlinked_ancestor_escape() {
    let f = fixture().await;
    let outside = test_tempdir("intentd-retire-outside-");
    let id = WorkspaceId::from("ws-symlinked");
    let real = outside_linked_worktree(&f, outside.path(), &id);
    std::os::unix::fs::symlink(outside.path().join(id.as_str()), f.root.join(id.as_str())).unwrap();
    let via_link = f.root.join(id.as_str()).join("repo");
    assert!(
        via_link.join("keep.txt").is_file(),
        "link resolves to the checkout"
    );
    assert_escape_refused(&f, &id, &via_link, &real).await;
}

/// A missing checkout whose `<id>` parent resolves outside the root (via a
/// symlinked directory under the root) is refused: no sweep removes the
/// outside parent folder.
/// Unix-only: it builds the escape with `std::os::unix::fs::symlink`.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn retire_refuses_a_missing_checkout_whose_parent_is_outside() {
    let f = fixture().await;
    let outside = test_tempdir("intentd-retire-outside-");
    let id = WorkspaceId::from("ws-missing-outside");
    let outside_parent = outside.path().join(id.as_str());
    std::fs::create_dir_all(&outside_parent).unwrap();
    std::fs::write(outside_parent.join("keep.txt"), "user data").unwrap();
    std::os::unix::fs::symlink(outside.path(), f.root.join("link")).unwrap();
    let missing = f.root.join("link").join(id.as_str()).join("repo");
    assert!(!missing.exists());
    insert_active_row(&f, &id, &missing).await;

    let err = f.svc.retire_workspace(id.clone(), None).await.unwrap_err();
    assert!(matches!(err, Error::RetireUnsupported { .. }), "{err:?}");
    assert!(!f.store.get_workspace(&id).await.unwrap().is_retired());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        outside_parent.join("keep.txt").is_file(),
        "outside parent kept"
    );
}

// ---------------------------------------------------------------------------
// Terminal admission: terminalId-only mutations and creation racing retire.
// ---------------------------------------------------------------------------

/// A direct `file.placeAttachment` that passed its first retired check
/// while retire holds the archive fence waits for it, re-checks, and is
/// refused: nothing is placed or registered for the retired workspace.
#[intent_test_macros::daemon_test]
async fn place_attachment_racing_retire_places_nothing() {
    use base64::Engine as _;
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let guard = f.svc.archive_fence.acquire(&ws.id).await;
    let place = intent_core::spawn_daemon({
        let svc = f.svc.clone();
        let id = ws.id.clone();
        async move {
            svc.file_place_attachment(
                id,
                "late.txt".into(),
                Some(base64::engine::general_purpose::STANDARD.encode(b"late")),
                None,
                None,
                None,
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    flip_retired(&f, &ws.id).await;
    drop(guard);

    let err = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        place,
    )
    .await
    .expect("placement finishes")
    .expect("join")
    .unwrap_err();
    assert!(matches!(err, Error::WorkspaceRetired { .. }), "{err:?}");
    assert!(f.store.list_attachments(&ws.id).await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Purge status message (PROTOCOL §5.1 Retired workspaces): a retire sets the
// workspace `statusMessage` so the card shows the folder is gone; a revive
// clears it only while it is unchanged.
// ---------------------------------------------------------------------------

/// A manual retire replaces the status message with the purge text in the
/// same row write and the single retire delta.
#[intent_test_macros::daemon_test]
async fn retire_sets_the_purge_status_message_in_its_single_delta() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .update_workspace(
            ws.id.clone(),
            intent_core::WorkspaceUpdate {
                status_message: Some("PR #12 merged".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("set status");
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");
    let mut sub = subscribe_updates(&f, &ws.id);
    let retired = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert_eq!(
        retired.status_message.as_deref(),
        Some(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
    );
    let changes = drain_changes(&mut sub).await;
    let retire_deltas: Vec<_> = changes
        .iter()
        .filter(|c| c["status"] == "Retired")
        .collect();
    assert_eq!(retire_deltas.len(), 1, "{changes:?}");
    assert_eq!(
        retire_deltas[0]["statusMessage"],
        serde_json::json!(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
    );
}

/// The automatic purge sets the same status message.
#[intent_test_macros::daemon_test]
async fn auto_purge_sets_the_purge_status_message() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");
    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.retired, 1, "{report:?}");
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert_eq!(
        row.status_message.as_deref(),
        Some(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
    );
}

/// A message written into the row after the purge (here directly, standing
/// in for an agent or the daemon's own status writers) survives the revive;
/// only the unchanged purge text is cleared.
#[intent_test_macros::daemon_test]
async fn revive_keeps_a_status_message_changed_since_the_purge() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    let mut row = f.store.get_workspace(&ws.id).await.unwrap();
    row.status_message = Some("Waiting on review".to_string());
    f.store.update_workspace(&row).await.expect("write message");
    let mut sub = subscribe_updates(&f, &ws.id);
    let revived = f
        .svc
        .unarchive_workspace(ws.id.clone())
        .await
        .expect("revive");
    assert_eq!(revived.status_message.as_deref(), Some("Waiting on review"));
    let changes = drain_changes(&mut sub).await;
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert!(changes[0].get("statusMessage").is_none(), "{changes:?}");
}

// ---------------------------------------------------------------------------
// Automatic purge safety: folders holding work the purge would destroy are
// skipped and retried on the next sweep.
// ---------------------------------------------------------------------------

/// An archived workspace with uncommitted changes is skipped (folder kept,
/// still Archived); once the changes are committed the next sweep purges it.
#[intent_test_macros::daemon_test]
async fn auto_purge_skips_a_folder_with_uncommitted_changes_until_it_is_clean() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    std::fs::write(wt.join("draft.txt"), "unsaved work").unwrap();
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.retired, 0, "{report:?}");
    assert_eq!(report.skipped, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert!(wt.join("draft.txt").is_file(), "unsaved work kept");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().is_retired());

    std::fs::remove_file(wt.join("draft.txt")).unwrap();
    let report = f
        .svc
        .sweep_auto_retire_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.retired, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().is_retired());
}

/// Commit `name` on top of `HEAD` in the repository at `dir`, returning it.
fn commit_in(dir: &std::path::Path, name: &str) -> git2::Oid {
    let repo = git2::Repository::open(dir).expect("open repo");
    let parent = repo.head().ok().and_then(|h| h.peel_to_commit().ok());
    std::fs::write(dir.join(name), "work\n").unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(std::path::Path::new(name)).unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    let parents: Vec<&git2::Commit<'_>> = parent.iter().collect();
    repo.commit(Some("HEAD"), &sig, &sig, name, &tree, &parents)
        .unwrap()
}

/// An archived workspace whose nested repository holds commits on no remote
/// ref is skipped; a merged PR for an older commit (new work since) still
/// skips it; a merged PR covering the nested `HEAD` lets the purge run —
/// the commits survive on the forge.
#[intent_test_macros::daemon_test]
async fn auto_purge_lets_a_merged_pr_cover_nested_unpushed_commits() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    let primary = git2::Repository::open(&wt).expect("open worktree");
    let exclude = primary.commondir().join("info").join("exclude");
    std::fs::create_dir_all(exclude.parent().unwrap()).unwrap();
    std::fs::write(&exclude, "packages/\n").unwrap();

    let nested = wt.join("packages").join("sub");
    std::fs::create_dir_all(&nested).unwrap();
    git2::Repository::init(&nested).expect("init nested");
    let base = commit_in(&nested, "base.txt");
    git2::Repository::open(&nested)
        .unwrap()
        .reference("refs/remotes/origin/main", base, true, "base")
        .unwrap();
    let merged = commit_in(&nested, "feature.txt");

    let mut root = intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: ws.id.clone(),
        path: nested.to_str().unwrap().into(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    f.store
        .upsert_workspace_git_root(&root)
        .await
        .expect("register nested root");
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");
    let merged_pr = |head: git2::Oid| {
        serde_json::from_value::<intent_core::PullRequestInfo>(serde_json::json!({
            "id": "PR_9",
            "number": 9,
            "url": "https://github.com/o/sub/pull/9",
            "title": "feat: sub",
            "status": "Merged",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-02T00:00:00Z",
            "headSha": head.to_string(),
        }))
        .expect("pr record")
    };
    let sweep = || f.svc.sweep_auto_retire_after(Some(7), days_from_now(8));

    let report = sweep().await;
    assert_eq!((report.retired, report.skipped), (0, 1), "{report:?}");

    let head = commit_in(&nested, "follow-up.txt");
    root.pull_requests = Some(vec![merged_pr(merged)]);
    f.store
        .update_workspace_git_root_pr(&root)
        .await
        .expect("store merged PR");
    let report = sweep().await;
    assert_eq!(
        (report.retired, report.skipped),
        (0, 1),
        "a commit after the merge is not covered: {report:?}"
    );
    assert!(nested.join("follow-up.txt").is_file());

    root.pull_requests = Some(vec![merged_pr(head)]);
    f.store
        .update_workspace_git_root_pr(&root)
        .await
        .expect("store merged PR");
    let report = sweep().await;
    assert_eq!(report.retired, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().is_retired());
}

/// The sweep reads the renamed setting live: `workspace.purgeArchivedAfterDays
/// = 7` turns it on without a restart.
#[intent_test_macros::daemon_test]
async fn auto_purge_reads_the_live_purge_setting() {
    let f = fixture().await;
    let cfg = f.root.join("auto-purge.config.toml");
    let registry = std::sync::Arc::new(crate::SettingsRegistry::load(cfg).expect("registry"));
    let svc = f.svc.clone().with_settings_registry(registry.clone());
    let ws = create_workspace(&f).await;
    svc.archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    assert_eq!(svc.sweep_auto_retire_at(days_from_now(8)).await.retired, 0);
    registry
        .apply(&[(
            "workspace.purgeArchivedAfterDays".to_string(),
            serde_json::json!(7),
        )])
        .expect("set");
    assert_eq!(svc.sweep_auto_retire_at(days_from_now(8)).await.retired, 1);
    assert!(f.store.get_workspace(&ws.id).await.unwrap().is_retired());
}

// ---------------------------------------------------------------------------
// Standalone (`direct` / `cow`) checkouts: the folder IS the repository, so
// the purge removes it only when nothing in it exists solely there
// (`LocalWorkRule::PurgeStandalone`).
// ---------------------------------------------------------------------------

use crate::test_support::submodule_fixture::git;

/// A daemon-owned standalone checkout laid out like cache hydration:
/// `<root>/<id>/repo`, cloned from a bare `origin.git`, with
/// `repositoryPath == worktreePath`, archived now. `with_sub` adds a pushed
/// submodule `sub` (bare origin `sub.git`).
struct Standalone {
    _remotes: tempfile::TempDir,
    remotes: std::path::PathBuf,
    ws: intent_core::Workspace,
    folder: std::path::PathBuf,
}

async fn insert_standalone(
    f: &Fixture,
    name: &str,
    mode: intent_core::CheckoutMode,
    with_sub: bool,
) -> Standalone {
    insert_standalone_with(f, name, mode, with_sub, |_| {}).await
}

async fn insert_standalone_with(
    f: &Fixture,
    name: &str,
    mode: intent_core::CheckoutMode,
    with_sub: bool,
    tweak: impl FnOnce(&mut intent_core::Workspace),
) -> Standalone {
    let remotes_dir = test_tempdir("intentd-standalone-remotes-");
    let remotes = remotes_dir.path().to_path_buf();
    git(
        &remotes,
        &[
            "clone",
            "-q",
            "--bare",
            f.repo.to_str().unwrap(),
            "origin.git",
        ],
    );
    let id = WorkspaceId::from(name);
    let ws_dir = f.root.join(id.as_str());
    std::fs::create_dir_all(ws_dir.join(".workspace")).unwrap();
    let origin = remotes.join("origin.git");
    git(&ws_dir, &["clone", "-q", origin.to_str().unwrap(), "repo"]);
    let folder = ws_dir.join("repo");
    if with_sub {
        let sub_src = remotes.join("sub-src");
        crate::test_support::submodule_fixture::init_repo(&sub_src);
        git(&remotes, &["clone", "-q", "--bare", "sub-src", "sub.git"]);
        let sub_url = remotes.join("sub.git");
        git(
            &folder,
            &["submodule", "add", "-q", sub_url.to_str().unwrap(), "sub"],
        );
        git(&folder, &["commit", "-q", "-m", "add sub"]);
        git(&folder, &["push", "-q", "origin", "HEAD"]);
    }
    let mut ws = super::workspace(&id);
    ws.repository_path = Some(folder.to_string_lossy().to_string());
    ws.worktree_path = ws.repository_path.clone();
    ws.repository_owner = Some("o".to_string());
    ws.repository_name = Some("repo".to_string());
    ws.branch = git(&folder, &["rev-parse", "--abbrev-ref", "HEAD"]);
    ws.checkout_mode = Some(mode);
    ws.archived = true;
    ws.archived_at = Some(crate::now_iso());
    tweak(&mut ws);
    f.store.insert_workspace(&ws).await.expect("insert");
    Standalone {
        _remotes: remotes_dir,
        remotes,
        ws,
        folder,
    }
}

fn commit_file(dir: &std::path::Path, name: &str) -> String {
    std::fs::write(dir.join(name), format!("{name}\n")).unwrap();
    git(dir, &["add", name]);
    git(dir, &["commit", "-q", "-m", name]);
    git(dir, &["rev-parse", "HEAD"])
}

fn purge_sweep(f: &Fixture) -> impl std::future::Future<Output = crate::AutoRetireReport> + '_ {
    f.svc.sweep_auto_retire_after(Some(7), days_from_now(8))
}

/// A clean `direct` checkout (and a clean `cow` one) archived before the
/// cutoff — pushed primary, pushed submodule — is retired through the
/// retire path: one retired delta, and the whole `<root>/<id>` folder is
/// removed.
#[intent_test_macros::daemon_test]
async fn auto_purge_removes_a_clean_standalone_checkout() {
    let f = fixture().await;
    let direct = insert_standalone(
        &f,
        "ws-direct-clean",
        intent_core::CheckoutMode::Direct,
        true,
    )
    .await;
    let cow = insert_standalone(&f, "ws-cow-clean", intent_core::CheckoutMode::Cow, false).await;
    let mut sub = subscribe_updates(&f, &direct.ws.id);

    let report = purge_sweep(&f).await;
    assert_eq!(
        (report.retired, report.skipped, report.failed),
        (2, 0, 0),
        "{report:?}"
    );
    for s in [&direct, &cow] {
        assert!(f.store.get_workspace(&s.ws.id).await.unwrap().is_retired());
        assert!(!s.folder.exists(), "checkout gone when retire returns");
        wait_gone(&f.root.join(s.ws.id.as_str())).await;
    }
    let changes = drain_changes(&mut sub).await;
    assert!(
        changes
            .iter()
            .any(|c| c["status"] == "Retired" && c["archived"] == true),
        "retire delta published: {changes:?}"
    );
}

/// Work that exists only in the folder blocks the purge, and the folder
/// survives: uncommitted changes, an unpushed commit on a local branch
/// other than `HEAD`, a stash, and an unpushed commit in a nested `.dev`
/// clone. Each is counted as a local-work skip.
#[intent_test_macros::daemon_test]
async fn auto_purge_keeps_a_standalone_checkout_with_work_only_in_the_folder() {
    let f = fixture().await;
    let dirty = insert_standalone(
        &f,
        "ws-direct-dirty",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    std::fs::write(dirty.folder.join("draft.txt"), "unsaved").unwrap();

    let branch = insert_standalone(
        &f,
        "ws-direct-branch",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    git(&branch.folder, &["checkout", "-q", "-b", "side"]);
    commit_file(&branch.folder, "side.txt");
    git(&branch.folder, &["checkout", "-q", "-"]);

    let stash = insert_standalone(
        &f,
        "ws-direct-stash",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    std::fs::write(stash.folder.join("README.md"), "wip").unwrap();
    git(&stash.folder, &["stash", "-q"]);

    let nested = insert_standalone(
        &f,
        "ws-direct-nested",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    std::fs::write(nested.folder.join(".git/info/exclude"), ".dev/\n").unwrap();
    let dev = nested.folder.join(".dev");
    std::fs::create_dir_all(&dev).unwrap();
    git(
        &dev,
        &[
            "clone",
            "-q",
            nested.remotes.join("origin.git").to_str().unwrap(),
            "wt",
        ],
    );
    commit_file(&dev.join("wt"), "nested.txt");

    let report = purge_sweep(&f).await;
    assert_eq!((report.retired, report.failed), (0, 0), "{report:?}");
    assert_eq!(report.skipped, 4, "{report:?}");
    assert_eq!(report.skipped_local_work, 4, "{report:?}");
    for s in [&dirty, &branch, &stash, &nested] {
        let row = f.store.get_workspace(&s.ws.id).await.unwrap();
        assert!(!row.is_retired(), "{}", s.ws.id);
        assert!(row.archived, "{}", s.ws.id);
        assert!(
            s.folder.join("README.md").is_file(),
            "{} folder kept",
            s.ws.id
        );
    }
    assert!(dev.join("wt/nested.txt").is_file());
}

/// A gitlink-only change — the submodule moved to a commit its remote has —
/// does not block the purge.
#[intent_test_macros::daemon_test]
async fn auto_purge_allows_a_gitlink_moved_to_a_pushed_commit() {
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-gitlink",
        intent_core::CheckoutMode::Direct,
        true,
    )
    .await;
    let sub_src = s.remotes.join("sub-src");
    commit_file(&sub_src, "next.txt");
    git(
        &sub_src,
        &[
            "push",
            "-q",
            s.remotes.join("sub.git").to_str().unwrap(),
            "main",
        ],
    );
    let sub = s.folder.join("sub");
    git(&sub, &["fetch", "-q", "origin"]);
    git(&sub, &["merge", "-q", "--ff-only", "origin/main"]);
    assert!(
        !git(&s.folder, &["status", "--porcelain"]).is_empty(),
        "the gitlink moved"
    );

    let report = purge_sweep(&f).await;
    assert_eq!(report.retired, 1, "{report:?}");
    wait_gone(&f.root.join(s.ws.id.as_str())).await;
}

/// Unpushed commits on `HEAD` block until a merged PR covers `HEAD` — the
/// commits survive on the forge.
#[intent_test_macros::daemon_test]
async fn auto_purge_lets_a_merged_pr_cover_standalone_head() {
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-merged",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    let head = commit_file(&s.folder, "feature.txt");
    let report = purge_sweep(&f).await;
    assert_eq!(
        (report.retired, report.skipped_local_work),
        (0, 1),
        "{report:?}"
    );

    let mut ws = f.store.get_workspace(&s.ws.id).await.unwrap();
    ws.pull_requests = Some(vec![serde_json::from_value(serde_json::json!({
        "id": "PR_7",
        "number": 7,
        "url": "https://github.com/o/repo/pull/7",
        "title": "feat: feature",
        "status": "Merged",
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-02T00:00:00Z",
        "headSha": head,
    }))
    .expect("pr record")]);
    f.store
        .update_workspace(&ws)
        .await
        .expect("store merged PR");
    let report = purge_sweep(&f).await;
    assert_eq!(report.retired, 1, "{report:?}");
    wait_gone(&f.root.join(s.ws.id.as_str())).await;
}

/// The sweep summary counts skips per reason, so a purge that cannot act
/// is visible: unsupported (remote, skipIsolation, a standalone folder that
/// is a linked worktree), local work and undated rows.
#[intent_test_macros::daemon_test]
async fn auto_purge_reports_skip_counts_per_reason() {
    let f = fixture().await;
    let dirty = insert_standalone(
        &f,
        "ws-count-dirty",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    std::fs::write(dirty.folder.join("draft.txt"), "x").unwrap();
    let mut rows = Vec::new();
    for (name, remote, skip, dated) in [
        ("ws-count-remote", true, false, true),
        ("ws-count-skip", false, true, true),
        ("ws-count-undated", false, false, false),
    ] {
        let s = insert_standalone_with(&f, name, intent_core::CheckoutMode::Direct, false, |ws| {
            ws.is_remote = remote;
            ws.skip_worktree = skip;
            ws.archived_at = dated.then(crate::now_iso);
        })
        .await;
        rows.push(s);
    }
    let gitfile = insert_standalone(
        &f,
        "ws-count-gitfile",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    std::fs::remove_dir_all(&gitfile.folder).unwrap();
    git(
        &f.repo,
        &[
            "worktree",
            "add",
            "-q",
            "--detach",
            gitfile.folder.to_str().unwrap(),
        ],
    );

    let report = purge_sweep(&f).await;
    assert_eq!((report.retired, report.failed), (0, 0), "{report:?}");
    assert_eq!(report.skipped, 5, "{report:?}");
    assert_eq!(report.skipped_unsupported, 3, "{report:?}");
    assert_eq!(report.skipped_local_work, 1, "{report:?}");
    assert_eq!(report.skipped_undated, 1, "{report:?}");
    for s in rows.iter().chain([&dirty, &gitfile]) {
        assert!(s.folder.join("README.md").is_file(), "{} kept", s.ws.id);
    }
}

/// A manual `workspace.retire` of a standalone checkout uses the same gate:
/// refused while a commit exists only in the folder (folder kept, still
/// archived), retired once it is pushed.
#[intent_test_macros::daemon_test]
async fn manual_retire_of_a_standalone_checkout_refuses_unpushed_work() {
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-manual",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    commit_file(&s.folder, "local.txt");
    let err = f
        .svc
        .retire_workspace(s.ws.id.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    assert!(s.folder.join("local.txt").is_file(), "folder kept");
    let row = f.store.get_workspace(&s.ws.id).await.unwrap();
    assert!(!row.is_retired() && row.archived);

    git(&s.folder, &["push", "-q", "origin", "HEAD"]);
    let retired = f
        .svc
        .retire_workspace(s.ws.id.clone(), None)
        .await
        .expect("retire");
    assert_eq!(retired.status, WorkspaceStatus::Retired);
    wait_gone(&f.root.join(s.ws.id.as_str())).await;
}

/// Revive of a retired standalone checkout: a fresh clone from the repo
/// cache at the same path, on a new branch cut from the default branch,
/// with `origin` pointing at the cache's upstream. Without a cache the
/// revive is refused and the workspace stays retired.
#[intent_test_macros::daemon_test]
async fn revive_reclones_a_standalone_checkout_from_the_repo_cache() {
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-revive",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    f.svc
        .retire_workspace(s.ws.id.clone(), None)
        .await
        .expect("retire");
    wait_gone(&f.root.join(s.ws.id.as_str())).await;

    let err = f
        .svc
        .unarchive_workspace(s.ws.id.clone())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    assert!(f.store.get_workspace(&s.ws.id).await.unwrap().is_retired());

    let cache = intent_git::repo_cache::cache_path_for(
        &intent_git::repo_cache::cache_root_for(&f.root),
        "o",
        "repo",
    );
    std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
    let origin = s.remotes.join("origin.git");
    git(
        cache.parent().unwrap(),
        &["clone", "-q", origin.to_str().unwrap(), "repo"],
    );

    let revived = f
        .svc
        .unarchive_workspace(s.ws.id.clone())
        .await
        .expect("revive");
    assert_eq!(revived.status, WorkspaceStatus::Active);
    assert_eq!(
        revived.branch,
        format!("{}-2", s.ws.branch),
        "never the old branch name, which the remote still holds"
    );
    assert_eq!(revived.worktree_path, s.ws.worktree_path);
    assert!(s.folder.join("README.md").is_file(), "fresh checkout");
    assert!(
        s.folder.join(".git").is_dir(),
        "a standalone repository again"
    );
    assert_eq!(head_branch(&s.folder), revived.branch);
    assert_eq!(
        git(&s.folder, &["remote", "get-url", "origin"]),
        origin.to_string_lossy(),
        "origin is the cache's upstream, never the cache"
    );
}

/// A remote agent pushes its work refs into the workspace's own repository,
/// which for a standalone checkout is the folder: while the workspace has a
/// remote agent, neither the purge nor a manual retire removes it.
#[intent_test_macros::daemon_test]
async fn a_standalone_checkout_with_a_remote_agent_is_kept() {
    use intent_core::nodes::{
        LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
        NodeRecord, NodeState,
    };
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-remote-agent",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    let agent = intent_core::AgentId::from("agent-remote");
    f.store
        .insert_agent_session(&crate::test_support::fixtures::session(&agent, &s.ws.id))
        .await
        .expect("session");
    let node = NodeRecord {
        id: "node-test".into(),
        head_id: "head-test".into(),
        node_identity: "identity-test".into(),
        name: "Build host".into(),
        kind: NodeKind::Static,
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
        state: NodeState::Ready,
        draining: false,
        capacity: NodeCapacity {
            max_agents: 4,
            reserved_agents: 0,
            memory_budget_bytes: 1 << 30,
            used_memory_bytes: 0,
            exclusive_reserved: false,
        },
        version: "test".into(),
        endpoint: Some("wss://node.example:443/node".into()),
        certificate_sha256: Some("a".repeat(64)),
        last_seen_at: None,
    };
    let lease = NodeLease {
        owner: LeaseOwner {
            head_id: node.head_id.clone(),
            node_id: node.id.clone(),
            node_identity: node.node_identity.clone(),
            lease_id: "lease-test".into(),
            incarnation: uuid::Uuid::new_v4().to_string(),
        },
        state: LeaseState::Acquiring,
        created_at: crate::now_iso(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    };
    f.store
        .enroll_node(&node, &lease)
        .await
        .expect("enroll node");
    f.store
        .record_remote_agent(&intent_core::remote_launch::RemoteAgentRecord {
            agent_id: agent.clone(),
            workspace_id: s.ws.id.clone(),
            node_id: node.id.clone(),
            lease_id: lease.owner.lease_id.clone(),
            created_at: crate::now_iso(),
        })
        .await
        .expect("record remote agent");

    let report = purge_sweep(&f).await;
    assert_eq!(
        (report.retired, report.skipped_local_work),
        (0, 1),
        "{report:?}"
    );
    let err = f
        .svc
        .retire_workspace(s.ws.id.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    assert!(s.folder.join("README.md").is_file(), "folder kept");
    assert!(!f.store.get_workspace(&s.ws.id).await.unwrap().is_retired());
}

/// Deleting a remote agent drops its `remote_agent` row, but its final stop
/// stays recorded until the machine's last push lands — in this folder.
/// While it is pending the purge keeps the folder.
#[intent_test_macros::daemon_test]
async fn a_standalone_checkout_with_a_pending_final_stop_is_kept() {
    let f = fixture().await;
    let s = insert_standalone(
        &f,
        "ws-direct-final-stop",
        intent_core::CheckoutMode::Direct,
        false,
    )
    .await;
    f.store
        .record_final_stop(&intent_core::remote_launch::RemoteFinalStopRecord {
            agent_id: intent_core::AgentId::from("agent-deleted"),
            workspace_id: s.ws.id.clone(),
            machine_id: "node-test".into(),
            lease_id: "lease-test".into(),
            stop_id: "stop-1".into(),
            reason: intent_core::remote_launch::FinalStopReason::AgentDeleted,
            repo_path: s.folder.to_string_lossy().to_string(),
            requested_at: crate::now_iso(),
            submodules: Vec::new(),
        })
        .await
        .expect("record final stop");

    let report = purge_sweep(&f).await;
    assert_eq!(
        (report.retired, report.skipped_local_work),
        (0, 1),
        "{report:?}"
    );
    assert!(s.folder.join("README.md").is_file(), "folder kept");

    assert!(f
        .store
        .finish_final_stop(&intent_core::AgentId::from("agent-deleted"), "stop-1")
        .await
        .expect("finish"));
    let report = purge_sweep(&f).await;
    assert_eq!(report.retired, 1, "{report:?}");
}
