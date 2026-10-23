//! `workspace.retire` (PROTOCOL §5.1): the Retired lifecycle state at the
//! service layer. Retire deletes the checkout folder, keeps every DB row and
//! the git branch, hides the workspace from every list unless the caller
//! opts in with `includeRetired`, and refuses disk-backed mutations with
//! `workspace-retired`.

use std::time::Duration;

use intent_core::{
    Error, WorkspaceApi, WorkspaceCreate, WorkspaceId, WorkspaceListFilter, WorkspaceStatus,
};
use intent_store::Store;

use super::turn_end_unread_gate::session;
use super::{note, test_tempdir, TempDb};
use crate::{EventBus, Services};

struct Fixture {
    _tmp: TempDb,
    _repo: tempfile::TempDir,
    _root: tempfile::TempDir,
    root: std::path::PathBuf,
    repo: std::path::PathBuf,
    store: Store,
    bus: EventBus,
    svc: Services,
}

/// A git repo with one commit, plus a `Services` with a per-test workspaces
/// root and an event bus.
async fn fixture() -> Fixture {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let repo_dir = test_tempdir("intentd-retire-repo-");
    let repo = git2::Repository::init(repo_dir.path()).expect("init repo");
    {
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Tester").unwrap();
        cfg.set_str("user.email", "t@e.dev").unwrap();
    }
    std::fs::write(repo_dir.path().join("README.md"), "init\n").unwrap();
    let mut index = repo.index().unwrap();
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "chore: init", &tree, &[])
        .unwrap();
    let root_dir = test_tempdir("intentd-retire-root-");
    let bus = EventBus::new(store.clone());
    let svc = Services::new(store.clone())
        .with_workspaces_root(root_dir.path().to_path_buf())
        .with_event_bus(bus.clone());
    Fixture {
        root: root_dir.path().to_path_buf(),
        repo: repo_dir.path().to_path_buf(),
        _tmp: tmp,
        _repo: repo_dir,
        _root: root_dir,
        store,
        bus,
        svc,
    }
}

/// Provision a real worktree workspace off the fixture repo, with one note
/// and one agent session so "rows are kept" is observable.
async fn create_workspace(f: &Fixture) -> intent_core::Workspace {
    let ws = f
        .svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(f.repo.to_string_lossy().to_string()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create")
        .workspace;
    assert!(
        std::path::Path::new(ws.worktree_path.as_deref().expect("worktree path")).is_dir(),
        "checkout provisioned"
    );
    f.store
        .insert_note(&note(&ws.id, &format!("note-{}", ws.id.0), "kept"))
        .await
        .expect("note");
    let agent = intent_core::AgentId::from(format!("agent-{}", ws.id.0));
    f.store
        .insert_agent_session(&session(&agent, &ws.id))
        .await
        .expect("session");
    ws
}

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

/// Disk-backed mutations on a retired workspace are refused with
/// `workspace-retired`, as is archive; `workspace.delete` still works.
/// (Unarchive revives instead — see the revive tests below.)
#[intent_test_macros::daemon_test]
async fn retired_workspace_refuses_disk_mutations_and_still_deletes() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    let id = || ws.id.clone();
    let is_retired = |r: &Error| matches!(r, Error::WorkspaceRetired { workspace_id } if *workspace_id == ws.id.0);

    let err = f
        .svc
        .file_write(id(), "a.txt".into(), "x".into(), None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "file.write: {err:?}");
    let err = f.svc.file_mkdir(id(), "d".into(), None).await.unwrap_err();
    assert!(is_retired(&err), "file.mkdir: {err:?}");
    let err = f
        .svc
        .file_delete(id(), "README.md".into(), None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "file.delete: {err:?}");
    // script.start/restart/run are refused at the entry gates (RPC
    // `retired_gate`, ws.* dispatcher), not in the service: an extra await
    // before the launch reservation would reorder the start/stop race.
    let err = f
        .svc
        .git_stage(id(), serde_json::json!(["README.md"]))
        .await
        .unwrap_err();
    assert!(is_retired(&err), "git.stage: {err:?}");
    let err = f
        .svc
        .git_agent_commit(id(), "msg".into(), None, None, None, true, None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "git.agentCommit: {err:?}");
    let err = f
        .svc
        .terminal_create(id(), 80, 24, None, None, None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "terminal.create: {err:?}");
    let err = f.svc.archive_workspace(id(), None).await.unwrap_err();
    assert!(is_retired(&err), "workspace.archive: {err:?}");
    assert_eq!(
        f.store.get_workspace(&ws.id).await.unwrap().status,
        WorkspaceStatus::Retired,
        "refusals leave the row retired"
    );

    f.svc.delete_workspace(id()).await.expect("delete retired");
    assert!(matches!(
        f.store.get_workspace(&ws.id).await,
        Err(Error::NotFound(_))
    ));
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

/// Retire only admits daemon-owned git worktrees: `cow` / `direct`
/// checkouts, `skipIsolation`, remote and checkout-less workspaces are
/// refused with `retire-unsupported` and left untouched.
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

/// Retire kills the workspace's terminals: their shells ran in the deleted
/// folder, and a write addressed only by `terminalId` must have nothing left
/// to reach.
#[intent_test_macros::daemon_test]
async fn retire_kills_workspace_terminals() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .terminal_create(ws.id.clone(), 80, 24, None, None, None)
        .await
        .expect("terminal");
    assert_eq!(f.svc.pty.list_scope(ws.id.as_str()).len(), 1);
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert!(
        f.svc.pty.list_scope(ws.id.as_str()).is_empty(),
        "terminals killed on retire"
    );
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

fn subscribe_updates(f: &Fixture, ws: &WorkspaceId) -> crate::Subscription {
    f.bus.subscribe(crate::SubscriptionFilter {
        event_types: vec!["workspace:updated".to_string()],
        workspace_id: Some(ws.0.clone()),
        ..Default::default()
    })
}

/// Collect the `changes` of every `workspace:updated` delta that arrives
/// within a short quiet window.
async fn drain_changes(sub: &mut crate::Subscription) -> Vec<serde_json::Value> {
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
        })
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
