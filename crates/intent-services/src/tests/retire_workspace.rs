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
/// `workspace-retired`, as are archive and unarchive; `workspace.delete`
/// still works.
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
    let err = f
        .svc
        .script_start(id(), "missing".into())
        .await
        .unwrap_err();
    assert!(is_retired(&err), "script.start: {err:?}");
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
    let err = f.svc.unarchive_workspace(id()).await.unwrap_err();
    assert!(is_retired(&err), "workspace.unarchive: {err:?}");
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
    let cases = [
        (cow, Some(intent_core::CheckoutMode::Cow)),
        (direct, Some(intent_core::CheckoutMode::Direct)),
        (skip, None),
        (remote, None),
        (bare, None),
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
