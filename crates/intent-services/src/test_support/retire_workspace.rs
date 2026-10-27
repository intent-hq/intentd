//! The `workspace.retire` fixture: a git repo with one commit, services
//! with a per-test workspaces root and an event bus, and a provisioned
//! workspace with a note and an agent row.

use intent_core::{WorkspaceApi, WorkspaceCreate, WorkspaceId};
use intent_store::Store;

use crate::test_support::fixtures::{note, session, TempDb};
use crate::test_support::test_tempdir;
use crate::{EventBus, Services};

pub struct Fixture {
    pub _tmp: TempDb,
    pub _repo: tempfile::TempDir,
    pub _root: tempfile::TempDir,
    pub root: std::path::PathBuf,
    pub repo: std::path::PathBuf,
    pub store: Store,
    pub bus: EventBus,
    pub svc: Services,
}

/// A git repo with one commit, plus a `Services` with a per-test workspaces
/// root and an event bus.
pub async fn fixture() -> Fixture {
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
pub async fn create_workspace(f: &Fixture) -> intent_core::Workspace {
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

pub fn days_from_now(days: i64) -> time::OffsetDateTime {
    time::OffsetDateTime::now_utc() + time::Duration::days(days)
}

/// Flip `ws` to Retired in the store only (the window before retire's
/// teardown, or a terminal that escaped the sweep).
pub async fn flip_retired(f: &Fixture, ws: &WorkspaceId) {
    f.store
        .archive_workspace_detaching_guests(ws, &intent_core::now_iso())
        .await
        .unwrap();
    assert!(f
        .store
        .retire_workspace_if_archived(ws, &intent_core::now_iso())
        .await
        .unwrap());
}
