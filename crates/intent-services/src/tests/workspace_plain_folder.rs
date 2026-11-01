//! `workspace.create { plainFolder: true }` (PROTOCOL §5.1 "Plain-folder
//! workspaces"): a new folder is created with no `.git`, an existing non-git
//! folder gets a row only, rejected paths and git-only options leave no side
//! effects, and the persisted flag survives a restart and a later `.git`.

use std::path::{Path, PathBuf};

use intent_core::{
    Error, PlainFolderPathReason, WorkspaceApi, WorkspaceCreate, WorkspaceListFilter,
    WorkspaceUpdate,
};
use intent_store::Store;

use super::{TempDb, WorkspacesRoot};
use crate::test_support::test_tempdir;
use crate::Services;

struct Harness {
    tmp: TempDb,
    ws_root: WorkspacesRoot,
    services: Services,
    store: Store,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_root = WorkspacesRoot::new();
    let services =
        Services::new_isolated(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
    Harness {
        tmp,
        ws_root,
        services,
        store,
    }
}

fn plain(path: &Path) -> WorkspaceCreate {
    WorkspaceCreate {
        plain_folder: Some(true),
        repository_path: Some(path.to_string_lossy().into_owned()),
        ..Default::default()
    }
}

async fn row_count(store: &Store) -> usize {
    store
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list")
        .len()
}

fn entries(dir: &Path) -> Vec<PathBuf> {
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|e| e.expect("entry").path())
        .collect();
    names.sort();
    names
}

#[intent_test_macros::daemon_test]
async fn new_folder_is_created_without_git_and_marked_plain() {
    let h = harness().await;
    let parent = test_tempdir("intentd-plain-new-");
    let folder = parent.path().join("My Notes");

    let ws = h
        .services
        .create_workspace(plain(&folder), None)
        .await
        .expect("create")
        .workspace;

    assert!(folder.is_dir(), "the new folder exists");
    assert!(!folder.join(".git").exists(), "no git repository");
    assert!(
        entries(&folder).is_empty(),
        "nothing written into the folder"
    );
    assert!(ws.plain_folder);
    // The canonical folder is stored (§5.1 data safety).
    let canonical = std::fs::canonicalize(&folder).expect("canonical");
    assert_eq!(ws.repository_path.as_deref(), canonical.to_str());
    assert_eq!(
        ws.worktree_path, None,
        "the user's folder is never worktreePath"
    );
    assert_eq!(ws.checkout_mode, None);
    assert_eq!(ws.branch, "");
    assert_eq!(ws.base_ref, None);
    assert_eq!(ws.base_commit_sha, None);
    assert_eq!(ws.repository_owner, None);
    assert_eq!(ws.repository_name.as_deref(), Some("My Notes"));

    let wire = serde_json::to_value(&ws).expect("serialize");
    assert_eq!(wire["plainFolder"], serde_json::json!(true));
    assert_eq!(wire["branch"], serde_json::json!(""), "no git branch");
    for key in [
        "worktreePath",
        "checkoutMode",
        "baseRef",
        "prUrl",
        "prNumber",
    ] {
        assert!(wire.get(key).is_none(), "{key} is omitted: {wire}");
    }
    assert!(
        h.store.list_known_repos().await.expect("repos").is_empty(),
        "a plain folder is not registered as a repository"
    );
    let stored = h.store.get_workspace(&ws.id).await.expect("get");
    assert!(stored.plain_folder);
}

/// Caller-supplied `path`, blank `worktreePath` / `baseRef` and a
/// `baseCommitSha` never persist on a plain-folder row: the folder at
/// `repositoryPath` stays the agents' working directory (PR review).
#[intent_test_macros::daemon_test]
async fn plain_folder_row_drops_caller_path_and_git_fields() {
    let h = harness().await;
    let folder = test_tempdir("intentd-plain-fields-");
    let elsewhere = test_tempdir("intentd-plain-elsewhere-");
    let mut input = plain(folder.path());
    input.path = Some(elsewhere.path().to_string_lossy().into_owned());
    input.worktree_path = Some(String::new());
    input.base_ref = Some("  ".to_string());
    input.base_commit_sha = Some("0123456789abcdef0123456789abcdef01234567".to_string());

    let ws = h
        .services
        .create_workspace(input, None)
        .await
        .expect("create")
        .workspace;
    let stored = h.store.get_workspace(&ws.id).await.expect("get");
    for row in [&ws, &stored] {
        assert_eq!(row.path, None, "caller path is not persisted");
        assert_eq!(row.worktree_path, None);
        assert_eq!(row.base_ref, None);
        assert_eq!(row.base_commit_sha, None);
    }
    assert_eq!(
        crate::git_ops::worktree_path(&stored).as_deref(),
        Some(folder.path()),
        "agents resolve the plain folder, not the caller path"
    );
    assert_eq!(
        stored.effective_path(),
        folder.path().to_str(),
        "the effective path is the plain folder"
    );
}

#[intent_test_macros::daemon_test]
async fn existing_folder_gets_a_row_only_and_its_files_are_untouched() {
    let h = harness().await;
    let folder = test_tempdir("intentd-plain-existing-");
    std::fs::write(folder.path().join("draft.md"), "hello\n").expect("seed");
    let before = entries(folder.path());

    let ws = h
        .services
        .create_workspace(plain(folder.path()), None)
        .await
        .expect("create")
        .workspace;

    assert!(ws.plain_folder);
    assert_eq!(entries(folder.path()), before, "folder contents unchanged");
    assert_eq!(
        std::fs::read_to_string(folder.path().join("draft.md")).expect("read"),
        "hello\n"
    );
    assert!(!folder.path().join(".git").exists());
    assert_eq!(ws.worktree_path, None);
    assert_eq!(
        crate::git_ops::worktree_path(&ws).as_deref(),
        Some(folder.path()),
        "agents resolve the folder as their working directory"
    );
}

#[intent_test_macros::daemon_test]
async fn plain_folder_survives_restart_and_a_later_git_directory() {
    let h = harness().await;
    let folder = test_tempdir("intentd-plain-restart-");
    let id = h
        .services
        .create_workspace(plain(folder.path()), None)
        .await
        .expect("create")
        .workspace
        .id;
    std::fs::create_dir(folder.path().join(".git")).expect("git appears later");

    // A fresh store + services over the same database (daemon restart).
    let reopened = Store::open(&h.tmp.path).await.expect("reopen");
    let services = Services::new_isolated(reopened.clone())
        .with_workspaces_root(h.ws_root.path().to_path_buf());
    let ws = services.get_workspace(id.clone()).await.expect("get");
    assert!(ws.plain_folder, "still plain after restart and a new .git");
    let listed = reopened
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    assert!(listed.iter().any(|w| w.id == id && w.plain_folder));
}

#[intent_test_macros::daemon_test]
async fn rejected_paths_have_no_side_effects() {
    let h = harness().await;
    let scratch = test_tempdir("intentd-plain-reject-");

    let repo = scratch.path().join("repo");
    git2::Repository::init(&repo).expect("init");
    let file = scratch.path().join("file.txt");
    std::fs::write(&file, "x").expect("write");
    let inside_root = h.ws_root.path().join("not-allowed");
    let cases = [
        (repo.join("docs"), PlainFolderPathReason::InsideGitRepo),
        (repo.clone(), PlainFolderPathReason::InsideGitRepo),
        (
            inside_root.clone(),
            PlainFolderPathReason::InsideWorkspacesRoot,
        ),
        (
            h.ws_root.path().to_path_buf(),
            PlainFolderPathReason::InsideWorkspacesRoot,
        ),
        (file.clone(), PlainFolderPathReason::NotADirectory),
        (
            scratch.path().join("a").join("b"),
            PlainFolderPathReason::ParentMissing,
        ),
    ];
    for (path, want) in cases {
        let err = h
            .services
            .create_workspace(plain(&path), None)
            .await
            .expect_err("rejected");
        assert_eq!(err.code(), -32602, "{path:?}");
        match &err {
            Error::PlainFolderPathRejected(r) => {
                assert_eq!(r.reason, want, "{path:?}");
                assert_eq!(r.path, path.to_string_lossy());
                assert_eq!(
                    r.repository_root.is_some(),
                    want == PlainFolderPathReason::InsideGitRepo,
                    "{path:?}"
                );
            }
            other => panic!("{path:?}: {other:?}"),
        }
    }
    assert!(!repo.join("docs").exists());
    assert!(!inside_root.exists());
    assert!(!scratch.path().join("a").exists());
    assert_eq!(row_count(&h.store).await, 0, "no row persisted");
}

#[intent_test_macros::daemon_test]
async fn git_only_options_are_rejected_before_any_side_effect() {
    let h = harness().await;
    let parent = test_tempdir("intentd-plain-opts-");
    let folder = parent.path().join("new");
    let mut input = plain(&folder);
    input.is_new_repo = Some(true);
    let err = h
        .services
        .create_workspace(input, None)
        .await
        .expect_err("isNewRepo");
    assert!(
        matches!(&err, Error::PlainFolderInvalidParams(m) if m.contains("isNewRepo")),
        "{err:?}"
    );
    assert_eq!(err.code(), -32602);
    assert!(!folder.exists(), "no folder created");
    assert_eq!(row_count(&h.store).await, 0);

    let err = h
        .services
        .create_workspace(
            WorkspaceCreate {
                plain_folder: Some(true),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("missing path");
    assert!(matches!(err, Error::PlainFolderInvalidParams(_)), "{err:?}");
}

/// Without `plainFolder` a non-git `repositoryPath` keeps its legacy
/// row-only behavior: a named branch, no plain-folder flag.
#[intent_test_macros::daemon_test]
async fn legacy_non_git_create_is_unchanged() {
    let h = harness().await;
    let folder = test_tempdir("intentd-plain-legacy-");
    let ws = h
        .services
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(folder.path().to_string_lossy().into_owned()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create")
        .workspace;
    assert!(!ws.plain_folder);
    assert!(!ws.branch.is_empty(), "legacy rows keep a generated branch");
    let wire = serde_json::to_value(&ws).expect("serialize");
    assert!(wire.get("plainFolder").is_none(), "{wire}");
    assert!(wire.get("branch").is_some(), "{wire}");
}

/// `workspace.update` never writes `plainFolder`: the key fails the update
/// parse whatever its value, so nothing is written.
#[test]
fn workspace_update_rejects_plain_folder_key() {
    for value in [
        serde_json::json!(true),
        serde_json::json!(false),
        serde_json::Value::Null,
    ] {
        let err = serde_json::from_value::<WorkspaceUpdate>(
            serde_json::json!({ "title": "x", "plainFolder": value }),
        )
        .expect_err("plainFolder is create-only");
        assert!(err.to_string().contains("plainFolder"), "{err}");
    }
    let ok: WorkspaceUpdate =
        serde_json::from_value(serde_json::json!({ "title": "x" })).expect("parse");
    assert!(ok.plain_folder.is_none());
    let changes = serde_json::to_value(&ok).expect("serialize");
    assert!(changes.get("plainFolder").is_none(), "{changes}");
}
