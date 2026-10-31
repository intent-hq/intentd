//! Plain-folder data safety (PROTOCOL §5.1 "Plain-folder workspaces": "The
//! folder is never deleted, moved or modified by Intent"). For both a new
//! folder the create made and an existing folder the user picked, every
//! lifecycle operation leaves the folder byte-identical: delete, archive,
//! the automatic archive, manual and automatic purge (`workspace.retire`),
//! and duplicate. Purge and duplicate are refused with the documented
//! plain-folder errors. The delete sweep never removes a plain folder that
//! sits where a `<root>/<id>` candidate points (intent-hq/intent#6229).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::{
    Error, Workspace, WorkspaceApi, WorkspaceCreate, WorkspaceId, WorkspaceListFilter,
    WorkspaceStatus,
};
use intent_store::Store;

use super::{TempDb, WorkspacesRoot};
use crate::test_support::retire_workspace::days_from_now;
use crate::test_support::test_tempdir;
use crate::{EventBus, Services, SettingsRegistry};

const SENTINEL: &[u8] = b"user data \xF0\x9F\x93\x9D\nline two\n";

/// Every entry under `dir`, recursively: file bytes, `None` for a directory.
type Snapshot = BTreeMap<PathBuf, Option<Vec<u8>>>;

fn snapshot(dir: &Path) -> Snapshot {
    fn walk(base: &Path, dir: &Path, out: &mut Snapshot) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            .map(|e| e.expect("entry").path())
            .collect();
        entries.sort();
        for path in entries {
            let rel = path.strip_prefix(base).expect("relative").to_path_buf();
            let meta = path.symlink_metadata().expect("metadata");
            if meta.is_dir() {
                out.insert(rel, None);
                walk(base, &path, out);
            } else {
                out.insert(rel, Some(std::fs::read(&path).expect("read file")));
            }
        }
    }
    assert!(dir.is_dir(), "{} is a directory", dir.display());
    let mut out = Snapshot::new();
    walk(dir, dir, &mut out);
    out
}

/// The folder still exists with exactly the contents it had before, and the
/// sentinel is byte-identical.
fn assert_untouched(folder: &Path, before: &Snapshot, op: &str) {
    assert!(folder.is_dir(), "{op}: the plain folder still exists");
    assert_eq!(
        std::fs::read(folder.join("notes.md")).expect("sentinel"),
        SENTINEL,
        "{op}: sentinel byte-identical"
    );
    assert_eq!(&snapshot(folder), before, "{op}: folder contents unchanged");
}

#[derive(Debug, Clone, Copy)]
enum Variant {
    /// The create made the folder (it did not exist).
    NewFolder,
    /// The user picked an existing non-git folder with files in it.
    ExistingFolder,
}

const VARIANTS: [Variant; 2] = [Variant::NewFolder, Variant::ExistingFolder];

struct Harness {
    _tmp: TempDb,
    _ws_root: WorkspacesRoot,
    root: PathBuf,
    store: Store,
    svc: Services,
    parent_dir: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let root = ws_root.path().to_path_buf();
        let bus = EventBus::new(store.clone());
        let svc = Services::new_isolated(store.clone())
            .with_workspaces_root(root.clone())
            .with_event_bus(bus);
        Self {
            _tmp: tmp,
            _ws_root: ws_root,
            root,
            store,
            svc,
            parent_dir: test_tempdir("intentd-plain-safety-"),
        }
    }

    fn parent(&self) -> &Path {
        self.parent_dir.path()
    }

    /// Create a plain-folder workspace for `variant` with the sentinel (and,
    /// for an existing folder, a nested file) inside; returns the row, the
    /// folder and its snapshot.
    async fn plain_workspace(&self, variant: Variant, name: &str) -> (Workspace, PathBuf) {
        let folder = self.parent().join(name);
        if matches!(variant, Variant::ExistingFolder) {
            std::fs::create_dir_all(folder.join("drafts")).expect("existing folder");
            std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
            std::fs::write(folder.join("drafts").join("a.txt"), b"draft").expect("nested");
        }
        let ws = self
            .svc
            .create_workspace(
                WorkspaceCreate {
                    plain_folder: Some(true),
                    repository_path: Some(folder.to_string_lossy().into_owned()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create plain folder")
            .workspace;
        assert!(ws.plain_folder);
        if matches!(variant, Variant::NewFolder) {
            std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        }
        (ws, folder)
    }

    async fn row_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .store
            .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
            .await
            .expect("list")
            .into_iter()
            .map(|w| w.id.0)
            .collect();
        ids.sort();
        ids
    }
}

/// `git init` the folder after create (the flag stays set, §5.1) with one
/// commit, so git-aware cleanup code would have something to act on.
fn git_appears_later(folder: &Path) {
    let repo = git2::Repository::init(folder).expect("git init");
    let mut cfg = repo.config().expect("config");
    cfg.set_str("user.name", "Tester").expect("name");
    cfg.set_str("user.email", "t@e.dev").expect("email");
    let mut index = repo.index().expect("index");
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .expect("add");
    index.write().expect("write index");
    let tree = repo
        .find_tree(index.write_tree().expect("tree"))
        .expect("find tree");
    let sig = git2::Signature::now("Tester", "t@e.dev").expect("sig");
    repo.commit(Some("HEAD"), &sig, &sig, "user commit", &tree, &[])
        .expect("commit");
}

fn assert_plain_retire_refusal(err: &Error, op: &str) {
    assert!(
        matches!(
            err,
            Error::RetireUnsupported {
                plain_folder: true,
                checkout_mode: None,
                ..
            }
        ),
        "{op}: retire-unsupported with plainFolder, got {err:?}"
    );
    assert_eq!(err.code(), -32602, "{op}");
    assert_eq!(
        err.to_string(),
        "workspace.retire is not available in plain-folder workspaces",
        "{op}"
    );
}

/// `workspace.delete` removes the row (and the daemon's own `<root>/<id>`
/// metadata) but never the folder.
#[intent_test_macros::daemon_test]
async fn delete_keeps_the_plain_folder() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let (ws, folder) = h.plain_workspace(variant, "Notes").await;
        let before = snapshot(&folder);
        h.svc.delete_workspace(ws.id.clone()).await.expect("delete");
        h.svc.store_tasks.drain_finite().await;
        assert!(
            h.store.get_workspace(&ws.id).await.is_err(),
            "{variant:?}: row deleted"
        );
        assert!(
            !h.root.join(ws.id.as_str()).exists(),
            "{variant:?}: the background sweep ran (daemon metadata removed)"
        );
        assert_untouched(&folder, &before, &format!("{variant:?} delete"));
    }
}

/// A `.git` that appeared later does not route the folder into git cleanup.
#[intent_test_macros::daemon_test]
async fn delete_keeps_the_plain_folder_after_git_appears() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let (ws, folder) = h.plain_workspace(variant, "Notes").await;
        git_appears_later(&folder);
        let before = snapshot(&folder);
        h.svc.delete_workspace(ws.id.clone()).await.expect("delete");
        h.svc.store_tasks.drain_finite().await;
        assert_untouched(&folder, &before, &format!("{variant:?} delete after git"));
    }
}

/// intent-hq/intent#6229: the delete sweep removes `<root>/<id>` for every
/// candidate root without checking what lives there. A plain folder that
/// sits exactly at a candidate (the boot root later moved to the folder's
/// parent, e.g. `INTENTD_WORKSPACES_DIR=~/Documents`, and the id matches
/// the folder name) must survive.
#[intent_test_macros::daemon_test]
async fn delete_sweep_skips_a_plain_folder_at_the_boot_root_candidate() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let id = WorkspaceId::from("my-notes");
        let folder = h.parent().join(id.as_str());
        std::fs::create_dir_all(&folder).expect("folder");
        std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        if matches!(variant, Variant::ExistingFolder) {
            std::fs::create_dir_all(folder.join(".workspace")).expect("lookalike");
            std::fs::write(folder.join(".workspace").join("mine.json"), b"{}").expect("file");
        }
        h.store
            .insert_workspace(&plain_row(&id, &folder))
            .await
            .expect("insert");
        let svc =
            Services::new_isolated(h.store.clone()).with_workspaces_root(h.parent().to_path_buf());
        let before = snapshot(&folder);
        svc.delete_workspace(id.clone()).await.expect("delete");
        svc.store_tasks.drain_finite().await;
        assert!(h.store.get_workspace(&id).await.is_err(), "row deleted");
        assert_untouched(&folder, &before, &format!("{variant:?} boot-root sweep"));
    }
}

/// Same as above for the `workspace.worktreesLocation` candidate.
#[intent_test_macros::daemon_test]
async fn delete_sweep_skips_a_plain_folder_at_the_worktrees_location_candidate() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let id = WorkspaceId::from("my-notes");
        let folder = h.parent().join(id.as_str());
        std::fs::create_dir_all(&folder).expect("folder");
        std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        if matches!(variant, Variant::ExistingFolder) {
            std::fs::write(folder.join("other.txt"), b"other").expect("file");
        }
        h.store
            .insert_workspace(&plain_row(&id, &folder))
            .await
            .expect("insert");
        let cfg = h.root.join("plain-safety.config.toml");
        let registry = Arc::new(SettingsRegistry::load(cfg).expect("registry"));
        registry
            .apply(&[(
                "workspace.worktreesLocation".to_string(),
                serde_json::json!(h.parent().to_string_lossy()),
            )])
            .expect("apply worktreesLocation");
        let svc = h.svc.clone().with_settings_registry(registry);
        let before = snapshot(&folder);
        svc.delete_workspace(id.clone()).await.expect("delete");
        svc.store_tasks.drain_finite().await;
        assert!(h.store.get_workspace(&id).await.is_err(), "row deleted");
        assert_untouched(&folder, &before, &format!("{variant:?} location sweep"));
    }
}

/// A plain-folder row as `workspace.create` persists it.
fn plain_row(id: &WorkspaceId, folder: &Path) -> Workspace {
    let ts = intent_core::now_iso();
    Workspace {
        id: id.clone(),
        title: "My notes".to_string(),
        branch: String::new(),
        created_at: ts.clone(),
        updated_at: ts,
        repository_path: Some(folder.to_string_lossy().into_owned()),
        repository_name: Some("my-notes".to_string()),
        plain_folder: true,
        ..Default::default()
    }
}

/// `workspace.archive` updates rows only.
#[intent_test_macros::daemon_test]
async fn archive_keeps_the_plain_folder() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let (ws, folder) = h.plain_workspace(variant, "Notes").await;
        git_appears_later(&folder);
        let before = snapshot(&folder);
        h.svc
            .archive_workspace(ws.id.clone(), None)
            .await
            .expect("archive");
        assert!(h.store.get_workspace(&ws.id).await.unwrap().archived);
        h.svc
            .unarchive_workspace(ws.id.clone())
            .await
            .expect("unarchive");
        h.svc.store_tasks.drain_finite().await;
        assert_untouched(&folder, &before, &format!("{variant:?} archive"));
    }
}

/// The automatic archive archives an idle plain folder (rows only) and
/// never touches the folder.
#[intent_test_macros::daemon_test]
async fn auto_archive_keeps_the_plain_folder() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let (ws, folder) = h.plain_workspace(variant, "Notes").await;
        let before = snapshot(&folder);
        let report = h
            .svc
            .sweep_auto_archive_after(Some(7), days_from_now(30))
            .await;
        assert_eq!(report.archived, 1, "{variant:?}: {report:?}");
        let row = h.store.get_workspace(&ws.id).await.unwrap();
        assert_eq!(row.status, WorkspaceStatus::Archived, "{variant:?}");
        h.svc.store_tasks.drain_finite().await;
        assert_untouched(&folder, &before, &format!("{variant:?} auto-archive"));
    }
}

/// Manual **Purge workspace folder** is refused for an Active and an
/// Archived plain folder with the documented error, and changes nothing —
/// also once a `.git` appeared in the folder.
#[intent_test_macros::daemon_test]
async fn manual_purge_is_refused_and_keeps_the_plain_folder() {
    for variant in VARIANTS {
        for git_later in [false, true] {
            let h = Harness::new().await;
            let (ws, folder) = h.plain_workspace(variant, "Notes").await;
            if git_later {
                git_appears_later(&folder);
            }
            let before = snapshot(&folder);
            let op = format!("{variant:?} git_later={git_later}");

            let err = h
                .svc
                .retire_workspace(ws.id.clone(), None)
                .await
                .expect_err("retire of an Active plain folder is refused");
            assert_plain_retire_refusal(&err, &op);
            let row = h.store.get_workspace(&ws.id).await.unwrap();
            assert_eq!(row.status, WorkspaceStatus::Active, "{op}: nothing changed");
            assert!(!row.archived, "{op}: not archived by the refused retire");

            h.svc
                .archive_workspace(ws.id.clone(), None)
                .await
                .expect("archive");
            let err = h
                .svc
                .retire_workspace(ws.id.clone(), None)
                .await
                .expect_err("retire of an Archived plain folder is refused");
            assert_plain_retire_refusal(&err, &op);
            let row = h.store.get_workspace(&ws.id).await.unwrap();
            assert_eq!(row.status, WorkspaceStatus::Archived, "{op}");
            assert!(!row.is_retired(), "{op}");
            h.svc.store_tasks.drain_finite().await;
            assert_untouched(&folder, &before, &op);
        }
    }
}

/// The automatic purge skips an archived plain folder, however long ago it
/// was archived.
#[intent_test_macros::daemon_test]
async fn automatic_purge_skips_the_plain_folder() {
    for variant in VARIANTS {
        for git_later in [false, true] {
            let h = Harness::new().await;
            let (ws, folder) = h.plain_workspace(variant, "Notes").await;
            if git_later {
                git_appears_later(&folder);
            }
            h.svc
                .archive_workspace(ws.id.clone(), None)
                .await
                .expect("archive");
            let before = snapshot(&folder);
            let op = format!("{variant:?} git_later={git_later}");
            let report = h
                .svc
                .sweep_auto_retire_after(Some(1), days_from_now(365))
                .await;
            assert_eq!(report.retired, 0, "{op}: {report:?}");
            assert_eq!(report.failed, 0, "{op}: {report:?}");
            assert_eq!(report.skipped, 1, "{op}: {report:?}");
            assert!(!h.store.get_workspace(&ws.id).await.unwrap().is_retired());
            h.svc.store_tasks.drain_finite().await;
            assert_untouched(&folder, &before, &op);
        }
    }
}

/// `workspace.duplicate` is refused with the plain-folder error: no new
/// row, no checkout, and nothing written into the folder (or, once a `.git`
/// appeared, into the user's repository — no worktree, no branch).
#[intent_test_macros::daemon_test]
async fn duplicate_is_refused_and_keeps_the_plain_folder() {
    for variant in VARIANTS {
        for git_later in [false, true] {
            let h = Harness::new().await;
            let (ws, folder) = h.plain_workspace(variant, "Notes").await;
            if git_later {
                git_appears_later(&folder);
            }
            let before = snapshot(&folder);
            let rows_before = h.row_ids().await;
            let root_before: Vec<_> = std::fs::read_dir(&h.root)
                .expect("root")
                .map(|e| e.expect("entry").file_name())
                .collect();
            let op = format!("{variant:?} git_later={git_later}");

            let err = h
                .svc
                .duplicate_workspace(ws.id.clone(), None)
                .await
                .expect_err("duplicate of a plain folder is refused");
            assert!(
                matches!(
                    err,
                    Error::PlainFolderUnsupported {
                        operation: "workspace.duplicate"
                    }
                ),
                "{op}: {err:?}"
            );
            assert_eq!(err.code(), -32602, "{op}: {err:?}");
            assert_eq!(
                err.to_string(),
                "workspace.duplicate is not available in plain-folder workspaces",
                "{op}"
            );
            assert_eq!(h.row_ids().await, rows_before, "{op}: no new row");
            let root_after: Vec<_> = std::fs::read_dir(&h.root)
                .expect("root")
                .map(|e| e.expect("entry").file_name())
                .collect();
            assert_eq!(root_after, root_before, "{op}: nothing provisioned");
            h.svc.store_tasks.drain_finite().await;
            assert_untouched(&folder, &before, &op);
        }
    }
}

/// Services whose boot root is `root` over `store`.
fn services_at(store: &Store, root: &Path) -> Services {
    Services::new_isolated(store.clone()).with_workspaces_root(root.to_path_buf())
}

/// A row without a checkout (no `worktreePath`), so its delete sweeps
/// `<root>/<id>` only.
fn git_less_row(id: &WorkspaceId) -> Workspace {
    let ts = intent_core::now_iso();
    Workspace {
        id: id.clone(),
        title: "Other".to_string(),
        branch: "main".to_string(),
        created_at: ts.clone(),
        updated_at: ts,
        ..Default::default()
    }
}

/// A repeated `workspace.delete` after the plain row is gone (the row read
/// is `NotFound`) still keeps the folder: the deleted plain folder is
/// remembered on the delete tombstone (PR review). The second delete runs on
/// a fresh `Services` (a drained task set admits no new sweep) with a
/// `worktreesLocation` canary that must be swept, proving the sweep ran.
#[intent_test_macros::daemon_test]
async fn repeated_delete_keeps_a_plain_folder_at_a_candidate() {
    let h = Harness::new().await;
    let id = WorkspaceId::from("my-notes");
    let folder = h.parent().join(id.as_str());
    std::fs::create_dir_all(&folder).expect("folder");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    h.store
        .insert_workspace(&plain_row(&id, &folder))
        .await
        .expect("insert");
    let before = snapshot(&folder);
    let first = services_at(&h.store, h.parent());
    first.delete_workspace(id.clone()).await.expect("delete");
    first.store_tasks.drain_finite().await;
    assert_untouched(&folder, &before, "first delete");

    let location = test_tempdir("intentd-plain-safety-canary-");
    let canary = location.path().join(id.as_str());
    std::fs::create_dir_all(canary.join(".workspace")).expect("canary");
    let registry = Arc::new(
        SettingsRegistry::load(h.root.join("plain-safety-repeat.config.toml")).expect("registry"),
    );
    registry
        .apply(&[(
            "workspace.worktreesLocation".to_string(),
            serde_json::json!(location.path().to_string_lossy()),
        )])
        .expect("apply worktreesLocation");
    let second = services_at(&h.store, h.parent()).with_settings_registry(registry);
    second
        .delete_workspace(id.clone())
        .await
        .expect("delete again");
    second.store_tasks.drain_finite().await;
    assert!(!canary.exists(), "the repeated delete's sweep ran");
    assert_untouched(&folder, &before, "repeated delete");
}

/// Deleting ANOTHER workspace whose id matches a plain folder's name (after
/// a root change) keeps that plain folder — live or already deleted.
#[intent_test_macros::daemon_test]
async fn deleting_another_workspace_keeps_a_plain_folder_at_its_candidate() {
    for plain_deleted_first in [false, true] {
        let h = Harness::new().await;
        let other = WorkspaceId::from("my-notes");
        let folder = h.parent().join(other.as_str());
        std::fs::create_dir_all(&folder).expect("folder");
        std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        let plain = WorkspaceId::from("plain-owner");
        h.store
            .insert_workspace(&plain_row(&plain, &folder))
            .await
            .expect("insert plain");
        if plain_deleted_first {
            h.svc
                .delete_workspace(plain.clone())
                .await
                .expect("delete plain");
            h.svc.store_tasks.drain_finite().await;
        }
        h.store
            .insert_workspace(&git_less_row(&other))
            .await
            .expect("insert other");
        let svc = services_at(&h.store, h.parent());
        let before = snapshot(&folder);
        svc.delete_workspace(other.clone())
            .await
            .expect("delete other");
        svc.store_tasks.drain_finite().await;
        assert!(
            h.store.get_workspace(&other).await.is_err(),
            "other deleted"
        );
        assert_untouched(
            &folder,
            &before,
            &format!("other delete, plain_deleted_first={plain_deleted_first}"),
        );
    }
}

/// Both overlap directions: a candidate inside the plain folder (the plain
/// folder is the boot root's own directory) is kept as well.
#[intent_test_macros::daemon_test]
async fn delete_sweep_keeps_a_candidate_inside_a_plain_folder() {
    let h = Harness::new().await;
    let folder = h.parent().to_path_buf();
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    let id = WorkspaceId::from("inner-dir");
    std::fs::create_dir_all(folder.join(id.as_str())).expect("user subfolder");
    std::fs::write(folder.join(id.as_str()).join("keep.txt"), b"keep").expect("file");
    h.store
        .insert_workspace(&plain_row(&id, &folder))
        .await
        .expect("insert");
    let svc = services_at(&h.store, &folder);
    let before = snapshot(&folder);
    svc.delete_workspace(id.clone()).await.expect("delete");
    svc.store_tasks.drain_finite().await;
    assert_untouched(&folder, &before, "candidate inside the plain folder");
}

/// `workspace.cleanup` removes `<candidate>/cache`: never inside a plain
/// folder that sits at a candidate.
#[intent_test_macros::daemon_test]
async fn cleanup_keeps_the_cache_dir_of_a_plain_folder_at_a_candidate() {
    let h = Harness::new().await;
    let id = WorkspaceId::from("my-notes");
    let folder = h.parent().join(id.as_str());
    std::fs::create_dir_all(folder.join("cache")).expect("user cache dir");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    std::fs::write(folder.join("cache").join("photo.jpg"), b"jpg").expect("file");
    h.store
        .insert_workspace(&plain_row(&id, &folder))
        .await
        .expect("insert");
    let svc = services_at(&h.store, h.parent());
    let before = snapshot(&folder);
    svc.cleanup_workspace(id.clone()).await.expect("cleanup");
    assert_untouched(&folder, &before, "workspace.cleanup");
}

/// Purging a GIT workspace whose `<location>/<id>` candidate is a plain
/// folder (after a `worktreesLocation` change) keeps that plain folder; the
/// git workspace's own checkout is still removed.
#[intent_test_macros::daemon_test]
async fn purging_a_git_workspace_keeps_a_plain_folder_at_its_candidate() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let wt = PathBuf::from(git_ws.worktree_path.clone().expect("worktree"));
    let location = test_tempdir("intentd-plain-safety-loc-");
    let folder = location.path().join(git_ws.id.as_str());
    std::fs::create_dir_all(&folder).expect("folder");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &folder))
        .await
        .expect("insert plain");
    let registry = Arc::new(
        SettingsRegistry::load(f.root.join("plain-safety-retire.config.toml")).expect("registry"),
    );
    registry
        .apply(&[(
            "workspace.worktreesLocation".to_string(),
            serde_json::json!(location.path().to_string_lossy()),
        )])
        .expect("apply worktreesLocation");
    let svc = f.svc.clone().with_settings_registry(registry);
    let before = snapshot(&folder);
    svc.archive_workspace(git_ws.id.clone(), None)
        .await
        .expect("archive");
    let retired = svc
        .retire_workspace(git_ws.id.clone(), None)
        .await
        .expect("retire the git workspace");
    assert_eq!(retired.status, WorkspaceStatus::Retired);
    svc.store_tasks.drain_finite().await;
    assert!(!wt.exists(), "the git checkout is still purged");
    assert_untouched(
        &folder,
        &before,
        "git purge with a plain folder at a candidate",
    );
}

/// The startup orphaned-trash sweep never enters a plain folder sitting at
/// `<root>/<x>`, even one holding a `*.deleting-*` directory.
#[intent_test_macros::daemon_test]
async fn orphaned_trash_sweep_keeps_a_plain_folder_under_the_root() {
    let h = Harness::new().await;
    let folder = h.parent().join("my-notes");
    std::fs::create_dir_all(folder.join("draft.deleting-1")).expect("lookalike");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    std::fs::write(folder.join("draft.deleting-1").join("x.txt"), b"x").expect("file");
    h.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &folder))
        .await
        .expect("insert");
    let svc = services_at(&h.store, h.parent());
    let before = snapshot(&folder);
    assert_eq!(svc.sweep_orphaned_worktree_trash().await, 0);
    assert_untouched(&folder, &before, "orphaned trash sweep");
}

/// Symlinked alias: the plain folder is recorded through a symlink and the
/// sweep candidate is the real path (and vice versa) — still kept.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn delete_sweep_keeps_a_plain_folder_reached_through_a_symlink() {
    for folder_via_link in [true, false] {
        let h = Harness::new().await;
        let real_parent = h.parent().join("real");
        std::fs::create_dir_all(&real_parent).expect("real parent");
        let link_parent = h.parent().join("link");
        std::os::unix::fs::symlink(&real_parent, &link_parent).expect("symlink");
        let id = WorkspaceId::from("my-notes");
        let folder = real_parent.join(id.as_str());
        std::fs::create_dir_all(&folder).expect("folder");
        std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        let (recorded, root) = if folder_via_link {
            (link_parent.join(id.as_str()), real_parent.clone())
        } else {
            (folder.clone(), link_parent.clone())
        };
        h.store
            .insert_workspace(&plain_row(&id, &recorded))
            .await
            .expect("insert");
        let svc = services_at(&h.store, &root);
        let before = snapshot(&folder);
        svc.delete_workspace(id.clone()).await.expect("delete");
        svc.store_tasks.drain_finite().await;
        assert_untouched(
            &folder,
            &before,
            &format!("symlink alias, folder_via_link={folder_via_link}"),
        );
    }
}

/// Settings registry with `workspace.worktreesLocation` set to `location`
/// (written as given, `..` and all).
fn location_registry(dir: &Path, location: &str) -> Arc<SettingsRegistry> {
    let registry = Arc::new(
        SettingsRegistry::load(dir.join("plain-safety-location.config.toml")).expect("registry"),
    );
    registry
        .apply(&[(
            "workspace.worktreesLocation".to_string(),
            serde_json::json!(location),
        )])
        .expect("apply worktreesLocation");
    registry
}

/// Verifier repro: `worktreesLocation` = `<T>/alias/..` where `alias` is a
/// symlink to `<T>/real/sub`. The kernel resolves `<T>/alias/../victim` to
/// `<T>/real/victim` (the plain folder), while a lexical `..` first would
/// see `<T>/victim`. Deleting another workspace with id `victim` and
/// `workspace.cleanup` on it must keep the plain folder (new and existing).
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn sweeps_resolve_symlink_dotdot_locations_like_the_kernel() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let t = h.parent().join("T");
        std::fs::create_dir_all(t.join("real").join("sub")).expect("real/sub");
        std::os::unix::fs::symlink(t.join("real").join("sub"), t.join("alias")).expect("alias");
        let folder = t.join("real").join("victim");
        if matches!(variant, Variant::ExistingFolder) {
            std::fs::create_dir_all(folder.join("cache")).expect("existing folder");
            std::fs::write(folder.join("cache").join("photo.jpg"), b"jpg").expect("file");
        }
        let plain = h
            .svc
            .create_workspace(
                WorkspaceCreate {
                    plain_folder: Some(true),
                    repository_path: Some(folder.to_string_lossy().into_owned()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create plain folder")
            .workspace;
        assert!(plain.plain_folder);
        std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
        std::fs::create_dir_all(folder.join("cache")).expect("cache dir");
        let other = WorkspaceId::from("victim");
        h.store
            .insert_workspace(&git_less_row(&other))
            .await
            .expect("insert other");
        let location = format!("{}/alias/..", t.display());
        let svc = h
            .svc
            .clone()
            .with_settings_registry(location_registry(&h.root, &location));
        let before = snapshot(&folder);
        svc.cleanup_workspace(other.clone()).await.expect("cleanup");
        assert_untouched(
            &folder,
            &before,
            &format!("{variant:?} cleanup via alias/.."),
        );
        svc.delete_workspace(other.clone())
            .await
            .expect("delete other");
        svc.store_tasks.drain_finite().await;
        assert_untouched(
            &folder,
            &before,
            &format!("{variant:?} delete via alias/.."),
        );
    }
}

/// A git workspace whose checkout lies inside a plain folder (the plain
/// folder is its `<root>/<id>`): purge is refused up front with the
/// plain-folder refusal and nothing is detached, renamed or removed; the
/// automatic purge skips it.
#[intent_test_macros::daemon_test]
async fn purge_refuses_a_checkout_nested_in_a_plain_folder() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let wt = PathBuf::from(git_ws.worktree_path.clone().expect("worktree"));
    let folder = wt.parent().expect("parent").to_path_buf();
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &folder))
        .await
        .expect("insert plain");
    f.svc
        .archive_workspace(git_ws.id.clone(), None)
        .await
        .expect("archive");
    let before = snapshot(&folder);
    let err = f
        .svc
        .retire_workspace(git_ws.id.clone(), None)
        .await
        .expect_err("purge of a checkout inside a plain folder is refused");
    assert!(
        matches!(
            err,
            Error::RetireUnsupported {
                plain_folder: true,
                ..
            }
        ),
        "{err:?}"
    );
    let report = f
        .svc
        .sweep_auto_retire_after(Some(1), days_from_now(365))
        .await;
    assert_eq!(report.retired, 0, "{report:?}");
    f.svc.store_tasks.drain_finite().await;
    assert!(!f
        .store
        .get_workspace(&git_ws.id)
        .await
        .unwrap()
        .is_retired());
    assert!(wt.is_dir(), "checkout kept");
    assert_untouched(&folder, &before, "purge with a nested checkout");
}

/// Delete of a git workspace whose `<root>/<id>/.workspace` metadata sibling
/// is a plain folder: the locked cleanup (trash rename, metadata removal,
/// empty-parent removal) must not touch it.
#[intent_test_macros::daemon_test]
async fn delete_keeps_a_plain_folder_at_the_metadata_sibling() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let wt = PathBuf::from(git_ws.worktree_path.clone().expect("worktree"));
    let folder = wt.parent().expect("parent").join(".workspace");
    std::fs::create_dir_all(&folder).expect("metadata dir");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &folder))
        .await
        .expect("insert plain");
    let before = snapshot(&folder);
    f.svc
        .delete_workspace(git_ws.id.clone())
        .await
        .expect("delete");
    f.svc.store_tasks.drain_finite().await;
    assert_untouched(&folder, &before, "metadata sibling");
}

/// Duplicate is refused before the workspaces parent is resolved: a
/// `worktreesLocation` inside the plain folder (missing) is never created,
/// and the error is the plain-folder refusal.
#[intent_test_macros::daemon_test]
async fn duplicate_refuses_before_creating_the_workspaces_parent() {
    for variant in VARIANTS {
        let h = Harness::new().await;
        let (ws, folder) = h.plain_workspace(variant, "Notes").await;
        let location = folder.join("new-child");
        let svc = h
            .svc
            .clone()
            .with_settings_registry(location_registry(&h.root, &location.to_string_lossy()));
        let before = snapshot(&folder);
        let err = svc
            .duplicate_workspace(ws.id.clone(), None)
            .await
            .expect_err("refused");
        assert!(
            matches!(
                err,
                Error::PlainFolderUnsupported {
                    operation: "workspace.duplicate"
                }
            ),
            "{variant:?}: {err:?}"
        );
        assert!(!location.exists(), "{variant:?}: location not created");
        assert_untouched(&folder, &before, &format!("{variant:?} duplicate"));
    }
}

/// A git `workspace.create` whose `worktreesLocation` lies inside a plain
/// folder is refused before anything is created there.
#[intent_test_macros::daemon_test]
async fn git_create_refuses_a_workspaces_parent_inside_a_plain_folder() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let folder = test_tempdir("intentd-plain-safety-parent-");
    std::fs::write(folder.path().join("notes.md"), SENTINEL).expect("sentinel");
    let plain = folder.path().to_path_buf();
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &plain))
        .await
        .expect("insert plain");
    let location = plain.join("worktrees");
    let svc = f
        .svc
        .clone()
        .with_settings_registry(location_registry(&f.root, &location.to_string_lossy()));
    let before = snapshot(&plain);
    let err = svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(f.repo.to_string_lossy().to_string()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("refused");
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    assert!(!location.exists(), "location not created");
    assert_untouched(&plain, &before, "git create inside a plain folder");
}

/// A git checkout whose `<root>/<id>` parent merely HOLDS an unrelated plain
/// folder (a sibling) is still cleaned up on delete; only the plain folder
/// is kept (PR review: the parent check must not be bidirectional).
#[intent_test_macros::daemon_test]
async fn delete_still_cleans_a_checkout_beside_a_plain_folder() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let wt = PathBuf::from(git_ws.worktree_path.clone().expect("worktree"));
    let folder = wt.parent().expect("parent").join("notes");
    std::fs::create_dir_all(&folder).expect("sibling folder");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &folder))
        .await
        .expect("insert plain");
    let before = snapshot(&folder);
    f.svc
        .delete_workspace(git_ws.id.clone())
        .await
        .expect("delete");
    f.svc.store_tasks.drain_finite().await;
    assert!(!wt.exists(), "the git checkout is still removed");
    assert_untouched(&folder, &before, "plain folder beside the checkout");
}

/// Reviving (unarchiving) a purged git workspace never provisions a checkout
/// inside a plain folder that now sits at its `<root>/<id>`.
#[intent_test_macros::daemon_test]
async fn revive_refuses_to_provision_inside_a_plain_folder() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let wt = PathBuf::from(git_ws.worktree_path.clone().expect("worktree"));
    let parent = wt.parent().expect("parent").to_path_buf();
    f.svc
        .retire_workspace(git_ws.id.clone(), None)
        .await
        .expect("retire");
    f.svc.store_tasks.drain_finite().await;
    let svc = services_at(&f.store, &f.root);
    std::fs::create_dir_all(&parent).expect("user folder at the old parent");
    std::fs::write(parent.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &parent))
        .await
        .expect("insert plain");
    let before = snapshot(&parent);
    assert!(
        svc.unarchive_workspace(git_ws.id.clone()).await.is_err(),
        "revive refused"
    );
    assert!(f
        .store
        .get_workspace(&git_ws.id)
        .await
        .unwrap()
        .is_retired());
    assert_untouched(&parent, &before, "revive into a plain folder");
}

/// Duplicating a GIT workspace whose `worktreesLocation` lies inside a plain
/// folder is refused before the location is created.
#[intent_test_macros::daemon_test]
async fn git_duplicate_refuses_a_workspaces_parent_inside_a_plain_folder() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let git_ws = crate::test_support::retire_workspace::create_workspace(&f).await;
    let folder = test_tempdir("intentd-plain-safety-dup-");
    let plain = folder.path().to_path_buf();
    std::fs::write(plain.join("notes.md"), SENTINEL).expect("sentinel");
    f.store
        .insert_workspace(&plain_row(&WorkspaceId::from("plain-owner"), &plain))
        .await
        .expect("insert plain");
    let location = plain.join("worktrees");
    let svc = f
        .svc
        .clone()
        .with_settings_registry(location_registry(&f.root, &location.to_string_lossy()));
    let before = snapshot(&plain);
    let err = svc
        .duplicate_workspace(git_ws.id.clone(), None)
        .await
        .expect_err("refused");
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    assert!(!location.exists(), "location not created");
    assert_untouched(&plain, &before, "git duplicate inside a plain folder");
}

/// With no plain folders at all, an unresolvable `worktreesLocation` keeps
/// its ordinary error instead of a misleading plain-folder one.
#[intent_test_macros::daemon_test]
async fn no_plain_folders_means_no_plain_folder_refusal() {
    let f = crate::test_support::retire_workspace::fixture().await;
    let svc = f
        .svc
        .clone()
        .with_settings_registry(location_registry(&f.root, "relative/worktrees"));
    let err = svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(f.repo.to_string_lossy().to_string()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("relative location is rejected");
    assert!(
        !err.to_string().contains("plain-folder"),
        "ordinary error, got: {err}"
    );
}

/// A plain folder that would CONTAIN a workspaces root (the boot root or a
/// configured `worktreesLocation`) is rejected at create with
/// `contains-workspaces-root`; an unrelated sibling is accepted.
#[intent_test_macros::daemon_test]
async fn create_rejects_a_plain_folder_containing_a_workspaces_root() {
    let h = Harness::new().await;
    let home = h.parent().join("home");
    let location = home.join("Projects").join("worktrees");
    std::fs::create_dir_all(&location).expect("location");
    let svc = h
        .svc
        .clone()
        .with_settings_registry(location_registry(&h.root, &location.to_string_lossy()));
    let boot_parent = h.root.parent().expect("root parent").to_path_buf();
    for path in [home.clone(), home.join("Projects"), boot_parent] {
        let err = svc
            .create_workspace(
                WorkspaceCreate {
                    plain_folder: Some(true),
                    repository_path: Some(path.to_string_lossy().into_owned()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("a folder holding a workspaces root is rejected");
        match &err {
            Error::PlainFolderPathRejected(r) => {
                assert_eq!(r.reason.as_str(), "contains-workspaces-root", "{path:?}");
            }
            other => panic!("{path:?}: {other:?}"),
        }
    }
    let sibling = home.join("Notes");
    svc.create_workspace(
        WorkspaceCreate {
            plain_folder: Some(true),
            repository_path: Some(sibling.to_string_lossy().into_owned()),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("an unrelated sibling is accepted");
}

/// A plain folder created through a symlinked alias is stored by its
/// canonical path, and a later unreadable alias ancestor cannot hide it from
/// the guard: deleting another workspace whose candidate is the real folder
/// keeps it (verifier: no lexical fallback for protected paths).
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn plain_folder_created_through_an_alias_stays_protected_when_the_alias_is_unreadable() {
    use std::os::unix::fs::PermissionsExt;
    let h = Harness::new().await;
    let t = h.parent().join("T");
    let accessible = t.join("accessible");
    let locked = t.join("locked");
    std::fs::create_dir_all(accessible.join("victim")).expect("victim");
    std::fs::create_dir_all(&locked).expect("locked");
    std::os::unix::fs::symlink(accessible.join("victim"), locked.join("link")).expect("link");
    let folder = accessible.join("victim");
    std::fs::write(folder.join("notes.md"), SENTINEL).expect("sentinel");
    let plain = h
        .svc
        .create_workspace(
            WorkspaceCreate {
                plain_folder: Some(true),
                repository_path: Some(locked.join("link").to_string_lossy().into_owned()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create through the alias")
        .workspace;
    let canonical = std::fs::canonicalize(&folder).expect("canonical");
    assert_eq!(
        plain.repository_path.as_deref(),
        canonical.to_str(),
        "the canonical folder is stored"
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    let other = WorkspaceId::from("victim");
    h.store
        .insert_workspace(&git_less_row(&other))
        .await
        .expect("insert other");
    let svc = services_at(&h.store, &accessible);
    let before = snapshot(&folder);
    svc.delete_workspace(other.clone())
        .await
        .expect("delete other");
    svc.store_tasks.drain_finite().await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod back");
    assert_untouched(&folder, &before, "alias behind an unreadable ancestor");
}
