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
