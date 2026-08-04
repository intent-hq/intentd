//! `.git` metadata watch → git-status refresh triggers (monorepo#1397).
//!
//! The main recursive [`super::watcher::FileWatcher`] deliberately ignores
//! `.git` (objects churn would be noisy), so git operations run from an
//! outside terminal — `git commit`, `git checkout`, `git fetch` — never reach
//! the `file:*` → `changes:git-status` bridge. [`GitMetadataWatcher`] closes
//! that gap with a narrow, NON-recursive watch per workspace on the `.git`
//! metadata that those operations touch, and routes detections straight into
//! [`GitStatusRefresher::trigger`] (the same debounced recompute path as the
//! `file:*` bridge). No `file:*` events are ever emitted for `.git` paths.
//!
//! Watch roots: the `.git` directory itself and `.git/refs`, both
//! non-recursive. Watching the directories rather than the `HEAD`/`index`
//! files directly keeps the watch alive across git's atomic
//! write-lock-then-rename updates (an inode-level file watch dies with the
//! replaced inode on inotify). Events are then filtered to the metadata of
//! interest: `HEAD`, `index`, `packed-refs`, and anything under `refs/`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::WorkspaceId;
use notify::event::EventKind;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

use super::git_status_refresher::GitStatusRefresher;

/// A live `.git` metadata watch for one workspace. Holds only the `notify`
/// watcher — the OS subscription ends when it drops (clean-shutdown contract
/// shared with the other watchers); debouncing lives in the refresher.
pub struct GitMetadataWatcher {
    _watcher: RecommendedWatcher,
}

impl GitMetadataWatcher {
    /// Start the narrow `.git` metadata watch on `root`, routing detections
    /// to `refresher` for `workspace_id`. Returns `Ok(None)` when `root` has
    /// no `.git` directory (not a git repo, or a gitfile worktree) — a
    /// legitimate state, not an error.
    pub fn start(
        refresher: Arc<GitStatusRefresher>,
        workspace_id: WorkspaceId,
        root: PathBuf,
    ) -> notify::Result<Option<Self>> {
        // Canonicalize so the prefix strip works against the paths the OS
        // reports (macOS FSEvents resolves `/var/...` → `/private/var/...`).
        let root = std::fs::canonicalize(&root).unwrap_or(root);
        let git_dir = root.join(".git");
        if !git_dir.is_dir() {
            return Ok(None);
        }
        let filter_dir = git_dir.clone();
        // `GitStatusRefresher::trigger` is a synchronous unbounded send, so
        // the notify callback (off-runtime) can call it directly — no
        // channel/task of our own is needed.
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) => {
                    if is_mutation_kind(&event.kind)
                        && event
                            .paths
                            .iter()
                            .any(|p| is_git_metadata_path(&filter_dir, p))
                    {
                        refresher.trigger(workspace_id.clone());
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "git metadata watcher callback error; external git operations may be missed"
                    );
                }
            })?;
        watcher.watch(&git_dir, RecursiveMode::NonRecursive)?;
        // `refs` is a subdirectory, invisible to the non-recursive `.git`
        // watch; a second non-recursive watch covers loose-ref churn there.
        let refs_dir = git_dir.join("refs");
        if refs_dir.is_dir() {
            watcher.watch(&refs_dir, RecursiveMode::NonRecursive)?;
        }
        Ok(Some(Self { _watcher: watcher }))
    }
}

/// Event kinds that carry a mutation (mirrors the main watcher's `action_for`:
/// access/other kinds are dropped).
fn is_mutation_kind(kind: &EventKind) -> bool {
    !matches!(kind, EventKind::Access(_) | EventKind::Other)
}

/// Whether `abs` is one of the watched git metadata paths under `git_dir`:
/// `HEAD`, `index`, `packed-refs` (refs after `git pack-refs`/gc), or anything
/// under `refs/`. Everything else in `.git` (config, hooks, `COMMIT_EDITMSG`,
/// lock files, …) is filtered out.
fn is_git_metadata_path(git_dir: &Path, abs: &Path) -> bool {
    let Ok(rel) = abs.strip_prefix(git_dir) else {
        return false;
    };
    let mut components = rel.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return false;
    };
    match first.to_str() {
        Some("refs") => true,
        Some("HEAD") | Some("index") | Some("packed-refs") => components.next().is_none(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use intent_core::events::CHANGES_GIT_STATUS;
    use intent_core::{chief_workspace, BoxFuture, Error, Event, Result, Workspace, WorkspaceApi};
    use intent_store::Store;
    use notify::event::{AccessKind, CreateKind, ModifyKind};
    use tokio::time::{timeout, Instant};

    use super::super::bus::{EventBus, Subscription};
    use super::super::filter::SubscriptionFilter;
    use super::super::watcher::FileWatcher;
    use super::*;

    /// Self-cleaning temp directory (workspace worktrees).
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-gmw-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-gmw-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    /// Minimal [`WorkspaceApi`] over a fixed workspace list: the refresher
    /// resolves workspaces via `get_workspace` at refresh time.
    struct FakeApi {
        workspaces: Mutex<Vec<Workspace>>,
    }

    impl FakeApi {
        fn new(workspaces: Vec<Workspace>) -> Self {
            Self {
                workspaces: Mutex::new(workspaces),
            }
        }
    }

    impl WorkspaceApi for FakeApi {
        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let found = self
                .workspaces
                .lock()
                .unwrap()
                .iter()
                .find(|ws| ws.id == id)
                .cloned();
            Box::pin(async move {
                found.ok_or_else(|| Error::NotFound(format!("workspace {}", id.as_str())))
            })
        }
    }

    /// Workspace whose worktree resolves to `path`.
    fn test_workspace(id: &str, path: &Path) -> Workspace {
        let mut ws = chief_workspace();
        ws.id = WorkspaceId::from(id);
        ws.title = id.to_string();
        ws.worktree_path = Some(path.to_string_lossy().into_owned());
        ws
    }

    /// Real repo with a seed commit on `main` at `path`.
    fn init_repo(path: &Path) -> git2::Repository {
        use git2::{Repository, Signature};
        let repo = Repository::init(path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        {
            let tree = repo.find_tree(tree_oid).unwrap();
            let sig = Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
                .unwrap();
        }
        repo
    }

    async fn bus_and_subs() -> (TempDb, EventBus, Subscription, Subscription) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store);
        let status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });
        let file_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec!["file:*".to_string()],
            ..SubscriptionFilter::default()
        });
        (db, bus, status_sub, file_sub)
    }

    /// Await the next event for `ws_id` on `sub` within `overall`.
    async fn next_event(
        sub: &mut Subscription,
        ws_id: &WorkspaceId,
        overall: Duration,
    ) -> Option<Event> {
        let deadline = Instant::now() + overall;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => {
                    if let Some(ev) = batch.into_iter().find(|ev| &ev.workspace_id == ws_id) {
                        return Some(ev);
                    }
                }
                _ => return None,
            }
        }
    }

    #[test]
    fn metadata_path_filter_is_narrow() {
        let git_dir = Path::new("/ws/root/.git");
        assert!(is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/HEAD")
        ));
        assert!(is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/index")
        ));
        assert!(is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/packed-refs")
        ));
        assert!(is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/refs")
        ));
        assert!(is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/refs/heads/main")
        ));
        // Everything else in `.git` stays out (objects churn, config, locks…).
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/config")
        ));
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/COMMIT_EDITMSG")
        ));
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/objects/ab/cdef")
        ));
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/index.lock")
        ));
        // HEAD/index/packed-refs match only as direct children.
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/.git/logs/HEAD")
        ));
        // Outside the git dir entirely.
        assert!(!is_git_metadata_path(
            git_dir,
            Path::new("/ws/root/src/main.rs")
        ));
    }

    #[test]
    fn mutation_kind_filter_drops_access_and_other() {
        assert!(is_mutation_kind(&EventKind::Create(CreateKind::File)));
        assert!(is_mutation_kind(&EventKind::Modify(ModifyKind::Any)));
        assert!(is_mutation_kind(&EventKind::Any));
        assert!(!is_mutation_kind(&EventKind::Access(AccessKind::Any)));
        assert!(!is_mutation_kind(&EventKind::Other));
    }

    #[tokio::test]
    async fn non_git_root_starts_no_watch() {
        let (_db, bus, _status_sub, _file_sub) = bus_and_subs().await;
        let root = TempDir::new("nogit");
        let ws = test_workspace("ws-nogit", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(bus, api));

        let watcher = GitMetadataWatcher::start(refresher, ws.id.clone(), root.path.clone())
            .expect("start must not error on a non-git root");
        assert!(watcher.is_none(), "no `.git` dir → no watch");
    }

    #[tokio::test]
    async fn external_git_operation_triggers_status_refresh_without_file_events() {
        let (_db, bus, mut status_sub, mut file_sub) = bus_and_subs().await;
        let root = TempDir::new("repo");
        let repo = init_repo(&root.path);

        let ws = test_workspace("ws-repo", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(bus.clone(), api));
        // The main recursive watcher runs alongside to prove `.git` metadata
        // churn produces no `file:*` events (IGNORED_DIRS keeps `.git` out).
        let _file_watcher = FileWatcher::start(bus.clone(), ws.id.clone(), root.path.clone())
            .expect("start file watcher");
        let _watcher = GitMetadataWatcher::start(refresher, ws.id.clone(), root.path.clone())
            .expect("start git metadata watcher")
            .expect("git repo must gain a metadata watch");
        // Let the OS watches settle before mutating.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // External `git checkout`-style operation: only `.git` metadata moves
        // (HEAD rewrite; no worktree change).
        repo.set_head("refs/heads/other").unwrap();

        let ev = next_event(&mut status_sub, &ws.id, Duration::from_secs(10))
            .await
            .expect("external HEAD change must yield a changes:git-status event");
        assert_eq!(ev.event_type, CHANGES_GIT_STATUS);
        assert_eq!(ev.data["workspaceId"], ws.id.as_str());
        assert!(ev.data["status"].get("uncommittedCount").is_some());

        // No `file:*` event may surface for `.git`-internal paths.
        let file_ev = next_event(&mut file_sub, &ws.id, Duration::from_secs(1)).await;
        assert!(
            file_ev.is_none(),
            "`.git` metadata churn must not emit file:* events, got {file_ev:?}"
        );
    }

    #[tokio::test]
    async fn irrelevant_git_file_does_not_trigger_refresh() {
        let (_db, bus, mut status_sub, _file_sub) = bus_and_subs().await;
        let root = TempDir::new("quiet");
        let _repo = init_repo(&root.path);

        let ws = test_workspace("ws-quiet", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(bus.clone(), api));
        let _watcher = GitMetadataWatcher::start(refresher, ws.id.clone(), root.path.clone())
            .expect("start git metadata watcher")
            .expect("git repo must gain a metadata watch");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // `COMMIT_EDITMSG` lives in `.git` but is not watched metadata.
        std::fs::write(root.path.join(".git/COMMIT_EDITMSG"), "msg\n").unwrap();

        let ev = next_event(&mut status_sub, &ws.id, Duration::from_secs(3)).await;
        assert!(
            ev.is_none(),
            "non-metadata `.git` files must not trigger a refresh, got {ev:?}"
        );
    }
}
