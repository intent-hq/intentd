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
//! The detection rides the recursive stream [`SharedWatchHub`] already keeps
//! for the workspace root (the same stream the main watcher consumes), so no
//! `.git` streams of its own are created: it subscribes to that root and keeps
//! only the events whose paths are the metadata of interest — `HEAD`, `index`,
//! `packed-refs`, and anything under `refs/`. Path-based filtering (rather than
//! watching the `HEAD`/`index` files directly) is what keeps detection alive
//! across git's atomic write-lock-then-rename updates.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::WorkspaceId;
use notify::event::EventKind;
use tokio::task::JoinHandle;

use super::git_status_refresher::GitStatusRefresher;
use super::shared_watch::{SharedWatchHub, SubHandle};

/// A live `.git` metadata watch for one workspace: a subscription to the shared
/// workspace-root stream plus the filtering task. Both end when it drops
/// (clean-shutdown contract shared with the other watchers); debouncing lives
/// in the refresher.
pub struct GitMetadataWatcher {
    _sub: SubHandle,
    task: JoinHandle<()>,
}

impl Drop for GitMetadataWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl GitMetadataWatcher {
    /// Start the `.git` metadata detection for `root`, routing detections to
    /// `refresher` for `workspace_id`. Returns `None` when `root` has no `.git`
    /// directory (not a git repo, or a gitfile worktree) — a legitimate state,
    /// not an error.
    pub(super) fn start(
        hub: &Arc<SharedWatchHub>,
        refresher: Arc<GitStatusRefresher>,
        workspace_id: WorkspaceId,
        root: PathBuf,
    ) -> Option<Self> {
        // `subscribe` returns the canonical root it demuxes against, so the
        // prefix strip works against the paths the OS reports (macOS FSEvents
        // resolves `/var/...` → `/private/var/...`).
        let (sub, mut rx, root) = hub.subscribe(&root);
        let git_dir = root.join(".git");
        if !git_dir.is_dir() {
            return None;
        }
        let task = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if is_mutation_kind(&event.kind)
                    && event
                        .paths
                        .iter()
                        .any(|p| is_git_metadata_path(&git_dir, p))
                {
                    refresher.trigger(workspace_id.clone());
                }
            }
        });
        Some(Self { _sub: sub, task })
    }

    /// Await the shared watch on this workspace's root actually being
    /// established. Registration is deferred off the caller's thread
    /// (monorepo#1572), so tests must wait for it before mutating `.git`.
    #[cfg(test)]
    async fn wait_established(&self, timeout: std::time::Duration) {
        self._sub.wait_established(timeout).await;
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
        let refresher = Arc::new(GitStatusRefresher::start(
            bus,
            api,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));

        let watcher = GitMetadataWatcher::start(
            &SharedWatchHub::new(),
            refresher,
            ws.id.clone(),
            root.path.clone(),
        );
        assert!(watcher.is_none(), "no `.git` dir → no watch");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn external_git_operation_triggers_status_refresh_without_file_events() {
        // Serialized with the other real-watcher tests: these now ride shared
        // streams, and running several of them concurrently delays registration
        // enough to blow the delivery timeouts below.
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (_db, bus, mut status_sub, mut file_sub) = bus_and_subs().await;
        let root = TempDir::new("repo");
        let repo = init_repo(&root.path);

        let ws = test_workspace("ws-repo", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            api,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        let _watcher = GitMetadataWatcher::start(
            &SharedWatchHub::new(),
            refresher,
            ws.id.clone(),
            root.path.clone(),
        )
        .expect("git repo must gain a metadata watch");
        // Let the OS watch settle before mutating.
        _watcher.wait_established(crate::events::LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // External `git checkout`-style operation: only `.git` metadata moves
        // (HEAD rewrite; no worktree change). Retried because the shared stream
        // can begin delivering a little after registration lands, and a HEAD
        // rewrite that predates delivery leaves nothing to observe. Attempt
        // count sized so the total probe budget (attempts x 1500ms) reaches
        // `LIVENESS` — a pure-liveness bound (monorepo#1630).
        let attempts = crate::events::LIVENESS.as_millis() / 1500;
        let mut ev = None;
        for i in 0..attempts {
            let target = if i % 2 == 0 {
                "refs/heads/other"
            } else {
                "refs/heads/main"
            };
            repo.set_head(target).unwrap();
            ev = next_event(&mut status_sub, &ws.id, Duration::from_millis(1500)).await;
            if ev.is_some() {
                break;
            }
        }
        let ev = ev.expect("external HEAD change must yield a changes:git-status event");
        assert_eq!(ev.event_type, CHANGES_GIT_STATUS);
        assert_eq!(ev.data["workspaceId"], ws.id.as_str());
        assert!(ev.data["status"].get("uncommittedCount").is_some());

        // The metadata watcher routes through the refresher only — no
        // `file:*` event may surface for `.git`-internal paths. (The main
        // recursive watcher independently keeps `.git` out via IGNORED_DIRS,
        // covered by its own tests; no FileWatcher runs here to keep
        // real-watcher pressure low under nextest parallelism.)
        let file_ev = next_event(&mut file_sub, &ws.id, Duration::from_secs(1)).await;
        assert!(
            file_ev.is_none(),
            "`.git` metadata churn must not emit file:* events, got {file_ev:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn irrelevant_git_file_does_not_trigger_refresh() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (_db, bus, mut status_sub, _file_sub) = bus_and_subs().await;
        let root = TempDir::new("quiet");
        let _repo = init_repo(&root.path);

        let ws = test_workspace("ws-quiet", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            api,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        let _watcher = GitMetadataWatcher::start(
            &SharedWatchHub::new(),
            refresher,
            ws.id.clone(),
            root.path.clone(),
        )
        .expect("git repo must gain a metadata watch");
        _watcher.wait_established(crate::events::LIVENESS).await;
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
