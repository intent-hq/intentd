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
//! Two repository shapes are handled (monorepo#1663):
//!
//! - **Regular repository** (`root/.git` is a directory): the detection rides
//!   the recursive stream [`SharedWatchHub`] already keeps for the workspace
//!   root (the same stream the main watcher consumes), so no `.git` streams of
//!   its own are created: it subscribes to that root and keeps only the events
//!   whose paths are the metadata of interest — `HEAD`, `index`, `packed-refs`,
//!   and anything under `refs/`.
//! - **Linked worktree** (`root/.git` is a `gitdir:` pointer file — how intentd
//!   provisions workspaces by default): the metadata lives outside the root,
//!   split across two directories. The per-worktree gitdir
//!   (`<main>/.git/worktrees/<name>`: `HEAD`, `index`) is unique to the
//!   workspace and gets its own subscription. The repo's common dir
//!   (`<main>/.git`: `refs/`, `packed-refs`) is shared by every worktree of
//!   the repo, so it is watched ONCE per canonical common dir via the
//!   refcounted [`GitCommonDirWatches`] registry, and one ref change fans out
//!   [`GitStatusRefresher::trigger`] to every registered workspace (a
//!   fetch/commit in the shared repo changes status for all of its worktrees).
//!   The common dir's own `HEAD` is deliberately not matched — that is the
//!   main checkout's HEAD, not this workspace's.
//!
//! Path-based filtering (rather than watching the `HEAD`/`index` files
//! directly) is what keeps detection alive across git's atomic
//! write-lock-then-rename updates.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use intent_core::WorkspaceId;
use notify::event::EventKind;
use tokio::task::JoinHandle;

use super::git_status_refresher::GitStatusRefresher;
use super::shared_watch::{SharedWatchHub, SubHandle};

/// Poison-tolerant lock (one panicking task must not wedge the registry).
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A live `.git` metadata watch for one workspace: a subscription to the shared
/// stream carrying its `.git` metadata (the workspace root for a regular repo,
/// the per-worktree gitdir for a linked worktree) plus the filtering task, and
/// — for linked worktrees — a registration on the repo's shared common-dir
/// watch. All of it ends when the watcher drops (clean-shutdown contract shared
/// with the other watchers); debouncing lives in the refresher.
pub(crate) struct GitMetadataWatcher {
    _sub: SubHandle,
    task: JoinHandle<()>,
    /// Linked worktrees only: this workspace's registration on the shared
    /// common-dir watch; dropping it releases the fan-out slot (and the watch
    /// itself once the last workspace is gone).
    _common: Option<CommonDirGuard>,
}

impl Drop for GitMetadataWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl GitMetadataWatcher {
    /// Start the `.git` metadata detection for `root`, routing detections to
    /// `refresher` for `workspace_id`. Handles both a `.git` directory
    /// (regular repository) and a `.git` gitfile (linked worktree, resolved
    /// via `git2`); returns `None` when `root` is not a git repo — a
    /// legitimate state, not an error.
    pub(super) fn start(
        hub: &Arc<SharedWatchHub>,
        common_watches: &Arc<GitCommonDirWatches>,
        refresher: Arc<GitStatusRefresher>,
        workspace_id: WorkspaceId,
        root: PathBuf,
    ) -> Option<Self> {
        let dot_git = root.join(".git");
        if dot_git.is_dir() {
            // Regular repository. `subscribe` returns the canonical root it
            // demuxes against, so the prefix strip works against the paths the
            // OS reports (macOS FSEvents resolves `/var/...` →
            // `/private/var/...`).
            let (sub, mut rx, root) = hub.subscribe(&root);
            let git_dir = root.join(".git");
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
            return Some(Self {
                _sub: sub,
                task,
                _common: None,
            });
        }
        if !dot_git.is_file() {
            return None;
        }
        // Linked worktree: resolve the `gitdir:` pointer and its `commondir`
        // through git2 rather than parsing the files by hand.
        let (gitdir, common_dir) = match git2::Repository::open(&root) {
            Ok(repo) => (repo.path().to_path_buf(), repo.commondir().to_path_buf()),
            Err(e) => {
                tracing::warn!(
                    root = %root.display(),
                    error = %e,
                    "gitfile present but repository could not be opened; not watching git metadata"
                );
                return None;
            }
        };
        let (sub, mut rx, gitdir) = hub.subscribe(&gitdir);
        let ws_id = workspace_id.clone();
        let gitdir_refresher = Arc::clone(&refresher);
        let task = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if is_mutation_kind(&event.kind)
                    && event
                        .paths
                        .iter()
                        .any(|p| is_worktree_gitdir_metadata_path(&gitdir, p))
                {
                    gitdir_refresher.trigger(ws_id.clone());
                }
            }
        });
        let common = common_watches.register(hub, refresher, workspace_id, &common_dir);
        Some(Self {
            _sub: sub,
            task,
            _common: Some(common),
        })
    }

    /// Await every shared watch relevant to this workspace actually being
    /// established (the root/gitdir subscription, plus the common-dir one for
    /// linked worktrees). Registration is deferred off the caller's thread
    /// (monorepo#1572), so tests must wait for it before mutating `.git`.
    #[cfg(test)]
    async fn wait_established(&self, timeout: std::time::Duration) {
        self._sub.wait_established(timeout).await;
        if let Some(common) = &self._common {
            common.sub.wait_established(timeout).await;
        }
    }
}

/// Refcounted registry of shared common-dir watches, keyed by canonical common
/// dir. A repo's common dir (`<main>/.git`) is shared by every linked worktree
/// of that repo, so it gets ONE subscription + filter task regardless of how
/// many workspaces ride it ([`SharedWatchHub`] dedups the OS stream by root;
/// this registry dedups the subscription/filter layer and provides the fan-out
/// mapping). Owned by the [`super::registry::WatcherRegistry`] alongside the
/// hub; dropping it drops every entry, ending the subscriptions and tasks.
pub(super) struct GitCommonDirWatches {
    state: Mutex<HashMap<PathBuf, CommonDirEntry>>,
    /// Source of per-registration identity tokens (see [`Registration`]).
    next_token: AtomicU64,
}

/// One workspace's slot in a common-dir fan-out set: the refresher it
/// registered (so heterogeneous refreshers — tests — route correctly) plus the
/// identity token of the registration that owns the slot. The token is what
/// makes deregistration replacement-safe: when `start_watches` replaces a
/// workspace's watcher, the new guard registers (overwriting this slot with a
/// fresh token) BEFORE `HashMap::insert` drops the old watcher, and the stale
/// guard's drop must not remove the successor's registration.
struct Registration {
    token: u64,
    refresher: Arc<GitStatusRefresher>,
}

/// One shared common-dir watch: the subscription + filter task, and the
/// workspaces registered for fan-out.
struct CommonDirEntry {
    /// Held for RAII (dropping it ends the shared subscription); read only by
    /// tests awaiting establishment.
    _sub: Arc<SubHandle>,
    task: JoinHandle<()>,
    workspaces: Arc<Mutex<HashMap<WorkspaceId, Registration>>>,
}

impl Drop for CommonDirEntry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One workspace's registration on a shared common-dir watch. Dropping it
/// removes the workspace from the fan-out set; the last one out retires the
/// entry (subscription and task included).
struct CommonDirGuard {
    registry: Arc<GitCommonDirWatches>,
    key: PathBuf,
    ws_id: WorkspaceId,
    /// Identity of THIS registration; deregistration is a no-op unless the
    /// live slot still carries it (drop-ordering immunity on replacement).
    token: u64,
    /// Retained so [`GitMetadataWatcher::wait_established`] can await the
    /// shared common-dir subscription too.
    #[cfg(test)]
    sub: Arc<SubHandle>,
}

impl Drop for CommonDirGuard {
    fn drop(&mut self) {
        self.registry.deregister(&self.key, &self.ws_id, self.token);
    }
}

impl GitCommonDirWatches {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(0),
        })
    }

    /// Register `ws_id` on the shared watch for `common_dir`, starting the
    /// subscription + filter task if this is the first workspace on it. On a
    /// `refs/` or `packed-refs` mutation the task fans out
    /// `refresher.trigger` to EVERY registered workspace.
    fn register(
        self: &Arc<Self>,
        hub: &Arc<SharedWatchHub>,
        refresher: Arc<GitStatusRefresher>,
        ws_id: WorkspaceId,
        common_dir: &Path,
    ) -> CommonDirGuard {
        // Canonicalize for the map key so two worktrees of one repo agree on
        // the entry regardless of the path form their gitfiles carry. If
        // canonicalization fails (repo vanished mid-flight), the raw path is
        // the key; worktrees carrying different spellings of a vanished
        // common dir may then key separately — a graceful degradation, not a
        // correctness issue (each entry still watches and triggers on its
        // own, and every guard deregisters under the key it registered with).
        let key = std::fs::canonicalize(common_dir).unwrap_or_else(|_| common_dir.to_path_buf());
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let mut state = lock(&self.state);
        let entry = state.entry(key.clone()).or_insert_with(|| {
            let (sub, mut rx, common_dir) = hub.subscribe(&key);
            let workspaces: Arc<Mutex<HashMap<WorkspaceId, Registration>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let fan_out = Arc::clone(&workspaces);
            let task = tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    if is_mutation_kind(&event.kind)
                        && event
                            .paths
                            .iter()
                            .any(|p| is_common_dir_ref_path(&common_dir, p))
                    {
                        // Snapshot the targets so the lock is not held across
                        // the triggers.
                        let targets: Vec<_> = lock(&fan_out)
                            .iter()
                            .map(|(id, reg)| (id.clone(), Arc::clone(&reg.refresher)))
                            .collect();
                        for (id, refresher) in targets {
                            refresher.trigger(id);
                        }
                    }
                }
            });
            CommonDirEntry {
                _sub: Arc::new(sub),
                task,
                workspaces,
            }
        });
        lock(&entry.workspaces).insert(ws_id.clone(), Registration { token, refresher });
        #[cfg(test)]
        let sub = Arc::clone(&entry._sub);
        drop(state);
        CommonDirGuard {
            registry: Arc::clone(self),
            key,
            ws_id,
            token,
            #[cfg(test)]
            sub,
        }
    }

    /// Remove `ws_id` from the entry for `key` — but only while the live slot
    /// still carries `token`, so a stale guard (from a watcher replaced via
    /// the registry's `start_watches`) cannot deregister its successor; the
    /// last workspace out drops the entry, ending the subscription and filter
    /// task.
    fn deregister(&self, key: &Path, ws_id: &WorkspaceId, token: u64) {
        let mut state = lock(&self.state);
        let Some(entry) = state.get_mut(key) else {
            return;
        };
        let empty = {
            let mut workspaces = lock(&entry.workspaces);
            if workspaces.get(ws_id).is_some_and(|reg| reg.token == token) {
                workspaces.remove(ws_id);
            }
            workspaces.is_empty()
        };
        if empty {
            state.remove(key);
        }
    }

    /// Number of live shared common-dir watches — the dedup/refcount
    /// invariant under test (here and in the registry lifecycle tests).
    #[cfg(test)]
    pub(super) fn watch_count(&self) -> usize {
        lock(&self.state).len()
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

/// Whether `abs` is one of the per-worktree metadata paths under the worktree
/// gitdir (`<main>/.git/worktrees/<name>`): `HEAD` or `index` as direct
/// children, or anything under the per-worktree ref namespaces `refs/`
/// (`refs/bisect/`, `refs/worktree/`, `refs/rewritten/` —
/// gitrepository-layout(5)). Shared refs live in the common dir, watched
/// separately.
fn is_worktree_gitdir_metadata_path(gitdir: &Path, abs: &Path) -> bool {
    let Ok(rel) = abs.strip_prefix(gitdir) else {
        return false;
    };
    let mut components = rel.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return false;
    };
    match first.to_str() {
        Some("refs") => true,
        Some("HEAD") | Some("index") => components.next().is_none(),
        _ => false,
    }
}

/// Whether `abs` is one of the shared ref paths under the common dir
/// (`<main>/.git`): anything under `refs/`, or `packed-refs` as a direct
/// child. Deliberately NOT `HEAD` (the main checkout's, not this workspace's)
/// and not the per-worktree state under `worktrees/` (first component
/// `worktrees`, so it never matches here).
fn is_common_dir_ref_path(common_dir: &Path, abs: &Path) -> bool {
    let Ok(rel) = abs.strip_prefix(common_dir) else {
        return false;
    };
    let mut components = rel.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return false;
    };
    match first.to_str() {
        Some("refs") => true,
        Some("packed-refs") => components.next().is_none(),
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
    fn worktree_gitdir_path_filter_matches_head_index_and_per_worktree_refs() {
        let gitdir = Path::new("/main/.git/worktrees/wt");
        assert!(is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/HEAD")
        ));
        assert!(is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/index")
        ));
        // Per-worktree ref namespaces (gitrepository-layout(5)).
        assert!(is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/refs/bisect/bad")
        ));
        assert!(is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/refs/worktree/x")
        ));
        assert!(is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/refs/rewritten/y")
        ));
        // Shared refs live in the common dir; nothing else in the gitdir
        // matches.
        assert!(!is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/ORIG_HEAD")
        ));
        assert!(!is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/index.lock")
        ));
        assert!(!is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/wt/logs/HEAD")
        ));
        // Outside this worktree's gitdir (sibling worktree, common dir).
        assert!(!is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/worktrees/other/HEAD")
        ));
        assert!(!is_worktree_gitdir_metadata_path(
            gitdir,
            Path::new("/main/.git/HEAD")
        ));
    }

    #[test]
    fn common_dir_path_filter_matches_refs_but_not_head() {
        let common = Path::new("/main/.git");
        assert!(is_common_dir_ref_path(
            common,
            Path::new("/main/.git/refs/heads/main")
        ));
        assert!(is_common_dir_ref_path(common, Path::new("/main/.git/refs")));
        assert!(is_common_dir_ref_path(
            common,
            Path::new("/main/.git/packed-refs")
        ));
        // The common dir's HEAD is the main checkout's, not a worktree's.
        assert!(!is_common_dir_ref_path(
            common,
            Path::new("/main/.git/HEAD")
        ));
        // Per-worktree state under `worktrees/` never matches here.
        assert!(!is_common_dir_ref_path(
            common,
            Path::new("/main/.git/worktrees/wt/HEAD")
        ));
        assert!(!is_common_dir_ref_path(
            common,
            Path::new("/main/.git/index")
        ));
        assert!(!is_common_dir_ref_path(
            common,
            Path::new("/main/.git/config")
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
            &GitCommonDirWatches::new(),
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
            &GitCommonDirWatches::new(),
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

    /// Linked worktree of `repo` at `path` (`.git` is a gitfile pointer);
    /// returns the worktree's own repository handle.
    fn add_worktree(repo: &git2::Repository, name: &str, path: &Path) -> git2::Repository {
        repo.worktree(name, path, None).unwrap();
        git2::Repository::open(path).unwrap()
    }

    /// Regression (monorepo#1663): a workspace provisioned as a linked git
    /// worktree (`.git` is a `gitdir:` pointer file) must gain a metadata
    /// watch, and an external HEAD change in the worktree's own gitdir must
    /// yield `changes:git-status` — mirroring
    /// `external_git_operation_triggers_status_refresh_without_file_events`.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn external_head_change_in_linked_worktree_triggers_status_refresh() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (_db, bus, mut status_sub, mut file_sub) = bus_and_subs().await;
        let main_root = TempDir::new("wt-main");
        let main_repo = init_repo(&main_root.path);
        let wt_parent = TempDir::new("wt-linked");
        let wt_path = wt_parent.path.join("wt");
        let wt_repo = add_worktree(&main_repo, "wt", &wt_path);

        let ws = test_workspace("ws-worktree", &wt_path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            api,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        let watcher = GitMetadataWatcher::start(
            &SharedWatchHub::new(),
            &GitCommonDirWatches::new(),
            refresher,
            ws.id.clone(),
            wt_path.clone(),
        )
        .expect("linked worktree workspace must gain a metadata watch");
        watcher.wait_established(Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // External `git checkout`-style HEAD rewrite in the worktree's own
        // gitdir (`<main>/.git/worktrees/wt/HEAD`). Alternates between two
        // worktree-local branches (`main` is checked out in the main repo, so
        // git refuses it here). Retried for the same delivery-start race as
        // the regular-repo test above.
        let head_commit = wt_repo.head().unwrap().peel_to_commit().unwrap();
        wt_repo.branch("wt-alt", &head_commit, false).unwrap();
        let mut ev = None;
        for i in 0..20 {
            let target = if i % 2 == 0 {
                "refs/heads/wt-alt"
            } else {
                "refs/heads/wt"
            };
            wt_repo.set_head(target).unwrap();
            ev = next_event(&mut status_sub, &ws.id, Duration::from_millis(1500)).await;
            if ev.is_some() {
                break;
            }
        }
        let ev = ev.expect("external worktree HEAD change must yield a changes:git-status event");
        assert_eq!(ev.event_type, CHANGES_GIT_STATUS);
        assert_eq!(ev.data["workspaceId"], ws.id.as_str());

        // No `file:*` leakage for `.git`-internal paths, same contract as the
        // regular-repo test.
        let file_ev = next_event(&mut file_sub, &ws.id, Duration::from_secs(1)).await;
        assert!(
            file_ev.is_none(),
            "`.git` metadata churn must not emit file:* events, got {file_ev:?}"
        );
    }

    /// Two worktrees of one repo share ONE common-dir watch, a ref change in
    /// the shared repo fans out to both workspaces, and the watch retires only
    /// when the last workspace's watcher drops (monorepo#1663).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn shared_common_dir_ref_change_fans_out_to_all_worktrees() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (_db, bus, mut status_sub_a, _file_sub) = bus_and_subs().await;
        // Second status subscription so B's event cannot be discarded while
        // draining a batch for A.
        let mut status_sub_b = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });
        let main_root = TempDir::new("fan-main");
        let main_repo = init_repo(&main_root.path);
        let wt_parent = TempDir::new("fan-linked");
        let wt_a_path = wt_parent.path.join("wt-a");
        let wt_b_path = wt_parent.path.join("wt-b");
        add_worktree(&main_repo, "wt-a", &wt_a_path);
        add_worktree(&main_repo, "wt-b", &wt_b_path);

        let ws_a = test_workspace("ws-fan-a", &wt_a_path);
        let ws_b = test_workspace("ws-fan-b", &wt_b_path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws_a.clone(), ws_b.clone()]));
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            api,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        let hub = SharedWatchHub::new();
        let common = GitCommonDirWatches::new();
        let watcher_a = GitMetadataWatcher::start(
            &hub,
            &common,
            Arc::clone(&refresher),
            ws_a.id.clone(),
            wt_a_path.clone(),
        )
        .expect("worktree A must gain a metadata watch");
        let watcher_b = GitMetadataWatcher::start(
            &hub,
            &common,
            Arc::clone(&refresher),
            ws_b.id.clone(),
            wt_b_path.clone(),
        )
        .expect("worktree B must gain a metadata watch");
        assert_eq!(
            common.watch_count(),
            1,
            "one shared watch per canonical common dir"
        );
        watcher_a.wait_established(Duration::from_secs(10)).await;
        watcher_b.wait_established(Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // External ref churn in the SHARED repo (`<main>/.git/refs/heads/…`),
        // e.g. a commit/fetch in the main checkout. Retried for the same
        // delivery-start race as the other real-watcher tests.
        let head_commit = main_repo.head().unwrap().peel_to_commit().unwrap();
        let mut ev_a = None;
        for i in 0..20 {
            main_repo
                .branch(&format!("fan-{i}"), &head_commit, true)
                .unwrap();
            ev_a = next_event(&mut status_sub_a, &ws_a.id, Duration::from_millis(1500)).await;
            if ev_a.is_some() {
                break;
            }
        }
        let ev_a = ev_a.expect("shared ref change must refresh worktree A");
        assert_eq!(ev_a.event_type, CHANGES_GIT_STATUS);
        let ev_b = next_event(&mut status_sub_b, &ws_b.id, Duration::from_secs(5))
            .await
            .expect("shared ref change must fan out to worktree B");
        assert_eq!(ev_b.event_type, CHANGES_GIT_STATUS);

        // Refcount lifecycle: the first drop keeps the shared watch alive for
        // the survivor; the last drop retires it.
        drop(watcher_a);
        assert_eq!(
            common.watch_count(),
            1,
            "shared watch must survive while a workspace still rides it"
        );
        drop(watcher_b);
        assert_eq!(
            common.watch_count(),
            0,
            "last workspace out must retire the shared watch"
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
            &GitCommonDirWatches::new(),
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
