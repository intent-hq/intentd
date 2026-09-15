//! Shared `FSEvents` streams + in-process demux.
//!
//! Every watcher family used to own its own `notify` watcher, and each
//! `RecommendedWatcher` is one OS-level stream (on macOS: one
//! `FSEventStreamCreate` plus its `notify-rs fsevents loop` thread). That made
//! the steady-state stream count `O(workspaces × tiers)` — about five or six
//! per workspace once the file watcher, the `.git` metadata watch and the four
//! project-tier skills/specialists watches are counted — which is what loads
//! `fseventsd`.
//!
//! [`SharedWatchHub`] collapses that fan-out. Roots are assigned to groups (see
//! [`group_key`] for the per-OS keying), each group owns ONE `notify` watcher,
//! and every root in the group is added to it with a further `watch()` call
//! (`notify` appends to the same stream rather than creating another).
//! Consumers no longer create watchers at all: they
//! [`SharedWatchHub::subscribe`] to a root and receive the raw `notify` events
//! whose paths fall under it, demuxed in-process by path prefix. Filtering,
//! debouncing and publishing stay exactly where they were, so the `file:*` wire
//! shape is untouched.
//!
//! How coarse the grouping is depends on the backend's cost model:
//!
//! - **macOS (`FSEvents`)**: roots are grouped by their parent directory. The
//!   trade-off is that `notify`'s macOS backend rebuilds a group's stream on
//!   every `watch`/`unwatch` (`watch_inner` stops the run loop, appends the
//!   path and starts again), so registering or retiring one root briefly
//!   interrupts delivery for the group's other roots. That was already true
//!   per workspace, and the transitions that trigger it are rare workspace
//!   lifecycle events (create/open/close/delete, archive/unarchive) — none of
//!   them a steady-state path. Grouping any coarser would widen the blast
//!   radius of each rebuild for no fd savings (`FSEvents` streams are not fds).
//! - **Linux (inotify)**: ALL roots share a single global group. inotify has
//!   no rebuild trade-off — `watch`/`unwatch` on one instance is independent
//!   per root — while every `notify` watcher costs one inotify instance (one
//!   fd, capped by `fs.inotify.max_user_instances`, default 128). Workspace
//!   roots live under per-workspace parent directories, so parent-dir grouping
//!   made the instance count scale with the workspace count and exhausted the
//!   cap on multi-workspace hosts (intent-hq/intent#3708). One global group
//!   keeps it at one instance total; co-tenant isolation is preserved by the
//!   demux, which already narrows each event to the sink's own root.
//!
//! Registration never runs on the caller's thread — the reasoning of
//! intent-hq/monorepo#1572 applies unchanged: a `notify` registration can park
//! indefinitely and used to stall daemon startup. Each group therefore owns a
//! detached OS thread (a `register_off_thread`-shaped registrar, deliberately
//! not `spawn_blocking`, which the runtime waits for on shutdown) that builds
//! the watcher and serves `watch`/`unwatch` commands. Subscribing and
//! unsubscribing are pure in-memory bookkeeping plus a channel send, so they
//! never block a caller and a failed registration is logged and skipped without
//! affecting the other roots or the other groups. Watcher creation failure does
//! not kill a group either: the registrar keeps serving commands (settling
//! incoming registrations as failed so waiters never hang) and retries creation
//! with capped exponential backoff, re-registering the group's roots once it
//! succeeds. Because one `notify` watcher
//! is a single object, a group's registrations are serialized on its own
//! thread, so isolation is per group. On macOS that granularity is per parent
//! directory — a wedged backend is a property of the volume a group's roots
//! live on, and every other group (and the runtime) keeps making progress. On
//! Linux the single global group makes registration serialization global: one
//! slow or wedged registration (a huge tree on a slow or hung volume) delays
//! every other root's registration daemon-wide. That trade-off is deliberate:
//! registrations are rare workspace lifecycle events that already run off
//! every caller's thread, and steady-state event delivery is unaffected —
//! only initial coverage of a newly-registered root can lag behind a slow
//! neighbour.
//!
//! Roots are watched recursively by default; [`SharedWatchHub::subscribe_with`]
//! also takes [`RecursiveMode::NonRecursive`] for the per-daemon singletons
//! that used to own a watcher each — the `config.toml` directory watch and the
//! ancestor watches [`super::root_watch`] parks on the nearest existing parent
//! of a missing user-tier skills/specialists root (intent-hq/intent#4953). A
//! non-recursive sink is narrowed to the root itself and its direct children,
//! exactly what a dedicated non-recursive watch would have delivered, so
//! sharing the stream changes what the OS watches but not what a subscriber
//! sees. When the same root is wanted both ways, the OS watch is recursive —
//! the non-recursive sinks still see only their slice — and stays recursive
//! until the root's last subscriber drops; downgrading would mean an unwatch
//! (which on inotify strips nested roots' descriptors) for a case that does
//! not occur in practice. The same rule applies across roots: inotify keys its
//! descriptors per directory, not per `watch()` call, so a non-recursive root
//! nested under a recursive root (a missing project-tier skills root parked
//! on `<workspace>/.agents` under the recursive workspace root) shares the
//! ancestor's descriptors. It is registered recursively on the ancestor's
//! behalf and is not unwatched while the ancestor survives — otherwise the
//! shallow registration would stop the ancestor auto-watching directories
//! created under it, and the unwatch would strip the subtree from the ancestor
//! outright ([`covered_recursively`]).
//!
//! On Linux a recursive root is NOT handed to `notify` as one recursive
//! watch: its inotify backend walks the whole tree and registers one
//! descriptor per directory, `node_modules`/`target`/`vendor` included, which
//! is how an installed daemon re-accumulated ~496k of the 500k
//! `max_user_watches` within a day (intent-hq/intent#5026). The registrar
//! instead walks the tree itself, pruning [`super::watcher::NOISE_DIRS`] at
//! any depth, and registers one NON-recursive descriptor per surviving
//! directory ([`PrunedWatches`]). Because those descriptors carry no
//! recursion flag, the backend no longer auto-watches directories created
//! later; the group's event callback feeds `Create(Folder)` / rename-to
//! events back to the registrar, which walks the new directory the same way.
//! Nothing is pruned beneath a [`PRUNE_EXEMPT_DIRS`] component — `.git`,
//! `.intent`, `.augment`, `.agents`, `.claude` are trees a subscriber reads
//! through the workspace-root watch and filters itself, and a nested name
//! colliding with the noise list (`.git/refs/heads/build`,
//! `.intent/skills/build/`) must stay watched. The residual is a root that
//! itself lives under such a component: it is walked unpruned. macOS
//! (`FSEvents`) is untouched — its streams cost no per-directory resource.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::root_watch::{canonical_root, find_existing_ancestor};

/// Directory names beneath which the pruned walk never prunes (see the module
/// header): subscriber-owned trees whose contents are filtered downstream.
#[cfg(target_os = "linux")]
const PRUNE_EXEMPT_DIRS: &[&str] = &[".git", ".intent", ".augment", ".agents", ".claude"];

/// Event callback a group's watcher invokes with each raw result; boxed so the
/// watcher factory below can be swapped out.
type EventCallback = Box<dyn FnMut(notify::Result<notify::Event>) + Send>;

/// Builds one group's watcher. Production is [`notify::recommended_watcher`];
/// tests inject failing factories to exercise the creation-retry path.
type WatcherFactory =
    dyn Fn(EventCallback) -> notify::Result<Box<dyn Watcher + Send>> + Send + Sync;

/// Test seam selecting one `watch()` call to fail: the `fail_attempt`-th
/// (1-based) call whose path's file name is `name`. Attempts on other paths
/// pass through untouched, so the fault can target a RE-registration — e.g.
/// a later `watch()` of a root, after its first went live — while the
/// surrounding registrations succeed for real. The pruned walk issues one
/// `watch()` per directory, so a directory named `name` nested under another
/// root counts too. Linux-only alongside the tests that use it: survivor
/// re-registration exists only in the global inotify group.
#[cfg(all(test, target_os = "linux"))]
pub(crate) struct WatchFault {
    name: std::ffi::OsString,
    fail_attempt: usize,
    attempts: std::sync::atomic::AtomicUsize,
}

#[cfg(all(test, target_os = "linux"))]
impl WatchFault {
    pub(crate) fn nth(name: &str, fail_attempt: usize) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            fail_attempt,
            attempts: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// `watch()` calls seen so far on roots named `name`.
    pub(crate) fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    fn intercept(&self, path: &Path) -> notify::Result<()> {
        if path.file_name() != Some(self.name.as_os_str()) {
            return Ok(());
        }
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == self.fail_attempt {
            return Err(
                notify::Error::generic("injected watch failure").add_path(path.to_path_buf())
            );
        }
        Ok(())
    }
}

/// Real backend wrapped by a [`WatchFault`]; see
/// [`SharedWatchHub::with_watch_fault`].
#[cfg(all(test, target_os = "linux"))]
struct FaultyWatcher {
    inner: notify::RecommendedWatcher,
    fault: Arc<WatchFault>,
}

#[cfg(all(test, target_os = "linux"))]
impl Watcher for FaultyWatcher {
    fn new<F: notify::EventHandler>(handler: F, config: notify::Config) -> notify::Result<Self> {
        Ok(Self {
            inner: notify::RecommendedWatcher::new(handler, config)?,
            fault: WatchFault::nth("", 0),
        })
    }

    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        self.fault.intercept(path)?;
        self.inner.watch(path, mode)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        self.inner.unwatch(path)
    }

    fn kind() -> notify::WatcherKind {
        notify::RecommendedWatcher::kind()
    }
}

/// One demux destination: raw events whose paths fall under `root` — or, for
/// a non-recursive sink, are the root or one of its direct children (see
/// [`covers`]) — are cloned into `tx`.
struct Sink {
    id: u64,
    root: PathBuf,
    recursive: bool,
    tx: mpsc::UnboundedSender<notify::Event>,
}

/// The per-mode containment check: recursive means any descendant, non-
/// recursive means the root itself or a direct child (what a dedicated
/// non-recursive OS watch reports).
fn covers(root: &Path, recursive: bool, path: &Path) -> bool {
    if recursive {
        path.starts_with(root)
    } else {
        path == root || path.parent() == Some(root)
    }
}

/// Command to a group's registrar thread. Dropping every sender ends the
/// thread, which drops the watcher and tears the stream down. `Watch` carries
/// the [`Registration`] the registrar settles once the root is actually
/// registered. `AddDir` / `Forget` are fed by the group's own event callback
/// (see [`track_directories`]) to keep the pruned per-directory descriptors in
/// step with directories created and deleted after registration.
enum Cmd {
    Watch(PathBuf, RecursiveMode, Arc<Registration>),
    Unwatch(PathBuf),
    #[cfg(target_os = "linux")]
    AddDir(PathBuf),
    #[cfg(target_os = "linux")]
    Forget(PathBuf),
}

/// Handle to a registrar's command channel. The hub holds the strong count;
/// the watcher callback holds only a `Weak`, so retiring a group still closes
/// the channel and ends the thread even though the watcher (and its callback)
/// live on that thread.
type CmdSender = Arc<std::sync::mpsc::Sender<Cmd>>;

/// Outcome of one deferred `watcher.watch()`, shared between the registrar
/// thread and everyone waiting on it.
///
/// Failure is tracked distinctly from success because a root that failed to
/// register is dead but still refcounted: without the distinction a later
/// subscriber would join the existing `Root` entry, never re-send `Cmd::Watch`,
/// and silently receive a channel that can never deliver — with no recovery
/// until every subscriber drops. [`SharedWatchHub::subscribe`] retries such a
/// root instead.
///
/// A registration is also [`Self::reset`] and re-run while its subscribers
/// stay attached — widening to recursive, re-registering the survivors of a
/// recursive ancestor's unwatch. Those subscribers already saw `live` and are
/// parked on their channels, so a failure there cannot be left as a state
/// flag for them to notice: [`register`] closes the root's sinks instead, and
/// `was_live` is what tells a first-time failure (the subscriber is still
/// waiting on the registration and handles it) from a lost live watch.
#[derive(Default)]
struct Registration {
    /// [`REG_PENDING`] / [`REG_LIVE`] / [`REG_FAILED`].
    state: std::sync::atomic::AtomicU8,
    /// Set once the root has been live; survives [`Self::reset`].
    was_live: std::sync::atomic::AtomicBool,
}

const REG_PENDING: u8 = 0;
const REG_LIVE: u8 = 1;
const REG_FAILED: u8 = 2;

impl Registration {
    /// Whether the registrar has answered, either way. Waiters unblock here:
    /// nothing will ever arrive for a failed registration, so waiting past it
    /// would only stall.
    fn settled(&self) -> bool {
        self.state.load(Ordering::Acquire) != REG_PENDING
    }

    fn failed(&self) -> bool {
        self.state.load(Ordering::Acquire) == REG_FAILED
    }

    fn live(&self) -> bool {
        self.state.load(Ordering::Acquire) == REG_LIVE
    }

    #[cfg(test)]
    fn describe(&self) -> &'static str {
        match self.state.load(Ordering::Acquire) {
            REG_LIVE => "live",
            REG_FAILED => "failed",
            _ => "pending",
        }
    }

    fn settle(&self, live: bool) {
        if live {
            self.was_live.store(true, Ordering::Release);
        }
        self.state
            .store(if live { REG_LIVE } else { REG_FAILED }, Ordering::Release);
    }

    fn was_live(&self) -> bool {
        self.was_live.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.state.store(REG_PENDING, Ordering::Release);
    }
}

/// A watched root: how many subscribers reference it (two workspaces can
/// resolve to the same root), the mode the OS watch was requested in, and the
/// state of its deferred registration.
struct Root {
    subscribers: usize,
    /// Recursive as soon as any subscriber wants it recursive; never
    /// downgraded while subscribers remain (see the module header).
    recursive: bool,
    registration: Arc<Registration>,
}

/// Whether a recursive root in `roots` other than `path` itself is an ancestor
/// of `path`. inotify keys its descriptor table per directory, not per
/// `watch()` call, so a root nested under a recursive root shares the
/// ancestor's descriptors: registering it non-recursively would flip those
/// descriptors' recursion flag (newly created subdirectories under it stop
/// being auto-watched for the ancestor), and unwatching it would strip them
/// from the ancestor's coverage outright. Such a root is therefore always
/// registered in the ancestor's mode and never unwatched while the ancestor
/// survives. On Linux an ancestor whose pruned walk skips `path` (a root
/// under one of its noise subtrees, see [`prunes`]) holds no descriptor there
/// and so does NOT cover it: such a root is registered in its own mode and
/// unwatched normally, otherwise its descriptors would outlive its
/// subscribers until the ancestor retired. On macOS nested roots have
/// distinct parents and so live in distinct groups; this never fires there.
fn covered_recursively(roots: &HashMap<PathBuf, Root>, path: &Path) -> bool {
    roots.iter().any(|(root, state)| {
        state.recursive
            && root.as_path() != path
            && path.starts_with(root)
            && !pruned_under(root, path)
    })
}

#[cfg(target_os = "linux")]
fn pruned_under(root: &Path, path: &Path) -> bool {
    prunes(root, path)
}

#[cfg(not(target_os = "linux"))]
fn pruned_under(_root: &Path, _path: &Path) -> bool {
    false
}

/// The mode `path` must be registered in: recursive when its own subscribers
/// asked for that OR when [`covered_recursively`] by a co-tenant of the group.
fn os_mode(roots: &HashMap<PathBuf, Root>, path: &Path, recursive: bool) -> RecursiveMode {
    if recursive || covered_recursively(roots, path) {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    }
}

/// One shared stream: the registrar handle, the roots on it, and its demux
/// sinks.
struct Group {
    cmd: CmdSender,
    roots: HashMap<PathBuf, Root>,
    sinks: Arc<Mutex<Vec<Sink>>>,
    /// Set by the registrar once its OS watcher is actually created. The group
    /// exists — and retries creation — before that, so [`WatchHealth`] must
    /// not count it as an active stream until then.
    watcher_live: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Default)]
struct HubState {
    groups: HashMap<PathBuf, Group>,
    next_id: u64,
}

/// Owns the shared streams and the demux table. Created by the composition
/// root and shared between the [`super::registry::WatcherRegistry`] and the
/// `config.toml` watcher for the daemon's lifetime; dropping the last handle
/// drops every group, which ends the registrar threads and the streams.
pub struct SharedWatchHub {
    state: Mutex<HubState>,
    /// Builds each group's watcher; injectable so tests can fail creation.
    factory: Arc<WatcherFactory>,
}

/// Point-in-time aggregate of the hub's watch coverage, rendered into
/// `system.status` (intent-hq/intent#3708). `failed_roots > 0` means lost
/// coverage: those roots emit no file events until a retry succeeds — the
/// rejoin retry in [`SharedWatchHub::subscribe`] or the registrar's
/// creation-retry backoff — so surfacing the count is what turns the WARN-only
/// degradation into something a client can see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchHealthSnapshot {
    /// Shared streams (groups) whose OS watcher is actually created — one
    /// `notify` watcher each. A group stuck in the creation-retry loop is NOT
    /// counted, so this reads 0 while `total_roots > 0` in the
    /// watcher-creation-failure degradation this snapshot exists to surface.
    pub active_streams: usize,
    /// Roots currently requested across every group, whatever their state.
    pub total_roots: usize,
    /// Roots whose registration settled as FAILED (watcher creation failure,
    /// `ENOSPC`, a vanished directory). A still-pending registration is not a
    /// failure.
    pub failed_roots: usize,
}

/// Cloneable handle the composition root polls for [`WatchHealthSnapshot`]s.
///
/// Created before the watcher registry exists — registry init is backgrounded
/// so it cannot delay the UDS bind (monorepo#1581) — and attached to the hub
/// when the registry starts. `snapshot()` is therefore `None` (rendered as an
/// absent `fileWatch` field) until then, and again once the registry — and
/// with it the hub — is dropped at shutdown. Holds only a `Weak`, so the
/// handle never extends the hub's lifetime.
#[derive(Clone, Default)]
pub struct WatchHealth {
    hub: Arc<Mutex<std::sync::Weak<SharedWatchHub>>>,
}

impl WatchHealth {
    /// Point this handle at `hub`; called once by the registry at start.
    pub(super) fn attach(&self, hub: &Arc<SharedWatchHub>) {
        let mut slot = match self.hub.lock() {
            Ok(slot) => slot,
            Err(e) => e.into_inner(),
        };
        *slot = Arc::downgrade(hub);
    }

    /// Aggregate the hub's current coverage; `None` while unattached (the
    /// registry has not started yet) or after the hub is gone.
    #[must_use]
    pub fn snapshot(&self) -> Option<WatchHealthSnapshot> {
        let hub = {
            let slot = match self.hub.lock() {
                Ok(slot) => slot,
                Err(e) => e.into_inner(),
            };
            slot.upgrade()?
        };
        let state = match hub.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        let mut active_streams = 0;
        let mut total_roots = 0;
        let mut failed_roots = 0;
        for group in state.groups.values() {
            if group.watcher_live.load(Ordering::Acquire) {
                active_streams += 1;
            }
            for root in group.roots.values() {
                total_roots += 1;
                if root.registration.failed() {
                    failed_roots += 1;
                }
            }
        }
        Some(WatchHealthSnapshot {
            active_streams,
            total_roots,
            failed_roots,
        })
    }
}

/// A live subscription. Dropping it removes the sink and, when the root has no
/// subscribers left, unwatches it (and retires the group once it is empty).
pub(crate) struct SubHandle {
    hub: Arc<SharedWatchHub>,
    group: PathBuf,
    root: PathBuf,
    id: u64,
    registration: Arc<Registration>,
}

impl SubHandle {
    /// Await this subscription's registration settling, returning on timeout
    /// rather than waiting forever. Registration is deferred off the caller's
    /// thread (monorepo#1572), so a caller whose correctness depends on the
    /// watch existing — a catch-up flush, or a test about to mutate the watched
    /// tree — has to wait for it. (In-crate the only such caller is
    /// [`watch_tiers`], which reads the registration directly; the handle-level
    /// wrapper exists for the watcher tests.)
    #[cfg(test)]
    pub(crate) async fn wait_established(&self, timeout: std::time::Duration) {
        wait_settled(&self.registration, timeout).await;
    }

    /// Await the registration and report whether the OS watch is live. `false`
    /// covers both a settled failure and a registrar that has not answered
    /// within [`ESTABLISH_TIMEOUT`] — either way the subscriber cannot count on
    /// events arriving.
    pub(crate) async fn wait_live(&self) -> bool {
        self.established().await
    }

    /// [`Self::wait_live`] as an owned future, for a subscriber whose event
    /// loop runs on a spawned task while the handle itself stays with the
    /// owner that tears the subscription down.
    pub(crate) fn established(&self) -> impl Future<Output = bool> + Send + 'static {
        let registration = Arc::clone(&self.registration);
        async move {
            wait_settled(&registration, ESTABLISH_TIMEOUT).await;
            registration.live()
        }
    }

    /// Detach a [`RegistrationProbe`] for this subscription, so a test can
    /// await the watch going live without holding whatever lock guards the
    /// handle itself.
    #[cfg(test)]
    pub(crate) fn probe(&self) -> RegistrationProbe {
        let watcher_live = {
            let state = match self.hub.state.lock() {
                Ok(state) => state,
                Err(e) => e.into_inner(),
            };
            state
                .groups
                .get(&self.group)
                .map(|group| Arc::clone(&group.watcher_live))
                .unwrap_or_default()
        };
        RegistrationProbe {
            root: self.root.clone(),
            registration: Arc::clone(&self.registration),
            watcher_live,
        }
    }
}

/// Test-only view of one subscription's registration, detached from its
/// [`SubHandle`] / [`TierWatch`].
///
/// [`SubHandle::wait_established`] returns as soon as the registration
/// *settles*, failure included — the right contract for the creation-retry
/// tests, but the wrong sync point for a test about to mutate the tree: a
/// registration settled as failed while the group's watcher creation is being
/// retried (inotify instance exhaustion under full-suite parallelism,
/// intent-hq/intent#4845 / #4852) returns immediately, the test writes, and
/// the write lands before the retry re-registers the root — so the awaited
/// event never arrives and the test hangs into nextest's kill.
/// [`Self::wait_live`] waits for the watch to actually be live instead.
#[cfg(test)]
pub(crate) struct RegistrationProbe {
    root: PathBuf,
    registration: Arc<Registration>,
    watcher_live: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl RegistrationProbe {
    /// Await the watch being live, riding out a creation-retry: a
    /// registration the registrar settled as failed while it had no watcher
    /// is re-registered once creation succeeds, so keep waiting through that
    /// state. Panics — naming the root, the registration state and the OS
    /// watch limits — when the group's watcher is live yet the root's own
    /// `watch()` failed (nothing will retry that), or when `timeout` elapses,
    /// so a dead watch is diagnosed here rather than as a downstream
    /// "no event" hang.
    pub(crate) async fn wait_live(&self, timeout: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.registration.live() {
                return;
            }
            let watcher_live = self.watcher_live.load(Ordering::Acquire);
            assert!(
                !(watcher_live && self.registration.failed()),
                "shared watch registration failed for {} with the group's watcher live; {}",
                self.root.display(),
                os_watch_limits()
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared watch on {} not live within {timeout:?} (registration {}, group watcher live: {watcher_live}); {}",
                self.root.display(),
                self.registration.describe(),
                os_watch_limits()
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

/// How long a deferred catch-up waits for its registration before giving up and
/// flushing anyway: a wedged backend must not suppress the flush entirely.
const ESTABLISH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Poll `registration` until the registrar settles it or `timeout` elapses.
/// Returns either way — a failure settles it too, so the timeout only covers a
/// backend that never answers at all.
async fn wait_settled(registration: &Registration, timeout: std::time::Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !registration.settled() {
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

impl Drop for SubHandle {
    fn drop(&mut self) {
        // Take the sinks handle out from under `state`, then release `state`
        // before touching the sinks lock. `demux` holds the sinks lock while it
        // runs, so blocking on it here — with the hub-wide `state` mutex held —
        // would let one group's stream stall every other group's `subscribe`,
        // exactly the cross-group coupling the module header disclaims.
        let sinks = {
            let state = match self.hub.state.lock() {
                Ok(state) => state,
                Err(e) => e.into_inner(),
            };
            let Some(group) = state.groups.get(&self.group) else {
                return;
            };
            Arc::clone(&group.sinks)
        };
        if let Ok(mut sinks) = sinks.lock() {
            sinks.retain(|s| s.id != self.id);
        }

        let mut state = match self.hub.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        let Some(group) = state.groups.get_mut(&self.group) else {
            return;
        };
        let drop_root = match group.roots.get_mut(&self.root) {
            Some(root) => {
                root.subscribers -= 1;
                root.subscribers == 0
            }
            None => false,
        };
        if drop_root {
            let retired = group.roots.remove(&self.root);
            // Under a surviving recursive ancestor this root's descriptors ARE
            // the ancestor's (see `covered_recursively`): unwatching would
            // strip them from its coverage, and the ancestor's own unwatch
            // retires them later. Leave them in place.
            if !covered_recursively(&group.roots, &self.root) {
                let _ = group.cmd.send(Cmd::Unwatch(self.root.clone()));
                // Unwatching a recursive root removes the target AND every
                // descendant descriptor without per-root ref-counting (the
                // registrar's pruned-walk strip, mirroring `notify`'s own
                // recursive inotify unwatch),
                // so retiring a RECURSIVE root silently strips coverage from
                // any still-subscribed root nested under it (a Linux
                // global-group concern; on macOS nested roots under distinct
                // parents live in distinct groups). Re-register the survivors:
                // reset each one's registration and re-send `Cmd::Watch`,
                // which the registrar serves after the `Unwatch` above (the
                // channel is ordered). A non-recursive root owns exactly one
                // descriptor, so its unwatch touches no nested root and the
                // survivors — notably a root just promoted off the ancestor
                // watch `root_watch` parks on its parent — keep their live
                // registrations rather than being reset behind their owner's
                // successful `wait_live`. An ancestor watch a recursive
                // co-subscriber once widened stays recursive, so its retirement
                // DOES reset the promoted root; should that re-registration
                // fail, `register` closes the survivor's channels so the owner
                // can re-subscribe instead of parking on a dead watch.
                if retired.is_some_and(|root| root.recursive) {
                    for (nested, root) in &group.roots {
                        if nested.starts_with(&self.root) {
                            root.registration.reset();
                            let _ = group.cmd.send(Cmd::Watch(
                                nested.clone(),
                                os_mode(&group.roots, nested, root.recursive),
                                Arc::clone(&root.registration),
                            ));
                        }
                    }
                }
            }
        }
        if group.roots.is_empty() {
            // Dropping the group drops the command sender, so the registrar
            // thread returns and the stream goes away with its watcher.
            state.groups.remove(&self.group);
        }
    }
}

/// Which shared watcher `root` rides — the grouping decision the module header
/// documents.
///
/// On Linux every root maps to one global group ("/"): inotify `watch`/
/// `unwatch` is independent per root, so coarser grouping has no rebuild cost,
/// and each additional group would cost another inotify instance fd
/// (intent-hq/intent#3708). On macOS (and any other OS) roots group by parent
/// directory, bounding the blast radius of the `FSEvents` stream rebuild that
/// every `watch`/`unwatch` triggers there.
fn group_key(root: &Path) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        let _ = root;
        PathBuf::from("/")
    }
    #[cfg(not(target_os = "linux"))]
    {
        root.parent()
            .map_or_else(|| root.to_path_buf(), Path::to_path_buf)
    }
}

/// E2E seam: when set (to anything but `0`), every watcher creation fails, so
/// an out-of-process test can drive the daemon into the degraded state that
/// `system.status` must surface. Read per creation attempt, not once at
/// startup, purely for simplicity — production never sets it.
pub(super) const TEST_FAIL_WATCHER_CREATION_ENV: &str = "INTENTD_TEST_FAIL_WATCHER_CREATION";

impl SharedWatchHub {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Self::with_factory(Arc::new(|callback: EventCallback| {
            if std::env::var(TEST_FAIL_WATCHER_CREATION_ENV).is_ok_and(|v| v != "0") {
                return Err(notify::Error::generic(
                    "watcher creation failed by test seam",
                ));
            }
            notify::recommended_watcher(callback).map(|w| Box::new(w) as Box<dyn Watcher + Send>)
        }))
    }

    /// Hub with an injected watcher factory, so tests can fail creation
    /// deterministically. Production goes through [`Self::new`].
    fn with_factory(factory: Arc<WatcherFactory>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            factory,
        })
    }

    /// Hub whose watchers fail the `watch()` calls `fault` selects, so a test
    /// can fail one specific (re-)registration deterministically while every
    /// other call reaches the real backend.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn with_watch_fault(fault: &Arc<WatchFault>) -> Arc<Self> {
        let fault = Arc::clone(fault);
        Self::with_factory(Arc::new(move |callback: EventCallback| {
            let inner = notify::recommended_watcher(callback)?;
            Ok(Box::new(FaultyWatcher {
                inner,
                fault: Arc::clone(&fault),
            }) as Box<dyn Watcher + Send>)
        }))
    }

    /// Subscribe to raw events under `root` (recursively), joining (or
    /// starting) the shared stream for its group. Returns the canonical root
    /// the demux matches against, so callers can build their own path filters
    /// on the same form the OS reports.
    pub(super) fn subscribe(
        self: &Arc<Self>,
        root: &Path,
    ) -> (SubHandle, mpsc::UnboundedReceiver<notify::Event>, PathBuf) {
        self.subscribe_with(root, RecursiveMode::Recursive)
    }

    /// [`Self::subscribe`] with an explicit mode. A non-recursive subscription
    /// receives only events on `root` itself and its direct children; the OS
    /// watch it rides is recursive whenever any co-subscriber of the same root
    /// asked for that (see the module header).
    pub(crate) fn subscribe_with(
        self: &Arc<Self>,
        root: &Path,
        mode: RecursiveMode,
    ) -> (SubHandle, mpsc::UnboundedReceiver<notify::Event>, PathBuf) {
        let recursive = matches!(mode, RecursiveMode::Recursive);
        let root = match std::fs::canonicalize(root) {
            Ok(canonical) => canonical,
            Err(e) => {
                // The raw form will not prefix-match the canonical paths the OS
                // reports (macOS `/var` vs `/private/var`), so this subscription
                // may see nothing on the fast pass. Callers guard with an
                // existence check, so it means the root vanished underneath
                // them; log it so "watch registered but nothing arrives" is
                // diagnosable rather than silent.
                tracing::warn!(
                    root = %root.display(),
                    error = %e,
                    "watch root could not be canonicalized; demux may not match its events"
                );
                root.to_path_buf()
            }
        };
        let group_key = group_key(&root);
        let (tx, rx) = mpsc::unbounded_channel();

        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        let id = state.next_id;
        state.next_id += 1;
        let group = state.groups.entry(group_key.clone()).or_insert_with(|| {
            let sinks: Arc<Mutex<Vec<Sink>>> = Arc::new(Mutex::new(Vec::new()));
            let watcher_live = Arc::new(std::sync::atomic::AtomicBool::new(false));
            Group {
                cmd: spawn_registrar(
                    Arc::clone(&sinks),
                    group_key.clone(),
                    Arc::clone(&self.factory),
                    Arc::clone(&watcher_live),
                ),
                roots: HashMap::new(),
                sinks,
                watcher_live,
            }
        });
        if let Ok(mut sinks) = group.sinks.lock() {
            sinks.push(Sink {
                id,
                root: root.clone(),
                recursive,
                tx,
            });
        }
        let covered = covered_recursively(&group.roots, &root);
        let entry = group.roots.entry(root.clone()).or_insert_with(|| Root {
            subscribers: 0,
            recursive,
            registration: Arc::new(Registration::default()),
        });
        entry.subscribers += 1;
        // A root whose registration failed is dead but still refcounted, so a
        // new subscriber joining it would otherwise inherit a channel that can
        // never deliver, with no recovery until every subscriber drops. Retry
        // instead: a transient cause (the directory briefly missing) resolves,
        // and a persistent one just fails again and is logged again.
        let retry_failed = entry.subscribers > 1 && entry.registration.failed();
        // A recursive subscriber joining a root watched non-recursively widens
        // the OS watch. Replace rather than re-add: `notify` merges a repeated
        // `watch()` per backend in ways that differ (inotify merges masks and
        // walks the tree, `FSEvents` appends the path a second time), and an
        // explicit unwatch first makes the outcome the same everywhere. The
        // registrar serves the pair in order. A root already registered
        // recursively on a recursive ancestor's behalf has nothing to widen
        // (and its unwatch would strip the ancestor's descriptors).
        let widen = entry.subscribers > 1 && recursive && !entry.recursive && !covered;
        if retry_failed || widen {
            entry.registration.reset();
        }
        entry.recursive |= recursive;
        if widen {
            let _ = group.cmd.send(Cmd::Unwatch(root.clone()));
        }
        let registration = Arc::clone(&entry.registration);
        let register = entry.subscribers == 1 || retry_failed || widen;
        let root_recursive = entry.recursive;
        if register {
            let mode = os_mode(&group.roots, &root, root_recursive);
            let _ = group
                .cmd
                .send(Cmd::Watch(root.clone(), mode, Arc::clone(&registration)));
        }
        drop(state);

        (
            SubHandle {
                hub: Arc::clone(self),
                group: group_key,
                root: root.clone(),
                id,
                registration,
            },
            rx,
            root,
        )
    }

    /// Number of live shared streams. The consolidation invariant under test:
    /// this stays a handful regardless of the workspace count.
    #[cfg(test)]
    pub(super) fn stream_count(&self) -> usize {
        match self.state.lock() {
            Ok(state) => state.groups.len(),
            Err(e) => e.into_inner().groups.len(),
        }
    }

    /// Which shared stream `root` rides, as the key of the group whose root
    /// table holds a subscription for it, or `None` when no such entry exists.
    /// Group membership only: a root stays in the table while its registration
    /// is pending or failed, so `Some(key)` says nothing about a live OS watch
    /// — that is [`Self::root_established`]'s job. The per-group consolidation
    /// invariant under test: sibling roots resolve to the same key regardless
    /// of how many other groups the hub supervises (the count differs per OS —
    /// see [`group_key`]). Path is canonicalized to match the form
    /// [`Self::subscribe`] keys roots by.
    #[cfg(test)]
    pub(super) fn stream_for_root(&self, root: &Path) -> Option<PathBuf> {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        state
            .groups
            .iter()
            .find(|(_, g)| g.roots.contains_key(&root))
            .map(|(key, _)| key.clone())
    }

    /// Registration state of one root: `None` when nothing watches it,
    /// `Some(false)` while the watch request is still pending, `Some(true)` once
    /// the registrar has answered (either way — a failed registration will never
    /// settle further, so waiters must not park on it). Path is canonicalized to
    /// match the form [`Self::subscribe`] keys roots by.
    #[cfg(test)]
    pub(super) fn root_established(&self, root: &Path) -> Option<bool> {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        state
            .groups
            .values()
            .find_map(|g| g.roots.get(&root))
            .map(|r| r.registration.settled())
    }

    /// Human-readable registration state of one root for test diagnostics:
    /// `pending` / `live` / `failed`, or `unwatched` when nothing watches it.
    #[cfg(test)]
    pub(super) fn root_registration_state(&self, root: &Path) -> &'static str {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        state
            .groups
            .values()
            .find_map(|g| g.roots.get(&root))
            .map_or("unwatched", |r| r.registration.describe())
    }

    /// Await every currently-requested root being registered with the OS.
    /// Registration is deferred off the caller's thread (monorepo#1572), so
    /// tests that drive the hub indirectly (through the registry) need this
    /// before mutating a watched tree.
    ///
    /// `expect_roots` guards the race this would otherwise have: a caller
    /// waiting right after publishing a lifecycle event can arrive before the
    /// registry has subscribed at all, when "nothing is pending" is trivially
    /// true. The wait does not finish until at least that many roots exist.
    #[cfg(test)]
    pub(super) async fn wait_all_established(
        &self,
        expect_roots: usize,
        timeout: std::time::Duration,
    ) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let ready = {
                let state = match self.state.lock() {
                    Ok(state) => state,
                    Err(e) => e.into_inner(),
                };
                let roots: Vec<_> = state
                    .groups
                    .values()
                    .flat_map(|g| g.roots.values())
                    .collect();
                roots.len() >= expect_roots && roots.iter().all(|r| r.registration.settled())
            };
            if ready || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

/// First delay before retrying a failed watcher creation; doubles per failure.
/// Shared with [`super::root_watch`]'s registration retry so both watch
/// families recover from the same transient failure on the same schedule.
pub(crate) const CREATE_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Ceiling for the creation-retry backoff, so a persistent failure (fd
/// exhaustion, intent-hq/intent#3708) keeps probing about once a minute.
pub(crate) const CREATE_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(60);

/// Start a group's registrar: a DETACHED OS thread that builds the shared
/// watcher and then serves `watch`/`unwatch` commands. Detached rather than
/// `spawn_blocking` for the intent-hq/monorepo#1572 reason — the runtime waits
/// for the blocking pool on shutdown, so a registration parked inside a wedged
/// backend would turn a startup stall into a shutdown hang. Failures are logged
/// and skipped; one bad root never stops the others. Watcher creation failure
/// does not end the thread either — see [`build_watcher_serving`].
fn spawn_registrar(
    sinks: Arc<Mutex<Vec<Sink>>>,
    group: PathBuf,
    factory: Arc<WatcherFactory>,
    watcher_live: Arc<std::sync::atomic::AtomicBool>,
) -> CmdSender {
    let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
    let tx = Arc::new(tx);
    #[cfg(target_os = "linux")]
    let feedback = Arc::downgrade(&tx);
    std::thread::spawn(move || {
        let demux_sinks = Arc::clone(&sinks);
        let make = move || {
            let sinks = Arc::clone(&demux_sinks);
            #[cfg(target_os = "linux")]
            let feedback = feedback.clone();
            factory(Box::new(
                move |res: notify::Result<notify::Event>| match res {
                    Ok(event) => {
                        #[cfg(target_os = "linux")]
                        track_directories(&feedback, &event);
                        demux(&sinks, &event);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "shared watcher callback error; events may be missed");
                    }
                },
            ))
        };
        let Some((mut watcher, pending)) = build_watcher_serving(&rx, make, &group) else {
            // Every sender dropped: the group was retired before a watcher
            // could be built.
            return;
        };
        let mut pruned = PrunedWatches::default();
        for (root, mode, registration) in pending {
            register(
                watcher.as_mut(),
                &mut pruned,
                &root,
                mode,
                &registration,
                &sinks,
            );
        }
        // Flag the stream live only now: the group exists (and this thread
        // retries creation) before any OS watcher does, and `WatchHealth`
        // must not count a stream that is not there yet.
        watcher_live.store(true, Ordering::Release);
        while let Ok(cmd) = rx.recv() {
            match cmd {
                Cmd::Watch(root, mode, registration) => {
                    register(
                        watcher.as_mut(),
                        &mut pruned,
                        &root,
                        mode,
                        &registration,
                        &sinks,
                    );
                }
                Cmd::Unwatch(root) => {
                    if let Err(e) = pruned.unwatch(watcher.as_mut(), &root) {
                        tracing::debug!(root = %root.display(), error = %e, "shared watch removal failed");
                    }
                }
                #[cfg(target_os = "linux")]
                Cmd::AddDir(dir) => pruned.add_dir(watcher.as_mut(), &dir),
                #[cfg(target_os = "linux")]
                Cmd::Forget(dir) => pruned.forget(&dir),
            }
        }
    });
    tx
}

/// Registrar-side descriptor bookkeeping for the pruned recursive
/// registration (module header, intent-hq/intent#5026). Lives on the registrar
/// thread next to the watcher it drives; a no-op shell off Linux, where the
/// backend's own recursive watch is used unchanged.
#[derive(Default)]
struct PrunedWatches {
    /// Every directory currently holding a descriptor through a pruned walk,
    /// across all roots. inotify keys descriptors per directory, so two
    /// nested roots share entries here exactly as they share descriptors.
    #[cfg(target_os = "linux")]
    dirs: std::collections::BTreeSet<PathBuf>,
    /// Roots registered through a pruned walk — the ones whose unwatch strips
    /// their whole subtree, and the ancestors a newly created directory is
    /// checked against before it is walked.
    #[cfg(target_os = "linux")]
    roots: std::collections::HashSet<PathBuf>,
}

impl PrunedWatches {
    /// One `watch()` of `root` in `mode`: on Linux a recursive root becomes
    /// one non-recursive descriptor per directory of the pruned walk;
    /// everything else is the backend's own watch.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn watch(
        &mut self,
        watcher: &mut dyn Watcher,
        root: &Path,
        mode: RecursiveMode,
    ) -> notify::Result<()> {
        #[cfg(target_os = "linux")]
        if matches!(mode, RecursiveMode::Recursive) {
            return self.watch_pruned(watcher, root);
        }
        watcher.watch(root, mode)
    }

    /// Undo [`Self::watch`]. A root registered through a pruned walk releases
    /// every descriptor under it — nested roots' included, without
    /// ref-counting, exactly like `notify`'s recursive inotify unwatch the hub
    /// already re-registers survivors for (`SubHandle::drop`). Any other root
    /// releases its single descriptor.
    #[cfg_attr(not(target_os = "linux"), expect(clippy::unused_self))]
    fn unwatch(&mut self, watcher: &mut dyn Watcher, root: &Path) -> notify::Result<()> {
        #[cfg(target_os = "linux")]
        if self.roots.remove(root) {
            self.roots.retain(|r| !r.starts_with(root));
            let mut stripped = Vec::new();
            self.dirs.retain(|dir| {
                let under = dir.starts_with(root);
                if under {
                    stripped.push(dir.clone());
                }
                !under
            });
            for dir in &stripped {
                // A directory deleted since it was walked has no descriptor
                // left to remove; that is the expected outcome, not a fault.
                if let Err(e) = watcher.unwatch(dir) {
                    tracing::trace!(dir = %dir.display(), error = %e, "pruned descriptor already gone");
                }
            }
            return Ok(());
        }
        watcher.unwatch(root)
    }

    #[cfg(target_os = "linux")]
    fn watch_pruned(&mut self, watcher: &mut dyn Watcher, root: &Path) -> notify::Result<()> {
        self.roots.insert(root.to_path_buf());
        let descriptors = self.add_tree(watcher, root, root)?;
        tracing::warn!(
            root = %root.display(),
            descriptors,
            "shared watch registered; inotify descriptors held for this root after pruning noise dirs"
        );
        Ok(())
    }

    /// Walk `start` (which lies under `root`, whose prune rules apply) and
    /// register one non-recursive descriptor per surviving directory,
    /// returning how many. `start` itself is registered first and outside the
    /// walk, so a missing or unreadable `start` fails with the backend's own
    /// error (the walk would merely yield nothing for it) and the caller
    /// settles the root as failed, as `notify`'s own watch would. The walk
    /// also aborts on `MaxFilesWatch` anywhere — coverage is genuinely lost;
    /// the descriptors added so far stay tracked so an unwatch still releases
    /// them. Any other per-directory failure (vanished or unreadable
    /// directory) is skipped.
    #[cfg(target_os = "linux")]
    fn add_tree(
        &mut self,
        watcher: &mut dyn Watcher,
        root: &Path,
        start: &Path,
    ) -> notify::Result<usize> {
        watcher.watch(start, RecursiveMode::NonRecursive)?;
        self.dirs.insert(start.to_path_buf());
        let mut count = 1;
        for dir in pruned_dirs(root, start).filter(|dir| dir != start) {
            match watcher.watch(&dir, RecursiveMode::NonRecursive) {
                Ok(()) => {
                    self.dirs.insert(dir);
                    count += 1;
                }
                Err(e) if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) => {
                    return Err(e);
                }
                Err(e) => {
                    tracing::debug!(dir = %dir.display(), error = %e, "pruned walk skipped a directory");
                }
            }
        }
        Ok(count)
    }

    /// A directory appeared (created or moved in) under the group's coverage:
    /// walk it, pruned by the rules of the first recursive root it falls
    /// under and is not pruned by, and register its descriptors. Under no
    /// such root — a non-recursive root's child, or a noise subtree — it is
    /// left alone. Cheap when it lands nowhere; the callback forwards every
    /// directory creation on the stream.
    #[cfg(target_os = "linux")]
    fn add_dir(&mut self, watcher: &mut dyn Watcher, dir: &Path) {
        let Some(root) = self
            .roots
            .iter()
            .find(|root| dir.starts_with(root) && !prunes(root, dir))
            .cloned()
        else {
            return;
        };
        match self.add_tree(watcher, &root, dir) {
            Ok(_) => {}
            Err(e) if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) => {
                tracing::warn!(
                    root = %root.display(),
                    dir = %dir.display(),
                    error = %e,
                    os_watch_limits = %os_watch_limits(),
                    "new directory under a watched root could not be watched; events under it are lost"
                );
            }
            Err(e) => {
                tracing::debug!(dir = %dir.display(), error = %e, "new directory vanished before it could be watched");
            }
        }
    }

    /// A tracked directory was deleted: inotify already dropped its
    /// descriptor (and `notify` its table entry), so drop ours. Renames are
    /// deliberately NOT forgotten — the descriptor lives on under the old
    /// path in `notify`'s table, and only an unwatch under that path
    /// releases it.
    #[cfg(target_os = "linux")]
    fn forget(&mut self, dir: &Path) {
        self.dirs.remove(dir);
    }
}

/// Whether the pruned walk of `root` skips `dir`: the first noise component
/// on the path from `root` down to `dir` prunes it, unless a
/// [`PRUNE_EXEMPT_DIRS`] component comes first — or `root` itself lies under
/// one, in which case the whole root is walked unpruned (module header).
#[cfg(target_os = "linux")]
fn prunes(root: &Path, dir: &Path) -> bool {
    let is_exempt = |c: std::path::Component<'_>| matches!(c, std::path::Component::Normal(n) if n.to_str().is_some_and(|n| PRUNE_EXEMPT_DIRS.contains(&n)));
    if root.components().any(is_exempt) {
        return false;
    }
    let Ok(rel) = dir.strip_prefix(root) else {
        return false;
    };
    for component in rel.components() {
        if is_exempt(component) {
            return false;
        }
        if let std::path::Component::Normal(name) = component {
            if name
                .to_str()
                .is_some_and(|n| super::watcher::NOISE_DIRS.contains(&n))
            {
                return true;
            }
        }
    }
    false
}

/// The directories of `start`'s subtree (itself included) that survive
/// [`prunes`] under `root`'s rules. Symlinked directories are followed as
/// `notify`'s own walk does; unreadable entries are skipped.
#[cfg(target_os = "linux")]
fn pruned_dirs(root: &Path, start: &Path) -> impl Iterator<Item = PathBuf> {
    let root = root.to_path_buf();
    ignore::WalkBuilder::new(start)
        .standard_filters(false)
        .follow_links(true)
        .filter_entry(move |entry| {
            !entry.file_type().is_some_and(|t| t.is_dir()) || !prunes(&root, entry.path())
        })
        .build()
        .flatten()
        .filter(|entry| entry.file_type().is_some_and(|t| t.is_dir()))
        .map(ignore::DirEntry::into_path)
}

/// Group event callback hook: forward directory creations (and moves in) as
/// [`Cmd::AddDir`] and directory deletions as [`Cmd::Forget`] to the
/// registrar, which owns the descriptor table. Runs on the backend's event
/// thread, so it only enqueues; the `Weak` upgrade fails once the group is
/// retired, which is the right time to stop.
#[cfg(target_os = "linux")]
fn track_directories(cmd: &std::sync::Weak<std::sync::mpsc::Sender<Cmd>>, event: &notify::Event) {
    use notify::event::{CreateKind, EventKind, ModifyKind, RemoveKind, RenameMode};
    let cmds: Vec<Cmd> = match event.kind {
        EventKind::Create(CreateKind::Folder) => {
            event.paths.iter().cloned().map(Cmd::AddDir).collect()
        }
        // `notify` emits a standalone `To` alongside a paired `Both`, so the
        // destination is covered once by handling `To` alone.
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event
            .paths
            .iter()
            .filter(|p| p.is_dir())
            .cloned()
            .map(Cmd::AddDir)
            .collect(),
        EventKind::Remove(RemoveKind::Folder) => {
            event.paths.iter().cloned().map(Cmd::Forget).collect()
        }
        _ => return,
    };
    let Some(tx) = cmd.upgrade() else {
        return;
    };
    for cmd in cmds {
        let _ = tx.send(cmd);
    }
}

/// Obtain the group's watcher, surviving creation failure. On failure the
/// registrar does NOT return: it keeps serving the command channel — every
/// incoming `Cmd::Watch` settles as failed immediately, so waiters never hang,
/// and `Cmd::Unwatch` drops the root — while creation is retried with
/// exponential backoff capped at [`CREATE_RETRY_CAP`]. Roots settled as failed
/// while no watcher existed are re-registered once creation succeeds, because
/// the hub only re-sends `Cmd::Watch` when a NEW subscriber joins a failed root
/// ([`SharedWatchHub::subscribe`]'s retry) — without the re-registration a root
/// whose subscribers all predate the recovery would stay dead forever; they
/// are returned alongside the watcher for the caller to register. `None`
/// when every sender dropped, i.e. the group was retired.
#[expect(clippy::type_complexity)] // the pending list mirrors `Cmd::Watch`'s fields
fn build_watcher_serving(
    rx: &std::sync::mpsc::Receiver<Cmd>,
    make: impl Fn() -> notify::Result<Box<dyn Watcher + Send>>,
    group: &Path,
) -> Option<(
    Box<dyn Watcher + Send>,
    Vec<(PathBuf, RecursiveMode, Arc<Registration>)>,
)> {
    let mut backoff = CREATE_RETRY_INITIAL;
    let mut failures = 0u64;
    let mut pending: Vec<(PathBuf, RecursiveMode, Arc<Registration>)> = Vec::new();
    loop {
        match make() {
            Ok(watcher) => {
                if failures > 0 {
                    tracing::info!(
                        group = %group.display(),
                        failed_attempts = failures,
                        "shared watcher created after earlier failures; re-registering its roots"
                    );
                }
                return Some((watcher, pending));
            }
            Err(e) => {
                failures += 1;
                tracing::warn!(
                    group = %group.display(),
                    error = %e,
                    retry_in = ?backoff,
                    os_watch_limits = %os_watch_limits(),
                    "shared watcher creation failed; roots in this group are unwatched until a retry succeeds"
                );
            }
        }
        let deadline = std::time::Instant::now() + backoff;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match rx.recv_timeout(remaining) {
                Ok(Cmd::Watch(root, mode, registration)) => {
                    registration.settle(false);
                    pending.retain(|(r, _, _)| r != &root);
                    pending.push((root, mode, registration));
                }
                Ok(Cmd::Unwatch(root)) => pending.retain(|(r, _, _)| r != &root),
                // No watcher, no descriptors to keep in step.
                #[cfg(target_os = "linux")]
                Ok(Cmd::AddDir(_) | Cmd::Forget(_)) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return None,
            }
        }
        backoff = (backoff * 2).min(CREATE_RETRY_CAP);
    }
}

/// One deferred `watcher.watch()`. Settled either way, so waiters do not hang
/// on a failure; the failed state is distinct so a later subscriber to the
/// same root can retry it rather than inheriting a dead channel.
///
/// A failed RE-registration of a root that has already been live (a widen, or
/// a survivor re-registered after a recursive ancestor's unwatch stripped its
/// descriptors) is a lost watch, not a pending one: its subscribers passed
/// their liveness wait long ago and sit on `recv()`, where a state flag would
/// never reach them. Their sinks are removed instead, so the channel closes
/// and each subscriber can tear down and re-subscribe — the alternative is a
/// live-looking watch that silently delivers nothing for the process lifetime
/// (intent-hq/intent#4852).
fn register(
    watcher: &mut dyn Watcher,
    pruned: &mut PrunedWatches,
    root: &Path,
    mode: RecursiveMode,
    registration: &Registration,
    sinks: &Arc<Mutex<Vec<Sink>>>,
) {
    match pruned.watch(watcher, root, mode) {
        Ok(()) => registration.settle(true),
        Err(e) => {
            let lost = registration.was_live();
            tracing::warn!(
                root = %root.display(),
                error = %e,
                os_watch_limits = %os_watch_limits(),
                lost_live_watch = lost,
                "shared watch registration failed"
            );
            registration.settle(false);
            if lost {
                if let Ok(mut sinks) = sinks.lock() {
                    sinks.retain(|s| s.root != root);
                }
            }
        }
    }
}

/// Human-readable OS watch limits for the failure WARNs, so an operator can
/// judge at a glance whether a failure is cap exhaustion. On Linux the live
/// inotify sysctls (`max_user_instances` / `max_user_watches`); elsewhere
/// there is no equivalent user-tunable cap to read, and unreadable procfs
/// values degrade to `?`.
pub(crate) fn os_watch_limits() -> String {
    #[cfg(target_os = "linux")]
    {
        let read = |name: &str| {
            std::fs::read_to_string(format!("/proc/sys/fs/inotify/{name}"))
                .map_or_else(|_| "?".to_string(), |v| v.trim().to_string())
        };
        format!(
            "inotify max_user_instances={} max_user_watches={}",
            read("max_user_instances"),
            read("max_user_watches")
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        "n/a".to_string()
    }
}

/// Route one raw event to every sink whose root contains any of its paths,
/// carrying only the paths that are actually under that root.
///
/// The narrowing matters because one shared stream can report several roots'
/// paths in a single event: a `notify` rename spanning two co-tenant workspaces
/// arrives as one event holding both the source and the destination (the inotify
/// backend pairs `MOVED_FROM`/`MOVED_TO` into a `RenameMode::Both`, which only
/// exists now that both sides land on the same watcher). Forwarding the event
/// whole would hand each subscriber its co-tenant's path — every consumer
/// filters those out downstream, but a sink must not observe them at all, which
/// is the whole contract that keeps demuxed workspaces isolated.
///
/// The cheap `starts_with` pass runs first and is the only one needed in
/// practice: the roots are canonicalized at subscribe time and `FSEvents` reports
/// canonical paths. Resolution (a symlinked root, or a deleted path that cannot
/// be canonicalized directly) is attempted only for the paths that no sink's
/// root contains raw, so a busy stream costs no filesystem syscalls per event.
/// Doing it per path rather than per event matters for multi-path events: one
/// path matching raw must not suppress the fallback for a sibling path that
/// needs it. A non-recursive sink additionally drops paths deeper than its
/// direct children ([`covers`]); such a path still counts as contained
/// for the fallback's purposes, since resolving it cannot move it under a
/// different root.
///
/// The routing table is snapshotted and the lock released before any of that
/// work happens. Resolution stats the filesystem, and holding the sinks lock
/// across it would make a wedged volume block `SubHandle::drop` — which holds
/// the hub-wide `state` mutex — and through it every other group's `subscribe`,
/// contradicting the per-group isolation this module claims. A sink that goes
/// away mid-send just makes the send a no-op.
fn demux(sinks: &Arc<Mutex<Vec<Sink>>>, event: &notify::Event) {
    let routes: Vec<(PathBuf, bool, mpsc::UnboundedSender<notify::Event>)> = match sinks.lock() {
        Ok(sinks) => sinks
            .iter()
            .map(|s| (s.root.clone(), s.recursive, s.tx.clone()))
            .collect(),
        Err(_) => return,
    };

    let mut unmatched: Vec<usize> = (0..event.paths.len()).collect();
    for (root, recursive, tx) in &routes {
        let mine = narrow(event, &event.paths, root, *recursive);
        unmatched.retain(|i| !event.paths[*i].starts_with(root));
        send_narrowed(tx, event, mine);
    }
    if unmatched.is_empty() {
        return;
    }

    // Resolve only the leftovers; every other index keeps a path no root can
    // match, so it cannot be rescued and must not be sent.
    let mut resolved = event.paths.clone();
    for i in &unmatched {
        let raw = &event.paths[*i];
        resolved[*i] = canonical_root(raw, &find_existing_ancestor(raw));
    }
    for (root, recursive, tx) in &routes {
        let mine: Vec<PathBuf> = unmatched
            .iter()
            .filter(|i| covers(root, *recursive, &resolved[**i]))
            .map(|i| event.paths[*i].clone())
            .collect();
        send_narrowed(tx, event, mine);
    }
}

/// The raw paths of `event` whose `candidates` counterpart falls within the
/// sink's slice of `root` (see [`covers`]).
fn narrow(
    event: &notify::Event,
    candidates: &[PathBuf],
    root: &Path,
    recursive: bool,
) -> Vec<PathBuf> {
    candidates
        .iter()
        .zip(event.paths.iter())
        .filter(|(candidate, _)| covers(root, recursive, candidate))
        .map(|(_, raw)| raw.clone())
        .collect()
}

/// Forward `event` carrying only `paths` (already narrowed to the destination's
/// root), preserving the original event kind. No-op when nothing matched.
fn send_narrowed(
    tx: &mpsc::UnboundedSender<notify::Event>,
    event: &notify::Event,
    paths: Vec<PathBuf>,
) {
    if paths.is_empty() {
        return;
    }
    let mut narrowed = event.clone();
    narrowed.paths = paths;
    let _ = tx.send(narrowed);
}

/// Whether an event should be forwarded for a tier-style watch: any of its
/// paths falls under one of `tier_roots` and either matches `filename_matches`
/// or is directory-level. Mirrors [`super::root_watch`]'s per-root filter, which
/// the skills/specialists project tiers used before they rode the shared
/// stream, so tier-directory deletions (`rm -rf`) still surface.
fn tier_event_matches(
    event: &notify::Event,
    tier_roots: &[PathBuf],
    filename_matches: fn(&Path) -> bool,
) -> bool {
    event.paths.iter().any(|p| {
        tier_roots.iter().any(|root| p.starts_with(root))
            && (filename_matches(p) || super::root_watch::directory_level(p))
    })
}

/// A tier watch riding a shared stream. Dropping it ends the forwarding task
/// and releases the subscription.
pub(super) struct TierWatch {
    _sub: SubHandle,
    task: JoinHandle<()>,
}

impl Drop for TierWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TierWatch {
    /// Detach a [`RegistrationProbe`] for the shared watch this tier rides.
    /// Tier watches are held behind a `std::sync::Mutex` by their owners, so
    /// the probe is what a test awaits, not the watch itself.
    #[cfg(test)]
    #[expect(clippy::used_underscore_binding)] // RAII field; underscore documents production lifetime-only intent
    pub(super) fn probe(&self) -> RegistrationProbe {
        self._sub.probe()
    }
}

/// Watch the tier directories `subpaths` (relative to `workspace_root`) via the
/// shared stream on the workspace root, invoking `on_change` for matching
/// events.
///
/// This replaces the per-tier [`super::root_watch::watch_root`] streams the
/// skills/specialists project tiers used to own. The ancestor-watch/promotion
/// dance those needed for missing tier dirs is gone with them: the shared watch
/// is recursive on the workspace root, so a tier directory created later is
/// simply seen.
///
/// `on_change` still fires once as a catch-up, matching what `watch_root` did,
/// and — as there — only **after** the registration has landed. Firing it up
/// front would leave a gap: a change arriving between the flush and the OS watch
/// existing produces neither an event nor a catch-up, silently missing a tier
/// update. Callers' fingerprint checks suppress the no-op case.
pub(super) fn watch_tiers(
    hub: &Arc<SharedWatchHub>,
    workspace_root: &Path,
    subpaths: &[&str],
    filename_matches: fn(&Path) -> bool,
    on_change: impl Fn() + Send + 'static,
) -> TierWatch {
    let (sub, mut rx, canonical) = hub.subscribe(workspace_root);
    let tier_roots: Vec<PathBuf> = subpaths
        .iter()
        .map(|rel| {
            rel.split('/')
                .fold(canonical.clone(), |acc, part| acc.join(part))
        })
        .collect();
    let registration = Arc::clone(&sub.registration);
    let task = tokio::spawn(async move {
        wait_settled(&registration, ESTABLISH_TIMEOUT).await;
        on_change();
        while let Some(event) = rx.recv().await {
            if tier_event_matches(&event, &tier_roots, filename_matches) {
                on_change();
            }
        }
    });
    TierWatch { _sub: sub, task }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::events::LIVENESS;

    /// Self-cleaning temp directory.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "intentd-shared-watch-{tag}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Await the first event for `rel` on `rx`, ignoring events for other
    /// paths; `None` on timeout.
    async fn next_for(
        rx: &mut mpsc::UnboundedReceiver<notify::Event>,
        root: &Path,
        rel: &str,
        overall: Duration,
    ) -> Option<notify::Event> {
        let want = root.join(rel);
        let deadline = tokio::time::Instant::now() + overall;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Some(event)) if event.paths.iter().any(|p| p == &want) => return Some(event),
                Ok(Some(_)) => {}
                _ => return None,
            }
        }
    }

    /// The consolidation invariant: two workspace roots under one parent share
    /// a SINGLE stream, and the demux still keeps them isolated — each sink
    /// sees only the events under its own root.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn sibling_roots_share_one_stream_and_stay_isolated() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("siblings");
        let a = parent.path.join("ws-a");
        let b = parent.path.join("ws-b");
        std::fs::create_dir_all(&a).expect("mk ws-a");
        std::fs::create_dir_all(&b).expect("mk ws-b");

        let hub = SharedWatchHub::new();
        let (_sub_a, mut rx_a, root_a) = hub.subscribe(&a);
        let (_sub_b, mut rx_b, root_b) = hub.subscribe(&b);
        assert_eq!(
            hub.stream_count(),
            1,
            "sibling roots must ride one shared stream"
        );
        // Wait for BOTH registrations, not just the first: adding a root
        // rebuilds the group's stream, so the second `watch` restarts what the
        // first established. The negative assertion below would pass vacuously
        // against an unestablished watch.
        hub.wait_all_established(2, LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Probe until delivery is actually flowing, then assert isolation.
        // Attempt count sized so the total probe budget (attempts x 500ms)
        // reaches `LIVENESS` — a pure-liveness bound (monorepo#1630).
        let attempts = LIVENESS.as_millis() / 500;
        for attempt in 0..attempts {
            std::fs::write(a.join(".probe"), format!("{attempt}")).expect("write probe");
            if next_for(&mut rx_a, &root_a, ".probe", Duration::from_millis(500))
                .await
                .is_some()
            {
                break;
            }
            assert!(
                attempt < attempts - 1,
                "shared stream never began delivering"
            );
        }
        std::fs::remove_file(a.join(".probe")).expect("rm probe");
        while next_for(&mut rx_a, &root_a, ".probe", Duration::from_millis(300))
            .await
            .is_some()
        {}
        while rx_b.try_recv().is_ok() {}

        // Prove b's sink delivers at all before asserting what it must NOT
        // receive: a mis-wired subscription that delivers nothing would satisfy
        // the negative assertion vacuously.
        std::fs::write(b.join("only-b.txt"), "x").expect("write in b");
        assert!(
            next_for(&mut rx_b, &root_b, "only-b.txt", LIVENESS)
                .await
                .is_some(),
            "b's own change must reach its sink"
        );

        std::fs::write(a.join("only-a.txt"), "x").expect("write in a");
        assert!(
            next_for(&mut rx_a, &root_a, "only-a.txt", LIVENESS)
                .await
                .is_some(),
            "a root's own change must reach its sink"
        );
        assert!(
            next_for(&mut rx_b, &root_b, "only-a.txt", Duration::from_secs(1))
                .await
                .is_none(),
            "another root's change must not reach this sink"
        );
    }

    /// Dropping the last subscription for a group retires the stream, so an
    /// archived/closed workspace stops consuming fseventsd capacity.
    #[tokio::test]
    async fn dropping_the_last_subscription_retires_the_stream() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("retire");
        let root = parent.path.join("ws");
        std::fs::create_dir_all(&root).expect("mk ws");

        let hub = SharedWatchHub::new();
        let (first, _rx1, _) = hub.subscribe(&root);
        let (second, _rx2, _) = hub.subscribe(&root);
        assert_eq!(hub.stream_count(), 1);

        // The root is still referenced by `second`, so the stream survives.
        drop(first);
        assert_eq!(hub.stream_count(), 1, "stream must survive a live consumer");
        drop(second);
        assert_eq!(
            hub.stream_count(),
            0,
            "stream must be retired once nothing consumes it"
        );
    }

    /// The intent-hq/intent#3708 invariant: on Linux every root rides ONE
    /// global group regardless of its parent directory, so the inotify
    /// instance count does not scale with the workspace count. Retirement
    /// still works at root granularity — the group survives until its last
    /// root drops, then retires.
    #[cfg(target_os = "linux")]
    #[test]
    fn distinct_parent_roots_share_one_global_group_on_linux() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("global-group");
        let hub = SharedWatchHub::new();
        let mut subs = Vec::new();
        for i in 0..4 {
            let root = base.path.join(format!("parent-{i}")).join("ws");
            std::fs::create_dir_all(&root).expect("mk ws");
            let (sub, rx, _) = hub.subscribe(&root);
            subs.push((sub, rx));
            assert_eq!(
                hub.stream_count(),
                1,
                "distinct-parent roots must collapse into a single global group"
            );
        }
        while subs.len() > 1 {
            subs.pop();
            assert_eq!(
                hub.stream_count(),
                1,
                "the global group must survive while any root remains"
            );
        }
        subs.pop();
        assert_eq!(hub.stream_count(), 0, "the empty global group must retire");
    }

    /// Retiring a root must not strip coverage from a still-subscribed root
    /// nested under it. `notify`'s recursive inotify `unwatch` removes the
    /// target and every descendant descriptor without per-root ref-counting,
    /// so on Linux (one global group) the co-tenant nested root would go
    /// silently dead without the re-registration in `SubHandle::drop`.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn dropping_an_outer_root_keeps_a_nested_root_covered() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("nested-unwatch");
        let outer = base.path.join("outer");
        let nested = outer.join("nested");
        std::fs::create_dir_all(&nested).expect("mk nested");

        let hub = SharedWatchHub::new();
        let (sub_nested, mut rx_nested, root_nested) = hub.subscribe(&nested);
        let (sub_outer, _rx_outer, _) = hub.subscribe(&outer);
        hub.wait_all_established(2, LIVENESS).await;

        // Retiring the outer root recursively unwatches the nested root too
        // when both ride one watcher; the drop path must re-register it.
        drop(sub_outer);
        sub_nested.wait_established(LIVENESS).await;

        std::fs::write(root_nested.join("still-covered.txt"), b"x").expect("write");
        assert!(
            next_for(&mut rx_nested, &root_nested, "still-covered.txt", LIVENESS)
                .await
                .is_some(),
            "the nested root must keep delivering after its outer co-tenant retires"
        );
    }

    /// Write `rel` under `root` and keep touching it until `rx` delivers an
    /// event for it. A directory created under a recursive watch is added to
    /// inotify only after the create event that announced it is dispatched, so
    /// a single write racing that add can land before the descriptor exists;
    /// every later touch is a fresh chance to be seen.
    async fn touch_until_seen(
        rx: &mut mpsc::UnboundedReceiver<notify::Event>,
        root: &Path,
        rel: &str,
    ) -> bool {
        let path = root.join(rel);
        let deadline = tokio::time::Instant::now() + LIVENESS;
        while tokio::time::Instant::now() < deadline {
            std::fs::write(&path, b"x").expect("write probe file");
            if next_for(rx, root, rel, Duration::from_millis(250))
                .await
                .is_some()
            {
                return true;
            }
        }
        false
    }

    /// Drive a recursive `outer` root with a NON-recursive co-tenant nested
    /// under it — the shape a missing project-tier skills root parked on
    /// `<workspace>/.agents` takes under the recursive workspace root — and
    /// assert the outer root still auto-watches directories created under
    /// the nested one, then keeps them after the nested subscriber retires.
    /// Without the ancestor rule in `subscribe_with` / `SubHandle::drop`, the
    /// shallow registration flips the shared inotify descriptor's recursion
    /// flag off (new subdirectories are never added) and the shallow unwatch
    /// strips the descriptor from the outer root entirely.
    async fn recursive_outer_survives_shallow_nested(outer_first: bool, tag: &str) {
        let base = TempDir::new(tag);
        let outer = base.path.join("outer");
        let nested = outer.join("nested");
        std::fs::create_dir_all(&nested).expect("mk nested");

        let hub = SharedWatchHub::new();
        let (sub_outer, mut rx_outer, root_outer, sub_nested, mut rx_nested, root_nested) =
            if outer_first {
                let (so, ro, po) = hub.subscribe(&outer);
                let (sn, rn, pn) = hub.subscribe_with(&nested, RecursiveMode::NonRecursive);
                (so, ro, po, sn, rn, pn)
            } else {
                let (sn, rn, pn) = hub.subscribe_with(&nested, RecursiveMode::NonRecursive);
                let (so, ro, po) = hub.subscribe(&outer);
                (so, ro, po, sn, rn, pn)
            };
        hub.wait_all_established(2, LIVENESS).await;

        // A directory created under the shallow root must still be picked up
        // by the recursive outer root, and its contents delivered there.
        std::fs::create_dir(root_nested.join("created-later")).expect("mk created-later");
        assert!(
            touch_until_seen(
                &mut rx_outer,
                &root_outer,
                "nested/created-later/expected.txt"
            )
            .await,
            "outer_first={outer_first}: the recursive outer root lost coverage of a \
             directory created under its shallow co-tenant"
        );
        // The shallow sink stays shallow: it sees its direct child, not the
        // grandchild the outer root just saw.
        std::fs::write(root_nested.join("direct.txt"), b"x").expect("write direct");
        assert!(
            next_for(&mut rx_nested, &root_nested, "direct.txt", LIVENESS)
                .await
                .is_some(),
            "outer_first={outer_first}: the shallow sink must see its direct child"
        );
        while let Ok(event) = rx_nested.try_recv() {
            assert!(
                !event
                    .paths
                    .iter()
                    .any(|p| p.starts_with(root_nested.join("created-later"))),
                "outer_first={outer_first}: shallow sink leaked a grandchild event: {event:?}"
            );
        }

        // Retiring the shallow root must leave the outer root's descriptors
        // for that subtree in place.
        drop(sub_nested);
        sub_outer.wait_established(LIVENESS).await;
        std::fs::create_dir(root_nested.join("after-retire")).expect("mk after-retire");
        assert!(
            touch_until_seen(
                &mut rx_outer,
                &root_outer,
                "nested/after-retire/expected.txt"
            )
            .await,
            "outer_first={outer_first}: retiring the shallow co-tenant stripped the \
             outer root's coverage of the nested subtree"
        );
        drop(sub_outer);
    }

    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_shallow_root_under_a_recursive_root_keeps_the_ancestor_recursive() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recursive_outer_survives_shallow_nested(true, "shallow-after-outer").await;
    }

    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_recursive_root_over_an_existing_shallow_root_watches_its_subtree() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recursive_outer_survives_shallow_nested(false, "outer-after-shallow").await;
    }

    /// Regression for the widened-ancestor promotion hole (PR #1876 review):
    /// a non-recursive ancestor watch that a recursive co-subscriber once
    /// widened stays recursive after that co-subscriber leaves, so retiring it
    /// re-registers the roots nested under it — including a root promoted off
    /// it whose owner already passed `wait_live`. When that re-registration
    /// fails, the owner must find out: its channel closes (a) and the
    /// registration reads as failed (b), instead of a live-looking watch that
    /// never delivers again. Linux only: on macOS the two roots live in
    /// distinct groups and no survivor re-registration happens.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_failed_re_registration_of_a_live_root_closes_its_subscribers() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("lost-live");
        let ancestor = base.path.join("parent");
        let desired = ancestor.join("desired");
        std::fs::create_dir_all(&desired).expect("mk desired");

        // `desired` sees one `watch()` from the ancestor's widened pruned walk
        // and one on its own subscribe; the third is the survivor
        // re-registration the ancestor's retirement sends.
        let fault = WatchFault::nth("desired", 3);
        let hub = SharedWatchHub::with_watch_fault(&fault);

        let (sub_ancestor, _rx_ancestor, _) =
            hub.subscribe_with(&ancestor, RecursiveMode::NonRecursive);
        sub_ancestor.wait_established(LIVENESS).await;
        let (sub_wide, _rx_wide, _) = hub.subscribe(&ancestor);
        sub_wide.wait_established(LIVENESS).await;
        drop(sub_wide);

        let (sub_desired, mut rx_desired, _) = hub.subscribe(&desired);
        sub_desired.wait_established(LIVENESS).await;
        assert!(
            sub_desired.registration.live(),
            "precondition: the promoted root goes live on its first registration"
        );
        assert_eq!(fault.attempts(), 2);

        drop(sub_ancestor);

        // (a) The subscriber's channel closes rather than parking forever.
        let closed = tokio::time::timeout(LIVENESS, async {
            while rx_desired.recv().await.is_some() {}
        })
        .await;
        assert!(
            closed.is_ok(),
            "a failed re-registration of a live root must close its subscribers' channels"
        );
        // (b) The state agrees, and the failure was the targeted third call.
        assert!(sub_desired.registration.failed());
        assert_eq!(fault.attempts(), 3);
    }

    /// Inodes currently held by an inotify watch descriptor anywhere in this
    /// process, parsed from the `inotify wd:N ino:HEX ...` lines of
    /// `/proc/self/fdinfo/*` (the counting method of intent-hq/intent#3708).
    #[cfg(target_os = "linux")]
    fn inotify_watched_inodes() -> std::collections::HashSet<u64> {
        let mut inodes = std::collections::HashSet::new();
        let Ok(fds) = std::fs::read_dir("/proc/self/fd") else {
            return inodes;
        };
        for fd in fds.flatten() {
            let fdinfo = std::path::Path::new("/proc/self/fdinfo").join(fd.file_name());
            let Ok(text) = std::fs::read_to_string(fdinfo) else {
                continue;
            };
            for line in text.lines().filter(|l| l.starts_with("inotify wd:")) {
                let Some(ino) = line
                    .split_whitespace()
                    .find_map(|field| field.strip_prefix("ino:"))
                    .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                else {
                    continue;
                };
                inodes.insert(ino);
            }
        }
        inodes
    }

    #[cfg(target_os = "linux")]
    fn inode_of(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path)
            .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
            .ino()
    }

    /// Poll until `dir` holds (or, with `expect = false`, no longer holds) an
    /// inotify descriptor; descriptor adds for directories created after
    /// registration ride the event path, so they land asynchronously.
    #[cfg(target_os = "linux")]
    async fn wait_watched(dir: &Path, expect: bool) -> bool {
        let ino = inode_of(dir);
        let deadline = tokio::time::Instant::now() + LIVENESS;
        while tokio::time::Instant::now() < deadline {
            if inotify_watched_inodes().contains(&ino) == expect {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// The intent-hq/intent#5026 invariant: a recursive root's OS watch must
    /// not descend into `NOISE_DIRS` (`node_modules`, `target`, `vendor`, …)
    /// at any depth — those subtrees cost `max_user_watches` for events every
    /// consumer drops — while everything else, including directories created
    /// AFTER registration, stays covered and delivers. Subscriber-owned trees
    /// (`.git`, `.intent`) are exempt from pruning even where a nested name
    /// collides with the noise list (`.git/refs/heads/build`), and retiring
    /// the root releases every descriptor the pruned walk registered.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_recursive_root_does_not_watch_noise_dirs() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("prune-noise");
        let ws = base.path.join("ws");
        let watched = [
            "src",
            "src/a",
            "src/b/deep",
            ".git/refs/heads/build",
            ".intent/skills/build",
            "docs",
        ];
        let noise = [
            "node_modules/pkg-a/lib",
            "node_modules/pkg-b/dist",
            "target/debug/deps",
            "target/debug/build/x",
            "vendor/dep",
            "src/b/node_modules/nested",
        ];
        for rel in watched.iter().chain(noise.iter()) {
            std::fs::create_dir_all(ws.join(rel)).expect("mk tree");
        }

        let hub = SharedWatchHub::new();
        let (sub, mut rx, root) = hub.subscribe(&ws);
        sub.probe().wait_live(LIVENESS).await;

        let inodes = inotify_watched_inodes();
        assert!(
            inodes.contains(&inode_of(&root)),
            "the root itself is watched"
        );
        for rel in watched {
            assert!(
                inodes.contains(&inode_of(&ws.join(rel))),
                "{rel} must hold an inotify descriptor"
            );
        }
        // From the first noise component down, no directory may be watched.
        for rel in noise {
            let mut prefix = PathBuf::new();
            let mut pruned = false;
            for component in Path::new(rel).components() {
                prefix.push(component);
                pruned |= component
                    .as_os_str()
                    .to_str()
                    .is_some_and(|n| super::super::watcher::NOISE_DIRS.contains(&n));
                assert!(
                    !pruned || !inodes.contains(&inode_of(&ws.join(&prefix))),
                    "{} must NOT hold an inotify descriptor",
                    prefix.display()
                );
            }
            assert!(pruned, "test tree: {rel} must contain a noise component");
        }

        // Directories created after registration — a fresh top-level tree and
        // a nested one — get descriptors and deliver, while a noise directory
        // created later stays unwatched (also when nested under a new dir).
        std::fs::create_dir_all(root.join("later/deeper")).expect("mk later");
        std::fs::create_dir_all(root.join("src/a/new")).expect("mk src/a/new");
        assert!(
            touch_until_seen(&mut rx, &root, "later/deeper/file.txt").await,
            "a directory created after registration must deliver events"
        );
        assert!(
            touch_until_seen(&mut rx, &root, "src/a/new/file.txt").await,
            "a nested directory created after registration must deliver events"
        );
        std::fs::create_dir_all(root.join("later/node_modules/pkg")).expect("mk later noise");
        std::fs::create_dir_all(root.join("target/release")).expect("mk target/release");
        assert!(
            wait_watched(&root.join("later"), true).await,
            "later/ must be watched"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let inodes = inotify_watched_inodes();
        for rel in [
            "later/node_modules",
            "later/node_modules/pkg",
            "target/release",
        ] {
            assert!(
                !inodes.contains(&inode_of(&root.join(rel))),
                "{rel} created after registration must NOT be watched"
            );
        }

        // Retiring the root releases every descriptor the walk registered.
        drop(sub);
        for rel in [
            "",
            "src",
            "src/b/deep",
            "later/deeper",
            ".git/refs/heads/build",
        ] {
            assert!(
                wait_watched(&root.join(rel), false).await,
                "unwatch must release the descriptor for {rel:?}"
            );
        }
    }

    /// A recursive root that does not exist must settle as FAILED, as the
    /// backend's own recursive watch did: the pruned walk yields nothing for
    /// a missing start, and a registration settled live with zero
    /// descriptors would bypass the caller's failed-registration recovery.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_missing_recursive_root_settles_as_failed() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("prune-missing");
        let missing = base.path.join("ws");

        let hub = SharedWatchHub::new();
        let (sub, _rx, _) = hub.subscribe(&missing);
        sub.wait_established(LIVENESS).await;
        assert!(
            sub.registration.settled(),
            "registration must settle for a missing root"
        );
        assert!(
            sub.registration.failed(),
            "a missing recursive root must settle as failed, not live"
        );
    }

    /// A root nested inside a noise subtree of a recursive co-tenant
    /// (`ws/target/repo` under `ws`) shares no descriptors with it — the
    /// co-tenant's walk pruned `target` — so it is not "covered": it must be
    /// registered in its own mode and, when its last subscriber drops, be
    /// unwatched outright rather than left holding (and growing) descriptors
    /// until the outer root retires.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn a_root_under_a_pruned_subtree_is_retired_with_its_subscriber() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("prune-nested");
        let ws = base.path.join("ws");
        let nested = ws.join("target").join("repo");
        std::fs::create_dir_all(nested.join("src")).expect("mk tree");
        std::fs::create_dir_all(ws.join("src")).expect("mk src");

        let hub = SharedWatchHub::new();
        let (sub_ws, _rx_ws, ws) = hub.subscribe(&ws);
        sub_ws.probe().wait_live(LIVENESS).await;
        let nested = ws.join("target").join("repo");
        assert!(
            !inotify_watched_inodes().contains(&inode_of(&nested)),
            "precondition: the outer root prunes target/"
        );

        let (sub_nested, mut rx_nested, _) = hub.subscribe(&nested);
        sub_nested.probe().wait_live(LIVENESS).await;
        let inodes = inotify_watched_inodes();
        assert!(inodes.contains(&inode_of(&nested)), "nested root watched");
        assert!(
            inodes.contains(&inode_of(&nested.join("src"))),
            "nested root's own walk covers its subtree"
        );
        assert!(
            touch_until_seen(&mut rx_nested, &nested, "src/file.txt").await,
            "the nested root delivers while subscribed"
        );

        drop(sub_nested);
        for rel in ["", "src"] {
            assert!(
                wait_watched(&nested.join(rel), false).await,
                "dropping the nested root's last subscriber must release {rel:?}"
            );
        }
        // Nor does the registrar keep growing it: a directory created under
        // the retired root lands under the outer root's pruned subtree and
        // stays unwatched.
        std::fs::create_dir_all(nested.join("later")).expect("mk later");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !inotify_watched_inodes().contains(&inode_of(&nested.join("later"))),
            "no descriptors may be added under a retired nested root"
        );
        assert!(
            sub_ws.registration.live(),
            "the outer root is untouched by the nested root's retirement"
        );
    }

    /// macOS keeps parent-directory grouping: the `FSEvents` stream rebuild on
    /// every `watch`/`unwatch` is per group, so distinct-parent roots must NOT
    /// collapse into one global group there.
    #[cfg(target_os = "macos")]
    #[test]
    fn distinct_parent_roots_get_distinct_groups_on_macos() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = TempDir::new("per-parent");
        let a = base.path.join("parent-a").join("ws");
        let b = base.path.join("parent-b").join("ws");
        std::fs::create_dir_all(&a).expect("mk a");
        std::fs::create_dir_all(&b).expect("mk b");

        let hub = SharedWatchHub::new();
        let (_sub_a, _rx_a, _) = hub.subscribe(&a);
        let (_sub_b, _rx_b, _) = hub.subscribe(&b);
        assert_eq!(
            hub.stream_count(),
            2,
            "distinct parents must keep distinct FSEvents groups on macOS"
        );
    }

    /// A rename between two co-tenants of one stream arrives as a SINGLE event
    /// holding both sides' paths, so forwarding it whole would leak the
    /// co-tenant's path into each sink. Each side must see only its own path —
    /// and with the original event kind, so the source still reads as a rename
    /// rather than being reclassified.
    #[test]
    fn a_cross_root_rename_is_narrowed_to_each_sink_own_paths() {
        use notify::event::{EventKind, ModifyKind, RenameMode};

        let a = PathBuf::from("/parent/ws-a");
        let b = PathBuf::from("/parent/ws-b");
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        let sinks = Arc::new(Mutex::new(vec![
            Sink {
                id: 0,
                root: a.clone(),
                recursive: true,
                tx: tx_a,
            },
            Sink {
                id: 1,
                root: b.clone(),
                recursive: true,
                tx: tx_b,
            },
        ]));

        let event = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(a.join("moved.txt"))
            .add_path(b.join("moved.txt"));
        demux(&sinks, &event);

        let got_a = rx_a.try_recv().expect("source side must be delivered");
        assert_eq!(
            got_a.paths,
            vec![a.join("moved.txt")],
            "sink must not observe its co-tenant's path"
        );
        assert_eq!(got_a.kind, event.kind, "event kind must survive narrowing");
        let got_b = rx_b.try_recv().expect("destination side must be delivered");
        assert_eq!(got_b.paths, vec![b.join("moved.txt")]);
    }

    /// A path under neither root reaches neither sink, even when it shares the
    /// group's parent directory.
    #[test]
    fn a_group_sibling_outside_every_root_reaches_no_sink() {
        use notify::event::{CreateKind, EventKind};

        let a = PathBuf::from("/parent/ws-a");
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let sinks = Arc::new(Mutex::new(vec![Sink {
            id: 0,
            root: a,
            recursive: true,
            tx: tx_a,
        }]));

        let event = notify::Event::new(EventKind::Create(CreateKind::File))
            .add_path(PathBuf::from("/parent/loose.txt"));
        demux(&sinks, &event);

        assert!(rx_a.try_recv().is_err(), "unrelated path must not deliver");
    }

    /// A non-recursive sink sees the root itself and its direct children —
    /// what a dedicated non-recursive OS watch reports — and nothing deeper,
    /// even though the shared stream it rides is recursive. A recursive
    /// co-subscriber of the same root keeps the full view.
    #[test]
    fn a_non_recursive_sink_is_narrowed_to_direct_children() {
        use notify::event::{CreateKind, EventKind};

        let root = PathBuf::from("/home/u/.intent");
        let (tx_shallow, mut rx_shallow) = mpsc::unbounded_channel();
        let (tx_deep, mut rx_deep) = mpsc::unbounded_channel();
        let sinks = Arc::new(Mutex::new(vec![
            Sink {
                id: 0,
                root: root.clone(),
                recursive: false,
                tx: tx_shallow,
            },
            Sink {
                id: 1,
                root: root.clone(),
                recursive: true,
                tx: tx_deep,
            },
        ]));

        let direct = notify::Event::new(EventKind::Create(CreateKind::File))
            .add_path(root.join("config.toml"));
        demux(&sinks, &direct);
        assert_eq!(
            rx_shallow.try_recv().expect("direct child delivers").paths,
            vec![root.join("config.toml")]
        );
        assert!(rx_deep.try_recv().is_ok(), "recursive sink sees it too");

        let itself =
            notify::Event::new(EventKind::Create(CreateKind::Folder)).add_path(root.clone());
        demux(&sinks, &itself);
        assert!(
            rx_shallow.try_recv().is_ok(),
            "the root itself is a non-recursive event"
        );
        rx_deep.try_recv().expect("recursive sink sees it too");

        let nested = notify::Event::new(EventKind::Create(CreateKind::File))
            .add_path(root.join("specialists").join("x.md"));
        demux(&sinks, &nested);
        assert!(
            rx_shallow.try_recv().is_err(),
            "a grandchild must not reach a non-recursive sink"
        );
        assert_eq!(
            rx_deep
                .try_recv()
                .expect("recursive sink sees the grandchild")
                .paths,
            vec![root.join("specialists").join("x.md")]
        );
    }

    /// The resolution fallback is per path, not per event: one path of a
    /// multi-path event matching raw must not suppress resolution for a sibling
    /// path that only reaches its sink after canonicalization.
    #[cfg(unix)]
    #[test]
    fn a_raw_match_does_not_suppress_resolution_for_sibling_paths() {
        use notify::event::{EventKind, ModifyKind, RenameMode};

        let parent = TempDir::new("fallback");
        // A symlinked root: the sink is keyed by the canonical form, so the raw
        // path the "OS" reports under the link only matches after resolution.
        let real = parent.path.join("real");
        let link = parent.path.join("link");
        std::fs::create_dir_all(&real).expect("mk real");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        let canonical_real = std::fs::canonicalize(&real).expect("canonicalize real");

        let plain = parent.path.join("plain");
        std::fs::create_dir_all(&plain).expect("mk plain");
        let canonical_plain = std::fs::canonicalize(&plain).expect("canonicalize plain");

        let (tx_plain, mut rx_plain) = mpsc::unbounded_channel();
        let (tx_linked, mut rx_linked) = mpsc::unbounded_channel();
        let sinks = Arc::new(Mutex::new(vec![
            Sink {
                id: 0,
                root: canonical_plain.clone(),
                recursive: true,
                tx: tx_plain,
            },
            Sink {
                id: 1,
                root: canonical_real,
                recursive: true,
                tx: tx_linked,
            },
        ]));

        // One event, two paths: the first matches raw, the second needs
        // resolution. Before the per-path fallback the first suppressed the
        // second entirely.
        std::fs::write(plain.join("moved.txt"), "x").expect("write plain");
        std::fs::write(real.join("moved.txt"), "x").expect("write real");
        let via_link = link.join("moved.txt");
        let event = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(canonical_plain.join("moved.txt"))
            .add_path(via_link.clone());
        demux(&sinks, &event);

        assert_eq!(
            rx_plain
                .try_recv()
                .expect("raw-matching sink must be delivered")
                .paths,
            vec![canonical_plain.join("moved.txt")]
        );
        assert_eq!(
            rx_linked
                .try_recv()
                .expect("sink reachable only after resolution must still be delivered")
                .paths,
            vec![via_link],
            "the resolved sink receives the raw path the OS reported"
        );
    }

    /// A root whose registration failed is dead but still refcounted, so a later
    /// subscriber must re-request the watch rather than inherit a channel that
    /// can never deliver.
    #[test]
    fn a_failed_registration_is_retried_by_the_next_subscriber() {
        let parent = TempDir::new("retry");
        let root = parent.path.join("ws");
        std::fs::create_dir_all(&root).expect("mk ws");

        let hub = SharedWatchHub::new();
        let (_first, _rx1, canonical) = hub.subscribe(&root);
        // Simulate the registrar reporting a failure (e.g. the directory was
        // briefly missing) rather than racing a real one.
        {
            let state = hub.state.lock().unwrap();
            let entry = state
                .groups
                .values()
                .find_map(|g| g.roots.get(&canonical))
                .expect("root must be tracked");
            entry.registration.settle(false);
            assert!(entry.registration.failed());
        }

        let (_second, _rx2, _) = hub.subscribe(&root);
        let state = hub.state.lock().unwrap();
        let entry = state
            .groups
            .values()
            .find_map(|g| g.roots.get(&canonical))
            .expect("root must still be tracked");
        assert_eq!(entry.subscribers, 2);
        assert!(
            !entry.registration.failed(),
            "joining a failed root must re-request the watch, not inherit the failure"
        );
    }

    /// Watcher-creation failure must not permanently kill the group
    /// (intent-hq/intent#3708): registrations arriving while no watcher exists
    /// settle as failed rather than hang, the registrar keeps serving its
    /// command channel and retries creation with backoff, and once the factory
    /// recovers the failed roots are re-registered and deliver events.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn watcher_creation_failure_settles_registrations_and_recovers() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("create-fail");
        let root = parent.path.join("ws");
        std::fs::create_dir_all(&root).expect("mk ws");

        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let fail_in_factory = Arc::clone(&fail);
        let hub = SharedWatchHub::with_factory(Arc::new(move |callback: EventCallback| {
            if fail_in_factory.load(Ordering::SeqCst) {
                Err(notify::Error::generic("injected creation failure"))
            } else {
                notify::recommended_watcher(callback)
                    .map(|w| Box::new(w) as Box<dyn Watcher + Send>)
            }
        }));

        let (sub, mut rx, canonical) = hub.subscribe(&root);
        // (a) The registration settles as failed instead of hanging forever.
        sub.wait_established(LIVENESS).await;
        assert!(
            sub.registration.settled(),
            "registration must settle while creation keeps failing"
        );
        assert!(
            sub.registration.failed(),
            "creation failure must settle the registration as failed"
        );

        // (b) Once the factory recovers, the registrar's backoff retry builds
        // the watcher and re-registers the root — no new subscriber needed.
        fail.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + LIVENESS;
        while !sub.registration.settled() || sub.registration.failed() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "watch must go live after the factory recovers"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // The re-registered watch actually delivers. Probe until delivery
        // flows; budget sized against `LIVENESS` like the sibling test.
        let attempts = LIVENESS.as_millis() / 500;
        for attempt in 0..attempts {
            std::fs::write(root.join(".probe"), format!("{attempt}")).expect("write probe");
            if next_for(&mut rx, &canonical, ".probe", Duration::from_millis(500))
                .await
                .is_some()
            {
                return;
            }
        }
        panic!("recovered watch never delivered events");
    }

    /// Regression for intent-hq/intent#4845 / #4852: the sync point a test
    /// uses before mutating a watched tree must wait for the watch to be
    /// LIVE, not merely settled. A registration settled as failed during a
    /// creation retry must keep the waiter parked (a) and release it once the
    /// retry re-registers the root (b) — otherwise the test writes before the
    /// OS watch exists and its event wait hangs into nextest's kill.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn probe_wait_live_rides_out_creation_retry() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("probe-live");
        let root = parent.path.join("ws");
        std::fs::create_dir_all(&root).expect("mk ws");

        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let fail_in_factory = Arc::clone(&fail);
        let hub = SharedWatchHub::with_factory(Arc::new(move |callback: EventCallback| {
            if fail_in_factory.load(Ordering::SeqCst) {
                Err(notify::Error::generic("injected creation failure"))
            } else {
                notify::recommended_watcher(callback)
                    .map(|w| Box::new(w) as Box<dyn Watcher + Send>)
            }
        }));

        let (sub, _rx, _canonical) = hub.subscribe(&root);
        sub.wait_established(LIVENESS).await;
        assert!(
            sub.registration.failed(),
            "precondition: creation failure settles the registration as failed"
        );

        // (a) Settled-as-failed is not live: the probe keeps waiting.
        let probe = sub.probe();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), probe.wait_live(LIVENESS))
                .await
                .is_err(),
            "wait_live must not return on a registration failed during a creation retry"
        );

        // (b) Once the factory recovers the retry re-registers the root and
        // the probe releases with the registration live.
        fail.store(false, Ordering::SeqCst);
        probe.wait_live(LIVENESS).await;
        assert!(
            sub.registration.live(),
            "wait_live must return only once live"
        );
    }

    /// The health handle tracks the hub through its lifecycle: `None` before
    /// attachment, healthy counts while watches are live, failed-root counts
    /// when a registration settles as failed, and `None` again once the hub
    /// is dropped (the `Weak` must not extend the hub's lifetime).
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn watch_health_snapshot_tracks_roots_failures_and_hub_lifetime() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("health");
        let a = parent.path.join("ws-a");
        let b = parent.path.join("ws-b");
        std::fs::create_dir_all(&a).expect("mk ws-a");
        std::fs::create_dir_all(&b).expect("mk ws-b");

        let health = WatchHealth::default();
        assert!(
            health.snapshot().is_none(),
            "unattached handle must report None, not a fake healthy zero"
        );

        let hub = SharedWatchHub::new();
        health.attach(&hub);
        let (sub_a, _rx_a, canonical_a) = hub.subscribe(&a);
        let (sub_b, _rx_b, _) = hub.subscribe(&b);
        hub.wait_all_established(2, LIVENESS).await;

        let snap = health.snapshot().expect("attached handle must snapshot");
        assert_eq!(snap.total_roots, 2);
        assert_eq!(snap.failed_roots, 0, "established roots are not failures");
        assert_eq!(snap.active_streams, hub.stream_count());

        // A settled failure surfaces as a failed root; the other root's
        // health is unaffected.
        {
            let state = hub.state.lock().unwrap();
            let entry = state
                .groups
                .values()
                .find_map(|g| g.roots.get(&canonical_a))
                .expect("root a must be tracked");
            entry.registration.settle(false);
        }
        let snap = health.snapshot().expect("snapshot after failure");
        assert_eq!(snap.total_roots, 2);
        assert_eq!(snap.failed_roots, 1);

        drop((sub_a, sub_b));
        drop(hub);
        assert!(
            health.snapshot().is_none(),
            "a dropped hub must read as None, not a stale snapshot"
        );
    }

    /// Watcher-creation failure shows up in the snapshot as failed roots (the
    /// registrar settles incoming registrations as failed while no watcher
    /// exists), and recovery drains the count back to zero.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn watch_health_reflects_creation_failure_and_recovery() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = TempDir::new("health-create-fail");
        let root = parent.path.join("ws");
        std::fs::create_dir_all(&root).expect("mk ws");

        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let fail_in_factory = Arc::clone(&fail);
        let hub = SharedWatchHub::with_factory(Arc::new(move |callback: EventCallback| {
            if fail_in_factory.load(Ordering::SeqCst) {
                Err(notify::Error::generic("injected creation failure"))
            } else {
                notify::recommended_watcher(callback)
                    .map(|w| Box::new(w) as Box<dyn Watcher + Send>)
            }
        }));
        let health = WatchHealth::default();
        health.attach(&hub);

        let (sub, _rx, _) = hub.subscribe(&root);
        sub.wait_established(LIVENESS).await;
        let snap = health.snapshot().expect("snapshot while degraded");
        assert_eq!(snap.total_roots, 1);
        assert_eq!(
            snap.failed_roots, 1,
            "creation failure must surface as a failed root"
        );
        assert_eq!(
            snap.active_streams, 0,
            "a group whose watcher never got created is not an active stream"
        );

        fail.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let snap = health.snapshot().expect("snapshot during recovery");
            if snap.failed_roots == 0 && snap.total_roots == 1 && snap.active_streams == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "failed roots must drain and the stream go active once the factory recovers, still {snap:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
