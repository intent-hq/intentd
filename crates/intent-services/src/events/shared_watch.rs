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
//! is a single object, a group's registrations are serialized on its own thread;
//! isolation is therefore per group, which is the useful granularity — a wedged
//! backend is a property of the volume the group's roots live on, and every
//! other group (and the runtime) keeps making progress.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::root_watch::{canonical_root, find_existing_ancestor};

/// Event callback a group's watcher invokes with each raw result; boxed so the
/// watcher factory below can be swapped out.
type EventCallback = Box<dyn FnMut(notify::Result<notify::Event>) + Send>;

/// Builds one group's watcher. Production is [`notify::recommended_watcher`];
/// tests inject failing factories to exercise the creation-retry path.
type WatcherFactory =
    dyn Fn(EventCallback) -> notify::Result<Box<dyn Watcher + Send>> + Send + Sync;

/// One demux destination: raw events whose paths fall under `root` are cloned
/// into `tx`.
struct Sink {
    id: u64,
    root: PathBuf,
    tx: mpsc::UnboundedSender<notify::Event>,
}

/// Command to a group's registrar thread. Dropping the sender ends the thread,
/// which drops the watcher and tears the stream down. `Watch` carries the
/// [`Registration`] the registrar settles once the root is actually registered.
enum Cmd {
    Watch(PathBuf, Arc<Registration>),
    Unwatch(PathBuf),
}

/// Outcome of one deferred `watcher.watch()`, shared between the registrar
/// thread and everyone waiting on it.
///
/// Failure is tracked distinctly from success because a root that failed to
/// register is dead but still refcounted: without the distinction a later
/// subscriber would join the existing `Root` entry, never re-send `Cmd::Watch`,
/// and silently receive a channel that can never deliver — with no recovery
/// until every subscriber drops. [`SharedWatchHub::subscribe`] retries such a
/// root instead.
#[derive(Default)]
struct Registration {
    /// [`REG_PENDING`] / [`REG_LIVE`] / [`REG_FAILED`].
    state: std::sync::atomic::AtomicU8,
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

    fn settle(&self, live: bool) {
        self.state
            .store(if live { REG_LIVE } else { REG_FAILED }, Ordering::Release);
    }

    fn reset(&self) {
        self.state.store(REG_PENDING, Ordering::Release);
    }
}

/// A watched root: how many subscribers reference it (two workspaces can
/// resolve to the same root) and the state of its deferred registration.
struct Root {
    subscribers: usize,
    registration: Arc<Registration>,
}

/// One shared stream: the registrar handle, the roots on it, and its demux
/// sinks.
struct Group {
    cmd: std::sync::mpsc::Sender<Cmd>,
    roots: HashMap<PathBuf, Root>,
    sinks: Arc<Mutex<Vec<Sink>>>,
}

#[derive(Default)]
struct HubState {
    groups: HashMap<PathBuf, Group>,
    next_id: u64,
}

/// Owns the shared streams and the demux table. Held by the
/// [`super::registry::WatcherRegistry`] for the daemon's lifetime; dropping it
/// drops every group, which ends the registrar threads and the streams.
pub(super) struct SharedWatchHub {
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
    /// Live shared streams (groups) — one OS watcher each.
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
        let mut total_roots = 0;
        let mut failed_roots = 0;
        for group in state.groups.values() {
            for root in group.roots.values() {
                total_roots += 1;
                if root.registration.failed() {
                    failed_roots += 1;
                }
            }
        }
        Some(WatchHealthSnapshot {
            active_streams: state.groups.len(),
            total_roots,
            failed_roots,
        })
    }
}

/// A live subscription. Dropping it removes the sink and, when the root has no
/// subscribers left, unwatches it (and retires the group once it is empty).
pub(super) struct SubHandle {
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
    pub(super) async fn wait_established(&self, timeout: std::time::Duration) {
        wait_settled(&self.registration, timeout).await;
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
            group.roots.remove(&self.root);
            let _ = group.cmd.send(Cmd::Unwatch(self.root.clone()));
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
    pub(super) fn new() -> Arc<Self> {
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

    /// Subscribe to raw events under `root`, joining (or starting) the shared
    /// stream for its group. Returns the canonical root the demux matches
    /// against, so callers can build their own path filters on the same form
    /// the OS reports.
    pub(super) fn subscribe(
        self: &Arc<Self>,
        root: &Path,
    ) -> (SubHandle, mpsc::UnboundedReceiver<notify::Event>, PathBuf) {
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
            Group {
                cmd: spawn_registrar(
                    Arc::clone(&sinks),
                    group_key.clone(),
                    Arc::clone(&self.factory),
                ),
                roots: HashMap::new(),
                sinks,
            }
        });
        if let Ok(mut sinks) = group.sinks.lock() {
            sinks.push(Sink {
                id,
                root: root.clone(),
                tx,
            });
        }
        let entry = group.roots.entry(root.clone()).or_insert_with(|| Root {
            subscribers: 0,
            registration: Arc::new(Registration::default()),
        });
        entry.subscribers += 1;
        // A root whose registration failed is dead but still refcounted, so a
        // new subscriber joining it would otherwise inherit a channel that can
        // never deliver, with no recovery until every subscriber drops. Retry
        // instead: a transient cause (the directory briefly missing) resolves,
        // and a persistent one just fails again and is logged again.
        let retry_failed = entry.subscribers > 1 && entry.registration.failed();
        if retry_failed {
            entry.registration.reset();
        }
        let registration = Arc::clone(&entry.registration);
        if entry.subscribers == 1 || retry_failed {
            let _ = group
                .cmd
                .send(Cmd::Watch(root.clone(), Arc::clone(&registration)));
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
const CREATE_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Ceiling for the creation-retry backoff, so a persistent failure (fd
/// exhaustion, intent-hq/intent#3708) keeps probing about once a minute.
const CREATE_RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(60);

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
) -> std::sync::mpsc::Sender<Cmd> {
    let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
    std::thread::spawn(move || {
        let make = move || {
            let sinks = Arc::clone(&sinks);
            factory(Box::new(
                move |res: notify::Result<notify::Event>| match res {
                    Ok(event) => demux(&sinks, &event),
                    Err(e) => {
                        tracing::warn!(error = %e, "shared watcher callback error; events may be missed");
                    }
                },
            ))
        };
        let Some(mut watcher) = build_watcher_serving(&rx, make, &group) else {
            // Every sender dropped: the group was retired before a watcher
            // could be built.
            return;
        };
        while let Ok(cmd) = rx.recv() {
            match cmd {
                Cmd::Watch(root, registration) => {
                    register(watcher.as_mut(), &root, &registration);
                }
                Cmd::Unwatch(root) => {
                    if let Err(e) = watcher.unwatch(&root) {
                        tracing::debug!(root = %root.display(), error = %e, "shared watch removal failed");
                    }
                }
            }
        }
    });
    tx
}

/// Obtain the group's watcher, surviving creation failure. On failure the
/// registrar does NOT return: it keeps serving the command channel — every
/// incoming `Cmd::Watch` settles as failed immediately, so waiters never hang,
/// and `Cmd::Unwatch` drops the root — while creation is retried with
/// exponential backoff capped at [`CREATE_RETRY_CAP`]. Roots settled as failed
/// while no watcher existed are re-registered once creation succeeds, because
/// the hub only re-sends `Cmd::Watch` when a NEW subscriber joins a failed root
/// ([`SharedWatchHub::subscribe`]'s retry) — without the re-registration a root
/// whose subscribers all predate the recovery would stay dead forever. `None`
/// when every sender dropped, i.e. the group was retired.
fn build_watcher_serving(
    rx: &std::sync::mpsc::Receiver<Cmd>,
    make: impl Fn() -> notify::Result<Box<dyn Watcher + Send>>,
    group: &Path,
) -> Option<Box<dyn Watcher + Send>> {
    let mut backoff = CREATE_RETRY_INITIAL;
    let mut failures = 0u64;
    let mut pending: Vec<(PathBuf, Arc<Registration>)> = Vec::new();
    loop {
        match make() {
            Ok(mut watcher) => {
                if failures > 0 {
                    tracing::info!(
                        group = %group.display(),
                        failed_attempts = failures,
                        "shared watcher created after earlier failures; re-registering its roots"
                    );
                }
                for (root, registration) in pending {
                    register(watcher.as_mut(), &root, &registration);
                }
                return Some(watcher);
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
                Ok(Cmd::Watch(root, registration)) => {
                    registration.settle(false);
                    pending.retain(|(r, _)| r != &root);
                    pending.push((root, registration));
                }
                Ok(Cmd::Unwatch(root)) => pending.retain(|(r, _)| r != &root),
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
fn register(watcher: &mut dyn Watcher, root: &Path, registration: &Registration) {
    match watcher.watch(root, RecursiveMode::Recursive) {
        Ok(()) => registration.settle(true),
        Err(e) => {
            tracing::warn!(
                root = %root.display(),
                error = %e,
                os_watch_limits = %os_watch_limits(),
                "shared watch registration failed"
            );
            registration.settle(false);
        }
    }
}

/// Human-readable OS watch limits for the failure WARNs, so an operator can
/// judge at a glance whether a failure is cap exhaustion. On Linux the live
/// inotify sysctls (`max_user_instances` / `max_user_watches`); elsewhere
/// there is no equivalent user-tunable cap to read, and unreadable procfs
/// values degrade to `?`.
pub(super) fn os_watch_limits() -> String {
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
/// be canonicalized directly) is attempted only for the paths that no sink
/// matched raw, so a busy stream costs no filesystem syscalls per event. Doing
/// it per path rather than per event matters for multi-path events: one path
/// matching raw must not suppress the fallback for a sibling path that needs it.
///
/// The routing table is snapshotted and the lock released before any of that
/// work happens. Resolution stats the filesystem, and holding the sinks lock
/// across it would make a wedged volume block `SubHandle::drop` — which holds
/// the hub-wide `state` mutex — and through it every other group's `subscribe`,
/// contradicting the per-group isolation this module claims. A sink that goes
/// away mid-send just makes the send a no-op.
fn demux(sinks: &Arc<Mutex<Vec<Sink>>>, event: &notify::Event) {
    let routes: Vec<(PathBuf, mpsc::UnboundedSender<notify::Event>)> = match sinks.lock() {
        Ok(sinks) => sinks
            .iter()
            .map(|s| (s.root.clone(), s.tx.clone()))
            .collect(),
        Err(_) => return,
    };

    let mut unmatched: Vec<usize> = (0..event.paths.len()).collect();
    for (root, tx) in &routes {
        let mine = narrow(event, &event.paths, root);
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
    for (root, tx) in &routes {
        let mine: Vec<PathBuf> = unmatched
            .iter()
            .filter(|i| resolved[**i].starts_with(root))
            .map(|i| event.paths[*i].clone())
            .collect();
        send_narrowed(tx, event, mine);
    }
}

/// The raw paths of `event` whose `candidates` counterpart falls under `root`.
fn narrow(event: &notify::Event, candidates: &[PathBuf], root: &Path) -> Vec<PathBuf> {
    candidates
        .iter()
        .zip(event.paths.iter())
        .filter(|(candidate, _)| candidate.starts_with(root))
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
    #[allow(clippy::await_holding_lock)]
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
    #[allow(clippy::await_holding_lock)]
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

    /// macOS keeps parent-directory grouping: the FSEvents stream rebuild on
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
                tx: tx_a,
            },
            Sink {
                id: 1,
                root: b.clone(),
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
            tx: tx_a,
        }]));

        let event = notify::Event::new(EventKind::Create(CreateKind::File))
            .add_path(PathBuf::from("/parent/loose.txt"));
        demux(&sinks, &event);

        assert!(rx_a.try_recv().is_err(), "unrelated path must not deliver");
    }

    /// The resolution fallback is per path, not per event: one path of a
    /// multi-path event matching raw must not suppress resolution for a sibling
    /// path that only reaches its sink after canonicalization.
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
                tx: tx_plain,
            },
            Sink {
                id: 1,
                root: canonical_real,
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
    #[allow(clippy::await_holding_lock)]
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

    /// The health handle tracks the hub through its lifecycle: `None` before
    /// attachment, healthy counts while watches are live, failed-root counts
    /// when a registration settles as failed, and `None` again once the hub
    /// is dropped (the `Weak` must not extend the hub's lifetime).
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
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
    #[allow(clippy::await_holding_lock)]
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

        fail.store(false, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let snap = health.snapshot().expect("snapshot during recovery");
            if snap.failed_roots == 0 && snap.total_roots == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "failed-root count must drain once the factory recovers, still {snap:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
