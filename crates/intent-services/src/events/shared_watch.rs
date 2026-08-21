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
//! [`SharedWatchHub`] collapses that fan-out. Workspace roots are grouped by
//! their parent directory, each group owns ONE `notify` watcher, and every root
//! in the group is added to it with a further `watch()` call (`notify` appends
//! to the same stream rather than creating another). Consumers no longer create
//! watchers at all: they [`SharedWatchHub::subscribe`] to a root and receive the
//! raw `notify` events whose paths fall under it, demuxed in-process by path
//! prefix. Filtering, debouncing and publishing stay exactly where they were, so
//! the `file:*` wire shape is untouched.
//!
//! The trade-off is that `notify`'s macOS backend rebuilds a group's stream on
//! every `watch`/`unwatch` (`watch_inner` stops the run loop, appends the path
//! and starts again), so registering or retiring one root briefly interrupts
//! delivery for the group's other roots. That was already true per workspace,
//! and the transitions that trigger it are rare workspace lifecycle events
//! (create/open/close/delete, archive/unarchive) — none of them a steady-state
//! path.
//!
//! Registration never runs on the caller's thread — the reasoning of
//! intent-hq/monorepo#1572 applies unchanged: a `notify` registration can park
//! indefinitely and used to stall daemon startup. Each group therefore owns a
//! detached OS thread (a `register_off_thread`-shaped registrar, deliberately
//! not `spawn_blocking`, which the runtime waits for on shutdown) that builds
//! the watcher and serves `watch`/`unwatch` commands. Subscribing and
//! unsubscribing are pure in-memory bookkeeping plus a channel send, so they
//! never block a caller and a failed registration is logged and skipped without
//! affecting the other roots or the other groups. Because one `notify` watcher
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
#[derive(Default)]
pub(super) struct SharedWatchHub {
    state: Mutex<HubState>,
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

impl SharedWatchHub {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self::default())
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
        let group_key = root
            .parent()
            .map_or_else(|| root.clone(), Path::to_path_buf);
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
                cmd: spawn_registrar(Arc::clone(&sinks), group_key.clone()),
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

/// Start a group's registrar: a DETACHED OS thread that builds the shared
/// watcher and then serves `watch`/`unwatch` commands. Detached rather than
/// `spawn_blocking` for the intent-hq/monorepo#1572 reason — the runtime waits
/// for the blocking pool on shutdown, so a registration parked inside a wedged
/// backend would turn a startup stall into a shutdown hang. Failures are logged
/// and skipped; one bad root never stops the others.
fn spawn_registrar(sinks: Arc<Mutex<Vec<Sink>>>, group: PathBuf) -> std::sync::mpsc::Sender<Cmd> {
    let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
    std::thread::spawn(move || {
        let mut watcher = match notify::recommended_watcher(
            move |res: notify::Result<notify::Event>| match res {
                Ok(event) => demux(&sinks, &event),
                Err(e) => {
                    tracing::warn!(error = %e, "shared watcher callback error; events may be missed");
                }
            },
        ) {
            Ok(watcher) => watcher,
            Err(e) => {
                tracing::warn!(group = %group.display(), error = %e, "shared watcher creation failed; roots in this group are unwatched");
                return;
            }
        };
        while let Ok(cmd) = rx.recv() {
            match cmd {
                Cmd::Watch(root, registration) => {
                    // Settled either way, so waiters do not hang on a failure;
                    // the failed state is distinct so a later subscriber to the
                    // same root can retry it rather than inheriting a dead
                    // channel.
                    match watcher.watch(&root, RecursiveMode::Recursive) {
                        Ok(()) => registration.settle(true),
                        Err(e) => {
                            tracing::warn!(root = %root.display(), error = %e, "shared watch registration failed");
                            registration.settle(false);
                        }
                    }
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
}
