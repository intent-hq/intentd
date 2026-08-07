//! Shared FSEvents streams + in-process demux.
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
use std::sync::atomic::{AtomicBool, Ordering};
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
/// which drops the watcher and tears the stream down. `Watch` carries the flag
/// the registrar raises once the root is actually registered.
enum Cmd {
    Watch(PathBuf, Arc<AtomicBool>),
    Unwatch(PathBuf),
}

/// A watched root: how many subscribers reference it (two workspaces can
/// resolve to the same root) and whether its registration has landed.
struct Root {
    subscribers: usize,
    established: Arc<AtomicBool>,
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
    /// Only tests read this; production code never needs to know when a
    /// registration landed, which is the whole point of deferring it.
    #[cfg(test)]
    established: Arc<AtomicBool>,
}

impl SubHandle {
    /// Await this subscription's root actually being registered with the OS.
    /// Registration is deferred off the caller's thread (monorepo#1572), so
    /// tests must wait for it before mutating the watched tree.
    #[cfg(test)]
    pub(super) async fn wait_established(&self, timeout: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while !self.established.load(Ordering::Acquire) {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

impl Drop for SubHandle {
    fn drop(&mut self) {
        let mut state = match self.hub.state.lock() {
            Ok(state) => state,
            Err(e) => e.into_inner(),
        };
        let Some(group) = state.groups.get_mut(&self.group) else {
            return;
        };
        if let Ok(mut sinks) = group.sinks.lock() {
            sinks.retain(|s| s.id != self.id);
        }
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
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        let group_key = root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| root.clone());
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
            established: Arc::new(AtomicBool::new(false)),
        });
        entry.subscribers += 1;
        let established = Arc::clone(&entry.established);
        if entry.subscribers == 1 {
            let _ = group
                .cmd
                .send(Cmd::Watch(root.clone(), Arc::clone(&established)));
        }
        drop(state);

        (
            SubHandle {
                hub: Arc::clone(self),
                group: group_key,
                root: root.clone(),
                id,
                #[cfg(test)]
                established,
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
    /// `Some(false)` when a watch is requested but has not landed yet,
    /// `Some(true)` once the OS has it. Path is canonicalized to match the form
    /// [`Self::subscribe`] keys roots by.
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
            .map(|r| r.established.load(Ordering::Acquire))
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
                roots.len() >= expect_roots
                    && roots.iter().all(|r| r.established.load(Ordering::Acquire))
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
                Cmd::Watch(root, established) => {
                    if let Err(e) = watcher.watch(&root, RecursiveMode::Recursive) {
                        tracing::warn!(root = %root.display(), error = %e, "shared watch registration failed");
                    }
                    // Raised either way: a failed registration will never
                    // establish, and waiters must not hang on it.
                    established.store(true, Ordering::Release);
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

/// Route one raw event to every sink whose root contains any of its paths.
///
/// The cheap `starts_with` pass runs first and is the only one needed in
/// practice: the roots are canonicalized at subscribe time and FSEvents reports
/// canonical paths. Only when no sink matches raw are the paths resolved (a
/// symlinked root, or a deleted path that cannot be canonicalized directly) and
/// the pass repeated, so a busy stream costs no filesystem syscalls per event.
fn demux(sinks: &Arc<Mutex<Vec<Sink>>>, event: &notify::Event) {
    let Ok(sinks) = sinks.lock() else {
        return;
    };
    let mut matched = false;
    for sink in sinks.iter() {
        if event.paths.iter().any(|p| p.starts_with(&sink.root)) {
            matched = true;
            let _ = sink.tx.send(event.clone());
        }
    }
    if matched {
        return;
    }
    let resolved: Vec<PathBuf> = event
        .paths
        .iter()
        .map(|p| canonical_root(p, &find_existing_ancestor(p)))
        .collect();
    for sink in sinks.iter() {
        if resolved.iter().any(|p| p.starts_with(&sink.root)) {
            let _ = sink.tx.send(event.clone());
        }
    }
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
/// simply seen. `on_change` still fires once up front, matching the catch-up
/// flush `watch_root` performed, so a change landing before the registration
/// lands is not absorbed as pre-existing (callers' fingerprint checks suppress
/// the no-op case).
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
    on_change();
    let task = tokio::spawn(async move {
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
                Ok(Some(_)) => continue,
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
            .unwrap_or_else(|e| e.into_inner());
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
        hub.wait_all_established(2, Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Probe until delivery is actually flowing, then assert isolation.
        for attempt in 0..40 {
            std::fs::write(a.join(".probe"), format!("{attempt}")).expect("write probe");
            if next_for(&mut rx_a, &root_a, ".probe", Duration::from_millis(500))
                .await
                .is_some()
            {
                break;
            }
            assert!(attempt < 39, "shared stream never began delivering");
        }
        std::fs::remove_file(a.join(".probe")).expect("rm probe");
        while next_for(&mut rx_a, &root_a, ".probe", Duration::from_millis(300))
            .await
            .is_some()
        {}
        while rx_b.try_recv().is_ok() {}

        std::fs::write(a.join("only-a.txt"), "x").expect("write in a");
        assert!(
            next_for(&mut rx_a, &root_a, "only-a.txt", Duration::from_secs(10))
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
            .unwrap_or_else(|e| e.into_inner());
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
}
