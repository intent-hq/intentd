//! The unified PTY host (§12.1): spawn, attach (back-fill + live tail), write
//! (serialized stdin), resize, signal, and scope-scoped kill with no orphans.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use portable_pty::{
    native_pty_system, Child, ChildKiller, CommandBuilder, MasterPty, PtyPair,
    PtySize as PortablePtySize, SlavePty,
};
use tokio::sync::broadcast;

use intent_core::{Error, Result};

use crate::scrollback::{Scrollback, DEFAULT_SCROLLBACK_BYTES};

/// Broadcast backlog of output chunks buffered per subscriber before lagging.
const FANOUT_CAPACITY: usize = 2048;
/// Read chunk size for the PTY reader loop.
const READ_CHUNK: usize = 8192;
/// Grace period between SIGTERM and SIGKILL during teardown (mirrors M5).
const TERM_GRACE: Duration = Duration::from_secs(2);
/// Poll interval while waiting for a signalled child to exit.
const REAP_POLL: Duration = Duration::from_millis(20);
/// Poll interval while the exit watcher waits for the reader to drain the
/// master queue after the child has been reaped (monorepo#587).
const DRAIN_POLL: Duration = Duration::from_millis(5);
/// Upper bound on that drain wait: the child is already dead, so the queue is
/// finite and normally empties within a few reads — the bound only guards
/// against a wedged reader keeping the held slave fd open forever.
const DRAIN_GRACE: Duration = Duration::from_secs(5);
/// Attempt budget for `openpty`: transient pty/fd-pool exhaustion (e.g.
/// EMFILE/ENFILE-class errors under heavy parallel load — monorepo#653)
/// usually clears within milliseconds as other PTYs close, so a short
/// backed-off retry rides it out. A persistent failure still surfaces as the
/// same `Internal` error after the last attempt.
const OPENPTY_ATTEMPTS: u32 = 8;
/// Initial backoff between `openpty` attempts; doubles per retry up to
/// [`OPENPTY_BACKOFF_CAP`] (~0.8s worst-case total across all attempts).
const OPENPTY_BACKOFF: Duration = Duration::from_millis(10);
/// Ceiling for the per-attempt `openpty` backoff.
const OPENPTY_BACKOFF_CAP: Duration = Duration::from_millis(250);

/// Opaque identifier for a spawned PTY, unique within a [`PtyHost`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct PtyId(u64);

impl std::fmt::Display for PtyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pty-{}", self.0)
    }
}

impl PtyId {
    /// Parse the `pty-{n}` [`Display`](std::fmt::Display) form back into a
    /// `PtyId` (the wire id used by `terminal.*` / ACP `terminal/*`). Returns
    /// `None` for any other shape.
    pub fn parse(s: &str) -> Option<PtyId> {
        s.strip_prefix("pty-")
            .and_then(|n| n.parse::<u64>().ok())
            .map(PtyId)
    }
}

/// Visible terminal dimensions in character cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PtySize {
    pub rows: u16,
    pub cols: u16,
}

impl Default for PtySize {
    fn default() -> Self {
        Self { rows: 24, cols: 80 }
    }
}

impl PtySize {
    fn to_portable(self) -> PortablePtySize {
        PortablePtySize {
            rows: self.rows,
            cols: self.cols,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

/// A terminated PTY child's exit status. `signal` is unavailable through the
/// `portable-pty` child abstraction, so only `exit_code` is populated; callers
/// that need richer parity treat a non-success code as the failure indicator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PtyExit {
    /// The raw process exit code as reported by the platform.
    pub exit_code: u32,
    /// Whether the process exited successfully (code 0).
    pub success: bool,
}

/// A signal to deliver to a PTY's process group.
#[derive(Clone, Copy, Debug)]
pub enum PtySignal {
    /// SIGINT — the Ctrl-C interrupt.
    Interrupt,
    /// SIGTERM — graceful termination request.
    Terminate,
    /// SIGKILL — unconditional kill.
    Kill,
}

/// Inputs for spawning a PTY. Both terminals and scripts use this shape.
#[derive(Clone, Debug)]
pub struct SpawnSpec {
    /// Lifetime scope (e.g. a session or workspace id); kill the scope to kill
    /// this PTY along with its peers.
    pub scope: String,
    /// Program to execute (argv[0]).
    pub command: String,
    /// Arguments passed to the program.
    pub args: Vec<String>,
    /// Working directory; inherits the daemon's cwd when `None`.
    pub cwd: Option<PathBuf>,
    /// Environment overrides layered onto the inherited environment.
    pub env: Vec<(String, String)>,
    /// Environment variable names to remove from the inherited environment.
    /// The overlay in `env` can only add/override keys, so scrubbing an
    /// inherited var (e.g. `npm_config_prefix`, which breaks nvm) needs an
    /// explicit removal applied via `CommandBuilder::env_remove`.
    pub env_remove: Vec<String>,
    /// Initial terminal size.
    pub size: PtySize,
    /// Scrollback retention budget in bytes.
    pub scrollback_bytes: usize,
    /// Optional display name surfaced through `terminal.list` (e.g. "Setup
    /// Script"); `None` for unnamed terminals.
    pub name: Option<String>,
    /// Whether this PTY is surfaced through `terminal.list`. Script-owned PTYs
    /// set this false because scripts have their own list/runtime UI.
    pub listed: bool,
}

impl SpawnSpec {
    /// A spec for `command` in `scope` with default size and scrollback.
    pub fn new(scope: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            env_remove: Vec::new(),
            size: PtySize::default(),
            scrollback_bytes: DEFAULT_SCROLLBACK_BYTES,
            name: None,
            listed: true,
        }
    }
}

/// A client's view onto a PTY: the recent history to render first, then a live
/// receiver tailing every subsequent output chunk (§12.1 back-fill-then-tail).
pub struct Attachment {
    /// Recent scrollback captured at attach time, to be written before tailing.
    pub backlog: Vec<u8>,
    /// Live output stream; each item is a shared output chunk.
    pub live: broadcast::Receiver<Arc<Vec<u8>>>,
}

/// Scrollback + broadcast guarded together so attach (snapshot + subscribe) and
/// the reader (append + send) are atomic relative to each other — guaranteeing a
/// late subscriber sees each chunk exactly once (history XOR live, never both).
struct Fanout {
    scrollback: Scrollback,
    tx: broadcast::Sender<Arc<Vec<u8>>>,
}

/// A point-in-time view of a tracked PTY's metadata (`terminal.list` /
/// `terminal.readOutput`). `cwd` is the working directory resolved at spawn;
/// `alive` reflects whether the child has not yet exited; `name` is the
/// optional display name given at spawn (`SpawnSpec::name`).
#[derive(Clone, Debug)]
pub struct PtyInfo {
    /// Lifetime scope the PTY was spawned under (workspace or session id).
    pub scope: String,
    /// Working directory resolved at spawn, if one could be determined.
    pub cwd: Option<String>,
    /// Whether the child process has not yet exited.
    pub alive: bool,
    /// Display name given at spawn, if any (`SpawnSpec::name`).
    pub name: Option<String>,
}

struct PtySession {
    scope: String,
    /// Display name given at spawn (`SpawnSpec::name`), if any.
    name: Option<String>,
    /// Whether this PTY is surfaced through `terminal.list`.
    listed: bool,
    /// Working directory resolved at spawn (`spec.cwd`, else the daemon's cwd).
    cwd: Option<String>,
    pid: Option<u32>,
    master: Mutex<Option<Box<dyn MasterPty + Send>>>,
    /// Parent-side slave end, held open until the child is reaped and the
    /// reader has drained the master queue (monorepo#587): if the child's exit
    /// closed the *last* slave fd, macOS would discard any PTY output the
    /// reader had not yet read — a fast-exiting child's entire output could be
    /// lost. The exit watcher (or teardown) takes it so the reader still
    /// observes EOF and its thread exits.
    slave: Mutex<Option<Box<dyn SlavePty + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
    child: Mutex<Box<dyn Child + Send + Sync>>,
    killer: Mutex<Box<dyn ChildKiller + Send + Sync>>,
    fanout: Arc<Mutex<Fanout>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    /// Exit watcher thread: reaps the child, then releases `slave` once the
    /// reader has drained the queue (see `exit_watch_loop`).
    watcher: Mutex<Option<JoinHandle<()>>>,
    /// Cached exit status, latched the first time the child is observed exited so
    /// it survives later teardown/removal (the `portable-pty` child only yields
    /// its status once).
    exit: Mutex<Option<PtyExit>>,
}

/// Latch and return a session's exit status: returns the cached value, or polls
/// the child once (non-blocking) and caches the result when it has exited.
fn observe_exit(session: &PtySession) -> Option<PtyExit> {
    let mut cached = session.exit.lock().unwrap();
    if let Some(exit) = cached.as_ref() {
        return Some(exit.clone());
    }
    match session.child.lock().unwrap().try_wait() {
        Ok(Some(status)) => {
            let exit = PtyExit {
                exit_code: status.exit_code(),
                success: status.success(),
            };
            *cached = Some(exit.clone());
            Some(exit)
        }
        _ => None,
    }
}

fn internal(e: impl std::fmt::Display) -> Error {
    Error::Internal(e.to_string())
}

/// Open a fresh PTY pair, retrying transient failures with bounded backoff
/// (monorepo#653). `openpty` has no non-transient failure mode for a valid
/// size — an error means pty/fd pressure — so every failure is retried until
/// the attempt budget runs out, then the last error is returned unchanged.
fn openpty_with_retry(size: PortablePtySize) -> Result<PtyPair> {
    retry_transient(|| native_pty_system().openpty(size))
}

/// Run `op` up to [`OPENPTY_ATTEMPTS`] times, sleeping an exponentially
/// growing backoff between attempts; the final attempt's error is mapped to
/// [`Error::Internal`].
fn retry_transient<T, E: std::fmt::Display>(
    mut op: impl FnMut() -> std::result::Result<T, E>,
) -> Result<T> {
    let mut backoff = OPENPTY_BACKOFF;
    for _ in 1..OPENPTY_ATTEMPTS {
        if let Ok(value) = op() {
            return Ok(value);
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(OPENPTY_BACKOFF_CAP);
    }
    op().map_err(internal)
}

/// The unified host owning every spawned PTY (terminals and scripts).
#[derive(Default)]
pub struct PtyHost {
    sessions: Mutex<HashMap<PtyId, Arc<PtySession>>>,
    next_id: AtomicU64,
    /// Latched by [`kill_all`](Self::kill_all) (clean daemon shutdown): once
    /// set, `spawn` refuses new sessions so a request already in flight when
    /// the shutdown sweep drains the map cannot register a PTY afterwards and
    /// leak its process group past daemon exit (monorepo#1526).
    closed: AtomicBool,
}

impl PtyHost {
    /// Create an empty host.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a process attached to a fresh PTY and start fanning out its output.
    ///
    /// This is a blocking call (`openpty`/fork are blocking syscalls); on
    /// transient `openpty` failure it additionally sleeps between bounded
    /// retries (monorepo#653), up to ~0.8s worst-case before giving up. Async
    /// callers that cannot tolerate that on a runtime thread should wrap the
    /// call in `spawn_blocking`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if `openpty` keeps failing after the bounded retries or spawning the child fails.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn spawn(&self, spec: SpawnSpec) -> Result<PtyId> {
        let pair = openpty_with_retry(spec.size.to_portable())?;

        let mut cmd = CommandBuilder::new(&spec.command);
        cmd.args(&spec.args);
        if let Some(cwd) = &spec.cwd {
            cmd.cwd(cwd);
        }
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        for k in &spec.env_remove {
            cmd.env_remove(k);
        }

        let child = pair.slave.spawn_command(cmd).map_err(internal)?;
        // Keep the parent-side slave open (monorepo#587): if we dropped it
        // here, a fast-exiting child would close the *last* slave fd before
        // the reader thread's first read(), and macOS discards buffered PTY
        // output on last-slave close — the child's output would be lost
        // entirely. The exit watcher releases it once the child is reaped and
        // the reader has drained the queue, so the reader still observes EOF
        // and its thread exits (no fd or thread leak).

        let pid = child.process_id();
        let killer = child.clone_killer();
        let writer = pair.master.take_writer().map_err(internal)?;
        let reader = pair.master.try_clone_reader().map_err(internal)?;

        let (tx, _rx) = broadcast::channel(FANOUT_CAPACITY);
        let fanout = Arc::new(Mutex::new(Fanout {
            scrollback: Scrollback::new(spec.scrollback_bytes),
            tx,
        }));

        let reader_fanout = Arc::clone(&fanout);
        let handle = std::thread::spawn(move || read_loop(reader, reader_fanout));

        let cwd = spec
            .cwd
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .or_else(|| {
                std::env::current_dir()
                    .ok()
                    .map(|p| p.to_string_lossy().into_owned())
            });

        let session = Arc::new(PtySession {
            scope: spec.scope,
            name: spec.name,
            listed: spec.listed,
            cwd,
            pid,
            master: Mutex::new(Some(pair.master)),
            slave: Mutex::new(Some(pair.slave)),
            writer: Mutex::new(writer),
            child: Mutex::new(child),
            killer: Mutex::new(killer),
            fanout,
            reader: Mutex::new(Some(handle)),
            watcher: Mutex::new(None),
            exit: Mutex::new(None),
        });

        let watcher_session = Arc::clone(&session);
        let watcher = std::thread::spawn(move || exit_watch_loop(&watcher_session));
        *session.watcher.lock().unwrap() = Some(watcher);

        let id = PtyId(self.next_id.fetch_add(1, Ordering::Relaxed));
        // Registration checks the shutdown latch under the same sessions lock
        // that `kill_all` drains: either this insert lands before the drain
        // (the sweep reaps it) or it observes `closed` and self-reaps here —
        // a spawn racing daemon shutdown can never leave an untracked group.
        {
            let mut sessions = self.sessions.lock().unwrap();
            if self.closed.load(Ordering::SeqCst) {
                drop(sessions);
                reap_refused_spawn(&session);
                return Err(internal("pty host is shut down"));
            }
            sessions.insert(id, session);
        }
        Ok(id)
    }

    /// Attach a new subscriber: capture recent scrollback to back-fill, then a
    /// live receiver for subsequent output. The snapshot and subscription are
    /// taken under one lock so no chunk is lost or duplicated across the seam.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn attach(&self, id: PtyId) -> Result<Attachment> {
        let session = self.get(id)?;
        let guard = session.fanout.lock().unwrap();
        let backlog = guard.scrollback.snapshot();
        let live = guard.tx.subscribe();
        drop(guard);
        Ok(Attachment { backlog, live })
    }

    /// Snapshot the PTY's current scrollback for replay (`terminal.getBuffer` /
    /// ACP `terminal/output`), without subscribing to live output.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn scrollback(&self, id: PtyId) -> Result<Vec<u8>> {
        let session = self.get(id)?;
        let guard = session.fanout.lock().unwrap();
        Ok(guard.scrollback.snapshot())
    }

    /// The PTY child's process id, if the platform reported one at spawn.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn pid(&self, id: PtyId) -> Option<u32> {
        self.sessions.lock().unwrap().get(&id).and_then(|s| s.pid)
    }

    /// The child's exit status if it has already exited, else `None`. Latches
    /// the status so it stays observable after the stream closes.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`.
    pub fn try_exit(&self, id: PtyId) -> Result<Option<PtyExit>> {
        let session = self.get(id)?;
        Ok(observe_exit(&session))
    }

    /// Wait until the PTY's child exits and return its status (ACP
    /// `terminal/wait_for_exit`). Polls the child rather than blocking a thread.
    ///
    /// Note: exit becomes observable as soon as the child is reaped, which can
    /// be slightly before the reader has drained the last of its output into
    /// scrollback (monorepo#587 makes that output eventually-complete rather
    /// than lost). Callers reading scrollback right after `wait()` should poll
    /// briefly rather than assume it is final.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`.
    pub async fn wait(&self, id: PtyId) -> Result<PtyExit> {
        let session = self.get(id)?;
        loop {
            if let Some(exit) = observe_exit(&session) {
                return Ok(exit);
            }
            tokio::time::sleep(REAP_POLL).await;
        }
    }

    /// Write input to the PTY master. The per-PTY writer mutex serializes
    /// concurrent writers so each write lands as one contiguous chunk (§12.1).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`; `Error::Internal` if the write or flush fails.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn write(&self, id: PtyId, data: &[u8]) -> Result<()> {
        let session = self.get(id)?;
        let mut writer = session.writer.lock().unwrap();
        writer.write_all(data).map_err(internal)?;
        writer.flush().map_err(internal)?;
        Ok(())
    }

    /// Resize the PTY's visible area.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`; `Error::Internal` if the master is already torn down or the resize fails.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn resize(&self, id: PtyId, size: PtySize) -> Result<()> {
        let session = self.get(id)?;
        let guard = session.master.lock().unwrap();
        let master = guard
            .as_ref()
            .ok_or_else(|| internal("pty master already torn down"))?;
        master.resize(size.to_portable()).map_err(internal)
    }

    /// Deliver a signal to the PTY's process group (SIGINT/Ctrl-C, etc.).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no session exists for `id`; `Error::Internal` if the session has no process id or delivering the signal fails.
    pub fn signal(&self, id: PtyId, sig: PtySignal) -> Result<()> {
        let session = self.get(id)?;
        #[cfg(unix)]
        {
            let pid = session
                .pid
                .ok_or_else(|| internal("pty has no process id"))?;
            kill_group(pid, sig).map_err(internal)
        }
        #[cfg(not(unix))]
        {
            if matches!(sig, PtySignal::Terminate | PtySignal::Kill) {
                session.killer.lock().unwrap().kill().map_err(internal)?;
            }
            Ok(())
        }
    }

    /// Whether the PTY exists and its child has not yet exited.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn is_alive(&self, id: PtyId) -> bool {
        match self.sessions.lock().unwrap().get(&id).cloned() {
            Some(session) => matches!(session.child.lock().unwrap().try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Metadata for one tracked PTY (`terminal.list` / `terminal.readOutput`):
    /// its `scope`, display name, working directory, and whether its child is
    /// still running. `None` when the id is unknown.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn info(&self, id: PtyId) -> Option<PtyInfo> {
        let session = self.sessions.lock().unwrap().get(&id).cloned()?;
        let alive = matches!(session.child.lock().unwrap().try_wait(), Ok(None));
        Some(PtyInfo {
            scope: session.scope.clone(),
            cwd: session.cwd.clone(),
            alive,
            name: session.name.clone(),
        })
    }

    /// Number of PTYs currently tracked.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    /// The live, list-visible PTYs currently tracked under `scope`. Hidden and
    /// exited sessions remain addressable for output and explicit release.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub fn list_scope(&self, scope: &str) -> Vec<PtyId> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.scope == scope && s.listed && observe_exit(s).is_none())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Kill one PTY and reap its whole process group. Returns whether it existed.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub async fn kill(&self, id: PtyId) -> bool {
        let session = self.sessions.lock().unwrap().remove(&id);
        match session {
            Some(session) => {
                teardown(&session).await;
                true
            }
            None => false,
        }
    }

    /// Reap process-group members that outlived the direct child: once the
    /// child has exited, any remaining group members get the same
    /// SIGTERM→grace→SIGKILL escalation as teardown, keyed on the group being
    /// empty (monorepo#1300). The session stays registered so its scrollback
    /// and latched exit status remain readable. No-op when the session is
    /// unknown, the child is still running (the group is legitimately
    /// alive), or the group is already empty.
    pub async fn reap_group_stragglers(&self, id: PtyId) {
        let Ok(session) = self.get(id) else { return };
        #[cfg(unix)]
        {
            if observe_exit(&session).is_none() {
                return;
            }
            if let Some(pid) = session.pid {
                escalate_group_kill(pid, &session).await;
            }
        }
        #[cfg(not(unix))]
        drop(session);
    }

    /// Kill every PTY under `scope` (session/workspace teardown). Returns the
    /// number reaped. No process-group orphans are left behind.
    #[cfg(test)]
    pub(crate) async fn kill_scope(&self, scope: &str) -> usize {
        let victims: Vec<Arc<PtySession>> = {
            let mut sessions = self.sessions.lock().unwrap();
            let ids: Vec<PtyId> = sessions
                .iter()
                .filter(|(_, s)| s.scope == scope)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| sessions.remove(&id))
                .collect()
        };
        let count = victims.len();
        for session in victims {
            teardown(&session).await;
        }
        count
    }

    /// Kill every tracked PTY across all scopes (clean daemon shutdown —
    /// monorepo#1526). Teardowns run concurrently so the wall-clock cost of
    /// the sweep stays one SIGTERM grace, not one per session, keeping the
    /// whole shutdown inside the FE sidecar's own kill grace. Returns the
    /// number reaped. No process-group orphans are left behind.
    ///
    /// Permanently closes the host: a `spawn` racing the sweep (e.g. a
    /// `terminal.create` already in flight on an accepted connection when the
    /// listener stops) either registers before the drain and is reaped by it,
    /// or observes the latch and is refused with its child reaped in place.
    ///
    /// # Panics
    ///
    /// Panics if a per-session mutex is poisoned (a prior panic while holding the lock).
    pub async fn kill_all(&self) -> usize {
        self.closed.store(true, Ordering::SeqCst);
        let victims: Vec<Arc<PtySession>> = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.drain().map(|(_, session)| session).collect()
        };
        let count = victims.len();
        let mut teardowns = tokio::task::JoinSet::new();
        for session in victims {
            teardowns.spawn(async move { teardown(&session).await });
        }
        while let Some(res) = teardowns.join_next().await {
            if let Err(e) = res {
                tracing::warn!(error = %e, "pty teardown task failed during kill_all");
            }
        }
        count
    }

    fn get(&self, id: PtyId) -> Result<Arc<PtySession>> {
        self.sessions
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("{id}")))
    }

    /// Whether the PTY's reader thread has finished (leak inspection).
    #[cfg(all(test, unix))]
    fn reader_finished(&self, id: PtyId) -> bool {
        self.sessions.lock().unwrap().get(&id).is_none_or(|s| {
            s.reader
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        })
    }

    /// Whether the parent-side slave fd is still held (leak inspection).
    #[cfg(all(test, unix))]
    fn slave_held(&self, id: PtyId) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .get(&id)
            .is_some_and(|s| s.slave.lock().unwrap().is_some())
    }
}

/// Whether the PTY master has unread output pending in the kernel queue
/// (POLLIN with a zero timeout). `false` once the reader has drained
/// everything, or when the master is already torn down. A poll error is
/// reported as *pending*: prematurely declaring "drained" would close the
/// held slave and could discard queued output (the exact loss this guards
/// against), while over-reporting only costs up to `DRAIN_GRACE`.
#[cfg(unix)]
fn master_pending(session: &PtySession) -> bool {
    use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
    use std::os::fd::BorrowedFd;

    let guard = session.master.lock().unwrap();
    let Some(fd) = guard.as_ref().and_then(|m| m.as_raw_fd()) else {
        return false;
    };
    // SAFETY: `guard` keeps the master (and thus `fd`) alive for the borrow.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let mut fds = [PollFd::new(borrowed, PollFlags::POLLIN)];
    match poll(&mut fds, PollTimeout::ZERO) {
        Ok(_) => fds[0]
            .revents()
            .is_some_and(|r| r.contains(PollFlags::POLLIN)),
        Err(_) => true,
    }
}

#[cfg(not(unix))]
fn master_pending(_session: &PtySession) -> bool {
    false
}

/// Per-PTY exit watcher (own thread): poll until the child is reaped (latching
/// its exit status), then wait — bounded — for the reader to drain any output
/// still queued on the master, and release the held slave fd so the reader
/// observes EOF and its thread exits (monorepo#587). Returns early when
/// teardown has already released the slave.
fn exit_watch_loop(session: &PtySession) {
    loop {
        if session.slave.lock().unwrap().is_none() {
            return; // torn down; teardown joins the reader itself
        }
        if observe_exit(session).is_some() {
            break;
        }
        std::thread::sleep(REAP_POLL);
    }
    // The child is reaped, so no further output arrives from it — only unread
    // bytes can remain queued. Closing the held slave while bytes are queued
    // would discard them on macOS (the very loss this fd guards against), so
    // wait for the reader to drain the queue first. The bound only protects
    // against a wedged reader; a grandchild that inherited the slave keeps
    // the stream open regardless of when we release ours.
    let deadline = std::time::Instant::now() + DRAIN_GRACE;
    while master_pending(session) && std::time::Instant::now() < deadline {
        std::thread::sleep(DRAIN_POLL);
    }
    session.slave.lock().unwrap().take();
}

/// Blocking reader loop (own thread): append each chunk to scrollback and
/// broadcast it under one lock so attach sees a consistent history/live seam.
fn read_loop(mut reader: Box<dyn Read + Send>, fanout: Arc<Mutex<Fanout>>) {
    let mut buf = [0u8; READ_CHUNK];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let chunk = Arc::new(buf[..n].to_vec());
                let mut guard = fanout.lock().unwrap();
                guard.scrollback.push(&chunk);
                let _ = guard.tx.send(chunk);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

/// Terminate a session's whole process group (SIGTERM→grace→SIGKILL), then drop
/// the master and join the reader thread. The PTY child is a `setsid` session
/// leader so `killpg` reaps grandchildren too (no orphans, mirroring M5).
async fn teardown(session: &PtySession) {
    #[cfg(unix)]
    {
        if let Some(pid) = session.pid {
            escalate_group_kill(pid, session).await;
        } else {
            let _ = session.killer.lock().unwrap().kill();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = session.killer.lock().unwrap().kill();
    }
    // Release the held slave and the master fd so the reader observes EOF,
    // then join the reader and exit-watcher threads (the watcher exits on its
    // own once it sees the slave released).
    session.slave.lock().unwrap().take();
    session.master.lock().unwrap().take();
    if let Some(handle) = session.reader.lock().unwrap().take() {
        let _ = handle.join();
    }
    if let Some(handle) = session.watcher.lock().unwrap().take() {
        let _ = handle.join();
    }
}

/// Reap a session whose registration was refused by the shutdown latch
/// (monorepo#1526): the child is milliseconds old and the daemon is exiting,
/// so SIGKILL its group immediately — no TERM grace — reap the direct child
/// (no zombie), then release the fds and join the session threads, exactly
/// as `teardown` would. Synchronous because `spawn` is.
fn reap_refused_spawn(session: &PtySession) {
    #[cfg(unix)]
    {
        if let Some(pid) = session.pid {
            let _ = kill_group(pid, PtySignal::Kill);
        } else {
            let _ = session.killer.lock().unwrap().kill();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = session.killer.lock().unwrap().kill();
    }
    // Blocking reap is fine: the child was just SIGKILLed, so `wait` returns
    // promptly, and this path only runs during daemon shutdown.
    let _ = session.child.lock().unwrap().wait();
    session.slave.lock().unwrap().take();
    session.master.lock().unwrap().take();
    if let Some(handle) = session.reader.lock().unwrap().take() {
        let _ = handle.join();
    }
    if let Some(handle) = session.watcher.lock().unwrap().take() {
        let _ = handle.join();
    }
}

/// SIGTERM the process group, then escalate to SIGKILL when the group is
/// non-empty after [`TERM_GRACE`]. Escalation is keyed on the *process
/// group* emptying, not on the direct child's exit: a descendant that
/// survives SIGTERM must still be `SIGKILLed` even when the shell itself
/// exited promptly (monorepo#1300). Keep reaping the direct child — a zombie
/// leader keeps the pgid occupied, so the ESRCH probe only reports empty
/// once the leader is reaped. Reap via `observe_exit` so the one-shot exit
/// status is latched for concurrent observers (`wait`/`try_exit`/the exit
/// watcher) instead of discarded.
#[cfg(unix)]
async fn escalate_group_kill(pid: u32, session: &PtySession) {
    let _ = kill_group(pid, PtySignal::Terminate);
    let mut group_empty = false;
    let iters = (TERM_GRACE.as_millis() / REAP_POLL.as_millis()).max(1);
    for _ in 0..iters {
        let _ = observe_exit(session);
        if process_group_empty(pid) {
            group_empty = true;
            break;
        }
        tokio::time::sleep(REAP_POLL).await;
    }
    if !group_empty {
        let _ = kill_group(pid, PtySignal::Kill);
    }
}

/// Signal a whole process group by its leader pid (pgid == pid via `setsid`).
#[cfg(unix)]
fn kill_group(pid: u32, sig: PtySignal) -> std::result::Result<(), nix::errno::Errno> {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    let signal = match sig {
        PtySignal::Interrupt => Signal::SIGINT,
        PtySignal::Terminate => Signal::SIGTERM,
        PtySignal::Kill => Signal::SIGKILL,
    };
    killpg(Pid::from_raw(pid as i32), signal)
}

/// Whether the process group led by `pid` (pgid == pid via `setsid`) has no
/// members left: probe with `killpg(pgid, 0)` (signal 0) and treat `ESRCH` as
/// empty. A zombie leader still occupies the pgid, so callers must pair this
/// probe with reaping the direct child.
#[cfg(unix)]
fn process_group_empty(pid: u32) -> bool {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    matches!(
        killpg(Pid::from_raw(pid as i32), None),
        Err(nix::errno::Errno::ESRCH)
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Instant;

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// Deadline scale factor for slow environments (coverage runs export
    /// `INTENTD_TEST_TIMEOUT_MULTIPLIER`); never below 1.0, and non-finite
    /// values (`inf`/`NaN`) are ignored so `Duration::mul_f64` cannot panic.
    fn timeout_multiplier() -> f64 {
        std::env::var("INTENTD_TEST_TIMEOUT_MULTIPLIER")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|m| m.is_finite())
            .unwrap_or(1.0)
            .max(1.0)
    }

    /// Drain a live receiver until `needle` is seen or the deadline passes.
    async fn collect_until(
        rx: &mut broadcast::Receiver<Arc<Vec<u8>>>,
        needle: &[u8],
        timeout: Duration,
    ) -> Vec<u8> {
        let mut acc = Vec::new();
        let deadline = Instant::now() + timeout;
        while !contains(&acc, needle) {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) => d,
                None => break,
            };
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(chunk)) => acc.extend_from_slice(&chunk),
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Err(_) => break,
            }
        }
        acc
    }

    /// Drain a live receiver until every needle in `needles` is present or the
    /// deadline passes. Used when output arrives in an arbitrary order and no
    /// single chunk can serve as a completion sentinel.
    async fn collect_until_all(
        rx: &mut broadcast::Receiver<Arc<Vec<u8>>>,
        needles: &[Vec<u8>],
        timeout: Duration,
    ) -> Vec<u8> {
        let mut acc = Vec::new();
        let deadline = Instant::now() + timeout;
        while !needles.iter().all(|n| contains(&acc, n)) {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) => d,
                None => break,
            };
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(chunk)) => acc.extend_from_slice(&chunk),
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Err(_) => break,
            }
        }
        acc
    }

    fn pid_alive(pid: u32) -> bool {
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        matches!(
            kill(Pid::from_raw(pid as i32), None),
            Ok(()) | Err(nix::errno::Errno::EPERM)
        )
    }

    /// Spec for `cat`, which echoes its stdin back through the PTY.
    fn cat_spec(scope: &str) -> SpawnSpec {
        SpawnSpec::new(scope, "cat")
    }

    /// Transient failures inside the attempt budget are retried to success and
    /// never surface to the caller (monorepo#653).
    #[test]
    fn retry_transient_rides_out_transient_failures() {
        let mut attempts = 0u32;
        let out = retry_transient(|| {
            attempts += 1;
            if attempts < 3 {
                Err("pty pool exhausted")
            } else {
                Ok(attempts)
            }
        });
        assert_eq!(out.unwrap(), 3);
    }

    /// A persistent failure exhausts the whole attempt budget, then surfaces
    /// the last error as `Internal` — the same shape as before the retry.
    #[test]
    fn retry_transient_surfaces_persistent_failure_after_budget() {
        let mut attempts = 0u32;
        let out: Result<()> = retry_transient(|| {
            attempts += 1;
            Err::<(), _>("failed to openpty")
        });
        assert_eq!(attempts, OPENPTY_ATTEMPTS);
        match out {
            Err(Error::Internal(msg)) => assert!(msg.contains("failed to openpty")),
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    /// `SpawnSpec::name` is surfaced through `info()`; unnamed PTYs stay `None`.
    #[tokio::test]
    async fn spawn_name_is_surfaced_through_info() {
        let host = PtyHost::new();
        let mut named = cat_spec("s");
        named.name = Some("Setup Script".to_string());
        let named_id = host.spawn(named).unwrap();
        let unnamed_id = host.spawn(cat_spec("s")).unwrap();

        assert_eq!(
            host.info(named_id).unwrap().name.as_deref(),
            Some("Setup Script")
        );
        assert_eq!(host.info(unnamed_id).unwrap().name, None);

        host.kill(named_id).await;
        host.kill(unnamed_id).await;
    }

    /// Hidden PTYs stay fully addressable by id while being omitted from the
    /// terminal-list view used by clients to build interactive terminal tabs.
    #[tokio::test]
    async fn hidden_pty_is_omitted_from_list_but_keeps_buffer_access() {
        let host = PtyHost::new();
        let mut hidden = cat_spec("s");
        hidden.listed = false;
        let hidden_id = host.spawn(hidden).unwrap();
        let visible_id = host.spawn(cat_spec("s")).unwrap();

        assert_eq!(host.list_scope("s"), vec![visible_id]);
        assert!(host.scrollback(hidden_id).is_ok());

        host.kill(hidden_id).await;
        host.kill(visible_id).await;
    }

    /// Every attached subscriber receives byte-identical fan-out (§12.1).
    #[tokio::test]
    async fn multi_subscriber_fan_out_is_identical() {
        let host = PtyHost::new();
        let id = host.spawn(cat_spec("s")).unwrap();
        let mut a = host.attach(id).unwrap().live;
        let mut b = host.attach(id).unwrap().live;

        host.write(id, b"ping-fanout\n").unwrap();

        let from_a = collect_until(&mut a, b"ping-fanout", Duration::from_secs(5)).await;
        let from_b = collect_until(&mut b, b"ping-fanout", Duration::from_secs(5)).await;
        assert!(contains(&from_a, b"ping-fanout"));
        assert_eq!(from_a, from_b, "subscribers must see identical output");

        host.kill(id).await;
    }

    /// A late subscriber back-fills history first, then tails live output, with
    /// no overlap across the seam (§12.1 back-fill-then-tail).
    #[tokio::test]
    async fn late_subscriber_backfills_history_then_tails_live() {
        let host = PtyHost::new();
        // Emit the history marker from the program's own stdout (one production),
        // not via stdin: a stdin write is echoed by the PTY line discipline AND by
        // `cat`, producing `HIST` twice. If an attach cut falls between those two
        // productions, the second copy is genuine post-snapshot output and lands
        // live — a test-marker race, not a seam bug. Sourcing it once keeps the
        // back-fill→tail seam deterministic without weakening the assertions.
        let mut spec = SpawnSpec::new("s", "sh");
        spec.args = vec!["-c".into(), "printf 'HIST\\n'; exec cat".into()];
        let id = host.spawn(spec).unwrap();

        // Poll fresh attachments until the reader has captured the history.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut attachment = host.attach(id).unwrap();
        while !contains(&attachment.backlog, b"HIST") {
            assert!(
                Instant::now() < deadline,
                "history never reached scrollback"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            attachment = host.attach(id).unwrap();
        }

        // History is in the backlog; the live future must not have replayed it.
        assert!(contains(&attachment.backlog, b"HIST"));
        assert!(!contains(&attachment.backlog, b"LIVE"));

        host.write(id, b"LIVE\n").unwrap();
        let live = collect_until(&mut attachment.live, b"LIVE", Duration::from_secs(5)).await;
        assert!(
            contains(&live, b"LIVE"),
            "live tail must deliver new output"
        );
        assert!(
            !contains(&live, b"HIST"),
            "history must not be replayed live"
        );

        host.kill(id).await;
    }

    /// Concurrent writers are serialized: each write lands as one contiguous
    /// chunk, so every distinct payload survives intact (§12.1).
    #[tokio::test]
    async fn concurrent_writes_are_serialized() {
        let host = Arc::new(PtyHost::new());
        let id = host.spawn(cat_spec("s")).unwrap();
        let mut rx = host.attach(id).unwrap().live;

        let n: u8 = 12;
        let mut tasks = Vec::new();
        for i in 0..n {
            let host = Arc::clone(&host);
            tasks.push(tokio::spawn(async move {
                let mut line = vec![b'a' + i; 64];
                line.push(b'\n');
                host.write(id, &line).unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }

        let payloads: Vec<Vec<u8>> = (0..n).map(|i| vec![b'a' + i; 64]).collect();
        let acc = collect_until_all(&mut rx, &payloads, Duration::from_secs(10)).await;
        for (i, payload) in payloads.iter().enumerate() {
            assert!(
                contains(&acc, payload),
                "payload {i} was split — writes interleaved"
            );
        }

        host.kill(id).await;
    }

    /// Resizing the PTY is reflected in the child's view of the terminal.
    #[tokio::test]
    async fn resize_updates_terminal_dimensions() {
        let host = PtyHost::new();
        let id = host.spawn(SpawnSpec::new("s", "sh")).unwrap();
        let mut rx = host.attach(id).unwrap().live;

        host.resize(
            id,
            PtySize {
                rows: 50,
                cols: 120,
            },
        )
        .unwrap();
        host.write(id, b"stty size\n").unwrap();

        let out = collect_until(&mut rx, b"50 120", Duration::from_secs(5)).await;
        assert!(
            contains(&out, b"50 120"),
            "resize not reflected by stty size"
        );

        host.write(id, b"exit\n").unwrap();
        host.kill(id).await;
    }

    /// Regression test for monorepo#587: a fast-exiting child's PTY output must
    /// not be lost. Pre-fix, `PtyHost::spawn` dropped the parent-side slave fd
    /// immediately, so when the child wrote and exited before the reader
    /// thread's first `read()` drained it, closing the last slave fd (the
    /// child's, at exit) discarded the buffered PTY output on macOS — the
    /// scrollback stayed empty forever even though `wait()` returned exit 0
    /// (see intentd#411 / monorepo#587). Concurrent spawn bursts of a bare
    /// `echo` (no shell startup) on a multi-threaded runtime amplify the repro
    /// odds under parallel host load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fast_exiting_child_output_is_captured() {
        let host = Arc::new(PtyHost::new());
        for round in 0..4 {
            let mut tasks = Vec::new();
            for i in 0..32 {
                let host = Arc::clone(&host);
                tasks.push(tokio::spawn(async move {
                    let marker = format!("pty587-fast-exit-{round}-{i}");
                    let mut spec = SpawnSpec::new("s", "echo");
                    spec.args = vec![marker.clone()];
                    let id = host.spawn(spec).unwrap();

                    // Exit codes must be unaffected by the race (or the fix).
                    let exit = host.wait(id).await.unwrap();
                    assert_eq!(exit.exit_code, 0, "{marker}: child must exit 0");
                    assert!(exit.success);

                    // Poll scrollback for the marker, event-driven rather than
                    // on a fixed clock (monorepo#648): the drain is provably
                    // over once the reader thread has exited (it only exits at
                    // EOF, after pushing everything it read into scrollback),
                    // so a missing marker at that point is genuine *loss*, not
                    // lateness. The generous deadline only guards against a
                    // drain that never completes on a badly overloaded host.
                    let deadline =
                        Instant::now() + Duration::from_secs(60).mul_f64(timeout_multiplier());
                    loop {
                        let drained = host.reader_finished(id);
                        let out = host.scrollback(id).unwrap();
                        if contains(&out, marker.as_bytes()) {
                            break;
                        }
                        if drained {
                            panic!(
                                "{marker}: fast-exiting child's output was lost; \
                                 exit 0 and reader drained to EOF but scrollback stayed {:?}",
                                String::from_utf8_lossy(&out)
                            );
                        }
                        if Instant::now() >= deadline {
                            panic!(
                                "{marker}: output drain never completed within deadline; \
                                 scrollback so far {:?}",
                                String::from_utf8_lossy(&out)
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }

                    host.kill(id).await;
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        }
        assert_eq!(host.count(), 0);
    }

    /// After a fast-exiting child is reaped and its output drained, the host
    /// releases the held slave fd on its own and the reader thread exits — no
    /// fd or reader-thread leak for naturally-exiting children (monorepo#587).
    #[tokio::test]
    async fn fast_exit_releases_slave_and_reader_without_kill() {
        let host = PtyHost::new();
        let mut spec = SpawnSpec::new("s", "echo");
        spec.args = vec!["pty587-leak-check".into()];
        let id = host.spawn(spec).unwrap();

        let exit = host.wait(id).await.unwrap();
        assert_eq!(exit.exit_code, 0);

        let deadline = Instant::now() + Duration::from_secs(10);
        while host.slave_held(id) || !host.reader_finished(id) {
            assert!(
                Instant::now() < deadline,
                "held slave fd or reader thread leaked after natural exit \
                 (slave_held={}, reader_finished={})",
                host.slave_held(id),
                host.reader_finished(id)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The drained output must have reached scrollback before the release.
        assert!(contains(
            &host.scrollback(id).unwrap(),
            b"pty587-leak-check"
        ));
        host.kill(id).await;
        assert_eq!(host.count(), 0);
    }

    /// A long-running child keeps the held slave and its reader thread until
    /// teardown; `kill()` releases both and joins the reader and watcher
    /// threads promptly (no leak on the kill path either).
    #[tokio::test]
    async fn long_running_child_holds_slave_until_kill() {
        let host = PtyHost::new();
        let id = host.spawn(cat_spec("s")).unwrap();
        let mut rx = host.attach(id).unwrap().live;

        host.write(id, b"still-alive\n").unwrap();
        let out = collect_until(&mut rx, b"still-alive", Duration::from_secs(5)).await;
        assert!(contains(&out, b"still-alive"));

        assert!(host.slave_held(id), "slave must stay held while running");
        assert!(
            !host.reader_finished(id),
            "reader must keep tailing a live child"
        );

        assert!(host.kill(id).await);
        assert_eq!(host.count(), 0);
    }

    /// Killing a scope reaps the whole process group: a backgrounded grandchild
    /// is terminated too, leaving no orphan (mirrors the M5 reaping test).
    #[tokio::test]
    async fn kill_scope_leaves_no_process_group_orphan() {
        let host = PtyHost::new();
        let mut spec = SpawnSpec::new("scope-x", "sh");
        spec.args = vec!["-c".into(), "sleep 300 & echo $!; sleep 300".into()];
        let id = host.spawn(spec).unwrap();

        // Poll the scrollback snapshot for the grandchild PID rather than
        // tailing a live receiver: the reader thread can capture the `echo $!`
        // line before `attach()` subscribes, in which case the PID only ever
        // exists in scrollback and a live tail would wait out the whole
        // deadline. Honor INTENTD_TEST_TIMEOUT_MULTIPLIER for coverage runs.
        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());

        let grandchild: u32 = loop {
            let out = host.scrollback(id).unwrap();
            let text = String::from_utf8_lossy(&out);
            if let Some(pid) = text.split_whitespace().find_map(|t| t.parse().ok()) {
                break pid;
            }
            if Instant::now() >= deadline {
                panic!("grandchild pid never printed within deadline; scrollback: {text:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        assert!(pid_alive(grandchild), "grandchild alive before teardown");

        let reaped = host.kill_scope("scope-x").await;
        assert_eq!(reaped, 1);
        assert_eq!(host.count(), 0);
        assert!(!host.is_alive(id));

        let mut dead = false;
        for _ in 0..100 {
            if !pid_alive(grandchild) {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(dead, "grandchild must die with its process group");
    }

    /// SIGKILL escalation is keyed on the *process group* being empty, not on
    /// the direct child's exit: the shell dies to SIGTERM within the grace
    /// period while its signal-trapping descendant survives, so teardown must
    /// still escalate to SIGKILL (monorepo#1300).
    #[tokio::test]
    async fn teardown_kills_term_trapping_descendant_after_shell_exit() {
        let host = PtyHost::new();
        let mut spec = SpawnSpec::new("scope-trap", "sh");
        // The descendant prints its PID *after* installing the traps so the
        // test never signals it before TERM/HUP are ignored (HUP too: the
        // session leader's death sends SIGHUP to the foreground group). It
        // sleeps in a loop rather than one long `sleep`: the sleeps are
        // separate children, so even if the shell's wait is interrupted by a
        // sleep dying to SIGTERM, the trapped loop keeps running — only
        // SIGKILL removes the descendant.
        spec.args = vec![
            "-c".into(),
            r#"sh -c 'trap "" TERM HUP; echo "trapped-$$"; while :; do sleep 1; done' & sleep 300"#
                .into(),
        ];
        let id = host.spawn(spec).unwrap();

        // Poll the scrollback snapshot for the descendant PID (see
        // kill_scope_leaves_no_process_group_orphan for why not a live tail).
        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());
        let descendant: u32 = loop {
            let out = host.scrollback(id).unwrap();
            let text = String::from_utf8_lossy(&out);
            if let Some(pid) = text
                .split_whitespace()
                .find_map(|t| t.strip_prefix("trapped-").and_then(|p| p.parse().ok()))
            {
                break pid;
            }
            if Instant::now() >= deadline {
                panic!("descendant pid never printed within deadline; scrollback: {text:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        assert!(pid_alive(descendant), "descendant alive before teardown");

        assert!(host.kill(id).await);
        assert_eq!(host.count(), 0);

        let mut dead = false;
        for _ in 0..100 {
            if !pid_alive(descendant) {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            dead,
            "SIGKILL escalation must reap the TERM-trapping descendant"
        );
    }

    /// Clean daemon shutdown (monorepo#1526): `kill_all` reaps every tracked
    /// session across all scopes — including a TERM+HUP-trapping descendant —
    /// leaving an empty host and no process-group orphans.
    #[tokio::test]
    async fn kill_all_reaps_every_scope_including_trapped_descendants() {
        let host = PtyHost::new();
        let plain = host.spawn(cat_spec("scope-a")).unwrap();
        let mut spec = SpawnSpec::new("scope-b", "sh");
        // Same trap shape as teardown_kills_term_trapping_descendant_...: the
        // descendant prints its PID only after TERM/HUP are ignored, and
        // sleeps in a loop so only SIGKILL removes it.
        spec.args = vec![
            "-c".into(),
            r#"sh -c 'trap "" TERM HUP; echo "trapped-$$"; while :; do sleep 1; done' & sleep 300"#
                .into(),
        ];
        let trapped = host.spawn(spec).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());
        let descendant: u32 = loop {
            let out = host.scrollback(trapped).unwrap();
            let text = String::from_utf8_lossy(&out);
            if let Some(pid) = text
                .split_whitespace()
                .find_map(|t| t.strip_prefix("trapped-").and_then(|p| p.parse().ok()))
            {
                break pid;
            }
            if Instant::now() >= deadline {
                panic!("descendant pid never printed within deadline; scrollback: {text:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(pid_alive(descendant), "descendant alive before kill_all");

        let reaped = host.kill_all().await;
        assert_eq!(reaped, 2);
        assert_eq!(host.count(), 0);
        assert!(!host.is_alive(plain));
        assert!(!host.is_alive(trapped));

        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());
        while pid_alive(descendant) {
            assert!(
                Instant::now() < deadline,
                "TERM-trapping descendant {descendant} survived kill_all"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A `spawn` racing `kill_all` (monorepo#1526): a request already in
    /// flight when the shutdown sweep drains the map must not register a PTY
    /// afterwards — the latched host refuses the spawn and the just-forked
    /// child is reaped in place, so no process group outlives the sweep.
    #[tokio::test]
    async fn spawn_after_kill_all_is_refused_and_reaped() {
        let host = PtyHost::new();
        assert_eq!(host.kill_all().await, 0);

        // The shell records its pid before exec'ing the long sleep; the
        // refused-spawn reap SIGKILLs the group and waits on the direct
        // child, so by the time `spawn` errors, any recorded pid is dead.
        let pidfile = std::env::temp_dir().join(format!(
            "intent-pty-late-pid-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut spec = SpawnSpec::new("scope-late", "sh");
        spec.args = vec![
            "-c".into(),
            format!("echo $$ > \"{}\"; exec sleep 300", pidfile.display()),
        ];
        let err = host.spawn(spec).expect_err("spawn refused after kill_all");
        assert!(
            err.to_string().contains("shut down"),
            "refusal names the shutdown: {err}"
        );
        assert_eq!(host.count(), 0, "nothing registered by the refused spawn");
        if let Some(pid) = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
        {
            assert!(!pid_alive(pid), "refused spawn's child {pid} was reaped");
        }
        let _ = std::fs::remove_file(&pidfile);
    }

    /// After the direct child exits on its own, `reap_group_stragglers`
    /// SIGKILLs group members that trapped both SIGTERM and SIGHUP, while the
    /// session (scrollback, latched exit) stays registered and readable
    /// (monorepo#1300).
    #[tokio::test]
    async fn reap_group_stragglers_kills_survivors_and_keeps_session() {
        let host = PtyHost::new();
        // The straggler touches `flag` only after installing its traps; the
        // leader echoes the straggler's pid while the tty is still attached,
        // waits for the flag, then exits — so by the time `wait()` returns
        // the pid is in scrollback and the traps are active.
        let flag = std::env::temp_dir().join(format!(
            "intent-pty-trap-flag-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut spec = SpawnSpec::new("scope-straggler", "sh");
        spec.args = vec![
            "-c".into(),
            format!(
                r#"sh -c 'trap "" TERM HUP; : > "{f}"; sleep 300' & echo "straggler-$!"; while [ ! -e "{f}" ]; do sleep 0.05; done; exit 0"#,
                f = flag.display()
            ),
        ];
        let id = host.spawn(spec).unwrap();
        let exit = host.wait(id).await.unwrap();
        let _ = std::fs::remove_file(&flag);
        assert!(exit.success, "shell exited cleanly");

        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());
        let straggler: u32 = loop {
            let out = host.scrollback(id).unwrap();
            let text = String::from_utf8_lossy(&out);
            if let Some(pid) = text
                .split_whitespace()
                .find_map(|t| t.strip_prefix("straggler-").and_then(|p| p.parse().ok()))
            {
                break pid;
            }
            if Instant::now() >= deadline {
                panic!("straggler pid never printed within deadline; scrollback: {text:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(pid_alive(straggler), "straggler outlives the shell");

        host.reap_group_stragglers(id).await;
        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(timeout_multiplier());
        while pid_alive(straggler) {
            assert!(
                Instant::now() < deadline,
                "straggler {straggler} still alive after reap"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The session survives the reap: exit status and scrollback readable.
        assert_eq!(host.try_exit(id).unwrap(), Some(exit));
        assert!(host.scrollback(id).is_ok());
        assert!(host.kill(id).await);
    }
}
