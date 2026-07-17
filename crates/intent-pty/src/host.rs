//! The unified PTY host (§12.1): spawn, attach (back-fill + live tail), write
//! (serialized stdin), resize, signal, and scope-scoped kill with no orphans.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use portable_pty::{
    native_pty_system, Child, ChildKiller, CommandBuilder, MasterPty, PtySize as PortablePtySize,
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
/// `alive` reflects whether the child has not yet exited.
#[derive(Clone, Debug)]
pub struct PtyInfo {
    /// Lifetime scope the PTY was spawned under (workspace or session id).
    pub scope: String,
    /// Working directory resolved at spawn, if one could be determined.
    pub cwd: Option<String>,
    /// Whether the child process has not yet exited.
    pub alive: bool,
}

struct PtySession {
    scope: String,
    /// Working directory resolved at spawn (`spec.cwd`, else the daemon's cwd).
    cwd: Option<String>,
    pid: Option<u32>,
    master: Mutex<Option<Box<dyn MasterPty + Send>>>,
    writer: Mutex<Box<dyn Write + Send>>,
    child: Mutex<Box<dyn Child + Send + Sync>>,
    killer: Mutex<Box<dyn ChildKiller + Send + Sync>>,
    fanout: Arc<Mutex<Fanout>>,
    reader: Mutex<Option<JoinHandle<()>>>,
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

/// The unified host owning every spawned PTY (terminals and scripts).
#[derive(Default)]
pub struct PtyHost {
    sessions: Mutex<HashMap<PtyId, Arc<PtySession>>>,
    next_id: AtomicU64,
}

impl PtyHost {
    /// Create an empty host.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn a process attached to a fresh PTY and start fanning out its output.
    pub fn spawn(&self, spec: SpawnSpec) -> Result<PtyId> {
        let pair = native_pty_system()
            .openpty(spec.size.to_portable())
            .map_err(internal)?;

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
        // Drop the slave so the master reader observes EOF once the child exits;
        // otherwise our retained slave fd keeps the stream open forever.
        drop(pair.slave);

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
            cwd,
            pid,
            master: Mutex::new(Some(pair.master)),
            writer: Mutex::new(writer),
            child: Mutex::new(child),
            killer: Mutex::new(killer),
            fanout,
            reader: Mutex::new(Some(handle)),
            exit: Mutex::new(None),
        });

        let id = PtyId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.sessions.lock().unwrap().insert(id, session);
        Ok(id)
    }

    /// Attach a new subscriber: capture recent scrollback to back-fill, then a
    /// live receiver for subsequent output. The snapshot and subscription are
    /// taken under one lock so no chunk is lost or duplicated across the seam.
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
    pub fn scrollback(&self, id: PtyId) -> Result<Vec<u8>> {
        let session = self.get(id)?;
        let guard = session.fanout.lock().unwrap();
        Ok(guard.scrollback.snapshot())
    }

    /// The PTY child's process id, if the platform reported one at spawn.
    pub fn pid(&self, id: PtyId) -> Option<u32> {
        self.sessions.lock().unwrap().get(&id).and_then(|s| s.pid)
    }

    /// The child's exit status if it has already exited, else `None`. Latches
    /// the status so it stays observable after the stream closes.
    pub fn try_exit(&self, id: PtyId) -> Result<Option<PtyExit>> {
        let session = self.get(id)?;
        Ok(observe_exit(&session))
    }

    /// Wait until the PTY's child exits and return its status (ACP
    /// `terminal/wait_for_exit`). Polls the child rather than blocking a thread.
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
    pub fn write(&self, id: PtyId, data: &[u8]) -> Result<()> {
        let session = self.get(id)?;
        let mut writer = session.writer.lock().unwrap();
        writer.write_all(data).map_err(internal)?;
        writer.flush().map_err(internal)?;
        Ok(())
    }

    /// Resize the PTY's visible area.
    pub fn resize(&self, id: PtyId, size: PtySize) -> Result<()> {
        let session = self.get(id)?;
        let guard = session.master.lock().unwrap();
        let master = guard
            .as_ref()
            .ok_or_else(|| internal("pty master already torn down"))?;
        master.resize(size.to_portable()).map_err(internal)
    }

    /// Deliver a signal to the PTY's process group (SIGINT/Ctrl-C, etc.).
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
    pub fn is_alive(&self, id: PtyId) -> bool {
        match self.sessions.lock().unwrap().get(&id).cloned() {
            Some(session) => matches!(session.child.lock().unwrap().try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Metadata for one tracked PTY (`terminal.list` / `terminal.readOutput`):
    /// its `scope`, working directory, and whether its child is still running.
    /// `None` when the id is unknown.
    pub fn info(&self, id: PtyId) -> Option<PtyInfo> {
        let session = self.sessions.lock().unwrap().get(&id).cloned()?;
        let alive = matches!(session.child.lock().unwrap().try_wait(), Ok(None));
        Some(PtyInfo {
            scope: session.scope.clone(),
            cwd: session.cwd.clone(),
            alive,
        })
    }

    /// Number of PTYs currently tracked.
    pub fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    /// The PTYs currently tracked under `scope`.
    pub fn list_scope(&self, scope: &str) -> Vec<PtyId> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.scope == scope)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Kill one PTY and reap its whole process group. Returns whether it existed.
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

    /// Kill every PTY under `scope` (session/workspace teardown). Returns the
    /// number reaped. No process-group orphans are left behind.
    pub async fn kill_scope(&self, scope: &str) -> usize {
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

    fn get(&self, id: PtyId) -> Result<Arc<PtySession>> {
        self.sessions
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("{id}")))
    }
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
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
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
            let _ = kill_group(pid, PtySignal::Terminate);
            let mut exited = false;
            let iters = (TERM_GRACE.as_millis() / REAP_POLL.as_millis()).max(1);
            for _ in 0..iters {
                if matches!(session.child.lock().unwrap().try_wait(), Ok(Some(_))) {
                    exited = true;
                    break;
                }
                tokio::time::sleep(REAP_POLL).await;
            }
            if !exited {
                let _ = kill_group(pid, PtySignal::Kill);
            }
        } else {
            let _ = session.killer.lock().unwrap().kill();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = session.killer.lock().unwrap().kill();
    }
    // Drop the master fd so the reader observes EOF, then join its thread.
    session.master.lock().unwrap().take();
    if let Some(handle) = session.reader.lock().unwrap().take() {
        let _ = handle.join();
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Instant;

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        needle.is_empty() || haystack.windows(needle.len()).any(|w| w == needle)
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
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
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
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
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

    /// Killing a scope reaps the whole process group: a backgrounded grandchild
    /// is terminated too, leaving no orphan (mirrors the M5 reaping test).
    #[tokio::test]
    async fn kill_scope_leaves_no_process_group_orphan() {
        let host = PtyHost::new();
        let mut spec = SpawnSpec::new("scope-x", "sh");
        spec.args = vec!["-c".into(), "sleep 300 & echo $!; sleep 300".into()];
        let id = host.spawn(spec).unwrap();
        let mut rx = host.attach(id).unwrap().live;

        // Poll for the grandchild PID with retry to handle scheduling/output delays
        // on loaded runners. Honor INTENTD_TEST_TIMEOUT_MULTIPLIER for coverage runs.
        let multiplier = std::env::var("INTENTD_TEST_TIMEOUT_MULTIPLIER")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0)
            .max(1.0);
        let deadline = Instant::now() + Duration::from_secs(10).mul_f64(multiplier);

        let grandchild: u32 = loop {
            if Instant::now() >= deadline {
                panic!("grandchild pid never printed within deadline");
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("deadline not yet reached");
            let out = collect_until(&mut rx, b"\n", remaining).await;
            let line = String::from_utf8_lossy(&out);
            if let Some(pid) = line.split_whitespace().find_map(|t| t.parse().ok()) {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
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
}
