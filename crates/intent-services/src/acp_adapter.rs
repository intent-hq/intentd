//! Shared plumbing for ephemeral ACP adapter runs: how to launch the adapter,
//! the npx-aware per-stage timeout budgets, the `initialize` params, and the
//! teardown/exit-attribution helpers.
//!
//! Two callers share this module: the model probe
//! ([`crate::provider_models`], "spawn → initialize → session/new → collect
//! models → kill") and the one-shot completion runner
//! ([`crate::one_shot_acp`], which adds a `session/prompt` phase). Everything
//! here is stage-agnostic — the stage sequencing itself lives with each
//! caller.
//!
//! It also owns the daemon-wide **adapter concurrency bound**
//! ([`AdapterSlots`], monorepo#2062). An ephemeral adapter is not a cheap
//! child: a measured one-shot chain (npx → adapter → provider CLI) costs
//! ~610 MB and lives up to the caller's timeout, and one-shots never enter
//! `ProcessRegistry`, so they consume no `agents.maxConcurrent` slot. Before
//! this bound the only ceiling was `server.maxOutstandingRpcs` (256), i.e.
//! ~156 GB of adapters. The bound lives here, at [`spawn_adapter`], rather
//! than at each call site because this is the single place an ephemeral
//! adapter is born: every present and future caller is covered, and the
//! permit's lifetime binds to the returned [`SpawnedAdapter`], so it is
//! released exactly when the child is reaped or dropped — including on the
//! panic and early-return paths a call-site guard would have to re-derive.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use intent_acp::spawn::NPX_NO_WORKSPACES_ARG;
#[cfg(unix)]
use intent_acp::{descendant_pids, sweep_escaped_descendants};
use intent_acp::{
    Connection, ConnectionHooks, IncomingNotification, IncomingRequest, NpxLaunchDir,
};
use intent_core::config::DEFAULT_MAX_CONCURRENT_ADAPTERS;
use intent_providers::enhanced_path;
use serde_json::{json, Value};
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

/// Hard cap on the setup phase (`initialize` + `session/new`) for resolved
/// binaries (mirrors the FE's 15s outer timeout). Deliberately smaller than
/// the sum of the per-stage budgets (4s + 10s + 2s grace), matching the FE:
/// the outer cap is the real bound and preempts slow-but-not-stuck stages.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Per-request timeout for `initialize` for resolved binaries (FE: 4s).
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(4);
/// `initialize` budget for npx-run adapters: a cold `npx -y <pkg>@<version>`
/// downloads and installs the package before the adapter can answer, which
/// routinely takes tens of seconds. A pinned-version bump must not guarantee
/// a static-fallback cycle just because the cache is cold.
const NPX_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(45);
/// Overall setup cap for npx-run adapters, kept bounded but sized to cover
/// the full per-stage sum (45s initialize + 20s session/new + 2s grace) so a
/// cold install that eats the initialize budget cannot starve `session/new`
/// of its own window. This is also the worst-case latency of a `forceRefresh`
/// `models.list` against a hung npx adapter — an accepted trade-off for
/// surviving cold installs.
const NPX_OVERALL_TIMEOUT: Duration = Duration::from_secs(70);
/// Per-request timeout for `session/new` for resolved binaries (FE: 8–10s).
const SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(10);
/// `session/new` budget for npx-run adapters: claude-agent-acp boots the
/// underlying CLI while creating the session, which alone takes ~10s even
/// with a warm npx cache — a flat 10s budget times out right at the wire.
const NPX_SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(20);

/// The daemon-wide ephemeral-adapter concurrency bound: a counting semaphore
/// plus the limit it was built with (kept for the queue-timeout diagnostic,
/// since a semaphore cannot report its own capacity).
///
/// Fair by construction — `tokio::sync::Semaphore` hands permits out in FIFO
/// order — so a queued caller cannot be starved by later arrivals and the
/// wait a caller observes is bounded by the runs ahead of it.
pub(crate) struct AdapterSlots {
    permits: Arc<Semaphore>,
    limit: u32,
}

impl AdapterSlots {
    /// A bound admitting `limit` concurrent adapters. `limit` is clamped to at
    /// least 1: a zero here would wedge every adapter run forever, and the
    /// settings schema already rejects it.
    pub(crate) fn new(limit: u32) -> Self {
        let limit = limit.max(1);
        Self {
            permits: Arc::new(Semaphore::new(limit as usize)),
            limit,
        }
    }

    /// The configured cap.
    pub(crate) fn limit(&self) -> u32 {
        self.limit
    }

    /// Slots currently free (test/diagnostic view).
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// Chains currently live: permits handed out and not yet returned. A permit
    /// is taken before the child is spawned and returned when
    /// [`SpawnedAdapter`] drops — after the reap — so this spans the whole
    /// lifetime of every ephemeral chain, which is exactly the window the
    /// descendant-tree sampler needs to be watching (monorepo#2107).
    pub(crate) fn live(&self) -> usize {
        (self.limit as usize).saturating_sub(self.permits.available_permits())
    }

    /// Claim a slot, waiting at most `wait` for one to free up. `None` means
    /// the caller's budget expired while queued — the caller turns that into
    /// its own distinguishable queue-timeout error rather than spawning.
    async fn acquire(&self, wait: Duration) -> Option<OwnedSemaphorePermit> {
        // Fast path: a free slot costs no timer and no log line.
        if let Ok(permit) = self.permits.clone().try_acquire_owned() {
            return Some(permit);
        }
        tracing::debug!(
            limit = self.limit,
            wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
            "ephemeral adapter bound reached; queueing for a slot"
        );
        // `acquire_owned` only errors on a closed semaphore, which never
        // happens here (the bound outlives every caller) — treat it like a
        // queue timeout rather than panicking on an unreachable branch.
        tokio::time::timeout(wait, self.permits.clone().acquire_owned())
            .await
            .ok()?
            .ok()
    }
}

/// The process-wide bound, installed once at daemon startup from
/// `agents.maxConcurrentAdapters` ([`init_adapter_slots`]). Uninitialized —
/// in tests and in embedders that never call the installer — it falls back to
/// [`DEFAULT_MAX_CONCURRENT_ADAPTERS`], so the bound is never simply absent.
static ADAPTER_SLOTS: OnceLock<AdapterSlots> = OnceLock::new();

/// Install the daemon-wide adapter bound from settings. Returns `false` when a
/// bound was already installed (or already lazily defaulted by an earlier
/// spawn), leaving the existing one untouched — the setting applies on daemon
/// restart, like `agents.maxConcurrent`.
pub fn init_adapter_slots(limit: u32) -> bool {
    ADAPTER_SLOTS.set(AdapterSlots::new(limit)).is_ok()
}

/// The daemon-wide bound, defaulting on first use if startup never installed
/// one.
pub(crate) fn adapter_slots() -> &'static AdapterSlots {
    ADAPTER_SLOTS.get_or_init(|| AdapterSlots::new(DEFAULT_MAX_CONCURRENT_ADAPTERS))
}

/// The effective daemon-wide adapter cap. Reading it back is how a caller
/// (notably an e2e test) sizes work to the bound actually in force, rather
/// than to the value it asked [`init_adapter_slots`] for — which is ignored
/// when a bound was already installed.
#[must_use]
pub fn adapter_slot_limit() -> u32 {
    adapter_slots().limit()
}

/// Ephemeral adapter chains alive daemon-wide right now (monorepo#2107).
///
/// The `system.status` descendant-tree sampler polls this to decide whether a
/// process-table sweep is worth its cost: a non-zero answer means a burst is in
/// flight *now*, which is the only window in which a chain's memory can be
/// observed at all — measured, 16 concurrent one-shots take 6.97 GB and are
/// spawned and fully reaped inside 3.3 s.
///
/// Deliberately reads the bound without installing it, unlike
/// [`adapter_slot_limit`]: a lazy default here would let a caller that runs
/// before [`init_adapter_slots`] silently pin the shipped cap in place of the
/// configured one. No bound installed means no adapter has ever spawned, so
/// nothing is live.
pub fn live_adapters() -> usize {
    ADAPTER_SLOTS.get().map_or(0, AdapterSlots::live)
}

/// How to launch an ephemeral ACP adapter.
pub(crate) struct AcpAdapterCommand {
    program: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, OsString)>,
    envs_removed: Vec<String>,
    auth_required_stdout_marker: Option<&'static str>,
    /// The `session/new` `cwd`, and the process cwd of a resolved binary.
    /// `None` means the system temp dir (the ephemeral default).
    cwd: Option<PathBuf>,
    /// npx-run adapters get the longer cold-install timeout budget and start
    /// in a neutral [`NpxLaunchDir`] rather than `cwd`.
    via_npx: bool,
    /// Parent of the per-launch [`NpxLaunchDir`]; `None` is the OS temp dir.
    npx_launch_root: Option<PathBuf>,
}

impl AcpAdapterCommand {
    /// Run a pinned npm package via `npx --workspaces=false -y <package>`
    /// (the same npm-isolation argv as `intent_acp::spawn::build_args`;
    /// the switch precedes the package because npx forwards everything after
    /// it to the adapter).
    pub(crate) fn npx(npx: PathBuf, package: &str) -> Self {
        Self {
            program: npx,
            args: vec![
                NPX_NO_WORKSPACES_ARG.to_string(),
                "-y".to_string(),
                package.to_string(),
            ],
            envs: Vec::new(),
            envs_removed: Vec::new(),
            auth_required_stdout_marker: None,
            cwd: None,
            via_npx: true,
            npx_launch_root: None,
        }
    }

    /// Run a resolved adapter binary with the given args.
    pub(crate) fn binary(bin: PathBuf, args: Vec<String>) -> Self {
        Self {
            program: bin,
            args,
            envs: Vec::new(),
            envs_removed: Vec::new(),
            auth_required_stdout_marker: None,
            cwd: None,
            via_npx: false,
            npx_launch_root: None,
        }
    }

    /// Root the npx launch dir under `root` instead of the OS temp dir.
    #[cfg(test)]
    pub(crate) fn npx_launch_root(mut self, root: PathBuf) -> Self {
        self.npx_launch_root = Some(root);
        self
    }

    /// Append extra launch arguments after the ones already assembled.
    pub(crate) fn args(mut self, extra: impl IntoIterator<Item = String>) -> Self {
        self.args.extend(extra);
        self
    }

    /// Pin the `session/new` `cwd` (and a resolved binary's working
    /// directory; see [`Self::working_dir`]). Callers that leave this unset
    /// get the system temp dir.
    pub(crate) fn cwd(mut self, dir: PathBuf) -> Self {
        self.cwd = Some(dir);
        self
    }

    /// The launch's `session/new` `cwd` — and, for a resolved binary, its
    /// process cwd. An npx launch's process cwd is its [`NpxLaunchDir`]
    /// instead (intent-hq/intent#5738): npm resolves its project root by
    /// walking up from the process cwd, and a workspace manifest there
    /// (`catalog:` specifiers, a matching `workspaces` glob, a project
    /// `.npmrc`) breaks `npx -y <adapter>` before the adapter can start.
    pub(crate) fn working_dir(&self) -> PathBuf {
        self.cwd.clone().unwrap_or_else(std::env::temp_dir)
    }

    /// Add an environment-variable override for the adapter child.
    pub(crate) fn env(mut self, key: impl Into<String>, value: impl Into<OsString>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// Remove an environment variable from the adapter child's inherited env.
    pub(crate) fn env_remove(mut self, key: impl Into<String>) -> Self {
        self.envs_removed.push(key.into());
        self
    }

    /// Recognize the controlled browser helper's immediate auth signal.
    pub(crate) fn auth_required_marker(mut self, marker: &'static str) -> Self {
        self.auth_required_stdout_marker = Some(marker);
        self
    }

    #[cfg(test)]
    pub(crate) fn program(&self) -> &std::path::Path {
        &self.program
    }

    #[cfg(test)]
    pub(crate) fn env_vars(&self) -> &[(String, OsString)] {
        &self.envs
    }

    #[cfg(test)]
    pub(crate) fn removed_env_vars(&self) -> &[String] {
        &self.envs_removed
    }

    /// Per-request `initialize` budget for this launch.
    pub(crate) fn initialize_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_INITIALIZE_TIMEOUT
        } else {
            INITIALIZE_TIMEOUT
        }
    }

    /// Per-request `session/new` budget for this launch.
    pub(crate) fn session_new_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_SESSION_NEW_TIMEOUT
        } else {
            SESSION_NEW_TIMEOUT
        }
    }

    /// Cap on the whole setup phase (`initialize` + `session/new`) for this
    /// launch. The one-shot runner bounds its `session/prompt` phase
    /// separately with the caller's timeout.
    pub(crate) fn setup_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_OVERALL_TIMEOUT
        } else {
            OVERALL_TIMEOUT
        }
    }
}

/// A spawned adapter: the child, its ACP connection, and the inbound
/// notification/request streams the caller drives.
pub(crate) struct SpawnedAdapter {
    /// The adapter process (reap with [`reap_child`]).
    pub(crate) child: tokio::process::Child,
    /// The JSON-RPC connection over the child's piped stdio.
    pub(crate) conn: Connection,
    /// Agent → client notifications (`session/update`, …).
    pub(crate) notifications: mpsc::UnboundedReceiver<IncomingNotification>,
    /// Agent → client requests (`session/request_permission`, `fs/*`, …).
    pub(crate) requests: mpsc::UnboundedReceiver<IncomingRequest>,
    /// The neutral directory an npx launch runs in (intent-hq/intent#5738);
    /// `None` for resolved binaries. Held, like the slot, until the adapter
    /// value is dropped after the child has been reaped, so the directory
    /// outlives the whole process tree.
    _npx_launch_dir: Option<NpxLaunchDir>,
    /// This run's slot in the daemon-wide bound. Never read — held so the
    /// slot is returned when the adapter value is dropped, which is after the
    /// child has been reaped on every exit path.
    _slot: OwnedSemaphorePermit,
}

/// Why an adapter could not be started.
#[derive(Debug)]
pub(crate) enum SpawnError {
    /// No slot in the daemon-wide bound came free within the caller's budget:
    /// the run never spawned anything. Distinct from every in-run timeout so
    /// callers can report queueing pressure as itself (monorepo#2062).
    QueueTimeout { waited: Duration, limit: u32 },
    /// The adapter process could not be spawned.
    Spawn(String),
}

/// Claim a slot in the daemon-wide bound (waiting at most `queue_wait`), then
/// spawn the adapter with piped stdio, its own process group, and the
/// enhanced PATH, and wire an ACP [`Connection`] around it. Failures come back
/// as [`SpawnError`] so callers can map them onto their own error types. The
/// child is `kill_on_drop`, so an early return still reaps it, and the slot
/// rides on the returned [`SpawnedAdapter`] — released when the caller drops
/// it after reaping, never before.
pub(crate) async fn spawn_adapter(
    cmd: &AcpAdapterCommand,
    queue_wait: Duration,
) -> Result<SpawnedAdapter, SpawnError> {
    spawn_adapter_in(adapter_slots(), cmd, queue_wait).await
}

/// [`spawn_adapter`] against a caller-supplied bound instead of the
/// process-global one. Production always goes through [`spawn_adapter`]; this
/// seam exists so a test can run against a private [`AdapterSlots`] and stay
/// insulated from slot pressure created by sibling tests sharing the global
/// bound (monorepo#2379).
pub(crate) async fn spawn_adapter_in(
    slots: &AdapterSlots,
    cmd: &AcpAdapterCommand,
    queue_wait: Duration,
) -> Result<SpawnedAdapter, SpawnError> {
    let started = std::time::Instant::now();
    let Some(slot) = slots.acquire(queue_wait).await else {
        let waited = started.elapsed();
        tracing::warn!(
            limit = slots.limit(),
            waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            program = %cmd.program.display(),
            "gave up waiting for an ephemeral adapter slot"
        );
        return Err(SpawnError::QueueTimeout {
            waited,
            limit: slots.limit(),
        });
    };
    spawn_admitted_adapter(cmd, slot).map_err(SpawnError::Spawn)
}

/// The spawn itself, once a slot is held. Split out so the bound and the
/// process plumbing stay separately readable; `slot` is moved into the
/// returned adapter and released with it.
fn spawn_admitted_adapter(
    cmd: &AcpAdapterCommand,
    slot: OwnedSemaphorePermit,
) -> Result<SpawnedAdapter, String> {
    let npx_launch_dir = if cmd.via_npx {
        Some(
            NpxLaunchDir::create(cmd.npx_launch_root.as_deref())
                .map_err(|e| format!("{}: npx launch dir: {e}", cmd.program.display()))?,
        )
    } else {
        None
    };
    let process_cwd = npx_launch_dir
        .as_ref()
        .map_or_else(|| cmd.working_dir(), |dir| dir.path().to_path_buf());
    let mut command = tokio::process::Command::new(&cmd.program);
    command
        .args(&cmd.args)
        .current_dir(process_cwd)
        .env("PATH", enhanced_path(Some(&cmd.program)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in &cmd.envs {
        command.env(key, value);
    }
    for key in &cmd.envs_removed {
        command.env_remove(key);
    }
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", cmd.program.display()))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "child stdin not piped".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "child stdout not piped".to_string())?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);

    let (note_tx, notifications) = mpsc::unbounded_channel();
    let (req_tx, requests) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        auth_required_stdout_marker: cmd.auth_required_stdout_marker,
        notifications: Some(note_tx),
        requests: Some(req_tx),
        ..Default::default()
    };
    let conn = Connection::new(stdin, stdout, stderr, hooks);
    Ok(SpawnedAdapter {
        child,
        conn,
        notifications,
        requests,
        _npx_launch_dir: npx_launch_dir,
        _slot: slot,
    })
}

/// The `initialize` params every ephemeral adapter run sends: no filesystem
/// capabilities, so the adapter never expects the client to serve `fs/*`.
pub(crate) fn initialize_params() -> Value {
    json!({
        "protocolVersion": 1,
        "clientInfo": { "name": "Intent", "version": env!("CARGO_PKG_VERSION") },
        "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false } },
    })
}

/// Bounded window to observe a crashed adapter's exit status and let the
/// stderr reader drain its final lines before attribution. A crashing child's
/// stdout close (the transport error that reports the crash) races both the
/// exit-status reap and the stderr drain, so a bare `try_wait` snapshot can
/// misattribute a genuine crash as a plain transport failure.
///
/// Latency cost: on a timeout with a hung (still-running) child,
/// `child.wait()` burns this full window before falling back to `try_wait`,
/// so a timed-out run takes ~500ms beyond its budget in production. Bounded
/// and error-path-only, so accepted.
const EXIT_OBSERVE_GRACE: Duration = Duration::from_millis(500);

/// Observe whether the adapter already exited, waiting briefly for both the
/// exit status and (on an unsuccessful exit) the child's final stderr lines
/// to land in the connection's ring buffer.
pub(crate) async fn observe_exit_status(
    child: &mut tokio::process::Child,
    conn: &Connection,
) -> Option<std::process::ExitStatus> {
    let status = match tokio::time::timeout(EXIT_OBSERVE_GRACE, child.wait()).await {
        Ok(Ok(status)) => Some(status),
        _ => child.try_wait().ok().flatten(),
    };
    if status.is_some_and(|s| !s.success()) {
        // The exited child's final stderr may still be in flight to the
        // reader task; wait briefly for the first line so the attribution
        // can carry it.
        let deadline = tokio::time::Instant::now() + EXIT_OBSERVE_GRACE;
        while conn.recent_stderr().is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    status
}

/// How many trailing stderr lines to include in an exit attribution. npm's
/// final line is typically just "A complete log of this run can be found
/// in: …" with the actual cause a few lines earlier, so a single line is
/// not enough.
const STDERR_TAIL_LINES: usize = 3;
/// Character bound on the joined stderr tail (kept from the end).
const STDERR_TAIL_MAX_CHARS: usize = 300;

/// The "adapter died" detail for an observed exit: `Some("<status>; stderr:
/// …")` when the child exited unsuccessfully, `None` when it is still running
/// or exited cleanly (a clean exit is a genuine empty/short result, not a
/// crash).
pub(crate) fn exited_detail(
    status: Option<std::process::ExitStatus>,
    stderr: &[String],
) -> Option<String> {
    let status = status?;
    if status.success() {
        return None;
    }
    let tail = match stderr_tail(stderr) {
        Some(t) => format!("; stderr: {t}"),
        None => String::new(),
    };
    Some(format!("{status}{tail}"))
}

/// Join the last [`STDERR_TAIL_LINES`] non-empty stderr lines, bounded to
/// [`STDERR_TAIL_MAX_CHARS`] characters kept from the end.
fn stderr_tail(stderr: &[String]) -> Option<String> {
    let non_empty: Vec<&str> = stderr
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    let start = non_empty.len().saturating_sub(STDERR_TAIL_LINES);
    let joined = non_empty[start..].join(" | ");
    if joined.is_empty() {
        return None;
    }
    let count = joined.chars().count();
    Some(
        joined
            .chars()
            .skip(count.saturating_sub(STDERR_TAIL_MAX_CHARS))
            .collect(),
    )
}

/// Grace window between SIGTERM and SIGKILL when reaping an adapter child
/// (mirrors `host_exec::TERM_GRACE` / `mcp_servers::reap`).
#[cfg(unix)]
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Kill the adapter child and reap it. Signals the whole process group (the
/// child is its own group leader via `process_group(0)`) so grandchildren
/// (e.g. `npx` → `node`) die too, following the crate's SIGTERM → grace →
/// SIGKILL pattern, then waits briefly so the child does not linger as a
/// zombie. `kill_on_drop(true)` back-stops any wait timeout.
///
/// Group signalling alone is not enough: adapters can start MCP servers that
/// move into their OWN process groups, so descendants are snapshotted before
/// the kill and any survivors swept afterwards regardless of process group —
/// see `intent_acp::descendant_sweep` for the shared backstop and its
/// snapshot-before-kill rationale.
pub(crate) async fn reap_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    let descendants = match child.id() {
        Some(pid) => descendant_pids(pid).await,
        None => Vec::new(),
    };
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;
        let pgid = Pid::from_raw(pid.cast_signed());
        let _ = killpg(pgid, Signal::SIGTERM);
        tokio::time::sleep(TERM_GRACE).await;
        if !matches!(child.try_wait(), Ok(Some(_))) {
            let _ = killpg(pgid, Signal::SIGKILL);
        }
    }
    let _ = child.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    #[cfg(unix)]
    sweep_escaped_descendants(&descendants).await;
}

/// Unit tests for the daemon-wide adapter bound itself (monorepo#2062).
/// These build their own [`AdapterSlots`] rather than touching the process
/// global, so they say nothing about — and are unaffected by — whatever the
/// rest of the binary installed.
#[cfg(test)]
mod slot_tests {
    use super::*;

    /// The bound admits exactly `limit` holders at once; the next caller waits
    /// and is admitted the moment a permit drops (which, in the real runner,
    /// is after the previous child has been reaped).
    #[tokio::test]
    async fn slots_admit_the_limit_then_queue_until_one_is_released() {
        let slots = AdapterSlots::new(2);
        let first = slots.acquire(Duration::from_secs(5)).await.expect("1st");
        let second = slots.acquire(Duration::from_secs(5)).await.expect("2nd");
        assert_eq!(slots.available(), 0, "both slots are held");

        // A third caller cannot get in while both are held...
        assert!(
            slots.acquire(Duration::from_millis(50)).await.is_none(),
            "third caller must not be admitted over the limit"
        );
        // ...but does as soon as one is returned.
        drop(first);
        assert!(
            slots.acquire(Duration::from_secs(5)).await.is_some(),
            "a released slot must admit the queued caller"
        );
        drop(second);
    }

    /// A queued caller waits out its whole budget before giving up — it does
    /// not fail fast — so a burst that drains in time still completes.
    #[tokio::test]
    async fn queued_caller_waits_its_budget_before_giving_up() {
        let slots = AdapterSlots::new(1);
        let held = slots.acquire(Duration::from_secs(5)).await.expect("held");
        let started = std::time::Instant::now();
        assert!(slots.acquire(Duration::from_millis(300)).await.is_none());
        assert!(
            started.elapsed() >= Duration::from_millis(250),
            "gave up after {:?}, before the budget elapsed",
            started.elapsed()
        );
        drop(held);
    }

    /// `live()` is what tells the `system.status` sampler a burst is in flight
    /// (monorepo#2107), so it has to track held permits exactly: zero when the
    /// bound is untouched, one per chain that has spawned and not been reaped,
    /// and back to zero once they are.
    #[tokio::test]
    async fn live_counts_chains_that_hold_a_slot() {
        let slots = AdapterSlots::new(4);
        assert_eq!(slots.live(), 0, "no chain has spawned yet");
        let first = slots.acquire(Duration::from_secs(5)).await.expect("1st");
        let second = slots.acquire(Duration::from_secs(5)).await.expect("2nd");
        assert_eq!(slots.live(), 2);
        drop(first);
        assert_eq!(slots.live(), 1, "a reaped chain stops counting");
        drop(second);
        assert_eq!(slots.live(), 0);
    }

    /// A zero limit would wedge every adapter run forever; the schema rejects
    /// it, and the type refuses it as a second line of defence.
    #[tokio::test]
    async fn zero_limit_is_clamped_to_one_rather_than_deadlocking() {
        let slots = AdapterSlots::new(0);
        assert_eq!(slots.limit(), 1);
        assert!(slots.acquire(Duration::from_millis(50)).await.is_some());
    }

    /// The global is never *absent*: an embedder that skips
    /// [`init_adapter_slots`] gets the shipped default rather than an
    /// unbounded spawn, and no path can leave it at zero or above the schema
    /// ceiling.
    ///
    /// Deliberately asserts only the invariant, not a specific number: the
    /// global is a `OnceLock` shared by every test in this binary, so under a
    /// single-process runner whichever test touches it first decides its
    /// value, and pinning `== DEFAULT` here would make the suite depend on
    /// unspecified test ordering. The exact fallback value is pinned
    /// deterministically instead by `settings::tests::
    /// max_concurrent_adapters_catalog_entry_and_resolver` (resolver) and
    /// `settings_file::tests::max_concurrent_adapters_defaults_and_template_round_trip`
    /// (schema), neither of which touches global state.
    #[tokio::test]
    async fn global_bound_is_always_installed_and_in_range() {
        let limit = adapter_slots().limit();
        assert!(
            limit > 0 && limit <= intent_core::config::MAX_CONCURRENT_ADAPTERS_LIMIT,
            "the daemon-wide bound must always be a usable cap, got {limit}"
        );
        assert_eq!(
            limit,
            adapter_slot_limit(),
            "the public accessor must report the same bound the spawner uses"
        );
    }
}

#[cfg(all(test, unix))]
mod reap_tests {
    use super::*;

    // Table-walk unit tests for the sweep live with the shared helper in
    // `intent_acp::descendant_sweep`; this module keeps the adapter-level
    // integration regression.

    #[expect(clippy::similar_names)] // pid/pgid are the POSIX terms
    /// Regression for the live escape: an MCP-server-style grandchild that
    /// moves into its OWN process group survives `killpg` on the adapter
    /// group (observed: codex-acp's auggie ran with pgid == its own pid); the
    /// descendant sweep must still reap it. Mirrors intent-acp's
    /// `kill_reaps_grandchildren_via_process_group`, except the grandchild
    /// escapes the group via `set -m` job control (background jobs become
    /// their own group leaders).
    #[tokio::test]
    async fn reap_child_sweeps_grandchild_in_foreign_process_group() {
        use nix::unistd::{getpgid, Pid};

        let pidfile =
            std::env::temp_dir().join(format!("intent-probe-sweep-{}.pid", uuid::Uuid::new_v4()));
        let mut command = tokio::process::Command::new("bash");
        command
            .arg("-c")
            .arg(r#"set -m; sleep 300 & echo $! > "$INTENT_TEST_PIDFILE"; wait"#)
            .env("INTENT_TEST_PIDFILE", &pidfile)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command.process_group(0);
        let mut child = command.spawn().expect("spawn bash child");

        let mut grandchild_pid = None;
        for _ in 0..250 {
            if let Ok(s) = tokio::fs::read_to_string(&pidfile).await {
                if let Ok(pid) = s.trim().parse::<i32>() {
                    grandchild_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild_pid = grandchild_pid.expect("grandchild pid written");

        // Prove the grandchild actually escaped the adapter's process group —
        // otherwise killpg would reach it and the test would be vacuous.
        let child_pgid = getpgid(Some(Pid::from_raw(
            child.id().expect("child pid").cast_signed(),
        )))
        .expect("child pgid");
        let grandchild_pgid =
            getpgid(Some(Pid::from_raw(grandchild_pid))).expect("grandchild pgid");
        assert_ne!(
            grandchild_pgid, child_pgid,
            "grandchild must be in a foreign process group for this regression test"
        );

        // Distinct failure signal for the snapshot path: if `ps` stalls past
        // its budget on a loaded runner the snapshot comes back empty and the
        // sweep silently no-ops — fail here, not at the terminal panic below.
        let snapshot = descendant_pids(child.id().expect("child pid")).await;
        assert!(
            snapshot.contains(&grandchild_pid),
            "descendant snapshot {snapshot:?} must include grandchild {grandchild_pid} \
             (empty/partial snapshot ⇒ `ps` walk failed, not the sweep)"
        );

        reap_child(&mut child).await;
        tokio::fs::remove_file(&pidfile).await.ok();

        // `kill(pid, 0)` returns ESRCH once the pid is gone (the grandchild
        // is not our direct child, so init reaps it after the sweep's kill).
        for _ in 0..100 {
            if nix::sys::signal::kill(Pid::from_raw(grandchild_pid), None).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("grandchild pid {grandchild_pid} still alive after reap_child sweep");
    }
}

/// intent-hq/intent#5738 for the ephemeral launcher: an npx-run adapter
/// (model probes, one-shot completions) starts npx in a neutral launch dir,
/// never in the caller's workspace, while `working_dir()` — the `session/new`
/// `cwd` — is untouched. Shared fake-npx helpers are `pub(crate)` for the
/// one-shot runner's end-to-end regression.
#[cfg(all(test, unix))]
pub(crate) mod npx_launch_tests {
    use super::*;
    use crate::test_support::test_tempdir;
    use std::path::Path;

    /// A workspace fixture reproducing the intent-hq/intent#5738 failure mode:
    /// a pnpm/Bun workspace whose `package.json` uses `catalog:` specifiers.
    /// The directory name carries a space so the ACP cwd path shape is
    /// exercised.
    pub(crate) fn catalog_workspace(tmp: &Path) -> PathBuf {
        let workspace = tmp.join("bun workspace");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(
            workspace.join("package.json"),
            r#"{"name":"catalog-workspace","private":true,"dependencies":{"zod":"catalog:"}}"#,
        )
        .unwrap();
        std::fs::write(
            workspace.join("pnpm-workspace.yaml"),
            "packages:\n  - packages/*\ncatalog:\n  zod: ^3.23.0\n",
        )
        .unwrap();
        workspace
    }

    /// The fake `npx`: records its cwd and that directory's entries to
    /// `$INTENTD_FAKE_NPX_REPORT` (atomically), then mirrors npm's project-root
    /// discovery (`@npmcli/config` `loadLocalPrefix`): the nearest
    /// `package.json` up from the cwd, and — unless `--workspaces=false` /
    /// `--no-workspaces` precedes the package positional — an ancestor manifest
    /// declaring `workspaces` instead. It fails like npm when that root's
    /// manifest uses `catalog:` (`EUNSUPPORTEDPROTOCOL`, exit 1) or its `.npmrc`
    /// names a `script-shell` that does not exist (`ENOENT`, exit 254).
    /// Otherwise it execs `node $INTENTD_FAKE_NPX_ADAPTER` when set (the
    /// "installed" adapter), else exits 0. Never downloads anything.
    const FAKE_NPX_SCRIPT: &str = r#"#!/bin/sh
{ printf '%s\n' "$PWD"; ls -A; } > "$INTENTD_FAKE_NPX_REPORT.tmp" && mv "$INTENTD_FAKE_NPX_REPORT.tmp" "$INTENTD_FAKE_NPX_REPORT"
no_ws=0
for a in "$@"; do
  case "$a" in
    --) break ;;
    --workspaces=false|--no-workspaces) no_ws=1 ;;
    -*) ;;
    *) break ;;
  esac
done
root=""
d="$PWD"
while :; do
  if [ -e "$d/package.json" ]; then
    if [ -z "$root" ]; then
      root="$d"
      [ "$no_ws" = 1 ] && break
    elif grep -q '"workspaces"' "$d/package.json"; then
      root="$d"
      break
    fi
  fi
  [ "$d" = / ] && break
  d=$(dirname "$d")
done
if [ -n "$root" ]; then
  if grep -q 'catalog:' "$root/package.json"; then
    echo 'npm error code EUNSUPPORTEDPROTOCOL' >&2
    echo 'npm error Unsupported URL Type "catalog:": catalog:' >&2
    exit 1
  fi
  shell=$(sed -n 's/^script-shell=//p' "$root/.npmrc" 2>/dev/null)
  if [ -n "$shell" ] && [ ! -x "$shell" ]; then
    echo 'npm error code ENOENT' >&2
    echo "npm error enoent spawn $shell ENOENT" >&2
    exit 254
  fi
fi
if [ -n "$INTENTD_FAKE_NPX_ADAPTER" ]; then
  exec node "$INTENTD_FAKE_NPX_ADAPTER"
fi
exit 0
"#;

    /// Write [`FAKE_NPX_SCRIPT`] as an executable `npx` into `dir`.
    pub(crate) fn write_fake_npx(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let npx = dir.join("npx");
        std::fs::write(&npx, FAKE_NPX_SCRIPT).unwrap();
        std::fs::set_permissions(&npx, std::fs::Permissions::from_mode(0o755)).unwrap();
        npx
    }

    /// Poll `report` until the fake npx has renamed it into place: the cwd it
    /// ran in and the names of that directory's entries.
    pub(crate) async fn read_npx_report(report: &Path) -> (PathBuf, Vec<String>) {
        for _ in 0..400 {
            if let Ok(s) = tokio::fs::read_to_string(report).await {
                let mut lines = s.lines();
                if let Some(cwd) = lines.next() {
                    return (PathBuf::from(cwd), lines.map(str::to_owned).collect());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fake npx never reported its cwd to {}", report.display());
    }

    /// The launch dir holds exactly the private sentinel `package.json` that
    /// makes it npm's nearest project root and nothing else.
    pub(crate) fn assert_neutral_launch_dir(npx_cwd: &Path, entries: &[String]) {
        assert_eq!(
            entries,
            ["package.json"],
            "npx launch dir {} must hold only the sentinel manifest",
            npx_cwd.display()
        );
        let sentinel: Value =
            serde_json::from_str(&std::fs::read_to_string(npx_cwd.join("package.json")).unwrap())
                .expect("sentinel package.json is JSON");
        assert_eq!(sentinel["private"], json!(true), "{sentinel}");
        assert!(
            sentinel.get("dependencies").is_none() && sentinel.get("workspaces").is_none(),
            "sentinel must not declare dependencies or workspaces: {sentinel}"
        );
    }

    /// Seed `ancestor` (an ancestor of the npx launch root at
    /// `ancestor/.intent/agent-configs`) with the intent-hq/intent#5738
    /// fixture: a `catalog:` manifest whose `workspaces` glob matches every
    /// launch dir, plus a `.npmrc` whose `script-shell` does not exist.
    fn seed_matching_workspace_ancestor(ancestor: &Path) -> PathBuf {
        let launch_root = ancestor.join(".intent").join("agent-configs");
        std::fs::create_dir_all(&launch_root).unwrap();
        std::fs::write(
            ancestor.join("package.json"),
            r#"{"name":"catalog-parent","private":true,"workspaces":[".intent/agent-configs/*"],"dependencies":{"zod":"catalog:"}}"#,
        )
        .unwrap();
        std::fs::write(
            ancestor.join(".npmrc"),
            "script-shell=/intentd-test-nonexistent-shell\nregistry=http://127.0.0.1:9/\n",
        )
        .unwrap();
        launch_root
    }

    /// The npx argv is pinned: the workspaces switch precedes `-y <package>`
    /// (npx forwards everything after the package to the adapter), and extra
    /// launch args land after the package.
    #[test]
    fn npx_command_disables_workspace_root_adoption_ahead_of_the_package() {
        let cmd = AcpAdapterCommand::npx(PathBuf::from("/usr/bin/npx"), "codex-acp@1.2.3")
            .args(["--model".to_string(), "gpt".to_string()]);
        assert_eq!(
            cmd.args,
            [
                NPX_NO_WORKSPACES_ARG,
                "-y",
                "codex-acp@1.2.3",
                "--model",
                "gpt"
            ]
        );
        assert!(cmd.via_npx);
        let bin = AcpAdapterCommand::binary(PathBuf::from("/opt/codex-acp"), vec!["a".into()]);
        assert_eq!(bin.args, ["a"], "direct binaries take no npx switches");
    }

    /// An npx launch from inside a `catalog:` workspace runs npx in a neutral
    /// launch dir (so npm never sees the workspace manifest) while
    /// `working_dir()` — what `session/new` receives — stays the workspace;
    /// the launch dir is removed once the spawned adapter is dropped.
    #[tokio::test]
    async fn npx_adapter_runs_outside_the_workspace_and_keeps_it_as_session_cwd() {
        let tmp = test_tempdir("intent-adapter-npx-");
        let workspace = catalog_workspace(tmp.path());
        let report = tmp.path().join("npx-report");
        let npx = write_fake_npx(tmp.path());
        let cmd = AcpAdapterCommand::npx(npx, "claude-agent-acp@0.0.0-test")
            .cwd(workspace.clone())
            .env("INTENTD_FAKE_NPX_REPORT", report.as_os_str());

        let slots = AdapterSlots::new(1);
        let mut adapter = spawn_adapter_in(&slots, &cmd, Duration::from_secs(5))
            .await
            .expect("spawn fake npx");
        let (npx_cwd, entries) = read_npx_report(&report).await;
        let status = adapter.child.wait().await.expect("wait fake npx");
        assert!(
            status.success(),
            "npx must not see the workspace package.json (exit {status:?})"
        );
        assert_ne!(npx_cwd, workspace, "npx ran inside the workspace");
        assert!(
            !npx_cwd.starts_with(&workspace),
            "npx cwd {} is under the workspace",
            npx_cwd.display()
        );
        assert_neutral_launch_dir(&npx_cwd, &entries);
        assert_eq!(cmd.working_dir(), workspace, "session cwd untouched");
        assert!(npx_cwd.is_dir(), "launch dir lives as long as the adapter");
        drop(adapter);
        assert!(
            !npx_cwd.exists(),
            "launch dir {} must be removed with the adapter",
            npx_cwd.display()
        );
    }

    /// The launch root's own ancestors cannot reach the launch either: an
    /// ancestor `package.json` whose `workspaces` glob matches the launch dir
    /// (with a broken `.npmrc`) must not be adopted as npm's project root.
    #[tokio::test]
    async fn npx_adapter_is_isolated_from_the_launch_roots_ancestors() {
        let tmp = test_tempdir("intent-adapter-npx-ancestor-");
        let workspace = tmp.path().join("plain workspace");
        std::fs::create_dir(&workspace).unwrap();
        let launch_root = seed_matching_workspace_ancestor(&tmp.path().join("home"));
        let report = tmp.path().join("npx-report");
        let npx = write_fake_npx(tmp.path());
        let cmd = AcpAdapterCommand::npx(npx, "claude-agent-acp@0.0.0-test")
            .cwd(workspace)
            .npx_launch_root(launch_root.clone())
            .env("INTENTD_FAKE_NPX_REPORT", report.as_os_str());

        let slots = AdapterSlots::new(1);
        let mut adapter = spawn_adapter_in(&slots, &cmd, Duration::from_secs(5))
            .await
            .expect("spawn fake npx");
        let (npx_cwd, entries) = read_npx_report(&report).await;
        let status = adapter.child.wait().await.expect("wait fake npx");
        assert!(
            npx_cwd.starts_with(&launch_root),
            "npx cwd {} is not under the launch root {}",
            npx_cwd.display(),
            launch_root.display()
        );
        assert!(
            status.success(),
            "npx must not adopt the launch root's ancestor workspace manifest (exit {status:?})"
        );
        assert_neutral_launch_dir(&npx_cwd, &entries);
    }

    /// Control: a resolved binary keeps `working_dir()` as its process cwd and
    /// takes no launch dir — the isolation is npx-only.
    #[tokio::test]
    async fn binary_adapter_keeps_the_pinned_cwd_as_its_process_cwd() {
        let tmp = test_tempdir("intent-adapter-bin-cwd-");
        let workspace = catalog_workspace(tmp.path());
        let report = tmp.path().join("bin-report");
        let cmd = AcpAdapterCommand::binary(
            PathBuf::from("sh"),
            vec!["-c".into(), "pwd > \"$INTENTD_BIN_REPORT\"".into()],
        )
        .cwd(workspace.clone())
        .env("INTENTD_BIN_REPORT", report.as_os_str());

        let slots = AdapterSlots::new(1);
        let mut adapter = spawn_adapter_in(&slots, &cmd, Duration::from_secs(5))
            .await
            .expect("spawn sh");
        assert!(adapter.child.wait().await.expect("wait sh").success());
        let pwd = std::fs::read_to_string(&report).expect("sh reported its cwd");
        assert_eq!(
            PathBuf::from(pwd.trim()).canonicalize().unwrap(),
            workspace.canonicalize().unwrap()
        );
    }

    /// A local, dependency-less npm package whose `bin` records `process.cwd()`
    /// into `$ADAPTER_REPORT`, then stays alive on stdin — what
    /// `npx -y <this path>` resolves offline, standing in for a pinned adapter.
    fn local_adapter_package(dir: &Path) -> PathBuf {
        let adapter = dir.join("adapter");
        std::fs::create_dir_all(&adapter).unwrap();
        std::fs::write(
            adapter.join("package.json"),
            r#"{"name":"intentd-test-adapter","version":"1.0.0","bin":{"intentd-test-adapter":"cli.js"}}"#,
        )
        .unwrap();
        std::fs::write(
            adapter.join("cli.js"),
            "#!/usr/bin/env node\n\
             require('fs').writeFileSync(process.env.ADAPTER_REPORT, 'ADAPTER_STARTED ' + process.cwd() + '\\n');\n\
             process.stdin.resume();\n",
        )
        .unwrap();
        adapter
    }

    /// intent-hq/intent#5738 against the REAL npm CLI (fake npx scripts only
    /// approximate `@npmcli/config`): under an ancestor whose `workspaces` glob
    /// matches the launch dirs and whose `.npmrc` sets a nonexistent
    /// `script-shell`, an ephemeral `npx -y <adapter>` must still start the
    /// adapter from a first launch dir (without the workspaces flag npm adopts
    /// the ancestor and dies with `ENOENT`, exit 254) and from a second one
    /// while the first is still alive (two same-named sentinels would be
    /// rejected as duplicate workspaces, exit 1). Offline, with private
    /// user/global npmrc and cache; skips without `npx`.
    #[tokio::test]
    async fn real_npx_adapter_ignores_matching_ancestor_workspaces_and_sibling_launch_dirs() {
        let Some(npx) = intent_providers::resolve_on_path("npx") else {
            eprintln!("skipping real-npx ephemeral adapter regression: npx not on PATH");
            return;
        };
        let tmp = test_tempdir("intent-adapter-npx-real-");
        let workspace = tmp.path().join("plain workspace");
        std::fs::create_dir(&workspace).unwrap();
        let home = tmp.path().join("clean-home");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(home.join("user.npmrc"), "").unwrap();
        std::fs::write(home.join("global.npmrc"), "").unwrap();
        let launch_root = seed_matching_workspace_ancestor(&tmp.path().join("parent"));
        let adapter = local_adapter_package(tmp.path());

        let slots = AdapterSlots::new(2);
        let mut launched = Vec::new();
        let mut alive = Vec::new();
        for label in ["first", "second"] {
            let report = tmp.path().join(format!("adapter-report-{label}"));
            let mut cmd = AcpAdapterCommand::npx(npx.clone(), adapter.to_str().unwrap())
                .cwd(workspace.clone())
                .npx_launch_root(launch_root.clone())
                .env("ADAPTER_REPORT", report.as_os_str());
            for (k, v) in [
                ("HOME", home.clone()),
                ("npm_config_userconfig", home.join("user.npmrc")),
                ("npm_config_globalconfig", home.join("global.npmrc")),
                ("npm_config_cache", tmp.path().join("npm-cache")),
            ] {
                cmd = cmd.env(k, v.as_os_str());
            }
            for (k, v) in [
                ("npm_config_offline", "true"),
                ("npm_config_update_notifier", "false"),
                ("npm_config_loglevel", "error"),
                ("DD_TRACE_ENABLED", "false"),
            ] {
                cmd = cmd.env(k, v);
            }
            // The previous launch dir is still alive (`alive`), so npm now
            // sees two same-named sentinels under the matching glob.
            let mut spawned = spawn_adapter_in(&slots, &cmd, Duration::from_secs(5))
                .await
                .expect("spawn real npx");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            let started = loop {
                if let Ok(s) = std::fs::read_to_string(&report) {
                    break s;
                }
                if let Ok(Some(status)) = spawned.child.try_wait() {
                    panic!(
                        "{label} launch: real npx exited {status:?} before the adapter started \
                         (254 = ancestor .npmrc script-shell adopted, 1 = duplicate workspace \
                         names); stderr: {:?}",
                        spawned.conn.recent_stderr()
                    );
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label} launch: adapter never started"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            let cwd = PathBuf::from(
                started
                    .trim()
                    .strip_prefix("ADAPTER_STARTED ")
                    .unwrap_or_else(|| panic!("{label} launch: unexpected report {started:?}")),
            );
            assert!(
                cwd.starts_with(&launch_root),
                "{label} launch: adapter cwd {} is not under the launch root {}",
                cwd.display(),
                launch_root.display()
            );
            launched.push(cwd);
            alive.push(spawned);
        }
        assert_ne!(launched[0], launched[1], "each launch gets its own dir");
        for mut spawned in alive {
            reap_child(&mut spawned.child).await;
        }
        for cwd in &launched {
            assert!(
                !cwd.exists(),
                "launch dir {} swept after reap",
                cwd.display()
            );
        }
    }
}
