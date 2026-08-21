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

#[cfg(unix)]
use intent_acp::{descendant_pids, sweep_escaped_descendants};
use intent_acp::{Connection, ConnectionHooks, IncomingNotification, IncomingRequest};
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
    /// Working directory for the child and the `session/new` `cwd`. `None`
    /// runs the adapter in the system temp dir (the ephemeral default).
    cwd: Option<PathBuf>,
    /// npx-run adapters get the longer cold-install timeout budget.
    via_npx: bool,
}

impl AcpAdapterCommand {
    /// Run a pinned npm package via `npx -y <package>`.
    pub(crate) fn npx(npx: PathBuf, package: &str) -> Self {
        Self {
            program: npx,
            args: vec!["-y".to_string(), package.to_string()],
            envs: Vec::new(),
            envs_removed: Vec::new(),
            cwd: None,
            via_npx: true,
        }
    }

    /// Run a resolved adapter binary with the given args.
    pub(crate) fn binary(bin: PathBuf, args: Vec<String>) -> Self {
        Self {
            program: bin,
            args,
            envs: Vec::new(),
            envs_removed: Vec::new(),
            cwd: None,
            via_npx: false,
        }
    }

    /// Append extra launch arguments after the ones already assembled.
    pub(crate) fn args(mut self, extra: impl IntoIterator<Item = String>) -> Self {
        self.args.extend(extra);
        self
    }

    /// Pin the adapter's working directory (also used as the `session/new`
    /// `cwd`). Callers that leave this unset run in the system temp dir.
    pub(crate) fn cwd(mut self, dir: PathBuf) -> Self {
        self.cwd = Some(dir);
        self
    }

    /// The effective working directory for this launch.
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
    let mut command = tokio::process::Command::new(&cmd.program);
    command
        .args(&cmd.args)
        .current_dir(cmd.working_dir())
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
