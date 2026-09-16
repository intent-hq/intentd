//! Shared test utilities for intentd integration tests.
//!
//! This module provides RAII guards for spawned daemon processes to prevent
//! process leaks when tests panic or fail to clean up explicitly, plus
//! multiplier-aware timeout helpers so budgets are centrally tunable.

// Each integration test binary compiles this module independently and only
// uses a subset of it, so unused items are expected.
#![allow(dead_code)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::{Arc, Mutex, Once};
use std::thread::ThreadId;
use std::time::Duration;

/// Force the hermetic-root guard on for every integration-test binary that
/// compiles this module. Runs before `main()` — and therefore before any test
/// threads exist, making `set_var` race-free — so any in-process code path
/// that falls back to the default `~/intent/workspaces` root panics loudly
/// (see `assert_hermetic_root_absent` in intent-services). Spawned daemons
/// inherit the variable, which is the already-supported hermetic mode: the
/// spawn helpers set `INTENTD_WORKSPACES_DIR` to a tempdir.
#[ctor::ctor(unsafe)]
fn force_hermetic_root_guard() {
    std::env::set_var("INTENTD_ASSERT_HERMETIC_ROOT", "1");
    // Node children spawned by tests (mock ACP agents, MCP fixtures) inherit
    // this and skip `module.enableCompileCache()`, which would otherwise leave
    // a `node-compile-cache/` residue at the TMPDIR root after the suite.
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
    // Daemon-spawned provider probes can exec a real `pi` CLI, whose jiti
    // extension loader transpile-caches `.mjs` files under `$TMPDIR/jiti/`.
    // jiti honors this boolean env (`_booleanEnv("JITI_FS_CACHE", ...)`), so
    // disabling the fs cache keeps the TMPDIR root clean after e2e suites.
    std::env::set_var("JITI_FS_CACHE", "false");
}

/// Apply the timeout multiplier from the environment for coverage
/// instrumentation. Reads `INTENTD_TEST_TIMEOUT_MULTIPLIER` (defaults to 1.0;
/// non-finite values are ignored and values below 1.0 are clamped so budgets
/// can only be extended; overflow saturates to `Duration::MAX`).
pub fn test_timeout(base: Duration) -> Duration {
    let multiplier = std::env::var("INTENTD_TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|m| m.is_finite())
        .unwrap_or(1.0);
    Duration::try_from_secs_f64(base.as_secs_f64() * multiplier.max(1.0)).unwrap_or(Duration::MAX)
}

/// Shared budget for waiting on daemon startup (UDS/socket ready): 60s base,
/// scaled by `INTENTD_TEST_TIMEOUT_MULTIPLIER`. The generous budget absorbs
/// coverage-instrumented startup on oversubscribed CI runners.
pub fn daemon_startup_timeout() -> Duration {
    test_timeout(Duration::from_secs(60))
}

/// Shared budget for one RPC/frame read against a live daemon — UDS
/// `read_line` responses, WSS `ws.next()` responses/events. Delegates to
/// [`daemon_startup_timeout`] (60s base, scaled by
/// `INTENTD_TEST_TIMEOUT_MULTIPLIER`): the bound only guards against hangs —
/// a healthy daemon answers in milliseconds — while absorbing CPU contention
/// when the parallel suite saturates the machine (intent-hq/monorepo#615).
pub fn rpc_read_timeout() -> Duration {
    daemon_startup_timeout()
}

/// Create a temp dir with a recognizable `prefix` under the system temp root.
/// The returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
///
/// When the creating test thread panics, the dir is retained for post-mortem
/// instead — see [`register_for_failure_retention`].
pub fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test tempdir");
    if keep_tmp_requested() {
        dir.disable_cleanup(true);
    }
    register_for_failure_retention(dir.path());
    dir
}

/// Like [`test_tempdir`], but rooted at `base` instead of the system temp
/// root. Use with `"/tmp"` when the dir must stay short enough for a UDS
/// socket path (macOS caps them at ~104 bytes; `temp_dir()` resolves to a
/// long `/var/folders/...` path).
pub fn test_tempdir_in(base: &str, prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(base)
        .expect("create test tempdir");
    if keep_tmp_requested() {
        dir.disable_cleanup(true);
    }
    register_for_failure_retention(dir.path());
    dir
}

fn keep_tmp_requested() -> bool {
    std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty())
}

/// Test tempdirs still eligible for failure-time retention, keyed by the
/// thread that created them. Entries whose dir no longer exists (dropped by
/// a passing test) are pruned opportunistically; the panic hook removes the
/// entries it retains.
static RETENTION_REGISTRY: Mutex<Vec<(ThreadId, PathBuf)>> = Mutex::new(Vec::new());
static RETENTION_HOOK: Once = Once::new();

thread_local! {
    static RETENTION_SUPPRESSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Prefix of a retained (renamed) test tempdir. Deliberately not `itd-` so
/// retained evidence never counts as a leak in `/tmp/itd-*` hygiene sweeps.
const RETAINED_DIR_PREFIX: &str = "failed-";

/// Cap on the `daemon.log` lines echoed when a tempdir is retained.
const RETAINED_LOG_TAIL_LINES: usize = 200;

/// Keep `dir` for post-mortem when the current thread panics (intent-hq/intent#4971).
///
/// `tempfile::TempDir` sweeps its dir during unwinding, which destroys the
/// daemon log / config / database state of an intermittently failing e2e before
/// anyone can read it. A process-wide panic hook (installed once, chaining the
/// previous hook) renames every dir registered by the panicking thread to a
/// `failed-<original name>` sibling *before* unwinding reaches the guard, so
/// the guard's `remove_dir_all` finds nothing and the evidence survives. The
/// retained path and a bounded `daemon.log` tail are echoed to stderr, which
/// libtest/nextest surface for failed tests. Under `INTENTD_TEST_KEEP_TMP` the
/// dir is already kept, so only the path and tail are echoed.
///
/// Scoping is by creating thread, so a passing test never has its dir
/// retained: with nextest each test is its own process, and under `cargo
/// test` a panic on one test thread does not touch sibling tests' dirs. Tests
/// that *deliberately* trigger caught panics on the test thread (e.g.
/// `catch_unwind`-guarded handler panics on a current-thread runtime) hold a
/// [`suppress_failure_retention`] guard across that window or they leave a
/// `failed-*` dir behind on every passing run.
pub fn register_for_failure_retention(dir: &Path) {
    RETENTION_HOOK.call_once(install_retention_panic_hook);
    if let Ok(mut registry) = RETENTION_REGISTRY.lock() {
        registry.retain(|(_, path)| path.exists());
        registry.push((std::thread::current().id(), dir.to_path_buf()));
    }
}

/// Opt the current thread out of failure-time tempdir retention while the
/// returned guard lives. For tests whose *passing* path panics on the test
/// thread (caught panics); see [`register_for_failure_retention`].
///
/// A genuine failure inside the window still keeps its evidence: the panic
/// hook skips the thread, but the guard is dropped by the unwinding itself and
/// then runs the retention. Declare the guard *after* the tempdirs it covers
/// so it drops before their `TempDir` guards sweep.
#[must_use = "retention is suppressed only while the guard is alive"]
pub fn suppress_failure_retention() -> RetentionSuppressed {
    let previous = RETENTION_SUPPRESSED.with(|flag| flag.replace(true));
    RetentionSuppressed { previous }
}

/// Guard returned by [`suppress_failure_retention`].
pub struct RetentionSuppressed {
    previous: bool,
}

impl Drop for RetentionSuppressed {
    fn drop(&mut self) {
        RETENTION_SUPPRESSED.with(|flag| flag.set(self.previous));
        if std::thread::panicking() && !self.previous {
            retain_tempdirs_of_panicking_thread();
        }
    }
}

/// Where [`register_for_failure_retention`] moves `dir` on failure: a
/// `failed-`-prefixed sibling in the same parent.
pub fn retained_path_for(dir: &Path) -> PathBuf {
    let name = dir
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default();
    dir.with_file_name(format!("{RETAINED_DIR_PREFIX}{name}"))
}

fn install_retention_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        previous(info);
        retain_tempdirs_of_panicking_thread();
    }));
}

fn retain_tempdirs_of_panicking_thread() {
    if RETENTION_SUPPRESSED.with(std::cell::Cell::get) {
        return;
    }
    let me = std::thread::current().id();
    // Bounded spin rather than `lock()`: a panic raised while this thread
    // holds the registry (inside `register_for_failure_retention`) would
    // otherwise deadlock the hook.
    let mut mine = Vec::new();
    for _ in 0..50 {
        match RETENTION_REGISTRY.try_lock() {
            Ok(mut registry) => {
                registry.retain(|(_, path)| path.exists());
                let (taken, kept): (Vec<_>, Vec<_>) =
                    registry.drain(..).partition(|(tid, _)| *tid == me);
                *registry = kept;
                mine = taken.into_iter().map(|(_, path)| path).collect();
                break;
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return,
        }
    }
    if mine.is_empty() {
        return;
    }
    let thread = std::thread::current();
    let test = thread.name().unwrap_or("<unnamed>");
    let mut report = String::new();
    for original in mine {
        let retained = if keep_tmp_requested() {
            original.clone()
        } else {
            let target = retained_path_for(&original);
            match std::fs::rename(&original, &target) {
                Ok(()) => target,
                Err(e) => {
                    let _ = writeln!(
                        report,
                        "--- test tempdir NOT retained (test `{test}`): rename {} -> {} failed: {e} ---",
                        original.display(),
                        target.display()
                    );
                    continue;
                }
            }
        };
        report.push_str(&retained_dir_report(test, &original, &retained));
    }
    eprint!("{report}");
}

/// What the retention hook prints for one retained dir: the retained and
/// original paths, then the last [`RETAINED_LOG_TAIL_LINES`] lines of its
/// `daemon.log` (or a placeholder when there is none).
fn retained_dir_report(test: &str, original: &Path, retained: &Path) -> String {
    let mut report = format!(
        "--- test tempdir retained for post-mortem (test `{test}`) ---\nretained: {}\noriginal: {}\n",
        retained.display(),
        original.display()
    );
    let log = retained.join("daemon.log");
    if log.is_file() {
        let _ = writeln!(
            report,
            "{}",
            log_tail_section(&log, RETAINED_LOG_TAIL_LINES).trim_start_matches('\n')
        );
    } else {
        let _ = writeln!(report, "(no daemon.log in retained dir)");
    }
    report
}

/// Return a unique, hermetic workspaces root under the OS temp dir.
///
/// In-process integration tests must chain
/// `.with_workspaces_root(root.path().to_path_buf())` onto every
/// `Services::new(...)` so tests never resolve the real `~/intent/workspaces`.
/// The returned `TempDir` guard must be held for the full test lifetime: it
/// removes the tree on drop (including on panic), so dropping it early would
/// let the services layer recreate — and leak — the root. Set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep the dir for debugging.
pub fn hermetic_workspaces_root() -> tempfile::TempDir {
    test_tempdir("itd-ws-")
}

/// A settings registry (backed by `config.toml` under `dir`) seeding
/// `model.defaultProvider = "auggie"`: since monorepo#3044 there is no positional
/// provider fallback, so in-process tests that create/delegate agents without
/// an explicit provider or model must chain
/// `.with_settings_registry(common::registry_with_default_provider(dir))`
/// onto `Services::new(...)` to have a configured default to resolve to. The
/// `providers.paths` override points auggie at a deterministic executable so
/// availability checks (`agent.delegate`) pass without the real binary on
/// the test host.
pub fn registry_with_default_provider(
    dir: &std::path::Path,
) -> Arc<intent_services::SettingsRegistry> {
    let registry = Arc::new(
        intent_services::SettingsRegistry::load(dir.join("config.toml")).expect("load registry"),
    );
    registry
        .apply(&[
            (
                "model.defaultProvider".to_string(),
                serde_json::json!("auggie"),
            ),
            (
                "providers.paths".to_string(),
                serde_json::json!({ "auggie": "/bin/sh" }),
            ),
        ])
        .expect("seed default provider");
    registry
}

/// Wait until a freshly spawned `intentd serve` child accepts connections on
/// its UDS `socket`, budgeted by [`daemon_startup_timeout`]. Fails fast —
/// panicking with the daemon log — if the child exits before listening, so
/// tests don't keep polling a dead daemon for the full window. Unix-only
/// (UDS); gated so test binaries without `#![cfg(unix)]` still compile
/// `common` on non-Unix targets and keep the ctor guard.
#[cfg(unix)]
pub async fn await_daemon_listening(child: &mut Child, socket: &Path, log_path: &Path) {
    let budget = daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if tokio::net::UnixStream::connect(socket).await.is_ok() {
            return;
        }
        if let Ok(Some(status)) = child.try_wait() {
            let logs = std::fs::read_to_string(log_path).unwrap_or_default();
            panic!(
                "daemon exited ({status}) before listening on {}\n--- daemon log ---\n{logs}",
                socket.display()
            );
        }
        if tokio::time::Instant::now() >= deadline {
            let logs = std::fs::read_to_string(log_path).unwrap_or_default();
            panic!(
                "daemon never listened on {} within {budget:?}\n--- daemon log ---\n{logs}",
                socket.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// One `system.status` round-trip over the daemon's UDS control socket. The
/// whole connect + write + read sequence shares a single `budget` timeout so a
/// wedged daemon cannot stall the readiness poll below beyond that bound.
#[cfg(unix)]
async fn try_status_rpc(socket: &Path, budget: Duration) -> Result<serde_json::Value, String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let rpc = async {
        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|e| format!("uds connect failed: {e}"))?;
        let (read_half, mut write_half) = stream.into_split();
        let frame = "{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"system.status\",\"params\":{}}\n";
        write_half
            .write_all(frame.as_bytes())
            .await
            .map_err(|e| format!("uds write failed: {e}"))?;
        write_half
            .flush()
            .await
            .map_err(|e| format!("uds write failed: {e}"))?;
        let mut buf = String::new();
        BufReader::new(read_half)
            .read_line(&mut buf)
            .await
            .map_err(|e| format!("uds read failed: {e}"))?;
        serde_json::from_str(buf.trim_end()).map_err(|e| format!("invalid JSON frame: {e}"))
    };
    tokio::time::timeout(budget, rpc)
        .await
        .map_err(|_| format!("status rpc timed out after {budget:?}"))?
}

/// Cap on the daemon-log lines included in a readiness-timeout panic, so
/// the tail stays readable in test output.
#[cfg(unix)]
const LOG_TAIL_LINES: usize = 100;

/// Render the tail of the daemon log at `log_path` as a panic-message
/// section, mirroring [`await_daemon_listening`]'s log-dump pattern so a
/// readiness timeout is attributable (monorepo#1051: "slow" vs "bind
/// failed"). Returns an empty string when no log path was provided; an
/// unreadable log yields a placeholder rather than masking the timeout
/// panic. Bounded to the last [`LOG_TAIL_LINES`] lines.
#[cfg(unix)]
fn daemon_log_tail_section(log_path: Option<&Path>) -> String {
    let Some(path) = log_path else {
        return String::new();
    };
    log_tail_section(path, LOG_TAIL_LINES)
}

/// Render the last `max_lines` of the log at `path` as a
/// `--- daemon log tail ---` section (leading newline included); an
/// unreadable log yields a placeholder instead.
fn log_tail_section(path: &Path, max_lines: usize) -> String {
    let logs = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => return format!("\n--- daemon log ({}) unreadable: {e} ---", path.display()),
    };
    let lines: Vec<&str> = logs.lines().collect();
    let skipped = lines.len().saturating_sub(max_lines);
    let tail = lines[skipped..].join("\n");
    format!(
        "\n--- daemon log tail ({}{}) ---\n{tail}",
        path.display(),
        if skipped > 0 {
            format!(", {skipped} earlier lines omitted")
        } else {
            String::new()
        }
    )
}

/// Poll `system.status` over the daemon's UDS control socket until the WSS
/// listener is bound — i.e. the response carries `result.port` — and return
/// that full JSON-RPC response (intent-hq/monorepo#559). The UDS socket can
/// accept (and `system.status` answer) while the status snapshot still lacks
/// the WSS port, so a single-shot lookup panics on `expect("port")` under
/// parallel load. Bounded by [`daemon_startup_timeout`] with a short
/// exponential backoff; a daemon whose WSS listener never binds still fails
/// deterministically, panicking with the last observed response. Readiness
/// poll ONLY — callers must not use this to retry assertions or other RPCs.
///
/// Prefer [`await_wss_status_logged`] where the daemon log path is at hand:
/// it additionally dumps the log tail on timeout (monorepo#1051).
#[cfg(unix)]
pub async fn await_wss_status(socket: &Path) -> serde_json::Value {
    await_wss_status_impl(socket, None).await
}

/// [`await_wss_status`] variant that also surfaces the tail of the daemon
/// log at `log_path` in the timeout panic, so a readiness flake is
/// attributable from the failure output alone (monorepo#1051).
#[cfg(unix)]
pub async fn await_wss_status_logged(socket: &Path, log_path: &Path) -> serde_json::Value {
    await_wss_status_impl(socket, Some(log_path)).await
}

#[cfg(unix)]
async fn await_wss_status_impl(socket: &Path, log_path: Option<&Path>) -> serde_json::Value {
    let budget = daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    let rpc_budget = test_timeout(Duration::from_secs(5));
    let mut backoff = Duration::from_millis(25);
    let mut attempts: u32 = 0;
    let mut last: String;
    loop {
        attempts += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match try_status_rpc(
            socket,
            rpc_budget.min(remaining.max(Duration::from_millis(1))),
        )
        .await
        {
            Ok(resp) => {
                if resp["result"]["port"].as_u64().is_some() {
                    return resp;
                }
                last = resp.to_string();
            }
            Err(e) => last = e,
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "WSS listener not ready: system.status returned no result.port on {} within \
             {budget:?} ({attempts} attempts); last: {last}{}",
            socket.display(),
            daemon_log_tail_section(log_path)
        );
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(Duration::from_millis(500));
    }
}

/// Poll `system.status` over the daemon's UDS control socket until the WSS
/// listener is stopped — i.e. the response carries a null `result.port` —
/// the counterpart of [`await_wss_status`] for runtime-disable paths
/// (monorepo#515). Listener teardown after a `settings.update` disable is
/// asynchronous, so a fixed post-disable sleep plus a single-shot status
/// lookup flakes under parallel load. Same budget/backoff discipline;
/// readiness poll ONLY — callers must not use this to retry assertions.
///
/// Prefer [`await_wss_stopped_logged`] where the daemon log path is at hand:
/// it additionally dumps the log tail on timeout (monorepo#1051).
#[cfg(unix)]
pub async fn await_wss_stopped(socket: &Path) {
    await_wss_stopped_impl(socket, None).await;
}

/// [`await_wss_stopped`] variant that also surfaces the tail of the daemon
/// log at `log_path` in the timeout panic, so a teardown flake is
/// attributable from the failure output alone (monorepo#1051).
#[cfg(unix)]
pub async fn await_wss_stopped_logged(socket: &Path, log_path: &Path) {
    await_wss_stopped_impl(socket, Some(log_path)).await;
}

#[cfg(unix)]
async fn await_wss_stopped_impl(socket: &Path, log_path: Option<&Path>) {
    let budget = daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    let rpc_budget = test_timeout(Duration::from_secs(5));
    let mut backoff = Duration::from_millis(25);
    let mut attempts: u32 = 0;
    let mut last: String;
    loop {
        attempts += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match try_status_rpc(
            socket,
            rpc_budget.min(remaining.max(Duration::from_millis(1))),
        )
        .await
        {
            Ok(resp) => {
                // Require a real success envelope: an error response also has
                // a null `result.port` under serde_json indexing, but proves
                // nothing about the listener.
                if resp["result"].is_object() && resp["result"]["port"].is_null() {
                    return;
                }
                last = resp.to_string();
            }
            Err(e) => last = e,
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "WSS listener not stopped: system.status still reports result.port on {} within \
             {budget:?} ({attempts} attempts); last: {last}{}",
            socket.display(),
            daemon_log_tail_section(log_path)
        );
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(Duration::from_millis(500));
    }
}

/// The one way an e2e suite spawns `intentd serve`: the `intentd` test binary
/// with the `serve` subcommand and the `INTENTD_TCP_PORT=0` ephemeral-port
/// seam (monorepo#1051) already set, so a daemon whose WSS listener is enabled
/// ([`enable_ws_api`]) binds a true OS-assigned port instead of racing another
/// process for the seeded one. Callers add everything else themselves (data
/// dir, workspaces dir, token, stdio, `process_group`, mock-agent env): the
/// builder stays thin so migration is mechanical. The seam is inert for a
/// UDS-only daemon (no WSS listener, no bind). A later `.env("INTENTD_TCP_PORT",
/// …)` on the returned `Command` overrides the seam, so a deliberate pin (e.g.
/// an out-of-range value to prove startup refusal) still works.
///
/// `serve_spawn_guard.rs` is a bounded textual backstop for this: it fails
/// the suite on a single-statement `Command::new(env!("CARGO_BIN_EXE_intentd"))
/// … "serve"` outside this module (30-line cap), and on a file whose code calls
/// [`enable_ws_api`] without a builder call in code. A split-statement raw
/// spawn in a file that also calls a builder is not detected.
pub fn serve_command() -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve").env("INTENTD_TCP_PORT", "0");
    cmd
}

/// [`serve_command`] WITHOUT the `INTENTD_TCP_PORT=0` seam: the WSS listener
/// binds the `server.wsApi.port` seeded by [`enable_ws_api`] (or set later via
/// `settings.update`), accepting the reserve-then-release TOCTOU window on
/// that port. Only for suites that need the settings-file port to be the
/// bound port — a listener restart that must rebind the same port, or a
/// settings batch whose explicit port is exactly what the test proves.
pub fn serve_command_fixed_port() -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve");
    cmd
}

/// Enable the WSS/TCP listener for a daemon booted from `data_dir` by seeding
/// `config.toml` with `[server.wsApi] enabled = true` plus an OS-assigned free
/// port (the config-driven replacement for the retired `serve --listen both`
/// flag: UDS always serves; the WSS listener boot-starts iff the effective
/// `server.wsApi.enabled` is true, binding `server.wsApi.port`).
///
/// Port interplay: daemons spawned via [`serve_command`] carry the
/// `INTENTD_TCP_PORT=0` seam, which wins over the seeded settings port, so
/// they get a true OS-assigned ephemeral bind and never race another process
/// for the port this helper reserved and released before the daemon booted
/// (monorepo#1051). The seeded port is bound only by
/// [`serve_command_fixed_port`] spawns, keeping them off the fixed 5181
/// default that would collide across parallel daemons. Either way, read the
/// real port from `system.status` ([`await_wss_status`]), never from the
/// seeded config value — with the seam, the ephemeral port changes across
/// boots on the same data dir. `serve_spawn_guard.rs` backs this up with a
/// file-level rule: a file whose code calls this helper must also call one of
/// the two builders in code (comments do not count), or carry a reasoned
/// `serve-spawn: allow` marker. Appends to an existing seeded config; no-op if
/// the table is already present.
pub fn enable_ws_api(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("read {}: {e}", path.display()),
    };
    if text
        .lines()
        .any(|l| l.trim_start().starts_with("[server.wsApi]"))
    {
        return;
    }
    let port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    let _ = write!(text, "\n[server.wsApi]\nenabled = true\nport = {port}\n");
    std::fs::write(&path, text).expect("seed config.toml with server.wsApi.enabled");
}

/// Seed the daemon's `config.toml` under `data_dir` with
/// `[model] defaultProvider = "auggie"`: since monorepo#3044 there is no
/// positional provider fallback, so spawned-daemon suites whose tests
/// create/delegate agents without an explicit provider or model must seed a
/// configured default before boot. Also seeds a `providers.paths` override
/// pointing `auggie` at `/bin/sh` so availability checks (e.g.
/// `agent.delegate`'s `ensure_provider_available`) stay hermetic — no real
/// auggie binary required on the test host (monorepo#3162). Appends to an
/// existing seeded config; no-op if a `[model]` table is already present.
pub fn seed_default_provider(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("read {}: {e}", path.display()),
    };
    if text.lines().any(|l| l.trim_start().starts_with("[model]")) {
        return;
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(
        "\n[model]\ndefaultProvider = \"auggie\"\n\n[providers.paths]\nauggie = \"/bin/sh\"\n",
    );
    std::fs::write(&path, text).expect("seed config.toml with model.defaultProvider");
}

/// Seed `config.toml` with `[agents] resumeInterruptedOnStart = "off"` so a
/// restarted daemon does NOT auto-resume pending interrupted agents. Restart
/// suites that assert rows stay pending in `agent.listInterrupted` need this
/// pin: the setting defaults to `auto`, which resumes on headless hosts (no
/// display) — exactly what CI runners are. Appends to an existing seeded
/// config; idempotent when the pin is already present (restart suites call
/// this on every daemon boot). Panics if an `[agents]` table exists WITHOUT
/// the pin — silently skipping would only surface as a flake on headless
/// runners.
pub fn disable_resume_on_start(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("read {}: {e}", path.display()),
    };
    if text.lines().any(|l| l.trim_start().starts_with("[agents]")) {
        if text
            .lines()
            .any(|l| l.trim() == "resumeInterruptedOnStart = \"off\"")
        {
            return;
        }
        panic!(
            "{} already has an [agents] table without resumeInterruptedOnStart = \
             \"off\" — disable_resume_on_start cannot append a second table; merge \
             the pin into the existing [agents] table at the test site instead",
            path.display()
        );
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str("\n[agents]\nresumeInterruptedOnStart = \"off\"\n");
    std::fs::write(&path, text).expect("seed config.toml with agents.resumeInterruptedOnStart");
}

/// The pinned-TLS WebSocket client stream type shared by the WSS e2e suites.
pub type TlsWs =
    tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

/// Bounded retry budget for WSS/TLS connection establishment (#553): up to 5
/// attempts with a short exponential backoff. Retries cover **connection
/// establishment only** — TCP connect, TLS handshake, WebSocket upgrade — and
/// trigger only on transient connect-phase I/O errors (reset / refused /
/// aborted / broken pipe / unexpected EOF), which the daemon's accept path can
/// produce when the machine is saturated by the parallel test suite. Genuine
/// failures (auth rejections, fingerprint mismatches, timeouts) stay fatal on
/// the first attempt, so a daemon that never accepts still fails the test
/// within a bounded time.
const CONNECT_ATTEMPTS: u32 = 5;
const CONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Per-phase budget for one connection attempt, scaled by
/// `INTENTD_TEST_TIMEOUT_MULTIPLIER` like every other test budget.
fn connect_phase_timeout() -> Duration {
    test_timeout(Duration::from_secs(5))
}

/// One failed connection-establishment phase. `transient` marks the
/// load-induced I/O errors worth retrying; everything else is fatal.
struct ConnectAttemptError {
    phase: &'static str,
    message: String,
    transient: bool,
}

fn transient_connect_kind(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    )
}

/// One TCP + TLS connection attempt, each phase bounded by
/// [`connect_phase_timeout`].
async fn try_tls_connect(
    port: u16,
    cfg: Arc<rustls::ClientConfig>,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, ConnectAttemptError> {
    let budget = connect_phase_timeout();
    let tcp = match tokio::time::timeout(
        budget,
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)),
    )
    .await
    {
        Err(_) => {
            return Err(ConnectAttemptError {
                phase: "tcp connect",
                message: format!("timed out after {budget:?}"),
                transient: false,
            })
        }
        Ok(Err(e)) => {
            return Err(ConnectAttemptError {
                phase: "tcp connect",
                transient: transient_connect_kind(e.kind()),
                message: e.to_string(),
            })
        }
        Ok(Ok(tcp)) => tcp,
    };
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    match tokio::time::timeout(
        budget,
        tokio_rustls::TlsConnector::from(cfg).connect(name, tcp),
    )
    .await
    {
        Err(_) => Err(ConnectAttemptError {
            phase: "tls connect",
            message: format!("timed out after {budget:?}"),
            transient: false,
        }),
        Ok(Err(e)) => Err(ConnectAttemptError {
            phase: "tls connect",
            transient: transient_connect_kind(e.kind()),
            message: e.to_string(),
        }),
        Ok(Ok(tls)) => Ok(tls),
    }
}

/// One full connection-establishment attempt: TCP + TLS + WebSocket upgrade.
async fn try_wss_connect(
    port: u16,
    cfg: Arc<rustls::ClientConfig>,
    url: &str,
) -> Result<TlsWs, ConnectAttemptError> {
    let tls = try_tls_connect(port, cfg).await?;
    let budget = connect_phase_timeout();
    match tokio::time::timeout(budget, tokio_tungstenite::client_async(url, tls)).await {
        Err(_) => Err(ConnectAttemptError {
            phase: "ws handshake",
            message: format!("timed out after {budget:?}"),
            transient: false,
        }),
        Ok(Err(e)) => Err(ConnectAttemptError {
            phase: "ws handshake",
            transient: matches!(
                &e,
                tokio_tungstenite::tungstenite::Error::Io(io) if transient_connect_kind(io.kind())
            ),
            message: e.to_string(),
        }),
        Ok(Ok((ws, _resp))) => Ok(ws),
    }
}

/// Open a pinned TLS stream to `127.0.0.1:port` (SNI `localhost`), retrying
/// transient connect-phase failures per the bounded policy above. Panics —
/// like the `expect`-based helpers it replaces — once the attempt budget is
/// exhausted or on any non-transient failure.
pub async fn tls_connect_with_retry(
    port: u16,
    cfg: Arc<rustls::ClientConfig>,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let mut backoff = CONNECT_INITIAL_BACKOFF;
    let mut attempt = 1;
    loop {
        match try_tls_connect(port, cfg.clone()).await {
            Ok(tls) => return tls,
            Err(e) if e.transient && attempt < CONNECT_ATTEMPTS => {
                eprintln!(
                    "tls connect attempt {attempt}/{CONNECT_ATTEMPTS} failed during {} ({}); \
                     retrying in {backoff:?}",
                    e.phase, e.message
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                attempt += 1;
            }
            Err(e) => panic!(
                "{} failed on attempt {attempt}/{CONNECT_ATTEMPTS}: {}",
                e.phase, e.message
            ),
        }
    }
}

/// Establish a pinned-TLS WebSocket connection to `url`, retrying transient
/// connect-phase failures (each retry redoes TCP + TLS + upgrade on a fresh
/// socket). Only connection establishment is retried — RPCs, event waits, and
/// assertions never pass through this helper. `url` must target the same
/// `port` the socket is opened against (`wss://localhost:{port}/…`).
pub async fn wss_connect_with_retry(port: u16, cfg: Arc<rustls::ClientConfig>, url: &str) -> TlsWs {
    assert!(
        url.starts_with(&format!("wss://localhost:{port}/")),
        "wss_connect_with_retry: url {url:?} does not target wss://localhost:{port}/"
    );
    let mut backoff = CONNECT_INITIAL_BACKOFF;
    let mut attempt = 1;
    loop {
        match try_wss_connect(port, cfg.clone(), url).await {
            Ok(ws) => return ws,
            Err(e) if e.transient && attempt < CONNECT_ATTEMPTS => {
                eprintln!(
                    "wss connect attempt {attempt}/{CONNECT_ATTEMPTS} failed during {} ({}); \
                     retrying in {backoff:?}",
                    e.phase, e.message
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                attempt += 1;
            }
            Err(e) => panic!(
                "{} failed on attempt {attempt}/{CONNECT_ATTEMPTS}: {}",
                e.phase, e.message
            ),
        }
    }
}

/// RAII guard for a spawned `intentd serve` process.
///
/// Ensures the daemon child process is killed on drop (SIGKILL to the process
/// group) and optionally removes the temp data directory. This prevents leaked
/// daemon processes when tests panic or abort before explicit cleanup.
///
/// The guard sends SIGKILL to the process group (not just the parent PID),
/// which also terminates any child processes spawned by the daemon (e.g., Node
/// mock agents in ACP provider tests).
pub struct DaemonGuard {
    child: Child,
    data_dir: Option<PathBuf>,
}

impl DaemonGuard {
    /// Create a new daemon guard that will kill the child process on drop.
    ///
    /// If `cleanup_data_dir` is true, the data directory will be removed on drop.
    pub fn new(child: Child, data_dir: PathBuf, cleanup_data_dir: bool) -> Self {
        Self {
            child,
            data_dir: if cleanup_data_dir {
                Some(data_dir)
            } else {
                None
            },
        }
    }

    /// Create a daemon guard that only kills the process (no data dir cleanup).
    pub fn process_only(child: Child) -> Self {
        Self {
            child,
            data_dir: None,
        }
    }

    /// Get a mutable reference to the child process.
    ///
    /// Useful for calling `wait()`, `try_wait()`, or `kill()` explicitly.
    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Take ownership of the child process, consuming the guard.
    ///
    /// The caller is responsible for cleanup after this point.
    pub fn into_child(mut self) -> Child {
        let child = std::mem::replace(
            &mut self.child,
            // Placeholder - will be dropped immediately after we return the real child
            unsafe { std::mem::zeroed() },
        );
        // Prevent Drop from running by forgetting self
        std::mem::forget(self);
        child
    }

    /// Disable data directory cleanup on drop.
    ///
    /// Useful when the test wants to inspect the data directory after the daemon stops.
    pub fn keep_data_dir(mut self) -> Self {
        self.data_dir = None;
        self
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        // SIGKILL the process (or process group if set).
        // Ignore errors - the process may have already exited.
        let _ = self.child.kill();
        let _ = self.child.wait();

        // Clean up data directory if requested.
        if let Some(ref dir) = self.data_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn guard_kills_process_on_drop() {
        // Spawn a sleep process. Detach all three stdio streams so the child
        // never inherits nextest's stdout/stderr capture pipes — an inherited
        // pipe is what nextest's leak detector keys on (intent-hq/intent#4284).
        let child = Command::new("sleep")
            .arg("3600")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        {
            let _guard = DaemonGuard::process_only(child);
            // Guard goes out of scope here
        }

        // Process should be dead. Probe with signal 0 in-process instead of
        // spawning an external `kill -0`, which would inherit the same pipes.
        let probe = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid.cast_signed()), None);

        assert!(
            probe.is_err(),
            "process should be dead after guard drop (kill(pid, 0) returned {probe:?})"
        );
    }

    /// Run `f` on a fresh thread so the retention registry / suppression
    /// flag see a thread that owns nothing but what `f` creates.
    fn on_fresh_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::spawn(f).join().expect("test thread")
    }

    fn caught_panic() {
        let _ = std::panic::catch_unwind(|| panic!("intentional: exercise retention hook"));
    }

    #[test]
    fn tempdir_is_retained_when_creating_thread_panics() {
        let (original, retained) = on_fresh_thread(|| {
            let dir = test_tempdir("itd-retain-");
            std::fs::write(dir.path().join("daemon.log"), "line\n".repeat(3)).expect("write log");
            let original = dir.path().to_path_buf();
            let retained = retained_path_for(&original);
            assert_eq!(
                retained.file_name().unwrap().to_string_lossy(),
                format!("failed-{}", original.file_name().unwrap().to_string_lossy())
            );
            caught_panic();
            (original, retained)
        });
        if keep_tmp_requested() {
            assert!(original.is_dir(), "KEEP_TMP keeps the dir in place");
            let _ = std::fs::remove_dir_all(&original);
            return;
        }
        assert!(!original.exists(), "original swept after retention");
        assert!(
            retained.is_dir(),
            "retained dir exists at {}",
            retained.display()
        );
        assert!(
            retained.join("daemon.log").is_file(),
            "daemon.log travelled with the dir"
        );
        std::fs::remove_dir_all(&retained).expect("clean up retained dir");
    }

    #[test]
    fn tempdir_is_not_retained_on_success() {
        let (original, retained) = on_fresh_thread(|| {
            let dir = test_tempdir("itd-retain-");
            let original = dir.path().to_path_buf();
            (original.clone(), retained_path_for(&original))
        });
        if keep_tmp_requested() {
            let _ = std::fs::remove_dir_all(&original);
        } else {
            assert!(!original.exists(), "passing thread sweeps its dir");
        }
        assert!(!retained.exists(), "no failed-* sibling without a panic");
    }

    #[test]
    fn retained_dir_report_carries_bounded_log_tail() {
        let dir = test_tempdir("itd-retain-");
        let total = RETAINED_LOG_TAIL_LINES + 50;
        let log = (1..=total).fold(String::new(), |mut s, i| {
            let _ = writeln!(s, "line {i}");
            s
        });
        std::fs::write(dir.path().join("daemon.log"), log).expect("write log");
        let original = Path::new("/tmp/itd-original"); // tmp-hygiene: allow — path arithmetic only, never touched

        let report = retained_dir_report("some_test", original, dir.path());
        assert!(report.contains("(test `some_test`)"), "{report}");
        assert!(
            report.contains(&format!("retained: {}\n", dir.path().display())),
            "{report}"
        );
        assert!(report.contains("original: /tmp/itd-original\n"), "{report}");
        assert!(
            report.contains(&format!(
                "--- daemon log tail ({}, 50 earlier lines omitted) ---\n",
                dir.path().join("daemon.log").display()
            )),
            "{report}"
        );
        let emitted: Vec<&str> = report.lines().filter(|l| l.starts_with("line ")).collect();
        assert_eq!(emitted.len(), RETAINED_LOG_TAIL_LINES, "{report}");
        assert_eq!(emitted.first().copied(), Some("line 51"));
        assert_eq!(
            emitted.last().copied(),
            Some(format!("line {total}").as_str())
        );

        std::fs::remove_file(dir.path().join("daemon.log")).expect("remove log");
        let report = retained_dir_report("some_test", original, dir.path());
        assert!(
            report.ends_with("(no daemon.log in retained dir)\n"),
            "{report}"
        );
    }

    #[test]
    fn suppressed_thread_keeps_normal_cleanup_on_caught_panic() {
        let (original, retained) = on_fresh_thread(|| {
            let dir = test_tempdir("itd-retain-");
            let original = dir.path().to_path_buf();
            {
                let _suppress = suppress_failure_retention();
                caught_panic();
            }
            assert!(original.is_dir(), "suppressed: dir untouched by the hook");
            caught_panic();
            assert!(
                !original.exists(),
                "retention is back once the guard is dropped"
            );
            (original.clone(), retained_path_for(&original))
        });
        if keep_tmp_requested() {
            let _ = std::fs::remove_dir_all(&original);
            return;
        }
        assert!(retained.is_dir(), "renamed by the post-guard panic");
        std::fs::remove_dir_all(&retained).expect("clean up retained dir");
    }

    #[test]
    fn real_failure_inside_suppression_window_still_retains() {
        let (tx, rx) = std::sync::mpsc::channel();
        let outcome = std::thread::spawn(move || {
            let dir = test_tempdir("itd-retain-");
            let original = dir.path().to_path_buf();
            tx.send((original.clone(), retained_path_for(&original)))
                .expect("send paths");
            let _suppress = suppress_failure_retention();
            caught_panic();
            assert!(original.is_dir(), "caught panic under the guard: untouched");
            panic!("intentional: uncaught failure while suppressed");
        })
        .join();
        assert!(outcome.is_err(), "thread must have failed");
        let (original, retained) = rx.recv().expect("paths");
        if keep_tmp_requested() {
            let _ = std::fs::remove_dir_all(&original);
            return;
        }
        assert!(!original.exists(), "original swept after retention");
        assert!(
            retained.is_dir(),
            "guard dropped during unwinding retained {}",
            retained.display()
        );
        std::fs::remove_dir_all(&retained).expect("clean up retained dir");
    }

    #[test]
    fn sibling_thread_panic_does_not_retain_my_dir() {
        let (original, retained) = on_fresh_thread(|| {
            let dir = test_tempdir("itd-retain-");
            let original = dir.path().to_path_buf();
            on_fresh_thread(caught_panic);
            assert!(
                original.is_dir(),
                "another thread's panic leaves my dir alone"
            );
            (original.clone(), retained_path_for(&original))
        });
        if keep_tmp_requested() {
            let _ = std::fs::remove_dir_all(&original);
        }
        assert!(
            !retained.exists(),
            "retention is scoped to the panicking thread"
        );
    }
}
