//! Shared test utilities for intentd integration tests.
//!
//! This module provides RAII guards for spawned daemon processes to prevent
//! process leaks when tests panic or fail to clean up explicitly, plus
//! multiplier-aware timeout helpers so budgets are centrally tunable.

// Each integration test binary compiles this module independently and only
// uses a subset of it, so unused items are expected.
#![allow(dead_code)]

#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::sync::Arc;
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
pub fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test tempdir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
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
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
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
    let logs = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => return format!("\n--- daemon log ({}) unreadable: {e} ---", path.display()),
    };
    let lines: Vec<&str> = logs.lines().collect();
    let skipped = lines.len().saturating_sub(LOG_TAIL_LINES);
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

/// Enable the WSS/TCP listener for a daemon booted from `data_dir` by seeding
/// `config.toml` with `[server.wsApi] enabled = true` plus an OS-assigned free
/// port (the config-driven replacement for the retired `serve --listen both`
/// flag: UDS always serves; the WSS listener boot-starts iff the effective
/// `server.wsApi.enabled` is true, binding `server.wsApi.port`).
///
/// Port interplay with the `INTENTD_TCP_PORT=0` seam (monorepo#1051): suites
/// that spawn the daemon with `INTENTD_TCP_PORT=0` get a true OS-assigned
/// ephemeral bind — the seam wins over the seeded settings port, eliminating
/// the reserve-then-release TOCTOU race where another process grabbed the
/// seeded port between this helper releasing it and the daemon binding it
/// after full boot. The seeded port is only a fallback for spawns without the
/// seam, keeping them off the fixed 5181 default that would collide across
/// parallel daemons; such suites (and any daemon restarted on the same data
/// dir with the seam, whose ephemeral port changes across boots) must read
/// the real port from `system.status` ([`await_wss_status`]), never from the
/// seeded config value. Appends to an existing seeded config; no-op if the
/// table is already present.
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
    text.push_str(&format!(
        "\n[server.wsApi]\nenabled = true\nport = {port}\n"
    ));
    std::fs::write(&path, text).expect("seed config.toml with server.wsApi.enabled");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn guard_kills_process_on_drop() {
        // Spawn a sleep process
        let child = Command::new("sleep")
            .arg("3600")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        {
            let _guard = DaemonGuard::process_only(child);
            // Guard goes out of scope here
        }

        // Process should be dead
        // Check using kill -0 (send signal 0 to test if process exists)
        let status = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .expect("run kill -0");

        assert!(!status.success(), "process should be dead after guard drop");
    }
}
