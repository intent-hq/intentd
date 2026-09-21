//! Daemon side of the sitter's staged-restart handshake, driven against the
//! REAL `intentd serve` process: a supervised daemon (the sitter advertises
//! the handshake via `INTENTD_SITTER_IDLE_RESTART`) that receives SIGUSR2
//! ("a newer version is staged") exits with the restart-for-update code —
//! immediately when idle, and only once the in-flight turn ends when busy —
//! a SIGTERM while that restart is still pending exits 0, a SIGUSR2 landing
//! while a requested stop (SIGTERM or `system.shutdown`) is still tearing
//! down is ignored (the teardown is held open deterministically by a blocked
//! fake `tailcat genkey`), and an unsupervised daemon ignores the signal.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use intentd_test_support::{Barrier, GuardedChild};
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;

/// Mirrors `RESTART_FOR_UPDATE_EXIT_CODE` in the daemon and
/// `intentd_sitter::supervisor::RESTART_FOR_UPDATE_EXIT_CODE`.
const RESTART_FOR_UPDATE_EXIT_CODE: i32 = 75;

/// Env var the sitter sets to advertise the idle-restart handshake.
const SITTER_IDLE_RESTART_ENV: &str = "INTENTD_SITTER_IDLE_RESTART";

/// Env var (inherited by the daemon, hence by the fake tailcat it spawns)
/// naming the per-test release file ([`Barrier::path`]) the held fake
/// `genkey` blocks on.
const FAKE_TAILCAT_RELEASE_ENV: &str = "FAKE_TAILCAT_RELEASE";

/// Fixed WSS bearer token (test-only `INTENTD_AUTH_TOKEN` seam) for the
/// held-teardown launches, which need the WSS listener up so the tunnel can
/// be enabled at runtime.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// The daemon under test; `child` kills the whole process group (daemon plus
/// any parked fake sidecar) on drop, including on a mid-test panic.
struct Daemon {
    child: GuardedChild,
    log_path: PathBuf,
}

impl Daemon {
    fn signal(&self, sig: Signal) {
        self.child.signal(sig).expect("signal daemon");
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Poll the log for `needle` within `budget`; panics (with the log) if it
    /// never appears.
    async fn wait_for_log(&self, needle: &str, budget: Duration) {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            if self.log().contains(needle) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "daemon log never contained {needle:?}\n--- daemon log ---\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Poll for process exit within `budget`; `None` if still running.
    async fn wait_exit(&mut self, budget: Duration) -> Option<ExitStatus> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return Some(status);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn mock_agent_script() -> Option<String> {
    let script = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    if intent_providers::resolve_on_path("node").is_none() || !Path::new(&script).exists() {
        eprintln!("skipping sitter staged-restart e2e: mock agent unavailable");
        return None;
    }
    Some(script)
}

/// Launch the REAL daemon; `advertised` sets the sitter's handshake marker.
async fn launch_daemon(data_dir: &Path, script: &str, advertised: bool) -> (Daemon, PathBuf) {
    launch_daemon_with(data_dir, script, advertised, &[]).await
}

/// [`launch_daemon`] with extra environment for the daemon (inherited by
/// every child it spawns, including the tailcat sidecar).
async fn launch_daemon_with(
    data_dir: &Path,
    script: &str,
    advertised: bool,
    extra_env: &[(&str, &str)],
) -> (Daemon, PathBuf) {
    let log_path = data_dir.join("daemon.log");
    let log = std::fs::File::create(&log_path).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir workspaces dir");
    let behavior = json!({ "blockUntilCancel": true, "response": "parked" }).to_string();
    let mut command = common::serve_command();
    command
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("MOCK_AGENT_SCRIPT_PATH", script)
        .env("MOCK_AGENT_BEHAVIOR", behavior)
        .env("RUST_LOG", "info")
        .env_remove(SITTER_IDLE_RESTART_ENV)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    if advertised {
        command.env(SITTER_IDLE_RESTART_ENV, "1");
    }
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let child = GuardedChild::spawn(&mut command).expect("spawn intentd serve");
    let mut daemon = Daemon { child, log_path };
    let socket = data_dir.join("intentd.sock");
    common::await_daemon_listening(&mut daemon.child, &socket, &daemon.log_path).await;
    (daemon, socket)
}

/// Write the HELD fake tailcat into `dir`: `genkey` records its pid in
/// `<release>.pid` (see [`HeldGenkey::pid`]), touches `<release>.entered`
/// (proof the daemon's tunnel mutex is now held across the blocked
/// `ensure_key`; see [`Barrier::entered`]), blocks until the release file
/// named by [`FAKE_TAILCAT_RELEASE_ENV`] exists (the same wait loop as
/// [`Barrier::sh_wait`], reading the path from the environment), then writes
/// the key; `serve` behaves like the other fake tailcats (prints the JSON
/// address, sleeps).
fn write_held_fake_tailcat(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("fake-tailcat-held.sh");
    let release = format!("${{{FAKE_TAILCAT_RELEASE_ENV}}}");
    let script = format!(
        r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${{arg#--key=}}" ;;
  esac
done
case "$1" in
  genkey)
    echo $$ > "{release}.pid"
    : > "{release}.entered"
    while [ ! -e "{release}" ]; do sleep 0.05; done
    printf 'key-%s' $$ > "$key"
    ;;
  serve)
    printf '{{"listenAddr":"tc-%s"}}\n' "$(cat "$key")"
    sleep 600
    ;;
esac
"#
    );
    std::fs::write(&path, script).expect("write held fake tailcat");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod held fake tailcat");
    path
}

/// The held fake `genkey`'s barrier plus its release-on-unwind fallback.
///
/// `GuardedChild` (correctly) skips the group kill once the daemon has been
/// reaped, so a test that reaps its daemon and then panics would leave the
/// fake `genkey` parked on the barrier forever. Dropping this guard releases
/// the barrier and waits, bounded, for the recorded `genkey` pid to be gone,
/// so the sidecar cannot race the `TempDir` removal. Declare it after the
/// `TempDir` *and* after a [`common::suppress_failure_retention`] guard, and
/// before the [`Daemon`]: locals drop in reverse order, so the daemon goes
/// first, then this guard, then the retention guard, then the directory.
///
/// The retention guard is load-bearing, not a courtesy: without it the
/// failure-retention panic hook renames the data dir to `failed-*` *before*
/// unwinding starts, so this drop would write the release to a path that no
/// longer exists and find no pid — the parked `genkey` would leak. Under the
/// guard the hook stands down and the retention (rename + diagnostics) runs
/// from the guard's own drop, i.e. after this release has completed.
struct HeldGenkey {
    barrier: Barrier,
    pid_path: PathBuf,
}

impl HeldGenkey {
    fn new(data_dir: &Path) -> Self {
        let barrier = Barrier::new(data_dir, "genkey");
        let mut pid_path = barrier.path().as_os_str().to_owned();
        pid_path.push(".pid");
        Self {
            barrier,
            pid_path: pid_path.into(),
        }
    }

    fn barrier(&self) -> &Barrier {
        &self.barrier
    }

    /// The pid the fake `genkey` recorded on arrival, once it has.
    fn pid(&self) -> Option<u32> {
        std::fs::read_to_string(&self.pid_path)
            .ok()?
            .trim()
            .parse()
            .ok()
    }
}

impl Drop for HeldGenkey {
    fn drop(&mut self) {
        let _ = std::fs::write(self.barrier.path(), b"");
        let Some(pid) = self.pid() else {
            return;
        };
        if !wait_for_process_exit(pid, common::test_timeout(Duration::from_secs(5))) {
            eprintln!("fake tailcat genkey {pid} still alive after the barrier release");
        }
    }
}

/// Whether `pid` still names a process (signal 0 probe), zombies included.
fn process_alive(pid: u32) -> bool {
    match kill(Pid::from_raw(pid.cast_signed()), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(e) => panic!("kill({pid}, 0): {e}"),
    }
}

/// Poll `kill(pid, 0)` until it reports `ESRCH` or `budget` elapses; whether
/// the process left in time.
fn wait_for_process_exit(pid: u32, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while process_alive(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        // timing-guard: poll interval
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

/// Poll for the fake `genkey` to reach `barrier` within `budget`; panics
/// (with the daemon log) if it never does.
async fn wait_for_entered(barrier: &Barrier, budget: Duration, daemon: &Daemon) {
    let deadline = tokio::time::Instant::now() + budget;
    while !barrier.entered() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "fake tailcat genkey never reached {}\n--- daemon log ---\n{}",
            barrier.path().display(),
            daemon.log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Boot a supervised daemon with the WSS listener up and a HELD fake tailcat
/// in `data_dir`, enable the tunnel at runtime (the RPC parks inside
/// `start_tunnel` holding the tunnel mutex, blocked on the fake `genkey`),
/// and wait for the `genkey` to reach `genkey`'s barrier. The returned
/// [`Client`] is the never-awaited tunnel connection; keep it alive for as
/// long as the hold must last.
async fn launch_held_teardown(
    data_dir: &Path,
    script: &str,
    genkey: &HeldGenkey,
) -> (Daemon, PathBuf, Client) {
    let tailcat = write_held_fake_tailcat(data_dir);
    common::enable_ws_api(data_dir);
    let tailcat_s = tailcat.to_string_lossy().to_string();
    let release_s = genkey.barrier().path().to_string_lossy().to_string();
    // `common::serve_command` binds an OS-assigned port, overriding the fixed
    // port `enable_ws_api` reserved and released, so a concurrent test cannot
    // claim it first. Nothing here needs the actual port number.
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TAILCAT_BIN", &tailcat_s),
        (FAKE_TAILCAT_RELEASE_ENV, &release_s),
    ];
    let (daemon, socket) = launch_daemon_with(data_dir, script, true, &env).await;
    // `start_tunnel` requires the WSS listener to be up.
    let status = common::await_wss_status_logged(&socket, &daemon.log_path).await;
    assert!(status["result"]["port"].as_u64().is_some(), "{status}");

    // Enable the tunnel on a dedicated connection whose response is never
    // awaited: the hook is parked in `start_tunnel` → `ensure_key` → the
    // held fake `genkey`, holding the tunnel mutex until released.
    let mut tunnel_client = Client::connect(&socket).await;
    tunnel_client
        .send(
            1,
            "settings.update",
            json!({ "changes": [{ "path": "server.tunnel.enabled", "value": true }] }),
        )
        .await;
    wait_for_entered(genkey.barrier(), exit_budget(), &daemon).await;
    (daemon, socket, tunnel_client)
}

/// How the requested stop that must win the shutdown race is triggered.
#[derive(Clone, Copy)]
enum StopTrigger {
    Sigterm,
    SystemShutdown,
}

/// Held-teardown scenario shared by the two late-SIGUSR2 tests: boot a
/// supervised daemon with the WSS listener up and a HELD fake tailcat, enable
/// the tunnel at runtime (the RPC parks inside `start_tunnel` holding the
/// tunnel mutex, blocked on the fake `genkey`), request a stop, prove the
/// daemon is in teardown and still alive, send SIGUSR2, prove it neither
/// exits nor accepts a staged restart, release the barrier, and assert a
/// clean exit 0 with no staged-restart exit.
async fn sigusr2_during_held_teardown(trigger: StopTrigger) {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir("itd-sr-");
    let data_dir = data_dir_guard.path();
    // Drop order on unwind is `daemon` → `genkey` → `_retain` → the TempDir:
    // the daemon (and the fake `genkey` parked in its process group) is torn
    // down first; `genkey` then releases whatever is still waiting on the
    // barrier while the data dir is still at its original path; only then
    // does `_retain` rename it to `failed-*` for post-mortem.
    let _retain = common::suppress_failure_retention();
    let genkey = HeldGenkey::new(data_dir);
    let (mut daemon, socket, _tunnel_client) =
        launch_held_teardown(data_dir, &script, &genkey).await;

    match trigger {
        StopTrigger::Sigterm => daemon.signal(Signal::SIGTERM),
        StopTrigger::SystemShutdown => {
            let mut client = Client::connect(&socket).await;
            let stopped = client.rpc(1, "system.shutdown", json!({})).await;
            assert_eq!(stopped["stopping"], json!(true), "{stopped}");
        }
    }
    daemon
        .wait_for_log("shutdown cause latched: requested stop", exit_budget())
        .await;
    daemon
        .wait_for_log("intentd UDS listener stopped", exit_budget())
        .await;
    // The stop has won and the daemon is in teardown, parked on
    // `stop_tunnel` behind the held mutex — provably still alive.
    assert!(
        daemon.child.try_wait().expect("try_wait").is_none(),
        "daemon exited before the barrier was released\n--- daemon log ---\n{}",
        daemon.log()
    );

    daemon.signal(Signal::SIGUSR2);
    assert!(
        daemon.wait_exit(stay_alive_window()).await.is_none(),
        "daemon must stay parked in teardown after SIGUSR2\n--- daemon log ---\n{}",
        daemon.log()
    );
    let log = daemon.log();
    assert!(!log.contains("staged update restart accepted"), "{log}");

    genkey.barrier().release();
    let status = daemon.wait_exit(exit_budget()).await.unwrap_or_else(|| {
        panic!(
            "daemon did not exit after the barrier was released\n--- daemon log ---\n{}",
            daemon.log()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "exit status {status}\n--- daemon log ---\n{}",
        daemon.log()
    );
    let log = daemon.log();
    assert!(!log.contains("staged update restart accepted"), "{log}");
    assert!(!log.contains("exiting for staged update restart"), "{log}");
}

struct Client {
    write: tokio::net::unix::OwnedWriteHalf,
    read: BufReader<OwnedReadHalf>,
}

impl Client {
    async fn connect(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).await.expect("connect UDS");
        let (read, write) = stream.into_split();
        Self {
            write,
            read: BufReader::new(read),
        }
    }

    /// Write one request frame without awaiting its response.
    async fn send(&mut self, id: i64, method: &str, params: Value) {
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let line = serde_json::to_string(&frame).unwrap();
        self.write.write_all(line.as_bytes()).await.unwrap();
        self.write.write_all(b"\n").await.unwrap();
        self.write.flush().await.unwrap();
    }

    async fn rpc(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.send(id, method, params).await;
        loop {
            let mut buf = String::new();
            let n = tokio::time::timeout(common::rpc_read_timeout(), self.read.read_line(&mut buf))
                .await
                .expect("RPC timed out")
                .expect("read failed");
            assert!(n > 0, "connection closed while awaiting {method}");
            let value: Value = serde_json::from_str(buf.trim_end()).expect("JSON frame");
            if value["id"] == json!(id) {
                assert!(
                    value.get("error").is_none(),
                    "rpc {method} errored: {value}"
                );
                return value["result"].clone();
            }
        }
    }
}

async fn seed_workspace(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let id = WorkspaceId::new();
    let timestamp = now_iso();
    let workspace = Workspace {
        id: id.clone(),
        title: "Staged Restart E2E".into(),
        branch: "main".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: timestamp.clone(),
        updated_at: timestamp,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    };
    store
        .insert_workspace(&workspace)
        .await
        .expect("insert workspace");
    id.0
}

/// Window in which a daemon that must NOT exit is observed staying alive.
fn stay_alive_window() -> Duration {
    common::test_timeout(Duration::from_secs(3))
}

/// Budget for a daemon's graceful exit after the trigger fires.
fn exit_budget() -> Duration {
    common::test_timeout(Duration::from_secs(30))
}

/// An idle supervised daemon answers SIGUSR2 by exiting right away with the
/// restart-for-update code.
#[tokio::test]
async fn idle_daemon_exits_with_restart_code_on_sigusr2() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir("itd-sr-");
    let (mut daemon, _socket) = launch_daemon(data_dir_guard.path(), &script, true).await;

    daemon.signal(Signal::SIGUSR2);
    let status = daemon.wait_exit(exit_budget()).await.unwrap_or_else(|| {
        panic!(
            "idle daemon did not exit\n--- daemon log ---\n{}",
            daemon.log()
        )
    });
    assert_eq!(
        status.code(),
        Some(RESTART_FOR_UPDATE_EXIT_CODE),
        "exit status {status}\n--- daemon log ---\n{}",
        daemon.log()
    );
    let log = daemon.log();
    assert!(log.contains("staged update restart accepted"), "{log}");
    assert!(log.contains("exiting for staged update restart"), "{log}");
}

/// A supervised daemon with a turn in flight accepts SIGUSR2 but keeps
/// serving until the turn ends, then exits with the restart code.
#[tokio::test]
async fn busy_daemon_defers_restart_until_turn_ends() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir("itd-sr-");
    let workspace_id = seed_workspace(data_dir_guard.path()).await;
    let (mut daemon, socket) = launch_daemon(data_dir_guard.path(), &script, true).await;
    let mut client = Client::connect(&socket).await;

    let created = client
        .rpc(
            1,
            "agent.create",
            json!({ "workspaceId": workspace_id, "name": "Busy", "model": "default", "provider": "mock" }),
        )
        .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = client
        .rpc(
            2,
            "agent.sendMessage",
            json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "park" }),
        )
        .await;
    assert_eq!(sent["success"], true, "send response: {sent}");
    let active = client.rpc(3, "agent.listActive", json!({})).await;
    assert_eq!(
        active["streams"].as_array().map(Vec::len),
        Some(1),
        "{active}"
    );

    daemon.signal(Signal::SIGUSR2);
    assert!(
        daemon.wait_exit(stay_alive_window()).await.is_none(),
        "busy daemon must not exit while a turn is in flight\n--- daemon log ---\n{}",
        daemon.log()
    );
    // Still serving: the restart was accepted, not applied.
    let status = client.rpc(4, "system.status", json!({})).await;
    assert!(status.is_object(), "{status}");
    let log = daemon.log();
    assert!(log.contains("staged update restart accepted"), "{log}");
    assert!(!log.contains("exiting for staged update restart"), "{log}");

    let stopped = client
        .rpc(5, "agent.stop", json!({ "agentId": agent_id }))
        .await;
    assert_eq!(stopped, json!({ "success": true }));
    let status = daemon.wait_exit(exit_budget()).await.unwrap_or_else(|| {
        panic!(
            "daemon did not exit after the turn ended\n--- daemon log ---\n{}",
            daemon.log()
        )
    });
    assert_eq!(
        status.code(),
        Some(RESTART_FOR_UPDATE_EXIT_CODE),
        "exit status {status}\n--- daemon log ---\n{}",
        daemon.log()
    );
}

/// A pending staged restart (SIGUSR2 accepted while busy) does not hijack an
/// unrelated shutdown: SIGTERM while the turn is still in flight exits 0, not
/// the restart code — the exit code is keyed on the exit-when-idle actually
/// firing, not on the restart being pending.
#[tokio::test]
async fn sigterm_during_pending_restart_exits_cleanly() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir("itd-sr-");
    let workspace_id = seed_workspace(data_dir_guard.path()).await;
    let (mut daemon, socket) = launch_daemon(data_dir_guard.path(), &script, true).await;
    let mut client = Client::connect(&socket).await;

    let created = client
        .rpc(
            1,
            "agent.create",
            json!({ "workspaceId": workspace_id, "name": "Busy", "model": "default", "provider": "mock" }),
        )
        .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = client
        .rpc(
            2,
            "agent.sendMessage",
            json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "park" }),
        )
        .await;
    assert_eq!(sent["success"], true, "send response: {sent}");
    let active = client.rpc(3, "agent.listActive", json!({})).await;
    assert_eq!(
        active["streams"].as_array().map(Vec::len),
        Some(1),
        "{active}"
    );

    daemon.signal(Signal::SIGUSR2);
    assert!(
        daemon.wait_exit(stay_alive_window()).await.is_none(),
        "busy daemon must not exit while a turn is in flight\n--- daemon log ---\n{}",
        daemon.log()
    );
    let log = daemon.log();
    assert!(log.contains("staged update restart accepted"), "{log}");

    // SIGTERM while the restart is pending and the turn still in flight.
    daemon.signal(Signal::SIGTERM);
    let status = daemon.wait_exit(exit_budget()).await.unwrap_or_else(|| {
        panic!(
            "daemon did not exit on SIGTERM\n--- daemon log ---\n{}",
            daemon.log()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "exit status {status}\n--- daemon log ---\n{}",
        daemon.log()
    );
    let log = daemon.log();
    assert!(!log.contains("exiting for staged update restart"), "{log}");
}

/// A requested stop that has already won is never hijacked by a staged
/// restart landing during teardown: once SIGTERM has latched the shutdown
/// cause and the daemon is provably still tearing down (held open by the
/// blocked fake `tailcat genkey`, idle, so the exit-when-idle would otherwise
/// fire at once), a SIGUSR2 is ignored and the daemon exits 0 — the sitter
/// must not respawn a daemon the user stopped.
#[tokio::test]
async fn sigusr2_during_held_teardown_after_sigterm_does_not_hijack_the_exit_code() {
    sigusr2_during_held_teardown(StopTrigger::Sigterm).await;
}

/// Same as the SIGTERM case, with the requested stop coming from the
/// `system.shutdown` RPC over UDS instead.
#[tokio::test]
async fn sigusr2_during_held_teardown_after_system_shutdown_does_not_hijack_the_exit_code() {
    sigusr2_during_held_teardown(StopTrigger::SystemShutdown).await;
}

/// A held-teardown test that reaps its daemon and then PANICS must not leave
/// the fake `genkey` parked behind the barrier: `GuardedChild` skips the
/// group kill on a reaped pid, so the release is on [`HeldGenkey`], which
/// also waits for the sidecar to be gone before the `TempDir` is removed.
///
/// The panic is real and caught here, with every guard created inside the
/// catch boundary and failure-time tempdir retention at its default, so the
/// same panic hook a genuine assertion failure would trigger is what runs:
/// it renames the data dir to `failed-*` before unwinding unless the test
/// body holds the retention guard, in which case the release lands first.
/// A plain `#[test]` with its own runtime, because `catch_unwind` needs the
/// panic to cross a synchronous `block_on`.
#[test]
fn reaped_daemon_still_releases_the_parked_genkey_on_panic() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    let mut observed: Option<(u32, PathBuf)> = None;

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            let data_dir_guard = common::test_tempdir("itd-sr-");
            let data_dir = data_dir_guard.path();
            let _retain = common::suppress_failure_retention();
            let genkey = HeldGenkey::new(data_dir);
            let (mut daemon, _socket, _tunnel_client) =
                launch_held_teardown(data_dir, &script, &genkey).await;
            let genkey_pid = genkey
                .pid()
                .expect("fake genkey recorded its pid on arrival");
            assert!(process_alive(genkey_pid), "fake genkey must be parked");

            // Mirror the early-exit path: SIGKILL only the daemon and reap it,
            // so the guard's group kill on drop is (correctly) a no-op.
            daemon.child.kill().expect("kill daemon");
            daemon.child.wait().expect("reap daemon");
            assert!(
                process_alive(genkey_pid),
                "fake genkey must outlive its reaped daemon until released"
            );
            observed = Some((genkey_pid, data_dir.to_path_buf()));
            panic!("intentional: unwind through the held-teardown guards");
        });
    }));
    assert!(outcome.is_err(), "the intentional panic must propagate");
    let (genkey_pid, original) = observed.expect("panicked after the daemon was reaped");

    let gone = wait_for_process_exit(genkey_pid, common::test_timeout(Duration::from_secs(5)));
    // The panic retained the data dir as `failed-*`; sweep it (and, under
    // `INTENTD_TEST_KEEP_TMP`, leave the kept original alone) so a passing
    // run leaves nothing behind before the outcome is asserted.
    let retained = common::retained_path_for(&original);
    let _ = std::fs::remove_dir_all(&retained);
    assert!(
        gone,
        "fake genkey {genkey_pid} still alive after the held-teardown guards unwound"
    );
}

/// Without the sitter's handshake marker SIGUSR2 is logged and ignored (the
/// daemon keeps serving), and a plain SIGTERM still exits cleanly.
#[tokio::test]
async fn sigusr2_is_ignored_without_the_sitter_handshake() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir("itd-sr-");
    let (mut daemon, socket) = launch_daemon(data_dir_guard.path(), &script, false).await;
    let mut client = Client::connect(&socket).await;

    daemon.signal(Signal::SIGUSR2);
    assert!(
        daemon.wait_exit(stay_alive_window()).await.is_none(),
        "unsupervised daemon must ignore SIGUSR2\n--- daemon log ---\n{}",
        daemon.log()
    );
    let status = client.rpc(1, "system.status", json!({})).await;
    assert!(status.is_object(), "{status}");
    let log = daemon.log();
    assert!(
        log.contains("did not advertise the idle-restart handshake"),
        "{log}"
    );
    assert!(!log.contains("staged update restart accepted"), "{log}");

    daemon.signal(Signal::SIGTERM);
    let status = daemon.wait_exit(exit_budget()).await.unwrap_or_else(|| {
        panic!(
            "daemon did not exit on SIGTERM\n--- daemon log ---\n{}",
            daemon.log()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "exit status {status}\n--- daemon log ---\n{}",
        daemon.log()
    );
}
