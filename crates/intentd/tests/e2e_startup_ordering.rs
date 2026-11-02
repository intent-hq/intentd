//! E2e for startup ordering (monorepo#1581): the UDS control listener must
//! bind — and `system.status` answer — without waiting on the slow
//! initializations that follow it (MCP server startup, filesystem watcher
//! registration). On macOS each `FSEventStreamStart` is a synchronous IPC to
//! `fseventsd` that can take seconds, which used to delay the bind past the FE
//! sidecar's probe window and get the daemon killed as unresponsive.
//!
//! Drives the real `intentd serve` binary with the
//! `INTENTD_TEST_WATCHER_INIT_GATE` seam standing in for a wedged `fseventsd`:
//! watcher init blocks until the test creates the gate file, which it does only
//! after `system.status` has answered, so readiness provably lands while init
//! is still parked — a bind behind init hangs into the hang guards instead of
//! passing slowly. The seam blocks the *thread* (not the task), matching the
//! synchronous `fseventsd` IPC it stands in for, so the test also fails if init
//! merely moves to another Tokio worker without `block_in_place`.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;

/// Layout for the spawned daemon: a short base dir (macOS caps UDS paths at
/// ~104 bytes) holding the data dir and a hermetic workspaces root. The base
/// guard removes the whole tree on drop; keep it alive past the daemon guard.
struct TestDirs {
    _base: tempfile::TempDir,
    data_dir: PathBuf,
    workspaces: PathBuf,
    /// Watcher init blocks until this file exists.
    watcher_gate: PathBuf,
}

fn make_dirs() -> TestDirs {
    let base = common::test_tempdir_in("/tmp", "itd-so-");
    let data_dir = base.path().join("data");
    let workspaces = base.path().join("workspaces");
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    std::fs::create_dir_all(&workspaces).expect("mkdir workspaces root");
    let watcher_gate = base.path().join("watcher-gate");
    TestDirs {
        _base: base,
        data_dir,
        workspaces,
        watcher_gate,
    }
}

fn spawn_daemon(dirs: &TestDirs) -> GuardedChild {
    let log = std::fs::File::create(dirs.data_dir.join("daemon.log")).expect("create daemon log");
    GuardedChild::spawn(
        common::hermetic_serve_command(&dirs.data_dir)
            .env("INTENTD_DATA_DIR", &dirs.data_dir)
            .env("INTENTD_WORKSPACES_DIR", &dirs.workspaces)
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env("INTENTD_TEST_WATCHER_INIT_GATE", &dirs.watcher_gate)
            // One Tokio worker: the harshest version of the saturated-runtime case.
            // A blocking init that is merely `spawn`ed lands on the same worker that
            // must reach the UDS bind, so this fails without `block_in_place`.
            .env("TOKIO_WORKER_THREADS", "1")
            .env_remove("INTENTD_AUTH_TOKEN")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log)),
    )
    .expect("spawn intentd serve")
}

/// One `system.status` round-trip over the daemon's UDS control socket.
async fn status_rpc(socket: &Path) -> serde_json::Value {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .expect("uds connect");
    let (read_half, mut write_half) = stream.into_split();
    let frame = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"system.status\",\"params\":{}}\n";
    write_half
        .write_all(frame.as_bytes())
        .await
        .expect("uds write");
    write_half.flush().await.expect("uds flush");
    let mut buf = String::new();
    tokio::time::timeout(
        common::rpc_read_timeout(),
        BufReader::new(read_half).read_line(&mut buf),
    )
    .await
    .expect("system.status response within budget")
    .expect("uds read");
    serde_json::from_str(buf.trim_end()).expect("valid JSON-RPC frame")
}

#[tokio::test]
async fn listeners_bind_before_slow_watcher_init() {
    let dirs = make_dirs();
    let socket = dirs.data_dir.join("intentd.sock");
    let log_path = dirs.data_dir.join("daemon.log");

    let mut daemon = common::DaemonGuard::process_only(spawn_daemon(&dirs));
    common::await_daemon_listening(daemon.child_mut(), &socket, &log_path).await;

    // `system.status` must answer over the freshly bound socket, not merely
    // accept the connection — the FE probe requires a real response.
    let resp = status_rpc(&socket).await;
    assert!(
        resp["result"].is_object(),
        "system.status must succeed while watcher init is still held; got {resp}"
    );
    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        log.contains("waiting for the gate file") && !log.contains("watcher registry ready"),
        "watcher init must still be parked on the gate when the daemon answers\n\
         --- daemon log ---\n{log}"
    );
    std::fs::write(&dirs.watcher_gate, "").expect("open the watcher-init gate");
}
