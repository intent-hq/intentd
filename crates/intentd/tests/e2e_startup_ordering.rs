//! E2e for startup ordering (monorepo#1581): the UDS control listener must
//! bind — and `system.status` answer — without waiting on the slow
//! initializations that follow it (MCP server startup, filesystem watcher
//! registration). On macOS each `FSEventStreamStart` is a synchronous IPC to
//! `fseventsd` that can take seconds, which used to delay the bind past the FE
//! sidecar's probe window and get the daemon killed as unresponsive.
//!
//! Drives the real `intentd serve` binary with the
//! `INTENTD_TEST_WATCHER_INIT_DELAY_MS` seam standing in for a wedged
//! `fseventsd`: readiness must land while watcher init is still parked in that
//! delay, proving it is off the bind critical path. The seam sleeps the *thread*
//! (not `tokio::time::sleep`), matching the synchronous `fseventsd` IPC it stands
//! in for, so the test also fails if init merely moves to another Tokio worker
//! without `block_in_place`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use uuid::Uuid;

/// Artificial watcher-init delay: far longer than any plausible daemon boot,
/// so a daemon that still binds behind watcher init cannot pass by being fast.
const WATCHER_DELAY: Duration = Duration::from_secs(120);

/// Layout for the spawned daemon: a short base dir (macOS caps UDS paths at
/// ~104 bytes) holding the data dir and a hermetic workspaces root.
struct TestDirs {
    base: PathBuf,
    data_dir: PathBuf,
    workspaces: PathBuf,
}

fn make_dirs() -> TestDirs {
    let id = Uuid::new_v4().simple().to_string();
    let base = PathBuf::from("/tmp").join(format!("itd-so-{}", &id[..8]));
    let data_dir = base.join("data");
    let workspaces = base.join("workspaces");
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    std::fs::create_dir_all(&workspaces).expect("mkdir workspaces root");
    TestDirs {
        base,
        data_dir,
        workspaces,
    }
}

fn spawn_daemon(dirs: &TestDirs) -> Child {
    let log = std::fs::File::create(dirs.data_dir.join("daemon.log")).expect("create daemon log");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", &dirs.data_dir)
        .env("INTENTD_WORKSPACES_DIR", &dirs.workspaces)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_TCP_PORT", "0")
        .env(
            "INTENTD_TEST_WATCHER_INIT_DELAY_MS",
            WATCHER_DELAY.as_millis().to_string(),
        )
        // One Tokio worker: the harshest version of the saturated-runtime case.
        // A blocking init that is merely `spawn`ed lands on the same worker that
        // must reach the UDS bind, so this fails without `block_in_place`.
        .env("TOKIO_WORKER_THREADS", "1")
        .env_remove("INTENTD_AUTH_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
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

    let started = Instant::now();
    let mut daemon = common::DaemonGuard::new(spawn_daemon(&dirs), dirs.base.clone(), true);
    common::await_daemon_listening(daemon.child_mut(), &socket, &log_path).await;

    // `system.status` must answer over the freshly bound socket, not merely
    // accept the connection — the FE probe requires a real response.
    let resp = status_rpc(&socket).await;
    let elapsed = started.elapsed();
    assert!(
        resp["result"].is_object(),
        "system.status must succeed while watcher init is still delayed; got {resp}"
    );

    // Readiness budget: generously scaled for instrumented/oversubscribed
    // runs, but well inside the artificial watcher delay, so passing proves
    // the bind did not wait on watcher registration.
    let budget = common::test_timeout(Duration::from_secs(20));
    assert!(
        budget < WATCHER_DELAY,
        "readiness budget {budget:?} must stay inside the watcher delay {WATCHER_DELAY:?}"
    );
    assert!(
        elapsed < budget,
        "daemon answered system.status only after {elapsed:?} (budget {budget:?}) with a \
         {WATCHER_DELAY:?} watcher-init delay in effect — the listeners are still behind \
         slow initialization\n--- daemon log ---\n{}",
        std::fs::read_to_string(&log_path).unwrap_or_default()
    );
}
