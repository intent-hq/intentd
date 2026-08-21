//! Regression guard: workspace provisioning must never silently target
//! `$HOME/intent/workspaces` from the test harness.
//!
//! Spawns the REAL `intentd serve` binary with a fake `$HOME` and
//! `INTENTD_ASSERT_HERMETIC_ROOT=1` (the posture every test spawn helper uses)
//! and proves both directions of the guard:
//!
//! - WITHOUT `INTENTD_WORKSPACES_DIR`, a `workspace.create` that reaches the
//!   `$HOME` fallback refuses loudly (hermetic-root panic) instead of writing
//!   under the fake `$HOME/intent/workspaces`, and persists no row.
//! - WITH `INTENTD_WORKSPACES_DIR`, the same create succeeds and provisions
//!   strictly under the injected temp root, leaving `$HOME` untouched.
//!
//! All environment is set at spawn time only — no in-process `set_var`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

/// Layout for one spawned daemon: a short base dir (macOS caps UDS paths at
/// ~104 bytes) holding the data dir and a fake `$HOME`.
struct TestDirs {
    base: PathBuf,
    data_dir: PathBuf,
    home: PathBuf,
}

fn make_dirs() -> TestDirs {
    let id = Uuid::new_v4().simple().to_string();
    let base = PathBuf::from("/tmp").join(format!("itdh-{}", &id[..8]));
    let data_dir = base.join("data");
    let home = base.join("home");
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    std::fs::create_dir_all(&home).expect("mkdir fake home");
    TestDirs {
        base,
        data_dir,
        home,
    }
}

/// Spawn `intentd serve` with a fake `$HOME` and the hermetic-root assertion
/// armed. `workspaces_dir: None` reproduces the unguarded-harness regression
/// this suite protects against: no `INTENTD_WORKSPACES_DIR`, so resolving the
/// default workspaces root must refuse instead of falling back to `$HOME`.
fn spawn_daemon(dirs: &TestDirs, workspaces_dir: Option<&Path>) -> Child {
    let log = std::fs::File::create(dirs.data_dir.join("daemon.log")).expect("create daemon log");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("HOME", &dirs.home)
        .env("INTENTD_DATA_DIR", &dirs.data_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_TCP_PORT", "0")
        .env_remove("INTENTD_AUTH_TOKEN")
        .env_remove("INTENTD_WORKSPACES_DIR")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    if let Some(dir) = workspaces_dir {
        cmd.env("INTENTD_WORKSPACES_DIR", dir);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_socket(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// Send one JSON-RPC frame; `Some(response)` or `None` when no response
/// arrives within `read_timeout`. The guarded create never answers: the
/// handler task panics and tokio reaps it before a frame is written back.
async fn rpc(socket: &Path, frame: Value, read_timeout: Duration) -> Option<Value> {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(&frame).unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    match timeout(read_timeout, reader.read_line(&mut buf)).await {
        Ok(Ok(n)) if n > 0 => Some(serde_json::from_str(buf.trim_end()).expect("valid JSON frame")),
        _ => None,
    }
}

/// Poll the daemon's stderr log until it contains `marker` (panic-hook output
/// lands there), bounded by the shared startup budget.
async fn wait_for_log_marker(log: &Path, marker: &str) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if std::fs::read_to_string(log).is_ok_and(|s| s.contains(marker)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn workspace_create_without_workspaces_dir_refuses_home_fallback() {
    let dirs = make_dirs();
    let socket = dirs.data_dir.join("intentd.sock");
    let _daemon = common::DaemonGuard::new(spawn_daemon(&dirs, None), dirs.base.clone(), true);
    assert!(await_socket(&socket).await, "daemon did not start");

    // Drive workspace.create with NO INTENTD_WORKSPACES_DIR: resolving the
    // default root must panic (INTENTD_ASSERT_HERMETIC_ROOT), so the call
    // yields either no response (reaped handler task) or an error — never a
    // provisioned workspace.
    let resp = rpc(
        &socket,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "workspace.create",
            "params": { "title": "Leak Guard WS" }
        }),
        common::test_timeout(Duration::from_secs(10)),
    )
    .await;
    if let Some(resp) = &resp {
        assert!(
            resp["result"].is_null() && resp.get("error").is_some(),
            "workspace.create must not succeed without INTENTD_WORKSPACES_DIR: {resp}"
        );
    }

    // The refusal must be loud: the hermetic-root panic lands in the daemon's
    // stderr log. If this marker never appears, the guard (or its plumbing)
    // has been removed and the create silently resolved a real root.
    assert!(
        wait_for_log_marker(&dirs.data_dir.join("daemon.log"), "hermetic-tests").await,
        "expected the hermetic-root panic in daemon.log; workspace.create \
         resolved a workspaces root despite INTENTD_ASSERT_HERMETIC_ROOT with \
         no INTENTD_WORKSPACES_DIR"
    );

    // Nothing may have been provisioned under the fake $HOME fallback.
    let leak = dirs.home.join("intent").join("workspaces");
    assert!(
        !leak.exists(),
        "guard bypassed: daemon provisioned under fake $HOME at {}",
        leak.display()
    );

    // And the refused create must not have persisted a workspace row: the
    // daemon survives the reaped handler task, so list it directly.
    let list = rpc(
        &socket,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "workspace.list", "params": {} }),
        common::test_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("daemon should still answer workspace.list after the refused create");
    let workspaces = list["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(
        workspaces
            .iter()
            .all(|w| w["title"] != json!("Leak Guard WS")),
        "refused workspace.create must not persist a row: {list}"
    );
}

#[tokio::test]
async fn workspace_create_with_workspaces_dir_provisions_under_temp_root() {
    let dirs = make_dirs();
    let workspaces_dir = dirs.base.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let socket = dirs.data_dir.join("intentd.sock");
    let _daemon = common::DaemonGuard::new(
        spawn_daemon(&dirs, Some(&workspaces_dir)),
        dirs.base.clone(),
        true,
    );
    assert!(await_socket(&socket).await, "daemon did not start");

    // Happy path: with INTENTD_WORKSPACES_DIR set the same create succeeds…
    let resp = rpc(
        &socket,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "workspace.create",
            "params": { "title": "Hermetic Root WS" }
        }),
        common::test_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("workspace.create response");
    let id = resp["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("created workspace id: {resp}"));

    // …and provisions strictly under the injected temp root (the metadata
    // file is written synchronously before the response).
    let meta = workspaces_dir
        .join(id)
        .join(".workspace")
        .join("workspace.json");
    assert!(
        meta.exists(),
        "workspace.create must provision under INTENTD_WORKSPACES_DIR: missing {}",
        meta.display()
    );
    let leak = dirs.home.join("intent").join("workspaces");
    assert!(
        !leak.exists(),
        "workspace.create leaked under fake $HOME at {}",
        leak.display()
    );
}
