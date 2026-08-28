//! `system.*` control + `intentd stop` over-the-wire integration test (§5.7).
//!
//! Launches the REAL `intentd serve` daemon over UDS, drives the `system.status`
//! control RPC (asserting the live transport/host fields), then runs the real
//! `intentd stop` subcommand and asserts the daemon exits gracefully, cleans up
//! its socket + pidfile, and can be restarted cleanly (the UDS analog of the
//! §5.6 "no EADDRINUSE on restart" guarantee).

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn spawn_daemon(data_dir: &PathBuf) -> Child {
    spawn_daemon_with_env(data_dir, &[])
}

fn spawn_daemon_with_env(data_dir: &PathBuf, extra_env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_socket(socket: &PathBuf) -> bool {
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

async fn rpc(socket: &PathBuf, method: &str) -> Value {
    rpc_with_params(socket, method, json!({})).await
}

async fn rpc_with_params(socket: &PathBuf, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let mut line = serde_json::to_string(&frame).unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("status rpc timed out")
        .expect("read status response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

#[tokio::test]
async fn status_then_stop_shuts_down_and_restarts_cleanly() {
    // Keep the data dir short so `data_dir/intentd.sock` fits within SUN_LEN.
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let pidfile = data_dir.join("intentd.pid");

    let mut child = spawn_daemon(&data_dir);
    assert!(await_socket(&socket).await, "daemon did not start");

    // system.status renders from live state (§5.7): liveness, transport, caps.
    let resp = rpc(&socket, "system.status").await;
    let r = &resp["result"];
    assert_eq!(r["running"], true, "status: {resp}");
    assert_eq!(r["listenMode"], "uds");
    assert_eq!(r["transports"], json!(["uds"]));
    assert_eq!(r["host"]["locality"], "local", "UDS ⇒ local (§12.3)");
    assert!(r["host"]["os"].is_string());
    assert!(r["host"]["arch"].is_string());
    assert!(r["host"]["hasDisplay"].is_boolean());
    assert_eq!(r["agents"], 0);
    // New fields: maxAgents, version, uptimeSeconds.
    assert!(
        r["maxAgents"].as_u64().unwrap() > 0,
        "maxAgents > 0: {resp}"
    );
    assert!(r["version"].is_string(), "version is string: {resp}");
    assert!(r["uptimeSeconds"].is_u64(), "uptimeSeconds is u64: {resp}");
    // Process resource sample: cpuPercent may be 0 right after start (sysinfo
    // needs two refreshes), but memoryBytes must be live for a running daemon.
    assert!(r["cpuPercent"].is_number(), "cpuPercent is number: {resp}");
    assert!(
        r["memoryBytes"].as_u64().unwrap() > 0,
        "memoryBytes > 0: {resp}"
    );

    // host.status is the §5.14 capability probe, answered on the same UDS
    // connection with the resolved locality (UDS ⇒ local) and host fields.
    let host = rpc(&socket, "host.status").await;
    let h = &host["result"];
    assert_eq!(h["locality"], "local", "UDS host.status ⇒ local (§5.14)");
    assert!(h["os"].is_string());
    assert!(h["arch"].is_string());
    assert!(h["hostname"].is_string());
    assert!(
        !h["prettyHostname"]
            .as_str()
            .expect("prettyHostname is string")
            .is_empty(),
        "prettyHostname non-empty"
    );
    assert!(h["hasDisplay"].is_boolean());

    // `intentd stop` issues the graceful control RPC then escalates if needed.
    // Run it in a blocking thread while we concurrently reap the daemon: the
    // daemon is THIS test's child, so its post-exit zombie would otherwise keep
    // `stop`'s signal-0 liveness probe seeing it as alive (under launchd/systemd
    // the supervisor reaps it — a test-only artifact).
    let stop_data = data_dir.clone();
    let stop = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_intentd"))
            .arg("stop")
            .env("INTENTD_DATA_DIR", &stop_data)
            .output()
            .expect("run intentd stop")
    });
    let reaped = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(reaped, "daemon did not exit after stop");

    let stop = stop.await.expect("join stop task");
    assert!(
        stop.status.success(),
        "stop exited non-zero: {}",
        String::from_utf8_lossy(&stop.stderr)
    );

    // Graceful shutdown cleans the socket + pidfile.
    assert!(!socket.exists(), "socket not cleaned after stop");
    assert!(!pidfile.exists(), "pidfile not cleaned after stop");

    // Restart on the same data dir must succeed (no stale-owner refusal): the
    // UDS analog of a clean port release with no EADDRINUSE.
    let restart = Daemon {
        child: spawn_daemon(&data_dir),
        data_dir: data_dir.clone(),
    };
    assert!(
        await_socket(&socket).await,
        "daemon did not restart cleanly after stop"
    );
    let resp = rpc(&socket, "system.status").await;
    assert_eq!(resp["result"]["running"], true, "restart status: {resp}");
    drop(restart);
}

/// Poll `system.status` until `pred` accepts the `fileWatch` object (absent ⇒
/// `Value::Null` is passed), or the startup budget elapses; returns the last
/// full response either way for the caller's assertion message.
async fn await_file_watch(socket: &PathBuf, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + common::daemon_startup_timeout();
    loop {
        let resp = rpc(socket, "system.status").await;
        if pred(&resp["result"]["fileWatch"]) || tokio::time::Instant::now() >= deadline {
            return resp;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Watch-coverage health over the wire (intent-hq/intent#3708): `fileWatch`
/// is absent until the backgrounded watcher registry attaches, reads healthy
/// (`failedRoots: 0`) on a daemon whose watches register cleanly, and reports
/// `failedRoots > 0` when watcher creation fails (the test seam stands in for
/// real inotify-instance exhaustion, which needs a loaded host to reproduce).
#[tokio::test]
async fn system_status_surfaces_file_watch_coverage_and_degradation() {
    // Healthy daemon: once the registry is up, fileWatch is present with a
    // zero failed count (the hermetic boot has no workspaces, so zero roots).
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdw-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let healthy = Daemon {
        child: spawn_daemon(&data_dir),
        data_dir: data_dir.clone(),
    };
    assert!(await_socket(&socket).await, "healthy daemon did not start");
    let resp = await_file_watch(&socket, Value::is_object).await;
    let fw = &resp["result"]["fileWatch"];
    assert!(
        fw.is_object(),
        "fileWatch must appear once the registry attaches: {resp}"
    );
    assert_eq!(fw["failedRoots"], 0, "clean boot must be healthy: {resp}");
    assert!(fw["activeStreams"].is_u64(), "activeStreams: {resp}");
    assert!(fw["totalRoots"].is_u64(), "totalRoots: {resp}");
    drop(healthy);

    // Degraded daemon: every watcher creation fails (test seam), so watching
    // a workspace must surface as failed roots rather than a silent WARN.
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdd-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let degraded = Daemon {
        child: spawn_daemon_with_env(&data_dir, &[("INTENTD_TEST_FAIL_WATCHER_CREATION", "1")]),
        data_dir: data_dir.clone(),
    };
    assert!(await_socket(&socket).await, "degraded daemon did not start");

    // A workspace over an existing plain directory: `workspace:created` +
    // the immediate no-script setup completion drive runtime registration,
    // whose watch requests all settle as failed under the seam.
    let checkout = data_dir.join("checkout");
    std::fs::create_dir_all(&checkout).expect("mkdir checkout");
    let create = rpc_with_params(
        &socket,
        "workspace.create",
        json!({
            "title": "Degraded",
            "branch": "main",
            "skipWorktree": true,
            "path": checkout.to_string_lossy(),
        }),
    )
    .await;
    assert!(
        create["result"]["workspace"]["id"].is_string(),
        "workspace.create failed: {create}"
    );

    let resp = await_file_watch(&socket, |fw| {
        fw["failedRoots"].as_u64().is_some_and(|n| n > 0)
    })
    .await;
    let fw = &resp["result"]["fileWatch"];
    assert!(
        fw["failedRoots"].as_u64().is_some_and(|n| n > 0),
        "failed watch registrations must surface in fileWatch: {resp}"
    );
    assert!(
        fw["totalRoots"].as_u64().unwrap() >= fw["failedRoots"].as_u64().unwrap(),
        "failedRoots is a subset of totalRoots: {resp}"
    );
    drop(degraded);
}
