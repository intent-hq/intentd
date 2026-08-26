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
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
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
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": {} });
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
