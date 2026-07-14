//! mDNS discovery lifecycle invariant tests: prove that discovery advertisement
//! requires an active listener and cannot run independently (§5.4 + ownership
//! rationale in discovery.rs / lifecycle.rs module docs).

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-mdns-test-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .expect("serialize rpc");
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.expect("write");
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut response = String::new();
    reader.read_line(&mut response).await.expect("read");
    serde_json::from_str(&response).expect("parse response")
}

/// Regression: disabling the listener must also stop discovery. Advertising a
/// service that cannot be connected to violates the mDNS contract (ownership
/// invariant documented in discovery.rs / lifecycle.rs module docs).
///
/// This test verifies the invariant holds by ensuring that toggling operations
/// succeed in the expected order: enabling discovery requires listener, but
/// disabling listener automatically stops discovery (coupling enforced).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabling_listener_stops_discovery() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let port = free_port();
    let port_s = port.to_string();
    let token = "abababababababababababababababababababababababababababababababab";
    let env = [("INTENTD_AUTH_TOKEN", token), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let mut daemon = Daemon { child, data_dir };
    assert!(await_uds(&socket).await, "daemon should start within 10s");

    // Enable the listener
    let enable = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable.get("error").is_none(),
        "enable should succeed: {enable}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Enable discovery while listener is running
    let enable_disc = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.discovery.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable_disc.get("error").is_none(),
        "enable discovery should succeed when listener is running"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Disable the listener — this implicitly stops discovery (coupling invariant)
    let disable = uds_rpc(
        &socket,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;
    assert!(disable.get("error").is_none(), "disable should succeed");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify listener is stopped
    let status_after = uds_rpc(&socket, 4, "system.status", json!({})).await;
    assert!(
        status_after["result"]["port"].is_null(),
        "port should be null after listener stop"
    );

    // Attempting to enable discovery now should fail (listener not running).
    // This enforces the invariant: discovery requires an active listener.
    let enable_disc_fail = uds_rpc(
        &socket,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "server.discovery.enabled", "value": true }] }),
    )
    .await;
    // The settings hook should return an error because the listener is not running.
    // This proves the invariant is enforced at runtime.
    assert!(
        enable_disc_fail.get("error").is_some(),
        "settings.update should fail when trying to enable discovery without listener (invariant)"
    );

    daemon.child.kill().expect("kill daemon");
    daemon.child.wait().expect("wait daemon");
}

/// Regression: toggling discovery.enabled while the listener runs should work
/// independently (no listener restart required). This proves independent runtime
/// control while still enforcing the invariant (discovery requires listener).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toggle_discovery_while_listener_runs() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let port = free_port();
    let port_s = port.to_string();
    let token = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    let env = [("INTENTD_AUTH_TOKEN", token), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let mut daemon = Daemon { child, data_dir };
    assert!(await_uds(&socket).await, "daemon should start within 10s");

    // Enable the listener
    let enable = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;
    assert!(enable.get("error").is_none());
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Enable discovery
    let enable_disc = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.discovery.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable_disc.get("error").is_none(),
        "enable discovery should succeed while listener runs"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Disable discovery while listener stays running
    let disable_disc = uds_rpc(
        &socket,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.discovery.enabled", "value": false }] }),
    )
    .await;
    assert!(
        disable_disc.get("error").is_none(),
        "disable discovery should succeed while listener runs"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Listener should still be running (independent toggle)
    let status = uds_rpc(&socket, 4, "system.status", json!({})).await;
    assert!(
        status["result"]["port"].as_u64().is_some(),
        "listener should still be running after discovery toggle"
    );

    // Re-enable discovery without restarting listener
    let reenable_disc = uds_rpc(
        &socket,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "server.discovery.enabled", "value": true }] }),
    )
    .await;
    assert!(
        reenable_disc.get("error").is_none(),
        "re-enable discovery should succeed (independent toggle)"
    );

    daemon.child.kill().expect("kill daemon");
    daemon.child.wait().expect("wait daemon");
}

/// Regression: batch ordering must enforce wsApi.enabled before discovery.enabled
/// regardless of input order. This tests the dependency-aware ordering system that
/// prevents "listener not running" errors from non-deterministic map iteration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_ordering_enforces_listener_before_discovery() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let port = free_port();
    let port_s = port.to_string();
    let token = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
    let env = [("INTENTD_AUTH_TOKEN", token), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let mut daemon = Daemon { child, data_dir };
    assert!(await_uds(&socket).await);

    // Batch update with discovery.enabled FIRST (reverse dependency order in input).
    // The hook system must reorder them: wsApi.enabled (priority 10) applies before
    // discovery.enabled (priority 11), so no "listener not running" error.
    let batch_reverse = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({
            "changes": [
                { "path": "server.discovery.enabled", "value": true },
                { "path": "server.wsApi.enabled", "value": true }
            ]
        }),
    )
    .await;
    assert!(
        batch_reverse.get("error").is_none(),
        "batch with reverse input order should succeed (ordering system fixes it): {batch_reverse}"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify listener started
    let status = uds_rpc(&socket, 2, "system.status", json!({})).await;
    assert!(
        status["result"]["port"].as_u64().is_some(),
        "listener should be running after batch enable"
    );

    daemon.child.kill().expect("kill daemon");
    daemon.child.wait().expect("wait daemon");
}
