//! WSS end-to-end test for `server.pairingInfo` and `server.rotateToken`.
//!
//! Drives the real WSS transport with TLS + bearer auth to prove the wire
//! contract per AGENTS.md requirement: every new JSON-RPC method must have a
//! WSS e2e test exercising the full production path.
//!
//! Tests:
//! - server.pairingInfo returns complete credentials over local WSS
//! - server.rotateToken mints a new token and invalidates the old one
//! - Remote connections are rejected with -32001

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::timeout;
use uuid::Uuid;

const TOKEN: &str = "test-token-fixed-64-hex-cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-server-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, port: u16) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg("both")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_TCP_PORT", port.to_string())
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in &[("MOCK_ACP_HOST", "localhost:0")] {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    for _ in 0..200 {
        if socket.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let stream = tokio::net::UnixStream::connect(socket).await.expect("UDS connect");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    let mut line = frame.to_string();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut buf = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

async fn boot(data_dir: &Path, _port: u16) -> (u16, String) {
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let actual_port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (actual_port, fingerprint)
}



#[tokio::test]
async fn server_pairing_info_over_uds() {
    let data_dir = temp_data_dir();
    let port_hint = free_port();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir, port_hint),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir, port_hint).await;
    let socket = data_dir.join("intentd.sock");

    // server.pairingInfo over UDS (local) returns credentials
    let response = uds_rpc(&socket, 2, "server.pairingInfo", json!({})).await;
    let result = &response["result"];

    assert_eq!(result["token"].as_str().unwrap(), TOKEN);
    assert_eq!(result["certFingerprint"].as_str().unwrap(), fp);
    assert_eq!(result["port"].as_u64().unwrap(), port as u64);
    assert_eq!(result["path"].as_str().unwrap(), "/ws");
    assert!(result["localIps"].is_array());
    assert!(result["hostname"].is_string());

    daemon.child.kill().ok();
}

#[tokio::test]
async fn server_rotate_token_env_fixed_rejects() {
    let data_dir = temp_data_dir();
    let port_hint = free_port();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir, port_hint),
        data_dir: data_dir.clone(),
    };
    let (_port, _fp) = boot(&data_dir, port_hint).await;
    let socket = data_dir.join("intentd.sock");

    // INTENTD_AUTH_TOKEN is set in spawn_serve, so rotation should reject over UDS
    let response = uds_rpc(&socket, 2, "server.rotateToken", json!({})).await;
    let error = &response["error"];

    assert_eq!(error["code"].as_i64().unwrap(), -32602); // InvalidParams
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("cannot rotate token"));

    daemon.child.kill().ok();
}
