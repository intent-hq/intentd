//! Over-the-wire `mcp.servers.*` slice (PROTOCOL §5.22, §18.3): drive
//! list/create/update/delete/toggle/restart/getStatus against the daemon over a
//! temp UDS, proving camelCase shapes, sensitive `env`/`headers` **redaction**,
//! the spawn/stop/restart lifecycle of a real stdio MCP server, and the
//! `mcp.servers:status-changed` event. Uses an in-memory secret store (never the
//! real keychain) and a mock node MCP stdio server fixture (skipped if node is
//! unavailable).

mod common;

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, InMemorySecretStore, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-mcp-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(common::rpc_read_timeout(), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    for _ in 0..100 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("subscriber_count never reached {target}");
}

/// Read `mcp.servers:status-changed` events until one reports `state`, returning
/// its status payload. Fails if no matching event arrives in time.
async fn wait_for_state(reader: &mut BufReader<OwnedReadHalf>, state: &str) -> Value {
    for _ in 0..10 {
        let ev = read_json(reader).await;
        assert_eq!(ev["method"], "events.event");
        let event = &ev["params"]["event"];
        assert_eq!(event["type"], "mcp.servers:status-changed");
        if event["data"]["status"]["state"] == state {
            return event["data"]["status"].clone();
        }
    }
    panic!("never observed mcp status-changed state={state}");
}

const SECRET: &str = "supersecret_env_value_0123456789";

#[tokio::test]
async fn mcp_servers_lifecycle_redaction_and_status_event() {
    if !node_available() {
        eprintln!("skipping mcp.servers E2E: node not on PATH");
        return;
    }
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    if !PathBuf::from(script).exists() {
        eprintln!("skipping mcp.servers E2E: fixture not found at {script}");
        return;
    }

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone())
            .with_secret_store(Arc::new(InMemorySecretStore::default())),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-mcp-");
    let socket = sock_dir.path().join("uds.sock");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (rpc_read, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rpc_read);

    // list — empty before any config exists.
    let list = rpc(&mut w, &mut r, 1, "mcp.servers.list", json!({})).await;
    assert_eq!(list["servers"].as_array().unwrap().len(), 0);

    // create — a stdio server with a sensitive env value; response redacts env.
    let created = rpc(
        &mut w,
        &mut r,
        2,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Mock",
            "transport": "stdio",
            "command": "node",
            "args": [script],
            "env": { "SECRET": SECRET },
            "enabled": false,
        } }),
    )
    .await;
    let server_id = created["server"]["id"].as_str().expect("id").to_string();
    assert_eq!(created["server"]["transport"], "stdio");
    assert_ne!(created["server"]["env"]["SECRET"], json!(SECRET));
    assert!(
        !serde_json::to_string(&created).unwrap().contains(SECRET),
        "secret leaked in create result"
    );

    // list — one server, env still redacted.
    let list = rpc(&mut w, &mut r, 3, "mcp.servers.list", json!({})).await;
    assert_eq!(list["servers"].as_array().unwrap().len(), 1);
    assert!(
        !serde_json::to_string(&list).unwrap().contains(SECRET),
        "secret leaked in list"
    );

    // Subscribe to the status-changed stream on a second connection.
    let (sub_read, mut sw) = connect_retry(&socket).await.into_split();
    let mut sr = BufReader::new(sub_read);
    send(
        &mut sw,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["mcp.servers:status-changed"] },
        }))
        .unwrap(),
    )
    .await;
    let _ = read_json(&mut sr).await;
    wait_for_subscriber_count(&bus, 1).await;

    // toggle enable → spawns the mock; status running with pid + toolCount == 2.
    let toggled = rpc(
        &mut w,
        &mut r,
        4,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "running");
    assert!(toggled["status"]["pid"].is_number(), "running pid present");
    assert_eq!(toggled["status"]["toolCount"], json!(2));
    let ev = wait_for_state(&mut sr, "running").await;
    assert_eq!(ev["serverId"], json!(server_id));

    // getStatus → live running snapshot.
    let got = rpc(
        &mut w,
        &mut r,
        5,
        "mcp.servers.getStatus",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(got["status"]["state"], "running");

    // restart → stop-then-start; eventually running again.
    let restarted = rpc(
        &mut w,
        &mut r,
        6,
        "mcp.servers.restart",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(restarted["status"]["state"], "running");
    let _ = wait_for_state(&mut sr, "running").await;

    // toggle disable → stops; status stopped + a stopped event.
    let off = rpc(
        &mut w,
        &mut r,
        7,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": false }),
    )
    .await;
    assert_eq!(off["status"]["state"], "stopped");
    let _ = wait_for_state(&mut sr, "stopped").await;

    // update — edit the definition; env stays redacted in the response.
    let updated = rpc(
        &mut w,
        &mut r,
        8,
        "mcp.servers.update",
        json!({ "serverId": server_id, "config": {
            "name": "Mock Renamed",
            "transport": "stdio",
            "command": "node",
            "args": [script],
            "env": { "SECRET": SECRET },
            "enabled": false,
        } }),
    )
    .await;
    assert_eq!(updated["server"]["name"], "Mock Renamed");
    assert_eq!(updated["server"]["id"], json!(server_id));
    assert!(!serde_json::to_string(&updated).unwrap().contains(SECRET));

    // delete — removes the definition; list empty again.
    let deleted = rpc(
        &mut w,
        &mut r,
        9,
        "mcp.servers.delete",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(deleted["success"], json!(true));
    let list = rpc(&mut w, &mut r, 10, "mcp.servers.list", json!({})).await;
    assert_eq!(list["servers"].as_array().unwrap().len(), 0);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// Spawn the mock MCP server in `--http` mode and return (child, base url).
/// The fixture announces its ephemeral port as `PORT=<n>` on stdout.
async fn spawn_http_fixture(script: &str) -> (tokio::process::Child, String) {
    let mut child = tokio::process::Command::new("node")
        .arg(script)
        .arg("--http")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn mock http mcp server");
    let stdout = child.stdout.take().expect("fixture stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("timed out waiting for fixture PORT line")
        .expect("read fixture stdout")
        .expect("fixture exited before announcing PORT");
    let port = line
        .strip_prefix("PORT=")
        .expect("fixture PORT line")
        .trim()
        .to_string();
    (child, format!("http://127.0.0.1:{port}"))
}

/// Remote `http` transport: the daemon probes the endpoint over the network
/// (MCP handshake via streamable HTTP POST) — reachable server → `running` with
/// the advertised toolCount, dead port → `error` with a reachability message.
#[tokio::test]
async fn mcp_servers_http_probe_running_and_unreachable() {
    if !node_available() {
        eprintln!("skipping mcp.servers http E2E: node not on PATH");
        return;
    }
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    if !PathBuf::from(script).exists() {
        eprintln!("skipping mcp.servers http E2E: fixture not found at {script}");
        return;
    }
    let (_fixture, base_url) = spawn_http_fixture(script).await;

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone())
            .with_secret_store(Arc::new(InMemorySecretStore::default())),
    );
    let sock_dir = common::test_tempdir_in("/tmp", "itd-mcph-");
    let socket = sock_dir.path().join("uds.sock");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (rpc_read, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rpc_read);

    // Subscribe to the status-changed stream on a second connection.
    let (sub_read, mut sw) = connect_retry(&socket).await.into_split();
    let mut sr = BufReader::new(sub_read);
    send(
        &mut sw,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["mcp.servers:status-changed"] },
        }))
        .unwrap(),
    )
    .await;
    let _ = read_json(&mut sr).await;
    wait_for_subscriber_count(&bus, 1).await;

    // create an http server pointing at the live fixture (headers redacted).
    let created = rpc(
        &mut w,
        &mut r,
        2,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Mock HTTP",
            "transport": "http",
            "url": base_url,
            "headers": { "Authorization": SECRET },
            "enabled": false,
        } }),
    )
    .await;
    let server_id = created["server"]["id"].as_str().expect("id").to_string();
    assert_eq!(created["server"]["transport"], "http");
    assert!(
        !serde_json::to_string(&created).unwrap().contains(SECRET),
        "secret leaked in create result"
    );

    // toggle enable → daemon probes the endpoint; running, no pid, toolCount 2.
    let toggled = rpc(
        &mut w,
        &mut r,
        3,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "running");
    assert!(
        toggled["status"]["pid"].is_null(),
        "remote servers have no pid"
    );
    assert_eq!(toggled["status"]["toolCount"], json!(2));
    let ev = wait_for_state(&mut sr, "running").await;
    assert_eq!(ev["toolCount"], json!(2));

    // getStatus → live running snapshot.
    let got = rpc(
        &mut w,
        &mut r,
        4,
        "mcp.servers.getStatus",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(got["status"]["state"], "running");

    // restart on a remote server = re-probe; still running.
    let restarted = rpc(
        &mut w,
        &mut r,
        5,
        "mcp.servers.restart",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(restarted["status"]["state"], "running");

    // A dead port: bind + drop a listener so the address is closed at probe
    // time; the daemon reports error with a reachability message.
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_url = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let created = rpc(
        &mut w,
        &mut r,
        6,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Dead HTTP",
            "transport": "http",
            "url": dead_url,
            "enabled": false,
        } }),
    )
    .await;
    let dead_id = created["server"]["id"].as_str().expect("id").to_string();
    let toggled = rpc(
        &mut w,
        &mut r,
        7,
        "mcp.servers.toggle",
        json!({ "serverId": dead_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "error");
    assert!(
        toggled["status"]["lastError"]
            .as_str()
            .unwrap()
            .contains("unreachable from daemon host"),
        "got: {}",
        toggled["status"]["lastError"]
    );
    let _ = wait_for_state(&mut sr, "error").await;

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
