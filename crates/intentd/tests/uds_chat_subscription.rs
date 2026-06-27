//! Integration tests for the per-agent `chat` subscription channel (CS-0) over
//! UDS: `chat.subscribe {agentId}` returns `{ subscriptionId }`, then a
//! `subscription.push` snapshot (seq 0) equal to the agent's newest
//! `agent.getConversation` page (the `messages[]` OBJECT shape, CS-0 D3).
//! `chat.unsubscribe` cleans up; a missing `agentId` is `-32602`; snapshots are
//! isolated per agent. The live delta mapper is CS-3 (not exercised here).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_services::{EventBus, Services};
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
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
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
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
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

async fn boot(bus: &EventBus) -> (PathBuf, tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
    let socket = std::env::temp_dir().join(format!("intentd-uds-{}.sock", Uuid::new_v4()));
    let services: Arc<dyn intent_core::WorkspaceApi> =
        Arc::new(Services::new(bus.store().clone()).with_event_bus(bus.clone()));
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
    (socket, server, shutdown_tx)
}

/// Create a workspace + agent on a fresh control connection; return `(socket,
/// server, shutdown_tx, ws_id, agent_id)` plus the live rpc connection halves.
async fn setup() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    TempDb,
) {
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx) = boot(&bus).await;
    (socket, server, shutdown_tx, tmp)
}

#[tokio::test]
async fn chat_subscribe_snapshot_matches_conversation_then_unsubscribe() {
    let (socket, server, shutdown_tx, _tmp) = setup().await;
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = tokio::io::BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();
    let a = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "A1" }),
    )
    .await;
    let agent_id = a["agent"]["id"].as_str().unwrap().to_string();
    // The expected newest page is exactly what `agent.getConversation` returns.
    let want = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;

    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(
            &json!({ "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe", "params": { "agentId": agent_id } }),
        )
        .unwrap(),
    )
    .await;
    let resp = read_json(&mut sub_reader).await;
    assert_eq!(resp["id"], 1);
    assert!(resp["result"]["subscriptionId"].as_str().is_some());

    let snap = read_json(&mut sub_reader).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    // seq-0 snapshot equals the agent's newest conversation page (object shape).
    assert_eq!(snap["params"]["snapshot"], want);
    assert_eq!(snap["params"]["snapshot"]["agentId"], agent_id.as_str());

    // chat.unsubscribe cleans up the subscription.
    let ok = rpc(
        &mut sub_write,
        &mut sub_reader,
        2,
        "chat.unsubscribe",
        json!({ "subscriptionId": resp["result"]["subscriptionId"] }),
    )
    .await;
    assert_eq!(ok["success"], true);
    let again = rpc(
        &mut sub_write,
        &mut sub_reader,
        3,
        "chat.unsubscribe",
        json!({ "subscriptionId": resp["result"]["subscriptionId"] }),
    )
    .await;
    assert_eq!(again["success"], false, "second unsubscribe is a no-op");

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn chat_subscribe_missing_agent_id_is_invalid_params() {
    let (socket, server, shutdown_tx, _tmp) = setup().await;
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(
            &json!({ "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe", "params": {} }),
        )
        .unwrap(),
    )
    .await;
    let resp = read_json(&mut sub_reader).await;
    assert_eq!(resp["id"], 1);
    assert_eq!(resp["error"]["code"], -32602);
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("agentId is required"));

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn chat_subscribe_isolates_snapshot_per_agent() {
    let (socket, server, shutdown_tx, _tmp) = setup().await;
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = tokio::io::BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();
    let a = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "A" }),
    )
    .await;
    let b = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "B" }),
    )
    .await;
    let a_id = a["agent"]["id"].as_str().unwrap().to_string();
    let b_id = b["agent"]["id"].as_str().unwrap().to_string();
    assert_ne!(a_id, b_id);

    for agent_id in [&a_id, &b_id] {
        let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
        let mut sub_reader = tokio::io::BufReader::new(sub_read);
        send(
            &mut sub_write,
            &serde_json::to_string(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe",
                "params": { "agentId": agent_id }
            }))
            .unwrap(),
        )
        .await;
        let _resp = read_json(&mut sub_reader).await;
        let snap = read_json(&mut sub_reader).await;
        // Each subscription's snapshot is scoped to its own agent.
        assert_eq!(snap["params"]["snapshot"]["agentId"], agent_id.as_str());
    }

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
