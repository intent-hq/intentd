//! Integration tests for the TB-5 subscription channels (`task`, `agent`,
//! `workspace`, `comment`) over UDS: each `*.subscribe` returns
//! `{ subscriptionId }`, then a `subscription.push` snapshot (seq 0), then
//! `{ added, updated, removedIds }` deltas mapped from bus change events via the
//! re-read strategy (PROTOCOL §6, TB-0 §2/§3).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{ActorType, EventActor};
use intent_services::{EventBus, Services};
use intent_store::{NewEvent, Store};
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
            path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
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

/// Issue one JSON-RPC request on a dedicated (non-subscribed) connection and
/// return its `result` object.
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

/// Subscribe on a fresh connection and return `(reader, write_half, sub_id,
/// snapshot_array)`.
async fn subscribe(
    socket: &PathBuf,
    method: &str,
    params: Value,
) -> (
    BufReader<OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
    String,
    Vec<Value>,
) {
    let (sub_read, mut sub_write) = connect_retry(socket).await.into_split();
    let mut reader = tokio::io::BufReader::new(sub_read);
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    send(&mut sub_write, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(&mut reader).await;
    assert_eq!(resp["id"], 1);
    let sub_id = resp["result"]["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();
    let snap = read_json(&mut reader).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    let arr = snap["params"]["snapshot"]
        .as_array()
        .expect("snapshot array")
        .clone();
    (reader, sub_write, sub_id, arr)
}

fn boot(
    bus: &EventBus,
) -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
    let socket = sock_dir.path().join("uds.sock");
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn intent_core::WorkspaceApi> = Arc::new(
        Services::new(bus.store().clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
            .with_event_bus(bus.clone()),
    );
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
    (socket, server, shutdown_tx, ws_root, sock_dir)
}

fn find<'a>(arr: &'a [Value], id: &str) -> Option<&'a Value> {
    arr.iter().find(|e| e["id"] == id)
}

#[tokio::test]
async fn agent_channel_snapshot_then_removed_delta() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

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

    // Snapshot (seq 0) lists the created agent. `agent.subscribe` carries no
    // `eventTypes`, so it routes to the collection channel (not the alias).
    let (mut sub_reader, _sub_write, sub_id, snap) =
        subscribe(&socket, "agent.subscribe", json!({ "workspaceId": ws_id })).await;
    assert!(sub_id.starts_with("ws-sub-"));
    let agent = find(&snap, &agent_id).expect("agent in snapshot");
    assert!(
        agent["rev"].is_null(),
        "agent entities carry no rev (R3 scopes rev to Note/Task)"
    );

    // agent.delete → AGENT_DELETED → removedIds delta (seq 1).
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.delete",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let d1 = read_json(&mut sub_reader).await;
    assert_eq!(d1["params"]["kind"], "delta");
    assert_eq!(d1["params"]["seq"], 1);
    assert_eq!(d1["params"]["delta"]["removedIds"][0], agent_id.as_str());

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn task_channel_snapshot_then_updated_delta() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

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
    let n = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "T" }),
    )
    .await;
    let note_id = n["note"]["id"].as_str().unwrap().to_string();
    // A plain note is not in the task snapshot; mark it a task first.
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "not_started" }),
    )
    .await;

    let (mut sub_reader, _sub_write, _sub_id, snap) =
        subscribe(&socket, "task.subscribe", json!({ "workspaceId": ws_id })).await;
    let task = find(&snap, &note_id).expect("task in snapshot");
    assert!(
        task["metadata"]["task"].is_object(),
        "task metadata present in snapshot"
    );
    assert!(
        task["rev"].is_number(),
        "rev echoed on task snapshot entity"
    );
    assert_eq!(
        task["specLinked"], false,
        "specLinked stamped on snapshot rows (unlinked from spec body)"
    );

    // task.updateNoteStatus → task:status-changed → updated delta (seq 1).
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        13,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    let d1 = read_json(&mut sub_reader).await;
    assert_eq!(d1["params"]["kind"], "delta");
    assert_eq!(d1["params"]["seq"], 1);
    assert_eq!(d1["params"]["delta"]["updated"][0]["id"], note_id.as_str());
    assert_eq!(
        d1["params"]["delta"]["updated"][0]["metadata"]["task"]["status"],
        "in_progress"
    );
    assert!(
        d1["params"]["delta"]["updated"][0]["rev"].is_number(),
        "rev echoed on task updated delta entity"
    );
    assert_eq!(
        d1["params"]["delta"]["updated"][0]["specLinked"], false,
        "specLinked stamped on updated delta rows"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn comment_channel_snapshot_then_updated_delta() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

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
    let n = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "N", "content": "anchor target text here" }),
    )
    .await;
    let note_id = n["note"]["id"].as_str().unwrap().to_string();

    // Snapshot is the (empty) thread list for this note.
    let (mut sub_reader, _sub_write, _sub_id, snap) = subscribe(
        &socket,
        "comment.subscribe",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(snap.is_empty(), "no threads yet");

    // comment.add → COMMENT_ADDED → updated delta carrying the thread (seq 1).
    let c = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "anchor target text here",
            "commentTarget": "target",
            "comment": "looks good"
        }),
    )
    .await;
    let comment_id = c["commentId"].as_str().unwrap().to_string();
    let d1 = read_json(&mut sub_reader).await;
    assert_eq!(d1["params"]["kind"], "delta");
    assert_eq!(d1["params"]["seq"], 1);
    let thread = &d1["params"]["delta"]["updated"][0];
    assert_eq!(thread["threadId"], comment_id.as_str());
    assert_eq!(thread["noteId"], note_id.as_str());
    assert!(
        thread["rev"].is_null(),
        "comment entities carry no rev (R3 scopes rev to Note/Task)"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn workspace_channel_snapshot_then_updated_delta() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, _ws_root, _sock_dir) = boot(&bus);

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

    // An archived workspace must also appear in the seq-0 snapshot, matching
    // the deltas which upsert archived workspaces (intent-hq/monorepo#775).
    let archived = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "workspace.create",
        json!({ "title": "Archived WS" }),
    )
    .await;
    let archived_id = archived["workspace"]["id"].as_str().unwrap().to_string();
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "workspace.archive",
        json!({ "workspaceId": archived_id }),
    )
    .await;

    // The workspace channel is global (no `workspaceId` param).
    let (mut sub_reader, _sub_write, _sub_id, snap) =
        subscribe(&socket, "workspace.subscribe", json!({})).await;
    let ws_entry = find(&snap, &ws_id).expect("workspace in snapshot");
    assert!(
        ws_entry["rev"].is_null(),
        "workspace entities carry no rev (R3 scopes rev to Note/Task)"
    );
    // The seq-0 lite snapshot is self-sufficient for client status rendering:
    // rows carry taskStats + displayStatus + cowSupported while the heavy
    // aggregates (agentSummary/diffSummary) stay omitted.
    assert!(
        ws_entry["taskStats"]["total"].is_number(),
        "snapshot rows carry taskStats: {ws_entry}"
    );
    assert!(
        ws_entry["displayStatus"].is_string(),
        "snapshot rows carry displayStatus: {ws_entry}"
    );
    assert!(
        ws_entry["cowSupported"].is_boolean(),
        "snapshot rows carry cowSupported: {ws_entry}"
    );
    assert!(
        ws_entry.get("agentSummary").is_none(),
        "snapshot rows omit agentSummary: {ws_entry}"
    );
    assert!(
        ws_entry.get("diffSummary").is_none(),
        "snapshot rows omit diffSummary: {ws_entry}"
    );
    // The snapshot's displayStatus matches a subsequent enriched
    // workspace.get for the same data (same derivation, no drift).
    let got = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        13,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["workspace"]["displayStatus"], ws_entry["displayStatus"],
        "lite snapshot displayStatus matches enriched workspace.get"
    );
    assert_eq!(
        got["workspace"]["taskStats"], ws_entry["taskStats"],
        "lite snapshot taskStats matches enriched workspace.get"
    );
    let archived_entry = find(&snap, &archived_id).expect("archived workspace in snapshot");
    assert_eq!(
        archived_entry["status"], "Archived",
        "snapshot includes archived workspaces with their status"
    );

    // A workspace status event re-reads the workspace into an `updated` delta.
    bus.publish(&NewEvent {
        workspace_id: intent_core::WorkspaceId::from(ws_id.clone()),
        timestamp: "2026-06-26T00:00:00.000Z".to_string(),
        event_type: "workspace:attention-changed".to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "workspaceId": ws_id, "attention": "none" }),
    })
    .await
    .expect("publish");
    let d1 = read_json(&mut sub_reader).await;
    assert_eq!(d1["params"]["kind"], "delta");
    assert_eq!(d1["params"]["seq"], 1);
    assert_eq!(d1["params"]["delta"]["updated"][0]["id"], ws_id.as_str());

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
