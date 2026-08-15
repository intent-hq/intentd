//! Integration test for the M2.3 UDS event fast-path: a client subscribes,
//! receives pushed `events.event` notifications for matching published events,
//! unsubscribes, and a dropped connection releases its subscriptions (§6).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{ActorType, EventActor, WorkspaceApi, WorkspaceId};
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

fn new_event(event_type: &str, workspace_id: &str) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(workspace_id),
        timestamp: "2026-06-17T04:35:04.055Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some("agent-123".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "noteId": "spec", "action": "update" }),
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

async fn expect_no_frame(reader: &mut BufReader<OwnedReadHalf>) {
    let mut line = String::new();
    let r = timeout(Duration::from_millis(400), reader.read_line(&mut line)).await;
    if let Ok(Ok(n)) = r {
        assert!(n == 0, "unexpected frame after unsubscribe: {line}");
    }
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    for _ in 0..100 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "subscriber_count never reached {target} (last={})",
        bus.subscriber_count()
    );
}

#[tokio::test]
async fn subscribe_push_filter_unsubscribe_and_disconnect_cleanup() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
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

    let (read_half, mut write_half) = connect_retry(&socket).await.into_split();
    let mut reader = BufReader::new(read_half);

    // Invalid subscription params (missing eventTypes) → -32602 with the
    // machine-readable discriminator (PROTOCOL §3.3, monorepo#1364).
    send(
        &mut write_half,
        r#"{"jsonrpc":"2.0","id":0,"method":"events.subscribe","params":{}}"#,
    )
    .await;
    let bad = read_json(&mut reader).await;
    assert_eq!(bad["id"], 0);
    assert_eq!(bad["error"]["code"], json!(-32602));
    assert_eq!(bad["error"]["data"]["code"], "invalid-params");

    send(&mut write_half, r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["note:*"],"workspaceId":"ws-1"}}"#).await;
    let resp = read_json(&mut reader).await;
    assert_eq!(resp["id"], 1);
    let sub_id = resp["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sub_id.starts_with("ws-sub-"));
    wait_for_subscriber_count(&bus, 1).await;

    // Filtered push: agent:idle and the ws-2 event are dropped; only the two
    // matching note:* events in ws-1 are delivered, in order.
    bus.publish(&new_event("agent:idle", "ws-1")).await.unwrap();
    let m1 = bus
        .publish(&new_event("note:updated", "ws-1"))
        .await
        .unwrap();
    let n1 = read_json(&mut reader).await;
    assert_eq!(n1["method"], "events.event");
    assert_eq!(n1["params"]["subscriptionId"], sub_id.as_str());
    assert_eq!(n1["params"]["event"]["type"], "note:updated");
    assert_eq!(n1["params"]["event"]["id"], m1.id.as_str());
    assert_eq!(n1["params"]["event"]["workspaceId"], "ws-1");

    bus.publish(&new_event("note:created", "ws-2"))
        .await
        .unwrap();
    let m2 = bus
        .publish(&new_event("note:created", "ws-1"))
        .await
        .unwrap();
    let n2 = read_json(&mut reader).await;
    assert_eq!(n2["params"]["event"]["id"], m2.id.as_str());

    send(&mut write_half, &format!(r#"{{"jsonrpc":"2.0","id":2,"method":"events.unsubscribe","params":{{"subscriptionId":"{sub_id}"}}}}"#)).await;
    let unsub = read_json(&mut reader).await;
    assert_eq!(unsub["id"], 2);
    assert_eq!(unsub["result"]["success"], true);
    wait_for_subscriber_count(&bus, 0).await;

    bus.publish(&new_event("note:updated", "ws-1"))
        .await
        .unwrap();
    expect_no_frame(&mut reader).await;

    // Disconnect cleanup: a fresh connection subscribes, then drops its socket.
    let (read2, mut write2) = connect_retry(&socket).await.into_split();
    let mut reader2 = BufReader::new(read2);
    send(&mut write2, r#"{"jsonrpc":"2.0","id":9,"method":"events.subscribe","params":{"eventTypes":["note:*"]}}"#).await;
    let _ = read_json(&mut reader2).await;
    wait_for_subscriber_count(&bus, 1).await;
    drop(write2);
    drop(reader2);
    wait_for_subscriber_count(&bus, 0).await;

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// Issue one JSON-RPC request on a dedicated (non-subscribed) connection and
/// return its `result` object. The connection carries only responses, so reads
/// never interleave with pushed `events.event` notifications.
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

/// End-to-end change-event proof (M2.6): one connection subscribes; another runs
/// CRUD across workspace/note/task/comment over JSON-RPC; the subscriber receives
/// the matching `events.event` notifications with the camelCase envelope + payload
/// shapes the iOS client expects (PROTOCOL §6.5).
#[tokio::test]
async fn crud_mutations_emit_change_events_over_uds() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    // The services surface must publish onto the SAME bus the transport reads.
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
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

    // RPC connection (mutations + their responses only).
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);

    // Create the workspace first so the subscription below can scope to its
    // id (its `workspace:created` fires before the subscribe, so it is not
    // observed here — covered by workspace_create_emits_workspace_created).
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        10,
        "workspace.create",
        json!({ "title": "WS" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    // Subscriber connection, scoped to this workspace across the change families.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events.subscribe",
            "params": { "eventTypes": ["note:*", "task:*", "comment:*", "workspace:*"], "workspaceId": ws_id },
        }))
        .unwrap(),
    )
    .await;
    let sub_resp = read_json(&mut sub_reader).await;
    let sub_id = sub_resp["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    wait_for_subscriber_count(&bus, 1).await;

    // note.create → note:created { noteId, title, action: "create" }.
    let created = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        11,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note" }),
    )
    .await;
    let note_id = created["note"]["id"].as_str().unwrap().to_string();
    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["method"], "events.event");
    assert_eq!(ev["params"]["subscriptionId"], sub_id.as_str());
    let e = &ev["params"]["event"];
    // Envelope camelCase parity (PROTOCOL §6.3 / §6.5).
    assert_eq!(e["type"], "note:created");
    assert_eq!(e["workspaceId"], ws_id.as_str());
    assert!(e["id"].is_string());
    assert!(e["timestamp"].is_string());
    assert_eq!(
        e["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    assert_eq!(
        e["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "create" })
    );

    // note.update (content) → note:updated.
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "note.update",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": "hello world" }),
    )
    .await;
    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["params"]["event"]["type"], "note:updated");
    assert_eq!(
        ev["params"]["event"]["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "update" })
    );

    // task.markAsTask makes it a task → note:updated (the metadata write) then
    // task:created, since the note was not already a task. task.updateNoteStatus
    // then → task:status-changed with the previous/new status payload.
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        13,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "not_started" }),
    )
    .await;
    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["params"]["event"]["type"], "note:updated");
    assert_eq!(
        ev["params"]["event"]["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "update" })
    );
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "task:created");
    assert_eq!(
        e["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    assert_eq!(e["data"]["noteId"], note_id.as_str());
    assert_eq!(e["data"]["noteTitle"], "Note");
    assert_eq!(e["data"]["status"], "not_started");
    assert!(e["data"]["createdAt"].is_string());
    assert!(e["data"].get("agentId").is_none());

    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        14,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "task:status-changed");
    assert_eq!(e["data"]["noteId"], note_id.as_str());
    assert_eq!(e["data"]["noteTitle"], "Note");
    assert_eq!(e["data"]["previousStatus"], "not_started");
    assert_eq!(e["data"]["newStatus"], "in_progress");
    assert!(e["data"]["changedAt"].is_string());

    // The status change recomputes the ready set → task:ready-tasks-changed
    // carrying the full readyTaskIds list plus the triggering transition.
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "task:ready-tasks-changed");
    assert_eq!(e["data"]["readyTaskIds"], json!([note_id]));
    assert_eq!(e["data"]["triggeredBy"]["noteId"], note_id.as_str());
    assert_eq!(e["data"]["triggeredBy"]["previousStatus"], "not_started");
    assert_eq!(e["data"]["triggeredBy"]["newStatus"], "in_progress");
    assert!(e["data"]["computedAt"].is_string());

    // comment.add → note:updated (the add rewrites the note markdown with
    // anchor markers, monorepo#638) followed by comment:added.
    let added = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        15,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "hello world",
            "commentTarget": "hello",
            "comment": "nice",
        }),
    )
    .await;
    let comment_id = added["commentId"].as_str().unwrap().to_string();
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "note:updated");
    assert_eq!(
        e["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "update" })
    );
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "comment:added");
    assert_eq!(
        e["data"],
        json!({ "noteId": note_id, "commentId": comment_id })
    );

    // Raise then dismiss attention. `workspace.update` emits
    // `workspace:updated` (§6.5) with the applied `WorkspaceUpdate` delta;
    // `workspace.dismissAttention` follows with
    // `workspace:attention-changed`. The unread flag is not a displayStatus
    // axis (§6.5), so neither mutation emits a
    // `workspace:displayStatus-changed` — the stream is sequential, so the
    // dismiss's attention-changed arriving directly after the update's
    // workspace:updated proves no displayStatus event interleaved.
    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        16,
        "workspace.update",
        json!({ "workspaceId": ws_id, "attention": "unread" }),
    )
    .await;
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "workspace:updated");
    assert_eq!(e["workspaceId"], ws_id.as_str());
    assert_eq!(
        e["data"],
        json!({ "workspaceId": ws_id, "changes": { "attention": "unread" } }),
    );

    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        17,
        "workspace.dismissAttention",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let ev = read_json(&mut sub_reader).await;
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "workspace:attention-changed");
    assert_eq!(
        e["data"],
        json!({ "workspaceId": ws_id, "attention": "none" })
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// `workspace.create` emits `workspace:created` (PROTOCOL §6.5): a subscriber
/// on the `workspace:*` family (no workspace filter — the id is minted by the
/// create) receives the event with the self-sufficient `{ workspaceId,
/// workspace }` payload (§6.7) matching the RPC result.
#[tokio::test]
async fn workspace_create_emits_workspace_created() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
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

    // Subscriber first: the workspace id does not exist yet, so no filter.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["workspace:*"]}}"#,
    )
    .await;
    let sub_resp = read_json(&mut sub_reader).await;
    assert!(sub_resp["result"]["subscriptionId"].is_string());
    wait_for_subscriber_count(&bus, 1).await;

    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        2,
        "workspace.create",
        json!({ "title": "Created WS", "branch": "feat/created-event" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["method"], "events.event");
    let e = &ev["params"]["event"];
    // Envelope camelCase parity (PROTOCOL §6.3 / §6.5).
    assert_eq!(e["type"], "workspace:created");
    assert_eq!(e["workspaceId"], ws_id.as_str());
    assert!(e["id"].is_string());
    assert!(e["timestamp"].is_string());
    assert_eq!(
        e["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    // Self-sufficient payload: the event carries the same Workspace the RPC
    // result returned, so clients render it without a follow-up read.
    assert_eq!(e["data"]["workspaceId"], ws_id.as_str());
    assert_eq!(e["data"]["workspace"], ws["workspace"]);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// `workspace.update` emits `workspace:updated` (PROTOCOL §6.5): the payload
/// carries the applied `WorkspaceUpdate` delta as `changes` (reference-parity
/// FE emitter), so a subscriber can mirror the mutation without a follow-up
/// `workspace.get` read.
#[tokio::test]
async fn workspace_update_emits_workspace_updated_with_delta() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
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

    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        1,
        "workspace.create",
        json!({ "title": "Original", "branch": "main" }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    // Subscribe after create so the pre-subscribe workspace:created is not
    // observed here (covered by workspace_create_emits_workspace_created).
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "events.subscribe",
            "params": { "eventTypes": ["workspace:updated"], "workspaceId": ws_id },
        }))
        .unwrap(),
    )
    .await;
    let sub_resp = read_json(&mut sub_reader).await;
    assert!(sub_resp["result"]["subscriptionId"].is_string());
    wait_for_subscriber_count(&bus, 1).await;

    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        3,
        "workspace.update",
        json!({ "workspaceId": ws_id, "title": "Renamed", "tags": ["a", "b"] }),
    )
    .await;

    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["method"], "events.event");
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "workspace:updated");
    assert_eq!(e["workspaceId"], ws_id.as_str());
    assert!(e["id"].is_string());
    assert!(e["timestamp"].is_string());
    assert_eq!(
        e["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    // `changes` is the applied WorkspaceUpdate delta only (Option::is_none
    // fields are skipped in serialization), so absent fields do not leak.
    assert_eq!(
        e["data"],
        json!({
            "workspaceId": ws_id,
            "changes": { "title": "Renamed", "tags": ["a", "b"] },
        })
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// `workspace.delete` emits `workspace:deleted` (PROTOCOL §6.5): minimal
/// `{ workspaceId }` payload (reference-parity FE emitter). The event fires
/// only after the store row is actually removed.
#[tokio::test]
async fn workspace_delete_emits_workspace_deleted() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
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

    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    let ws = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        1,
        "workspace.create",
        json!({ "title": "ToDelete", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "events.subscribe",
            "params": { "eventTypes": ["workspace:deleted"], "workspaceId": ws_id },
        }))
        .unwrap(),
    )
    .await;
    let sub_resp = read_json(&mut sub_reader).await;
    assert!(sub_resp["result"]["subscriptionId"].is_string());
    wait_for_subscriber_count(&bus, 1).await;

    rpc(
        &mut rpc_write,
        &mut rpc_reader,
        3,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let ev = read_json(&mut sub_reader).await;
    assert_eq!(ev["method"], "events.event");
    let e = &ev["params"]["event"];
    assert_eq!(e["type"], "workspace:deleted");
    assert_eq!(e["workspaceId"], ws_id.as_str());
    assert_eq!(e["data"], json!({ "workspaceId": ws_id }));

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
