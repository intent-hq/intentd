//! Integration tests for the per-agent `chat` subscription channel (CS-0) over
//! UDS: `chat.subscribe {agentId}` returns `{ subscriptionId }`, then a
//! `subscription.push` snapshot (seq 0) equal to the agent's newest
//! `agent.getConversation` page (the `messages[]` OBJECT shape, CS-0 D3).
//! `chat.unsubscribe` cleans up; a missing `agentId` is `-32602`; snapshots are
//! isolated per agent. CS-3 adds the live delta mapper: a mock streaming turn
//! (text chunks + a tool call + `stream:end`) is driven over the bus and the
//! emitted `subscription.push` deltas are reduced on top of the seq-0 snapshot
//! and asserted to equal a fresh `agent.getConversation` snapshot (the
//! reconciliation invariant).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::{AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_TOOL_CALL};
use intent_core::{now_iso, ActorType, AgentId, EventActor, WorkspaceId};
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

/// Like [`setup`] but also returns the live [`EventBus`] so a test can persist
/// messages and publish `agent:stream:*` events that drive the chat forwarder.
async fn setup_with_bus() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    TempDb,
    EventBus,
) {
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx) = boot(&bus).await;
    (socket, server, shutdown_tx, tmp, bus)
}

/// Publish one `agent:stream:*` event scoped to `agent_id` (the chat forwarder
/// filters on `sessionId == agentId`), mirroring `publish_agent_event`.
async fn publish_stream(
    bus: &EventBus,
    ws_id: &str,
    agent_id: &str,
    event_type: &str,
    data: Value,
) {
    let ev = NewEvent {
        workspace_id: WorkspaceId::from(ws_id),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(agent_id.to_string()),
            ..Default::default()
        },
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    };
    bus.publish(&ev).await.expect("publish stream event");
}

/// Apply one delta entity (`{ messageId, role, messageSeq?, timestamp?, block }`)
/// onto a reconstructed `messages[]`: find-or-create the message envelope, refresh
/// any authoritative fields, then upsert the block by id (preserving order).
fn apply_entity(messages: &mut Vec<Value>, entity: &Value) {
    let message_id = entity["messageId"].as_str().expect("messageId").to_string();
    let idx = messages
        .iter()
        .position(|m| m["id"].as_str() == Some(message_id.as_str()))
        .unwrap_or_else(|| {
            messages.push(json!({
                "id": message_id,
                "agentId": Value::Null,
                "seq": Value::Null,
                "role": Value::Null,
                "contentBlocks": [],
                "timestamp": Value::Null,
            }));
            messages.len() - 1
        });
    let msg = &mut messages[idx];
    if let Some(v) = entity.get("agentId") {
        msg["agentId"] = v.clone();
    }
    if let Some(v) = entity.get("role") {
        msg["role"] = v.clone();
    }
    if let Some(v) = entity.get("messageSeq") {
        msg["seq"] = v.clone();
    }
    if let Some(v) = entity.get("timestamp") {
        msg["timestamp"] = v.clone();
    }
    let block = entity["block"].clone();
    let block_id = block["id"].as_str().expect("block id").to_string();
    let blocks = msg["contentBlocks"].as_array_mut().expect("contentBlocks");
    match blocks
        .iter()
        .position(|b| b["id"].as_str() == Some(block_id.as_str()))
    {
        Some(bi) => blocks[bi] = block,
        None => blocks.push(block),
    }
}

/// Reduce one `{ added, updated, removedIds }` delta onto `messages` (added then
/// updated upserts, then `removedIds` block removals).
fn apply_delta(messages: &mut Vec<Value>, delta: &Value) {
    for key in ["added", "updated"] {
        for entity in delta[key].as_array().into_iter().flatten() {
            apply_entity(messages, entity);
        }
    }
    for removed in delta["removedIds"].as_array().into_iter().flatten() {
        let Some(id) = removed.as_str() else { continue };
        for msg in messages.iter_mut() {
            if let Some(blocks) = msg["contentBlocks"].as_array_mut() {
                blocks.retain(|b| b["id"].as_str() != Some(id));
            }
        }
    }
}

/// Whether a delta is the terminal (`stream:end`) reconcile frame — its entities
/// carry `streamingComplete: true`.
fn is_terminal_delta(delta: &Value) -> bool {
    ["added", "updated"].iter().any(|key| {
        delta[*key]
            .as_array()
            .into_iter()
            .flatten()
            .any(|e| e.get("streamingComplete") == Some(&Value::Bool(true)))
    })
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

/// CS-3 reconciliation invariant: the seq-0 snapshot reduced with the live
/// `stream:chunk`/`tool:call`/`stream:end` deltas equals a fresh
/// `agent.getConversation` snapshot. Drives a mock turn — two text chunks that
/// coalesce onto one block (added → updated), a tool call (tool_use → tool_use
/// updated + tool_result added), then trailing text — persists the assistant
/// message exactly as `run_prompt_turn` would, and finally emits `stream:end`.
#[tokio::test]
async fn chat_delta_stream_reconciles_with_fresh_snapshot() {
    let (socket, server, shutdown_tx, _tmp, bus) = setup_with_bus().await;
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

    // A persisted user message makes the seq-0 snapshot non-trivial; it must be
    // preserved untouched through reconciliation.
    let store = bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            &now_iso(),
        )
        .await
        .expect("append user message");

    // Subscribe AFTER the user message exists so it lands in the seq-0 snapshot.
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
    let resp = read_json(&mut sub_reader).await;
    assert_eq!(resp["id"], 1);
    let snap = read_json(&mut sub_reader).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    let mut reconstructed: Vec<Value> = snap["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");
    assert_eq!(reconstructed.len(), 1, "snapshot holds the user message");

    // Drive the assistant turn over the bus (enriched payloads per CS-1 D4).
    let mid = Uuid::now_v7().to_string();
    let chunk = |idx: u64, text: &str| {
        json!({
            "agentId": agent_id, "content": text, "messageId": mid,
            "blockIndex": idx, "blockId": format!("{mid}:{idx}"), "blockType": "text",
        })
    };
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_STREAM_CHUNK,
        chunk(0, "I'll run "),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_STREAM_CHUNK,
        chunk(0, "the tests."),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_TOOL_CALL,
        json!({
            "agentId": agent_id, "toolName": "run_tests", "toolKind": "terminal",
            "toolCallId": "call_abc", "input": { "path": "." }, "status": "started",
            "messageId": mid, "blockIndex": 1, "blockId": format!("{mid}:1"),
        }),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_TOOL_CALL,
        json!({
            "agentId": agent_id, "toolName": "run_tests", "toolKind": "terminal",
            "toolCallId": "call_abc", "input": { "path": "." }, "status": "completed",
            "output": "12 passed", "messageId": mid, "blockIndex": 1,
            "blockId": format!("{mid}:1"),
        }),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_STREAM_CHUNK,
        chunk(3, "Done."),
    )
    .await;

    // Persist the assistant message BEFORE stream:end (as run_prompt_turn does),
    // so the terminal reconcile re-reads the now-durable transcript.
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &mid,
            "assistant",
            &json!([
                { "type": "text", "id": format!("{mid}:0"), "text": "I'll run the tests." },
                { "type": "tool_use", "id": format!("{mid}:1"), "name": "run_tests",
                  "input": { "path": "." }, "toolCallId": "call_abc",
                  "metadata": { "toolKind": "terminal", "status": "completed" } },
                { "type": "tool_result", "id": format!("{mid}:2"), "tool_use_id": "call_abc",
                  "output": "12 passed", "is_error": false },
                { "type": "text", "id": format!("{mid}:3"), "text": "Done." },
            ]),
            &now_iso(),
        )
        .await
        .expect("append assistant message");
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_STREAM_END,
        json!({ "agentId": agent_id }),
    )
    .await;

    // Reduce every delta onto the snapshot until the terminal frame arrives.
    let mut expected_seq = 1u64;
    let mut saw_tool_result = false;
    let mut saw_text_growth = false;
    loop {
        let frame = read_json(&mut sub_reader).await;
        assert_eq!(frame["params"]["kind"], "delta");
        assert_eq!(
            frame["params"]["seq"].as_u64().unwrap(),
            expected_seq,
            "delta seq is monotonic from 1"
        );
        expected_seq += 1;
        let delta = frame["params"]["delta"].clone();
        if delta["updated"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|e| e["block"]["id"].as_str() == Some(format!("{mid}:0").as_str()))
        {
            saw_text_growth = true;
        }
        if delta["added"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|e| e["block"]["type"].as_str() == Some("tool_result"))
        {
            saw_tool_result = true;
        }
        apply_delta(&mut reconstructed, &delta);
        if is_terminal_delta(&delta) {
            break;
        }
    }
    assert!(saw_text_growth, "a text block grew via an updated delta");
    assert!(saw_tool_result, "a tool_result block was added");

    // The reduced state must equal a fresh getConversation snapshot exactly.
    let want = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        Value::Array(reconstructed),
        want["messages"],
        "snapshot + deltas reconcile to the fresh conversation snapshot"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
