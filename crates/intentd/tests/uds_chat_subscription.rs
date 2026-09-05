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

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::{AGENT_STREAM_END, AGENT_TOOL_CALL, CHAT_STREAM_DELTA};
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

fn boot(
    bus: &EventBus,
) -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    Arc<Services>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
    let socket = sock_dir.path().join("uds.sock");
    // Keep a typed handle so tests can drive the live-turn slot directly (the
    // server is handed the same handle coerced to `Arc<dyn WorkspaceApi>`).
    let ws_root = common::hermetic_workspaces_root();
    let services = Arc::new(
        Services::new(bus.store().clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
            .with_event_bus(bus.clone()),
    );
    let api: Arc<dyn intent_core::WorkspaceApi> = services.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(api, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });
    (socket, server, shutdown_tx, services, ws_root, sock_dir)
}

/// Create a workspace + agent on a fresh control connection; return `(socket,
/// server, shutdown_tx, ws_id, agent_id)` plus the live rpc connection halves.
async fn setup() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    TempDb,
    Arc<Services>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, services, ws_root, sock_dir) = boot(&bus);
    (
        socket,
        server,
        shutdown_tx,
        tmp,
        services,
        ws_root,
        sock_dir,
    )
}

/// Like [`setup`] but also returns the live [`EventBus`] so a test can persist
/// messages and publish `agent:stream:*` events that drive the chat forwarder.
async fn setup_with_bus() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    TempDb,
    EventBus,
    Arc<Services>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store);
    let (socket, server, shutdown_tx, services, ws_root, sock_dir) = boot(&bus);
    (
        socket,
        server,
        shutdown_tx,
        tmp,
        bus,
        services,
        ws_root,
        sock_dir,
    )
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
    // Re-read entities (`agent:message` rows and the terminal reconcile) lift
    // the persisted row's `metadata`; a client applies it to the message
    // envelope so interrupted / finish-reason state renders without a refetch.
    if let Some(v) = entity.get("metadata") {
        msg["metadata"] = v.clone();
    }
    // The terminal reconcile (`streamingComplete: true`) flips an in-flight
    // message to its durable form: a client drops the transient `isStreaming`
    // render hint the mid-turn snapshot carried so it converges to the persisted
    // (non-streaming) message.
    if entity.get("streamingComplete") == Some(&Value::Bool(true)) {
        if let Some(obj) = msg.as_object_mut() {
            obj.remove("isStreaming");
        }
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
    let (socket, server, shutdown_tx, _tmp, _services, _ws_root, _sock_dir) = setup().await;
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
    // seq-0 snapshot equals the agent's newest conversation page plus the
    // daemon-owned activity flags (PROTOCOL §7.1; all false for an idle agent).
    let mut want = want;
    let want_obj = want.as_object_mut().unwrap();
    want_obj.insert("isResponding".into(), json!(false));
    want_obj.insert("isWaitingOnTool".into(), json!(false));
    want_obj.insert("isWaitingForOtherAgents".into(), json!(false));
    want_obj.insert("waitingForAgentIds".into(), json!([]));
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
    let (socket, server, shutdown_tx, _tmp, _services, _ws_root, _sock_dir) = setup().await;
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
    let (socket, server, shutdown_tx, _tmp, _services, _ws_root, _sock_dir) = setup().await;
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
/// `chat:stream:delta`/`tool:call`/`stream:end` deltas equals a fresh
/// `agent.getConversation` snapshot. Drives a mock turn — two text chunks that
/// coalesce onto one block (added → updated), a tool call (`tool_use` → `tool_use`
/// updated + `tool_result` added), then trailing text — persists the assistant
/// message exactly as `run_prompt_turn` would, and finally emits `stream:end`.
#[tokio::test]
async fn chat_delta_stream_reconciles_with_fresh_snapshot() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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
            None,
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
        CHAT_STREAM_DELTA,
        chunk(0, "I'll run "),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
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
            "blockId": format!("{mid}:1"), "resultBlockIndex": 2,
            "resultBlockId": format!("{mid}:2"),
        }),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        chunk(3, "Done."),
    )
    .await;

    // Persist the assistant message BEFORE stream:end (as run_prompt_turn does),
    // so the terminal reconcile re-reads the now-durable transcript. The row
    // carries interrupted-turn metadata (intent#4409): the terminal frame must
    // lift it so the reduced state matches `agent.getConversation` exactly.
    let row_metadata = json!({
        "interrupted": true,
        "stopReason": "interrupted",
        "interruptReason": "user_stop",
    });
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
            Some(&row_metadata),
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
            for e in ["added", "updated"]
                .iter()
                .flat_map(|k| delta[*k].as_array().into_iter().flatten())
            {
                assert_eq!(
                    e["metadata"], row_metadata,
                    "every terminal entity carries the persisted row metadata: {delta}"
                );
            }
            break;
        }
        for e in ["added", "updated"]
            .iter()
            .flat_map(|k| delta[*k].as_array().into_iter().flatten())
        {
            assert!(
                e.get("metadata").is_none(),
                "mid-turn live entities carry no row metadata: {delta}"
            );
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

/// CS-4 mid-turn resume (CS-0 D5): a `chat.subscribe` arriving WHILE a turn is
/// streaming receives a coherent snapshot — the persisted messages PLUS the
/// in-flight partial assistant message (`isStreaming: true`) reconstructed from
/// the live-turn slot. Its first continuing chunk carries the FULL accumulated
/// text (proving the delta state was seeded from the snapshot), and the
/// snapshot + deltas reconcile to a fresh `agent.getConversation` snapshot.
#[tokio::test]
async fn chat_mid_turn_resume_snapshot_includes_in_flight_then_reconciles() {
    let (socket, server, shutdown_tx, _tmp, bus, services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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
    let agent = AgentId::from(agent_id.as_str());

    // A persisted user message (seq 0): the durable part of the snapshot.
    let store = bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &agent,
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("append user message");

    // Simulate a turn ALREADY mid-flight: the assistant has streamed "I'll run "
    // into block {mid}:0 but nothing is persisted yet (run_prompt_turn drives this
    // slot via begin_live_turn/update_live_turn). `chat_snapshot` gates the
    // live-turn merge on `agent_is_busy`, so the test must also claim the
    // busy slot the same way `run_prompt_turn`'s `try_begin` would.
    let mid = Uuid::now_v7().to_string();
    services.set_live_turn(
        &agent,
        &mid,
        vec![json!({ "type": "text", "id": format!("{mid}:0"), "text": "I'll run " })],
    );
    services.set_test_busy(&agent, true);

    // Subscribe mid-turn: the snapshot must merge the in-flight message.
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
    assert_eq!(snap["params"]["snapshot"]["totalMessages"], 2);
    // STAB-125: the flags overlay carries turn-liveness — a mid-turn snapshot
    // reports the open live-turn slot and its last stream-activity stamp.
    assert_eq!(snap["params"]["snapshot"]["turnInFlight"], true);
    assert!(
        snap["params"]["snapshot"]["lastStreamActivityAt"].is_string(),
        "mid-turn snapshot carries lastStreamActivityAt: {}",
        snap["params"]["snapshot"]
    );
    let mut reconstructed: Vec<Value> = snap["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");
    assert_eq!(
        reconstructed.len(),
        2,
        "snapshot merges user + in-flight assistant message"
    );
    let inflight = &reconstructed[1];
    assert_eq!(inflight["id"], mid.as_str());
    assert_eq!(inflight["isStreaming"], true);
    assert_eq!(
        inflight["seq"], 1,
        "in-flight seq is the next monotonic value"
    );
    assert_eq!(inflight["role"], "assistant");
    assert_eq!(inflight["contentBlocks"][0]["text"], "I'll run ");

    // Continue the turn over the bus.
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
        CHAT_STREAM_DELTA,
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
            "blockId": format!("{mid}:1"), "resultBlockIndex": 2,
            "resultBlockId": format!("{mid}:2"),
        }),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        chunk(3, "Done."),
    )
    .await;

    // Persist the full assistant message + clear the slot (as run_prompt_turn
    // does), then emit the terminal stream:end.
    store
        .append_agent_message_with_id(
            &agent,
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
            None,
            &now_iso(),
        )
        .await
        .expect("append assistant message");
    services.clear_live_turn(&agent);
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_STREAM_END,
        json!({ "agentId": agent_id }),
    )
    .await;

    // Reduce deltas; the resumed text block's delta must carry the FULL text.
    let mut resumed_full_text = false;
    loop {
        let frame = read_json(&mut sub_reader).await;
        assert_eq!(frame["params"]["kind"], "delta");
        let delta = frame["params"]["delta"].clone();
        for e in delta["updated"].as_array().into_iter().flatten() {
            if e["block"]["id"].as_str() == Some(format!("{mid}:0").as_str())
                && e["block"]["text"].as_str() == Some("I'll run the tests.")
            {
                resumed_full_text = true;
            }
        }
        apply_delta(&mut reconstructed, &delta);
        if is_terminal_delta(&delta) {
            break;
        }
    }
    assert!(
        resumed_full_text,
        "the resumed text delta carried the full accumulated text, not just the new fragment"
    );

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
        "mid-turn snapshot + deltas reconcile to the fresh conversation snapshot"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// monorepo#2104 — the end-to-end shape of the orphan-slot rule, deliberately
/// superseding the Iter#1c heal-gate assertion this test used to make (that a
/// live-turn slot with no busy claim is not merged AT ALL). The objection Iter#1c
/// encoded was to labelling orphan content "streaming", not to showing it: a
/// mid-turn crash or a flush that failed and kept the slot leaves real streamed
/// output in the daemon that no snapshot could otherwise ever show. So the slot
/// is merged and `agent_is_busy` only decides the flag — over the wire, the
/// orphan arrives as a NON-streaming message, and the STAB-125 turn-liveness
/// fields stay gated on the busy claim, so nothing claims a turn is in flight.
#[tokio::test]
async fn chat_snapshot_serves_an_orphan_live_turn_as_a_non_streaming_message() {
    let (socket, server, shutdown_tx, _tmp, bus, services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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
    let agent = AgentId::from(agent_id.as_str());

    // A persisted user message (seq 0) — the durable page.
    let store = bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &agent,
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("append user message");

    // A lingering live-turn slot with NO busy claim — the orphan shape: real
    // streamed content the daemon still holds, with nothing coming for it.
    let mid = Uuid::now_v7().to_string();
    services.set_live_turn(
        &agent,
        &mid,
        vec![json!({ "type": "text", "id": format!("{mid}:0"), "text": "I'll run " })],
    );

    // Subscribe and capture the seq-0 snapshot.
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
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(
        snap["params"]["snapshot"]["totalMessages"], 2,
        "the orphan slot's content is served, and counted so seq stays contiguous"
    );
    let messages = snap["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");
    assert_eq!(
        messages.len(),
        2,
        "the user message plus the orphan content"
    );
    assert_eq!(messages[0]["id"], user_id.as_str());
    assert!(
        messages[0].get("isStreaming").is_none()
            || messages[0]["isStreaming"] == Value::Bool(false),
        "no streaming flag on the durable user message"
    );
    assert_eq!(messages[1]["id"], mid.as_str());
    assert_eq!(
        messages[1]["contentBlocks"][0]["text"], "I'll run ",
        "…with the streamed-so-far blocks intact: {snap}"
    );
    assert_eq!(
        messages[1]["isStreaming"],
        Value::Bool(false),
        "an orphaned slot must never claim to be streaming: {snap}"
    );
    // STAB-125: the orphan slot must not report a phantom in-flight turn
    // either — liveness stays gated on the busy claim.
    assert_eq!(snap["params"]["snapshot"]["turnInFlight"], false);
    assert!(snap["params"]["snapshot"]["lastStreamActivityAt"].is_null());

    // Claiming the busy slot flips the SAME merged message to streaming:
    // re-subscribing on a fresh connection sees it flagged in-flight.
    services.set_test_busy(&agent, true);
    let (sub2_read, mut sub2_write) = connect_retry(&socket).await.into_split();
    let mut sub2_reader = tokio::io::BufReader::new(sub2_read);
    send(
        &mut sub2_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 2, "method": "chat.subscribe",
            "params": { "agentId": agent_id }
        }))
        .unwrap(),
    )
    .await;
    let _resp2 = read_json(&mut sub2_reader).await;
    let snap2 = read_json(&mut sub2_reader).await;
    assert_eq!(snap2["params"]["snapshot"]["totalMessages"], 2);
    let messages2 = snap2["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");
    assert_eq!(messages2.len(), 2);
    assert_eq!(messages2[1]["id"], mid.as_str());
    assert_eq!(messages2[1]["isStreaming"], true);
    // With the busy claim in place the turn-liveness fields go live too.
    assert_eq!(snap2["params"]["snapshot"]["turnInFlight"], true);
    assert!(snap2["params"]["snapshot"]["lastStreamActivityAt"].is_string());

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// CS-4 cross-agent isolation: a `chat.subscribe` for agent A must NOT receive
/// agent B's `agent:stream:*` events — the forwarder filters on
/// `sessionId == agentId`. B's chunk is published first (and dropped); the next
/// (and only) delta A's subscription sees is A's own chunk.
#[tokio::test]
async fn chat_subscription_isolates_stream_across_agents() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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

    // Subscribe chat to agent A only.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe",
            "params": { "agentId": a_id }
        }))
        .unwrap(),
    )
    .await;
    let _resp = read_json(&mut sub_reader).await;
    let _snap = read_json(&mut sub_reader).await;

    // Publish a chunk for B FIRST (it must be filtered out), then one for A.
    let b_mid = Uuid::now_v7().to_string();
    publish_stream(
        &bus,
        &ws_id,
        &b_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": b_id, "content": "secret from B", "messageId": b_mid,
            "blockIndex": 0, "blockId": format!("{b_mid}:0"), "blockType": "text",
        }),
    )
    .await;
    let a_mid = Uuid::now_v7().to_string();
    publish_stream(
        &bus,
        &ws_id,
        &a_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": a_id, "content": "hello A", "messageId": a_mid,
            "blockIndex": 0, "blockId": format!("{a_mid}:0"), "blockType": "text",
        }),
    )
    .await;

    // The next delta A receives is A's own — B's (published first) was filtered.
    let frame = read_json(&mut sub_reader).await;
    assert_eq!(frame["params"]["kind"], "delta");
    let entity = &frame["params"]["delta"]["added"][0];
    assert_eq!(entity["agentId"], a_id.as_str());
    assert_eq!(entity["messageId"], a_mid.as_str());
    assert_eq!(entity["block"]["text"], "hello A");

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// CS-4 firehose coexistence (Risk R1): the legacy `events.subscribe` firehose
/// receives the content-free `agent:stream:activity` signal WHILE a
/// `chat.subscribe` receives the block delta mapped from `chat:stream:delta`
/// for the SAME turn — both fire for one chunk (the emit path publishes the
/// delta plus, on the throttle's leading edge, the activity signal).
#[tokio::test]
async fn chat_subscription_coexists_with_events_firehose() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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
    let a_id = a["agent"]["id"].as_str().unwrap().to_string();

    // The legacy firehose still works: a separate connection subscribes to the
    // agent event family via events.subscribe.
    let (fh_read, mut fh_write) = connect_retry(&socket).await.into_split();
    let mut fh_reader = tokio::io::BufReader::new(fh_read);
    send(
        &mut fh_write,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{{"eventTypes":["agent:*"],"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let _ = read_json(&mut fh_reader).await; // subscribe ack

    // A chat subscription for the same agent.
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = tokio::io::BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe",
            "params": { "agentId": a_id }
        }))
        .unwrap(),
    )
    .await;
    let _resp = read_json(&mut sub_reader).await;
    let _snap = read_json(&mut sub_reader).await;

    // Publish a single chunk for A as the emit path does: the content-bearing
    // chat delta plus (leading edge of the throttle) the content-free
    // activity signal.
    let mid = Uuid::now_v7().to_string();
    publish_stream(
        &bus,
        &ws_id,
        &a_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": a_id, "content": "hello", "messageId": mid,
            "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
        }),
    )
    .await;
    publish_stream(
        &bus,
        &ws_id,
        &a_id,
        intent_core::events::AGENT_STREAM_ACTIVITY,
        json!({ "agentId": a_id, "messageId": mid }),
    )
    .await;

    // The firehose sees the content-free activity signal (the chat delta is
    // outside the `agent:*` family, so the firehose never receives content).
    let fh = read_json(&mut fh_reader).await;
    assert_eq!(fh["method"], "events.event");
    assert_eq!(fh["params"]["event"]["type"], "agent:stream:activity");
    assert_eq!(fh["params"]["event"]["data"]["agentId"], a_id.as_str());
    assert_eq!(fh["params"]["event"]["data"]["messageId"], mid.as_str());
    assert!(
        fh["params"]["event"]["data"].get("content").is_none(),
        "activity payload is content-free"
    );

    // The chat subscription sees the mapped block delta for the same chunk.
    let delta = read_json(&mut sub_reader).await;
    assert_eq!(delta["params"]["kind"], "delta");
    assert_eq!(
        delta["params"]["delta"]["added"][0]["block"]["text"],
        "hello"
    );
    assert_eq!(
        delta["params"]["delta"]["added"][0]["block"]["id"],
        format!("{mid}:0")
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// CS-5 orphan self-heal: a turn where TEXT INTERLEAVES AFTER a tool call and a
/// trailing partial text block (`{mid}:4`) the model streamed live but the
/// durable turn dropped — it exists in no persisted block, so the terminal
/// reconcile lists it in a NON-EMPTY `removedIds`. The assertion is the
/// reconciliation invariant: the seq-0 snapshot reduced with every delta
/// (HONORING `removedIds`) equals a fresh `agent.getConversation` snapshot, and
/// the terminal `removedIds` is non-empty.
///
/// The `tool_result` id is NOT part of the divergence any more (monorepo#2029):
/// the completion event carries the real `resultBlockId` (`{mid}:3`, what
/// `record_tool` assigned after flushing the interleaved text into `{mid}:2`),
/// so the live mapper stamps it verbatim instead of predicting `tool_use + 1`
/// and clobbering the interleaved text block for the rest of the turn. This
/// test asserts that too — `{mid}:2` must never be emitted as a `tool_result` —
/// while still exercising the genuine-orphan self-heal path via `{mid}:4`.
#[tokio::test]
async fn chat_delta_orphaned_block_reconciles_via_nonempty_removed_ids() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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

    // A persisted user message anchors a non-trivial seq-0 snapshot.
    let store = bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("append user message");

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

    let mid = Uuid::now_v7().to_string();
    let chunk = |idx: u64, text: &str| {
        json!({
            "agentId": agent_id, "content": text, "messageId": mid,
            "blockIndex": idx, "blockId": format!("{mid}:{idx}"), "blockType": "text",
        })
    };
    // 1) Opening text → {mid}:0.
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        chunk(0, "I'll run the tests. "),
    )
    .await;
    // 2) Tool starts → tool_use {mid}:1.
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
    // 3) TEXT INTERLEAVES AFTER the tool call → live text {mid}:2.
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        chunk(2, "Checking output. "),
    )
    .await;
    // 4) Tool completes WITH output. The event carries the REAL result id the
    //    durable transcript assigned ({mid}:3 — the interleaved text took
    //    {mid}:2), so the mapper stamps it instead of predicting {mid}:2.
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        AGENT_TOOL_CALL,
        json!({
            "agentId": agent_id, "toolName": "run_tests", "toolKind": "terminal",
            "toolCallId": "call_abc", "input": { "path": "." }, "status": "completed",
            "output": "12 passed", "messageId": mid, "blockIndex": 1,
            "blockId": format!("{mid}:1"), "resultBlockIndex": 3,
            "resultBlockId": format!("{mid}:3"),
        }),
    )
    .await;
    // 5) A trailing partial the model streamed but the durable turn drops → {mid}:4.
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        chunk(4, "Let me also "),
    )
    .await;

    // The DURABLE transcript (what run_prompt_turn persists): the interleaved text
    // landed at {mid}:2 and the real tool_result at {mid}:3; the trailing partial
    // {mid}:4 is NOT committed. {mid}:4 therefore orphans the live state.
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &mid,
            "assistant",
            &json!([
                { "type": "text", "id": format!("{mid}:0"), "text": "I'll run the tests. " },
                { "type": "tool_use", "id": format!("{mid}:1"), "name": "run_tests",
                  "input": { "path": "." }, "toolCallId": "call_abc",
                  "metadata": { "toolKind": "terminal", "status": "completed" } },
                { "type": "text", "id": format!("{mid}:2"), "text": "Checking output. " },
                { "type": "tool_result", "id": format!("{mid}:3"), "tool_use_id": "call_abc",
                  "output": "12 passed", "is_error": false },
            ]),
            None,
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

    // Reduce every delta; capture the terminal frame's removedIds.
    let mut terminal_removed: Vec<String> = Vec::new();
    loop {
        let frame = read_json(&mut sub_reader).await;
        assert_eq!(frame["params"]["kind"], "delta");
        let delta = frame["params"]["delta"].clone();
        // monorepo#2029: no live delta may stamp a `tool_result` onto the
        // interleaved text block's id — the completion event named the real
        // {mid}:3 and the mapper must not derive {mid}:2 from the tool_use.
        for key in ["added", "updated"] {
            for entity in delta[key].as_array().into_iter().flatten() {
                let block = &entity["block"];
                assert!(
                    !(block["id"] == json!(format!("{mid}:2"))
                        && block["type"] == json!("tool_result")),
                    "the interleaved text block {{mid}}:2 is never overwritten live: {entity}"
                );
            }
        }
        let terminal = is_terminal_delta(&delta);
        if terminal {
            terminal_removed = delta["removedIds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
        }
        apply_delta(&mut reconstructed, &delta);
        if terminal {
            break;
        }
    }

    // The orphaned trailing partial drives a NON-EMPTY removedIds (the prior tests
    // only ever saw empty removedIds).
    assert!(
        !terminal_removed.is_empty(),
        "terminal reconcile emits a non-empty removedIds for the orphaned block"
    );
    assert!(
        terminal_removed.contains(&format!("{mid}:4")),
        "the orphaned trailing partial {{mid}}:4 is in removedIds, got {terminal_removed:?}"
    );

    // Honoring removedIds, the reduced state equals a fresh snapshot exactly.
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
        "snapshot + deltas (honoring removedIds) reconcile to the fresh conversation snapshot"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// monorepo#958: the seq-0 snapshot for a LARGE transcript is the bounded
/// newest `agent.getConversation` page — not a re-hydration of the full
/// history. With 120 persisted messages the snapshot carries exactly the
/// newest 50 (the server default page), `truncated: true`,
/// `totalMessages: 120`, and a non-null `nextToken` so older pages stay
/// client-pulled via `agent.getConversation { nextToken }`.
#[tokio::test]
async fn chat_subscribe_snapshot_is_bounded_for_large_transcript() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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
    let agent = AgentId::from(agent_id.as_str());

    // A 120-message transcript — well past the 50-message default page.
    let store = bus.store();
    for i in 0..120 {
        let mid = Uuid::now_v7().to_string();
        let (role, text) = if i % 2 == 0 {
            ("user", format!("prompt {i}"))
        } else {
            ("assistant", format!("reply {i}"))
        };
        store
            .append_agent_message_with_id(
                &agent,
                &mid,
                role,
                &json!([{ "type": "text", "id": format!("{mid}:0"), "text": text }]),
                None,
                &now_iso(),
            )
            .await
            .expect("append message");
    }

    // The expected snapshot is exactly the newest bounded page.
    let want = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(want["messages"].as_array().unwrap().len(), 50);

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
    assert!(resp["result"]["subscriptionId"].as_str().is_some());
    let snap = read_json(&mut sub_reader).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);

    let snapshot = &snap["params"]["snapshot"];
    let messages = snapshot["messages"].as_array().expect("snapshot messages");
    assert_eq!(
        messages.len(),
        50,
        "snapshot is the bounded default page, not the full 120-message history"
    );
    // The page is the NEWEST 50 (seq 70..=119, oldest→newest within the page).
    assert_eq!(messages[0]["seq"], 70);
    assert_eq!(messages[49]["seq"], 119);
    assert_eq!(snapshot["truncated"], true);
    assert_eq!(snapshot["totalMessages"], 120);
    assert!(
        snapshot["nextToken"].as_str().is_some(),
        "a truncated snapshot carries the cursor for the older pages"
    );

    // Byte-for-byte: the bounded page + the daemon-owned activity flags — the
    // same shape as the small-transcript snapshot (PROTOCOL §7.1).
    let mut want = want;
    let want_obj = want.as_object_mut().unwrap();
    want_obj.insert("isResponding".into(), json!(false));
    want_obj.insert("isWaitingOnTool".into(), json!(false));
    want_obj.insert("isWaitingForOtherAgents".into(), json!(false));
    want_obj.insert("waitingForAgentIds".into(), json!([]));
    assert_eq!(snap["params"]["snapshot"], want);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}

/// Lag self-heal: a broadcast-ring drop that swallows the turn's tail (the
/// trailing chunk delta AND `agent:stream:end`) must not strand the transcript
/// mid-turn. The forwarder sees the in-band lag marker and re-emits a fresh
/// bounded snapshot at the next seq — the client converges to the persisted
/// message (all blocks, no `isStreaming`) with no seq gap and no resubscribe —
/// and the subscription stays live for the next turn's deltas.
///
/// The drop is forced deterministically: on the test's current-thread runtime a
/// non-yielding `publish_transient` loop starves the delivery task, so the ring
/// (capacity 1024) drops the oldest undelivered events — the tail published
/// first — before the task ever runs.
#[tokio::test]
async fn chat_subscription_self_heals_after_broadcast_lag_drops_turn_tail() {
    let (socket, server, shutdown_tx, _tmp, bus, _services, _ws_root, _sock_dir) =
        setup_with_bus().await;
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

    // A persisted user message anchors the seq-0 snapshot.
    let store = bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("append user message");

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
    assert!(resp["result"]["subscriptionId"].as_str().is_some());
    let snap = read_json(&mut sub_reader).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);

    // The turn starts normally: the first chunk arrives as delta seq 1.
    let mid = Uuid::now_v7().to_string();
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": agent_id, "content": "I'll run ", "messageId": mid,
            "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
        }),
    )
    .await;
    let first = read_json(&mut sub_reader).await;
    assert_eq!(first["params"]["kind"], "delta");
    assert_eq!(first["params"]["seq"], 1);

    // The turn completes durably (as run_prompt_turn persists before
    // `stream:end`), but its live tail is LOST: the trailing chunk and
    // `stream:end` are published into the ring and then buried under a
    // non-yielding flood that overflows the ring before the delivery task
    // can drain — exactly the slow-consumer drop of the incident.
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &mid,
            "assistant",
            &json!([
                { "type": "text", "id": format!("{mid}:0"), "text": "I'll run the tests." },
            ]),
            None,
            &now_iso(),
        )
        .await
        .expect("append assistant message");
    let stream_event = |event_type: &str, data: Value| NewEvent {
        workspace_id: WorkspaceId::from(ws_id.as_str()),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(agent_id.clone()),
            ..Default::default()
        },
        session_id: Some(agent_id.clone()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    };
    let _ = bus.publish_transient(&stream_event(
        CHAT_STREAM_DELTA,
        json!({
            "agentId": agent_id, "content": "the tests.", "messageId": mid,
            "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
        }),
    ));
    let _ = bus.publish_transient(&stream_event(
        AGENT_STREAM_END,
        json!({ "agentId": agent_id }),
    ));
    // 2048 filler events push the ring (capacity 1024) far past the tail.
    for _ in 0..2048 {
        let _ = bus.publish_transient(&NewEvent {
            workspace_id: WorkspaceId::from(ws_id.as_str()),
            timestamp: now_iso(),
            event_type: "note:created".to_string(),
            actor: EventActor {
                actor_type: ActorType::User,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({}),
        });
    }

    // Self-heal: the next frame is a fresh snapshot at the next seq (no gap),
    // carrying the fully persisted transcript with no in-flight message.
    let recovery = read_json(&mut sub_reader).await;
    assert_eq!(
        recovery["params"]["kind"], "snapshot",
        "lag recovery re-emits a snapshot, got: {recovery}"
    );
    assert_eq!(recovery["params"]["seq"], 2, "recovery takes the next seq");
    let want = rpc(
        &mut rpc_write,
        &mut rpc_reader,
        12,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let mut want = want;
    let want_obj = want.as_object_mut().unwrap();
    want_obj.insert("isResponding".into(), json!(false));
    want_obj.insert("isWaitingOnTool".into(), json!(false));
    want_obj.insert("isWaitingForOtherAgents".into(), json!(false));
    want_obj.insert("waitingForAgentIds".into(), json!([]));
    assert_eq!(
        recovery["params"]["snapshot"], want,
        "recovery snapshot equals a fresh getConversation page"
    );
    let messages = recovery["params"]["snapshot"]["messages"]
        .as_array()
        .unwrap();
    assert_eq!(messages.len(), 2, "user + persisted assistant message");
    assert!(
        messages.iter().all(|m| m.get("isStreaming").is_none()),
        "the recovered transcript is not stranded mid-turn"
    );

    // The subscription stays live: the next turn's chunk arrives as a delta
    // at the following seq.
    let mid2 = Uuid::now_v7().to_string();
    publish_stream(
        &bus,
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": agent_id, "content": "Next", "messageId": mid2,
            "blockIndex": 0, "blockId": format!("{mid2}:0"), "blockType": "text",
        }),
    )
    .await;
    let next = read_json(&mut sub_reader).await;
    assert_eq!(next["params"]["kind"], "delta");
    assert_eq!(next["params"]["seq"], 3);

    let _ = shutdown_tx.send(());
    let _ = server.await;
}
