//! WSS end-to-end for the workspace-context + task↔agent-linkage surface
//! (PROTOCOL §5.1 `workspace.getContext`/`updateContext`, §5.4
//! `task.linkAgent`/`unlinkAgent`/`listAgentLinks`, §6.5
//! `workspace:context-changed` / `task:agent-linked` /
//! `task:agent-unlinked`). Drives a real [`WsApiServer`] over plain `ws://`
//! (insecure dev mode, same pattern as `e2e_wss_sticky_reverse.rs`) so the
//! WebSocket-upgrade → JSON-RPC → router → services → store round-trip is
//! exercised end-to-end, matching the FE call sites in
//! `packages/cloudlands-fe/src/store/renderer/slices/context/` and
//! `.../task-agent-associations/`. TLS pinning, bearer auth, origin
//! allow-list and heartbeat are already covered end-to-end by the
//! spawned-daemon suites (see `e2e_wss_workspace_worktree.rs` /
//! `e2e_wss_agent_rehydration.rs`); the router path exercised here reuses
//! that shared WSS listener, so per-method suites do not re-verify the
//! TLS/auth handshake.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-ctxlink-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        _dir: TempDir(dir),
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Like [`wss_rpc`] but returns the full envelope so callers can assert on
/// `error.code` for negative-path tests (invalid params, etc.).
async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send");
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        assert!(
            !remaining.is_zero(),
            "wss_rpc timed out: id={id} method={method}"
        );
        let frame = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss_rpc read timeout id={id} method={method}"));
        match frame {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
                if v.get("id") == Some(&json!(id)) {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Read the next `events.event` notification frame from `ws`, echoing pings
/// and skipping unrelated `id`-carrying response frames.
async fn next_event(ws: &mut PlainWs) -> Value {
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        assert!(!remaining.is_zero(), "next_event timed out");
        let frame = timeout(remaining, ws.next())
            .await
            .expect("next_event read timeout");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
                if v.get("method") == Some(&json!("events.event")) {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// `workspace.getContext` starts empty; `workspace.updateContext` persists
/// the caller-supplied `ContextItem[]` verbatim (including provider-specific
/// extras) and pushes a `workspace:context-changed` event carrying the new
/// list. FE parity: matches
/// `packages/cloudlands-fe/src/store/renderer/slices/context/context-slice.ts`
/// hydrate/add/remove/update collapsed to a single authoritative-list write.
#[tokio::test]
async fn workspace_context_round_trip_and_event() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let mut sub = connect(fx.port).await;

    // Create a workspace so `workspace:*` scoping in the subscription works.
    let ws = wss_rpc(&mut rpc, 1, "workspace.create", json!({ "title": "Ctx" })).await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    // Subscribe to workspace-context events for this workspace.
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:context-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].as_str().is_some());

    // Empty initial state.
    let initial = wss_rpc(
        &mut rpc,
        3,
        "workspace.getContext",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(initial["items"], json!([]));

    // Write a list with a linear-issue and a note item, preserving order and
    // provider-specific extras (`identifier`, `noteId`).
    let items = json!([
        {
            "id": "ctx-linear-1",
            "type": "linear-issue",
            "provider": "linear",
            "title": "ENG-42: bug",
            "identifier": "ENG-42",
            "createdAt": "2026-07-13T00:00:00Z",
            "updatedAt": "2026-07-13T00:00:00Z"
        },
        {
            "id": "ctx-note-1",
            "type": "note",
            "provider": "internal",
            "title": "Design doc",
            "noteId": "note-abc",
            "createdAt": "2026-07-13T00:00:00Z",
            "updatedAt": "2026-07-13T00:00:00Z"
        }
    ]);
    let updated = wss_rpc(
        &mut rpc,
        4,
        "workspace.updateContext",
        json!({ "workspaceId": ws_id, "items": items }),
    )
    .await;
    let stored = updated["items"].as_array().expect("items array");
    assert_eq!(stored.len(), 2, "persisted list: {updated}");
    assert_eq!(stored[0]["id"], "ctx-linear-1");
    assert_eq!(stored[0]["identifier"], "ENG-42");
    assert_eq!(stored[1]["id"], "ctx-note-1");
    assert_eq!(stored[1]["noteId"], "note-abc");

    // The event carries the same list.
    let ev = next_event(&mut sub).await;
    assert_eq!(ev["params"]["event"]["type"], "workspace:context-changed");
    assert_eq!(ev["params"]["event"]["workspaceId"], ws_id.as_str());
    let ev_data = &ev["params"]["event"]["data"];
    assert_eq!(ev_data["workspaceId"], ws_id.as_str());
    assert_eq!(ev_data["items"].as_array().unwrap().len(), 2);
    assert_eq!(ev_data["items"][0]["id"], "ctx-linear-1");

    // Read-back returns the same list.
    let read = wss_rpc(
        &mut rpc,
        5,
        "workspace.getContext",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(read["items"], updated["items"]);

    // Replace with a shorter list — full-list replacement, not merge.
    let items2 = json!([{
        "id": "ctx-note-1",
        "type": "note",
        "provider": "internal",
        "title": "Design doc",
        "noteId": "note-abc",
        "createdAt": "2026-07-13T00:00:00Z",
        "updatedAt": "2026-07-13T00:00:00Z"
    }]);
    let updated2 = wss_rpc(
        &mut rpc,
        6,
        "workspace.updateContext",
        json!({ "workspaceId": ws_id, "items": items2 }),
    )
    .await;
    assert_eq!(updated2["items"].as_array().unwrap().len(), 1);
    let ev2 = next_event(&mut sub).await;
    assert_eq!(ev2["params"]["event"]["type"], "workspace:context-changed");
    assert_eq!(
        ev2["params"]["event"]["data"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

/// `task.linkAgent` / `unlinkAgent` / `listAgentLinks` migrate the
/// renderer-only `localStorage["task-agent-associations:{wsId}"]` map into
/// daemon-owned rows. FE parity: the response `linksByNoteId` mirrors
/// `TaskAgentAssociationsState.byNoteId → byTaskKey → association` so the FE
/// hydration is a mechanical cut-over.
#[tokio::test]
async fn task_agent_link_lifecycle_and_events() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let mut sub = connect(fx.port).await;

    let ws = wss_rpc(&mut rpc, 1, "workspace.create", json!({ "title": "Link" })).await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["task:agent-linked", "task:agent-unlinked"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].as_str().is_some());

    // Empty initial state.
    let initial = wss_rpc(
        &mut rpc,
        3,
        "task.listAgentLinks",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(initial["links"], json!([]));
    assert_eq!(initial["linksByNoteId"], json!({}));

    // Link two tasks in one note + one task in another. `taskKey` defaults
    // to `taskText` when omitted (FE parity with
    // `association.taskKey ?? association.taskText`).
    let a = wss_rpc(
        &mut rpc,
        4,
        "task.linkAgent",
        json!({
            "workspaceId": ws_id,
            "noteId": "spec",
            "taskText": "Ship it",
            "agentId": "agent-alpha",
        }),
    )
    .await;
    assert_eq!(a["link"]["taskKey"], "Ship it");
    assert_eq!(a["link"]["agentId"], "agent-alpha");
    assert!(a["link"]["createdAt"].is_i64());
    let ev = next_event(&mut sub).await;
    assert_eq!(ev["params"]["event"]["type"], "task:agent-linked");
    assert_eq!(ev["params"]["event"]["data"]["noteId"], "spec");
    assert_eq!(ev["params"]["event"]["data"]["taskKey"], "Ship it");
    assert_eq!(
        ev["params"]["event"]["data"]["link"]["agentId"],
        "agent-alpha"
    );

    let _b = wss_rpc(
        &mut rpc,
        5,
        "task.linkAgent",
        json!({
            "workspaceId": ws_id,
            "noteId": "spec",
            "taskKey": "agent:task-2",
            "taskText": "Fix bug",
            "agentId": "agent-beta",
        }),
    )
    .await;
    let _ = next_event(&mut sub).await;

    let _c = wss_rpc(
        &mut rpc,
        6,
        "task.linkAgent",
        json!({
            "workspaceId": ws_id,
            "noteId": "other-note",
            "taskText": "Write tests",
            "agentId": "agent-gamma",
        }),
    )
    .await;
    let _ = next_event(&mut sub).await;

    // list groups by noteId → taskKey (FE-parity shape).
    let listed = wss_rpc(
        &mut rpc,
        7,
        "task.listAgentLinks",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(listed["links"].as_array().unwrap().len(), 3);
    assert_eq!(
        listed["linksByNoteId"]["spec"]["Ship it"]["agentId"],
        "agent-alpha"
    );
    assert_eq!(
        listed["linksByNoteId"]["spec"]["agent:task-2"]["agentId"],
        "agent-beta"
    );
    assert_eq!(
        listed["linksByNoteId"]["other-note"]["Write tests"]["agentId"],
        "agent-gamma"
    );

    // Unlink one row emits `task:agent-unlinked` and shrinks the list.
    let unlinked = wss_rpc(
        &mut rpc,
        8,
        "task.unlinkAgent",
        json!({ "workspaceId": ws_id, "noteId": "spec", "taskKey": "Ship it" }),
    )
    .await;
    assert_eq!(unlinked["removed"], true);
    let ev = next_event(&mut sub).await;
    assert_eq!(ev["params"]["event"]["type"], "task:agent-unlinked");
    assert_eq!(ev["params"]["event"]["data"]["noteId"], "spec");
    assert_eq!(ev["params"]["event"]["data"]["taskKey"], "Ship it");

    // Unlinking an unknown row is a no-op (no event emitted).
    let noop = wss_rpc(
        &mut rpc,
        9,
        "task.unlinkAgent",
        json!({ "workspaceId": ws_id, "noteId": "spec", "taskKey": "never-existed" }),
    )
    .await;
    assert_eq!(noop["removed"], false);

    let final_list = wss_rpc(
        &mut rpc,
        10,
        "task.listAgentLinks",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(final_list["links"].as_array().unwrap().len(), 2);
    assert!(final_list["linksByNoteId"]["spec"].get("Ship it").is_none());
}

/// Negative path: `workspace.updateContext` rejects duplicate item ids with
/// `-32602 Invalid params` (the store rows are keyed by
/// `(workspace_id, id)`, so the router validates up front to keep the error
/// contract clean instead of surfacing a store constraint violation).
#[tokio::test]
async fn workspace_update_context_rejects_duplicate_ids() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let ws = wss_rpc(&mut rpc, 1, "workspace.create", json!({ "title": "Dup" })).await;
    let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();

    let dup = json!([
        { "id": "dup", "type": "note", "provider": "internal" },
        { "id": "dup", "type": "note", "provider": "internal" },
    ]);
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.updateContext",
        json!({ "workspaceId": ws_id, "items": dup }),
    )
    .await;
    let err = resp.get("error").expect("duplicate-id must error");
    assert_eq!(err["code"], -32602, "invalid params: {resp}");
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("duplicate") && msg.contains("dup"),
        "duplicate-id message: {msg}"
    );
}
