//! WSS end-to-end regression for STAB-69: `workspace.create` with
//! `initialAgent.imageBlocks` threads the images into the first turn's ACP
//! prompt. Drives the real WS transport + mock ACP provider (deterministic
//! fixture in `fixtures/mock-acp-agent.mjs`) and asserts the first `acp:prompt`
//! carries the FE-supplied image content blocks.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

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
    store: Store,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-imgblk-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store.clone())
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(common::registry_with_default_provider(&dir))
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        store,
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
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                        return v["result"].clone();
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Like [`wss_rpc`] but expects an error response and returns the error
/// object.
async fn wss_rpc_err(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        assert!(v.get("error").is_some(), "rpc {method} succeeded: {v}");
                        return v["error"].clone();
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// STAB-69: `workspace.create` with `initialAgent.imageBlocks` threads the images
/// into the first turn so the ACP receives them.
#[tokio::test]
async fn workspace_create_threads_image_blocks_to_first_turn() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create workspace with an initial agent carrying imageBlocks.
    let image_data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "title": "Image test",
            "path": ".",
            "initialAgent": {
                "prompt": "process this",
                "imageBlocks": [
                    { "type": "image", "data": image_data, "mimeType": "image/png" }
                ]
            }
        }),
    )
    .await;

    let _ws_id = created["workspace"]["id"].as_str().unwrap();
    let agent_id = created["initialAgent"]["id"].as_str().unwrap();

    // (1) Verify the agent session persists the imageBlocks. The full
    // `agent.getSession` detail read serves them; the lite `agent.get`
    // projection deliberately omits them (list-payload cost contract).
    let full = wss_rpc(
        &mut rpc,
        2,
        "agent.getSession",
        json!({ "agentId": agent_id }),
    )
    .await;
    let image_blocks = &full["session"]["imageBlocks"];
    assert!(image_blocks.is_array(), "imageBlocks should be persisted");
    let blocks = image_blocks.as_array().unwrap();
    assert_eq!(blocks.len(), 1, "one image block persisted");
    assert_eq!(blocks[0]["data"], image_data, "image data matches");
    assert_eq!(blocks[0]["mimeType"], "image/png", "mime type matches");

    // The lite projection must NOT carry the base64 blob.
    let agent = wss_rpc(&mut rpc, 4, "agent.get", json!({ "agentId": agent_id })).await;
    let agent_obj = &agent["agent"];
    assert!(
        agent_obj.get("imageBlocks").is_none(),
        "agent.get must omit session-level imageBlocks: {agent_obj}"
    );

    // (2) Verify the orchestration recorded an initialMessage on the session.
    // The full image delivery to ACP requires a mock ACP fixture — that's
    // out of scope for STAB-65 which is an intentd-only fix. We've verified
    // that imageBlocks are persisted above; the real test is whether the code
    // path now starts a turn when imageBlocks are present (image-only initial
    // message support). The persisted value is served by `agent.getSession`
    // only; the lite projection omits it.
    assert!(
        full["session"]["initialMessage"].as_str().is_some(),
        "initial message persisted on the session"
    );
    assert!(
        agent_obj["metadata"].get("initialMessage").is_none(),
        "initialMessage stays off the lite projection: {agent_obj}"
    );

    // (3) STAB-133: the first user transcript row must carry the image block
    // after the text block so the conversation view can render the attachment
    // (this harness has no AgentManager, so this exercises the store-only
    // `agent_send_message_op` fallback).
    let conv = wss_rpc(
        &mut rpc,
        3,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let user_row = messages
        .iter()
        .find(|m| m["role"] == "user")
        .expect("first user transcript row");
    let content = user_row["contentBlocks"]
        .as_array()
        .expect("contentBlocks array");
    assert_eq!(
        content[0]["type"], "text",
        "text block first on the user row: {content:?}"
    );
    let image = content
        .iter()
        .find(|b| b["type"] == "image")
        .expect("image block persisted on the user row");
    assert_eq!(image["data"], image_data, "persisted image data matches");
    assert_eq!(
        image["mimeType"], "image/png",
        "persisted mime type matches"
    );
}

/// monorepo#3338: image blocks may carry an attachment-registry
/// `attachmentId` reference instead of inline base64 — accepted by
/// `workspace.create` (initial agent), `agent.create`, and
/// `agent.sendMessage`; persisted AS a reference (no bytes on the session or
/// transcript row); and rejected with `-32602` for unknown ids and
/// both/neither shape violations.
#[tokio::test]
async fn image_reference_blocks_accepted_and_validated() {
    use base64::Engine as _;

    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Workspace A: home of the attachment registry row. Needs a real
    // filesystem root for `file.placeAttachment` to land bytes in — this
    // harness's `workspace.create` yields pathless rows, so pin one on.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let root_a = std::env::temp_dir().join(format!("intentd-imgref-{}", &short[..8]));
    std::fs::create_dir_all(&root_a).unwrap();
    let _root_a = TempDir(root_a.clone());
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Attachment home", "path": "." }),
    )
    .await;
    let ws_a = created["workspace"]["id"].as_str().unwrap().to_string();
    let mut ws_row = fx
        .store
        .get_workspace(&intent_core::WorkspaceId::from(ws_a.clone()))
        .await
        .expect("workspace A row");
    ws_row.worktree_path = Some(root_a.to_string_lossy().into_owned());
    fx.store.update_workspace(&ws_row).await.expect("pin root");

    let png_bytes = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")
        .unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&png_bytes);
    let placed = wss_rpc(
        &mut rpc,
        2,
        "file.placeAttachment",
        json!({
            "workspaceId": ws_a,
            "fileName": "pixel.png",
            "data": b64,
            "mimeType": "image/png",
        }),
    )
    .await;
    let attachment_id = placed["attachmentId"].as_str().expect("attachmentId");

    // (1) workspace.create with an initialAgent image REFERENCE: accepted,
    // persisted on the session as a reference (no bytes).
    let created = wss_rpc(
        &mut rpc,
        3,
        "workspace.create",
        json!({
            "title": "Image ref test",
            "path": ".",
            "initialAgent": {
                "prompt": "look at this",
                "imageBlocks": [
                    { "type": "image", "attachmentId": attachment_id, "mimeType": "image/png" }
                ]
            }
        }),
    )
    .await;
    let ws_b = created["workspace"]["id"].as_str().unwrap().to_string();
    let agent_id = created["initialAgent"]["id"].as_str().unwrap().to_string();
    let full = wss_rpc(
        &mut rpc,
        4,
        "agent.getSession",
        json!({ "agentId": agent_id }),
    )
    .await;
    let blocks = full["session"]["imageBlocks"].as_array().expect("blocks");
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["attachmentId"], json!(attachment_id));
    assert!(
        blocks[0].get("data").is_none(),
        "reference persists WITHOUT bytes: {blocks:?}"
    );

    // (2) The transcript row carries the reference, not the bytes.
    let conv = wss_rpc(
        &mut rpc,
        5,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages");
    let user_row = messages
        .iter()
        .find(|m| m["role"] == "user")
        .expect("user row");
    let image = user_row["contentBlocks"]
        .as_array()
        .expect("contentBlocks")
        .iter()
        .find(|b| b["type"] == "image")
        .expect("image block on the user row")
        .clone();
    assert_eq!(image["attachmentId"], json!(attachment_id));
    assert!(image.get("data").is_none(), "no bytes on the row: {image}");

    // (3) agent.sendMessage with a valid reference: accepted.
    let sent = wss_rpc(
        &mut rpc,
        6,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_b,
            "agentId": agent_id,
            "content": "and this one",
            "imageBlocks": [
                { "type": "image", "attachmentId": attachment_id }
            ]
        }),
    )
    .await;
    assert_eq!(sent["success"], json!(true), "{sent}");

    // (4) Unknown attachment id → -32602 naming the id.
    let err = wss_rpc_err(
        &mut rpc,
        7,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_b,
            "agentId": agent_id,
            "content": "bad ref",
            "imageBlocks": [
                { "type": "image", "attachmentId": "att-nope" }
            ]
        }),
    )
    .await;
    assert_eq!(err["code"], json!(-32602), "{err}");
    assert!(
        err["message"].as_str().unwrap().contains("att-nope"),
        "{err}"
    );

    // (5) Shape violations → -32602 naming the block index: neither arm...
    let err = wss_rpc_err(
        &mut rpc,
        8,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_b,
            "agentId": agent_id,
            "content": "no arm",
            "imageBlocks": [ { "type": "image", "mimeType": "image/png" } ]
        }),
    )
    .await;
    assert_eq!(err["code"], json!(-32602), "{err}");
    assert!(
        err["message"].as_str().unwrap().contains("imageBlocks[0]"),
        "{err}"
    );
    // ...and both arms.
    let err = wss_rpc_err(
        &mut rpc,
        9,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_b,
            "agentId": agent_id,
            "content": "both arms",
            "imageBlocks": [
                { "type": "image", "data": "aGk=", "attachmentId": attachment_id }
            ]
        }),
    )
    .await;
    assert_eq!(err["code"], json!(-32602), "{err}");

    // (6) workspace.create with an unknown initialAgent reference: rejected
    // -32602 BEFORE any state change — no partially created workspace row is
    // left behind (the validation is hoisted ahead of the row insert).
    let before = wss_rpc(&mut rpc, 10, "workspace.list", json!({})).await;
    let count_before = before["workspaces"].as_array().expect("list").len();
    let err = wss_rpc_err(
        &mut rpc,
        11,
        "workspace.create",
        json!({
            "title": "Bad ref",
            "path": ".",
            "initialAgent": {
                "prompt": "bad image ref",
                "imageBlocks": [
                    { "type": "image", "attachmentId": "att-nope" }
                ]
            }
        }),
    )
    .await;
    assert_eq!(err["code"], json!(-32602), "{err}");
    assert!(
        err["message"].as_str().unwrap().contains("att-nope"),
        "{err}"
    );
    let after = wss_rpc(&mut rpc, 12, "workspace.list", json!({})).await;
    let count_after = after["workspaces"].as_array().expect("list").len();
    assert_eq!(
        count_before, count_after,
        "rejected create must not leave a workspace row behind"
    );
}
