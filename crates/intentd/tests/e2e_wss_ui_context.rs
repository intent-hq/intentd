//! WSS end-to-end for workspace UI context (PROTOCOL §5.1
//! `workspace.getUiContext` / `updateUiContext`). Drives a real [`WsApiServer`]
//! over plain `ws://` (insecure dev mode) so the WebSocket-upgrade → JSON-RPC →
//! router → services → store round-trip is exercised end-to-end. The critical
//! correctness requirement: the blob must round-trip verbatim — no shape
//! interpretation, no coercion (a non-array-to-[] coercion in the first adoption
//! attempt would have destroyed user data; that is the failure mode to design
//! against).

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
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-uictx-{}", &short[..8]));
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
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
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

async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
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
                        return v;
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

/// `workspace.getUiContext` starts null; `workspace.updateUiContext` persists
/// the caller-supplied blob verbatim (including arbitrary nested fields) and
/// round-trips byte-for-byte. No shape interpretation, no coercion.
#[tokio::test]
async fn workspace_ui_context_round_trip() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create workspace.
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "UI ctx test", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap();

    // Initial read returns null (pre-first-save default).
    let initial = wss_rpc(
        &mut rpc,
        2,
        "workspace.getUiContext",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(initial["uiContext"], json!(null));

    // Write a blob with arbitrary nested structure.
    let ui_ctx = json!({
        "workspaceId": ws_id,
        "mainContentType": "note",
        "mainContentId": "note-123",
        "mainContentPath": null,
        "diffInfo": null,
        "lastUpdated": "2026-07-15T00:00:00Z"
    });
    let updated = wss_rpc(
        &mut rpc,
        3,
        "workspace.updateUiContext",
        json!({ "workspaceId": ws_id, "uiContext": ui_ctx }),
    )
    .await;
    assert_eq!(updated["uiContext"], ui_ctx);

    // Read-back returns the same blob.
    let read = wss_rpc(
        &mut rpc,
        4,
        "workspace.getUiContext",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(read["uiContext"], ui_ctx);

    // Replace with a different shape — full replacement, not merge.
    let ui_ctx2 = json!({
        "workspaceId": ws_id,
        "mainContentType": "diff",
        "mainContentId": "src/main.rs",
        "mainContentPath": "src/main.rs",
        "diffInfo": {
            "additions": 10,
            "deletions": 5,
            "isStaged": false,
            "gitStatus": "modified"
        },
        "lastUpdated": "2026-07-15T01:00:00Z"
    });
    let updated2 = wss_rpc(
        &mut rpc,
        5,
        "workspace.updateUiContext",
        json!({ "workspaceId": ws_id, "uiContext": ui_ctx2 }),
    )
    .await;
    assert_eq!(updated2["uiContext"], ui_ctx2);

    let read2 = wss_rpc(
        &mut rpc,
        6,
        "workspace.getUiContext",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(read2["uiContext"], ui_ctx2);
}

/// Unknown workspace → -32602 Invalid params (same as workspace.get).
#[tokio::test]
async fn workspace_ui_context_unknown_workspace() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let err = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.getUiContext",
        json!({ "workspaceId": "ws-nonexistent" }),
    )
    .await;
    assert_eq!(err["error"]["code"], json!(-32602));

    let err2 = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.updateUiContext",
        json!({ "workspaceId": "ws-nonexistent", "uiContext": json!({ "foo": "bar" }) }),
    )
    .await;
    assert_eq!(err2["error"]["code"], json!(-32602));
}
