//! End-to-end WSS coverage for REV-1 first-client-sticky reverse dispatch.
//!
//! Drives a real [`WsApiServer`] (insecure dev mode: plain `ws://`, no TLS/
//! bearer, so the setup stays hermetic) with a shared
//! [`PrimaryReverseRegistry`], connects two WebSocket clients, and calls
//! [`WorkspaceApi::browser_exec`] directly on the shared service — the same
//! entry point the MCP `ws.browser.exec` binding uses when an agent triggers
//! a reverse RPC. The test asserts that:
//!   1. the first-connected client receives the reverse RPC (sticky primary),
//!   2. the second client sees nothing,
//!   3. dropping the first client promotes the second one for the next call.

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{PrimaryReverseRegistry, WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Fixture {
    ws: WsApiServer,
    api: Arc<dyn WorkspaceApi>,
    port: u16,
    _dir: std::path::PathBuf,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::path::Path::new("/tmp").join(format!("intentd-sticky-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let registry = Arc::new(PrimaryReverseRegistry::new());
    let services = Services::new(store)
        .with_assets_root(dir.join("assets"))
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(registry.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: free_port(),
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure_with_reverse(api.clone(), bus, opts, registry);
    let port = ws.start().await.expect("start");
    Fixture {
        ws,
        api,
        port,
        _dir: dir,
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

/// Read the next `Message::Text` frame, answering pings inline. Returns `None`
/// if the deadline elapses so a caller can assert "no traffic on this socket".
async fn try_read_text(ws: &mut PlainWs, dur: Duration) -> Option<Value> {
    loop {
        match timeout(dur, ws.next()).await {
            Err(_) => return None,
            Ok(Some(Ok(Message::Text(text)))) => {
                return Some(serde_json::from_str(&text).expect("json"));
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(other) => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Play the FE role for the primary client: answer the daemon-initiated
/// `browser.exec` reverse RPC by echoing `result` under the rev id.
async fn answer_reverse(ws: &mut PlainWs, dur: Duration, result: Value) -> Value {
    let frame = try_read_text(ws, dur)
        .await
        .expect("primary should see reverse RPC");
    assert_eq!(frame["method"], "browser.exec");
    let rev_id = frame["id"].as_str().unwrap().to_string();
    assert!(rev_id.starts_with("rev-"));
    let reply = json!({
        "jsonrpc": "2.0",
        "id": rev_id,
        "result": result,
    });
    ws.send(Message::Text(reply.to_string())).await.unwrap();
    frame
}

#[tokio::test]
async fn agent_browser_exec_routes_to_first_client_and_fails_over_on_disconnect() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let mut b = connect(fx.port).await;
    // Give the server a moment to register both connections in arrival order.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // First round: call from the "agent" side. Client A is primary and must
    // see the reverse RPC; client B must see nothing.
    let call_a = tokio::spawn({
        let api = fx.api.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "listTabs" })],
                Some("tab-1".to_string()),
                None,
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "listTabs", "success": true, "result": [] }]
    });
    let forwarded = answer_reverse(&mut a, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["tabId"], "tab-1");
    // REV-1: attribution — the reverse-RPC params must carry `workspaceId`
    // (mirrors the client-triggered `browser.exec` contract in PROTOCOL
    // §5.14 so the FE sees a byte-identical envelope regardless of caller).
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    assert!(
        try_read_text(&mut b, Duration::from_millis(200))
            .await
            .is_none(),
        "secondary client must not see the reverse RPC while A is primary",
    );
    let out = call_a.await.expect("join").expect("ok");
    assert_eq!(out["action"], "listTabs");

    // Failover: drop client A and let the server observe the close, then call
    // again. Client B is now primary.
    let _ = a.close(None).await;
    drop(a);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let call_b = tokio::spawn({
        let api = fx.api.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "screenshot" })],
                None,
                None,
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "screenshot", "success": true, "result": { "base64": "..." } }]
    });
    let forwarded = answer_reverse(&mut b, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["actions"][0]["action"], "screenshot");
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    let out = call_b.await.expect("join").expect("ok");
    assert_eq!(out["action"], "screenshot");

    fx.ws.stop().await;
}

#[tokio::test]
async fn agent_browser_exec_without_any_client_reports_no_client_error() {
    let fx = boot().await;
    let err = fx
        .api
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
        .await
        .expect_err("no client");
    let s = err.to_string();
    assert!(s.contains("no client connected"), "unexpected error: {s}");
    fx.ws.stop().await;
}
