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

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{PrimaryReverseRegistry, WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Owns the fixture's scratch directory and removes it on drop so a panicking
/// test does not leak files under the system tempdir (matches the pattern
/// used by `TempDir` in `uds_specialist.rs`).
struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    ws: WsApiServer,
    api: Arc<dyn WorkspaceApi>,
    port: u16,
    /// Shared handle to the daemon's reverse-dispatch registry so the failover
    /// test can poll `len()` until the closing client's guard has actually
    /// dropped, instead of waiting on an arbitrary sleep.
    registry: Arc<PrimaryReverseRegistry>,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-sticky-{}", &short[..8]));
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
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure_with_reverse(api.clone(), bus, opts, registry.clone(), None);
    let port = ws.start().await.expect("start");
    Fixture {
        ws,
        api,
        port,
        registry,
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

/// One bounded JSON-RPC round-trip on `ws`: send `method`/`params` with the
/// caller-supplied `id`, then wait for the matching response frame under a
/// single overall deadline of [`common::rpc_read_timeout`], echoing pings
/// inline. Used as a lightweight barrier — a successful reply proves the
/// server-side `connection_loop` is running past the point where it
/// registered its reverse channel with `PrimaryReverseRegistry`, so pairing
/// two sequential `client.hello` calls yields a deterministic arrival order.
/// The read budget is a *total* budget across all frames (ping / unrelated
/// notification loops included), matching the `try_read_text` pattern below
/// so pings can't extend the wait indefinitely.
async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "wss_rpc timed out waiting for response to id={id} method={method}"
        );
        match timeout(remaining, ws.next()).await.unwrap_or_else(|_| {
            panic!("wss_rpc timed out waiting for response to id={id} method={method}")
        }) {
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

/// Drive `ws.next()` until the peer's close reply / EOF (or `dur` elapses),
/// echoing pings inline so heartbeat traffic doesn't stall the drain. Used
/// after `ws.close(None).await` to prove the server-side `connection_loop`
/// has observed our close and exited its read arm.
async fn drain_until_close(ws: &mut PlainWs, dur: Duration) {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        match timeout(remaining, ws.next()).await {
            Err(_) | Ok(None | Some(Ok(Message::Close(_)) | Err(_))) => return,
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// Read the next `Message::Text` frame, answering pings inline. Returns `None`
/// if the deadline elapses so a caller can assert "no traffic on this socket".
/// `dur` is the *total* budget across all frames (ping-answer loops included):
/// each iteration recomputes the remaining time against a fixed deadline so a
/// steady stream of pings can't extend the wait indefinitely.
async fn try_read_text(ws: &mut PlainWs, dur: Duration) -> Option<Value> {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match timeout(remaining, ws.next()).await {
            Err(_) => return None,
            Ok(Some(Ok(Message::Text(text)))) => {
                return Some(serde_json::from_str(&text).expect("json"));
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
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
    ws.send(Message::Text(reply.to_string().into()))
        .await
        .unwrap();
    frame
}

#[tokio::test]
async fn agent_browser_exec_routes_to_first_client_and_fails_over_on_disconnect() {
    let fx = boot().await;
    // Deterministic arrival-order barrier: connect A and complete a
    // lightweight `client.hello` round-trip before B is even dialled. A
    // successful reply on A guarantees its `connection_loop` has run past
    // `PrimaryReverseRegistry::register`, so B (dialled and hello-ed second)
    // must land behind A in the sticky queue — no sleep needed.
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", json!({ "name": "sticky-a" })).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", json!({ "name": "sticky-b" })).await;
    assert_eq!(
        fx.registry.len(),
        2,
        "both connections must be registered before the first reverse dispatch",
    );

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

    // Failover: close client A and wait for the server to actually
    // deregister it from the sticky queue before dispatching again. We drive
    // `a` to close/EOF so the connection loop breaks its read arm, then poll
    // the shared registry until its length drops below 2 — the definitive
    // signal that A's `PrimaryReverseGuard` has been dropped and B is now
    // the sole primary. Both waits share a single 2s deadline instead of the
    // former arbitrary 300ms sleep.
    let _ = a.close(None).await;
    drain_until_close(&mut a, Duration::from_secs(2)).await;
    drop(a);
    let dereg_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if fx.registry.len() < 2 {
            break;
        }
        assert!(
            Instant::now() < dereg_deadline,
            "sticky registry did not deregister client A within deadline (len={})",
            fx.registry.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
