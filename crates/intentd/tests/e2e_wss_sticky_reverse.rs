//! End-to-end WSS coverage for REV-2 capability-gated, identity-aware reverse
//! dispatch.
//!
//! Drives a real [`WsApiServer`] (insecure dev mode: plain `ws://`, no TLS/
//! bearer, so the setup stays hermetic) with a shared
//! [`PrimaryReverseRegistry`], connects WebSocket clients that `client.hello`
//! with or without `capabilities.browserExec`, and calls
//! [`WorkspaceApi::browser_exec`] directly on the shared service — the same
//! entry point the MCP `ws.browser.exec` binding uses when an agent triggers
//! a reverse RPC. The tests assert that:
//!   1. the first-connected **eligible** client receives the reverse RPC,
//!   2. other clients see nothing,
//!   3. dropping the primary promotes the next eligible client,
//!   4. a first client without the capability is skipped (the iOS /
//!      auxiliary-connection misrouting regression),
//!   5. `ReverseTarget::Pinned` routes to the named client regardless of
//!      arrival order and reports a typed `ClientOffline` error when it is
//!      gone,
//!   6. `client:connected` / `client:disconnected` reach an
//!      `events.subscribe` subscriber — including when the heartbeat reaper
//!      aborts a silent client's task, and in registry order when the same
//!      client reconnects right behind its own disconnect.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    AgentReverseDispatch, ClientId, ReverseDispatchError, ReverseTarget, WorkspaceApi, WorkspaceId,
};
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
    boot_with(WsOptions::default()).await
}

/// [`boot`] with caller-supplied listener options (`base_port` and
/// `bind_addresses` are always overridden to an ephemeral loopback port).
async fn boot_with(opts: WsOptions) -> Fixture {
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
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..opts
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

/// `client.hello` params for logical client `client_id`, advertising (or not)
/// the `browserExec` capability (REV-2 eligibility, PROTOCOL §5.17).
fn hello(client_id: &str, browser_exec: bool) -> Value {
    json!({
        "clientId": client_id,
        "name": format!("Intent Desktop @ {client_id}"),
        "capabilities": { "browserExec": browser_exec },
    })
}

/// Close `ws` and wait until the server has actually dropped its registry
/// entry (the definitive signal that the connection's reverse guard is gone),
/// instead of sleeping. `expected_len` is the registry size once it has.
async fn close_and_await_deregistration(
    mut ws: PlainWs,
    registry: &PrimaryReverseRegistry,
    expected_len: usize,
) {
    let _ = ws.close(None).await;
    drain_until_close(&mut ws, Duration::from_secs(2)).await;
    drop(ws);
    let deadline = Instant::now() + Duration::from_secs(2);
    while registry.len() > expected_len {
        assert!(
            Instant::now() < deadline,
            "registry did not deregister the closed client within deadline (len={})",
            registry.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Pump an `events.subscribe` connection until an `events.event` of
/// `event_type` arrives (bounded), answering pings inline. Returns the event.
async fn await_event(ws: &mut PlainWs, event_type: &str, dur: Duration) -> Value {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {event_type} event"
        );
        match timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {event_type} event"))
        {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" && v["params"]["event"]["type"] == event_type {
                    return v["params"]["event"].clone();
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

/// Like [`await_event`] but returns the next `client:*` event of either
/// type, so a caller can assert the *order* of a connected/disconnected pair.
async fn await_client_event(ws: &mut PlainWs, dur: Duration) -> Value {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for a client:* event"
        );
        match timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for a client:* event"))
        {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event"
                    && v["params"]["event"]["type"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("client:"))
                {
                    return v["params"]["event"].clone();
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

/// Wait (bounded, no fixed sleep) until the registry holds exactly
/// `expected_len` entries.
async fn await_registry_len(registry: &PrimaryReverseRegistry, expected_len: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while registry.len() != expected_len {
        assert!(
            Instant::now() < deadline,
            "registry did not reach len={expected_len} within deadline (len={})",
            registry.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
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
    // `client.hello` round-trip (advertising `browserExec`, so A is an
    // eligible target) before B is even dialled. A successful reply on A
    // guarantees its `connection_loop` has run past
    // `PrimaryReverseRegistry::register` AND bound A's identity, so B
    // (dialled and hello-ed second) must land behind A — no sleep needed.
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("sticky-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("sticky-b", true)).await;
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
    // deregister it before dispatching again — the definitive signal that
    // A's `PrimaryReverseGuard` has been released and B is now the sole
    // eligible client.
    close_and_await_deregistration(a, &fx.registry, 1).await;
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

#[tokio::test]
async fn agent_screenshot_timeout_returns_before_outer_deadline() {
    let fx = boot().await;
    let mut client = connect(fx.port).await;
    let _ = wss_rpc(
        &mut client,
        1,
        "client.hello",
        hello("timeout-client", true),
    )
    .await;
    let started = Instant::now();
    let call = tokio::spawn({
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

    let reverse = try_read_text(&mut client, Duration::from_secs(2))
        .await
        .expect("primary should receive screenshot reverse request");
    assert_eq!(reverse["method"], "browser.exec");
    assert_eq!(reverse["params"]["actions"][0]["action"], "screenshot");
    // Leave the reverse request unanswered and prove the same sticky-primary
    // path used by ws.browser.exec reports the error inside the outer budget.
    let err = timeout(Duration::from_secs(25), call)
        .await
        .expect("inner screenshot timeout must settle first")
        .expect("join")
        .expect_err("unanswered screenshot must fail");
    let elapsed = started.elapsed();
    assert!(
        err.to_string()
            .contains("reverse request timed out: browser.exec"),
        "unexpected error: {err}"
    );
    assert!(elapsed >= Duration::from_secs(19), "elapsed={elapsed:?}");
    assert!(elapsed < Duration::from_secs(30), "elapsed={elapsed:?}");

    fx.ws.stop().await;
}

/// Regression for the REV-1 misrouting (intent-hq/intent#461): a client that
/// connects first but is not a browser host — iOS (never hellos) or an FE
/// auxiliary `JsonRpcClient` (hellos without `browserExec`) — must never
/// receive the agent's `browser.exec`; the desktop that hellos with the
/// capability gets it even though it arrived last.
#[tokio::test]
async fn agent_browser_exec_skips_clients_without_the_browser_exec_capability() {
    let fx = boot().await;
    // iOS-like: connected, never sends `client.hello`. Barrier on `host.status`
    // so its connection loop is registered before the next client dials.
    let mut ios = connect(fx.port).await;
    let _ = wss_rpc(&mut ios, 1, "host.status", json!({})).await;
    // FE auxiliary connection: hellos, but without the capability.
    let mut aux = connect(fx.port).await;
    let _ = wss_rpc(&mut aux, 1, "client.hello", hello("desktop-a", false)).await;
    assert_eq!(fx.registry.len(), 2);
    assert!(
        !fx.registry.is_connected(),
        "neither an un-hello'd nor a capability-less connection is an eligible target"
    );
    // The agent's call before any eligible client exists is the typed
    // `no client connected` failure, not a 20 s hang on iOS.
    let err = fx
        .api
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
        .await
        .expect_err("no eligible client");
    assert!(
        err.to_string().contains("no client connected"),
        "unexpected error: {err}"
    );
    assert!(try_read_text(&mut ios, Duration::from_millis(200))
        .await
        .is_none());
    assert!(try_read_text(&mut aux, Duration::from_millis(200))
        .await
        .is_none());

    // The desktop main connection arrives last, advertising the capability.
    let mut desktop = connect(fx.port).await;
    let _ = wss_rpc(&mut desktop, 1, "client.hello", hello("desktop-a", true)).await;
    assert_eq!(fx.registry.len(), 3);
    assert!(fx.registry.is_connected());

    let call = tokio::spawn({
        let api = fx.api.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "listTabs" })],
                None,
                None,
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "listTabs", "success": true, "result": [] }]
    });
    let forwarded = answer_reverse(&mut desktop, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out["action"], "listTabs");
    assert!(
        try_read_text(&mut ios, Duration::from_millis(200))
            .await
            .is_none(),
        "iOS must never see the reverse RPC"
    );
    assert!(
        try_read_text(&mut aux, Duration::from_millis(200))
            .await
            .is_none(),
        "the auxiliary connection must never see the reverse RPC"
    );

    fx.ws.stop().await;
}

/// `ReverseTarget::Pinned` (the per-workspace browser-client pin) routes to
/// the named client's connection regardless of arrival order, and the
/// registry's `resolve` probe reports the same answer with the hello `name`.
#[tokio::test]
async fn pinned_target_routes_to_the_named_client_regardless_of_arrival_order() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;
    assert_eq!(fx.registry.len(), 2);

    let resolved = fx
        .registry
        .resolve(&ReverseTarget::Pinned(ClientId::from_string("desktop-b")))
        .expect("pinned client is connected");
    assert_eq!(resolved.client_id.as_str(), "desktop-b");
    assert_eq!(resolved.name.as_deref(), Some("Intent Desktop @ desktop-b"));
    // `Default` still prefers the first-connected eligible client.
    assert_eq!(
        fx.registry
            .resolve(&ReverseTarget::Default)
            .expect("default")
            .client_id
            .as_str(),
        "desktop-a"
    );

    let call = tokio::spawn({
        let registry = fx.registry.clone();
        async move {
            registry
                .dispatch(
                    "browser.exec",
                    json!({ "actions": [{ "action": "listTabs" }], "workspaceId": "ws-1" }),
                    ReverseTarget::Pinned(ClientId::from_string("desktop-b")),
                )
                .await
        }
    });
    let forwarded =
        answer_reverse(&mut b, Duration::from_secs(2), json!({ "success": true })).await;
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out, json!({ "success": true }));
    assert!(
        try_read_text(&mut a, Duration::from_millis(200))
            .await
            .is_none(),
        "the first-connected client must not see a dispatch pinned elsewhere"
    );

    let mut clients = fx.registry.live_clients();
    clients.sort_by(|x, y| x.client_id.as_str().cmp(y.client_id.as_str()));
    assert_eq!(clients.len(), 2);
    assert_eq!(clients[0].client_id.as_str(), "desktop-a");
    assert_eq!(clients[0].connections, 1);
    assert_eq!(clients[1].client_id.as_str(), "desktop-b");
    assert_eq!(clients[1].capabilities, json!({ "browserExec": true }));

    fx.ws.stop().await;
}

/// A pinned client that has disconnected (or never connected) yields the
/// typed `ClientOffline { pinned: true }` error — no silent fallback to the
/// remaining eligible client, which keeps serving `Default`.
#[tokio::test]
async fn pinned_target_offline_reports_typed_error_without_fallback() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let b = {
        let mut b = connect(fx.port).await;
        let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;
        b
    };
    assert_eq!(fx.registry.len(), 2);
    close_and_await_deregistration(b, &fx.registry, 1).await;

    let pinned_b = ReverseTarget::Pinned(ClientId::from_string("desktop-b"));
    let err = fx
        .registry
        .dispatch(
            "browser.exec",
            json!({ "actions": [{ "action": "listTabs" }] }),
            pinned_b.clone(),
        )
        .await
        .expect_err("pinned client is offline");
    assert_eq!(
        err,
        ReverseDispatchError::ClientOffline {
            client_id: ClientId::from_string("desktop-b"),
            name: None,
            pinned: true,
        }
    );
    assert_eq!(
        err.to_string(),
        "pinned browser client desktop-b is not connected"
    );
    assert_eq!(fx.registry.resolve(&pinned_b), Err(err));
    // A never-seen pin behaves the same way.
    assert!(matches!(
        fx.registry
            .resolve(&ReverseTarget::Pinned(ClientId::from_string("ghost"))),
        Err(ReverseDispatchError::ClientOffline { pinned: true, .. })
    ));
    assert!(
        try_read_text(&mut a, Duration::from_millis(200))
            .await
            .is_none(),
        "no silent fallback to the remaining client"
    );

    // `Default` (what `Services::browser_exec` uses today) still reaches A.
    let call = tokio::spawn({
        let api = fx.api.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "listTabs" })],
                None,
                None,
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "listTabs", "success": true, "result": [] }]
    });
    answer_reverse(&mut a, Duration::from_secs(2), fe_result).await;
    call.await.expect("join").expect("ok");

    fx.ws.stop().await;
}

/// `client:connected` / `client:disconnected` (global, no `workspaceId`)
/// reach an `events.subscribe` subscriber with
/// `data: { clientId, name?, capabilities }` — once per logical client, not
/// per connection: a second connection of the same `clientId` is silent, and
/// `client:disconnected` fires only when the last one goes away.
#[tokio::test]
async fn client_connected_and_disconnected_events_are_published_per_logical_client() {
    let fx = boot().await;
    let mut sub = connect(fx.port).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["client:connected", "client:disconnected"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    let mut main = connect(fx.port).await;
    let _ = wss_rpc(&mut main, 1, "client.hello", hello("desktop-a", true)).await;
    let ev = await_event(&mut sub, "client:connected", Duration::from_secs(2)).await;
    assert_eq!(ev["type"], "client:connected");
    assert_eq!(
        ev["data"],
        json!({
            "clientId": "desktop-a",
            "name": "Intent Desktop @ desktop-a",
            "capabilities": { "browserExec": true },
        })
    );

    // A second connection of the same logical client (an FE auxiliary
    // socket) does not re-announce the client.
    let aux = {
        let mut aux = connect(fx.port).await;
        let _ = wss_rpc(&mut aux, 1, "client.hello", hello("desktop-a", false)).await;
        aux
    };
    // Closing that auxiliary connection is silent too: the client is still
    // live through `main`.
    close_and_await_deregistration(aux, &fx.registry, 2).await;
    assert!(
        try_read_text(&mut sub, Duration::from_millis(300))
            .await
            .is_none(),
        "no client:* event while the logical client stays live"
    );

    // Closing the last connection announces the disconnect.
    close_and_await_deregistration(main, &fx.registry, 1).await;
    let ev = await_event(&mut sub, "client:disconnected", Duration::from_secs(2)).await;
    assert_eq!(ev["data"]["clientId"], "desktop-a");
    assert_eq!(ev["data"]["capabilities"], json!({ "browserExec": true }));
    assert!(fx.registry.live_clients().is_empty());

    fx.ws.stop().await;
}

/// The heartbeat reaper terminates a silent connection by **aborting** its
/// task (`ws.rs` `heartbeat_loop`), so the connection loop never reaches its
/// normal epilogue — the registry entry is dropped by RAII. That departure
/// must still be announced: `client:disconnected` reaches the subscriber and
/// the client is gone from `live_clients()`.
#[tokio::test]
async fn heartbeat_abort_publishes_client_disconnected() {
    let fx = boot_with(WsOptions {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(200),
        ..WsOptions::default()
    })
    .await;
    let mut sub = connect(fx.port).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["client:connected", "client:disconnected"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    // Hello with the capability, then never poll the socket again so no
    // pong is ever answered; the reaper aborts the server task.
    let silent = {
        let mut silent = connect(fx.port).await;
        let _ = wss_rpc(&mut silent, 1, "client.hello", hello("desktop-a", true)).await;
        silent
    };
    let ev = await_event(&mut sub, "client:connected", Duration::from_secs(2)).await;
    assert_eq!(ev["data"]["clientId"], "desktop-a");
    assert!(fx.registry.is_connected());

    let ev = await_event(&mut sub, "client:disconnected", Duration::from_secs(5)).await;
    assert_eq!(
        ev["data"],
        json!({
            "clientId": "desktop-a",
            "name": "Intent Desktop @ desktop-a",
            "capabilities": { "browserExec": true },
        })
    );
    await_registry_len(&fx.registry, 1).await;
    assert!(fx.registry.live_clients().is_empty());
    assert!(!fx.registry.is_connected());
    drop(silent);

    fx.ws.stop().await;
}

/// Transitions are published in registry-mutation order. A same-client
/// reconnect dialled the instant the stale entry is gone (the barrier is the
/// registry length, not a sleep) must yield `client:disconnected` **then**
/// `client:connected`; a re-hello that moves a connection to another
/// `clientId` yields the old client's disconnect before the new one's
/// connect — never a stale disconnect trailing a fresh connect.
#[tokio::test]
async fn client_events_keep_registry_order_across_reconnect_and_rehello() {
    let fx = boot().await;
    let mut sub = connect(fx.port).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["client:connected", "client:disconnected"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    let mut first = connect(fx.port).await;
    let _ = wss_rpc(&mut first, 1, "client.hello", hello("desktop-a", true)).await;
    let ev = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(ev["type"], "client:connected");
    assert_eq!(ev["data"]["clientId"], "desktop-a");

    // Close without draining and dial the replacement as soon as the
    // registry has forgotten the first connection — i.e. inside the window
    // between the registry mutation and the event reaching subscribers.
    let _ = first.close(None).await;
    drop(first);
    await_registry_len(&fx.registry, 1).await;
    let mut second = connect(fx.port).await;
    let _ = wss_rpc(&mut second, 1, "client.hello", hello("desktop-a", true)).await;
    let ev1 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    let ev2 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(
        (ev1["type"].as_str(), ev2["type"].as_str()),
        (Some("client:disconnected"), Some("client:connected")),
        "stale disconnect must precede the reconnect: {ev1} then {ev2}"
    );
    assert_eq!(ev1["data"]["clientId"], "desktop-a");
    assert_eq!(ev2["data"]["clientId"], "desktop-a");

    // Re-hello under a different clientId on the same connection.
    let _ = wss_rpc(&mut second, 2, "client.hello", hello("desktop-b", true)).await;
    let ev1 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    let ev2 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(ev1["type"], "client:disconnected");
    assert_eq!(ev1["data"]["clientId"], "desktop-a");
    assert_eq!(ev2["type"], "client:connected");
    assert_eq!(ev2["data"]["clientId"], "desktop-b");
    let clients = fx.registry.live_clients();
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[0].client_id.as_str(), "desktop-b");

    // A same-client second connection followed by the first one leaving is
    // a silent hand-over: exactly one disconnect, only once both are gone.
    let mut third = connect(fx.port).await;
    let _ = wss_rpc(&mut third, 1, "client.hello", hello("desktop-b", false)).await;
    close_and_await_deregistration(second, &fx.registry, 2).await;
    assert!(
        try_read_text(&mut sub, Duration::from_millis(300))
            .await
            .is_none(),
        "no client:* event while desktop-b stays live through its other connection"
    );
    close_and_await_deregistration(third, &fx.registry, 1).await;
    let ev = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(ev["type"], "client:disconnected");
    assert_eq!(ev["data"]["clientId"], "desktop-b");
    assert!(
        try_read_text(&mut sub, Duration::from_millis(300))
            .await
            .is_none(),
        "exactly one disconnect for the logical client"
    );

    fx.ws.stop().await;
}
