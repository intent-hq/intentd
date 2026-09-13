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
//!      client reconnects right behind its own disconnect,
//!   7. a workspace pinned via `workspace.setBrowserClient` routes an agent
//!      `browser.exec` to the pinned client's eligible connection and fails
//!      typed (no fallback) once that client is gone,
//!   8. REV-2 Model 5 routing: an agent `browser.exec` goes to the
//!      workspace's driving client (the host of its claimed tabs when
//!      unpinned, even behind a first-connected client), the agent-path
//!      `listTabs` is answered from the registry across hosts with no reverse
//!      call, and `browser.navigateTab` routes an unclaimed tab to its
//!      physical host,
//!   9. `browser.closeTab` routes to the host while it is online, is a typed
//!      offline error without `force`, and with `force` tombstones the row
//!      (`browser:tab-closed`) once the host is gone,
//!  10. a `claimTab` executed on the driving client re-homes the tab there
//!      (`browser:tab-updated { changes: { hostClientId, ownerAgentId } }`)
//!      and a later pin change migrates the claimed tab again.
//!
//! The wire contract of the pin RPCs themselves (`client.list`,
//! `workspace.getBrowserClient` / `setBrowserClient`, their events and
//! `-32602` paths) is covered over the secure TLS + bearer + fingerprint-pinned
//! transport in `e2e_wss_browser_client_pin.rs`; this file only sets the pin
//! as setup for the in-process dispatch assertions.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
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

struct Fixture {
    ws: WsApiServer,
    api: Arc<dyn WorkspaceApi>,
    port: u16,
    /// Shared handle to the daemon's reverse-dispatch registry so the failover
    /// test can poll `len()` until the closing client's guard has actually
    /// dropped, instead of waiting on an arbitrary sleep.
    registry: Arc<PrimaryReverseRegistry>,
    _dir: tempfile::TempDir,
}

async fn boot() -> Fixture {
    boot_with(WsOptions::default()).await
}

/// [`boot`] with caller-supplied listener options (`base_port` and
/// `bind_addresses` are always overridden to an ephemeral loopback port).
async fn boot_with(opts: WsOptions) -> Fixture {
    let dir_guard = common::test_tempdir("intentd-sticky-");
    let dir = dir_guard.path().to_path_buf();
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
        _dir: dir_guard,
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
/// Assert the full JSON-RPC 2.0 response envelope: `jsonrpc == "2.0"`, the
/// request `id` echoed, and exactly one of `result` / `error` with no other
/// top-level keys. An `error` member carries an integer `code` and a string
/// `message` (plus optional `data`).
fn assert_envelope(v: &Value, id: i64, method: &str) {
    let obj = v
        .as_object()
        .unwrap_or_else(|| panic!("{method}: response is not an object: {v}"));
    assert_eq!(obj.get("jsonrpc"), Some(&json!("2.0")), "{method}: {v}");
    assert_eq!(obj.get("id"), Some(&json!(id)), "{method}: {v}");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    match (obj.get("result"), obj.get("error")) {
        (Some(_), None) => assert_eq!(keys, ["id", "jsonrpc", "result"], "{method}: {v}"),
        (None, Some(err)) => {
            assert_eq!(keys, ["error", "id", "jsonrpc"], "{method}: {v}");
            let err = err
                .as_object()
                .unwrap_or_else(|| panic!("{method}: error is not an object: {v}"));
            assert!(err["code"].is_i64(), "{method}: error.code: {v}");
            assert!(err["message"].is_string(), "{method}: error.message: {v}");
            let mut err_keys: Vec<&str> = err.keys().map(String::as_str).collect();
            err_keys.sort_unstable();
            assert!(
                err_keys == ["code", "message"] || err_keys == ["code", "data", "message"],
                "{method}: error keys {err_keys:?}: {v}"
            );
        }
        _ => panic!("{method}: exactly one of result/error required: {v}"),
    }
}

async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    send_request(ws, id, method, params).await;
    await_response(ws, id, method).await
}

/// The send half of [`wss_rpc`]: fire `method`/`params` under `id` without
/// waiting, so a test can play the routed target on *another* socket before
/// collecting the caller's reply with [`await_response`].
async fn send_request(ws: &mut PlainWs, id: i64, method: &str, params: Value) {
    let req = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
}

/// The read half of [`wss_rpc`]: the response frame for `id` (envelope
/// asserted), skipping unrelated frames and echoing pings inline.
async fn await_response(ws: &mut PlainWs, id: i64, method: &str) -> Value {
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
                    assert_envelope(&v, id, method);
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
/// the `browserExec` capability (REV-2 eligibility, PROTOCOL §5.17), with the
/// client's own host identification (`hostname` / `prettyHostname` /
/// `deviceKind`, mirroring `host.status`).
fn hello(client_id: &str, browser_exec: bool) -> Value {
    json!({
        "clientId": client_id,
        "name": format!("Intent Desktop @ {client_id}"),
        "capabilities": { "browserExec": browser_exec },
        "hostname": format!("{client_id}.local"),
        "prettyHostname": format!("{client_id} (pretty)"),
        "deviceKind": "laptop",
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

/// A host-reported tab object for `browser.upsertTab` (unclaimed unless the
/// caller adds `ownerAgentId`).
fn tab(tab_id: &str, url: &str) -> Value {
    json!({ "tabId": tab_id, "url": url, "title": tab_id, "visibility": "visible" })
}

/// `workspace.create` over `ws`; returns the new workspace id.
async fn create_workspace(ws: &mut PlainWs, id: i64, title: &str) -> String {
    let created = wss_rpc(ws, id, "workspace.create", json!({ "title": title })).await;
    created["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("created id: {created}"))
        .to_string()
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

    // The tab an agent batch names must be a registered tab of the
    // workspace (REV-2 Model 5): A hosts an unclaimed `tab-1`.
    let ws_id = create_workspace(&mut a, 2, "Sticky").await;
    let _ = wss_rpc(
        &mut a,
        3,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-1", "https://a.test/") }),
    )
    .await;

    // First round: call from the "agent" side. Client A is primary and must
    // see the reverse RPC; client B must see nothing.
    let call_a = tokio::spawn({
        let api = fx.api.clone();
        let ws_id = ws_id.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from(ws_id.as_str()),
                vec![json!({ "action": "getAccessibilityTree" })],
                Some("tab-1".to_string()),
                None,
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "getAccessibilityTree", "success": true, "result": "- root" }]
    });
    let forwarded = answer_reverse(&mut a, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["tabId"], "tab-1");
    // REV-1: attribution — the reverse-RPC params must carry `workspaceId`
    // (mirrors the client-triggered `browser.exec` contract in PROTOCOL
    // §5.14 so the FE sees a byte-identical envelope regardless of caller).
    assert_eq!(forwarded["params"]["workspaceId"], ws_id.as_str());
    assert!(
        try_read_text(&mut b, Duration::from_millis(200))
            .await
            .is_none(),
        "secondary client must not see the reverse RPC while A is primary",
    );
    let out = call_a.await.expect("join").expect("ok");
    assert_eq!(out["action"], "getAccessibilityTree");

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
            vec![json!({ "action": "screenshot" })],
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
            vec![json!({ "action": "screenshot" })],
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
    let forwarded = answer_reverse(&mut desktop, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out["action"], "screenshot");
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
///
/// The abort is forced, not raced: [`WsOptions::heartbeat_gate`] holds the
/// reaper's abort back until the test has observed the connected state, so
/// no amount of scheduling delay between the hello and that observation can
/// let the 200ms pong deadline win (intent-hq/intent#4851).
#[tokio::test]
async fn heartbeat_abort_publishes_client_disconnected() {
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let fx = boot_with(WsOptions {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(200),
        heartbeat_gate: Some(gate_rx),
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

    // Hello with the capability, then never poll the socket again, so no
    // pong is ever answered. The gate is still closed: the reaper keeps
    // pinging but cannot abort, so the connected state is observed on a
    // connection that is guaranteed to still be registered — however long
    // the hello reply or the subscriber's frame took to arrive.
    let silent = {
        let mut silent = connect(fx.port).await;
        let _ = wss_rpc(&mut silent, 1, "client.hello", hello("desktop-a", true)).await;
        silent
    };
    let ev = await_event(&mut sub, "client:connected", Duration::from_secs(2)).await;
    assert_eq!(ev["data"]["clientId"], "desktop-a");
    assert!(fx.registry.is_connected());
    // Well past the pong deadline the held-back reaper has still not fired.
    assert!(
        try_read_text(&mut sub, Duration::from_millis(600))
            .await
            .is_none(),
        "reaper aborted while gated"
    );
    assert!(fx.registry.is_connected());

    // Release the reaper; the next tick past the deadline aborts the task.
    gate_tx.send(true).expect("gate receiver alive");
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

/// Transitions are published in registry-mutation order. The race is forced,
/// not hoped for: [`WsOptions::cleanup_gate`] parks the closing connection's
/// loop right after it has left the registry, a same-client reconnect is
/// registered *inside* that held-open window, and only then is the gate
/// released. The subscriber must still see `client:disconnected` **then**
/// `client:connected` (a design that publishes the disconnect from the
/// closing loop after its cleanup emits them the other way round). A
/// re-hello that moves a connection to another `clientId` likewise yields
/// the old client's disconnect before the new one's connect.
#[tokio::test]
async fn client_events_keep_registry_order_across_reconnect_and_rehello() {
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let fx = boot_with(WsOptions {
        cleanup_gate: Some(gate_rx),
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

    let mut first = connect(fx.port).await;
    let _ = wss_rpc(&mut first, 1, "client.hello", hello("desktop-a", true)).await;
    let ev = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(ev["type"], "client:connected");
    assert_eq!(ev["data"]["clientId"], "desktop-a");

    // Close the first connection. Its loop leaves the registry (the length
    // barrier observes that) and then parks on the closed gate, so the
    // replacement below registers strictly inside the held-open window.
    let _ = first.close(None).await;
    drop(first);
    await_registry_len(&fx.registry, 1).await;
    let mut second = connect(fx.port).await;
    let _ = wss_rpc(&mut second, 1, "client.hello", hello("desktop-a", true)).await;
    await_registry_len(&fx.registry, 2).await;
    let clients = fx.registry.live_clients();
    assert_eq!(
        clients.len(),
        1,
        "reconnect registered in-window: {clients:?}"
    );
    assert_eq!(clients[0].client_id.as_str(), "desktop-a");
    assert_eq!(clients[0].capabilities["browserExec"], true);
    assert_eq!(clients[0].connections, 1);

    // Release the parked loop; the stale disconnect must still be first.
    gate_tx.send(true).expect("gate receiver alive");
    let ev1 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    let ev2 = await_client_event(&mut sub, Duration::from_secs(2)).await;
    assert_eq!(
        (ev1["type"].as_str(), ev2["type"].as_str()),
        (Some("client:disconnected"), Some("client:connected")),
        "stale disconnect must precede the reconnect: {ev1} then {ev2}"
    );
    assert_eq!(ev1["data"]["clientId"], "desktop-a");
    assert_eq!(ev2["data"]["clientId"], "desktop-a");
    let clients = fx.registry.live_clients();
    assert_eq!(clients.len(), 1, "{clients:?}");
    assert_eq!(clients[0].client_id.as_str(), "desktop-a");

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

/// A workspace pinned via `workspace.setBrowserClient` routes an agent
/// `browser.exec` to the pinned client's **eligible** connection (not the
/// first-connected client, not the pinned client's non-capable auxiliary
/// socket) and, once that client is gone entirely, fails with the typed
/// pinned-offline message instead of falling back to the default client. The
/// pin is set over the wire as setup only — the RPC contract lives in
/// `e2e_wss_browser_client_pin.rs`.
#[tokio::test]
async fn pinned_workspace_browser_exec_routes_to_pinned_client_and_fails_typed_when_gone() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;
    // desktop-b's newer auxiliary connection lacks the capability.
    let mut aux = connect(fx.port).await;
    let _ = wss_rpc(&mut aux, 1, "client.hello", hello("desktop-b", false)).await;
    assert_eq!(fx.registry.len(), 3);

    let created = wss_rpc(
        &mut a,
        2,
        "workspace.create",
        json!({ "title": "Pinned browser" }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("created id: {created}"))
        .to_string();
    let set = wss_rpc(
        &mut a,
        3,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": "desktop-b" }),
    )
    .await;
    assert_eq!(
        set["result"]["browserClient"]["resolved"]["clientId"], "desktop-b",
        "{set}"
    );

    // An agent browser.exec in the pinned workspace reaches desktop-b's
    // eligible connection (`b`), not `a` and not the auxiliary socket.
    let call = tokio::spawn({
        let api = fx.api.clone();
        let ws_id = ws_id.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from(ws_id.as_str()),
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
    assert_eq!(forwarded["params"]["workspaceId"], ws_id.as_str());
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out["action"], "screenshot");
    for (name, sock) in [("a", &mut a), ("aux", &mut aux)] {
        assert!(
            try_read_text(sock, Duration::from_millis(200))
                .await
                .is_none(),
            "{name} must not see a dispatch pinned to desktop-b's eligible connection"
        );
    }

    // desktop-b goes away entirely: the pin stays and an agent browser.exec
    // is a hard error naming the persisted client — no silent fallback to
    // desktop-a.
    close_and_await_deregistration(b, &fx.registry, 2).await;
    close_and_await_deregistration(aux, &fx.registry, 1).await;
    let err = fx
        .api
        .browser_exec(
            WorkspaceId::from(ws_id.as_str()),
            vec![json!({ "action": "screenshot" })],
            None,
            None,
        )
        .await
        .expect_err("pinned client offline");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(m)
                if m == "browser.exec: browser client \"Intent Desktop @ desktop-b\" (desktop-b) for this workspace is not connected"
        ),
        "-32603 with the driving-client-offline message (Model 5): {err:?}"
    );
    assert!(
        try_read_text(&mut a, Duration::from_millis(200))
            .await
            .is_none(),
        "no silent fallback to desktop-a"
    );

    fx.ws.stop().await;
}

/// REV-2 Model 5 routing over two hosts. desktop-a connects first (the
/// default target) but desktop-b hosts the workspace's claimed tab, so
/// desktop-b is the unpinned workspace's **driving client**: an agent
/// `browser.exec` naming desktop-a's tab still dispatches to desktop-b
/// (one host per workspace, no per-tab lookup). The agent-path `listTabs`
/// is answered from the registry — both hosts' tabs, decorated with
/// `hostClientId` / `hostConnected` — and neither socket sees a reverse
/// call. `browser.navigateTab` on the unclaimed tab routes to its physical
/// host (desktop-a) as a `navigate` action carrying the tab's attribution.
#[tokio::test]
async fn agent_browser_exec_routes_to_the_claimed_tabs_host_and_list_tabs_aggregates_hosts() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;
    assert_eq!(fx.registry.len(), 2);

    let ws_id = create_workspace(&mut a, 2, "Two hosts").await;
    let _ = wss_rpc(
        &mut a,
        3,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-a", "https://a.test/") }),
    )
    .await;
    let mut claimed = tab("tab-b", "https://b.test/");
    claimed["ownerAgentId"] = json!("agent-1");
    claimed["ownerAgentName"] = json!("Agent One");
    let _ = wss_rpc(
        &mut b,
        2,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": claimed }),
    )
    .await;

    // (f) listTabs: answered by the daemon across hosts, no reverse RPC.
    let listed = fx
        .api
        .browser_exec(
            WorkspaceId::from(ws_id.as_str()),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
        .await
        .expect("listTabs from the registry");
    assert_eq!(listed["action"], "listTabs");
    assert_eq!(listed["success"], true);
    let entries = listed["result"].as_array().expect("tabs array");
    let mut hosts: Vec<(&str, &str, bool, &Value)> = entries
        .iter()
        .map(|e| {
            (
                e["tabId"].as_str().unwrap(),
                e["hostClientId"].as_str().unwrap(),
                e["hostConnected"].as_bool().unwrap(),
                &e["ownerAgentId"],
            )
        })
        .collect();
    hosts.sort_unstable_by_key(|h| h.0);
    assert_eq!(
        hosts,
        [
            ("tab-a", "desktop-a", true, &Value::Null),
            ("tab-b", "desktop-b", true, &json!("agent-1")),
        ],
        "{listed}"
    );
    for (name, sock) in [("a", &mut a), ("b", &mut b)] {
        assert!(
            try_read_text(sock, Duration::from_millis(200))
                .await
                .is_none(),
            "{name} must not see a reverse call for the registry-answered listTabs"
        );
    }

    // (b) An agent batch — even one naming desktop-a's own tab — goes to the
    // driving client desktop-b (host of the claimed tab), not first-connected
    // desktop-a.
    let call = tokio::spawn({
        let api = fx.api.clone();
        let ws_id = ws_id.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from(ws_id.as_str()),
                vec![json!({ "action": "screenshot" })],
                Some("tab-a".to_string()),
                Some(intent_core::AgentId::from("agent-1")),
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "screenshot", "success": true, "result": { "base64": "..." } }]
    });
    let forwarded = answer_reverse(&mut b, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["workspaceId"], ws_id.as_str());
    assert_eq!(forwarded["params"]["tabId"], "tab-a");
    assert_eq!(forwarded["params"]["agentId"], "agent-1");
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out["action"], "screenshot");
    assert!(
        try_read_text(&mut a, Duration::from_millis(200))
            .await
            .is_none(),
        "first-connected desktop-a is not the driving client"
    );

    // An unknown tabId is rejected before any dispatch.
    let err = fx
        .api
        .browser_exec(
            WorkspaceId::from(ws_id.as_str()),
            vec![json!({ "action": "screenshot", "tabId": "tab-nope" })],
            None,
            None,
        )
        .await
        .expect_err("unknown tab");
    assert!(
        matches!(&err, intent_core::Error::InvalidParams(m) if m == "browser.exec: tab not found: tab-nope"),
        "{err:?}"
    );

    // `browser.navigateTab` on the unclaimed tab-a routes to its physical
    // host desktop-a as a `navigate` action; desktop-b (the caller) only
    // sees its own response.
    send_request(
        &mut b,
        3,
        "browser.navigateTab",
        json!({ "tabId": "tab-a", "url": "https://a.test/next" }),
    )
    .await;
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "navigate", "success": true, "result": { "url": "https://a.test/next" } }]
    });
    let forwarded = answer_reverse(&mut a, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["workspaceId"], ws_id.as_str());
    assert_eq!(forwarded["params"]["tabId"], "tab-a");
    assert_eq!(forwarded["params"]["actions"][0]["action"], "navigate");
    assert_eq!(
        forwarded["params"]["actions"][0]["url"],
        "https://a.test/next"
    );
    let res = await_response(&mut b, 3, "browser.navigateTab").await;
    assert_eq!(res["result"]["action"], "navigate", "{res}");
    assert_eq!(res["result"]["result"]["url"], "https://a.test/next");

    fx.ws.stop().await;
}

/// `browser.closeTab` routing and the daemon-side tombstone (REV-2 Model 6):
/// while the host is online the close is a routed `closeTab` action; once
/// the host is gone a plain close is the typed `-32603` offline error and a
/// `force` close tombstones the row, publishing `browser:tab-closed` and
/// dropping the tab from the registry.
#[tokio::test]
async fn close_tab_routes_to_the_host_and_force_tombstones_an_offline_host() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;

    let ws_id = create_workspace(&mut a, 2, "Close routing").await;
    let mut sub = connect(fx.port).await;
    let _ = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["browser:tab-closed"], "workspaceId": ws_id }),
    )
    .await;
    let _ = wss_rpc(
        &mut b,
        2,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-b", "https://b.test/") }),
    )
    .await;

    // Online: routed to desktop-b, which acknowledges.
    send_request(&mut a, 3, "browser.closeTab", json!({ "tabId": "tab-b" })).await;
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "closeTab", "success": true, "result": { "closed": true } }]
    });
    let forwarded = answer_reverse(&mut b, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["tabId"], "tab-b");
    assert_eq!(forwarded["params"]["actions"][0]["action"], "closeTab");
    let res = await_response(&mut a, 3, "browser.closeTab").await;
    assert_eq!(res["result"], json!({ "ok": true }), "{res}");

    // The host reports the close itself normally; here it goes away
    // instead, leaving the row open and its host offline.
    close_and_await_deregistration(b, &fx.registry, 2).await;
    let res = wss_rpc(&mut a, 4, "browser.closeTab", json!({ "tabId": "tab-b" })).await;
    assert_eq!(res["error"]["code"], -32603, "{res}");
    assert_eq!(
        res["error"]["message"],
        "internal error: browser.closeTab: browser client \"Intent Desktop @ desktop-b\" (desktop-b) for this workspace is not connected"
    );

    let res = wss_rpc(
        &mut a,
        5,
        "browser.closeTab",
        json!({ "tabId": "tab-b", "force": true }),
    )
    .await;
    assert_eq!(res["result"], json!({ "ok": true }), "{res}");
    let ev = await_event(&mut sub, "browser:tab-closed", Duration::from_secs(2)).await;
    assert_eq!(ev["workspaceId"], ws_id.as_str());
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-b");
    assert_eq!(ev["data"]["tab"]["hostClientId"], "desktop-b");
    let listed = wss_rpc(
        &mut a,
        6,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(listed["result"]["tabs"], json!([]), "{listed}");
    // A tombstoned id is unknown to the tab-addressed methods.
    let res = wss_rpc(&mut a, 7, "browser.closeTab", json!({ "tabId": "tab-b" })).await;
    assert_eq!(res["error"]["code"], -32602, "{res}");

    fx.ws.stop().await;
}

/// Claim migration (REV-2 Model 5 & 10). desktop-a is the unpinned
/// workspace's driving client (first connected, no claimed tabs yet), so an
/// agent `claimTab` on desktop-b's unclaimed tab executes on desktop-a; once
/// the FE reports success the daemon re-homes the row to desktop-a with the
/// agent as owner (`browser:tab-updated { changes: { hostClientId,
/// ownerAgentId } }`). Pinning the workspace to desktop-b afterwards
/// migrates the claimed tab there again.
#[tokio::test]
async fn successful_claim_rehomes_the_tab_to_the_driving_client_and_pin_changes_migrate() {
    let fx = boot().await;
    let mut a = connect(fx.port).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(fx.port).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;

    let ws_id = create_workspace(&mut a, 2, "Claim migration").await;
    let mut sub = connect(fx.port).await;
    let _ = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["browser:tab-updated"], "workspaceId": ws_id }),
    )
    .await;
    let _ = wss_rpc(
        &mut b,
        2,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-b", "https://b.test/") }),
    )
    .await;

    let call = tokio::spawn({
        let api = fx.api.clone();
        let ws_id = ws_id.clone();
        async move {
            api.browser_exec(
                WorkspaceId::from(ws_id.as_str()),
                vec![json!({ "action": "claimTab", "tabId": "tab-b", "width": 1280 })],
                None,
                Some(intent_core::AgentId::from("agent-1")),
            )
            .await
        }
    });
    let fe_result = json!({
        "success": true,
        "results": [{ "action": "claimTab", "success": true, "result": { "tabId": "tab-b" } }]
    });
    let forwarded = answer_reverse(&mut a, Duration::from_secs(2), fe_result).await;
    assert_eq!(forwarded["params"]["actions"][0]["action"], "claimTab");
    assert_eq!(forwarded["params"]["agentId"], "agent-1");
    let out = call.await.expect("join").expect("ok");
    assert_eq!(out["action"], "claimTab");
    assert!(
        try_read_text(&mut b, Duration::from_millis(200))
            .await
            .is_none(),
        "the claim executes on the driving client, not the tab's physical host"
    );

    let ev = await_event(&mut sub, "browser:tab-updated", Duration::from_secs(2)).await;
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-b");
    assert_eq!(ev["data"]["tab"]["hostClientId"], "desktop-a");
    assert_eq!(ev["data"]["tab"]["ownerAgentId"], "agent-1");
    assert_eq!(
        ev["data"]["changes"],
        json!({ "hostClientId": "desktop-a", "ownerAgentId": "agent-1" }),
        "{ev}"
    );
    assert_eq!(ev["actor"]["id"], "desktop-a", "{ev}");

    // The migrated claim now makes desktop-a the driving client by claimed
    // host too; pinning desktop-b moves the claimed tab back.
    let set = wss_rpc(
        &mut a,
        3,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": "desktop-b" }),
    )
    .await;
    assert!(set.get("error").is_none(), "{set}");
    let ev = await_event(&mut sub, "browser:tab-updated", Duration::from_secs(2)).await;
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-b");
    assert_eq!(ev["data"]["tab"]["hostClientId"], "desktop-b");
    assert_eq!(ev["data"]["tab"]["ownerAgentId"], "agent-1");
    assert_eq!(
        ev["data"]["changes"],
        json!({ "hostClientId": "desktop-b" }),
        "{ev}"
    );
    let listed = wss_rpc(
        &mut a,
        4,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        listed["result"]["tabs"][0]["hostClientId"], "desktop-b",
        "{listed}"
    );
    assert_eq!(listed["result"]["tabs"][0]["ownerAgentId"], "agent-1");

    fx.ws.stop().await;
}
