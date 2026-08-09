//! WSS end-to-end for the daemon-wide outstanding-slow-path-RPC cap
//! (`server.maxOutstandingRpcs`). Drives a real [`WsApiServer`] over plain
//! `ws://` (insecure dev mode) so the WebSocket-upgrade → JSON-RPC → limiter →
//! router round-trip is exercised end-to-end.
//!
//! The contract under test: once the cap is reached, further slow-path requests
//! are REJECTED immediately with `-32011 "Server overloaded"` echoing the
//! request `id` (never queued, never delayed); notification-shaped frames get
//! no response at all; and once the in-flight requests drain, the freed slots
//! serve new requests normally. With the cap unset (unlimited) a concurrent
//! burst is unaffected.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{RpcLimiter, WsApiServer, WsOptions};
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

/// Boot a daemon whose UDS+WSS listeners share one limiter capped at
/// `max_outstanding` (`0` = unlimited, the shipped "off" value).
async fn boot(max_outstanding: u32) -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-rpclimit-{}", &short[..8]));
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
        rpc_limiter: RpcLimiter::new(max_outstanding),
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

/// A slow `host.exec` that occupies one limiter slot for `seconds`.
fn sleep_request(id: i64, seconds: &str) -> Message {
    Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "host.exec",
            "params": { "command": "/bin/sleep", "args": [seconds] },
        })
        .to_string()
        .into(),
    )
}

async fn send(ws: &mut PlainWs, msg: Message) {
    ws.send(msg).await.expect("send frame");
}

/// Read response frames until `wanted` distinct ids have arrived, returning
/// them keyed by `id`. Pings/pongs and unrelated pushes are skipped.
async fn collect_responses(ws: &mut PlainWs, wanted: &[i64]) -> Vec<(i64, Value)> {
    timeout(common::rpc_read_timeout(), async {
        let mut got: Vec<(i64, Value)> = Vec::new();
        while got.len() < wanted.len() {
            match ws.next().await.expect("stream open").expect("frame") {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    let Some(id) = v.get("id").and_then(Value::as_i64) else {
                        continue;
                    };
                    if wanted.contains(&id) && !got.iter().any(|(seen, _)| *seen == id) {
                        got.push((id, v));
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                other => panic!("unexpected message: {other:?}"),
            }
        }
        got
    })
    .await
    .expect("responses timed out")
}

/// Assert one frame is the exact `-32011` overload envelope with the echoed id.
fn assert_overload(id: i64, frame: &Value) {
    assert_eq!(frame["jsonrpc"], "2.0", "overload frame: {frame}");
    assert_eq!(frame["id"], json!(id), "overload frame echoes id: {frame}");
    assert_eq!(
        frame["error"]["code"],
        json!(-32011),
        "overload code: {frame}"
    );
    assert_eq!(
        frame["error"]["message"], "Server overloaded",
        "overload message: {frame}"
    );
    assert!(
        frame.get("result").is_none(),
        "overload frame carries no result: {frame}"
    );
}

/// With the cap at 1, a second concurrent slow request is rejected with the
/// exact `-32011` envelope while the first is still in flight; when the
/// in-flight request drains, the freed slot serves a new request normally.
#[tokio::test]
async fn over_limit_requests_are_rejected_and_slots_are_reusable() {
    let fx = boot(1).await;
    let mut ws = connect(fx.port).await;

    // Occupy the single slot with a slow request, then flood.
    send(&mut ws, sleep_request(1, "2")).await;
    for id in 2..=5 {
        send(&mut ws, sleep_request(id, "2")).await;
    }

    // Ids 2..=5 must all come back as overload rejections — immediately, long
    // before the in-flight sleep finishes.
    let rejected = collect_responses(&mut ws, &[2, 3, 4, 5]).await;
    for (id, frame) in &rejected {
        assert_overload(*id, frame);
    }

    // The in-flight request still completes successfully.
    let [(_, first)] = collect_responses(&mut ws, &[1]).await.try_into().unwrap();
    assert!(
        first.get("error").is_none(),
        "the in-flight request must succeed: {first}"
    );

    // Its slot is released, so a fresh request is served normally.
    send(
        &mut ws,
        Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "host.exec",
                "params": { "command": "/bin/echo", "args": ["ok"] },
            })
            .to_string()
            .into(),
        ),
    )
    .await;
    let [(_, after)] = collect_responses(&mut ws, &[6]).await.try_into().unwrap();
    assert!(
        after.get("error").is_none(),
        "a drained slot must serve new requests: {after}"
    );
}

/// A notification-shaped frame (no `id`) rejected at the cap gets NO response
/// (PROTOCOL §9), and the connection keeps serving subsequent requests.
#[tokio::test]
async fn over_limit_notifications_get_no_response() {
    let fx = boot(1).await;
    let mut ws = connect(fx.port).await;

    send(&mut ws, sleep_request(1, "2")).await;
    // No `id` ⇒ notification; it hits the cap and must be dropped silently.
    send(
        &mut ws,
        Message::Text(
            json!({
                "jsonrpc": "2.0",
                "method": "host.exec",
                "params": { "command": "/bin/echo", "args": ["dropped"] },
            })
            .to_string()
            .into(),
        ),
    )
    .await;
    // A follow-up request also hits the cap and DOES answer, proving the
    // notification produced no frame ahead of it (frames are ordered).
    send(&mut ws, sleep_request(2, "2")).await;

    let [(_, rejected)] = collect_responses(&mut ws, &[2]).await.try_into().unwrap();
    assert_overload(2, &rejected);
    let [(_, first)] = collect_responses(&mut ws, &[1]).await.try_into().unwrap();
    assert!(first.get("error").is_none(), "in-flight succeeds: {first}");
}

/// Envelope validation is not masked by the cap: with the limiter saturated,
/// malformed JSON still answers `-32700` and an invalid envelope still answers
/// `-32600` — including an invalid notification-shaped frame, which the router
/// must answer even though valid notifications get no response.
#[tokio::test]
async fn invalid_frames_keep_their_error_codes_at_the_cap() {
    let fx = boot(1).await;
    let mut ws = connect(fx.port).await;

    // Saturate the single slot.
    send(&mut ws, sleep_request(1, "2")).await;

    // Malformed JSON → -32700 with a null id.
    send(&mut ws, Message::Text("{ not json".to_string().into())).await;
    // Invalid envelope, notification-shaped (no id) → -32600 with a null id.
    send(
        &mut ws,
        Message::Text(
            json!({ "jsonrpc": "1.0", "method": "workspace.list" })
                .to_string()
                .into(),
        ),
    )
    .await;

    let frames = timeout(common::rpc_read_timeout(), async {
        let mut got: Vec<Value> = Vec::new();
        while got.len() < 2 {
            match ws.next().await.expect("stream open").expect("frame") {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["id"].is_null() && v.get("error").is_some() {
                        got.push(v);
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                other => panic!("unexpected message: {other:?}"),
            }
        }
        got
    })
    .await
    .expect("error frames timed out");

    let codes: Vec<i64> = frames
        .iter()
        .map(|f| f["error"]["code"].as_i64().unwrap())
        .collect();
    assert_eq!(
        codes,
        vec![-32700, -32600],
        "the cap must not mask the router's error matrix: {frames:?}"
    );

    let [(_, first)] = collect_responses(&mut ws, &[1]).await.try_into().unwrap();
    assert!(first.get("error").is_none(), "in-flight succeeds: {first}");
}

/// With the cap unset (`0` = unlimited) a concurrent burst is unaffected: every
/// request succeeds and none is rejected.
#[tokio::test]
async fn unlimited_cap_never_rejects() {
    let fx = boot(0).await;
    let mut ws = connect(fx.port).await;

    let ids: Vec<i64> = (1..=8).collect();
    for id in &ids {
        send(
            &mut ws,
            Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "host.exec",
                    "params": { "command": "/bin/echo", "args": ["ok"] },
                })
                .to_string()
                .into(),
            ),
        )
        .await;
    }
    for (id, frame) in collect_responses(&mut ws, &ids).await {
        assert!(
            frame.get("error").is_none(),
            "request {id} must not be rejected under an unlimited cap: {frame}"
        );
    }
}

/// Normal traffic below the cap is unaffected: a burst smaller than the limit
/// all succeeds.
#[tokio::test]
async fn traffic_under_the_limit_is_unaffected() {
    let fx = boot(8).await;
    let mut ws = connect(fx.port).await;

    let ids: Vec<i64> = (1..=4).collect();
    for id in &ids {
        send(
            &mut ws,
            Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "host.exec",
                    "params": { "command": "/bin/echo", "args": ["ok"] },
                })
                .to_string()
                .into(),
            ),
        )
        .await;
    }
    for (id, frame) in collect_responses(&mut ws, &ids).await {
        assert!(
            frame.get("error").is_none(),
            "request {id} under the cap must succeed: {frame}"
        );
    }
}
