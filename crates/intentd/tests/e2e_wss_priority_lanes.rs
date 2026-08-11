//! WSS end-to-end for the two-lane outbound queue: an RPC response
//! (`host.status`) must overtake a saturated stream of `events.event`
//! notifications. The bulk lane is flooded with large transient `file:*`
//! events while the client is NOT reading, so the socket back-pressures and
//! frames queue in the daemon; a `host.status` sent at that point must be
//! answered on the priority lane — i.e. arrive over the wire BEFORE the
//! queued event traffic has drained. On the old single-FIFO queue the
//! response could only arrive after every previously queued event frame.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intent_core::{ActorType, EventActor, WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::{NewEvent, Store};
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Enough queued event bytes to overflow loopback socket buffers many times
/// over, so bulk frames are still queued daemon-side when the RPC arrives.
const EVENT_COUNT: usize = 200;
const EVENT_PAYLOAD_BYTES: usize = 64 * 1024;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _ws: WsApiServer,
    bus: EventBus,
    port: u16,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-priolane-{}", &short[..8]));
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
    let ws = WsApiServer::new_insecure(api, bus.clone(), opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        bus,
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

async fn send_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
}

/// Read frames until the response with `id` arrives; panics on error frames.
async fn read_until_response(ws: &mut PlainWs, id: i64) -> Value {
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        assert!(v.get("error").is_none(), "rpc {id} errored: {v}");
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

/// A transient `file:changed` event (non-agent actor ⇒ never persisted, so
/// SQLite cannot throttle the flood) with a `payload` of `size` bytes.
fn flood_event(i: usize, size: usize) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from("ws-priority-lanes"),
        timestamp: "2026-08-11T00:00:00.000Z".to_string(),
        event_type: "file:changed".to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "seq": i, "payload": "x".repeat(size) }),
    }
}

/// Saturate the bulk lane with ~12.5 MiB of `events.event` notifications while
/// the client is not reading (kernel buffers fill, the daemon writer blocks,
/// frames queue on the bulk lane), then send `host.status`. The response must
/// arrive over the wire while event frames are still queued — i.e. at least
/// one `events.event` follows it — proving the priority lane overtakes bulk.
/// On a single-FIFO outbound queue the response could only arrive after every
/// previously queued event frame.
#[tokio::test]
async fn rpc_response_overtakes_saturated_event_stream() {
    let fx = boot().await;
    let mut ws = connect(fx.port).await;

    // Subscribe to the flood category over the wire (fast-path).
    send_rpc(
        &mut ws,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["file:*"] }),
    )
    .await;
    let sub = read_until_response(&mut ws, 1).await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe confirm: {sub}"
    );

    // Flood: transient publishes broadcast synchronously (no SQLite in the
    // way); the forwarder queues them on the bulk lane. The client is NOT
    // reading, so the socket back-pressures and the writer blocks mid-drain.
    for i in 0..EVENT_COUNT {
        fx.bus
            .publish(&flood_event(i, EVENT_PAYLOAD_BYTES))
            .await
            .expect("publish flood event");
    }
    // Let the delivery pipeline saturate (forwarder → bulk lane → blocked
    // socket write) before the RPC lands.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The latency-critical RPC, sent while megabytes of events are queued.
    send_rpc(&mut ws, 2, "host.status", json!({})).await;

    // Drain the socket, recording where the response lands in the stream.
    let mut events_before_response = 0usize;
    let mut events_after_response = 0usize;
    let mut response_seen = false;
    timeout(common::rpc_read_timeout(), async {
        while events_before_response + events_after_response < EVENT_COUNT || !response_seen {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(2)) {
                        assert!(v.get("error").is_none(), "host.status errored: {v}");
                        response_seen = true;
                    } else if v.get("method") == Some(&json!("events.event")) {
                        if response_seen {
                            events_after_response += 1;
                        } else {
                            events_before_response += 1;
                        }
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                other => panic!("unexpected message: {other:?}"),
            }
        }
    })
    .await
    .expect("drain timeout: response or events never arrived");

    assert_eq!(
        events_before_response + events_after_response,
        EVENT_COUNT,
        "no event may be lost"
    );
    assert!(
        events_after_response > 0,
        "host.status must overtake queued bulk traffic \
         (events before response: {events_before_response}, after: {events_after_response})"
    );
}
