//! WSS end-to-end for lossless egress conflation under backpressure: a real
//! `events.subscribe` client that stalls its reads while the daemon pushes a
//! multi-megabyte `terminal:data` burst must still receive EVERY byte (chunks
//! may arrive merged — decoded content is what's asserted) and must see the
//! stream's `terminal:exit` barrier strictly AFTER all data, per the
//! conflation ordering guarantee. Drives a real [`WsApiServer`] over plain
//! `ws://` (insecure dev mode) so the WebSocket-upgrade → JSON-RPC → router →
//! bus-subscription → conflating forwarder → writer path is exercised
//! end-to-end.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
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
    let dir = std::env::temp_dir().join(format!("intentd-conflate-{}", &short[..8]));
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

fn terminal_event(ws_id: &str, event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(ws_id),
        timestamp: "2026-08-11T00:00:00.000Z".to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// A stalled `events.subscribe` consumer receiving a multi-megabyte
/// `terminal:data` burst gets every byte back — chunks may arrive merged
/// (conflated), but the decoded concatenation is exact — and the stream's
/// `terminal:exit` barrier arrives strictly after all of its data.
#[tokio::test]
async fn stalled_subscriber_receives_burst_losslessly_with_exit_after_data() {
    const CHUNK_BYTES: usize = 4 * 1024;
    const CHUNKS: usize = 600;

    let fx = boot().await;
    let mut ws = connect(fx.port).await;
    let ws_id = "ws-conflate-e2e";

    let sub = wss_rpc(
        &mut ws,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["terminal:data", "terminal:exit"] }),
    )
    .await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");

    // Publish the burst while the client is NOT reading: the socket and the
    // connection's outbound lane fill up, engaging the conflating forwarder.
    let mut expected: Vec<u8> = Vec::with_capacity(CHUNKS * CHUNK_BYTES);
    for i in 0..CHUNKS {
        let byte = (i % 251) as u8;
        let chunk = vec![byte; CHUNK_BYTES];
        expected.extend_from_slice(&chunk);
        fx.bus.publish_transient(&terminal_event(
            ws_id,
            "terminal:data",
            json!({ "terminalId": "t-1", "chunk": BASE64.encode(&chunk) }),
        ));
        // Keep the bus's delivery task ahead of the broadcast buffer, as the
        // in-daemon publishers do.
        tokio::task::yield_now().await;
    }
    fx.bus.publish_transient(&terminal_event(
        ws_id,
        "terminal:exit",
        json!({ "terminalId": "t-1", "exitCode": 0 }),
    ));

    // Resume reading: collect this subscription's frames until terminal:exit.
    let mut received: Vec<u8> = Vec::with_capacity(expected.len());
    let mut data_frames = 0usize;
    let mut saw_exit = false;
    timeout(Duration::from_secs(30), async {
        while !saw_exit {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] != json!("events.event")
                        || v["params"]["subscriptionId"] != json!(sub_id)
                    {
                        continue;
                    }
                    let event = &v["params"]["event"];
                    match event["type"].as_str() {
                        Some("terminal:data") => {
                            assert!(!saw_exit, "no data may follow the exit barrier");
                            let chunk = event["data"]["chunk"].as_str().expect("chunk");
                            received.extend_from_slice(&BASE64.decode(chunk).unwrap());
                            data_frames += 1;
                        }
                        Some("terminal:exit") => saw_exit = true,
                        other => panic!("unexpected event type {other:?}"),
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => panic!("connection closed mid-stream"),
                _ => {}
            }
        }
    })
    .await
    .expect("burst + exit not fully received in time");

    assert_eq!(
        received.len(),
        expected.len(),
        "lossless: every published byte arrives exactly once \
         ({data_frames} data frames for {CHUNKS} published chunks)"
    );
    assert_eq!(received, expected, "byte content and order are exact");
    assert!(saw_exit, "terminal:exit arrives after all data");
}
