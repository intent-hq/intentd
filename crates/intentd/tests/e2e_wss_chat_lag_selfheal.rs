//! WSS end-to-end for the chat subscription's lag self-heal: a broadcast-ring
//! drop that swallows a turn's tail (trailing `chat:stream:delta` +
//! `agent:stream:end`) must not strand the client transcript mid-turn. The
//! forwarder receives the in-band lag marker and re-emits a fresh BOUNDED
//! snapshot at the next seq — the client's reconciler rebuilds from it (no seq
//! gap, no resubscribe) and the subscription stays live for the next turn.
//!
//! Drives a real [`WsApiServer`] over TLS with bearer-token auth and a pinned
//! self-signed fingerprint (the production transport path), so the
//! WebSocket-upgrade → JSON-RPC → router → bus-subscription → chat forwarder →
//! writer path is exercised end-to-end. The drop is forced deterministically:
//! on the test's current-thread runtime a non-yielding `publish_transient`
//! flood starves the delivery task, so the ring (capacity 1024) drops the
//! oldest undelivered events — the tail published first — before the task
//! ever runs.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::events::{AGENT_STREAM_END, CHAT_STREAM_DELTA};
use intent_core::{
    now_iso, ActorType, AgentId, EventActor, Result as CoreResult, WorkspaceApi, WorkspaceId,
};
use intent_services::{EventBus, Services};
use intent_store::{NewEvent, Store};
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "cececececececececececececececececececececececececececececececece";

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex)
/// and otherwise validates the handshake signature with the ring provider.
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

struct Fixture {
    _ws: WsApiServer,
    bus: EventBus,
    port: u16,
    cfg: Arc<ClientConfig>,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-chatlag-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(common::registry_with_default_provider(&dir))
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new(api, bus.clone(), &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        bus,
        port,
        cfg,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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

/// Await the next `subscription.push` frame for `sub_id`, skipping unrelated
/// traffic (pings, other notifications).
async fn next_push(ws: &mut TlsWs, sub_id: &str) -> Value {
    timeout(Duration::from_secs(15), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("subscription.push")
                        && v["params"]["subscriptionId"] == json!(sub_id)
                    {
                        return v;
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => panic!("connection closed mid-stream"),
                _ => {}
            }
        }
    })
    .await
    .expect("subscription.push timeout")
}

/// One `agent:stream:*`-family transient event scoped to `agent_id` (the chat
/// forwarder narrows by `sessionId == agentId`).
fn stream_event(ws_id: &str, agent_id: &str, event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from(ws_id),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(agent_id.to_string()),
            ..Default::default()
        },
        session_id: Some(agent_id.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// A broadcast-ring drop that swallows the turn's tail (trailing chunk +
/// `agent:stream:end`) heals over the real WSS transport: the client receives
/// a fresh snapshot at the next seq that equals `agent.getConversation`
/// (converged, not mid-turn), then keeps receiving the next turn's deltas.
#[tokio::test]
async fn chat_subscription_self_heals_over_wss_after_broadcast_lag() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let created = wss_rpc(&mut ws, 1, "workspace.create", json!({ "title": "WS" })).await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let a = wss_rpc(
        &mut ws,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "A1" }),
    )
    .await;
    let agent_id = a["agent"]["id"].as_str().unwrap().to_string();

    // A persisted user message anchors the seq-0 snapshot.
    let store = fx.bus.store();
    let user_id = Uuid::now_v7().to_string();
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &user_id,
            "user",
            &json!([{ "type": "text", "id": format!("{user_id}:0"), "text": "Run the tests" }]),
            None,
            &now_iso(),
        )
        .await
        .expect("append user message");

    let sub = wss_rpc(&mut ws, 3, "chat.subscribe", json!({ "agentId": agent_id })).await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");
    let snap = next_push(&mut ws, sub_id).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);

    // The turn starts normally: the first chunk arrives as delta seq 1.
    let mid = Uuid::now_v7().to_string();
    fx.bus
        .publish(&stream_event(
            &ws_id,
            &agent_id,
            CHAT_STREAM_DELTA,
            json!({
                "agentId": agent_id, "content": "I'll run ", "messageId": mid,
                "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
            }),
        ))
        .await
        .expect("publish first chunk");
    let first = next_push(&mut ws, sub_id).await;
    assert_eq!(first["params"]["kind"], "delta");
    assert_eq!(first["params"]["seq"], 1);

    // The turn completes durably, but its live tail is LOST: the trailing
    // chunk and `stream:end` are published into the ring and buried under a
    // non-yielding transient flood that overflows the ring (capacity 1024)
    // before the delivery task can drain.
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &mid,
            "assistant",
            &json!([
                { "type": "text", "id": format!("{mid}:0"), "text": "I'll run the tests." },
            ]),
            None,
            &now_iso(),
        )
        .await
        .expect("append assistant message");
    let _ = fx.bus.publish_transient(&stream_event(
        &ws_id,
        &agent_id,
        CHAT_STREAM_DELTA,
        json!({
            "agentId": agent_id, "content": "the tests.", "messageId": mid,
            "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
        }),
    ));
    let _ = fx.bus.publish_transient(&stream_event(
        &ws_id,
        &agent_id,
        AGENT_STREAM_END,
        json!({ "agentId": agent_id }),
    ));
    for _ in 0..2048 {
        let _ = fx.bus.publish_transient(&NewEvent {
            workspace_id: WorkspaceId::from(ws_id.as_str()),
            timestamp: now_iso(),
            event_type: "note:created".to_string(),
            actor: EventActor {
                actor_type: ActorType::User,
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({}),
        });
    }

    // Self-heal: the next push is a fresh snapshot at the next seq (no gap),
    // equal to a fresh bounded getConversation page — converged, not mid-turn.
    let recovery = next_push(&mut ws, sub_id).await;
    assert_eq!(
        recovery["params"]["kind"], "snapshot",
        "lag recovery re-emits a snapshot, got: {recovery}"
    );
    assert_eq!(recovery["params"]["seq"], 2, "recovery takes the next seq");
    let want = wss_rpc(
        &mut ws,
        4,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let mut want = want;
    let want_obj = want.as_object_mut().unwrap();
    want_obj.insert("isResponding".into(), json!(false));
    want_obj.insert("isWaitingOnTool".into(), json!(false));
    want_obj.insert("isWaitingForOtherAgents".into(), json!(false));
    want_obj.insert("waitingForAgentIds".into(), json!([]));
    assert_eq!(
        recovery["params"]["snapshot"], want,
        "recovery snapshot equals a fresh getConversation page"
    );
    let messages = recovery["params"]["snapshot"]["messages"]
        .as_array()
        .unwrap();
    assert_eq!(messages.len(), 2, "user + persisted assistant message");
    assert!(
        messages.iter().all(|m| m.get("isStreaming").is_none()),
        "the recovered transcript is not stranded mid-turn"
    );

    // The subscription stays live: the next turn's chunk arrives as a delta
    // at the following seq.
    let mid2 = Uuid::now_v7().to_string();
    fx.bus
        .publish(&stream_event(
            &ws_id,
            &agent_id,
            CHAT_STREAM_DELTA,
            json!({
                "agentId": agent_id, "content": "Next", "messageId": mid2,
                "blockIndex": 0, "blockId": format!("{mid2}:0"), "blockType": "text",
            }),
        ))
        .await
        .expect("publish next-turn chunk");
    let next = next_push(&mut ws, sub_id).await;
    assert_eq!(next["params"]["kind"], "delta");
    assert_eq!(next["params"]["seq"], 3);
}
