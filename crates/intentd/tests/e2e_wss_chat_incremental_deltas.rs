//! WSS end-to-end for the opt-in incremental chat delta encoding
//! (monorepo#2675): `chat.subscribe { deltaEncoding: "incremental" }` makes
//! live text/thinking chunk deltas carry only the new fragment (`textDelta`)
//! instead of the full accumulated text, eliminating the quadratic wire
//! amplification. The seq-0 snapshot (and any lag-recovery snapshot) echoes
//! `deltaEncoding: "incremental"`, the terminal `stream:end` reconcile stays
//! full-text/authoritative, and a client applying the documented append
//! reducer converges to `agent.getConversation` (§7.1).
//!
//! Drives a real [`WsApiServer`] over TLS with bearer-token auth and a pinned
//! self-signed fingerprint — the production transport path.

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
    let dir = std::env::temp_dir().join(format!("intentd-chatinc-{}", &short[..8]));
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

/// Like [`wss_rpc`] but returns the raw response frame so error envelopes can
/// be asserted (invalid `deltaEncoding` → `-32602`).
async fn wss_rpc_raw(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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

/// Apply one incremental-mode delta entity onto a reconstructed `messages[]`:
/// a block carrying `textDelta` appends the fragment to the existing block's
/// `text` (creating it empty first); a full block replaces/inserts as-is.
/// This is the documented client reducer for `deltaEncoding: "incremental"`.
fn apply_incremental_entity(messages: &mut Vec<Value>, entity: &Value) {
    let message_id = entity["messageId"].as_str().expect("messageId").to_string();
    let idx = messages
        .iter()
        .position(|m| m["id"].as_str() == Some(message_id.as_str()))
        .unwrap_or_else(|| {
            messages.push(json!({
                "id": message_id,
                "agentId": Value::Null,
                "seq": Value::Null,
                "role": Value::Null,
                "contentBlocks": [],
                "timestamp": Value::Null,
            }));
            messages.len() - 1
        });
    let msg = &mut messages[idx];
    if let Some(v) = entity.get("agentId") {
        msg["agentId"] = v.clone();
    }
    if let Some(v) = entity.get("role") {
        msg["role"] = v.clone();
    }
    if let Some(v) = entity.get("messageSeq") {
        msg["seq"] = v.clone();
    }
    if let Some(v) = entity.get("timestamp") {
        msg["timestamp"] = v.clone();
    }
    if entity.get("streamingComplete") == Some(&Value::Bool(true)) {
        if let Some(obj) = msg.as_object_mut() {
            obj.remove("isStreaming");
        }
    }
    let block = entity["block"].clone();
    let block_id = block["id"].as_str().expect("block id").to_string();
    let blocks = msg["contentBlocks"].as_array_mut().expect("contentBlocks");
    let pos = blocks
        .iter()
        .position(|b| b["id"].as_str() == Some(block_id.as_str()));
    if let Some(fragment) = block.get("textDelta").and_then(|v| v.as_str()) {
        // Incremental fragment: append onto the accumulated text.
        match pos {
            Some(bi) => {
                let acc = blocks[bi]["text"].as_str().unwrap_or_default().to_string();
                blocks[bi]["text"] = json!(format!("{acc}{fragment}"));
            }
            None => {
                let mut b = block.clone();
                b.as_object_mut().unwrap().remove("textDelta");
                b["text"] = json!(fragment);
                blocks.push(b);
            }
        }
    } else {
        // Full block (non-text passthrough or terminal reconcile): upsert.
        match pos {
            Some(bi) => blocks[bi] = block,
            None => blocks.push(block),
        }
    }
}

/// Reduce one `{ added, updated, removedIds }` incremental delta onto
/// `messages`.
fn apply_incremental_delta(messages: &mut Vec<Value>, delta: &Value) {
    for key in ["added", "updated"] {
        for entity in delta[key].as_array().into_iter().flatten() {
            apply_incremental_entity(messages, entity);
        }
    }
    for removed in delta["removedIds"].as_array().into_iter().flatten() {
        let Some(id) = removed.as_str() else { continue };
        for msg in messages.iter_mut() {
            if let Some(blocks) = msg["contentBlocks"].as_array_mut() {
                blocks.retain(|b| b["id"].as_str() != Some(id));
            }
        }
    }
}

/// Whether a delta is the terminal (`stream:end`) reconcile frame — its
/// entities carry `streamingComplete: true`.
fn is_terminal_delta(delta: &Value) -> bool {
    ["added", "updated"].iter().any(|key| {
        delta[*key]
            .as_array()
            .into_iter()
            .flatten()
            .any(|e| e.get("streamingComplete") == Some(&Value::Bool(true)))
    })
}

/// Every non-terminal text-block entity in a delta, as `(bucket, block)`.
fn text_block_entities(delta: &Value) -> Vec<(&'static str, Value)> {
    let mut out = Vec::new();
    for key in ["added", "updated"] {
        for e in delta[key].as_array().into_iter().flatten() {
            if e["block"]["type"] == json!("text") && e.get("streamingComplete").is_none() {
                out.push((
                    if key == "added" { "added" } else { "updated" },
                    e["block"].clone(),
                ));
            }
        }
    }
    out
}

/// The full opt-in path over the real WSS transport: the seq-0 snapshot echoes
/// `deltaEncoding: "incremental"`, live text chunk deltas carry ONLY the new
/// fragment (`textDelta`, never accumulated `text`), non-text blocks pass
/// through whole, the terminal reconcile emits authoritative full blocks, and
/// the documented append reducer converges to `agent.getConversation` (§7.1).
#[tokio::test]
async fn chat_incremental_subscription_streams_fragments_and_converges() {
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

    let sub = wss_rpc(
        &mut ws,
        3,
        "chat.subscribe",
        json!({ "agentId": agent_id, "deltaEncoding": "incremental" }),
    )
    .await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");
    let snap = next_push(&mut ws, sub_id).await;
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    assert_eq!(
        snap["params"]["snapshot"]["deltaEncoding"],
        json!("incremental"),
        "the seq-0 snapshot echoes the negotiated encoding: {snap}"
    );
    let mut reconstructed: Vec<Value> = snap["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");
    assert_eq!(reconstructed.len(), 1, "snapshot holds the user message");

    // Drive an assistant turn: two fragments of the same text block, a tool
    // call (non-text passthrough), and a closing text block.
    let mid = Uuid::now_v7().to_string();
    let chunk = |idx: u64, text: &str| {
        json!({
            "agentId": agent_id, "content": text, "messageId": mid,
            "blockIndex": idx, "blockId": format!("{mid}:{idx}"), "blockType": "text",
        })
    };
    for data in [
        chunk(0, "I'll run "),
        chunk(0, "the tests."),
        json!({
            "agentId": agent_id, "toolName": "run_tests", "toolKind": "terminal",
            "toolCallId": "call_abc", "input": { "path": "." }, "status": "started",
            "messageId": mid, "blockIndex": 1, "blockId": format!("{mid}:1"),
            "blockType": "tool_use",
        }),
        chunk(2, "Done."),
    ] {
        let event_type = if data.get("toolName").is_some() {
            intent_core::events::AGENT_TOOL_CALL
        } else {
            CHAT_STREAM_DELTA
        };
        fx.bus
            .publish(&stream_event(&ws_id, &agent_id, event_type, data))
            .await
            .expect("publish stream event");
    }

    // Persist the assistant message BEFORE stream:end (as run_prompt_turn
    // does), so the terminal reconcile re-reads the durable transcript.
    store
        .append_agent_message_with_id(
            &AgentId::from(agent_id.as_str()),
            &mid,
            "assistant",
            &json!([
                { "type": "text", "id": format!("{mid}:0"), "text": "I'll run the tests." },
                { "type": "tool_use", "id": format!("{mid}:1"), "name": "run_tests",
                  "input": { "path": "." }, "toolCallId": "call_abc",
                  "metadata": { "toolKind": "terminal", "status": "started" } },
                { "type": "text", "id": format!("{mid}:2"), "text": "Done." },
            ]),
            None,
            &now_iso(),
        )
        .await
        .expect("append assistant message");
    fx.bus
        .publish(&stream_event(
            &ws_id,
            &agent_id,
            AGENT_STREAM_END,
            json!({ "agentId": agent_id, "messageId": mid }),
        ))
        .await
        .expect("publish stream end");

    // Reduce every delta until the terminal frame; assert the wire shapes.
    let mut expected_seq = 1u64;
    let mut fragments: Vec<String> = Vec::new();
    loop {
        let frame = next_push(&mut ws, sub_id).await;
        assert_eq!(frame["params"]["kind"], "delta");
        assert_eq!(
            frame["params"]["seq"].as_u64().unwrap(),
            expected_seq,
            "delta seq is contiguous from 1"
        );
        expected_seq += 1;
        let delta = frame["params"]["delta"].clone();
        let terminal = is_terminal_delta(&delta);
        for (_bucket, block) in text_block_entities(&delta) {
            if terminal {
                assert!(
                    block.get("textDelta").is_none(),
                    "the terminal frame is full-text, never incremental: {delta}"
                );
            } else {
                let fragment = block["textDelta"]
                    .as_str()
                    .unwrap_or_else(|| panic!("live text block must carry textDelta: {delta}"));
                assert!(
                    block.get("text").is_none(),
                    "incremental deltas never carry accumulated text: {delta}"
                );
                fragments.push(fragment.to_string());
            }
        }
        apply_incremental_delta(&mut reconstructed, &delta);
        if terminal {
            break;
        }
    }
    assert_eq!(
        fragments,
        vec!["I'll run ", "the tests.", "Done."],
        "each live delta carried exactly the new fragment"
    );

    // Convergence (§7.1): snapshot + deltas equals a fresh conversation page.
    let want = wss_rpc(
        &mut ws,
        4,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        Value::Array(reconstructed),
        want["messages"],
        "snapshot + incremental deltas reconcile to the fresh conversation"
    );
}

/// Omitting `deltaEncoding` keeps the wire byte-identical to today: no
/// `deltaEncoding` echo on the snapshot and full accumulated `text` (no
/// `textDelta`) on live chunk deltas.
#[tokio::test]
async fn chat_default_subscription_still_streams_full_text() {
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

    let sub = wss_rpc(&mut ws, 3, "chat.subscribe", json!({ "agentId": agent_id })).await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");
    let snap = next_push(&mut ws, sub_id).await;
    assert_eq!(snap["params"]["seq"], 0);
    assert!(
        snap["params"]["snapshot"].get("deltaEncoding").is_none(),
        "default subscriptions stamp no deltaEncoding: {snap}"
    );

    let mid = Uuid::now_v7().to_string();
    for text in ["Hello", ", world"] {
        fx.bus
            .publish(&stream_event(
                &ws_id,
                &agent_id,
                CHAT_STREAM_DELTA,
                json!({
                    "agentId": agent_id, "content": text, "messageId": mid,
                    "blockIndex": 0, "blockId": format!("{mid}:0"), "blockType": "text",
                }),
            ))
            .await
            .expect("publish chunk");
    }
    let first = next_push(&mut ws, sub_id).await;
    assert_eq!(
        first["params"]["delta"]["added"][0]["block"]["text"],
        "Hello"
    );
    let second = next_push(&mut ws, sub_id).await;
    let block = &second["params"]["delta"]["updated"][0]["block"];
    assert_eq!(
        block["text"],
        json!("Hello, world"),
        "full mode still accumulates: {second}"
    );
    assert!(
        block.get("textDelta").is_none(),
        "full mode never emits textDelta: {second}"
    );
}

/// An unknown `deltaEncoding` is rejected as invalid params (`-32602`), never
/// silently coerced to a mode the client did not ask for.
#[tokio::test]
async fn chat_subscribe_rejects_unknown_delta_encoding() {
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

    let resp = wss_rpc_raw(
        &mut ws,
        3,
        "chat.subscribe",
        json!({ "agentId": agent_id, "deltaEncoding": "diff" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602, "invalid params: {resp}");
}

/// Lag recovery on an incremental subscription re-emits a BOUNDED snapshot at
/// the next seq that ALSO echoes `deltaEncoding: "incremental"`, and the
/// replacement mapper keeps streaming fragments afterwards.
#[tokio::test]
async fn chat_incremental_lag_recovery_snapshot_echoes_encoding() {
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

    let sub = wss_rpc(
        &mut ws,
        3,
        "chat.subscribe",
        json!({ "agentId": agent_id, "deltaEncoding": "incremental" }),
    )
    .await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");
    let snap = next_push(&mut ws, sub_id).await;
    assert_eq!(snap["params"]["seq"], 0);
    assert_eq!(
        snap["params"]["snapshot"]["deltaEncoding"],
        json!("incremental")
    );

    // A first live chunk proves the stream is flowing (seq 1)…
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
    assert_eq!(first["params"]["seq"], 1);
    assert_eq!(
        first["params"]["delta"]["added"][0]["block"]["textDelta"],
        "I'll run "
    );

    // …then the turn's tail is lost to a broadcast-ring overflow: the durable
    // assistant row exists, but the trailing chunk + stream:end are buried
    // under a non-yielding transient flood (ring capacity 1024).
    let store = fx.bus.store();
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

    // The recovery snapshot takes the next seq and echoes the encoding.
    let recovery = next_push(&mut ws, sub_id).await;
    assert_eq!(
        recovery["params"]["kind"], "snapshot",
        "lag recovery re-emits a snapshot: {recovery}"
    );
    assert_eq!(recovery["params"]["seq"], 2, "recovery takes the next seq");
    assert_eq!(
        recovery["params"]["snapshot"]["deltaEncoding"],
        json!("incremental"),
        "the recovery snapshot echoes the negotiated encoding: {recovery}"
    );

    // The replacement mapper keeps the incremental encoding for the next turn.
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
    assert_eq!(
        next["params"]["delta"]["added"][0]["block"]["textDelta"], "Next",
        "post-recovery deltas stay incremental: {next}"
    );
}
