//! WSS end-to-end for capped and paged `agent.getMessageBlock` reads and
//! `agent.editAndRegenerate` stored image block references
//! (docs/protocol/methods/agents.md §5.5 "Capped and paged block reads",
//! capability `messageBlockPages: 1`), over the production wire path (TLS
//! upgrade → JSON-RPC → router → services → store).

#![cfg(unix)]

use crate::common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use intent_core::{AgentId, Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
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

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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
        .unwrap()
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
    port: u16,
    cfg: Arc<ClientConfig>,
    store: Store,
    _dir: tempfile::TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over a hermetic workspaces root.
async fn boot() -> Fixture {
    let tmp = common::test_tempdir("intentd-msgblock-");
    let dir = tmp.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new_isolated(store.clone())
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
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        cfg,
        store,
        _dir: tmp,
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

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
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// The response frame budget every `agent.getMessageBlock` reply must meet.
const READ_BUDGET_BYTES: usize = 512_000;

/// Create a workspace and an agent, append one assistant message with the
/// given blocks, and return `(workspaceId, agentId, messageId)`.
async fn seed(rpc: &mut TlsWs, blocks: Value) -> (String, String, String) {
    let created = wss_rpc(
        rpc,
        1,
        "workspace.create",
        json!({ "title": "message block e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let agent = wss_rpc(
        rpc,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Blocks" }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_string();
    let appended = wss_rpc(
        rpc,
        3,
        "agent.appendMessage",
        json!({ "agentId": agent_id, "role": "user", "contentBlocks": blocks }),
    )
    .await;
    let message_id = appended["message"]["id"].as_str().unwrap().to_string();
    (ws_id, agent_id, message_id)
}

/// Send a raw request and return the reply plus its frame size.
async fn block_raw(rpc: &mut TlsWs, id: i64, params: Value) -> (Value, usize) {
    let v = wss_rpc_raw(rpc, id, "agent.getMessageBlock", params).await;
    let size = serde_json::to_vec(&v).unwrap().len();
    (v, size)
}

#[intent_test_macros::daemon_test]
async fn hello_advertises_message_block_pages() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let hello = wss_rpc(&mut rpc, 1, "client.hello", json!({})).await;
    assert_eq!(
        hello["server"]["capabilities"]["messageBlockPages"], 1,
        "{hello}"
    );
    let caps = wss_rpc(&mut rpc, 2, "system.capabilities", json!({})).await;
    assert_eq!(caps["messageBlockPages"], 1, "{caps}");
}

/// A small block keeps its stored shape inside `block` and the result
/// carries exactly the documented keys; multi-MB text and tool output
/// page under the budget and reassemble byte-for-byte; an over-cap image
/// is served without `data`.
#[intent_test_macros::daemon_test]
async fn get_message_block_is_capped_and_pages_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let text = "wire ✓ \"page\"\n\u{1}".repeat(200_000);
    let input = json!({ "command": "x".repeat(1_500_000), "args": ["a", "b"] });
    let image = "QUJD".repeat(400_000);
    let (_ws, agent_id, message_id) = seed(
        &mut rpc,
        json!([
            { "type": "text", "text": "small" },
            { "type": "text", "text": text },
            { "type": "tool_use", "id": "tu", "name": "Bash", "input": input },
            { "type": "image", "data": image, "mimeType": "image/png" },
        ]),
    )
    .await;

    let (small, _) = block_raw(
        &mut rpc,
        10,
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": format!("{message_id}:0") }),
    )
    .await;
    assert_eq!(
        small["result"],
        json!({
            "block": { "type": "text", "text": "small", "id": format!("{message_id}:0") },
            "truncated": false,
            "field": "text",
            "encoding": "text",
            "totalBytes": 5,
            "nextOffset": null,
        }),
        "{small}"
    );

    for (block_id, encoding, body) in [
        (format!("{message_id}:1"), "text", text.clone()),
        (
            "tu".to_string(),
            "json",
            serde_json::to_string(&input).unwrap(),
        ),
    ] {
        let mut joined = String::new();
        let mut offset = json!(null);
        let mut pages = 0;
        loop {
            let (reply, size) = block_raw(
                &mut rpc,
                11 + pages,
                json!({ "agentId": agent_id, "messageId": message_id, "blockId": block_id, "offset": offset }),
            )
            .await;
            assert!(size < READ_BUDGET_BYTES, "frame {size} bytes over budget");
            let r = &reply["result"];
            let mut keys: Vec<_> = r.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            assert_eq!(
                keys,
                [
                    "block",
                    "encoding",
                    "field",
                    "nextOffset",
                    "totalBytes",
                    "truncated"
                ],
                "{reply}"
            );
            assert_eq!(r["truncated"], true);
            assert_eq!(r["encoding"], encoding);
            assert_eq!(r["totalBytes"], json!(body.len()));
            let field = r["field"].as_str().unwrap();
            joined.push_str(r["block"][field].as_str().unwrap());
            pages += 1;
            if r["nextOffset"].is_null() {
                break;
            }
            offset = r["nextOffset"].clone();
        }
        assert!(pages > 1, "paged");
        assert_eq!(joined, body, "block {block_id} reassembles");
    }
    let parsed: Value = serde_json::from_str(&serde_json::to_string(&input).unwrap()).unwrap();
    assert_eq!(parsed, input);

    let (img, size) = block_raw(
        &mut rpc,
        90,
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": format!("{message_id}:3") }),
    )
    .await;
    assert!(size < READ_BUDGET_BYTES);
    let r = &img["result"];
    assert!(r["block"].get("data").is_none(), "{img}");
    assert_eq!(r["block"]["dataTruncated"], true);
    assert_eq!(r["block"]["dataBytes"], json!(image.len()));
    assert_eq!(r["truncated"], true);
    assert_eq!(r["field"], "data");
    assert_eq!(r["encoding"], Value::Null);
    assert_eq!(r["nextOffset"], Value::Null);

    let small_cap = block_raw(
        &mut rpc,
        91,
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": format!("{message_id}:1"), "maxBytes": 1 }),
    )
    .await
    .0;
    assert!(
        serde_json::to_vec(&small_cap["result"]["block"])
            .unwrap()
            .len()
            <= 4_096,
        "maxBytes clamps up to 4,096"
    );
}

/// The documented `-32602` messages for invalid paging params.
#[intent_test_macros::daemon_test]
async fn get_message_block_rejects_bad_paging_params() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let (_ws, agent_id, message_id) = seed(
        &mut rpc,
        json!([
            { "type": "text", "text": "aé" },
            { "type": "image", "data": "QUJD", "mimeType": "image/png" },
        ]),
    )
    .await;
    let text = format!("{message_id}:0");
    let img = format!("{message_id}:1");
    let cases = [
        (
            json!({ "blockId": text, "maxBytes": 0 }),
            "maxBytes must be a positive integer",
        ),
        (
            json!({ "blockId": text, "maxBytes": "10" }),
            "maxBytes must be a positive integer",
        ),
        (
            json!({ "blockId": text, "maxBytes": 1.5 }),
            "maxBytes must be a positive integer",
        ),
        (
            json!({ "blockId": text, "offset": -1 }),
            "offset must be a non-negative integer",
        ),
        (
            json!({ "blockId": text, "offset": "2" }),
            "offset must be a non-negative integer",
        ),
        (
            json!({ "blockId": text, "offset": 9 }),
            "offset 9 is past the end of the block body (3 bytes)",
        ),
        (
            json!({ "blockId": text, "offset": 2 }),
            "offset 2 is not on a UTF-8 character boundary",
        ),
        (
            json!({ "blockId": img, "offset": 1 }),
            "offset is not supported for this block",
        ),
    ];
    for (i, (mut params, want)) in cases.into_iter().enumerate() {
        params["agentId"] = json!(agent_id);
        params["messageId"] = json!(message_id);
        let (reply, _) = block_raw(&mut rpc, 20 + i64::try_from(i).unwrap(), params.clone()).await;
        assert_eq!(reply["error"]["code"], -32602, "{params}: {reply}");
        assert_eq!(reply["error"]["message"], want, "{params}: {reply}");
    }
}

/// `agent.editAndRegenerate` with a stored image block reference persists
/// the original full bytes on the regenerated user row, while the capped
/// read never hands them to the client; a non-image reference is `-32602`.
#[intent_test_macros::daemon_test]
async fn edit_and_regenerate_by_block_reference_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let image = "QUJD".repeat(300_000);
    let (ws_id, agent_id, message_id) = seed(
        &mut rpc,
        json!([
            { "type": "text", "text": "see image" },
            { "type": "image", "data": image, "mimeType": "image/png" },
        ]),
    )
    .await;
    let block_id = format!("{message_id}:1");
    let (capped, _) = block_raw(
        &mut rpc,
        30,
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": block_id }),
    )
    .await;
    assert!(capped["result"]["block"].get("data").is_none());

    let bad = wss_rpc_raw(
        &mut rpc,
        31,
        "agent.editAndRegenerate",
        json!({
            "agentId": agent_id, "workspaceId": ws_id, "messageId": message_id, "content": "again",
            "imageBlocks": [{ "type": "image", "messageId": message_id, "blockId": format!("{message_id}:0") }],
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    assert_eq!(
        bad["error"]["message"],
        format!("agent.editAndRegenerate: imageBlocks[0] references block {message_id}:0, which is not an image with stored bytes"),
    );

    let edited = wss_rpc(
        &mut rpc,
        32,
        "agent.editAndRegenerate",
        json!({
            "agentId": agent_id, "workspaceId": ws_id, "messageId": message_id, "content": "again",
            "imageBlocks": [{ "type": "image", "messageId": message_id, "blockId": block_id }],
        }),
    )
    .await;
    let new_id = edited["messageId"].as_str().expect("new message id");
    let row = fx
        .store
        .get_agent_message_by_id(&AgentId::from(agent_id.as_str()), new_id)
        .await
        .expect("read")
        .expect("row");
    let img = row
        .content
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["type"] == "image")
        .expect("image block");
    assert_eq!(
        img["data"].as_str(),
        Some(image.as_str()),
        "full original bytes"
    );
    assert_eq!(img["mimeType"], "image/png");
}
