//! WSS end-to-end for `note.setContent` with a stale `expectedVersion`
//! (docs/protocol/methods/notes-tasks.md §5.2): an agent-style `note.add`
//! advances the note past the rev a full-content writer read, and the
//! writer's `note.setContent` carrying that pre-add rev succeeds by
//! three-way-merging its intent onto the current text instead of failing with
//! `-32005`. The response carries the post-write `rev` (equal to `note.get`),
//! the merged content holds both edits, and each write emits exactly one
//! `note:updated`. Drives a real [`WsApiServer`] over TLS with bearer auth
//! and a fingerprint-pinned client, so the production wire path (TLS upgrade
//! → JSON-RPC → router → services → store) is exercised end-to-end.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
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
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over a hermetic workspaces root.
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-setcontent-{}", &short[..8]));
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

/// Drain `events.event` notifications from the subscriber socket until it has
/// been quiet for `settle`, returning the `note:updated` events for `note_id`.
async fn drain_note_updated(evt: &mut TlsWs, note_id: &str, settle: Duration) -> Vec<Value> {
    let mut seen = Vec::new();
    loop {
        match timeout(settle, evt.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    let event = &v["params"]["event"];
                    if event["type"] == "note:updated" && event["data"]["noteId"] == note_id {
                        seen.push(event.clone());
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = evt.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("subscriber socket ended unexpectedly: {other:?}"),
            Err(_elapsed) => return seen,
        }
    }
}

/// Stale-rev `note.setContent` merges over the wire (§5.2 three-way merge):
/// `note.add` (agent-style append) moves the note to rev 1; a
/// `note.setContent` that read rev 0 and edits a different line succeeds,
/// keeps both edits, returns `rev` equal to `note.get`, and each write emits
/// exactly one `note:updated`.
#[tokio::test]
async fn note_set_content_stale_expected_version_merges_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let mut evt = connect(fx.port, fx.cfg.clone()).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "setContent merge e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();

    let note = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Merge me", "content": "alpha\nbeta\ngamma" }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().expect("note id").to_string();
    let base_rev = note["note"]["rev"].as_i64().expect("rev");
    assert_eq!(base_rev, 0, "freshly created note starts at rev 0");

    let sub = wss_rpc(
        &mut evt,
        3,
        "events.subscribe",
        json!({ "workspaceId": ws_id, "eventTypes": ["note:updated"] }),
    )
    .await;
    assert!(sub["subscriptionId"].is_string(), "subscribe: {sub}");

    // Agent-style append lands first and advances the note past `base_rev`.
    wss_rpc(
        &mut rpc,
        4,
        "note.add",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": "delta" }),
    )
    .await;
    let after_add = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let add_rev = after_add["note"]["rev"].as_i64().expect("rev after add");
    assert_eq!(add_rev, base_rev + 1);
    let add_events = drain_note_updated(&mut evt, &note_id, Duration::from_millis(500)).await;
    assert_eq!(
        add_events.len(),
        1,
        "note.add emits exactly one note:updated: {add_events:?}"
    );

    // The full-content writer still carries the pre-add rev: instead of
    // `-32005` its intent (beta → beta-A) is merged onto the current text.
    let set = wss_rpc(
        &mut rpc,
        6,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "alpha\nbeta-A\ngamma",
            "expectedVersion": base_rev,
        }),
    )
    .await;
    assert_eq!(set["ok"], json!(true));
    assert_eq!(set["noteId"], json!(note_id));
    let new_content = set["newContent"].as_str().expect("newContent");
    assert!(
        new_content.contains("beta-A"),
        "writer's edit applied: {new_content:?}"
    );
    assert!(
        new_content.contains("delta"),
        "concurrent append preserved: {new_content:?}"
    );
    assert!(
        !new_content.contains("\nbeta\n"),
        "replaced base line is gone: {new_content:?}"
    );
    let set_rev = set["rev"].as_i64().expect("result rev");
    assert_eq!(set_rev, add_rev + 1, "merge write bumps rev once");

    let after_set = wss_rpc(
        &mut rpc,
        7,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(after_set["note"]["rev"], json!(set_rev));
    assert_eq!(after_set["note"]["content"], json!(new_content));

    let set_events = drain_note_updated(&mut evt, &note_id, Duration::from_millis(500)).await;
    assert_eq!(
        set_events.len(),
        1,
        "note.setContent emits exactly one note:updated: {set_events:?}"
    );
    assert_eq!(set_events[0]["workspaceId"], json!(ws_id));
    assert_eq!(set_events[0]["data"]["action"], json!("update"));

    // The non-merging conditional writes keep the `-32005` contract.
    let stale_meta = wss_rpc_raw(
        &mut rpc,
        8,
        "note.updateMetadata",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "title": "stale",
            "expectedVersion": base_rev,
        }),
    )
    .await;
    assert_eq!(stale_meta["error"]["code"], json!(-32005), "{stale_meta}");
    assert_eq!(stale_meta["error"]["data"]["code"], json!("conflict"));
    assert_eq!(
        stale_meta["error"]["data"]["current"]["rev"],
        json!(set_rev)
    );
}
