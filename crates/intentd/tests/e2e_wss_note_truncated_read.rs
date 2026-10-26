//! WSS end-to-end for truncated note reads
//! (docs/protocol/methods/notes-tasks.md "Truncated note reads"): the
//! `noteTruncatedRead: 1` capability, `note.get` / `note.subscribe` with
//! `maxContentBytes`, and the `baseContentBytes` write guard on
//! `note.setContent` / `note.update`, over the production wire path (TLS
//! upgrade → JSON-RPC → router → services → store).

#![cfg(unix)]

use crate::common;

use std::fmt::Write as _;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

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
    let tmp = common::test_tempdir("intentd-setcontent-");
    let dir = tmp.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store.clone())
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
/// Await the next `subscription.push` frame for `sub_id`.
async fn next_push(ws: &mut TlsWs, sub_id: &str) -> Value {
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == "subscription.push" && v["params"]["subscriptionId"] == sub_id
                    {
                        return v["params"].clone();
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("subscription.push timeout")
}

/// 3,000 lines of mixed-width text, ~70 KB: large against the test limit.
fn large_body() -> String {
    (0..3_000).fold(String::new(), |mut out, i| {
        let _ = writeln!(out, "line {i}: é中😀");
        out
    })
}

async fn create_note(rpc: &mut TlsWs, content: &str) -> (String, String) {
    let created = wss_rpc(
        rpc,
        1,
        "workspace.create",
        json!({ "title": "truncated read e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let note = wss_rpc(
        rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Large", "content": content }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().unwrap().to_string();
    (ws_id, note_id)
}

#[intent_test_macros::daemon_test]
async fn hello_advertises_note_truncated_read() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let hello = wss_rpc(&mut rpc, 1, "client.hello", json!({})).await;
    assert_eq!(
        hello["server"]["capabilities"]["noteTruncatedRead"], 1,
        "{hello}"
    );
}

/// Without `maxContentBytes` (absent or null) `note.get` is byte-identical
/// to today; with it a large note is cut at a line feed and carries the
/// markers; invalid limits are `-32602`.
#[intent_test_macros::daemon_test]
async fn note_get_truncates_only_on_request() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let body = large_body();
    let (ws_id, note_id) = create_note(&mut rpc, &body).await;

    let plain = wss_rpc_raw(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let null = wss_rpc_raw(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": null }),
    )
    .await;
    assert_eq!(plain["result"]["note"]["content"], body.as_str());
    assert!(plain["result"]["note"].get("contentTruncated").is_none());
    assert_eq!(
        plain.to_string(),
        null.to_string(),
        "null keeps the unbounded read"
    );
    let stored = fx
        .store
        .get_note(
            &intent_core::WorkspaceId::from(ws_id.as_str()),
            &intent_core::NoteId::from(note_id.as_str()),
        )
        .await
        .expect("stored note");
    assert_eq!(
        plain["result"].to_string(),
        json!({ "note": stored }).to_string(),
        "byte-identical to the unbounded serializer"
    );

    let limit = 10_000;
    let cut = wss_rpc(
        &mut rpc,
        4,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": limit }),
    )
    .await;
    let note = &cut["note"];
    let prefix = note["content"].as_str().unwrap();
    assert!(
        prefix.len() <= limit && prefix.ends_with('\n'),
        "cut at a line feed"
    );
    assert!(body.starts_with(prefix));
    assert_eq!(note["contentTruncated"], true);
    assert_eq!(note["contentBytes"], body.len());
    assert_eq!(note["rev"], plain["result"]["note"]["rev"]);
    assert_eq!(note["title"], "Large");

    let fits = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": body.len() }),
    )
    .await;
    assert_eq!(
        fits.to_string(),
        plain["result"].to_string(),
        "exactly at the limit is full"
    );

    for bad in [json!(0), json!(-1), json!(1.5), json!("10"), json!(true)] {
        let v = wss_rpc_raw(
            &mut rpc,
            6,
            "note.get",
            json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": bad }),
        )
        .await;
        assert_eq!(v["error"]["code"], -32602, "{bad}: {v}");
        assert_eq!(
            v["error"]["message"],
            "maxContentBytes must be a positive integer"
        );
    }
}

/// A whole-content write built from a truncated read is refused with the
/// documented error and changes nothing; a full-read writer and an
/// unguarded (agent/MCP-style) writer still write.
#[intent_test_macros::daemon_test]
async fn truncated_base_write_is_refused_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let body = large_body();
    let (ws_id, note_id) = create_note(&mut rpc, &body).await;
    let cut = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": 4_096 }),
    )
    .await;
    let prefix = cut["note"]["content"].as_str().unwrap().to_string();
    let rev = cut["note"]["rev"].as_i64().unwrap();

    for (method, extra) in [
        ("note.setContent", json!({ "expectedVersion": rev })),
        ("note.update", json!({ "expectedVersion": rev })),
        ("note.setContent", json!({})),
    ] {
        let mut params = json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": format!("{prefix}edited\n"),
            "baseContentBytes": prefix.len(),
        });
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let v = wss_rpc_raw(&mut rpc, 4, method, params).await;
        assert_eq!(v["error"]["code"], -32602, "{method}: {v}");
        assert_eq!(
            v["error"]["message"],
            "Content base does not match the stored note; re-read the full note and retry."
        );
        assert_eq!(
            v["error"]["data"],
            json!({
                "code": "content-base-mismatch",
                "baseContentBytes": prefix.len(),
                "resolvedBaseBytes": body.len(),
            })
        );
    }
    let after = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(
        after["note"]["content"],
        body.as_str(),
        "nothing was written"
    );
    assert_eq!(after["note"]["rev"], rev);

    for bad in [json!(-1), json!(1.5), json!("10"), json!(true)] {
        let v = wss_rpc_raw(
            &mut rpc,
            6,
            "note.setContent",
            json!({ "workspaceId": ws_id, "noteId": note_id, "content": "x", "baseContentBytes": bad }),
        )
        .await;
        assert_eq!(v["error"]["code"], -32602, "{bad}: {v}");
        assert_eq!(
            v["error"]["message"],
            "baseContentBytes must be a non-negative integer"
        );
    }

    let full = format!("{body}full-read edit\n");
    let ok = wss_rpc(
        &mut rpc,
        7,
        "note.setContent",
        json!({
            "workspaceId": ws_id, "noteId": note_id, "content": full,
            "expectedVersion": rev, "baseContentBytes": body.len(),
        }),
    )
    .await;
    assert_eq!(ok["rev"], rev + 1);
    let agent = format!("{full}agent edit\n");
    let ok = wss_rpc(
        &mut rpc,
        8,
        "note.setContent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": agent }),
    )
    .await;
    assert_eq!(ok["newContent"], agent.as_str());
}

/// `note.subscribe` with `maxContentBytes` cuts the seq-0 snapshot and live
/// `updated` rows like `note.get`; invalid limits fail before the ack.
#[intent_test_macros::daemon_test]
async fn note_subscribe_truncates_snapshot_and_live_updates() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let body = large_body();
    let (ws_id, note_id) = create_note(&mut rpc, &body).await;
    let limit = 2_048;

    let bad = wss_rpc_raw(
        &mut sub,
        3,
        "note.subscribe",
        json!({ "workspaceId": ws_id, "maxContentBytes": 0 }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    let ack = wss_rpc(
        &mut sub,
        4,
        "note.subscribe",
        json!({ "workspaceId": ws_id, "maxContentBytes": limit }),
    )
    .await;
    let sub_id = ack["subscriptionId"].as_str().unwrap().to_string();
    let snapshot = next_push(&mut sub, &sub_id).await;
    let rows = snapshot["snapshot"].as_array().expect("snapshot rows");
    let row = rows
        .iter()
        .find(|r| r["id"] == note_id.as_str())
        .expect("large note row");
    let want = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": limit }),
    )
    .await;
    assert_eq!(row, &want["note"], "snapshot row matches note.get");

    let grown = format!("{body}live line\n");
    wss_rpc(
        &mut rpc,
        6,
        "note.setContent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": grown }),
    )
    .await;
    let delta = next_push(&mut sub, &sub_id).await;
    let updated = &delta["delta"]["updated"][0];
    assert_eq!(updated["contentTruncated"], true, "{delta}");
    assert_eq!(updated["contentBytes"], grown.len());
    let want = wss_rpc(
        &mut rpc,
        7,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id, "maxContentBytes": limit }),
    )
    .await;
    assert_eq!(updated, &want["note"], "live row matches note.get");

    wss_rpc(
        &mut rpc,
        8,
        "note.setContent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": "short\n", "confirmReplacement": true }),
    )
    .await;
    let delta = next_push(&mut sub, &sub_id).await;
    let updated = &delta["delta"]["updated"][0];
    assert_eq!(updated["content"], "short\n");
    assert!(updated.get("contentTruncated").is_none() && updated.get("contentBytes").is_none());
}
