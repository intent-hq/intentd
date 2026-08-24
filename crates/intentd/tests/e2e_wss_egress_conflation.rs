//! WSS end-to-end for lossless egress conflation under backpressure: a real
//! `events.subscribe` client that stalls its reads while the daemon pushes a
//! multi-megabyte `terminal:data` burst must still receive EVERY byte (chunks
//! may arrive merged — decoded content is what's asserted) and must see the
//! stream's `terminal:exit` barrier strictly AFTER all data, per the
//! conflation ordering guarantee. Drives a real [`WsApiServer`] over TLS with
//! bearer-token auth and a pinned self-signed fingerprint (the production
//! transport path) so the WebSocket-upgrade → JSON-RPC → router →
//! bus-subscription → conflating forwarder → writer path is exercised
//! end-to-end.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use intent_core::{ActorType, EventActor, Result as CoreResult, WorkspaceApi, WorkspaceId};
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

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
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
    let mut ws = connect(fx.port, fx.cfg.clone()).await;
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
        let byte = u8::try_from(i % 251).expect("< 251");
        let chunk = vec![byte; CHUNK_BYTES];
        expected.extend_from_slice(&chunk);
        let _ = fx.bus.publish_transient(&terminal_event(
            ws_id,
            "terminal:data",
            json!({ "terminalId": "t-1", "chunk": BASE64.encode(&chunk) }),
        ));
        // Keep the bus's delivery task ahead of the broadcast buffer, as the
        // in-daemon publishers do.
        tokio::task::yield_now().await;
    }
    let _ = fx.bus.publish_transient(&terminal_event(
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
