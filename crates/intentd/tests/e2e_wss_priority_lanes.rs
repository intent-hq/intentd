//! WSS end-to-end for the two-lane outbound queue: an RPC response
//! (`host.status`) must overtake a saturated stream of `events.event`
//! notifications. The bulk lane is flooded with large transient `file:*`
//! events while the client is NOT reading, so the socket back-pressures and
//! frames queue in the daemon; a `host.status` sent at that point must be
//! answered on the priority lane — i.e. arrive over the wire BEFORE the
//! queued event traffic has drained. On the old single-FIFO queue the
//! response could only arrive after every previously queued event frame.
//!
//! Drives the real WSS path — TLS with the pinned self-signed fingerprint and
//! bearer-token auth — like the other `e2e_wss_*` suites.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::TlsWs;
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

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// In-memory [`TokenStore`] so the test never touches the real OS keychain.
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

/// Client cert verifier pinning the server's SHA-256 fingerprint (colon-UPPER
/// hex, matching `TlsCertificate::fingerprint256`); handshake signatures are
/// validated with the ring provider — no PKI/hostname checks.
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

/// A pinning [`ClientConfig`] on the ring provider (the only one compiled in).
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
    cfg: Arc<ClientConfig>,
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
        cfg,
        port,
        _dir: TempDir(dir),
    }
}

/// Open an authenticated, fingerprint-pinned WSS connection (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn send_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
}

/// Read frames until the response with `id` arrives; panics on error frames.
async fn read_until_response(ws: &mut TlsWs, id: i64) -> Value {
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
/// `SQLite` cannot throttle the flood) with a `payload` of `size` bytes.
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
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

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
