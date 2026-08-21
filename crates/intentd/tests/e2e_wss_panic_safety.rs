//! WSS e2e for JSON-RPC handler panic-safety (#457).
//!
//! Uses the debug-only `INTENTD_TEST_PANIC_METHOD` injection hook to panic
//! inside handlers on ONE live WSS connection and asserts:
//! - a panicking request yields `-32603 Internal error` with the echoed `id`
//!   (both the spawned router path and the inline `events.subscribe` fast
//!   path);
//! - a panicking notification yields no response frame;
//! - the connection survives and keeps serving subsequent requests.

#![cfg(unix)]

use std::net::Ipv4Addr;
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
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

mod common;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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

async fn make_services() -> (Arc<dyn WorkspaceApi>, EventBus, tempfile::TempDir) {
    // Short base under /tmp; the guard removes the dir on drop — hold it for
    // the full test (`INTENTD_TEST_KEEP_TMP` keeps it for debugging).
    let dir = common::test_tempdir_in("/tmp", "itd-panic-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let services = Services::new(store)
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    (api, bus, dir)
}

/// Start a WSS listener on a free localhost port; returns the server handle
/// (kept alive), the port, and the pinning client config.
async fn start_server() -> (WsApiServer, u16, Arc<ClientConfig>, tempfile::TempDir) {
    let (api, bus, dir) = make_services().await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    (ws, port, cfg, dir)
}

type WsClient = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> WsClient {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// RAII guard: sets `INTENTD_TEST_PANIC_METHOD` and removes it on drop, so
/// cleanup also runs when an assertion above the end of the test panics.
struct PanicMethodEnv;

impl PanicMethodEnv {
    fn set(methods: &str) -> Self {
        std::env::set_var("INTENTD_TEST_PANIC_METHOD", methods);
        PanicMethodEnv
    }
}

impl Drop for PanicMethodEnv {
    fn drop(&mut self) {
        std::env::remove_var("INTENTD_TEST_PANIC_METHOD");
    }
}

/// Read frames until the next Text frame, skipping Ping/Pong.
async fn next_text(ws: &mut WsClient) -> Value {
    loop {
        match timeout(common::test_timeout(Duration::from_secs(10)), ws.next())
            .await
            .expect("timed out waiting for text frame")
        {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// One live connection: panic in the spawned router path (request → -32603
/// with echoed id; notification → no frame), panic in the inline
/// `events.subscribe` fast path, then a healthy request on the SAME
/// connection still succeeds — the connection and daemon survive throughout.
#[tokio::test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "INTENTD_TEST_PANIC_METHOD injection is compiled out of release builds"
)]
async fn wss_handler_panics_yield_internal_error_and_connection_survives() {
    // Set BEFORE the server starts; read by the debug-only injection hook at
    // dispatch time inside this same process. `note.list` exercises the
    // spawned `handle_message` path; `events.subscribe` the inline fast path.
    // The guard removes the var on drop, including during unwinding.
    let _env = PanicMethodEnv::set("note.list,events.subscribe");
    let (_srv, port, cfg, _dir) = start_server().await;
    let mut ws = connect_ws(port, cfg).await;

    // 1) Panicking REQUEST (spawned router path) → -32603 with echoed id.
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":41,"method":"note.list","params":{"workspaceId":"w1"}}"#.into(),
    ))
    .await
    .expect("send");
    let resp = next_text(&mut ws).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 41);
    assert_eq!(resp["error"]["code"], -32603);
    assert_eq!(resp["error"]["message"], "Internal error");

    // 2) Panicking NOTIFICATION (no id) on the INLINE `events.subscribe` fast
    //    path → no response frame. The inline path is strictly ordered on the
    //    read loop, so a (buggy) frame from it would have to arrive before the
    //    response to the next request — its absence below is the proof.
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","method":"events.subscribe","params":{"eventTypes":["*"]}}"#.into(),
    ))
    .await
    .expect("send");

    // 3) Panicking REQUEST on the INLINE fast path → -32603 with echoed id.
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":42,"method":"events.subscribe","params":{"eventTypes":["*"]}}"#
            .into(),
    ))
    .await
    .expect("send");
    let resp = next_text(&mut ws).await;
    assert_eq!(
        resp["id"], 42,
        "notification in step 2 must not produce a frame"
    );
    assert_eq!(resp["error"]["code"], -32603);
    assert_eq!(resp["error"]["message"], "Internal error");

    // 4) The SAME connection keeps serving: a healthy request round-trips.
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":43,"method":"workspace.list","params":{}}"#.into(),
    ))
    .await
    .expect("send");
    let resp = next_text(&mut ws).await;
    assert_eq!(resp["id"], 43);
    assert!(
        resp.get("result").is_some(),
        "healthy request after panics must succeed, got {resp}"
    );

    // Brief drain: no stray frame (e.g. from the step-2 notification) may
    // trail the final response. Loop for the full window, skipping Ping/Pong,
    // so a heartbeat can't mask a trailing Text frame. `timeout_at` keeps the
    // fixed deadline without computing a (potentially negative) remainder.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) => break,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            Ok(other) => panic!("unexpected trailing frame: {other:?}"),
        }
    }
}
