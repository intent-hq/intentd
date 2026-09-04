//! End-to-end coverage for the daemon-wide outstanding-slow-path-RPC cap
//! (`server.maxOutstandingRpcs`). Drives a real [`WsApiServer`] over the
//! production transport path — TLS with a pinned self-signed fingerprint and
//! bearer-token auth — so the WSS upgrade → JSON-RPC → limiter → router
//! round-trip is exercised end-to-end, plus a UDS listener sharing the same
//! limiter so the "daemon-wide, both transports" claim is asserted rather than
//! assumed.
//!
//! The contract under test: once the cap is reached, further slow-path requests
//! are REJECTED immediately with `-32011 "Server overloaded"` echoing the
//! request `id` (never queued, never delayed); notification-shaped frames get
//! no response at all; and once the in-flight requests drain, the freed slots
//! serve new requests normally. With the cap unset (unlimited) a concurrent
//! burst is unaffected.

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
    ensure_tls_certificate, serve_uds_with_reverse, AsyncTokenStore, PrimaryReverseRegistry,
    RpcLimiter, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
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
    /// UDS listener sharing the WSS listener's limiter.
    socket: PathBuf,
    _uds_shutdown: tokio::sync::oneshot::Sender<()>,
    _dir: tempfile::TempDir,
}

/// How long an in-flight `host.exec` holds its slot. Scaled by
/// `INTENTD_TEST_TIMEOUT_MULTIPLIER` so the slot stays occupied for the whole
/// flood even on coverage-instrumented, oversubscribed CI runners.
fn slot_hold_seconds() -> String {
    common::test_timeout(Duration::from_secs(2))
        .as_secs()
        .to_string()
}

/// Boot a daemon whose UDS + TLS-WSS listeners share one limiter capped at
/// `max_outstanding` (`0` = unlimited, the shipped "off" value). The WSS
/// listener runs the production transport path: a self-signed certificate the
/// client pins by fingerprint plus bearer-token auth. The temp dir is rooted at
/// `/tmp` so `data_dir/intentd.sock` fits within `SUN_LEN`.
async fn boot(max_outstanding: u32) -> Fixture {
    let dir = common::test_tempdir_in("/tmp", "itd-rpclimit-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let limiter = RpcLimiter::new(max_outstanding);
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).expect("seed token");
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        rpc_limiter: limiter.clone(),
        ..Default::default()
    };
    let ws = WsApiServer::new(
        Arc::clone(&api),
        bus.clone(),
        &tls,
        &token_store,
        opts,
        None,
    )
    .expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");

    // The same limiter instance also backs a UDS listener, so the cap is
    // asserted to be daemon-wide rather than per-transport.
    let socket = dir.path().join("intentd.sock");
    let (uds_shutdown, rx) = tokio::sync::oneshot::channel::<()>();
    let socket_for_task = socket.clone();
    tokio::spawn(async move {
        let _ = serve_uds_with_reverse(
            api,
            bus,
            &socket_for_task,
            None,
            None,
            Arc::new(PrimaryReverseRegistry::new()),
            limiter,
            async move {
                let _ = rx.await;
            },
        )
        .await;
    });
    let deadline = std::time::Instant::now() + common::daemon_startup_timeout();
    while !socket.exists() {
        assert!(std::time::Instant::now() < deadline, "uds never bound");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    Fixture {
        _ws: ws,
        port,
        cfg,
        socket,
        _uds_shutdown: uds_shutdown,
        _dir: dir,
    }
}

/// A pinned-TLS WebSocket plus a buffer of already-read frames. Responses
/// arrive in an order the test does not control (a rejection can land before or
/// after the in-flight response), so frames outside the currently awaited id
/// set are buffered instead of discarded — discarding them would hang the next
/// read.
struct Conn {
    ws: TlsWs,
    buffered: Vec<(i64, Value)>,
    /// Id-less error frames (`-32700`/`-32600`), which carry a null id.
    null_id_errors: Vec<Value>,
}

impl Conn {
    /// Authenticated WSS connection over pinned TLS (token in the query
    /// string), i.e. the same path production clients take.
    async fn connect(fx: &Fixture) -> Self {
        let url = format!("wss://localhost:{}/ws?token={TOKEN}", fx.port);
        let ws = common::wss_connect_with_retry(fx.port, fx.cfg.clone(), &url).await;
        Self {
            ws,
            buffered: Vec::new(),
            null_id_errors: Vec::new(),
        }
    }

    async fn send(&mut self, msg: Message) {
        self.ws.send(msg).await.expect("send frame");
    }

    /// Read one text frame, sorting it into the id-keyed or null-id buffer.
    async fn read_one(&mut self) {
        loop {
            match self.ws.next().await.expect("stream open").expect("frame") {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    match v.get("id").and_then(Value::as_i64) {
                        Some(id) => self.buffered.push((id, v)),
                        None if v.get("error").is_some() => self.null_id_errors.push(v),
                        // Unrelated pushes (subscription events) are ignored.
                        None => continue,
                    }
                    return;
                }
                Message::Ping(_) | Message::Pong(_) => {}
                other => panic!("unexpected message: {other:?}"),
            }
        }
    }

    /// Wait until every id in `wanted` has been seen, returning those frames in
    /// `wanted` order. Frames for other ids stay buffered for a later call.
    async fn responses(&mut self, wanted: &[i64]) -> Vec<(i64, Value)> {
        timeout(common::rpc_read_timeout(), async {
            while wanted
                .iter()
                .any(|id| !self.buffered.iter().any(|(seen, _)| seen == id))
            {
                self.read_one().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("responses for {wanted:?} timed out"));
        wanted
            .iter()
            .map(|id| {
                let idx = self
                    .buffered
                    .iter()
                    .position(|(seen, _)| seen == id)
                    .expect("buffered");
                self.buffered.remove(idx)
            })
            .collect()
    }

    /// Wait for one response and assert it succeeded.
    async fn expect_ok(&mut self, id: i64) {
        let [(_, frame)] = self.responses(&[id]).await.try_into().unwrap();
        assert!(
            frame.get("error").is_none(),
            "request {id} must succeed: {frame}"
        );
    }

    /// Wait until `count` null-id error frames have arrived, in arrival order.
    async fn null_id_errors(&mut self, count: usize) -> Vec<Value> {
        timeout(common::rpc_read_timeout(), async {
            while self.null_id_errors.len() < count {
                self.read_one().await;
            }
        })
        .await
        .expect("error frames timed out");
        self.null_id_errors.drain(..count).collect()
    }
}

/// A slow `host.exec` that occupies one limiter slot for `seconds`.
fn sleep_request(id: i64, seconds: &str) -> Message {
    Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "host.exec",
            "params": { "command": "/bin/sleep", "args": [seconds] },
        })
        .to_string()
        .into(),
    )
}

/// A fast `host.exec` that occupies a slot only briefly.
fn echo_request(id: i64) -> Message {
    Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "host.exec",
            "params": { "command": "/bin/echo", "args": ["ok"] },
        })
        .to_string()
        .into(),
    )
}

/// Assert one frame is the exact `-32011` overload envelope with the echoed id.
fn assert_overload(id: i64, frame: &Value) {
    assert_eq!(frame["jsonrpc"], "2.0", "overload frame: {frame}");
    assert_eq!(frame["id"], json!(id), "overload frame echoes id: {frame}");
    assert_eq!(
        frame["error"]["code"],
        json!(-32011),
        "overload code: {frame}"
    );
    assert_eq!(
        frame["error"]["message"], "Server overloaded",
        "overload message: {frame}"
    );
    assert!(
        frame.get("result").is_none(),
        "overload frame carries no result: {frame}"
    );
}

/// With the cap at 1, a second concurrent slow request is rejected with the
/// exact `-32011` envelope while the first is still in flight; when the
/// in-flight request drains, the freed slot serves a new request normally.
#[tokio::test]
async fn over_limit_requests_are_rejected_and_slots_are_reusable() {
    let fx = boot(1).await;
    let mut conn = Conn::connect(&fx).await;
    let hold = slot_hold_seconds();

    // Occupy the single slot with a slow request, then flood.
    conn.send(sleep_request(1, &hold)).await;
    for id in 2..=5 {
        conn.send(sleep_request(id, &hold)).await;
    }

    // Ids 2..=5 must all come back as overload rejections — immediately, long
    // before the in-flight sleep finishes.
    for (id, frame) in conn.responses(&[2, 3, 4, 5]).await {
        assert_overload(id, &frame);
    }

    // The in-flight request still completes successfully.
    conn.expect_ok(1).await;

    // Its slot is released, so a fresh request is served normally.
    conn.send(echo_request(6)).await;
    conn.expect_ok(6).await;
}

/// A notification-shaped frame (no `id`) rejected at the cap gets NO response
/// (PROTOCOL §9), and the connection keeps serving subsequent requests.
#[tokio::test]
async fn over_limit_notifications_get_no_response() {
    let fx = boot(1).await;
    let mut conn = Conn::connect(&fx).await;
    let hold = slot_hold_seconds();

    conn.send(sleep_request(1, &hold)).await;
    // No `id` ⇒ notification; it hits the cap and must be dropped silently.
    conn.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "method": "host.exec",
            "params": { "command": "/bin/echo", "args": ["dropped"] },
        })
        .to_string()
        .into(),
    ))
    .await;
    // A follow-up request also hits the cap and DOES answer, proving the
    // notification produced no frame ahead of it (frames are ordered).
    conn.send(sleep_request(2, &hold)).await;

    let [(_, rejected)] = conn.responses(&[2]).await.try_into().unwrap();
    assert_overload(2, &rejected);
    conn.expect_ok(1).await;
}

/// The `browser.*` arm is gated by the same limiter: with the cap saturated a
/// `browser.exec` is rejected with `-32011` rather than waiting on a reverse
/// RPC (no FE is attached here, so an ungated call would hang until timeout).
#[tokio::test]
async fn browser_exec_is_rejected_at_the_cap() {
    let fx = boot(1).await;
    let mut conn = Conn::connect(&fx).await;

    conn.send(sleep_request(1, &slot_hold_seconds())).await;
    conn.send(Message::Text(
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "browser.exec",
            "params": { "actions": [{ "action": "listTabs" }] },
        })
        .to_string()
        .into(),
    ))
    .await;

    let [(_, rejected)] = conn.responses(&[2]).await.try_into().unwrap();
    assert_overload(2, &rejected);
    conn.expect_ok(1).await;
}

/// Envelope validation is not masked by the cap: with the limiter saturated,
/// malformed JSON still answers `-32700` and an invalid envelope still answers
/// `-32600` — including an invalid notification-shaped frame, which the router
/// must answer even though valid notifications get no response.
#[tokio::test]
async fn invalid_frames_keep_their_error_codes_at_the_cap() {
    let fx = boot(1).await;
    let mut conn = Conn::connect(&fx).await;

    // Saturate the single slot.
    conn.send(sleep_request(1, &slot_hold_seconds())).await;

    // Malformed JSON → -32700 with a null id.
    conn.send(Message::Text("{ not json".to_string().into()))
        .await;
    // Invalid envelope, notification-shaped (no id) → -32600 with a null id.
    conn.send(Message::Text(
        json!({ "jsonrpc": "1.0", "method": "workspace.list" })
            .to_string()
            .into(),
    ))
    .await;

    let frames = conn.null_id_errors(2).await;
    let codes: Vec<i64> = frames
        .iter()
        .map(|f| f["error"]["code"].as_i64().unwrap())
        .collect();
    assert_eq!(
        codes,
        vec![-32700, -32600],
        "the cap must not mask the router's error matrix: {frames:?}"
    );

    conn.expect_ok(1).await;
}

/// With the cap unset (`0` = unlimited) a concurrent burst is unaffected: every
/// request succeeds and none is rejected.
#[tokio::test]
async fn unlimited_cap_never_rejects() {
    let fx = boot(0).await;
    let mut conn = Conn::connect(&fx).await;

    let ids: Vec<i64> = (1..=8).collect();
    for id in &ids {
        conn.send(echo_request(*id)).await;
    }
    for (id, frame) in conn.responses(&ids).await {
        assert!(
            frame.get("error").is_none(),
            "request {id} must not be rejected under an unlimited cap: {frame}"
        );
    }
}

/// Normal traffic below the cap is unaffected: a burst smaller than the limit
/// all succeeds.
#[tokio::test]
async fn traffic_under_the_limit_is_unaffected() {
    let fx = boot(8).await;
    let mut conn = Conn::connect(&fx).await;

    let ids: Vec<i64> = (1..=4).collect();
    for id in &ids {
        conn.send(echo_request(*id)).await;
    }
    for (id, frame) in conn.responses(&ids).await {
        assert!(
            frame.get("error").is_none(),
            "request {id} under the cap must succeed: {frame}"
        );
    }
}

/// The cap is daemon-wide, not per-transport: a WSS request that occupies the
/// only slot makes a UDS request on the same daemon answer `-32011`, and the
/// UDS connection stays usable once the slot drains.
#[tokio::test]
async fn the_cap_is_shared_across_uds_and_wss() {
    let fx = boot(1).await;
    let mut conn = Conn::connect(&fx).await;

    // Occupy the single shared slot over WSS.
    conn.send(sleep_request(1, &slot_hold_seconds())).await;

    let uds = UnixStream::connect(&fx.socket).await.expect("uds connect");
    let (read_half, mut write_half) = uds.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    write_half
        .write_all(
            format!(
                "{}\n",
                json!({
                    "jsonrpc": "2.0",
                    "id": 100,
                    "method": "host.exec",
                    "params": { "command": "/bin/echo", "args": ["ok"] },
                })
            )
            .as_bytes(),
        )
        .await
        .expect("write uds frame");
    timeout(common::rpc_read_timeout(), reader.read_line(&mut line))
        .await
        .expect("uds read timed out")
        .expect("uds read");
    let rejected: Value = serde_json::from_str(&line).expect("uds frame is json");
    assert_overload(100, &rejected);

    // Once the WSS request drains, the freed slot serves the UDS connection.
    conn.expect_ok(1).await;
    line.clear();
    write_half
        .write_all(
            format!(
                "{}\n",
                json!({
                    "jsonrpc": "2.0",
                    "id": 101,
                    "method": "host.exec",
                    "params": { "command": "/bin/echo", "args": ["ok"] },
                })
            )
            .as_bytes(),
        )
        .await
        .expect("write uds frame");
    timeout(common::rpc_read_timeout(), reader.read_line(&mut line))
        .await
        .expect("uds read timed out")
        .expect("uds read");
    let after: Value = serde_json::from_str(&line).expect("uds frame is json");
    assert!(
        after.get("error").is_none(),
        "a drained slot must serve UDS requests: {after}"
    );
}
