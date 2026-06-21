//! WSS listener + lifecycle integration tests (M5.3, §5.2/§5.6).
//!
//! Drives a real [`WsApiServer`] over TLS: `/health`, the upgrade auth gate,
//! a JSON-RPC round-trip that must be byte-identical to the UDS transport, and
//! the §5.6 hardening guarantees (port backoff, graceful-shutdown restart,
//! heartbeat termination). The client pins the M5.1 self-signed fingerprint.

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{ensure_tls_certificate, serve_uds, TokenStore, WsApiServer, WsOptions};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;

/// A fixed 64-char hex token (valid shape) shared by server + client in tests.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex,
/// matching [`TlsCertificate::fingerprint256`]) and otherwise validates the
/// handshake signature with the ring provider — no PKI/hostname checks.
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

/// A pinning [`ClientConfig`] built on the ring provider (the only provider
/// compiled in — see the workspace `rustls` feature set).
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

/// A free localhost TCP port (bound then released to discover the number).
fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Build a real `Services` API + event bus over a fresh temp SQLite store.
async fn make_services() -> (Arc<dyn WorkspaceApi>, EventBus, std::path::PathBuf) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-wss-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
    (api, bus, dir)
}

/// A started WSS listener plus everything a test client needs (the API/bus are
/// held so the shared services stay alive for the listener's lifetime).
struct Server {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    _dir: std::path::PathBuf,
}

/// Build + start a WSS listener with the given options on a free base port.
async fn start(mut opts: WsOptions) -> Server {
    let (api, bus, dir) = make_services().await;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let store = Arc::new(MemTokenStore::default());
    store.store_token(TOKEN).unwrap();
    if opts.base_port == WsOptions::default().base_port {
        opts.base_port = free_port();
    }
    opts.bind_address = Ipv4Addr::LOCALHOST.into();
    let ws = WsApiServer::new(api.clone(), bus.clone(), &tls, store, opts).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Server {
        ws,
        port,
        cfg,
        api,
        bus,
        _dir: dir,
    }
}

/// Open a pinned TLS stream to the listener.
async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

/// Send a raw HTTP request over TLS and read the whole response (the server
/// closes the socket for `/health` and every rejection).
async fn https_request(port: u16, cfg: Arc<ClientConfig>, request: &str) -> String {
    let mut tls = tls_connect(port, cfg).await;
    tls.write_all(request.as_bytes()).await.expect("write");
    tls.flush().await.expect("flush");
    let mut buf = Vec::new();
    let _ = tls.read_to_end(&mut buf).await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// Parse the numeric status code from an HTTP/1.1 status line.
fn status_code(response: &str) -> u16 {
    response
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

/// Build a WebSocket upgrade request head with optional Origin / bearer token.
fn upgrade_req(target: &str, origin: Option<&str>, bearer: Option<&str>) -> String {
    let mut r = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
    );
    if let Some(o) = origin {
        r.push_str(&format!("Origin: {o}\r\n"));
    }
    if let Some(b) = bearer {
        r.push_str(&format!("Authorization: Bearer {b}\r\n"));
    }
    r.push_str("\r\n");
    r
}

/// Establish an authenticated WSS connection (token in the query string).
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

/// One authenticated WSS JSON-RPC round-trip: send `frame`, return the first
/// text response parsed as JSON.
async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Drive several JSON-RPC frames over ONE WSS connection so the per-connection
/// `client_id` binding from `client.hello` persists across them (§16).
async fn wss_session(port: u16, cfg: Arc<ClientConfig>, frames: Vec<String>) -> Vec<Value> {
    let mut ws = connect_ws(port, cfg).await;
    let mut out = Vec::new();
    for frame in frames {
        ws.send(Message::Text(frame)).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    out.push(serde_json::from_str(&text).expect("json"));
                    break;
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    out
}

#[tokio::test]
async fn wss_client_hello_and_drafts_round_trip() {
    let srv = start(WsOptions::default()).await;
    // Create a workspace first (drafts FK to `workspace`); a fresh connection is
    // fine — only `drafts.*` needs the per-connection client binding.
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Draft WS"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            r#"{"jsonrpc":"2.0","id":2,"method":"client.hello","params":{"clientId":"cli-wss","name":"WSS"}}"#.to_string(),
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"drafts.set","params":{{"workspaceId":"{ws_id}","agentId":"agent-wss","text":"wss draft"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"drafts.get","params":{{"workspaceId":"{ws_id}","agentId":"agent-wss"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["clientId"], "cli-wss");
    assert_eq!(
        sess[0]["result"]["server"]["locality"], "remote",
        "WSS ⇒ remote in the client.hello server block (§5.14/§5.17)"
    );
    assert_eq!(sess[1]["result"]["ok"], true);
    assert!(sess[1]["result"]["updatedAt"].is_string());
    assert_eq!(sess[2]["result"]["text"], "wss draft");

    // Reconnect with the same clientId restores the persisted draft.
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            r#"{"jsonrpc":"2.0","id":5,"method":"client.hello","params":{"clientId":"cli-wss"}}"#.to_string(),
            format!(
                r#"{{"jsonrpc":"2.0","id":6,"method":"drafts.get","params":{{"workspaceId":"{ws_id}","agentId":"agent-wss"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(
        sess[1]["result"]["text"], "wss draft",
        "reconnect restores the draft"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn health_reports_ok_and_client_count() {
    let srv = start(WsOptions::default()).await;
    let resp = https_request(
        srv.port,
        srv.cfg.clone(),
        "GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status_code(&resp), 200, "health status: {resp}");
    assert!(resp.contains("\"status\":\"ok\""), "health body: {resp}");
    assert!(resp.contains("\"clients\":0"), "health body: {resp}");

    // An open client bumps the count.
    let _ws = connect_ws(srv.port, srv.cfg.clone()).await;
    for _ in 0..50 {
        if srv.ws.client_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let resp = https_request(
        srv.port,
        srv.cfg.clone(),
        "GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(resp.contains("\"clients\":1"), "health body: {resp}");
    srv.ws.stop().await;
}

#[tokio::test]
async fn upgrade_rejected_without_or_with_bad_token() {
    let srv = start(WsOptions::default()).await;
    let no_token = https_request(srv.port, srv.cfg.clone(), &upgrade_req("/ws", None, None)).await;
    assert_eq!(status_code(&no_token), 401, "{no_token}");
    let bad = https_request(
        srv.port,
        srv.cfg.clone(),
        &upgrade_req("/ws?token=nope", None, None),
    )
    .await;
    assert_eq!(status_code(&bad), 401, "{bad}");
    srv.ws.stop().await;
}

#[tokio::test]
async fn upgrade_rejected_when_disabled() {
    let srv = start(WsOptions {
        enabled: false,
        ..WsOptions::default()
    })
    .await;
    let resp = https_request(
        srv.port,
        srv.cfg.clone(),
        &upgrade_req(&format!("/ws?token={TOKEN}"), None, None),
    )
    .await;
    assert_eq!(status_code(&resp), 403, "{resp}");
    srv.ws.stop().await;
}

#[tokio::test]
async fn upgrade_rejected_bad_origin() {
    let srv = start(WsOptions::default()).await;
    let resp = https_request(
        srv.port,
        srv.cfg.clone(),
        &upgrade_req(
            &format!("/ws?token={TOKEN}"),
            Some("http://evil.example"),
            None,
        ),
    )
    .await;
    assert_eq!(status_code(&resp), 403, "{resp}");
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_jsonrpc_roundtrip_matches_uds() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::UnixStream;

    let srv = start(WsOptions::default()).await;

    // Serve UDS on the SAME shared services + bus so the wire result is
    // produced by one router; only the framing differs.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let socket = Path::new("/tmp").join(format!("intentd-wss-{}.sock", &short[..8]));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let (api, bus, sock) = (srv.api.clone(), srv.bus.clone(), socket.clone());
    let uds = tokio::spawn(async move {
        serve_uds(api, bus, &sock, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve uds");
    });
    for _ in 0..50 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let frame = r#"{"jsonrpc":"2.0","id":1,"method":"agent.getModels"}"#;
    let wss_resp = wss_call(srv.port, srv.cfg.clone(), frame).await;

    let stream = UnixStream::connect(&socket).await.expect("uds connect");
    let (rd, mut wr) = stream.into_split();
    wr.write_all(frame.as_bytes()).await.unwrap();
    wr.write_all(b"\n").await.unwrap();
    wr.flush().await.unwrap();
    let mut line = String::new();
    BufReader::new(rd).read_line(&mut line).await.unwrap();
    let uds_resp: Value = serde_json::from_str(line.trim()).unwrap();

    assert!(
        !wss_resp["result"]["models"].as_array().unwrap().is_empty(),
        "models must be non-empty"
    );
    assert_eq!(
        wss_resp["result"], uds_resp["result"],
        "WSS result must be byte-identical to UDS"
    );
    assert_eq!(wss_resp["id"], 1);

    let _ = tx.send(());
    let _ = uds.await;
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_host_status_reports_remote_locality() {
    // host.status is answered on the WSS transport (§5.14) and reports `remote`
    // by default, with the host capability fields a client gates UI on.
    let srv = start(WsOptions::default()).await;
    let frame = r#"{"jsonrpc":"2.0","id":5,"method":"host.status"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    let h = &resp["result"];
    assert_eq!(resp["id"], 5);
    assert_eq!(h["locality"], "remote", "WSS ⇒ remote (§5.14)");
    assert!(h["os"].is_string());
    assert!(h["arch"].is_string());
    assert!(h["hostname"].is_string());
    assert!(h["hasDisplay"].is_boolean());
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_host_status_override_forces_local() {
    // `--mode local` / `server.locality=local` forces local even over WSS.
    let srv = start(WsOptions {
        locality_override: Some(true),
        ..WsOptions::default()
    })
    .await;
    let frame = r#"{"jsonrpc":"2.0","id":6,"method":"host.status"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(
        resp["result"]["locality"], "local",
        "override forces local over WSS (§5.14)"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn port_backoff_picks_next_port() {
    let base = free_port();
    // Occupy the base port for the whole test so the listener must advance.
    let _hog = StdTcpListener::bind((Ipv4Addr::LOCALHOST, base)).unwrap();
    let srv = start(WsOptions {
        base_port: base,
        ..WsOptions::default()
    })
    .await;
    // `free_port()` discovers a number by binding port 0 then releasing it, so
    // any concurrently-running test may grab `base + 1` before this listener's
    // backoff reaches it — a TOCTOU that is fundamental to OS port assignment.
    // The guarantee under test is "a busy base port makes the listener walk
    // forward within the attempt window", so assert that range rather than one
    // exact port (which is inherently racy under default parallelism).
    let max = base + intent_transport::lifecycle::MAX_PORT_ATTEMPTS;
    assert!(
        srv.port > base && srv.port < max,
        "should walk forward past the busy base {base} within the attempt window, got {}",
        srv.port
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn graceful_shutdown_allows_immediate_restart() {
    let srv = start(WsOptions::default()).await;
    let port = srv.port;
    srv.ws.stop().await;
    // Re-start the SAME listener immediately; the freed port must rebind.
    let again = srv
        .ws
        .start()
        .await
        .expect("restart binds with no EADDRINUSE");
    assert_eq!(again, port, "restart should reclaim the same port");
    srv.ws.stop().await;
}

#[tokio::test]
async fn heartbeat_terminates_silent_client() {
    let srv = start(WsOptions {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(200),
        ..WsOptions::default()
    })
    .await;
    // Connect, then never poll the stream so we never auto-pong.
    let _silent = connect_ws(srv.port, srv.cfg.clone()).await;
    for _ in 0..50 {
        if srv.ws.client_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(srv.ws.client_count(), 1, "client should register");
    let mut terminated = false;
    for _ in 0..100 {
        if srv.ws.client_count() == 0 {
            terminated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(terminated, "silent client must be terminated by heartbeat");
    srv.ws.stop().await;
}
