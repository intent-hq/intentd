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
use intent_core::{
    now_iso, ContentType, Note, NoteId, NoteVisibility, Result as CoreResult, TaskMetadata,
    TaskStatus, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
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
/// The store is returned alongside so tests that need to seed fixtures with a
/// fixed id (e.g. the workspace `spec` note) can `store.insert_*` directly,
/// since `note.create` mints a fresh `NoteId` by design. `auggie_bin`
/// optionally pins the auggie binary `agent.enhancePrompt` spawns (§5.31) to a
/// deterministic fixture script.
async fn make_services(
    auggie_bin: Option<std::path::PathBuf>,
) -> (Arc<dyn WorkspaceApi>, EventBus, Store, std::path::PathBuf) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-wss-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let mut services = Services::new(store.clone()).with_assets_root(dir.join("assets"));
    if let Some(bin) = auggie_bin {
        services = services.with_auggie_bin(bin);
    }
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    (api, bus, store, dir)
}

/// A started WSS listener plus everything a test client needs (the API/bus are
/// held so the shared services stay alive for the listener's lifetime).
struct Server {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    store: Store,
    _dir: std::path::PathBuf,
}

/// Build + start a WSS listener with the given options on a free base port.
async fn start(opts: WsOptions) -> Server {
    start_with_auggie(opts, None).await
}

/// [`start`] with an optional auggie-binary override for `agent.enhancePrompt`
/// tests (§5.31).
async fn start_with_auggie(mut opts: WsOptions, auggie_bin: Option<std::path::PathBuf>) -> Server {
    let (api, bus, store, dir) = make_services(auggie_bin).await;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store = Arc::new(MemTokenStore::default());
    token_store.store_token(TOKEN).unwrap();
    if opts.base_port == WsOptions::default().base_port {
        opts.base_port = free_port();
    }
    opts.bind_address = Ipv4Addr::LOCALHOST.into();
    let ws = WsApiServer::new(api.clone(), bus.clone(), &tls, token_store, opts).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Server {
        ws,
        port,
        cfg,
        api,
        bus,
        store,
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

/// `agent.create` accepts a client-supplied `agentId` and the follow-up
/// `agent.sendMessage` addressed to the same id lands on the persisted session
/// instead of the pre-fix `-32602 not found: agent session` (PROTOCOL §5.5).
/// This proves the create+send race the FE `UnifiedAgentFactory` was hitting.
#[tokio::test]
async fn wss_agent_create_honors_client_supplied_agent_id() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Agent ID"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let requested = format!("agent-{}", uuid::Uuid::new_v4());

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","agentId":"{requested}","name":"WSS Client Id"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"agent.get","params":{{"agentId":"{requested}"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(
        sess[0]["result"]["agent"]["id"].as_str(),
        Some(requested.as_str()),
        "agent.create must adopt the client-supplied agentId verbatim: {}",
        sess[0]
    );
    assert_eq!(
        sess[1]["result"]["agent"]["id"].as_str(),
        Some(requested.as_str()),
        "agent.get at the client-supplied id must resolve: {}",
        sess[1]
    );

    // A malformed id is `-32602` (PROTOCOL §5.5 / §9) — a stray/hand-typed id
    // must not slip through and collide with future daemon-minted ids.
    let bad = wss_call(
        srv.port,
        srv.cfg.clone(),
        format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","agentId":"not-an-agent"}}}}"#
        )
        .as_str(),
    )
    .await;
    assert_eq!(
        bad["error"]["code"].as_i64(),
        Some(-32602),
        "malformed agentId must be -32602: {bad}"
    );

    srv.ws.stop().await;
}

/// `agent.create` accepts the widened P2-12a wire shape (optional `provider`,
/// `agentType`, `metadata`, `workspacePath`, `workspaceContext`) and returns
/// the full `AgentLite` projection instead of the pre-widening `{id, name}`
/// snippet. All new params are additive — omitted params still succeed and
/// still round-trip through `agent.get` (PROTOCOL §5.5).
#[tokio::test]
async fn wss_agent_create_widened_params_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Widen"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let requested = format!("agent-{}", uuid::Uuid::new_v4());

    // Full-param create: exercise every new optional field. `provider` and
    // `isBackground` (G-A1/P3-1.2c) persist on the session;
    // `agentType`/`workspacePath`/`workspaceContext` are accepted but
    // deferred (per P2-12a audit).
    let params = format!(
        concat!(
            r#"{{"workspaceId":"{ws}","agentId":"{aid}","name":"WSS Wide","#,
            r#""model":"auggie:sonnet4.5","specialistId":"implementor","#,
            r#""provider":"auggie","agentType":"task-loop","#,
            r#""metadata":{{"tag":"unit"}},"workspacePath":"/tmp/wid","#,
            r#""workspaceContext":{{"selection":"note:1"}},"isBackground":true}}"#
        ),
        ws = ws_id,
        aid = requested,
    );
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{params}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"agent.get","params":{{"agentId":"{requested}"}}}}"#
            ),
        ],
    )
    .await;

    let created = &sess[0]["result"]["agent"];
    // Return shape is the full `AgentLite` projection — a superset of the
    // pre-widening `{id, name}` snippet. Assert the persisted fields land.
    assert_eq!(
        created["id"].as_str(),
        Some(requested.as_str()),
        "widened create must adopt the client-supplied id: {}",
        sess[0]
    );
    assert_eq!(created["name"].as_str(), Some("WSS Wide"));
    assert_eq!(created["model"].as_str(), Some("auggie:sonnet4.5"));
    assert_eq!(created["provider"].as_str(), Some("auggie"));
    assert_eq!(created["workspaceId"].as_str(), Some(ws_id.as_str()));
    assert_eq!(created["metadata"]["specialist"], "implementor");
    assert_eq!(
        created["metadata"]["isBackground"], true,
        "isBackground must persist and be served on the projection: {}",
        sess[0]
    );
    // Full-`AgentLite` shape check: `messageCount` is present on the projection.
    assert!(
        created.get("messageCount").is_some(),
        "AgentLite projection must expose messageCount: {}",
        sess[0]
    );

    // `agent.get` must resolve at the same id and return the same projection.
    assert_eq!(
        sess[1]["result"]["agent"]["id"].as_str(),
        Some(requested.as_str()),
    );
    assert_eq!(
        sess[1]["result"]["agent"]["provider"].as_str(),
        Some("auggie"),
    );
    assert_eq!(
        sess[1]["result"]["agent"]["metadata"]["isBackground"], true,
        "isBackground survives the persist → agent.get round-trip",
    );

    // Backward-compat: a create that omits every widened param still returns
    // the full `AgentLite` shape (no error, no missing required fields).
    let minimal_id = format!("agent-{}", uuid::Uuid::new_v4());
    let minimal = wss_call(
        srv.port,
        srv.cfg.clone(),
        format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","agentId":"{minimal_id}"}}}}"#
        )
        .as_str(),
    )
    .await;
    assert_eq!(
        minimal["result"]["agent"]["id"].as_str(),
        Some(minimal_id.as_str()),
        "minimal create must still succeed: {minimal}",
    );
    assert!(
        minimal["result"]["agent"].get("createdAt").is_some(),
        "minimal create still returns full AgentLite: {minimal}",
    );
    assert_eq!(
        minimal["result"]["agent"]["metadata"]["isBackground"], false,
        "omitting isBackground defaults to foreground: {minimal}",
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
async fn wss_models_list_returns_catalog_with_source() {
    // models.list (§5.30): the rich FE model catalog — `{ models, source }`
    // where `source` is "auggie" (live CLI) or "static" (tier fallback), and
    // every row carries the id/name/provider triple; never empty.
    let srv = start(WsOptions::default()).await;
    let frame = r#"{"jsonrpc":"2.0","id":7,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 7);
    let models = resp["result"]["models"].as_array().expect("models array");
    assert!(!models.is_empty(), "catalog must never be empty");
    for m in models {
        assert!(m["id"].is_string(), "{m}");
        assert!(m["name"].is_string(), "{m}");
        assert!(m["provider"].is_string(), "{m}");
    }
    let source = resp["result"]["source"].as_str().expect("source");
    assert!(source == "auggie" || source == "static", "source: {source}");
    srv.ws.stop().await;
}

/// Write a deterministic fake `auggie` script for `agent.enhancePrompt` tests
/// (§5.31): swallows the piped stdin, then runs `body`.
#[cfg(unix)]
fn fake_auggie_script(tag: &str, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let dir = Path::new("/tmp").join(format!("intentd-wss-auggie-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let bin = dir.join("auggie");
    std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_round_trip() {
    // agent.enhancePrompt (§5.31): `mode: "enhance"` (the default) extracts the
    // `<augment-enhanced-prompt>` payload; `mode: "layout"` returns the full
    // cleaned reply. Both `{ enhanced, original, mode }` shapes ride the same
    // deterministic fixture CLI.
    let bin = fake_auggie_script(
        "ok",
        "printf '\u{1b}[32m🔧 Tool call: noise\u{1b}[0m\\n🤖\\n<augment-enhanced-prompt>Enhanced: ship it</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":31,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 31);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["result"]["enhanced"], "Enhanced: ship it");
    assert_eq!(resp["result"]["original"], "ship it");
    assert_eq!(resp["result"]["mode"], "enhance");

    // Layout mode: no template wrap, no tag extraction — the cleaned reply
    // (everything after the 🤖 marker) comes back verbatim.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":32,"method":"agent.enhancePrompt","params":{"prompt":"make a layout","mode":"layout"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 32);
    assert_eq!(
        resp["result"]["enhanced"],
        "<augment-enhanced-prompt>Enhanced: ship it</augment-enhanced-prompt>"
    );
    assert_eq!(resp["result"]["original"], "make a layout");
    assert_eq!(resp["result"]["mode"], "layout");
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_parse_failure_is_internal_error() {
    // A reply without the `<augment-enhanced-prompt>` tags in enhance mode is
    // the documented -32603 parse failure (§5.31).
    let bin = fake_auggie_script("notags", "printf '🤖\\nno tags here\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":33,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 33);
    assert_eq!(resp["error"]["code"], -32603);
    assert_eq!(
        resp["error"]["data"],
        "Failed to parse enhanced prompt from response"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_agent_enhance_prompt_cli_missing_is_internal_error() {
    // A missing/unspawnable auggie binary is a hard -32603 (§5.31) — there is
    // no static fallback for enhancement.
    let srv = start_with_auggie(
        WsOptions::default(),
        Some(std::path::PathBuf::from("/nonexistent/intentd-wss/auggie")),
    )
    .await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":34,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 34);
    assert_eq!(resp["error"]["code"], -32603);
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_agent_enhance_prompt_validates_params() {
    // Router-side -32602s (§5.31): missing prompt, unknown mode — rejected
    // before any CLI spawn, so no auggie override is needed.
    let srv = start(WsOptions::default()).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":35,"method":"agent.enhancePrompt","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "Missing required parameter: prompt"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":36,"method":"agent.enhancePrompt","params":{"prompt":"x","mode":"summarize"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "mode must be \"enhance\" or \"layout\""
    );
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

/// Minimal `Workspace` fixture used by `task.list` seeding below.
fn fixture_workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    }
}

/// Minimal `Note` fixture used by `task.list` seeding below.
fn fixture_note(ws: &WorkspaceId, id: &str, content: &str) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: id.to_string(),
        content: content.to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

/// `task.list` over the real WSS wire returns `{ tasks, stats }` and the
/// `stats` aggregate mirrors the FE `computeTaskStats` (`task-stats.ts`)
/// classification: `total` excludes `cancelled`, `completed` counts `complete`,
/// and `inProgress` counts `in_progress` + `review_required`. The optional
/// `status` filter narrows `tasks` only — `stats` stays the unfiltered rollup
/// so the FE renders the progress bar verbatim regardless of the active filter
/// (PROTOCOL §5.4).
#[tokio::test]
async fn wss_task_list_emits_stats_aggregate() {
    let srv = start(WsOptions::default()).await;

    // Seed a workspace + spec note + four task notes directly through the
    // shared store — `note.create` mints a fresh `NoteId`, so the spec note
    // (which must have id == "spec") can only be created out-of-band.
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .expect("insert workspace");

    let spec_body = "\
- [A](intent://local/task/task-a)\n\
- [B](intent://local/task/task-b)\n\
- [C](intent://local/task/task-c)\n\
- [D](intent://local/task/task-d)\n";
    srv.store
        .insert_note(&fixture_note(&ws, "spec", spec_body))
        .await
        .expect("insert spec");

    let mk_task = |id: &str, title: &str, status: TaskStatus| {
        let mut n = fixture_note(&ws, id, "body");
        n.title = title.to_string();
        n.parent_id = Some(NoteId::from("spec"));
        n.task = Some(TaskMetadata {
            status,
            ..Default::default()
        });
        n
    };
    for n in [
        mk_task("task-a", "Alpha", TaskStatus::InProgress),
        mk_task("task-b", "Beta", TaskStatus::Complete),
        mk_task("task-c", "Gamma", TaskStatus::ReviewRequired),
        mk_task("task-d", "Delta", TaskStatus::Cancelled),
    ] {
        srv.store.insert_note(&n).await.expect("insert task note");
    }

    // Unfiltered: returns the four spec-linked tasks (cancelled included) and
    // a `stats` rollup where `total` excludes the cancelled task and
    // `inProgress` counts both in_progress + review_required.
    let req = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"task.list","params":{{"workspaceId":"{}"}}}}"#,
        ws.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &req).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let result = &resp["result"];
    assert!(
        result["error"].is_null(),
        "unexpected error envelope: {resp}"
    );

    let tasks = result["tasks"].as_array().expect("tasks array");
    // Order is store-list order (`ORDER BY created_at` with second precision —
    // intentionally undefined for fixtures inserted in the same tick); compare
    // as a set so the assertion isn't flaky on the timestamp tie-break.
    let mut task_ids: Vec<&str> = tasks
        .iter()
        .map(|t| t["id"].as_str().expect("task id"))
        .collect();
    task_ids.sort_unstable();
    assert_eq!(task_ids, vec!["task-a", "task-b", "task-c", "task-d"]);
    let by_id: std::collections::HashMap<&str, &Value> = tasks
        .iter()
        .map(|t| (t["id"].as_str().unwrap(), t))
        .collect();
    assert_eq!(by_id["task-a"]["title"], "Alpha");
    assert_eq!(by_id["task-a"]["status"], "in_progress");
    assert_eq!(by_id["task-b"]["status"], "complete");
    assert_eq!(by_id["task-c"]["status"], "review_required");
    assert_eq!(by_id["task-d"]["status"], "cancelled");
    assert!(by_id["task-a"]["updatedAt"].is_string());

    let stats = &result["stats"];
    assert_eq!(stats["total"], 3, "total excludes cancelled: {stats}");
    assert_eq!(stats["completed"], 1, "completed = 1 complete: {stats}");
    assert_eq!(
        stats["inProgress"], 2,
        "inProgress = in_progress + review_required: {stats}"
    );
    // Only the documented fields cross the wire (camelCase, no `tasks` array).
    let stats_keys: Vec<&str> = stats
        .as_object()
        .expect("stats object")
        .keys()
        .map(String::as_str)
        .collect();
    let mut sorted = stats_keys.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec!["completed", "inProgress", "total"]);

    // Status filter narrows `tasks` only; `stats` stays the full workspace
    // rollup so the FE renders the progress bar verbatim.
    let req = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"task.list","params":{{"workspaceId":"{}","status":"complete"}}}}"#,
        ws.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &req).await;
    let result = &resp["result"];
    let tasks = result["tasks"].as_array().expect("tasks array (filtered)");
    assert_eq!(tasks.len(), 1, "status=complete narrows to one task");
    assert_eq!(tasks[0]["id"], "task-b");
    assert_eq!(result["stats"]["total"], 3, "stats stay the full rollup");
    assert_eq!(result["stats"]["completed"], 1);
    assert_eq!(result["stats"]["inProgress"], 2);

    srv.ws.stop().await;
}

/// A workspace with no task notes still emits a well-formed `stats` aggregate
/// (zeroed counts) so the FE never sees a missing `stats` field. Covers the
/// "fresh workspace" branch the FE renderer hits on the first load.
#[tokio::test]
async fn wss_task_list_empty_workspace_emits_zero_stats() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Empty"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    let req = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"task.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &req).await;
    let result = &resp["result"];
    assert_eq!(result["tasks"].as_array().expect("tasks array").len(), 0);
    assert_eq!(result["stats"]["total"], 0);
    assert_eq!(result["stats"]["completed"], 0);
    assert_eq!(result["stats"]["inProgress"], 0);

    srv.ws.stop().await;
}

/// `git.commitDetails` + `git.diffs` (with `commitHash`) round-trip over WSS:
/// proves the daemon's per-commit reads reach a pinned-TLS WebSocket client
/// with the documented PROTOCOL §5.6 wire shape.
#[tokio::test]
async fn wss_git_commit_details_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with two commits so HEAD has a non-empty parent diff.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let repo = Path::new("/tmp").join(format!("intentd-wssgit-{}", &short[..8]));
    std::fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "seed"]);
    std::fs::write(repo.join("seed.txt"), "seed\nadded\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "second"]);
    let head = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(&repo)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("rev-parse")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();

    // Create a workspace pointing at the seeded repo.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS git WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // git.commitDetails over WSS — shape parity with the UDS coverage.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.commitDetails","params":{{"workspaceId":"{ws_id}","commitHash":"{head}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["commitHash"], Value::from(head.clone()));
    assert_eq!(resp["result"]["author"], "Test");
    assert_eq!(resp["result"]["authorEmail"], "test@example.com");
    assert_eq!(resp["result"]["message"], "second");
    let file_details = resp["result"]["fileDetails"]
        .as_array()
        .expect("fileDetails array");
    let seed = file_details
        .iter()
        .find(|f| f["path"] == "seed.txt")
        .expect("seed.txt fileDetails");
    assert_eq!(seed["additions"], 1);
    assert_eq!(seed["deletions"], 0);
    assert_eq!(resp["result"]["files"], serde_json::json!(["seed.txt"]));

    // git.diffs with commitHash over WSS returns the commit's hunks.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","commitHash":"{head}","path":"seed.txt"}}}}"#
        ),
    )
    .await;
    let arr = resp["result"].as_array().expect("diffs array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["path"], "seed.txt");
    let lines = arr[0]["hunks"][0]["lines"].as_array().expect("hunk lines");
    assert!(lines
        .iter()
        .any(|l| l["type"] == "Addition" && l["content"].as_str().unwrap_or("").contains("added")));

    // Missing commitHash → -32602, matching PROTOCOL §9.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.commitDetails","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    srv.ws.stop().await;
    std::fs::remove_dir_all(&repo).ok();
}

/// `git.branchStatus` + `git.getBranches` over WSS — the path-based
/// BranchSelector seam (§5.6). Drives the happy path (response shape parity
/// with the UDS coverage), the missing-branchName -32602, the
/// nonexistent-path -32602, and the unregistered-repo branch listing used by
/// the workspace-create flow.
#[tokio::test]
async fn wss_git_branch_status_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with one commit so the worktree has a valid HEAD.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let repo = Path::new("/tmp").join(format!("intentd-wssbs-{}", &short[..8]));
    std::fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "seed"]);
    let head_branch = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(&repo)
            .args(["branch", "--show-current"])
            .output()
            .expect("branch --show-current")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();

    // Register the repo as a workspace (the path-based reads do not require
    // it, but a registered repo must keep working unchanged).
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS branchStatus WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let _ = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;

    // (a) Clean repo, queried branch == current → flags align.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            head_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["branch"], head_branch);
    assert_eq!(resp["result"]["currentBranch"], head_branch);
    assert_eq!(resp["result"]["isCurrentBranch"], true);
    assert_eq!(resp["result"]["ahead"], 0);
    assert_eq!(resp["result"]["behind"], 0);
    assert_eq!(resp["result"]["hasUncommittedChanges"], false);

    // (b) Untracked file → hasUncommittedChanges flips to true.
    std::fs::write(repo.join("new.txt"), "fresh\n").unwrap();
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            head_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["hasUncommittedChanges"], true);

    // (c) Missing branchName → -32602 with the verbatim message.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.branchStatus","params":{{"repoPath":"{}"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "Missing required parameter: branchName"
    );

    // (d) Nonexistent repo path → -32602 with the validation message.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":5,"method":"git.branchStatus","params":{"repoPath":"/no/such/repo","branchName":"main"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "Repository path does not exist: /no/such/repo"
    );

    // (e) `git.getBranches` on a valid local repo the daemon has never seen →
    // succeeds (the workspace-create flow lists branches before the repo is
    // registered; PROTOCOL §5.6).
    let unreg = Path::new("/tmp").join(format!("intentd-wssgb-{}", &short[..8]));
    std::fs::create_dir_all(&unreg).unwrap();
    let git_in = |dir: &Path, args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git_in(&unreg, &["init", "-q"]);
    git_in(&unreg, &["config", "user.name", "Test"]);
    git_in(&unreg, &["config", "user.email", "test@example.com"]);
    git_in(&unreg, &["config", "commit.gpgsign", "false"]);
    std::fs::write(unreg.join("seed.txt"), "seed\n").unwrap();
    git_in(&unreg, &["add", "."]);
    git_in(&unreg, &["commit", "-q", "-m", "seed"]);
    let unreg_head = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(&unreg)
            .args(["branch", "--show-current"])
            .output()
            .expect("branch --show-current")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"git.getBranches","params":{{"repoPath":"{}"}}}}"#,
            unreg.display(),
        ),
    )
    .await;
    assert_eq!(resp["result"]["currentBranch"], unreg_head);
    assert!(resp["result"]["branches"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!(unreg_head)));

    // (f) `git.getBranches` on an existing non-git directory → -32602 with the
    // distinct message.
    let plain = Path::new("/tmp").join(format!("intentd-wsspl-{}", &short[..8]));
    std::fs::create_dir_all(&plain).unwrap();
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"git.getBranches","params":{{"repoPath":"{}"}}}}"#,
            plain.display(),
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        format!("Path is not a git repository: {}", plain.display())
    );

    srv.ws.stop().await;
    std::fs::remove_dir_all(&repo).ok();
    std::fs::remove_dir_all(&unreg).ok();
    std::fs::remove_dir_all(&plain).ok();
}

/// `git.pull` over WSS — the workspace-create auto-pull seam (§5.6).
/// Path-based like `git.getBranches`: the repo is never registered as a
/// workspace. Drives the checked-out fast-forward pull (`{ ok: true }`), the
/// structured `{ ok: false, error }` failure for a repo without a remote, and
/// the nonexistent-path -32602.
#[tokio::test]
async fn wss_git_pull_round_trip() {
    let srv = start(WsOptions::default()).await;

    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-wsspull-{}", &short[..8]));
    std::fs::create_dir_all(&base).unwrap();
    let git_in = |dir: &Path, args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    let seed = |dir: &Path| {
        std::fs::create_dir_all(dir).unwrap();
        git_in(dir, &["init", "-q"]);
        git_in(dir, &["config", "user.name", "Test"]);
        git_in(dir, &["config", "user.email", "test@example.com"]);
        git_in(dir, &["config", "commit.gpgsign", "false"]);
        std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
        git_in(dir, &["add", "."]);
        git_in(dir, &["commit", "-q", "-m", "seed"]);
    };
    let head_branch = |dir: &Path| {
        String::from_utf8(
            std::process::Command::new("git")
                .current_dir(dir)
                .args(["branch", "--show-current"])
                .output()
                .expect("branch --show-current")
                .stdout,
        )
        .expect("utf8")
        .trim()
        .to_string()
    };

    // `repo` tracks a bare origin and is one commit behind it.
    let repo = base.join("repo");
    seed(&repo);
    let bare = base.join("origin.git");
    git_in(&base, &["init", "-q", "--bare", "origin.git"]);
    git_in(&repo, &["remote", "add", "origin", bare.to_str().unwrap()]);
    let branch = head_branch(&repo);
    git_in(&repo, &["push", "-q", "origin", &branch]);
    std::fs::write(repo.join("remote.txt"), "from-remote\n").unwrap();
    git_in(&repo, &["add", "."]);
    git_in(&repo, &["commit", "-q", "-m", "remote change"]);
    git_in(&repo, &["push", "-q", "origin", &branch]);
    git_in(&repo, &["reset", "-q", "--hard", "HEAD~1"]);

    // (a) Behind checked-out branch → fast-forward pull succeeds with the
    // exact `{ ok: true }` result (`error` omitted on success).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.pull","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            branch,
        ),
    )
    .await;
    assert_eq!(resp["result"], serde_json::json!({ "ok": true }));
    assert!(repo.join("remote.txt").exists());

    // (b) Repo without an `origin` remote → structured `{ ok: false, error }`.
    let lone = base.join("lone");
    seed(&lone);
    let lone_branch = head_branch(&lone);
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.pull","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            lone.display(),
            lone_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["ok"], false);
    assert!(!resp["result"]["error"].as_str().unwrap().is_empty());

    // (c) Nonexistent repo path → -32602 with the validation message.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":3,"method":"git.pull","params":{"repoPath":"/no/such/repo","branchName":"main"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "Repository path does not exist: /no/such/repo"
    );

    srv.ws.stop().await;
    std::fs::remove_dir_all(&base).ok();
}

/// Note version history over WSS (PROTOCOL §5.2 version-history extensions):
/// every content mutation appends a full snapshot, `note.listVersions` returns
/// summaries (no content blob), `note.getVersion` returns one snapshot with
/// content, and `note.restoreVersion` resets the note to an old snapshot while
/// appending a new version that captures the restored state.
#[tokio::test]
async fn wss_note_version_history_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Versions"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // v1: create; v2: full set; v3: append.
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"note.create","params":{{"workspaceId":"{ws_id}","title":"Versioned","content":"first draft"}}}}"#
            ),
        ],
    )
    .await;
    let note_id = sess[0]["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"note.setContent","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}","content":"second draft"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"note.add","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}","content":"appended line"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"note.listVersions","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":6,"method":"note.getVersion","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}","v":2}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":7,"method":"note.restoreVersion","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}","v":2}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":8,"method":"note.get","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":9,"method":"note.getVersion","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}","v":99}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":10,"method":"note.getVersion","params":{{"workspaceId":"{ws_id}","noteId":"{note_id}"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["ok"], true, "setContent: {}", sess[0]);
    assert_eq!(sess[1]["result"]["ok"], true, "add: {}", sess[1]);

    // listVersions: bare ascending array of snapshot summaries, no content.
    let versions = sess[2]["result"].as_array().expect("versions array");
    assert_eq!(versions.len(), 3, "create+setContent+add: {}", sess[2]);
    for (i, entry) in versions.iter().enumerate() {
        assert_eq!(entry["v"].as_i64(), Some(i as i64 + 1));
        assert_eq!(entry["type"], "snapshot");
        assert_eq!(entry["author"]["type"], "system");
        assert!(entry["date"].is_string());
        assert!(entry["contentLength"].is_i64());
        assert!(entry.get("content").is_none(), "summaries carry no content");
    }
    assert_eq!(versions[0]["title"], "Versioned");

    // getVersion: one full snapshot with content.
    assert_eq!(sess[3]["result"]["v"].as_i64(), Some(2));
    assert_eq!(sess[3]["result"]["content"], "second draft");
    assert_eq!(sess[3]["result"]["type"], "snapshot");

    // restoreVersion: content reset to v2, new v4 appended.
    assert_eq!(sess[4]["result"]["ok"], true, "restore: {}", sess[4]);
    assert_eq!(sess[4]["result"]["restoredFrom"].as_i64(), Some(2));
    assert_eq!(sess[4]["result"]["v"].as_i64(), Some(4));
    assert_eq!(sess[4]["result"]["note"]["content"], "second draft");

    // note.get confirms the persisted content matches the restored snapshot.
    assert_eq!(sess[5]["result"]["note"]["content"], "second draft");

    // Unknown version → -32602 (NotFound, PROTOCOL §9); missing `v` → -32602.
    assert_eq!(sess[6]["error"]["code"].as_i64(), Some(-32602));
    assert_eq!(sess[7]["error"]["code"].as_i64(), Some(-32602));
    assert_eq!(sess[7]["error"]["message"], "Missing required parameter: v");

    srv.ws.stop().await;
}

/// `git.showFile` over WSS (PROTOCOL §5.6 extensions): file content at a
/// revision (`HEAD` / `HEAD^`), the empty-content fallback for a path missing
/// at the ref, and -32603 for an unresolvable ref.
#[tokio::test]
async fn wss_git_show_file_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with two commits so HEAD and HEAD^ differ.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let repo = Path::new("/tmp").join(format!("intentd-wsssf-{}", &short[..8]));
    std::fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "seed"]);
    std::fs::write(repo.join("seed.txt"), "seed\nadded\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "second"]);

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS showFile WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // Content at HEAD and at the parent revision.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"seed.txt","ref":"HEAD"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["content"], "seed\nadded\n");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"seed.txt","ref":"HEAD^"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["content"], "seed\n");

    // Missing path at the ref → empty content, not an error.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"nope.txt","ref":"HEAD"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["content"], "");

    // Unresolvable ref → -32603; missing ref param → -32602 (PROTOCOL §9).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"seed.txt","ref":"no-such-ref"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32603);
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"seed.txt"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    srv.ws.stop().await;
    std::fs::remove_dir_all(&repo).ok();
}

/// `note.saveAsset` over WSS (PROTOCOL §5.2 — additive asset write): the write
/// returns `{ assetId, path, url }` and the asset round-trips back through
/// `note.readAsset`; a missing `data` param is -32602.
#[tokio::test]
async fn wss_note_save_asset_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Save with a data-URL prefix (the FE sends FileReader.readAsDataURL output).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"note.saveAsset","params":{"workspaceId":"ws-asset","data":"data:image/png;base64,aGVsbG8=","mimeType":"image/png","originalName":"pasted.png"}}"#,
    )
    .await;
    let asset_id = resp["result"]["assetId"].as_str().expect("assetId");
    assert!(
        asset_id.ends_with(".png"),
        "mime-derived extension: {asset_id}"
    );
    assert_eq!(
        resp["result"]["url"],
        Value::from(format!("workspace-asset://ws-asset/{asset_id}"))
    );
    let path = resp["result"]["path"].as_str().expect("path");
    assert!(path.ends_with(asset_id));
    let url = resp["result"]["url"].as_str().unwrap().to_string();

    // Round-trip through note.readAsset by workspace-asset:// URL.
    let read_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"note.readAsset","params":{{"workspaceId":"ws-asset","asset":"{url}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &read_frame).await;
    assert_eq!(resp["result"]["assetId"], Value::from(asset_id));
    assert_eq!(resp["result"]["mimeType"], "image/png");
    assert_eq!(resp["result"]["data"], "aGVsbG8=");

    // Missing data → -32602 (PROTOCOL §9).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":3,"method":"note.saveAsset","params":{"workspaceId":"ws-asset","mimeType":"image/png"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(resp["error"]["message"], "Missing required parameter: data");

    srv.ws.stop().await;
}

/// Client-called `host.openInEditor` over WSS (PROTOCOL §5.14): a WSS
/// connection resolves as remote, so the daemon re-dispatches the intent to
/// the connected client as the FE-served reverse RPC (`id: "rev-<n>"`) and
/// echoes `{ ok: true }` back on the original request; missing params are
/// -32602.
#[tokio::test]
async fn wss_host_open_in_editor_reverse_round_trip() {
    let srv = start(WsOptions::default()).await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;

    let call = r#"{"jsonrpc":"2.0","id":1,"method":"host.openInEditor","params":{"editorId":"vscode","path":"/repo/src/main.rs","line":12,"column":3}}"#;
    ws.send(Message::Text(call.to_string()))
        .await
        .expect("send");

    // The daemon turns the trigger into a reverse request on the same socket.
    let mut final_response = None;
    while final_response.is_none() {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "host.openInEditor" {
                    let id = v["id"].as_str().expect("reverse id");
                    assert!(id.starts_with("rev-"), "reverse ids use rev-: {id}");
                    assert_eq!(v["params"]["editorId"], "vscode");
                    assert_eq!(v["params"]["path"], "/repo/src/main.rs");
                    assert_eq!(v["params"]["line"], 12);
                    assert_eq!(v["params"]["column"], 3);
                    let reply = serde_json::json!({
                        "jsonrpc": "2.0", "id": id, "result": { "ok": true }
                    });
                    ws.send(Message::Text(reply.to_string()))
                        .await
                        .expect("send reverse reply");
                } else if v["id"] == 1 {
                    final_response = Some(v);
                }
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let resp = final_response.unwrap();
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    assert_eq!(resp["result"]["ok"], true);

    // Missing editorId → -32602 without any reverse dispatch.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"host.openInEditor","params":{"path":"/repo/src/main.rs"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    srv.ws.stop().await;
}
