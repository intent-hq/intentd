//! WSS listener + lifecycle integration tests (M5.3, §5.2/§5.6).
//!
//! Drives a real [`WsApiServer`] over TLS: `/health`, the upgrade auth gate,
//! a JSON-RPC round-trip that must be byte-identical to the UDS transport, and
//! the §5.6 hardening guarantees (fail-fast bind on an occupied port,
//! graceful-shutdown restart, heartbeat termination). The client pins the
//! M5.1 self-signed fingerprint. A separate insecure-mode test proves the
//! plain-`ws://` accept path serves JSON-RPC with no TLS and no bearer token.

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, Result as CoreResult,
    TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, serve_uds, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
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
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let mut services = Services::new(store.clone())
        .with_assets_root(dir.join("assets"))
        .with_workspaces_root(workspaces_root);
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
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    if opts.base_port == WsOptions::default().base_port {
        opts.base_port = 0;
    }
    opts.bind_address = Ipv4Addr::LOCALHOST.into();
    let ws =
        WsApiServer::new(api.clone(), bus.clone(), &tls, token_store, opts, None).expect("server");
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

/// Agent ids are server-assigned: `agent.create` rejects a client-supplied
/// `agentId` with `-32602` ("server-assigned"), and a create without the
/// field mints an `agent-{uuid}` id that `agent.get` resolves (PROTOCOL §5.5).
#[tokio::test]
async fn wss_agent_create_rejects_client_supplied_agent_id() {
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

    // Stale-client shape: any client-supplied agentId is -32602.
    let reject_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","agentId":"{requested}","name":"WSS Client Id"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &reject_frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "client-supplied agentId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("server-assigned"),
        "error must say agent IDs are server-assigned: {rejected}"
    );

    // Without the field the daemon mints the id and agent.get resolves it.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"WSS Minted"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let minted = created["result"]["agent"]["id"]
        .as_str()
        .expect("server-minted id")
        .to_string();
    assert!(
        minted
            .strip_prefix("agent-")
            .is_some_and(|tail| uuid::Uuid::parse_str(tail).is_ok()),
        "server mints agent-{{uuid}}: {created}"
    );
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.get","params":{{"agentId":"{minted}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_eq!(
        got["result"]["agent"]["id"].as_str(),
        Some(minted.as_str()),
        "agent.get at the server-minted id must resolve: {got}"
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

    // Full-param create: exercise every new optional field. `provider` and
    // `isBackground` (G-A1/P3-1.2c) persist on the session;
    // `agentType`/`workspacePath`/`workspaceContext` are accepted but
    // deferred (per P2-12a audit).
    let params = format!(
        concat!(
            r#"{{"workspaceId":"{ws}","name":"WSS Wide","#,
            r#""model":"auggie:sonnet4.5","specialistId":"implementor","#,
            r#""provider":"auggie","agentType":"task-loop","#,
            r#""metadata":{{"tag":"unit"}},"workspacePath":"/tmp/wid","#,
            r#""workspaceContext":{{"selection":"note:1"}},"isBackground":true}}"#
        ),
        ws = ws_id,
    );
    let create_frame =
        format!(r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{params}}}"#);
    let created_resp = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let minted = created_resp["result"]["agent"]["id"]
        .as_str()
        .expect("server-minted id")
        .to_string();
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.get","params":{{"agentId":"{minted}"}}}}"#
    );
    let got_resp = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    let sess = [created_resp, got_resp];

    let created = &sess[0]["result"]["agent"];
    // Return shape is the full `AgentLite` projection — a superset of the
    // pre-widening `{id, name}` snippet. Assert the persisted fields land.
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
        Some(minted.as_str()),
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
    let minimal_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let minimal = wss_call(srv.port, srv.cfg.clone(), &minimal_frame).await;
    assert!(
        minimal["result"]["agent"]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("agent-")),
        "minimal create must still succeed with a server-minted id: {minimal}",
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

/// A8: the four session-shape RPCs unblocking the FE agent-backend-handler
/// retirement (C1d/C1e) — `agent.getSession`, `agent.update`,
/// `agent.appendMessage`, `agent.replaceMessages` — over the real WSS
/// transport. Asserts the wire contract PROTOCOL §5.5 documents: full
/// `AgentSession` projection (superset of `AgentLite`), whitelisted partial
/// updates round-tripping through `agent.getSession`, append-then-swap
/// transcript mutations under freshly-minted `seq: 0..n`, and the `-32602`
/// error codes for unknown agents / unknown fields.
#[tokio::test]
async fn wss_agent_session_shape_rpcs_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"A8"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            // 1) create an agent to operate on.
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"A8"}}}}"#
            ),
        ],
    )
    .await;
    let agent_id = sess[0]["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // 2) drive the four new RPCs on one long-lived WSS session so the wire
    //    contract is exercised end-to-end (upgrade → JSON-RPC → services →
    //    store, and back). All four ids are unique so a session-level replay
    //    would fail loudly.
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            // agent.getSession — full projection (has `messages` array).
            format!(
                r#"{{"jsonrpc":"2.0","id":10,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // agent.update — patch systemPrompt + isBackground.
            format!(
                r#"{{"jsonrpc":"2.0","id":11,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"systemPrompt":"be helpful","isBackground":true}}}}}}"#
            ),
            // agent.getSession — verify patch persisted.
            format!(
                r#"{{"jsonrpc":"2.0","id":12,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // agent.update — unknown field → -32602.
            format!(
                r#"{{"jsonrpc":"2.0","id":13,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"nope":"x"}}}}}}"#
            ),
            // STAB-19: getSession before append to capture baseline updated_at.
            format!(
                r#"{{"jsonrpc":"2.0","id":13.5,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // agent.appendMessage — append one user message.
            format!(
                r#"{{"jsonrpc":"2.0","id":14,"method":"agent.appendMessage","params":{{"agentId":"{agent_id}","role":"user","contentBlocks":[{{"type":"text","text":"wake"}}]}}}}"#
            ),
            // STAB-19: getSession after append to verify updated_at advanced.
            format!(
                r#"{{"jsonrpc":"2.0","id":14.5,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // agent.replaceMessages — atomic swap → seq 0/1.
            format!(
                r#"{{"jsonrpc":"2.0","id":15,"method":"agent.replaceMessages","params":{{"agentId":"{agent_id}","messages":[{{"role":"user","contentBlocks":[{{"type":"text","text":"edit"}}]}},{{"role":"assistant","contentBlocks":[{{"type":"text","text":"ok"}}]}}]}}}}"#
            ),
            // agent.getSession unknown → -32602 "Agent not found".
            r#"{"jsonrpc":"2.0","id":16,"method":"agent.getSession","params":{"agentId":"agent-00000000-0000-0000-0000-000000000000"}}"#.to_string(),
        ],
    )
    .await;

    // agent.getSession returns the full `AgentSession` shape.
    assert_eq!(
        sess[0]["result"]["session"]["id"].as_str(),
        Some(agent_id.as_str()),
        "getSession returns session: {}",
        sess[0]
    );
    assert!(
        sess[0]["result"]["session"]["messages"].is_array(),
        "getSession carries messages array (AgentSession, not AgentLite): {}",
        sess[0]
    );

    // agent.update returns { success, agent: AgentLite }.
    assert_eq!(
        sess[1]["result"]["success"],
        Value::Bool(true),
        "update: {}",
        sess[1]
    );
    assert_eq!(
        sess[1]["result"]["agent"]["id"].as_str(),
        Some(agent_id.as_str())
    );

    // The patch round-trips through getSession.
    assert_eq!(
        sess[2]["result"]["session"]["systemPrompt"].as_str(),
        Some("be helpful"),
        "update persisted: {}",
        sess[2]
    );
    assert_eq!(
        sess[2]["result"]["session"]["isBackground"].as_bool(),
        Some(true)
    );

    // Unknown fields in `changes` → -32602.
    assert_eq!(
        sess[3]["error"]["code"].as_i64(),
        Some(-32602),
        "unknown field must be -32602: {}",
        sess[3]
    );

    // STAB-19: capture updated_at before append.
    let before_updated_at = sess[4]["result"]["session"]["updatedAt"]
        .as_str()
        .expect("updated_at before append");

    // appendMessage persists one row.
    assert_eq!(sess[5]["result"]["success"], Value::Bool(true));
    assert_eq!(sess[5]["result"]["message"]["role"].as_str(), Some("user"));

    // STAB-19: updated_at must advance after append (STAB-19 regression).
    let after_updated_at = sess[6]["result"]["session"]["updatedAt"]
        .as_str()
        .expect("updated_at after append");
    assert!(
        after_updated_at > before_updated_at,
        "agent_session.updated_at must advance when a message is appended (STAB-19): before={}, after={}",
        before_updated_at,
        after_updated_at
    );

    // replaceMessages atomically swaps under fresh seq.
    assert_eq!(sess[7]["result"]["success"], Value::Bool(true));
    let swapped = sess[7]["result"]["messages"]
        .as_array()
        .expect("messages array");
    assert_eq!(swapped.len(), 2);
    assert_eq!(swapped[0]["seq"].as_i64(), Some(0));
    assert_eq!(swapped[1]["seq"].as_i64(), Some(1));

    // Unknown-agent lookups surface as -32602 "Agent not found".
    assert_eq!(
        sess[8]["error"]["code"].as_i64(),
        Some(-32602),
        "unknown agent must be -32602: {}",
        sess[8]
    );
    assert_eq!(
        sess[8]["error"]["message"].as_str(),
        Some("Agent not found")
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

#[tokio::test]
async fn wss_models_list_with_provider_id_and_force_refresh() {
    // models.list { providerId, forceRefresh } (§5.30): per-provider catalog
    // through the generic cache. Unknown providers degrade to the static
    // fallback (`source: "static"` + warning, never an error); cortex is
    // feature-code gated (empty list + warning under its own source tag).
    let srv = start(WsOptions::default()).await;

    let frame = r#"{"jsonrpc":"2.0","id":8,"method":"models.list","params":{"providerId":"no-such-provider","forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 8);
    assert_eq!(resp["result"]["providerId"], "no-such-provider");
    assert_eq!(resp["result"]["source"], "static");
    assert!(resp["result"]["models"]
        .as_array()
        .expect("models")
        .is_empty());
    assert!(resp["result"]["warning"].is_string(), "{resp}");

    let frame =
        r#"{"jsonrpc":"2.0","id":9,"method":"models.list","params":{"providerId":"cortex"}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 9);
    assert_eq!(resp["result"]["providerId"], "cortex");
    assert_eq!(resp["result"]["source"], "cortex");
    assert!(resp["result"]["models"]
        .as_array()
        .expect("models")
        .is_empty());
    assert!(
        resp["result"]["warning"]
            .as_str()
            .expect("warning")
            .contains("Cortex"),
        "{resp}"
    );
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
    // Provider-neutrality: set auggie as active provider (these operations are auggie-specific).
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");

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
async fn wss_agent_enhance_prompt_unavailable_when_provider_not_auggie() {
    // Provider-neutrality gate: with a non-auggie active provider,
    // agent.enhancePrompt returns a typed `{ available: false, reason }`
    // result instead of an error, so the FE can hide the affordance.
    let bin = fake_auggie_script(
        "gated-enhance",
        "printf '🤖\\n<augment-enhanced-prompt>never runs</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.store
        .set_setting("providers.active", "\"claude-code\"")
        .await
        .expect("set active provider");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":34,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 34);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "enhance-prompt requires auggie as the active provider"
        })
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_parse_failure_is_internal_error() {
    // A reply without the `<augment-enhanced-prompt>` tags in enhance mode is
    // the documented -32603 parse failure (§5.31).
    let bin = fake_auggie_script("notags", "printf '🤖\\nno tags here\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");
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
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");
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

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_round_trip() {
    // agent.completeOnce (§5.32) — stateless one-shot prompt→completion.
    // `{ prompt }` returns `{ text }` with the cleaned CLI reply verbatim,
    // over the real pinned-TLS WSS transport.
    let bin = fake_auggie_script(
        "complete-ok",
        "printf '\u{1b}[32m🔧 Tool call: noise\u{1b}[0m\\n🤖\\nfix-login-flow\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":41,"method":"agent.completeOnce","params":{"prompt":"slug for login fix"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 41);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["result"]["text"], "fix-login-flow");
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_unavailable_when_provider_not_auggie() {
    // Provider-neutrality gate: with a non-auggie active provider,
    // agent.completeOnce returns a typed `{ available: false, reason }`
    // result instead of an error.
    let bin = fake_auggie_script("gated-complete", "printf '🤖\\nnever-runs\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.store
        .set_setting("providers.active", "\"claude-code\"")
        .await
        .expect("set active provider");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":44,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 44);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "completeOnce requires auggie as the active provider"
        })
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_agent_complete_once_cli_missing_is_internal_error() {
    // A missing/unspawnable auggie binary surfaces as -32603 rather than
    // hanging — the daemon reaps and returns a JSON-RPC error (§5.32).
    let srv = start_with_auggie(
        WsOptions::default(),
        Some(std::path::PathBuf::from("/nonexistent/intentd-wss/auggie")),
    )
    .await;
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":42,"method":"agent.completeOnce","params":{"prompt":"hi"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 42);
    assert_eq!(resp["error"]["code"], -32603);
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_timeout_reaps_and_errors() {
    // A hung CLI is reaped when the client-provided timeout elapses; the
    // response is a -32603 whose `data` carries the timeout message. Proves
    // the standing design principle — the daemon owns cleanup on in-flight
    // failure, no session/agent state is leaked.
    let bin = fake_auggie_script("complete-slow", "sleep 30");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.store
        .set_setting("providers.active", "\"auggie\"")
        .await
        .expect("set active provider");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":43,"method":"agent.completeOnce","params":{"prompt":"hi","timeoutMs":200}}"#,
    )
    .await;
    assert_eq!(resp["id"], 43);
    assert_eq!(resp["error"]["code"], -32603);
    let data = resp["error"]["data"].as_str().unwrap_or_default();
    assert!(
        data.contains("timed out after 200ms"),
        "unexpected data: {data}"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_agent_complete_once_validates_params() {
    // Router-side -32602s (§5.32): missing prompt, blank prompt, non-positive
    // timeoutMs — all rejected before any CLI spawn.
    let srv = start(WsOptions::default()).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":44,"method":"agent.completeOnce","params":{}}"#,
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
        r#"{"jsonrpc":"2.0","id":45,"method":"agent.completeOnce","params":{"prompt":"   "}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(resp["error"]["message"], "prompt cannot be empty");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":46,"method":"agent.completeOnce","params":{"prompt":"hi","timeoutMs":0}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        "timeoutMs must be a positive integer"
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
async fn bind_fails_fast_on_occupied_port() {
    // Fixed-port fail-fast (§5.6): a busy configured port must surface the OS
    // bind error immediately — no port walking, no retry. Occupy the port for
    // the whole test so the listener has no chance to bind, then assert
    // `start()` returns an `AddrInUse` error on the SAME port it was asked for.
    // Bind the hog listener first, keep it open, and use its port for the test
    // to avoid TOCTOU (no free_port() release-then-rebind window).
    let _hog = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let base = _hog.local_addr().unwrap().port();
    let (api, bus, _store, dir) = make_services(None).await;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let mut opts = WsOptions {
        base_port: base,
        ..WsOptions::default()
    };
    opts.bind_address = Ipv4Addr::LOCALHOST.into();
    let ws = WsApiServer::new(api, bus, &tls, token_store, opts, None).expect("server");
    let err = ws
        .start()
        .await
        .expect_err("start must fail when the configured port is occupied");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::AddrInUse,
        "expected AddrInUse for busy port {base}, got {err:?}"
    );
    // The bound-port accessor must stay `None` — a failed bind never records a
    // port, so a subsequent restart can retry cleanly on the same configured
    // port (proven by the graceful_shutdown_allows_immediate_restart test).
    assert_eq!(ws.bound_port().await, None);
    ws.stop().await;
}

#[tokio::test]
async fn insecure_mode_serves_plain_ws_without_token() {
    // Insecure dev mode: no TLS acceptor, no bearer-token enforcement. A plain
    // TCP client must be able to open `ws://.../ws` with NO `Authorization`
    // header and complete a JSON-RPC round-trip. The listener's `fingerprint()`
    // is `None` and `is_insecure()` reports `true` so `system.status` surfaces
    // the real posture.
    let (api, bus, _store, _dir) = make_services(None).await;
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    assert!(ws.is_insecure(), "constructor selects insecure posture");
    assert!(ws.fingerprint().is_none(), "no TLS cert ⇒ no fingerprint");
    let port = ws.start().await.expect("start");
    // Open a plain WebSocket (no TLS wrapping) to the listener with NO token
    // in either the query string or the `Authorization` header.
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#.to_string(),
    ))
    .await
    .expect("send");
    let resp = loop {
        match sock.next().await {
            Some(Ok(Message::Text(text))) => {
                break serde_json::from_str::<Value>(&text).expect("json");
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    };
    assert_eq!(resp["id"], 1, "id echoed");
    assert!(
        resp.get("result").is_some(),
        "insecure ws:// round-trip returns a result: {resp}"
    );
    ws.stop().await;
}

#[tokio::test]
async fn graceful_shutdown_allows_immediate_restart() {
    // NOTE: This test verifies fixed-port restart semantics (same port reclaimed
    // after stop). Pick a dynamically-available port to avoid hard-coded collisions.
    let fixed_port = free_port();
    let srv = start(WsOptions {
        base_port: fixed_port,
        ..WsOptions::default()
    })
    .await;
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
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
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
        metadata: NoteMetadata::default(),
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
        n.metadata.task = Some(TaskMetadata {
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

/// `task.removeAgentFromAllTasks` (PROTOCOL §5.4 extension) over the real WSS
/// wire: strips the target agent from every task-note's `assignedAgentIds` in
/// the workspace and reports the number of task-notes touched. The response
/// envelope is `{ ok: true, updatedCount: <n> }` and the mutation persists
/// through the shared store so subsequent `note.get` reads see the stripped
/// arrays. Non-target agents and non-task notes are left untouched. Replay is
/// idempotent — a second call with the same agent id updates zero notes.
#[tokio::test]
async fn wss_task_remove_agent_from_all_tasks_round_trip() {
    use intent_core::AgentId;

    let srv = start(WsOptions::default()).await;

    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .expect("insert workspace");

    let victim = AgentId::from("agent-victim");
    let other = AgentId::from("agent-other");

    let mk_task = |id: &str, agents: Vec<AgentId>| {
        let mut n = fixture_note(&ws, id, "body");
        n.metadata.task = Some(TaskMetadata {
            status: TaskStatus::InProgress,
            assigned_agent_ids: agents,
            ..Default::default()
        });
        n
    };
    for n in [
        mk_task("task-a", vec![victim.clone(), other.clone()]),
        mk_task("task-b", vec![other.clone()]),
        mk_task("task-c", vec![victim.clone()]),
    ] {
        srv.store.insert_note(&n).await.expect("insert task");
    }
    // A plain (non-task) note is left alone even if it shares an id shape.
    srv.store
        .insert_note(&fixture_note(&ws, "plain", "not a task"))
        .await
        .expect("insert plain");

    let req = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"task.removeAgentFromAllTasks","params":{{"workspaceId":"{}","agentId":"{}"}}}}"#,
        ws.0, victim.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &req).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    assert!(resp["error"].is_null(), "unexpected error envelope: {resp}");
    let result = &resp["result"];
    assert_eq!(result["ok"], true, "envelope: {resp}");
    assert_eq!(result["updatedCount"], 2, "two matching tasks: {resp}");

    // Post-condition: victim is stripped from A and C; B is untouched.
    let a = srv
        .api
        .get_note(ws.clone(), NoteId::from("task-a"))
        .await
        .expect("get_note task-a");
    assert_eq!(
        a.metadata.task.as_ref().unwrap().assigned_agent_ids,
        vec![other.clone()]
    );
    let b = srv
        .api
        .get_note(ws.clone(), NoteId::from("task-b"))
        .await
        .expect("get_note task-b");
    assert_eq!(
        b.metadata.task.as_ref().unwrap().assigned_agent_ids,
        vec![other]
    );
    let c = srv
        .api
        .get_note(ws.clone(), NoteId::from("task-c"))
        .await
        .expect("get_note task-c");
    assert!(c
        .metadata
        .task
        .as_ref()
        .unwrap()
        .assigned_agent_ids
        .is_empty());

    // Replay is idempotent — the second call touches zero notes.
    let req2 = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"task.removeAgentFromAllTasks","params":{{"workspaceId":"{}","agentId":"{}"}}}}"#,
        ws.0, victim.0
    );
    let resp2 = wss_call(srv.port, srv.cfg.clone(), &req2).await;
    let result2 = &resp2["result"];
    assert_eq!(result2["ok"], true);
    assert_eq!(result2["updatedCount"], 0);

    // Param validation still routes through `-32602` on the wire.
    let missing_agent = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"task.removeAgentFromAllTasks","params":{{"workspaceId":"{}"}}}}"#,
        ws.0
    );
    let resp3 = wss_call(srv.port, srv.cfg.clone(), &missing_agent).await;
    assert_eq!(resp3["error"]["code"], -32602);
    assert_eq!(
        resp3["error"]["message"],
        "Missing required parameter: agentId"
    );

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

/// `file-tracking.loadCommits` with workspace boundary over WSS: proves the
/// daemon returns `boundarySha` and bounds commits to `boundary..HEAD`, and
/// the `includeOlder` parameter fetches pre-boundary commits.
#[tokio::test]
async fn wss_file_tracking_load_commits_bounded() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with a base commit on main + workspace commit on a branch.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let repo = Path::new("/tmp").join(format!("intentd-wssftlc-{}", &short[..8]));
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
    std::fs::write(repo.join("base.txt"), "base content\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "base commit"]);
    let base_sha = String::from_utf8(
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

    git(&["checkout", "-q", "-b", "feat/test"]);
    std::fs::write(repo.join("feature.txt"), "workspace content\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "workspace commit"]);

    // Create workspace with baseRef=main and baseCommitSha set.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"File Tracking WSS","worktreePath":"{}","path":"{}","baseRef":"main","baseCommitSha":"{}"}}}}"#,
        repo.display(),
        repo.display(),
        base_sha,
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // (a) file-tracking.loadCommits default → bounded to workspace commits + boundarySha returned.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{}","limit":50}}}}"#,
            ws_id
        ),
    )
    .await;
    assert!(
        resp["result"]["commits"].is_array(),
        "commits field: {resp}"
    );
    assert!(
        resp["result"]["boundarySha"].is_string(),
        "boundarySha field: {resp}"
    );
    assert!(
        resp["result"]["nextToken"].is_null(),
        "nextToken field: {resp}"
    );

    let commits = resp["result"]["commits"].as_array().unwrap();
    let boundary_sha = resp["result"]["boundarySha"].as_str().unwrap();

    // Should have exactly 1 commit (workspace commit), not 2.
    assert_eq!(commits.len(), 1, "should only return workspace commits");
    assert_eq!(commits[0]["message"], "workspace commit");

    // The boundary should match the base commit.
    assert_eq!(boundary_sha, base_sha, "boundary should be base commit SHA");

    // (b) file-tracking.loadCommits with includeOlder: true → pre-boundary commits returned.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{}","limit":50,"includeOlder":true}}}}"#,
            ws_id
        ),
    )
    .await;
    let older_commits = resp["result"]["commits"].as_array().unwrap();

    // Should return the base commit (pre-boundary history).
    assert_eq!(
        older_commits.len(),
        1,
        "should return pre-boundary commits with includeOlder"
    );
    assert_eq!(older_commits[0]["message"], "base commit");

    // (c) Workspace without boundary info → unbounded, boundarySha null.
    let create_unbounded = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"workspace.create","params":{{"title":"Unbounded WSS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created_unbounded = wss_call(srv.port, srv.cfg.clone(), &create_unbounded).await;
    let ws_id_unbounded = created_unbounded["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id unbounded")
        .to_string();

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{}","limit":50}}}}"#,
            ws_id_unbounded
        ),
    )
    .await;
    assert!(
        resp["result"]["boundarySha"].is_null(),
        "boundarySha should be null without boundary info"
    );
    let unbounded_commits = resp["result"]["commits"].as_array().unwrap();
    // Should return all commits unbounded.
    assert_eq!(
        unbounded_commits.len(),
        2,
        "should return all commits without boundary"
    );

    // (d) Fail-closed safety net: workspace with boundary info but unresolvable → empty.
    let create_unresolvable = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.create","params":{{"title":"Unresolvable WSS","worktreePath":"{}","path":"{}","baseRef":"nonexistent","baseCommitSha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created_unresolvable = wss_call(srv.port, srv.cfg.clone(), &create_unresolvable).await;
    let ws_id_unresolvable = created_unresolvable["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id unresolvable")
        .to_string();

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{}","limit":50}}}}"#,
            ws_id_unresolvable
        ),
    )
    .await;
    assert!(
        resp["result"]["boundarySha"].is_null(),
        "boundarySha should be null when boundary is unresolvable"
    );
    let unresolvable_commits = resp["result"]["commits"].as_array().unwrap();
    // Fail-closed safety net: should return empty (not arbitrary base-branch commits).
    assert_eq!(
        unresolvable_commits.len(),
        0,
        "should return empty when boundary info exists but is unresolvable (fail-closed)"
    );

    // (e) Fail-closed holds with includeOlder: boundary info exists but unresolvable + includeOlder → still empty.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{}","limit":50,"includeOlder":true}}}}"#,
            ws_id_unresolvable
        ),
    )
    .await;
    let unresolvable_older = resp["result"]["commits"].as_array().unwrap();
    // Fail-closed safety net must hold even when includeOlder is true.
    assert_eq!(
        unresolvable_older.len(),
        0,
        "fail-closed safety net must hold with includeOlder=true"
    );

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
        // JSON-RPC (FE) note mutations resolve the version author to `user`
        // (reference parity with `notes.service.ts`); the `system` author is
        // reserved for daemon-internal writes such as the workspace-seed
        // spec-note snapshot.
        assert_eq!(entry["author"]["type"], "user");
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

/// `repo.remove` over WSS (PROTOCOL §5.11): removing a registered path deletes
/// it from the known-repo registry (`removed: true`, gone from `repo.list`);
/// removing an unknown path is `removed: false`; missing `path` is -32602.
#[tokio::test]
async fn wss_repo_remove_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed two known repos directly in the store (repo.list is read-only).
    srv.store
        .upsert_known_repo("/src/keep", "keep", None)
        .await
        .expect("seed keep");
    srv.store
        .upsert_known_repo("/src/gone", "gone", Some("owner"))
        .await
        .expect("seed gone");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"repo.list","params":{}}"#,
    )
    .await;
    assert_eq!(resp["result"]["repos"].as_array().unwrap().len(), 2);

    // Remove one; the response carries `{ removed: true }`.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"repo.remove","params":{"path":"/src/gone"}}"#,
    )
    .await;
    assert_eq!(resp["result"], serde_json::json!({ "removed": true }));

    // The registry no longer lists it — this is what the FE re-reads.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":3,"method":"repo.list","params":{}}"#,
    )
    .await;
    let repos = resp["result"]["repos"].as_array().unwrap();
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0]["path"], "/src/keep");

    // Removing it again is a no-op, not an error.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":4,"method":"repo.remove","params":{"path":"/src/gone"}}"#,
    )
    .await;
    assert_eq!(resp["result"], serde_json::json!({ "removed": false }));

    // Missing path → -32602 (PROTOCOL §9).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":5,"method":"repo.remove","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(resp["error"]["message"], "Missing required parameter: path");

    srv.ws.stop().await;
}

/// End-to-end WSS coverage for the workspace lifecycle helpers added by the
/// thin-FE remediation (PROTOCOL.md §5.1): `workspace.duplicate`,
/// `workspace.restore`, `workspace.cleanup`, `workspace.findRepositories`,
/// and `workspace.initializeRepository`. Every method is driven over the
/// real pinned-TLS WebSocket transport and its response envelope is asserted
/// against the documented shape.
#[tokio::test]
async fn wss_workspace_lifecycle_helpers_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a workspace to duplicate/restore/clean up.
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Source WS"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    // workspace.duplicate returns { workspace }, defaulting the title to the
    // "<source> (Copy)" convention.
    let dup = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.duplicate","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert!(dup["result"]["workspace"].is_object(), "workspace object");
    assert_eq!(dup["result"]["workspace"]["title"], "Source WS (Copy)");
    let dup_id = dup["result"]["workspace"]["id"]
        .as_str()
        .expect("dup id")
        .to_string();
    assert_ne!(dup_id, ws_id, "duplicate must mint a fresh id");

    // Explicit newTitle overrides the auto-suffix.
    let dup2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"workspace.duplicate","params":{{"workspaceId":"{ws_id}","newTitle":"Custom Copy"}}}}"#
        ),
    )
    .await;
    assert_eq!(dup2["result"]["workspace"]["title"], "Custom Copy");

    // workspace.restore alias of unarchive: archive first, then restore.
    let _ = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"workspace.archive","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let restored = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"workspace.restore","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(restored["result"]["workspace"]["id"], ws_id);
    assert_eq!(restored["result"]["workspace"]["archived"], false);

    // workspace.cleanup returns { success: true } on an existing workspace.
    let cleanup = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.cleanup","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(cleanup["result"], serde_json::json!({ "success": true }));

    // workspace.cleanup on a missing workspace → -32602 "Workspace not found".
    let cleanup_missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":7,"method":"workspace.cleanup","params":{"workspaceId":"ghost"}}"#,
    )
    .await;
    assert_eq!(cleanup_missing["error"]["code"], -32602);
    assert_eq!(cleanup_missing["error"]["message"], "Workspace not found");

    // workspace.findRepositories returns { repositories: string[] }. Seed a
    // scratch dir with a fake `.git` folder so the scan produces a match.
    let scratch =
        std::env::temp_dir().join(format!("itd-find-repos-{}", uuid::Uuid::new_v4().simple()));
    let repo_a = scratch.join("repo-a");
    std::fs::create_dir_all(repo_a.join(".git")).expect("mkdir repo-a/.git");
    std::fs::create_dir_all(scratch.join("plain")).expect("mkdir plain");
    let find = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.findRepositories","params":{{"directory":"{}"}}}}"#,
            scratch.display()
        ),
    )
    .await;
    let repos = find["result"]["repositories"]
        .as_array()
        .expect("repositories array");
    assert!(
        repos
            .iter()
            .any(|r| r.as_str() == Some(repo_a.to_str().unwrap())),
        "repo-a must be in {repos:?}"
    );
    let _ = std::fs::remove_dir_all(&scratch);

    // workspace.findRepositories without `directory` → -32602.
    let find_missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":10,"method":"workspace.findRepositories","params":{}}"#,
    )
    .await;
    assert_eq!(find_missing["error"]["code"], -32602);

    // workspace.initializeRepository returns { success: true }. Point it at a
    // fresh scratch dir and assert `.git` shows up. Gated on `git` being on
    // PATH; skip the assertion cleanly when it's absent.
    if std::process::Command::new("git")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
    {
        let init_path =
            std::env::temp_dir().join(format!("itd-init-{}", uuid::Uuid::new_v4().simple()));
        let init = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":11,"method":"workspace.initializeRepository","params":{{"path":"{}"}}}}"#,
                init_path.display()
            ),
        )
        .await;
        assert_eq!(init["result"], serde_json::json!({ "success": true }));
        assert!(init_path.join(".git").exists(), ".git directory seeded");
        assert!(init_path.join("README.md").exists(), "README seeded");
        assert!(init_path.join(".gitignore").exists(), ".gitignore seeded");
        let _ = std::fs::remove_dir_all(&init_path);
    }

    // workspace.initializeRepository without `path` → -32602.
    let init_missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":12,"method":"workspace.initializeRepository","params":{}}"#,
    )
    .await;
    assert_eq!(init_missing["error"]["code"], -32602);

    srv.ws.stop().await;
}

/// Helper to obtain an ephemeral port by bind-then-release. Only used for tests
/// that genuinely need a fixed port to exercise fixed-port semantics (e.g.
/// graceful_shutdown_allows_immediate_restart). Prefer `base_port: 0` for normal tests.
fn free_port() -> u16 {
    use std::net::TcpListener;
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}
