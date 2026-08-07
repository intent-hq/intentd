//! WSS listener + lifecycle integration tests (M5.3, §5.2/§5.6).
//!
//! Drives a real [`WsApiServer`] over TLS: `/health`, the upgrade auth gate,
//! a JSON-RPC round-trip that must be byte-identical to the UDS transport, and
//! the §5.6 hardening guarantees (fail-fast bind on an occupied port,
//! graceful-shutdown restart, heartbeat termination). The client pins the
//! M5.1 self-signed fingerprint. A separate insecure-mode test proves the
//! plain-`ws://` accept path serves JSON-RPC with no TLS and no bearer token.

mod common;

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
    MAX_INBOUND_MESSAGE_BYTES,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

/// Create a temp dir with a recognizable prefix under the system temp root.
/// The returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test tempdir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
}

/// Build a real `Services` API + event bus over a fresh temp SQLite store.
/// The store is returned alongside so tests that need to seed fixtures with a
/// fixed id (e.g. the workspace `spec` note) can `store.insert_*` directly,
/// since `note.create` mints a fresh `NoteId` by design. `auggie_bin`
/// optionally pins the auggie binary `agent.enhancePrompt` spawns (§5.31) to a
/// deterministic fixture script.
/// Note: the event bus is attached to `Services` (`with_event_bus`), so
/// service-emitted events (`workspace:updated`, note events, …) flow to
/// `events.subscribe` subscribers in EVERY test built on this harness — tests
/// that read frames in a loop should match on `id`/`method` rather than
/// assume the next frame is their RPC response.
async fn make_services(
    auggie_bin: Option<std::path::PathBuf>,
    models_cache_dir: Option<std::path::PathBuf>,
) -> (
    Arc<dyn WorkspaceApi>,
    EventBus,
    Store,
    Arc<intent_services::SettingsRegistry>,
    tempfile::TempDir,
) {
    let dir = test_tempdir("intentd-wss-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let registry = Arc::new(
        intent_services::SettingsRegistry::load(dir.path().join("config.toml"))
            .expect("load settings registry"),
    );
    let mut services = Services::new(store.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(registry.clone())
        .with_event_bus(bus.clone());
    if let Some(bin) = auggie_bin {
        services = services.with_auggie_bin(bin);
    }
    if let Some(cache_dir) = models_cache_dir {
        services = services.with_models_cache_dir(cache_dir);
    }
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    (api, bus, store, registry, dir)
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
    registry: Arc<intent_services::SettingsRegistry>,
    _dir: tempfile::TempDir,
}

impl Server {
    /// Seed a TOML-backed setting through the wired registry (the source
    /// `Services::effective_settings` reads).
    fn set_setting(&self, path: &str, value: Value) {
        self.registry
            .apply(&[(path.to_string(), value)])
            .expect("apply setting");
    }
}

/// Build + start a WSS listener with the given options on a free base port.
async fn start(opts: WsOptions) -> Server {
    start_with_auggie(opts, None).await
}

/// [`start`] with an optional auggie-binary override for `agent.enhancePrompt`
/// tests (§5.31).
async fn start_with_auggie(opts: WsOptions, auggie_bin: Option<std::path::PathBuf>) -> Server {
    start_with_auggie_and_models_cache(opts, auggie_bin, None).await
}

/// [`start_with_auggie`] with an optional persisted models-cache dir so
/// `models.list` cache-fallback tests (§5.30) can seed a last-good entry.
async fn start_with_auggie_and_models_cache(
    mut opts: WsOptions,
    auggie_bin: Option<std::path::PathBuf>,
    models_cache_dir: Option<std::path::PathBuf>,
) -> Server {
    let (api, bus, store, registry, dir) = make_services(auggie_bin, models_cache_dir).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
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
        registry,
        _dir: dir,
    }
}

/// Open a pinned TLS stream to the listener.
async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    common::tls_connect_with_retry(port, cfg).await
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
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One authenticated WSS JSON-RPC round-trip: send `frame`, return the first
/// text response parsed as JSON.
async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string().into()))
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
        ws.send(Message::Text(frame.into())).await.expect("send");
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
                r#"{{"jsonrpc":"2.0","id":3,"method":"drafts.set","params":{{"workspaceId":"{ws_id}","agentId":"agent-wss","text":"wss draft","attachments":[{{"type":"image","imageData":"aGk=","imageMimeType":"image/png"}}]}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"drafts.get","params":{{"workspaceId":"{ws_id}","agentId":"agent-wss"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["clientId"], "cli-wss");
    assert_eq!(
        sess[0]["result"]["protocolVersion"], "6.1",
        "explicit top-level protocolVersion in the client.hello result (§5.17)"
    );
    assert_eq!(
        sess[0]["result"]["server"]["locality"], "remote",
        "WSS ⇒ remote in the client.hello server block (§5.14/§5.17)"
    );
    assert_eq!(sess[1]["result"]["ok"], true);
    assert!(sess[1]["result"]["updatedAt"].is_string());
    assert_eq!(sess[2]["result"]["text"], "wss draft");
    assert_eq!(
        sess[2]["result"]["attachments"],
        serde_json::json!([{ "type": "image", "imageData": "aGk=", "imageMimeType": "image/png" }]),
        "attachments round-trip verbatim (§5.16)"
    );

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
    assert_eq!(
        sess[1]["result"]["attachments"][0]["imageData"], "aGk=",
        "reconnect restores the attachments"
    );
    srv.ws.stop().await;
}

/// `workspace.getAutoCommit` / `workspace.setAutoCommit` (§5.1): a freshly
/// created workspace mirrors the global `git.autoCommit` (default true) as
/// its own override (`source: "workspace"`); the setter persists a toggle
/// that the getter reads back and emits a self-sufficient `workspace:updated`
/// event carrying the `autoCommitEnabled` delta (§6.5); a missing or
/// wrong-typed `enabled` and an unknown workspace all surface `-32602`.
#[tokio::test]
async fn wss_workspace_auto_commit_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS AutoCommit"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    // Mirror-at-creation: the new row owns its override.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.getAutoCommit","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(got["result"]["autoCommit"]["enabled"], true);
    assert_eq!(got["result"]["autoCommit"]["source"], "workspace");

    // One persistent connection: subscribe first so the `workspace:updated`
    // notification from the toggle below is delivered to this client.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    async fn send_and_wait(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        frame: String,
        id: i64,
    ) -> Value {
        ws.send(Message::Text(frame.into())).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v.get("id") == Some(&serde_json::json!(id)) {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    let sub = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"events.subscribe","params":{{"eventTypes":["workspace:updated"],"workspaceId":"{ws_id}"}}}}"#
        ),
        3,
    )
    .await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // Toggle off; the setter echoes the persisted state.
    let set = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"workspace.setAutoCommit","params":{{"workspaceId":"{ws_id}","enabled":false}}}}"#
        ),
        4,
    )
    .await;
    assert_eq!(set["result"]["autoCommit"]["enabled"], false);
    assert_eq!(set["result"]["autoCommit"]["source"], "workspace");

    // The `workspace:updated` event's `changes` delta is self-sufficient
    // (§6.5): subscribers see the toggled flag without a follow-up read.
    let evt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "workspace:updated"
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for workspace:updated");
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"]["changes"],
        serde_json::json!({ "autoCommitEnabled": false }),
        "event delta carries the toggled flag: {evt}"
    );

    // Read-back sees the persisted override.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"workspace.getAutoCommit","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(got["result"]["autoCommit"]["enabled"], false);
    assert_eq!(got["result"]["autoCommit"]["source"], "workspace");

    // Missing `enabled` → -32602.
    let bad = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.setAutoCommit","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602);
    assert_eq!(
        bad["error"]["message"],
        "Missing required parameter: enabled (boolean)"
    );

    // Present-but-wrong-typed `enabled` → -32602 with the invalid wording.
    let wrong = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"workspace.setAutoCommit","params":{{"workspaceId":"{ws_id}","enabled":"true"}}}}"#
        ),
    )
    .await;
    assert_eq!(wrong["error"]["code"], -32602);
    assert_eq!(
        wrong["error"]["message"],
        "Invalid parameter: enabled must be a boolean"
    );

    // Unknown workspace → -32602 "Workspace not found".
    let missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":8,"method":"workspace.getAutoCommit","params":{"workspaceId":"ws-none"}}"#,
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602);
    assert_eq!(missing["error"]["message"], "Workspace not found");
    srv.ws.stop().await;
}

/// Regression (PROTOCOL §5.16 "Opaque keys & reserved sentinels"): draft keys
/// are opaque — the FE's New Workspace modal saves its pre-creation draft
/// under `__new-workspace__` / `__initializer__` before any workspace row
/// exists, so `drafts.set` → `drafts.get` → `drafts.clear` must round-trip
/// without a workspace.
#[tokio::test]
async fn wss_drafts_sentinel_keys_round_trip_without_workspace() {
    let srv = start(WsOptions::default()).await;
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            r#"{"jsonrpc":"2.0","id":1,"method":"client.hello","params":{"clientId":"cli-sentinel","name":"WSS"}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":2,"method":"drafts.set","params":{"workspaceId":"__new-workspace__","agentId":"__initializer__","text":"pre-create draft"}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":3,"method":"drafts.get","params":{"workspaceId":"__new-workspace__","agentId":"__initializer__"}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":4,"method":"drafts.clear","params":{"workspaceId":"__new-workspace__","agentId":"__initializer__"}}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":5,"method":"drafts.get","params":{"workspaceId":"__new-workspace__","agentId":"__initializer__"}}"#.to_string(),
        ],
    )
    .await;
    assert_eq!(sess[0]["result"]["clientId"], "cli-sentinel");
    assert_eq!(
        sess[1]["result"]["ok"], true,
        "drafts.set under the sentinel keys succeeds with no workspace row (§5.16)"
    );
    assert!(sess[1]["result"]["updatedAt"].is_string());
    assert_eq!(sess[2]["result"]["text"], "pre-create draft");
    assert_eq!(sess[3]["result"]["ok"], true);
    assert!(
        sess[4]["result"].is_null(),
        "cleared sentinel draft reads back null"
    );
    srv.ws.stop().await;
}

/// Fast-path `-32602` discriminator (PROTOCOL §3.3, monorepo#1364): every
/// fast-path family that rejects invalid params — subscription params
/// (`events.subscribe`), `drafts.*`, `forward.*`, `host.*`, `browser.exec`,
/// `client.hello` — carries the machine-readable `error.data.code =
/// "invalid-params"` on the wire, mirroring the dispatcher. `browser.exec`
/// validation short-circuits before the FE reverse RPC, so no frontend is
/// needed.
#[tokio::test]
async fn wss_fast_path_invalid_params_carry_data_code() {
    let srv = start(WsOptions::default()).await;
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            // events.subscribe: missing eventTypes.
            r#"{"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{}}"#.to_string(),
            // drafts.set: missing workspaceId/agentId.
            r#"{"jsonrpc":"2.0","id":2,"method":"drafts.set","params":{"text":"x"}}"#.to_string(),
            // forward.create: missing remotePort.
            r#"{"jsonrpc":"2.0","id":3,"method":"forward.create","params":{}}"#.to_string(),
            // host.directoryStatus: missing path.
            r#"{"jsonrpc":"2.0","id":4,"method":"host.directoryStatus","params":{}}"#.to_string(),
            // browser.exec: missing actions (rejected before the reverse RPC).
            r#"{"jsonrpc":"2.0","id":5,"method":"browser.exec","params":{}}"#.to_string(),
            // client.hello: non-string clientId.
            r#"{"jsonrpc":"2.0","id":6,"method":"client.hello","params":{"clientId":42}}"#
                .to_string(),
        ],
    )
    .await;
    for (i, resp) in sess.iter().enumerate() {
        assert_eq!(
            resp["error"]["code"].as_i64(),
            Some(-32602),
            "frame {i} is -32602: {resp}"
        );
        assert_eq!(
            resp["error"]["data"]["code"], "invalid-params",
            "frame {i} carries the data.code discriminator: {resp}"
        );
    }
    srv.ws.stop().await;
}

/// Transport size-limit regression (monorepo#472, monorepo#495): a text
/// message past the 40 MiB cap terminates the connection with a 1009
/// (Message Too Big) close frame; a large-but-legit single-frame message
/// above tungstenite's 16 MiB default frame size still round-trips, as does
/// a normal request on a fresh connection.
#[tokio::test]
async fn wss_oversized_message_terminates_connection() {
    use tokio_tungstenite::tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
    use tokio_tungstenite::tungstenite::protocol::frame::Frame;

    let srv = start(WsOptions::default()).await;

    // Over-limit fragmented message: the first fragment sits exactly at the
    // cap (legal on its own), the continuation pushes the accumulated size
    // past it, surfacing tungstenite's message-capacity error after the
    // client has finished writing — so the 1009 close frame the server sends
    // is reliably delivered (no bytes left in flight to reset the socket).
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let first = Frame::message(
        "a".repeat(MAX_INBOUND_MESSAGE_BYTES).into_bytes(),
        OpCode::Data(Data::Text),
        false,
    );
    let last = Frame::message(vec![b'a'; 1024], OpCode::Data(Data::Continue), true);
    ws.send(Message::Frame(first)).await.expect("send first");
    ws.send(Message::Frame(last)).await.expect("send last");
    let close_code = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) => break None,
                Some(Ok(Message::Close(frame))) => break frame.map(|f| f.code),
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .expect("oversized message must terminate the connection");
    assert_eq!(
        close_code,
        Some(CloseCode::Size),
        "oversized message must be rejected with close code 1009"
    );

    // Over-limit single frame: rejected fast on the frame header (payload is
    // never buffered) and the connection terminates. The client is usually
    // still mid-write, so the 1009 close frame may be lost to the reset —
    // only termination is asserted here.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let oversized = "a".repeat(MAX_INBOUND_MESSAGE_BYTES + 1024);
    let _ = ws.send(Message::Text(oversized.into())).await;
    let closed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "oversized message must terminate the connection"
    );

    // Under-limit but above the 16 MiB tungstenite default frame size: the
    // raised `max_frame_size` must let a single-frame message through to the
    // router (unknown method ⇒ -32601 proves the full round-trip).
    let pad = "a".repeat(20 * 1024 * 1024);
    let large_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"nosuch.method","params":{{"pad":"{pad}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &large_frame).await;
    assert_eq!(
        resp["error"]["code"].as_i64(),
        Some(-32601),
        "20 MiB single-frame message must reach the router: {}",
        resp["error"]
    );

    // A normal-size request on a fresh connection still works.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"client.hello","params":{"clientId":"cli-size"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["clientId"], "cli-size");
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

/// monorepo#564: `agent.sendMessage` to a nonexistent agent id (e.g. a
/// truncated id) fails closed with `-32602` naming the unknown id — it must
/// NOT auto-queue a phantom message (`queued: true`) the sender then waits on
/// forever. A send to a real agent on the same connection still succeeds.
#[tokio::test]
async fn wss_agent_send_message_rejects_unknown_agent() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Send Unknown"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Nonexistent (truncated-style) id → -32602 naming the id, no queueing.
    let ghost = "agent-00000000-0000-0000-0000-000000000000";
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.sendMessage","params":{{"workspaceId":"{ws_id}","agentId":"{ghost}","content":"hello?"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "sendMessage to an unknown agent must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains(ghost),
        "error must name the unknown agent id: {rejected}"
    );

    // No phantom queue entry was created for the ghost id.
    let queue_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getQueue","params":{{"agentId":"{ghost}"}}}}"#
    );
    let queue = wss_call(srv.port, srv.cfg.clone(), &queue_frame).await;
    assert_eq!(
        queue["result"]["queue"].as_array().map(Vec::len),
        Some(0),
        "no phantom queue entry for an unknown agent: {queue}"
    );

    // A send to a REAL agent still succeeds (the store-only fallback path).
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Real Recv"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let send_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.sendMessage","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","content":"hi"}}}}"#
    );
    let sent = wss_call(srv.port, srv.cfg.clone(), &send_frame).await;
    assert_eq!(
        sent["result"]["success"],
        Value::Bool(true),
        "send to a real agent succeeds: {sent}"
    );
    assert_eq!(
        sent["result"]["queued"],
        Value::Bool(false),
        "send to a real agent persists directly: {sent}"
    );

    srv.ws.stop().await;
}

/// monorepo#568: `agent.queueMessage` to a nonexistent agent id fails closed
/// with `-32602` naming the unknown id — it must NOT create a queue entry
/// that never drains. A queue to a real agent on the same connection still
/// succeeds.
#[tokio::test]
async fn wss_agent_queue_message_rejects_unknown_agent() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Queue Unknown"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Nonexistent (truncated-style) id → -32602 naming the id, no queueing.
    let ghost = "agent-00000000-0000-0000-0000-000000000000";
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.queueMessage","params":{{"workspaceId":"{ws_id}","agentId":"{ghost}","content":"hello?"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "queueMessage to an unknown agent must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains(ghost),
        "error must name the unknown agent id: {rejected}"
    );

    // No phantom queue entry was created for the ghost id.
    let queue_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getQueue","params":{{"agentId":"{ghost}"}}}}"#
    );
    let queue = wss_call(srv.port, srv.cfg.clone(), &queue_frame).await;
    assert_eq!(
        queue["result"]["queue"].as_array().map(Vec::len),
        Some(0),
        "no phantom queue entry for an unknown agent: {queue}"
    );

    // A queue to a REAL agent still succeeds.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Real Queue Recv"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let queue_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.queueMessage","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","content":"hi"}}}}"#
    );
    let queued = wss_call(srv.port, srv.cfg.clone(), &queue_frame).await;
    assert_eq!(
        queued["result"]["success"],
        Value::Bool(true),
        "queue to a real agent succeeds: {queued}"
    );
    assert_eq!(
        queued["result"]["queuedMessage"]["content"], "hi",
        "queued entry carries the content: {queued}"
    );

    srv.ws.stop().await;
}

/// `agent.diagnostics` reports real pending-message queue snapshots over the
/// WSS wire: after `agent.queueMessage`, `diagnostics.queues` carries the
/// target's queue (drain-order entries with `queuedAt`, content truncated to
/// 200 chars) and `summary.queuedAgents` counts it; after
/// `agent.removeQueuedMessage` the snapshot is empty again.
#[tokio::test]
async fn wss_agent_diagnostics_reports_queue_snapshots() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Diag Queues"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Diag Queue Target"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Enqueue a >200-char message so the diagnostics preview truncation shows.
    let long_content = "z".repeat(250);
    let queue_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.queueMessage","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","content":"{long_content}"}}}}"#
    );
    let queued = wss_call(srv.port, srv.cfg.clone(), &queue_frame).await;
    assert_eq!(queued["result"]["success"], Value::Bool(true), "{queued}");
    let message_id = queued["result"]["queuedMessage"]["id"]
        .as_str()
        .expect("queued message id")
        .to_string();

    let diag_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.diagnostics","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let diag = wss_call(srv.port, srv.cfg.clone(), &diag_frame).await;
    let d = &diag["result"]["diagnostics"];
    assert_eq!(
        d["summary"]["queuedAgents"],
        Value::from(1),
        "one agent has a pending queue: {d}"
    );
    let queues = d["queues"].as_array().expect("queues array");
    assert_eq!(queues.len(), 1, "one queue snapshot: {d}");
    let q = &queues[0];
    assert_eq!(q["agentId"], Value::String(agent_id.clone()));
    assert_eq!(q["agentName"], Value::String("Diag Queue Target".into()));
    assert_eq!(q["queueLength"], Value::from(1));
    let entries = q["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["id"], Value::String(message_id.clone()));
    assert_eq!(entries[0]["position"], Value::from(0));
    assert!(entries[0]["queuedAt"].is_string(), "queuedAt: {entries:?}");
    let preview = entries[0]["content"].as_str().expect("content string");
    assert_eq!(
        preview.chars().count(),
        201,
        "content truncated to 200 chars plus ellipsis marker"
    );
    assert!(preview.ends_with('…'));
    assert!(long_content.starts_with(preview.trim_end_matches('…')));
    let text = diag["result"]["text"].as_str().expect("text");
    assert!(text.contains("Queued agents: 1"), "text: {text}");
    assert!(text.contains("Pending message queues:"), "text: {text}");

    // Remove the entry — diagnostics returns to an empty snapshot.
    let remove_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.removeQueuedMessage","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","messageId":"{message_id}"}}}}"#
    );
    let removed = wss_call(srv.port, srv.cfg.clone(), &remove_frame).await;
    assert_eq!(removed["result"]["success"], Value::Bool(true), "{removed}");

    let diag_frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.diagnostics","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let diag = wss_call(srv.port, srv.cfg.clone(), &diag_frame).await;
    let d = &diag["result"]["diagnostics"];
    assert_eq!(d["queues"], serde_json::json!([]), "empty after removal");
    assert_eq!(d["summary"]["queuedAgents"], Value::from(0));

    srv.ws.stop().await;
}

/// Unknown providers hard-fail at the front door (PROTOCOL §5.5, §9):
/// `agent.create` with an unknown explicit `provider` or an unknown compound
/// model prefix, and `agent.setModel` with an unknown compound prefix, are all
/// rejected with `-32602` naming the unknown id — no session row is persisted
/// and no default-provider fallback occurs.
#[tokio::test]
async fn wss_agent_create_and_set_model_reject_unknown_provider() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Unknown Provider"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Explicit unknown `provider` param → -32602 naming the unknown id.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad","provider":"nonexistent"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "unknown explicit provider must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.create: unknown provider: nonexistent"),
        "error must name the unknown provider: {rejected}"
    );

    // Unknown compound model prefix → the same -32602.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad2","model":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "unknown compound-prefix provider must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.create: unknown provider: nonexistent"),
        "error must name the unknown provider: {rejected}"
    );

    // Explicit VALID provider + unknown compound model prefix → also -32602
    // (the spawn path gives the model prefix precedence over session.provider).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad3","provider":"auggie","model":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "valid provider + unknown model prefix must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.create: unknown provider: nonexistent"),
        "error must name the unknown provider: {rejected}"
    );

    // No rejection persisted a session row.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    assert_eq!(
        listed["result"]["agents"].as_array().map(Vec::len),
        Some(0),
        "no session row may persist after the rejections: {listed}"
    );

    // A create without a provider still succeeds (defaulting stays valid)…
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Good"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("server-minted id")
        .to_string();
    let model_before = created["result"]["agent"]["model"].clone();

    // …and agent.setModel rejects an unknown compound prefix the same way.
    let set_frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &set_frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "setModel with unknown compound-prefix provider must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.setModel: unknown provider: nonexistent"),
        "error must name the unknown provider: {rejected}"
    );

    // The rejected setModel left the session untouched.
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_eq!(
        got["result"]["agent"]["model"], model_before,
        "model must be unchanged after the rejected setModel: {got}"
    );

    srv.ws.stop().await;
}

/// Regression for monorepo#607 over the real WSS wire: a bare model id whose
/// ownership by the requested provider is disproven by cached catalogs
/// (seeded through the persisted models-cache file) is rejected with the
/// exact `-32602` JSON-RPC error envelope on both `agent.create` (explicit
/// mismatched `provider`) and `agent.setModel` (session's effective
/// provider), no session row / model mutation persists, and a bare id
/// unknown to every cached catalog still passes.
#[tokio::test]
async fn wss_agent_create_and_set_model_reject_bare_model_mismatch() {
    let dir = test_tempdir("intentd-wss-bare-mismatch-");
    // Ownership evidence ignores TTL (fetchedAtMs: 0 is fine): only the
    // version key must match each provider's current one ("" — no pin).
    let cache = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "sonnet4.5", "name": "Sonnet 4.5", "provider": "auggie" } ]
            },
            "grok": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "grok-4-fast", "name": "Grok 4 Fast", "provider": "grok" } ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&cache).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        None,
        Some(dir.path().to_path_buf()),
    )
    .await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Bare Model Mismatch"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Incident-shaped create: explicit `provider: "grok"` + a bare model
    // claimed by auggie's cached catalog and absent from grok's → -32602
    // naming model, provider, and owner.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad","provider":"grok","model":"sonnet4.5"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["jsonrpc"],
        Value::from("2.0"),
        "envelope: {rejected}"
    );
    assert_eq!(rejected["id"], Value::from(2), "envelope: {rejected}");
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "bare-model/provider mismatch must be -32602: {rejected}"
    );
    let msg = rejected["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("agent.create: model sonnet4.5 does not belong to provider grok"),
        "error must name the model and provider: {rejected}"
    );
    assert!(
        msg.contains("auggie"),
        "error must name the owning provider: {rejected}"
    );

    // No session row persisted by the rejection.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    assert_eq!(
        listed["result"]["agents"].as_array().map(Vec::len),
        Some(0),
        "no session row may persist after the rejection: {listed}"
    );

    // A bare id unknown to every cached catalog passes for the same
    // provider (ownership cannot be proven).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Good","provider":"grok","model":"grok-9-experimental"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        created["result"]["agent"]["model"],
        Value::from("grok-9-experimental"),
        "unknown-to-all bare id must pass: {created}"
    );

    // An auggie session (compound-prefix derived provider) rejects a bare
    // grok-claimed model via agent.setModel with the same envelope shape…
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Auggie","model":"auggie:sonnet4.5"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let set_frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"grok-4-fast"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &set_frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "setModel bare-model mismatch must be -32602: {rejected}"
    );
    let msg = rejected["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("agent.setModel: model grok-4-fast does not belong to provider auggie"),
        "error must name the model and provider: {rejected}"
    );
    assert!(
        msg.contains("grok"),
        "error must name the owning provider: {rejected}"
    );

    // …and the rejected setModel left the session's model untouched.
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_eq!(
        got["result"]["agent"]["model"],
        Value::from("auggie:sonnet4.5"),
        "model must be unchanged after the rejected setModel: {got}"
    );

    srv.ws.stop().await;
}

/// `agent.setModel` optional `providerId` param over the real WSS wire
/// (monorepo#1657, PROTOCOL §5.5): a bare model claimed by another
/// provider's cached catalog is rejected -32602 without `providerId` (the
/// message carries the pass-providerId hint), succeeds WITH the owning
/// `providerId` and reconciles the served `provider`, an unknown
/// `providerId` is -32602, a non-string `providerId` is -32602 at the
/// router boundary, and a compound `modelId` conflicting with `providerId`
/// is -32602 with the session untouched. Every response is checked for the
/// JSON-RPC envelope (`jsonrpc: "2.0"` + request-id correlation).
#[tokio::test]
async fn wss_agent_set_model_provider_id_param() {
    /// Assert the JSON-RPC response envelope: version and id correlation.
    fn assert_envelope(resp: &Value, id: i64) {
        assert_eq!(resp["jsonrpc"], Value::from("2.0"), "envelope: {resp}");
        assert_eq!(resp["id"], Value::from(id), "envelope: {resp}");
    }
    let dir = test_tempdir("intentd-wss-setmodel-pid-");
    // Ownership evidence ignores TTL (fetchedAtMs: 0 is fine): only the
    // version key must match each provider's current one ("" — no pin).
    let cache = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "sonnet4.5", "name": "Sonnet 4.5", "provider": "auggie" } ]
            },
            "grok": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "grok-4-fast", "name": "Grok 4 Fast", "provider": "grok" } ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&cache).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        None,
        Some(dir.path().to_path_buf()),
    )
    .await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS SetModel ProviderId"}}"#,
    )
    .await;
    assert_envelope(&created_ws, 1);
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Pid","model":"auggie:sonnet4.5"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&created, 2);
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Without providerId the bare grok-claimed model is rejected against the
    // session's auggie provider — and the message carries the hint.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"grok-4-fast"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&rejected, 3);
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "bare mismatch without providerId must be -32602: {rejected}"
    );
    let msg = rejected["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("pass providerId"),
        "rejection must hint at providerId: {rejected}"
    );

    // Unknown explicit providerId → -32602 before any mutation.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"grok-4-fast","providerId":"nonexistent"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&rejected, 4);
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "unknown providerId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.setModel: unknown provider: nonexistent"),
        "error must name the unknown provider: {rejected}"
    );

    // Compound modelId whose prefix conflicts with providerId → -32602.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"auggie:sonnet4.5","providerId":"grok"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&rejected, 5);
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "conflicting providerId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("names provider auggie but providerId is grok"),
        "error must name both providers: {rejected}"
    );

    // A present non-string providerId is malformed — -32602 at the router
    // boundary, not a silent fall-back to the legacy session-provider path.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"grok-4-fast","providerId":42}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&rejected, 6);
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "non-string providerId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("agent.setModel: providerId must be a string"),
        "error must name the malformed param: {rejected}"
    );

    // All rejections left the session untouched.
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_envelope(&got, 7);
    assert_eq!(
        got["result"]["agent"]["model"],
        Value::from("auggie:sonnet4.5"),
        "model must be unchanged after the rejections: {got}"
    );
    assert_eq!(
        got["result"]["agent"]["provider"],
        Value::from("auggie"),
        "provider must be unchanged after the rejections: {got}"
    );

    // With the owning providerId the same bare model passes, and the served
    // provider reconciles to it.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"grok-4-fast","providerId":"grok"}}}}"#
    );
    let accepted = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&accepted, 8);
    assert!(
        accepted.get("error").is_none(),
        "bare model with owning providerId must pass: {accepted}"
    );
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":9,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_envelope(&got, 9);
    assert_eq!(
        got["result"]["agent"]["model"],
        Value::from("grok-4-fast"),
        "model must be updated: {got}"
    );
    assert_eq!(
        got["result"]["agent"]["provider"],
        Value::from("grok"),
        "provider must reconcile to the explicit providerId: {got}"
    );

    srv.ws.stop().await;
}

/// Regression for monorepo#607 (dynamic gap) over the real WSS wire: with a
/// warm auggie catalog cached (seeded through the persisted models-cache
/// file) that claims the dynamic-only `fable-5` and a grok catalog without
/// it, the exact incident payload — `agent.create` with `provider: "grok"` +
/// bare `model: "fable-5"` — is rejected -32602 naming auggie, and no
/// session row persists. The same id passes when grok has no cached catalog
/// entry (absence of evidence is not a mismatch).
#[tokio::test]
async fn wss_agent_create_rejects_bare_dynamic_model_via_cached_catalog() {
    let dir = test_tempdir("intentd-wss-bare-cache-");
    // Ownership evidence ignores TTL (fetchedAtMs: 0 is fine): only the
    // version key must match each provider's current one ("" — no pin).
    let cache = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "fable-5", "name": "Fable 5", "provider": "auggie" } ]
            },
            "grok": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "grok-4-fast", "name": "Grok 4 Fast", "provider": "grok" } ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&cache).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        None,
        Some(dir.path().to_path_buf()),
    )
    .await;

    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Cached Catalog Guard"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // The exact incident payload: grok + bare auggie dynamic model.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad","provider":"grok","model":"fable-5"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "cached-catalog mismatch must be -32602: {rejected}"
    );
    let msg = rejected["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("agent.create: model fable-5 does not belong to provider grok"),
        "error must name the model and provider: {rejected}"
    );
    assert!(
        msg.contains("auggie"),
        "error must name the owning provider: {rejected}"
    );

    // No session row persisted by the rejection.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    assert_eq!(
        listed["result"]["agents"].as_array().map(Vec::len),
        Some(0),
        "no session row may persist after the rejection: {listed}"
    );

    srv.ws.stop().await;

    // Cold-start counterpart: without any cached catalogs the same payload
    // passes — absence of evidence is not a mismatch (Phase 2 behavior).
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":4,"method":"workspace.create","params":{"title":"WSS Cold Cache"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Cold","provider":"grok","model":"fable-5"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        created["result"]["agent"]["model"],
        Value::from("fable-5"),
        "cold start must pass without cache evidence: {created}"
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

/// `agent.markSeen` (PROTOCOL §5.5, v4.5 seen marker) over the real WSS
/// transport: persists `lastSeenMessageId` in session metadata, emits
/// `agent:updated { agentId, lastSeenMessageId }`, serves the marker on the
/// `agent.get` metadata projection, applies the monotonic no-op when naming
/// an older message, and rejects missing params with `-32602`.
#[tokio::test]
async fn wss_agent_mark_seen_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Seen"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Seed an agent with two persisted transcript rows.
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Seen Agent"}}}}"#
            ),
        ],
    )
    .await;
    let agent_id = sess[0]["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let append = |id: i64, text: &str| {
        format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"agent.appendMessage","params":{{"agentId":"{agent_id}","role":"assistant","contentBlocks":[{{"type":"text","text":"{text}"}}]}}}}"#
        )
    };
    let appended = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![append(3, "first"), append(4, "second")],
    )
    .await;
    let first_id = appended[0]["result"]["message"]["id"]
        .as_str()
        .expect("first message id")
        .to_string();
    let second_id = appended[1]["result"]["message"]["id"]
        .as_str()
        .expect("second message id")
        .to_string();

    // One persistent connection: subscribe first so the `agent:updated`
    // notification from the markSeen below is delivered to this client.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    async fn send_and_wait(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        frame: String,
        id: i64,
    ) -> Value {
        ws.send(Message::Text(frame.into())).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v.get("id") == Some(&serde_json::json!(id)) {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    let sub = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"events.subscribe","params":{{"eventTypes":["agent:updated"],"workspaceId":"{ws_id}"}}}}"#
        ),
        5,
    )
    .await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // Mark the second (newest) message seen: `{ success, lastSeenMessageId }`.
    let marked = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.markSeen","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","messageId":"{second_id}"}}}}"#
        ),
        6,
    )
    .await;
    assert_eq!(marked["result"]["success"], true, "markSeen: {marked}");
    assert_eq!(marked["result"]["lastSeenMessageId"], second_id.as_str());

    // The `agent:updated` event carries the marker (self-sufficient, §6.5).
    let evt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "agent:updated"
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for agent:updated");
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"]["agentId"], agent_id.as_str());
    assert_eq!(evt["data"]["lastSeenMessageId"], second_id.as_str());

    // Served on the `agent.get` metadata projection.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        got["result"]["agent"]["metadata"]["lastSeenMessageId"],
        second_id.as_str(),
        "marker served on the AgentLite metadata projection: {got}"
    );

    // Monotonic: naming the OLDER message is a no-op returning the current
    // marker.
    let older = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"agent.markSeen","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","messageId":"{first_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(older["result"]["success"], true);
    assert_eq!(
        older["result"]["lastSeenMessageId"],
        second_id.as_str(),
        "older markSeen must return the unchanged current marker: {older}"
    );

    // Missing `messageId` → -32602.
    let bad = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"agent.markSeen","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "missing messageId: {bad}");

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

/// `reasoningEffort` wire contract (PROTOCOL §5.5, Option B): settable at
/// `agent.create` (echoed on the returned `AgentLite`), patchable and
/// nullable via `agent.update`, and served on `agent.getSession` /
/// `agent.get` — over the real WSS transport. The daemon stores the level
/// as-is (no vocabulary validation), so an arbitrary string passes.
#[tokio::test]
async fn wss_agent_reasoning_effort_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Effort"}}"#,
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
            // 1) create with reasoningEffort — echoed on the AgentLite result.
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Effort","model":"codex:gpt-5.3-codex","reasoningEffort":"xhigh"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(
        sess[0]["result"]["agent"]["reasoningEffort"].as_str(),
        Some("xhigh"),
        "create echoes reasoningEffort: {}",
        sess[0]
    );
    let agent_id = sess[0]["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            // getSession serves the persisted value.
            format!(
                r#"{{"jsonrpc":"2.0","id":10,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // agent.update patches it (unknown level passes — stored as-is).
            format!(
                r#"{{"jsonrpc":"2.0","id":11,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"reasoningEffort":"ultracode"}}}}}}"#
            ),
            // agent.get (AgentLite) serves the patched value.
            format!(
                r#"{{"jsonrpc":"2.0","id":12,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
            ),
            // null clears it.
            format!(
                r#"{{"jsonrpc":"2.0","id":13,"method":"agent.update","params":{{"agentId":"{agent_id}","changes":{{"reasoningEffort":null}}}}}}"#
            ),
            // Cleared → the key is omitted from the session projection.
            format!(
                r#"{{"jsonrpc":"2.0","id":14,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
            ),
        ],
    )
    .await;
    assert_eq!(
        sess[0]["result"]["session"]["reasoningEffort"].as_str(),
        Some("xhigh"),
        "getSession serves persisted effort: {}",
        sess[0]
    );
    assert_eq!(
        sess[1]["result"]["agent"]["reasoningEffort"].as_str(),
        Some("ultracode"),
        "update result echoes patched effort: {}",
        sess[1]
    );
    assert_eq!(
        sess[2]["result"]["agent"]["reasoningEffort"].as_str(),
        Some("ultracode"),
        "agent.get serves patched effort: {}",
        sess[2]
    );
    assert_eq!(
        sess[3]["result"]["success"],
        Value::Bool(true),
        "null clear succeeds: {}",
        sess[3]
    );
    assert!(
        sess[4]["result"]["session"]
            .as_object()
            .expect("session object")
            .get("reasoningEffort")
            .is_none(),
        "cleared effort is omitted, never null: {}",
        sess[4]
    );

    srv.ws.stop().await;
}

/// `agent.delegate` accepts the additive `reasoningEffort` param (PROTOCOL
/// §5.5 / §5.11) and persists it on the delegated child session, over the real
/// WSS transport. A fresh daemon has no cached model catalog, so there is no
/// `effortLevels` evidence and an arbitrary level passes through unvalidated —
/// the "absence of evidence is not a mismatch" rule; the rejection arm is
/// covered by the service-layer unit test that seeds the catalog cache.
#[tokio::test]
async fn wss_agent_delegate_persists_reasoning_effort() {
    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Delegate effort"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.delegate","params":{{"workspaceId":"{ws_id}","taskText":"do the thing","reasoningEffort":"xhigh"}}}}"#
        )],
    )
    .await;
    assert!(
        sess[0].get("error").is_none(),
        "delegate with reasoningEffort must succeed without catalog evidence: {}",
        sess[0]
    );
    let agent_id = sess[0]["result"]["agentId"]
        .as_str()
        .expect("agentId")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
        )],
    )
    .await;
    assert_eq!(
        sess[0]["result"]["session"]["reasoningEffort"].as_str(),
        Some("xhigh"),
        "delegated child persists the requested effort: {}",
        sess[0]
    );

    srv.ws.stop().await;
}

/// `agent.create` validates `reasoningEffort` against the resolved model's
/// cached `effortLevels` over the real WSS wire (PROTOCOL §5.5), with the same
/// `-32602` valid-values contract as `agent.delegate`/`agent.wakeOrCreate`: an
/// unsupported level is rejected naming the valid values and persists no
/// session row, a supported level matches case-insensitively, and a model with
/// no `effortLevels` evidence passes through unvalidated.
#[tokio::test]
async fn wss_agent_create_validates_reasoning_effort_against_cached_effort_levels() {
    let dir = test_tempdir("intentd-wss-create-effort-");
    let cache = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [
                    { "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                      "effortLevels": ["low", "high"] },
                    { "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie" }
                ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&cache).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        None,
        Some(dir.path().to_path_buf()),
    )
    .await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Create Effort"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad","model":"fable-5","reasoningEffort":"xhigh"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "unsupported effort must be -32602: {rejected}"
    );
    let msg = rejected["error"]["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("agent.create: reasoningEffort xhigh is not supported by model fable-5"),
        "error must name the level and model: {rejected}"
    );
    assert!(
        msg.contains("low, high"),
        "error must name the valid values: {rejected}"
    );

    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    assert_eq!(
        listed["result"]["agents"].as_array().map(Vec::len),
        Some(0),
        "no session row may persist after the rejection: {listed}"
    );

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Good","model":"fable-5","reasoningEffort":"HIGH"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        created["result"]["agent"]["reasoningEffort"],
        Value::from("HIGH"),
        "supported level matches case-insensitively and persists as written: {created}"
    );

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"NoEvidence","model":"sonnet5","reasoningEffort":"xhigh"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        created["result"]["agent"]["reasoningEffort"],
        Value::from("xhigh"),
        "model without effortLevels evidence passes through: {created}"
    );

    srv.ws.stop().await;
}

/// `model.defaultReasoningEffort` as the last rung of the creation-time
/// effort resolution, over the real WSS transport: a no-model `agent.create`
/// whose model resolves from the settings chain pins the configured effort,
/// an explicit `model` (which pins the model itself) suppresses it, and a
/// level the resolved model's cached `effortLevels` does not list is dropped
/// with a warn rather than rejected — settings-chain leniency, so the create
/// still succeeds with the effort omitted from the `AgentLite` payload.
#[tokio::test]
async fn wss_agent_create_applies_settings_default_reasoning_effort() {
    let dir = test_tempdir("intentd-wss-settings-effort-");
    let cache = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [
                    { "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                      "effortLevels": ["low", "high"] }
                ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&cache).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        None,
        Some(dir.path().to_path_buf()),
    )
    .await;
    srv.set_setting("model.default", serde_json::json!("auggie:fable-5"));
    srv.set_setting("model.defaultReasoningEffort", serde_json::json!("high"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Settings Effort"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Settings default"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        created["result"]["agent"]["model"],
        Value::from("auggie:fable-5"),
        "settings default model pinned: {created}"
    );
    assert_eq!(
        created["result"]["agent"]["reasoningEffort"],
        Value::from("high"),
        "settings default effort pinned alongside the settings default model: {created}"
    );

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Explicit model","model":"auggie:fable-5"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert!(
        created["result"]["agent"]
            .as_object()
            .expect("agent object")
            .get("reasoningEffort")
            .is_none(),
        "an explicitly pinned model suppresses the settings default effort: {created}"
    );

    srv.set_setting("model.defaultReasoningEffort", serde_json::json!("xhigh"));
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Unsupported"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert!(
        created.get("error").is_none(),
        "an unsupported settings level must never reject the create: {created}"
    );
    assert!(
        created["result"]["agent"]
            .as_object()
            .expect("agent object")
            .get("reasoningEffort")
            .is_none(),
        "an unsupported settings level is dropped, leaving the effort unset: {created}"
    );

    // Boundary check: a present-but-blank `reasoningEffort` is an explicit
    // clear that must survive the router (`opt_str`, not `opt_nonempty_str`)
    // and suppress the settings default instead of being collapsed to absent.
    srv.set_setting("model.defaultReasoningEffort", serde_json::json!("high"));
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Explicit clear","reasoningEffort":""}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert!(
        created["result"]["agent"]
            .as_object()
            .expect("agent object")
            .get("reasoningEffort")
            .is_none(),
        "a blank `reasoningEffort` is an explicit clear, not a fall-through: {created}"
    );

    srv.ws.stop().await;
}

/// `system.capabilities` (PROTOCOL §5.7): machine-level capabilities with no
/// params and no workspaceId. The result is a plain object whose optional
/// `cowSupported` mirrors the cached workspaces-root CoW probe that fills
/// `Workspace.cowSupported` (§5.1). The harness injects an existing hermetic
/// workspaces root, so the probe always runs and the field is present as a
/// boolean (true on CoW filesystems like APFS, false on e.g. ext4); when the
/// probe cannot run the field is omitted, never null.
#[tokio::test]
async fn wss_system_capabilities_reports_cow_supported() {
    let srv = start(WsOptions::default()).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"system.capabilities","params":{}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let result = resp["result"].as_object().expect("result is an object");
    assert!(
        result.get("cowSupported").is_some_and(Value::is_boolean),
        "cowSupported present as a boolean when the probe ran (hermetic root exists): {resp}"
    );
    srv.ws.stop().await;
}

/// `providers.catalog` (monorepo#928): no params, no workspaceId — the
/// provider registry is compiled-in daemon data. Asserts the documented
/// result shape: one row per `ACP_PROVIDERS` entry in registry order,
/// daemon-evaluated `visible` with the raw gating fields passed through
/// (cortex's feature code always gates — default-deny), and no default
/// designation or tier metadata anywhere in the payload.
#[tokio::test]
async fn wss_providers_catalog_round_trip() {
    let srv = start(WsOptions::default()).await;

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"providers.catalog","params":{}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let result = resp["result"].as_object().expect("result is an object");
    // No privileged provider: the payload carries no defaultProviderId.
    assert!(
        result.get("defaultProviderId").is_none(),
        "catalog must not carry defaultProviderId: {resp}"
    );

    let providers = result["providers"].as_array().expect("providers array");
    let ids: Vec<&str> = providers
        .iter()
        .map(|p| p["id"].as_str().expect("provider id"))
        .collect();
    assert_eq!(
        ids,
        vec![
            "auggie",
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "unsloth",
            "pi",
            "droid",
            "grok",
            "mock"
        ],
        "one row per registry entry, registry order: {resp}"
    );

    // Row shape: required fields present on every row; no per-row default
    // designation or tier metadata.
    for p in providers {
        for field in ["displayName", "shortName", "command"] {
            assert!(p[field].is_string(), "{field} on {}: {resp}", p["id"]);
        }
        for field in ["canBeDisabled", "visible"] {
            assert!(p[field].is_boolean(), "{field} on {}: {resp}", p["id"]);
        }
        assert!(
            p.get("isDefault").is_none(),
            "row must not carry isDefault on {}: {resp}",
            p["id"]
        );
        assert!(
            p.get("modelTiers").is_none(),
            "row must not carry modelTiers on {}: {resp}",
            p["id"]
        );
    }

    // Gating: cortex's feature code always gates (the daemon stores no
    // feature-code enablement — default-deny), with the raw field passed
    // through; ungated providers are visible.
    let cortex = &providers[3];
    assert_eq!(cortex["shortName"], "Cortex");
    assert_eq!(cortex["visible"], Value::Bool(false));
    assert_eq!(cortex["requiresFeatureCode"].as_str(), Some("cortex"));
    let auggie = &providers[0];
    assert_eq!(auggie["shortName"], "Auggie");
    assert_eq!(auggie["visible"], Value::Bool(true));

    // mock's env-var gate passes the raw field through regardless of the
    // daemon environment.
    let mock = &providers[9];
    assert_eq!(
        mock["requiresEnvVar"].as_str(),
        Some("MOCK_AGENT_SCRIPT_PATH")
    );

    srv.ws.stop().await;
}

/// `unsloth.status` / `unsloth.stop` (monorepo#878 follow-up): no params, no
/// workspaceId — the managed Unsloth server is daemon-global. This harness's
/// `Services` is never attached to a real `AgentManager`
/// (`attach_agent_manager` is composition-root-only wiring), so both methods
/// exercise their documented "unattached" degrade path — the same shape a
/// freshly-started daemon reports before any unsloth-provider agent has ever
/// spawned. The daemon-managed server itself needs a real `unsloth` binary
/// and is out of scope for CI; `intent-services`' `unsloth_server` unit tests
/// cover the running-server shapes (status fields, resource sampling, stop
/// terminating the process tree) against a stubbed process.
#[tokio::test]
async fn wss_unsloth_status_and_stop_round_trip() {
    let srv = start(WsOptions::default()).await;

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"unsloth.status","params":{}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    assert_eq!(
        resp["result"],
        serde_json::json!({ "running": false }),
        "no AgentManager attached: unsloth.status degrades to the absent shape: {resp}"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"unsloth.stop","params":{}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 2);
    assert_eq!(
        resp["result"],
        serde_json::json!({ "stopped": false }),
        "no AgentManager attached: unsloth.stop is a no-op: {resp}"
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
    // produced by one router; only the framing differs. The socket lives in
    // the harness TempDir so it is cleaned up with the rest of the fixture.
    let socket = srv._dir.path().join("intentd-wss.sock");
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

    // The list may be empty when no auggie CLI is available (no static
    // fallback catalog remains) — the parity contract is the point here.
    assert!(
        wss_resp["result"]["models"].is_array(),
        "models must be an array: {wss_resp}"
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
    // where `source` is "auggie" (live CLI) or "static" (empty fallback —
    // no static tier catalog remains), and every row present carries the
    // id/name/provider triple.
    let srv = start(WsOptions::default()).await;
    let frame = r#"{"jsonrpc":"2.0","id":7,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 7);
    let models = resp["result"]["models"].as_array().expect("models array");
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
async fn wss_stats_get_usage_round_trip_with_seeded_store() {
    // stats.getUsage: the global usage-stats read behind the agentic
    // usage-stats cards. Seed two hourly buckets straight into the store,
    // then drive both a "month" and a "24h" read over the real WSS path and
    // assert the documented result shape.
    let srv = start(WsOptions::default()).await;

    // The current hour bucket is inside both the current month and the
    // trailing-24h window; a bucket 48h back is outside the 24h window.
    // Bucket keys are the store's RFC-3339 UTC hour floors.
    use chrono::{Datelike, Timelike, Utc};
    let now = Utc::now();
    let bucket_key = |t: chrono::DateTime<Utc>| t.format("%Y-%m-%dT%H:00:00Z").to_string();
    let stamp = |t: chrono::DateTime<Utc>, hour: u8| intent_store::LocalStamp {
        date: t.format("%Y-%m-%d").to_string(),
        hour,
    };
    let bucket_now = bucket_key(now);
    let bucket_old = bucket_key(now - chrono::Duration::hours(48));
    // Stamp the current bucket with a local hour that DIFFERS from its UTC
    // hour (same date, so month filtering is unaffected): the month view
    // must group by the recorded stamp while the 24h view ignores it (D12).
    let divergent_hour = (now.hour() as u8 + 5) % 24;
    let delta = intent_store::UsageStatsDelta {
        input_tokens: 100,
        output_tokens: 40,
        cache_read_tokens: 20,
        cache_creation_tokens: 10,
        runs: 2,
        sessions_started: 1,
        longest_run_ms: 5_000,
        lines_added: 12,
        lines_deleted: 3,
    };
    srv.store
        .add_usage_stats(
            &bucket_now,
            "Opus 4.8",
            "claude-code",
            Some(&stamp(now, divergent_hour)),
            &delta,
        )
        .await
        .expect("seed current bucket");
    let old = now - chrono::Duration::hours(48);
    srv.store
        .add_usage_stats(
            &bucket_old,
            "Sonnet 5",
            "codex",
            Some(&stamp(old, old.hour() as u8)),
            &intent_store::UsageStatsDelta {
                input_tokens: 7,
                runs: 1,
                ..Default::default()
            },
        )
        .await
        .expect("seed old bucket");

    // 24h period: only the current bucket is inside the trailing window.
    let frame = r#"{"jsonrpc":"2.0","id":10,"method":"stats.getUsage","params":{"period":"24h","tzOffsetMinutes":0}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 10);
    assert!(resp.get("error").is_none(), "{resp}");
    let r = &resp["result"];
    assert_eq!(r["totals"]["inputTokens"], 100);
    assert_eq!(r["totals"]["outputTokens"], 40);
    assert_eq!(r["totals"]["cacheReadTokens"], 20);
    assert_eq!(r["totals"]["cacheCreationTokens"], 10);
    assert_eq!(r["runs"], 2);
    assert_eq!(r["sessions"], 1);
    assert_eq!(r["longestRunMs"], 5_000);
    assert_eq!(r["linesAdded"], 12);
    assert_eq!(r["linesDeleted"], 3);
    let by_model = r["byModel"].as_array().expect("byModel");
    assert_eq!(by_model.len(), 1, "{resp}");
    assert_eq!(by_model[0]["model"], "Opus 4.8");
    assert_eq!(by_model[0]["runs"], 2);
    // byProvider mirrors byModel with raw provider ids: only the in-window
    // claude-code bucket shows; the out-of-window codex bucket is excluded.
    let by_provider = r["byProvider"].as_array().expect("byProvider");
    assert_eq!(by_provider.len(), 1, "{resp}");
    assert_eq!(by_provider[0]["provider"], "claude-code");
    assert_eq!(by_provider[0]["runs"], 2);
    assert_eq!(by_provider[0]["inputTokens"], 100);
    assert_eq!(by_provider[0]["outputTokens"], 40);
    assert_eq!(by_provider[0]["cacheReadTokens"], 20);
    assert_eq!(by_provider[0]["cacheCreationTokens"], 10);
    let by_hour = r["byHourOfDay"].as_array().expect("byHourOfDay");
    assert_eq!(by_hour.len(), 24);
    // The seeded bucket occupies exactly one trailing-window slot (the newest
    // one, unless the wall-clock hour ticked mid-test), labelled with the
    // bucket's UTC hour (tzOffsetMinutes is 0) — the divergent local stamp
    // must not affect 24h slotting or labels.
    let seeded: Vec<&Value> = by_hour.iter().filter(|h| h["inputTokens"] == 100).collect();
    assert_eq!(seeded.len(), 1, "{resp}");
    assert_eq!(seeded[0]["hour"], u64::from(now.hour()));
    assert_eq!(r["byMonth"].as_array().expect("byMonth").len(), 12);
    // availablePeriods spans ALL rows, including the out-of-window one.
    let months = r["availablePeriods"]["months"].as_array().expect("months");
    assert!(!months.is_empty(), "{resp}");

    // month period for the current UTC month: the current bucket counts;
    // exact totals depend on whether the 48h-old bucket shares the month, so
    // assert per-model rows instead.
    let month_key = format!("{:04}-{:02}", now.year(), now.month());
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":11,"method":"stats.getUsage","params":{{"period":"month","key":"{month_key}","tzOffsetMinutes":0}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["id"], 11);
    assert!(resp.get("error").is_none(), "{resp}");
    let by_model = resp["result"]["byModel"].as_array().expect("byModel");
    let opus = by_model
        .iter()
        .find(|m| m["model"] == "Opus 4.8")
        .expect("Opus row in current month");
    assert_eq!(opus["inputTokens"], 100);
    assert_eq!(opus["runs"], 2);
    let by_provider = resp["result"]["byProvider"].as_array().expect("byProvider");
    let claude = by_provider
        .iter()
        .find(|p| p["provider"] == "claude-code")
        .expect("claude-code row in current month");
    assert_eq!(claude["inputTokens"], 100);
    assert_eq!(claude["runs"], 2);
    // Month-view hour-of-day grouping follows the recorded local stamp, not
    // the UTC bucket hour (D12).
    let by_hour = resp["result"]["byHourOfDay"]
        .as_array()
        .expect("byHourOfDay");
    assert_eq!(
        by_hour[usize::from(divergent_hour)]["inputTokens"],
        100,
        "{resp}"
    );

    // Malformed params surface as -32602 over the wire.
    let frame =
        r#"{"jsonrpc":"2.0","id":12,"method":"stats.getUsage","params":{"period":"month"}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["error"]["code"], -32602, "{resp}");

    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_stats_get_rate_history_round_trip_with_seeded_store() {
    // stats.getRateHistory (§5.39): the global per-minute token-rate history
    // behind the HUD TOK/MIN chart. Seed minute buckets straight into the
    // store — current minute, two minutes back, and one outside a 5-sample
    // window — then read over the real WSS path and assert the documented
    // zero-filled, chronological result shape.
    let srv = start(WsOptions::default()).await;

    use chrono::{Duration as ChronoDuration, Utc};
    let now = Utc::now();
    let bucket_key = |t: chrono::DateTime<Utc>| t.format("%Y-%m-%dT%H:%M:00Z").to_string();
    srv.store
        .add_usage_rate(
            &bucket_key(now),
            &intent_store::UsageRateDelta {
                input_tokens: 100,
                output_tokens: 40,
                cache_read_tokens: 20,
                cache_creation_tokens: 10,
                thought_tokens: 12,
            },
        )
        .await
        .expect("seed current minute");
    // Second fold into the same bucket must accumulate, not replace.
    srv.store
        .add_usage_rate(
            &bucket_key(now),
            &intent_store::UsageRateDelta {
                input_tokens: 1,
                thought_tokens: 3,
                ..Default::default()
            },
        )
        .await
        .expect("fold current minute");
    srv.store
        .add_usage_rate(
            &bucket_key(now - ChronoDuration::minutes(2)),
            &intent_store::UsageRateDelta {
                input_tokens: 7,
                output_tokens: 2,
                ..Default::default()
            },
        )
        .await
        .expect("seed minute-2 bucket");
    srv.store
        .add_usage_rate(
            &bucket_key(now - ChronoDuration::minutes(30)),
            &intent_store::UsageRateDelta {
                input_tokens: 999,
                ..Default::default()
            },
        )
        .await
        .expect("seed out-of-window bucket");

    let frame = r#"{"jsonrpc":"2.0","id":20,"method":"stats.getRateHistory","params":{"limit":5}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 20);
    assert!(resp.get("error").is_none(), "{resp}");
    let samples = resp["result"]["samples"].as_array().expect("samples");
    assert_eq!(samples.len(), 5, "{resp}");
    // Chronological order, one sample per minute, every field present.
    for s in samples {
        assert!(s["bucketUtc"].is_string(), "{s}");
        assert!(s["inputTokens"].is_u64(), "{s}");
        assert!(s["outputTokens"].is_u64(), "{s}");
        assert!(s["cacheReadTokens"].is_u64(), "{s}");
        assert!(s["cacheCreationTokens"].is_u64(), "{s}");
        assert!(s["thoughtTokens"].is_u64(), "{s}");
    }
    let buckets: Vec<&str> = samples
        .iter()
        .map(|s| s["bucketUtc"].as_str().expect("bucketUtc"))
        .collect();
    let mut sorted = buckets.clone();
    sorted.sort_unstable();
    assert_eq!(buckets, sorted, "samples must be oldest-first: {resp}");
    // The seeded minutes land in their buckets (the newest sample is the
    // current minute unless the wall clock ticked mid-test, so locate rows
    // by bucket key rather than by index).
    let find = |key: &str| samples.iter().find(|s| s["bucketUtc"] == key);
    if let Some(cur) = find(&bucket_key(now)) {
        assert_eq!(cur["inputTokens"], 101, "{resp}");
        assert_eq!(cur["outputTokens"], 40, "{resp}");
        assert_eq!(cur["cacheReadTokens"], 20, "{resp}");
        assert_eq!(cur["cacheCreationTokens"], 10, "{resp}");
        assert_eq!(cur["thoughtTokens"], 15, "{resp}");
    } else {
        panic!("current-minute bucket missing from window: {resp}");
    }
    let mid = find(&bucket_key(now - ChronoDuration::minutes(2)))
        .expect("minute-2 bucket inside the window");
    assert_eq!(mid["inputTokens"], 7, "{resp}");
    assert_eq!(mid["outputTokens"], 2, "{resp}");
    // A minute recorded without any reasoning tokens still carries the field
    // (dense samples), reading back as the additive column's 0 default.
    assert_eq!(mid["thoughtTokens"], 0, "{resp}");
    // Untouched minutes are zero-filled; the 30-minute-old row is outside.
    assert!(
        !samples.iter().any(|s| s["inputTokens"] == 999),
        "out-of-window bucket leaked: {resp}"
    );
    assert!(
        samples.iter().any(|s| s["inputTokens"] == 0),
        "expected zero-filled gap minutes: {resp}"
    );

    // Default limit is 60 samples.
    let frame = r#"{"jsonrpc":"2.0","id":21,"method":"stats.getRateHistory","params":{}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(
        resp["result"]["samples"].as_array().expect("samples").len(),
        60,
        "{resp}"
    );

    // Out-of-range / malformed limits surface as -32602 over the wire.
    let frame = r#"{"jsonrpc":"2.0","id":22,"method":"stats.getRateHistory","params":{"limit":0}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["error"]["code"], -32602, "{resp}");
    let frame =
        r#"{"jsonrpc":"2.0","id":23,"method":"stats.getRateHistory","params":{"limit":1441}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["error"]["code"], -32602, "{resp}");
    let frame =
        r#"{"jsonrpc":"2.0","id":24,"method":"stats.getRateHistory","params":{"limit":"x"}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["error"]["code"], -32602, "{resp}");

    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_models_list_with_provider_id_and_force_refresh() {
    // models.list { providerId, forceRefresh } (§5.30): per-provider catalog
    // through the generic cache. Unknown providers degrade to the empty
    // static fallback (`source: "static"` + warning, never an error); cortex
    // is feature-code gated (empty list + warning under its own source tag).
    let srv = start(WsOptions::default()).await;

    let frame = r#"{"jsonrpc":"2.0","id":8,"method":"models.list","params":{"providerId":"no-such-provider","forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 8);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["providerId"], "no-such-provider");
    assert_eq!(resp["result"]["source"], "static");
    assert!(resp["result"]["models"]
        .as_array()
        .expect("models")
        .is_empty());
    assert!(resp["result"]["warning"].is_string(), "{resp}");
    // Exactly the documented keys — degraded (not stale) data carries no
    // `stale` flag and no extras.
    let mut keys: Vec<_> = resp["result"]
        .as_object()
        .expect("result object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        ["models", "providerId", "source", "warning"],
        "{resp}"
    );

    let frame =
        r#"{"jsonrpc":"2.0","id":9,"method":"models.list","params":{"providerId":"cortex"}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 9);
    assert!(resp.get("error").is_none(), "{resp}");
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
    // Gated empty success is fresh, not stale: same exact key set.
    let mut keys: Vec<_> = resp["result"]
        .as_object()
        .expect("result object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        ["models", "providerId", "source", "warning"],
        "{resp}"
    );

    // Legacy path with only `forceRefresh` (no providerId): still routes and
    // keeps the legacy shape. On a fresh daemon there is no last-good cache
    // entry, so a failed forced probe degrades straight to the empty static
    // fallback — exactly `{ models, source }`, no providerId/stale/warning
    // fields.
    let frame =
        r#"{"jsonrpc":"2.0","id":10,"method":"models.list","params":{"forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 10);
    assert!(resp.get("error").is_none(), "{resp}");
    assert!(resp["result"]["models"].is_array(), "{resp}");
    let source = resp["result"]["source"].as_str().expect("source");
    assert!(source == "auggie" || source == "static", "source: {source}");
    let mut keys: Vec<_> = resp["result"]
        .as_object()
        .expect("result object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(keys, ["models", "source"], "{resp}");

    // A newly registered discovery provider (opencode: native CLI probe).
    // The probe's outcome depends on the host — either branch must be a
    // documented §5.30 shape, never an error: dynamic rows tagged with the
    // provider's own source, or the static fallback + warning when the
    // binary is unavailable.
    let frame = r#"{"jsonrpc":"2.0","id":11,"method":"models.list","params":{"providerId":"opencode","forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 11);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["providerId"], "opencode");
    let models = resp["result"]["models"].as_array().expect("models");
    match resp["result"]["source"].as_str().expect("source") {
        "opencode" => {
            assert!(!models.is_empty(), "{resp}");
            for m in models {
                assert!(m["id"].is_string(), "{m}");
                assert!(m["name"].is_string(), "{m}");
                assert!(m["provider"].is_string(), "{m}");
            }
        }
        "static" => assert!(resp["result"]["warning"].is_string(), "{resp}"),
        other => panic!("unexpected source '{other}': {resp}"),
    }

    // grok (native `grok models` CLI probe): same host-dependent contract —
    // dynamic rows under `source: "grok"`, or the static fallback + warning
    // when the CLI is missing/unauthenticated. Never an error.
    let frame = r#"{"jsonrpc":"2.0","id":12,"method":"models.list","params":{"providerId":"grok","forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 12);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["providerId"], "grok");
    let models = resp["result"]["models"].as_array().expect("models");
    match resp["result"]["source"].as_str().expect("source") {
        "grok" => {
            assert!(!models.is_empty(), "{resp}");
            for m in models {
                assert!(m["id"].is_string(), "{m}");
                assert!(m["name"].is_string(), "{m}");
                assert_eq!(m["provider"], "grok", "{m}");
            }
        }
        "static" => assert!(resp["result"]["warning"].is_string(), "{resp}"),
        other => panic!("unexpected source '{other}': {resp}"),
    }
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_models_list_negative_cache_suppresses_reprobe_force_refresh_bypasses() {
    // models.list legacy path probe guards (§5.30) over the real WSS
    // transport: a failed auggie probe is negatively cached for 60s — a
    // non-forced read within the window serves the static catalog without
    // re-spawning the CLI — while forceRefresh bypasses the negative entry
    // and re-probes. The fake auggie appends to a counter file per
    // invocation and always fails, making CLI spawns observable.
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir("intentd-wss-models-neg-");
    let count = dir.path().join("count");
    let bin = dir.path().join("auggie");
    std::fs::write(
        &bin,
        format!("#!/bin/sh\necho x >> {}\nexit 1\n", count.display()),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    let calls = || {
        std::fs::read_to_string(&count)
            .map(|s| s.lines().count())
            .unwrap_or(0)
    };

    // Cold read: the probe runs (and fails) → empty static fallback, legacy
    // shape.
    let frame = r#"{"jsonrpc":"2.0","id":40,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 40);
    assert_eq!(resp["result"]["source"], "static");
    assert!(resp["result"]["models"]
        .as_array()
        .expect("models")
        .is_empty());
    let after_probe = calls();
    assert!(after_probe > 0, "cold read must spawn the CLI");

    // Within the negative window: same static fallback, no CLI spawn.
    let frame = r#"{"jsonrpc":"2.0","id":41,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 41);
    assert_eq!(resp["result"]["source"], "static");
    assert_eq!(
        calls(),
        after_probe,
        "negative window must suppress the re-probe"
    );

    // forceRefresh bypasses the negative entry and re-probes.
    let frame =
        r#"{"jsonrpc":"2.0","id":42,"method":"models.list","params":{"forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 42);
    assert_eq!(resp["result"]["source"], "static");
    assert!(
        calls() > after_probe,
        "forceRefresh must bypass the negative window and re-probe"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_models_list_legacy_old_entry_served_and_forced_failure_stale() {
    // models.list legacy path contract (§5.30) over the real WSS transport:
    // cached entries are served indefinitely — a NON-forced read whose
    // persisted entry is arbitrarily old (fetchedAtMs: 0) serves it plainly
    // (`{ models, source }`, no `stale`, no `warning`, no probe). A FORCED
    // read whose probe fails serves the same last-good list labeled
    // `stale: true` + `warning` — exactly `{ models, source, stale, warning }`
    // with `source: "auggie"`, never a silent static fallback. The fake
    // auggie appends to a counter file per invocation and always fails,
    // making CLI spawns observable.
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir("intentd-wss-models-stale-");
    let count = dir.path().join("count");
    let bin = dir.path().join("auggie");
    std::fs::write(
        &bin,
        format!("#!/bin/sh\necho x >> {}\nexit 1\n", count.display()),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let calls = || {
        std::fs::read_to_string(&count)
            .map(|s| s.lines().count())
            .unwrap_or(0)
    };
    let last_good = serde_json::json!({
        "version": 1,
        "entries": {
            "auggie": {
                "versionKey": "",
                "fetchedAtMs": 0,
                "models": [ { "id": "lg", "name": "LG", "provider": "auggie" } ]
            }
        }
    });
    std::fs::write(
        dir.path().join("models-cache.json"),
        serde_json::to_vec(&last_good).unwrap(),
    )
    .unwrap();
    let srv = start_with_auggie_and_models_cache(
        WsOptions::default(),
        Some(bin),
        Some(dir.path().to_path_buf()),
    )
    .await;

    // Non-forced: the arbitrarily old persisted entry is a plain cache hit.
    let frame = r#"{"jsonrpc":"2.0","id":45,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 45);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["source"], "auggie");
    assert_eq!(
        resp["result"]["models"],
        serde_json::json!([ { "id": "lg", "name": "LG", "provider": "auggie" } ])
    );
    let mut keys: Vec<_> = resp["result"]
        .as_object()
        .expect("result object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(keys, ["models", "source"], "{resp}");
    assert_eq!(calls(), 0, "non-forced cache hit must not spawn the CLI");

    // Forced: the probe runs, fails, and the last-good list is served stale.
    let frame =
        r#"{"jsonrpc":"2.0","id":46,"method":"models.list","params":{"forceRefresh":true}}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 46);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["source"], "auggie");
    assert_eq!(resp["result"]["stale"], true, "{resp}");
    assert!(resp["result"]["warning"].is_string(), "{resp}");
    assert_eq!(
        resp["result"]["models"],
        serde_json::json!([ { "id": "lg", "name": "LG", "provider": "auggie" } ])
    );
    // Legacy shape plus the documented degradation labels — no providerId.
    let mut keys: Vec<_> = resp["result"]
        .as_object()
        .expect("result object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(keys, ["models", "source", "stale", "warning"], "{resp}");
    assert!(calls() > 0, "forced read must spawn the CLI");
    srv.ws.stop().await;
}

/// Write a deterministic fake `auggie` script for `agent.enhancePrompt` tests
/// (§5.31): swallows the piped stdin, then runs `body`.
#[cfg(unix)]
fn fake_auggie_script(tag: &str, body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir(&format!("intentd-wss-auggie-{tag}-"));
    let bin = dir.path().join("auggie");
    std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    (dir, bin)
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_round_trip() {
    // agent.enhancePrompt (§5.31): `mode: "enhance"` (the default) extracts the
    // `<augment-enhanced-prompt>` payload; `mode: "layout"` returns the full
    // cleaned reply. Both `{ enhanced, original, mode }` shapes ride the same
    // deterministic fixture CLI.
    let (_auggie_dir, bin) = fake_auggie_script(
        "ok",
        "printf '\u{1b}[32m🔧 Tool call: noise\u{1b}[0m\\n🤖\\n<augment-enhanced-prompt>Enhanced: ship it</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    // Provider-neutrality: set auggie as active provider (these operations are auggie-specific).
    srv.set_setting("providers.active", serde_json::json!("auggie"));

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
    let (_auggie_dir, bin) = fake_auggie_script(
        "gated-enhance",
        "printf '🤖\\n<augment-enhanced-prompt>never runs</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("providers.active", serde_json::json!("claude-code"));
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
            "reason": "enhance-prompt requires auggie as the effective default provider"
        })
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_unavailable_when_settings_unset() {
    // Gate closed on unset settings: with neither `model.default` nor
    // `providers.active` configured, the derived default is undecidable and
    // the gate must resolve CLOSED — falling through to the first registered
    // provider would always be auggie and functionally reinstate the removed
    // hardcoded default (coordinator ruling; matches FE #759 where unset
    // resolves disabled).
    let (_auggie_dir, bin) = fake_auggie_script(
        "unset-enhance",
        "printf '🤖\\n<augment-enhanced-prompt>never runs</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":40,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 40);
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "enhance-prompt requires auggie as the effective default provider"
        }),
        "unset provider settings resolve the gate closed, not open via the positional fallback"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_model_default_prefix_outranks_active() {
    // Gate precedence: the effective provider derives from the `model.default`
    // compound prefix FIRST, then `providers.active`. Both directions:
    // a non-auggie prefix closes the gate even with auggie active, and an
    // auggie prefix opens it even with a non-auggie active provider. An
    // unknown prefix is not trusted — it falls through to `providers.active`.
    let (_auggie_dir, bin) = fake_auggie_script(
        "prefix-enhance",
        "printf '🤖\\n<augment-enhanced-prompt>Enhanced: via prefix</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    // Direction 1: claude-code prefix outranks auggie active → gate closes.
    srv.set_setting("providers.active", serde_json::json!("auggie"));
    srv.set_setting("model.default", serde_json::json!("claude-code:sonnet4.5"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":37,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 37);
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "enhance-prompt requires auggie as the effective default provider"
        }),
        "non-auggie model.default prefix outranks auggie providers.active"
    );

    // Direction 2: auggie prefix outranks claude-code active → gate passes.
    srv.set_setting("providers.active", serde_json::json!("claude-code"));
    srv.set_setting("model.default", serde_json::json!("auggie:sonnet4.5"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":38,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 38);
    assert_eq!(
        resp["result"]["enhanced"], "Enhanced: via prefix",
        "auggie model.default prefix outranks non-auggie providers.active"
    );

    // Unknown prefix: falls through to providers.active (auggie) → gate passes.
    srv.set_setting("providers.active", serde_json::json!("auggie"));
    srv.set_setting("model.default", serde_json::json!("typo:foo"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":39,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 39);
    assert_eq!(
        resp["result"]["enhanced"], "Enhanced: via prefix",
        "unknown model.default prefix falls through to providers.active"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_enhance_prompt_parse_failure_is_internal_error() {
    // A reply without the `<augment-enhanced-prompt>` tags in enhance mode is
    // the documented -32603 parse failure (§5.31).
    let (_auggie_dir, bin) = fake_auggie_script("notags", "printf '🤖\\nno tags here\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("providers.active", serde_json::json!("auggie"));
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
    srv.set_setting("providers.active", serde_json::json!("auggie"));
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
    let (_auggie_dir, bin) = fake_auggie_script(
        "complete-ok",
        "printf '\u{1b}[32m🔧 Tool call: noise\u{1b}[0m\\n🤖\\nfix-login-flow\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("providers.active", serde_json::json!("auggie"));

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
    let (_auggie_dir, bin) = fake_auggie_script("gated-complete", "printf '🤖\\nnever-runs\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("providers.active", serde_json::json!("claude-code"));
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
            "reason": "completeOnce requires auggie as the effective default provider"
        })
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_unavailable_when_settings_unset() {
    // Gate closed on unset settings — mirror of the enhance-prompt test.
    let (_auggie_dir, bin) = fake_auggie_script("unset-complete", "printf '🤖\\nnever-runs\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":47,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 47);
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "completeOnce requires auggie as the effective default provider"
        }),
        "unset provider settings resolve the gate closed, not open via the positional fallback"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_model_default_prefix_outranks_active() {
    // Gate precedence mirror of the enhance-prompt test: `model.default`
    // compound prefix outranks `providers.active` in both directions.
    let (_auggie_dir, bin) = fake_auggie_script("prefix-complete", "printf '🤖\\nvia-prefix\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    // Direction 1: claude-code prefix outranks auggie active → gate closes.
    srv.set_setting("providers.active", serde_json::json!("auggie"));
    srv.set_setting("model.default", serde_json::json!("claude-code:sonnet4.5"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":45,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 45);
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "completeOnce requires auggie as the effective default provider"
        }),
        "non-auggie model.default prefix outranks auggie providers.active"
    );

    // Direction 2: auggie prefix outranks claude-code active → gate passes.
    srv.set_setting("providers.active", serde_json::json!("claude-code"));
    srv.set_setting("model.default", serde_json::json!("auggie:sonnet4.5"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":46,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 46);
    assert_eq!(
        resp["result"]["text"], "via-prefix",
        "auggie model.default prefix outranks non-auggie providers.active"
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
    srv.set_setting("providers.active", serde_json::json!("auggie"));
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
    let (_auggie_dir, bin) = fake_auggie_script("complete-slow", "sleep 30");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("providers.active", serde_json::json!("auggie"));
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
    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
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
    let (api, bus, _store, _registry, _dir) = make_services(None, None).await;
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
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#.into(),
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
    // Verifies fixed-port restart semantics: a graceful `stop()` fully releases
    // the listen port (it awaits the accept loop) so the SAME listener can
    // immediately rebind it. `free_port()` hands back a port from the kernel's
    // ephemeral range, so under parallel test load another process's
    // `base_port: 0` bind can be assigned that port inside either
    // release->bind window (before the first start, or between stop and
    // restart); the listener is single-bind fail-fast by design (§5.6, no port
    // walking), so that exogenous contention surfaces as `AddrInUse`. Retry the
    // whole scenario on a fresh port within a bounded number of attempts
    // (monorepo#466); any non-`AddrInUse` error still fails immediately.
    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    const MAX_ATTEMPTS: u32 = 10;
    for _ in 0..MAX_ATTEMPTS {
        let fixed_port = free_port();
        let opts = WsOptions {
            base_port: fixed_port,
            bind_address: Ipv4Addr::LOCALHOST.into(),
            ..WsOptions::default()
        };
        let ws = WsApiServer::new(
            api.clone(),
            bus.clone(),
            &tls,
            token_store.clone(),
            opts,
            None,
        )
        .expect("server");
        let port = match ws.start().await {
            Ok(port) => port,
            // A concurrent ephemeral bind stole the port before our first
            // bind; the scenario never started, so retry on a fresh port.
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(e) => panic!("first start failed with non-contention error: {e}"),
        };
        assert_eq!(port, fixed_port, "fixed-port bind honours base_port");
        ws.stop().await;
        // Re-start the SAME listener immediately; the freed port must rebind.
        match ws.start().await {
            Ok(again) => {
                assert_eq!(again, port, "restart should reclaim the same port");
                ws.stop().await;
                return;
            }
            // stop() released the port but an exogenous bind grabbed it in
            // the stop->restart gap; retry on a fresh port.
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(e) => panic!("restart failed with non-contention error: {e}"),
        }
    }
    panic!(
        "gave up after {MAX_ATTEMPTS} attempts: every scenario lost its port to \
         concurrent ephemeral binds"
    );
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
        status_image_asset_id: None,
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
        display_status: None,
        checkout_mode: None,
        disk_usage: None,
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

/// `workspace.update` with the clearable `statusImageAssetId` field
/// (intent-hq/monorepo#997 part 1) over the real WSS wire: setting an asset id
/// persists it, surfaces it on `workspace.get`, and emits a self-sufficient
/// `workspace:updated` event whose `changes` delta carries the new value;
/// a wire `null` clears the stored id (and the cleared field is omitted from
/// the returned `Workspace` payload per `skip_serializing_if`).
#[tokio::test]
async fn wss_workspace_update_status_image_asset_id_round_trip() {
    let srv = start(WsOptions::default()).await;

    let ws_id = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws_id))
        .await
        .expect("insert workspace");

    // One persistent connection: subscribe first so the `workspace:updated`
    // notification from the mutation below is delivered to this client.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let rpc = |id: i64, method: &str, params: Value| {
        serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            .to_string()
    };
    async fn send_and_wait(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        frame: String,
        id: i64,
    ) -> Value {
        ws.send(Message::Text(frame.into())).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v.get("id") == Some(&serde_json::json!(id)) {
                        return v;
                    }
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }

    let sub = send_and_wait(
        &mut ws,
        rpc(
            1,
            "events.subscribe",
            serde_json::json!({
                "eventTypes": ["workspace:updated"],
                "workspaceId": ws_id.as_str(),
            }),
        ),
        1,
    )
    .await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // Set: camelCase wire field lands on the row and echoes in the response.
    let resp = send_and_wait(
        &mut ws,
        rpc(
            2,
            "workspace.update",
            serde_json::json!({
                "workspaceId": ws_id.as_str(),
                "statusImageAssetId": "asset-abc123",
            }),
        ),
        2,
    )
    .await;
    assert!(resp.get("error").is_none(), "update errored: {resp}");
    assert_eq!(
        resp["result"]["workspace"]["statusImageAssetId"], "asset-abc123",
        "response workspace carries the new asset id: {resp}"
    );

    // The `workspace:updated` event's `changes` delta is self-sufficient
    // (§6.5): subscribers see the new asset id without a follow-up read.
    let evt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "workspace:updated"
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for workspace:updated");
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"]["changes"]["statusImageAssetId"], "asset-abc123",
        "event delta carries the asset id: {evt}"
    );

    // Read-back proves persistence through the store.
    let got = send_and_wait(
        &mut ws,
        rpc(
            3,
            "workspace.get",
            serde_json::json!({ "workspaceId": ws_id.as_str() }),
        ),
        3,
    )
    .await;
    assert_eq!(
        got["result"]["workspace"]["statusImageAssetId"],
        "asset-abc123"
    );

    // Clear: wire `null` (double-option `Some(None)`) empties the column and
    // the cleared field is omitted from the returned payload.
    let cleared = send_and_wait(
        &mut ws,
        rpc(
            4,
            "workspace.update",
            serde_json::json!({
                "workspaceId": ws_id.as_str(),
                "statusImageAssetId": Value::Null,
            }),
        ),
        4,
    )
    .await;
    assert!(cleared.get("error").is_none(), "clear errored: {cleared}");
    // `skip_serializing_if` contract: the cleared field must be OMITTED from
    // the payload, not serialized as an explicit `null` (index-based `is_null`
    // can't tell the two apart, `get` can).
    assert!(
        cleared["result"]["workspace"]
            .as_object()
            .expect("workspace object")
            .get("statusImageAssetId")
            .is_none(),
        "cleared asset id must be omitted, not null: {cleared}"
    );
    let got = send_and_wait(
        &mut ws,
        rpc(
            5,
            "workspace.get",
            serde_json::json!({ "workspaceId": ws_id.as_str() }),
        ),
        5,
    )
    .await;
    assert!(
        got["result"]["workspace"]
            .as_object()
            .expect("workspace object")
            .get("statusImageAssetId")
            .is_none(),
        "clear persists as an omitted field: {got}"
    );

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
    let repo_dir = test_tempdir("intentd-wssgit-");
    let repo = repo_dir.path().to_path_buf();
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
}

/// `git.diffs` with the §5.6 `paths` narrowing param over WSS: the daemon
/// prunes the unstaged walk to exactly the requested workspace-relative files,
/// the legacy single `path` unions with `paths`, an absolute path under the
/// worktree is normalized to its relative form (same narrowed result), and an
/// absent/empty `paths` keeps the full-tree behavior.
#[tokio::test]
async fn wss_git_diffs_paths_narrowing_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a repo with one commit, then two tracked edits + one untracked file.
    let repo_dir = test_tempdir("intentd-wssdiffs-");
    let repo = repo_dir.path().to_path_buf();
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
    std::fs::write(repo.join("a.txt"), "a\n").unwrap();
    std::fs::write(repo.join("b.txt"), "b\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "seed"]);
    std::fs::write(repo.join("a.txt"), "a\nchanged\n").unwrap();
    std::fs::write(repo.join("b.txt"), "b\nchanged\n").unwrap();
    std::fs::write(repo.join("c.txt"), "new\n").unwrap();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS diffs WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // paths narrows to exactly the requested files (tracked edit + untracked).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":["a.txt","c.txt"]}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 2);
    let arr = resp["result"].as_array().expect("diffs array");
    let paths: Vec<&str> = arr.iter().map(|d| d["path"].as_str().unwrap()).collect();
    assert_eq!(arr.len(), 2, "exactly the requested files: {paths:?}");
    assert!(paths.contains(&"a.txt"));
    assert!(paths.contains(&"c.txt"));
    let a = arr.iter().find(|d| d["path"] == "a.txt").unwrap();
    let lines = a["hunks"][0]["lines"].as_array().expect("hunk lines");
    assert!(lines.iter().any(
        |l| l["type"] == "Addition" && l["content"].as_str().unwrap_or("").contains("changed")
    ));

    // Legacy single `path` unions with `paths`.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":["a.txt"],"path":"b.txt"}}}}"#
        ),
    )
    .await;
    let arr = resp["result"].as_array().expect("diffs array");
    let paths: Vec<&str> = arr.iter().map(|d| d["path"].as_str().unwrap()).collect();
    assert_eq!(arr.len(), 2, "union of paths + path: {paths:?}");
    assert!(paths.contains(&"a.txt"));
    assert!(paths.contains(&"b.txt"));

    // Defense-in-depth normalization: an absolute path under the worktree
    // returns the same narrowed result as the relative form (result `path`
    // values stay worktree-relative), and an absolute path outside the
    // worktree matches nothing.
    let relative = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":["a.txt"]}}}}"#
        ),
    )
    .await;
    let absolute = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":["{}"]}}}}"#,
            repo.join("a.txt").display()
        ),
    )
    .await;
    assert_eq!(
        absolute["result"], relative["result"],
        "absolute form narrows like relative"
    );
    let arr = absolute["result"].as_array().expect("diffs array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["path"], "a.txt", "result path stays relative");
    let outside = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":["/no/such/root/a.txt"]}}}}"#
        ),
    )
    .await;
    assert_eq!(outside["result"], serde_json::json!([]));

    // An empty `paths` array keeps the full-tree behavior.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":[]}}}}"#
        ),
    )
    .await;
    let arr = resp["result"].as_array().expect("diffs array");
    let paths: Vec<&str> = arr.iter().map(|d| d["path"].as_str().unwrap()).collect();
    assert_eq!(arr.len(), 3, "full tree: {paths:?}");
    for p in ["a.txt", "b.txt", "c.txt"] {
        assert!(paths.contains(&p), "missing {p} in {paths:?}");
    }

    // Malformed `paths` (non-array, or array with a non-string element) → -32602.
    for bad in [r#""a.txt""#, "[1]"] {
        let resp = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","paths":{bad}}}}}"#
            ),
        )
        .await;
        assert_eq!(resp["error"]["code"], -32602, "paths={bad}");
    }

    srv.ws.stop().await;
}

/// `accept-changes.getStatus` over WSS: proves the wire shape from PROTOCOL
/// §5.18 — `localCommits` entries are metadata-only (`hash`, `message`,
/// `author`, `date`, `isPushed`) and omit `files`/`filesChanged`, which
/// clients fetch on demand via `git.commitDetails`.
#[tokio::test]
async fn wss_accept_changes_get_status_local_commits_are_metadata_only() {
    let srv = start(WsOptions::default()).await;

    // Seed a repo: one commit on main, then a feature branch with one commit.
    let repo_dir = test_tempdir("intentd-wssacgs-");
    let repo = repo_dir.path().to_path_buf();
    let git = |args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q", "-b", "main"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "seed"]);
    git(&["checkout", "-q", "-b", "feature/wss"]);
    std::fs::write(repo.join("feat.txt"), "feat\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "add feat"]);

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS AC WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"accept-changes.getStatus","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let result = &resp["result"];
    assert_eq!(result["branch"], "feature/wss");
    assert_eq!(result["trunkBranch"], "main");
    assert_eq!(result["hasRemote"], false);
    assert_eq!(result["aheadOfTrunk"], 1);
    assert_eq!(result["uncommittedCount"], 0);
    assert_eq!(result["stagedCount"], 0);
    let commits = result["localCommits"].as_array().expect("localCommits");
    assert_eq!(commits.len(), 1);
    let c = &commits[0];
    assert!(c["hash"].is_string());
    assert_eq!(c["message"], "add feat");
    assert_eq!(c["author"], "Test");
    assert!(c["date"].is_string());
    assert_eq!(c["isPushed"], false);
    // Metadata-only walk: no per-commit tree diffs in getStatus.
    assert!(c.get("files").is_none());
    assert!(c.get("filesChanged").is_none());

    srv.ws.stop().await;
}

/// `file-tracking.loadCommits` with workspace boundary over WSS: proves the
/// daemon returns `boundarySha` and bounds commits to `boundary..HEAD`, and
/// the `includeOlder` parameter fetches pre-boundary commits.
#[tokio::test]
async fn wss_file_tracking_load_commits_bounded() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with a base commit on main + workspace commit on a branch.
    let repo_dir = test_tempdir("intentd-wssftlc-");
    let repo = repo_dir.path().to_path_buf();
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
    // Metadata-only list payload: `files`/`filesChanged` are omitted (no
    // per-commit tree diff); clients fetch details via git.commitDetails.
    assert!(commits[0].get("files").is_none(), "files omitted: {resp}");
    assert!(
        commits[0].get("filesChanged").is_none(),
        "filesChanged omitted: {resp}"
    );

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
    let repo_dir = test_tempdir("intentd-wssbs-");
    let repo = repo_dir.path().to_path_buf();
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
    let unreg_dir = test_tempdir("intentd-wssgb-");
    let unreg = unreg_dir.path().to_path_buf();
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
    let plain_dir = test_tempdir("intentd-wsspl-");
    let plain = plain_dir.path().to_path_buf();
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
}

/// `git.pull` over WSS — the workspace-create auto-pull seam (§5.6).
/// Path-based like `git.getBranches`: the repo is never registered as a
/// workspace. Drives the checked-out fast-forward pull (`{ ok: true }`), the
/// structured `{ ok: false, error }` failure for a repo without a remote, and
/// the nonexistent-path -32602.
#[tokio::test]
async fn wss_git_pull_round_trip() {
    let srv = start(WsOptions::default()).await;

    let base_dir = test_tempdir("intentd-wsspull-");
    let base = base_dir.path().to_path_buf();
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

/// Regression for monorepo#721 over the real WSS wire: full-content note
/// writes (`note.setContent`) with non-ASCII content (emoji/CJK) must not
/// corrupt content or panic the daemon. The CRDT merge engine computes
/// UTF-16 code-unit offsets, but the `yrs` doc used byte offsets — so the
/// second full-content write (the diff path over a doc already holding
/// multi-byte chars) landed at wrong byte positions, panicking inside `yrs`
/// and poisoning the sessions mutex (every later CRDT call then panicked,
/// dropping the WSS connection). Post-fix: setContent with emoji/CJK, an
/// edited setContent (diff path), a surgical `note.add`, and a reseeded
/// setContent after the add all succeed with the expected merged content.
#[tokio::test]
async fn wss_note_set_content_non_ascii_merge_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Unicode Merge"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"note.create","params":{{"workspaceId":"{ws_id}","title":"Unicode","content":"ascii seed"}}}}"#
        )],
    )
    .await;
    let note_id = sess[0]["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // v1 introduces emoji + CJK; v2 edits between multi-byte regions (common
    // prefix ends on the emoji, common suffix starts mid-CJK) so the diff
    // offsets are only correct under UTF-16 offset kind — this exact frame
    // panicked the pre-fix daemon and dropped the connection.
    let v1 = "Intro 😀 中文段落 emoji tail ✅";
    let v2 = "Intro 😀🎉 中文段落改写 emoji tail ✅";
    let appended = "尾注 with emoji 🚀";
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"note.setContent",
                "params":{"workspaceId":ws_id,"noteId":note_id,"content":v1}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":4,"method":"note.get",
                "params":{"workspaceId":ws_id,"noteId":note_id}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":5,"method":"note.setContent",
                "params":{"workspaceId":ws_id,"noteId":note_id,"content":v2}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":6,"method":"note.get",
                "params":{"workspaceId":ws_id,"noteId":note_id}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":7,"method":"note.add",
                "params":{"workspaceId":ws_id,"noteId":note_id,"content":appended,"position":"end"}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":8,"method":"note.get",
                "params":{"workspaceId":ws_id,"noteId":note_id}})
            .to_string(),
        ],
    )
    .await;
    for (i, resp) in sess.iter().enumerate() {
        assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
        assert_eq!(resp["id"].as_i64(), Some(i as i64 + 3), "envelope: {resp}");
        assert!(
            resp.get("error").is_none(),
            "all frames must be success envelopes: {resp}"
        );
    }
    assert_eq!(sess[0]["result"]["ok"], true, "setContent v1: {}", sess[0]);
    assert_eq!(
        sess[1]["result"]["note"]["content"], v1,
        "v1 persisted verbatim: {}",
        sess[1]
    );
    assert_eq!(
        sess[2]["result"]["ok"], true,
        "setContent v2 (diff path over multi-byte doc): {}",
        sess[2]
    );
    assert_eq!(
        sess[3]["result"]["note"]["content"], v2,
        "v2 merged without corruption: {}",
        sess[3]
    );
    assert_eq!(sess[4]["result"]["ok"], true, "note.add: {}", sess[4]);
    let after_add = sess[5]["result"]["note"]["content"]
        .as_str()
        .expect("content after add")
        .to_string();
    assert!(
        after_add.starts_with(v2),
        "surgical add preserves the merged v2 prefix: {after_add}"
    );
    assert!(
        after_add.contains(appended),
        "surgical add appends the non-ASCII content: {after_add}"
    );

    // The surgical add invalidated the CRDT session; a follow-up full-content
    // write reseeds from the persisted content and must again merge a
    // multi-byte edit correctly.
    let v3 = after_add.replace("中文段落改写", "中文段落终稿");
    assert_ne!(v3, after_add, "fixture edit must change the content");
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &serde_json::json!({"jsonrpc":"2.0","id":9,"method":"note.setContent",
            "params":{"workspaceId":ws_id,"noteId":note_id,"content":v3}})
        .to_string(),
    )
    .await;
    assert_eq!(
        resp["result"]["ok"], true,
        "reseeded setContent after surgical add: {resp}"
    );
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &serde_json::json!({"jsonrpc":"2.0","id":10,"method":"note.get",
            "params":{"workspaceId":ws_id,"noteId":note_id}})
        .to_string(),
    )
    .await;
    assert_eq!(
        resp["result"]["note"]["content"].as_str(),
        Some(v3.as_str()),
        "v3 merged from the reseeded session: {resp}"
    );

    srv.ws.stop().await;
}

/// `git.showFile` over WSS (PROTOCOL §5.6 extensions): file content at a
/// revision (`HEAD` / `HEAD^`), the empty-content fallback for a path missing
/// at the ref, and -32603 for an unresolvable ref.
#[tokio::test]
async fn wss_git_show_file_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed a real git repo with two commits so HEAD and HEAD^ differ.
    let repo_dir = test_tempdir("intentd-wsssf-");
    let repo = repo_dir.path().to_path_buf();
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
    ws.send(Message::Text(call.to_string().into()))
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
                    ws.send(Message::Text(reply.to_string().into()))
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

/// monorepo#958 — the bounded agent read paths over the real WSS transport:
/// `agent.list` / `agent.get` (metadata + last-rows projection), a full
/// `agent.getConversation` multi-page `nextToken` walk plus the
/// `aroundMessageId` seek (centered page, `prevToken` walk newer, `-32602`
/// on an unknown id), and the `chat.subscribe` seq-0 snapshot, all against
/// one seeded 120-message session. Then the hydration regression at the wire
/// level: with every row OLDER than the newest bounded page corrupted to
/// non-JSON (which errors any path that decodes it — `agent.getSession`
/// demonstrates), the bounded reads still answer correctly, proving they
/// never fetch/decode beyond their page.
#[tokio::test]
async fn wss_agent_read_paths_bounded_pagination_round_trip() {
    use intent_core::AgentId;
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Paged"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Paged"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // Seed a 120-message transcript — well past the 50-message default page.
    for i in 0..120 {
        let (role, text) = if i % 2 == 0 {
            ("user", format!("prompt {i}"))
        } else {
            ("assistant", format!("reply {i}"))
        };
        srv.store
            .append_agent_message(
                &agent,
                role,
                &json!([{ "type": "text", "text": text }]),
                &now_iso(),
            )
            .await
            .expect("append message");
    }

    // agent.list — `{ agents: [AgentLite] }`: aggregate `messageCount` plus the
    // newest user/assistant projections, no `messages` array (PROTOCOL §5.5).
    let list = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(list["jsonrpc"], "2.0");
    assert_eq!(list["id"], 3);
    let agents = list["result"]["agents"].as_array().expect("agents array");
    assert_eq!(agents.len(), 1);
    let lite = &agents[0];
    assert_eq!(lite["id"].as_str(), Some(agent_id.as_str()));
    assert_eq!(lite["messageCount"], 120);
    assert_eq!(lite["lastUserMessage"].as_str(), Some("prompt 118"));
    assert_eq!(lite["lastAgentResponse"].as_str(), Some("reply 119"));
    assert_eq!(
        lite["lastMessageRole"].as_str(),
        Some("assistant"),
        "newest seeded message is the assistant reply: {lite}"
    );
    assert!(
        lite.get("messages").is_none(),
        "AgentLite carries no transcript: {lite}"
    );

    // lastMessageRole on the wire for the awaiting-reply shape: a second
    // agent whose only message is the user's serves "user"; a fresh agent
    // with no messages omits the field entirely.
    let created2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":30,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"AwaitingReply"}}}}"#
        ),
    )
    .await;
    let agent2_id = created2["result"]["agent"]["id"]
        .as_str()
        .expect("agent2 id")
        .to_string();
    assert!(
        created2["result"]["agent"].get("lastMessageRole").is_none(),
        "no messages yet: field omitted: {created2}"
    );
    srv.store
        .append_agent_message(
            &AgentId::from(agent2_id.as_str()),
            "user",
            &json!([{ "type": "text", "text": "no reply yet" }]),
            &now_iso(),
        )
        .await
        .expect("append user message");
    let got2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":31,"method":"agent.get","params":{{"agentId":"{agent2_id}","workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let lite2 = &got2["result"]["agent"];
    assert_eq!(
        lite2["lastMessageRole"].as_str(),
        Some("user"),
        "user message with no assistant reply serves \"user\": {lite2}"
    );
    assert!(
        lite2.get("lastAgentResponse").is_none(),
        "no assistant reply yet: {lite2}"
    );

    // agent.get — `{ agent: AgentLite }`, byte-identical to the list entry.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.get","params":{{"agentId":"{agent_id}","workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        got["result"]["agent"], *lite,
        "agent.get == agent.list entry"
    );

    // agent.getConversation — full multi-page walk. Default page (no limit) is
    // the newest 50 (seq 70..=119, oldest→newest within the page); each
    // `nextToken` steps one page older; the oldest page is short (20) with
    // `truncated: false` and a null `nextToken`.
    let mut token: Option<String> = None;
    let mut pages: Vec<Vec<i64>> = Vec::new();
    let mut rpc_id = 5;
    loop {
        let params = match &token {
            Some(t) => format!(r#"{{"agentId":"{agent_id}","nextToken":"{t}"}}"#),
            None => format!(r#"{{"agentId":"{agent_id}"}}"#),
        };
        let resp = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":{rpc_id},"method":"agent.getConversation","params":{params}}}"#
            ),
        )
        .await;
        rpc_id += 1;
        let result = &resp["result"];
        assert_eq!(result["agentId"].as_str(), Some(agent_id.as_str()));
        assert_eq!(result["totalMessages"], 120);
        let msgs = result["messages"].as_array().expect("messages array");
        let seqs: Vec<i64> = msgs.iter().map(|m| m["seq"].as_i64().unwrap()).collect();
        pages.push(seqs);
        token = result["nextToken"].as_str().map(str::to_string);
        assert_eq!(
            result["truncated"].as_bool(),
            Some(token.is_some()),
            "truncated iff a nextToken remains: {result}"
        );
        if token.is_none() {
            break;
        }
        assert!(pages.len() < 10, "token walk must terminate");
    }
    assert_eq!(pages.len(), 3, "120 messages @ default 50 → 3 pages");
    assert_eq!(pages[0], (70..=119).collect::<Vec<i64>>());
    assert_eq!(pages[1], (20..=69).collect::<Vec<i64>>());
    assert_eq!(pages[2], (0..=19).collect::<Vec<i64>>());

    // Explicit `limit` is honored: the newest 10 only.
    let limited = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":20,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10}}}}"#
        ),
    )
    .await;
    let msgs = limited["result"]["messages"].as_array().expect("messages");
    let seqs: Vec<i64> = msgs.iter().map(|m| m["seq"].as_i64().unwrap()).collect();
    assert_eq!(seqs, (110..=119).collect::<Vec<i64>>());
    assert_eq!(limited["result"]["truncated"], true);
    assert!(
        limited["result"].get("prevToken").is_none(),
        "seek-free responses carry no prevToken key: {}",
        limited["result"]
    );

    // agent.getConversation seek (§5.5): `aroundMessageId` returns the page
    // containing the target with the standard backward `nextToken` plus a
    // `prevToken` walking newer toward the live tail.
    let target_id = srv
        .store
        .get_agent_messages_page(&agent, 60, 1)
        .await
        .expect("target row")
        .pop()
        .expect("seq 60 exists")
        .id;
    let seek = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":21,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"aroundMessageId":"{target_id}"}}}}"#
        ),
    )
    .await;
    let result = &seek["result"];
    let seqs: Vec<i64> = result["messages"]
        .as_array()
        .expect("seek messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(
        seqs,
        (55..=64).collect::<Vec<i64>>(),
        "half the budget older than seq 60, the rest at/after: {result}"
    );
    let seek_next = result["nextToken"].as_str().expect("older cursor");
    let seek_prev = result["prevToken"].as_str().expect("newer cursor");

    // prevToken pages newer (seq 65..=74); nextToken pages older (45..=54).
    let newer = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":22,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"nextToken":"{seek_prev}"}}}}"#
        ),
    )
    .await;
    let seqs: Vec<i64> = newer["result"]["messages"]
        .as_array()
        .expect("newer messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, (65..=74).collect::<Vec<i64>>());
    assert!(
        newer["result"]["prevToken"].is_string(),
        "newer rows remain toward the tail: {}",
        newer["result"]
    );
    let older = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":23,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"nextToken":"{seek_next}"}}}}"#
        ),
    )
    .await;
    let seqs: Vec<i64> = older["result"]["messages"]
        .as_array()
        .expect("older messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, (45..=54).collect::<Vec<i64>>());

    // Unknown aroundMessageId → -32602 naming the id (PROTOCOL §9).
    let bad_seek = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":24,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","aroundMessageId":"msg-nope"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad_seek["error"]["code"], -32602);
    assert!(
        bad_seek["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("msg-nope"),
        "error names the unknown id: {bad_seek}"
    );

    // chat.subscribe — the seq-0 snapshot over WSS is the bounded newest
    // `agent.getConversation` page (PROTOCOL §7.1), not the full history.
    let mut sub = connect_ws(srv.port, srv.cfg.clone()).await;
    sub.send(Message::Text(
        format!(
            r#"{{"jsonrpc":"2.0","id":21,"method":"chat.subscribe","params":{{"agentId":"{agent_id}"}}}}"#
        )
        .into(),
    ))
    .await
    .expect("send subscribe");
    let mut sub_resp: Option<Value> = None;
    let mut snap: Option<Value> = None;
    while sub_resp.is_none() || snap.is_none() {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), sub.next())
            .await
            .expect("chat.subscribe frame timed out");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "subscription.push" {
                    snap = Some(v);
                } else if v["id"] == 21 {
                    sub_resp = Some(v);
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let sub_resp = sub_resp.unwrap();
    assert!(
        sub_resp["result"]["subscriptionId"].as_str().is_some(),
        "chat.subscribe returns subscriptionId: {sub_resp}"
    );
    let snap = snap.unwrap();
    assert_eq!(snap["params"]["kind"], "snapshot");
    assert_eq!(snap["params"]["seq"], 0);
    let snapshot = &snap["params"]["snapshot"];
    let snap_msgs = snapshot["messages"].as_array().expect("snapshot messages");
    assert_eq!(
        snap_msgs.len(),
        50,
        "seq-0 snapshot is the bounded default page, not all 120"
    );
    assert_eq!(snap_msgs[0]["seq"], 70);
    assert_eq!(snap_msgs[49]["seq"], 119);
    assert_eq!(snapshot["truncated"], true);
    assert_eq!(snapshot["totalMessages"], 120);
    assert!(
        snapshot["nextToken"].as_str().is_some(),
        "truncated snapshot carries the older-pages cursor"
    );
    drop(sub);

    // Hydration regression: corrupt every row OLDER than the newest bounded
    // page — any path that fetches/decodes them now fails hard.
    sqlx::query("UPDATE agent_message SET content = 'not-json{' WHERE agent_id = ? AND seq < 70")
        .bind(&agent.0)
        .execute(srv.store.write_pool())
        .await
        .expect("corrupt old rows");

    // The full-hydration read errors on the poisoned rows (proving the poison
    // is potent)…
    let full = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":22,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert!(
        full.get("error").is_some(),
        "full transcript hydration must fail on corrupted rows: {full}"
    );

    // …while the bounded reads still answer correctly: they never touch rows
    // outside their page.
    let list = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":23,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let lite = list["result"]["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .find(|a| a["id"].as_str() == Some(agent_id.as_str()))
        .expect("seeded agent listed");
    assert_eq!(lite["messageCount"], 120);
    assert_eq!(lite["lastUserMessage"].as_str(), Some("prompt 118"));
    assert_eq!(lite["lastAgentResponse"].as_str(), Some("reply 119"));

    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":24,"method":"agent.get","params":{{"agentId":"{agent_id}","workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(got["result"]["agent"]["messageCount"], 120);

    let newest = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":25,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let msgs = newest["result"]["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 50, "newest page decodes only its own 50 rows");
    assert_eq!(msgs[0]["seq"], 70);
    assert_eq!(msgs[49]["seq"], 119);

    srv.ws.stop().await;
}

/// `search.messages` over the real WSS wire (§5.15): FTS5-backed search over
/// persisted user/assistant messages. Covers the reworked contract — global
/// scope when `workspaceId` is absent, `workspaceId` as a hard scope filter,
/// `preferWorkspaceId` as a soft ranking boost, the archived-workspace soft
/// ranking penalty (equally-relevant matches tier preferred → other active →
/// archived), the enriched match shape
/// (`workspaceId`/`agentName`/`role`/`timestamp`/`score`), and that raw FTS5
/// operator syntax in the query never surfaces as an error.
#[tokio::test]
async fn wss_search_messages_fts_global_scope_and_prefer_boost() {
    use intent_core::{AgentId, AgentSession, AgentStatus};

    let srv = start(WsOptions::default()).await;

    // Three workspaces (two active, one archived), one agent each, all
    // holding an identically-worded message (equal bm25 rank) so ordering is
    // decided by the `preferWorkspaceId` boost and the archived penalty
    // alone. The FTS index rows come from the 0074 insert trigger — no
    // manual rebuild.
    let ws_a = WorkspaceId::new();
    let ws_b = WorkspaceId::new();
    let ws_c = WorkspaceId::new();
    let ts = now_iso();
    let seed = |id: &str, ws: &WorkspaceId, name: &str| AgentSession {
        id: AgentId(id.to_string()),
        workspace_id: ws.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: name.to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        provider: None,
        status: AgentStatus::Completed,
        is_active: false,
        system_prompt: None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        parent_agent_id: None,
        specialist: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        messages: vec![],
        stats: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
    };
    for (ws, agent, name) in [
        (&ws_a, "agent-fts-a", "Alpha Agent"),
        (&ws_b, "agent-fts-b", "Beta Agent"),
    ] {
        srv.store
            .insert_workspace(&fixture_workspace(ws))
            .await
            .expect("insert workspace");
        srv.store
            .insert_agent_session(&seed(agent, ws, name))
            .await
            .expect("insert session");
    }
    let mut archived_ws = fixture_workspace(&ws_c);
    archived_ws.archived = true;
    archived_ws.archived_at = Some(ts.clone());
    srv.store
        .insert_workspace(&archived_ws)
        .await
        .expect("insert archived workspace");
    srv.store
        .insert_agent_session(&seed("agent-fts-c", &ws_c, "Gamma Agent"))
        .await
        .expect("insert archived-workspace session");
    // ws-a: plain-string user message. ws-b: content-block assistant message
    // with the same words (block extraction must index it identically).
    // ws-c (archived): the same words again, so only the penalty separates it.
    srv.store
        .append_agent_message(
            &AgentId("agent-fts-a".into()),
            "user",
            &serde_json::json!("deploy pipeline status check"),
            &ts,
        )
        .await
        .expect("append ws-a message");
    srv.store
        .append_agent_message(
            &AgentId("agent-fts-b".into()),
            "assistant",
            &serde_json::json!([{ "type": "text", "text": "deploy pipeline status check" }]),
            &ts,
        )
        .await
        .expect("append ws-b message");
    srv.store
        .append_agent_message(
            &AgentId("agent-fts-c".into()),
            "user",
            &serde_json::json!("deploy pipeline status check"),
            &ts,
        )
        .await
        .expect("append ws-c message");

    // Global search (no workspaceId): every workspace's match, enriched
    // shape, archived-workspace match tiered last by the soft penalty.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":30,"method":"search.messages","params":{"query":"deploy pipeline","requestId":"srch-g"}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 30);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["requestId"], "srch-g");
    let matches = resp["result"]["matches"].as_array().expect("matches");
    assert_eq!(matches.len(), 3, "global search spans workspaces: {resp}");
    let ws_ids: Vec<&str> = matches
        .iter()
        .map(|m| m["workspaceId"].as_str().expect("workspaceId"))
        .collect();
    assert!(
        ws_ids.contains(&ws_a.0.as_str())
            && ws_ids.contains(&ws_b.0.as_str())
            && ws_ids.contains(&ws_c.0.as_str())
    );
    assert_eq!(
        matches[2]["workspaceId"],
        ws_c.0.as_str(),
        "archived-workspace match ranks below equally-relevant active ones: {resp}"
    );
    let a = matches
        .iter()
        .find(|m| m["workspaceId"] == ws_a.0.as_str())
        .expect("ws-a match");
    assert_eq!(a["agentId"], "agent-fts-a");
    assert_eq!(a["agentName"], "Alpha Agent");
    assert_eq!(a["role"], "user");
    assert_eq!(a["timestamp"].as_str(), Some(ts.as_str()));
    assert!(a["messageId"].is_string());
    assert!(a["score"].is_number());
    assert!(a["preview"].as_str().unwrap().contains("deploy"));

    // preferWorkspaceId lifts the preferred workspace's (equally-relevant)
    // match to the top — in both directions — while the archived workspace's
    // match stays tiered last: preferred → other active → archived.
    for (prefer, expect_first) in [(&ws_b, &ws_b), (&ws_a, &ws_a)] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":31,"method":"search.messages","params":{{"query":"deploy pipeline","preferWorkspaceId":"{}"}}}}"#,
            prefer.0
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        let matches = resp["result"]["matches"].as_array().expect("matches");
        assert_eq!(matches.len(), 3, "boost never excludes: {resp}");
        assert_eq!(
            matches[0]["workspaceId"],
            expect_first.0.as_str(),
            "preferred workspace ranks first: {resp}"
        );
        assert_eq!(
            matches[2]["workspaceId"],
            ws_c.0.as_str(),
            "archived workspace ranks last: {resp}"
        );
    }

    // workspaceId is a hard scope filter — and scoping to the archived
    // workspace still returns its match (the penalty never excludes).
    for ws in [&ws_a, &ws_c] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":32,"method":"search.messages","params":{{"query":"deploy pipeline","workspaceId":"{}"}}}}"#,
            ws.0
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        let matches = resp["result"]["matches"].as_array().expect("matches");
        assert_eq!(matches.len(), 1, "{resp}");
        assert_eq!(matches[0]["workspaceId"], ws.0.as_str());
    }

    // Raw FTS5 operator/quote punctuation is sanitized (treated as token
    // separators), never a wire error; a query with no searchable tokens
    // yields empty matches.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":33,"method":"search.messages","params":{"query":"deploy:(pipeline\" -*"}}"#,
    )
    .await;
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(
        resp["result"]["matches"].as_array().expect("matches").len(),
        3
    );
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":34,"method":"search.messages","params":{"query":"*(\"-:"}}"#,
    )
    .await;
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(
        resp["result"]["matches"].as_array().expect("matches").len(),
        0
    );

    srv.ws.stop().await;
}

/// PROTOCOL §3.3/§9 (monorepo#1320): router-constructed `-32602` errors carry
/// the machine-readable `error.data.code` discriminator on the real WSS wire —
/// `"not-found"` for lookups of nonexistent entities (`agent.get`, `note.get`)
/// and `"invalid-params"` for missing required params — while the rest of the
/// envelope (`jsonrpc`, `id`, numeric `code`, `message`) is unchanged.
#[tokio::test]
async fn wss_error_data_code_discriminates_not_found_from_invalid_params() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Error Discriminator"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    // agent.get with an unknown agentId → -32602 + data.code "not-found".
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"agent.get","params":{"agentId":"agent-00000000-0000-0000-0000-000000000000"}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 2, "envelope: {resp}");
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert_eq!(resp["error"]["message"], "Agent not found", "{resp}");
    assert_eq!(
        resp["error"]["data"],
        serde_json::json!({ "code": "not-found" }),
        "unknown agent must carry the not-found discriminator: {resp}"
    );

    // note.get with an unknown noteId in a real workspace → the same
    // not-found shape.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"note.get","params":{{"workspaceId":"{ws_id}","noteId":"note-nonexistent"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 3, "envelope: {resp}");
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert_eq!(resp["error"]["message"], "Note not found", "{resp}");
    assert_eq!(
        resp["error"]["data"],
        serde_json::json!({ "code": "not-found" }),
        "unknown note must carry the not-found discriminator: {resp}"
    );

    // note.get missing the required noteId → -32602 + data.code
    // "invalid-params"; the message is byte-identical to before.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"note.get","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 4, "envelope: {resp}");
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert_eq!(
        resp["error"]["message"], "Missing required parameter: noteId",
        "{resp}"
    );
    assert_eq!(
        resp["error"]["data"],
        serde_json::json!({ "code": "invalid-params" }),
        "missing param must carry the invalid-params discriminator: {resp}"
    );

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
