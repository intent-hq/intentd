//! WSS listener + lifecycle integration tests (M5.3, §5.2/§5.6).
//!
//! Drives a real [`WsApiServer`] over TLS: `/health`, the upgrade auth gate,
//! a JSON-RPC round-trip that must be byte-identical to the UDS transport, and
//! the §5.6 hardening guarantees (fail-fast bind on an occupied port,
//! graceful-shutdown restart, heartbeat termination). The client pins the
//! M5.1 self-signed fingerprint. A separate insecure-mode test proves the
//! plain-`ws://` accept path serves JSON-RPC with no TLS and no bearer token.

mod common;

use std::fmt::Write as _;
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, AgentReverseDispatch, ContentType, Note, NoteId, NoteMetadata, NoteVisibility,
    Result as CoreResult, TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, GitStatusRefresher, Services, WatchHealth, WatcherRegistry};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, serve_uds, AsyncTokenStore, FileWatchStatus, PrimaryReverseRegistry,
    SystemControl, SystemStatus, TokenStore, WsApiServer, WsOptions, MAX_INBOUND_MESSAGE_BYTES,
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
/// Persisted cache key for the current Auggie catalog wire shape.
/// Keep in sync with intent-services/src/model_catalog.rs; that constant is crate-private.
const AUGGIE_CATALOG_VERSION: &str = "preserve-legacy-v1";

/// Lowercase hex sha256 digest of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
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

/// Build a real `Services` API + event bus over a fresh temp `SQLite` store.
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
        services = services.with_models_cache_dir(&cache_dir);
    }
    // Mirror the production composition root (main.rs): the AS-3
    // completion-delivery worker turns agent completion events
    // (idle/failed/deleted/retired) into watcher wakes. Detached — the task
    // ends when the bus is dropped with the harness.
    let _completion_worker = services.spawn_completion_delivery_loop();
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
    reverse_registry: Arc<PrimaryReverseRegistry>,
    dir: tempfile::TempDir,
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
    opts.bind_addresses = vec![Ipv4Addr::LOCALHOST.into()];
    let reverse_registry = Arc::new(PrimaryReverseRegistry::new());
    let ws = WsApiServer::new_with_reverse(
        api.clone(),
        bus.clone(),
        &tls,
        &token_store,
        opts,
        reverse_registry.clone(),
        None,
    )
    .expect("server");
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
        reverse_registry,
        dir,
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
        let _ = write!(r, "Origin: {o}\r\n");
    }
    if let Some(b) = bearer {
        let _ = write!(r, "Authorization: Bearer {b}\r\n");
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Send a `chat.subscribe` frame over a fresh WSS connection, wait for both
/// the `{ subscriptionId }` response (matched on `id`) and the seq-0
/// `subscription.push` snapshot, and return the snapshot object (§7.1).
async fn chat_subscribe_snapshot(port: u16, cfg: Arc<ClientConfig>, frame: &str, id: i64) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send subscribe");
    let mut sub_resp: Option<Value> = None;
    let mut snap: Option<Value> = None;
    while sub_resp.is_none() || snap.is_none() {
        let next = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next())
            .await
            .expect("chat.subscribe frame timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "subscription.push" {
                    snap = Some(v);
                } else if v["id"] == id {
                    sub_resp = Some(v);
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
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
    snap["params"]["snapshot"].clone()
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
                Some(Ok(_)) => {}
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
        sess[0]["result"]["protocolVersion"],
        intent_transport::PROTOCOL_VERSION,
        "explicit top-level protocolVersion in the client.hello result (§5.17)"
    );
    assert_eq!(
        sess[0]["result"]["server"]["locality"], "remote",
        "WSS ⇒ remote in the client.hello server block (§5.14/§5.17)"
    );
    assert_eq!(
        sess[0]["result"]["server"]["version"],
        env!("CARGO_PKG_VERSION"),
        "client.hello exposes the daemon package version"
    );
    match intent_transport::BUILD_COMMIT {
        Some(build_commit) => assert_eq!(
            sess[0]["result"]["server"]["buildCommit"], build_commit,
            "client.hello exposes the daemon build commit when available"
        ),
        None => assert!(
            sess[0]["result"]["server"].get("buildCommit").is_none(),
            "client.hello omits unavailable build metadata"
        ),
    }
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
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
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
                Some(Ok(_)) => {}
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

/// `workspace.create` `contextLinks` over WSS (§5.1): a valid list persists
/// and returns on the created workspace and on `workspace.get` /
/// `workspace.list` rows in the documented camelCase + lowercase-kind wire
/// shape; a workspace created without the param omits the field; a malformed
/// list rejects `-32602` before any state change.
#[tokio::test]
async fn wss_workspace_create_context_links_round_trip_and_validation() {
    let srv = start(WsOptions::default()).await;
    let links = serde_json::json!([
        {
            "kind": "issue",
            "url": "https://github.com/intent-hq/intent/issues/42",
            "owner": "intent-hq",
            "repo": "intent",
            "number": 42,
        },
        {
            "kind": "pr",
            "url": "https://github.com/intent-hq/intentd/pull/7",
            "owner": "intent-hq",
            "repo": "intentd",
            "number": 7,
        },
    ]);
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS Ctx Links","contextLinks":{links}}}}}"#
        ),
    )
    .await;
    assert_eq!(
        created["result"]["workspace"]["contextLinks"], links,
        "create result carries the persisted links: {created}"
    );
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    // Read-back on `workspace.get` serves the same wire shape.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.get","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(got["result"]["workspace"]["contextLinks"], links);

    // `workspace.list` rows carry it too.
    let listed = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":3,"method":"workspace.list","params":{}}"#,
    )
    .await;
    let row = listed["result"]["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == ws_id.as_str())
        .expect("created row listed");
    assert_eq!(row["contextLinks"], links);

    // A create without the param omits the field (absent, never null/[]).
    let plain = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":4,"method":"workspace.create","params":{"title":"WSS No Links"}}"#,
    )
    .await;
    assert!(
        plain["result"]["workspace"].get("contextLinks").is_none(),
        "no-links create omits the field: {plain}"
    );

    // Empty-field validation → -32602 naming the offending entry, no row.
    let bad = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":5,"method":"workspace.create","params":{"title":"WSS Bad Links","contextLinks":[{"kind":"issue","url":"","owner":"o","repo":"r","number":1}]}}"#,
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "bad: {bad}");
    assert!(
        bad["error"]["message"]
            .as_str()
            .is_some_and(|s| s.contains("contextLinks[0].url")),
        "error names the offending entry: {bad}"
    );

    // Unknown `kind` rejects at parse time, same code.
    let bad_kind = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":6,"method":"workspace.create","params":{"title":"WSS Bad Kind","contextLinks":[{"kind":"discussion","url":"https://example.com","owner":"o","repo":"r","number":1}]}}"#,
    )
    .await;
    assert_eq!(bad_kind["error"]["code"], -32602, "bad kind: {bad_kind}");

    // A negative `number` also rejects at parse time (`u64` field): still
    // `-32602`, but without the `contextLinks[i].number` naming — that
    // wording is reserved for the in-range zero case above.
    let bad_number = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":7,"method":"workspace.create","params":{"title":"WSS Bad Number","contextLinks":[{"kind":"pr","url":"https://example.com","owner":"o","repo":"r","number":-1}]}}"#,
    )
    .await;
    assert_eq!(
        bad_number["error"]["code"], -32602,
        "bad number: {bad_number}"
    );

    // Atomic validation: none of the rejected creates left a row behind.
    let after = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":8,"method":"workspace.list","params":{}}"#,
    )
    .await;
    let titles: Vec<&str> = after["result"]["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .filter_map(|w| w["title"].as_str())
        .collect();
    for rejected in ["WSS Bad Links", "WSS Bad Kind", "WSS Bad Number"] {
        assert!(
            !titles.contains(&rejected),
            "rejected create `{rejected}` must not persist a workspace: {titles:?}"
        );
    }
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
                Some(Ok(_)) => {}
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
                None | Some(Err(_) | Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
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
/// The created/resolved `AgentLite` payloads also carry the harness stamp
/// (`harnessVersion` = current constant, `harnessFeatures` = the captured
/// agentFeatures snapshot) over the real wire (intent-hq/monorepo#2459).
#[tokio::test]
async fn wss_agent_create_rejects_client_supplied_agent_id() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    // Harness stamp on the create response (monorepo#2459): the wire carries
    // the current version constant and the full captured agentFeatures
    // snapshot in camelCase.
    assert_eq!(
        created["result"]["agent"]["harnessVersion"].as_str(),
        Some(intent_core::CURRENT_HARNESS_VERSION),
        "agent.create response carries harnessVersion: {created}"
    );
    let features = &created["result"]["agent"]["harnessFeatures"];
    assert!(
        features.is_object(),
        "agent.create response carries the harnessFeatures snapshot: {created}"
    );
    for key in [
        "backgroundHooks",
        "hostExec",
        "scripts",
        "terminalAccess",
        "browserAutomation",
        "richChatBlocks",
        "structuredQuestions",
        "attentionRequests",
        "stateSnapshot",
    ] {
        assert!(
            features[key].is_boolean(),
            "harnessFeatures.{key} must be a boolean toggle: {created}"
        );
    }
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.get","params":{{"agentId":"{minted}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert_eq!(
        got["result"]["agent"]["id"].as_str(),
        Some(minted.as_str()),
        "agent.get at the server-minted id must resolve: {got}"
    );
    // The stamp survives the read path too (agent.get projects the persisted
    // row, not the create-time in-memory value).
    assert_eq!(
        got["result"]["agent"]["harnessVersion"].as_str(),
        Some(intent_core::CURRENT_HARNESS_VERSION),
        "agent.get response carries harnessVersion: {got}"
    );
    assert_eq!(
        got["result"]["agent"]["harnessFeatures"], *features,
        "agent.get returns the same persisted harnessFeatures snapshot: {got}"
    );
    // agent.list projects the same stamp on its AgentLite rows.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    let row = listed["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"].as_str() == Some(minted.as_str()))
        .expect("minted agent in list");
    assert_eq!(
        row["harnessVersion"].as_str(),
        Some(intent_core::CURRENT_HARNESS_VERSION),
        "agent.list rows carry harnessVersion: {listed}"
    );
    assert_eq!(
        row["harnessFeatures"], *features,
        "agent.list rows carry the persisted harnessFeatures snapshot: {listed}"
    );

    srv.ws.stop().await;
}

/// Extending monorepo#2932 — `metadata.initialMessage` is stripped from the
/// whole `AgentLite` projection: the full spawn-time first message persists
/// on the session and is served by `agent.getSession`, but `agent.get` and
/// `agent.list` rows OMIT it (absent, never `null`) — it was the last
/// unbounded per-row field on real workspaces and no client reads it off
/// agent rows.
#[tokio::test]
async fn wss_agent_lite_omits_initial_message() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"List Slim"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Persist an initialMessage via the create-time metadata harvest.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Slim","metadata":{{"initialMessage":"a long spawn-time first message"}}}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // The session detail read still serves the persisted value.
    let session_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getSession","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let session = wss_call(srv.port, srv.cfg.clone(), &session_frame).await;
    assert_eq!(
        session["result"]["session"]["initialMessage"].as_str(),
        Some("a long spawn-time first message"),
        "agent.getSession keeps serving initialMessage: {session}"
    );

    // `agent.get` omits the field entirely.
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    assert!(
        got["result"]["agent"]["metadata"].is_object(),
        "agent.get row must carry a metadata object: {got}"
    );
    assert!(
        got["result"]["agent"]["metadata"]
            .get("initialMessage")
            .is_none(),
        "agent.get rows must omit metadata.initialMessage: {got}"
    );

    // The list row omits the field entirely.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    let row = listed["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"].as_str() == Some(agent_id.as_str()))
        .expect("created agent in list");
    assert!(
        row["metadata"].is_object(),
        "list row must carry a metadata object: {row}"
    );
    assert!(
        row["metadata"].get("initialMessage").is_none(),
        "agent.list rows must omit metadata.initialMessage: {row}"
    );

    srv.ws.stop().await;
}

/// Soft retire round-trip over the real WSS transport: `agent.retire` (via
/// the service seam the MCP binding calls) marks the session inert —
/// excluded from default `agent.list`, served by `includeRetired: true` with
/// `retiredAt`, still readable via `agent.get`, rejecting `agent.sendMessage`
/// — and the wire `agent.restore` method returns it to service. Both
/// transitions emit their events (`agent:retired` / `agent:restored`) to an
/// `events.subscribe` subscriber.
#[tokio::test]
async fn wss_agent_soft_retire_and_restore_round_trip() {
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
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    async fn next_event(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        event_type: &str,
    ) -> Value {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).expect("json");
                        if v["method"] == "events.event"
                            && v["params"]["event"]["type"] == event_type
                        {
                            return v["params"]["event"].clone();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("expected text frame, got {other:?}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
    }

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Soft Retire"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Elder"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Subscribe to both lifecycle events on a persistent connection.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let sub = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"events.subscribe","params":{{"eventTypes":["agent:retired","agent:restored"],"workspaceId":"{ws_id}"}}}}"#
        ),
        3,
    )
    .await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // Retire via the WorkspaceApi seam (the MCP `ws.agent.retire` binding
    // routes here; there is deliberately no wire agent.retire method).
    let retired = srv
        .api
        .agent_retire(
            intent_core::AgentId::from(agent_id.as_str()),
            Some(WorkspaceId(ws_id.clone())),
            Some("handing off".to_string()),
        )
        .await
        .expect("retire");
    assert_eq!(retired["success"], serde_json::json!(true));
    let retired_at = retired["retiredAt"]
        .as_str()
        .expect("retiredAt")
        .to_string();
    let retired_at = retired_at.as_str();

    // agent:retired reaches the subscriber with name + reason.
    let evt = next_event(&mut ws, "agent:retired").await;
    assert_eq!(evt["data"]["agentId"], agent_id.as_str());
    assert_eq!(evt["data"]["agentName"], "Elder");
    assert_eq!(evt["data"]["reason"], "handing off");

    // Default agent.list excludes the retired row…
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    assert!(
        listed["result"]["agents"]
            .as_array()
            .expect("agents array")
            .iter()
            .all(|a| a["id"].as_str() != Some(agent_id.as_str())),
        "retired row must be excluded from the default list: {listed}"
    );
    // …but carries the always-present retiredCount (v8.2)…
    assert_eq!(
        listed["result"]["retiredCount"],
        serde_json::json!(1),
        "default list carries retiredCount: {listed}"
    );
    // …includeRetired serves it, carrying retiredAt…
    let list_all_frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.list","params":{{"workspaceId":"{ws_id}","includeRetired":true}}}}"#
    );
    let listed_all = wss_call(srv.port, srv.cfg.clone(), &list_all_frame).await;
    let row = listed_all["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"].as_str() == Some(agent_id.as_str()))
        .expect("retired row under includeRetired");
    assert_eq!(
        row["retiredAt"].as_str(),
        Some(retired_at),
        "includeRetired rows carry retiredAt: {row}"
    );
    assert_eq!(
        listed_all["result"]["retiredCount"],
        serde_json::json!(1),
        "includeRetired carries retiredCount too: {listed_all}"
    );
    // …retiredOnly serves ONLY the retired row (v8.2)…
    let list_retired_frame = format!(
        r#"{{"jsonrpc":"2.0","id":15,"method":"agent.list","params":{{"workspaceId":"{ws_id}","retiredOnly":true}}}}"#
    );
    let listed_retired = wss_call(srv.port, srv.cfg.clone(), &list_retired_frame).await;
    let retired_rows = listed_retired["result"]["agents"]
        .as_array()
        .expect("agents array");
    assert_eq!(
        retired_rows.len(),
        1,
        "retiredOnly serves only retired rows: {listed_retired}"
    );
    assert_eq!(retired_rows[0]["id"].as_str(), Some(agent_id.as_str()));
    assert_eq!(
        retired_rows[0]["retiredAt"].as_str(),
        Some(retired_at),
        "retiredOnly rows carry retiredAt: {listed_retired}"
    );
    assert_eq!(
        listed_retired["result"]["retiredCount"],
        serde_json::json!(1),
        "retiredOnly carries retiredCount too: {listed_retired}"
    );
    // …and the contradictory flag pair is rejected with -32602.
    let both_frame = format!(
        r#"{{"jsonrpc":"2.0","id":16,"method":"agent.list","params":{{"workspaceId":"{ws_id}","includeRetired":true,"retiredOnly":true}}}}"#
    );
    let both = wss_call(srv.port, srv.cfg.clone(), &both_frame).await;
    assert_eq!(
        both["error"]["code"],
        serde_json::json!(-32602),
        "includeRetired + retiredOnly is -32602: {both}"
    );
    // …agent.get still serves the retired row (FE renders it read-only)…
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        got["result"]["agent"]["retiredAt"].as_str(),
        Some(retired_at),
        "agent.get keeps serving the retired row: {got}"
    );
    // …and sends fail closed with the retired error.
    let send_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.sendMessage","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","content":"hello?"}}}}"#
    );
    let send = wss_call(srv.port, srv.cfg.clone(), &send_frame).await;
    assert_eq!(send["error"]["code"], -32602, "send to retired: {send}");
    assert!(
        send["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("retired"),
        "error names the retired state: {send}"
    );

    // agent.restore returns it to service and emits agent:restored.
    let restore_frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"agent.restore","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}"}}}}"#
    );
    let restored = wss_call(srv.port, srv.cfg.clone(), &restore_frame).await;
    assert_eq!(restored["result"]["restored"], serde_json::json!(true));
    let evt = next_event(&mut ws, "agent:restored").await;
    assert_eq!(evt["data"]["agentId"], agent_id.as_str());

    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    let row = listed["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"].as_str() == Some(agent_id.as_str()))
        .expect("restored row back in the default list");
    assert!(
        row.get("retiredAt").is_none(),
        "restored rows omit retiredAt: {row}"
    );
    assert_eq!(
        listed["result"]["retiredCount"],
        serde_json::json!(0),
        "restore drops retiredCount back to zero: {listed}"
    );
    let listed_retired = wss_call(srv.port, srv.cfg.clone(), &list_retired_frame).await;
    assert_eq!(
        listed_retired["result"]["agents"],
        serde_json::json!([]),
        "restored row must leave the retiredOnly list: {listed_retired}"
    );
    // Idempotent-friendly no-op on a second restore.
    let restored_again = wss_call(srv.port, srv.cfg.clone(), &restore_frame).await;
    assert_eq!(
        restored_again["result"]["restored"],
        serde_json::json!(false),
        "second restore is the documented no-op: {restored_again}"
    );

    srv.ws.stop().await;
}

/// Retire cascade + cleanup over the real WSS transport (PROTOCOL §5.5):
/// retiring a parent with an ACTIVE child is rejected (`InvalidParams`
/// naming the child, nothing mutated); after the child settles the retire
/// succeeds and cascades — parent AND child emit `agent:retired` to an
/// `events.subscribe` subscriber (the child's reason names the parent), the
/// parent's active hook is cancelled (`hook:cancelled`, no owner wake), and
/// a watcher on the child receives the one-shot retired-notice wake (its
/// completion watch is consumed). `agent.restore` resurrects neither the
/// cancelled hook nor the consumed watch. PR-monitor cancellation is
/// asserted at the unit level (`agent_ops` tests); this e2e covers the
/// hook + watch sweeps.
#[tokio::test]
async fn wss_agent_retire_cascade_guard_hooks_and_watches() {
    /// Next `events.event` frame of any type, bounded by `deadline`.
    async fn next_event_any(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        deadline: tokio::time::Instant,
    ) -> Value {
        tokio::time::timeout_at(deadline, async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).expect("json");
                        if v["method"] == "events.event" {
                            return v["params"]["event"].clone();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("expected text frame, got {other:?}"),
                }
            }
        })
        .await
        .expect("timed out waiting for events.event")
    }

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Retire Cascade"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let workspace_id = WorkspaceId(ws_id.clone());
    let create_agent = |name: &str, id: i64| {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"{name}"}}}}"#
        );
        let port = srv.port;
        let cfg = srv.cfg.clone();
        async move {
            let created = wss_call(port, cfg, &frame).await;
            created["result"]["agent"]["id"]
                .as_str()
                .expect("agent id")
                .to_string()
        }
    };
    let parent_id = create_agent("Coordinator", 2).await;
    let child_id = create_agent("Junior", 3).await;
    let watcher_id = create_agent("Watcher", 4).await;
    let parent = intent_core::AgentId::from(parent_id.as_str());
    let child = intent_core::AgentId::from(child_id.as_str());
    let watcher = intent_core::AgentId::from(watcher_id.as_str());

    // Link the child under the parent and mark it mid-turn (Active).
    let mut child_session = srv.store.get_agent_session(&child).await.expect("child");
    child_session.parent_agent_id = Some(parent.clone());
    srv.store
        .update_agent_session(&workspace_id, &child_session)
        .await
        .expect("link child to parent");
    srv.store
        .set_agent_session_status(
            &workspace_id,
            &child,
            intent_core::AgentStatus::Active,
            true,
            &now_iso(),
            None,
        )
        .await
        .expect("set child active");

    // Guard: retire fails while a descendant is running a turn — the error
    // names the child and NOTHING is mutated.
    let err = srv
        .api
        .agent_retire(parent.clone(), Some(workspace_id.clone()), None)
        .await
        .expect_err("retire with an active child must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("active child agent(s) still running a turn") && msg.contains("Junior"),
        "guard error names the active child: {msg}"
    );
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"agent.get","params":{{"agentId":"{parent_id}"}}}}"#
        ),
    )
    .await;
    assert!(
        got["result"]["agent"].get("retiredAt").is_none(),
        "rejected retire mutates nothing: {got}"
    );

    // Watcher arms a completion watch on the still-active child (an idle
    // nothing-pending target would be rejected), then the child settles
    // idle with no event — the watch stays armed for the retire.
    let watch_result = srv
        .api
        .agent_watch(workspace_id.clone(), watcher.clone(), child.clone())
        .await
        .expect("watch child");
    assert_eq!(
        watch_result["ok"],
        serde_json::json!(true),
        "{watch_result}"
    );
    srv.store
        .set_agent_session_status(
            &workspace_id,
            &child,
            intent_core::AgentStatus::RuntimeIdle,
            false,
            &now_iso(),
            None,
        )
        .await
        .expect("settle child");

    // Seed an ACTIVE hook owned by the retiring parent.
    let hook_id = "hook-retire-cascade-e2e";
    srv.store
        .insert_hook(&intent_core::Hook {
            hook_id: intent_core::HookId::from(hook_id),
            workspace_id: workspace_id.clone(),
            agent_id: parent.clone(),
            name: "retire-watchdog".to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 600_000,
            cron: None,
            run_at: None,
            state: intent_core::HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: Some(now_iso()),
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some("2999-01-01T00:00:00Z".to_string()),
            perpetual: false,
            dispatch_count: 0,
        })
        .await
        .expect("seed hook");

    // Subscribe BEFORE the retire so no event can be missed.
    let mut sub = connect_ws(srv.port, srv.cfg.clone()).await;
    sub.send(Message::Text(
        format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"events.subscribe","params":{{"eventTypes":["agent:retired","hook:cancelled"],"workspaceId":"{ws_id}"}}}}"#
        )
        .into(),
    ))
    .await
    .expect("send subscribe");
    loop {
        match sub.next().await {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
                if v.get("id") == Some(&serde_json::json!(6)) {
                    assert!(v["result"]["subscriptionId"].is_string(), "subscribe: {v}");
                    break;
                }
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }

    // Retire the parent: guard passes now, the cascade retires the child.
    let retired = srv
        .api
        .agent_retire(
            parent.clone(),
            Some(workspace_id.clone()),
            Some("shutting down".to_string()),
        )
        .await
        .expect("retire parent");
    assert_eq!(retired["success"], serde_json::json!(true), "{retired}");

    // Collect the three lifecycle events (relative order not asserted).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let (mut parent_retired, mut child_retired, mut hook_cancelled) = (None, None, None);
    while parent_retired.is_none() || child_retired.is_none() || hook_cancelled.is_none() {
        let ev = next_event_any(&mut sub, deadline).await;
        match ev["type"].as_str().unwrap_or_default() {
            "agent:retired" if ev["data"]["agentId"] == serde_json::json!(parent_id) => {
                parent_retired = Some(ev);
            }
            "agent:retired" if ev["data"]["agentId"] == serde_json::json!(child_id) => {
                child_retired = Some(ev);
            }
            "hook:cancelled" if ev["data"]["hookId"] == serde_json::json!(hook_id) => {
                hook_cancelled = Some(ev);
            }
            _ => {}
        }
    }
    let parent_retired = parent_retired.expect("parent agent:retired");
    assert_eq!(parent_retired["data"]["agentName"], "Coordinator");
    assert_eq!(parent_retired["data"]["reason"], "shutting down");
    let child_retired = child_retired.expect("child agent:retired");
    assert_eq!(child_retired["data"]["agentName"], "Junior");
    assert_eq!(
        child_retired["data"]["reason"], "parent Coordinator retired",
        "cascade reason names the parent: {child_retired}"
    );
    let hook_cancelled = hook_cancelled.expect("hook:cancelled");
    assert_eq!(
        hook_cancelled["data"]["state"], "cancelled",
        "{hook_cancelled}"
    );

    // Both rows are inert: excluded from the default list, retiredAt served.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    let listed_ids: Vec<&str> = listed["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .filter_map(|a| a["id"].as_str())
        .collect();
    assert!(
        !listed_ids.contains(&parent_id.as_str()) && !listed_ids.contains(&child_id.as_str()),
        "retired parent+child excluded from the default list: {listed}"
    );

    // The watcher's one-shot watch settles with the retired-notice wake. No
    // AgentManager is attached in this harness, so the wake persists
    // directly to the watcher's transcript — poll agent.getConversation.
    let notice = "The agent retired and cannot be re-watched or woken again";
    let conv_frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"agent.getConversation","params":{{"agentId":"{watcher_id}"}}}}"#
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let wake_text = loop {
        let conv = wss_call(srv.port, srv.cfg.clone(), &conv_frame).await;
        let found = conv["result"]["messages"]
            .as_array()
            .expect("messages array")
            .iter()
            .filter(|m| m["role"] == "user")
            .find_map(|m| {
                let text = serde_json::to_string(&m["contentBlocks"]).unwrap_or_default();
                text.contains(notice).then_some(text)
            });
        if let Some(text) = found {
            break text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retired-notice wake never reached the watcher: {conv}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        wake_text.contains("Junior"),
        "the wake names the retired agent: {wake_text}"
    );

    // The consumed watch is gone from the watcher's subscriptions.
    let subs_frame = format!(
        r#"{{"jsonrpc":"2.0","id":9,"method":"agent.getSubscriptions","params":{{"workspaceId":"{ws_id}","agentId":"{watcher_id}"}}}}"#
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let subs = wss_call(srv.port, srv.cfg.clone(), &subs_frame).await;
        let remaining = subs["result"]["subscriptions"]
            .as_array()
            .expect("subscriptions array");
        if remaining.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "consumed watch still listed: {subs}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Restore BOTH sessions: neither the cancelled hook nor the consumed
    // watch is resurrected, and restoring the parent earlier would not have
    // restored the child (each row is individually restorable).
    for (id, agent_id) in [(10, &parent_id), (11, &child_id)] {
        let restore_frame = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"agent.restore","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}"}}}}"#
        );
        let restored = wss_call(srv.port, srv.cfg.clone(), &restore_frame).await;
        assert_eq!(
            restored["result"]["restored"],
            serde_json::json!(true),
            "restore {agent_id}: {restored}"
        );
    }
    let hooks = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":12,"method":"hook.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let hook_row = hooks["result"]["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == serde_json::json!(hook_id))
        .expect("seeded hook listed");
    assert_eq!(
        hook_row["state"],
        serde_json::json!("cancelled"),
        "restore does not resurrect the cancelled hook: {hook_row}"
    );
    let subs = wss_call(srv.port, srv.cfg.clone(), &subs_frame).await;
    assert!(
        subs["result"]["subscriptions"]
            .as_array()
            .expect("subscriptions array")
            .is_empty(),
        "restore does not resurrect the consumed watch: {subs}"
    );

    srv.ws.stop().await;
}

/// List-payload cost contract (extending monorepo#2932): `agent.list` rows
/// bound every render-preview field — `lastAgentResponse`, `lastUserMessage`,
/// `digest`, `lastToolUse`, `metadata.completionReport` — to the render-sized
/// per-field budget, while `agent.get` keeps serving the full values for the
/// same session. Asserts the wire shape over the real WSS transport.
#[tokio::test]
async fn wss_agent_list_caps_previews_get_serves_full() {
    const BUDGET: usize = intent_core::AGENT_LIST_PREVIEW_BUDGET_BYTES;
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"List Cap"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Capped"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Persist a long user message, a long assistant response + digest with an
    // over-budget tool_use, and a long completionReport.
    let long_user = format!("user ask starts {}", "u".repeat(BUDGET * 3));
    let long_line = format!("final answer {}", "a".repeat(BUDGET * 3));
    let long_digest = format!("digest {}", "d".repeat(BUDGET * 3));
    let long_report = format!("report {}", "r".repeat(BUDGET * 3));
    let user_frame = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "agent.appendMessage",
        "params": {
            "agentId": agent_id, "role": "user",
            "contentBlocks": [{ "type": "text", "text": long_user }],
        },
    });
    wss_call(srv.port, srv.cfg.clone(), &user_frame.to_string()).await;
    let assistant_frame = serde_json::json!({
        "jsonrpc": "2.0", "id": 4, "method": "agent.appendMessage",
        "params": {
            "agentId": agent_id, "role": "assistant",
            "contentBlocks": [
                {
                    "type": "tool_use", "id": "m:0", "name": "write_file",
                    "input": { "path": "/tmp/a.txt", "content": "x".repeat(BUDGET * 3) },
                    "toolCallId": "t1",
                },
                {
                    "type": "text",
                    "text": format!("{long_line}\n<agent_digest>{long_digest}</agent_digest>"),
                },
            ],
        },
    });
    wss_call(srv.port, srv.cfg.clone(), &assistant_frame.to_string()).await;
    let update_frame = serde_json::json!({
        "jsonrpc": "2.0", "id": 5, "method": "agent.update",
        "params": { "agentId": agent_id, "changes": { "completionReport": long_report } },
    });
    wss_call(srv.port, srv.cfg.clone(), &update_frame.to_string()).await;

    // The detail read serves the full values.
    let get_frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
    );
    let got = wss_call(srv.port, srv.cfg.clone(), &get_frame).await;
    let agent = &got["result"]["agent"];
    assert_eq!(
        agent["lastAgentResponse"].as_str(),
        Some(long_line.as_str()),
        "agent.get keeps the full lastAgentResponse"
    );
    assert_eq!(
        agent["lastUserMessage"].as_str(),
        Some(long_user.as_str()),
        "agent.get keeps the full lastUserMessage"
    );
    assert_eq!(
        agent["digest"].as_str(),
        Some(long_digest.as_str()),
        "agent.get keeps the full digest"
    );
    assert_eq!(
        agent["metadata"]["completionReport"].as_str(),
        Some(long_report.as_str()),
        "agent.get keeps the full metadata.completionReport"
    );
    assert_eq!(
        agent["lastToolUse"]["input"]["content"]
            .as_str()
            .map(str::len),
        Some(BUDGET * 3),
        "agent.get keeps the full lastToolUse input"
    );

    // The list row caps every preview field to the render budget.
    let list_frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let listed = wss_call(srv.port, srv.cfg.clone(), &list_frame).await;
    let row = listed["result"]["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"].as_str() == Some(agent_id.as_str()))
        .expect("created agent in list");
    assert_eq!(
        row["lastAgentResponse"].as_str().map(str::len),
        Some(BUDGET),
        "agent.list caps lastAgentResponse: {row}"
    );
    assert_eq!(
        row["lastUserMessage"].as_str().map(str::len),
        Some(BUDGET),
        "agent.list caps lastUserMessage: {row}"
    );
    assert_eq!(
        row["digest"].as_str().map(str::len),
        Some(BUDGET),
        "agent.list caps digest: {row}"
    );
    assert_eq!(
        row["metadata"]["completionReport"].as_str().map(str::len),
        Some(BUDGET),
        "agent.list caps metadata.completionReport (capped, not omitted): {row}"
    );
    assert_eq!(
        row["lastToolUse"]["name"].as_str(),
        Some("write_file"),
        "the tool name survives the capped list preview: {row}"
    );
    // The list re-cap keeps the §5.5 preview contract: the over-budget input
    // carries `inputTruncated: true` + `inputBytes` (original size).
    assert_eq!(
        row["lastToolUse"]["inputTruncated"].as_bool(),
        Some(true),
        "agent.list stamps inputTruncated on the capped preview: {row}"
    );
    assert!(
        row["lastToolUse"]["inputBytes"].as_u64().unwrap() > BUDGET as u64,
        "inputBytes names the original input's serialized size: {row}"
    );
    let served_tool = row["lastToolUse"]["input"].to_string();
    assert!(
        served_tool.len() <= BUDGET * 2,
        "agent.list caps lastToolUse input near the budget, got {} bytes: {row}",
        served_tool.len()
    );

    srv.ws.stop().await;
}

/// monorepo#3041 over the real WSS wire (§5.1): `workspace.list` rows omit
/// `tokenUsage` entirely (absent, never null) and archived rows additionally
/// omit `agentSummary`, while active rows keep it; the `workspace.subscribe`
/// seq-0 snapshot rows omit `tokenUsage` the same way; `workspace.get` keeps
/// both fields for detail reads, archived included.
#[tokio::test]
async fn wss_workspace_list_slims_token_usage_and_archived_agent_summary() {
    use std::collections::BTreeMap;

    use intent_core::{AgentId, AgentSession, AgentStatus, TokenUsage, TokenUsageTotals};

    let srv = start(WsOptions::default()).await;

    // Seed one active and one archived workspace directly through the shared
    // store, both carrying a persisted `tokenUsage` rollup, each with one
    // agent session so the list enrichment builds an `agentSummary`.
    let usage = TokenUsage {
        by_agent_id: BTreeMap::from([(
            "agent-slim-a".to_string(),
            TokenUsageTotals {
                input_tokens: 10,
                output_tokens: 20,
                cache_read_tokens: 5,
                cache_creation_tokens: 3,
                thought_tokens: 0,
                cost: None,
            },
        )]),
        totals: TokenUsageTotals {
            input_tokens: 10,
            output_tokens: 20,
            cache_read_tokens: 5,
            cache_creation_tokens: 3,
            thought_tokens: 0,
            cost: None,
        },
        by_model: BTreeMap::new(),
        last_scan_at: Some(now_iso()),
    };
    let ws_active = WorkspaceId::new();
    let mut active = fixture_workspace(&ws_active);
    active.token_usage = Some(usage.clone());
    srv.store
        .insert_workspace(&active)
        .await
        .expect("insert active workspace");
    let ws_archived = WorkspaceId::new();
    let mut archived = fixture_workspace(&ws_archived);
    archived.archived = true;
    archived.archived_at = Some(now_iso());
    archived.token_usage = Some(usage);
    srv.store
        .insert_workspace(&archived)
        .await
        .expect("insert archived workspace");

    let ts = now_iso();
    let mk_session = |id: &str, ws: &WorkspaceId| AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId(id.to_string()),
        workspace_id: ws.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: "Slim Agent".to_string(),
        name_explicitly_set: true,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        status: AgentStatus::Idle,
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
        file_blocks: None,
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
        pending_delete_at: None,
        retired_at: None,
    };
    srv.store
        .insert_agent_session(&mk_session("agent-slim-a", &ws_active))
        .await
        .expect("insert active-workspace session");
    srv.store
        .insert_agent_session(&mk_session("agent-slim-b", &ws_archived))
        .await
        .expect("insert archived-workspace session");

    // workspace.list (includeArchived): tokenUsage absent on every row;
    // agentSummary absent on the archived row, present on the active one.
    let listed = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list","params":{"includeArchived":true}}"#,
    )
    .await;
    let rows = listed["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    let row_active = rows
        .iter()
        .find(|w| w["id"] == ws_active.0.as_str())
        .expect("active row");
    let row_archived = rows
        .iter()
        .find(|w| w["id"] == ws_archived.0.as_str())
        .expect("archived row");
    assert!(
        row_active.get("tokenUsage").is_none(),
        "list rows omit tokenUsage (monorepo#3041): {row_active}"
    );
    assert!(
        row_archived.get("tokenUsage").is_none(),
        "archived list rows omit tokenUsage: {row_archived}"
    );
    assert!(
        row_active["agentSummary"].is_object(),
        "active list row keeps agentSummary: {row_active}"
    );
    assert!(
        row_archived.get("agentSummary").is_none(),
        "archived list rows omit agentSummary (monorepo#3041): {row_archived}"
    );

    // workspace.get keeps both fields for detail reads — archived included.
    for ws_id in [&ws_active, &ws_archived] {
        let got = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.get","params":{{"workspaceId":"{}"}}}}"#,
                ws_id.0
            ),
        )
        .await;
        let ws = &got["result"]["workspace"];
        assert!(
            ws["tokenUsage"].is_object(),
            "workspace.get keeps tokenUsage: {ws}"
        );
        assert!(
            ws["agentSummary"].is_object(),
            "workspace.get keeps agentSummary: {ws}"
        );
    }

    // workspace.subscribe seq-0 snapshot (lite list path, global channel —
    // archived rows are always included, monorepo#775): rows omit tokenUsage
    // the same way.
    let mut sub = connect_ws(srv.port, srv.cfg.clone()).await;
    sub.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":3,"method":"workspace.subscribe","params":{}}"#
            .to_string()
            .into(),
    ))
    .await
    .expect("send workspace.subscribe");
    let mut sub_resp: Option<Value> = None;
    let mut snap: Option<Value> = None;
    while sub_resp.is_none() || snap.is_none() {
        let frame = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("workspace.subscribe frame timed out");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "subscription.push" {
                    snap = Some(v);
                } else if v["id"] == 3 {
                    sub_resp = Some(v);
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let snap = snap.unwrap();
    assert_eq!(snap["params"]["kind"], "snapshot", "{snap}");
    assert_eq!(snap["params"]["seq"], 0, "{snap}");
    let snap_rows = snap["params"]["snapshot"].as_array().expect("snapshot");
    for ws_id in [&ws_active, &ws_archived] {
        let row = snap_rows
            .iter()
            .find(|w| w["id"] == ws_id.0.as_str())
            .expect("seeded workspace in seq-0 snapshot");
        assert!(
            row.get("tokenUsage").is_none(),
            "seq-0 snapshot rows omit tokenUsage (monorepo#3041): {row}"
        );
    }

    srv.ws.stop().await;
}

/// monorepo#564: `agent.sendMessage` to a nonexistent agent id (e.g. a
/// truncated id) fails closed with `-32602` naming the unknown id — it must
/// NOT auto-queue a phantom message (`queued: true`) the sender then waits on
/// forever. A send to a real agent on the same connection still succeeds.
#[tokio::test]
async fn wss_agent_send_message_rejects_unknown_agent() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// `agent.diagnostics` reports real pending-message queue snapshots over the
/// WSS wire: after `agent.queueMessage`, `diagnostics.queues` carries the
/// target's queue (drain-order entries with `queuedAt`, content truncated to
/// 200 chars) and `summary.queuedAgents` counts it; after
/// `agent.removeQueuedMessage` the snapshot is empty again.
#[tokio::test]
async fn wss_agent_diagnostics_reports_queue_snapshots() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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

/// intent-hq/monorepo#1897 over the real WSS wire: a ready-to-send queue
/// entry older than the stale-queue threshold on an idle agent surfaces as a
/// `stale-queue-entry` stuck-risk in the `agent.diagnostics` response —
/// `stuckRisks` names the agent and oldest entry, `summary.stuckRisks` counts
/// it, and the `text` rendering mentions it. The stale entry is seeded via
/// the durable queue snapshot + rehydration path (the same path a daemon
/// restart uses), so the wire read reflects the live in-memory queue.
#[tokio::test]
async fn wss_agent_diagnostics_flags_stale_queue_entry() {
    let dir = test_tempdir("intentd-wss-stalequeue-");
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
    registry
        .apply(&[("model.defaultProvider".into(), serde_json::json!("auggie"))])
        .expect("seed default provider");
    let services = Services::new(store.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    // Keep a concrete handle (Services is Clone over shared internals) so the
    // test can rehydrate the seeded queue into the same live registry the
    // WSS listener serves from.
    let services_handle = services.clone();
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let mut opts = WsOptions {
        base_port: 0,
        ..WsOptions::default()
    };
    opts.bind_addresses = vec![Ipv4Addr::LOCALHOST.into()];
    let server = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = server.start().await.expect("start");

    let created_ws = wss_call(
        port,
        cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Stale Queue"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Stale Queue Target"}}}}"#
    );
    let created = wss_call(port, cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = intent_core::AgentId::from(agent_id.as_str());

    // Seed one backdated and one fresh ready-to-send entry through the
    // durable snapshot, then rehydrate into the live in-memory registry.
    let stale_at = "2020-01-01T00:00:00Z";
    let fresh_at = now_iso();
    let row = |id: &str, position: i64, queued_at: &str| intent_store::AgentQueueRow {
        id: id.into(),
        agent_id: agent.clone(),
        position,
        payload: serde_json::json!({
            "id": id,
            "content": "undelivered wake",
            "queuedAt": queued_at,
        }),
        created_at: queued_at.into(),
        turn_id: id.into(),
    };
    store
        .replace_agent_queue(
            &agent,
            &[
                row("qmsg-stale", 0, stale_at),
                row("qmsg-fresh", 1, &fresh_at),
            ],
        )
        .await
        .expect("seed queue");
    let rehydrated = services_handle
        .rehydrate_agent_queues()
        .await
        .expect("rehydrate queues");
    assert_eq!(rehydrated, 2, "both seeded entries rehydrated");

    let diag_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.diagnostics","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let diag = wss_call(port, cfg.clone(), &diag_frame).await;
    assert_eq!(diag["jsonrpc"], Value::String("2.0".into()), "{diag}");
    assert_eq!(diag["id"], Value::from(3), "{diag}");
    let d = &diag["result"]["diagnostics"];
    let risks = d["stuckRisks"].as_array().expect("stuckRisks array");
    let risk = risks
        .iter()
        .find(|r| r["type"] == serde_json::json!("stale-queue-entry"))
        .expect("stale-queue-entry risk on the wire");
    assert_eq!(risk["severity"], Value::String("warning".into()), "{risk}");
    assert_eq!(risk["agentId"], Value::String(agent_id.clone()), "{risk}");
    assert_eq!(
        risk["entryId"],
        Value::String("qmsg-stale".into()),
        "{risk}"
    );
    assert_eq!(
        risk["count"],
        Value::from(1),
        "fresh entry excluded: {risk}"
    );
    assert!(
        risk["ageMs"].as_i64().expect("ageMs") > 5 * 60 * 1000,
        "{risk}"
    );
    assert_eq!(
        d["summary"]["stuckRisks"],
        Value::from(risks.len()),
        "summary counts the risks: {d}"
    );
    let text = diag["result"]["text"].as_str().expect("text");
    assert!(text.contains("stale-queue-entry"), "text: {text}");

    server.stop().await;
}

/// intent-hq/monorepo#2669 over the real WSS wire: `agent.diagnostics` agent
/// rows carry `conversationBytes` (total persisted conversation size), and a
/// conversation past the 4 MiB threshold raises a `large-conversation`
/// stuck-risk naming the agent and sizes so coordinators can rotate to a
/// fresh agent before turns start silently truncating.
#[tokio::test]
async fn wss_agent_diagnostics_reports_conversation_bytes_and_large_risk() {
    let dir = test_tempdir("intentd-wss-largeconv-");
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
    registry
        .apply(&[("model.defaultProvider".into(), serde_json::json!("auggie"))])
        .expect("seed default provider");
    let services = Services::new(store.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let mut opts = WsOptions {
        base_port: 0,
        ..WsOptions::default()
    };
    opts.bind_addresses = vec![Ipv4Addr::LOCALHOST.into()];
    let server = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = server.start().await.expect("start");

    let created_ws = wss_call(
        port,
        cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Large Conversation"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bloated Target"}}}}"#
    );
    let created = wss_call(port, cfg.clone(), &create_frame).await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = intent_core::AgentId::from(agent_id.as_str());

    // Seed a >4 MiB persisted conversation directly in the store.
    let big_text = "x".repeat(1024 * 1024);
    for _ in 0..5 {
        store
            .append_agent_message(
                &agent,
                "assistant",
                &serde_json::json!([{ "type": "text", "text": big_text }]),
                &now_iso(),
            )
            .await
            .expect("append bloated message");
    }

    let diag_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.diagnostics","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let diag = wss_call(port, cfg.clone(), &diag_frame).await;
    assert_eq!(diag["jsonrpc"], Value::String("2.0".into()), "{diag}");
    let d = &diag["result"]["diagnostics"];
    let rows = d["agents"].as_array().expect("agents array");
    let row = rows
        .iter()
        .find(|r| r["id"] == Value::String(agent_id.clone()))
        .expect("agent row");
    let bytes = row["conversationBytes"]
        .as_u64()
        .expect("conversationBytes on the wire");
    assert!(bytes > 4 * 1024 * 1024, "bytes: {bytes}");
    let risks = d["stuckRisks"].as_array().expect("stuckRisks array");
    let risk = risks
        .iter()
        .find(|r| r["type"] == serde_json::json!("large-conversation"))
        .expect("large-conversation risk on the wire");
    assert_eq!(risk["severity"], Value::String("warning".into()), "{risk}");
    assert_eq!(risk["agentId"], Value::String(agent_id.clone()), "{risk}");
    assert_eq!(risk["conversationBytes"], Value::from(bytes), "{risk}");
    assert_eq!(
        risk["thresholdBytes"],
        Value::from(4u64 * 1024 * 1024),
        "{risk}"
    );

    server.stop().await;
}

/// Unknown providers hard-fail at the front door (PROTOCOL §5.5, §9):
/// `agent.create` with an unknown explicit `provider` or an unknown compound
/// model prefix, and `agent.setModel` with an unknown compound prefix, are all
/// rejected with `-32602` naming the unknown id — no session row is persisted
/// and no default-provider fallback occurs.
#[tokio::test]
async fn wss_agent_create_and_set_model_reject_unknown_provider() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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

    // Compound model id → -32602 at the wire boundary (PROTOCOL §5.5:
    // compound `provider:model` ids are rejected before any side effect).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad2","model":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "compound model id must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("model must be a bare model id"),
        "error must tell the caller to pass a bare model id: {rejected}"
    );

    // Explicit VALID provider + compound model → the same wire-boundary
    // -32602 (compound ids reject regardless of the provider param).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Bad3","provider":"auggie","model":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "valid provider + compound model must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("model must be a bare model id"),
        "error must tell the caller to pass a bare model id: {rejected}"
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

    // …and agent.setModel rejects a compound modelId the same way.
    let set_frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"nonexistent:foo"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &set_frame).await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "setModel with a compound modelId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("modelId must be a bare model id"),
        "error must name modelId and tell the caller to pass a bare model id: {rejected}"
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

/// Legacy compound rows serialize SPLIT over the real WSS transport
/// (migration 0113 + the read backstop): a row seeded directly in the DB
/// with a compound `provider:model` id — as an older daemon could have
/// persisted it — comes back from `agent.get` and `agent.list` with the
/// bare model and the prefix-won provider; no wire payload ever carries a
/// colon-compound model id. An effort-lookalike bare id (`opus[1m]`) passes
/// through untouched.
#[tokio::test]
async fn wss_agent_reads_serialize_legacy_compound_rows_split() {
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Compound Split"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Seed the legacy rows straight into the store — the wire hard-rejects
    // compound ids (-32602), so only a pre-0113 daemon could have written
    // them. Bypassing the RPC layer is the point: prove the READ path splits.
    let create_agent = |name: &str, id: i64| {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"{name}"}}}}"#
        );
        let port = srv.port;
        let cfg = srv.cfg.clone();
        async move {
            let created = wss_call(port, cfg, &frame).await;
            created["result"]["agent"]["id"]
                .as_str()
                .expect("agent id")
                .to_string()
        }
    };
    let compound_id = create_agent("Legacy", 2).await;
    let bare_id = create_agent("Bare", 3).await;
    sqlx::query("UPDATE agent_session SET model = 'anthropic:claude-opus-4', provider = 'stale' WHERE id = ?")
        .bind(&compound_id)
        .execute(srv.store.write_pool())
        .await
        .expect("seed compound row");
    sqlx::query(
        "UPDATE agent_session SET model = 'opus[1m]', provider = 'claude-code' WHERE id = ?",
    )
    .bind(&bare_id)
    .execute(srv.store.write_pool())
    .await
    .expect("seed effort-lookalike row");

    // agent.get: the compound row serializes split — prefix wins provider.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.get","params":{{"agentId":"{compound_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        got["result"]["agent"]["model"],
        Value::from("claude-opus-4"),
        "agent.get must serialize the split model: {got}"
    );
    assert_eq!(
        got["result"]["agent"]["provider"],
        Value::from("anthropic"),
        "agent.get must serialize the prefix-won provider: {got}"
    );

    // agent.list: same split on the summary projection, and the
    // effort-lookalike bare id passes through untouched.
    let listed = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"agent.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let agents = listed["result"]["agents"].as_array().expect("agents");
    let by_id = |id: &str| {
        agents
            .iter()
            .find(|a| a["id"].as_str() == Some(id))
            .unwrap_or_else(|| panic!("agent {id} in list: {listed}"))
    };
    let compound = by_id(&compound_id);
    assert_eq!(
        compound["model"],
        Value::from("claude-opus-4"),
        "agent.list must serialize the split model: {listed}"
    );
    assert_eq!(
        compound["provider"],
        Value::from("anthropic"),
        "agent.list must serialize the prefix-won provider: {listed}"
    );
    let bare = by_id(&bare_id);
    assert_eq!(
        bare["model"],
        Value::from("opus[1m]"),
        "effort-lookalike bare id must pass through untouched: {listed}"
    );
    assert_eq!(
        bare["provider"],
        Value::from("claude-code"),
        "bare id must not disturb the provider: {listed}"
    );

    // No wire payload carries a compound id.
    for agent in agents {
        let model = agent["model"].as_str().unwrap_or_default();
        assert!(
            !model.contains(':'),
            "no listed agent may serve a compound model id: {listed}"
        );
    }

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
    // version key must match each provider's current one (Auggie is shape-versioned;
    // Grok has no pin).
    let cache = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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

    // An auggie session (explicit provider param) rejects a bare
    // grok-claimed model via agent.setModel with the same envelope shape…
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Auggie","model":"sonnet4.5","provider":"auggie"}}}}"#
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
        Value::from("sonnet4.5"),
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
    // version key must match each provider's current one (Auggie is shape-versioned;
    // Grok has no pin).
    let cache = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Pid","model":"sonnet4.5","provider":"auggie"}}}}"#
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

    // Compound modelId → -32602 at the wire boundary (PROTOCOL §5.5:
    // compound `provider:model` ids are rejected regardless of providerId).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"agent.setModel","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","modelId":"auggie:sonnet4.5","providerId":"grok"}}}}"#
    );
    let rejected = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_envelope(&rejected, 5);
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "compound modelId must be -32602: {rejected}"
    );
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("modelId must be a bare model id"),
        "error must name modelId and tell the caller to pass a bare model id: {rejected}"
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
        Value::from("sonnet4.5"),
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
    // version key must match each provider's current one (Auggie is shape-versioned;
    // Grok has no pin).
    let cache = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
            r#""model":"sonnet4.5","provider":"auggie","specialistId":"implementor","#,
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
    assert_eq!(created["model"].as_str(), Some("sonnet4.5"));
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
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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

    // Mark the second (newest) message seen: `{ success, lastSeenMessageId }`
    // plus the `agent:updated` marker event (self-sufficient, §6.5). The
    // response and the notification race on the wire (the op keeps working —
    // workspace-unread settlement — after the publish), so collect both in
    // arrival order.
    ws.send(Message::Text(
        format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.markSeen","params":{{"workspaceId":"{ws_id}","agentId":"{agent_id}","messageId":"{second_id}"}}}}"#
        )
        .into(),
    ))
    .await
    .expect("send markSeen");
    let (marked, evt) = tokio::time::timeout(Duration::from_secs(10), async {
        let mut marked: Option<Value> = None;
        let mut evt: Option<Value> = None;
        loop {
            if let (Some(m), Some(e)) = (&marked, &evt) {
                return (m.clone(), e.clone());
            }
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v.get("id") == Some(&serde_json::json!(6)) {
                        marked = Some(v);
                    } else if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "agent:updated"
                    {
                        evt = Some(v["params"]["event"].clone());
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for markSeen response + agent:updated");
    assert_eq!(marked["result"]["success"], true, "markSeen: {marked}");
    assert_eq!(marked["result"]["lastSeenMessageId"], second_id.as_str());
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

/// `agent:last-message` + `AgentLite.lastToolUse` (PROTOCOL §6.5/§5.5): a
/// transcript persist emits BOTH the lean `agent:message` echo (unchanged
/// shape) and the content-bearing `agent:last-message` companion carrying
/// the preview projections — including `lastToolUse` (`{ name, input }`)
/// when the appended row bears a `tool_use` block — and `agent.get` serves
/// the same preview from the persisted 0098 column. A follow-up tool-less
/// assistant persist emits a companion WITHOUT `lastToolUse` (the cleared
/// state) and `agent.get` drops the field.
#[tokio::test]
async fn wss_agent_last_message_event_and_last_tool_use_round_trip() {
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
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    async fn next_event(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        event_type: &str,
    ) -> Value {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).expect("json");
                        if v["method"] == "events.event"
                            && v["params"]["event"]["type"] == event_type
                        {
                            return v["params"]["event"].clone();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = ws.send(Message::Pong(p)).await;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("expected text frame, got {other:?}"),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
    }
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS LastToolUse"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"ToolUse Agent"}}}}"#
        )],
    )
    .await;
    let agent_id = sess[0]["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // One persistent connection: subscribe BEFORE the persists so both
    // event notifications are delivered to this client.
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    let sub = send_and_wait(
        &mut ws,
        format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"events.subscribe","params":{{"eventTypes":["agent:message","agent:last-message"],"workspaceId":"{ws_id}"}}}}"#
        ),
        3,
    )
    .await;
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // Persist an assistant row bearing a tool_use block. Issued over a
    // SEPARATE connection so the subscriber socket above receives only the
    // event notifications (send_and_wait would otherwise discard an event
    // frame interleaved before the RPC response).
    let appended = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.appendMessage","params":{{"agentId":"{agent_id}","role":"assistant","contentBlocks":[{{"type":"text","text":"running a tool"}},{{"type":"tool_use","id":"m:1","name":"view","input":{{"path":"/tmp/f"}},"toolCallId":"tc-1"}}]}}}}"#
        ),
    )
    .await;
    let message_id = appended["result"]["message"]["id"]
        .as_str()
        .expect("appended message id")
        .to_string();

    // The lean echo keeps its pre-existing shape (no preview fields).
    let echo = next_event(&mut ws, "agent:message").await;
    assert_eq!(echo["data"]["agentId"], agent_id.as_str());
    assert_eq!(echo["data"]["messageId"], message_id.as_str());
    assert_eq!(echo["data"]["role"], "assistant");
    assert!(
        echo["data"].get("lastToolUse").is_none(),
        "agent:message stays the id-only echo: {echo}"
    );

    // The companion carries the preview projections (§6.5).
    let last = next_event(&mut ws, "agent:last-message").await;
    assert_eq!(last["data"]["agentId"], agent_id.as_str());
    assert_eq!(last["data"]["messageId"], message_id.as_str());
    assert_eq!(last["data"]["role"], "assistant");
    assert_eq!(last["data"]["lastMessageRole"], "assistant");
    assert_eq!(last["data"]["lastMessageId"], message_id.as_str());
    assert_eq!(last["data"]["lastAgentResponse"], "running a tool");
    assert_eq!(
        last["data"]["lastToolUse"],
        serde_json::json!({"name": "view", "input": {"path": "/tmp/f"}}),
        "companion carries the bounded tool preview: {last}"
    );
    assert!(
        last["data"].get("lastUserMessage").is_none(),
        "assistant echo omits lastUserMessage: {last}"
    );

    // Served on the AgentLite projection from the persisted column.
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        got["result"]["agent"]["lastToolUse"],
        serde_json::json!({"name": "view", "input": {"path": "/tmp/f"}}),
        "agent.get serves lastToolUse from the persisted column: {got}"
    );

    // A newer tool-less assistant persist clears: the companion omits
    // `lastToolUse` and `agent.get` drops the field. Separate connection,
    // same reasoning as the first persist.
    let cleared = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.appendMessage","params":{{"agentId":"{agent_id}","role":"assistant","contentBlocks":[{{"type":"text","text":"all done"}}]}}}}"#
        ),
    )
    .await;
    assert_eq!(cleared["result"]["success"], true, "append: {cleared}");
    let last2 = next_event(&mut ws, "agent:last-message").await;
    assert_eq!(last2["data"]["lastAgentResponse"], "all done");
    assert!(
        last2["data"].get("lastToolUse").is_none(),
        "tool-less persist omits lastToolUse (cleared state): {last2}"
    );
    let got2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"agent.get","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert!(
        got2["result"]["agent"].get("lastToolUse").is_none(),
        "agent.get drops lastToolUse after a tool-less persist: {got2}"
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
        "agent_session.updated_at must advance when a message is appended (STAB-19): before={before_updated_at}, after={after_updated_at}"
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
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Effort","model":"gpt-5.3-codex","provider":"codex","reasoningEffort":"xhigh"}}}}"#
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    // Hermeticity (monorepo#3162): point discovery at a deterministic
    // executable so the delegate availability check passes without a real
    // auggie on the test host.
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "auggie": "/bin/sh" }),
    );
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
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    srv.set_setting("model.default", serde_json::json!("fable-5"));
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
        Value::from("fable-5"),
        "settings default model pinned: {created}"
    );
    assert_eq!(
        created["result"]["agent"]["reasoningEffort"],
        Value::from("high"),
        "settings default effort pinned alongside the settings default model: {created}"
    );

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Explicit model","model":"fable-5","provider":"auggie"}}}}"#
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

/// The catalog-default rung of the creation-time default-model resolution
/// (PROTOCOL §5.5), over the real WSS transport: with a warm cached catalog
/// whose `isDefault` row is `sonnet5` and no settings default, a no-model
/// `agent.create` pins that row's id to `session.model` (served on the
/// `AgentLite` payload); a configured settings default still outranks it; and
/// the settings default reasoning effort does NOT companion a
/// catalog-default-resolved model.
#[tokio::test]
async fn wss_agent_create_pins_the_catalog_default_model() {
    let dir = test_tempdir("intentd-wss-catalog-default-");
    let cache = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
                "fetchedAtMs": 0,
                "models": [
                    { "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                      "effortLevels": ["low", "high"] },
                    { "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie",
                      "isDefault": true }
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    srv.set_setting("model.defaultReasoningEffort", serde_json::json!("high"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Catalog Default"}}"#,
    )
    .await;
    let ws_id = created_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Catalog default"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(created["jsonrpc"], "2.0");
    assert_eq!(created["id"], 2);
    assert_eq!(
        created["result"]["agent"]["model"],
        Value::from("sonnet5"),
        "the cached catalog's isDefault row is pinned: {created}"
    );
    assert!(
        created["result"]["agent"]
            .as_object()
            .expect("agent object")
            .get("reasoningEffort")
            .is_none(),
        "the settings default effort must not companion a catalog-default model: {created}"
    );

    // A configured settings default outranks the catalog rung.
    srv.set_setting("model.default", serde_json::json!("fable-5"));
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Settings wins"}}}}"#
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(created["jsonrpc"], "2.0");
    assert_eq!(created["id"], 3);
    assert_eq!(
        created["result"]["agent"]["model"],
        Value::from("fable-5"),
        "the settings chain outranks the catalog default: {created}"
    );

    srv.ws.stop().await;
}

/// `system.capabilities` (PROTOCOL §5.7): machine-level capabilities with no
/// params and no workspaceId. The result is a plain object whose optional
/// `cowSupported` mirrors the cached workspaces-root `CoW` probe that fills
/// `Workspace.cowSupported` (§5.1). The harness injects an existing hermetic
/// workspaces root, so the probe always runs and the field is present as a
/// boolean (true on `CoW` filesystems like APFS, false on e.g. ext4); when the
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

/// `debug.sampleStacks` (PROTOCOL §5.43, monorepo#1755): point-in-time
/// sample of the daemon's own thread stacks — no workspaceId, both params
/// optional and clamped server-side. Asserts the documented result shape
/// (`report` non-empty string, echoed effective `durationMs`/`frequencyHz`,
/// numeric `sampleCount`/`distinctStacks`) over the real WSS transport, plus
/// the `-32602` caller error for a non-numeric param. Unix-only capture —
/// these test hosts are Unix, so the success path is exercised directly.
#[cfg(unix)]
#[tokio::test]
async fn wss_debug_sample_stacks_returns_report() {
    let srv = start(WsOptions::default()).await;

    // durationMs below the 100ms floor is clamped, keeping the test fast.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"debug.sampleStacks","params":{"durationMs":1,"frequencyHz":99}}"#,
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let result = resp["result"].as_object().expect("result is an object");
    let report = result["report"].as_str().expect("report is a string");
    assert!(
        report.contains("intentd stack sample"),
        "report carries the header even with zero samples: {resp}"
    );
    assert_eq!(result["durationMs"], 100, "clamped to the 100ms floor");
    assert_eq!(result["frequencyHz"], 99);
    assert!(result["sampleCount"].is_number(), "sampleCount: {resp}");
    assert!(
        result["distinctStacks"].is_number(),
        "distinctStacks: {resp}"
    );

    // A present non-numeric param is a caller error, not a silent default.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"debug.sampleStacks","params":{"durationMs":"long"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602, "non-numeric param: {resp}");

    srv.ws.stop().await;
}

/// `providers.catalog` (monorepo#928): no params, no workspaceId — the
/// provider registry is compiled-in daemon data. Asserts the documented
/// result shape: one row per `ACP_PROVIDERS` entry in registry order,
/// daemon-evaluated `visible` with the raw gating fields passed through
/// (env-var gates: mock, plus cortex/droid hidden by default behind
/// `INTENTD_ENABLE_CORTEX` / `INTENTD_ENABLE_DROID`), and no default
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
            "antigravity",
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

    // Gating: cortex and droid are hidden by default — visible: false with
    // the raw requiresEnvVar passed through (the test daemon sets neither
    // enable var); ungated providers are visible.
    let cortex = &providers[3];
    assert_eq!(cortex["shortName"], "Cortex");
    assert_eq!(cortex["visible"], Value::Bool(false));
    assert_eq!(
        cortex["requiresEnvVar"].as_str(),
        Some("INTENTD_ENABLE_CORTEX")
    );
    assert!(
        cortex.get("requiresFeatureCode").is_none(),
        "cortex must carry no requiresFeatureCode: {resp}"
    );
    let droid = &providers[7];
    assert_eq!(droid["shortName"], "Droid");
    assert_eq!(droid["visible"], Value::Bool(false));
    assert_eq!(
        droid["requiresEnvVar"].as_str(),
        Some("INTENTD_ENABLE_DROID")
    );
    let auggie = &providers[0];
    assert_eq!(auggie["shortName"], "Auggie");
    assert_eq!(auggie["visible"], Value::Bool(true));

    // mock's env-var gate passes the raw field through regardless of the
    // daemon environment.
    let mock = providers
        .iter()
        .find(|provider| provider["id"] == "mock")
        .unwrap();
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

/// Open a pinned TLS stream to `addr:port` (the fixed-target variant of
/// [`tls_connect`] for multi-bind tests).
async fn tls_connect_to(
    addr: std::net::IpAddr,
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((addr, port)).await.expect("tcp connect");
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    tokio_rustls::TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

/// Whether this machine can bind the IPv6 loopback (rare CI sandboxes lack
/// IPv6; multi-bind tests use `127.0.0.1` + `::1` as the two-address set).
fn ipv6_loopback_available() -> bool {
    StdTcpListener::bind(("::1", 0)).is_ok()
}

/// monorepo#3314: a `server.bindAddress` list binds one listener per address
/// on the same port — both addresses accept and serve the same instance.
#[tokio::test]
async fn multi_bind_serves_every_configured_address() {
    if !ipv6_loopback_available() {
        eprintln!("skipping: IPv6 loopback unavailable");
        return;
    }
    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![
            Ipv4Addr::LOCALHOST.into(),
            std::net::Ipv6Addr::LOCALHOST.into(),
        ],
        ..WsOptions::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start binds both addresses");

    for addr in [
        std::net::IpAddr::from(Ipv4Addr::LOCALHOST),
        std::net::IpAddr::from(std::net::Ipv6Addr::LOCALHOST),
    ] {
        let mut tls_stream = tls_connect_to(addr, port, cfg.clone()).await;
        tls_stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        tls_stream.flush().await.expect("flush");
        let mut buf = Vec::new();
        let _ = tls_stream.read_to_end(&mut buf).await;
        let resp = String::from_utf8_lossy(&buf);
        assert_eq!(status_code(&resp), 200, "health on {addr}: {resp}");
        assert!(
            resp.contains("\"status\":\"ok\""),
            "health body on {addr}: {resp}"
        );
    }
    ws.stop().await;

    // stop() releases every listener: both addresses refuse new connections.
    for addr in [
        std::net::IpAddr::from(Ipv4Addr::LOCALHOST),
        std::net::IpAddr::from(std::net::Ipv6Addr::LOCALHOST),
    ] {
        assert!(
            TcpStream::connect((addr, port)).await.is_err(),
            "{addr}:{port} still accepting after stop"
        );
    }
}

/// monorepo#3314: partial bind failure is a hard error — when any address in
/// the set cannot bind, `start()` fails and the addresses that DID bind are
/// released (never silently serve fewer interfaces than configured).
#[tokio::test]
async fn multi_bind_partial_failure_is_all_or_nothing() {
    if !ipv6_loopback_available() {
        eprintln!("skipping: IPv6 loopback unavailable");
        return;
    }
    // Occupy a port on ::1 only, then ask for [127.0.0.1, ::1] on it.
    let blocker = StdTcpListener::bind(("::1", 0)).expect("blocker bind");
    let port = blocker.local_addr().expect("blocker addr").port();

    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: port,
        bind_addresses: vec![
            Ipv4Addr::LOCALHOST.into(),
            std::net::Ipv6Addr::LOCALHOST.into(),
        ],
        ..WsOptions::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let err = ws
        .start()
        .await
        .expect_err("start must fail when ::1 is occupied");
    assert!(
        err.to_string().contains("::1"),
        "bind error names the failing address: {err}"
    );

    // All-or-nothing: the successfully-bound 127.0.0.1 listener was released.
    assert!(
        TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .is_err(),
        "127.0.0.1:{port} must not accept after a partial bind failure"
    );
}

/// `WsOptions` is public, so an empty bind set must be a hard `start()`
/// error in release builds too — never a "successful" start with a
/// heartbeat and reported port but no TCP listener behind it.
#[tokio::test]
async fn empty_bind_set_is_a_start_error() {
    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: Vec::new(),
        ..WsOptions::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let err = ws
        .start()
        .await
        .expect_err("start must fail on an empty bind set");
    assert!(
        err.to_string().contains("non-empty"),
        "error names the invariant: {err}"
    );
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
    let socket = srv.dir.path().join("intentd-wss.sock");
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

#[cfg(unix)]
#[tokio::test]
async fn wss_models_list_preserves_legacy_metadata_through_cache() {
    use std::os::unix::fs::PermissionsExt;

    let dir = test_tempdir("intentd-wss-models-legacy-");
    let calls = dir.path().join("calls");
    let bin = dir.path().join("auggie");
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> '{}'
if [ "$*" != "model list --json" ]; then
  exit 1
fi
cat <<'JSON'
{{"models":[{{"shortName":"current","displayName":"Current","modelGroupPriority":1,"priority":1,"isLegacyModel":false}},{{"shortName":"legacy","displayName":"Legacy","modelGroupPriority":2,"priority":1,"isLegacyModel":true}}]}}
JSON
"#,
        calls.display()
    );
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    let request =
        r#"{"jsonrpc":"2.0","id":47,"method":"models.list","params":{"providerId":"auggie"}}"#;
    let response = wss_call(srv.port, srv.cfg.clone(), request).await;
    assert_eq!(
        response,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 47,
            "result": {
                "providerId": "auggie",
                "models": [
                    { "id": "current", "name": "Current", "provider": "auggie",
                      "modelGroupPriority": 1, "priority": 1 },
                    { "id": "legacy", "name": "Legacy", "provider": "auggie",
                      "modelGroupPriority": 2, "priority": 1, "isLegacyModel": true }
                ],
                "source": "auggie"
            }
        })
    );
    assert_eq!(
        std::fs::read_to_string(&calls).unwrap(),
        "model list --json\n"
    );

    let cached_request = r#"{"jsonrpc":"2.0","id":48,"method":"models.list"}"#;
    let cached = wss_call(srv.port, srv.cfg.clone(), cached_request).await;
    assert_eq!(cached["result"]["models"], response["result"]["models"]);
    assert_eq!(cached["result"]["source"], response["result"]["source"]);
    assert!(cached["result"].get("providerId").is_none());
    assert_eq!(
        std::fs::read_to_string(&calls).unwrap(),
        "model list --json\n"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_models_list_provider_auggie_failure_includes_warning() {
    use std::os::unix::fs::PermissionsExt;

    let dir = test_tempdir("intentd-wss-models-provider-failure-");
    let bin = dir.path().join("auggie");
    std::fs::write(&bin, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    let request =
        r#"{"jsonrpc":"2.0","id":49,"method":"models.list","params":{"providerId":"auggie"}}"#;
    let response = wss_call(srv.port, srv.cfg.clone(), request).await;
    assert_eq!(
        response,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 49,
            "result": {
                "providerId": "auggie",
                "models": [],
                "source": "static",
                "warning": "auggie CLI unavailable or returned no models"
            }
        })
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_stats_get_usage_round_trip_with_seeded_store() {
    // The current hour bucket is inside both the current month and the
    // trailing-24h window; a bucket 48h back is outside the 24h window.
    // Bucket keys are the store's RFC-3339 UTC hour floors.
    use chrono::{Datelike, Timelike, Utc};
    // stats.getUsage: the global usage-stats read behind the agentic
    // usage-stats cards. Seed two hourly buckets straight into the store,
    // then drive both a "month" and a "24h" read over the real WSS path and
    // assert the documented result shape.
    let srv = start(WsOptions::default()).await;

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
    let divergent_hour = (u8::try_from(now.hour()).expect("hour < 24") + 5) % 24;
    let delta = intent_store::UsageStatsDelta {
        input_tokens: 100,
        output_tokens: 40,
        cache_read_tokens: 20,
        cache_creation_tokens: 10,
        thought_tokens: 15,
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
            Some(&stamp(old, u8::try_from(old.hour()).expect("hour < 24"))),
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
    assert_eq!(r["totals"]["thoughtTokens"], 15);
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
    assert_eq!(by_provider[0]["thoughtTokens"], 15);
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
    assert_eq!(opus["thoughtTokens"], 15);
    // Zero-thought rollups omit the field entirely (§5.23 convention) —
    // byte-compatible with the pre-thought_tokens response shape.
    if let Some(sonnet) = by_model.iter().find(|m| m["model"] == "Sonnet 5") {
        assert!(sonnet.get("thoughtTokens").is_none(), "{resp}");
    }
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
    use chrono::{Duration as ChronoDuration, Utc};
    // stats.getRateHistory (§5.39): the global per-minute token-rate history
    // behind the HUD TOK/MIN chart. Seed minute buckets straight into the
    // store — current minute, two minutes back, and one outside a 5-sample
    // window — then read over the real WSS path and assert the documented
    // zero-filled, chronological result shape.
    let srv = start(WsOptions::default()).await;

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
    // is hidden by default (the test daemon sets no INTENTD_ENABLE_CORTEX)
    // and serves a gated empty list with a warning under its own source tag.
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
    let warning = resp["result"]["warning"]
        .as_str()
        .expect("closed gate ⇒ warning");
    assert!(
        warning.contains("INTENTD_ENABLE_CORTEX"),
        "gate warning names the env var: {resp}"
    );
    // Gated empty success is fresh, not stale: exactly the documented keys,
    // with the gating warning and no stale flag.
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
    let calls = || std::fs::read_to_string(&count).map_or(0, |s| s.lines().count());

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
async fn wss_models_list_legacy_fresh_entry_served_and_forced_failure_stale() {
    // models.list legacy path contract (§5.30) over the real WSS transport:
    // a NON-forced read whose persisted entry is fresh (younger than the 24h
    // staleness threshold) serves it plainly (`{ models, source }`, no
    // `stale`, no `warning`, no probe). A FORCED read whose probe fails
    // serves the same last-good list labeled `stale: true` + `warning` —
    // exactly `{ models, source, stale, warning }` with `source: "auggie"`,
    // never a silent static fallback. The fake auggie appends to a counter
    // file per invocation and always fails, making CLI spawns observable.
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
    let calls = || std::fs::read_to_string(&count).map_or(0, |s| s.lines().count());
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis(),
    )
    .expect("unix ms fits in u64");
    let last_good = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
                "fetchedAtMs": now_ms,
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

    // Non-forced: the fresh persisted entry is a plain cache hit.
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
    assert_eq!(
        calls(),
        0,
        "non-forced fresh cache hit must not spawn the CLI"
    );

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

#[cfg(unix)]
#[tokio::test]
async fn wss_models_list_legacy_aged_entry_reprobe_failure_serves_stale() {
    // models.list legacy path staleness contract (§5.30) over the real WSS
    // transport: a NON-forced read whose persisted entry is past the 24h
    // staleness threshold (fetchedAtMs: 0) serves the last-good list
    // immediately, labeled `stale: true` + `warning` — exactly
    // `{ models, source, stale, warning }` with `source: "auggie"`, never a
    // silent static fallback — while the refresh probe runs in the
    // BACKGROUND (stale-while-revalidate, intent-hq/intent#3874); the failed
    // probe then arms the negative window so subsequent reads do not
    // re-spawn the CLI. The fake auggie appends to a counter file per
    // invocation and always fails, making CLI spawns observable.
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir("intentd-wss-models-aged-");
    let count = dir.path().join("count");
    let bin = dir.path().join("auggie");
    std::fs::write(
        &bin,
        format!("#!/bin/sh\necho x >> {}\nexit 1\n", count.display()),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let calls = || std::fs::read_to_string(&count).map_or(0, |s| s.lines().count());
    let last_good = serde_json::json!({
        "version": 2,
        "entries": {
            "auggie": {
                "versionKey": AUGGIE_CATALOG_VERSION,
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

    // Non-forced: the aged entry is served immediately with the degradation
    // labels; the refresh probe runs in the background.
    let frame = r#"{"jsonrpc":"2.0","id":47,"method":"models.list"}"#;
    let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
    assert_eq!(resp["id"], 47);
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["source"], "auggie");
    assert_eq!(resp["result"]["stale"], true, "{resp}");
    assert!(resp["result"]["warning"].is_string(), "{resp}");
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
    assert_eq!(keys, ["models", "source", "stale", "warning"], "{resp}");
    // The detached background refresh spawns the CLI shortly after the
    // response; poll until the counter file shows the spawn.
    let mut waited = 0u32;
    while calls() == 0 && waited < 200 {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        waited += 1;
    }
    assert!(calls() > 0, "background refresh must spawn the CLI");

    // Wait for the failed probe's OUTCOME to land, not just its start: the
    // SWR spawn path and the negative-window path serve distinguishable
    // warnings ("refreshing in the background" vs "serving last known model
    // list"), so poll reads until the negative-cache wording shows up. A
    // read landing while the in-flight slot is still held serves the former
    // and spawns nothing, so this cannot pass on slot suppression alone; if
    // the outcome were never recorded, reads after the slot release would
    // keep re-spawning and the spawn-count assertion below would fail.
    let mut req_id = 48u32;
    let mut waited = 0u32;
    let resp = loop {
        let frame = format!(r#"{{"jsonrpc":"2.0","id":{req_id},"method":"models.list"}}"#);
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        assert_eq!(resp["id"], req_id);
        assert!(resp.get("error").is_none(), "{resp}");
        assert_eq!(resp["result"]["source"], "auggie");
        assert_eq!(resp["result"]["stale"], true, "{resp}");
        let warning = resp["result"]["warning"].as_str().unwrap_or_default();
        if warning.contains("serving last known model list") {
            break resp;
        }
        assert!(
            waited < 200,
            "probe failure never reached the negative cache: {resp}"
        );
        waited += 1;
        req_id += 1;
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert_eq!(
        resp["result"]["models"],
        serde_json::json!([ { "id": "lg", "name": "LG", "provider": "auggie" } ])
    );
    let after_probe = calls();

    // Within the negative window: the stale last-good list keeps being
    // served without re-spawning the CLI (no new background refresh).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":{},"method":"models.list"}}"#,
        req_id + 1
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert!(resp.get("error").is_none(), "{resp}");
    assert_eq!(resp["result"]["source"], "auggie");
    assert_eq!(resp["result"]["stale"], true, "{resp}");
    assert_eq!(
        calls(),
        after_probe,
        "negative window must suppress the re-probe"
    );
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));

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
    srv.set_setting("model.defaultProvider", serde_json::json!("claude-code"));
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
    // Gate closed on unset settings: with `model.defaultProvider` not
    // configured, the derived default is undecidable and
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
async fn wss_agent_enhance_prompt_gate_follows_model_default_provider() {
    // Gate precedence: the effective provider derives from
    // `model.defaultProvider` alone — `model.default` is a bare model id and
    // never carries a provider. Both directions: a non-auggie default
    // provider closes the gate regardless of `model.default`, and an auggie
    // one opens it. An unknown default provider is not trusted → closed.
    let (_auggie_dir, bin) = fake_auggie_script(
        "prefix-enhance",
        "printf '🤖\\n<augment-enhanced-prompt>Enhanced: via prefix</augment-enhanced-prompt>\\n'",
    );
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    // Direction 1: claude-code default provider → gate closes; the
    // `model.default` value is irrelevant to provider derivation.
    srv.set_setting("model.defaultProvider", serde_json::json!("claude-code"));
    srv.set_setting("model.default", serde_json::json!("sonnet4.5"));
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
        "non-auggie model.defaultProvider closes the gate"
    );

    // Direction 2: auggie default provider → gate passes.
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":38,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 38);
    assert_eq!(
        resp["result"]["enhanced"], "Enhanced: via prefix",
        "auggie model.defaultProvider opens the gate"
    );

    // An unknown model.defaultProvider is not trusted → gate closes.
    srv.set_setting("model.defaultProvider", serde_json::json!("typo"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":39,"method":"agent.enhancePrompt","params":{"prompt":"ship it"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 39);
    assert_eq!(
        resp["result"]["available"],
        serde_json::json!(false),
        "unknown model.defaultProvider closes the gate"
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));

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

/// Write an executable stand-in for a non-auggie provider's ACP adapter: a
/// shell wrapper that execs the deterministic `mock-acp-agent.mjs` fixture
/// with `MOCK_AGENT_BEHAVIOR` pinned in the wrapper itself (never in the test
/// process env, which parallel tests share). `behavior` is the fixture's JSON
/// behavior document.
#[cfg(unix)]
fn fake_acp_adapter_script(tag: &str, behavior: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let fixture = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let dir = test_tempdir(&format!("intentd-wss-acp-{tag}-"));
    let bin = dir.path().join("codex-acp");
    std::fs::write(
        &bin,
        format!("#!/bin/sh\nMOCK_AGENT_BEHAVIOR='{behavior}' exec node {fixture:?} \"$@\"\n"),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    (dir, bin)
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_routes_non_auggie_provider_via_ephemeral_acp() {
    // Provider-neutral routing (§5.32): with codex as the effective default
    // provider the daemon runs an EPHEMERAL ACP session (initialize →
    // session/new → one session/prompt → reap) against the mock agent and
    // returns the same `{ text }` envelope the auggie route does — the
    // streamed reply cleaned by `cleanAgentMessage`.
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping non-auggie completeOnce e2e: node not on PATH");
        return;
    }
    let (_adapter_dir, bin) =
        fake_acp_adapter_script("complete", r#"{"response":"🤖\nfix-login-flow"}"#);
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("codex"));
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "codex": bin.to_string_lossy() }),
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":48,"method":"agent.completeOnce","params":{"prompt":"slug for login fix"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 48);
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(
        resp["result"],
        serde_json::json!({ "text": "fix-login-flow" }),
        "the ACP route returns the §5.32 `{{ text }}` envelope, like the auggie route"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_acp_adapter_failure_is_internal_error() {
    // A RESOLVED adapter that dies before completing the turn is a hard
    // -32603 (§5.32), not `{ available: false }` — the unavailable result is
    // reserved for routing/resolution, and the reason is prefixed with the
    // provider id. The daemon reaps the child on this path.
    use std::os::unix::fs::PermissionsExt;
    let adapter_dir = test_tempdir("intentd-wss-acp-dead-");
    let bin = adapter_dir.path().join("codex-acp");
    std::fs::write(&bin, "#!/bin/sh\nexit 9\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("codex"));
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "codex": bin.to_string_lossy() }),
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":49,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 49);
    assert_eq!(resp["error"]["code"], -32603);
    let data = resp["error"]["data"].as_str().unwrap_or_default();
    assert!(data.starts_with("codex: "), "unexpected data: {data}");
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_host_provider_test_prompt_success_and_auth_required_paths() {
    // host.providerTestPrompt (§5.14) over the real wire, both terminal
    // shapes against the mock ACP fixture. A provider whose adapter answers
    // the live "say hello" turn is `{ ok: true }` and the cached
    // host.providerAuthStatus verdict is promoted to a hard `true`; an
    // adapter that rejects session/prompt with an auth-required JSON-RPC
    // error is `{ ok: false, reason: "auth-required" }` and the verdict is
    // demoted to `false` — observed here through non-forced
    // host.providerAuthStatus reads, which serve the cache before any probe.
    //
    // One sequential test (not two) because the daemon runs in-process and
    // the auth-verdict cache is process-global: two parallel tests promoting
    // and demoting the same provider would race each other's assertions.
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping providerTestPrompt e2e: node not on PATH");
        return;
    }
    let (_ok_dir, ok_bin) = fake_acp_adapter_script("test-prompt-ok", r#"{"response":"hello"}"#);
    let (_auth_dir, auth_bin) = fake_acp_adapter_script(
        "test-prompt-auth",
        r#"{"promptRpcError":{"code":-32000,"message":"Authentication required"}}"#,
    );
    let srv = start(WsOptions::default()).await;
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "codex": ok_bin.to_string_lossy() }),
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":70,"method":"host.providerTestPrompt","params":{"providerId":"codex"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 70);
    assert_eq!(
        resp["result"],
        serde_json::json!({ "ok": true }),
        "a live answered turn is a bare pass: {resp}"
    );
    let status = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":71,"method":"host.providerAuthStatus","params":{"providerId":"codex"}}"#,
    )
    .await;
    assert_eq!(
        status["result"]["providers"][0]["authenticated"], true,
        "a passed test prompt promotes the cached verdict: {status}"
    );

    // Same provider, now behind an adapter that rejects the prompt with the
    // claude-code auth-required shape (-32000 + auth-pattern message).
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "codex": auth_bin.to_string_lossy() }),
    );
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":72,"method":"host.providerTestPrompt","params":{"providerId":"codex"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 72);
    assert_eq!(resp["result"]["ok"], false);
    assert_eq!(
        resp["result"]["reason"], "auth-required",
        "the -32000/auth-pattern mapping from the spawn-feedback seam: {resp}"
    );
    assert!(
        resp["result"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("authentication required"),
        "message carries the adapter's error: {resp}"
    );
    let status = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":73,"method":"host.providerAuthStatus","params":{"providerId":"codex"}}"#,
    )
    .await;
    assert_eq!(
        status["result"]["providers"][0]["authenticated"], false,
        "an auth-required test prompt demotes the cached verdict: {status}"
    );
    srv.ws.stop().await;
}

/// Like [`fake_acp_adapter_script`] but the wrapper also points the fixture's
/// `MOCK_AGENT_SESSION_LOG` seam at `session_log`, so the test can read back
/// the `NODE_OPTIONS` the daemon handed the child (the fixture records it on
/// every `session/new`).
#[cfg(unix)]
fn fake_acp_adapter_script_with_session_log(
    tag: &str,
    behavior: &str,
    session_log: &Path,
) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let fixture = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let dir = test_tempdir(&format!("intentd-wss-acp-{tag}-"));
    let bin = dir.path().join("opencode");
    std::fs::write(
        &bin,
        format!(
            "#!/bin/sh\nMOCK_AGENT_BEHAVIOR='{behavior}' MOCK_AGENT_SESSION_LOG={:?} exec node {fixture:?} \"$@\"\n",
            session_log.to_string_lossy()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    (dir, bin)
}

#[cfg(unix)]
#[tokio::test]
async fn wss_acp_node_max_old_space_mb_setting_reaches_provider_test_prompt_child() {
    // `agents.acpNodeMaxOldSpaceMb` (§5.12, intent-hq/intent#4330) over the
    // real wire: the unset key reads back as `null` with the catalog default
    // (8192) on the definition, an out-of-range update is `-32602`, and an
    // in-range update lands as `NODE_OPTIONS=
    // --max-old-space-size=<MB>` on the NEXT provider child — observed here
    // via host.providerTestPrompt against a Node-runtime provider (opencode)
    // pinned to the mock fixture, which records its inherited NODE_OPTIONS on
    // `session/new`. This pins the wire shape end to end: `settings.get`
    // reports Number settings as floats, so the host-side reader must parse
    // `4096.0` — an integer-only read silently kept the default.
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping acpNodeMaxOldSpaceMb e2e: node not on PATH");
        return;
    }
    let log_dir = test_tempdir("intentd-wss-heapcap-log-");
    let session_log = log_dir.path().join("sessions.txt");
    let (_adapter_dir, bin) = fake_acp_adapter_script_with_session_log(
        "heap-cap",
        r#"{"response":"hello"}"#,
        &session_log,
    );
    let srv = start(WsOptions::default()).await;
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "opencode": bin.to_string_lossy() }),
    );

    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":80,"method":"settings.get","params":{"path":"agents.acpNodeMaxOldSpaceMb"}}"#,
    )
    .await;
    assert_eq!(got["id"], 80);
    assert_eq!(
        got["result"]["value"],
        Value::Null,
        "unset key reads null (the spawn path resolves the catalog default): {got}"
    );
    assert_eq!(got["result"]["origin"], "default", "{got}");
    assert_eq!(
        got["result"]["definition"]["defaultValue"],
        serde_json::json!(8192.0),
        "catalog default on the wire: {got}"
    );

    let rejected = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":81,"method":"settings.update","params":{"changes":[{"path":"agents.acpNodeMaxOldSpaceMb","value":512}]}}"#,
    )
    .await;
    assert_eq!(rejected["id"], 81);
    assert_eq!(
        rejected["error"]["code"], -32602,
        "below the 1024 MB floor is invalid params: {rejected}"
    );

    let applied = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":82,"method":"settings.update","params":{"changes":[{"path":"agents.acpNodeMaxOldSpaceMb","value":4096}]}}"#,
    )
    .await;
    assert_eq!(applied["id"], 82);
    assert_eq!(
        applied["result"]["applied"][0]["path"], "agents.acpNodeMaxOldSpaceMb",
        "applied: {applied}"
    );
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":83,"method":"settings.get","params":{"path":"agents.acpNodeMaxOldSpaceMb"}}"#,
    )
    .await;
    assert_eq!(
        got["result"]["value"],
        serde_json::json!(4096.0),
        "updated cap reads back as a float: {got}"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":84,"method":"host.providerTestPrompt","params":{"providerId":"opencode"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 84);
    assert_eq!(
        resp["result"],
        serde_json::json!({ "ok": true }),
        "the probe turn against the mock passes: {resp}"
    );

    let raw = std::fs::read_to_string(&session_log).expect("mock session log written");
    let lines: Vec<Value> = raw
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("session log line json"))
        .collect();
    assert_eq!(lines.len(), 1, "one session/new for the probe: {lines:?}");
    assert_eq!(lines[0]["method"], "session/new");
    // Both the env override and an inherited `NODE_OPTIONS` that already caps
    // the heap legitimately win over the setting (the daemon never clobbers a
    // parent cap), so the child-side assertion only holds in a clean env —
    // same guard as the intent-acp spawn tests.
    let parent_caps_heap = std::env::var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB").is_ok()
        || std::env::var("NODE_OPTIONS").is_ok_and(|v| v.contains("--max-old-space-size"));
    if parent_caps_heap {
        eprintln!(
            "skipping NODE_OPTIONS assertion: parent env already caps the heap ({:?})",
            lines[0]["nodeOptions"]
        );
    } else {
        let node_options = lines[0]["nodeOptions"]
            .as_str()
            .expect("probe child inherited NODE_OPTIONS");
        assert!(
            node_options.contains("--max-old-space-size=4096"),
            "probe child NODE_OPTIONS carries the configured cap, got {node_options:?}"
        );
    }
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_saturated_bound_returns_adapter_busy_and_queued_calls_complete() {
    // Adapters that hold their slot for ~10s before answering the turn, so the
    // bound is saturated for a wide, non-racy window. The wrapper records one
    // line per adapter actually launched: the assertions below count it rather
    // than inferring from the response which branch ran, so a queue timeout
    // that quietly spawned (or a `-32603` arriving from some unrelated
    // failure) cannot pass as a bound that held.
    use std::os::unix::fs::PermissionsExt;
    // The daemon-wide ephemeral-adapter bound over the real wire (§5.32,
    // monorepo#2062). With the bound saturated by parked adapters, a call
    // whose own `timeoutMs` expires while queued comes back as -32603 with
    // OBJECT-shaped `error.data` — `{ code: "adapter-busy", provider,
    // waitedMs, limit }` — which is what distinguishes queueing pressure from
    // every other completeOnce failure on this method, all of which carry a
    // bare STRING `data` (see the adapter-failure test above). The parked
    // callers then finish normally: the bound queues work, it does not shed
    // it.
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping adapter-busy e2e: node not on PATH");
        return;
    }
    let adapter_dir = test_tempdir("intentd-wss-acp-busy-");
    let spawn_log = adapter_dir.path().join("spawns.log");
    let bin = adapter_dir.path().join("codex-acp");
    let fixture = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::write(
        &bin,
        format!(
            "#!/bin/sh\necho $$ >> {log:?}\n\
             MOCK_AGENT_BEHAVIOR='{{\"firstTurnDelayMs\":10000,\"response\":\"🤖\\nparked-reply\"}}' \
             exec node {fixture:?} \"$@\"\n",
            log = spawn_log.to_string_lossy(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let spawned_count = || -> usize {
        std::fs::read_to_string(&spawn_log)
            .map_or(0, |s| s.lines().filter(|l| !l.trim().is_empty()).count())
    };
    let spawned = |what: &str| -> usize {
        let n = spawned_count();
        eprintln!("adapter-busy e2e: {what}: {n} adapter(s) launched");
        n
    };
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("codex"));
    srv.set_setting(
        "providers.paths",
        serde_json::json!({ "codex": bin.to_string_lossy() }),
    );

    // The bound is a process-global installed once; ask for 1 and fill
    // whatever is actually in force, so this holds under any test runner.
    intent_services::init_adapter_slots(1);
    let limit = intent_services::adapter_slot_limit() as usize;

    let parked: Vec<_> = (0..limit)
        .map(|i| {
            let (port, cfg) = (srv.port, srv.cfg.clone());
            tokio::spawn(async move {
                wss_call(
                    port,
                    cfg,
                    &format!(
                        r#"{{"jsonrpc":"2.0","id":{},"method":"agent.completeOnce","params":{{"prompt":"park","timeoutMs":30000}}}}"#,
                        600 + i
                    ),
                )
                .await
            })
        })
        .collect();
    // Wait for the bound to be OBSERVABLY saturated rather than sleeping a
    // guessed interval: provider discovery before the spawn takes seconds on
    // some hosts, and a fixed sleep would probe the queue before the parked
    // calls hold their slots — the probe would then measure nothing and the
    // test would pass or fail on timing luck.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while spawned_count() < limit && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if spawned("bound saturated") != limit {
        // Name the actual failure instead of just "0 != 1": a parked call that
        // never launched an adapter has an answer (a gate/unavailable result,
        // or an error), and that answer is the diagnosis.
        let mut outcomes = Vec::new();
        for run in parked {
            outcomes.push(
                match tokio::time::timeout(std::time::Duration::from_secs(5), run).await {
                    Ok(Ok(v)) => v.to_string(),
                    Ok(Err(e)) => format!("<join error: {e}>"),
                    Err(_) => "<still in flight>".to_string(),
                },
            );
        }
        panic!(
            "the parked calls launched {} of {limit} adapters, so the queue was \
             never saturated and the probe below would measure nothing; parked \
             call outcomes: {outcomes:?}",
            spawned_count()
        );
    }

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":50,"method":"agent.completeOnce","params":{"prompt":"slug","timeoutMs":500}}"#,
    )
    .await;
    assert_eq!(resp["id"], 50);
    assert_eq!(
        resp["error"]["code"], -32603,
        "a queue timeout is an error, not a result: {resp}"
    );
    assert_eq!(
        resp["error"]["data"]["code"], "adapter-busy",
        "machine-readable discriminator so clients never match on prose: {resp}"
    );
    assert_eq!(resp["error"]["data"]["provider"], "codex");
    assert_eq!(resp["error"]["data"]["limit"], limit as u64);
    assert!(
        resp["error"]["data"]["waitedMs"].as_u64().unwrap_or(0) >= 400,
        "the caller waited out its own budget before giving up: {resp}"
    );
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("codex"),
        "human message names the provider: {resp}"
    );
    // The point of the bound: the rejected call spawned NOTHING, which is also
    // why retrying it is safe.
    assert_eq!(
        spawned("after the adapter-busy rejection"),
        limit,
        "a queue-timed-out call must not have launched an adapter: {resp}"
    );

    // Everything that held a slot still completes — queued, not shed.
    for (i, run) in parked.into_iter().enumerate() {
        let r = run.await.expect("parked call joins");
        assert_eq!(
            r["result"]["text"], "parked-reply",
            "parked one-shot #{i} must complete normally: {r}"
        );
    }
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_unavailable_when_adapter_unresolvable() {
    // The resolution tier of the gate: a one-shot-capable provider whose
    // adapter resolves to nothing (no binary, no npx for the pinned fallback
    // package) returns `{ available: false, reason }`, never an error.
    // Environment-gated — npx or an installed codex-acp both make the launch
    // resolvable, and neither can be hidden hermetically.
    if intent_providers::find_npx().is_some()
        || intent_providers::find_provider_binary("codex", "codex-acp", None).is_some()
    {
        eprintln!("skipping unresolvable-adapter e2e: npx or codex-acp is installed");
        return;
    }
    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("codex"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":50,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 50);
    assert_eq!(
        resp["result"],
        serde_json::json!({
            "available": false,
            "reason": "codex: no adapter could be resolved (binary not found and npx unavailable)"
        })
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_unavailable_when_provider_has_no_one_shot() {
    // Routing gate: claude-code / codex / pi run the ephemeral ACP route, but
    // a provider with no one-shot support returns a typed
    // `{ available: false, reason }` result instead of an error.
    let (_auggie_dir, bin) = fake_auggie_script("gated-complete", "printf '🤖\\nnever-runs\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("opencode"));
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
            "reason": "completeOnce is not supported for the resolved provider: opencode"
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
            "reason": "completeOnce requires a decidable effective default provider"
        }),
        "unset provider settings resolve the gate closed, not open via the positional fallback"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_gate_follows_model_default_provider() {
    // Gate precedence mirror of the enhance-prompt test: the effective
    // provider derives from `model.defaultProvider` alone — `model.default`
    // is a bare model id and never carries a provider.
    let (_auggie_dir, bin) = fake_auggie_script("prefix-complete", "printf '🤖\\nvia-prefix\\n'");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;

    // Direction 1: an opencode default provider (no one-shot route) → the
    // auggie CLI path is not taken, regardless of `model.default`.
    srv.set_setting("model.defaultProvider", serde_json::json!("opencode"));
    srv.set_setting("model.default", serde_json::json!("some-model"));
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
            "reason": "completeOnce is not supported for the resolved provider: opencode"
        }),
        "non-auggie model.defaultProvider routes away from the auggie CLI"
    );

    // Direction 2: an auggie default provider takes the CLI path. The
    // configured foreign bare model is dropped by the ownership guard, so
    // the CLI runs on its own default.
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":46,"method":"agent.completeOnce","params":{"prompt":"slug"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 46);
    assert_eq!(
        resp["result"]["text"], "via-prefix",
        "auggie model.defaultProvider takes the CLI path"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_resolves_quick_action_settings() {
    // monorepo#1734: over the wire, a `agent.completeOnce` call with no
    // explicit `model` picks up the user's quick-action settings —
    // `quickActions.typeOverrides[type]` first, then
    // `quickActions.defaultModel` — while an explicit `model` still wins.
    // The fixture CLI echoes its own argv so the resolved `--model` is
    // observable in the `{ text }` envelope.
    let (_auggie_dir, bin) = fake_auggie_script("quick-actions", "printf '🤖\\n%s\\n' \"$*\"");
    let srv = start_with_auggie(WsOptions::default(), Some(bin)).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    srv.set_setting("quickActions.defaultModel", serde_json::json!("sonnet4.5"));
    srv.set_setting(
        "quickActions.typeOverrides",
        serde_json::json!({ "commit": "haiku4.5" }),
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":49,"method":"agent.completeOnce","params":{"prompt":"msg","type":"commit"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 49);
    let text = resp["result"]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("--model haiku4.5"),
        "the commit type override must be resolved daemon-side, got {resp}"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":50,"method":"agent.completeOnce","params":{"prompt":"msg","type":"pr"}}"#,
    )
    .await;
    let text = resp["result"]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("--model sonnet4.5"),
        "an unset override falls through to quickActions.defaultModel, got {resp}"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":51,"method":"agent.completeOnce","params":{"prompt":"msg","type":"commit","model":"opus4.7"}}"#,
    )
    .await;
    let text = resp["result"]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("--model opus4.7"),
        "an explicit model outranks the quick-action settings, got {resp}"
    );
    srv.ws.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn wss_agent_complete_once_legacy_compound_quick_action_routes_to_its_provider() {
    // A user-authored `quickActions.defaultModel = "codex:gpt-5"` (legacy
    // compound; the wire rejects compounds but user files are never
    // rejected) splits on read into a (codex, gpt-5) pair and routes the
    // one-shot over the ephemeral ACP route — with `model.defaultProvider`
    // left UNSET, proving a decidable compound rung opens the gate on its
    // own instead of dying on the closed-gate path.
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping legacy compound quick-action e2e: node not on PATH");
        return;
    }
    let (_adapter_dir, bin) =
        fake_acp_adapter_script("compound-quick", r#"{"response":"🤖\ncompound-routed"}"#);
    let srv = start(WsOptions::default()).await;
    // Seed via reload() — the live-reload watcher path for an externally
    // edited config.toml — since settings.update rejects compound values.
    srv.registry
        .reload(&format!(
            "[providers.paths]\ncodex = {:?}\n\n[quickActions]\ndefaultModel = \"codex:gpt-5\"\n",
            bin.to_string_lossy()
        ))
        .expect("reload legacy config");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":52,"method":"agent.completeOnce","params":{"prompt":"msg"}}"#,
    )
    .await;
    assert_eq!(resp["id"], 52);
    assert_eq!(
        resp["result"],
        serde_json::json!({ "text": "compound-routed" }),
        "a legacy compound quick-action value must route to its own provider"
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    assert!(
        !h["prettyHostname"]
            .as_str()
            .expect("prettyHostname is string")
            .is_empty(),
        "prettyHostname non-empty: {h}"
    );
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
async fn wss_host_check_node_and_check_gh_answered_on_wss() {
    // host.checkNode / host.checkGh (§5.14, protocol 6.4) ride the same
    // cross-transport host.* fast-path as host.checkGit: always answered on
    // WSS with `{ available: false }` or `{ available: true, version, path }`
    // — never an RPC error.
    let srv = start(WsOptions::default()).await;
    for (id, frame) in [
        (7, r#"{"jsonrpc":"2.0","id":7,"method":"host.checkNode"}"#),
        (8, r#"{"jsonrpc":"2.0","id":8,"method":"host.checkGh"}"#),
    ] {
        let resp = wss_call(srv.port, srv.cfg.clone(), frame).await;
        assert_eq!(resp["id"], id);
        assert_eq!(resp["jsonrpc"], "2.0");
        assert!(resp.get("error").is_none(), "never an RPC error");
        let r = &resp["result"];
        assert!(r["available"].is_boolean(), "available is always present");
        if r["available"] == true {
            assert!(r["version"].is_string());
            assert!(r["path"].is_string());
        } else {
            assert!(r.get("version").is_none());
            assert!(r.get("path").is_none());
        }
    }
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
    let hog = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let base = hog.local_addr().unwrap().port();
    let (api, bus, _store, _registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let mut opts = WsOptions {
        base_port: base,
        ..WsOptions::default()
    };
    opts.bind_addresses = vec![Ipv4Addr::LOCALHOST.into()];
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
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
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
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
            Some(Ok(_)) => {}
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
    const MAX_ATTEMPTS: u32 = 10;
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
    for _ in 0..MAX_ATTEMPTS {
        let fixed_port = free_port();
        let opts = WsOptions {
            base_port: fixed_port,
            bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
            ..WsOptions::default()
        };
        let ws = WsApiServer::new(
            api.clone(),
            bus.clone(),
            &tls,
            &token_store.clone(),
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
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {}
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

/// Companion to [`heartbeat_terminates_silent_client`] and regression guard
/// for intent-hq/intent#3712: a client that keeps polling its stream (and so
/// auto-pongs every server ping) must SURVIVE many heartbeat interval+timeout
/// cycles. This would catch a mixed-clock regression in the pong bookkeeping —
/// e.g. storing wall-clock ms on pong receipt while the reaper compares
/// against monotonic ms (or vice versa) — which makes even responsive clients
/// look stale and get reaped within one timeout window.
#[tokio::test]
async fn heartbeat_keeps_responsive_client_alive() {
    let srv = start(WsOptions {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(200),
        ..WsOptions::default()
    })
    .await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    // Keep the stream polled so tungstenite answers each Ping with a Pong.
    let poller = tokio::spawn(async move { while let Some(Ok(_)) = ws.next().await {} });
    for _ in 0..50 {
        if srv.ws.client_count() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(srv.ws.client_count(), 1, "client should register");
    // Outlive many timeout windows (2s >> 200ms timeout, >= 20 ping cycles).
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        srv.ws.client_count(),
        1,
        "responsive client must survive heartbeat cycles"
    );
    poller.abort();
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
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
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

/// `task.list` over the real WSS wire returns `{ tasks, stats }`: `tasks`
/// membership is workspace-wide (every task note except the spec — direct
/// spec children, subtasks, and unlinked tasks alike), each row carrying the
/// `specLinked` flag (true iff the id appears in the spec body's
/// `intent://local/task/{id}` links), while the `stats` aggregate stays the
/// spec-linked direct-child rollup mirroring the FE `computeTaskStats`
/// (`task-stats.ts`) classification: `total` excludes `cancelled`,
/// `completed` counts `complete`, and `inProgress` counts `in_progress` +
/// `review_required`. The optional `status` filter narrows `tasks` only —
/// `stats` stays the unfiltered rollup so the FE renders the progress bar
/// verbatim regardless of the active filter (PROTOCOL §5.4).
#[tokio::test]
async fn wss_task_list_emits_stats_aggregate() {
    let srv = start(WsOptions::default()).await;

    // Seed a workspace + spec note + task notes directly through the
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
    // An unlinked task and a subtask (child of task-a) — both returned with
    // `specLinked: false`, both excluded from the spec-linked `stats` rollup.
    let mut orphan = mk_task("task-x", "Orphan", TaskStatus::NotStarted);
    orphan.parent_id = None;
    let mut sub = mk_task("task-sub", "Subtask", TaskStatus::InProgress);
    sub.parent_id = Some(NoteId::from("task-a"));
    for n in [
        mk_task("task-a", "Alpha", TaskStatus::InProgress),
        mk_task("task-b", "Beta", TaskStatus::Complete),
        mk_task("task-c", "Gamma", TaskStatus::ReviewRequired),
        mk_task("task-d", "Delta", TaskStatus::Cancelled),
        orphan,
        sub,
    ] {
        srv.store.insert_note(&n).await.expect("insert task note");
    }

    // Unfiltered: returns all six task notes (cancelled, unlinked, and
    // subtask included) and a `stats` rollup over the spec-linked set only,
    // where `total` excludes the cancelled task and `inProgress` counts both
    // in_progress + review_required.
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
    assert_eq!(
        task_ids,
        vec!["task-a", "task-b", "task-c", "task-d", "task-sub", "task-x"]
    );
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
    // `specLinked` is always serialized: true for spec-linked ids, false for
    // the unlinked task and the subtask.
    for id in ["task-a", "task-b", "task-c", "task-d"] {
        assert_eq!(by_id[id]["specLinked"], true, "{id} is spec-linked");
    }
    for id in ["task-x", "task-sub"] {
        assert_eq!(by_id[id]["specLinked"], false, "{id} is not spec-linked");
    }
    // `parentId` rides along (omitted when the note has no parent), so the
    // subtask is distinguishable from the unlinked top-level task.
    assert_eq!(by_id["task-a"]["parentId"], "spec");
    assert_eq!(by_id["task-sub"]["parentId"], "task-a");
    assert!(
        by_id["task-x"].get("parentId").is_none(),
        "parentId omitted for parentless notes: {}",
        by_id["task-x"]
    );

    let stats = &result["stats"];
    assert_eq!(stats["total"], 3, "total excludes cancelled: {stats}");
    assert_eq!(stats["completed"], 1, "completed = 1 complete: {stats}");
    assert_eq!(
        stats["inProgress"], 2,
        "inProgress = spec-linked in_progress + review_required: {stats}"
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
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
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
                Some(Ok(_)) => {}
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
        // Strip inherited GIT_AUTHOR_*/GIT_COMMITTER_* so the repo-local
        // user.* config below wins (env outranks config; monorepo#4191).
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .env_remove("GIT_AUTHOR_NAME")
            .env_remove("GIT_AUTHOR_EMAIL")
            .env_remove("GIT_COMMITTER_NAME")
            .env_remove("GIT_COMMITTER_EMAIL")
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

/// `gitRoot.list` + the `gitRootId` param on the git reads over WSS
/// (monorepo#2053): a registered secondary git root (a nested repo inside the
/// workspace worktree) appears in `gitRoot.list` with its live-read branch,
/// `git.status`/`git.changes` scoped by `gitRootId` target the nested repo
/// instead of the workspace worktree, and an unknown `gitRootId` is `-32602`.
#[tokio::test]
async fn wss_git_root_list_and_scoped_reads_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed the primary repo with one commit and a nested repo inside it.
    let repo_dir = test_tempdir("intentd-wssgitroot-");
    let repo = repo_dir.path().to_path_buf();
    let nested = repo.join("vendor/nested");
    std::fs::create_dir_all(&nested).unwrap();
    let git = |dir: &std::path::PathBuf, args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    for dir in [&repo, &nested] {
        git(dir, &["init", "-q"]);
        git(dir, &["config", "user.name", "Test"]);
        git(dir, &["config", "user.email", "test@example.com"]);
        git(dir, &["config", "commit.gpgsign", "false"]);
        std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
        git(dir, &["add", "seed.txt"]);
        git(dir, &["commit", "-q", "-m", "seed"]);
    }
    // A second commit unique to the nested repo — the identically-seeded
    // repos can otherwise produce colliding seed-commit hashes, which would
    // defeat the scoped-vs-unscoped `git.commitDetails` proof below.
    std::fs::write(nested.join("nested-second.txt"), "nested\n").unwrap();
    git(&nested, &["add", "nested-second.txt"]);
    git(&nested, &["commit", "-q", "-m", "nested-second"]);
    // An untracked file only the nested repo can see.
    std::fs::write(nested.join("root-only.txt"), "hi\n").unwrap();

    // Create a workspace pointing at the primary repo.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS gitRoot WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // No roots registered yet → empty list.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"gitRoot.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["gitRoots"], serde_json::json!([]));

    // Register the nested repo as a git root directly through the store (the
    // agent-facing `ws.git.registerRoot` MCP binding lands separately).
    let ts = now_iso();
    let root = intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: WorkspaceId::from(ws_id.as_str()),
        path: nested.to_string_lossy().into_owned(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![intent_core::AgentId::from("agent-1")],
        registered_commit_sha: Some("feedfacefeedfacefeedfacefeedfacefeedface".into()),
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: ts.clone(),
        updated_at: ts,
    };
    srv.store
        .upsert_workspace_git_root(&root)
        .await
        .expect("register root");

    // gitRoot.list returns the root with its live-read branch + attribution.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"gitRoot.list","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let roots = resp["result"]["gitRoots"].as_array().expect("gitRoots");
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["id"], root.id.as_str());
    assert_eq!(roots[0]["path"], root.path);
    assert_eq!(roots[0]["source"], "agent");
    assert_eq!(
        roots[0]["registeredByAgentIds"],
        serde_json::json!(["agent-1"])
    );
    assert_eq!(
        roots[0]["registeredCommitSha"],
        "feedfacefeedfacefeedfacefeedfacefeedface"
    );
    assert!(
        roots[0]["branch"].as_str().is_some_and(|b| !b.is_empty()),
        "live-read branch present: {:?}",
        roots[0]
    );

    // git.status scoped by gitRootId sees the nested repo's untracked file.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.status","params":{{"workspaceId":"{ws_id}","gitRootId":"{}"}}}}"#,
            root.id.as_str()
        ),
    )
    .await;
    let files = resp["result"]["files"].as_array().expect("files");
    assert!(
        files.iter().any(|f| f["path"] == "root-only.txt"),
        "nested repo scan: {files:?}"
    );

    // git.changes scoped by gitRootId projects the same nested file list.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"git.changes","params":{{"workspaceId":"{ws_id}","gitRootId":"{}"}}}}"#,
            root.id.as_str()
        ),
    )
    .await;
    let changes = resp["result"].as_array().expect("changes array");
    assert!(changes.iter().any(|c| c["path"] == "root-only.txt"));

    // The unscoped reads still target the primary worktree.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"git.changes","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let changes = resp["result"].as_array().expect("changes array");
    assert!(
        !changes.iter().any(|c| c["path"] == "root-only.txt"),
        "primary scan unaffected: {changes:?}"
    );

    // Unknown gitRootId → -32602 (PROTOCOL §9).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"git.status","params":{{"workspaceId":"{ws_id}","gitRootId":"nope"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    // git.commitDetails scoped by gitRootId resolves hashes in the nested
    // repo's odb (monorepo#2477).
    let nested_head = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(&nested)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("rev-parse")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"git.commitDetails","params":{{"workspaceId":"{ws_id}","commitHash":"{nested_head}","gitRootId":"{}"}}}}"#,
            root.id.as_str()
        ),
    )
    .await;
    assert_eq!(
        resp["result"]["commitHash"],
        Value::from(nested_head.clone())
    );
    assert_eq!(resp["result"]["message"], "nested-second");
    assert_eq!(
        resp["result"]["files"],
        serde_json::json!(["nested-second.txt"])
    );

    // The unscoped read cannot resolve the nested repo's hash — the empty
    // envelope, not an error.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"git.commitDetails","params":{{"workspaceId":"{ws_id}","commitHash":"{nested_head}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["files"], serde_json::json!([]));

    // Unknown gitRootId on git.commitDetails → -32602 (never an empty fallback).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"git.commitDetails","params":{{"workspaceId":"{ws_id}","commitHash":"{nested_head}","gitRootId":"nope"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    srv.ws.stop().await;
}

/// `workspace.localChanges` over WSS (§5.1): a workspace whose primary
/// worktree has a never-pushed commit and an untracked file, plus a registered
/// secondary root, answers `{ roots, hasUnpushedCommits, hasUncommittedChanges }`
/// with the primary row first and the secondary row carrying its `gitRootId`;
/// a missing `workspaceId` and an unknown workspace are both `-32602`.
#[tokio::test]
async fn wss_workspace_local_changes_round_trip() {
    let srv = start(WsOptions::default()).await;

    let repo_dir = test_tempdir("intentd-wsslocalchanges-");
    let repo = repo_dir.path().to_path_buf();
    let nested = repo.join("vendor/nested");
    std::fs::create_dir_all(&nested).unwrap();
    let git = |dir: &std::path::PathBuf, args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    for dir in [&repo, &nested] {
        git(dir, &["init", "-q"]);
        git(dir, &["config", "user.name", "Test"]);
        git(dir, &["config", "user.email", "test@example.com"]);
        git(dir, &["config", "commit.gpgsign", "false"]);
        std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
        git(dir, &["add", "seed.txt"]);
        git(dir, &["commit", "-q", "-m", "seed"]);
    }
    // Primary: one untracked file. Nested: clean worktree.
    std::fs::write(repo.join("scratch.txt"), "wip\n").unwrap();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS localChanges WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();
    let primary_path = created["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath")
        .to_string();

    let ts = now_iso();
    let root = intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: WorkspaceId::from(ws_id.as_str()),
        path: nested.to_string_lossy().into_owned(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![intent_core::AgentId::from("agent-1")],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: ts.clone(),
        updated_at: ts,
    };
    srv.store
        .upsert_workspace_git_root(&root)
        .await
        .expect("register root");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.localChanges","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 2);
    let result = &resp["result"];
    assert_eq!(result["hasUnpushedCommits"], true, "{resp}");
    assert_eq!(result["hasUncommittedChanges"], true, "{resp}");
    let roots = result["roots"].as_array().expect("roots");
    assert_eq!(roots.len(), 2, "{resp}");

    assert_eq!(roots[0]["kind"], "primary");
    assert!(roots[0].get("gitRootId").is_none(), "{resp}");
    assert_eq!(roots[0]["path"], primary_path);
    assert!(roots[0]["branch"].as_str().is_some_and(|b| !b.is_empty()));
    assert_eq!(roots[0]["hasRemoteRefs"], false);
    assert_eq!(roots[0]["unpushedCount"], 1);
    assert_eq!(roots[0]["uncommittedCount"], 1);
    assert!(roots[0].get("error").is_none(), "{resp}");

    assert_eq!(roots[1]["kind"], "secondary");
    assert_eq!(roots[1]["gitRootId"], root.id.as_str());
    assert_eq!(roots[1]["path"], root.path);
    assert_eq!(roots[1]["hasRemoteRefs"], false);
    assert_eq!(roots[1]["unpushedCount"], 1);
    assert_eq!(roots[1]["uncommittedCount"], 0);
    assert!(roots[1].get("error").is_none(), "{resp}");

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":3,"method":"workspace.localChanges","params":{}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":4,"method":"workspace.localChanges","params":{"workspaceId":"nope"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);

    srv.ws.stop().await;
}

/// `git.agentCommit` targeted at a registered secondary git root over WSS
/// (monorepo#2053 follow-up): with `gitRootId` + an explicit `files` list the
/// commit lands in the nested repo (its HEAD advances, the primary repo is
/// untouched), the response carries the §5.6 `{ ok, hash, files, fileCount }`
/// envelope, and an unknown `gitRootId` is `-32602` with the same message as
/// the root-scoped reads.
#[tokio::test]
async fn wss_git_agent_commit_in_registered_root_round_trip() {
    let srv = start(WsOptions::default()).await;

    // Seed the primary repo with one commit and a nested repo inside it.
    let repo_dir = test_tempdir("intentd-wssrootcommit-");
    let repo = repo_dir.path().to_path_buf();
    let nested = repo.join("vendor/nested");
    std::fs::create_dir_all(&nested).unwrap();
    let git = |dir: &std::path::PathBuf, args: &[&str]| {
        let ok = std::process::Command::new("git")
            .current_dir(dir)
            .args(args)
            .status()
            .expect("run git")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    let rev_parse_head = |dir: &std::path::PathBuf| -> String {
        String::from_utf8(
            std::process::Command::new("git")
                .current_dir(dir)
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("rev-parse")
                .stdout,
        )
        .expect("utf8")
        .trim()
        .to_string()
    };
    for dir in [&repo, &nested] {
        git(dir, &["init", "-q"]);
        git(dir, &["config", "user.name", "Test"]);
        git(dir, &["config", "user.email", "test@example.com"]);
        git(dir, &["config", "commit.gpgsign", "false"]);
        std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
        git(dir, &["add", "seed.txt"]);
        git(dir, &["commit", "-q", "-m", "seed"]);
    }

    // Create a workspace pointing at the primary repo.
    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS rootCommit WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // Register the nested repo as a git root directly through the store.
    let ts = now_iso();
    let root = intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: WorkspaceId::from(ws_id.as_str()),
        path: nested.to_string_lossy().into_owned(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![intent_core::AgentId::from("agent-1")],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: ts.clone(),
        updated_at: ts,
    };
    srv.store
        .upsert_workspace_git_root(&root)
        .await
        .expect("register root");

    // Dirty the nested repo, then commit it through the workspace with
    // `gitRootId` + an explicit `files` list.
    std::fs::write(nested.join("root-change.txt"), "hi\n").unwrap();
    let nested_head_before = rev_parse_head(&nested);
    let primary_head_before = rev_parse_head(&repo);

    // Subscribe on a persistent connection FIRST so the commit's `git:commit`
    // and `changes:git-status` notifications (§6.5) are delivered to this
    // client and their additive `gitRootId` can be asserted on the wire.
    let mut sub_ws = connect_ws(srv.port, srv.cfg.clone()).await;
    sub_ws
        .send(Message::Text(
            format!(
                r#"{{"jsonrpc":"2.0","id":10,"method":"events.subscribe","params":{{"eventTypes":["git:commit","changes:git-status"],"workspaceId":"{ws_id}"}}}}"#
            )
            .into(),
        ))
        .await
        .expect("send subscribe");
    let sub_resp = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match sub_ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["id"] == 10 {
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub_ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("subscribe response timed out");
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribe: {sub_resp}"
    );

    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.agentCommit","params":{{"workspaceId":"{ws_id}","message":"root commit","files":["root-change.txt"],"gitRootId":"{}"}}}}"#,
            root.id.as_str()
        ),
    )
    .await;
    // Response envelope shape (§5.6): { ok, hash, files, fileCount }.
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 2);
    assert_eq!(resp["result"]["ok"], serde_json::json!(true));
    assert_eq!(
        resp["result"]["files"],
        serde_json::json!(["root-change.txt"])
    );
    assert_eq!(resp["result"]["fileCount"], serde_json::json!(1));
    let hash = resp["result"]["hash"].as_str().expect("commit hash");

    // The commit landed in the nested repo: HEAD advanced to the reported
    // hash with the requested message.
    let nested_head_after = rev_parse_head(&nested);
    assert_ne!(
        nested_head_after, nested_head_before,
        "nested HEAD advanced"
    );
    assert_eq!(nested_head_after, hash, "reported hash is the nested HEAD");

    // The subscriber sees both notifications, each carrying the additive
    // `gitRootId`, and the status payload is read from the target root (the
    // nested repo is clean after the commit).
    let mut commit_evt = None;
    let mut status_evt = None;
    tokio::time::timeout(Duration::from_secs(10), async {
        while commit_evt.is_none() || status_evt.is_none() {
            match sub_ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event" {
                        match v["params"]["event"]["type"].as_str() {
                            Some("git:commit") => commit_evt = Some(v["params"]["event"].clone()),
                            Some("changes:git-status") => {
                                status_evt = Some(v["params"]["event"].clone());
                            }
                            _ => {}
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub_ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for git:commit / changes:git-status");
    let commit_evt = commit_evt.expect("git:commit event");
    let status_evt = status_evt.expect("changes:git-status event");
    assert_eq!(commit_evt["workspaceId"], ws_id.as_str());
    assert_eq!(commit_evt["data"]["gitRootId"], root.id.as_str());
    assert_eq!(commit_evt["data"]["commit"], hash);
    assert_eq!(commit_evt["data"]["message"], "root commit");
    assert_eq!(
        commit_evt["data"]["files"],
        serde_json::json!(["root-change.txt"])
    );
    assert_eq!(status_evt["workspaceId"], ws_id.as_str());
    assert_eq!(status_evt["data"]["gitRootId"], root.id.as_str());
    assert_eq!(status_evt["data"]["status"]["files"], serde_json::json!([]));
    let msg = String::from_utf8(
        std::process::Command::new("git")
            .current_dir(&nested)
            .args(["log", "-1", "--format=%s"])
            .output()
            .expect("git log")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    assert_eq!(msg, "root commit");

    // The primary repo is untouched: HEAD unchanged, nothing staged.
    assert_eq!(rev_parse_head(&repo), primary_head_before);
    let staged = std::process::Command::new("git")
        .current_dir(&repo)
        .args(["diff", "--cached", "--name-only"])
        .output()
        .expect("git diff");
    assert!(
        staged.stdout.is_empty(),
        "primary index untouched: {:?}",
        String::from_utf8_lossy(&staged.stdout)
    );

    // Unknown gitRootId → -32602 with the same message as the scoped reads,
    // and no commit lands anywhere.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.agentCommit","params":{{"workspaceId":"{ws_id}","message":"nope","files":["root-change.txt"],"gitRootId":"nope"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["message"],
        serde_json::json!("invalid params: Unknown git root: nope")
    );
    assert_eq!(rev_parse_head(&nested), nested_head_after);
    assert_eq!(rev_parse_head(&repo), primary_head_before);

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
        // Strip inherited GIT_AUTHOR_*/GIT_COMMITTER_* so the repo-local
        // user.* config below wins (env outranks config; monorepo#4191).
        let ok = std::process::Command::new("git")
            .current_dir(&repo)
            .env_remove("GIT_AUTHOR_NAME")
            .env_remove("GIT_AUTHOR_EMAIL")
            .env_remove("GIT_COMMITTER_NAME")
            .env_remove("GIT_COMMITTER_EMAIL")
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

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{ws_id}","limit":50}}}}"#
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
            r#"{{"jsonrpc":"2.0","id":3,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{ws_id}","limit":50,"includeOlder":true}}}}"#
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
            r#"{{"jsonrpc":"2.0","id":5,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{ws_id_unbounded}","limit":50}}}}"#
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
            r#"{{"jsonrpc":"2.0","id":7,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{ws_id_unresolvable}","limit":50}}}}"#
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
            r#"{{"jsonrpc":"2.0","id":8,"method":"file-tracking.loadCommits","params":{{"workspaceId":"{ws_id_unresolvable}","limit":50,"includeOlder":true}}}}"#
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
/// `BranchSelector` seam (§5.6). Drives the happy path (response shape parity
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

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
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
        assert_eq!(
            entry["v"].as_i64(),
            Some(i64::try_from(i).expect("value fits in i64") + 1)
        );
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

/// Regression for monorepo#3573 over the real WSS wire: a workspace whose
/// notes carry >1 MiB of content used to serialize a full-content
/// `note.list` frame past the 1 MiB outbound warn threshold.
/// `projection: "slim"` (§5.2, v8.1) serves bounded rows — `content`
/// replaced by `contentPreview` (500 chars) + `contentLength` — so the slim
/// frame stays far under the threshold, while the default (absent
/// projection) still round-trips the complete content for existing clients.
#[tokio::test]
async fn wss_note_list_slim_projection_bounds_large_frames() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Slim Notes"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // One giant note (~1.1 MiB) reproduces the oversized-list-frame report.
    let big = "x".repeat(1_100_000);
    let create_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "note.create",
        "params": { "workspaceId": ws_id, "title": "Giant", "content": big },
    });
    let sess = wss_session(srv.port, srv.cfg.clone(), vec![create_req.to_string()]).await;
    assert!(sess[0]["result"]["note"]["id"].is_string(), "{}", sess[0]);

    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"note.list","params":{{"workspaceId":"{ws_id}","projection":"slim"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"note.list","params":{{"workspaceId":"{ws_id}"}}}}"#
            ),
        ],
    )
    .await;

    // Slim: every row bounded, the giant note's content omitted, and the
    // whole response well under the 1 MiB warn threshold (< 256 KiB).
    let slim = &sess[0];
    let rows = slim["result"]["notes"].as_array().expect("notes array");
    let giant = rows
        .iter()
        .find(|n| n["title"] == "Giant")
        .expect("giant note listed");
    assert!(giant.get("content").is_none(), "slim omits content");
    assert_eq!(
        giant["contentPreview"].as_str().map(str::len),
        Some(500),
        "preview bounded"
    );
    assert_eq!(giant["contentLength"].as_i64(), Some(1_100_000));
    let frame = serde_json::to_string(slim).expect("serialize");
    assert!(
        frame.len() < 256 * 1024,
        "slim frame stays bounded: {} bytes",
        frame.len()
    );

    // Default (absent projection): full rows, complete content intact.
    let full = &sess[1];
    let rows = full["result"]["notes"].as_array().expect("notes array");
    let giant = rows
        .iter()
        .find(|n| n["title"] == "Giant")
        .expect("giant note listed");
    assert_eq!(giant["content"].as_str().map(str::len), Some(1_100_000));
    assert!(giant.get("contentPreview").is_none());

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
        assert_eq!(
            resp["id"].as_i64(),
            Some(i64::try_from(i).expect("value fits in i64") + 3),
            "envelope: {resp}"
        );
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

/// Regression for monorepo#4208 over the real WSS wire: writing the
/// line-numbered `note.read` display presentation (`   N | line`) back via
/// `note.setContent` must be rejected with -32602 and leave the note
/// untouched, while the same Markdown without the prefixes still persists.
#[tokio::test]
async fn wss_note_set_content_rejects_numbered_read_presentation() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Numbered Guard"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let seed = "# Spec\n\n## Goal\n- [ ] item";
    let numbered = "   1 | # Spec\n   2 | \n   3 | ## Goal\n   4 | - [ ] item";
    let raw_edit = "# Spec\n\n## Goal\n- [ ] item\n- [ ] new item";
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"note.create",
            "params":{"workspaceId":ws_id,"title":"Guarded","content":seed}})
            .to_string(),
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
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"note.setContent",
                "params":{"workspaceId":ws_id,"noteId":note_id,"content":numbered}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":4,"method":"note.get",
                "params":{"workspaceId":ws_id,"noteId":note_id}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":5,"method":"note.setContent",
                "params":{"workspaceId":ws_id,"noteId":note_id,"content":raw_edit}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":6,"method":"note.get",
                "params":{"workspaceId":ws_id,"noteId":note_id}})
            .to_string(),
        ],
    )
    .await;
    for (i, resp) in sess.iter().enumerate() {
        assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
        assert_eq!(
            resp["id"].as_i64(),
            Some(i64::try_from(i).expect("value fits in i64") + 3),
            "envelope: {resp}"
        );
    }
    assert_eq!(
        sess[0]["error"]["code"], -32602,
        "numbered presentation is rejected as invalid params: {}",
        sess[0]
    );
    let msg = sess[0]["error"]["message"].as_str().expect("error message");
    assert!(
        msg.contains("note.read") && msg.contains("rawContent"),
        "error names the remediation: {msg}"
    );
    assert_eq!(
        sess[1]["result"]["note"]["content"], seed,
        "rejected write leaves the note unchanged: {}",
        sess[1]
    );
    assert!(
        sess[2].get("error").is_none(),
        "raw Markdown write succeeds: {}",
        sess[2]
    );
    assert_eq!(
        sess[3]["result"]["note"]["content"], raw_edit,
        "raw Markdown persisted verbatim: {}",
        sess[3]
    );

    srv.ws.stop().await;
}

/// Regression for monorepo#4299 over the real WSS wire: `task.createPrerequisite`
/// materializes note content outside the `note.*` surface, so its `content`
/// must hit the same numbered-`note.read` guard — -32602, no child note
/// created — while raw Markdown still creates the prerequisite.
#[tokio::test]
async fn wss_task_create_prerequisite_rejects_numbered_read_presentation() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Prereq Numbered Guard"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let raw = "# Step\n\n- [ ] do it";
    let numbered = "   1 | # Step\n   2 | \n   3 | - [ ] do it";
    let sess = wss_session(
        srv.port,
        srv.cfg.clone(),
        vec![
            serde_json::json!({"jsonrpc":"2.0","id":2,"method":"task.createPrerequisite",
                "params":{"workspaceId":ws_id,"dependentNoteId":"spec","title":"Numbered Step","content":numbered}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":3,"method":"note.list",
                "params":{"workspaceId":ws_id}})
            .to_string(),
            serde_json::json!({"jsonrpc":"2.0","id":4,"method":"task.createPrerequisite",
                "params":{"workspaceId":ws_id,"dependentNoteId":"spec","title":"Raw Step","content":raw}})
            .to_string(),
        ],
    )
    .await;
    for (i, resp) in sess.iter().enumerate() {
        assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
        assert_eq!(
            resp["id"].as_i64(),
            Some(i64::try_from(i).expect("value fits in i64") + 2),
            "envelope: {resp}"
        );
    }
    assert_eq!(
        sess[0]["error"]["code"], -32602,
        "numbered presentation is rejected as invalid params: {}",
        sess[0]
    );
    let msg = sess[0]["error"]["message"].as_str().expect("error message");
    assert!(
        msg.contains("note.read") && msg.contains("rawContent"),
        "error names the remediation: {msg}"
    );
    let rows = sess[1]["result"]["notes"].as_array().expect("notes array");
    assert!(
        rows.iter().all(|n| n["title"] != "Numbered Step"),
        "rejected createPrerequisite must persist no child note: {}",
        sess[1]
    );
    assert!(
        sess[2].get("error").is_none(),
        "raw Markdown content creates the prerequisite: {}",
        sess[2]
    );
    assert_eq!(sess[2]["result"]["ok"], true, "{}", sess[2]);
    assert_eq!(sess[2]["result"]["title"], "Raw Step", "{}", sess[2]);
    let child_id = sess[2]["result"]["prerequisiteNoteId"]
        .as_str()
        .expect("prerequisiteNoteId")
        .to_string();
    let got = wss_call(
        srv.port,
        srv.cfg.clone(),
        &serde_json::json!({"jsonrpc":"2.0","id":5,"method":"note.get",
            "params":{"workspaceId":ws_id,"noteId":child_id}})
        .to_string(),
    )
    .await;
    assert_eq!(
        got["result"]["note"]["content"], raw,
        "raw Markdown persisted verbatim: {got}"
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
    // Commit a gitlink (submodule pin) via plumbing — the pin commit need not
    // exist in this odb, exactly like a real submodule bump (monorepo#1739).
    let pin_sha = "7257a190564088376227525989c4994e46082ad1";
    git(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("160000,{pin_sha},sub"),
    ]);
    git(&["commit", "-q", "-m", "add gitlink"]);

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

    // Content at HEAD and at an earlier revision (HEAD~2 — before the
    // "second" edit; HEAD^ is the gitlink-less "second" commit).
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
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"seed.txt","ref":"HEAD~2"}}}}"#
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

    // Gitlink (submodule pin) path → typed -32602 with
    // `data = { code: "not-a-file", path, mode }` (monorepo#1739).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"git.showFile","params":{{"workspaceId":"{ws_id}","filePath":"sub","ref":"HEAD"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert_eq!(
        resp["error"]["data"],
        serde_json::json!({ "code": "not-a-file", "path": "sub", "mode": "160000" })
    );

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

/// Gitlink (submodule) wire shapes over WSS (monorepo#1739): `git.status`
/// carries the additive `mode`/`oldSha`/`newSha` fields on the gitlink entry
/// only, `git.changes` mirrors the same list, and `git.diffs` emits the
/// synthesized one-line `Subproject commit <sha>` pseudo-hunk for the staged
/// pin change.
#[tokio::test]
async fn wss_git_gitlink_status_and_diffs_wire_shape() {
    let srv = start(WsOptions::default()).await;

    let repo_dir = test_tempdir("intentd-wssgl-");
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
    // Commit a gitlink pin, then stage a bump to a new pin (both via
    // plumbing — the pin commits need not exist in this odb).
    let old_pin = "7257a190564088376227525989c4994e46082ad1";
    let new_pin = "7908777602d4e96f13c663c8a70a617163f38413";
    git(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("160000,{old_pin},sub"),
    ]);
    git(&["commit", "-q", "-m", "add gitlink"]);
    git(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("160000,{new_pin},sub"),
    ]);
    // A plain unstaged edit alongside, to assert regular files stay bare.
    std::fs::write(repo.join("seed.txt"), "seed\nedited\n").unwrap();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS gitlink WS","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("ws id")
        .to_string();

    // git.status: the gitlink entry carries mode/oldSha/newSha; the regular
    // file entry omits all three (additive, backward-compatible).
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.status","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let files = resp["result"]["files"].as_array().expect("files");
    let sub = files
        .iter()
        .find(|f| f["path"] == "sub")
        .expect("gitlink entry");
    assert_eq!(sub["status"], "M");
    assert_eq!(sub["staged"], true);
    assert_eq!(sub["mode"], "160000");
    assert_eq!(sub["oldSha"], old_pin);
    assert_eq!(sub["newSha"], new_pin);
    let seed = files
        .iter()
        .find(|f| f["path"] == "seed.txt")
        .expect("regular entry");
    for k in ["mode", "oldSha", "newSha"] {
        assert!(seed.get(k).is_none(), "regular file must omit {k}");
    }

    // git.changes mirrors the same list.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.changes","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let sub = resp["result"]
        .as_array()
        .expect("files")
        .iter()
        .find(|f| f["path"] == "sub")
        .cloned()
        .expect("gitlink entry");
    assert_eq!(sub["mode"], "160000");
    assert_eq!(sub["oldSha"], old_pin);
    assert_eq!(sub["newSha"], new_pin);

    // git.diffs (staged): the gitlink delta yields the synthesized
    // `Subproject commit <sha>` pseudo-hunk.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.diffs","params":{{"workspaceId":"{ws_id}","staged":true,"path":"sub"}}}}"#
        ),
    )
    .await;
    let entries = resp["result"].as_array().expect("diff entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["path"], "sub");
    let hunks = entries[0]["hunks"].as_array().expect("hunks");
    assert_eq!(hunks.len(), 1);
    assert_eq!(
        hunks[0],
        serde_json::json!({
            "oldStart": 1, "oldLines": 1, "newStart": 1, "newLines": 1,
            "lines": [
                { "type": "Deletion", "content": format!("Subproject commit {old_pin}\n"), "oldNumber": 1 },
                { "type": "Addition", "content": format!("Subproject commit {new_pin}\n"), "newNumber": 1 },
            ],
        })
    );

    srv.ws.stop().await;
}

/// `note.saveAsset` over WSS (PROTOCOL §5.2 — additive asset write): the write
/// returns `{ assetId, path, url }` and the asset round-trips back through
/// `note.readAsset`; a missing `data` param is -32602.
#[tokio::test]
#[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
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

/// An authenticated, fingerprint-pinned WSS client can serve the selected
/// screenshot reverse request and return its result over the same connection.
#[tokio::test]
async fn wss_browser_screenshot_reverse_round_trip() {
    let srv = start(WsOptions::default()).await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":1,"method":"browser.exec","params":{"actions":[{"action":"screenshot"}]}}"#
            .to_string()
            .into(),
    ))
    .await
    .expect("send browser.exec");

    let mut final_response = None;
    while final_response.is_none() {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("json frame");
                if frame["method"] == "browser.exec" {
                    assert_eq!(frame["params"]["actions"][0]["action"], "screenshot");
                    let reply = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": frame["id"],
                        "result": { "success": true, "results": [{ "success": true }] }
                    });
                    ws.send(Message::Text(reply.to_string().into()))
                        .await
                        .expect("send reverse reply");
                } else if frame["id"] == 1 {
                    final_response = Some(frame);
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await.expect("pong");
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let response = final_response.expect("browser.exec response");
    assert!(response.get("error").is_none(), "unexpected: {response}");
    assert_eq!(response["result"]["success"], true);
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_disconnect_wakes_accepted_screenshot_request() {
    let srv = start(WsOptions::default()).await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while srv.reverse_registry.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reverse registration");

    let reverse_registry = srv.reverse_registry.clone();
    let request = tokio::spawn(async move {
        reverse_registry
            .dispatch(
                "browser.exec",
                serde_json::json!({ "actions": [{ "action": "screenshot" }] }),
            )
            .await
    });
    let frame = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).expect("json frame");
                    if frame["method"] == "browser.exec" {
                        break frame;
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    ws.send(Message::Pong(payload)).await.expect("pong");
                }
                Some(Ok(_)) => {}
                other => panic!("expected reverse frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("accepted reverse frame");
    assert_eq!(frame["params"]["actions"][0]["action"], "screenshot");
    ws.send(Message::Close(None)).await.expect("close WSS");

    let err = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .expect("disconnect wakes request")
        .expect("join")
        .expect_err("request fails on disconnect");
    assert!(err.to_string().contains("closed"), "unexpected: {err}");
    srv.ws.stop().await;
}

#[tokio::test]
async fn wss_heartbeat_abort_wakes_accepted_screenshot_request() {
    let srv = start(WsOptions {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(300),
        ..WsOptions::default()
    })
    .await;
    let mut ws = connect_ws(srv.port, srv.cfg.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while srv.reverse_registry.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reverse registration");

    let reverse_registry = srv.reverse_registry.clone();
    let request = tokio::spawn(async move {
        reverse_registry
            .dispatch(
                "browser.exec",
                serde_json::json!({ "actions": [{ "action": "screenshot" }] }),
            )
            .await
    });
    let frame = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).expect("json frame");
                    if frame["method"] == "browser.exec" {
                        break frame;
                    }
                }
                Some(Ok(_)) => {}
                other => panic!("expected reverse frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("accepted reverse frame");
    assert_eq!(frame["params"]["actions"][0]["action"], "screenshot");

    let err = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .expect("heartbeat abort wakes request")
        .expect("join")
        .expect_err("request fails on heartbeat abort");
    assert!(err.to_string().contains("closed"), "unexpected: {err}");
    assert!(srv.reverse_registry.is_empty());
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
            Some(Ok(_)) => {}
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
/// thin-FE remediation (docs/protocol/methods/workspace.md §5.1): `workspace.duplicate`,
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
        .is_ok_and(|s| s.success())
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

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// monorepo#958 — the bounded agent read paths over the real WSS transport:
/// `agent.list` / `agent.get` (metadata + last-rows projection), a full
/// `agent.getConversation` multi-page `nextToken` walk plus the
/// `aroundMessageId` seek (centered page, `prevToken` walk newer, `-32602`
/// on an unknown id), the `aroundIndex` ordinal seek (same centered page,
/// out-of-range clamp, `-32602` on a negative index or when combined with
/// `aroundMessageId`), and the `chat.subscribe` seq-0 snapshot (standard,
/// resumed via `sinceMessageId`, and the unknown-id fallback), all against
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
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
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
    // Capture the id at seq 100 (inside the bounded newest page 70..=119) for
    // the `chat.subscribe` resume path below.
    let mut newest_message_id = String::new();
    let mut seq_100_message_id = String::new();
    for i in 0..120 {
        let (role, text) = if i % 2 == 0 {
            ("user", format!("prompt {i}"))
        } else {
            ("assistant", format!("reply {i}"))
        };
        newest_message_id = srv
            .store
            .append_agent_message(
                &agent,
                role,
                &json!([{ "type": "text", "text": text }]),
                &now_iso(),
            )
            .await
            .expect("append message")
            .id;
        if i == 100 {
            seq_100_message_id = newest_message_id.clone();
        }
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
    assert_eq!(
        lite["lastMessageId"].as_str(),
        Some(newest_message_id.as_str()),
        "lastMessageId is the newest seeded row's id: {lite}"
    );
    assert!(
        lite.get("messages").is_none(),
        "AgentLite carries no transcript: {lite}"
    );

    // lastMessageRole/lastMessageId on the wire for the awaiting-reply
    // shape: a second agent whose only message is the user's serves "user"
    // and that message's id; a fresh agent with no messages omits both
    // fields entirely.
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
    assert!(
        created2["result"]["agent"].get("lastMessageId").is_none(),
        "no messages yet: lastMessageId omitted: {created2}"
    );
    let user_only_msg = srv
        .store
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
    assert_eq!(
        lite2["lastMessageId"].as_str(),
        Some(user_only_msg.id.as_str()),
        "lastMessageId is the sole user message's id: {lite2}"
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

    // agent.getConversation ordinal seek (§5.5): `aroundIndex` returns the
    // page containing that 0-based ordinal from the oldest message — the
    // same centered split and dual cursors as `aroundMessageId`.
    let idx_seek = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":25,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"aroundIndex":60}}}}"#
        ),
    )
    .await;
    let result = &idx_seek["result"];
    let seqs: Vec<i64> = result["messages"]
        .as_array()
        .expect("ordinal seek messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(
        seqs,
        (55..=64).collect::<Vec<i64>>(),
        "ordinal 60 yields the same page as seeking its message id: {result}"
    );
    let idx_prev = result["prevToken"].as_str().expect("newer cursor");
    assert!(result["nextToken"].is_string(), "older cursor: {result}");

    // The ordinal seek's prevToken walks newer exactly like a message-id
    // seek's (seq 65..=74).
    let idx_newer = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":26,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"nextToken":"{idx_prev}"}}}}"#
        ),
    )
    .await;
    let seqs: Vec<i64> = idx_newer["result"]["messages"]
        .as_array()
        .expect("ordinal newer messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, (65..=74).collect::<Vec<i64>>());

    // Out-of-range ordinal clamps to the newest window (never an error).
    let idx_clamped = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":27,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"aroundIndex":999999}}}}"#
        ),
    )
    .await;
    let result = &idx_clamped["result"];
    let seqs: Vec<i64> = result["messages"]
        .as_array()
        .expect("clamped messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, (110..=119).collect::<Vec<i64>>());
    assert!(
        result["prevToken"].is_null(),
        "clamped to the live tail: {result}"
    );

    // An integer beyond i64::MAX is still a valid overshoot — it clamps to
    // the newest window like any other out-of-range estimate.
    let idx_huge = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":37,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","limit":10,"aroundIndex":18446744073709551615}}}}"#
        ),
    )
    .await;
    let result = &idx_huge["result"];
    let seqs: Vec<i64> = result["messages"]
        .as_array()
        .expect("u64-overshoot messages")
        .iter()
        .map(|m| m["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, (110..=119).collect::<Vec<i64>>());

    // Non-integer aroundIndex → -32602 naming the param.
    let frac_idx = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":38,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","aroundIndex":60.5}}}}"#
        ),
    )
    .await;
    assert_eq!(frac_idx["error"]["code"], -32602);
    assert!(
        frac_idx["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("aroundIndex"),
        "error names the param: {frac_idx}"
    );

    // Negative aroundIndex → -32602 (PROTOCOL §9).
    let neg_idx = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":28,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","aroundIndex":-1}}}}"#
        ),
    )
    .await;
    assert_eq!(neg_idx["error"]["code"], -32602);
    assert!(
        neg_idx["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("aroundIndex"),
        "error names the param: {neg_idx}"
    );

    // Both seek params together → -32602 naming the conflict.
    let both_seek = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":29,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","aroundMessageId":"{target_id}","aroundIndex":60}}}}"#
        ),
    )
    .await;
    assert_eq!(both_seek["error"]["code"], -32602);
    assert!(
        both_seek["error"]["message"]
            .as_str()
            .expect("error message")
            .contains("mutually exclusive"),
        "error names the conflict: {both_seek}"
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
            Some(Ok(_)) => {}
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
    assert!(
        snapshot.get("resumed").is_none(),
        "no sinceMessageId: snapshot carries no resumed key: {snapshot}"
    );
    drop(sub);

    // chat.subscribe resume (PROTOCOL §7.1): `sinceMessageId` inside the
    // bounded page yields only the messages AFTER it, `resumed: true`, and no
    // older-pages cursor (the client already holds everything up to the id).
    let resumed = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":40,"method":"chat.subscribe","params":{{"agentId":"{agent_id}","sinceMessageId":"{seq_100_message_id}"}}}}"#
        ),
        40,
    )
    .await;
    let msgs = resumed["messages"].as_array().expect("resumed messages");
    assert_eq!(
        msgs.len(),
        19,
        "only rows after seq 100 (101..=119): {resumed}"
    );
    assert_eq!(msgs[0]["seq"], 101);
    assert_eq!(msgs[18]["seq"], 119);
    assert_eq!(resumed["resumed"], true);
    assert_eq!(resumed["truncated"], false);
    assert!(resumed["nextToken"].is_null(), "no gap cursor: {resumed}");
    assert_eq!(
        resumed["totalMessages"], 120,
        "totalMessages stays the transcript-wide count"
    );

    // An unknown / pruned sinceMessageId falls back to the standard full
    // bounded page with `resumed: false` — the client must rehydrate.
    let fallback = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":41,"method":"chat.subscribe","params":{{"agentId":"{agent_id}","sinceMessageId":"msg-nope"}}}}"#
        ),
        41,
    )
    .await;
    let msgs = fallback["messages"].as_array().expect("fallback messages");
    assert_eq!(
        msgs.len(),
        50,
        "full bounded page on unknown id: {fallback}"
    );
    assert_eq!(msgs[0]["seq"], 70);
    assert_eq!(fallback["resumed"], false);
    assert_eq!(fallback["truncated"], true);
    assert!(
        fallback["nextToken"].as_str().is_some(),
        "fallback keeps the older-pages cursor"
    );

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

/// `agent.getConversation` / `chat.subscribe` slim projection over the real
/// WSS wire (§5.5, §7.1): slim bounds oversized `tool_use/tool_result`
/// bodies (additive `inputTruncated`/`outputTruncated` flags, pairing ids
/// intact) and swaps oversized image data for the write-time thumbnail
/// (`dataTruncated`/`dataIsThumbnail`/`dataBytes`); the seq-0 snapshot of a
/// slim subscription serves the same bounded blocks; an absent param serves
/// the same slim blocks (the v8.0 wire default); a bad value is `-32602`.
#[tokio::test]
async fn wss_conversation_slim_projection_bounds_blocks() {
    use base64::Engine as _;
    use intent_core::{AgentId, SLIM_PROJECTION_BUDGET_BYTES};
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Slim"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Slim"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // One message with an oversized tool pair, an under-budget tool pair,
    // and an oversized image (noise PNG defeats compression, so the write
    // path persists a real thumbnail for it).
    let big = "x".repeat(SLIM_PROJECTION_BUDGET_BYTES * 4);
    let img = image::RgbImage::from_fn(512, 384, |x, y| {
        let v = (x.wrapping_mul(31).wrapping_add(y.wrapping_mul(17)) % 251) as u8;
        image::Rgb([v, v.wrapping_add(97), v.wrapping_add(193)])
    });
    let mut buf = Vec::new();
    image::DynamicImage::ImageRgb8(img)
        .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
        .expect("encode test png");
    let img_b64 = base64::engine::general_purpose::STANDARD.encode(&buf);
    assert!(img_b64.len() > SLIM_PROJECTION_BUDGET_BYTES);
    let content = json!([
        { "type": "tool_use", "id": "m:0", "name": "view", "toolCallId": "tc-1",
          "input": { "path": "big.rs", "blob": big } },
        { "type": "tool_result", "id": "m:1", "tool_use_id": "tc-1",
          "output": big, "is_error": false },
        { "type": "tool_use", "id": "m:2", "name": "ls", "toolCallId": "tc-2",
          "input": { "path": "." } },
        { "type": "tool_result", "id": "m:3", "tool_use_id": "tc-2",
          "output": "ok", "is_error": false },
        { "type": "image", "id": "m:4", "data": img_b64, "mimeType": "image/png" },
    ]);
    srv.store
        .append_agent_message(&agent, "assistant", &content, &now_iso())
        .await
        .expect("append message");

    // Slim read: bounded bodies, additive flags, pairing intact.
    let slim = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","projection":"slim"}}}}"#
        ),
    )
    .await;
    let blocks = slim["result"]["messages"][0]["contentBlocks"]
        .as_array()
        .expect("slim blocks");
    let assert_slim_blocks = |blocks: &[Value], label: &str| {
        let tu = &blocks[0];
        assert_eq!(tu["inputTruncated"], true, "{label}: {tu}");
        assert_eq!(tu["name"], "view", "{label}");
        assert_eq!(tu["toolCallId"], "tc-1", "{label}");
        assert!(
            serde_json::to_string(&tu["input"]).unwrap().len()
                <= SLIM_PROJECTION_BUDGET_BYTES + 256,
            "{label}: input bounded"
        );
        let tr = &blocks[1];
        assert_eq!(tr["outputTruncated"], true, "{label}");
        assert_eq!(tr["tool_use_id"], "tc-1", "{label}");
        assert!(
            tr["output"].as_str().unwrap().len() <= SLIM_PROJECTION_BUDGET_BYTES,
            "{label}: output bounded"
        );
        assert_eq!(blocks[2], content[2], "{label}: under-budget untouched");
        assert_eq!(blocks[3], content[3], "{label}: under-budget untouched");
        let img_block = &blocks[4];
        assert_eq!(img_block["dataTruncated"], true, "{label}");
        assert_eq!(img_block["dataIsThumbnail"], true, "{label}");
        assert_eq!(
            usize::try_from(img_block["dataBytes"].as_u64().unwrap()).expect("value fits in usize"),
            img_b64.len(),
            "{label}"
        );
        let served = img_block["data"].as_str().expect("thumbnail data");
        assert!(served.len() < img_b64.len(), "{label}: thumbnail smaller");
    };
    assert_slim_blocks(blocks, "slim read");

    // Absent param: slim is the wire default since v8.0 — the same bounded
    // blocks are served, so no consumer can pull an unbudgeted transcript.
    let defaulted = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let default_blocks = defaulted["result"]["messages"][0]["contentBlocks"]
        .as_array()
        .expect("defaulted blocks");
    assert_slim_blocks(default_blocks, "absent-param read (slim default)");

    // chat.subscribe with the slim projection: the seq-0 snapshot serves the
    // same bounded blocks.
    let snap = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"chat.subscribe","params":{{"agentId":"{agent_id}","projection":"slim"}}}}"#
        ),
        5,
    )
    .await;
    let snap_blocks = snap["messages"][0]["contentBlocks"]
        .as_array()
        .expect("snapshot blocks");
    assert_slim_blocks(snap_blocks, "slim snapshot");

    // chat.subscribe with NO projection param: slim is the wire default
    // since v8.0 on the subscription snapshot too, not just the paged read.
    let default_snap = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"chat.subscribe","params":{{"agentId":"{agent_id}"}}}}"#
        ),
        8,
    )
    .await;
    let default_snap_blocks = default_snap["messages"][0]["contentBlocks"]
        .as_array()
        .expect("defaulted snapshot blocks");
    assert_slim_blocks(default_snap_blocks, "absent-param snapshot (slim default)");

    // A bad projection value is -32602 on both methods.
    let bad = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","projection":"tiny"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602);
    let bad_sub = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"chat.subscribe","params":{{"agentId":"{agent_id}","projection":"tiny"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad_sub["error"]["code"], -32602);

    // `includeInProgress` (§5.5, monorepo#3647): boolean values are
    // accepted — with no turn in flight both shapes match the opted-out
    // read — and a non-boolean is `-32602`.
    let opted_in = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","includeInProgress":true}}}}"#
        ),
    )
    .await;
    let opted_out = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","includeInProgress":false}}}}"#
        ),
    )
    .await;
    assert_eq!(
        opted_in["result"], opted_out["result"],
        "idle agent: includeInProgress adds nothing"
    );
    assert!(opted_in["result"]["messages"][0]["inProgress"].is_null());
    let bad_flag = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","includeInProgress":"yes"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad_flag["error"]["code"], -32602);

    srv.ws.stop().await;
}

/// Slim page byte budget over the real WSS wire (§5.5): a block-heavy
/// transcript (the observed 2.87MB-frame class: many messages × ~80 capped
/// tool blocks each) served with `projection: "slim"` stops early at the
/// ~512KB page budget — every frame stays bounded by budget + one message —
/// the `nextToken` walk reconstructs the full transcript with no gaps or
/// duplicates, and the `chat.subscribe` seq-0 snapshot (which reuses the
/// read) stays under the bound too. Slim is the wire default since v8.0,
/// so absent-projection reads get the same budget.
#[tokio::test]
async fn wss_slim_conversation_pages_are_byte_budgeted() {
    use intent_core::{AgentId, SLIM_PAGE_BUDGET_BYTES, SLIM_PROJECTION_BUDGET_BYTES};
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Budget"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Budget"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // 46 messages × 40 tool pairs (80 blocks) with oversized bodies: each
    // block caps at the ~2KB block budget, but 80 capped blocks × 46
    // messages ≈ 7MB+ of slim page weight — the exact class the page budget
    // exists to bound.
    let big = "x".repeat(SLIM_PROJECTION_BUDGET_BYTES * 2);
    for m in 0..46 {
        let mut blocks = Vec::new();
        for p in 0..40 {
            blocks.push(json!({
                "type": "tool_use", "id": format!("m{m}:{}", p * 2), "name": "view",
                "toolCallId": format!("tc-{m}-{p}"),
                "input": { "path": "big.rs", "blob": big },
            }));
            blocks.push(json!({
                "type": "tool_result", "id": format!("m{m}:{}", p * 2 + 1),
                "tool_use_id": format!("tc-{m}-{p}"), "output": big, "is_error": false,
            }));
        }
        srv.store
            .append_agent_message(&agent, "assistant", &json!(blocks), &now_iso())
            .await
            .expect("append block-heavy message");
    }

    // Slim nextToken walk: every frame bounded, sequence exact.
    let mut walked: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let token_field = match &token {
            Some(t) => format!(r#","nextToken":"{t}""#),
            None => String::new(),
        };
        let page = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getConversation","params":{{"agentId":"{agent_id}","projection":"slim"{token_field}}}}}"#
            ),
        )
        .await;
        let frame_bytes = serde_json::to_string(&page["result"]).unwrap().len();
        // One slim message here is ~80 blocks × ~2KB caps ≈ 170KB; allow
        // budget + one message of headroom, still far under the old
        // multi-MB frames and the transport's 1 MiB warn threshold.
        assert!(
            frame_bytes <= SLIM_PAGE_BUDGET_BYTES + 256 * 1024,
            "slim frame bounded by budget + one message: {frame_bytes}"
        );
        let msgs = page["result"]["messages"].as_array().expect("messages");
        assert!(!msgs.is_empty(), "budgeted pages are never empty");
        assert_eq!(page["result"]["totalMessages"], 46);
        let ids: Vec<String> = msgs
            .iter()
            .map(|m| m["id"].as_str().unwrap().to_string())
            .collect();
        walked.splice(0..0, ids);
        pages += 1;
        assert!(pages <= 46, "walk terminates");
        match page["result"]["nextToken"].as_str() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    assert!(pages > 1, "budget forced multiple pages: {pages}");
    assert_eq!(walked.len(), 46, "no gaps or duplicates in the walk");
    let unique: std::collections::HashSet<&String> = walked.iter().collect();
    assert_eq!(unique.len(), 46, "no duplicates in the walk");

    // The slim seq-0 chat snapshot reuses the budgeted read: bounded too.
    let snap = chat_subscribe_snapshot(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"chat.subscribe","params":{{"agentId":"{agent_id}","projection":"slim"}}}}"#
        ),
        4,
    )
    .await;
    let snap_bytes = serde_json::to_string(&snap).unwrap().len();
    assert!(
        snap_bytes <= SLIM_PAGE_BUDGET_BYTES + 256 * 1024,
        "slim seq-0 snapshot bounded: {snap_bytes}"
    );
    assert!(
        !snap["messages"].as_array().expect("messages").is_empty(),
        "snapshot serves the newest bounded page"
    );

    srv.ws.stop().await;
}

/// `agent.getMessageBlock` over the real WSS wire (§5.5): one FULL content
/// block by id — the on-demand counterpart of the slim projection. A
/// persisted assistant block id and a serve-time synthetic
/// `{messageId}:{index}` id both resolve; the returned block is the full,
/// unprojected body (no slim flags) even when a slim read truncated it;
/// unknown message/block ids are `-32602` naming the id; a workspace
/// mismatch and an unknown agent are not-found; missing required params are
/// `-32602`.
#[tokio::test]
async fn wss_agent_get_message_block_serves_full_block() {
    use intent_core::{AgentId, SLIM_PROJECTION_BUDGET_BYTES};
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Block"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Block"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // One row: an id-less text block (synthetic id at serve time) plus an
    // oversized tool_result with a persisted id (slim would truncate it).
    let big = "x".repeat(SLIM_PROJECTION_BUDGET_BYTES * 4);
    let content = json!([
        { "type": "text", "text": "hello" },
        { "type": "tool_result", "id": "blk-full", "tool_use_id": "tc-1",
          "output": big, "is_error": false },
    ]);
    let row_id = srv
        .store
        .append_agent_message(&agent, "user", &content, &now_iso())
        .await
        .expect("append message")
        .id;

    // Persisted block id: the full, unprojected body — no slim flags.
    let by_persisted = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"{row_id}","blockId":"blk-full","workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(by_persisted["jsonrpc"], "2.0");
    assert_eq!(by_persisted["id"], 3);
    let block = &by_persisted["result"]["block"];
    assert_eq!(block["output"].as_str().expect("full output"), big);
    assert!(
        block.get("outputTruncated").is_none(),
        "no slim flags on the full block: {block}"
    );
    assert_eq!(block, &content[1], "byte-identical to the stored block");

    // Synthetic block id: `{messageId}:0` resolves the id-less text block,
    // served with the synthetic id stamped (matching agent.getConversation).
    let by_synthetic = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"{row_id}","blockId":"{row_id}:0"}}}}"#
        ),
    )
    .await;
    let block = &by_synthetic["result"]["block"];
    assert_eq!(block["text"], "hello");
    assert_eq!(block["id"].as_str(), Some(format!("{row_id}:0").as_str()));

    // Unknown message id / unknown block id: -32602 naming the id.
    let bad_msg = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"m-missing","blockId":"b"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad_msg["error"]["code"], -32602);
    assert!(
        bad_msg["error"]["message"]
            .as_str()
            .unwrap()
            .contains("m-missing"),
        "names the message id: {bad_msg}"
    );
    let bad_block = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"{row_id}","blockId":"{row_id}:9"}}}}"#
        ),
    )
    .await;
    assert_eq!(bad_block["error"]["code"], -32602);
    assert!(
        bad_block["error"]["message"]
            .as_str()
            .unwrap()
            .contains(":9"),
        "names the block id: {bad_block}"
    );

    // Cross-workspace mismatch and unknown agent: not-found, fail closed.
    let other_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":7,"method":"workspace.create","params":{"title":"Other"}}"#,
    )
    .await;
    let other_ws_id = other_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("other workspace id");
    let mismatch = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"{row_id}","blockId":"{row_id}:0","workspaceId":"{other_ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(mismatch["error"]["code"], -32602);
    assert_eq!(mismatch["error"]["data"]["code"], "not-found");
    let unknown_agent = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"agent.getMessageBlock","params":{{"agentId":"agent-00000000-0000-0000-0000-000000000000","messageId":"{row_id}","blockId":"{row_id}:0"}}}}"#
        ),
    )
    .await;
    assert_eq!(unknown_agent["error"]["code"], -32602);
    assert_eq!(unknown_agent["error"]["data"]["code"], "not-found");

    // Missing required params: -32602.
    let missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602);

    srv.ws.stop().await;
}

/// End-to-end 0108 heavy-payload round trip over the real WSS wire
/// (intent-hq/intent#3884): a message whose tool bodies cross the extraction
/// threshold is persisted with side-table rows and a slim-preview
/// placeholder in the content column; the slim `agent.getConversation` read
/// serves exactly the blocks a pre-0108 server would have produced by
/// slimming the full body at serve time (shared transform — byte parity),
/// and `agent.getMessageBlock` hydrates the FULL body back from the side
/// table, byte-identical to what was appended.
#[tokio::test]
async fn wss_extracted_payload_round_trip_slim_and_full() {
    use intent_core::{AgentId, SLIM_PROJECTION_BUDGET_BYTES};
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Extract"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"Extract"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // Bodies well past the 4 KiB extraction threshold (and the 2 KiB slim
    // budget), plus an under-threshold body that must stay inline.
    let big_in = "i".repeat(64 * 1024);
    let big_out = "o".repeat(64 * 1024);
    let content = json!([
        { "type": "tool_use", "id": "b:0", "name": "bash", "toolCallId": "tc-1",
          "input": { "cmd": big_in } },
        { "type": "tool_result", "id": "b:1", "tool_use_id": "tc-1",
          "output": big_out, "is_error": false },
        { "type": "tool_result", "id": "b:2", "tool_use_id": "tc-2",
          "output": "small", "is_error": false },
    ]);
    let row_id = srv
        .store
        .append_agent_message(&agent, "assistant", &content, &now_iso())
        .await
        .expect("append message")
        .id;

    // The write path externalized exactly the two heavy bodies, and the
    // stored content column is bounded — no multi-KB body inline.
    let side_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_message_payload WHERE message_id = ?")
            .bind(&row_id)
            .fetch_one(srv.store.read_pool())
            .await
            .expect("count side rows");
    assert_eq!(side_rows, 2, "both heavy bodies externalized");
    let stored_len: i64 =
        sqlx::query_scalar("SELECT LENGTH(content) FROM agent_message WHERE id = ?")
            .bind(&row_id)
            .fetch_one(srv.store.read_pool())
            .await
            .expect("stored content length");
    assert!(
        stored_len < 8 * 1024,
        "stored column is bounded, got {stored_len} bytes"
    );

    // Slim read (the v8.0 default): served straight from the stored column,
    // byte-identical to slimming the full bodies at serve time.
    let conv = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.getConversation","params":{{"agentId":"{agent_id}"}}}}"#
        ),
    )
    .await;
    let msg = conv["result"]["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|m| m["id"].as_str() == Some(row_id.as_str()))
        .expect("appended message served");
    let blocks = msg["contentBlocks"].as_array().expect("content blocks");
    assert_eq!(blocks[0]["inputTruncated"], json!(true));
    assert_eq!(
        blocks[0]["inputBytes"],
        json!(intent_core::slim_body_size(&content[0]["input"]))
    );
    let preview = blocks[0]["input"]["cmd"].as_str().expect("input preview");
    assert!(big_in.starts_with(preview) && preview.len() < big_in.len());
    assert_eq!(blocks[1]["outputTruncated"], json!(true));
    assert_eq!(blocks[1]["outputBytes"], json!(big_out.len()));
    let preview = blocks[1]["output"].as_str().expect("output preview");
    assert!(big_out.starts_with(preview) && preview.len() < big_out.len());
    assert!(preview.len() <= SLIM_PROJECTION_BUDGET_BYTES);
    // The under-threshold body is served whole, no flags.
    assert_eq!(blocks[2]["output"], "small");
    assert!(blocks[2].get("outputTruncated").is_none());
    // Byte parity with the serve-time transform on the full body.
    let mut served_like = content.clone();
    for (i, field, tflag, bflag) in [
        (0, "input", "inputTruncated", "inputBytes"),
        (1, "output", "outputTruncated", "outputBytes"),
    ] {
        drop(intent_core::slim_heavy_body(
            &mut served_like[i],
            field,
            tflag,
            bflag,
            SLIM_PROJECTION_BUDGET_BYTES,
        ));
    }
    assert_eq!(
        &json!(blocks),
        &served_like,
        "stored placeholder equals serve-time slim of the full body"
    );

    // Full-fidelity read: agent.getMessageBlock hydrates the original body
    // back from the side table, byte-identical, no flags.
    for (block_id, field, want) in [("b:0", "input", None), ("b:1", "output", Some(&big_out))] {
        let got = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":4,"method":"agent.getMessageBlock","params":{{"agentId":"{agent_id}","messageId":"{row_id}","blockId":"{block_id}"}}}}"#
            ),
        )
        .await;
        let block = &got["result"]["block"];
        match want {
            Some(s) => assert_eq!(block[field].as_str(), Some(s.as_str())),
            None => assert_eq!(block[field]["cmd"].as_str(), Some(big_in.as_str())),
        }
        assert!(
            block.get(format!("{field}Truncated")).is_none(),
            "hydrated block carries no slim flags: {block}"
        );
    }

    srv.ws.stop().await;
}

/// `agent.listUserMessages` over the real WSS wire (§5.5): all user-role
/// messages of one agent as lightweight index items, oldest→newest —
/// `{ agentId, items: [{ id, preview, createdAt, metadata? }], total }`.
/// Non-user rows are never included; previews are bounded to
/// `previewChars` (default 300, server-clamped into [1, 2000], with the
/// lenient `opt_int` parsing: non-numeric → absent, floats truncate);
/// `metadata` passes through verbatim when present; a workspace mismatch
/// and an unknown agent are not-found; a missing `agentId` is `-32602`.
#[tokio::test]
async fn wss_agent_list_user_messages_serves_bounded_index() {
    use intent_core::AgentId;
    use serde_json::json;

    let srv = start(WsOptions::default()).await;
    srv.set_setting("model.defaultProvider", serde_json::json!("auggie"));
    let created_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"UserIndex"}}"#,
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
            r#"{{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{{"workspaceId":"{ws_id}","name":"UserIndex"}}}}"#
        ),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId::from(agent_id.as_str());

    // Interleave user rows with an assistant row; the second user row
    // carries metadata (the automated-row marker) and the third is
    // oversized so the default bound truncates it.
    let ts = now_iso();
    let first = srv
        .store
        .append_agent_message(
            &agent,
            "user",
            &json!([{ "type": "text", "text": "first question" }]),
            &ts,
        )
        .await
        .expect("append user 1")
        .id;
    srv.store
        .append_agent_message(
            &agent,
            "assistant",
            &json!([{ "type": "text", "text": "an answer" }]),
            &ts,
        )
        .await
        .expect("append assistant");
    let second = srv
        .store
        .append_agent_message_with_metadata(
            &agent,
            "user",
            &json!([{ "type": "text", "text": "automated follow-up" }]),
            Some(&json!({ "automated": true })),
            &ts,
        )
        .await
        .expect("append user 2")
        .id;
    let long_text = "x".repeat(500);
    let third = srv
        .store
        .append_agent_message(
            &agent,
            "user",
            &json!([{ "type": "text", "text": long_text }]),
            &ts,
        )
        .await
        .expect("append user 3")
        .id;

    // Default bound: user rows only, oldest→newest, previews capped at 300.
    let listed = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(listed["jsonrpc"], "2.0");
    assert_eq!(listed["id"], 3);
    let result = &listed["result"];
    assert_eq!(result["agentId"], agent_id);
    assert_eq!(result["total"], 3);
    let items = result["items"].as_array().expect("items");
    assert_eq!(items.len(), 3, "non-user rows never included: {result}");
    assert_eq!(items[0]["id"], first);
    assert_eq!(items[0]["preview"], "first question");
    assert!(
        items[0].get("metadata").is_none(),
        "metadata omitted when absent: {}",
        items[0]
    );
    assert!(items[0]["createdAt"]
        .as_str()
        .is_some_and(|s| !s.is_empty()));
    assert_eq!(items[1]["id"], second);
    assert_eq!(
        items[1]["metadata"],
        json!({ "automated": true }),
        "metadata passes through verbatim"
    );
    assert_eq!(items[2]["id"], third);
    let preview = items[2]["preview"].as_str().expect("preview");
    assert_eq!(preview.chars().count(), 300, "default previewChars bound");

    // Explicit previewChars applies; a non-positive value clamps to 1 and
    // an over-cap value clamps to 2000 (the 500-char third row passes
    // through whole under the clamped bound).
    let tight = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","previewChars":5}}}}"#
        ),
    )
    .await;
    assert_eq!(tight["result"]["items"][0]["preview"], "first");
    let clamped = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","previewChars":0}}}}"#
        ),
    )
    .await;
    assert_eq!(clamped["result"]["items"][0]["preview"], "f");
    let upper = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","previewChars":999999}}}}"#
        ),
    )
    .await;
    let upper_preview = upper["result"]["items"][2]["preview"]
        .as_str()
        .expect("preview");
    assert_eq!(
        upper_preview.chars().count(),
        500,
        "over-cap previewChars clamps to 2000, so the 500-char row is whole"
    );

    // Lenient previewChars parsing (the opt_int convention, unlike the
    // strict v7.1 aroundIndex): a non-numeric value is treated as absent
    // (default 300) and a float is truncated toward zero.
    let lenient_string = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":11,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","previewChars":"nope"}}}}"#
        ),
    )
    .await;
    let default_preview = lenient_string["result"]["items"][2]["preview"]
        .as_str()
        .expect("preview");
    assert_eq!(
        default_preview.chars().count(),
        300,
        "non-numeric previewChars falls back to the 300 default"
    );
    let lenient_float = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":12,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","previewChars":5.9}}}}"#
        ),
    )
    .await;
    assert_eq!(
        lenient_float["result"]["items"][0]["preview"], "first",
        "float previewChars truncates toward zero (5.9 → 5)"
    );

    // Cross-workspace mismatch and unknown agent: not-found, fail closed.
    let other_ws = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":6,"method":"workspace.create","params":{"title":"Other"}}"#,
    )
    .await;
    let other_ws_id = other_ws["result"]["workspace"]["id"]
        .as_str()
        .expect("other workspace id");
    let mismatch = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"agent.listUserMessages","params":{{"agentId":"{agent_id}","workspaceId":"{other_ws_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(mismatch["error"]["code"], -32602);
    assert_eq!(mismatch["error"]["data"]["code"], "not-found");
    let unknown_agent = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":8,"method":"agent.listUserMessages","params":{"agentId":"agent-00000000-0000-0000-0000-000000000000"}}"#,
    )
    .await;
    assert_eq!(unknown_agent["error"]["code"], -32602);
    assert_eq!(unknown_agent["error"]["data"]["code"], "not-found");

    // Missing required agentId: -32602.
    let missing = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":9,"method":"agent.listUserMessages","params":{}}"#,
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602);

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
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId(id.to_string()),
        workspace_id: ws.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: name.to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
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
        file_blocks: None,
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
        pending_delete_at: None,
        retired_at: None,
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

/// `workspace.transfer.plan` over the real WSS transport (PROTOCOL §5.1):
/// the result carries `{ plan }` with the versioned manifest (formatVersion,
/// creatingIntentdVersion, tables with rowCount/approxBytes, assets, git
/// summary) and the size breakdown summing to `totalSizeBytes`; `event` is
/// never listed; a repo holding an untracked nested git repo surfaces the
/// `nested-repos-skipped` warning naming the dir (and no spurious
/// `uncommitted-changes`); unknown workspace ids map to
/// `-32602 Workspace not found`.
#[tokio::test]
async fn wss_workspace_transfer_plan_round_trip() {
    let srv = start(WsOptions::default()).await;
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Transfer"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.transfer.plan","params":{{"workspaceId":"{ws_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 2, "envelope: {resp}");
    let plan = &resp["result"]["plan"];
    let manifest = &plan["manifest"];
    assert_eq!(manifest["formatVersion"], 1, "{resp}");
    assert!(
        manifest["creatingIntentdVersion"].is_string(),
        "manifest records the creating daemon version: {resp}"
    );
    assert_eq!(manifest["workspaceId"], ws_id.as_str(), "{resp}");
    let tables = manifest["tables"].as_array().expect("tables array");
    assert!(
        tables.iter().any(|t| t["name"] == "workspace"
            && t["rowCount"] == 1
            && t["approxBytes"].as_i64().unwrap_or(0) > 0),
        "workspace row is counted with a byte estimate: {resp}"
    );
    assert!(
        tables.iter().all(|t| t["name"] != "event"),
        "event log is excluded from the manifest: {resp}"
    );
    assert!(manifest["assets"].is_array(), "{resp}");
    assert!(manifest["attachments"].is_array(), "{resp}");
    assert!(manifest["git"]["hasRepository"].is_boolean(), "{resp}");
    let total = plan["totalSizeBytes"].as_u64().expect("total");
    let db = plan["dbRowBytes"].as_u64().expect("db");
    let assets = plan["assetBytes"].as_u64().expect("assets");
    let attachments = plan["attachmentBytes"].as_u64().expect("attachments");
    let bundle = plan["estimatedGitBundleBytes"].as_u64().expect("bundle");
    assert_eq!(
        total,
        db + assets + attachments + bundle,
        "size breakdown sums: {resp}"
    );
    assert!(plan["warnings"].is_array(), "{resp}");

    // A git-backed workspace whose repo holds an untracked nested git repo:
    // the plan's warnings carry the `nested-repos-skipped` code naming the
    // directory, and the nested dir never shows up in `dirtyFiles`.
    let repo_dir = test_tempdir("intentd-wss-transfer-nested-");
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
    let nested = repo.join(".import-wt");
    std::fs::create_dir_all(&nested).unwrap();
    let ok = std::process::Command::new("git")
        .current_dir(&nested)
        .args(["init", "-q", "-b", "main"])
        .status()
        .expect("run git")
        .success();
    assert!(ok, "nested git init failed");
    std::fs::write(nested.join("inner.txt"), "inner\n").unwrap();

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"workspace.create","params":{{"title":"WSS Transfer Nested","worktreePath":"{}","path":"{}"}}}}"#,
        repo.display(),
        repo.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let nested_ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"workspace.transfer.plan","params":{{"workspaceId":"{nested_ws_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    let plan = &resp["result"]["plan"];
    assert_eq!(plan["manifest"]["git"]["hasRepository"], true, "{resp}");
    assert_eq!(
        plan["manifest"]["git"]["dirtyFiles"],
        serde_json::json!([]),
        "nested repo dir must not appear as a dirty file: {resp}"
    );
    let warnings = plan["warnings"].as_array().expect("warnings array");
    let nested_warn = warnings
        .iter()
        .find(|w| w["code"] == "nested-repos-skipped")
        .unwrap_or_else(|| panic!("nested-repos-skipped warning missing: {resp}"));
    assert!(
        nested_warn["message"]
            .as_str()
            .expect("message string")
            .contains(".import-wt"),
        "warning names the skipped dir: {resp}"
    );
    assert!(
        !warnings.iter().any(|w| w["code"] == "uncommitted-changes"),
        "nested repo alone is not an uncommitted change: {resp}"
    );

    // Unknown workspace → the standard workspace-not-found mapping.
    let resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":5,"method":"workspace.transfer.plan","params":{"workspaceId":"missing"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert_eq!(resp["error"]["message"], "Workspace not found", "{resp}");

    srv.ws.stop().await;
}

/// `workspace.transfer.plan` over the real WSS transport (PROTOCOL §5.1) for
/// a worktree whose submodule checkout points at a commit that exists only
/// locally: the plan carries exactly one `submodule-unpublished-commits`
/// warning naming the submodule path, short sha and branch, and
/// `manifest.git.submodules` lists the finding as
/// `{ name, path, commitSha, branch, carried: true }`. After the commit is
/// pushed to the submodule's origin the warning and the entry are gone.
#[tokio::test]
async fn wss_workspace_transfer_plan_reports_unpublished_submodule_commits() {
    let srv = start(WsOptions::default()).await;
    let root_dir = test_tempdir("intentd-wss-transfer-submodule-");
    let root = root_dir.path().to_path_buf();
    let git = |dir: &std::path::Path, args: &[&str]| -> String {
        let out = std::process::Command::new("git")
            .current_dir(dir)
            .args(["-c", "protocol.file.allow=always"])
            .args(args)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} in {} failed: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let init_repo = |dir: &std::path::Path| {
        std::fs::create_dir_all(dir).unwrap();
        git(dir, &["init", "-q", "-b", "main"]);
        std::fs::write(dir.join("README.md"), "hello\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-q", "-m", "seed"]);
    };

    // Submodule origin: a bare repo seeded with one commit. Superproject
    // tracks it at `sub`, checked out on `main` (attached).
    init_repo(&root.join("sub-src"));
    git(&root, &["clone", "-q", "--bare", "sub-src", "origin.git"]);
    let origin = root.join("origin.git");
    let sup = root.join("super");
    init_repo(&sup);
    git(
        &sup,
        &["submodule", "add", "-q", origin.to_str().unwrap(), "sub"],
    );
    git(&sup, &["commit", "-q", "-m", "add submodule"]);
    let sub = sup.join("sub");
    git(&sub, &["checkout", "-q", "main"]);

    let create_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{{"title":"WSS Transfer Submodule","worktreePath":"{}","path":"{}"}}}}"#,
        sup.display(),
        sup.display(),
    );
    let created = wss_call(srv.port, srv.cfg.clone(), &create_frame).await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    let plan_frame = |id: u32| {
        format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"workspace.transfer.plan","params":{{"workspaceId":"{ws_id}"}}}}"#
        )
    };

    // Everything published: no warning, empty submodules list.
    let resp = wss_call(srv.port, srv.cfg.clone(), &plan_frame(2)).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 2, "envelope: {resp}");
    let plan = &resp["result"]["plan"];
    assert_eq!(plan["manifest"]["git"]["hasRepository"], true, "{resp}");
    assert_eq!(
        plan["manifest"]["git"]["submodules"],
        serde_json::json!([]),
        "published submodule yields no entry: {resp}"
    );
    assert!(
        !plan["warnings"]
            .as_array()
            .expect("warnings array")
            .iter()
            .any(|w| w["code"] == "submodule-unpublished-commits"),
        "published submodule yields no warning: {resp}"
    );
    let clean_bundle = plan["estimatedGitBundleBytes"].as_u64().expect("bundle");

    // A commit made inside the submodule checkout and never pushed.
    std::fs::write(sub.join("wip.txt"), "wip\n").unwrap();
    git(&sub, &["add", "wip.txt"]);
    git(&sub, &["commit", "-q", "-m", "local wip"]);
    let sha = git(&sub, &["rev-parse", "HEAD"]);

    let resp = wss_call(srv.port, srv.cfg.clone(), &plan_frame(3)).await;
    assert_eq!(resp["id"], 3, "envelope: {resp}");
    let plan = &resp["result"]["plan"];
    let warnings = plan["warnings"].as_array().expect("warnings array");
    let sub_warns: Vec<_> = warnings
        .iter()
        .filter(|w| w["code"] == "submodule-unpublished-commits")
        .collect();
    assert_eq!(sub_warns.len(), 1, "exactly one submodule warning: {resp}");
    let message = sub_warns[0]["message"].as_str().expect("message string");
    assert!(
        message.contains(&format!("sub @ {} (main)", &sha[..7])),
        "warning names path, short sha and branch: {resp}"
    );
    assert!(
        message.contains("will ride in the archive"),
        "worktree finding is carried: {resp}"
    );
    assert_eq!(
        plan["manifest"]["git"]["submodules"],
        serde_json::json!([{
            "name": "sub",
            "path": "sub",
            "commitSha": sha,
            "branch": "main",
            "carried": true
        }]),
        "manifest lists the unpublished submodule: {resp}"
    );
    assert!(
        plan["estimatedGitBundleBytes"].as_u64().expect("bundle") > clean_bundle,
        "estimate grows by the submodule objects: {resp}"
    );
    let total = plan["totalSizeBytes"].as_u64().expect("total");
    let db = plan["dbRowBytes"].as_u64().expect("db");
    let assets = plan["assetBytes"].as_u64().expect("assets");
    let attachments = plan["attachmentBytes"].as_u64().expect("attachments");
    let bundle = plan["estimatedGitBundleBytes"].as_u64().expect("bundle");
    assert_eq!(
        total,
        db + assets + attachments + bundle,
        "size breakdown still sums: {resp}"
    );
    // The superproject sees the gitlink move; the plan itself never wrote.
    assert_eq!(
        plan["manifest"]["git"]["dirtyFiles"],
        serde_json::json!(["sub"]),
        "{resp}"
    );

    // Publishing the commit clears the warning and the manifest entry.
    git(&sub, &["push", "-q", "origin", "main"]);
    let resp = wss_call(srv.port, srv.cfg.clone(), &plan_frame(4)).await;
    assert_eq!(resp["id"], 4, "envelope: {resp}");
    let plan = &resp["result"]["plan"];
    assert_eq!(
        plan["manifest"]["git"]["submodules"],
        serde_json::json!([]),
        "pushed submodule commit yields no entry: {resp}"
    );
    assert!(
        !plan["warnings"]
            .as_array()
            .expect("warnings array")
            .iter()
            .any(|w| w["code"] == "submodule-unpublished-commits"),
        "pushed submodule commit yields no warning: {resp}"
    );

    srv.ws.stop().await;
}

/// `file.placeAttachment` over the real WSS wire (PROTOCOL §5.9,
/// monorepo#1948): a base64 payload lands in the workspace's
/// `.intent/attachments/` directory and the response carries the
/// workspace-relative `{ ok, path, fileName, size }`; a same-name re-place
/// answers a collision-suffixed name; the `.intent/.gitignore` exclusion file
/// is ensured; and the exactly-one-of `data`/`sourcePath` violation is the
/// documented `-32602`.
#[tokio::test]
async fn wss_file_place_attachment_round_trip() {
    use base64::Engine as _;

    let srv = start(WsOptions::default()).await;

    let ws = WorkspaceId::new();
    let dir = test_tempdir("intentd-wss-placeatt-");
    let root = std::fs::canonicalize(dir.path()).expect("canonicalize root");
    let mut w = fixture_workspace(&ws);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    srv.store.insert_workspace(&w).await.expect("insert ws");

    let b64 = base64::engine::general_purpose::STANDARD.encode(b"oversized attachment bytes");
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"file.placeAttachment","params":{{"workspaceId":"{}","fileName":"trace.har","data":"{b64}","mimeType":"application/json"}}}}"#,
        ws.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["jsonrpc"], "2.0", "envelope: {resp}");
    assert_eq!(resp["id"], 1, "envelope: {resp}");
    assert_eq!(resp["result"]["ok"], serde_json::json!(true), "{resp}");
    assert_eq!(
        resp["result"]["path"],
        serde_json::json!(".intent/attachments/trace.har"),
        "{resp}"
    );
    assert_eq!(
        resp["result"]["fileName"],
        serde_json::json!("trace.har"),
        "{resp}"
    );
    assert_eq!(resp["result"]["size"], serde_json::json!(26), "{resp}");
    // v6.12 additive attachment-registry fields.
    let attachment_id = resp["result"]["attachmentId"]
        .as_str()
        .expect("attachmentId")
        .to_string();
    assert_eq!(
        resp["result"]["mimeType"],
        serde_json::json!("application/json"),
        "{resp}"
    );
    assert!(
        resp["result"]["uploadedAt"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "{resp}"
    );
    assert_eq!(
        std::fs::read(root.join(".intent/attachments/trace.har")).expect("placed file"),
        b"oversized attachment bytes"
    );
    // The exclusion contract: the default `.intent/.gitignore` (ignore
    // everything except config.json) was ensured on the way.
    let gitignore =
        std::fs::read_to_string(root.join(".intent/.gitignore")).expect("gitignore ensured");
    assert!(gitignore.contains('*'), "gitignore content: {gitignore}");

    // Same name again → collision-suffixed `trace-2.har`.
    let frame2 = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"file.placeAttachment","params":{{"workspaceId":"{}","fileName":"trace.har","data":"{b64}"}}}}"#,
        ws.0
    );
    let resp2 = wss_call(srv.port, srv.cfg.clone(), &frame2).await;
    assert_eq!(resp2["result"]["fileName"], "trace-2.har", "{resp2}");
    assert_eq!(
        resp2["result"]["path"], ".intent/attachments/trace-2.har",
        "{resp2}"
    );

    // Neither `data` nor `sourcePath` → -32602.
    let frame3 = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"file.placeAttachment","params":{{"workspaceId":"{}","fileName":"x.bin"}}}}"#,
        ws.0
    );
    let resp3 = wss_call(srv.port, srv.cfg.clone(), &frame3).await;
    assert_eq!(resp3["error"]["code"].as_i64(), Some(-32602), "{resp3}");

    // `sourcePath` classification over the wire (monorepo#2144): a directory
    // source is the documented -32602 with the reason in the message, not a
    // -32603 Internal.
    let dir_source = root.join("some-dir");
    std::fs::create_dir_all(&dir_source).expect("mkdir dir source");
    let frame_dir = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"file.placeAttachment","params":{{"workspaceId":"{}","fileName":"some-dir","sourcePath":{}}}}}"#,
        ws.0,
        serde_json::json!(dir_source.to_string_lossy())
    );
    let resp_dir = wss_call(srv.port, srv.cfg.clone(), &frame_dir).await;
    assert_eq!(
        resp_dir["error"]["code"].as_i64(),
        Some(-32602),
        "{resp_dir}"
    );
    assert!(
        resp_dir["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("sourcePath is a directory")),
        "{resp_dir}"
    );

    // A missing source is equally -32602 ("does not exist").
    let frame_missing = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"file.placeAttachment","params":{{"workspaceId":"{}","fileName":"gone.txt","sourcePath":{}}}}}"#,
        ws.0,
        serde_json::json!(root.join("gone.txt").to_string_lossy())
    );
    let resp_missing = wss_call(srv.port, srv.cfg.clone(), &frame_missing).await;
    assert_eq!(
        resp_missing["error"]["code"].as_i64(),
        Some(-32602),
        "{resp_missing}"
    );
    assert!(
        resp_missing["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("sourcePath does not exist")),
        "{resp_missing}"
    );

    // `file.getAttachmentInfo` (v6.12) serves the registry row with `exists`.
    let frame4 = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"file.getAttachmentInfo","params":{{"attachmentId":"{attachment_id}"}}}}"#
    );
    let resp4 = wss_call(srv.port, srv.cfg.clone(), &frame4).await;
    assert_eq!(
        resp4["result"]["attachmentId"],
        serde_json::json!(attachment_id),
        "{resp4}"
    );
    assert_eq!(
        resp4["result"]["fileName"],
        serde_json::json!("trace.har"),
        "{resp4}"
    );
    assert_eq!(
        resp4["result"]["path"],
        serde_json::json!(".intent/attachments/trace.har"),
        "{resp4}"
    );
    assert_eq!(
        resp4["result"]["exists"],
        serde_json::json!(true),
        "{resp4}"
    );

    // Unknown attachment id → -32602 ("unknown attachment id").
    let frame5 = r#"{"jsonrpc":"2.0","id":5,"method":"file.getAttachmentInfo","params":{"attachmentId":"nope"}}"#;
    let resp5 = wss_call(srv.port, srv.cfg.clone(), frame5).await;
    assert_eq!(resp5["error"]["code"].as_i64(), Some(-32602), "{resp5}");
    assert!(
        resp5["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("unknown attachment id")),
        "{resp5}"
    );

    srv.ws.stop().await;
}

/// `file.readChunk` over the real WSS wire (PROTOCOL §5.9, v6.18,
/// monorepo#2458): raw bytes of a binary workspace file are served as
/// offset-windowed base64 chunks `{ content, bytesRead, size }` and
/// reassemble byte-identically; a window past EOF is an empty chunk; a
/// directory and an over-cap `length` are the documented `-32602`; and a
/// traversal path is rejected by the containment guard (`-32603`).
#[tokio::test]
async fn wss_file_read_chunk_round_trip() {
    use base64::Engine as _;

    let srv = start(WsOptions::default()).await;

    let ws = WorkspaceId::new();
    let dir = test_tempdir("intentd-wss-readchunk-");
    let root = std::fs::canonicalize(dir.path()).expect("canonicalize root");
    let mut w = fixture_workspace(&ws);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    srv.store.insert_workspace(&w).await.expect("insert ws");

    // Binary payload (invalid UTF-8 — `file.read` would fail on it).
    let payload: Vec<u8> = (0..2048u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(root.join("blob.bin"), &payload).expect("write blob");

    // Reassemble the file in two windows and verify byte identity.
    let frame1 = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"blob.bin","offset":0,"length":1024}}}}"#,
        ws.0
    );
    let resp1 = wss_call(srv.port, srv.cfg.clone(), &frame1).await;
    assert_eq!(resp1["jsonrpc"], "2.0", "envelope: {resp1}");
    assert_eq!(resp1["id"], 1, "envelope: {resp1}");
    assert_eq!(
        resp1["result"]["bytesRead"],
        serde_json::json!(1024),
        "{resp1}"
    );
    assert_eq!(
        resp1["result"]["size"],
        serde_json::json!(2048u64),
        "{resp1}"
    );
    let mut assembled = base64::engine::general_purpose::STANDARD
        .decode(resp1["result"]["content"].as_str().expect("content"))
        .expect("valid base64");

    let frame2 = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"blob.bin","offset":1024,"length":4096}}}}"#,
        ws.0
    );
    let resp2 = wss_call(srv.port, srv.cfg.clone(), &frame2).await;
    // Short read at EOF: only the remaining bytes come back.
    assert_eq!(
        resp2["result"]["bytesRead"],
        serde_json::json!(1024),
        "{resp2}"
    );
    assembled.extend(
        base64::engine::general_purpose::STANDARD
            .decode(resp2["result"]["content"].as_str().expect("content"))
            .expect("valid base64"),
    );
    assert_eq!(assembled, payload, "reassembled bytes differ");

    // Window at/past EOF → empty chunk, not an error.
    let frame3 = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"blob.bin","offset":2048,"length":16}}}}"#,
        ws.0
    );
    let resp3 = wss_call(srv.port, srv.cfg.clone(), &frame3).await;
    assert_eq!(
        resp3["result"]["bytesRead"],
        serde_json::json!(0),
        "{resp3}"
    );
    assert_eq!(resp3["result"]["content"], serde_json::json!(""), "{resp3}");
    assert_eq!(
        resp3["result"]["size"],
        serde_json::json!(2048u64),
        "{resp3}"
    );

    // Directory → -32602 naming the cause.
    std::fs::create_dir_all(root.join("subdir")).expect("mkdir");
    let frame4 = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"subdir","offset":0,"length":16}}}}"#,
        ws.0
    );
    let resp4 = wss_call(srv.port, srv.cfg.clone(), &frame4).await;
    assert_eq!(resp4["error"]["code"].as_i64(), Some(-32602), "{resp4}");
    assert!(
        resp4["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("directory")),
        "{resp4}"
    );

    // Over-cap length (> 16 MiB decoded) → -32602.
    let frame5 = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"blob.bin","offset":0,"length":16777217}}}}"#,
        ws.0
    );
    let resp5 = wss_call(srv.port, srv.cfg.clone(), &frame5).await;
    assert_eq!(resp5["error"]["code"].as_i64(), Some(-32602), "{resp5}");

    // Containment guard: a traversal path is rejected (-32603, same as the
    // other file ops).
    let frame6 = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"file.readChunk","params":{{"workspaceId":"{}","path":"../escape.bin","offset":0,"length":16}}}}"#,
        ws.0
    );
    let resp6 = wss_call(srv.port, srv.cfg.clone(), &frame6).await;
    assert_eq!(resp6["error"]["code"].as_i64(), Some(-32603), "{resp6}");

    srv.ws.stop().await;
}

/// Workspace-containment fail-closed guard over the real WSS wire
/// (intent-hq/intent#3839): a `file.*` request carrying a `workspaceId` that
/// does not exist — or one whose workspace row has no filesystem root — must
/// be rejected with the containment `-32603`, never resolved CWD-relative or
/// via the absolute path. A read of an absolute host path and a write to a
/// unique temp target both fail closed, and the write target is never
/// created on disk; the same requests against a real rooted workspace still
/// succeed, proving the guard rejects only the empty-root case.
#[tokio::test]
async fn wss_file_ops_unknown_workspace_fail_closed() {
    let srv = start(WsOptions::default()).await;

    // A workspace that exists but has no path/worktree resolves to an empty
    // containment root, exactly like an unknown workspaceId.
    let pathless = WorkspaceId::new();
    let w = fixture_workspace(&pathless);
    srv.store.insert_workspace(&w).await.expect("insert ws");

    let escape = std::env::temp_dir().join(format!("intentd-wss-escape-{}", uuid::Uuid::new_v4()));
    let escape_s = escape.to_string_lossy().into_owned();

    for ws_id in ["ws-does-not-exist", pathless.0.as_str()] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"file.read","params":{{"workspaceId":"{ws_id}","path":"/etc/passwd"}}}}"#,
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
        assert!(
            resp["error"]["data"]
                .as_str()
                .is_some_and(|m| m.contains("Access denied")),
            "{resp}"
        );

        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"file.write","params":{{"workspaceId":"{ws_id}","path":"{escape_s}","content":"x"}}}}"#,
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
        assert!(!escape.exists(), "escape write must not land on disk");

        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"file.readChunk","params":{{"workspaceId":"{ws_id}","path":"/etc/passwd","offset":0,"length":16}}}}"#,
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
    }

    // Control: the same ops against a real rooted workspace succeed, so the
    // rejections above are the empty-root guard, not a broken file path.
    let rooted = WorkspaceId::new();
    let dir = test_tempdir("intentd-wss-failclosed-");
    let root = std::fs::canonicalize(dir.path()).expect("canonicalize root");
    std::fs::write(root.join("ok.txt"), "ok").expect("write fixture");
    let mut w = fixture_workspace(&rooted);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    srv.store.insert_workspace(&w).await.expect("insert ws");
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"file.read","params":{{"workspaceId":"{}","path":"ok.txt"}}}}"#,
        rooted.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["result"], serde_json::json!("ok"), "{resp}");

    srv.ws.stop().await;
}

/// `file.attachmentUpload.begin` / `.chunk` / `.commit` / `.abort` (§5.9,
/// v6.16): the staged chunked attachment upload lifecycle over the real WSS
/// transport. A two-chunk payload is staged and committed; the commit result
/// is byte-shape-identical to a successful `file.placeAttachment` result
/// (registry fields included) and the reassembled bytes land under
/// `.intent/attachments/`. An unknown uploadId is the documented -32602, the
/// 5th concurrent per-workspace begin is the documented -32602 naming the
/// cap (monorepo#2275), and `abort` retires a pending session idempotently.
#[tokio::test]
async fn wss_file_attachment_upload_round_trip() {
    use base64::Engine as _;

    let srv = start(WsOptions::default()).await;

    let ws = WorkspaceId::new();
    let dir = test_tempdir("intentd-wss-attup-");
    let root = std::fs::canonicalize(dir.path()).expect("canonicalize root");
    let mut w = fixture_workspace(&ws);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    srv.store.insert_workspace(&w).await.expect("insert ws");

    let payload: Vec<u8> = (0u32..50_000).flat_map(u32::to_le_bytes).collect();
    let sha = sha256_hex(&payload);
    let mid = payload.len() / 2;
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);

    // begin → { uploadId, maxChunkBytes }.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"file.attachmentUpload.begin","params":{{"workspaceId":"{}","fileName":"big.bin","sizeBytes":{},"sha256":"{sha}","mimeType":"application/octet-stream"}}}}"#,
        ws.0,
        payload.len()
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    let upload_id = resp["result"]["uploadId"]
        .as_str()
        .expect("uploadId")
        .to_string();
    assert_eq!(
        resp["result"]["maxChunkBytes"].as_u64(),
        Some(16 * 1024 * 1024),
        "{resp}"
    );

    // Two chunks; receivedBytes accumulates.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"file.attachmentUpload.chunk","params":{{"uploadId":"{upload_id}","seq":0,"data":"{}"}}}}"#,
        b64(&payload[..mid])
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["result"]["seq"].as_u64(), Some(0), "{resp}");
    assert_eq!(
        resp["result"]["receivedBytes"].as_u64(),
        Some(mid as u64),
        "{resp}"
    );
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"file.attachmentUpload.chunk","params":{{"uploadId":"{upload_id}","seq":1,"data":"{}"}}}}"#,
        b64(&payload[mid..])
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        resp["result"]["receivedBytes"].as_u64(),
        Some(payload.len() as u64),
        "{resp}"
    );

    // commit → byte-shape-identical to a placeAttachment success.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"file.attachmentUpload.commit","params":{{"uploadId":"{upload_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["result"]["ok"], serde_json::json!(true), "{resp}");
    assert_eq!(
        resp["result"]["path"],
        serde_json::json!(".intent/attachments/big.bin"),
        "{resp}"
    );
    assert_eq!(
        resp["result"]["fileName"],
        serde_json::json!("big.bin"),
        "{resp}"
    );
    assert_eq!(
        resp["result"]["size"].as_u64(),
        Some(payload.len() as u64),
        "{resp}"
    );
    assert!(resp["result"]["attachmentId"].is_string(), "{resp}");
    assert_eq!(
        resp["result"]["mimeType"],
        serde_json::json!("application/octet-stream"),
        "{resp}"
    );
    assert!(
        resp["result"]["uploadedAt"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "{resp}"
    );
    assert_eq!(
        std::fs::read(root.join(".intent/attachments/big.bin")).expect("placed file"),
        payload
    );

    // The settled uploadId is unknown now → -32602.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"file.attachmentUpload.chunk","params":{{"uploadId":"{upload_id}","seq":2,"data":"{}"}}}}"#,
        b64(b"late")
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("no attachment upload in progress")),
        "{resp}"
    );

    // Per-workspace session cap (monorepo#2275): the 5th live begin is the
    // documented -32602 naming the cap, and settling a session (abort)
    // frees the slot.
    let mut cap_ids = Vec::new();
    for i in 0..4 {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":40,"method":"file.attachmentUpload.begin","params":{{"workspaceId":"{}","fileName":"cap-{i}.bin","sizeBytes":4,"sha256":"{sha}"}}}}"#,
            ws.0
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        cap_ids.push(
            resp["result"]["uploadId"]
                .as_str()
                .expect("uploadId")
                .to_string(),
        );
    }
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":41,"method":"file.attachmentUpload.begin","params":{{"workspaceId":"{}","fileName":"fifth.bin","sizeBytes":4,"sha256":"{sha}"}}}}"#,
        ws.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32602), "{resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("attachment uploads in progress") && m.contains("max 4")),
        "{resp}"
    );
    for (i, cap_id) in cap_ids.iter().enumerate() {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":42,"method":"file.attachmentUpload.abort","params":{{"uploadId":"{cap_id}"}}}}"#
        );
        let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
        assert_eq!(
            resp["result"]["aborted"],
            serde_json::json!(true),
            "abort {i}: {resp}"
        );
    }

    // abort retires a pending session; a second abort is the idempotent
    // non-error (`aborted: false`).
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"file.attachmentUpload.begin","params":{{"workspaceId":"{}","fileName":"other.bin","sizeBytes":4,"sha256":"{sha}"}}}}"#,
        ws.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    let abort_id = resp["result"]["uploadId"]
        .as_str()
        .expect("uploadId")
        .to_string();
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"file.attachmentUpload.abort","params":{{"uploadId":"{abort_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["result"]["aborted"], serde_json::json!(true), "{resp}");
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":8,"method":"file.attachmentUpload.abort","params":{{"uploadId":"{abort_id}"}}}}"#
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(
        resp["result"]["aborted"],
        serde_json::json!(false),
        "{resp}"
    );

    srv.ws.stop().await;
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// `workspace.import.begin` / `.chunk` / `.commit` / `.abort` (§5.1): the
/// staged, atomic import lifecycle over the real WSS transport. A fixture
/// zip archive (manifest + rows) is uploaded in two chunks and committed;
/// the imported workspace only appears in `workspace.list` after commit,
/// with the import transforms applied, and the commit's `workspace:created`
/// event reaches an `events.subscribe` subscriber. A version-mismatched
/// manifest is rejected by `begin` naming both versions, and `abort` retires
/// a pending session idempotently.
#[tokio::test]
async fn wss_workspace_import_lifecycle() {
    use base64::Engine as _;
    use std::io::Write as _;

    let srv = start(WsOptions::default()).await;
    let ws_id = "ws-wss-imported";
    let t = "2026-08-11T00:00:00Z";

    // Manifest must carry the exact intentd version (workspace-synchronized
    // crate versions make CARGO_PKG_VERSION valid here).
    let manifest = serde_json::json!({
        "formatVersion": intent_core::transfer::TRANSFER_FORMAT_VERSION,
        "creatingIntentdVersion": env!("CARGO_PKG_VERSION"),
        "workspaceId": ws_id,
        "createdAt": t,
        "tables": [],
        "assets": [],
        "git": { "hasRepository": false, "dirtyFiles": [], "sandboxBranches": [] }
    });

    // Fixture archive: manifest.json + rows/{workspace,agent_session}.jsonl.
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buf);
    let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
    zip.start_file("manifest.json", options).expect("manifest");
    zip.write_all(manifest.to_string().as_bytes())
        .expect("manifest bytes");
    zip.start_file("rows/workspace.jsonl", options)
        .expect("rows");
    zip.write_all(
        format!(
            "{}\n",
            serde_json::json!({
                "id": ws_id, "title": "WSS Imported", "branch": "main",
                "status": "Active", "created_at": t, "updated_at": t
            })
        )
        .as_bytes(),
    )
    .expect("workspace row");
    zip.start_file("rows/agent_session.jsonl", options)
        .expect("rows");
    zip.write_all(
        format!(
            "{}\n",
            serde_json::json!({
                "id": "agent-wss-import", "workspace_id": ws_id, "name": "A",
                "status": "active", "is_active": 1, "acp_session_id": "acp-stale",
                "created_at": t, "updated_at": t
            })
        )
        .as_bytes(),
    )
    .expect("session row");
    zip.finish().expect("zip");
    let archive = buf.into_inner();
    let sha = sha256_hex(&archive);

    // begin → { importId, maxChunkBytes }.
    let begin = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.import.begin","params":{{"manifest":{manifest},"archiveSizeBytes":{},"archiveSha256":"{sha}"}}}}"#,
            archive.len()
        ),
    )
    .await;
    let import_id = begin["result"]["importId"]
        .as_str()
        .unwrap_or_else(|| panic!("importId: {begin}"))
        .to_string();
    assert!(begin["result"]["maxChunkBytes"].as_u64().unwrap() > 0);

    // Not visible before commit.
    let list = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":2,"method":"workspace.list","params":{}}"#,
    )
    .await;
    assert!(
        !list["result"]["workspaces"]
            .as_array()
            .expect("workspaces")
            .iter()
            .any(|w| w["id"] == ws_id),
        "workspace must not be listed before commit"
    );

    // Two chunks; seq 0 retried idempotently.
    let mid = archive.len() / 2;
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    for (id, seq, part) in [
        (3, 0, &archive[..mid]),
        (4, 1, &archive[mid..]),
        (5, 0, &archive[..mid]),
    ] {
        let resp = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"workspace.import.chunk","params":{{"importId":"{import_id}","seq":{seq},"data":"{}"}}}}"#,
                b64(part)
            ),
        )
        .await;
        assert_eq!(resp["result"]["seq"], seq, "{resp}");
    }

    // Subscribe BEFORE commit so the `workspace:created` notification the
    // commit publishes (§6.5) is delivered to this connection.
    let mut sub_ws = connect_ws(srv.port, srv.cfg.clone()).await;
    sub_ws
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":100,"method":"events.subscribe","params":{"eventTypes":["workspace:created","workspace:setup:completed"]}}"#
                .to_string()
                .into(),
        ))
        .await
        .expect("send subscribe");
    let sub = loop {
        match sub_ws.next().await {
            Some(Ok(Message::Text(text))) => {
                break serde_json::from_str::<Value>(&text).expect("json")
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    };
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // commit → workspace live with transforms applied.
    let committed = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.import.commit","params":{{"importId":"{import_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(committed["result"]["workspace"]["id"], ws_id, "{committed}");
    assert_eq!(
        committed["result"]["interruptedAgents"],
        serde_json::json!(["agent-wss-import"]),
        "in-flight agent surfaced as interrupted: {committed}"
    );
    assert!(committed["result"]["importedRows"].as_u64().unwrap() >= 2);

    // The commit's `workspace:created` event reaches the subscriber (§6.3).
    let evt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match sub_ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "workspace:created"
                        && v["params"]["event"]["workspaceId"] == ws_id
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub_ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for workspace:created after import commit");
    assert_eq!(evt["workspaceId"], ws_id, "{evt}");

    // Imports run no setup stage, so the commit pairs `workspace:created`
    // with an immediate `workspace:setup:completed { ranScript: false }` —
    // the watcher registry must not defer this workspace to the backstop.
    let evt = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match sub_ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "workspace:setup:completed"
                        && v["params"]["event"]["workspaceId"] == ws_id
                    {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub_ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for workspace:setup:completed after import commit");
    assert_eq!(
        evt["data"],
        serde_json::json!({ "workspaceId": ws_id, "ranScript": false }),
        "{evt}"
    );

    let list = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":7,"method":"workspace.list","params":{}}"#,
    )
    .await;
    assert!(
        list["result"]["workspaces"]
            .as_array()
            .expect("workspaces")
            .iter()
            .any(|w| w["id"] == ws_id),
        "workspace listed after commit"
    );
    let interrupted = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":8,"method":"agent.listInterrupted","params":{}}"#,
    )
    .await;
    assert!(
        interrupted["result"]["agents"]
            .as_array()
            .expect("interrupted agents")
            .iter()
            .any(|r| r["agentId"] == "agent-wss-import"),
        "imported in-flight agent offered for resumption: {interrupted}"
    );

    // Version mismatch → -32602 naming both versions.
    let mut wrong = manifest.clone();
    wrong["creatingIntentdVersion"] = serde_json::json!("0.0.1-elsewhere");
    wrong["workspaceId"] = serde_json::json!("ws-other");
    let rejected = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.import.begin","params":{{"manifest":{wrong},"archiveSizeBytes":10,"archiveSha256":"{sha}"}}}}"#
        ),
    )
    .await;
    assert_eq!(
        rejected["error"]["code"].as_i64(),
        Some(-32602),
        "{rejected}"
    );
    let msg = rejected["error"]["message"].as_str().expect("message");
    assert!(
        msg.contains("0.0.1-elsewhere") && msg.contains(env!("CARGO_PKG_VERSION")),
        "error names both versions: {msg}"
    );

    // abort: pending session → true, retired/unknown → false (idempotent).
    let mut ok = manifest.clone();
    ok["workspaceId"] = serde_json::json!("ws-abortable");
    let begun = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"workspace.import.begin","params":{{"manifest":{ok},"archiveSizeBytes":10,"archiveSha256":"{sha}"}}}}"#
        ),
    )
    .await;
    let abort_id = begun["result"]["importId"].as_str().expect("importId");
    let aborted = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":11,"method":"workspace.import.abort","params":{{"importId":"{abort_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(aborted["result"]["aborted"], true, "{aborted}");
    let again = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":12,"method":"workspace.import.abort","params":{{"importId":"{abort_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(again["result"]["aborted"], false, "{again}");

    srv.ws.stop().await;
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// `workspace.export.start` / `.read` / `.finalize` / `.abort` (§5.1): the
/// source-side export lifecycle over the real WSS transport. A subscriber
/// receives the `workspace:transfer:progress` and `:ready` events (§6.5)
/// carrying the manifest + checksum; chunked reads reassemble to the exact
/// archive bytes (checksum-verified — the same contract the FE relies on to
/// relay into `workspace.import.begin`); finalize applies the final status
/// message + archives the source; a second export session is then started
/// and aborted idempotently.
#[tokio::test]
async fn wss_workspace_export_lifecycle() {
    use base64::Engine as _;

    let srv = start(WsOptions::default()).await;

    // Repo-less source workspace (skips the bundler, so no `git` needed).
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WSS Export Source"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("workspace id: {created}"))
        .to_string();

    // Subscribe BEFORE start so this connection sees the build's events.
    let mut sub_ws = connect_ws(srv.port, srv.cfg.clone()).await;
    sub_ws
        .send(Message::Text(
            format!(
                r#"{{"jsonrpc":"2.0","id":100,"method":"events.subscribe","params":{{"eventTypes":["workspace:transfer:progress","workspace:transfer:ready","workspace:transfer:failed"],"workspaceId":"{ws_id}"}}}}"#
            )
            .into(),
        ))
        .await
        .expect("send subscribe");
    let sub = loop {
        match sub_ws.next().await {
            Some(Ok(Message::Text(text))) => {
                break serde_json::from_str::<Value>(&text).expect("json")
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    };
    assert!(
        sub["result"]["subscriptionId"].is_string(),
        "subscribe: {sub}"
    );

    // start → { exportId, maxChunkBytes } immediately.
    let started = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.export.start","params":{{"workspaceId":"{ws_id}"}}}}"#
        ),
    )
    .await;
    let export_id = started["result"]["exportId"]
        .as_str()
        .unwrap_or_else(|| panic!("exportId: {started}"))
        .to_string();
    assert!(started["result"]["maxChunkBytes"].as_u64().unwrap() > 0);

    // The subscriber sees ≥1 progress event and then the ready event whose
    // payload carries everything workspace.import.begin needs.
    let (saw_progress, ready) = tokio::time::timeout(Duration::from_secs(30), async {
        let mut saw_progress = false;
        loop {
            match sub_ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] != "events.event" {
                        continue;
                    }
                    let event = &v["params"]["event"];
                    match event["type"].as_str() {
                        Some("workspace:transfer:progress") => {
                            assert_eq!(event["data"]["exportId"], export_id.as_str(), "{event}");
                            assert!(event["data"]["stage"].is_string(), "{event}");
                            saw_progress = true;
                        }
                        Some("workspace:transfer:ready") => {
                            return (saw_progress, event.clone());
                        }
                        Some("workspace:transfer:failed") => {
                            panic!("export failed: {event}");
                        }
                        _ => {}
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub_ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for workspace:transfer:ready");
    assert!(saw_progress, "at least one progress event before ready");
    let data = &ready["data"];
    assert_eq!(data["exportId"], export_id.as_str(), "{ready}");
    assert_eq!(data["manifest"]["workspaceId"], ws_id.as_str(), "{ready}");
    let size = data["archiveSizeBytes"].as_u64().expect("size");
    let sha = data["archiveSha256"].as_str().expect("sha").to_string();
    let total_chunks = data["totalChunks"].as_u64().expect("totalChunks");
    assert!(size > 0 && total_chunks >= 1);

    // Chunked reads reassemble to the exact archive; seq 0 re-read is
    // idempotent and an out-of-range seq is rejected with -32602.
    let mut archive = Vec::new();
    for seq in 0..total_chunks {
        let chunk = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"workspace.export.read","params":{{"exportId":"{export_id}","seq":{seq}}}}}"#,
                10 + seq
            ),
        )
        .await;
        assert_eq!(chunk["result"]["totalChunks"], total_chunks, "{chunk}");
        archive.extend(
            base64::engine::general_purpose::STANDARD
                .decode(chunk["result"]["data"].as_str().expect("data"))
                .expect("base64"),
        );
    }
    assert_eq!(archive.len() as u64, size);
    assert_eq!(sha256_hex(&archive), sha);
    let reread = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":30,"method":"workspace.export.read","params":{{"exportId":"{export_id}","seq":0}}}}"#
        ),
    )
    .await;
    assert!(reread["result"]["data"].is_string(), "{reread}");
    let out_of_range = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":31,"method":"workspace.export.read","params":{{"exportId":"{export_id}","seq":{total_chunks}}}}}"#
        ),
    )
    .await;
    assert_eq!(out_of_range["error"]["code"].as_i64(), Some(-32602));

    // finalize with archiveSource + finalStatusMessage.
    let finalized = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":32,"method":"workspace.export.finalize","params":{{"exportId":"{export_id}","archiveSource":true,"finalStatusMessage":"Transferred via WSS e2e"}}}}"#
        ),
    )
    .await;
    assert_eq!(finalized["result"]["finalized"], true, "{finalized}");
    assert_eq!(
        finalized["result"]["workspace"]["status"], "Archived",
        "{finalized}"
    );
    assert_eq!(
        finalized["result"]["workspace"]["statusMessage"], "Transferred via WSS e2e",
        "{finalized}"
    );
    // The session is retired: further reads are NotFound (-32603 internal
    // taxonomy is not used here — NotFound maps to -32602 per §9).
    let gone = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":33,"method":"workspace.export.read","params":{{"exportId":"{export_id}","seq":0}}}}"#
        ),
    )
    .await;
    assert_eq!(gone["error"]["code"], -32602, "read after finalize: {gone}");
    assert_eq!(
        gone["error"]["data"]["code"], "not-found",
        "read after finalize: {gone}"
    );

    // abort: ready session → true; retired/unknown → false (idempotent).
    // (A Building session stays registered until the build task observes
    // the abort flag, so wait for :ready to make the second abort's `false`
    // deterministic.)
    let ws2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":40,"method":"workspace.create","params":{"title":"WSS Export Abort"}}"#,
    )
    .await;
    let ws2_id = ws2["result"]["workspace"]["id"].as_str().expect("id");
    let mut sub2 = connect_ws(srv.port, srv.cfg.clone()).await;
    sub2.send(Message::Text(
        format!(
            r#"{{"jsonrpc":"2.0","id":101,"method":"events.subscribe","params":{{"eventTypes":["workspace:transfer:ready","workspace:transfer:failed"],"workspaceId":"{ws2_id}"}}}}"#
        )
        .into(),
    ))
    .await
    .expect("send subscribe 2");
    loop {
        match sub2.next().await {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
                if v.get("id") == Some(&serde_json::json!(101)) {
                    break;
                }
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let started2 = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":41,"method":"workspace.export.start","params":{{"workspaceId":"{ws2_id}"}}}}"#
        ),
    )
    .await;
    let export2 = started2["result"]["exportId"].as_str().expect("exportId");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match sub2.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json");
                    if v["method"] == "events.event"
                        && v["params"]["event"]["type"] == "workspace:transfer:ready"
                    {
                        return;
                    }
                    assert!(
                        v["params"]["event"]["type"] != "workspace:transfer:failed",
                        "second export failed: {v}"
                    );
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub2.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for second export's ready event");
    let aborted = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":42,"method":"workspace.export.abort","params":{{"exportId":"{export2}"}}}}"#
        ),
    )
    .await;
    assert_eq!(aborted["result"]["aborted"], true, "{aborted}");
    let again = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":43,"method":"workspace.export.abort","params":{{"exportId":"{export2}"}}}}"#
        ),
    )
    .await;
    assert_eq!(again["result"]["aborted"], false, "{again}");
    // The aborted workspace is intact and still Active.
    let get = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":44,"method":"workspace.get","params":{{"workspaceId":"{ws2_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(get["result"]["workspace"]["status"], "Active", "{get}");

    srv.ws.stop().await;
}

/// `workspace.import.commit` with a git payload over the real WSS transport
/// (§5.1): the archive carries `git/repo.bundle` + `git/refs.json` built by
/// the export bundler from a dirty source repo. Commit materializes the
/// checkout under the daemon's workspaces root (WIP snapshot unwound — the
/// dirty file restored, not committed), rewrites the stored workspace row
/// (`repositoryPath` → the checkout, `worktreePath` cleared, `checkoutMode`
/// direct) — WITHOUT registering the workspace-owned checkout in
/// `known_repo` (intent-hq/monorepo#2227). Skips when `git` is unavailable
/// on PATH (the bundler shells out to it).
#[tokio::test]
async fn wss_workspace_import_commit_materializes_git() {
    use base64::Engine as _;
    use std::io::Write as _;

    if std::process::Command::new("git")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_or(true, |s| !s.success())
    {
        eprintln!("skipping WSS git import E2E: git not available");
        return;
    }

    let srv = start(WsOptions::default()).await;
    let ws_id = "ws-wss-git-imported";
    let t = "2026-08-11T00:00:00Z";

    // Source repo: one commit on `main` plus an untracked file, so the
    // bundle carries a WIP snapshot the import must unwind.
    let src = test_tempdir("intentd-wss-import-git-src-");
    let repo = src.path().join("source-repo");
    {
        std::fs::create_dir_all(&repo).expect("mkdir repo");
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "Tester")
                .env("GIT_AUTHOR_EMAIL", "t@e.dev")
                .env("GIT_COMMITTER_NAME", "Tester")
                .env("GIT_COMMITTER_EMAIL", "t@e.dev")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            assert!(ok, "git {args:?}");
        };
        git(&["init", "--quiet", "-b", "main"]);
        std::fs::write(repo.join("README.md"), "hello\n").expect("seed file");
        git(&["add", "README.md"]);
        git(&["commit", "-q", "-m", "chore: init"]);
        std::fs::write(repo.join("wip.txt"), "uncommitted\n").expect("wip file");
    }

    // Bundle the source exactly as the export orchestrator would.
    let mut src_ws = fixture_workspace(&WorkspaceId(ws_id.to_string()));
    src_ws.repository_path = Some(repo.to_string_lossy().into_owned());
    src_ws.repository_name = Some("test-repo".to_string());
    let staging = src.path().join("staging");
    let (bundle_path, refs) =
        intent_services::transfer_git::create_transfer_bundle(&src_ws, &[], &staging)
            .expect("bundle");
    assert!(
        refs.workspace_wip_commit_sha.is_some(),
        "source was dirty: {refs:?}"
    );

    let manifest = serde_json::json!({
        "formatVersion": intent_core::transfer::TRANSFER_FORMAT_VERSION,
        "creatingIntentdVersion": env!("CARGO_PKG_VERSION"),
        "workspaceId": ws_id,
        "createdAt": t,
        "tables": [],
        "assets": [],
        "git": { "hasRepository": true, "dirtyFiles": [], "sandboxBranches": [] }
    });
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buf);
    let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
    zip.start_file("manifest.json", options).expect("manifest");
    zip.write_all(manifest.to_string().as_bytes())
        .expect("manifest bytes");
    zip.start_file("rows/workspace.jsonl", options)
        .expect("rows");
    zip.write_all(
        format!(
            "{}\n",
            serde_json::json!({
                "id": ws_id, "title": "WSS Git Imported", "branch": "main",
                "status": "Active",
                "repository_path": repo.to_string_lossy(),
                "repository_name": "test-repo",
                "created_at": t, "updated_at": t
            })
        )
        .as_bytes(),
    )
    .expect("workspace row");
    zip.start_file("git/repo.bundle", options).expect("bundle");
    zip.write_all(&std::fs::read(&bundle_path).expect("bundle bytes"))
        .expect("bundle write");
    zip.start_file("git/refs.json", options).expect("refs");
    zip.write_all(serde_json::to_string(&refs).expect("refs json").as_bytes())
        .expect("refs write");
    zip.finish().expect("zip");
    let archive = buf.into_inner();
    let sha = sha256_hex(&archive);

    // begin → chunk → commit over the wire.
    let begin = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.import.begin","params":{{"manifest":{manifest},"archiveSizeBytes":{},"archiveSha256":"{sha}"}}}}"#,
            archive.len()
        ),
    )
    .await;
    let import_id = begin["result"]["importId"]
        .as_str()
        .unwrap_or_else(|| panic!("importId: {begin}"))
        .to_string();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&archive);
    let chunk = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.import.chunk","params":{{"importId":"{import_id}","seq":0,"data":"{b64}"}}}}"#
        ),
    )
    .await;
    assert_eq!(chunk["result"]["seq"], 0, "{chunk}");
    let committed = wss_call(
        srv.port,
        srv.cfg.clone(),
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"workspace.import.commit","params":{{"importId":"{import_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(committed["result"]["workspace"]["id"], ws_id, "{committed}");

    // The result's workspace payload carries the materialized checkout
    // (PROTOCOL §5.1: the workspace envelope after import transforms).
    let ws_payload = &committed["result"]["workspace"];
    let checkout = srv
        .dir
        .path()
        .join("workspaces")
        .join(ws_id)
        .join("test-repo");
    assert_eq!(
        ws_payload["repositoryPath"].as_str(),
        checkout.to_str(),
        "{committed}"
    );
    assert!(ws_payload["worktreePath"].is_null(), "{committed}");
    assert_eq!(ws_payload["checkoutMode"], "direct", "{committed}");

    // On disk: checkout exists with the WIP snapshot unwound — the dirty
    // file restored as uncommitted work.
    assert_eq!(
        std::fs::read_to_string(checkout.join("README.md")).expect("committed file"),
        "hello\n"
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("wip.txt")).expect("wip file restored"),
        "uncommitted\n"
    );

    // The materialized checkout is workspace-owned storage under the
    // workspaces root and stays out of known_repo (intent-hq/monorepo#2227).
    assert_eq!(
        srv.store.list_known_repos().await.expect("known repos"),
        vec![],
        "materialized checkout is not registered in known_repo"
    );

    srv.ws.stop().await;
}

/// Helper to obtain an ephemeral port by bind-then-release. Only used for tests
/// that genuinely need a fixed port to exercise fixed-port semantics (e.g.
/// `graceful_shutdown_allows_immediate_restart`). Prefer `base_port: 0` for normal tests.
fn free_port() -> u16 {
    use std::net::TcpListener;
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

// ---------------------------------------------------------------------------
// RFC 7692 permessage-deflate negotiation on the WSS listener (monorepo#1971).
// ---------------------------------------------------------------------------

/// A transparent [`tokio::io::AsyncRead`]/[`tokio::io::AsyncWrite`] wrapper
/// that counts the bytes crossing the underlying socket in each direction.
/// Installed on the raw TCP stream *beneath* TLS, so the counts reflect real
/// on-the-wire traffic (TLS records included).
struct CountingStream<S> {
    inner: S,
    read: Arc<std::sync::atomic::AtomicUsize>,
    written: Arc<std::sync::atomic::AtomicUsize>,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountingStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &poll {
            self.read.fetch_add(
                buf.filled().len() - before,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        poll
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CountingStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let poll = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &poll {
            self.written
                .fetch_add(*n, std::sync::atomic::Ordering::Relaxed);
        }
        poll
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Byte counters for one instrumented connection: (bytes read, bytes written)
/// at the TCP layer.
type WireCounters = (
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicUsize>,
);

/// An instrumented authenticated WSS connection: TCP → [`CountingStream`] →
/// pinned TLS → WebSocket upgrade, optionally offering permessage-deflate.
/// Returns the socket, the handshake response (for `Sec-WebSocket-Extensions`
/// assertions), and the wire counters.
async fn connect_ws_counting(
    port: u16,
    cfg: Arc<ClientConfig>,
    offer_deflate: bool,
) -> (
    tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<CountingStream<TcpStream>>>,
    tokio_tungstenite::tungstenite::handshake::client::Response,
    WireCounters,
) {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
    let _ = tcp.set_nodelay(true);
    let read = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let written = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = CountingStream {
        inner: tcp,
        read: read.clone(),
        written: written.clone(),
    };
    let name = ServerName::try_from("localhost").unwrap();
    let tls = tokio_rustls::TlsConnector::from(cfg)
        .connect(name, counted)
        .await
        .expect("tls connect");
    let mut ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    if offer_deflate {
        ws_config.extensions.permessage_deflate = Some(
            tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig::default(),
        );
    }
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, resp) = tokio_tungstenite::client_async_with_config(url, tls, Some(ws_config))
        .await
        .expect("ws upgrade");
    (ws, resp, (read, written))
}

/// Drive several JSON-RPC frames over an already-open socket, returning one
/// parsed response per frame (same shape as [`wss_session`], generic over the
/// instrumented stream).
async fn drive_frames<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    frames: Vec<String>,
) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut out = Vec::new();
    for frame in frames {
        ws.send(Message::Text(frame.into())).await.expect("send");
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    out.push(serde_json::from_str(&text).expect("json"));
                    break;
                }
                Some(Ok(_)) => {}
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }
    out
}

/// One draft round-trip carrying `payload` over an instrumented connection:
/// `client.hello` → `workspace.create` → `drafts.set` → `drafts.get`. Returns
/// the text echoed back by `drafts.get`.
async fn draft_round_trip<S>(
    ws: &mut tokio_tungstenite::WebSocketStream<S>,
    client_id: &str,
    payload: &str,
) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = drive_frames(
        ws,
        vec![
            format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"client.hello","params":{{"clientId":"{client_id}"}}}}"#
            ),
            format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"workspace.create","params":{{"title":"Deflate {client_id}"}}}}"#
            ),
        ],
    )
    .await;
    let ws_id = resp[1]["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let set = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "drafts.set",
        "params": { "workspaceId": ws_id, "agentId": "agent-deflate", "text": payload }
    });
    let get = serde_json::json!({
        "jsonrpc": "2.0", "id": 4, "method": "drafts.get",
        "params": { "workspaceId": ws_id, "agentId": "agent-deflate" }
    });
    let resp = drive_frames(ws, vec![set.to_string(), get.to_string()]).await;
    assert_eq!(resp[0]["result"]["ok"], true, "drafts.set: {}", resp[0]);
    resp[1]["result"]["text"]
        .as_str()
        .expect("draft text")
        .to_string()
}

/// E2E (monorepo#1971): a client offering `permessage-deflate` negotiates
/// compression on the real WSS transport — the 101 response echoes the agreed
/// extension and a highly compressible JSON payload is measurably smaller on
/// the wire than the same payload over a non-offering control connection,
/// which itself sees no `Sec-WebSocket-Extensions` header and identical
/// payload semantics (today's behavior).
#[tokio::test]
async fn wss_deflate_negotiation_compresses_on_the_wire() {
    let srv = start(WsOptions::default()).await;
    // ~128 KiB of highly compressible text, well past any handshake noise.
    let payload = "intentd permessage-deflate ".repeat(5000);

    // Deflate-offering connection.
    let (mut ws, resp, (read, written)) =
        connect_ws_counting(srv.port, srv.cfg.clone(), true).await;
    let agreed = resp
        .headers()
        .get("sec-websocket-extensions")
        .expect("server echoes the negotiated extension")
        .to_str()
        .expect("ascii header");
    assert!(
        agreed.starts_with("permessage-deflate"),
        "negotiated header names the extension: {agreed}"
    );
    let echoed = draft_round_trip(&mut ws, "cli-deflate", &payload).await;
    assert_eq!(
        echoed, payload,
        "payload survives the compressed round-trip intact"
    );
    let _ = ws.close(None).await;
    let deflate_read = read.load(std::sync::atomic::Ordering::Relaxed);
    let deflate_written = written.load(std::sync::atomic::Ordering::Relaxed);

    // Control: identical session over a non-offering connection.
    let (mut ws, resp, (read, written)) =
        connect_ws_counting(srv.port, srv.cfg.clone(), false).await;
    assert!(
        resp.headers().get("sec-websocket-extensions").is_none(),
        "no offer ⇒ no Sec-WebSocket-Extensions in the 101 response"
    );
    let echoed = draft_round_trip(&mut ws, "cli-plain", &payload).await;
    assert_eq!(echoed, payload, "control round-trip intact");
    let _ = ws.close(None).await;
    let plain_read = read.load(std::sync::atomic::Ordering::Relaxed);
    let plain_written = written.load(std::sync::atomic::Ordering::Relaxed);

    // The compressible payload dominates both directions (~128 KiB each way
    // uncompressed); compression must shrink the wire traffic dramatically.
    // "< half" is a deliberately loose bound — in practice it is >90% smaller.
    assert!(
        deflate_read < plain_read / 2,
        "server→client traffic compressed: deflate={deflate_read}B plain={plain_read}B"
    );
    assert!(
        deflate_written < plain_written / 2,
        "client→server traffic compressed: deflate={deflate_written}B plain={plain_written}B"
    );
    srv.ws.stop().await;
}

/// E2E control (monorepo#1971): a client that offers an unacceptable
/// extension set is declined per RFC 7692 §7 — no `Sec-WebSocket-Extensions`
/// in the 101 response — and gets a clean uncompressed connection with a
/// working JSON-RPC round-trip, byte-identical behavior to a client that
/// never offered.
#[tokio::test]
async fn wss_unacceptable_extension_offer_declines_to_plain_connection() {
    let srv = start(WsOptions::default()).await;

    // Hand-rolled upgrade with an offer the server must decline (unknown
    // parameter, RFC 7692 §7). `tls_connect` + raw HTTP keeps the client free
    // to send an arbitrary header the fork's client API would never produce.
    let mut tls = tls_connect(srv.port, srv.cfg.clone()).await;
    let req = format!(
        "GET /ws?token={TOKEN} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Extensions: permessage-deflate; parameter-from-the-future=3\r\n\r\n"
    );
    tls.write_all(req.as_bytes()).await.expect("write upgrade");
    tls.flush().await.expect("flush");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        assert!(
            tls.read(&mut byte).await.expect("read head") == 1,
            "connection closed before 101 head"
        );
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head);
    assert!(
        head.starts_with("HTTP/1.1 101"),
        "declined offer still upgrades: {head}"
    );
    assert!(
        !head
            .to_ascii_lowercase()
            .contains("sec-websocket-extensions"),
        "declined offer ⇒ no extensions header in the 101 response: {head}"
    );

    // The upgraded socket is a plain uncompressed WebSocket: a JSON-RPC
    // round-trip behaves exactly as today.
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        tls,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let resp = drive_frames(
        &mut ws,
        vec![r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list","params":{}}"#.to_string()],
    )
    .await;
    assert!(
        resp[0]["result"]["workspaces"].is_array(),
        "plain round-trip works after the declined offer: {}",
        resp[0]
    );
    let _ = ws.close(None).await;
    srv.ws.stop().await;
}

/// Minimal [`SystemControl`] whose `fileWatch` comes from a real
/// [`WatchHealth`] handle — the same mapping the composition root (main.rs)
/// performs — so the WSS e2e below exercises the real
/// registry → hub → snapshot → render path. Every other field is a fixed
/// plausible value; only `fileWatch` is under test.
struct WatchHealthControl {
    health: WatchHealth,
}

impl SystemControl for WatchHealthControl {
    fn status(&self) -> SystemStatus {
        SystemStatus {
            listen_mode: "both".to_string(),
            uds: true,
            tcp: true,
            port: None,
            clients: 1,
            agents: 0,
            fingerprint: None,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            has_display: false,
            max_agents: 1,
            version: "0.0.0-test".to_string(),
            build_commit: None,
            uptime_seconds: 0,
            local_ips: Vec::new(),
            tc_address: None,
            hostname: "test".to_string(),
            pretty_hostname: "test".to_string(),
            device_kind: None,
            hardware_model: None,
            cpu_percent: 0.0,
            memory_bytes: 0,
            child_processes: None,
            child_memory_bytes: None,
            child_memory_peak_bytes: None,
            agent_memory_budget_bytes: None,
            agent_memory_charged_bytes: None,
            queued_spawns: None,
            workspaces_disk_available_bytes: None,
            workspaces_disk_total_bytes: None,
            file_watch: self.health.snapshot().map(|s| FileWatchStatus {
                active_streams: s.active_streams,
                total_roots: s.total_roots,
                failed_roots: s.failed_roots,
            }),
            update_supported: false,
        }
    }
    fn host_environment(&self) -> intent_transport::HostEnvironment {
        intent_transport::HostEnvironment {
            hostname: "test".to_string(),
            pretty_hostname: "test".to_string(),
            device_kind: None,
            hardware_model: None,
        }
    }
    fn request_shutdown(&self) {}
    fn request_update(&self) -> std::result::Result<(), String> {
        Err("not supported in this test".to_string())
    }
    fn import_legacy(
        &self,
        _force: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<Value, String>> + Send + '_>,
    > {
        Box::pin(async { Err("not supported in this test".to_string()) })
    }
    fn git_credential(
        &self,
        _client_pid: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(String, String)>> + Send + '_>>
    {
        Box::pin(async { None })
    }
}

/// Watch-coverage health over the real WSS transport (intent-hq/intent#3708):
/// `fileWatch` is absent from `system.status` until the watcher registry
/// attaches the [`WatchHealth`] handle, then present — including when healthy
/// (`failedRoots: 0`) — through the same TLS + bearer-auth upgrade path
/// production clients use. The degraded (watcher-creation-failure) shape is
/// covered end-to-end over UDS in `uds_control.rs`; both transports share one
/// render path (`status_json`), so this presence/shape proof carries over.
#[tokio::test]
async fn system_status_surfaces_file_watch_coverage_over_wss() {
    let dir = test_tempdir("intentd-wss-fw-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir workspaces root");
    let registry = Arc::new(
        intent_services::SettingsRegistry::load(dir.path().join("config.toml"))
            .expect("load settings registry"),
    );
    let services = Services::new(store.clone())
        .with_assets_root(dir.path().join("assets"))
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let status_cache = services.git_status_cache();
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Health handle created BEFORE the registry, as in the composition root:
    // registry init is backgrounded in production, so `fileWatch` must read as
    // absent until attachment.
    let health = WatchHealth::default();
    let control: Arc<dyn SystemControl> = Arc::new(WatchHealthControl {
        health: health.clone(),
    });

    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..WsOptions::default()
    };
    let ws = WsApiServer::new(
        api.clone(),
        bus.clone(),
        &tls,
        &token_store,
        opts,
        Some(control),
    )
    .expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");

    // Before the registry starts: the whole `fileWatch` object is absent, not
    // null — presence-detected on the wire.
    let frame = r#"{"jsonrpc":"2.0","id":1,"method":"system.status"}"#;
    let resp = wss_call(port, cfg.clone(), frame).await;
    assert_eq!(resp["id"], 1, "id echoed: {resp}");
    assert!(
        resp["result"].is_object(),
        "system.status over WSS returns a result: {resp}"
    );
    assert!(
        resp["result"].get("fileWatch").is_none(),
        "fileWatch must be absent before the registry attaches: {resp}"
    );

    // Start the real watcher registry (attaches `health` to the hub). No
    // workspaces exist, so the healthy snapshot is present with zero counts —
    // present-when-healthy is the contract, letting clients distinguish
    // "healthy" from "not yet measurable".
    let refresher = Arc::new(GitStatusRefresher::start(
        bus.clone(),
        api.clone(),
        status_cache,
    ));
    let _watcher_registry =
        WatcherRegistry::start_with_health(bus.clone(), api.clone(), refresher, &health).await;

    let resp = wss_call(port, cfg, frame).await;
    let fw = &resp["result"]["fileWatch"];
    assert!(
        fw.is_object(),
        "fileWatch must appear once the registry attaches: {resp}"
    );
    assert_eq!(fw["failedRoots"], 0, "healthy daemon: {resp}");
    assert!(fw["activeStreams"].is_u64(), "activeStreams: {resp}");
    assert!(fw["totalRoots"].is_u64(), "totalRoots: {resp}");
    ws.stop().await;
}

/// Symlink-aware workspace containment over the real WSS wire
/// (intent-hq/intent#3847): a symlink planted inside a rooted workspace that
/// points outside the root passes the lexical prefix guard, but the OS
/// follows it — `file.read` / `file.write` through such a link must be
/// rejected with the containment `-32603` ("Access denied" rides in
/// `error.data`, not `error.message`), and the outside target must be
/// neither read out nor modified. A control read through an IN-workspace
/// symlink still succeeds, proving the gate rejects only escaping links.
#[cfg(unix)]
#[tokio::test]
async fn wss_file_ops_symlink_escape_rejected() {
    let srv = start(WsOptions::default()).await;

    let dir = test_tempdir("intentd-wss-symlink-ws-");
    let root = std::fs::canonicalize(dir.path()).expect("canonicalize root");
    let outside = test_tempdir("intentd-wss-symlink-outside-");
    let outside_root = std::fs::canonicalize(outside.path()).expect("canonicalize outside");
    std::fs::write(outside_root.join("secret.txt"), "top secret").expect("write outside fixture");
    std::os::unix::fs::symlink(&outside_root, root.join("escape")).expect("plant escape symlink");

    let ws_id = WorkspaceId::new();
    let mut w = fixture_workspace(&ws_id);
    w.worktree_path = Some(root.to_string_lossy().into_owned());
    srv.store.insert_workspace(&w).await.expect("insert ws");

    // Read THROUGH the escaping symlink → containment -32603, no content out.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"file.read","params":{{"workspaceId":"{}","path":"escape/secret.txt"}}}}"#,
        ws_id.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
    assert!(
        resp["error"]["data"]
            .as_str()
            .is_some_and(|m| m.contains("Access denied")),
        "{resp}"
    );

    // Write a NEW file through the link → -32603, nothing lands outside.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"file.write","params":{{"workspaceId":"{}","path":"escape/planted.txt","content":"x"}}}}"#,
        ws_id.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
    assert!(
        !outside_root.join("planted.txt").exists(),
        "escape write must not land outside the workspace"
    );

    // Overwrite the EXISTING outside file through the link → -32603, bytes intact.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"file.write","params":{{"workspaceId":"{}","path":"escape/secret.txt","content":"clobbered"}}}}"#,
        ws_id.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["error"]["code"].as_i64(), Some(-32603), "{resp}");
    assert_eq!(
        std::fs::read_to_string(outside_root.join("secret.txt")).expect("outside file readable"),
        "top secret",
        "outside target must not be modified"
    );

    // Control: an in-workspace symlink to an in-workspace target still works,
    // so the rejections above are the symlink gate, not a broken file path.
    std::fs::create_dir_all(root.join("data")).expect("mkdir data");
    std::fs::write(root.join("data/real.txt"), "ok").expect("write in-ws fixture");
    std::os::unix::fs::symlink(root.join("data"), root.join("alias")).expect("in-ws symlink");
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":4,"method":"file.read","params":{{"workspaceId":"{}","path":"alias/real.txt"}}}}"#,
        ws_id.0
    );
    let resp = wss_call(srv.port, srv.cfg.clone(), &frame).await;
    assert_eq!(resp["result"], serde_json::json!("ok"), "{resp}");

    srv.ws.stop().await;
}
