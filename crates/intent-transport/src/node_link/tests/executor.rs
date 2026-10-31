//! The executor link served on the daemon's own WSS listener: the full
//! admission matrix (path → enabled flags → Origin → executor-only token),
//! per-credential binding, git admission, frame scope and revocation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, http, Message};

use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{BindDecision, JournalBody, JournalRecord};
use intent_core::{BoxFuture, ExecutorCredentialId, HostRole, PrincipalId, WorkspaceApi};
use intent_services::EventBus;

use super::super::pin;
use super::*;
use crate::{AsyncTokenStore, TokenStore, WsApiServer, WsOptions};

const MACHINE: &str = "machine-1";
const BACKEND: &str = "backend-1";
const OTHER_BACKEND: &str = "backend-2";

fn owner_token() -> String {
    "o".repeat(64)
}
fn member_token() -> String {
    "m".repeat(64)
}
fn executor_token() -> String {
    "e".repeat(64)
}
fn other_executor_token() -> String {
    "f".repeat(64)
}
fn revoked_token() -> String {
    "r".repeat(64)
}
/// Resolves as active at the gate but is gone by the bind (a revocation
/// racing the upgrade).
fn racing_token() -> String {
    "g".repeat(64)
}

fn credential(backend: &str) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::new(),
        backend_id: backend.into(),
        backend_name: "Backend".into(),
        backend_fingerprint: None,
        created_at: "2026-10-30T00:00:00Z".into(),
        last_used_at: None,
        revoked_at: None,
    }
}

struct OwnerToken;

impl TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some(owner_token())
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

/// The daemon's executor seams, in memory, with the store's bind rules.
struct MachineApi {
    principal: PrincipalId,
    member_hash: String,
    by_hash: Mutex<HashMap<String, ExecutorCredential>>,
    binds: Mutex<HashMap<ExecutorCredentialId, LeaseBinding>>,
    unbindable: Mutex<Vec<ExecutorCredentialId>>,
    revocations: broadcast::Sender<ExecutorCredentialId>,
    lookups: Mutex<Vec<String>>,
}

impl MachineApi {
    fn new() -> Self {
        Self {
            principal: PrincipalId::new(),
            member_hash: crate::auth::hash_token(&member_token()),
            by_hash: Mutex::new(HashMap::new()),
            binds: Mutex::new(HashMap::new()),
            unbindable: Mutex::new(Vec::new()),
            revocations: broadcast::channel(16).0,
            lookups: Mutex::new(Vec::new()),
        }
    }

    fn add(&self, token: &str, credential: ExecutorCredential) -> ExecutorCredential {
        self.by_hash
            .lock()
            .unwrap()
            .insert(crate::auth::hash_token(token), credential.clone());
        credential
    }

    fn revoke(&self, id: &ExecutorCredentialId) {
        for credential in self.by_hash.lock().unwrap().values_mut() {
            if &credential.id == id {
                credential.revoked_at = Some("2026-10-30T01:00:00Z".into());
            }
        }
        let _ = self.revocations.send(id.clone());
    }

    fn binding_of(&self, id: &ExecutorCredentialId) -> Option<LeaseBinding> {
        self.binds.lock().unwrap().get(id).cloned()
    }
}

impl WorkspaceApi for MachineApi {
    fn primary_principal_id(&self) -> BoxFuture<'_, intent_core::Result<PrincipalId>> {
        Box::pin(async move { Ok(self.principal.clone()) })
    }

    fn principal_host_role(
        &self,
        _id: PrincipalId,
    ) -> BoxFuture<'_, intent_core::Result<HostRole>> {
        Box::pin(async { Ok(HostRole::Member) })
    }

    fn resolve_principal_credential(
        &self,
        token_hash: String,
    ) -> BoxFuture<'_, intent_core::Result<Option<PrincipalId>>> {
        Box::pin(
            async move { Ok((token_hash == self.member_hash).then(|| self.principal.clone())) },
        )
    }

    fn resolve_executor_credential(
        &self,
        token_hash: String,
    ) -> BoxFuture<'_, intent_core::Result<Option<ExecutorCredential>>> {
        Box::pin(async move {
            self.lookups.lock().unwrap().push(token_hash.clone());
            Ok(self
                .by_hash
                .lock()
                .unwrap()
                .get(&token_hash)
                .filter(|c| c.is_active())
                .cloned())
        })
    }

    fn bind_executor(
        &self,
        credential_id: ExecutorCredentialId,
        node_id: String,
        offered: LeaseBinding,
    ) -> BoxFuture<'_, intent_core::Result<BindDecision>> {
        Box::pin(async move {
            let credential = self
                .by_hash
                .lock()
                .unwrap()
                .values()
                .find(|c| c.id == credential_id && c.is_active())
                .cloned();
            let Some(credential) = credential else {
                return Err(intent_core::Error::NotFound("executor credential".into()));
            };
            if self.unbindable.lock().unwrap().contains(&credential_id) {
                return Err(intent_core::Error::NotFound("executor credential".into()));
            }
            if offered.node_id == node_id && offered.head_id != credential.backend_id {
                return Ok(BindDecision::OtherHead);
            }
            let mut binds = self.binds.lock().unwrap();
            let decision = LeaseBinding::decide(&node_id, binds.get(&credential_id), &offered);
            if decision == BindDecision::Accept {
                binds.insert(credential_id, offered);
            }
            Ok(decision)
        })
    }

    fn subscribe_executor_revocations(&self) -> Option<broadcast::Receiver<ExecutorCredentialId>> {
        Some(self.revocations.subscribe())
    }
}

/// The executor service stand-in.
struct Host {
    enabled: AtomicBool,
    accepted: mpsc::UnboundedSender<(ExecutorCredentialId, NodeConnection)>,
}

impl ExecutorHost for Host {
    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
    fn node_id(&self) -> String {
        MACHINE.into()
    }
    fn welcome(&self, _credential: &ExecutorCredential, hello: &LinkHello) -> LinkWelcome {
        let mut welcome = (welcome())(hello);
        welcome.hosts = vec!["10.0.0.7".into()];
        welcome
    }
    fn accept(&self, credential: ExecutorCredential, connection: NodeConnection) {
        let _ = self.accepted.send((credential.id, connection));
    }
}

enum Mode {
    /// No executor service installed.
    NoHost,
    Secure {
        ws_enabled: bool,
    },
    Insecure,
}

struct Machine {
    server: WsApiServer,
    port: u16,
    certificate: TlsCertificate,
    api: Arc<MachineApi>,
    host: Option<Arc<Host>>,
    accepted: mpsc::UnboundedReceiver<(ExecutorCredentialId, NodeConnection)>,
    executor: ExecutorCredential,
    other: ExecutorCredential,
    racing: ExecutorCredential,
    _dir: tempfile::TempDir,
}

impl Machine {
    async fn start(mode: Mode) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bus = EventBus::new(
            intent_store::Store::open(&dir.path().join("bus.db"))
                .await
                .unwrap(),
        );
        let api = Arc::new(MachineApi::new());
        let executor = api.add(&executor_token(), credential(BACKEND));
        let other = api.add(&other_executor_token(), credential(OTHER_BACKEND));
        let racing = api.add(&racing_token(), credential(BACKEND));
        api.unbindable.lock().unwrap().push(racing.id.clone());
        let revoked = api.add(&revoked_token(), credential(BACKEND));
        api.revoke(&revoked.id);
        let certificate = certificate();
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
        let options = WsOptions {
            base_port: 0,
            enabled: !matches!(mode, Mode::Secure { ws_enabled: false }),
            ..WsOptions::default()
        };
        let mut server = if matches!(mode, Mode::Insecure) {
            WsApiServer::new_insecure(api.clone(), bus, options, None)
        } else {
            WsApiServer::new(api.clone(), bus, &certificate, &tokens, options, None).unwrap()
        };
        let (tx, accepted) = mpsc::unbounded_channel();
        let host = (!matches!(mode, Mode::NoHost)).then(|| {
            Arc::new(Host {
                enabled: AtomicBool::new(true),
                accepted: tx,
            })
        });
        if let Some(host) = &host {
            server.install_executor_links(ExecutorLinks::new(host.clone()));
        }
        let port = server.start().await.unwrap();
        Self {
            server,
            port,
            certificate,
            api,
            host,
            accepted,
            executor,
            other,
            racing,
            _dir: dir,
        }
    }

    fn endpoint(&self, token: &str) -> NodeEndpoint {
        NodeEndpoint {
            url: format!("wss://127.0.0.1:{}", self.port),
            pin: CertificatePin::parse(&self.certificate.fingerprint256).unwrap(),
            token: LeaseToken::new(token).unwrap(),
            tailcat: None,
        }
    }

    fn set_enabled(&self, enabled: bool) {
        self.host
            .as_ref()
            .unwrap()
            .enabled
            .store(enabled, Ordering::SeqCst);
    }

    async fn accepted(&mut self) -> (ExecutorCredentialId, NodeConnection) {
        tokio::time::timeout(wait(), self.accepted.recv())
            .await
            .expect("an admitted connection")
            .unwrap()
    }

    async fn accepted_link(&mut self) -> (ExecutorCredentialId, NodeLink, LinkEvents) {
        match self.accepted().await {
            (id, NodeConnection::Link { link, events, .. }) => (id, link, events),
            (_, other) => panic!("expected a link, got {other:?}"),
        }
    }

    async fn nothing_accepted(&mut self) {
        assert!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(300)),
                self.accepted.recv(),
            )
            .await
            .is_err(),
            "no connection may be admitted"
        );
    }

    /// Upgrade `path` with `headers` and report the HTTP status: `101` on
    /// success (with the socket), the refusal status otherwise.
    async fn upgrade(&self, target: &str, headers: &[(&str, &str)]) -> (u16, Option<Socket>) {
        let tcp = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let mut request = format!("wss://127.0.0.1:{}{target}", self.port)
            .into_client_request()
            .unwrap();
        for (name, value) in headers {
            request.headers_mut().insert(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value).unwrap(),
            );
        }
        let pin = CertificatePin::parse(&self.certificate.fingerprint256).unwrap();
        let tls = TlsConnector::from(Arc::new(pin::client_config(pin).unwrap()))
            .connect(
                rustls_pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        status_of(tokio_tungstenite::client_async(request, MaybeTls::Tls(Box::new(tls))).await)
    }

    async fn upgrade_plain(&self, target: &str, headers: &[(&str, &str)]) -> u16 {
        let tcp = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let mut request = format!("ws://127.0.0.1:{}{target}", self.port)
            .into_client_request()
            .unwrap();
        for (name, value) in headers {
            request.headers_mut().insert(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                http::HeaderValue::from_str(value).unwrap(),
            );
        }
        status_of(tokio_tungstenite::client_async(request, MaybeTls::Plain(tcp)).await).0
    }
}

type Socket = tokio_tungstenite::WebSocketStream<MaybeTls>;

/// A client stream that is TLS or (insecure mode) plain TCP.
enum MaybeTls {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(TcpStream),
}

impl tokio::io::AsyncRead for MaybeTls {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for MaybeTls {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_write(cx, buf),
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_flush(cx),
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_shutdown(cx),
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

fn status_of(
    result: std::result::Result<
        (Socket, tungstenite::handshake::client::Response),
        tungstenite::Error,
    >,
) -> (u16, Option<Socket>) {
    match result {
        Ok((socket, _)) => (101, Some(socket)),
        Err(tungstenite::Error::Http(response)) => (response.status().as_u16(), None),
        Err(other) => panic!("upgrade failed without an HTTP status: {other}"),
    }
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

fn machine_binding(head: &str, generation: u64) -> LeaseBinding {
    LeaseBinding {
        head_id: head.into(),
        node_id: MACHINE.into(),
        lease_id: "lease-1".into(),
        generation: NodeCounter(generation),
    }
}

fn git_headers(binding: &LeaseBinding) -> Vec<(&'static str, String)> {
    vec![
        ("x-intent-head-id", binding.head_id.clone()),
        ("x-intent-node-id", binding.node_id.clone()),
        ("x-intent-lease-id", binding.lease_id.clone()),
        ("x-intent-generation", binding.generation.0.to_string()),
    ]
}

/// Bind `binding` as a raw client and return the socket after the welcome.
async fn raw_bind(machine: &Machine, token: &str, binding: LeaseBinding) -> Socket {
    let auth = bearer(token);
    let (status, socket) = machine
        .upgrade(LINK_PATH, &[("authorization", auth.as_str())])
        .await;
    assert_eq!(status, 101);
    let mut socket = socket.unwrap();
    socket
        .send(Message::Binary(
            LinkFrame::Hello(hello(binding)).encode().unwrap().into(),
        ))
        .await
        .unwrap();
    match tokio::time::timeout(wait(), socket.next()).await.unwrap() {
        Some(Ok(Message::Binary(bytes))) => {
            assert!(matches!(
                LinkFrame::decode(&bytes).unwrap(),
                LinkFrame::Welcome(_)
            ));
        }
        other => panic!("expected a welcome, got {other:?}"),
    }
    socket
}

/// The next close frame on `socket`, skipping heartbeats.
async fn close_frame(socket: &mut Socket) -> Option<(CloseCode, String)> {
    loop {
        match tokio::time::timeout(wait(), socket.next()).await.unwrap() {
            Some(Ok(Message::Close(frame))) => {
                return frame.map(|f| (f.code, f.reason.to_string()))
            }
            Some(Ok(_)) => {}
            _ => return None,
        }
    }
}

const NODE_PATHS: [&str; 2] = [LINK_PATH, GIT_PATH];

#[tokio::test]
async fn node_paths_are_not_found_until_an_executor_service_is_installed() {
    let machine = Machine::start(Mode::NoHost).await;
    let auth = bearer(&executor_token());
    for path in NODE_PATHS {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 404, "{path}");
    }
    assert!(machine.api.lookups.lock().unwrap().is_empty());
}

#[tokio::test]
async fn node_paths_are_forbidden_while_the_listener_api_is_disabled() {
    let machine = Machine::start(Mode::Secure { ws_enabled: false }).await;
    let auth = bearer(&executor_token());
    for path in NODE_PATHS {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 403, "{path}");
    }
    assert!(machine.api.lookups.lock().unwrap().is_empty());
}

#[tokio::test]
async fn node_paths_are_forbidden_while_remote_agents_are_disabled() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    machine.set_enabled(false);
    let auth = bearer(&executor_token());
    for path in NODE_PATHS {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 403, "{path}");
    }
    assert!(
        machine.api.lookups.lock().unwrap().is_empty(),
        "no credential is looked up while remote agents are off"
    );
    machine.nothing_accepted().await;
    machine.set_enabled(true);
    let (status, _) = machine
        .upgrade(LINK_PATH, &[("authorization", auth.as_str())])
        .await;
    assert_eq!(status, 101, "turning the setting on admits the same token");
}

#[tokio::test]
async fn any_origin_header_is_forbidden_on_node_paths() {
    let machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let auth = bearer(&executor_token());
    for origin in ["https://intent.local", "null", "app://intent", ""] {
        for path in NODE_PATHS {
            let (status, _) = machine
                .upgrade(
                    path,
                    &[("authorization", auth.as_str()), ("origin", origin)],
                )
                .await;
            assert_eq!(status, 403, "{path} origin {origin:?}");
        }
    }
    assert!(machine.api.lookups.lock().unwrap().is_empty());
}

#[tokio::test]
async fn only_executor_credentials_are_admitted_on_node_paths() {
    let machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let refused: Vec<(&str, Vec<(&str, String)>)> = vec![
        ("no token", vec![]),
        (
            "owner token",
            vec![("authorization", bearer(&owner_token()))],
        ),
        (
            "member token",
            vec![("authorization", bearer(&member_token()))],
        ),
        (
            "unknown token",
            vec![("authorization", bearer(&"u".repeat(64)))],
        ),
        (
            "revoked executor",
            vec![("authorization", bearer(&revoked_token()))],
        ),
        (
            "basic scheme",
            vec![("authorization", format!("Basic {}", executor_token()))],
        ),
        ("empty bearer", vec![("authorization", "Bearer ".into())]),
    ];
    for path in NODE_PATHS {
        for (case, headers) in &refused {
            let headers: Vec<(&str, &str)> =
                headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
            let (status, _) = machine.upgrade(path, &headers).await;
            assert_eq!(status, 401, "{path}: {case}");
        }
        let query = format!("{path}?token={}", executor_token());
        let (status, _) = machine.upgrade(&query, &[]).await;
        assert_eq!(
            status, 401,
            "{path}: the token is taken from the header only"
        );
    }
    let auth = bearer(&executor_token());
    let (status, _) = machine
        .upgrade(LINK_PATH, &[("authorization", auth.as_str())])
        .await;
    assert_eq!(status, 101, "an active executor credential is admitted");
}

#[tokio::test]
async fn executor_tokens_are_refused_on_every_other_path() {
    let machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let auth = bearer(&executor_token());
    for path in ["/ws", "/tunnel"] {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 401, "{path}");
        let query = format!("{path}?token={}", executor_token());
        let (status, _) = machine.upgrade(&query, &[]).await;
        assert_eq!(status, 401, "{path} by query");
    }
    for path in ["/node", "/node/", "/node/other", "/nodes/link"] {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 404, "{path}");
    }
    assert!(
        machine.api.lookups.lock().unwrap().is_empty(),
        "executor credentials are never looked up off /node/*"
    );
    // The owner token still opens /ws: the refusal is specific to the
    // executor token.
    let owner = bearer(&owner_token());
    let (status, _) = machine
        .upgrade("/ws", &[("authorization", owner.as_str())])
        .await;
    assert_eq!(status, 101);
}

#[tokio::test]
async fn insecure_mode_never_skips_the_executor_token() {
    let machine = Machine::start(Mode::Insecure).await;
    for path in NODE_PATHS {
        assert_eq!(machine.upgrade_plain(path, &[]).await, 401, "{path}");
        let owner = bearer(&owner_token());
        assert_eq!(
            machine
                .upgrade_plain(path, &[("authorization", owner.as_str())])
                .await,
            401,
            "{path}: owner token"
        );
    }
    let auth = bearer(&executor_token());
    assert_eq!(
        machine
            .upgrade_plain(LINK_PATH, &[("authorization", auth.as_str())])
            .await,
        101
    );
}

#[tokio::test]
async fn an_executor_binds_and_reaches_the_host_without_a_guest_seat() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let dialed = dial_link(
        &machine.endpoint(&executor_token()),
        hello(machine_binding(BACKEND, 1)),
    )
    .await
    .unwrap();
    assert_eq!(dialed.welcome.node_id, MACHINE);
    assert_eq!(dialed.welcome.protocol, NODE_LINK_PROTOCOLS.max);
    assert_eq!(dialed.welcome.hosts, vec!["10.0.0.7".to_owned()]);
    let (id, link, _events) = machine.accepted_link().await;
    assert_eq!(id, machine.executor.id);
    assert_eq!(link.binding(), &machine_binding(BACKEND, 1));
    assert_eq!(
        machine.api.binding_of(&machine.executor.id),
        Some(machine_binding(BACKEND, 1)),
        "the binding is persisted per credential"
    );
    assert_eq!(
        machine.server.client_count(),
        0,
        "an executor link is not a JSON-RPC client"
    );
}

#[tokio::test]
async fn a_bind_must_name_the_credentials_backend_and_this_machine() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let endpoint = machine.endpoint(&executor_token());
    let error = dial_link(&endpoint, hello(machine_binding(OTHER_BACKEND, 1)))
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), Some(codes::OTHER_HEAD), "{error}");
    let mut wrong_node = machine_binding(BACKEND, 1);
    wrong_node.node_id = "machine-2".into();
    let error = dial_link(&endpoint, hello(wrong_node)).await.unwrap_err();
    assert_eq!(error_code(&error), Some(codes::WRONG_NODE), "{error}");
    machine.nothing_accepted().await;
    assert_eq!(machine.api.binding_of(&machine.executor.id), None);

    dial_link(&endpoint, hello(machine_binding(BACKEND, 5)))
        .await
        .unwrap();
    let _first = machine.accepted_link().await;
    let error = dial_link(&endpoint, hello(machine_binding(BACKEND, 5)))
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), Some(codes::STALE), "{error}");
}

#[tokio::test]
async fn a_credential_revoked_after_the_gate_cannot_bind() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let error = dial_link(
        &machine.endpoint(&racing_token()),
        hello(machine_binding(BACKEND, 1)),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), Some(codes::UNAUTHORIZED), "{error}");
    machine.nothing_accepted().await;
    assert_eq!(machine.api.binding_of(&machine.racing.id), None);
}

#[tokio::test]
async fn credentials_bind_independently_and_a_newer_generation_replaces_only_its_own() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let ours = machine.endpoint(&executor_token());
    let theirs = machine.endpoint(&other_executor_token());
    let first = dial_link(&ours, hello(machine_binding(BACKEND, 1)))
        .await
        .unwrap();
    let (_, node_first, _) = machine.accepted_link().await;
    let other = dial_link(&theirs, hello(machine_binding(OTHER_BACKEND, 1)))
        .await
        .unwrap();
    let (other_id, node_other, _) = machine.accepted_link().await;
    assert_eq!(other_id, machine.other.id);

    let _second = dial_link(&ours, hello(machine_binding(BACKEND, 2)))
        .await
        .unwrap();
    let _ = machine.accepted_link().await;
    tokio::time::timeout(wait(), node_first.closed())
        .await
        .unwrap();
    tokio::time::timeout(wait(), first.link.closed())
        .await
        .unwrap();
    assert!(
        !node_other.is_closed(),
        "another backend's link is untouched"
    );
    assert!(!other.link.is_closed());
}

#[tokio::test]
async fn git_is_admitted_only_for_the_credentials_current_binding() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let ours = machine.endpoint(&executor_token());
    let binding = machine_binding(BACKEND, 3);

    let before = dial_git(&ours, &binding).await;
    assert_eq!(
        before.as_ref().err().and_then(error_code),
        Some(codes::STALE),
        "no git before the link binds"
    );

    let _link = dial_link(&ours, hello(binding.clone())).await.unwrap();
    let _ = machine.accepted_link().await;
    let git = dial_git(&ours, &binding).await.unwrap();
    let node_git = match machine.accepted().await {
        (id, NodeConnection::Git(git)) => {
            assert_eq!(id, machine.executor.id);
            git
        }
        (_, other) => panic!("expected git, got {other:?}"),
    };
    assert_eq!(node_git.binding(), &binding);
    assert!(!git.is_closed());

    for (case, offered) in [
        ("older generation", machine_binding(BACKEND, 2)),
        ("other lease", {
            let mut b = binding.clone();
            b.lease_id = "lease-2".into();
            b
        }),
    ] {
        let error = dial_git(&ours, &offered).await.unwrap_err();
        assert_eq!(error_code(&error), Some(codes::STALE), "{case}");
    }
    let error = dial_git(&machine.endpoint(&other_executor_token()), &binding)
        .await
        .unwrap_err();
    assert_eq!(
        error_code(&error),
        Some(codes::STALE),
        "another credential cannot use this credential's binding"
    );
    let headers = git_headers(&binding);
    let mut partial: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
    partial.pop();
    let auth = bearer(&executor_token());
    partial.push(("authorization", auth.as_str()));
    let (status, _) = machine.upgrade(GIT_PATH, &partial).await;
    assert_eq!(status, 409, "missing binding headers");
    machine.nothing_accepted().await;

    // A newer bind retires the older binding's git connection.
    let _newer = dial_link(&ours, hello(machine_binding(BACKEND, 4)))
        .await
        .unwrap();
    let _ = machine.accepted_link().await;
    tokio::time::timeout(wait(), node_git.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn frames_outside_the_executor_list_close_the_link() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let mut socket = raw_bind(&machine, &executor_token(), machine_binding(BACKEND, 1)).await;
    let (_, link, _) = machine.accepted_link().await;
    let journal = LinkFrame::Journal(JournalRecord {
        seq: NodeCounter(1),
        agent_id: AgentId::new(),
        workspace_id: intent_core::WorkspaceId::new(),
        turn_id: None,
        recorded_at: "2026-10-30T00:00:00Z".into(),
        body: JournalBody::Exited {
            exit_code: Some(0),
            error: None,
            failure: None,
        },
    });
    socket
        .send(Message::Binary(journal.encode().unwrap().into()))
        .await
        .unwrap();
    tokio::time::timeout(wait(), link.closed()).await.unwrap();
}

#[tokio::test]
async fn revocation_closes_link_and_git_with_1008_and_refuses_the_token_after() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let binding = machine_binding(BACKEND, 1);
    let mut socket = raw_bind(&machine, &executor_token(), binding.clone()).await;
    let (_, node_link, _) = machine.accepted_link().await;
    let headers = git_headers(&binding);
    let auth = bearer(&executor_token());
    let mut with_auth: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
    with_auth.push(("authorization", auth.as_str()));
    let (status, git_socket) = machine.upgrade(GIT_PATH, &with_auth).await;
    assert_eq!(status, 101);
    let mut git_socket = git_socket.unwrap();
    let _node_git = machine.accepted().await;

    let other = dial_link(
        &machine.endpoint(&other_executor_token()),
        hello(machine_binding(OTHER_BACKEND, 1)),
    )
    .await
    .unwrap();
    let _ = machine.accepted_link().await;

    machine.api.revoke(&machine.executor.id);

    let expected = Some((CloseCode::Policy, REVOKED_CLOSE_REASON.to_owned()));
    assert_eq!(close_frame(&mut socket).await, expected, "link");
    assert_eq!(close_frame(&mut git_socket).await, expected, "git");
    assert_eq!(u16::from(CloseCode::Policy), REVOKED_CLOSE_CODE);
    assert!(node_link.is_closed());
    assert!(!other.link.is_closed(), "other backends keep their link");

    for path in NODE_PATHS {
        let (status, _) = machine
            .upgrade(path, &[("authorization", auth.as_str())])
            .await;
        assert_eq!(status, 401, "{path} after revocation");
    }
}

#[tokio::test]
async fn stopping_the_listener_closes_executor_links() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let dialed = dial_link(
        &machine.endpoint(&executor_token()),
        hello(machine_binding(BACKEND, 1)),
    )
    .await
    .unwrap();
    let (_, node_link, _) = machine.accepted_link().await;
    machine.server.stop().await;
    tokio::time::timeout(wait(), node_link.closed())
        .await
        .unwrap();
    tokio::time::timeout(wait(), dialed.link.closed())
        .await
        .unwrap();
}

/// A tailcat stand-in: pipe-client mode bridges stdio to the machine's
/// listener port, and only for the expected address.
#[cfg(unix)]
fn fake_tailcat(dir: &std::path::Path, address: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let path = dir.join("tailcat");
    std::fs::write(
        &path,
        format!(
            "#!/usr/bin/env bash\n[ \"$1\" = \"{address}\" ] || exit 7\n\
             exec 3<>/dev/tcp/127.0.0.1/$2 || exit 8\ncat <&3 &\ncat >&3\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

#[cfg(unix)]
#[tokio::test]
async fn the_dialer_reaches_the_machine_through_a_tailcat_pipe_with_the_same_pin() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let dir = tempfile::tempdir().unwrap();
    let route = TailcatRoute {
        bin: fake_tailcat(dir.path(), "tcmachine1"),
        address: "tcmachine1".into(),
    };
    let pin = CertificatePin::parse(&machine.certificate.fingerprint256).unwrap();
    let token = LeaseToken::new(executor_token()).unwrap();
    let endpoint = NodeEndpoint::via_tailcat(route.clone(), machine.port, pin, token.clone());
    let binding = machine_binding(BACKEND, 1);
    let dialed = dial_link(&endpoint, hello(binding.clone())).await.unwrap();
    assert_eq!(dialed.welcome.node_id, MACHINE);
    let _ = machine.accepted_link().await;
    let _git = dial_git(&endpoint, &binding).await.unwrap();
    assert!(matches!(machine.accepted().await.1, NodeConnection::Git(_)));

    let wrong_pin = NodeEndpoint::via_tailcat(
        route.clone(),
        machine.port,
        CertificatePin::parse(&certificate().fingerprint256).unwrap(),
        token.clone(),
    );
    let error = dial_link(&wrong_pin, hello(machine_binding(BACKEND, 2)))
        .await
        .unwrap_err();
    assert_eq!(
        error_code(&error),
        Some(codes::CERTIFICATE_MISMATCH),
        "the pin is checked end to end over tailcat"
    );

    let unknown = TailcatRoute {
        address: "tcelsewhere".into(),
        ..route.clone()
    };
    let error = dial_link(
        &NodeEndpoint::via_tailcat(unknown, machine.port, pin, token.clone()),
        hello(machine_binding(BACKEND, 3)),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), Some(codes::UNAVAILABLE), "{error}");

    let flag = TailcatRoute {
        address: "--help".into(),
        ..route
    };
    let error = dial_link(
        &NodeEndpoint::via_tailcat(flag, machine.port, pin, token),
        hello(machine_binding(BACKEND, 4)),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), Some(codes::CONFIG), "{error}");
}

#[tokio::test]
async fn a_link_upgraded_before_stop_cannot_bind_after_it() {
    let mut machine = Machine::start(Mode::Secure { ws_enabled: true }).await;
    let auth = bearer(&executor_token());
    let (status, socket) = machine
        .upgrade(LINK_PATH, &[("authorization", auth.as_str())])
        .await;
    assert_eq!(status, 101);
    let mut socket = socket.unwrap();

    machine.server.stop().await;

    let _ = socket
        .send(Message::Binary(
            LinkFrame::Hello(hello(machine_binding(BACKEND, 1)))
                .encode()
                .unwrap()
                .into(),
        ))
        .await;
    loop {
        match tokio::time::timeout(wait(), socket.next()).await.unwrap() {
            Some(Ok(Message::Binary(bytes))) => {
                panic!("no frame after stop, got {:?}", LinkFrame::decode(&bytes))
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            _ => break,
        }
    }
    machine.nothing_accepted().await;
    assert_eq!(
        machine.api.binding_of(&machine.executor.id),
        None,
        "nothing is persisted after stop"
    );

    // A restarted listener admits the backend again.
    let port = machine.server.start().await.unwrap();
    machine.port = port;
    dial_link(
        &machine.endpoint(&executor_token()),
        hello(machine_binding(BACKEND, 2)),
    )
    .await
    .unwrap();
    let _ = machine.accepted_link().await;
}
