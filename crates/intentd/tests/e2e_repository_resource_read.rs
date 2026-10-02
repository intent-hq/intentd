//! Original listeners, private Store membership and managed cache on real UDS/WSS.
//! Controlled provider only; this never launches a daemon, Git or native review.
#![cfg(unix)]
mod common;
use futures_util::{SinkExt, StreamExt};
use intent_core::{now_iso, Principal, PrincipalId, WorkspaceApi, WorkspaceId, WorkspaceRole};
use intent_services::{EventBus, Services, SettingsRegistry};
use intent_sourcecontrol::{GitlabDescriptor, GitlabHost, GitlabInstance};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
const ACQUIRE: Duration = Duration::from_secs(5);
const TOKEN: &str = "cececececececececececececececececececececececececececececececece";
const MEMBER: &str = "edededededededededededededededededededededededededededededededed";
const GUEST: &str = "abababababababababababababababababababababababababababababababab";
const INSTANCE: &str = "https://forge.test:8443/install";
const PROJECT: &str = "Team/Sub/Project";
#[derive(Default)]
struct Provider {
    routes: Mutex<Vec<(String, String)>>,
    replies: Mutex<HashMap<String, (u16, Value)>>,
    pause: Mutex<Option<String>>,
    entered: Notify,
    release: Notify,
    active: AtomicUsize,
    completed: AtomicUsize,
}
struct Server {
    host: GitlabHost,
    descriptor: GitlabDescriptor,
    state: Arc<Provider>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Server {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        eprintln!(
            "resource provider instance={INSTANCE} endpoint={endpoint} credential=owned-synthetic"
        );
        let host = GitlabHost::parse("forge.test:8443")
            .unwrap()
            .with_api_origin(&endpoint)
            .unwrap();
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse(INSTANCE).unwrap(),
            &endpoint,
        )
        .unwrap();
        let state = Arc::new(Provider::default());
        let child_state = state.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    result = listener.accept() => {
                        let (mut stream, _) = result.unwrap(); let state = child_state.clone();
                        children.spawn(async move {
                            struct Done(Arc<Provider>);
                            impl Drop for Done {fn drop(&mut self){self.0.active.fetch_sub(1,Ordering::SeqCst);self.0.completed.fetch_add(1,Ordering::SeqCst);}}
                            state.active.fetch_add(1, Ordering::SeqCst);
                            let _done = Done(state.clone());
                            let mut bytes = Vec::new(); let mut buf = [0;4096];
                            loop {
                                let n = stream.read(&mut buf).await.unwrap_or(0); if n==0{return;}
                                bytes.extend_from_slice(&buf[..n]); assert!(bytes.len()<=16384);
                                if bytes.windows(4).any(|v|v==b"\r\n\r\n"){break;}
                            }
                            let header=String::from_utf8(bytes).unwrap(); let mut words=header.split_whitespace();
                            let method=words.next().unwrap().to_owned(); let path=words.next().unwrap().to_owned();
                            assert_eq!(method,"GET"); assert!(header.contains("stored-pat"));
                            state.routes.lock().unwrap().push((method,path.clone()));
                            let route=path.split('?').next().unwrap();
                            // The response belongs to this original request, even if a newer
                            // request changes the configured reply before this hold is released.
                            let reply=state.replies.lock().unwrap().get(route).cloned().unwrap_or_else(||default_reply(route));
                            let pause={let mut p=state.pause.lock().unwrap();if p.as_deref()==Some(route){p.take();true}else{false}};
                            if pause {state.entered.notify_one();state.release.notified().await;}
                            let body=reply.1.to_string();
                            let head=format!("HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRateLimit-Remaining: 17\r\nRateLimit-Limit: 100\r\nRateLimit-Reset: 1900000000\r\nConnection: close\r\n\r\n",reply.0,body.len());
                            let _=stream.write_all(format!("{head}{body}").as_bytes()).await;
                        });
                    },
                    result=children.join_next(),if !children.is_empty()=>{result.unwrap().unwrap();}
                }
            }
            // Every retained provider response has been released by the caller.
            while let Some(result) = children.join_next().await {
                result.unwrap();
            }
        });
        Self {
            host,
            descriptor,
            state,
            stop: Some(stop),
            task: Some(task),
        }
    }
    fn count(&self) -> usize {
        self.state.routes.lock().unwrap().len()
    }
    fn set(&self, path: &str, status: u16) {
        self.state.replies.lock().unwrap().insert(
            path.into(),
            (status, json!({"message":"private failure text"})),
        );
    }
    async fn finish(mut self) {
        self.state.release.notify_waiters();
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(ACQUIRE, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        eprintln!(
            "resource provider joined: calls={} completed={} active={}",
            self.count(),
            self.state.completed.load(Ordering::SeqCst),
            self.state.active.load(Ordering::SeqCst)
        );
        assert_eq!(self.state.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.state.completed.load(Ordering::SeqCst), self.count());
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.state.release.notify_waiters();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
fn route(kind: &str) -> String {
    format!("/api/v4/projects/Team%2FSub%2FProject/{kind}/7")
}
fn resource(kind: &str) -> Value {
    json!({"iid":7,"project_id":42,"source_project_id":42,"target_project_id":42,"source_branch":"topic","target_branch":"main","state":"opened","draft":false,"title":format!("{kind} seven"),"description":"original body","web_url":format!("{INSTANCE}/{PROJECT}/-/{kind}/7"),"sha":"0123456789012345678901234567890123456789","author":{"username":"fixture"},"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"})
}
fn default_reply(path: &str) -> (u16, Value) {
    if path == "/api/v4/user" {
        (200, json!({"id":42,"username":"fixture","name":"Fixture"}))
    } else if path == route("merge_requests") {
        (200, resource("merge_requests"))
    } else if path == route("issues") {
        (200, resource("issues"))
    } else if path.ends_with("/approvals") {
        (
            200,
            json!({"approved_by":[],"approvals_left":0,"approvals_required":0}),
        )
    } else if path.ends_with("/discussions") {
        (200, json!([]))
    } else if path == "/api/v4/projects/Team%2FSub%2FProject" {
        (
            200,
            json!({"id":42,"path_with_namespace":PROJECT,"web_url":format!("{INSTANCE}/{PROJECT}"),"only_allow_merge_if_pipeline_succeeds":false,"only_allow_merge_if_all_discussions_are_resolved":false}),
        )
    } else {
        (404, json!({"message":"not found"}))
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

struct OwnedToken;
impl TokenStore for OwnedToken {
    fn load_token(&self) -> Option<String> {
        Some(TOKEN.into())
    }
    fn store_token(&self, _: &str) -> intent_core::Result<()> {
        Err(intent_core::Error::Forbidden(
            "immutable test credential".into(),
        ))
    }
}
struct Harness {
    dir: tempfile::TempDir,
    server: Option<Server>,
    store: Store,
    workspace: WorkspaceId,
    member: PrincipalId,
    ws: Arc<WsApiServer>,
    port: u16,
    cfg: Arc<ClientConfig>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    listener: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}
impl Harness {
    async fn new() -> Self {
        let dir = common::test_tempdir("itd-resource-wire-");
        let server = Server::new().await;
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        let mut workspace = intent_core::chief_workspace();
        workspace.id = WorkspaceId::new();
        workspace.path = None;
        workspace.repository_path = None;
        store.insert_workspace(&workspace).await.unwrap();
        let owner = store.get_primary_principal().await.unwrap();
        let mut member_id = None;
        for (token, is_member) in [(MEMBER, true), (GUEST, false)] {
            let person = Principal {
                id: PrincipalId::new(),
                identity: None,
                github_user_id: Some(if is_member { 51 } else { 52 }),
                login: Some(if is_member { "member" } else { "guest" }.into()),
                display_name: None,
                avatar_url: None,
                is_primary: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            };
            store.upsert_principal(&person).await.unwrap();
            let hash = Sha256::digest(token.as_bytes()).iter().fold(
                String::with_capacity(64),
                |mut text, byte| {
                    write!(text, "{byte:02x}").unwrap();
                    text
                },
            );
            if is_member {
                let invite = intent_core::HostInvite::new(
                    "resource-wire-member".into(),
                    owner.id.clone(),
                    person.identity_key().unwrap(),
                    "member".into(),
                    "owned-proof".into(),
                    None,
                )
                .unwrap();
                store.insert_host_invite(&invite).await.unwrap();
                let generation = store
                    .host_membership_state()
                    .await
                    .unwrap()
                    .authorization_generation;
                store
                    .join_host_by_invite(
                        &invite.id,
                        &person,
                        intent_store::HostJoinCredential::Proof {
                            token_hash: &hash,
                            authorization_generation: generation,
                        },
                    )
                    .await
                    .unwrap();
                member_id = Some(person.id.clone());
            } else {
                store
                    .insert_principal_credential(&person.id, &hash)
                    .await
                    .unwrap();
            }
            store
                .add_workspace_member(&workspace.id, &person.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                (
                    "sourceControl.gitlab.host".into(),
                    json!(server.host.host()),
                ),
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!(INSTANCE),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(server.host.base_url()),
                ),
                ("sourceControl.gitlab.oauthClientId".into(), json!("client")),
            ])
            .unwrap();
        let secrets = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
        secrets
            .store("sourceControl.gitlab.token", "stored-pat")
            .unwrap();
        let bus = EventBus::new(store.clone());
        let services = Arc::new(
            Services::new_repository_fixture(
                store.clone(),
                secrets,
                Some(server.descriptor.clone()),
            )
            .with_settings_registry(registry)
            .with_event_bus(bus.clone()),
        );
        services
            .initialize_repository_test_fixture(server.descriptor.clone())
            .await
            .unwrap();
        services.initialize_repository_wire().await.unwrap();
        let api: Arc<dyn WorkspaceApi> = services;
        let tls = ensure_tls_certificate(dir.path()).unwrap();
        let cfg = client_config(&tls.fingerprint256);
        let token = Arc::new(AsyncTokenStore::new(Arc::new(OwnedToken)));
        let ws = Arc::new(
            WsApiServer::new(
                api.clone(),
                bus.clone(),
                &tls,
                &token,
                WsOptions {
                    base_port: 0,
                    bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
                    ..Default::default()
                },
                None,
            )
            .unwrap(),
        );
        let port = ws.start().await.unwrap();
        let socket = dir.path().join("intentd.sock");
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let listener = tokio::spawn(async move {
            intent_transport::serve_uds(api, bus, &socket, None, async move {
                let _ = stopped.await;
            })
            .await
        });
        tokio::time::timeout(ACQUIRE, async {
            loop {
                if dir.path().join("intentd.sock").exists() {
                    break;
                }
                // timing-guard: only the original local listener bind readiness.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Self {
            dir,
            server: Some(server),
            store,
            workspace: workspace.id,
            member: member_id.unwrap(),
            ws,
            port,
            cfg,
            shutdown: Some(shutdown),
            listener: Some(listener),
        }
    }
    async fn uds(&self) -> Client {
        Client {
            socket: Socket::Uds(BufReader::new(
                UnixStream::connect(self.dir.path().join("intentd.sock"))
                    .await
                    .unwrap(),
            )),
            id: 0,
            notices: Vec::new(),
        }
    }
    async fn wss(&self, token: &str) -> Client {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .unwrap();
        let tls = tokio_rustls::TlsConnector::from(self.cfg.clone())
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let mut request = format!("wss://localhost:{}/ws", self.port)
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
            .headers_mut()
            .insert("origin", "http://localhost".parse().unwrap());
        let (ws, _) = tokio_tungstenite::client_async(request, tls).await.unwrap();
        Client {
            socket: Socket::Ws(Box::new(ws)),
            id: 0,
            notices: Vec::new(),
        }
    }
    fn detail(&self, c: &Value, kind: &str) -> Value {
        json!({"workspaceId":self.workspace,"readLifetimeId":c["readLifetimeId"],"target":{"repository":{"provider":"gitlab","instanceBaseUrl":INSTANCE,"projectPath":PROJECT},"kind":kind,"number":7}})
    }
    async fn finish(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        tokio::time::timeout(ACQUIRE, self.listener.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.ws.stop().await;
        assert_eq!(self.ws.bound_port().await, None);
        assert_eq!(self.ws.client_count(), 0);
        self.server.take().unwrap().finish().await;
        assert!(UnixStream::connect(self.dir.path().join("intentd.sock"))
            .await
            .is_err());
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.listener.take() {
            task.abort();
        }
    }
}
enum Socket {
    Uds(BufReader<UnixStream>),
    Ws(
        Box<
            tokio_tungstenite::WebSocketStream<
                tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
            >,
        >,
    ),
}
struct Client {
    socket: Socket,
    id: u64,
    notices: Vec<Value>,
}
impl Client {
    async fn next(&mut self) -> Value {
        tokio::time::timeout(ACQUIRE, async {
            loop {
                match &mut self.socket {
                    Socket::Uds(s) => {
                        let mut line = String::new();
                        assert!(s.read_line(&mut line).await.unwrap() > 0);
                        return serde_json::from_str(&line).unwrap();
                    }
                    Socket::Ws(s) => match s.next().await {
                        Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
                        Some(Ok(Message::Ping(p))) => s.send(Message::Pong(p)).await.unwrap(),
                        Some(Ok(_)) => {}
                        v => panic!("original WSS closed {v:?}"),
                    },
                }
            }
        })
        .await
        .unwrap()
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let request = json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params});
        eprintln!("resource original request {request}");
        let line = request.to_string();
        match &mut self.socket {
            Socket::Uds(s) => s
                .get_mut()
                .write_all(format!("{line}\n").as_bytes())
                .await
                .unwrap(),
            Socket::Ws(s) => s.send(Message::Text(line.into())).await.unwrap(),
        }
        loop {
            let result = self.next().await;
            eprintln!("resource original response {result}");
            if result["id"] == self.id {
                assert_eq!(result["jsonrpc"], "2.0");
                return result;
            }
            self.notices.push(result);
        }
    }
    async fn close(mut self) {
        match &mut self.socket {
            Socket::Uds(s) => s.get_mut().shutdown().await.unwrap(),
            Socket::Ws(s) => s.as_mut().close(None).await.unwrap(),
        }
    }
}
fn success(value: &Value) -> &Value {
    assert!(value.get("error").is_none(), "{value}");
    &value["result"]
}

#[intent_test_macros::daemon_test]
async fn repository_resource_real_owner_uds_and_member_tls_wss_contract() {
    let h = Harness::new().await;
    for remote in [false, true] {
        let mut c = if remote {
            h.wss(MEMBER).await
        } else {
            h.uds().await
        };
        let hello = c.rpc("client.hello", json!({})).await;
        assert_eq!(success(&hello)["protocolVersion"], "10.14");
        assert_eq!(
            hello["result"]["server"]["capabilities"]["repositoryResourceRead"],
            1
        );
        let capture = c
            .rpc(
                "sourceControl.read.capture",
                json!({"workspaceId":h.workspace}),
            )
            .await;
        let capture = success(&capture).clone();
        assert_eq!(
            capture["instances"],
            json!([{"provider":"gitlab","instanceBaseUrl":INSTANCE,"availability":"connected"}])
        );
        for kind in ["merge-request", "issue"] {
            let query = h.detail(&capture, kind);
            let result = c.rpc("sourceControl.read.detail", query.clone()).await;
            assert_eq!(success(&result)["outcome"]["kind"], kind);
            let count = h.server.as_ref().unwrap().count();
            let warm = c.rpc("sourceControl.read.detail", query).await;
            assert_eq!(success(&result), success(&warm));
            assert_eq!(h.server.as_ref().unwrap().count(), count);
        }
        let bad=c.rpc("sourceControl.read.detail",json!({"workspaceId":h.workspace,"readLifetimeId":capture["readLifetimeId"],"target":null})).await;
        assert_eq!(bad["error"]["code"], -32602);
        let q = json!({"workspaceId":h.workspace,"readLifetimeId":capture["readLifetimeId"]});
        assert_eq!(
            success(&c.rpc("sourceControl.read.release", q.clone()).await)["released"],
            true
        );
        assert_eq!(
            success(&c.rpc("sourceControl.read.release", q).await)["released"],
            true
        );
        let stale = c
            .rpc("sourceControl.read.detail", h.detail(&capture, "issue"))
            .await;
        assert_eq!(stale["error"]["code"], -32003);
        assert!(c
            .notices
            .iter()
            .any(|v| v["method"] == "sourceControl.read.retired"
                && v["params"]["readLifetimeIds"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&capture["readLifetimeId"]))));
        c.close().await;
    }
    let mut guest = h.wss(GUEST).await;
    let count = h.server.as_ref().unwrap().count();
    let denied = guest
        .rpc(
            "sourceControl.read.capture",
            json!({"workspaceId":h.workspace}),
        )
        .await;
    assert_eq!(denied["error"]["code"], -32003);
    assert_eq!(count, h.server.as_ref().unwrap().count());
    guest.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_resource_real_socket_identity_and_durable_retirement() {
    let h = Harness::new().await;
    let mut a = h.wss(MEMBER).await;
    let mut b = h.wss(MEMBER).await;
    let capture = a
        .rpc(
            "sourceControl.read.capture",
            json!({"workspaceId":h.workspace}),
        )
        .await;
    let capture = success(&capture).clone();
    let count = h.server.as_ref().unwrap().count();
    let foreign = b
        .rpc("sourceControl.read.detail", h.detail(&capture, "issue"))
        .await;
    assert_eq!(foreign["error"]["code"], -32003);
    assert_eq!(count, h.server.as_ref().unwrap().count());
    h.store.remove_host_member(&h.member).await.unwrap();
    let retired = a
        .rpc("sourceControl.read.detail", h.detail(&capture, "issue"))
        .await;
    assert!(retired.get("error").is_some());
    assert_eq!(count, h.server.as_ref().unwrap().count());
    a.close().await;
    b.close().await;
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn repository_resource_real_failure_classes_keep_original_wire_evidence() {
    for (route_suffix, status, expected) in [
        ("issues", 401, "authentication"),
        ("issues", 403, "resource-denied"),
        ("issues", 404, "resource-denied"),
        ("issues", 500, "transient"),
        ("issues", 429, "rate-limited"),
        ("merge_requests/approvals", 403, "restricted"),
    ] {
        let h = Harness::new().await;
        let mut client = h.wss(MEMBER).await;
        let captured = client
            .rpc(
                "sourceControl.read.capture",
                json!({"workspaceId":h.workspace}),
            )
            .await;
        let captured = success(&captured).clone();
        let optional = route_suffix.ends_with("/approvals");
        let path = if optional {
            format!("{}/approvals", route("merge_requests"))
        } else {
            route(route_suffix)
        };
        h.server.as_ref().unwrap().set(&path, status);
        let result = client
            .rpc(
                "sourceControl.read.detail",
                h.detail(&captured, if optional { "merge-request" } else { "issue" }),
            )
            .await;
        let value = success(&result);
        assert!(!value.to_string().contains("private failure text"));
        if optional {
            assert_eq!(value["outcome"]["kind"], "merge-request");
            assert_eq!(
                value["outcome"]["snapshot"]["availability"]["approvals"],
                expected
            );
        } else {
            assert_eq!(value["outcome"]["kind"], "failure");
            assert_eq!(value["outcome"]["code"], expected);
        }
        assert_eq!(
            value["quota"]["remaining"],
            if status == 429 { "0" } else { "17" }
        );
        client.close().await;
        h.finish().await;
    }
}
