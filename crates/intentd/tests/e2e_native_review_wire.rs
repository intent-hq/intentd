//! Explicit cold fixture composition on real UDS/TLS WSS, owned Git/HTTPS and loopback GitLab.
//! This is not a normal spawned-daemon or production provider-TLS positive.
//! No hosted provider, user account, adapter, model or installed daemon.
#![cfg(unix)]
mod common;
use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_core::{
    now_iso, Principal, PrincipalId, WorkspaceGitRootId, WorkspaceId, WorkspaceRole,
};
use intent_services::{events::EventBus, Services, SettingsRegistry};
use intent_sourcecontrol::{GitlabDescriptor, GitlabInstance};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, serve_uds, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
const TOKEN: &str = "cececececececececececececececececececececececececececececececece";
const MEMBER: &str = "edededededededededededededededededededededededededededededededed";
const GUEST: &str = "abababababababababababababababababababababababababababababababab";
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

struct FixtureToken;
impl TokenStore for FixtureToken {
    fn load_token(&self) -> Option<String> {
        Some(TOKEN.into())
    }
    fn store_token(&self, _: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

struct Harness {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    listener: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    ws: Arc<WsApiServer>,
    store: Store,
    member: PrincipalId,
    fixture: GuardedChild,
    instance: String,
    fixture_state: PathBuf,
    dir: tempfile::TempDir,
    root: PathBuf,
    workspace: WorkspaceId,
    registered: WorkspaceGitRootId,
    guest: PrincipalId,
    port: u16,
    cfg: Arc<ClientConfig>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.fixture.kill();
        let _ = self.fixture.wait();
    }
}
impl Harness {
    async fn boot() -> Self {
        Self::boot_with_ids(None).await
    }
    async fn boot_with_ids(ids: Option<(WorkspaceId, WorkspaceGitRootId)>) -> Self {
        Self::boot_composed(ids, true).await
    }
    async fn boot_composed(ids: Option<(WorkspaceId, WorkspaceGitRootId)>, approved: bool) -> Self {
        Self::boot_driver(ids, approved, None).await
    }
    async fn boot_driver(
        ids: Option<(WorkspaceId, WorkspaceGitRootId)>,
        approved: bool,
        driver: Option<&DriverDescriptor>,
    ) -> Self {
        let dir = if let Some(driver) = driver {
            common::test_tempdir_in(driver.directory.to_str().unwrap(), "host-")
        } else {
            common::test_tempdir_in("/tmp", "itd-review-wire-")
        };
        if let Some(driver) = driver {
            private_json(&dir.path().join("driver.json"), &json!({"version":1,"matchingReview":driver.scenarios.iter().any(|s| s == "held-stop")})).unwrap();
        }
        let root = dir.path().join("repo");
        std::fs::create_dir(&root).unwrap();
        let registered_path = dir.path().join("secondary");
        std::fs::create_dir(&registered_path).unwrap();
        init_repo(&root);
        init_repo(&registered_path);
        let (fixture, instance, endpoint, fixture_state) = remote_fixture(dir.path(), &root).await;
        git(
            &root,
            &[
                "remote",
                "add",
                "forge",
                &format!("{instance}/group/project.git"),
            ],
        );
        git(
            &registered_path,
            &[
                "remote",
                "add",
                "forge",
                &format!("{instance}/group/project.git"),
            ],
        );
        if driver.is_some_and(|d| d.scenarios.iter().any(|s| s == "frontend")) {
            for path in [&root, &registered_path] {
                std::fs::write(path.join("staged.txt"), "owned staged change\n").unwrap();
                std::fs::write(path.join("unstaged.txt"), "owned unstaged change\n").unwrap();
                git(path, &["add", "staged.txt"]);
            }
        }
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        let mut ws = intent_core::chief_workspace();
        ws.id = ids
            .as_ref()
            .map_or_else(WorkspaceId::new, |ids| ids.0.clone());
        ws.branch = "main".into();
        ws.base_ref = Some("trunk".into());
        ws.path = Some(dir.path().join("workspace").to_string_lossy().into_owned());
        ws.repository_path = Some(root.to_string_lossy().into_owned());
        store.insert_workspace(&ws).await.unwrap();
        let registered = ids.map_or_else(WorkspaceGitRootId::new, |ids| ids.1);
        let row=serde_json::from_value(json!({"id":registered,"workspaceId":ws.id,"path":registered_path,"source":"auto","registeredByAgentIds":[],"createdAt":now_iso(),"updatedAt":now_iso()})).unwrap();
        store.upsert_workspace_git_root(&row).await.unwrap();
        let guest = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: Some(4242),
            login: Some("guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&guest).await.unwrap();
        let hash = Sha256::digest(GUEST.as_bytes()).iter().fold(
            String::with_capacity(64),
            |mut hash, byte| {
                use std::fmt::Write as _;
                write!(hash, "{byte:02x}").unwrap();
                hash
            },
        );
        store
            .insert_principal_credential(&guest.id, &hash)
            .await
            .unwrap();
        store
            .add_workspace_member(&ws.id, &guest.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        let mut member = guest.clone();
        member.id = PrincipalId::new();
        member.github_user_id = Some(4243);
        member.login = Some("member".into());
        store.upsert_principal(&member).await.unwrap();
        let owner = store.get_primary_principal().await.unwrap();
        let invite = intent_core::HostInvite::new(
            "review-wire-member".into(),
            owner.id,
            member.identity_key().unwrap(),
            "member".into(),
            "owned-invite-proof".into(),
            None,
        )
        .unwrap();
        store.insert_host_invite(&invite).await.unwrap();
        let generation = store
            .host_membership_state()
            .await
            .unwrap()
            .authorization_generation;
        let hash = Sha256::digest(MEMBER.as_bytes()).iter().fold(
            String::with_capacity(64),
            |mut value, byte| {
                write!(value, "{byte:02x}").unwrap();
                value
            },
        );
        store
            .join_host_by_invite(
                &invite.id,
                &member,
                intent_store::HostJoinCredential::Proof {
                    token_hash: &hash,
                    authorization_generation: generation,
                },
            )
            .await
            .unwrap();
        store
            .add_workspace_member(&ws.id, &member.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        std::fs::write(dir.path().join("config.toml"),"[providers]\nenabled = {}\n[mcp]\nenableUserServers = false\n[agents]\nresumeInterruptedOnStart = \"off\"\n").unwrap();
        let mut config = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("config.toml"))
            .unwrap();
        writeln!(config, "[sourceControl.gitlab]\nhost = {}\ninstanceBaseUrl = {}\napiBaseUrl = {}\noauthClientId = \"fixture-client\"", json!(instance.strip_prefix("https://").unwrap()), json!(instance), json!(endpoint)).unwrap();
        let secrets = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
        secrets
            .store("sourceControl.gitlab.token", "stored-pat")
            .unwrap();
        for path in [&root, &registered_path] {
            git(
                path,
                &[
                    "config",
                    "http.sslCAInfo",
                    dir.path().join("ca.pem").to_str().unwrap(),
                ],
            );
        }
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse(&instance).unwrap(),
            &endpoint,
        )
        .unwrap();
        let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        let bus = EventBus::new(store.clone());
        let services = if approved {
            Services::new_repository_fixture(store.clone(), secrets, Some(descriptor.clone()))
        } else {
            // The feature is compiled, but ordinary composition still has no fixture grant.
            Services::new(store.clone())
        };
        let services = Arc::new(
            services
                .with_settings_registry(registry)
                .with_event_bus(bus.clone())
                .with_workspaces_root(dir.path().join("workspaces")),
        );
        if approved {
            services
                .initialize_repository_test_fixture(descriptor.clone())
                .await
                .unwrap();
            assert!(services
                .initialize_repository_test_fixture(descriptor)
                .await
                .is_err());
        } else {
            assert!(services
                .initialize_gitlab_repository_binding()
                .await
                .is_err());
        }
        services.initialize_repository_wire().await.unwrap();
        let api: Arc<dyn WorkspaceApi> = services.clone();
        let tls = ensure_tls_certificate(dir.path()).unwrap();
        let cfg = client_config(&tls.fingerprint256);
        let token_store = Arc::new(AsyncTokenStore::new(Arc::new(FixtureToken)));
        let options = WsOptions {
            base_port: 0,
            bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        };
        let ws_server = Arc::new(
            WsApiServer::new(api.clone(), bus.clone(), &tls, &token_store, options, None).unwrap(),
        );
        let port = ws_server.start().await.unwrap();
        let (shutdown, receive) = tokio::sync::oneshot::channel();
        let socket = dir.path().join("intentd.sock");
        let socket_task = socket.clone();
        let ws_task = ws_server.clone();
        let listener = tokio::spawn(async move {
            let result = serve_uds(api, bus, &socket_task, None, async move {
                let _ = receive.await;
            })
            .await;
            ws_task.stop().await;
            result
        });
        timeout(common::daemon_startup_timeout(), async {
            loop {
                if UnixStream::connect(&socket).await.is_ok() {
                    break;
                }
                // timing-guard: real local listener readiness, not behavior synchronization
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Self {
            shutdown: Some(shutdown),
            listener: Some(listener),
            ws: ws_server,
            store,
            member: member.id,
            fixture,
            instance,
            fixture_state,
            dir,
            root,
            workspace: ws.id,
            registered,
            guest: guest.id,
            port,
            cfg,
        }
    }
    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).unwrap();
        }
        timeout(Duration::from_secs(5), self.listener.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(self.ws.bound_port().await, None);
        assert!(UnixStream::connect(self.dir.path().join("intentd.sock"))
            .await
            .is_err());
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .is_err());
        self.fixture.kill().unwrap();
        self.fixture.wait().unwrap();
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
        let url = format!("wss://localhost:{}/ws?token={token}", self.port);
        Client {
            socket: Socket::Ws(Box::new(
                common::wss_connect_with_retry(self.port, self.cfg.clone(), &url).await,
            )),
            id: 0,
            notices: Vec::new(),
        }
    }
    fn prepare(&self, action: &str) -> Value {
        json!({"workspaceId":self.workspace,"action":action,"review":{"root":{"workspaceId":self.workspace,"kind":"primary"},"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":self.instance,"projectPath":"group/project"}},"targetBranch":"trunk","pushRemote":"forge"}})
    }
    fn counts(&self) -> Value {
        serde_json::from_slice(&std::fs::read(&self.fixture_state).unwrap()).unwrap()
    }
}
enum Socket {
    Uds(BufReader<UnixStream>),
    Ws(Box<common::TlsWs>),
}
struct Client {
    socket: Socket,
    id: u64,
    notices: Vec<Value>,
}
impl Client {
    async fn next(&mut self) -> Value {
        timeout(common::rpc_read_timeout(), async {
            loop {
                match &mut self.socket {
                    Socket::Uds(stream) => {
                        let mut line = String::new();
                        assert!(stream.read_line(&mut line).await.unwrap() > 0);
                        return serde_json::from_str(&line).unwrap();
                    }
                    Socket::Ws(ws) => match ws.next().await {
                        Some(Ok(Message::Text(text))) => {
                            return serde_json::from_str(&text).unwrap()
                        }
                        Some(Ok(Message::Ping(p))) => {
                            ws.send(Message::Pong(p)).await.unwrap();
                        }
                        Some(Ok(_)) => {}
                        x => panic!("socket closed {x:?}"),
                    },
                }
            }
        })
        .await
        .unwrap()
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        let text =
            json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params}).to_string();
        match &mut self.socket {
            Socket::Uds(stream) => {
                stream
                    .get_mut()
                    .write_all(format!("{text}\n").as_bytes())
                    .await
                    .unwrap();
            }
            Socket::Ws(ws) => ws.send(Message::Text(text.into())).await.unwrap(),
        }
        loop {
            let frame = self.next().await;
            if frame["id"] == self.id {
                return frame;
            }
            self.notices.push(frame);
        }
    }
}
fn success(value: &Value) -> &Value {
    assert!(value.get("error").is_none(), "{value}");
    &value["result"]
}
fn git(path: &std::path::Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "owned Git fixture failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
fn init_repo(path: &std::path::Path) {
    git(path, &["init", "-b", "main"]);
    git(path, &["config", "user.name", "Fixture"]);
    git(path, &["config", "user.email", "fixture@example.invalid"]);
    std::fs::write(path.join("seed.txt"), "seed").unwrap();
    git(path, &["add", "seed.txt"]);
    git(path, &["commit", "-m", "initial"]);
    git(path, &["branch", "trunk"]);
}
// Each socket test owns a child test process so the fixture CA is installed
// before libgit2/OpenSSL initialization. No process-global trust is mutated and
// no production verifier, daemon argument or settings fallback is introduced.
async fn run_in_tls_process(name: &str) -> bool {
    if std::env::var("INTENT_REVIEW_TLS_TEST").ok().as_deref() == Some(name) {
        return false;
    }
    let directory = common::test_tempdir_in("/tmp", "itd-review-tls-");
    make_fixture_certificate(directory.path());
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture"])
        .env("INTENT_REVIEW_TLS_TEST", name)
        .env("INTENT_REVIEW_TLS_FIXTURE", directory.path())
        .env("SSL_CERT_FILE", directory.path().join("ca.pem"))
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    let result = tokio::task::spawn_blocking(move || {
        child.wait_with_timeout(Duration::from_secs(60)).unwrap()
    })
    .await
    .unwrap()
    .expect("owned TLS test process completed");
    assert!(result.success(), "owned TLS test process: {result}");
    drop(directory);
    true
}
fn make_fixture_certificate(directory: &std::path::Path) {
    let cert = directory.join("ca.pem");
    let key = directory.join("key.pem");
    let out = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .output()
        .unwrap();
    assert!(out.status.success(), "owned fixture CA generation");
}

async fn remote_fixture(
    dir: &std::path::Path,
    root: &std::path::Path,
) -> (GuardedChild, String, String, PathBuf) {
    let remote = dir.join("remote/group");
    std::fs::create_dir_all(&remote).unwrap();
    git(
        dir,
        &[
            "clone",
            "--bare",
            root.to_str().unwrap(),
            remote.join("project.git").to_str().unwrap(),
        ],
    );
    git(
        &remote.join("project.git"),
        &["config", "http.receivepack", "true"],
    );
    let trust = PathBuf::from(std::env::var_os("INTENT_REVIEW_TLS_FIXTURE").unwrap());
    std::fs::copy(trust.join("ca.pem"), dir.join("ca.pem")).unwrap();
    std::fs::copy(trust.join("key.pem"), dir.join("key.pem")).unwrap();
    let script = dir.join("fixture.py");
    let source = if dir.join("driver.json").exists() {
        format!("{DRIVER_PROVIDER_SUPPORT}\n{REMOTE_FIXTURE}")
    } else {
        REMOTE_FIXTURE.to_owned()
    };
    std::fs::write(&script, source).unwrap();
    let mut command = std::process::Command::new("python3");
    command
        .arg(&script)
        .arg(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(dir.join("fixture.log")).unwrap(),
        ));
    let child = GuardedChild::spawn(&mut command).unwrap();
    let endpoint = dir.join("endpoints.json");
    timeout(Duration::from_secs(5), async {
        while !endpoint.exists() {
            // timing-guard: owned fixture readiness file
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let data: Value = serde_json::from_slice(&std::fs::read(endpoint).unwrap()).unwrap();
    (
        child,
        data["instance"].as_str().unwrap().into(),
        data["endpoint"].as_str().unwrap().into(),
        dir.join("effects.json"),
    )
}
const REMOTE_FIXTURE: &str = r#"
import base64, http.server, json, os, pathlib, ssl, subprocess, sys, threading, urllib.parse
root=pathlib.Path(sys.argv[1]);driver=DriverProvider(root) if (root/'driver.json').exists() else None;lock=threading.Lock();state={'posts':0,'pushes':0,'reviews':[],'gitRequests':0,'gitAuthenticated':0};instance=''
def save():
    temp=root/'effects.tmp';temp.write_text(json.dumps(state));temp.replace(root/'effects.json')
def sha(branch):
    p=subprocess.run(['git','-C',str(root/'remote/group/project.git'),'rev-parse','--verify','refs/heads/'+branch],capture_output=True,text=True)
    return p.stdout.strip() if p.returncode==0 else None
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def body(self):
        if self.headers.get('Transfer-Encoding','').lower()=='chunked':
            out=b''
            while True:
                n=int(self.rfile.readline().strip(),16)
                if n==0:self.rfile.readline();break
                out+=self.rfile.read(n);self.rfile.read(2)
            return out
        return self.rfile.read(int(self.headers.get('Content-Length','0')))
    def answer(self,status,body):
        if driver and driver.answer(self,status,body): return
        data=json.dumps(body).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_GET(self): self.dispatch()
    def do_POST(self): self.dispatch()
    def dispatch(self):
        request=driver.enter(self) if driver else None
        self.driver_request=request
        try:
            if not driver or not driver.intercept(self,request): self.dispatch_original()
        finally:
            if driver: driver.finish(request)
    def dispatch_original(self):
        url=urllib.parse.urlsplit(self.path);path=urllib.parse.unquote(url.path)
        if path.startswith('/api/v4/'):
            assert self.headers.get('PRIVATE-TOKEN')=='stored-pat' or self.headers.get('Authorization')=='Bearer stored-pat'
            if path=='/api/v4/user': return self.answer(200,{'id':42,'username':'fixture','name':'Fixture','web_url':instance+'/fixture'})
            if path.endswith('/repository/branches'):
                return self.answer(200,[{'name':n,'commit':{'id':s}} for n in ['main','trunk'] if (s:=sha(n))])
            if '/repository/branches/' in path:
                n=path.rsplit('/',1)[1];s=sha(n)
                return self.answer(200 if s else 404,{'name':n,'commit':{'id':s}})
            if path.endswith('/merge_requests'):
                if driver: return driver.merge_request(self)
                with lock:
                    if self.command=='POST':
                        data=json.loads(self.body());assert data['source_branch']=='main' and data['target_branch']=='trunk';assert not data.get('draft',False)
                        state['posts']+=1
                        state['reviews'].append({'iid':7,'project_id':42,'source_project_id':42,'target_project_id':42,'source_branch':'main','target_branch':'trunk','state':'opened','draft':False,'title':'Observed ready MR','description':'observed','web_url':instance+'/group/project/-/merge_requests/7','sha':sha('main'),'author':{'username':'fixture'},'created_at':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z'})
                        save();return self.answer(201,state['reviews'][-1])
                    return self.answer(200,state['reviews'])
            if path in ['/api/v4/projects/group/project','/api/v4/projects/42']:
                return self.answer(200,{'id':42,'path_with_namespace':'group/project','name':'project','default_branch':'trunk','web_url':instance+'/group/project'})
            return self.answer(404,{'message':'missing'})
        with lock:state['gitRequests']+=1;save()
        expected='Basic '+base64.b64encode(b'oauth2:stored-pat').decode()
        if self.headers.get('Authorization')!=expected:
            self.send_response(401);self.send_header('WWW-Authenticate','Basic realm="owned fixture"');self.send_header('Content-Length','0');self.end_headers();return
        with lock:state['gitAuthenticated']+=1;save()
        data=self.body()
        env=dict(os.environ,GIT_PROJECT_ROOT=str(root/'remote'),GIT_HTTP_EXPORT_ALL='1',REQUEST_METHOD=self.command,PATH_INFO=url.path,QUERY_STRING=url.query,CONTENT_TYPE=self.headers.get('Content-Type',''),CONTENT_LENGTH=str(len(data)),REMOTE_USER='fixture',GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_SYSTEM='/dev/null')
        out=subprocess.run(['git','http-backend'],input=data,capture_output=True,env=env)
        if out.returncode: return self.answer(500,{'message':'fixture backend failed'})
        headers,body=out.stdout.split(b'\r\n\r\n',1);parsed=[line.decode().split(':',1) for line in headers.split(b'\r\n') if b':' in line]
        status=next((int(v.strip().split()[0]) for k,v in parsed if k.lower()=='status'),200)
        if self.command=='POST' and url.path.endswith('/git-receive-pack'):
            with lock:state['pushes']+=1;save()
            if driver: driver.effect(self.driver_request,'git-receive-pack-finished')
        self.send_response(status)
        for k,v in parsed:
            if k.lower()!='status':self.send_header(k,v.strip())
        self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
api=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
git=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(root/'ca.pem',root/'key.pem');git.socket=context.wrap_socket(git.socket,server_side=True)
instance='https://localhost:'+str(git.server_port)
if driver: driver.seed()
with lock:save()
temp=root/'endpoints.tmp';temp.write_text(json.dumps({'instance':instance,'endpoint':'http://127.0.0.1:'+str(api.server_port)}));temp.replace(root/'endpoints.json')
threading.Thread(target=git.serve_forever,daemon=True).start();api.serve_forever()
"#;
fn execute_query(h: &Harness, p: &Value, action: &str) -> Value {
    json!({"workspaceId":h.workspace,"action":action,"review":{"operationId":p["reviewOperation"]["operationId"],"root":p["reviewOperation"]["root"]},"commitMessage":"native owned commit","prTitle":"native owned ready MR","prBody":"body"})
}
fn reconcile_query(h: &Harness, p: &Value) -> Value {
    json!({"workspaceId":h.workspace,"root":p["reviewOperation"]["root"],"operationId":p["reviewOperation"]["operationId"]})
}
#[tokio::test]
async fn native_review_real_cold_uds_wss_commit_https_push_ready_create_and_receipts() {
    if run_in_tls_process(
        "native_review_real_cold_uds_wss_commit_https_push_ready_create_and_receipts",
    )
    .await
    {
        return;
    }
    let h = Harness::boot().await;
    let mut uds = h.uds().await;
    let mut ws = h.wss(TOKEN).await;
    let mut guest = h.wss(GUEST).await;
    for client in [&mut uds, &mut ws] {
        let hello = client
            .rpc("client.hello", json!({"clientId":"owned-review"}))
            .await;
        assert_eq!(success(&hello)["server"]["capabilities"]["nativeReview"], 1);
        assert_eq!(success(&hello)["server"]["protocolVersion"], "10.12");
    }
    let denied = guest
        .rpc("accept-changes.prepare", h.prepare("create-pr"))
        .await;
    assert!(
        denied.get("error").is_some(),
        "Guest cannot execute native stages"
    );
    std::fs::write(h.root.join("staged"), "one").unwrap();
    git(&h.root, &["add", "staged"]);
    std::fs::write(h.root.join("unstaged"), "leave").unwrap();
    let mut q = h.prepare("commit");
    q["options"] = json!({"pushAfterCommit":true,"createPRAfterPush":true});
    let p = uds.rpc("accept-changes.prepare", q).await;
    let p = success(&p).clone();
    let op = execute_query(&h, &p, "commit");
    assert!(ws
        .rpc("accept-changes.execute", op.clone())
        .await
        .get("error")
        .is_some());
    let result = uds.rpc("accept-changes.execute", op.clone()).await;
    let result = success(&result);
    assert_eq!(result["success"], true, "{result}; effects={}", h.counts());
    let e = &result["reviewExecution"];
    assert_eq!(e["outcome"]["status"], "created");
    assert_eq!(e["outcome"]["review"]["draft"], false);
    assert_eq!(e["gitReceipts"].as_array().unwrap().len(), 2);
    let head = git(&h.root, &["rev-parse", "HEAD"]);
    assert_eq!(e["gitReceipts"][0]["commitHash"], head);
    assert_eq!(e["gitReceipts"][1]["pushedSha"], head);
    assert_eq!(
        git(
            &h.dir.path().join("remote/group/project.git"),
            &["rev-parse", "main"]
        ),
        head
    );
    assert_eq!(
        git(&h.root, &["show", "--pretty=", "--name-only", "HEAD"]),
        "staged"
    );
    assert_eq!(e["publication"]["state"], "included");
    assert_eq!(h.counts()["posts"], 1);
    assert_eq!(h.counts()["pushes"], 1);
    let dup = uds.rpc("accept-changes.execute", op).await;
    assert_eq!(success(&dup), result);
    assert_eq!(h.counts()["posts"], 1);
    let p = ws
        .rpc("accept-changes.prepare", h.prepare("create-pr"))
        .await;
    let p = success(&p).clone();
    let reuse = ws
        .rpc("accept-changes.execute", execute_query(&h, &p, "create-pr"))
        .await;
    assert_eq!(
        success(&reuse)["reviewExecution"]["outcome"]["status"],
        "reused"
    );
    assert_eq!(success(&reuse)["reviewExecution"]["gitReceipts"], json!([]));
    assert_eq!(git(&h.root, &["rev-parse", "HEAD"]), head);
    let state = ws
        .rpc("accept-changes.reconcile", reconcile_query(&h, &p))
        .await;
    assert_eq!(success(&state)["state"], "settled");
    assert_eq!(
        success(&state)["reviewExecution"],
        success(&reuse)["reviewExecution"]
    );
    let released = ws
        .rpc("accept-changes.release", reconcile_query(&h, &p))
        .await;
    assert_eq!(success(&released), &json!({"released":true}));
    let after_release = ws
        .rpc("accept-changes.reconcile", reconcile_query(&h, &p))
        .await;
    assert_eq!(
        success(&after_release)["reviewExecution"],
        success(&reuse)["reviewExecution"]
    );
    // The retirement feed is private to this original socket and carries IDs,
    // not root/account payloads. Receiving it never grants final disclosure.
    while !ws.notices.iter().any(|notice| {
        notice["method"] == "accept-changes.retired"
            && notice["params"]["operationIds"]
                .as_array()
                .is_some_and(|ids| ids.contains(&p["reviewOperation"]["operationId"]))
    }) {
        let notice = ws.next().await;
        ws.notices.push(notice);
    }
    let notice = ws
        .notices
        .iter()
        .find(|notice| notice["method"] == "accept-changes.retired")
        .unwrap();
    assert!(notice["params"]["sequence"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .is_ok());
    assert_eq!(notice["params"].as_object().unwrap().len(), 4);
    // Separate native commit and push calls retain independent original receipts.
    std::fs::write(h.root.join("second-staged"), "second").unwrap();
    git(&h.root, &["add", "second-staged"]);
    let commit_p = ws.rpc("accept-changes.prepare", h.prepare("commit")).await;
    let commit_p = success(&commit_p).clone();
    let committed = ws
        .rpc(
            "accept-changes.execute",
            execute_query(&h, &commit_p, "commit"),
        )
        .await;
    assert_eq!(success(&committed)["success"], true, "{committed}");
    assert_eq!(
        success(&committed)["reviewExecution"]["gitReceipts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(h.counts()["pushes"], 1);
    let second_head = git(&h.root, &["rev-parse", "HEAD"]);
    let push_p = ws.rpc("accept-changes.prepare", h.prepare("push")).await;
    let push_p = success(&push_p).clone();
    let pushed = ws
        .rpc("accept-changes.execute", execute_query(&h, &push_p, "push"))
        .await;
    assert_eq!(success(&pushed)["success"], true, "{pushed}");
    assert_eq!(
        success(&pushed)["reviewExecution"]["gitReceipts"],
        json!([{"stage":"push","pushedSha":second_head}])
    );
    assert_eq!(
        git(
            &h.dir.path().join("remote/group/project.git"),
            &["rev-parse", "main"]
        ),
        second_head
    );
    assert_eq!(git(&h.root, &["rev-parse", "HEAD"]), second_head);
    assert_eq!(h.counts()["posts"], 1);
    assert_eq!(h.counts()["pushes"], 2);
    let mut member = h.wss(MEMBER).await;
    let member_p = member
        .rpc("accept-changes.prepare", h.prepare("create-pr"))
        .await;
    let member_p = success(&member_p).clone();
    assert!(member_p["reviewPreparation"]["source"]
        .get("connection")
        .is_none());
    assert!(member_p["reviewPreparation"]["target"]
        .get("connection")
        .is_none());
    let member_result = member
        .rpc(
            "accept-changes.execute",
            execute_query(&h, &member_p, "create-pr"),
        )
        .await;
    assert_eq!(
        success(&member_result)["reviewExecution"]["outcome"]["status"],
        "reused",
        "{member_result}; effects={}",
        h.counts()
    );
    assert!(
        success(&member_result)["reviewExecution"]["preparation"]["source"]
            .get("connection")
            .is_none()
    );
    h.store.remove_host_member(&h.member).await.unwrap();
    let refused = member
        .rpc("accept-changes.reconcile", reconcile_query(&h, &member_p))
        .await;
    assert!(
        refused.get("error").is_some(),
        "durable removal cannot disclose the receipt"
    );
    drop(member);
    drop(ws);
    let mut replacement = h.wss(TOKEN).await;
    assert!(replacement
        .rpc("accept-changes.reconcile", reconcile_query(&h, &p))
        .await
        .get("error")
        .is_some());
    assert_eq!(h.counts()["posts"], 1);
    assert_eq!(h.counts()["pushes"], 2);
    assert!(!h.guest.as_str().is_empty());
    assert!(!h.registered.as_str().is_empty());
    drop((uds, guest, replacement));
    h.shutdown().await;
}
#[tokio::test]
async fn native_review_real_equal_id_hosts_registered_root_and_no_cross_socket_grant() {
    if run_in_tls_process(
        "native_review_real_equal_id_hosts_registered_root_and_no_cross_socket_grant",
    )
    .await
    {
        return;
    }
    let host_a = Harness::boot().await;
    let host_b =
        Harness::boot_with_ids(Some((host_a.workspace.clone(), host_a.registered.clone()))).await;
    let mut client_a = host_a.uds().await;
    let mut client_b = host_b.uds().await;
    let mut query = host_a.prepare("commit");
    query["review"]["root"] =
        json!({"workspaceId":host_a.workspace,"kind":"registered","gitRootId":host_a.registered});
    let path = host_a.dir.path().join("secondary");
    std::fs::write(path.join("exact"), "exact").unwrap();
    git(&path, &["add", "exact"]);
    let preparation = client_a.rpc("accept-changes.prepare", query).await;
    let preparation = success(&preparation).clone();
    let execution = execute_query(&host_a, &preparation, "commit");
    assert!(client_b
        .rpc("accept-changes.execute", execution.clone())
        .await
        .get("error")
        .is_some());
    let primary = git(&host_a.root, &["rev-parse", "HEAD"]);
    let result = client_a.rpc("accept-changes.execute", execution).await;
    assert_eq!(success(&result)["success"], true, "{result}");
    assert_eq!(git(&host_a.root, &["rev-parse", "HEAD"]), primary);
    assert_eq!(
        git(&path, &["show", "--pretty=", "--name-only", "HEAD"]),
        "exact"
    );
    let malformed=client_a.rpc("accept-changes.execute",json!({"workspaceId":host_a.workspace,"action":"commit","review":null,"commitMessage":"must not downgrade"})).await;
    assert_eq!(malformed["error"]["code"], -32602);
    assert_eq!(host_a.counts()["posts"], 0);
    assert_eq!(host_b.counts()["posts"], 0);
    drop((client_a, client_b));
    host_a.shutdown().await;
    host_b.shutdown().await;
}

#[tokio::test]
async fn native_review_ordinary_composition_refuses_fixture_endpoint_before_effects() {
    if run_in_tls_process(
        "native_review_ordinary_composition_refuses_fixture_endpoint_before_effects",
    )
    .await
    {
        return;
    }
    let h = Harness::boot_composed(None, false).await;
    let mut uds = h.uds().await;
    let mut ws = h.wss(TOKEN).await;
    for client in [&mut uds, &mut ws] {
        let reply = client
            .rpc("accept-changes.prepare", h.prepare("create-pr"))
            .await;
        assert!(reply.get("error").is_some(), "{reply}");
    }
    assert_eq!(h.counts()["posts"], 0);
    assert_eq!(h.counts()["pushes"], 0);
    drop((uds, ws));
    h.shutdown().await;
}

// Driver-only protocol. No production Wire methods or authority seams are added.
const DRIVER_FRAME: usize = 8192;
const DRIVER_COMMANDS: usize = 128;
type DriverResult<T> = anyhow::Result<T>;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DriverDescriptor {
    version: u8,
    run_id: String,
    source_commit: String,
    source_tree: String,
    source_sha256: String,
    executable_sha256: String,
    lifetime_seconds: u64,
    scenarios: Vec<String>,
    #[serde(skip)]
    directory: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DriverRequest {
    version: u8,
    run_id: String,
    id: String,
    action: DriverAction,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "command",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum DriverAction {
    Snapshot {
        host: usize,
    },
    ArmProvider {
        host: usize,
        barrier: DriverBarrier,
    },
    BarrierStatus {
        host: usize,
    },
    ReleaseBarrier {
        host: usize,
        barrier_id: String,
    },
    RevokeMember {
        host: usize,
    },
    Stop {
        phase: DriverStop,
        pending: Vec<DriverPending>,
        envelopes: Vec<DriverCompletion>,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
enum DriverStop {
    Begin,
    Finish,
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DriverPending {
    host: usize,
    operation_id: String,
    socket_id: String,
    request_id: Value,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DriverCompletion {
    request: DriverPending,
    original_response: Value,
    history: Option<Value>,
}

fn driver_pending(pending: &[DriverPending]) -> DriverResult<()> {
    anyhow::ensure!(pending.len() <= 16, "pending bound");
    for (index, request) in pending.iter().enumerate() {
        anyhow::ensure!(
            request.host < 2
                && uuid::Uuid::parse_str(&request.operation_id).is_ok()
                && uuid::Uuid::parse_str(&request.socket_id).is_ok()
                && (request.request_id.is_string() || request.request_id.is_u64())
                && !pending[..index]
                    .iter()
                    .any(|p| p.host == request.host && p.operation_id == request.operation_id),
            "invalid or duplicate pending identity"
        );
    }
    Ok(())
}

fn driver_completed(pending: &[DriverPending], completed: &[DriverCompletion]) -> DriverResult<()> {
    anyhow::ensure!(
        completed.len() == pending.len(),
        "missing controller completion"
    );
    for request in pending {
        let matches: Vec<_> = completed.iter().filter(|c| c.request == *request).collect();
        anyhow::ensure!(matches.len() == 1, "completion identity mismatch");
        let observation = matches[0];
        anyhow::ensure!(
            observation.original_response.is_null()
                || observation.original_response["id"] == request.request_id,
            "original response identity mismatch"
        );
        let settled = |envelope: &Value| {
            envelope["jsonrpc"] == "2.0"
                && envelope.get("error").is_none()
                && envelope["result"]["state"] == "settled"
                && envelope["result"]["operationId"] == request.operation_id
                && envelope["result"]["reviewExecution"]["requestId"] == request.operation_id
        };
        anyhow::ensure!(
            settled(&observation.original_response)
                || observation.history.as_ref().is_some_and(settled),
            "original completion remains unobserved"
        );
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DriverBarrier {
    id: String,
    operation_id: String,
    method: String,
    route: String,
    ordinal: u64,
    mode: String,
    hold_seconds: u64,
}

fn driver_hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut text, byte| {
            write!(text, "{byte:02x}").unwrap();
            text
        })
}

fn driver_source_hash() -> String {
    driver_hash(include_bytes!("e2e_native_review_wire.rs"))
}

fn driver_identity() -> Value {
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    json!({"sourceCommit":git(&repository,&["rev-parse","HEAD"]),
        "sourceTree":git(&repository,&["rev-parse","HEAD^{tree}"]),
        "sourceSha256":driver_source_hash(),
        "executableSha256":driver_hash(&std::fs::read(std::env::current_exe().unwrap()).unwrap())})
}

fn private_json(path: &std::path::Path, value: &Value) -> DriverResult<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn publish_driver(path: &std::path::Path, value: &Value) -> DriverResult<()> {
    let temporary = path.with_extension("pending");
    private_json(&temporary, value)?;
    // Atomic no-clobber publication: an existing destination is never overwritten.
    std::fs::hard_link(&temporary, path)?;
    std::fs::remove_file(temporary)?;
    Ok(())
}

fn driver_descriptor(path: &std::path::Path, child: bool) -> DriverResult<DriverDescriptor> {
    driver_descriptor_phase(path, child, true, child)
}

fn driver_descriptor_phase(
    path: &std::path::Path,
    child: bool,
    check_identity: bool,
    tls_created: bool,
) -> DriverResult<DriverDescriptor> {
    use std::os::unix::fs::MetadataExt;
    anyhow::ensure!(
        path.is_absolute() && path.file_name().is_some_and(|n| n == "descriptor.json"),
        "descriptor path"
    );
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("descriptor parent"))?;
    anyhow::ensure!(
        parent.as_os_str().len() <= 64,
        "owned Unix socket path bound"
    );
    anyhow::ensure!(std::fs::canonicalize(parent)? == parent, "symlink parent");
    let directory = std::fs::symlink_metadata(parent)?;
    let file = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid takes no pointers and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    anyhow::ensure!(
        directory.is_dir() && directory.mode() & 0o777 == 0o700 && directory.uid() == uid,
        "private directory"
    );
    anyhow::ensure!(
        file.is_file() && file.mode() & 0o777 == 0o600 && file.uid() == uid && file.nlink() == 1,
        "private descriptor"
    );
    anyhow::ensure!(file.len() <= DRIVER_FRAME as u64, "descriptor bound");
    let mut descriptor: DriverDescriptor = serde_json::from_slice(&std::fs::read(path)?)?;
    anyhow::ensure!(
        descriptor.version == 1 && uuid::Uuid::parse_str(&descriptor.run_id).is_ok(),
        "descriptor version/run"
    );
    anyhow::ensure!(
        (30..=1200).contains(&descriptor.lifetime_seconds),
        "lifetime bound"
    );
    anyhow::ensure!(
        !descriptor.scenarios.is_empty()
            && descriptor.scenarios.len() <= 6
            && descriptor
                .scenarios
                .iter()
                .all(|s| matches!(s.as_str(), "ready-stop" | "held-stop" | "frontend")),
        "scenario list"
    );
    if check_identity {
        let actual = driver_identity();
        for (key, value) in [
            ("sourceCommit", &descriptor.source_commit),
            ("sourceTree", &descriptor.source_tree),
            ("sourceSha256", &descriptor.source_sha256),
            ("executableSha256", &descriptor.executable_sha256),
        ] {
            anyhow::ensure!(
                actual[key].as_str() == Some(value),
                "source/artifact identity mismatch"
            );
        }
    }
    for name in [
        "ready.json",
        "stopped.json",
        "control.sock",
        "control.jsonl",
        "credentials",
        "tls",
        "worker.json",
        "failed.json",
        "worker-stopped.json",
        "worker-failed.json",
        "ownership.jsonl",
        "supervisor.json",
    ] {
        if child && matches!(name, "worker.json" | "ownership.jsonl" | "supervisor.json") {
            // The parent creates this checkpoint immediately after spawning us.
            // It is never an input to native authority or successful completion.
            continue;
        }
        if child && name == "tls" {
            if tls_created {
                anyhow::ensure!(
                    std::fs::canonicalize(parent.join(name))? == parent.join(name),
                    "owned TLS path"
                );
            } else {
                anyhow::ensure!(!parent.join(name).exists(), "pre-existing TLS path");
            }
            anyhow::ensure!(
                std::env::var_os("INTENT_REVIEW_TLS_FIXTURE").as_deref()
                    == Some(parent.join(name).as_os_str()),
                "owned TLS input"
            );
            anyhow::ensure!(
                std::env::var_os("SSL_CERT_FILE").as_deref()
                    == Some(parent.join(name).join("ca.pem").as_os_str()),
                "owned CA input"
            );
        } else {
            anyhow::ensure!(
                std::fs::symlink_metadata(parent.join(name))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                "pre-existing output"
            );
        }
    }
    descriptor.directory = parent.into();
    Ok(descriptor)
}

async fn driver_frame(stream: &mut BufReader<UnixStream>) -> DriverResult<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        let available = stream.fill_buf().await?;
        anyhow::ensure!(!available.is_empty(), "incomplete control frame");
        let end = available.iter().position(|b| *b == b'\n');
        let count = end.map_or(available.len(), |n| n + 1);
        anyhow::ensure!(
            bytes.len() + count <= DRIVER_FRAME,
            "control frame overflow"
        );
        bytes.extend_from_slice(&available[..count]);
        stream.consume(count);
        if end.is_some() {
            return Ok(bytes);
        }
    }
}

async fn driver_write(stream: &mut BufReader<UnixStream>, value: &Value) -> DriverResult<()> {
    let bytes = serde_json::to_vec(value)?;
    anyhow::ensure!(bytes.len() < DRIVER_FRAME, "response frame overflow");
    stream.get_mut().write_all(&bytes).await?;
    stream.get_mut().write_all(b"\n").await?;
    Ok(())
}

#[derive(Default)]
struct DriverCommands(std::collections::BTreeMap<String, (Value, Value)>);
impl DriverCommands {
    fn previous(&self, request: &DriverRequest, raw: &Value) -> DriverResult<Option<Value>> {
        anyhow::ensure!(
            request.version == 1 && uuid::Uuid::parse_str(&request.id).is_ok(),
            "request version/id"
        );
        if let Some((old, reply)) = self.0.get(&request.id) {
            anyhow::ensure!(old == raw, "changed duplicate request");
            return Ok(Some(reply.clone()));
        }
        anyhow::ensure!(self.0.len() < DRIVER_COMMANDS, "control command overflow");
        Ok(None)
    }
    fn remember(&mut self, id: String, raw: Value, reply: Value) {
        self.0.insert(id, (raw, reply));
    }
}

impl DriverBarrier {
    fn validate(&self) -> DriverResult<()> {
        anyhow::ensure!(
            uuid::Uuid::parse_str(&self.id).is_ok()
                && uuid::Uuid::parse_str(&self.operation_id).is_ok(),
            "barrier identity"
        );
        anyhow::ensure!(
            matches!(self.method.as_str(), "GET" | "POST")
                && matches!(
                    self.route.as_str(),
                    "project" | "branches" | "mergeRequests"
                ),
            "provider route"
        );
        anyhow::ensure!(
            self.method != "POST" || self.route == "mergeRequests",
            "provider write route"
        );
        anyhow::ensure!(
            matches!(
                self.mode.as_str(),
                "hold" | "refuse" | "malformed" | "loseAfterPost"
            ),
            "provider mode"
        );
        anyhow::ensure!(
            self.mode != "loseAfterPost" || self.method == "POST",
            "lost response requires POST"
        );
        anyhow::ensure!(
            (1..=1024).contains(&self.ordinal) && (1..=60).contains(&self.hold_seconds),
            "barrier bound"
        );
        Ok(())
    }
}

async fn driver_provider(host: &Harness, query: Value) -> DriverResult<Value> {
    let mut socket =
        BufReader::new(UnixStream::connect(host.dir.path().join("driver-control.sock")).await?);
    driver_write(&mut socket, &query).await?;
    let response: Value = serde_json::from_slice(&driver_frame(&mut socket).await?)?;
    anyhow::ensure!(
        response.get("error").is_none(),
        "fixture control refusal: {response}"
    );
    Ok(response)
}

fn driver_snapshot(host: &Harness, provider: &Value) -> Value {
    json!({"provider":provider,"effects":host.counts(),
        "primaryHead":git(&host.root,&["rev-parse","HEAD"]),
        "registeredHead":git(&host.dir.path().join("secondary"),&["rev-parse","HEAD"]),
        "remoteHead":git(&host.dir.path().join("remote/group/project.git"),&["rev-parse","main"]),
        "index":driver_hash(git(&host.root,&["ls-files","--stage"]).as_bytes()),
        "worktree":driver_hash(git(&host.root,&["diff","--binary","HEAD"]).as_bytes()),
        "status":git(&host.root,&["status","--porcelain=v1"])})
}

fn driver_safe(value: &Value) -> bool {
    match value {
        Value::String(text) => ![TOKEN, MEMBER, GUEST, "stored-pat"]
            .iter()
            .any(|s| text.contains(s)),
        Value::Array(items) => items.iter().all(driver_safe),
        Value::Object(map) => map.iter().all(|(key, value)| {
            !matches!(
                key.as_str(),
                "token" | "password" | "Authorization" | "PRIVATE-TOKEN" | "privateKey"
            ) && driver_safe(value)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
    }
}

async fn driver_action(
    hosts: &[Harness],
    action: &DriverAction,
    stopping: &mut Option<Vec<DriverPending>>,
) -> DriverResult<(Value, bool)> {
    let host_at = |index: usize| {
        hosts
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("unknown host"))
    };
    match action {
        DriverAction::Snapshot { host } => {
            let host = host_at(*host)?;
            Ok((
                driver_snapshot(
                    host,
                    &driver_provider(host, json!({"command":"snapshot"})).await?,
                ),
                false,
            ))
        }
        DriverAction::BarrierStatus { host } => Ok((
            driver_provider(host_at(*host)?, json!({"command":"snapshot"})).await?,
            false,
        )),
        DriverAction::ArmProvider { host, barrier } => {
            anyhow::ensure!(stopping.is_none(), "driver stopping");
            barrier.validate()?;
            Ok((
                driver_provider(host_at(*host)?, json!({"command":"arm","barrier":barrier}))
                    .await?,
                false,
            ))
        }
        DriverAction::ReleaseBarrier { host, barrier_id } => Ok((
            driver_provider(
                host_at(*host)?,
                json!({"command":"release","id":barrier_id}),
            )
            .await?,
            false,
        )),
        DriverAction::RevokeMember { host } => {
            anyhow::ensure!(stopping.is_none(), "driver stopping");
            let host = host_at(*host)?;
            host.store.remove_host_member(&host.member).await?;
            Ok((json!({"revoked":true}), false))
        }
        DriverAction::Stop {
            phase,
            pending,
            envelopes,
        } => {
            anyhow::ensure!(
                envelopes.len() <= 16 && driver_safe(&serde_json::to_value(envelopes)?),
                "unsafe/unbounded controller observation"
            );
            match phase {
                DriverStop::Begin => {
                    anyhow::ensure!(stopping.is_none() && envelopes.is_empty(), "stop phase");
                    driver_pending(pending)?;
                    *stopping = Some(pending.clone());
                    let mut result = Vec::new();
                    for host in hosts {
                        result.push(driver_provider(host, json!({"command":"drain"})).await?);
                    }
                    Ok((
                        json!({"state":"stopping","provider":result,"controllerPending":pending}),
                        false,
                    ))
                }
                DriverStop::Finish => {
                    anyhow::ensure!(
                        stopping.is_some() && pending.is_empty(),
                        "controller must join original requests before finish"
                    );
                    driver_completed(stopping.as_ref().unwrap(), envelopes)?;
                    let mut result = Vec::new();
                    for host in hosts {
                        let state = timeout(Duration::from_secs(5), async {
                            loop {
                                let state =
                                    driver_provider(host, json!({"command":"snapshot"})).await?;
                                if state["active"] == 0 {
                                    break Ok::<_, anyhow::Error>(state);
                                }
                                // timing-guard: wait for this fixture's actual handler completion
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        })
                        .await??;
                        anyhow::ensure!(
                            state["active"] == 0
                                && state["overflow"] == false
                                && state["failure"].is_null(),
                            "provider not cleanly drained: {state}"
                        );
                        result.push(driver_snapshot(host, &state));
                    }
                    Ok((
                        json!({"state":"drained","hosts":result,"controllerEnvelopes":envelopes,
                        "commandJoinEvidence":"external controller; fixture handler completion is separate"}),
                        true,
                    ))
                }
            }
        }
    }
}

async fn driver_loop(descriptor: &DriverDescriptor) -> DriverResult<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let started = std::time::Instant::now();
    let a = Harness::boot_driver(None, true, Some(descriptor)).await;
    let b = Harness::boot_driver(
        Some((a.workspace.clone(), a.registered.clone())),
        true,
        Some(descriptor),
    )
    .await;
    let mut hosts = vec![a, b];
    let credentials = descriptor.directory.join("credentials");
    std::fs::create_dir(&credentials)?;
    std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o700))?;
    for (name, token) in [("owner", TOKEN), ("member", MEMBER), ("guest", GUEST)] {
        private_json(&credentials.join(name), &json!({"token":token}))?;
    }
    let socket = descriptor.directory.join("control.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let endpoints: Vec<_> = hosts
        .iter()
        .map(|host| {
            json!({"uds":host.dir.path().join("intentd.sock"),
        "wss":format!("wss://localhost:{}/ws",host.port),"fingerprint":host.ws.fingerprint(),
        "workspaceId":host.workspace,"registeredRootId":host.registered,"instance":host.instance,
        "providerTransport":"explicit loopback HTTP","gitTransport":"verified HTTPS DNS localhost",
        "fixturePid":host.fixture.id(),"fixtureProcess":driver_process(host.fixture.id())})
        })
        .collect();
    publish_driver(
        &descriptor.directory.join("ready.json"),
        &json!({"version":1,"runId":descriptor.run_id,
        "identity":driver_identity(),"pid":std::process::id(),"process":driver_process(std::process::id()),"hosts":endpoints,"credentialDirectory":credentials,
        "control":socket,"readyIsAdmission":false}),
    )?;
    let mut journal = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(descriptor.directory.join("control.jsonl"))?;
    let mut cache = DriverCommands::default();
    let mut stopping = None;
    let mut terminal = None;
    let work = async {
        loop {
            let (connection, _) = listener.accept().await?;
            let mut stream = BufReader::new(connection);
            let raw: Value = serde_json::from_slice(
                &timeout(Duration::from_secs(5), driver_frame(&mut stream)).await??,
            )?;
            let request: DriverRequest = serde_json::from_value(raw.clone())?;
            anyhow::ensure!(request.run_id == descriptor.run_id, "foreign run");
            if let Some(reply) = cache.previous(&request, &raw)? {
                driver_write(&mut stream, &reply).await?;
                continue;
            }
            let (reply, finished) =
                match driver_action(&hosts, &request.action, &mut stopping).await {
                    Ok((value, finished)) => (json!({"id":request.id,"result":value}), finished),
                    Err(error) => (json!({"id":request.id,"error":error.to_string()}), false),
                };
            anyhow::ensure!(
                driver_safe(&raw) && driver_safe(&reply),
                "unsafe observation"
            );
            writeln!(
                journal,
                "{}",
                json!({"sequence":cache.0.len()+1,"request":raw,"response":reply})
            )?;
            journal.sync_all()?;
            let response = if finished {
                json!({"id":request.id,"result":{"state":"drained","hostsDrained":2}})
            } else {
                reply.clone()
            };
            cache.remember(request.id, raw, response.clone());
            driver_write(&mut stream, &response).await?;
            if finished {
                terminal = Some(reply);
                break;
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    let outcome = timeout(
        Duration::from_secs(descriptor.lifetime_seconds.saturating_sub(10))
            .saturating_sub(started.elapsed()),
        work,
    )
    .await;
    drop(listener);
    for host in &hosts {
        let _ = driver_provider(host, json!({"command":"drain"})).await;
    }
    // Closing listeners is not a join of a foreign client's request. Only a
    // successful two-phase controller stop can produce a successful stopped file.
    let mut exits = Vec::new();
    for host in hosts.drain(..) {
        let pid = host.fixture.id();
        let uds = host.dir.path().join("intentd.sock");
        let port = host.port;
        let status = driver_shutdown(host).await?;
        exits.push(json!({"fixturePid":pid,"status":status,"reaped":true,"udsClosed":UnixStream::connect(uds).await.is_err(),
            "tcpClosed":tokio::net::TcpStream::connect(("127.0.0.1",port)).await.is_err()}));
    }
    std::fs::remove_file(socket)?;
    if !matches!(&outcome, Ok(Ok(()))) {
        private_json(
            &descriptor.directory.join("worker-failed.json"),
            &json!({"success":false,"outcome":format!("{outcome:?}"),"cleanup":exits,"nativeCompletion":"not established"}),
        )?;
    }
    outcome??;
    publish_driver(
        &descriptor.directory.join("worker-stopped.json"),
        &json!({"version":1,"runId":descriptor.run_id,
        "identity":driver_identity(),"terminal":terminal,"cleanup":exits,"success":true}),
    )?;
    Ok(())
}

#[test]
#[ignore = "explicit owned fixture descriptor and separate execution release required"]
fn native_review_fixture_driver() {
    #[cfg(target_os = "linux")]
    driver_ownership::entry().unwrap();
    #[cfg(not(target_os = "linux"))]
    panic!("The owned fixture supervisor requires Linux pidfd/subreaper support");
}

const DRIVER_PROVIDER_SUPPORT: &str = r#"
import socket,time
class DriverProvider:
    def __init__(self,root):
        self.root=root;self.lock=threading.RLock();self.events=[];self.sequence=0;self.active=0;self.overflow=False;self.failure=None
        self.arms={};self.counts={};self.operations={};self.draining=False
        self.server=socket.socket(socket.AF_UNIX);self.server.bind(str(root/'driver-control.sock'));os.chmod(root/'driver-control.sock',0o600);self.server.listen(1)
        threading.Thread(target=self.control,daemon=True).start()
    def seed(self):
        if json.loads((self.root/'driver.json').read_text()).get('matchingReview'):
            state['reviews']=[{'iid':7,'project_id':42,'source_project_id':42,'target_project_id':42,'source_branch':'main','target_branch':'trunk','state':'opened','draft':False,'title':'Existing fixture MR','description':'seeded before listen','web_url':instance+'/group/project/-/merge_requests/7','sha':sha('main'),'author':{'username':'fixture'},'created_at':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z'}]
    def event(self,request,phase):
        operation=request.get('operation','unassigned');self.operations[operation]=self.operations.get(operation,0)+1
        if len(self.events)>=1024 or self.operations[operation]>256:self.overflow=True;self.failure='observation overflow';return
        self.sequence+=1;self.events.append(dict(sequence=self.sequence,request=request['number'],method=request['method'],route=request['route'],ordinal=request['ordinal'],operation=operation,phase=phase,authenticated=request['authenticated']))
        temp=self.root/'driver-events.pending';temp.write_text(json.dumps(self.events));temp.replace(self.root/'driver-events.json')
    def snapshot(self):
        return dict(active=self.active,overflow=self.overflow,failure=self.failure,sequence=self.sequence,counts=self.counts,
                    barriers=[dict(id=a['id'],entered=a.get('entered',False),released=a.get('released',False),finished=a.get('finished',False)) for a in self.arms.values()],events=self.events[-8:])
    def control(self):
        while True:
            connection,_=self.server.accept()
            try:
                connection.settimeout(5);data=b''
                while not data.endswith(b'\n') and len(data)<8192:
                    part=connection.recv(1)
                    if not part:break
                    data+=part
                if len(data)>=8192:raise ValueError('frame overflow')
                command=json.loads(data)
                with self.lock:
                    kind=command['command']
                    if kind=='arm':
                        a=command['barrier'];key=(a['method'],a['route'],a['ordinal'])
                        if self.draining or len(self.arms)>=16 or key in self.arms or a['ordinal']<=self.counts.get(a['method']+':'+a['route'],0):raise ValueError('barrier unavailable')
                        a['gate']=threading.Event();self.arms[key]=a
                    elif kind=='release':
                        arm=next((a for a in self.arms.values() if a['id']==command['id']),None)
                        if arm is None:raise ValueError('unknown barrier')
                        arm['released']=True;arm['gate'].set()
                    elif kind=='drain':
                        self.draining=True
                        for arm in self.arms.values():arm['released']=True;arm['gate'].set()
                    elif kind!='snapshot':raise ValueError('unknown control')
                    result=self.snapshot()
                connection.sendall(json.dumps(result).encode()+b'\n')
            except Exception:connection.sendall(b'{"error":"owned provider control refused"}\n')
            finally:connection.close()
    def enter(self,handler):
        path=urllib.parse.unquote(urllib.parse.urlsplit(handler.path).path)
        api=path.startswith('/api/v4/')
        authenticated=(handler.headers.get('PRIVATE-TOKEN')=='stored-pat' or handler.headers.get('Authorization')=='Bearer stored-pat') if api else handler.headers.get('Authorization')=='Basic '+base64.b64encode(b'oauth2:stored-pat').decode()
        route=('mergeRequests' if path.endswith('/merge_requests') else 'branches' if '/repository/branches' in path else 'project' if '/projects/' in path else 'user') if api else 'git'
        with self.lock:
            key=handler.command+':'+route;ordinal=self.counts.get(key,0)+1;self.counts[key]=ordinal
            arm=self.arms.get((handler.command,route,ordinal)) if authenticated else None;request=dict(number=sum(self.counts.values()),method=handler.command,route=route,ordinal=ordinal,arm=arm,authenticated=authenticated)
            if arm:arm['entered']=True;request['operation']=arm['operationId']
            self.active+=1;self.event(request,'entered');return request
    def intercept(self,handler,request):
        if request is None:return False
        arm=request.get('arm')
        if not arm:return False
        if arm['mode']=='refuse':handler.answer(403,{'message':'owned refusal'});return True
        if arm['mode']=='malformed':
            handler.send_response(200);handler.send_header('Content-Length','1');handler.end_headers();handler.wfile.write(b'{');return True
        return False
    def answer(self,handler,status,body):
        request=handler.driver_request
        if request is None:return False
        arm=request.get('arm')
        if arm and arm['mode']=='hold':
            if not arm['gate'].wait(arm['holdSeconds']):
                with self.lock:self.failure='provider hold timed out'
                handler.close_connection=True;return True
        if arm and arm['mode']=='loseAfterPost' and status==201:handler.close_connection=True;return True
        return False
    def effect(self,request,phase):
        if request:
            with self.lock:self.event(request,phase)
    def finish(self,request):
        if request:
            with self.lock:
                self.event(request,'finished');self.active-=1
                if request.get('arm'):request['arm']['finished']=True
    def merge_request(self,handler):
        with lock:
            if handler.command=='POST':
                data=json.loads(handler.body());assert data['source_branch']=='main' and data['target_branch']=='trunk';assert not data.get('draft',False)
                state['posts']+=1
                state['reviews'].append({'iid':7,'project_id':42,'source_project_id':42,'target_project_id':42,'source_branch':'main','target_branch':'trunk','state':'opened','draft':False,'title':'Observed ready MR','description':'observed','web_url':instance+'/group/project/-/merge_requests/7','sha':sha('main'),'author':{'username':'fixture'},'created_at':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z'})
                save();body=state['reviews'][-1];status=201
            else:body=list(state['reviews']);status=200
        if status==201:self.effect(handler.driver_request,'mr-persisted')
        handler.answer(status,body)
"#;

async fn driver_shutdown(mut host: Harness) -> DriverResult<Value> {
    use std::os::unix::process::ExitStatusExt;
    if let Some(shutdown) = host.shutdown.take() {
        let _ = shutdown.send(());
    }
    if let Some(listener) = host.listener.take() {
        timeout(Duration::from_secs(5), listener).await???;
    }
    anyhow::ensure!(host.ws.bound_port().await.is_none(), "WSS did not stop");
    host.fixture.kill()?;
    let status = host.fixture.wait()?;
    Ok(
        json!({"code":status.code(),"signal":status.signal(),"reason":"owned fixture server stopped after handler drain"}),
    )
}

#[cfg(target_os = "linux")]
struct DriverTest {
    directory: tempfile::TempDir,
    child: driver_ownership::Controller,
    ready: Value,
    run_id: String,
}
fn driver_process(pid: u32) -> Value {
    let start = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ")
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .map(str::to_owned)
        });
    json!({"pid":pid,"startTicks":start})
}

#[cfg(target_os = "linux")]
impl DriverTest {
    async fn start(scenario: &str) -> Self {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let directory = common::test_tempdir_in("/tmp", "itd-driver-control-");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let identity = driver_identity();
        let run_id = uuid::Uuid::new_v4().to_string();
        let descriptor = json!({"version":1,"runId":run_id,"sourceCommit":identity["sourceCommit"],
            "sourceTree":identity["sourceTree"],"sourceSha256":identity["sourceSha256"],
            "executableSha256":identity["executableSha256"],"lifetimeSeconds":90,"scenarios":[scenario]});
        private_json(&directory.path().join("descriptor.json"), &descriptor).unwrap();
        let log = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.path().join("child.log"))
            .unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "native_review_fixture_driver",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                "INTENT_REVIEW_DRIVER_DESCRIPTOR",
                directory.path().join("descriptor.json"),
            )
            .env_remove("INTENT_REVIEW_DRIVER_CHILD")
            .env("INTENTD_TEST_KEEP_TMP", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        let child = driver_ownership::Controller::spawn(&mut command).unwrap();
        let mut owned = Self {
            directory,
            child,
            ready: Value::Null,
            run_id,
        };
        let ready = timeout(Duration::from_secs(25), async {
            loop {
                if let Ok(bytes) = std::fs::read(owned.directory.path().join("ready.json")) {
                    break serde_json::from_slice(&bytes).unwrap();
                }
                assert!(
                    owned.child.poll().unwrap().is_none(),
                    "driver exited: {}",
                    std::fs::read_to_string(owned.directory.path().join("child.log")).unwrap()
                );
                // timing-guard: wait for this owned child's atomic readiness file
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        owned.ready = ready;
        owned
    }
    async fn control(&self, action: Value) -> Value {
        let request = json!({"version":1,"runId":self.run_id,"id":uuid::Uuid::new_v4().to_string(),"action":action});
        let mut stream = BufReader::new(
            UnixStream::connect(self.directory.path().join("control.sock"))
                .await
                .unwrap(),
        );
        driver_write(&mut stream, &request).await.unwrap();
        serde_json::from_slice(
            &timeout(Duration::from_secs(10), driver_frame(&mut stream))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }
    fn record(&self, name: &str, value: &Value) {
        assert!(driver_safe(value));
        private_json(&self.directory.path().join(name), value).unwrap();
    }
    async fn finish(&mut self, envelopes: Vec<Value>) -> Value {
        let finish = self
            .control(json!({"command":"stop","phase":"finish","pending":[],"envelopes":envelopes}))
            .await;
        self.record("finish-response.json", &finish);
        assert!(finish.get("error").is_none(), "{finish}");
        let status = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.poll().unwrap() {
                    break status;
                }
                // timing-guard: reap this original driver's observed child exit
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        self.record(
            "child-exit.json",
            &json!({"pid":self.child.id(),"code":status.code(),"success":status.success()}),
        );
        assert!(
            status.success(),
            "driver child failed; retained {}",
            self.directory.path().display()
        );
        let stopped: Value = serde_json::from_slice(
            &std::fs::read(self.directory.path().join("stopped.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stopped["success"], true);
        assert_eq!(stopped["ownership"]["complete"], true);
        assert_eq!(stopped["ownership"]["failed"], false);
        assert!(stopped["worker"]["cleanup"]
            .as_array()
            .unwrap()
            .iter()
            .all(|x| x["udsClosed"] == true && x["tcpClosed"] == true && x["reaped"] == true));
        stopped
    }
}

struct DriverWire {
    socket_id: String,
    writer: futures_util::stream::SplitSink<common::TlsWs, Message>,
    replies: tokio::sync::mpsc::Receiver<Value>,
    reader: tokio::task::JoinHandle<()>,
    buffered: Vec<Value>,
    next_id: u64,
}
impl DriverWire {
    async fn member(ready: &Value) -> Self {
        let host = &ready["hosts"][0];
        let url = format!("{}?token={MEMBER}", host["wss"].as_str().unwrap());
        let port = url
            .split(':')
            .nth(2)
            .unwrap()
            .split('/')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let socket = common::wss_connect_with_retry(
            port,
            client_config(host["fingerprint"].as_str().unwrap()),
            &url,
        )
        .await;
        let (writer, mut input) = socket.split();
        let (send, replies) = tokio::sync::mpsc::channel(64);
        let reader = tokio::spawn(async move {
            while let Some(frame) = input.next().await {
                match frame.unwrap() {
                    Message::Text(text) => {
                        if send
                            .send(serde_json::from_str(&text).unwrap())
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(_)
                    | Message::Pong(_)
                    | Message::Binary(_)
                    | Message::Frame(_) => {}
                }
            }
        });
        Self {
            socket_id: uuid::Uuid::new_v4().to_string(),
            writer,
            replies,
            reader,
            buffered: Vec::new(),
            next_id: 0,
        }
    }
    async fn send(&mut self, method: &str, params: Value) -> u64 {
        self.next_id += 1;
        self.writer
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":self.next_id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        self.next_id
    }
    async fn response(&mut self, id: u64) -> Value {
        if let Some(index) = self.buffered.iter().position(|v| v["id"] == id) {
            return self.buffered.remove(index);
        }
        timeout(Duration::from_secs(15), async {
            loop {
                let value = self.replies.recv().await.expect("original response reader");
                if value["id"] == id {
                    return value;
                }
                assert!(
                    self.buffered.len() < 64,
                    "original response observation overflow"
                );
                self.buffered.push(value);
            }
        })
        .await
        .unwrap()
    }
    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params).await;
        self.response(id).await
    }
    async fn close(mut self) {
        self.writer.send(Message::Close(None)).await.unwrap();
        timeout(Duration::from_secs(5), self.reader)
            .await
            .unwrap()
            .unwrap();
    }
}

#[test]
fn native_review_driver_control_contract() {
    let raw = json!({"version":1,"runId":uuid::Uuid::new_v4().to_string(),"id":uuid::Uuid::new_v4().to_string(),"action":{"command":"snapshot","host":0}});
    let request: DriverRequest = serde_json::from_value(raw.clone()).unwrap();
    let mut cache = DriverCommands::default();
    assert!(cache.previous(&request, &raw).unwrap().is_none());
    cache.remember(request.id.clone(), raw.clone(), json!({"result":"once"}));
    assert_eq!(
        cache.previous(&request, &raw).unwrap(),
        Some(json!({"result":"once"}))
    );
    let mut changed = raw.clone();
    changed["action"]["host"] = json!(1);
    assert!(cache.previous(&request, &changed).is_err());
    for _ in 1..DRIVER_COMMANDS {
        cache.remember(uuid::Uuid::new_v4().to_string(), json!(null), json!(null));
    }
    let mut fresh = raw.clone();
    fresh["id"] = json!(uuid::Uuid::new_v4().to_string());
    assert!(cache
        .previous(&serde_json::from_value(fresh.clone()).unwrap(), &fresh)
        .is_err());
    let mut extra = raw;
    extra["action"]["sql"] = json!("forbidden");
    assert!(serde_json::from_value::<DriverRequest>(extra).is_err());
    let mut barrier = DriverBarrier {
        id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        method: "GET".into(),
        route: "mergeRequests".into(),
        ordinal: 1,
        mode: "hold".into(),
        hold_seconds: 5,
    };
    assert!(barrier.validate().is_ok());
    barrier.route = "arbitrary".into();
    assert!(barrier.validate().is_err());
    assert!(!driver_safe(&json!({"value":MEMBER})));
    let observations = common::test_tempdir_in("/tmp", "itd-driver-events-");
    let check = format!(
        "{DRIVER_PROVIDER_SUPPORT}\n{check}",
        check = r"
import json,pathlib,sys
p=DriverProvider.__new__(DriverProvider);p.root=pathlib.Path(sys.argv[1]);p.events=[];p.sequence=0;p.operations={};p.overflow=False;p.failure=None
request=dict(number=1,method='GET',route='mergeRequests',ordinal=1,operation='one',authenticated=True)
for _ in range(257):p.event(request,'entered')
assert p.overflow and len(p.events)==256 and p.sequence==256
p.events=[];p.sequence=0;p.operations={};p.overflow=False;p.failure=None
for operation in range(4):
    request['operation']=str(operation)
    for _ in range(256):p.event(request,'finished')
request['operation']='overflow';p.event(request,'entered')
assert p.overflow and len(p.events)==1024 and p.sequence==1024
assert [e['sequence'] for e in p.events]==list(range(1,1025))
"
    );
    let result = std::process::Command::new("python3")
        .args(["-c", &check])
        .arg(observations.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let pending = DriverPending {
        host: 0,
        operation_id: uuid::Uuid::new_v4().to_string(),
        socket_id: uuid::Uuid::new_v4().to_string(),
        request_id: json!(7),
    };
    driver_pending(std::slice::from_ref(&pending)).unwrap();
    assert!(driver_pending(&[pending.clone(), pending.clone()]).is_err());
    assert!(driver_completed(std::slice::from_ref(&pending), &[]).is_err());
    let mut observation = DriverCompletion {
        request: pending.clone(),
        original_response: json!({"jsonrpc":"2.0","id":7,"error":{"code":-1}}),
        history: None,
    };
    assert!(driver_completed(
        std::slice::from_ref(&pending),
        std::slice::from_ref(&observation)
    )
    .is_err());
    observation.history = Some(
        json!({"jsonrpc":"2.0","id":8,"result":{"state":"pending","operationId":pending.operation_id}}),
    );
    assert!(driver_completed(
        std::slice::from_ref(&pending),
        std::slice::from_ref(&observation)
    )
    .is_err());
    observation.history = Some(
        json!({"jsonrpc":"2.0","id":8,"result":{"state":"settled","operationId":pending.operation_id,"reviewExecution":{"requestId":pending.operation_id}}}),
    );
    driver_completed(
        std::slice::from_ref(&pending),
        std::slice::from_ref(&observation),
    )
    .unwrap();
    observation.request.socket_id = uuid::Uuid::new_v4().to_string();
    assert!(driver_completed(
        std::slice::from_ref(&pending),
        std::slice::from_ref(&observation)
    )
    .is_err());
}

#[tokio::test]
async fn native_review_driver_path_and_frame_bounds() {
    use std::os::unix::fs::PermissionsExt;
    let directory = common::test_tempdir_in("/tmp", "itd-driver-schema-");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = directory.path().join("descriptor.json");
    private_json(&path, &json!({"version":1,"extra":true})).unwrap();
    assert!(driver_descriptor(&path, false).is_err());
    assert!(private_json(&path, &json!({})).is_err());
    let link = directory.path().join("alias");
    std::os::unix::fs::symlink(directory.path(), &link).unwrap();
    assert!(driver_descriptor(&link.join("descriptor.json"), false).is_err());
    let (read, mut write) = UnixStream::pair().unwrap();
    let sender = tokio::spawn(async move {
        write
            .write_all(&vec![b'x'; DRIVER_FRAME + 1])
            .await
            .unwrap();
    });
    assert!(driver_frame(&mut BufReader::new(read)).await.is_err());
    sender.await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_review_driver_ready_stop() {
    let mut driver = DriverTest::start("ready-stop").await;
    assert_eq!(
        driver.ready["hosts"][0]["workspaceId"],
        driver.ready["hosts"][1]["workspaceId"]
    );
    assert_eq!(
        driver.ready["hosts"][0]["registeredRootId"],
        driver.ready["hosts"][1]["registeredRootId"]
    );
    assert_ne!(
        driver.ready["hosts"][0]["instance"],
        driver.ready["hosts"][1]["instance"]
    );
    let mut wire = DriverWire::member(&driver.ready).await;
    let hello = wire
        .call("client.hello", json!({"clientId":"driver-member-control"}))
        .await;
    driver.record("hello.json", &hello);
    assert_eq!(success(&hello)["server"]["capabilities"]["nativeReview"], 1);
    wire.close().await;
    let begin = driver
        .control(json!({"command":"stop","phase":"begin","pending":[],"envelopes":[]}))
        .await;
    driver.record("begin-response.json", &begin);
    assert!(begin.get("error").is_none(), "{begin}");
    driver.finish(vec![]).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_review_driver_stop_with_admitted_response() {
    let mut driver = DriverTest::start("held-stop").await;
    let mut wire = DriverWire::member(&driver.ready).await;
    let host = &driver.ready["hosts"][0];
    let workspace = host["workspaceId"].clone();
    let query = json!({"workspaceId":workspace,"action":"create-pr","review":{"root":{"workspaceId":workspace,"kind":"primary"},"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":host["instance"],"projectPath":"group/project"}},"targetBranch":"trunk","pushRemote":"forge"}});
    let prepared = wire.call("accept-changes.prepare", query).await;
    driver.record("prepare.json", &prepared);
    let preparation = success(&prepared);
    let operation = &preparation["reviewOperation"];
    let status = driver
        .control(json!({"command":"barrierStatus","host":0}))
        .await;
    let ordinal = status["result"]["counts"]["GET:mergeRequests"]
        .as_u64()
        .unwrap_or(0)
        + 1;
    let arm = driver.control(json!({"command":"armProvider","host":0,"barrier":{"id":uuid::Uuid::new_v4().to_string(),"operationId":operation["operationId"],"method":"GET","route":"mergeRequests","ordinal":ordinal,"mode":"hold","holdSeconds":30}})).await;
    driver.record("armed.json", &arm);
    assert!(arm.get("error").is_none(), "{arm}");
    let execute = wire.send("accept-changes.execute",json!({"workspaceId":workspace,"action":"create-pr","review":{"root":operation["root"],"operationId":operation["operationId"]},"prTitle":"existing ready MR","prBody":"fixture"})).await;
    let state = {
        let arrived = timeout(Duration::from_secs(10), async {
            loop {
                let state = driver
                    .control(json!({"command":"barrierStatus","host":0}))
                    .await;
                if state["result"]["barriers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|b| b["entered"] == true)
                {
                    break state;
                }
                // timing-guard: actual owned authenticated provider request entry
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        tokio::pin!(arrived);
        tokio::select! {
            response = wire.response(execute) => { driver.record("unexpected-execute.json",&response); let effects=driver.control(json!({"command":"snapshot","host":0})).await;driver.record("unexpected-effects.json",&effects); panic!("original request did not reach admitted barrier: {response}"); },
            state = &mut arrived => state.unwrap(),
        }
    };
    driver.record("entered.json", &state);
    assert_eq!(state["result"]["active"], 1);
    let claim = json!({"workspaceId":workspace,"root":operation["root"],"operationId":operation["operationId"]});
    let released = wire.call("accept-changes.release", claim.clone()).await;
    driver.record("release.json", &released);
    assert_eq!(success(&released)["released"], true);
    let pending = json!({"host":0,"operationId":operation["operationId"],"socketId":wire.socket_id,"requestId":execute});
    let begin = driver
        .control(json!({"command":"stop","phase":"begin","pending":[pending],"envelopes":[]}))
        .await;
    driver.record("begin-response.json", &begin);
    assert!(begin.get("error").is_none(), "{begin}");
    let execution = wire.response(execute).await;
    let history = wire.call("accept-changes.reconcile", claim).await;
    let effects = driver.control(json!({"command":"snapshot","host":0})).await;
    driver.record(
        "settlement.json",
        &json!({"execution":execution,"history":history,"effects":effects}),
    );
    assert_eq!(
        success(&execution)["reviewExecution"]["outcome"]["status"],
        "reused",
        "{execution}"
    );
    assert_eq!(
        success(&history)["reviewExecution"],
        success(&execution)["reviewExecution"]
    );
    assert!(
        success(&execution)["reviewExecution"]["preparation"]["source"]
            .get("connection")
            .is_none()
    );
    assert!(
        success(&execution)["reviewExecution"]["preparation"]["target"]
            .get("connection")
            .is_none()
    );
    assert_eq!(effects["result"]["effects"]["posts"], 0);
    assert_eq!(effects["result"]["effects"]["pushes"], 0);
    wire.close().await;
    driver
        .finish(vec![
            json!({"request":pending,"originalResponse":execution,"history":history}),
        ])
        .await;
}

// This module is confined to the separately invoked fixture executable. No
// production process, general test runner or shared child helper is reconfigured.
#[cfg(target_os = "linux")]
mod driver_ownership {
    use super::*;
    use nix::errno::Errno;
    use nix::sys::wait::{waitid, Id, WaitPidFlag, WaitStatus};
    use nix::unistd::Pid;
    use std::collections::BTreeSet;
    use std::io::Read as _;
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::process::CommandExt as _;
    use std::process::{ChildStdin, Command, ExitStatus};
    use std::time::Instant;

    const ENTRY: &str = "native_review_fixture_driver";
    const OBSERVE: WaitPidFlag =
        WaitPidFlag::from_bits_retain(libc::WEXITED | libc::WNOHANG | libc::WNOWAIT);

    struct Capability<T>(Option<T>);
    impl<T> Capability<T> {
        fn apply<U>(&self, f: impl FnOnce(&T) -> DriverResult<U>) -> DriverResult<U> {
            f(self
                .0
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("allocation already reaped"))?)
        }
        fn retire(&mut self) {
            self.0.take();
        }
    }

    fn pidfd(pid: Pid) -> DriverResult<OwnedFd> {
        // SAFETY: pidfd_open takes only scalar arguments; the returned fd is new.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid.as_raw(), 0) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: the successful syscall transferred this unique fd to us.
        Ok(unsafe { OwnedFd::from_raw_fd(i32::try_from(fd)?) })
    }

    fn signal(fd: &OwnedFd, value: i32) -> DriverResult<()> {
        // SAFETY: an owned open pidfd pins the allocation; null siginfo and
        // flags=0 mean an individual process, never a numeric process group.
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                fd.as_raw_fd(),
                value,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }

    fn terminal(status: WaitStatus) -> bool {
        matches!(status, WaitStatus::Exited(..) | WaitStatus::Signaled(..))
    }

    struct Allocation {
        pid: Pid,
        capability: Capability<OwnedFd>,
        // raw-child: allow — sealed pidfd owner; only waitid below consumes this child
        child: Option<std::process::Child>,
        status: Option<WaitStatus>,
    }
    impl Allocation {
        fn adopt(pid: Pid) -> DriverResult<Self> {
            // Sole reaper + normal SIGCHLD keeps this child allocation waitable
            // until enrollment, even if it exits between these two syscalls.
            waitid(Id::Pid(pid), OBSERVE)?;
            Ok(Self {
                pid,
                capability: Capability(Some(pidfd(pid)?)),
                child: None,
                status: None,
            })
        }
        fn spawn(command: &mut Command) -> DriverResult<Self> {
            // A fresh worker must wait on stdin for R before launching children.
            // On enrollment failure EOF prevents bootstrap; retain and wait its
            // direct Child, with no numeric signal fallback.
            let mut child = command.stdin(Stdio::piped()).spawn()?;
            let Some(mut gate) = child.stdin.take() else {
                let waited = child.wait();
                anyhow::bail!("missing bootstrap pipe; direct wait {waited:?}");
            };
            let enrolled = Self::adopt(Pid::from_raw(child.id().cast_signed()));
            let mut owned = match enrolled {
                Ok(owned) => owned,
                Err(error) => {
                    drop(gate);
                    let waited = child.wait();
                    return Err(error.context(format!("bootstrap refused; direct wait {waited:?}")));
                }
            };
            owned.child = Some(child);
            gate.write_all(b"R")?;
            drop(gate);
            Ok(owned)
        }
        fn observe(&self) -> DriverResult<WaitStatus> {
            self.capability.apply(|fd| {
                Ok(waitid(
                    Id::PIDFd(fd.as_fd()),
                    OBSERVE | WaitPidFlag::WSTOPPED,
                )?)
            })
        }
        fn exit_ready(&self) -> DriverResult<bool> {
            self.capability.apply(|fd| {
                let mut poll = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: this borrowed pidfd remains open throughout the poll.
                let result = unsafe { libc::poll(&raw mut poll, 1, 0) };
                anyhow::ensure!(result >= 0, "pidfd poll failed");
                Ok(result > 0)
            })
        }
        fn send(&self, value: i32) -> DriverResult<()> {
            self.capability.apply(|fd| signal(fd, value))
        }
        fn reap(&mut self) -> DriverResult<Option<WaitStatus>> {
            if let Some(status) = self.status {
                return Ok(Some(status));
            }
            let status = self.capability.apply(|fd| {
                Ok(waitid(
                    Id::PIDFd(fd.as_fd()),
                    WaitPidFlag::WEXITED | WaitPidFlag::WNOHANG,
                )?)
            })?;
            if terminal(status) {
                self.status = Some(status);
                self.capability.retire();
                self.child.take();
                Ok(Some(status))
            } else {
                Ok(None)
            }
        }
        fn finish(&mut self) -> DriverResult<WaitStatus> {
            if self.status.is_none() {
                self.send(libc::SIGKILL)?;
            }
            loop {
                if let Some(status) = self.reap()? {
                    return Ok(status);
                }
                // timing-guard: retain the pidfd until actual waitid completion
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    impl Drop for Allocation {
        fn drop(&mut self) {
            if self.status.is_none() {
                // Never drop an enrolled live allocation because an observation
                // failed. An unkillable kernel task leaves this owner incomplete.
                while let Err(error) = self.finish() {
                    eprintln!("owned allocation cleanup incomplete: {error}");
                    // timing-guard: retry owned cleanup, never a native operation
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    pub(super) struct Controller {
        // raw-child: allow — lifetime-pipe owner only waits its direct supervisor, never signals PIDs
        child: std::process::Child,
        lifetime: Option<ChildStdin>,
    }
    impl Controller {
        pub(super) fn spawn(command: &mut Command) -> DriverResult<Self> {
            // The supervisor survives loss of the controller's process group.
            let mut child = command.process_group(0).stdin(Stdio::piped()).spawn()?;
            let lifetime = child.stdin.take();
            let mut owned = Self { child, lifetime };
            owned
                .lifetime
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("lifetime pipe"))?
                .write_all(b"R")?;
            Ok(owned)
        }
        pub(super) fn id(&self) -> u32 {
            self.child.id()
        }
        pub(super) fn poll(&mut self) -> std::io::Result<Option<ExitStatus>> {
            self.child.try_wait()
        }
        fn close(&mut self) {
            self.lifetime.take();
        }
        fn wait(&mut self) -> std::io::Result<ExitStatus> {
            self.child.wait()
        }
    }
    impl Drop for Controller {
        fn drop(&mut self) {
            self.close();
            // No timeout-triggered kill of the supervisor: it retains its
            // descendants until actual cleanup, including kernel-stalled cases.
            if let Err(error) = self.child.wait() {
                eprintln!("supervisor wait incomplete: {error}");
            }
        }
    }

    fn children() -> DriverResult<BTreeSet<Pid>> {
        let mut found = BTreeSet::new();
        // Only this isolated process's threads, never a global PID/argv search.
        for task in std::fs::read_dir("/proc/self/task")? {
            let file = task?.path().join("children");
            match std::fs::read_to_string(file) {
                Ok(text) => {
                    for value in text.split_whitespace() {
                        found.insert(Pid::from_raw(value.parse()?));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(found)
    }

    fn prerequisite_rules(
        default_sigchld: bool,
        auto_reap: bool,
        supported: bool,
    ) -> DriverResult<()> {
        anyhow::ensure!(
            default_sigchld && !auto_reap && supported,
            "owned-child prerequisites unavailable"
        );
        Ok(())
    }
    fn preflight() -> DriverResult<()> {
        let expected = [
            "--ignored",
            "--exact",
            ENTRY,
            "--nocapture",
            "--test-threads=1",
        ];
        anyhow::ensure!(
            std::env::args().skip(1).eq(expected),
            "supervisor requires its exact isolated invocation"
        );
        // SAFETY: valid writable sigaction storage; null new action is read-only.
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        let read = unsafe { libc::sigaction(libc::SIGCHLD, std::ptr::null(), &raw mut action) };
        anyhow::ensure!(read == 0, "SIGCHLD inspection failed");
        prerequisite_rules(
            action.sa_sigaction == libc::SIG_DFL,
            action.sa_flags & libc::SA_NOCLDWAIT != 0,
            true,
        )?;
        anyhow::ensure!(children()?.is_empty(), "supervisor already has children");
        anyhow::ensure!(
            matches!(waitid(Id::All, OBSERVE), Err(Errno::ECHILD)),
            "competing child owner"
        );
        let own = pidfd(Pid::this())?;
        signal(&own, 0)?;
        anyhow::ensure!(
            matches!(waitid(Id::PIDFd(own.as_fd()), OBSERVE), Err(Errno::ECHILD)),
            "pidfd wait support"
        );
        nix::sys::prctl::set_child_subreaper(true)?;
        anyhow::ensure!(
            nix::sys::prctl::get_child_subreaper()?,
            "subreaper unavailable"
        );
        Ok(())
    }

    fn lifetime_closed() -> DriverResult<bool> {
        let mut poll = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: one initialized pollfd; zero timeout never blocks cleanup.
        let result = unsafe { libc::poll(&raw mut poll, 1, 0) };
        anyhow::ensure!(result >= 0, "lifetime poll failed");
        if result == 0 {
            return Ok(false);
        }
        let mut byte = [0];
        anyhow::ensure!(
            std::io::stdin().read(&mut byte)? == 0,
            "unexpected lifetime data"
        );
        Ok(true)
    }

    fn controller_gate() -> DriverResult<()> {
        use std::os::unix::fs::FileTypeExt as _;
        let metadata = std::fs::metadata("/proc/self/fd/0")?;
        anyhow::ensure!(
            metadata.file_type().is_fifo(),
            "controller stdin is not a lifetime pipe"
        );
        let mut poll = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        // SAFETY: one valid pollfd, bounded pre-child enrollment handshake.
        let result = unsafe { libc::poll(&raw mut poll, 1, 5000) };
        anyhow::ensure!(result > 0, "controller bootstrap unavailable");
        let mut gate = [0];
        std::io::stdin().read_exact(&mut gate)?;
        anyhow::ensure!(gate == *b"R", "controller lifetime pipe missing");
        Ok(())
    }

    struct Tree {
        worker: Allocation,
        journal: std::fs::File,
        events: Vec<Value>,
        complete: bool,
        failed: bool,
        error: Option<String>,
    }
    impl Tree {
        fn record(&mut self, value: Value) {
            if self.events.len() >= 2048 {
                self.failed = true;
                self.error
                    .get_or_insert_with(|| "ownership journal overflow".into());
                return;
            }
            if writeln!(self.journal, "{value}")
                .and_then(|()| self.journal.sync_all())
                .is_err()
            {
                self.failed = true;
                self.error
                    .get_or_insert_with(|| "ownership journal I/O failed".into());
            }
            self.events.push(value);
        }
        fn drain(&mut self, failure: bool) -> DriverResult<Value> {
            self.failed |= failure;
            if self.worker.status.is_none() {
                let observed = self.worker.observe()?;
                if !terminal(observed) {
                    self.failed = true;
                    self.worker.send(libc::SIGKILL)?;
                    self.record(json!({"kind":"signal","pid":self.worker.pid.as_raw(),"via":"pidfd","flags":0}));
                }
                let worker_status = loop {
                    if let Some(status) = self.worker.reap()? {
                        break status;
                    }
                    // timing-guard: original worker exit and adoption precede descendant cleanup
                    std::thread::sleep(Duration::from_millis(10));
                };
                self.record(json!({"kind":"wait","role":"worker","pid":self.worker.pid.as_raw(),"status":format!("{worker_status:?}")}));
                self.failed |= !matches!(worker_status, WaitStatus::Exited(_, 0));
            }
            loop {
                let mut candidates = children()?;
                match waitid(Id::All, OBSERVE) {
                    Err(Errno::ECHILD) => {
                        self.complete = true;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                    Ok(status) => {
                        if let Some(pid) = status.pid() {
                            candidates.insert(pid);
                        }
                    }
                }
                for pid in candidates {
                    let mut owned = Allocation::adopt(pid)?;
                    self.failed = true; // An unexpected remaining descendant is not graceful stop.
                    self.record(json!({"kind":"adopt","pid":pid.as_raw(),"capability":"waitable-child+pidfd"}));
                    if !terminal(owned.observe()?) {
                        owned.send(libc::SIGKILL)?;
                        self.record(
                            json!({"kind":"signal","pid":pid.as_raw(),"via":"pidfd","flags":0}),
                        );
                    }
                    let status = loop {
                        if let Some(status) = owned.reap()? {
                            break status;
                        }
                        // timing-guard: observe and reap this exact adopted allocation
                        std::thread::sleep(Duration::from_millis(10));
                    };
                    self.record(json!({"kind":"wait","role":"adopted","pid":pid.as_raw(),"status":format!("{status:?}")}));
                }
                // timing-guard: child-list reads are discovery, only ECHILD proves completion
                std::thread::sleep(Duration::from_millis(10));
            }
            self.record(json!({"kind":"complete","waitResult":"ECHILD"}));
            Ok(
                json!({"complete":self.complete,"failed":self.failed,"error":self.error,"events":self.events}),
            )
        }
    }
    impl Drop for Tree {
        fn drop(&mut self) {
            while !self.complete {
                if let Err(error) = self.drain(true) {
                    self.failed = true;
                    self.error.get_or_insert_with(|| error.to_string());
                    eprintln!("fixture tree cleanup incomplete: {error}");
                    // timing-guard: retain sole ownership through cleanup errors
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    pub(super) fn entry() -> DriverResult<()> {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let path = PathBuf::from(
            std::env::var_os("INTENT_REVIEW_DRIVER_DESCRIPTOR")
                .ok_or_else(|| anyhow::anyhow!("owned descriptor"))?,
        );
        if std::env::var_os("INTENT_REVIEW_DRIVER_CHILD").is_some() {
            let mut gate = [0];
            std::io::stdin().read_exact(&mut gate)?;
            anyhow::ensure!(gate == *b"R", "worker not enrolled");
            let descriptor = driver_descriptor_phase(&path, true, true, false)?;
            if let Ok(case) = std::env::var("INTENT_REVIEW_DRIVER_INERT") {
                return inert_worker(&descriptor.directory, &case);
            }
            let tls = descriptor.directory.join("tls");
            std::fs::create_dir(&tls)?;
            std::fs::set_permissions(&tls, std::fs::Permissions::from_mode(0o700))?;
            make_fixture_certificate(&tls);
            let descriptor = driver_descriptor(&path, true)?;
            // companion-observation: begin worker collector
            let _companion_observer = CompanionFixtureObservation::install(&descriptor.directory);
            // companion-observation: end worker collector
            return tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(driver_loop(&descriptor));
        }
        preflight()?;
        let started = Instant::now();
        controller_gate()?;
        let descriptor = driver_descriptor_phase(&path, false, false, false)?;
        let journal = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(descriptor.directory.join("ownership.jsonl"))?;
        private_json(
            &descriptor.directory.join("supervisor.json"),
            &json!({"pid":std::process::id(),"subreaper":true,"runId":descriptor.run_id,"signals":"individual pidfd flags0"}),
        )?;
        let tls = descriptor.directory.join("tls");
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--ignored",
                "--exact",
                ENTRY,
                "--nocapture",
                "--test-threads=1",
            ])
            .env("INTENT_REVIEW_DRIVER_CHILD", "1")
            .env("INTENT_REVIEW_TLS_FIXTURE", &tls)
            .env("SSL_CERT_FILE", tls.join("ca.pem"))
            .env("INTENTD_TEST_KEEP_TMP", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let worker = Allocation::spawn(&mut command)?;
        let mut tree = Tree {
            worker,
            journal,
            events: Vec::new(),
            complete: false,
            failed: false,
            error: None,
        };
        tree.record(json!({"kind":"enrolled","role":"worker","pid":tree.worker.pid.as_raw()}));
        let watching = (|| -> DriverResult<Option<String>> {
            private_json(
                &descriptor.directory.join("worker.json"),
                &json!({"pid":tree.worker.pid.as_raw(),"owner":"supervisor","allocation":"pidfd"}),
            )?;
            let exited_control =
                std::env::var("INTENT_REVIEW_DRIVER_INERT").as_deref() == Ok("exited-worker");
            let mut observed_stop = false;
            let mut first_status = true;
            loop {
                if lifetime_closed()? {
                    return Ok(Some("controller lifetime ended".into()));
                }
                if started.elapsed() >= Duration::from_secs(descriptor.lifetime_seconds) {
                    return Ok(Some("driver deadline".into()));
                }
                // The inert exited-worker control makes the original allocation
                // exit before the first wait-status observation. Poll is not reap.
                if !exited_control || tree.worker.exit_ready()? {
                    let status = tree.worker.observe()?;
                    if first_status {
                        tree.record(json!({"kind":"first-status","status":format!("{status:?}")}));
                        first_status = false;
                    }
                    if matches!(status, WaitStatus::Stopped(..)) && !observed_stop {
                        tree.record(
                            json!({"kind":"worker-stopped","status":format!("{status:?}")}),
                        );
                        observed_stop = true;
                    }
                    if terminal(status) {
                        return Ok(None);
                    }
                }
                // timing-guard: original allocation/lifetime/deadline observations
                std::thread::sleep(Duration::from_millis(10));
            }
        })();
        let reason = watching
            .unwrap_or_else(|error| Some(format!("supervisor observation failed: {error}")));
        let cleanup = tree.drain(reason.is_some())?;
        let intermediate = std::fs::read(descriptor.directory.join("worker-stopped.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let successful = !tree.failed
            && intermediate
                .as_ref()
                .is_some_and(|value| value["success"] == true);
        let result = json!({"version":1,"runId":descriptor.run_id,"supervisorPid":std::process::id(),
            "success":successful,"reason":reason,"ownership":cleanup,"worker":intermediate,
            "nativeCompletion":if successful {"original controller envelopes in worker record"} else {"not established by cleanup"}});
        publish_driver(
            &descriptor.directory.join(if successful {
                "stopped.json"
            } else {
                "failed.json"
            }),
            &result,
        )?;
        anyhow::ensure!(
            successful,
            "fixture supervisor retained a failed outcome: {reason:?}"
        );
        Ok(())
    }

    const INERT_PROVIDER: &str = r"
import json,os,pathlib,signal,subprocess,sys
root=pathlib.Path(sys.argv[1]);role=sys.argv[2]
if role=='provider-a':
    subprocess.Popen([sys.executable,__file__,str(root),'grandchild'],start_new_session=True)
(root/(role+'.json')).write_text(json.dumps({'pid':os.getpid(),'pgid':os.getpgrp(),'role':role}))
while True:signal.pause()
";
    fn inert_worker(root: &std::path::Path, case: &str) -> DriverResult<()> {
        anyhow::ensure!(
            matches!(case, "stopped-worker" | "exited-worker" | "controller-loss"),
            "inert case"
        );
        let script = root.join("inert.py");
        std::fs::write(&script, INERT_PROVIDER)?;
        let mut providers = Vec::new();
        for role in ["provider-a", "provider-b"] {
            let mut command = Command::new("python3");
            command
                .arg(&script)
                .arg(root)
                .arg(role)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            providers.push(GuardedChild::spawn(&mut command)?);
        }
        let started = Instant::now();
        while !["provider-a", "provider-b", "grandchild"]
            .iter()
            .all(|name| root.join(format!("{name}.json")).exists())
        {
            anyhow::ensure!(
                started.elapsed() < Duration::from_secs(10),
                "inert children startup"
            );
            // timing-guard: all original inert descendants acknowledge before fault injection
            std::thread::sleep(Duration::from_millis(10));
        }
        private_json(
            &root.join("inert-entered.json"),
            &json!({"worker":std::process::id(),"case":case,"children":3}),
        )?;
        if case == "exited-worker" {
            std::process::exit(17);
        }
        if case == "stopped-worker" {
            // SAFETY: stop only this executing inert worker, never a looked-up PID.
            unsafe {
                libc::raise(libc::SIGSTOP);
            }
        }
        loop {
            std::thread::park();
        }
    }

    fn evidence_directory() -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = common::test_tempdir_in("/tmp", "itd-cleanup-");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }
    fn observed_json(path: &std::path::Path) -> Value {
        let started = Instant::now();
        loop {
            if let Ok(bytes) = std::fs::read(path) {
                if let Ok(value) = serde_json::from_slice(&bytes) {
                    return value;
                }
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "missing owned observation {}",
                path.display()
            );
            // timing-guard: wait for the original owned process's acknowledgment
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn inert_command(directory: &std::path::Path, case: &str) -> Command {
        use std::os::unix::fs::OpenOptionsExt as _;
        let identity = driver_identity();
        let descriptor = json!({"version":1,"runId":uuid::Uuid::new_v4().to_string(),
            "sourceCommit":identity["sourceCommit"],"sourceTree":identity["sourceTree"],
            "sourceSha256":identity["sourceSha256"],"executableSha256":identity["executableSha256"],
            "lifetimeSeconds":30,"scenarios":["ready-stop"]});
        private_json(&directory.join("descriptor.json"), &descriptor).unwrap();
        let log = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(directory.join("child.log"))
            .unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                ENTRY,
                "--nocapture",
                "--test-threads=1",
            ])
            .env(
                "INTENT_REVIEW_DRIVER_DESCRIPTOR",
                directory.join("descriptor.json"),
            )
            .env("INTENT_REVIEW_DRIVER_INERT", case)
            .env_remove("INTENT_REVIEW_DRIVER_CHILD")
            .env("INTENTD_TEST_KEEP_TMP", "1")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        command
    }
    fn assert_inert_cleanup(directory: &std::path::Path, supervisor: &mut Controller) -> Value {
        let status = supervisor.wait().unwrap();
        let record = observed_json(&directory.join("failed.json"));
        private_json(
            &directory.join("outer-supervisor-wait.json"),
            &json!({"pid":supervisor.id(),"status":status.to_string(),"success":status.success()}),
        )
        .unwrap();
        assert!(!status.success());
        assert_eq!(record["success"], false);
        assert_eq!(record["ownership"]["complete"], true);
        assert_eq!(record["ownership"]["failed"], true);
        assert!(!directory.join("ready.json").exists());
        assert!(!directory.join("stopped.json").exists());
        let events = record["ownership"]["events"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["waitResult"], "ECHILD");
        assert_eq!(
            events
                .iter()
                .filter(|e| e["kind"] == "wait" && e["role"] == "worker")
                .count(),
            1
        );
        let mut groups = BTreeSet::new();
        for role in ["provider-a", "provider-b", "grandchild"] {
            let ack = observed_json(&directory.join(format!("{role}.json")));
            groups.insert(ack["pgid"].as_u64().unwrap());
            assert_eq!(ack["pgid"], ack["pid"], "separate original group");
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e["kind"] == "adopt" && e["pid"] == ack["pid"])
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e["kind"] == "wait" && e["pid"] == ack["pid"])
                    .count(),
                1
            );
        }
        assert_eq!(groups.len(), 3);
        assert!(events
            .iter()
            .filter(|e| e["kind"] == "signal")
            .all(|e| e["via"] == "pidfd" && e["flags"] == 0));
        record
    }

    #[test]
    fn native_review_cleanup_stopped_worker_deadline() {
        let directory = evidence_directory();
        let mut supervisor =
            Controller::spawn(&mut inert_command(directory.path(), "stopped-worker")).unwrap();
        let entered = observed_json(&directory.path().join("inert-entered.json"));
        assert_eq!(entered["children"], 3);
        let record = assert_inert_cleanup(directory.path(), &mut supervisor);
        assert_eq!(record["reason"], "driver deadline");
        assert!(record["ownership"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "worker-stopped"
                && e["status"].as_str().unwrap().contains("SIGSTOP")));
    }

    #[test]
    fn native_review_cleanup_exited_worker_adoption() {
        let directory = evidence_directory();
        let mut supervisor =
            Controller::spawn(&mut inert_command(directory.path(), "exited-worker")).unwrap();
        observed_json(&directory.path().join("inert-entered.json"));
        let record = assert_inert_cleanup(directory.path(), &mut supervisor);
        let first = record["ownership"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["kind"] == "first-status")
            .unwrap();
        assert!(first["status"].as_str().unwrap().ends_with(", 17)"));
        assert!(record["reason"].is_null());
    }

    #[test]
    fn native_review_cleanup_controller_loss_before_ready() {
        let directory = evidence_directory();
        let mut supervisor =
            Controller::spawn(&mut inert_command(directory.path(), "controller-loss")).unwrap();
        observed_json(&directory.path().join("inert-entered.json"));
        assert!(!directory.path().join("ready.json").exists());
        // Transfer the sole writer to an owned inert controller; the outer test
        // retains the supervisor Child, not a second writer or signal authority.
        let pipe: OwnedFd = supervisor.lifetime.take().unwrap().into();
        let mut command = Command::new("python3");
        command.args(["-c", "import os,pathlib,signal,sys;assert os.read(0,1)==b'R';pathlib.Path(sys.argv[1]).write_text('ready');signal.pause()"])
            .arg(directory.path().join("writer-ready")).stdout(Stdio::from(pipe)).stderr(Stdio::inherit());
        let mut writer = Allocation::spawn(&mut command).unwrap();
        drop(command); // Command's Stdio must not retain a copy of the writer.
        let started = Instant::now();
        while !directory.path().join("writer-ready").exists() {
            assert!(started.elapsed() < Duration::from_secs(10));
            // timing-guard: writer transfer is acknowledged before its owned death
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = writer.finish().unwrap();
        private_json(&directory.path().join("outer-controller-wait.json"), &json!({"pid":writer.pid.as_raw(),"status":format!("{status:?}"),"via":"pidfd","writerCopiesInOuter":0})).unwrap();
        assert!(matches!(
            status,
            WaitStatus::Signaled(_, nix::sys::signal::Signal::SIGKILL, _)
        ));
        let record = assert_inert_cleanup(directory.path(), &mut supervisor);
        assert_eq!(record["reason"], "controller lifetime ended");
    }

    const NONCE_CHILD: &str = r"
import json,os,pathlib,sys,time
assert os.read(0,1)==b'R'
root=pathlib.Path(sys.argv[1]);mode=sys.argv[2]
(root/(mode+'-ready')).write_text(str(os.getpid()))
while True:
    if mode=='exit' and (root/'exit-release').exists():sys.exit(23)
    if mode=='sentinel' and (root/'nonce').exists():
        nonce=(root/'nonce').read_text();(root/'response').write_text(nonce)
    time.sleep(.005)
";
    fn nonce_child(directory: &std::path::Path, mode: &str) -> Allocation {
        let mut command = Command::new("python3");
        command
            .args(["-c", NONCE_CHILD])
            .arg(directory)
            .arg(mode)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        Allocation::spawn(&mut command).unwrap()
    }
    fn nonce(directory: &std::path::Path) -> String {
        let challenge = uuid::Uuid::new_v4().to_string();
        std::fs::write(directory.join("nonce"), &challenge).unwrap();
        let started = Instant::now();
        loop {
            if std::fs::read_to_string(directory.join("response"))
                .is_ok_and(|value| value == challenge)
            {
                return challenge;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "independent sentinel response"
            );
            // timing-guard: original sentinel must answer this fresh nonce
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn native_review_cleanup_pidfd_and_reuse() {
        let directory = evidence_directory();
        let mut sentinel = nonce_child(directory.path(), "sentinel");
        let before = nonce(directory.path());
        // This supervisor owns a different subtree; the sentinel is a sibling.
        let mut supervisor =
            Controller::spawn(&mut inert_command(directory.path(), "exited-worker")).unwrap();
        observed_json(&directory.path().join("inert-entered.json"));
        assert_inert_cleanup(directory.path(), &mut supervisor);
        let after = nonce(directory.path());
        let mut allocation = nonce_child(directory.path(), "exit");
        let alive = allocation.observe().unwrap();
        assert!(!terminal(alive));
        std::fs::write(directory.path().join("exit-release"), "release").unwrap();
        let started = Instant::now();
        while !allocation.exit_ready().unwrap() {
            assert!(started.elapsed() < Duration::from_secs(10));
            // timing-guard: nonconsuming original allocation exit observation
            std::thread::sleep(Duration::from_millis(10));
        }
        let exited = allocation.observe().unwrap();
        assert!(matches!(exited, WaitStatus::Exited(_, 23)));
        allocation.send(libc::SIGKILL).unwrap(); // Exit happened after the live observation.
        let consumed = allocation.reap().unwrap().unwrap();
        assert_eq!(consumed, exited);
        assert!(allocation.send(0).is_err());
        assert!(allocation.observe().is_err());
        assert_eq!(allocation.reap().unwrap(), Some(consumed)); // Cached, no second syscall.
        assert_eq!(waitid(Id::Pid(allocation.pid), OBSERVE), Err(Errno::ECHILD));
        // Instrumented identities, not host PID/FD churn. A stale capability
        // cannot call the backend for either old or numerically reused identity.
        let mut capability = Capability(Some((41_u32, 9_u32, "original")));
        let mut calls = Vec::new();
        capability
            .apply(|identity| {
                calls.push(*identity);
                Ok(())
            })
            .unwrap();
        capability.retire();
        let replacement = (41_u32, 9_u32, "replacement");
        assert!(capability
            .apply(|_| {
                calls.push(replacement);
                Ok(())
            })
            .is_err());
        assert_eq!(calls, vec![(41, 9, "original")]);
        let still_alive = nonce(directory.path());
        let sentinel_status = sentinel.finish().unwrap();
        private_json(&directory.path().join("allocation-proof.json"), &json!({
            "actual":{"live":format!("{alive:?}"),"exit":format!("{exited:?}"),"consumed":format!("{consumed:?}"),"postReap":"ECHILD"},
            "instrumentedOnly":{"old":[41,9,"original"],"reused":[41,9,"replacement"],"backendCalls":calls},
            "sentinel":{"before":before,"afterSubtree":after,"afterStale":still_alive,"pid":sentinel.pid.as_raw(),"wait":format!("{sentinel_status:?}")}
        })).unwrap();
        assert!(matches!(
            sentinel_status,
            WaitStatus::Signaled(_, nix::sys::signal::Signal::SIGKILL, _)
        ));
    }

    #[test]
    fn native_review_cleanup_preflight_refuses() {
        let directory = evidence_directory();
        assert!(prerequisite_rules(true, false, true).is_ok());
        assert!(prerequisite_rules(false, false, true).is_err());
        assert!(prerequisite_rules(true, true, true).is_err());
        assert!(prerequisite_rules(true, false, false).is_err());
        let mut command = inert_command(directory.path(), "controller-loss");
        // SAFETY: only the separately exec'd exact-entry child changes SIGCHLD;
        // signal is async-signal-safe, and no Rust allocation occurs here.
        unsafe {
            command.pre_exec(|| {
                if libc::signal(libc::SIGCHLD, libc::SIG_IGN) == libc::SIG_ERR {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let status = command.stdin(Stdio::null()).status().unwrap();
        private_json(&directory.path().join("preflight-proof.json"), &json!({"actualAutoReapExit":status.to_string(),"instrumentedUnsupported":true,"launchedWorker":false})).unwrap();
        assert!(!status.success());
        for name in [
            "worker.json",
            "supervisor.json",
            "inert-entered.json",
            "ready.json",
            "stopped.json",
        ] {
            assert!(!directory.path().join(name).exists());
        }
        let log = std::fs::read_to_string(directory.path().join("child.log")).unwrap();
        assert!(
            log.contains("owned-child prerequisites unavailable"),
            "{log}"
        );
    }
}

// These prerequisite checks follow the same public workspace/context/prepare
// sequence as a client. They never execute a review stage.
async fn prerequisite_harness() -> (tempfile::TempDir, Harness) {
    let directory = common::test_tempdir_in("/tmp", "itd-target-prerequisite-");
    let identity = driver_identity();
    let descriptor = DriverDescriptor {
        version: 1,
        run_id: uuid::Uuid::new_v4().to_string(),
        source_commit: identity["sourceCommit"].as_str().unwrap().into(),
        source_tree: identity["sourceTree"].as_str().unwrap().into(),
        source_sha256: identity["sourceSha256"].as_str().unwrap().into(),
        executable_sha256: identity["executableSha256"].as_str().unwrap().into(),
        lifetime_seconds: 90,
        scenarios: vec!["ready-stop".into()],
        directory: directory.path().to_owned(),
    };
    let harness = Harness::boot_driver(None, true, Some(&descriptor)).await;
    (directory, harness)
}

async fn prerequisite_snapshot(h: &Harness) -> Value {
    let provider = timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = driver_provider(h, json!({"command":"snapshot"}))
                .await
                .unwrap();
            if snapshot["active"] == 0 {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned provider requests finish");
    driver_snapshot(h, &provider)
}

fn prerequisite_unchanged(before: &Value, after: &Value) {
    for key in [
        "primaryHead",
        "registeredHead",
        "remoteHead",
        "index",
        "worktree",
        "status",
        "effects",
    ] {
        assert_eq!(before[key], after[key], "prerequisite changed {key}");
    }
    assert_eq!(after["provider"]["active"], 0);
    assert_eq!(after["provider"]["overflow"], false);
    assert!(after["provider"]["failure"].is_null());
}

async fn prerequisite_context(h: &Harness, client: &mut Client) -> (Value, Value) {
    let capture = client
        .rpc(
            "workspace.repositoryContext.capture",
            json!({"workspaceId":h.workspace}),
        )
        .await;
    eprintln!(
        "original context capture: {}",
        json!({"capture":capture,"facts":prerequisite_snapshot(h).await})
    );
    let lifetime = success(&capture)["lifetimeId"].clone();
    let context = client
        .rpc(
            "workspace.repositoryContext",
            json!({"workspaceId":h.workspace,"repositoryLifetimeId":lifetime}),
        )
        .await;
    eprintln!("original context read: {context}");
    (capture, context)
}

fn prerequisite_saved(h: &Harness, target: &Value) -> Value {
    json!({"workspaceId":h.workspace,"action":"create-pr","review":{"root":{"workspaceId":h.workspace,"kind":"primary"},"choice":{"kind":"saved"},"targetBranch":target}})
}

fn prerequisite_workspace<'a>(h: &Harness, frame: &'a Value) -> &'a Value {
    success(frame)["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == h.workspace.as_str())
        .unwrap()
}

#[intent_test_macros::daemon_test]
async fn native_review_target_prerequisite_owner_uds() {
    if run_in_tls_process("native_review_target_prerequisite_owner_uds").await {
        return;
    }
    let (_directory, h) = prerequisite_harness().await;
    let mut client = h.uds().await;
    let stored = h.store.get_workspace(&h.workspace).await.unwrap();
    let list = client.rpc("workspace.list", json!({})).await;
    let before = prerequisite_snapshot(&h).await;
    let local_trunk = git(&h.root, &["rev-parse", "refs/heads/trunk"]);
    let remote_trunk = git(
        &h.dir.path().join("remote/group/project.git"),
        &["rev-parse", "refs/heads/trunk"],
    );
    eprintln!(
        "owner prerequisite initial: {}",
        json!({"list":list,"storedBaseRef":stored.base_ref,"storedBaseCommitSha":stored.base_commit_sha,"localTrunk":local_trunk,"remoteTrunk":remote_trunk,"facts":before})
    );
    assert_eq!(stored.base_ref.as_deref(), Some("trunk"));
    assert!(stored.base_commit_sha.is_none());
    assert_eq!(local_trunk, remote_trunk);
    assert_eq!(local_trunk, before["primaryHead"]);
    let base = prerequisite_workspace(&h, &list)["baseRef"].clone();
    assert_eq!(base, "trunk");
    let (capture, context) = prerequisite_context(&h, &mut client).await;
    let after_context = prerequisite_snapshot(&h).await;
    eprintln!("owner context facts: {after_context}");
    prerequisite_unchanged(&before, &after_context);
    assert_eq!(
        before["provider"]["counts"],
        after_context["provider"]["counts"]
    );
    let roots = success(&context)["roots"].as_array().unwrap();
    assert!(roots
        .iter()
        .all(|r| !r["targets"].as_array().unwrap().is_empty()));
    let mut omitted = prerequisite_saved(&h, &base);
    omitted["review"]
        .as_object_mut()
        .unwrap()
        .remove("targetBranch");
    let missing = client.rpc("accept-changes.prepare", omitted).await;
    let same = client
        .rpc(
            "accept-changes.prepare",
            prerequisite_saved(&h, &json!("main")),
        )
        .await;
    let after_refusals = prerequisite_snapshot(&h).await;
    eprintln!(
        "owner refusal envelopes and facts: {}",
        json!({"missing":missing,"same":same,"facts":after_refusals})
    );
    for refused in [&missing, &same] {
        assert_eq!(refused["error"]["code"], -32602);
        assert!(refused.get("result").is_none());
    }
    prerequisite_unchanged(&before, &after_refusals);
    let prepared = client
        .rpc("accept-changes.prepare", prerequisite_saved(&h, &base))
        .await;
    let after_prepare = prerequisite_snapshot(&h).await;
    eprintln!(
        "owner saved preparation and facts: {}",
        json!({"prepared":prepared,"facts":after_prepare})
    );
    let prepared = success(&prepared);
    assert_eq!(prepared["reviewPreparation"]["target"]["branch"], base);
    assert_eq!(prepared["reviewPreparation"]["source"]["branch"], "main");
    assert_eq!(
        prepared["reviewPreparation"]["target"]["repository"]["instanceBaseUrl"],
        h.instance
    );
    prerequisite_unchanged(&before, &after_prepare);
    let released = client.rpc("accept-changes.release", json!({"workspaceId":h.workspace,"root":prepared["reviewOperation"]["root"],"operationId":prepared["reviewOperation"]["operationId"]})).await;
    eprintln!("owner preparation release: {released}");
    success(&released);
    success(&client.rpc("workspace.repositoryContext.release", json!({"workspaceId":h.workspace,"repositoryLifetimeId":success(&capture)["lifetimeId"]})).await);
    drop(client);
    h.shutdown().await;
}

#[intent_test_macros::daemon_test]
async fn native_review_target_prerequisite_member_wss() {
    if run_in_tls_process("native_review_target_prerequisite_member_wss").await {
        return;
    }
    let (_directory, h) = prerequisite_harness().await;
    let mut client = h.wss(MEMBER).await;
    let list = client.rpc("workspace.list", json!({})).await;
    let before = prerequisite_snapshot(&h).await;
    eprintln!(
        "member workspace list and facts: {}",
        json!({"list":list,"facts":before})
    );
    let base = prerequisite_workspace(&h, &list)["baseRef"].clone();
    assert_eq!(base, "trunk");
    let (capture, context) = prerequisite_context(&h, &mut client).await;
    let after_context = prerequisite_snapshot(&h).await;
    eprintln!("member context facts: {after_context}");
    prerequisite_unchanged(&before, &after_context);
    assert_eq!(
        before["provider"]["counts"],
        after_context["provider"]["counts"]
    );
    let encoded = context.to_string();
    for name in [
        "connectionId",
        "accountId",
        "connectionGeneration",
        "providerProjectId",
    ] {
        assert!(!encoded.contains(name), "member context exposes {name}");
    }
    assert!(driver_safe(&context));
    for root in success(&context)["roots"].as_array().unwrap() {
        assert_eq!(root["reviewSelection"]["outcome"]["state"], "resolved");
        assert!(!root["targets"].as_array().unwrap().is_empty());
        for target in root["targets"].as_array().unwrap() {
            assert_eq!(target["target"]["provider"], "gitlab");
            assert_eq!(target["target"]["instanceBaseUrl"], h.instance);
            assert_eq!(target["target"]["projectPath"], "group/project");
            assert_eq!(target["availability"], "connected");
            assert!(target["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .all(|c| c["state"] == "unknown"));
        }
    }
    let prepared = client
        .rpc("accept-changes.prepare", prerequisite_saved(&h, &base))
        .await;
    let after_prepare = prerequisite_snapshot(&h).await;
    eprintln!(
        "member saved preparation and facts: {}",
        json!({"prepared":prepared,"facts":after_prepare})
    );
    let prepared_value = success(&prepared);
    for branch in ["source", "target"] {
        assert!(prepared_value["reviewPreparation"][branch]
            .get("connection")
            .is_none());
        assert_eq!(
            prepared_value["reviewPreparation"][branch]["repository"]["instanceBaseUrl"],
            h.instance
        );
    }
    assert_eq!(
        prepared_value["reviewPreparation"]["target"]["branch"],
        base
    );
    prerequisite_unchanged(&before, &after_prepare);
    let mut guest = h.wss(GUEST).await;
    let denied = guest
        .rpc("accept-changes.prepare", prerequisite_saved(&h, &base))
        .await;
    let after_guest = prerequisite_snapshot(&h).await;
    eprintln!(
        "guest native refusal and facts: {}",
        json!({"denied":denied,"facts":after_guest})
    );
    assert!(denied.get("error").is_some());
    assert!(denied.get("result").is_none());
    prerequisite_unchanged(&before, &after_guest);
    let released = client.rpc("accept-changes.release", json!({"workspaceId":h.workspace,"root":prepared_value["reviewOperation"]["root"],"operationId":prepared_value["reviewOperation"]["operationId"]})).await;
    eprintln!("member preparation release: {released}");
    success(&released);
    success(&client.rpc("workspace.repositoryContext.release", json!({"workspaceId":h.workspace,"repositoryLifetimeId":success(&capture)["lifetimeId"]})).await);
    drop(guest);
    drop(client);
    h.shutdown().await;
}

// companion-observation: begin fixture collector
struct CompanionFixtureObservation(Option<Box<dyn FnOnce()>>);
impl CompanionFixtureObservation {
    fn install(directory: &std::path::Path) -> Self {
        let (dispatch, collector) = intent_services::Services::native_companion_observer(Some(
            &directory.join("companion-preparation-v1.jsonl"),
        ));
        let process = collector.process();
        // Only this explicitly selected, isolated fixture worker installs it.
        let installed = tracing::dispatcher::set_global_default(dispatch).is_ok();
        let summary = directory.join("companion-preparation-v1-final.json");
        Self(Some(Box::new(move || {
            drop(process);
            let report = collector.finish();
            let value = json!({"collectorInstalled":installed,"observation":report});
            if private_json(&summary, &value).is_err() {
                eprintln!("companion-preparation-v1: finalization output failed");
            }
        })))
    }
}
impl Drop for CompanionFixtureObservation {
    fn drop(&mut self) {
        if let Some(finish) = self.0.take() {
            finish();
        }
    }
}
// companion-observation: end fixture collector
