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
        let dir = common::test_tempdir_in("/tmp", "itd-review-wire-");
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
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        let mut ws = intent_core::chief_workspace();
        ws.id = ids
            .as_ref()
            .map_or_else(WorkspaceId::new, |ids| ids.0.clone());
        ws.branch = "main".into();
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
    std::fs::write(&script, REMOTE_FIXTURE).unwrap();
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
root=pathlib.Path(sys.argv[1]);lock=threading.Lock();state={'posts':0,'pushes':0,'reviews':[],'gitRequests':0,'gitAuthenticated':0};instance=''
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
        data=json.dumps(body).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_GET(self): self.dispatch()
    def do_POST(self): self.dispatch()
    def dispatch(self):
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
        self.send_response(status)
        for k,v in parsed:
            if k.lower()!='status':self.send_header(k,v.strip())
        self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
api=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
git=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(root/'ca.pem',root/'key.pem');git.socket=context.wrap_socket(git.socket,server_side=True)
instance='https://localhost:'+str(git.server_port)
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
