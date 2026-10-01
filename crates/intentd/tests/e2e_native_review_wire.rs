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

// Metadata composition owns no command or shutdown capability. The legacy
// SystemControl callbacks below are tripwires: typed dispatch must never invoke
// them. In particular its void shutdown callback is not a refusal mechanism.
struct FixtureMetadata {
    ws: std::sync::OnceLock<std::sync::Weak<WsApiServer>>,
    uds: std::sync::atomic::AtomicBool,
    host: intent_transport::HostEnvironment,
    has_display: bool,
    started: std::time::Instant,
    process: std::sync::Mutex<sysinfo::System>,
    pid: sysinfo::Pid,
    dir: PathBuf,
    token: Arc<AsyncTokenStore>,
    unsupported_calls: std::sync::atomic::AtomicUsize,
}
struct MetadataToken {
    writes: std::sync::atomic::AtomicUsize,
}
impl TokenStore for MetadataToken {
    fn load_token(&self) -> Option<String> {
        FixtureToken.load_token()
    }
    fn store_token(&self, _: &str) -> intent_core::Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(intent_core::Error::Forbidden(
            "immutable fixture token".into(),
        ))
    }
}
impl FixtureMetadata {
    fn new(dir: PathBuf, token: Arc<AsyncTokenStore>) -> Self {
        Self {
            ws: std::sync::OnceLock::new(),
            uds: std::sync::atomic::AtomicBool::new(false),
            host: intent_transport::host_env::detect_host_environment(),
            has_display: intent_transport::host_env::detect_has_display(),
            started: std::time::Instant::now(),
            process: std::sync::Mutex::new(sysinfo::System::new()),
            pid: sysinfo::get_current_pid().unwrap(),
            dir,
            token,
            unsupported_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn server(&self) -> Option<Arc<WsApiServer>> {
        self.ws.get().and_then(std::sync::Weak::upgrade)
    }
    fn snapshot(
        &self,
        server: Option<&WsApiServer>,
        port: Option<u16>,
    ) -> intent_transport::SystemStatus {
        let mut system = self.process.lock().unwrap();
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[self.pid]),
            true,
            sysinfo::ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory(),
        );
        let process = system
            .process(self.pid)
            .expect("original fixture process sample");
        intent_transport::SystemStatus {
            listen_mode: if port.is_some() { "both" } else { "uds" }.into(),
            uds: self.uds.load(std::sync::atomic::Ordering::SeqCst),
            tcp: port.is_some(),
            port,
            clients: server.as_ref().map_or(0, |s| s.client_count()),
            // This fixture does not attach an AgentManager or launch agents.
            agents: 0,
            max_agents: 0,
            busy_agents: 0,
            fingerprint: server
                .as_ref()
                .and_then(|s| s.fingerprint().map(str::to_owned)),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            has_display: self.has_display,
            version: env!("CARGO_PKG_VERSION").into(),
            build_commit: intent_transport::BUILD_COMMIT.map(str::to_owned),
            uptime_seconds: self.started.elapsed().as_secs(),
            local_ips: intent_transport::server::collect_local_ips(),
            tc_address: None,
            hostname: self.host.hostname.clone(),
            pretty_hostname: self.host.pretty_hostname.clone(),
            device_kind: self.host.device_kind.clone(),
            hardware_model: self.host.hardware_model.clone(),
            cpu_percent: process.cpu_usage(),
            memory_bytes: process.memory(),
            child_processes: None,
            child_memory_bytes: None,
            child_memory_peak_bytes: None,
            agent_memory_bytes: None,
            agent_process_count: None,
            agent_memory_budget_bytes: None,
            agent_memory_charged_bytes: None,
            queued_spawns: None,
            workspaces_disk_available_bytes: None,
            workspaces_disk_total_bytes: None,
            file_watch: None,
            fd_count: None,
            fd_limit: None,
            update_supported: false,
            idle_update_check: intent_transport::IdleUpdateCheckStatus::default(),
        }
    }
    fn unsupported(&self) {
        self.unsupported_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl intent_transport::SystemControl for FixtureMetadata {
    fn services(&self) -> intent_transport::control::SystemServices {
        intent_transport::control::SystemServices::StatusOnly
    }
    fn status(&self) -> intent_transport::SystemStatus {
        // Legacy synchronous trait access is not the installed metadata route.
        // The original live await is selected by StatusOnly dispatch.
        use futures_util::FutureExt;
        let server = self.server();
        let port = server
            .as_ref()
            .and_then(|s| s.bound_port().now_or_never().flatten());
        self.snapshot(server.as_deref(), port)
    }
    fn metadata_status(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = intent_transport::SystemStatus> + Send + '_>,
    > {
        Box::pin(async {
            let server = self.server();
            let port = match &server {
                Some(server) => server.bound_port().await,
                None => None,
            };
            self.snapshot(server.as_deref(), port)
        })
    }
    fn host_environment(&self) -> intent_transport::HostEnvironment {
        self.host.clone()
    }
    fn request_shutdown(&self) {
        self.unsupported();
    }
    fn request_update(&self) -> Result<(), String> {
        self.unsupported();
        Err("metadata fixture has no updater".into())
    }
    fn import_legacy(
        &self,
        _: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>>
    {
        self.unsupported();
        Box::pin(async { Err("metadata fixture has no importer".into()) })
    }
    fn git_credential(
        &self,
        _: Option<u64>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Option<intent_transport::control::GitCredential>>
                + Send
                + '_,
        >,
    > {
        self.unsupported();
        Box::pin(async { None })
    }
}
impl intent_transport::ServerPairingInfo for FixtureMetadata {
    fn services(&self) -> intent_transport::server::PairingServices {
        intent_transport::server::PairingServices::LocalInfoOnly
    }
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = intent_transport::PairingSnapshot> + Send + '_>,
    > {
        Box::pin(async {
            let port = match self.server() {
                Some(s) => s.bound_port().await,
                None => None,
            };
            intent_transport::PairingSnapshot {
                port,
                bind_addresses: port.map(|_| vec![std::net::Ipv4Addr::LOCALHOST.into()]),
                tc_address: None,
            }
        })
    }
    fn host_environment(&self) -> intent_transport::HostEnvironment {
        self.host.clone()
    }
    fn data_dir(&self) -> &std::path::Path {
        &self.dir
    }
    fn token_store(&self) -> &AsyncTokenStore {
        &self.token
    }
}

struct Harness {
    metadata: Arc<FixtureMetadata>,
    metadata_token: Arc<MetadataToken>,
    api: Arc<dyn WorkspaceApi>,
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
        let _ = startup_milestones::waited(self.fixture.id(), self.fixture.wait(), |s| {
            use std::os::unix::process::ExitStatusExt;
            (s.code(), s.signal())
        });
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
        let startup_repositories =
            startup_milestones::begin(startup_milestones::Phase::Repositories);
        let root = dir.path().join("repo");
        std::fs::create_dir(&root).unwrap();
        let registered_path = dir.path().join("secondary");
        std::fs::create_dir(&registered_path).unwrap();
        init_repo(&root);
        init_repo(&registered_path);
        startup_repositories.returned();
        let startup_provider = startup_milestones::begin(startup_milestones::Phase::Provider);
        let (fixture, instance, endpoint, fixture_state) = remote_fixture(dir.path(), &root).await;
        startup_provider.returned();
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
        let startup_store = startup_milestones::begin(startup_milestones::Phase::Store);
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
        startup_store.returned();
        let startup_roles = startup_milestones::begin(startup_milestones::Phase::Roles);
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
        startup_roles.returned();
        let startup_services = startup_milestones::begin(startup_milestones::Phase::Services);
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
        let metadata_token = Arc::new(MetadataToken {
            writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let token_store = Arc::new(AsyncTokenStore::new(metadata_token.clone()));
        let metadata = Arc::new(FixtureMetadata::new(
            dir.path().to_owned(),
            token_store.clone(),
        ));
        let options = WsOptions {
            base_port: 0,
            bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        };
        let mut ws_server = WsApiServer::new(
            api.clone(),
            bus.clone(),
            &tls,
            &token_store,
            options,
            Some(metadata.clone()),
        )
        .unwrap();
        ws_server.install_pairing_info(metadata.clone());
        let ws_server = Arc::new(ws_server);
        metadata.ws.set(Arc::downgrade(&ws_server)).unwrap();
        startup_services.returned();
        let startup_wss = startup_milestones::begin(startup_milestones::Phase::Wss);
        let port = ws_server.start().await.unwrap();
        startup_wss.returned();
        let startup_uds = startup_milestones::begin(startup_milestones::Phase::Uds);
        let (shutdown, receive) = tokio::sync::oneshot::channel();
        let socket = dir.path().join("intentd.sock");
        let socket_task = socket.clone();
        let ws_task = ws_server.clone();
        let api_task = api.clone();
        let metadata_task = metadata.clone();
        let listener = tokio::spawn(async move {
            let result = intent_transport::serve_uds_with_reverse(
                api_task,
                bus,
                &socket_task,
                Some(metadata_task.clone()),
                Some(metadata_task.clone()),
                Arc::new(intent_transport::PrimaryReverseRegistry::new()),
                intent_transport::RpcLimiter::unlimited(),
                async move {
                    let _ = receive.await;
                },
            )
            .await;
            metadata_task
                .uds
                .store(false, std::sync::atomic::Ordering::SeqCst);
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
        metadata
            .uds
            .store(true, std::sync::atomic::Ordering::SeqCst);
        startup_uds.returned();
        Self {
            metadata,
            metadata_token,
            api,
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
    startup_milestones::allocated(child.id());
    let startup_endpoints = startup_milestones::begin(startup_milestones::Phase::ProviderEndpoints);
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
    startup_endpoints.returned();
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
    json!({"sourceCommit":startup_milestones::call(startup_milestones::Phase::IdentityCommit, || git(&repository,&["rev-parse","HEAD"])),
        "sourceTree":startup_milestones::call(startup_milestones::Phase::IdentityTree, || git(&repository,&["rev-parse","HEAD^{tree}"])),
        "sourceSha256":startup_milestones::call(startup_milestones::Phase::IdentitySource, driver_source_hash),
        "executableSha256":startup_milestones::call(startup_milestones::Phase::IdentityExecutable, || driver_hash(&std::fs::read(std::env::current_exe().unwrap()).unwrap()))})
}

fn private_json(path: &std::path::Path, value: &Value) -> DriverResult<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = startup_milestones::io(startup_milestones::Phase::FileOpen, || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
    })?;
    startup_milestones::io(startup_milestones::Phase::Serialize, || {
        serde_json::to_writer(&mut file, value)
    })?;
    file.write_all(b"\n")?;
    startup_milestones::io(startup_milestones::Phase::Sync, || file.sync_all())?;
    Ok(())
}

fn publish_driver(path: &std::path::Path, value: &Value) -> DriverResult<()> {
    let startup_publication = startup_milestones::begin(startup_milestones::Phase::Publication);
    let temporary = path.with_extension("pending");
    private_json(&temporary, value)?;
    // Atomic no-clobber publication: an existing destination is never overwritten.
    startup_milestones::result(startup_milestones::Phase::Link, || {
        std::fs::hard_link(&temporary, path)
    })?;
    startup_milestones::result(startup_milestones::Phase::Unlink, || {
        std::fs::remove_file(temporary)
    })?;
    startup_publication.returned();
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
    let startup_descriptor_bytes = std::fs::read(path)?;
    let mut descriptor: DriverDescriptor = serde_json::from_slice(&startup_descriptor_bytes)?;
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
    if child {
        startup_milestones::binding(&descriptor, &startup_descriptor_bytes);
    }
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
    startup_milestones::reached();
    let startup_host_a = startup_milestones::host(startup_milestones::Host::A);
    let a = Harness::boot_driver(None, true, Some(descriptor)).await;
    startup_host_a.returned();
    let startup_host_b = startup_milestones::host(startup_milestones::Host::B);
    let b = Harness::boot_driver(
        Some((a.workspace.clone(), a.registered.clone())),
        true,
        Some(descriptor),
    )
    .await;
    startup_host_b.returned();
    let mut hosts = vec![a, b];
    let startup_credentials = startup_milestones::begin(startup_milestones::Phase::Credentials);
    let credentials = descriptor.directory.join("credentials");
    std::fs::create_dir(&credentials)?;
    std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o700))?;
    for (name, token) in [("owner", TOKEN), ("member", MEMBER), ("guest", GUEST)] {
        private_json(&credentials.join(name), &json!({"token":token}))?;
    }
    startup_credentials.returned();
    let startup_control = startup_milestones::begin(startup_milestones::Phase::Control);
    let socket = descriptor.directory.join("control.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    startup_control.returned();
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
    startup_milestones::finish_ready();
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
    let status = startup_milestones::waited(host.fixture.id(), host.fixture.wait(), |s| {
        (s.code(), s.signal())
    })?;
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
            // startup-milestones: original enrolled worker only; no observer task.
            let _startup_journal = startup_milestones::Session::from_environment();
            let descriptor =
                startup_milestones::result(startup_milestones::Phase::Validation, || {
                    driver_descriptor_phase(&path, true, true, false)
                })?;
            if let Ok(case) = std::env::var("INTENT_REVIEW_DRIVER_INERT") {
                return inert_worker(&descriptor.directory, &case);
            }
            let startup_tls = startup_milestones::begin(startup_milestones::Phase::Tls);
            let tls = descriptor.directory.join("tls");
            std::fs::create_dir(&tls)?;
            std::fs::set_permissions(&tls, std::fs::Permissions::from_mode(0o700))?;
            make_fixture_certificate(&tls);
            startup_tls.returned();
            let descriptor =
                startup_milestones::result(startup_milestones::Phase::Validation, || {
                    driver_descriptor(&path, true)
                })?;
            // companion-observation: begin worker collector
            let _companion_observer =
                startup_milestones::call(startup_milestones::Phase::Observer, || {
                    CompanionFixtureObservation::install(&descriptor.directory)
                });
            // companion-observation: end worker collector
            return startup_milestones::result(startup_milestones::Phase::Runtime, || {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
            })?
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

// Metadata qualification uses original listeners and admission, never the
// standalone driver entry point. Private pairing replies stay in memory.
async fn metadata_effects(h: &Harness) -> Value {
    json!({"head":git(&h.root,&["rev-parse","HEAD"]),
        "index":git(&h.root,&["ls-files","--stage"]),
        "worktree":git(&h.root,&["diff","--binary"]),
        "provider":h.counts(),
        "workspaceInvites":h.store.count_open_workspace_invites().await.unwrap(),
        "hostInvites":h.store.list_open_host_invites().await.unwrap().len(),
        "unsupportedCalls":h.metadata.unsupported_calls.load(std::sync::atomic::Ordering::SeqCst),
        "tokenWrites":h.metadata_token.writes.load(std::sync::atomic::Ordering::SeqCst)})
}
fn metadata_public(frame: &Value) {
    let text = frame.to_string();
    for token in [TOKEN, MEMBER, GUEST] {
        assert!(
            !text.contains(token),
            "public response contained fixture credential"
        );
    }
    assert!(!text.contains("BEGIN CERTIFICATE"));
    assert!(frame.get("result").and_then(|r| r.get("token")).is_none());
}
async fn metadata_close(h: Harness) {
    let metadata = h.metadata.clone();
    let ws = Arc::downgrade(&h.ws);
    h.shutdown().await;
    eprintln!(
        "metadata cleanup: {}",
        json!({"originalHarnessShutdownJoined":true,"wsOwnerReleased":ws.upgrade().is_none(),"metadataStrongCount":Arc::strong_count(&metadata),"uds":metadata.uds.load(std::sync::atomic::Ordering::SeqCst)})
    );
    assert!(ws.upgrade().is_none());
    assert_eq!(Arc::strong_count(&metadata), 1);
    assert!(!metadata.uds.load(std::sync::atomic::Ordering::SeqCst));
}
#[intent_test_macros::daemon_test]
async fn native_fixture_metadata_status_pairing_roles() {
    if run_in_tls_process("native_fixture_metadata_status_pairing_roles").await {
        return;
    }
    use futures_util::FutureExt;
    eprintln!(
        "metadata worker: {}",
        json!({"test":"native_fixture_metadata_status_pairing_roles","pid":std::process::id()})
    );
    let a = Harness::boot().await;
    let b = Harness::boot().await;
    let result=std::panic::AssertUnwindSafe(async {
        let before_a=metadata_effects(&a).await;
        let before_b=metadata_effects(&b).await;
        for h in [&a,&b] {
            let mut uds=h.uds().await;
            let status=uds.rpc("system.status",json!({})).await;
            metadata_public(&status);
            eprintln!("metadata UDS status: {status}");
            let value=success(&status);
            assert_eq!(value["port"],h.port);
            assert_eq!(value["transports"],json!(["uds","tcp"]));
            assert_eq!(value["fingerprint"],h.ws.fingerprint().unwrap());
            assert_eq!(value["host"]["locality"],"local");
            assert_eq!(value["agents"],0);
            assert_eq!(value["maxAgents"],0);
            assert_eq!(value["updateSupported"],false);
            assert_eq!(value["exactUpdateSupported"],false);
            assert_eq!(value["idleUpdateCheck"]["supported"],false);
            assert!(value["memoryBytes"].as_u64().unwrap()>0);
            assert!(value["cpuPercent"].as_f64().unwrap().is_finite());
            assert!(value["childProcesses"].is_null());
            assert!(value.get("fileWatch").is_none());
            let pairing=uds.rpc("server.pairingInfo",json!({})).await;
            // Do not print a private reply, including on an assertion failure.
            assert!(pairing.get("error").is_none(),"local pairing refused");
            let p=&pairing["result"];
            let token_matches=p["token"].as_str()==Some(TOKEN);
            let certificate_matches=p["certFingerprint"].as_str()==h.ws.fingerprint();
            let port_matches=p["port"]==h.port;
            eprintln!("metadata local pairing checks: {}",json!({"tokenMatches":token_matches,"certificateMatches":certificate_matches,"portMatches":port_matches,"pathMatches":p["path"]=="/ws"}));
            assert!(token_matches && certificate_matches && port_matches);
            assert_eq!(p["path"],"/ws");
            for (role,token) in [("owner",TOKEN),("member",MEMBER),("guest",GUEST)] {
                let mut remote=h.wss(token).await;
                let status=remote.rpc("system.status",json!({})).await;
                metadata_public(&status);
                eprintln!("metadata {role} WSS status: {status}");
                let v=success(&status);
                assert_eq!(v["port"],h.port);
                assert_eq!(v["host"]["locality"],"remote");
                if role=="owner" {
                    assert_eq!(v["clients"],h.ws.client_count());
                    assert_eq!(v["updateSupported"],false);
                } else {
                    for key in ["clients","agents","maxAgents","cpuPercent","memoryBytes","updateSupported","exactUpdateSupported","idleUpdateCheck","busyAgents"] { assert!(v.get(key).is_none(),"collaborator received {key}"); }
                }
                let denied=remote.rpc("server.pairingInfo",json!({})).await;
                metadata_public(&denied);
                eprintln!("metadata {role} remote pairing refusal: {denied}");
                assert!(denied.get("result").is_none());
                assert_eq!(denied["error"]["code"],if role=="owner" {-32001} else {-32003});
            }
            let url=format!("wss://127.0.0.1:{}/ws?token=invalid-metadata-token",h.port);
            let status=timeout(common::rpc_read_timeout(),async {
            let tcp=tokio::net::TcpStream::connect(("127.0.0.1",h.port)).await.unwrap();
            let tls=tokio_rustls::TlsConnector::from(h.cfg.clone()).connect(ServerName::try_from("localhost").unwrap().to_owned(),tcp).await.unwrap();
            let rejected=tokio_tungstenite::client_async(&url,tls).await;
            match rejected {Err(tokio_tungstenite::tungstenite::Error::Http(reply))=>Some(reply.status().as_u16()),_=>None}
            }).await.expect("original invalid bearer upgrade bounded");
            eprintln!("metadata invalid bearer actual HTTP status: {status:?}");
            assert_eq!(status,Some(401));
        }
        assert_ne!(a.port,b.port);
        assert_ne!(a.workspace,b.workspace);
        assert_ne!(a.dir.path(),b.dir.path());
        // The original TLS helper caches one WSS certificate per process. Use
        // the separately owned provider CA as the genuinely different pin.
        let foreign=std::process::Command::new("openssl")
            .args(["x509","-in"]).arg(a.dir.path().join("ca.pem"))
            .args(["-outform","DER"]).output().unwrap();
        assert!(foreign.status.success(),"owned provider certificate read failed");
        let foreign_pin=Sha256::digest(&foreign.stdout).iter()
            .map(|byte| format!("{byte:02X}")).collect::<Vec<_>>().join(":");
        assert_ne!(Some(foreign_pin.as_str()),b.ws.fingerprint());
        let cross=timeout(common::rpc_read_timeout(),async {
        let tcp=tokio::net::TcpStream::connect(("127.0.0.1",b.port)).await.unwrap();
        tokio_rustls::TlsConnector::from(client_config(&foreign_pin)).connect(ServerName::try_from("localhost").unwrap().to_owned(),tcp).await
        }).await.expect("original foreign certificate rejection bounded");
        eprintln!("metadata foreign certificate TLS rejected: {}",cross.is_err());
        assert!(cross.is_err(),"foreign pinned certificate upgraded");
        let after_a=metadata_effects(&a).await;let after_b=metadata_effects(&b).await;
        eprintln!("metadata original effects: {}",json!({"beforeA":before_a,"afterA":after_a,"beforeB":before_b,"afterB":after_b}));
        assert_eq!(before_a,after_a);assert_eq!(before_b,after_b);
    }).catch_unwind().await;
    metadata_close(a).await;
    metadata_close(b).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

impl Client {
    async fn metadata_notification(&mut self, method: &str, params: &Value) {
        let frame = json!({"jsonrpc":"2.0","method":method,"params":params}).to_string();
        match &mut self.socket {
            Socket::Uds(stream) => stream
                .get_mut()
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .unwrap(),
            Socket::Ws(ws) => ws.send(Message::Text(frame.into())).await.unwrap(),
        }
    }
}
#[intent_test_macros::daemon_test]
async fn native_fixture_metadata_unsupported_matches_absent() {
    if run_in_tls_process("native_fixture_metadata_unsupported_matches_absent").await {
        return;
    }
    use futures_util::FutureExt;
    eprintln!(
        "metadata worker: {}",
        json!({"test":"native_fixture_metadata_unsupported_matches_absent","pid":std::process::id()})
    );
    let h = Harness::boot().await;
    let socket = h.dir.path().join("absent.sock");
    let (stop, receive) = tokio::sync::oneshot::channel();
    let api = h.api.clone();
    let task_socket = socket.clone();
    let absent_bus = EventBus::new(h.store.clone());
    let listener = tokio::spawn(async move {
        serve_uds(api, absent_bus, &task_socket, None, async {
            let _ = receive.await;
        })
        .await
    });
    let tls = ensure_tls_certificate(h.dir.path()).unwrap();
    let absent = WsApiServer::new(
        h.api.clone(),
        EventBus::new(h.store.clone()),
        &tls,
        &h.metadata.token,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let port = absent.start().await.unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let before = metadata_effects(&h).await;
        let stream = timeout(common::daemon_startup_timeout(), async {
            loop {
                if let Ok(s) = UnixStream::connect(&socket).await {
                    break s;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut baseline = Client {
            socket: Socket::Uds(BufReader::new(stream)),
            id: 0,
            notices: Vec::new(),
        };
        let mut metadata = h.uds().await;
        let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
        let ws = common::wss_connect_with_retry(port, h.cfg.clone(), &url).await;
        let mut baseline_remote = Client {
            socket: Socket::Ws(Box::new(ws)),
            id: 0,
            notices: Vec::new(),
        };
        let mut metadata_remote = h.wss(TOKEN).await;
        for (method, params) in [
            ("system.shutdown", json!({})),
            ("system.requestUpdate", json!({})),
            ("system.requestUpdate", json!({"targetVersion":"1.2.3"})),
            ("system.requestUpdate", json!({"targetVersion":42})),
            ("system.importLegacy", json!({"force":true})),
            (
                "system.gitCredential",
                json!({"protocol":"https","host":"github.com"}),
            ),
            (
                "system.gitCredential",
                json!({"protocol":"ssh","host":"other.invalid"}),
            ),
            ("server.rotateToken", json!({})),
            ("pairing.getInfo", json!({})),
            ("pairing.getSelfInfo", json!({})),
            (
                "workspace.invite.create",
                json!({"workspaceId":h.workspace}),
            ),
            (
                "host.invite.create",
                json!({"pinLogin":"fixture","pinProvider":"github"}),
            ),
        ] {
            for (transport, a, b) in [
                ("uds", &mut metadata, &mut baseline),
                ("wss", &mut metadata_remote, &mut baseline_remote),
            ] {
                a.metadata_notification(method, &params).await;
                b.metadata_notification(method, &params).await;
                let mut actual = a.rpc(method, params.clone()).await;
                let mut absent = b.rpc(method, params.clone()).await;
                metadata_public(&actual);
                metadata_public(&absent);
                actual.as_object_mut().unwrap().remove("id");
                absent.as_object_mut().unwrap().remove("id");
                eprintln!(
                    "metadata unsupported {transport} {method}: {}",
                    json!({"actual":actual,"absent":absent})
                );
                assert!(actual.get("result").is_none());
                assert_eq!(actual, absent);
            }
        }
        success(&metadata.rpc("system.status", json!({})).await);
        success(&metadata_remote.rpc("system.status", json!({})).await);
        let notification_frames=[metadata.notices.len(),baseline.notices.len(),metadata_remote.notices.len(),baseline_remote.notices.len()];
        eprintln!("metadata unsolicited frames after notification/request routing: {notification_frames:?}");
        assert_eq!(notification_frames,[0,0,0,0]);
        let after = metadata_effects(&h).await;
        eprintln!(
            "metadata unsupported effects: {}",
            json!({"before":before,"after":after})
        );
        assert_eq!(before, after);
        assert_eq!(after["unsupportedCalls"], 0);
        assert_eq!(after["tokenWrites"], 0);
    })
    .catch_unwind()
    .await;
    stop.send(()).unwrap();
    timeout(Duration::from_secs(5), listener)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    absent.stop().await;
    assert!(absent.bound_port().await.is_none());
    assert!(UnixStream::connect(&socket).await.is_err());
    assert!(tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_err());
    eprintln!("metadata absent listener original close and join completed");
    metadata_close(h).await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[intent_test_macros::daemon_test]
async fn native_fixture_metadata_failed_bind_releases_original_owners() {
    if run_in_tls_process("native_fixture_metadata_failed_bind_releases_original_owners").await {
        return;
    }
    eprintln!(
        "metadata worker: {}",
        json!({"test":"native_fixture_metadata_failed_bind_releases_original_owners","pid":std::process::id()})
    );
    let h = Harness::boot().await;
    let before = metadata_effects(&h).await;
    let path = h.dir.path().join("occupied-socket");
    std::fs::create_dir(&path).unwrap();
    let result = intent_transport::serve_uds_with_reverse(
        h.api.clone(),
        EventBus::new(h.store.clone()),
        &path,
        Some(h.metadata.clone()),
        Some(h.metadata.clone()),
        Arc::new(intent_transport::PrimaryReverseRegistry::new()),
        intent_transport::RpcLimiter::unlimited(),
        std::future::pending::<()>(),
    )
    .await;
    eprintln!(
        "metadata original failed bind: {}",
        json!({"errorKind":result.as_ref().err().map(|e|format!("{:?}",e.kind())),"effects":metadata_effects(&h).await})
    );
    let failed = result.is_err();
    let after = metadata_effects(&h).await;
    metadata_close(h).await;
    assert!(failed);
    assert_eq!(before, after);
}

// startup-milestones: begin private producer and reader
// One original worker thread owns this diagnostic journal. It does not propagate
// into spawned tasks, change tracing, or own a host/child. Completion covers only
// original startup through ready publication, never request or shutdown success.
mod startup_milestones {
    use super::*;
    use std::cell::RefCell;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::os::unix::io::{AsRawFd, FromRawFd};
    pub(super) const FILE: &str = "native-startup-milestones-v1.jsonl";
    const RECORDS: usize = 96;
    const BYTES: usize = 48 * 1024;
    const FRAME: usize = 1024;
    #[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub(super) enum Phase {
        Process,
        Validation,
        Tls,
        Observer,
        Runtime,
        Loop,
        Host,
        Repositories,
        Provider,
        ProviderEndpoints,
        Store,
        Roles,
        Services,
        Wss,
        Uds,
        Credentials,
        Control,
        IdentityCommit,
        IdentityTree,
        IdentitySource,
        IdentityExecutable,
        Publication,
        FileOpen,
        Serialize,
        Sync,
        Link,
        Unlink,
        Binding,
        Child,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub(super) enum Host {
        A,
        B,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum Outcome {
        Enter,
        Return,
        Error,
        Unwind,
        Abandoned,
        Allocated,
        Waited,
        Bound,
        Reached,
        Complete,
    }
    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Binding {
        run: uuid::Uuid,
        descriptor: String,
        artifact: String,
        source: String,
        commit: String,
        tree: String,
    }
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Counts {
        observed: u64,
        written: u64,
        dropped: u64,
        overflow: u64,
        io: u64,
        unmatched: u64,
    }
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Frame {
        v: u8,
        pid: u32,
        parent: u32,
        wall_ms: u64,
        clock: String,
        seq: u64,
        ns: u64,
        phase: Phase,
        outcome: Outcome,
        span: Option<u64>,
        host: Option<Host>,
        child: Option<u32>,
        code: Option<i32>,
        signal: Option<i32>,
        binding: Option<Binding>,
        counts: Option<Counts>,
    }
    struct State {
        file: std::fs::File,
        start: std::time::Instant,
        pid: u32,
        parent: u32,
        wall: u64,
        counts: Counts,
        bytes: usize,
        stack: Vec<(u64, Phase, Option<Host>)>,
        host: Option<Host>,
        children: Vec<(u32, Host)>,
        bound: Option<Binding>,
        failed: bool,
        ready: bool,
        ended: bool,
    }
    thread_local! { static STATE: RefCell<Option<State>> = const { RefCell::new(None) }; }
    pub(super) struct Session(bool);
    impl Session {
        pub(super) fn from_environment() -> Self {
            if std::env::var("NATIVE_REVIEW_COMPANION_DIAGNOSTIC_6328").as_deref() != Ok("1") {
                return Self(false);
            }
            let Some(path) = std::env::var_os("NATIVE_REVIEW_EVIDENCE_DIR") else {
                return Self(false);
            };
            Self::open(&PathBuf::from(path))
        }
        pub(super) fn open(path: &std::path::Path) -> Self {
            if STATE.with(|s| s.borrow().is_some()) {
                return Self(false);
            }
            let opened = (|| -> std::io::Result<std::fs::File> {
                if !path.is_absolute() || std::fs::canonicalize(path)? != path {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                let directory = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(path)?;
                let metadata = directory.metadata()?;
                // SAFETY: geteuid has no arguments; openat uses an owned directory fd
                // and a fixed NUL-terminated basename, never a caller-controlled path.
                let uid = unsafe { libc::geteuid() };
                if !metadata.is_dir() || metadata.mode() & 0o777 != 0o700 || metadata.uid() != uid {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                // SAFETY: the directory and literal remain live through the call.
                let fd = unsafe {
                    libc::openat(
                        directory.as_raw_fd(),
                        c"native-startup-milestones-v1.jsonl".as_ptr(),
                        libc::O_WRONLY
                            | libc::O_CREAT
                            | libc::O_EXCL
                            | libc::O_NOFOLLOW
                            | libc::O_CLOEXEC,
                        0o600,
                    )
                };
                if fd < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // SAFETY: openat returned a new owned fd exactly once.
                let file = unsafe { std::fs::File::from_raw_fd(fd) };
                let metadata = file.metadata()?;
                if !metadata.is_file()
                    || metadata.mode() & 0o777 != 0o600
                    || metadata.nlink() != 1
                    || metadata.uid() != uid
                {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                Ok(file)
            })();
            Self::with_file(opened)
        }
        fn with_file(file: std::io::Result<std::fs::File>) -> Self {
            let occupied = STATE.with(|s| s.borrow().is_some());
            if occupied {
                return Self(false);
            }
            let Ok(file) = file else {
                eprintln!("native-startup-milestones-v1: private journal unavailable");
                return Self(false);
            };
            let wall = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |n| u64::try_from(n.as_millis()).unwrap_or(u64::MAX));
            STATE.with(|s| {
                *s.borrow_mut() = Some(State {
                    file,
                    start: std::time::Instant::now(),
                    pid: std::process::id(),
                    // SAFETY: getppid has no arguments and is diagnostic identity only.
                    parent: u32::try_from(unsafe { libc::getppid() }).unwrap_or(0),
                    wall,
                    counts: Counts {
                        observed: 0,
                        written: 0,
                        dropped: 0,
                        overflow: 0,
                        io: 0,
                        unmatched: 0,
                    },
                    bytes: 0,
                    stack: vec![],
                    host: None,
                    children: vec![],
                    bound: None,
                    failed: false,
                    ready: false,
                    ended: false,
                });
            });
            let _ = begin(Phase::Process).disarm();
            Self(true)
        }
    }
    impl Drop for Session {
        fn drop(&mut self) {
            if self.0 {
                STATE.with(|s| {
                    if let Some(mut state) = s.borrow_mut().take() {
                        if !state.ended {
                            state.end(if std::thread::panicking() {
                                Outcome::Unwind
                            } else {
                                Outcome::Abandoned
                            });
                        }
                    }
                });
            }
        }
    }
    impl State {
        fn frame(&mut self, phase: Phase, outcome: Outcome, span: Option<u64>) -> Frame {
            self.counts.observed += 1;
            Frame {
                v: 1,
                pid: self.pid,
                parent: self.parent,
                wall_ms: self.wall,
                clock: "worker-instant".into(),
                seq: self.counts.observed,
                ns: u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                phase,
                outcome,
                span,
                host: self.host,
                child: None,
                code: None,
                signal: None,
                binding: None,
                counts: None,
            }
        }
        fn write(&mut self, frame: &Frame, final_row: bool) {
            let Ok(mut bytes) = serde_json::to_vec(frame) else {
                self.counts.dropped += 1;
                self.failed = true;
                return;
            };
            bytes.push(b'\n');
            let reserved = if final_row { 0 } else { FRAME };
            if bytes.len() > FRAME
                || self.counts.written >= u64::try_from(RECORDS).unwrap() - u64::from(!final_row)
                || self
                    .bytes
                    .saturating_add(bytes.len())
                    .saturating_add(reserved)
                    > BYTES
            {
                self.counts.dropped += 1;
                self.counts.overflow += 1;
                self.failed = true;
                return;
            }
            self.bytes += bytes.len(); // Conservatively accounts attempted bytes, including partial I/O.
            if self.file.write_all(&bytes).is_ok() {
                self.counts.written += 1;
            } else {
                self.counts.dropped += 1;
                self.counts.io += 1;
                self.failed = true;
            }
        }
        fn end(&mut self, outcome: Outcome) {
            if self.ended {
                return;
            }
            let valid = self.stack == [(1, Phase::Process, None)];
            if !valid {
                self.counts.unmatched += 1;
                self.failed = true;
            }
            let outcome = if outcome == Outcome::Complete
                && (!self.ready || self.bound.is_none() || self.failed)
            {
                Outcome::Abandoned
            } else {
                outcome
            };
            let mut frame = self.frame(Phase::Process, outcome, Some(1));
            frame.host = None;
            frame.counts = Some(self.counts.clone());
            self.write(&frame, true);
            self.ended = true;
        }
    }
    pub(super) struct Span {
        key: Option<(u64, Phase, Option<Host>)>,
    }
    impl Span {
        fn disarm(mut self) -> Option<(u64, Phase, Option<Host>)> {
            self.key.take()
        }
        pub(super) fn returned(mut self) {
            self.close(Outcome::Return);
        }
        fn close(&mut self, outcome: Outcome) {
            if let Some(key) = self.key.take() {
                STATE.with(|s| {
                    if let Some(state) = s.borrow_mut().as_mut().filter(|s| !s.ended) {
                        if state.stack.pop() != Some(key) {
                            state.counts.unmatched += 1;
                            state.failed = true;
                        }
                        state.failed |= outcome != Outcome::Return;
                        let mut frame = state.frame(key.1, outcome, Some(key.0));
                        frame.host = key.2;
                        self::State::write(state, &frame, false);
                        if key.1 == Phase::Publication && outcome == Outcome::Return {
                            state.ready = true;
                        }
                    }
                });
            }
        }
    }
    impl Drop for Span {
        fn drop(&mut self) {
            self.close(if std::thread::panicking() {
                Outcome::Unwind
            } else {
                Outcome::Abandoned
            });
        }
    }
    pub(super) fn begin(phase: Phase) -> Span {
        let key = STATE.with(|s| {
            let mut s = s.borrow_mut();
            let state = s.as_mut().filter(|s| !s.ended)?;
            if state.stack.len() >= 32 {
                state.failed = true;
                state.counts.observed += 1;
                state.counts.dropped += 1;
                state.counts.overflow += 1;
                return None;
            }
            let seq = state.counts.observed + 1;
            let key = (seq, phase, state.host);
            state.stack.push(key);
            let frame = state.frame(phase, Outcome::Enter, Some(seq));
            state.write(&frame, false);
            Some(key)
        });
        Span { key }
    }
    pub(super) fn call<T>(phase: Phase, call: impl FnOnce() -> T) -> T {
        let span = begin(phase);
        let value = call();
        span.returned();
        value
    }
    pub(super) fn result<T, E>(phase: Phase, call: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        let mut span = begin(phase);
        let value = call();
        span.close(if value.is_ok() {
            Outcome::Return
        } else {
            Outcome::Error
        });
        value
    }
    pub(super) struct HostScope {
        previous: Option<Host>,
        span: Option<Span>,
    }
    pub(super) fn host(host: Host) -> HostScope {
        let previous = STATE.with(|s| s.borrow_mut().as_mut().and_then(|s| s.host.replace(host)));
        HostScope {
            previous,
            span: Some(begin(Phase::Host)),
        }
    }
    impl HostScope {
        pub(super) fn returned(mut self) {
            if let Some(span) = self.span.take() {
                span.returned();
            }
        }
    }
    impl Drop for HostScope {
        fn drop(&mut self) {
            drop(self.span.take());
            STATE.with(|s| {
                if let Some(s) = s.borrow_mut().as_mut() {
                    s.host = self.previous;
                }
            });
        }
    }
    pub(super) fn binding(descriptor: &DriverDescriptor, bytes: &[u8]) {
        STATE.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut().filter(|s| !s.ended) {
                let Ok(run) = uuid::Uuid::parse_str(&descriptor.run_id) else {
                    s.failed = true;
                    return;
                };
                let valid =
                    |text: &str, n| text.len() == n && text.bytes().all(|c| c.is_ascii_hexdigit());
                if !valid(&descriptor.executable_sha256, 64)
                    || !valid(&descriptor.source_sha256, 64)
                    || !valid(&descriptor.source_commit, 40)
                    || !valid(&descriptor.source_tree, 40)
                {
                    s.failed = true;
                    return;
                }
                let binding = Binding {
                    run,
                    descriptor: driver_hash(bytes),
                    artifact: descriptor.executable_sha256.clone(),
                    source: descriptor.source_sha256.clone(),
                    commit: descriptor.source_commit.clone(),
                    tree: descriptor.source_tree.clone(),
                };
                if let Some(original) = &s.bound {
                    if original != &binding {
                        s.failed = true;
                        s.counts.unmatched += 1;
                    }
                    return;
                }
                let mut frame = s.frame(Phase::Binding, Outcome::Bound, None);
                frame.binding = Some(binding.clone());
                s.write(&frame, false);
                s.bound = Some(binding);
            }
        });
    }
    pub(super) fn reached() {
        STATE.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut().filter(|s| !s.ended) {
                let frame = s.frame(Phase::Loop, Outcome::Reached, None);
                s.write(&frame, false);
            }
        });
    }
    pub(super) fn allocated(pid: u32) {
        STATE.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut().filter(|s| !s.ended) {
                if let Some(host) = s.host.filter(|_| s.children.len() < 2) {
                    s.children.push((pid, host));
                    let mut frame = s.frame(Phase::Child, Outcome::Allocated, None);
                    frame.child = Some(pid);
                    s.write(&frame, false);
                } else {
                    s.failed = true;
                    s.counts.unmatched += 1;
                }
            }
        });
    }
    pub(super) fn waited<T>(
        pid: u32,
        result: std::io::Result<T>,
        status: impl FnOnce(&T) -> (Option<i32>, Option<i32>),
    ) -> std::io::Result<T> {
        STATE.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut().filter(|s| !s.ended) {
                if let Some((_, host)) = s.children.iter().find(|(p, _)| *p == pid).copied() {
                    let mut frame = s.frame(
                        Phase::Child,
                        if result.is_ok() {
                            Outcome::Waited
                        } else {
                            Outcome::Error
                        },
                        None,
                    );
                    frame.child = Some(pid);
                    frame.host = Some(host);
                    if let Ok(value) = &result {
                        (frame.code, frame.signal) = status(value);
                    }
                    s.write(&frame, false);
                }
            }
        });
        result
    }
    pub(super) fn finish_ready() {
        STATE.with(|s| {
            if let Some(s) = s.borrow_mut().as_mut() {
                s.end(Outcome::Complete);
            }
        });
    }
    pub(super) fn publishing() -> bool {
        STATE.with(|s| {
            s.borrow()
                .as_ref()
                .is_some_and(|s| !s.ended && s.stack.iter().any(|k| k.1 == Phase::Publication))
        })
    }
    pub(super) fn io<T, E>(phase: Phase, call: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        if publishing() {
            result(phase, call)
        } else {
            call()
        }
    }
    #[derive(Debug)]
    pub(super) struct Report {
        pub complete: bool,
        pub malformed: bool,
        pub records: usize,
        pub open: usize,
    }
    pub(super) fn read(bytes: &[u8]) -> Report {
        if bytes.len() > BYTES {
            return Report {
                complete: false,
                malformed: true,
                records: 0,
                open: 0,
            };
        }
        let mut malformed = !bytes.is_empty() && !bytes.ends_with(b"\n");
        let mut stack = vec![];
        let mut seen = std::collections::HashSet::new();
        let mut identity = None;
        let mut children = std::collections::HashMap::new();
        let mut previous = 0;
        let mut ns = 0;
        let mut bound = false;
        let mut ready = false;
        let mut ended = false;
        let mut loss = false;
        let mut records = 0;
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            records += 1;
            if records > RECORDS {
                malformed = true;
                break;
            }
            let Ok(frame) = serde_json::from_slice::<Frame>(line) else {
                malformed = true;
                continue;
            };
            if line.len() + 1 > FRAME
                || records > RECORDS
                || ended
                || frame.v != 1
                || frame.clock != "worker-instant"
                || frame.pid == 0
                || frame.parent == 0
                || frame.wall_ms == 0
                || frame.seq != previous + 1
                || frame.ns < ns
            {
                malformed = true;
            }
            if records == 1
                && (frame.phase != Phase::Process
                    || frame.outcome != Outcome::Enter
                    || frame.span != Some(1)
                    || frame.host.is_some())
            {
                malformed = true;
            }
            if frame.phase != Phase::Process && frame.counts.is_some() {
                malformed = true;
            }
            previous = frame.seq;
            ns = frame.ns;
            let key = (frame.pid, frame.parent, frame.wall_ms);
            if identity.is_some_and(|i| i != key) {
                malformed = true;
            }
            identity = Some(key);
            match frame.outcome {
                Outcome::Enter => {
                    if let Some(span) = frame.span {
                        if span != frame.seq || !seen.insert(span) {
                            malformed = true;
                        }
                        stack.push((span, frame.phase, frame.host));
                    } else {
                        malformed = true;
                    }
                }
                Outcome::Return
                | Outcome::Error
                | Outcome::Unwind
                | Outcome::Abandoned
                | Outcome::Complete
                    if frame.span.is_some() =>
                {
                    let paired = stack.pop() == frame.span.map(|s| (s, frame.phase, frame.host));
                    if !paired {
                        malformed = true;
                    }
                    if frame.phase == Phase::Publication
                        && frame.outcome == Outcome::Return
                        && paired
                    {
                        ready = true;
                    }
                    loss |= frame.outcome != Outcome::Return && frame.outcome != Outcome::Complete;
                    if frame.phase == Phase::Process {
                        ended = true;
                        let valid = frame.counts.as_ref().is_some_and(|c| {
                            c.observed == frame.seq
                                && c.written + 1 == records as u64
                                && c.dropped == 0
                                && c.overflow == 0
                                && c.io == 0
                                && c.unmatched == 0
                        });
                        loss |= !valid || frame.outcome != Outcome::Complete || !stack.is_empty();
                    } else if frame.counts.is_some() {
                        malformed = true;
                    }
                }
                Outcome::Bound if frame.phase == Phase::Binding && frame.span.is_none() => {
                    let valid = frame.binding.as_ref().is_some_and(|b| {
                        !b.run.is_nil()
                            && [
                                (&b.descriptor, 64),
                                (&b.artifact, 64),
                                (&b.source, 64),
                                (&b.commit, 40),
                                (&b.tree, 40),
                            ]
                            .iter()
                            .all(|(v, n)| v.len() == *n && v.bytes().all(|c| c.is_ascii_hexdigit()))
                    });
                    malformed |= bound || !valid;
                    bound = true;
                }
                Outcome::Allocated | Outcome::Waited
                    if frame.phase == Phase::Child
                        && frame.span.is_none()
                        && frame.child.is_some_and(|pid| pid > 0)
                        && frame.host.is_some() =>
                {
                    let pid = frame.child.unwrap();
                    let host = frame.host.unwrap();
                    if frame.outcome == Outcome::Allocated {
                        malformed |= children.values().any(|h| *h == host)
                            || children.insert(pid, host).is_some();
                    } else {
                        malformed |= children.get(&pid) != Some(&host);
                    }
                }
                Outcome::Reached if frame.phase == Phase::Loop && frame.span.is_none() => {}
                Outcome::Error if frame.phase == Phase::Child && frame.span.is_none() => {
                    loss = true;
                }
                _ => {
                    malformed = true;
                }
            }
            if frame.phase != Phase::Binding && frame.binding.is_some() {
                malformed = true;
            }
            if frame.phase != Phase::Child
                && (frame.child.is_some() || frame.code.is_some() || frame.signal.is_some())
            {
                malformed = true;
            }
        }
        Report {
            complete: !malformed && !loss && ended && bound && ready && stack.is_empty(),
            malformed,
            records,
            open: stack.len(),
        }
    }
    #[test]
    fn native_startup_milestones_reader_bounds_and_privacy() {
        let dir = super::startup_test_directory("nsm-reader-");
        let session = Session::open(dir.path());
        let descriptor = DriverDescriptor {
            version: 1,
            run_id: uuid::Uuid::new_v4().to_string(),
            source_commit: "a".repeat(40),
            source_tree: "b".repeat(40),
            source_sha256: "c".repeat(64),
            executable_sha256: "d".repeat(64),
            lifetime_seconds: 90,
            scenarios: vec!["ready-stop".into()],
            directory: dir.path().into(),
        };
        // Deliberate controlled descriptor, not an executed driver/artifact claim.
        binding(&descriptor, b"controlled schema qualification");
        let value = call(Phase::Runtime, || 17);
        assert_eq!(value, 17);
        super::publish_driver(
            &dir.path().join("ready.json"),
            &json!({"secret":"not collected"}),
        )
        .unwrap();
        finish_ready();
        drop(session);
        let bytes = super::startup_test_read(dir.path(), "reader-valid-source-publish");
        let report = read(&bytes);
        assert!(report.complete, "{report:?}");
        assert!(!String::from_utf8_lossy(&bytes).contains("not collected"));
        assert!(!String::from_utf8_lossy(&bytes).contains(dir.path().to_str().unwrap()));
        let frames: Vec<Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect();
        let encode = |frames: &[Value]| {
            frames
                .iter()
                .fold(String::new(), |mut text, frame| {
                    writeln!(text, "{frame}").unwrap();
                    text
                })
                .into_bytes()
        };
        for terminal in ["complete", "unwind"] {
            let mut one = frames.last().unwrap().clone();
            one["seq"] = json!(1);
            one["span"] = Value::Null;
            one["outcome"] = json!(terminal);
            one["counts"]["observed"] = json!(1);
            one["counts"]["written"] = json!(0);
            let invalid = read(&encode(&[one]));
            assert!(invalid.malformed && !invalid.complete);
        }
        for subcase in [
            "missing",
            "duplicate",
            "order",
            "clock",
            "span",
            "unknown-field",
            "unknown-enum",
            "counter",
            "trailing",
            "child-host",
        ] {
            let mut altered = frames.clone();
            match subcase {
                "missing" => {
                    altered.remove(2);
                }
                "duplicate" => {
                    altered.insert(2, altered[1].clone());
                }
                "order" => {
                    altered.swap(2, 3);
                }
                "clock" => {
                    altered[2]["clock"] = json!("foreign-clock");
                }
                "span" => {
                    altered.last_mut().unwrap()["span"] = json!(999);
                }
                "unknown-field" => {
                    altered[2]["credential"] = json!("never accepted");
                }
                "unknown-enum" => {
                    altered[2]["phase"] = json!("arbitrary stage");
                }
                "counter" => {
                    altered.last_mut().unwrap()["counts"]["dropped"] = json!(1);
                }
                "trailing" => {
                    altered.push(altered[0].clone());
                }
                "child-host" => {
                    altered[2]["child"] = json!(42);
                }
                _ => unreachable!(),
            }
            let bad = read(&encode(&altered));
            assert!(!bad.complete, "{subcase}: {bad:?}");
        }
        let partial = read(&encode(&frames[..frames.len() - 1]));
        assert!(!partial.complete && !partial.malformed);
        let truncated = read(&bytes[..bytes.len() - 2]);
        assert!(truncated.malformed && !truncated.complete);
        let cap = super::startup_test_directory("nsm-cap-");
        let session = Session::open(cap.path());
        for _ in 0..100 {
            call(Phase::Runtime, || ());
        }
        finish_ready();
        drop(session);
        let bytes = super::startup_test_read(cap.path(), "cap-loss");
        assert!(bytes.len() <= BYTES);
        assert!(
            bytes
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .count()
                <= RECORDS
        );
        assert!(!read(&bytes).complete);
        assert!(String::from_utf8_lossy(&bytes).contains("\"overflow\":"));
        let full = std::fs::OpenOptions::new().write(true).open("/dev/full");
        let session = Session::with_file(full);
        let original: Result<(), u8> = result(Phase::Runtime, || Err(23));
        assert_eq!(original, Err(23));
        let counters = STATE.with(|s| {
            let s = s.borrow();
            let s = s.as_ref().unwrap();
            (s.counts.io, s.counts.dropped, s.failed)
        });
        eprintln!("startup real write failure: {counters:?}");
        assert!(counters.0 > 0 && counters.1 > 0 && counters.2);
        drop(session);
    }
    #[test]
    fn native_startup_milestones_destination_and_lifetime() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let dir = super::startup_test_directory("nsm-path-");
        let foreign = super::startup_test_directory("nsm-foreign-");
        let alias = dir.path().join("alias");
        symlink(foreign.path(), &alias).unwrap();
        let rejected = Session::open(&alias);
        assert!(!rejected.0);
        assert!(!foreign.path().join(FILE).exists());
        let target = foreign.path().join("sentinel");
        std::fs::write(&target, b"original").unwrap();
        symlink(&target, dir.path().join(FILE)).unwrap();
        let rejected = Session::open(dir.path());
        assert!(!rejected.0);
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        let mode = super::startup_test_directory("nsm-mode-");
        std::fs::set_permissions(mode.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!Session::open(mode.path()).0);
        assert!(!mode.path().join(FILE).exists());
        let valid = super::startup_test_directory("nsm-life-");
        let other = super::startup_test_directory("nsm-nested-");
        let session = Session::open(valid.path());
        assert!(session.0);
        assert!(!Session::open(other.path()).0);
        assert!(!other.path().join(FILE).exists());
        let metadata = std::fs::metadata(valid.path().join(FILE)).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        drop(session);
        let raw = super::startup_test_read(valid.path(), "once-only-drop");
        assert!(!read(&raw).complete);
        assert!(!Session::open(valid.path()).0);
        assert_eq!(std::fs::read(valid.path().join(FILE)).unwrap(), raw);
    }
}
// startup-milestones: end private producer and reader

// startup-milestones: begin finite source controls
fn startup_test_directory(prefix: &str) -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = common::test_tempdir_in("/tmp", prefix);
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
fn startup_test_descriptor(directory: &std::path::Path) -> (DriverDescriptor, Vec<u8>) {
    let identity = driver_identity();
    let value = json!({"version":1,"runId":uuid::Uuid::new_v4(),"sourceCommit":identity["sourceCommit"],
        "sourceTree":identity["sourceTree"],"sourceSha256":identity["sourceSha256"],
        "executableSha256":identity["executableSha256"],"lifetimeSeconds":90,"scenarios":["ready-stop"]});
    private_json(&directory.join("descriptor.json"), &value).unwrap();
    let bytes = std::fs::read(directory.join("descriptor.json")).unwrap();
    let descriptor = startup_milestones::result(startup_milestones::Phase::Validation, || {
        driver_descriptor_phase(&directory.join("descriptor.json"), false, true, false)
    })
    .unwrap();
    (descriptor, bytes)
}
fn startup_test_read(directory: &std::path::Path, label: &str) -> Vec<u8> {
    let bytes = std::fs::read(directory.join(startup_milestones::FILE)).unwrap();
    eprintln!(
        "startup-milestone-checkpoint {label}: {}",
        String::from_utf8_lossy(&bytes)
    );
    bytes
}
async fn startup_test_control(descriptor: &DriverDescriptor, phase: &str) -> Value {
    let mut connection = BufReader::new(
        UnixStream::connect(descriptor.directory.join("control.sock"))
            .await
            .unwrap(),
    );
    driver_write(
        &mut connection,
        &json!({"version":1,"runId":descriptor.run_id,"id":uuid::Uuid::new_v4(),
        "action":{"command":"stop","phase":phase,"pending":[],"envelopes":[]}}),
    )
    .await
    .unwrap();
    serde_json::from_slice(
        &timeout(Duration::from_secs(5), driver_frame(&mut connection))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
#[test]
fn native_startup_milestones_actual_startup() {
    use startup_milestones::{Phase, Session};
    let bootstrap = tokio::runtime::Runtime::new().unwrap();
    if bootstrap.block_on(run_in_tls_process(
        "native_startup_milestones_actual_startup",
    )) {
        return;
    }
    drop(bootstrap);
    let directory = startup_test_directory("nsm-host-");
    let evidence = startup_test_directory("nsm-proof-");
    // This control invokes original source functions, never the standalone driver
    // entry, supervisor, external controller, copied artifact or native operation.
    let session = Session::open(evidence.path());
    let (descriptor, bytes) = startup_test_descriptor(directory.path());
    startup_milestones::binding(&descriptor, &bytes);
    let observer = startup_milestones::call(Phase::Observer, || {
        CompanionFixtureObservation::install(directory.path())
    });
    let runtime = startup_milestones::result(Phase::Runtime, || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
    })
    .unwrap();
    let (result,replies,partial,ready)=runtime.block_on(async {
        let mut work=Box::pin(driver_loop(&descriptor));
        let first=futures_util::poll!(&mut work);
        let partial=startup_test_read(evidence.path(),"original-first-poll");
        eprintln!("startup original first poll pending: {}",first.is_pending());
        if let std::task::Poll::Ready(result)=first {return (result,Vec::new(),partial,Value::Null);}
        // Retaining the original future unpolled is the only hold. The prefix
        // identifies observed calls, not an internal mutex/timer wait instruction.
        let control=async {
            let ready=timeout(Duration::from_secs(25),async {
                loop {
                    if let Ok(bytes)=std::fs::read(directory.path().join("ready.json")) {break serde_json::from_slice::<Value>(&bytes).unwrap();}
                    // timing-guard: original atomic publication of this owned source control
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.unwrap();
            let begin=startup_test_control(&descriptor,"begin").await;
            let finish=startup_test_control(&descriptor,"finish").await;
            (ready,vec![begin,finish])
        };
        let (result,(ready,replies))=futures_util::future::join(work,control).await;
        eprintln!("startup original ready observed: {}",json!({"runId":ready["runId"],"identity":ready["identity"],"hosts":ready["hosts"].as_array().map(Vec::len),"pid":ready["pid"]}));
        (result,replies,partial,ready)
    });
    let raw = startup_test_read(evidence.path(), "original-ready-and-source-join");
    let stopped = std::fs::read(directory.path().join("worker-stopped.json"))
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok());
    eprintln!(
        "startup original result/stop: {}",
        json!({"returnedOk":result.is_ok(),"replies":replies,"stopped":stopped.as_ref().map(|s|json!({"success":s["success"],"cleanup":s["cleanup"]}))})
    );
    drop(runtime);
    drop(observer);
    drop(session);
    let report = startup_milestones::read(&raw);
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(ready["runId"], descriptor.run_id);
    assert_eq!(ready["pid"], std::process::id());
    assert_eq!(ready["hosts"].as_array().unwrap().len(), 2);
    for (field, expected) in [
        ("sourceCommit", &descriptor.source_commit),
        ("sourceTree", &descriptor.source_tree),
        ("sourceSha256", &descriptor.source_sha256),
        ("executableSha256", &descriptor.executable_sha256),
    ] {
        assert_eq!(ready["identity"][field], *expected);
    }
    assert_eq!(replies.len(), 2);
    assert!(replies.iter().all(|r| r.get("error").is_none()));
    assert!(report.complete, "{report:?}");
    assert!(!report.malformed);
    let pending = startup_milestones::read(&partial);
    assert!(!pending.complete);
    assert!(pending.open > 0);
    let frames: Vec<Value> = raw
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect();
    for host in ["a", "b"] {
        assert!(frames
            .iter()
            .any(|f| f["phase"] == "host" && f["host"] == host && f["outcome"] == "return"));
        let allocated: Vec<_> = frames
            .iter()
            .filter(|f| f["phase"] == "child" && f["host"] == host && f["outcome"] == "allocated")
            .collect();
        assert_eq!(allocated.len(), 1);
        assert!(stopped.as_ref().unwrap()["cleanup"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["fixturePid"] == allocated[0]["child"]
                && c["reaped"] == true
                && c["udsClosed"] == true
                && c["tcpClosed"] == true));
    }
    assert_eq!(stopped.as_ref().unwrap()["success"], true);
    assert!(report.records <= 96 && raw.len() <= 48 * 1024);
}
#[test]
fn native_startup_milestones_publication_failures_and_off() {
    use startup_milestones::{Phase, Session};
    use std::io::{Read as _, Seek as _};
    let off = startup_test_directory("nsm-off-");
    let value = json!({"sentinel":7});
    publish_driver(&off.path().join("ready.json"), &value).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(off.path().join("ready.json")).unwrap())
            .unwrap(),
        value
    );
    assert!(!off.path().join(startup_milestones::FILE).exists());
    for existing in [true, false] {
        let dir = startup_test_directory("nsm-error-");
        let evidence = startup_test_directory("nsm-error-proof-");
        let session = Session::open(evidence.path());
        let path = if existing {
            dir.path().join("ready.json")
        } else {
            dir.path().join("absent/ready.json")
        };
        if existing {
            private_json(&path, &value).unwrap();
        }
        let result = publish_driver(&path, &json!({"replacement":true}));
        let partial = startup_test_read(evidence.path(), "original-publication-error");
        eprintln!(
            "startup publication error category: {}",
            result
                .as_ref()
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind()
        );
        assert!(result.is_err());
        assert!(!startup_milestones::read(&partial).complete);
        if existing {
            assert_eq!(
                serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
                value
            );
        } else {
            assert!(!path.exists());
        }
        drop(session);
        let report = startup_milestones::read(&startup_test_read(
            evidence.path(),
            "publication-failure-final",
        ));
        assert!(!report.complete);
        assert!(!report.malformed, "{report:?}");
    }
    let dir = startup_test_directory("nsm-unwind-");
    let evidence = startup_test_directory("nsm-unwind-proof-");
    let session = Session::open(evidence.path());
    let file = dir.path().join("not-a-repository");
    std::fs::write(&file, b"owned").unwrap();
    // The existing panic hook renames registered test directories before catch_unwind
    // returns. Retain this original file handle, not a guessed post-mortem path.
    let mut original_journal =
        std::fs::File::open(evidence.path().join(startup_milestones::FILE)).unwrap();
    let panic = std::panic::catch_unwind(|| {
        startup_milestones::call(Phase::Repositories, || init_repo(&file));
    });
    let mut raw = Vec::new();
    original_journal.read_to_end(&mut raw).unwrap();
    eprintln!(
        "startup-milestone-checkpoint original-git-panic-owned-handle: {}",
        String::from_utf8_lossy(&raw)
    );
    assert!(panic.is_err());
    assert!(String::from_utf8_lossy(&raw).contains("\"outcome\":\"unwind\""));
    drop(session);
    original_journal.rewind().unwrap();
    raw.clear();
    original_journal.read_to_end(&mut raw).unwrap();
    eprintln!(
        "startup-milestone-checkpoint unwind-final-owned-handle: {}",
        String::from_utf8_lossy(&raw)
    );
    let report = startup_milestones::read(&raw);
    assert!(!report.complete && !report.malformed, "{report:?}");
}
#[intent_test_macros::daemon_test]
async fn native_startup_milestones_original_child_exit() {
    use startup_milestones::{Host, Session};
    use std::os::unix::process::ExitStatusExt;
    if run_in_tls_process("native_startup_milestones_original_child_exit").await {
        return;
    }
    let dir = startup_test_directory("nsm-child-");
    let evidence = startup_test_directory("nsm-child-proof-");
    let root = dir.path().join("repo");
    std::fs::create_dir(&root).unwrap();
    init_repo(&root);
    let session = Session::open(evidence.path());
    let host = startup_milestones::host(Host::A);
    let (mut child, _, _, _) = remote_fixture(dir.path(), &root).await;
    let pid = child.id();
    child.kill().unwrap();
    let wait = startup_milestones::waited(pid, child.wait(), |s| (s.code(), s.signal()));
    let raw = startup_test_read(evidence.path(), "actual-child-wait");
    eprintln!(
        "startup original child wait: {}",
        json!({"pid":pid,"waited":wait.is_ok(),"code":wait.as_ref().ok().and_then(std::process::ExitStatus::code),"signal":wait.as_ref().ok().and_then(std::os::unix::process::ExitStatusExt::signal)})
    );
    host.returned();
    drop(session);
    assert!(wait.is_ok());
    assert!(!startup_milestones::read(&raw).complete);
    let frames: Vec<Value> = raw
        .split(|b| *b == b'\n')
        .filter(|b| !b.is_empty())
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect();
    assert!(frames
        .iter()
        .any(|f| f["outcome"] == "allocated" && f["child"] == pid && f["host"] == "a"));
    assert!(frames
        .iter()
        .any(|f| f["outcome"] == "waited" && f["child"] == pid && f["host"] == "a"));
}
// startup-milestones: end finite source controls
