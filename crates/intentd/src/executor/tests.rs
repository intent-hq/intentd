//! The executor service end to end over a real link on the daemon's WSS
//! listener: the backend (the test) dials with the certificate pin and an
//! executor credential, binds, launches, reads the journal stream,
//! acknowledges and re-binds.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::broadcast;

use intent_acp::PermissionPolicy;
use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{
    BindDecision, JournalBody, JournalRecord, LaunchCommand, LeaseBinding, LinkFrame, LinkHello,
    MachineCapacity, ProviderStatus, NODE_LINK_PROTOCOLS,
};
use intent_core::nodes::NodeCounter;
use intent_core::placement::CapacityRefusal;
use intent_core::{AgentId, BoxFuture, HostRole, PrincipalId, WorkspaceApi, WorkspaceId};
use intent_transport::node_link::{
    dial_link, CertificatePin, DialedLink, ExecutorLinks, ExecutorToken, LinkEvent, NodeEndpoint,
};

use super::*;

const NODE_ID: &str = "node-1";
const HEAD: &str = "head-1";
const OTHER_HEAD: &str = "head-2";

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

fn token() -> String {
    "k".repeat(48)
}

fn other_token() -> String {
    "q".repeat(48)
}

/// This daemon's store seams, in memory: two executor credentials, one per
/// backend, bound by the store's generation rule.
struct MachineApi {
    principal: PrincipalId,
    credentials: HashMap<String, ExecutorCredential>,
    binds: Mutex<HashMap<ExecutorCredentialId, LeaseBinding>>,
    revocations: broadcast::Sender<ExecutorCredentialId>,
    /// Revoked in the store, whether or not the feed carried it.
    revoked: Mutex<std::collections::HashSet<ExecutorCredentialId>>,
}

impl MachineApi {
    fn new() -> Arc<Self> {
        let credential = |backend: &str| ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: backend.into(),
            backend_name: "Backend".into(),
            backend_fingerprint: None,
            created_at: "2026-10-31T00:00:00Z".into(),
            last_used_at: None,
            revoked_at: None,
        };
        Arc::new(Self {
            principal: PrincipalId::new(),
            credentials: HashMap::from([
                (
                    intent_transport::auth::hash_token(&token()),
                    credential(HEAD),
                ),
                (
                    intent_transport::auth::hash_token(&other_token()),
                    credential(OTHER_HEAD),
                ),
            ]),
            binds: Mutex::new(HashMap::new()),
            revocations: broadcast::channel(16).0,
            revoked: Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// The credentials still active in the store.
    fn active(&self) -> std::collections::HashSet<ExecutorCredentialId> {
        let revoked = self.revoked.lock().unwrap();
        self.credentials
            .values()
            .map(|c| c.id.clone())
            .filter(|id| !revoked.contains(id))
            .collect()
    }

    fn credential(&self, token: &str) -> ExecutorCredential {
        self.credentials[&intent_transport::auth::hash_token(token)].clone()
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

    fn resolve_executor_credential(
        &self,
        token_hash: String,
    ) -> BoxFuture<'_, intent_core::Result<Option<ExecutorCredential>>> {
        Box::pin(async move { Ok(self.credentials.get(&token_hash).cloned()) })
    }

    fn bind_executor(
        &self,
        credential_id: ExecutorCredentialId,
        node_id: String,
        offered: LeaseBinding,
    ) -> BoxFuture<'_, intent_core::Result<BindDecision>> {
        Box::pin(async move {
            let credential = self
                .credentials
                .values()
                .find(|c| c.id == credential_id)
                .cloned()
                .ok_or_else(|| intent_core::Error::NotFound("executor credential".into()))?;
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

struct OwnerToken;

impl intent_transport::TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some("o".repeat(64))
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

/// The machine's shared limits as a plain counter: what the test admits as
/// "local" agents counts against remote launches too.
struct CountAdmission {
    cap: u32,
    running: Mutex<Vec<AgentId>>,
}

impl CountAdmission {
    fn new(cap: u32) -> Arc<Self> {
        Arc::new(Self {
            cap,
            running: Mutex::new(Vec::new()),
        })
    }
}

impl Admission for CountAdmission {
    fn try_admit(&self, agent_id: &AgentId) -> Result<(), CapacityRefusal> {
        let mut running = self.running.lock().unwrap();
        let used = u32::try_from(running.len()).unwrap();
        if used >= self.cap {
            return Err(CapacityRefusal::Count {
                running_agents: used,
                agent_cap: self.cap,
            });
        }
        running.push(agent_id.clone());
        Ok(())
    }
    fn release(&self, agent_id: &AgentId) {
        self.running.lock().unwrap().retain(|id| id != agent_id);
    }
    fn capacity(&self) -> MachineCapacity {
        MachineCapacity {
            running_agents: u32::try_from(self.running.lock().unwrap().len()).unwrap(),
            agent_cap: self.cap,
            charged_memory_bytes: 0,
            memory_budget_bytes: None,
        }
    }
}

/// Providers by id; anything else is "not installed".
struct FixedLaunch(BTreeMap<String, ProviderLaunch>);

impl MachineLaunch for FixedLaunch {
    fn resolve(&self, provider: &str) -> Result<ProviderLaunch, String> {
        self.0
            .get(provider)
            .cloned()
            .ok_or_else(|| "not installed".to_owned())
    }
    fn policy(&self) -> PermissionPolicy {
        PermissionPolicy::AllowAll
    }
}

/// A fixed report; counts refreshes and records default-account auth
/// failures, after which its provider reports `auth-failed`.
struct FixedReport {
    refreshes: std::sync::atomic::AtomicUsize,
    primes: std::sync::atomic::AtomicUsize,
    auth_failures: Mutex<Vec<String>>,
}

impl MachineReport for FixedReport {
    fn providers(&self) -> Vec<ProviderCapability> {
        let failed = lock(&self.auth_failures).iter().any(|p| p == "mock");
        vec![ProviderCapability {
            id: "mock".into(),
            visible: true,
            status: if failed {
                ProviderStatus::AuthFailed
            } else {
                ProviderStatus::Unknown
            },
            models: vec!["mock-model".into()],
            models_stale: false,
        }]
    }
    fn routes(&self) -> (Vec<String>, Option<String>) {
        (vec!["10.0.0.7".into()], Some("tc-machine".into()))
    }
    fn refresh(&self) -> intent_core::BoxFuture<'_, ()> {
        self.refreshes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {})
    }
    fn prime(&self) {
        self.primes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn launch_auth_failed(&self, provider: &str) {
        lock(&self.auth_failures).push(provider.to_owned());
    }
}

/// The fixture agent as the `mock` provider, when `node` is on PATH.
fn fixture_providers() -> Option<BTreeMap<String, ProviderLaunch>> {
    fixture_providers_with(&[])
}

/// [`fixture_providers`], the fixture run with `args`.
fn fixture_providers_with(args: &[&str]) -> Option<BTreeMap<String, ProviderLaunch>> {
    let node = intent_providers::resolve_on_path("node")?;
    let script = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut base_args = vec![&*Box::leak(script.into_boxed_str())];
    base_args.extend(
        args.iter()
            .map(|a| &*Box::leak((*a).to_owned().into_boxed_str())),
    );
    let base_args: &'static [&'static str] = Box::leak(base_args.into_boxed_slice());
    Some(BTreeMap::from([(
        "mock".to_owned(),
        ProviderLaunch {
            config: intent_providers::ProviderConfig {
                base_args,
                ..*intent_providers::find_provider("mock").unwrap()
            },
            binary: Some(node),
            npx_fallback_binary: None,
            npx_fallback_package: None,
            extra_env: BTreeMap::new(),
        },
    )]))
}

/// A daemon serving the executor link, with the executor service installed.
struct Running {
    /// `remoteAgents.enabled`.
    enabled: Arc<std::sync::atomic::AtomicBool>,
    server: intent_transport::WsApiServer,
    service: Arc<ExecutorService>,
    api: Arc<MachineApi>,
    report: Arc<FixedReport>,
    endpoint: NodeEndpoint,
    _bus_dir: tempfile::TempDir,
}

impl Running {
    async fn start(data_dir: &std::path::Path, cap: u32) -> Self {
        Self::start_with(
            data_dir,
            MachineApi::new(),
            CountAdmission::new(cap),
            BTreeMap::new(),
        )
        .await
    }

    async fn start_with(
        data_dir: &std::path::Path,
        api: Arc<MachineApi>,
        admission: Arc<CountAdmission>,
        providers: BTreeMap<String, ProviderLaunch>,
    ) -> Self {
        let report = Arc::new(FixedReport {
            refreshes: std::sync::atomic::AtomicUsize::new(0),
            primes: std::sync::atomic::AtomicUsize::new(0),
            auth_failures: Mutex::new(Vec::new()),
        });
        let enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));
        // Agents work in plain directories here: no backend serves git. The
        // git checkout path is covered by the e2e tests.
        let service = ExecutorService::start_with_checkouts(
            ExecutorDeps {
                data_dir: data_dir.to_owned(),
                node_id: NODE_ID.into(),
                enabled: {
                    let enabled = enabled.clone();
                    Arc::new(move || enabled.load(std::sync::atomic::Ordering::SeqCst))
                },
                admission,
                launch: Arc::new(FixedLaunch(providers)),
                report: report.clone(),
                executable: std::env::current_exe().unwrap(),
                active_credentials: {
                    let api = api.clone();
                    Arc::new(move || {
                        let api = api.clone();
                        Box::pin(async move { Ok(api.active()) })
                    })
                },
            },
            api.subscribe_executor_revocations(),
            |storage, _cache| checkout::PlainCheckouts::new(storage),
        )
        .unwrap();
        let bus_dir = tempfile::tempdir().unwrap();
        let bus = intent_services::EventBus::new(
            intent_store::Store::open(&bus_dir.path().join("bus.db"))
                .await
                .unwrap(),
        );
        let certificate = intent_transport::generate_certificate(vec!["127.0.0.1".into()]).unwrap();
        let tokens = Arc::new(intent_transport::AsyncTokenStore::new(Arc::new(OwnerToken)));
        let options = intent_transport::WsOptions {
            base_port: 0,
            ..intent_transport::WsOptions::default()
        };
        let mut server = intent_transport::WsApiServer::new(
            api.clone(),
            bus,
            &certificate,
            &tokens,
            options,
            None,
        )
        .unwrap();
        let links = ExecutorLinks::new(service.clone());
        service.attach_links(&links);
        server.install_executor_links(links);
        let port = server.start().await.unwrap();
        let endpoint = NodeEndpoint {
            url: format!("wss://127.0.0.1:{port}"),
            pin: CertificatePin::parse(&certificate.fingerprint256).unwrap(),
            token: ExecutorToken::new(token()).unwrap(),
            tailcat: None,
        };
        Self {
            enabled,
            server,
            service,
            api,
            report,
            endpoint,
            _bus_dir: bus_dir,
        }
    }

    fn endpoint_for(&self, token: &str) -> NodeEndpoint {
        NodeEndpoint {
            token: ExecutorToken::new(token).unwrap(),
            ..self.endpoint.clone()
        }
    }

    async fn stop(self) -> Arc<MachineApi> {
        self.server.stop().await;
        self.service.shutdown().await;
        self.api
    }
}

fn hello(generation: u64, lease: &str, acked: u64) -> LinkHello {
    hello_from(HEAD, generation, lease, acked)
}

fn hello_from(head: &str, generation: u64, lease: &str, acked: u64) -> LinkHello {
    LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "test".into(),
        binding: LeaseBinding {
            head_id: head.into(),
            node_id: NODE_ID.into(),
            lease_id: lease.into(),
            generation: NodeCounter(generation),
        },
        acked_seq: NodeCounter(acked),
    }
}

fn launch_of(id: &str, provider: &str) -> LinkFrame {
    LinkFrame::Launch(LaunchCommand {
        agent_id: AgentId::from(id),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: provider.into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        profile_env: None,
        agent_features: serde_json::json!({}),
        is_sub_agent: false,
    })
}

/// A launch the machine refuses without spawning anything (no such
/// provider), which journals exactly one exit record.
fn refused_launch(id: &str) -> LinkFrame {
    launch_of(id, "codex")
}

async fn next_frame(dialed: &mut DialedLink) -> Option<LinkFrame> {
    loop {
        match tokio::time::timeout(wait(), dialed.events.recv())
            .await
            .expect("timed out waiting for a frame")
        {
            Some(LinkEvent::Frame(frame)) => return Some(frame),
            Some(LinkEvent::Request(_)) => {}
            None => return None,
        }
    }
}

async fn next_record(dialed: &mut DialedLink) -> JournalRecord {
    match next_frame(dialed).await {
        Some(LinkFrame::Journal(record)) => record,
        other => panic!("expected a journal record, got {other:?}"),
    }
}

async fn replay_complete(dialed: &mut DialedLink) -> NodeCounter {
    match next_frame(dialed).await {
        Some(LinkFrame::ReplayComplete { through_seq }) => through_seq,
        other => panic!("expected replay complete, got {other:?}"),
    }
}

#[tokio::test]
async fn executor_binds_reports_capabilities_and_streams_launch_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 3).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(dialed.welcome.node_id, NODE_ID);
    assert_eq!(dialed.welcome.capabilities.capacity.agent_cap, 3);
    assert_eq!(dialed.welcome.capabilities.providers[0].id, "mock");
    assert_eq!(
        dialed.welcome.capabilities.providers[0].models,
        ["mock-model"]
    );
    assert_eq!(dialed.welcome.hosts, ["10.0.0.7"]);
    assert_eq!(dialed.welcome.tc_address.as_deref(), Some("tc-machine"));
    assert_eq!(
        dialed.welcome.capabilities.platform,
        host_platform().unwrap()
    );
    assert!(dialed.welcome.running_agents.is_empty());
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));

    dialed.link.send(&refused_launch("a1")).await.unwrap();
    let record = next_record(&mut dialed).await;
    assert_eq!(record.seq, NodeCounter(1));
    assert_eq!(record.agent_id, AgentId::from("a1"));
    assert!(
        matches!(&record.body, JournalBody::Exited { error: Some(e), .. }
            if e.starts_with(runner::codes::PROVIDER)),
        "{record:?}"
    );
    node.stop().await;
}

#[tokio::test]
async fn executor_replays_unacknowledged_records_after_rebind_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    for id in ["a1", "a2", "a3"] {
        first.link.send(&refused_launch(id)).await.unwrap();
    }
    for seq in 1..=3 {
        assert_eq!(next_record(&mut first).await.seq, NodeCounter(seq));
    }
    first
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(1),
        })
        .await
        .unwrap();

    // A newer generation replaces the link; head says it ingested 2.
    let mut second = dial_link(&node.endpoint, hello(2, "lease-1", 2))
        .await
        .unwrap();
    assert!(next_frame(&mut first).await.is_none(), "older link closed");
    assert_eq!(next_record(&mut second).await.seq, NodeCounter(3));
    assert_eq!(replay_complete(&mut second).await, NodeCounter(3));
    // An older generation can no longer bind.
    assert!(dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .is_err());
    let api = node.stop().await;

    // The unacknowledged record survives a daemon restart.
    let node = Running::start_with(dir.path(), api, CountAdmission::new(2), BTreeMap::new()).await;
    let mut third = dial_link(&node.endpoint, hello(3, "lease-1", 2))
        .await
        .unwrap();
    let replayed = next_record(&mut third).await;
    assert_eq!(replayed.seq, NodeCounter(3));
    assert_eq!(replayed.agent_id, AgentId::from("a3"));
    assert_eq!(replay_complete(&mut third).await, NodeCounter(3));
    third
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(3),
        })
        .await
        .unwrap();
    // New records continue the sequence.
    third.link.send(&refused_launch("a4")).await.unwrap();
    assert_eq!(next_record(&mut third).await.seq, NodeCounter(4));
    node.stop().await;
}

#[tokio::test]
async fn executor_refuses_a_link_that_acknowledges_unwritten_records() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 7))
        .await
        .unwrap();
    assert!(next_frame(&mut dialed).await.is_none());

    // A good link still works afterwards.
    let mut dialed = dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));
    // So does a bad acknowledgement on a live link: it is closed.
    dialed
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(9),
        })
        .await
        .unwrap();
    assert!(next_frame(&mut dialed).await.is_none());
    node.stop().await;
}

#[tokio::test]
async fn executor_restarts_the_sequence_for_a_new_lease() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    first.link.send(&refused_launch("a1")).await.unwrap();
    assert_eq!(next_record(&mut first).await.seq, NodeCounter(1));

    let mut second = dial_link(&node.endpoint, hello(2, "lease-2", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut second).await, NodeCounter(0));
    second.link.send(&refused_launch("a2")).await.unwrap();
    let record = next_record(&mut second).await;
    assert_eq!(
        (record.seq, record.agent_id),
        (NodeCounter(1), AgentId::from("a2"))
    );
    node.stop().await;
}

/// Requests run only from the machine to the backend: a backend-initiated
/// request is outside the executor frame list and closes the link.
#[tokio::test]
async fn executor_closes_a_link_on_a_backend_request() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 1).await;
    let dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    let error = dialed
        .link
        .call(
            AgentId::from("a1"),
            "workspace.read",
            serde_json::json!({}),
            wait(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        intent_transport::node_link::error_code(&error),
        Some(intent_transport::node_link::codes::CLOSED),
        "{error}"
    );
    node.stop().await;
}

/// Launches that keep arriving on a live link while the node shuts down
/// are either stopped by that shutdown or refused: every agent that left a
/// record has its exit journaled, and no provider process survives.
#[tokio::test]
async fn executor_shutdown_settles_launches_arriving_on_a_live_link() {
    let Some(providers) = fixture_providers() else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let api = MachineApi::new();
    let root = dir
        .path()
        .join("executor")
        .join(&api.credential(&token()).id.0);
    let node = Running::start_with(dir.path(), api, CountAdmission::new(8), providers).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    let launch = |id: String| {
        LinkFrame::Launch(LaunchCommand {
            agent_id: AgentId::from(id),
            workspace_id: WorkspaceId::from("ws-1"),
            provider: "mock".into(),
            model: None,
            base_commit: "base".into(),
            repo_key: "repo".into(),
            profile_env: None,
            agent_features: serde_json::json!({}),
            is_sub_agent: false,
        })
    };
    dialed.link.send(&launch("a0".into())).await.unwrap();
    dialed
        .link
        .send(&LinkFrame::Prompt {
            agent_id: AgentId::from("a0"),
            turn_id: "t1".into(),
            prompt: vec![serde_json::json!({ "type": "text", "text": "many" })],
        })
        .await
        .unwrap();
    // Shut down while a0's burst is being journaled (after its Started).
    let mut first = next_record(&mut dialed).await;
    if matches!(first.body, JournalBody::Started { .. }) {
        first = next_record(&mut dialed).await;
    }
    assert!(matches!(first.body, JournalBody::Notification { .. }));
    let stopping = tokio::spawn(node.stop());
    for i in 1..=20 {
        if dialed.link.send(&launch(format!("late{i}"))).await.is_err() {
            break;
        }
    }
    stopping.await.unwrap();

    let journal = Journal::open(&root.join("journal")).unwrap();
    let records = journal.records_after(NodeCounter(0)).unwrap();
    let mut agents: Vec<&str> = records.iter().map(|r| r.agent_id.0.as_str()).collect();
    agents.sort_unstable();
    agents.dedup();
    assert!(agents.contains(&"a0"), "{records:#?}");
    for agent in agents {
        let exits = records
            .iter()
            .filter(|r| r.agent_id.0 == agent && matches!(r.body, JournalBody::Exited { .. }))
            .count();
        assert_eq!(exits, 1, "{agent}: {records:#?}");
    }
    for entry in std::fs::read_dir(root.join("agents")).unwrap() {
        let pid_file = entry.unwrap().path().join("work").join("agent.pid");
        let Ok(bytes) = std::fs::read(&pid_file) else {
            continue;
        };
        let pids: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for key in ["agent", "child"] {
            let pid = i32::try_from(pids[key].as_i64().unwrap()).unwrap();
            let deadline = tokio::time::Instant::now() + wait();
            while !process_gone(pid) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} {key} {pid} survived shutdown",
                    pid_file.display()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

/// Whether `pid` has exited (a zombie awaiting its reaper counts).
fn process_gone(pid: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => stat
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z')),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
    }
}

#[test]
fn executor_platform_is_linux_or_macos_on_x86_64_or_aarch64() {
    assert_eq!(
        platform_of("linux", "x86_64").unwrap(),
        AgentPlatform {
            os: NodeOs::Linux,
            arch: NodeArch::X86_64
        }
    );
    assert_eq!(
        platform_of("macos", "aarch64").unwrap(),
        AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64
        }
    );
    assert!(platform_of("windows", "x86_64").is_err());
    assert!(platform_of("linux", "riscv64").is_err());
}

/// A data dir too deep for a Unix socket path (`SUN_LEN`) still serves its
/// backends: the git helper socket moves to a short private path.
#[tokio::test]
async fn a_long_data_dir_still_binds_and_launches() {
    let dir = tempfile::tempdir().unwrap();
    let mut data_dir = dir.path().to_owned();
    while data_dir.as_os_str().len() < 160 {
        data_dir.push("a-rather-long-data-dir-segment");
    }
    std::fs::create_dir_all(&data_dir).unwrap();
    let node = Running::start(&data_dir, 3).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));
    dialed.link.send(&refused_launch("a1")).await.unwrap();
    let record = next_record(&mut dialed).await;
    assert_eq!(record.agent_id, AgentId::from("a1"));
    let socket = storage::git_socket_path(
        &data_dir.join(EXECUTOR_DIR),
        &node.api.credential(&token()).id.0,
        &storage::runtime_base(),
    )
    .unwrap();
    assert!(socket.exists(), "{}", socket.display());
    node.stop().await;
    // The socket sits in a shared runtime directory: it goes with its backend.
    assert!(!socket.exists(), "{}", socket.display());
}

/// Each backend (credential) has its own storage, journal and sequence
/// under `<data_dir>/executor/<credentialId>/`.
#[tokio::test]
async fn each_credential_gets_its_own_storage_and_journal() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 4).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    first.link.send(&refused_launch("a1")).await.unwrap();
    first.link.send(&refused_launch("a2")).await.unwrap();
    assert_eq!(next_record(&mut first).await.seq, NodeCounter(1));
    assert_eq!(next_record(&mut first).await.seq, NodeCounter(2));

    let mut other = dial_link(
        &node.endpoint_for(&other_token()),
        hello_from(OTHER_HEAD, 1, "lease-1", 0),
    )
    .await
    .unwrap();
    assert_eq!(
        replay_complete(&mut other).await,
        NodeCounter(0),
        "the other backend sees none of the first one's records"
    );
    other.link.send(&refused_launch("b1")).await.unwrap();
    let record = next_record(&mut other).await;
    assert_eq!(
        (record.seq, record.agent_id),
        (NodeCounter(1), AgentId::from("b1"))
    );
    for token in [token(), other_token()] {
        let root = dir
            .path()
            .join("executor")
            .join(&node.api.credential(&token).id.0);
        assert!(root.join("journal").is_dir(), "{}", root.display());
    }
    // A credential bound to one backend cannot be used by another.
    assert!(
        dial_link(&node.endpoint, hello_from(OTHER_HEAD, 9, "lease-1", 0))
            .await
            .is_err()
    );
    node.stop().await;
}

/// Remote agents of every backend and the machine's own local agents share
/// one set of limits; a launch over them is refused at once with the
/// structured `node-capacity` failure, never queued.
#[tokio::test]
async fn local_and_remote_agents_share_the_limits_and_over_limit_launches_are_refused() {
    let Some(providers) = fixture_providers() else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let admission = CountAdmission::new(2);
    // A local agent of this machine holds one of the two slots.
    admission.try_admit(&AgentId::from("local-agent")).unwrap();
    let node =
        Running::start_with(dir.path(), MachineApi::new(), admission.clone(), providers).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    assert_eq!(first.welcome.capabilities.capacity.running_agents, 1);
    first.link.send(&launch_of("a1", "mock")).await.unwrap();
    assert!(matches!(
        next_record(&mut first).await.body,
        JournalBody::Started { .. }
    ));

    // The other backend finds the machine full.
    let mut other = dial_link(
        &node.endpoint_for(&other_token()),
        hello_from(OTHER_HEAD, 1, "lease-1", 0),
    )
    .await
    .unwrap();
    replay_complete(&mut other).await;
    assert_eq!(other.welcome.capabilities.capacity.running_agents, 2);
    other.link.send(&launch_of("b1", "mock")).await.unwrap();
    let refusal = next_record(&mut other).await;
    match &refusal.body {
        JournalBody::Exited {
            error: Some(error),
            failure: Some(failure),
            ..
        } => {
            assert!(error.starts_with(runner::codes::CAPACITY), "{error}");
            assert_eq!(
                failure.data,
                Some(CapacityRefusal::Count {
                    running_agents: 2,
                    agent_cap: 2,
                })
            );
        }
        other => panic!("expected a capacity refusal, got {other:?}"),
    }
    assert_eq!(
        node.service.status(&node.api.credential(&other_token()).id),
        ExecutorLiveStatus {
            connected: true,
            running_agents: 0,
        },
        "nothing was queued for the refused backend"
    );
    assert_eq!(
        node.service
            .status(&node.api.credential(&token()).id)
            .running_agents,
        1
    );

    // The local agent exits: the freed slot admits the other backend.
    admission.release(&AgentId::from("local-agent"));
    other.link.send(&launch_of("b2", "mock")).await.unwrap();
    assert!(matches!(
        next_record(&mut other).await.body,
        JournalBody::Started { .. }
    ));
    node.stop().await;
    assert!(
        admission.running.lock().unwrap().is_empty(),
        "stopping the service released every remote slot"
    );
}

/// A capabilities request refreshes the report and answers with the
/// shared limits and providers.
#[tokio::test]
async fn executor_answers_a_capabilities_request_with_a_fresh_report() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 3).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&LinkFrame::Capabilities).await.unwrap();
    match next_frame(&mut dialed).await {
        Some(LinkFrame::CapabilitiesReply { capabilities }) => {
            assert_eq!(capabilities.capacity.agent_cap, 3);
            assert_eq!(capabilities.providers[0].models, ["mock-model"]);
        }
        other => panic!("expected a capabilities reply, got {other:?}"),
    }
    assert_eq!(
        node.report
            .refreshes
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    node.stop().await;
}

/// A remote launch without a profile that the provider refuses as not
/// signed in is reported to the machine report, so the next capabilities
/// reply says `auth-failed`.
#[tokio::test]
async fn a_default_account_auth_failure_reaches_the_capabilities_report() {
    let Some(providers) = fixture_providers_with(&["--auth-required-setup"]) else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start_with(
        dir.path(),
        MachineApi::new(),
        CountAdmission::new(3),
        providers,
    )
    .await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&launch_of("a1", "mock")).await.unwrap();
    loop {
        let record = next_record(&mut dialed).await;
        if matches!(record.body, JournalBody::Exited { .. }) {
            break;
        }
    }
    assert_eq!(*lock(&node.report.auth_failures), ["mock"]);
    dialed.link.send(&LinkFrame::Capabilities).await.unwrap();
    loop {
        match next_frame(&mut dialed).await {
            Some(LinkFrame::CapabilitiesReply { capabilities }) => {
                assert_eq!(capabilities.providers[0].status, ProviderStatus::AuthFailed);
                break;
            }
            Some(LinkFrame::Journal(_)) => {}
            other => panic!("expected a capabilities reply, got {other:?}"),
        }
    }
    node.stop().await;
}

/// Revoking a credential stops its backend's agents here and forgets its
/// live state.
#[tokio::test]
async fn revoking_a_credential_stops_its_backends_agents() {
    let Some(providers) = fixture_providers() else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let admission = CountAdmission::new(4);
    let node =
        Running::start_with(dir.path(), MachineApi::new(), admission.clone(), providers).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&launch_of("a1", "mock")).await.unwrap();
    assert!(matches!(
        next_record(&mut dialed).await.body,
        JournalBody::Started { .. }
    ));
    let id = node.api.credential(&token()).id;
    assert_eq!(node.service.status(&id).running_agents, 1);
    node.api.revocations.send(id.clone()).unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    while !admission.running.lock().unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "agents still run");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(node.service.status(&id), ExecutorLiveStatus::default());
    node.stop().await;
}

/// Before the service is installed (the listener starts before the store
/// is ready) the slot reports the executor link as disabled.
#[test]
fn the_slot_is_disabled_until_the_service_is_installed() {
    let slot = ExecutorSlot::default();
    assert!(!slot.enabled());
    assert!(slot.service().is_none());
    assert_eq!(
        slot.status(&ExecutorCredentialId::new()),
        ExecutorLiveStatus::default()
    );
}

/// Starts a backend with one running fixture agent; returns the service,
/// its admission, the backend's credential and its dialed link.
async fn running_agent(
    dir: &std::path::Path,
) -> Option<(
    Running,
    Arc<CountAdmission>,
    ExecutorCredentialId,
    DialedLink,
)> {
    let providers = fixture_providers()?;
    let admission = CountAdmission::new(4);
    let node = Running::start_with(dir, MachineApi::new(), admission.clone(), providers).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&launch_of("a1", "mock")).await.unwrap();
    assert!(matches!(
        next_record(&mut dialed).await.body,
        JournalBody::Started { .. }
    ));
    let id = node.api.credential(&token()).id;
    Some((node, admission, id, dialed))
}

async fn wait_released(admission: &CountAdmission) {
    let deadline = tokio::time::Instant::now() + wait();
    while !admission.running.lock().unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "agents still run");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A revocation the feed dropped (the receiver lagged) is not lost: the
/// service re-reads the active credentials and stops the revoked backend.
#[tokio::test]
async fn a_lagged_revocation_feed_still_stops_revoked_backends() {
    let dir = tempfile::tempdir().unwrap();
    let Some((node, admission, id, _dialed)) = running_agent(dir.path()).await else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    node.api.revoked.lock().unwrap().insert(id.clone());
    // Overflow the feed before the service reads it: the revocation itself
    // is among the dropped messages.
    let _ = node.api.revocations.send(id.clone());
    for _ in 0..40 {
        let _ = node.api.revocations.send(ExecutorCredentialId::new());
    }
    wait_released(&admission).await;
    assert_eq!(node.service.status(&id), ExecutorLiveStatus::default());
    node.stop().await;
}

/// A daemon shutdown racing a revocation still stops the revoked
/// backend's agents.
#[tokio::test]
async fn shutdown_during_a_revocation_still_stops_its_agents() {
    let dir = tempfile::tempdir().unwrap();
    let Some((node, admission, id, _dialed)) = running_agent(dir.path()).await else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    node.api.revoked.lock().unwrap().insert(id.clone());
    node.api.revocations.send(id.clone()).unwrap();
    // Shut down as soon as the revocation has taken the backend out of the
    // service, while its agents are still being stopped.
    let deadline = tokio::time::Instant::now() + wait();
    while node.service.status(&id) != ExecutorLiveStatus::default() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    node.service.shutdown().await;
    assert!(
        admission.running.lock().unwrap().is_empty(),
        "shutdown returned with the revoked backend's agents still running"
    );
    node.stop().await;
}

/// Turning `remoteAgents.enabled` off closes live executor connections: an
/// already-bound backend can launch nothing more on its old session, and
/// after re-enabling it binds afresh.
#[tokio::test]
async fn disabling_remote_agents_closes_live_links_and_re_enabling_admits_a_fresh_bind() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let mut old = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut old).await;
    old.link.send(&refused_launch("a1")).await.unwrap();
    assert_eq!(next_record(&mut old).await.seq, NodeCounter(1));

    node.enabled
        .store(false, std::sync::atomic::Ordering::SeqCst);
    node.service.on_settings_changed().await;
    assert!(next_frame(&mut old).await.is_none(), "the live link closed");
    // A launch on the old session is not applied: nothing is journaled.
    let _ = old.link.send(&refused_launch("a2")).await;
    assert!(
        dial_link(&node.endpoint, hello(2, "lease-1", 1))
            .await
            .is_err(),
        "disabled: no new bind"
    );

    node.enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    node.service.on_settings_changed().await;
    let mut fresh = dial_link(&node.endpoint, hello(3, "lease-1", 1))
        .await
        .unwrap();
    assert_eq!(
        replay_complete(&mut fresh).await,
        NodeCounter(1),
        "the old session's launch after disable was never journaled"
    );
    fresh.link.send(&refused_launch("a3")).await.unwrap();
    assert_eq!(next_record(&mut fresh).await.seq, NodeCounter(2));
    node.stop().await;
}

/// A frame that reaches a session after the setting turned off (before the
/// close lands) is not applied either.
#[tokio::test]
async fn a_session_applies_no_frame_while_remote_agents_are_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let mut old = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut old).await;
    // Off, but no settings-change notification yet.
    node.enabled
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let _ = old.link.send(&refused_launch("a1")).await;
    assert!(
        next_frame(&mut old).await.is_none(),
        "the session closed instead of launching"
    );
    node.enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut fresh = dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut fresh).await, NodeCounter(0));
    node.stop().await;
}

/// On a multi-thread runtime a shutdown can land while the worker is
/// between taking a revoked backend out of the service and registering
/// its stop: shutdown must wait for the worker, so it still joins that
/// stop before returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_a_revocation_paused_between_removal_and_registration() {
    let dir = tempfile::tempdir().unwrap();
    let Some((node, admission, id, _dialed)) = running_agent(dir.path()).await else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let (removed_tx, removed) = std::sync::mpsc::sync_channel(1);
    let (resume, resume_rx) = std::sync::mpsc::sync_channel(1);
    *node.service.revoke_pause.lock().unwrap() = Some((removed_tx, resume_rx));
    node.api.revoked.lock().unwrap().insert(id.clone());
    node.api.revocations.send(id.clone()).unwrap();
    tokio::task::spawn_blocking(move || removed.recv().unwrap())
        .await
        .unwrap();
    let shutdown = {
        let service = node.service.clone();
        tokio::spawn(async move { service.shutdown().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned while the worker was still registering a stop"
    );
    resume.send(()).unwrap();
    tokio::time::timeout(wait(), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(
        admission.running.lock().unwrap().is_empty(),
        "shutdown returned with the revoked backend's agents still running"
    );
    node.stop().await;
}

fn capability(id: &str, status: ProviderStatus) -> ProviderCapability {
    ProviderCapability {
        id: id.into(),
        visible: true,
        status,
        models: Vec::new(),
        models_stale: false,
    }
}

/// A default-account failure shows at once, except on a provider that
/// cannot launch: the installation is the more useful thing to report.
#[test]
fn an_auth_failure_marks_only_a_launchable_provider() {
    let mut providers = vec![
        capability("claude-code", ProviderStatus::SignedIn),
        capability("codex", ProviderStatus::CliMissing),
        capability("pi", ProviderStatus::PrerequisiteMissing),
    ];
    for id in ["claude-code", "codex", "pi", "absent"] {
        mark_auth_failed(&mut providers, id);
    }
    let statuses: Vec<_> = providers.iter().map(|p| p.status).collect();
    assert_eq!(
        statuses,
        [
            ProviderStatus::AuthFailed,
            ProviderStatus::CliMissing,
            ProviderStatus::PrerequisiteMissing,
        ]
    );
}

/// A refresh forgets the failures its probes cleared, keeps the ones it
/// still reports, and marks the ones that arrived while it ran.
#[test]
fn a_refresh_settles_the_failures_it_saw_and_keeps_newer_ones() {
    let then = std::time::Instant::now();
    let later = then + Duration::from_secs(1);
    let seen = HashMap::from([
        ("claude-code".to_owned(), then),
        ("codex".to_owned(), then),
        ("opencode".to_owned(), then),
    ]);
    let mut failures = HashMap::from([
        // Cleared by a newer conclusive probe.
        ("claude-code".to_owned(), then),
        // Still failed.
        ("codex".to_owned(), then),
        // Failed again while the refresh ran.
        ("opencode".to_owned(), later),
        // Failed for the first time while the refresh ran.
        ("grok".to_owned(), later),
    ]);
    let mut providers = vec![
        capability("claude-code", ProviderStatus::SignedIn),
        capability("codex", ProviderStatus::AuthFailed),
        capability("opencode", ProviderStatus::SignedIn),
        capability("grok", ProviderStatus::Unknown),
    ];
    settle_auth_failures(&mut providers, &mut failures, &seen);
    let mut kept: Vec<_> = failures.keys().cloned().collect();
    kept.sort();
    assert_eq!(kept, ["codex", "grok", "opencode"]);
    let statuses: Vec<_> = providers.iter().map(|p| p.status).collect();
    assert_eq!(
        statuses,
        [
            ProviderStatus::SignedIn,
            ProviderStatus::AuthFailed,
            ProviderStatus::AuthFailed,
            ProviderStatus::AuthFailed,
        ]
    );
}

/// End to end over the daemon's WSS listener: the backend's stop is
/// acknowledged `stopped` only once the agent's process tree is gone and
/// its shared slot is free; repeating the same stop id, or stopping an
/// agent that is not running, is acknowledged `notRunning`.
#[tokio::test]
async fn a_stop_is_acknowledged_only_after_the_agent_is_actually_stopped() {
    use intent_core::node_link::{StopId, StopOutcome};
    let dir = tempfile::tempdir().unwrap();
    let Some((node, admission, id, dialed)) = running_agent(dir.path()).await else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let pid_file = dir
        .path()
        .join("executor")
        .join(&id.0)
        .join("agents")
        .join("a1")
        .join("work")
        .join("agent.pid");
    let pids: serde_json::Value = {
        let deadline = tokio::time::Instant::now() + wait();
        loop {
            if let Some(pids) = std::fs::read(&pid_file)
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            {
                break pids;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "agent never started"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let pid = |key: &str| i32::try_from(pids[key].as_i64().unwrap()).unwrap();
    let stop_id = StopId::new_v4();
    let outcome = dialed
        .link
        .stop(AgentId::from("a1"), stop_id, wait())
        .await
        .unwrap();
    assert_eq!(outcome, StopOutcome::Stopped);
    // Already true when the acknowledgement arrives: no polling.
    assert!(process_gone(pid("agent")), "the provider still runs");
    assert!(process_gone(pid("child")), "its helper still runs");
    assert!(admission.running.lock().unwrap().is_empty(), "slot held");
    assert_eq!(node.service.status(&id).running_agents, 0);

    // A resend of the same stop is idempotent.
    let again = dialed
        .link
        .stop(AgentId::from("a1"), stop_id, wait())
        .await
        .unwrap();
    assert_eq!(again, StopOutcome::NotRunning);
    let unknown = dialed
        .link
        .stop(AgentId::from("never-launched"), StopId::new_v4(), wait())
        .await
        .unwrap();
    assert_eq!(unknown, StopOutcome::NotRunning);
    node.stop().await;
}

/// Turning `remoteAgents.enabled` on fills the provider list with what is
/// cheap to know before anything else (so a backend binding right away
/// sees the providers) and starts a full report, instead of waiting for
/// the periodic refresh; turning it off does neither.
#[tokio::test]
async fn enabling_remote_agents_primes_and_refreshes_the_report() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    node.enabled.store(false, Ordering::SeqCst);
    node.service.on_settings_changed().await;
    let primes = node.report.primes.load(Ordering::SeqCst);
    let refreshes = node.report.refreshes.load(Ordering::SeqCst);

    node.enabled.store(true, Ordering::SeqCst);
    node.service.on_settings_changed().await;
    assert_eq!(
        node.report.primes.load(Ordering::SeqCst),
        primes + 1,
        "primed before returning"
    );
    tokio::time::timeout(wait(), async {
        while node.report.refreshes.load(Ordering::SeqCst) == refreshes {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a full report starts");

    // Already on: a settings change elsewhere does not prime again.
    node.service.on_settings_changed().await;
    assert_eq!(node.report.primes.load(Ordering::SeqCst), primes + 1);
    node.stop().await;
}
