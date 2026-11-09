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
    MachineCapacity, ProviderStatus, NODE_LINK_PROTOCOLS, SKILLS_PROTOCOL,
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
    /// Reconciles with the enabled set, after which `mock` is reported as
    /// enabled or not per `mock_enabled`.
    reconciles: std::sync::atomic::AtomicUsize,
    mock_enabled: std::sync::atomic::AtomicBool,
}

impl MachineReport for FixedReport {
    fn providers(&self) -> Vec<ProviderCapability> {
        if !self.mock_enabled.load(std::sync::atomic::Ordering::SeqCst) {
            return Vec::new();
        }
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
    fn reconcile(&self) {
        self.reconciles
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn disabled_providers(&self) -> Vec<String> {
        if self.mock_enabled.load(std::sync::atomic::Ordering::SeqCst) {
            Vec::new()
        } else {
            vec!["mock".into()]
        }
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
        // Agents work in plain directories here: no backend serves git. The
        // git checkout path is covered by the e2e tests.
        Self::start_with_checkouts(data_dir, api, admission, providers, |storage, _cache| {
            checkout::PlainCheckouts::new(storage)
        })
        .await
    }

    async fn start_with_checkouts(
        data_dir: &std::path::Path,
        api: Arc<MachineApi>,
        admission: Arc<CountAdmission>,
        providers: BTreeMap<String, ProviderLaunch>,
        make_checkouts: MakeCheckouts,
    ) -> Self {
        let report = Arc::new(FixedReport {
            refreshes: std::sync::atomic::AtomicUsize::new(0),
            primes: std::sync::atomic::AtomicUsize::new(0),
            auth_failures: Mutex::new(Vec::new()),
            reconciles: std::sync::atomic::AtomicUsize::new(0),
            mock_enabled: std::sync::atomic::AtomicBool::new(true),
        });
        let service = ExecutorService::start_with_checkouts(
            ExecutorDeps {
                data_dir: data_dir.to_owned(),
                node_id: NODE_ID.into(),
                admission,
                launch: Arc::new(FixedLaunch(providers)),
                report: report.clone(),
                executable: std::env::current_exe().unwrap(),
                link_traffic: Some(Arc::new(intent_core::link_traffic::LinkTraffic::new())),
                active_credentials: {
                    let api = api.clone();
                    Arc::new(move || {
                        let api = api.clone();
                        Box::pin(async move { Ok(api.active()) })
                    })
                },
            },
            api.subscribe_executor_revocations(),
            make_checkouts,
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
        server.install_executor_links(ExecutorLinks::new(service.clone()));
        let port = server.start().await.unwrap();
        let endpoint = NodeEndpoint {
            tap: None,
            url: format!("wss://127.0.0.1:{port}"),
            pin: CertificatePin::parse(&certificate.fingerprint256).unwrap(),
            token: ExecutorToken::new(token()).unwrap(),
            tailcat: None,
        };
        Self {
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
        lease_agents: Vec::new(),
        head_session: None,
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
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
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

/// The next journal record other than a `skills` report (nodeProtocol 12),
/// which [`skills_are_journaled_before_started_only_on_protocol_12`] covers.
async fn next_record(dialed: &mut DialedLink) -> JournalRecord {
    loop {
        let record = next_raw_record(dialed).await;
        if !matches!(record.body, JournalBody::Skills { .. }) {
            return record;
        }
    }
}

async fn next_raw_record(dialed: &mut DialedLink) -> JournalRecord {
    match next_frame(dialed).await {
        Some(LinkFrame::Journal(record)) => record,
        other => panic!("expected a journal record, got {other:?}"),
    }
}

/// The machine reports the agent's skills right before `started` when the
/// link negotiated protocol 12, and journals no `skills` record below it
/// (an older backend could not decode one).
#[tokio::test]
async fn skills_are_journaled_before_started_only_on_protocol_12() {
    let Some(providers) = fixture_providers() else {
        return;
    };
    for (max, reports) in [
        (NODE_LINK_PROTOCOLS.max, true),
        (SKILLS_PROTOCOL - 1, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let node = Running::start_with(
            dir.path(),
            MachineApi::new(),
            CountAdmission::new(4),
            providers.clone(),
        )
        .await;
        let mut hello = hello(1, "lease-1", 0);
        hello.protocol = intent_core::node_link::ProtocolRange { min: 7, max };
        let mut dialed = dial_link(&node.endpoint, hello).await.unwrap();
        replay_complete(&mut dialed).await;
        dialed.link.send(&launch_of("a1", "mock")).await.unwrap();
        let first = next_raw_record(&mut dialed).await;
        if reports {
            assert!(
                matches!(first.body, JournalBody::Skills { .. }),
                "protocol {max}: {first:?}"
            );
            let started = next_raw_record(&mut dialed).await;
            assert!(
                matches!(started.body, JournalBody::Started { .. }),
                "{started:?}"
            );
        } else {
            assert!(
                matches!(first.body, JournalBody::Started { .. }),
                "protocol {max}: {first:?}"
            );
        }
        node.stop().await;
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

/// While the owner captures link traffic, the machine records each
/// backend's link both ways, against that backend; the slot hands the
/// listener the service's capture.
#[tokio::test]
async fn executor_records_backend_links_while_capture_is_on() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 3).await;
    let slot = ExecutorSlot::default();
    assert!(slot.link_traffic().is_none());
    slot.install(node.service.clone());
    let traffic = slot.link_traffic().expect("the service's capture");
    traffic.start(true);
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));
    dialed.link.send(&refused_launch("a1")).await.unwrap();
    let _ = next_record(&mut dialed).await;
    let deadline = tokio::time::Instant::now() + wait();
    let kinds = loop {
        let records = traffic.snapshot(None, None)["records"]
            .as_array()
            .unwrap()
            .clone();
        let kinds: Vec<(String, String)> = records
            .iter()
            .filter(|r| !r["frameType"].as_str().unwrap().starts_with("heartbeat"))
            .map(|r| {
                (
                    r["direction"].as_str().unwrap().to_owned(),
                    r["frameType"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        if kinds.len() >= 5 || tokio::time::Instant::now() > deadline {
            assert!(records
                .iter()
                .all(|r| r["peer"]["kind"] == "backend" && r["peer"]["id"] == HEAD));
            break kinds;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let expected: Vec<(String, String)> = [
        ("in", "hello"),
        ("out", "welcome"),
        ("out", "replayComplete"),
        ("in", "launch"),
        ("out", "journal"),
    ]
    .iter()
    .map(|(d, t)| ((*d).to_owned(), (*t).to_owned()))
    .collect();
    assert_eq!(kinds, expected);
    traffic.stop();
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
            checkout: None,
            mcp_proxies: Vec::new(),
            compact_tool_descriptions: false,
            checkout_of: None,
            forge: None,
            forge_fetch: None,
            git_identity: None,
            new_machine: None,
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
/// is ready, or the service failed to start) the slot is no executor host.
#[test]
fn the_slot_is_not_ready_until_the_service_is_installed() {
    let slot = ExecutorSlot::default();
    assert!(!slot.ready());
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

/// The provider report runs only while some executor credential is active:
/// right away on the first poll that sees one, then every
/// [`REPORT_INTERVAL`]; never on a machine no backend was given access to.
#[test]
fn the_report_is_due_only_while_a_credential_is_active() {
    let now = std::time::Instant::now();
    assert!(!report_due(false, None, now), "no credential: no report");
    assert!(report_due(true, None, now), "first mint: report right away");
    assert!(!report_due(true, Some(now), now));
    assert!(!report_due(
        true,
        Some(now),
        now + REPORT_INTERVAL.saturating_sub(Duration::from_secs(1))
    ));
    assert!(report_due(true, Some(now), now + REPORT_INTERVAL));
    assert!(!report_due(false, Some(now), now + REPORT_INTERVAL));
}

/// A backend that binds right after its first mint sees the providers:
/// the welcome fills in what is cheap to know (launch resolution, no
/// provider CLI) before the first full report, as turning the retired
/// setting on used to.
#[tokio::test]
async fn a_bind_primes_the_report_before_its_welcome() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    assert_eq!(node.report.primes.load(Ordering::SeqCst), 0);
    let dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(dialed.welcome.capabilities.providers[0].id, "mock");
    assert!(
        node.report.primes.load(Ordering::SeqCst) >= 1,
        "the welcome primed the report"
    );
    assert_eq!(
        node.report.refreshes.load(Ordering::SeqCst),
        0,
        "a bind never runs the full report"
    );
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

/// A running agent reports its provider process's pid with what its launch
/// said and the backend it runs for, so the memory sampler can credit its
/// tree to it; once the agent stops it is no longer reported.
#[tokio::test]
async fn a_running_agent_reports_its_root_pid_and_owner() {
    let dir = tempfile::tempdir().unwrap();
    let Some((node, admission, id, _dialed)) = running_agent(dir.path()).await else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let roots = node.service.agent_roots();
    assert_eq!(roots.len(), 1, "{roots:?}");
    let root = &roots[0];
    assert_eq!(root.agent_id, AgentId::from("a1"));
    assert_eq!(root.workspace_id, WorkspaceId::from("ws-1"));
    assert_eq!(root.provider, "mock");
    assert_eq!(root.model, None);
    assert_eq!(
        root.owner,
        intent_services::RemoteOwner {
            backend_id: HEAD.into(),
            backend_name: "Backend".into(),
        }
    );
    assert!(root.root_pid > 0);
    assert!(!process_gone(i32::try_from(root.root_pid).unwrap()));

    node.api.revocations.send(id).unwrap();
    wait_released(&admission).await;
    assert!(node.service.agent_roots().is_empty());
    node.stop().await;
}

/// The connected-backend count is the backends holding an open inbound
/// link now: one while a backend's link is up, none once it closes.
#[tokio::test]
async fn linked_backends_count_open_inbound_links() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 1).await;
    assert_eq!(node.service.linked_backends(), 0);
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    assert_eq!(node.service.linked_backends(), 1);
    dialed.link.close();
    let deadline = tokio::time::Instant::now() + wait();
    while node.service.linked_backends() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the closed link still counts"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.stop().await;
}

/// Before the service is installed the slot reports no agents and no links.
#[test]
fn an_uninstalled_slot_reports_no_agents_or_links() {
    let slot = ExecutorSlot::default();
    assert!(slot.agent_roots().is_empty());
    assert_eq!(slot.linked_backends(), 0);
}

/// Revoking a linked backend's credential closes its link and drops it from
/// the connected-backend count.
#[tokio::test]
async fn a_revoked_backend_no_longer_counts_as_linked() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 1).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    assert_eq!(node.service.linked_backends(), 1);
    let id = node.api.credential(&token()).id;
    node.api.revocations.send(id).unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    while node.service.linked_backends() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the revoked backend still counts"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.stop().await;
}

/// A backend that reconnects counts once, whether its old link closed first
/// or is replaced by a newer bind; a second backend counts separately.
#[tokio::test]
async fn a_reconnecting_backend_counts_once() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 1).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    first.link.close();
    let mut second = dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut second).await;
    assert_eq!(node.service.linked_backends(), 1, "after close and rebind");
    let mut third = dial_link(&node.endpoint, hello(3, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut third).await;
    assert_eq!(
        node.service.linked_backends(),
        1,
        "a newer bind beside an open one"
    );
    let mut other = dial_link(
        &node.endpoint_for(&other_token()),
        hello_from(OTHER_HEAD, 1, "lease-o", 0),
    )
    .await
    .unwrap();
    replay_complete(&mut other).await;
    assert_eq!(node.service.linked_backends(), 2);
    node.stop().await;
}

/// A revoked credential's storage (its checkouts included) goes: when the
/// revocation arrives, at startup, and at the next bind of any backend.
/// Another backend's active credential keeps its storage.
#[tokio::test]
async fn a_revoked_credentials_storage_is_removed_and_active_ones_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 4).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    let mut other = dial_link(
        &node.endpoint_for(&other_token()),
        hello_from(OTHER_HEAD, 1, "lease-1", 0),
    )
    .await
    .unwrap();
    replay_complete(&mut other).await;
    let roots: HashMap<String, std::path::PathBuf> = [token(), other_token()]
        .into_iter()
        .map(|t| {
            let root = dir
                .path()
                .join("executor")
                .join(&node.api.credential(&t).id.0);
            (t, root)
        })
        .collect();
    let root = |token: &str| roots[token].clone();
    let gone = |path: std::path::PathBuf| async move {
        let deadline = tokio::time::Instant::now() + wait();
        while path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} remains",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    // Revoked live: its storage goes, the other backend's stays.
    let id = node.api.credential(&token()).id;
    node.api.revoked.lock().unwrap().insert(id.clone());
    node.api.revocations.send(id).unwrap();
    gone(root(&token())).await;
    assert!(root(&other_token()).join("journal").is_dir());

    // A revoked credential's directory left behind (a crash before the
    // removal) goes at the next bind.
    let stale = dir.path().join("executor").join("stalecredential");
    std::fs::create_dir_all(stale.join("agents").join("a1")).unwrap();
    drop(other);
    let mut again = dial_link(
        &node.endpoint_for(&other_token()),
        hello_from(OTHER_HEAD, 2, "lease-1", 0),
    )
    .await
    .unwrap();
    replay_complete(&mut again).await;
    gone(stale.clone()).await;
    assert!(root(&other_token()).join("journal").is_dir());
    let api = node.stop().await;

    // And at startup.
    std::fs::create_dir_all(stale.join("agents")).unwrap();
    let node = Running::start_with(dir.path(), api, CountAdmission::new(4), BTreeMap::new()).await;
    gone(stale).await;
    assert!(root(&other_token()).join("journal").is_dir());
    node.stop().await;
}

/// A final stop ends the agent's script runs before its last push and the
/// checkout's removal: when the stop is acknowledged the script's process
/// is already gone, and so is the checkout.
#[tokio::test]
async fn a_final_stop_ends_the_agents_scripts_before_it_is_acknowledged() {
    use intent_core::node_link::{FinalPush, ScriptStartCommand, StopId, StopOutcome};
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let agent = AgentId::from("a-scripted");
    let mut offer = hello(1, "lease-1", 0);
    offer.lease_agents = vec![agent.clone()];
    let mut dialed = dial_link(&node.endpoint, offer).await.unwrap();
    assert_eq!(dialed.link.protocol(), NODE_LINK_PROTOCOLS.max);
    replay_complete(&mut dialed).await;
    let checkout = dir
        .path()
        .join("executor")
        .join(&node.api.credential(&token()).id.0)
        .join("agents")
        .join(&agent.0)
        .join("work");
    std::fs::create_dir_all(&checkout).unwrap();

    dialed
        .link
        .send(&LinkFrame::ScriptStart(ScriptStartCommand {
            agent_id: agent.clone(),
            script_id: "s1".into(),
            spawn_id: "spawn-1".into(),
            // Ignores the polite signals, so only a kill that is waited for
            // to the end (its escalation to SIGKILL) ends it.
            command: "trap '' TERM HUP INT; while :; do sleep 1; done".into(),
            mode: intent_core::ScriptMode::Command,
            cwd: None,
            env: vec![],
        }))
        .await
        .unwrap();
    let pid = loop {
        match next_frame(&mut dialed).await {
            Some(LinkFrame::ScriptStarted(started)) if started.spawn_id == "spawn-1" => {
                break i32::try_from(started.pid).unwrap();
            }
            Some(LinkFrame::ScriptExited(exited)) => panic!("script did not start: {exited:?}"),
            Some(_) => {}
            None => panic!("the link closed"),
        }
    };
    assert!(!process_gone(pid), "the script runs");

    let acked = dialed
        .link
        .stop_with(agent.clone(), StopId::new_v4(), true, wait())
        .await
        .unwrap();
    assert!(process_gone(pid), "acknowledged while the script still ran");
    assert_eq!(acked.outcome, StopOutcome::NotRunning);
    assert_eq!(acked.final_push, Some(FinalPush::Skipped));
    assert!(!checkout.exists());
}

/// The gate both backends' shutdown pushes meet at in
/// [`shutdown_stops_backends_together`]: it opens only once both are under
/// way at once.
static BOTH_PUSHING: std::sync::LazyLock<Arc<tokio::sync::Barrier>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Barrier::new(2)));

/// A daemon shutdown stops every backend's agents at once, each one's last
/// push included, so it takes one drain however many backends there are
/// (intent-hq/intent#8437): each push here is held until the other
/// backend's is under way too, which a one-by-one stop never reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_backends_together() {
    let Some(providers) = fixture_providers() else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let admission = CountAdmission::new(4);
    let node = Running::start_with_checkouts(
        dir.path(),
        MachineApi::new(),
        admission.clone(),
        providers,
        |storage, _cache| checkout::PlainCheckouts::with_push_gate(storage, BOTH_PUSHING.clone()),
    )
    .await;
    let mut links = Vec::new();
    for (endpoint, hello) in [
        (node.endpoint.clone(), hello(1, "lease-1", 0)),
        (
            node.endpoint_for(&other_token()),
            hello_from(OTHER_HEAD, 1, "lease-o", 0),
        ),
    ] {
        let mut dialed = dial_link(&endpoint, hello).await.unwrap();
        replay_complete(&mut dialed).await;
        dialed.link.send(&launch_of("a1", "mock")).await.unwrap();
        assert!(matches!(
            next_record(&mut dialed).await.body,
            JournalBody::Started { .. }
        ));
        links.push(dialed);
    }
    assert_eq!(admission.running.lock().unwrap().len(), 2);

    tokio::time::timeout(wait(), node.service.shutdown())
        .await
        .expect("both backends' pushes run together");
    assert!(
        admission.running.lock().unwrap().is_empty(),
        "shutdown returned with agents still running"
    );
    node.stop().await;
}

/// A final stop over a script whose reaper never finishes is not
/// acknowledged (the backend resends it), and it answers within the reap
/// bound instead of hanging, so a later stop or launch is not stuck behind
/// it.
#[tokio::test]
async fn a_final_stop_over_a_stuck_script_answers_without_an_ack() {
    use intent_core::node_link::ScriptStartCommand;
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let agent = AgentId::from("a-stuck-final");
    crate::executor::scripts::hold_reaps_for_test(&agent);
    let mut offer = hello(1, "lease-1", 0);
    offer.lease_agents = vec![agent.clone()];
    let mut dialed = dial_link(&node.endpoint, offer).await.unwrap();
    replay_complete(&mut dialed).await;
    let checkout = dir
        .path()
        .join("executor")
        .join(&node.api.credential(&token()).id.0)
        .join("agents")
        .join(&agent.0)
        .join("work");
    std::fs::create_dir_all(&checkout).unwrap();
    dialed
        .link
        .send(&LinkFrame::ScriptStart(ScriptStartCommand {
            agent_id: agent.clone(),
            script_id: "s1".into(),
            spawn_id: "spawn-1".into(),
            command: "sleep 60".into(),
            mode: intent_core::ScriptMode::Command,
            cwd: None,
            env: vec![],
        }))
        .await
        .unwrap();
    loop {
        match next_frame(&mut dialed).await {
            Some(LinkFrame::ScriptStarted(started)) if started.spawn_id == "spawn-1" => break,
            Some(LinkFrame::ScriptExited(exited)) => panic!("script did not start: {exited:?}"),
            Some(_) => {}
            None => panic!("the link closed"),
        }
    }
    let runner = node
        .service
        .runners_for_test()
        .pop()
        .expect("a linked backend");
    let bound = crate::executor::scripts::reap_wait() + Duration::from_secs(3);
    let answer = tokio::time::timeout(bound, runner.final_stop(&agent))
        .await
        .expect("the final stop hung on the stuck script");
    assert_eq!(answer, None, "acknowledged with a script still unreaped");
    assert!(
        checkout.exists(),
        "the checkout went with a script still unreaped"
    );
    node.stop().await;
}

/// A final stop over a script whose kill or reaper panicked is not
/// acknowledged (the backend resends it); after a panicked kill, the resent
/// stop kills the script again and is acknowledged.
#[tokio::test]
async fn a_final_stop_over_a_panicked_script_teardown_gives_no_ack() {
    use intent_core::node_link::{FinalPush, ScriptStartCommand, StopOutcome};
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 2).await;
    let killed = AgentId::from("a-panicked-kill");
    let reaped = AgentId::from("a-panicked-reap");
    crate::executor::scripts::panic_reaps_for_test(&reaped);
    let mut offer = hello(1, "lease-1", 0);
    offer.lease_agents = vec![killed.clone(), reaped.clone()];
    let mut dialed = dial_link(&node.endpoint, offer).await.unwrap();
    replay_complete(&mut dialed).await;
    let root = dir
        .path()
        .join("executor")
        .join(&node.api.credential(&token()).id.0)
        .join("agents");
    for agent in [&killed, &reaped] {
        std::fs::create_dir_all(root.join(&agent.0).join("work")).unwrap();
        dialed
            .link
            .send(&LinkFrame::ScriptStart(ScriptStartCommand {
                agent_id: agent.clone(),
                script_id: "s1".into(),
                spawn_id: format!("spawn-{}", agent.0),
                command: "sleep 60".into(),
                mode: intent_core::ScriptMode::Command,
                cwd: None,
                env: vec![],
            }))
            .await
            .unwrap();
    }
    let mut started = 0;
    while started < 2 {
        match next_frame(&mut dialed).await {
            Some(LinkFrame::ScriptStarted(_)) => started += 1,
            Some(LinkFrame::ScriptExited(exited)) => panic!("script did not start: {exited:?}"),
            Some(_) => {}
            None => panic!("the link closed"),
        }
    }
    let runner = node
        .service
        .runners_for_test()
        .pop()
        .expect("a linked backend");
    let bound = crate::executor::scripts::reap_wait() + Duration::from_secs(3);

    crate::executor::scripts::panic_next_kill_for_test(&killed);
    let answer = tokio::time::timeout(bound, runner.final_stop(&killed))
        .await
        .expect("the final stop hung on a panicked kill");
    assert_eq!(answer, None, "acknowledged over a panicked kill");
    assert!(root.join(&killed.0).join("work").exists());
    let answer = tokio::time::timeout(bound, runner.final_stop(&killed))
        .await
        .expect("the resent final stop hung");
    assert_eq!(answer, Some((StopOutcome::NotRunning, FinalPush::Skipped)));
    assert!(!root.join(&killed.0).join("work").exists());

    let answer = tokio::time::timeout(bound, runner.final_stop(&reaped))
        .await
        .expect("the final stop hung on a panicked reaper");
    assert_eq!(answer, None, "acknowledged over a panicked reaper");
    assert!(root.join(&reaped.0).join("work").exists());
    node.stop().await;
}

/// The gate the agent's shutdown push meets the test at in
/// [`shutdown_over_a_stuck_script_stays_within_one_reap_wait`].
static SHUTDOWN_PUSH: std::sync::LazyLock<Arc<tokio::sync::Barrier>> =
    std::sync::LazyLock::new(|| Arc::new(tokio::sync::Barrier::new(2)));

/// A machine shutdown over a script whose reaper never finishes waits for
/// it once, bounded by the reap wait: the agent's last push starts right
/// after, and the whole executor shutdown, its links' close included,
/// returns without a second reap wait at the link's end
/// (intent-hq/intent#8437).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_over_a_stuck_script_stays_within_one_reap_wait() {
    use intent_core::node_link::ScriptStartCommand;
    let Some(providers) = fixture_providers() else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let admission = CountAdmission::new(4);
    let node = Running::start_with_checkouts(
        dir.path(),
        MachineApi::new(),
        admission.clone(),
        providers,
        |storage, _cache| checkout::PlainCheckouts::with_push_gate(storage, SHUTDOWN_PUSH.clone()),
    )
    .await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    let agent = AgentId::from("a-stuck-shutdown");
    crate::executor::scripts::hold_reaps_for_test(&agent);
    dialed
        .link
        .send(&launch_of(&agent.0, "mock"))
        .await
        .unwrap();
    assert!(matches!(
        next_record(&mut dialed).await.body,
        JournalBody::Started { .. }
    ));
    dialed
        .link
        .send(&LinkFrame::ScriptStart(ScriptStartCommand {
            agent_id: agent.clone(),
            script_id: "s1".into(),
            spawn_id: "spawn-1".into(),
            command: "sleep 60".into(),
            mode: intent_core::ScriptMode::Command,
            cwd: None,
            env: vec![],
        }))
        .await
        .unwrap();
    loop {
        match next_frame(&mut dialed).await {
            Some(LinkFrame::ScriptStarted(started)) if started.spawn_id == "spawn-1" => break,
            Some(LinkFrame::ScriptExited(exited)) => panic!("script did not start: {exited:?}"),
            Some(_) => {}
            None => panic!("the link closed"),
        }
    }

    // One reap wait for the stuck script, plus room to stop the agent and
    // close the links; a second reap wait at the links' close overruns it.
    let margin = intentd_test_support::test_timeout(Duration::from_secs(4));
    let bound = crate::executor::scripts::reap_wait() + margin;
    let service = node.service.clone();
    let shutdown = tokio::spawn(async move { service.shutdown().await });
    tokio::time::timeout(bound, SHUTDOWN_PUSH.wait())
        .await
        .expect("the agent's push did not start within one reap wait");
    tokio::time::timeout(margin, shutdown)
        .await
        .expect("the shutdown waited on the stuck script again after the push")
        .unwrap();
    assert!(
        admission.running.lock().unwrap().is_empty(),
        "shutdown returned with the agent still running"
    );
    node.stop().await;
}

/// When the machine's enabled providers change, every live backend link
/// gets the cheap reconciled report at once and then the fully refreshed
/// one, without asking: a provider the owner disabled drops out of the
/// backend's placement right away, and one enabled again comes back.
#[tokio::test]
async fn an_enabled_providers_change_pushes_the_report_unasked() {
    use std::sync::atomic::Ordering;
    async fn pushed(dialed: &mut DialedLink) -> (Vec<String>, Vec<String>) {
        loop {
            match next_frame(dialed).await {
                Some(LinkFrame::CapabilitiesReply { capabilities }) => {
                    let listed = capabilities.providers.into_iter().map(|p| p.id).collect();
                    return (listed, capabilities.disabled_providers);
                }
                Some(LinkFrame::Journal(_)) => {}
                other => panic!("expected a pushed capabilities report, got {other:?}"),
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 3).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;

    node.report.mock_enabled.store(false, Ordering::SeqCst);
    node.service.enabled_providers_changed().await;
    let off = (Vec::<String>::new(), vec!["mock".to_owned()]);
    assert_eq!(pushed(&mut dialed).await, off, "the reconciled report");
    assert_eq!(pushed(&mut dialed).await, off, "the refreshed report");
    assert_eq!(node.report.reconciles.load(Ordering::SeqCst), 1);
    assert_eq!(node.report.refreshes.load(Ordering::SeqCst), 1);

    node.report.mock_enabled.store(true, Ordering::SeqCst);
    node.service.enabled_providers_changed().await;
    let on = (vec!["mock".to_owned()], Vec::<String>::new());
    assert_eq!(pushed(&mut dialed).await, on);
    assert_eq!(pushed(&mut dialed).await, on);
    node.stop().await;
}

/// The report follows the enabled set by value: a change is reported once,
/// however many settings notices it arrived in (or none, when a newer
/// notice replaced it), and changing back before the loop acts reports
/// nothing.
#[test]
fn an_enabled_set_change_is_taken_once_by_value() {
    let set = |pairs: &[(&str, bool)]| {
        Some(
            pairs
                .iter()
                .map(|(id, on)| ((*id).to_owned(), *on))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )
    };
    let mut watch = EnabledWatch::new(None);
    assert!(!watch.take_change(None), "unchanged");
    assert!(
        watch.take_change(set(&[("codex", false)])),
        "disabled codex"
    );
    assert!(
        !watch.take_change(set(&[("codex", false)])),
        "already taken"
    );
    assert!(watch.take_change(set(&[("codex", true)])), "enabled again");
    assert!(watch.take_change(None), "back to the default");
}

fn observed(id: &str, status: ProviderStatus, models: &[&str]) -> ProviderCapability {
    ProviderCapability {
        id: id.into(),
        visible: true,
        status,
        models: models.iter().map(|m| (*m).to_owned()).collect(),
        models_stale: models.is_empty(),
    }
}

/// Reconciling drops a provider disabled since the cached report, adds one
/// enabled since with its launch status only, and keeps what was observed
/// of the others.
#[test]
fn reconciling_follows_the_enabled_set_and_keeps_observations() {
    let cached = vec![
        observed("claude-code", ProviderStatus::SignedIn, &["opus"]),
        observed("codex", ProviderStatus::SignedOut, &["gpt"]),
    ];
    let enabled_now = vec![
        observed("claude-code", ProviderStatus::Unknown, &[]),
        observed("opencode", ProviderStatus::Unknown, &[]),
    ];
    assert_eq!(
        reconcile_providers(&cached, enabled_now.clone()),
        vec![
            observed("claude-code", ProviderStatus::SignedIn, &["opus"]),
            observed("opencode", ProviderStatus::Unknown, &[]),
        ]
    );
    assert_eq!(reconcile_providers(&[], enabled_now.clone()), enabled_now);
}

/// A launch, model switch or failover reaches the machine as a launch of
/// the provider, which resolves from the machine's own settings: a provider
/// disabled there is refused before any process starts (journaled as
/// `node-provider-unavailable`).
#[test]
fn the_machine_refuses_a_provider_disabled_in_its_own_settings() {
    let dir = tempfile::tempdir().unwrap();
    let settings =
        Arc::new(intent_services::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    settings
        .apply(&[(
            "providers.enabled".into(),
            serde_json::json!({ "codex": false }),
        )])
        .unwrap();
    let launch = SettingsLaunch(settings);
    assert_eq!(
        launch.resolve("codex").err().as_deref(),
        Some("unknown or disabled")
    );
    assert_ne!(
        launch.resolve("claude-code").err().as_deref(),
        Some("unknown or disabled"),
        "an enabled provider is not refused as disabled"
    );
}

/// Disabling a provider changes only the report: an agent already running
/// on it keeps its process; only its next start is refused.
#[tokio::test]
async fn disabling_a_provider_keeps_its_running_agent() {
    use std::sync::atomic::Ordering;
    let Some(providers) = fixture_providers() else {
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
    assert!(matches!(
        next_record(&mut dialed).await.body,
        JournalBody::Started { .. }
    ));

    node.report.mock_enabled.store(false, Ordering::SeqCst);
    node.service.enabled_providers_changed().await;
    let running: Vec<_> = node
        .service
        .runners_for_test()
        .iter()
        .flat_map(Runner::running)
        .collect();
    assert_eq!(running, [AgentId::from("a1")]);
    node.stop().await;
}

/// The forge token reply on `dialed`, skipping other frames.
async fn forge_reply(dialed: &mut DialedLink) -> intent_core::node_link::ForgeTokenReply {
    loop {
        match next_frame(dialed).await {
            Some(LinkFrame::ForgeTokenReply(reply)) => return reply,
            Some(_) => {}
            None => panic!("the link closed"),
        }
    }
}

/// nodeProtocol 15 over the real link: a `forgeToken` is held and
/// answered; a rebind that negotiates 14 drops it, and so does revoking the
/// backend's credential.
#[tokio::test]
async fn a_forge_token_is_held_until_an_older_rebind_or_a_revocation() {
    use intent_core::node_link::{
        ForgeToken, LinkId, ProtocolRange, SecretOutcome, SecretValue, FORGE_HOST_GITHUB,
    };
    let send_token = |request: u128| {
        LinkFrame::ForgeToken(ForgeToken {
            request_id: LinkId::from_u128(request),
            host: FORGE_HOST_GITHUB.to_owned(),
            token: Some(SecretValue::from("ghp_executorCanary")),
        })
    };
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(dir.path(), 4).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&send_token(1)).await.unwrap();
    let reply = forge_reply(&mut dialed).await;
    assert_eq!(reply.outcome, SecretOutcome::Ok);
    assert_eq!(reply.request_id, LinkId::from_u128(1));
    let runner = node.service.runners_for_test().pop().unwrap();
    let held = || runner.head_link().forge().get().is_some();
    assert!(held());

    let mut older = hello(2, "lease-1", 0);
    older.protocol = ProtocolRange { min: 7, max: 14 };
    let mut rebound = dial_link(&node.endpoint, older).await.unwrap();
    replay_complete(&mut rebound).await;
    assert!(!held(), "a protocol-14 bind drops the token");

    let mut current = dial_link(&node.endpoint, hello(3, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut current).await;
    current.link.send(&send_token(2)).await.unwrap();
    assert_eq!(forge_reply(&mut current).await.outcome, SecretOutcome::Ok);
    assert!(held());
    node.api
        .revocations
        .send(node.api.credential(&token()).id)
        .unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    while held() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a revocation drops the token"
        );
        // timing-guard: poll interval for the revocation task (no event exposes it)
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.stop().await;
}
