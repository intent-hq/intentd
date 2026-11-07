//! The connector, the remote runtime and `remoteMachines.*` against a
//! machine served in process: a real machine `Services` and store behind the
//! daemon's WSS listener with the executor link installed (the admission
//! path of `/node/link` and `/node/git`), and a test executor host that
//! hands every admitted connection to the test.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::mpsc;

use intent_acp::session::{ActivityTracker, ContentBlock, StopReason};
use intent_acp::AcpError;
use intent_core::agent_runtime::AgentRuntime;
use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{
    JournalBody, JournalRecord, LaunchCommand, LinkFrame, LinkHello, LinkOutcome, LinkWelcome,
    MachineCapabilities, StopId, StopOutcome,
};
use intent_core::nodes::{NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::{
    with_caller, AgentId, Caller, ExecutorCredentialId, WorkspaceApi as _, WorkspaceId,
};
use intent_services::feature_api::settings::InMemorySecretStore;
use intent_services::feature_api::tests::TempDb;
use intent_services::feature_api::{EventBus, Services, SettingsRegistry};
use intent_store::Store;
use intent_transport::node_link::{
    Backoff, ExecutorHost, ExecutorLinks, GitConnection, LinkEvent, LinkEvents, NodeConnection,
    NodeLink,
};
use intent_transport::{
    generate_certificate, hash_token, AsyncTokenStore, TlsCertificate, TokenStore, WsApiServer,
    WsOptions,
};

use crate::connect::ConnectTuning;
use crate::connector::{RemoteNode, Tuning};
use crate::machine::MachineTarget;
use crate::RemoteServicesExt as _;

mod cloud;
mod launch;
mod methods;

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

fn tuning() -> Tuning {
    Tuning {
        ack_interval: Duration::from_millis(20),
        ack_batch: 256,
        link_loss_grace: Duration::from_millis(200),
        turn_poll: Duration::from_millis(20),
        permanent_retry: Duration::from_millis(50),
        backoff: Backoff::new(Duration::from_millis(10), Duration::from_millis(50)),
        idle_timeout: Duration::from_secs(600),
        idle_check: Duration::from_millis(20),
        capabilities_timeout: wait(),
        stop_resend: Backoff::new(Duration::from_millis(150), Duration::from_millis(600)),
        remove_timeout: wait(),
        final_remove_timeout: wait(),
        stop_wait: wait(),
        final_stop_bound: intent_core::remote_launch::FINAL_STOP_BOUND,
        legacy_push_grace: Duration::from_millis(100),
        final_stop_check: Duration::from_millis(50),
        clock: crate::final_stop::Clock::default(),
        connect: ConnectTuning {
            attempt: Duration::from_millis(500),
            total: wait(),
        },
        fail_ack_writes: Arc::default(),
        fail_claim_reads: Arc::default(),
        before_publish: crate::connector::BeforePublish::default(),
        before_claim: crate::connector::BeforePublish::default(),
        grace_gate: crate::connector::GraceGate::default(),
    }
}

/// A machine report observed now: macOS on Apple silicon with two agent
/// slots and the mock provider signed in.
fn capabilities(running: u32) -> MachineCapabilities {
    let mut capabilities = MachineCapabilities::new(
        AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        },
        "machine-test",
        2,
        intent_core::now_iso(),
    );
    capabilities.capacity.running_agents = running;
    capabilities
        .providers
        .push(intent_core::node_link::ProviderCapability {
            id: "mock".into(),
            visible: true,
            status: intent_core::node_link::ProviderStatus::SignedIn,
            models: vec!["mock-model".into()],
            models_stale: false,
        });
    capabilities
}

struct OwnerToken;

impl TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some("o".repeat(64))
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

/// The machine's executor service stand-in: reports a fixed welcome and
/// hands every admitted connection to the test.
struct Host {
    node_id: String,
    running: Mutex<Vec<AgentId>>,
    hosts: Mutex<Vec<String>>,
    tc_address: Mutex<Option<String>>,
    accepted: mpsc::UnboundedSender<NodeConnection>,
    /// Git connections admitted so far.
    git_accepted: std::sync::atomic::AtomicUsize,
}

impl ExecutorHost for Host {
    fn ready(&self) -> bool {
        true
    }
    fn node_id(&self) -> String {
        self.node_id.clone()
    }
    fn welcome(&self, _credential: &ExecutorCredential, _hello: &LinkHello) -> LinkWelcome {
        LinkWelcome {
            protocol: 0,
            node_id: String::new(),
            running_agents: self.running.lock().unwrap().clone(),
            capabilities: capabilities(0),
            hosts: self.hosts.lock().unwrap().clone(),
            tc_address: self.tc_address.lock().unwrap().clone(),
        }
    }
    fn accept(&self, _credential: ExecutorCredential, connection: NodeConnection) {
        if matches!(connection, NodeConnection::Git(_)) {
            self.git_accepted
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let _ = self.accepted.send(connection);
    }
}

/// A machine serving the executor link in process.
struct Machine {
    server: WsApiServer,
    port: u16,
    certificate: TlsCertificate,
    services: Arc<Services>,
    host: Arc<Host>,
    /// The credential minted for the backend, and its token.
    credential: ExecutorCredential,
    token: String,
    accepted: mpsc::UnboundedReceiver<NodeConnection>,
    git: Vec<GitConnection>,
    /// The loopback address the machine listens on.
    ip: [u8; 4],
    /// While stopped: the machine's port, bound but not listening, so
    /// connections are refused and no other socket can take the port
    /// before [`Machine::restart`].
    held: std::sync::Mutex<Option<socket2::Socket>>,
    _tmp: TempDb,
}

impl Machine {
    /// A machine that minted a credential for `backend_id`, listening on a
    /// port the system assigns.
    async fn start(backend_id: &str) -> Self {
        Self::start_on([127, 0, 0, 1], backend_id, 0).await
    }

    /// A machine listening only on loopback address `ip`, on `port` (`0`
    /// for one the system assigns, kept across restarts).
    async fn start_on(ip: [u8; 4], backend_id: &str, port: u16) -> Self {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let services = Arc::new(Services::new(store.clone()));
        let token = "e".repeat(64);
        let credential = Self::mint(&store, backend_id, &token).await;
        let certificate =
            generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let (tx, accepted) = mpsc::unbounded_channel();
        let host = Arc::new(Host {
            node_id: store.daemon_id().await.unwrap(),
            running: Mutex::new(Vec::new()),
            hosts: Mutex::new(vec!["127.0.0.1".into()]),
            tc_address: Mutex::new(None),
            accepted: tx,
            git_accepted: std::sync::atomic::AtomicUsize::new(0),
        });
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
        let mut server = WsApiServer::new(
            services.clone(),
            EventBus::new(store),
            &certificate,
            &tokens,
            WsOptions {
                bind_addresses: vec![std::net::IpAddr::from(ip)],
                base_port: port,
                ..WsOptions::default()
            },
            None,
        )
        .unwrap();
        server.install_executor_links(ExecutorLinks::new(host.clone()));
        // Port 0 is chosen and bound in one step, so no other socket can
        // take it in between; later starts reuse the assigned port. An
        // explicit port is bound as given.
        let port = if port == 0 {
            server.start_with_port_assignment(|_| Ok(())).await
        } else {
            server.start().await
        }
        .unwrap();
        Self {
            server,
            port,
            certificate,
            services,
            host,
            credential,
            token,
            accepted,
            git: Vec::new(),
            ip,
            held: std::sync::Mutex::new(None),
            _tmp: tmp,
        }
    }

    async fn mint(store: &Store, backend_id: &str, token: &str) -> ExecutorCredential {
        let credential = ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: backend_id.into(),
            backend_name: "Backend".into(),
            backend_fingerprint: None,
            created_at: intent_core::now_iso(),
            last_used_at: None,
            revoked_at: None,
        };
        store
            .insert_executor_credential(&credential, &hash_token(token))
            .await
            .unwrap();
        credential
    }

    fn id(&self) -> String {
        self.host.node_id.clone()
    }

    /// The `remoteMachines.add` params the frontend would relay.
    fn add_params(&self) -> Value {
        json!({
            "machineId": self.id(),
            "name": "Test machine",
            "hosts": ["127.0.0.1"],
            "port": self.port,
            "fingerprint": self.certificate.fingerprint256,
            "credentialId": self.credential.id,
            "token": self.token,
        })
    }

    /// The next link. Head's git connections, dialed right after each
    /// bind, are kept in `git` (the test serves no git).
    async fn accept(&mut self) -> NodeEnd {
        loop {
            match tokio::time::timeout(wait(), self.accepted.recv()).await {
                Ok(Some(NodeConnection::Link {
                    link,
                    events,
                    hello,
                })) => {
                    return NodeEnd {
                        link,
                        events,
                        hello,
                    }
                }
                Ok(Some(NodeConnection::Git(git))) => self.git.push(git),
                other => panic!("expected a link, got {other:?}"),
            }
        }
    }

    /// No new link is bound for `within`.
    async fn no_link(&mut self, within: Duration) {
        let found = tokio::time::timeout(intentd_test_support::test_timeout(within), async {
            loop {
                match self.accepted.recv().await {
                    Some(NodeConnection::Link { hello, .. }) => return hello,
                    Some(NodeConnection::Git(git)) => self.git.push(git),
                    None => std::future::pending::<()>().await,
                }
            }
        })
        .await;
        if let Ok(hello) = found {
            panic!(
                "no link expected, got one at generation {}",
                hello.binding.generation.0
            );
        }
    }

    /// Stop listening, keeping the port: connections are refused as by a
    /// machine that is down, and the port stays this machine's.
    async fn stop(&self) {
        self.server.stop().await;
        let held = hold_port(self.ip, self.port);
        *self.held.lock().unwrap() = Some(held);
    }

    async fn restart(&mut self) {
        // Released right before the listener binds it again.
        drop(self.held.lock().unwrap().take());
        let port = self.server.start().await.unwrap();
        assert_eq!(port, self.port, "the machine comes back on its port");
    }

    async fn revoke(&self) {
        with_caller(
            Caller::Daemon,
            self.services
                .executor_revoke(self.credential.id.to_string()),
        )
        .await
        .unwrap();
    }
}

/// `ip:port` bound without listening: connections to it are refused, and
/// no other socket can bind or connect from it until it is dropped.
fn hold_port(ip: [u8; 4], port: u16) -> socket2::Socket {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    // The stopped listener's closed connections may linger in TIME_WAIT.
    socket.set_reuse_address(true).unwrap();
    let address = std::net::SocketAddr::from((ip, port));
    socket
        .bind(&address.into())
        .unwrap_or_else(|error| panic!("hold the stopped machine's port {address}: {error}"));
    socket
}

/// The machine's end of one bound link.
struct NodeEnd {
    link: NodeLink,
    events: LinkEvents,
    hello: LinkHello,
}

impl NodeEnd {
    async fn journal(&self, record: JournalRecord) {
        self.link.send(&LinkFrame::Journal(record)).await.unwrap();
    }

    async fn replay_complete(&self, through: u64) {
        self.link
            .send(&LinkFrame::ReplayComplete {
                through_seq: NodeCounter(through),
            })
            .await
            .unwrap();
    }

    /// The next frame matching `pick`, skipping others.
    async fn expect<T>(&mut self, mut pick: impl FnMut(LinkFrame) -> Option<T>) -> T {
        tokio::time::timeout(wait(), async {
            loop {
                match self.events.recv().await {
                    Some(LinkEvent::Frame(frame)) => {
                        if let Some(found) = pick(frame) {
                            return found;
                        }
                    }
                    Some(LinkEvent::Request(_)) => {}
                    None => panic!("the link closed"),
                }
            }
        })
        .await
        .expect("the expected frame arrives")
    }

    /// Wait until head acks `seq`.
    async fn acked(&mut self, seq: u64) {
        self.expect(|frame| match frame {
            LinkFrame::Ack { seq: acked } if acked.0 >= seq => {
                assert_eq!(acked.0, seq, "head acks no further than it ingested");
                Some(())
            }
            _ => None,
        })
        .await;
    }

    /// The id of the next stop head sends for `agent`, skipping other
    /// frames.
    async fn stop_for(&mut self, agent: &AgentId) -> StopId {
        self.expect(|frame| match frame {
            LinkFrame::Stop {
                agent_id, stop_id, ..
            } if agent_id == *agent => Some(stop_id),
            _ => None,
        })
        .await
    }

    /// Acknowledge stop `stop_id` for `agent`.
    async fn ack_stop(&self, stop_id: StopId, agent: &AgentId, outcome: StopOutcome) {
        self.link
            .send(&LinkFrame::StopAck {
                stop_id,
                agent_id: agent.clone(),
                outcome,
                final_push: None,
            })
            .await
            .unwrap();
    }

    /// Answer head's next capabilities request with `capabilities`.
    async fn answer_capabilities(&mut self, capabilities: MachineCapabilities) {
        self.expect(|frame| matches!(frame, LinkFrame::Capabilities).then_some(()))
            .await;
        self.link
            .send(&LinkFrame::CapabilitiesReply { capabilities })
            .await
            .unwrap();
    }

    /// Wait until head closes the link.
    async fn closed_by_head(&mut self) {
        tokio::time::timeout(wait(), async {
            while self.events.recv().await.is_some() {}
        })
        .await
        .expect("head closes the link");
    }
}

fn workspace() -> WorkspaceId {
    WorkspaceId::from("ws-remote")
}

fn note(seq: u64, agent: &AgentId, text: &str) -> JournalRecord {
    JournalRecord {
        seq: NodeCounter(seq),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body: JournalBody::Notification {
            notification: json!({
                "sessionId": "node-session",
                "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}},
            }),
        },
    }
}

fn turn_ended(seq: u64, agent: &AgentId, turn: &str, error: Option<&str>) -> JournalRecord {
    JournalRecord {
        seq: NodeCounter(seq),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: Some(turn.into()),
        recorded_at: intent_core::now_iso(),
        body: JournalBody::TurnEnded {
            stop_reason: if error.is_some() {
                "refusal"
            } else {
                "end_turn"
            }
            .into(),
            error: error.map(str::to_owned),
        },
    }
}

fn exited(seq: u64, agent: &AgentId) -> JournalRecord {
    JournalRecord {
        seq: NodeCounter(seq),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body: JournalBody::Exited {
            exit_code: Some(3),
            error: None,
            failure: None,
        },
    }
}

/// The backend: services with the remote feature registered, an
/// in-memory secret store and an event bus.
struct Backend {
    services: Services,
    bus: EventBus,
    _config: tempfile::TempDir,
    _tmp: TempDb,
}

impl Backend {
    async fn new() -> Self {
        Self::with_tuning(tuning()).await
    }

    async fn with_tuning(tuning: Tuning) -> Self {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let config = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SettingsRegistry::load(config.path().join("config.toml")).unwrap());
        let bus = EventBus::new(store.clone());
        let services = crate::register(
            Services::new(store)
                .with_settings_registry(registry)
                .with_secret_store(Arc::new(InMemorySecretStore::default()))
                .with_event_bus(bus.clone()),
        );
        *crate::lock(&services.remote_feature().unwrap().tuning) = Some(tuning);
        Self {
            services,
            bus,
            _config: config,
            _tmp: tmp,
        }
    }

    fn store(&self) -> &Store {
        self.services.store()
    }

    async fn id(&self) -> String {
        self.store().daemon_id().await.unwrap()
    }

    /// A machine that minted a credential for this backend.
    async fn machine(&self) -> Machine {
        Machine::start(&self.id().await).await
    }

    async fn call(&self, method: &'static str, params: Value) -> intent_core::Result<Value> {
        with_caller(
            Caller::Daemon,
            self.services.remote_machines(method, params),
        )
        .await
    }

    /// Expose `machine` (consuming the probe's link) and return its
    /// connector.
    async fn expose(&self, machine: &mut Machine) -> RemoteNode {
        self.call("add", machine.add_params()).await.expect("add");
        machine.accept().await.closed_by_head().await;
        self.node(&machine.id()).await
    }

    async fn node(&self, id: &str) -> RemoteNode {
        self.services
            .remote_feature()
            .unwrap()
            .node(&self.services, id)
            .await
            .unwrap()
    }

    async fn target(&self, machine: &Machine) -> MachineTarget {
        let row = self
            .store()
            .get_exposed_machine(&machine.id())
            .await
            .unwrap()
            .unwrap();
        MachineTarget::new(&row, &machine.token, None).unwrap()
    }

    /// A connector to `machine` with `tuning` that answers no machine
    /// request and announces nothing.
    async fn bare_with(&self, machine: &Machine, tuning: Tuning) -> RemoteNode {
        RemoteNode::start(
            self.store().clone(),
            self.target(machine).await,
            tuning,
            None,
        )
        .await
        .unwrap()
    }

    async fn bare(&self, machine: &Machine) -> RemoteNode {
        self.bare_with(machine, tuning()).await
    }

    async fn persisted_ack(&self, node: &RemoteNode) -> NodeCounter {
        let lease_id = node.status().lease_id;
        self.store()
            .list_owned_node_leases(&self.id().await)
            .await
            .unwrap()
            .into_iter()
            .find(|lease| lease.owner.lease_id == lease_id)
            .unwrap()
            .ack_seq
    }
}

/// An exposed machine and a bare connector to it (the feature's own
/// connector is shut down).
async fn exposed() -> (Machine, Backend, RemoteNode) {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await.shutdown();
    let node = backend.bare(&machine).await;
    (machine, backend, node)
}

/// The text of each notification the runtime has received so far.
async fn texts(runtime: &crate::RemoteAgentRuntime, count: usize) -> Vec<String> {
    let notes = runtime.notifications();
    let mut notes = notes.lock().await;
    let mut out = Vec::new();
    tokio::time::timeout(wait(), async {
        while out.len() < count {
            let note = notes.recv().await.expect("notification stream open");
            assert_eq!(note.method, "session/update");
            out.push(
                note.params["update"]["content"]["text"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
    })
    .await
    .expect("notifications arrive");
    // Nothing else is queued: no record was delivered twice.
    assert!(notes.try_recv().is_err(), "no duplicate deliveries");
    out
}

fn text_prompt(text: &str) -> Vec<ContentBlock> {
    vec![serde_json::from_value(json!({"type": "text", "text": text})).unwrap()]
}

/// Wait until `node` has bound the link the machine accepted. A prompt
/// waits only the (short, test) link-loss grace for a link, so a test that
/// prompts right after the machine accepts must first let the bind land.
async fn bound(node: &RemoteNode) {
    tokio::time::timeout(wait(), async {
        while node.shared_ref().current_link().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the connector publishes the bound link");
}

/// Wait until `node` reports no link.
async fn disconnected(node: &RemoteNode) {
    tokio::time::timeout(wait(), async {
        while node.status().connected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the connector reports the disconnect");
}

#[tokio::test]
async fn ingests_in_order_and_acks_the_highest_contiguous_sequence() {
    let (mut machine, backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    assert_eq!(link.hello.acked_seq, NodeCounter(0));
    assert_eq!(link.hello.binding.head_id, backend.id().await);
    assert_eq!(link.hello.binding.node_id, machine.id());

    link.replay_complete(0).await;
    for (seq, text) in [(1, "one"), (2, "two"), (3, "three")] {
        link.journal(note(seq, &agent, text)).await;
    }
    link.acked(3).await;
    assert_eq!(texts(&runtime, 3).await, ["one", "two", "three"]);
    assert_eq!(node.ingested(), NodeCounter(3));
    assert_eq!(backend.persisted_ack(&node).await, NodeCounter(3));

    let status = node.status();
    assert!(status.connected);
    assert_eq!(
        status.platform,
        Some(AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64
        })
    );
    assert_eq!(status.max_agents, 2);
    node.shutdown();
}

#[tokio::test]
async fn a_reconnect_replays_from_the_last_ack_without_duplicates() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let mut first = machine.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(2, &agent, "two")).await;
    first.acked(2).await;
    first.link.close();

    // A live agent: the lost link is redialed.
    let mut second = machine.accept().await;
    assert_eq!(
        second.hello.acked_seq,
        NodeCounter(2),
        "resumes from the ack"
    );
    assert!(second.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(second.hello.binding.lease_id, first.hello.binding.lease_id);
    // A replay that starts before head's ack is deduplicated by sequence.
    for (seq, text) in [(1, "one"), (2, "two"), (3, "three")] {
        second.journal(note(seq, &agent, text)).await;
    }
    second.replay_complete(3).await;
    second.acked(3).await;
    second.journal(note(4, &agent, "four")).await;
    second.acked(4).await;
    assert_eq!(texts(&runtime, 4).await, ["one", "two", "three", "four"]);
    node.shutdown();
}

#[tokio::test]
async fn a_restarted_backend_resumes_from_the_persisted_ack() {
    let (mut machine, backend, node) = exposed().await;
    let agent = AgentId::new();
    let _runtime = node.attach(&agent, &workspace());
    let mut first = machine.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(2, &agent, "two")).await;
    first.acked(2).await;
    node.shutdown();
    first.closed_by_head().await;

    let restarted = backend.bare(&machine).await;
    restarted.connect().await.unwrap();
    let second = machine.accept().await;
    assert_eq!(second.hello.acked_seq, NodeCounter(2));
    assert_eq!(second.hello.binding.head_id, first.hello.binding.head_id);
    assert_eq!(second.hello.binding.lease_id, first.hello.binding.lease_id);
    assert!(second.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(restarted.ingested(), NodeCounter(2));
    restarted.shutdown();
}

#[tokio::test]
async fn a_live_gap_rebinds_and_resumes_from_the_last_ack() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let mut first = machine.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(3, &agent, "three")).await;
    first.closed_by_head().await;

    let mut second = machine.accept().await;
    assert_eq!(second.hello.acked_seq, NodeCounter(1));
    second.journal(note(2, &agent, "two")).await;
    second.journal(note(3, &agent, "three")).await;
    second.replay_complete(3).await;
    second.acked(3).await;
    assert_eq!(texts(&runtime, 3).await, ["one", "two", "three"]);
    node.shutdown();
}

#[tokio::test]
async fn a_gap_in_the_replay_is_taken_as_records_the_node_no_longer_has() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.journal(note(4, &agent, "four")).await;
    link.replay_complete(4).await;
    link.acked(4).await;
    link.journal(note(5, &agent, "five")).await;
    link.acked(5).await;
    assert_eq!(texts(&runtime, 2).await, ["four", "five"]);
    node.shutdown();
}

#[tokio::test]
async fn records_of_agents_not_attached_here_are_acked_and_dropped() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.replay_complete(0).await;
    link.journal(note(1, &AgentId::new(), "stranger")).await;
    let mut other_workspace = note(2, &agent, "elsewhere");
    other_workspace.workspace_id = WorkspaceId::from("ws-other");
    link.journal(other_workspace).await;
    link.journal(note(3, &agent, "mine")).await;
    link.acked(3).await;
    assert_eq!(texts(&runtime, 1).await, ["mine"]);
    node.shutdown();
}

#[tokio::test]
async fn a_prompt_completes_with_its_turn_end() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.replay_complete(0).await;
    bound(&node).await;

    assert!(runtime.is_alive());
    assert_eq!(runtime.spawned_pid(), None);
    assert_eq!(runtime.root_pid(), None);
    let before = runtime.client_request_seq();
    let prompt = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime
                .prompt("head-session", text_prompt("hello"), &activity)
                .await
        })
    };
    let (turn, sent) = link
        .expect(|frame| match frame {
            LinkFrame::Prompt {
                agent_id,
                turn_id,
                prompt,
            } if agent_id == agent => Some((turn_id, prompt)),
            _ => None,
        })
        .await;
    assert_eq!(sent, vec![json!({"type": "text", "text": "hello"})]);
    assert!(runtime.client_request_seq() > before);

    let mut reply = note(1, &agent, "hi there");
    reply.turn_id = Some(turn.clone());
    link.journal(reply).await;
    // A turn end for another turn does not complete this prompt.
    link.journal(turn_ended(2, &agent, "some-other-turn", None))
        .await;
    link.journal(turn_ended(3, &agent, &turn, None)).await;
    let outcome = tokio::time::timeout(wait(), prompt)
        .await
        .unwrap()
        .unwrap()
        .expect("the turn ends normally");
    assert_eq!(outcome.stop_reason, StopReason::EndTurn);
    assert_eq!(texts(&runtime, 1).await, ["hi there"]);
    assert_eq!(runtime.response_seq(), 2);
    assert!(runtime.await_response_after(1, wait()).await);
    assert!(
        !runtime
            .await_response_after(2, Duration::from_millis(20))
            .await
    );

    runtime.cancel("head-session").await.unwrap();
    link.expect(|frame| match frame {
        LinkFrame::Cancel { agent_id } if agent_id == agent => Some(()),
        _ => None,
    })
    .await;
    node.shutdown();
}

#[tokio::test]
async fn a_failed_turn_and_an_exit_fail_the_prompt() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    link.replay_complete(0).await;
    bound(&node).await;

    let spawn_prompt = |runtime: Arc<crate::RemoteAgentRuntime>| {
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime.prompt("s", text_prompt("go"), &activity).await
        })
    };
    let next_turn = |frame: LinkFrame| match frame {
        LinkFrame::Prompt { turn_id, .. } => Some(turn_id),
        _ => None,
    };

    let failed = spawn_prompt(runtime.clone());
    let turn = link.expect(next_turn).await;
    link.journal(turn_ended(1, &agent, &turn, Some("node-turn-failed: boom")))
        .await;
    match failed.await.unwrap() {
        Err(AcpError::Rpc(error)) => assert_eq!(error.message, "node-turn-failed: boom"),
        other => panic!("expected the node's turn error, got {other:?}"),
    }

    let waiting = spawn_prompt(runtime.clone());
    link.expect(next_turn).await;
    link.journal(exited(2, &agent)).await;
    match waiting.await.unwrap() {
        Err(AcpError::Transport(message)) => {
            assert!(message.starts_with("node-agent-not-running"), "{message}");
            assert!(message.contains("status 3"), "{message}");
        }
        other => panic!("expected an exit error, got {other:?}"),
    }
    assert!(!runtime.is_alive());
    assert!(matches!(
        spawn_prompt(runtime.clone()).await.unwrap(),
        Err(AcpError::Transport(_))
    ));
    node.shutdown();
}

#[tokio::test]
async fn a_turn_fails_when_the_link_stays_down_past_the_grace() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    bound(&node).await;
    let prompt = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime.prompt("s", text_prompt("go"), &activity).await
        })
    };
    link.expect(|frame| matches!(frame, LinkFrame::Prompt { .. }).then_some(()))
        .await;
    // The machine goes away and does not come back.
    machine.stop().await;
    link.link.close();
    match tokio::time::timeout(wait(), prompt).await.unwrap().unwrap() {
        Err(AcpError::Transport(message)) => {
            assert!(message.starts_with("node-link-unavailable"), "{message}");
        }
        other => panic!("expected a link-loss error, got {other:?}"),
    }
    assert!(runtime.is_alive(), "the agent may still run on the machine");
    assert!(matches!(
        runtime.cancel("s").await,
        Err(AcpError::Transport(_))
    ));
    node.shutdown();
}

#[tokio::test]
async fn a_stop_is_sent_once_now_or_after_the_next_bind() {
    let backend = Backend::new().await;
    let mut machine = Machine::start(&backend.id().await).await;
    backend.expose(&mut machine).await.shutdown();
    machine.stop().await;
    let node = backend.bare(&machine).await;

    let early = AgentId::new();
    let runtime = node.attach(&early, &workspace());
    runtime.stop().await;
    runtime.stop().await;
    assert!(!runtime.is_alive());

    // The pending stop keeps the connector redialing until it is sent.
    machine.restart().await;
    let mut link = machine.accept().await;
    let early_stop = link.stop_for(&early).await;
    // The machine confirms it; until then the stop stays queued and would be
    // sent again.
    link.ack_stop(early_stop, &early, StopOutcome::Stopped)
        .await;
    tokio::time::timeout(wait(), async {
        while node.shared_ref().has_pending_stops() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the machine's ack confirms the stop");

    // Stopped agents' records are no longer delivered.
    link.replay_complete(0).await;
    link.journal(note(1, &early, "late")).await;
    link.acked(1).await;
    assert!(runtime.notifications().lock().await.try_recv().is_err());

    let live = AgentId::new();
    drop(node.attach(&live, &workspace()));
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id, .. } if agent_id == live => Some(()),
        LinkFrame::Stop { agent_id, .. } if agent_id == early => {
            panic!("a stop is sent once")
        }
        _ => None,
    })
    .await;
    node.shutdown();
}

#[tokio::test]
async fn machine_requests_are_answered_as_unsupported_without_services() {
    let (mut machine, _backend, node) = exposed().await;
    node.connect().await.unwrap();
    let link = machine.accept().await;
    let outcome = link
        .link
        .call(AgentId::new(), "note.list", json!({}), wait())
        .await
        .unwrap();
    match outcome {
        LinkOutcome::Err(error) => {
            assert_eq!(error.code, -32601);
            assert!(error.message.starts_with("remote-request-unsupported"));
        }
        LinkOutcome::Ok(value) => panic!("expected a refusal, got {value}"),
    }
    node.shutdown();
}

#[tokio::test]
async fn a_machine_whose_lease_ended_is_not_bound_again() {
    let (machine, backend, node) = exposed().await;
    node.shutdown();
    backend
        .store()
        .end_machine_leases(&machine.id(), &backend.id().await)
        .await
        .unwrap();
    let error = RemoteNode::start(
        backend.store().clone(),
        backend.target(&machine).await,
        tuning(),
        None,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("remote-node-lease-ended"),
        "{error}"
    );
}

/// The runtime fits the agent manager's runtime slot: the same associated
/// types as the local ACP runtime, as a shared trait object.
#[test]
fn the_remote_runtime_is_an_agent_manager_runtime() {
    type ManagerRuntime = dyn AgentRuntime<
        Prompt = Vec<ContentBlock>,
        PromptOutcome = intent_acp::session::PromptOutcome,
        Activity = ActivityTracker,
        Error = AcpError,
        Notifications = Arc<
            tokio::sync::Mutex<
                tokio::sync::mpsc::UnboundedReceiver<intent_acp::transport::IncomingNotification>,
            >,
        >,
    >;
    fn accepts(_: Option<Arc<ManagerRuntime>>) {}
    accepts(None::<Arc<crate::RemoteAgentRuntime>>.map(|r| r as Arc<ManagerRuntime>));
}

#[tokio::test]
async fn stopping_the_agent_fails_a_waiting_prompt() {
    let (mut machine, _backend, node) = exposed().await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = machine.accept().await;
    bound(&node).await;
    let prompt = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime.prompt("s", text_prompt("go"), &activity).await
        })
    };
    link.expect(|frame| matches!(frame, LinkFrame::Prompt { .. }).then_some(()))
        .await;
    runtime.stop().await;
    match tokio::time::timeout(wait(), prompt).await.unwrap().unwrap() {
        Err(AcpError::Transport(message)) => {
            assert!(message.contains("stopped"), "{message}");
        }
        other => panic!("expected a stopped error, got {other:?}"),
    }
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id, .. } if agent_id == agent => Some(()),
        _ => None,
    })
    .await;
    node.shutdown();
}

#[tokio::test]
async fn a_prompt_waiting_for_the_link_is_not_sent_after_a_stop() {
    let backend = Backend::new().await;
    let mut machine = Machine::start(&backend.id().await).await;
    backend.expose(&mut machine).await.shutdown();
    machine.stop().await;
    let mut slow = tuning();
    slow.link_loss_grace = wait();
    let node = backend.bare_with(&machine, slow).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let prompt = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime.prompt("s", text_prompt("go"), &activity).await
        })
    };
    // The prompt has started and is waiting for the link.
    tokio::time::timeout(wait(), async {
        while runtime.client_request_seq() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the prompt starts");
    runtime.stop().await;
    match tokio::time::timeout(wait(), prompt).await.unwrap().unwrap() {
        Err(AcpError::Transport(message)) => {
            assert!(message.contains("stopped"), "{message}");
        }
        other => panic!("expected a stopped error, got {other:?}"),
    }

    machine.restart().await;
    let mut link = machine.accept().await;
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id, .. } if agent_id == agent => Some(()),
        LinkFrame::Prompt { .. } => panic!("a stopped agent's prompt is never sent"),
        _ => None,
    })
    .await;
    node.shutdown();
}

#[tokio::test]
async fn a_rebind_never_acks_more_than_head_persisted() {
    let (mut machine, backend, bare) = exposed().await;
    bare.shutdown();
    let flaky = tuning();
    let fail = flaky.fail_ack_writes.clone();
    let node = backend.bare_with(&machine, flaky).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let first = machine.accept().await;
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(2, &agent, "two")).await;
    assert_eq!(texts(&runtime, 2).await, ["one", "two"]);
    tokio::time::timeout(wait(), async {
        while node.ingested() < NodeCounter(2) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first.link.close();

    // The writes still fail: the rebind offers only the saved ack, so the
    // machine keeps records 1 and 2.
    let mut second = machine.accept().await;
    assert_eq!(second.hello.acked_seq, NodeCounter(0));
    assert!(second.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(backend.persisted_ack(&node).await, NodeCounter(0));

    // Persistence recovers: the replay is deduplicated and the ack advances.
    fail.store(false, std::sync::atomic::Ordering::SeqCst);
    for (seq, text) in [(1, "one"), (2, "two"), (3, "three")] {
        second.journal(note(seq, &agent, text)).await;
    }
    second.replay_complete(3).await;
    second.acked(3).await;
    assert_eq!(texts(&runtime, 1).await, ["three"]);
    assert_eq!(backend.persisted_ack(&node).await, NodeCounter(3));
    second.link.close();
    let third = machine.accept().await;
    assert_eq!(third.hello.acked_seq, NodeCounter(3));
    node.shutdown();
}

/// On-demand connect, idle disconnect and reconnect while agents are live.
mod on_demand {
    use super::*;

    fn quick_idle() -> Tuning {
        let mut tuning = tuning();
        tuning.idle_timeout = Duration::from_millis(300);
        tuning
    }

    #[tokio::test]
    async fn nothing_is_dialed_until_the_machine_is_needed() {
        let (mut machine, _backend, node) = exposed().await;
        machine.no_link(Duration::from_millis(300)).await;
        assert!(!node.status().connected);
        node.connect().await.unwrap();
        let link = machine.accept().await;
        assert!(node.status().connected);
        assert_eq!(link.hello.acked_seq, NodeCounter(0));
        node.shutdown();
    }

    /// intent-hq/intent#8383: a launch goes out as soon as the link is
    /// published, and its checkout fetches over the git connection at once,
    /// so head dials the git connection before it publishes the link.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_git_connection_is_bound_before_the_link_is_published() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let host = machine.host.clone();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let mut hooked = tuning();
        let record = seen.clone();
        hooked.before_publish = crate::connector::BeforePublish(Some(Arc::new(
            move |_: &Arc<crate::connector::Shared>| {
                // The machine admits a dialed connection right after its
                // upgrade, on its own task: give it that moment.
                let deadline = std::time::Instant::now()
                    + intentd_test_support::test_timeout(Duration::from_secs(2));
                let mut count = host.git_accepted.load(std::sync::atomic::Ordering::SeqCst);
                while count == 0 && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(5));
                    count = host.git_accepted.load(std::sync::atomic::Ordering::SeqCst);
                }
                *record.lock().unwrap() = Some(count);
            },
        )));
        let node = backend.bare_with(&machine, hooked).await;
        node.connect().await.unwrap();
        let _link = machine.accept().await;
        assert_eq!(
            *seen.lock().unwrap(),
            Some(1),
            "the git connection is admitted before the link is published"
        );
        node.shutdown();
    }

    #[tokio::test]
    async fn an_idle_link_is_closed_and_not_redialed() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let node = backend.bare_with(&machine, quick_idle()).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        link.closed_by_head().await;
        disconnected(&node).await;
        machine.no_link(Duration::from_millis(500)).await;

        // Needed again: it connects again.
        node.connect().await.unwrap();
        let again = machine.accept().await;
        assert!(again.hello.binding.generation > link.hello.binding.generation);
        node.shutdown();
    }

    #[tokio::test]
    async fn a_live_agent_keeps_the_link_until_it_exits() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let node = backend.bare_with(&machine, quick_idle()).await;
        let agent = AgentId::new();
        let _runtime = node.attach(&agent, &workspace());
        let mut link = machine.accept().await;
        link.replay_complete(0).await;
        // timing-guard: three idle windows pass while the agent is live
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(!link.link.is_closed(), "a live agent holds the link");
        assert!(node.status().connected);

        link.journal(exited(1, &agent)).await;
        link.closed_by_head().await;
        machine.no_link(Duration::from_millis(500)).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn a_lost_link_without_live_agents_is_not_redialed() {
        let (mut machine, _backend, node) = exposed().await;
        node.connect().await.unwrap();
        let link = machine.accept().await;
        link.link.close();
        disconnected(&node).await;
        machine.no_link(Duration::from_millis(500)).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn a_lost_link_with_a_live_agent_is_redialed_and_replayed() {
        let (mut machine, _backend, node) = exposed().await;
        let agent = AgentId::new();
        let runtime = node.attach(&agent, &workspace());
        let mut first = machine.accept().await;
        first.replay_complete(0).await;
        first.journal(note(1, &agent, "before")).await;
        first.acked(1).await;
        // The machine drops off and comes back.
        machine.stop().await;
        machine.restart().await;
        let mut second = machine.accept().await;
        assert_eq!(second.hello.acked_seq, NodeCounter(1));
        second.journal(note(2, &agent, "after")).await;
        second.replay_complete(2).await;
        second.acked(2).await;
        assert_eq!(texts(&runtime, 2).await, ["before", "after"]);
        node.shutdown();
    }
}

/// Stop acknowledgement (nodeProtocol 7): a stop stays queued, and is
/// resent with its id, until the machine acks that id.
mod stop_acks {
    use super::*;
    use intentd_test_support::test_timeout;

    pub(super) fn launch_of(agent: &AgentId) -> LaunchCommand {
        LaunchCommand {
            agent_id: agent.clone(),
            workspace_id: workspace(),
            provider: "codex".into(),
            model: None,
            base_commit: "0".repeat(40),
            repo_key: "repo".into(),
            agent_features: serde_json::json!({}),
            is_sub_agent: false,
            checkout: None,
            mcp_proxies: Vec::new(),
            compact_tool_descriptions: false,
            checkout_of: None,
            profile_env: None,
        }
    }

    async fn confirmed(node: &RemoteNode) {
        tokio::time::timeout(wait(), async {
            while node.shared_ref().has_pending_stops() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stop is confirmed");
    }

    /// A live agent's link, bound and replayed.
    async fn live(
        machine: &mut Machine,
        node: &RemoteNode,
    ) -> (AgentId, Arc<crate::RemoteAgentRuntime>, NodeEnd) {
        let agent = AgentId::new();
        let runtime = node.attach(&agent, &workspace());
        let link = machine.accept().await;
        link.replay_complete(0).await;
        bound(node).await;
        (agent, runtime, link)
    }

    #[tokio::test]
    async fn a_stop_is_confirmed_only_by_its_own_ack() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;

        // Neither a capabilities reply, an exit record, another stop's ack
        // nor this stop's id for another agent confirms it.
        link.link
            .send(&LinkFrame::CapabilitiesReply {
                capabilities: capabilities(0),
            })
            .await
            .unwrap();
        link.journal(exited(1, &agent)).await;
        link.ack_stop(StopId::new_v4(), &agent, StopOutcome::Stopped)
            .await;
        link.ack_stop(stop_id, &AgentId::new(), StopOutcome::Stopped)
            .await;
        // timing-guard: the wrong acks have been read by head
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(node.shared_ref().has_pending_stops());

        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        confirmed(&node).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn an_unacked_stop_is_resent_with_the_same_id() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let first = link.stop_for(&agent).await;
        let again = link.stop_for(&agent).await;
        assert_eq!(again, first, "a resend keeps the stop id");
        link.ack_stop(first, &agent, StopOutcome::NotRunning).await;
        confirmed(&node).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn an_older_ack_never_confirms_a_newer_stop() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let older = link.stop_for(&agent).await;
        link.ack_stop(older, &agent, StopOutcome::Stopped).await;
        confirmed(&node).await;

        // The agent runs again and is stopped again: a new stop id.
        let again = node.attach(&agent, &workspace());
        again.stop().await;
        let newer = link.stop_for(&agent).await;
        assert_ne!(newer, older);
        // A late duplicate ack of the older stop confirms nothing.
        link.ack_stop(older, &agent, StopOutcome::NotRunning).await;
        // timing-guard: the late ack has been read by head
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(node.shared_ref().has_pending_stops());
        link.ack_stop(newer, &agent, StopOutcome::Stopped).await;
        confirmed(&node).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn queued_stops_go_out_first_after_a_reconnect_with_their_ids() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        // The launch below waits for the machine to come back.
        let mut slow = tuning();
        slow.link_loss_grace = wait();
        let node = backend.bare_with(&machine, slow).await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        // The machine drops off before acking; the agent never shows in a
        // welcome, which is no confirmation either.
        machine.stop().await;
        disconnected(&node).await;
        assert!(node.shared_ref().has_pending_stops());

        let other = AgentId::new();
        let launching = {
            let node = node.clone();
            let command = launch_of(&other);
            // The launched runtime is kept: dropping it would stop the agent.
            tokio::spawn(async move { node.launch(command).await })
        };
        machine.restart().await;
        let mut link = machine.accept().await;
        let first = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } => Some((agent_id, stop_id)),
                LinkFrame::Launch(_) | LinkFrame::Prompt { .. } => {
                    panic!("a launch went out before the queued stop")
                }
                _ => None,
            })
            .await;
        assert_eq!(first, (agent.clone(), stop_id), "resent with its id");
        link.ack_stop(stop_id, &agent, StopOutcome::NotRunning)
            .await;
        confirmed(&node).await;
        link.expect(|frame| matches!(frame, LinkFrame::Launch(_)).then_some(()))
            .await;
        let _launched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        node.shutdown();
    }

    /// A lost ack and its resend must never stop a newer process: the agent
    /// is not launched again until its stop is acknowledged.
    #[tokio::test]
    async fn a_launch_waits_for_its_agents_unconfirmed_stop() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        // The ack is lost. The agent is launched again meanwhile.
        let launching = {
            let node = node.clone();
            let command = launch_of(&agent);
            // The launched runtime is kept: dropping it would stop the agent.
            tokio::spawn(async move { node.launch(command).await })
        };
        let resent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                LinkFrame::Launch(_) => panic!("launched behind an unconfirmed stop"),
                _ => None,
            })
            .await;
        assert_eq!(resent, stop_id);
        assert!(!launching.is_finished(), "the launch waits for the ack");
        // As a machine does, the exit is journaled before the ack.
        link.journal(exited(1, &agent)).await;
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            LinkFrame::Stop { .. } => panic!("a resend raced the launch"),
            _ => None,
        })
        .await;
        let _launched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // No resend follows the ack: the new process is never stopped.
        let late = tokio::time::timeout(test_timeout(Duration::from_millis(800)), async {
            link.expect(|frame| matches!(frame, LinkFrame::Stop { .. }).then_some(()))
                .await;
        })
        .await;
        assert!(late.is_err(), "the confirmed stop is not sent again");
        node.shutdown();
    }

    /// Relaunch `agent` right after its stop, in the background.
    fn relaunch(
        node: &RemoteNode,
        agent: &AgentId,
    ) -> tokio::task::JoinHandle<intent_core::Result<Arc<crate::RemoteAgentRuntime>>> {
        let node = node.clone();
        let command = launch_of(agent);
        tokio::spawn(async move { node.launch(command).await })
    }

    /// intent-hq/intent#8457: the stopped process's exit record, arriving
    /// while its relaunch waits for the stop's ack, belongs to the old
    /// process. The relaunched runtime must not take it as its own exit.
    #[tokio::test]
    async fn a_relaunch_ignores_the_stopped_processs_exit_sent_before_the_ack() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        // The machine journals the old process's exit, then acks the stop.
        link.journal(exited(1, &agent)).await;
        link.acked(1).await;
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            _ => None,
        })
        .await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            relaunched.is_running(),
            "the old process's exit ended the relaunched runtime: {:?}",
            relaunched.exit_error()
        );
        node.shutdown();
    }

    /// intent-hq/intent#8457: the machine acks a stop once the exit record
    /// is journaled, which can be before that record is streamed. A
    /// `stopped` ack means the record is coming: the relaunch waits for it
    /// before it attaches, so the record never reaches the new runtime.
    #[tokio::test]
    async fn a_relaunch_waits_for_the_stopped_processs_exit_sent_after_the_ack() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        // The ack overtakes the exit record.
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        let early = tokio::time::timeout(test_timeout(Duration::from_millis(500)), async {
            link.expect(|frame| match frame {
                LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
                _ => None,
            })
            .await;
        })
        .await;
        assert!(early.is_err(), "launched before the old exit arrived");
        link.journal(exited(1, &agent)).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            _ => None,
        })
        .await;
        // Head has ingested (and delivered) the old exit before the check.
        link.acked(1).await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            relaunched.is_running(),
            "the old process's exit ended the relaunched runtime: {:?}",
            relaunched.exit_error()
        );
        node.shutdown();
    }

    /// An exit record that never arrives (lost on the machine) holds the
    /// relaunch back only for the stop wait, not for good.
    #[tokio::test]
    async fn a_relaunch_gives_up_on_an_exit_record_that_never_arrives() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let mut quick = tuning();
        quick.stop_wait = Duration::from_millis(300);
        let node = backend.bare_with(&machine, quick).await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            _ => None,
        })
        .await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(relaunched.is_running());
        node.shutdown();
    }

    /// A `not-running` ack promises no exit record: the relaunch goes out
    /// at once.
    #[tokio::test]
    async fn a_relaunch_after_a_not_running_ack_does_not_wait_for_an_exit() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        link.ack_stop(stop_id, &agent, StopOutcome::NotRunning)
            .await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            _ => None,
        })
        .await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(relaunched.is_running());
        node.shutdown();
    }

    /// Wait until `launching` is ready, asserting no launch frame went out
    /// before the old exit arrived, then deliver that exit (`seq`) and check
    /// the relaunched runtime survives it.
    async fn relaunch_survives_a_late_exit(
        link: &mut NodeEnd,
        node: &RemoteNode,
        agent: &AgentId,
        seq: u64,
        launching: tokio::task::JoinHandle<intent_core::Result<Arc<crate::RemoteAgentRuntime>>>,
    ) {
        let early = tokio::time::timeout(test_timeout(Duration::from_millis(500)), async {
            link.expect(|frame| match frame {
                LinkFrame::Launch(command) if command.agent_id == *agent => Some(()),
                _ => None,
            })
            .await;
        })
        .await;
        assert!(early.is_err(), "launched before the old exit arrived");
        link.journal(exited(seq, agent)).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == *agent => Some(()),
            _ => None,
        })
        .await;
        link.acked(seq).await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            relaunched.is_running(),
            "the old process's exit ended the relaunched runtime: {:?}",
            relaunched.exit_error()
        );
        node.shutdown();
    }

    /// Review of intentd#3044: the machine answers `not-running` for an
    /// agent still exiting, until its exit is journaled. A resend (the
    /// first frame's ack timed out) answered `not-running` may find the
    /// agent the first frame stopped, so its exit record is still owed.
    #[tokio::test]
    async fn a_resent_stop_answered_not_running_still_waits_for_the_exit() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let first = link.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        // The first frame goes unacknowledged; head resends it.
        let resent = link.stop_for(&agent).await;
        assert_eq!(resent, first, "a resend reuses the stop id");
        link.ack_stop(resent, &agent, StopOutcome::NotRunning).await;
        relaunch_survives_a_late_exit(&mut link, &node, &agent, 1, launching).await;
    }

    /// Review of intentd#3044: the same after a link loss. The stop sent on
    /// the lost link is sent again on the next one and answered
    /// `not-running` while the stopped process's exit is still unstreamed.
    #[tokio::test]
    async fn a_stop_resent_after_a_link_loss_answered_not_running_still_waits_for_the_exit() {
        let (mut machine, _backend, node) = exposed().await;
        let (agent, runtime, mut first) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = first.stop_for(&agent).await;
        let launching = relaunch(&node, &agent);
        first.link.close();
        let mut second = machine.accept().await;
        second.replay_complete(0).await;
        assert_eq!(second.stop_for(&agent).await, stop_id);
        second
            .ack_stop(stop_id, &agent, StopOutcome::NotRunning)
            .await;
        relaunch_survives_a_late_exit(&mut second, &node, &agent, 1, launching).await;
    }

    /// Review of intentd#2872: a stop queued while a bind is between
    /// sending the queued stops and publishing the link must still go out
    /// on that link, without a reconnect.
    #[tokio::test]
    async fn a_stop_queued_during_a_bind_goes_out_on_that_link() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let late = AgentId::new();
        let mut hooked = tuning();
        let queued = late.clone();
        hooked.before_publish = crate::connector::BeforePublish(Some(Arc::new(
            move |shared: &Arc<crate::connector::Shared>| shared.request_stop(&queued),
        )));
        let node = backend.bare_with(&machine, hooked).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        let stop_id = link.stop_for(&late).await;
        link.ack_stop(stop_id, &late, StopOutcome::NotRunning).await;
        confirmed(&node).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn a_queued_stop_keeps_the_link_up() {
        let (mut machine, backend, bare) = exposed().await;
        bare.shutdown();
        let mut quick = tuning();
        quick.idle_timeout = Duration::from_millis(300);
        let node = backend.bare_with(&machine, quick).await;
        let (agent, runtime, mut link) = live(&mut machine, &node).await;
        runtime.stop().await;
        let stop_id = link.stop_for(&agent).await;
        // timing-guard: three idle windows pass while the stop is unacked
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(!link.link.is_closed(), "a queued stop holds the link");
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        link.closed_by_head().await;
        node.shutdown();
    }

    #[tokio::test]
    async fn a_revoked_credential_drops_the_queue() {
        let (mut machine, _backend, node) = exposed().await;
        let (_agent, runtime, _link) = live(&mut machine, &node).await;
        machine.stop().await;
        disconnected(&node).await;
        runtime.stop().await;
        assert!(node.shared_ref().has_pending_stops());
        // Revocation stops the binding's agents on the machine: the queued
        // stop is moot and dropped once the machine refuses the credential.
        machine.revoke().await;
        machine.restart().await;
        confirmed(&node).await;
        node.shutdown();
    }
}

/// While the owner captures link traffic, the backend records its links to
/// an exposed machine (the add probe's and the connector's) against that
/// machine.
#[tokio::test]
async fn the_backend_records_its_machine_links_while_capture_is_on() {
    let backend = Backend::new().await;
    let traffic = backend.services.link_traffic().clone();
    traffic.start(true);
    let mut machine = backend.machine().await;
    let node = backend.expose(&mut machine).await;
    let agent = AgentId::new();
    let _runtime = node.attach(&agent, &workspace());
    let link = machine.accept().await;
    link.replay_complete(0).await;
    let deadline = tokio::time::Instant::now() + wait();
    let records = loop {
        let records = traffic.snapshot(None, None)["records"]
            .as_array()
            .unwrap()
            .clone();
        if records
            .iter()
            .any(|r| r["frameType"] == "replayComplete" && r["direction"] == "in")
            || tokio::time::Instant::now() > deadline
        {
            break records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let kinds: Vec<(&str, &str)> = records
        .iter()
        .map(|r| {
            (
                r["direction"].as_str().unwrap(),
                r["frameType"].as_str().unwrap(),
            )
        })
        .collect();
    // The probe's handshake, then the connector's.
    let hellos = kinds.iter().filter(|k| **k == ("out", "hello")).count();
    let welcomes = kinds.iter().filter(|k| **k == ("in", "welcome")).count();
    assert!(hellos >= 2 && welcomes >= 2, "{kinds:?}");
    assert!(kinds.contains(&("in", "replayComplete")), "{kinds:?}");
    let name = machine.add_params()["name"].clone();
    for record in &records {
        assert_eq!(record["peer"]["kind"], "machine");
        assert_eq!(record["peer"]["id"], machine.id().as_str());
        assert_eq!(record["peer"]["name"], name);
    }
    traffic.stop();
    node.shutdown();
}

/// Final stops (nodeProtocol 9): recorded durably, resent after a backend
/// restart, sent with `final`, and dropped on the machine's acknowledgement.
mod final_stops {
    use super::*;
    use intent_core::node_link::FinalPush;
    use intent_core::remote_launch::{FinalStopReason, RemoteAgentRecord};
    use intent_transport::node_link::NodeGitAccess as _;

    /// A session (in workspace `workspace()`) placed on `node`'s lease.
    async fn placed(backend: &Backend, node: &RemoteNode, machine: &Machine) -> AgentId {
        let agent = AgentId::new();
        let now = intent_core::now_iso();
        let _ = backend
            .store()
            .insert_workspace(&intent_core::Workspace {
                id: workspace(),
                title: "Remote".into(),
                branch: "main".into(),
                repository_path: Some(std::env::temp_dir().to_string_lossy().into_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
                ..Default::default()
            })
            .await;
        sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'remote', 'idle', 'mock', ?, ?)")
            .bind(agent.as_str())
            .bind(workspace().as_str())
            .bind(&now)
            .bind(&now)
            .execute(backend.store().write_pool())
            .await
            .unwrap();
        backend
            .store()
            .record_remote_agent(&RemoteAgentRecord {
                agent_id: agent.clone(),
                workspace_id: workspace(),
                node_id: machine.id(),
                lease_id: node.status().lease_id,
                created_at: now,
            })
            .await
            .unwrap();
        agent
    }

    /// End `agent`'s session: retire it, or mark it deleted.
    async fn end_session(backend: &Backend, agent: &AgentId, deleted: bool) {
        let sql = if deleted {
            "UPDATE agent_session SET status = 'deleted' WHERE id = ?"
        } else {
            "UPDATE agent_session SET retired_at = '2026-11-03T00:00:00Z' WHERE id = ?"
        };
        sqlx::query(sql)
            .bind(agent.as_str())
            .execute(backend.store().write_pool())
            .await
            .unwrap();
    }

    async fn final_stop_for(link: &mut NodeEnd, agent: &AgentId) -> StopId {
        link.expect(|frame| match frame {
            LinkFrame::Stop {
                agent_id,
                stop_id,
                final_stop: true,
            } if agent_id == *agent => Some(stop_id),
            _ => None,
        })
        .await
    }

    async fn ack_final(link: &NodeEnd, stop_id: StopId, agent: &AgentId, push: FinalPush) {
        link.link
            .send(&LinkFrame::StopAck {
                stop_id,
                agent_id: agent.clone(),
                outcome: StopOutcome::Stopped,
                final_push: Some(push),
            })
            .await
            .unwrap();
    }

    async fn until(
        what: &str,
        mut check: impl FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>>,
    ) {
        tokio::time::timeout(wait(), async {
            while !check().await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    #[tokio::test]
    async fn a_final_stop_survives_a_restart_and_its_ack_drops_the_records() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        machine.stop().await;
        let node = backend.bare(&machine).await;
        let agent = placed(&backend, &node, &machine).await;

        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let recorded = backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recorded.stop_id, stop_id.to_string());
        assert_eq!(recorded.reason, FinalStopReason::AgentDeleted);
        // A second request keeps the first record and its id.
        assert_eq!(
            node.final_stop(&agent, FinalStopReason::AgentRetired).await,
            stop_id
        );

        // The restart: the in-memory queue is gone, the record is not.
        node.shutdown();
        let node = backend.bare(&machine).await;
        // Loading it begins the agent's final stop before any link, so its
        // scripts are refused as ending until the ack (review of intentd#3005).
        assert_eq!(
            node.shared_ref().final_begins_for_test(),
            vec![agent.clone()]
        );
        machine.restart().await;
        let mut link = machine.accept().await;
        assert!(link.hello.lease_agents.contains(&agent));
        assert_eq!(final_stop_for(&mut link, &agent).await, stop_id);
        ack_final(&link, stop_id, &agent, FinalPush::Pushed).await;

        let store = backend.store().clone();
        until("the ack drops both records", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move {
                store.get_final_stop(&agent).await.unwrap().is_none()
                    && store.get_remote_agent(&agent).await.unwrap().is_none()
            })
        })
        .await;
        node.shutdown();
    }

    /// A connector shut down before its dial loop first ran (a restart
    /// replacing it at once) binds no link: one would claim the lease and
    /// then close under the connector that replaced it (intent#8611).
    #[tokio::test]
    async fn a_connector_shut_down_before_it_dials_binds_no_link() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        machine.stop().await;
        let node = backend.bare(&machine).await;
        let agent = placed(&backend, &node, &machine).await;
        node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        node.shutdown();
        // The first connector's refused dials end while the machine is away.
        tokio::time::sleep(Duration::from_millis(200)).await;
        machine.restart().await;

        // The pending final stop wants a link, but this connector is shut
        // down before its loop is first polled (single-threaded runtime).
        let stale = backend.bare(&machine).await;
        stale.shutdown();
        machine.no_link(Duration::from_millis(500)).await;

        // Its replacement binds and sends the final stop.
        let node = backend.bare(&machine).await;
        let mut link = machine.accept().await;
        assert!(link.hello.lease_agents.contains(&agent));
        final_stop_for(&mut link, &agent).await;
        node.shutdown();
    }

    #[tokio::test]
    async fn a_retired_agents_placement_outlives_its_final_stop() {
        let (mut machine, backend, node) = exposed().await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentRetired).await;
        assert_eq!(final_stop_for(&mut link, &agent).await, stop_id);
        ack_final(&link, stop_id, &agent, FinalPush::Failed).await;
        let store = backend.store().clone();
        until("the ack drops the final stop", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move { store.get_final_stop(&agent).await.unwrap().is_none() })
        })
        .await;
        assert!(backend
            .store()
            .get_remote_agent(&agent)
            .await
            .unwrap()
            .is_some());
        node.shutdown();
    }

    /// `agent.delete` records the final stop before it stops the runtime: the
    /// plain stop that stop sends joins the queued final stop (same id, still
    /// final) instead of replacing it.
    #[tokio::test]
    async fn a_plain_stop_after_a_final_stop_stays_final() {
        let (mut machine, backend, node) = exposed().await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        assert_eq!(final_stop_for(&mut link, &agent).await, stop_id);
        node.shared().request_stop(&agent);
        for _ in 0..3 {
            let (sent, is_final) = link
                .expect(|frame| match frame {
                    LinkFrame::Stop {
                        agent_id,
                        stop_id,
                        final_stop,
                    } if agent_id == agent => Some((stop_id, final_stop)),
                    _ => None,
                })
                .await;
            assert_eq!(sent, stop_id, "the plain stop replaced the final one");
            assert!(is_final, "the plain stop replaced the final one");
        }
        ack_final(&link, stop_id, &agent, FinalPush::Pushed).await;
        let store = backend.store().clone();
        until("the ack drops the final stop", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move { store.get_final_stop(&agent).await.unwrap().is_none() })
        })
        .await;
        node.shutdown();
    }

    /// Review of intentd#3044: a plain stop queued while the link was down,
    /// for an agent the machine still reports running at the next bind. The
    /// claim must not park a runtime for it: a launch would take that
    /// runtime at once, and the stopped process's exit would end it.
    #[tokio::test]
    async fn a_relaunch_after_a_stop_queued_while_unbound_survives_the_old_exit() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        let node = backend.bare(&machine).await;
        let agent = placed(&backend, &node, &machine).await;
        node.shared().request_stop(&agent);
        *machine.host.running.lock().unwrap() = vec![agent.clone()];
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        link.replay_complete(0).await;
        bound(&node).await;
        let launching = {
            let node = node.clone();
            let command = super::stop_acks::launch_of(&agent);
            tokio::spawn(async move { node.launch(command).await })
        };
        let stop_id = link.stop_for(&agent).await;
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        link.journal(exited(1, &agent)).await;
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) if command.agent_id == agent => Some(()),
            _ => None,
        })
        .await;
        link.acked(1).await;
        let relaunched = tokio::time::timeout(wait(), launching)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            relaunched.is_running(),
            "the old process's exit ended the relaunched runtime: {:?}",
            relaunched.exit_error()
        );
        assert!(!node.shared_ref().is_parked(&agent));
        node.shutdown();
    }

    /// Review of intentd#3044: a plain stop queued for an agent whose
    /// session has ended still lets the claim upgrade it to a recorded
    /// final stop (checkout and placement cleanup), and nothing is parked.
    #[tokio::test]
    async fn an_ended_session_with_a_plain_stop_queued_still_gets_a_final_stop_at_bind() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        let node = backend.bare(&machine).await;
        let agent = placed(&backend, &node, &machine).await;
        end_session(&backend, &agent, false).await;
        node.shared().request_stop(&agent);
        *machine.host.running.lock().unwrap() = vec![agent.clone()];
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        link.replay_complete(0).await;
        let stop_id = final_stop_for(&mut link, &agent).await;
        let row = backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .expect("the ended session's stop is recorded as final");
        assert_eq!(row.stop_id, stop_id.to_string());
        assert_eq!(row.reason, FinalStopReason::AgentRetired);
        assert!(!node.shared_ref().is_parked(&agent));
        assert!(!node.shared_ref().agents.is_attached(&agent));
        node.shutdown();
    }

    /// #8428: an agent still running at bind whose session was retired or
    /// deleted, with no final stop recorded (a head that crashed between the
    /// session change and the stop, or one from before final stops), gets a
    /// recorded final stop with the matching reason, is never parked or
    /// attached, and its last push is admitted.
    #[tokio::test]
    async fn an_ended_session_running_at_bind_gets_a_recorded_final_stop() {
        for (deleted, reason) in [
            (false, FinalStopReason::AgentRetired),
            (true, FinalStopReason::AgentDeleted),
        ] {
            let backend = Backend::new().await;
            let mut machine = Machine::start(&backend.id().await).await;
            backend.expose(&mut machine).await.shutdown();
            let node = backend.bare(&machine).await;
            let agent = placed(&backend, &node, &machine).await;
            end_session(&backend, &agent, deleted).await;
            assert!(backend
                .store()
                .get_final_stop(&agent)
                .await
                .unwrap()
                .is_none());
            *machine.host.running.lock().unwrap() = vec![agent.clone()];

            node.connect().await.unwrap();
            let mut link = machine.accept().await;
            link.replay_complete(0).await;
            let stop_id = final_stop_for(&mut link, &agent).await;
            let row = backend
                .store()
                .get_final_stop(&agent)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.stop_id, stop_id.to_string(), "deleted: {deleted}");
            assert_eq!(row.reason, reason);
            assert!(!node.shared_ref().is_parked(&agent));
            assert!(!node.shared_ref().agents.is_attached(&agent));
            bound(&node).await;
            let access = crate::git::HeadGitAccess::new(node.shared());
            let remote =
                intent_git::node_git::AgentRemote::new(workspace().as_str(), agent.as_str())
                    .unwrap();
            assert!(access
                .repository(
                    &link.hello.binding,
                    &remote,
                    intent_git::node_git::GitService::ReceivePack
                )
                .await
                .is_ok());
            node.shutdown();
        }
    }

    /// `leaseAgents` keeps live agents and pending final stops, and leaves
    /// out agents whose session ended with no final stop pending (an ack
    /// below protocol 9, a stop past its bound, or one from before final
    /// stops), so the machine sweeps their checkouts.
    #[tokio::test]
    async fn lease_agents_leave_out_ended_sessions_without_a_pending_final_stop() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        machine.stop().await;
        let node = backend.bare(&machine).await;
        let live = placed(&backend, &node, &machine).await;
        let retired = placed(&backend, &node, &machine).await;
        let deleted = placed(&backend, &node, &machine).await;
        let pending = placed(&backend, &node, &machine).await;
        end_session(&backend, &retired, false).await;
        end_session(&backend, &deleted, true).await;
        end_session(&backend, &pending, false).await;
        node.final_stop(&pending, FinalStopReason::AgentRetired)
            .await;

        machine.restart().await;
        let link = machine.accept().await;
        let mut listed = link.hello.lease_agents.clone();
        listed.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = vec![live, pending];
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(listed, expected);
        assert!(backend
            .store()
            .get_remote_agent(&retired)
            .await
            .unwrap()
            .is_some());
        node.shutdown();
    }

    /// The bound is checked on a timer, not only at bind: a machine that
    /// stays away stops being redialed once its final stops age out.
    #[tokio::test]
    async fn a_final_stop_ages_out_while_its_machine_stays_away() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        machine.stop().await;
        let clock = crate::final_stop::Clock::default();
        let mut quick = tuning();
        quick.clock = clock.clone();
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            node.shared_ref().has_pending_stops(),
            "kept within its bound"
        );
        assert!(backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .is_some());

        clock.offset_secs.store(
            i64::try_from(intent_core::remote_launch::FINAL_STOP_BOUND.as_secs()).unwrap(),
            std::sync::atomic::Ordering::SeqCst,
        );
        let store = backend.store().clone();
        until("the timer drops the expired final stop", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move { store.get_final_stop(&agent).await.unwrap().is_none() })
        })
        .await;
        assert!(backend
            .store()
            .get_remote_agent(&agent)
            .await
            .unwrap()
            .is_none());
        assert!(!node.shared_ref().has_pending_stops());
        // The expired stop ends the agent's final stop too: its machine runs
        // settle and its scripts are no longer refused as ending (review of
        // intentd#3005).
        assert_eq!(
            node.shared_ref().final_settles_for_test(),
            vec![agent.clone()]
        );
        // Nothing is left to send: the machine coming back is not dialed.
        machine.restart().await;
        machine.no_link(Duration::from_millis(300)).await;
        node.shutdown();
    }

    /// The expiry timer and the dial loop can both list one expired final
    /// stop: only the load that drops its record settles it, so the
    /// machine runs settle once (intent#8610).
    #[tokio::test]
    async fn two_loads_of_one_expired_final_stop_settle_it_once() {
        let backend = Backend::new().await;
        let mut machine = Machine::start(&backend.id().await).await;
        backend.expose(&mut machine).await.shutdown();
        machine.stop().await;
        let clock = crate::final_stop::Clock::default();
        let mut quick = tuning();
        quick.clock = clock.clone();
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let shared = node.shared_ref();
        // Both loads listed the record before either dropped it.
        let listed = shared.list_final_stops_for_test().await;
        assert_eq!(listed.len(), 1);
        clock.offset_secs.store(
            i64::try_from(intent_core::remote_launch::FINAL_STOP_BOUND.as_secs()).unwrap(),
            std::sync::atomic::Ordering::SeqCst,
        );
        shared.settle_twice_for_test(listed).await;
        assert!(backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .is_none());
        // The timer or the dial loop may have dropped it first; either
        // way it settles exactly once.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        node.shutdown();
    }

    /// A final stop can expire while its ack is in flight: the expiry
    /// drops the record and settles the stop, so the ack that comes back
    /// next settles nothing (review of intentd#3098).
    #[tokio::test]
    async fn an_ack_in_flight_when_its_final_stop_expires_settles_nothing() {
        ack_in_flight_at_expiry(false).await;
    }

    /// The same below nodeProtocol 9, where the ack would start the grace.
    #[tokio::test]
    async fn a_legacy_ack_in_flight_when_its_final_stop_expires_settles_nothing() {
        ack_in_flight_at_expiry(true).await;
    }

    async fn ack_in_flight_at_expiry(legacy: bool) {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        backend.expose(&mut machine).await.shutdown();
        let clock = crate::final_stop::Clock::default();
        let mut quick = tuning();
        quick.clock = clock.clone();
        // One send stays in flight until the ack: no resend times it out.
        quick.stop_resend = Backoff::new(wait(), wait());
        // Only this test's explicit load expires the stop.
        quick.final_stop_check = Duration::from_secs(3600);
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        let shared = node.shared_ref();
        if legacy {
            shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        }
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                _ => None,
            })
            .await;
        assert_eq!(sent, stop_id);
        assert!(shared.resending_for_test(stop_id), "the ack is awaited");

        // The bound passes before the ack: the expiry drops and settles it.
        clock.offset_secs.store(
            i64::try_from(intent_core::remote_launch::FINAL_STOP_BOUND.as_secs()).unwrap(),
            std::sync::atomic::Ordering::SeqCst,
        );
        shared.load_final_stops().await;
        assert!(backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .is_none());
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);

        // The ack the send awaited arrives: its resender handles it and ends.
        if legacy {
            link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        } else {
            ack_final(&link, stop_id, &agent, FinalPush::Pushed).await;
        }
        tokio::time::timeout(wait(), async {
            while shared.resending_for_test(stop_id) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the resender handles the ack");
        assert_eq!(
            shared.final_settles_for_test(),
            vec![agent.clone()],
            "the expiry settled it; the late ack must not settle it again"
        );
        assert!(!shared.in_grace_for_test(stop_id), "no grace after expiry");
        node.shutdown();
    }

    /// A load can list a final stop's record just before its ack drops it
    /// (at once, or after the grace below nodeProtocol 9): the stop is
    /// settled, so that load queues it again for nothing, and below 9 its
    /// second ack would settle it again (review of intentd#3102).
    #[tokio::test]
    async fn a_load_listed_before_the_ack_dropped_the_record_queues_nothing() {
        stale_listing_after_ack(false).await;
    }

    #[tokio::test]
    async fn a_load_listed_before_the_grace_dropped_the_record_queues_nothing() {
        stale_listing_after_ack(true).await;
    }

    async fn stale_listing_after_ack(legacy: bool) {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        backend.expose(&mut machine).await.shutdown();
        let mut quick = tuning();
        quick.final_stop_check = Duration::from_secs(3600);
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        let shared = node.shared_ref();
        if legacy {
            shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        }
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                _ => None,
            })
            .await;
        assert_eq!(sent, stop_id);
        // A load lists the record before the ack's drop.
        let listed = shared.list_final_stops_for_test().await;
        assert_eq!(listed.len(), 1);
        if legacy {
            link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        } else {
            ack_final(&link, stop_id, &agent, FinalPush::Pushed).await;
        }
        let store = backend.store().clone();
        until("the record is dropped and the grace is over", || {
            let store = store.clone();
            let agent = agent.clone();
            let in_grace = shared.in_grace_for_test(stop_id);
            let resending = shared.resending_for_test(stop_id);
            Box::pin(async move {
                !in_grace && !resending && store.get_final_stop(&agent).await.unwrap().is_none()
            })
        })
        .await;
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        assert!(!shared.has_pending_stops());

        // That load goes on with its earlier listing.
        shared.settle_listed_for_test(listed).await;
        assert!(
            !shared.has_pending_stops(),
            "a settled stop is not queued again"
        );
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        node.shutdown();
    }

    /// Below nodeProtocol 9 the ack settles the stop and its record waits
    /// out the grace. A load that listed the record before the grace task
    /// dropped it must not settle it again, even once the stop is past its
    /// bound (review of intentd#3098).
    #[tokio::test]
    async fn a_load_during_the_grace_tasks_drop_settles_nothing() {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        backend.expose(&mut machine).await.shutdown();
        let clock = crate::final_stop::Clock::default();
        let gate = Arc::new((tokio::sync::Notify::new(), tokio::sync::Notify::new()));
        let mut quick = tuning();
        quick.clock = clock.clone();
        quick.grace_gate = crate::connector::GraceGate(Some(gate.clone()));
        // Only this test's explicit load expires the stop.
        quick.final_stop_check = Duration::from_secs(3600);
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        let shared = node.shared_ref();
        shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                _ => None,
            })
            .await;
        assert_eq!(sent, stop_id);
        // A load lists the record before the grace task drops it.
        let listed = shared.list_final_stops_for_test().await;
        assert_eq!(listed.len(), 1);
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;

        // The grace task stops at its gate, between its two steps.
        tokio::time::timeout(wait(), gate.0.notified())
            .await
            .expect("the grace task reaches its gate");
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        clock.offset_secs.store(
            i64::try_from(intent_core::remote_launch::FINAL_STOP_BOUND.as_secs()).unwrap(),
            std::sync::atomic::Ordering::SeqCst,
        );
        shared.settle_listed_for_test(listed).await;
        assert_eq!(
            shared.final_settles_for_test(),
            vec![agent.clone()],
            "the ack settled it; the load must not settle it again"
        );
        gate.1.notify_one();
        let store = backend.store().clone();
        until("the grace task drops the record", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move { store.get_final_stop(&agent).await.unwrap().is_none() })
        })
        .await;
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        node.shutdown();
    }

    /// Below nodeProtocol 9 the record (and placement) outlive the ack by
    /// the grace, so a second final stop of the agent then (deleted, then
    /// its workspace retired) gets the settled stop's id back. It must not
    /// begin the stop again: its ack settles nothing, so that begin would
    /// leave the agent's machine runs ending for good (review of
    /// intentd#3102).
    #[tokio::test]
    async fn a_second_final_stop_during_the_grace_begins_nothing() {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        backend.expose(&mut machine).await.shutdown();
        let mut quick = tuning();
        quick.final_stop_check = Duration::from_secs(3600);
        // The grace never ends during the test: the record stays.
        quick.legacy_push_grace = Duration::from_secs(3600);
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        let shared = node.shared_ref();
        shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                _ => None,
            })
            .await;
        assert_eq!(sent, stop_id);
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        tokio::time::timeout(wait(), async {
            while shared.resending_for_test(stop_id) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the resender handles the ack");
        assert!(shared.in_grace_for_test(stop_id));
        assert_eq!(shared.final_settles_for_test(), vec![agent.clone()]);
        assert_eq!(shared.final_begin_count_for_test(), 1);

        // The second final stop, within the grace: same record, same id.
        assert_eq!(
            node.final_stop(&agent, FinalStopReason::WorkspaceRetired)
                .await,
            stop_id
        );
        assert_eq!(
            shared.final_begin_count_for_test(),
            shared.final_settles_for_test().len(),
            "every begin of the agent's final stop is matched by a settle"
        );
        assert!(!shared.has_pending_stops(), "nothing left to acknowledge");
        node.shutdown();
    }

    /// A bind can queue a settled final stop again (the machine reports
    /// its agent running while the record lingers): it is sent, so the
    /// agent stops, but it begins nothing, since its ack settles nothing.
    #[tokio::test]
    async fn a_settled_final_stop_queued_again_at_bind_begins_nothing() {
        let backend = Backend::new().await;
        let mut machine = backend.machine().await;
        backend.expose(&mut machine).await.shutdown();
        let mut quick = tuning();
        quick.final_stop_check = Duration::from_secs(3600);
        quick.legacy_push_grace = Duration::from_secs(3600);
        let node = backend.bare_with(&machine, quick).await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        let shared = node.shared_ref();
        shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id, stop_id, ..
                } if agent_id == agent => Some(stop_id),
                _ => None,
            })
            .await;
        assert_eq!(sent, stop_id);
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        tokio::time::timeout(wait(), async {
            while shared.resending_for_test(stop_id) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the resender handles the ack");
        assert_eq!(shared.final_begin_count_for_test(), 1);

        // As the bind's claim does for a recorded stop of a running agent.
        shared.queue_final_stop_for_test(&agent, stop_id);
        assert!(shared.has_pending_stops(), "sent again, so the agent stops");
        assert_eq!(
            shared.final_begin_count_for_test(),
            shared.final_settles_for_test().len(),
            "every begin of the agent's final stop is matched by a settle"
        );
        node.shutdown();
    }

    /// `remoteMachines.remove` waits the longer budget only on a link that
    /// speaks final stops.
    #[tokio::test]
    async fn the_remove_budget_follows_the_links_protocol() {
        let (_machine, _backend, node) = exposed().await;
        let shared = node.shared_ref();
        let mut expected = tuning();
        expected.remove_timeout = Duration::from_secs(30);
        expected.final_remove_timeout = Duration::from_secs(90);
        shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        assert_eq!(shared.remove_budget(), shared.tuning_ref().remove_timeout);
        shared.set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL);
        assert_eq!(
            shared.remove_budget(),
            shared.tuning_ref().final_remove_timeout
        );
        let defaults = crate::connector::Tuning::default();
        assert_eq!(defaults.remove_timeout, expected.remove_timeout);
        assert_eq!(defaults.final_remove_timeout, expected.final_remove_timeout);
        assert_eq!(defaults.stop_wait, Duration::from_secs(30));
        node.shutdown();
    }

    /// Below nodeProtocol 9 the ack may precede the exit push: the record,
    /// and the push it admits, outlive the ack by the grace, then go.
    #[tokio::test]
    async fn below_protocol_9_the_record_outlives_the_ack_by_the_grace() {
        let (mut machine, backend, node) = exposed().await;
        let agent = placed(&backend, &node, &machine).await;
        node.connect().await.unwrap();
        let mut link = machine.accept().await;
        bound(&node).await;
        // As a protocol-8 machine would see it: the stop is not final.
        node.shared_ref()
            .set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
        let stop_id = node.final_stop(&agent, FinalStopReason::AgentDeleted).await;
        let sent = link
            .expect(|frame| match frame {
                LinkFrame::Stop {
                    agent_id,
                    stop_id,
                    final_stop,
                } if agent_id == agent => Some((stop_id, final_stop)),
                _ => None,
            })
            .await;
        assert_eq!(sent, (stop_id, false));
        link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            backend
                .store()
                .get_final_stop(&agent)
                .await
                .unwrap()
                .is_some(),
            "the push is still admitted within the grace"
        );
        assert!(!node.shared_ref().has_pending_stops(), "acknowledged once");
        // The agent is still ended for good: its machine runs settle and its
        // scripts may no longer be refused as ending (review of intentd#3005).
        assert_eq!(
            node.shared_ref().final_settles_for_test(),
            vec![agent.clone()]
        );
        let store = backend.store().clone();
        until("the grace ends", || {
            let store = store.clone();
            let agent = agent.clone();
            Box::pin(async move { store.get_final_stop(&agent).await.unwrap().is_none() })
        })
        .await;
        node.shutdown();
    }
}

/// A machine pushes an unsolicited `capabilitiesReply` when its enabled
/// providers change: the backend records it like a requested one, so a
/// provider the machine disabled stops being offered at once and one it
/// enables again comes back.
#[tokio::test]
async fn an_unsolicited_capabilities_report_replaces_the_recorded_one() {
    let (mut machine, backend, node) = exposed().await;
    let agent = AgentId::new();
    let _runtime = node.attach(&agent, &workspace());
    let link = machine.accept().await;
    link.replay_complete(0).await;
    bound(&node).await;

    let recorded_providers = || async {
        backend
            .store()
            .get_exposed_machine(&machine.id())
            .await
            .unwrap()
            .unwrap()
            .capabilities
            .map(|capabilities| {
                capabilities
                    .providers
                    .into_iter()
                    .map(|provider| provider.id)
                    .collect::<Vec<_>>()
            })
    };
    let wait_for = |expected: Vec<String>| {
        let recorded_providers = &recorded_providers;
        async move {
            tokio::time::timeout(wait(), async {
                while recorded_providers().await.as_ref() != Some(&expected) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the backend records providers {expected:?}"));
        }
    };

    let mut disabled = capabilities(0);
    disabled.providers.clear();
    link.link
        .send(&LinkFrame::CapabilitiesReply {
            capabilities: disabled,
        })
        .await
        .unwrap();
    wait_for(Vec::new()).await;

    link.link
        .send(&LinkFrame::CapabilitiesReply {
            capabilities: capabilities(0),
        })
        .await
        .unwrap();
    wait_for(vec!["mock".to_owned()]).await;
    node.shutdown();
}

/// nodeProtocol 14 (node-link.md § Secrets on the machine): forgets queued
/// while the machine was away go out before a later `secretPut` of the same
/// agent (one of the name being put may be dropped instead), so a queued
/// forget never wipes the newer, confirmed value.
#[tokio::test]
async fn a_queued_secret_forget_never_lands_after_a_newer_put() {
    use intent_core::node_link::{SecretOutcome, SecretReply, SecretValue};
    use intent_services::feature_api::feature_ports::{MachineSecretAnswer, MachineSecretOp};
    let backend = Backend::new().await;
    let mut machine = Machine::start(&backend.id().await).await;
    backend.expose(&mut machine).await.shutdown();
    machine.stop().await;
    let node = backend.bare(&machine).await;
    let agent = AgentId::new();
    for name in ["API_KEY", "OTHER"] {
        let forget = MachineSecretOp::Forget {
            name: Some(name.to_owned()),
        };
        assert_eq!(
            node.machine_secret(&agent, forget, Duration::from_millis(300))
                .await,
            MachineSecretAnswer::Queued
        );
    }

    machine.restart().await;
    let putting = {
        let node = node.clone();
        let agent = agent.clone();
        tokio::spawn(async move {
            let put = MachineSecretOp::Put {
                name: "API_KEY".into(),
                value: SecretValue::from("v-new").into_inner(),
            };
            node.machine_secret(&agent, put, wait()).await
        })
    };
    let mut link = machine.accept().await;
    let mut forgets = Vec::new();
    let put_id = link
        .expect(|frame| match frame {
            LinkFrame::SecretForget(forget) => {
                forgets.push(forget.name);
                None
            }
            LinkFrame::SecretPut(put) => Some(put.request_id),
            _ => None,
        })
        .await;
    // The bind may flush the queue before the put takes the agent's turn
    // (then both forgets go first); either way nothing follows the put.
    assert!(forgets.contains(&Some("OTHER".to_owned())), "{forgets:?}");
    let reply = |request_id, names| {
        LinkFrame::SecretReply(SecretReply {
            request_id,
            agent_id: agent.clone(),
            outcome: SecretOutcome::Ok,
            names,
            forgotten: None,
            error: None,
        })
    };
    link.link.send(&reply(put_id, None)).await.unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), putting)
            .await
            .unwrap()
            .unwrap(),
        MachineSecretAnswer::Stored
    );

    // The superseded forget never follows: the next secret frame is a list.
    let listing = {
        let node = node.clone();
        let agent = agent.clone();
        tokio::spawn(async move {
            node.machine_secret(&agent, MachineSecretOp::List, wait())
                .await
        })
    };
    let list_id = link
        .expect(|frame| match frame {
            LinkFrame::SecretForget(forget) => panic!("a superseded forget was sent: {forget:?}"),
            LinkFrame::SecretList(list) => Some(list.request_id),
            _ => None,
        })
        .await;
    link.link
        .send(&reply(list_id, Some(vec!["API_KEY".into()])))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), listing)
            .await
            .unwrap()
            .unwrap(),
        MachineSecretAnswer::Names(vec!["API_KEY".into()])
    );
    node.shutdown();
}
