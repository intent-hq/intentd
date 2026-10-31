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
    JournalBody, JournalRecord, LinkFrame, LinkHello, LinkOutcome, LinkWelcome,
    MachineCapabilities,
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
        connect: ConnectTuning {
            attempt: Duration::from_millis(500),
            total: wait(),
        },
        fail_ack_writes: Arc::default(),
    }
}

/// A machine report: macOS on Apple silicon with two agent slots.
fn capabilities(running: u32) -> MachineCapabilities {
    let mut capabilities = MachineCapabilities::new(
        AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        },
        "machine-test",
        2,
        "2026-10-30T00:00:00Z",
    );
    capabilities.capacity.running_agents = running;
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
}

impl ExecutorHost for Host {
    fn enabled(&self) -> bool {
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
    _tmp: TempDb,
}

impl Machine {
    /// A machine that minted a credential for `backend_id`, listening on
    /// `port` (`0` for any).
    async fn start(backend_id: &str, port: u16) -> Self {
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
        });
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
        let mut server = WsApiServer::new(
            services.clone(),
            EventBus::new(store),
            &certificate,
            &tokens,
            WsOptions {
                base_port: port,
                ..WsOptions::default()
            },
            None,
        )
        .unwrap();
        server.install_executor_links(ExecutorLinks::new(host.clone()));
        let port = server.start().await.unwrap();
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

    async fn stop(&self) {
        self.server.stop().await;
    }

    async fn restart(&mut self) {
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

/// A port nothing listens on.
fn free_port() -> u16 {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    reserved.local_addr().unwrap().port()
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

/// The backend: services with the remote feature registered,
/// `experimental.remoteAgents` on, an in-memory secret store and an event
/// bus.
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
        let registry =
            Arc::new(SettingsRegistry::load(config.path().join("config.toml")).unwrap());
        registry
            .apply(&[("experimental.remoteAgents".to_owned(), json!(true))])
            .unwrap();
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
        Machine::start(&self.id().await, free_port()).await
    }

    async fn call(&self, method: &'static str, params: Value) -> intent_core::Result<Value> {
        with_caller(Caller::Daemon, self.services.remote_machines(method, params)).await
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
    let mut machine = Machine::start(&backend.id().await, free_port()).await;
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
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id } if agent_id == early => Some(()),
        _ => None,
    })
    .await;

    // Stopped agents' records are no longer delivered.
    link.replay_complete(0).await;
    link.journal(note(1, &early, "late")).await;
    link.acked(1).await;
    assert!(runtime.notifications().lock().await.try_recv().is_err());

    let live = AgentId::new();
    drop(node.attach(&live, &workspace()));
    link.expect(|frame| match frame {
        LinkFrame::Stop { agent_id } if agent_id == live => Some(()),
        LinkFrame::Stop { agent_id } if agent_id == early => {
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
        LinkFrame::Stop { agent_id } if agent_id == agent => Some(()),
        _ => None,
    })
    .await;
    node.shutdown();
}

#[tokio::test]
async fn a_prompt_waiting_for_the_link_is_not_sent_after_a_stop() {
    let backend = Backend::new().await;
    let mut machine = Machine::start(&backend.id().await, free_port()).await;
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
        LinkFrame::Stop { agent_id } if agent_id == agent => Some(()),
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
