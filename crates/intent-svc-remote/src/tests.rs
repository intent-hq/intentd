//! The connector and the remote runtime against a fake node on the real
//! pinned-TLS node listener.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use intent_acp::session::{ActivityTracker, ContentBlock, StopReason};
use intent_acp::AcpError;
use intent_core::agent_runtime::AgentRuntime;
use intent_core::node_link::{
    JournalBody, JournalRecord, LinkFrame, LinkHello, LinkOutcome, LinkWelcome,
};
use intent_core::nodes::{LeaseFence, LeaseFenceKind, LeaseState, NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::{AgentId, WorkspaceId};
use intent_services::feature_api::tests::TempDb;
use intent_store::Store;
use intent_transport::node_link::{
    BindingFile, LeaseToken, LinkEvent, LinkEvents, NodeConnection, NodeLink, NodeListener,
    NodeListenerConfig, WelcomeFn,
};
use intent_transport::{generate_certificate, TlsCertificate};

use crate::config::HeadNodeConfig;
use crate::connector::{RemoteNode, Tuning};
use intent_transport::node_link::Backoff;

const NODE: &str = "node-1";

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
        fail_ack_writes: Arc::default(),
    }
}

fn token() -> LeaseToken {
    LeaseToken::new("t".repeat(40)).unwrap()
}

fn welcome() -> WelcomeFn {
    welcome_running(Vec::new())
}

/// A welcome reporting `running` as the node's running agents.
fn welcome_running(running: Vec<AgentId>) -> WelcomeFn {
    Arc::new(move |_| LinkWelcome {
        protocol: 0,
        node_id: String::new(),
        running_agents: running.clone(),
        capabilities: intent_core::node_link::MachineCapabilities::new(
            AgentPlatform {
                os: NodeOs::Macos,
                arch: NodeArch::Aarch64,
            },
            "node-test",
            2,
            "2026-10-30T00:00:00Z",
        ),
        hosts: Vec::new(),
        tc_address: None,
    })
}

/// A node that hands every admitted link to the test.
struct FakeNode {
    listener: NodeListener,
    certificate: TlsCertificate,
    git: Vec<intent_transport::node_link::GitConnection>,
    _dir: tempfile::TempDir,
}

impl FakeNode {
    async fn start() -> Self {
        Self::start_at("127.0.0.1:0".parse().unwrap()).await
    }

    async fn start_at(address: SocketAddr) -> Self {
        Self::start_with(address, welcome()).await
    }

    async fn start_with(address: SocketAddr, welcome: WelcomeFn) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let certificate =
            generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let listener = Self::listen_with(&dir, &certificate, address, welcome).await;
        Self {
            listener,
            certificate,
            git: Vec::new(),
            _dir: dir,
        }
    }

    async fn listen(
        dir: &tempfile::TempDir,
        certificate: &TlsCertificate,
        address: SocketAddr,
    ) -> NodeListener {
        Self::listen_with(dir, certificate, address, welcome()).await
    }

    async fn listen_with(
        dir: &tempfile::TempDir,
        certificate: &TlsCertificate,
        address: SocketAddr,
        welcome: WelcomeFn,
    ) -> NodeListener {
        NodeListener::bind(
            address,
            NodeListenerConfig {
                node_id: NODE.into(),
                token: token(),
                binding_file: BindingFile::new(dir.path().join("binding.json")),
                certificate: certificate.clone(),
            },
            welcome,
        )
        .await
        .unwrap()
    }

    fn config(&self) -> HeadNodeConfig {
        config_for(self.listener.local_addr(), &self.certificate)
    }

    /// The next link. Head's git connections, dialed right after each
    /// bind, are kept in `git` (the fake serves no git).
    async fn accept(&mut self) -> NodeEnd {
        loop {
            match tokio::time::timeout(wait(), self.listener.accept()).await {
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
}

fn config_for(address: SocketAddr, certificate: &TlsCertificate) -> HeadNodeConfig {
    HeadNodeConfig::parse(
        json!({
            "nodeId": NODE,
            "name": "Test node",
            "endpoint": format!("wss://{address}"),
            "certificateSha256": certificate.fingerprint256,
            "token": token().expose(),
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap()
}

/// The node's end of one bound link.
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

struct Head {
    _tmp: TempDb,
    store: Store,
}

impl Head {
    async fn new() -> Self {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        Self { _tmp: tmp, store }
    }

    async fn start(&self, config: HeadNodeConfig) -> RemoteNode {
        RemoteNode::start_with(self.store.clone(), config, tuning(), None)
            .await
            .unwrap()
    }

    async fn persisted_ack(&self, node: &RemoteNode) -> NodeCounter {
        let lease_id = node.status().lease_id;
        let head_id = self.store.node_head_id().await.unwrap().unwrap();
        self.store
            .list_owned_node_leases(&head_id)
            .await
            .unwrap()
            .into_iter()
            .find(|lease| lease.owner.lease_id == lease_id)
            .unwrap()
            .ack_seq
    }
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

#[tokio::test]
async fn ingests_in_order_and_acks_the_highest_contiguous_sequence() {
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let mut link = fake.accept().await;
    assert_eq!(link.hello.acked_seq, NodeCounter(0));
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    link.replay_complete(0).await;
    for (seq, text) in [(1, "one"), (2, "two"), (3, "three")] {
        link.journal(note(seq, &agent, text)).await;
    }
    link.acked(3).await;
    assert_eq!(texts(&runtime, 3).await, ["one", "two", "three"]);
    assert_eq!(node.ingested(), NodeCounter(3));
    assert_eq!(head.persisted_ack(&node).await, NodeCounter(3));

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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let mut first = fake.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(2, &agent, "two")).await;
    first.acked(2).await;
    first.link.close();

    let mut second = fake.accept().await;
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
async fn a_restarted_head_resumes_from_the_persisted_ack() {
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let _runtime = node.attach(&agent, &workspace());
    let mut first = fake.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(2, &agent, "two")).await;
    first.acked(2).await;
    node.shutdown();
    first.closed_by_head().await;

    let restarted = head.start(fake.config()).await;
    let second = fake.accept().await;
    assert_eq!(second.hello.acked_seq, NodeCounter(2));
    assert_eq!(second.hello.binding.head_id, first.hello.binding.head_id);
    assert_eq!(second.hello.binding.lease_id, first.hello.binding.lease_id);
    assert!(second.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(restarted.ingested(), NodeCounter(2));
    restarted.shutdown();
}

#[tokio::test]
async fn a_live_gap_rebinds_and_resumes_from_the_last_ack() {
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let mut first = fake.accept().await;
    first.replay_complete(0).await;
    first.journal(note(1, &agent, "one")).await;
    first.journal(note(3, &agent, "three")).await;
    first.closed_by_head().await;

    let mut second = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
    let prompt = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let activity = ActivityTracker::new();
            runtime.prompt("s", text_prompt("go"), &activity).await
        })
    };
    link.expect(|frame| matches!(frame, LinkFrame::Prompt { .. }).then_some(()))
        .await;
    // The node goes away and does not come back.
    drop(fake);
    link.link.close();
    match tokio::time::timeout(wait(), prompt).await.unwrap().unwrap() {
        Err(AcpError::Transport(message)) => {
            assert!(message.starts_with("node-link-unavailable"), "{message}");
        }
        other => panic!("expected a link-loss error, got {other:?}"),
    }
    assert!(runtime.is_alive(), "the agent may still run on the node");
    assert!(matches!(
        runtime.cancel("s").await,
        Err(AcpError::Transport(_))
    ));
    node.shutdown();
}

#[tokio::test]
async fn a_stop_is_sent_once_now_or_after_the_next_bind() {
    // Reserve a port with nothing listening yet.
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let certificate = generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let head = Head::new().await;
    let node = head.start(config_for(address, &certificate)).await;

    let early = AgentId::new();
    let runtime = node.attach(&early, &workspace());
    runtime.stop().await;
    runtime.stop().await;
    assert!(!runtime.is_alive());

    let dir = tempfile::tempdir().unwrap();
    let listener = FakeNode::listen(&dir, &certificate, address).await;
    let mut fake = FakeNode {
        listener,
        certificate,
        git: Vec::new(),
        _dir: dir,
    };
    let mut link = fake.accept().await;
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
async fn node_requests_are_answered_as_unsupported() {
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let link = fake.accept().await;
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
async fn a_node_whose_lease_ended_is_not_bound_again() {
    let fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    node.shutdown();
    let head_id = head.store.node_head_id().await.unwrap().unwrap();
    let lease = head.store.list_owned_node_leases(&head_id).await.unwrap()[0]
        .owner
        .clone();
    head.store.request_node_lease_release(&lease).await.unwrap();
    head.store
        .finish_node_lease(
            &lease,
            LeaseState::Released,
            LeaseFence {
                kind: LeaseFenceKind::ProcessesStopped,
                recorded_at: intent_core::now_iso(),
            },
        )
        .await
        .unwrap();
    let error = RemoteNode::start_with(head.store.clone(), fake.config(), tuning(), None)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("remote-node-lease-ended"),
        "{error}"
    );
}

mod config {
    use super::*;

    fn raw(token: &str) -> Value {
        json!({
            "nodeId": "build-box",
            "endpoint": "wss://10.0.0.5:7443/",
            "certificateSha256": "AB:".repeat(31) + "AB",
            "token": token,
        })
    }

    #[test]
    fn a_valid_configuration_parses() {
        let config = HeadNodeConfig::parse(raw(&"s".repeat(32)).to_string().as_bytes()).unwrap();
        assert_eq!(config.node_id, "build-box");
        assert_eq!(config.name, "build-box", "the name defaults to the node id");
        assert_eq!(config.endpoint.url, "wss://10.0.0.5:7443");
        assert_eq!(config.certificate_sha256, "ab".repeat(32));
        assert!(!format!("{config:?}").contains(&"s".repeat(32)));
    }

    #[test]
    fn errors_never_quote_the_token() {
        let secret = "SECRET-".repeat(6);
        let cases = [
            json!({"nodeId": "n", "endpoint": "wss://h:1", "certificateSha256": "ab".repeat(32), "token": secret, "extra": secret}),
            json!({"nodeId": "n", "endpoint": "https://h:1", "certificateSha256": "ab".repeat(32), "token": secret}),
            json!({"nodeId": "n", "endpoint": "wss://h:1", "certificateSha256": "nope", "token": secret}),
            json!({"nodeId": "bad id", "endpoint": "wss://h:1", "certificateSha256": "ab".repeat(32), "token": secret}),
            json!({"nodeId": "n", "endpoint": "wss://user@h:1", "certificateSha256": "ab".repeat(32), "token": secret}),
            json!({"nodeId": "n", "endpoint": "wss://h:1", "certificateSha256": "ab".repeat(32), "token": 42}),
        ];
        for case in cases {
            let error = HeadNodeConfig::parse(case.to_string().as_bytes()).unwrap_err();
            let message = error.to_string();
            assert!(message.contains("remote-node-config"), "{message}");
            assert!(!message.contains("SECRET"), "{message}");
        }
        let error = HeadNodeConfig::parse(raw("short").to_string().as_bytes()).unwrap_err();
        assert!(error.to_string().contains("32 printable"));
    }

    #[test]
    fn a_missing_file_is_no_configuration() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            HeadNodeConfig::read_if_present(&dir.path().join(crate::CONFIG_FILE))
                .unwrap()
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_file_must_be_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::CONFIG_FILE);
        std::fs::write(&path, raw(&"s".repeat(32)).to_string()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = HeadNodeConfig::read_if_present(&path).unwrap_err();
        assert!(error.to_string().contains("chmod 600"), "{error}");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(HeadNodeConfig::read_if_present(&path).unwrap().is_some());
    }
}

#[cfg(unix)]
mod services {
    use super::*;
    use crate::RemoteServicesExt as _;
    use intent_services::feature_api::Services;

    #[tokio::test]
    async fn the_connector_starts_only_with_a_configuration() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let services = crate::register(Services::new(store));
        let data = tempfile::tempdir().unwrap();
        assert!(!services.start_remote_node(data.path()).await.unwrap());
        assert!(services.remote_node().is_none());

        let mut fake = FakeNode::start().await;
        let config = json!({
            "nodeId": NODE,
            "endpoint": format!("wss://{}", fake.listener.local_addr()),
            "certificateSha256": fake.certificate.fingerprint256,
            "token": token().expose(),
        });
        let path = data.path().join(crate::CONFIG_FILE);
        std::fs::write(&path, config.to_string()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            !crate::experimental_enabled(),
            "tests run without {}",
            crate::EXPERIMENTAL_ENV
        );
        assert!(
            !services.start_remote_node(data.path()).await.unwrap(),
            "remote agents stay off without the experimental switch"
        );
        assert!(services.remote_node().is_none());
        assert!(crate::start_configured(&services, data.path())
            .await
            .unwrap());
        assert!(
            !crate::start_configured(&services, data.path())
                .await
                .unwrap(),
            "one connector at a time"
        );
        let link = fake.accept().await;
        assert_eq!(link.hello.binding.node_id, NODE);
        let node = services.remote_node().expect("connector registered");
        assert_eq!(node.status().node_name, NODE);
        node.shutdown();
    }
}

#[cfg(unix)]
mod node_mode {
    use super::*;
    use crate::RemoteServicesExt as _;
    use intent_services::feature_api::Services;

    /// A configured head restarted in node mode never starts the outbound
    /// connector, so no remote agent can dispatch forwarded calls into it.
    #[tokio::test]
    async fn node_mode_never_starts_the_head_connector() {
        use std::os::unix::fs::PermissionsExt as _;
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let services = crate::register(Services::new(store).with_node_mode(true));
        let data = tempfile::tempdir().unwrap();
        let mut fake = FakeNode::start().await;
        let config = json!({
            "nodeId": NODE,
            "endpoint": format!("wss://{}", fake.listener.local_addr()),
            "certificateSha256": fake.certificate.fingerprint256,
            "token": token().expose(),
        });
        let path = data.path().join(crate::CONFIG_FILE);
        std::fs::write(&path, config.to_string()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!crate::start_configured(&services, data.path())
            .await
            .unwrap());
        assert!(!services.start_remote_node(data.path()).await.unwrap());
        assert!(
            services.remote_node().is_none(),
            "no connector in node mode"
        );
        let accepted = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            fake.listener.accept(),
        )
        .await;
        assert!(accepted.is_err(), "the node is never dialed");
    }
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let node = head.start(fake.config()).await;
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());
    let mut link = fake.accept().await;
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
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let certificate = generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let head = Head::new().await;
    let mut slow = tuning();
    slow.link_loss_grace = wait();
    let node = RemoteNode::start_with(
        head.store.clone(),
        config_for(address, &certificate),
        slow,
        None,
    )
    .await
    .unwrap();
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

    let dir = tempfile::tempdir().unwrap();
    let listener = FakeNode::listen(&dir, &certificate, address).await;
    let mut fake = FakeNode {
        listener,
        certificate,
        git: Vec::new(),
        _dir: dir,
    };
    let mut link = fake.accept().await;
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
    let mut fake = FakeNode::start().await;
    let head = Head::new().await;
    let flaky = tuning();
    let fail = flaky.fail_ack_writes.clone();
    let node = RemoteNode::start_with(head.store.clone(), fake.config(), flaky, None)
        .await
        .unwrap();
    let agent = AgentId::new();
    let runtime = node.attach(&agent, &workspace());

    let first = fake.accept().await;
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
    // node keeps records 1 and 2.
    let mut second = fake.accept().await;
    assert_eq!(second.hello.acked_seq, NodeCounter(0));
    assert!(second.hello.binding.generation > first.hello.binding.generation);
    assert_eq!(head.persisted_ack(&node).await, NodeCounter(0));

    // Persistence recovers: the replay is deduplicated and the ack advances.
    fail.store(false, std::sync::atomic::Ordering::SeqCst);
    for (seq, text) in [(1, "one"), (2, "two"), (3, "three")] {
        second.journal(note(seq, &agent, text)).await;
    }
    second.replay_complete(3).await;
    second.acked(3).await;
    assert_eq!(texts(&runtime, 1).await, ["three"]);
    assert_eq!(head.persisted_ack(&node).await, NodeCounter(3));
    second.link.close();
    let third = fake.accept().await;
    assert_eq!(third.hello.acked_seq, NodeCounter(3));
    node.shutdown();
}

/// The remote-launch port against a fake node: admission, placing an agent,
/// its first-turn launch, the projected placement, and bind-time claims.
mod launch {
    use std::path::Path;
    use std::process::Command;

    use intent_core::placement::{AgentPlacement, PlacementUnavailableReason};
    use intent_core::remote_launch::{
        EffectiveIsolation, RemoteLaunchRequest, RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY,
    };
    use intent_core::{Error, Workspace};
    use intent_git::node_git::{self, AgentRemote, GitService};
    use intent_services::feature_api::feature_ports::RemoteLaunchPort;
    use intent_services::feature_api::Services;
    use intent_transport::node_link::NodeGitAccess;

    use super::*;

    fn git(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "user.name=t", "-c", "user.email=t@t"])
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    /// Head services with the remote feature, a workspace on a one-commit
    /// repository, and the connector bound to `fake`.
    struct Fixture {
        services: Services,
        node: RemoteNode,
        repo: tempfile::TempDir,
        base: String,
        _tmp: TempDb,
    }

    impl Fixture {
        async fn start(fake: &mut FakeNode) -> (Self, NodeEnd) {
            let tmp = TempDb::new();
            let store = Store::open(&tmp.path).await.unwrap();
            let services = crate::register(Services::new(store.clone()));
            let repo = tempfile::tempdir().unwrap();
            git(repo.path(), &["init", "--quiet"]);
            std::fs::write(repo.path().join("README.md"), "base\n").unwrap();
            git(repo.path(), &["add", "README.md"]);
            git(repo.path(), &["commit", "--quiet", "-m", "base"]);
            let base = git(repo.path(), &["rev-parse", "HEAD"]);
            let now = intent_core::now_iso();
            store
                .insert_workspace(&Workspace {
                    id: workspace(),
                    title: "Remote".into(),
                    branch: "main".into(),
                    repository_path: Some(repo.path().to_string_lossy().into_owned()),
                    created_at: now.clone(),
                    updated_at: now,
                    ..Default::default()
                })
                .await
                .unwrap();
            let calls = crate::rpc::HeadCalls::new(services.clone());
            let node = RemoteNode::start_with(store, fake.config(), tuning(), Some(calls))
                .await
                .unwrap();
            crate::install_node(&services, node.clone());
            let link = fake.accept().await;
            link.replay_complete(0).await;
            let fixture = Self {
                services,
                node,
                repo,
                base,
                _tmp: tmp,
            };
            fixture.connected().await;
            (fixture, link)
        }

        async fn connected(&self) {
            tokio::time::timeout(wait(), async {
                while !self.node.status().connected {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the link binds");
        }

        fn port(&self) -> &dyn RemoteLaunchPort {
            self.services.feature_remote_launch_port()
        }

        /// A created agent, placed on the node with `view`.
        async fn agent(&self, view: &RemotePlacementView) -> AgentId {
            self.agent_with(view, "mock").await
        }

        /// A created agent of `provider`, placed on the node with `view`.
        async fn agent_with(&self, view: &RemotePlacementView, provider: &str) -> AgentId {
            let agent = AgentId::new();
            let now = intent_core::now_iso();
            sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'remote', 'idle', ?, ?, ?)")
                .bind(agent.as_str())
                .bind(workspace().as_str())
                .bind(provider)
                .bind(&now)
                .bind(&now)
                .execute(self.services.store().write_pool())
                .await
                .unwrap();
            self.services
                .store()
                .set_agent_session_metadata_key_json(
                    &workspace(),
                    &agent,
                    REMOTE_PLACEMENT_METADATA_KEY,
                    &serde_json::to_string(view).unwrap(),
                    &now,
                )
                .await
                .unwrap();
            agent
        }

        async fn view(&self, agent: &AgentId) -> RemotePlacementView {
            let session = self
                .services
                .store()
                .get_agent_session(agent)
                .await
                .unwrap();
            RemotePlacementView::from_metadata(session.metadata.as_ref()).unwrap()
        }

        async fn admit(&self, placement: &str) -> intent_core::Result<RemotePlacementView> {
            let placement: AgentPlacement = serde_json::from_str(placement).unwrap();
            self.port()
                .admit(&self.services, &workspace(), &placement)
                .await
        }

        async fn placed(&self) -> (AgentId, RemotePlacementView) {
            let view = self.admit(r#"{"target":"remote"}"#).await.unwrap();
            let agent = self.agent(&view).await;
            self.port()
                .launch(
                    &self.services,
                    RemoteLaunchRequest {
                        workspace_id: workspace(),
                        agent_id: agent.clone(),
                        view: view.clone(),
                    },
                )
                .await
                .unwrap();
            (agent, view)
        }
    }

    fn unavailable(
        result: intent_core::Result<RemotePlacementView>,
    ) -> intent_core::placement::PlacementUnavailable {
        match result {
            Err(Error::PlacementUnavailable(failure)) => *failure,
            other => panic!("expected placement-unavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn admission_matches_the_nodes_platform_and_capacity() {
        let mut fake = FakeNode::start().await;
        let (f, _link) = Fixture::start(&mut fake).await;

        let view = f
            .admit(r#"{"target":"remote","os":"macos","arch":"aarch64"}"#)
            .await
            .unwrap();
        assert_eq!(view.node_id, NODE);
        assert_eq!(view.node_name, "Test node");
        assert_eq!(view.lease_id, f.node.status().lease_id);
        assert_eq!(view.base_commit, f.base);
        assert_eq!(view.effective_isolation, EffectiveIsolation::Pending);
        assert_eq!(
            serde_json::to_value(&view.placement).unwrap(),
            json!({"target":"remote","checkout":"isolated","os":"macos","arch":"aarch64","nodeId":NODE})
        );

        let linux = unavailable(f.admit(r#"{"os":"linux"}"#).await);
        assert_eq!(
            linux.reason,
            PlacementUnavailableReason::UnsupportedPlatform
        );
        assert_eq!(
            linux.message(),
            "Unsupported linux; available: macos/aarch64; supported: macos/aarch64"
        );
        let elsewhere = unavailable(f.admit(r#"{"nodeId":"other-node"}"#).await);
        assert_eq!(
            elsewhere.reason,
            PlacementUnavailableReason::UnsupportedPlatform
        );

        // Both of the node's slots taken: retryable no-capacity.
        let _one = f.node.attach(&AgentId::new(), &workspace());
        let _two = f.node.attach(&AgentId::new(), &workspace());
        let full = unavailable(f.admit(r#"{"target":"remote"}"#).await);
        assert_eq!(full.reason, PlacementUnavailableReason::NoCapacity);
        assert_eq!(full.data()["retryable"], true);
        assert_eq!(
            full.data()["supportedPlatforms"],
            json!([{"os":"macos","arch":"aarch64"}])
        );

        let help = f.port().placement_help(&f.services).await.unwrap();
        assert!(help.contains("\"Test node\""), "{help}");
        assert!(help.contains("platforms: macos/aarch64"), "{help}");
        assert!(
            help.contains("available now: none (2 of 2 agents running)"),
            "{help}"
        );
        assert!(
            help.contains("uncommitted changes are not carried"),
            "{help}"
        );
        f.node.shutdown();
    }

    #[tokio::test]
    async fn an_offline_node_admits_nothing() {
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        f.node.shutdown();
        drop(link);
        tokio::time::timeout(wait(), async {
            while f.node.status().connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let offline = unavailable(f.admit(r#"{"target":"remote"}"#).await);
        assert_eq!(offline.reason, PlacementUnavailableReason::NoCapacity);
        assert!(offline.availability.available_platforms.is_empty());
    }

    /// Placing an agent records it on the lease and writes its base ref; its
    /// first runtime sends the launch with that commit and the repository's
    /// cache key; `Started` then marks its checkout isolated.
    #[tokio::test]
    async fn a_placed_agent_launches_on_its_first_runtime() {
        let mut fake = FakeNode::start().await;
        let (f, mut link) = Fixture::start(&mut fake).await;
        let (agent, view) = f.placed().await;
        let record = f
            .services
            .store()
            .get_remote_agent(&agent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.lease_id, view.lease_id);
        let base_ref = AgentRemote::new(workspace().as_str(), agent.as_str())
            .unwrap()
            .ref_name(intent_git::hub::AgentRef::Base);
        assert_eq!(git(f.repo.path(), &["rev-parse", &base_ref]), f.base);

        let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
        let launch = link
            .expect(|frame| match frame {
                LinkFrame::Launch(command) => Some(command),
                _ => None,
            })
            .await;
        assert_eq!(launch.agent_id, agent);
        assert_eq!(launch.provider, "mock");
        // No profile selected: nothing is sent and the node uses its own
        // provider login.
        assert_eq!(launch.profile_env, None);
        assert_eq!(launch.base_commit, f.base);
        assert_eq!(launch.repo_key, node_git::repo_key(f.repo.path()).unwrap());
        // The node gates the agent's workspace API with its session's
        // captured features (review of intentd#2786).
        let session = f.services.store().get_agent_session(&agent).await.unwrap();
        assert_eq!(
            launch.agent_features,
            serde_json::to_value(f.services.session_agent_features(&session)).unwrap()
        );
        assert!(!launch.is_sub_agent);
        assert!(runtime.is_alive());

        link.journal(JournalRecord {
            seq: NodeCounter(1),
            agent_id: agent.clone(),
            workspace_id: workspace(),
            turn_id: None,
            recorded_at: intent_core::now_iso(),
            body: JournalBody::Started {
                base_commit: f.base.clone(),
            },
        })
        .await;
        link.acked(1).await;
        let placed = f.view(&agent).await;
        assert_eq!(placed.effective_isolation, EffectiveIsolation::Isolated);
        assert_eq!(placed.placement_error, None);
        f.node.shutdown();
    }

    /// An agent of a saved provider profile starts on the node with the
    /// profile's environment: the base provider id, and the overlay with
    /// account directories as typed (the node expands `~`).
    #[tokio::test]
    async fn a_profile_agent_launches_with_the_profiles_environment() {
        use intent_core::WorkspaceApi as _;
        let mut fake = FakeNode::start().await;
        let (f, mut link) = Fixture::start(&mut fake).await;
        let created = intent_core::caller::with_caller(
            intent_core::caller::Caller::Daemon,
            f.services.provider_profiles_create(json!({
                "baseProviderId": "codex",
                "name": "Work",
                "env": [
                    { "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-work" },
                    { "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": "canary-head-secret" },
                    { "key": "OPENAI_BASE_URL", "kind": "unset" },
                ],
            })),
        )
        .await
        .expect("create profile");
        let reference = created["profile"]["providerRef"].as_str().unwrap();
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let agent = f.agent_with(&view, reference).await;
        f.port()
            .launch(
                &f.services,
                RemoteLaunchRequest {
                    workspace_id: workspace(),
                    agent_id: agent.clone(),
                    view,
                },
            )
            .await
            .unwrap();
        let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
        let launch = link
            .expect(|frame| match frame {
                LinkFrame::Launch(command) => Some(command),
                _ => None,
            })
            .await;
        assert_eq!(launch.provider, "codex");
        let wire = launch.profile_env.expect("the profile's overlay").0;
        assert_eq!(wire["providerRef"], reference);
        assert_eq!(
            wire["env"],
            json!([
                { "key": "CODEX_HOME", "value": "~/.codex-work" },
                { "key": "CODEX_ACCESS_TOKEN", "value": "canary-head-secret" },
                { "key": "OPENAI_BASE_URL", "value": null },
            ])
        );
        assert!(runtime.is_alive());
        f.node.shutdown();
    }

    /// A selected profile that no longer resolves fails the start: nothing
    /// is sent, so the node never runs it on its own login.
    #[tokio::test]
    async fn an_unresolved_profile_fails_the_start_without_launching() {
        let mut fake = FakeNode::start().await;
        let (f, mut link) = Fixture::start(&mut fake).await;
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let agent = f.agent_with(&view, "codex@gone").await;
        f.port()
            .launch(
                &f.services,
                RemoteLaunchRequest {
                    workspace_id: workspace(),
                    agent_id: agent.clone(),
                    view,
                },
            )
            .await
            .unwrap();
        let Err(error) = f.port().runtime(&f.services, &agent).await else {
            panic!("an unresolved profile must fail the start");
        };
        assert!(
            matches!(&error, Error::ProviderProfile(failure)
                if failure.data["code"] == "provider-profile-replacement-required"),
            "{error:?}"
        );
        assert!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(200)),
                link.expect(|frame| match frame {
                    LinkFrame::Launch(command) => Some(command),
                    _ => None,
                })
            )
            .await
            .is_err(),
            "no launch is sent for an unresolved profile"
        );
        f.node.shutdown();
    }

    #[tokio::test]
    async fn a_start_failure_is_the_agents_placement_error() {
        let mut fake = FakeNode::start().await;
        let (f, mut link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
        link.journal(JournalRecord {
            seq: NodeCounter(1),
            agent_id: agent.clone(),
            workspace_id: workspace(),
            turn_id: None,
            recorded_at: intent_core::now_iso(),
            body: JournalBody::Exited {
                exit_code: None,
                error: Some("node-provider-unavailable: provider \"mock\" is not configured on this node (configured: codex)".into()),
                failure: None,
            },
        })
        .await;
        link.acked(1).await;
        let view = f.view(&agent).await;
        assert_eq!(view.effective_isolation, EffectiveIsolation::Pending);
        let error = view.placement_error.unwrap();
        assert_eq!(error.code, "node-provider-unavailable");
        assert!(
            error.detail.contains("not configured on this node"),
            "{}",
            error.detail
        );
        assert!(!runtime.is_alive());
        f.node.shutdown();
    }

    #[tokio::test]
    async fn an_agent_never_placed_has_no_remote_runtime() {
        let mut fake = FakeNode::start().await;
        let (f, _link) = Fixture::start(&mut fake).await;
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let unplaced = f.agent(&view).await;
        let result = f.port().runtime(&f.services, &unplaced).await;
        assert!(
            matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("node-agent-unplaced")),
            "{:?}",
            result.err()
        );
        f.node.shutdown();
    }

    /// At bind, an agent the node runs that head has no record of is
    /// stopped; one head placed keeps running, and its output reaches the
    /// runtime head later takes for it, with no second launch.
    #[tokio::test]
    async fn binding_stops_unclaimed_agents_and_keeps_claimed_ones() {
        let mut first = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut first).await;
        let (claimed, _) = f.placed().await;
        let config = f.node.status();
        f.node.shutdown();
        drop(link);
        drop(first);

        // A head restart: a new connector on the same store.
        let stray = AgentId::new();
        let mut fake = FakeNode::start_with(
            "127.0.0.1:0".parse().unwrap(),
            welcome_running(vec![claimed.clone(), stray.clone()]),
        )
        .await;
        let node =
            RemoteNode::start_with(f.services.store().clone(), fake.config(), tuning(), None)
                .await
                .unwrap();
        assert_eq!(node.status().lease_id, config.lease_id);
        crate::install_node(&f.services, node.clone());
        let mut link = fake.accept().await;
        let stopped = link
            .expect(|frame| match frame {
                LinkFrame::Stop { agent_id } => Some(agent_id),
                _ => None,
            })
            .await;
        assert_eq!(stopped, stray);
        link.replay_complete(0).await;
        link.journal(note(1, &claimed, "still working")).await;
        link.acked(1).await;
        let runtime = f.port().runtime(&f.services, &claimed).await.unwrap();
        let notes = runtime.notifications();
        let received = tokio::time::timeout(wait(), async { notes.lock().await.recv().await })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            received.params["update"]["content"]["text"],
            "still working"
        );
        node.shutdown();
    }

    /// Head serves an agent's refs only on the current binding and only for
    /// an agent recorded on that binding's lease.
    #[tokio::test]
    async fn git_sessions_are_confined_to_the_leases_agents() {
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let unplaced = f.agent(&view).await;
        let access = crate::git::HeadGitAccess::new(f.node.shared());
        let binding = link.hello.binding.clone();
        let remote =
            |agent: &AgentId| AgentRemote::new(workspace().as_str(), agent.as_str()).unwrap();

        let path = access.repository(&binding, &remote(&agent)).await.unwrap();
        assert_eq!(path, f.repo.path());
        assert!(matches!(
            access.repository(&binding, &remote(&unplaced)).await,
            Err(Error::Forbidden(_))
        ));
        let other_workspace = AgentRemote::new("ws-other", agent.as_str()).unwrap();
        assert!(matches!(
            access.repository(&binding, &other_workspace).await,
            Err(Error::Forbidden(_))
        ));
        let mut stale = binding.clone();
        stale.generation = NodeCounter(binding.generation.0 - 1);
        assert!(access.repository(&stale, &remote(&agent)).await.is_err());
        let _ = GitService::UploadPack;
        f.node.shutdown();
    }

    /// Forward one `workspace_api` frame for `agent` over the node's link.
    async fn forward(link: &NodeEnd, agent: &AgentId, method: &str, args: Value) -> LinkOutcome {
        link.link
            .call(
                agent.clone(),
                intent_core::remote_launch::workspace_api::CALL_METHOD,
                json!({ "method": method, "args": args }),
                wait(),
            )
            .await
            .unwrap()
    }

    fn refused(outcome: LinkOutcome) -> intent_core::node_link::LinkRpcError {
        match outcome {
            LinkOutcome::Err(error) => error,
            LinkOutcome::Ok(value) => panic!("expected a refusal, got {value}"),
        }
    }

    /// A placed agent's forwarded frame runs on head as that agent, in its
    /// workspace.
    #[tokio::test]
    async fn a_lease_agents_forwarded_call_runs_on_head_as_the_agent() {
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        let created = match forward(
            &link,
            &agent,
            "note.create",
            json!({ "title": "From the node", "content": "remote body" }),
        )
        .await
        {
            LinkOutcome::Ok(value) => value,
            LinkOutcome::Err(error) => panic!("forwarded call failed: {error:?}"),
        };
        let notes = f.services.store().list_notes(&workspace()).await.unwrap();
        let note = notes
            .iter()
            .find(|note| note.title == "From the node")
            .unwrap_or_else(|| panic!("note created on head: {created}"));
        assert_eq!(created["id"], json!(note.id.as_str()), "{created}");
        f.node.shutdown();
    }

    /// Head refuses what the node must not get: an agent not on the lease,
    /// a frame head does not forward (even if the node sends it), a
    /// malformed call and an unknown link method.
    #[tokio::test]
    async fn forwarded_calls_are_confined_to_the_leases_agents_and_the_allowlist() {
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let unplaced = f.agent(&view).await;

        let other = refused(forward(&link, &unplaced, "note.list", json!({})).await);
        assert!(other.message.starts_with("node-agent-denied"), "{other:?}");
        let unknown = refused(forward(&link, &AgentId::new(), "note.list", json!({})).await);
        assert!(
            unknown.message.starts_with("node-agent-denied"),
            "{unknown:?}"
        );

        for method in ["script.list", "file.read", "hook.list", "workspace.info"] {
            let error = refused(forward(&link, &agent, method, json!({})).await);
            assert!(
                error.message.starts_with("remote-agent-unsupported"),
                "{method}: {error:?}"
            );
        }
        let malformed = refused(
            link.link
                .call(
                    agent.clone(),
                    intent_core::remote_launch::workspace_api::CALL_METHOD,
                    json!({ "args": {} }),
                    wait(),
                )
                .await
                .unwrap(),
        );
        assert_eq!(malformed.code, -32602, "{malformed:?}");
        let method = refused(
            link.link
                .call(agent.clone(), "note.list", json!({}), wait())
                .await
                .unwrap(),
        );
        assert_eq!(method.code, -32601, "{method:?}");
        assert!(f
            .services
            .store()
            .list_notes(&workspace())
            .await
            .unwrap()
            .is_empty());
        f.node.shutdown();
    }

    /// A request that arrived on a replaced binding is refused.
    #[tokio::test]
    async fn a_forwarded_call_on_a_replaced_binding_is_refused() {
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        let calls = crate::rpc::HeadCalls::new(f.services.clone());
        let shared = f.node.shared();
        let current = link.hello.binding.clone();
        let mut stale = current.clone();
        stale.generation = NodeCounter(current.generation.0 - 1);
        let request = intent_core::node_link::LinkRequest {
            request_id: uuid::Uuid::new_v4(),
            agent_id: agent.clone(),
            method: intent_core::remote_launch::workspace_api::CALL_METHOD.into(),
            params: json!({ "method": "note.list", "args": {} }),
            timeout_ms: 5_000,
        };
        for current in [Some(&current), None] {
            let error = refused(calls.outcome(&shared, current, &stale, &request).await);
            assert!(error.message.starts_with("node-link-stale"), "{error:?}");
        }
        let fresh = calls
            .outcome(&shared, Some(&current), &current, &request)
            .await;
        assert!(matches!(fresh, LinkOutcome::Ok(_)), "{fresh:?}");
        f.node.shutdown();
    }

    /// Review of intentd#2786: the node reads the workspace's auto-commit
    /// policy before an agent-initiated commit; head answers it only for a
    /// lease agent, from the workspace's own setting.
    #[tokio::test]
    async fn the_commit_policy_is_the_workspaces_and_only_for_lease_agents() {
        use intent_core::remote_launch::workspace_api::COMMIT_POLICY_METHOD;
        let mut fake = FakeNode::start().await;
        let (f, link) = Fixture::start(&mut fake).await;
        let (agent, _) = f.placed().await;
        for enabled in [false, true] {
            f.services
                .store()
                .set_workspace_auto_commit(&workspace(), enabled)
                .await
                .unwrap();
            match forward(&link, &agent, COMMIT_POLICY_METHOD, Value::Null).await {
                LinkOutcome::Ok(value) => assert_eq!(value, json!({ "autoCommit": enabled })),
                LinkOutcome::Err(error) => panic!("policy read failed: {error:?}"),
            }
        }
        let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
        let unplaced = f.agent(&view).await;
        let denied = refused(forward(&link, &unplaced, COMMIT_POLICY_METHOD, Value::Null).await);
        assert!(
            denied.message.starts_with("node-agent-denied"),
            "{denied:?}"
        );
        f.node.shutdown();
    }
}
