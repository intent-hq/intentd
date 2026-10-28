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
    }
}

fn token() -> LeaseToken {
    LeaseToken::new("t".repeat(40)).unwrap()
}

fn welcome() -> WelcomeFn {
    Arc::new(|_| LinkWelcome {
        protocol: 0,
        intentd_version: "node-test".into(),
        node_id: String::new(),
        platform: AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        },
        max_agents: 2,
        running_agents: vec![],
    })
}

/// A node that hands every admitted link to the test.
struct FakeNode {
    listener: NodeListener,
    certificate: TlsCertificate,
    dir: tempfile::TempDir,
}

impl FakeNode {
    async fn start() -> Self {
        Self::start_at("127.0.0.1:0".parse().unwrap()).await
    }

    async fn start_at(address: SocketAddr) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let certificate =
            generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let listener = Self::listen(&dir, &certificate, address).await;
        Self {
            listener,
            certificate,
            dir,
        }
    }

    async fn listen(
        dir: &tempfile::TempDir,
        certificate: &TlsCertificate,
        address: SocketAddr,
    ) -> NodeListener {
        NodeListener::bind(
            address,
            NodeListenerConfig {
                node_id: NODE.into(),
                token: token(),
                binding_file: BindingFile::new(dir.path().join("binding.json")),
                certificate: certificate.clone(),
            },
            welcome(),
        )
        .await
        .unwrap()
    }

    fn config(&self) -> HeadNodeConfig {
        config_for(self.listener.local_addr(), &self.certificate)
    }

    async fn accept(&mut self) -> NodeEnd {
        match tokio::time::timeout(wait(), self.listener.accept()).await {
            Ok(Some(NodeConnection::Link {
                link,
                events,
                hello,
            })) => NodeEnd {
                link,
                events,
                hello,
            },
            other => panic!("expected a link, got {other:?}"),
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
        RemoteNode::start_with(self.store.clone(), config, tuning())
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
        dir,
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
    let error = RemoteNode::start_with(head.store.clone(), fake.config(), tuning())
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

mod services {
    use super::*;
    use crate::RemoteServicesExt as _;
    use intent_services::feature_api::Services;

    #[cfg(unix)]
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
        assert!(services.start_remote_node(data.path()).await.unwrap());
        assert!(
            !services.start_remote_node(data.path()).await.unwrap(),
            "one connector at a time"
        );
        let link = fake.accept().await;
        assert_eq!(link.hello.binding.node_id, NODE);
        let node = services.remote_node().expect("connector registered");
        assert_eq!(node.status().node_name, NODE);
        node.shutdown();
        drop(fake.dir);
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
    let node = RemoteNode::start_with(head.store.clone(), config_for(address, &certificate), slow)
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
        dir,
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
