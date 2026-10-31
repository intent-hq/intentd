use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::watch;

use intent_core::node_link::{MachineCapabilities, ProtocolRange, NODE_LINK_PROTOCOLS};
use intent_core::nodes::{NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::AgentId;

use super::dial::{decode_rejection, dial_link_with, read_welcome};
use super::link::Tuning;
use super::*;
use crate::protocol::test_capture::Capture;
use crate::{generate_certificate, TlsCertificate};

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}
const NODE: &str = "node-1";
const HEAD: &str = "head-1";

fn certificate() -> TlsCertificate {
    generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap()
}

fn token(text: &str) -> LeaseToken {
    LeaseToken::new(format!("{text}{}", "x".repeat(32))).unwrap()
}

fn binding(generation: u64) -> LeaseBinding {
    LeaseBinding {
        head_id: HEAD.into(),
        node_id: NODE.into(),
        lease_id: "lease-1".into(),
        generation: NodeCounter(generation),
    }
}

fn hello(binding: LeaseBinding) -> LinkHello {
    LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "test".into(),
        binding,
        acked_seq: NodeCounter(0),
    }
}

fn welcome() -> WelcomeFn {
    Arc::new(|_| LinkWelcome {
        protocol: 0,
        node_id: String::new(),
        running_agents: vec![],
        capabilities: MachineCapabilities::new(
            AgentPlatform {
                os: NodeOs::Linux,
                arch: NodeArch::X86_64,
            },
            "test",
            2,
            "2026-10-30T00:00:00Z",
        ),
        hosts: Vec::new(),
        tc_address: None,
    })
}

struct Node {
    listener: NodeListener,
    certificate: TlsCertificate,
    token: LeaseToken,
    dir: tempfile::TempDir,
}

impl Node {
    async fn start(token: LeaseToken) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let certificate = certificate();
        let listener = bind_node(&dir, &certificate, &token, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        Self {
            listener,
            certificate,
            token,
            dir,
        }
    }

    fn endpoint(&self) -> NodeEndpoint {
        NodeEndpoint {
            url: format!("wss://{}", self.listener.local_addr()),
            pin: CertificatePin::parse(&self.certificate.fingerprint256).unwrap(),
            token: self.token.clone(),
            tailcat: None,
        }
    }

    async fn accept_link(&mut self) -> (NodeLink, LinkEvents, LinkHello) {
        match tokio::time::timeout(wait(), self.listener.accept()).await {
            Ok(Some(NodeConnection::Link {
                link,
                events,
                hello,
            })) => (link, events, hello),
            other => panic!("expected a link connection, got {other:?}"),
        }
    }

    async fn accept_git(&mut self) -> GitConnection {
        match tokio::time::timeout(wait(), self.listener.accept()).await {
            Ok(Some(NodeConnection::Git(git))) => git,
            other => panic!("expected a git connection, got {other:?}"),
        }
    }

    async fn nothing_admitted(&mut self) {
        assert!(
            tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(300)),
                self.listener.accept(),
            )
            .await
            .is_err(),
            "no connection may be admitted"
        );
    }
}

async fn bind_node(
    dir: &tempfile::TempDir,
    certificate: &TlsCertificate,
    token: &LeaseToken,
    address: SocketAddr,
) -> Result<NodeListener, intent_core::Error> {
    NodeListener::bind(
        address,
        NodeListenerConfig {
            node_id: NODE.into(),
            token: token.clone(),
            binding_file: BindingFile::new(dir.path().join("binding.json")),
            certificate: certificate.clone(),
        },
        welcome(),
    )
    .await
}

fn code(error: &intent_core::Error) -> &str {
    error_code(error).unwrap_or("not a link error")
}

async fn dial(node: &Node, generation: u64) -> DialedLink {
    dial_link(&node.endpoint(), hello(binding(generation)))
        .await
        .unwrap()
}

#[tokio::test]
async fn binds_and_exchanges_frames() {
    let mut node = Node::start(token("t")).await;
    let dialed = dial(&node, 1).await;
    assert_eq!(dialed.welcome.node_id, NODE);
    assert_eq!(dialed.welcome.protocol, NODE_LINK_PROTOCOLS.max);
    assert_eq!(dialed.welcome.capabilities.capacity.agent_cap, 2);
    let (node_link, mut node_events, node_hello) = node.accept_link().await;
    assert_eq!(node_hello.binding, binding(1));
    assert_eq!(node_link.binding(), &binding(1));
    assert_eq!(node.listener.binding(), Some(binding(1)));

    let agent_id = AgentId::new();
    dialed
        .link
        .send(&LinkFrame::Cancel {
            agent_id: agent_id.clone(),
        })
        .await
        .unwrap();
    match tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    {
        Some(LinkEvent::Frame(LinkFrame::Cancel { agent_id: got })) => assert_eq!(got, agent_id),
        other => panic!("unexpected {other:?}"),
    }
    node_link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(3),
        })
        .await
        .unwrap();
    let mut head_events = dialed.events;
    match tokio::time::timeout(wait(), head_events.recv())
        .await
        .unwrap()
    {
        Some(LinkEvent::Frame(LinkFrame::Ack { seq })) => assert_eq!(seq, NodeCounter(3)),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn rpc_frames_cannot_be_sent_directly() {
    let node = Node::start(token("t")).await;
    let dialed = dial(&node, 1).await;
    let error = dialed
        .link
        .send(&LinkFrame::CancelRequest {
            request_id: uuid::Uuid::new_v4(),
        })
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::PROTOCOL);
}

#[tokio::test]
async fn wrong_pin_is_rejected_before_the_token_is_sent() {
    let mut node = Node::start(token("t")).await;
    let mut endpoint = node.endpoint();
    endpoint.pin = CertificatePin::parse(&certificate().fingerprint256).unwrap();
    let error = dial_link(&endpoint, hello(binding(1))).await.unwrap_err();
    assert_eq!(code(&error), codes::CERTIFICATE_MISMATCH);
    assert!(!is_retryable(&error));
    node.nothing_admitted().await;
    assert_eq!(node.listener.binding(), None);
}

#[tokio::test]
async fn wrong_token_is_rejected() {
    let mut node = Node::start(token("node-token")).await;
    let mut endpoint = node.endpoint();
    endpoint.token = token("other-token");
    let error = dial_link(&endpoint, hello(binding(1))).await.unwrap_err();
    assert_eq!(code(&error), codes::UNAUTHORIZED);
    assert!(!is_retryable(&error));
    let error = dial_git(&endpoint, &binding(1)).await.unwrap_err();
    assert_eq!(code(&error), codes::UNAUTHORIZED);
    node.nothing_admitted().await;
    assert_eq!(node.listener.binding(), None);
}

#[tokio::test]
async fn lower_or_equal_generation_is_rejected() {
    let mut node = Node::start(token("t")).await;
    let _first = dial(&node, 5).await;
    let _ = node.accept_link().await;
    for generation in [5, 4, 1] {
        let error = dial_link(&node.endpoint(), hello(binding(generation)))
            .await
            .unwrap_err();
        assert_eq!(code(&error), codes::STALE, "generation {generation}");
        assert!(
            error.to_string().contains("generation 5"),
            "the error names the node's generation: {error}"
        );
        assert!(is_retryable(&error));
    }
    node.nothing_admitted().await;
    assert_eq!(node.listener.binding(), Some(binding(5)));
}

#[tokio::test]
async fn generation_survives_a_node_restart() {
    let node = Node::start(token("t")).await;
    let _link = dial(&node, 7).await;
    let address = node.listener.local_addr();
    let Node {
        listener,
        certificate,
        token,
        dir,
    } = node;
    drop(listener);
    let persisted = BindingFile::new(dir.path().join("binding.json"))
        .load()
        .unwrap();
    assert_eq!(persisted, Some(binding(7)));

    let listener = bind_node(&dir, &certificate, &token, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    assert_ne!(listener.local_addr(), address);
    let mut node = Node {
        listener,
        certificate,
        token,
        dir,
    };
    assert_eq!(node.listener.binding(), Some(binding(7)));
    let error = dial_link(&node.endpoint(), hello(binding(7)))
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::STALE);
    let _newer = dial(&node, 8).await;
    let _ = node.accept_link().await;
    assert_eq!(node.listener.binding(), Some(binding(8)));
}

#[tokio::test]
async fn another_head_or_node_is_rejected() {
    let mut node = Node::start(token("t")).await;
    let _first = dial(&node, 1).await;
    let _ = node.accept_link().await;

    let mut other_head = binding(9);
    other_head.head_id = "head-2".into();
    let error = dial_link(&node.endpoint(), hello(other_head))
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::OTHER_HEAD);
    assert!(!is_retryable(&error));

    let mut other_node = binding(9);
    other_node.node_id = "node-2".into();
    let error = dial_link(&node.endpoint(), hello(other_node))
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::WRONG_NODE);
    node.nothing_admitted().await;
    assert_eq!(node.listener.binding(), Some(binding(1)));
}

#[tokio::test]
async fn protocol_mismatch_is_rejected() {
    let mut node = Node::start(token("t")).await;
    let mut offer = hello(binding(1));
    let newer = NODE_LINK_PROTOCOLS.max + 1;
    offer.protocol = ProtocolRange {
        min: newer,
        max: newer,
    };
    let error = dial_link(&node.endpoint(), offer).await.unwrap_err();
    assert_eq!(code(&error), codes::VERSION_MISMATCH);
    assert!(error
        .to_string()
        .contains(&format!("node speaks {}", NODE_LINK_PROTOCOLS.max)));
    node.nothing_admitted().await;
    assert_eq!(node.listener.binding(), None);
}

#[tokio::test]
async fn the_node_picks_the_highest_common_protocol_version() {
    let node = Node::start(token("t")).await;
    let mut offer = hello(binding(1));
    offer.protocol = ProtocolRange {
        min: NODE_LINK_PROTOCOLS.min,
        max: NODE_LINK_PROTOCOLS.max + 3,
    };
    let dialed = dial_link(&node.endpoint(), offer).await.unwrap();
    assert_eq!(dialed.welcome.protocol, NODE_LINK_PROTOCOLS.max);
}

#[tokio::test]
async fn rebind_closes_the_older_link_and_its_git_connections() {
    let mut node = Node::start(token("t")).await;
    let old = dial(&node, 1).await;
    let (old_node_link, _old_events, _) = node.accept_link().await;
    let mut old_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let old_node_git = node.accept_git().await;
    assert_eq!(old_node_git.binding(), &binding(1));

    let new = dial(&node, 2).await;
    let (new_node_link, _new_events, _) = node.accept_link().await;

    tokio::time::timeout(wait(), old.link.closed())
        .await
        .unwrap();
    tokio::time::timeout(wait(), old_node_link.closed())
        .await
        .unwrap();
    tokio::time::timeout(wait(), old_node_git.closed())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), old_git.recv()).await.unwrap(),
        None
    );
    assert!(!new.link.is_closed());
    assert!(!new_node_link.is_closed());

    let error = dial_git(&node.endpoint(), &binding(1)).await.unwrap_err();
    assert_eq!(code(&error), codes::STALE);
    let error = old
        .link
        .call(AgentId::new(), "agent.status", json!({}), wait())
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
}

#[tokio::test]
async fn git_connection_carries_bytes_both_ways() {
    let mut node = Node::start(token("t")).await;
    let _link = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let mut head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let mut node_git = node.accept_git().await;
    head_git.send(b"want abc".to_vec()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), node_git.recv()).await.unwrap(),
        Some(b"want abc".to_vec())
    );
    node_git.send(b"PACK".to_vec()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), head_git.recv()).await.unwrap(),
        Some(b"PACK".to_vec())
    );
    node_git.close();
    assert_eq!(
        tokio::time::timeout(wait(), head_git.recv()).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn git_connection_needs_a_current_binding() {
    let mut node = Node::start(token("t")).await;
    let error = dial_git(&node.endpoint(), &binding(1)).await.unwrap_err();
    assert_eq!(code(&error), codes::STALE);
    let _link = dial(&node, 3).await;
    let _ = node.accept_link().await;
    let mut wrong_lease = binding(3);
    wrong_lease.lease_id = "lease-2".into();
    let error = dial_git(&node.endpoint(), &wrong_lease).await.unwrap_err();
    assert_eq!(code(&error), codes::STALE);
    node.nothing_admitted().await;
}

/// Spawn a node-side handler answering every request with its method name.
fn echo_requests(mut events: LinkEvents) {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let LinkEvent::Request(request) = event {
                let method = request.request().method.clone();
                let _ = request
                    .respond(LinkOutcome::Ok(json!({ "method": method })))
                    .await;
            }
        }
    });
}

#[tokio::test]
async fn rpc_round_trips_in_both_directions() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (node_link, node_events, _) = node.accept_link().await;
    echo_requests(node_events);
    echo_requests(head.events);
    let outcome = head
        .link
        .call(AgentId::new(), "agent.status", json!({}), wait())
        .await
        .unwrap();
    assert_eq!(
        outcome,
        LinkOutcome::Ok(json!({ "method": "agent.status" }))
    );
    let outcome = node_link
        .call(AgentId::new(), "note.read", json!({ "id": "spec" }), wait())
        .await
        .unwrap();
    assert_eq!(outcome, LinkOutcome::Ok(json!({ "method": "note.read" })));
}

#[tokio::test]
async fn incoming_request_carries_the_link_binding() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 4).await;
    let (_node_link, mut node_events, _) = node.accept_link().await;
    let agent_id = AgentId::new();
    let call = {
        let link = head.link.clone();
        let agent_id = agent_id.clone();
        tokio::spawn(async move { link.call(agent_id, "x", json!(null), wait()).await })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    assert_eq!(request.binding(), &binding(4));
    assert_eq!(request.request().agent_id, agent_id);
    assert!(request.binding().ensure_current(&binding(4)).is_ok());
    assert!(request.binding().ensure_current(&binding(5)).is_err());
    request.respond(LinkOutcome::Ok(json!(1))).await.unwrap();
    assert_eq!(call.await.unwrap().unwrap(), LinkOutcome::Ok(json!(1)));
}

#[tokio::test]
async fn timed_out_call_cancels_the_peer_request() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (_node_link, mut node_events, _) = node.accept_link().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move {
            link.call(
                AgentId::new(),
                "slow",
                json!({}),
                Duration::from_millis(200),
            )
            .await
        })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    assert_eq!(request.request().timeout_ms, 200);
    assert!(!request.is_cancelled());
    tokio::time::timeout(wait(), request.cancelled())
        .await
        .unwrap();
    assert!(request.is_cancelled());
    let error = call.await.unwrap().unwrap_err();
    assert_eq!(code(&error), codes::TIMEOUT);
}

#[tokio::test]
async fn dropping_a_call_cancels_the_peer_request() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (_node_link, mut node_events, _) = node.accept_link().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move { link.call(AgentId::new(), "slow", json!({}), wait()).await })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    call.abort();
    tokio::time::timeout(wait(), request.cancelled())
        .await
        .unwrap();
}

#[tokio::test]
async fn dropped_request_answers_with_an_error() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (_node_link, mut node_events, _) = node.accept_link().await;
    tokio::spawn(async move {
        while let Some(event) = node_events.recv().await {
            drop(event);
        }
    });
    let outcome = head
        .link
        .call(AgentId::new(), "x", json!({}), wait())
        .await
        .unwrap();
    let LinkOutcome::Err(error) = outcome else {
        panic!("expected an error outcome");
    };
    assert_eq!(error.message, "node-link-request-dropped");
}

#[tokio::test]
async fn closing_the_link_fails_pending_calls_and_cancels_requests() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (node_link, mut node_events, _) = node.accept_link().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move { link.call(AgentId::new(), "slow", json!({}), wait()).await })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    node_link.close();
    tokio::time::timeout(wait(), request.cancelled())
        .await
        .unwrap();
    let error = tokio::time::timeout(wait(), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
    tokio::time::timeout(wait(), head.link.closed())
        .await
        .unwrap();
    let mut head_events = head.events;
    assert!(tokio::time::timeout(wait(), head_events.recv())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn heartbeats_keep_an_idle_link_open() {
    let mut node = Node::start(token("t")).await;
    let tuning = Tuning {
        heartbeat: Duration::from_millis(50),
        dead_after: Duration::from_millis(300),
        ..Tuning::default()
    };
    let head = dial_link_with(&node.endpoint(), hello(binding(1)), tuning)
        .await
        .unwrap();
    let (node_link, _events, _) = node.accept_link().await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(!head.link.is_closed());
    assert!(!node_link.is_closed());
}

#[tokio::test]
async fn reconnects_with_backoff_once_the_node_is_up() {
    let dir = tempfile::tempdir().unwrap();
    let certificate = certificate();
    let token = token("t");
    let address = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap()
    };
    let endpoint = NodeEndpoint {
        url: format!("wss://{address}"),
        pin: CertificatePin::parse(&certificate.fingerprint256).unwrap(),
        token: token.clone(),
        tailcat: None,
    };
    let generations = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let next_hello = {
        let generations = generations.clone();
        move || {
            let generation = generations.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            async move { Ok(hello(binding(generation))) }
        }
    };
    let (_stop, shutdown) = watch::channel(false);
    let mut backoff = Backoff::new(Duration::from_millis(50), Duration::from_millis(200));
    let dial = tokio::spawn(async move {
        dial_with_backoff(&endpoint, next_hello, &mut backoff, shutdown).await
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!dial.is_finished());
    let mut listener = bind_node(&dir, &certificate, &token, address)
        .await
        .unwrap();
    let dialed = tokio::time::timeout(wait(), dial)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(generations.load(std::sync::atomic::Ordering::SeqCst) > 1);
    assert_eq!(listener.binding(), Some(dialed.link.binding().clone()));
    let Some(NodeConnection::Link { .. }) = tokio::time::timeout(wait(), listener.accept())
        .await
        .unwrap()
    else {
        panic!("expected the link");
    };
}

#[tokio::test]
async fn reconnect_moves_past_a_stale_generation() {
    let mut node = Node::start(token("t")).await;
    let _first = dial(&node, 5).await;
    let _ = node.accept_link().await;
    let generations = Arc::new(std::sync::atomic::AtomicU64::new(3));
    let next_hello = {
        let generations = generations.clone();
        move || {
            let generation = generations.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            async move { Ok(hello(binding(generation))) }
        }
    };
    let (_stop, shutdown) = watch::channel(false);
    let mut backoff = Backoff::new(Duration::from_millis(10), Duration::from_millis(20));
    let dialed = tokio::time::timeout(
        wait(),
        dial_with_backoff(&node.endpoint(), next_hello, &mut backoff, shutdown),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(dialed.link.binding().generation, NodeCounter(6));
}

#[tokio::test]
async fn reconnect_stops_on_a_permanent_error_or_shutdown() {
    let node = Node::start(token("t")).await;
    let mut endpoint = node.endpoint();
    endpoint.token = token("wrong");
    let (_stop, shutdown) = watch::channel(false);
    let mut backoff = Backoff::new(Duration::from_millis(10), Duration::from_millis(20));
    let error = tokio::time::timeout(
        wait(),
        dial_with_backoff(
            &endpoint,
            || async { Ok(hello(binding(1))) },
            &mut backoff,
            shutdown,
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(code(&error), codes::UNAUTHORIZED);

    let unreachable = NodeEndpoint {
        url: "wss://127.0.0.1:1".into(),
        ..node.endpoint()
    };
    let (stop, shutdown) = watch::channel(false);
    let mut backoff = Backoff::new(Duration::from_millis(10), Duration::from_millis(20));
    let dial = tokio::spawn(async move {
        dial_with_backoff(
            &unreachable,
            || async { Ok(hello(binding(1))) },
            &mut backoff,
            shutdown,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.send_replace(true);
    let error = tokio::time::timeout(wait(), dial)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
}

#[test]
fn backoff_doubles_to_the_cap_and_resets() {
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
    let delays: Vec<_> = (0..6).map(|_| backoff.next_delay()).collect();
    let within = |d: Duration, base: u64| {
        d >= Duration::from_millis(base * 8 / 10) && d <= Duration::from_millis(base * 12 / 10)
    };
    assert!(within(delays[0], 100), "{delays:?}");
    assert!(within(delays[1], 200), "{delays:?}");
    assert!(within(delays[2], 400), "{delays:?}");
    assert!(within(delays[3], 800), "{delays:?}");
    assert!(delays[4..].iter().all(|d| *d <= Duration::from_secs(1)));
    backoff.reset();
    assert!(within(backoff.next_delay(), 100));
}

#[test]
fn certificate_pin_parses_both_hex_forms() {
    let certificate = certificate();
    let colons = CertificatePin::parse(&certificate.fingerprint256).unwrap();
    let plain =
        CertificatePin::parse(&certificate.fingerprint256.replace(':', "").to_lowercase()).unwrap();
    assert_eq!(colons, plain);
    for bad in ["", "abcd", &"zz".repeat(32), &"ab".repeat(33)] {
        let error = CertificatePin::parse(bad).unwrap_err();
        assert_eq!(code(&error), codes::CONFIG);
    }
}

#[test]
fn lease_token_rejects_short_or_unprintable_values_and_hides_itself() {
    assert!(LeaseToken::new("short").is_err());
    assert!(LeaseToken::new(format!("{} with space", "x".repeat(32))).is_err());
    let generated = LeaseToken::generate().unwrap();
    assert_eq!(generated.expose().len(), 64);
    assert!(generated.matches(generated.expose()));
    assert!(!generated.matches("nope"));
    assert_eq!(format!("{generated:?}"), "LeaseToken([redacted])");
}

#[test]
fn rejection_reasons_never_echo_peer_text() {
    let injected = "node-link-stale: generation 9 secret=hunter2";
    let error = decode_rejection(injected);
    assert_eq!(code(&error), codes::STALE);
    assert!(!error.to_string().contains("hunter2"));
    let error = decode_rejection("totally made up hunter2");
    assert_eq!(code(&error), codes::PROTOCOL);
    assert!(!error.to_string().contains("hunter2"));
}

#[tokio::test]
async fn corrupt_binding_file_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("binding.json"), b"{not json").unwrap();
    let error = bind_node(
        &dir,
        &certificate(),
        &token("t"),
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(code(&error), codes::BINDING_STORE);
}

#[cfg(unix)]
#[tokio::test]
async fn binding_file_is_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let node = Node::start(token("t")).await;
    let _link = dial(&node, 1).await;
    let mode = std::fs::metadata(node.dir.path().join("binding.json"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[tokio::test]
async fn endpoint_must_be_wss_without_credentials() {
    let node = Node::start(token("t")).await;
    let address = node.listener.local_addr();
    for url in [
        format!("ws://{address}"),
        format!("https://{address}"),
        format!("wss://user:pass@{address}"),
        format!("wss://{address}/?x=1"),
    ] {
        let endpoint = NodeEndpoint {
            url,
            ..node.endpoint()
        };
        let error = dial_link(&endpoint, hello(binding(1))).await.unwrap_err();
        assert_eq!(code(&error), codes::CONFIG, "{}", endpoint.url);
    }
}

/// The token never appears in any link error or log line, on either side,
/// across every failure the link reports.
#[tokio::test]
async fn canary_secret_never_appears_in_errors_or_logs() {
    let capture = Capture::default();
    let _guard = tracing::dispatcher::set_default(&tracing::Dispatch::new(capture.clone()));
    let canary = "CANARY-7f3a9c";

    let mut node = Node::start(token(canary)).await;
    let good = node.endpoint();
    let mut texts = vec![format!("{good:?}"), format!("{:?}", good.token)];
    let mut record = |error: &intent_core::Error| {
        texts.push(format!("{error} {error:?}"));
    };

    let mut wrong_pin = good.clone();
    wrong_pin.pin = CertificatePin::parse(&certificate().fingerprint256).unwrap();
    record(&dial_link(&wrong_pin, hello(binding(1))).await.unwrap_err());

    let mut wrong_token = good.clone();
    wrong_token.token = token(&format!("{canary}-head"));
    record(
        &dial_link(&wrong_token, hello(binding(1)))
            .await
            .unwrap_err(),
    );
    record(&dial_git(&wrong_token, &binding(1)).await.unwrap_err());

    let unreachable = NodeEndpoint {
        url: "wss://127.0.0.1:1".into(),
        ..good.clone()
    };
    record(
        &dial_link(&unreachable, hello(binding(1)))
            .await
            .unwrap_err(),
    );

    let link = dial_link(&good, hello(binding(2))).await.unwrap();
    let (node_link, _events, _) = node.accept_link().await;
    record(&dial_link(&good, hello(binding(1))).await.unwrap_err());
    record(&dial_git(&good, &binding(1)).await.unwrap_err());
    let mut other_head = binding(3);
    other_head.head_id = "head-2".into();
    record(&dial_link(&good, hello(other_head)).await.unwrap_err());
    node_link.close();
    record(
        &link
            .link
            .call(AgentId::new(), "x", json!({}), wait())
            .await
            .unwrap_err(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let logs = capture.lines();
    assert!(!logs.is_empty(), "the failures above are logged");
    for line in logs.iter().map(|(_, line)| line).chain(texts.iter()) {
        assert!(!line.contains(canary), "secret leaked: {line}");
    }
}

#[test]
fn wire_trace_is_recognized_for_log_filtering() {
    use tracing::Level;
    for target in [
        "tungstenite",
        "tungstenite::handshake::client",
        "tokio_tungstenite::handshake",
    ] {
        assert!(is_wire_trace(target, &Level::TRACE), "{target}");
        assert!(is_wire_trace(target, &Level::DEBUG), "{target}");
        assert!(!is_wire_trace(target, &Level::INFO), "{target}");
    }
    for target in ["tungstenite_extra", "intent_transport::node_link", "rustls"] {
        assert!(!is_wire_trace(target, &Level::TRACE), "{target}");
    }
}

/// Every `log` record (the bridge intentd installs forwards these into its
/// tracing outputs), with its target and level.
struct WireLog(std::sync::Mutex<Vec<(String, log::Level, String)>>);

impl log::Log for WireLog {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push((
            record.target().to_owned(),
            record.level(),
            record.args().to_string(),
        ));
    }
    fn flush(&self) {}
}

static WIRE_LOG: WireLog = WireLog(std::sync::Mutex::new(Vec::new()));

fn tracing_level(level: log::Level) -> tracing::Level {
    match level {
        log::Level::Error => tracing::Level::ERROR,
        log::Level::Warn => tracing::Level::WARN,
        log::Level::Info => tracing::Level::INFO,
        log::Level::Debug => tracing::Level::DEBUG,
        log::Level::Trace => tracing::Level::TRACE,
    }
}

/// With every library logging at trace, a full authenticated connect, git
/// connection, RPC and a rejected connect leave the canary token only in
/// the WebSocket library's wire trace, which the daemon's output filter
/// ([`is_wire_trace`]) drops; nothing that passes the filter, and no
/// tracing event, contains it.
#[tokio::test]
async fn canary_secret_never_reaches_trace_level_log_outputs() {
    let _ = log::set_logger(&WIRE_LOG);
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let _guard = tracing::dispatcher::set_default(&tracing::Dispatch::new(capture.clone()));
    let canary = "CANARY-TRACE-51d0e2";

    let mut node = Node::start(token(canary)).await;
    let good = node.endpoint();
    let head = dial_link(&good, hello(binding(1))).await.unwrap();
    let (_node_link, node_events, _) = node.accept_link().await;
    echo_requests(node_events);
    head.link
        .call(AgentId::new(), "x", json!({}), wait())
        .await
        .unwrap();
    let _git = dial_git(&good, &binding(1)).await.unwrap();
    let _ = node.accept_git().await;
    let mut wrong = good.clone();
    wrong.token = token(&format!("{canary}-wrong"));
    dial_link(&wrong, hello(binding(2))).await.unwrap_err();
    dial_link(&good, hello(binding(1))).await.unwrap_err();
    tokio::time::sleep(Duration::from_millis(200)).await;
    log::set_max_level(log::LevelFilter::Off);

    let records = WIRE_LOG.0.lock().unwrap().clone();
    assert!(
        records
            .iter()
            .any(|(target, level, text)| text.contains(canary)
                && is_wire_trace(target, &tracing_level(*level))),
        "the wire trace carries the token, so the filter is load-bearing"
    );
    for (target, level, text) in &records {
        if !is_wire_trace(target, &tracing_level(*level)) {
            assert!(!text.contains(canary), "secret leaked via {target}: {text}");
        }
    }
    for (_, line) in capture.lines() {
        assert!(!line.contains(canary), "secret leaked: {line}");
    }
}

/// A peer cannot get text into head's errors through a malformed frame:
/// serde's decode errors quote the input.
#[tokio::test]
async fn malformed_welcome_does_not_echo_peer_text() {
    use futures_util::SinkExt as _;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    for payload in [
        r#"{"type":"CANARY-peer-text"}"#,
        r#"{"type":"welcome","protocol":5,"CANARY-peer-text":1}"#,
        "CANARY-peer-text",
    ] {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        server
            .send(Message::Binary(payload.as_bytes().to_vec().into()))
            .await
            .unwrap();
        let error = read_welcome(&mut client, &binding(1)).await.unwrap_err();
        assert_eq!(code(&error), codes::PROTOCOL, "{payload}");
        assert!(!format!("{error} {error:?}").contains("CANARY"), "{error}");
    }
}

/// A node-side git connection whose owner stopped draining it still closes
/// when a newer bind retires its binding.
#[tokio::test]
async fn rebind_closes_a_git_connection_with_a_full_receive_queue() {
    let mut node = Node::start(token("t")).await;
    let _old = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let node_git = node.accept_git().await;
    for i in 0..200_u32 {
        if head_git.send(i.to_be_bytes().to_vec()).await.is_err() {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _new = dial(&node, 2).await;
    let _ = node.accept_link().await;
    tokio::time::timeout(wait(), node_git.closed())
        .await
        .unwrap();
    tokio::time::timeout(wait(), head_git.closed())
        .await
        .unwrap();
}

/// Dropping the listener does not retire established git connections.
#[tokio::test]
async fn dropping_the_listener_keeps_git_connections_open() {
    let mut node = Node::start(token("t")).await;
    let _link = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let mut head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let node_git = node.accept_git().await;
    drop(node.listener);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!node_git.is_closed());
    node_git.send(b"still here".to_vec()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), head_git.recv()).await.unwrap(),
        Some(b"still here".to_vec())
    );
}

fn small_queues() -> Tuning {
    Tuning {
        outbound_queue: 2,
        event_queue: 2,
        ..Tuning::default()
    }
}

/// A node with tiny queues and the head link dialed with tiny queues.
async fn tuned_pair() -> (Node, DialedLink, NodeLink, LinkEvents) {
    let dir = tempfile::tempdir().unwrap();
    let certificate = certificate();
    let token = token("t");
    let listener = NodeListener::bind_with(
        "127.0.0.1:0".parse().unwrap(),
        NodeListenerConfig {
            node_id: NODE.into(),
            token: token.clone(),
            binding_file: BindingFile::new(dir.path().join("binding.json")),
            certificate: certificate.clone(),
        },
        welcome(),
        small_queues(),
    )
    .await
    .unwrap();
    let mut node = Node {
        listener,
        certificate,
        token,
        dir,
    };
    let head = dial_link_with(&node.endpoint(), hello(binding(1)), small_queues())
        .await
        .unwrap();
    let (node_link, node_events, _) = node.accept_link().await;
    (node, head, node_link, node_events)
}

/// Keep sending large frames on `link` until it closes. With the peer not
/// draining its events, the peer's reader stops, the socket buffers fill,
/// and then `link`'s outbound queue.
fn saturate(link: NodeLink) -> tokio::task::JoinHandle<()> {
    let frame = LinkFrame::Prompt {
        agent_id: AgentId::from_string("agent"),
        turn_id: "turn".into(),
        prompt: vec![json!("x".repeat(256 * 1024))],
    };
    let handle = tokio::spawn(async move { while link.send(&frame).await.is_ok() {} });
    handle
}

fn drain(mut events: LinkEvents) {
    tokio::spawn(async move { while events.recv().await.is_some() {} });
}

async fn saturated() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn call_deadline_covers_a_full_outbound_queue() {
    let (_node, head, _node_link, _node_events) = tuned_pair().await;
    let _flood = saturate(head.link.clone());
    saturated().await;
    let call = head
        .link
        .call(AgentId::new(), "x", json!({}), Duration::from_millis(300));
    let error = tokio::time::timeout(wait(), call)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(code(&error), codes::TIMEOUT);
}

#[tokio::test]
async fn closing_releases_a_call_blocked_on_a_full_outbound_queue() {
    let (_node, head, _node_link, _node_events) = tuned_pair().await;
    let _flood = saturate(head.link.clone());
    saturated().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move {
            link.call(AgentId::new(), "x", json!({}), Duration::from_secs(600))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!call.is_finished());
    head.link.close();
    let error = tokio::time::timeout(wait(), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
}

#[tokio::test]
async fn cancellation_is_not_lost_behind_a_full_outbound_queue() {
    let (_node, head, _node_link, mut node_events) = tuned_pair().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move {
            link.call(
                AgentId::new(),
                "slow",
                json!({}),
                Duration::from_millis(1500),
            )
            .await
        })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    let _flood = saturate(head.link.clone());
    let error = tokio::time::timeout(wait(), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(code(&error), codes::TIMEOUT);
    assert!(!head.link.is_closed());
    drain(node_events);
    tokio::time::timeout(wait(), request.cancelled())
        .await
        .unwrap();
}

#[tokio::test]
async fn dropped_request_reply_is_not_lost_behind_a_full_outbound_queue() {
    let (_node, head, node_link, mut node_events) = tuned_pair().await;
    let call = {
        let link = head.link.clone();
        tokio::spawn(async move { link.call(AgentId::new(), "x", json!({}), wait()).await })
    };
    let Some(LinkEvent::Request(request)) = tokio::time::timeout(wait(), node_events.recv())
        .await
        .unwrap()
    else {
        panic!("expected a request");
    };
    let _flood = saturate(node_link.clone());
    saturated().await;
    drop(request);
    assert!(!node_link.is_closed());
    drain(head.events);
    let outcome = tokio::time::timeout(wait(), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let LinkOutcome::Err(error) = outcome else {
        panic!("expected an error outcome");
    };
    assert_eq!(error.message, "node-link-request-dropped");
}

/// A request still queued behind a stalled writer when its call gives up
/// is never transmitted, so the peer never sees an abandoned request
/// without its cancellation, nor a cancellation ahead of its request.
#[tokio::test]
async fn abandoned_queued_request_is_never_sent_uncancelled() {
    use futures_util::StreamExt as _;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    let (near, far) = tokio::io::duplex(64);
    let near = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let mut peer = WebSocketStream::from_raw_socket(far, Role::Server, None).await;
    let (link, _events) = NodeLink::start(
        near,
        binding(1),
        Tuning {
            outbound_queue: 2,
            ..Tuning::default()
        },
    );
    link.send(&LinkFrame::Prompt {
        agent_id: AgentId::new(),
        turn_id: "turn".into(),
        prompt: vec![json!("x".repeat(4096))],
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let error = link
        .call(AgentId::new(), "slow", json!({}), Duration::from_millis(50))
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::TIMEOUT);

    let mut seen = Vec::new();
    while let Ok(Some(Ok(message))) = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(500)),
        peer.next(),
    )
    .await
    {
        if let Message::Binary(bytes) = message {
            seen.push(LinkFrame::decode(&bytes).unwrap());
        }
    }
    assert!(
        matches!(seen.first(), Some(LinkFrame::Prompt { .. })),
        "the stalled prompt is delivered: {seen:?}"
    );
    let requests: Vec<_> = seen
        .iter()
        .filter_map(|f| match f {
            LinkFrame::Request(r) => Some(r.request_id),
            _ => None,
        })
        .collect();
    let cancels: Vec<_> = seen
        .iter()
        .filter_map(|f| match f {
            LinkFrame::CancelRequest { request_id } => Some(*request_id),
            _ => None,
        })
        .collect();
    assert!(requests.is_empty(), "abandoned request was sent: {seen:?}");
    assert!(cancels.is_empty(), "cancel for an unsent request: {seen:?}");
    link.close();
}

mod executor;
mod git_sessions;
