//! Correlated stop acknowledgements on the backend's link
//! ([`NodeLink::stop`]): a stop is confirmed only by the machine's
//! `stopAck` carrying its own stop id.

use intent_core::node_link::{StopId, StopOutcome};

use super::*;

/// A bound pair: the backend's dialed link and the machine's accepted link.
async fn pair() -> (Node, DialedLink, NodeLink, LinkEvents) {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (machine, events, _) = node.accept_link().await;
    (node, head, machine, events)
}

/// The next stop the machine received.
async fn next_stop(events: &mut LinkEvents) -> (AgentId, StopId) {
    loop {
        match tokio::time::timeout(wait(), events.recv()).await.unwrap() {
            Some(LinkEvent::Frame(LinkFrame::Stop { agent_id, stop_id })) => {
                return (agent_id, stop_id)
            }
            Some(_) => {}
            None => panic!("the machine's link closed"),
        }
    }
}

async fn ack(machine: &NodeLink, stop_id: StopId, agent_id: &AgentId, outcome: StopOutcome) {
    machine
        .send(&LinkFrame::StopAck {
            stop_id,
            agent_id: agent_id.clone(),
            outcome,
        })
        .await
        .unwrap();
}

fn spawn_stop(
    link: &NodeLink,
    agent_id: &AgentId,
    stop_id: StopId,
    timeout: Duration,
) -> tokio::task::JoinHandle<intent_core::Result<StopOutcome>> {
    let link = link.clone();
    let agent_id = agent_id.clone();
    tokio::spawn(async move { link.stop(agent_id, stop_id, timeout).await })
}

/// Not finished after a short grace: nothing confirmed it.
async fn still_waiting<T>(task: &tokio::task::JoinHandle<T>) {
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!task.is_finished(), "the stop must still be waiting");
}

#[tokio::test]
async fn a_stop_is_confirmed_only_by_its_own_ack() {
    let (_node, head, machine, mut events) = pair().await;
    let agent = AgentId::new();
    let older = StopId::new_v4();
    let newer = StopId::new_v4();
    let stop = spawn_stop(&head.link, &agent, newer, wait());
    let (got_agent, got_id) = next_stop(&mut events).await;
    assert_eq!((got_agent, got_id), (agent.clone(), newer));

    // An older stop's reply, for the same agent, never confirms the newer one.
    ack(&machine, older, &agent, StopOutcome::Stopped).await;
    still_waiting(&stop).await;

    ack(&machine, newer, &agent, StopOutcome::NotRunning).await;
    assert_eq!(stop.await.unwrap().unwrap(), StopOutcome::NotRunning);
}

#[tokio::test]
async fn acks_may_arrive_out_of_order() {
    let (_node, head, machine, mut events) = pair().await;
    let (first_agent, second_agent) = (AgentId::new(), AgentId::new());
    let (first_id, second_id) = (StopId::new_v4(), StopId::new_v4());
    let first = spawn_stop(&head.link, &first_agent, first_id, wait());
    assert_eq!(next_stop(&mut events).await.1, first_id);
    let second = spawn_stop(&head.link, &second_agent, second_id, wait());
    assert_eq!(next_stop(&mut events).await.1, second_id);

    ack(&machine, second_id, &second_agent, StopOutcome::Stopped).await;
    assert_eq!(second.await.unwrap().unwrap(), StopOutcome::Stopped);
    still_waiting(&first).await;
    ack(&machine, first_id, &first_agent, StopOutcome::NotRunning).await;
    assert_eq!(first.await.unwrap().unwrap(), StopOutcome::NotRunning);
}

#[tokio::test]
async fn duplicate_stop_ids_resolve_together_on_one_ack() {
    let (_node, head, machine, mut events) = pair().await;
    let agent = AgentId::new();
    let stop_id = StopId::new_v4();
    let a = spawn_stop(&head.link, &agent, stop_id, wait());
    let b = spawn_stop(&head.link, &agent, stop_id, wait());
    // Each call sends its stop; the machine treats a repeat as the same stop.
    assert_eq!(next_stop(&mut events).await.1, stop_id);
    assert_eq!(next_stop(&mut events).await.1, stop_id);
    ack(&machine, stop_id, &agent, StopOutcome::Stopped).await;
    assert_eq!(a.await.unwrap().unwrap(), StopOutcome::Stopped);
    assert_eq!(b.await.unwrap().unwrap(), StopOutcome::Stopped);
    // A second ack for the same, now settled, id is ignored.
    ack(&machine, stop_id, &agent, StopOutcome::Stopped).await;
    assert!(!head.link.is_closed());
}

#[tokio::test]
async fn unknown_ack_ids_are_ignored_and_never_surface_as_events() {
    let (_node, mut head, machine, mut events) = pair().await;
    let agent = AgentId::new();
    ack(&machine, StopId::new_v4(), &agent, StopOutcome::Stopped).await;
    // The link stays usable: a later stop is still confirmed by its own ack.
    let stop_id = StopId::new_v4();
    let stop = spawn_stop(&head.link, &agent, stop_id, wait());
    assert_eq!(next_stop(&mut events).await.1, stop_id);
    ack(&machine, stop_id, &agent, StopOutcome::Stopped).await;
    assert_eq!(stop.await.unwrap().unwrap(), StopOutcome::Stopped);
    assert!(!head.link.is_closed());
    // Acks are consumed by the link, not handed to the owner.
    machine
        .send(&LinkFrame::Ack {
            seq: NodeCounter(1),
        })
        .await
        .unwrap();
    match tokio::time::timeout(wait(), head.events.recv())
        .await
        .unwrap()
    {
        Some(LinkEvent::Frame(LinkFrame::Ack { .. })) => {}
        other => panic!("expected only the journal ack, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unacked_stop_times_out_and_a_resend_with_the_same_id_is_confirmed() {
    let (_node, mut head, machine, mut events) = pair().await;
    let agent = AgentId::new();
    let stop_id = StopId::new_v4();
    let error = head
        .link
        .stop(agent.clone(), stop_id, Duration::from_millis(200))
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::TIMEOUT);
    assert_eq!(next_stop(&mut events).await.1, stop_id);
    // A late ack for the timed-out wait is ignored; the caller resends.
    ack(&machine, stop_id, &agent, StopOutcome::Stopped).await;
    // The backend reads frames in order: once it has the journal ack sent
    // after it, the late stop ack has been handled (and dropped).
    machine
        .send(&LinkFrame::Ack {
            seq: NodeCounter(1),
        })
        .await
        .unwrap();
    match tokio::time::timeout(wait(), head.events.recv())
        .await
        .unwrap()
    {
        Some(LinkEvent::Frame(LinkFrame::Ack { .. })) => {}
        other => panic!("expected the journal ack, got {other:?}"),
    }
    let resend = spawn_stop(&head.link, &agent, stop_id, wait());
    assert_eq!(next_stop(&mut events).await.1, stop_id);
    still_waiting(&resend).await;
    ack(&machine, stop_id, &agent, StopOutcome::NotRunning).await;
    assert_eq!(resend.await.unwrap().unwrap(), StopOutcome::NotRunning);
}

#[tokio::test]
async fn closing_the_link_fails_a_waiting_stop() {
    let (_node, head, _machine, mut events) = pair().await;
    let agent = AgentId::new();
    let stop = spawn_stop(&head.link, &agent, StopId::new_v4(), wait());
    let _ = next_stop(&mut events).await;
    head.link.close();
    let error = stop.await.unwrap().unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
    let error = head
        .link
        .stop(agent, StopId::new_v4(), wait())
        .await
        .unwrap_err();
    assert_eq!(code(&error), codes::CLOSED);
}

#[tokio::test]
async fn a_backend_may_not_send_a_stop_ack() {
    let (_node, head, machine, _events) = pair().await;
    head.link
        .send(&LinkFrame::StopAck {
            stop_id: StopId::new_v4(),
            agent_id: AgentId::new(),
            outcome: StopOutcome::Stopped,
        })
        .await
        .unwrap();
    // The machine closes a link whose executor peer sends a machine frame.
    tokio::time::timeout(wait(), machine.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_protocol_6_backend_is_refused() {
    let mut node = Node::start(token("t")).await;
    let mut offer = hello(binding(1));
    offer.protocol = ProtocolRange { min: 6, max: 6 };
    let error = dial_link(&node.endpoint(), offer).await.unwrap_err();
    assert_eq!(code(&error), codes::VERSION_MISMATCH);
    node.nothing_admitted().await;
}

#[tokio::test]
async fn a_protocol_6_machine_is_refused() {
    use futures_util::SinkExt as _;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    // A machine that cannot acknowledge stops answers with protocol 6; the
    // backend refuses it rather than ever take its stops as confirmed.
    let mut old = welcome();
    old.protocol = 6;
    old.node_id = NODE.into();
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
    server
        .send(Message::Binary(
            LinkFrame::Welcome(old).encode().unwrap().into(),
        ))
        .await
        .unwrap();
    let error = read_welcome(&mut client, &binding(1)).await.unwrap_err();
    assert_eq!(code(&error), codes::VERSION_MISMATCH);
}
