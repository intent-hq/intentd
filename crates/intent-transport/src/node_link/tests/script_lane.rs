//! The protocol-8 script lane: script frames travel only on a link that
//! negotiated 8, behind every other lane, and queued output is bounded.

use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use intent_core::node_link::{
    ScriptExited, ScriptOutput, ScriptProcessRef, ScriptStartCommand, SCRIPT_OUTPUT_QUEUE_BYTES,
};

use super::*;

fn start_frame(agent_id: &AgentId) -> LinkFrame {
    LinkFrame::ScriptStart(ScriptStartCommand {
        agent_id: agent_id.clone(),
        script_id: "s1".into(),
        spawn_id: "spawn-1".into(),
        command: "echo hi".into(),
        mode: intent_core::ScriptMode::Command,
        cwd: None,
        env: vec![],
    })
}

fn output(agent_id: &AgentId, seq: u64, data: String) -> ScriptOutput {
    ScriptOutput {
        agent_id: agent_id.clone(),
        script_id: "s1".into(),
        spawn_id: "spawn-1".into(),
        seq,
        data,
    }
}

#[tokio::test]
async fn script_frames_travel_both_ways_on_a_protocol_8_link() {
    let mut node = Node::start(token("t")).await;
    let mut offer = hello(binding(1));
    offer.protocol = ProtocolRange { min: 7, max: 8 };
    let head = dial_link(&node.endpoint(), offer).await.unwrap();
    let (machine, machine_events, _) = node.accept_link().await;
    assert_eq!(head.link.protocol(), 8);
    assert_eq!(machine.protocol(), 8);
    script_frames_travel_both_ways(head, machine, machine_events).await;
}

/// Two current builds negotiate their highest protocol, which keeps
/// protocol 8's script frames.
#[tokio::test]
async fn script_frames_travel_both_ways_on_the_current_protocol_link() {
    let mut node = Node::start(token("t")).await;
    let head = dial(&node, 1).await;
    let (machine, machine_events, _) = node.accept_link().await;
    let current = intent_core::node_link::NODE_LINK_PROTOCOLS.max;
    assert_eq!(head.link.protocol(), current);
    assert_eq!(machine.protocol(), current);
    script_frames_travel_both_ways(head, machine, machine_events).await;
}

async fn script_frames_travel_both_ways(
    head: DialedLink,
    machine: NodeLink,
    mut machine_events: LinkEvents,
) {
    let agent_id = AgentId::new();
    head.link.send(&start_frame(&agent_id)).await.unwrap();
    match tokio::time::timeout(wait(), machine_events.recv())
        .await
        .unwrap()
    {
        Some(LinkEvent::Frame(LinkFrame::ScriptStart(start))) => {
            assert_eq!(start.agent_id, agent_id);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(machine
        .try_send_script_output(output(&agent_id, 0, "hi\n".into()))
        .unwrap());
    machine
        .send(&LinkFrame::ScriptExited(ScriptExited {
            agent_id: agent_id.clone(),
            script_id: "s1".into(),
            spawn_id: "spawn-1".into(),
            exit_code: Some(0),
            error: None,
        }))
        .await
        .unwrap();
    let mut head_events = head.events;
    let mut got = Vec::new();
    while got.len() < 2 {
        match tokio::time::timeout(wait(), head_events.recv())
            .await
            .unwrap()
        {
            Some(LinkEvent::Frame(frame)) => got.push(frame),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(matches!(&got[0], LinkFrame::ScriptOutput(o) if o.data == "hi\n" && o.seq == 0));
    assert!(matches!(&got[1], LinkFrame::ScriptExited(e) if e.exit_code == Some(0)));
}

#[tokio::test]
async fn a_protocol_7_link_neither_sends_nor_accepts_script_frames() {
    let mut node = Node::start(token("t")).await;
    let mut offer = hello(binding(1));
    offer.protocol = ProtocolRange { min: 7, max: 7 };
    let head = dial_link(&node.endpoint(), offer).await.unwrap();
    let (machine, _events, _) = node.accept_link().await;
    assert_eq!(head.link.protocol(), 7);
    assert_eq!(machine.protocol(), 7);
    let agent_id = AgentId::new();
    let error = head.link.send(&start_frame(&agent_id)).await.unwrap_err();
    assert_eq!(code(&error), codes::PROTOCOL);
    let error = machine
        .try_send_script_output(output(&agent_id, 0, "x".into()))
        .unwrap_err();
    assert_eq!(code(&error), codes::PROTOCOL);
    assert!(!head.link.is_closed());
}

#[tokio::test]
async fn a_script_frame_received_on_a_protocol_7_link_closes_it() {
    let (near, far) = tokio::io::duplex(64 * 1024);
    let near = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let mut peer = WebSocketStream::from_raw_socket(far, Role::Server, None).await;
    let (link, _events) = NodeLink::start_at(near, binding(1), Tuning::default(), 7);
    let bytes = LinkFrame::ScriptOutput(output(&AgentId::new(), 0, "x".into()))
        .encode()
        .unwrap();
    peer.send(Message::Binary(bytes.into())).await.unwrap();
    tokio::time::timeout(wait(), link.closed()).await.unwrap();
}

#[tokio::test]
async fn control_frames_overtake_queued_script_output() {
    let (near, far) = tokio::io::duplex(1024);
    let near = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let mut peer = WebSocketStream::from_raw_socket(far, Role::Server, None).await;
    let (link, _events) = NodeLink::start_at(near, binding(1), Tuning::default(), 8);
    let agent_id = AgentId::new();
    for seq in 0..8 {
        assert!(link
            .try_send_script_output(output(&agent_id, seq, "y".repeat(16 * 1024)))
            .unwrap());
    }
    link.send(&LinkFrame::Cancel {
        agent_id: agent_id.clone(),
    })
    .await
    .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 9 {
        match tokio::time::timeout(wait(), peer.next()).await.unwrap() {
            Some(Ok(Message::Binary(bytes))) => seen.push(LinkFrame::decode(&bytes).unwrap()),
            Some(Ok(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let cancel = seen
        .iter()
        .position(|f| matches!(f, LinkFrame::Cancel { .. }))
        .unwrap();
    assert!(
        cancel < 8,
        "the control frame waited behind every output frame: {cancel}"
    );
}

#[tokio::test]
async fn queued_script_output_is_bounded_but_lifecycle_frames_are_never_dropped() {
    let (near, _far) = tokio::io::duplex(64);
    let near = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let (link, _events) = NodeLink::start_at(near, binding(1), Tuning::default(), 8);
    let agent_id = AgentId::new();
    let chunk = "z".repeat(64 * 1024);
    let mut queued = 0_usize;
    let mut seq = 0;
    while link
        .try_send_script_output(output(&agent_id, seq, chunk.clone()))
        .unwrap()
    {
        queued += chunk.len();
        seq += 1;
        assert!(
            queued <= SCRIPT_OUTPUT_QUEUE_BYTES + chunk.len(),
            "unbounded: {queued}"
        );
    }
    assert!(
        queued >= SCRIPT_OUTPUT_QUEUE_BYTES - chunk.len(),
        "bound too small: {queued}"
    );
    tokio::time::timeout(
        wait(),
        link.send(&LinkFrame::ScriptStop(ScriptProcessRef {
            agent_id: agent_id.clone(),
            script_id: "s1".into(),
            spawn_id: "spawn-1".into(),
        })),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!link.is_closed());
    link.close();
}

/// Later protocols include protocol 8: a link that negotiated 9 still
/// sends and accepts script frames (the gate is ">= 8", never "== 8").
#[tokio::test]
async fn a_link_above_protocol_8_still_carries_script_frames() {
    let (near, far) = tokio::io::duplex(64 * 1024);
    let near = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let mut peer = WebSocketStream::from_raw_socket(far, Role::Server, None).await;
    let (link, mut events) = NodeLink::start_at(near, binding(1), Tuning::default(), 9);
    let agent_id = AgentId::new();
    link.send(&start_frame(&agent_id)).await.unwrap();
    assert!(link
        .try_send_script_output(output(&agent_id, 0, "x".into()))
        .unwrap());
    let bytes = LinkFrame::ScriptOutput(output(&agent_id, 0, "y".into()))
        .encode()
        .unwrap();
    peer.send(Message::Binary(bytes.into())).await.unwrap();
    match tokio::time::timeout(wait(), events.recv()).await.unwrap() {
        Some(LinkEvent::Frame(LinkFrame::ScriptOutput(o))) => assert_eq!(o.data, "y"),
        other => panic!("unexpected {other:?}"),
    }
    let mut sent = Vec::new();
    while sent.len() < 2 {
        match tokio::time::timeout(wait(), peer.next()).await.unwrap() {
            Some(Ok(Message::Binary(bytes))) => sent.push(LinkFrame::decode(&bytes).unwrap()),
            Some(Ok(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(matches!(sent[0], LinkFrame::ScriptStart(_)));
    assert!(matches!(sent[1], LinkFrame::ScriptOutput(_)));
    assert!(!link.is_closed());
}
