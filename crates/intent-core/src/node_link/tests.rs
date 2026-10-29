use serde_json::json;

use super::*;
use crate::nodes::{NodeArch, NodeOs};

fn binding(head: &str, generation: u64) -> LeaseBinding {
    LeaseBinding {
        head_id: head.into(),
        node_id: "node-1".into(),
        lease_id: format!("lease-{generation}"),
        generation: NodeCounter(generation),
    }
}

#[test]
fn the_first_bind_from_any_head_is_accepted() {
    assert_eq!(
        LeaseBinding::decide("node-1", None, &binding("head", 1)),
        BindDecision::Accept
    );
}

#[test]
fn only_a_newer_generation_from_the_owning_head_rebinds() {
    let current = binding("head", 5);
    assert_eq!(
        LeaseBinding::decide("node-1", Some(&current), &binding("head", 6)),
        BindDecision::Accept
    );
    for generation in [1, 4, 5] {
        assert_eq!(
            LeaseBinding::decide("node-1", Some(&current), &binding("head", generation)),
            BindDecision::Stale {
                current: NodeCounter(5)
            },
            "generation {generation}"
        );
    }
    assert_eq!(
        LeaseBinding::decide("node-1", Some(&current), &binding("other-head", 99)),
        BindDecision::OtherHead
    );
}

#[test]
fn a_bind_for_another_node_is_refused() {
    let mut offered = binding("head", 1);
    offered.node_id = "node-2".into();
    assert_eq!(
        LeaseBinding::decide("node-1", None, &offered),
        BindDecision::WrongNode
    );
}

#[test]
fn head_dispatch_rejects_a_replaced_binding() {
    let old = binding("head", 1);
    let current = binding("head", 2);
    current.ensure_current(&current).unwrap();
    assert!(matches!(
        old.ensure_current(&current),
        Err(Error::InvalidParams(m)) if m == "node-link-stale"
    ));
    let mut other_lease = current.clone();
    other_lease.lease_id = "lease-x".into();
    assert!(other_lease.ensure_current(&current).is_err());
}

#[test]
fn the_binding_wire_shape_uses_decimal_string_generations() {
    let value = serde_json::to_value(binding("head", 7)).unwrap();
    assert_eq!(
        value,
        json!({ "headId": "head", "nodeId": "node-1", "leaseId": "lease-7", "generation": "7" })
    );
    assert!(serde_json::from_value::<LeaseBinding>(json!({
        "headId": "head", "nodeId": "node-1", "leaseId": "l", "generation": 7
    }))
    .is_err());
    assert!(serde_json::from_value::<LeaseBinding>(json!({
        "headId": "head", "nodeId": "node-1", "leaseId": "l", "generation": "7", "extra": 1
    }))
    .is_err());
}

#[test]
fn protocol_versions_must_match_exactly() {
    ensure_protocol(NODE_LINK_PROTOCOL).unwrap();
    for peer in [0, 4, NODE_LINK_PROTOCOL + 1] {
        assert!(matches!(
            ensure_protocol(peer),
            Err(Error::InvalidParams(m)) if m.starts_with("node-link-version-mismatch")
        ));
    }
}

fn record(seq: u64, body: JournalBody) -> JournalRecord {
    JournalRecord {
        seq: NodeCounter(seq),
        agent_id: AgentId::new(),
        workspace_id: WorkspaceId::new(),
        turn_id: Some("turn-1".into()),
        recorded_at: "2026-10-27T00:00:00Z".into(),
        body,
    }
}

fn every_frame() -> Vec<LinkFrame> {
    let agent_id = AgentId::new();
    let request_id = LinkId::new_v4();
    vec![
        LinkFrame::Hello(LinkHello {
            protocol: NODE_LINK_PROTOCOL,
            intentd_version: "0.11.62".into(),
            binding: binding("head", 3),
            acked_seq: NodeCounter(10),
        }),
        LinkFrame::Welcome(LinkWelcome {
            protocol: NODE_LINK_PROTOCOL,
            intentd_version: "0.11.62".into(),
            node_id: "node-1".into(),
            platform: AgentPlatform {
                os: NodeOs::Macos,
                arch: NodeArch::Aarch64,
            },
            max_agents: 4,
            running_agents: vec![agent_id.clone()],
        }),
        LinkFrame::Heartbeat { nonce: 1 },
        LinkFrame::HeartbeatReply { nonce: 1 },
        LinkFrame::Launch(LaunchCommand {
            agent_id: agent_id.clone(),
            workspace_id: WorkspaceId::new(),
            provider: "claude".into(),
            model: Some("opus".into()),
            base_commit: "base".into(),
            repo_key: "repo".into(),
            agent_features: serde_json::json!({}),
            is_sub_agent: false,
        }),
        LinkFrame::Prompt {
            agent_id: agent_id.clone(),
            turn_id: "turn-1".into(),
            prompt: vec![json!({ "type": "text", "text": "hi" })],
        },
        LinkFrame::Cancel {
            agent_id: agent_id.clone(),
        },
        LinkFrame::Stop {
            agent_id: agent_id.clone(),
        },
        LinkFrame::Journal(record(
            11,
            JournalBody::Notification {
                notification: json!({ "sessionId": "s", "update": {} }),
            },
        )),
        LinkFrame::Journal(record(
            12,
            JournalBody::TurnEnded {
                stop_reason: "end_turn".into(),
                error: None,
            },
        )),
        LinkFrame::Journal(record(
            13,
            JournalBody::Exited {
                exit_code: Some(1),
                error: Some("crashed".into()),
            },
        )),
        LinkFrame::Ack {
            seq: NodeCounter(13),
        },
        LinkFrame::ReplayComplete {
            through_seq: NodeCounter(13),
        },
        LinkFrame::Request(LinkRequest {
            request_id,
            agent_id,
            method: "note.list".into(),
            params: json!({}),
            timeout_ms: 30_000,
        }),
        LinkFrame::Response {
            request_id,
            outcome: LinkOutcome::Ok(json!([])),
        },
        LinkFrame::Response {
            request_id,
            outcome: LinkOutcome::Err(LinkRpcError {
                code: -32602,
                message: "invalid params: x".into(),
                data: Some(json!({ "code": "x" })),
            }),
        },
        LinkFrame::CancelRequest { request_id },
    ]
}

#[test]
fn every_frame_round_trips_through_the_codec() {
    for frame in every_frame() {
        let bytes = frame.encode().unwrap();
        assert_eq!(LinkFrame::decode(&bytes).unwrap(), frame);
    }
}

#[test]
fn frames_are_routed_to_their_lane() {
    use LinkChannel::{Control, Journal, Rpc};
    let lanes: Vec<_> = every_frame().iter().map(LinkFrame::channel).collect();
    assert_eq!(
        lanes,
        [
            Control, Control, Control, Control, Control, Control, Control, Control, Journal,
            Journal, Journal, Journal, Journal, Rpc, Rpc, Rpc, Rpc
        ]
    );
}

#[test]
fn frame_wire_shapes_are_tagged_camel_case() {
    let ack = serde_json::to_value(LinkFrame::Ack {
        seq: NodeCounter(3),
    })
    .unwrap();
    assert_eq!(ack, json!({ "type": "ack", "seq": "3" }));
    let replay = serde_json::to_value(LinkFrame::ReplayComplete {
        through_seq: NodeCounter(3),
    })
    .unwrap();
    assert_eq!(
        replay,
        json!({ "type": "replayComplete", "throughSeq": "3" })
    );
    let journal = serde_json::to_value(LinkFrame::Journal(record(
        1,
        JournalBody::TurnEnded {
            stop_reason: "cancelled".into(),
            error: None,
        },
    )))
    .unwrap();
    assert_eq!(journal["type"], "journal");
    assert_eq!(journal["seq"], "1");
    assert_eq!(
        journal["body"],
        json!({ "kind": "turnEnded", "stopReason": "cancelled" })
    );
}

#[test]
fn malformed_and_unknown_frames_reject() {
    for raw in [
        r#"{"type":"teleport"}"#,
        r#"{"seq":"1"}"#,
        r#"{"type":"ack","seq":1}"#,
        r#"{"type":"ack","seq":"01"}"#,
        "not json",
    ] {
        assert!(
            matches!(
                LinkFrame::decode(raw.as_bytes()),
                Err(Error::InvalidParams(m)) if m.starts_with("node-link-frame-invalid")
            ),
            "{raw}"
        );
    }
}

#[test]
fn oversized_frames_reject_both_ways() {
    let big = vec![b' '; MAX_FRAME_BYTES + 1];
    assert!(matches!(
        LinkFrame::decode(&big),
        Err(Error::InvalidParams(m)) if m.starts_with("node-link-frame-too-large")
    ));
    let frame = LinkFrame::Prompt {
        agent_id: AgentId::new(),
        turn_id: "t".into(),
        prompt: vec![json!("x".repeat(MAX_FRAME_BYTES))],
    };
    assert!(matches!(
        frame.encode(),
        Err(Error::InvalidParams(m)) if m.starts_with("node-link-frame-too-large")
    ));
}

#[test]
fn journal_ingest_order_is_contiguous_and_dedupes_replays() {
    let classify = |acked, seq| JournalOrder::classify(NodeCounter(acked), NodeCounter(seq));
    assert_eq!(classify(0, 1), JournalOrder::Next);
    assert_eq!(classify(41, 42), JournalOrder::Next);
    assert_eq!(classify(42, 42), JournalOrder::Duplicate);
    assert_eq!(classify(42, 7), JournalOrder::Duplicate);
    assert_eq!(classify(42, 44), JournalOrder::Gap);
    assert_eq!(classify(u64::MAX, u64::MAX), JournalOrder::Duplicate);
}
