use serde_json::json;

use super::*;
use crate::nodes::{NodeArch, NodeOs};
use crate::placement::CapacityRefusal;

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
fn the_daemon_to_daemon_link_speaks_protocol_7_only() {
    // 7 adds the correlated stop acknowledgement. A protocol-6 peer cannot
    // send `stopAck`, so it never links: a backend must not take a stop as
    // confirmed by a machine that cannot confirm it.
    assert_eq!(NODE_LINK_PROTOCOLS, ProtocolRange { min: 7, max: 7 });
    ensure_protocol(7).unwrap();
    for peer in [0, 4, 5, 6, NODE_LINK_PROTOCOLS.max + 1] {
        assert!(matches!(
            ensure_protocol(peer),
            Err(Error::InvalidParams(m)) if m.starts_with("node-link-version-mismatch")
        ));
    }
}

#[test]
fn protocol_ranges_negotiate_the_highest_common_version() {
    let range = |min, max| ProtocolRange { min, max };
    assert_eq!(range(6, 8).highest_common(range(7, 9)), Some(8));
    assert_eq!(range(7, 9).highest_common(range(6, 8)), Some(8));
    assert_eq!(range(6, 6).highest_common(range(6, 9)), Some(6));
    assert_eq!(range(6, 7).highest_common(range(8, 9)), None);
    assert_eq!(range(8, 9).highest_common(range(6, 7)), None);
    assert_eq!(range(7, 6).highest_common(range(6, 7)), None, "empty range");
    assert!(range(6, 8).contains(7));
    assert!(!range(6, 8).contains(9));
    assert_eq!(range(6, 6).to_string(), "6");
    assert_eq!(range(6, 8).to_string(), "6-8");
    assert_eq!(
        serde_json::to_value(range(6, 8)).unwrap(),
        json!({ "min": 6, "max": 8 })
    );
    assert!(serde_json::from_value::<ProtocolRange>(json!({ "min": 6 })).is_err());
}

fn capabilities() -> MachineCapabilities {
    MachineCapabilities {
        platform: AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        },
        intentd_version: "0.12.0".into(),
        capacity: MachineCapacity {
            running_agents: 1,
            agent_cap: 4,
            charged_memory_bytes: 1 << 30,
            memory_budget_bytes: Some(8 << 30),
        },
        providers: vec![
            ProviderCapability {
                id: "claude".into(),
                visible: true,
                status: ProviderStatus::SignedIn,
                models: vec!["opus".into(), "sonnet".into()],
                models_stale: false,
            },
            ProviderCapability {
                id: "codex".into(),
                visible: true,
                status: ProviderStatus::CliMissing,
                models: vec![],
                models_stale: true,
            },
        ],
        observed_at: "2026-10-30T00:00:00Z".into(),
    }
}

/// The report as `docs/protocol/node-link.md` "Capabilities report" spells
/// it.
#[test]
fn the_capabilities_report_matches_the_documented_shape() {
    let canonical = json!({
        "platform": { "os": "macos", "arch": "aarch64" },
        "intentdVersion": "0.12.0",
        "capacity": {
            "runningAgents": 1,
            "agentCap": 4,
            "chargedMemoryBytes": 1_073_741_824_u64,
            "memoryBudgetBytes": 8_589_934_592_u64,
        },
        "providers": [
            { "id": "claude", "visible": true, "status": "signed-in",
              "models": ["opus", "sonnet"], "modelsStale": false },
            { "id": "codex", "visible": true, "status": "cli-missing",
              "models": [], "modelsStale": true },
        ],
        "observedAt": "2026-10-30T00:00:00Z",
    });
    assert_eq!(serde_json::to_value(capabilities()).unwrap(), canonical);
    assert_eq!(
        serde_json::from_value::<MachineCapabilities>(canonical).unwrap(),
        capabilities()
    );
    let mut unlimited = capabilities();
    unlimited.capacity.memory_budget_bytes = None;
    let value = serde_json::to_value(&unlimited).unwrap();
    assert!(
        value["capacity"].get("memoryBudgetBytes").is_none(),
        "absent when unlimited"
    );
    assert_eq!(
        serde_json::from_value::<MachineCapabilities>(value).unwrap(),
        unlimited
    );
}

#[test]
fn every_provider_status_has_its_kebab_case_name() {
    for (status, name) in [
        (ProviderStatus::SignedIn, "signed-in"),
        (ProviderStatus::SignedOut, "signed-out"),
        (ProviderStatus::AuthFailed, "auth-failed"),
        (ProviderStatus::CliMissing, "cli-missing"),
        (ProviderStatus::PrerequisiteMissing, "prerequisite-missing"),
        (ProviderStatus::Unknown, "unknown"),
    ] {
        assert_eq!(serde_json::to_value(status).unwrap(), json!(name));
        assert_eq!(
            serde_json::from_value::<ProviderStatus>(json!(name)).unwrap(),
            status
        );
    }
}

#[test]
fn later_additions_within_a_version_decode_on_an_older_peer() {
    let status: ProviderStatus = serde_json::from_value(json!("rate-limited")).unwrap();
    assert_eq!(
        status,
        ProviderStatus::Unknown,
        "a newer status reads as unknown"
    );

    let mut hello = serde_json::to_value(LinkFrame::Hello(LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "0.12.0".into(),
        binding: binding("head", 1),
        acked_seq: NodeCounter(0),
    }))
    .unwrap();
    hello["futureOption"] = json!(true);
    assert!(matches!(
        serde_json::from_value::<LinkFrame>(hello).unwrap(),
        LinkFrame::Hello(_)
    ));

    let mut report = serde_json::to_value(capabilities()).unwrap();
    report["gpu"] = json!("m3");
    report["capacity"]["gpuSlots"] = json!(2);
    report["providers"][0]["quota"] = json!(3);
    assert_eq!(
        serde_json::from_value::<MachineCapabilities>(report.clone()).unwrap(),
        capabilities()
    );
    let reply = json!({ "type": "capabilitiesReply", "capabilities": report, "extra": 1 });
    assert!(matches!(
        LinkFrame::decode(reply.to_string().as_bytes()).unwrap(),
        LinkFrame::CapabilitiesReply { .. }
    ));

    let mut launch = serde_json::to_value(match &every_frame()[4] {
        frame @ LinkFrame::Launch(_) => frame.clone(),
        other => panic!("expected launch, got {other:?}"),
    })
    .unwrap();
    launch["futureOption"] = json!("x");
    assert!(matches!(
        serde_json::from_value::<LinkFrame>(launch).unwrap(),
        LinkFrame::Launch(_)
    ));
}

#[test]
fn protocol_6_launches_require_the_workspace_api_fields() {
    let launch = serde_json::to_value(&every_frame()[4]).unwrap();
    for field in ["agentFeatures", "isSubAgent", "baseCommit", "repoKey"] {
        let mut missing = launch.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<LinkFrame>(missing).is_err(),
            "{field} is required in protocol 6"
        );
    }
}

#[test]
fn the_binding_stays_strict() {
    let mut value = serde_json::to_value(binding("head", 1)).unwrap();
    value["extra"] = json!(1);
    assert!(serde_json::from_value::<LeaseBinding>(value).is_err());
}

fn frame_tag(frame: &LinkFrame) -> String {
    serde_json::to_value(frame).unwrap()["type"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Golden: changing what an executor credential may send is a deliberate
/// security decision, so the list is spelled out here.
#[test]
fn executor_link_requests_golden() {
    assert_eq!(
        EXECUTOR_LINK_REQUESTS,
        [
            "ack",
            "cancel",
            "capabilities",
            "heartbeat",
            "heartbeatReply",
            "hello",
            "launch",
            "prompt",
            "response",
            "stop",
        ]
    );
    let mut sorted = EXECUTOR_LINK_REQUESTS.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted, EXECUTOR_LINK_REQUESTS, "sorted and unique");
}

#[test]
fn every_frame_is_classified_for_executors_consistently_with_the_list() {
    let mut admitted = Vec::new();
    let mut refused = Vec::new();
    for frame in every_frame() {
        let tag = frame_tag(&frame);
        assert_eq!(
            frame.admitted_from_executor(),
            EXECUTOR_LINK_REQUESTS.contains(&tag.as_str()),
            "{tag}"
        );
        if frame.admitted_from_executor() {
            admitted.push(tag);
        } else {
            refused.push(tag);
        }
    }
    admitted.sort_unstable();
    admitted.dedup();
    assert_eq!(
        admitted, EXECUTOR_LINK_REQUESTS,
        "every listed frame exists"
    );
    refused.sort_unstable();
    refused.dedup();
    assert_eq!(
        refused,
        [
            "cancelRequest",
            "capabilitiesReply",
            "journal",
            "replayComplete",
            "request",
            "stopAck",
            "welcome"
        ]
    );
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
            protocol: NODE_LINK_PROTOCOLS,
            intentd_version: "0.12.0".into(),
            binding: binding("head", 3),
            acked_seq: NodeCounter(10),
        }),
        LinkFrame::Welcome(LinkWelcome {
            protocol: 7,
            node_id: "node-1".into(),
            running_agents: vec![agent_id.clone()],
            capabilities: capabilities(),
            hosts: vec!["192.168.1.20".into()],
            tc_address: Some("mac-mini.tc".into()),
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
            profile_env: Some(LaunchProfileEnv(json!({
                "providerRef": "claude-code@work",
                "fingerprint": "f1",
                "env": [{ "key": "CLAUDE_CONFIG_DIR", "value": "~/.claude-work" }],
            }))),
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
            stop_id: request_id,
        },
        LinkFrame::StopAck {
            stop_id: request_id,
            agent_id: agent_id.clone(),
            outcome: StopOutcome::Stopped,
        },
        LinkFrame::StopAck {
            stop_id: request_id,
            agent_id: agent_id.clone(),
            outcome: StopOutcome::NotRunning,
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
                failure: None,
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
        LinkFrame::Capabilities,
        LinkFrame::CapabilitiesReply {
            capabilities: capabilities(),
        },
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
            Control, Control, Control, Control, Control, Control, Control, Control, Control,
            Control, Journal, Journal, Journal, Journal, Journal, Rpc, Rpc, Rpc, Rpc, Control,
            Control
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
fn stop_and_stop_ack_wire_shapes() {
    let agent_id = AgentId::new();
    let stop_id = StopId::new_v4();
    assert_eq!(
        serde_json::to_value(LinkFrame::Stop {
            agent_id: agent_id.clone(),
            stop_id,
        })
        .unwrap(),
        json!({ "type": "stop", "agentId": agent_id.0, "stopId": stop_id.to_string() })
    );
    for (outcome, wire) in [
        (StopOutcome::Stopped, "stopped"),
        (StopOutcome::NotRunning, "not-running"),
    ] {
        assert_eq!(
            serde_json::to_value(LinkFrame::StopAck {
                stop_id,
                agent_id: agent_id.clone(),
                outcome,
            })
            .unwrap(),
            json!({
                "type": "stopAck",
                "stopId": stop_id.to_string(),
                "agentId": agent_id.0,
                "outcome": wire,
            })
        );
    }
    // Protocol 7 requires the stop id: a stop without one is malformed.
    assert!(LinkFrame::decode(
        json!({ "type": "stop", "agentId": agent_id.0 })
            .to_string()
            .as_bytes()
    )
    .is_err());
    assert!(LinkFrame::decode(
        json!({ "type": "stop", "agentId": agent_id.0, "stopId": "not-a-uuid" })
            .to_string()
            .as_bytes()
    )
    .is_err());
    assert!(LinkFrame::decode(
        json!({ "type": "stopAck", "stopId": stop_id.to_string(), "agentId": agent_id.0, "outcome": "notRunning" })
            .to_string()
            .as_bytes()
    )
    .is_err());
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

#[test]
fn launch_profile_env_never_debug_renders_its_values() {
    let command = LaunchCommand {
        agent_id: AgentId::from("a1"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: false,
        profile_env: Some(LaunchProfileEnv(json!({
            "providerRef": "codex@work",
            "fingerprint": "f1",
            "env": [{ "key": "CODEX_ACCESS_TOKEN", "value": "canary-secret-7f3a" }],
        }))),
    };
    let rendered = format!("{command:?} {:?}", LinkFrame::Launch(command.clone()));
    assert!(!rendered.contains("canary-secret-7f3a"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
    let wire = serde_json::to_value(LinkFrame::Launch(command)).unwrap();
    assert_eq!(
        wire["profileEnv"]["env"][0]["value"], "canary-secret-7f3a",
        "the overlay itself crosses the authenticated link"
    );
}

#[test]
fn launch_without_a_profile_omits_profile_env() {
    let command = LaunchCommand {
        agent_id: AgentId::from("a1"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: false,
        profile_env: None,
    };
    let wire = serde_json::to_value(&command).unwrap();
    assert!(wire.get("profileEnv").is_none(), "{wire}");
}

/// The executor frames of `docs/protocol/node-link.md`: an empty
/// `capabilities` request, a `capabilitiesReply`, and the welcome's routes.
#[test]
fn executor_frames_match_the_documented_wire_shapes() {
    let request = json!({ "type": "capabilities" });
    assert_eq!(
        serde_json::to_value(LinkFrame::Capabilities).unwrap(),
        request
    );
    assert_eq!(
        LinkFrame::decode(request.to_string().as_bytes()).unwrap(),
        LinkFrame::Capabilities
    );
    let reply = serde_json::to_value(LinkFrame::CapabilitiesReply {
        capabilities: capabilities(),
    })
    .unwrap();
    assert_eq!(reply["type"], "capabilitiesReply");
    assert_eq!(reply["capabilities"]["capacity"]["agentCap"], 4);

    let welcome = serde_json::to_value(&every_frame()[1]).unwrap();
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(welcome["protocol"], 7);
    assert_eq!(welcome["hosts"], json!(["192.168.1.20"]));
    assert_eq!(welcome["tcAddress"], "mac-mini.tc");
    let mut no_tailcat = welcome.clone();
    no_tailcat.as_object_mut().unwrap().remove("tcAddress");
    match serde_json::from_value::<LinkFrame>(no_tailcat).unwrap() {
        LinkFrame::Welcome(welcome) => assert_eq!(welcome.tc_address, None),
        other => panic!("expected welcome, got {other:?}"),
    }
}

/// `exited.failure` as `docs/protocol/node-link.md` "Capacity refusal on
/// the wire" spells it: an optional structured failure whose `data` is the
/// closed capacity refusal.
#[test]
fn exited_carries_the_documented_capacity_failure() {
    let canonical = json!({
        "kind": "exited",
        "error": "node-capacity: this machine is running 4 of 4 agents",
        "failure": {
            "code": "node-capacity",
            "data": { "limit": "count", "runningAgents": 4, "agentCap": 4 },
        },
    });
    let body: JournalBody = serde_json::from_value(canonical.clone()).unwrap();
    assert_eq!(
        body,
        JournalBody::Exited {
            exit_code: None,
            error: Some("node-capacity: this machine is running 4 of 4 agents".into()),
            failure: Some(ExitFailure {
                code: "node-capacity".into(),
                data: Some(CapacityRefusal::Count {
                    running_agents: 4,
                    agent_cap: 4,
                }),
            }),
        }
    );
    assert_eq!(serde_json::to_value(&body).unwrap(), canonical);

    let mut open_data = canonical.clone();
    open_data["failure"]["data"]["note"] = json!("x");
    assert!(
        serde_json::from_value::<JournalBody>(open_data).is_err(),
        "the capacity refusal is closed"
    );

    let plain = json!({ "kind": "exited", "exitCode": 1 });
    assert!(matches!(
        serde_json::from_value::<JournalBody>(plain).unwrap(),
        JournalBody::Exited { failure: None, .. }
    ));
}
