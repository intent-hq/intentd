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
fn the_daemon_to_daemon_link_starts_at_protocol_6() {
    assert_eq!(NODE_LINK_PROTOCOLS, ProtocolRange { min: 6, max: 6 });
    ensure_protocol(6).unwrap();
    for peer in [0, 4, 5, NODE_LINK_PROTOCOLS.max + 1] {
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
        max_agents: 4,
        running_agents: 1,
        memory_budget_bytes: Some(8 << 30),
        memory_charged_bytes: Some(1 << 30),
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

#[test]
fn the_capabilities_report_wire_shape() {
    assert_eq!(
        serde_json::to_value(capabilities()).unwrap(),
        json!({
            "platform": { "os": "macos", "arch": "aarch64" },
            "intentdVersion": "0.12.0",
            "maxAgents": 4,
            "runningAgents": 1,
            "memoryBudgetBytes": 8_589_934_592_u64,
            "memoryChargedBytes": 1_073_741_824_u64,
            "providers": [
                { "id": "claude", "visible": true, "status": "signed-in",
                  "models": ["opus", "sonnet"], "modelsStale": false },
                { "id": "codex", "visible": true, "status": "cli-missing",
                  "models": [], "modelsStale": true },
            ],
            "observedAt": "2026-10-30T00:00:00Z",
        })
    );
    let mut lean = capabilities();
    lean.memory_budget_bytes = None;
    lean.memory_charged_bytes = None;
    let value = serde_json::to_value(&lean).unwrap();
    assert!(value.get("memoryBudgetBytes").is_none());
    assert!(value.get("memoryChargedBytes").is_none());
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
    report["providers"][0]["quota"] = json!(3);
    report["providers"][0]
        .as_object_mut()
        .unwrap()
        .remove("modelsStale");
    let decoded: MachineCapabilities = serde_json::from_value(report.clone()).unwrap();
    assert!(
        !decoded.providers[0].models_stale,
        "modelsStale defaults to false"
    );
    report.as_object_mut().unwrap().remove("providers");
    let decoded: MachineCapabilities = serde_json::from_value(report).unwrap();
    assert!(decoded.providers.is_empty(), "providers default to none");

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
            "capabilitiesQuery",
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
            "capabilities",
            "journal",
            "replayComplete",
            "request",
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
            protocol: 6,
            node_id: "node-1".into(),
            running_agents: vec![agent_id.clone()],
            capabilities: capabilities(),
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
        LinkFrame::CapabilitiesQuery { nonce: 7 },
        LinkFrame::Capabilities {
            nonce: 7,
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
            Control, Control, Control, Control, Control, Control, Control, Control, Journal,
            Journal, Journal, Journal, Journal, Rpc, Rpc, Rpc, Rpc, Control, Control
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
