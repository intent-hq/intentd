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
fn the_daemon_to_daemon_link_speaks_protocols_7_to_16() {
    // 7 adds the correlated stop acknowledgement. A protocol-6 peer cannot
    // send `stopAck`, so it never links: a backend must not take a stop as
    // confirmed by a machine that cannot confirm it. 8 adds the script
    // frames, 9 final stops, 10 the prepared checkout, 11 the user MCP
    // server proxies, 12 the machine's skills, 13 sub-agent checkouts, 14
    // agent secrets on the machine, 15 the backend's forge token, 16 reads
    // of an agent's checkout files, 17 sub-agent fetch through the backend,
    // 18 an orchestrator's quick actions on the machine, 19 workspace
    // homes; a protocol-7 peer still binds and runs agents.
    assert_eq!(NODE_LINK_PROTOCOLS, ProtocolRange { min: 7, max: 19 });
    assert_eq!(ONE_SHOT_PROTOCOL, 18);
    assert_eq!(SCRIPT_PROTOCOL, 8);
    assert_eq!(FINAL_STOP_PROTOCOL, 9);
    assert_eq!(PREPARED_CHECKOUT_PROTOCOL, 10);
    assert_eq!(MCP_PROXY_PROTOCOL, 11);
    assert_eq!(CHECKOUT_FILES_PROTOCOL, 16);
    assert_eq!(SKILLS_PROTOCOL, 12);
    assert_eq!(SHARED_CHECKOUT_PROTOCOL, 13);
    assert_eq!(SECRETS_PROTOCOL, 14);
    assert_eq!(FORGE_TOKEN_PROTOCOL, 15);
    assert_eq!(CHECKOUT_FILES_PROTOCOL, 16);
    assert_eq!(FORGE_FETCH_PROTOCOL, 17);
    assert_eq!(HOMES_PROTOCOL, NODE_LINK_PROTOCOLS.max);
    assert!(offers_prepared_checkout());
    for version in 7..=19 {
        ensure_protocol(version).unwrap();
    }
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
        disabled_providers: vec![],
        observed_at: "2026-10-30T00:00:00Z".into(),
        homes: None,
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

/// `disabledProviders` names the providers switched off in the machine's
/// own settings; it is optional, so a report from an older machine (without
/// it) still decodes, with none disabled, and an empty list is omitted.
#[test]
fn disabled_providers_are_optional_and_omitted_when_empty() {
    let mut value = serde_json::to_value(capabilities()).unwrap();
    assert!(
        value.get("disabledProviders").is_none(),
        "omitted when empty"
    );
    assert_eq!(
        serde_json::from_value::<MachineCapabilities>(value.clone())
            .unwrap()
            .disabled_providers,
        Vec::<String>::new(),
        "an older machine's report"
    );
    value["disabledProviders"] = json!(["droid"]);
    let decoded = serde_json::from_value::<MachineCapabilities>(value.clone()).unwrap();
    assert_eq!(decoded.disabled_providers, ["droid"]);
    assert_eq!(serde_json::to_value(&decoded).unwrap(), value);
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
        (ProviderStatus::Disabled, "disabled"),
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
        lease_agents: Vec::new(),
        head_session: None,
        lease_homes: None,
    }))
    .unwrap();
    assert!(
        hello.get("leaseAgents").is_none(),
        "an empty leaseAgents is left out"
    );
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
            "checkoutRead",
            "forgeToken",
            "heartbeat",
            "heartbeatReply",
            "hello",
            "homeOpen",
            "homePush",
            "launch",
            "oneShot",
            "oneShotCancel",
            "prompt",
            "ptyAck",
            "ptyInput",
            "ptyKill",
            "ptyOpen",
            "ptyResize",
            "pushWip",
            "response",
            "scriptStart",
            "scriptStop",
            "secretForget",
            "secretList",
            "secretPut",
            "skills",
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
            "checkoutReadReply",
            "forgeTokenReply",
            "homeOpenReply",
            "homePushReply",
            "journal",
            "oneShotReply",
            "ptyExited",
            "ptyOutput",
            "pushWipReply",
            "replayComplete",
            "request",
            "scriptExited",
            "scriptOutput",
            "scriptStarted",
            "secretReply",
            "skillsReply",
            "stopAck",
            "welcome"
        ]
    );
}

fn record(seq: u64, body: JournalBody) -> JournalRecord {
    JournalRecord {
        seq: NodeCounter(seq),
        owner: AgentId::new().into(),
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
            lease_agents: Vec::new(),
            head_session: None,
            lease_homes: None,
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
            checkout: None,
            mcp_proxies: Vec::new(),
            compact_tool_descriptions: false,
            checkout_of: None,
            forge: None,
            forge_fetch: None,
            git_identity: None,
            new_machine: None,
            resumed_from_disk: false,
            harness_version: None,
            labels: Box::default(),
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
            final_stop: false,
        },
        LinkFrame::StopAck {
            stop_id: request_id,
            agent_id: agent_id.clone(),
            outcome: StopOutcome::Stopped,
            final_push: None,
        },
        LinkFrame::StopAck {
            stop_id: request_id,
            agent_id: agent_id.clone(),
            outcome: StopOutcome::NotRunning,
            final_push: None,
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
        LinkFrame::ScriptStart(script_start()),
        LinkFrame::ScriptStop(ScriptProcessRef {
            owner: AgentId::from("a1").into(),
            script_id: "s1".into(),
            spawn_id: "r1".into(),
        }),
        LinkFrame::ScriptStarted(ScriptStarted {
            owner: AgentId::from("a1").into(),
            script_id: "s1".into(),
            spawn_id: "r1".into(),
            pid: 4242,
        }),
        LinkFrame::ScriptOutput(ScriptOutput {
            owner: AgentId::from("a1").into(),
            script_id: "s1".into(),
            spawn_id: "r1".into(),
            seq: 0,
            data: "hello\n".into(),
        }),
        LinkFrame::ScriptExited(ScriptExited {
            owner: AgentId::from("a1").into(),
            script_id: "s1".into(),
            spawn_id: "r1".into(),
            exit_code: Some(0),
            error: None,
        }),
        LinkFrame::Skills(SkillsQuery {
            query_id: request_id,
        }),
        LinkFrame::SkillsReply(SkillsReply {
            query_id: request_id,
            skills: vec![machine_skill("review", "user")],
        }),
        LinkFrame::Journal(record(
            14,
            JournalBody::Skills {
                skills: vec![machine_skill("deploy", "project")],
            },
        )),
        LinkFrame::PushWip(PushWipRequest {
            request_id,
            agent_id: AgentId::from("a1"),
        }),
        LinkFrame::PushWipReply(PushWipResult {
            request_id,
            agent_id: AgentId::from("a1"),
            outcome: FinalPush::Failed,
            error: Some("head refused the push".into()),
        }),
        LinkFrame::SecretPut(SecretPut {
            request_id,
            agent_id: AgentId::from("a1"),
            name: "API_KEY".into(),
            value: SecretValue::from("canary-agent-secret"),
        }),
        LinkFrame::SecretForget(SecretForget {
            request_id,
            agent_id: AgentId::from("a1"),
            name: Some("API_KEY".into()),
        }),
        LinkFrame::SecretList(SecretList {
            request_id,
            agent_id: AgentId::from("a1"),
        }),
        LinkFrame::SecretReply(SecretReply {
            request_id,
            agent_id: AgentId::from("a1"),
            outcome: SecretOutcome::Ok,
            names: Some(vec!["API_KEY".into()]),
            forgotten: None,
            error: None,
        }),
        LinkFrame::ForgeToken(ForgeToken {
            request_id,
            host: FORGE_HOST_GITHUB.into(),
            token: Some(SecretValue::from("canary-forge-token")),
        }),
        LinkFrame::ForgeToken(ForgeToken {
            request_id,
            host: FORGE_HOST_GITHUB.into(),
            token: None,
        }),
        LinkFrame::ForgeTokenReply(ForgeTokenReply {
            request_id,
            outcome: SecretOutcome::Failed,
            error: Some(FORGE_TOKEN_INVALID.into()),
        }),
        LinkFrame::CheckoutRead(CheckoutRead {
            request_id,
            agent_id: AgentId::from("a1"),
            path: "out/report.txt".into(),
            offset: 0,
            length: 4 * 1024 * 1024,
            transfer_id: None,
        }),
        LinkFrame::CheckoutReadReply(CheckoutReadReply {
            request_id,
            agent_id: AgentId::from("a1"),
            outcome: CheckoutReadOutcome::Ok,
            piece: Some(CheckoutPiece {
                kind: CheckoutEntryKind::Directory,
                size: 3,
                offset: 0,
                data: "YWJj".into(),
                sha256: Some("ba7816bf".into()),
                transfer_id: "t1".into(),
                skipped: Some(2),
            }),
            error: None,
        }),
        LinkFrame::CheckoutReadReply(CheckoutReadReply {
            request_id,
            agent_id: AgentId::from("a1"),
            outcome: CheckoutReadOutcome::Failed,
            piece: None,
            error: Some("file-pull-not-found: out/missing".into()),
        }),
        LinkFrame::OneShot(OneShot {
            request_id,
            operation: OneShotOperation::Complete,
            input: OneShotInput::Complete(OneShotCompleteInput {
                provider_id: "codex".into(),
                prompt: "canary-one-shot-prompt".into(),
                system_prompt: None,
                model: Some("gpt-5".into()),
                effort: Some(EffortCandidates {
                    requested: None,
                    saved: vec!["high".into()],
                }),
            }),
            profile_env: None,
            timeout_ms: 30_000,
        }),
        LinkFrame::OneShotCancel(OneShotCancel { request_id }),
        LinkFrame::OneShotReply(OneShotReply::refused(
            request_id,
            OneShotOutcome::Unavailable,
            "no-capacity",
            "every adapter slot is busy",
        )),
        LinkFrame::HomeOpen(home_open()),
        LinkFrame::HomeOpenReply(HomeOpenReply {
            request_id,
            workspace_id: WorkspaceId::from("ws-1"),
            home_id: HomeId::from("home-1"),
            outcome: HomeOpenOutcome::Ready,
            error: None,
        }),
        LinkFrame::HomePush(HomePush {
            request_id,
            workspace_id: WorkspaceId::from("ws-1"),
            home_id: HomeId::from("home-1"),
        }),
        LinkFrame::HomePushReply(HomePushReply {
            request_id,
            workspace_id: WorkspaceId::from("ws-1"),
            home_id: HomeId::from("home-1"),
            outcome: HomePushOutcome::Pushed,
            head: Some("a".repeat(40)),
            wip: Some("b".repeat(40)),
            error: None,
        }),
        LinkFrame::PtyOpen(PtyOpen {
            terminal_id: "t1".into(),
            workspace_id: WorkspaceId::from("ws-1"),
            home_id: HomeId::from("home-1"),
            mode: PtyMode::Terminal,
            cwd: Some("src".into()),
            command: None,
            env: vec![PtyEnvVar {
                key: "TERM".into(),
                value: "xterm-256color".into(),
            }],
            cols: Some(80),
            rows: Some(24),
        }),
        LinkFrame::PtyInput(PtyInput {
            terminal_id: "t1".into(),
            data: "bHMK".into(),
        }),
        LinkFrame::PtyResize(PtyResize {
            terminal_id: "t1".into(),
            cols: 120,
            rows: 40,
        }),
        LinkFrame::PtyKill(PtyKill {
            terminal_id: "t1".into(),
        }),
        LinkFrame::PtyAck(PtyAck {
            terminal_id: "t1".into(),
            seq: 3,
        }),
        LinkFrame::PtyOutput(PtyOutput {
            terminal_id: "t1".into(),
            seq: 3,
            data: "b2sK".into(),
            stream: Some(PtyStream::Stderr),
        }),
        LinkFrame::PtyExited(PtyExited {
            terminal_id: "t1".into(),
            exit_code: Some(0),
            error: None,
        }),
    ]
}

fn home_open() -> HomeOpen {
    HomeOpen {
        request_id: LinkId::new_v4(),
        workspace_id: WorkspaceId::from("ws-1"),
        home_id: HomeId::from("home-1"),
        base_commit: "a".repeat(40),
        repo_key: "repo".into(),
        checkout: PreparedCheckout::default(),
        restore: Some(HomeRestore {
            head: "a".repeat(40),
            wip: None,
        }),
    }
}

/// nodeProtocol 19 on the wire: a journal record and a setup output name
/// exactly one owner, `checkoutOf` is a string for an agent and an object
/// for a home, and every home addition needs protocol 19.
#[test]
fn home_owners_round_trip_and_need_protocol_19() {
    let home = HomeRef {
        workspace_id: WorkspaceId::from("ws-1"),
        home_id: HomeId::from("home-1"),
    };
    let record = JournalRecord {
        seq: NodeCounter(3),
        owner: RecordOwner::Home(home.home_id.clone()),
        workspace_id: home.workspace_id.clone(),
        turn_id: None,
        recorded_at: "2026-11-12T00:00:00Z".into(),
        body: JournalBody::HomePushed {
            head: Some("a".repeat(40)),
            wip: None,
            rewritten: Some("c".repeat(40)),
        },
    };
    let wire = serde_json::to_value(&record).unwrap();
    assert_eq!(wire["homeId"], "home-1");
    assert!(wire.get("agentId").is_none(), "{wire}");
    assert_eq!(wire["body"]["kind"], "homePushed");
    assert_eq!(
        serde_json::from_value::<JournalRecord>(wire.clone()).unwrap(),
        record
    );
    let mut both = wire.clone();
    both["agentId"] = json!("agent-1");
    assert!(serde_json::from_value::<JournalRecord>(both).is_err());
    let mut neither = wire;
    neither.as_object_mut().unwrap().remove("homeId");
    assert!(serde_json::from_value::<JournalRecord>(neither).is_err());
    assert_eq!(LinkFrame::Journal(record).min_protocol(), HOMES_PROTOCOL);

    let output = SetupOutput {
        owner: CheckoutOwner::Home(home.clone()),
        seq: 0,
        data: "x".into(),
    };
    let wire = serde_json::to_value(&output).unwrap();
    assert_eq!(
        wire,
        json!({ "workspaceId": "ws-1", "homeId": "home-1", "seq": 0, "data": "x" })
    );
    assert_eq!(serde_json::from_value::<SetupOutput>(wire).unwrap(), output);
    assert!(serde_json::from_value::<SetupOutput>(
        json!({ "agentId": "a", "homeId": "home-1", "seq": 0, "data": "x" })
    )
    .is_err());
    assert!(serde_json::from_value::<SetupOutput>(
        json!({ "homeId": "home-1", "seq": 0, "data": "x" })
    )
    .is_err());
    assert_eq!(
        LinkFrame::SetupOutput(output).min_protocol(),
        HOMES_PROTOCOL
    );

    assert_eq!(
        serde_json::to_value(CheckoutOf::Agent(AgentId::from("owner"))).unwrap(),
        json!("owner")
    );
    assert_eq!(
        serde_json::from_value::<CheckoutOf>(json!({ "workspaceId": "ws-1", "homeId": "home-1" }))
            .unwrap(),
        CheckoutOf::Home(home.clone())
    );
    assert!(serde_json::from_value::<CheckoutOf>(
        json!({ "workspaceId": "ws-1", "homeId": "home-1", "extra": true })
    )
    .is_err());

    let mut hello = serde_json::to_value(LinkFrame::Hello(LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "test".into(),
        binding: binding("head", 1),
        acked_seq: NodeCounter(0),
        lease_agents: Vec::new(),
        head_session: None,
        lease_homes: None,
    }))
    .unwrap();
    assert!(hello.get("leaseHomes").is_none(), "{hello}");
    hello["leaseHomes"] = json!([{ "workspaceId": "ws-1", "homeId": "home-1" }]);
    let LinkFrame::Hello(decoded) = serde_json::from_value(hello).unwrap() else {
        panic!("not a hello");
    };
    assert_eq!(decoded.lease_homes, Some(vec![home]));
}

/// The terminal frames (nodeProtocol 19): closed bodies, base64 data,
/// and an optional exec stream.
#[test]
fn pty_frame_wire_shapes() {
    let open = serde_json::to_value(LinkFrame::PtyOpen(PtyOpen {
        terminal_id: "t1".into(),
        workspace_id: WorkspaceId::from("ws-1"),
        home_id: HomeId::from("home-1"),
        mode: PtyMode::Exec,
        cwd: None,
        command: Some("ls".into()),
        env: Vec::new(),
        cols: None,
        rows: None,
    }))
    .unwrap();
    assert_eq!(
        open,
        json!({ "type": "ptyOpen", "terminalId": "t1", "workspaceId": "ws-1", "homeId": "home-1", "mode": "exec", "command": "ls" })
    );
    let mut extra = open;
    extra["shell"] = json!("zsh");
    assert!(serde_json::from_value::<LinkFrame>(extra).is_err());
    let output = serde_json::to_value(LinkFrame::PtyOutput(PtyOutput {
        terminal_id: "t1".into(),
        seq: 0,
        data: "aGk=".into(),
        stream: None,
    }))
    .unwrap();
    assert_eq!(
        output,
        json!({ "type": "ptyOutput", "terminalId": "t1", "seq": 0, "data": "aGk=" })
    );
    let input = LinkFrame::PtyInput(PtyInput {
        terminal_id: "t1".into(),
        data: "c2VjcmV0".into(),
    });
    assert!(!format!("{input:?}").contains("c2VjcmV0"));
}

fn script_start() -> ScriptStartCommand {
    ScriptStartCommand {
        owner: AgentId::from("a1").into(),
        script_id: "s1".into(),
        spawn_id: "r1".into(),
        command: "npm test".into(),
        mode: crate::ScriptMode::Command,
        cwd: Some("web".into()),
        env: vec![ScriptEnvVar {
            key: "API_TOKEN".into(),
            value: "canary-script-secret".into(),
        }],
    }
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
    use LinkChannel::{Control, Journal, Rpc, Script};
    let lanes: Vec<_> = every_frame().iter().map(LinkFrame::channel).collect();
    assert_eq!(
        lanes,
        [
            Control, Control, Control, Control, Control, Control, Control, Control, Control,
            Control, Journal, Journal, Journal, Journal, Journal, Rpc, Rpc, Rpc, Rpc, Control,
            Control, Script, Script, Script, Script, Script, Control, Control, Journal, Control,
            Control, Control, Control, Control, Control, Control, Control, Control, Script, Script,
            Script, Rpc, Rpc, Rpc, Control, Control, Control, Control, Control, Control, Control,
            Control, Control, Script, Script,
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
            final_stop: false,
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
                final_push: None,
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
    // nodeProtocol 9: a final stop and its acknowledgement's push report.
    let final_stop = serde_json::to_value(LinkFrame::Stop {
        agent_id: agent_id.clone(),
        stop_id,
        final_stop: true,
    })
    .unwrap();
    assert_eq!(
        final_stop,
        json!({ "type": "stop", "agentId": agent_id.0, "stopId": stop_id.to_string(), "final": true })
    );
    assert!(matches!(
        LinkFrame::decode(final_stop.to_string().as_bytes()).unwrap(),
        LinkFrame::Stop {
            final_stop: true,
            ..
        }
    ));
    for (push, wire) in [
        (FinalPush::Pushed, "pushed"),
        (FinalPush::Failed, "failed"),
        (FinalPush::Skipped, "skipped"),
    ] {
        let ack = LinkFrame::StopAck {
            stop_id,
            agent_id: agent_id.clone(),
            outcome: StopOutcome::Stopped,
            final_push: Some(push),
        };
        let value = serde_json::to_value(&ack).unwrap();
        assert_eq!(value["finalPush"], wire);
        assert_eq!(
            LinkFrame::decode(value.to_string().as_bytes()).unwrap(),
            ack
        );
    }
    let hello = LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "0.12.0".into(),
        binding: binding("head", 1),
        acked_seq: NodeCounter(0),
        lease_agents: vec![agent_id.clone()],
        head_session: None,
        lease_homes: None,
    };
    let value = serde_json::to_value(LinkFrame::Hello(hello.clone())).unwrap();
    assert_eq!(value["leaseAgents"], json!([agent_id.0]));
    assert_eq!(
        LinkFrame::decode(value.to_string().as_bytes()).unwrap(),
        LinkFrame::Hello(hello)
    );
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
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::default(),
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
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::default(),
    };
    let wire = serde_json::to_value(&command).unwrap();
    assert!(wire.get("profileEnv").is_none(), "{wire}");
    assert!(wire.get("checkout").is_none(), "{wire}");
}

/// nodeProtocol 10's `checkout` member: closed objects, the pins in
/// order, `setup` absent when there is no script; a launch without it
/// (protocol 7 to 9) still decodes.
#[test]
fn a_prepared_checkout_matches_the_documented_wire_shape() {
    let wire = json!({
        "submodules": [
            { "path": "a", "name": "a", "commit": "c1", "moduleKey": "repo-1" },
            { "path": "a/b", "name": "b", "commit": "c2", "moduleKey": "repo-2" },
        ],
    });
    let checkout: PreparedCheckout = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(checkout.submodules[1].path, "a/b");
    assert_eq!(checkout.submodules[1].module_key, "repo-2");
    assert_eq!(checkout.setup, None);
    assert_eq!(serde_json::to_value(&checkout).unwrap(), wire);
    let setup = json!({
        "submodules": [],
        "setup": { "script": "make", "branchName": "feat/x", "sourceBranch": "main", "timeoutSecs": 60 },
    });
    let checkout: PreparedCheckout = serde_json::from_value(setup.clone()).unwrap();
    assert_eq!(checkout.setup.as_ref().unwrap().timeout_secs, Some(60));
    assert_eq!(serde_json::to_value(&checkout).unwrap(), setup);
    for open in [
        json!({ "submodules": [], "extra": 1 }),
        json!({ "submodules": [{ "path": "a", "name": "a", "commit": "c", "moduleKey": "k", "url": "x" }] }),
        json!({ "submodules": [], "setup": { "script": "s", "branchName": "b", "sourceBranch": "m", "env": {} } }),
    ] {
        assert!(
            serde_json::from_value::<PreparedCheckout>(open.clone()).is_err(),
            "{open}"
        );
    }
    let old = json!({
        "agentId": "a1", "workspaceId": "ws-1", "provider": "codex", "baseCommit": "base",
        "repoKey": "repo", "agentFeatures": {}, "isSubAgent": false,
    });
    assert_eq!(
        serde_json::from_value::<LaunchCommand>(old)
            .unwrap()
            .checkout,
        None
    );
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

/// The script frames of `docs/protocol/node-link.md` "Saved scripts
/// (nodeProtocol 8)": camelCase members, closed bodies.
#[test]
fn script_frames_match_the_documented_wire_shapes() {
    assert_eq!(
        serde_json::to_value(LinkFrame::ScriptStart(script_start())).unwrap(),
        json!({
            "type": "scriptStart",
            "agentId": "a1",
            "scriptId": "s1",
            "spawnId": "r1",
            "command": "npm test",
            "mode": "command",
            "cwd": "web",
            "env": [{ "key": "API_TOKEN", "value": "canary-script-secret" }],
        })
    );
    let bare = ScriptStartCommand {
        cwd: None,
        env: Vec::new(),
        mode: crate::ScriptMode::Service,
        ..script_start()
    };
    let wire = serde_json::to_value(LinkFrame::ScriptStart(bare)).unwrap();
    assert!(
        wire.get("cwd").is_none() && wire.get("env").is_none(),
        "{wire}"
    );
    assert_eq!(wire["mode"], "service");
    assert_eq!(
        serde_json::to_value(LinkFrame::ScriptStop(ScriptProcessRef {
            owner: AgentId::from("a1").into(),
            script_id: "s1".into(),
            spawn_id: "r1".into(),
        }))
        .unwrap(),
        json!({ "type": "scriptStop", "agentId": "a1", "scriptId": "s1", "spawnId": "r1" })
    );
    assert_eq!(
        serde_json::to_value(&every_frame()[23]).unwrap(),
        json!({ "type": "scriptStarted", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "pid": 4242 })
    );
    assert_eq!(
        serde_json::to_value(&every_frame()[24]).unwrap(),
        json!({ "type": "scriptOutput", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "seq": 0, "data": "hello\n" })
    );
    assert_eq!(
        serde_json::to_value(&every_frame()[25]).unwrap(),
        json!({ "type": "scriptExited", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "exitCode": 0 })
    );
    let refused = LinkFrame::ScriptExited(ScriptExited {
        owner: AgentId::from("a1").into(),
        script_id: "s1".into(),
        spawn_id: "r1".into(),
        exit_code: None,
        error: Some("node-script-invalid-cwd: /etc".into()),
    });
    assert_eq!(
        serde_json::to_value(refused).unwrap(),
        json!({ "type": "scriptExited", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "error": "node-script-invalid-cwd: /etc" })
    );
}

#[test]
fn script_frame_bodies_are_closed() {
    for raw in [
        json!({ "type": "scriptStop", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "extra": 1 }),
        json!({ "type": "scriptStop", "agentId": "a1", "scriptId": "s1" }),
        json!({ "type": "scriptStop", "scriptId": "s1", "spawnId": "r1" }),
        json!({ "type": "scriptExited", "scriptId": "s1", "spawnId": "r1", "exitCode": 0 }),
        json!({ "type": "scriptStarted", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "pid": "1" }),
        json!({ "type": "scriptOutput", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "seq": 0, "data": "x", "more": true }),
        json!({ "type": "scriptExited", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "exitCode": "0" }),
        json!({ "type": "scriptStart", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "command": "x", "mode": "daemon" }),
        json!({ "type": "scriptStart", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "command": "x", "mode": "command", "autoStart": true }),
        json!({ "type": "scriptStart", "agentId": "a1", "scriptId": "s1", "spawnId": "r1", "command": "x", "mode": "command", "env": [{ "key": "A", "value": "b", "secret": true }] }),
    ] {
        assert!(
            LinkFrame::decode(raw.to_string().as_bytes()).is_err(),
            "{raw}"
        );
    }
}

#[test]
fn script_env_never_debug_renders_its_values() {
    let frame = LinkFrame::ScriptStart(script_start());
    let rendered = format!("{frame:?}");
    assert!(!rendered.contains("canary-script-secret"), "{rendered}");
    assert!(rendered.contains("API_TOKEN"), "{rendered}");
}

#[test]
fn script_frames_need_protocol_8_skills_frames_12_and_push_frames_13() {
    for frame in every_frame() {
        let script = matches!(frame.channel(), LinkChannel::Script);
        let skills = matches!(
            &frame,
            LinkFrame::Skills(_)
                | LinkFrame::SkillsReply(_)
                | LinkFrame::Journal(JournalRecord {
                    body: JournalBody::Skills { .. },
                    ..
                })
        );
        let push = matches!(frame, LinkFrame::PushWip(_) | LinkFrame::PushWipReply(_));
        let secret = matches!(
            frame,
            LinkFrame::SecretPut(_)
                | LinkFrame::SecretForget(_)
                | LinkFrame::SecretList(_)
                | LinkFrame::SecretReply(_)
        );
        let forge = matches!(
            frame,
            LinkFrame::ForgeToken(_) | LinkFrame::ForgeTokenReply(_)
        );
        let checkout = matches!(
            frame,
            LinkFrame::CheckoutRead(_) | LinkFrame::CheckoutReadReply(_)
        );
        let one_shot = matches!(
            frame,
            LinkFrame::OneShot(_) | LinkFrame::OneShotCancel(_) | LinkFrame::OneShotReply(_)
        );
        let home = matches!(
            frame,
            LinkFrame::HomeOpen(_)
                | LinkFrame::HomeOpenReply(_)
                | LinkFrame::HomePush(_)
                | LinkFrame::HomePushReply(_)
                | LinkFrame::PtyOpen(_)
                | LinkFrame::PtyInput(_)
                | LinkFrame::PtyResize(_)
                | LinkFrame::PtyKill(_)
                | LinkFrame::PtyAck(_)
                | LinkFrame::PtyOutput(_)
                | LinkFrame::PtyExited(_)
        );
        let expected = if home {
            HOMES_PROTOCOL
        } else if one_shot {
            ONE_SHOT_PROTOCOL
        } else if checkout {
            CHECKOUT_FILES_PROTOCOL
        } else if forge {
            FORGE_TOKEN_PROTOCOL
        } else if secret {
            SECRETS_PROTOCOL
        } else if push {
            SHARED_CHECKOUT_PROTOCOL
        } else if skills {
            12
        } else if script {
            8
        } else {
            7
        };
        assert_eq!(frame.min_protocol(), expected, "{}", frame_tag(&frame));
    }
}

fn machine_skill(name: &str, scope: &str) -> MachineSkill {
    MachineSkill {
        name: name.into(),
        description: format!("{name} things"),
        location: format!("/Users/me/.claude/skills/{name}/SKILL.md"),
        resource_directory: None,
        scope: scope.into(),
        allowed_tools: None,
        compatibility: None,
    }
}

/// The wire shapes of the skills frames and journal body (`node-link.md`
/// § Skills on the machine).
#[test]
fn skills_frames_and_journal_body_have_the_documented_shape() {
    let id = LinkId::new_v4();
    let query = LinkFrame::Skills(SkillsQuery { query_id: id });
    assert_eq!(
        serde_json::to_value(&query).unwrap(),
        json!({ "type": "skills", "queryId": id })
    );
    let reply = LinkFrame::SkillsReply(SkillsReply {
        query_id: id,
        skills: vec![machine_skill("review", "user")],
    });
    assert_eq!(
        serde_json::to_value(&reply).unwrap(),
        json!({
            "type": "skillsReply",
            "queryId": id,
            "skills": [{
                "name": "review",
                "description": "review things",
                "location": "/Users/me/.claude/skills/review/SKILL.md",
                "scope": "user",
            }],
        })
    );
    let body = JournalBody::Skills {
        skills: vec![machine_skill("deploy", "project")],
    };
    let wire = serde_json::to_value(&body).unwrap();
    assert_eq!(wire["kind"], "skills");
    assert_eq!(wire["skills"][0]["scope"], "project");
    for frame in [query, reply] {
        let bytes = frame.encode().unwrap();
        assert_eq!(LinkFrame::decode(&bytes).unwrap(), frame);
    }
    assert_eq!(serde_json::from_value::<JournalBody>(wire).unwrap(), body);
}

/// A machine below nodeProtocol 10 rejects a launch that carries `checkout`
/// (`node-link-protocol`): `LaunchCommand` takes unknown members, so the
/// gate is explicit. Without `checkout` a launch travels on any link.
#[test]
fn a_launch_with_a_checkout_needs_protocol_10() {
    let launch = |checkout: Option<Box<PreparedCheckout>>| {
        LinkFrame::Launch(LaunchCommand {
            agent_id: AgentId::from("a1"),
            workspace_id: WorkspaceId::from("ws-1"),
            provider: "codex".into(),
            model: None,
            base_commit: "base".into(),
            repo_key: "repo".into(),
            agent_features: json!({}),
            is_sub_agent: false,
            profile_env: None,
            checkout,
            mcp_proxies: Vec::new(),
            compact_tool_descriptions: false,
            checkout_of: None,
            forge: None,
            forge_fetch: None,
            git_identity: None,
            new_machine: None,
            resumed_from_disk: false,
            harness_version: None,
            labels: Box::default(),
        })
    };
    assert_eq!(launch(None).min_protocol(), NODE_LINK_PROTOCOLS.min);
    assert_eq!(
        launch(Some(Box::default())).min_protocol(),
        PREPARED_CHECKOUT_PROTOCOL
    );
}

/// nodeProtocol 11's `mcpProxies`: names and ids only, closed entries; a
/// launch that carries any needs protocol 11 (an older machine would start
/// the agent without them), and one without them travels as before.
#[test]
fn a_launch_with_mcp_proxies_needs_protocol_11_and_carries_names_only() {
    let launch =
        |mcp_proxies: Vec<McpProxy>, checkout: Option<Box<PreparedCheckout>>| LaunchCommand {
            agent_id: AgentId::from("a1"),
            workspace_id: WorkspaceId::from("ws-1"),
            provider: "codex".into(),
            model: None,
            base_commit: "base".into(),
            repo_key: "repo".into(),
            agent_features: json!({}),
            is_sub_agent: false,
            profile_env: None,
            checkout,
            mcp_proxies,
            compact_tool_descriptions: false,
            checkout_of: None,
            forge: None,
            forge_fetch: None,
            git_identity: None,
            new_machine: None,
            resumed_from_disk: false,
            harness_version: None,
            labels: Box::default(),
        };
    let proxy = McpProxy {
        name: "Linear".into(),
        server_id: "srv-1".into(),
    };
    let with = launch(vec![proxy.clone()], Some(Box::default()));
    assert_eq!(
        LinkFrame::Launch(with.clone()).min_protocol(),
        MCP_PROXY_PROTOCOL
    );
    assert_eq!(
        LinkFrame::Launch(launch(Vec::new(), None)).min_protocol(),
        NODE_LINK_PROTOCOLS.min
    );
    let wire = serde_json::to_value(&with).unwrap();
    assert_eq!(
        wire["mcpProxies"],
        json!([{ "name": "Linear", "serverId": "srv-1" }])
    );
    let bare = serde_json::to_value(launch(Vec::new(), None)).unwrap();
    assert!(bare.get("mcpProxies").is_none(), "{bare}");
    assert!(bare.get("compactToolDescriptions").is_none(), "{bare}");
    assert!(serde_json::from_value::<McpProxy>(
        json!({ "name": "x", "serverId": "y", "command": "leak" })
    )
    .is_err());
    let decoded: LaunchCommand = serde_json::from_value(bare).unwrap();
    assert!(decoded.mcp_proxies.is_empty());
    assert!(!decoded.compact_tool_descriptions);
}

/// `harnessVersion` rides every launch on any protocol: present as the
/// session's stamp, absent from an older backend's launch (which decodes
/// to none), and no protocol gate.
#[test]
fn a_launch_carries_the_harness_version_on_any_protocol() {
    let launch = |harness_version: Option<Box<str>>| LaunchCommand {
        agent_id: AgentId::from("a1"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: false,
        profile_env: None,
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version,
        labels: Box::default(),
    };
    let with = launch(Some("7.42".into()));
    assert_eq!(
        LinkFrame::Launch(with.clone()).min_protocol(),
        NODE_LINK_PROTOCOLS.min
    );
    let wire = serde_json::to_value(&with).unwrap();
    assert_eq!(wire["harnessVersion"], json!("7.42"));
    let decoded: LaunchCommand = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded.harness_version.as_deref(), Some("7.42"));
    let bare = serde_json::to_value(launch(None)).unwrap();
    assert!(bare.get("harnessVersion").is_none(), "{bare}");
    let decoded: LaunchCommand = serde_json::from_value(bare).unwrap();
    assert!(decoded.harness_version.is_none());
}

/// A machine below nodeProtocol 13 rejects a launch into another agent's
/// checkout: it would ignore `checkoutOf` and make a checkout of its own.
#[test]
fn a_launch_into_another_agents_checkout_needs_protocol_13() {
    let mut command = LaunchCommand {
        agent_id: AgentId::from("child"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: true,
        profile_env: None,
        checkout: None,
        checkout_of: Some(AgentId::from("parent").into()),
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::default(),
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
    };
    let wire = serde_json::to_value(LinkFrame::Launch(command.clone())).unwrap();
    assert_eq!(wire["checkoutOf"], "parent", "{wire}");
    assert_eq!(
        LinkFrame::Launch(command.clone()).min_protocol(),
        SHARED_CHECKOUT_PROTOCOL
    );
    command.checkout_of = None;
    let wire = serde_json::to_value(LinkFrame::Launch(command)).unwrap();
    assert!(wire.get("checkoutOf").is_none(), "{wire}");
    assert!(NODE_LINK_PROTOCOLS.contains(SHARED_CHECKOUT_PROTOCOL));
}

/// `pushWip` and its reply are closed bodies with the documented wire names.
#[test]
fn push_wip_frames_have_closed_bodies() {
    let reply = LinkFrame::decode(
        json!({ "type": "pushWipReply", "requestId": "6f9619ff-8b86-d011-b42d-00c04fc964ff", "agentId": "a1", "outcome": "pushed" })
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    assert!(
        matches!(
            &reply,
            LinkFrame::PushWipReply(PushWipResult {
                outcome: FinalPush::Pushed,
                error: None,
                ..
            })
        ),
        "{reply:?}"
    );
    for raw in [
        json!({ "type": "pushWip", "requestId": "6f9619ff-8b86-d011-b42d-00c04fc964ff", "agentId": "a1", "extra": 1 }),
        json!({ "type": "pushWip", "agentId": "a1" }),
        json!({ "type": "pushWipReply", "requestId": "6f9619ff-8b86-d011-b42d-00c04fc964ff", "agentId": "a1", "outcome": "maybe" }),
    ] {
        assert!(
            LinkFrame::decode(raw.to_string().as_bytes()).is_err(),
            "{raw}"
        );
    }
}

/// The secret frames (`node-link.md` § Secrets on the machine) have the
/// documented wire names and closed bodies.
#[test]
fn secret_frames_have_the_documented_shape_and_closed_bodies() {
    let id = LinkId::new_v4();
    let put = LinkFrame::SecretPut(SecretPut {
        request_id: id,
        agent_id: AgentId::from("a1"),
        name: "API_KEY".into(),
        value: SecretValue::from("v-1"),
    });
    assert_eq!(
        serde_json::to_value(&put).unwrap(),
        json!({ "type": "secretPut", "requestId": id, "agentId": "a1", "name": "API_KEY", "value": "v-1" })
    );
    let forget_all = LinkFrame::SecretForget(SecretForget {
        request_id: id,
        agent_id: AgentId::from("a1"),
        name: None,
    });
    assert_eq!(
        serde_json::to_value(&forget_all).unwrap(),
        json!({ "type": "secretForget", "requestId": id, "agentId": "a1" })
    );
    let reply = LinkFrame::decode(
        json!({ "type": "secretReply", "requestId": id, "agentId": "a1", "outcome": "failed", "error": "agent-not-on-lease" })
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    assert!(
        matches!(
            &reply,
            LinkFrame::SecretReply(SecretReply {
                outcome: SecretOutcome::Failed,
                error: Some(error),
                names: None,
                ..
            }) if error == "agent-not-on-lease"
        ),
        "{reply:?}"
    );
    for raw in [
        json!({ "type": "secretPut", "requestId": id, "agentId": "a1", "name": "K", "value": "v", "extra": 1 }),
        json!({ "type": "secretPut", "requestId": id, "agentId": "a1", "name": "K" }),
        json!({ "type": "secretList", "requestId": id, "agentId": "a1", "name": "K" }),
        json!({ "type": "secretForget", "agentId": "a1" }),
        json!({ "type": "secretReply", "requestId": id, "agentId": "a1", "outcome": "maybe" }),
    ] {
        assert!(
            LinkFrame::decode(raw.to_string().as_bytes()).is_err(),
            "{raw}"
        );
    }
}

/// A secret value never shows in `Debug` output, nor in the error for a
/// malformed secret frame.
#[test]
fn secret_values_never_render_in_debug_or_decode_errors() {
    let frame = LinkFrame::SecretPut(SecretPut {
        request_id: LinkId::new_v4(),
        agent_id: AgentId::from("a1"),
        name: "API_KEY".into(),
        value: SecretValue::from("canary-agent-secret"),
    });
    let rendered = format!("{frame:?}");
    assert!(!rendered.contains("canary-agent-secret"), "{rendered}");
    assert!(rendered.contains("API_KEY"), "{rendered}");
    for raw in [
        json!({ "type": "secretPut", "requestId": "canary-agent-secret", "agentId": "a1", "name": "K", "value": "canary-agent-secret" }),
        json!({ "type": "secretPut", "requestId": LinkId::new_v4(), "agentId": "a1", "name": "K", "value": "v", "canary-agent-secret": 1 }),
        json!({ "type": "secretPut", "requestId": LinkId::new_v4(), "agentId": "a1", "name": 1, "value": ["canary-agent-secret"] }),
        json!({ "type": "secretPut", "requestId": LinkId::new_v4(), "agentId": "a1", "name": "canary-agent-secret" }),
    ] {
        let Err(error) = LinkFrame::decode(raw.to_string().as_bytes()) else {
            panic!("decoded {raw}");
        };
        let message = error.to_string();
        assert!(!message.contains("canary-agent-secret"), "{message}");
        assert!(message.contains("node-link-frame-invalid"), "{message}");
    }
}

/// `hello.headSession` (protocol 14) is optional on the wire: an older
/// backend's hello still decodes, without one.
#[test]
fn hello_head_session_is_optional() {
    let mut hello = LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: "0.12.0".into(),
        binding: binding("head", 3),
        acked_seq: NodeCounter(0),
        lease_agents: Vec::new(),
        head_session: None,
        lease_homes: None,
    };
    let value = serde_json::to_value(LinkFrame::Hello(hello.clone())).unwrap();
    assert!(value.get("headSession").is_none(), "{value}");
    hello.head_session = Some("hs-1".into());
    let value = serde_json::to_value(LinkFrame::Hello(hello.clone())).unwrap();
    assert_eq!(value["headSession"], "hs-1");
    assert_eq!(
        LinkFrame::decode(value.to_string().as_bytes()).unwrap(),
        LinkFrame::Hello(hello)
    );
}

/// The forge token frames (`node-link.md` § Forge token on the machine)
/// have the documented wire names and closed bodies; `token: null` drops
/// the held value.
#[test]
fn forge_token_frames_have_the_documented_shape_and_closed_bodies() {
    let id = LinkId::new_v4();
    let put = LinkFrame::ForgeToken(ForgeToken {
        request_id: id,
        host: "github.com".into(),
        token: Some(SecretValue::from("t-1")),
    });
    assert_eq!(
        serde_json::to_value(&put).unwrap(),
        json!({ "type": "forgeToken", "requestId": id, "host": "github.com", "token": "t-1" })
    );
    let drop = LinkFrame::decode(
        json!({ "type": "forgeToken", "requestId": id, "host": "github.com", "token": null })
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    assert!(
        matches!(&drop, LinkFrame::ForgeToken(ForgeToken { token: None, .. })),
        "{drop:?}"
    );
    let reply = LinkFrame::ForgeTokenReply(ForgeTokenReply {
        request_id: id,
        outcome: SecretOutcome::Ok,
        error: None,
    });
    assert_eq!(
        serde_json::to_value(&reply).unwrap(),
        json!({ "type": "forgeTokenReply", "requestId": id, "outcome": "ok" })
    );
    for raw in [
        json!({ "type": "forgeToken", "requestId": id, "host": "github.com", "token": "t", "extra": 1 }),
        json!({ "type": "forgeToken", "requestId": id, "token": "t" }),
        json!({ "type": "forgeTokenReply", "requestId": id, "outcome": "ok", "token": "t" }),
        json!({ "type": "forgeTokenReply", "requestId": id, "outcome": "maybe" }),
    ] {
        assert!(
            LinkFrame::decode(raw.to_string().as_bytes()).is_err(),
            "{raw}"
        );
    }
}

/// The forge token never shows in `Debug` output, nor in the error for a
/// malformed frame.
#[test]
fn forge_tokens_never_render_in_debug_or_decode_errors() {
    let frame = LinkFrame::ForgeToken(ForgeToken {
        request_id: LinkId::new_v4(),
        host: "github.com".into(),
        token: Some(SecretValue::from("canary-forge-token")),
    });
    let rendered = format!("{frame:?}");
    assert!(!rendered.contains("canary-forge-token"), "{rendered}");
    for raw in [
        json!({ "type": "forgeToken", "requestId": "canary-forge-token", "host": "github.com", "token": "canary-forge-token" }),
        json!({ "type": "forgeToken", "requestId": LinkId::new_v4(), "host": "github.com", "token": ["canary-forge-token"] }),
        json!({ "type": "forgeToken", "requestId": LinkId::new_v4(), "host": "github.com", "token": "canary-forge-token", "canary-forge-token": 1 }),
        json!({ "type": "nope", "value": "canary-forge-token" }),
    ] {
        let Err(error) = LinkFrame::decode(raw.to_string().as_bytes()) else {
            panic!("decoded {raw}");
        };
        let message = error.to_string();
        assert!(!message.contains("canary-forge-token"), "{message}");
        assert!(message.contains("node-link-frame-invalid"), "{message}");
    }
}

/// A launch's `forge` member (protocol 15) is optional and closed, and
/// raises the launch's protocol only when present.
#[test]
fn launch_forge_fetch_needs_protocol_17_and_is_closed() {
    let LinkFrame::Launch(mut command) = every_frame()
        .into_iter()
        .find(|frame| matches!(frame, LinkFrame::Launch(c) if c.checkout.is_none() && c.mcp_proxies.is_empty() && c.checkout_of.is_none()))
        .unwrap()
    else {
        unreachable!()
    };
    command.is_sub_agent = true;
    command.forge_fetch = Some(Box::new(
        ForgeFetchLaunch::for_github("git@github.com:o/r.git").unwrap(),
    ));
    let frame = LinkFrame::Launch(command);
    let mut value = serde_json::to_value(&frame).unwrap();
    assert_eq!(
        value["forgeFetch"],
        json!({ "remoteUrl": "https://github.com/o/r.git" })
    );
    assert_eq!(frame.min_protocol(), FORGE_FETCH_PROTOCOL);
    // A sharing sub-agent's launch needs 17 too, not just the 13 of
    // `checkoutOf`.
    let LinkFrame::Launch(mut sharing) = frame.clone() else {
        unreachable!()
    };
    sharing.checkout_of = Some(AgentId::from("agent-owner").into());
    assert_eq!(
        LinkFrame::Launch(sharing).min_protocol(),
        FORGE_FETCH_PROTOCOL
    );
    assert_eq!(
        LinkFrame::decode(value.to_string().as_bytes()).unwrap(),
        frame
    );
    value["forgeFetch"]["extra"] = json!(1);
    assert!(LinkFrame::decode(value.to_string().as_bytes()).is_err());
    assert!(ForgeFetchLaunch::for_github("https://gitlab.com/o/r.git").is_none());
    let bad = ForgeFetchLaunch {
        remote_url: "https://github.com/o/r".into(),
    };
    assert!(bad.owner_repo().is_none());
}

#[test]
fn launch_forge_is_optional_and_needs_protocol_15() {
    let LinkFrame::Launch(mut command) = every_frame()
        .into_iter()
        .find(|frame| matches!(frame, LinkFrame::Launch(c) if c.checkout.is_none() && c.mcp_proxies.is_empty() && c.checkout_of.is_none()))
        .unwrap()
    else {
        unreachable!()
    };
    let plain = LinkFrame::Launch(command.clone());
    assert!(serde_json::to_value(&plain).unwrap().get("forge").is_none());
    assert!(plain.min_protocol() < FORGE_TOKEN_PROTOCOL);
    command.forge = Some(Box::new(ForgeLaunch {
        remote_url: "https://github.com/o/r.git".into(),
        branch: "feat/x".into(),
    }));
    let forged = LinkFrame::Launch(command);
    let value = serde_json::to_value(&forged).unwrap();
    assert_eq!(
        value["forge"],
        json!({ "remoteUrl": "https://github.com/o/r.git", "branch": "feat/x" })
    );
    assert_eq!(forged.min_protocol(), FORGE_TOKEN_PROTOCOL);
    assert_eq!(
        LinkFrame::decode(value.to_string().as_bytes()).unwrap(),
        forged
    );
    let mut open = value;
    open["forge"]["extra"] = json!(1);
    assert!(LinkFrame::decode(open.to_string().as_bytes()).is_err());
}

#[test]
fn forge_launch_normalizes_github_origins_and_refuses_the_rest() {
    for origin in [
        "git@github.com:acme/widget.git",
        "https://x-access-token@github.com/acme/widget",
        "ssh://git@github.com/acme/widget.git",
    ] {
        assert_eq!(
            ForgeLaunch::for_github(origin, "feat/x"),
            Some(ForgeLaunch {
                remote_url: "https://github.com/acme/widget.git".into(),
                branch: "feat/x".into(),
            }),
            "{origin}"
        );
    }
    for origin in [
        "https://gitlab.com/acme/widget.git",
        "/srv/widget.git",
        "https://example.invalid/a@github.com/acme/widget.git",
    ] {
        assert_eq!(ForgeLaunch::for_github(origin, "main"), None, "{origin}");
    }
    for branch in [
        "", "@", "-x", "a..b", "a b", "a:b", "x.lock", "a/.b", "a//b", "a/", "a@{1}", "a\u{7f}",
    ] {
        assert_eq!(
            ForgeLaunch::for_github("git@github.com:acme/widget.git", branch),
            None,
            "{branch:?}"
        );
    }
    let bad_url = ForgeLaunch {
        remote_url: "https://github.com/acme/widget.git/../x.git".into(),
        branch: "main".into(),
    };
    assert!(!bad_url.is_valid());
}

/// `launch.gitIdentity` is optional on the open launch body: absent from the
/// wire when no identity resolved, a closed `{ name, email }` when sent, and
/// ignored by a machine that does not know it (as an unknown member is).
#[test]
fn a_launchs_git_identity_is_optional_and_closed() {
    let mut command = LaunchCommand {
        agent_id: AgentId::from("a1"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: false,
        profile_env: None,
        checkout: None,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::default(),
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
    };
    let bare = serde_json::to_value(&command).unwrap();
    assert!(bare.get("gitIdentity").is_none(), "{bare}");

    command.git_identity = Some(Box::new(GitIdentity {
        name: "Ada".into(),
        email: "ada@example.invalid".into(),
    }));
    let wire = serde_json::to_value(&command).unwrap();
    assert_eq!(
        wire["gitIdentity"],
        json!({ "name": "Ada", "email": "ada@example.invalid" })
    );
    assert_eq!(
        LinkFrame::Launch(command.clone()).min_protocol(),
        LinkFrame::Launch(LaunchCommand {
            git_identity: None,
            new_machine: None,
            resumed_from_disk: false,
            harness_version: None,
            labels: Box::default(),
            ..command.clone()
        })
        .min_protocol()
    );
    let decoded: LaunchCommand = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(decoded, command);

    let mut extra = wire.clone();
    extra["gitIdentity"]["signingKey"] = json!("k");
    assert!(serde_json::from_value::<LaunchCommand>(extra).is_err());

    // An older machine meets the member as an unknown one and starts as
    // before.
    let mut older = bare;
    older["laterMember"] = wire["gitIdentity"].clone();
    let decoded: LaunchCommand = serde_json::from_value(older).unwrap();
    assert_eq!(decoded.git_identity, None);

    let empty = GitIdentity {
        name: String::new(),
        email: "ada@example.invalid".into(),
    };
    assert_eq!(empty.usable(), None);
    assert_eq!(
        command.git_identity.as_ref().unwrap().usable(),
        Some(("Ada", "ada@example.invalid"))
    );
}

fn labelled_launch(labels: LaunchLabels) -> LaunchCommand {
    LaunchCommand {
        agent_id: AgentId::from("a1"),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        agent_features: json!({}),
        is_sub_agent: false,
        profile_env: None,
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::new(labels),
    }
}

/// The display labels ride every launch at the top level of the body as
/// `agentName` / `workspaceTitle`, need no protocol, and an older
/// backend's launch (no labels) decodes to none and sends no keys.
#[test]
fn a_launch_carries_display_labels_on_any_protocol() {
    let with = labelled_launch(LaunchLabels::new(Some("Fix login"), Some("Auth work")));
    assert_eq!(
        LinkFrame::Launch(with.clone()).min_protocol(),
        NODE_LINK_PROTOCOLS.min
    );
    let wire = serde_json::to_value(LinkFrame::Launch(with)).unwrap();
    assert_eq!(wire["agentName"], json!("Fix login"), "{wire}");
    assert_eq!(wire["workspaceTitle"], json!("Auth work"), "{wire}");
    let LinkFrame::Launch(decoded) = serde_json::from_value(wire).unwrap() else {
        panic!("not a launch");
    };
    assert_eq!(decoded.labels.agent_name.as_deref(), Some("Fix login"));
    assert_eq!(decoded.labels.workspace_title.as_deref(), Some("Auth work"));

    let bare = serde_json::to_value(labelled_launch(LaunchLabels::default())).unwrap();
    assert!(bare.get("agentName").is_none(), "{bare}");
    assert!(bare.get("workspaceTitle").is_none(), "{bare}");
    let decoded: LaunchCommand = serde_json::from_value(bare).unwrap();
    assert_eq!(*decoded.labels, LaunchLabels::default());
}

/// The machine treats a label that is not a string, is blank or is over
/// the byte cap as absent, and still accepts the launch.
#[test]
fn a_bad_display_label_counts_as_absent_and_keeps_the_launch() {
    let mut wire = serde_json::to_value(labelled_launch(LaunchLabels::default())).unwrap();
    for bad in [
        json!(42),
        json!(null),
        json!({ "name": "x" }),
        json!("   "),
        json!("x".repeat(LaunchLabels::MAX_BYTES + 1)),
    ] {
        wire["agentName"] = bad.clone();
        wire["workspaceTitle"] = bad.clone();
        let decoded: LaunchCommand = serde_json::from_value(wire.clone())
            .unwrap_or_else(|error| panic!("{bad} refused the launch: {error}"));
        assert_eq!(*decoded.labels, LaunchLabels::default(), "{bad}");
    }
    wire["agentName"] = json!("x".repeat(LaunchLabels::MAX_BYTES));
    let decoded: LaunchCommand = serde_json::from_value(wire).unwrap();
    assert_eq!(
        decoded.labels.agent_name.as_deref().map(str::len),
        Some(LaunchLabels::MAX_BYTES)
    );
}

/// The backend trims each label, drops a blank one and cuts a long one at
/// a character boundary at or below the cap, so the machine never drops a
/// label the backend meant to send.
#[test]
fn the_backend_trims_and_cuts_display_labels() {
    let labels = LaunchLabels::new(Some("  Fix login \n"), Some("   "));
    assert_eq!(labels.agent_name.as_deref(), Some("Fix login"));
    assert_eq!(labels.workspace_title, None);

    let long = format!("{}é", "a".repeat(LaunchLabels::MAX_BYTES - 1));
    let labels = LaunchLabels::new(None, Some(&long));
    assert_eq!(labels.agent_name, None);
    let title = labels.workspace_title.clone().unwrap();
    assert_eq!(&*title, "a".repeat(LaunchLabels::MAX_BYTES - 1));
    let wire = serde_json::to_value(labelled_launch(labels)).unwrap();
    let decoded: LaunchCommand = serde_json::from_value(wire).unwrap();
    assert_eq!(
        decoded.labels.workspace_title,
        Some(title),
        "a cut label survives the machine's check"
    );
}

/// A `oneShot` whose input does not match its operation, or with a mistyped
/// mode, does not decode; the timeout is clamped by the machine, not here.
#[test]
fn one_shot_input_must_match_its_operation() {
    let frame = |operation: &str, input: Value| {
        json!({
            "type": "oneShot",
            "requestId": "00000000-0000-0000-0000-000000000001",
            "operation": operation,
            "input": input,
            "timeoutMs": 999_999_999u64,
        })
    };
    let enhance = json!({ "prompt": "p", "mode": "layout" });
    let complete = json!({ "providerId": "codex", "prompt": "p" });
    let decode = |value: Value| LinkFrame::decode(&serde_json::to_vec(&value).unwrap());
    assert!(decode(frame("enhance", enhance.clone())).is_ok());
    assert!(decode(frame("complete", complete.clone())).is_ok());
    assert!(decode(frame("complete", enhance)).is_err());
    assert!(decode(frame("enhance", complete)).is_err());
    assert!(decode(frame("enhance", json!({ "prompt": "p", "mode": "loud" }))).is_err());
    assert!(decode(frame(
        "complete",
        json!({ "providerId": "codex", "prompt": "p", "extra": 1 })
    ))
    .is_err());
}

#[test]
fn script_frames_name_an_agent_or_a_home_and_a_home_needs_protocol_19() {
    let home = HomeRef {
        workspace_id: WorkspaceId::from("ws-1"),
        home_id: HomeId::from("home-1"),
    };
    let start = ScriptStartCommand {
        owner: CheckoutOwner::Home(home.clone()),
        script_id: "s1".into(),
        spawn_id: "p1".into(),
        command: "echo hi".into(),
        mode: crate::ScriptMode::Command,
        cwd: Some("web".into()),
        env: Vec::new(),
    };
    let wire = serde_json::to_value(LinkFrame::ScriptStart(start.clone())).unwrap();
    assert_eq!(wire["workspaceId"], "ws-1");
    assert_eq!(wire["homeId"], "home-1");
    assert!(wire.get("agentId").is_none());
    assert_eq!(
        serde_json::from_value::<LinkFrame>(wire.clone()).unwrap(),
        LinkFrame::ScriptStart(start.clone())
    );
    assert_eq!(
        LinkFrame::ScriptStart(start.clone()).min_protocol(),
        HOMES_PROTOCOL
    );

    // Exactly one owner: both, neither, or half a home are refused.
    let mut both = wire.clone();
    both["agentId"] = json!("a1");
    assert!(serde_json::from_value::<LinkFrame>(both).is_err());
    let mut neither = wire.clone();
    let body = neither.as_object_mut().unwrap();
    body.remove("workspaceId");
    body.remove("homeId");
    assert!(serde_json::from_value::<LinkFrame>(neither).is_err());
    let mut half = wire.clone();
    half.as_object_mut().unwrap().remove("homeId");
    assert!(serde_json::from_value::<LinkFrame>(half).is_err());
    // The body stays closed.
    let mut extra = wire;
    extra["extra"] = json!(1);
    assert!(serde_json::from_value::<LinkFrame>(extra).is_err());

    let owner = CheckoutOwner::Home(home);
    let frames = [
        LinkFrame::ScriptStop(ScriptProcessRef {
            owner: owner.clone(),
            script_id: "s1".into(),
            spawn_id: "p1".into(),
        }),
        LinkFrame::ScriptStarted(ScriptStarted {
            owner: owner.clone(),
            script_id: "s1".into(),
            spawn_id: "p1".into(),
            pid: 7,
        }),
        LinkFrame::ScriptOutput(ScriptOutput {
            owner: owner.clone(),
            script_id: "s1".into(),
            spawn_id: "p1".into(),
            seq: 0,
            data: "hi\n".into(),
        }),
        LinkFrame::ScriptExited(ScriptExited {
            owner: owner.clone(),
            script_id: "s1".into(),
            spawn_id: "p1".into(),
            exit_code: Some(0),
            error: None,
        }),
    ];
    for frame in frames {
        assert_eq!(frame.script_owner(), Some(&owner));
        assert_eq!(frame.min_protocol(), HOMES_PROTOCOL);
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(serde_json::from_value::<LinkFrame>(wire).unwrap(), frame);
    }

    // An agent's run keeps its wire shape and protocol.
    let agent_start = ScriptStartCommand {
        owner: AgentId::from("a1").into(),
        ..start
    };
    let wire = serde_json::to_value(LinkFrame::ScriptStart(agent_start.clone())).unwrap();
    assert_eq!(wire["agentId"], "a1");
    assert!(wire.get("homeId").is_none());
    assert_eq!(
        LinkFrame::ScriptStart(agent_start).min_protocol(),
        SCRIPT_PROTOCOL
    );
}
