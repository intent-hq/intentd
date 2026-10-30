use serde::Deserialize;
use serde_json::json;

use super::*;

/// The `placementCases` of monorepo
/// `docs/protocol/fixtures/nodes/platform-routing.json`, copied verbatim
/// (`invalid-json` is not a placement case: it fails as JSON).
const FIXTURE_CASES: &[(&str, &str, bool)] = &[
    ("arch-only", r#"{"arch":"x86_64"}"#, true),
    ("os-only", r#"{"os":"linux"}"#, true),
    ("both", r#"{"os":"macos","arch":"aarch64"}"#, true),
    ("empty", "{}", true),
    (
        "old-complete",
        r#"{"target":"remote","checkout":"isolated","os":"linux","nodeId":"node-1","exclusive":true}"#,
        true,
    ),
    (
        "local-shared",
        r#"{"target":"local","checkout":"shared"}"#,
        true,
    ),
    ("null", "null", false),
    ("array", "[]", false),
    ("unknown", r#"{"platform":"linux"}"#, false),
    ("bad-arch", r#"{"arch":"amd64"}"#, false),
    ("unimplemented-os", r#"{"os":"windows"}"#, false),
    ("null-arch", r#"{"arch":null}"#, false),
    (
        "remote-shared",
        r#"{"target":"remote","checkout":"shared"}"#,
        false,
    ),
    (
        "exclusive-local",
        r#"{"target":"local","exclusive":true}"#,
        false,
    ),
    ("pool-as-location", r#"{"target":"static-builders"}"#, false),
    ("empty-node", r#"{"nodeId":""}"#, false),
    ("duplicate", r#"{"arch":"x86_64","arch":"aarch64"}"#, false),
    (
        "escaped-duplicate",
        r#"{"arch":"x86_64","arch":"aarch64"}"#,
        false,
    ),
    ("invalid-json", r#"{"arch":"x86_64",}"#, false),
];

#[test]
fn placement_parsing_matches_the_protocol_fixture() {
    for (id, raw, valid) in FIXTURE_CASES {
        let parsed = serde_json::from_str::<AgentPlacement>(raw);
        assert_eq!(parsed.is_ok(), *valid, "{id}: {raw} -> {parsed:?}");
    }
}

#[test]
fn more_invalid_placements_reject() {
    for raw in [
        r#"{"exclusive":null}"#,
        r#"{"arch":1}"#,
        r#"{"exclusive":true,"checkout":"worktree"}"#,
        r#"{"target":"remote","checkout":"worktree"}"#,
        r#"{"os":"linux","os":"macos"}"#,
    ] {
        assert!(
            serde_json::from_str::<AgentPlacement>(raw).is_err(),
            "{raw}"
        );
    }
    let longest = json!({ "nodeId": "é".repeat(64) });
    assert!(serde_json::from_value::<AgentPlacement>(longest).is_ok());
    let oversized = json!({ "nodeId": "é".repeat(65) });
    assert!(serde_json::from_value::<AgentPlacement>(oversized).is_err());
}

#[test]
fn valid_placements_round_trip_unchanged() {
    for (id, raw, valid) in FIXTURE_CASES {
        if !valid {
            continue;
        }
        let parsed: AgentPlacement = serde_json::from_str(raw).unwrap();
        let reparsed: AgentPlacement =
            serde_json::from_str(&serde_json::to_string(&parsed).unwrap()).unwrap();
        assert_eq!(parsed, reparsed, "{id}");
        assert_eq!(
            serde_json::to_value(&parsed).unwrap(),
            serde_json::from_str::<serde_json::Value>(raw).unwrap(),
            "{id}"
        );
    }
}

#[test]
fn absent_checkout_means_isolated() {
    let placement: AgentPlacement = serde_json::from_str(r#"{"target":"remote"}"#).unwrap();
    assert_eq!(placement.effective_checkout(), PlacementCheckout::Isolated);
    assert_eq!(placement.checkout, None, "the request is kept as sent");
}

#[test]
fn an_omitted_placement_differs_from_an_empty_one_and_null_rejects() {
    #[derive(Deserialize)]
    struct Input {
        #[serde(default, deserialize_with = "present")]
        placement: Option<AgentPlacement>,
    }
    let parse = |raw: &str| serde_json::from_str::<Input>(raw).map(|input| input.placement);
    assert_eq!(parse("{}").unwrap(), None);
    assert_eq!(
        parse(r#"{"placement":{}}"#).unwrap(),
        Some(AgentPlacement::default())
    );
    assert!(parse(r#"{"placement":null}"#).is_err());
}

#[test]
fn os_and_arch_match_independently() {
    let linux_x86 = AgentPlatform {
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
    };
    let matches = |raw: &str| {
        serde_json::from_str::<AgentPlacement>(raw)
            .unwrap()
            .matches_platform(linux_x86)
    };
    assert!(matches("{}"));
    assert!(matches(r#"{"os":"linux"}"#));
    assert!(matches(r#"{"arch":"x86_64"}"#));
    assert!(matches(r#"{"os":"linux","arch":"x86_64"}"#));
    assert!(!matches(r#"{"os":"macos"}"#));
    assert!(!matches(r#"{"arch":"aarch64"}"#));
    assert!(!matches(r#"{"os":"macos","arch":"x86_64"}"#));
}

#[test]
fn remote_launch_supports_the_documented_subset_only() {
    for raw in [
        "{}",
        r#"{"target":"remote"}"#,
        r#"{"target":"remote","checkout":"isolated","os":"linux","arch":"x86_64","nodeId":"node-1"}"#,
        r#"{"target":"remote","exclusive":false}"#,
    ] {
        let placement: AgentPlacement = serde_json::from_str(raw).unwrap();
        placement
            .check_remote_supported()
            .unwrap_or_else(|e| panic!("{raw}: {e}"));
    }
    let exclusive: AgentPlacement =
        serde_json::from_str(r#"{"target":"remote","exclusive":true}"#).unwrap();
    let err = exclusive.check_remote_supported().unwrap_err();
    assert!(
        matches!(&err, Error::InvalidParams(m) if m.contains("placement.exclusive")),
        "{err:?}"
    );
    assert_eq!(err.code(), -32602);
    let local: AgentPlacement =
        serde_json::from_str(r#"{"target":"local","checkout":"shared"}"#).unwrap();
    assert!(matches!(
        local.check_remote_supported(),
        Err(Error::InvalidParams(m)) if m.contains("target")
    ));
    for raw in [r#"{"checkout":"shared"}"#, r#"{"checkout":"worktree"}"#] {
        let placement: AgentPlacement = serde_json::from_str(raw).unwrap();
        placement
            .validate()
            .unwrap_or_else(|e| panic!("{raw}: {e}"));
        let err = placement.check_remote_supported().unwrap_err();
        assert!(
            matches!(&err, Error::InvalidParams(m) if m.contains("isolated checkout")),
            "{raw}: {err:?}"
        );
    }
    let built = AgentPlacement {
        node_id: Some(String::new()),
        ..AgentPlacement::default()
    };
    assert!(
        built.check_remote_supported().is_err(),
        "placements built in Rust are validated too"
    );
}

fn platform(os: NodeOs, arch: NodeArch) -> AgentPlatform {
    AgentPlatform { os, arch }
}

/// The two `errors` of the protocol fixture, rebuilt from typed data.
#[test]
fn unavailable_errors_match_the_protocol_fixture() {
    let linux_x86 = platform(NodeOs::Linux, NodeArch::X86_64);
    let observed = "2026-10-01T00:00:00Z".to_string();

    let no_capacity = PlatformAvailability::new([linux_x86], [], observed.clone())
        .unavailable(serde_json::from_str(r#"{"arch":"x86_64"}"#).unwrap());
    assert_eq!(
        no_capacity.message(),
        "No capacity for x86_64; available: none; supported: linux/x86_64"
    );
    assert_eq!(
        no_capacity.data(),
        json!({
            "code": "placement-unavailable",
            "requested": { "arch": "x86_64" },
            "reason": "no-capacity",
            "availablePlatforms": [],
            "supportedPlatforms": [{ "os": "linux", "arch": "x86_64" }],
            "availableOs": [],
            "retryable": true,
            "observedAt": "2026-10-01T00:00:00Z"
        })
    );

    let unsupported = PlatformAvailability::new([linux_x86], [linux_x86], observed)
        .unavailable(serde_json::from_str(r#"{"os":"macos","arch":"x86_64"}"#).unwrap());
    assert_eq!(
        unsupported.message(),
        "Unsupported macos/x86_64; available: linux/x86_64; supported: linux/x86_64"
    );
    assert_eq!(
        unsupported.data(),
        json!({
            "code": "placement-unavailable",
            "requested": { "os": "macos", "arch": "x86_64" },
            "reason": "unsupported-platform",
            "availablePlatforms": [{ "os": "linux", "arch": "x86_64" }],
            "supportedPlatforms": [{ "os": "linux", "arch": "x86_64" }],
            "availableOs": ["linux"],
            "retryable": false,
            "observedAt": "2026-10-01T00:00:00Z"
        })
    );
}

#[test]
fn availability_sorts_dedupes_and_projects_available_os() {
    let availability = PlatformAvailability::new(
        [
            platform(NodeOs::Macos, NodeArch::Aarch64),
            platform(NodeOs::Linux, NodeArch::X86_64),
            platform(NodeOs::Linux, NodeArch::Aarch64),
        ],
        [
            platform(NodeOs::Macos, NodeArch::Aarch64),
            platform(NodeOs::Linux, NodeArch::X86_64),
            platform(NodeOs::Linux, NodeArch::Aarch64),
            platform(NodeOs::Linux, NodeArch::X86_64),
        ],
        "t".into(),
    );
    assert_eq!(
        availability.available_platforms,
        [
            platform(NodeOs::Linux, NodeArch::Aarch64),
            platform(NodeOs::Linux, NodeArch::X86_64),
            platform(NodeOs::Macos, NodeArch::Aarch64),
        ]
    );
    assert_eq!(availability.supported_platforms.len(), 3);
    assert_eq!(availability.available_os, [NodeOs::Linux, NodeOs::Macos]);
    let failure = availability.unavailable(AgentPlacement::default());
    assert_eq!(failure.reason, PlacementUnavailableReason::NoCapacity);
    assert!(failure.message().starts_with(
        "No capacity for any platform; available: linux/aarch64, linux/x86_64, macos/aarch64;"
    ));
}

#[test]
fn with_no_node_every_request_is_an_unsupported_platform() {
    let failure = PlatformAvailability::none("t".into()).unavailable(AgentPlacement::default());
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );
    assert!(!failure.reason.retryable());
    assert_eq!(
        failure.message(),
        "Unsupported any platform; available: none; supported: none"
    );
}

#[test]
fn the_error_text_keeps_the_typed_data_for_mcp() {
    let failure =
        PlatformAvailability::new([platform(NodeOs::Linux, NodeArch::X86_64)], [], "t".into())
            .unavailable(serde_json::from_str(r#"{"arch":"aarch64"}"#).unwrap());
    let error = Error::PlacementUnavailable(Box::new(failure));
    assert_eq!(error.code(), -32602);
    let text = error.to_string();
    for expected in [
        "Unsupported aarch64; available: none; supported: linux/x86_64",
        "\"code\":\"placement-unavailable\"",
        "\"requested\":{\"arch\":\"aarch64\"}",
        "\"reason\":\"unsupported-platform\"",
        "\"supportedPlatforms\":[{\"os\":\"linux\",\"arch\":\"x86_64\"}]",
        "\"availablePlatforms\":[]",
        "\"retryable\":false",
    ] {
        assert!(text.contains(expected), "{expected} missing from {text}");
    }
}

#[test]
fn no_capacity_names_the_shared_limit_it_hit() {
    let mac = platform(NodeOs::Macos, NodeArch::Aarch64);
    let availability = || PlatformAvailability::new([mac], [], "t".into());
    let count = availability()
        .unavailable(AgentPlacement::default())
        .with_capacity(CapacityLimit::Count {
            running_agents: 3,
            max_agents: 3,
        });
    assert_eq!(
        count.message(),
        "No capacity for any platform (3 of 3 agents running); available: none; supported: macos/aarch64"
    );
    let data = count.data();
    assert_eq!(data["reason"], "no-capacity");
    assert_eq!(data["limit"], "count");
    assert_eq!(data["runningAgents"], 3);
    assert_eq!(data["maxAgents"], 3);
    assert_eq!(data["retryable"], true);

    let memory = availability()
        .unavailable(AgentPlacement::default())
        .with_capacity(CapacityLimit::Memory {
            charged_bytes: 7 << 30,
            budget_bytes: 8 << 30,
        });
    assert!(memory
        .message()
        .starts_with("No capacity for any platform (7168 of 8192 MiB agent memory in use);"));
    let data = memory.data();
    assert_eq!(data["limit"], "memory");
    assert_eq!(data["chargedBytes"], 7_u64 << 30);
    assert_eq!(data["budgetBytes"], 8_u64 << 30);
    assert_eq!(data["retryable"], true);
    assert!(data.get("machine").is_none());
}

#[test]
fn capacity_limits_round_trip_tagged_by_limit() {
    for (limit, wire) in [
        (
            CapacityLimit::Count {
                running_agents: 1,
                max_agents: 2,
            },
            json!({ "limit": "count", "runningAgents": 1, "maxAgents": 2 }),
        ),
        (
            CapacityLimit::Memory {
                charged_bytes: 5,
                budget_bytes: 9,
            },
            json!({ "limit": "memory", "chargedBytes": 5, "budgetBytes": 9 }),
        ),
    ] {
        assert_eq!(serde_json::to_value(limit).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<CapacityLimit>(wire).unwrap(),
            limit
        );
    }
}

#[test]
fn machine_refusals_name_the_machine_and_say_whether_to_retry() {
    let requested: AgentPlacement =
        serde_json::from_str(r#"{"target":"remote","nodeId":"m-1"}"#).unwrap();
    let machine = MachineRef {
        node_id: "m-1".into(),
        name: "mac-mini".into(),
    };
    for (reason, wire, lead, retryable) in [
        (
            PlacementUnavailableReason::MachineUnreachable,
            "machine-unreachable",
            "Machine \"mac-mini\" is unreachable",
            true,
        ),
        (
            PlacementUnavailableReason::MachineUnknown,
            "machine-unknown",
            "No machine \"mac-mini\" is exposed to this backend",
            false,
        ),
        (
            PlacementUnavailableReason::MachineRevoked,
            "machine-revoked",
            "Machine \"mac-mini\" revoked this backend's access",
            false,
        ),
    ] {
        let failure = PlatformAvailability::none("t".into()).machine_unavailable(
            requested.clone(),
            reason,
            machine.clone(),
        );
        assert_eq!(reason.retryable(), retryable);
        assert_eq!(
            failure.message(),
            format!("{lead}; available: none; supported: none")
        );
        let data = failure.data();
        assert_eq!(data["code"], "placement-unavailable");
        assert_eq!(data["reason"], wire);
        assert_eq!(data["retryable"], retryable);
        assert_eq!(
            data["machine"],
            json!({ "nodeId": "m-1", "name": "mac-mini" })
        );
        assert_eq!(data["requested"]["nodeId"], "m-1");
        assert!(data.get("limit").is_none());
    }
}
