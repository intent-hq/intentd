use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::json;

use super::*;
use crate::ids::ExecutorCredentialId;
use crate::machines::UpdateSide;
use crate::node_link::{MachineCapacity, ProtocolRange};
use crate::nodes::{NodeArch, NodeOs};
use crate::placement::{AgentPlatform, PlacementTarget};

const NOW: &str = "2026-10-31T12:00:00Z";
const FRESH: &str = "2026-10-31T11:30:00Z";
const STALE: &str = "2026-10-31T10:00:00Z";

fn now() -> OffsetDateTime {
    OffsetDateTime::parse(NOW, &Rfc3339).unwrap()
}

const MAC: AgentPlatform = AgentPlatform {
    os: NodeOs::Macos,
    arch: NodeArch::Aarch64,
};
const LINUX: AgentPlatform = AgentPlatform {
    os: NodeOs::Linux,
    arch: NodeArch::X86_64,
};

fn provider(id: &str, status: ProviderStatus, models: &[&str]) -> ProviderCapability {
    ProviderCapability {
        id: id.into(),
        visible: true,
        status,
        models: models.iter().map(|m| (*m).to_owned()).collect(),
        models_stale: false,
    }
}

fn report(
    platform: AgentPlatform,
    observed_at: &str,
    providers: Vec<ProviderCapability>,
) -> MachineCapabilities {
    MachineCapabilities {
        platform,
        intentd_version: "0.11.87".into(),
        capacity: MachineCapacity {
            running_agents: 0,
            agent_cap: 4,
            charged_memory_bytes: 0,
            memory_budget_bytes: None,
        },
        providers,
        observed_at: observed_at.into(),
    }
}

fn machine(id: &str, caps: Option<MachineCapabilities>) -> ExposedMachine {
    ExposedMachine {
        id: id.into(),
        name: format!("{id}-name"),
        hosts: vec!["10.0.0.1".into()],
        port: 7443,
        tc_address: Some("100.64.0.1".into()),
        fingerprint: "ab".repeat(32),
        credential_id: ExecutorCredentialId::new(),
        platform: caps.as_ref().map(|c| c.platform),
        observed_at: caps.as_ref().map(|c| c.observed_at.clone()),
        capabilities: caps,
        state: RemoteMachineState::Disconnected,
        added_at: "2026-10-01T00:00:00Z".into(),
        last_connected_at: None,
    }
}

fn claude(models: &[&str]) -> Vec<ProviderCapability> {
    vec![provider("claude-code", ProviderStatus::SignedIn, models)]
}

/// The machine list plus the refresh outcome per machine (a machine
/// without one fails the test if refreshed).
#[derive(Default)]
struct Fake {
    rows: Vec<ExposedMachine>,
    refreshes: HashMap<String, std::result::Result<MachineCapabilities, RefreshFailure>>,
    refreshed: Mutex<Vec<String>>,
}

impl Fake {
    fn new(rows: Vec<ExposedMachine>) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    fn on_refresh(
        mut self,
        id: &str,
        outcome: std::result::Result<MachineCapabilities, RefreshFailure>,
    ) -> Self {
        self.refreshes.insert(id.into(), outcome);
        self
    }

    fn refreshed(&self) -> Vec<String> {
        self.refreshed.lock().unwrap().clone()
    }
}

impl PlacementMachines for Fake {
    fn machines(&self) -> BoxFuture<'_, Result<Vec<ExposedMachine>>> {
        Box::pin(async move { Ok(self.rows.clone()) })
    }

    fn refresh<'a>(
        &'a self,
        machine_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<MachineCapabilities, RefreshFailure>> {
        Box::pin(async move {
            self.refreshed.lock().unwrap().push(machine_id.to_owned());
            self.refreshes
                .get(machine_id)
                .cloned()
                .unwrap_or_else(|| panic!("unexpected refresh of {machine_id}"))
        })
    }
}

fn remote() -> AgentPlacement {
    AgentPlacement {
        target: Some(PlacementTarget::Remote),
        ..AgentPlacement::default()
    }
}

fn on(placement: &AgentPlacement, node_id: &str) -> AgentPlacement {
    AgentPlacement {
        node_id: Some(node_id.into()),
        ..placement.clone()
    }
}

fn request<'a>(placement: &'a AgentPlacement, model: Option<&'a str>) -> PlacementRequest<'a> {
    PlacementRequest {
        placement,
        provider: "claude-code",
        model,
        with_profile: false,
    }
}

async fn select(fake: &Fake, request: PlacementRequest<'_>) -> Result<Vec<String>> {
    select_machines(fake, request, now())
        .await
        .map(|rows| rows.into_iter().map(|row| row.id).collect())
}

fn placement_failure(result: Result<Vec<String>>) -> PlacementUnavailable {
    match result {
        Err(Error::PlacementUnavailable(failure)) => *failure,
        other => panic!("expected placement-unavailable, got {other:?}"),
    }
}

fn model_failure(result: Result<Vec<String>>) -> ModelUnavailable {
    match result {
        Err(Error::ModelUnavailable(failure)) => *failure,
        other => panic!("expected model-unavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn picks_fewest_running_then_most_recently_used() {
    let mut busy = report(MAC, FRESH, claude(&["opus"]));
    busy.capacity.running_agents = 2;
    let mut older = machine("older", Some(report(MAC, FRESH, claude(&["opus"]))));
    older.last_connected_at = Some("2026-10-30T00:00:00Z".into());
    let mut newer = machine("newer", Some(report(MAC, FRESH, claude(&["opus"]))));
    newer.last_connected_at = Some("2026-10-31T09:00:00Z".into());
    let unused = machine("never", Some(report(MAC, FRESH, claude(&["opus"]))));
    let fake = Fake::new(vec![machine("busy", Some(busy)), older, unused, newer]);
    let placement = remote();
    assert_eq!(
        select(&fake, request(&placement, Some("opus")))
            .await
            .unwrap(),
        ["newer", "older", "never", "busy"]
    );
    assert!(
        fake.refreshed().is_empty(),
        "fresh reports are used as cached"
    );
}

#[tokio::test]
async fn filters_platform_installed_sign_in_and_model() {
    let fake = Fake::new(vec![
        machine("linux", Some(report(LINUX, FRESH, claude(&["opus"])))),
        machine(
            "missing",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::CliMissing,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "prereq",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::PrerequisiteMissing,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "out",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::SignedOut,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "failed",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::AuthFailed,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "unknown",
            Some(report(
                MAC,
                FRESH,
                vec![provider("claude-code", ProviderStatus::Unknown, &["opus"])],
            )),
        ),
        machine("sonnet-only", Some(report(MAC, FRESH, claude(&["sonnet"])))),
        machine(
            "codex-only",
            Some(report(
                MAC,
                FRESH,
                vec![provider("codex", ProviderStatus::SignedIn, &["opus"])],
            )),
        ),
        machine("ok", Some(report(MAC, FRESH, claude(&["opus", "sonnet"])))),
    ]);
    let placement = AgentPlacement {
        os: Some(NodeOs::Macos),
        ..remote()
    };
    let mut chosen = select(&fake, request(&placement, Some("opus")))
        .await
        .unwrap();
    chosen.sort();
    assert_eq!(chosen, ["ok", "unknown"]);

    // No model requested: any listed model will do.
    let mut chosen = select(&fake, request(&placement, None)).await.unwrap();
    chosen.sort();
    assert_eq!(chosen, ["ok", "sonnet-only", "unknown"]);
}

#[tokio::test]
async fn a_selected_profile_ignores_the_machines_own_sign_in() {
    let fake = Fake::new(vec![
        machine(
            "out",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::SignedOut,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "failed",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::AuthFailed,
                    &["opus"],
                )],
            )),
        ),
        machine(
            "missing",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::CliMissing,
                    &["opus"],
                )],
            )),
        ),
    ]);
    let placement = remote();
    let mut chosen = select(
        &fake,
        PlacementRequest {
            with_profile: true,
            ..request(&placement, Some("opus"))
        },
    )
    .await
    .unwrap();
    chosen.sort();
    assert_eq!(chosen, ["failed", "out"], "installation still applies");
}

#[tokio::test]
async fn model_unavailable_lists_each_platform_matching_machine() {
    let fake = Fake::new(vec![
        machine(
            "mac",
            Some(report(MAC, FRESH, claude(&["sonnet", "haiku"]))),
        ),
        machine(
            "studio",
            Some(report(
                MAC,
                FRESH,
                vec![provider(
                    "claude-code",
                    ProviderStatus::SignedOut,
                    &["opus"],
                )],
            )),
        ),
        machine("linux", Some(report(LINUX, FRESH, claude(&["opus"])))),
    ]);
    let placement = AgentPlacement {
        os: Some(NodeOs::Macos),
        ..remote()
    };
    let failure = model_failure(select(&fake, request(&placement, Some("opus"))).await);
    assert_eq!(
        failure.data(),
        json!({
            "code": "model-unavailable",
            "provider": "claude-code",
            "model": "opus",
            "machines": [
                { "nodeId": "mac", "name": "mac-name",
                  "platform": { "os": "macos", "arch": "aarch64" },
                  "status": "signed-in", "models": ["sonnet", "haiku"], "observedAt": FRESH },
                { "nodeId": "studio", "name": "studio-name",
                  "platform": { "os": "macos", "arch": "aarch64" },
                  "status": "signed-out", "models": ["opus"], "observedAt": FRESH },
            ],
            "retryable": false,
        })
    );
    assert!(
        failure
            .message()
            .contains("isn't available on studio-name (macos/aarch64, claude-code: signed-out)"),
        "{}",
        failure.message()
    );
}

#[tokio::test]
async fn model_unavailable_without_a_model_names_the_provider_status() {
    let fake = Fake::new(vec![machine(
        "out",
        Some(report(
            MAC,
            FRESH,
            vec![provider(
                "claude-code",
                ProviderStatus::SignedOut,
                &["opus"],
            )],
        )),
    )]);
    let placement = remote();
    let failure = model_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(
        failure.message(),
        "Provider \"claude-code\" can't start on out-name (macos/aarch64, status: signed-out)."
    );
    assert_eq!(failure.data()["model"], "");
}

#[tokio::test]
async fn model_unavailable_is_retryable_with_a_stale_list_or_an_unreachable_machine() {
    let mut stale = provider("claude-code", ProviderStatus::SignedIn, &["sonnet"]);
    stale.models_stale = true;
    // Refreshed first; the machine still could not refresh its list.
    let fake = Fake::new(vec![machine(
        "mac",
        Some(report(MAC, FRESH, vec![stale.clone()])),
    )])
    .on_refresh("mac", Ok(report(MAC, NOW, vec![stale])));
    let placement = remote();
    assert!(model_failure(select(&fake, request(&placement, Some("opus"))).await).retryable);
    assert_eq!(fake.refreshed(), ["mac"]);

    let fake = Fake::new(vec![
        machine("mac", Some(report(MAC, FRESH, claude(&["sonnet"])))),
        machine("away", None),
    ])
    .on_refresh("away", Err(RefreshFailure::Unreachable));
    assert!(model_failure(select(&fake, request(&placement, Some("opus"))).await).retryable);
}

#[tokio::test]
async fn refreshes_missing_stale_and_update_required_reports_first() {
    let mut needs_update = machine("updated", Some(report(MAC, FRESH, claude(&["sonnet"]))));
    needs_update.state = RemoteMachineState::UpdateRequired;
    let fake = Fake::new(vec![
        machine("fresh", Some(report(MAC, FRESH, claude(&["sonnet"])))),
        machine("stale", Some(report(MAC, STALE, claude(&["sonnet"])))),
        machine("new", None),
        needs_update,
        // A cached platform that cannot match is not dialed.
        machine("linux", Some(report(LINUX, STALE, claude(&["opus"])))),
    ])
    .on_refresh("stale", Ok(report(MAC, NOW, claude(&["opus"]))))
    .on_refresh("new", Ok(report(MAC, NOW, claude(&["opus"]))))
    .on_refresh("updated", Ok(report(MAC, NOW, claude(&["opus"]))));
    let placement = AgentPlacement {
        os: Some(NodeOs::Macos),
        ..remote()
    };
    let mut chosen = select(&fake, request(&placement, Some("opus")))
        .await
        .unwrap();
    chosen.sort();
    assert_eq!(chosen, ["new", "stale", "updated"]);
    assert_eq!(fake.refreshed(), ["stale", "new", "updated"]);
}

#[test]
fn a_report_is_stale_after_an_hour_or_when_unreadable() {
    assert!(!report_is_stale(Some(FRESH), now()));
    assert!(!report_is_stale(Some("2026-10-31T11:00:00Z"), now()));
    assert!(report_is_stale(Some(STALE), now()));
    assert!(report_is_stale(Some("yesterday"), now()));
    assert!(report_is_stale(None, now()));
}

#[tokio::test]
async fn no_capacity_names_the_limit_of_one_full_machine() {
    let mut full = report(MAC, FRESH, claude(&["opus"]));
    full.capacity.running_agents = 4;
    let fake = Fake::new(vec![machine("full", Some(full))]);
    let placement = remote();
    let failure = placement_failure(select(&fake, request(&placement, Some("opus"))).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::NoCapacity);
    assert_eq!(
        failure.message(),
        "No capacity on full-name: 4 of 4 agents running"
    );
    let data = failure.data();
    assert_eq!(data["nodeId"], "full");
    assert_eq!(data["limit"], "count");
    assert_eq!(data["retryable"], true);
}

#[tokio::test]
async fn no_capacity_lists_each_full_machine_and_its_limit() {
    let mut count = report(MAC, FRESH, claude(&["opus"]));
    count.capacity.running_agents = 4;
    let mut memory = report(MAC, FRESH, claude(&["opus"]));
    memory.capacity.charged_memory_bytes = 8 << 30;
    memory.capacity.memory_budget_bytes = Some(8 << 30);
    let fake = Fake::new(vec![
        machine("count", Some(count)),
        machine("memory", Some(memory)),
    ]);
    let placement = remote();
    let failure = placement_failure(select(&fake, request(&placement, Some("opus"))).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::NoCapacity);
    assert_eq!(
        failure.data()["machines"],
        json!([
            { "nodeId": "count", "limit": "count", "runningAgents": 4, "agentCap": 4 },
            { "nodeId": "memory", "limit": "memory",
              "chargedMemoryBytes": 8u64 << 30, "memoryBudgetBytes": 8u64 << 30 },
        ])
    );
}

#[tokio::test]
async fn unsupported_platform_reports_what_is_supported() {
    let fake = Fake::new(vec![machine(
        "linux",
        Some(report(LINUX, FRESH, claude(&["opus"]))),
    )]);
    let placement = AgentPlacement {
        os: Some(NodeOs::Macos),
        ..remote()
    };
    let failure = placement_failure(select(&fake, request(&placement, Some("opus"))).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );
    assert_eq!(failure.availability.supported_platforms, [LINUX]);

    let named = on(&placement, "linux");
    let failure = placement_failure(select(&fake, request(&named, Some("opus"))).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );
    assert_eq!(failure.message(), "linux-name does not run macos");
}

#[tokio::test]
async fn a_named_machine_never_falls_back() {
    let fake = Fake::new(vec![
        machine("a", Some(report(MAC, FRESH, claude(&["sonnet"])))),
        machine("b", Some(report(MAC, FRESH, claude(&["opus"])))),
    ]);
    let placement = on(&remote(), "a");
    let failure = model_failure(select(&fake, request(&placement, Some("opus"))).await);
    assert_eq!(failure.machines.len(), 1);
    assert_eq!(failure.machines[0].node_id, "a");

    let placement = on(&remote(), "b");
    assert_eq!(
        select(&fake, request(&placement, Some("opus")))
            .await
            .unwrap(),
        ["b"]
    );
}

#[tokio::test]
async fn unknown_and_revoked_machines() {
    let mut revoked = machine("gone", Some(report(MAC, FRESH, claude(&["opus"]))));
    revoked.state = RemoteMachineState::Revoked;
    let fake = Fake::new(vec![revoked]);

    let placement = on(&remote(), "nope");
    let failure = placement_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::MachineUnknown);
    assert_eq!(
        failure.message(),
        "No machine nope is exposed to this backend"
    );
    assert_eq!(failure.data()["retryable"], false);

    let placement = on(&remote(), "gone");
    let failure = placement_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::MachineRevoked);
    assert_eq!(failure.message(), "gone-name revoked this backend's access");

    // Unnamed: revoked machines are not candidates.
    let placement = remote();
    let failure = placement_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::MachineRevoked);
    assert!(fake.refreshed().is_empty());

    // No machine at all keeps the single-node shape.
    let failure = placement_failure(select(&Fake::new(vec![]), request(&placement, None)).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );
}

#[tokio::test]
async fn refresh_failures_of_the_only_candidates() {
    let placement = remote();
    let fake =
        Fake::new(vec![machine("away", None)]).on_refresh("away", Err(RefreshFailure::Unreachable));
    let failure = placement_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::MachineUnreachable
    );
    assert_eq!(failure.data()["retryable"], true);
    let named = on(&remote(), "away");
    let failure = placement_failure(select(&fake, request(&named, None)).await);
    assert_eq!(failure.message(), "away-name is unreachable");

    let fake =
        Fake::new(vec![machine("gone", None)]).on_refresh("gone", Err(RefreshFailure::Revoked));
    let failure = placement_failure(select(&fake, request(&placement, None)).await);
    assert_eq!(failure.reason, PlacementUnavailableReason::MachineRevoked);
}

/// A machine on protocol 6 when this backend offers only 7 (stop
/// acknowledgement): the machine must update.
#[tokio::test]
async fn a_version_gap_is_machine_update_required() {
    let update = MachineUpdateRequired {
        machine_name: "old-name".into(),
        machine_version: "0.11.80".into(),
        backend_version: "0.11.90".into(),
        update: UpdateSide::for_ranges(
            ProtocolRange { min: 7, max: 7 },
            ProtocolRange { min: 6, max: 6 },
        ),
    };
    assert_eq!(update.update, UpdateSide::Machine);
    let mut old = machine("old", Some(report(MAC, STALE, claude(&["opus"]))));
    old.state = RemoteMachineState::UpdateRequired;
    let fake = Fake::new(vec![old]).on_refresh(
        "old",
        Err(RefreshFailure::UpdateRequired(Box::new(update.clone()))),
    );
    for placement in [remote(), on(&remote(), "old")] {
        match select(&fake, request(&placement, Some("opus"))).await {
            Err(Error::MachineUpdateRequired(failure)) => {
                assert_eq!(*failure, update);
                assert_eq!(
                    failure.message(),
                    "Update Intent on old-name (it runs 0.11.80; this backend needs 0.11.90 or newer)."
                );
            }
            other => panic!("expected machine-update-required, got {other:?}"),
        }
    }
    // A newer machine against an older backend: the backend must update.
    assert_eq!(
        UpdateSide::for_ranges(
            ProtocolRange { min: 6, max: 6 },
            ProtocolRange { min: 7, max: 7 }
        ),
        UpdateSide::Backend
    );
}

#[tokio::test]
async fn an_unreachable_candidate_does_not_hide_a_reachable_one() {
    let fake = Fake::new(vec![
        machine("away", None),
        machine("here", Some(report(MAC, FRESH, claude(&["opus"])))),
    ])
    .on_refresh("away", Err(RefreshFailure::Unreachable));
    let placement = remote();
    assert_eq!(
        select(&fake, request(&placement, Some("opus")))
            .await
            .unwrap(),
        ["here"]
    );
}

#[test]
fn help_lists_machines_providers_and_sign_in_without_routes() {
    let mut mac = machine(
        "d-mac",
        Some(report(
            MAC,
            FRESH,
            vec![
                provider("claude-code", ProviderStatus::SignedIn, &["opus", "sonnet"]),
                provider("codex", ProviderStatus::SignedOut, &[]),
                provider("droid", ProviderStatus::CliMissing, &["x"]),
            ],
        )),
    );
    mac.name = "Mac mini".into();
    let mut linux = machine("d-box", Some(report(LINUX, FRESH, claude(&["opus"]))));
    linux.last_connected_at = Some("2026-10-30T08:00:00Z".into());
    let mut revoked = machine("d-gone", None);
    revoked.state = RemoteMachineState::Revoked;
    let text = exposed_machines_help(&[mac, linux, revoked], &|id| id == "d-mac").unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        "Remote placement (experimental): machines exposed to this backend (2):"
    );
    assert_eq!(
        lines[1],
        format!(
            "- \"Mac mini\" (nodeId `d-mac`, macos/aarch64, connected, report {FRESH}): claude-code signed-in [opus, sonnet]; codex signed-out [no models listed]"
        )
    );
    assert_eq!(
        lines[2],
        format!(
            "- \"d-box-name\" (nodeId `d-box`, linux/x86_64, last seen 2026-10-30T08:00:00Z, report {FRESH}): claude-code signed-in [opus]"
        )
    );
    assert!(lines[3].starts_with("To run an agent on one, pass `placement"));
    for secret in ["10.0.0.1", "100.64.0.1", "7443", &"ab".repeat(32), "d-gone"] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    assert_eq!(exposed_machines_help(&[], &|_| false), None);
}

#[test]
fn help_caps_models_machines_and_bytes() {
    let many: Vec<String> = (0..15).map(|i| format!("model-{i}")).collect();
    let refs: Vec<&str> = many.iter().map(String::as_str).collect();
    let rows: Vec<ExposedMachine> = (0..12)
        .map(|i| machine(&format!("m{i}"), Some(report(MAC, FRESH, claude(&refs)))))
        .collect();
    let text = exposed_machines_help(&rows, &|_| false).unwrap();
    assert!(text.contains("model-9, +5 more]"), "{text}");
    assert!(!text.contains("model-10"));
    assert_eq!(
        text.lines().filter(|l| l.starts_with("- \"")).count(),
        HELP_MAX_MACHINES
    );
    assert!(text.contains(
        "- … 4 more not shown; a `model-unavailable` error lists every machine's models."
    ));

    // Long names hit the byte budget before the machine cap.
    let wide: Vec<ExposedMachine> = (0..8)
        .map(|i| {
            let mut row = machine(&format!("w{i}"), Some(report(MAC, FRESH, claude(&refs))));
            row.name = "n".repeat(1500);
            row
        })
        .collect();
    let text = exposed_machines_help(&wide, &|_| false).unwrap();
    let listed = text.lines().filter(|l| l.starts_with("- \"")).count();
    assert!(listed < 8 && listed > 0, "{listed}");
    assert!(text.contains(&format!("- … {} more not shown", 8 - listed)));
}

#[test]
fn a_requested_model_matches_its_listed_id_or_an_alias_of_it() {
    for (listed, requested, expected) in [
        ("claude-opus-4-1", "claude-opus-4-1", true),
        ("claude-opus-4-1", "Claude-Opus-4-1", true),
        ("claude-opus-4-1", "opus", true),
        ("claude-opus-4-1", "opus-4-1", true),
        ("claude-opus-4-1", "opus-4", true),
        ("claude-opus-4-1", "sonnet", false),
        ("claude-opus-4-1", "opus-4-5", false),
        ("gpt-5-codex", "gpt-5", true),
        ("gpt-5", "gpt-5-codex", false),
        ("gpt-5", "", false),
    ] {
        assert_eq!(
            model_listed(listed, requested),
            expected,
            "{listed} / {requested}"
        );
    }
}

/// A machine's welcome right after remote agents were turned on carries no
/// providers yet; placement asks for a fresh report instead of refusing.
#[tokio::test]
async fn a_report_without_providers_is_refreshed() {
    let fake = Fake::new(vec![machine("early", Some(report(MAC, FRESH, vec![])))])
        .on_refresh("early", Ok(report(MAC, NOW, claude(&["opus"]))));
    let placement = remote();
    assert_eq!(
        select(&fake, request(&placement, Some("opus")))
            .await
            .unwrap(),
        ["early"]
    );
    assert_eq!(fake.refreshed(), ["early"]);
}

/// A reachable machine that is slow to report keeps its cached report: an
/// empty one gives a retryable `model-unavailable` with status unknown, a
/// usable one still takes the agent.
#[tokio::test]
async fn a_slow_report_keeps_the_cached_one_and_is_retryable() {
    let fake = Fake::new(vec![machine("early", Some(report(MAC, FRESH, vec![])))])
        .on_refresh("early", Err(RefreshFailure::Slow));
    let placement = remote();
    let failure = model_failure(select(&fake, request(&placement, None)).await);
    assert!(failure.retryable);
    assert_eq!(failure.machines[0].status, ProviderStatus::Unknown);

    let fake = Fake::new(vec![machine(
        "stale",
        Some(report(MAC, STALE, claude(&["opus"]))),
    )])
    .on_refresh("stale", Err(RefreshFailure::Slow));
    assert_eq!(
        select(&fake, request(&placement, Some("opus")))
            .await
            .unwrap(),
        ["stale"]
    );
}

/// A machine with no report yet that is slow to send one is not taken as
/// unsupported: the refusal names it and is retryable.
#[tokio::test]
async fn a_slow_machine_without_a_report_is_a_retryable_refusal() {
    let fake = Fake::new(vec![machine("new", None)]).on_refresh("new", Err(RefreshFailure::Slow));
    let unnamed = remote();
    let failure = placement_failure(select(&fake, request(&unnamed, None)).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::MachineUnreachable
    );
    assert_eq!(failure.data()["retryable"], true);

    let named = on(&remote(), "new");
    let failure = placement_failure(select(&fake, request(&named, None)).await);
    assert_eq!(
        failure.reason,
        PlacementUnavailableReason::MachineUnreachable
    );
    assert_eq!(failure.data()["nodeId"], "new");
    assert_eq!(failure.data()["retryable"], true);
}

/// A fresh report whose provider models are still being discovered (the
/// machine's quick first list: empty and stale) is refreshed when it cannot
/// answer the requested model, for named and unnamed placement.
#[tokio::test]
async fn a_stale_model_list_that_lacks_the_model_is_refreshed() {
    let mut primed = provider("claude-code", ProviderStatus::Unknown, &[]);
    primed.models_stale = true;
    for placement in [remote(), on(&remote(), "early")] {
        let fake = Fake::new(vec![machine(
            "early",
            Some(report(MAC, FRESH, vec![primed.clone()])),
        )])
        .on_refresh("early", Ok(report(MAC, NOW, claude(&["opus"]))));
        assert_eq!(
            select(&fake, request(&placement, Some("opus")))
                .await
                .unwrap(),
            ["early"]
        );
        assert_eq!(fake.refreshed(), ["early"]);
    }

    // Still discovering: the refusal stays retryable, and the next attempt
    // asks again.
    let fake = Fake::new(vec![machine(
        "early",
        Some(report(MAC, FRESH, vec![primed.clone()])),
    )])
    .on_refresh("early", Err(RefreshFailure::Slow));
    let placement = remote();
    assert!(model_failure(select(&fake, request(&placement, Some("opus"))).await).retryable);
    assert!(model_failure(select(&fake, request(&placement, Some("opus"))).await).retryable);
    assert_eq!(fake.refreshed(), ["early", "early"]);

    // A stale list that has the model, or a request without one, decides
    // on the cached report.
    let mut listed = provider("claude-code", ProviderStatus::SignedIn, &["opus"]);
    listed.models_stale = true;
    let fake = Fake::new(vec![
        machine("listed", Some(report(MAC, FRESH, vec![listed]))),
        machine("primed", Some(report(MAC, FRESH, vec![primed]))),
    ]);
    let mut chosen = select(&fake, request(&placement, None)).await.unwrap();
    chosen.sort();
    assert_eq!(chosen, ["listed", "primed"]);
    assert!(fake.refreshed().is_empty());
}
