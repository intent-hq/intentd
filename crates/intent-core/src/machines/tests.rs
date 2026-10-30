use serde_json::json;

use super::*;
use crate::nodes::{NodeArch, NodeOs};
use crate::Error;

fn machine(name: &str, status: ProviderStatus, models: usize) -> MachineModels {
    MachineModels {
        node_id: format!("{name}-id"),
        name: name.into(),
        platform: AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64,
        },
        status,
        models: (0..models).map(|i| format!("m{i}")).collect(),
        observed_at: "2026-10-30T00:00:00Z".into(),
    }
}

#[test]
fn model_unavailable_lists_what_each_machine_has_and_its_status() {
    let failure = ModelUnavailable::new(
        "claude",
        "opus",
        [
            machine("mac-mini", ProviderStatus::SignedIn, 2),
            machine("studio", ProviderStatus::SignedOut, 1),
            machine("pi", ProviderStatus::CliMissing, 0),
        ],
        false,
    );
    assert_eq!(
        failure.message(),
        "Model \"opus\" isn't available on mac-mini (macos/aarch64, claude: signed-in); it has: m0, m1. \
         Model \"opus\" isn't available on studio (macos/aarch64, claude: signed-out); it has: m0. \
         Model \"opus\" isn't available on pi (macos/aarch64, claude: cli-missing); it has: no models listed."
    );
    assert_eq!(
        failure.data(),
        json!({
            "code": "model-unavailable",
            "provider": "claude",
            "model": "opus",
            "machines": [
                { "nodeId": "mac-mini-id", "name": "mac-mini",
                  "platform": { "os": "macos", "arch": "aarch64" },
                  "status": "signed-in", "models": ["m0", "m1"],
                  "observedAt": "2026-10-30T00:00:00Z" },
                { "nodeId": "studio-id", "name": "studio",
                  "platform": { "os": "macos", "arch": "aarch64" },
                  "status": "signed-out", "models": ["m0"],
                  "observedAt": "2026-10-30T00:00:00Z" },
                { "nodeId": "pi-id", "name": "pi",
                  "platform": { "os": "macos", "arch": "aarch64" },
                  "status": "cli-missing", "models": [],
                  "observedAt": "2026-10-30T00:00:00Z" },
            ],
            "retryable": false,
        })
    );
}

#[test]
fn model_unavailable_caps_models_in_data_and_message() {
    let failure = ModelUnavailable::new(
        "codex",
        "gpt-x",
        [machine("box", ProviderStatus::Unknown, 80)],
        true,
    );
    assert_eq!(
        failure.machines[0].models.len(),
        MODEL_UNAVAILABLE_MAX_MODELS
    );
    assert!(failure
        .message()
        .ends_with("(macos/aarch64, codex: unknown); it has: m0, m1, m2, m3, m4, m5, m6, m7, m8, m9 (+40 more)."));
    assert_eq!(failure.data()["retryable"], true);
}

#[test]
fn model_unavailable_without_machines_says_none_is_exposed() {
    let failure = ModelUnavailable::new("claude", "opus", [], false);
    assert_eq!(
        failure.message(),
        "Model \"opus\" (claude) isn't available: no machine is exposed to this backend."
    );
    assert_eq!(failure.data()["machines"], json!([]));
}

#[test]
fn machine_models_round_trip() {
    let entry = machine("mac", ProviderStatus::AuthFailed, 1);
    let wire = serde_json::to_value(&entry).unwrap();
    assert_eq!(wire["status"], "auth-failed");
    assert_eq!(
        serde_json::from_value::<MachineModels>(wire).unwrap(),
        entry
    );
}

#[test]
fn update_side_follows_the_older_range() {
    let range = |min, max| ProtocolRange { min, max };
    assert_eq!(
        UpdateSide::for_ranges(range(7, 8), range(6, 6)),
        UpdateSide::Machine
    );
    assert_eq!(
        UpdateSide::for_ranges(range(6, 6), range(7, 8)),
        UpdateSide::Backend
    );
}

#[test]
fn machine_update_required_tells_which_side_to_update() {
    let machine = MachineUpdateRequired {
        machine_name: "mac-mini".into(),
        machine_version: "0.11.0".into(),
        backend_version: "0.12.0".into(),
        update: UpdateSide::Machine,
    };
    assert_eq!(
        machine.message(),
        "Update Intent on mac-mini (it runs 0.11.0; this backend needs 0.12.0 or newer)."
    );
    assert_eq!(
        machine.data(),
        json!({
            "code": "machine-update-required",
            "machineVersion": "0.11.0",
            "backendVersion": "0.12.0",
            "update": "machine",
        })
    );
    let backend = MachineUpdateRequired {
        machine_name: "mac-mini".into(),
        machine_version: "0.13.0".into(),
        backend_version: "0.12.0".into(),
        update: UpdateSide::Backend,
    };
    assert_eq!(
        backend.message(),
        "Update Intent on this backend (it runs 0.12.0; mac-mini needs 0.13.0 or newer)."
    );
    assert_eq!(backend.data()["update"], "backend");
}

#[test]
fn both_errors_are_invalid_params_with_typed_text_for_mcp() {
    let model =
        Error::ModelUnavailable(Box::new(ModelUnavailable::new("claude", "opus", [], false)));
    assert_eq!(model.code(), -32602);
    assert!(model.to_string().contains("\"code\":\"model-unavailable\""));
    let update = Error::MachineUpdateRequired(Box::new(MachineUpdateRequired {
        machine_name: "m".into(),
        machine_version: "1".into(),
        backend_version: "2".into(),
        update: UpdateSide::Machine,
    }));
    assert_eq!(update.code(), -32602);
    let text = update.to_string();
    assert!(text.starts_with("Update Intent on m (it runs 1;"));
    assert!(text.contains("\"code\":\"machine-update-required\""));
}

#[test]
fn remote_machine_state_names_match_the_wire() {
    for state in [
        RemoteMachineState::Connected,
        RemoteMachineState::Disconnected,
        RemoteMachineState::Unreachable,
        RemoteMachineState::Revoked,
        RemoteMachineState::UpdateRequired,
    ] {
        assert_eq!(serde_json::to_value(state).unwrap(), json!(state.as_str()));
        assert_eq!(RemoteMachineState::parse(state.as_str()), Some(state));
    }
    assert_eq!(RemoteMachineState::parse("online"), None);
}
