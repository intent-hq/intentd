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
        hint: None,
        reason: None,
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
        hint: None,
        reason: None,
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
        hint: None,
        reason: None,
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

#[test]
fn executor_live_status_defaults_to_disconnected() {
    assert_eq!(
        ExecutorLiveStatus::default(),
        ExecutorLiveStatus {
            connected: false,
            running_agents: 0,
        }
    );
}

#[test]
fn machine_routes_debug_names_every_route() {
    let routes = MachineRoutes {
        name: "Build box".into(),
        port: 7443,
        hosts: vec!["10.0.0.2".into()],
        tc_address: Some("tc1".into()),
        fingerprint: "AA:BB".into(),
    };
    let text = format!("{routes:?}");
    assert!(text.contains("7443") && text.contains("10.0.0.2") && text.contains("tc1"));
}

/// `Error::with_hint` appends the hint to each admission failure's message
/// and leaves its code and `error.data` as they were.
#[test]
fn a_hint_extends_admission_messages_only() {
    let hint = "Pass placement { target: \"local\" } to run it on the backend.";
    let model = ModelUnavailable::new("claude", "opus", [], false);
    let (message, data) = (model.message(), model.data());
    let Error::ModelUnavailable(hinted) = Error::ModelUnavailable(Box::new(model)).with_hint(hint)
    else {
        panic!("still model-unavailable");
    };
    assert_eq!(hinted.message(), format!("{message} {hint}"));
    assert_eq!(hinted.data(), data);

    let update = MachineUpdateRequired {
        machine_name: "m".into(),
        machine_version: "1".into(),
        backend_version: "2".into(),
        update: UpdateSide::Machine,
        hint: None,
        reason: None,
    };
    let (message, data) = (update.message(), update.data());
    let Error::MachineUpdateRequired(hinted) =
        Error::MachineUpdateRequired(Box::new(update)).with_hint(hint)
    else {
        panic!("still machine-update-required");
    };
    assert_eq!(hinted.message(), format!("{message} {hint}"));
    assert_eq!(hinted.data(), data);

    let profile = Error::provider_profile(false, "Uses an account folder.", json!({"code":"x"}))
        .with_hint(hint);
    assert_eq!(profile.code(), -32602);
    assert_eq!(
        profile.to_string(),
        format!("Uses an account folder. {hint}")
    );
    let invalid = Error::InvalidParams("bad".into()).with_hint(hint);
    assert!(matches!(&invalid, Error::InvalidParams(m) if *m == format!("bad {hint}")));
    let internal = Error::Internal("boom".into()).with_hint(hint);
    assert!(matches!(&internal, Error::Internal(m) if m == "boom"));
}

#[test]
fn a_prepared_checkout_refusal_names_the_reason() {
    let update = MachineUpdateRequired::prepared_checkout("mac-mini".into(), "0.11.0".into());
    assert_eq!(
        update.message(),
        "Update Intent on mac-mini to run agents for this workspace: it has submodules or a setup script."
    );
    assert_eq!(update.data()["reason"], "prepared-checkout");
    assert_eq!(update.data()["update"], "machine");
}
