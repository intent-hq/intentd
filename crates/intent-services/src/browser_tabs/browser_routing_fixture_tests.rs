//! The workspace half of the shared REV-2 routing cases
//! (`docs/protocol/fixtures/browser-routing/routing.json`, mirrored in
//! intent-transport): for every case, the workspace state (Chief, pin, active
//! desktop session, registry tabs) yields the case's `expect.target` from
//! [`Services::driving_client_target`]. intent-transport runs the other half,
//! resolving that target against the case's connections.

use intent_core::{ClientHostInfo, ClientId, ReverseTarget, WorkspaceId};
use serde_json::{json, Value};

use super::browser_client_pin_tests::{setup, FakeRegistry};
use super::browser_routing_tests::register_tab;
use crate::Services;

const ROUTING_FIXTURE: &str =
    include_str!("../../../intent-transport/tests/fixtures/browser-routing.json");

fn expected_target(case: &Value) -> ReverseTarget {
    let target = &case["expect"]["target"];
    let client_id = || {
        ClientId::from_string(
            target["clientId"]
                .as_str()
                .unwrap_or_else(|| panic!("{}: target without clientId", case["id"])),
        )
    };
    match target["kind"].as_str() {
        Some("default") => ReverseTarget::Default,
        Some("client") => ReverseTarget::Client(client_id()),
        Some("pinned") => ReverseTarget::Pinned(client_id()),
        other => panic!("{}: unknown target kind {other:?}", case["id"]),
    }
}

async fn known_client(svc: &Services, id: &str) -> ClientId {
    let client = ClientId::from_string(id);
    svc.store()
        .upsert_client(
            &client,
            Some(id),
            Some(&json!({ "browserExec": true })),
            &ClientHostInfo::default(),
        )
        .await
        .expect("client row");
    client
}

/// Every shared case on one store, each in its own fresh workspace (Chief
/// only for the Chief case), reporting all failing cases at once.
#[intent_test_macros::daemon_test]
async fn routing_fixture_every_case_derives_the_expected_target() {
    let fixture: Value = serde_json::from_str(ROUTING_FIXTURE).expect("routing fixture json");
    assert_eq!(fixture["schema"], "browser-routing/v1");
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(!cases.is_empty(), "the routing fixture has no cases");
    let (_t, _r, svc, _bus, _primary) = setup(FakeRegistry::new(&[])).await;
    let mut failures = Vec::new();
    for case in cases {
        let name = case["id"].as_str().expect("case id");
        let state = &case["workspace"];
        let ws = match state["chief"].as_bool() {
            Some(true) => WorkspaceId::chief(),
            Some(false) => {
                let ws = WorkspaceId::new();
                svc.store()
                    .insert_workspace(&crate::test_support::fixtures::workspace(&ws))
                    .await
                    .expect("workspace");
                ws
            }
            None => panic!("{name}: workspace.chief must be a boolean"),
        };
        if let Some(pin) = state["pin"].as_str() {
            let client = known_client(&svc, pin).await;
            svc.store()
                .set_workspace_browser_client(&ws, Some(&client))
                .await
                .expect("pin");
        }
        for tab in state["tabs"].as_array().expect("tabs") {
            register_tab(
                &svc,
                &ws,
                &format!("{name}/{}", tab["tabId"].as_str().expect("tabId")),
                tab["host"].as_str().expect("host"),
                tab["owner"].as_str(),
            )
            .await;
        }
        if let Some(client) = state["desktopSession"].as_str() {
            svc.feature_desktop()
                .activate_for_test(&ws, ClientId::from_string(client));
        }
        let got = svc.driving_client_target(&ws).await.expect(name);
        let want = expected_target(case);
        if got != want {
            failures.push(format!("{name}: expected {want:?}, got {got:?}"));
        }
        svc.feature_desktop().deactivate_for_test();
    }
    assert!(
        failures.is_empty(),
        "routing fixture cases failed:\n{}",
        failures.join("\n")
    );
}
