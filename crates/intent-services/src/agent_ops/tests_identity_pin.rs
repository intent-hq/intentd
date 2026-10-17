//! Every create seam pins the session's effective provider on the row, so a
//! later `model.defaultProvider` change never moves an existing conversation
//! (a NULL provider is a legacy row). Harness shared with the specialist
//! provider suite: default provider `grok`, settings model `default-model`.

use std::sync::Arc;

use intent_acp::WorkspaceMcpServer;
use intent_core::{AgentCreateExtra, AgentId, WorkspaceId};
use serde_json::{json, Value};

use super::tests_specialist_provider::{set, setup};
use crate::Services;

async fn create_plain(
    svc: &Services,
    ws: &WorkspaceId,
    model: Option<&str>,
    specialist: Option<&str>,
) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            None,
            model.map(str::to_string),
            specialist.map(str::to_string),
            None,
            None,
            false,
            AgentCreateExtra::default(),
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().expect("created id"))
}

async fn stored_provider(svc: &Services, id: &AgentId) -> Option<String> {
    svc.store()
        .get_agent_session(id)
        .await
        .expect("persisted session")
        .provider
}

#[tokio::test]
async fn model_only_create_pins_the_default_provider_across_a_default_change() {
    let (_tmp, svc, ws) = setup().await;
    let id = create_plain(&svc, &ws, Some("default-model"), None).await;
    assert_eq!(stored_provider(&svc, &id).await.as_deref(), Some("grok"));

    set(&svc, "model.defaultProvider", json!("auggie"));
    assert_eq!(
        stored_provider(&svc, &id).await.as_deref(),
        Some("grok"),
        "a default change must not move an existing conversation"
    );
}

#[tokio::test]
async fn bare_create_pins_provider_and_model() {
    let (_tmp, svc, ws) = setup().await;
    let id = create_plain(&svc, &ws, None, None).await;
    let session = svc.store().get_agent_session(&id).await.unwrap();
    assert_eq!(session.provider.as_deref(), Some("grok"));
    assert_eq!(session.model.as_deref(), Some("default-model"));
    assert_eq!(session.reasoning_effort.as_deref(), Some("low"));
}

#[tokio::test]
async fn specialist_without_coding_agent_pins_the_default_provider() {
    let (tmp, svc, ws) = setup().await;
    let specialists = tmp.path.parent().unwrap().join("specialists");
    std::fs::write(
        specialists.join("free.md"),
        "---\nname: free\ndescription: Test\n---\nTest prompt.",
    )
    .unwrap();
    let id = create_plain(&svc, &ws, None, Some("free")).await;
    assert_eq!(stored_provider(&svc, &id).await.as_deref(), Some("grok"));
    let id = create_plain(&svc, &ws, Some("default-model"), Some("pin-alias")).await;
    assert_eq!(
        stored_provider(&svc, &id).await.as_deref(),
        Some("grok"),
        "a model-only create keeps resolving on the settings default, now pinned"
    );
}

/// A settings default model owned by another provider is dropped; the
/// provider's cached catalog `isDefault` row still pins a concrete model.
#[tokio::test]
async fn mismatched_default_model_falls_back_to_the_catalog_default() {
    let (_tmp, svc, ws) = setup().await;
    svc.models_catalog.store_for_test(
        "grok",
        &(crate::model_catalog::source_for("grok")
            .unwrap()
            .version_key)(),
        vec![
            json!({"id": "default-model", "name": "default-model", "provider": "grok",
                   "isDefault": true}),
        ],
    );
    set(&svc, "model.default", json!("pinned-model"));
    let id = create_plain(&svc, &ws, None, None).await;
    let session = svc.store().get_agent_session(&id).await.unwrap();
    assert_eq!(session.provider.as_deref(), Some("grok"));
    assert_eq!(session.model.as_deref(), Some("default-model"));
}

#[intent_test_macros::daemon_test]
async fn workspace_api_create_with_model_pins_the_default_provider() {
    let (_tmp, svc, ws) = setup().await;
    set(&svc, "workspaceApi.toonOutput", json!(false));
    let server = WorkspaceMcpServer::new(Arc::new(svc.clone()), ws.clone());
    let response = server
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "workspace_api", "arguments": {
                "code": "return await ws.agent.create('Child', 'Do work', {specialist: 'pin-alias', model: 'default-model'});",
                "summary": "create child with model"
            }}
        }))
        .await
        .expect("binding response");
    assert_ne!(response["result"]["isError"], true, "{response}");
    let result: Value =
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
            .expect("binding JSON");
    let id = AgentId::from(result["id"].as_str().expect("created id"));
    assert_eq!(stored_provider(&svc, &id).await.as_deref(), Some("grok"));
}

/// Turn a freshly created session into a legacy row (NULL provider), as
/// written before every create pinned its provider.
async fn make_legacy(svc: &Services, ws: &WorkspaceId, id: &AgentId, last_turn: Option<&str>) {
    let model = svc
        .store()
        .get_agent_session(id)
        .await
        .unwrap()
        .model
        .expect("created with a model");
    svc.store()
        .set_agent_session_model(ws, id, &model, None, &intent_core::now_iso())
        .await
        .expect("clear provider");
    if let Some(provider) = last_turn {
        svc.store()
            .set_agent_session_last_turn_model(ws, id, Some(&model), provider)
            .await
            .expect("seed last turn");
    }
    assert_eq!(stored_provider(svc, id).await, None);
}

#[tokio::test]
async fn set_model_pins_a_legacy_session_onto_its_last_turn_provider() {
    let (_tmp, svc, ws) = setup().await;
    let id = create_plain(&svc, &ws, Some("default-model"), None).await;
    make_legacy(&svc, &ws, &id, Some("auggie")).await;
    // `pinned-model` is auggie's: validating against the current default
    // (grok) would reject it as another provider's model.
    svc.agent_set_model_op(id.clone(), "pinned-model".into(), None)
        .await
        .expect("setModel validates against the last-turn provider");
    assert_eq!(
        stored_provider(&svc, &id).await.as_deref(),
        Some("auggie"),
        "the provider the conversation last ran on, not the current default"
    );
}

#[tokio::test]
async fn set_model_pins_a_never_run_legacy_session_onto_the_current_default() {
    let (_tmp, svc, ws) = setup().await;
    let id = create_plain(&svc, &ws, Some("default-model"), None).await;
    make_legacy(&svc, &ws, &id, None).await;
    svc.agent_set_model_op(id.clone(), "default-model".into(), None)
        .await
        .expect("setModel");
    assert_eq!(stored_provider(&svc, &id).await.as_deref(), Some("grok"));
}

/// An explicit `providerId` is gated against the legacy row's EFFECTIVE
/// provider — its last turn's, not the current default: naming the default
/// is a real cross-provider move and must pass the availability gate.
#[tokio::test]
async fn set_model_gates_an_explicit_provider_against_the_last_turn_provider() {
    let (_tmp, svc, ws) = setup().await;
    let id = create_plain(&svc, &ws, Some("default-model"), None).await;
    make_legacy(&svc, &ws, &id, Some("auggie")).await;
    set(&svc, "providers.enabled", json!({"grok": false}));
    let err = svc
        .agent_set_model_op(id.clone(), "default-model".into(), Some("grok".into()))
        .await
        .expect_err("moving off auggie onto a disabled grok is gated");
    assert!(err.to_string().contains("not enabled"), "{err}");
    assert_eq!(stored_provider(&svc, &id).await, None, "nothing written");
}

/// A level the daemon pinned from Auto follows the model: a real model
/// switch resets it to Auto (the new model's default applies). A level
/// someone chose survives the switch, and choosing one clears the Auto pin.
#[tokio::test]
async fn model_switch_resets_only_an_auto_pinned_effort() {
    let (_tmp, svc, ws) = setup().await;
    let store = svc.store();
    let auto = create_plain(&svc, &ws, Some("default-model"), None).await;
    let mut row = store.get_agent_session(&auto).await.unwrap();
    row.reasoning_effort = None;
    store.update_agent_session(&ws, &row).await.unwrap();
    assert!(store
        .fill_agent_session_identity_if_null(
            &ws,
            &auto,
            None,
            Some("high"),
            &intent_core::now_iso()
        )
        .await
        .unwrap());
    // Rewriting the row with the same level keeps the Auto pin.
    let same = store.get_agent_session(&auto).await.unwrap();
    store.update_agent_session(&ws, &same).await.unwrap();
    svc.agent_set_model_op(auto.clone(), "default-model".into(), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session(&auto)
            .await
            .unwrap()
            .reasoning_effort
            .as_deref(),
        Some("high"),
        "same model: nothing to reset"
    );
    svc.models_catalog.store_for_test(
        "grok",
        &(crate::model_catalog::source_for("grok")
            .unwrap()
            .version_key)(),
        vec![
            json!({"id": "default-model", "name": "default-model", "provider": "grok"}),
            json!({"id": "other-model", "name": "other-model", "provider": "grok"}),
        ],
    );
    svc.agent_set_model_op(auto.clone(), "other-model".into(), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session(&auto)
            .await
            .unwrap()
            .reasoning_effort,
        None,
        "an Auto-pinned level resets on a model switch"
    );

    let chosen = create_plain(&svc, &ws, Some("default-model"), None).await;
    let mut row = store.get_agent_session(&chosen).await.unwrap();
    row.reasoning_effort = None;
    store.update_agent_session(&ws, &row).await.unwrap();
    store
        .fill_agent_session_identity_if_null(
            &ws,
            &chosen,
            None,
            Some("high"),
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    let mut row = store.get_agent_session(&chosen).await.unwrap();
    row.reasoning_effort = Some("low".into());
    store.update_agent_session(&ws, &row).await.unwrap();
    svc.agent_set_model_op(chosen.clone(), "other-model".into(), None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_agent_session(&chosen)
            .await
            .unwrap()
            .reasoning_effort
            .as_deref(),
        Some("low"),
        "a chosen level survives the switch"
    );
}
