//! Regression tests for STAB-115: model changes + settings-based default resolution.
//!
//! Covers:
//! - Bug A: `agent.setModel` triggering a provider respawn on the next turn
//! - Bug B: model resolution precedence (workspace > background > global defaults)

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};

use super::tests::{workspace, TempDb};
use crate::Services;

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let services = Services::new(store);
    (tmp, services, ws)
}

async fn create_agent(
    svc: &Services,
    ws: &WorkspaceId,
    name: &str,
    model: Option<String>,
) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            model,
            None,
            None,
            None,
            false,
            None,
            Default::default(),
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

/// Bug B: When no explicit model is supplied at creation time, resolve from
/// settings `model.default` and persist to `session.model`.
#[tokio::test]
async fn agent_create_resolves_model_from_settings_default() {
    let (_t, svc, ws) = setup().await;

    // Set model.default in settings
    svc.store
        .set_setting("model.default", &json!("auggie:sonnet4.5").to_string())
        .await
        .expect("set setting");

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the session has the resolved model persisted
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}

/// Bug B: Workspace-specific overrides take precedence over global default.
#[tokio::test]
async fn agent_create_workspace_override_beats_global_default() {
    let (_t, svc, ws) = setup().await;

    // Set both global default and workspace-specific override
    svc.store
        .set_setting("model.default", &json!("auggie:sonnet4.5").to_string())
        .await
        .expect("set global");

    let overrides = json!({ ws.as_str(): "auggie:opus" });
    svc.store
        .set_setting("model.workspaceOverrides", &overrides.to_string())
        .await
        .expect("set override");

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the workspace override won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// Bug B: Background agents check backgroundAgents.defaultModel before global default.
#[tokio::test]
async fn agent_create_background_agent_uses_background_default() {
    let (_t, svc, ws) = setup().await;

    // Set both global default and background default
    svc.store
        .set_setting("model.default", &json!("auggie:sonnet4.5").to_string())
        .await
        .expect("set global");

    svc.store
        .set_setting(
            "backgroundAgents.defaultModel",
            &json!("auggie:haiku").to_string(),
        )
        .await
        .expect("set background");

    // Create background agent without explicit model
    let extra = intent_core::AgentCreateExtra {
        is_background: Some(true),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("BackgroundAgent".to_string()),
            None,
            None,
            None,
            None,
            false,
            None,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    // Verify the background default won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// Bug B: Explicit model at creation time overrides all settings.
#[tokio::test]
async fn agent_create_explicit_model_wins_over_settings() {
    let (_t, svc, ws) = setup().await;

    // Set global default
    svc.store
        .set_setting("model.default", &json!("auggie:sonnet4.5").to_string())
        .await
        .expect("set global");

    // Create agent with explicit model
    let id = create_agent(&svc, &ws, "TestAgent", Some("auggie:opus".into())).await;

    // Verify the explicit model won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}
