//! Regression tests for STAB-115: settings-based default model resolution.
//!
//! Covers Bug B: model resolution precedence at creation time:
//! - workspace override > background type override > background default > provider defaults > global default
//!
//! Bug A (agent.setModel respawn) is covered by the WSS e2e test in
//! `crates/intentd/tests/e2e_wss_agent_set_model.rs`, which exercises the full
//! wire path and verifies the respawn behavior end-to-end.

use intent_core::{AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use std::sync::Arc;

use super::tests::{workspace, TempDb};
use crate::Services;

async fn setup() -> (TempDb, Services, WorkspaceId, tempfile::TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let registry = Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    let services = Services::new(store).with_settings_registry(registry);
    (tmp, services, ws, config_dir)
}

/// Seed a TOML-backed setting through the wired registry.
fn set(svc: &Services, path: &str, value: serde_json::Value) {
    svc.settings_registry()
        .expect("registry wired")
        .apply(&[(path.to_string(), value)])
        .expect("apply setting");
}

/// Seed a SQLite-backed state blob (e.g. `model.workspaceOverrides`) directly
/// in the `settings` table — these keys are not TOML-backed.
async fn set_blob(svc: &Services, path: &str, value: serde_json::Value) {
    svc.store()
        .set_setting(path, &value.to_string())
        .await
        .expect("persist state blob");
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
    let (_t, svc, ws, _cfg) = setup().await;

    // Set model.default in settings
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the session has the resolved model persisted
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}

/// Bug B: Workspace-specific overrides take precedence over global default.
#[tokio::test]
async fn agent_create_workspace_override_beats_global_default() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both global default and workspace-specific override
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    let overrides = json!({ ws.as_str(): "auggie:opus" });
    set_blob(&svc, "model.workspaceOverrides", overrides).await;

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the workspace override won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// Bug B: Background agents check backgroundAgents.defaultModel before global default.
#[tokio::test]
async fn agent_create_background_agent_uses_background_default() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both global default and background default
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    set(&svc, "backgroundAgents.defaultModel", json!("auggie:haiku"));

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
    let (_t, svc, ws, _cfg) = setup().await;

    // Set global default
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    // Create agent with explicit model
    let id = create_agent(&svc, &ws, "TestAgent", Some("auggie:opus".into())).await;

    // Verify the explicit model won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// STAB-117 extension: providerDefaults applies when nothing more specific is set.
#[tokio::test]
async fn agent_create_resolves_from_provider_defaults() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set providerDefaults only
    let provider_defaults = json!({ "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    // Create agent without explicit model or provider
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the default provider's model from providerDefaults won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("fable-5"));
}

/// STAB-117 extension: providerDefaults[provider] is used when provider is set.
#[tokio::test]
async fn agent_create_uses_provider_defaults_for_explicit_provider() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set providerDefaults for opencode
    let provider_defaults = json!({ "opencode": "kimi-k3", "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    // Create agent with explicit provider but no model
    let extra = intent_core::AgentCreateExtra {
        provider: Some("opencode".to_string()),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("TestAgent".to_string()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    // Verify the opencode provider default won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("kimi-k3"));
}

/// STAB-117 extension: more-specific settings (workspace override) beat providerDefaults.
#[tokio::test]
async fn agent_create_workspace_override_beats_provider_defaults() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both providerDefaults and workspace override
    let provider_defaults = json!({ "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    let overrides = json!({ ws.as_str(): "auggie:opus" });
    set_blob(&svc, "model.workspaceOverrides", overrides).await;

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the workspace override won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:opus"));
}

/// STAB-117 extension: background default beats providerDefaults.
#[tokio::test]
async fn agent_create_background_default_beats_provider_defaults() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both providerDefaults and background default
    let provider_defaults = json!({ "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    set(&svc, "backgroundAgents.defaultModel", json!("auggie:haiku"));

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
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    // Verify the background default won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:haiku"));
}

/// STAB-117 extension: providerDefaults beats global default.
#[tokio::test]
async fn agent_create_provider_defaults_beats_global_default() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both global default and providerDefaults
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    let provider_defaults = json!({ "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the providerDefaults won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("fable-5"));
}

/// STAB-117 extension: unknown provider key in providerDefaults falls through to global default.
#[tokio::test]
async fn agent_create_unknown_provider_falls_through_to_global() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set global default and providerDefaults with a different provider
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    let provider_defaults = json!({ "opencode": "kimi-k3" });
    set(&svc, "model.providerDefaults", provider_defaults);

    // Create agent for default provider (auggie) with no explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // Verify the global default won (auggie not in providerDefaults)
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}
