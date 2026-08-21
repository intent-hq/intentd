//! Regression tests for STAB-115: settings-based default model resolution.
//!
//! Covers Bug B: model resolution precedence at creation time:
//! - provider defaults > global default
//!
//! (The per-workspace override tier was removed in monorepo#1000; a stale
//! `model.workspaceOverrides` SQLite row must be ignored. The background-agent
//! tier was removed in monorepo#1729 — the renamed `quickActions.*` keys scope
//! to single-shot quick actions and never to agent sessions.)
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

/// Seed a SQLite-backed state blob directly in the `settings` table — used to
/// plant a stale retired-key row for the regression test below.
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
            intent_core::AgentCreateExtra::default(),
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

/// monorepo#1000 regression: a stale `model.workspaceOverrides` SQLite row
/// (left behind by a pre-removal daemon) must NOT influence resolution — the
/// retired tier is gone, so the chain proceeds to the live tiers.
#[tokio::test]
async fn agent_create_ignores_stale_workspace_override_row() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Plant a stale override row for this workspace plus a global default.
    set(&svc, "model.default", json!("auggie:sonnet4.5"));
    let overrides = json!({ ws.as_str(): "auggie:opus" });
    set_blob(&svc, "model.workspaceOverrides", overrides).await;

    // Create agent without explicit model
    let id = create_agent(&svc, &ws, "TestAgent", None).await;

    // The stale override is ignored; the global default wins.
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}

/// monorepo#1729: the quick-action default is scoped to single-shot quick
/// actions and must never pin a background agent's model — the global default
/// wins.
#[tokio::test]
async fn agent_create_background_agent_ignores_quick_action_default() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both global default and quick-action default
    set(&svc, "model.default", json!("auggie:sonnet4.5"));

    set(&svc, "quickActions.defaultModel", json!("auggie:haiku"));

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

    // Verify the global default won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
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

/// monorepo#1729 (issue repro): with a quick-action default set, a background
/// agent still resolves `model.providerDefaults` — the quick-action model
/// never sits above the provider default for a session.
#[tokio::test]
async fn agent_create_background_resolves_provider_defaults_over_quick_action() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Set both providerDefaults and quick-action default
    let provider_defaults = json!({ "auggie": "fable-5" });
    set(&svc, "model.providerDefaults", provider_defaults);

    set(&svc, "quickActions.defaultModel", json!("auggie:haiku"));

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

    // Verify the provider default won
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("fable-5"));
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

/// monorepo#607 refinement: a *settings-derived* bare default whose ownership
/// by the requested provider is disproven by cached catalogs must not
/// hard-fail `agent.create` — the caller never sent the model. It falls back
/// to the CLI default (`session.model = None`) instead; only a
/// client-supplied mismatch rejects.
#[tokio::test]
async fn agent_create_mismatched_settings_default_falls_back_to_cli_default() {
    let (_t, svc, ws, _cfg) = setup().await;

    // Warm caches: auggie claims sonnet4.5, grok's catalog lacks it.
    let now = crate::model_catalog::ModelCatalogCache::now_ms();
    svc.models_catalog.test_store(
        "auggie",
        "",
        vec![serde_json::json!({ "id": "sonnet4.5", "name": "Sonnet 4.5", "provider": "auggie" })],
        now,
    );
    svc.models_catalog.test_store(
        "grok",
        "",
        vec![serde_json::json!({ "id": "grok-4-fast", "name": "Grok 4 Fast", "provider": "grok" })],
        now,
    );

    // Global default is a bare auggie model.
    set(&svc, "model.default", json!("sonnet4.5"));

    // Explicit grok provider, no model param: creation succeeds and the
    // mismatched settings default is dropped, not persisted or rejected.
    let extra = intent_core::AgentCreateExtra {
        provider: Some("grok".to_string()),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("GrokNoModel".to_string()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create must not fail on a settings-derived mismatch");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(
        got.model, None,
        "mismatched settings default must fall back to the CLI default"
    );

    // The same bare model sent explicitly by the client still rejects.
    let extra = intent_core::AgentCreateExtra {
        provider: Some("grok".to_string()),
        ..Default::default()
    };
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("GrokExplicit".to_string()),
            Some("sonnet4.5".to_string()),
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect_err("explicit bare mismatch must still reject");
    assert!(
        err.to_string()
            .contains("model sonnet4.5 does not belong to provider grok"),
        "unexpected err: {err}"
    );

    // A matching provider keeps consuming the settings default as before.
    let id = create_agent(&svc, &ws, "AuggieDefault", None).await;
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.model.as_deref(), Some("sonnet4.5"));
}
