//! Unknown-model rejection at the create seams (intent-hq/intent#7121).
//!
//! A caller-supplied `model` that the resolved provider provably cannot
//! resolve is rejected with `-32602` naming the valid models, before any
//! session row is persisted. "Provably" means the provider's cached catalog
//! is warm AND the provider's own resolution rules are known: Claude Code's
//! adapter resolves aliases, full model ids and family names against its
//! picker rows (`resolveModelPreference`), so the guard mirrors that. Every
//! other provider, and every cold/empty/stale catalog, passes through. A
//! model inherited from stored state (wakeOrCreate) is never re-validated.

use intent_core::{
    AgentCreateExtra, AgentDelegateInput, AgentWakeOrCreateInput, Error, WorkspaceApi, WorkspaceId,
};
use serde_json::json;

use super::tests::{seed_task, setup};
use crate::Services;

/// Make claude-code the settings default and runnable in tests.
fn use_claude_code(svc: &Services) {
    crate::test_support::provider_installed::mark_on(svc, &["claude-code"]);
    svc.settings_registry()
        .expect("registry")
        .apply(&[("model.defaultProvider".into(), json!("claude-code"))])
        .expect("apply settings");
}

/// Warm claude-code catalog under its current version key.
fn seed_claude_code(svc: &Services, rows: Vec<serde_json::Value>) {
    svc.models_catalog.store_for_test(
        "claude-code",
        &crate::model_catalog::source_for("claude-code")
            .map(|s| (s.version_key)())
            .unwrap_or_default(),
        rows,
    );
}

/// Alias-only picker rows (no version metadata), as the adapter lists them.
fn alias_rows() -> Vec<serde_json::Value> {
    vec![
        json!({ "id": "opus[1m]", "name": "Opus", "provider": "claude-code" }),
        json!({ "id": "sonnet", "name": "Sonnet", "provider": "claude-code" }),
        json!({ "id": "haiku", "name": "Haiku", "provider": "claude-code" }),
    ]
}

async fn create(
    svc: &Services,
    ws: &WorkspaceId,
    model: &str,
    provider: Option<&str>,
) -> crate::Result<serde_json::Value> {
    svc.agent_create_op(
        ws.clone(),
        Some("Agent".into()),
        Some(model.to_string()),
        None,
        None,
        None,
        false,
        AgentCreateExtra {
            provider: provider.map(str::to_string),
            ..Default::default()
        },
    )
    .await
}

async fn sessions(svc: &Services, ws: &WorkspaceId) -> Vec<intent_core::AgentSession> {
    svc.store()
        .list_agent_sessions(ws)
        .await
        .expect("list sessions")
}

fn assert_unknown_model(err: &Error, method: &str, model: &str, provider: &str) {
    assert!(matches!(err, Error::InvalidParams(_)), "got: {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains(&format!(
            "{method}: model {model} is not available for provider {provider}"
        )),
        "unexpected err: {msg}"
    );
    for valid in ["opus[1m]", "sonnet", "haiku"] {
        assert!(
            msg.contains(valid),
            "valid model {valid} must be named: {msg}"
        );
    }
}

#[tokio::test]
async fn create_rejects_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    seed_claude_code(&svc, alias_rows());
    let err = create(&svc, &ws, "no-such-model-xyz", None)
        .await
        .expect_err("unknown model must be rejected");
    assert_unknown_model(&err, "agent.create", "no-such-model-xyz", "claude-code");
    let err = create(&svc, &ws, "no-such-model-xyz", Some("claude-code"))
        .await
        .expect_err("unknown model with explicit provider must be rejected");
    assert_unknown_model(&err, "agent.create", "no-such-model-xyz", "claude-code");
    assert!(
        sessions(&svc, &ws).await.is_empty(),
        "no agent row may be created"
    );
}

/// Numeric context-hint-shaped ids reject with `-32602` rather than panicking
/// in the suffix scan (intent-hq/intent#7121 review).
#[tokio::test]
async fn create_rejects_numeric_suffix_ids_without_panicking() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    seed_claude_code(&svc, alias_rows());
    for model in ["123m", "123M", "12345", "日本語"] {
        let err = create(&svc, &ws, model, None)
            .await
            .expect_err("a numeric id the adapter cannot resolve must be rejected");
        assert_unknown_model(&err, "agent.create", model, "claude-code");
    }
    assert!(
        sessions(&svc, &ws).await.is_empty(),
        "no agent row may be created"
    );
}

/// Names the pinned adapter resolves against alias-only picker rows pass:
/// exact ids, bare aliases of bracketed rows, full Claude model ids, and the
/// `opusplan` family alias. The stored model is the caller's spelling.
#[tokio::test]
async fn create_accepts_adapter_resolvable_names() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    seed_claude_code(&svc, alias_rows());
    for model in [
        "default",
        "sonnet",
        "opus",
        "opus[1m]",
        "Sonnet",
        "claude-sonnet-4-5",
        "claude-opus-4-1",
        "claude-haiku-4-5-20251001",
        "opusplan",
    ] {
        let created = create(&svc, &ws, model, Some("claude-code"))
            .await
            .unwrap_or_else(|e| panic!("{model} must pass: {e}"));
        assert_eq!(created["agent"]["model"], json!(model));
    }
}

#[tokio::test]
async fn create_passes_through_without_catalog_evidence() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    create(&svc, &ws, "no-such-model-xyz", None)
        .await
        .expect("cold catalog must not block creation");
    seed_claude_code(&svc, vec![]);
    create(&svc, &ws, "another-unknown", None)
        .await
        .expect("empty catalog must not block creation");
    svc.models_catalog
        .store_for_test("claude-code", "stale-version-key", alias_rows());
    create(&svc, &ws, "third-unknown", None)
        .await
        .expect("stale catalog must not block creation");
}

/// Providers whose model-resolution rules are not mirrored pass through even
/// with a warm catalog: rejecting could refuse ids the provider accepts.
#[tokio::test]
async fn create_passes_through_for_providers_without_known_resolution() {
    let (_t, svc, ws) = setup().await;
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![json!({ "id": "sonnet4.5", "name": "Sonnet 4.5", "provider": "auggie" })],
    );
    let created = create(&svc, &ws, "no-such-model-xyz", None)
        .await
        .expect("auggie (the default provider here) passes through");
    assert_eq!(created["agent"]["model"], json!("no-such-model-xyz"));
}

#[tokio::test]
async fn delegate_rejects_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    seed_claude_code(&svc, alias_rows());
    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                model: Some("no-such-model-xyz".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect_err("delegate with unknown model must be rejected");
    assert_unknown_model(&err, "agent.create", "no-such-model-xyz", "claude-code");
    assert!(
        sessions(&svc, &ws).await.is_empty(),
        "no child row may be created"
    );
}

#[intent_test_macros::daemon_test]
async fn wake_or_create_rejects_explicit_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    seed_claude_code(&svc, alias_rows());
    let note_id = seed_task(&svc, &ws, "Model check").await;
    let err = svc
        .agent_wake_or_create_op(
            ws.clone(),
            note_id,
            "continue".into(),
            AgentWakeOrCreateInput {
                model: Some("no-such-model-xyz".into()),
                ..Default::default()
            },
        )
        .await
        .expect_err("wakeOrCreate with an explicit unknown model must be rejected");
    assert_unknown_model(&err, "agent.create", "no-such-model-xyz", "claude-code");
    assert!(
        sessions(&svc, &ws).await.is_empty(),
        "no agent row may be created"
    );
}

/// Coordinator decision on intent-hq/intent#7121: a model inherited from
/// stored state is never re-validated, so a later catalog change cannot block
/// waking an existing task.
#[intent_test_macros::daemon_test]
async fn wake_or_create_keeps_an_inherited_delisted_model() {
    let (_t, svc, ws) = setup().await;
    use_claude_code(&svc);
    let note_id = seed_task(&svc, &ws, "Inherit").await;
    // Created while the catalog was cold, so the model was accepted then.
    let prev = create(&svc, &ws, "legacy-model-q", None)
        .await
        .expect("create prev on a cold catalog");
    let prev_id = prev["agent"]["id"].as_str().unwrap().to_string();
    svc.assign_agent(ws.clone(), note_id.clone(), prev_id.clone(), None)
        .await
        .expect("assign prev");
    let mut prev_session = svc
        .store()
        .get_agent_session(&intent_core::AgentId::from(prev_id.as_str()))
        .await
        .expect("load prev");
    prev_session.status = intent_core::AgentStatus::Deleted;
    prev_session.updated_at = intent_core::now_iso();
    svc.store()
        .update_agent_session(&prev_session.workspace_id.clone(), &prev_session)
        .await
        .expect("mark prev deleted");
    // The catalog warms up without the inherited model.
    seed_claude_code(&svc, alias_rows());

    svc.agent_wake_or_create_op(
        ws.clone(),
        note_id,
        "continue".into(),
        AgentWakeOrCreateInput::default(),
    )
    .await
    .expect("an inherited delisted model must not block the wake");
    let replacement = sessions(&svc, &ws)
        .await
        .into_iter()
        .find(|s| s.id.0 != prev_id)
        .expect("a replacement agent was created");
    assert_eq!(replacement.model.as_deref(), Some("legacy-model-q"));
}
