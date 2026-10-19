//! Unknown-model rejection at the create seams (intent-hq/intent#7121).
//!
//! An explicit `model` that the resolved provider's cached catalog provably
//! does not offer is rejected with `-32602` naming the valid models, before
//! any session row is persisted. The rule is evidence-only, like the
//! `reasoningEffort` and cross-provider ownership guards: with no usable
//! cached catalog the model passes through, and a catalog row carrying a
//! bracketed variant suffix (`opus[1m]`) still admits its bare alias.

use intent_core::{AgentCreateExtra, AgentDelegateInput, AgentWakeOrCreateInput, Error};
use serde_json::json;

use super::tests::{seed_task, setup};
use crate::Services;

fn seed_auggie(svc: &Services) {
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "sonnet4.5", "name": "Sonnet 4.5", "provider": "auggie" }),
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" }),
        ],
    );
}

fn seed_claude_code(svc: &Services) {
    svc.models_catalog.store_for_test(
        "claude-code",
        &crate::model_catalog::source_for("claude-code")
            .map(|s| (s.version_key)())
            .unwrap_or_default(),
        vec![
            json!({ "id": "default", "name": "Default", "provider": "claude-code" }),
            json!({ "id": "opus[1m]", "name": "Opus", "provider": "claude-code" }),
            json!({ "id": "sonnet", "name": "Sonnet", "provider": "claude-code" }),
        ],
    );
}

async fn create(
    svc: &Services,
    ws: &intent_core::WorkspaceId,
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

async fn session_count(svc: &Services, ws: &intent_core::WorkspaceId) -> usize {
    svc.store()
        .list_agent_sessions(ws)
        .await
        .expect("list sessions")
        .len()
}

fn assert_unknown_model(err: &Error, method: &str, model: &str, provider: &str, valid: &[&str]) {
    assert!(matches!(err, Error::InvalidParams(_)), "got: {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains(&format!(
            "{method}: model {model} is not available for provider {provider}"
        )),
        "unexpected err: {msg}"
    );
    for v in valid {
        assert!(msg.contains(v), "valid model {v} must be named: {msg}");
    }
}

#[tokio::test]
async fn create_rejects_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    seed_auggie(&svc);
    let err = create(&svc, &ws, "no-such-model-xyz", None)
        .await
        .expect_err("unknown model must be rejected");
    assert_unknown_model(
        &err,
        "agent.create",
        "no-such-model-xyz",
        "auggie",
        &["sonnet4.5", "fable-5"],
    );
    let err = create(&svc, &ws, "no-such-model-xyz", Some("auggie"))
        .await
        .expect_err("unknown model with explicit provider must be rejected");
    assert_unknown_model(
        &err,
        "agent.create",
        "no-such-model-xyz",
        "auggie",
        &["sonnet4.5"],
    );
    assert_eq!(
        session_count(&svc, &ws).await,
        0,
        "no agent row may be created"
    );
}

#[tokio::test]
async fn create_accepts_known_model_default_sentinel_and_aliases() {
    let (_t, svc, ws) = setup().await;
    seed_auggie(&svc);
    seed_claude_code(&svc);
    let created = create(&svc, &ws, "fable-5", None)
        .await
        .expect("known model");
    assert_eq!(created["agent"]["model"], json!("fable-5"));
    create(&svc, &ws, "default", None)
        .await
        .expect("the default sentinel always passes");
    for model in ["sonnet", "opus", "opus[1m]", "sonnet[1m]"] {
        let created = create(&svc, &ws, model, Some("claude-code"))
            .await
            .unwrap_or_else(|e| panic!("alias {model} must pass: {e}"));
        assert_eq!(created["agent"]["model"], json!(model));
    }
    let err = create(&svc, &ws, "opusplan-xyz", Some("claude-code"))
        .await
        .expect_err("unknown claude-code model must be rejected");
    assert_unknown_model(
        &err,
        "agent.create",
        "opusplan-xyz",
        "claude-code",
        &["opus[1m]", "sonnet"],
    );
}

#[tokio::test]
async fn create_passes_through_without_catalog_evidence() {
    let (_t, svc, ws) = setup().await;
    let created = create(&svc, &ws, "no-such-model-xyz", None)
        .await
        .expect("cold catalog must not block creation");
    assert_eq!(created["agent"]["model"], json!("no-such-model-xyz"));
    // An empty cached catalog is no evidence either.
    svc.models_catalog.store_for_test(
        "auggie",
        crate::model_catalog::AUGGIE_CATALOG_VERSION,
        vec![],
    );
    create(&svc, &ws, "another-unknown", None)
        .await
        .expect("empty catalog must not block creation");
    // A stale-pin entry (version-key mismatch) is no evidence.
    svc.models_catalog.store_for_test(
        "auggie",
        "stale-version-key",
        vec![json!({ "id": "sonnet4.5", "provider": "auggie" })],
    );
    create(&svc, &ws, "third-unknown", None)
        .await
        .expect("stale catalog must not block creation");
}

#[tokio::test]
async fn delegate_rejects_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    seed_auggie(&svc);
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
    assert_unknown_model(
        &err,
        "agent.create",
        "no-such-model-xyz",
        "auggie",
        &["fable-5"],
    );
    assert_eq!(
        session_count(&svc, &ws).await,
        0,
        "no child row may be created"
    );
}

#[intent_test_macros::daemon_test]
async fn wake_or_create_rejects_unknown_model_and_persists_nothing() {
    let (_t, svc, ws) = setup().await;
    seed_auggie(&svc);
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
        .expect_err("wakeOrCreate with unknown model must be rejected");
    assert!(matches!(err, Error::InvalidParams(_)), "got: {err:?}");
    assert!(
        err.to_string()
            .contains("model no-such-model-xyz is not available"),
        "unexpected err: {err}"
    );
    assert_eq!(
        session_count(&svc, &ws).await,
        0,
        "no agent row may be created"
    );
}
