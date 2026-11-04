//! A child created by an agent without an explicit provider inherits its
//! creator's provider ahead of `model.defaultProvider` (intent-hq/intent#8479).
//! The report: a coordinator on Claude Code delegated, the child landed on
//! the settings default (Auggie) the user was not signed in to, and failed.
//! The harness's settings default is `auggie`; the parent runs `claude-code`.

use intent_core::{AgentCreateExtra, AgentDelegateInput, AgentId, WorkspaceId};
use serde_json::json;

use super::tests::setup;
use crate::Services;

/// Restores the seeded verdict to cached-unknown on drop.
struct AuthSeedReset(&'static str);
impl Drop for AuthSeedReset {
    fn drop(&mut self) {
        crate::provider_auth::seed_auth_verdict_for_tests(self.0, None);
    }
}

async fn parent_on_claude_code(svc: &Services, ws: &WorkspaceId) -> AgentId {
    crate::test_support::provider_installed::mark_on(svc, &["claude-code"]);
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Coordinator".into()),
            None,
            None,
            None,
            None,
            false,
            AgentCreateExtra {
                provider: Some("claude-code".into()),
                ..Default::default()
            },
        )
        .await
        .expect("create parent");
    AgentId::from(created["agent"]["id"].as_str().expect("parent id"))
}

async fn delegate(
    svc: &Services,
    ws: &WorkspaceId,
    parent: Option<&AgentId>,
    model: Option<&str>,
) -> String {
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                model: model.map(str::to_string),
                ..Default::default()
            },
            parent.cloned(),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    provider_of(svc, &child).await
}

async fn provider_of(svc: &Services, id: &AgentId) -> String {
    svc.store()
        .get_agent_session_summary(id)
        .await
        .expect("child session")
        .provider
        .expect("every new session pins a provider")
}

#[intent_test_macros::daemon_test]
async fn delegated_child_inherits_the_parent_provider() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;

    assert_eq!(
        delegate(&svc, &ws, Some(&parent), None).await,
        "claude-code"
    );
    assert_eq!(
        delegate(&svc, &ws, None, None).await,
        "auggie",
        "a parentless delegate still uses model.defaultProvider"
    );
}

#[intent_test_macros::daemon_test]
async fn created_sub_agent_inherits_the_parent_provider() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;

    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Helper".into()),
            None,
            None,
            Some(parent),
            None,
            false,
            AgentCreateExtra::default(),
        )
        .await
        .expect("create sub-agent");
    let child = AgentId::from(created["agent"]["id"].as_str().expect("child id"));
    assert_eq!(provider_of(&svc, &child).await, "claude-code");
}

/// A parent provider the front door would refuse is skipped, never an error:
/// signed out (cached hard-false verdict) or disabled after the parent was
/// created both fall back to the settings default.
#[intent_test_macros::daemon_test]
async fn unusable_parent_provider_falls_back_to_the_settings_default() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;

    crate::provider_auth::seed_auth_verdict_for_tests("claude-code", Some(false));
    assert_eq!(
        delegate(&svc, &ws, Some(&parent), None).await,
        "auggie",
        "signed-out parent provider"
    );

    crate::provider_auth::seed_auth_verdict_for_tests("claude-code", None);
    svc.settings_registry()
        .expect("registry")
        .apply(&[("providers.enabled".into(), json!({ "claude-code": false }))])
        .expect("disable claude-code");
    assert_eq!(
        delegate(&svc, &ws, Some(&parent), None).await,
        "auggie",
        "disabled parent provider"
    );
}

/// With an explicit model the parent's provider is kept only when it can run
/// that model: a model another provider's cached catalog owns falls back to
/// the settings default, as before.
#[intent_test_macros::daemon_test]
async fn explicit_model_inherits_only_a_provider_that_owns_it() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;
    for (provider, rows) in [
        (
            "claude-code",
            vec![json!({ "id": "sonnet", "name": "Sonnet", "provider": "claude-code" })],
        ),
        (
            "auggie",
            vec![json!({ "id": "auggie-only", "name": "Auggie only", "provider": "auggie" })],
        ),
    ] {
        svc.models_catalog.store_for_test(
            provider,
            &crate::model_catalog::source_for(provider)
                .map(|s| (s.version_key)())
                .unwrap_or_default(),
            rows,
        );
    }

    assert_eq!(
        delegate(&svc, &ws, Some(&parent), Some("sonnet")).await,
        "claude-code"
    );
    assert_eq!(
        delegate(&svc, &ws, Some(&parent), Some("auggie-only")).await,
        "auggie"
    );
}

/// A child never inherits its parent's named profile (PROTOCOL §5.54
/// "Delegation"): a parent on `claude-code@work` passes nothing on, and the
/// child resolves the settings default exactly as before.
#[intent_test_macros::daemon_test]
async fn parent_profile_is_never_inherited() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;
    svc.store()
        .set_agent_session_model(
            &ws,
            &parent,
            "sonnet",
            Some("claude-code@work"),
            &intent_core::now_iso(),
        )
        .await
        .expect("park the parent on a profile");

    assert_eq!(delegate(&svc, &ws, Some(&parent), None).await, "auggie");
}
