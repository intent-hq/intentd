//! A child created by an agent without an explicit provider inherits its
//! creator's provider ahead of `model.defaultProvider` (intent-hq/intent#8479).
//! The report: a coordinator on Claude Code delegated, the child landed on
//! the settings default (Auggie) the user was not signed in to, and failed.
//! The harness's settings default is `auggie`; the parent runs `claude-code`.

use std::sync::Arc;

use intent_core::{
    AgentCreateExtra, AgentDelegateInput, AgentId, BatchTaskEntry, NoteCreate, NoteId,
    WorkspaceApi as _, WorkspaceId,
};
use intent_store::Store;
use serde_json::json;

use super::tests::{setup, test_registry_with_default_provider, workspace, TempDb};
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
/// "Delegation"): a parent on a saved, enabled `claude-code` profile passes
/// nothing on, and the child resolves the settings default exactly as
/// before. The profile is real, so without the rule it would be inherited.
#[intent_test_macros::daemon_test]
async fn parent_profile_is_never_inherited() {
    let _reset = AuthSeedReset("claude-code");
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let data = tempfile::tempdir().expect("data dir");
    let svc = Services::new_isolated(store)
        .with_settings_registry(test_registry_with_default_provider(&tmp))
        .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()))
        .with_provider_profiles_dir(data.path());
    let created = svc
        .provider_profiles_create_impl(
            json!({ "baseProviderId": "claude-code", "name": "Work", "env": [] }),
        )
        .await
        .expect("save a claude-code profile");
    let profile_ref = created["profile"]["providerRef"]
        .as_str()
        .expect("providerRef")
        .to_string();
    let parent = parent_on_claude_code(&svc, &ws).await;
    svc.store()
        .set_agent_session_model(
            &ws,
            &parent,
            "sonnet",
            Some(&profile_ref),
            &intent_core::now_iso(),
        )
        .await
        .expect("park the parent on the saved profile");

    assert_eq!(delegate(&svc, &ws, Some(&parent), None).await, "auggie");
}

/// An explicit model the parent's provider cannot resolve keeps the old
/// resolution (the settings default) instead of inheriting a provider the
/// create front door would then reject.
#[intent_test_macros::daemon_test]
async fn explicit_model_the_parent_provider_cannot_offer_keeps_the_default() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;
    svc.models_catalog.store_for_test(
        "claude-code",
        &crate::model_catalog::source_for("claude-code")
            .map(|s| (s.version_key)())
            .unwrap_or_default(),
        vec![json!({ "id": "sonnet", "name": "Sonnet", "provider": "claude-code" })],
    );

    assert_eq!(
        delegate(&svc, &ws, Some(&parent), Some("not-a-claude-model")).await,
        "auggie"
    );
}

/// The specialist's `codingAgent` still beats the parent's provider.
#[intent_test_macros::daemon_test]
async fn specialist_provider_beats_the_parent_provider() {
    let _reset = AuthSeedReset("claude-code");
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let specialists = tempfile::tempdir().expect("specialists dir");
    std::fs::write(
        specialists.path().join("codex-specialist.md"),
        "---\nname: \"codex-specialist\"\ndescription: \"Test\"\ncodingAgent: \"codex\"\n---\n\nTest prompt",
    )
    .expect("write specialist");
    let svc = Services::new_isolated(store)
        .with_settings_registry(test_registry_with_default_provider(&tmp))
        .with_specialist_dirs(
            Some(specialists.path().to_path_buf()),
            Some(specialists.path().to_path_buf()),
        );
    let parent = parent_on_claude_code(&svc, &ws).await;
    crate::test_support::provider_installed::mark_on(&svc, &["claude-code", "codex"]);

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                specialist: Some("codex-specialist".into()),
                ..Default::default()
            },
            Some(parent),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert_eq!(provider_of(&svc, &child).await, "codex");
}

async fn seed_task(svc: &Services, ws: &WorkspaceId, title: &str) -> NoteId {
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: title.into(),
                content: Some(format!("{title} body")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "not_started".into(),
        vec![],
        None,
        None,
        None,
        None,
    )
    .await
    .expect("markAsTask");
    note.id
}

/// Batch delegation from an agent inherits the same way as a single one.
#[intent_test_macros::daemon_test]
async fn batch_delegated_children_inherit_the_parent_provider() {
    let _reset = AuthSeedReset("claude-code");
    let (_t, svc, ws) = setup().await;
    let parent = parent_on_claude_code(&svc, &ws).await;
    let tasks = [
        seed_task(&svc, &ws, "First").await,
        seed_task(&svc, &ws, "Second").await,
    ];

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                tasks: Some(tasks.iter().cloned().map(BatchTaskEntry::Id).collect()),
                ..Default::default()
            },
            Some(parent),
        )
        .await
        .expect("batch delegate");
    let rows = resp["tasks"].as_array().expect("tasks array");
    assert_eq!(rows.len(), 2, "{resp}");
    for row in rows {
        assert_eq!(row["disposition"], "started", "{resp}");
        let child = AgentId::from(row["agentId"].as_str().expect("agentId"));
        assert_eq!(provider_of(&svc, &child).await, "claude-code");
    }
}

/// The create front door gates a Default with saved overrides on its
/// overlay's verdict, not the bare install's: a parent on bare `claude-code`
/// whose overridden Default is cached signed out is not inherited, so the
/// child falls back to the settings default instead of failing the call.
#[intent_test_macros::daemon_test]
async fn signed_out_overridden_default_is_not_inherited() {
    let _reset = AuthSeedReset("claude-code");
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let data = tempfile::tempdir().expect("data dir");
    let svc = Services::new_isolated(store)
        .with_settings_registry(test_registry_with_default_provider(&tmp))
        .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()))
        .with_provider_profiles_dir(data.path());
    svc.provider_profiles_update_default_impl(json!({
        "baseProviderId": "claude-code",
        "env": [{ "key": "ANTHROPIC_API_KEY", "kind": "set", "value": "sk-test-work" }],
    }))
    .await
    .expect("save Default overrides");
    assert!(svc.default_has_overrides("claude-code"));
    let parent = parent_on_claude_code(&svc, &ws).await;
    crate::provider_auth::seed_auth_verdict_for_tests("claude-code", Some(true));
    let overlay = svc
        .resolve_profile_env("claude-code")
        .await
        .expect("resolve overlay")
        .expect("saved overlay");
    crate::provider_auth::demote_profile_auth_verdict(&overlay);

    assert_eq!(delegate(&svc, &ws, Some(&parent), None).await, "auggie");
}
