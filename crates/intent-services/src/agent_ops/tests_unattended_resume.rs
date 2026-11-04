//! Unattended resumes (the startup sweep, forced after an update restart, and
//! the wake sweep) leave an agent on a disabled or signed-out provider
//! pending instead of re-running it (intent-hq/intent#8478): re-running it
//! can only fail again, and that failure wakes the parent with a stale alert.
//! The harness's default provider is `auggie`, the provider in the report.

use std::time::Duration;

use intent_core::events::AGENT_UPDATED;
use intent_core::{now_iso, AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use tokio::time::timeout;

use super::tests::{
    create_agent, setup, setup_with_bus, test_registry_with_default_provider, workspace, TempDb,
};
use crate::test_support::fixtures::EnvGuard;
use crate::{Services, SubscriptionFilter};

/// Restores the seeded verdict to cached-unknown on drop, so a panicking test
/// cannot leave a hard-false verdict in the process-wide cache.
struct AuthSeedReset(&'static str);
impl Drop for AuthSeedReset {
    fn drop(&mut self) {
        crate::provider_auth::seed_auth_verdict_for_tests(self.0, None);
    }
}

async fn interrupted_agent(svc: &Services, ws: &WorkspaceId) -> AgentId {
    let id = create_agent(svc, ws, "Interrupted on auggie").await;
    svc.store
        .insert_interrupted_agent(&id, ws, "active", &now_iso())
        .await
        .unwrap();
    id
}

async fn user_messages(svc: &Services, id: &AgentId) -> usize {
    svc.store
        .get_agent_messages(id, None)
        .await
        .unwrap()
        .iter()
        .filter(|m| m.role == "user")
        .count()
}

#[intent_test_macros::daemon_test]
async fn startup_resume_leaves_signed_out_provider_pending() {
    let _env = EnvGuard::apply(&[]);
    let _reset = AuthSeedReset("auggie");
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = interrupted_agent(&svc, &ws).await;
    crate::provider_auth::seed_auth_verdict_for_tests("auggie", Some(false));
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string()],
        ..Default::default()
    });

    let candidates = svc.prepare_startup_resume().await.unwrap();
    let err = svc
        .resume_startup_candidate(&candidates, &id)
        .await
        .expect_err("a signed-out provider must not be resumed unattended");
    let message = err.to_string();
    assert!(
        message.contains("left pending for manual resume") && message.contains("auggie login"),
        "names the hold and the login remedy: {message}"
    );
    assert!(
        svc.store
            .get_interrupted_agent(&id)
            .await
            .unwrap()
            .is_some(),
        "the row stays pending for a manual retry"
    );
    assert_eq!(
        user_messages(&svc, &id).await,
        0,
        "no continuation is delivered to the held agent"
    );
    // The reservation hid the row from `agent.listInterrupted`; this update is
    // what makes a window that listed during the sweep show it again.
    let batch = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("the held agent must publish a recovery-failed update")
    .expect("subscription closed");
    assert_eq!(batch[0].workspace_id, ws);
    assert_eq!(batch[0].data["agentId"].as_str(), Some(id.0.as_str()));
    assert_eq!(batch[0].data["startupRecoveryFailed"], json!(true));
}

#[intent_test_macros::daemon_test]
async fn startup_resume_leaves_disabled_provider_pending() {
    let _env = EnvGuard::apply(&[]);
    let _reset = AuthSeedReset("auggie");
    let (_t, svc, ws) = setup().await;
    let id = interrupted_agent(&svc, &ws).await;
    crate::provider_auth::seed_auth_verdict_for_tests("auggie", Some(true));
    svc.settings_registry()
        .expect("registry")
        .apply(&[("providers.enabled".into(), json!({ "auggie": false }))])
        .expect("disable auggie");

    let candidates = svc.prepare_startup_resume().await.unwrap();
    let err = svc
        .resume_startup_candidate(&candidates, &id)
        .await
        .expect_err("a disabled provider must not be resumed unattended");
    assert!(
        err.to_string().contains("is not enabled"),
        "names the disabled provider: {err}"
    );
    assert!(svc
        .store
        .get_interrupted_agent(&id)
        .await
        .unwrap()
        .is_some());
}

/// Only an explicit signed-out verdict holds: signed in and unknown (an
/// inconclusive probe) both let the unattended resume proceed.
#[intent_test_macros::daemon_test]
async fn unattended_resume_hold_is_permissive_unless_signed_out() {
    let _env = EnvGuard::apply(&[]);
    let _reset = AuthSeedReset("auggie");
    let (_t, svc, ws) = setup().await;
    let id = interrupted_agent(&svc, &ws).await;

    crate::provider_auth::seed_auth_verdict_for_tests("auggie", Some(true));
    assert_eq!(svc.unattended_resume_hold(&id).await, None, "signed in");
    crate::provider_auth::seed_auth_verdict_for_tests("auggie", None);
    assert_eq!(svc.unattended_resume_hold(&id).await, None, "unknown");
    crate::provider_auth::seed_auth_verdict_for_tests("auggie", Some(false));
    assert!(
        svc.unattended_resume_hold(&id).await.is_some(),
        "signed out holds"
    );
    assert_eq!(
        svc.unattended_resume_hold(&AgentId::from("agent-missing"))
            .await,
        None,
        "an unreadable session never holds"
    );
}

#[intent_test_macros::daemon_test]
async fn wake_resume_skips_signed_out_provider() {
    let _env = EnvGuard::apply(&[]);
    let _reset = AuthSeedReset("auggie");
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let svc = Services::new_isolated(store)
        .with_settings_registry(test_registry_with_default_provider(&tmp));
    let id = create_agent(&svc, &ws, "Suspended on auggie").await;
    svc.store
        .set_acp_session_id(&ws, &id, "suspend-session")
        .await
        .unwrap();
    svc.store
        .insert_interrupted_agent_with_reason(
            &id,
            &ws,
            "active",
            &now_iso(),
            Some("system_suspend"),
        )
        .await
        .unwrap();
    crate::provider_auth::seed_auth_verdict_for_tests("auggie", Some(false));

    assert_eq!(svc.resume_suspend_interrupted_agents().await, 0);
    assert!(
        svc.store
            .get_interrupted_agent(&id)
            .await
            .unwrap()
            .is_some(),
        "the row stays pending for a manual retry"
    );
    assert_eq!(user_messages(&svc, &id).await, 0);
}
