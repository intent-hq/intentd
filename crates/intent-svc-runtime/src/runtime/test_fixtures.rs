//! Fixtures the runtime's tests share: copies of core `test_support`
//! helpers that stay `cfg(test)` there because they need dev-only crates
//! (`intentd_test_support::test_timeout` for hang guards, `sqlx` for raw
//! seeding), which core's `test-hooks` build cannot use. Keep each copy in
//! step with the original it names.

#[cfg(unix)]
use std::time::Duration;

use intent_core::AgentId;
#[cfg(unix)]
use serde_json::Value;

use intent_services::feature_api::Services;
#[cfg(unix)]
use intent_services::feature_api::Subscription;

/// Copy of `test_support::goldens::wake_texts_when`.
///
/// All persisted message texts for an agent (each message's text blocks
/// joined), oldest-first — the wake-delivery capture used by the goldens.
/// Wake persistence can lag the op return, so poll until at least `expected`
/// messages are present (generous deadline, monorepo#1358 precedent); on
/// timeout return whatever was seen and let the caller's assert report it.
pub(crate) async fn wake_texts_when(svc: &Services, id: &AgentId, expected: usize) -> Vec<String> {
    let deadline = std::time::Instant::now()
        + intentd_test_support::test_timeout(std::time::Duration::from_secs(10));
    loop {
        let session = svc.store().get_agent_session(id).await.expect("session");
        let texts: Vec<String> = session
            .messages
            .iter()
            .map(|m| {
                m.content
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            })
            .collect();
        if texts.len() >= expected || std::time::Instant::now() >= deadline {
            return texts;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[cfg(unix)]
/// Copy of `test_support::setup_lifecycle::recv_setup`.
///
/// Receive one setup event (generous timeout: script paths cross a PTY
/// spawn + reap poll), serialized to wire JSON.
pub(crate) async fn recv_setup(sub: &mut Subscription) -> Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        sub.recv(),
    )
    .await
    .expect("setup event delivered")
    .expect("subscription open");
    assert_eq!(batch.len(), 1, "expected exactly one event per delivery");
    serde_json::to_value(&batch[0]).expect("serialize event")
}

#[cfg(unix)]
/// Copy of `test_support::setup_lifecycle::assert_quiet`.
pub(crate) async fn assert_quiet(sub: &mut Subscription) {
    let quiet = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(300)),
        sub.recv(),
    )
    .await;
    assert!(quiet.is_err(), "no further setup events, got: {quiet:?}");
}

/// Copy of `test_support::capability_fixtures::host_role` (`cfg(test)` there:
/// seeding the host member needs the dev-only `sqlx`).
pub(crate) mod host_role {
    use intent_core::{now_iso, Caller, Principal, PrincipalId};
    use intent_store::Store;

    use intent_services::feature_api::test_support::fixtures::TempDb;
    use intent_services::feature_api::Services;

    pub(crate) async fn fixture(tmp: &TempDb) -> (Services, PrincipalId, PrincipalId) {
        let store = Store::open(&tmp.path).await.unwrap();
        let owner = store.get_primary_principal().await.unwrap();
        assert!(owner.identity_key().is_none(), "no forge account is needed");
        let member = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&member).await.unwrap();
        sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
            .bind(&member.id.0)
            .bind(now_iso())
            .execute(store.write_pool())
            .await
            .unwrap();
        let root = tmp.path.parent().unwrap().join("workspaces");
        (
            crate::test_runtime::with_runtime(Services::new(store)).with_workspaces_root(root),
            owner.id,
            member.id,
        )
    }

    #[must_use]
    pub(crate) fn caller(id: &PrincipalId) -> Caller {
        Caller::Wire {
            principal_id: id.clone(),
            host_role: intent_core::HostRole::Member,
        }
    }
}
