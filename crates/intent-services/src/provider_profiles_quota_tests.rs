//! Editing a configuration clears its shared quota state (PROTOCOL §5.54
//! Backup failover, "Shared quota state"): the saved settings may now reach
//! another account or key, so the old observation no longer applies.

use chrono::{Duration as ChronoDuration, Utc};

use super::*;

impl Fixture {
    fn cache(&self) -> &crate::provider_quota_cache::ProviderQuotaCache {
        &self.svc.provider_quota_cache
    }

    fn mark_exhausted(&self, selection: &str) {
        self.cache()
            .mark_exhausted(selection, None, Utc::now() - ChronoDuration::seconds(1));
    }

    fn is_exhausted(&self, selection: &str) -> bool {
        self.cache().is_exhausted(selection, Utc::now())
    }

    async fn profile_ref(&self, base: &str, name: &str) -> (String, String) {
        let created = self
            .create(
                base,
                name,
                json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }]),
            )
            .await;
        let id = created["profile"]["id"].as_str().unwrap().to_owned();
        (id.clone(), format!("{base}@{id}"))
    }
}

#[intent_test_macros::daemon_test]
async fn an_env_edit_clears_only_that_profiles_quota_state() {
    let fx = Fixture::new().await;
    let (id, edited) = fx.profile_ref("codex", "Work").await;
    let (_, other) = fx.profile_ref("codex", "Spare").await;
    for selection in [edited.as_str(), other.as_str(), "codex"] {
        fx.mark_exhausted(selection);
    }

    fx.svc
        .provider_profiles_update_impl(
            id,
            json!({ "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": "sk-new-key" }] }),
        )
        .await
        .expect("edit env");

    assert!(!fx.is_exhausted(&edited));
    assert!(fx.is_exhausted(&other));
    assert!(fx.is_exhausted("codex"));
}

#[intent_test_macros::daemon_test]
async fn a_working_mark_is_cleared_too() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    let now = Utc::now();
    fx.cache().mark_working(&reference, now, now);
    assert!(fx.cache().is_known_working(&reference));

    fx.svc
        .provider_profiles_update_impl(
            id,
            json!({ "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": "sk-new-key" }] }),
        )
        .await
        .expect("edit env");

    assert!(!fx.cache().is_known_working(&reference));
}

#[intent_test_macros::daemon_test]
async fn renaming_or_toggling_backup_clears_the_quota_state() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    for edit in [json!({ "name": "Work 2" }), json!({ "backup": true })] {
        fx.mark_exhausted(&reference);
        fx.svc
            .provider_profiles_update_impl(id.clone(), edit.clone())
            .await
            .expect("edit");
        assert!(!fx.is_exhausted(&reference), "{edit}");
    }
}

#[intent_test_macros::daemon_test]
async fn an_update_that_saves_nothing_keeps_the_quota_state() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    fx.mark_exhausted(&reference);

    fx.svc
        .provider_profiles_update_impl(id.clone(), json!({ "name": "Work" }))
        .await
        .expect("unchanged update");
    assert!(fx.is_exhausted(&reference));

    fx.svc
        .provider_profiles_update_impl(id, json!({ "name": "" }))
        .await
        .expect_err("invalid name");
    assert!(fx.is_exhausted(&reference));
}

#[intent_test_macros::daemon_test]
async fn editing_the_default_configuration_clears_the_bare_reference_only() {
    let fx = Fixture::new().await;
    let (_, profile) = fx.profile_ref("codex", "Work").await;
    for selection in ["codex", "claude-code", profile.as_str()] {
        fx.mark_exhausted(selection);
    }

    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
        }))
        .await
        .expect("edit default env");
    assert!(!fx.is_exhausted("codex"));
    assert!(fx.is_exhausted("claude-code"));
    assert!(fx.is_exhausted(&profile));

    fx.mark_exhausted("claude-code");
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "name": "Personal",
        }))
        .await
        .expect("rename default");
    assert!(!fx.is_exhausted("claude-code"));
}

#[intent_test_macros::daemon_test]
async fn a_default_update_that_saves_nothing_keeps_the_quota_state() {
    let fx = Fixture::new().await;
    fx.mark_exhausted("codex");
    fx.svc
        .provider_profiles_update_default_impl(json!({ "baseProviderId": "codex" }))
        .await
        .expect("unchanged default");
    assert!(fx.is_exhausted("codex"));
}

#[intent_test_macros::daemon_test]
async fn deleting_a_profile_drops_its_quota_state() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    fx.mark_exhausted(&reference);
    fx.svc
        .provider_profiles_delete_impl(id)
        .await
        .expect("delete");
    assert!(!fx.is_exhausted(&reference));
    assert!(fx.cache().exhausted(Utc::now()).is_empty());
}
