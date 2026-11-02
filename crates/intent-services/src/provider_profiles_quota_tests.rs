//! Editing a configuration clears its shared quota state (PROTOCOL §5.54
//! Backup failover, "Shared quota state"), and a process still running on
//! the settings from before the edit cannot mark it again.

use chrono::{Duration as ChronoDuration, Utc};
use intent_core::{AgentId, WorkspaceId};

use super::*;

impl Fixture {
    fn cache(&self) -> &crate::provider_quota_cache::ProviderQuotaCache {
        &self.svc.provider_quota_cache
    }

    fn mark_exhausted(&self, selection: &str) {
        self.cache().mark_exhausted(
            selection,
            None,
            Utc::now() - ChronoDuration::seconds(1),
            || true,
        );
    }

    /// A live provider process for a new agent, started under `reference`
    /// with settings `fingerprint` (driven like `create_agent` does).
    fn process(&self, reference: &str, fingerprint: &str) -> AgentId {
        let agent = AgentId::new();
        self.svc
            .profile_activation()
            .register(&WorkspaceId::new(), &agent, reference, fingerprint)
            .expect("profile-capable reference");
        agent
    }

    /// `agent`'s process hit its quota on `reference`.
    fn report_quota_failure(&self, agent: &AgentId, reference: &str) {
        self.svc
            .note_provider_exhausted(agent, reference, None, Utc::now());
    }

    /// `agent`'s process produced output on an attempt on `reference`.
    fn report_output(&self, agent: &AgentId, reference: &str) {
        self.svc.arm_quota_failover(agent, false);
        self.svc.note_quota_failover_attempt(agent, reference, None);
        self.svc.note_provider_working(agent);
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
    fx.cache().mark_working(&reference, now, now, || true);
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

#[intent_test_macros::daemon_test]
async fn a_process_on_the_earlier_settings_cannot_mark_an_edited_profile_again() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    let old = fx.process(&reference, "f-before-edit");
    fx.mark_exhausted(&reference);

    let updated = fx
        .svc
        .provider_profiles_update_impl(
            id,
            json!({ "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": "sk-new-key" }] }),
        )
        .await
        .expect("edit env");

    fx.report_quota_failure(&old, &reference);
    assert!(!fx.is_exhausted(&reference));
    fx.report_output(&old, &reference);
    assert!(!fx.cache().is_known_working(&reference));

    let saved = updated["profile"]["settingsFingerprint"].as_str().unwrap();
    let fresh = fx.process(&reference, saved);
    fx.report_quota_failure(&fresh, &reference);
    assert!(fx.is_exhausted(&reference));
}

#[intent_test_macros::daemon_test]
async fn a_process_on_the_earlier_settings_cannot_mark_an_edited_default_again() {
    let fx = Fixture::new().await;
    let old = fx.process("codex", "");
    fx.mark_exhausted("codex");

    let updated = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
        }))
        .await
        .expect("edit default env");

    fx.report_quota_failure(&old, "codex");
    assert!(!fx.is_exhausted("codex"));
    fx.report_output(&old, "codex");
    assert!(!fx.cache().is_known_working("codex"));

    let saved = updated["defaultConfig"]["settingsFingerprint"]
        .as_str()
        .unwrap();
    let fresh = fx.process("codex", saved);
    fx.report_quota_failure(&fresh, "codex");
    assert!(fx.is_exhausted("codex"));
}

/// A rename keeps the account and key, so a running process still reports
/// on the same quota.
#[intent_test_macros::daemon_test]
async fn a_running_process_still_marks_a_renamed_profile() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    let running = fx.process(&reference, "f-current");
    fx.svc
        .provider_profiles_update_impl(id, json!({ "name": "Work 2" }))
        .await
        .expect("rename");

    fx.report_quota_failure(&running, &reference);
    assert!(fx.is_exhausted(&reference));
}

#[intent_test_macros::daemon_test]
async fn a_process_of_a_deleted_profile_cannot_mark_it_again() {
    let fx = Fixture::new().await;
    let (id, reference) = fx.profile_ref("codex", "Work").await;
    let old = fx.process(&reference, "f-current");
    fx.svc
        .provider_profiles_delete_impl(id)
        .await
        .expect("delete");

    fx.report_quota_failure(&old, &reference);
    assert!(fx.cache().exhausted(Utc::now()).is_empty());
}
