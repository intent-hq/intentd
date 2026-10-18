//! Selection routing for named provider profiles (PROTOCOL §5.54): the
//! catalog offers profiles, and every selection path — defaults, specialist
//! pins and options, create, setModel, delegation, quick actions, model
//! discovery — carries the profile reference unchanged. A missing profile or
//! a disabled base provider needs an explicit replacement and never falls
//! back to the base's Default account or the app default.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::{
    AgentCreateExtra, AgentDelegateInput, AgentId, Error, WorkspaceApi, WorkspaceId,
};
use intent_store::Store;
use serde_json::{json, Value};

use crate::settings::InMemorySecretStore;
use crate::test_support::test_tempdir;
use crate::tests::TempDb;
use crate::{Services, SettingsRegistry};

struct Fixture {
    _db: TempDb,
    root: tempfile::TempDir,
    svc: Services,
    ws: WorkspaceId,
}

impl Fixture {
    async fn new() -> Self {
        let root = test_tempdir("provider-profiles-routing-");
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&crate::tests::workspace(&ws))
            .await
            .expect("workspace");
        let data = root.path().join("data");
        let specialists = root.path().join("specialists");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&specialists).unwrap();
        let registry =
            Arc::new(SettingsRegistry::load(data.join("config.toml")).expect("load registry"));
        let svc = Services::new(store)
            .with_settings_registry(registry)
            .with_secret_store(Arc::new(InMemorySecretStore::default()))
            .with_specialist_dirs(Some(specialists.clone()), Some(specialists))
            .with_provider_profiles_dir(&data);
        let fx = Self {
            _db: db,
            root,
            svc,
            ws,
        };
        // Deterministic availability for both bases: an executable override
        // (no provider process is launched — Services has no agent manager).
        fx.set(
            "providers.paths",
            json!({ "codex": "/bin/sh", "claude-code": "/bin/sh" }),
        );
        fx
    }

    fn specialists(&self) -> PathBuf {
        self.root.path().join("specialists")
    }

    /// Apply a setting directly, bypassing `settings.update` reference
    /// validation (simulates a stale or hand-edited reference).
    fn set(&self, path: &str, value: Value) {
        self.svc
            .settings_registry()
            .expect("registry")
            .apply(&[(path.to_string(), value)])
            .expect("apply setting");
    }

    async fn profile(&self, base: &str, name: &str) -> String {
        let created = self
            .svc
            .provider_profiles_create_impl(json!({
                "baseProviderId": base,
                "name": name,
                "env": [{ "key": "PROFILE_ROUTING_TOKEN", "kind": "set", "value": name }],
            }))
            .await
            .expect("create profile");
        created["profile"]["providerRef"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn create(&self, provider: Option<&str>) -> crate::Result<Value> {
        self.svc
            .agent_create_op(
                self.ws.clone(),
                None,
                None,
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

    async fn delegate(
        &self,
        input: AgentDelegateInput,
        parent: Option<&AgentId>,
    ) -> crate::Result<Value> {
        self.svc
            .agent_delegate_op(
                self.ws.clone(),
                AgentDelegateInput {
                    agent_instructions: Some("do the thing".into()),
                    ..input
                },
                parent.cloned(),
            )
            .await
    }

    async fn provider_of(&self, response: &Value) -> Option<String> {
        let id = agent_id(response);
        self.svc
            .store()
            .get_agent_session(&id)
            .await
            .expect("session")
            .provider
    }
}

/// The agent id of a create (`agent.id`) or delegate (`agentId`) response.
fn agent_id(response: &Value) -> AgentId {
    AgentId::from(
        response["agentId"]
            .as_str()
            .or_else(|| response["agent"]["id"].as_str())
            .unwrap_or_else(|| panic!("agent id in {response}")),
    )
}

fn write_specialist(dir: &Path, id: &str, frontmatter: &str) {
    std::fs::write(
        dir.join(format!("{id}.md")),
        format!("---\nname: \"{id}\"\ndescription: \"d\"\n{frontmatter}\n---\n\nBody."),
    )
    .unwrap();
}

fn replacement(err: &Error) -> (String, String) {
    let Error::ProviderProfile(failure) = err else {
        panic!("expected provider-profile-replacement-required, got {err:?}");
    };
    assert_eq!(err.code(), -32602, "{err:?}");
    assert_eq!(
        failure.data["code"], "provider-profile-replacement-required",
        "{err:?}"
    );
    (
        failure.data["providerRef"].as_str().unwrap().to_string(),
        failure.data["reason"].as_str().unwrap().to_string(),
    )
}

#[intent_test_macros::daemon_test]
async fn catalog_offers_profiles_and_rename_changes_only_the_label() {
    let fx = Fixture::new().await;
    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert_eq!(catalog["profiles"], json!([]));
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    let claude = fx.profile("claude-code", "Work").await;

    let catalog = fx.svc.providers_catalog().await.unwrap();
    let rows = catalog["providers"].as_array().unwrap();
    assert_eq!(
        rows.len(),
        intent_providers::ACP_PROVIDERS.len(),
        "one row per provider type"
    );
    let profiles = catalog["profiles"].as_array().unwrap();
    let refs: Vec<&str> = profiles
        .iter()
        .map(|p| p["providerRef"].as_str().unwrap())
        .collect();
    assert_eq!(refs, [work.as_str(), personal.as_str(), claude.as_str()]);
    for p in profiles {
        let mut keys: Vec<&String> = p.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(keys, ["baseProviderId", "id", "name", "providerRef"]);
    }
    let rendered = catalog.to_string();
    assert!(!rendered.contains("PROFILE_ROUTING_TOKEN"), "{rendered}");

    // Rename: label changes, identity and routing do not.
    let agent = fx.create(Some(&work)).await.unwrap();
    let id = work.split_once('@').unwrap().1.to_string();
    fx.svc
        .provider_profiles_update_impl(id.clone(), json!({ "name": "Work (renamed)" }))
        .await
        .unwrap();
    let catalog = fx.svc.providers_catalog().await.unwrap();
    let row = catalog["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == id.as_str())
        .unwrap()
        .clone();
    assert_eq!(row["name"], "Work (renamed)");
    assert_eq!(row["providerRef"], work.as_str());
    assert_eq!(fx.provider_of(&agent).await.as_deref(), Some(work.as_str()));
    assert!(fx.svc.ensure_provider_ref_selectable(&work).is_ok());
}

#[intent_test_macros::daemon_test]
async fn create_persists_the_reference_and_missing_or_disabled_requires_replacement() {
    let fx = Fixture::new().await;
    let work = fx.profile("codex", "Work").await;
    let created = fx.create(Some(&work)).await.unwrap();
    assert_eq!(
        fx.provider_of(&created).await.as_deref(),
        Some(work.as_str())
    );

    for missing in ["codex@pnotsaved", "codex@BAD", "auggie@p1", "codex@p1@p2"] {
        let err = fx.create(Some(missing)).await.unwrap_err();
        assert_eq!(
            replacement(&err),
            (missing.to_string(), "profile-missing".to_string())
        );
    }

    fx.set("providers.enabled", json!({ "codex": false }));
    let err = fx.create(Some(&work)).await.unwrap_err();
    assert_eq!(
        replacement(&err),
        (work.clone(), "provider-disabled".to_string())
    );
    // Legacy bare selection of a disabled provider: today's error, unchanged.
    let legacy = fx.create(Some("codex")).await.unwrap_err();
    assert!(matches!(legacy, Error::InvalidParams(_)), "{legacy:?}");
    assert!(legacy.to_string().contains("is not enabled"), "{legacy}");
}

#[intent_test_macros::daemon_test]
async fn default_provider_reference_is_used_and_never_falls_back() {
    let fx = Fixture::new().await;
    let work = fx.profile("claude-code", "Work").await;
    fx.svc
        .settings_update_checking_profiles(
            json!([{ "path": "model.defaultProvider", "value": work }]),
        )
        .await
        .unwrap();
    assert_eq!(
        crate::agent_session::derived_default_provider(&fx.svc.effective_settings()).as_deref(),
        Some(work.as_str())
    );
    let created = fx.create(None).await.unwrap();
    let effective = crate::agent_session::resolve_provider_id(
        fx.provider_of(&created).await.as_deref(),
        crate::agent_session::derived_default_provider(&fx.svc.effective_settings()).as_deref(),
    );
    assert_eq!(effective.as_deref(), Some(work.as_str()));

    // A stale reference (its profile is gone) is kept, then rejected — never
    // read as unset, never run on the base's Default account.
    fx.set("model.defaultProvider", json!("claude-code@pgone"));
    assert_eq!(
        crate::agent_session::derived_default_provider(&fx.svc.effective_settings()).as_deref(),
        Some("claude-code@pgone")
    );
    let err = fx.create(None).await.unwrap_err();
    assert_eq!(replacement(&err).1, "profile-missing");
    let err = fx
        .delegate(AgentDelegateInput::default(), None)
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).0, "claude-code@pgone");
}

#[intent_test_macros::daemon_test]
async fn provider_defaults_walk_the_reference_then_its_base() {
    let fx = Fixture::new().await;
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    fx.svc
        .settings_update_checking_profiles(json!([{
            "path": "model.providerDefaults",
            "value": { "codex": "base-model", work.clone(): "work-model" },
        }]))
        .await
        .unwrap();
    let resolve = |provider: &str| {
        crate::agent_ops::resolve_agent_default_model(&fx.svc, None, None, Some(provider))
    };
    assert_eq!(resolve(&work).as_deref(), Some("work-model"));
    assert_eq!(resolve(&personal).as_deref(), Some("base-model"));
    assert_eq!(resolve("codex").as_deref(), Some("base-model"));
    let created = fx.create(Some(&work)).await.unwrap();
    let session = fx
        .svc
        .store()
        .get_agent_session(&agent_id(&created))
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("work-model"));
}

#[intent_test_macros::daemon_test]
async fn set_model_switches_profiles_and_rejects_missing_references() {
    let fx = Fixture::new().await;
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    let created = fx.create(Some("codex")).await.unwrap();
    let agent = agent_id(&created);
    for target in [&work, &personal, &"codex".to_string()] {
        fx.svc
            .agent_set_model_op(agent.clone(), "same-model".into(), Some(target.clone()))
            .await
            .unwrap();
        let session = fx.svc.store().get_agent_session(&agent).await.unwrap();
        assert_eq!(session.provider.as_deref(), Some(target.as_str()));
    }
    let err = fx
        .svc
        .agent_set_model_op(
            agent.clone(),
            "same-model".into(),
            Some("codex@pgone".into()),
        )
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).1, "profile-missing");
    let session = fx.svc.store().get_agent_session(&agent).await.unwrap();
    assert_eq!(
        session.provider.as_deref(),
        Some("codex"),
        "nothing changed"
    );
}

#[intent_test_macros::daemon_test]
async fn delegation_keeps_explicit_then_specialist_then_default_without_inheritance() {
    let fx = Fixture::new().await;
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    let claude_work = fx.profile("claude-code", "Work").await;
    fx.set("model.defaultProvider", json!("codex"));
    write_specialist(
        &fx.specialists(),
        "pinned",
        &format!("codingAgent: \"{personal}\""),
    );

    // The parent runs on Work; children never inherit it.
    let parent = fx.create(Some(&work)).await.unwrap();
    let parent = agent_id(&parent);
    let child = fx
        .delegate(AgentDelegateInput::default(), Some(&parent))
        .await
        .unwrap();
    assert_eq!(fx.provider_of(&child).await.as_deref(), Some("codex"));

    // Specialist pin wins over the app default.
    let child = fx
        .delegate(
            AgentDelegateInput {
                specialist: Some("pinned".into()),
                ..Default::default()
            },
            Some(&parent),
        )
        .await
        .unwrap();
    assert_eq!(
        fx.provider_of(&child).await.as_deref(),
        Some(personal.as_str())
    );

    // Explicit selection wins over the specialist pin, across providers too,
    // and no account is inferred from a matching profile name.
    let child = fx
        .delegate(
            AgentDelegateInput {
                specialist: Some("pinned".into()),
                provider: Some(claude_work.clone()),
                ..Default::default()
            },
            Some(&parent),
        )
        .await
        .unwrap();
    assert_eq!(
        fx.provider_of(&child).await.as_deref(),
        Some(claude_work.as_str())
    );

    // The app default can itself be a profile.
    fx.set("model.defaultProvider", json!(claude_work));
    let child = fx
        .delegate(AgentDelegateInput::default(), Some(&parent))
        .await
        .unwrap();
    let effective = crate::agent_session::resolve_provider_id(
        fx.provider_of(&child).await.as_deref(),
        Some(claude_work.as_str()),
    );
    assert_eq!(effective.as_deref(), Some(claude_work.as_str()));

    // A missing specialist pin or explicit reference fails clearly.
    write_specialist(&fx.specialists(), "stale", "codingAgent: \"codex@pgone\"");
    let err = fx
        .delegate(
            AgentDelegateInput {
                specialist: Some("stale".into()),
                ..Default::default()
            },
            Some(&parent),
        )
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).0, "codex@pgone");
    let err = fx
        .delegate(
            AgentDelegateInput {
                provider: Some("codex@pgone".into()),
                ..Default::default()
            },
            Some(&parent),
        )
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).0, "codex@pgone");
}

#[intent_test_macros::daemon_test]
async fn quick_actions_on_a_missing_profile_require_replacement() {
    let fx = Fixture::new().await;
    fx.set("model.defaultProvider", json!("codex@pgone"));
    let err = fx
        .svc
        .agent_complete_once_op("hi".into(), None, None, None, None, Some(1_000), None)
        .await
        .unwrap_err();
    assert_eq!(
        replacement(&err),
        ("codex@pgone".to_string(), "profile-missing".to_string())
    );
}

#[intent_test_macros::daemon_test]
async fn models_list_for_a_reference_requires_a_saved_profile() {
    let fx = Fixture::new().await;
    let err = fx
        .svc
        .models_list_op(Some("codex@pgone".into()), false)
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).1, "profile-missing");
}

#[test]
fn selection_identity_separates_profiles_and_default() {
    use crate::provider_profiles::canonical_selection;
    assert_eq!(canonical_selection("codex"), "codex");
    assert_eq!(
        canonical_selection("acp"),
        intent_providers::provider_config("acp").id
    );
    assert_eq!(canonical_selection("codex@pwork"), "codex@pwork");
    assert_ne!(
        canonical_selection("codex@pwork"),
        canonical_selection("codex")
    );
    assert_ne!(
        canonical_selection("codex@pwork"),
        canonical_selection("codex@ppersonal")
    );
}

#[intent_test_macros::daemon_test]
async fn profile_auth_status_reports_each_reference_and_requires_a_saved_one() {
    let fx = Fixture::new().await;
    assert_eq!(
        fx.svc
            .provider_profiles_auth_status(None, false)
            .await
            .unwrap(),
        json!([])
    );
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    let swept = fx
        .svc
        .provider_profiles_auth_status(None, false)
        .await
        .unwrap();
    let ids: Vec<&str> = swept
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [work.as_str(), personal.as_str()]);
    let one = fx
        .svc
        .provider_profiles_auth_status(Some(work.clone()), false)
        .await
        .unwrap();
    assert_eq!(one.as_array().unwrap().len(), 1);
    assert_eq!(one[0]["id"], work.as_str());
    assert!(!one.to_string().contains("PROFILE_ROUTING_TOKEN"));
    let err = fx
        .svc
        .provider_profiles_auth_status(Some("codex@pgone".into()), false)
        .await
        .unwrap_err();
    assert_eq!(replacement(&err).1, "profile-missing");
}

/// Explicit delegation onto a profile observed signed out fails fast with the
/// login remedy, while the base's Default configuration and another profile
/// stay selectable (the profile's own verdict gates; never the base's).
#[intent_test_macros::daemon_test]
async fn explicit_delegation_checks_the_profiles_own_auth_verdict() {
    let fx = Fixture::new().await;
    let work = fx.profile("codex", "Work").await;
    let personal = fx.profile("codex", "Personal").await;
    let env = fx.svc.resolve_profile_env(&work).await.unwrap().unwrap();
    crate::provider_auth::demote_profile_auth_verdict(&env);
    let delegate = |provider: &str| AgentDelegateInput {
        provider: Some(provider.to_string()),
        ..Default::default()
    };
    let err = fx.delegate(delegate(&work), None).await.unwrap_err();
    assert!(err.to_string().contains("not authenticated"), "{err}");
    for ok in [personal.as_str(), "codex"] {
        let child = fx.delegate(delegate(ok), None).await.unwrap();
        assert_eq!(fx.provider_of(&child).await.as_deref(), Some(ok));
    }
}

/// `agent.sendMessage` on a conversation whose next turn would be refused
/// is rejected at admission with the structured replacement error, before
/// anything is persisted, so the client's send-refusal path runs. The prose
/// never names the raw reference (it is only in `data.providerRef`).
#[intent_test_macros::daemon_test]
async fn send_message_is_refused_at_admission_when_a_replacement_is_required() {
    for base in ["codex", "claude-code"] {
        let fx = Fixture::new().await;
        let send = |agent: AgentId| {
            let svc = fx.svc.clone();
            let ws = fx.ws.clone();
            async move {
                svc.agent_send_message(
                    ws,
                    agent,
                    "next turn".into(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    intent_core::MessageOrigin::User,
                )
                .await
            }
        };
        let transcript_len = |agent: AgentId| {
            let svc = fx.svc.clone();
            async move {
                svc.store()
                    .get_agent_messages(&agent, None)
                    .await
                    .unwrap()
                    .len()
            }
        };

        // Removed profile.
        let gone = fx.profile(base, "Gone").await;
        let on_gone = agent_id(&fx.create(Some(&gone)).await.unwrap());
        let profile_id = gone.split('@').nth(1).unwrap().to_string();
        fx.svc
            .provider_profiles_delete_impl(profile_id)
            .await
            .expect("delete");
        let before = transcript_len(on_gone.clone()).await;
        let err = send(on_gone.clone()).await.unwrap_err();
        assert_eq!(
            replacement(&err),
            (gone.clone(), "profile-missing".into()),
            "{base}"
        );
        assert!(!err.to_string().contains(&gone), "{base}: {err}");
        assert_eq!(
            transcript_len(on_gone).await,
            before,
            "{base}: nothing persisted"
        );

        // Disabled base provider.
        let kept = fx.profile(base, "Kept").await;
        let on_kept = agent_id(&fx.create(Some(&kept)).await.unwrap());
        fx.set("providers.enabled", json!({ base: false }));
        let err = send(on_kept.clone()).await.unwrap_err();
        assert_eq!(
            replacement(&err),
            (kept.clone(), "provider-disabled".into()),
            "{base}"
        );
        assert!(!err.to_string().contains(&kept), "{base}: {err}");

        // Legacy bare selection on a disabled provider keeps today's path.
        fx.set("providers.enabled", json!({ base: true }));
        let legacy = agent_id(&fx.create(Some(base)).await.unwrap());
        fx.set("providers.enabled", json!({ base: false }));
        send(legacy).await.expect("legacy send is admitted");
        fx.set("providers.enabled", json!({ base: true }));

        // A conversation with no saved provider resolves through the app
        // default; a missing default reference is refused the same way.
        let unpinned = agent_id(&fx.create(Some(base)).await.unwrap());
        fx.svc
            .store()
            .set_agent_session_model(&fx.ws, &unpinned, "m", None, &intent_core::now_iso())
            .await
            .unwrap();
        fx.set("model.defaultProvider", json!(format!("{base}@pmissing")));
        let err = send(unpinned).await.unwrap_err();
        assert_eq!(
            replacement(&err),
            (format!("{base}@pmissing"), "profile-missing".into()),
            "{base}"
        );
    }
}
