//! Unit tests for the Default configuration's name and environment
//! overrides (PROTOCOL §5.54 "Default configuration overrides").

use super::*;

impl Fixture {
    async fn update_default(&self, params: Value) -> Result<Value> {
        self.svc.provider_profiles_update_default_impl(params).await
    }

    fn file(&self) -> Value {
        serde_json::from_str(&std::fs::read_to_string(self.profiles_file()).unwrap()).unwrap()
    }

    fn default_env_id(&self, base: &str) -> Option<String> {
        self.file()["defaults"][base]["envId"]
            .as_str()
            .map(str::to_owned)
    }

    fn secret(&self, env_id: &str) -> Option<String> {
        SecretStore::load(&*self.secrets, &env_account(env_id)).unwrap()
    }

    async fn defaults_row(&self, base: &str) -> Value {
        let listed = self.svc.provider_profiles_list_impl(None).await.unwrap();
        listed["defaults"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["baseProviderId"] == base)
            .cloned()
            .expect("a defaults row per profile base")
    }
}

#[intent_test_macros::daemon_test]
async fn list_reports_every_unrenamed_default_with_no_entries() {
    let fx = Fixture::new().await;
    let listed = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    let expected: Vec<Value> = PROFILE_BASE_PROVIDERS
        .iter()
        .map(|base| {
            json!({
                "baseProviderId": base,
                "providerRef": base,
                "name": "Default",
                "env": [],
                "settingsFingerprint": "",
                "activation": { "runningProcesses": 0 },
            })
        })
        .collect();
    assert_eq!(listed["defaults"], Value::Array(expected));
    let filtered = fx
        .svc
        .provider_profiles_list_impl(Some("codex".into()))
        .await
        .unwrap();
    assert_eq!(filtered["defaults"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["defaults"][0]["baseProviderId"], "codex");
    // Nothing saved: the bare reference launches exactly as before.
    assert!(fx.svc.resolve_profile_env("codex").await.unwrap().is_none());
    assert!(!fx.profiles_file().exists() || fx.file().get("defaults").is_none());
}

#[intent_test_macros::daemon_test]
async fn update_default_renames_masks_and_applies_to_the_bare_reference() {
    let fx = Fixture::new().await;
    let mut sub = fx.subscribe();
    let result = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "name": "  Personal ",
            "env": [
                { "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET },
                { "key": "EMPTY", "kind": "set", "value": "" },
                { "key": "OPENAI_BASE_URL", "kind": "unset" },
            ],
        }))
        .await
        .unwrap();
    let config = &result["defaultConfig"];
    assert_eq!(config["baseProviderId"], "codex");
    assert_eq!(config["providerRef"], "codex");
    assert_eq!(config["name"], "Personal");
    assert_eq!(
        config["env"],
        json!([
            { "key": "OPENAI_API_KEY", "kind": "set", "value": MASK },
            { "key": "EMPTY", "kind": "set", "value": "" },
            { "key": "OPENAI_BASE_URL", "kind": "unset" },
        ])
    );
    let fingerprint = config["settingsFingerprint"].as_str().unwrap().to_owned();
    assert!(!fingerprint.is_empty());
    assert_eq!(result["activation"]["envChanged"], true);
    assert!(!result.to_string().contains(SECRET));

    let event = recv_profiles_event(&mut sub).await;
    assert_eq!(event["data"]["change"], "default-updated");
    assert_eq!(event["data"]["baseProviderId"], "codex");
    assert_eq!(event["data"]["envChanged"], true);
    assert_eq!(event["data"]["revision"], result["revision"]);
    assert!(event["data"].get("profileId").is_none());
    assert!(event["data"].get("profile").is_none());
    assert!(!event.to_string().contains(SECRET));

    let overlay = fx
        .svc
        .resolve_profile_env("codex")
        .await
        .unwrap()
        .expect("Default overrides resolve for the bare reference");
    assert_eq!(overlay.provider_ref(), "codex");
    assert_eq!(overlay.fingerprint(), fingerprint);
    assert_eq!(
        entries_of(&overlay),
        vec![
            ("OPENAI_API_KEY".into(), Some(SECRET.into())),
            ("EMPTY".into(), Some(String::new())),
            ("OPENAI_BASE_URL".into(), None),
        ]
    );
    // The other base keeps today's behavior.
    assert!(fx
        .svc
        .resolve_profile_env("claude-code")
        .await
        .unwrap()
        .is_none());

    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert_eq!(catalog["defaultNames"], json!({ "codex": "Personal" }));
    assert_eq!(fx.defaults_row("codex").await, *config);

    // Values stay out of the profile file; only the name and envId land.
    let file = std::fs::read_to_string(fx.profiles_file()).unwrap();
    assert!(!file.contains(SECRET), "{file}");
    let env_id = fx.default_env_id("codex").expect("envId recorded");
    assert!(is_valid_profile_id(&env_id));
    assert!(fx.secret(&env_id).unwrap().contains(SECRET));
}

#[intent_test_macros::daemon_test]
async fn blank_or_default_names_restore_the_unrenamed_default() {
    let fx = Fixture::new().await;
    fx.update_default(json!({ "baseProviderId": "claude-code", "name": "Team" }))
        .await
        .unwrap();
    for name in ["   ", "default", "DEFAULT"] {
        fx.update_default(json!({ "baseProviderId": "claude-code", "name": "Team" }))
            .await
            .unwrap();
        let result = fx
            .update_default(json!({ "baseProviderId": "claude-code", "name": name }))
            .await
            .unwrap();
        assert_eq!(result["defaultConfig"]["name"], "Default", "{name:?}");
        assert_eq!(result["activation"]["envChanged"], false);
        let catalog = fx.svc.providers_catalog().await.unwrap();
        assert!(catalog.get("defaultNames").is_none(), "{name:?}: {catalog}");
        assert!(fx.file().get("defaults").is_none(), "{name:?}");
    }
    // Omitting `name` keeps the saved one.
    fx.update_default(json!({ "baseProviderId": "claude-code", "name": "Team" }))
        .await
        .unwrap();
    let kept = fx
        .update_default(json!({ "baseProviderId": "claude-code", "env": [] }))
        .await
        .unwrap();
    assert_eq!(kept["defaultConfig"]["name"], "Team");
}

#[intent_test_macros::daemon_test]
async fn default_and_profile_names_cannot_collide_within_one_base() {
    let fx = Fixture::new().await;
    let work = fx.create("codex", "Work", json!([])).await;
    let work_id = work["profile"]["id"].as_str().unwrap().to_owned();
    let err = fx
        .update_default(json!({ "baseProviderId": "codex", "name": "WORK" }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("name".into(), "name-duplicate".into())]
    );

    fx.update_default(json!({ "baseProviderId": "codex", "name": "Personal" }))
        .await
        .unwrap();
    let err = fx
        .svc
        .provider_profiles_create_impl(json!({ "baseProviderId": "codex", "name": "personal" }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("name".into(), "name-duplicate".into())]
    );
    let err = fx
        .svc
        .provider_profiles_update_impl(work_id, json!({ "name": " PERSONAL " }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("name".into(), "name-duplicate".into())]
    );
    // Another base provider may reuse the name.
    fx.create("claude-code", "Personal", json!([])).await;
    // Overlong names are invalid.
    let err = fx
        .update_default(json!({ "baseProviderId": "codex", "name": "x".repeat(65) }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("name".into(), "name-invalid".into())]
    );
}

#[intent_test_macros::daemon_test]
async fn update_default_validates_like_profiles_and_keeps_masked_entries() {
    let fx = Fixture::new().await;
    let err = fx
        .update_default(json!({ "baseProviderId": "auggie", "name": "x" }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("baseProviderId".into(), "base-provider-unsupported".into())]
    );
    let err = fx.update_default(json!({ "name": "x" })).await.unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");

    let err = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [
                { "key": "PATH", "kind": "set", "value": "/x" },
                { "key": "MISSING", "kind": "keep" },
                { "key": "OPENAI_API_KEY", "kind": "set", "value": MASK },
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![
            ("env[0].key".into(), "env-key-reserved".into()),
            ("env[1].kind".into(), "env-keep-missing".into()),
            ("env[2].value".into(), "env-value-placeholder".into()),
        ]
    );
    assert!(!fx.profiles_file().exists() || fx.default_env_id("codex").is_none());

    let first = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    let kept = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "keep" }],
        }))
        .await
        .unwrap();
    assert_eq!(kept["activation"]["envChanged"], false);
    assert_eq!(
        kept["revision"], first["revision"],
        "a no-op commits nothing"
    );
    assert_eq!(
        kept["defaultConfig"]["settingsFingerprint"],
        first["defaultConfig"]["settingsFingerprint"]
    );
    let overlay = fx.svc.resolve_profile_env("codex").await.unwrap().unwrap();
    assert_eq!(
        entries_of(&overlay),
        vec![("OPENAI_API_KEY".into(), Some(SECRET.into()))]
    );
}

#[intent_test_macros::daemon_test]
async fn env_changes_mint_a_new_secret_and_remove_the_previous_one() {
    let fx = Fixture::new().await;
    fx.update_default(json!({
        "baseProviderId": "claude-code",
        "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-a" }],
    }))
    .await
    .unwrap();
    let first = fx.default_env_id("claude-code").unwrap();
    let before = fx
        .svc
        .resolve_profile_env("claude-code")
        .await
        .unwrap()
        .unwrap();

    fx.update_default(json!({
        "baseProviderId": "claude-code",
        "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-b" }],
    }))
    .await
    .unwrap();
    let second = fx.default_env_id("claude-code").unwrap();
    assert_ne!(first, second);
    assert!(fx.secret(&first).is_none(), "previous secret removed");
    assert!(fx.secret(&second).is_some());
    assert!(
        fx.file().get("pendingEnvCleanup").is_none(),
        "{}",
        fx.file()
    );
    let after = fx
        .svc
        .resolve_profile_env("claude-code")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(before.fingerprint(), after.fingerprint());
    assert_ne!(before.cache_key(), after.cache_key());

    // An empty list removes every override, the envId and its secret.
    let cleared = fx
        .update_default(json!({ "baseProviderId": "claude-code", "env": [] }))
        .await
        .unwrap();
    assert_eq!(cleared["activation"]["envChanged"], true);
    assert_eq!(cleared["defaultConfig"]["settingsFingerprint"], "");
    assert_eq!(cleared["defaultConfig"]["env"], json!([]));
    assert!(fx.secret(&second).is_none());
    assert!(fx.file().get("defaults").is_none(), "{}", fx.file());
    assert!(fx
        .svc
        .resolve_profile_env("claude-code")
        .await
        .unwrap()
        .is_none());
}

#[intent_test_macros::daemon_test]
async fn default_overrides_persist_across_restart_and_survive_the_startup_sweep() {
    let fx = Fixture::new().await;
    fx.update_default(json!({
        "baseProviderId": "codex",
        "name": "Personal",
        "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
    }))
    .await
    .unwrap();
    let env_id = fx.default_env_id("codex").unwrap();
    // An interrupted replacement journals the live id and an orphan.
    SecretStore::store(&*fx.secrets, &env_account("dorphan"), "{}").unwrap();
    let mut file = fx.file();
    file["pendingEnvCleanup"] = json!(["dorphan", env_id]);
    std::fs::write(fx.profiles_file(), file.to_string()).unwrap();

    let fx = fx.restart().await;
    fx.svc.sweep_provider_profile_secrets().await;
    assert!(fx.secret("dorphan").is_none());
    assert!(
        fx.secret(&env_id).is_some(),
        "the current Default secret is kept"
    );
    assert!(fx.file().get("pendingEnvCleanup").is_none());
    let row = fx.defaults_row("codex").await;
    assert_eq!(row["name"], "Personal");
    assert_eq!(row["env"][0]["value"], MASK);
    let overlay = fx.svc.resolve_profile_env("codex").await.unwrap().unwrap();
    assert_eq!(
        entries_of(&overlay),
        vec![("OPENAI_API_KEY".into(), Some(SECRET.into()))]
    );
}

#[intent_test_macros::daemon_test]
async fn files_without_defaults_load_and_hand_edited_default_names_normalize() {
    let fx = Fixture::new().await;
    fx.create("codex", "Work", json!([])).await;
    let mut file = fx.file();
    assert!(file.get("defaults").is_none(), "{file}");
    file["defaults"] = json!({ "codex": { "name": "default" }, "claude-code": { "name": "Team" } });
    std::fs::write(fx.profiles_file(), file.to_string()).unwrap();
    let fx = fx.restart().await;
    assert_eq!(fx.defaults_row("codex").await["name"], "Default");
    assert_eq!(fx.defaults_row("claude-code").await["name"], "Team");
    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert_eq!(catalog["defaultNames"], json!({ "claude-code": "Team" }));
}

#[intent_test_macros::daemon_test]
async fn an_unreadable_default_environment_fails_closed() {
    let fx = Fixture::new().await;
    fx.update_default(json!({
        "baseProviderId": "codex",
        "env": [{ "key": "CODEX_HOME", "kind": "set", "value": "/work" }],
    }))
    .await
    .unwrap();
    let env_id = fx.default_env_id("codex").unwrap();
    SecretStore::delete(&*fx.secrets, &env_account(&env_id)).unwrap();
    let err = fx.svc.resolve_profile_env("codex").await.unwrap_err();
    assert!(matches!(err, Error::Internal(_)), "{err:?}");
    assert!(fx.svc.resolve_selected_profile("codex").await.is_err());
    // The other base is unaffected.
    assert!(fx
        .svc
        .resolve_profile_env("claude-code")
        .await
        .unwrap()
        .is_none());
    // Settings still list (the missing document reads empty) and a save
    // replaces it, which recovers launches.
    assert_eq!(fx.defaults_row("codex").await["env"], json!([]));
    // Clearing the (already missing) overrides removes the dangling envId.
    let cleared = fx
        .update_default(json!({ "baseProviderId": "codex", "env": [] }))
        .await
        .unwrap();
    assert_eq!(cleared["activation"]["envChanged"], true);
    assert!(fx.default_env_id("codex").is_none());
    assert!(fx.svc.resolve_profile_env("codex").await.unwrap().is_none());
    fx.update_default(json!({
        "baseProviderId": "codex",
        "env": [{ "key": "CODEX_HOME", "kind": "set", "value": "/work" }],
    }))
    .await
    .unwrap();
    assert!(fx.svc.resolve_profile_env("codex").await.unwrap().is_some());
}

#[test]
fn an_unbound_native_session_matches_only_the_empty_default_fingerprint() {
    assert!(binding_matches(None, ""));
    assert!(!binding_matches(None, "fabc"));
    assert!(binding_matches(Some("fabc"), "fabc"));
    assert!(!binding_matches(Some("fabc"), ""));
    assert!(!binding_matches(Some(""), "fabc"));
}

#[intent_test_macros::daemon_test]
async fn default_process_fingerprint_is_empty_until_overrides_are_saved() {
    let fx = Fixture::new().await;
    let agent = intent_core::AgentId::from("a-default");
    assert_eq!(
        fx.svc
            .profile_process_fingerprint(&agent, "codex")
            .await
            .as_deref(),
        Some("")
    );
    assert_eq!(
        fx.svc.profile_process_fingerprint(&agent, "auggie").await,
        None
    );
    let saved = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    assert_eq!(
        fx.svc
            .profile_process_fingerprint(&agent, "codex")
            .await
            .as_deref(),
        saved["defaultConfig"]["settingsFingerprint"].as_str()
    );
}

#[intent_test_macros::daemon_test]
async fn a_default_edit_marks_running_default_processes_as_earlier_settings() {
    let fx = Fixture::new().await;
    let ws = intent_core::WorkspaceId::from("ws-default");
    let agent = intent_core::AgentId::from("a-default");
    let lease = fx
        .svc
        .register_default_process(&ws, &agent, "codex")
        .await
        .expect("a Default process is tracked");
    assert_eq!(
        fx.svc
            .profile_activation()
            .running_processes(&default_activation_key("codex")),
        0
    );
    let mut sub = fx.subscribe();
    let saved = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    assert_eq!(saved["activation"]["runningProcesses"], 1);
    assert_eq!(saved["defaultConfig"]["activation"]["runningProcesses"], 1);
    assert!(fx.svc.profile_activation().uses_earlier_settings(&agent));
    let updated = recv_profiles_event(&mut sub).await;
    assert_eq!(updated["data"]["change"], "default-updated");
    let activation = recv_profiles_event(&mut sub).await;
    assert_eq!(activation["data"]["change"], "default-activation");
    assert_eq!(activation["data"]["baseProviderId"], "codex");
    assert!(activation["data"].get("profileId").is_none());
    // The live process keeps the fingerprint it started with.
    assert_eq!(
        fx.svc
            .profile_process_fingerprint(&agent, "codex")
            .await
            .as_deref(),
        Some("")
    );
    drop(lease);
    assert_eq!(
        fx.svc
            .profile_activation()
            .running_processes(&default_activation_key("codex")),
        0
    );
    assert!(!fx.svc.profile_activation().uses_earlier_settings(&agent));
}

#[intent_test_macros::daemon_test]
async fn default_values_never_reach_events_or_the_profile_file() {
    let fx = Fixture::new().await;
    let mut sub = fx.subscribe();
    fx.update_default(json!({
        "baseProviderId": "claude-code",
        "name": "Team",
        "env": [{ "key": "ANTHROPIC_API_KEY", "kind": "set", "value": SECRET }],
    }))
    .await
    .unwrap();
    let event = recv_profiles_event(&mut sub).await;
    assert!(!event.to_string().contains(SECRET));
    assert!(!std::fs::read_to_string(fx.profiles_file())
        .unwrap()
        .contains(SECRET));
    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert!(!catalog.to_string().contains(SECRET));
}

#[intent_test_macros::daemon_test]
async fn only_a_default_with_saved_overrides_skips_the_bare_verdict() {
    let fx = Fixture::new().await;
    fx.update_default(json!({ "baseProviderId": "codex", "name": "Personal" }))
        .await
        .unwrap();
    // A rename alone is not an override.
    assert!(!fx.svc.default_has_overrides("codex"));
    fx.update_default(json!({
        "baseProviderId": "codex",
        "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
    }))
    .await
    .unwrap();
    assert!(fx.svc.default_has_overrides("codex"));
    assert!(!fx.svc.default_has_overrides("claude-code"));
    assert!(!fx.svc.default_has_overrides("auggie"));
    assert!(!fx.svc.default_has_overrides("codex@pwork"));
}

#[intent_test_macros::daemon_test]
async fn an_unknown_base_in_defaults_is_ignored_not_fatal() {
    let fx = Fixture::new().await;
    fx.create("codex", "Work", json!([])).await;
    let mut file = fx.file();
    file["defaults"] = json!({ "future-cli": { "name": "X" }, "codex": { "name": "  Home " } });
    std::fs::write(fx.profiles_file(), file.to_string()).unwrap();
    let fx = fx.restart().await;
    let listed = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(listed["profiles"].as_array().unwrap().len(), 1);
    assert_eq!(fx.defaults_row("codex").await["name"], "Home");
}

#[test]
fn default_activation_keys_round_trip_and_never_match_profile_ids() {
    for base in PROFILE_BASE_PROVIDERS {
        let key = default_activation_key(base);
        assert_eq!(parse_default_activation_key(&key), Some(*base));
        assert!(!is_valid_profile_id(&key));
    }
    assert_eq!(parse_default_activation_key("pwork"), None);
    assert!(is_unrenamed_default_name(" default "));
    assert!(is_unrenamed_default_name(""));
    assert!(!is_unrenamed_default_name("Defaults"));
}

// ---------------------------------------------------------------------------
// Auth demotion follows the settings a live process actually runs with.
// ---------------------------------------------------------------------------

async fn saved_overlay(fx: &Fixture, reference: &str) -> intent_acp::ProfileEnv {
    fx.svc
        .resolve_profile_env(reference)
        .await
        .unwrap()
        .expect("saved overlay")
}

#[intent_test_macros::daemon_test]
async fn an_earlier_settings_default_process_never_demotes_the_newly_saved_account() {
    let fx = Fixture::new().await;
    let ws = intent_core::WorkspaceId::from("ws-auth");
    let old = intent_core::AgentId::from("a-old");
    // Account A: the bare Default, no overrides yet.
    let _lease = fx
        .svc
        .register_default_process(&ws, &old, "codex")
        .await
        .unwrap();
    // The user saves account B while A's turn runs.
    fx.update_default(json!({
        "baseProviderId": "codex",
        "env": [{ "key": "CODEX_HOME", "kind": "set", "value": "/accounts/b" }],
    }))
    .await
    .unwrap();
    let b = saved_overlay(&fx, "codex").await;
    // A reports auth-required: nothing is demoted, B stays usable.
    fx.svc.auth_owner(&old, "codex").await.demote();
    assert_eq!(crate::provider_auth::cached_profile_auth_verdict(&b), None);
    assert_ne!(
        crate::provider_auth::cached_auth_verdict("codex"),
        Some(false)
    );
    // A process on the saved settings still demotes them.
    let fresh = intent_core::AgentId::from("a-new");
    fx.svc.auth_owner(&fresh, "codex").await.demote();
    assert_eq!(
        crate::provider_auth::cached_profile_auth_verdict(&b),
        Some(false)
    );
}

#[intent_test_macros::daemon_test]
async fn an_overlaid_process_never_demotes_the_bare_account_after_overrides_are_cleared() {
    let fx = Fixture::new().await;
    let ws = intent_core::WorkspaceId::from("ws-auth");
    let old = intent_core::AgentId::from("a-old");
    fx.update_default(json!({
        "baseProviderId": "claude-code",
        "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "/accounts/a" }],
    }))
    .await
    .unwrap();
    let a = saved_overlay(&fx, "claude-code").await;
    let _lease = fx
        .svc
        .register_profile_process(&ws, &old, &a)
        .await
        .unwrap();
    crate::provider_auth::seed_auth_verdict_for_tests("claude-code", Some(true));
    fx.update_default(json!({ "baseProviderId": "claude-code", "env": [] }))
        .await
        .unwrap();
    fx.svc.auth_owner(&old, "claude-code").await.demote();
    assert_eq!(
        crate::provider_auth::cached_auth_verdict("claude-code"),
        Some(true),
        "the bare installation keeps its verdict"
    );
}

#[intent_test_macros::daemon_test]
async fn an_earlier_settings_profile_process_never_demotes_the_saved_profile() {
    let fx = Fixture::new().await;
    let ws = intent_core::WorkspaceId::from("ws-auth");
    let old = intent_core::AgentId::from("a-old");
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "CODEX_HOME", "kind": "set", "value": "/accounts/a" }]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_owned();
    let reference = format!("codex@{id}");
    let a = saved_overlay(&fx, &reference).await;
    let _lease = fx
        .svc
        .register_profile_process(&ws, &old, &a)
        .await
        .unwrap();
    fx.svc
        .provider_profiles_update_impl(
            id,
            json!({ "env": [{ "key": "CODEX_HOME", "kind": "set", "value": "/accounts/b" }] }),
        )
        .await
        .unwrap();
    let b = saved_overlay(&fx, &reference).await;
    fx.svc.auth_owner(&old, &reference).await.demote();
    assert_eq!(crate::provider_auth::cached_profile_auth_verdict(&b), None);
}

#[intent_test_macros::daemon_test]
async fn default_reads_return_folder_paths_as_stored() {
    let fx = Fixture::new().await;
    let result = fx
        .update_default(json!({
            "baseProviderId": "codex",
            "env": [
                { "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-personal" },
                { "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET },
            ],
        }))
        .await
        .unwrap();
    let expected = json!([
        { "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-personal" },
        { "key": "OPENAI_API_KEY", "kind": "set", "value": MASK },
    ]);
    assert_eq!(result["defaultConfig"]["env"], expected);
    assert!(!result.to_string().contains(SECRET));
    assert_eq!(fx.defaults_row("codex").await["env"], expected);

    let claude = fx
        .update_default(json!({
            "baseProviderId": "claude-code",
            "env": [{ "key": "Claude_Config_Dir", "kind": "set", "value": "/accounts/claude" }],
        }))
        .await
        .unwrap();
    let expected = json!([
        { "key": "Claude_Config_Dir", "kind": "set", "value": "/accounts/claude" },
    ]);
    assert_eq!(claude["defaultConfig"]["env"], expected);
    assert_eq!(fx.defaults_row("claude-code").await["env"], expected);

    // Launches still expand the saved `~`.
    let overlay = fx.svc.resolve_profile_env("codex").await.unwrap().unwrap();
    let expanded = daemon_home().join(".codex-personal");
    assert_eq!(
        entries_of(&overlay)[0],
        (
            "CODEX_HOME".into(),
            Some(expanded.to_string_lossy().into_owned())
        )
    );
}
