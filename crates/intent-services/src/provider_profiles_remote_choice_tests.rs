//! Remote compatibility as a saved choice, machine sign-in ("naked mode")
//! and the launch rules for a remote agent (§5.54 "Remote compatibility",
//! nodes.md "Placement across machines").

use super::*;

const TOKEN: &str = "CLAUDE_CODE_OAUTH_TOKEN";

impl Fixture {
    fn choices_file(&self) -> PathBuf {
        self.data_dir().join(REMOTE_CHOICES_FILE)
    }

    async fn create_with(&self, params: Value) -> Result<Value> {
        self.svc.provider_profiles_create_impl(params).await
    }

    fn machine_sign_in(&self, value: Value) {
        self.svc
            .settings_registry()
            .expect("registry")
            .apply(&[("providers.remoteMachineSignIn".to_string(), value)])
            .expect("apply setting");
    }

    async fn plan(&self, reference: &str) -> Result<RemoteLaunchPlan> {
        self.svc
            .remote_launch_plan(reference, RemoteTarget::Machine)
            .await
    }

    async fn cloud_plan(&self, reference: &str) -> Result<RemoteLaunchPlan> {
        self.svc
            .remote_launch_plan(reference, RemoteTarget::Cloud)
            .await
    }
}

fn reference_of(created: &Value) -> String {
    created["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn id_of(created: &Value) -> String {
    created["profile"]["id"].as_str().unwrap().to_owned()
}

fn refusal(err: &Error) -> Value {
    let Error::ProviderProfile(failure) = err else {
        panic!("expected a provider profile error, got {err:?}");
    };
    assert_eq!(failure.data["code"], PROFILE_NOT_REMOTE_COMPATIBLE, "{err}");
    failure.data.clone()
}

fn token_env() -> Value {
    json!([{ "key": TOKEN, "kind": "set", "value": SECRET }])
}

fn folder_env() -> Value {
    json!([{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-work" }])
}

async fn default_row(fx: &Fixture, base: &str) -> Value {
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    list["defaults"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["baseProviderId"] == base)
        .cloned()
        .unwrap()
}

// --- Writes -----------------------------------------------------------------

#[intent_test_macros::daemon_test]
async fn create_saves_the_choice_and_defaults_to_the_migration_rule() {
    let fx = Fixture::new().await;
    let unmarked = fx
        .create_with(json!({
            "baseProviderId": "claude-code", "name": "Kept here",
            "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap();
    assert_eq!(unmarked["profile"]["remoteCompatible"], false);
    assert_eq!(
        unmarked["profile"]["remoteIncompatibility"],
        json!({ "reason": "not-marked", "keys": [] })
    );
    let omitted = fx.create("claude-code", "API", token_env()).await;
    assert_eq!(
        omitted["profile"]["remoteCompatible"], true,
        "an older client's create keeps today's rule"
    );
    let folder = fx.create("claude-code", "Login", folder_env()).await;
    assert_eq!(folder["profile"]["remoteCompatible"], false);
    assert_eq!(
        folder["profile"]["remoteIncompatibility"]["reason"],
        "account-folder"
    );
    let err = fx
        .create_with(json!({
            "baseProviderId": "claude-code", "name": "Marked login",
            "env": folder_env(), "remoteCompatible": true,
        }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("remoteCompatible".into(), "remote-account-folder".into())]
    );
    let Error::ProviderProfile(failure) = &err else {
        panic!("{err:?}");
    };
    let message = failure.data["fieldErrors"][0]["message"].as_str().unwrap();
    assert!(message.contains("CLAUDE_CONFIG_DIR"), "{message}");
    let err = fx
        .create_with(json!({
            "baseProviderId": "codex", "name": "Bad", "remoteCompatible": "yes",
        }))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
}

#[intent_test_macros::daemon_test]
async fn update_keeps_the_choice_and_adding_a_folder_unmarks() {
    let fx = Fixture::new().await;
    let created = fx
        .create_with(json!({
            "baseProviderId": "claude-code", "name": "API",
            "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap();
    let id = id_of(&created);
    let marked = fx
        .svc
        .provider_profiles_update_impl(id.clone(), json!({ "remoteCompatible": true }))
        .await
        .unwrap();
    assert_eq!(marked["profile"]["remoteCompatible"], true);
    assert_eq!(marked["activation"]["envChanged"], false);
    assert!(marked["revision"].as_u64().unwrap() > created["revision"].as_u64().unwrap());

    let renamed = fx
        .svc
        .provider_profiles_update_impl(id.clone(), json!({ "name": "API key" }))
        .await
        .unwrap();
    assert_eq!(
        renamed["profile"]["remoteCompatible"], true,
        "omitted keeps it"
    );

    let err = fx
        .svc
        .provider_profiles_update_impl(
            id.clone(),
            json!({ "env": folder_env(), "remoteCompatible": true }),
        )
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("remoteCompatible".into(), "remote-account-folder".into())]
    );

    let folder = fx
        .svc
        .provider_profiles_update_impl(id.clone(), json!({ "env": folder_env() }))
        .await
        .unwrap();
    assert_eq!(folder["profile"]["remoteCompatible"], false);
    let cleared = fx
        .svc
        .provider_profiles_update_impl(id, json!({ "env": token_env() }))
        .await
        .unwrap();
    assert_eq!(
        cleared["profile"]["remoteIncompatibility"]["reason"], "not-marked",
        "removing the folder never marks it again"
    );
}

#[intent_test_macros::daemon_test]
async fn default_choice_and_machine_sign_in_decide_remote_launch() {
    let fx = Fixture::new().await;
    let row = default_row(&fx, "claude-code").await;
    assert_eq!(row["remoteCompatible"], false);
    assert_eq!(row["remoteLaunch"], "machine-sign-in");

    fx.machine_sign_in(json!({ "claude-code": false }));
    assert_eq!(
        default_row(&fx, "claude-code").await["remoteLaunch"],
        "refused"
    );
    assert_eq!(
        default_row(&fx, "codex").await["remoteLaunch"],
        "machine-sign-in"
    );

    let marked = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "remoteCompatible": true,
        }))
        .await
        .unwrap();
    assert_eq!(marked["defaultConfig"]["remoteCompatible"], true);
    assert_eq!(marked["defaultConfig"]["remoteLaunch"], "send");
    assert_eq!(marked["activation"]["envChanged"], false);

    let err = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": folder_env(), "remoteCompatible": true,
        }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("remoteCompatible".into(), "remote-account-folder".into())]
    );
    let folder = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": folder_env(),
        }))
        .await
        .unwrap();
    assert_eq!(folder["defaultConfig"]["remoteCompatible"], false);
    assert_eq!(folder["defaultConfig"]["remoteLaunch"], "refused");
}

#[intent_test_macros::daemon_test]
async fn machine_sign_in_setting_validates_and_announces_the_default_change() {
    let fx = Fixture::new().await;
    for invalid in [
        json!({ "auggie": true }),
        json!({ "codex@p3k9x2m4q7t1a": true }),
        json!({ "codex": "yes" }),
        json!([true]),
    ] {
        assert!(
            fx.svc
                .settings_registry()
                .unwrap()
                .apply(&[("providers.remoteMachineSignIn".into(), invalid.clone())])
                .is_err(),
            "{invalid}"
        );
    }
    let mut events = fx.subscribe();
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "providers.remoteMachineSignIn", "value": { "codex": false } },
        ]))
        .await
        .unwrap();
    let mut bases = Vec::new();
    let event = loop {
        let event = recv_profiles_event(&mut events).await;
        bases.push(event["data"]["baseProviderId"].clone());
        if event["data"]["baseProviderId"] == "codex" {
            break event;
        }
    };
    assert!(
        bases.len() <= 2,
        "one event per profile-capable base: {bases:?}"
    );
    assert_eq!(event["data"]["change"], "default-updated");
    assert_eq!(event["data"]["baseProviderId"], "codex");
    assert_eq!(event["data"]["envChanged"], false);
}

#[intent_test_macros::daemon_test]
async fn the_catalog_lists_only_defaults_that_cannot_run() {
    let fx = Fixture::new().await;
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": folder_env(),
        }))
        .await
        .unwrap();
    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert_eq!(
        catalog["defaultRemoteLaunch"],
        json!({ "claude-code": "machine-sign-in", "codex": "machine-sign-in" })
    );
    assert!(
        catalog.get("defaultRemoteIncompatibility").is_none(),
        "an unmarked Default still runs through machine sign-in"
    );
    fx.machine_sign_in(json!({ "claude-code": false, "codex": false }));
    let catalog = fx.svc.providers_catalog().await.unwrap();
    assert_eq!(
        catalog["defaultRemoteIncompatibility"],
        json!({
            "claude-code": { "reason": "account-folder", "keys": ["CLAUDE_CONFIG_DIR"] },
            "codex": { "reason": "not-marked", "keys": [] },
        })
    );
    assert_eq!(catalog["defaultRemoteLaunch"]["codex"], "refused");
}

// --- Migration and storage --------------------------------------------------

/// A data dir from before the saved choice: profiles and Default overrides
/// with no choices file. Restart migrates today's behaviour into the file.
#[intent_test_macros::daemon_test]
async fn migration_keeps_todays_behaviour_and_writes_the_choices_once() {
    let fx = Fixture::new().await;
    let api = id_of(&fx.create("claude-code", "API", token_env()).await);
    let login = id_of(&fx.create("claude-code", "Login", folder_env()).await);
    let unset = id_of(
        &fx.create(
            "claude-code",
            "Advanced",
            json!([{ "key": "CLAUDE_CONFIG_DIR", "kind": "unset" }]),
        )
        .await,
    );
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    std::fs::remove_file(fx.choices_file()).ok();

    let fx = fx.restart().await;
    fx.svc.migrate_remote_choices().await;
    let saved: Value =
        serde_json::from_slice(&std::fs::read(fx.choices_file()).expect("choices file")).unwrap();
    assert_eq!(saved["version"], 1);
    assert_eq!(saved["profiles"][&api], true);
    assert_eq!(saved["profiles"][&login], false);
    assert_eq!(
        saved["profiles"][&unset], true,
        "an unset folder is not a folder"
    );
    assert_eq!(
        saved["defaults"]["codex"], true,
        "overrides without a folder"
    );
    assert_eq!(saved["defaults"]["claude-code"], false, "no overrides");
    assert!(!saved.to_string().contains(SECRET));

    // After migration the choice is fixed: env changes never mark.
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    let row = |id: &str| {
        list["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == id)
            .cloned()
            .unwrap()
    };
    assert_eq!(row(&api)["remoteCompatible"], true);
    assert_eq!(
        row(&login)["remoteIncompatibility"]["reason"],
        "account-folder"
    );
}

/// The choices live outside the profile file, which an older daemon
/// rewrites without unknown members: a downgrade and upgrade keeps them.
#[intent_test_macros::daemon_test]
async fn an_older_daemon_rewriting_the_profile_file_keeps_the_choices() {
    let fx = Fixture::new().await;
    let created = fx
        .create_with(json!({
            "baseProviderId": "claude-code", "name": "API",
            "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap();
    let profiles = std::fs::read_to_string(fx.profiles_file()).unwrap();
    assert!(
        !profiles.contains("remoteCompatible"),
        "the profile file keeps its old shape"
    );
    let fx = fx.restart().await;
    fx.svc.migrate_remote_choices().await;
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(list["profiles"][0]["id"], created["profile"]["id"]);
    assert_eq!(list["profiles"][0]["remoteCompatible"], false);
}

#[intent_test_macros::daemon_test]
async fn deleting_a_profile_drops_its_choice() {
    let fx = Fixture::new().await;
    let id = id_of(&fx.create("codex", "API", json!([])).await);
    fx.svc
        .provider_profiles_delete_impl(id.clone())
        .await
        .unwrap();
    let saved: Value = serde_json::from_slice(&std::fs::read(fx.choices_file()).unwrap()).unwrap();
    assert!(saved["profiles"].get(&id).is_none(), "{saved}");
}

// --- Launch rules -----------------------------------------------------------

#[intent_test_macros::daemon_test]
async fn launch_rules_for_profiles() {
    let fx = Fixture::new().await;
    let marked = reference_of(&fx.create("claude-code", "Work", token_env()).await);
    let plan = fx.plan(&marked).await.unwrap();
    assert!(!plan.machine_sign_in);
    assert_eq!(plan.env.expect("sent").provider_ref(), marked);

    let unmarked = reference_of(
        &fx.create_with(json!({
            "baseProviderId": "claude-code", "name": "Personal",
            "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap(),
    );
    let err = fx.plan(&unmarked).await.unwrap_err();
    let data = refusal(&err);
    assert_eq!(data["reason"], "not-marked");
    assert_eq!(data["name"], "Personal");
    assert_eq!(data["providerRef"], unmarked);
    assert_eq!(data["keys"], json!([]));
    assert!(data.get("machineSignIn").is_none(), "{data}");
    let message = err.to_string();
    assert!(
        message.contains("Personal isn't marked for remote machines"),
        "{message}"
    );
    assert!(
        message.contains("Settings → Providers → Claude Code"),
        "{message}"
    );

    let folder = reference_of(&fx.create("claude-code", "Login", folder_env()).await);
    let data = refusal(&fx.plan(&folder).await.unwrap_err());
    assert_eq!(data["reason"], "account-folder");
    assert_eq!(data["keys"], json!(["CLAUDE_CONFIG_DIR"]));
}

#[intent_test_macros::daemon_test]
async fn launch_rules_for_the_default() {
    let fx = Fixture::new().await;
    // Not marked, machine sign-in on (the default): nothing is sent.
    let plan = fx.plan("claude-code").await.unwrap();
    assert!(plan.env.is_none());
    assert!(plan.machine_sign_in);

    // Not marked with overrides: still nothing is sent.
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap();
    let plan = fx.plan("claude-code").await.unwrap();
    assert!(
        plan.env.is_none(),
        "an unmarked Default's overrides stay here"
    );
    assert!(plan.machine_sign_in);

    // Not marked, machine sign-in off: refused.
    fx.machine_sign_in(json!({ "claude-code": false }));
    let err = fx.plan("claude-code").await.unwrap_err();
    let data = refusal(&err);
    assert_eq!(data["reason"], "not-marked");
    assert_eq!(data["machineSignIn"], false);
    assert_eq!(data["name"], "Default");
    assert!(
        err.to_string()
            .contains("using the machine's own sign-in is off for Claude Code"),
        "{err}"
    );

    // Marked: its overrides are sent, whatever machine sign-in says.
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "remoteCompatible": true,
        }))
        .await
        .unwrap();
    let plan = fx.plan("claude-code").await.unwrap();
    assert!(!plan.machine_sign_in);
    assert_eq!(plan.env.expect("overrides").provider_ref(), "claude-code");

    // Marked with no overrides: nothing to send, the machine's sign-in runs it.
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex", "remoteCompatible": true,
        }))
        .await
        .unwrap();
    fx.machine_sign_in(json!({ "codex": false }));
    let plan = fx.plan("codex").await.unwrap();
    assert!(plan.env.is_none());
    assert!(plan.machine_sign_in);
}

#[intent_test_macros::daemon_test]
async fn launch_rules_for_providers_without_profiles() {
    let fx = Fixture::new().await;
    for provider in ["unsloth", "auggie"] {
        let plan = fx.plan(provider).await.unwrap();
        assert!(plan.env.is_none(), "{provider}");
        assert!(!plan.machine_sign_in, "{provider}: no Default to mark");
    }
}

#[intent_test_macros::daemon_test]
async fn launch_rules_on_e2b_never_use_machine_sign_in() {
    let fx = Fixture::new().await;
    let err = fx.cloud_plan("claude-code").await.unwrap_err();
    let data = refusal(&err);
    assert_eq!(data["reason"], "not-marked");
    assert_eq!(data["machineSignIn"], true);
    assert!(
        err.to_string().contains("E2B has no sign-in of its own"),
        "{err}"
    );

    let marked = reference_of(&fx.create("claude-code", "Token", token_env()).await);
    let plan = fx.cloud_plan(&marked).await.unwrap();
    assert!(plan.env.is_some());
    let unmarked = reference_of(
        &fx.create_with(json!({
            "baseProviderId": "codex", "name": "Off",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
            "remoteCompatible": false,
        }))
        .await
        .unwrap(),
    );
    assert_eq!(
        refusal(&fx.cloud_plan(&unmarked).await.unwrap_err())["reason"],
        "not-marked"
    );
    assert!(fx.cloud_plan("unsloth").await.unwrap().env.is_none());
}

/// A malformed choices file is left untouched: every configuration reads
/// as not marked (a folder still reads as account-folder), an explicit
/// choice is refused, and a write without one still works.
#[intent_test_macros::daemon_test]
async fn an_unreadable_choices_file_reads_not_marked_and_refuses_choices() {
    let fx = Fixture::new().await;
    let id = id_of(&fx.create("claude-code", "API", token_env()).await);
    std::fs::write(fx.choices_file(), b"{ not json").unwrap();
    let fx = fx.restart().await;
    fx.svc.migrate_remote_choices().await;
    assert_eq!(
        std::fs::read(fx.choices_file()).unwrap(),
        b"{ not json",
        "left untouched"
    );
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(
        list["profiles"][0]["remoteIncompatibility"]["reason"],
        "not-marked"
    );
    for params in [
        json!({ "remoteCompatible": false }),
        json!({ "remoteCompatible": true }),
    ] {
        let err = fx
            .svc
            .provider_profiles_update_impl(id.clone(), params)
            .await
            .unwrap_err();
        assert_eq!(data_code(&err), "provider-profiles-unreadable");
    }
    let err = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex", "remoteCompatible": false,
        }))
        .await
        .unwrap_err();
    assert_eq!(data_code(&err), "provider-profiles-unreadable");
    fx.svc
        .provider_profiles_update_impl(id, json!({ "name": "API key" }))
        .await
        .expect("a write without a choice still works");
}

async fn default_marked(fx: &Fixture, base: &str) -> bool {
    default_row(fx, base).await["remoteCompatible"] == true
}

/// A Default write that adds a credential marks it for remote machines
/// (§5.54 "Credential auto-mark"), for every client.
#[intent_test_macros::daemon_test]
async fn adding_a_credential_marks_the_default() {
    let fx = Fixture::new().await;
    assert!(!default_marked(&fx, "claude-code").await);
    let saved = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "env": [{ "key": "anthropic_api_key", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    assert_eq!(saved["defaultConfig"]["remoteCompatible"], true);
    assert_eq!(saved["defaultConfig"]["remoteLaunch"], "send");
    for (base, key) in [
        ("codex", "CODEX_API_KEY"),
        ("codex", "OPENAI_API_KEY"),
        ("codex", "CODEX_ACCESS_TOKEN"),
        ("claude-code", "ANTHROPIC_AUTH_TOKEN"),
        ("claude-code", "CLAUDE_CODE_OAUTH_TOKEN"),
    ] {
        fx.svc
            .provider_profiles_update_default_impl(json!({
                "baseProviderId": base, "env": [], "remoteCompatible": false,
            }))
            .await
            .unwrap();
        fx.svc
            .provider_profiles_update_default_impl(json!({
                "baseProviderId": base,
                "env": [{ "key": key, "kind": "set", "value": SECRET }],
            }))
            .await
            .unwrap();
        assert!(default_marked(&fx, base).await, "{base} {key}");
    }
}

#[intent_test_macros::daemon_test]
async fn an_explicit_choice_in_the_same_write_wins_over_the_auto_mark() {
    let fx = Fixture::new().await;
    let saved = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }],
            "remoteCompatible": false,
        }))
        .await
        .unwrap();
    assert_eq!(saved["defaultConfig"]["remoteCompatible"], false);
}

#[intent_test_macros::daemon_test]
async fn a_default_with_an_account_folder_stays_unmarked_when_a_credential_is_added() {
    let fx = Fixture::new().await;
    let saved = fx
        .svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "env": [
                { "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-a" },
                { "key": TOKEN, "kind": "set", "value": SECRET },
            ],
        }))
        .await
        .expect("no field error: the auto-mark never asks for a folder");
    assert_eq!(saved["defaultConfig"]["remoteCompatible"], false);
    assert_eq!(
        saved["defaultConfig"]["remoteIncompatibility"]["reason"],
        "account-folder"
    );
}

#[intent_test_macros::daemon_test]
async fn removing_the_credential_keeps_the_mark() {
    let fx = Fixture::new().await;
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": token_env(),
        }))
        .await
        .unwrap();
    assert!(default_marked(&fx, "claude-code").await);
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": [],
        }))
        .await
        .unwrap();
    assert!(default_marked(&fx, "claude-code").await);
}

#[intent_test_macros::daemon_test]
async fn a_write_that_adds_no_credential_does_not_mark() {
    let fx = Fixture::new().await;
    // An unmarked Default that already holds a credential.
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code", "env": token_env(), "remoteCompatible": false,
        }))
        .await
        .unwrap();
    for params in [
        json!({ "baseProviderId": "claude-code", "name": "Work" }),
        json!({ "baseProviderId": "claude-code", "env": [
            { "key": TOKEN, "kind": "keep" },
            { "key": "ANTHROPIC_BASE_URL", "kind": "set", "value": "https://proxy.example" },
        ] }),
        json!({ "baseProviderId": "claude-code", "env": [
            { "key": TOKEN, "kind": "set", "value": "rotated-value" },
        ] }),
        json!({ "baseProviderId": "claude-code", "env": [
            { "key": TOKEN, "kind": "keep" },
            { "key": "OPENAI_API_KEY", "kind": "unset" },
        ] }),
    ] {
        fx.svc
            .provider_profiles_update_default_impl(params.clone())
            .await
            .unwrap();
        assert!(!default_marked(&fx, "claude-code").await, "{params}");
    }
    // Profiles never auto-mark.
    let profile = fx
        .create_with(json!({
            "baseProviderId": "codex", "name": "Off", "remoteCompatible": false,
        }))
        .await
        .unwrap();
    let updated = fx
        .svc
        .provider_profiles_update_impl(
            id_of(&profile),
            json!({ "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }] }),
        )
        .await
        .unwrap();
    assert_eq!(updated["profile"]["remoteCompatible"], false);
}
