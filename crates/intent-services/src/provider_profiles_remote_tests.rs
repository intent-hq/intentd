//! The overlay a remote node launch receives for the selected provider
//! reference: same selection rules as a local launch, but account-directory
//! values are sent as typed so the node expands `~` against its own home.

use super::*;

impl Fixture {
    fn set_setting(&self, path: &str, value: Value) {
        self.svc
            .settings_registry()
            .expect("registry")
            .apply(&[(path.to_string(), value)])
            .expect("apply setting");
    }
}

async fn remote(fx: &Fixture, reference: &str) -> Result<Option<intent_acp::ProfileEnv>> {
    fx.svc.resolve_remote_profile_env(reference).await
}

#[intent_test_macros::daemon_test]
async fn remote_overlay_carries_the_profile_entries_with_account_dirs_as_typed() {
    let fx = Fixture::new().await;
    for (base, key, token) in [
        (
            "claude-code",
            "CLAUDE_CONFIG_DIR",
            "CLAUDE_CODE_OAUTH_TOKEN",
        ),
        ("codex", "CODEX_HOME", "CODEX_ACCESS_TOKEN"),
    ] {
        let created = fx
            .create(
                base,
                "Remote",
                json!([
                    { "key": key, "kind": "set", "value": "~/.account-remote" },
                    { "key": token, "kind": "set", "value": SECRET },
                    { "key": "OPENAI_BASE_URL", "kind": "unset" },
                ]),
            )
            .await;
        let reference = created["profile"]["providerRef"].as_str().unwrap();
        let fingerprint = created["profile"]["settingsFingerprint"].as_str().unwrap();
        let profile = remote(&fx, reference)
            .await
            .unwrap()
            .expect("profile overlay");
        assert_eq!(profile.provider_ref(), reference);
        assert_eq!(profile.fingerprint(), fingerprint);
        assert_eq!(
            entries_of(&profile),
            vec![
                (key.into(), Some("~/.account-remote".into())),
                (token.into(), Some(SECRET.into())),
                ("OPENAI_BASE_URL".into(), None),
            ],
            "{base}: the head never expands ~ for a remote node"
        );
        assert!(!format!("{profile:?}").contains(SECRET));
        let local = fx
            .svc
            .resolve_profile_env(reference)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            entries_of(&local)[0].1.as_deref(),
            Some("~/.account-remote"),
            "{base}: local launches keep expanding against the daemon home"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn remote_default_overlay_is_sent_only_when_overrides_are_saved() {
    let fx = Fixture::new().await;
    assert!(remote(&fx, "codex").await.unwrap().is_none());
    assert!(remote(&fx, "claude-code").await.unwrap().is_none());
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [
                { "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-default" },
                { "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": SECRET },
            ],
        }))
        .await
        .unwrap();
    let profile = remote(&fx, "codex")
        .await
        .unwrap()
        .expect("default overrides");
    assert_eq!(profile.provider_ref(), "codex");
    assert_eq!(
        entries_of(&profile),
        vec![
            ("CODEX_HOME".into(), Some("~/.codex-default".into())),
            ("CODEX_ACCESS_TOKEN".into(), Some(SECRET.into())),
        ]
    );
    assert!(
        remote(&fx, "claude-code").await.unwrap().is_none(),
        "no overrides: the node's own login is used"
    );
}

#[intent_test_macros::daemon_test]
async fn remote_missing_profile_refuses_without_falling_back() {
    let fx = Fixture::new().await;
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "codex",
            "env": [{ "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": SECRET }],
        }))
        .await
        .unwrap();
    let created = fx.create("codex", "Gone", json!([])).await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    fx.svc
        .provider_profiles_delete_impl(id.clone())
        .await
        .unwrap();
    for reference in [format!("codex@{id}"), "codex@p0000000000000".into()] {
        let err = remote(&fx, &reference).await.unwrap_err();
        assert_eq!(data_code(&err), "provider-profile-replacement-required");
        let Error::ProviderProfile(failure) = &err else {
            panic!("{err:?}");
        };
        assert_eq!(failure.data["reason"], "profile-missing");
        assert!(!err.to_string().contains(SECRET));
    }
}

#[intent_test_macros::daemon_test]
async fn remote_profile_of_a_disabled_base_refuses_without_falling_back() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": SECRET }]),
        )
        .await;
    let reference = created["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_owned();
    fx.set_setting("providers.enabled", json!({ "codex": false }));
    let err = remote(&fx, &reference).await.unwrap_err();
    assert_eq!(data_code(&err), "provider-profile-replacement-required");
    let Error::ProviderProfile(failure) = &err else {
        panic!("{err:?}");
    };
    assert_eq!(failure.data["reason"], "provider-disabled");
    assert!(!err.to_string().contains(SECRET));
}

#[intent_test_macros::daemon_test]
async fn remote_overlay_never_carries_reserved_launch_keys() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": "kept" }]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let doc = json!({
        "fingerprint": "ftampered",
        "entries": [
            { "kind": "set", "key": "PATH", "value": "/evil" },
            { "kind": "unset", "key": "HOME" },
            { "kind": "set", "key": "INTENT_SESSION", "value": "evil" },
            { "kind": "set", "key": "CODEX_ACCESS_TOKEN", "value": "kept" },
        ],
    });
    SecretStore::store(&*fx.secrets, &env_account(&id), &doc.to_string()).unwrap();
    let profile = remote(&fx, &format!("codex@{id}")).await.unwrap().unwrap();
    assert_eq!(
        entries_of(&profile),
        vec![("CODEX_ACCESS_TOKEN".into(), Some("kept".into()))]
    );
}

/// Remote compatibility (intent-hq/intent#8317): a configuration that sets
/// an account folder is tied to this backend and never runs on another
/// machine; the machine's own sign-in (no overrides), an environment of
/// other values (API keys), and unsetting a folder all run anywhere.
#[intent_test_macros::daemon_test]
async fn only_a_set_account_folder_makes_a_configuration_backend_only() {
    let fx = Fixture::new().await;
    let reference = |created: Value| {
        created["profile"]["providerRef"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let folder = reference(
        fx.create(
            "claude-code",
            "Work account",
            json!([{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-work" }]),
        )
        .await,
    );
    let codex_folder = reference(
        fx.create(
            "codex",
            "Codex work",
            json!([{ "key": "codex_home", "kind": "set", "value": "/accounts/b" }]),
        )
        .await,
    );
    let api_key = reference(
        fx.create(
            "claude-code",
            "API key",
            json!([{ "key": "ANTHROPIC_API_KEY", "kind": "set", "value": SECRET }]),
        )
        .await,
    );
    let unset = reference(
        fx.create(
            "claude-code",
            "Machine login",
            json!([{ "key": "CLAUDE_CONFIG_DIR", "kind": "unset" }]),
        )
        .await,
    );

    assert_eq!(
        fx.svc.remote_compatibility(&folder).await.unwrap(),
        RemoteCompatibility::NotRemoteCompatible {
            reason: REMOTE_INCOMPATIBLE_ACCOUNT_FOLDER,
            keys: vec!["CLAUDE_CONFIG_DIR".into()],
        }
    );
    assert_eq!(
        fx.svc.remote_compatibility(&codex_folder).await.unwrap(),
        RemoteCompatibility::NotRemoteCompatible {
            reason: REMOTE_INCOMPATIBLE_ACCOUNT_FOLDER,
            keys: vec!["codex_home".into()],
        }
    );
    for compatible in [api_key.as_str(), unset.as_str(), "claude-code", "codex"] {
        assert_eq!(
            fx.svc.remote_compatibility(compatible).await.unwrap(),
            RemoteCompatibility::Compatible,
            "{compatible}"
        );
    }
    assert!(
        fx.svc
            .remote_compatibility("claude-code@p0000000000000")
            .await
            .is_err(),
        "an unknown profile is not classified"
    );
}

/// The Default configuration's overrides count like a profile's.
#[intent_test_macros::daemon_test]
async fn default_overrides_with_an_account_folder_are_backend_only() {
    let fx = Fixture::new().await;
    fx.svc
        .provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-a" }],
        }))
        .await
        .unwrap();
    assert!(matches!(
        fx.svc.remote_compatibility("claude-code").await.unwrap(),
        RemoteCompatibility::NotRemoteCompatible { .. }
    ));
    assert_eq!(
        fx.svc.remote_compatibility("codex").await.unwrap(),
        RemoteCompatibility::Compatible
    );
}
