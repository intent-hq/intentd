//! Unit tests for named provider profiles (PROTOCOL §5.54).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_core::Error;
use intent_store::Store;
use serde_json::{json, Value};

use super::*;
use crate::events::{EventBus, Subscription, SubscriptionFilter};
use crate::settings::{InMemorySecretStore, SecretStore};
use crate::test_support::test_tempdir;
use crate::tests::TempDb;
use crate::{Services, SettingsRegistry};

const SECRET: &str = "sk-very-secret-value-1234";

struct Fixture {
    _db: TempDb,
    root: tempfile::TempDir,
    secrets: Arc<InMemorySecretStore>,
    svc: Services,
    bus: EventBus,
}

impl Fixture {
    async fn new() -> Self {
        let root = test_tempdir("provider-profiles-");
        let secrets = Arc::new(InMemorySecretStore::default());
        Self::open(root, secrets).await
    }

    async fn open(root: tempfile::TempDir, secrets: Arc<InMemorySecretStore>) -> Self {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let data = root.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let user = root.path().join("specialists-user");
        let bundled = root.path().join("specialists-bundled");
        std::fs::create_dir_all(&user).unwrap();
        std::fs::create_dir_all(&bundled).unwrap();
        let registry =
            Arc::new(SettingsRegistry::load(data.join("config.toml")).expect("load registry"));
        let svc = Services::new(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry)
            .with_secret_store(secrets.clone())
            .with_specialist_dirs(Some(user), Some(bundled))
            .with_provider_profiles_dir(&data);
        Self {
            _db: db,
            root,
            secrets,
            svc,
            bus,
        }
    }

    /// Simulate a daemon restart over the same data dir and secret store.
    async fn restart(self) -> Self {
        let Self { root, secrets, .. } = self;
        Self::open(root, secrets).await
    }

    fn data_dir(&self) -> PathBuf {
        self.root.path().join("data")
    }

    fn profiles_file(&self) -> PathBuf {
        self.data_dir().join(PROFILES_FILE)
    }

    fn user_specialists(&self) -> PathBuf {
        self.root.path().join("specialists-user")
    }

    fn subscribe(&self) -> Subscription {
        self.bus.subscribe(SubscriptionFilter::default())
    }

    async fn create(&self, base: &str, name: &str, env: Value) -> Value {
        self.svc
            .provider_profiles_create_impl(
                json!({ "baseProviderId": base, "name": name, "env": env }),
            )
            .await
            .expect("create profile")
    }
}

fn field_codes(err: &Error) -> Vec<(String, String)> {
    let Error::ProviderProfile(failure) = err else {
        panic!("expected a provider profile error, got {err:?}");
    };
    assert!(!failure.internal);
    let data = &failure.data;
    assert_eq!(data["code"], "invalid-params", "{data}");
    data["fieldErrors"]
        .as_array()
        .expect("fieldErrors")
        .iter()
        .map(|e| {
            (
                e["field"].as_str().unwrap().to_string(),
                e["code"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn data_code(err: &Error) -> String {
    match err {
        Error::ProviderProfile(failure) => failure.data["code"].as_str().unwrap().to_string(),
        other => panic!("expected a provider profile error, got {other:?}"),
    }
}

async fn recv_profiles_event(sub: &mut Subscription) -> Value {
    loop {
        let batch = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        for event in batch {
            let ev = serde_json::to_value(&event).unwrap();
            if ev["type"] == intent_core::events::PROVIDERS_PROFILES_CHANGED {
                return ev;
            }
        }
    }
}

fn write_specialist(dir: &Path, id: &str, frontmatter: &str) {
    std::fs::write(
        dir.join(format!("{id}.md")),
        format!("---\nname: \"{id}\"\ndescription: \"d\"\n{frontmatter}\n---\n\nBody."),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Pure validation
// ---------------------------------------------------------------------------

#[test]
fn profile_refs_parse_only_canonical_qualified_forms() {
    assert_eq!(
        parse_profile_ref("codex@p3k9x2m4q7t1a"),
        Some(("codex", "p3k9x2m4q7t1a"))
    );
    assert_eq!(
        parse_profile_ref("claude-code@a-1"),
        Some(("claude-code", "a-1"))
    );
    for bad in [
        "codex",
        "auggie@p1",
        "Codex@p1",
        "codex@P1",
        " codex@p1",
        "codex@p1@p2",
        "codex@",
        "codex@-p1",
        "claude@p1",
        "codex@p12345678901234567890123456789012",
    ] {
        assert_eq!(parse_profile_ref(bad), None, "{bad}");
    }
}

#[test]
fn name_rules() {
    let existing = vec![ProfileMeta {
        id: "p1".into(),
        base_provider: "codex".into(),
        name: "Work".into(),
        created_at: String::new(),
        updated_at: String::new(),
    }];
    let check = |raw: &str, base: &str, self_id: Option<&str>| {
        let mut errors = Vec::new();
        let name = validate_name(raw, base, &existing, self_id, &mut errors);
        (name, errors.into_iter().map(|e| e.code).collect::<Vec<_>>())
    };
    assert_eq!(
        check("  Personal \u{2003}", "codex", None),
        ("Personal".into(), vec![])
    );
    assert_eq!(check(" \u{3000} ", "codex", None).1, vec!["name-blank"]);
    assert_eq!(check("a\u{7}b", "codex", None).1, vec!["name-invalid"]);
    assert_eq!(check(&"é".repeat(64), "codex", None).1, Vec::<&str>::new());
    assert_eq!(
        check(&"é".repeat(65), "codex", None).1,
        vec!["name-invalid"]
    );
    assert_eq!(check("DEFAULT", "codex", None).1, vec!["name-reserved"]);
    assert_eq!(check("wORK", "codex", None).1, vec!["name-duplicate"]);
    // Same name under another base provider, or renaming itself, is fine.
    assert_eq!(check("Work", "claude-code", None).1, Vec::<&str>::new());
    assert_eq!(check("WORK", "codex", Some("p1")).1, Vec::<&str>::new());
}

#[test]
fn env_key_value_and_reserved_rules() {
    let set = |k: &str, v: &str| EnvWrite::Set {
        key: k.into(),
        value: v.into(),
    };
    let writes = vec![
        set("OPENAI_BASE_URL", "https://x"),
        set("1BAD", "v"),
        set("openai_base_url", "dup"),
        set("Path", "/bin"),
        set("NUL", "a\0b"),
        set("BIG", &"x".repeat(32 * 1024 + 1)),
        set("MASKED", MASK),
        set("npm_config_workspaces", "x"),
        set("INTENTD_TOKEN", "x"),
        set("intent_session", "x"),
        EnvWrite::Keep {
            key: "KEEP_ME".into(),
        },
    ];
    let mut errors = Vec::new();
    validate_env(&writes, "codex", None, &mut errors);
    let got: Vec<(String, &str)> = errors.iter().map(|e| (e.field.clone(), e.code)).collect();
    assert_eq!(
        got,
        vec![
            ("env[1].key".into(), "env-key-invalid"),
            ("env[2].key".into(), "env-key-duplicate"),
            ("env[3].key".into(), "env-key-reserved"),
            ("env[4].value".into(), "env-value-invalid"),
            ("env[5].value".into(), "env-value-invalid"),
            ("env[6].value".into(), "env-value-placeholder"),
            ("env[7].key".into(), "env-key-reserved"),
            ("env[8].key".into(), "env-key-reserved"),
            ("env[9].key".into(), "env-key-reserved"),
            ("env[10].kind".into(), "env-keep-missing"),
        ]
    );
    // Values never appear in validation messages.
    assert!(errors.iter().all(|e| !e.message.contains("https://x")));
}

#[test]
fn home_reservation_points_to_the_account_variable() {
    let codex = reserved_key_reason("home", "codex").unwrap();
    assert!(codex.contains("CODEX_HOME"), "{codex}");
    let claude = reserved_key_reason("USERPROFILE", "claude-code").unwrap();
    assert!(claude.contains("CLAUDE_CONFIG_DIR"), "{claude}");
    for key in [
        "PATH",
        "CODEX_PATH",
        "claude_code_executable",
        "CODEX_CONFIG",
        "NODE_OPTIONS",
        "npm_workspace_x",
        "npm_config_include_workspace_root",
        "INTENT_X",
    ] {
        assert!(reserved_key_reason(key, "codex").is_some(), "{key}");
    }
    for key in [
        "CODEX_HOME",
        "CLAUDE_CONFIG_DIR",
        "ANTHROPIC_API_KEY",
        "INTENTX",
        "npm_config_cache",
    ] {
        assert!(reserved_key_reason(key, "codex").is_none(), "{key}");
    }
}

#[test]
fn env_limit_is_reported_on_the_env_field() {
    let writes: Vec<EnvWrite> = (0..65)
        .map(|i| EnvWrite::Unset {
            key: format!("K{i}"),
        })
        .collect();
    let mut errors = Vec::new();
    validate_env(&writes, "codex", None, &mut errors);
    assert_eq!(errors.len(), 1);
    assert_eq!(
        (errors[0].field.as_str(), errors[0].code),
        ("env", "env-limit")
    );
}

#[test]
fn malformed_env_shapes_are_plain_invalid_params() {
    for env in [
        json!({}),
        json!(["x"]),
        json!([{ "kind": "set", "value": "v" }]),
        json!([{ "key": "K", "kind": "drop" }]),
        json!([{ "key": "K", "kind": "set" }]),
    ] {
        assert!(
            matches!(parse_env_writes(&env), Err(Error::InvalidParams(_))),
            "{env}"
        );
    }
}

// ---------------------------------------------------------------------------
// Service surface
// ---------------------------------------------------------------------------

#[intent_test_macros::daemon_test]
async fn create_masks_values_and_keeps_set_empty_and_unset_distinct() {
    let fx = Fixture::new().await;
    let mut sub = fx.subscribe();
    let created = fx
        .create(
            "codex",
            "  Work  ",
            json!([
                { "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET },
                { "key": "EMPTY", "kind": "set", "value": "" },
                { "key": "OPENAI_BASE_URL", "kind": "unset" },
            ]),
        )
        .await;
    let profile = &created["profile"];
    let id = profile["id"].as_str().unwrap();
    assert!(
        id.starts_with('p') && id.len() == 13 && is_valid_profile_id(id),
        "{id}"
    );
    assert_eq!(profile["providerRef"], format!("codex@{id}"));
    assert_eq!(profile["baseProviderId"], "codex");
    assert_eq!(profile["name"], "Work");
    assert_eq!(
        profile["env"],
        json!([
            { "key": "OPENAI_API_KEY", "kind": "set", "value": "********" },
            { "key": "EMPTY", "kind": "set", "value": "" },
            { "key": "OPENAI_BASE_URL", "kind": "unset" },
        ])
    );
    assert!(profile["settingsFingerprint"]
        .as_str()
        .is_some_and(|f| !f.is_empty()));
    assert_eq!(created["revision"], 1);

    // The secret store holds the literal values; the metadata file does not.
    let raw = SecretStore::load(&*fx.secrets, &env_account(id))
        .unwrap()
        .unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(doc["entries"][0]["value"], SECRET);
    assert_eq!(doc["entries"][1]["value"], "");
    assert_eq!(
        doc["entries"][2],
        json!({ "kind": "unset", "key": "OPENAI_BASE_URL" })
    );
    let file = std::fs::read_to_string(fx.profiles_file()).unwrap();
    assert!(
        !file.contains(SECRET) && !file.contains("OPENAI_API_KEY"),
        "{file}"
    );
    let parsed: Value = serde_json::from_str(&file).unwrap();
    assert_eq!(parsed["version"], 1);
    assert!(parsed.get("pendingEnvCleanup").is_none(), "{file}");
    assert_eq!(
        parsed["profiles"][0]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["id", "baseProvider", "name", "createdAt", "updatedAt"]
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(fx.profiles_file())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // One event, with the summary only — no env keys or values.
    let ev = recv_profiles_event(&mut sub).await;
    assert_eq!(
        ev["data"],
        json!({
            "revision": 1,
            "change": "created",
            "profileId": id,
            "profile": { "id": id, "providerRef": format!("codex@{id}"), "baseProviderId": "codex", "name": "Work" },
        })
    );
    assert_eq!(ev["workspaceId"], "");

    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(list["revision"], 1);
    assert_eq!(list["profiles"], json!([profile]));
    assert!(!list.to_string().contains(SECRET));
    let filtered = fx
        .svc
        .provider_profiles_list_impl(Some("claude-code".into()))
        .await
        .unwrap();
    assert_eq!(filtered["profiles"], json!([]));
}

#[intent_test_macros::daemon_test]
async fn create_reports_every_field_error_and_writes_nothing() {
    let fx = Fixture::new().await;
    let err = fx
        .svc
        .provider_profiles_create_impl(json!({
            "baseProviderId": "auggie",
            "name": "default",
            "env": [
                { "key": "PATH", "kind": "set", "value": "/x" },
                { "key": "A", "kind": "keep" },
            ],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), -32602);
    assert_eq!(
        field_codes(&err),
        vec![
            ("baseProviderId".into(), "base-provider-unsupported".into()),
            ("name".into(), "name-reserved".into()),
            ("env[0].key".into(), "env-key-reserved".into()),
            ("env[1].kind".into(), "env-keep-missing".into()),
        ]
    );
    assert!(!fx.profiles_file().exists());
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(list, json!({ "profiles": [], "revision": 0 }));

    fx.create("codex", "Work", json!([])).await;
    let dup = fx
        .svc
        .provider_profiles_create_impl(json!({ "baseProviderId": "codex", "name": "work" }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&dup),
        vec![("name".into(), "name-duplicate".into())]
    );
    // The same name may exist under another base provider.
    fx.create("claude-code", "Work", json!([])).await;
}

#[intent_test_macros::daemon_test]
async fn profile_limit_is_per_base_provider() {
    let fx = Fixture::new().await;
    for i in 0..32 {
        fx.create("codex", &format!("P{i}"), json!([])).await;
    }
    let err = fx
        .svc
        .provider_profiles_create_impl(json!({ "baseProviderId": "codex", "name": "One more" }))
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("baseProviderId".into(), "profile-limit".into())]
    );
    fx.create("claude-code", "Other base", json!([])).await;
}

#[intent_test_macros::daemon_test]
async fn update_keep_preserves_masked_values_and_omitted_keys_restore_inheritance() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "claude-code",
            "Team",
            json!([
                { "key": "ANTHROPIC_API_KEY", "kind": "set", "value": SECRET },
                { "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "/accounts/team" },
                { "key": "HTTP_PROXY", "kind": "unset" },
            ]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let first_fp = created["profile"]["settingsFingerprint"].clone();
    let mut sub = fx.subscribe();

    // Rename only: env untouched, fingerprint unchanged, envChanged false.
    let renamed = fx
        .svc
        .provider_profiles_update_impl(id.clone(), json!({ "profileId": id, "name": "Team B" }))
        .await
        .unwrap();
    assert_eq!(renamed["profile"]["name"], "Team B");
    assert_eq!(renamed["profile"]["id"], id.as_str());
    assert_eq!(
        renamed["profile"]["providerRef"],
        format!("claude-code@{id}")
    );
    assert_eq!(renamed["profile"]["settingsFingerprint"], first_fp);
    assert_eq!(
        renamed["activation"],
        json!({ "envChanged": false, "runningProcesses": 0 })
    );
    assert_eq!(renamed["revision"], 2);
    let ev = recv_profiles_event(&mut sub).await;
    assert_eq!(ev["data"]["change"], "updated");
    assert_eq!(ev["data"]["envChanged"], false);
    assert_eq!(ev["data"]["revision"], 2);

    // Keep the secret (masked in the client), drop CLAUDE_CONFIG_DIR, add one.
    let updated = fx
        .svc
        .provider_profiles_update_impl(
            id.clone(),
            json!({ "env": [
                { "key": "anthropic_api_key", "kind": "keep" },
                { "key": "HTTP_PROXY", "kind": "keep" },
                { "key": "ANTHROPIC_BASE_URL", "kind": "set", "value": "" },
            ] }),
        )
        .await
        .unwrap();
    assert_eq!(updated["activation"]["envChanged"], true);
    assert_ne!(updated["profile"]["settingsFingerprint"], first_fp);
    assert_eq!(
        updated["profile"]["env"],
        json!([
            { "key": "anthropic_api_key", "kind": "set", "value": "********" },
            { "key": "HTTP_PROXY", "kind": "unset" },
            { "key": "ANTHROPIC_BASE_URL", "kind": "set", "value": "" },
        ])
    );
    let raw = SecretStore::load(&*fx.secrets, &env_account(&id))
        .unwrap()
        .unwrap();
    assert!(raw.contains(SECRET));
    assert!(!raw.contains("/accounts/team"));
    let ev = recv_profiles_event(&mut sub).await;
    assert_eq!(ev["data"]["envChanged"], true);
    assert_eq!(ev["data"]["revision"], 3);
    assert!(!ev.to_string().contains("ANTHROPIC"));

    // Keeping a key with no saved entry fails and changes nothing.
    let err = fx
        .svc
        .provider_profiles_update_impl(
            id.clone(),
            json!({ "env": [{ "key": "NOT_SAVED", "kind": "keep" }] }),
        )
        .await
        .unwrap_err();
    assert_eq!(
        field_codes(&err),
        vec![("env[0].kind".into(), "env-keep-missing".into())]
    );
    // Omitting env leaves all entries unchanged.
    let same = fx
        .svc
        .provider_profiles_update_impl(id.clone(), json!({ "name": "Team B" }))
        .await
        .unwrap();
    assert_eq!(same["profile"]["env"], updated["profile"]["env"]);
    assert_eq!(same["revision"], 3, "a no-op update commits nothing");

    let missing = fx
        .svc
        .provider_profiles_update_impl("pnope".into(), json!({ "name": "x" }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), -32602);
    assert_eq!(data_code(&missing), "not-found");
}

#[intent_test_macros::daemon_test]
async fn profiles_persist_across_restart_and_revision_resets() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "CODEX_HOME", "kind": "set", "value": "/a" }]),
        )
        .await;
    let fx = fx.restart().await;
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(list["profiles"], json!([created["profile"]]));
    assert_eq!(list["revision"], 0, "revision is process-local");
}

#[intent_test_macros::daemon_test]
async fn malformed_file_is_left_untouched_and_reported_unreadable() {
    for bad in [
        "{ not json".to_string(),
        json!({ "version": 2, "profiles": [] }).to_string(),
        json!({ "version": 1, "profiles": [{ "id": "BAD", "baseProvider": "codex", "name": "x", "createdAt": "", "updatedAt": "" }] }).to_string(),
    ] {
        let fx = Fixture::new().await;
        std::fs::write(fx.profiles_file(), &bad).unwrap();
        let fx = fx.restart().await;
        fx.svc.sweep_provider_profile_secrets().await;
        let err = fx.svc.provider_profiles_list_impl(None).await.unwrap_err();
        assert_eq!(err.code(), -32603, "{bad}");
        assert_eq!(data_code(&err), "provider-profiles-unreadable");
        let err = fx
            .svc
            .provider_profiles_create_impl(json!({ "baseProviderId": "codex", "name": "x" }))
            .await
            .unwrap_err();
        assert_eq!(data_code(&err), "provider-profiles-unreadable");
        // Qualified references name no saved profile while unreadable.
        let err = fx
            .svc
            .settings_update_checking_profiles(json!([{ "path": "model.defaultProvider", "value": "codex@p1" }]))
            .await
            .unwrap_err();
        assert_eq!(data_code(&err), "provider-profile-not-found");
        assert_eq!(std::fs::read_to_string(fx.profiles_file()).unwrap(), bad);
    }
}

#[intent_test_macros::daemon_test]
async fn startup_sweep_removes_only_journaled_orphan_secrets() {
    let fx = Fixture::new().await;
    let kept = fx.create("codex", "Kept", json!([])).await;
    let kept_id = kept["profile"]["id"].as_str().unwrap().to_string();
    // An interrupted create: journaled id + secret, no metadata. An
    // unrelated daemon's profile secret shares the store but is not journaled.
    SecretStore::store(&*fx.secrets, &env_account("porphan"), "{}").unwrap();
    SecretStore::store(&*fx.secrets, &env_account("pforeign"), "{}").unwrap();
    let mut file: Value =
        serde_json::from_str(&std::fs::read_to_string(fx.profiles_file()).unwrap()).unwrap();
    file["pendingEnvCleanup"] = json!(["porphan", kept_id]);
    std::fs::write(fx.profiles_file(), file.to_string()).unwrap();

    let fx = fx.restart().await;
    fx.svc.sweep_provider_profile_secrets().await;
    assert!(SecretStore::load(&*fx.secrets, &env_account("porphan"))
        .unwrap()
        .is_none());
    assert!(SecretStore::load(&*fx.secrets, &env_account("pforeign"))
        .unwrap()
        .is_some());
    assert!(SecretStore::load(&*fx.secrets, &env_account(&kept_id))
        .unwrap()
        .is_some());
    let file: Value =
        serde_json::from_str(&std::fs::read_to_string(fx.profiles_file()).unwrap()).unwrap();
    assert!(file.get("pendingEnvCleanup").is_none(), "{file}");
    assert_eq!(file["profiles"].as_array().unwrap().len(), 1);
}

#[intent_test_macros::daemon_test]
async fn delete_is_blocked_with_named_reasons_then_removes_both_records() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let reference = format!("codex@{id}");

    fx.svc
        .settings_update_checking_profiles(
            json!([{ "path": "model.defaultProvider", "value": reference }]),
        )
        .await
        .expect("select the profile as default");
    write_specialist(
        &fx.user_specialists(),
        "pinned",
        &format!("codingAgent: \"{reference}\""),
    );
    write_specialist(
        &fx.user_specialists(),
        "optioned",
        &format!(
            "modelOptions: [{{\"provider\":\"codex\",\"model\":\"m0\"}},{{\"provider\":\"{reference}\",\"model\":\"m1\"}}]"
        ),
    );

    let err = fx
        .svc
        .provider_profiles_delete_impl(id.clone())
        .await
        .unwrap_err();
    assert_eq!(err.code(), -32602);
    let Error::ProviderProfile(failure) = &err else {
        panic!("{err:?}")
    };
    let data = &failure.data;
    assert_eq!(data["code"], "provider-profile-in-use");
    assert_eq!(
        data["inUse"],
        json!([
            { "kind": "defaultProvider" },
            { "kind": "specialistModelOption", "specialistId": "optioned", "source": "user", "index": 1 },
            { "kind": "specialistProvider", "specialistId": "pinned", "source": "user" },
        ])
    );
    assert!(SecretStore::load(&*fx.secrets, &env_account(&id))
        .unwrap()
        .is_some());

    // Clear every selection; a providerDefaults key does not block and is pruned.
    std::fs::remove_file(fx.user_specialists().join("pinned.md")).unwrap();
    std::fs::remove_file(fx.user_specialists().join("optioned.md")).unwrap();
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.defaultProvider", "value": "codex" },
            { "path": "model.providerDefaults", "value": { "codex": "m0", reference.clone(): "m1" } },
        ]))
        .await
        .unwrap();
    let mut sub = fx.subscribe();
    let deleted = fx
        .svc
        .provider_profiles_delete_impl(id.clone())
        .await
        .unwrap();
    assert_eq!(deleted, json!({ "profileId": id, "revision": 2 }));
    let ev = recv_profiles_event(&mut sub).await;
    assert_eq!(
        ev["data"],
        json!({ "revision": 2, "change": "deleted", "profileId": id })
    );
    assert!(SecretStore::load(&*fx.secrets, &env_account(&id))
        .unwrap()
        .is_none());
    assert_eq!(
        fx.svc.effective_settings().model.provider_defaults,
        std::collections::BTreeMap::from([("codex".to_string(), "m0".to_string())])
    );
    let file = std::fs::read_to_string(fx.profiles_file()).unwrap();
    assert!(!file.contains(&id), "{file}");
    let again = fx.svc.provider_profiles_delete_impl(id).await.unwrap_err();
    assert_eq!(data_code(&again), "not-found");
}

#[intent_test_macros::daemon_test]
async fn settings_update_rejects_missing_profile_references_only_when_new_or_changed() {
    let fx = Fixture::new().await;
    let created = fx.create("codex", "Work", json!([])).await;
    let reference = created["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_string();

    let err = fx
        .svc
        .settings_update_checking_profiles(
            json!([{ "path": "model.defaultProvider", "value": "codex@pmissing" }]),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), -32602);
    assert_eq!(data_code(&err), "provider-profile-not-found");
    for bad in ["auggie@p1", "Codex@p1"] {
        let err = fx
            .svc
            .settings_update_checking_profiles(
                json!([{ "path": "model.defaultProvider", "value": bad }]),
            )
            .await
            .unwrap_err();
        assert_eq!(data_code(&err), "provider-profile-not-found", "{bad}");
    }

    // A saved reference and a key for a profile that is later lost.
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { reference.clone(): "m1", "codex@pgone": "m2" } },
        ]))
        .await
        .expect_err("a new missing key is rejected");
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { reference.clone(): "m1" } },
        ]))
        .await
        .unwrap();
    // Simulate the profile vanishing (e.g. file restored from a backup).
    std::fs::write(
        fx.profiles_file(),
        json!({ "version": 1, "profiles": [] }).to_string(),
    )
    .unwrap();
    let fx = fx.restart().await;
    // Unchanged stored key round-trips; a changed value is rejected.
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { reference.clone(): "m1", "codex": "m0" } },
        ]))
        .await
        .expect("unchanged key round-trips");
    let err = fx
        .svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { reference.clone(): "m9" } },
        ]))
        .await
        .unwrap_err();
    assert_eq!(data_code(&err), "provider-profile-not-found");
    // Bare ids are unaffected.
    fx.svc
        .settings_update_checking_profiles(
            json!([{ "path": "model.defaultProvider", "value": "claude-code" }]),
        )
        .await
        .unwrap();
}

#[intent_test_macros::daemon_test]
async fn profile_values_never_reach_logs() {
    #[derive(Clone, Default)]
    struct Capture(Arc<std::sync::Mutex<Vec<String>>>);
    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, "{}={:?} ", field.name(), value);
                }
            }
            let mut visitor = Visitor(String::new());
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    let capture = Capture::default();
    let _guard = crate::test_tracing::set_capture_default(capture.clone());
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let _ = fx
        .svc
        .provider_profiles_update_impl(
            id.clone(),
            json!({ "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": format!("{SECRET}-2") }] }),
        )
        .await
        .unwrap();
    let _ = fx
        .svc
        .provider_profiles_update_impl(
            id.clone(),
            json!({ "env": [{ "key": "PATH", "kind": "set", "value": SECRET }] }),
        )
        .await
        .unwrap_err();
    fx.svc.provider_profiles_delete_impl(id).await.unwrap();
    let lines = capture.0.lock().unwrap().clone();
    assert!(lines.iter().all(|l| !l.contains(SECRET)), "{lines:?}");
}

// ---------------------------------------------------------------------------
// Review regressions: settled secret writes and the providerDefaults prune
// ---------------------------------------------------------------------------

/// Secret backend that parks the next profile `store` / `delete` until the
/// test releases it, so a write can outlive the caller's write budget.
#[derive(Default)]
struct GatedSecrets {
    inner: InMemorySecretStore,
    gate: std::sync::Mutex<Option<Gate>>,
}

struct Gate {
    op: &'static str,
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl GatedSecrets {
    fn arm(
        &self,
        op: &'static str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release) = std::sync::mpsc::channel();
        *self.gate.lock().unwrap() = Some(Gate {
            op,
            entered,
            release,
        });
        (entered_rx, release_tx)
    }

    fn pause(&self, op: &str, account: &str) {
        if !account.starts_with("providerProfiles.") {
            return;
        }
        let gate = {
            let mut gate = self.gate.lock().unwrap();
            if gate.as_ref().is_some_and(|g| g.op == op) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }
    }
}

impl SecretStore for GatedSecrets {
    fn load(&self, account: &str) -> intent_core::Result<Option<String>> {
        self.inner.load(account)
    }
    fn store(&self, account: &str, value: &str) -> intent_core::Result<()> {
        self.pause("store", account);
        self.inner.store(account, value)
    }
    fn delete(&self, account: &str) -> intent_core::Result<()> {
        self.pause("delete", account);
        self.inner.delete(account)
    }
}

/// Install `gated` behind an async wrapper with a `write_timeout` budget.
fn install_gated(fx: &mut Fixture, gated: &Arc<GatedSecrets>, write_timeout: Duration) {
    fx.svc.secrets = Arc::new(crate::settings::AsyncSecretStore::with_timings(
        gated.clone(),
        Duration::from_secs(5),
        write_timeout,
        Duration::from_secs(60),
        Duration::from_secs(60),
    ));
}

/// Release the parked write only after the caller's write budget expired.
fn release_after_budget(release: std::sync::mpsc::Sender<()>) {
    std::thread::spawn(move || {
        // timing-guard: the regression needs the write to outlive its 50ms budget
        std::thread::sleep(Duration::from_millis(300));
        let _ = release.send(());
    });
}

#[intent_test_macros::daemon_test]
async fn create_waits_for_a_slow_secret_write_instead_of_orphaning_it() {
    let mut fx = Fixture::new().await;
    let gated = Arc::new(GatedSecrets::default());
    install_gated(&mut fx, &gated, Duration::from_millis(50));
    let (entered, release) = gated.arm("store");
    let svc = fx.svc.clone();
    let create = intent_core::spawn_daemon(async move {
        svc.provider_profiles_create_impl(json!({ "baseProviderId": "codex", "name": "Slow" }))
            .await
    });
    entered.await.unwrap();
    release_after_budget(release);
    let created = create
        .await
        .unwrap()
        .expect("the settled write commits the profile");
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    assert!(gated.inner.load(&env_account(&id)).unwrap().is_some());
    let list = fx.svc.provider_profiles_list_impl(None).await.unwrap();
    assert_eq!(list["profiles"].as_array().unwrap().len(), 1);
    let file: Value =
        serde_json::from_str(&std::fs::read_to_string(fx.profiles_file()).unwrap()).unwrap();
    assert!(file.get("pendingEnvCleanup").is_none(), "{file}");
}

#[intent_test_macros::daemon_test]
async fn update_reports_a_slow_but_successful_env_write_honestly() {
    let mut fx = Fixture::new().await;
    let gated = Arc::new(GatedSecrets::default());
    install_gated(&mut fx, &gated, Duration::from_millis(50));
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "A", "kind": "set", "value": "1" }]),
        )
        .await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let mut sub = fx.subscribe();
    let (entered, release) = gated.arm("store");
    let svc = fx.svc.clone();
    let update_id = id.clone();
    let update = intent_core::spawn_daemon(async move {
        svc.provider_profiles_update_impl(
            update_id,
            json!({ "env": [{ "key": "B", "kind": "unset" }] }),
        )
        .await
    });
    entered.await.unwrap();
    release_after_budget(release);
    let updated = update
        .await
        .unwrap()
        .expect("the landed write is reported as success");
    assert_eq!(updated["activation"]["envChanged"], true);
    assert_eq!(
        updated["profile"]["env"],
        json!([{ "key": "B", "kind": "unset" }])
    );
    let ev = recv_profiles_event(&mut sub).await;
    assert_eq!(ev["data"]["change"], "updated");
    assert_eq!(ev["data"]["envChanged"], true);
}

#[intent_test_macros::daemon_test]
async fn delete_prune_does_not_revert_a_concurrent_provider_defaults_write() {
    let mut fx = Fixture::new().await;
    let gated = Arc::new(GatedSecrets::default());
    install_gated(&mut fx, &gated, Duration::from_secs(10));
    let created = fx.create("codex", "Work", json!([])).await;
    let id = created["profile"]["id"].as_str().unwrap().to_string();
    let reference = format!("codex@{id}");
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { "codex": "m0", reference.clone(): "m1" } },
        ]))
        .await
        .unwrap();

    // Park the delete between its settings read and the prune, then let
    // another writer replace the map (dropping the profile key).
    let (entered, release) = gated.arm("delete");
    let svc = fx.svc.clone();
    let delete_id = id.clone();
    let delete =
        intent_core::spawn_daemon(
            async move { svc.provider_profiles_delete_impl(delete_id).await },
        );
    entered.await.unwrap();
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.providerDefaults", "value": { "claude-code": "s1" } },
        ]))
        .await
        .unwrap();
    release.send(()).unwrap();
    delete.await.unwrap().unwrap();
    assert_eq!(
        fx.svc.effective_settings().model.provider_defaults,
        std::collections::BTreeMap::from([("claude-code".to_string(), "s1".to_string())])
    );
}

#[intent_test_macros::daemon_test]
async fn profile_values_never_reach_config_toml() {
    let fx = Fixture::new().await;
    let created = fx
        .create(
            "codex",
            "Work",
            json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }]),
        )
        .await;
    let reference = created["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_string();
    fx.svc
        .settings_update_checking_profiles(json!([
            { "path": "model.defaultProvider", "value": reference.clone() },
            { "path": "model.providerDefaults", "value": { reference: "m1" } },
        ]))
        .await
        .unwrap();
    let config = std::fs::read_to_string(fx.data_dir().join("config.toml")).unwrap();
    assert!(config.contains("codex@"), "{config}");
    assert!(
        !config.contains(SECRET) && !config.contains("OPENAI_API_KEY"),
        "{config}"
    );
}
