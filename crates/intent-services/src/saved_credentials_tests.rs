//! Every integration credential saved through `settings.update` must reach
//! the lookup its integration reads (intent-hq/intent#9645: Settings saved the
//! Sentry account under one name and Sentry read another).
//!
//! The saved paths are literals on purpose: they are what Settings sends, so
//! renaming a shared constant fails here instead of silently orphaning saved
//! credentials.

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_core::settings_file::AutocompleteService;
use intent_core::WorkspaceApi;
use intent_store::Store;
use serde_json::json;

use crate::test_support::fixtures::EnvGuard;
use crate::test_support::test_tempdir;
use crate::{Services, SettingsRegistry};

const SECRETS_FILE_ENV: &str = "INTENTD_SECRETS_FILE";

/// Sensitive settings that are not an integration credential.
const NOT_INTEGRATION_CREDENTIALS: &[&str] = &["server.auth.token", "mcp.servers"];

const SAVED: &[(&str, &str)] = &[
    ("sourceControl.github.token", "gh-saved"),
    ("sourceControl.gitlab.token", "glpat-saved"),
    ("linear.token", "lin_api_saved"),
    ("accounts.sentry.token", "sntryu_saved"),
    ("accounts.sentry.organization", "acme"),
    ("voice.elevenlabs.apiKey", "el-saved"),
    ("voice.openai.apiKey", "sk-openai-saved"),
    ("autocomplete.kilo.apiKey", "kilo-saved"),
    ("autocomplete.openrouter.apiKey", "sk-or-saved"),
];

/// What each integration's lookup resolves for `svc`: the stored-secret
/// sources only, so env / `gh` fallbacks never leak into the comparison.
async fn integration_lookups(svc: &Services) -> BTreeMap<&'static str, Option<String>> {
    let secrets = svc.secret_source();
    let sentry = svc.saved_sentry_settings().await.expect("sentry account");
    [
        (
            "sourceControl.github.token",
            intent_sourcecontrol::token::resolve(
                &intent_sourcecontrol::TokenSource::Explicit,
                &secrets,
            )
            .await,
        ),
        (
            "sourceControl.gitlab.token",
            intent_sourcecontrol::gitlab_token::resolve_gitlab_token(
                intent_sourcecontrol::GitlabTokenSource::Explicit,
                &secrets,
            )
            .await,
        ),
        (
            "linear.token",
            intent_linear::token::resolve(&intent_linear::TokenSource::Explicit, &secrets).await,
        ),
        ("accounts.sentry.token", sentry.token),
        ("accounts.sentry.organization", sentry.organization),
        (
            "voice.elevenlabs.apiKey",
            intent_voice::token::resolve(intent_voice::VoiceProvider::ElevenLabs, &secrets).await,
        ),
        (
            "voice.openai.apiKey",
            intent_voice::token::resolve(intent_voice::VoiceProvider::OpenAi, &secrets).await,
        ),
        (
            "autocomplete.kilo.apiKey",
            svc.saved_autocomplete_key(AutocompleteService::Kilo)
                .await
                .expect("kilo key"),
        ),
        (
            "autocomplete.openrouter.apiKey",
            svc.saved_autocomplete_key(AutocompleteService::Openrouter)
                .await
                .expect("openrouter key"),
        ),
    ]
    .into_iter()
    .collect()
}

/// A `Services` over its own secrets file and settings registry under `dir`,
/// with every `SAVED` credential written as `<value><suffix>`.
async fn services_with_saved(dir: &std::path::Path, suffix: &str) -> Services {
    let store = Store::open(&dir.join("db.sqlite"))
        .await
        .expect("open store");
    let registry =
        Arc::new(SettingsRegistry::load(dir.join("config.toml")).expect("load registry"));
    let svc = Services::new_with_secret_file(
        store,
        intent_core::FileSecretStore::with_path(dir.join(".secrets.json")),
    )
    .with_settings_registry(registry);
    let changes: Vec<_> = SAVED
        .iter()
        .map(|(path, value)| json!({ "path": path, "value": format!("{value}{suffix}") }))
        .collect();
    svc.settings_update(json!(changes))
        .await
        .expect("settings.update saves every credential");
    svc
}

#[intent_test_macros::daemon_test]
async fn saved_credentials_reach_their_integrations() {
    let dir = test_tempdir("intentd-saved-credentials-");
    let svc = services_with_saved(dir.path(), "").await;
    let found = integration_lookups(&svc).await;
    let want: BTreeMap<&str, Option<String>> = SAVED
        .iter()
        .map(|(path, value)| (*path, Some((*value).to_string())))
        .collect();
    assert_eq!(
        found, want,
        "a saved credential did not reach its integration"
    );

    for def in crate::settings::definitions() {
        if def.sensitive && !NOT_INTEGRATION_CREDENTIALS.contains(&def.path) {
            assert!(
                want.contains_key(def.path),
                "`{}` is a saved credential: add it and its integration lookup here",
                def.path
            );
        }
    }
}

/// Two orchestrator tenants in one process, each with its own secrets file,
/// never see each other's credentials — nor the process-wide default file
/// (`INTENTD_SECRETS_FILE`), which every integration lookup used to read.
#[intent_test_macros::daemon_test]
async fn tenant_credentials_stay_in_their_own_store() {
    let host = test_tempdir("intentd-host-secrets-");
    let host_file = host.path().join(".secrets.json");
    let host_store = intent_core::FileSecretStore::with_path(&host_file);
    for (path, value) in SAVED {
        host_store
            .store(path, &format!("{value}-host"))
            .expect("seed the host file");
    }
    let _env = EnvGuard::set_all(&[(
        SECRETS_FILE_ENV,
        host_file.to_str().expect("utf-8 temp path"),
    )]);

    let a_dir = test_tempdir("intentd-tenant-a-");
    let b_dir = test_tempdir("intentd-tenant-b-");
    let a = services_with_saved(a_dir.path(), "-a").await;
    let b = services_with_saved(b_dir.path(), "-b").await;
    let empty_dir = test_tempdir("intentd-tenant-empty-");
    let empty = Services::new_with_secret_file(
        Store::open(&empty_dir.path().join("db.sqlite"))
            .await
            .expect("open store"),
        intent_core::FileSecretStore::with_path(empty_dir.path().join(".secrets.json")),
    )
    .with_settings_registry(Arc::new(
        SettingsRegistry::load(empty_dir.path().join("config.toml")).expect("load registry"),
    ));

    for (svc, suffix) in [(&a, "-a"), (&b, "-b")] {
        let want: BTreeMap<&str, Option<String>> = SAVED
            .iter()
            .map(|(path, value)| (*path, Some(format!("{value}{suffix}"))))
            .collect();
        assert_eq!(
            integration_lookups(svc).await,
            want,
            "tenant{suffix} resolved a credential that is not its own"
        );
    }
    let none: BTreeMap<&str, Option<String>> =
        SAVED.iter().map(|(path, _)| (*path, None)).collect();
    assert_eq!(
        integration_lookups(&empty).await,
        none,
        "a tenant with no credentials resolved the host's"
    );
    for (path, value) in SAVED {
        assert_eq!(
            host_store
                .load(path)
                .expect("read the host file")
                .as_deref(),
            Some(format!("{value}-host").as_str()),
            "a tenant write reached the host file"
        );
    }
}

/// The `auto` chains' host-wide fallbacks (env vars; `gh` is after env, so
/// never reached here) serve the desktop but never an orchestrator tenant.
#[intent_test_macros::daemon_test]
async fn tenant_never_borrows_host_env_credentials() {
    let _env = EnvGuard::set_all(&[
        ("GITHUB_TOKEN", "gh-host-env"),
        ("GITLAB_TOKEN", "glpat-host-env"),
        ("LINEAR_API_KEY", "lin-host-env"),
        ("ELEVENLABS_API_KEY", "el-host-env"),
        ("OPENAI_API_KEY", "sk-host-env"),
        ("SENTRY_API_TOKEN", "sntry-host-env"),
        ("SENTRY_ORG", "host-org"),
    ]);
    let open = |dir: &std::path::Path| {
        let dir = dir.to_path_buf();
        async move {
            Services::new_with_secret_file(
                Store::open(&dir.join("db.sqlite"))
                    .await
                    .expect("open store"),
                intent_core::FileSecretStore::with_path(dir.join(".secrets.json")),
            )
            .with_settings_registry(Arc::new(
                SettingsRegistry::load(dir.join("config.toml")).expect("load registry"),
            ))
        }
    };
    let desktop_dir = test_tempdir("intentd-desktop-env-");
    let tenant_dir = test_tempdir("intentd-tenant-env-");
    let desktop = open(desktop_dir.path()).await;
    let tenant = open(tenant_dir.path()).await.with_orchestrator_mode(true);

    async fn auto(svc: &Services) -> [Option<String>; 5] {
        let secrets = svc.secret_source();
        [
            intent_sourcecontrol::token::resolve(
                &intent_sourcecontrol::TokenSource::Auto,
                &secrets,
            )
            .await,
            intent_sourcecontrol::gitlab_token::resolve_gitlab_token(
                intent_sourcecontrol::GitlabTokenSource::Auto,
                &secrets,
            )
            .await,
            intent_linear::token::resolve(&intent_linear::TokenSource::Auto, &secrets).await,
            intent_voice::token::resolve(intent_voice::VoiceProvider::ElevenLabs, &secrets).await,
            intent_voice::token::resolve(intent_voice::VoiceProvider::OpenAi, &secrets).await,
        ]
    }
    assert_eq!(
        auto(&desktop).await,
        [
            "gh-host-env",
            "glpat-host-env",
            "lin-host-env",
            "el-host-env",
            "sk-host-env"
        ]
        .map(|v| Some(v.to_string())),
        "desktop keeps its env fallbacks"
    );
    assert_eq!(
        auto(&tenant).await,
        [None, None, None, None, None],
        "a tenant borrowed a host env credential"
    );
    assert!(desktop.resolve_sentry_engine().await.is_ok());
    assert!(
        tenant.resolve_sentry_engine().await.is_err(),
        "a tenant borrowed the host's SENTRY_* account"
    );
    let skipped = intent_sourcecontrol::token::resolve_detailed(
        &intent_sourcecontrol::TokenSource::GhCli,
        &tenant.secret_source(),
    )
    .await;
    assert!(skipped.token.is_none());
    assert!(
        skipped
            .skipped
            .iter()
            .any(|s| s.contains("orchestrator tenant")),
        "{:?}",
        skipped.skipped
    );
}
