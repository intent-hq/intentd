//! Every integration credential saved through `settings.update` must reach
//! the lookup its integration reads (intent-hq/intent#9645: Settings saved the
//! Sentry account under one name and Sentry read another).
//!
//! The saved paths are literals on purpose: they are what Settings sends, so
//! renaming a shared constant fails here instead of silently orphaning saved
//! credentials.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;

use intent_core::settings_file::AutocompleteService;
use intent_core::WorkspaceApi;
use intent_store::Store;
use serde_json::json;

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

/// Points the default secrets store, which the module-level token loaders
/// (GitHub, GitLab, Linear, voice) read, at the daemon's own secrets file,
/// as in production where both resolve to the same default path.
struct SecretsFileEnv(Option<OsString>);

impl SecretsFileEnv {
    fn set(path: &Path) -> Self {
        let saved = std::env::var_os(SECRETS_FILE_ENV);
        std::env::set_var(SECRETS_FILE_ENV, path);
        Self(saved)
    }
}

impl Drop for SecretsFileEnv {
    fn drop(&mut self) {
        match &self.0 {
            Some(v) => std::env::set_var(SECRETS_FILE_ENV, v),
            None => std::env::remove_var(SECRETS_FILE_ENV),
        }
    }
}

#[intent_test_macros::daemon_test]
async fn saved_credentials_reach_their_integrations() {
    let dir = test_tempdir("intentd-saved-credentials-");
    let secrets_path = dir.path().join(".secrets.json");
    let _env = SecretsFileEnv::set(&secrets_path);
    let store = Store::open(&dir.path().join("db.sqlite"))
        .await
        .expect("open store");
    let registry =
        Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry"));
    let svc = Services::new_with_file_secrets(
        store,
        intent_core::FileSecretStore::with_path(&secrets_path),
    )
    .with_settings_registry(registry);

    let changes: Vec<_> = SAVED
        .iter()
        .map(|(path, value)| json!({ "path": path, "value": value }))
        .collect();
    svc.settings_update(json!(changes))
        .await
        .expect("settings.update saves every credential");

    let sentry = svc.saved_sentry_settings().await.expect("sentry account");
    let found: BTreeMap<&str, Option<String>> = [
        (
            "sourceControl.github.token",
            intent_sourcecontrol::token::resolve(&intent_sourcecontrol::TokenSource::Explicit)
                .await,
        ),
        (
            "sourceControl.gitlab.token",
            intent_sourcecontrol::gitlab_token::resolve_gitlab_token(
                intent_sourcecontrol::GitlabTokenSource::Explicit,
            )
            .await,
        ),
        (
            "linear.token",
            intent_linear::token::resolve(&intent_linear::TokenSource::Explicit).await,
        ),
        ("accounts.sentry.token", sentry.token),
        ("accounts.sentry.organization", sentry.organization),
        (
            "voice.elevenlabs.apiKey",
            intent_voice::token::resolve(intent_voice::VoiceProvider::ElevenLabs).await,
        ),
        (
            "voice.openai.apiKey",
            intent_voice::token::resolve(intent_voice::VoiceProvider::OpenAi).await,
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
    .collect();
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
