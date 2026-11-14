//! The one-time settings upgrades the daemon runs at boot, in dependency
//! order. Each step tolerates a failed `config.toml` write (it logs and the
//! next boot retries), so the order and the hand-off between steps are the
//! contract this module owns and tests as a whole.

use intent_core::Result;
use intent_store::Store;

use crate::settings::{
    cleanup_retired_settings, import_legacy_settings, migrate_active_provider_setting,
    migrate_auto_retire_setting, migrate_default_vocabulary, migrate_quick_action_settings,
    migrate_quick_action_shared_default,
};
use crate::settings_registry::SettingsRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BootSettingsMigration {
    /// `[backgroundAgents]` → `quickActions.*` (monorepo#1729).
    QuickActionCarryOver,
    /// `providers.active` → `model.defaultProvider`.
    ActiveProvider,
    /// Shared quick action model and effort → each action.
    QuickActionSharedDefault,
    /// `workspace.autoRetireArchivedAfterDays` → `workspace.purgeArchivedAfterDays`.
    AutoRetire,
    /// Import, then strip, the retired keys captured at load.
    ImportLegacy,
    /// Delete `SQLite` rows of retired settings.
    CleanupRetired,
    /// Delete a stored `voice.vocabulary` equal to the retired seed default.
    DefaultVocabulary,
}

/// Dependencies:
/// - `QuickActionCarryOver` reads `[backgroundAgents]`, which `ImportLegacy`
///   strips, and can fill the shared key `QuickActionSharedDefault` folds.
///   While a carry-over write is pending, `ImportLegacy` is skipped.
/// - `QuickActionSharedDefault` reads the default provider; it also reads a
///   still-pending `providers.active`, so it survives a failed
///   `ActiveProvider` write.
pub(crate) const BOOT_SETTINGS_MIGRATIONS: [BootSettingsMigration; 7] = [
    BootSettingsMigration::QuickActionCarryOver,
    BootSettingsMigration::ActiveProvider,
    BootSettingsMigration::QuickActionSharedDefault,
    BootSettingsMigration::AutoRetire,
    BootSettingsMigration::ImportLegacy,
    BootSettingsMigration::CleanupRetired,
    BootSettingsMigration::DefaultVocabulary,
];

/// Run every boot settings migration in [`BOOT_SETTINGS_MIGRATIONS`] order.
///
/// # Errors
///
/// Returns the first store error (importing a legacy value, deleting a
/// retired row); later steps do not run. A failed `config.toml` write never
/// errors: the step logs it and the next boot retries.
pub async fn run_boot_settings_migrations(
    registry: &SettingsRegistry,
    store: &Store,
) -> Result<()> {
    run_steps(registry, store, |_| {}).await
}

async fn run_steps(
    registry: &SettingsRegistry,
    store: &Store,
    mut before_step: impl FnMut(BootSettingsMigration),
) -> Result<()> {
    let mut legacy_carried = true;
    for step in BOOT_SETTINGS_MIGRATIONS {
        before_step(step);
        match step {
            BootSettingsMigration::QuickActionCarryOver => {
                legacy_carried = migrate_quick_action_settings(registry);
            }
            BootSettingsMigration::ActiveProvider => migrate_active_provider_setting(registry),
            BootSettingsMigration::QuickActionSharedDefault => {
                migrate_quick_action_shared_default(registry);
            }
            BootSettingsMigration::AutoRetire => migrate_auto_retire_setting(registry),
            BootSettingsMigration::ImportLegacy if !legacy_carried => {
                tracing::warn!(
                    "keeping legacy config.toml keys until [backgroundAgents] is carried over \
                     (next boot retries)"
                );
            }
            BootSettingsMigration::ImportLegacy => {
                import_legacy_settings(registry, store).await?;
            }
            BootSettingsMigration::CleanupRetired => cleanup_retired_settings(store).await?,
            BootSettingsMigration::DefaultVocabulary => migrate_default_vocabulary(store).await?,
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use serde_json::{json, Value};

    use super::*;
    use crate::settings::{
        DEPRECATED_ACTIVE_PROVIDER_PATH, DEPRECATED_AUTO_RETIRE_PATH,
        DEPRECATED_QUICK_ACTION_SHARED_PATHS, PURGE_ARCHIVED_AFTER_DAYS_PATH,
        RETIRED_WORKSPACE_OVERRIDES_PATH, VOICE_VOCABULARY_PATH,
    };
    use crate::settings_registry::SettingOrigin;

    /// A config.toml from before every upgrade step.
    const OLD_CONFIG: &str = "# keep me\n\
        [providers]\nactive = \"codex\"\n\n\
        [backgroundAgents]\ndefaultModel = \"codex:gpt-5\"\ntypeOverrides = { commit = \"o3\" }\n\n\
        [quickActions]\ndefaultReasoningEffort = \"low\"\n\n\
        [workspace]\nautoRetireArchivedAfterDays = 14\n";

    struct Fixture {
        _dir: tempfile::TempDir,
        config_dir: PathBuf,
        config_path: PathBuf,
        store: Store,
    }

    impl Fixture {
        async fn new(config: &str) -> Self {
            let dir = crate::test_support::test_tempdir("intentd-boot-migrations-");
            let config_dir = dir.path().join("config");
            std::fs::create_dir(&config_dir).expect("mkdir config");
            let config_path = config_dir.join("config.toml");
            std::fs::write(&config_path, config).expect("seed config");
            let store = Store::open(&dir.path().join("intent.db"))
                .await
                .expect("open store");
            let legacy_vocabulary =
                serde_json::to_string(&json!(crate::voice_ops::LEGACY_DEFAULT_VOCABULARY))
                    .expect("encode vocabulary");
            store
                .set_setting(VOICE_VOCABULARY_PATH, &legacy_vocabulary)
                .await
                .expect("seed vocabulary");
            store
                .set_setting(RETIRED_WORKSPACE_OVERRIDES_PATH, "{\"ws1\":\"m1\"}")
                .await
                .expect("seed retired row");
            Self {
                _dir: dir,
                config_dir,
                config_path,
                store,
            }
        }

        /// One daemon boot. When `fail` is set, that step's `config.toml`
        /// writes fail (read-only config directory) and every other step's
        /// succeed.
        async fn boot(&self, fail: Option<BootSettingsMigration>) -> SettingsRegistry {
            let registry = SettingsRegistry::load(&self.config_path).expect("load registry");
            let result = run_steps(&registry, &self.store, |step| {
                set_writable(&self.config_dir, Some(step) != fail);
            })
            .await;
            set_writable(&self.config_dir, true);
            result.expect("boot migrations");
            registry
        }

        fn text(&self) -> String {
            std::fs::read_to_string(&self.config_path).expect("read config")
        }
    }

    fn set_writable(dir: &Path, writable: bool) {
        let mode = if writable { 0o755 } else { 0o555 };
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn all_actions(value: &str) -> Value {
        json!({ "commit": value, "pr": value, "review": value, "walkthrough": value })
    }

    /// Every user choice in [`OLD_CONFIG`] lands on its current key, and no
    /// deprecated key is left in the file.
    async fn assert_upgraded(context: &str, fx: &Fixture, registry: &SettingsRegistry) {
        assert_eq!(
            registry.get("model.defaultProvider"),
            Some(json!("codex")),
            "{context}"
        );
        assert_eq!(
            registry.get("quickActions.typeOverrides"),
            Some(
                json!({ "commit": "o3", "pr": "gpt-5", "review": "gpt-5", "walkthrough": "gpt-5" })
            ),
            "{context}: the [backgroundAgents] shared model reaches every inheriting action"
        );
        assert_eq!(
            registry.get("quickActions.typeReasoningEffortOverrides"),
            Some(all_actions("low")),
            "{context}"
        );
        assert_eq!(
            registry.get(PURGE_ARCHIVED_AFTER_DAYS_PATH),
            Some(json!(14)),
            "{context}"
        );
        let removed = [DEPRECATED_ACTIVE_PROVIDER_PATH, DEPRECATED_AUTO_RETIRE_PATH]
            .into_iter()
            .chain(DEPRECATED_QUICK_ACTION_SHARED_PATHS.iter().copied());
        for path in removed {
            assert_eq!(
                registry.origin(path),
                Some(SettingOrigin::Default),
                "{context}: {path}"
            );
        }
        let text = fx.text();
        assert!(
            text.contains("# keep me"),
            "{context}: comment lost:\n{text}"
        );
        for key in [
            "active",
            "backgroundAgents",
            "defaultModel",
            "defaultReasoningEffort",
            "autoRetire",
        ] {
            assert!(!text.contains(key), "{context}: {key} left in:\n{text}");
        }
        assert_eq!(
            fx.store
                .get_setting(VOICE_VOCABULARY_PATH)
                .await
                .expect("read"),
            None,
            "{context}"
        );
        assert_eq!(
            fx.store
                .get_setting(RETIRED_WORKSPACE_OVERRIDES_PATH)
                .await
                .expect("read"),
            None,
            "{context}"
        );
    }

    #[tokio::test]
    async fn boot_settings_migrations_upgrade_an_old_config() {
        let fx = Fixture::new(OLD_CONFIG).await;
        let registry = fx.boot(None).await;
        assert_upgraded("first boot", &fx, &registry).await;
        let reloaded = SettingsRegistry::load(&fx.config_path).expect("reload");
        assert_upgraded("reloaded file", &fx, &reloaded).await;

        let migrated = fx.text();
        fx.boot(None).await;
        assert_eq!(fx.text(), migrated, "a second boot never rewrites the file");
    }

    /// intentd#3349: while the `providers.active` carry-over is pending, the
    /// shared model of that provider is still kept.
    #[tokio::test]
    async fn boot_settings_migrations_keep_the_shared_model_when_the_provider_write_fails() {
        let fx = Fixture::new(OLD_CONFIG).await;
        let registry = fx.boot(Some(BootSettingsMigration::ActiveProvider)).await;
        assert_eq!(
            registry.get("quickActions.typeOverrides"),
            Some(
                json!({ "commit": "o3", "pr": "gpt-5", "review": "gpt-5", "walkthrough": "gpt-5" })
            )
        );
        assert_eq!(
            registry.get(DEPRECATED_ACTIVE_PROVIDER_PATH),
            Some(json!("codex"))
        );
        assert_upgraded("next boot", &fx, &fx.boot(None).await).await;
    }

    /// A step whose write fails loses nothing: the next boot reaches the same
    /// final settings as a boot where every step succeeded.
    #[tokio::test]
    async fn boot_settings_migrations_recover_from_any_failed_step() {
        for step in BOOT_SETTINGS_MIGRATIONS {
            let fx = Fixture::new(OLD_CONFIG).await;
            fx.boot(Some(step)).await;
            let registry = fx.boot(None).await;
            assert_upgraded(
                &format!("boot after a failed {step:?} write"),
                &fx,
                &registry,
            )
            .await;
        }
    }
}
