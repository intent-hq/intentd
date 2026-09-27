use super::*;
use crate::repository_credentials::{RepositoryConnectionDirectory, RepositoryCredentialError};
use crate::source_control_auth_ops::repository_owner_tests::Server;
use intent_core::WorkspaceApi;
use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
use serde_json::json;

struct Fixture {
    dir: tempfile::TempDir,
    services: crate::Services,
    registry: Arc<SettingsRegistry>,
    directory: Arc<RepositoryConnectionDirectory>,
}
impl Fixture {
    async fn invalid_boot(server: &Server, case: usize, installed: bool) -> Self {
        let dir = crate::test_support::test_tempdir("repository-invalid-boot-settings");
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let logical = match case {
            0 => "host = \"gitlab.test\"\ninstanceBaseUrl = \"https://other.test/forge\"",
            1 => "host = \"gitlab.test\"\ninstanceBaseUrl = \"https://gitlab.test/forge/%2e%2e\"",
            _ => "host = \"https://user:fixture-secret@gitlab.test\"",
        };
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            format!(
                "[sourceControl.gitlab]\n{logical}\noauthClientId = \"client\"\napiBaseUrl = {:?}\n",
                server.host.base_url()
            ),
        )
        .unwrap();
        let registry = Arc::new(SettingsRegistry::load(path).unwrap());
        let secret = FileSecretStore::with_path(dir.path().join("secrets.json"));
        secret.store(SECRET_ACCOUNT, "stored-pat").unwrap();
        let services = crate::Services::new_repository_fixture(store, secret, None)
            .with_settings_registry(registry.clone());
        let directory = services.repository_connection_directory();
        if installed {
            services
                .gitlab_credential_gate
                .install_settings_boundary(
                    &registry,
                    &services.secrets,
                    &services.gitlab_secret_store,
                    Some(server.descriptor.clone()),
                )
                .unwrap();
        }
        Self {
            dir,
            services,
            registry,
            directory,
        }
    }

    async fn uninstalled(server: &Server) -> Self {
        let dir = crate::test_support::test_tempdir("repository-adoption");
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!("gitlab.test")),
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!("https://gitlab.test/forge"),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(server.host.base_url()),
                ),
                ("sourceControl.gitlab.oauthClientId".into(), json!("client")),
            ])
            .unwrap();
        let secret = FileSecretStore::with_path(dir.path().join("secrets.json"));
        secret.store(SECRET_ACCOUNT, "stored-pat").unwrap();
        let services = crate::Services::new_repository_fixture(store, secret, None)
            .with_settings_registry(registry.clone());
        let directory = services.repository_connection_directory();
        Self {
            dir,
            services,
            registry,
            directory,
        }
    }
    async fn new(server: &Server) -> Self {
        let f = Self::uninstalled(server).await;
        f.services
            .gitlab_credential_gate
            .install_settings_boundary(
                &f.registry,
                &f.services.secrets,
                &f.services.gitlab_secret_store,
                Some(server.descriptor.clone()),
            )
            .unwrap();
        f
    }

    fn service(&self, write: Arc<RepositorySettingsWrite>) -> crate::settings::SettingsService<'_> {
        self.services
            .settings_service()
            .with_repository_write(Some(write))
    }
}

#[derive(Clone, Copy)]
enum UnrelatedWrite {
    Apply,
    Reload,
    Pin,
    Noop,
    PinnedReload,
}

async fn unchanged_invalid_boot_survives(operation: UnrelatedWrite) {
    let mut failures = Vec::new();
    for installed in [false, true] {
        for case in 0..3 {
            let server = Server::new().await;
            let f = Fixture::invalid_boot(&server, case, installed).await;
            let before = f
                .registry
                .snapshot()
                .effective
                .source_control
                .gitlab
                .clone();
            let secret = std::fs::read(f.services.gitlab_secret_store.path()).unwrap();
            let text = std::fs::read_to_string(f.registry.config_path()).unwrap();
            let result = match operation {
                UnrelatedWrite::Apply => f
                    .registry
                    .apply(&[("git.autoCommit".into(), json!(false))])
                    .map(|_| ()),
                UnrelatedWrite::Reload => f
                    .registry
                    .prepare_repository_reload(&format!("{text}\n[git]\nautoCommit = false\n"))
                    .and_then(|prepared| f.registry.publish_repository_reload(prepared, None))
                    .map(|_| ()),
                UnrelatedWrite::Pin => f.registry.pin("git.autoCommit", json!(false), "fixture"),
                UnrelatedWrite::Noop => f
                    .registry
                    .apply(&[("sourceControl.gitlab.oauthClientId".into(), json!("client"))])
                    .map(|_| ()),
                UnrelatedWrite::PinnedReload => f
                    .registry
                    .pin(
                        "sourceControl.gitlab.oauthClientId",
                        json!("client"),
                        "fixture",
                    )
                    .and_then(|()| {
                        f.registry.prepare_repository_reload(
                            &text.replace("\"client\"", "\"file-only-client\""),
                        )
                    })
                    .and_then(|prepared| f.registry.publish_repository_reload(prepared, None))
                    .map(|_| ()),
            };
            if let Err(error) = result {
                failures.push(format!("case {case}, boundary {installed}: {error}"));
            } else if matches!(
                operation,
                UnrelatedWrite::Apply | UnrelatedWrite::Reload | UnrelatedWrite::Pin
            ) {
                assert_eq!(f.registry.get("git.autoCommit"), Some(json!(false)));
            }
            assert_eq!(
                f.registry.snapshot().effective.source_control.gitlab,
                before
            );
            assert_eq!(
                f.directory.binding().unwrap_err(),
                RepositoryCredentialError::Unverified
            );
            assert!(f
                .services
                .reconcile_gitlab_repository_binding()
                .await
                .is_err());
            assert!(server.control.requests.lock().unwrap().is_empty());
            assert_eq!(
                std::fs::read(f.services.gitlab_secret_store.path()).unwrap(),
                secret
            );
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_boot_gitlab_allows_unrelated_apply() {
    unchanged_invalid_boot_survives(UnrelatedWrite::Apply).await;
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_boot_gitlab_allows_prepared_reload() {
    unchanged_invalid_boot_survives(UnrelatedWrite::Reload).await;
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_boot_gitlab_allows_unrelated_pin() {
    unchanged_invalid_boot_survives(UnrelatedWrite::Pin).await;
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_boot_gitlab_allows_effective_noop() {
    unchanged_invalid_boot_survives(UnrelatedWrite::Noop).await;
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_boot_gitlab_compares_pinned_effective_values_on_reload() {
    unchanged_invalid_boot_survives(UnrelatedWrite::PinnedReload).await;
}

#[intent_test_macros::daemon_test]
async fn relevant_invalid_gitlab_transitions_still_fail_before_publication() {
    for installed in [false, true] {
        let server = Server::new().await;
        let f = if installed {
            Fixture::new(&server).await
        } else {
            Fixture::uninstalled(&server).await
        };
        let before = f.registry.snapshot();
        let bytes = std::fs::read(f.registry.config_path()).unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(f
            .registry
            .apply(&[(
                "sourceControl.gitlab.instanceBaseUrl".into(),
                json!("https://other.test/forge"),
            )])
            .is_err());
        assert!(f
            .registry
            .prepare_repository_reload(
                &text.replace("https://gitlab.test/forge", "https://other.test/forge",)
            )
            .is_err());
        assert!(f
            .registry
            .pin(
                "sourceControl.gitlab.instanceBaseUrl",
                json!("https://other.test/forge"),
                "fixture",
            )
            .is_err());
        assert!(Arc::ptr_eq(&before, &f.registry.snapshot()));
        assert_eq!(std::fs::read(f.registry.config_path()).unwrap(), bytes);
        assert_eq!(
            f.directory.binding().unwrap_err(),
            RepositoryCredentialError::Unverified
        );
        assert!(server.control.requests.lock().unwrap().is_empty());
    }
}

#[intent_test_macros::daemon_test]
async fn prepared_unchanged_invalid_gitlab_cannot_republish_after_relevant_change() {
    for installed in [false, true] {
        let server = Server::new().await;
        let f = Fixture::invalid_boot(&server, 0, installed).await;
        let text = std::fs::read_to_string(f.registry.config_path()).unwrap();
        let prepared = f.registry.prepare_repository_reload(&text).unwrap();
        let changes = vec![(
            "sourceControl.gitlab.instanceBaseUrl".into(),
            json!("https://gitlab.test/forge"),
        )];
        let guard = f.services.gitlab_credential_gate.lock().await;
        let write = if installed {
            let candidate = f.registry.preview(&changes).unwrap();
            Some(
                f.services
                    .gitlab_credential_gate
                    .prepare_settings(&f.registry, &candidate, &guard)
                    .unwrap(),
            )
        } else {
            None
        };
        f.registry
            .apply_with_repository_write(&changes, write.as_deref(), None)
            .unwrap();
        let changed = f.registry.snapshot();
        assert!(f
            .registry
            .publish_repository_reload(prepared, None)
            .is_err());
        assert!(Arc::ptr_eq(&changed, &f.registry.snapshot()));
        assert!(server.control.requests.lock().unwrap().is_empty());
    }
}

#[intent_test_macros::daemon_test]
async fn unchanged_invalid_gitlab_does_not_skip_actual_secret_mutation() {
    let server = Server::new().await;
    let f = Fixture::invalid_boot(&server, 0, true).await;
    let original = f
        .registry
        .snapshot()
        .effective
        .source_control
        .gitlab
        .clone();
    f.services
        .settings_update(json!([{"path":SECRET_ACCOUNT,"value":"pat-second"}]))
        .await
        .unwrap();
    assert_eq!(
        f.registry.snapshot().effective.source_control.gitlab,
        original
    );
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("pat-second")
    );
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    assert!(server.control.requests.lock().unwrap().is_empty());
}

#[intent_test_macros::daemon_test]
async fn initializer_waits_for_original_gate_before_installing_settings_boundary() {
    use std::future::Future;
    use std::task::Poll;

    let server = Server::new().await;
    let f = Fixture::uninstalled(&server).await;
    let guard = f.services.gitlab_credential_gate.lock().await;
    let initializer = f.services.initialize_gitlab_repository_binding();
    tokio::pin!(initializer);
    std::future::poll_fn(|cx| {
        assert!(initializer.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(!f.services.gitlab_credential_gate.has_settings_boundary());
    assert!(server.control.requests.lock().unwrap().is_empty());
    drop(guard);
    // Without an approved fixture transport the real initializer remains
    // unavailable, but installs exactly once after the original writer releases.
    assert!(initializer.await.is_err());
    assert!(f.services.gitlab_credential_gate.has_settings_boundary());
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    assert!(server.control.requests.lock().unwrap().is_empty());
    assert!(f
        .services
        .initialize_gitlab_repository_binding()
        .await
        .is_err());
}

#[intent_test_macros::daemon_test]
async fn unprepared_registry_apply_is_rejected_before_file_or_snapshot_effect() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = std::fs::read(f.registry.config_path()).unwrap();
    let snapshot = f.registry.snapshot();
    let result = f.registry.apply(&[(
        "sourceControl.gitlab.oauthClientId".into(),
        json!("new-client"),
    )]);
    assert!(
        result.is_err(),
        "an unprepared relevant write must not escape the boundary"
    );
    assert_eq!(std::fs::read(f.registry.config_path()).unwrap(), before);
    assert!(Arc::ptr_eq(&snapshot, &f.registry.snapshot()));
}

#[intent_test_macros::daemon_test]
async fn first_pat_adoption_uses_verified_account_without_rewriting_the_secret() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = std::fs::read(f.services.gitlab_secret_store.path()).unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    assert_eq!(f.directory.binding().unwrap().account.account_id, "42");
    assert_eq!(
        std::fs::read(f.services.gitlab_secret_store.path()).unwrap(),
        before
    );
    assert_eq!(
        server.control.requests.lock().unwrap().as_slice(),
        ["/api/v4/user", "/api/v4/personal_access_tokens/self"]
    );
}

#[intent_test_macros::daemon_test]
async fn unprepared_reload_and_pin_fail_but_unrelated_and_effective_noops_survive() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let binding = f.directory.binding().unwrap();
    let text = std::fs::read_to_string(f.registry.config_path()).unwrap();
    assert!(f
        .registry
        .reload(&text.replace("client", "other-client"))
        .is_err());
    assert!(f
        .registry
        .pin("sourceControl.gitlab.oauthClientId", json!("other"), "test")
        .is_err());
    f.registry
        .apply(&[("git.autoCommit".into(), json!(false))])
        .unwrap();
    f.registry
        .apply(&[("sourceControl.gitlab.oauthClientId".into(), json!("client"))])
        .unwrap();
    assert_eq!(f.directory.binding().unwrap(), binding);
}

#[intent_test_macros::daemon_test]
async fn unproven_transport_and_mixed_store_never_dispatch_a_managed_token() {
    for mixed in [false, true] {
        let server = Server::new().await;
        let mut f = Fixture::uninstalled(&server).await;
        if mixed {
            f.services.secrets = Arc::new(AsyncSecretStore::new(Arc::new(
                crate::settings::InMemorySecretStore::default(),
            )));
        }
        f.services
            .gitlab_credential_gate
            .install_settings_boundary(
                &f.registry,
                &f.services.secrets,
                &f.services.gitlab_secret_store,
                if mixed {
                    Some(server.descriptor.clone())
                } else {
                    None
                },
            )
            .unwrap();
        assert!(f
            .services
            .reconcile_gitlab_repository_binding()
            .await
            .is_err());
        assert!(server.control.requests.lock().unwrap().is_empty());
        assert_eq!(
            f.directory.binding().unwrap_err(),
            RepositoryCredentialError::Unverified
        );
    }
}

#[intent_test_macros::daemon_test]
async fn prepared_reload_rejects_a_changed_original_snapshot_before_retirement() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let binding = f.directory.binding().unwrap();
    let text = std::fs::read_to_string(f.registry.config_path())
        .unwrap()
        .replace("client", "new-client");
    let prepared = f.registry.prepare_repository_reload(&text).unwrap();
    f.registry
        .apply(&[("git.autoCommit".into(), json!(false))])
        .unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    let write = f
        .services
        .gitlab_credential_gate
        .prepare_settings(&f.registry, prepared.snapshot(), &guard)
        .unwrap();
    assert!(f
        .registry
        .publish_repository_reload(prepared, Some(&write))
        .is_err());
    assert!(!write.began());
    assert_eq!(f.directory.binding().unwrap(), binding);
}

#[intent_test_macros::daemon_test]
async fn actual_settings_pat_replacement_verifies_only_after_full_batch() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let prior = f.directory.binding().unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    let changes = json!([{"path":SECRET_ACCOUNT,"value":"pat-second"}]);
    let write = f
        .services
        .prepare_gitlab_repository_settings(&changes, Some(&guard))
        .unwrap()
        .unwrap();
    let service = f.service(write.clone());
    service.update(&changes).await.unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    f.services
        .finish_gitlab_repository_settings(&write, false)
        .await
        .unwrap();
    let next = f.directory.binding().unwrap();
    assert_eq!(next.account.account_id, "43");
    assert_ne!(prior.scope, next.scope);
}

#[intent_test_macros::daemon_test]
async fn identical_pat_placeholder_and_rejected_preflight_do_not_retire() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let prior = f.directory.binding().unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    for value in ["stored-pat", crate::settings::REDACTED_PLACEHOLDER] {
        let changes = json!([{"path":SECRET_ACCOUNT,"value":value}]);
        let write = f
            .services
            .prepare_gitlab_repository_settings(&changes, Some(&guard))
            .unwrap()
            .unwrap();
        f.service(write.clone()).update(&changes).await.unwrap();
        f.services
            .finish_gitlab_repository_settings(&write, false)
            .await
            .unwrap();
        assert!(!write.began());
        assert_eq!(f.directory.binding().unwrap(), prior);
    }
    let malformed = json!([{"path":SECRET_ACCOUNT,"value":"pat-second"},{"path":"sourceControl.gitlab.instanceBaseUrl","value":42}]);
    assert!(f
        .services
        .prepare_gitlab_repository_settings(&malformed, Some(&guard))
        .is_err());
    assert_eq!(f.directory.binding().unwrap(), prior);
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
}

#[intent_test_macros::daemon_test]
async fn reset_disconnects_only_after_actual_secret_and_sibling_completion() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    let write = f
        .services
        .prepare_gitlab_repository_reset(SECRET_ACCOUNT, Some(&guard))
        .unwrap()
        .unwrap();
    f.service(write.clone())
        .reset_with_change(SECRET_ACCOUNT)
        .await
        .unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    f.services
        .finish_gitlab_repository_settings(&write, false)
        .await
        .unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Disconnected
    );
}

#[intent_test_macros::daemon_test]
async fn root_replacement_does_not_probe_the_previous_token_at_a_new_endpoint() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    let changes = json!([{"path":"sourceControl.gitlab.instanceBaseUrl","value":"https://gitlab.test/another"}]);
    let write = f
        .services
        .prepare_gitlab_repository_settings(&changes, Some(&guard))
        .unwrap()
        .unwrap();
    f.service(write.clone()).update(&changes).await.unwrap();
    let requests = server.control.requests.lock().unwrap().len();
    assert!(f
        .services
        .finish_gitlab_repository_settings(&write, false)
        .await
        .is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    f.services
        .settle_gitlab_repository_settings(Some(&write), false, false)
        .await;
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
}

#[intent_test_macros::daemon_test]
async fn actual_secret_then_config_failure_compensates_without_early_ready() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let prior = f.directory.binding().unwrap();
    let guard = f.services.gitlab_credential_gate.lock().await;
    let changes = json!([{"path":SECRET_ACCOUNT,"value":"pat-second"},{"path":"sourceControl.gitlab.oauthClientId","value":"next-client"}]);
    let write = f
        .services
        .prepare_gitlab_repository_settings(&changes, Some(&guard))
        .unwrap()
        .unwrap();
    let service = f.service(write.clone());
    let update = service.update_secrets(&changes).await.unwrap();
    let saved = f.dir.path().join("config.saved");
    std::fs::rename(f.registry.config_path(), &saved).unwrap();
    std::fs::create_dir(f.registry.config_path()).unwrap();
    assert!(service.update_non_secrets(&changes).await.is_err());
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    assert!(!update.rollback(&f.services.secrets).await);
    std::fs::remove_dir(f.registry.config_path()).unwrap();
    std::fs::rename(&saved, f.registry.config_path()).unwrap();
    f.services
        .finish_gitlab_repository_settings(&write, true)
        .await
        .unwrap();
    let restored = f.directory.binding().unwrap();
    assert_eq!(restored.account, prior.account);
    assert_ne!(restored.scope, prior.scope);
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
}

#[intent_test_macros::daemon_test]
async fn late_pat_result_cannot_publish_after_a_settings_owner_begins() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    *server.control.pause.lock().unwrap() = Some("pat-second");
    let svc = f.services.clone();
    let host = server.host.clone();
    let connect =
        tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-second".into()).await });
    server.entered().await;
    {
        let guard = f.services.gitlab_credential_gate.lock().await;
        let changes = json!([{"path":"sourceControl.gitlab.oauthClientId","value":"next-client"}]);
        let write = f
            .services
            .prepare_gitlab_repository_settings(&changes, Some(&guard))
            .unwrap()
            .unwrap();
        f.service(write.clone()).update(&changes).await.unwrap();
        f.services
            .finish_gitlab_repository_settings(&write, false)
            .await
            .unwrap();
    }
    let current = f.directory.binding().unwrap();
    server.control.release.notify_one();
    assert!(connect.await.unwrap().is_err());
    assert_eq!(f.directory.binding().unwrap(), current);
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
}

#[intent_test_macros::daemon_test]
async fn auth_connect_uses_its_own_publication_and_attachment_is_one_shot() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert_eq!(f.directory.binding().unwrap().account.account_id, "43");
    assert!(f
        .services
        .gitlab_credential_gate
        .install_settings_boundary(
            &f.registry,
            &f.services.secrets,
            &f.services.gitlab_secret_store,
            Some(server.descriptor.clone())
        )
        .is_err());
    assert_eq!(f.directory.binding().unwrap().account.account_id, "43");
}

#[intent_test_macros::daemon_test]
async fn failed_account_evidence_cancels_unstarted_adoption_without_retirement() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "zero-account")
        .unwrap();
    assert!(f
        .services
        .reconcile_gitlab_repository_binding()
        .await
        .is_err());
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("zero-account")
    );
}

#[intent_test_macros::daemon_test]
async fn retired_account_proof_cannot_adopt_or_rewrite_the_stored_token() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = std::fs::read(f.services.gitlab_secret_store.path()).unwrap();
    *server.control.pause.lock().unwrap() = Some("/api/v4/user");
    let services = f.services.clone();
    let proof = tokio::spawn(async move { services.reconcile_gitlab_repository_binding().await });
    server.entered().await;
    f.directory.retire().unwrap();
    server.control.release.notify_one();
    assert!(proof.await.unwrap().is_err());
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert_eq!(
        std::fs::read(f.services.gitlab_secret_store.path()).unwrap(),
        before
    );
}

struct WritePause(Arc<(Mutex<bool>, std::sync::Condvar)>);
impl Drop for WritePause {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
}

#[intent_test_macros::daemon_test]
async fn abandoned_settings_write_retains_original_gate_and_leaves_unknown_settlement() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let pause = WritePause(Arc::new((Mutex::new(false), std::sync::Condvar::new())));
    let hit = entered.clone();
    let held = pause.0.clone();
    f.services
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            hit.notify_one();
            let mut released = held.0.lock().unwrap();
            while !*released {
                released = held.1.wait(released).unwrap();
            }
        }));
    let svc = f.services.clone();
    let task = tokio::spawn(async move {
        let guard = svc.gitlab_credential_gate.lock().await;
        let changes = json!([{"path":SECRET_ACCOUNT,"value":"pat-second"}]);
        let write = svc
            .prepare_gitlab_repository_settings(&changes, Some(&guard))
            .unwrap()
            .unwrap();
        svc.settings_service()
            .with_repository_write(Some(write))
            .update_secrets(&changes)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    assert!(f.services.gitlab_credential_gate.mutex.try_lock().is_err());
    drop(pause);
    let _finished = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.services.gitlab_credential_gate.lock(),
    )
    .await
    .unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("pat-second")
    );
}

#[intent_test_macros::daemon_test]
async fn attached_same_account_refresh_preserves_real_ticket_quota_and_rejects_old_denial() {
    use crate::repository_credentials::authority::{
        RepositoryAuthorityRequest, RepositoryCredentialTransport,
    };
    use crate::repository_credentials::{RepositoryCredentialUse, RepositorySecretReader};
    use crate::source_control_auth_ops::repository_owner_tests::{
        FixtureAuthority, FixtureSecretReader,
    };
    use intent_sourcecontrol::gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT};
    use std::time::{Duration, Instant};
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let binding = f.directory.binding().unwrap();
    let admission = f
        .directory
        .admit(
            &binding,
            RepositoryAuthorityRequest {
                execution: intent_core::ExecutionScope {
                    daemon_id: binding.daemon_id.clone(),
                    authority_scope_id: "fixture-authority".into(),
                    authority_generation: 1,
                },
                connection: binding.scope.clone(),
                target: intent_core::RepositoryTarget {
                    provider: intent_core::RepositoryProvider::Gitlab,
                    instance_base_url: binding.account.instance_base_url.clone(),
                    project_path: "group/project".into(),
                },
                use_kind: RepositoryCredentialUse::NativeRead,
                allowed_transport: RepositoryCredentialTransport::GitlabApi(
                    server.descriptor.clone(),
                ),
            },
            Arc::new(FixtureAuthority),
        )
        .unwrap();
    let reader = FixtureSecretReader(f.services.gitlab_secret_store.clone());
    let ticket = f
        .directory
        .acquire_exact(
            &admission,
            &reader as &dyn RepositorySecretReader,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    f.services
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
        .unwrap();
    f.services
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    *server.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let svc = f.services.clone();
    let refresh = tokio::spawn(async move { svc.reconcile_gitlab_repository_binding().await });
    server.entered().await;
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    assert!(f
        .directory
        .record_backoff(
            ticket.dispatch_stamp(),
            Instant::now() + Duration::from_secs(3600)
        )
        .unwrap());
    assert!(f
        .directory
        .record_backoff(ticket.dispatch_stamp(), Instant::now())
        .unwrap());
    server.control.release.notify_one();
    refresh.await.unwrap().unwrap();
    assert_eq!(f.directory.binding().unwrap(), binding);
    assert!(!f
        .directory
        .reject_current_credential(ticket.dispatch_stamp())
        .unwrap());
    assert!(matches!(
        f.directory
            .acquire_exact(&admission, &reader, Duration::from_secs(2))
            .await,
        Err(RepositoryCredentialError::Backoff)
    ));
}

#[test]
fn malformed_instance_configuration_does_not_echo_credentials() {
    let config = intent_core::settings_file::GitlabSettings {
        host: "https://user:credential-marker@forge.test".into(),
        ..Default::default()
    };
    let error = logical_instance(&config).unwrap_err();
    assert!(!error.to_string().contains("credential-marker"));
}

#[intent_test_macros::daemon_test]
async fn services_constructor_clones_share_one_directory_and_independent_instances_do_not() {
    let server = Server::new().await;
    let first = Fixture::uninstalled(&server).await;
    let cloned = first.services.clone();
    assert!(Arc::ptr_eq(
        &first.directory,
        &cloned.repository_connection_directory()
    ));
    assert!(Arc::ptr_eq(
        &first.services.gitlab_credential_gate.mutex,
        &cloned.gitlab_credential_gate.mutex
    ));
    let second = Fixture::uninstalled(&server).await;
    assert!(!Arc::ptr_eq(&first.directory, &second.directory));
    assert_ne!(
        first.services.daemon_boot_id,
        second.services.daemon_boot_id
    );
    assert_eq!(
        first.directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    assert_eq!(
        second.directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    assert!(server.control.requests.lock().unwrap().is_empty());
}

#[intent_test_macros::daemon_test]
async fn services_late_source_builders_retire_the_original_shared_directory() {
    for source in ["settings", "gitlab", "registry"] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        f.services
            .reconcile_gitlab_repository_binding()
            .await
            .unwrap();
        let requests = server.control.requests.lock().unwrap().len();
        let different = FileSecretStore::with_path(f.dir.path().join("different.json"));
        let changed = match source {
            "settings" => f.services.clone().with_secret_store(Arc::new(different)),
            "gitlab" => f.services.clone().with_gitlab_secret_store(different),
            "registry" => f
                .services
                .clone()
                .with_settings_registry(f.registry.clone()),
            _ => unreachable!(),
        };
        assert!(Arc::ptr_eq(
            &f.directory,
            &changed.repository_connection_directory()
        ));
        assert_eq!(
            f.directory.binding().unwrap_err(),
            RepositoryCredentialError::Retired
        );
        assert!(f
            .services
            .reconcile_gitlab_repository_binding()
            .await
            .is_err());
        assert_eq!(server.control.requests.lock().unwrap().len(), requests);
    }
}

#[intent_test_macros::daemon_test]
async fn services_settings_update_and_reset_settle_the_original_directory() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let prior = f.directory.binding().unwrap();
    f.services
        .settings_update(json!([{ "path":SECRET_ACCOUNT, "value":"pat-second" }]))
        .await
        .unwrap();
    let replacement = f.directory.binding().unwrap();
    assert_eq!(replacement.account.account_id, "43");
    assert_ne!(replacement.scope, prior.scope);
    f.services
        .settings_reset(SECRET_ACCOUNT.into())
        .await
        .unwrap();
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Disconnected
    );
    assert_eq!(
        f.services.gitlab_secret_store.load(SECRET_ACCOUNT).unwrap(),
        None
    );
}

#[intent_test_macros::daemon_test]
async fn services_confirmed_no_effect_config_failure_restores_a_verified_generation() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let original = f.directory.binding().unwrap();
    let first = std::sync::atomic::AtomicBool::new(true);
    let path = f.registry.config_path().to_path_buf();
    let saved = f.dir.path().join("config.saved");
    f.services
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                std::fs::rename(&path, &saved).unwrap();
                std::fs::create_dir(&path).unwrap();
            }
        }));
    assert!(f
        .services
        .settings_update(json!([
            {"path":SECRET_ACCOUNT,"value":"pat-second"},
            {"path":"sourceControl.gitlab.oauthClientId","value":"new-client"}
        ]))
        .await
        .is_err());
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
    assert_eq!(
        f.registry.get("sourceControl.gitlab.oauthClientId"),
        Some(json!("client"))
    );
    let restored = f.directory.binding().expect(
        "fully settled no-effect ordinary failure and actual secret compensation must be verified",
    );
    assert_eq!(restored.account, original.account);
    assert_ne!(restored.scope, original.scope);
}

#[intent_test_macros::daemon_test]
async fn services_partial_ordinary_failure_cannot_claim_whole_batch_compensation() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    f.services
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_workspace_rules BEFORE INSERT ON settings WHEN new.key = 'workspaceRules' BEGIN SELECT RAISE(FAIL, 'fixture refusal'); END")
        .execute(f.services.store.write_pool()).await.unwrap();
    assert!(f
        .services
        .settings_update(json!([
            {"path":SECRET_ACCOUNT,"value":"pat-second"},
            {"path":"sourceControl.gitlab.oauthClientId","value":"new-client"},
            {"path":"userRules","value":{"rule":"landed"}},
            {"path":"workspaceRules","value":{"rule":"refused"}}
        ]))
        .await
        .is_err());
    assert_eq!(
        f.services
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
    assert_eq!(
        f.registry.get("sourceControl.gitlab.oauthClientId"),
        Some(json!("client"))
    );
    assert_eq!(
        f.services
            .store
            .get_setting("userRules")
            .await
            .unwrap()
            .as_deref(),
        Some("{\"rule\":\"landed\"}")
    );
    assert_eq!(
        f.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
}
