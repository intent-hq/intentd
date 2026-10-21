//! Actual paired owner/file/loopback behavior. Caller authority is injected;
//! authentic process/UDS ownership belongs to the Services entry tests.
use super::*;
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
    refresh_original, Fixture, PausedRead, Server,
};
use intent_git::native_checkout::NativeCheckoutCredentials;
use intent_sourcecontrol::gitlab_token::{
    EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
};
use intent_sourcecontrol::ExposeSecret;
use std::sync::Mutex;

struct Caller(Mutex<bool>);
impl Caller {
    fn current() -> Arc<Self> {
        Arc::new(Self(Mutex::new(true)))
    }
    fn retire(&self) {
        *self.0.lock().unwrap() = false;
    }
}
impl CheckoutAuthority for Caller {
    fn dispatch(&self, action: &mut (dyn FnMut() -> CoreResult<()> + Send)) -> CoreResult<()> {
        let current = self.0.lock().unwrap();
        if !*current {
            return Err(local(Error::AuthorityDenied));
        }
        action()
    }
}
fn capture(f: &Fixture) -> GitlabChildPolicyCapture {
    f.service.capture_gitlab_child_policy().unwrap()
}
fn expected(status: &GitlabChildPolicyStatus) -> GitlabChildPolicyExpected {
    GitlabChildPolicyExpected {
        instance_base_url: status.instance_base_url.clone(),
        account_id: status.account_id.clone(),
        connection_id: status.connection_id.clone(),
        connection_generation: status.connection_generation,
        revision: status.revision,
    }
}
impl GitlabChildPolicyCapture {
    async fn status_current(
        &self,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<GitlabChildPolicyStatus> {
        self.status(authority)
            .await
            .map(|output| output.into_parts().0)
    }

    async fn set_current(
        self,
        enabled: bool,
        authority: Arc<dyn CheckoutAuthority>,
    ) -> CoreResult<GitlabChildPolicyStatus> {
        let observed = self.status_current(authority.clone()).await?;
        self.set(expected(&observed), enabled, authority)
            .await
            .map(|output| output.into_parts().0)
    }
}
async fn credential(f: &Fixture) -> Option<SecretString> {
    capture(f)
        .credential(
            "https",
            "gitlab.test",
            "forge/group/project.git",
            None,
            Caller::current(),
        )
        .await
        .unwrap()
        .map(|credential| credential.into_parts().0)
}

#[intent_test_macros::daemon_test]
async fn child_policy_unset_is_off_and_every_lookup_observes_the_current_choice() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = server.control.requests.lock().unwrap().len();
    let initial = capture(&f).status_current(Caller::current()).await.unwrap();
    assert!(!initial.enabled);
    assert!(credential(&f).await.is_none());
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    let enabled = capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    assert!(enabled.enabled);
    assert!(enabled.revision > initial.revision);
    assert_eq!(enabled.instance_base_url, "https://gitlab.test/forge");
    assert_eq!(enabled.account_id, "42");
    assert_eq!(enabled.connection_id, initial.connection_id);
    assert_eq!(credential(&f).await.unwrap().expose_secret(), "stored-pat");
    let disabled = capture(&f)
        .set_current(false, Caller::current())
        .await
        .unwrap();
    assert!(!disabled.enabled);
    assert!(disabled.revision > enabled.revision);
    assert!(credential(&f).await.is_none());
    assert_eq!(
        server.control.requests.lock().unwrap().len(),
        before,
        "metadata/helper lookup performs no provider probe"
    );
    let raw = f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .unwrap();
    assert!(!raw.contains("stored-pat"));
    assert_eq!(
        f.service
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
}

#[intent_test_macros::daemon_test]
async fn child_policy_full_root_and_explicit_identity_are_exact() {
    let mut server = Server::new().await;
    let origin = server.host.base_url().to_owned();
    server.host = GitlabHost::parse("https://gitlab.test:8443/Forge")
        .unwrap()
        .with_api_origin(&origin)
        .unwrap();
    server.descriptor = intent_sourcecontrol::GitlabDescriptor::with_loopback_endpoint(
        GitlabInstance::parse(server.host.logical_base_url()).unwrap(),
        &origin,
    )
    .unwrap();
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    for (protocol, host, path, username) in [
        ("http", "gitlab.test:8443", "Forge/group/project.git", None),
        ("ssh", "gitlab.test:8443", "Forge/group/project.git", None),
        ("https", "gitlab.test", "Forge/group/project.git", None),
        (
            "https",
            "gitlab.test.evil:8443",
            "Forge/group/project.git",
            None,
        ),
        ("https", "gitlab.test:8443", "forge/group/project.git", None),
        (
            "https",
            "gitlab.test:8443",
            "Forge-other/group/project.git",
            None,
        ),
        (
            "https",
            "gitlab.test:8443",
            "Forge/../group/project.git",
            None,
        ),
        (
            "https",
            "gitlab.test:8443",
            "Forge/group/%2e%2e/project.git",
            None,
        ),
        (
            "https",
            "gitlab.test:8443",
            "Forge/group/project.git",
            Some("alice"),
        ),
    ] {
        assert!(capture(&f)
            .credential(protocol, host, path, username, Caller::current())
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(
        capture(&f)
            .credential(
                "HTTPS",
                "GITLAB.TEST:8443",
                "Forge/group/project.git",
                Some("oauth2"),
                Caller::current()
            )
            .await
            .unwrap()
            .unwrap()
            .into_parts()
            .0
            .expose_secret(),
        "stored-pat"
    );
}

#[intent_test_macros::daemon_test]
async fn child_policy_verified_same_account_refresh_preserves_choice_and_uses_new_token() {
    let server = Server::new().await;
    let f = Fixture::unadopted(&server).await;
    f.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let old_request = f.request();
    let enabled = capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let record = f.service.gitlab_secret_store.load(ACCOUNT).unwrap();
    refresh_original(&f.service, &server.host).await;
    let fresh = capture(&f).status_current(Caller::current()).await.unwrap();
    assert!(fresh.enabled);
    assert_eq!(fresh.connection_id, enabled.connection_id);
    assert_eq!(fresh.revision, enabled.revision);
    assert!(f.request().secret_revision > old_request.secret_revision);
    assert_eq!(f.service.gitlab_secret_store.load(ACCOUNT).unwrap(), record);
    assert_eq!(credential(&f).await.unwrap().expose_secret(), "rotated");
}

#[intent_test_macros::daemon_test]
async fn child_policy_account_replacement_and_revoke_never_restore_an_old_choice() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let old = capture(&f);
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert!(old.status_current(Caller::current()).await.is_err());
    let other = capture(&f).status_current(Caller::current()).await.unwrap();
    assert_eq!(other.account_id, "43");
    assert!(!other.enabled);
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-first".into())
        .await
        .unwrap();
    assert!(
        !capture(&f)
            .status_current(Caller::current())
            .await
            .unwrap()
            .enabled
    );
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    f.service
        .gitlab_revoke_owned(server.host.clone())
        .await
        .unwrap();
    assert!(f.service.capture_gitlab_child_policy().is_err());
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-first".into())
        .await
        .unwrap();
    assert!(credential(&f).await.is_none());
}

#[intent_test_macros::daemon_test]
async fn child_policy_restart_restores_only_actual_verified_matching_account() {
    for choice in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        capture(&f)
            .set_current(choice, Caller::current())
            .await
            .unwrap();
        let record = f.service.gitlab_secret_store.load(ACCOUNT).unwrap();
        f.service
            .repository_connection_directory()
            .retire()
            .unwrap();
        let registry = Arc::new(crate::SettingsRegistry::load(f.registry.config_path()).unwrap());
        let service = crate::Services::new_repository_fixture(
            f.service.store().clone(),
            f.service.gitlab_secret_store.clone(),
            None,
        )
        .with_settings_registry(registry.clone());
        let guard = service.gitlab_credential_gate.lock().await;
        service
            .gitlab_credential_gate
            .install_settings_boundary(
                &registry,
                &service.secrets,
                &service.gitlab_secret_store,
                Some(server.descriptor.clone()),
            )
            .unwrap();
        drop(guard);
        assert!(
            service.capture_gitlab_child_policy().is_err(),
            "persisted choice does not verify a source"
        );
        service.reconcile_gitlab_repository_binding().await.unwrap();
        assert_eq!(
            service
                .capture_gitlab_child_policy()
                .unwrap()
                .status_current(Caller::current())
                .await
                .unwrap()
                .enabled,
            choice
        );
        assert_eq!(
            service.gitlab_secret_store.load(ACCOUNT).unwrap(),
            record,
            "both explicit disabled and enabled choices survive verified restart"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn child_policy_invalid_record_cannot_grant_or_block_original_revoke() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(ACCOUNT, "invalid-choice")
        .unwrap();
    assert!(capture(&f).status_current(Caller::current()).await.is_err());
    f.service
        .gitlab_checkout_connection(Caller::current())
        .unwrap()
        .native_credential("https://gitlab.test/forge/group/project.git")
        .await
        .unwrap()
        .with_current(&mut || Ok(()))
        .unwrap();
    f.service
        .gitlab_revoke_owned(server.host.clone())
        .await
        .unwrap();
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    assert!(f
        .service
        .gitlab_secret_store
        .load(SECRET_ACCOUNT)
        .unwrap()
        .is_none());
}

#[intent_test_macros::daemon_test]
async fn child_policy_source_change_and_restoration_do_not_revive_attestation() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "unverified-replacement")
        .unwrap();
    assert!(capture(&f)
        .credential(
            "https",
            "gitlab.test",
            "forge/group/project.git",
            None,
            Caller::current()
        )
        .await
        .is_err());
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "stored-pat")
        .unwrap();
    assert!(f.service.capture_gitlab_child_policy().is_err());
}

#[intent_test_macros::daemon_test]
async fn child_policy_original_caller_retirement_during_owned_read_releases_no_token() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let mut paused = PausedRead::install(&f);
    let caller = Caller::current();
    let original = capture(&f);
    let read_caller = caller.clone();
    let read = tokio::spawn(async move {
        original
            .credential(
                "https",
                "gitlab.test",
                "forge/group/project.git",
                None,
                read_caller,
            )
            .await
    });
    paused.entered().await;
    caller.retire();
    paused.resume();
    assert!(read.await.unwrap().is_err());
    assert_eq!(
        credential(&f).await.unwrap().expose_secret(),
        "stored-pat",
        "a genuinely new caller is independent"
    );
}

#[intent_test_macros::daemon_test]
async fn child_policy_full_instance_transition_clears_durable_choice_before_unverified_state() {
    use intent_core::WorkspaceApi;
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let original = capture(&f);
    f.service
        .settings_update(serde_json::json!([{
            "path":"sourceControl.gitlab.instanceBaseUrl", "value":"https://gitlab.test/other"
        }]))
        .await
        .unwrap();
    assert!(original.status_current(Caller::current()).await.is_err());
    assert!(f.service.capture_gitlab_child_policy().is_err());
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    f.service
        .settings_update(serde_json::json!([{
            "path":"sourceControl.gitlab.instanceBaseUrl", "value":"https://gitlab.test/forge"
        }]))
        .await
        .unwrap();
    assert!(
        f.service.capture_gitlab_child_policy().is_err(),
        "restored settings do not republish attestation"
    );
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-first".into())
        .await
        .unwrap();
    assert!(credential(&f).await.is_none());
}

#[intent_test_macros::daemon_test]
async fn child_policy_cancelled_caller_cannot_abandon_the_admitted_original_write() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let mut original = capture(&f);
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, receive) = std::sync::mpsc::channel();
    let receive = Mutex::new(receive);
    original.write_probe = Some(Arc::new(move || {
        signal.notify_one();
        receive.lock().unwrap().recv().unwrap();
    }));
    let update = tokio::spawn(async move { original.set_current(true, Caller::current()).await });
    tokio::time::timeout(
        crate::test_support::fixture_timeout(Duration::from_secs(5)),
        entered.notified(),
    )
    .await
    .unwrap();
    update.abort();
    assert!(matches!(update.await, Err(error) if error.is_cancelled()));
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    release.send(()).unwrap();
    let status = tokio::time::timeout(
        crate::test_support::fixture_timeout(Duration::from_secs(5)),
        capture(&f).status_current(Caller::current()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(status.enabled);
    assert_eq!(credential(&f).await.unwrap().expose_secret(), "stored-pat");
}

#[intent_test_macros::daemon_test]
async fn child_policy_cancelled_file_read_retains_lease_until_worker_exits() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let mut paused = PausedRead::install(&f);
    let original = capture(&f);
    let read = tokio::spawn(async move {
        original
            .credential(
                "https",
                "gitlab.test",
                "forge/group/project.git",
                None,
                Caller::current(),
            )
            .await
    });
    paused.entered().await;
    read.abort();
    assert!(matches!(read.await, Err(error) if error.is_cancelled()));
    let update = capture(&f);
    let mut disable =
        tokio::spawn(async move { update.set_current(false, Caller::current()).await });
    assert!(
        // fixed-deadline: allow — short window proving the disable is still held, not a hang guard
        tokio::time::timeout(Duration::from_millis(30), &mut disable)
            .await
            .is_err()
    );
    paused.resume();
    assert!(
        !tokio::time::timeout(
            crate::test_support::fixture_timeout(Duration::from_secs(5)),
            disable
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .enabled
    );
    assert!(credential(&f).await.is_none());
}

#[intent_test_macros::daemon_test]
async fn child_policy_disabled_does_not_disable_native_credentials() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let native = f
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    assert!(credential(&f).await.is_none());
    native
        .native_credential("https://gitlab.test/forge/group/project.git")
        .await
        .unwrap()
        .with_current(&mut || Ok(()))
        .unwrap();
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    capture(&f)
        .set_current(false, Caller::current())
        .await
        .unwrap();
    native
        .native_credential("https://gitlab.test/forge/group/project.git")
        .await
        .unwrap()
        .with_current(&mut || Ok(()))
        .unwrap();
}

#[intent_test_macros::daemon_test]
async fn child_policy_expected_binding_and_revision_are_checked_before_owned_write() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let initial = capture(&f).status_current(Caller::current()).await.unwrap();
    let original = expected(&initial);
    for field in 0..5 {
        let mut wrong = original.clone();
        match field {
            0 => wrong.instance_base_url.push_str("/other"),
            1 => wrong.account_id.push_str("-other"),
            2 => wrong.connection_id.push_str("-other"),
            3 => wrong.connection_generation += 1,
            _ => wrong.revision += 1,
        }
        assert!(capture(&f)
            .set(wrong, true, Caller::current())
            .await
            .is_err());
        assert!(f
            .service
            .gitlab_secret_store
            .load(ACCOUNT)
            .unwrap()
            .is_none());
    }
    capture(&f)
        .set(original.clone(), true, Caller::current())
        .await
        .unwrap();
    let persisted = f.service.gitlab_secret_store.load(ACCOUNT).unwrap();
    assert!(capture(&f)
        .set(original, false, Caller::current())
        .await
        .is_err());
    assert_eq!(
        f.service.gitlab_secret_store.load(ACCOUNT).unwrap(),
        persisted
    );
    assert!(
        capture(&f)
            .status_current(Caller::current())
            .await
            .unwrap()
            .enabled
    );
}

#[intent_test_macros::daemon_test]
async fn child_credential_final_receipt_refuses_disable_revoke_refresh_replacement_source_and_caller_change(
) {
    for change in 0..6 {
        let server = Server::new().await;
        let f = if change == 2 {
            let fixture = Fixture::unadopted(&server).await;
            fixture
                .service
                .gitlab_secret_store
                .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
                .unwrap();
            fixture
                .service
                .gitlab_secret_store
                .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
                .unwrap();
            fixture
                .service
                .reconcile_gitlab_repository_binding()
                .await
                .unwrap();
            fixture
        } else {
            Fixture::new(&server).await
        };
        capture(&f)
            .set_current(true, Caller::current())
            .await
            .unwrap();
        let caller = Caller::current();
        let acquired = capture(&f)
            .credential(
                "https",
                "gitlab.test",
                "forge/group/project.git",
                None,
                caller.clone(),
            )
            .await
            .unwrap()
            .unwrap();
        let (token, receipt) = acquired.into_parts();
        assert_eq!(
            token.expose_secret(),
            if change == 2 { "rotated" } else { "stored-pat" }
        );
        match change {
            0 => {
                capture(&f)
                    .set_current(false, Caller::current())
                    .await
                    .unwrap();
            }
            1 => {
                f.service
                    .gitlab_revoke_owned(server.host.clone())
                    .await
                    .unwrap();
            }
            2 => {
                refresh_original(&f.service, &server.host).await;
            }
            3 => {
                f.service
                    .gitlab_connect_pat(server.host.clone(), "pat-second".into())
                    .await
                    .unwrap();
            }
            4 => {
                f.service
                    .gitlab_secret_store
                    .store(SECRET_ACCOUNT, "unverified-external")
                    .unwrap();
                assert!(capture(&f)
                    .credential(
                        "https",
                        "gitlab.test",
                        "forge/group/project.git",
                        None,
                        Caller::current()
                    )
                    .await
                    .is_err());
                f.service
                    .gitlab_secret_store
                    .store(SECRET_ACCOUNT, "stored-pat")
                    .unwrap();
            }
            _ => caller.retire(),
        }
        let mut outputs = 0;
        assert!(receipt
            .with_current(|| {
                outputs += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(
            outputs, 0,
            "original private token output after change {change}"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn child_credential_final_receipt_keeps_admitted_output_and_action_error_truthful() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    capture(&f)
        .set_current(true, Caller::current())
        .await
        .unwrap();
    let acquired = capture(&f)
        .credential(
            "https",
            "gitlab.test",
            "forge/group/project.git",
            None,
            Caller::current(),
        )
        .await
        .unwrap()
        .unwrap();
    let (_, receipt) = acquired.into_parts();
    let mut outputs = 0;
    let failure = receipt.with_current(|| {
        outputs += 1;
        Err(CoreError::Internal("original-transfer-failure".into()))
    });
    assert!(
        matches!(failure, Err(CoreError::Internal(message)) if message == "original-transfer-failure")
    );
    assert_eq!(outputs, 1);
    assert!(receipt
        .with_current(|| {
            outputs += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(outputs, 1, "an admitted failing transfer is never retried");
    let (_, receipt) = capture(&f)
        .credential(
            "https",
            "gitlab.test",
            "forge/group/project.git",
            None,
            Caller::current(),
        )
        .await
        .unwrap()
        .unwrap()
        .into_parts();
    receipt
        .with_current(|| {
            outputs += 1;
            Ok(())
        })
        .unwrap();
    f.service
        .gitlab_revoke_owned(server.host.clone())
        .await
        .unwrap();
    assert_eq!(outputs, 2, "already admitted effects are not recalled");
    assert!(receipt
        .with_current(|| {
            outputs += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(outputs, 2);
}

#[intent_test_macros::daemon_test]
async fn child_policy_original_expected_binding_survives_verified_refresh_only() {
    let server = Server::new().await;
    let f = Fixture::unadopted(&server).await;
    f.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let original = capture(&f);
    let status = original.status_current(Caller::current()).await.unwrap();
    let old_secret = f.request();
    refresh_original(&f.service, &server.host).await;
    assert_eq!(f.request().binding, old_secret.binding);
    assert!(f.request().secret_revision > old_secret.secret_revision);
    let choice = original
        .set(expected(&status), true, Caller::current())
        .await
        .unwrap()
        .into_parts()
        .0;
    assert!(choice.enabled);
    assert_eq!(choice.connection_generation, status.connection_generation);
    assert_eq!(credential(&f).await.unwrap().expose_secret(), "rotated");
}

#[intent_test_macros::daemon_test]
async fn child_status_receipt_admits_disabled_and_enabled_once_under_original_locks() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let original = capture(&f);
    let before = server.control.requests.lock().unwrap().len();
    for enabled in [false, true, false] {
        let output = if enabled {
            let status = original
                .status(Caller::current())
                .await
                .unwrap()
                .into_parts()
                .0;
            capture(&f)
                .set(expected(&status), true, Caller::current())
                .await
                .unwrap()
        } else {
            capture(&f)
                .set_current(false, Caller::current())
                .await
                .unwrap();
            original.status(Caller::current()).await.unwrap()
        };
        let (status, receipt) = output.into_parts();
        assert_eq!(status.enabled, enabled);
        assert_eq!(status.instance_base_url, "https://gitlab.test/forge");
        let mut packet = Some(status);
        let mut delivered = None;
        let result = receipt.with_current(|| {
            assert!(original
                .owner
                .settings
                .get()
                .unwrap()
                .config
                .try_lock()
                .is_err());
            assert!(original.owner.descriptor.try_lock().is_err());
            delivered = packet.take();
            Err(CoreError::Internal("original-status-transfer".into()))
        });
        assert!(
            matches!(result, Err(CoreError::Internal(message)) if message == "original-status-transfer")
        );
        assert_eq!(delivered.unwrap().enabled, enabled);
        assert!(receipt
            .with_current(|| panic!("one-use output repeated"))
            .is_err());
    }
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
}

#[intent_test_macros::daemon_test]
async fn child_status_receipt_refuses_choice_aba_revoke_replacement_root_source_and_caller() {
    use intent_core::WorkspaceApi;
    for change in 0..7 {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        capture(&f)
            .set_current(true, Caller::current())
            .await
            .unwrap();
        let caller = Caller::current();
        let (status, receipt) = capture(&f)
            .status(caller.clone())
            .await
            .unwrap()
            .into_parts();
        match change {
            0 => {
                capture(&f)
                    .set_current(false, Caller::current())
                    .await
                    .unwrap();
            }
            1 => {
                capture(&f)
                    .set_current(false, Caller::current())
                    .await
                    .unwrap();
                capture(&f)
                    .set_current(true, Caller::current())
                    .await
                    .unwrap();
            }
            2 => {
                f.service
                    .gitlab_revoke_owned(server.host.clone())
                    .await
                    .unwrap();
            }
            3 => {
                f.service
                    .gitlab_connect_pat(server.host.clone(), "pat-second".into())
                    .await
                    .unwrap();
            }
            4 => {
                f.service.settings_update(serde_json::json!([{"path":"sourceControl.gitlab.instanceBaseUrl","value":"https://gitlab.test/other"}])).await.unwrap();
                f.service.settings_update(serde_json::json!([{"path":"sourceControl.gitlab.instanceBaseUrl","value":"https://gitlab.test/forge"}])).await.unwrap();
            }
            5 => {
                f.service
                    .gitlab_secret_store
                    .store(SECRET_ACCOUNT, "unverified-external")
                    .unwrap();
                assert!(capture(&f)
                    .credential(
                        "https",
                        "gitlab.test",
                        "forge/group/project.git",
                        None,
                        Caller::current()
                    )
                    .await
                    .is_err());
                f.service
                    .gitlab_secret_store
                    .store(SECRET_ACCOUNT, "stored-pat")
                    .unwrap();
            }
            _ => caller.retire(),
        }
        assert!(status.enabled, "past snapshot remains unchanged");
        let mut outputs = 0;
        assert!(
            receipt
                .with_current(|| {
                    outputs += 1;
                    Ok(())
                })
                .is_err(),
            "change {change}"
        );
        assert_eq!(outputs, 0);
    }
}

#[intent_test_macros::daemon_test]
async fn child_status_receipt_pending_write_and_poison_refuse_without_output() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let (_, receipt) = capture(&f)
        .status(Caller::current())
        .await
        .unwrap()
        .into_parts();
    let mut update = capture(&f);
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, receive) = std::sync::mpsc::channel();
    let receive = Mutex::new(receive);
    update.write_probe = Some(Arc::new(move || {
        signal.notify_one();
        receive.lock().unwrap().recv().unwrap();
    }));
    let write = tokio::spawn(async move { update.set_current(true, Caller::current()).await });
    tokio::time::timeout(
        crate::test_support::fixture_timeout(Duration::from_secs(5)),
        entered.notified(),
    )
    .await
    .unwrap();
    assert!(receipt
        .with_current(|| panic!("pending policy cannot deliver"))
        .is_err());
    release.send(()).unwrap();
    write.await.unwrap().unwrap();
    assert!(receipt
        .with_current(|| panic!("old revision cannot deliver"))
        .is_err());
    let original = capture(&f);
    let (_, receipt) = original
        .status(Caller::current())
        .await
        .unwrap()
        .into_parts();
    let owner = original.owner.clone();
    assert!(std::thread::spawn(move || {
        let _guard = owner.descriptor.lock().unwrap();
        panic!("private poison fixture");
    })
    .join()
    .is_err());
    assert!(receipt
        .with_current(|| panic!("poison cannot deliver"))
        .is_err());
}

#[intent_test_macros::daemon_test]
async fn child_status_receipt_verified_refresh_remains_current_without_revising_old_facts() {
    let server = Server::new().await;
    let f = Fixture::unadopted(&server).await;
    f.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let before = f.request();
    let (status, receipt) = capture(&f)
        .status(Caller::current())
        .await
        .unwrap()
        .into_parts();
    refresh_original(&f.service, &server.host).await;
    assert_eq!(f.request().binding, before.binding);
    assert!(f.request().secret_revision > before.secret_revision);
    // Private quota metadata fixture: this is not an upstream HTTP observation.
    let owner = capture(&f);
    let stamp = owner
        .original
        .with_checkout_current(None, &[], false, |_, stamp| Ok(stamp))
        .unwrap();
    owner
        .owner
        .directory
        // fixed-deadline: allow — backoff expiry under test, not a hang guard
        .record_backoff(&stamp, std::time::Instant::now() + Duration::from_secs(60))
        .unwrap();
    let before_http = server.control.requests.lock().unwrap().len();
    let mut result = None;
    receipt
        .with_current(|| {
            result = Some(status);
            Ok(())
        })
        .unwrap();
    let original = result.unwrap();
    let current = capture(&f)
        .status(Caller::current())
        .await
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(current.revision, original.revision);
    assert_eq!(
        current.connection_generation,
        original.connection_generation
    );
    assert!(!original.enabled);
    assert_eq!(server.control.requests.lock().unwrap().len(), before_http);
    let (_, next_receipt) = owner.status(Caller::current()).await.unwrap().into_parts();
    next_receipt.with_current(|| Ok(())).unwrap();
    assert_eq!(server.control.requests.lock().unwrap().len(), before_http);
    f.service
        .gitlab_revoke_owned(server.host.clone())
        .await
        .unwrap();
    assert!(receipt
        .with_current(|| panic!("admitted output cannot be repeated"))
        .is_err());
    assert!(
        !original.enabled,
        "already admitted snapshot is not recalled"
    );
}

#[intent_test_macros::daemon_test]
async fn child_output_guard_rechecks_one_bound_frame_across_all_transport_phases() {
    for token_frame in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        capture(&f)
            .set_current(token_frame, Caller::current())
            .await
            .unwrap();
        let caller = Caller::current();
        let guard = if token_frame {
            capture(&f)
                .credential(
                    "https",
                    "gitlab.test",
                    "forge/group/project.git",
                    None,
                    caller.clone(),
                )
                .await
                .unwrap()
                .unwrap()
                .into_parts()
                .1
                .into_output()
                .unwrap()
        } else {
            capture(&f)
                .status(caller.clone())
                .await
                .unwrap()
                .into_parts()
                .1
                .into_output()
                .unwrap()
        };
        // These are bounded phase controls, not an actual UDS/WSS writer test.
        let mut phases = Vec::new();
        for phase in [
            "ready-pending",
            "ready",
            "write-part",
            "write-rest",
            "newline",
            "flush-pending",
            "flush",
        ] {
            guard
                .with_current(|| {
                    phases.push(phase);
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(phases.len(), 7);
        caller.retire();
        assert!(guard
            .with_current(|| {
                phases.push("retired-flush");
                Ok(())
            })
            .is_err());
        assert_eq!(
            phases.len(),
            7,
            "admitted phases stay truthful, later phases refuse"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn child_output_guard_refuses_changed_proof_between_phases_and_cannot_rebind_consumed_receipt(
) {
    for change in 0..4 {
        let server = Server::new().await;
        let f = if change == 2 {
            let fixture = Fixture::unadopted(&server).await;
            fixture
                .service
                .gitlab_secret_store
                .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
                .unwrap();
            fixture
                .service
                .gitlab_secret_store
                .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
                .unwrap();
            fixture
                .service
                .reconcile_gitlab_repository_binding()
                .await
                .unwrap();
            fixture
        } else {
            Fixture::new(&server).await
        };
        capture(&f)
            .set_current(true, Caller::current())
            .await
            .unwrap();
        let (_, status_receipt) = capture(&f)
            .status(Caller::current())
            .await
            .unwrap()
            .into_parts();
        status_receipt.with_current(|| Ok(())).unwrap();
        assert!(status_receipt.into_output().is_err());
        let (_, used) = capture(&f)
            .credential(
                "https",
                "gitlab.test",
                "forge/group/project.git",
                None,
                Caller::current(),
            )
            .await
            .unwrap()
            .unwrap()
            .into_parts();
        used.with_current(|| Ok(())).unwrap();
        assert!(used.into_output().is_err());
        let guard = capture(&f)
            .credential(
                "https",
                "gitlab.test",
                "forge/group/project.git",
                None,
                Caller::current(),
            )
            .await
            .unwrap()
            .unwrap()
            .into_parts()
            .1
            .into_output()
            .unwrap();
        let mut writes = 0;
        guard
            .with_current(|| {
                writes += 1;
                Ok(())
            })
            .unwrap();
        match change {
            0 => {
                capture(&f)
                    .set_current(false, Caller::current())
                    .await
                    .unwrap();
            }
            1 => {
                f.service
                    .gitlab_revoke_owned(server.host.clone())
                    .await
                    .unwrap();
            }
            2 => {
                refresh_original(&f.service, &server.host).await;
            }
            _ => {
                f.service
                    .gitlab_connect_pat(server.host.clone(), "pat-second".into())
                    .await
                    .unwrap();
            }
        }
        assert!(guard
            .with_current(|| {
                writes += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(writes, 1);
    }
}
