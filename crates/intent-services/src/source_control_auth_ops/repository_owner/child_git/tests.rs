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
}

#[intent_test_macros::daemon_test]
async fn child_policy_unset_is_off_and_every_lookup_observes_the_current_choice() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = server.control.requests.lock().unwrap().len();
    let initial = capture(&f)
        .status(Caller::current().as_ref())
        .await
        .unwrap();
    assert!(!initial.enabled);
    assert!(credential(&f).await.is_none());
    assert!(f
        .service
        .gitlab_secret_store
        .load(ACCOUNT)
        .unwrap()
        .is_none());
    let enabled = capture(&f).set(true, Caller::current()).await.unwrap();
    assert!(enabled.enabled);
    assert!(enabled.revision > initial.revision);
    assert_eq!(enabled.instance_base_url, "https://gitlab.test/forge");
    assert_eq!(enabled.account_id, "42");
    assert_eq!(enabled.connection_id, initial.connection_id);
    assert_eq!(credential(&f).await.unwrap().expose_secret(), "stored-pat");
    let disabled = capture(&f).set(false, Caller::current()).await.unwrap();
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
    capture(&f).set(true, Caller::current()).await.unwrap();
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
    let enabled = capture(&f).set(true, Caller::current()).await.unwrap();
    let record = f.service.gitlab_secret_store.load(ACCOUNT).unwrap();
    refresh_original(&f.service, &server.host).await;
    let fresh = capture(&f)
        .status(Caller::current().as_ref())
        .await
        .unwrap();
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
    capture(&f).set(true, Caller::current()).await.unwrap();
    let old = capture(&f);
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert!(old.status(Caller::current().as_ref()).await.is_err());
    let other = capture(&f)
        .status(Caller::current().as_ref())
        .await
        .unwrap();
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
            .status(Caller::current().as_ref())
            .await
            .unwrap()
            .enabled
    );
    capture(&f).set(true, Caller::current()).await.unwrap();
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
        capture(&f).set(choice, Caller::current()).await.unwrap();
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
                .status(Caller::current().as_ref())
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
    capture(&f).set(true, Caller::current()).await.unwrap();
    f.service
        .gitlab_secret_store
        .store(ACCOUNT, "invalid-choice")
        .unwrap();
    assert!(capture(&f)
        .status(Caller::current().as_ref())
        .await
        .is_err());
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
    capture(&f).set(true, Caller::current()).await.unwrap();
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
    capture(&f).set(true, Caller::current()).await.unwrap();
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
    capture(&f).set(true, Caller::current()).await.unwrap();
    let original = capture(&f);
    f.service
        .settings_update(serde_json::json!([{
            "path":"sourceControl.gitlab.instanceBaseUrl", "value":"https://gitlab.test/other"
        }]))
        .await
        .unwrap();
    assert!(original.status(Caller::current().as_ref()).await.is_err());
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
    let update = tokio::spawn(async move { original.set(true, Caller::current()).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
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
        Duration::from_secs(5),
        capture(&f).status(Caller::current().as_ref()),
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
    capture(&f).set(true, Caller::current()).await.unwrap();
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
    let mut disable = tokio::spawn(async move { update.set(false, Caller::current()).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut disable)
            .await
            .is_err()
    );
    paused.resume();
    assert!(
        !tokio::time::timeout(Duration::from_secs(5), disable)
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
    capture(&f).set(true, Caller::current()).await.unwrap();
    capture(&f).set(false, Caller::current()).await.unwrap();
    native
        .native_credential("https://gitlab.test/forge/group/project.git")
        .await
        .unwrap()
        .with_current(&mut || Ok(()))
        .unwrap();
}
