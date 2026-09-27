use super::*;
use crate::repository_credentials::authority::{
    RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    BoundGitlabRequestCredentials, RepositoryCredentialAdmission, RepositoryCredentialUse,
};
pub(crate) use crate::source_control_auth_ops::repository_owner_tests::Server;
use intent_sourcecontrol::{GitLabSourceControl, GitlabRequestCredentials, SourceControl};
use serde_json::json;
use std::time::Duration;

// Authority is injected in this private reader suite; original Services, file
// persistence, auth owners and HTTP callback are real disposable components.
struct Allowed;
impl RepositoryAuthority for Allowed {
    fn revalidate<'a>(
        &'a self,
        _: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async { Ok(Box::new(Self) as Box<dyn RepositoryAuthorityFence>) })
    }
}
impl RepositoryAuthorityFence for Allowed {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        action()
    }
}

pub(crate) struct Fixture {
    _dir: tempfile::TempDir,
    pub(crate) service: Arc<crate::Services>,
    pub(crate) registry: Arc<crate::SettingsRegistry>,
}
impl Fixture {
    pub(crate) async fn unadopted(server: &Server) -> Self {
        let dir = crate::test_support::test_tempdir("repository-secret-reader");
        let db = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let registry =
            Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                ("sourceControl.gitlab.host".into(), json!("gitlab.test")),
                (
                    "sourceControl.gitlab.instanceBaseUrl".into(),
                    json!(server.descriptor.instance().as_str()),
                ),
                (
                    "sourceControl.gitlab.apiBaseUrl".into(),
                    json!(server.host.base_url()),
                ),
                ("sourceControl.gitlab.oauthClientId".into(), json!("client")),
            ])
            .unwrap();
        let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
        secrets.store(SECRET_ACCOUNT, "stored-pat").unwrap();
        let service = Arc::new(
            crate::Services::new_repository_fixture(db, secrets, None)
                .with_settings_registry(registry.clone()),
        );
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
        *server.control.directory.lock().unwrap() = Some(service.repository_connection_directory());
        Self {
            _dir: dir,
            service,
            registry,
        }
    }
    pub(crate) async fn new(server: &Server) -> Self {
        let f = Self::unadopted(server).await;
        f.service
            .reconcile_gitlab_repository_binding()
            .await
            .unwrap();
        f
    }
    fn reader(&self) -> Arc<dyn RepositorySecretReader> {
        self.service.gitlab_repository_secret_reader().unwrap()
    }
    pub(crate) fn request(&self) -> RepositorySecretRequest {
        let directory = self.service.repository_connection_directory();
        directory
            .selected_secret_request(&directory.binding().unwrap())
            .unwrap()
    }
    fn admission(
        &self,
        server: &Server,
        authority: Arc<dyn RepositoryAuthority>,
    ) -> RepositoryCredentialAdmission {
        let directory = self.service.repository_connection_directory();
        let binding = directory.binding().unwrap();
        directory
            .admit(
                &binding,
                RepositoryAuthorityRequest {
                    execution: intent_core::ExecutionScope {
                        daemon_id: binding.daemon_id.clone(),
                        authority_scope_id: "reader-fixture".into(),
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
                authority,
            )
            .unwrap()
    }
    async fn provider_read(&self, server: &Server, token: &'static str) {
        *server.control.expected_project_token.lock().unwrap() = Some(token);
        let provider =
            GitLabSourceControl::new(server.descriptor.clone(), Arc::new(self.callback(server)))
                .unwrap();
        let repo = provider.get_repo("group", "project").await.unwrap();
        assert_eq!(repo.owner, "group");
        assert_eq!(repo.name, "project");
    }
    fn callback(&self, server: &Server) -> BoundGitlabRequestCredentials {
        BoundGitlabRequestCredentials::new(
            self.service.repository_connection_directory(),
            self.admission(server, Arc::new(Allowed)),
            self.reader(),
            Duration::from_secs(2),
        )
        .unwrap()
    }
}

#[intent_test_macros::daemon_test]
async fn reader_uses_real_startup_proof_without_more_http_or_writes() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let before = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
    let calls = server.control.requests.lock().unwrap().len();
    let snapshot = f.reader().load(&f.request()).await.unwrap();
    assert_eq!(snapshot.request.binding.account.account_id, "42");
    assert!(!format!("{snapshot:?}").contains("stored-pat"));
    let token = f
        .callback(&server)
        .token_for_request(
            server.descriptor.instance(),
            intent_sourcecontrol::gitlab::GitlabCredentialRequest::direct(
                &server.descriptor,
                "projects/group%2Fproject",
                false,
            ),
        )
        .await
        .unwrap();
    assert!(!format!("{token:?}").contains("stored-pat"));
    assert_eq!(server.control.requests.lock().unwrap().len(), calls);
    assert_eq!(
        std::fs::read(f.service.gitlab_secret_store.path()).unwrap(),
        before
    );
    f.provider_read(&server, "stored-pat").await;
}

#[intent_test_macros::daemon_test]
async fn reader_rejects_changed_disk_and_cannot_revive_observed_proof() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let reader = f.reader();
    let expected = f.request();
    assert!(reader.load(&expected).await.is_ok());
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "external-replacement")
        .unwrap();
    assert_eq!(
        reader.load(&expected).await.unwrap_err(),
        Error::SecretMismatch
    );
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "stored-pat")
        .unwrap();
    assert_eq!(reader.load(&expected).await.unwrap_err(), Error::Unverified);
}

#[intent_test_macros::daemon_test]
async fn reader_pat_persistence_mints_new_proof_and_stale_input_cannot_clear_it() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let reader = f.reader();
    let old = f.request();
    f.service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert_eq!(reader.load(&old).await.unwrap_err(), Error::Retired);
    let new = f.request();
    assert_eq!(new.binding.account.account_id, "43");
    reader.load(&new).await.unwrap();
    f.provider_read(&server, "pat-second").await;
}

#[test]
fn source_fingerprints_separate_nonce_presence_lengths_and_raw_fields() {
    let a = SourceEvidence::new();
    let b = SourceEvidence::new();
    let original = a.fingerprint(Some("access"), Some("refresh"), Some("42"));
    assert!(original == a.fingerprint(Some("access"), Some("refresh"), Some("42")));
    assert!(original != b.fingerprint(Some("access"), Some("refresh"), Some("42")));
    assert!(original != a.fingerprint(Some(" access "), Some("refresh"), Some("42")));
    assert!(
        a.fingerprint(Some("ab"), Some("c"), None) != a.fingerprint(Some("a"), Some("bc"), None)
    );
    assert!(a.fingerprint(Some("a"), None, None) != a.fingerprint(Some("a"), Some(""), None));
    assert!(
        a.fingerprint(Some("a"), Some("42"), None) != a.fingerprint(Some("a"), None, Some("42"))
    );
}

#[intent_test_macros::daemon_test]
async fn reader_observes_each_raw_field_and_requires_new_proof_after_restoration() {
    for (key, value) in [
        (SECRET_ACCOUNT, " stored-pat "),
        (REFRESH_SECRET_ACCOUNT, "external-refresh"),
        (EXPIRES_AT_SECRET_ACCOUNT, "0042"),
    ] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let original = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
        let expected = f.request();
        f.service.gitlab_secret_store.store(key, value).unwrap();
        let count = server.control.requests.lock().unwrap().len();
        assert_eq!(
            f.reader().load(&expected).await.unwrap_err(),
            Error::SecretMismatch
        );
        std::fs::write(f.service.gitlab_secret_store.path(), &original).unwrap();
        assert_eq!(
            f.reader().load(&expected).await.unwrap_err(),
            Error::Unverified
        );
        assert_eq!(server.control.requests.lock().unwrap().len(), count);
        assert_eq!(
            std::fs::read(f.service.gitlab_secret_store.path()).unwrap(),
            original
        );
        f.service
            .reconcile_gitlab_repository_binding()
            .await
            .unwrap();
        f.reader().load(&f.request()).await.unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn reader_missing_or_blank_invalidates_only_the_observed_proof() {
    for missing in [true, false] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let expected = f.request();
        if missing {
            f.service
                .gitlab_secret_store
                .delete(SECRET_ACCOUNT)
                .unwrap();
        } else {
            f.service
                .gitlab_secret_store
                .store(SECRET_ACCOUNT, "  ")
                .unwrap();
        }
        assert_eq!(
            f.reader().load(&expected).await.unwrap_err(),
            Error::Missing
        );
        f.service
            .gitlab_secret_store
            .store(SECRET_ACCOUNT, "stored-pat")
            .unwrap();
        assert_eq!(
            f.reader().load(&expected).await.unwrap_err(),
            Error::Unverified
        );
    }
}

#[intent_test_macros::daemon_test]
async fn reader_unprovable_file_failure_is_local_without_claiming_a_change() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let bytes = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
    let expected = f.request();
    std::fs::write(
        f.service.gitlab_secret_store.path(),
        "corrupt-fixture-secret",
    )
    .unwrap();
    let error = f.reader().load(&expected).await.unwrap_err();
    assert_eq!(error, Error::Indeterminate);
    assert!(!format!("{error:?} {error}").contains("corrupt-fixture-secret"));
    assert_eq!(
        std::fs::read_to_string(f.service.gitlab_secret_store.path()).unwrap(),
        "corrupt-fixture-secret"
    );
    std::fs::write(f.service.gitlab_secret_store.path(), bytes).unwrap();
    f.reader().load(&expected).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn reader_foreign_expected_and_environment_do_not_clear_original_evidence() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let expected = f.request();
    for field in 0..5 {
        let mut foreign = expected.clone();
        match field {
            0 => foreign.binding.account.instance_base_url.push_str("/other"),
            1 => foreign.binding.account.account_id = "43".into(),
            2 => foreign.binding.scope.connection_id = "foreign".into(),
            3 => foreign.secret_revision += 1,
            _ => foreign.source = RepositoryCredentialSource::GitlabEnvironment,
        }
        assert!(f.reader().load(&foreign).await.is_err());
        f.reader().load(&expected).await.unwrap();
    }
    let other = Fixture::new(&server).await;
    assert_eq!(
        other.reader().load(&expected).await.unwrap_err(),
        Error::Retired
    );
    other.reader().load(&other.request()).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn reader_startup_oauth_uses_existing_verified_persistence_once() {
    let server = Server::new().await;
    let f = Fixture::unadopted(&server).await;
    f.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
        .unwrap();
    f.service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let count = server.control.requests.lock().unwrap().len();
    let bytes = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
    f.reader().load(&f.request()).await.unwrap();
    assert_eq!(server.control.requests.lock().unwrap().len(), count);
    assert_eq!(
        server
            .control
            .exchanges
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        std::fs::read(f.service.gitlab_secret_store.path()).unwrap(),
        bytes
    );
    f.provider_read(&server, "rotated").await;
}

#[intent_test_macros::daemon_test]
async fn reader_device_completion_uses_original_grant_and_cancellation_keeps_prior_proof() {
    for cancel in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let original = f.request();
        *server.control.pause.lock().unwrap() = Some("grant_type=urn");
        f.service
            .gitlab_connect_device(server.host.clone())
            .await
            .unwrap();
        server.entered().await;
        if cancel {
            f.service.gitlab_auth.lock().await.flow = None;
        }
        server.control.release.notify_one();
        let guard = f.service.gitlab_credential_gate.lock().await;
        drop(guard);
        let current = f.request();
        if cancel {
            assert_eq!(current, original);
        } else {
            assert_ne!(current.binding.scope, original.binding.scope);
        }
        f.reader().load(&current).await.unwrap();
        f.provider_read(&server, if cancel { "stored-pat" } else { "rotated" })
            .await;
    }
}

#[intent_test_macros::daemon_test]
async fn reader_adoption_rejects_sibling_change_during_account_verification() {
    for key in [
        SECRET_ACCOUNT,
        REFRESH_SECRET_ACCOUNT,
        EXPIRES_AT_SECRET_ACCOUNT,
    ] {
        let server = Server::new().await;
        let f = Fixture::unadopted(&server).await;
        *server.control.pause.lock().unwrap() = Some("/api/v4/user");
        let svc = f.service.clone();
        let task = tokio::spawn(async move { svc.reconcile_gitlab_repository_binding().await });
        server.entered().await;
        f.service
            .gitlab_secret_store
            .store(key, "changed-during-proof")
            .unwrap();
        server.control.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            f.service
                .repository_connection_directory()
                .binding()
                .unwrap_err(),
            Error::Unverified
        );
        assert!(f
            .service
            .gitlab_credential_gate
            .repository
            .get()
            .unwrap()
            .evidence
            .published
            .lock()
            .unwrap()
            .is_none());
    }
}

pub(crate) struct PausedRead {
    entered: Arc<tokio::sync::Notify>,
    release: Option<std::sync::mpsc::Sender<()>>,
}
impl PausedRead {
    pub(crate) fn install(f: &Fixture) -> Self {
        let entered = Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let (release, receive) = std::sync::mpsc::channel();
        let receive = Mutex::new(receive);
        let once = std::sync::atomic::AtomicBool::new(true);
        *f.service
            .gitlab_credential_gate
            .repository
            .get()
            .unwrap()
            .evidence
            .read_probe
            .lock()
            .unwrap() = Some(Arc::new(move || {
            if once.swap(false, std::sync::atomic::Ordering::SeqCst) {
                signal.notify_one();
                receive.lock().unwrap().recv().unwrap();
            }
        }));
        Self {
            entered,
            release: Some(release),
        }
    }
    pub(crate) async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
    pub(crate) fn resume(&mut self) {
        if let Some(release) = self.release.take() {
            release.send(()).unwrap();
        }
    }
}
impl Drop for PausedRead {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[intent_test_macros::daemon_test]
async fn reader_waits_for_original_gate_and_detached_file_read_keeps_its_lease() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let directory = f.service.repository_connection_directory();
    let admission = f.admission(&server, Arc::new(Allowed));
    let reader = f.reader();
    let guard = f.service.gitlab_credential_gate.lock().await;
    assert!(matches!(
        directory
            .acquire_exact(&admission, reader.as_ref(), Duration::from_millis(20))
            .await,
        Err(Error::TimedOut)
    ));
    drop(guard);
    let mut paused = PausedRead::install(&f);
    let owner = directory.clone();
    let task = tokio::spawn(async move {
        owner
            .acquire_exact(&admission, reader.as_ref(), Duration::from_millis(100))
            .await
    });
    paused.entered().await;
    assert!(matches!(task.await.unwrap(), Err(Error::TimedOut)));
    assert!(
        f.service.gitlab_credential_gate.mutex.try_lock().is_err(),
        "detached blocking read retains the ORIGINAL gate"
    );
    paused.resume();
    let guard = tokio::time::timeout(
        Duration::from_secs(5),
        f.service.gitlab_credential_gate.lock(),
    )
    .await
    .unwrap();
    drop(guard);
    f.reader().load(&f.request()).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn reader_rechecks_retirement_and_changed_file_after_blocking_read() {
    for retired in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let expected = f.request();
        let reader = f.reader();
        let mut paused = PausedRead::install(&f);
        let task = tokio::spawn(async move { reader.load(&expected).await });
        paused.entered().await;
        if retired {
            f.service
                .repository_connection_directory()
                .retire()
                .unwrap();
        } else {
            f.service
                .gitlab_secret_store
                .store(SECRET_ACCOUNT, "changed-while-read-pending")
                .unwrap();
        }
        paused.resume();
        assert_eq!(
            task.await.unwrap().unwrap_err(),
            if retired {
                Error::Retired
            } else {
                Error::SecretMismatch
            }
        );
    }
}

struct PausedAuthority {
    gate: GitlabCredentialGate,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    deny: std::sync::atomic::AtomicBool,
}
impl RepositoryAuthority for PausedAuthority {
    fn revalidate<'a>(
        &'a self,
        _: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            assert!(
                self.gate.mutex.try_lock().is_ok(),
                "reader must release the credential gate before the authority fence"
            );
            self.entered.notify_one();
            self.release.notified().await;
            if self.deny.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::AuthorityDenied);
            }
            Ok(Box::new(Allowed) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

#[intent_test_macros::daemon_test]
async fn reader_post_load_authority_denial_or_real_replacement_prevents_http() {
    for replace in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server).await;
        let authority = Arc::new(PausedAuthority {
            gate: f.service.gitlab_credential_gate.clone(),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            deny: std::sync::atomic::AtomicBool::new(false),
        });
        let callback = BoundGitlabRequestCredentials::new(
            f.service.repository_connection_directory(),
            f.admission(&server, authority.clone()),
            f.reader(),
            Duration::from_secs(5),
        )
        .unwrap();
        let provider = callback.into_provider().unwrap();
        let read = tokio::spawn(async move { provider.get_repo("group", "project").await });
        tokio::time::timeout(Duration::from_secs(5), authority.entered.notified())
            .await
            .unwrap();
        if replace {
            f.service
                .gitlab_connect_pat(server.host.clone(), "pat-second".into())
                .await
                .unwrap();
        } else {
            authority
                .deny
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let before = server.control.requests.lock().unwrap().len();
        authority.release.notify_one();
        assert!(read.await.unwrap().is_err());
        assert_eq!(
            server.control.requests.lock().unwrap().len(),
            before,
            "no provider dispatch after failed final authority/lifetime check"
        );
        f.reader().load(&f.request()).await.unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn reader_child_policy_changes_preserve_native_source_and_noops_preserve_proof() {
    use intent_core::WorkspaceApi;
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    let directory = f.service.repository_connection_directory();
    let original = f.request();
    let native = f.admission(&server, Arc::new(Allowed));
    directory.set_child_policy(&original.binding, true).unwrap();
    let child = directory
        .child_policy_checkpoint(original.binding.clone())
        .unwrap();
    let pending = directory.begin_child_policy(&child).unwrap();
    directory.finish_child_policy(&pending, false).unwrap();
    let reader = f.reader();
    directory
        .acquire_exact(&native, reader.as_ref(), Duration::from_secs(2))
        .await
        .unwrap();
    let calls = server.control.requests.lock().unwrap().len();
    f.service
        .settings_update(json!([
            {"path":"git.autoCommit","value":false},
            {"path":"sourceControl.gitlab.oauthClientId","value":"client"},
            {"path":SECRET_ACCOUNT,"value":"stored-pat"}
        ]))
        .await
        .unwrap();
    assert_eq!(f.request(), original);
    assert_eq!(server.control.requests.lock().unwrap().len(), calls);
    reader.load(&original).await.unwrap();
    assert_eq!(f.registry.get("git.autoCommit"), Some(json!(false)));
}

#[intent_test_macros::daemon_test]
async fn reader_reused_provider_reloads_source_before_every_request() {
    let server = Server::new().await;
    let f = Fixture::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    let provider = f.callback(&server).into_provider().unwrap();
    provider.get_repo("group", "project").await.unwrap();
    let calls = server.control.requests.lock().unwrap().len();
    f.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "external-after-first-request")
        .unwrap();
    assert!(matches!(
        provider.get_repo("group", "project").await,
        Err(intent_sourcecontrol::Error::AdmissionUnavailable(_))
    ));
    assert_eq!(
        server.control.requests.lock().unwrap().len(),
        calls,
        "no cached token or second HTTP request"
    );
}

#[intent_test_macros::daemon_test]
async fn reader_requires_the_original_installed_source_and_preserves_existing_normalization() {
    let server = Server::new().await;
    let dir = crate::test_support::test_tempdir("reader-without-attachment");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
    let service = crate::Services::new_repository_fixture(store, secrets, None);
    assert!(matches!(
        service.gitlab_repository_secret_reader(),
        Err(Error::Unverified)
    ));
    assert!(server.control.requests.lock().unwrap().is_empty());

    let f = Fixture::unadopted(&server).await;
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, " stored-pat ")
        .unwrap();
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    f.reader().load(&f.request()).await.unwrap();
    f.provider_read(&server, "stored-pat").await;
    let bytes = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
    // The existing strict store parser defines empty strings as missing. The
    // new selected-key reader preserves that compatibility, not raw JSON identity.
    let mut map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&bytes).unwrap();
    map.insert(REFRESH_SECRET_ACCOUNT.into(), json!(""));
    std::fs::write(
        f.service.gitlab_secret_store.path(),
        serde_json::to_vec(&map).unwrap(),
    )
    .unwrap();
    f.reader().load(&f.request()).await.unwrap();
    assert_eq!(
        f.service
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some(" stored-pat ")
    );
}
