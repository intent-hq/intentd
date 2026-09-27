use super::*;
use crate::repository_credentials::{RepositoryConnectionDirectory, RepositoryCredentialError};
use intent_sourcecontrol::gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT};
use intent_sourcecontrol::{GitlabDescriptor, GitlabInstance};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::timeout;

#[derive(Default)]
pub(super) struct Control {
    pub(super) pause: Mutex<Option<&'static str>>,
    pub(super) entered: Notify,
    pub(super) release: Notify,
    pub(super) token_reply: Mutex<Option<(u16, Value)>>,
    pub(super) denied_user: Mutex<Option<&'static str>>,
    pub(super) requests: Mutex<Vec<String>>,
    pub(super) exchanges: AtomicUsize,
    pub(super) directory: Mutex<Option<Arc<RepositoryConnectionDirectory>>>,
}
pub(super) struct Server {
    pub(super) host: GitlabHost,
    pub(super) descriptor: GitlabDescriptor,
    pub(super) control: Arc<Control>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(super) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let host = GitlabHost::parse("gitlab.test")
            .unwrap()
            .with_api_origin(&origin)
            .unwrap();
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse("https://gitlab.test/forge").unwrap(),
            &origin,
        )
        .unwrap();
        let control = Arc::new(Control::default());
        let task_control = control.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let control = task_control.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut chunk = [0; 4096];
                    loop {
                        let count = socket.read(&mut chunk).await.unwrap();
                        if count == 0 {
                            return;
                        }
                        request.extend_from_slice(&chunk[..count]);
                        if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&request[..end]);
                            let size = header
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if request.len() >= end + 4 + size {
                                break;
                            }
                        }
                    }
                    let request = String::from_utf8(request).unwrap();
                    let path = request.split_whitespace().nth(1).unwrap();
                    control.requests.lock().unwrap().push(path.into());
                    let pause = {
                        let mut pause = control.pause.lock().unwrap();
                        if pause.is_some_and(|value| request.contains(value)) {
                            pause.take();
                            true
                        } else {
                            false
                        }
                    };
                    if pause {
                        control.entered.notify_one();
                        control.release.notified().await;
                    }
                    let (status, body) = if path == "/oauth/authorize_device" {
                        (
                            200,
                            json!({"device_code":"device-code", "user_code":"CODE", "verification_uri":"https://gitlab.test/device", "expires_in":900, "interval":1}),
                        )
                    } else if path == "/oauth/token" {
                        if request.contains("grant_type=refresh_token") {
                            if let Some(directory) = control.directory.lock().unwrap().clone() {
                                assert!(
                                    matches!(
                                        directory.binding(),
                                        Err(RepositoryCredentialError::Mutating)
                                    ),
                                    "retire before the actual refresh exchange"
                                );
                            }
                        }
                        control.exchanges.fetch_add(1, Ordering::SeqCst);
                        control.token_reply.lock().unwrap().clone().unwrap_or_else(|| (200, json!({"access_token":"rotated", "refresh_token":"refresh-rotated", "expires_in":7200, "token_type":"Bearer", "scope":"api"})))
                    } else if path == "/api/v4/user" {
                        if control
                            .denied_user
                            .lock()
                            .unwrap()
                            .is_some_and(|token| request.contains(token))
                        {
                            (401, json!({"message":"rejected"}))
                        } else {
                            let id = if request.contains("zero-account") {
                                0
                            } else if request.contains("pat-second") {
                                43
                            } else {
                                42
                            };
                            (200, json!({"id":id,"username":"fixture", "name":"Fixture"}))
                        }
                    } else {
                        (404, json!({}))
                    };
                    let body = body.to_string();
                    let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            host,
            descriptor,
            control,
            task,
        }
    }
    pub(super) async fn entered(&self) {
        timeout(Duration::from_secs(5), self.control.entered.notified())
            .await
            .unwrap();
    }
}
struct Fixture {
    tempdir: tempfile::TempDir,
    svc: Arc<crate::Services>,
    directory: Arc<RepositoryConnectionDirectory>,
}
impl Fixture {
    async fn new(server: &Server, approved: bool) -> Self {
        let dir = crate::test_support::test_tempdir("gitlab-owner-");
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let registry =
            Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                (
                    "sourceControl.gitlab.host".into(),
                    json!(server.host.host()),
                ),
                (
                    "sourceControl.gitlab.oauthClientId".into(),
                    json!("fixture-client"),
                ),
            ])
            .unwrap();
        let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
        let svc = Arc::new(
            crate::Services::new_repository_fixture(
                store,
                secrets,
                approved.then(|| server.descriptor.clone()),
            )
            .with_settings_registry(registry)
            .with_workspaces_root(dir.path().join("workspaces")),
        );
        let directory = svc.repository_connection_directory();
        *server.control.directory.lock().unwrap() = Some(directory.clone());
        Self {
            tempdir: dir,
            svc,
            directory,
        }
    }
    async fn pat(&self, server: &Server, token: &str) {
        self.svc
            .gitlab_connect_pat(server.host.clone(), token.into())
            .await
            .unwrap();
    }
    fn token(&self) -> Option<String> {
        self.svc
            .gitlab_secret_store
            .load(GITLAB_SECRET_ACCOUNT)
            .unwrap()
    }
    fn device_expiry(&self, expires_at: &str) {
        self.svc
            .gitlab_secret_store
            .store(REFRESH_SECRET_ACCOUNT, "old-refresh")
            .unwrap();
        self.svc
            .gitlab_secret_store
            .store(EXPIRES_AT_SECRET_ACCOUNT, expires_at)
            .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn pat_writer_publishes_only_the_explicit_full_instance_and_verified_account() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let old = f.directory.binding().unwrap();
    assert_eq!(old.account.instance_base_url, "https://gitlab.test/forge");
    assert_eq!(old.account.account_id, "42");
    f.directory.set_child_policy(&old, true).unwrap();
    f.pat(&server, "pat-second").await;
    let new = f.directory.binding().unwrap();
    assert_eq!(new.account.account_id, "43");
    assert_ne!(old.scope.connection_id, new.scope.connection_id);
    assert_eq!(f.token().as_deref(), Some("pat-second"));
}

#[intent_test_macros::daemon_test]
async fn unadopted_host_and_successful_client_response_cannot_publish_ready() {
    let server = Server::new().await;
    let f = Fixture::new(&server, false).await;
    f.pat(&server, "pat-first").await;
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn failed_existing_host_publication_cannot_publish_a_verified_repository_binding() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    let registry = f.svc.settings_registry.as_ref().unwrap();
    registry
        .pin(
            "sourceControl.gitlab.host",
            json!("pinned.test"),
            "fixture-instance",
        )
        .unwrap();
    // Positive account/descriptor evidence alone cannot replace the existing
    // settings owner's rejected binding publication.
    f.pat(&server, "pat-first").await;
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        registry.get("sourceControl.gitlab.host"),
        Some(json!("pinned.test"))
    );
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn pending_owner_cannot_be_rebound_to_a_replacement_directory_attachment() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    *server.control.pause.lock().unwrap() = Some("pat-slow");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let pending =
        tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-slow".into()).await });
    server.entered().await;
    let replacement = Arc::new(RepositoryConnectionDirectory::new("other-daemon".into()));
    assert_eq!(
        f.svc
            .gitlab_credential_gate
            .attach_repository(replacement.clone(), None),
        Err(RepositoryCredentialError::StaleMutation)
    );
    server.control.release.notify_one();
    pending.await.unwrap().unwrap();
    let binding = f.directory.binding().unwrap();
    assert_eq!(binding.daemon_id, f.svc.daemon_boot_id);
    assert_eq!(
        binding.account.instance_base_url,
        "https://gitlab.test/forge"
    );
    assert_eq!(binding.account.account_id, "42");
    assert_eq!(
        replacement.binding(),
        Err(RepositoryCredentialError::Unverified)
    );
}

#[intent_test_macros::daemon_test]
async fn late_pat_validation_cannot_overwrite_a_newer_original_writer() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    *server.control.pause.lock().unwrap() = Some("pat-slow");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let old = tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-slow".into()).await });
    server.entered().await;
    f.pat(&server, "pat-second").await;
    let binding = f.directory.binding().unwrap();
    server.control.release.notify_one();
    assert!(old.await.unwrap().is_err());
    assert_eq!(f.token().as_deref(), Some("pat-second"));
    assert_eq!(f.directory.binding().unwrap(), binding);
}

#[intent_test_macros::daemon_test]
async fn rejected_pat_preflight_keeps_the_original_connection_and_secrets() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let binding = f.directory.binding().unwrap();
    *server.control.denied_user.lock().unwrap() = Some("pat-rejected");
    assert!(f
        .svc
        .gitlab_connect_pat(server.host.clone(), "pat-rejected".into())
        .await
        .is_err());
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(f.directory.binding().unwrap(), binding);
}

#[intent_test_macros::daemon_test]
async fn proactive_refresh_and_proof_read_preserve_binding_and_child_checkpoint() {
    for proof in [false, true] {
        let server = Server::new().await;
        let f = Fixture::new(&server, true).await;
        f.pat(&server, "pat-first").await;
        let binding = f.directory.binding().unwrap();
        let child = f
            .directory
            .child_policy_checkpoint(binding.clone())
            .unwrap();
        f.device_expiry("0");
        if proof {
            assert_eq!(
                f.svc
                    .stored_proof_token(&Target::Gitlab {
                        host: server.host.clone()
                    })
                    .await
                    .unwrap(),
                "rotated"
            );
        } else {
            let result = probe_gitlab(
                &server.host,
                &|| true,
                Some("fixture-client"),
                f.svc.gitlab_secret_store.clone(),
                &f.svc.gitlab_credential_gate,
                None,
            )
            .await
            .unwrap();
            assert!(matches!(
                result,
                ProbeOutcome::Configured {
                    method: "device",
                    ..
                }
            ));
        }
        assert_eq!(f.directory.binding().unwrap(), binding);
        let ticket = f.directory.begin_child_policy(&child).unwrap();
        f.directory.finish_child_policy(&ticket, true).unwrap();
        assert_eq!(server.control.exchanges.load(Ordering::SeqCst), 1);
        assert_eq!(f.token().as_deref(), Some("rotated"));
    }
}

#[intent_test_macros::daemon_test]
async fn rejected_access_token_refresh_uses_the_same_directory_owner() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let binding = f.directory.binding().unwrap();
    f.device_expiry("9999999999");
    *server.control.denied_user.lock().unwrap() = Some("pat-first");
    assert!(matches!(
        probe_gitlab(
            &server.host,
            &|| true,
            Some("fixture-client"),
            f.svc.gitlab_secret_store.clone(),
            &f.svc.gitlab_credential_gate,
            None
        )
        .await
        .unwrap(),
        ProbeOutcome::Configured { .. }
    ));
    assert_eq!(f.directory.binding().unwrap(), binding);
    assert_eq!(server.control.exchanges.load(Ordering::SeqCst), 1);
}

#[intent_test_macros::daemon_test]
async fn refused_refresh_deletes_through_its_original_owner() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("0");
    *server.control.token_reply.lock().unwrap() = Some((400, json!({"error":"invalid_grant"})));
    assert_eq!(
        probe_gitlab(
            &server.host,
            &|| true,
            Some("fixture-client"),
            f.svc.gitlab_secret_store.clone(),
            &f.svc.gitlab_credential_gate,
            None
        )
        .await
        .unwrap(),
        ProbeOutcome::NotConfigured
    );
    assert_eq!(f.token(), None);
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Disconnected)
    );
}

#[intent_test_macros::daemon_test]
async fn transient_refresh_retains_secrets_without_claiming_settlement() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("0");
    *server.control.token_reply.lock().unwrap() =
        Some((503, json!({"error":"temporarily_unavailable"})));
    assert!(matches!(
        probe_gitlab(
            &server.host,
            &|| true,
            Some("fixture-client"),
            f.svc.gitlab_secret_store.clone(),
            &f.svc.gitlab_credential_gate,
            None
        )
        .await
        .unwrap(),
        ProbeOutcome::Configured { .. }
    ));
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn wrong_host_and_old_rejection_are_noops_but_matching_cleanup_retires() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let binding = f.directory.binding().unwrap();
    f.svc
        .gitlab_revoke_owned(GitlabHost::parse("other.test").unwrap())
        .await
        .unwrap();
    disconnect_gitlab_if_current(
        f.svc.gitlab_secret_store.clone(),
        &|| true,
        &f.svc.gitlab_credential_gate,
        None,
        &server.host,
        "old-token",
    )
    .await
    .unwrap();
    assert_eq!(f.directory.binding().unwrap(), binding);
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    disconnect_gitlab_if_current(
        f.svc.gitlab_secret_store.clone(),
        &|| true,
        &f.svc.gitlab_credential_gate,
        None,
        &server.host,
        "pat-first",
    )
    .await
    .unwrap();
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Disconnected)
    );
    assert_eq!(f.token(), None);
}

#[intent_test_macros::daemon_test]
async fn late_device_grant_after_cancel_keeps_the_prior_connection() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let binding = f.directory.binding().unwrap();
    *server.control.pause.lock().unwrap() = Some("grant_type=urn");
    f.svc
        .gitlab_connect_device(server.host.clone())
        .await
        .unwrap();
    server.entered().await;
    f.svc.gitlab_auth.lock().await.flow = None;
    server.control.release.notify_one();
    let _gate = f.svc.gitlab_credential_gate.lock().await;
    assert_eq!(f.directory.binding().unwrap(), binding);
    assert_eq!(f.token().as_deref(), Some("pat-first"));
}

#[intent_test_macros::daemon_test]
async fn device_completion_uses_actual_grant_verification_and_persistence() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    *server.control.pause.lock().unwrap() = Some("grant_type=urn");
    f.svc
        .gitlab_connect_device(server.host.clone())
        .await
        .unwrap();
    server.entered().await;
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Unverified)
    );
    server.control.release.notify_one();
    let _gate = f.svc.gitlab_credential_gate.lock().await;
    assert_eq!(f.directory.binding().unwrap().account.account_id, "42");
    assert_eq!(f.token().as_deref(), Some("rotated"));
    assert!(f.svc.gitlab_auth.lock().await.flow.is_none());
}

#[intent_test_macros::daemon_test]
async fn unrelated_revoke_does_not_steal_a_pending_pat_reservation() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    *server.control.pause.lock().unwrap() = Some("pat-slow");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let pending =
        tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-slow".into()).await });
    server.entered().await;
    f.svc
        .gitlab_revoke_owned(GitlabHost::parse("other.test").unwrap())
        .await
        .unwrap();
    server.control.release.notify_one();
    pending.await.unwrap().unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-slow"));
}

#[intent_test_macros::daemon_test]
async fn unrelated_revoke_remains_noop_when_original_settlement_is_unknown() {
    let server = Server::new().await;
    let f = Fixture::new(&server, false).await;
    f.pat(&server, "pat-first").await;
    f.svc
        .gitlab_revoke_owned(GitlabHost::parse("other.test").unwrap())
        .await
        .unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn refresh_with_unverified_or_changed_account_cannot_publish_continuity() {
    for rejected in [true, false] {
        let server = Server::new().await;
        let f = Fixture::new(&server, true).await;
        f.pat(&server, "pat-first").await;
        f.device_expiry("0");
        if rejected {
            *server.control.denied_user.lock().unwrap() = Some("rotated");
        } else {
            *server.control.token_reply.lock().unwrap() = Some((
                200,
                json!({"access_token":"pat-second", "refresh_token":"new-refresh", "expires_in":7200}),
            ));
        }
        let _ = probe_gitlab(
            &server.host,
            &|| true,
            Some("fixture-client"),
            f.svc.gitlab_secret_store.clone(),
            &f.svc.gitlab_credential_gate,
            None,
        )
        .await;
        assert_eq!(
            f.directory.binding(),
            Err(RepositoryCredentialError::Indeterminate)
        );
        assert_eq!(
            f.token().as_deref(),
            Some(if rejected { "rotated" } else { "pat-second" })
        );
    }
}

use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    RepositoryCredentialAdmission, RepositoryCredentialUse, RepositorySecretReader,
    RepositorySecretRequest, RepositorySecretSnapshot,
};

pub(super) struct FixtureAuthority;
impl RepositoryAuthority for FixtureAuthority {
    fn revalidate<'a>(
        &'a self,
        _request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async { Ok(Box::new(Self) as Box<dyn RepositoryAuthorityFence>) })
    }
}
impl RepositoryAuthorityFence for FixtureAuthority {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> crate::repository_credentials::Result<()> + Send),
    ) -> crate::repository_credentials::Result<()> {
        action()
    }
}
pub(super) struct FixtureSecretReader(pub(super) FileSecretStore);
impl RepositorySecretReader for FixtureSecretReader {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> CredentialFuture<'a, RepositorySecretSnapshot> {
        Box::pin(async move {
            Ok(RepositorySecretSnapshot {
                request: expected.clone(),
                token: intent_sourcecontrol::SecretString::from(
                    self.0.load(GITLAB_SECRET_ACCOUNT).unwrap().unwrap(),
                ),
            })
        })
    }
}
impl Fixture {
    fn admission(&self, server: &Server) -> RepositoryCredentialAdmission {
        let binding = self.directory.binding().unwrap();
        self.directory
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
            .unwrap()
    }
}

#[intent_test_macros::daemon_test]
async fn real_refresh_retains_in_flight_quota_and_ignores_old_token_rejection() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("0");
    let binding = f.directory.binding().unwrap();
    let admission = f.admission(&server);
    let reader = FixtureSecretReader(f.svc.gitlab_secret_store.clone());
    let ticket = f
        .directory
        .acquire_exact(&admission, &reader, Duration::from_secs(2))
        .await
        .unwrap();
    *server.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let refresh =
        tokio::spawn(async move { svc.stored_proof_token(&Target::Gitlab { host }).await });
    server.entered().await;
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Mutating)
    );
    assert!(f
        .directory
        .record_backoff(
            ticket.dispatch_stamp(),
            std::time::Instant::now() + Duration::from_secs(3600)
        )
        .unwrap());
    assert!(f
        .directory
        .record_backoff(ticket.dispatch_stamp(), std::time::Instant::now())
        .unwrap());
    assert!(!f
        .directory
        .reject_current_credential(ticket.dispatch_stamp())
        .unwrap());
    server.control.release.notify_one();
    assert_eq!(refresh.await.unwrap().unwrap(), "rotated");
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
    f.pat(&server, "pat-second").await;
    assert!(!f
        .directory
        .record_backoff(
            ticket.dispatch_stamp(),
            std::time::Instant::now() + Duration::from_secs(7200)
        )
        .unwrap());
    let new_admission = f.admission(&server);
    assert!(f
        .directory
        .acquire_exact(&new_admission, &reader, Duration::from_secs(2))
        .await
        .is_ok());
}

struct EffectPause {
    entered: Arc<Notify>,
    state: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl EffectPause {
    fn install(f: &Fixture, call: usize) -> Self {
        let entered = Arc::new(Notify::new());
        let state = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let hit = entered.clone();
        let held = state.clone();
        let count = AtomicUsize::new(0);
        let directory = f.directory.clone();
        let store = f.svc.gitlab_secret_store.clone();
        f.svc
            .gitlab_credential_gate
            .set_write_probe(Arc::new(move || {
                if count.fetch_add(1, Ordering::SeqCst) + 1 != call {
                    return;
                }
                assert_eq!(
                    directory.binding(),
                    Err(RepositoryCredentialError::Mutating)
                );
                assert_eq!(
                    store.load(GITLAB_SECRET_ACCOUNT).unwrap().as_deref(),
                    Some("pat-first")
                );
                hit.notify_one();
                let (lock, ready) = &*held;
                let mut release = lock.lock().unwrap();
                while !*release {
                    release = ready.wait(release).unwrap();
                }
            }));
        Self { entered, state }
    }
    fn release(&self) {
        *self.state.0.lock().unwrap() = true;
        self.state.1.notify_all();
    }
}
impl Drop for EffectPause {
    fn drop(&mut self) {
        self.release();
    }
}

#[intent_test_macros::daemon_test]
async fn abandoned_pat_persistence_holds_original_gate_and_never_claims_binding_publication() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let paused = EffectPause::install(&f, 1);
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-second".into()).await });
    timeout(Duration::from_secs(5), paused.entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(timeout(
        Duration::from_millis(20),
        f.svc.gitlab_credential_gate.lock()
    )
    .await
    .is_err());
    let requests = server.control.requests.lock().unwrap().len();
    assert!(f
        .svc
        .gitlab_connect_pat(server.host.clone(), "successor".into())
        .await
        .is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
    paused.release();
    let _gate = timeout(Duration::from_secs(5), f.svc.gitlab_credential_gate.lock())
        .await
        .unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-second"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn abandoned_refresh_settles_only_from_the_actual_persistence_owner() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let binding = f.directory.binding().unwrap();
    f.device_expiry("0");
    let paused = EffectPause::install(&f, 2);
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move { svc.stored_proof_token(&Target::Gitlab { host }).await });
    timeout(Duration::from_secs(5), paused.entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Mutating)
    );
    assert!(timeout(
        Duration::from_millis(20),
        f.svc.gitlab_credential_gate.lock()
    )
    .await
    .is_err());
    paused.release();
    {
        let _gate = timeout(Duration::from_secs(5), f.svc.gitlab_credential_gate.lock())
            .await
            .unwrap();
        assert_eq!(f.token().as_deref(), Some("rotated"));
        assert_eq!(f.directory.binding().unwrap(), binding);
    }
    f.pat(&server, "pat-second").await;
    assert_ne!(f.directory.binding().unwrap().scope, binding.scope);
}

#[intent_test_macros::daemon_test]
async fn failed_actual_store_write_and_external_repair_do_not_claim_compensation() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    let path = f.tempdir.path().join("secrets.json");
    let original = std::fs::read(&path).unwrap();
    let corrupted = path.clone();
    f.svc
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            std::fs::remove_file(&corrupted).unwrap();
            std::fs::create_dir(&corrupted).unwrap();
        }));
    assert!(f
        .svc
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .is_err());
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(path, original).unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn changed_transport_with_same_host_and_user_cannot_adopt_a_connection() {
    let server = Server::new().await;
    let foreign = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.svc
        .gitlab_connect_pat(foreign.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-second"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
}

#[intent_test_macros::daemon_test]
async fn retired_directory_during_refresh_cannot_persist_the_late_exchange() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("0");
    *server.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move { svc.stored_proof_token(&Target::Gitlab { host }).await });
    server.entered().await;
    f.directory.retire().unwrap();
    // This test intentionally retires after exchange dispatch. A late response
    // must not mutate the local pair, even though the upstream effect was sent.
    *server.control.directory.lock().unwrap() = None;
    server.control.release.notify_one();
    assert!(
        matches!(task.await.unwrap(), Err(Error::Internal(_))),
        "local writer retirement must not return a fallback proof token"
    );
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.svc
            .gitlab_secret_store
            .load(REFRESH_SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("old-refresh")
    );
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Retired)
    );
}

#[intent_test_macros::daemon_test]
async fn retired_rejection_refresh_stays_local_without_clearing_the_pair() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("9999999999");
    *server.control.denied_user.lock().unwrap() = Some("pat-first");
    *server.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move {
        probe_gitlab(
            &host,
            &|| true,
            Some("fixture-client"),
            svc.gitlab_secret_store.clone(),
            &svc.gitlab_credential_gate,
            None,
        )
        .await
    });
    server.entered().await;
    f.directory.retire().unwrap();
    *server.control.directory.lock().unwrap() = None;
    server.control.release.notify_one();
    assert!(matches!(task.await.unwrap(), Err(Error::Internal(_))));
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.svc
            .gitlab_secret_store
            .load(REFRESH_SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("old-refresh")
    );
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Retired)
    );
}

#[intent_test_macros::daemon_test]
async fn administrator_checked_revoke_route_clears_the_pair_and_pending_startup() {
    use intent_core::WorkspaceApi;
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry("9999999999");
    f.svc.gitlab_auth.lock().await.starting = Some(GitlabStartupIntent {
        host: server.host.host().into(),
        id: github_auth_ops::next_flow_id(),
    });
    assert_eq!(
        f.svc
            .source_control_revoke("gitlab".into(), None)
            .await
            .unwrap(),
        json!({"ok":true})
    );
    assert_eq!(f.token(), None);
    assert_eq!(
        f.svc
            .gitlab_secret_store
            .load(REFRESH_SECRET_ACCOUNT)
            .unwrap(),
        None
    );
    assert_eq!(
        f.svc
            .gitlab_secret_store
            .load(EXPIRES_AT_SECRET_ACCOUNT)
            .unwrap(),
        None
    );
    assert!(f.svc.gitlab_auth.lock().await.starting.is_none());
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Disconnected)
    );
}

async fn transient_after_await(retry: bool, retired: bool) {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    f.device_expiry(if retry { "9999999999" } else { "0" });
    *server.control.denied_user.lock().unwrap() = retry.then_some("pat-first");
    *server.control.token_reply.lock().unwrap() = Some((503, json!({"error":"unavailable"})));
    *server.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move {
        if retry {
            let result = probe_gitlab(
                &host,
                &|| true,
                Some("fixture-client"),
                svc.gitlab_secret_store.clone(),
                &svc.gitlab_credential_gate,
                None,
            )
            .await;
            if !retired {
                assert!(matches!(&result, Ok(ProbeOutcome::Rejected)));
            }
            result.map(|_| ())
        } else {
            svc.stored_proof_token(&Target::Gitlab { host })
                .await
                .map(|token| assert_eq!(token, "pat-first"))
        }
    });
    server.entered().await;
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Mutating)
    );
    if retired {
        f.directory.retire().unwrap();
        *server.control.directory.lock().unwrap() = None;
    }
    server.control.release.notify_one();
    let result = task.await.unwrap();
    if retired {
        assert!(
            matches!(result, Err(Error::Internal(ref message)) if message == "repository admission retired"),
            "retirement during an upstream failure must stop the original caller locally"
        );
    } else {
        result.unwrap();
    }
    assert_eq!(f.token().as_deref(), Some("pat-first"));
    assert_eq!(
        f.svc
            .gitlab_secret_store
            .load(REFRESH_SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("old-refresh")
    );
    assert_eq!(
        f.directory.binding(),
        Err(if retired {
            RepositoryCredentialError::Retired
        } else {
            RepositoryCredentialError::Indeterminate
        })
    );
}

#[intent_test_macros::daemon_test]
async fn transient_failure_after_retired_proactive_refresh_is_local() {
    transient_after_await(false, true).await;
}

#[intent_test_macros::daemon_test]
async fn transient_failure_after_retired_rejection_refresh_is_local() {
    transient_after_await(true, true).await;
}

#[intent_test_macros::daemon_test]
async fn transient_failure_after_current_rejection_refresh_preserves_legacy_result() {
    transient_after_await(true, false).await;
}

#[test]
fn owner_check_rejects_unstarted_foreign_settled_and_retired_mutations() {
    use crate::repository_credentials::SettledCredentialState;
    let directory = RepositoryConnectionDirectory::new("daemon".into());
    let ticket = directory
        .reserve_mutation(RepositoryMutationKind::Replace)
        .unwrap();
    assert_eq!(
        directory.check_mutation(&ticket),
        Err(RepositoryCredentialError::StaleMutation)
    );
    directory.begin_mutation(&ticket).unwrap();
    assert!(directory.check_mutation(&ticket).is_ok());
    let foreign = RepositoryConnectionDirectory::new("daemon".into());
    let foreign_ticket = foreign
        .reserve_mutation(RepositoryMutationKind::Replace)
        .unwrap();
    foreign.begin_mutation(&foreign_ticket).unwrap();
    assert_eq!(
        directory.check_mutation(&foreign_ticket),
        Err(RepositoryCredentialError::StaleMutation)
    );
    directory
        .finish_mutation(&ticket, SettledCredentialState::Indeterminate)
        .unwrap();
    assert!(directory.check_mutation(&ticket).is_ok());
    assert_eq!(
        directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
    directory
        .finish_mutation(&ticket, SettledCredentialState::Disconnected)
        .unwrap();
    assert_eq!(
        directory.check_mutation(&ticket),
        Err(RepositoryCredentialError::StaleMutation)
    );
    let successor = directory
        .reserve_mutation(RepositoryMutationKind::Replace)
        .unwrap();
    directory.begin_mutation(&successor).unwrap();
    assert_eq!(
        directory.check_mutation(&ticket),
        Err(RepositoryCredentialError::StaleMutation)
    );
    assert!(directory.check_mutation(&successor).is_ok());
    directory.retire().unwrap();
    assert_eq!(
        directory.check_mutation(&successor),
        Err(RepositoryCredentialError::StaleMutation)
    );
}

#[test]
fn owner_check_preserves_original_indeterminate_completion_without_reopening_settled_work() {
    use crate::repository_credential_writers::{
        RepositoryCredentialWriters, RepositoryWriterPreflight,
    };
    use crate::repository_credentials::SettledCredentialState;
    let directory = Arc::new(RepositoryConnectionDirectory::new("daemon".into()));
    let writers = RepositoryCredentialWriters::new(directory.clone());
    let mutation = writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap()
        .begin(|| Ok(RepositoryWriterPreflight::Change))
        .unwrap()
        .unwrap();
    assert!(mutation.check_current().is_ok());
    let completion = mutation.completion();
    completion.indeterminate().unwrap();
    assert!(mutation.check_current().is_ok());
    assert_eq!(
        directory.binding(),
        Err(RepositoryCredentialError::Indeterminate)
    );
    completion
        .complete(SettledCredentialState::Disconnected)
        .unwrap();
    assert_eq!(
        mutation.check_current(),
        Err(RepositoryCredentialError::StaleMutation)
    );
    drop(mutation);
    assert_eq!(
        directory.binding(),
        Err(RepositoryCredentialError::Disconnected)
    );
}

#[intent_test_macros::daemon_test]
async fn retirement_after_write_admission_cannot_recall_the_effect_or_publish_ready() {
    let server = Server::new().await;
    let f = Fixture::new(&server, true).await;
    f.pat(&server, "pat-first").await;
    // The probe pauses inside the blocking writer AFTER its synchronous check.
    // This models an already admitted effect, not a fresh callback admission.
    let paused = EffectPause::install(&f, 1);
    let svc = f.svc.clone();
    let host = server.host.clone();
    let task = tokio::spawn(async move { svc.gitlab_connect_pat(host, "pat-second".into()).await });
    timeout(Duration::from_secs(5), paused.entered.notified())
        .await
        .unwrap();
    f.directory.retire().unwrap();
    paused.release();
    task.await.unwrap().unwrap();
    assert_eq!(f.token().as_deref(), Some("pat-second"));
    assert_eq!(
        f.directory.binding(),
        Err(RepositoryCredentialError::Retired)
    );
    let requests = server.control.requests.lock().unwrap().len();
    assert!(f
        .svc
        .gitlab_connect_pat(server.host.clone(), "new-request".into())
        .await
        .is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
    assert_eq!(f.token().as_deref(), Some("pat-second"));
}
