//! Actual paired Services/file/owner and loopback HTTP. Caller authority is
//! deliberately injected; real socket/host capture is qualified by R's tests.
use super::*;
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{Fixture, Server};
use intent_git::native_checkout::NativeCheckoutCredentials;
use intent_sourcecontrol::{PageParams, SourceControl};
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
    fn dispatch(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        let current = self.0.lock().unwrap();
        if !*current {
            return Err(local(RepositoryCredentialError::AuthorityDenied));
        }
        action()
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_capture_is_metadata_only_until_original_admission() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let before = server.control.requests.lock().unwrap().len();
    let bytes = std::fs::read(fixture.service.gitlab_secret_store.path()).unwrap();
    let guard = fixture.service.gitlab_credential_gate.lock().await;
    let capture = fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap();
    let connection = capture.admit(Caller::current()).unwrap();
    assert_eq!(connection.instance_base_url(), "https://gitlab.test/forge");
    drop(guard);
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
    assert_eq!(
        std::fs::read(fixture.service.gitlab_secret_store.path()).unwrap(),
        bytes
    );
}

#[intent_test_macros::daemon_test]
async fn checkout_capture_refuses_replacement_during_pre_admission_awaits() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let capture = fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap();
    fixture
        .service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let before = server.control.requests.lock().unwrap().len();
    assert!(capture.admit(Caller::current()).is_err());
    fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap()
        .admit(Caller::current())
        .unwrap();
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
}

#[intent_test_macros::daemon_test]
async fn checkout_capture_refuses_refresh_revision_but_admitted_connection_can_reobserve() {
    use intent_sourcecontrol::gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT};
    let server = Server::new().await;
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
    let capture = fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap();
    let before = capture.original.selected().clone();
    let admitted = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    crate::source_control_auth_ops::repository_owner::secret_reader::tests::refresh_original(
        &fixture.service,
        &server.host,
    )
    .await;
    let current = fixture.request();
    assert_eq!(before.binding, current.binding);
    assert_ne!(before.secret_revision, current.secret_revision);
    assert_eq!(capture.original.selected(), &before);
    assert!(capture.admit(Caller::current()).is_err());
    admitted.with_current(&mut || Ok(())).unwrap();
    fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap()
        .admit(Caller::current())
        .unwrap();
}

#[intent_test_macros::daemon_test]
async fn checkout_capture_does_not_supply_caller_authority() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let capture = fixture
        .service
        .capture_gitlab_checkout_connection()
        .unwrap();
    let caller = Caller::current();
    caller.retire();
    let before = server.control.requests.lock().unwrap().len();
    assert!(capture.admit(caller).is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
}

#[intent_test_macros::daemon_test]
async fn checkout_connection_requires_real_settlement_and_no_default_token_hook() {
    let server = Server::new().await;
    let fixture = Fixture::unadopted(&server).await;
    let caller = Caller::current();
    assert!(fixture
        .service
        .gitlab_checkout_connection(caller.clone())
        .is_err());
    fixture
        .service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let connection = fixture.service.gitlab_checkout_connection(caller).unwrap();
    assert_eq!(connection.instance_base_url(), "https://gitlab.test/forge");
    let before = server.control.requests.lock().unwrap().len();
    assert!(connection
        .token_for(server.descriptor.instance())
        .await
        .is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    let project = connection
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .unwrap();
    assert_eq!(project.owner, "group");
    let mut delivered = 0;
    connection
        .with_project_current("group/project", &mut || {
            delivered += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(delivered, 1);
}

#[intent_test_macros::daemon_test]
async fn checkout_project_denial_survives_new_capture_and_fresh_cache_eligibility() {
    for status in [403, 404] {
        let server = Server::new().await;
        let fixture = Fixture::new(&server).await;
        let caller = Caller::current();
        let first = fixture
            .service
            .gitlab_checkout_connection(caller.clone())
            .unwrap();
        *server.control.project_status.lock().unwrap() = Some(status);
        assert!(first
            .provider()
            .unwrap()
            .get_repo("group", "project")
            .await
            .is_err());
        let second = fixture.service.gitlab_checkout_connection(caller).unwrap();
        let mut delivered = 0;
        assert!(second
            .with_project_current("group/project", &mut || {
                delivered += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(delivered, 0);
        second
            .with_project_current("other/project", &mut || Ok(()))
            .unwrap();
        *server.control.project_status.lock().unwrap() = None;
        fixture
            .service
            .gitlab_connect_pat(server.host.clone(), "pat-second".into())
            .await
            .unwrap();
        assert!(first.with_current(&mut || Ok(())).is_err());
        let recovered = fixture
            .service
            .gitlab_checkout_connection(Caller::current())
            .unwrap();
        recovered
            .with_project_current("group/project", &mut || Ok(()))
            .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_cache_uses_actual_connection_denial_even_before_any_cache_hit() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let connection = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    let root = crate::test_support::test_tempdir("checkout-original-cache");
    let url = "https://gitlab.test/forge/group/project.git";
    let cache = connection.cache(root.path(), url).unwrap();
    assert!(cache.branches().await.unwrap().is_none());
    *server.control.project_status.lock().unwrap() = Some(403);
    assert!(connection
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .is_err());
    let requests = server.control.requests.lock().unwrap().len();
    assert!(cache.branches().await.is_err());
    assert!(connection.cache(root.path(), url).is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
}

#[intent_test_macros::daemon_test]
async fn checkout_current_401_disconnects_but_late_old_401_cannot_reject_replacement() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let first = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    *server.control.pause.lock().unwrap() = Some("/api/v4/projects/");
    let provider = first.provider().unwrap();
    let call = provider.get_repo("group", "project");
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("expected held HTTP: {result:?}"), () = server.entered() => {} }
    fixture
        .service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    server.control.release.notify_one();
    assert!(call.await.is_err());
    let replacement = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    replacement.with_current(&mut || Ok(())).unwrap();
    assert!(replacement
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .is_err());
    assert!(replacement.with_current(&mut || Ok(())).is_err());
}

#[intent_test_macros::daemon_test]
async fn checkout_admitted_success_is_preserved_but_retired_caller_cannot_deliver() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    let caller = Caller::current();
    let connection = fixture
        .service
        .gitlab_checkout_connection(caller.clone())
        .unwrap();
    *server.control.pause.lock().unwrap() = Some("/api/v4/projects/");
    let provider = connection.provider().unwrap();
    let call = provider.get_repo("group", "project");
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("expected held HTTP: {result:?}"), () = server.entered() => {} }
    caller.retire();
    server.control.release.notify_one();
    assert!(
        call.await.is_ok(),
        "actual admitted response stays truthful"
    );
    let mut delivered = 0;
    assert!(connection
        .with_project_current("group/project", &mut || {
            delivered += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(delivered, 0);
    let before = server.control.requests.lock().unwrap().len();
    assert!(provider.list_repos(PageParams::first(20)).await.is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
}

#[intent_test_macros::daemon_test]
async fn checkout_observed_file_change_is_unavailable_after_byte_restoration() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let connection = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    let before = std::fs::read(fixture.service.gitlab_secret_store.path()).unwrap();
    let requests = server.control.requests.lock().unwrap().len();
    fixture
        .service
        .gitlab_secret_store
        .store(
            intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
            "foreign-token",
        )
        .unwrap();
    assert!(connection
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .is_err());
    std::fs::write(fixture.service.gitlab_secret_store.path(), before).unwrap();
    assert!(connection.with_current(&mut || Ok(())).is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
}

#[intent_test_macros::daemon_test]
async fn checkout_native_credential_is_exact_url_original_owner_and_child_independent() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let caller = Caller::current();
    let connection = fixture
        .service
        .gitlab_checkout_connection(caller.clone())
        .unwrap();
    let url = "https://gitlab.test/forge/group/project.git";
    let mut credential = connection.native_credential(url).await.unwrap();
    assert!(credential
        .with_basic_auth(
            "https://gitlab.test/other/group/project.git",
            &mut |_, _| Ok(())
        )
        .is_err());
    assert!(credential.with_basic_auth(url, &mut |_, _| Ok(())).is_ok());
    caller.retire();
    assert!(credential.with_basic_auth(url, &mut |_, _| Ok(())).is_err());
    for foreign in [
        "https://gitlab.test/forgex/group/project.git",
        "https://gitlab.test:8443/forge/group/project.git",
        "https://other.test/forge/group/project.git",
    ] {
        assert!(connection.native_credential(foreign).await.is_err());
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_late_project_denial_does_not_quarantine_a_new_verified_account() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let connection = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    *server.control.project_status.lock().unwrap() = Some(403);
    *server.control.pause.lock().unwrap() = Some("/api/v4/projects/");
    let provider = connection.provider().unwrap();
    let call = provider.get_repo("group", "project");
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("expected held HTTP: {result:?}"), () = server.entered() => {} }
    fixture
        .service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    server.control.release.notify_one();
    assert!(call.await.is_err());
    let replacement = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    replacement
        .with_project_current("group/project", &mut || Ok(()))
        .unwrap();
}

#[intent_test_macros::daemon_test]
async fn checkout_request_view_replaces_authority_and_checks_all_projects_once() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let parent = Caller::current();
    let original = fixture
        .service
        .gitlab_checkout_connection(parent.clone())
        .unwrap();
    let request = Caller::current();
    let view = original.for_request(request.clone());
    assert_eq!(original.connection_key(), view.connection_key());
    assert_eq!(original.cursor_scope, view.cursor_scope);
    parent.retire(); // the supplied request view represents its own complete R fence
    let mut output = 0;
    view.with_projects_current(
        &["other/project".into(), "group/project".into()],
        &mut || {
            output += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(output, 1);
    *server.control.project_status.lock().unwrap() = Some(403);
    assert!(view
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .is_err());
    assert!(view
        .with_projects_current(
            &[
                "other/project".into(),
                "group/project".into(),
                "other/project".into()
            ],
            &mut || {
                output += 1;
                Ok(())
            }
        )
        .is_err());
    assert_eq!(output, 1, "one denied member refuses the complete page");
    request.retire();
    assert!(view.with_current(&mut || Ok(())).is_err());
}

#[intent_test_macros::daemon_test]
async fn checkout_quota_preserves_metadata_but_refuses_http_and_native_dispatch() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let connection = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    *server.control.project_status.lock().unwrap() = Some(429);
    assert!(matches!(
        connection
            .provider()
            .unwrap()
            .get_repo("group", "project")
            .await,
        Err(intent_sourcecontrol::Error::RateLimited { .. })
    ));
    connection
        .with_project_current("group/project", &mut || Ok(()))
        .unwrap();
    let before = server.control.requests.lock().unwrap().len();
    assert!(matches!(
        connection
            .provider()
            .unwrap()
            .get_repo("group", "project")
            .await,
        Err(intent_sourcecontrol::Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert!(connection
        .native_credential("https://gitlab.test/forge/group/project.git")
        .await
        .is_err());
    assert_eq!(server.control.requests.lock().unwrap().len(), before);
}

#[intent_test_macros::daemon_test]
async fn checkout_pending_pat_cancel_is_original_prefix_scoped_and_has_no_write() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    let original = fixture
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let before = std::fs::read(fixture.service.gitlab_secret_store.path()).unwrap();
    *server.control.pause.lock().unwrap() = Some("/api/v4/user");
    let connect = fixture
        .service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into());
    tokio::pin!(connect);
    tokio::select! { result = &mut connect => panic!("expected pending PAT: {result:?}"), () = server.entered() => {} }
    let wrong = intent_sourcecontrol::GitlabHost::parse("https://gitlab.test/another").unwrap();
    assert_eq!(
        fixture.service.gitlab_cancel_auth(&wrong).await.unwrap()["cancelled"],
        false
    );
    assert_eq!(
        fixture
            .service
            .gitlab_cancel_auth(&server.host)
            .await
            .unwrap()["cancelled"],
        true
    );
    server.control.release.notify_one();
    assert!(connect.await.is_err());
    assert_eq!(
        std::fs::read(fixture.service.gitlab_secret_store.path()).unwrap(),
        before
    );
    assert_eq!(
        original.reobserve().unwrap().selected(),
        original.selected()
    );
    assert_eq!(
        fixture
            .service
            .gitlab_cancel_auth(&server.host)
            .await
            .unwrap()["cancelled"],
        false
    );
}

#[intent_test_macros::daemon_test]
async fn checkout_settled_pat_is_not_retroactively_cancelled() {
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    fixture
        .service
        .gitlab_connect_pat(server.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .service
            .gitlab_cancel_auth(&server.host)
            .await
            .unwrap()["cancelled"],
        false
    );
    assert_eq!(
        fixture
            .service
            .gitlab_secret_store
            .load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("pat-second")
    );
    assert_eq!(
        fixture
            .service
            .gitlab_repository_settled_connection()
            .unwrap()
            .selected()
            .binding
            .account
            .account_id,
        "43"
    );
}

#[intent_test_macros::daemon_test]
async fn checkout_known_project_denial_blocks_follow_up_before_http() {
    for status in [403, 404] {
        let server = Server::new().await;
        let fixture = Fixture::new(&server).await;
        *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
        let connection = fixture
            .service
            .gitlab_checkout_connection(Caller::current())
            .unwrap();
        let provider = connection.provider().unwrap();
        *server.control.project_status.lock().unwrap() = Some(status);
        assert!(provider.get_repo("group", "project").await.is_err());
        let requests = server.control.requests.lock().unwrap().len();
        *server.control.project_status.lock().unwrap() = None;
        assert!(
            provider.get_repo("group", "project").await.is_err(),
            "known denial cannot be cleared by a speculative successful read"
        );
        assert!(matches!(
            provider
                .list_remote_branches("group", "project", None, PageParams::first(20))
                .await,
            Err(intent_sourcecontrol::Error::AdmissionUnavailable(
                intent_sourcecontrol::error::AdmissionUnavailable::AuthorityDenied
            ))
        ));
        assert_eq!(
            server.control.requests.lock().unwrap().len(),
            requests,
            "no follow-up provider request after original project denial"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn checkout_project_denial_during_original_read_refuses_final_http_admission() {
    struct HeldRead {
        original: Arc<dyn RepositorySecretReader>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    impl RepositorySecretReader for HeldRead {
        fn load<'a>(
            &'a self,
            expected: &'a RepositorySecretRequest,
        ) -> crate::repository_credentials::authority::CredentialFuture<'a, RepositorySecretSnapshot>
        {
            Box::pin(async move {
                let snapshot = self.original.load(expected).await?;
                self.entered.notify_one();
                self.release.notified().await;
                Ok(snapshot)
            })
        }
    }
    let server = Server::new().await;
    let fixture = Fixture::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    let original = fixture
        .service
        .gitlab_checkout_connection(Caller::current())
        .unwrap();
    let reader = Arc::new(HeldRead {
        original: original.reader.clone(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    // Scheduling wrapper only: it delegates to the actual paired file reader.
    let pending = Arc::new(GitlabCheckoutConnection {
        original: original.original.clone(),
        reader: reader.clone(),
        authority: original.authority.clone(),
        cursor_scope: original.cursor_scope.clone(),
    });
    let provider = pending.provider().unwrap();
    let call = provider.get_repo("group", "project");
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("expected held source read: {result:?}"), () = reader.entered.notified() => {} }
    *server.control.project_status.lock().unwrap() = Some(403);
    assert!(original
        .provider()
        .unwrap()
        .get_repo("group", "project")
        .await
        .is_err());
    let requests = server.control.requests.lock().unwrap().len();
    *server.control.project_status.lock().unwrap() = None;
    reader.release.notify_one();
    assert!(matches!(
        call.await,
        Err(intent_sourcecontrol::Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::AuthorityDenied
        ))
    ));
    assert_eq!(server.control.requests.lock().unwrap().len(), requests);
}
