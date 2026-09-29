//! Actual paired-file/owner/provider tests with explicitly injected caller
//! authority. These do not install a `NativeRead` entry or prove caller policy.
use super::tests::{Fixture, PausedRead, Server};
use super::*;
use crate::repository_credentials::authority::{
    RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::read::{
    RepositoryReadOperation, RepositoryResponseAttribution, RepositoryResponseDisposition as D,
};
use crate::repository_credentials::{RepositoryCredentialAdmission, RepositoryCredentialUse};
use intent_core::{
    ExecutionScope, RepositoryProvider, RepositoryResourceKind, RepositoryTarget, ReviewTarget,
};
use intent_sourcecontrol::{error::ProviderFailureKind, Error as ScError, ProviderAvailability};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::time::timeout;

const BUDGET: Duration = Duration::from_secs(5);
const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";
const PROJECT: &str = "/api/v4/projects/group%2Fproject";
const ISSUE: &str = "/api/v4/projects/group%2Fproject/issues/4";

#[derive(Default)]
struct InjectedAuthority {
    denied: AtomicBool,
    calls: AtomicUsize,
}
impl RepositoryAuthority for InjectedAuthority {
    fn revalidate<'a>(
        &'a self,
        _: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.denied.load(Ordering::SeqCst) {
                return Err(Error::AuthorityDenied);
            }
            Ok(Box::new(InjectedFence) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}
struct InjectedFence;
impl RepositoryAuthorityFence for InjectedFence {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        action()
    }
}

#[derive(Default)]
struct Replies {
    statuses: Mutex<HashMap<String, u16>>,
    pause: Mutex<Option<String>>,
    entered: Notify,
    release: Notify,
    calls: Mutex<Vec<String>>,
}
struct ReadServer {
    fixture: Server,
    replies: Arc<Replies>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ReadServer {
    fn drop(&mut self) {
        self.replies.release.notify_waiters();
        self.task.abort();
    }
}
impl ReadServer {
    async fn new() -> Self {
        let mut fixture = Server::new().await;
        let upstream = fixture
            .host
            .base_url()
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        // Only select the disposable endpoint BEFORE original Services creation.
        // Auth/adoption still forwards to the unchanged original owner fixture.
        fixture.host = intent_sourcecontrol::GitlabHost::parse("gitlab.test")
            .unwrap()
            .with_api_origin(&endpoint)
            .unwrap();
        fixture.descriptor = GitlabDescriptor::with_loopback_endpoint(
            fixture.descriptor.instance().clone(),
            &endpoint,
        )
        .unwrap();
        let replies = Arc::new(Replies::default());
        let control = replies.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let control = control.clone();
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 4096];
                    loop {
                        let Ok(n) = socket.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                        assert!(request.len() < 65_536);
                        if let Some(end) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&request[..end]);
                            let len = header
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if request.len() >= end + 4 + len {
                                break;
                            }
                        }
                    }
                    let text = String::from_utf8_lossy(&request);
                    let path = text.split_whitespace().nth(1).unwrap().to_owned();
                    if !path.starts_with("/api/v4/projects/") {
                        let mut remote = tokio::net::TcpStream::connect(&upstream).await.unwrap();
                        remote.write_all(&request).await.unwrap();
                        let mut response = Vec::new();
                        remote.read_to_end(&mut response).await.unwrap();
                        let _ = socket.write_all(&response).await;
                        return;
                    }
                    assert!(
                        text.contains("stored-pat")
                            || text.contains("rotated")
                            || text.contains("pat-second"),
                        "only real original-owner tokens reach this fixture"
                    );
                    control.calls.lock().unwrap().push(path.clone());
                    let status = *control.statuses.lock().unwrap().get(&path).unwrap_or(&200);
                    let pause = {
                        let mut pause = control.pause.lock().unwrap();
                        if pause.as_ref() == Some(&path) {
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
                    let body=if path==PROJECT {
                        json!({"id":42,"path_with_namespace":"group/project",
                            "only_allow_merge_if_pipeline_succeeds":false,
                            "only_allow_merge_if_all_discussions_are_resolved":false})
                    } else if path.ends_with("/approvals") {
                        json!({"approvals_required":0,"approvals_left":0,"approved_by":[]})
                    } else if path.contains("/discussions") { json!([]) }
                    else if path==MR {
                        json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
                            "title":"actual review","state":"opened","draft":false,"source_branch":"feature","target_branch":"main",
                            "source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
                    } else {
                        json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/issues/4",
                            "title":"actual issue","state":"opened","created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
                    }.to_string();
                    let extra = if status == 429 {
                        "Retry-After: 60\r\n"
                    } else {
                        ""
                    };
                    let response=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRateLimit-Remaining: 23\r\nRateLimit-Reset: 4000000000\r\n{extra}Connection: close\r\n\r\n{body}",body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            fixture,
            replies,
            task,
        }
    }
    fn status(&self, path: &str, status: u16) {
        self.replies
            .statuses
            .lock()
            .unwrap()
            .insert(path.into(), status);
    }
    fn pause(&self, path: &str) {
        *self.replies.pause.lock().unwrap() = Some(path.into());
    }
    async fn entered(&self) {
        timeout(BUDGET, self.replies.entered.notified())
            .await
            .unwrap();
    }
    fn resume(&self) {
        self.replies.release.notify_one();
    }
    fn count(&self) -> usize {
        self.replies.calls.lock().unwrap().len()
    }
}

struct ReadFixture {
    auth: Fixture,
    authority: Arc<InjectedAuthority>,
}
impl ReadFixture {
    async fn new(server: &ReadServer, oauth: bool) -> Self {
        let auth = if oauth {
            let f = Fixture::unadopted(&server.fixture).await;
            f.service
                .gitlab_secret_store
                .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
                .unwrap();
            let expiry = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 7200;
            f.service
                .gitlab_secret_store
                .store(EXPIRES_AT_SECRET_ACCOUNT, &expiry.to_string())
                .unwrap();
            f.service
                .reconcile_gitlab_repository_binding()
                .await
                .unwrap();
            f
        } else {
            Fixture::new(&server.fixture).await
        };
        Self {
            auth,
            authority: Arc::new(InjectedAuthority::default()),
        }
    }
    fn target(&self, kind: RepositoryResourceKind) -> ReviewTarget {
        ReviewTarget {
            repository: RepositoryTarget {
                provider: RepositoryProvider::Gitlab,
                instance_base_url: self.auth.request().binding.account.instance_base_url,
                project_path: "group/project".into(),
            },
            kind,
            number: 4,
        }
    }
    fn admission(&self, server: &ReadServer) -> RepositoryCredentialAdmission {
        let directory = self.auth.service.repository_connection_directory();
        let binding = directory.binding().unwrap();
        directory
            .admit(
                &binding,
                RepositoryAuthorityRequest {
                    execution: ExecutionScope {
                        daemon_id: binding.daemon_id.clone(),
                        authority_scope_id: "injected-reader-test".into(),
                        authority_generation: 1,
                    },
                    connection: binding.scope.clone(),
                    target: self.target(RepositoryResourceKind::MergeRequest).repository,
                    use_kind: RepositoryCredentialUse::NativeRead,
                    allowed_transport: RepositoryCredentialTransport::GitlabApi(
                        server.fixture.descriptor.clone(),
                    ),
                },
                self.authority.clone(),
            )
            .unwrap()
    }
    fn operation(
        &self,
        server: &ReadServer,
        kind: RepositoryResourceKind,
    ) -> (
        RepositoryReadEligibility,
        RepositoryReadOperation,
        ReviewTarget,
    ) {
        let target = self.target(kind);
        let admission = self.admission(server);
        let eligibility = self
            .auth
            .service
            .gitlab_repository_read_eligibility(&admission)
            .unwrap();
        let operation = RepositoryReadOperation::new(
            self.auth.service.repository_connection_directory(),
            admission,
            self.auth.service.gitlab_repository_secret_reader().unwrap(),
            BUDGET,
            target.clone(),
        )
        .unwrap();
        (eligibility, operation, target)
    }
    async fn refresh(&self, server: &ReadServer) {
        self.auth
            .service
            .gitlab_secret_store
            .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
            .unwrap();
        self.auth
            .service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
                host: server.fixture.host.clone(),
            })
            .await
            .unwrap();
    }
}
fn applied(
    eligibility: &RepositoryReadEligibility,
    receipt: &RepositoryResponseAttribution,
    target: &ReviewTarget,
) -> Result<D> {
    let mut result = None;
    eligibility.with_response(receipt, target, &mut |d| {
        assert!(result.replace(d).is_none());
        Ok(())
    })?;
    Ok(result.unwrap())
}
fn denial<T>(result: &intent_sourcecontrol::Result<T>, kind: ProviderFailureKind, status: u16) {
    assert!(matches!(result,Err(ScError::Provider(f)) if f.kind==kind && f.status==Some(status)));
}

#[intent_test_macros::daemon_test]
async fn fixed_reads_are_lazy_and_use_actual_owner_for_all_three_shapes() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
    assert_eq!(s.count(), 0);
    assert_eq!(f.authority.calls.load(Ordering::SeqCst), 0);
    e.check().unwrap();
    let (result, quota, receipt) = op.read_issue().await.into_parts();
    assert_eq!(result.unwrap().title, "actual issue");
    assert_eq!(quota.remaining, Some(23));
    assert_eq!(applied(&e, &receipt, &t).unwrap(), D::NoDenial);
    let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    assert_eq!(
        op.review_details()
            .await
            .into_parts()
            .0
            .unwrap()
            .confirmed_draft,
        Some(false)
    );
    let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    assert_eq!(
        op.review_observation()
            .await
            .into_parts()
            .0
            .unwrap()
            .details
            .review
            .title,
        "actual review"
    );
    assert!(f.authority.calls.load(Ordering::SeqCst) >= 12);
}

#[intent_test_macros::daemon_test]
async fn wrong_method_and_target_never_dispatch_or_invent_denial() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
    let (result, _, a) = op.review_details().await.into_parts();
    assert!(matches!(
        result,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::BoundaryMismatch
        ))
    ));
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    assert_eq!(s.count(), 0);
    let mut target = t.clone();
    target.repository.project_path = "other/project".into();
    assert!(RepositoryReadOperation::new(
        f.auth.service.repository_connection_directory(),
        f.admission(&s),
        f.auth.service.gitlab_repository_secret_reader().unwrap(),
        BUDGET,
        target
    )
    .is_err());
}

#[intent_test_macros::daemon_test]
async fn current_401_retains_raw_error_quota_and_accepted_original_rejection() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 401);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, quota, a) = op.review_details().await.into_parts();
    denial(&result, ProviderFailureKind::CredentialRejected, 401);
    assert_eq!(quota.remaining, Some(23));
    assert!(e.check().is_err());
    assert_eq!(applied(&e, &a, &t).unwrap(), D::AcceptedCredentialRejection);
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn old_401_403_404_after_original_refresh_are_not_applied() {
    for status in [401, 403, 404] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, true).await;
        let original = f.auth.request();
        s.status(MR, status);
        s.pause(MR);
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let call = tokio::spawn(op.review_details());
        s.entered().await;
        f.refresh(&s).await;
        let current = f.auth.request();
        assert_eq!(current.binding, original.binding);
        assert!(current.secret_revision > original.secret_revision);
        s.resume();
        let (result, _, a) = call.await.unwrap().into_parts();
        denial(
            &result,
            if status == 401 {
                ProviderFailureKind::CredentialRejected
            } else {
                ProviderFailureKind::ResourceDenied
            },
            status,
        );
        assert_eq!(applied(&e, &a, &t).unwrap(), D::NotApplied);
        e.check().unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn rejection_during_owned_refresh_is_not_an_obsolete_token_claim() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    s.status(MR, 401);
    s.pause(MR);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let call = tokio::spawn(op.review_details());
    s.entered().await;
    *s.fixture.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    let refresh = f.refresh(&s);
    tokio::pin!(refresh);
    tokio::select! {()=s.fixture.entered()=>{},()=&mut refresh=>panic!("refresh did not pause")}
    assert_eq!(e.check(), Err(Error::Mutating));
    s.resume();
    let (result, _, a) = call.await.unwrap().into_parts();
    denial(&result, ProviderFailureKind::CredentialRejected, 401);
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NotApplied);
    s.fixture.control.release.notify_one();
    refresh.await;
    e.check().unwrap();
}

#[intent_test_macros::daemon_test]
async fn project_and_item_breadth_comes_from_actual_terminal_provider_kind() {
    for (path, kind, disposition) in [
        (
            PROJECT,
            ProviderFailureKind::ProjectDenied,
            D::CurrentProjectDenial,
        ),
        (
            MR,
            ProviderFailureKind::ResourceDenied,
            D::CurrentResourceDenial,
        ),
    ] {
        for status in [403, 404] {
            let s = ReadServer::new().await;
            let f = ReadFixture::new(&s, false).await;
            s.status(path, status);
            let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
            let (result, _, a) = op.review_observation().await.into_parts();
            denial(&result, kind, status);
            assert_eq!(applied(&e, &a, &t).unwrap(), disposition);
            e.check().unwrap();
        }
    }
}

#[intent_test_macros::daemon_test]
async fn same_revision_late_denial_is_not_waived_by_newer_success() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 403);
    s.pause(MR);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let call = tokio::spawn(op.review_details());
    s.entered().await;
    s.status(MR, 200);
    let (_, new, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    new.review_details().await.into_parts().0.unwrap();
    s.resume();
    let (result, _, a) = call.await.unwrap().into_parts();
    denial(&result, ProviderFailureKind::ResourceDenied, 403);
    assert_eq!(applied(&e, &a, &t).unwrap(), D::CurrentResourceDenial);
}

#[intent_test_macros::daemon_test]
async fn early_quota_keeps_partial_data_eligible_but_dispatch_stays_blocked() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(PROJECT, 429);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, quota, a) = op.review_observation().await.into_parts();
    let result = result.unwrap();
    assert_eq!(result.details.review.title, "actual review");
    assert_eq!(
        result.availability.policy,
        ProviderAvailability::RateLimited
    );
    assert_eq!(
        result.availability.approvals,
        ProviderAvailability::RateLimited
    );
    assert_eq!(s.count(), 2);
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    e.check().unwrap();
    let (_, next, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    assert_eq!(s.count(), 2);
    assert!(matches!(
        next.review_details().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    e.with_current(&mut || Ok(())).unwrap();
    assert_eq!(s.count(), 2);
}

#[intent_test_macros::daemon_test]
async fn issue_rejection_is_attributed_and_create_authority_cannot_be_borrowed() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let directory = f.auth.service.repository_connection_directory();
    let binding = f.auth.request().binding;
    let target = f.target(RepositoryResourceKind::Issue);
    let create = directory
        .admit(
            &binding,
            RepositoryAuthorityRequest {
                execution: ExecutionScope {
                    daemon_id: binding.daemon_id.clone(),
                    authority_scope_id: "injected-create-test".into(),
                    authority_generation: 1,
                },
                target: target.repository.clone(),
                connection: binding.scope.clone(),
                use_kind: RepositoryCredentialUse::NativeReviewCreate,
                allowed_transport: RepositoryCredentialTransport::GitlabApi(
                    s.fixture.descriptor.clone(),
                ),
            },
            f.authority.clone(),
        )
        .unwrap();
    assert!(f
        .auth
        .service
        .gitlab_repository_read_eligibility(&create)
        .is_err());
    assert!(RepositoryReadOperation::new(
        directory,
        create,
        f.auth.service.gitlab_repository_secret_reader().unwrap(),
        BUDGET,
        target
    )
    .is_err());
    assert_eq!(s.count(), 0);
    s.status(ISSUE, 401);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
    let (result, _, a) = op.read_issue().await.into_parts();
    denial(&result, ProviderFailureKind::CredentialRejected, 401);
    assert_eq!(applied(&e, &a, &t).unwrap(), D::AcceptedCredentialRejection);
}

#[intent_test_macros::daemon_test]
async fn old_project_denial_after_refresh_has_no_current_project_effect() {
    for status in [403, 404] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, true).await;
        s.status(PROJECT, status);
        s.pause(PROJECT);
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let task = tokio::spawn(op.review_observation());
        s.entered().await;
        f.refresh(&s).await;
        s.resume();
        let (result, _, a) = task.await.unwrap().into_parts();
        denial(&result, ProviderFailureKind::ProjectDenied, status);
        assert_eq!(applied(&e, &a, &t).unwrap(), D::NotApplied);
        e.check().unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn optional_restriction_is_field_local_and_never_primary_denial() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(&format!("{MR}/approvals"), 403);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, _, a) = op.review_observation().await.into_parts();
    assert_eq!(
        result.unwrap().availability.approvals,
        ProviderAvailability::Restricted
    );
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    e.check().unwrap();
}

#[intent_test_macros::daemon_test]
async fn fresh_owner_refresh_preserves_quota_and_only_updates_eligibility_source() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f.auth.request();
    s.status(ISSUE, 429);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
    let (result, quota, a) = op.read_issue().await.into_parts();
    assert!(matches!(result, Err(ScError::RateLimited(_))));
    assert_eq!(quota.remaining, Some(0));
    f.refresh(&s).await;
    let fresh = f.auth.request();
    assert_eq!(fresh.binding, original.binding);
    assert!(fresh.secret_revision > original.secret_revision);
    e.check().unwrap();
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
    assert!(matches!(
        op.read_issue().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn missing_observed_source_prevents_existing_response_application() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 403);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, _, a) = op.review_details().await.into_parts();
    denial(&result, ProviderFailureKind::ResourceDenied, 403);
    f.auth
        .service
        .gitlab_secret_store
        .delete(SECRET_ACCOUNT)
        .unwrap();
    assert_eq!(
        f.auth
            .service
            .gitlab_repository_secret_reader()
            .unwrap()
            .load(&f.auth.request())
            .await
            .unwrap_err(),
        Error::Missing
    );
    let mut called = false;
    assert_eq!(
        e.with_response(&a, &t, &mut |_| {
            called = true;
            Ok(())
        }),
        Err(Error::Unverified)
    );
    assert!(!called);
    f.auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "stored-pat")
        .unwrap();
    assert_eq!(e.check(), Err(Error::Unverified));
}

#[intent_test_macros::daemon_test]
async fn poisoned_owner_proof_preserves_provider_error_without_application() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 404);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, quota, a) = op.review_details().await.into_parts();
    let owner = f
        .auth
        .service
        .gitlab_credential_gate
        .repository
        .get()
        .unwrap()
        .clone();
    let _ = std::thread::spawn(move || {
        let _guard = owner.evidence.published.lock().unwrap();
        panic!("fixture poisons metadata");
    })
    .join();
    assert_eq!(e.check(), Err(Error::Indeterminate));
    let mut called = false;
    assert_eq!(
        e.with_response(&a, &t, &mut |_| {
            called = true;
            Ok(())
        }),
        Err(Error::Indeterminate)
    );
    assert!(!called);
    denial(&result, ProviderFailureKind::ResourceDenied, 404);
    assert_eq!(quota.remaining, Some(23));
}

#[intent_test_macros::daemon_test]
async fn source_invalidation_and_byte_restoration_cannot_restore_eligibility() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    e.check().unwrap();
    f.auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "external-changed")
        .unwrap();
    assert!(matches!(
        op.review_details().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::SecretChanged
        ))
    ));
    assert_eq!(e.check(), Err(Error::Unverified));
    f.auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "stored-pat")
        .unwrap();
    assert_eq!(e.check(), Err(Error::Unverified));
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn replacement_cannot_retarget_eligibility_or_original_response() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 404);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, _, a) = op.review_details().await.into_parts();
    denial(&result, ProviderFailureKind::ResourceDenied, 404);
    f.auth
        .service
        .gitlab_connect_pat(s.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    assert_eq!(e.check(), Err(Error::Retired));
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NotApplied);
    let (fresh, _, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    fresh.check().unwrap();
    assert_eq!(applied(&fresh, &a, &t), Err(Error::BoundaryMismatch));
}

#[intent_test_macros::daemon_test]
async fn separate_call_receipts_cannot_be_borrowed_for_another_target_or_owner() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (_, _, a) = op.review_details().await.into_parts();
    let mut other = t.clone();
    other.number += 1;
    assert_eq!(applied(&e, &a, &other), Err(Error::BoundaryMismatch));
    let independent = ReadFixture::new(&s, false).await;
    let (foreign, _, _) = independent.operation(&s, RepositoryResourceKind::MergeRequest);
    assert_eq!(applied(&foreign, &a, &t), Err(Error::BoundaryMismatch));
    s.status(PROJECT, 403);
    let (full_e, full, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (error, _, full_a) = full.review_observation().await.into_parts();
    denial(&error, ProviderFailureKind::ProjectDenied, 403);
    assert_eq!(
        applied(&full_e, &full_a, &t).unwrap(),
        D::CurrentProjectDenial
    );
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
}

#[intent_test_macros::daemon_test]
async fn guarded_actions_keep_original_metadata_locked_and_preserve_action_error() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    s.status(MR, 403);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (_, _, a) = op.review_details().await.into_parts();
    let owner = f
        .auth
        .service
        .gitlab_credential_gate
        .repository
        .get()
        .unwrap();
    let check = || {
        assert!(owner.settings.get().unwrap().config.try_lock().is_err());
        assert!(owner.descriptor.try_lock().is_err());
        assert!(owner.evidence.published.try_lock().is_err());
        Err(Error::AuthorityDenied)
    };
    assert_eq!(e.with_current(&mut || check()), Err(Error::AuthorityDenied));
    assert_eq!(
        e.with_response(&a, &t, &mut |d| {
            assert_eq!(d, D::CurrentResourceDenial);
            check()
        }),
        Err(Error::AuthorityDenied)
    );
    e.check().unwrap();
}

#[intent_test_macros::daemon_test]
async fn actual_file_read_cancellation_retains_lease_and_never_dispatches() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let mut paused = PausedRead::install(&f.auth);
    let task = tokio::spawn(op.review_details());
    paused.entered().await;
    task.abort();
    assert!(task.await.is_err());
    assert!(f
        .auth
        .service
        .gitlab_credential_gate
        .mutex
        .try_lock()
        .is_err());
    f.auth
        .service
        .repository_connection_directory()
        .retire()
        .unwrap();
    paused.resume();
    drop(
        timeout(BUDGET, f.auth.service.gitlab_credential_gate.lock())
            .await
            .unwrap(),
    );
    assert_eq!(e.check(), Err(Error::Retired));
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn local_authority_refusal_is_not_provider_denial_and_released_result_is_truthful() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    f.authority.denied.store(true, Ordering::SeqCst);
    let (result, _, a) = op.review_details().await.into_parts();
    assert!(matches!(
        result,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::AuthorityDenied
        ))
    ));
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    assert_eq!(s.count(), 0);
    f.authority.denied.store(false, Ordering::SeqCst);
    s.pause(MR);
    let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let task = tokio::spawn(op.review_details());
    s.entered().await;
    f.auth
        .service
        .repository_connection_directory()
        .retire()
        .unwrap();
    s.resume();
    let (result, _, a) = task.await.unwrap().into_parts();
    assert_eq!(result.unwrap().review.title, "actual review");
    assert_eq!(applied(&e, &a, &t).unwrap(), D::NoDenial);
    assert_eq!(e.check(), Err(Error::Retired));
}

// Batch tests retain the real paired owner and file, with caller/target authority
// explicitly injected. They prove metadata admission, not an aggregate entry.
fn batch_scope(f: &ReadFixture, s: &ReadServer, project: &str) -> RepositoryReadEligibility {
    let directory = f.auth.service.repository_connection_directory();
    let binding = f.auth.request().binding;
    let admission = directory
        .admit(
            &binding,
            RepositoryAuthorityRequest {
                execution: ExecutionScope {
                    daemon_id: binding.daemon_id.clone(),
                    authority_scope_id: format!("injected-batch-{project}"),
                    authority_generation: 1,
                },
                connection: binding.scope.clone(),
                target: RepositoryTarget {
                    provider: RepositoryProvider::Gitlab,
                    instance_base_url: binding.account.instance_base_url.clone(),
                    project_path: project.into(),
                },
                use_kind: RepositoryCredentialUse::NativeRead,
                allowed_transport: RepositoryCredentialTransport::GitlabApi(
                    s.fixture.descriptor.clone(),
                ),
            },
            Arc::new(InjectedAuthority::default()),
        )
        .unwrap();
    f.auth
        .service
        .gitlab_repository_read_eligibility(&admission)
        .unwrap()
}

fn batch_refuses(originals: &[&RepositoryReadEligibility], expected: Error) {
    let mut calls = 0;
    assert_eq!(
        RepositoryReadEligibility::with_all_current(originals, || {
            calls += 1;
            Ok(())
        }),
        Err(expected)
    );
    assert_eq!(calls, 0);
}

#[test]
fn batch_empty_set_is_unverified_without_transfer() {
    batch_refuses(&[], Error::Unverified);
}

#[intent_test_macros::daemon_test]
async fn batch_shared_owner_keeps_distinct_scopes_and_duplicate_handles() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    let b = batch_scope(&f, &s, "another/project");
    let output = vec![
        "first private result".to_owned(),
        "second private result".into(),
    ];
    let mut transferred = None;
    RepositoryReadEligibility::with_all_current(&[&a, &b, &a, &b], || {
        assert!(a.owner.settings.get().unwrap().config.try_lock().is_err());
        assert!(a.owner.descriptor.try_lock().is_err());
        assert!(a.owner.evidence.published.try_lock().is_err());
        assert!(transferred.replace(output).is_none());
        Ok(())
    })
    .unwrap();
    assert_eq!(transferred.unwrap().len(), 2);
    a.check().unwrap();
    b.check().unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_stale_member_cannot_hide_behind_fresh_same_directory() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let old = batch_scope(&f, &s, "group/project");
    f.auth
        .service
        .gitlab_connect_pat(s.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let fresh = batch_scope(&f, &s, "group/project");
    assert!(Arc::ptr_eq(&old.owner, &fresh.owner));
    for members in [[&old, &fresh], [&fresh, &old]] {
        batch_refuses(&members, Error::Retired);
    }
    RepositoryReadEligibility::with_all_current(&[&fresh], || Ok(())).unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_shared_directory_does_not_merge_distinct_original_owners() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    // Negative private-association fixture only: both owners were genuinely
    // adopted, but the second owner never attested the first owner's binding.
    let foreign = RepositoryReadEligibility {
        owner: other
            .auth
            .service
            .gitlab_credential_gate
            .repository
            .get()
            .unwrap()
            .clone(),
        scope: RepositoryReadScope::capture(
            f.auth.service.repository_connection_directory(),
            &f.admission(&s),
        )
        .unwrap(),
    };
    assert_eq!(
        f.auth.request().binding.account,
        other.auth.request().binding.account
    );
    for members in [[&a, &foreign], [&foreign, &a]] {
        batch_refuses(&members, Error::SecretMismatch);
    }
    a.check().unwrap();
}

#[intent_test_macros::daemon_test]
async fn batch_requires_each_original_descriptor_and_attestation() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    let b = batch_scope(&other, &s, "group/project");
    // Inject inconsistent owner metadata, never a replacement proof/credential.
    let original = b.owner.descriptor.lock().unwrap().take();
    batch_refuses(&[&a, &b], Error::BoundaryMismatch);
    *b.owner.descriptor.lock().unwrap() = original;
    b.owner.evidence.invalidate().unwrap();
    batch_refuses(&[&b, &a], Error::Unverified);
    a.check().unwrap();
}

#[intent_test_macros::daemon_test]
async fn batch_observed_missing_file_and_restoration_do_not_revive_output() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    let b = batch_scope(&other, &s, "group/project");
    let reader = other
        .auth
        .service
        .gitlab_repository_secret_reader()
        .unwrap();
    let expected = other.auth.request();
    other
        .auth
        .service
        .gitlab_secret_store
        .delete(SECRET_ACCOUNT)
        .unwrap();
    assert_eq!(reader.load(&expected).await.unwrap_err(), Error::Missing);
    batch_refuses(&[&a, &b], Error::Unverified);
    other
        .auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "stored-pat")
        .unwrap();
    batch_refuses(&[&b, &a], Error::Unverified);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_reversed_directories_progress_with_all_owner_locks_held() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let a = Arc::new(batch_scope(&f, &s, "group/project"));
    let b = Arc::new(batch_scope(&other, &s, "group/project"));
    let start = Arc::new(std::sync::Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for pair in [[a.clone(), b.clone()], [b, a]] {
        let start = start.clone();
        let calls = calls.clone();
        tasks.push(tokio::task::spawn_blocking(move || {
            start.wait();
            for _ in 0..4 {
                RepositoryReadEligibility::with_all_current(&[&pair[0], &pair[1]], || {
                    for member in &pair {
                        assert!(member
                            .owner
                            .settings
                            .get()
                            .unwrap()
                            .config
                            .try_lock()
                            .is_err());
                        assert!(member.owner.descriptor.try_lock().is_err());
                        assert!(member.owner.evidence.published.try_lock().is_err());
                    }
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap();
            }
        }));
    }
    for task in tasks {
        timeout(BUDGET, task).await.unwrap().unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_poison_in_any_lock_rank_refuses_all_transfer() {
    for rank in 0..4 {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let other = ReadFixture::new(&s, false).await;
        let good = batch_scope(&f, &s, "group/project");
        let bad = Arc::new(batch_scope(&other, &s, "group/project"));
        let poison = bad.clone();
        assert!(std::thread::spawn(move || match rank {
            0 => {
                let _guard = poison.owner.settings.get().unwrap().config.lock().unwrap();
                panic!("fixture config poison");
            }
            1 => {
                let _guard = poison.owner.descriptor.lock().unwrap();
                panic!("fixture descriptor poison");
            }
            2 => {
                let _ = poison
                    .scope
                    .with_current(&mut |_| panic!("fixture directory poison"));
            }
            _ => {
                let _guard = poison.owner.evidence.published.lock().unwrap();
                panic!("fixture proof poison");
            }
        })
        .join()
        .is_err());
        batch_refuses(&[&good, &bad], Error::Indeterminate);
        good.check().unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn batch_transfer_error_is_once_and_releases_every_guard() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    let mut calls = 0;
    let owned = Box::new("prebuilt private output");
    assert_eq!(
        RepositoryReadEligibility::with_all_current(&[&a, &a], || {
            drop(owned);
            calls += 1;
            Err(Error::TimedOut)
        }),
        Err(Error::TimedOut)
    );
    assert_eq!(calls, 1);
    a.check().unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_admitted_output_survives_later_actual_pat_replacement() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    *s.fixture.control.pause.lock().unwrap() = Some("pat-second");
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let replace =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    s.fixture.entered().await;
    let mut transferred = None;
    RepositoryReadEligibility::with_all_current(&[&e], || {
        transferred = Some("admitted before the pending writer's first effect");
        Ok(())
    })
    .unwrap();
    s.fixture.control.release.notify_one();
    replace.await.unwrap().unwrap();
    assert!(transferred.is_some());
    batch_refuses(&[&e], Error::Retired);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_admission_and_observed_file_invalidation_have_distinct_orders() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
    let expected = f.auth.request();
    let mut paused = PausedRead::install(&f.auth);
    f.auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "external-change")
        .unwrap();
    let read = tokio::spawn(async move { reader.load(&expected).await });
    paused.entered().await;
    // Eligibility is original observed metadata, not an unmanaged disk watcher.
    let mut transferred = false;
    RepositoryReadEligibility::with_all_current(&[&e], || {
        transferred = true;
        Ok(())
    })
    .unwrap();
    paused.resume();
    assert_eq!(read.await.unwrap().unwrap_err(), Error::SecretMismatch);
    batch_refuses(&[&e], Error::Unverified);
    assert!(transferred);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn batch_backoff_refresh_and_other_member_refusal_preserve_actual_quota() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let other = ReadFixture::new(&s, false).await;
    let b = batch_scope(&other, &s, "group/project");
    let (e, op, target) = f.operation(&s, RepositoryResourceKind::Issue);
    let before = f.auth.request();
    s.status(ISSUE, 429);
    let (result, quota, receipt) = op.read_issue().await.into_parts();
    assert!(matches!(&result, Err(ScError::RateLimited(_))));
    assert_eq!(quota.remaining, Some(0));
    RepositoryReadEligibility::with_all_current(&[&e, &b], || Ok(())).unwrap();
    // Server owner instrumentation must track the actual refresh's directory.
    *s.fixture.control.directory.lock().unwrap() =
        Some(f.auth.service.repository_connection_directory());
    f.refresh(&s).await;
    assert_eq!(before.binding, f.auth.request().binding);
    assert!(before.secret_revision < f.auth.request().secret_revision);
    RepositoryReadEligibility::with_all_current(&[&b, &e], || Ok(())).unwrap();
    b.owner.evidence.invalidate().unwrap();
    batch_refuses(&[&e, &b], Error::Unverified);
    assert_eq!(applied(&e, &receipt, &target).unwrap(), D::NoDenial);
    let (_, next, _) = f.operation(&s, RepositoryResourceKind::Issue);
    assert!(matches!(
        next.read_issue().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert!(matches!(&result, Err(ScError::RateLimited(_))));
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn batch_refusal_keeps_current_and_old_401_original_evidence() {
    for old in [false, true] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, old).await;
        let other = ReadFixture::new(&s, false).await;
        let b = batch_scope(&other, &s, "group/project");
        let (e, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        s.status(MR, 401);
        s.pause(MR);
        let call = tokio::spawn(op.review_details());
        s.entered().await;
        if old {
            *s.fixture.control.directory.lock().unwrap() =
                Some(f.auth.service.repository_connection_directory());
            f.refresh(&s).await;
        }
        s.resume();
        let (result, quota, receipt) = call.await.unwrap().into_parts();
        b.owner.evidence.invalidate().unwrap();
        let mut transferred = false;
        assert!(RepositoryReadEligibility::with_all_current(&[&e, &b], || {
            transferred = true;
            Ok(())
        })
        .is_err());
        assert!(!transferred);
        denial(&result, ProviderFailureKind::CredentialRejected, 401);
        assert_eq!(quota.remaining, Some(23));
        assert_eq!(
            applied(&e, &receipt, &target).unwrap(),
            if old {
                D::NotApplied
            } else {
                D::AcceptedCredentialRejection
            }
        );
        assert_eq!(s.count(), 1);
    }
}

#[intent_test_macros::daemon_test]
async fn batch_refusal_keeps_each_project_or_item_denial_separate() {
    for (path, status, kind, disposition) in [
        (
            PROJECT,
            403,
            ProviderFailureKind::ProjectDenied,
            D::CurrentProjectDenial,
        ),
        (
            MR,
            404,
            ProviderFailureKind::ResourceDenied,
            D::CurrentResourceDenial,
        ),
    ] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let other = ReadFixture::new(&s, false).await;
        let b = batch_scope(&other, &s, "group/project");
        let (e, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        s.status(path, status);
        let (result, quota, receipt) = op.review_observation().await.into_parts();
        b.owner.evidence.invalidate().unwrap();
        batch_refuses(&[&e, &b], Error::Unverified);
        denial(&result, kind, status);
        assert_eq!(quota.remaining, Some(23));
        assert_eq!(applied(&e, &receipt, &target).unwrap(), disposition);
        assert_eq!(applied(&b, &receipt, &target), Err(Error::BoundaryMismatch));
    }
}

// Settled facts are observed before any admission. Only the response/quota
// controls below use the existing explicitly injected caller authority.
fn settled_error(result: Result<RepositorySettledConnection>, expected: Error) {
    assert_eq!(result.err(), Some(expected));
}

struct SettledWritePause {
    entered: Arc<Notify>,
    released: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl SettledWritePause {
    fn install(service: &crate::Services, call: usize) -> Self {
        let entered = Arc::new(Notify::new());
        let released = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let signal = entered.clone();
        let held = released.clone();
        let calls = AtomicUsize::new(0);
        service
            .gitlab_credential_gate
            .set_write_probe(Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) + 1 != call {
                    return;
                }
                signal.notify_one();
                let (lock, ready) = &*held;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }));
        Self { entered, released }
    }
    async fn entered(&self) {
        timeout(BUDGET, self.entered.notified()).await.unwrap();
    }
    fn resume(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
impl Drop for SettledWritePause {
    fn drop(&mut self) {
        self.resume();
    }
}

#[intent_test_macros::daemon_test]
async fn settled_factory_requires_installed_paired_and_verified_source() {
    let s = Server::new().await;
    let f = Fixture::unadopted(&s).await;
    settled_error(
        f.service.gitlab_repository_settled_connection(),
        Error::Unverified,
    );
    assert!(s.control.requests.lock().unwrap().is_empty());

    // A separate original Services with an actually unpaired settings store.
    // Equal file contents or a configured descriptor cannot attest that source.
    let registry = Arc::new(crate::SettingsRegistry::load(f.registry.config_path()).unwrap());
    let service = crate::Services::new_repository_fixture(
        f.service.store.clone(),
        f.service.gitlab_secret_store.clone(),
        None,
    )
    .with_settings_registry(registry.clone())
    .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()));
    settled_error(
        service.gitlab_repository_settled_connection(),
        Error::Unverified,
    );
    service
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &service.secrets,
            &service.gitlab_secret_store,
            Some(s.descriptor.clone()),
        )
        .unwrap();
    assert!(service.reconcile_gitlab_repository_binding().await.is_err());
    settled_error(
        service.gitlab_repository_settled_connection(),
        Error::Unverified,
    );
    assert!(s.control.requests.lock().unwrap().is_empty());

    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let facts = f.service.gitlab_repository_settled_connection().unwrap();
    assert_eq!(facts.selected(), &f.request());
    assert_eq!(facts.selected().binding.account.account_id, "42");
}

#[intent_test_macros::daemon_test]
async fn settled_full_descriptor_and_revision_need_no_gate_file_or_http_work() {
    let mut s = Server::new().await;
    s.descriptor = GitlabDescriptor::with_loopback_endpoint(
        intent_sourcecontrol::GitlabInstance::parse("https://gitlab.test:8443/forge/team").unwrap(),
        s.host.base_url(),
    )
    .unwrap();
    // The shared helper fixes a port-free host; this original configuration
    // supplies the same full authority for its explicit port-bearing root.
    let dir = crate::test_support::test_tempdir("settled-port");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.test:8443"),
            ),
            (
                "sourceControl.gitlab.instanceBaseUrl".into(),
                json!(s.descriptor.instance().as_str()),
            ),
            (
                "sourceControl.gitlab.apiBaseUrl".into(),
                json!(s.host.base_url()),
            ),
        ])
        .unwrap();
    let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
    secrets.store(SECRET_ACCOUNT, "stored-pat").unwrap();
    let service = crate::Services::new_repository_fixture(store, secrets, None)
        .with_settings_registry(registry.clone());
    let guard = service.gitlab_credential_gate.lock().await;
    service
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &service.secrets,
            &service.gitlab_secret_store,
            Some(s.descriptor.clone()),
        )
        .unwrap();
    drop(guard);
    service.reconcile_gitlab_repository_binding().await.unwrap();
    let directory = service.repository_connection_directory();
    let selected = directory
        .selected_secret_request(&directory.binding().unwrap())
        .unwrap();
    let requests = s.control.requests.lock().unwrap().len();
    let loads = Arc::new(AtomicUsize::new(0));
    let observed = loads.clone();
    let owner = service.gitlab_credential_gate.repository.get().unwrap();
    *owner.evidence.read_probe.lock().unwrap() = Some(Arc::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    let gate = service.gitlab_credential_gate.lock().await;
    let facts = service.gitlab_repository_settled_connection().unwrap();
    let fresh = facts.reobserve().unwrap();
    assert_eq!(facts.descriptor(), &s.descriptor);
    assert_eq!(fresh.descriptor(), facts.descriptor());
    assert_eq!(facts.selected(), &selected);
    assert_eq!(fresh.selected(), &selected);
    assert_eq!(
        selected.binding.account.instance_base_url,
        "https://gitlab.test:8443/forge/team"
    );
    assert_eq!(selected.binding.account.account_id, "42");
    assert_eq!(selected.binding.scope.account_id, "42");
    assert_eq!(
        selected.source,
        RepositoryCredentialSource::GitlabSecretSlot
    );
    assert!(selected.secret_revision > 0);
    assert!(Arc::ptr_eq(&facts.owner, owner));
    assert!(Arc::ptr_eq(
        &facts.gate.mutex,
        &service.gitlab_credential_gate.mutex
    ));
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    assert_eq!(s.control.requests.lock().unwrap().len(), requests);
    drop(gate);
}

#[intent_test_macros::daemon_test]
async fn settled_pat_reservation_stays_ready_then_actual_effect_retires_old_facts() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let original = f.service.gitlab_repository_settled_connection().unwrap();
    let selected = original.selected().clone();
    *s.control.pause.lock().unwrap() = Some("pat-second");
    let pause = SettledWritePause::install(&f.service, 1);
    let service = f.service.clone();
    let host = s.host.clone();
    let write =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    s.entered().await;
    assert_eq!(original.reobserve().unwrap().selected(), &selected);
    s.control.release.notify_one();
    pause.entered().await;
    settled_error(original.reobserve(), Error::Retired);
    settled_error(
        f.service.gitlab_repository_settled_connection(),
        Error::Mutating,
    );
    pause.resume();
    timeout(BUDGET, write).await.unwrap().unwrap().unwrap();
    settled_error(original.reobserve(), Error::Retired);
    let next = f.service.gitlab_repository_settled_connection().unwrap();
    assert_eq!(next.selected().binding.account.account_id, "43");
    assert_ne!(next.selected().binding.scope, selected.binding.scope);
    assert_eq!(original.selected(), &selected);
}

#[intent_test_macros::daemon_test]
async fn settled_refresh_accepts_new_actual_proof_without_mutating_old_observation() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let selected = original.selected().clone();
    let proof = original
        .owner
        .evidence
        .published
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    *s.fixture.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let refresh = tokio::spawn(async move {
        service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab { host })
            .await
    });
    s.fixture.entered().await;
    settled_error(original.reobserve(), Error::Mutating);
    settled_error(
        f.auth.service.gitlab_repository_settled_connection(),
        Error::Mutating,
    );
    s.fixture.control.release.notify_one();
    timeout(BUDGET, refresh).await.unwrap().unwrap().unwrap();
    let next = original.reobserve().unwrap();
    let fresh_proof = next
        .owner
        .evidence
        .published
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert!(!Arc::ptr_eq(&proof, &fresh_proof));
    assert_eq!(next.selected().binding, selected.binding);
    assert_eq!(next.descriptor(), original.descriptor());
    assert_eq!(next.selected().source, selected.source);
    assert!(next.selected().secret_revision > selected.secret_revision);
    assert_eq!(original.selected(), &selected);
    assert_eq!(proof.request, selected);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn settled_unknown_refresh_and_external_repair_remain_unavailable() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let path = f.auth.service.gitlab_secret_store.path().to_path_buf();
    let saved = std::fs::read(&path).unwrap();
    let broken = path.clone();
    f.auth
        .service
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            std::fs::remove_file(&broken).unwrap();
            std::fs::create_dir(&broken).unwrap();
        }));
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    assert!(f
        .auth
        .service
        .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
            host: s.fixture.host.clone()
        },)
        .await
        .is_err());
    settled_error(original.reobserve(), Error::Indeterminate);
    settled_error(
        f.auth.service.gitlab_repository_settled_connection(),
        Error::Indeterminate,
    );
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(path, saved).unwrap();
    settled_error(original.reobserve(), Error::Indeterminate);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn settled_same_account_reconnect_disconnect_and_retire_cannot_rebind() {
    use intent_core::WorkspaceApi;
    for token in ["pat-first", "pat-second"] {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let old = f.service.gitlab_repository_settled_connection().unwrap();
        f.service
            .gitlab_connect_pat(s.host.clone(), token.into())
            .await
            .unwrap();
        let next = f.service.gitlab_repository_settled_connection().unwrap();
        assert_ne!(old.selected().binding.scope, next.selected().binding.scope);
        assert_eq!(
            next.selected().binding.account.account_id,
            if token == "pat-first" { "42" } else { "43" }
        );
        settled_error(old.reobserve(), Error::Retired);
        f.service
            .settings_reset(SECRET_ACCOUNT.into())
            .await
            .unwrap();
        settled_error(next.reobserve(), Error::Retired);
        settled_error(
            f.service.gitlab_repository_settled_connection(),
            Error::Disconnected,
        );
        f.service
            .repository_connection_directory()
            .retire()
            .unwrap();
        settled_error(
            f.service.gitlab_repository_settled_connection(),
            Error::Retired,
        );
    }
}

#[intent_test_macros::daemon_test]
async fn settled_descriptor_and_source_changes_cannot_select_another_owner() {
    use intent_core::WorkspaceApi;
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let original = f.service.gitlab_repository_settled_connection().unwrap();
    let requests = s.control.requests.lock().unwrap().len();
    f.service
        .settings_update(json!([{
            "path":"sourceControl.gitlab.instanceBaseUrl", "value":"https://gitlab.test/other"
        }]))
        .await
        .unwrap();
    settled_error(original.reobserve(), Error::Retired);
    settled_error(
        f.service.gitlab_repository_settled_connection(),
        Error::Indeterminate,
    );
    assert_eq!(s.control.requests.lock().unwrap().len(), requests);

    let other = Fixture::new(&s).await;
    let captured = other
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let independent =
        (*other.service)
            .clone()
            .with_gitlab_secret_store(FileSecretStore::with_path(
                other
                    .service
                    .gitlab_secret_store
                    .path()
                    .with_file_name("other.json"),
            ));
    settled_error(captured.reobserve(), Error::Retired);
    settled_error(
        independent.gitlab_repository_settled_connection(),
        Error::Retired,
    );
    let fresh = Fixture::new(&s).await;
    let facts = fresh
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    assert_ne!(
        facts.selected().binding.daemon_id,
        captured.selected().binding.daemon_id
    );
    settled_error(captured.reobserve(), Error::Retired);
}

#[intent_test_macros::daemon_test]
async fn settled_real_compensation_requires_a_fresh_connection_observation() {
    use intent_core::WorkspaceApi;
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let original = f.service.gitlab_repository_settled_connection().unwrap();
    let path = f.registry.config_path().to_path_buf();
    let saved = path.with_file_name("config.saved");
    let first = AtomicBool::new(true);
    f.service
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            if first.swap(false, Ordering::SeqCst) {
                std::fs::rename(&path, &saved).unwrap();
                std::fs::create_dir(&path).unwrap();
            }
        }));
    assert!(f
        .service
        .settings_update(json!([
            {"path":SECRET_ACCOUNT,"value":"pat-second"},
            {"path":"sourceControl.gitlab.oauthClientId","value":"new-client"}
        ]))
        .await
        .is_err());
    let restored = f.service.gitlab_repository_settled_connection().unwrap();
    assert_eq!(
        restored.selected().binding.account,
        original.selected().binding.account
    );
    assert_ne!(
        restored.selected().binding.scope,
        original.selected().binding.scope
    );
    assert_eq!(
        f.service
            .gitlab_secret_store
            .load(SECRET_ACCOUNT)
            .unwrap()
            .as_deref(),
        Some("stored-pat")
    );
    settled_error(original.reobserve(), Error::Retired);
}

#[intent_test_macros::daemon_test]
async fn settled_missing_and_changed_proof_are_not_repaired_by_restored_file_bytes() {
    for missing in [false, true] {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let old = f.service.gitlab_repository_settled_connection().unwrap();
        let saved = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
        if missing {
            f.service
                .gitlab_secret_store
                .delete(SECRET_ACCOUNT)
                .unwrap();
        } else {
            f.service
                .gitlab_secret_store
                .store(SECRET_ACCOUNT, "unmanaged-change")
                .unwrap();
        }
        // No file read: unobserved external change is outside metadata knowledge.
        assert_eq!(old.reobserve().unwrap().selected(), old.selected());
        let reader = f.service.gitlab_repository_secret_reader().unwrap();
        assert_eq!(
            reader.load(old.selected()).await.unwrap_err(),
            if missing {
                Error::Missing
            } else {
                Error::SecretMismatch
            }
        );
        settled_error(old.reobserve(), Error::Unverified);
        std::fs::write(f.service.gitlab_secret_store.path(), saved).unwrap();
        settled_error(
            f.service.gitlab_repository_settled_connection(),
            Error::Unverified,
        );
        settled_error(old.reobserve(), Error::Unverified);
    }
}

#[intent_test_macros::daemon_test]
async fn settled_ready_metadata_requires_exact_published_attestation_and_descriptor() {
    for case in 0..5 {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let old = f.service.gitlab_repository_settled_connection().unwrap();
        let owner = &old.owner;
        // Explicit negative metadata faults after real adoption, never producers
        // of positive account, descriptor or source evidence.
        let expected = match case {
            0 => {
                owner.evidence.invalidate().unwrap();
                Error::Unverified
            }
            1 => {
                let mut proof = owner.evidence.published.lock().unwrap();
                let prior = proof.as_ref().unwrap();
                let mut request = prior.request.clone();
                request.secret_revision += 1;
                *proof = Some(Arc::new(AttestedSource {
                    request,
                    descriptor: prior.descriptor.clone(),
                    fingerprint: prior.fingerprint,
                }));
                Error::SecretMismatch
            }
            2 => {
                let mut proof = owner.evidence.published.lock().unwrap();
                let prior = proof.as_ref().unwrap();
                *proof = Some(Arc::new(AttestedSource {
                    request: prior.request.clone(),
                    descriptor: GitlabDescriptor::new(
                        intent_sourcecontrol::GitlabInstance::parse("https://gitlab.test/other")
                            .unwrap(),
                    ),
                    fingerprint: prior.fingerprint,
                }));
                Error::BoundaryMismatch
            }
            3 => {
                // Same logical root, different transport: full descriptor matters.
                let other = Server::new().await;
                *owner.descriptor.lock().unwrap() = Some(other.descriptor.clone());
                Error::BoundaryMismatch
            }
            _ => {
                owner
                    .settings
                    .get()
                    .unwrap()
                    .config
                    .lock()
                    .unwrap()
                    .instance_base_url = Some("https://gitlab.test/other".into());
                Error::BoundaryMismatch
            }
        };
        assert!(
            owner.directory.binding().is_ok(),
            "a Ready directory alone is not source proof"
        );
        settled_error(old.reobserve(), expected);
        settled_error(f.service.gitlab_repository_settled_connection(), expected);
    }
}

#[intent_test_macros::daemon_test]
async fn settled_poison_in_each_metadata_rank_fails_locally() {
    for rank in 0..4 {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let original = f.service.gitlab_repository_settled_connection().unwrap();
        let owner = original.owner.clone();
        assert!(std::thread::spawn(move || match rank {
            0 => {
                let _guard = owner.settings.get().unwrap().config.lock().unwrap();
                panic!("fixture config poison");
            }
            1 => {
                let _guard = owner.descriptor.lock().unwrap();
                panic!("fixture descriptor poison");
            }
            2 => {
                let _ = owner
                    .directory
                    .with_settled_metadata(None, |_, _| -> Result<()> {
                        panic!("fixture directory poison");
                    });
            }
            _ => {
                let _guard = owner.evidence.published.lock().unwrap();
                panic!("fixture proof poison");
            }
        })
        .join()
        .is_err());
        settled_error(original.reobserve(), Error::Indeterminate);
        settled_error(
            f.service.gitlab_repository_settled_connection(),
            Error::Indeterminate,
        );
    }
}

#[intent_test_macros::daemon_test]
async fn settled_early_quota_and_verified_refresh_never_enable_another_dispatch() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    s.status(ISSUE, 429);
    let (e, op, target) = f.operation(&s, RepositoryResourceKind::Issue);
    let (result, quota, receipt) = op.read_issue().await.into_parts();
    assert!(matches!(&result, Err(ScError::RateLimited(_))));
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(
        original.reobserve().unwrap().selected(),
        original.selected()
    );
    f.refresh(&s).await;
    let updated = original.reobserve().unwrap();
    assert_eq!(updated.selected().binding, original.selected().binding);
    assert!(updated.selected().secret_revision > original.selected().secret_revision);
    let (_, next, _) = f.operation(&s, RepositoryResourceKind::Issue);
    assert!(matches!(
        next.read_issue().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(applied(&e, &receipt, &target).unwrap(), D::NoDenial);
    assert_eq!(quota.remaining, Some(0));
    assert!(matches!(&result, Err(ScError::RateLimited(_))));
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn settled_reobservation_never_reattributes_an_old_acquired_401() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let selected = original.selected().clone();
    s.status(MR, 401);
    s.pause(MR);
    let (e, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let call = tokio::spawn(op.review_details());
    s.entered().await;
    f.refresh(&s).await;
    let next = original.reobserve().unwrap();
    s.resume();
    let (result, quota, receipt) = timeout(BUDGET, call).await.unwrap().unwrap().into_parts();
    denial(&result, ProviderFailureKind::CredentialRejected, 401);
    assert_eq!(applied(&e, &receipt, &target).unwrap(), D::NotApplied);
    assert_eq!(quota.remaining, Some(23));
    assert_eq!(original.selected(), &selected);
    assert_eq!(original.reobserve().unwrap().selected(), next.selected());
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn settled_actual_ready_before_proof_publication_does_not_attest_a_snapshot() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    // The first refresh check precedes OAuth on the async caller. Pause the
    // second check inside the actual blocking persistence owner instead.
    let pause = SettledWritePause::install(&f.auth.service, 2);
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let refresh = tokio::spawn(async move {
        service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab { host })
            .await
    });
    pause.entered().await;

    // Hold descriptor while the factory takes config. The real persistence
    // owner can settle its directory, but cannot publish proof through config
    // until that factory has examined the actual Ready/no-proof interval.
    let (locked, ready) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let owner = original.owner.clone();
    let holder = std::thread::spawn(move || {
        let _descriptor = owner.descriptor.lock().unwrap();
        let _ = locked.send(());
        let _ = hold.recv();
    });
    timeout(BUDGET, ready).await.unwrap().unwrap();
    let service = f.auth.service.clone();
    let observation =
        tokio::task::spawn_blocking(move || service.gitlab_repository_settled_connection());
    timeout(BUDGET, async {
        loop {
            let config_held = matches!(
                original.owner.settings.get().unwrap().config.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            if config_held {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pause.resume();
    timeout(BUDGET, async {
        loop {
            match original.owner.directory.binding() {
                Ok(binding) => {
                    assert_eq!(&binding, &original.selected().binding);
                    break;
                }
                Err(Error::Mutating) => tokio::task::yield_now().await,
                other => panic!("unexpected original writer state: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(original.owner.evidence.published.lock().unwrap().is_none());
    drop(release);
    holder.join().unwrap();
    settled_error(
        timeout(BUDGET, observation).await.unwrap().unwrap(),
        Error::Unverified,
    );
    timeout(BUDGET, refresh).await.unwrap().unwrap().unwrap();
    let settled = original.reobserve().unwrap();
    assert!(settled.selected().secret_revision > original.selected().secret_revision);
    assert_eq!(settled.selected().binding, original.selected().binding);
    assert_eq!(s.count(), 0);
}

// New metadata-only projection tests. Positive identities come from original
// owner settlement; the existing provider controls inject caller authority.
use crate::repository_credentials::{
    RepositoryConnectionState as Lifecycle, RepositoryMutationKind as MutationKind,
};

fn connection_facts(service: &crate::Services) -> RepositoryConnectionFacts {
    service.gitlab_repository_connection_facts().unwrap()
}
fn unready(facts: &RepositoryConnectionFacts, reason: Error) {
    assert_eq!(facts.unavailable_reason(), Some(reason));
    assert!(facts.settled().is_none());
    assert!(facts.backoff_until().is_none());
    assert!(facts.child_policy().is_none());
}

#[intent_test_macros::daemon_test]
async fn facts_default_missing_boundary_and_pairing_are_not_secret_absence() {
    let dir = crate::test_support::test_tempdir("connection-facts-default");
    let db = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    // A missing config is materialized as a template with File origins by load.
    // An existing empty document exercises real default-origin settings.
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(config).unwrap());
    let secrets = FileSecretStore::with_path(dir.path().join("missing-secrets.json"));
    let service = crate::Services::new_repository_fixture(db.clone(), secrets.clone(), None)
        .with_settings_registry(registry.clone());
    let missing = connection_facts(&service);
    assert!(missing.attachment() == RepositoryAttachmentState::BoundaryMissing);
    assert!(missing.approval() == RepositoryDescriptorState::Unavailable);
    assert_eq!(missing.lifecycle(), Lifecycle::Unverified);
    unready(&missing, Error::Unverified);
    service
        .gitlab_credential_gate
        .install_settings_boundary(&registry, &service.secrets, &secrets, None)
        .unwrap();
    let defaults = connection_facts(&service);
    assert!(defaults.attachment() == RepositoryAttachmentState::Paired);
    assert!(defaults.approval() == RepositoryDescriptorState::Approved);
    assert_eq!(
        defaults.descriptor().unwrap().instance().as_str(),
        "https://gitlab.com"
    );
    assert_eq!(
        registry.snapshot().origin("sourceControl.gitlab.host"),
        Some(crate::settings_registry::SettingOrigin::Default)
    );
    unready(&defaults, Error::Unverified);
    registry
        .pin(
            "sourceControl.gitlab.host",
            json!("gitlab.com"),
            "--gitlab-host",
        )
        .unwrap();
    assert_eq!(
        registry.snapshot().origin("sourceControl.gitlab.host"),
        Some(crate::settings_registry::SettingOrigin::Flag)
    );
    let pinned = connection_facts(&service);
    assert_eq!(pinned.descriptor(), defaults.descriptor());
    unready(&pinned, Error::Unverified);
    assert!(
        !secrets.path().exists(),
        "projection does not open or create a store"
    );

    let other_registry = Arc::new(crate::SettingsRegistry::load(registry.config_path()).unwrap());
    let other = crate::Services::new_repository_fixture(db, secrets.clone(), None)
        .with_settings_registry(other_registry.clone())
        .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()));
    other
        .gitlab_credential_gate
        .install_settings_boundary(&other_registry, &other.secrets, &secrets, None)
        .unwrap();
    let unpaired = connection_facts(&other);
    assert!(unpaired.attachment() == RepositoryAttachmentState::Unpaired);
    assert!(unpaired.approval() == RepositoryDescriptorState::Approved);
    unready(&unpaired, Error::Unverified);

    // Private negative fixture: production constructors always attach once.
    let mut unattached = service.clone();
    unattached.gitlab_credential_gate = super::super::GitlabCredentialGate::new();
    let facts = connection_facts(&unattached);
    assert!(facts.attachment() == RepositoryAttachmentState::Unattached);
    assert!(facts.descriptor().is_none());
    unready(&facts, Error::Unverified);
}

#[intent_test_macros::daemon_test]
async fn facts_full_descriptor_matches_settled_without_async_gate_or_file_work() {
    let mut s = Server::new().await;
    s.descriptor = GitlabDescriptor::with_loopback_endpoint(
        intent_sourcecontrol::GitlabInstance::parse("https://gitlab.test:8443/forge/team").unwrap(),
        s.host.base_url(),
    )
    .unwrap();
    let dir = crate::test_support::test_tempdir("connection-facts-port");
    let db = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[
            (
                "sourceControl.gitlab.host".into(),
                json!("gitlab.test:8443"),
            ),
            (
                "sourceControl.gitlab.instanceBaseUrl".into(),
                json!(s.descriptor.instance().as_str()),
            ),
            (
                "sourceControl.gitlab.apiBaseUrl".into(),
                json!(s.host.base_url()),
            ),
        ])
        .unwrap();
    let secrets = FileSecretStore::with_path(dir.path().join("secrets.json"));
    secrets.store(SECRET_ACCOUNT, "stored-pat").unwrap();
    let service = crate::Services::new_repository_fixture(db, secrets, None)
        .with_settings_registry(registry.clone());
    service
        .gitlab_credential_gate
        .install_settings_boundary(
            &registry,
            &service.secrets,
            &service.gitlab_secret_store,
            Some(s.descriptor.clone()),
        )
        .unwrap();
    service.reconcile_gitlab_repository_binding().await.unwrap();
    let original = service.gitlab_repository_settled_connection().unwrap();
    let loads = Arc::new(AtomicUsize::new(0));
    let counter = loads.clone();
    *original.owner.evidence.read_probe.lock().unwrap() = Some(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    let requests = s.control.requests.lock().unwrap().len();
    let _gate = service.gitlab_credential_gate.lock().await;
    let facts = connection_facts(&service);
    assert_eq!(facts.descriptor(), Some(&s.descriptor));
    assert_eq!(facts.settled().unwrap().selected(), original.selected());
    assert_eq!(
        facts
            .settled()
            .unwrap()
            .selected()
            .binding
            .account
            .account_id,
        "42"
    );
    assert_eq!(facts.lifecycle(), Lifecycle::Ready);
    assert_eq!(facts.unavailable_reason(), None);
    assert!(!facts.preflight_pending());
    assert_eq!(facts.mutation(), None);
    assert!(matches!(
        facts.child_policy(),
        Some((RepositoryChildPolicyState::Disabled, _))
    ));
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    assert_eq!(s.control.requests.lock().unwrap().len(), requests);
    assert_eq!(
        registry.snapshot().origin("sourceControl.gitlab.host"),
        Some(crate::settings_registry::SettingOrigin::File)
    );
}

#[intent_test_macros::daemon_test]
async fn facts_reservation_then_actual_pat_replace_preserves_old_observation() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let old = connection_facts(&f.service);
    let selected = old.settled().unwrap().selected().clone();
    *s.control.pause.lock().unwrap() = Some("pat-second");
    let pause = SettledWritePause::install(&f.service, 1);
    let service = f.service.clone();
    let host = s.host.clone();
    let write =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    s.entered().await;
    let reserved = connection_facts(&f.service);
    assert!(reserved.preflight_pending());
    assert_eq!(reserved.lifecycle(), Lifecycle::Ready);
    assert_eq!(reserved.mutation(), None);
    assert_eq!(reserved.settled().unwrap().selected(), &selected);
    s.control.release.notify_one();
    pause.entered().await;
    let begun = connection_facts(&f.service);
    assert_eq!(begun.lifecycle(), Lifecycle::Mutating);
    assert_eq!(begun.mutation(), Some(MutationKind::Replace));
    assert!(!begun.preflight_pending());
    unready(&begun, Error::Mutating);
    assert_eq!(begun.descriptor(), Some(&s.descriptor));
    pause.resume();
    timeout(BUDGET, write).await.unwrap().unwrap().unwrap();
    let next = connection_facts(&f.service);
    assert_eq!(
        next.settled()
            .unwrap()
            .selected()
            .binding
            .account
            .account_id,
        "43"
    );
    assert_ne!(next.settled().unwrap().selected().binding, selected.binding);
    assert_eq!(old.settled().unwrap().selected(), &selected);
    settled_error(old.settled().unwrap().reobserve(), Error::Retired);
}

#[intent_test_macros::daemon_test]
async fn facts_oauth_refresh_reports_actual_mutation_and_new_settled_revision() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let old = connection_facts(&f.auth.service);
    let selected = old.settled().unwrap().selected().clone();
    *s.fixture.control.pause.lock().unwrap() = Some("grant_type=refresh_token");
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let refresh = tokio::spawn(async move {
        service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab { host })
            .await
    });
    s.fixture.entered().await;
    let pending = connection_facts(&f.auth.service);
    assert_eq!(pending.mutation(), Some(MutationKind::Refresh));
    assert_eq!(pending.lifecycle(), Lifecycle::Mutating);
    unready(&pending, Error::Mutating);
    s.fixture.control.release.notify_one();
    timeout(BUDGET, refresh).await.unwrap().unwrap().unwrap();
    let fresh = connection_facts(&f.auth.service);
    assert_eq!(
        fresh.settled().unwrap().selected().binding,
        selected.binding
    );
    assert!(fresh.settled().unwrap().selected().secret_revision > selected.secret_revision);
    assert!(fresh.child_policy() == old.child_policy());
    assert_eq!(old.settled().unwrap().selected(), &selected);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn facts_unknown_store_failure_cannot_be_fixed_by_external_byte_restore() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let path = f.auth.service.gitlab_secret_store.path().to_path_buf();
    let saved = std::fs::read(&path).unwrap();
    let broken = path.clone();
    let first = AtomicBool::new(true);
    f.auth
        .service
        .gitlab_credential_gate
        .set_write_probe(Arc::new(move || {
            if first.swap(false, Ordering::SeqCst) {
                std::fs::remove_file(&broken).unwrap();
                std::fs::create_dir(&broken).unwrap();
            }
        }));
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    assert!(f
        .auth
        .service
        .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab {
            host: s.fixture.host.clone()
        })
        .await
        .is_err());
    let unknown = connection_facts(&f.auth.service);
    assert_eq!(unknown.lifecycle(), Lifecycle::Indeterminate);
    assert_eq!(unknown.mutation(), Some(MutationKind::Refresh));
    unready(&unknown, Error::Indeterminate);
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(path, saved).unwrap();
    unready(&connection_facts(&f.auth.service), Error::Indeterminate);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn facts_aborted_pat_keeps_original_blocking_completion_unavailable() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let pause = SettledWritePause::install(&f.service, 1);
    let service = f.service.clone();
    let host = s.host.clone();
    let call =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    pause.entered().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    unready(&connection_facts(&f.service), Error::Mutating);
    pause.resume();
    // The original lease is held until the blocking persistence actually exits.
    let _gate = timeout(BUDGET, f.service.gitlab_credential_gate.lock())
        .await
        .unwrap();
    let unknown = connection_facts(&f.service);
    assert_eq!(unknown.lifecycle(), Lifecycle::Indeterminate);
    unready(&unknown, Error::Indeterminate);
}

#[intent_test_macros::daemon_test]
async fn facts_disconnect_and_source_override_retain_config_not_old_identity() {
    use intent_core::WorkspaceApi;
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let old = connection_facts(&f.service);
    f.service
        .settings_reset(SECRET_ACCOUNT.into())
        .await
        .unwrap();
    let disconnected = connection_facts(&f.service);
    assert_eq!(disconnected.lifecycle(), Lifecycle::Disconnected);
    assert!(disconnected.approval() == RepositoryDescriptorState::Approved);
    assert_eq!(disconnected.descriptor(), Some(&s.descriptor));
    unready(&disconnected, Error::Disconnected);
    settled_error(old.settled().unwrap().reobserve(), Error::Retired);
    let overridden = (*f.service)
        .clone()
        .with_gitlab_secret_store(FileSecretStore::with_path(
            f.service
                .gitlab_secret_store
                .path()
                .with_file_name("other.json"),
        ));
    unready(&connection_facts(&overridden), Error::Retired);
    assert_eq!(connection_facts(&f.service).lifecycle(), Lifecycle::Retired);
}

#[intent_test_macros::daemon_test]
async fn facts_child_policy_is_separate_from_native_settlement() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let original = connection_facts(&f.auth.service);
    let selected = original.settled().unwrap().selected().clone();
    let directory = f.auth.service.repository_connection_directory();
    assert!(matches!(
        original.child_policy(),
        Some((RepositoryChildPolicyState::Disabled, _))
    ));
    let checkpoint = directory
        .child_policy_checkpoint(selected.binding.clone())
        .unwrap();
    let pending = directory.begin_child_policy(&checkpoint).unwrap();
    let snapshot = connection_facts(&f.auth.service);
    assert!(matches!(
        snapshot.child_policy(),
        Some((RepositoryChildPolicyState::Mutating, _))
    ));
    assert_eq!(snapshot.settled().unwrap().selected(), &selected);
    let (eligibility, _, _) = f.operation(&s, RepositoryResourceKind::Issue);
    eligibility.check().unwrap();
    directory.mark_child_policy_indeterminate(&pending).unwrap();
    assert!(matches!(
        connection_facts(&f.auth.service).child_policy(),
        Some((RepositoryChildPolicyState::Indeterminate, _))
    ));
    directory.finish_child_policy(&pending, true).unwrap();
    let enabled = connection_facts(&f.auth.service);
    assert!(matches!(
        enabled.child_policy(),
        Some((RepositoryChildPolicyState::Enabled, _))
    ));
    directory
        .set_child_policy(&selected.binding, false)
        .unwrap();
    let disabled = connection_facts(&f.auth.service);
    assert!(matches!(
        disabled.child_policy(),
        Some((RepositoryChildPolicyState::Disabled, _))
    ));
    assert_ne!(
        disabled.child_policy().unwrap().1,
        enabled.child_policy().unwrap().1
    );
    assert_eq!(disabled.settled().unwrap().selected(), &selected);
    eligibility.check().unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn facts_quota_uses_captured_deadline_not_elapsed_remaining_duration() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    s.status(ISSUE, 429);
    let (eligibility, op, target) = f.operation(&s, RepositoryResourceKind::Issue);
    let (result, quota, receipt) = op.read_issue().await.into_parts();
    assert!(matches!(&result, Err(ScError::RateLimited(_))));
    let original = connection_facts(&f.auth.service);
    let deadline = original.backoff_until().unwrap();
    tokio::task::yield_now().await;
    assert_eq!(
        connection_facts(&f.auth.service).backoff_until(),
        Some(deadline)
    );
    eligibility.check().unwrap();
    f.refresh(&s).await;
    let fresh = connection_facts(&f.auth.service);
    assert_eq!(fresh.backoff_until(), Some(deadline));
    assert_eq!(
        fresh.settled().unwrap().selected().binding,
        original.settled().unwrap().selected().binding
    );
    assert!(
        fresh.settled().unwrap().selected().secret_revision
            > original.settled().unwrap().selected().secret_revision
    );
    let (_, next, _) = f.operation(&s, RepositoryResourceKind::Issue);
    assert!(matches!(
        next.read_issue().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(
        applied(&eligibility, &receipt, &target).unwrap(),
        D::NoDenial
    );
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn facts_current_rejection_is_disconnected_not_a_fabricated_denial_reason() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let old = connection_facts(&f.auth.service);
    s.status(MR, 401);
    let (eligibility, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
    let (result, quota, receipt) = op.review_details().await.into_parts();
    denial(&result, ProviderFailureKind::CredentialRejected, 401);
    assert_eq!(
        applied(&eligibility, &receipt, &target).unwrap(),
        D::AcceptedCredentialRejection
    );
    let facts = connection_facts(&f.auth.service);
    assert_eq!(facts.lifecycle(), Lifecycle::Disconnected);
    unready(&facts, Error::Disconnected);
    assert_eq!(facts.descriptor(), old.descriptor());
    assert_eq!(quota.remaining, Some(23));
    settled_error(old.settled().unwrap().reobserve(), Error::Retired);
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn facts_resource_denials_do_not_erase_current_connection_metadata() {
    for status in [403, 404] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let old = connection_facts(&f.auth.service);
        s.status(MR, status);
        let (eligibility, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let (result, quota, receipt) = op.review_details().await.into_parts();
        assert!(result.is_err());
        assert!(matches!(
            applied(&eligibility, &receipt, &target).unwrap(),
            D::CurrentProjectDenial | D::CurrentResourceDenial
        ));
        let facts = connection_facts(&f.auth.service);
        assert_eq!(
            facts.settled().unwrap().selected(),
            old.settled().unwrap().selected()
        );
        assert_eq!(facts.unavailable_reason(), None);
        assert_eq!(quota.remaining, Some(23));
        assert_eq!(s.count(), 1);
    }
}

#[intent_test_macros::daemon_test]
async fn facts_metadata_faults_and_poison_fail_without_exporting_a_binding() {
    for case in 0..5 {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let original = f.service.gitlab_repository_settled_connection().unwrap();
        let owner = &original.owner;
        let expected = match case {
            0 => {
                owner.evidence.invalidate().unwrap();
                Error::Unverified
            }
            1 => {
                let mut proof = owner.evidence.published.lock().unwrap();
                let old = proof.as_ref().unwrap();
                let mut request = old.request.clone();
                request.secret_revision += 1;
                *proof = Some(Arc::new(AttestedSource {
                    request,
                    descriptor: old.descriptor.clone(),
                    fingerprint: old.fingerprint,
                }));
                Error::SecretMismatch
            }
            2 => {
                *owner.descriptor.lock().unwrap() = Some(GitlabDescriptor::new(
                    intent_sourcecontrol::GitlabInstance::parse("https://gitlab.test/other")
                        .unwrap(),
                ));
                Error::BoundaryMismatch
            }
            3 => {
                owner
                    .settings
                    .get()
                    .unwrap()
                    .config
                    .lock()
                    .unwrap()
                    .instance_base_url = Some("invalid://root".into());
                Error::BoundaryMismatch
            }
            _ => {
                let other = Server::new().await;
                let mut proof = owner.evidence.published.lock().unwrap();
                let old = proof.as_ref().unwrap();
                *proof = Some(Arc::new(AttestedSource {
                    request: old.request.clone(),
                    descriptor: other.descriptor.clone(),
                    fingerprint: old.fingerprint,
                }));
                Error::BoundaryMismatch
            }
        };
        let facts = connection_facts(&f.service);
        assert_eq!(facts.lifecycle(), Lifecycle::Ready);
        unready(&facts, expected);
        if matches!(case, 2 | 3) {
            assert!(facts.approval() == RepositoryDescriptorState::Unapproved);
            assert!(facts.descriptor().is_none());
        }
    }
    for rank in 0..4 {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let owner = f
            .service
            .gitlab_credential_gate
            .repository
            .get()
            .unwrap()
            .clone();
        assert!(std::thread::spawn(move || {
            match rank {
                0 => {
                    let _g = owner.settings.get().unwrap().config.lock().unwrap();
                    panic!("negative config poison");
                }
                1 => {
                    let _g = owner.descriptor.lock().unwrap();
                    panic!("negative descriptor poison");
                }
                2 => {
                    let _ = owner
                        .directory
                        .with_connection_metadata::<()>(|_| panic!("negative directory poison"));
                }
                _ => {
                    let _g = owner.evidence.published.lock().unwrap();
                    panic!("negative proof poison");
                }
            }
        })
        .join()
        .is_err());
        assert_eq!(
            f.service.gitlab_repository_connection_facts().err(),
            Some(Error::Indeterminate)
        );
    }
}

#[intent_test_macros::daemon_test]
async fn facts_observed_source_loss_is_not_repaired_by_restoring_file_bytes() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let old = connection_facts(&f.service);
    let selected = old.settled().unwrap().selected();
    let saved = std::fs::read(f.service.gitlab_secret_store.path()).unwrap();
    f.service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "unmanaged")
        .unwrap();
    assert!(
        connection_facts(&f.service).settled().is_some(),
        "unobserved external change is outside metadata knowledge"
    );
    let reader = f.service.gitlab_repository_secret_reader().unwrap();
    assert_eq!(
        reader.load(selected).await.unwrap_err(),
        Error::SecretMismatch
    );
    unready(&connection_facts(&f.service), Error::Unverified);
    std::fs::write(f.service.gitlab_secret_store.path(), saved).unwrap();
    unready(&connection_facts(&f.service), Error::Unverified);
    assert_eq!(old.settled().unwrap().selected(), selected);
}

#[intent_test_macros::daemon_test]
async fn facts_actual_ready_before_proof_interval_is_not_settled() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = f
        .auth
        .service
        .gitlab_repository_settled_connection()
        .unwrap();
    let pause = SettledWritePause::install(&f.auth.service, 2);
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let refresh = tokio::spawn(async move {
        service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab { host })
            .await
    });
    pause.entered().await;
    let (locked, ready) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let owner = original.owner.clone();
    let holder = std::thread::spawn(move || {
        let _descriptor = owner.descriptor.lock().unwrap();
        let _ = locked.send(());
        let _ = hold.recv();
    });
    timeout(BUDGET, ready).await.unwrap().unwrap();
    let service = f.auth.service.clone();
    let observation = tokio::task::spawn_blocking(move || connection_facts(&service));
    timeout(BUDGET, async {
        loop {
            if matches!(
                original.owner.settings.get().unwrap().config.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pause.resume();
    timeout(BUDGET, async {
        loop {
            match original.owner.directory.binding() {
                Ok(_) => break,
                Err(Error::Mutating) => tokio::task::yield_now().await,
                other => panic!("unexpected state: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(original.owner.evidence.published.lock().unwrap().is_none());
    drop(release);
    holder.join().unwrap();
    let gap = timeout(BUDGET, observation).await.unwrap().unwrap();
    assert_eq!(gap.lifecycle(), Lifecycle::Ready);
    unready(&gap, Error::Unverified);
    timeout(BUDGET, refresh).await.unwrap().unwrap().unwrap();
    let next = connection_facts(&f.auth.service);
    assert!(
        next.settled().unwrap().selected().secret_revision > original.selected().secret_revision
    );
    assert_eq!(
        next.settled().unwrap().selected().binding,
        original.selected().binding
    );
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn facts_disconnect_reservation_and_begun_effect_have_distinct_metadata() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let original = connection_facts(&f.service);
    let guard = f.service.gitlab_credential_gate.lock().await;
    let pause = SettledWritePause::install(&f.service, 1);
    let service = f.service.clone();
    let host = s.host.clone();
    let disconnect = tokio::spawn(async move { service.gitlab_revoke_owned(host).await });
    timeout(BUDGET, async {
        loop {
            let facts = connection_facts(&f.service);
            if facts.preflight_pending() {
                assert_eq!(facts.lifecycle(), Lifecycle::Ready);
                assert_eq!(facts.mutation(), None);
                assert_eq!(
                    facts.settled().unwrap().selected(),
                    original.settled().unwrap().selected()
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(guard);
    pause.entered().await;
    let begun = connection_facts(&f.service);
    assert_eq!(begun.lifecycle(), Lifecycle::Mutating);
    assert_eq!(begun.mutation(), Some(MutationKind::Disconnect));
    unready(&begun, Error::Mutating);
    pause.resume();
    timeout(BUDGET, disconnect).await.unwrap().unwrap().unwrap();
    let final_facts = connection_facts(&f.service);
    assert_eq!(final_facts.lifecycle(), Lifecycle::Disconnected);
    assert_eq!(final_facts.mutation(), None);
    unready(&final_facts, Error::Disconnected);
    assert_eq!(final_facts.descriptor(), original.descriptor());
    settled_error(original.settled().unwrap().reobserve(), Error::Retired);
}

#[intent_test_macros::daemon_test]
async fn facts_same_account_reconnect_and_compensation_keep_old_observation_retired() {
    use intent_core::WorkspaceApi;
    for compensated in [false, true] {
        let s = Server::new().await;
        let f = Fixture::new(&s).await;
        let original = connection_facts(&f.service);
        if compensated {
            let path = f.registry.config_path().to_path_buf();
            let saved = path.with_file_name("config.saved");
            let first = AtomicBool::new(true);
            f.service
                .gitlab_credential_gate
                .set_write_probe(Arc::new(move || {
                    if first.swap(false, Ordering::SeqCst) {
                        std::fs::rename(&path, &saved).unwrap();
                        std::fs::create_dir(&path).unwrap();
                    }
                }));
            assert!(f
                .service
                .settings_update(json!([
                    {"path":SECRET_ACCOUNT,"value":"pat-second"},
                    {"path":"sourceControl.gitlab.oauthClientId","value":"new-client"}
                ]))
                .await
                .is_err());
        } else {
            f.service
                .gitlab_connect_pat(s.host.clone(), "pat-first".into())
                .await
                .unwrap();
        }
        let next = connection_facts(&f.service);
        assert_eq!(next.lifecycle(), Lifecycle::Ready);
        assert_eq!(
            next.settled().unwrap().selected().binding.account,
            original.settled().unwrap().selected().binding.account
        );
        assert_ne!(
            next.settled().unwrap().selected().binding.scope,
            original.settled().unwrap().selected().binding.scope
        );
        assert!(matches!(
            next.child_policy(),
            Some((RepositoryChildPolicyState::Disabled, _))
        ));
        settled_error(original.settled().unwrap().reobserve(), Error::Retired);
    }
}

#[intent_test_macros::daemon_test]
async fn facts_captured_installation_absence_is_refused_after_actual_publication() {
    let dir = crate::test_support::test_tempdir("connection-facts-installation");
    let db = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let secrets = FileSecretStore::with_path(dir.path().join("missing.json"));
    let service = crate::Services::new_repository_fixture(db, secrets.clone(), None)
        .with_settings_registry(registry.clone());
    let gate = &service.gitlab_credential_gate;
    let owner = gate.repository.get().unwrap();
    let original_attachment = owner.settings.get();
    assert!(original_attachment.is_none());
    gate.install_settings_boundary(&registry, &service.secrets, &secrets, None)
        .unwrap();
    let directory = service.repository_connection_directory();
    // Private negative capture fixtures retain absence from before publication.
    // They exercise the same held-metadata phase as the no-argument factory.
    assert_eq!(
        RepositoryConnectionFacts::observe_captured(
            gate,
            &directory,
            Some(owner),
            original_attachment
        )
        .err(),
        Some(Error::Unverified)
    );
    assert_eq!(
        RepositoryConnectionFacts::observe_captured(gate, &directory, None, None).err(),
        Some(Error::Unverified)
    );
    let fresh = connection_facts(&service);
    assert!(fresh.attachment() == RepositoryAttachmentState::Paired);
    assert!(fresh.approval() == RepositoryDescriptorState::Approved);
    unready(&fresh, Error::Unverified);
    assert!(!secrets.path().exists());
}

// Companion tests use the real paired owner and local provider. Caller authority
// remains explicitly injected; optional-only calls do not certify an ACP seal.
fn optional_choice(
    required: &[&RepositoryReadEligibility],
    optional: Option<&RepositoryConnectionFacts>,
) -> Result<bool> {
    let mut choice = None;
    let result = RepositoryReadEligibility::with_all_current_and_facts(required, optional, |v| {
        assert!(choice.replace(v).is_none());
        Ok(())
    });
    match result {
        Ok(()) => Ok(choice.expect("one prebuilt transfer")),
        Err(error) => {
            assert!(choice.is_none(), "required failure cannot transfer");
            Err(error)
        }
    }
}
fn optional_only(facts: Option<&RepositoryConnectionFacts>) -> bool {
    let mut choice = None;
    RepositoryConnectionFacts::with_optional_current(facts, |v| {
        assert!(choice.replace(v).is_none());
        Ok(())
    })
    .unwrap();
    choice.unwrap()
}

struct HeldOutputRank {
    release: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for HeldOutputRank {
    fn drop(&mut self) {
        drop(self.release.take());
        self.worker.take().unwrap().join().unwrap();
    }
}
async fn hold_output_rank(original: Arc<RepositoryReadEligibility>, rank: usize) -> HeldOutputRank {
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let worker = std::thread::spawn(move || {
        let mut entered = Some(entered);
        let mut hold = move || {
            entered.take().unwrap().send(()).unwrap();
            let _ = wait.recv();
        };
        match rank {
            0 => {
                let _guard = original
                    .owner
                    .settings
                    .get()
                    .unwrap()
                    .config
                    .lock()
                    .unwrap();
                hold();
            }
            1 => {
                let _guard = original.owner.descriptor.lock().unwrap();
                hold();
            }
            2 => original
                .scope
                .with_current(&mut |_| {
                    hold();
                    Ok(())
                })
                .unwrap(),
            _ => {
                let _guard = original.owner.evidence.published.lock().unwrap();
                hold();
            }
        }
    });
    let held = HeldOutputRank {
        release: Some(release),
        worker: Some(worker),
    };
    timeout(BUDGET, ready).await.unwrap().unwrap();
    held
}

#[intent_test_macros::daemon_test]
async fn optional_output_empty_and_ordinary_branches_are_distinct() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let facts = connection_facts(&f.auth.service);
    assert_eq!(optional_choice(&[], Some(&facts)), Err(Error::Unverified));
    batch_refuses(&[], Error::Unverified);
    assert!(optional_only(Some(&facts)));
    assert!(!optional_only(None));
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_shared_owner_retains_each_scope_and_all_guards() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let a = batch_scope(&f, &s, "group/project");
    let b = batch_scope(&f, &s, "other/project");
    let facts = connection_facts(&f.auth.service);
    let packet = Box::new("original prebuilt output");
    let mut moved = None;
    RepositoryReadEligibility::with_all_current_and_facts(&[&a, &b, &a], Some(&facts), |include| {
        assert!(include);
        assert!(a.owner.settings.get().unwrap().config.try_lock().is_err());
        assert!(a.owner.descriptor.try_lock().is_err());
        assert!(a.owner.evidence.published.try_lock().is_err());
        assert!(moved.replace(packet).is_none());
        Ok(())
    })
    .unwrap();
    assert!(moved.is_some());
    a.check().unwrap();
    assert!(!optional_choice(&[&b], None).unwrap());
    f.auth
        .service
        .gitlab_connect_pat(s.fixture.host.clone(), "pat-second".into())
        .await
        .unwrap();
    let fresh = batch_scope(&f, &s, "group/project");
    for required in [[&fresh, &b], [&b, &fresh]] {
        assert_eq!(
            optional_choice(&required, Some(&facts)),
            Err(Error::Retired)
        );
    }
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_busy_optional_locks_never_delay_required_at_any_rank() {
    for rank in 0..4 {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let other = ReadFixture::new(&s, false).await;
        let good = Arc::new(batch_scope(&f, &s, "group/project"));
        let optional = Arc::new(batch_scope(&other, &s, "group/project"));
        let facts = Arc::new(connection_facts(&other.auth.service));
        let held = hold_output_rank(optional, rank).await;
        let captured = facts.clone();
        let admission =
            tokio::task::spawn_blocking(move || optional_choice(&[&good], Some(&captured)));
        // Required output must finish BEFORE releasing the additional busy lock.
        let result = timeout(BUDGET, admission).await;
        drop(held);
        assert!(!result.unwrap().unwrap().unwrap(), "rank {rank}");
        assert!(optional_only(Some(&facts)));
        assert_eq!(s.count(), 0);
    }
}

#[intent_test_macros::daemon_test]
async fn optional_output_poison_is_optional_only_until_a_required_member_shares_it() {
    for rank in 0..4 {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let other = ReadFixture::new(&s, false).await;
        let good = batch_scope(&f, &s, "group/project");
        let bad = Arc::new(batch_scope(&other, &s, "group/project"));
        let facts = connection_facts(&other.auth.service);
        let poison = bad.clone();
        assert!(std::thread::spawn(move || match rank {
            0 => {
                let _guard = poison.owner.settings.get().unwrap().config.lock().unwrap();
                panic!("private config fault");
            }
            1 => {
                let _guard = poison.owner.descriptor.lock().unwrap();
                panic!("private descriptor fault");
            }
            2 => {
                let _ = poison
                    .scope
                    .with_current(&mut |_| panic!("private directory fault"));
            }
            _ => {
                let _guard = poison.owner.evidence.published.lock().unwrap();
                panic!("private proof fault");
            }
        })
        .join()
        .is_err());
        assert!(!optional_choice(&[&good], Some(&facts)).unwrap());
        assert!(!optional_only(Some(&facts)));
        for required in [[&good, bad.as_ref()], [bad.as_ref(), &good]] {
            assert_eq!(
                optional_choice(&required, Some(&facts)),
                Err(Error::Indeterminate)
            );
        }
        good.check().unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn optional_output_shared_directory_does_not_merge_foreign_owner_attestation() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let good = batch_scope(&f, &s, "group/project");
    // Private negative association, not an attestation factory: second owner is
    // real but did not settle this directory's binding.
    let foreign = RepositoryReadEligibility {
        owner: other
            .auth
            .service
            .gitlab_credential_gate
            .repository
            .get()
            .unwrap()
            .clone(),
        scope: RepositoryReadScope::capture(
            f.auth.service.repository_connection_directory(),
            &f.admission(&s),
        )
        .unwrap(),
    };
    let facts = connection_facts(&other.auth.service);
    assert_eq!(
        optional_choice(&[&good, &foreign], Some(&facts)),
        Err(Error::SecretMismatch)
    );
    assert_eq!(
        optional_choice(&[&foreign, &good], None),
        Err(Error::SecretMismatch)
    );
    assert!(optional_choice(&[&good], Some(&facts)).unwrap());
}

#[intent_test_macros::daemon_test]
async fn optional_output_reversed_owner_and_directory_orders_make_progress() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let other = ReadFixture::new(&s, false).await;
    let a = Arc::new(batch_scope(&f, &s, "group/project"));
    let b = Arc::new(batch_scope(&other, &s, "group/project"));
    let facts = Arc::new(connection_facts(&f.auth.service));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut work = Vec::new();
    for pair in [[a.clone(), b.clone()], [b, a]] {
        let facts = facts.clone();
        let barrier = barrier.clone();
        work.push(tokio::task::spawn_blocking(move || {
            barrier.wait();
            for _ in 0..4 {
                assert!(optional_choice(&[&pair[0], &pair[1]], Some(&facts)).unwrap());
            }
        }));
    }
    for task in work {
        timeout(BUDGET, task).await.unwrap().unwrap();
    }
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_real_preflight_and_replacement_select_original_or_omit() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let good = batch_scope(&f, &s, "group/project");
    let old = connection_facts(&f.auth.service);
    assert!(optional_choice(&[&good], Some(&old)).unwrap());
    *s.fixture.control.pause.lock().unwrap() = Some("pat-second");
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let writer =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    s.fixture.entered().await;
    assert!(!optional_choice(&[&good], Some(&old)).unwrap());
    let preflight = connection_facts(&f.auth.service);
    assert!(preflight.preflight_pending());
    // This ownership transfer is admitted before the original writer's effect.
    assert!(optional_choice(&[&good], Some(&preflight)).unwrap());
    s.fixture.control.release.notify_one();
    writer.await.unwrap().unwrap();
    assert_eq!(
        optional_choice(&[&good], Some(&preflight)),
        Err(Error::Retired)
    );
    let fresh = batch_scope(&f, &s, "group/project");
    assert!(!optional_choice(&[&fresh], Some(&preflight)).unwrap());
    assert!(optional_choice(&[&fresh], Some(&connection_facts(&f.auth.service))).unwrap());
}

#[intent_test_macros::daemon_test]
async fn optional_output_child_changes_do_not_refuse_native_required_output() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    let old = connection_facts(&f.auth.service);
    let directory = f.auth.service.repository_connection_directory();
    let binding = f.auth.request().binding;
    let checkpoint = directory.child_policy_checkpoint(binding.clone()).unwrap();
    let ticket = directory.begin_child_policy(&checkpoint).unwrap();
    assert!(!optional_choice(&[&e], Some(&old)).unwrap());
    let pending = connection_facts(&f.auth.service);
    assert!(optional_choice(&[&e], Some(&pending)).unwrap());
    directory.mark_child_policy_indeterminate(&ticket).unwrap();
    assert!(!optional_choice(&[&e], Some(&pending)).unwrap());
    directory.finish_child_policy(&ticket, false).unwrap();
    let disabled = connection_facts(&f.auth.service);
    directory.set_child_policy(&binding, false).unwrap();
    assert!(!optional_choice(&[&e], Some(&disabled)).unwrap());
    assert!(optional_choice(&[&e], Some(&connection_facts(&f.auth.service))).unwrap());
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_observed_file_mismatch_has_both_admission_orders() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    let old = connection_facts(&f.auth.service);
    let bytes = std::fs::read(f.auth.service.gitlab_secret_store.path()).unwrap();
    let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
    let expected = f.auth.request();
    let mut pause = PausedRead::install(&f.auth);
    f.auth
        .service
        .gitlab_secret_store
        .store(SECRET_ACCOUNT, "external-change")
        .unwrap();
    let load = tokio::spawn(async move { reader.load(&expected).await });
    pause.entered().await;
    assert!(
        optional_choice(&[&e], Some(&old)).unwrap(),
        "external change not yet observed"
    );
    pause.resume();
    assert_eq!(load.await.unwrap().unwrap_err(), Error::SecretMismatch);
    assert!(!optional_only(Some(&old)));
    assert_eq!(optional_choice(&[&e], Some(&old)), Err(Error::Unverified));
    std::fs::write(f.auth.service.gitlab_secret_store.path(), bytes).unwrap();
    assert!(!optional_only(Some(&old)));
    let unknown = connection_facts(&f.auth.service);
    assert_eq!(unknown.lifecycle(), Lifecycle::Ready);
    assert!(unknown.settled().is_none());
    assert!(
        optional_only(Some(&unknown)),
        "only unknown metadata, no read permission"
    );
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_refresh_preserves_quota_but_omits_old_secret_revision() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let before = connection_facts(&f.auth.service);
    let (e, op, target) = f.operation(&s, RepositoryResourceKind::Issue);
    s.status(ISSUE, 429);
    let (result, quota, receipt) = op.read_issue().await.into_parts();
    assert!(!optional_choice(&[&e], Some(&before)).unwrap());
    let backoff = connection_facts(&f.auth.service);
    tokio::task::yield_now().await;
    assert!(optional_choice(&[&e], Some(&backoff)).unwrap());
    f.refresh(&s).await;
    let refreshed = connection_facts(&f.auth.service);
    assert_eq!(backoff.backoff_until(), refreshed.backoff_until());
    assert_eq!(
        backoff.settled().unwrap().selected().binding,
        refreshed.settled().unwrap().selected().binding
    );
    assert!(!optional_choice(&[&e], Some(&backoff)).unwrap());
    assert!(optional_choice(&[&e], Some(&refreshed)).unwrap());
    let (_, blocked, _) = f.operation(&s, RepositoryResourceKind::Issue);
    assert!(matches!(
        blocked.read_issue().await.into_parts().0,
        Err(ScError::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert!(matches!(result, Err(ScError::RateLimited(_))));
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(applied(&e, &receipt, &target).unwrap(), D::NoDenial);
    assert_eq!(s.count(), 1);
}

#[intent_test_macros::daemon_test]
async fn optional_output_refusal_keeps_current_and_obsolete_401_receipts() {
    for old in [false, true] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, old).await;
        let facts = connection_facts(&f.auth.service);
        let (e, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        s.status(MR, 401);
        s.pause(MR);
        let request = tokio::spawn(op.review_details());
        s.entered().await;
        if old {
            f.refresh(&s).await;
        }
        s.resume();
        let (result, quota, receipt) = request.await.unwrap().into_parts();
        if old {
            assert!(!optional_choice(&[&e], Some(&facts)).unwrap());
        } else {
            assert_eq!(optional_choice(&[&e], Some(&facts)), Err(Error::Retired));
        }
        denial(&result, ProviderFailureKind::CredentialRejected, 401);
        assert_eq!(quota.remaining, Some(23));
        assert_eq!(
            applied(&e, &receipt, &target).unwrap(),
            if old {
                D::NotApplied
            } else {
                D::AcceptedCredentialRejection
            }
        );
        assert_eq!(s.count(), 1);
    }
}

#[intent_test_macros::daemon_test]
async fn optional_output_optional_refusal_keeps_project_and_item_denials() {
    for (path, status, kind, disposition) in [
        (
            PROJECT,
            403,
            ProviderFailureKind::ProjectDenied,
            D::CurrentProjectDenial,
        ),
        (
            MR,
            404,
            ProviderFailureKind::ResourceDenied,
            D::CurrentResourceDenial,
        ),
    ] {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let other = ReadFixture::new(&s, false).await;
        let facts = connection_facts(&other.auth.service);
        let other_e = batch_scope(&other, &s, "group/project");
        let (e, op, target) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        s.status(path, status);
        let (result, quota, receipt) = op.review_observation().await.into_parts();
        other_e.owner.evidence.invalidate().unwrap();
        assert!(!optional_choice(&[&e], Some(&facts)).unwrap());
        assert_eq!(
            optional_choice(&[&e, &other_e], Some(&facts)),
            Err(Error::Unverified)
        );
        denial(&result, kind, status);
        assert_eq!(quota.remaining, Some(23));
        assert_eq!(applied(&e, &receipt, &target).unwrap(), disposition);
    }
}

#[intent_test_macros::daemon_test]
async fn optional_output_installed_unavailable_facts_need_no_async_gate_or_io() {
    let s = Server::new().await;
    let f = Fixture::unadopted(&s).await;
    let unknown = connection_facts(&f.service);
    let gate = f.service.gitlab_credential_gate.lock().await;
    let owner = gate_owner(&f.service);
    *owner.evidence.read_probe.lock().unwrap() =
        Some(Arc::new(|| panic!("metadata must not read a file")));
    assert!(optional_only(Some(&unknown)));
    drop(gate);
    *owner.evidence.read_probe.lock().unwrap() = None;
    f.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    assert!(!optional_only(Some(&unknown)));
    let settled = connection_facts(&f.service);
    f.service.gitlab_revoke_owned(s.host.clone()).await.unwrap();
    assert!(!optional_only(Some(&settled)));
    let disconnected = connection_facts(&f.service);
    assert_eq!(disconnected.lifecycle(), Lifecycle::Disconnected);
    assert!(disconnected.settled().is_none());
    assert!(optional_only(Some(&disconnected)));
}
fn gate_owner(service: &crate::Services) -> Arc<super::super::RepositoryOwner> {
    service
        .gitlab_credential_gate
        .repository
        .get()
        .unwrap()
        .clone()
}

#[intent_test_macros::daemon_test]
async fn optional_output_original_absence_never_recaptures_installed_owner() {
    let dir = crate::test_support::test_tempdir("optional-absent-owner");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let secrets = FileSecretStore::with_path(dir.path().join("missing.json"));
    let mut service = crate::Services::new_repository_fixture(store, secrets.clone(), None)
        .with_settings_registry(registry.clone());
    service.gitlab_credential_gate = super::super::GitlabCredentialGate::new(); // private absence fixture
    let unattached = connection_facts(&service);
    assert!(!optional_only(Some(&unattached)));
    service
        .gitlab_credential_gate
        .attach_repository(service.repository_connection_directory(), None)
        .unwrap();
    let boundary_missing = connection_facts(&service);
    assert!(!optional_only(Some(&boundary_missing)));
    service
        .gitlab_credential_gate
        .install_settings_boundary(&registry, &service.secrets, &secrets, None)
        .unwrap();
    assert!(!optional_only(Some(&unattached)));
    assert!(!optional_only(Some(&boundary_missing)));
    assert!(optional_only(Some(&connection_facts(&service))));
    assert!(!secrets.path().exists());
}

#[intent_test_macros::daemon_test]
async fn optional_output_transfer_error_and_panic_are_never_retried() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = Arc::new(batch_scope(&f, &s, "group/project"));
    let facts = Arc::new(connection_facts(&f.auth.service));
    let mut calls = 0;
    let action: Box<dyn FnOnce(bool) -> Result<()> + Send> = Box::new(|include| {
        assert!(include);
        calls += 1;
        Err(Error::TimedOut)
    });
    assert_eq!(
        RepositoryReadEligibility::with_all_current_and_facts(&[&e], Some(&facts), action),
        Err(Error::TimedOut)
    );
    assert_eq!(calls, 1);
    e.check().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let counted = count.clone();
    let original = e.clone();
    assert!(std::thread::spawn(move || {
        let _ = RepositoryReadEligibility::with_all_current_and_facts(
            &[&original],
            Some(&facts),
            |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                panic!("private transfer panic");
            },
        );
    })
    .join()
    .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(optional_choice(&[&e], None), Err(Error::Indeterminate));
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_settings_and_source_override_do_not_rebind_facts() {
    use intent_core::WorkspaceApi;
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    let old = connection_facts(&f.auth.service);
    f.auth
        .service
        .settings_update(json!([
            {"path":"sourceControl.gitlab.oauthClientId","value":"changed-client"}
        ]))
        .await
        .unwrap();
    assert!(!optional_only(Some(&old)));
    assert_eq!(optional_choice(&[&e], Some(&old)), Err(Error::Retired));
    let current = connection_facts(&f.auth.service);
    assert!(optional_only(Some(&current)));
    let source = FileSecretStore::with_path(
        f.auth
            .service
            .gitlab_secret_store
            .path()
            .with_file_name("override.json"),
    );
    let overridden = (*f.auth.service).clone().with_gitlab_secret_store(source);
    assert!(!optional_only(Some(&current)));
    let retired = connection_facts(&overridden);
    assert_eq!(retired.lifecycle(), Lifecycle::Retired);
    assert!(retired.settled().is_none());
    assert!(
        optional_only(Some(&retired)),
        "original retired metadata, never an identity grant"
    );
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_ready_before_proof_cannot_reuse_old_settled_facts() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = connection_facts(&f.auth.service);
    let e = batch_scope(&f, &s, "group/project");
    let pause = SettledWritePause::install(&f.auth.service, 2);
    f.auth
        .service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    let service = f.auth.service.clone();
    let host = s.fixture.host.clone();
    let refresh = tokio::spawn(async move {
        service
            .stored_proof_token(&crate::source_control_auth_ops::Target::Gitlab { host })
            .await
    });
    pause.entered().await;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let owner = e.owner.clone();
    let holder = std::thread::spawn(move || {
        let _guard = owner.descriptor.lock().unwrap();
        entered.send(()).unwrap();
        let _ = wait.recv();
    });
    timeout(BUDGET, ready).await.unwrap().unwrap();
    let service = f.auth.service.clone();
    let observation = tokio::task::spawn_blocking(move || connection_facts(&service));
    timeout(BUDGET, async {
        loop {
            if matches!(
                e.owner.settings.get().unwrap().config.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pause.resume();
    timeout(BUDGET, async {
        loop {
            match e.owner.directory.binding() {
                Ok(_) => break,
                Err(Error::Mutating) => tokio::task::yield_now().await,
                other => panic!("unexpected state: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(e.owner.evidence.published.lock().unwrap().is_none());
    // Optional-only comparison cannot block on the held config/descriptor.
    assert!(!optional_only(Some(&original)));
    drop(release);
    holder.join().unwrap();
    let gap = timeout(BUDGET, observation).await.unwrap().unwrap();
    assert_eq!(gap.lifecycle(), Lifecycle::Ready);
    assert!(gap.settled().is_none());
    timeout(BUDGET, refresh).await.unwrap().unwrap().unwrap();
    assert!(!optional_choice(&[&e], Some(&gap)).unwrap());
    assert!(!optional_choice(&[&e], Some(&original)).unwrap());
    assert!(optional_choice(&[&e], Some(&connection_facts(&f.auth.service))).unwrap());
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn optional_output_cancelled_auth_retains_actual_unsettled_owner() {
    let s = Server::new().await;
    let f = Fixture::new(&s).await;
    let old = connection_facts(&f.service);
    let pause = SettledWritePause::install(&f.service, 1);
    let service = f.service.clone();
    let host = s.host.clone();
    let call =
        tokio::spawn(async move { service.gitlab_connect_pat(host, "pat-second".into()).await });
    pause.entered().await;
    assert!(!optional_only(Some(&old)));
    let mutating = connection_facts(&f.service);
    assert_eq!(mutating.lifecycle(), Lifecycle::Mutating);
    assert!(optional_only(Some(&mutating)));
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert!(
        optional_only(Some(&mutating)),
        "cancellation is not writer settlement"
    );
    pause.resume();
    let _gate = timeout(BUDGET, f.service.gitlab_credential_gate.lock())
        .await
        .unwrap();
    let unknown = connection_facts(&f.service);
    assert_eq!(unknown.lifecycle(), Lifecycle::Indeterminate);
    assert!(!optional_only(Some(&old)));
    assert!(!optional_only(Some(&mutating)));
    assert!(optional_only(Some(&unknown)));
}

#[intent_test_macros::daemon_test]
async fn optional_output_preflight_boolean_does_not_claim_mutation_history() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let e = batch_scope(&f, &s, "group/project");
    let directory = f.auth.service.repository_connection_directory();
    let ticket = directory.reserve_mutation(MutationKind::Replace).unwrap();
    let facts = connection_facts(&f.auth.service);
    assert!(facts.preflight_pending());
    directory.cancel_reservation(&ticket).unwrap();
    assert!(!optional_choice(&[&e], Some(&facts)).unwrap());
    let next = directory.reserve_mutation(MutationKind::Replace).unwrap();
    assert!(
        optional_choice(&[&e], Some(&facts)).unwrap(),
        "same represented bool, not the same reservation"
    );
    directory.cancel_reservation(&next).unwrap();
    assert_eq!(s.count(), 0);
}

// These P-only calls exercise actual paired-owner metadata. They do not supply
// the separately owned original prompt capture or its pending-ID/one-permit.
#[intent_test_macros::daemon_test]
async fn prompt_facts_current_and_refreshed_revision_select_original_payload() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = connection_facts(&f.auth.service);
    let mut choices = Vec::new();
    let gate = f.auth.service.gitlab_credential_gate.lock().await;
    RepositoryConnectionFacts::with_prompt_current(Some(&original), |include| {
        choices.push(include);
        Ok(())
    })
    .unwrap();
    drop(gate);
    f.refresh(&s).await;
    let refreshed = connection_facts(&f.auth.service);
    assert_eq!(
        original.settled().unwrap().selected().binding,
        refreshed.settled().unwrap().selected().binding
    );
    assert_ne!(
        original.settled().unwrap().selected().secret_revision,
        refreshed.settled().unwrap().selected().secret_revision
    );
    for facts in [&original, &refreshed] {
        RepositoryConnectionFacts::with_prompt_current(Some(facts), |include| {
            choices.push(include);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(choices, [true, false, true]);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn prompt_facts_busy_optional_lock_omits_before_release() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let eligibility = Arc::new(batch_scope(&f, &s, "group/project"));
    let facts = Arc::new(connection_facts(&f.auth.service));
    let held = hold_output_rank(eligibility, 1).await;
    let original = facts.clone();
    let admission = tokio::task::spawn_blocking(move || {
        let mut selected = None;
        RepositoryConnectionFacts::with_prompt_current(Some(&original), |include| {
            assert!(selected.replace(include).is_none());
            Ok(())
        })
        .unwrap();
        selected.unwrap()
    });
    let result = timeout(BUDGET, admission).await;
    drop(held);
    assert!(!result.unwrap().unwrap());
    RepositoryConnectionFacts::with_prompt_current(Some(&facts), |include| {
        assert!(include);
        Ok(())
    })
    .unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn prompt_facts_original_missing_boundary_stays_omitted_after_install() {
    let dir = crate::test_support::test_tempdir("prompt-facts-missing-boundary");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let secrets = FileSecretStore::with_path(dir.path().join("missing.json"));
    let service = crate::Services::new_repository_fixture(store, secrets.clone(), None)
        .with_settings_registry(registry.clone());
    let missing = connection_facts(&service);
    assert!(missing.attachment() == RepositoryAttachmentState::BoundaryMissing);
    let mut choices = Vec::new();
    RepositoryConnectionFacts::with_prompt_current(Some(&missing), |include| {
        choices.push(include);
        Ok(())
    })
    .unwrap();
    service
        .gitlab_credential_gate
        .install_settings_boundary(&registry, &service.secrets, &secrets, None)
        .unwrap();
    for facts in [Some(&missing), None] {
        RepositoryConnectionFacts::with_prompt_current(facts, |include| {
            choices.push(include);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(choices, [false, false, false]);
    assert!(!secrets.path().exists());
}

#[intent_test_macros::daemon_test]
async fn prompt_facts_callback_preserves_original_error_and_owned_effect_once() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let facts = connection_facts(&f.auth.service);
    let packet = Box::new(42_u8);
    let mut output = None;
    let mut calls = 0;
    assert_eq!(
        RepositoryConnectionFacts::with_prompt_current(Some(&facts), |include| {
            assert!(include);
            calls += 1;
            assert!(output.replace(packet).is_none());
            Err(Error::TimedOut)
        }),
        Err(Error::TimedOut)
    );
    assert_eq!(calls, 1);
    assert_eq!(output.as_deref(), Some(&42));
    assert_eq!(s.count(), 0);
}

// Actual owner wrapper controls; only Services/transport tests establish original Wire provenance.
#[intent_test_macros::daemon_test]
async fn native_facts_current_and_refreshed_revision_select_original_payload() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, true).await;
    let original = connection_facts(&f.auth.service);
    let mut choices = Vec::new();
    let gate = f.auth.service.gitlab_credential_gate.lock().await;
    RepositoryConnectionFacts::with_native_context_current(Some(&original), |include| {
        choices.push(include);
        Ok(())
    })
    .unwrap();
    drop(gate);
    f.refresh(&s).await;
    let refreshed = connection_facts(&f.auth.service);
    assert_eq!(
        original.settled().unwrap().selected().binding,
        refreshed.settled().unwrap().selected().binding
    );
    assert_ne!(
        original.settled().unwrap().selected().secret_revision,
        refreshed.settled().unwrap().selected().secret_revision
    );
    for facts in [&original, &refreshed] {
        RepositoryConnectionFacts::with_native_context_current(Some(facts), |include| {
            choices.push(include);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(choices, [true, false, true]);
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn native_facts_busy_optional_lock_omits_before_release() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let eligibility = Arc::new(batch_scope(&f, &s, "group/project"));
    let facts = Arc::new(connection_facts(&f.auth.service));
    let held = hold_output_rank(eligibility, 1).await;
    let original = facts.clone();
    let admission = tokio::task::spawn_blocking(move || {
        let mut selected = None;
        RepositoryConnectionFacts::with_native_context_current(Some(&original), |include| {
            assert!(selected.replace(include).is_none());
            Ok(())
        })
        .unwrap();
        selected.unwrap()
    });
    let result = timeout(BUDGET, admission).await;
    drop(held);
    assert!(!result.unwrap().unwrap());
    RepositoryConnectionFacts::with_native_context_current(Some(&facts), |include| {
        assert!(include);
        Ok(())
    })
    .unwrap();
    assert_eq!(s.count(), 0);
}

#[intent_test_macros::daemon_test]
async fn native_facts_original_missing_boundary_stays_omitted_after_install() {
    let dir = crate::test_support::test_tempdir("native-facts-missing-boundary");
    let store = intent_store::Store::open(&dir.path().join("store.db"))
        .await
        .unwrap();
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    let secrets = FileSecretStore::with_path(dir.path().join("missing.json"));
    let service = crate::Services::new_repository_fixture(store, secrets.clone(), None)
        .with_settings_registry(registry.clone());
    let missing = connection_facts(&service);
    assert!(missing.attachment() == RepositoryAttachmentState::BoundaryMissing);
    let mut choices = Vec::new();
    RepositoryConnectionFacts::with_native_context_current(Some(&missing), |include| {
        choices.push(include);
        Ok(())
    })
    .unwrap();
    service
        .gitlab_credential_gate
        .install_settings_boundary(&registry, &service.secrets, &secrets, None)
        .unwrap();
    for facts in [Some(&missing), None] {
        RepositoryConnectionFacts::with_native_context_current(facts, |include| {
            choices.push(include);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(choices, [false, false, false]);
    assert!(!secrets.path().exists());
}

#[intent_test_macros::daemon_test]
async fn native_facts_callback_preserves_original_error_and_owned_effect_once() {
    let s = ReadServer::new().await;
    let f = ReadFixture::new(&s, false).await;
    let facts = connection_facts(&f.auth.service);
    let packet = Box::new(42_u8);
    let mut output = None;
    let mut calls = 0;
    assert_eq!(
        RepositoryConnectionFacts::with_native_context_current(Some(&facts), |include| {
            assert!(include);
            calls += 1;
            assert!(output.replace(packet).is_none());
            Err(Error::TimedOut)
        }),
        Err(Error::TimedOut)
    );
    assert_eq!(calls, 1);
    assert_eq!(output.as_deref(), Some(&42));
    assert_eq!(s.count(), 0);
}
