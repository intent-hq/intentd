use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::{future::Future, pin::Pin};

use intent_sourcecontrol::model::ProviderAvailability;
use intent_sourcecontrol::{
    error::ProviderFailureKind, gitlab::GitlabCredentialRequest, Error, GitlabInstance,
    GitlabRequestCredentials, NewPullRequest, RepoRef, ReviewBranchIdentity, SourceControl,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::tests_state::*;
use super::*;

struct Reply {
    status: u16,
    body: Value,
    next: Option<u32>,
}
impl Reply {
    fn ok(body: Value) -> Self {
        Self {
            status: 200,
            body,
            next: None,
        }
    }
}
type Handler = Box<dyn Fn(&str, &str) -> Reply + Send + Sync>;
struct Server {
    test: Arc<Test>,
    seen: Arc<Mutex<Vec<(String, String)>>>,
    handler: Arc<Mutex<Handler>>,
    headers: Arc<Mutex<Vec<(String, String)>>>,
    response_pause: Arc<Mutex<Option<Arc<Pause>>>>,
    response_completed: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let descriptor = GitlabDescriptor::with_loopback_endpoint(
            GitlabInstance::parse(INSTANCE).unwrap(),
            &format!("http://{}/fixture", listener.local_addr().unwrap()),
        )
        .unwrap();
        let test = Arc::new(Test::with_descriptor(descriptor));
        let seen = Arc::new(Mutex::new(vec![]));
        let handler: Arc<Mutex<Handler>> =
            Arc::new(Mutex::new(Box::new(|_, _| Reply::ok(json!([])))));
        let observed = seen.clone();
        let respond = handler.clone();
        let headers = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let response_headers = headers.clone();
        let response_pause = Arc::new(Mutex::new(None::<Arc<Pause>>));
        let paused = response_pause.clone();
        let response_completed = Arc::new(tokio::sync::Notify::new());
        let completed = response_completed.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let observed = observed.clone();
                let respond = respond.clone();
                let response_headers = response_headers.clone();
                let pause = paused.lock().unwrap().clone();
                let completed = completed.clone();
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut chunk = [0; 4096];
                    let (header_end, size) = loop {
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..n]);
                        if let Some(i) = buffer.windows(4).position(|v| v == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buffer[..i]);
                            let size = head
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|s| s.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            break (i + 4, size);
                        }
                    };
                    while buffer.len() < header_end + size {
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..n]);
                    }
                    let head = String::from_utf8_lossy(&buffer[..header_end]);
                    let line = head.lines().next().unwrap().to_owned();
                    let mut words = line.split_whitespace();
                    let method = words.next().unwrap();
                    let path = words.next().unwrap();
                    observed
                        .lock()
                        .unwrap()
                        .push((line.clone(), head.to_string()));
                    let reply = respond.lock().unwrap()(method, path);
                    if let Some(pause) = pause {
                        pause.entered.notify_one();
                        pause.release.acquire().await.unwrap().forget();
                    }
                    let body = reply.body.to_string();
                    let next = reply
                        .next
                        .map_or_else(String::new, |page| format!("x-next-page: {page}\r\n"));
                    let mut extra = String::new();
                    for (key, value) in response_headers.lock().unwrap().iter() {
                        write!(&mut extra, "{key}: {value}\r\n").unwrap();
                    }
                    let response = format!("HTTP/1.1 {} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{next}{extra}connection: close\r\n\r\n{body}", reply.status, body.len());
                    // Cancellation can close an already admitted socket.
                    let _ = socket.write_all(response.as_bytes()).await;
                    completed.notify_one();
                });
            }
        });
        Self {
            test,
            seen,
            handler,
            headers,
            response_pause,
            response_completed,
            task,
        }
    }
    fn provider(
        &self,
        use_kind: RepositoryCredentialUse,
    ) -> intent_sourcecontrol::GitLabSourceControl {
        BoundGitlabRequestCredentials::new(
            self.test.directory.clone(),
            self.test.admit(use_kind),
            self.test.secrets.clone(),
            Duration::from_secs(2),
        )
        .unwrap()
        .into_provider()
        .unwrap()
    }
}
fn repo() -> RepoRef {
    RepoRef {
        owner: "team/sub".into(),
        name: "project".into(),
    }
}
fn mr() -> Value {
    json!({"iid":4,"web_url":format!("{INSTANCE}/{PROJECT}/-/merge_requests/4"),
        "title":"actual title","state":"opened","draft":false,"source_branch":"feature","target_branch":"main",
        "source_project_id":41,"target_project_id":41,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
}
fn project() -> Value {
    json!({"id":41,"path_with_namespace":PROJECT,"only_allow_merge_if_pipeline_succeeds":false,"only_allow_merge_if_all_discussions_are_resolved":false})
}
fn pair(branch: &str) -> ReviewBranchIdentity {
    ReviewBranchIdentity {
        instance_base_url: INSTANCE.into(),
        project_id: 41,
        project_path: Some(PROJECT.into()),
        branch: branch.into(),
    }
}

// These fixtures use an injected authority to schedule the real managed callback
// after token release. They do not exercise concrete Store/Git authority.
#[derive(Clone, Copy)]
enum AfterRelease {
    Retire,
    Refresh,
    Replace,
}
struct ReleaseAuthority {
    test: Arc<Test>,
    action: AfterRelease,
    calls: std::sync::atomic::AtomicUsize,
}
struct ReleaseFence {
    inner: Box<dyn super::authority::RepositoryAuthorityFence>,
    test: Arc<Test>,
    after: Option<AfterRelease>,
}
impl super::authority::RepositoryAuthorityFence for ReleaseFence {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.inner.dispatch(action)?;
        match self.after {
            Some(AfterRelease::Retire) => self.test.directory.retire()?,
            Some(AfterRelease::Refresh) => self.test.refresh("token-new"),
            Some(AfterRelease::Replace) => {
                self.test.replace(self.test.verified.clone());
            }
            None => {}
        }
        Ok(())
    }
}
impl RepositoryAuthority for ReleaseAuthority {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> super::authority::CredentialFuture<'a, Box<dyn super::authority::RepositoryAuthorityFence>>
    {
        Box::pin(async move {
            let inner = self.test.authority.revalidate(request).await?;
            Ok(Box::new(ReleaseFence {
                inner,
                test: self.test.clone(),
                after: (self.calls.fetch_add(1, Ordering::SeqCst) == 0).then_some(self.action),
            })
                as Box<dyn super::authority::RepositoryAuthorityFence>)
        })
    }
}
async fn refuses_after_token_release(action: AfterRelease) {
    let server = Server::new().await;
    let authority = Arc::new(ReleaseAuthority {
        test: server.test.clone(),
        action,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let admission = server
        .test
        .directory
        .admit(
            &server.test.directory.binding().unwrap(),
            server
                .test
                .request(RepositoryCredentialUse::NativeReviewCreate),
            authority,
        )
        .unwrap();
    let provider = BoundGitlabRequestCredentials::new(
        server.test.directory.clone(),
        admission,
        server.test.secrets.clone(),
        Duration::from_secs(2),
    )
    .unwrap()
    .into_provider()
    .unwrap();
    let result = provider.list_comments(&repo(), 4).await;
    assert!(
        matches!(
            result,
            Err(Error::AdmissionRetired | Error::AdmissionUnavailable(_))
        ),
        "stale preparation reached HTTP: {result:?}"
    );
    assert!(server.seen.lock().unwrap().is_empty());
    assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn http_admission_retirement_after_token_release_sends_nothing() {
    refuses_after_token_release(AfterRelease::Retire).await;
}
#[tokio::test]
async fn http_admission_refresh_after_token_release_cannot_send_old_secret() {
    refuses_after_token_release(AfterRelease::Refresh).await;
}
#[tokio::test]
async fn http_admission_replacement_after_token_release_cannot_rebind() {
    refuses_after_token_release(AfterRelease::Replace).await;
}

#[tokio::test]
async fn bound_callback_reacquires_fresh_token_and_original_authority_on_each_page() {
    let server = Server::new().await;
    let test = server.test.clone();
    *server.handler.lock().unwrap() = Box::new(move |_, path| {
        let second = path.contains("page=2");
        if !second {
            test.refresh("token-new");
        }
        Reply {
            status: 200,
            body: json!([]),
            next: (!second).then_some(2),
        }
    });
    server
        .provider(RepositoryCredentialUse::NativeRead)
        .list_comments(&repo(), 4)
        .await
        .unwrap();
    let seen = server.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen[0].1.contains("Bearer token-old"));
    assert!(seen[1].1.contains("Bearer token-new"));
    assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 2);
    let requests = server.test.authority.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| request == &requests[0]));
}

#[tokio::test]
async fn bound_callback_stops_next_page_when_original_authority_retires() {
    let server = Server::new().await;
    let test = server.test.clone();
    *server.handler.lock().unwrap() = Box::new(move |_, _| {
        *test.authority.revision.lock().unwrap() += 1;
        Reply {
            status: 200,
            body: json!([]),
            next: Some(2),
        }
    });
    let error = server
        .provider(RepositoryCredentialUse::NativeRead)
        .list_comments(&repo(), 4)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AdmissionRetired));
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn optional_requests_propagate_local_retirement_and_actual_credential_denial_separately() {
    for retire in [false, true] {
        let server = Server::new().await;
        let test = server.test.clone();
        *server.handler.lock().unwrap() = Box::new(move |_, path| {
            if path.ends_with("/merge_requests/4") {
                return Reply::ok(mr());
            }
            if path.ends_with("team%2Fsub%2Fproject") {
                if retire {
                    test.directory.retire().unwrap();
                }
                return Reply::ok(project());
            }
            Reply {
                status: 401,
                body: json!({}),
                next: None,
            }
        });
        let error = server
            .provider(RepositoryCredentialUse::NativeRead)
            .observe_review(&repo(), 4)
            .await
            .unwrap_err();
        if retire {
            assert!(matches!(error, Error::AdmissionRetired));
            assert_eq!(server.seen.lock().unwrap().len(), 2);
        } else {
            assert!(
                matches!(error, Error::Provider(p) if p.kind == ProviderFailureKind::CredentialRejected)
            );
            assert_eq!(server.seen.lock().unwrap().len(), 3);
            // The original response receipt fences eligibility; it never deletes
            // the stored credential or claims the auth owner has logged out.
            assert_eq!(
                server.test.directory.binding(),
                Err(RepositoryCredentialError::Disconnected)
            );
            assert_eq!(*server.test.secrets.value.lock().unwrap(), "token-old");
        }
    }
}

#[tokio::test]
async fn callback_rejects_foreign_instance_project_transport_or_write_under_read_grant() {
    let test = Test::new();
    let callback = BoundGitlabRequestCredentials::new(
        test.directory.clone(),
        test.admit(RepositoryCredentialUse::NativeRead),
        test.secrets.clone(),
        Duration::from_secs(1),
    )
    .unwrap();
    let descriptor = test.verified.descriptor.clone();
    let foreign = GitlabInstance::parse("https://git.example:8443/elsewhere").unwrap();
    let redirected = GitlabDescriptor::with_loopback_endpoint(
        descriptor.instance().clone(),
        "http://127.0.0.1:1",
    )
    .unwrap();
    for (instance, transport, path, writing) in [
        (
            &foreign,
            &descriptor,
            "projects/team%2Fsub%2Fproject",
            false,
        ),
        (
            descriptor.instance(),
            &redirected,
            "projects/team%2Fsub%2Fproject",
            false,
        ),
        (
            descriptor.instance(),
            &descriptor,
            "projects/foreign%2Frepo",
            false,
        ),
        (
            descriptor.instance(),
            &descriptor,
            "projects/team%2Fsub%2Fproject/merge_requests",
            true,
        ),
    ] {
        let error = callback
            .token_for_request(
                instance,
                GitlabCredentialRequest::direct(transport, path, writing),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::AdmissionUnavailable(_)));
    }
    assert!(matches!(
        callback.token_for(descriptor.instance()).await.unwrap_err(),
        Error::AdmissionUnavailable(_)
    ));
    assert_eq!(test.secrets.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bound_create_keeps_uncertain_sent_write_after_retirement_without_retry() {
    let server = Server::new().await;
    let test = server.test.clone();
    *server.handler.lock().unwrap() = Box::new(move |method, path| {
        if method == "POST" {
            test.directory.retire().unwrap();
            return Reply {
                status: 503,
                body: json!({}),
                next: None,
            };
        }
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply::ok(project());
        }
        if path.contains("/merge_requests?") {
            return Reply::ok(json!([]));
        }
        if path.contains("/repository/branches/") {
            return Reply::ok(
                json!({"name":path.rsplit('/').next().unwrap(),"commit":{"id":"actual-sha"}}),
            );
        }
        Reply {
            status: 404,
            body: json!({}),
            next: None,
        }
    });
    let input = NewPullRequest {
        title: "sent title".into(),
        body: Some("sent body".into()),
        source_branch: "feature".into(),
        target_branch: "main".into(),
        draft: false,
    };
    let error = server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .create_same_project(&repo(), input, &pair("feature"), &pair("main"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Provider(p) if p.kind == ProviderFailureKind::WriteUncertain),
        "{error:?}"
    );
    assert_eq!(
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(line, _)| line.starts_with("POST "))
            .count(),
        1
    );
}

#[tokio::test]
async fn bound_fork_review_allows_corroborated_pipeline_project_follow_up() {
    let server = Server::new().await;
    *server.handler.lock().unwrap() = Box::new(|_, path| {
        if path.ends_with("/merge_requests/4") {
            let mut body = mr();
            body["source_project_id"] = json!(82);
            body["head_pipeline"] = json!({"id":9,"project_id":82,"status":"success"});
            Reply::ok(body)
        } else if path.ends_with("team%2Fsub%2Fproject") {
            Reply::ok(project())
        } else if path.ends_with("/approvals") {
            Reply::ok(json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}))
        } else {
            Reply::ok(json!([]))
        }
    });
    let observation = server
        .provider(RepositoryCredentialUse::NativeRead)
        .observe_review(&repo(), 4)
        .await
        .unwrap();
    assert_eq!(observation.details.source.unwrap().project_id, 82);
    assert!(server
        .seen
        .lock()
        .unwrap()
        .iter()
        .any(|(line, _)| line.contains("projects/82/pipelines/9/jobs")));
}

#[tokio::test]
async fn bound_pipeline_rejects_unrelated_or_unconfirmed_provenance_without_follow_up() {
    for case in 0..12 {
        let server = Server::new().await;
        *server.handler.lock().unwrap() = Box::new(move |_, path| {
            if path.ends_with("/merge_requests/4") {
                let mut body = mr();
                body["source_project_id"] = json!(82);
                body["head_pipeline"] = json!({"id":9,"project_id":82,"status":"success"});
                match case {
                    0 => body["head_pipeline"]["project_id"] = json!(99),
                    1 => body["source_project_id"] = Value::Null,
                    2 => body["target_project_id"] = Value::Null,
                    3 => body["target_project_id"] = json!(99),
                    4 => body["iid"] = json!(5),
                    5 => body["head_pipeline"]["project_id"] = Value::Null,
                    6 => body["head_pipeline"]["project_id"] = json!("82"),
                    7 => body["project_id"] = json!(99),
                    8 => body["head_pipeline"]["id"] = json!(0),
                    _ => {}
                }
                return Reply::ok(body);
            }
            if path.ends_with("team%2Fsub%2Fproject") {
                let mut body = project();
                match case {
                    9 => body["path_with_namespace"] = json!("elsewhere/project"),
                    10 => body["id"] = json!(99),
                    11 => body["id"] = Value::Null,
                    _ => {}
                }
                return Reply::ok(body);
            }
            if path.ends_with("/approvals") {
                return Reply::ok(
                    json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}),
                );
            }
            Reply::ok(json!([]))
        });
        let result = server
            .provider(RepositoryCredentialUse::NativeRead)
            .observe_review(&repo(), 4)
            .await;
        let seen = server.seen.lock().unwrap();
        assert!(
            !seen.iter().any(|(line, _)| line.contains("/jobs")),
            "case {case} sent an unconfirmed follow-up"
        );
        assert_eq!(
            server.test.secrets.calls.load(Ordering::SeqCst),
            seen.len(),
            "case {case} released a token without a request"
        );
        match result {
            Err(Error::AdmissionUnavailable(_)) => assert_eq!(seen.len(), 3, "case {case}"),
            Ok(observation) => assert_eq!(
                observation.availability.checks,
                intent_sourcecontrol::model::ProviderAvailability::Unknown,
                "case {case}"
            ),
            other => panic!("unexpected provenance outcome for case {case}: {other:?}"),
        }
    }
}

#[derive(Clone, Copy)]
enum AlterRequest {
    BranchProject,
    BranchName,
    BranchWrite,
    BranchEndpoint,
    CreateProject,
    PipelineProject,
    PipelineId,
    PipelineWrite,
    PipelineProof,
}
struct AlteredCredentials {
    bound: BoundGitlabRequestCredentials,
    alter: AlterRequest,
}
impl GitlabRequestCredentials for AlteredCredentials {
    fn token_for<'s, 'i, 'f>(
        &'s self,
        instance: &'i GitlabInstance,
    ) -> Pin<Box<dyn Future<Output = intent_sourcecontrol::Result<SecretString>> + Send + 'f>>
    where
        's: 'f,
        'i: 'f,
        Self: 'f,
    {
        self.bound.token_for(instance)
    }
    fn token_for_request<'s, 'i, 'r, 'f>(
        &'s self,
        instance: &'i GitlabInstance,
        mut request: GitlabCredentialRequest<'r>,
    ) -> Pin<Box<dyn Future<Output = intent_sourcecontrol::Result<SecretString>> + Send + 'f>>
    where
        's: 'f,
        'i: 'f,
        'r: 'f,
        Self: 'f,
    {
        match self.alter {
            AlterRequest::BranchProject if request.path.contains("/repository/branches/") => {
                request.path = "projects/99/repository/branches/feature";
            }
            AlterRequest::BranchName if request.path.contains("/repository/branches/") => {
                request.path = "projects/41/repository/branches/other";
            }
            AlterRequest::BranchWrite if request.path.contains("/repository/branches/") => {
                request.path = "projects/41/merge_requests";
                request.writing = true;
            }
            AlterRequest::BranchEndpoint if request.path.contains("/repository/branches/") => {
                request.path = "projects/41/issues";
            }
            AlterRequest::CreateProject if request.writing => {
                request.path = "projects/99/merge_requests";
            }
            AlterRequest::PipelineProject if request.path.contains("/pipelines/") => {
                request.path = "projects/99/pipelines/9/jobs";
            }
            AlterRequest::PipelineId if request.path.contains("/pipelines/") => {
                request.path = "projects/82/pipelines/10/jobs";
            }
            AlterRequest::PipelineWrite if request.path.contains("/pipelines/") => {
                request.path = "projects/82/merge_requests";
                request.writing = true;
            }
            AlterRequest::PipelineProof if request.path.contains("/pipelines/") => {
                request = GitlabCredentialRequest::direct(
                    request.descriptor,
                    request.path,
                    request.writing,
                );
            }
            _ => {}
        }
        self.bound.token_for_request(instance, request)
    }
}

#[tokio::test]
async fn bound_branch_read_proof_is_exact_and_never_grants_a_write() {
    for alter in [
        AlterRequest::BranchName,
        AlterRequest::BranchWrite,
        AlterRequest::BranchEndpoint,
    ] {
        let server = Server::new().await;
        *server.handler.lock().unwrap() = Box::new(|_, path| {
            if path.ends_with("team%2Fsub%2Fproject") {
                Reply::ok(project())
            } else if path.contains("/repository/branches/") {
                Reply::ok(
                    json!({"name":path.rsplit('/').next().unwrap(),"commit":{"id":"actual-sha"}}),
                )
            } else {
                Reply::ok(json!([]))
            }
        });
        let result = altered_provider(&server, alter)
            .create_same_project(
                &repo(),
                NewPullRequest {
                    title: "actual title".into(),
                    body: None,
                    source_branch: "feature".into(),
                    target_branch: "main".into(),
                    draft: false,
                },
                &pair("feature"),
                &pair("main"),
            )
            .await;
        assert!(
            matches!(result, Err(Error::AdmissionUnavailable(_))),
            "{result:?}"
        );
        assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.seen.lock().unwrap().len(), 2);
    }
}
fn altered_provider(
    server: &Server,
    alter: AlterRequest,
) -> intent_sourcecontrol::GitLabSourceControl {
    let bound = BoundGitlabRequestCredentials::new(
        server.test.directory.clone(),
        server
            .test
            .admit(RepositoryCredentialUse::NativeReviewCreate),
        server.test.secrets.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    intent_sourcecontrol::GitLabSourceControl::new(
        server.test.verified.descriptor.clone(),
        Arc::new(AlteredCredentials { bound, alter }),
    )
    .unwrap()
}

#[tokio::test]
async fn bound_numeric_create_rejects_changed_project_before_secret_or_dispatch() {
    for alter in [AlterRequest::BranchProject, AlterRequest::CreateProject] {
        let server = Server::new().await;
        *server.handler.lock().unwrap() = Box::new(|_, path| {
            if path.ends_with("team%2Fsub%2Fproject") {
                Reply::ok(project())
            } else if path.contains("/repository/branches/") {
                Reply::ok(
                    json!({"name":path.rsplit('/').next().unwrap(),"commit":{"id":"actual-sha"}}),
                )
            } else {
                Reply::ok(json!([]))
            }
        });
        let error = altered_provider(&server, alter)
            .create_same_project(
                &repo(),
                NewPullRequest {
                    title: "actual title".into(),
                    body: None,
                    source_branch: "feature".into(),
                    target_branch: "main".into(),
                    draft: false,
                },
                &pair("feature"),
                &pair("main"),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::AdmissionUnavailable(_)), "{error:?}");
        let expected = if matches!(alter, AlterRequest::BranchProject) {
            2
        } else {
            4
        };
        assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), expected);
        assert_eq!(server.seen.lock().unwrap().len(), expected);
    }
}

#[tokio::test]
async fn bound_pipeline_proof_cannot_grant_another_project_pipeline_or_write() {
    for alter in [
        AlterRequest::PipelineProject,
        AlterRequest::PipelineId,
        AlterRequest::PipelineWrite,
        AlterRequest::PipelineProof,
    ] {
        let server = Server::new().await;
        *server.handler.lock().unwrap() = Box::new(|_, path| {
            if path.ends_with("/merge_requests/4") {
                let mut body = mr();
                body["source_project_id"] = json!(82);
                body["head_pipeline"] = json!({"id":9,"project_id":82,"status":"success"});
                Reply::ok(body)
            } else if path.ends_with("team%2Fsub%2Fproject") {
                Reply::ok(project())
            } else if path.ends_with("/approvals") {
                Reply::ok(json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}))
            } else {
                Reply::ok(json!([]))
            }
        });
        let error = altered_provider(&server, alter)
            .observe_review(&repo(), 4)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::AdmissionUnavailable(_)), "{error:?}");
        assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 3);
        assert_eq!(server.seen.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn bound_numeric_create_returns_the_confirmed_result() {
    let server = Server::new().await;
    *server.handler.lock().unwrap() = Box::new(|method, path| {
        if method == "POST" {
            return Reply::ok(mr());
        }
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply::ok(project());
        }
        if path.contains("/repository/branches/") {
            return Reply::ok(
                json!({"name":path.rsplit('/').next().unwrap(),"commit":{"id":"actual-sha"}}),
            );
        }
        Reply::ok(json!([]))
    });
    let created = server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .create_same_project(
            &repo(),
            NewPullRequest {
                title: "requested title".into(),
                body: None,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                draft: false,
            },
            &pair("feature"),
            &pair("main"),
        )
        .await
        .unwrap();
    assert_eq!(
        created.outcome,
        intent_sourcecontrol::model::ReviewCreateOutcome::Created
    );
    assert_eq!(created.details.review.title, "actual title");
    assert_eq!(
        created.details.source.unwrap().project_path.as_deref(),
        Some(PROJECT)
    );
    assert_eq!(created.details.target.unwrap().project_id, 41);
    let seen = server.seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    assert!(seen[2]
        .0
        .contains("/projects/41/repository/branches/feature"));
    assert!(seen[3].0.contains("/projects/41/repository/branches/main"));
    assert!(seen[4]
        .0
        .starts_with("POST /fixture/api/v4/projects/41/merge_requests "));
    assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 5);
    assert!(server
        .test
        .authority
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.use_kind == RepositoryCredentialUse::NativeReviewCreate));
}

#[tokio::test]
async fn bound_target_pipeline_keeps_optional_restrictions_separate_from_denial() {
    use intent_sourcecontrol::model::ProviderAvailability;
    for (status, expected) in [
        (200, Some(ProviderAvailability::Available)),
        (403, Some(ProviderAvailability::Restricted)),
        (404, Some(ProviderAvailability::Unavailable)),
        (401, None),
    ] {
        let server = Server::new().await;
        *server.handler.lock().unwrap() = Box::new(move |_, path| {
            if path.ends_with("/merge_requests/4") {
                let mut body = mr();
                body["source_project_id"] = json!(82);
                body["head_pipeline"] = json!({"id":9,"project_id":41,"status":"success"});
                return Reply::ok(body);
            }
            if path.ends_with("team%2Fsub%2Fproject") {
                return Reply::ok(project());
            }
            if path.ends_with("/approvals") {
                return Reply::ok(
                    json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}),
                );
            }
            if path.contains("/pipelines/") {
                return Reply {
                    status,
                    body: json!([]),
                    next: None,
                };
            }
            Reply::ok(json!([]))
        });
        let result = server
            .provider(RepositoryCredentialUse::NativeRead)
            .observe_review(&repo(), 4)
            .await;
        if let Some(expected) = expected {
            assert_eq!(result.unwrap().availability.checks, expected);
        } else {
            assert!(
                matches!(result,Err(Error::Provider(p)) if p.kind==ProviderFailureKind::CredentialRejected)
            );
        }
        assert!(server
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|(line, _)| line.contains("/projects/41/pipelines/9/jobs")));
    }
}

// Private authority/secret doubles schedule the actual Bound/provider chain.
// Original Store/Git authority and paired-file evidence are covered separately.
struct AdmissionSchedule {
    test: Arc<Test>,
    calls: std::sync::atomic::AtomicUsize,
    pause: Option<Arc<Pause>>,
    after: Option<AfterRelease>,
}
impl RepositoryAuthority for AdmissionSchedule {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> super::authority::CredentialFuture<'a, Box<dyn super::authority::RepositoryAuthorityFence>>
    {
        Box::pin(async move {
            let inner = self.test.authority.revalidate(request).await?;
            let final_check = self.calls.fetch_add(1, Ordering::SeqCst) == 1;
            if final_check {
                if let Some(pause) = &self.pause {
                    pause.entered.notify_one();
                    pause.release.acquire().await.unwrap().forget();
                }
            }
            Ok(Box::new(ReleaseFence {
                inner,
                test: self.test.clone(),
                after: final_check.then_some(self.after).flatten(),
            })
                as Box<dyn super::authority::RepositoryAuthorityFence>)
        })
    }
}
fn scheduled_provider(
    server: &Server,
    pause: Option<Arc<Pause>>,
    after: Option<AfterRelease>,
    budget: Duration,
) -> intent_sourcecontrol::GitLabSourceControl {
    let authority = Arc::new(AdmissionSchedule {
        test: server.test.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        pause,
        after,
    });
    let admission = server
        .test
        .directory
        .admit(
            &server.test.directory.binding().unwrap(),
            server
                .test
                .request(RepositoryCredentialUse::NativeReviewCreate),
            authority,
        )
        .unwrap();
    BoundGitlabRequestCredentials::new(
        server.test.directory.clone(),
        admission,
        server.test.secrets.clone(),
        budget,
    )
    .unwrap()
    .into_provider()
    .unwrap()
}
async fn reached(pause: &Pause) {
    tokio::time::timeout(Duration::from_secs(2), pause.entered.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn http_admission_final_fence_rechecks_directory_and_original_authority() {
    for change in 0..4 {
        let server = Server::new().await;
        let pause = Pause::new();
        let provider =
            scheduled_provider(&server, Some(pause.clone()), None, Duration::from_secs(2));
        let task = tokio::spawn(async move { provider.list_comments(&repo(), 4).await });
        reached(&pause).await;
        match change {
            0 => server.test.directory.retire().unwrap(),
            1 => server.test.refresh("token-new"),
            2 => {
                server.test.replace(server.test.verified.clone());
            }
            _ => *server.test.authority.revision.lock().unwrap() += 1,
        }
        pause.release.add_permits(1);
        let error = task.await.unwrap().unwrap_err();
        if change == 1 {
            assert!(matches!(
                error,
                Error::AdmissionUnavailable(
                    intent_sourcecontrol::error::AdmissionUnavailable::SecretChanged
                )
            ));
        } else {
            assert!(matches!(error, Error::AdmissionRetired));
        }
        assert!(server.seen.lock().unwrap().is_empty());
        assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn http_admission_winning_transfer_preserves_known_success_after_retirement() {
    let server = Server::new().await;
    let provider = scheduled_provider(
        &server,
        None,
        Some(AfterRelease::Retire),
        Duration::from_secs(2),
    );
    provider.list_comments(&repo(), 4).await.unwrap();
    assert_eq!(server.seen.lock().unwrap().len(), 1);
    assert!(matches!(
        provider.list_comments(&repo(), 4).await,
        Err(Error::AdmissionRetired)
    ));
    assert_eq!(server.seen.lock().unwrap().len(), 1);
    assert_eq!(server.test.authority.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn http_admission_cancellation_at_prepared_fence_sends_nothing() {
    let server = Server::new().await;
    let pause = Pause::new();
    let provider = scheduled_provider(&server, Some(pause.clone()), None, Duration::from_secs(2));
    let task = tokio::spawn(async move { provider.list_comments(&repo(), 4).await });
    reached(&pause).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    pause.release.add_permits(1);
    assert!(server.seen.lock().unwrap().is_empty());
    server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .list_comments(&repo(), 4)
        .await
        .unwrap();
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_admission_detached_preparation_still_refuses_original_retirement() {
    let server = Server::new().await;
    let pause = Pause::new();
    let provider = scheduled_provider(&server, Some(pause.clone()), None, Duration::from_secs(2));
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = send.send(provider.list_comments(&repo(), 4).await);
    });
    reached(&pause).await;
    drop(task); // Detaching is not cancellation or authority.
    server.test.directory.retire().unwrap();
    pause.release.add_permits(1);
    assert!(matches!(
        receive.await.unwrap(),
        Err(Error::AdmissionRetired)
    ));
    assert!(server.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_admission_timeout_after_preparation_is_local_and_unsent() {
    let server = Server::new().await;
    let pause = Pause::new();
    let provider = scheduled_provider(
        &server,
        Some(pause.clone()),
        None,
        Duration::from_millis(100),
    );
    let task = tokio::spawn(async move { provider.list_comments(&repo(), 4).await });
    reached(&pause).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::TimedOut
        ))
    ));
    assert!(server.seen.lock().unwrap().is_empty());
    assert!(server.test.directory.binding().is_ok());
}

#[tokio::test]
async fn http_admission_old_401_cannot_disconnect_refreshed_or_replaced_secret() {
    for replace in [false, true] {
        let server = Server::new().await;
        let test = server.test.clone();
        *server.handler.lock().unwrap() = Box::new(move |_, _| {
            if replace {
                test.replace(test.verified.clone());
                *test.secrets.value.lock().unwrap() = "replacement-token".into();
            } else {
                test.refresh("token-new");
            }
            Reply {
                status: 401,
                body: json!({}),
                next: None,
            }
        });
        let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
        assert!(
            matches!(provider.list_comments(&repo(),4).await, Err(Error::Provider(p)) if p.kind==ProviderFailureKind::CredentialRejected)
        );
        assert!(server.test.directory.binding().is_ok());
        *server.handler.lock().unwrap() = Box::new(|_, _| Reply::ok(json!([])));
        if replace {
            assert!(matches!(
                provider.list_comments(&repo(), 4).await,
                Err(Error::AdmissionRetired)
            ));
            assert_eq!(server.seen.lock().unwrap().len(), 1);
        } else {
            provider.list_comments(&repo(), 4).await.unwrap();
        }
        server
            .provider(RepositoryCredentialUse::NativeReviewCreate)
            .list_comments(&repo(), 4)
            .await
            .unwrap();
        let seen = server.seen.lock().unwrap();
        assert!(seen[0].1.contains("Bearer token-old"));
        assert!(seen.last().unwrap().1.contains(if replace {
            "Bearer replacement-token"
        } else {
            "Bearer token-new"
        }));
    }
}

#[tokio::test]
async fn http_admission_current_401_retires_only_volatile_eligibility() {
    let server = Server::new().await;
    *server.handler.lock().unwrap() = Box::new(|_, _| Reply {
        status: 401,
        body: json!({}),
        next: None,
    });
    let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
    assert!(
        matches!(provider.list_comments(&repo(),4).await,Err(Error::Provider(p)) if p.kind==ProviderFailureKind::CredentialRejected)
    );
    assert_eq!(
        server.test.directory.binding(),
        Err(RepositoryCredentialError::Disconnected)
    );
    assert_eq!(*server.test.secrets.value.lock().unwrap(), "token-old");
    assert!(matches!(
        provider.list_comments(&repo(), 4).await,
        Err(Error::AdmissionRetired)
    ));
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_admission_429_during_refresh_keeps_original_quota_and_longest_deadline() {
    let server = Server::new().await;
    server
        .headers
        .lock()
        .unwrap()
        .push(("retry-after".into(), "120".into()));
    let test = server.test.clone();
    let held = Arc::new(Mutex::new(None));
    let pending = held.clone();
    let prior = server
        .test
        .acquire(
            &server
                .test
                .admit(RepositoryCredentialUse::NativeReviewCreate),
        )
        .await
        .unwrap()
        .stamp;
    let floor = Instant::now() + Duration::from_secs(300);
    *server.handler.lock().unwrap() = Box::new(move |_, _| {
        test.directory.record_backoff(&prior, floor).unwrap();
        let ticket = test
            .directory
            .reserve_mutation(RepositoryMutationKind::Refresh)
            .unwrap();
        test.directory.begin_mutation(&ticket).unwrap();
        *pending.lock().unwrap() = Some(ticket);
        Reply {
            status: 429,
            body: json!({}),
            next: None,
        }
    });
    let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
    assert!(matches!(
        provider.list_comments(&repo(), 4).await,
        Err(Error::RateLimited(_))
    ));
    let deadline = server.test.directory.lock().unwrap().backoff_until.unwrap();
    assert!(deadline >= floor);
    assert_eq!(
        server.test.directory.lock().unwrap().status,
        RepositoryConnectionState::Mutating
    );
    let ticket = held.lock().unwrap().take().unwrap();
    *server.test.secrets.value.lock().unwrap() = "token-new".into();
    server
        .test
        .directory
        .finish_mutation(
            &ticket,
            SettledCredentialState::Verified(server.test.verified.clone()),
        )
        .unwrap();
    assert_eq!(
        server.test.directory.lock().unwrap().backoff_until,
        Some(deadline)
    );
    assert!(matches!(
        provider.list_comments(&repo(), 4).await,
        Err(Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_admission_replaced_binding_does_not_inherit_old_quota() {
    let server = Server::new().await;
    server
        .headers
        .lock()
        .unwrap()
        .push(("retry-after".into(), "120".into()));
    let test = server.test.clone();
    *server.handler.lock().unwrap() = Box::new(move |_, _| {
        test.replace(test.verified.clone());
        Reply {
            status: 429,
            body: json!({}),
            next: None,
        }
    });
    assert!(matches!(
        server
            .provider(RepositoryCredentialUse::NativeReviewCreate)
            .list_comments(&repo(), 4)
            .await,
        Err(Error::RateLimited(_))
    ));
    assert!(server
        .test
        .directory
        .lock()
        .unwrap()
        .backoff_until
        .is_none());
    *server.handler.lock().unwrap() = Box::new(|_, _| Reply::ok(json!([])));
    server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .list_comments(&repo(), 4)
        .await
        .unwrap();
    assert_eq!(server.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn http_admission_optional_quota_is_observed_before_partial_result() {
    let server = Server::new().await;
    server
        .headers
        .lock()
        .unwrap()
        .push(("retry-after".into(), "120".into()));
    *server.handler.lock().unwrap() = Box::new(|_, path| {
        if path.ends_with("/merge_requests/4") {
            return Reply::ok(mr());
        }
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply::ok(project());
        }
        if path.ends_with("/approvals") {
            return Reply::ok(json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}));
        }
        Reply {
            status: 429,
            body: json!([]),
            next: None,
        }
    });
    let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
    let result = provider.observe_review(&repo(), 4).await.unwrap();
    assert_eq!(
        result.availability.discussions,
        intent_sourcecontrol::model::ProviderAvailability::RateLimited
    );
    assert!(server
        .test
        .directory
        .lock()
        .unwrap()
        .backoff_until
        .is_some());
    assert!(matches!(
        provider.observe_review(&repo(), 4).await,
        Err(Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(server.seen.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn http_admission_detach_or_cancel_cannot_recall_a_request_already_seen() {
    for cancel in [false, true] {
        let server = Server::new().await;
        let pause = Pause::new();
        *server.response_pause.lock().unwrap() = Some(pause.clone());
        let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
        let (send, receive) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = send.send(provider.list_comments(&repo(), 4).await);
        });
        reached(&pause).await;
        assert_eq!(server.seen.lock().unwrap().len(), 1);
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            drop(task);
        }
        server.test.directory.retire().unwrap();
        pause.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), server.response_completed.notified())
            .await
            .unwrap();
        if cancel {
            // Dropping a sent future cannot prove that its effect was unsent.
            assert!(receive.await.is_err());
        } else {
            receive.await.unwrap().unwrap();
        }
        assert_eq!(server.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn http_admission_early_optional_quota_blocks_follow_up_without_auth_denial() {
    let server = Server::new().await;
    server
        .headers
        .lock()
        .unwrap()
        .push(("retry-after".into(), "120".into()));
    *server.handler.lock().unwrap() = Box::new(|_, path| {
        if path.ends_with("/merge_requests/4") {
            return Reply::ok(mr());
        }
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply::ok(project());
        }
        Reply {
            status: 429,
            body: json!([]),
            next: None,
        }
    });
    let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
    let result = provider.observe_review(&repo(), 4).await.unwrap();
    // Approval quota is observed before degradation. The later discussion request
    // is locally refused, while the fetched primary and policy remain available.
    assert_eq!(result.details.review.title, "actual title");
    assert!(result.signals.branch_rules.is_some());
    assert_eq!(
        result.availability.approvals,
        ProviderAvailability::RateLimited
    );
    assert_eq!(
        result.availability.discussions,
        ProviderAvailability::RateLimited
    );
    assert!(server
        .test
        .directory
        .lock()
        .unwrap()
        .backoff_until
        .is_some());
    assert!(server.test.directory.binding().is_ok());
    assert_eq!(server.seen.lock().unwrap().len(), 3);
    assert_eq!(
        provider.rate_limit_status().await.unwrap().remaining,
        Some(0)
    );
}

// These use the actual managed provider with the injected Test authority/reader.
// Every MR has a valid numeric pipeline, including the policy-quota case where
// its project corroboration is unavailable and no jobs request may be invented.
fn optional_quota_reply(path: &str, quota_at: usize) -> Reply {
    if path.ends_with("/merge_requests/4") {
        let mut review = mr();
        review["head_pipeline"] = json!({"id":9,"project_id":41,"status":"success"});
        return Reply::ok(review);
    }
    let (index, body) = if path.ends_with("team%2Fsub%2Fproject") {
        (0, project())
    } else if path.ends_with("/approvals") {
        (
            1,
            json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}),
        )
    } else if path.contains("/projects/41/pipelines/9/jobs") {
        (2, json!([]))
    } else {
        assert!(path.contains("/discussions"), "unexpected fixture endpoint");
        (3, json!([]))
    };
    if index == quota_at {
        Reply {
            status: 429,
            body: json!({}),
            next: None,
        }
    } else {
        Reply::ok(body)
    }
}

async fn optional_quota_server(quota_at: usize) -> Server {
    let server = Server::new().await;
    server
        .headers
        .lock()
        .unwrap()
        .push(("retry-after".into(), "120".into()));
    *server.handler.lock().unwrap() = Box::new(move |_, path| optional_quota_reply(path, quota_at));
    server
}

async fn assert_optional_quota_partial(quota_at: usize) {
    let server = optional_quota_server(quota_at).await;
    let original_binding = server.test.directory.binding().unwrap();
    let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
    let result = provider.observe_review(&repo(), 4).await.unwrap();
    assert_eq!(result.details.review.title, "actual title");
    assert_eq!(result.details.source.as_ref().unwrap().project_id, 41);
    assert_eq!(result.details.target.as_ref().unwrap().project_id, 41);
    assert_eq!(result.details.confirmed_draft, Some(false));
    assert_eq!(
        result.details.confirmed_state,
        Some(intent_sourcecontrol::model::ConfirmedReviewState::Open)
    );
    for (index, availability) in [
        result.availability.policy,
        result.availability.approvals,
        result.availability.checks,
        result.availability.discussions,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            availability,
            if index < quota_at {
                ProviderAvailability::Available
            } else {
                ProviderAvailability::RateLimited
            }
        );
    }
    assert_eq!(result.signals.branch_rules.is_some(), quota_at > 0);
    assert_eq!(result.reviews.is_some(), quota_at > 1);
    assert_eq!(result.signals.checks_known, quota_at > 2);
    assert!(result.threads.is_none());
    assert!(result.conversation_count.is_none());
    let deadline = server.test.directory.lock().unwrap().backoff_until.unwrap();
    assert!(deadline > Instant::now());
    assert_eq!(server.test.directory.binding().unwrap(), original_binding);
    assert_eq!(*server.test.secrets.value.lock().unwrap(), "token-old");
    assert_eq!(server.seen.lock().unwrap().len(), quota_at + 2);
    assert_eq!(
        server.test.secrets.calls.load(Ordering::SeqCst),
        quota_at + 2
    );
    assert_eq!(
        provider.rate_limit_status().await.unwrap().remaining,
        Some(0)
    );
    // A new primary read has no fetched data to preserve and remains a local refusal.
    assert!(matches!(
        provider.observe_review(&repo(), 4).await,
        Err(Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::Backoff
        ))
    ));
    assert_eq!(server.seen.lock().unwrap().len(), quota_at + 2);
    assert_eq!(
        server.test.directory.lock().unwrap().backoff_until,
        Some(deadline)
    );
}

#[tokio::test]
async fn http_admission_optional_policy_quota_retains_partial_with_numeric_pipeline() {
    assert_optional_quota_partial(0).await;
}

#[tokio::test]
async fn http_admission_optional_approvals_quota_retains_partial_with_numeric_pipeline() {
    assert_optional_quota_partial(1).await;
}

#[tokio::test]
async fn http_admission_optional_checks_quota_retains_partial_with_numeric_pipeline() {
    assert_optional_quota_partial(2).await;
}

#[tokio::test]
async fn http_admission_optional_discussions_quota_retains_partial_with_numeric_pipeline() {
    assert_optional_quota_partial(3).await;
}

#[tokio::test]
async fn http_admission_optional_quota_legacy_projections_remain_rate_limited() {
    for quota_at in 0..4 {
        for projection in 0..4 {
            let server = optional_quota_server(quota_at).await;
            let provider = server.provider(RepositoryCredentialUse::NativeReviewCreate);
            let result = match projection {
                0 => provider.review_decision(&repo(), 4).await.map(|_| ()),
                1 => provider.merge_requirements(&repo(), 4).await.map(|_| ()),
                2 => provider.mergeability(&repo(), 4).await.map(|_| ()),
                _ => provider.pr_observation(&repo(), 4).await.map(|_| ()),
            };
            assert!(
                matches!(result, Err(Error::RateLimited(_))),
                "quota endpoint {quota_at}, projection {projection}: {result:?}"
            );
            assert_eq!(server.seen.lock().unwrap().len(), quota_at + 2);
            assert!(server.test.directory.binding().is_ok());
            assert!(server
                .test
                .directory
                .lock()
                .unwrap()
                .backoff_until
                .is_some());
        }
    }
}

#[tokio::test]
async fn http_admission_optional_quota_does_not_hide_retirement_or_replacement() {
    for quota_at in 0..3 {
        for replace in [false, true] {
            let server = optional_quota_server(quota_at).await;
            let test = server.test.clone();
            *server.handler.lock().unwrap() = Box::new(move |_, path| {
                let reply = optional_quota_reply(path, quota_at);
                if reply.status == 429 {
                    if replace {
                        test.replace(test.verified.clone());
                    } else {
                        test.directory.retire().unwrap();
                    }
                }
                reply
            });
            let error = server
                .provider(RepositoryCredentialUse::NativeReviewCreate)
                .observe_review(&repo(), 4)
                .await
                .unwrap_err();
            assert!(matches!(error, Error::AdmissionRetired), "{error:?}");
            assert_eq!(server.seen.lock().unwrap().len(), quota_at + 2);
            assert_eq!(*server.test.secrets.value.lock().unwrap(), "token-old");
            if replace {
                assert!(server.test.directory.binding().is_ok());
                assert!(server
                    .test
                    .directory
                    .lock()
                    .unwrap()
                    .backoff_until
                    .is_none());
            }
        }
    }
}

#[tokio::test]
async fn http_admission_optional_nonquota_policy_failure_still_requires_numeric_proof() {
    let server = Server::new().await;
    *server.handler.lock().unwrap() = Box::new(|_, path| {
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply {
                status: 503,
                body: json!({}),
                next: None,
            };
        }
        optional_quota_reply(path, usize::MAX)
    });
    let error = server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .observe_review(&repo(), 4)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::AdmissionUnavailable(
            intent_sourcecontrol::error::AdmissionUnavailable::BoundaryMismatch
        )
    ));
    // Missing policy from a non-quota failure is not an excuse to bypass proof.
    assert_eq!(server.seen.lock().unwrap().len(), 3);
    assert!(server.test.directory.binding().is_ok());
    assert!(server
        .test
        .directory
        .lock()
        .unwrap()
        .backoff_until
        .is_none());
}

#[tokio::test]
async fn http_admission_optional_authority_retirement_remains_fatal() {
    let server = Server::new().await;
    let authority = server.test.authority.clone();
    *server.handler.lock().unwrap() = Box::new(move |_, path| {
        if path.ends_with("team%2Fsub%2Fproject") {
            *authority.revision.lock().unwrap() += 1;
        }
        optional_quota_reply(path, usize::MAX)
    });
    let error = server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .observe_review(&repo(), 4)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AdmissionRetired));
    assert_eq!(server.seen.lock().unwrap().len(), 2);
    assert!(server.test.directory.binding().is_ok());
    assert!(server
        .test
        .directory
        .lock()
        .unwrap()
        .backoff_until
        .is_none());
}

struct FaultyAdmission {
    test: Arc<Test>,
    calls: std::sync::atomic::AtomicUsize,
    fault: u8,
}
struct FaultyFence {
    inner: Box<dyn super::authority::RepositoryAuthorityFence>,
    fault: u8,
}
impl super::authority::RepositoryAuthorityFence for FaultyFence {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.inner.dispatch(&mut || match self.fault {
            0 => Ok(()),
            1 => {
                action()?;
                action()
            }
            _ => {
                action()?;
                Err(RepositoryCredentialError::AuthorityUnavailable)
            }
        })
    }
}
impl RepositoryAuthority for FaultyAdmission {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> super::authority::CredentialFuture<'a, Box<dyn super::authority::RepositoryAuthorityFence>>
    {
        Box::pin(async move {
            let inner = self.test.authority.revalidate(request).await?;
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(inner);
            }
            Ok(Box::new(FaultyFence {
                inner,
                fault: self.fault,
            })
                as Box<dyn super::authority::RepositoryAuthorityFence>)
        })
    }
}
#[tokio::test]
async fn http_admission_transfer_requires_one_successful_consuming_action() {
    for fault in 0..3 {
        let server = Server::new().await;
        let authority = Arc::new(FaultyAdmission {
            test: server.test.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            fault,
        });
        let admission = server
            .test
            .directory
            .admit(
                &server.test.directory.binding().unwrap(),
                server
                    .test
                    .request(RepositoryCredentialUse::NativeReviewCreate),
                authority,
            )
            .unwrap();
        let provider = BoundGitlabRequestCredentials::new(
            server.test.directory.clone(),
            admission,
            server.test.secrets.clone(),
            Duration::from_secs(2),
        )
        .unwrap()
        .into_provider()
        .unwrap();
        let error = provider.list_comments(&repo(), 4).await.unwrap_err();
        assert!(matches!(error, Error::AdmissionUnavailable(_)));
        assert!(server.seen.lock().unwrap().is_empty());
        assert_eq!(server.test.secrets.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn http_admission_confirmed_create_is_not_rewritten_as_unsent_after_retirement() {
    let server = Server::new().await;
    let test = server.test.clone();
    *server.handler.lock().unwrap() = Box::new(move |method, path| {
        if method == "POST" {
            test.directory.retire().unwrap();
            return Reply::ok(mr());
        }
        if path.ends_with("team%2Fsub%2Fproject") {
            return Reply::ok(project());
        }
        if path.contains("/repository/branches/") {
            return Reply::ok(
                json!({"name":path.rsplit('/').next().unwrap(),"commit":{"id":"actual-sha"}}),
            );
        }
        Reply::ok(json!([]))
    });
    let result = server
        .provider(RepositoryCredentialUse::NativeReviewCreate)
        .create_same_project(
            &repo(),
            NewPullRequest {
                title: "sent title".into(),
                body: None,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                draft: false,
            },
            &pair("feature"),
            &pair("main"),
        )
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        intent_sourcecontrol::model::ReviewCreateOutcome::Created
    );
    assert_eq!(result.details.review.title, "actual title");
    assert_eq!(
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(line, _)| line.starts_with("POST "))
            .count(),
        1
    );
}
