use std::sync::atomic::Ordering;
use std::{future::Future, pin::Pin};

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
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let observed = observed.clone();
                let respond = respond.clone();
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
                    let body = reply.body.to_string();
                    let next = reply
                        .next
                        .map_or_else(String::new, |page| format!("x-next-page: {page}\r\n"));
                    let response = format!("HTTP/1.1 {} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{next}connection: close\r\n\r\n{body}", reply.status, body.len());
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            test,
            seen,
            handler,
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
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
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
            // A provider error alone cannot delete credentials or mutate the directory.
            assert!(server.test.directory.binding().is_ok());
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
