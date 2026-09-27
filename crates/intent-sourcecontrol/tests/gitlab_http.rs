//! GitLab provider contracts against a local HTTP fixture. No real credentials or CLI.
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use intent_sourcecontrol::{
    error::{ProviderFailure, ProviderFailureKind},
    Error, GitLabSourceControl, GitlabDescriptor, GitlabInstance, GitlabRequestCredentials,
    IssueQuery, NewPullRequest, PageParams, PrQuery, ProviderAvailability, RepoRef,
    ReviewBranchIdentity, ReviewCreateOutcome, SourceControl,
};
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const INSTANCE: &str = "https://git.example:8443/forge";
#[derive(Clone)]
struct Request {
    method: String,
    path: String,
    headers: String,
    body: Value,
}
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "fixtures pass disposable JSON response values"
)]
fn reply(status: u16, body: Value) -> Reply {
    Reply {
        status,
        headers: vec![],
        body: body.to_string(),
    }
}
type Responder = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

struct Credentials {
    token: Mutex<Option<String>>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl GitlabRequestCredentials for Credentials {
    async fn token_for(
        &self,
        instance: &GitlabInstance,
    ) -> intent_sourcecontrol::Result<SecretString> {
        self.calls.lock().unwrap().push(instance.as_str().into());
        self.token
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| SecretString::from(s.clone()))
            .ok_or_else(|| Error::NotConfigured("retired scope".into()))
    }
}
struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
    credentials: Arc<Credentials>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new(respond: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/fixture", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let seen = requests.clone();
        let respond: Responder = Arc::new(respond);
        let server = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let seen = seen.clone();
                let respond = respond.clone();
                tokio::spawn(async move {
                    serve(socket, &seen, &respond).await;
                });
            }
        });
        Self {
            endpoint,
            requests,
            credentials: Arc::new(Credentials {
                token: Mutex::new(Some("fixture-token".into())),
                calls: Mutex::new(vec![]),
            }),
            server,
        }
    }
    fn provider(&self) -> GitLabSourceControl {
        GitLabSourceControl::new(
            GitlabDescriptor::with_loopback_endpoint(
                GitlabInstance::parse(INSTANCE).unwrap(),
                &self.endpoint,
            )
            .unwrap(),
            self.credentials.clone(),
        )
        .unwrap()
    }
    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}
async fn serve(mut socket: TcpStream, seen: &Mutex<Vec<Request>>, respond: &Responder) {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    let (head_end, size) = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        if n == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(i) = buffer.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&buffer[..i]);
            let size = headers
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|s| s.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            break (i + 4, size);
        }
    };
    while buffer.len() < head_end + size {
        let n = socket.read(&mut chunk).await.unwrap();
        if n == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let headers = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut first = headers.lines().next().unwrap().split_whitespace();
    let request = Request {
        method: first.next().unwrap().into(),
        path: first.next().unwrap().into(),
        headers: headers.clone(),
        body: serde_json::from_slice(&buffer[head_end..]).unwrap_or(Value::Null),
    };
    seen.lock().unwrap().push(request.clone());
    let response = respond(&request);
    let extras = response
        .headers
        .iter()
        .fold(String::new(), |mut output, (key, value)| {
            use std::fmt::Write as _;
            write!(output, "{key}: {value}\r\n").unwrap();
            output
        });
    let wire=format!("HTTP/1.1 {} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{extras}connection: close\r\n\r\n{}",response.status,response.body.len(),response.body);
    let _ = socket.write_all(wire.as_bytes()).await;
}
fn repo() -> RepoRef {
    RepoRef::new("Team/Sub", "Project")
}
fn project() -> Value {
    json!({"id":73,"path_with_namespace":"Team/Sub/Project","web_url":format!("{INSTANCE}/Team/Sub/Project"),"default_branch":"main"})
}
fn mr() -> Value {
    json!({"iid":7,"web_url":format!("{INSTANCE}/Team/Sub/Project/-/merge_requests/7"),"title":"Actual existing title","description":"Actual body","state":"opened","draft":false,"source_project_id":73,"target_project_id":73,"source_branch":"feature","target_branch":"main","author":{"username":"alice"},"sha":"remote-A","detailed_merge_status":"checking","created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
}
fn identity(branch: &str) -> ReviewBranchIdentity {
    ReviewBranchIdentity {
        instance_base_url: INSTANCE.into(),
        project_id: 73,
        project_path: Some("Team/Sub/Project".into()),
        branch: branch.into(),
    }
}
fn input() -> NewPullRequest {
    NewPullRequest {
        title: "Requested new title".into(),
        body: Some("Requested body".into()),
        source_branch: "feature".into(),
        target_branch: "main".into(),
        draft: false,
    }
}

#[test]
fn instance_canonicalization_and_boundaries_are_independent_of_transport() {
    assert_eq!(
        GitlabInstance::parse("GitLab.COM:443/").unwrap().as_str(),
        "https://gitlab.com"
    );
    let i = GitlabInstance::parse("https://Git.Example:8443/forge/").unwrap();
    assert_eq!(i.as_str(), INSTANCE);
    assert!(i.contains_url(&format!("{INSTANCE}/Team/Sub/Project")));
    for bad in [
        "https://git.example:8443/forge2/project",
        "https://git.example/forge/project",
        "https://git.example:8443/forge/../escape",
        "https://git.example:8443/forge/%2e%2e/escape",
        "https://alice@git.example:8443/forge/project",
    ] {
        assert!(!i.contains_url(bad), "{bad}");
    }
    for bad in [
        "http://git.example/forge",
        "https://git.example/forge?x=1",
        "https://git.example/forge#frag",
        "https://git.example/a/../forge",
        "https://git.example/a%2fforge",
        "https://git.example/for\\ge",
        "https://git.exa\nmple/forge",
        "https://*.example/forge",
    ] {
        assert!(GitlabInstance::parse(bad).is_err(), "{bad}");
    }
    assert!(GitlabDescriptor::with_loopback_endpoint(i, "https://other.example/forge").is_err());
}

#[tokio::test]
async fn nested_projects_branches_and_credentials_use_the_right_endpoint() {
    let f = Fixture::new(|r| {
        if r.path.contains("repository/branches") {
            reply(
                200,
                json!([{"name":"feature/a","commit":{"id":"sha-A"},"protected":false}]),
            )
        } else {
            reply(200, project())
        }
    })
    .await;
    let sc = f.provider();
    let p = sc.get_repo("Team/Sub", "Project").await.unwrap();
    assert_eq!(p.owner, "Team/Sub");
    assert_eq!(p.default_branch.as_deref(), Some("main"));
    let branches = sc
        .list_remote_branches(
            "Team/Sub",
            "Project",
            Some("feature/"),
            PageParams::first(20),
        )
        .await
        .unwrap();
    assert_eq!(branches.items[0].commit_sha.as_deref(), Some("sha-A"));
    *f.credentials.token.lock().unwrap() = Some("rotated-token".into());
    sc.get_repo("Team/Sub", "Project").await.unwrap();
    let requests = f.requests();
    assert_eq!(
        requests[0].path,
        "/fixture/api/v4/projects/Team%2FSub%2FProject"
    );
    assert!(requests[1].path.contains("search=%5Efeature%2F"));
    assert!(requests[0].headers.contains("Bearer fixture-token"));
    assert!(requests[2].headers.contains("Bearer rotated-token"));
    assert!(f
        .credentials
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|s| s == INSTANCE));
    *f.credentials.token.lock().unwrap() = None;
    assert!(matches!(
        sc.get_repo("Team/Sub", "Project").await,
        Err(Error::NotConfigured(_))
    ));
    assert_eq!(f.requests().len(), 3);
}

#[tokio::test]
async fn primary_denial_is_typed_and_never_includes_response_secrets() {
    for (status, kind) in [
        (401, ProviderFailureKind::CredentialRejected),
        (403, ProviderFailureKind::ResourceDenied),
        (404, ProviderFailureKind::ResourceDenied),
    ] {
        let f = Fixture::new(move |_| {
            reply(status, json!({"message":"echo fixture-token private-body"}))
        })
        .await;
        let error = f.provider().get_pr(&repo(), 7).await.unwrap_err();
        assert!(
            matches!(error,Error::Provider(ProviderFailure {kind:k,status:Some(s)}) if s==status && k==kind)
        );
        assert!(!format!("{error:?}").contains("fixture-token"));
        assert!(!error.to_string().contains("private-body"));
    }
}

#[tokio::test]
async fn redirects_are_refused_before_credentials_can_cross_an_instance() {
    let receiver = Fixture::new(|_| reply(200, mr())).await;
    let location = format!("{}/api/v4/leak", receiver.endpoint);
    let origin = Fixture::new(move |_| Reply {
        status: 302,
        headers: vec![("location".into(), location.clone())],
        body: String::new(),
    })
    .await;
    assert!(origin.provider().get_pr(&repo(), 7).await.is_err());
    assert!(receiver.requests().is_empty());
}

#[tokio::test]
async fn pagination_cursors_are_bound_to_query_and_logical_request() {
    let f = Fixture::new(|r| {
        let mut res = reply(200, json!([project()]));
        res.headers.push((
            "x-next-page".into(),
            if r.path.contains("page=2") { "" } else { "2" }.into(),
        ));
        res
    })
    .await;
    let sc = f.provider();
    let first = sc
        .search_repos("first", PageParams::first(1))
        .await
        .unwrap();
    let cursor = first.next_cursor.unwrap();
    assert!(matches!(
        sc.search_repos(
            "other",
            PageParams {
                limit: 1,
                cursor: Some(cursor.clone())
            }
        )
        .await,
        Err(Error::Config(_))
    ));
    assert_eq!(f.requests().len(), 1);
    let second = sc
        .search_repos(
            "first",
            PageParams {
                limit: 1,
                cursor: Some(cursor),
            },
        )
        .await
        .unwrap();
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn pagination_cannot_follow_foreign_links_or_loop() {
    for header in [
        (
            "link",
            "<https://other.example/api/v4/projects?page=2>; rel=\"next\"",
        ),
        ("x-next-page", "1"),
    ] {
        let f = Fixture::new(move |_| Reply {
            status: 200,
            headers: vec![(header.0.into(), header.1.into())],
            body: json!([project()]).to_string(),
        })
        .await;
        assert!(f.provider().list_repos(PageParams::first(1)).await.is_err());
        assert_eq!(f.requests().len(), 1);
    }
}

#[tokio::test]
async fn fork_and_missing_project_metadata_remain_readable_but_not_reusable() {
    for source in [json!(91), Value::Null] {
        let f = Fixture::new(move |_| {
            let mut v = mr();
            v["source_project_id"] = source.clone();
            reply(200, v)
        })
        .await;
        let detail = f.provider().review_details(&repo(), 7).await.unwrap();
        assert!(!detail.matches_open(&identity("feature"), &identity("main")));
        assert_eq!(detail.review.head_sha.as_deref(), Some("remote-A"));
    }
}

#[tokio::test]
async fn unknown_merge_status_is_unknown_not_passing_or_known_blocked() {
    let f = Fixture::new(|_| {
        let mut v = mr();
        v["detailed_merge_status"] = json!("future-status");
        reply(200, v)
    })
    .await;
    let detail = f.provider().get_pr(&repo(), 7).await.unwrap();
    assert_eq!(detail.mergeable, None);
    assert_eq!(detail.mergeable_state.as_deref(), Some("unknown"));
}

fn observation_reply(r: &Request) -> Reply {
    if r.path.contains("/discussions") {
        return reply(
            200,
            json!([{ "notes":[{"id":1,"body":"Comment","author":{"username":"alice"},"system":false,"created_at":"now","resolvable":true,"resolved":false},{"id":2,"body":"Reply","author":{"username":"bob"},"system":false,"created_at":"now","resolvable":false}] }]),
        );
    }
    if r.path.ends_with("/approvals") {
        return reply(
            200,
            json!({"approved":false,"approvals_required":2,"approvals_left":1,"approved_by":[{"user":{"username":"alice"}}]}),
        );
    }
    if r.path.contains("/pipelines/") {
        return reply(
            200,
            json!([{"name":"optional","status":"failed","allow_failure":true},{"name":"required","status":"manual","allow_failure":false}]),
        );
    }
    if r.path.ends_with("/merge_requests/7") {
        let mut v = mr();
        v["head_pipeline"] = json!({"id":19,"project_id":91,"sha":"synthetic","status":"running"});
        return reply(200, v);
    }
    let mut p = project();
    p["only_allow_merge_if_pipeline_succeeds"] = json!(true);
    p["only_allow_merge_if_all_discussions_are_resolved"] = json!(true);
    reply(200, p)
}

#[tokio::test]
async fn observation_uses_the_mr_pipeline_project_and_head_with_honest_checks_and_threads() {
    let f = Fixture::new(observation_reply).await;
    let o = f.provider().observe_review(&repo(), 7).await.unwrap();
    assert_eq!(o.signals.checks_head_sha.as_deref(), Some("remote-A"));
    assert!(o.signals.checks_known);
    assert_eq!(
        o.signals.checks[1].state,
        intent_sourcecontrol::CheckState::Neutral
    );
    assert!(!o.signals.checks[1].is_required);
    assert_eq!(
        o.signals.checks[2].state,
        intent_sourcecontrol::CheckState::Pending
    );
    assert!(o.signals.checks[2].is_required);
    assert_eq!(o.threads.unwrap().unresolved, 1);
    assert_eq!(o.conversation_count, Some(0));
    assert_eq!(
        o.signals
            .branch_rules
            .unwrap()
            .required_approving_review_count,
        Some(2)
    );
    assert!(f.requests().iter().any(|r| r
        .path
        .starts_with("/fixture/api/v4/projects/91/pipelines/19/jobs")));
}

#[tokio::test]
async fn optional_denials_transients_and_rate_limits_remain_local_and_explicit() {
    for (status, expected) in [
        (403, ProviderAvailability::Restricted),
        (404, ProviderAvailability::Unavailable),
        (503, ProviderAvailability::Transient),
        (429, ProviderAvailability::RateLimited),
    ] {
        let f = Fixture::new(move |r| {
            if r.path.ends_with("/approvals") {
                reply(status, Value::Null)
            } else {
                observation_reply(r)
            }
        })
        .await;
        let sc = f.provider();
        let o = sc.observe_review(&repo(), 7).await.unwrap();
        assert_eq!(o.availability.approvals, expected);
        assert_eq!(o.details.review.number, 7);
        assert_eq!(o.reviews, None);
        assert_eq!(o.signals.review_decision, None);
        if status == 429 {
            assert!(matches!(
                sc.merge_requirements(&repo(), 7).await,
                Err(Error::RateLimited(_))
            ));
            assert_eq!(sc.rate_limit_status().await.unwrap().remaining, Some(0));
        }
    }
}

#[tokio::test]
async fn unreadable_discussions_are_not_reported_as_zero_and_missing_ci_is_not_green() {
    let f = Fixture::new(|r| {
        if r.path.contains("/discussions") {
            return reply(403, Value::Null);
        }
        if r.path.ends_with("/merge_requests/7") {
            return reply(200, mr());
        }
        observation_reply(r)
    })
    .await;
    let sc = f.provider();
    let o = sc.observe_review(&repo(), 7).await.unwrap();
    assert_eq!(o.conversation_count, None);
    assert_eq!(o.availability.discussions, ProviderAvailability::Restricted);
    assert!(!o.signals.checks_known);
    assert_eq!(
        o.signals.checks[0].state,
        intent_sourcecontrol::CheckState::Pending
    );
    assert!(
        !sc.mergeability(&repo(), 7)
            .await
            .unwrap()
            .required_checks_passed
    );
    assert!(sc.pr_observation(&repo(), 7).await.is_err());
}

#[tokio::test]
async fn unknown_project_pair_is_rejected_before_any_request() {
    let f = Fixture::new(|_| reply(500, Value::Null)).await;
    let sc = f.provider();
    for (id, instance) in [(0, INSTANCE), (91, INSTANCE), (73, "https://other.example")] {
        let mut source = identity("feature");
        source.project_id = id;
        source.instance_base_url = instance.into();
        assert!(sc
            .create_same_project(&repo(), input(), &source, &identity("main"))
            .await
            .is_err());
    }
    assert!(f.requests().is_empty());
}

fn create_reply(r: &Request) -> Reply {
    if r.method == "POST" {
        return reply(201, mr());
    }
    if r.path.contains("/repository/branches/") {
        let branch = r.path.rsplit('/').next().unwrap();
        return reply(200, json!({"name":branch,"commit":{"id":"remote-A"}}));
    }
    if r.path.contains("/merge_requests?") {
        return reply(200, json!([]));
    }
    reply(200, project())
}

#[tokio::test]
async fn create_uses_only_confirmed_same_project_remote_branches_and_reports_actual_sha() {
    let f = Fixture::new(create_reply).await;
    let result = f
        .provider()
        .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
        .await
        .unwrap();
    assert_eq!(result.outcome, ReviewCreateOutcome::Created);
    assert_eq!(result.details.review.head_sha.as_deref(), Some("remote-A"));
    let requests = f.requests();
    let writes: Vec<_> = requests.iter().filter(|r| r.method != "GET").collect();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].path, "/fixture/api/v4/projects/73/merge_requests");
    assert_eq!(writes[0].body["source_branch"], "feature");
    assert_eq!(writes[0].body["title"], "Requested new title");
    assert!(writes[0].body.get("target_project_id").is_none());
}

#[tokio::test]
async fn matching_open_review_is_reused_without_modifying_actual_title_body_or_draft() {
    let f = Fixture::new(|r| {
        if r.path.contains("/merge_requests?") {
            let mut v = mr();
            v["draft"] = json!(true);
            reply(200, json!([v]))
        } else {
            create_reply(r)
        }
    })
    .await;
    let result = f
        .provider()
        .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
        .await
        .unwrap();
    assert_eq!(result.outcome, ReviewCreateOutcome::Reused);
    assert!(result.details.review.draft);
    assert_eq!(result.details.review.title, "Actual existing title");
    assert_eq!(result.details.review.body.as_deref(), Some("Actual body"));
    assert!(f.requests().iter().all(|r| r.method == "GET"));
}

#[tokio::test]
async fn incomplete_matching_open_metadata_blocks_duplicate_post() {
    for missing in [
        "source_project_id",
        "target_project_id",
        "source_branch",
        "target_branch",
    ] {
        let f = Fixture::new(move |r| {
            if r.path.contains("/merge_requests?") {
                let mut v = mr();
                v.as_object_mut().unwrap().remove(missing);
                reply(200, json!([v]))
            } else {
                create_reply(r)
            }
        })
        .await;
        assert!(
            f.provider()
                .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
                .await
                .is_err(),
            "missing {missing}"
        );
        assert!(f.requests().iter().all(|r| r.method == "GET"));
    }
}

#[tokio::test]
async fn changed_project_or_missing_remote_branch_prevents_post() {
    for changed_project in [true, false] {
        let f = Fixture::new(move |r| {
            if changed_project && !r.path.contains("merge_requests") {
                let mut p = project();
                p["id"] = json!(99);
                return reply(200, p);
            }
            if r.path.contains("/repository/branches/") {
                return reply(404, Value::Null);
            }
            create_reply(r)
        })
        .await;
        assert!(f
            .provider()
            .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
            .await
            .is_err());
        assert!(f.requests().iter().all(|r| r.method == "GET"));
    }
}

#[tokio::test]
async fn uncertain_write_is_never_replayed_and_partial_response_is_not_a_full_review() {
    for status in [200, 408, 503] {
        let f = Fixture::new(move |r| {
            if r.method == "POST" {
                reply(status, json!({"iid":7,"web_url":"hidden"}))
            } else {
                create_reply(r)
            }
        })
        .await;
        let err = f
            .provider()
            .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::WriteUncertain,
                ..
            })
        ));
        assert_eq!(
            f.requests().iter().filter(|r| r.method == "POST").count(),
            1
        );
    }
}

#[tokio::test]
async fn malformed_optional_signals_never_claim_complete_or_passing_evidence() {
    for broken in [
        "pipeline",
        "job-status",
        "job-policy",
        "policy",
        "approvals",
        "discussion",
    ] {
        let f = Fixture::new(move |r| {
            let mut response = observation_reply(r);
            let mut body: Value = serde_json::from_str(&response.body).unwrap();
            match broken {
                "pipeline" if r.path.ends_with("/merge_requests/7") => {
                    body["head_pipeline"]["status"] = json!("future-status");
                }
                "job-status" if r.path.contains("/pipelines/") => {
                    body[0]["status"] = json!("future-status");
                }
                "job-policy" if r.path.contains("/pipelines/") => {
                    body[0].as_object_mut().unwrap().remove("allow_failure");
                }
                "policy" if r.path.ends_with("Team%2FSub%2FProject") => {
                    body.as_object_mut()
                        .unwrap()
                        .remove("only_allow_merge_if_pipeline_succeeds");
                }
                "approvals" if r.path.ends_with("/approvals") => {
                    body = json!({"approved":true});
                }
                "discussion" if r.path.contains("/discussions") => {
                    body[0]["notes"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("resolved");
                }
                _ => {}
            }
            response.body = body.to_string();
            response
        })
        .await;
        let o = f.provider().observe_review(&repo(), 7).await.unwrap();
        match broken {
            "policy" => {
                assert_eq!(o.availability.policy, ProviderAvailability::Unknown);
                assert!(!o.signals.checks_known);
            }
            "approvals" => {
                assert_eq!(o.availability.approvals, ProviderAvailability::Unknown);
                assert_eq!(o.signals.review_decision, None);
                assert_eq!(o.reviews, None);
            }
            "discussion" => {
                assert_eq!(o.availability.discussions, ProviderAvailability::Unknown);
                assert_eq!(o.threads, None);
                assert_eq!(o.conversation_count, None);
            }
            _ => {
                assert_eq!(
                    o.availability.checks,
                    ProviderAvailability::Unknown,
                    "{broken}"
                );
                assert!(!o.signals.checks_known, "{broken}");
            }
        }
    }
}

#[tokio::test]
async fn review_lists_reject_foreign_logical_urls() {
    let f = Fixture::new(|_| {
        let mut v = mr();
        v["web_url"] = json!("https://another.example/Team/Sub/Project/-/merge_requests/7");
        reply(200, json!([v]))
    })
    .await;
    assert!(f
        .provider()
        .list_prs(&repo(), PrQuery::default())
        .await
        .is_err());
}

#[tokio::test]
async fn retiring_the_injected_scope_between_branch_reads_prevents_create() {
    let retired = Arc::new(Mutex::new(None::<Arc<Credentials>>));
    let seen = retired.clone();
    let f = Fixture::new(move |r| {
        if r.path.ends_with("/repository/branches/main") {
            *seen.lock().unwrap().as_ref().unwrap().token.lock().unwrap() = None;
        }
        create_reply(r)
    })
    .await;
    *retired.lock().unwrap() = Some(f.credentials.clone());
    assert!(matches!(
        f.provider()
            .create_same_project(&repo(), input(), &identity("feature"), &identity("main"))
            .await,
        Err(Error::NotConfigured(_))
    ));
    assert!(f.requests().iter().all(|r| r.method == "GET"));
}

#[tokio::test]
async fn unsupported_write_capabilities_do_not_send_requests() {
    let f = Fixture::new(|_| reply(500, Value::Null)).await;
    let sc = f.provider();
    let cap = sc.capabilities();
    assert!(!cap.squash_merge);
    assert!(!cap.rebase_merge);
    assert!(!cap.review_required_changes);
    assert!(!cap.draft_prs);
    assert!(cap.issues && cap.check_runs);
    assert!(matches!(
        sc.update_branch(&repo(), 7).await,
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        sc.create_issue(&repo(), "title", None).await,
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        sc.resolve_thread("id").await,
        Err(Error::Unsupported(_))
    ));
    assert!(f.requests().is_empty());
}

#[tokio::test]
async fn project_issue_and_mr_search_keep_distinct_paths_and_pagination() {
    let f=Fixture::new(|r| if r.path.contains("/issues") {reply(200,json!([{"iid":7,"title":"Issue","description":null,"state":"opened","web_url":format!("{INSTANCE}/Team/Sub/Project/-/issues/7"),"author":{"username":"alice"},"created_at":"now","updated_at":"now"}]))} else {reply(200,json!([mr()]))}).await;
    let sc = f.provider();
    let issues = sc
        .list_issues(
            &repo(),
            IssueQuery {
                search: Some("needle".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let prs = sc
        .list_prs(
            &repo(),
            PrQuery {
                search: Some("needle".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(issues.items[0].number, prs.items[0].number);
    assert_ne!(issues.items[0].url, prs.items[0].url);
}

#[tokio::test]
async fn ci_policy_distinguishes_skipped_optional_and_missing_pipelines() {
    for (required, skipped_allowed, pipeline_status, known, passed) in [
        (true, Some(true), Some("skipped"), true, true),
        (true, Some(false), Some("skipped"), true, false),
        (true, None, Some("skipped"), false, false),
        (false, None, None, true, true),
        (true, None, None, false, false),
        (true, None, Some("future-state"), false, false),
    ] {
        let f = Fixture::new(move |r| {
            if r.path.ends_with("/merge_requests/7") {
                let mut v = mr();
                if let Some(status) = pipeline_status {
                    v["head_pipeline"] = json!({"id":19,"status":status});
                }
                return reply(200, v);
            }
            if r.path.contains("/pipelines/") {
                return reply(200, json!([]));
            }
            if r.path.ends_with("Team%2FSub%2FProject") {
                let mut v = project();
                v["only_allow_merge_if_pipeline_succeeds"] = json!(required);
                v["only_allow_merge_if_all_discussions_are_resolved"] = json!(false);
                v["allow_merge_on_skipped_pipeline"] = json!(skipped_allowed);
                return reply(200, v);
            }
            observation_reply(r)
        })
        .await;
        let sc = f.provider();
        let o = sc.observe_review(&repo(), 7).await.unwrap();
        assert_eq!(o.signals.checks_known, known, "{pipeline_status:?}");
        assert_eq!(
            sc.mergeability(&repo(), 7)
                .await
                .unwrap()
                .required_checks_passed,
            passed,
            "{pipeline_status:?}"
        );
    }
}

#[tokio::test]
async fn file_and_commit_status_reads_encode_refs_and_follow_the_confirmed_commit() {
    let f = Fixture::new(|r| {
        if r.path.contains("/repository/files/") {
            return reply(200, json!({"encoding":"base64", "content":"aGVsbG8=\n"}));
        }
        if r.path.contains("/repository/commits/") {
            return if r.path.contains("/statuses") {
                reply(200, json!([{"name":"external","status":"success","target_url":"https://ci.example"}]))
            } else { reply(200, json!({"id":"confirmed-sha"})) };
        }
        if r.path.contains("/pipelines/19/jobs") {
            return reply(200, json!([{"name":"build","status":"success","allow_failure":false}]));
        }
        let mut response = reply(200, json!([{"id":19,"status":"success"}]));
        response.headers.push(("x-next-page".into(), String::new()));
        response
    }).await;
    let sc = f.provider();
    assert_eq!(
        sc.get_file_content(&repo(), "a/b.txt", Some("feature/a"))
            .await
            .unwrap(),
        Some("hello".into())
    );
    let checks = sc.check_runs(&repo(), "feature/a").await.unwrap();
    assert_eq!(
        checks.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["external", "GitLab pipeline", "build"]
    );
    let requests = f.requests();
    assert!(requests[0]
        .path
        .contains("/files/a%2Fb.txt?ref=feature%2Fa"));
    assert!(requests[1].path.ends_with("/commits/feature%2Fa"));
    assert!(requests[2].path.contains("/commits/confirmed-sha/statuses"));
    assert!(requests[3].path.contains("sha=confirmed-sha"));
}

#[tokio::test]
async fn discussion_comments_keep_reply_and_instance_qualified_thread_identity() {
    let f = Fixture::new(|_| reply(200, json!([{"id":"discussion-1","notes":[
        {"id":1,"type":"DiffNote","body":"Root","author":{"username":"alice"},"position":{"new_path":"src/a.rs","new_line":9},"resolvable":true,"resolved":false,"system":false,"created_at":"now","updated_at":"now"},
        {"id":2,"type":"DiffNote","body":"Reply","author":{"username":"bob"},"resolvable":false,"system":false,"created_at":"now","updated_at":"now"}
    ]}]))).await;
    let sc = f.provider();
    let comments = sc
        .list_review_comments(&repo(), 7, PageParams::first(20))
        .await
        .unwrap();
    assert_eq!(comments.items[1].in_reply_to_id, Some(1));
    let threads = sc
        .get_review_threads(&repo(), 7, PageParams::first(20))
        .await
        .unwrap();
    assert!(!threads.items[0].is_resolved);
    assert_eq!(threads.items[0].comments[1].path, "src/a.rs");
    assert_eq!(threads.items[0].comments[1].line, Some(9));
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(threads.items[0].id.strip_prefix("gitlab:").unwrap())
        .unwrap();
    let identity: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        identity,
        json!([INSTANCE, "Team/Sub/Project", 7, "discussion-1"])
    );
}

#[tokio::test]
async fn detail_identity_is_additive_and_absence_never_implies_reuse() {
    let f = Fixture::new(|_| reply(200, mr())).await;
    let mut details = f.provider().review_details(&repo(), 7).await.unwrap();
    let json = serde_json::to_value(&details).unwrap();
    assert_eq!(
        json["source"],
        json!({"instanceBaseUrl":INSTANCE,"projectId":73,"branch":"feature"})
    );
    assert!(json["review"].get("source").is_none());
    assert!(json["review"].get("target").is_none());
    assert!(details.matches_open(&identity("feature"), &identity("main")));
    details.source.as_mut().unwrap().project_id = 0;
    details.target.as_mut().unwrap().project_id = 0;
    let mut source = identity("feature");
    let mut target = identity("main");
    source.project_id = 0;
    target.project_id = 0;
    assert!(!details.matches_open(&source, &target));
}

#[tokio::test]
async fn malformed_and_transient_primary_responses_do_not_invalidate_as_denial() {
    for (status, body, kind) in [
        (200, "not-json", ProviderFailureKind::Unknown),
        (503, "unavailable", ProviderFailureKind::Transient),
        (408, "timeout", ProviderFailureKind::Transient),
    ] {
        let f = Fixture::new(move |_| Reply {
            status,
            headers: vec![],
            body: body.into(),
        })
        .await;
        let error = f.provider().get_pr(&repo(), 7).await.unwrap_err();
        assert!(matches!(error, Error::Provider(ProviderFailure{kind:k,..}) if k==kind));
    }
}

#[tokio::test]
async fn optional_authentication_rejection_fails_the_aggregate_after_primary_success() {
    for endpoint in ["/approvals", "/pipelines/19/jobs", "/discussions"] {
        let f = Fixture::new(move |r| {
            if r.path.split('?').next().unwrap().ends_with(endpoint) {
                reply(401, json!({"message":"credential rejected"}))
            } else {
                observation_reply(r)
            }
        })
        .await;
        let sc = f.provider();
        let error = sc.observe_review(&repo(), 7).await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::Provider(ProviderFailure {
                    kind: ProviderFailureKind::CredentialRejected,
                    status: Some(401)
                })
            ),
            "{endpoint}: {error}"
        );
        assert!(f.requests()[0].path.ends_with("/merge_requests/7"));
        assert!(matches!(
            sc.pr_observation(&repo(), 7).await,
            Err(Error::Provider(ProviderFailure {
                kind: ProviderFailureKind::CredentialRejected,
                ..
            }))
        ));
    }
}

#[tokio::test]
async fn parent_project_denial_cannot_hide_as_missing_optional_policy() {
    for (status, kind) in [
        (401, ProviderFailureKind::CredentialRejected),
        (403, ProviderFailureKind::ResourceDenied),
        (404, ProviderFailureKind::ResourceDenied),
    ] {
        let f = Fixture::new(move |r| {
            if r.path.ends_with("Team%2FSub%2FProject") {
                reply(status, Value::Null)
            } else {
                observation_reply(r)
            }
        })
        .await;
        let sc = f.provider();
        let error = sc.observe_review(&repo(), 7).await.unwrap_err();
        assert!(
            matches!(error, Error::Provider(ProviderFailure{kind:k,status:Some(s)}) if k==kind && s==status)
        );
        assert!(matches!(sc.branch_rules(&repo(), "main").await,
            Err(Error::Provider(ProviderFailure{kind:k,..})) if k==kind));
        assert!(f.requests().iter().all(|r| !r.path.contains("/approvals")));
    }
}

#[tokio::test]
async fn optional_rate_limit_retains_retry_after_despite_later_success_headers() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let f = Fixture::new(move |r| {
        if r.path.ends_with("/approvals") {
            let mut response = reply(429, Value::Null);
            response.headers = vec![
                ("retry-after".into(), "120".into()),
                ("ratelimit-reset".into(), (start + 5).to_string()),
                ("ratelimit-limit".into(), "600".into()),
            ];
            response
        } else {
            let mut response = observation_reply(r);
            response.headers = vec![
                ("ratelimit-remaining".into(), "500".into()),
                ("ratelimit-reset".into(), (start + 5).to_string()),
            ];
            response
        }
    })
    .await;
    let sc = f.provider();
    let observation = sc.observe_review(&repo(), 7).await.unwrap();
    assert_eq!(
        observation.availability.approvals,
        ProviderAvailability::RateLimited
    );
    let quota = sc.rate_limit_status().await.unwrap();
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(quota.limit, Some(600));
    assert!(quota.reset_at.is_some_and(|at| at >= start + 120));
    assert!(matches!(
        sc.merge_requirements(&repo(), 7).await,
        Err(Error::RateLimited(_))
    ));
    assert!(matches!(
        sc.pr_observation(&repo(), 7).await,
        Err(Error::RateLimited(_))
    ));
}

#[tokio::test]
async fn optional_admission_rejection_propagates_without_an_upstream_request() {
    let credentials = Arc::new(Mutex::new(None::<Arc<Credentials>>));
    let captured = credentials.clone();
    let f = Fixture::new(move |r| {
        if r.path.ends_with("Team%2FSub%2FProject") {
            *captured
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .token
                .lock()
                .unwrap() = None;
        }
        observation_reply(r)
    })
    .await;
    *credentials.lock().unwrap() = Some(f.credentials.clone());
    assert!(matches!(
        f.provider().observe_review(&repo(), 7).await,
        Err(Error::NotConfigured(_))
    ));
    assert_eq!(f.requests().len(), 2);
    assert!(f.requests().iter().all(|r| !r.path.contains("/approvals")));
}
