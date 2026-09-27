//! Final local HTTP admission uses the same real source and reader as the
//! parent fixture. Target/feed and ACP-response production remain fixtures;
//! no wire/daemon entry, `NativeRead`, adapter or final Git worker is installed.

use std::pin::Pin;

use super::*;

async fn reached<F: Future>(operation: &mut Pin<Box<F>>, entered: &Notify) {
    timeout(BUDGET, async {
        tokio::select! {
            () = entered.notified() => {},
            _ = operation => panic!("HTTP completed before the required original authority fence"),
        }
    })
    .await
    .expect("original authority fence was not reached");
}

async fn hold_final<F: Future>(operation: &mut Pin<Box<F>>, entered: &Notify, release: &Notify) {
    reached(operation, entered).await;
    release.notify_one(); // Consume the first, token-release fence.
    reached(operation, entered).await; // The SAME authority, now after preparation.
}

#[intent_test_macros::daemon_test]
async fn final_original_source_retirement_prevents_http_after_token_release() {
    for change in 0..3 {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let binding = f.auth.request().binding;
                let entered = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let mut call = Box::pin(RepositoryDispatchStamp::probe_credential_fence(
                    entered.clone(),
                    release.clone(),
                    provider.get_repo("group", "project"),
                ));
                hold_final(&mut call, &entered, &release).await;
                assert_eq!(project_calls(server), 0);
                drop(
                    timeout(BUDGET, f.auth.service.gitlab_credential_gate.lock())
                        .await
                        .unwrap(),
                );
                match change {
                    0 => f.owner.retirement().retire(),
                    1 => {
                        f.auth
                            .service
                            .store
                            .archive_workspace_detaching_guests(
                                &f.git.workspace.id,
                                "2026-09-27T21:00:00Z",
                            )
                            .await
                            .unwrap();
                    }
                    _ => {
                        f.auth
                            .service
                            .store
                            .set_agent_session_model(
                                &f.git.workspace.id,
                                &f.agent,
                                "replacement-model",
                                None,
                                "2026-09-27T21:00:00Z",
                            )
                            .await
                            .unwrap();
                    }
                }
                release.notify_one();
                assert!(matches!(
                    call.await,
                    Err(intent_sourcecontrol::Error::AdmissionRetired)
                ));
                assert_eq!(project_calls(server), 0);
                assert_eq!(f.auth.request().binding, binding);
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::CreatePr).await,
                    Err(AdmissionError::Retired)
                ));
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn final_same_or_different_account_replacement_refuses_the_original_prepared_request() {
    for token in ["stored-pat", "pat-second"] {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let original = f.auth.request().binding;
                let entered = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let mut call = Box::pin(RepositoryDispatchStamp::probe_credential_fence(
                    entered.clone(),
                    release.clone(),
                    provider.get_repo("group", "project"),
                ));
                hold_final(&mut call, &entered, &release).await;
                f.auth
                    .service
                    .gitlab_connect_pat(server.host.clone(), token.into())
                    .await
                    .unwrap();
                let replacement = f.auth.request().binding;
                assert_ne!(replacement.scope, original.scope);
                assert_eq!(
                    replacement.account == original.account,
                    token == "stored-pat"
                );
                release.notify_one();
                assert!(matches!(
                    call.await,
                    Err(intent_sourcecontrol::Error::AdmissionRetired)
                ));
                assert_eq!(project_calls(server), 0);
                assert_eq!(f.auth.request().binding, replacement);
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn final_verified_refresh_rejects_old_header_then_reacquires_same_binding() {
    let server = Server::new().await;
    let auth = Fixture::unadopted(&server).await;
    auth.service
        .gitlab_secret_store
        .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
        .unwrap();
    auth.service
        .gitlab_secret_store
        .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
        .unwrap();
    auth.service
        .reconcile_gitlab_repository_binding()
        .await
        .unwrap();
    let f = Interaction::from_auth(auth, &server).await;
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let provider = f.callback(&stamp).into_provider().unwrap();
            let original = f.auth.request();
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let mut call = Box::pin(RepositoryDispatchStamp::probe_credential_fence(
                entered.clone(),
                release.clone(),
                provider.get_repo("group", "project"),
            ));
            hold_final(&mut call, &entered, &release).await;
            f.auth
                .service
                .gitlab_secret_store
                .store(EXPIRES_AT_SECRET_ACCOUNT, "0")
                .unwrap();
            f.auth
                .service
                .stored_proof_token(&Target::Gitlab {
                    host: server.host.clone(),
                })
                .await
                .unwrap();
            let fresh = f.auth.request();
            assert_eq!(fresh.binding, original.binding);
            assert!(fresh.secret_revision > original.secret_revision);
            release.notify_one();
            assert!(matches!(
                call.await,
                Err(intent_sourcecontrol::Error::AdmissionUnavailable(
                    intent_sourcecontrol::error::AdmissionUnavailable::SecretChanged
                ))
            ));
            assert_eq!(project_calls(server), 0);
            *server.control.expected_project_token.lock().unwrap() = Some("rotated");
            provider.get_repo("group", "project").await.unwrap();
            assert_eq!(project_calls(server), 1);
            assert_eq!(server.control.exchanges.load(Ordering::SeqCst), 2);
            Ok(())
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn final_revalidation_observes_retryable_git_unavailability_after_token_release() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let provider = f.callback(&stamp).into_provider().unwrap();
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let mut call = Box::pin(RepositoryDispatchStamp::probe_credential_fence(
                entered.clone(),
                release.clone(),
                provider.get_repo("group", "project"),
            ));
            reached(&mut call, &entered).await;
            let moved = f.git.dir.path().join("http-unavailable-worktree");
            std::fs::rename(&f.git.path, &moved).unwrap();
            release.notify_one();
            assert!(matches!(
                call.await,
                Err(intent_sourcecontrol::Error::AdmissionUnavailable(
                    intent_sourcecontrol::error::AdmissionUnavailable::AuthorityUnavailable
                ))
            ));
            assert_eq!(project_calls(server), 0);
            std::fs::rename(&moved, &f.git.path).unwrap();
            *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
            provider.get_repo("group", "project").await.unwrap();
            assert_eq!(project_calls(server), 1);
            Ok(())
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn final_cancelled_or_detached_preparation_never_borrows_later_authority() {
    for detached in [false, true] {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let entered = Arc::new(Notify::new());
                let release = Arc::new(Notify::new());
                let (send, receive) = tokio::sync::oneshot::channel();
                let (e, r) = (entered.clone(), release.clone());
                let mut task = Box::pin(tokio::spawn(async move {
                    let result = RepositoryDispatchStamp::probe_credential_fence(
                        e,
                        r,
                        provider.get_repo("group", "project"),
                    )
                    .await;
                    let _ = send.send(result);
                }));
                hold_final(&mut task, &entered, &release).await;
                if detached {
                    drop(task);
                    f.owner.retirement().retire();
                    release.notify_one();
                    assert!(matches!(
                        timeout(BUDGET, receive).await.unwrap().unwrap(),
                        Err(intent_sourcecontrol::Error::AdmissionRetired)
                    ));
                } else {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                    assert!(receive.await.is_err());
                    release.notify_one();
                }
                assert_eq!(project_calls(server), 0);
                drop(
                    timeout(BUDGET, f.auth.service.gitlab_credential_gate.lock())
                        .await
                        .unwrap(),
                );
                if !detached {
                    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
                    f.callback(&stamp)
                        .into_provider()
                        .unwrap()
                        .get_repo("group", "project")
                        .await
                        .unwrap();
                    assert_eq!(project_calls(server), 1);
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn admitted_success_survives_original_retirement_but_cannot_send_again() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    *server.control.pause.lock().unwrap() = Some("/api/v4/projects/");
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let provider = f.callback(&stamp).into_provider().unwrap();
            let call = provider.get_repo("group", "project");
            let retirement = async {
                server.entered().await;
                assert_eq!(project_calls(server), 1);
                f.owner.retirement().retire();
                server.control.release.notify_one();
            };
            let (result, ()) = tokio::join!(call, retirement);
            assert_eq!(result.unwrap().name, "project");
            assert!(matches!(
                provider.get_repo("group", "project").await,
                Err(intent_sourcecontrol::Error::AdmissionRetired)
            ));
            assert_eq!(project_calls(server), 1);
            drop(stamp);
            assert!(matches!(
                admission.execution().unwrap().outcome,
                NativeReviewOutcome::Uncertain { .. }
            ));
            Ok(())
        }
    })
    .await
    .unwrap();
}

struct Replies {
    seen: std::sync::Mutex<Vec<(String, String)>>,
    project_status: std::sync::atomic::AtomicU16,
    post_status: std::sync::atomic::AtomicU16,
    pause: std::sync::atomic::AtomicU8,
    entered: Notify,
    release: Notify,
}

struct EffectServer {
    fixture: Server,
    replies: Arc<Replies>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for EffectServer {
    fn drop(&mut self) {
        self.replies.release.notify_waiters();
        self.task.abort();
    }
}

impl EffectServer {
    async fn new() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut fixture = Server::new().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        // Choose the disposable endpoint BEFORE Services creates/adopts its
        // original owner. No installed reader, binding or endpoint is retargeted.
        fixture.host = intent_sourcecontrol::GitlabHost::parse("gitlab.test")
            .unwrap()
            .with_api_origin(&endpoint)
            .unwrap();
        fixture.descriptor = intent_sourcecontrol::GitlabDescriptor::with_loopback_endpoint(
            fixture.descriptor.instance().clone(),
            &endpoint,
        )
        .unwrap();
        let replies = Arc::new(Replies {
            seen: std::sync::Mutex::new(Vec::new()),
            project_status: std::sync::atomic::AtomicU16::new(200),
            post_status: std::sync::atomic::AtomicU16::new(201),
            pause: std::sync::atomic::AtomicU8::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let control = replies.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let control = control.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let Ok(size) = socket.read(&mut chunk).await else {
                            return;
                        };
                        if size == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..size]);
                        assert!(bytes.len() < 65_536);
                        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    let (key, value) = line.split_once(':')?;
                                    key.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let mut parts = request.split_whitespace();
                    let method = parts.next().unwrap();
                    let path = parts.next().unwrap();
                    control
                        .seen
                        .lock()
                        .unwrap()
                        .push((method.into(), path.into()));
                    let project = path == "/api/v4/projects/group%2Fproject";
                    let post = method == "POST" && path == "/api/v4/projects/42/merge_requests";
                    let should_pause = if post { 2 } else { u8::from(project) };
                    if should_pause != 0
                        && control
                            .pause
                            .compare_exchange(should_pause, 0, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        control.entered.notify_one();
                        control.release.notified().await;
                    }
                    let (status, body) = if path == "/api/v4/user" {
                        (
                            200,
                            serde_json::json!({"id": if request.contains("pat-second") {43} else {42}, "username":"fixture", "name":"Fixture"}),
                        )
                    } else if project {
                        (
                            control.project_status.load(Ordering::SeqCst),
                            serde_json::json!({"id":42,"name":"project","path":"project","path_with_namespace":"group/project","web_url":"https://gitlab.test/forge/group/project","visibility":"private","default_branch":"main"}),
                        )
                    } else if post {
                        (
                            control.post_status.load(Ordering::SeqCst),
                            serde_json::json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4","title":"observed title","state":"opened","draft":false,"source_branch":"main","target_branch":"target","source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"}),
                        )
                    } else if path.contains("/repository/branches/") {
                        (
                            200,
                            serde_json::json!({"name":path.rsplit('/').next().unwrap(), "commit":{"id":"provider-observed-sha"}}),
                        )
                    } else if path.starts_with("/api/v4/projects/group%2Fproject/merge_requests?") {
                        (200, serde_json::json!([]))
                    } else {
                        (404, serde_json::json!({}))
                    };
                    let extra = if status == 429 {
                        "Retry-After: 60\r\n"
                    } else {
                        ""
                    };
                    let body = body.to_string();
                    let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n{body}", body.len());
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

    fn calls(&self, method: &str) -> usize {
        self.replies
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, p)| m == method && p.starts_with("/api/v4/projects/"))
            .count()
    }
}

async fn run_create<T: Send, F, Fut>(
    f: &Interaction,
    server: &Server,
    action: F,
) -> AdmissionResult<T>
where
    F: FnOnce(RepositoryOperationAdmission) -> Fut + Send,
    Fut: Future<Output = AdmissionResult<T>> + Send,
{
    let callback = f.owner.callback();
    let scope = McpRequestContext::capture(&callback);
    let mut result = None;
    with_caller(
        Caller::Agent {
            agent_id: f.agent.clone(),
        },
        with_wire_credential(
            None,
            scope.scope(Box::pin(async {
                let original =
                    OriginalRepositoryCaller::capture(RepositoryEntry::AgentCallback).unwrap();
                let mut input = f.input(server).await;
                // An explicit target projection fixture, before capture. The actual
                // source ref stays main; the provider separately verifies target below.
                input.facts.preparation.target.branch = "target".into();
                result = Some(
                    with_captured_repository_source(
                        &f.auth.service,
                        original,
                        "http-create-result".into(),
                        vec![NativeReviewStage::Commit, NativeReviewStage::CreatePr],
                        input,
                        action,
                    )
                    .await,
                );
            })),
        ),
    )
    .await;
    result.unwrap()
}

fn actual_review(
    details: intent_sourcecontrol::model::ReviewDetails,
) -> Box<intent_core::NativeReviewDetails> {
    use intent_core::{
        NativeReviewBranchIdentity, NativeReviewDetails, NativeReviewState, RepositoryProvider,
        RepositoryResourceKind, ReviewTarget,
    };

    let branch =
        |b: intent_sourcecontrol::model::ReviewBranchIdentity| NativeReviewBranchIdentity {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: b.instance_base_url,
            project_id: b.project_id.to_string(),
            project_path: b.project_path,
            branch: b.branch,
        };
    let target = details.target.as_ref().unwrap();
    assert_eq!(
        details.confirmed_state,
        Some(intent_sourcecontrol::model::ConfirmedReviewState::Open)
    );
    // Receipt-only test projection from the parsed actual response. This is
    // not an installed native/feed conversion or an inference from submitted UI.
    Box::new(NativeReviewDetails {
        resource: ReviewTarget {
            repository: RepositoryTarget {
                provider: RepositoryProvider::Gitlab,
                instance_base_url: target.instance_base_url.clone(),
                project_path: target.project_path.clone().unwrap(),
            },
            kind: RepositoryResourceKind::MergeRequest,
            number: details.review.number,
        },
        url: details.review.url,
        title: details.review.title,
        body: details.review.body,
        state: Some(NativeReviewState::Open),
        draft: details.confirmed_draft,
        source_branch: details.source.as_ref().map(|b| b.branch.clone()),
        target_branch: details.target.as_ref().map(|b| b.branch.clone()),
        source: details.source.map(branch),
        target: details.target.map(branch),
        author: (!details.review.author.is_empty()).then_some(details.review.author),
        mergeable: details.review.mergeable,
        mergeable_state: details.review.mergeable_state,
        head_sha: details.review.head_sha,
        created_at: Some(details.review.created_at),
        updated_at: Some(details.review.updated_at),
    })
}

#[intent_test_macros::daemon_test]
async fn admitted_create_preserves_actual_success_uncertainty_and_completed_git_receipt() {
    for status in [201_u16, 500, 0] {
        let server = EffectServer::new().await;
        let f = Interaction::new(&server.fixture).await;
        server
            .replies
            .post_status
            .store(if status == 0 { 201 } else { status }, Ordering::SeqCst);
        server.replies.pause.store(2, Ordering::SeqCst);
        run_create(&f, &server.fixture, |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::Commit).await;
                let hash = {
                    let repository = git2::Repository::open(&f.git.path).unwrap();
                    let parent = repository.head().unwrap().peel_to_commit().unwrap();
                    let tree = parent.tree().unwrap();
                    let signature = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
                    repository.commit(Some("HEAD"), &signature, &signature, "actual HTTP fixture commit", &tree, &[&parent]).unwrap().to_string()
                };
                classify_repository_completion(stamp, RepositoryCompletion::Committed { hash: hash.clone(), staging_after: None }).unwrap();
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let repo = intent_sourcecontrol::model::RepoRef { owner: "group".into(), name: "project".into() };
                // Expected branch identities are fixture input; the provider
                // confirms the project and both branches over actual HTTP.
                let identity = |branch: &str| intent_sourcecontrol::model::ReviewBranchIdentity {
                    instance_base_url: server.fixture.descriptor.instance().as_str().into(), project_id: 42,
                    project_path: Some("group/project".into()), branch: branch.into(),
                };
                let (source, target) = (identity("main"), identity("target"));
                let mut call = Box::pin(provider.create_same_project(&repo, intent_sourcecontrol::model::NewPullRequest {
                    title: "submitted title".into(), body: None, source_branch: "main".into(), target_branch: "target".into(), draft: false,
                }, &source, &target));
                reached(&mut call, &server.replies.entered).await;
                assert_eq!(server.calls("POST"), 1);
                f.owner.retirement().retire();
                let receipt = if status == 0 {
                    drop(call);
                    server.replies.release.notify_one();
                    drop(stamp);
                    admission.execution().unwrap()
                } else {
                    server.replies.release.notify_one();
                    let result = call.await;
                    if status == 201 {
                        let created = result.unwrap();
                        assert_eq!(created.outcome, intent_sourcecontrol::model::ReviewCreateOutcome::Created);
                        assert_eq!(created.details.review.title, "observed title");
                        classify_repository_completion(stamp, RepositoryCompletion::Created(actual_review(created.details))).unwrap()
                    } else {
                        assert!(matches!(&result, Err(intent_sourcecontrol::Error::Provider(failure)) if failure.kind == intent_sourcecontrol::error::ProviderFailureKind::WriteUncertain));
                        classify_repository_completion(stamp, RepositoryCompletion::Uncertain { message: result.unwrap_err().to_string() }).unwrap()
                    }
                };
                assert_eq!(receipt.git_receipts, vec![NativeReviewGitReceipt::Commit { commit_hash: hash.clone() }]);
                assert!(matches!(receipt.publication, NativeReviewPublication::Unknown { local_head_sha: Some(ref local), remote_source_sha: None } if local == &hash));
                if status == 201 {
                    assert!(matches!(receipt.outcome, NativeReviewOutcome::Created { ref review } if review.title == "observed title" && review.resource.number == 4));
                } else {
                    assert!(matches!(receipt.outcome, NativeReviewOutcome::Uncertain { stage: NativeReviewStage::CreatePr, .. }));
                }
                let before = server.calls("GET");
                assert!(matches!(provider.get_repo("group", "project").await, Err(intent_sourcecontrol::Error::AdmissionRetired)));
                assert_eq!(server.calls("POST"), 1);
                assert_eq!(server.calls("GET"), before);
                Ok(())
            }
        }).await.unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn admitted_response_receipts_keep_actual_rejection_or_quota_on_original_binding_only() {
    for (status, replacement) in [(401_u16, false), (401, true), (429, false), (429, true)] {
        let server = EffectServer::new().await;
        let f = Interaction::new(&server.fixture).await;
        server
            .replies
            .project_status
            .store(status, Ordering::SeqCst);
        server.replies.pause.store(1, Ordering::SeqCst);
        f.run(&server.fixture, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let original = f.auth.request().binding;
                let mut call = Box::pin(provider.get_repo("group", "project"));
                reached(&mut call, &server.replies.entered).await;
                if replacement {
                    f.auth.service.gitlab_connect_pat(server.fixture.host.clone(), "pat-second".into()).await.unwrap();
                }
                server.replies.release.notify_one();
                let result = call.await;
                if status == 401 {
                    assert!(matches!(result, Err(intent_sourcecontrol::Error::Provider(failure)) if failure.kind == intent_sourcecontrol::error::ProviderFailureKind::CredentialRejected));
                } else {
                    assert!(matches!(result, Err(intent_sourcecontrol::Error::RateLimited(_))));
                }
                let directory = f.auth.service.repository_connection_directory();
                if replacement {
                    assert_ne!(directory.binding().unwrap(), original);
                } else if status == 401 {
                    assert!(directory.binding().is_err());
                    assert_eq!(f.auth.service.gitlab_secret_store.load(SECRET_ACCOUNT).unwrap().as_deref(), Some("stored-pat"));
                } else {
                    assert_eq!(directory.binding().unwrap(), original);
                    assert!(matches!(provider.get_repo("group", "project").await, Err(intent_sourcecontrol::Error::AdmissionUnavailable(intent_sourcecontrol::error::AdmissionUnavailable::Backoff))));
                }
                assert_eq!(server.calls("GET"), 1);
                Ok(())
            }
        }).await.unwrap();
        if replacement {
            server.replies.project_status.store(200, Ordering::SeqCst);
            f.run(
                &server.fixture,
                vec![NativeReviewStage::CreatePr],
                |admission| {
                    let f = &f;
                    async move {
                        let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                        f.callback(&stamp)
                            .into_provider()
                            .unwrap()
                            .get_repo("group", "project")
                            .await
                            .unwrap();
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();
            assert_eq!(server.calls("GET"), 2);
        }
    }
}
