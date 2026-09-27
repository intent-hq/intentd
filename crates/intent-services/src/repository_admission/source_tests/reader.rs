//! The provider's original paired file/adoption fixture is composed with real
//! Services permissions, Store initialization, request leaves, Git and active
//! stage authority. Target/scope production and ACP responses remain explicit
//! fixtures. No wire/daemon producer, `NativeRead` or final worker is installed.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use intent_acp::mcp_server::request_context::McpRequestContext;
use intent_core::caller::{with_caller, with_wire_credential};
use intent_core::{
    AgentId, ExecutionScope, NativeReviewBranchTarget, NativeReviewGitReceipt, NativeReviewOutcome,
    NativeReviewPreparation, NativeReviewPublication, NativeReviewTransport,
    RepositoryAvailability, RepositoryContextRevision, RepositoryRootId, RepositoryRootKind,
    RepositoryTarget, RepositoryTargetContext, SavedReviewSelection, WorkspaceGitRoot,
    WorkspaceGitRootId, WorkspaceGitRootSource,
};
use intent_sourcecontrol::gitlab_token::{
    EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT, SECRET_ACCOUNT,
};
use intent_sourcecontrol::remote_project::RemoteInstance;
use intent_sourcecontrol::SourceControl;
use tokio::sync::Notify;
use tokio::time::timeout;

use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner, RepositoryPhysicalOwner,
};
use crate::repository_admission::{
    begin_repository_stage, classify_repository_completion, revalidate_repository_stage,
    RepositoryCompletion, RepositoryDispatchStamp, RepositoryEntry,
};
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use crate::repository_context_reader::read_repository_context_with_resolver;
use crate::repository_credentials::authority::RepositoryCredentialTransport;
use crate::repository_credentials::{
    BoundGitlabRequestCredentials, RepositoryAuthorityRequest, RepositoryCredentialAdmission,
    RepositoryCredentialError, RepositoryCredentialUse,
};
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{
    Fixture, PausedRead, Server,
};
use crate::source_control_auth_ops::Target;

const BUDGET: Duration = Duration::from_secs(5);

struct Interaction {
    auth: Fixture,
    git: GitFixture,
    agent: AgentId,
    owner: RepositoryPhysicalOwner,
    root: RepositoryRootId,
}

impl Interaction {
    async fn new(server: &Server) -> Self {
        Self::from_auth(Fixture::new(server).await, server).await
    }

    async fn from_auth(auth: Fixture, server: &Server) -> Self {
        let mut git = GitFixture::new().await;
        // Reuse the disposable Git setup, but every exercised source and writer
        // uses the SAME Store owned by the adopted Services instance.
        auth.service
            .store
            .insert_workspace(&git.workspace)
            .await
            .unwrap();
        git.store = auth.service.store.clone();
        let url = format!(
            "{}/group/project.git",
            server.descriptor.instance().as_str()
        );
        git.git(&git.path, &["remote", "add", "origin", &url]);
        let agent = super::tests::agent(&git).await;
        let registry = auth.service.repository_lifecycle_registry().await.unwrap();
        let cloned = (*auth.service).clone();
        assert!(Arc::ptr_eq(
            &registry,
            &cloned.repository_lifecycle_registry().await.unwrap()
        ));
        assert!(Arc::ptr_eq(
            &auth.service.repository_connection_directory(),
            &cloned.repository_connection_directory()
        ));
        let creator = RepositoryCreationOwner::allocate(
            &registry,
            &auth.service.store,
            git.workspace.id.clone(),
            agent.clone(),
            RepositoryCreationIntent::Loaded {
                session_id: "original-acp".into(),
            },
        )
        .unwrap();
        let pending = creator.callback().capture();
        let owner = creator
            .initialize(&auth.service.store, || async { Ok("original-acp".into()) })
            .await
            .unwrap();
        with_caller(
            Caller::Agent {
                agent_id: agent.clone(),
            },
            async {
                assert!(matches!(
                    pending.source_lifetime(),
                    Err(AdmissionError::Unavailable)
                ));
            },
        )
        .await;
        let root = git.root();
        Self {
            auth,
            git,
            agent,
            owner,
            root,
        }
    }

    async fn fresh_owner(&mut self) {
        let registry = self
            .auth
            .service
            .repository_lifecycle_registry()
            .await
            .unwrap();
        self.owner = RepositoryCreationOwner::allocate(
            &registry,
            &self.auth.service.store,
            self.git.workspace.id.clone(),
            self.agent.clone(),
            RepositoryCreationIntent::Loaded {
                session_id: "original-acp".into(),
            },
        )
        .unwrap()
        .initialize(&self.auth.service.store, || async {
            Ok("original-acp".into())
        })
        .await
        .unwrap();
    }

    async fn input(&self, server: &Server) -> RepositorySourceInput {
        let selected = self.auth.request();
        let snapshot = self
            .auth
            .service
            .store
            .repository_workspace_authority_snapshot(&self.git.workspace.id)
            .await
            .unwrap();
        // This projection is a fixture, not an installed scope/feed producer.
        // It uses actual daemon, connection and durable counters, never a made-up
        // directory generation; permission still comes from the source gates.
        let scope = ExecutionScope {
            daemon_id: selected.binding.daemon_id.clone(),
            authority_scope_id: format!("fixture-agent:{}:{}", self.agent, self.git.workspace.id),
            authority_generation: snapshot.host_authorization_generation,
        };
        let revision = RepositoryContextRevision::new(
            &selected.binding.daemon_id,
            snapshot.workspace.revision.unwrap().get(),
        );
        let target = RepositoryTarget {
            provider: selected.binding.account.provider,
            instance_base_url: selected.binding.account.instance_base_url.clone(),
            project_path: "group/project".into(),
        };
        let context = RepositoryContextInput {
            scope: scope.clone(),
            revision: revision.clone(),
            roots: vec![AdmittedRepositoryRoot {
                root: self.root.clone(),
                path: self.git.path.clone(),
                saved_selection: SavedReviewSelection::Automatic,
                explicit_target: None,
                targets: vec![RepositoryTargetContext {
                    target: target.clone(),
                    provider_project_id: None,
                    connection: Some(selected.binding.scope.clone()),
                    availability: RepositoryAvailability::Connected,
                    capabilities: Vec::new(),
                }],
            }],
        };
        let resolver = CanonicalRemoteResolver::new(
            vec![RemoteInstance::gitlab(server.descriptor.instance().clone())],
            Vec::new(),
        )
        .unwrap();
        let environment = self.git.environment();
        let read =
            read_repository_context_with_resolver(&context, &resolver, &environment).unwrap();
        let remote = &read.context.roots[0].remotes[0];
        let private = &read.private_roots[0];
        let branch = NativeReviewBranchTarget {
            repository: target.clone(),
            provider_project_id: None,
            connection: Some(selected.binding.scope.clone()),
            branch: "main".into(),
        };
        let push_destinations = private.remotes[0].push.clone();
        RepositorySourceInput {
            facts: RepositoryOperationFacts {
                preparation: NativeReviewPreparation {
                    operation_id: "reader-interaction".into(),
                    scope: scope.clone(),
                    context_revision: revision,
                    root: self.root.clone(),
                    worktree_id: self.git.workspace.id.to_string(),
                    source: branch.clone(),
                    target: branch,
                    local_head_sha: read.context.roots[0].head_sha.clone(),
                    transport: Some(NativeReviewTransport {
                        remote_name: remote.name.clone(),
                        fetch_urls: remote.fetch.iter().map(|e| e.url.clone()).collect(),
                        push_urls: remote.push.iter().map(|e| e.url.clone()).collect(),
                    }),
                },
                worktree_path: self.git.path.clone(),
                git_dir: read.change_inputs[0].git_dir.clone(),
                common_dir: read.change_inputs[0].common_dir.clone(),
                source_ref: private.source_ref.clone().unwrap(),
                staging_fingerprint: None,
                fetch_destinations: private.remotes[0].fetch.clone(),
                push_destinations: push_destinations.clone(),
                credential_requests: vec![
                    RepositoryAuthorityRequest {
                        execution: scope.clone(),
                        connection: selected.binding.scope.clone(),
                        target: target.clone(),
                        use_kind: RepositoryCredentialUse::NativePush,
                        allowed_transport: RepositoryCredentialTransport::GitHttps(
                            push_destinations,
                        ),
                    },
                    RepositoryAuthorityRequest {
                        execution: scope,
                        connection: selected.binding.scope,
                        target,
                        use_kind: RepositoryCredentialUse::NativeReviewCreate,
                        allowed_transport: RepositoryCredentialTransport::GitlabApi(
                            server.descriptor.clone(),
                        ),
                    },
                ],
            },
            context,
            resolver,
            environment,
            before_lock: None,
        }
    }

    async fn run<T: Send, F, Fut>(
        &self,
        server: &Server,
        stages: Vec<NativeReviewStage>,
        action: F,
    ) -> AdmissionResult<T>
    where
        F: FnOnce(RepositoryOperationAdmission) -> Fut + Send,
        Fut: Future<Output = AdmissionResult<T>> + Send,
    {
        let callback = self.owner.callback();
        let scope = McpRequestContext::capture(&callback);
        let mut result = None;
        with_caller(
            Caller::Agent {
                agent_id: self.agent.clone(),
            },
            with_wire_credential(
                None,
                scope.scope(Box::pin(async {
                    let original =
                        OriginalRepositoryCaller::capture(RepositoryEntry::AgentCallback).unwrap();
                    let input = self.input(server).await;
                    result = Some(
                        with_captured_repository_source(
                            &self.auth.service,
                            original,
                            "original-reader-request".into(),
                            stages,
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

    fn admission(&self, stamp: &RepositoryDispatchStamp) -> RepositoryCredentialAdmission {
        let (request, authority) = stamp.credential_authority().unwrap();
        let directory = self.auth.service.repository_connection_directory();
        directory
            .admit(&self.auth.request().binding, request, authority)
            .unwrap()
    }

    fn callback(&self, stamp: &RepositoryDispatchStamp) -> BoundGitlabRequestCredentials {
        BoundGitlabRequestCredentials::new(
            self.auth.service.repository_connection_directory(),
            self.admission(stamp),
            self.auth.service.gitlab_repository_secret_reader().unwrap(),
            BUDGET,
        )
        .unwrap()
    }
}

async fn start(
    admission: &RepositoryOperationAdmission,
    stage: NativeReviewStage,
) -> RepositoryDispatchStamp {
    begin_repository_stage(revalidate_repository_stage(admission, stage).await.unwrap()).unwrap()
}

fn project_calls(server: &Server) -> usize {
    server
        .control
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.starts_with("/api/v4/projects/"))
        .count()
}

#[intent_test_macros::daemon_test]
async fn original_reader_uses_real_agent_store_git_and_active_create_or_push_authority() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    for stage in [NativeReviewStage::Push, NativeReviewStage::CreatePr] {
        f.run(&server, vec![stage], |admission| {
            let f = &f;
            async move {
                let stamp = start(&admission, stage).await;
                if stage == NativeReviewStage::CreatePr {
                    let provider = f.callback(&stamp).into_provider().unwrap();
                    let repo = provider.get_repo("group", "project").await.unwrap();
                    assert_eq!(
                        (repo.owner.as_str(), repo.name.as_str()),
                        ("group", "project")
                    );
                } else {
                    let request = f.admission(&stamp);
                    f.auth
                        .service
                        .repository_connection_directory()
                        .acquire_exact(
                            &request,
                            f.auth
                                .service
                                .gitlab_repository_secret_reader()
                                .unwrap()
                                .as_ref(),
                            BUDGET,
                        )
                        .await
                        .unwrap();
                }
                assert!(admission.execution().unwrap().git_receipts.is_empty());
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
    assert_eq!(project_calls(&server), 1);
}

#[intent_test_macros::daemon_test]
async fn actual_file_wait_then_store_or_original_owner_retirement_prevents_provider_dispatch() {
    for change in 0..3 {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let binding = f.auth.request().binding;
                let mut paused = PausedRead::install(&f.auth);
                let call = provider.get_repo("group", "project");
                let mutation = async {
                    paused.entered().await;
                    match change {
                        0 => {
                            f.auth
                                .service
                                .store
                                .set_agent_session_model(
                                    &f.git.workspace.id,
                                    &f.agent,
                                    "changed-model",
                                    None,
                                    "2026-09-27T21:00:00Z",
                                )
                                .await
                                .unwrap();
                        }
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
                        _ => f.owner.retirement().retire(),
                    }
                    paused.resume();
                };
                let (result, ()) = tokio::join!(call, mutation);
                assert!(matches!(
                    result,
                    Err(intent_sourcecontrol::Error::AdmissionRetired)
                ));
                assert_eq!(project_calls(server), 0);
                assert_eq!(
                    f.auth.request().binding,
                    binding,
                    "local retirement is not an auth rejection"
                );
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
async fn actual_observed_denial_then_restore_never_revives_original_reader_authority() {
    for change in 0..3 {
        let server = Server::new().await;
        let mut f = Interaction::new(&server).await;
        *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                match change {
                    0 => {
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
                    1 => {
                        f.auth.service.pending_workspace_deletes.schedule(
                            f.git.workspace.id.to_string(),
                            "2026-09-27T21:00:00Z".into(),
                            |_| tokio::spawn(async {}),
                        );
                    }
                    _ => {
                        f.git.git(&f.git.path, &["checkout", "--detach", "main"]);
                    }
                }
                assert!(provider.get_repo("group", "project").await.is_err());
                match change {
                    0 => {
                        f.auth
                            .service
                            .store
                            .unarchive_workspace_if_archived(
                                &f.git.workspace.id,
                                "2026-09-27T21:01:00Z",
                            )
                            .await
                            .unwrap();
                    }
                    1 => {
                        f.auth
                            .service
                            .pending_workspace_deletes
                            .cancel(f.git.workspace.id.as_str());
                    }
                    _ => {
                        f.git.git(&f.git.path, &["checkout", "main"]);
                    }
                }
                assert!(matches!(
                    provider.get_repo("group", "project").await,
                    Err(intent_sourcecontrol::Error::AdmissionRetired)
                ));
                assert_eq!(project_calls(server), 0);
                Ok(())
            }
        })
        .await
        .unwrap();
        f.fresh_owner().await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
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
        })
        .await
        .unwrap();
        assert_eq!(project_calls(&server), 1);
    }
}

#[intent_test_macros::daemon_test]
async fn registered_root_delete_recreate_during_file_read_retires_original_authority() {
    let server = Server::new().await;
    let mut f = Interaction::new(&server).await;
    let root = WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: f.git.workspace.id.clone(),
        path: f.git.path.to_str().unwrap().into(),
        source: WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: "2026-09-27T21:00:00Z".into(),
        updated_at: "2026-09-27T21:00:00Z".into(),
    };
    f.auth
        .service
        .store
        .upsert_workspace_git_root(&root)
        .await
        .unwrap();
    f.root.kind = RepositoryRootKind::Registered {
        git_root_id: root.id.clone(),
    };
    f.fresh_owner().await;
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        let root = &root;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let provider = f.callback(&stamp).into_provider().unwrap();
            let mut paused = PausedRead::install(&f.auth);
            let mutation = async {
                paused.entered().await;
                f.auth
                    .service
                    .store
                    .delete_workspace_git_root(&root.id)
                    .await
                    .unwrap();
                f.auth
                    .service
                    .store
                    .upsert_workspace_git_root(root)
                    .await
                    .unwrap();
                paused.resume();
            };
            let (result, ()) = tokio::join!(provider.get_repo("group", "project"), mutation);
            assert!(matches!(
                result,
                Err(intent_sourcecontrol::Error::AdmissionRetired)
            ));
            assert!(matches!(
                provider.get_repo("group", "project").await,
                Err(intent_sourcecontrol::Error::AdmissionRetired)
            ));
            assert_eq!(project_calls(server), 0);
            Ok(())
        }
    })
    .await
    .unwrap();
    f.fresh_owner().await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
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
    })
    .await
    .unwrap();
    assert_eq!(project_calls(&server), 1);
}

#[intent_test_macros::daemon_test]
async fn transient_git_read_failure_preserves_original_reader_request_and_account() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    *server.control.expected_project_token.lock().unwrap() = Some("stored-pat");
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let request = f.admission(&stamp);
            let directory = f.auth.service.repository_connection_directory();
            let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
            let moved = f.git.dir.path().join("unavailable-worktree");
            std::fs::rename(&f.git.path, &moved).unwrap();
            assert!(matches!(
                directory
                    .acquire_exact(&request, reader.as_ref(), BUDGET)
                    .await,
                Err(RepositoryCredentialError::AuthorityUnavailable)
            ));
            std::fs::rename(&moved, &f.git.path).unwrap();
            directory
                .acquire_exact(&request, reader.as_ref(), BUDGET)
                .await
                .unwrap();
            f.callback(&stamp)
                .into_provider()
                .unwrap()
                .get_repo("group", "project")
                .await
                .unwrap();
            assert_eq!(project_calls(server), 1);
            Ok(())
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn actual_pat_and_settings_replacement_never_rebind_an_existing_reader_stage() {
    use intent_core::WorkspaceApi as _;

    for change in 0..3 {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let original = f.auth.request().binding;
                if change == 2 {
                    let changes = serde_json::json!([{
                        "path": "sourceControl.gitlab.oauthClientId",
                        "value": "changed-client"
                    }]);
                    f.auth.service.settings_update(changes).await.unwrap();
                    assert_eq!(
                        f.auth.registry.get("sourceControl.gitlab.oauthClientId"),
                        Some(serde_json::json!("changed-client"))
                    );
                } else {
                    f.auth
                        .service
                        .gitlab_connect_pat(
                            server.host.clone(),
                            if change == 0 {
                                "stored-pat"
                            } else {
                                "pat-second"
                            }
                            .into(),
                        )
                        .await
                        .unwrap();
                    let replacement = f.auth.request().binding;
                    assert_ne!(replacement.scope, original.scope);
                    assert_eq!(replacement.account == original.account, change == 0);
                }
                assert!(provider.get_repo("group", "project").await.is_err());
                assert_eq!(project_calls(server), 0);
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn actual_secret_content_mismatch_stays_local_and_restoration_cannot_republish_proof() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
        let f = &f;
        let server = &server;
        async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let request = f.admission(&stamp);
            let directory = f.auth.service.repository_connection_directory();
            let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
            let original = f.auth.request();
            f.auth
                .service
                .gitlab_secret_store
                .store(SECRET_ACCOUNT, "external-change")
                .unwrap();
            assert!(matches!(
                directory
                    .acquire_exact(&request, reader.as_ref(), BUDGET)
                    .await,
                Err(RepositoryCredentialError::SecretMismatch)
            ));
            f.auth
                .service
                .gitlab_secret_store
                .store(SECRET_ACCOUNT, "stored-pat")
                .unwrap();
            assert!(matches!(
                directory
                    .acquire_exact(&request, reader.as_ref(), BUDGET)
                    .await,
                Err(RepositoryCredentialError::Unverified)
            ));
            assert_eq!(f.auth.request(), original);
            assert_eq!(project_calls(server), 0);
            Ok(())
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn cancelled_actual_file_read_retains_original_gate_until_worker_exit() {
    for cancelled in [false, true] {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| {
            let f = &f;
            let server = &server;
            async move {
                let stamp = start(&admission, NativeReviewStage::CreatePr).await;
                let provider = f.callback(&stamp).into_provider().unwrap();
                let request = f.admission(&stamp);
                let directory = f.auth.service.repository_connection_directory();
                let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
                let mut paused = PausedRead::install(&f.auth);
                let task = tokio::spawn(async move {
                    directory
                        .acquire_exact(
                            &request,
                            reader.as_ref(),
                            if cancelled {
                                BUDGET
                            } else {
                                Duration::from_millis(100)
                            },
                        )
                        .await
                });
                paused.entered().await;
                if cancelled {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    assert!(matches!(
                        task.await.unwrap(),
                        Err(RepositoryCredentialError::TimedOut)
                    ));
                }
                assert!(timeout(
                    Duration::from_millis(30),
                    f.auth.service.gitlab_credential_gate.lock()
                )
                .await
                .is_err());
                f.owner.retirement().retire();
                paused.resume();
                drop(
                    timeout(BUDGET, f.auth.service.gitlab_credential_gate.lock())
                        .await
                        .unwrap(),
                );
                assert!(matches!(
                    provider.get_repo("group", "project").await,
                    Err(intent_sourcecontrol::Error::AdmissionRetired)
                ));
                assert_eq!(project_calls(server), 0);
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn real_post_load_authority_fence_rechecks_original_retirement_and_directory() {
    for replace_account in [false, true] {
        let server = Server::new().await;
        let f = Interaction::new(&server).await;
        f.run(&server, vec![NativeReviewStage::CreatePr], |admission| { let f = &f; let server = &server; async move {
            let stamp = start(&admission, NativeReviewStage::CreatePr).await;
            let provider = f.callback(&stamp).into_provider().unwrap();
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let read = RepositoryDispatchStamp::probe_credential_fence(entered.clone(), release.clone(), provider.get_repo("group", "project"));
            let mutation = async {
                timeout(BUDGET, entered.notified()).await.unwrap();
                drop(timeout(BUDGET, f.auth.service.gitlab_credential_gate.lock()).await.expect("file reader must release the original gate before actual R authority"));
                if replace_account {
                    f.auth.service.gitlab_connect_pat(server.host.clone(), "pat-second".into()).await.unwrap();
                } else {
                    f.owner.retirement().retire();
                }
                release.notify_one();
            };
            let (result, ()) = tokio::join!(read, mutation);
            assert!(matches!(result, Err(intent_sourcecontrol::Error::AdmissionRetired)));
            assert_eq!(project_calls(server), 0);
            Ok(())
        }}).await.unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn verified_same_binding_refresh_reacquires_real_file_without_resetting_quota() {
    for quota in [false, true] {
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
                let request = f.admission(&stamp);
                let directory = f.auth.service.repository_connection_directory();
                let reader = f.auth.service.gitlab_repository_secret_reader().unwrap();
                let old = f.auth.request();
                let ticket = directory
                    .acquire_exact(&request, reader.as_ref(), BUDGET)
                    .await
                    .unwrap();
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
                assert_eq!(fresh.binding, old.binding);
                assert!(fresh.secret_revision > old.secret_revision);
                assert!(!directory
                    .reject_current_credential(ticket.dispatch_stamp())
                    .unwrap());
                if quota {
                    assert!(directory
                        .record_backoff(
                            ticket.dispatch_stamp(),
                            Instant::now() + Duration::from_secs(60)
                        )
                        .unwrap());
                    assert!(matches!(
                        directory
                            .acquire_exact(&request, reader.as_ref(), BUDGET)
                            .await,
                        Err(RepositoryCredentialError::Backoff)
                    ));
                    assert_eq!(project_calls(server), 0);
                } else {
                    directory
                        .acquire_exact(&request, reader.as_ref(), BUDGET)
                        .await
                        .unwrap();
                    *server.control.expected_project_token.lock().unwrap() = Some("rotated");
                    f.callback(&stamp)
                        .into_provider()
                        .unwrap()
                        .get_repo("group", "project")
                        .await
                        .unwrap();
                    assert_eq!(project_calls(server), 1);
                }
                assert_eq!(server.control.exchanges.load(Ordering::SeqCst), 2);
                Ok(())
            }
        })
        .await
        .unwrap();
    }
}

#[intent_test_macros::daemon_test]
async fn completed_local_commit_receipt_survives_reader_retirement_and_uncertain_stage() {
    let server = Server::new().await;
    let f = Interaction::new(&server).await;
    f.run(&server, vec![NativeReviewStage::Commit, NativeReviewStage::CreatePr], |admission| { let f = &f; let server = &server; async move {
        let stamp = start(&admission, NativeReviewStage::Commit).await;
        let hash = {
            let repo = git2::Repository::open(&f.git.path).unwrap();
            let parent = repo.head().unwrap().peel_to_commit().unwrap();
            let tree = parent.tree().unwrap();
            let signature = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
            let oid = repo.commit(Some("HEAD"), &signature, &signature, "local reader fixture", &tree, &[&parent]).unwrap();
            oid.to_string()
        };
        classify_repository_completion(stamp, RepositoryCompletion::Committed { hash: hash.clone(), staging_after: None }).unwrap();
        let stamp = start(&admission, NativeReviewStage::CreatePr).await;
        let provider = f.callback(&stamp).into_provider().unwrap();
        f.owner.retirement().retire();
        assert!(matches!(provider.get_repo("group", "project").await, Err(intent_sourcecontrol::Error::AdmissionRetired)));
        drop(stamp);
        let receipt = admission.execution().unwrap();
        assert_eq!(receipt.git_receipts, vec![NativeReviewGitReceipt::Commit { commit_hash: hash.clone() }]);
        assert!(matches!(receipt.outcome, NativeReviewOutcome::Uncertain { stage: NativeReviewStage::CreatePr, .. }));
        assert!(matches!(receipt.publication, NativeReviewPublication::Unknown { local_head_sha: Some(ref local), remote_source_sha: None } if local == &hash));
        assert_eq!(project_calls(server), 0);
        Ok(())
    }}).await.unwrap();
}
