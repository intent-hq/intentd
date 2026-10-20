//! Real original Services/Store/Git and adopted file-owner reads through ACP.
//! HTTP replies and the initial ACP completion are disposable fixtures; no R
//! permission, admission, revision or credential proof is injected.
use super::*;
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationIntent, RepositoryCreationOwner, RepositoryPhysicalOwner,
};
use crate::repository_admission::read_request::RepositoryReadOwner;
use crate::repository_admission::request_context::RepositoryCallbackContext;
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{Fixture, Server};
use intent_acp::mcp_server::WorkspaceMcpServer;
use intent_core::{AgentId, AgentSession, WorkspaceApi};
use intent_js::BoxFuture;
use intent_sourcecontrol::GitlabDescriptor;
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::time::timeout;
const BUDGET: Duration = Duration::from_secs(10);
const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";
const PROJECT: &str = "/api/v4/projects/group%2Fproject";

#[path = "tests/native_monitor.rs"]
pub(crate) mod native_monitor;
#[path = "tests/polling.rs"]
mod polling;

#[cfg(target_os = "linux")]
pub(crate) async fn admit_original_monitor(
    services: &Arc<Services>,
    retained: Arc<RepositoryReadRequest>,
    workspace: &WorkspaceId,
    target: ReviewTarget,
) -> AdmissionResult<Arc<MonitorAuthority>> {
    let settled = services
        .gitlab_repository_settled_connection()
        .map_err(|_| AdmissionError::Unavailable)?;
    MonitorAuthority::admit_original(services, retained, workspace, target, settled, None).await
}

pub(crate) struct ActualRead {
    pub(crate) auth: Fixture,
    pub(crate) git: GitFixture,
    pub(crate) agent: AgentId,
    pub(crate) owner: RepositoryPhysicalOwner,
}

impl ActualRead {
    pub(crate) async fn new(server: &ReadServer) -> Self {
        let auth = Fixture::new(&server.fixture).await;
        Self::from_auth(server, auth).await
    }

    async fn oauth(server: &ReadServer) -> Self {
        use intent_sourcecontrol::gitlab_token::{
            EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT,
        };
        let auth = Fixture::unadopted(&server.fixture).await;
        auth.service
            .gitlab_secret_store
            .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
            .unwrap();
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 7200;
        auth.service
            .gitlab_secret_store
            .store(EXPIRES_AT_SECRET_ACCOUNT, &expires.to_string())
            .unwrap();
        auth.service
            .reconcile_gitlab_repository_binding()
            .await
            .unwrap();
        Self::from_auth(server, auth).await
    }

    async fn from_auth(server: &ReadServer, mut auth: Fixture) -> Self {
        let mut git = GitFixture::new().await;
        auth.service = Arc::new(
            auth.service
                .as_ref()
                .clone()
                .with_workspaces_root(git.dir.path().join("owned-workspaces")),
        );
        auth.service
            .store
            .insert_workspace(&git.workspace)
            .await
            .unwrap();
        git.store = auth.service.store.clone();
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "origin",
                &format!(
                    "{}/group/project.git",
                    server.fixture.descriptor.instance().as_str()
                ),
            ],
        );
        let agent = AgentId::new();
        let session: AgentSession = serde_json::from_value(json!({
            "id":agent,"workspaceId":git.workspace.id,"name":"original-read",
            "status":"active","createdAt":"2026-09-28T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"
        }))
        .unwrap();
        auth.service
            .store
            .insert_agent_session(&session)
            .await
            .unwrap();
        let registry = auth.service.repository_lifecycle_registry().await.unwrap();
        let owner = RepositoryCreationOwner::allocate(
            &registry,
            &auth.service.store,
            git.workspace.id.clone(),
            agent.clone(),
            RepositoryCreationIntent::FirstSet,
        )
        .unwrap()
        .initialize(&auth.service.store, || async {
            Ok("original ACP completion fixture".into())
        })
        .await
        .unwrap();
        Self {
            auth,
            git,
            agent,
            owner,
        }
    }

    pub(crate) fn context(&self) -> RepositoryCallbackContext {
        self.owner.callback().with_read_owner(
            RepositoryReadOwner::capture(self.auth.service.clone())
                .map(RepositoryReadOwner::with_review_monitoring),
        )
    }

    fn legacy_server(&self) -> WorkspaceMcpServer {
        let context = self
            .owner
            .callback()
            .with_read_owner(RepositoryReadOwner::capture(self.auth.service.clone()));
        WorkspaceMcpServer::new(self.api(), self.git.workspace.id.clone())
            .with_caller_agent_id(Some(self.agent.clone()))
            .with_request_context(Arc::new(context))
    }

    pub(crate) fn server(&self) -> WorkspaceMcpServer {
        WorkspaceMcpServer::new(self.api(), self.git.workspace.id.clone())
            .with_caller_agent_id(Some(self.agent.clone()))
            .with_request_context(Arc::new(self.context()))
    }

    pub(crate) fn api(&self) -> Arc<dyn WorkspaceApi> {
        self.auth.service.clone()
    }
}

/// Test-only raw cache evidence for response attribution controls. Public entry
/// and delivery tests use the original Services implementation directly.
struct TestReadApi(Arc<Services>, Option<Arc<Mutex<Vec<String>>>>);
impl WorkspaceApi for TestReadApi {
    fn agent_is_retired(&self, agent: AgentId) -> BoxFuture<'_, bool> {
        self.0.agent_is_retired(agent)
    }
    fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, intent_core::Result<intent_core::Workspace>> {
        self.0.get_workspace(id)
    }
    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.0.settings_get(path)
    }
    fn pr_state(
        &self,
        workspace: WorkspaceId,
        number: u64,
        _: Option<String>,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        let captured = CapturedReview::capture(&self.0, workspace, number);
        Box::pin(async move {
            match captured.read(&self.0).await.map_err(|_| refused())? {
                ReadOutcome::Unsupported => Err(intent_core::Error::Unsupported(
                    "Addressed review monitoring unavailable".into(),
                )),
                ReadOutcome::Github(repo) => {
                    Ok(json!({"ordinary":format!("{}/{}",repo.owner,repo.name)}))
                }
                ReadOutcome::Managed { target, result } => {
                    if let Some(log) = &self.1 {
                        log.lock().unwrap().push(format!("{result:?}"));
                    }
                    (*result).map(|read| json!({"resource":target,"observation": read.value, "fetched":read.fetched})).map_err(|_| refused())
                }
            }
        })
    }
}

pub(crate) fn call(code: &str) -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_api","arguments":{"summary":"Original NativeRead fixture","code":code}}})
}

pub(crate) async fn run(server: &WorkspaceMcpServer, code: &str) -> Value {
    timeout(BUDGET, server.handle_message(&call(code)))
        .await
        .unwrap()
        .unwrap()
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_unsupported_recipient_refuses_before_provider_read() {
    for namespace in ["pr", "mr"] {
        for selector in ["", ", { gitRootId: 'foreign-root' }"] {
            let provider = ReadServer::new().await;
            let f = ActualRead::new(&provider).await;
            let before = provider.count();
            let reply = run(
                &f.legacy_server(),
                &format!("return await ws.{namespace}.snapshot(4{selector});"),
            )
            .await;
            assert_eq!(
                provider.count(),
                before,
                "unsupported recipient must not cause a qualified read: {reply}"
            );
            assert_eq!(reply["result"]["isError"], true, "{reply}");
            assert!(reply
                .to_string()
                .contains("Addressed review monitoring unavailable"));
            assert!(!reply.to_string().contains("actual review"));
        }
    }
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_explicit_target_never_falls_back_to_selected_root() {
    for namespace in ["pr", "mr"] {
        let provider = ReadServer::new().await;
        let f = ActualRead::new(&provider).await;
        let before = provider.count();
        let target = json!({
            "repository": { "provider": "gitlab", "instanceBaseUrl":
                provider.fixture.descriptor.instance().as_str(), "projectPath": "other/nested/project" },
            "kind": "merge-request", "number": 4,
        });
        let reply = run(
            &f.legacy_server(),
            &format!("return await host({{method:'{namespace}.snapshot',args:{{prNumber:4,target:{target}}}}});"),
        ).await;
        assert_eq!(
            provider.count(),
            before,
            "explicit target was ignored: {reply}"
        );
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(!reply.to_string().contains("actual review"));
    }
}

#[intent_test_macros::daemon_test]
async fn actual_native_read_uses_original_member_git_file_http_and_both_transfers() {
    let server = ReadServer::new().await;
    let f = ActualRead::new(&server).await;
    let before = std::fs::read(f.auth.service.gitlab_secret_store.path()).unwrap();
    let reply = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert!(server.count() > 0);
    assert_eq!(
        before,
        std::fs::read(f.auth.service.gitlab_secret_store.path()).unwrap()
    );
    assert!(!reply.to_string().contains("stored-pat"));
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_explicit_gitlab_snapshot_without_remote_uses_both_aliases() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    f.git.git(&f.git.path, &["remote", "remove", "origin"]);
    let target = json!({
        "repository": { "provider": "gitlab", "instanceBaseUrl":
            provider.fixture.descriptor.instance().as_str(), "projectPath": "group/project" },
        "kind": "merge-request", "number": 4,
    });
    for namespace in ["pr", "mr"] {
        let reply = run(
            &f.server(),
            &format!("return await ws.{namespace}.snapshot(4,{{target:{target}}});"),
        )
        .await;
        assert_eq!(reply["result"]["isError"], false, "{reply}");
        assert!(reply.to_string().contains("actual review"), "{reply}");
        assert!(reply.to_string().contains("merge-request"), "{reply}");
    }
    let implicit = run(&f.server(), "return await ws.mr.snapshot(4);").await;
    assert_eq!(
        implicit["result"]["isError"], true,
        "per-call target changed defaults: {implicit}"
    );
}

pub(crate) fn routing_json(reply: &Value) -> Value {
    assert_eq!(reply["result"]["isError"], false, "{reply}");
    serde_json::from_str(reply["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_aliases_share_record_and_cancel_captured_target() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    f.auth
        .registry
        .apply(&[
            ("workspaceApi.toonOutput".into(), json!(false)),
            ("workspaceApi.maxOutputChars".into(), json!(0)),
        ])
        .unwrap();
    let target = json!({
        "repository": { "provider": "gitlab", "instanceBaseUrl":
            provider.fixture.descriptor.instance().as_str(), "projectPath": "group/project" },
        "kind": "merge-request", "number": 4,
    });
    let first = routing_json(
        &run(
            &f.server(),
            &format!("return await ws.pr.monitor(4,{{target:{target}}});"),
        )
        .await,
    );
    let second = routing_json(
        &run(
            &f.server(),
            &format!("return await ws.mr.monitor(4,{{target:{target}}});"),
        )
        .await,
    );
    assert_eq!(first["ok"], true);
    assert_eq!(
        first["monitor"]["monitorId"],
        second["monitor"]["monitorId"]
    );
    assert_eq!(first["monitor"]["resource"], target);
    assert_eq!(
        first["monitor"]["lastSnapshot"]["availability"]["checks"],
        "available"
    );
    assert!(!first.to_string().contains("monitorBinding"));
    f.git.git(
        &f.git.path,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/group/project.git",
        ],
    );
    let before = provider.count();
    let listed = routing_json(&run(&f.server(), "return await ws.mr.monitors();").await);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["resource"], target);
    let cancelled = routing_json(
        &run(
            &f.server(),
            &format!("return await ws.mr.unmonitor(4,{{target:{target}}});"),
        )
        .await,
    );
    assert_eq!(
        cancelled["monitor"]["monitorId"],
        first["monitor"]["monitorId"]
    );
    assert_eq!(cancelled["monitor"]["state"], "cancelled");
    assert_eq!(
        provider.count(),
        before,
        "listing/cancellation must not fetch the provider"
    );
    let id = intent_core::PrMonitorId::from(first["monitor"]["monitorId"].as_str().unwrap());
    let stored = f
        .auth
        .service
        .store
        .get_qualified_pr_monitor(&id)
        .await
        .unwrap();
    assert!(matches!(
        stored.target(),
        intent_store::PersistedMonitorTarget::Resolved { .. }
    ));
    assert!(stored
        .monitor()
        .last_snapshot
        .as_ref()
        .unwrap()
        .contains("monitorBinding"));
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_legacy_recipient_has_no_effects_or_owner_disclosure() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    for namespace in ["pr", "mr"] {
        for method in ["monitor", "unmonitor"] {
            let reply = run(
                &f.legacy_server(),
                &format!("return await ws.{namespace}.{method}(4);"),
            )
            .await;
            assert_eq!(reply["result"]["isError"], true, "{reply}");
            assert!(
                reply
                    .to_string()
                    .contains("Addressed review monitoring unavailable"),
                "{reply}"
            );
            assert!(!reply.to_string().contains("ownerAgentId"));
        }
    }
    assert_eq!(provider.count(), 0);
    assert!(f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap()
        .is_empty());
}

pub(crate) fn routing_options(f: &ActualRead) {
    f.auth
        .registry
        .apply(&[
            ("workspaceApi.toonOutput".into(), json!(false)),
            ("workspaceApi.maxOutputChars".into(), json!(0)),
        ])
        .unwrap();
}

async fn routing_actor(
    f: &ActualRead,
    parent: Option<&AgentId>,
) -> (AgentId, RepositoryPhysicalOwner, WorkspaceMcpServer) {
    let id = AgentId::new();
    let session: AgentSession = serde_json::from_value(json!({
        "id":id,"workspaceId":f.git.workspace.id,"name":"routing-contender",
        "parentAgentId":parent,"status":"active","createdAt":"2026-10-09T00:00:00Z",
        "updatedAt":"2026-10-09T00:00:00Z"
    }))
    .unwrap();
    let services = &f.auth.service;
    services.store.insert_agent_session(&session).await.unwrap();
    let registry = services.repository_lifecycle_registry().await.unwrap();
    let owner = RepositoryCreationOwner::allocate(
        &registry,
        &services.store,
        f.git.workspace.id.clone(),
        id.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap()
    .initialize(&services.store, || async {
        Ok("routing ACP fixture".into())
    })
    .await
    .unwrap();
    let context = owner.callback().with_read_owner(
        RepositoryReadOwner::capture(services.clone())
            .map(RepositoryReadOwner::with_review_monitoring),
    );
    let server = WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
        .with_caller_agent_id(Some(id.clone()))
        .with_request_context(Arc::new(context));
    (id, owner, server)
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_live_owner_refusal_then_orphan_or_parent_adoption() {
    for parent_takeover in [false, true] {
        let provider = ReadServer::new().await;
        let f = ActualRead::new(&provider).await;
        routing_options(&f);
        let (child, _physical, endpoint) =
            routing_actor(&f, parent_takeover.then_some(&f.agent)).await;
        let first = routing_json(&run(&endpoint, "return await ws.mr.monitor(4);").await);
        let before = provider.count();
        let refusal = routing_json(&run(&f.server(), "return await ws.pr.monitor(4);").await);
        assert_eq!(refusal["refused"], true, "{refusal}");
        assert_eq!(refusal["ownerAgentId"], json!(child));
        assert_eq!(refusal["resource"], first["monitor"]["resource"]);
        assert_eq!(
            provider.count(),
            before,
            "live-owner refusal must not fetch"
        );
        let status = if parent_takeover {
            intent_core::AgentStatus::RuntimeIdle
        } else {
            intent_core::AgentStatus::Error
        };
        f.auth
            .service
            .store
            .set_agent_session_status(
                &f.git.workspace.id,
                &child,
                status,
                false,
                "2026-10-09T00:01:00Z",
                None,
            )
            .await
            .unwrap();
        let adopted = routing_json(&run(&f.server(), "return await ws.pr.monitor(4);").await);
        assert_eq!(adopted["ok"], true, "{adopted}");
        assert_eq!(adopted["adoptedFrom"], json!(child));
        assert_eq!(
            adopted["monitor"]["monitorId"],
            first["monitor"]["monitorId"]
        );
        assert!(
            routing_json(&run(&endpoint, "return await ws.mr.monitors();").await)
                .as_array()
                .unwrap()
                .is_empty()
        );
        let stopped = routing_json(&run(&f.server(), "return await ws.mr.unmonitor(4);").await);
        assert_eq!(stopped["ok"], true);
    }
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_changes_during_provider_await_leave_no_registration() {
    for retire_original in [false, true] {
        let provider = ReadServer::new().await;
        let f = ActualRead::new(&provider).await;
        routing_options(&f);
        provider.pause(MR);
        let endpoint = f.server();
        let pending =
            tokio::spawn(async move { run(&endpoint, "return await ws.mr.monitor(4);").await });
        provider.entered().await;
        if retire_original {
            f.owner.retirement().retire();
        } else {
            f.git.git(
                &f.git.path,
                &[
                    "remote",
                    "set-url",
                    "origin",
                    "https://gitlab.test/forge/other/project.git",
                ],
            );
        }
        provider.resume();
        let reply = pending.await.unwrap();
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(!reply.to_string().contains("actual review"), "{reply}");
        assert!(f
            .auth
            .service
            .store
            .list_pr_monitors_by_agent(&f.agent)
            .await
            .unwrap()
            .is_empty());
    }
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_feature_off_raw_dispatch_has_no_effects() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    f.auth
        .registry
        .apply(&[("agentFeatures.prMonitor".into(), json!(false))])
        .unwrap();
    let endpoint =
        f.server()
            .with_agent_features(intent_core::settings_file::AgentFeaturesSettings {
                pr_monitor: false,
                ..Default::default()
            });
    for namespace in ["pr", "mr"] {
        for method in ["monitor", "unmonitor", "monitors"] {
            let reply = run(&endpoint, &format!(
                "return await host({{method:'{namespace}.{method}',args:{{prNumber:4,gitRootId:'missing'}}}});"
            )).await;
            assert_eq!(reply["result"]["isError"], true, "{reply}");
            assert!(!reply.to_string().contains("ownerAgentId"));
        }
    }
    assert_eq!(provider.count(), 0);
    assert!(f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap()
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_quota_defers_baseline_but_retains_private_target() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    provider.status(MR, 429);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    assert!(registered["requirements"].is_null());
    assert!(registered["monitor"]["lastSnapshot"].is_null());
    assert!(registered["pausedUntil"].is_string());
    assert_eq!(
        registered["monitor"]["pausedUntil"],
        registered["pausedUntil"]
    );
    assert!(!registered.to_string().contains("monitorBinding"));
    assert!(
        f.auth.service.sweep_rate_limit_paused_until().is_none(),
        "GitLab quota must not pause GitHub"
    );
    let rows = f
        .auth
        .service
        .store
        .list_pr_monitors_by_agent(&f.agent)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].last_polled_at.is_none());
    let private: Value = serde_json::from_str(rows[0].last_snapshot.as_ref().unwrap()).unwrap();
    assert_eq!(
        private["monitorBinding"]["resource"],
        registered["monitor"]["resource"]
    );
    let before = provider.count();
    let stopped = routing_json(&run(&f.server(), "return await ws.pr.unmonitor(4);").await);
    assert_eq!(stopped["ok"], true);
    assert_eq!(provider.count(), before);
}

#[intent_test_macros::daemon_test]
async fn review_monitor_registration_cap_allows_same_record_but_refuses_new_target_before_read() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    f.auth
        .registry
        .apply(&[("prMonitor.maxPerAgent".into(), json!(1))])
        .unwrap();
    let first = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let rearmed = routing_json(&run(&f.server(), "return await ws.pr.monitor(4);").await);
    assert_eq!(
        rearmed["monitor"]["monitorId"],
        first["monitor"]["monitorId"]
    );
    let before = provider.count();
    let refused = run(&f.server(), "return await ws.mr.monitor(5);").await;
    assert_eq!(refused["result"]["isError"], true);
    assert!(refused.to_string().contains("max 1"), "{refused}");
    assert_eq!(provider.count(), before);
    let rows = routing_json(&run(&f.server(), "return await ws.pr.monitors();").await);
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["monitorId"], first["monitor"]["monitorId"]);
}

async fn routing_registered_root(f: &ActualRead) -> (GitFixture, intent_core::WorkspaceGitRootId) {
    let root = GitFixture::new().await;
    root.git(
        &root.path,
        &[
            "remote",
            "add",
            "origin",
            "https://gitlab.test/forge/group/project.git",
        ],
    );
    let id = intent_core::WorkspaceGitRootId::new();
    let record: intent_core::WorkspaceGitRoot = serde_json::from_value(json!({
        "id":id,"workspaceId":f.git.workspace.id,"path":root.path.to_str().unwrap(),
        "source":"agent","registeredByAgentIds":[f.agent],
        "createdAt":"2026-10-09T00:00:00Z","updatedAt":"2026-10-09T00:00:00Z"
    }))
    .unwrap();
    f.auth
        .service
        .store
        .upsert_workspace_git_root(&record)
        .await
        .unwrap();
    (root, id)
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_registered_root_observes_only_its_own_selection() {
    for change_selected_root in [false, true] {
        let provider = ReadServer::new().await;
        let f = ActualRead::new(&provider).await;
        routing_options(&f);
        let (_repo, root_id) = routing_registered_root(&f).await;
        provider.pause(MR);
        let endpoint = f.server();
        let code = format!("return await ws.mr.monitor(4,{{gitRootId:'{root_id}'}});");
        let pending = tokio::spawn(async move { run(&endpoint, &code).await });
        provider.entered().await;
        let root = RepositoryRootId {
            workspace_id: f.git.workspace.id.clone(),
            kind: if change_selected_root {
                RepositoryRootKind::Registered {
                    git_root_id: root_id,
                }
            } else {
                RepositoryRootKind::Primary
            },
        };
        let before = f
            .auth
            .service
            .store
            .repository_selection_snapshot(&root)
            .await
            .unwrap();
        f.auth
            .service
            .store
            .write_repository_selection(
                &before,
                intent_store::RepositorySelectionChange::ExplicitRemote {
                    remote_name: "origin".into(),
                },
            )
            .await
            .result
            .unwrap();
        provider.resume();
        let reply = pending.await.unwrap();
        if change_selected_root {
            assert_eq!(reply["result"]["isError"], true, "{reply}");
            assert!(f
                .auth
                .service
                .store
                .list_pr_monitors_by_agent(&f.agent)
                .await
                .unwrap()
                .is_empty());
        } else {
            let body = routing_json(&reply);
            assert_eq!(body["ok"], true);
            assert_eq!(
                body["monitor"]["resource"]["repository"]["projectPath"],
                "group/project"
            );
        }
    }
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_nested_project_port_and_prefix_are_captured_exactly() {
    let instance = "https://gitlab.test:8443/scm";
    let provider = ReadServer::at_instance(instance).await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let project = "Team/Nested/project";
    let url = format!("{instance}/{project}/-/merge_requests/4");
    {
        let mut bodies = provider.replies.bodies.lock().unwrap();
        bodies.insert(
            "/api/v4/projects/Team%2FNested%2Fproject".into(),
            json!({
                "id":55,"path_with_namespace":project,"only_allow_merge_if_pipeline_succeeds":false,
                "only_allow_merge_if_all_discussions_are_resolved":false
            }),
        );
        bodies.insert("/api/v4/projects/Team%2FNested%2Fproject/merge_requests/4".into(), json!({
            "iid":4,"web_url":url,"title":"nested review","state":"opened","draft":false,
            "source_branch":"feature","target_branch":"main","source_project_id":55,"target_project_id":55,
            "created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"
        }));
    }
    let target = json!({"repository":{"provider":"gitlab","instanceBaseUrl":instance,"projectPath":project},"kind":"merge-request","number":4});
    let created = routing_json(
        &run(
            &f.server(),
            &format!("return await ws.pr.monitor(4,{{target:{target}}});"),
        )
        .await,
    );
    assert_eq!(created["monitor"]["resource"], target);
    assert_eq!(created["monitor"]["url"], url);
    assert_eq!(created["monitor"]["repo"], project);
    let before = provider.count();
    for wrong_instance in [
        "https://gitlab.test/scm",
        "https://gitlab.test:8443",
        "https://gitlab.test:8444/scm",
    ] {
        let mut wrong = target.clone();
        wrong["repository"]["instanceBaseUrl"] = json!(wrong_instance);
        let reply = run(
            &f.server(),
            &format!("return await ws.mr.monitor(4,{{target:{wrong}}});"),
        )
        .await;
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(!reply.to_string().contains("ownerAgentId"));
    }
    assert_eq!(
        provider.count(),
        before,
        "other instances must not use this account"
    );
    assert_eq!(
        f.auth
            .service
            .store
            .list_pr_monitors_by_agent(&f.agent)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[intent_test_macros::daemon_test]
async fn review_monitor_routing_native_unqualified_controls_refuse_before_effects() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    let first = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId::from(first["monitor"]["monitorId"].as_str().unwrap());
    let before = provider.count();
    let listed = f
        .auth
        .service
        .pr_monitor_list(f.git.workspace.id.clone(), None)
        .await
        .unwrap();
    assert_eq!(listed, json!({"monitors":[]}));
    for check in [false, true] {
        let error = f
            .auth
            .service
            .pr_monitor_flush_pending(f.git.workspace.id.clone(), id.clone(), check)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("Addressed review monitoring unavailable"));
    }
    let error = f
        .auth
        .service
        .pr_monitor_cancel_by_id(f.git.workspace.id.clone(), id.clone())
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Addressed review monitoring unavailable"));
    assert_eq!(provider.count(), before);
    assert_eq!(
        f.auth
            .service
            .store
            .get_pr_monitor(&id)
            .await
            .unwrap()
            .state,
        intent_core::PrMonitorState::Active
    );
    assert_eq!(
        routing_json(&run(&f.server(), "return await ws.pr.unmonitor(4);").await)["ok"],
        true
    );
}

#[intent_test_macros::daemon_test]
async fn review_monitor_producer_fixture_preserves_availability_nulls_and_omissions() {
    let provider = ReadServer::new().await;
    let f = ActualRead::new(&provider).await;
    routing_options(&f);
    provider.status(&format!("{MR}/approvals"), 403);
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let listed = routing_json(&run(&f.server(), "return await ws.pr.monitors();").await);
    assert_eq!(listed[0], registered["monitor"]);
    assert_eq!(listed[0]["monitorAvailability"]["state"], "available");
    assert_eq!(
        listed[0]["lastSnapshot"]["availability"]["approvals"],
        "restricted"
    );
    assert!(listed[0]["lastSnapshot"]["approvals"]["have"].is_null());
    assert!(listed[0]["lastSnapshot"]["approvals"]
        .get("needed")
        .is_none());
    assert!(listed[0].get("pausedUntil").is_none());
    assert_eq!(listed[0]["lastSnapshot"]["checks"]["failingRequired"], 0);
    assert_eq!(listed[0]["lastSnapshot"]["checks"]["pendingRequired"], 0);
    assert_eq!(
        registered["requirements"]["checks"]["failingRequired"],
        json!([])
    );
    assert_eq!(
        registered["requirements"]["checks"]["pendingRequired"],
        json!([])
    );
    let fixture = json!({"registered":registered,"listed":listed});
    assert!(!fixture.to_string().contains("monitorBinding"));
    println!("ROUTING_PRODUCER_FIXTURE={fixture}");
}

async fn routing_pipeline_fixture(provider: &ReadServer, restricted: bool) -> ActualRead {
    let f = ActualRead::new(provider).await;
    routing_options(&f);
    let jobs = "/api/v4/projects/42/pipelines/18/jobs?page=1&per_page=100";
    {
        let mut bodies = provider.replies.bodies.lock().unwrap();
        bodies.insert(
            PROJECT.into(),
            json!({"id":42,"path_with_namespace":"group/project",
            "only_allow_merge_if_pipeline_succeeds":true,
            "only_allow_merge_if_all_discussions_are_resolved":false}),
        );
        bodies.insert(MR.into(), json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
            "title":"pipeline review","sha":"review-head","state":"opened","draft":false,"source_branch":"feature","target_branch":"main",
            "source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z",
            "head_pipeline":{"id":18,"project_id":42,"status":"failed","sha":"review-head"}}));
        bodies.insert(
            jobs.into(),
            json!([
                {"name":"failed required","status":"failed","allow_failure":false},
                {"name":"pending required","status":"running","allow_failure":false},
                {"name":"optional failure","status":"failed","allow_failure":true}
            ]),
        );
    }
    if restricted {
        provider.status(jobs, 403);
    }
    f
}

#[intent_test_macros::daemon_test]
async fn review_monitor_producer_counts_required_checks_without_changing_rich_snapshot() {
    let provider = ReadServer::new().await;
    let f = routing_pipeline_fixture(&provider, false).await;
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let listed = routing_json(&run(&f.server(), "return await ws.pr.monitors();").await);
    assert_eq!(listed[0], registered["monitor"]);
    let full = &registered["requirements"]["checks"];
    assert_eq!(
        full["failingRequired"],
        json!(["GitLab pipeline", "failed required"])
    );
    assert_eq!(full["pendingRequired"], json!(["pending required"]));
    let summary = &listed[0]["lastSnapshot"]["checks"];
    assert_eq!(summary["failingRequired"], 2);
    assert_eq!(summary["pendingRequired"], 1);
    assert!(summary.get("items").is_none());
    assert!(!registered.to_string().contains("monitorBinding"));
}

#[intent_test_macros::daemon_test]
async fn review_monitor_producer_keeps_unavailable_check_counts_null() {
    let provider = ReadServer::new().await;
    let f = routing_pipeline_fixture(&provider, true).await;
    let registered = routing_json(&run(&f.server(), "return await ws.mr.monitor(4);").await);
    let listed = routing_json(&run(&f.server(), "return await ws.pr.monitors();").await);
    assert_eq!(listed[0], registered["monitor"]);
    assert_eq!(
        listed[0]["lastSnapshot"]["availability"]["checks"],
        "restricted"
    );
    for field in [
        "total",
        "passed",
        "failed",
        "pending",
        "failingRequired",
        "pendingRequired",
    ] {
        assert_eq!(listed[0]["lastSnapshot"]["checks"][field], Value::Null);
        assert_eq!(registered["requirements"]["checks"][field], Value::Null);
    }
    assert_eq!(listed[0]["lastSnapshot"]["checks"]["requiredKnown"], false);
}

#[intent_test_macros::daemon_test]
async fn same_request_cache_hit_and_repeated_records_share_one_fresh_final_child() {
    let server = ReadServer::new().await;
    let f = ActualRead::new(&server).await;
    let reply = run(&f.server(), "const first=await ws.pr.snapshot(4); const second=await ws.pr.snapshot(4); return [first.title,second.title];").await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    let calls = server.replies.calls.lock().unwrap().clone();
    assert_eq!(
        calls.iter().filter(|p| p.as_str() == MR).count(),
        1,
        "{calls:?}"
    );
}

// The provider changes only after the original successful Services read. The
// next MR read observes the real project denial through the same HTTP fixture.
struct DenyProjectAfterRead {
    services: Arc<Services>,
    replies: Arc<Replies>,
    status: u16,
}
impl WorkspaceApi for DenyProjectAfterRead {
    fn agent_is_retired(&self, agent: AgentId) -> BoxFuture<'_, bool> {
        self.services.agent_is_retired(agent)
    }
    fn get_workspace(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, intent_core::Result<intent_core::Workspace>> {
        self.services.get_workspace(id)
    }
    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.services.settings_get(path)
    }
    fn pr_state(
        &self,
        workspace: WorkspaceId,
        number: u64,
        repo: Option<String>,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            let result = self.services.pr_state(workspace, number, repo).await;
            if number == 4 && result.is_ok() {
                self.replies
                    .statuses
                    .lock()
                    .unwrap()
                    .insert(PROJECT.into(), self.status);
            }
            result
        })
    }
}

async fn retained_snapshot_after_project_denial(partial: bool, cached: bool) {
    for status in [403, 404] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        if partial {
            http.status(&format!("{MR}/approvals"), 500);
        }
        let before = std::fs::read(f.auth.service.gitlab_secret_store.path()).unwrap();
        let server = WorkspaceMcpServer::new(
            Arc::new(DenyProjectAfterRead {
                services: f.auth.service.clone(),
                replies: http.replies.clone(),
                status,
            }),
            f.git.workspace.id.clone(),
        )
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(f.context()));
        let code = if cached {
            "await ws.pr.snapshot(4); const a=await ws.pr.snapshot(4); try {await ws.pr.snapshot(5);} catch(e) {} return a;"
        } else {
            "const a=await ws.pr.snapshot(4); try {await ws.pr.snapshot(5);} catch(e) {} return a;"
        };
        let reply = run(&server, code).await;
        let calls = http.replies.calls.lock().unwrap().clone();
        eprintln!("project-denial partial={partial} cached={cached} status={status} calls={calls:?} reply={reply}");
        assert!(
            calls.iter().filter(|p| p.as_str() == PROJECT).count() >= 2,
            "original denial HTTP reached"
        );
        assert_eq!(
            calls.iter().filter(|p| p.as_str() == MR).count(),
            1,
            "original MR and cached duplicate"
        );
        assert_eq!(
            before,
            std::fs::read(f.auth.service.gitlab_secret_store.path()).unwrap()
        );
        assert!(!reply.to_string().contains("stored-pat"));
        assert!(
            !reply.to_string().contains("actual review"),
            "invalidated snapshot escaped: {reply}"
        );
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{reply}"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn mcp_delivery_complete_snapshot_rejects_later_project_denial() {
    retained_snapshot_after_project_denial(false, false).await;
}

#[intent_test_macros::daemon_test]
async fn mcp_delivery_cached_snapshot_rejects_later_project_denial() {
    retained_snapshot_after_project_denial(false, true).await;
}

#[intent_test_macros::daemon_test]
async fn mcp_delivery_partial_snapshot_rejects_later_project_denial() {
    retained_snapshot_after_project_denial(true, false).await;
}

#[intent_test_macros::daemon_test]
async fn mcp_delivery_denial_prevents_artifact_and_attachment_effects() {
    for artifact in [true, false] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        f.auth
            .registry
            .apply(&[("workspaceApi.maxOutputChars".into(), json!(1000))])
            .unwrap();
        let attachments = Arc::new(intent_core::TurnAttachmentRegistry::new());
        let server = WorkspaceMcpServer::new(
            Arc::new(DenyProjectAfterRead {
                services: f.auth.service.clone(),
                replies: http.replies.clone(),
                status: 403,
            }),
            f.git.workspace.id.clone(),
        )
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(f.context()))
        .with_turn_attachments(Some(attachments.clone()));
        let code = if artifact {
            "const a=await ws.pr.snapshot(4); try {await ws.pr.snapshot(5);} catch(e) {} return a.title.repeat(300);"
        } else {
            "const a=await ws.pr.snapshot(4); try {await ws.pr.snapshot(5);} catch(e) {} return {__mcpContentItems:[{type:'resource',resource:{uri:'fixture://denied',mimeType:'application/json',text:JSON.stringify({title:a.title})}}]};"
        };
        let reply = run(&server, code).await;
        assert!(
            reply
                .to_string()
                .contains("Private result delivery refused"),
            "{reply}"
        );
        assert!(!reply.to_string().contains("actual review"));
        assert!(!f.git.dir.path().join("tool-outputs").exists());
        assert_eq!(
            attachments.pending_count_by_mime(&f.agent, "application/json"),
            0
        );
        assert!(
            http.replies
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.as_str() == PROJECT)
                .count()
                >= 2
        );
    }
}

#[intent_test_macros::daemon_test]
async fn mcp_delivery_optional_context_cannot_disclose_denied_snapshot_over_tcp() {
    use crate::repository_context_live::tests::LiveFixture;
    use crate::repository_context_output::tests::{tcp, FACTS};

    let http = ReadServer::new().await;
    let f = LiveFixture::new(&http).await;
    // Keep the original final-delivery regression behind an explicitly capable
    // test recipient. Production harnesses remain unqualified in this wave.
    let owner = crate::repository_context_live::RepositoryContextOwner::bind(
        f.base.auth.service.clone(),
        RepositoryReadOwner::capture(f.base.auth.service.clone())
            .map(RepositoryReadOwner::with_review_monitoring),
        &f.physical,
    )
    .unwrap();
    let server = WorkspaceMcpServer::new(
        Arc::new(DenyProjectAfterRead {
            services: f.base.auth.service.clone(),
            replies: http.replies.clone(),
            status: 404,
        }),
        f.session.workspace_id.clone(),
    )
    .with_caller_agent_id(Some(f.session.id.clone()))
    .with_request_context(owner.mcp_context())
    .with_repository_guidance(&f.session, owner.guidance_source());
    let reply = tcp(
        server,
        "const a=await ws.pr.snapshot(4); try {await ws.pr.snapshot(5);} catch(e) {} return a;",
    )
    .await;
    owner.drain_jobs().await;
    assert!(!reply.to_string().contains("actual review"), "{reply}");
    assert!(!reply.to_string().contains(FACTS), "{reply}");
    assert!(
        reply
            .to_string()
            .contains("Private result delivery refused"),
        "{reply}"
    );
    assert!(
        http.replies
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.as_str() == PROJECT)
            .count()
            >= 2
    );
}

#[intent_test_macros::daemon_test]
async fn ambiguous_unmapped_and_missing_anchor_discovery_never_acquire_or_disclose() {
    for mode in ["ambiguous", "unmapped", "no-remote", "missing-anchor"] {
        let server = ReadServer::new().await;
        let f = ActualRead::new(&server).await;
        if mode == "ambiguous" {
            f.git.git(
                &f.git.path,
                &[
                    "remote",
                    "add",
                    "other",
                    "https://github.com/private/other.git",
                ],
            );
        } else if mode == "unmapped" {
            f.git.git(
                &f.git.path,
                &[
                    "remote",
                    "set-url",
                    "origin",
                    "git@alias:private/hidden.git",
                ],
            );
        }
        if mode == "no-remote" {
            f.git.git(&f.git.path, &["remote", "remove", "origin"]);
        }
        let mut endpoint = f.server();
        if mode == "missing-anchor" {
            endpoint = WorkspaceMcpServer::new(f.auth.service.clone(), f.git.workspace.id.clone())
                .with_caller_agent_id(Some(f.agent.clone()))
                .with_request_context(Arc::new(f.owner.callback()));
        }
        for namespace in ["pr", "mr"] {
            for action in ["snapshot", "monitor", "unmonitor"] {
                let reply = run(&endpoint, &format!("try {{ return await ws.{namespace}.{action}(4); }} catch(e) {{ return e.message; }}")).await;
                let expected = if mode == "missing-anchor" {
                    "Addressed review monitoring unavailable"
                } else {
                    REFUSAL
                };
                assert!(
                    reply.to_string().contains(expected),
                    "{mode}/{namespace}/{action}: {reply}"
                );
                assert_eq!(server.count(), 0);
                assert!(!reply.to_string().contains("private/hidden"));
                assert!(!reply.to_string().contains(f.git.path.to_str().unwrap()));
                assert!(!reply.to_string().contains("ownerAgentId"));
            }
        }
        assert!(f
            .auth
            .service
            .store
            .list_pr_monitors_by_agent(&f.agent)
            .await
            .unwrap()
            .is_empty());
    }
}

#[intent_test_macros::daemon_test]
async fn positive_github_control_discovery_needs_no_private_anchor_or_gitlab_account() {
    let git = GitFixture::new().await;
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "upstream",
            "https://github.com/Actual/Repository.git",
        ],
    );
    let services = Services::new(git.store.clone());
    intent_core::caller::with_caller(Caller::Daemon, async {
        let captured = CapturedReview::capture(&services, git.workspace.id.clone(), 4);
        assert!(captured.host.is_err());
        assert!(captured.settled.is_err());
        let ReadOutcome::Github(repo) = captured.read(&services).await.unwrap() else {
            panic!("ordinary GitHub only");
        };
        assert_eq!(repo.owner, "actual");
        assert_eq!(repo.name, "repository");
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "unresolved",
                "https://unknown.test/private/repo.git",
            ],
        );
        assert!(
            CapturedReview::capture(&services, git.workspace.id.clone(), 4)
                .read(&services)
                .await
                .is_err()
        );
    })
    .await;
}

/// intent-hq/intent#7449: a GitHub workspace inserted with repository
/// metadata gets an unresolved historical primary selection from the insert
/// trigger. A repo-less read must still resolve its own GitHub fetch remote,
/// while ambiguous, unmappable and missing remotes keep refusing.
#[intent_test_macros::daemon_test]
async fn implicit_github_read_resolves_unresolved_historical_selection_from_fetch_remotes() {
    let git = GitFixture::with_repository_metadata("actual", "repository").await;
    let services = Services::new(git.store.clone());
    let root = git.root();
    intent_core::caller::with_caller(Caller::Daemon, async {
        assert!(matches!(
            SelectionFacts::read(&services, &root)
                .await
                .ok()
                .and_then(|facts| facts.saved().ok()),
            Some(SavedReviewSelection::UnresolvedHistorical { .. })
        ));
        let read = || async {
            CapturedReview::capture(&services, git.workspace.id.clone(), 4)
                .read(&services)
                .await
        };
        assert!(read().await.is_err(), "no remote");
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/Actual/Repository.git",
            ],
        );
        let Ok(ReadOutcome::Github(repo)) = read().await else {
            panic!("repo-less read of a GitHub workspace must use its fetch remote");
        };
        assert_eq!(repo.owner, "actual");
        assert_eq!(repo.name, "repository");
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "other",
                "https://github.com/private/other.git",
            ],
        );
        assert!(read().await.is_err(), "ambiguous GitHub remotes");
        git.git(&git.path, &["remote", "remove", "other"]);
        git.git(
            &git.path,
            &[
                "remote",
                "add",
                "unmapped",
                "https://unknown.test/private/repo.git",
            ],
        );
        assert!(read().await.is_err(), "unmappable remote");
    })
    .await;
}

#[intent_test_macros::daemon_test]
async fn live_request_retirement_while_real_http_is_held_denies_later_private_delivery() {
    let server = ReadServer::new().await;
    let f = ActualRead::new(&server).await;
    server.pause(MR);
    let endpoint = f.server();
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.pr.snapshot(4);").await });
    server.entered().await;
    f.owner.interrupt_requests();
    server.resume();
    let reply = task.await.unwrap();
    assert!(!reply.to_string().contains("actual review"), "{reply}");
    assert!(reply.to_string().contains("refused"), "{reply}");
    let fresh = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(fresh.to_string().contains("actual review"), "{fresh}");
}

#[intent_test_macros::daemon_test]
async fn current_provider_denial_and_quota_never_become_successful_private_data() {
    for status in [401, 403, 404, 429] {
        let server = ReadServer::new().await;
        let f = ActualRead::new(&server).await;
        server.status(MR, status);
        let reply = run(
            &f.server(),
            "try { await ws.pr.snapshot(4); } catch(e) {} return 'constant';",
        )
        .await;
        assert!(
            !reply.to_string().contains("actual review"),
            "status {status}: {reply}"
        );
        assert!(server.count() > 0);
        if status == 401 {
            assert!(f
                .auth
                .service
                .gitlab_repository_settled_connection()
                .is_err());
            assert!(reply.to_string().contains("refused"), "{reply}");
        } else {
            assert!(f
                .auth
                .service
                .gitlab_repository_settled_connection()
                .is_ok());
        }
    }
}
#[derive(Default)]
struct Replies {
    statuses: Mutex<HashMap<String, u16>>,
    quota_headers: Mutex<HashMap<String, String>>,
    bodies: Mutex<HashMap<String, Value>>,
    pause: Mutex<Option<String>>,
    entered: Notify,
    release: Notify,
    calls: Mutex<Vec<String>>,
}
pub(crate) struct ReadServer {
    pub(crate) fixture: Server,
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
    pub(crate) fn set_monitor_review(&self, head: &str, state: &str, revision: u8) {
        self.replies.bodies.lock().unwrap().insert(
            MR.into(),
            json!({
                "iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
                "title":"actual review","state":state,"draft":false,"sha":head,
                "source_branch":"feature","target_branch":"main",
                "source_project_id":42,"target_project_id":42,
                "created_at":"2026-09-27T00:00:00Z",
                "updated_at":format!("2026-09-27T00:00:{revision:02}Z"),
            }),
        );
    }

    pub(crate) async fn new() -> Self {
        Self::at_instance("https://gitlab.test/forge").await
    }

    async fn at_instance(instance: &str) -> Self {
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
        fixture.host = intent_sourcecontrol::GitlabHost::parse(instance)
            .unwrap()
            .with_api_origin(&endpoint)
            .unwrap();
        fixture.descriptor = GitlabDescriptor::with_loopback_endpoint(
            intent_sourcecontrol::GitlabInstance::parse(instance).unwrap(),
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
                        if pause.as_deref() == path.split('?').next() {
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
                    let custom = control.bodies.lock().unwrap().get(&path).cloned();
                    let body=if let Some(custom) = custom { custom } else if path==PROJECT {
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
                    let quota = control
                        .quota_headers
                        .lock()
                        .unwrap()
                        .get(&path)
                        .cloned()
                        .unwrap_or_else(|| {
                            format!(
                                "RateLimit-Remaining: 23\r\nRateLimit-Reset: 4000000000\r\n{}",
                                if status == 429 {
                                    "Retry-After: 60\r\n"
                                } else {
                                    ""
                                }
                            )
                        });
                    let response=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{quota}Connection: close\r\n\r\n{body}",body.len());
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
    pub(crate) fn status(&self, path: &str, status: u16) {
        self.replies
            .statuses
            .lock()
            .unwrap()
            .insert(path.into(), status);
    }
    pub(crate) fn pause(&self, path: &str) {
        *self.replies.pause.lock().unwrap() = Some(path.into());
    }
    pub(crate) async fn entered(&self) {
        timeout(BUDGET, self.replies.entered.notified())
            .await
            .unwrap();
    }
    pub(crate) fn resume(&self) {
        self.replies.release.notify_one();
    }
    pub(crate) fn count(&self) -> usize {
        self.replies.calls.lock().unwrap().len()
    }
}

#[intent_test_macros::daemon_test]
async fn original_response_requires_fresh_git_facts_before_cache_apply() {
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    // Hold the final HTTP response, after every earlier provider fence.
    http.pause(&format!("{MR}/discussions"));
    let log = Arc::new(Mutex::new(Vec::new()));
    let endpoint = WorkspaceMcpServer::new(
        Arc::new(TestReadApi(f.auth.service.clone(), Some(log.clone()))),
        f.git.workspace.id.clone(),
    )
    .with_caller_agent_id(Some(f.agent.clone()))
    .with_request_context(Arc::new(f.context()));
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.pr.snapshot(4);").await });
    http.entered().await;
    f.git.git(
        &f.git.path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "new head before apply",
        ],
    );
    http.resume();
    let reply = task.await.unwrap();
    assert!(!reply.to_string().contains("actual review"), "{reply}");
    assert!(
        log.lock().unwrap()[0].starts_with("Err("),
        "{:?}",
        log.lock().unwrap()
    );
    let before = http.count();
    let fresh = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(fresh.to_string().contains("actual review"), "{fresh}");
    assert!(http.count() > before, "refused result was never cached");
}

#[intent_test_macros::daemon_test]
async fn detached_actual_file_read_cancellation_keeps_lease_and_never_dispatches() {
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::PausedRead;
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let mut file = PausedRead::install(&f.auth);
    let endpoint = f.server();
    let task = tokio::spawn(async move { run(&endpoint, "return await ws.pr.snapshot(4);").await });
    file.entered().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let gate = f.auth.service.gitlab_credential_gate.lock();
    tokio::pin!(gate);
    assert!(timeout(Duration::from_millis(40), &mut gate).await.is_err());
    file.resume();
    drop(timeout(BUDGET, gate).await.unwrap());
    assert_eq!(http.count(), 0);
    let fresh = run(&f.server(), "return await ws.pr.snapshot(4);").await;
    assert!(fresh.to_string().contains("actual review"), "{fresh}");
}

#[intent_test_macros::daemon_test]
async fn actual_same_binding_refresh_preserves_old_denial_quota_without_rebinding() {
    for status in [401, 403, 404] {
        let http = ReadServer::new().await;
        let f = ActualRead::oauth(&http).await;
        let original = f.auth.request();
        let log = Arc::new(Mutex::new(Vec::new()));
        http.status(MR, status);
        http.pause(MR);
        let endpoint = WorkspaceMcpServer::new(
            Arc::new(TestReadApi(f.auth.service.clone(), Some(log.clone()))),
            f.git.workspace.id.clone(),
        )
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(f.context()));
        let task = tokio::spawn(async move {
            run(
                &endpoint,
                "try { await ws.pr.snapshot(4); } catch(e) {} return 'original error retained';",
            )
            .await
        });
        http.entered().await;
        crate::source_control_auth_ops::repository_owner::secret_reader::tests::refresh_original(
            &f.auth.service,
            &http.fixture.host,
        )
        .await;
        let current = f.auth.request();
        assert_eq!(current.binding, original.binding);
        assert!(current.secret_revision > original.secret_revision);
        http.resume();
        let reply = task.await.unwrap();
        assert!(
            reply.to_string().contains("original error retained"),
            "{reply}"
        );
        {
            let results = log.lock().unwrap();
            assert!(
                results[0].contains(&format!("status: Some({status})")),
                "{:?}",
                *results
            );
            assert!(results[0].contains("remaining: Some(23)"), "{:?}", *results);
        }
        assert!(f
            .auth
            .service
            .gitlab_repository_settled_connection()
            .is_ok());
        http.status(MR, 200);
        let fresh = run(&f.server(), "return await ws.pr.snapshot(4);").await;
        assert!(fresh.to_string().contains("actual review"), "{fresh}");
    }
}

#[intent_test_macros::daemon_test]
async fn optional_quota_partial_is_retained_without_old_complete_cache_or_global_pause() {
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    http.status(&format!("{MR}/approvals"), 429);
    let log = Arc::new(Mutex::new(Vec::new()));
    let endpoint = WorkspaceMcpServer::new(
        Arc::new(TestReadApi(f.auth.service.clone(), Some(log.clone()))),
        f.git.workspace.id.clone(),
    )
    .with_caller_agent_id(Some(f.agent.clone()))
    .with_request_context(Arc::new(f.context()));
    let reply = run(&endpoint, "return await ws.pr.snapshot(4);").await;
    assert!(reply.to_string().contains("actual review"), "{reply}");
    assert!(reply.to_string().contains("rate-limited"), "{reply}");
    let results = log.lock().unwrap();
    assert!(results[0].contains("remaining: Some(0)"), "{:?}", *results);
    assert!(
        results[0].contains("reset_at: Some(4000000000)"),
        "{:?}",
        *results
    );
    assert!(f.auth.service.sweep_rate_limit_paused_until().is_none());
}

// Copy only actual opaque records for negative aggregation/lifetime controls.
// No fact, request, eligibility or authority constructor is supplied here.
pub(crate) async fn retain_and_check_records(
    evidence: &[intent_acp::mcp_server::private_results::McpReadEvidence],
) -> Vec<Arc<ReadRecord>> {
    let mut records = Vec::new();
    for value in evidence {
        let value = value.downcast_ref::<ReadRecord>().unwrap();
        assert_eq!(
            value.acquisition.child.retirement().check_current(),
            Err(AdmissionError::Retired)
        );
        assert!(value
            .acquisition
            .revalidate(&value.facts.request)
            .await
            .is_err());
        let mut transferred = false;
        assert!(Box::new(ReadFence(value.acquisition.clone()))
            .dispatch(&mut || {
                transferred = true;
                Ok(())
            })
            .is_err());
        assert!(!transferred);
        records.push(Arc::new(ReadRecord {
            request: value.request.clone(),
            facts: value.facts.clone(),
            operation: value.operation.clone(),
            eligibility: value.eligibility.clone(),
            delivery: value.delivery.clone(),
            acquisition: value.acquisition.clone(),
        }));
    }
    records
}

pub(crate) async fn refuse_foreign_record_set(
    originals: &[Arc<ReadRecord>],
    foreign: &[Arc<ReadRecord>],
) {
    let request = crate::repository_admission::request_context::current_read_request().unwrap();
    assert!(!Arc::ptr_eq(&request, &foreign[0].request));
    assert!(foreign[0]
        .operation
        .lock()
        .unwrap()
        .eq(&ReadState::Finished));
    let records = originals
        .iter()
        .chain(foreign)
        .map(AsRef::as_ref)
        .collect::<Vec<_>>();
    let mut transferred = false;
    let result = with_records(&request, &originals[0].facts.services, &records, || {
        transferred = true;
    })
    .await;
    assert_eq!(result, Err(AdmissionError::Denied));
    assert!(!transferred);
}

#[intent_test_macros::daemon_test]
async fn foreign_services_caller_and_wire_cannot_supply_qualified_read_ownership() {
    use intent_core::caller::{with_wire_credential, WireCredential};
    for mode in ["services", "caller", "wire", "unavailable"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let api = if mode == "services" {
            Arc::new(f.auth.service.as_ref().clone()) as Arc<dyn WorkspaceApi>
        } else {
            f.api()
        };
        let mut endpoint = WorkspaceMcpServer::new(api, f.git.workspace.id.clone())
            .with_caller_agent_id(Some(if mode == "caller" {
                AgentId::new()
            } else {
                f.agent.clone()
            }))
            .with_request_context(Arc::new(f.context()));
        if mode == "unavailable" {
            endpoint = WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
                .with_caller_agent_id(Some(f.agent.clone()));
        }
        for namespace in ["pr", "mr"] {
            for action in ["snapshot", "monitor", "unmonitor"] {
                let code = format!("try {{return await ws.{namespace}.{action}(4);}} catch(e) {{return e.message;}}");
                let body = run(&endpoint, &code);
                let reply = if mode == "wire" {
                    with_wire_credential(
                        Some(WireCredential::Principal {
                            principal_id: intent_core::PrincipalId::new(),
                            token_hash: "original wire fixture".into(),
                        }),
                        body,
                    )
                    .await
                } else {
                    body.await
                };
                assert!(
                    !reply.to_string().contains("actual review"),
                    "{mode}/{namespace}/{action}: {reply}"
                );
                assert_eq!(http.count(), 0);
                assert!(!reply.to_string().contains(f.git.path.to_str().unwrap()));
                assert!(!reply.to_string().contains("ownerAgentId"));
            }
        }
        assert!(f
            .auth
            .service
            .store
            .list_pr_monitors_by_agent(&f.agent)
            .await
            .unwrap()
            .is_empty());
    }
}

// Scheduling-only wrapper around the actual policy. The inactive planner's
// callback is a counted local probe; the real packet still uses original.admit.
#[derive(Clone, Copy)]
enum JointMode {
    Include,
    OptionalStale,
    RequiredGitStale,
    Panic,
}

struct JointContext {
    original: Arc<dyn intent_acp::mcp_server::request_context::McpRequestContext>,
    mode: JointMode,
    root: std::path::PathBuf,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}
struct JointScope {
    original: Arc<dyn intent_acp::mcp_server::request_context::McpRequestScope>,
    mode: JointMode,
    root: std::path::PathBuf,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}
struct JointPolicy {
    original: Arc<dyn intent_acp::mcp_server::private_results::McpPrivatePolicy>,
    mode: JointMode,
    root: std::path::PathBuf,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}
impl intent_acp::mcp_server::request_context::McpRequestContext for JointContext {
    fn capture(&self) -> Arc<dyn intent_acp::mcp_server::request_context::McpRequestScope> {
        Arc::new(JointScope {
            original: self.original.capture(),
            mode: self.mode,
            root: self.root.clone(),
            calls: self.calls.clone(),
            completed: self.completed.clone(),
        })
    }
}
impl intent_acp::mcp_server::request_context::McpRequestScope for JointScope {
    fn scope<'a>(
        &'a self,
        future: intent_acp::mcp_server::request_context::McpContextFuture<'a>,
    ) -> intent_acp::mcp_server::request_context::McpContextFuture<'a> {
        self.original.scope(future)
    }
    fn private_result_policy(
        &self,
    ) -> Option<Arc<dyn intent_acp::mcp_server::private_results::McpPrivatePolicy>> {
        self.original.private_result_policy().map(|original| {
            Arc::new(JointPolicy {
                original,
                mode: self.mode,
                root: self.root.clone(),
                calls: self.calls.clone(),
                completed: self.completed.clone(),
            }) as Arc<dyn intent_acp::mcp_server::private_results::McpPrivatePolicy>
        })
    }
}
impl intent_acp::mcp_server::private_results::McpPrivatePolicy for JointPolicy {
    fn capture_host(
        &self,
        call: McpHostCall,
    ) -> Box<dyn intent_acp::mcp_server::private_results::McpPrivateHostScope> {
        self.original.capture_host(call)
    }
    fn admit<'a>(
        &'a self,
        boundary: &'a intent_acp::mcp_server::private_results::McpPrivateBoundary,
        evidence: &'a [intent_acp::mcp_server::private_results::McpReadEvidence],
        packet: intent_acp::mcp_server::private_results::PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, intent_acp::mcp_server::private_results::McpPrivateAdmission> {
        Box::pin(async move {
            use intent_core::caller::with_caller;
            use intent_store::RepositoryLifecycleObserver;
            use std::sync::atomic::Ordering;
            if boundary.kind()
                != intent_acp::mcp_server::private_results::McpPrivateBoundaryKind::DirectResponse
            {
                return self.original.admit(boundary, evidence, packet).await;
            }
            let records = evidence
                .iter()
                .map(|item| item.downcast_ref::<ReadRecord>().unwrap())
                .collect::<Vec<_>>();
            assert!(records.len() >= 2);
            let distinct = records
                .iter()
                .copied()
                .find(|record| !Arc::ptr_eq(&record.operation, &records[0].operation))
                .expect("two original host calls have distinct operation allocations");
            let request =
                crate::repository_admission::request_context::current_read_request().unwrap();
            let services = records[0].facts.services.clone();
            let optional = request.capture_optional().unwrap();
            let key = RepositoryLifecycleKey::GitRoot(intent_core::WorkspaceGitRootId::new());
            optional
                .metadata()
                .subscribe_metadata(&[RepositoryLifecycleKey::Database, key.clone()])
                .unwrap();
            let ready = optional
                .run_optional(|_| async { Ok("prebuilt optional fixture") })
                .unwrap()
                .await
                .unwrap();
            let mut ordered = vec![records[0], distinct, records[0]];
            ordered.extend(records.iter().rev().copied());
            assert!(with_joint_local_records(
                &request,
                &services,
                &[],
                Some(ready.metadata()),
                |_| panic!("empty must not transfer")
            )
            .await
            .is_err());
            let foreign_services = Arc::new(services.as_ref().clone());
            assert!(with_joint_local_records(
                &request,
                &foreign_services,
                &ordered,
                Some(ready.metadata()),
                |_| panic!("foreign Services must not transfer")
            )
            .await
            .is_err());
            assert!(with_caller(
                Caller::Agent {
                    agent_id: AgentId::new()
                },
                with_joint_local_records(
                    &request,
                    &services,
                    &ordered,
                    Some(ready.metadata()),
                    |_| panic!("foreign caller must not transfer")
                )
            )
            .await
            .is_err());
            *records[0].operation.lock().unwrap() = ReadState::Acquiring;
            assert!(with_joint_local_records(
                &request,
                &services,
                &ordered,
                Some(ready.metadata()),
                |_| panic!("unfinished member must not transfer")
            )
            .await
            .is_err());
            *records[0].operation.lock().unwrap() = ReadState::Finished;
            if matches!(self.mode, JointMode::OptionalStale) {
                services
                    .repository_lifecycle_registry()
                    .await
                    .unwrap()
                    .begin_mutation(&[key])
                    .unwrap()
                    .settle_confirmed();
            }
            if matches!(self.mode, JointMode::RequiredGitStale) {
                std::fs::write(
                    self.root.join(".git/HEAD"),
                    "ref: refs/heads/joint-changed\n",
                )
                .unwrap();
            }
            if matches!(self.mode, JointMode::Panic) {
                let mut transfer = Box::pin(with_joint_local_records::<()>(
                    &request,
                    &services,
                    &ordered,
                    Some(ready.metadata()),
                    |_| {
                        self.calls.fetch_add(1, Ordering::SeqCst);
                        panic!("joint consuming action fixture");
                    },
                ));
                let result = std::future::poll_fn(|context| {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        std::future::Future::poll(transfer.as_mut(), context)
                    })) {
                        Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                        Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
                        Err(panic) => std::task::Poll::Ready(Err(panic)),
                    }
                })
                .await;
                drop(transfer);
                assert!(result.is_err());
            } else {
                let result = with_joint_local_records(
                    &request,
                    &services,
                    &ordered,
                    Some(ready.metadata()),
                    |include| {
                        self.calls.fetch_add(1, Ordering::SeqCst);
                        (include, Err::<(), _>("original consumer result"))
                    },
                )
                .await;
                match self.mode {
                    JointMode::RequiredGitStale => assert!(result.is_err()),
                    JointMode::Include => {
                        assert_eq!(result.unwrap(), (true, Err("original consumer result")));
                    }
                    JointMode::OptionalStale => {
                        assert_eq!(result.unwrap(), (false, Err("original consumer result")));
                    }
                    JointMode::Panic => unreachable!(),
                }
            }
            // Scope/value destruction occurs after all joint/P guards release.
            drop(ready);
            self.completed.store(true, Ordering::SeqCst);
            self.original.admit(boundary, evidence, packet).await
        })
    }
}

async fn joint_probe(mode: JointMode) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let http = ReadServer::new().await;
    let f = ActualRead::new(&http).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicBool::new(false));
    let server = WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
        .with_caller_agent_id(Some(f.agent.clone()))
        .with_request_context(Arc::new(JointContext {
            original: Arc::new(f.context()),
            mode,
            root: f.git.path.clone(),
            calls: calls.clone(),
            completed: completed.clone(),
        }));
    let response = run(
        &server,
        "await ws.pr.snapshot(4); return await ws.pr.snapshot(4);",
    )
    .await;
    assert!(
        completed.load(Ordering::SeqCst),
        "the complete final planner probe ran: {response}"
    );
    let successful = matches!(mode, JointMode::Include | JointMode::OptionalStale);
    assert_eq!(
        response.to_string().contains("actual review"),
        successful,
        "{response}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        usize::from(!matches!(mode, JointMode::RequiredGitStale))
    );
}

#[intent_test_macros::daemon_test]
async fn joint_local_records_require_every_original_and_transfer_once() {
    joint_probe(JointMode::Include).await;
}

#[intent_test_macros::daemon_test]
async fn joint_local_records_optional_invalidation_keeps_required_public_output() {
    joint_probe(JointMode::OptionalStale).await;
}

#[intent_test_macros::daemon_test]
async fn joint_local_records_required_git_change_refuses_even_current_optional() {
    joint_probe(JointMode::RequiredGitStale).await;
}

#[intent_test_macros::daemon_test]
async fn joint_local_records_action_panic_never_retries_or_recovers_required_output() {
    joint_probe(JointMode::Panic).await;
}

#[path = "sql_effects.rs"]
mod sql_effects;
