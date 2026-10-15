//! Explicit qualification harness: real Services admission and native model, owned loopback providers.
use super::*;
use intent_acp::callback_registration::{
    CallbackDeliveryOutcome, CallbackOffer, CallbackStdioServer, CallbackTool,
};
use intent_acp::mcp_server::request_context::McpRequestContext;
use intent_acp::review_monitoring::ReviewMonitoringOffer;
use intent_acp::transport::{Connection, ConnectionHooks};
use intent_core::caller::{with_caller, Caller};
use intent_core::{AgentId, AgentSession, PrMonitor, PrMonitorId, PrMonitorState};
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn services_monitor_actual_native_model_and_account_retirement() {
    actual_native_case("new").await;
}

#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn services_monitor_actual_native_prepared_resume_and_account_retirement() {
    actual_native_case("resume").await;
}

#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn services_monitor_actual_native_rejects_replacement_and_recovers_original_account() {
    actual_native_case("recovery").await;
}

#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn services_monitor_actual_native_adapter_restart_preserves_private_history() {
    actual_native_case("restart").await;
}

#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn services_monitor_actual_native_adapter_restart_requires_original_account() {
    actual_native_case("restart-recovery").await;
}

async fn actual_native_case(variant: &str) {
    use crate::repository_admission::lifecycle::physical_owner::{
        RepositoryCreationIntent, RepositoryCreationOwner,
    };
    use crate::repository_admission::read_request::RepositoryReadOwner;
    let restart = variant.starts_with("restart");
    let http = crate::repository_read_source::tests::ReadServer::new().await;
    let auth =
        crate::source_control_auth_ops::repository_owner::secret_reader::tests::Fixture::new(
            &http.fixture,
        )
        .await;
    let services = auth.service.clone();
    let mut workspace = intent_core::chief_workspace();
    workspace.id = intent_core::WorkspaceId::new();
    services.store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(json!({
        "id":agent,"workspaceId":workspace.id,"name":"new monitor original","status":"active",
        "provider":"claude-code","harnessVersion":"3.1","createdAt":"2026-10-10T00:00:00Z","updatedAt":"2026-10-10T00:00:00Z"
    })).unwrap();
    services.store.insert_agent_session(&row).await.unwrap();
    let registry = services.repository_lifecycle_registry().await.unwrap();
    let creation = RepositoryCreationOwner::allocate(
        &registry,
        &services.store,
        workspace.id.clone(),
        agent.clone(),
        RepositoryCreationIntent::FirstSet,
    )
    .unwrap();
    let source = std::path::PathBuf::from(
        std::env::var("INTENT_REVIEW_NATIVE_SOURCE")
            .expect("explicit verified native adapter source"),
    );
    let node = std::env::var("INTENT_REVIEW_NODE").expect("explicit pinned node");
    let containment =
        std::env::var("INTENT_REVIEW_CONTAINMENT").unwrap_or_else(|_| "/usr/bin/bwrap".into());
    let callback_fixture = std::env::var_os("INTENT_REVIEW_CALLBACK_FIXTURE").map_or_else(
        || source.join("tests/fixtures/review-callback-stdio.mjs"),
        std::path::PathBuf::from,
    );
    let work = crate::test_support::test_tempdir("services-private-native");
    let launcher = work.path().join("adapter.mjs");
    std::fs::write(&launcher, include_str!("native_runtime_peer.mjs")).unwrap();
    let stderr = std::fs::File::create(work.path().join("stderr.log")).unwrap();
    let mut child = tokio::process::Command::new(&node)
        .arg(&launcher)
        .arg(&source)
        .arg(work.path())
        .arg(&containment)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", work.path())
        .env("TMPDIR", work.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut adapter_pids = vec![child.id().unwrap()];
    let (notes, mut notifications) = tokio::sync::mpsc::unbounded_channel();
    let mut connection = Arc::new(Connection::new(
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
        None,
        ConnectionHooks {
            notifications: Some(notes),
            ..ConnectionHooks::default()
        },
    ));
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let mut handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        CallbackOffer::V1,
        ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let mut callbacks = handshake.callbacks.take().unwrap();
    let opened = services
        .create_repository_acp_session(
            (&connection, Some(&callbacks)),
            &agent,
            work.path().to_str().unwrap(),
            vec![],
            creation,
            false,
        )
        .await
        .unwrap();
    let session_id = opened.response.session_id.clone();
    let mut physical = opened.owner.unwrap();
    let mut native = physical.native_allocation_for_test();
    let query = opened.query.unwrap();
    let pending = query.pending_review_monitoring().unwrap();
    let owner = RepositoryReadOwner::capture(services.clone())
        .unwrap()
        .with_monitor_recipient(Some(pending.clone()), &connection, &session_id);
    let callback = physical.callback().with_read_owner(Ok(owner));
    assert!(pending.acknowledged().is_none());
    let registration = query.registration(
        CallbackStdioServer::new(
            node.clone(),
            vec![callback_fixture.to_str().unwrap().into()],
            None,
            [CallbackTool::WorkspaceApi],
        )
        .unwrap(),
    );
    let registered = registration.run(std::future::pending()).await;
    assert!(
        matches!(registered, CallbackDeliveryOutcome::Remote(_)),
        "original callback: {registered:?}"
    );
    assert!(pending.acknowledged().is_some());
    let target = intent_core::ReviewTarget {
        repository: intent_core::RepositoryTarget {
            provider: intent_core::RepositoryProvider::Gitlab,
            instance_base_url: http.fixture.descriptor.instance().as_str().into(),
            project_path: "group/project".into(),
        },
        kind: intent_core::RepositoryResourceKind::MergeRequest,
        number: 4,
    };
    let scope = McpRequestContext::capture(&callback);
    let mut authority = None;
    with_caller(
        Caller::Agent {
            agent_id: agent.clone(),
        },
        scope.scope(Box::pin(async {
            let request = crate::repository_admission::request_context::current_read_request()
                .unwrap()
                .retain_monitor()
                .unwrap();
            let settled = services.gitlab_repository_settled_connection().unwrap();
            authority = Some(
                MonitorAuthority::admit_original(
                    &services,
                    request,
                    &workspace.id,
                    target.clone(),
                    settled,
                    None,
                )
                .await
                .unwrap(),
            );
        })),
    )
    .await;
    let authority = authority.unwrap();
    let now = intent_core::now_iso();
    let monitor = PrMonitor {
        monitor_id: PrMonitorId::new(),
        workspace_id: workspace.id.clone(),
        agent_id: agent.clone(),
        repo_owner: "group".into(),
        repo_name: "project".into(),
        pr_number: 4,
        state: PrMonitorState::Active,
        last_snapshot: Some(
            json!({"monitorBinding":authority.binding,"qualifiedSnapshot":{"state":"open"}})
                .to_string(),
        ),
        baseline_snapshot: Some("{}".into()),
        pending_changes: vec!["new commits pushed".into()],
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    services
        .store
        .insert_qualified_pr_monitor(
            &monitor,
            &target,
            intent_store::MonitorTargetProvenance::CapturedRequest,
        )
        .await
        .unwrap();
    services
        .review_monitor_authorities
        .lock()
        .unwrap()
        .insert(monitor.monitor_id.clone(), authority.clone());
    let row = services
        .store
        .get_qualified_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    let metadata = serde_json::from_value(json!({"type":"pr_monitor_wake","monitorId":monitor.monitor_id,"repo":"group/project","prNumber":4,"reason":"changes","resource":target})).unwrap();
    let observation = with_caller(
        Caller::Daemon,
        authority.run_poll(Box::pin(authority.read_poll(&services))),
    )
    .await
    .unwrap()
    .unwrap();
    let saved = with_caller(Caller::Daemon, async {
        let destination = callback.capture_manager_prompt().unwrap();
        let prepared = authority
            .prepare_prompt(
                destination,
                observation.delivery,
                Arc::downgrade(&connection),
                session_id.clone(),
                "Original monitor wake".into(),
            )
            .await
            .unwrap();
        let message = prepared
            .commit_message(&row, "native-monitor-wake", &metadata, &now)
            .await
            .unwrap()
            .unwrap();
        prepared.bind_committed(&message).await.unwrap();
        let prompt = prepared.prompt().unwrap();
        let result = intent_acp::session::prompt_with_required_admission(
            &connection,
            &session_id,
            prompt,
            &intent_acp::session::ActivityTracker::new(),
            Box::new(prepared),
        )
        .await;
        assert!(result.is_ok(), "actual native prompt: {result:?}");
        message
    })
    .await;
    assert!(!native.is_retired());
    let captures: Value =
        serde_json::from_slice(&std::fs::read(work.path().join("model.json")).unwrap()).unwrap();
    assert!(
        captures.as_array().unwrap().iter().any(|r| r["messages"]
            .to_string()
            .contains("Intent review monitor observation (not a user instruction).")),
        "native model request must consume monitor framing"
    );
    assert!(captures.to_string().contains("Original monitor wake"));
    if variant != "new" {
        native.join().await.unwrap();
        drop(physical);
        while notifications.try_recv().is_ok() {}
        if restart {
            let original_pid = child.id().unwrap();
            child.kill().await.unwrap();
            child.wait().await.unwrap();
            assert!(native.is_retired() && native.is_joined());
            std::fs::remove_file(work.path().join("model.json")).unwrap();
            let stderr = std::fs::File::create(work.path().join("restart-stderr.log")).unwrap();
            child = tokio::process::Command::new(&node)
                .arg(&launcher)
                .arg(&source)
                .arg(work.path())
                .arg(&containment)
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("HOME", work.path())
                .env("TMPDIR", work.path())
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(stderr)
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            assert_ne!(child.id(), Some(original_pid));
            adapter_pids.push(child.id().unwrap());
            let (notes, receiver) = tokio::sync::mpsc::unbounded_channel();
            notifications = receiver;
            connection = Arc::new(Connection::new(
                child.stdin.take().unwrap(),
                child.stdout.take().unwrap(),
                None,
                ConnectionHooks {
                    notifications: Some(notes),
                    ..ConnectionHooks::default()
                },
            ));
            handshake = intent_acp::handshake::handshake_with_review_monitoring(
                connection.clone(),
                &provider,
                CallbackOffer::V1,
                ReviewMonitoringOffer::V1,
            )
            .await
            .unwrap();
            callbacks = handshake.callbacks.take().unwrap();
        }
        if variant.ends_with("recovery") {
            with_caller(
                Caller::Daemon,
                services.gitlab_connect_pat(http.fixture.host.clone(), "pat-second".into()),
            )
            .await
            .unwrap();
            let invalid = RepositoryCreationOwner::allocate(
                &registry,
                &services.store,
                workspace.id.clone(),
                agent.clone(),
                RepositoryCreationIntent::Loaded {
                    session_id: session_id.clone(),
                },
            )
            .unwrap();
            let refused = services
                .resume_repository_acp_session(
                    (&connection, Some(&callbacks)),
                    &handshake.ordinary.initialize,
                    &agent,
                    work.path(),
                    vec![],
                    invalid,
                )
                .await;
            assert!(
                refused.is_err(),
                "replacement account must not admit original private native history"
            );
            let mut rejected_replay = Vec::new();
            while let Ok(note) = notifications.try_recv() {
                rejected_replay.push(note.params);
            }
            assert!(
                !rejected_replay
                    .iter()
                    .any(|note| note.to_string().contains("Original monitor wake")),
                "refused history must not replay to the receiver"
            );
            if restart {
                assert!(
                    !work.path().join("model.json").exists(),
                    "refused restarted history must not reach the new model server"
                );
            } else {
                let requests: Value =
                    serde_json::from_slice(&std::fs::read(work.path().join("model.json")).unwrap())
                        .unwrap();
                assert_eq!(
                    requests.as_array().unwrap().len(),
                    1,
                    "refused history must not reach the model"
                );
            }
            assert_eq!(
                services
                    .store
                    .get_agent_session(&agent)
                    .await
                    .unwrap()
                    .acp_session_id
                    .as_deref(),
                Some(session_id.as_str())
            );
            with_caller(
                Caller::Daemon,
                services.gitlab_connect_pat(http.fixture.host.clone(), "stored-pat".into()),
            )
            .await
            .unwrap();
            assert!(
                native.is_retired() && native.is_joined(),
                "restored account cannot revive original allocation"
            );
        }
        let loaded = RepositoryCreationOwner::allocate(
            &registry,
            &services.store,
            workspace.id.clone(),
            agent.clone(),
            RepositoryCreationIntent::Loaded {
                session_id: session_id.clone(),
            },
        )
        .unwrap();
        let response = services
            .resume_repository_acp_session(
                (&connection, Some(&callbacks)),
                &handshake.ordinary.initialize,
                &agent,
                work.path(),
                vec![],
                loaded,
            )
            .await
            .expect("actual native held history must pass fresh all-source admission")
            .unwrap();
        assert_eq!(response.response.session_id, session_id);
        physical = response.owner.unwrap();
        native = physical.native_allocation_for_test();
        let bootstrap = response.query.unwrap().pending_review_monitoring().unwrap();
        assert!(
            bootstrap.acknowledged().is_none(),
            "history bootstrap is not a NEW callback ACK"
        );
        let mut replay = Vec::new();
        while let Ok(note) = notifications.try_recv() {
            replay.push(note.params);
        }
        assert!(
            replay
                .iter()
                .any(|note| note.to_string().contains("Original monitor wake")),
            "actual parent replay must use admitted native history"
        );
        let ordinary =
            serde_json::from_value(json!({"type":"text","text":"Ordinary followup control"}))
                .unwrap();
        let followup = intent_acp::session::prompt(
            &connection,
            &session_id,
            vec![ordinary],
            &intent_acp::session::ActivityTracker::new(),
        )
        .await;
        assert!(followup.is_ok(), "resumed native followup: {followup:?}");
        let requests: Value =
            serde_json::from_slice(&std::fs::read(work.path().join("model.json")).unwrap())
                .unwrap();
        assert_eq!(
            requests.as_array().unwrap().len(),
            if restart { 1 } else { 2 }
        );
        let last = requests.as_array().unwrap().last().unwrap();
        assert!(last["messages"]
            .to_string()
            .contains("Intent review monitor observation (not a user instruction)."));
        assert!(last["messages"]
            .to_string()
            .contains("Original monitor wake"));
        let ordinary_turns: Vec<_> = last["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| {
                message["role"] == "user"
                    && message["content"]
                        .to_string()
                        .contains("Ordinary followup control")
            })
            .collect();
        assert_eq!(
            ordinary_turns.len(),
            1,
            "identify the unique ordinary user followup"
        );
        assert_eq!(
            ordinary_turns[0]["content"],
            json!([{
                "type": "text", "text": "Ordinary followup control"
            }]),
            "ordinary user content must remain exact, without monitor framing or extra blocks"
        );
    }
    let changed = with_caller(
        Caller::Daemon,
        services.gitlab_connect_pat(http.fixture.host.clone(), "pat-second".into()),
    )
    .await;
    assert!(changed.is_ok(), "original native retirement: {changed:?}");
    assert!(native.is_retired());
    assert!(native.is_joined());
    assert_eq!(
        services
            .store
            .get_agent_message_by_id(&agent, &saved.id)
            .await
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(
        services
            .store
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap()
            .pending_changes,
        monitor.pending_changes
    );
    if variant == "new" {
        if let Some(path) = std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE") {
            let path = std::path::PathBuf::from(path);
            sqlx::query("VACUUM INTO ?")
                .bind(path.to_str().unwrap())
                .execute(services.store.write_pool())
                .await
                .unwrap();
            let manifest = json!({"agentId":agent,"workspaceId":workspace.id,"messageId":saved.id,
                "resource":target,"sessionId":session_id,
                "origin":"actual Services admitted pinned-native prompt and exact joined retirement; disposable cold daemon input"});
            std::fs::write(
                path.with_extension("json"),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();
        }
    }
    if let Ok(destination) = std::env::var("INTENT_REVIEW_CAPTURE") {
        let final_captures: Value =
            serde_json::from_slice(&std::fs::read(work.path().join("model.json")).unwrap())
                .unwrap();
        std::fs::write(format!("{destination}.{variant}.json"),serde_json::to_vec_pretty(&json!({"scope":"actual Services/Store original admission and pinned native model; controlled forge/model/MCP responder, not manager enqueue or deployment packaging","sessionId":session_id,"messageId":saved.id,"adapterPids":adapter_pids,"initialModelRequests":if restart {Some(&captures)} else {None},"modelRequests":final_captures,"retired":native.is_joined()})).unwrap()).unwrap();
    }
    drop(callback);
    drop(pending);
    drop(physical);
    drop(callbacks);
    drop(connection);
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}
