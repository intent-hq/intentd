//! Opt-in native manager queue composition; does not install or activate production support.
use super::super::*;
use crate::repository_admission::lifecycle::physical_owner::RepositoryCreationIntent;
use intent_acp::callback_registration::CallbackOffer;
use intent_acp::mcp_server::request_context::McpRequestContext;
use intent_acp::review_monitoring::ReviewMonitoringOffer;
use intent_acp::ConnectionHooks;
use intent_core::caller::{with_caller, Caller};
use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState};
use serde_json::{json, Value};
#[tokio::test]
#[ignore = "requires the explicit verified native adapter and Linux containment runner"]
async fn manager_monitor_actual_native_queue_and_account_retirement() {
    let http = crate::repository_read_source::tests::ReadServer::new().await;
    let auth =
        crate::source_control_auth_ops::repository_owner::secret_reader::tests::Fixture::new(
            &http.fixture,
        )
        .await;
    let bus = crate::EventBus::new(auth.service.store.clone());
    let services = Arc::new(auth.service.as_ref().clone().with_event_bus(bus.clone()));
    let manager = Arc::new(crate::AgentManager::new(
        services.as_ref().clone(),
        Arc::new(crate::agent_manager::BusEventSink::new(bus)),
        4,
    ));
    services.attach_agent_manager(&manager);
    let mut workspace = intent_core::chief_workspace();
    workspace.id = intent_core::WorkspaceId::new();
    services.store.insert_workspace(&workspace).await.unwrap();
    let agent = AgentId::new();
    let row: AgentSession = serde_json::from_value(json!({
        "id":agent,"workspaceId":workspace.id,"name":"new monitor original","status":"active",
        "provider":"claude-code","harnessVersion":"3.1","createdAt":"2026-10-10T00:00:00Z","updatedAt":"2026-10-10T00:00:00Z"
    })).unwrap();
    services.store.insert_agent_session(&row).await.unwrap();
    let origin = RepositoryOrigin::allocate(&services, &row).await;
    let (creation, attempt) = origin
        .begin_session(RepositoryCreationIntent::FirstSet)
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
    std::fs::write(
        &launcher,
        include_str!("../../../repository_admission/read_source/native_runtime_peer.mjs"),
    )
    .unwrap();
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
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (notes, notifications) = tokio::sync::mpsc::unbounded_channel();
    let connection = Arc::new(Connection::new(
        child.stdin.take().unwrap(),
        child.stdout.take().unwrap(),
        None,
        ConnectionHooks {
            notifications: Some(notes),
            ..ConnectionHooks::default()
        },
    ));
    let api: Arc<dyn intent_core::WorkspaceApi> = services.clone();
    let workspace_id = workspace.id.clone();
    let agent_id = agent.clone();
    let blueprint =
        ServerBlueprint::new(RepositoryReadOwner::capture(services.clone()), move || {
            WorkspaceMcpServer::for_agent_type(api.clone(), workspace_id.clone(), "default")
                .with_caller_agent_id(Some(agent_id.clone()))
        })
        .with_original_services(services.clone(), row.clone());
    origin.configure_callbacks(EndpointBlueprint {
        server: blueprint,
        command: node.clone(),
        args_before_address: vec![callback_fixture.to_str().unwrap().into()],
        env: BTreeMap::new(),
    });
    origin.state.lock().unwrap().callback_offer = CallbackOffer::V1;
    let (handle, _, _) = super::handle(origin.clone());
    {
        let mut resources = handle
            .execution
            .local
            .as_ref()
            .unwrap()
            .resources
            .lock()
            .unwrap();
        resources.connection = connection.clone();
        resources.notifications = Arc::new(tokio::sync::Mutex::new(notifications));
        resources.child_pid = child.id();
        resources.child = Some(child);
    }
    manager
        .handles
        .lock()
        .unwrap()
        .insert(agent.clone(), handle);
    let mut provider = *intent_providers::find_provider("claude-code").unwrap();
    provider.supports_authenticate = false;
    let handshake = intent_acp::handshake::handshake_with_review_monitoring(
        connection.clone(),
        &provider,
        CallbackOffer::V1,
        ReviewMonitoringOffer::V1,
    )
    .await
    .unwrap();
    let callbacks = handshake.callbacks.unwrap();
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
    let native = opened.owner.as_ref().unwrap().native_allocation_for_test();
    let pending = opened
        .query
        .as_ref()
        .unwrap()
        .pending_review_monitoring()
        .unwrap();
    assert!(pending.acknowledged().is_none());
    let (opened, delivery) = origin.accept_session(attempt, &connection, opened);
    assert!(
        deliver_captured(delivery).await.is_some(),
        "actual manager callback binding and native ACK"
    );
    assert!(pending.acknowledged().is_some());
    let callback = origin
        .state
        .lock()
        .unwrap()
        .endpoint
        .as_ref()
        .unwrap()
        .live
        .as_ref()
        .unwrap()
        .owner
        .as_ref()
        .unwrap()
        .clone();
    let target = intent_core::ReviewTarget {
        repository: intent_core::RepositoryTarget {
            provider: intent_core::RepositoryProvider::Gitlab,
            instance_base_url: http.fixture.descriptor.instance().as_str().into(),
            project_path: "group/project".into(),
        },
        kind: intent_core::RepositoryResourceKind::MergeRequest,
        number: 4,
    };
    let context = callback.mcp_context();
    let scope = McpRequestContext::capture(context.as_ref());
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
            authority = Some(
                crate::repository_read_source::tests::admit_original_monitor(
                    &services,
                    request,
                    &workspace.id,
                    target.clone(),
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
    with_caller(Caller::Daemon, async {
        manager
            .prepare_monitor_turn(
                row,
                authority.clone(),
                observation.delivery,
                "Original manager monitor wake".into(),
                metadata,
            )
            .await
            .unwrap()
            .track(&services)
            .unwrap()
            .enqueue(&services)
            .await
            .unwrap();
        manager
            .clone()
            .try_drain_queue(agent.clone(), workspace.id.clone())
            .await;
    })
    .await;
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(15)),
        async {
            loop {
                let stored = services
                    .store
                    .get_pr_monitor(&monitor.monitor_id)
                    .await
                    .unwrap();
                if stored.pending_changes.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .expect("manager monitor turn must execute and acknowledge exact original transfer");
    let messages = services
        .store
        .get_agent_messages(&agent, None)
        .await
        .unwrap();
    let private = messages
        .iter()
        .find(|message| {
            message
                .content
                .to_string()
                .contains("Original manager monitor wake")
        })
        .expect("actual manager committed wake");
    let captures: Value =
        serde_json::from_slice(&std::fs::read(work.path().join("model.json")).unwrap()).unwrap();
    assert!(captures[0]["messages"]
        .to_string()
        .contains("Intent review monitor observation (not a user instruction)."));
    assert!(captures[0]["messages"]
        .to_string()
        .contains("Original manager monitor wake"));
    with_caller(
        Caller::Daemon,
        services.gitlab_connect_pat(http.fixture.host.clone(), "pat-second".into()),
    )
    .await
    .unwrap();
    assert!(native.is_retired() && native.is_joined());
    assert_eq!(
        services
            .store
            .get_agent_message_by_id(&agent, &private.id)
            .await
            .unwrap()
            .unwrap(),
        *private
    );
    manager.shutdown_with_native_outcome().await.unwrap();
    if let Ok(path) = std::env::var("INTENT_REVIEW_CAPTURE") {
        std::fs::write(format!("{path}.manager.json"),serde_json::to_vec_pretty(&json!({"scope":"actual manager enqueue/worker/committed prompt/ACK with pinned native model; controlled forge/model/MCP responder and installed test handle","sessionId":opened.session_id,"messageId":private.id,"modelRequests":captures,"retired":native.is_joined(),"pendingCleared":true})).unwrap()).unwrap();
    }
}
