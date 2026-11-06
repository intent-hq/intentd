//! Actual Services + Store + source admission + ACP requests with a controlled
//! recipient. This does not qualify native model execution or containment.
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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn new_monitor_delivery_retains_actual_native_allocation_until_account_join() {
    new_monitor_native_case("live").await;
}

#[tokio::test]
async fn new_monitor_delivery_refuses_account_replacement_after_commit() {
    new_monitor_native_case("before-transfer").await;
}

#[tokio::test]
async fn new_monitor_delivery_preserves_uncertain_native_join_and_pending_ack() {
    new_monitor_native_case("rejected-join").await;
}

#[tokio::test]
async fn new_monitor_delivery_retires_original_native_on_external_secret_change() {
    new_monitor_native_case("external-secret").await;
}

#[tokio::test]
async fn new_monitor_delivery_retires_original_native_on_current_provider_rejection() {
    new_monitor_native_case("provider-rejection").await;
}

#[tokio::test]
async fn new_monitor_delivery_retires_original_native_on_attached_source_override() {
    new_monitor_native_case("source-override").await;
}

async fn new_monitor_native_case(variant: &'static str) {
    use crate::repository_admission::lifecycle::physical_owner::{
        RepositoryCreationIntent, RepositoryCreationOwner,
    };
    use crate::repository_admission::read_request::RepositoryReadOwner;
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
    let (client, peer) = tokio::io::duplex(16384);
    let (read, write) = tokio::io::split(client);
    let connection = Arc::new(Connection::new(
        write,
        read,
        None,
        ConnectionHooks::default(),
    ));
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seen = requests.clone();
    let server = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut context = Value::Null;
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            seen.lock().unwrap().push(request.clone());
            let result = match request["method"].as_str().unwrap() {
                "initialize" => {
                    json!({"protocolVersion":1,"agentCapabilities":{},"authMethods":[],"_meta":{"reviewMonitoring":1,"intentCallbackRegistration":{"version":1,"method":"_intent/session/register_mcp_callback"}}})
                }
                "session/new" => {
                    context = request["params"]["_meta"]["reviewMonitoringContext"].clone();
                    json!({"sessionId":"native-monitor","_meta":{"reviewMonitoring":1,"reviewMonitoringContext":context,"intentCallbackRegistration":{"version":1,"queryReceipt":"11111111-1111-4111-8111-111111111111"}}})
                }
                "_intent/session/register_mcp_callback" => {
                    json!({"version":1,"sessionId":"native-monitor","queryReceipt":request["params"]["queryReceipt"],"registrationId":request["params"]["registrationId"],"serverName":"intent-callback-22222222-2222-4222-8222-222222222222","status":"acknowledged","result":{"added":["intent-callback-22222222-2222-4222-8222-222222222222"],"removed":[],"errors":{}}})
                }
                "session/prompt" => {
                    assert_eq!(request["params"]["sessionId"], "native-monitor");
                    assert_eq!(
                        request["params"]["_meta"]["reviewMonitoringMessage"]["messageId"],
                        "native-monitor-wake"
                    );
                    json!({"stopReason":"end_turn"})
                }
                "_intent/session/retire_review_context" => {
                    assert_eq!(request["params"]["sessionId"], "native-monitor");
                    assert_eq!(request["params"]["loadAttemptId"], context["loadAttemptId"]);
                    json!({"retired":variant != "rejected-join"})
                }
                other => panic!("unexpected original new delivery request {other}"),
            };
            write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
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
            "/fixture",
            vec![],
            creation,
            false,
        )
        .await
        .unwrap();
    let physical = opened.owner.unwrap();
    let native = physical.native_allocation_for_test();
    let query = opened.query.unwrap();
    let pending = query.pending_review_monitoring().unwrap();
    let owner = RepositoryReadOwner::capture(services.clone())
        .unwrap()
        .with_monitor_recipient(Some(pending.clone()), &connection, "native-monitor");
    let callback = physical.callback().with_read_owner(Ok(owner));
    assert!(pending.acknowledged().is_none());
    let registration = query.registration(
        CallbackStdioServer::new(
            "fixture-mcp".into(),
            vec![],
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
                "native-monitor".into(),
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
        if variant == "before-transfer" {
            services
                .gitlab_connect_pat(http.fixture.host.clone(), "pat-second".into())
                .await
                .unwrap();
        }
        let result = intent_acp::session::prompt_with_required_admission(
            &connection,
            "native-monitor",
            prompt,
            &intent_acp::session::ActivityTracker::new(),
            Box::new(prepared),
        )
        .await;
        assert_eq!(
            result.is_ok(),
            variant != "before-transfer",
            "original account final transfer"
        );
        message
    })
    .await;
    assert!(!native.is_retired());
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "session/prompt")
            .count(),
        usize::from(variant != "before-transfer")
    );
    if variant == "before-transfer" {
        // No private transfer occurred; only explicit final cleanup retires this
        // still-ordinary allocation. A committed row supplied no permission.
        native.join().await.unwrap();
    } else if variant == "external-secret" {
        let expected = auth.request();
        let reader = services.gitlab_repository_secret_reader().unwrap();
        services
            .gitlab_secret_store
            .store(
                intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                "external-replacement",
            )
            .unwrap();
        assert!(reader.load(&expected).await.is_err());
        assert!(
            native.is_retired(),
            "proof invalidation must close eligibility synchronously"
        );
        services.store_tasks.drain_finite().await;
        assert_eq!(
            services
                .gitlab_secret_store
                .load(intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT)
                .unwrap()
                .as_deref(),
            Some("external-replacement")
        );
    } else if variant == "provider-rejection" {
        http.status("/api/v4/projects/group%2Fproject/merge_requests/4", 401);
        let before = http.count();
        let poll = with_caller(
            Caller::Daemon,
            authority.run_poll(Box::pin(authority.read_poll(&services))),
        )
        .await;
        assert!(
            poll.is_err() || poll.unwrap().is_err(),
            "current provider rejection cannot become private data"
        );
        assert!(
            http.count() > before,
            "rejection must come from actual HTTP"
        );
        assert!(
            native.is_retired(),
            "current rejection must close eligibility"
        );
        services.store_tasks.drain_finite().await;
    } else if variant == "source-override" {
        let replacement = crate::test_support::test_tempdir("native-source-override");
        let replaced = services.as_ref().clone().with_gitlab_secret_store(
            intent_core::FileSecretStore::with_path(replacement.path().join("secrets.json")),
        );
        assert!(
            native.is_retired(),
            "attached source replacement must retire the original directory synchronously"
        );
        assert!(replaced.gitlab_repository_settled_connection().is_err());
        services.store_tasks.drain_finite().await;
    } else {
        let result = with_caller(
            Caller::Daemon,
            services.gitlab_connect_pat(http.fixture.host.clone(), "pat-second".into()),
        )
        .await;
        assert_eq!(
            result.is_ok(),
            variant != "rejected-join",
            "credential publication and native join are distinct outcomes"
        );
    }
    assert!(
        native.is_retired(),
        "new private delivery omitted original credential retirement enrollment"
    );
    assert_eq!(
        native.is_joined(),
        variant != "rejected-join",
        "only exact positive remote completion is a join"
    );
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "_intent/session/retire_review_context")
            .count(),
        1
    );
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
        monitor.pending_changes,
        "native retirement is not delivery acknowledgement"
    );
    drop(physical);
    server.abort();
}
