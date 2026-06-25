//! `agent.*` service tests over a temp SQLite store: the [`AgentLite`]
//! projection (digest/lastResponse), conversation truncation, the queue
//! lifecycle, send/force semantics, summary, model catalog, and subscriptions.

use std::path::PathBuf;
use std::sync::Arc;

use intent_acp::WorkspaceMcpServer;
use intent_core::{
    now_iso, AgentDelegateInput, AgentId, Error, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use std::time::Duration;

use intent_store::Store;
use serde_json::json;
use tokio::time::timeout;

use intent_core::events::{AGENT_DELETED, AGENT_FAILED, AGENT_IDLE};
use intent_core::{ActorType, Event, EventActor};

use crate::{EventBus, SubscriptionFilter};

use crate::agent_ops::{parse_model_list_output, static_models};
use crate::Services;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("intentd-agentops-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        archived: false,
        archived_at: None,
    }
}

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let services = Services::new(store);
    (tmp, services, ws)
}

async fn setup_with_bus() -> (TempDb, Services, WorkspaceId, EventBus) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let services = Services::new(store);
    let bus = EventBus::new(services.store().clone());
    let services = services.with_event_bus(bus.clone());
    (tmp, services, ws, bus)
}

fn completion_event(
    workspace_id: &WorkspaceId,
    event_type: &str,
    child_id: &AgentId,
    data: serde_json::Value,
) -> Event {
    Event {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(child_id.0.clone()),
            ..Default::default()
        },
        session_id: Some(child_id.0.clone()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

#[tokio::test]
async fn delete_emits_agent_deleted_scoped_to_workspace() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Doomed").await;

    // Subscribe before the delete; no batching -> immediate single-event batch.
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_DELETED.to_string()],
        ..Default::default()
    });

    let r = svc.agent_delete_op(id.clone()).await.expect("delete");
    assert_eq!(r["success"], json!(true));

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event_type, AGENT_DELETED);
    assert_eq!(batch[0].workspace_id, ws);
    assert_eq!(batch[0].data["agentId"].as_str(), Some(id.0.as_str()));
}

#[tokio::test]
async fn delete_skips_emit_when_session_already_gone() {
    let (_t, svc, _ws, bus) = setup_with_bus().await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_DELETED.to_string()],
        ..Default::default()
    });

    let missing = AgentId::from("agent-00000000-0000-0000-0000-00000missing0");
    let r = svc
        .agent_delete_op(missing)
        .await
        .expect("idempotent delete");
    assert_eq!(r["success"], json!(true));

    // Nothing was emitted: the subscription stays empty within the window.
    let res = timeout(Duration::from_millis(300), sub.recv()).await;
    assert!(
        res.is_err(),
        "expected no agent:deleted emit for a missing session"
    );
}

#[tokio::test]
async fn completion_delivery_wakes_oneshot_parent_and_removes_watch() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;

    let sub_id = svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        true,
        None,
    );

    let event = completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({ "agentId": child.0, "lastResponseSummary": "shipped it" }),
    );
    svc.handle_completion_event(&event).await;

    // The parent received exactly one wake message via agent_send_message_op.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(parent_session.messages.len(), 1);

    // The oneShot watch was removed after delivery.
    assert!(svc.find_watches_for_child(&ws, &child).is_empty());
    assert!(!svc.remove_watch(&ws, &sub_id));
}

#[tokio::test]
async fn completion_delivery_leaves_group_watch_for_as4() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;

    svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        true,
        Some("group-1".into()),
    );

    let event = completion_event(
        &ws,
        AGENT_FAILED,
        &child,
        json!({ "agentId": child.0, "error": "boom" }),
    );
    svc.handle_completion_event(&event).await;

    // No wake delivered and the group watch is left in place for AS-4.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert!(parent_session.messages.is_empty());
    assert_eq!(svc.find_watches_for_child(&ws, &child).len(), 1);
}

async fn create_agent(svc: &Services, ws: &WorkspaceId, name: &str) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            Some("auggie:sonnet4.5".into()),
            None,
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

#[tokio::test]
async fn create_then_list_and_get_projects_agent_lite() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Builder").await;

    let agents = svc.agent_list_op(ws.clone()).await.expect("list");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].id, id);
    assert_eq!(agents[0].name, "Builder");
    assert_eq!(agents[0].message_count, 0);

    let got = svc.agent_get_op(id.clone()).await.expect("get");
    assert_eq!(got.id, id);
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}

#[tokio::test]
async fn get_unknown_agent_is_not_found() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_get_op(AgentId::from("agent-00000000-0000-0000-0000-000000000000"))
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::NotFound(_)));
}

#[tokio::test]
async fn list_derives_last_response_and_digest() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Talker").await;
    let content = json!([{
        "type": "text",
        "text": "Intermediate line\nFinal answer here\n<agent_digest>done the thing</agent_digest>",
    }]);
    svc.store()
        .append_agent_message(&id, "assistant", &content, &now_iso())
        .await
        .expect("append");

    let agents = svc.agent_list_op(ws).await.expect("list");
    assert_eq!(agents[0].message_count, 1);
    assert_eq!(agents[0].digest.as_deref(), Some("done the thing"));
    assert_eq!(
        agents[0].last_agent_response.as_deref(),
        Some("Final answer here")
    );
}

#[tokio::test]
async fn get_conversation_truncates_to_limit() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Chatty").await;
    for i in 0..5 {
        let c = json!([{ "type": "text", "text": format!("m{i}") }]);
        svc.store()
            .append_agent_message(&id, "assistant", &c, &now_iso())
            .await
            .expect("append");
    }
    let res = svc
        .agent_get_conversation_op(id.clone(), Some(2))
        .await
        .expect("conv");
    assert_eq!(res["totalMessages"], 5);
    assert_eq!(res["truncated"], true);
    let messages = res["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    // Most-recent kept, oldest→newest order. Wire key is `contentBlocks`
    // (TS `AgentMessage`), never `content`.
    assert_eq!(messages[1]["contentBlocks"][0]["text"], "m4");
    assert!(messages[1].get("content").is_none());
}

#[tokio::test]
async fn rename_and_set_model_persist() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Old").await;
    let r = svc
        .agent_rename_op(id.clone(), "New".into())
        .await
        .expect("rename");
    assert_eq!(r["name"], "New");
    svc.agent_set_model_op(id.clone(), "auggie:opus4.7".into())
        .await
        .expect("setModel");
    let got = svc.agent_get_op(id).await.expect("get");
    assert_eq!(got.name, "New");
    assert!(got.name_explicitly_set);
    assert_eq!(got.model.as_deref(), Some("auggie:opus4.7"));
}

#[tokio::test]
async fn rename_missing_agent_is_internal() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_rename_op(
            AgentId::from("agent-00000000-0000-0000-0000-000000000000"),
            "x".into(),
        )
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::Internal(_)));
}

#[tokio::test]
async fn delete_removes_session() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Doomed").await;
    let r = svc.agent_delete_op(id.clone()).await.expect("delete");
    assert_eq!(r["success"], true);
    assert!(svc.agent_get_op(id).await.is_err());
}

#[tokio::test]
async fn queue_lifecycle_add_get_edit_remove() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Q").await;
    let added = svc
        .agent_queue_message_op(id.clone(), "hello".into(), None)
        .await
        .expect("queue");
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();

    let q = svc.agent_get_queue_op(id.clone()).await.expect("getQueue");
    assert_eq!(q["queue"].as_array().unwrap().len(), 1);
    assert_eq!(q["queue"][0]["content"], "hello");

    svc.agent_edit_queued_message_op(id.clone(), mid.clone(), "edited".into())
        .await
        .expect("edit");
    let q = svc.agent_get_queue_op(id.clone()).await.expect("getQueue");
    assert_eq!(q["queue"][0]["content"], "edited");

    svc.agent_remove_queued_message_op(id.clone(), mid)
        .await
        .expect("remove");
    let q = svc.agent_get_queue_op(id).await.expect("getQueue");
    assert_eq!(q["queue"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn edit_missing_queued_message_errors() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Q").await;
    let err = svc
        .agent_edit_queued_message_op(id, "nope".into(), "x".into())
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::Internal(_)));
}

#[tokio::test]
async fn send_message_delivers_when_agent_exists() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Recv").await;
    let r = svc
        .agent_send_message_op(id.clone(), "do it".into(), Some("m1".into()))
        .await
        .expect("send");
    assert_eq!(r["queued"], false);
    assert_eq!(r["messageId"], "m1");
    let conv = svc.agent_get_conversation_op(id, None).await.expect("conv");
    assert_eq!(conv["totalMessages"], 1);
    assert_eq!(conv["messages"][0]["role"], "user");
}

#[tokio::test]
async fn send_message_auto_queues_for_unknown_agent() {
    let (_t, svc, _ws) = setup().await;
    let id = AgentId::from("agent-00000000-0000-0000-0000-000000000000");
    let r = svc
        .agent_send_message_op(id, "hi".into(), None)
        .await
        .expect("send");
    assert_eq!(r["queued"], true);
    assert_eq!(r["queuedMessage"]["content"], "hi");
}

#[tokio::test]
async fn summary_reports_counts_and_last_response() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Summed").await;
    let content = json!([
        { "type": "tool_use", "name": "read_file" },
        { "type": "text", "text": "all done" },
    ]);
    svc.store()
        .append_agent_message(&id, "assistant", &content, &now_iso())
        .await
        .expect("append");
    let s = svc.agent_summary_op(id.clone()).await.expect("summary");
    assert_eq!(s["agentName"], "Summed");
    assert_eq!(s["messageCount"], 1);
    assert_eq!(s["toolCallCounts"]["read_file"], 1);
    assert_eq!(s["lastResponse"], "all done");
}

#[tokio::test]
async fn get_models_returns_non_empty_catalog() {
    let (_t, svc, _ws) = setup().await;
    let res = svc.agent_get_models_op().await.expect("models");
    let models = res["models"].as_array().unwrap();
    assert!(!models.is_empty());
    assert!(models[0].get("id").is_some());
    assert!(models[0].get("provider").is_some());
}

#[test]
fn static_models_dedupes_and_labels_by_tier() {
    let models = static_models();
    assert!(models
        .iter()
        .any(|m| m["id"] == "haiku4.5" && m["name"] == "haiku4.5 (fast)"));
    // cortex opus appears once though it is both balanced + smart.
    let opus = models
        .iter()
        .filter(|m| m["provider"] == "cortex" && m["id"] == "claude-opus-4-5")
        .count();
    assert_eq!(opus, 1);
}

#[test]
fn parse_model_list_output_extracts_rows() {
    let out = "Available models:\n  - Sonnet 4.5 [sonnet4.5]\n    Balanced general model\n  - Haiku [haiku4.5]\n";
    let rows = parse_model_list_output(out);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "sonnet4.5");
    assert_eq!(rows[0].1, "Sonnet 4.5");
    assert_eq!(rows[0].2.as_deref(), Some("Balanced general model"));
    assert_eq!(rows[1].0, "haiku4.5");
    assert_eq!(rows[1].2, None);
}

#[tokio::test]
async fn subscribe_then_unsubscribe_roundtrips() {
    let (_t, svc, ws) = setup().await;
    let sub = svc
        .agent_subscribe(ws.clone(), vec!["agent:*".into()], None, None)
        .await
        .expect("subscribe");
    let id = sub["subscriptionId"].as_str().unwrap().to_string();
    let r = svc.agent_unsubscribe(ws.clone(), id).await.expect("unsub");
    assert_eq!(r["success"], true);
    let err = svc
        .agent_unsubscribe(ws, "missing".into())
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::Internal(_)));
}

/// A delegated caller (child whose `parentAgentId` is set) delivers the report
/// to the parent via the send-message path and returns the TS-shaped result.
#[tokio::test]
async fn report_to_parent_delivers_for_delegated_caller() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let created = svc
        .agent_create_op(ws.clone(), Some("Child".into()), None, Some(parent.clone()))
        .await
        .expect("create delegated child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let report = "done: shipped the thing";
    let result = svc
        .agent_report_to_parent_op(ws.clone(), json!(report), Some(child))
        .await
        .expect("report delivered");
    assert_eq!(result["ok"], json!(true));
    assert_eq!(result["parentAgentId"].as_str(), Some(parent.0.as_str()));
    assert_eq!(result["reportLength"], json!(report.chars().count() as i64));
    assert!(result["savedAt"].is_string());

    // The report reached the parent's transcript via agent_send_message_op.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(parent_session.messages.len(), 1);
}

/// A non-delegated caller (created directly, no `parentAgentId`) is rejected
/// with `-32603` and the canonical message.
#[tokio::test]
async fn report_to_parent_rejects_non_delegated_caller() {
    let (_t, svc, ws) = setup().await;
    let solo = create_agent(&svc, &ws, "Solo").await;
    let err = svc
        .agent_report_to_parent_op(ws, json!("a report"), Some(solo))
        .await
        .expect_err("not delegated");
    match err {
        Error::Internal(m) => {
            assert_eq!(m, "report_to_parent is only available to delegated agents")
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

/// The RPC front door (no caller context, `caller_agent_id = None`) keeps
/// returning `-32603` exactly as before.
#[tokio::test]
async fn report_to_parent_rejects_rpc_front_door() {
    let (_t, svc, ws) = setup().await;
    let err = svc
        .agent_report_to_parent(ws, json!("a report"), None)
        .await
        .expect_err("no caller context");
    match err {
        Error::Internal(m) => {
            assert_eq!(m, "report_to_parent is only available to delegated agents")
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test]
async fn get_subscriptions_has_stable_shape() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Sub").await;
    let r = svc.agent_get_subscriptions(ws, id).await.expect("subs");
    assert!(r["subscriptions"].is_array());
    assert!(r["delegationGroups"].is_array());
    assert!(r["agentStatuses"].is_object());
}

/// A delegate through the MCP front door (caller set) stamps the child's
/// `parentAgentId`; the same op through the RPC front door (caller `None`)
/// leaves it null.
#[tokio::test]
async fn mcp_delegate_stamps_parent_but_rpc_path_does_not() {
    let (_t, svc, ws) = setup().await;

    // MCP front door: caller set -> child parentAgentId == caller.
    let caller = AgentId::from("agent-00000000-0000-0000-0000-0000000caller");
    let api: Arc<dyn WorkspaceApi> = Arc::new(svc.clone());
    let server =
        WorkspaceMcpServer::new(api, ws.clone()).with_caller_agent_id(Some(caller.clone()));
    let resp = server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delegate_task_workspace-mcp",
                "arguments": { "agentInstructions": "do work" }
            }
        }))
        .await
        .expect("mcp response");
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("tool json");
    let child_id = AgentId::from(parsed["agentId"].as_str().expect("agentId"));
    let child = svc
        .store()
        .get_agent_session(&child_id)
        .await
        .expect("child session");
    assert_eq!(child.parent_agent_id, Some(caller));

    // RPC front door: caller None -> child parentAgentId null.
    let rpc_resp = svc
        .agent_delegate(ws.clone(), AgentDelegateInput::default(), None)
        .await
        .expect("rpc delegate");
    let rpc_child_id = AgentId::from(rpc_resp["agentId"].as_str().expect("rpc agentId"));
    let rpc_child = svc
        .store()
        .get_agent_session(&rpc_child_id)
        .await
        .expect("rpc child session");
    assert_eq!(rpc_child.parent_agent_id, None);
}

/// End-to-end parent-tracking loop driven entirely through the MCP front door
/// (`WorkspaceMcpServer` dispatch -> `Services` -> `Store`): a parent delegates a
/// child (caller set, so the child's `parentAgentId` == parent), then the child
/// reports back via `report_to_parent_workspace-mcp` (caller-aware) and the
/// report lands in the parent's transcript. The same report tool through a
/// caller-less server (the RPC / no-caller path) yields a `-32603` JSON-RPC
/// error. This is the service-level integration coverage chosen over a
/// node-gated UDS E2E so the full loop is exercised deterministically without an
/// external `node` dependency.
#[tokio::test]
async fn mcp_parent_tracking_loop_delegate_then_report_reaches_parent() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let api: Arc<dyn WorkspaceApi> = Arc::new(svc.clone());

    // Parent delegates a child through the MCP front door (caller = parent).
    let parent_server =
        WorkspaceMcpServer::new(api.clone(), ws.clone()).with_caller_agent_id(Some(parent.clone()));
    let resp = parent_server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delegate_task_workspace-mcp",
                "arguments": { "agentInstructions": "do work" }
            }
        }))
        .await
        .expect("delegate response");
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("tool json");
    let child = AgentId::from(parsed["agentId"].as_str().expect("agentId"));

    // The child carries the parent linkage stamped from the caller identity.
    let child_session = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child session");
    assert_eq!(child_session.parent_agent_id, Some(parent.clone()));

    // Child reports back through the MCP front door (caller = child).
    let child_server =
        WorkspaceMcpServer::new(api.clone(), ws.clone()).with_caller_agent_id(Some(child.clone()));
    let report = "done: shipped the thing";
    let report_resp = child_server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "report_to_parent_workspace-mcp",
                "arguments": { "report": report }
            }
        }))
        .await
        .expect("report response");
    let report_text = report_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("report text");
    let report_parsed: serde_json::Value = serde_json::from_str(report_text).expect("report json");
    assert_eq!(report_parsed["ok"], json!(true));
    assert_eq!(
        report_parsed["parentAgentId"].as_str(),
        Some(parent.0.as_str())
    );

    // The report reached the parent's transcript via the send-message path.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(parent_session.messages.len(), 1);
    let delivered = serde_json::to_string(&parent_session.messages).expect("serialize messages");
    assert!(
        delivered.contains(report),
        "parent transcript should contain the report text"
    );

    // RPC / no-caller path: the report tool surfaces a -32603 JSON-RPC error.
    let no_caller_server = WorkspaceMcpServer::new(api, ws.clone());
    let err_resp = no_caller_server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "report_to_parent_workspace-mcp",
                "arguments": { "report": "orphan" }
            }
        }))
        .await
        .expect("error response");
    assert_eq!(err_resp["error"]["code"], json!(-32603));
}

// ===========================================================================
// AS-2: completion-watch registry + auto-subscribe on delegate (immediate mode)
// ===========================================================================

/// The registry helpers register/find/list/remove parent→child watches.
#[tokio::test]
async fn completion_watch_registry_register_find_list_remove() {
    let (_t, svc, ws) = setup().await;
    let parent = AgentId::from("agent-00000000-0000-0000-0000-00000000paren");
    let child_a = AgentId::from("agent-00000000-0000-0000-0000-0000000child");
    let child_b = AgentId::from("agent-00000000-0000-0000-0000-000000childb");

    let sub_a = svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child_a.clone(),
        true,
        None,
    );
    let sub_b = svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child_b.clone(),
        true,
        None,
    );
    assert_ne!(sub_a, sub_b);

    let for_child_a = svc.find_watches_for_child(&ws, &child_a);
    assert_eq!(for_child_a.len(), 1);
    assert_eq!(for_child_a[0].id, sub_a);
    assert!(for_child_a[0].one_shot);
    assert_eq!(for_child_a[0].parent_agent_id, parent);
    assert_eq!(for_child_a[0].child_agent_id, child_a);

    assert_eq!(svc.list_watches_for_parent(&ws, &parent).len(), 2);

    assert!(svc.remove_watch(&ws, &sub_a));
    assert!(!svc.remove_watch(&ws, &sub_a));
    assert!(svc.find_watches_for_child(&ws, &child_a).is_empty());
    assert_eq!(svc.list_watches_for_parent(&ws, &parent).len(), 1);

    assert_eq!(svc.remove_all_for_parent(&ws, &parent), 1);
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
}

/// MCP front door (caller set), default wait mode: exactly one oneShot watch is
/// registered linking the caller (parent) to the freshly created child.
#[tokio::test]
async fn delegate_immediate_registers_one_oneshot_watch_for_mcp_caller() {
    let (_t, svc, ws) = setup().await;
    let caller = AgentId::from("agent-00000000-0000-0000-0000-0000000caller");

    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput::default(),
            Some(caller.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert!(watches[0].one_shot);
    assert_eq!(watches[0].child_agent_id, child);
    assert_eq!(svc.find_watches_for_child(&ws, &child).len(), 1);
}

/// RPC front door (caller `None`): no watch is registered.
#[tokio::test]
async fn delegate_rpc_path_registers_no_watch() {
    let (_t, svc, ws) = setup().await;
    let resp = svc
        .agent_delegate_op(ws.clone(), AgentDelegateInput::default(), None)
        .await
        .expect("rpc delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    assert!(svc.find_watches_for_child(&ws, &child).is_empty());
}

/// `wait_mode == "after_all"` (AS-4): the child is enrolled in the parent's
/// delegation group and a non-oneShot group watch (group_id = Some) is registered
/// instead of an immediate oneShot.
#[tokio::test]
async fn delegate_after_all_enrolls_group_and_registers_group_watch() {
    let (_t, svc, ws) = setup().await;
    let caller = AgentId::from("agent-00000000-0000-0000-0000-0000000caller");
    let input = AgentDelegateInput {
        wait_mode: Some("after_all".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, Some(caller.clone()))
        .await
        .expect("delegate after_all");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let group = svc
        .delegation_group_for_parent(&ws, &caller)
        .expect("group exists");
    assert_eq!(group.expected_agent_ids, vec![child.clone()]);

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert!(!watches[0].one_shot);
    assert_eq!(
        watches[0].group_id.as_deref(),
        Some(group.group_id.as_str())
    );
    assert_eq!(watches[0].child_agent_id, child);
}

/// The deleted-parent guard skips registration when the caller's session is
/// flagged `deleted` (TS `selectIsAgentDeleted`).
#[tokio::test]
async fn delegate_skips_watch_when_parent_deleted() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let mut session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    session.status = intent_core::AgentStatus::Deleted;
    svc.store()
        .update_agent_session(&session)
        .await
        .expect("flag deleted");

    svc.agent_delegate_op(
        ws.clone(),
        AgentDelegateInput::default(),
        Some(parent.clone()),
    )
    .await
    .expect("delegate");
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
}

/// End-to-end through the MCP front door: delegating with a caller registers
/// exactly one oneShot watch for the child returned by the tool.
#[tokio::test]
async fn mcp_delegate_immediate_registers_oneshot_watch() {
    let (_t, svc, ws) = setup().await;
    let caller = AgentId::from("agent-00000000-0000-0000-0000-0000000caller");
    let api: Arc<dyn WorkspaceApi> = Arc::new(svc.clone());
    let server =
        WorkspaceMcpServer::new(api, ws.clone()).with_caller_agent_id(Some(caller.clone()));
    let resp = server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delegate_task_workspace-mcp",
                "arguments": { "agentInstructions": "do work" }
            }
        }))
        .await
        .expect("mcp response");
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("tool json");
    let child = AgentId::from(parsed["agentId"].as_str().expect("agentId"));

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].child_agent_id, child);
    assert!(watches[0].one_shot);
}

// ===========================================================================
// AS-4: after_all delegation groups (aggregate + single wake)
// ===========================================================================

async fn delegate_after_all(svc: &Services, ws: &WorkspaceId, parent: &AgentId) -> AgentId {
    let input = AgentDelegateInput {
        wait_mode: Some("after_all".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, Some(parent.clone()))
        .await
        .expect("delegate after_all");
    AgentId::from(resp["agentId"].as_str().expect("agentId"))
}

async fn parent_message_count(svc: &Services, parent: &AgentId) -> usize {
    svc.store()
        .get_agent_session(parent)
        .await
        .expect("parent session")
        .messages
        .len()
}

async fn parent_messages_text(svc: &Services, parent: &AgentId) -> String {
    let session = svc
        .store()
        .get_agent_session(parent)
        .await
        .expect("parent session");
    serde_json::to_string(&session.messages).expect("serialize messages")
}

/// Two after_all delegates from one parent share a single group whose expected
/// set has both children, with two non-oneShot group watches and zero oneShots.
#[tokio::test]
async fn two_after_all_delegates_share_one_group() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    let group = svc
        .delegation_group_for_parent(&ws, &parent)
        .expect("group exists");
    assert_eq!(group.expected_agent_ids.len(), 2);
    assert!(group.expected_agent_ids.contains(&c1));
    assert!(group.expected_agent_ids.contains(&c2));

    let watches = svc.list_watches_for_parent(&ws, &parent);
    assert_eq!(watches.len(), 2);
    assert!(watches.iter().all(|w| !w.one_shot));
    assert!(watches
        .iter()
        .all(|w| w.group_id.as_deref() == Some(group.group_id.as_str())));
}

/// child idle (no fire) -> parent idle (seal, still incomplete, no fire) ->
/// second child idle -> exactly one aggregated wake; group + watches removed.
#[tokio::test]
async fn group_fires_once_after_parent_then_remaining_child() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c1,
        json!({ "agentId": c1.0, "lastResponseSummary": "one" }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c2,
        json!({ "agentId": c2.0, "lastResponseSummary": "two" }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    assert!(svc.delegation_group_for_parent(&ws, &parent).is_none());
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
}

/// Both children idle before the parent: no fire until the parent idles, then a
/// single aggregated wake.
#[tokio::test]
async fn group_fires_on_parent_idle_when_children_already_done() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    for c in [&c1, &c2] {
        svc.handle_completion_event(&completion_event(
            &ws,
            AGENT_IDLE,
            c,
            json!({ "agentId": c.0 }),
        ))
        .await;
    }
    assert_eq!(parent_message_count(&svc, &parent).await, 0);

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    assert!(svc.delegation_group_for_parent(&ws, &parent).is_none());
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
}

/// A deleted child counts toward completion as `partial`: after the parent
/// seals, one deleted + one idle child yields a single partial aggregated wake.
#[tokio::test]
async fn group_partial_when_child_deleted() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_DELETED,
        &c1,
        json!({ "agentId": c1.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 0);

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c2,
        json!({ "agentId": c2.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    let text = parent_messages_text(&svc, &parent).await;
    assert!(
        text.contains("partial"),
        "wake should report partial status"
    );
}

/// The group fires exactly once: a duplicate child completion and a second parent
/// idle after delivery do not deliver a second aggregated wake.
#[tokio::test]
async fn group_no_double_fire() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    for c in [&c1, &c2] {
        svc.handle_completion_event(&completion_event(
            &ws,
            AGENT_IDLE,
            c,
            json!({ "agentId": c.0 }),
        ))
        .await;
    }
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);

    // Duplicate child completion: the group is already gone -> no-op.
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c2,
        json!({ "agentId": c2.0 }),
    ))
    .await;
    // Second parent idle: no open group to seal -> no-op.
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
}
