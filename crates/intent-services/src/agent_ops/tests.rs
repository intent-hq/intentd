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
use intent_store::Store;
use serde_json::json;

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
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
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

async fn create_agent(svc: &Services, ws: &WorkspaceId, name: &str) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            Some("auggie:sonnet4.5".into()),
            None,
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
async fn agent_lite_carries_metadata_and_activity_fields() {
    let (_t, svc, ws) = setup().await;
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Spec".into()),
            None,
            Some("implementor".into()),
            None,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let lite = svc.agent_get_op(id).await.expect("get");
    let v = serde_json::to_value(&lite).unwrap();
    // Nested metadata object (iOS `parseAgent` reads metadata.specialist /
    // isBackground / createdByAgentId).
    assert_eq!(v["metadata"]["specialist"], "implementor");
    assert_eq!(v["metadata"]["isBackground"], false);
    assert!(v["metadata"].get("createdByAgentId").is_none());
    // Activity flags are present; the headless BE has no live stream so all false.
    assert_eq!(v["isStreaming"], false);
    assert_eq!(v["isProcessing"], false);
    assert_eq!(v["isResponding"], false);
    assert!(v["lastActivity"].is_string());
}

#[tokio::test]
async fn agent_lite_metadata_created_by_agent_id_from_parent() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let lite = svc.agent_get_op(child).await.expect("get");
    let v = serde_json::to_value(&lite).unwrap();
    assert_eq!(v["metadata"]["createdByAgentId"], parent.0);
    // No specialist supplied → omitted from metadata.
    assert!(v["metadata"].get("specialist").is_none());
}

#[tokio::test]
async fn agent_lite_derives_last_user_message() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Chatter").await;
    let content = json!([{ "type": "text", "text": "please do the thing" }]);
    svc.store()
        .append_agent_message(&id, "user", &content, &now_iso())
        .await
        .expect("append");
    let lite = svc.agent_get_op(id).await.expect("get");
    assert_eq!(
        lite.last_user_message.as_deref(),
        Some("please do the thing")
    );
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
    assert_eq!(added["success"], true);
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();
    // iOS-required wire shape: {id, content, queuedAt, position} (no createdAt/agentId).
    assert_eq!(added["queuedMessage"]["position"], 0);
    assert!(added["queuedMessage"]["queuedAt"].is_string());
    assert!(added["queuedMessage"].get("createdAt").is_none());
    assert!(added["queuedMessage"].get("agentId").is_none());

    let q = svc.agent_get_queue_op(id.clone()).await.expect("getQueue");
    assert_eq!(q["success"], true);
    assert_eq!(q["queue"].as_array().unwrap().len(), 1);
    assert_eq!(q["queue"][0]["content"], "hello");
    assert_eq!(q["queue"][0]["position"], 0);
    assert!(q["queue"][0]["queuedAt"].is_string());

    let edited = svc
        .agent_edit_queued_message_op(id.clone(), mid.clone(), "edited".into())
        .await
        .expect("edit");
    assert_eq!(edited["queuedMessage"]["position"], 0);
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
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
        )
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
