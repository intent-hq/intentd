//! `agent.*` service tests over a temp SQLite store: the [`AgentLite`]
//! projection (digest/lastResponse), conversation truncation, the queue
//! lifecycle, send/force semantics, summary, model catalog, and subscriptions.

use std::path::PathBuf;
use std::sync::Arc;

use intent_acp::WorkspaceMcpServer;
use intent_core::{
    now_iso, AgentDelegateInput, AgentId, Error, NoteCreate, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use std::time::Duration;

use intent_store::{NewEvent, Store};
use serde_json::json;
use tokio::time::timeout;

use intent_core::events::{AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_SESSION_STATS_CHANGED};
use intent_core::{ActorType, Event, EventActor, SessionStats};

use crate::{EventBus, SubscriptionFilter};

use crate::agent_ops::{parse_model_list_output, parse_session_stats_output, static_models};
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
        token_usage: None,
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
            None,
            None,
            false,
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
async fn agent_create_honors_client_supplied_agent_id() {
    // The FE (`UnifiedAgentFactory`) pre-mints an `agent-{uuid}` and immediately
    // addresses `agent.sendMessage` at it. When the daemon adopts the id
    // verbatim, the follow-up send lands on a persisted session instead of
    // `-32602 not found: agent session` (the create+send race this task fixes).
    let (_t, svc, ws) = setup().await;
    let requested = AgentId::from(format!("agent-{}", uuid::Uuid::new_v4()).as_str());
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Client-Minted".into()),
            None,
            None,
            None,
            None,
            false,
            Some(requested.clone()),
        )
        .await
        .expect("create honors client id");
    assert_eq!(created["agent"]["id"].as_str(), Some(requested.0.as_str()));
    // Round-trip through the store proves the session is addressable at the
    // client-supplied id.
    let got = svc.agent_get_op(requested.clone()).await.expect("get");
    assert_eq!(got.id, requested);
}

#[tokio::test]
async fn agent_create_rejects_malformed_client_agent_id() {
    // Anything other than `agent-{uuid}` is `-32602` (PROTOCOL §5.5 / §9): a
    // stray/hand-typed id must not collide with future daemon-minted ids.
    let (_t, svc, ws) = setup().await;
    for bad in ["not-an-agent", "agent-", "agent-not-a-uuid", ""] {
        let err = svc
            .agent_create_op(
                ws.clone(),
                None,
                None,
                None,
                None,
                None,
                false,
                Some(AgentId::from(bad)),
            )
            .await
            .expect_err("malformed id must be rejected");
        assert!(
            matches!(err, Error::InvalidParams(_)),
            "expected InvalidParams for {bad:?}, got {err:?}"
        );
    }
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
            None,
            false,
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
    // Activity flags are present; an idle agent (no worker, no watches) reports
    // every flag false.
    assert_eq!(v["isStreaming"], false);
    assert_eq!(v["isProcessing"], false);
    assert_eq!(v["isResponding"], false);
    assert_eq!(v["isWaitingOnTool"], false);
    assert_eq!(v["isWaitingForOtherAgents"], false);
    // `waitingForAgentIds` is always present (never null/omitted); an idle agent
    // with no pending completion watches reports an empty array.
    assert_eq!(v["waitingForAgentIds"], json!([]));
    assert!(v["lastActivity"].is_string());
}

#[tokio::test]
async fn agent_lite_activity_flags_reflect_busy_waiting_state() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;

    // An active worker draining an in-flight turn whose latest block is a
    // `tool_use` awaiting its result: isResponding + isWaitingOnTool.
    svc.set_test_busy(&parent, true);
    svc.set_live_turn(
        &parent,
        "msg-1",
        vec![json!({
            "type": "tool_use",
            "id": "msg-1:0",
            "name": "read_file",
            "input": {},
            "toolCallId": "call-1"
        })],
    );
    // The parent also parents a pending completion watch: isWaitingForOtherAgents.
    svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        true,
        None,
    );

    let lite = svc.agent_get_op(parent.clone()).await.expect("get");
    let v = serde_json::to_value(&lite).unwrap();
    assert_eq!(v["isResponding"], true);
    assert_eq!(v["isWaitingOnTool"], true);
    assert_eq!(v["isWaitingForOtherAgents"], true);
    // The waiting-on id list mirrors the bool: it carries the specific child
    // agent the parent's pending completion watch is registered against.
    assert_eq!(v["waitingForAgentIds"], json!([child.0]));

    // Once the tool result lands, the in-flight turn is no longer blocked on the
    // tool: still responding, but no longer waiting on it.
    svc.set_live_turn(
        &parent,
        "msg-1",
        vec![
            json!({
                "type": "tool_use",
                "id": "msg-1:0",
                "name": "read_file",
                "input": {},
                "toolCallId": "call-1"
            }),
            json!({
                "type": "tool_result",
                "id": "msg-1:1",
                "tool_use_id": "call-1",
                "output": "ok",
                "is_error": false
            }),
        ],
    );
    let v = serde_json::to_value(svc.agent_get_op(parent.clone()).await.expect("get")).unwrap();
    assert_eq!(v["isResponding"], true);
    assert_eq!(v["isWaitingOnTool"], false);
    assert_eq!(v["isWaitingForOtherAgents"], true);
    assert_eq!(v["waitingForAgentIds"], json!([child.0]));

    // A second watch against the SAME child must not duplicate the id in the
    // waiting-on list (distinct child ids, registration order).
    svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        true,
        None,
    );
    let v = serde_json::to_value(svc.agent_get_op(parent.clone()).await.expect("get")).unwrap();
    assert_eq!(v["waitingForAgentIds"], json!([child.0]));

    // The child has no worker and parents no watches: every flag false and the
    // waiting-on id list is the empty array (never null/omitted).
    let cv = serde_json::to_value(svc.agent_get_op(child).await.expect("get")).unwrap();
    assert_eq!(cv["isResponding"], false);
    assert_eq!(cv["isWaitingOnTool"], false);
    assert_eq!(cv["isWaitingForOtherAgents"], false);
    assert_eq!(cv["waitingForAgentIds"], json!([]));
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
            None,
            false,
            None,
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
        .agent_get_conversation_op(id.clone(), Some(2), None)
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

/// TA-2 / §5.5: `agent.getConversation` exposes an additive opaque `nextToken`
/// that walks backward to older pages; the page array stays oldest→newest and
/// the token is `null` once the oldest message has been returned. An absent
/// limit uses the default page (50) and clamps over-max requests to 200.
#[tokio::test]
async fn get_conversation_paginates_with_opaque_next_token() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Pager").await;
    for i in 0..5 {
        let c = json!([{ "type": "text", "text": format!("m{i}") }]);
        svc.store()
            .append_agent_message(&id, "assistant", &c, &now_iso())
            .await
            .expect("append");
    }

    // Page 1: newest two, oldest→newest within the page, nextToken present.
    let p1 = svc
        .agent_get_conversation_op(id.clone(), Some(2), None)
        .await
        .expect("p1");
    assert_eq!(p1["totalMessages"], 5);
    assert_eq!(p1["truncated"], true);
    let m1 = p1["messages"].as_array().unwrap();
    assert_eq!(m1.len(), 2);
    assert_eq!(m1[0]["contentBlocks"][0]["text"], "m3");
    assert_eq!(m1[1]["contentBlocks"][0]["text"], "m4");
    let t1 = p1["nextToken"].as_str().expect("nextToken").to_string();
    // Opaque: not a bare numeric offset.
    assert!(t1.parse::<u64>().is_err());

    // Page 2 follows the token to the next-older window.
    let p2 = svc
        .agent_get_conversation_op(id.clone(), Some(2), Some(t1))
        .await
        .expect("p2");
    let m2 = p2["messages"].as_array().unwrap();
    assert_eq!(m2[0]["contentBlocks"][0]["text"], "m1");
    assert_eq!(m2[1]["contentBlocks"][0]["text"], "m2");
    let t2 = p2["nextToken"].as_str().expect("nextToken2").to_string();

    // Page 3 is the final page: oldest message, no further token.
    let p3 = svc
        .agent_get_conversation_op(id.clone(), Some(2), Some(t2))
        .await
        .expect("p3");
    let m3 = p3["messages"].as_array().unwrap();
    assert_eq!(m3.len(), 1);
    assert_eq!(m3[0]["contentBlocks"][0]["text"], "m0");
    assert!(p3["nextToken"].is_null());
    assert_eq!(p3["truncated"], false);

    // No limit → default page returns all five with no token; an over-max limit
    // clamps to 200 and likewise fits all five in one page.
    let all = svc
        .agent_get_conversation_op(id.clone(), None, None)
        .await
        .expect("all");
    assert_eq!(all["messages"].as_array().unwrap().len(), 5);
    assert!(all["nextToken"].is_null());
    let clamped = svc
        .agent_get_conversation_op(id, Some(10_000), None)
        .await
        .expect("clamped");
    assert_eq!(clamped["messages"].as_array().unwrap().len(), 5);
    assert!(clamped["nextToken"].is_null());
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
        .agent_edit_queued_message_op(id.clone(), mid.clone(), "edited".into(), None)
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
        .agent_edit_queued_message_op(id, "nope".into(), "x".into(), None)
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::Internal(_)));
}

#[tokio::test]
async fn remove_queued_message_is_idempotent_for_unknown_id() {
    // Removing a message that's no longer in the BE queue (e.g. after a daemon
    // restart, or after the FE's seeded mirror diverged) must succeed so the
    // FE's optimistic delete sticks rather than rolling back.
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Q").await;
    let r = svc
        .agent_remove_queued_message_op(id, "msg-does-not-exist".into())
        .await
        .expect("idempotent remove");
    assert_eq!(r["success"], true);
}

#[tokio::test]
async fn remove_queued_message_is_idempotent_for_unknown_agent() {
    // Same idempotency contract when the agent has never had a queue at all
    // (no entry in the in-memory map).
    let (_t, svc, _ws) = setup().await;
    let unknown = AgentId::from("agent-00000000-0000-0000-0000-000000000000");
    let r = svc
        .agent_remove_queued_message_op(unknown, "anything".into())
        .await
        .expect("idempotent remove on unknown agent");
    assert_eq!(r["success"], true);
}

#[tokio::test]
async fn queue_message_emits_queue_updated_with_snapshot() {
    // `agent.queueMessage` must publish `agent:queue:updated` carrying the
    // current queue snapshot so subscribed FE clients mirror the live queue
    // (PROTOCOL §6.5).
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Q").await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_QUEUE_UPDATED.to_string()],
        ..Default::default()
    });

    let added = svc
        .agent_queue_message_op(id.clone(), "first".into(), None)
        .await
        .expect("queue");
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert!(!batch.is_empty(), "expected at least one event");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated emitted");
    assert_eq!(evt.workspace_id, ws);
    assert_eq!(evt.data["agentId"].as_str(), Some(id.0.as_str()));
    let queue = evt.data["queue"].as_array().expect("queue array");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0]["id"].as_str(), Some(mid.as_str()));
    assert_eq!(queue[0]["content"], "first");
    assert_eq!(queue[0]["position"], 0);
}

#[tokio::test]
async fn remove_queued_message_emits_queue_updated_only_when_present() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Q").await;

    // Seed one queued message, then drain the events for the seed enqueue.
    let added = svc
        .agent_queue_message_op(id.clone(), "first".into(), None)
        .await
        .expect("queue");
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_QUEUE_UPDATED.to_string()],
        ..Default::default()
    });

    // Idempotent remove of an unknown id does not emit (queue did not change).
    let r = svc
        .agent_remove_queued_message_op(id.clone(), "nope".into())
        .await
        .expect("idempotent remove");
    assert_eq!(r["success"], true);
    let none = timeout(Duration::from_millis(200), sub.recv()).await;
    assert!(none.is_err(), "no event when nothing was removed");

    // Real remove emits with the empty snapshot.
    svc.agent_remove_queued_message_op(id.clone(), mid)
        .await
        .expect("remove");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated emitted on real remove");
    assert_eq!(evt.data["agentId"].as_str(), Some(id.0.as_str()));
    assert!(evt.data["queue"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn editing_flag_excludes_message_from_dequeue() {
    // PROTOCOL §5.5/§6.5 invariant: a queued entry with `editing = true` is
    // excluded from the ready-to-send queue. `dequeue_message` must skip past
    // it and surface a later ready-to-send entry; with only-editing entries
    // remaining, `dequeue_message` returns `None` and `has_ready_to_send` is
    // false (so the agent is allowed to go idle).
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Q").await;

    let a = svc
        .agent_queue_message_op(id.clone(), "first".into(), None)
        .await
        .expect("queue first");
    let a_mid = a["queuedMessage"]["id"].as_str().unwrap().to_string();
    let b = svc
        .agent_queue_message_op(id.clone(), "second".into(), None)
        .await
        .expect("queue second");
    let b_mid = b["queuedMessage"]["id"].as_str().unwrap().to_string();

    // Mark the FIRST entry as under edit.
    let edited = svc
        .agent_edit_queued_message_op(id.clone(), a_mid.clone(), "first".into(), Some(true))
        .await
        .expect("mark editing");
    assert_eq!(
        edited["queuedMessage"]["editing"], true,
        "editing flag surfaced on the wire shape"
    );
    assert!(svc.has_ready_to_send(&id), "second is still ready-to-send");

    // Dequeue must skip the under-edit head and surface the second entry.
    let next = svc
        .dequeue_message(&id)
        .expect("dequeues non-editing entry");
    assert_eq!(next.id, b_mid, "dequeue skipped the editing entry");

    // With only the under-edit entry remaining, the agent has nothing ready-to-send.
    assert!(
        !svc.has_ready_to_send(&id),
        "editing-only queue is treated as empty for the idle invariant",
    );
    assert!(
        svc.dequeue_message(&id).is_none(),
        "dequeue returns None for an editing-only queue",
    );

    // Snapshot still carries the under-edit entry (so the FE can render it).
    let q = svc.queue_snapshot(&id);
    assert_eq!(q.len(), 1);
    assert_eq!(q[0]["id"].as_str(), Some(a_mid.as_str()));
    assert_eq!(q[0]["editing"], true);
}

#[tokio::test]
async fn clearing_editing_flag_emits_queue_updated() {
    // Toggling `editing` via `editQueuedMessage` must publish
    // `agent:queue:updated` carrying the post-edit snapshot, regardless of
    // whether the content actually changed.
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Q").await;
    let added = svc
        .agent_queue_message_op(id.clone(), "draft".into(), None)
        .await
        .expect("queue");
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_QUEUE_UPDATED.to_string()],
        ..Default::default()
    });

    // editing: false → true
    svc.agent_edit_queued_message_op(id.clone(), mid.clone(), "draft".into(), Some(true))
        .await
        .expect("mark editing");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated emitted on editing: true");
    assert_eq!(evt.data["queue"][0]["editing"], true);

    // editing: true → false (save) — must emit again with the cleared flag.
    svc.agent_edit_queued_message_op(id.clone(), mid.clone(), "saved".into(), Some(false))
        .await
        .expect("save edit");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated emitted on editing: false");
    assert_eq!(evt.data["queue"][0]["content"], "saved");
    assert!(
        evt.data["queue"][0].get("editing").is_none(),
        "editing flag omitted from the wire shape when false",
    );
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
    let conv = svc
        .agent_get_conversation_op(id, None, None)
        .await
        .expect("conv");
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
            None,
            false,
            None,
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
    // A freshly created agent watches nobody, so both lists are empty.
    assert!(r["subscriptions"].as_array().expect("array").is_empty());
    assert!(r["delegationGroups"].as_array().expect("array").is_empty());
}

/// After an immediate (default) delegate, `getSubscriptions(parent)` lists the
/// oneShot watch with `actorIds = [child]` and no delegation group.
#[tokio::test]
async fn get_subscriptions_lists_immediate_delegate_watch() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput::default(),
            Some(parent.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let r = svc
        .agent_get_subscriptions(ws, parent.clone())
        .await
        .expect("subs");
    let subs = r["subscriptions"].as_array().expect("array");
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0]["oneShot"], json!(true));
    assert_eq!(subs[0]["agentId"], json!(parent.0));
    assert_eq!(subs[0]["actorIds"], json!([child.0]));
    assert_eq!(subs[0]["delegationGroup"], serde_json::Value::Null);
    assert!(r["delegationGroups"].as_array().expect("array").is_empty());
}

/// After an `after_all` delegate, the watch is a non-oneShot group watch and one
/// `delegationGroups` entry lists the child in `expectedAgentIds` with the wire
/// `awaitMode` mapped from `after_all` to `"all"`.
#[tokio::test]
async fn get_subscriptions_lists_after_all_group() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = delegate_after_all(&svc, &ws, &parent).await;

    let r = svc
        .agent_get_subscriptions(ws, parent.clone())
        .await
        .expect("subs");
    let subs = r["subscriptions"].as_array().expect("array");
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0]["oneShot"], json!(false));
    assert_eq!(subs[0]["actorIds"], json!([child.0]));
    assert_eq!(subs[0]["delegationGroup"]["awaitMode"], json!("all"));

    let groups = r["delegationGroups"].as_array().expect("array");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["parentAgentId"], json!(parent.0));
    assert_eq!(groups[0]["awaitMode"], json!("all"));
    assert_eq!(groups[0]["expectedAgentIds"], json!([child.0]));
}

/// `cancelSubscriptions` drops the parent's watches and groups; a second cancel
/// with nothing left still returns `{ success: true }`.
#[tokio::test]
async fn cancel_subscriptions_clears_watches_and_groups_idempotently() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let _c1 = delegate_after_all(&svc, &ws, &parent).await;
    let _c2 = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput::default(),
            Some(parent.clone()),
        )
        .await
        .expect("delegate");

    let cancel = svc
        .agent_cancel_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("cancel");
    assert_eq!(cancel, json!({ "success": true }));

    let r = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert!(r["subscriptions"].as_array().expect("array").is_empty());
    assert!(r["delegationGroups"].as_array().expect("array").is_empty());

    // Idempotent: cancelling again with nothing left still succeeds.
    let again = svc
        .agent_cancel_subscriptions(ws, parent)
        .await
        .expect("cancel again");
    assert_eq!(again, json!({ "success": true }));
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
// Delegate first-message delivery: the child must receive its instructions and
// start its turn (PROTOCOL §5.5). Without a runtime `AgentManager` attached the
// delivery falls back to the store-only persist path (`agent_send_message_op`),
// so the child's transcript carries exactly one `user` message whose content is
// resolved by the documented fallback chain.
// ===========================================================================

async fn child_session_messages_json(svc: &Services, child: &AgentId) -> String {
    let session = svc
        .store()
        .get_agent_session(child)
        .await
        .expect("child session");
    serde_json::to_string(&session.messages).expect("serialize child messages")
}

/// Explicit `agentInstructions` become the child's first message.
#[tokio::test]
async fn delegate_delivers_agent_instructions_as_child_first_message() {
    let (_t, svc, ws) = setup().await;
    let input = AgentDelegateInput {
        agent_instructions: Some("build the thing".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let conv = svc
        .agent_get_conversation_op(child.clone(), None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 1, "child got exactly one message");
    assert_eq!(conv["messages"][0]["role"], "user");
    assert!(
        child_session_messages_json(&svc, &child)
            .await
            .contains("build the thing"),
        "child first message carries the agentInstructions"
    );
}

/// With no `agentInstructions`, the child's first message falls back to
/// `taskText`.
#[tokio::test]
async fn delegate_falls_back_to_task_text_for_child_first_message() {
    let (_t, svc, ws) = setup().await;
    let input = AgentDelegateInput {
        task_text: Some("the task text".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let conv = svc
        .agent_get_conversation_op(child.clone(), None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 1);
    assert!(child_session_messages_json(&svc, &child)
        .await
        .contains("the task text"));
}

/// `agentInstructions` take priority over `taskText` when both are present.
#[tokio::test]
async fn delegate_prefers_agent_instructions_over_task_text() {
    let (_t, svc, ws) = setup().await;
    let input = AgentDelegateInput {
        agent_instructions: Some("instructions win".into()),
        task_text: Some("task text loses".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let body = child_session_messages_json(&svc, &child).await;
    assert!(body.contains("instructions win"));
    assert!(!body.contains("task text loses"));
}

/// With neither `agentInstructions` nor `taskText`, the child's first message
/// falls back to the linked task note's content.
#[tokio::test]
async fn delegate_falls_back_to_task_note_content_for_child_first_message() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Task title".into(),
                content: Some("note content body".into()),
                tags: None,
                parent_id: None,
            },
            None,
        )
        .await
        .expect("create note");
    let input = AgentDelegateInput {
        task_note_id: Some(note.id.clone()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let conv = svc
        .agent_get_conversation_op(child.clone(), None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 1);
    assert!(child_session_messages_json(&svc, &child)
        .await
        .contains("note content body"));
}

/// A bare delegate (no instructions, no task text, no task note) creates the
/// child but delivers no first message — there is nothing to send.
#[tokio::test]
async fn delegate_without_message_source_delivers_nothing() {
    let (_t, svc, ws) = setup().await;
    let resp = svc
        .agent_delegate_op(ws.clone(), AgentDelegateInput::default(), None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let conv = svc
        .agent_get_conversation_op(child, None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 0, "no message delivered");
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

// ===========================================================================
// AS-6: joined end-to-end integration over the real EventBus + delivery loop
// ===========================================================================

/// Poll until the completion-delivery worker's broadcast receiver is live so a
/// published event never races ahead of the subscription.
async fn wait_for_subscriber(bus: &EventBus) {
    timeout(Duration::from_secs(2), async {
        while bus.subscriber_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delivery worker subscribed");
}

/// Publish an AGENT completion event (idle/failed/deleted) onto the bus in the
/// shape the delivery worker filters on (agentId in data + agent actor).
async fn publish_completion(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    event_type: &str,
    child_id: &AgentId,
    data: serde_json::Value,
) {
    let ev = NewEvent {
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
    };
    bus.publish(&ev).await.expect("publish completion event");
}

/// Poll until the parent transcript reaches expected messages (the worker wakes
/// the parent asynchronously through the spawned delivery task).
async fn wait_for_message_count(svc: &Services, parent: &AgentId, expected: usize) {
    timeout(Duration::from_secs(2), async {
        loop {
            if parent_message_count(svc, parent).await >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("parent did not reach {expected} messages in time"));
}

/// Poll until the parent's open delegation group has recorded at least n child
/// completions (completed + deleted), so the no-premature-fire assertion is
/// deterministic rather than timing-dependent.
async fn wait_for_group_children(
    svc: &Services,
    workspace_id: &WorkspaceId,
    parent: &AgentId,
    n: usize,
) {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(g) = svc.delegation_group_for_parent(workspace_id, parent) {
                if g.completed_agent_ids.len() + g.deleted_agent_ids.len() >= n {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delegation group recorded child completions");
}

/// One joined service-level integration test that drives the full
/// auto-subscription loop through the real spawn_completion_delivery_loop worker
/// and the EventBus publish path (not handle_completion_event directly):
///   (a) an immediate delegate registers a oneShot watch; the child's agent:idle
///       published on the bus wakes the parent exactly once and the watch is
///       cleared from the registry;
///   (b) an after_all group of two children yields no wake until the parent
///       seals on its own agent:idle and both children complete -- a deleted
///       child still counts -- then exactly one aggregated partial wake;
///   (c) agent.getSubscriptions / agent.cancelSubscriptions reflect the live
///       registry across the loop (populated mid-flight, empty after the group
///       settles and after an explicit cancel).
/// Chosen over a node-gated UDS E2E so the whole loop runs deterministically
/// with no external provider dependency, mirroring the AS-3/AS-4 worker tests.
#[tokio::test]
async fn as6_end_to_end_auto_subscription_over_bus() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let worker = svc.spawn_completion_delivery_loop();
    wait_for_subscriber(&bus).await;

    let parent = create_agent(&svc, &ws, "Parent").await;

    // ---- (a) immediate delegate -> single oneShot wake + watch cleanup ----
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput::default(),
            Some(parent.clone()),
        )
        .await
        .expect("immediate delegate");
    let child1 = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    let list = subs["subscriptions"].as_array().expect("array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["oneShot"], json!(true));
    assert_eq!(list[0]["actorIds"], json!([child1.0]));

    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &child1,
        json!({ "agentId": child1.0, "lastResponseSummary": "shipped" }),
    )
    .await;
    wait_for_message_count(&svc, &parent, 1).await;

    assert!(svc.find_watches_for_child(&ws, &child1).is_empty());
    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert!(subs["subscriptions"].as_array().expect("array").is_empty());

    // ---- (b) after_all two children -> single aggregated partial wake ----
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert_eq!(subs["subscriptions"].as_array().expect("array").len(), 2);
    let groups = subs["delegationGroups"].as_array().expect("array");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["awaitMode"], json!("all"));
    assert_eq!(
        groups[0]["expectedAgentIds"]
            .as_array()
            .expect("array")
            .len(),
        2
    );

    // c1 idle then c2 deleted: both recorded, but no wake while unsealed.
    publish_completion(&bus, &ws, AGENT_IDLE, &c1, json!({ "agentId": c1.0 })).await;
    publish_completion(&bus, &ws, AGENT_DELETED, &c2, json!({ "agentId": c2.0 })).await;
    wait_for_group_children(&svc, &ws, &parent, 2).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);

    // The parent's own idle seals the group; now complete -> ONE partial wake.
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    )
    .await;
    wait_for_message_count(&svc, &parent, 2).await;
    assert_eq!(parent_message_count(&svc, &parent).await, 2);
    assert!(
        parent_messages_text(&svc, &parent)
            .await
            .contains("partial"),
        "a deleted child should yield a partial aggregated wake"
    );

    assert!(svc.delegation_group_for_parent(&ws, &parent).is_none());
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert!(subs["delegationGroups"]
        .as_array()
        .expect("array")
        .is_empty());

    // ---- (c) cancelSubscriptions clears a live mid-flight watch ----
    svc.agent_delegate_op(
        ws.clone(),
        AgentDelegateInput::default(),
        Some(parent.clone()),
    )
    .await
    .expect("immediate delegate 3");
    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert_eq!(subs["subscriptions"].as_array().expect("array").len(), 1);

    let cancel = svc
        .agent_cancel_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("cancel");
    assert_eq!(cancel, json!({ "success": true }));
    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    assert!(subs["subscriptions"].as_array().expect("array").is_empty());
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());

    worker.abort();
}

/// `agent.diagnostics` answers `{ ok, diagnostics, text }` with the full
/// snapshot shape: summary counts, a subscriptions view backed by completion
/// watches, an agents view, zeroed deliveryStats, and a human-readable `text`.
#[tokio::test]
async fn diagnostics_snapshot_shape_and_subscriptions() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;
    svc.register_completion_watch(
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        true,
        None,
    );

    let result = svc
        .agent_diagnostics_op(ws.clone(), None, None, None)
        .await
        .expect("diagnostics");

    assert_eq!(result["ok"], json!(true));
    let diag = &result["diagnostics"];
    assert_eq!(diag["workspaceId"], json!(ws.0));
    assert!(diag["generatedAt"].is_string());
    assert_eq!(diag["summary"]["agents"], json!(2));
    assert_eq!(diag["summary"]["subscriptions"], json!(1));
    assert_eq!(diag["summary"]["queuedEvents"], json!(0));
    assert!(diag["queues"].as_array().expect("queues").is_empty());
    assert!(diag["recentEvents"]
        .as_array()
        .expect("recentEvents")
        .is_empty());
    // deliveryStats is the zeroed emptyDeliveryStats shape.
    assert_eq!(diag["deliveryStats"]["droppedEvents"], json!(0));
    assert!(diag["deliveryStats"]["lastFailureTime"].is_null());

    let subs = diag["subscriptions"].as_array().expect("subscriptions");
    assert_eq!(subs.len(), 1);
    let sub = &subs[0];
    assert_eq!(sub["agentId"], json!(parent.0));
    assert_eq!(sub["agentName"], json!("Parent"));
    assert_eq!(sub["actorIds"], json!([child.0]));
    assert_eq!(sub["eventTypes"].as_array().expect("eventTypes").len(), 3);
    assert_eq!(sub["priority"], json!("normal"));
    assert_eq!(sub["oneShot"], json!(true));
    assert_eq!(sub["orphaned"], json!(false));

    assert!(result["text"]
        .as_str()
        .expect("text")
        .contains("Agent diagnostics for workspace"));
}

/// `agent.diagnostics` `agentId` filter narrows the snapshot to the focused
/// agent (and the subscription actors in its scope).
#[tokio::test]
async fn diagnostics_agent_filter_narrows_scope() {
    let (_t, svc, ws) = setup().await;
    let a = create_agent(&svc, &ws, "A").await;
    let _b = create_agent(&svc, &ws, "B").await;

    let result = svc
        .agent_diagnostics_op(ws.clone(), Some(a.clone()), None, None)
        .await
        .expect("diagnostics");

    let diag = &result["diagnostics"];
    let agents = diag["agents"].as_array().expect("agents");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["id"], json!(a.0));
    assert_eq!(diag["filters"]["agentId"], json!(a.0));
}

/// A completion watch whose parent has no live session surfaces an
/// `orphaned-subscription` stuck-risk signal.
#[tokio::test]
async fn diagnostics_flags_orphaned_subscription() {
    let (_t, svc, ws) = setup().await;
    let child = create_agent(&svc, &ws, "Child").await;
    let ghost = AgentId::from("agent-ghost");
    svc.register_completion_watch(
        &ws,
        ghost.clone(),
        "Ghost".into(),
        child.clone(),
        true,
        None,
    );

    let result = svc
        .agent_diagnostics_op(ws.clone(), None, None, None)
        .await
        .expect("diagnostics");

    let diag = &result["diagnostics"];
    let risks = diag["stuckRisks"].as_array().expect("stuckRisks");
    assert!(risks
        .iter()
        .any(|r| r["type"] == json!("orphaned-subscription") && r["agentId"] == json!(ghost.0)));
}

/// The auggie `session stats --json` parser maps the camelCase CLI shape onto
/// [`SessionStats`]: `creditsUsed` flows through, counts default to 0 when
/// absent, and a non-object payload degrades to `None` (PROTOCOL §5.24).
#[test]
fn parse_session_stats_output_maps_cli_shape() {
    let full = parse_session_stats_output(r#"{"creditsUsed":12.5,"messageCount":7,"toolCount":3}"#)
        .expect("full object parses");
    assert_eq!(full.credits_used, Some(12.5));
    assert_eq!(full.message_count, 7);
    assert_eq!(full.tool_count, 3);

    // Missing credits + counts: creditsUsed -> None, counts -> 0.
    let partial = parse_session_stats_output(r#"{"messageCount":2}"#).expect("partial parses");
    assert_eq!(partial.credits_used, None);
    assert_eq!(partial.message_count, 2);
    assert_eq!(partial.tool_count, 0);

    // Non-object / unavailable-CLI plain text -> None (graceful degrade).
    assert!(parse_session_stats_output("auggie: session stats unavailable").is_none());
    assert!(parse_session_stats_output("").is_none());
}

/// `cache_and_emit_session_stats` pushes a self-sufficient
/// `agent:session-stats-changed` event the first time it observes a snapshot and
/// stays silent on an identical re-observation, then re-emits when the rollup
/// moves (PROTOCOL §5.24 / §6.5 change-detection).
#[tokio::test]
async fn session_stats_emits_only_on_change() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Stats").await;
    let session = svc.store().get_agent_session(&id).await.expect("session");

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_SESSION_STATS_CHANGED.to_string()],
        ..Default::default()
    });

    let stats = SessionStats {
        credits_used: Some(4.0),
        message_count: 5,
        tool_count: 2,
    };
    svc.cache_and_emit_session_stats(&session, &stats).await;

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].event_type, AGENT_SESSION_STATS_CHANGED);
    assert_eq!(batch[0].workspace_id, ws);
    assert_eq!(batch[0].data["sessionId"].as_str(), Some(id.0.as_str()));
    assert_eq!(batch[0].data["agentId"].as_str(), Some(id.0.as_str()));
    assert_eq!(batch[0].data["stats"]["messageCount"], json!(5));
    assert_eq!(batch[0].data["stats"]["toolCount"], json!(2));
    assert_eq!(batch[0].data["stats"]["creditsUsed"], json!(4.0));

    // Identical snapshot -> no second emit within the window.
    svc.cache_and_emit_session_stats(&session, &stats).await;
    let res = timeout(Duration::from_millis(300), sub.recv()).await;
    assert!(res.is_err(), "identical stats must not re-emit");

    // A moved rollup -> a fresh emit.
    let moved = SessionStats {
        credits_used: Some(9.0),
        message_count: 6,
        tool_count: 2,
    };
    svc.cache_and_emit_session_stats(&session, &moved).await;
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].data["stats"]["messageCount"], json!(6));
}

/// `agent.getSessionStats` for an unknown session surfaces `NotFound`, which the
/// router maps to JSON-RPC `-32602` (PROTOCOL §5.24).
#[tokio::test]
async fn get_session_stats_unknown_session_is_not_found() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_get_session_stats_op(AgentId::from("agent-00000000-0000-0000-0000-00000missing0"))
        .await
        .expect_err("unknown session");
    assert!(matches!(err, Error::NotFound(_)));
}
