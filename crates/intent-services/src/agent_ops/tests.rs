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

use intent_core::events::{
    AGENT_CREATED, AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_MESSAGE, AGENT_RENAMED,
    AGENT_SESSION_STATS_CHANGED, AGENT_SUBSCRIPTIONS_CHANGED, AGENT_UPDATED,
};
use intent_core::{ActorType, Event, EventActor, SessionStats};

use crate::{EventBus, SubscriptionFilter};

use crate::agent_ops::{
    finalize_model_rows, parse_model_list_json, parse_model_list_output,
    parse_session_stats_output, static_models,
};
use crate::Services;
use intent_core::MAX_DELEGATION_DEPTH;

pub(super) struct TempDb {
    pub(super) path: PathBuf,
}

impl TempDb {
    pub(super) fn new() -> Self {
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

pub(super) fn workspace(id: &WorkspaceId) -> Workspace {
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
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
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

pub(super) fn completion_event(
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

    let r = svc.agent_delete_op(id.clone(), None).await.expect("delete");
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
        .agent_delete_op(missing, None)
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

/// The immediate-path wake persists FE-shaped `event_notification` metadata on
/// the parent's user-message row so `EventWakeupBanner` can render a real
/// `eventCount` / `eventTypes` / per-agent `events` payload instead of the
/// fallback "Subscription update — 0 events".
#[tokio::test]
async fn completion_delivery_attaches_event_notification_metadata() {
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

    let event = completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({
            "agentId": child.0,
            "lastResponseSummary": "shipped it",
            "completionReport": "done",
        }),
    );
    svc.handle_completion_event(&event).await;

    let session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(session.messages.len(), 1);
    let msg = &session.messages[0];
    assert_eq!(msg.role, "user");
    let metadata = msg
        .metadata
        .as_ref()
        .expect("wake message carries event_notification metadata");
    assert_eq!(metadata["type"], json!("event_notification"));
    assert_eq!(metadata["eventCount"], json!(1));
    assert_eq!(metadata["eventTypes"], json!([AGENT_IDLE]));
    let events = metadata["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["id"], json!(event.id));
    assert_eq!(events[0]["type"], json!(AGENT_IDLE));
    assert_eq!(events[0]["timestamp"], json!(event.timestamp));
    assert_eq!(events[0]["data"]["agentId"], json!(child.0));
    assert_eq!(events[0]["data"]["completionReport"], json!("done"));
    assert_eq!(events[0]["actor"]["type"], json!("agent"));
    assert_eq!(events[0]["actor"]["id"], json!(child.0));
}

/// The aggregated after_all wake carries `event_notification` metadata whose
/// `eventCount` equals the group size and whose `events` array preserves each
/// child's raw completion event (id, type, data, timestamp, actor).
#[tokio::test]
async fn group_fire_attaches_event_notification_metadata() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    let e1 = completion_event(
        &ws,
        AGENT_IDLE,
        &c1,
        json!({ "agentId": c1.0, "lastResponseSummary": "one" }),
    );
    let e2 = completion_event(
        &ws,
        AGENT_IDLE,
        &c2,
        json!({ "agentId": c2.0, "lastResponseSummary": "two" }),
    );
    svc.handle_completion_event(&e1).await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    svc.handle_completion_event(&e2).await;

    let session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(session.messages.len(), 1);
    let metadata = session.messages[0]
        .metadata
        .as_ref()
        .expect("aggregated wake carries event_notification metadata");
    assert_eq!(metadata["type"], json!("event_notification"));
    assert_eq!(metadata["eventCount"], json!(2));
    assert_eq!(metadata["eventTypes"], json!([AGENT_IDLE]));
    let events = metadata["events"].as_array().expect("events array");
    assert_eq!(events.len(), 2);
    let ids: Vec<&str> = events.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert!(ids.contains(&e1.id.as_str()));
    assert!(ids.contains(&e2.id.as_str()));
    for e in events {
        assert_eq!(e["type"], json!(AGENT_IDLE));
        assert!(e["data"]["agentId"].is_string());
        assert!(e["timestamp"].is_string());
        assert_eq!(e["actor"]["type"], json!("agent"));
    }
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
            Default::default(),
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

    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    assert_eq!(got.id, id);
    assert_eq!(got.model.as_deref(), Some("auggie:sonnet4.5"));
}

#[tokio::test]
async fn agent_create_mints_server_assigned_agent_id() {
    // Agent ids are server-assigned: every create mints a fresh
    // `agent-{uuid}` (client-supplied ids are rejected at the transport
    // boundary and never reach this op).
    let (_t, svc, ws) = setup().await;
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Minted".into()),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create");
    let id = created["agent"]["id"].as_str().expect("agent id");
    let tail = id.strip_prefix("agent-").expect("agent-{uuid} form");
    uuid::Uuid::parse_str(tail).expect("uuid tail");
    // Round-trip through the store proves the session is addressable at the
    // server-minted id.
    let got = svc
        .agent_get_op(AgentId::from(id), None)
        .await
        .expect("get");
    assert_eq!(got.id.as_str(), id);
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
            Default::default(),
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let lite = svc.agent_get_op(id, None).await.expect("get");
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

    let lite = svc.agent_get_op(parent.clone(), None).await.expect("get");
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
    let v =
        serde_json::to_value(svc.agent_get_op(parent.clone(), None).await.expect("get")).unwrap();
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
    let v =
        serde_json::to_value(svc.agent_get_op(parent.clone(), None).await.expect("get")).unwrap();
    assert_eq!(v["waitingForAgentIds"], json!([child.0]));

    // The child has no worker and parents no watches: every flag false and the
    // waiting-on id list is the empty array (never null/omitted).
    let cv = serde_json::to_value(svc.agent_get_op(child, None).await.expect("get")).unwrap();
    assert_eq!(cv["isResponding"], false);
    assert_eq!(cv["isWaitingOnTool"], false);
    assert_eq!(cv["isWaitingForOtherAgents"], false);
    assert_eq!(cv["waitingForAgentIds"], json!([]));
}

/// STAB-125: `agent.get` surfaces turn-liveness — `turnInFlight` and
/// `lastStreamActivityAt` — from the live-turn slot so a poller can tell a
/// long-but-alive turn from a wedged agent before anything persists.
#[tokio::test]
async fn agent_lite_surfaces_turn_liveness_from_live_turn_slot() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Streamer").await;

    // Idle agent: no in-flight turn, timestamp omitted (skip_serializing_if).
    let v = serde_json::to_value(svc.agent_get_op(id.clone(), None).await.expect("get")).unwrap();
    assert_eq!(v["turnInFlight"], false);
    assert!(
        v.get("lastStreamActivityAt").is_none(),
        "idle agent omits lastStreamActivityAt: {v}"
    );

    // A worker draining an in-flight turn: turnInFlight with the slot's stamp.
    let before = now_iso();
    svc.set_test_busy(&id, true);
    svc.set_live_turn(
        &id,
        "msg-1",
        vec![json!({ "type": "text", "id": "msg-1:0", "text": "thinking…" })],
    );
    let v = serde_json::to_value(svc.agent_get_op(id.clone(), None).await.expect("get")).unwrap();
    assert_eq!(v["turnInFlight"], true);
    let stamp = v["lastStreamActivityAt"]
        .as_str()
        .expect("lastStreamActivityAt present while in flight")
        .to_string();
    // Compare parsed instants — RFC-3339 strings carry variable sub-second
    // precision, so lexicographic order is not chronological order.
    let parsed = intent_core::parse_iso(&stamp).expect("valid RFC-3339 stamp");
    let lo = intent_core::parse_iso(&before).unwrap();
    let hi = intent_core::parse_iso(&now_iso()).unwrap();
    assert!(
        parsed >= lo && parsed <= hi,
        "slot stamp within [begin, now]: {stamp}"
    );

    // Streaming progress re-stamps the slot: a later set_live_turn advances it.
    // Wait until the clock has observably moved past the first stamp so the
    // strict `>` holds even on coarse clock/formatting resolutions.
    while intent_core::parse_iso(&now_iso()).unwrap() <= parsed {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    svc.set_live_turn(
        &id,
        "msg-1",
        vec![json!({ "type": "text", "id": "msg-1:0", "text": "thinking… more" })],
    );
    let v = serde_json::to_value(svc.agent_get_op(id.clone(), None).await.expect("get")).unwrap();
    let stamp2 = v["lastStreamActivityAt"].as_str().expect("stamp2");
    let parsed2 = intent_core::parse_iso(stamp2).expect("valid RFC-3339 stamp2");
    assert!(
        parsed2 > parsed,
        "stream activity advances the stamp: {stamp} -> {stamp2}"
    );

    // An orphan slot with NO busy claim must not report a phantom turn —
    // same gate as chat_snapshot's live-turn merge.
    svc.set_test_busy(&id, false);
    let v = serde_json::to_value(svc.agent_get_op(id.clone(), None).await.expect("get")).unwrap();
    assert_eq!(
        v["turnInFlight"], false,
        "no busy worker → no in-flight turn"
    );
    assert!(v.get("lastStreamActivityAt").is_none());

    // Turn end clears the slot: back to the idle shape.
    svc.set_test_busy(&id, true);
    svc.clear_live_turn(&id);
    let v = serde_json::to_value(svc.agent_get_op(id, None).await.expect("get")).unwrap();
    assert_eq!(v["turnInFlight"], false);
    assert!(v.get("lastStreamActivityAt").is_none());
}

/// STAB-125: `agent.getConversation` carries the same turn-liveness fields, so
/// a conversation read mid-turn (nothing persisted yet) is distinguishable
/// from a wedged agent.
#[tokio::test]
async fn get_conversation_surfaces_turn_liveness() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Conv").await;

    // Idle: turnInFlight false, lastStreamActivityAt null (always present).
    let res = svc
        .agent_get_conversation_op(id.clone(), None, None, None)
        .await
        .expect("conv");
    assert_eq!(res["turnInFlight"], false);
    assert!(res["lastStreamActivityAt"].is_null());

    // Mid-turn (busy worker + open slot): both fields live.
    svc.set_test_busy(&id, true);
    svc.set_live_turn(
        &id,
        "msg-1",
        vec![json!({ "type": "text", "id": "msg-1:0", "text": "streaming…" })],
    );
    let res = svc
        .agent_get_conversation_op(id.clone(), None, None, None)
        .await
        .expect("conv");
    assert_eq!(res["turnInFlight"], true);
    assert!(res["lastStreamActivityAt"].is_string());
    // The long turn has persisted nothing: the page is still empty even though
    // the turn is provably alive — exactly the STAB-125 gap being closed.
    assert_eq!(res["totalMessages"], 0);

    // Turn end: slot cleared, fields fall back to the idle shape.
    svc.clear_live_turn(&id);
    svc.set_test_busy(&id, false);
    let res = svc
        .agent_get_conversation_op(id, None, None, None)
        .await
        .expect("conv");
    assert_eq!(res["turnInFlight"], false);
    assert!(res["lastStreamActivityAt"].is_null());
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
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let lite = svc.agent_get_op(child, None).await.expect("get");
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
    let lite = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(
        lite.last_user_message.as_deref(),
        Some("please do the thing")
    );
}

#[tokio::test]
async fn get_unknown_agent_is_not_found() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_get_op(
            AgentId::from("agent-00000000-0000-0000-0000-000000000000"),
            None,
        )
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
        .agent_get_conversation_op(id.clone(), Some(2), None, None)
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
        .agent_get_conversation_op(id.clone(), Some(2), None, None)
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
        .agent_get_conversation_op(id.clone(), Some(2), None, Some(t1))
        .await
        .expect("p2");
    let m2 = p2["messages"].as_array().unwrap();
    assert_eq!(m2[0]["contentBlocks"][0]["text"], "m1");
    assert_eq!(m2[1]["contentBlocks"][0]["text"], "m2");
    let t2 = p2["nextToken"].as_str().expect("nextToken2").to_string();

    // Page 3 is the final page: oldest message, no further token.
    let p3 = svc
        .agent_get_conversation_op(id.clone(), Some(2), None, Some(t2))
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
        .agent_get_conversation_op(id.clone(), None, None, None)
        .await
        .expect("all");
    assert_eq!(all["messages"].as_array().unwrap().len(), 5);
    assert!(all["nextToken"].is_null());
    let clamped = svc
        .agent_get_conversation_op(id, Some(10_000), None, None)
        .await
        .expect("clamped");
    assert_eq!(clamped["messages"].as_array().unwrap().len(), 5);
    assert!(clamped["nextToken"].is_null());
}

/// STAB-124 loading tolerance: rows persisted by pre-fix daemons can carry an
/// anonymous `tool_use` block (`name: ""`) plus its paired errored
/// `tool_result` at the head of an interrupt turn's assistant message.
/// `agent.getConversation` must strip the anonymous pair on read (keeping the
/// rest of the message intact) so the FE conversation load no longer breaks.
#[tokio::test]
async fn get_conversation_strips_anonymous_tool_use_pairs() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Interrupted").await;
    // The observed malformed shape (agent-695dcf49 seq 2): anonymous tool_use +
    // its abort-errored tool_result, then the real turn content — including a
    // NAMED tool pair that must survive the strip.
    let malformed = json!([
        { "type": "tool_use", "id": "m:0", "name": "", "input": {},
          "toolCallId": "stale-1", "metadata": { "toolKind": "other", "status": "error" } },
        { "type": "tool_result", "id": "m:1", "tool_use_id": "stale-1",
          "output": { "error": "The operation was aborted" }, "is_error": true },
        { "type": "text", "id": "m:2", "text": "Resuming after interrupt" },
        { "type": "tool_use", "id": "m:3", "name": "view", "input": { "path": "src" },
          "toolCallId": "real-1", "metadata": { "toolKind": "file", "status": "completed" } },
        { "type": "tool_result", "id": "m:4", "tool_use_id": "real-1",
          "output": { "files": 3 }, "is_error": false },
    ]);
    svc.store()
        .append_agent_message(&id, "assistant", &malformed, &now_iso())
        .await
        .expect("append");

    let res = svc
        .agent_get_conversation_op(id, None, None, None)
        .await
        .expect("conv");
    let blocks = res["messages"][0]["contentBlocks"].as_array().unwrap();
    assert_eq!(
        blocks.len(),
        3,
        "anonymous tool_use + its tool_result stripped, rest kept: {blocks:?}"
    );
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[1]["type"], "tool_use");
    assert_eq!(blocks[1]["name"], "view");
    assert_eq!(blocks[2]["type"], "tool_result");
    assert_eq!(blocks[2]["tool_use_id"], "real-1");
}

#[tokio::test]
async fn rename_and_set_model_persist() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Old").await;
    let r = svc
        .agent_rename_op(id.clone(), "New".into(), false)
        .await
        .expect("rename");
    assert_eq!(r["name"], "New");
    svc.agent_set_model_op(id.clone(), "auggie:opus4.7".into())
        .await
        .expect("setModel");
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert_eq!(got.name, "New");
    assert!(got.name_explicitly_set);
    assert_eq!(got.model.as_deref(), Some("auggie:opus4.7"));
}

/// `agent.setModel` reconciles session.provider when the new model is a
/// compound id whose provider differs from the current session provider.
/// This ensures cross-provider model switches spawn the new provider's binary.
#[tokio::test]
async fn set_model_reconciles_provider_on_cross_provider_switch() {
    let (_t, svc, ws) = setup().await;
    // Create an agent with an explicit auggie provider.
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Switch".into()),
            Some("auggie:sonnet4.5".into()),
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    // Initial state: auggie provider, auggie model.
    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    // Provider is inferred from the compound model on creation.
    assert_eq!(session.provider.as_deref(), Some("auggie"));
    // Set a compound model for a different provider.
    svc.agent_set_model_op(id.clone(), "opencode:opencode-go/kimi-k3".into())
        .await
        .expect("setModel");
    // session.provider should now match the compound prefix.
    let session = svc
        .agent_get_session_op(id.clone())
        .await
        .expect("get after");
    assert_eq!(
        session.model.as_deref(),
        Some("opencode:opencode-go/kimi-k3")
    );
    assert_eq!(session.provider.as_deref(), Some("opencode"));
}

/// `agent.setModel` leaves session.provider unchanged when the new model is
/// a bare id (no `:` prefix) or a compound id for the same provider.
#[tokio::test]
async fn set_model_preserves_provider_for_bare_or_same_provider() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Same").await;
    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    let orig_provider = session.provider.clone();
    // Bare model → provider unchanged.
    svc.agent_set_model_op(id.clone(), "opus4.7".into())
        .await
        .expect("setModel bare");
    let session = svc
        .agent_get_session_op(id.clone())
        .await
        .expect("get after bare");
    assert_eq!(session.provider, orig_provider);
    // Same-provider compound → provider unchanged (or set to match if None).
    svc.agent_set_model_op(id.clone(), "auggie:sonnet4.5".into())
        .await
        .expect("setModel same provider");
    let session = svc
        .agent_get_session_op(id.clone())
        .await
        .expect("get after same");
    assert_eq!(session.provider.as_deref(), Some("auggie"));
}

#[tokio::test]
async fn rename_missing_agent_is_internal() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_rename_op(
            AgentId::from("agent-00000000-0000-0000-0000-000000000000"),
            "x".into(),
            false,
        )
        .await
        .expect_err("missing");
    assert!(matches!(err, Error::Internal(_)));
}

/// `agent.rename` `skipIfExplicitlySet` (P3-1.2b): an explicitly-named session
/// is left untouched (`skipped: true`, existing name echoed); an auto-named
/// session is renamed normally, after which the skip flag holds.
#[tokio::test]
async fn rename_skip_if_explicitly_set() {
    let (_t, svc, ws) = setup().await;
    // `create_agent` supplies a name -> nameExplicitlySet = true.
    let explicit = create_agent(&svc, &ws, "Named").await;
    let r = svc
        .agent_rename_op(explicit.clone(), "Clobber".into(), true)
        .await
        .expect("skip rename");
    assert_eq!(r["success"], json!(true));
    assert_eq!(r["skipped"], json!(true));
    assert_eq!(r["name"], "Named");
    let got = svc.agent_get_op(explicit, None).await.expect("get");
    assert_eq!(got.name, "Named");

    // No client name -> auto-generated, nameExplicitlySet = false: the
    // skip-guarded rename applies (and `skipped` is absent from the result).
    let created = svc
        .agent_create_op(
            ws.clone(),
            None,
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create auto-named");
    let auto = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let r = svc
        .agent_rename_op(auto.clone(), "Chosen".into(), true)
        .await
        .expect("rename");
    assert_eq!(r["name"], "Chosen");
    assert!(r.get("skipped").is_none());
    let got = svc.agent_get_op(auto.clone(), None).await.expect("get");
    assert_eq!(got.name, "Chosen");
    assert!(got.name_explicitly_set);
    // Now explicitly set -> a further skip-guarded rename is a no-op.
    let r = svc
        .agent_rename_op(auto, "Again".into(), true)
        .await
        .expect("skip");
    assert_eq!(r["skipped"], json!(true));
    assert_eq!(r["name"], "Chosen");
}

/// `agent.create` harvests the persistence-gap fields (P3-1.2b) from the
/// `metadata` spawn hint / top-level params and re-serves them via
/// `agent.get`/`agent.list`: `metadata.delegationDepth`, `metadata.initialMessage`,
/// session-level `contextReferences` / `imageBlocks`, and
/// `metadata.isBackground` (G-A1/P3-1.2c).
#[tokio::test]
async fn create_persists_and_reserves_gap_fields() {
    let (_t, svc, ws) = setup().await;
    let extra = intent_core::AgentCreateExtra {
        metadata: Some(json!({
            "delegationDepth": 2,
            "initialMessage": "start here",
            "contextReferences": [{ "type": "file", "path": "src/a.rs" }],
            "isBackground": true,
        })),
        image_blocks: Some(json!([{ "type": "image", "data": "abc" }])),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Gaps".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());

    // Re-served on `agent.get` (session-level fields + nested metadata).
    let got = svc.agent_get_op(id.clone(), None).await.expect("get");
    let v = serde_json::to_value(&got).expect("lite json");
    assert_eq!(v["metadata"]["delegationDepth"], json!(2));
    assert_eq!(v["metadata"]["initialMessage"], "start here");
    assert_eq!(
        v["contextReferences"],
        json!([{ "type": "file", "path": "src/a.rs" }])
    );
    assert_eq!(
        v["imageBlocks"],
        json!([{ "type": "image", "data": "abc" }])
    );
    assert_eq!(v["metadata"]["isBackground"], json!(true));

    // And on `agent.list`.
    let agents = svc.agent_list_op(ws).await.expect("list");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].metadata.delegation_depth, Some(2));
    assert!(agents[0].metadata.is_background);
}

/// The top-level `isBackground` param wins over the `metadata` fallback, and
/// an agent created with neither defaults to foreground (G-A1/P3-1.2c).
#[tokio::test]
async fn create_is_background_top_level_wins_and_defaults_false() {
    let (_t, svc, ws) = setup().await;

    // Top-level `false` beats `metadata.isBackground: true`.
    let extra = intent_core::AgentCreateExtra {
        metadata: Some(json!({ "isBackground": true })),
        is_background: Some(false),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("FG".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let got = svc.agent_get_op(id, None).await.expect("get");
    let v = serde_json::to_value(&got).expect("lite json");
    assert_eq!(v["metadata"]["isBackground"], json!(false));

    // Neither param nor metadata → defaults to foreground.
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Plain".into()),
            None,
            None,
            None,
            None,
            false,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create plain");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let got = svc.agent_get_op(id, None).await.expect("get plain");
    let v = serde_json::to_value(&got).expect("lite json");
    assert_eq!(v["metadata"]["isBackground"], json!(false));
}

/// `agent.create` / `agent.rename` / `agent.setModel` emit their `agent:*`
/// invalidation events (P3-1.2b): `agent:created`, `agent:renamed`,
/// `agent:updated`.
#[tokio::test]
async fn create_rename_set_model_emit_agent_events() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![
            AGENT_CREATED.to_string(),
            AGENT_RENAMED.to_string(),
            AGENT_UPDATED.to_string(),
        ],
        ..Default::default()
    });

    let id = create_agent(&svc, &ws, "Evented").await;
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("created recv")
        .expect("sub closed");
    assert_eq!(batch[0].event_type, AGENT_CREATED);
    assert_eq!(batch[0].data["agentId"].as_str(), Some(id.0.as_str()));

    svc.agent_rename_op(id.clone(), "Renamed".into(), false)
        .await
        .expect("rename");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("renamed recv")
        .expect("sub closed");
    assert_eq!(batch[0].event_type, AGENT_RENAMED);
    assert_eq!(batch[0].data["name"], "Renamed");

    svc.agent_set_model_op(id.clone(), "auggie:opus4.7".into())
        .await
        .expect("setModel");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("updated recv")
        .expect("sub closed");
    assert_eq!(batch[0].event_type, AGENT_UPDATED);
    assert_eq!(batch[0].data["modelId"], "auggie:opus4.7");
}

/// A skipped rename mutates nothing and therefore emits no `agent:renamed`.
#[tokio::test]
async fn skipped_rename_emits_no_event() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Named").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_RENAMED.to_string()],
        ..Default::default()
    });
    let r = svc
        .agent_rename_op(id, "Clobber".into(), true)
        .await
        .expect("skip");
    assert_eq!(r["skipped"], json!(true));
    assert!(
        timeout(Duration::from_millis(300), sub.recv())
            .await
            .is_err(),
        "no agent:renamed expected for a skipped rename"
    );
}

/// `agent.reportToParent` persists `completionReport` /
/// `completionReportTimestamp` on the child session (re-served under
/// `metadata` by `agent.get`) in addition to delivering to the parent
/// (P3-1.2b).
#[tokio::test]
async fn report_to_parent_persists_completion_report() {
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
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let r = svc
        .agent_report_to_parent_op(ws.clone(), json!("all done"), Some(child.clone()))
        .await
        .expect("report");
    assert_eq!(r["ok"], json!(true));
    assert_eq!(r["parentAgentId"].as_str(), Some(parent.0.as_str()));

    let got = svc.agent_get_op(child, None).await.expect("get child");
    let v = serde_json::to_value(&got).expect("lite json");
    assert_eq!(v["metadata"]["completionReport"], "all done");
    assert_eq!(v["metadata"]["completionReportTimestamp"], r["savedAt"]);
}

/// TASK-B: on `agent.reportToParent`, the caller's linked task note
/// transitions from a non-terminal status (`in_progress`) to
/// `review_required`, mirroring the reference reportToParent writer.
#[tokio::test]
async fn report_to_parent_transitions_linked_task_to_review_required() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Ship feature X".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "in_progress".into(),
        vec![],
        None,
    )
    .await
    .expect("markAsTask");

    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
            Some(note.id.clone()),
            false,
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    svc.agent_report_to_parent_op(ws.clone(), json!("done"), Some(child))
        .await
        .expect("report");

    let refreshed = svc
        .store()
        .get_note(&ws, &note.id)
        .await
        .expect("refresh note");
    assert_eq!(
        refreshed.metadata.task.expect("task metadata").status,
        intent_core::TaskStatus::ReviewRequired
    );
}

/// TASK-B: terminal task statuses (`complete`, `cancelled`) MUST NOT be
/// overwritten by a late `reportToParent` — the reference writer is a strict
/// upgrade, never a downgrade of a done/cancelled task.
#[tokio::test]
async fn report_to_parent_does_not_overwrite_terminal_task_status() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Already done".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    svc.mark_as_task(ws.clone(), note.id.clone(), "complete".into(), vec![], None)
        .await
        .expect("markAsTask");

    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
            Some(note.id.clone()),
            false,
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    svc.agent_report_to_parent_op(ws.clone(), json!("late"), Some(child))
        .await
        .expect("report");

    let refreshed = svc
        .store()
        .get_note(&ws, &note.id)
        .await
        .expect("refresh note");
    assert_eq!(
        refreshed.metadata.task.expect("task metadata").status,
        intent_core::TaskStatus::Complete,
        "terminal status must not be downgraded to review_required"
    );
}

/// TASK-B: repeated `reportToParent` calls for the same delegated child
/// must not re-persist the linked task note once it has already been
/// transitioned to `review_required`. `task.updateNoteStatus` always
/// bumps `updated_at` and `rev` before checking for a status change, so
/// short-circuiting on the current status is what keeps repeated
/// child-reports from churning the note (unresolved copilot review
/// thread PRRT_kwDOS9Wxuc6QIRcj on PR #104).
#[tokio::test]
async fn report_to_parent_review_required_second_call_is_a_note_write_noop() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Ship feature Y".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "in_progress".into(),
        vec![],
        None,
    )
    .await
    .expect("markAsTask");

    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
            Some(note.id.clone()),
            false,
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let before_rev = svc
        .store()
        .get_note(&ws, &note.id)
        .await
        .expect("initial note")
        .rev;

    svc.agent_report_to_parent_op(ws.clone(), json!("first"), Some(child.clone()))
        .await
        .expect("first report");
    let after_first = svc
        .store()
        .get_note(&ws, &note.id)
        .await
        .expect("note after first");
    assert_eq!(
        after_first.metadata.task.as_ref().expect("task").status,
        intent_core::TaskStatus::ReviewRequired
    );
    assert!(
        after_first.rev > before_rev,
        "first reportToParent must persist the review_required transition (rev {before_rev} -> {})",
        after_first.rev
    );

    svc.agent_report_to_parent_op(ws.clone(), json!("second"), Some(child))
        .await
        .expect("second report");
    let after_second = svc
        .store()
        .get_note(&ws, &note.id)
        .await
        .expect("note after second");
    assert_eq!(
        after_second.metadata.task.as_ref().expect("task").status,
        intent_core::TaskStatus::ReviewRequired
    );
    assert_eq!(
        after_second.rev, after_first.rev,
        "second reportToParent must not re-persist the note (rev must not bump when already \
         review_required)"
    );
    assert_eq!(
        after_second.updated_at, after_first.updated_at,
        "second reportToParent must not bump updated_at when already review_required"
    );
}

/// TASK-B: an agent without a linked task note reports back without touching
/// any task metadata — the report is persisted and the call succeeds.
#[tokio::test]
async fn report_to_parent_without_linked_task_is_status_noop() {
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
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let r = svc
        .agent_report_to_parent_op(ws.clone(), json!("no task"), Some(child.clone()))
        .await
        .expect("report");
    assert_eq!(r["ok"], json!(true));
    let session = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child session");
    assert!(session.task_note_id.is_none());
    assert_eq!(session.completion_report.as_deref(), Some("no task"));
}

/// Workspace-scoping (Copilot review PRRT_kwDOS9Wxuc6QIaRJ on PR #104):
/// `agent.delegate` loads the linked task note via `crate::fetch_note`, which
/// is workspace-scoped. Passing a `taskNoteId` that belongs to another
/// workspace must NOT leak the foreign note's title/content into the TASK-C
/// preamble injected as the child's first message; the preamble is skipped and
/// the message falls back to the caller-supplied `agentInstructions`.
#[tokio::test]
async fn delegate_out_of_workspace_task_note_id_does_not_leak_into_preamble() {
    let (_t, svc, ws_a) = setup().await;
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("insert ws_b");

    let foreign = svc
        .create_note(
            ws_b.clone(),
            NoteCreate {
                title: "CROSS-WORKSPACE-SECRET-TITLE".into(),
                content: Some("CROSS-WORKSPACE-SECRET-BODY".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create foreign note");

    let input = AgentDelegateInput {
        task_note_id: Some(foreign.id.clone()),
        agent_instructions: Some("do the work".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws_a.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let body = child_session_messages_json(&svc, &child).await;
    assert!(
        body.contains("do the work"),
        "explicit instructions must still reach the child: {body}"
    );
    assert!(
        !body.contains("CROSS-WORKSPACE-SECRET-TITLE"),
        "foreign note title must not leak into preamble: {body}"
    );
    assert!(
        !body.contains("CROSS-WORKSPACE-SECRET-BODY"),
        "foreign note body must not leak into preamble: {body}"
    );
    assert!(
        !body.contains("**Your Task Note:**"),
        "preamble must be skipped when the linked note is out of workspace: {body}"
    );
}

/// Workspace-scoping (Copilot review PRRT_kwDOS9Wxuc6QIaRP on PR #104):
/// `transition_linked_task_to_review_required` must load the linked task note
/// via the workspace-scoped `crate::fetch_note` accessor. When a session is
/// linked to a task note that lives in a different workspace, the fetch
/// returns `NotFound` and the transition is a silent no-op: the foreign note's
/// task metadata is left untouched (no cross-workspace read, no cross-workspace
/// write).
#[tokio::test]
async fn report_to_parent_out_of_workspace_task_note_is_transition_noop() {
    let (_t, svc, ws_a) = setup().await;
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("insert ws_b");

    let foreign = svc
        .create_note(
            ws_b.clone(),
            NoteCreate {
                title: "Foreign task".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create foreign note");
    svc.mark_as_task(
        ws_b.clone(),
        foreign.id.clone(),
        "in_progress".into(),
        vec![],
        None,
    )
    .await
    .expect("markAsTask on foreign");

    let before = svc
        .store()
        .get_note(&ws_b, &foreign.id)
        .await
        .expect("initial foreign note");
    assert_eq!(
        before.metadata.task.as_ref().expect("task").status,
        intent_core::TaskStatus::InProgress
    );

    let parent = create_agent(&svc, &ws_a, "Parent").await;
    let created = svc
        .agent_create_op(
            ws_a.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
            Some(foreign.id.clone()),
            false,
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    svc.agent_report_to_parent_op(ws_a.clone(), json!("done"), Some(child))
        .await
        .expect("report");

    let after = svc
        .store()
        .get_note(&ws_b, &foreign.id)
        .await
        .expect("refresh foreign note");
    assert_eq!(
        after.metadata.task.as_ref().expect("task").status,
        intent_core::TaskStatus::InProgress,
        "foreign-workspace task status must not be mutated by a cross-workspace reportToParent"
    );
    assert_eq!(
        after.rev, before.rev,
        "foreign-workspace note rev must not be bumped (no cross-workspace write): {} -> {}",
        before.rev, after.rev
    );
    assert_eq!(
        after.updated_at, before.updated_at,
        "foreign-workspace note updated_at must not be bumped"
    );
}

/// Copilot #104 (thread PRRT_kwDOS9Wxuc6QKTPK): `agent.reportToParent` must
/// scope-guard the caller-supplied `workspace_id` the same way `agent.get` /
/// `agent.getConversation` do — a call whose `workspace_id` does not match
/// the caller session's own workspace is rejected with `NotFound` before any
/// state changes (completion-report persistence, `review_required`
/// transition, subscription notification). The child session must remain
/// untouched (no `completionReport`, no `updated_at` bump).
#[tokio::test]
async fn report_to_parent_cross_workspace_rejected_and_has_no_side_effects() {
    let (_t, svc, ws_a) = setup().await;
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("insert ws_b");

    let parent = create_agent(&svc, &ws_a, "Parent").await;
    let created = svc
        .agent_create_op(
            ws_a.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent.clone()),
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let before = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("load child before");
    assert!(before.completion_report.is_none());
    assert!(before.completion_report_timestamp.is_none());

    // Cross-workspace call: the child lives in ws_a but the caller supplies
    // ws_b. The scope guard mirrors `agent_get_op` / `agent_get_conversation_op`
    // and returns `NotFound`.
    let err = svc
        .agent_report_to_parent_op(ws_b.clone(), json!("cross-workspace"), Some(child.clone()))
        .await
        .expect_err("cross-workspace reportToParent must be rejected");
    match err {
        Error::NotFound(msg) => assert!(
            msg.contains(child.0.as_str()),
            "NotFound message should reference the child agent id: {msg}"
        ),
        other => panic!("expected Error::NotFound, got {other:?}"),
    }

    // No side effects: the persisted session is byte-identical to `before`.
    let after = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("load child after");
    assert!(
        after.completion_report.is_none(),
        "completion_report must not be persisted on a rejected cross-workspace call: {:?}",
        after.completion_report
    );
    assert!(
        after.completion_report_timestamp.is_none(),
        "completion_report_timestamp must not be set on a rejected cross-workspace call: {:?}",
        after.completion_report_timestamp
    );
    assert_eq!(
        after.updated_at, before.updated_at,
        "child session updated_at must not be bumped on rejection: {} -> {}",
        before.updated_at, after.updated_at
    );

    // Same-workspace call still succeeds (the guard is a scope check, not a
    // regression to the normal path).
    let ok = svc
        .agent_report_to_parent_op(ws_a.clone(), json!("in-workspace"), Some(child.clone()))
        .await
        .expect("in-workspace reportToParent must succeed");
    assert_eq!(ok["ok"], json!(true));
    assert_eq!(ok["parentAgentId"].as_str(), Some(parent.0.as_str()));
}

/// SUB-2 end-to-end: `agent.reportToParent` emits zero immediate wakes; the
/// single parent wake is delivered by the child's terminal `agent:idle` via
/// the still-armed completion watch, and the wake text carries the persisted
/// completion report (Report:...).
/// Test (a): double subscribe returns same ID, only one delivery
#[tokio::test]
async fn watch_completion_dedupe() {
    let (_tmp, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;

    // First subscribe
    let r1 = svc
        .agent_watch_completion_op(ws.clone(), parent.clone(), child.clone())
        .await
        .unwrap();
    let id1 = r1["subscriptionId"].as_str().unwrap();

    // Second subscribe (should return same ID)
    let r2 = svc
        .agent_watch_completion_op(ws.clone(), parent.clone(), child.clone())
        .await
        .unwrap();
    let id2 = r2["subscriptionId"].as_str().unwrap();

    assert_eq!(
        id1, id2,
        "repeated subscribe must return same subscriptionId"
    );

    // Only one watch should exist
    let watches = svc.list_watches_for_parent(&ws, &parent);
    assert_eq!(watches.len(), 1, "only one watch should exist after dedupe");

    // Deliver an idle event - parent should receive exactly ONE wake
    let baseline = parent_message_count(&svc, &parent).await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({ "agentId": child.0 }),
    ))
    .await;
    assert_eq!(
        parent_message_count(&svc, &parent).await,
        baseline + 1,
        "exactly one delivery even with dedupe"
    );
}

#[tokio::test]
async fn report_to_parent_delivers_immediate_wake_then_idle_suppressed() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    // Immediate-mode delegation arms a oneShot completion watch on the child.
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    // The child's delegate-time first message is already in the parent's
    // transcript queue path via the child (not the parent). Baseline: the
    // parent has no messages yet.
    let baseline = parent_message_count(&svc, &parent).await;

    let report = "shipped it";
    svc.agent_report_to_parent_op(ws.clone(), json!(report), Some(child.clone()))
        .await
        .expect("report");
    // Report-time wake: parent receives the wake immediately.
    assert_eq!(parent_message_count(&svc, &parent).await, baseline + 1);
    let text = parent_messages_text(&svc, &parent).await;
    assert!(
        text.contains(&format!("Report: {report}")),
        "wake text must carry the report at reportToParent time: {text}"
    );

    // Drive the child's `agent:idle` (mirrors the turn worker's
    // stream-complete branch). The wake is suppressed because the watch is
    // marked as report_delivered.
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({ "agentId": child.0, "report": report }),
    ))
    .await;
    // No second wake fires — idle suppression working.
    assert_eq!(parent_message_count(&svc, &parent).await, baseline + 1);
    // The oneShot watch is consumed after the idle suppression.
    assert!(svc.find_watches_for_child(&ws, &child).is_empty());
}

/// Regression for PR #237: after a child calls reportToParent (which marks the
/// watch as report_delivered and delivers an immediate wake), `agent:failed` and
/// `agent:deleted` events STILL deliver their completion wake to the parent.
/// Only `agent:idle` is suppressed by the report_delivered flag.
#[tokio::test]
async fn report_to_parent_then_failed_or_deleted_still_wakes_parent() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;

    // Scenario 1: reportToParent → agent:failed
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("do the thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child1");
    let child1 = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let baseline1 = parent_message_count(&svc, &parent).await;

    svc.agent_report_to_parent_op(ws.clone(), json!("partial report"), Some(child1.clone()))
        .await
        .expect("report child1");
    // Report wake delivered immediately.
    assert_eq!(parent_message_count(&svc, &parent).await, baseline1 + 1);

    // Child fails after reporting. This is a NEW signal (not a duplicate).
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_FAILED,
        &child1,
        json!({ "agentId": child1.0, "error": "crashed" }),
    ))
    .await;
    // Parent MUST receive the failed wake (report_delivered suppresses ONLY idle).
    assert_eq!(
        parent_message_count(&svc, &parent).await,
        baseline1 + 2,
        "parent must receive both report wake AND failed wake"
    );
    let text = parent_messages_text(&svc, &parent).await;
    assert!(
        text.contains("failed") || text.contains("crashed"),
        "parent must see failure notification in wake: {text}"
    );

    // Scenario 2: reportToParent → agent:deleted
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("another thing".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child2");
    let child2 = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let baseline2 = parent_message_count(&svc, &parent).await;

    svc.agent_report_to_parent_op(ws.clone(), json!("another report"), Some(child2.clone()))
        .await
        .expect("report child2");
    assert_eq!(parent_message_count(&svc, &parent).await, baseline2 + 1);

    // Child is deleted after reporting.
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_DELETED,
        &child2,
        json!({ "agentId": child2.0 }),
    ))
    .await;
    // Parent MUST receive the deleted wake.
    assert_eq!(
        parent_message_count(&svc, &parent).await,
        baseline2 + 2,
        "parent must receive both report wake AND deleted wake"
    );
}

/// SUB-2: repeated `agent.wakeOrCreate` for the same caller/target reuses the
/// live ungrouped watch instead of stacking duplicates. A single terminal
/// `agent:idle` then produces exactly one parent wake.
#[tokio::test]
async fn wake_or_create_reuses_existing_watch_no_duplicate() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-2 dedupe").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    let input = || AgentWakeOrCreateInput {
        caller_agent_id: Some(caller.clone()),
        ..Default::default()
    };
    let r1 = svc
        .agent_wake_or_create_op(ws.clone(), note_id.clone(), "resume 1".into(), input())
        .await
        .expect("wake 1");
    let r2 = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "resume 2".into(), input())
        .await
        .expect("wake 2");
    // The second wake reuses the first watch's subscription id.
    assert_eq!(r1["subscriptionId"], r2["subscriptionId"]);
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1, "no duplicate watches: {watches:?}");

    // A single terminal agent:idle produces exactly one parent wake.
    let baseline = parent_message_count(&svc, &caller).await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &target,
        json!({ "agentId": target.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &caller).await, baseline + 1);
}

/// SUB-2 (Copilot #104): reusing an existing ungrouped watch on a repeated
/// `agent.wakeOrCreate` refreshes the stored `parent_agent_name` so a rename
/// applied to the caller (via `agent.rename`) between wake calls surfaces
/// through `agent.getSubscriptions` / `describe_subscription`.
#[tokio::test]
async fn wake_or_create_reuse_refreshes_parent_agent_name() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "OldName").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-2 rename").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    let input = || AgentWakeOrCreateInput {
        caller_agent_id: Some(caller.clone()),
        ..Default::default()
    };
    let r1 = svc
        .agent_wake_or_create_op(ws.clone(), note_id.clone(), "resume 1".into(), input())
        .await
        .expect("wake 1");
    let sub_id = r1["subscriptionId"].as_str().expect("sub id").to_string();
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].parent_agent_name, "OldName");

    // Rename the caller between wakes (the exact `agent.rename` path a
    // long-lived coordinator would hit via `agent.rename` / `agent.update`).
    svc.agent_rename_op(caller.clone(), "NewName".into(), false)
        .await
        .expect("rename");

    // Second wake reuses the same watch id AND refreshes the stored name.
    let r2 = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "resume 2".into(), input())
        .await
        .expect("wake 2");
    assert_eq!(r2["subscriptionId"].as_str(), Some(sub_id.as_str()));
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1, "still no duplicate watches: {watches:?}");
    assert_eq!(
        watches[0].parent_agent_name, "NewName",
        "reused watch reflects the caller rename: {watches:?}"
    );
}

/// SUB-2 (Copilot #104 follow-up, thread PRRT_kwDOS9Wxuc6QKPyt): if a live
/// ungrouped watch is removed concurrently between find and refresh (by
/// [`Services::deliver_completion_to_watches`] dropping a oneShot watch, or
/// by an expired [`Services::spawn_watch_cleanup`] task), the follow-up
/// `agent.wakeOrCreate` must fall through to CREATING a new live watch —
/// not return the dead subscription id. Dropping the seeded watch directly
/// stands in for the concurrent removal that would race the pre-fix
/// non-atomic find/refresh pair.
#[tokio::test]
async fn wake_or_create_reuse_after_removal_registers_fresh_watch() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-2 reuse-after-removal").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    let input = || AgentWakeOrCreateInput {
        caller_agent_id: Some(caller.clone()),
        ..Default::default()
    };

    // First wake registers a fresh oneShot watch.
    let r1 = svc
        .agent_wake_or_create_op(ws.clone(), note_id.clone(), "resume 1".into(), input())
        .await
        .expect("wake 1");
    let sub1 = r1["subscriptionId"].as_str().expect("sub id").to_string();
    assert_eq!(svc.list_watches_for_parent(&ws, &caller).len(), 1);

    // Simulate the concurrent removal window (deliver_completion_to_watches
    // dropping the oneShot watch, or an expired queued-watch cleanup task
    // removing it) by dropping the seeded watch directly.
    assert!(
        svc.remove_watch(&ws, &sub1),
        "seeded watch must be removed for the race scenario"
    );
    assert!(svc.list_watches_for_parent(&ws, &caller).is_empty());

    // Second wake finds no live watch to reuse and MUST create a new one —
    // the caller must never be handed back the dead subscription id.
    let r2 = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "resume 2".into(), input())
        .await
        .expect("wake 2");
    let sub2 = r2["subscriptionId"].as_str().expect("sub id").to_string();
    assert_ne!(sub1, sub2, "must not reuse the dead subscription id");
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1, "one fresh live watch: {watches:?}");
    assert_eq!(watches[0].id, sub2, "returned id points to the live watch");
}

/// `agent.delegate` persists the resolved first message as
/// `metadata.initialMessage` and the child's `metadata.delegationDepth`
/// (parent depth + 1) so a wake-up can resume (P3-1.2b). Delegated children
/// are background agents (G-A1/P3-1.2c).
#[tokio::test]
async fn delegate_persists_initial_message_and_delegation_depth() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let out = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("Do the thing".into()),
                ..Default::default()
            },
            Some(parent),
        )
        .await
        .expect("delegate");
    let child = AgentId::from(out["agentId"].as_str().unwrap());
    let got = svc.agent_get_op(child, None).await.expect("get child");
    let v = serde_json::to_value(&got).expect("lite json");
    assert_eq!(v["metadata"]["initialMessage"], "Do the thing");
    assert_eq!(v["metadata"]["delegationDepth"], json!(1));
    assert_eq!(v["metadata"]["isBackground"], json!(true));
}

/// Port of the reference `MAX_DELEGATION_DEPTH` guard: a caller already at the
/// max depth cannot delegate further, and the error carries the depth in its
/// message so downstream tools can render it verbatim.
#[tokio::test]
async fn delegate_rejects_when_parent_at_max_depth() {
    let (_t, svc, ws) = setup().await;
    // Create a parent already at depth 2 (MAX_DELEGATION_DEPTH).
    let extra = intent_core::AgentCreateExtra {
        metadata: Some(json!({ "delegationDepth": intent_core::MAX_DELEGATION_DEPTH })),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("MaxDepth".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create parent at max depth");
    let parent = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let err = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("still trying".into()),
                ..Default::default()
            },
            Some(parent),
        )
        .await
        .expect_err("delegate must be refused at max depth");
    let msg = err.to_string();
    assert!(
        msg.contains("maximum delegation depth"),
        "unexpected err: {msg}"
    );
    assert!(
        msg.contains(&format!("({})", intent_core::MAX_DELEGATION_DEPTH)),
        "missing depth in err: {msg}"
    );
}

/// LC-1: the service-layer guard inside `agent_create_op` mirrors the MCP
/// `create_agent` front-door check, so RPC/service callers spawning a child
/// for a parent already at `MAX_DELEGATION_DEPTH` are also refused. A parent
/// below the max (or an unknown parent, read as depth 0) stays accepted.
#[tokio::test]
async fn create_rejects_when_parent_at_max_depth() {
    let (_t, svc, ws) = setup().await;
    let extra = intent_core::AgentCreateExtra {
        metadata: Some(json!({ "delegationDepth": intent_core::MAX_DELEGATION_DEPTH })),
        ..Default::default()
    };
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("MaxDepth".into()),
            None,
            None,
            None,
            None,
            false,
            extra,
        )
        .await
        .expect("create parent at max depth");
    let parent = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let err = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            None,
            None,
            Some(parent),
            None,
            false,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect_err("create must be refused when parent at max depth");
    let msg = err.to_string();
    assert!(
        msg.contains("maximum delegation depth"),
        "unexpected err: {msg}"
    );
    // A parent below the max still spawns children through the same path.
    let shallow = create_agent(&svc, &ws, "Shallow").await;
    svc.agent_create_op(
        ws.clone(),
        Some("Child OK".into()),
        None,
        None,
        Some(shallow),
        None,
        false,
        intent_core::AgentCreateExtra::default(),
    )
    .await
    .expect("create under a shallow parent succeeds");
}

/// When workspace.cowIsolation is enabled, the delegate logic attempts CoW
/// provisioning; when the workspace has a repository and worktree, effectiveIsolation
/// reports "cow". This test uses a workspace without repository_path, so CoW cannot
/// provision and effectiveIsolation is absent (graceful fallback to shared mode).
/// The setting is read and respected; actual provisioning is workspace-dependent.
#[tokio::test]
async fn delegate_reads_cow_isolation_setting() {
    let (_t, svc, ws) = setup().await;
    // Enable workspace.cowIsolation setting
    svc.settings_update(json!([{
        "path": "workspace.cowIsolation",
        "value": true
    }]))
    .await
    .expect("enable cowIsolation");

    // Delegate without explicit isolation parameter (workspace has no repository_path)
    let out = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("Do work".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");

    // Setting is enabled but CoW can't provision without repository; effectiveIsolation absent
    assert!(
        out.get("effectiveIsolation").is_none(),
        "expected absent effectiveIsolation when repository_path is None"
    );
}

/// When workspace.cowIsolation is disabled (default), delegations use shared mode.
#[tokio::test]
async fn delegate_defaults_to_shared_when_setting_disabled() {
    let (_t, svc, ws) = setup().await;
    // workspace.cowIsolation defaults to false, no need to set it

    let out = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("Do work".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");

    // effectiveIsolation should be absent (shared mode, no sandbox provisioning)
    assert!(
        out.get("effectiveIsolation").is_none(),
        "expected absent effectiveIsolation when setting disabled"
    );
}

/// Explicit isolation parameter overrides workspace.cowIsolation setting.
#[tokio::test]
async fn delegate_explicit_isolation_overrides_setting() {
    let (_t, svc, ws) = setup().await;
    // Enable workspace.cowIsolation setting
    svc.settings_update(json!([{
        "path": "workspace.cowIsolation",
        "value": true
    }]))
    .await
    .expect("enable cowIsolation");

    // Delegate WITH explicit isolation: "shared" (overriding the setting)
    let out = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("Do work".into()),
                isolation: Some("shared".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");

    // effectiveIsolation should be absent (explicit shared mode skips provisioning)
    assert!(
        out.get("effectiveIsolation").is_none(),
        "explicit shared isolation should skip provisioning"
    );
}

/// The top-level (RPC / user) front door stays parentless and is never
/// subject to the depth guard even when a foreground parent exists.
#[tokio::test]
async fn delegate_without_parent_bypasses_depth_guard() {
    let (_t, svc, ws) = setup().await;
    // No caller_agent_id: this is the top-level create path.
    let out = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("user-initiated".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("parentless delegate succeeds");
    assert!(out["agentId"].is_string());
}

#[tokio::test]
async fn delete_removes_session() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Doomed").await;
    let r = svc.agent_delete_op(id.clone(), None).await.expect("delete");
    assert_eq!(r["success"], true);
    assert!(svc.agent_get_op(id, None).await.is_err());
}

#[tokio::test]
async fn queue_lifecycle_add_get_edit_remove() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Q").await;
    let added = svc
        .agent_queue_message_op(id.clone(), "hello".into(), None, None)
        .await
        .expect("queue");
    assert_eq!(added["success"], true);
    let mid = added["queuedMessage"]["id"].as_str().unwrap().to_string();
    // iOS-required wire shape: {id, content, queuedAt, position} (no createdAt/agentId).
    assert_eq!(added["queuedMessage"]["position"], 0);
    assert!(added["queuedMessage"]["queuedAt"].is_string());
    assert!(added["queuedMessage"].get("createdAt").is_none());
    assert!(added["queuedMessage"].get("agentId").is_none());

    let q = svc
        .agent_get_queue_op(id.clone(), None)
        .await
        .expect("getQueue");
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
    let q = svc
        .agent_get_queue_op(id.clone(), None)
        .await
        .expect("getQueue");
    assert_eq!(q["queue"][0]["content"], "edited");

    svc.agent_remove_queued_message_op(id.clone(), mid)
        .await
        .expect("remove");
    let q = svc.agent_get_queue_op(id, None).await.expect("getQueue");
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
        .agent_queue_message_op(id.clone(), "first".into(), None, None)
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
        .agent_queue_message_op(id.clone(), "first".into(), None, None)
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
        .agent_queue_message_op(id.clone(), "first".into(), None, None)
        .await
        .expect("queue first");
    let a_mid = a["queuedMessage"]["id"].as_str().unwrap().to_string();
    let b = svc
        .agent_queue_message_op(id.clone(), "second".into(), None, None)
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
        .agent_queue_message_op(id.clone(), "draft".into(), None, None)
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
        .agent_send_message_op(
            id.clone(),
            "do it".into(),
            Some("m1".into()),
            None,
            None,
            None,
        )
        .await
        .expect("send");
    assert_eq!(r["queued"], false);
    assert_eq!(r["messageId"], "m1");
    let conv = svc
        .agent_get_conversation_op(id, None, None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 1);
    assert_eq!(conv["messages"][0]["role"], "user");
}

/// Sender attribution: the store-only `agent_send_message_op` (no runtime
/// manager wired) must persist a caller-supplied `messageMetadata` on the
/// transcript row instead of silently dropping it, so attribution is
/// consistent across deployments with and without an attached manager.
#[tokio::test]
async fn send_message_op_persists_message_metadata() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "MetaRecv").await;
    let metadata = json!({
        "type": "agent_message",
        "fromAgentId": "agent-11111111-1111-1111-1111-111111111111",
        "fromAgentName": "Coordinator"
    });
    let r = svc
        .agent_send_message_op(
            id.clone(),
            "tagged".into(),
            None,
            None,
            None,
            Some(metadata.clone()),
        )
        .await
        .expect("send");
    assert_eq!(r["queued"], false);
    let session = svc.store().get_agent_session(&id).await.expect("session");
    assert_eq!(session.messages.len(), 1);
    assert_eq!(
        session.messages[0].metadata.as_ref(),
        Some(&metadata),
        "store-only send must persist messageMetadata verbatim"
    );
}

/// Sender attribution: `agent_send_to_task_op` on the store-only fallback
/// path (no runtime manager) must plumb `message_metadata` through to the
/// persisted row rather than dropping it.
#[tokio::test]
async fn send_to_task_store_only_fallback_persists_message_metadata() {
    let (_t, svc, ws) = setup().await;
    let agent_id = create_agent(&svc, &ws, "TaskMetaRecv").await;
    let note_id = seed_task(&svc, &ws, "metadata fallback task").await;
    svc.assign_agent(ws.clone(), note_id.clone(), agent_id.0.clone())
        .await
        .expect("assign");
    let metadata = json!({
        "type": "agent_message",
        "fromAgentId": "agent-22222222-2222-2222-2222-222222222222",
        "fromAgentName": "Sender"
    });
    let r = svc
        .agent_send_to_task_op(
            ws.clone(),
            note_id,
            "tagged follow-up".into(),
            None,
            Some(metadata.clone()),
        )
        .await
        .expect("send_to_task");
    assert_eq!(r["ok"], true);
    let session = svc
        .store()
        .get_agent_session(&agent_id)
        .await
        .expect("session");
    assert_eq!(session.messages.len(), 1);
    assert_eq!(
        session.messages[0].metadata.as_ref(),
        Some(&metadata),
        "store-only sendToTask fallback must persist messageMetadata verbatim"
    );
}

#[tokio::test]
async fn send_message_auto_queues_for_unknown_agent() {
    let (_t, svc, _ws) = setup().await;
    let id = AgentId::from("agent-00000000-0000-0000-0000-000000000000");
    let r = svc
        .agent_send_message_op(id, "hi".into(), None, None, None, None)
        .await
        .expect("send");
    assert_eq!(r["queued"], true);
    assert_eq!(r["queuedMessage"]["content"], "hi");
}

/// STAB-7: agent_send_message_op fallback must preserve image_blocks and
/// file_blocks when auto-queueing on store failure (matching the runtime
/// manager path's behavior).
#[tokio::test]
async fn send_message_op_preserves_attachments_on_auto_queue() {
    let (_t, svc, _ws) = setup().await;
    let id = AgentId::from("agent-00000000-0000-0000-0000-000000000000");
    let image_blocks = json!([
        { "type": "image", "data": "base64data", "mimeType": "image/png" }
    ]);
    let file_blocks = json!([
        { "type": "file", "data": "filedata", "mimeType": "text/plain", "fileName": "test.txt" }
    ]);
    let r = svc
        .agent_send_message_op(
            id.clone(),
            "check these".into(),
            None,
            Some(image_blocks.clone()),
            Some(file_blocks.clone()),
            None,
        )
        .await
        .expect("send");
    assert_eq!(r["queued"], true);
    assert_eq!(r["queuedMessage"]["content"], "check these");
    assert_eq!(r["queuedMessage"]["imageBlocks"], image_blocks);
    assert_eq!(r["queuedMessage"]["fileBlocks"], file_blocks);
    // Also verify getQueue returns the same attachments.
    let queue = svc.agent_get_queue_op(id, None).await.expect("queue");
    assert_eq!(queue["queue"][0]["imageBlocks"], image_blocks);
    assert_eq!(queue["queue"][0]["fileBlocks"], file_blocks);
}

/// STAB-133: `agent_send_message_op` must persist FE-supplied image and file
/// blocks into the transcript row (after the text block) so the conversation
/// view can render them.
#[tokio::test]
async fn send_message_op_persists_attachment_blocks_in_transcript() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "AttachRecv").await;
    let image_blocks = json!([
        { "type": "image", "data": "imgdata", "mimeType": "image/png" }
    ]);
    let file_blocks = json!([
        { "type": "file", "data": "filedata", "mimeType": "text/plain", "fileName": "notes.txt" }
    ]);
    let r = svc
        .agent_send_message_op(
            id.clone(),
            "see attached".into(),
            None,
            Some(image_blocks),
            Some(file_blocks),
            None,
        )
        .await
        .expect("send");
    assert_eq!(r["queued"], false);
    let conv = svc
        .agent_get_conversation_op(id, None, None, None)
        .await
        .expect("conv");
    let content = &conv["messages"][0]["contentBlocks"];
    let blocks = content.as_array().expect("content blocks array");
    assert_eq!(blocks.len(), 3, "text + image + file blocks: {content}");
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "see attached");
    assert_eq!(blocks[1]["type"], "image");
    assert_eq!(blocks[1]["data"], "imgdata");
    assert_eq!(blocks[1]["mimeType"], "image/png");
    assert_eq!(blocks[2]["type"], "file");
    assert_eq!(blocks[2]["data"], "filedata");
    assert_eq!(blocks[2]["fileName"], "notes.txt");
    assert_eq!(blocks[2]["mimeType"], "text/plain");
}

/// STAB-133: `agent_force_message_op` must persist FE-supplied image and file
/// blocks into the transcript row (after the text block).
#[tokio::test]
async fn force_message_op_persists_attachment_blocks_in_transcript() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "AttachForce").await;
    let image_blocks = json!([
        { "type": "image", "data": "imgdata2", "mimeType": "image/jpeg" }
    ]);
    let r = svc
        .agent_force_message_op(
            id.clone(),
            "m-force-1".into(),
            "forced with image".into(),
            Some(image_blocks),
            None,
        )
        .await
        .expect("force");
    assert_eq!(r["queued"], false);
    let conv = svc
        .agent_get_conversation_op(id, None, None, None)
        .await
        .expect("conv");
    let content = &conv["messages"][0]["contentBlocks"];
    let blocks = content.as_array().expect("content blocks array");
    assert_eq!(blocks.len(), 2, "text + image blocks: {content}");
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "forced with image");
    assert_eq!(blocks[1]["type"], "image");
    assert_eq!(blocks[1]["data"], "imgdata2");
    assert_eq!(blocks[1]["mimeType"], "image/jpeg");
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

#[test]
fn parse_model_list_json_maps_rich_rows_and_skips_incomplete() {
    let out = r#"{ "models": [
        { "shortName": "sonnet4.5", "displayName": "Sonnet 4.5",
          "description": "Balanced general model", "modelGroupPriority": 1,
          "costTier": 2, "badges": [{ "color": "green", "label": "Auto" }],
          "effortLevels": ["low", "high"], "isDefault": true, "priority": 1 },
        { "shortName": "old-model", "displayName": "Old", "isLegacyModel": true },
        { "displayName": "No shortName" },
        { "shortName": "haiku4.5", "displayName": "Haiku", "description": "",
          "badges": [], "effortLevels": [] }
    ] }"#;
    let rows = parse_model_list_json(out).expect("parsed");
    assert_eq!(rows.len(), 3, "row without shortName is skipped");
    assert_eq!(rows[0]["id"], "sonnet4.5");
    assert_eq!(rows[0]["name"], "Sonnet 4.5");
    assert_eq!(rows[0]["provider"], "auggie");
    assert_eq!(rows[0]["description"], "Balanced general model");
    assert_eq!(rows[0]["modelGroupPriority"], 1);
    assert_eq!(rows[0]["costTier"], 2);
    assert_eq!(rows[0]["badges"][0]["label"], "Auto");
    assert_eq!(rows[0]["effortLevels"], json!(["low", "high"]));
    assert_eq!(rows[0]["isDefault"], true);
    assert_eq!(rows[0]["priority"], 1);
    assert_eq!(rows[1]["isLegacyModel"], true);
    // Empty description / empty arrays are omitted, not emitted as empties.
    let haiku = rows[2].as_object().unwrap();
    assert_eq!(haiku["id"], "haiku4.5");
    assert!(!haiku.contains_key("description"));
    assert!(!haiku.contains_key("badges"));
    assert!(!haiku.contains_key("effortLevels"));
}

#[test]
fn parse_model_list_json_rejects_non_catalog_payloads() {
    assert!(parse_model_list_json("not json").is_none());
    assert!(parse_model_list_json("{}").is_none());
    assert!(parse_model_list_json(r#"{ "models": "nope" }"#).is_none());
}

#[test]
fn finalize_model_rows_filters_legacy_and_sorts() {
    let rows = vec![
        json!({ "id": "z", "name": "Zeta", "provider": "auggie" }),
        json!({ "id": "old", "name": "Old", "provider": "auggie", "isLegacyModel": true }),
        json!({ "id": "b", "name": "Beta", "provider": "auggie",
                "modelGroupPriority": 2, "priority": 1 }),
        json!({ "id": "a", "name": "Alpha", "provider": "auggie",
                "modelGroupPriority": 1, "priority": 2 }),
        json!({ "id": "a2", "name": "Alpha2", "provider": "auggie",
                "modelGroupPriority": 1, "priority": 1 }),
    ];
    let out = finalize_model_rows(rows);
    let ids: Vec<&str> = out.iter().map(|r| r["id"].as_str().unwrap()).collect();
    // Group asc, then priority asc, then name; missing priorities sort last.
    assert_eq!(ids, vec!["a2", "a", "b", "z"]);
    assert!(out
        .iter()
        .all(|r| r.as_object().unwrap().get("isLegacyModel").is_none()));
}

#[tokio::test]
async fn models_list_returns_non_empty_catalog_with_source() {
    let (_t, svc, _ws) = setup().await;
    let res = svc.models_list_op().await.expect("models.list");
    let models = res["models"].as_array().unwrap();
    assert!(!models.is_empty());
    assert!(models[0].get("id").is_some());
    assert!(models[0].get("name").is_some());
    assert!(models[0].get("provider").is_some());
    let source = res["source"].as_str().unwrap();
    assert!(source == "auggie" || source == "static", "source: {source}");
    // A second call is served from the cache (auggie) or recomputed statics —
    // either way the result is stable within the TTL window.
    let again = svc.models_list_op().await.expect("models.list again");
    assert_eq!(res, again);
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

/// Report-time wake: a delegated caller's `reportToParent` delivers an
/// immediate parent wake containing the report. The report is persisted on the
/// child session (`completion_report`) and the TS-shaped result is returned.
/// The watch is marked as report_delivered, so the child's subsequent
/// `agent:idle` does NOT deliver a second wake (suppressed), which is asserted
/// by the sibling `report_to_parent_delivers_immediate_wake_then_idle_suppressed`
/// test.
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
            Default::default(),
        )
        .await
        .expect("create delegated child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    let report = "done: shipped the thing";
    let result = svc
        .agent_report_to_parent_op(ws.clone(), json!(report), Some(child.clone()))
        .await
        .expect("report delivered");
    assert_eq!(result["ok"], json!(true));
    assert_eq!(result["parentAgentId"].as_str(), Some(parent.0.as_str()));
    assert_eq!(result["reportLength"], json!(report.chars().count() as i64));
    assert!(result["savedAt"].is_string());

    // Report-time wake: reportToParent now delivers an immediate wake to the parent.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(parent_session.messages.len(), 1);
    let wake_text = parent_messages_text(&svc, &parent).await;
    assert!(
        wake_text.contains(&format!("Report: {report}")),
        "wake must contain the report: {wake_text}"
    );
    let child_session = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child session");
    assert_eq!(child_session.completion_report.as_deref(), Some(report));
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
    // Post-WSAPI-8: discrete `delegate_task` is gone; route through the
    // unified `workspace_api` tool + `ws.agent.delegate` binding, which
    // reaches the same caller-aware `agent_delegate` op.
    let resp = server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": {
                    "code": "return await ws.agent.delegate({ agentInstructions: 'do work' });",
                    "summary": "mcp delegate stamps parent"
                }
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
/// reports back via `report_to_parent` (caller-aware; the registry/dispatch name
/// is bare — agents still see `report_to_parent_workspace-mcp` because the
/// provider appends the server suffix). Report-time wake: reportToParent
/// delivers an immediate parent wake; the report is persisted on the child
/// session and the parent receives the wake containing the report immediately.
/// The same report tool through a caller-less server (the RPC / no-caller path)
/// yields an `isError: true` workspace_api tool result. This is the
/// service-level integration coverage chosen over a node-gated UDS E2E so the
/// full loop is exercised deterministically without an external `node`
/// dependency.
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
                "name": "workspace_api",
                "arguments": {
                    "code": "return await ws.agent.delegate({ agentInstructions: 'do work' });",
                    "summary": "parent delegates via ws.agent.delegate"
                }
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
                "name": "workspace_api",
                "arguments": {
                    "code": format!(
                        "return await ws.agent.reportToParent({});",
                        serde_json::json!(report)
                    ),
                    "summary": "child reports via ws.agent.reportToParent"
                }
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

    // Report-time wake: the parent receives an immediate wake containing the
    // report. The report is persisted on the child session.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(parent_session.messages.len(), 1);
    let wake_text = parent_messages_text(&svc, &parent).await;
    assert!(
        wake_text.contains(&format!("Report: {report}")),
        "wake must contain the report: {wake_text}"
    );
    let child_session = svc
        .store()
        .get_agent_session(&child)
        .await
        .expect("child session");
    assert_eq!(child_session.completion_report.as_deref(), Some(report));

    // RPC / no-caller path: after the WSAPI-8 cutover the report flows
    // through the unified `workspace_api` tool executing
    // `ws.agent.reportToParent`, and the daemon error surfaces as an
    // `isError: true` workspace_api tool result (workspace_api shapes
    // JS-side failures as tool-result text bodies rather than JSON-RPC
    // protocol errors — reference parity with the TS tool).
    let no_caller_server = WorkspaceMcpServer::new(api, ws.clone());
    let err_resp = no_caller_server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": {
                    "code": "return await ws.agent.reportToParent('orphan');",
                    "summary": "orphan report"
                }
            }
        }))
        .await
        .expect("error response");
    assert_eq!(err_resp["result"]["isError"], json!(true));
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
        .update_agent_session(&session.workspace_id.clone(), &session)
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

/// `agent_watch_completion_op` (AS-5, the MCP `create_agent` auto-subscribe):
/// registers exactly one oneShot watch for the parent→child pair and returns
/// its subscription id.
#[tokio::test]
async fn watch_completion_registers_oneshot_watch() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;

    let resp = svc
        .agent_watch_completion_op(ws.clone(), parent.clone(), child.clone())
        .await
        .expect("watch completion");
    assert_eq!(resp["ok"], serde_json::json!(true));
    let sub_id = resp["subscriptionId"].as_str().expect("subscriptionId");

    let watches = svc.list_watches_for_parent(&ws, &parent);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].id, sub_id);
    assert!(watches[0].one_shot);
    assert!(watches[0].group_id.is_none());
    assert_eq!(watches[0].child_agent_id, child);
    assert_eq!(svc.find_watches_for_child(&ws, &child).len(), 1);
}

/// The deleted-parent guard applies to the `create_agent` auto-subscribe too:
/// `ok: false`, no subscription id, no watch registered.
#[tokio::test]
async fn watch_completion_skips_when_parent_deleted() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let child = create_agent(&svc, &ws, "Child").await;
    let mut session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    session.status = intent_core::AgentStatus::Deleted;
    svc.store()
        .update_agent_session(&session.workspace_id.clone(), &session)
        .await
        .expect("flag deleted");

    let resp = svc
        .agent_watch_completion_op(ws.clone(), parent.clone(), child)
        .await
        .expect("watch completion");
    assert_eq!(resp["ok"], serde_json::json!(false));
    assert!(resp["subscriptionId"].is_null());
    assert!(svc.list_watches_for_parent(&ws, &parent).is_empty());
}

// SUB-1 — sender auto-subscribe on the send/wake coordination paths.
// ────────────────────────────────────────────────────────────────────────────

/// A foreground/coordinator sender is auto-subscribed: exactly one oneShot
/// caller→target watch, subscription id returned (the TS
/// `maybeSubscribeCallerToAgentCompletionForCoordinationMessage`).
#[tokio::test]
async fn sender_watch_registers_oneshot_for_foreground_caller() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Target").await;

    let resp = svc
        .agent_watch_completion_for_sender_op(ws.clone(), caller.clone(), target.clone())
        .await
        .expect("sender watch");
    assert_eq!(resp["ok"], serde_json::json!(true));
    let sub_id = resp["subscriptionId"].as_str().expect("subscriptionId");

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].id, sub_id);
    assert!(watches[0].one_shot);
    assert!(watches[0].group_id.is_none());
    assert_eq!(watches[0].child_agent_id, target);
}

/// A delegated background task sender (isBackground + metadata
/// `createdByAgentId` + `taskNoteId`, the TS
/// `isDelegatedBackgroundTaskSession`) is NOT passively subscribed:
/// `ok: false`, no subscription id, no watch.
#[tokio::test]
async fn sender_watch_skips_delegated_background_caller() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Background child").await;
    let target = create_agent(&svc, &ws, "Sibling").await;
    let mut session = svc
        .store()
        .get_agent_session(&caller)
        .await
        .expect("caller session");
    session.is_background = true;
    session.metadata = Some(json!({
        "createdByAgentId": "agent-parent",
        "taskNoteId": "note-1",
    }));
    svc.store()
        .update_agent_session(&session.workspace_id.clone(), &session)
        .await
        .expect("flag background");

    let resp = svc
        .agent_watch_completion_for_sender_op(ws.clone(), caller.clone(), target)
        .await
        .expect("sender watch");
    assert_eq!(resp["ok"], serde_json::json!(false));
    assert!(resp["subscriptionId"].is_null());
    assert!(svc.list_watches_for_parent(&ws, &caller).is_empty());
}

/// `agent.wakeOrCreate` woke-existing with a caller: the caller gets a oneShot
/// watch on the woken assignee; the response carries `subscriptionId` and the
/// reference tool's notification text.
#[tokio::test]
async fn wake_or_create_woke_existing_subscribes_caller() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-1 wake").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    let input = AgentWakeOrCreateInput {
        caller_agent_id: Some(caller.clone()),
        ..Default::default()
    };
    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "resume".into(), input)
        .await
        .expect("wake");
    assert_eq!(resp["action"], "woke_existing");
    let sub_id = resp["subscriptionId"].as_str().expect("subscriptionId");
    let message = resp["message"].as_str().expect("message");
    assert!(
        message.contains("You will be notified when the agent responds."),
        "notification text parity: {message}"
    );

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].id, sub_id);
    assert!(watches[0].one_shot);
    assert_eq!(watches[0].child_agent_id, target);
}

/// The caller-less (FE/RPC) wake registers nothing and the response stays in
/// the pre-SUB-1 shape (no `subscriptionId` / `message` keys).
#[tokio::test]
async fn wake_or_create_without_caller_registers_no_watch() {
    let (_t, svc, ws) = setup().await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-1 no caller").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    let resp = svc
        .agent_wake_or_create_op(
            ws.clone(),
            note_id,
            "resume".into(),
            AgentWakeOrCreateInput::default(),
        )
        .await
        .expect("wake");
    assert_eq!(resp["action"], "woke_existing");
    assert!(resp.get("subscriptionId").is_none());
    assert!(resp.get("message").is_none());
    assert!(svc.find_watches_for_child(&ws, &target).is_empty());
}

/// Queued-to-active wake: the context message queues behind the assignee's
/// in-flight turn, so the caller's watch is NON-oneShot (it must survive the
/// current turn's `agent:idle`) and the response carries the queued text.
#[tokio::test]
async fn wake_or_create_queued_registers_non_oneshot_watch() {
    let (_t, svc, manager, _bus, ws) = setup_with_manager().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Busy assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-1 queued").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");
    // Occupy the assignee's in-flight slot so `deliver_wake_message` takes the
    // enqueue branch deterministically.
    assert!(manager.try_begin_turn(&target, &ws).await, "claim slot");

    let input = AgentWakeOrCreateInput {
        caller_agent_id: Some(caller.clone()),
        ..Default::default()
    };
    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "follow up".into(), input)
        .await
        .expect("wake");
    assert_eq!(resp["action"], "message_queued_to_active_agent");
    let sub_id = resp["subscriptionId"].as_str().expect("subscriptionId");
    let message = resp["message"].as_str().expect("message");
    assert!(
        message.contains("Context message has been queued"),
        "queued text parity: {message}"
    );
    assert!(
        message.contains("You will be notified when the agent responds."),
        "notification text parity: {message}"
    );

    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(watches[0].id, sub_id);
    assert!(!watches[0].one_shot, "queued watch must survive agent:idle");
    assert_eq!(watches[0].child_agent_id, target);

    manager.release_slot(&target).await;
}

/// The queued watch's leak guard: `spawn_watch_cleanup` removes the watch
/// after the timeout elapses (the TS 5-minute `setTimeout` unsubscribe).
#[tokio::test]
async fn spawn_watch_cleanup_removes_watch_after_timeout() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Target").await;
    let sub_id = svc.register_completion_watch(
        &ws,
        caller.clone(),
        "Coordinator".into(),
        target.clone(),
        false,
        None,
    );
    assert_eq!(svc.list_watches_for_parent(&ws, &caller).len(), 1);

    svc.spawn_watch_cleanup(
        ws.clone(),
        caller.clone(),
        sub_id,
        Duration::from_millis(50),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if svc.list_watches_for_parent(&ws, &caller).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("cleanup did not remove the queued watch");
}

/// SUB-2 (unresolved copilot review thread PRRT_kwDOS9Wxuc6QIRcq on PR #104):
/// a queued wake must never reuse a pre-existing oneShot watch for the same
/// caller/target pair, because a oneShot watch is removed on the first
/// `agent:idle` — which is precisely the idle that a queued message needs to
/// survive. The queued path must therefore register a fresh non-oneShot
/// watch alongside the existing oneShot one. A pre-seeded oneShot watch
/// (registered via [`Services::register_completion_watch`] to sidestep
/// runtime turn-starting side effects) drives the queued wake through the
/// mode-mismatch fall-through in [`Services::agent_wake_or_create_op`].
#[tokio::test]
async fn wake_or_create_queued_does_not_reuse_or_receive_oneshot_watch() {
    let (_t, svc, manager, _bus, ws) = setup_with_manager().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let note_id = seed_task(&svc, &ws, "SUB-2 mode mismatch").await;
    svc.assign_agent(ws.clone(), note_id.clone(), target.0.clone())
        .await
        .expect("assign");

    // Seed a oneShot watch for this caller/target pair (as an earlier
    // non-queued wake would have registered).
    let oneshot_sub_id = svc.register_completion_watch(
        &ws,
        caller.clone(),
        "Coordinator".into(),
        target.clone(),
        true,
        None,
    );

    // Occupy the assignee's in-flight slot so the wakeOrCreate takes the
    // queued branch deterministically.
    assert!(manager.try_begin_turn(&target, &ws).await, "claim slot");

    let queued = svc
        .agent_wake_or_create_op(
            ws.clone(),
            note_id,
            "follow up".into(),
            AgentWakeOrCreateInput {
                caller_agent_id: Some(caller.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("queued wake");
    assert_eq!(queued["action"], "message_queued_to_active_agent");
    let queued_sub_id = queued["subscriptionId"]
        .as_str()
        .expect("queued subscriptionId")
        .to_string();

    assert_ne!(
        oneshot_sub_id, queued_sub_id,
        "queued wake must not reuse the oneShot subscription id"
    );

    // Both watches now coexist: the oneShot watch is unchanged and a
    // fresh non-oneShot watch was registered for the queued delivery.
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(
        watches.len(),
        2,
        "queued must add a distinct watch: {watches:?}"
    );
    let oneshot = watches
        .iter()
        .find(|w| w.id == oneshot_sub_id)
        .expect("original oneShot watch still present");
    assert!(
        oneshot.one_shot,
        "existing oneShot watch must remain oneShot"
    );
    let queued_watch = watches
        .iter()
        .find(|w| w.id == queued_sub_id)
        .expect("fresh queued watch present");
    assert!(
        !queued_watch.one_shot,
        "queued watch must be non-oneShot so it survives the current agent:idle"
    );

    manager.release_slot(&target).await;
}

/// SUB-2 (unresolved copilot review thread PRRT_kwDOS9Wxuc6QIRcq on PR #104):
/// reusing a queued watch across repeated `wakeOrCreate` calls must not
/// shorten its effective cleanup deadline. An earlier-spawned cleanup task
/// wakes first but must no-op because the deadline has been extended by a
/// later call; the later task then performs the removal at the new deadline.
#[tokio::test]
async fn spawn_watch_cleanup_extension_defers_removal_to_the_later_deadline() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Target").await;
    let sub_id = svc.register_completion_watch(
        &ws,
        caller.clone(),
        "Coordinator".into(),
        target.clone(),
        false,
        None,
    );
    assert_eq!(svc.list_watches_for_parent(&ws, &caller).len(), 1);

    // Arm a short cleanup, then immediately extend it with a much later
    // deadline before the first timer can fire.
    svc.spawn_watch_cleanup(
        ws.clone(),
        caller.clone(),
        sub_id.clone(),
        Duration::from_millis(80),
    );
    svc.spawn_watch_cleanup(
        ws.clone(),
        caller.clone(),
        sub_id.clone(),
        Duration::from_millis(600),
    );

    // Wait past the original (short) deadline. If the earlier task removed
    // the watch it would be gone here — that is the bug this test guards
    // against.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(
        watches.len(),
        1,
        "earlier cleanup task must not shorten an extended deadline (watches now: {watches:?})"
    );

    // Wait past the extended deadline; the later task must fire and remove.
    let removal_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < removal_deadline {
        if svc.list_watches_for_parent(&ws, &caller).is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("later cleanup task did not remove the watch after the extended deadline");
}

/// SUB-2 (Copilot #104 follow-up, thread PRRT_kwDOS9Wxuc6QKWuM):
/// `spawn_watch_cleanup` must not arm a sleeper task when the deadline
/// bump misses (e.g. the watch was removed concurrently between the reuse
/// find and the cleanup arm). The bump returns `false`, and no task is
/// spawned; the return value from `spawn_watch_cleanup` surfaces that.
#[tokio::test]
async fn spawn_watch_cleanup_skips_when_deadline_bump_misses() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Target").await;
    let sub_id = svc.register_completion_watch(
        &ws,
        caller.clone(),
        "Coordinator".into(),
        target.clone(),
        false,
        None,
    );

    // Stand in for a concurrent removal: drop the watch before the cleanup
    // task would be armed.
    assert!(svc.remove_watch(&ws, &sub_id), "seed removal");
    assert!(svc.list_watches_for_parent(&ws, &caller).is_empty());

    // The arm must observe the missing watch, skip the tokio::spawn, and
    // report `false`. Waiting past the requested delay must not resurrect
    // the empty registry or otherwise mutate state.
    let armed = svc.spawn_watch_cleanup(
        ws.clone(),
        caller.clone(),
        sub_id,
        Duration::from_millis(30),
    );
    assert!(
        !armed,
        "no cleanup task must be spawned when the bump misses"
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        svc.list_watches_for_parent(&ws, &caller).is_empty(),
        "registry stays empty; the skipped cleanup task cannot side-effect it",
    );
}

/// SUB-2 (Copilot #104 follow-up, thread PRRT_kwDOS9Wxuc6QKWuU):
/// when the caller's display name cannot be resolved
/// (`store.get_agent_session` failed), the reuse path must still return
/// the live subscription id — but must NOT overwrite the watch's stored
/// `parent_agent_name` with an empty placeholder. Callers pass `None`
/// through `find_and_refresh_ungrouped_watch` to signal the missing name.
#[tokio::test]
async fn find_and_refresh_ungrouped_watch_preserves_name_when_lookup_fails() {
    let (_t, svc, ws) = setup().await;
    let caller = create_agent(&svc, &ws, "Coordinator").await;
    let target = create_agent(&svc, &ws, "Assignee").await;
    let sub_id = svc.register_completion_watch(
        &ws,
        caller.clone(),
        "Coordinator".into(),
        target.clone(),
        true,
        None,
    );

    // Reuse with no resolved name: still returns the same subscription id
    // (reuse proceeds), and the stored `parent_agent_name` is untouched.
    let reused = svc.find_and_refresh_ungrouped_watch(&ws, &caller, &target, true, None);
    assert_eq!(reused.as_deref(), Some(sub_id.as_str()));
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches.len(), 1);
    assert_eq!(
        watches[0].parent_agent_name, "Coordinator",
        "failed name lookup must NOT overwrite an existing watch's parent_agent_name: {watches:?}"
    );

    // Sanity: a subsequent reuse with a real name still refreshes as
    // before, so the `None` short-circuit is scoped to the lookup-failed
    // case rather than disabling the refresh entirely.
    let reused =
        svc.find_and_refresh_ungrouped_watch(&ws, &caller, &target, true, Some("Renamed".into()));
    assert_eq!(reused.as_deref(), Some(sub_id.as_str()));
    let watches = svc.list_watches_for_parent(&ws, &caller);
    assert_eq!(watches[0].parent_agent_name, "Renamed");
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
    // Post-WSAPI-8: discrete `delegate_task` is replaced by
    // `workspace_api` + `ws.agent.delegate`; the caller-aware immediate
    // watch registration still reaches the same op.
    let resp = server
        .handle_message(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": {
                    "code": "return await ws.agent.delegate({ agentInstructions: 'do work' });",
                    "summary": "immediate delegate registers oneshot watch"
                }
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

/// Text of the child's first (delegated) message, joining every text content
/// block in order. Used for byte-exact assertions on the reference
/// `DelegateTaskTool` preamble.
async fn child_session_first_message_text(svc: &Services, child: &AgentId) -> String {
    let session = svc
        .store()
        .get_agent_session(child)
        .await
        .expect("child session");
    let first = session.messages.first().expect("first message");
    first
        .content
        .as_array()
        .expect("contentBlocks array")
        .iter()
        .filter_map(|b| b.get("text").and_then(|t| t.as_str()).map(str::to_owned))
        .collect::<Vec<_>>()
        .join("")
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
        .agent_get_conversation_op(child.clone(), None, None, None)
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
        .agent_get_conversation_op(child.clone(), None, None, None)
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
        .agent_get_conversation_op(child.clone(), None, None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 1);
    assert!(child_session_messages_json(&svc, &child)
        .await
        .contains("note content body"));
}

/// TASK-C2: `agent.delegate` with a linked task note APPENDS the reference
/// `DelegateTaskTool` preamble ("Your Task Note" + scope contract) after the
/// child's first message with a `---` separator. The task title and note id
/// appear verbatim so the child can self-mark the note complete when done.
#[tokio::test]
async fn delegate_appends_task_note_preamble_to_first_message() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Port frobnicator".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    let input = AgentDelegateInput {
        task_note_id: Some(note.id.clone()),
        agent_instructions: Some("do the work".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let body = child_session_messages_json(&svc, &child).await;
    assert!(
        body.contains("**Your Task Note:**"),
        "preamble marker missing: {body}"
    );
    assert!(
        body.contains("Port frobnicator"),
        "task title missing from preamble: {body}"
    );
    assert!(
        body.contains(note.id.as_str()),
        "task note id missing from preamble: {body}"
    );
    assert!(
        body.contains("**SCOPE: Complete THIS task only.**"),
        "scope contract missing: {body}"
    );
    // The original instructions are preserved above the preamble.
    assert!(
        body.contains("do the work"),
        "explicit instructions must survive the preamble: {body}"
    );
    // Exact first-message bytes: mirrors the reference `DelegateTaskTool`
    // composition `${msg}\n\n---\n${preamble}${commitInstruction}` from
    // `agent-interaction-tools.ts`. With `skipAutoCommit` unset the trailing
    // commit-instruction slot is empty.
    let expected_first_message = format!(
        "do the work\n\
\n\
---\n\
**Your Task Note:** \"Port frobnicator\" (ID: {note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks.",
        note_id = note.id.as_str(),
    );
    let first_message_text = child_session_first_message_text(&svc, &child).await;
    assert_eq!(
        first_message_text, expected_first_message,
        "first message must be byte-exact"
    );
}

/// TASK-C2: when `skipAutoCommit=true` the reference appends the
/// `**Auto-commit is OFF.**` instruction after the scope directive; assert
/// the exact bytes.
#[tokio::test]
async fn delegate_appends_skip_auto_commit_instruction_when_true() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Port frobnicator".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    let input = AgentDelegateInput {
        task_note_id: Some(note.id.clone()),
        agent_instructions: Some("do the work".into()),
        skip_auto_commit: Some(true),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let expected_first_message = format!(
        "do the work\n\
\n\
---\n\
**Your Task Note:** \"Port frobnicator\" (ID: {note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks.\n\
\n\
**Auto-commit is OFF.** Do not commit unless the user explicitly asks. If asked, use `agent_commit_changes` with `userRequested: true`.",
        note_id = note.id.as_str(),
    );
    let first_message_text = child_session_first_message_text(&svc, &child).await;
    assert_eq!(
        first_message_text, expected_first_message,
        "first message must be byte-exact when skipAutoCommit=true"
    );
}

/// TASK-C2: `skipAutoCommit=false` (explicit) matches the default and omits
/// the commit-instruction tail — regression guard so future refactors keep
/// the branch gated correctly.
#[tokio::test]
async fn delegate_omits_skip_auto_commit_instruction_when_false() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Port frobnicator".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    let input = AgentDelegateInput {
        task_note_id: Some(note.id.clone()),
        agent_instructions: Some("do the work".into()),
        skip_auto_commit: Some(false),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let first_message_text = child_session_first_message_text(&svc, &child).await;
    assert!(
        !first_message_text.contains("**Auto-commit is OFF.**"),
        "commit instruction must be omitted when skipAutoCommit=false: {first_message_text}"
    );
    assert!(
        first_message_text.ends_with(
            "**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks."
        ),
        "message must end with the scope directive when skipAutoCommit=false: {first_message_text}"
    );
}

/// TASK-C: delegating with a linked task note but no explicit
/// `agentInstructions` / `taskText` still injects the preamble (the note's
/// body/title fallback slots in above it).
#[tokio::test]
async fn delegate_task_note_only_injects_preamble_below_note_body() {
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

    let body = child_session_messages_json(&svc, &child).await;
    assert!(body.contains("**Your Task Note:**"), "preamble: {body}");
    assert!(body.contains("Task title"), "title: {body}");
    assert!(body.contains(note.id.as_str()), "note id: {body}");
    assert!(body.contains("note content body"), "note body: {body}");
    // Preamble sits BELOW the note body (reference appends after msg).
    let preamble_idx = body.find("**Your Task Note:**").expect("preamble idx");
    let body_idx = body.find("note content body").expect("body idx");
    assert!(
        body_idx < preamble_idx,
        "note body must precede the preamble"
    );
}

/// TASK-C: delegations without a task note deliver the message verbatim —
/// no preamble is injected.
#[tokio::test]
async fn delegate_without_task_note_omits_preamble() {
    let (_t, svc, ws) = setup().await;
    let input = AgentDelegateInput {
        agent_instructions: Some("just do it".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    let body = child_session_messages_json(&svc, &child).await;
    assert!(
        body.contains("just do it"),
        "instructions delivered: {body}"
    );
    assert!(
        !body.contains("**Your Task Note:**"),
        "no preamble without a task note: {body}"
    );
    assert!(
        !body.contains("**SCOPE:"),
        "no scope contract without a task note: {body}"
    );
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
        .agent_get_conversation_op(child, None, None, None)
        .await
        .expect("conv");
    assert_eq!(conv["totalMessages"], 0, "no message delivered");
}

/// NAME-1: delegating with a `taskNoteId` names the child from the resolved
/// task note's title (reference `DelegateTaskTool` taskNoteId path) and leaves
/// `nameExplicitlySet` unset so the child's opening-turn
/// `ws.workspace.setAgentName` (`skipIfExplicitlySet: true`) can still rename
/// it. Without this the child inherits the generic `Agent xxxxxx` fallback
/// that leaks into the waiting panel and `agent:idle` wake reports.
#[tokio::test]
async fn delegate_names_child_from_task_note_title() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Port frobnicator to Rust".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
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
    assert_eq!(resp["name"], "Port frobnicator to Rust");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(child, None).await.expect("get child");
    assert_eq!(got.name, "Port frobnicator to Rust");
    assert!(
        !got.name_explicitly_set,
        "delegated child must stay renameable by the setAgentName opening turn"
    );
}

/// NAME-1: the taskText delegate path names the child from the task text,
/// matching the reference `DelegateTaskTool` taskText branch. `taskText` wins
/// over the linked note's title when both are present.
#[tokio::test]
async fn delegate_names_child_from_task_text() {
    let (_t, svc, ws) = setup().await;
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Parent note title".into(),
                content: Some("body".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    let input = AgentDelegateInput {
        note_id: Some(note.id.clone()),
        task_text: Some("Fix the flaky delegate test".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    assert_eq!(resp["name"], "Fix the flaky delegate test");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let got = svc.agent_get_op(child, None).await.expect("get child");
    assert_eq!(got.name, "Fix the flaky delegate test");
    assert!(!got.name_explicitly_set);
}

/// NAME-1: task-derived names longer than 100 chars are truncated to the
/// first 97 chars + "..." (reference: `taskText.length > 100 ? taskText
/// .substring(0, 97) + '...' : taskText`). Boundary: len == 100 is untouched.
#[tokio::test]
async fn delegate_truncates_long_task_derived_names() {
    let (_t, svc, ws) = setup().await;
    // 150-char task text -> first 97 chars + "..." = 100 chars total.
    let long_text: String = "a".repeat(150);
    let input = AgentDelegateInput {
        task_text: Some(long_text.clone()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let name = resp["name"].as_str().expect("name").to_string();
    assert_eq!(name.chars().count(), 100);
    let expected_prefix: String = "a".repeat(97);
    assert_eq!(name, format!("{expected_prefix}..."));

    // Boundary: exactly 100 chars stays intact.
    let boundary: String = "b".repeat(100);
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                task_text: Some(boundary.clone()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    assert_eq!(resp["name"], boundary);

    // UTF-16 parity with the reference: non-BMP chars (e.g. emoji) count
    // as 2 code units under JS `.length`/`.substring`. 51 emoji = 102
    // UTF-16 units > 100, so the truncated name is 97 UTF-16 units + "..."
    // and never contains a lone surrogate.
    let emoji_text: String = "\u{1F600}".repeat(51);
    let resp = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                task_text: Some(emoji_text),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    let name = resp["name"].as_str().expect("name");
    // 97 UTF-16 units of surrogate-paired emoji = 48 whole emoji (96 units)
    // + one lone high surrogate which we strip -> 48 emoji + "..." total.
    assert_eq!(name, format!("{}...", "\u{1F600}".repeat(48)));
    // Sanity: the string is valid UTF-8 (no U+FFFD replacement chars).
    assert!(!name.contains('\u{FFFD}'));
}

/// NAME-1: because delegate keeps `nameExplicitlySet = false`, a subsequent
/// skip-guarded rename (the FE `ws.workspace.setAgentName` path uses
/// `skipIfExplicitlySet: true`) still applies to the delegated child.
#[tokio::test]
async fn delegate_leaves_child_renameable_by_skip_guarded_rename() {
    let (_t, svc, ws) = setup().await;
    let input = AgentDelegateInput {
        task_text: Some("Initial task-derived name".into()),
        ..Default::default()
    };
    let resp = svc
        .agent_delegate_op(ws.clone(), input, None)
        .await
        .expect("delegate");
    let child = AgentId::from(resp["agentId"].as_str().expect("agentId"));
    let r = svc
        .agent_rename_op(child.clone(), "Chosen by naming instruction".into(), true)
        .await
        .expect("skip-guarded rename");
    assert!(r.get("skipped").is_none(), "rename must not be skipped");
    assert_eq!(r["name"], "Chosen by naming instruction");
    let got = svc.agent_get_op(child, None).await.expect("get");
    assert_eq!(got.name, "Chosen by naming instruction");
    assert!(got.name_explicitly_set);
}

/// NAME-1: an explicit `agent.create` with no name still gets the generic
/// `Agent xxxxxx` fallback (out of delegate scope, unchanged behavior).
#[tokio::test]
async fn create_without_name_keeps_generic_agent_fallback() {
    let (_t, svc, ws) = setup().await;
    let created = svc
        .agent_create_op(
            ws.clone(),
            None,
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create");
    let name = created["agent"]["name"].as_str().expect("name").to_string();
    assert!(
        name.starts_with("Agent ") && name.len() == "Agent ".len() + 6,
        "expected generic fallback name, got {name:?}"
    );
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let got = svc.agent_get_op(id, None).await.expect("get");
    assert!(!got.name_explicitly_set);
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
    let blocks: Vec<_> = session.messages.iter().map(|m| &m.content).collect();
    serde_json::to_string(&blocks).expect("serialize content blocks")
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

/// Watch-set changes emit `agent:subscriptions-changed` carrying the parent's
/// refreshed waiting flags: `true` + the child id on registration (delegate),
/// `false` + empty after the aggregated wake clears the group watches.
#[tokio::test]
async fn watch_set_changes_emit_subscriptions_changed() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_SUBSCRIPTIONS_CHANGED.to_string()],
        ..Default::default()
    });

    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("subscriptions-changed after delegate")
        .expect("batch");
    assert_eq!(batch[0].event_type, AGENT_SUBSCRIPTIONS_CHANGED);
    assert_eq!(batch[0].data["agentId"], json!(parent.0));
    assert_eq!(batch[0].data["isWaitingForOtherAgents"], json!(true));
    assert_eq!(batch[0].data["waitingForAgentIds"], json!([c1.0]));

    // Settle the group: child idles, then the parent idles (seal + fire). The
    // group clear emits the refreshed (now empty) flags.
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c1,
        json!({ "agentId": c1.0 }),
    ))
    .await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("subscriptions-changed after group clear")
        .expect("batch");
    let last = batch.last().expect("event");
    assert_eq!(last.data["agentId"], json!(parent.0));
    assert_eq!(last.data["isWaitingForOtherAgents"], json!(false));
    assert_eq!(last.data["waitingForAgentIds"], json!([]));
}

/// `reportToParent` from a child enrolled in an undelivered after_all group is
/// suppressed: no immediate parent message, the report is still persisted, and
/// it reaches the parent only inside the single aggregated wake (as that
/// child's `Report:` line).
#[tokio::test]
async fn report_to_parent_suppressed_for_after_all_group_child() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;
    let c2 = delegate_after_all(&svc, &ws, &parent).await;

    let r1 = svc
        .agent_report_to_parent_op(ws.clone(), json!("report one"), Some(c1.clone()))
        .await
        .expect("report c1");
    assert_eq!(r1["ok"], json!(true));
    let r2 = svc
        .agent_report_to_parent_op(ws.clone(), json!("report two"), Some(c2.clone()))
        .await
        .expect("report c2");
    assert_eq!(r2["ok"], json!(true));
    // Suppressed: no immediate parent sends for grouped children.
    assert_eq!(parent_message_count(&svc, &parent).await, 0);

    // The reports are still persisted on the child sessions.
    for (c, expected) in [(&c1, "report one"), (&c2, "report two")] {
        let session = svc.store().get_agent_session(c).await.expect("child");
        assert_eq!(session.completion_report.as_deref(), Some(expected));
    }

    // Settle the group: both children idle, then the parent idles (seal+fire).
    for c in [&c1, &c2] {
        svc.handle_completion_event(&completion_event(
            &ws,
            AGENT_IDLE,
            c,
            json!({ "agentId": c.0, "lastResponseSummary": "turn summary" }),
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

    // Exactly one aggregated wake carrying BOTH reports (Report: wins over
    // the event's lastResponseSummary).
    assert_eq!(parent_message_count(&svc, &parent).await, 1);
    let text = parent_messages_text(&svc, &parent).await;
    assert!(text.contains("Report: report one"), "wake text: {text}");
    assert!(text.contains("Report: report two"), "wake text: {text}");
    assert!(!text.contains("turn summary"), "wake text: {text}");
}

/// SUB-2: `reportToParent` is metadata-only after SUB-2, so a late report
/// from a former group child (after the group has fired + removed) still
/// does not push an immediate wake — it just persists the fresh
/// `completion_report`. The wake belongs to the child's next `agent:idle`;
/// with the group + watches already gone there is no watch to fire, matching
/// the reference where `reportToParent` never issues a standalone wake.
#[tokio::test]
async fn report_to_parent_immediate_after_group_delivery() {
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent").await;
    let c1 = delegate_after_all(&svc, &ws, &parent).await;

    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &c1,
        json!({ "agentId": c1.0 }),
    ))
    .await;
    svc.handle_completion_event(&completion_event(
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0 }),
    ))
    .await;
    assert_eq!(parent_message_count(&svc, &parent).await, 1);

    let r = svc
        .agent_report_to_parent_op(ws.clone(), json!("late report"), Some(c1.clone()))
        .await
        .expect("late report");
    assert_eq!(r["ok"], json!(true));
    // Report-time wake: a late reportToParent (after group delivery) still delivers
    // an immediate wake because the child is no longer in an undelivered group.
    // The message count goes from 1 (group wake) to 2 (group wake + late report wake).
    assert_eq!(parent_message_count(&svc, &parent).await, 2);
    let child_session = svc
        .store()
        .get_agent_session(&c1)
        .await
        .expect("child session");
    assert_eq!(
        child_session.completion_report.as_deref(),
        Some("late report")
    );
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
        .agent_get_session_stats_op(
            AgentId::from("agent-00000000-0000-0000-0000-00000missing0"),
            None,
        )
        .await
        .expect_err("unknown session");
    assert!(matches!(err, Error::NotFound(_)));
}

// -- A8: agent.getSession / agent.update / agent.appendMessage / agent.replaceMessages --

/// `agent.getSession` returns the full [`AgentSession`] projection, including
/// the `systemPrompt`/`specialist`/persisted-metadata fields that [`AgentLite`]
/// strips (PROTOCOL §5.5, C1d/C1e). Also round-trips the `messages` log so a
/// `loadAgent` caller does not need a second `agent.getConversation` call.
#[tokio::test]
async fn agent_get_session_projects_full_session_shape() {
    let (_t, svc, ws) = setup().await;
    // Create with a `specialistId` so the session carries a persisted specialist
    // (the projection field `agent.get`/AgentLite strips into `metadata`).
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Full".into()),
            Some("auggie:sonnet4.5".into()),
            Some("implementor".into()),
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create");
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    // Directly set a systemPrompt via the update op so we can then read it back
    // via getSession (systemPrompt is stripped from AgentLite).
    svc.agent_update_op(
        id.clone(),
        json!({ "systemPrompt": "you are a helpful agent" }),
    )
    .await
    .expect("update systemPrompt");
    let session = svc
        .agent_get_session_op(id.clone())
        .await
        .expect("getSession");
    assert_eq!(session.id, id);
    assert_eq!(session.name, "Full");
    assert_eq!(session.specialist.as_deref(), Some("implementor"));
    assert_eq!(
        session.system_prompt.as_deref(),
        Some("you are a helpful agent")
    );
    assert!(session.messages.is_empty());
}

#[tokio::test]
async fn agent_get_session_unknown_agent_is_not_found() {
    let (_t, svc, _ws) = setup().await;
    let err = svc
        .agent_get_session_op(AgentId::from("agent-00000000-0000-0000-0000-00000missing0"))
        .await
        .expect_err("unknown agent");
    assert!(matches!(err, Error::NotFound(_)));
}

/// `agent.update` patches only listed fields; omitted fields survive the write.
/// Emits `agent:updated` with the payload the client sent.
#[tokio::test]
async fn agent_update_patches_listed_fields_and_emits_updated() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Patch").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string()],
        ..Default::default()
    });

    let r = svc
        .agent_update_op(
            id.clone(),
            json!({
                "systemPrompt": "patched",
                "isBackground": true,
                "delegationDepth": 2,
            }),
        )
        .await
        .expect("update");
    assert_eq!(r["success"], json!(true));

    let session = svc
        .agent_get_session_op(id.clone())
        .await
        .expect("getSession");
    assert_eq!(session.system_prompt.as_deref(), Some("patched"));
    assert!(session.is_background);
    assert_eq!(session.delegation_depth, Some(2));
    // Name (unmutated) survives.
    assert_eq!(session.name, "Patch");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    assert!(batch.iter().any(
        |e| e.event_type == AGENT_UPDATED && e.data["agentId"].as_str() == Some(id.0.as_str())
    ));
}

/// Name-only updates fold into `agent:renamed` (not `agent:updated`), matching
/// the existing `agent.rename` semantics.
#[tokio::test]
async fn agent_update_name_only_emits_agent_renamed() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "OldName").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_RENAMED.to_string()],
        ..Default::default()
    });

    svc.agent_update_op(id.clone(), json!({ "name": "NewName" }))
        .await
        .expect("update");

    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(session.name, "NewName");
    assert!(session.name_explicitly_set);

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    assert!(batch.iter().any(|e| e.event_type == AGENT_RENAMED));
}

/// Unknown fields in `changes` surface as `-32602` so callers cannot smuggle
/// stray keys that would silently no-op.
#[tokio::test]
async fn agent_update_rejects_unknown_field() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Strict").await;
    let err = svc
        .agent_update_op(id, json!({ "unknownKey": "x" }))
        .await
        .expect_err("unknown field");
    assert!(matches!(err, Error::InvalidParams(_)));
}

/// The immutable/write-once invariants on `provider`/`acpSessionId` are still
/// enforced by the store; `agent.update` surfaces them verbatim.
#[tokio::test]
async fn agent_update_respects_store_invariants() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Locked").await;
    svc.agent_update_op(id.clone(), json!({ "acpSessionId": "sess-first" }))
        .await
        .expect("first set");
    let err = svc
        .agent_update_op(id, json!({ "acpSessionId": "sess-second" }))
        .await
        .expect_err("write-once");
    assert!(matches!(err, Error::Internal(_)));
}

/// `agent.appendMessage` inserts one row and emits `agent:message`.
#[tokio::test]
async fn agent_append_message_persists_and_emits() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Appender").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_MESSAGE.to_string()],
        ..Default::default()
    });

    let r = svc
        .agent_append_message_op(
            id.clone(),
            "user".into(),
            json!([{ "type": "text", "text": "hello" }]),
            None,
        )
        .await
        .expect("append");
    assert_eq!(r["success"], json!(true));
    assert_eq!(r["message"]["role"], json!("user"));
    assert_eq!(r["message"]["seq"], json!(0));

    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(session.messages.len(), 1);

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    assert!(batch.iter().any(|e| e.event_type == AGENT_MESSAGE));
}

#[tokio::test]
async fn agent_append_message_rejects_bad_role() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "RoleGuard").await;
    let err = svc
        .agent_append_message_op(id, "bogus".into(), json!([]), None)
        .await
        .expect_err("bad role");
    assert!(matches!(err, Error::InvalidParams(_)));
}

/// `agent.replaceMessages` atomically swaps the transcript with fresh
/// `seq: 0..n` values under freshly-minted row ids.
#[tokio::test]
async fn agent_replace_messages_swaps_transcript_atomically() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Swapper").await;
    // Prime with two messages so we can prove the swap replaces rather than
    // appends.
    for i in 0..2 {
        svc.agent_append_message_op(
            id.clone(),
            "user".into(),
            json!([{ "type": "text", "text": format!("old {i}") }]),
            None,
        )
        .await
        .expect("append");
    }

    let r = svc
        .agent_replace_messages_op(
            id.clone(),
            json!([
                { "role": "user", "contentBlocks": [{ "type": "text", "text": "new0" }] },
                { "role": "assistant", "contentBlocks": [{ "type": "text", "text": "new1" }] },
            ]),
        )
        .await
        .expect("replace");
    assert_eq!(r["success"], json!(true));

    let session = svc.agent_get_session_op(id).await.expect("get");
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].seq, 0);
    assert_eq!(session.messages[1].seq, 1);
    assert_eq!(session.messages[0].role, "user");
    assert_eq!(session.messages[1].role, "assistant");
}

#[tokio::test]
async fn agent_replace_messages_rejects_non_array_and_bad_entries() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "ReplaceGuard").await;
    let err = svc
        .agent_replace_messages_op(id.clone(), json!({ "not": "array" }))
        .await
        .expect_err("non-array");
    assert!(matches!(err, Error::InvalidParams(_)));
    let err = svc
        .agent_replace_messages_op(id, json!([{ "role": "user" }]))
        .await
        .expect_err("missing content");
    assert!(matches!(err, Error::InvalidParams(_)));
}

// -- agent.editAndRegenerate service ops (validate + truncate) --

/// Seed a 4-message transcript (user, assistant, user, assistant) and return
/// the persisted message ids in order.
async fn seed_edit_transcript(svc: &Services, id: &AgentId) -> Vec<String> {
    let mut ids = Vec::new();
    for (role, text) in [
        ("user", "first question"),
        ("assistant", "first answer"),
        ("user", "second question"),
        ("assistant", "second answer"),
    ] {
        let r = svc
            .agent_append_message_op(
                id.clone(),
                role.into(),
                json!([{ "type": "text", "text": text }]),
                None,
            )
            .await
            .expect("append");
        ids.push(r["message"]["id"].as_str().unwrap().to_string());
    }
    ids
}

/// `agent_validate_edit_target_op` returns the 0-based index for an existing
/// user message and rejects unknown / non-user ids with `InvalidParams`
/// (→ `-32602` on the wire).
#[tokio::test]
async fn agent_validate_edit_target_accepts_user_rejects_others() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "EditTarget").await;
    let msg_ids = seed_edit_transcript(&svc, &id).await;

    let idx = svc
        .agent_validate_edit_target_op(&id, &msg_ids[2])
        .await
        .expect("valid user target");
    assert_eq!(idx, 2);

    let err = svc
        .agent_validate_edit_target_op(&id, "msg-missing")
        .await
        .expect_err("unknown id");
    assert!(matches!(err, Error::InvalidParams(_)));

    let err = svc
        .agent_validate_edit_target_op(&id, &msg_ids[1])
        .await
        .expect_err("assistant message is not editable");
    assert!(matches!(err, Error::InvalidParams(_)));
}

/// `agent_edit_truncate_op` truncates to just BEFORE the edited user message
/// (dropping it and everything after) and emits `agent:updated` with
/// `{ truncatedCount, remainingCount }`.
#[tokio::test]
async fn agent_edit_truncate_drops_edited_message_and_tail() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "EditTruncate").await;
    let msg_ids = seed_edit_transcript(&svc, &id).await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string()],
        ..Default::default()
    });

    let truncated = svc
        .agent_edit_truncate_op(&id, &msg_ids[2])
        .await
        .expect("truncate");
    assert_eq!(truncated, 2, "edited message + trailing assistant dropped");

    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(session.messages.len(), 2);
    assert_eq!(session.messages[0].role, "user");
    assert_eq!(session.messages[1].role, "assistant");
    assert_eq!(session.messages[0].seq, 0);
    assert_eq!(session.messages[1].seq, 1);
    // Content of the kept prefix survives the swap verbatim.
    assert_eq!(
        session.messages[0].content[0]["text"],
        json!("first question")
    );

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    assert!(batch.iter().any(|e| e.event_type == AGENT_UPDATED
        && e.data["truncatedCount"] == json!(2)
        && e.data["remainingCount"] == json!(2)));
}

/// Truncating at the FIRST user message empties the transcript.
#[tokio::test]
async fn agent_edit_truncate_first_message_empties_transcript() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "EditFirst").await;
    let msg_ids = seed_edit_transcript(&svc, &id).await;

    let truncated = svc
        .agent_edit_truncate_op(&id, &msg_ids[0])
        .await
        .expect("truncate at head");
    assert_eq!(truncated, 4);

    let session = svc.agent_get_session_op(id).await.expect("get");
    assert!(session.messages.is_empty());
}

/// A bad target leaves the transcript untouched (validation happens before
/// any mutation).
#[tokio::test]
async fn agent_edit_truncate_bad_target_mutates_nothing() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "EditGuard").await;
    let msg_ids = seed_edit_transcript(&svc, &id).await;

    let err = svc
        .agent_edit_truncate_op(&id, &msg_ids[3])
        .await
        .expect_err("assistant target");
    assert!(matches!(err, Error::InvalidParams(_)));

    let session = svc.agent_get_session_op(id).await.expect("get");
    assert_eq!(session.messages.len(), 4, "transcript untouched");
}

/// The `WorkspaceApi::agent_edit_and_regenerate` no-manager fallback applies
/// the `model` param (parity with the manager path), truncates, and persists
/// the edited message; a bad target is rejected BEFORE the model switch.
#[tokio::test]
async fn agent_edit_and_regenerate_fallback_applies_model_and_truncates() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "EditFallback").await;
    let msg_ids = seed_edit_transcript(&svc, &id).await;

    let result = svc
        .agent_edit_and_regenerate(
            ws.clone(),
            id.clone(),
            msg_ids[2].clone(),
            "edited via fallback".into(),
            None,
            None,
            Some("mock:other".into()),
        )
        .await
        .expect("fallback edit");
    assert_eq!(result["truncatedCount"], json!(2));

    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(
        session.model.as_deref(),
        Some("mock:other"),
        "model applied"
    );
    assert_eq!(session.messages.len(), 3, "prefix + edited message");
    assert_eq!(session.messages[2].role, "user");
    assert_eq!(
        session.messages[2].content[0]["text"],
        json!("edited via fallback")
    );

    // Bad target: rejected before ANY state change — model untouched.
    let err = svc
        .agent_edit_and_regenerate(
            ws,
            id.clone(),
            "msg-missing".into(),
            "x".into(),
            None,
            None,
            Some("mock:third".into()),
        )
        .await
        .expect_err("unknown target");
    assert!(matches!(err, Error::InvalidParams(_)));
    let session = svc.agent_get_session_op(id).await.expect("get");
    assert_eq!(
        session.model.as_deref(),
        Some("mock:other"),
        "model unchanged by rejected edit"
    );
    assert_eq!(session.messages.len(), 3, "transcript unchanged");
}

// ────────────────────────────────────────────────────────────────────────────
// `agent.wakeOrCreate` widening (C1d-10a) — behaviors B1-B8 + backward compat.
// Each test seeds a task note via `mark_as_task` and drives the widened
// service op directly so it doesn't depend on the runtime `AgentManager`.
// ────────────────────────────────────────────────────────────────────────────

use intent_core::{AgentCreateExtra, AgentWakeCreateOptions, AgentWakeOrCreateInput, NoteId};

async fn seed_task(svc: &Services, ws: &WorkspaceId, title: &str) -> NoteId {
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: title.into(),
                content: Some(format!("{title} body")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "not_started".into(),
        vec![],
        None,
    )
    .await
    .expect("markAsTask");
    note.id
}

fn wake_input(model: Option<&str>) -> AgentWakeOrCreateInput {
    AgentWakeOrCreateInput {
        model: model.map(str::to_string),
        ..Default::default()
    }
}

/// The pre-widening 3-required-params shape (`model` only) still creates and
/// assigns when the task has no prior agent; response carries the widened
/// `action`/`agentName`/`taskTitle` fields and `created: true`.
#[tokio::test]
async fn wake_or_create_backcompat_create_branch_widened_response() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Ship it").await;
    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "go".into(), wake_input(None))
        .await
        .expect("wake");
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["created"], true);
    assert_eq!(resp["action"], "created_new");
    assert_eq!(resp["taskTitle"], "Ship it");
    assert_eq!(resp["agentName"], "Task: Ship it");
    assert!(resp.get("cleanedUpAgentIds").is_none());
}

/// B1: newest-first. When the task has an older assignment plus a newer live
/// one, the newer one is woken (not the oldest) and `created: false`.
#[tokio::test]
async fn wake_or_create_wakes_newest_of_multiple_assignments() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Multi").await;
    // Two live sessions assigned in order: old first, then new.
    let old = svc
        .agent_create_op(
            ws.clone(),
            Some("old".into()),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create old");
    let new = svc
        .agent_create_op(
            ws.clone(),
            Some("new".into()),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create new");
    let old_id = old["agent"]["id"].as_str().unwrap().to_string();
    let new_id = new["agent"]["id"].as_str().unwrap().to_string();
    svc.assign_agent(ws.clone(), note_id.clone(), old_id.clone())
        .await
        .expect("assign old");
    svc.assign_agent(ws.clone(), note_id.clone(), new_id.clone())
        .await
        .expect("assign new");

    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "wake".into(), wake_input(None))
        .await
        .expect("wake");
    assert_eq!(resp["created"], false);
    assert_eq!(resp["agentId"], new_id);
    assert_eq!(resp["agentName"], "new");
    assert_eq!(resp["action"], "woke_existing");
}

/// B2: stale earlier assignment (session gone) is skipped, cleaned up from
/// the task's `assigned_agent_ids`, and reported in `cleanedUpAgentIds`; the
/// older-but-live agent is woken.
#[tokio::test]
async fn wake_or_create_skips_stale_and_reports_cleanup() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Stale").await;
    // A live agent + a stale (deleted) agent assigned later so the reverse
    // iteration hits the stale one first and falls through to the live one.
    let live = svc
        .agent_create_op(
            ws.clone(),
            Some("live".into()),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create live");
    let stale = svc
        .agent_create_op(
            ws.clone(),
            Some("stale".into()),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create stale");
    let live_id = live["agent"]["id"].as_str().unwrap().to_string();
    let stale_id = stale["agent"]["id"].as_str().unwrap().to_string();
    svc.assign_agent(ws.clone(), note_id.clone(), live_id.clone())
        .await
        .expect("assign live");
    svc.assign_agent(ws.clone(), note_id.clone(), stale_id.clone())
        .await
        .expect("assign stale");
    // Wipe the stale session so its assignment becomes NotFound-stale.
    svc.agent_delete_op(AgentId::from(stale_id.as_str()), None)
        .await
        .expect("delete stale");

    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id.clone(), "hi".into(), wake_input(None))
        .await
        .expect("wake");
    assert_eq!(resp["created"], false);
    assert_eq!(resp["agentId"], live_id);
    assert_eq!(
        resp["cleanedUpAgentIds"],
        json!([AgentId::from(stale_id.as_str())])
    );

    // Stale id is stripped from the task's assigned_agent_ids.
    let note = svc.get_note(ws, note_id).await.expect("note");
    let task = note.metadata.task.expect("task");
    assert!(task
        .assigned_agent_ids
        .iter()
        .all(|a| a.as_str() != stale_id));
}

/// B3: delegation-depth guard rejects when the explicit `delegationDepth`
/// meets or exceeds `MAX_DELEGATION_DEPTH` with an `InvalidParams` error.
#[tokio::test]
async fn wake_or_create_depth_guard_rejects_at_cap() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Deep").await;
    let input = AgentWakeOrCreateInput {
        delegation_depth: Some(MAX_DELEGATION_DEPTH),
        ..Default::default()
    };
    let err = svc
        .agent_wake_or_create_op(ws, note_id, "go".into(), input)
        .await
        .expect_err("must reject");
    assert!(
        matches!(err, Error::InvalidParams(ref m) if m.contains("MAX_DELEGATION_DEPTH")),
        "expected InvalidParams MAX_DELEGATION_DEPTH, got {err:?}",
    );
}

/// B3 (compute path): when `delegationDepth` is omitted but `callerAgentId`
/// is provided, the guard reads the caller session's `metadata.delegationDepth`.
#[tokio::test]
async fn wake_or_create_depth_guard_reads_caller_metadata() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Caller").await;
    // Seed a caller session at depth == MAX_DELEGATION_DEPTH so the guard
    // trips through the caller lookup path.
    let caller = svc
        .agent_create_op(
            ws.clone(),
            Some("caller".into()),
            None,
            None,
            None,
            None,
            false,
            AgentCreateExtra {
                metadata: Some(json!({ "delegationDepth": MAX_DELEGATION_DEPTH })),
                ..Default::default()
            },
        )
        .await
        .expect("create caller");
    let caller_id = caller["agent"]["id"].as_str().unwrap().to_string();
    let input = AgentWakeOrCreateInput {
        caller_agent_id: Some(AgentId::from(caller_id.as_str())),
        ..Default::default()
    };
    let err = svc
        .agent_wake_or_create_op(ws, note_id, "go".into(), input)
        .await
        .expect_err("must reject");
    assert!(matches!(err, Error::InvalidParams(_)));
}

/// B4 + B5 + B6: specialist inherits from the newest previous session; the
/// rich create payload (name / contextReferences / metadata / skipAutoCommit)
/// lands on the persisted session row so a child wake can read it back.
#[tokio::test]
async fn wake_or_create_inherits_specialist_and_persists_rich_payload() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Inherit").await;
    // Previous session with a specialist that should be inherited.
    let prev = svc
        .agent_create_op(
            ws.clone(),
            Some("prev".into()),
            Some("gpt-4".into()),
            Some("implementor".into()),
            None,
            None,
            false,
            Default::default(),
        )
        .await
        .expect("create prev");
    let prev_id = prev["agent"]["id"].as_str().unwrap().to_string();
    svc.assign_agent(ws.clone(), note_id.clone(), prev_id.clone())
        .await
        .expect("assign prev");
    // Flip the previous session to `Deleted` (row stays, marked as
    // non-resumable) so wake falls through to the create branch while the
    // inheritance source can still read specialist/model from the row.
    let mut prev_session = svc
        .store()
        .get_agent_session(&AgentId::from(prev_id.as_str()))
        .await
        .expect("load prev");
    prev_session.status = intent_core::AgentStatus::Deleted;
    prev_session.updated_at = intent_core::now_iso();
    svc.store()
        .update_agent_session(&prev_session.workspace_id.clone(), &prev_session)
        .await
        .expect("mark prev deleted");

    let input = AgentWakeOrCreateInput {
        create: Some(AgentWakeCreateOptions {
            name: Some("Explicit Name".into()),
            context_references: Some(json!([{ "type": "note", "id": "note-1" }])),
            metadata: Some(json!({ "custom": "field" })),
            skip_auto_commit: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "go".into(), input)
        .await
        .expect("wake");
    assert_eq!(resp["created"], true);
    assert_eq!(resp["action"], "created_new");
    assert_eq!(resp["agentName"], "Explicit Name");

    // Verify the new session persisted the rich payload.
    let new_id = resp["agentId"].as_str().unwrap();
    let session = svc
        .store()
        .get_agent_session(&AgentId::from(new_id))
        .await
        .expect("load new session");
    assert_eq!(session.name, "Explicit Name");
    assert!(session.skip_auto_commit, "skipAutoCommit honored");
    let md = session.metadata.as_ref().expect("metadata persisted");
    assert_eq!(md["custom"], "field");
    assert_eq!(md["source"], "wake_or_create_task_agent");
    assert_eq!(md["isBackground"], true);
    assert_eq!(md["contextReferences"][0]["id"], "note-1");
    assert_eq!(md["skipAutoCommit"], true);
    // Specialist was inherited from the previous (now-deleted) session.
    assert_eq!(session.specialist.as_deref(), Some("implementor"));
    // Depth defaults to `0` when neither caller nor explicit depth was given.
    assert_eq!(md["delegationDepth"], 0);
    assert!(!md["taskNoteId"].as_str().unwrap().is_empty());
}

/// B7: `messageMetadata` is folded onto the delivered content block on the
/// create branch (and by construction the wake branch shares the same helper).
#[tokio::test]
async fn wake_or_create_delivers_message_metadata_on_block() {
    let (_t, svc, ws) = setup().await;
    let note_id = seed_task(&svc, &ws, "Tag").await;
    let input = AgentWakeOrCreateInput {
        message_metadata: Some(json!({ "type": "task_wake", "source": "wake" })),
        ..Default::default()
    };
    let resp = svc
        .agent_wake_or_create_op(ws, note_id, "hello".into(), input)
        .await
        .expect("wake");
    let new_id = AgentId::from(resp["agentId"].as_str().unwrap());
    let conv = svc
        .agent_get_conversation_op(new_id, None, None, None)
        .await
        .expect("conv");
    // The delivered message is the first user message; its content block
    // carries `messageMetadata` verbatim.
    let msg = &conv["messages"][0];
    assert_eq!(msg["role"], "user");
    let block = &msg["contentBlocks"][0];
    assert_eq!(block["text"], "hello");
    assert_eq!(block["messageMetadata"]["type"], "task_wake");
}

// ────────────────────────────────────────────────────────────────────────────
// DELIV-1 regression: wake / send-to-task delivery must drive a REAL turn
// when the runtime `AgentManager` is attached. Both call sites previously
// persisted the user message store-only (never spawning a worker), so the
// coordinator's follow-up sends silently no-op'd — the "lost sends + empty
// idle wakes" signature. We attach a manager over a hermetic store, drive
// the widened op, and prove the runtime routing by observing the
// `agent:status-changed[active]` event emitted from the runtime's
// `try_begin` slot claim.
// ────────────────────────────────────────────────────────────────────────────

/// Helpers shared by the DELIV-1 regression tests: build a wired
/// (`Services` + attached `AgentManager` + subscription) harness over a
/// hermetic temp DB, and wait for a specific `agent:status-changed` value.
async fn setup_with_manager() -> (
    TempDb,
    Services,
    Arc<crate::agent_manager::AgentManager>,
    EventBus,
    WorkspaceId,
) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn intent_acp::EventSink> = Arc::new(crate::BusEventSink::new(bus.clone()));
    let manager = Arc::new(crate::agent_manager::AgentManager::new(
        services.clone(),
        sink,
        4,
    ));
    services.attach_agent_manager(&manager);
    (tmp, services, manager, bus, ws)
}

/// Subscribe to `agent:status-changed` up front so the check captures the
/// live-only broadcast events emitted during the following op.
fn subscribe_status(bus: &EventBus) -> crate::events::Subscription {
    bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:status-changed".to_string()],
        ..Default::default()
    })
}

async fn expect_status(
    sub: &mut crate::events::Subscription,
    agent_id: &AgentId,
    status: &str,
    within: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, sub.recv()).await {
            Ok(Some(batch)) => {
                for ev in batch {
                    if ev.event_type == "agent:status-changed"
                        && ev.data.get("agentId").and_then(serde_json::Value::as_str)
                            == Some(agent_id.0.as_str())
                        && ev.data.get("status").and_then(serde_json::Value::as_str) == Some(status)
                    {
                        return true;
                    }
                }
            }
            _ => return false,
        }
    }
    false
}

/// DELIV-1: `agent.wakeOrCreate` MUST route through the runtime
/// `AgentManager` when one is attached. The pre-fix store-only path
/// persisted the wake context message without ever triggering a turn —
/// the coordinator's follow-up looked "sent" but no work happened. Proof:
/// the runtime's `try_begin` slot claim emits `agent:status-changed`
/// with `status: "active"`; that event MUST appear on the create branch.
#[tokio::test]
async fn deliv1_wake_or_create_drives_turn_via_runtime() {
    let (_t, svc, manager, bus, ws) = setup_with_manager().await;
    let note_id = seed_task(&svc, &ws, "DELIV-1 wake").await;
    // Subscribe BEFORE the op so we catch the live-only broadcast events.
    let mut sub = subscribe_status(&bus);
    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "kickoff".into(), wake_input(None))
        .await
        .expect("wake");
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["created"], true);
    let agent_id = AgentId::from(resp["agentId"].as_str().expect("agentId"));

    assert!(
        expect_status(&mut sub, &agent_id, "active", Duration::from_secs(3)).await,
        "wakeOrCreate MUST emit agent:status-changed[active] via runtime"
    );

    // Tear the worker down so its background spawn attempt (which errors
    // without a provider available) doesn't outlive the test.
    manager.stop(&agent_id).await;
}

/// DELIV-1: the wake branch (existing live assignment) also drives a turn
/// via the runtime — not just the create branch — so a re-woken agent
/// actually processes the follow-up context message instead of silently
/// storing it. Same evidence: `agent:status-changed[active]` fires on
/// each wake.
#[tokio::test]
async fn deliv1_wake_existing_drives_turn_via_runtime() {
    let (_t, svc, manager, bus, ws) = setup_with_manager().await;
    let note_id = seed_task(&svc, &ws, "DELIV-1 wake-existing").await;

    // First wake creates + assigns; drain the "active" transition from the
    // create branch so the follow-up wake's "active" is unambiguously the
    // one we're testing.
    let mut sub = subscribe_status(&bus);
    let create = svc
        .agent_wake_or_create_op(
            ws.clone(),
            note_id.clone(),
            "kickoff".into(),
            wake_input(None),
        )
        .await
        .expect("create");
    let agent_id = AgentId::from(create["agentId"].as_str().expect("agentId"));
    assert!(
        expect_status(&mut sub, &agent_id, "active", Duration::from_secs(3)).await,
        "create branch active"
    );
    // Let the create-branch worker finish (its ensure_started fails without
    // a provider) before we drive the wake-existing branch.
    manager.stop(&agent_id).await;
    // Drop the old sub so its buffered "runtime_idle" transitions from the
    // stop() call above don't shadow the fresh "active" we're testing next.
    drop(sub);
    let mut sub = subscribe_status(&bus);

    let resp = svc
        .agent_wake_or_create_op(ws.clone(), note_id, "resume".into(), wake_input(None))
        .await
        .expect("wake");
    assert_eq!(resp["created"], false);
    assert_eq!(resp["action"], "woke_existing");
    assert!(
        expect_status(&mut sub, &agent_id, "active", Duration::from_secs(3)).await,
        "wake_existing MUST re-drive a turn via runtime"
    );
    manager.stop(&agent_id).await;
}

/// DELIV-1: `agent.sendToTask` with the default (non-interrupt) priority
/// MUST route through the runtime `AgentManager`. The pre-fix branch
/// called the store-only `agent_send_message_op` unconditionally, so
/// coordinator follow-ups over a task note silently no-op'd. Interrupt
/// priority already routed correctly; this test locks in the default.
#[tokio::test]
async fn deliv1_send_to_task_non_interrupt_drives_turn_via_runtime() {
    let (_t, svc, manager, bus, ws) = setup_with_manager().await;
    let agent_id = create_agent(&svc, &ws, "Follow-up target").await;
    let note_id = seed_task(&svc, &ws, "DELIV-1 send-to-task").await;
    svc.assign_agent(ws.clone(), note_id.clone(), agent_id.0.clone())
        .await
        .expect("assign");

    let mut sub = subscribe_status(&bus);
    let resp = svc
        .agent_send_to_task_op(ws.clone(), note_id, "follow up".into(), None, None)
        .await
        .expect("send_to_task");
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["agentId"], agent_id.0);

    assert!(
        expect_status(&mut sub, &agent_id, "active", Duration::from_secs(3)).await,
        "send_to_task (non-interrupt) MUST drive a turn via runtime"
    );
    manager.stop(&agent_id).await;
}

/// DELIV-1: the wake path preserves the wire contract — the delivered
/// user block still carries `messageMetadata` verbatim so
/// `agent.getConversation` consumers see the FE `task_wake` tag — while
/// ALSO driving a turn via the runtime. Guards against a regression that
/// might trade block-embedded metadata for row-level metadata when
/// routing through `agent_manager.send_message`.
#[tokio::test]
async fn deliv1_wake_or_create_persists_block_metadata_alongside_runtime_drive() {
    let (_t, svc, manager, _bus, ws) = setup_with_manager().await;
    let note_id = seed_task(&svc, &ws, "Tag").await;
    let input = AgentWakeOrCreateInput {
        message_metadata: Some(json!({ "type": "task_wake", "source": "wake" })),
        ..Default::default()
    };
    let resp = svc
        .agent_wake_or_create_op(ws, note_id, "hello".into(), input)
        .await
        .expect("wake");
    let agent_id = AgentId::from(resp["agentId"].as_str().unwrap());
    let conv = svc
        .agent_get_conversation_op(agent_id.clone(), None, None, None)
        .await
        .expect("conv");
    let msg = &conv["messages"][0];
    assert_eq!(msg["role"], "user");
    let block = &msg["contentBlocks"][0];
    assert_eq!(block["text"], "hello");
    assert_eq!(block["messageMetadata"]["type"], "task_wake");
    manager.stop(&agent_id).await;
}

/// STAB-118 regression: SUB-1 delegation-group dedupe.
/// When a coordinator delegates children with `waitMode: after_all` then sends
/// coordination messages (triggering SUB-1 auto-watch), the parent should receive exactly
/// ONE aggregated wake (not individual wakes + aggregated).
///
/// Repro: parent delegates 2 children with after_all, triggers SUB-1 watch registration
/// for each (simulating sendToTask/agent.send), both children complete.
/// Before fix: parent received individual wake for child A, aggregated "All 2 settled"
/// wake, AND duplicate individual wake for child B.
/// After fix: parent receives exactly ONE aggregated wake.
#[tokio::test]
async fn sub1_sendtotask_after_all_no_duplicate_wake() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let _worker = svc.spawn_completion_delivery_loop();
    wait_for_subscriber(&bus).await;
    let parent = create_agent(&svc, &ws, "Parent").await;

    // Delegate 2 children with after_all
    let resp1 = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("child A task".into()),
                wait_mode: Some("after_all".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child A");
    let child_a = AgentId::from(resp1["agentId"].as_str().expect("agentId"));

    let resp2 = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("child B task".into()),
                wait_mode: Some("after_all".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child B");
    let child_b = AgentId::from(resp2["agentId"].as_str().expect("agentId"));

    // Verify delegation group was created with both children
    let subs = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs");
    let groups = subs["delegationGroups"].as_array().expect("groups");
    assert_eq!(groups.len(), 1, "exactly one delegation group");
    let group = &groups[0];
    assert_eq!(group["awaitMode"], "all");
    let expected_ids = group["expectedAgentIds"].as_array().expect("expected");
    assert_eq!(expected_ids.len(), 2, "both children in group");

    // Verify child_in_undelivered_group returns true (the core of the fix)
    assert!(
        svc.child_in_undelivered_group(&ws, &parent, &child_a),
        "child A should be in undelivered group"
    );
    assert!(
        svc.child_in_undelivered_group(&ws, &parent, &child_b),
        "child B should be in undelivered group"
    );

    // Trigger SUB-1 auto-watch path (what sendToTask/agent.send does internally).
    // Before the fix, this would create competing ungrouped watches despite
    // child_in_undelivered_group returning true.
    svc.agent_watch_completion_for_sender_op(ws.clone(), parent.clone(), child_a.clone())
        .await
        .expect("watch child A completion");

    svc.agent_watch_completion_for_sender_op(ws.clone(), parent.clone(), child_b.clone())
        .await
        .expect("watch child B completion");

    // Verify NO ungrouped watches were created (they should have been suppressed by the
    // child_in_undelivered_group check in agent_watch_completion_for_sender_op).
    // Note: grouped watches (with group_id) SHOULD exist from delegation, but ungrouped
    // watches (with group_id=null) should NOT be created.
    let subs_mid = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs mid");
    let all_watches = subs_mid["subscriptions"].as_array().expect("subscriptions");
    let ungrouped_watches: Vec<_> = all_watches
        .iter()
        .filter(|w| w["delegationGroup"].is_null())
        .collect();
    assert_eq!(
        ungrouped_watches.len(),
        0,
        "SUB-1 should NOT create ungrouped watches when children are in undelivered group"
    );

    // Get baseline parent message count before completions
    let baseline = parent_message_count(&svc, &parent).await;

    // Both children complete
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &child_a,
        json!({ "agentId": child_a.0.clone(), "lastResponseSummary": "child A done" }),
    )
    .await;

    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &child_b,
        json!({ "agentId": child_b.0.clone(), "lastResponseSummary": "child B done" }),
    )
    .await;

    // Wait for both children to be recorded in the group before sealing.
    wait_for_group_children(&svc, &ws, &parent, 2).await;

    // Seal the delegation group by publishing parent idle (mimics parent finishing its turn).
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0.clone(), "lastResponseSummary": "coordination done" }),
    )
    .await;

    // CRITICAL ASSERTION: parent should receive exactly ONE wake message
    // (the aggregated group wake), NOT individual wakes for each child.
    // To avoid race conditions, wait specifically for the AGGREGATED wake content
    // to appear in the transcript, then assert count == baseline + 1, and re-check
    // after a short grace period to catch any late duplicate wakes.

    // Wait for the aggregated wake content to appear
    let mut attempts = 0;
    loop {
        let msgs_text = parent_messages_text(&svc, &parent).await;
        if msgs_text.contains("All 2 settled")
            || (msgs_text.contains("child A done") && msgs_text.contains("child B done"))
        {
            break;
        }
        attempts += 1;
        if attempts > 100 {
            panic!("Timeout waiting for aggregated wake content in transcript");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Assert exactly baseline + 1 messages (one aggregated wake)
    let count_after_wake = parent_message_count(&svc, &parent).await;
    assert_eq!(
        count_after_wake,
        baseline + 1,
        "Parent should have exactly 1 aggregated wake after content appears, not {} wakes",
        count_after_wake - baseline
    );

    // Grace period: wait 300ms to catch any late duplicate wakes
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    let final_count = parent_message_count(&svc, &parent).await;
    assert_eq!(
        final_count,
        baseline + 1,
        "Parent should still have exactly 1 wake after grace period, not {} wakes (late duplicates detected)",
        final_count - baseline
    );

    // Verify delegation group was delivered and cleaned up
    let subs_after = svc
        .agent_get_subscriptions(ws.clone(), parent.clone())
        .await
        .expect("subs after");
    let groups_after = subs_after["delegationGroups"]
        .as_array()
        .expect("groups after");
    assert_eq!(
        groups_after.len(),
        0,
        "delegation group should be deleted after delivery"
    );
}

/// Cross-workspace bare-id probes must NOT observe an agent that lives in a
/// different workspace: `agent_get_op` / `agent_get_conversation_op` /
/// `agent_get_queue_op` / `agent_get_session_stats_op` / `agent_delete_op` all
/// return `NotFound` when the caller's declared `workspaceId` does not match
/// the session's owning workspace (defense-in-depth against the
/// "know-the-id-to-mutate/read" attack).
#[tokio::test]
async fn agent_ops_reject_cross_workspace_bare_id_probes() {
    let (_t, svc, ws_a) = setup().await;
    // Provision a second workspace that shares the same store/services handle.
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("second workspace");

    let id = create_agent(&svc, &ws_a, "Owned").await;

    // The `None` workspace guard preserves the legacy behavior (all internal
    // callers still see the session).
    svc.agent_get_op(id.clone(), None)
        .await
        .expect("owner read");

    // A caller declaring ws_b sees NotFound rather than the ws_a row.
    let err = svc
        .agent_get_op(id.clone(), Some(ws_b.clone()))
        .await
        .expect_err("cross-ws get must not observe");
    assert!(matches!(err, Error::NotFound(_)), "get: {err:?}");

    let err = svc
        .agent_get_conversation_op(id.clone(), None, Some(ws_b.clone()), None)
        .await
        .expect_err("cross-ws conversation must not observe");
    assert!(matches!(err, Error::NotFound(_)), "conversation: {err:?}");

    let err = svc
        .agent_get_queue_op(id.clone(), Some(ws_b.clone()))
        .await
        .expect_err("cross-ws queue must not observe");
    assert!(matches!(err, Error::NotFound(_)), "queue: {err:?}");

    let err = svc
        .agent_get_session_stats_op(id.clone(), Some(ws_b.clone()))
        .await
        .expect_err("cross-ws stats must not observe");
    assert!(matches!(err, Error::NotFound(_)), "stats: {err:?}");

    // Delete: a cross-workspace probe must not remove the row.
    let err = svc
        .agent_delete_op(id.clone(), Some(ws_b.clone()))
        .await
        .expect_err("cross-ws delete must not observe");
    assert!(matches!(err, Error::NotFound(_)), "delete: {err:?}");

    // The row is still there for the owning workspace.
    svc.agent_get_op(id.clone(), Some(ws_a.clone()))
        .await
        .expect("owner still reads after failed cross-ws delete");
}

/// Store-layer defense-in-depth: even if an op-layer guard were bypassed, the
/// UPDATE/DELETE queries scope by `(id, workspace_id)` so a mutation issued
/// with the wrong workspace_id affects zero rows and surfaces `NotFound`.
#[tokio::test]
async fn agent_store_mutations_reject_cross_workspace_writes() {
    let (_t, svc, ws_a) = setup().await;
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("second workspace");

    let id = create_agent(&svc, &ws_a, "Owned").await;
    let mut session = svc
        .store()
        .get_agent_session(&id)
        .await
        .expect("owner session");
    session.name = "Renamed".to_string();
    session.updated_at = now_iso();

    // Wrong workspace → NotFound; the row is unchanged.
    let err = svc
        .store()
        .update_agent_session(&ws_b, &session)
        .await
        .expect_err("cross-ws update must not mutate");
    assert!(matches!(err, Error::NotFound(_)), "update: {err:?}");
    let reread = svc
        .store()
        .get_agent_session(&id)
        .await
        .expect("still there");
    assert_ne!(reread.name, "Renamed");

    let err = svc
        .store()
        .set_agent_session_status(
            &ws_b,
            &id,
            intent_core::AgentStatus::RuntimeIdle,
            false,
            &now_iso(),
            None,
        )
        .await
        .expect_err("cross-ws status write must not mutate");
    assert!(matches!(err, Error::NotFound(_)), "status: {err:?}");

    let err = svc
        .store()
        .set_acp_session_id(&ws_b, &id, "acp-x")
        .await
        .expect_err("cross-ws acp write must not mutate");
    assert!(matches!(err, Error::NotFound(_)), "acp: {err:?}");

    let removed = svc
        .store()
        .delete_agent_session(&ws_b, &id)
        .await
        .expect("delete returns bool");
    assert!(!removed, "cross-ws delete must remove zero rows");
    svc.store()
        .get_agent_session(&id)
        .await
        .expect("row still present after cross-ws delete");
}

/// `workspace.delete` must sweep every live in-memory agent registry keyed
/// off the workspace BEFORE the store cascade drops the session rows: live-
/// turn slots, pending message queues, and completion watches (both keys
/// under the workspace's `WorkspaceWatches` entry). One `agent:deleted` fires
/// per session ahead of the terminal `workspace:deleted`, so a same-slug
/// recreate observes zero ghost agents and no residual event traffic.
#[tokio::test]
async fn delete_workspace_terminates_agent_sessions_and_clears_in_memory_state() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    // The delete path walks `workspaces_root` to unlink the daemon-owned
    // workspace dir; pin a hermetic tempdir so it never falls through to the
    // real user home. The dir need not exist — `remove_dir_all` swallows
    // `NotFound`.
    let svc = svc.with_workspaces_root(_t.path.with_extension("workspaces"));
    let a = create_agent(&svc, &ws, "Alpha").await;
    let b = create_agent(&svc, &ws, "Beta").await;
    let c = create_agent(&svc, &ws, "Gamma").await;

    // Seed a completion watch (Alpha → Beta), a live-turn slot for Alpha,
    // and a queued message for Gamma — the three in-memory registries the
    // delete path must sweep.
    svc.register_completion_watch(&ws, a.clone(), "Alpha".into(), b.clone(), true, None);
    svc.set_live_turn(
        &a,
        "msg-live",
        vec![json!({ "type": "text", "text": "streaming…" })],
    );
    svc.enqueue_message(&c, "queued follow-up".to_string(), None, None, None);
    assert!(svc.live_turn(&a).is_some(), "live-turn slot seeded");
    assert!(svc.has_ready_to_send(&c), "queue seeded");
    assert_eq!(svc.find_watches_for_child(&ws, &b).len(), 1);

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![
            AGENT_DELETED.to_string(),
            intent_core::events::WORKSPACE_DELETED.to_string(),
        ],
        ..Default::default()
    });

    <Services as WorkspaceApi>::delete_workspace(&svc, ws.clone())
        .await
        .expect("delete workspace");

    // In-memory state is swept: no live-turn slot, no queued messages, no
    // completion-watch entries left for the workspace.
    assert!(svc.live_turn(&a).is_none(), "live-turn cleared on delete");
    assert!(!svc.has_ready_to_send(&c), "queue cleared on delete");
    assert!(svc.find_watches_for_child(&ws, &b).is_empty());
    assert!(svc.all_watches(&ws).is_empty());

    // Store rows are gone — the cascade ran after the live-state sweep.
    for id in [&a, &b, &c] {
        let err = svc.store().get_agent_session(id).await.expect_err("gone");
        assert!(matches!(err, Error::NotFound(_)), "{id}: {err:?}");
    }

    // Collect one full event window (batch may fan out across recvs).
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut deleted_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut saw_workspace_deleted = false;
    while std::time::Instant::now() < deadline && (deleted_ids.len() < 3 || !saw_workspace_deleted)
    {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let Ok(Some(batch)) = timeout(remaining, sub.recv()).await else {
            break;
        };
        for ev in batch {
            assert_eq!(ev.workspace_id, ws);
            match ev.event_type.as_str() {
                AGENT_DELETED => {
                    let id = ev.data["agentId"].as_str().unwrap().to_string();
                    deleted_ids.insert(id);
                }
                t if t == intent_core::events::WORKSPACE_DELETED => {
                    saw_workspace_deleted = true;
                    // The workspace event must arrive AFTER the per-agent
                    // events — subscribers see the tear-down first.
                    assert_eq!(deleted_ids.len(), 3, "workspace:deleted before all agents");
                }
                other => panic!("unexpected event type: {other}"),
            }
        }
    }
    assert!(saw_workspace_deleted, "workspace:deleted must fire");
    let expected: std::collections::HashSet<String> =
        [&a, &b, &c].into_iter().map(|id| id.0.clone()).collect();
    assert_eq!(deleted_ids, expected, "one agent:deleted per session");
}

/// When a delegated agent starts a new turn after persisting a completion
/// report, the store clears `completion_report` + `completion_report_timestamp`
/// and returns `true`. A subsequent `agent.get` shows no report in metadata.
/// When no report is set, the clear returns `false` (no-op, no write).
#[tokio::test]
async fn clear_completion_report_on_turn_begin() {
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
            Default::default(),
        )
        .await
        .expect("create child");
    let child = AgentId::from(created["agent"]["id"].as_str().unwrap());

    // No report initially — clear returns false.
    let ts = now_iso();
    let cleared = svc
        .store()
        .clear_completion_report(&ws, &child, &ts)
        .await
        .expect("clear when none");
    assert!(!cleared, "no report to clear initially");

    // Set a report.
    svc.agent_report_to_parent_op(ws.clone(), json!("shipped it"), Some(child.clone()))
        .await
        .expect("report");
    let before = svc.agent_get_op(child.clone(), None).await.expect("get");
    let v = serde_json::to_value(&before).expect("lite json");
    assert_eq!(v["metadata"]["completionReport"], "shipped it");

    // Clear the report (simulates the turn-begin hook).
    let ts2 = now_iso();
    let cleared = svc
        .store()
        .clear_completion_report(&ws, &child, &ts2)
        .await
        .expect("clear when set");
    assert!(cleared, "report was present and cleared");

    // The report is now absent from metadata.
    let after = svc
        .agent_get_op(child.clone(), None)
        .await
        .expect("get after clear");
    let v = serde_json::to_value(&after).expect("lite json");
    assert!(v["metadata"]["completionReport"].is_null());
    assert!(v["metadata"]["completionReportTimestamp"].is_null());

    // Second clear returns false (no report to clear).
    let ts3 = now_iso();
    let cleared = svc
        .store()
        .clear_completion_report(&ws, &child, &ts3)
        .await
        .expect("second clear");
    assert!(!cleared, "no report on second clear");
}

/// `agent_send_message_op` (store-only fallback when no AgentManager is attached)
/// emits `agent:message` with the persisted row's id.
#[tokio::test]
async fn agent_send_message_emits_agent_message_event() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Sender").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_MESSAGE.to_string()],
        ..Default::default()
    });

    let r = svc
        .agent_send_message_op(id.clone(), "hello".into(), None, None, None, None)
        .await
        .expect("send");
    assert_eq!(r["success"], json!(true));
    assert_eq!(r["queued"], json!(false));
    let response_message_id = r["messageId"].as_str().unwrap();

    // Verify the event was published with the correct messageId.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    let event = batch
        .iter()
        .find(|e| e.event_type == AGENT_MESSAGE)
        .expect("agent:message event");
    assert_eq!(event.data["agentId"], json!(id.0));
    assert_eq!(event.data["role"], json!("user"));
    let event_message_id = event.data["messageId"].as_str().unwrap();
    assert_eq!(event_message_id, response_message_id);

    // Verify the messageId matches the persisted row.
    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].id, event_message_id);
}

/// `agent_force_message_op` (store-only fallback when no AgentManager is attached)
/// emits `agent:message` with the persisted row's id.
#[tokio::test]
async fn agent_force_message_emits_agent_message_event() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Forcer").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_MESSAGE.to_string()],
        ..Default::default()
    });

    let r = svc
        .agent_force_message_op(
            id.clone(),
            "msg-123".into(),
            "forced content".into(),
            None,
            None,
        )
        .await
        .expect("force");
    assert_eq!(r["success"], json!(true));
    let response_message_id = r["messageId"].as_str().unwrap();

    // Verify the event was published with the correct messageId.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv")
        .expect("open");
    let event = batch
        .iter()
        .find(|e| e.event_type == AGENT_MESSAGE)
        .expect("agent:message event");
    assert_eq!(event.data["agentId"], json!(id.0));
    assert_eq!(event.data["role"], json!("user"));
    let event_message_id = event.data["messageId"].as_str().unwrap();
    assert_eq!(event_message_id, response_message_id);

    // Verify the messageId matches the persisted row.
    let session = svc.agent_get_session_op(id.clone()).await.expect("get");
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.messages[0].id, event_message_id);
}

/// STAB-112: `persist_error_and_requeue` must surface the `requeuedAfterFailure`
/// marker in `queue_snapshot` and `agent:queue:updated` payloads so the FE can
/// distinguish terminal-failure requeues from normal queued messages.
#[tokio::test]
async fn requeued_after_failure_marker_surfaces_in_queue_snapshot() {
    use crate::agent_ops::{new_message_id, QueuedMessage};

    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "RQF").await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_QUEUE_UPDATED.to_string()],
        ..Default::default()
    });

    // Simulate a terminal-failure requeue by directly calling requeue_front with
    // persisted=true (matching persist_error_and_requeue's behavior).
    let queued = QueuedMessage {
        id: new_message_id(),
        content: "failed message".to_string(),
        image_blocks: None,
        file_blocks: None,
        queued_at: now_iso(),
        editing: false,
        persisted: true,
        requeued_after_failure: true, // Terminal-failure requeue marker
        message_metadata: None,
    };

    svc.requeue_front(&id, queued);
    svc.publish_queue_updated(&id).await;

    // Verify queue_snapshot includes requeuedAfterFailure marker
    let snapshot = svc.queue_snapshot(&id);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["content"], "failed message");
    assert_eq!(
        snapshot[0]["requeuedAfterFailure"], true,
        "marker must be present"
    );

    // Verify agent:queue:updated event carries the marker
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated event");
    assert_eq!(evt.data["queue"].as_array().unwrap().len(), 1);
    assert_eq!(evt.data["queue"][0]["requeuedAfterFailure"], true);
}

/// `messageMetadata` captured at enqueue time (e.g. a parent wake's
/// `event_notification` payload) must surface on the queue wire shape via
/// `QueuedMessage::to_value`, and entries enqueued without metadata must keep
/// the legacy shape (no `messageMetadata` key).
#[tokio::test]
async fn queued_message_metadata_surfaces_in_queue_snapshot() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "QMM").await;

    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::AGENT_QUEUE_UPDATED.to_string()],
        ..Default::default()
    });

    let metadata = json!({
        "type": "event_notification",
        "eventType": "task_completion",
        "taskNoteId": "note-1",
    });
    let (queued, position) = svc.enqueue_message(
        &id,
        "wake while busy".to_string(),
        None,
        None,
        Some(metadata.clone()),
    );
    assert_eq!(queued.to_value(position)["messageMetadata"], metadata);
    svc.publish_queue_updated(&id).await;

    // Wire shape: metadata present on the tagged entry.
    let snapshot = svc.queue_snapshot(&id);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["content"], "wake while busy");
    assert_eq!(snapshot[0]["messageMetadata"], metadata);

    // agent:queue:updated event carries the same shape.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription closed");
    let evt = batch
        .iter()
        .find(|e| e.event_type == intent_core::events::AGENT_QUEUE_UPDATED)
        .expect("queue:updated event");
    assert_eq!(evt.data["queue"][0]["messageMetadata"], metadata);

    // Legacy shape: an entry enqueued without metadata omits the key.
    let (plain, plain_pos) = svc.enqueue_message(&id, "plain".to_string(), None, None, None);
    let v = plain.to_value(plain_pos);
    assert!(
        v.get("messageMetadata").is_none(),
        "no messageMetadata key without metadata"
    );
}

/// STAB-129 regression: a delegation group settling with a failed (not
/// deleted) child must not leave the parent with zero wake paths for that
/// child. Observed 2026-07-20: a grouped child hit the `session/prompt` idle
/// timeout mid-turn (`agent:failed`) while its underlying work was still
/// running; the group settled, group-watch removal dropped every parent
/// watch, and the child's eventual real completion (after a resume) never
/// woke the parent.
///
/// After the fix, `settle_group_watches` ensures each failed-not-deleted
/// member keeps exactly one ungrouped wake path at settlement time (before
/// the wake delivery await): the grouped watch is converted into an ungrouped
/// oneShot watch, unless a live ungrouped watch for the pair already exists,
/// in which case the grouped watch is simply dropped. Either way the child's
/// later settlement still wakes the parent.
#[tokio::test]
async fn group_settle_with_failed_child_reestablishes_parent_watch() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let _worker = svc.spawn_completion_delivery_loop();
    wait_for_subscriber(&bus).await;
    let parent = create_agent(&svc, &ws, "Parent").await;

    // Delegate 2 children with after_all.
    let resp1 = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("child A task".into()),
                wait_mode: Some("after_all".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child A");
    let child_a = AgentId::from(resp1["agentId"].as_str().expect("agentId"));
    let resp2 = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                agent_instructions: Some("child B task".into()),
                wait_mode: Some("after_all".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
        .expect("delegate child B");
    let child_b = AgentId::from(resp2["agentId"].as_str().expect("agentId"));

    let baseline = parent_message_count(&svc, &parent).await;

    // Child A completes normally; child B "fails" via the prompt idle timeout
    // (the exact error shape run_prompt_turn publishes on a timed-out turn).
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &child_a,
        json!({ "agentId": child_a.0.clone(), "lastResponseSummary": "child A done" }),
    )
    .await;
    publish_completion(
        &bus,
        &ws,
        AGENT_FAILED,
        &child_b,
        json!({
            "agentId": child_b.0.clone(),
            "error": "session/prompt failed: request `session/prompt idle timeout (1800s of silence)` timed out",
        }),
    )
    .await;
    wait_for_group_children(&svc, &ws, &parent, 2).await;

    // Seal the group by publishing parent idle (its delegating turn ended).
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &parent,
        json!({ "agentId": parent.0.clone(), "lastResponseSummary": "coordination done" }),
    )
    .await;

    // The group settles: exactly one aggregated wake reaches the parent.
    wait_for_message_count(&svc, &parent, baseline + 1).await;
    let msgs = parent_messages_text(&svc, &parent).await;
    assert!(
        msgs.contains("All 2 delegated child agent(s) settled"),
        "aggregated wake expected, got: {msgs}"
    );

    // REGRESSION ASSERTION: settlement must leave the parent an ungrouped
    // oneShot watch on the failed child (and none on the completed one), so
    // the failed-but-possibly-still-working child's later settlement wakes it.
    // try_fire_group swaps the watches before the wake-delivery await, but the
    // transcript write we synchronized on above is a separate async step, so
    // poll until the registry reaches its settled state.
    let watches = timeout(Duration::from_secs(2), async {
        loop {
            let watches = svc.list_watches_for_parent(&ws, &parent);
            let settled = watches.iter().all(|w| w.group_id.is_none())
                && watches.iter().any(|w| w.child_agent_id == child_b);
            if settled {
                return watches;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("failed-child watch re-established after group settlement");
    assert!(
        !watches.iter().any(|w| w.child_agent_id == child_a),
        "no watch should be retained for the successfully completed child"
    );
    let retained: Vec<_> = watches
        .iter()
        .filter(|w| w.child_agent_id == child_b)
        .collect();
    assert_eq!(
        retained.len(),
        1,
        "exactly one watch retained for the failed child, got: {watches:?}"
    );
    assert!(retained[0].one_shot, "retained watch must be oneShot");
    assert!(
        retained[0].group_id.is_none(),
        "retained watch must be ungrouped (the group is gone)"
    );

    // The failed child later genuinely completes (e.g. resumed via sendToTask):
    // its agent:idle must wake the parent again through the retained watch.
    publish_completion(
        &bus,
        &ws,
        AGENT_IDLE,
        &child_b,
        json!({ "agentId": child_b.0.clone(), "lastResponseSummary": "child B really done" }),
    )
    .await;
    wait_for_message_count(&svc, &parent, baseline + 2).await;
    let msgs = parent_messages_text(&svc, &parent).await;
    assert!(
        msgs.contains("child B really done"),
        "late completion wake expected, got: {msgs}"
    );

    // The oneShot watch is consumed by the delivery.
    let watches_after = svc.list_watches_for_parent(&ws, &parent);
    assert!(
        !watches_after.iter().any(|w| w.child_agent_id == child_b),
        "oneShot watch removed after the late completion delivered"
    );
}

// ── Durable queue: write-through persistence + startup rehydration ─────────

/// Load the persisted `agent_queue` snapshot for one agent, ordered by position.
async fn persisted_queue(svc: &Services, agent: &AgentId) -> Vec<serde_json::Value> {
    svc.store()
        .load_all_agent_queues()
        .await
        .expect("load agent queues")
        .into_iter()
        .filter(|r| r.agent_id == *agent)
        .map(|r| r.payload)
        .collect()
}

#[tokio::test]
async fn queue_mutations_write_through_to_store() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Durable").await;

    // Enqueue two messages → both persisted, in order, with attachments.
    let first = svc
        .agent_queue_message_op(
            id.clone(),
            "first".into(),
            Some(json!([{ "type": "image", "data": "abc" }])),
            None,
        )
        .await
        .expect("queue first");
    let first_id = first["queuedMessage"]["id"].as_str().unwrap().to_string();
    svc.agent_queue_message_op(id.clone(), "second".into(), None, None)
        .await
        .expect("queue second");
    let rows = persisted_queue(&svc, &id).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["content"], "first");
    assert_eq!(rows[0]["imageBlocks"][0]["data"], "abc");
    assert_eq!(rows[1]["content"], "second");

    // Edit (content + editing flag) → persisted snapshot reflects both.
    svc.agent_edit_queued_message_op(id.clone(), first_id.clone(), "edited".into(), Some(true))
        .await
        .expect("edit");
    let rows = persisted_queue(&svc, &id).await;
    assert_eq!(rows[0]["content"], "edited");
    assert_eq!(rows[0]["editing"], json!(true));

    // Remove → persisted snapshot shrinks with it.
    svc.agent_remove_queued_message_op(id.clone(), first_id)
        .await
        .expect("remove");
    let rows = persisted_queue(&svc, &id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["content"], "second");

    // Dequeue (the drain-side mutation) followed by the publish that every
    // drain site performs → persisted snapshot empties.
    let next = svc.dequeue_message(&id).expect("dequeue");
    assert_eq!(next.content, "second");
    svc.publish_queue_updated(&id).await;
    assert!(persisted_queue(&svc, &id).await.is_empty());
}

#[tokio::test]
async fn clear_queue_write_through_empties_persisted_snapshot() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Cleared").await;
    svc.agent_queue_message_op(id.clone(), "doomed".into(), None, None)
        .await
        .expect("queue");
    assert_eq!(persisted_queue(&svc, &id).await.len(), 1);

    // `force_message` clears then publishes through the same choke point.
    assert!(svc.clear_queue(&id));
    svc.publish_queue_updated(&id).await;
    assert!(persisted_queue(&svc, &id).await.is_empty());
}

#[tokio::test]
async fn rehydrate_restores_queue_resets_editing_and_keeps_flags() {
    let (tmp, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Restored").await;
    // Seed persisted rows the way a pre-shutdown daemon would have left them:
    // entry 0 mid-edit, entry 1 a persisted interrupt-requeue with metadata.
    svc.store()
        .replace_agent_queue(
            &id,
            &[
                intent_store::AgentQueueRow {
                    id: "q-0".into(),
                    agent_id: id.clone(),
                    position: 0,
                    payload: json!({
                        "id": "q-0",
                        "content": "was editing",
                        "queuedAt": now_iso(),
                        "editing": true,
                    }),
                    created_at: now_iso(),
                },
                intent_store::AgentQueueRow {
                    id: "q-1".into(),
                    agent_id: id.clone(),
                    position: 1,
                    payload: json!({
                        "id": "q-1",
                        "content": "requeued",
                        "queuedAt": now_iso(),
                        "editing": false,
                        "persisted": true,
                        "requeuedAfterFailure": true,
                        "messageMetadata": { "source": "event_notification" },
                    }),
                    created_at: now_iso(),
                },
            ],
        )
        .await
        .expect("seed persisted queue");

    // Fresh Services over the same store = a daemon restart (empty in-memory map).
    let store = Store::open(&tmp.path).await.expect("reopen store");
    let restarted = Services::new(store);
    let rehydrated = restarted.rehydrate_agent_queues().await.expect("rehydrate");
    assert_eq!(rehydrated, 2);

    // agent.getQueue sees both entries in original order; the mid-edit entry
    // came back ready-to-send (no `editing` on the wire).
    let q = restarted
        .agent_get_queue_op(id.clone(), None)
        .await
        .expect("getQueue");
    let queue = q["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 2);
    assert_eq!(queue[0]["content"], "was editing");
    assert!(queue[0].get("editing").is_none());
    assert_eq!(queue[1]["content"], "requeued");
    assert_eq!(queue[1]["requeuedAfterFailure"], json!(true));
    assert_eq!(queue[1]["messageMetadata"]["source"], "event_notification");

    // Internal flags round-trip: editing reset makes q-0 dequeuable first;
    // q-1 keeps `persisted` so a drain will not double-append the transcript row.
    let first = restarted.dequeue_message(&id).expect("dequeue q-0");
    assert_eq!(first.id, "q-0");
    assert!(!first.editing);
    assert!(!first.persisted);
    let second = restarted.dequeue_message(&id).expect("dequeue q-1");
    assert_eq!(second.id, "q-1");
    assert!(second.persisted);
    assert!(second.requeued_after_failure);
    assert!(restarted.dequeue_message(&id).is_none());
}

#[tokio::test]
async fn rehydrate_preserves_live_map() {
    let (tmp, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Live").await;
    svc.agent_queue_message_op(id.clone(), "persisted".into(), None, None)
        .await
        .expect("queue");

    // Rehydrating over a Services that already holds a live queue for the
    // agent keeps the live (newer) queue rather than clobbering it.
    let store = Store::open(&tmp.path).await.expect("reopen store");
    let restarted = Services::new(store);
    restarted
        .agent_queue_message_op(id.clone(), "live".into(), None, None)
        .await
        .expect("live queue");
    // The live enqueue's write-through replaced the persisted snapshot, so
    // rehydration loads that same single entry — the vacant-entry insert
    // leaves the in-memory queue untouched and counts nothing.
    let rehydrated = restarted.rehydrate_agent_queues().await.expect("rehydrate");
    assert_eq!(rehydrated, 0, "skipped live queue must not be counted");
    let q = restarted
        .agent_get_queue_op(id, None)
        .await
        .expect("getQueue");
    let queue = q["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0]["content"], "live");
}

/// Resume appends the system interruption marker before the continuation, and
/// the append is idempotent on retry: when a prior resume attempt already left
/// the marker as the transcript tail (continuation delivery failed, row reset
/// to pending), a second resume must not append a duplicate marker.
#[tokio::test]
async fn resume_interrupted_marker_is_idempotent_on_retry() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "Interrupted").await;
    let marker_content = json!([{
        "type": "text",
        "text": "The previous turn was interrupted because the harness shut down. Continuing below.",
        "meta": { "kind": "interruption" }
    }]);

    // First resume: appends marker + continuation.
    svc.store
        .insert_interrupted_agent(&id, &ws, "active", &now_iso())
        .await
        .expect("insert interrupted row");
    svc.resume_interrupted_agent(&id).await.expect("resume 1");
    let messages = svc.store.get_agent_messages(&id, None).await.expect("msgs");
    let markers: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, m)| m.role == "system" && m.content == marker_content)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(markers.len(), 1, "first resume appends exactly one marker");
    let continuation_idx = messages
        .iter()
        .rposition(|m| m.role == "user")
        .expect("continuation user message");
    assert!(
        markers[0] < continuation_idx,
        "marker precedes the continuation"
    );

    // Simulate a retry after a failed continuation delivery: a second agent
    // whose transcript tail is already the marker (the prior attempt appended
    // it, then the continuation failed and the row was reset to pending). The
    // resume must skip the duplicate marker append.
    let retry = create_agent(&svc, &ws, "Retry").await;
    svc.store
        .append_agent_message(&retry, "system", &marker_content, &now_iso())
        .await
        .expect("pre-append marker (prior failed attempt)");
    svc.store
        .insert_interrupted_agent(&retry, &ws, "active", &now_iso())
        .await
        .expect("insert interrupted row");
    svc.resume_interrupted_agent(&retry)
        .await
        .expect("resume retry");
    let messages = svc
        .store
        .get_agent_messages(&retry, None)
        .await
        .expect("msgs");
    let marker_count = messages
        .iter()
        .filter(|m| m.role == "system" && m.content == marker_content)
        .count();
    assert_eq!(marker_count, 1, "retry must not duplicate the marker");
    assert!(
        messages.iter().any(|m| m.role == "user"),
        "retry still delivers the continuation"
    );
}
