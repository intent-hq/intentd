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
            Default::default(),
        )
        .await
        .expect("create honors client id");
    assert_eq!(created["agent"]["id"].as_str(), Some(requested.0.as_str()));
    // Round-trip through the store proves the session is addressable at the
    // client-supplied id.
    let got = svc
        .agent_get_op(requested.clone(), None)
        .await
        .expect("get");
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
                Default::default(),
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
        None,
        intent_core::AgentCreateExtra::default(),
    )
    .await
    .expect("create under a shallow parent succeeds");
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
        .agent_send_message_op(id.clone(), "do it".into(), Some("m1".into()))
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
            Default::default(),
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
                "name": "delegate_task",
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
/// reports back via `report_to_parent` (caller-aware; the registry/dispatch name
/// is bare — agents still see `report_to_parent_workspace-mcp` because the
/// provider appends the server suffix) and the
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
                "name": "delegate_task",
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
                "name": "report_to_parent",
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
                "name": "report_to_parent",
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
                "name": "delegate_task",
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
            None,
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

/// After the group has fired (delivered + removed), a late `reportToParent`
/// from a former group child is no longer suppressed and delivers immediately.
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
        .agent_report_to_parent_op(ws.clone(), json!("late report"), Some(c1))
        .await
        .expect("late report");
    assert_eq!(r["ok"], json!(true));
    assert_eq!(parent_message_count(&svc, &parent).await, 2);
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
    let task = note.task.expect("task");
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
            None,
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
            None,
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
