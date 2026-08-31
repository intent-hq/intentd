//! E2E coverage follow-up part 3 — `agent_ops` services (PR C).
//!
//! Hermetic Services-level tests exercising `agent_ops` service methods. Tests cover:
//! agent.subscribe, agent.diagnostics, agent.status, agent.list, agent.readConversation,
//! agent.summary via in-process Services pattern (not spawned processes).
//! All tests assert concrete response contracts unconditionally.

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E Agent Bindings".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: path.as_ref().map(|p| p.to_string_lossy().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: path.map(|p| p.to_string_lossy().to_string()),
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
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

fn cleanup_db(db: &PathBuf) {
    std::fs::remove_file(db).ok();
    std::fs::remove_file(db.with_extension("db-wal")).ok();
    std::fs::remove_file(db.with_extension("db-shm")).ok();
}

async fn setup() -> (Arc<Services>, WorkspaceId, PathBuf, PathBuf) {
    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-agent-bind-{}.db",
        uuid::Uuid::new_v4()
    ));
    let ws_root =
        std::env::temp_dir().join(format!("itd-e2e-agent-bind-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_settings_registry(common::registry_with_default_provider(&ws_root))
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(ws_root.clone())))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, ws_root, db)
}

#[tokio::test]
async fn agent_subscribe_creates_event_subscription() {
    let (services, ws, ws_root, db) = setup().await;

    // Call agent.subscribe
    let result = services
        .agent_subscribe(
            ws.clone(),
            None,
            vec!["agent:*".to_string()],
            Some(false),
            None,
        )
        .await
        .expect("subscribe");

    // Assert concrete contract: subscription created with both subscriptionId and eventTypes
    assert!(result["subscriptionId"].is_string());
    let sub_id = result["subscriptionId"].as_str().unwrap().to_string();
    assert!(!sub_id.is_empty());
    assert!(result["eventTypes"].is_array());
    assert_eq!(result["eventTypes"].as_array().unwrap().len(), 1);
    assert_eq!(result["eventTypes"][0].as_str().unwrap(), "agent:*");

    // Verify subscription works by unsubscribing
    let unsub_result = services
        .agent_unsubscribe(ws.clone(), sub_id.clone())
        .await
        .expect("unsubscribe");
    assert!(unsub_result["success"].is_boolean());
    assert!(unsub_result["success"].as_bool().unwrap());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_diagnostics_returns_workspace_snapshot() {
    let (services, ws, ws_root, db) = setup().await;

    services
        .agent_create(
            ws.clone(),
            Some("DiagTest".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");

    // Call agent.diagnostics
    let result = services
        .agent_diagnostics(ws.clone(), None, None, None)
        .await
        .expect("diagnostics");

    // Assert concrete contract: full shape { ok, diagnostics, text }
    assert!(result["ok"].as_bool().unwrap());
    assert!(result["diagnostics"].is_object());
    assert!(result["text"].is_string());
    assert!(!result["text"].as_str().unwrap().is_empty());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_status_returns_full_metadata() {
    use intent_core::AgentStatus;
    let (services, ws, ws_root, db) = setup().await;

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("StatusTest".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Call agent.status (via agent_get)
    let result = services
        .agent_get(agent_id.clone(), Some(ws.clone()))
        .await
        .expect("get agent");

    // Assert concrete contract: AgentLite shape with stable fields
    assert_eq!(result.id, agent_id);
    assert_eq!(result.workspace_id, ws);
    assert_eq!(result.name, "StatusTest");
    assert_eq!(result.status, AgentStatus::Idle);
    assert!(!result.metadata.is_background);

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_list_returns_created_agents() {
    let (services, ws, ws_root, db) = setup().await;

    let agent1 = services
        .agent_create(
            ws.clone(),
            Some("ListTest1".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent 1");
    let id1 = AgentId::from(agent1["agent"]["id"].as_str().unwrap());

    let agent2 = services
        .agent_create(
            ws.clone(),
            Some("ListTest2".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent 2");
    let id2 = AgentId::from(agent2["agent"]["id"].as_str().unwrap());

    // Call agent.list
    let result = services.agent_list(ws.clone()).await.expect("list agents");

    // Assert concrete contract: array of AgentLite with both agents
    assert!(result.len() >= 2);
    assert!(result.iter().any(|a| a.id == id1 && a.name == "ListTest1"));
    assert!(result.iter().any(|a| a.id == id2 && a.name == "ListTest2"));

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_read_conversation_returns_messages() {
    let (services, ws, ws_root, db) = setup().await;

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("ConvTest".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Send a message to populate conversation
    services
        .agent_send_message(
            ws.clone(),
            agent_id.clone(),
            "test message".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            intent_core::MessageOrigin::User,
        )
        .await
        .expect("send message");

    // Call agent.readConversation
    let result = services
        .agent_get_conversation(
            agent_id.clone(),
            None,
            Some(ws.clone()),
            None,
            None,
            None,
            None,
            false,
        )
        .await
        .expect("get conversation");

    // Assert concrete contract: top-level fields and message shape
    assert_eq!(
        result["agentId"].as_str().unwrap(),
        agent_id.to_string().as_str()
    );
    assert!(result["truncated"].is_boolean());
    assert!(result["totalMessages"].is_number());
    assert!(result["messages"].is_array());
    let messages = result["messages"].as_array().unwrap();
    assert!(!messages.is_empty());
    // Assert first message has role and contentBlocks
    let first_msg = &messages[0];
    assert!(first_msg["role"].is_string());
    assert!(first_msg["contentBlocks"].is_array());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_summary_returns_shape() {
    let (services, ws, ws_root, db) = setup().await;

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("SummTest".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Call agent.summary
    let result = services
        .agent_summary(ws.clone(), agent_id.clone())
        .await
        .expect("get summary");

    // Assert concrete contract: object with required fields
    assert!(result.is_object());
    assert_eq!(
        result["agentId"].as_str().unwrap(),
        agent_id.to_string().as_str()
    );
    assert!(result["agentName"].is_string());
    assert!(result["messageCount"].is_number());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}
