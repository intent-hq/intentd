//! E2E coverage for agent.queueMessage / agent.getQueue / agent.removeQueuedMessage /
//! agent.readConversation / agent.summary / agent.diagnostics
//! (intent-services `agent_ops.rs` coverage boost).
//!
//! Tests call `intent_services::Services` directly (not via WSS transport) for hermetic
//! in-process coverage. Asserts on backend state changes.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use serde_json::json;

/// Clean up `SQLite` database including -wal and -shm sidecars.
fn cleanup_db(db: &PathBuf) {
    std::fs::remove_file(db).ok();
    std::fs::remove_file(db.with_extension("db-wal")).ok();
    std::fs::remove_file(db.with_extension("db-shm")).ok();
}

fn workspace(id: &WorkspaceId, path: &Path) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E-agent-queue".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec![],
        path: Some(path.display().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(path.display().to_string()),
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
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

async fn setup() -> (Arc<Services>, WorkspaceId, PathBuf, PathBuf) {
    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-agent-queue-{}.db",
        uuid::Uuid::new_v4()
    ));
    let ws_root =
        std::env::temp_dir().join(format!("itd-e2e-agent-queue-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_settings_registry(common::registry_with_default_provider(&ws_root))
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, &ws_root.clone()))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, ws_root, db)
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
#[tokio::test]
async fn agent_queue_add_get_remove_lifecycle() {
    let (services, ws, ws_root, db) = setup().await;

    // Create agent
    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("QueueTest".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Queue a message
    let queued = services
        .agent_queue_message(agent_id.clone(), "test message".into(), None, None)
        .await
        .expect("queue message");
    assert_eq!(queued["success"], true);
    let msg_id = queued["queuedMessage"]["id"].as_str().unwrap().to_string();
    assert_eq!(queued["queuedMessage"]["content"], "test message");
    assert_eq!(queued["queuedMessage"]["position"], 0);

    // Get queue - should have one message
    let queue_result = services
        .agent_get_queue(agent_id.clone(), Some(ws.clone()))
        .await
        .expect("get queue");
    assert_eq!(queue_result["success"], true);
    let queue = queue_result["queue"].as_array().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue[0]["id"].as_str().expect("id should be a string"),
        msg_id
    );
    assert_eq!(
        queue[0]["content"]
            .as_str()
            .expect("content should be a string"),
        "test message"
    );

    // Remove the message
    let removed = services
        .agent_remove_queued_message(agent_id.clone(), msg_id.clone())
        .await
        .expect("remove message");
    assert_eq!(removed["success"], true);

    // Get queue again - should be empty
    let queue_result2 = services
        .agent_get_queue(agent_id.clone(), Some(ws.clone()))
        .await
        .expect("get queue after remove");
    let queue2 = queue_result2["queue"].as_array().unwrap();
    assert_eq!(queue2.len(), 0);

    // Remove again - should be idempotent
    let removed2 = services
        .agent_remove_queued_message(agent_id.clone(), msg_id)
        .await
        .expect("remove message idempotent");
    assert_eq!(removed2["success"], true);

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_conversation_and_summary() {
    let (services, ws, ws_root, db) = setup().await;

    // Create agent
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

    // Append a message to the agent's conversation
    let blocks = json!([
        { "type": "text", "text": "Hello, I completed the task" }
    ]);
    services
        .store()
        .append_agent_message(&agent_id, "assistant", &blocks, &now_iso())
        .await
        .expect("append message");

    // Read conversation
    let conversation = services
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
    assert!(conversation["messages"].is_array());
    let messages = conversation["messages"].as_array().unwrap();
    assert!(!messages.is_empty());

    // Get summary
    let summary = services
        .agent_summary(ws.clone(), agent_id.clone())
        .await
        .expect("get summary");
    assert_eq!(
        summary["agentId"]
            .as_str()
            .expect("agentId should be a string"),
        agent_id.to_string()
    );
    assert_eq!(
        summary["agentName"]
            .as_str()
            .expect("agentName should be a string"),
        "ConvTest"
    );
    assert_eq!(
        summary["messageCount"]
            .as_i64()
            .expect("messageCount should be an integer"),
        1
    );

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_diagnostics_baseline() {
    let (services, ws, ws_root, db) = setup().await;

    // Get diagnostics with no agents
    let diag = services
        .agent_diagnostics(ws.clone(), None, None, None)
        .await
        .expect("get diagnostics");
    assert_eq!(diag["ok"], true);
    assert!(diag["diagnostics"].is_object());
    assert!(diag["text"].is_string());

    // Create an agent
    let agent_val = services
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
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Get diagnostics focused on this agent
    let diag2 = services
        .agent_diagnostics(ws.clone(), Some(agent_id.clone()), None, None)
        .await
        .expect("get diagnostics with agent filter");
    assert_eq!(diag2["ok"], true);
    assert!(diag2["diagnostics"]["agents"].is_array());

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}
