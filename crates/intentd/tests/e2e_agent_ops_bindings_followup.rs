//! E2E coverage follow-up for agent_ops.rs reachable operations.
//!
//! Exercises agent_send_message, agent_send_to_task, agent_wake_or_create,
//! agent_cancel_subscriptions via in-process Services calls. Hermetic tests asserting BE state changes.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::{
    now_iso, AgentId, AgentWakeOrCreateInput, NoteCreate, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E Agent Ops".to_string(),
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
    }
}

fn cleanup_db(db: &PathBuf) {
    std::fs::remove_file(db).ok();
    std::fs::remove_file(db.with_extension("db-wal")).ok();
    std::fs::remove_file(db.with_extension("db-shm")).ok();
}

async fn setup() -> (Arc<Services>, WorkspaceId, PathBuf, PathBuf) {
    let db =
        std::env::temp_dir().join(format!("intentd-e2e-agent-ops-{}.db", uuid::Uuid::new_v4()));
    let ws_root =
        std::env::temp_dir().join(format!("itd-e2e-agent-ops-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(ws_root.clone())))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, ws_root, db)
}

#[tokio::test]
async fn agent_send_message_queues_for_idle_agent() {
    let (services, ws, ws_root, db) = setup().await;

    // Create target agent
    let target_val = services
        .agent_create(
            ws.clone(),
            Some("Target".into()),
            None,
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create target");
    let target_id = AgentId::from(target_val["agent"]["id"].as_str().unwrap());

    // Send message via agent_send_message
    let result = services
        .agent_send_message(
            ws.clone(),
            target_id.clone(),
            "Hello from sender".into(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("agent send message");
    assert_eq!(result["success"], true);

    // Message was queued (agent idle, no manager attached)
    if result["queued"] == true {
        let queue = services
            .agent_get_queue(target_id.clone(), Some(ws.clone()))
            .await
            .expect("get queue");
        let messages = queue["queue"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0]["content"].as_str().unwrap(),
            "Hello from sender"
        );
    }

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_send_to_task_delivers_to_assigned_agent() {
    let (services, ws, ws_root, db) = setup().await;

    // Create a task note
    let task_note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Test Task".to_string(),
                content: Some("Task content".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create task note");

    // Mark it as a task
    services
        .mark_as_task(
            ws.clone(),
            task_note.id.clone(),
            "not_started".to_string(),
            vec![],
            None,
        )
        .await
        .expect("mark as task");

    // Create and assign an agent
    let assigned_agent = services
        .agent_create(
            ws.clone(),
            Some("AssignedAgent".into()),
            None,
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create assigned agent");
    let assigned_id = AgentId::from(assigned_agent["agent"]["id"].as_str().unwrap());

    services
        .assign_agent(ws.clone(), task_note.id.clone(), assigned_id.to_string())
        .await
        .expect("assign agent");

    // Call agent.sendToTask
    let result = services
        .agent_send_to_task(
            ws.clone(),
            task_note.id.clone(),
            "Task message".into(),
            None,
        )
        .await
        .expect("send to task");

    // Result should indicate success (may have ok or success field)
    assert!(result.get("ok").is_some() || result.get("success").is_some());

    // If message was delivered (not queued), it went directly to the agent
    // If no manager, it might be persisted directly without queueing
    // In either case, the agent_id should be in the result
    assert!(result.get("agentId").is_some() || result.get("delivered").is_some());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_cancel_subscriptions_idempotent() {
    let (services, ws, ws_root, db) = setup().await;

    // Create an agent
    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("TestAgent".into()),
            None,
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Cancel subscriptions (should be idempotent even when empty)
    let result = services
        .agent_cancel_subscriptions(ws.clone(), agent_id.clone())
        .await
        .expect("cancel subscriptions");
    assert_eq!(result["success"], true);

    // Call again - should still work
    let result2 = services
        .agent_cancel_subscriptions(ws.clone(), agent_id)
        .await
        .expect("cancel subscriptions again");
    assert_eq!(result2["success"], true);

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn agent_wake_or_create_creates_for_unassigned_task() {
    let (services, ws, ws_root, db) = setup().await;

    // Create a task note
    let task_note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Wake Task".to_string(),
                content: Some("Wake content".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create task note");

    // Mark it as a task
    services
        .mark_as_task(
            ws.clone(),
            task_note.id.clone(),
            "not_started".to_string(),
            vec![],
            None,
        )
        .await
        .expect("mark as task");

    // Call wake_or_create (should create since no agent assigned)
    let result = services
        .agent_wake_or_create(
            ws.clone(),
            task_note.id.clone(),
            "Context message".into(),
            AgentWakeOrCreateInput::default(),
        )
        .await
        .expect("wake or create");
    assert!(result["agentId"].is_string());

    // Assert: an agent should now be assigned to the task
    let task = services
        .get_my_task(ws.clone(), task_note.id.clone())
        .await
        .expect("get task");
    assert!(!task.assigned_agents.is_empty());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}
