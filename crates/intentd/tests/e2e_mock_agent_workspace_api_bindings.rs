//! Hermetic ACP E2E: comprehensive coverage of agent→BE workspace_api bindings.
//!
//! Each test spawns the mock ACP agent with `MOCK_AGENT_BEHAVIOR` that drives
//! real MCP `tools/call` invocations for the target binding namespace. We assert
//! BE state changed via Services reads, not just tool-call success — proving the
//! full loop works.
//!
//! This file covers: **task** and **comment** bindings.
//! See `e2e_mock_agent_workspace_api_bindings2.rs` for note, file, git, agent, event.
//!
//! Pattern: modeled after `tests/e2e_mock_agent.rs` single-turn execution.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{
    now_iso, AgentId, NoteCreate, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E Bindings".to_string(),
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

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping workspace_api bindings e2e: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping workspace_api bindings e2e: script missing at {script}");
        return None;
    }
    Some(script)
}

//
// Task bindings coverage
//

#[tokio::test]
async fn task_bindings_update_status_and_get() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-task-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, None))
        .await
        .expect("insert ws");

    // Create a task note with checkbox
    let task_note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Task".into(),
                content: Some("- [ ] test task\n".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create task note")
        .note;

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Task".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    let script_static: &'static str = Box::leak(script.into_boxed_str());
    let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
    let provider = ProviderConfig {
        command: "node",
        base_args,
        supports_authenticate: true,
        supports_mcp_config: true,
        mcp_config_flag: Some("--mcp-config"),
        ..*intent_providers::find_provider("mock").unwrap()
    };

    // Update task status and read it back
    let js = format!(
        r"
        await ws.task.updateStatus('{}', 'test task', 'in-progress');
        const tasks = await ws.note.listTasks('{}');
        return {{ tasks: tasks }};
        ",
        task_note.id.0, task_note.id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "task bindings e2e" }
        },
        "response": "task updated",
    })
    .to_string();

    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
    // Guarded agent cwd: context-engine children (auggie) write logs into
    // their cwd; a bare temp_dir() would leak them at the TMPDIR root.
    let cwd_dir = common::test_tempdir("itd-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(&cwd);
    opts.extra_env = extra_env;

    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = AgentManager::new(services.clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));

    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "E2E Task",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&agent_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "update task" }))
            .unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn")
    );

    // Assert BE state changed
    let updated = services
        .get_note(ws.clone(), task_note.id.clone())
        .await
        .expect("get note");
    assert!(
        updated.content.contains("- [/] test task"),
        "task status updated to in-progress: {}",
        updated.content
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

//
// Comment bindings coverage
//

#[tokio::test]
async fn comment_bindings_add_and_list() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-comment-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, None))
        .await
        .expect("insert ws");

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Note with content".into(),
                content: Some("Initial content\nTarget phrase for comment\nMore content".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Comment".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    let script_static: &'static str = Box::leak(script.into_boxed_str());
    let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
    let provider = ProviderConfig {
        command: "node",
        base_args,
        supports_authenticate: true,
        supports_mcp_config: true,
        mcp_config_flag: Some("--mcp-config"),
        ..*intent_providers::find_provider("mock").unwrap()
    };

    let js = format!(
        r"
        await ws.comment.add('{}', {{
            searchContext: 'Target phrase for comment',
            commentTarget: 'Target phrase',
            comment: 'test comment text',
            type: 'comment'
        }});
        const threads = await ws.comment.list('{}', {{ includeComments: true }});
        return {{ threads: threads }};
        ",
        note.id.0, note.id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "comment bindings e2e" }
        },
        "response": "comment added",
    })
    .to_string();

    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
    // Guarded agent cwd: context-engine children (auggie) write logs into
    // their cwd; a bare temp_dir() would leak them at the TMPDIR root.
    let cwd_dir = common::test_tempdir("itd-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(&cwd);
    opts.extra_env = extra_env;

    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = AgentManager::new(services.clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));

    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "E2E Comment",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&agent_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "add comment" }))
            .unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Assert via Services that the comment was actually persisted
    let result = services
        .comment_list(
            ws.clone(),
            note.id.clone(),
            None, // since
            None, // author_type
            None, // status
            true, // include_comments
        )
        .await
        .expect("comment_list");

    let threads = result.threads;
    assert!(!threads.is_empty(), "comment thread should exist");
    let thread = &threads[0];
    let comments = thread
        .comments
        .as_ref()
        .expect("comments should be included");
    assert!(
        comments
            .iter()
            .any(|c| c.content.contains("test comment text")),
        "comment text should be persisted"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
