//! Hermetic ACP E2E: event, git, file, agent, note bindings coverage.
//!
//! Each test spawns the mock ACP agent with `MOCK_AGENT_BEHAVIOR` that drives
//! real MCP `tools/call` invocations for the target binding namespace. We assert
//! BE state changed via Services reads, not just tool-call success — proving the
//! full loop works.
//!
//! This file covers: **note**, **file**, **git**, **agent**, and **event** bindings.
//! See `e2e_mock_agent_workspace_api_bindings.rs` for task and comment.

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
        title: "E2E Bindings 2".to_string(),
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
        eprintln!("skipping workspace_api bindings2 e2e: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping workspace_api bindings2 e2e: script missing");
        return None;
    }
    Some(script)
}

//
// Event bindings coverage
//

#[tokio::test]
async fn event_bindings_query_and_subscribe() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-event-{}.db", uuid::Uuid::new_v4()));
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

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Event".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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

    let js = r#"
        const events = await ws.event.query({ eventType: 'note:created', limit: 10 });
        const sub = await ws.event.subscribe(['note:*'], { excludeSelf: true });
        await ws.event.unsubscribe(sub.subscriptionId);
        return { events: events };
    "#;

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "event bindings e2e" }
        },
        "response": "events queried",
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
            "E2E Event",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "query events" }))
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

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

//
// File bindings coverage
//

#[tokio::test]
async fn file_bindings_read_write_list() {
    let Some(script) = gate() else { return };

    let ws_root = std::env::temp_dir().join(format!("itd-e2e-file-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("mkdir ws_root");
    std::fs::write(ws_root.join("existing.txt"), "existing content").expect("write existing");

    let db = std::env::temp_dir().join(format!("intentd-e2e-file-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.clone())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(ws_root.clone())))
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E File".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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

    let js = r#"
        const content = await ws.file.read('existing.txt');
        await ws.file.write('new.txt', 'new file content');
        await ws.file.mkdir('subdir');
        const files = await ws.file.list('.');
        await ws.file.rename('new.txt', 'renamed.txt');
        await ws.file.write('pkg/a.txt', 'aaa');
        await ws.file.write('pkg/nested/b.txt', 'bbb');
        await ws.file.rename('pkg', 'moved');
        return { content: content, files: files };
    "#;

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "file bindings e2e" }
        },
        "response": "file operations done",
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
            "E2E File",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "file ops" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Assert actual filesystem effects - Services resolves paths relative to workspace root
    let workspace_record = services
        .get_workspace(ws.clone())
        .await
        .expect("get workspace");
    let actual_ws_root = std::path::PathBuf::from(
        workspace_record
            .worktree_path
            .expect("workspace should have worktree_path"),
    );
    assert!(
        actual_ws_root.join("renamed.txt").exists(),
        "renamed.txt should exist in {:?}",
        actual_ws_root
    );
    let renamed_content =
        std::fs::read_to_string(actual_ws_root.join("renamed.txt")).expect("read renamed.txt");
    assert_eq!(renamed_content, "new file content");
    assert!(
        actual_ws_root.join("subdir").is_dir(),
        "subdir should exist"
    );

    // Directory rename (monorepo#957): every contained file must be attributed
    // to the agent — both old-side (deleted) and new-side (added) rows.
    assert!(
        actual_ws_root.join("moved/a.txt").exists()
            && actual_ws_root.join("moved/nested/b.txt").exists(),
        "moved dir contents should exist"
    );
    let tracked = store.list_tracked_changes(&ws).await.expect("tracked");
    for expected in [
        ("pkg/a.txt", "deleted"),
        ("pkg/nested/b.txt", "deleted"),
        ("moved/a.txt", "added"),
        ("moved/nested/b.txt", "added"),
    ] {
        assert!(
            tracked.iter().any(|t| t.path == expected.0
                && t.status == expected.1
                && t.agent_id.as_deref() == Some(agent_id.0.as_str())),
            "expected tracked change {expected:?} attributed to agent; got {:?}",
            tracked
                .iter()
                .map(|t| (t.path.clone(), t.status.clone(), t.agent_id.clone()))
                .collect::<Vec<_>>()
        );
    }

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

//
// Agent bindings coverage (read-side: list, status)
//

#[tokio::test]
async fn agent_bindings_list_and_status() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-agent-{}.db", uuid::Uuid::new_v4()));
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

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Agent".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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

    // List agents and get status
    let js = format!(
        r#"
        const agents = await ws.agent.list(false);
        const status = await ws.agent.status('{}');
        return {{ agents: agents, status: status }};
        "#,
        agent_id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "agent bindings e2e" }
        },
        "response": "agent listed",
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
            "E2E Agent",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "list agents" }))
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

    // Verify agent exists
    let session = store.get_agent_session(&agent_id).await.expect("session");
    assert_eq!(session.id, agent_id);

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// `ws.agent.getQueue` + `ws.agent.removeQueuedMessage` + the queue merged
/// into `ws.agent.status`: another sender's entry surfaces with attribution
/// (full content in getQueue, 200-char truncation in status), ordering is
/// next-delivery-first (interrupt ahead of normal FIFO), removal of the
/// caller's own entry succeeds, and removal of a foreign entry is rejected
/// by the ownership guard.
#[tokio::test]
async fn agent_bindings_get_queue_and_remove_queued_message() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-queue-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());
    // Pin `workspaceApi.toonOutput` off so the workspace_api tool body stays
    // plain JSON for the serde_json assertions below (TOON is on by default).
    services
        .settings_update(serde_json::json!([
            { "path": "workspaceApi.toonOutput", "value": false }
        ]))
        .await
        .expect("disable toonOutput");

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, None))
        .await
        .expect("insert ws");

    let caller_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Queue Caller".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create caller");
    let caller_id = AgentId::from(caller_val["agent"]["id"].as_str().unwrap());
    let target_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Queue Target".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create target");
    let target_id = AgentId::from(target_val["agent"]["id"].as_str().unwrap());

    // Seed the target's queue via the durable snapshot + rehydration path.
    // Stored order is deliberately divergent from drain order (normal entry
    // ahead of the interrupt) to exercise the binding's next-delivery-first
    // sort. Entry attribution mirrors the §5.5 `agent_message` auto-tag.
    let long_content = "F".repeat(300);
    let payload = |id: &str, content: &str, metadata: serde_json::Value, interrupt: bool| {
        serde_json::json!({
            "id": id,
            "content": content,
            "queuedAt": now_iso(),
            "messageMetadata": metadata,
            "interruptPriority": interrupt,
        })
    };
    let rows = vec![
        intent_store::AgentQueueRow {
            id: "qmsg-foreign".into(),
            agent_id: target_id.clone(),
            position: 0,
            payload: payload(
                "qmsg-foreign",
                &long_content,
                serde_json::json!({
                    "type": "agent_message",
                    "fromAgentId": "agent-other",
                    "fromAgentName": "Other Sender",
                }),
                false,
            ),
            created_at: now_iso(),
            turn_id: "qmsg-foreign".into(),
        },
        intent_store::AgentQueueRow {
            id: "qmsg-interrupt".into(),
            agent_id: target_id.clone(),
            position: 1,
            payload: payload("qmsg-interrupt", "urgent", serde_json::Value::Null, true),
            created_at: now_iso(),
            turn_id: "qmsg-interrupt".into(),
        },
        intent_store::AgentQueueRow {
            id: "qmsg-own".into(),
            agent_id: target_id.clone(),
            position: 2,
            payload: payload(
                "qmsg-own",
                "retract me",
                serde_json::json!({
                    "type": "agent_message",
                    "fromAgentId": caller_id.0,
                    "fromAgentName": "E2E Queue Caller",
                }),
                false,
            ),
            created_at: now_iso(),
            turn_id: "qmsg-own".into(),
        },
    ];
    store
        .replace_agent_queue(&target_id, &rows)
        .await
        .expect("seed queue");
    let rehydrated = services
        .rehydrate_agent_queues()
        .await
        .expect("rehydrate queues");
    assert_eq!(rehydrated, 3, "all seeded entries rehydrated");

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
        r#"
        const target = '{}';
        const out = {{}};
        out.queue = await ws.agent.getQueue(target);
        out.status = await ws.agent.status(target);
        out.removeOwn = await ws.agent.removeQueuedMessage(target, 'qmsg-own');
        try {{
            await ws.agent.removeQueuedMessage(target, 'qmsg-foreign');
            out.removeForeignError = null;
        }} catch (error) {{
            out.removeForeignError = error.message;
        }}
        return out;
        "#,
        target_id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "agent queue bindings e2e" }
        },
        "response": "queue inspected",
        "emitToolBlocks": true,
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
            caller_id.clone(),
            ws.clone(),
            "E2E Queue Caller",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&caller_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "inspect queue" }))
            .unwrap();
    let stop = manager
        .run_turn(&caller_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Pull the JS return value out of the persisted tool-result block.
    let transcript = services
        .agent_get_conversation(caller_id.clone(), None, Some(ws.clone()), None, None, None)
        .await
        .expect("get conversation");
    let messages = transcript["messages"].as_array().expect("messages array");
    let last_output = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter_map(|b| {
            (b["type"] == "tool_result")
                .then(|| b["output"].as_array())
                .flatten()
                .and_then(|arr| arr.first())
                .and_then(|item| item["text"].as_str())
        })
        .next_back()
        .expect("tool result block in transcript");
    let out: serde_json::Value =
        serde_json::from_str(last_output).expect("tool output should be JSON");

    // getQueue: drain order (interrupt first), attribution lifted, full content.
    let queue = out["queue"]["queue"].as_array().expect("queue array");
    assert_eq!(out["queue"]["queueLength"], 3, "{out}");
    let ids: Vec<&str> = queue.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        ["qmsg-interrupt", "qmsg-foreign", "qmsg-own"],
        "next-delivery-first: interrupt ahead of normal FIFO"
    );
    assert_eq!(queue[0]["interruptPriority"], true);
    assert_eq!(queue[0]["position"], 0);
    assert!(
        queue[0].get("fromAgentId").is_none(),
        "user-origin entry carries no attribution: {}",
        queue[0]
    );
    assert_eq!(queue[1]["fromAgentId"], "agent-other");
    assert_eq!(queue[1]["fromAgentName"], "Other Sender");
    assert_eq!(
        queue[1]["content"].as_str().unwrap().chars().count(),
        300,
        "getQueue carries full content"
    );
    assert_eq!(queue[2]["fromAgentId"], caller_id.0);

    // status: same queue merged in, content truncated to 200 chars + '…'.
    assert_eq!(out["status"]["queueLength"], 3, "{out}");
    let status_queue = out["status"]["queue"].as_array().expect("status queue");
    let foreign = &status_queue[1];
    let content = foreign["content"].as_str().expect("truncated content");
    assert_eq!(content.chars().count(), 201, "200 chars + ellipsis");
    assert!(content.ends_with('…'));

    // Removal: own entry succeeded, foreign entry rejected by ownership guard.
    assert_eq!(out["removeOwn"]["ok"], true, "{out}");
    let guard_err = out["removeForeignError"]
        .as_str()
        .expect("foreign removal must error");
    assert!(
        guard_err.contains("another sender"),
        "ownership guard names the violation: {guard_err}"
    );

    // Backend state: only the caller's own entry was removed (raw service
    // read is in stored order — foreign was seeded ahead of the interrupt).
    let remaining = services
        .agent_get_queue(target_id.clone(), Some(ws.clone()))
        .await
        .expect("backend queue");
    let remaining_ids: Vec<&str> = remaining["queue"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(remaining_ids, ["qmsg-foreign", "qmsg-interrupt"]);

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// Single-pending-message guard on `ws.agent.send` / `ws.agent.sendToTask`:
/// the target is pinned behind a question hold so agent-origin sends park in
/// its queue. The caller's FIRST send parks fine (a pre-seeded FOREIGN entry
/// does not trigger the guard); the second send and a `sendToTask` against
/// the same target are refused with `ok: false` + the full queue echo
/// (drain order, 200-char truncation); after `removeQueuedMessage` retracts
/// the caller's entry, a re-send parks again.
#[tokio::test]
async fn agent_bindings_send_single_pending_message_guard() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-sguard-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());
    // Pin `workspaceApi.toonOutput` off so the workspace_api tool body stays
    // plain JSON for the serde_json assertions below (TOON is on by default).
    services
        .settings_update(serde_json::json!([
            { "path": "workspaceApi.toonOutput", "value": false }
        ]))
        .await
        .expect("disable toonOutput");

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, None))
        .await
        .expect("insert ws");

    let caller_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Guard Caller".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create caller");
    let caller_id = AgentId::from(caller_val["agent"]["id"].as_str().unwrap());
    let target_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Guard Target".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create target");
    let target_id = AgentId::from(target_val["agent"]["id"].as_str().unwrap());

    // Pin the target behind a question hold (PROTOCOL §5.5): its last
    // transcript message is an assistant row carrying a question resource
    // block, so every agent-origin send parks in the queue instead of
    // driving a turn — the deterministic way to exercise the guard.
    store
        .append_agent_message(
            &target_id,
            "assistant",
            &serde_json::json!([{
                "type": "resource",
                "resource": {
                    "uri": "question://hold-1",
                    "mimeType": "application/vnd.intent.question+json",
                    "text": "{\"question\":\"Which environment?\"}",
                },
            }]),
            &now_iso(),
        )
        .await
        .expect("seed question hold");

    // Seed a FOREIGN pending entry (long content — the refusal echo must
    // truncate it): another agent's parked send must NOT trigger the guard
    // for this caller.
    let long_content = "F".repeat(300);
    store
        .replace_agent_queue(
            &target_id,
            &[intent_store::AgentQueueRow {
                id: "qmsg-foreign".into(),
                agent_id: target_id.clone(),
                position: 0,
                payload: serde_json::json!({
                    "id": "qmsg-foreign",
                    "content": long_content,
                    "queuedAt": now_iso(),
                    "messageMetadata": {
                        "type": "agent_message",
                        "fromAgentId": "agent-other",
                        "fromAgentName": "Other Sender",
                    },
                    "interruptPriority": false,
                }),
                created_at: now_iso(),
                turn_id: "qmsg-foreign".into(),
            }],
        )
        .await
        .expect("seed foreign entry");
    assert_eq!(
        services
            .rehydrate_agent_queues()
            .await
            .expect("rehydrate queues"),
        1
    );

    // Task note assigned to the target — `sendToTask` must hit the same guard.
    let task_note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Guard Task".into(),
                content: Some("Guarded task".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create task note")
        .note;
    services
        .mark_as_task(
            ws.clone(),
            task_note.id.clone(),
            "not_started".to_string(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("mark as task");
    services
        .assign_agent(
            ws.clone(),
            task_note.id.clone(),
            target_id.to_string(),
            None,
        )
        .await
        .expect("assign target");

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
        r#"
        const target = '{}';
        const out = {{}};
        out.first = await ws.agent.send(target, 'first: please review the diff');
        out.second = await ws.agent.send(target, 'second: also check the tests');
        out.interrupt = await ws.agent.send(target, 'urgent: drop everything', 'interrupt');
        out.toTask = await ws.agent.sendToTask('{}', 'task: status update please');
        out.removed = await ws.agent.removeQueuedMessage(target, out.first.queuedMessage.id);
        out.resend = await ws.agent.send(target, 'combined: review diff AND check tests');
        return out;
        "#,
        target_id.0, task_note.id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "single-pending guard e2e" }
        },
        "response": "guard exercised",
        "emitToolBlocks": true,
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
    let manager = Arc::new(
        AgentManager::new(services.clone(), sink, 8)
            .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd")),
    );
    services.attach_agent_manager(&manager);

    manager
        .create_agent(
            caller_id.clone(),
            ws.clone(),
            "E2E Guard Caller",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&caller_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "exercise guard" }))
            .unwrap();
    let stop = manager
        .run_turn(&caller_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Pull the JS return value out of the persisted tool-result block.
    let transcript = services
        .agent_get_conversation(caller_id.clone(), None, Some(ws.clone()), None, None, None)
        .await
        .expect("get conversation");
    let messages = transcript["messages"].as_array().expect("messages array");
    let last_output = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter_map(|b| {
            (b["type"] == "tool_result")
                .then(|| b["output"].as_array())
                .flatten()
                .and_then(|arr| arr.first())
                .and_then(|item| item["text"].as_str())
        })
        .next_back()
        .expect("tool result block in transcript");
    let out: serde_json::Value =
        serde_json::from_str(last_output).expect("tool output should be JSON");

    // First send parks (question hold) despite the pre-seeded FOREIGN entry:
    // another sender's pending message never triggers the guard.
    assert_eq!(out["first"]["ok"], true, "{out}");
    assert_eq!(out["first"]["queued"], true, "{out}");
    assert_eq!(out["first"]["heldForQuestions"], true, "{out}");
    let first_id = out["first"]["queuedMessage"]["id"]
        .as_str()
        .expect("first parked entry id");

    // Second send REFUSED: ok:false, names the pending entry, echoes the
    // target's full queue in drain order with 200-char truncation.
    let second = &out["second"];
    assert_eq!(second["ok"], false, "{out}");
    assert_eq!(second["agentId"], target_id.0, "{out}");
    assert_eq!(second["pendingMessageId"], first_id, "{out}");
    assert!(
        second["error"].as_str().unwrap().contains(first_id),
        "refusal names the pending entry: {second}"
    );
    assert!(
        second["instruction"]
            .as_str()
            .unwrap()
            .contains("removeQueuedMessage"),
        "refusal instructs retract-and-resend: {second}"
    );
    assert_eq!(second["queueLength"], 2, "{out}");
    let echo = second["queue"].as_array().expect("queue echo");
    let echo_ids: Vec<&str> = echo.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert_eq!(
        echo_ids,
        ["qmsg-foreign", first_id],
        "echo is the full queue in drain order"
    );
    assert_eq!(echo[0]["fromAgentId"], "agent-other", "{second}");
    assert_eq!(echo[1]["fromAgentId"], caller_id.0, "{second}");
    let foreign_echo = echo[0]["content"].as_str().unwrap();
    assert_eq!(
        foreign_echo.chars().count(),
        201,
        "echo content truncated to 200 chars + ellipsis"
    );
    assert!(foreign_echo.ends_with('…'));

    // Interrupt-priority send from the same caller: refused all the same —
    // interrupts get no exemption from the single-pending-message guard.
    let interrupt = &out["interrupt"];
    assert_eq!(interrupt["ok"], false, "{out}");
    assert_eq!(interrupt["agentId"], target_id.0, "{out}");
    assert_eq!(interrupt["pendingMessageId"], first_id, "{out}");

    // sendToTask against the same target: same refusal, tagged with the task.
    let to_task = &out["toTask"];
    assert_eq!(to_task["ok"], false, "{out}");
    assert_eq!(to_task["agentId"], target_id.0, "{out}");
    assert_eq!(to_task["taskNoteId"], task_note.id.0, "{out}");
    assert_eq!(to_task["pendingMessageId"], first_id, "{out}");

    // Retract-and-resend: removal succeeds, the re-send parks again.
    assert_eq!(out["removed"]["ok"], true, "{out}");
    assert_eq!(out["resend"]["ok"], true, "{out}");
    assert_eq!(out["resend"]["queued"], true, "{out}");

    // Backend state: foreign entry untouched, caller's queue slot holds ONLY
    // the combined re-send (the refused sends never parked).
    let remaining = services
        .agent_get_queue(target_id.clone(), Some(ws.clone()))
        .await
        .expect("backend queue");
    let remaining = remaining["queue"].as_array().unwrap();
    let ids: Vec<&str> = remaining
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 2, "foreign + one caller entry: {ids:?}");
    assert_eq!(ids[0], "qmsg-foreign");
    let contents: Vec<&str> = remaining
        .iter()
        .map(|e| e["content"].as_str().unwrap())
        .collect();
    assert!(
        contents[1].starts_with("combined:"),
        "only the re-send parked: {contents:?}"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

//
// Git bindings coverage
//

#[tokio::test]
async fn git_bindings_commit() {
    let Some(script) = gate() else { return };

    // Skip if git is not available
    if std::process::Command::new("git")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipping git e2e: git not available");
        return;
    }

    // Create a temp git repo
    let repo_dir = std::env::temp_dir().join(format!("itd-e2e-git-{}", uuid::Uuid::new_v4()));

    // Helper to run git commands and assert success
    let run_git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo_dir)
            .output()
            .unwrap_or_else(|e| panic!("git {} failed to execute: {}", args.join(" "), e));
        assert!(
            status.status.success(),
            "git {} failed with exit code {:?}: {}",
            args.join(" "),
            status.status.code(),
            String::from_utf8_lossy(&status.stderr)
        );
    };
    std::fs::create_dir_all(&repo_dir).expect("mkdir repo");
    run_git(&["init"]);
    run_git(&["config", "user.name", "Test"]);
    run_git(&["config", "user.email", "test@test.com"]);

    // Initial commit
    std::fs::write(repo_dir.join("README.md"), "initial").expect("write readme");
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-m", "initial"]);

    // Create new file for git operations
    std::fs::write(repo_dir.join("test.txt"), "test content").expect("write test");

    let db = std::env::temp_dir().join(format!("intentd-e2e-git-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(repo_dir.clone())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(repo_dir.clone())))
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Git".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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

    let js = r#"
        const committed = await ws.git.commit('test commit', { files: ['test.txt'], userRequested: true });
        return { committed: committed };
    "#;

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "git bindings e2e" }
        },
        "response": "git ops done",
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
            "E2E Git",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "git ops" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Tool call succeeded (bindings were exercised)
    // Note: The git operations may not persist due to how Services resolves the workspace path

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&repo_dir);
}

/// Attribution-filtered `ws.git.commit` fallback (monorepo#939): an
/// agent-context `ws.file.write` records a `tracked_changes` attribution row,
/// and a subsequent no-`files` `ws.git.commit` commits only the agent's
/// attributed path — a pre-existing unattributed dirty file stays in the
/// worktree. This drives the full ingest → filter loop over the real MCP
/// bridge (the same path idle auto-commit takes).
#[tokio::test]
async fn git_bindings_agent_commit_filters_to_attributed_paths() {
    let Some(script) = gate() else { return };

    if std::process::Command::new("git")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipping git e2e: git not available");
        return;
    }

    let repo_dir = std::env::temp_dir().join(format!("itd-e2e-gitattr-{}", uuid::Uuid::new_v4()));
    let run_git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo_dir)
            .output()
            .unwrap_or_else(|e| panic!("git {} failed to execute: {}", args.join(" "), e));
        assert!(
            out.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).to_string()
    };
    std::fs::create_dir_all(&repo_dir).expect("mkdir repo");
    run_git(&["init"]);
    run_git(&["config", "user.name", "Test"]);
    run_git(&["config", "user.email", "test@test.com"]);
    run_git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo_dir.join("README.md"), "initial").expect("write readme");
    run_git(&["add", "README.md"]);
    run_git(&["commit", "-m", "initial"]);

    // Unattributed dirty file: written outside any agent context.
    std::fs::write(repo_dir.join("unattributed.txt"), "someone else\n").expect("write dirty");

    let db = std::env::temp_dir().join(format!("intentd-e2e-gitattr-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(repo_dir.clone())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(repo_dir.clone())))
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Git Attr".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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

    // The agent writes its own file, then agent-commits with no `files` list:
    // the fallback must pick up only the attributed write.
    let js = r#"
        await ws.file.write('agent-file.txt', 'agent content\n');
        const committed = await ws.git.commit('agent scoped commit');
        return { committed: committed };
    "#;

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "attributed commit e2e" }
        },
        "response": "scoped commit done",
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
            "E2E Git Attr",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "scoped commit" }))
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

    // The ingest path recorded attribution for the agent's write.
    let rows = store.list_tracked_changes(&ws).await.expect("rows");
    let agent_row = rows
        .iter()
        .find(|r| r.path == "agent-file.txt")
        .expect("attribution row for agent-file.txt");
    assert_eq!(agent_row.agent_id.as_deref(), Some(agent_id.as_str()));
    assert_eq!(
        agent_row.stage, "committed",
        "attribution row advanced to committed by the filtered commit"
    );

    // The commit contains only the attributed path.
    let head_files = run_git(&["show", "--name-only", "--pretty=format:", "HEAD"]);
    let head_files: Vec<&str> = head_files
        .lines()
        .filter(|l| !l.trim().is_empty())
        .collect();
    assert_eq!(
        head_files,
        vec!["agent-file.txt"],
        "only the attributed path landed in the commit"
    );
    let head_message = run_git(&["log", "-1", "--pretty=format:%B"]);
    assert!(
        head_message.contains(&format!("Agent-Id: {}", agent_id.as_str())),
        "commit carries the Agent-Id trailer: {head_message}"
    );

    // The unattributed file survives, still dirty.
    assert!(repo_dir.join("unattributed.txt").exists());
    let porcelain = run_git(&["status", "--porcelain"]);
    assert!(
        porcelain.contains("unattributed.txt"),
        "unattributed file still dirty: {porcelain}"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&repo_dir);
}

//
// Note bindings - deepen coverage beyond basic ws.note.add
//

#[tokio::test]
async fn note_bindings_edit_and_edit_lines() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-note-{}.db", uuid::Uuid::new_v4()));
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
                title: "Note for editing".into(),
                content: Some("Line 1\nLine 2 original\nLine 3\nLine 4".into()),
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
            Some("E2E Note".into()),
            None,
            None,
            None,
            None,
            Default::default(),
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
        r#"
        await ws.note.edit('{}', {{ old: 'Line 2 original', new: 'Line 2 edited' }});
        await ws.note.editLines('{}', {{ start: 4, end: 4, content: 'Line 4 edited' }});
        const updated = await ws.note.read('{}');
        return {{ content: updated.content }};
        "#,
        note.id.0, note.id.0, note.id.0
    );

    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "note bindings e2e" }
        },
        "response": "note edited",
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
            "E2E Note",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "edit note" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "agent completed turn (not refusal)"
    );

    // Assert BE state changed
    let updated = services
        .get_note(ws.clone(), note.id.clone())
        .await
        .expect("get note");
    assert!(
        updated.content.contains("Line 2 edited"),
        "edit worked: {}",
        updated.content
    );
    assert!(
        updated.content.contains("Line 4 edited"),
        "editLines worked: {}",
        updated.content
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
