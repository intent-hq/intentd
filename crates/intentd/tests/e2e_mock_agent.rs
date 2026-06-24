//! Hermetic ACP E2E (spec §13.2 / §13.4 Phase 3): drive the mock agent through a
//! FULL turn whose work is a REAL spawned-child agent→BE MCP tool call.
//!
//! The mock node child reaches the in-process [`WorkspaceMcpServer`] over the
//! generated `--mcp-config` (the `intentd mcp-bridge` proxy → per-agent loopback
//! listener), mutates BE state via `add_to_note`, and the turn streams chunks and
//! ends once. We assert the note changed, the conversation persisted, and exactly
//! one `agent:stream:end` fired — NOT an in-process `handle_message` shortcut.
//!
//! Gated by `MOCK_AGENT_SCRIPT_PATH` (the CI ACP gate); skips cleanly when the
//! script or `node` is absent.

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::events::{AGENT_STREAM_CHUNK, AGENT_STREAM_END};
use intent_core::{
    now_iso, AgentId, NoteCreate, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;

const MARKER: &str = "MCP_TOOL_MARKER_e2e";

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E".to_string(),
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

#[tokio::test]
async fn mock_agent_full_turn_with_real_mcp_tool_call() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping mock-agent E2E: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping mock-agent E2E: script not found at {script}");
        return;
    }

    let db = std::env::temp_dir().join(format!("intentd-e2e-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws))
        .await
        .expect("insert ws");
    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Target".into(),
                content: Some("# Target\n".into()),
                tags: None,
                parent_id: None,
            },
        )
        .await
        .expect("create note");
    let agent_val = services
        .agent_create(ws.clone(), Some("E2E".into()), None, None, None)
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // A mock provider config that runs `node <script> --mcp-config <file>` and
    // performs the agent→BE tool call. `'static` leaks are fine in a test.
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
    let behavior = serde_json::json!({
        "toolCall": { "name": "add_to_note_workspace-mcp", "arguments": { "noteId": note.id.0, "content": MARKER } },
        "response": "added via mcp",
    })
    .to_string();
    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
    let cwd = std::env::temp_dir();
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
            "E2E",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "please add" }))
            .unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block])
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn")
    );

    // BE state changed via the real MCP tool call.
    let updated = services
        .get_note(ws.clone(), note.id.clone())
        .await
        .expect("get note");
    assert!(
        updated.content.contains(MARKER),
        "note mutated by MCP tool call: {}",
        updated.content
    );

    // Conversation persisted (one assistant message from the streamed chunk).
    let session = store.get_agent_session(&agent_id).await.expect("session");
    assert!(
        session.messages.iter().any(|m| m.role == "assistant"),
        "assistant message persisted"
    );

    // Exactly one terminal stream:end, plus at least one streamed chunk.
    let events = store.events_by_workspace(&ws, 200).await.expect("events");
    let ends = events
        .iter()
        .filter(|e| e.event_type == AGENT_STREAM_END)
        .count();
    let chunks = events
        .iter()
        .filter(|e| e.event_type == AGENT_STREAM_CHUNK)
        .count();
    assert_eq!(ends, 1, "exactly one stream:end per turn");
    assert!(chunks >= 1, "at least one streamed chunk");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
