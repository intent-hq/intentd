//! Hermetic ACP E2E: prove that a daemon-spawned agent can rename an untitled
//! workspace over the real MCP tool front door (TS-parity `ws.workspace.setTitle`).
//!
//! We seed a workspace whose `title == id` (the daemon's "still a slug" marker),
//! spawn the deterministic mock ACP agent with a behavior that drives an MCP
//! `set_workspace_title` `tools/call`, and after the turn ends we
//! assert:
//!   1. the workspace row was updated to the human title (persisted via
//!      `update_workspace` → `store.update_workspace`), and
//!   2. a `workspace:updated` event fired carrying the applied delta.
//!
//! The MCP round-trip runs through the intentd `mcp-bridge` proxy (matching the
//! existing `mock_agent_full_turn_with_real_mcp_tool_call` e2e), so this is not
//! an in-process shortcut — it exercises the same wire path production agents
//! use to rename their workspace on the first turn.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::events::WORKSPACE_UPDATED;
use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;

fn slug_workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    // Reference-parity: `create_workspace` seeds `title == id` when the caller
    // omits a title (intent-services/src/lib.rs "seed the title with the derived
    // id" comment). The initial-agent rename flow keys off this equality.
    Workspace {
        id: id.clone(),
        title: id.as_str().to_string(),
        branch: id.as_str().to_string(),
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
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

#[tokio::test]
async fn mock_agent_renames_workspace_via_mcp_set_title_tool() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping mock-agent e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping mock-agent e2e: script not found at {script}");
        return;
    }

    let db = std::env::temp_dir().join(format!("intentd-e2e-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
        .with_event_bus(bus.clone());

    // A fresh workspace whose title still matches its id — the initial-agent
    // rename branch under `set_workspace_title`.
    let ws = WorkspaceId::from_string("amber-forest");
    store
        .insert_workspace(&slug_workspace(&ws))
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    // Mock provider that drives the `set_workspace_title` call
    // via the daemon's mcp-bridge proxy.
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
    // Post-WSAPI-8: the daemon exposes exactly one MCP tool
    // (`workspace_api`); the equivalent of the discrete
    // `set_workspace_title` call is agent-supplied JS driving
    // `ws.workspace.setTitle`.
    let behavior = serde_json::json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": {
                "code": "return await ws.workspace.setTitle('Add dark mode support');",
                "summary": "mock-agent E2E workspace.setTitle"
            }
        },
        "response": "renamed the workspace",
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
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "please rename" }))
            .unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn")
    );

    // Workspace row now carries the human title (title-only; branch rename is
    // deferred until the daemon owns an equivalent rename path).
    let refreshed = services.get_workspace(ws.clone()).await.expect("get ws");
    assert_eq!(refreshed.title, "Add dark mode support");
    assert_eq!(refreshed.branch, "amber-forest", "branch unchanged");

    // The `workspace:updated` event fired with the applied delta so FE headers
    // update live (§6.5 reference-parity emit).
    let events = store.events_by_workspace(&ws, 200).await.expect("events");
    let mut ws_updates = events
        .iter()
        .filter(|e| e.event_type == WORKSPACE_UPDATED)
        .collect::<Vec<_>>();
    assert_eq!(
        ws_updates.len(),
        1,
        "expected one workspace:updated event, got {}",
        ws_updates.len()
    );
    let payload = &ws_updates.pop().unwrap().data;
    assert_eq!(
        payload
            .get("changes")
            .and_then(|c| c.get("title"))
            .and_then(|t| t.as_str()),
        Some("Add dark mode support"),
        "workspace:updated data missing applied title delta: {payload}",
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
