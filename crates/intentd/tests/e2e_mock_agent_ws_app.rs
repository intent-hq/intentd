//! E2E coverage for chief-gated ws.app.* surface via mock ACP agent.
//!
//! Drives real MCP tool invocations through the workspace_api tool with the
//! mock-acp-agent.mjs fixture, asserting:
//! - ws.app.workspaces.list returns user workspaces (never __chief__)
//! - ws.app.agents.list returns agent metadata
//! - ws.app.proposal.show persists application/vnd.intent.proposal+json resource
//! - Non-chief workspace agents are gated from ws.app.*
//!
//! Pattern: reuses the harness from e2e_mock_agent_workspace_api_bindings.rs.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;
use serde_json::json;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>, title: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: title.to_string(),
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
        execution_environment: None,
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
        eprintln!("skipping ws.app e2e: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping ws.app e2e: script missing at {script}");
        return None;
    }
    Some(script)
}

/// Gap 1 (P2): Chief-workspace agent calls ws.app.workspaces.list via MCP and
/// receives 2+ seeded user workspaces (never __chief__).
#[tokio::test]
async fn chief_agent_ws_app_workspaces_list() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!("intentd-e2e-ws-app-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());
    // Pin `workspaceApi.toonOutput` off so the workspace_api tool body stays
    // plain JSON for the serde_json assertions below (TOON is on by default).
    services
        .settings_update(json!([{ "path": "workspaceApi.toonOutput", "value": false }]))
        .await
        .expect("disable toonOutput");

    // Seed 2+ user workspaces
    let ws1 = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws1, None, "Amber Forest"))
        .await
        .expect("insert ws1");

    let ws2 = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws2, None, "Indigo Valley"))
        .await
        .expect("insert ws2");

    // Create a chief-workspace agent
    let chief_ws = WorkspaceId(CHIEF_WORKSPACE_ID.to_string());
    let agent_val = services
        .agent_create(
            chief_ws.clone(),
            Some("Chief E2E".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create chief agent");
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

    // Call ws.app.workspaces.list via MCP
    let js = r#"
        const result = await ws.app.workspaces.list({});
        return { workspaces: result };
    "#;

    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "ws.app.workspaces.list e2e" }
        },
        "response": "list workspaces",
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
            agent_id.clone(),
            chief_ws.clone(),
            "Chief E2E",
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
        serde_json::from_value(json!({ "type": "text", "text": "list workspaces" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &chief_ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // Assert the persisted tool output contains the seeded workspaces and __chief__ is excluded
    let transcript = services
        .agent_get_conversation(agent_id.clone(), None, Some(chief_ws.clone()), None, None)
        .await
        .expect("get conversation");
    let messages = transcript["messages"].as_array().expect("messages array");
    let tool_outputs: Vec<_> = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter_map(|b| {
            if b["type"] == "tool_result" {
                // output is an array of MCP content blocks; extract the first text block
                b["output"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|item| item["text"].as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(
        !tool_outputs.is_empty(),
        "Expected tool result blocks in transcript"
    );
    let last_output = tool_outputs.last().expect("tool outputs");
    let output_json: serde_json::Value =
        serde_json::from_str(last_output).expect("tool output should be JSON");
    let workspaces = output_json["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(
        workspaces
            .iter()
            .any(|w| w["title"] == json!("Amber Forest")),
        "Expected seeded 'Amber Forest' workspace"
    );
    assert!(
        workspaces
            .iter()
            .any(|w| w["title"] == json!("Indigo Valley")),
        "Expected seeded 'Indigo Valley' workspace"
    );
    assert!(
        !workspaces
            .iter()
            .any(|w| w["id"] == json!(CHIEF_WORKSPACE_ID)),
        "Expected __chief__ to be excluded from list"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// Gap 2 (P2): Chief-workspace agent calls ws.app.proposal.show and the
/// persisted transcript contains the application/vnd.intent.proposal+json
/// resource content item.
#[tokio::test]
async fn chief_agent_ws_app_proposal_resource_persisted() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-ws-app-prop-{}.db",
        uuid::Uuid::new_v4()
    ));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());

    // Create a chief-workspace agent
    let chief_ws = WorkspaceId(CHIEF_WORKSPACE_ID.to_string());
    let agent_val = services
        .agent_create(
            chief_ws.clone(),
            Some("Chief Proposal E2E".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create chief agent");
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

    // Call ws.app.proposal.show via MCP
    let js = r#"
        const proposal = {
            kind: "settings-change",
            payload: { key: "test.setting", value: "new-value" },
            preview: {
                title: "Update Test Setting",
                description: "Change test setting to new value"
            }
        };
        const result = await ws.app.proposal.show(proposal);
        return result;
    "#;

    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "ws.app.proposal.show e2e" }
        },
        "response": "show proposal",
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
            agent_id.clone(),
            chief_ws.clone(),
            "Chief Proposal E2E",
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
        serde_json::from_value(json!({ "type": "text", "text": "show proposal" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &chief_ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // Verify the proposal resource block is persisted in the transcript.
    // ws.app.proposal.show returns MCP content items: [text, resource].
    // The mock agent emits these via tool_call_update.rawOutput, which the daemon
    // stores in the tool_result block's `output` array.
    let conversation = services
        .agent_get_conversation(agent_id.clone(), None, Some(chief_ws.clone()), None, None)
        .await
        .expect("read conversation");
    let messages = conversation["messages"].as_array().expect("messages array");

    let has_proposal_resource = messages.iter().any(|msg| {
        if let Some(blocks) = msg["contentBlocks"].as_array() {
            blocks.iter().any(|block| {
                // The resource is nested in tool_result.output[N].resource
                if block["type"] == "tool_result" {
                    if let Some(output) = block["output"].as_array() {
                        return output.iter().any(|item| {
                            item["type"] == "resource"
                                && item["resource"]["mimeType"]
                                    == "application/vnd.intent.proposal+json"
                        });
                    }
                }
                false
            })
        } else {
            false
        }
    });
    assert!(
        has_proposal_resource,
        "Proposal resource not found in persisted transcript: {}",
        serde_json::to_string_pretty(&conversation).unwrap()
    );

    // §7.1: the proposal resource is ALSO lifted into its own standalone
    // top-level block right after the tool_result, so the FE can render a
    // ProposalCard without digging through tool output.
    let has_standalone_proposal_block = messages.iter().any(|msg| {
        msg["contentBlocks"].as_array().is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "resource"
                    && block["resource"]["mimeType"] == "application/vnd.intent.proposal+json"
                    && block["id"].is_string()
            })
        })
    });
    assert!(
        has_standalone_proposal_block,
        "Standalone proposal resource block not found in persisted transcript: {}",
        serde_json::to_string_pretty(&conversation).unwrap()
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// intent-hq/monorepo#511 regression class: a provider that collapses the MCP
/// content items into `{ output: "<stringified {ok, proposal}>" }` (auggie's
/// shape — the resource item is dropped entirely) still yields the standalone
/// proposal-resource block in the persisted transcript via the fallback lift.
#[tokio::test]
async fn chief_agent_ws_app_proposal_lifted_from_collapsed_output() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-ws-app-collapse-{}.db",
        uuid::Uuid::new_v4()
    ));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());

    // Create a chief-workspace agent
    let chief_ws = WorkspaceId(CHIEF_WORKSPACE_ID.to_string());
    let agent_val = services
        .agent_create(
            chief_ws.clone(),
            Some("Chief Collapse E2E".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create chief agent");
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

    // Call ws.app.proposal.show via MCP; the mock collapses the tool output.
    let js = r#"
        const proposal = {
            kind: "settings-change",
            payload: { key: "test.setting", value: "new-value" },
            preview: {
                title: "Update Test Setting",
                description: "Change test setting to new value"
            }
        };
        const result = await ws.app.proposal.show(proposal);
        return result;
    "#;

    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "ws.app.proposal.show collapsed e2e" }
        },
        "response": "show proposal",
        "emitToolBlocks": true,
        "collapseToolOutput": true,
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
            chief_ws.clone(),
            "Chief Collapse E2E",
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
        serde_json::from_value(json!({ "type": "text", "text": "show proposal" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &chief_ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let conversation = services
        .agent_get_conversation(agent_id.clone(), None, Some(chief_ws.clone()), None, None)
        .await
        .expect("read conversation");
    let messages = conversation["messages"].as_array().expect("messages array");

    // The collapsed tool_result carries NO resource item — the output is the
    // provider-flattened `{ output: "<string>" }` object.
    let collapsed_result_present = messages.iter().any(|msg| {
        msg["contentBlocks"].as_array().is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "tool_result" && block["output"]["output"].is_string()
            })
        })
    });
    assert!(
        collapsed_result_present,
        "Expected the collapsed tool_result shape in transcript: {}",
        serde_json::to_string_pretty(&conversation).unwrap()
    );

    // §7.1 fallback: the standalone proposal-resource block is still lifted,
    // rebuilt from the daemon's own {ok, proposal} payload.
    let standalone = messages.iter().find_map(|msg| {
        msg["contentBlocks"].as_array().and_then(|blocks| {
            blocks.iter().find(|block| {
                block["type"] == "resource"
                    && block["resource"]["mimeType"] == "application/vnd.intent.proposal+json"
                    && block["id"].is_string()
            })
        })
    });
    let standalone = standalone.unwrap_or_else(|| {
        panic!(
            "Standalone proposal resource block not lifted from collapsed output: {}",
            serde_json::to_string_pretty(&conversation).unwrap()
        )
    });
    assert_eq!(standalone["resource"]["name"], "Update Test Setting");
    assert_eq!(
        standalone["resource"]["uri"],
        "intent-proposal://settings-change/Update%20Test%20Setting"
    );
    let text = standalone["resource"]["text"].as_str().expect("text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("proposal text parses");
    assert_eq!(parsed["kind"], "settings-change");
    assert_eq!(parsed["payload"]["key"], "test.setting");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// §7.1 deterministic attach: a provider whose tool echo is GARBLED beyond
/// repair (collapsed, truncated, and corrupted — neither the resource item
/// nor a parseable `{ok, proposal}` payload survives, so both the array path
/// and the collapsed-output lift fail) still yields the standalone
/// proposal-resource block in the persisted transcript, because
/// `ws.app.proposal.show`'s dispatch registered the canonical payload in the
/// turn-attachment registry in-process and the transcript writer claims it
/// when the tool call completes.
#[tokio::test]
async fn chief_agent_ws_app_proposal_attached_from_garbled_output() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-ws-app-garble-{}.db",
        uuid::Uuid::new_v4()
    ));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());

    let chief_ws = WorkspaceId(CHIEF_WORKSPACE_ID.to_string());
    let agent_val = services
        .agent_create(
            chief_ws.clone(),
            Some("Chief Garble E2E".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create chief agent");
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

    // Call ws.app.proposal.show via MCP; the mock garbles the tool output.
    let js = r#"
        const proposal = {
            kind: "settings-change",
            payload: { key: "test.setting", value: "new-value" },
            preview: {
                title: "Update Test Setting",
                description: "Change test setting to new value"
            }
        };
        const result = await ws.app.proposal.show(proposal);
        return result;
    "#;

    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "ws.app.proposal.show garbled e2e" }
        },
        "response": "show proposal",
        "emitToolBlocks": true,
        "garbleToolOutput": true,
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
            chief_ws.clone(),
            "Chief Garble E2E",
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
        serde_json::from_value(json!({ "type": "text", "text": "show proposal" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &chief_ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let conversation = services
        .agent_get_conversation(agent_id.clone(), None, Some(chief_ws.clone()), None, None)
        .await
        .expect("read conversation");
    let messages = conversation["messages"].as_array().expect("messages array");

    // The garbled tool_result is preserved as echoed — truncated/corrupted,
    // NOT a parseable {ok, proposal} payload and NOT a content-item array.
    let garbled_result_present = messages.iter().any(|msg| {
        msg["contentBlocks"].as_array().is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "tool_result"
                    && block["output"]["output"]
                        .as_str()
                        .is_some_and(|s| s.starts_with("[tool ran]"))
            })
        })
    });
    assert!(
        garbled_result_present,
        "Expected the garbled tool_result shape in transcript: {}",
        serde_json::to_string_pretty(&conversation).unwrap()
    );

    // Deterministic attach: the standalone proposal-resource block is still
    // present, carrying the CANONICAL payload from the registry (not the
    // garbled echo).
    let standalone = messages.iter().find_map(|msg| {
        msg["contentBlocks"].as_array().and_then(|blocks| {
            blocks.iter().find(|block| {
                block["type"] == "resource"
                    && block["resource"]["mimeType"] == "application/vnd.intent.proposal+json"
                    && block["id"].is_string()
            })
        })
    });
    let standalone = standalone.unwrap_or_else(|| {
        panic!(
            "Standalone proposal resource block not attached from garbled output: {}",
            serde_json::to_string_pretty(&conversation).unwrap()
        )
    });
    assert_eq!(standalone["resource"]["name"], "Update Test Setting");
    assert_eq!(
        standalone["resource"]["uri"],
        "intent-proposal://settings-change/Update%20Test%20Setting"
    );
    let text = standalone["resource"]["text"].as_str().expect("text");
    let parsed: serde_json::Value = serde_json::from_str(text).expect("proposal text parses");
    assert_eq!(parsed["kind"], "settings-change");
    assert_eq!(parsed["payload"]["key"], "test.setting");
    assert_eq!(parsed["payload"]["value"], "new-value");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}

/// Gap 3 (P3): Non-chief workspace agent calls ws.app.* and receives the
/// gating error through the MCP tool result.
#[tokio::test]
async fn non_chief_agent_ws_app_gating_error() {
    let Some(script) = gate() else { return };

    let db = std::env::temp_dir().join(format!(
        "intentd-e2e-ws-app-gate-{}.db",
        uuid::Uuid::new_v4()
    ));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());
    // Pin `workspaceApi.toonOutput` off so the workspace_api tool body stays
    // plain JSON for the serde_json assertions below (TOON is on by default).
    services
        .settings_update(json!([{ "path": "workspaceApi.toonOutput", "value": false }]))
        .await
        .expect("disable toonOutput");

    // Create a regular (non-chief) workspace
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, None, "Regular Workspace"))
        .await
        .expect("insert ws");

    // Create an agent in the non-chief workspace
    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("Regular Agent".into()),
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

    // Try to call ws.app.workspaces.list from non-chief workspace
    let js = r#"
        try {
            const result = await ws.app.workspaces.list({});
            return { success: true, result };
        } catch (error) {
            return { success: false, error: error.message };
        }
    "#;

    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "ws.app gating e2e" }
        },
        "response": "gating check",
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
            agent_id.clone(),
            ws.clone(),
            "Regular Agent",
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
        serde_json::from_value(json!({ "type": "text", "text": "test gating" })).unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // Assert the persisted tool output contains success: false and the gating error string
    let transcript = services
        .agent_get_conversation(agent_id.clone(), None, Some(ws.clone()), None, None)
        .await
        .expect("get conversation");
    let messages = transcript["messages"].as_array().expect("messages array");
    let tool_outputs: Vec<_> = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter_map(|b| {
            if b["type"] == "tool_result" {
                // output is an array of MCP content blocks; extract the first text block
                b["output"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|item| item["text"].as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(
        !tool_outputs.is_empty(),
        "Expected tool result blocks in transcript"
    );
    let last_output = tool_outputs.last().expect("tool outputs");
    let output_json: serde_json::Value =
        serde_json::from_str(last_output).expect("tool output should be JSON");
    assert_eq!(
        output_json["success"],
        json!(false),
        "Expected success: false in gating error"
    );
    let error_msg = output_json["error"]
        .as_str()
        .expect("error should be a string");
    assert!(
        error_msg.contains("ws.app.* is only available in the Chief of Staff workspace"),
        "Expected gating error message in tool output, got: {}",
        error_msg
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
}
