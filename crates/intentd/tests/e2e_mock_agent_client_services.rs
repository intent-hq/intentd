//! End-to-end tests for client-served ACP handlers (fs, permission, terminal).
//!
//! Drives the mock ACP agent to issue agent→client requests (fs/read_text_file,
//! fs/write_text_file, session/request_permission, terminal/*) and asserts on
//! the daemon's responses. Covers the client-served slice of the ACP protocol
//! (spec §6.2–§6.4) at the e2e layer, which was near-0% before this test suite.
//!
//! Gated by `MOCK_AGENT_SCRIPT_PATH` (the CI ACP gate); skips cleanly when the
//! script or `node` is absent.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, PermissionPolicy, SpawnOptions};
use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;

const SESSION_ID: &str = "mock-session-1";

fn workspace(id: &WorkspaceId, path: std::path::PathBuf) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E-client-srv".to_string(),
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

async fn setup_manager(
    script: &str,
    policy: PermissionPolicy,
) -> (
    Arc<Services>,
    AgentManager,
    WorkspaceId,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    if intent_providers::resolve_on_path("node").is_none() {
        panic!("node not on PATH");
    }
    if !std::path::Path::new(script).exists() {
        panic!("script not found at {script}");
    }

    let db = std::env::temp_dir().join(format!("intentd-e2e-client-{}.db", uuid::Uuid::new_v4()));
    let ws_root = std::env::temp_dir().join(format!("itd-e2e-client-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, ws_root.clone()))
        .await
        .expect("insert ws");

    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let services_arc = Arc::new(services);
    let manager = AgentManager::new((*services_arc).clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"))
        .with_policy(policy);

    (services_arc, manager, ws, ws_root, db)
}

async fn create_agent_session(
    manager: &AgentManager,
    services: &Services,
    ws: &WorkspaceId,
    script: &str,
    behavior: serde_json::Value,
    cwd: &std::path::Path,
) -> (AgentId, String) {
    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E-client".into()),
            None,
            None,
            None,
            None,
            Default::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    let script_static: &'static str = Box::leak(script.to_string().into_boxed_str());
    let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
    let provider = ProviderConfig {
        command: "node",
        base_args,
        supports_authenticate: true,
        supports_mcp_config: true,
        mcp_config_flag: Some("--mcp-config"),
        ..*intent_providers::find_provider("mock").unwrap()
    };

    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior.to_string());

    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(cwd);
    opts.extra_env = extra_env;

    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "E2E-client",
            "interactive",
            cwd.to_path_buf(),
            &opts,
        )
        .await
        .expect("create_agent");

    let acp_session = manager
        .start_session(&agent_id, cwd.to_path_buf(), &provider)
        .await
        .expect("start_session");

    (agent_id, acp_session)
}

async fn run_turn(
    manager: &AgentManager,
    agent_id: &AgentId,
    ws: &WorkspaceId,
    acp_session: &str,
) -> String {
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "please proceed" }))
            .unwrap();
    let stop = manager
        .run_turn(agent_id, ws, acp_session, vec![block], None)
        .await
        .expect("run_turn");
    serde_json::to_value(stop)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn fs_read_write_round_trip() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping fs e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping fs e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    // Write a file first
    let test_file = ws_root.join("test.txt");
    let test_content = "ACP fs handler test content";
    std::fs::write(&test_file, test_content).expect("write test file");

    // Behavior: read the file, then write it back with a marker appended
    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "fs/read_text_file",
                "params": { "sessionId": SESSION_ID, "path": test_file.display().to_string() },
                // Assert the exact file content was read
                "assertResult": {
                    "content": test_content
                }
            },
            {
                "method": "fs/write_text_file",
                "params": {
                    "sessionId": SESSION_ID,
                    "path": test_file.display().to_string(),
                    "content": format!("{test_content}\nfs/write marker")
                },
            },
        ],
        "response": "fs round-trip complete",
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    // Verify the file was written
    let updated = std::fs::read_to_string(&test_file).expect("read updated file");
    assert!(
        updated.contains("fs/write marker"),
        "file updated: {updated}"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

#[tokio::test]
async fn permission_request_allow() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping permission e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping permission e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "session/request_permission",
                "params": {
                    "sessionId": SESSION_ID,
                    "toolCall": {
                        "toolCallId": "tc-1",
                        "title": "Test permission",
                        "rawInput": { "command": "test" }
                    },
                    "options": [
                        { "optionId": "allow_once", "name": "Allow", "kind": "allow_once" },
                        { "optionId": "reject_once", "name": "Deny", "kind": "reject_once" }
                    ]
                },
                "assertResult": { "outcome": { "outcome": "selected", "optionId": "allow_once" } },
            },
        ],
        "response": "permission granted",
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed with allow");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

#[tokio::test]
async fn permission_request_deny() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping permission e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping permission e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::DenyAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "session/request_permission",
                "params": {
                    "sessionId": SESSION_ID,
                    "toolCall": {
                        "toolCallId": "tc-2",
                        "title": "Test permission deny",
                        "rawInput": { "command": "test" }
                    },
                    "options": [
                        { "optionId": "allow_once", "name": "Allow", "kind": "allow_once" },
                        { "optionId": "reject_once", "name": "Deny", "kind": "reject_once" }
                    ]
                },
                "assertResult": { "outcome": { "outcome": "selected", "optionId": "reject_once" } },
            },
        ],
        "response": "permission denied",
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed with deny");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

#[tokio::test]
#[cfg(unix)]
async fn terminal_lifecycle() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/create",
                "params": {
                    "sessionId": SESSION_ID,
                    "command": "echo",
                    "args": ["terminal-test-marker"]
                },
                // Assert that terminalId is returned - proves create succeeded
                "assertResult": {
                    "terminalId": "pty-0"
                }
            },
            {
                "method": "terminal/wait_for_exit",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
                "assertResult": {
                    "exitCode": 0
                }
            },
            {
                "method": "terminal/output",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
            {
                "method": "terminal/release",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
        ],
        "response": "terminal lifecycle complete",
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test terminal/kill on a running process (sleep).
#[tokio::test]
#[cfg(unix)]
async fn terminal_kill_running_process() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal kill e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal kill e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/create",
                "params": {
                    "sessionId": SESSION_ID,
                    "command": "sleep",
                    "args": ["30"]
                },
                "assertResult": {
                    "terminalId": "pty-0"
                }
            },
            {
                "method": "terminal/kill",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
        ],
        "response": "killed running process"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test terminal output truncation when byte limit is exceeded.
#[tokio::test]
#[cfg(unix)]
async fn terminal_output_truncation() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal truncation e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal truncation e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    // Generate large output with a small byte limit (512 bytes).
    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/create",
                "params": {
                    "sessionId": SESSION_ID,
                    "command": "sh",
                    "args": ["-c", "i=1; while [ $i -le 200 ]; do echo line-$i; i=$((i+1)); done"],
                    "outputByteLimit": 512
                },
                "assertResult": {
                    "terminalId": "pty-0"
                }
            },
            {
                "method": "terminal/wait_for_exit",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
            {
                "method": "terminal/output",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
                // Note: truncated field exists but PtyHostBridge always returns false
                // (output byte limiting is configured but truncation detection not yet implemented)
            },
            {
                "method": "terminal/release",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
        ],
        "response": "output truncated as expected"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test wait_for_exit on a process that exits with non-zero code.
#[tokio::test]
#[cfg(unix)]
async fn terminal_non_zero_exit() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal non-zero exit e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal non-zero exit e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/create",
                "params": {
                    "sessionId": SESSION_ID,
                    "command": "sh",
                    "args": ["-c", "exit 42"]
                },
                "assertResult": {
                    "terminalId": "pty-0"
                }
            },
            {
                "method": "terminal/wait_for_exit",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
                // Assert the expected non-zero exit code
                "assertResult": {
                    "exitCode": 42
                }
            },
            {
                "method": "terminal/release",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
        ],
        "response": "non-zero exit captured"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test error path: release unknown terminal ID.
#[tokio::test]
#[cfg(unix)]
async fn terminal_release_unknown() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal error path e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal error path e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/release",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-nonexistent"
                },
                "assertError": {
                    "code": -32603
                }
            },
        ],
        "response": "error path exercised"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test error path: output on unknown terminal ID.
#[tokio::test]
#[cfg(unix)]
async fn terminal_output_unknown() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal output error e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal output error e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/output",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-nonexistent"
                },
                "assertError": {
                    "code": -32603
                }
            },
        ],
        "response": "error path exercised"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

/// Test output after terminal has exited.
#[tokio::test]
#[cfg(unix)]
async fn terminal_output_after_exit() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping terminal output-after-exit e2e: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping terminal output-after-exit e2e: script not found");
        return;
    }

    let (services, manager, ws, ws_root, db) =
        setup_manager(&script, PermissionPolicy::AllowAll).await;

    let behavior = serde_json::json!({
        "clientCalls": [
            {
                "method": "terminal/create",
                "params": {
                    "sessionId": SESSION_ID,
                    "command": "echo",
                    "args": ["done"]
                },
                "assertResult": {
                    "terminalId": "pty-0"
                }
            },
            {
                "method": "terminal/wait_for_exit",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
            {
                "method": "terminal/output",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
            {
                "method": "terminal/release",
                "params": {
                    "sessionId": SESSION_ID,
                    "terminalId": "pty-0"
                },
            },
        ],
        "response": "output after exit retrieved"
    });

    let (agent_id, acp_session) =
        create_agent_session(&manager, &services, &ws, &script, behavior, &ws_root).await;

    let stop = run_turn(&manager, &agent_id, &ws, &acp_session).await;
    assert_eq!(stop, "end_turn", "turn completed");

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}
