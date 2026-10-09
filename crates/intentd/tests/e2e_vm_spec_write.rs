//! Disposable diagnosis of the reported spec-write/tool-completion boundary.
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services, SubscriptionFilter};
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
        last_content_activity: None,
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
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
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

#[intent_test_macros::daemon_test]
async fn spec_add_task_blocks_returns_tool_result_and_ends_turn() {
    spec_write("add").await;
}

#[intent_test_macros::daemon_test]
async fn spec_set_task_blocks_returns_tool_result_and_ends_turn() {
    spec_write("setContent").await;
}

async fn spec_write(method: &str) {
    let script = gate().expect("mock ACP fixture and node required for diagnosis");
    let db_dir = common::test_tempdir("intentd-vm-spec-write-");
    let store = Store::open(&db_dir.path().join("intentd.db"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
        .with_event_bus(bus.clone());
    services
        .settings_update(serde_json::json!([
            {"path":"workspaceApi.toonOutput","value":false}
        ]))
        .await
        .unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws, None)).await.unwrap();
    services
        .list_notes(&ws)
        .await
        .expect("initialize spec through normal note list");
    let spec = services.get_note(ws.clone(), "spec".into()).await.unwrap();
    let created = services
        .agent_create(
            ws.clone(),
            Some("Spec write control".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .unwrap();
    let agent_id = AgentId::from(created["agent"]["id"].as_str().unwrap());
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
    let content = "# Test specification\n\nWrite and read ordinary **Markdown**.\n\n@@@task key=first\n# First task\nVerify persistence.\n@@@\n\n@@@task key=second dependsOn=first\n# Second task\nVerify response delivery.\n@@@\n";
    let content_json = serde_json::to_string(content).unwrap();
    let write = if method == "add" {
        format!("ws.note.add('spec', {{content:{content_json}}})")
    } else {
        format!("ws.note.setContent('spec', {content_json}, true)")
    };
    let js = format!(
        r"
        const started=Date.now();
        const written=await {write};
        const writeMs=Date.now()-started;
        if(written.convertedCount!==2) throw new Error('conversion count '+JSON.stringify(written));
        const tasks=await ws.note.listTasks('spec');
        const read=await ws.note.read('spec');
        if(read.rawContent.includes('@@@task')) throw new Error('unconverted task fence');
        if(!read.rawContent.includes('intent://local/task/')) throw new Error('missing linked task');
        return {{marker:'spec-write-complete',writeMs,totalMs:Date.now()-started,
            convertedCount:written.convertedCount,content:read.rawContent,tasks}};
    "
    );
    let release_file = db_dir.path().join("invoke-tool");
    let behavior = serde_json::json!({"toolCall":{"name":"workspace_api",
        "arguments":{"code":js,"summary":"Write the test spec with task blocks"}},
        "emitToolBlocks":true,"toolInvocationReleaseFile":release_file,"responseFromToolResultField":"marker"})
    .to_string();
    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".into(), behavior);
    let cwd_dir = common::test_tempdir("intentd-spec-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(&cwd);
    opts.extra_env = extra_env;
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = AgentManager::new(services.clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));
    let startup = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        manager
            .create_agent(
                agent_id.clone(),
                ws.clone(),
                "Spec write control",
                "interactive",
                cwd.clone(),
                &opts,
            )
            .await?;
        manager
            .start_session(&agent_id, cwd.clone(), &provider)
            .await
    })
    .await;
    let session = match startup {
        Ok(Ok(session)) => session,
        failure => {
            let cleanup =
                tokio::time::timeout(std::time::Duration::from_secs(10), manager.shutdown()).await;
            panic!("bounded startup failed: {failure:?}; cleanup: {cleanup:?}");
        }
    };
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![
            "agent:tool:call".into(),
            "agent:stream:end".into(),
            "agent:idle".into(),
        ],
        ..Default::default()
    });
    let block = serde_json::from_value(
        serde_json::json!({"type":"text","text":"Write the test spec with task blocks"}),
    )
    .unwrap();
    let started = std::time::Instant::now();
    // Capture all observations before shutdown, which can itself force idle.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let observe = async {
            let mut seen = Vec::new();
            let mut released = false;
            let mut tool_id = serde_json::Value::Null;
            let mut tool_completed = false;
            let mut ended = false;
            let mut idle = false;
            while !(ended && idle) {
                let batch = events
                    .recv()
                    .await
                    .ok_or_else(|| "event bus closed".to_owned())?;
                for event in batch {
                    if event.workspace_id != ws || event.data["agentId"] != agent_id.0 {
                        continue;
                    }
                    if event.event_type == "agent:tool:call"
                        && event.data["status"] == "started"
                        && !released
                    {
                        let before = services
                            .get_note(ws.clone(), spec.id.clone())
                            .await
                            .map_err(|e| e.to_string())?;
                        if before.content != spec.content {
                            return Err("write occurred before observed open tool".to_owned());
                        }
                        tokio::fs::write(&release_file, b"invoke")
                            .await
                            .map_err(|e| e.to_string())?;
                        released = true;
                        tool_id = event.data["toolCallId"].clone();
                    }
                    if released
                        && event.event_type == "agent:tool:call"
                        && event.data["toolCallId"] == tool_id
                        && event.data["status"] == "completed"
                    {
                        tool_completed = true;
                    }
                    // This fixture creates one agent/session/turn and subscribes
                    // after startup. Terminal frames must follow its exact tool.
                    if event.event_type == "agent:stream:end" || event.event_type == "agent:idle" {
                        if !tool_completed {
                            return Err("terminal event before current tool completed".into());
                        }
                        ended |= event.event_type == "agent:stream:end";
                        idle |= event.event_type == "agent:idle";
                    }
                    seen.push(event);
                }
            }
            Ok::<_, String>(seen)
        };
        let (turn, observed) = tokio::join!(
            manager.run_turn(&agent_id, &ws, &session, vec![block], None),
            observe
        );
        let transcript = services
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
            .await;
        let naturally_idle = !manager.list_busy().iter().any(|(id, _)| id == &agent_id);
        (turn, observed, transcript, naturally_idle)
    })
    .await;
    eprintln!(
        "VM_SPEC_CONTROL method={method} turn_elapsed_ms={} pre_cleanup={outcome:?}",
        started.elapsed().as_millis()
    );
    let cleanup =
        tokio::time::timeout(std::time::Duration::from_secs(10), manager.shutdown()).await;
    assert!(
        cleanup.is_ok(),
        "cleanup failed: {cleanup:?}; original outcome: {outcome:?}"
    );
    let (turn, observed, transcript, naturally_idle) =
        outcome.expect("turn and pre-cleanup events must settle");
    assert!(naturally_idle, "agent remained busy before shutdown");
    assert_eq!(
        serde_json::to_value(turn.expect("turn result")).unwrap(),
        serde_json::json!("end_turn")
    );
    let observed = observed.expect("pre-cleanup lifecycle");
    let open = observed
        .iter()
        .position(|e| e.event_type == "agent:tool:call" && e.data["status"] == "started")
        .expect("tool open");
    let complete = observed
        .iter()
        .position(|e| e.event_type == "agent:tool:call" && e.data["status"] == "completed")
        .expect("tool completed");
    let end = observed
        .iter()
        .position(|e| e.event_type == "agent:stream:end")
        .expect("stream end");
    let idle = observed
        .iter()
        .position(|e| e.event_type == "agent:idle")
        .expect("idle");
    assert!(open < complete && complete < end && complete < idle);
    assert!(observed[open].data["toolCallId"].is_string());
    assert_eq!(
        observed[open].data["toolCallId"],
        observed[complete].data["toolCallId"]
    );
    assert!(
        observed[end].data.get("finishReason").is_none(),
        "normal stream ending"
    );
    assert_eq!(observed[idle].data["finishReason"], "end_turn");
    assert_eq!(observed[idle].data["reason"], "stream_complete");
    assert_eq!(observed[idle].data["status"], "idle");
    let message_id = observed[end].data["messageId"]
        .as_str()
        .expect("terminal message identity");
    let transcript = transcript.expect("transcript before cleanup");
    let saved = services
        .get_note(ws.clone(), spec.id.clone())
        .await
        .unwrap();
    assert!(!saved.content.contains("@@@task"));
    assert_eq!(saved.content.matches("intent://local/task/").count(), 2);
    let children = store
        .list_notes(&ws)
        .await
        .unwrap()
        .into_iter()
        .filter(|n| n.parent_id.as_ref() == Some(&spec.id))
        .count();
    assert_eq!(children, 2);
    let messages = transcript["messages"].as_array().unwrap();
    assert!(
        messages.iter().any(|message| message["id"] == message_id
            && message["role"] == "assistant"
            && message["contentBlocks"]
                .as_array()
                .is_some_and(|blocks| blocks.iter().any(
                    |block| block["type"] == "text" && block["text"] == "spec-write-complete"
                ))),
        "assistant final marker persisted before cleanup: {transcript}"
    );
    let output = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter(|b| b["type"] == "tool_result")
        .filter_map(|b| b["output"].as_array())
        .flatten()
        .find_map(|item| item["text"].as_str())
        .expect("persisted tool response");
    let result: serde_json::Value = serde_json::from_str(output).unwrap();
    assert_eq!(result["marker"], "spec-write-complete");
    assert_eq!(result["content"], saved.content);
    eprintln!("VM_SPEC_CONTROL method={method} tool_result={result}");
    let page = services
        .get_note_page(
            ws,
            spec.id,
            serde_json::from_value(serde_json::json!({"kind":"source","at":0})).unwrap(),
            serde_json::json!(1),
        )
        .await
        .unwrap();
    eprintln!("VM_SPEC_CONTROL method={method} source_page={page}");
    assert_eq!(page["text"], saved.content);
}
