use super::*;

#[intent_test_macros::daemon_test]
async fn provider_profile_project_mcp_and_authenticated_tools_over_wss() {
    let Some(script) = gate("WSS session-mcpServers E2E") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let project = data_dir.join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join(".mcp.json"), r#"{"mcpServers":{"selected":{"command":"echo"},"blocked":{"command":"false"},"workspace-mcp":{"command":"false"}}}"#).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        "[mcp]\ndisabledServers = [\"blocked\"]\n",
    )
    .unwrap();
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    let mut workspace = store
        .get_workspace(&intent_core::WorkspaceId(ws_id.clone()))
        .await
        .unwrap();
    workspace.path = Some(project.to_string_lossy().into_owned());
    workspace.worktree_path = Some(project.to_string_lossy().into_owned());
    store.update_workspace(&workspace).await.unwrap();
    store.close().await;
    let session_log = data_dir.join("profile-sessions.jsonl");
    let js = format!(
        "const listed = await ws.mcp.listServers(); if (!Array.isArray(listed.servers)) throw new Error('missing MCP catalog'); return await ws.note.add({}, {{ content: {} }});",
        json!(note_id),
        json!(MARKER),
    );
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "WSS session-mcp E2E ws.note.add" },
        },
        "response": "added via session/new mcpServers",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_SESSION_MCP", "1"),
        ("MOCK_AGENT_SESSION_LOG", session_log.to_str().unwrap()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon { child };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "note:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-SessionMCP", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please add" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the terminal stream:end; the note:updated event proves the MCP
    // tool call went through the bridge the session/new request delivered.
    let mut ends = 0u32;
    let mut saw_note_updated = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            Some("note:updated") => saw_note_updated = true,
            _ => {}
        }
    }
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
    assert!(
        saw_note_updated,
        "tool's note:updated domain event delivered over WSS"
    );

    // The note mutated — reachable ONLY through the session/new-delivered
    // bridge entry (the mock provider got no --mcp-config in this mode).
    let note = wss_rpc(
        &mut rpc,
        13,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(
        note["note"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(MARKER)
            || note["content"]
                .as_str()
                .unwrap_or_default()
                .contains(MARKER),
        "note mutated via the session/new-delivered workspace-MCP bridge: {note}"
    );
    let records: Vec<Value> = std::fs::read_to_string(&session_log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let names = records[0]["mcpNames"].as_array().unwrap();
    assert_eq!(names.iter().filter(|n| **n == "workspace-mcp").count(), 1);
    assert!(
        names.contains(&json!("selected")),
        "project server: {names:?}"
    );
    assert!(!names.contains(&json!("blocked")), "global deny: {names:?}");
}
