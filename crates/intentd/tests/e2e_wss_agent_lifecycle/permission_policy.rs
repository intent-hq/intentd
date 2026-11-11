//! Per-agent permission policy (PROTOCOL §8 "Per-agent permission policy")
//! over authenticated, pinned WSS against the real mock ACP agent.
use super::*;

/// Wait for `agent:idle` of `agent_id`, failing on `agent:failed`. Returns
/// every `agent:permission:request` seen for the agent on the way.
async fn await_idle<S>(sub: &mut WebSocketStream<S>, agent_id: &str) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut prompts = Vec::new();
    loop {
        let frame = wss_event(sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"] != agent_id && ev["data"]["sessionId"] != agent_id {
            continue;
        }
        assert_ne!(ev["type"], "agent:failed", "{ev}");
        if ev["type"] == "agent:permission:request" {
            prompts.push(ev["data"].clone());
        }
        if ev["type"] == "agent:idle" {
            return prompts;
        }
    }
}

fn spawned_pids(log: &Path) -> Vec<u64> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|entry| entry["pid"].as_u64())
        .collect()
}

/// The daemon default (`allow`) auto-answers prompts; `agent.update
/// { permissionPolicy: "interactive" }` on one agent makes its next turn
/// restart the live child and block on the prompt until a client answers
/// it, and a same-policy turn reuses that child. Covers the `AgentLite`
/// fields, the canonical `agent:updated` value, `updatedAt` staying put,
/// `-32602` validation, `null` restoring the default, and the live
/// `agents.permissionPolicy` setting as the default under a chat override.
#[tokio::test]
async fn agent_permission_policy_applies_on_next_turn_over_wss() {
    let Some(script) = gate("WSS permission policy E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let ws_id = seed_workspace_only(&data_dir).await;
    let session_log = data_dir.join("mock-sessions.jsonl");
    let behavior = json!({"clientCalls":[{"method":"session/request_permission","params":{
        "sessionId":"mock-session","toolCall":{"toolCallId":"write","title":"Write file"},
        "options":[{"optionId":"allow_once","name":"Allow","kind":"allow_once"},
                   {"optionId":"reject_once","name":"Deny","kind":"reject_once"}]},
        "assertResult":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}],
        "response":"Done."})
    .to_string();
    let session_log_str = session_log.to_string_lossy().into_owned();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_SESSION_LOG", session_log_str.as_str()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());

    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "provider": "mock", "model": "default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();
    assert!(
        created["agent"]["permissionPolicy"].is_null()
            && created["agent"].get("permissionPolicy").is_some(),
        "override is present and null: {created}"
    );
    assert_eq!(created["agent"]["effectivePermissionPolicy"], "allow");

    for (id, bad) in [(3, json!("ask")), (4, json!(true))] {
        let rejected = wss_rpc_envelope(
            &mut rpc,
            id,
            "agent.update",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": bad } }),
        )
        .await;
        assert_eq!(rejected["error"]["code"], -32602, "{rejected}");
    }

    // Before any turn nothing else writes the row, so updatedAt is stable.
    let denied = wss_rpc(
        &mut rpc,
        40,
        "agent.update",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": "deny" } }),
    )
    .await;
    assert_eq!(denied["agent"]["effectivePermissionPolicy"], "deny", "{denied}");
    assert_eq!(
        denied["agent"]["updatedAt"], created["agent"]["updatedAt"],
        "a policy change is not activity"
    );
    let cleared = wss_rpc(
        &mut rpc,
        41,
        "agent.update",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": null } }),
    )
    .await;
    assert!(cleared["agent"]["permissionPolicy"].is_null(), "{cleared}");
    assert_eq!(cleared["agent"]["effectivePermissionPolicy"], "allow");

    // Default policy: the prompt is answered without a client.
    wss_rpc(
        &mut rpc,
        5,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    let prompts = await_idle(&mut sub, &agent_id).await;
    assert_eq!(prompts.len(), 1, "the default still reports the prompt");
    let first_pids = spawned_pids(&session_log);
    assert_eq!(first_pids.len(), 1, "one child: {first_pids:?}");

    let updated = wss_rpc(
        &mut rpc,
        7,
        "agent.update",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": "Interactive" } }),
    )
    .await;
    assert_eq!(
        updated["agent"]["permissionPolicy"], "interactive",
        "{updated}"
    );
    assert_eq!(updated["agent"]["effectivePermissionPolicy"], "interactive");
    loop {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"] == agent_id
            && ev["data"].get("permissionPolicy").is_some()
        {
            assert_eq!(ev["data"]["permissionPolicy"], "interactive", "{ev}");
            break;
        }
    }

    // Next turn: the child restarts and the prompt waits for a client.
    wss_rpc(
        &mut rpc,
        8,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second" }),
    )
    .await;
    let request = loop {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        assert_ne!(ev["type"], "agent:failed", "{ev}");
        if ev["type"] == "agent:permission:request" && ev["data"]["sessionId"] == agent_id {
            break ev["data"]["requestId"].as_str().unwrap().to_string();
        }
    };
    let pending = wss_rpc(
        &mut rpc,
        9,
        "agent.pendingPermissions",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        pending["requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["requestId"] == request.as_str()),
        "interactive prompt is pending: {pending}"
    );
    let resolved = wss_rpc(
        &mut rpc,
        10,
        "agent.respondPermission",
        json!({ "requestId": request, "outcome": { "outcome": "selected", "optionId": "allow_once" } }),
    )
    .await;
    assert_eq!(resolved["resolved"], true);
    await_idle(&mut sub, &agent_id).await;
    let second_pids = spawned_pids(&session_log);
    assert_eq!(
        second_pids.len(),
        2,
        "policy change restarts the child: {second_pids:?}"
    );
    assert_ne!(second_pids[0], second_pids[1]);

    // Same policy: the live child is reused.
    wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "third" }),
    )
    .await;
    let request = loop {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:permission:request" && ev["data"]["sessionId"] == agent_id {
            break ev["data"]["requestId"].as_str().unwrap().to_string();
        }
    };
    wss_rpc(
        &mut rpc,
        12,
        "agent.respondPermission",
        json!({ "requestId": request, "outcome": { "outcome": "selected", "optionId": "allow_once" } }),
    )
    .await;
    await_idle(&mut sub, &agent_id).await;
    assert_eq!(spawned_pids(&session_log), second_pids, "no restart");

    let cleared = wss_rpc(
        &mut rpc,
        13,
        "agent.update",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": null } }),
    )
    .await;
    assert!(cleared["agent"]["permissionPolicy"].is_null(), "{cleared}");
    assert_eq!(cleared["agent"]["effectivePermissionPolicy"], "allow");
    let listed = wss_rpc(&mut rpc, 14, "agent.list", json!({ "workspaceId": ws_id })).await;
    let row = listed["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == agent_id.as_str())
        .unwrap()
        .clone();
    assert!(row["permissionPolicy"].is_null(), "{row}");
    assert_eq!(row["effectivePermissionPolicy"], "allow", "{row}");

    // The `agents.permissionPolicy` setting is the live default; a chat
    // override still wins over it.
    wss_rpc(
        &mut rpc,
        15,
        "settings.update",
        json!({ "changes": [{ "path": "agents.permissionPolicy", "value": "interactive" }] }),
    )
    .await;
    let defaulted = wss_rpc(
        &mut rpc,
        16,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        defaulted["agent"]["effectivePermissionPolicy"], "interactive",
        "{defaulted}"
    );
    let overridden = wss_rpc(
        &mut rpc,
        17,
        "agent.update",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "changes": { "permissionPolicy": "deny" } }),
    )
    .await;
    assert_eq!(
        overridden["agent"]["effectivePermissionPolicy"], "deny",
        "{overridden}"
    );
    let invalid = wss_rpc_envelope(
        &mut rpc,
        18,
        "settings.update",
        json!({ "changes": [{ "path": "agents.permissionPolicy", "value": "ask" }] }),
    )
    .await;
    assert!(invalid.get("error").is_some(), "{invalid}");
}
