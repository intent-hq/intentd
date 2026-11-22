//! `startedBy` on script runs: an agent's `ws.script.*` start is attributed
//! to the agent, a client's to the user, and the settled `lastRun` keeps it
//! across a daemon restart.
use super::*;

async fn script_get(rpc: &mut TlsWs, ws_id: &str, id: &str) -> Value {
    wss_rpc(
        rpc,
        "script.get",
        json!({"workspaceId":ws_id,"scriptId":id}),
    )
    .await
}

async fn connect(data_dir: &Path) -> TlsWs {
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    connect_ws(port, cfg).await
}

#[tokio::test]
async fn script_runs_record_who_started_them_over_wss() {
    let Some((agent_script, _)) = gate("script startedBy") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let code = "await ws.script.create('Agent service','tail -f /dev/null','service',{purpose:'saved',scriptId:'agent-service'}); await ws.script.start('agent-service'); await ws.script.create('Agent command','true','command',{purpose:'saved',scriptId:'agent-command'}); return await ws.script.run('agent-command');";
    let behavior = json!({"rules":[{"ifPromptContains":"START_SCRIPTS","toolCall":{"name":"workspace_api","arguments":{"code":code,"summary":"Start scripts"}},"response":"SCRIPTS_STARTED"}],"response":"OK"}).to_string();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", agent_script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
        data_dir: data_dir.clone(),
    };
    let mut rpc = connect(&data_dir).await;
    let agent = wss_rpc(
        &mut rpc,
        "agent.create",
        json!({"workspaceId":ws_id,"name":"Script starter","model":"default","provider":"mock"}),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent_id,"content":"START_SCRIPTS"}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(90));
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "SCRIPTS_STARTED", deadline).await;
    let by_agent = json!({"kind":"agent","agentId":agent_id});
    let by_user = json!({"kind":"user"});

    let service = wss_rpc(
        &mut rpc,
        "script.status",
        json!({"workspaceId":ws_id,"scriptId":"agent-service"}),
    )
    .await;
    assert_eq!(service["startedBy"], by_agent, "{service}");
    let command = script_get(&mut rpc, &ws_id, "agent-command").await;
    assert_eq!(command["lastRun"]["startedBy"], by_agent, "{command}");
    assert_eq!(command["runtime"]["startedBy"], by_agent, "{command}");

    // A user restart re-attributes the service's new run.
    wss_rpc(
        &mut rpc,
        "script.restart",
        json!({"workspaceId":ws_id,"scriptId":"agent-service"}),
    )
    .await;
    let service = wss_rpc(
        &mut rpc,
        "script.status",
        json!({"workspaceId":ws_id,"scriptId":"agent-service"}),
    )
    .await;
    assert_eq!(service["startedBy"], by_user, "{service}");

    wss_rpc(
        &mut rpc,
        "script.create",
        json!({"workspaceId":ws_id,"scriptId":"user-command","name":"User command","command":"true","mode":"command","purpose":"saved"}),
    )
    .await;
    wss_rpc(
        &mut rpc,
        "script.run",
        json!({"workspaceId":ws_id,"scriptId":"user-command"}),
    )
    .await;
    let user = script_get(&mut rpc, &ws_id, "user-command").await;
    assert_eq!(user["lastRun"]["startedBy"], by_user, "{user}");
    assert_eq!(user["runtime"]["startedBy"], by_user, "{user}");
    wss_rpc(
        &mut rpc,
        "script.stop",
        json!({"workspaceId":ws_id,"scriptId":"agent-service"}),
    )
    .await;
    drop(rpc);
    drop(daemon);

    let _daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
        data_dir: data_dir.clone(),
    };
    let mut rpc = connect(&data_dir).await;
    let command = script_get(&mut rpc, &ws_id, "agent-command").await;
    assert_eq!(command["lastRun"]["startedBy"], by_agent, "{command}");
    assert_eq!(command["runtime"]["startedBy"], by_agent, "{command}");
    let user = script_get(&mut rpc, &ws_id, "user-command").await;
    assert_eq!(user["lastRun"]["startedBy"], by_user, "{user}");
}
