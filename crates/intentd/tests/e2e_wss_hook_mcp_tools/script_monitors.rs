//! Real agent → `workspace_api` → native monitor → durable queued owner wake.
use super::*;

#[tokio::test]
async fn script_monitor_mcp_output_wakes_owner_once_over_wss() {
    let Some((agent_script, _)) = gate("script monitor MCP") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let fifo = data_dir.join("output-gate");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let command = format!("cat '{}'; read hold", fifo.display());
    let code=format!("const s=await ws.script.create('Controlled output',{},'command',{{purpose:'saved'}}); const start=await ws.script.start(s.id); return await ws.script.monitor(s.id,{{ttlMs:60000,runId:start.runId,outputPattern:'^READY$',lineCount:1}});",json!(command));
    let behavior=json!({"rules":[{"ifPromptContains":"REGISTER_SCRIPT_MONITOR","toolCall":{"name":"workspace_api","arguments":{"code":code,"summary":"Register native script output monitor"}},"response":"MONITOR_REGISTERED"}],"response":"WAKE_ACKNOWLEDGED"}).to_string();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", agent_script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg).await;
    let agent=wss_rpc(&mut rpc,"agent.create",json!({"workspaceId":ws_id,"name":"Script monitor owner","model":"default","provider":"mock"})).await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent_id,"content":"REGISTER_SCRIPT_MONITOR"}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(90));
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "MONITOR_REGISTERED", deadline).await;
    let rows = wss_rpc(
        &mut rpc,
        "scriptMonitor.list",
        json!({"workspaceId":ws_id,"agentId":agent_id}),
    )
    .await;
    let row = rows["monitors"][0].clone();
    assert_eq!(row["state"], "active", "{rows}");
    tokio::task::spawn_blocking(move || std::fs::write(fifo, b"\x1b[32mREADY\x1b[0m\n"))
        .await
        .unwrap()
        .unwrap();
    let text =
        await_conversation_contains(&mut rpc, &ws_id, &agent_id, "script_monitor_wake", deadline)
            .await;
    assert!(text.contains("output-match"), "{text}");
    assert!(text.contains("Untrusted script output"), "{text}");
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "WAKE_ACKNOWLEDGED", deadline).await;
    let terminal = wss_rpc(&mut rpc, "scriptMonitor.list", json!({"workspaceId":ws_id})).await;
    assert_eq!(terminal["monitors"][0]["state"], "triggered");
    assert_eq!(
        terminal["monitors"][0]["trigger"],
        json!({"observedLineCount":1,"matchedLine":"READY"})
    );
    let repeated = wss_rpc(
        &mut rpc,
        "scriptMonitor.cancelRun",
        json!({"workspaceId":ws_id,"monitorId":row["monitorId"]}),
    )
    .await;
    assert_eq!(repeated["runStopped"], false);
    wss_rpc(
        &mut rpc,
        "script.stop",
        json!({"workspaceId":ws_id,"scriptId":row["scriptId"]}),
    )
    .await;
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    let id = intent_core::AgentId::from(agent_id);
    let mid = format!("script-monitor:{}", row["monitorId"].as_str().unwrap());
    assert!(store
        .get_agent_message_by_id_with_pruned(&id, &mid)
        .await
        .unwrap()
        .is_some());
    assert!(!store
        .script_monitor_wake_pending(row["monitorId"].as_str().unwrap())
        .await
        .unwrap());
}

#[tokio::test]
async fn script_monitor_mcp_nonzero_completion_wakes_idle_owner_over_wss() {
    completion_wakes_idle_owner(false).await;
}

#[tokio::test]
async fn script_monitor_mcp_completion_recovers_after_result_write_failure() {
    completion_wakes_idle_owner(true).await;
}

async fn completion_wakes_idle_owner(reject_result: bool) {
    let Some((agent_script, _)) = gate("script completion MCP") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let fifo = data_dir.join("completion-gate");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let command = format!("cat '{}'; exit 1", fifo.display());
    let code=format!("const s=await ws.script.create('Controlled nonzero exit',{},'command',{{purpose:'saved'}}); const start=await ws.script.start(s.id); return await ws.script.monitor(s.id,{{ttlMs:300000,runId:start.runId}});",json!(command));
    let rpc_log = data_dir.join("consumed-prompts.jsonl");
    let behavior=json!({"rules":[{"ifPromptContains":"VERIFY_COMPLETION_DELIVERY","response":"DELIVERY_VERIFIED"},{"ifPromptContains":"REGISTER_SCRIPT_MONITOR","toolCall":{"name":"workspace_api","arguments":{"code":code,"summary":"Register native script completion monitor"}},"response":"MONITOR_REGISTERED"}],"response":"WAKE_ACKNOWLEDGED"}).to_string();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", agent_script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_RPC_LOG", rpc_log.to_str().unwrap()),
    ];
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg).await;
    let agent=wss_rpc(&mut rpc,"agent.create",json!({"workspaceId":ws_id,"name":"Script monitor owner","model":"default","provider":"mock"})).await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":agent_id,"content":"REGISTER_SCRIPT_MONITOR"}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(90));
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "MONITOR_REGISTERED", deadline).await;
    loop {
        let owner = wss_rpc(&mut rpc, "agent.get", json!({"agentId":agent_id})).await;
        if owner["agent"]["status"] == "idle" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "owner never became idle: {owner}"
        );
        // timing-guard: poll the real owner's observable idle state before releasing the child.
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    if reject_result {
        sqlx::query("CREATE TRIGGER refuse_script_result BEFORE UPDATE OF latest_run_result ON script WHEN NEW.latest_run_result IS NOT NULL BEGIN SELECT RAISE(FAIL, 'injected result write failure'); END")
            .execute(store.write_pool()).await.unwrap();
    }
    let rows = wss_rpc(
        &mut rpc,
        "scriptMonitor.list",
        json!({"workspaceId":ws_id,"agentId":agent_id}),
    )
    .await;
    let row = rows["monitors"][0].clone();
    assert_eq!(row["state"], "active", "{rows}");
    tokio::task::spawn_blocking(move || std::fs::write(fifo, b"release\n"))
        .await
        .unwrap()
        .unwrap();
    if reject_result {
        loop {
            let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap();
            // The two original finalizers still run. Three observed failures
            // prove maintenance retried too before we release the fault.
            if log
                .matches("script result persistence failed; leaving active")
                .count()
                >= 3
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "result persistence fault was not reached: {log}"
            );
            // timing-guard: observe the actual supervisor settlement errors, not a fixed delay.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        sqlx::query("DROP TRIGGER refuse_script_result")
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    let delivered = timeout(
        common::test_timeout(Duration::from_secs(15)),
        await_conversation_contains(&mut rpc, &ws_id, &agent_id, "WAKE_ACKNOWLEDGED", deadline),
    )
    .await;
    if delivered.is_err() {
        let runtime = wss_rpc(
            &mut rpc,
            "script.status",
            json!({"workspaceId":ws_id,"scriptId":row["scriptId"]}),
        )
        .await;
        let monitors = wss_rpc(&mut rpc, "scriptMonitor.list", json!({"workspaceId":ws_id})).await;
        let pending = store.pending_script_runs().await.unwrap();
        let wake = store
            .get_agent_message_by_id_with_pruned(
                &intent_core::AgentId::from(agent_id.clone()),
                &format!("script-monitor:{}", row["monitorId"].as_str().unwrap()),
            )
            .await
            .unwrap();
        panic!("owner never acknowledged completion after result writes recovered: runtime={runtime}; monitors={monitors}; pending={pending:?}; wake={wake:?}");
    }
    let text =
        await_conversation_contains(&mut rpc, &ws_id, &agent_id, "script_monitor_wake", deadline)
            .await;
    assert!(text.contains("finished"), "{text}");
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "WAKE_ACKNOWLEDGED", deadline).await;
    let terminal = wss_rpc(&mut rpc, "scriptMonitor.list", json!({"workspaceId":ws_id})).await;
    assert_eq!(terminal["monitors"][0]["state"], "completed");
    assert_eq!(terminal["monitors"][0]["runId"], row["runId"]);
    assert_eq!(terminal["monitors"][0]["result"]["exitCode"], 1);
    assert_eq!(terminal["monitors"][0]["result"]["outcome"], "failed");
    let id = intent_core::AgentId::from(agent_id);
    let mid = format!("script-monitor:{}", row["monitorId"].as_str().unwrap());
    assert!(store
        .get_agent_message_by_id_with_pruned(&id, &mid)
        .await
        .unwrap()
        .is_some());
    assert!(!store
        .script_monitor_wake_pending(row["monitorId"].as_str().unwrap())
        .await
        .unwrap());
    // Replay the durable outbox twice after actual consumption, then wait for
    // maintenance to acknowledge each replay. This drives duplicate delivery
    // attempts without relying on an arbitrary sleep or transcript uniqueness.
    for _ in 0..2 {
        sqlx::query("UPDATE script_monitor SET wake_state='pending' WHERE id=?")
            .bind(row["monitorId"].as_str().unwrap())
            .execute(store.write_pool())
            .await
            .unwrap();
        loop {
            if !store
                .script_monitor_wake_pending(row["monitorId"].as_str().unwrap())
                .await
                .unwrap()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "outbox replay never acknowledged"
            );
            // timing-guard: wait for the maintenance delivery attempt to acknowledge the replay.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({"workspaceId":ws_id,"agentId":id,"content":"VERIFY_COMPLETION_DELIVERY"}),
    )
    .await;
    await_conversation_contains(&mut rpc, &ws_id, id.as_str(), "DELIVERY_VERIFIED", deadline).await;
    let calls: Vec<Value> = std::fs::read_to_string(&rpc_log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let completions: Vec<String> = calls
        .iter()
        .filter(|call| call["method"] == "session/prompt")
        .flat_map(|call| call["params"]["prompt"].as_array().unwrap())
        .filter_map(|part| part["text"].as_str())
        .filter(|text| text.contains("Monitoring ended."))
        .map(str::to_owned)
        .collect();
    assert_eq!(
        completions.len(),
        1,
        "actual consumed completion prompts: {calls:?}"
    );
    let prompt = &completions[0];
    for expected in [
        row["scriptId"].as_str().unwrap(),
        row["runId"].as_str().unwrap(),
        "finished",
        "\"outcome\":\"failed\"",
        "\"exitCode\":1",
    ] {
        assert!(prompt.contains(expected), "missing {expected}: {prompt}");
    }
    let acknowledgements: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_message WHERE agent_id=? AND role='assistant' AND content LIKE '%WAKE_ACKNOWLEDGED%'")
        .bind(id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(
        acknowledgements, 1,
        "exactly one actual completion acknowledgement"
    );
    let metadata: String = sqlx::query_scalar("SELECT metadata FROM agent_message WHERE id=?")
        .bind(&mid)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    let metadata: Value = serde_json::from_str(&metadata).unwrap();
    assert_eq!(metadata["scriptId"], row["scriptId"]);
    assert_eq!(metadata["runId"], row["runId"]);
    assert_eq!(metadata["reason"], "finished");
    assert_eq!(metadata["result"]["outcome"], "failed");
    assert_eq!(metadata["result"]["exitCode"], 1);
}
