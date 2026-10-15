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
    let code=format!("const s=await ws.script.create('Controlled output',{},'command',{{purpose:'saved'}}); const start=await ws.script.start(s.id); const watch=await ws.script.monitor(s.id,{{ttlMs:60000,runId:start.runId,outputPattern:'^READY$',lineCount:1}}); const page=await ws.script.listPage({{limit:1}}); const monitors=await ws.script.monitorsPage({{limit:1}}); const detail=await ws.script.monitorDetails(watch.monitor.monitorId); const script=await ws.script.get(s.id); if(page.view!=='summary'||page.scripts[0].command!==undefined||monitors.view!=='summary'||monitors.monitors[0].outputPattern!==undefined||detail.outputPattern!=='^READY$'||script.command!==s.command) throw new Error('compact binding mismatch'); await ws.script.create('Compact bindings verified','true','command',{{purpose:'saved',scriptId:'compact-bindings-verified'}}); return watch;",json!(command));
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
    let verified = wss_rpc(
        &mut rpc,
        "script.get",
        json!({"workspaceId":ws_id,"scriptId":"compact-bindings-verified"}),
    )
    .await;
    assert_eq!(
        verified["id"], "compact-bindings-verified",
        "all real compact bindings completed: {verified}"
    );
    assert!(text.contains("output-match"), "{text}");
    assert!(text.contains("Untrusted script output"), "{text}");
    await_conversation_contains(&mut rpc, &ws_id, &agent_id, "WAKE_ACKNOWLEDGED", deadline).await;
    let terminal = wss_rpc(&mut rpc, "scriptMonitor.list", json!({"workspaceId":ws_id})).await;
    assert!(terminal["monitors"].as_array().unwrap().is_empty());
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
    timeout(common::test_timeout(Duration::from_secs(10)), async {
        loop {
            let receipts: i64 =
                sqlx::query_scalar("SELECT count(*) FROM agent_queue_admission WHERE id=?")
                    .bind(&mid)
                    .fetch_one(store.read_pool())
                    .await
                    .unwrap();
            if receipts == 0 {
                break;
            }
            // timing-guard: poll cross-process SQL receipt deletion; no receipt event is emitted
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("actual successful worker cleans its admission receipt");
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
    assert!(terminal["monitors"].as_array().unwrap().is_empty());
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
    // Repeated stale callbacks cannot retire a nonexistent monitor or manufacture a job.
    let mut stale: intent_core::ScriptMonitor = serde_json::from_value(row.clone()).unwrap();
    stale.state = "completed".into();
    stale.reason = Some("finished".into());
    stale.settled_at = Some(intent_core::now_iso());
    assert!(!store.settle_script_monitor(&stale).await.unwrap());
    assert!(!store.settle_script_monitor(&stale).await.unwrap());
    assert!(store.script_notification_jobs().await.unwrap().is_empty());
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

#[tokio::test]
async fn script_monitor_worker_idle_warning_settles_delivery() {
    monitor_worker_receipt_lifecycle("idle-warning").await;
}

#[tokio::test]
async fn script_monitor_worker_cleanup_failure_does_not_replay() {
    monitor_worker_receipt_lifecycle("cleanup-failure").await;
}

#[tokio::test]
async fn script_monitor_worker_completion_marker_failure_retries_in_process() {
    monitor_worker_receipt_lifecycle("marker-failure").await;
}

#[tokio::test]
async fn script_monitor_worker_user_stop_settles_delivery() {
    monitor_worker_receipt_lifecycle("user-stop").await;
}

#[tokio::test]
async fn script_monitor_worker_acceptance_failure_recovers_without_restart() {
    monitor_worker_receipt_lifecycle("acceptance-failure").await;
}

#[tokio::test]
async fn script_monitor_worker_late_acceptance_failure_recovers_without_restart() {
    monitor_worker_receipt_lifecycle("late-acceptance-failure").await;
}

#[tokio::test]
async fn script_monitor_worker_crash_recovers_unfinished_delivery() {
    monitor_worker_receipt_lifecycle("crash").await;
}

/// Exercise the actual daemon worker, provider prompt/cancel, maintenance and
/// restart paths. Only the initial monitor outcome is seeded through the store;
/// no acceptance/completion helper stands in for worker execution.
async fn monitor_worker_receipt_lifecycle(scenario: &str) {
    use intent_core::{now_iso, AgentId, ScriptMonitor};
    let Some((script, _)) = gate("monitor worker delivery lifecycle") else {
        return;
    };
    let guard = temp_data_dir();
    let data_dir = guard.path().to_path_buf();
    let ws_id = seed_workspace_only(&data_dir).await;
    let prompt_log = data_dir.join("worker-prompts.jsonl");
    let behavior = json!({"silentUntilCancelTurns": i32::from(!matches!(scenario, "cleanup-failure" | "marker-failure" | "acceptance-failure" | "late-acceptance-failure")), "rules":[{"ifPromptContains":"ARM_ACCEPTANCE_WATCH","toolCall":{"name":"workspace_api","arguments":{"code":"const agents=await ws.agent.list(); const owner=agents.find(a=>a.name==='Worker receipt owner'); return await ws.agent.watch(owner.id);","summary":"Watch acceptance recovery completion"}},"response":"ACCEPTANCE_WATCH_ARMED"},{"ifPromptContains":"[WORKSPACE EVENTS]","response":"ACCEPTANCE_COMPLETION_ACK"}],"response":"WORKER_RECEIPT_DONE"}).to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log.to_str().unwrap()),
        (
            "INTENTD_PROMPT_IDLE_TIMEOUT_MS",
            if scenario == "idle-warning" {
                "1500"
            } else {
                "60000"
            },
        ),
    ];
    let daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(&mut rpc,"agent.create",json!({"workspaceId":ws_id,"name":"Worker receipt owner","model":"default","provider":"mock"})).await;
    let agent_id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .unwrap();
    if scenario == "cleanup-failure" {
        sqlx::query("CREATE TRIGGER fail_receipt_cleanup BEFORE DELETE ON agent_queue_admission BEGIN SELECT RAISE(ABORT,'injected receipt cleanup failure'); END")
            .execute(store.write_pool()).await.unwrap();
    }
    if scenario == "marker-failure" {
        sqlx::query("CREATE TRIGGER fail_completion_marker BEFORE UPDATE OF phase ON agent_queue_admission WHEN new.phase='completed' BEGIN SELECT RAISE(ABORT,'injected completion marker failure'); END")
            .execute(store.write_pool()).await.unwrap();
    }
    if matches!(scenario, "acceptance-failure" | "late-acceptance-failure") {
        sqlx::query("CREATE TABLE acceptance_attempts(turn_id TEXT)")
            .execute(store.write_pool())
            .await
            .unwrap();
        let old_phase = if scenario == "late-acceptance-failure" {
            "accepted"
        } else {
            "pending"
        };
        sqlx::query(&format!("CREATE TRIGGER fail_acceptance BEFORE UPDATE OF phase ON agent_queue_admission WHEN old.phase='{old_phase}' AND new.phase='accepted' BEGIN INSERT INTO acceptance_attempts VALUES(old.turn_id); SELECT RAISE(FAIL,'injected acceptance failure'); END"))
            .execute(store.write_pool()).await.unwrap();
    }
    let mut row: ScriptMonitor = serde_json::from_value(json!({"monitorId":"worker-receipt","workspaceId":ws_id,"agentId":agent_id,"scriptId":"fixture","runId":"fixture-run","scriptName":"Fixture","mode":"command","state":"active","createdAt":now_iso(),"expiresAt":"2099-01-01T00:00:00Z"})).unwrap();
    store.insert_script_monitor(&row).await.unwrap();
    row.state = "expired".into();
    row.reason = Some("ttl".into());
    row.settled_at = Some(now_iso());
    store.settle_script_monitor(&row).await.unwrap();
    let mut acceptance_watcher = None;
    if matches!(scenario, "acceptance-failure" | "late-acceptance-failure") {
        timeout(common::test_timeout(Duration::from_secs(10)), async {
            loop {
                let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM acceptance_attempts")
                    .fetch_one(store.read_pool())
                    .await
                    .unwrap();
                if attempts > 0 {
                    break;
                }
                // timing-guard: observe the real worker's failed SQL acceptance before removing the fault
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("worker attempts durable acceptance");
        assert!(
            !std::fs::read_to_string(&prompt_log).is_ok_and(|text| !text.is_empty()),
            "no provider prompt before acceptance"
        );
        let recovery = store.load_all_agent_queues().await.unwrap();
        assert_eq!(recovery.len(), 1);
        assert_eq!(recovery[0].turn_id, "script-monitor:worker-receipt");
        let watcher = wss_rpc(&mut rpc, "agent.create", json!({"workspaceId":ws_id,"name":"Acceptance completion watcher","model":"default","provider":"mock"})).await;
        let watcher_id = watcher["agent"]["id"].as_str().unwrap().to_owned();
        wss_rpc(
            &mut rpc,
            "agent.sendMessage",
            json!({"workspaceId":ws_id,"agentId":watcher_id,"content":"ARM_ACCEPTANCE_WATCH"}),
        )
        .await;
        await_conversation_contains(
            &mut rpc,
            &ws_id,
            &watcher_id,
            "ACCEPTANCE_WATCH_ARMED",
            tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30)),
        )
        .await;
        acceptance_watcher = Some(watcher_id);
        sqlx::query("DROP TRIGGER fail_acceptance")
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        loop {
            if std::fs::read_to_string(&prompt_log).is_ok_and(|text| !text.is_empty()) {
                break;
            }
            // timing-guard: poll the external mock-provider prompt log before stopping or crashing the daemon
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("provider received the real notification turn");
    if scenario == "user-stop" {
        wss_rpc(&mut rpc, "agent.stop", json!({"agentId":agent_id})).await;
    } else if scenario != "crash" {
        await_conversation_contains(
            &mut rpc,
            &ws_id,
            agent_id.as_str(),
            "WORKER_RECEIPT_DONE",
            tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30)),
        )
        .await;
    }
    if scenario == "marker-failure" {
        timeout(common::test_timeout(Duration::from_secs(10)), async {
            loop {
                if std::fs::read_to_string(data_dir.join("daemon.log"))
                    .is_ok_and(|log| log.contains("injected completion marker failure"))
                {
                    break;
                }
                // timing-guard: poll the daemon log to observe the injected SQL failure before removing its trigger
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("actual worker attempted durable completion");
        assert!(
            store
                .script_monitor_pending_for_agent(&agent_id)
                .await
                .unwrap(),
            "failed marker retains ownership"
        );
        sqlx::query("DROP TRIGGER fail_completion_marker")
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    if scenario == "crash" {
        assert!(store
            .script_monitor_pending_for_agent(&agent_id)
            .await
            .unwrap());
    } else {
        timeout(common::test_timeout(Duration::from_secs(10)),async {
            while store.script_monitor_pending_for_agent(&agent_id).await.unwrap() {
                // timing-guard: poll durable receipt settlement by the worker or maintenance retry
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await.expect("finished/replaced/cancelled worker must release pending delivery ownership even if cleanup fails");
        assert!(
            store.load_all_agent_queues().await.unwrap().is_empty(),
            "settled notification must not recover as runnable work"
        );
    }
    if let Some(watcher) = &acceptance_watcher {
        await_conversation_contains(
            &mut rpc,
            &ws_id,
            watcher,
            "ACCEPTANCE_COMPLETION_ACK",
            tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30)),
        )
        .await;
    }
    drop(rpc);
    drop(daemon);
    let prompts_before = std::fs::read_to_string(&prompt_log)
        .unwrap()
        .lines()
        .count();
    let resumed_behavior = json!({"response":"WORKER_RECEIPT_DONE"}).to_string();
    let mut restart_env = env;
    restart_env[2] = ("MOCK_AGENT_BEHAVIOR", resumed_behavior.as_str());
    let _restarted = Daemon {
        child: spawn_serve(&data_dir, &restart_env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg).await;
    if scenario == "crash" {
        await_conversation_contains(
            &mut rpc,
            &ws_id,
            agent_id.as_str(),
            "WORKER_RECEIPT_DONE",
            tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30)),
        )
        .await;
    }
    if scenario == "cleanup-failure" {
        assert!(
            store.load_all_agent_queues().await.unwrap().is_empty(),
            "cleanup failure across restart must not replay completed work"
        );
        sqlx::query("DROP TRIGGER fail_receipt_cleanup")
            .execute(store.write_pool())
            .await
            .unwrap();
    }
    timeout(common::test_timeout(Duration::from_secs(10)), async {
        loop {
            let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_queue_admission")
                .fetch_one(store.read_pool())
                .await
                .unwrap();
            if remaining == 0 {
                break;
            }
            // timing-guard: poll cross-process maintenance cleanup after daemon restart
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("maintenance cleans settled receipts after retry/restart");
    assert!(!store
        .script_monitor_pending_for_agent(&agent_id)
        .await
        .unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    let prompts_after = std::fs::read_to_string(&prompt_log)
        .unwrap()
        .lines()
        .count();
    if scenario == "crash" {
        // Startup also emits its ordinary supervisor continuation. Count only
        // direct notification prompts, excluding that separate recovery message.
        let direct_wakes = std::fs::read_to_string(&prompt_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|prompt| {
                prompt["text"]
                    .as_str()
                    .unwrap()
                    .ends_with("The monitor did not stop the script.")
            })
            .count();
        assert_eq!(direct_wakes, 2, "exactly one unfinished notification retry");
    } else {
        assert_eq!(
            prompts_after, prompts_before,
            "settled notification must not replay"
        );
    }
    if scenario == "idle-warning" {
        assert_eq!(prompts_before, 2, "timeout continues on a warning turn");
    }
    if matches!(scenario, "acceptance-failure" | "late-acceptance-failure") {
        assert_eq!(
            prompts_before, 3,
            "one owner turn plus watch registration and exactly one completion wake"
        );
        let attempts: Vec<String> = sqlx::query_scalar("SELECT turn_id FROM acceptance_attempts")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
        assert!(attempts
            .iter()
            .all(|turn| turn == "script-monitor:worker-receipt"));
    }
    let messages: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_message WHERE id='script-monitor:worker-receipt'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(
        messages, 1,
        "restart preserves the original notification identity"
    );
}
