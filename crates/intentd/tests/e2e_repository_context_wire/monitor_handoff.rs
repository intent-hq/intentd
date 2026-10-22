//! Explicit disposable fixture for the existing Electron verification owner.
//! Control files change the local forge or invoke real daemon operations only.
use super::*;

#[tokio::test]
#[ignore = "interactive disposable daemon; requires explicit handoff directory and verified native fixtures"]
async fn monitor_native_daemon_frontend_handoff() {
    let output = PathBuf::from(std::env::var_os("INTENT_REVIEW_FRONTEND_HANDOFF").unwrap());
    std::fs::create_dir_all(&output).unwrap();
    let input = PathBuf::from(std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE").unwrap());
    let account = OriginalAccount::new().await;
    let mut h =
        Harness::boot_case(None, true, Some((&input, &account.endpoint)), false, true).await;
    let native = h.dir.path().join("native-runtime");
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    {
        let mut ordinary = h.wss(TOKEN).await;
        authorize(&mut ordinary, "stored-pat").await;
    }
    let mut agent: Option<intent_core::AgentId> = None;
    let mut sequence = 0;
    let expires =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(3600));
    loop {
        let socket = h.dir.path().join("intentd.sock");
        let status = common::await_wss_status(&socket).await;
        let monitors = if let Some(agent) = &agent {
            store
                .list_active_pr_monitors_by_agent(agent)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.monitor_id)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let ready = json!({"scope":"disposable loopback native fixture, production support remains off","dataDir":h.dir.path(),"socket":socket,"port":h.port,"fingerprint":status["result"]["fingerprint"],"token":TOKEN,"workspaceId":h.workspace,"agentId":agent,"monitorIds":monitors,"nativeDir":native,"daemonPid":h.child.id(),"lastSequence":sequence});
        let temporary = output.join("ready.next.json");
        std::fs::write(&temporary, serde_json::to_vec_pretty(&ready).unwrap()).unwrap();
        std::fs::rename(temporary, output.join("ready.json")).unwrap();
        let request = loop {
            if tokio::time::Instant::now() >= expires {
                break json!({"sequence":sequence+1,"action":"stop"});
            }
            if let Ok(bytes) = std::fs::read(output.join("command.json")) {
                if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                    if value["sequence"]
                        .as_u64()
                        .is_some_and(|next| next > sequence)
                    {
                        break value;
                    }
                }
            }
            // timing-guard: wait for an explicit command owned by the frontend verifier
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        sequence = request["sequence"].as_u64().unwrap();
        let mut ordinary = h.wss(TOKEN).await;
        match request["action"].as_str().unwrap() {
            "register" => {
                assert!(
                    agent.is_none(),
                    "start a new fixture for a second original owner"
                );
                std::fs::write(native.join("monitor-request.json"), serde_json::to_vec(&json!({"summary":"Register original frontend monitor","code":"return await ws.pr.monitor(4, {target:{repository:{provider:'gitlab',instanceBaseUrl:'https://gitlab.test/forge',projectPath:'group/project'},kind:'merge-request',number:4}});"})).unwrap()).unwrap();
                let created = ordinary.rpc("agent.create", json!({"workspaceId":h.workspace,"name":"Native frontend review owner","model":"claude-sonnet-4-6","provider":"claude-code"})).await;
                let id =
                    intent_core::AgentId::from(success(&created)["agent"]["id"].as_str().unwrap());
                success(&ordinary.rpc("agent.sendMessage", json!({"workspaceId":h.workspace,"agentId":id,"content":"Register the original frontend review monitor"})).await);
                timeout(
                    intentd_test_support::test_timeout(Duration::from_secs(60)),
                    async {
                        loop {
                            if store.get_agent_session(&id).await.unwrap().status
                                == intent_core::AgentStatus::RuntimeIdle
                                && !store
                                    .list_active_pr_monitors_by_agent(&id)
                                    .await
                                    .unwrap()
                                    .is_empty()
                            {
                                break;
                            }
                            // timing-guard: wait for actual original callback registration
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    },
                )
                .await
                .unwrap();
                agent = Some(id);
            }
            "draft" => *account.draft.lock().unwrap() = true,
            "ready" => *account.draft.lock().unwrap() = false,
            "merged" => *account.review_state.lock().unwrap() = "merged",
            "account43" => authorize(&mut ordinary, "pat-second").await,
            "account42" => authorize(&mut ordinary, "stored-pat").await,
            "restart" => {
                let instruction = native.join("monitor-request.json");
                if instruction.exists() {
                    std::fs::remove_file(instruction).unwrap();
                }
                restart_native_daemon(&mut h, &account.endpoint).await;
                ordinary = h.wss(TOKEN).await;
                authorize(&mut ordinary, "stored-pat").await;
            }
            "resume" => {
                let id = agent.as_ref().expect("register first");
                success(&ordinary.rpc("agent.sendMessage", json!({"workspaceId":h.workspace,"agentId":id,"content":"Ordinary frontend explicit resume"})).await);
            }
            "stop" => break,
            action => panic!("unsupported local fixture control {action}"),
        }
        std::fs::write(
            output.join(format!("command-{sequence}.json")),
            serde_json::to_vec_pretty(&request).unwrap(),
        )
        .unwrap();
    }
    let mut shutdown = h.uds().await;
    success(&shutdown.rpc("system.shutdown", json!({})).await);
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            loop {
                if let Some(status) = h.child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                // timing-guard: preserve native join until the owned daemon shutdown completes
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
    )
    .await
    .unwrap();
    std::fs::write(output.join("stopped.json"), b"{\"stopped\":true}\n").unwrap();
}
