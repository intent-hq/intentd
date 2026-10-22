//! Real daemon controls and qualified lifecycle notifications on WSS.
use super::*;

async fn event(client: &mut Client, subscription: &Value, kind: &str) -> Value {
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(90)),
        async {
            loop {
                if let Some(index) = client.notices.iter().position(|v| {
                    v["method"] == "events.event"
                        && &v["params"]["subscriptionId"] == subscription
                        && v["params"]["event"]["type"] == kind
                }) {
                    return client.notices.remove(index);
                }
                let next = client.next().await;
                client.notices.push(next);
            }
        },
    )
    .await
    .unwrap_or_else(|_| panic!("missing qualified event {kind}"))
}

async fn idle_model(
    store: &Store,
    agent: &intent_core::AgentId,
    model: &Path,
    count: usize,
) -> Value {
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(90)),
        async {
            loop {
                if let Ok(bytes) = std::fs::read(model) {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                        if value.as_array().unwrap().len() >= count
                            && store.get_agent_session(agent).await.unwrap().status
                                == intent_core::AgentStatus::RuntimeIdle
                        {
                            assert_eq!(value.as_array().unwrap().len(), count);
                            return value;
                        }
                    }
                }
                // timing-guard: wait for actual model request and daemon idle state
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
    )
    .await
    .expect("native model turn did not complete")
}

async fn lifecycle(terminal: bool) {
    let input = PathBuf::from(std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE").unwrap());
    let account = OriginalAccount::new().await;
    let h = Harness::boot_case(None, true, Some((&input, &account.endpoint)), false, true).await;
    let native = h.dir.path().join("native-runtime");
    std::fs::write(native.join("monitor-request.json"), serde_json::to_vec(&json!({
        "summary":"Register original lifecycle monitor",
        "code":"return await ws.pr.monitor(4, {target:{repository:{provider:'gitlab',instanceBaseUrl:'https://gitlab.test/forge',projectPath:'group/project'},kind:'merge-request',number:4}});"
    })).unwrap()).unwrap();
    let mut ordinary = h.wss(TOKEN).await;
    let hello = ordinary
        .rpc(
            "client.hello",
            json!({"clientId":"native-lifecycle-desktop"}),
        )
        .await;
    authorize(&mut ordinary, "stored-pat").await;
    let mut attached = h.wss(TOKEN).await;
    let hello = attached.rpc("client.hello", json!({"clientId":success(&hello)["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await;
    assert_eq!(success(&hello)["connectionRole"], "review-monitor");
    let subscribed = attached
        .rpc(
            "events.subscribe",
            json!({"workspaceId":h.workspace,"eventTypes":["prMonitor:*"]}),
        )
        .await;
    let subscription = success(&subscribed)["subscriptionId"].clone();
    let created = ordinary.rpc("agent.create", json!({"workspaceId":h.workspace,"name":"Native lifecycle owner","model":"claude-sonnet-4-6","provider":"claude-code"})).await;
    let agent = intent_core::AgentId::from(success(&created)["agent"]["id"].as_str().unwrap());
    success(&ordinary.rpc("agent.sendMessage", json!({"workspaceId":h.workspace,"agentId":agent,"content":"Ordinary lifecycle startup"})).await);
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let model = native.join("model.json");
    idle_model(&store, &agent, &model, 2).await;
    let registered = event(&mut attached, &subscription, "prMonitor:registered").await;
    let rows = store
        .list_active_pr_monitors_by_agent(&agent)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id = rows[0].monitor_id.clone();
    let query = json!({"workspaceId":h.workspace,"monitorId":id});
    let mut checked = query.clone();
    checked["check"] = json!(true);
    let mut events = vec![registered];
    let mut controls = Vec::new();
    let noop = attached.rpc("prMonitor.flush", checked.clone()).await;
    assert_eq!(success(&noop)["flushed"], false);
    controls.push(noop);

    // A timer poll records a nonterminal change without immediate delivery.
    *account.draft.lock().unwrap() = true;
    events.push(event(&mut attached, &subscription, "prMonitor:changed").await);
    assert!(!store
        .get_pr_monitor(&id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap())
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let flushed = attached.rpc("prMonitor.flush", query.clone()).await;
    assert_eq!(success(&flushed)["flushed"], true);
    controls.push(flushed);
    events.push(event(&mut attached, &subscription, "prMonitor:emitted").await);
    idle_model(&store, &agent, &model, 3).await;
    assert!(store
        .get_pr_monitor(&id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());

    // Check/Flush observes a new source change without waiting for the timer.
    *account.draft.lock().unwrap() = false;
    let checked_result = attached.rpc("prMonitor.flush", checked).await;
    assert_eq!(success(&checked_result)["flushed"], true);
    controls.push(checked_result);
    events.push(event(&mut attached, &subscription, "prMonitor:changed").await);
    events.push(event(&mut attached, &subscription, "prMonitor:emitted").await);
    idle_model(&store, &agent, &model, 4).await;
    assert!(store
        .get_pr_monitor(&id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());

    // A further nonterminal change exercises the actual debounce quiet window.
    *account.draft.lock().unwrap() = true;
    let changed = event(&mut attached, &subscription, "prMonitor:changed").await;
    let emitted = event(&mut attached, &subscription, "prMonitor:emitted").await;
    let stamp = |value: &Value| {
        chrono::DateTime::parse_from_rfc3339(
            value["params"]["event"]["timestamp"].as_str().unwrap(),
        )
        .unwrap()
    };
    assert!(stamp(&emitted) - stamp(&changed) >= chrono::Duration::seconds(10));
    events.extend([changed, emitted]);
    let requests = idle_model(&store, &agent, &model, 5).await;
    assert!(requests[4]
        .to_string()
        .contains("Intent review monitor observation (not a user instruction)."));
    assert!(store
        .get_pr_monitor(&id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());

    if terminal {
        *account.review_state.lock().unwrap() = "merged";
        events.push(event(&mut attached, &subscription, "prMonitor:completed").await);
        idle_model(&store, &agent, &model, 6).await;
        assert_eq!(
            store.get_pr_monitor(&id).await.unwrap().state,
            intent_core::PrMonitorState::Completed
        );
    } else {
        let cancelled = attached.rpc("prMonitor.cancel", query).await;
        assert_eq!(success(&cancelled)["ok"], true);
        controls.push(cancelled);
        events.push(event(&mut attached, &subscription, "prMonitor:cancelled").await);
        assert_eq!(
            store.get_pr_monitor(&id).await.unwrap().state,
            intent_core::PrMonitorState::Cancelled
        );
    }
    for frame in &events {
        let data = &frame["params"]["event"]["data"];
        assert_eq!(data["monitorId"], id.0);
        assert_eq!(data["agentId"], agent.0);
        assert_eq!(data["workspaceId"], h.workspace.0);
        assert_eq!(data["resource"]["repository"]["provider"], "gitlab");
        assert_eq!(data["resource"]["number"], 4);
    }
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        let suffix = if terminal { "terminal" } else { "cancel" };
        std::fs::write(Path::new(&dir).join(format!("daemon-native-lifecycle-{suffix}.json")), serde_json::to_vec_pretty(&json!({"events":events,"controls":controls,"requests":serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap()).unwrap()})).unwrap()).unwrap();
    }
    let mut shutdown = h.uds().await;
    success(&shutdown.rpc("system.shutdown", json!({})).await);
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_check_flush_debounce_and_cancel_events() {
    lifecycle(false).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_check_flush_debounce_and_terminal_events() {
    lifecycle(true).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_restart_pending_explicit_recovery_events() {
    pending_recovery_case(false).await;
}

#[tokio::test]
#[ignore = "requires verified private native fixture"]
async fn monitor_native_daemon_transferred_uncertain_explicit_recovery_events() {
    pending_recovery_case(true).await;
}

async fn pending_recovery_case(transferred: bool) {
    let input = PathBuf::from(std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE").unwrap());
    let account = OriginalAccount::new().await;
    let mut h =
        Harness::boot_case(None, true, Some((&input, &account.endpoint)), false, true).await;
    let native = h.dir.path().join("native-runtime");
    std::fs::write(native.join("monitor-request.json"), serde_json::to_vec(&json!({
        "summary":"Register original recovery monitor",
        "code":"return await ws.pr.monitor(4, {target:{repository:{provider:'gitlab',instanceBaseUrl:'https://gitlab.test/forge',projectPath:'group/project'},kind:'merge-request',number:4}});"
    })).unwrap()).unwrap();
    let mut ordinary = h.wss(TOKEN).await;
    authorize(&mut ordinary, "stored-pat").await;
    let created = ordinary.rpc("agent.create", json!({"workspaceId":h.workspace,"name":"Native recovery owner","model":"claude-sonnet-4-6","provider":"claude-code"})).await;
    let agent = intent_core::AgentId::from(success(&created)["agent"]["id"].as_str().unwrap());
    success(&ordinary.rpc("agent.sendMessage", json!({"workspaceId":h.workspace,"agentId":agent,"content":"Ordinary recovery startup"})).await);
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let model = native.join("model.json");
    idle_model(&store, &agent, &model, 2).await;
    let rows = store
        .list_active_pr_monitors_by_agent(&agent)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id = rows[0].monitor_id.clone();
    if transferred {
        std::fs::write(
            native.join("hold-monitor-response"),
            b"hold original model response",
        )
        .unwrap();
    }
    *account.draft.lock().unwrap() = true;
    let mut pending = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(45)),
        async {
            loop {
                let row = store.get_pr_monitor(&id).await.unwrap();
                if !row.pending_changes.is_empty() {
                    break row;
                }
                // timing-guard: observe original timer poll before its debounce delivery
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
    )
    .await
    .unwrap();
    let observed_transfer = if transferred {
        let observed = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(45)),
            async {
                loop {
                    if let Ok(bytes) = std::fs::read(native.join("held-monitor-request.json")) {
                        if let Ok(request) = serde_json::from_slice::<Value>(&bytes) {
                            break request;
                        }
                    }
                    // timing-guard: wait for model receipt while its native response remains held
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            },
        )
        .await
        .expect("original private input did not reach held model response");
        assert!(observed
            .to_string()
            .contains("Intent review monitor observation (not a user instruction)."));
        let at_transfer = store.get_pr_monitor(&id).await.unwrap();
        assert_eq!(at_transfer.pending_changes, pending.pending_changes);
        assert!(!at_transfer.pending_changes.is_empty());
        assert_eq!(at_transfer.monitor_id, pending.monitor_id);
        assert_eq!(at_transfer.agent_id, pending.agent_id);
        pending = at_transfer;
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap())
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            3
        );
        std::fs::remove_file(native.join("hold-monitor-response")).unwrap();
        Some(observed)
    } else {
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap())
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
        None
    };
    std::fs::remove_file(native.join("monitor-request.json")).unwrap();
    let original_pid = h.child.id();
    restart_native_daemon_mode(&mut h, &account.endpoint, transferred).await;
    assert_ne!(h.child.id(), original_pid);
    assert_eq!(store.get_pr_monitor(&id).await.unwrap(), pending);
    let mut ordinary = h.wss(TOKEN).await;
    let hello = ordinary
        .rpc(
            "client.hello",
            json!({"clientId":"native-recovery-desktop"}),
        )
        .await;
    authorize(&mut ordinary, "stored-pat").await;
    let mut attached = h.wss(TOKEN).await;
    success(&attached.rpc("client.hello", json!({"clientId":success(&hello)["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await);
    let subscribed = attached
        .rpc(
            "events.subscribe",
            json!({"workspaceId":h.workspace,"eventTypes":["prMonitor:*"]}),
        )
        .await;
    let subscription = success(&subscribed)["subscriptionId"].clone();
    std::fs::rename(&model, native.join("model-before-recovery.json")).unwrap();
    success(&ordinary.rpc("agent.sendMessage", json!({"workspaceId":h.workspace,"agentId":agent,"content":"Ordinary original-account recovery control"})).await);
    let resumed = idle_model(&store, &agent, &model, 1).await;
    // Model receipt does not establish native transcript persistence before a crash.
    // The held response may leave the interrupted input absent from replay.
    let interrupted_input_replayed = resumed
        .to_string()
        .contains("Intent review monitor observation (not a user instruction).");
    if !transferred {
        assert!(!interrupted_input_replayed);
    }
    let ordinary_turns: Vec<_> = resumed[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| {
            message["role"] == "user"
                && message["content"].as_array().is_some_and(|blocks| {
                    blocks
                        .iter()
                        .any(|b| b["text"] == "Ordinary original-account recovery control")
                })
        })
        .collect();
    assert_eq!(ordinary_turns.len(), 1);
    assert!(!ordinary_turns[0]
        .to_string()
        .contains("Intent review monitor observation (not a user instruction)."));
    // timing-guard: observe 21 seconds with the configured 10-second scheduler interval, without automatic resend
    tokio::time::sleep(Duration::from_secs(21)).await;
    assert_eq!(store.get_pr_monitor(&id).await.unwrap(), pending);
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap()).unwrap(),
        resumed
    );
    let listed = attached
        .rpc("prMonitor.list", json!({"workspaceId":h.workspace}))
        .await;
    let listed_row = success(&listed)["monitors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["monitorId"] == id.0)
        .unwrap();
    assert_eq!(listed_row["hasPendingChanges"], true);
    let query = json!({"workspaceId":h.workspace,"monitorId":id,"check":true});
    // No net change must not acknowledge the saved original pending evidence.
    *account.draft.lock().unwrap() = false;
    let noop = attached.rpc("prMonitor.flush", query.clone()).await;
    assert_eq!(success(&noop)["flushed"], false);
    assert_eq!(store.get_pr_monitor(&id).await.unwrap(), pending);
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap()).unwrap(),
        resumed
    );
    *account.draft.lock().unwrap() = true;
    let recovered = attached.rpc("prMonitor.flush", query).await;
    assert_eq!(success(&recovered)["flushed"], true);
    let emitted = event(&mut attached, &subscription, "prMonitor:emitted").await;
    assert_eq!(emitted["params"]["event"]["data"]["monitorId"], id.0);
    let delivered = idle_model(&store, &agent, &model, 2).await;
    assert!(delivered[1]
        .to_string()
        .contains("Intent review monitor observation (not a user instruction)."));
    assert!(store
        .get_pr_monitor(&id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(Path::new(&dir).join(if transferred { "daemon-native-transferred-uncertain-recovery.json" } else { "daemon-native-lifecycle-recovery.json" }), serde_json::to_vec_pretty(&json!({"observedTransfer":observed_transfer,"interruptedInputReplayed":interrupted_input_replayed,"abrupt":transferred,"daemonPids":[original_pid,h.child.id()],"pending":pending,"list":listed,"noop":noop,"recovered":recovered,"emitted":emitted,"resumed":resumed,"delivered":delivered})).unwrap()).unwrap();
    }
    let mut shutdown = h.uds().await;
    success(&shutdown.rpc("system.shutdown", json!({})).await);
}
