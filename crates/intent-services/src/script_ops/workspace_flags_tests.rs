// `Workspace.scriptsRunning` and the script-monitor `Workspace.waiting` signal
// (PROTOCOL §5.1 / §6.5), driven through the real script manager.

/// Persisted boolean payloads of `event_type` for the harness workspace,
/// oldest-first.
async fn workspace_flag_events(h: &Harness, event_type: &str, key: &str) -> Vec<bool> {
    let mut events = h
        .services
        .store()
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec![event_type.to_string()],
            ..Default::default()
        })
        .await
        .expect("query workspace flag events");
    events.reverse();
    events
        .into_iter()
        .map(|e| e.data[key].as_bool().expect("boolean flag payload"))
        .collect()
}

async fn scripts_running_events(h: &Harness) -> Vec<bool> {
    workspace_flag_events(
        h,
        intent_core::events::WORKSPACE_SCRIPTS_RUNNING_CHANGED,
        "scriptsRunning",
    )
    .await
}

async fn waiting_flag_events(h: &Harness) -> Vec<bool> {
    workspace_flag_events(h, intent_core::events::WORKSPACE_WAITING_CHANGED, "waiting").await
}

/// Poll until `read` yields `expected` (the recompute runs just after the
/// `script:*` publication a test observes), failing at `LIVENESS`.
async fn await_flag_events<F, Fut>(expected: Vec<bool>, mut read: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Vec<bool>>,
{
    let deadline = tokio::time::Instant::now() + LIVENESS;
    loop {
        let seen = read().await;
        if seen == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "flag events never reached {expected:?}; last seen {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn served_scripts_running(h: &Harness) -> bool {
    let ws = intent_core::WorkspaceApi::get_workspace(&h.services, h.ws.clone())
        .await
        .expect("get workspace");
    let wire = serde_json::to_value(&ws).expect("serialize workspace");
    assert_eq!(
        wire.get("scriptsRunning").is_some(),
        ws.scripts_running,
        "presence-detected on the wire: {wire}"
    );
    ws.scripts_running
}

/// A running command raises `scriptsRunning` and emits one transition; its
/// exit drops it. A running service never counts, so it neither raises the
/// flag nor emits.
#[intent_test_macros::daemon_test]
async fn scripts_running_tracks_command_runs_and_ignores_services() {
    let h = harness().await;
    let mut sub = subscribe(&h);
    assert!(!served_scripts_running(&h).await, "nothing runs yet");

    let service = create_simple(&h, "dev server", SERVICE_CMD, ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), service.clone())
        .await
        .expect("start service");
    await_state(&mut sub, LIVENESS, |v| {
        v["data"]["scriptId"] == service.as_str() && v["data"]["status"] == "running"
    })
    .await;
    assert!(!served_scripts_running(&h).await, "a service never counts");
    assert!(scripts_running_events(&h).await.is_empty());

    let command = create_simple(&h, "check", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), command.clone())
        .await
        .expect("start command");
    await_flag_events(vec![true], || scripts_running_events(&h)).await;
    assert!(served_scripts_running(&h).await, "command run in flight");
    let events = h
        .services
        .store()
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec![intent_core::events::WORKSPACE_SCRIPTS_RUNNING_CHANGED.to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(events[0].data["workspaceId"], json!(h.ws.as_str()));

    // A no-op recompute stays silent.
    h.services.maybe_emit_scripts_running_changed(&h.ws).await;
    assert_eq!(scripts_running_events(&h).await, vec![true]);

    h.services
        .script_stop(h.ws.clone(), command)
        .await
        .expect("stop command");
    await_flag_events(vec![true, false], || scripts_running_events(&h)).await;
    assert!(!served_scripts_running(&h).await, "command settled");

    h.services
        .script_stop(h.ws.clone(), service)
        .await
        .expect("stop service");
    assert_eq!(scripts_running_events(&h).await, vec![true, false]);
}

/// Removing the definition of a running command drops the flag too.
#[intent_test_macros::daemon_test]
async fn scripts_running_drops_when_running_command_is_removed() {
    let h = harness().await;
    let command = create_simple(&h, "check", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), command.clone())
        .await
        .expect("start command");
    await_flag_events(vec![true], || scripts_running_events(&h)).await;
    h.services
        .script_remove(h.ws.clone(), command)
        .await
        .expect("remove command");
    await_flag_events(vec![true, false], || scripts_running_events(&h)).await;
    assert!(!h.services.workspace_scripts_running(&h.ws));
}

/// An active script run monitor is a `waiting` signal: registration raises
/// the flag and the cancel that ends observation drops it.
#[intent_test_macros::daemon_test]
async fn active_script_monitor_sets_and_clears_waiting() {
    let h = harness().await;
    let owner = monitor_owner(&h, "monitor owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    // Seed the waiting baseline (a seed never emits).
    h.services.maybe_emit_waiting_changed(&h.ws).await;
    assert!(waiting_flag_events(&h).await.is_empty());

    let mgr = h.services.script_manager();
    let row = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":60000}))
        .await
        .expect("register monitor")["monitor"]
        .clone();
    assert!(h.services.workspace_is_waiting(&h.ws).await);
    assert_eq!(waiting_flag_events(&h).await, vec![true]);
    let ws = intent_core::WorkspaceApi::get_workspace(&h.services, h.ws.clone())
        .await
        .expect("get workspace");
    assert!(ws.waiting, "served waiting while the monitor is active");

    mgr.cancel_monitor(&h.ws, row["monitorId"].as_str().unwrap(), None, false)
        .await
        .expect("cancel monitor");
    assert!(!h.services.workspace_is_waiting(&h.ws).await);
    assert_eq!(waiting_flag_events(&h).await, vec![true, false]);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}
