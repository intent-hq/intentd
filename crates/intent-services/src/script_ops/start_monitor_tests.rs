// Default-on start monitor (`WorkspaceApi::script_start_monitor`): an agent's
// `ws.script.start` arms the same monitor `ws.script.monitor` would, so the
// agent is woken when a command-mode run finishes even if it ends its turn
// without arming anything.

/// Start `id` as `agent` the way the MCP binding does: start, then arm.
async fn agent_start_armed(
    h: &Harness,
    agent: &intent_core::AgentId,
    id: &str,
) -> (String, Value) {
    let reply = h.services.script_start(h.ws.clone(), id.into()).await.unwrap();
    let run = reply["runId"].as_str().expect("start reply runId").to_owned();
    let armed = h
        .services
        .script_start_monitor(h.ws.clone(), agent.clone(), id.into(), run.clone())
        .await
        .unwrap();
    (run, armed)
}

async fn monitors_for(h: &Harness, id: &str) -> Vec<intent_core::ScriptMonitor> {
    h.services
        .store
        .script_monitors(&h.ws, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| m.script_id == id && m.state == "active")
        .collect()
}

#[intent_test_macros::daemon_test]
async fn start_monitor_wakes_starter_with_exit_status() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let gate = tempfile::tempdir().unwrap();
    let gate_path = gate.path().join("go");
    let command = format!(
        "while [ ! -e '{}' ]; do sleep 0.05; done; exit 3",
        gate_path.display()
    );
    let id = create_simple(&h, "long build", &command, ScriptMode::Command).await;
    let (run, armed) = agent_start_armed(&h, &agent, &id).await;
    assert_eq!(armed["ok"], true, "{armed}");
    let row: intent_core::ScriptMonitor =
        serde_json::from_value(armed["monitor"].clone()).unwrap();
    assert_eq!(row.agent_id, agent);
    assert_eq!(row.run_id, run);
    assert_eq!(row.state, "active");
    let ttl = chrono::DateTime::parse_from_rfc3339(&row.expires_at).unwrap()
        - chrono::DateTime::parse_from_rfc3339(&row.created_at).unwrap();
    assert_eq!(ttl.num_milliseconds(), AUTO_START_MONITOR_TTL_MS);

    std::fs::write(&gate_path, b"").unwrap();
    let done = monitor_row(&h, &row.monitor_id, "completed").await;
    assert_eq!(done.result.as_ref().and_then(|r| r.exit_code), Some(3));
    assert!(
        intent_core::script_monitor::script_monitor_wake_text(&done).contains("\"exitCode\":3"),
        "the wake carries the run's exit status"
    );
}

#[intent_test_macros::daemon_test]
async fn explicit_monitor_after_start_replaces_the_auto_monitor() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "watched build", "sleep 30", ScriptMode::Command).await;
    let (run, armed) = agent_start_armed(&h, &agent, &id).await;
    let auto_id = armed["monitor"]["monitorId"].as_str().unwrap().to_owned();
    let options = json!({"ttlMs": 60000, "runId": run, "outputPattern": "^READY$"});
    let explicit = h
        .services
        .script_monitor(h.ws.clone(), agent.clone(), id.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(explicit["ok"], true, "{explicit}");
    let explicit_id = explicit["monitor"]["monitorId"].as_str().unwrap().to_owned();
    assert_ne!(explicit_id, auto_id, "the agent's own options win");
    assert_eq!(explicit["monitor"]["outputPattern"], "^READY$");
    let active = monitors_for(&h, &id).await;
    assert_eq!(active.len(), 1, "no duplicate wake: {active:?}");
    assert_eq!(active[0].monitor_id, explicit_id);

    // A retry of the explicit request keeps the explicit monitor.
    let retry = h
        .services
        .script_monitor(h.ws.clone(), agent.clone(), id.clone(), options)
        .await
        .unwrap();
    assert_eq!(retry["monitor"]["monitorId"], json!(explicit_id), "{retry}");
    assert_eq!(monitors_for(&h, &id).await.len(), 1);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn another_agents_explicit_monitor_does_not_replace_the_auto_monitor() {
    let h = harness().await;
    let starter = monitor_owner(&h, "starter").await;
    let other = monitor_owner(&h, "other").await;
    let id = create_simple(&h, "owned build", "sleep 30", ScriptMode::Command).await;
    let (run, armed) = agent_start_armed(&h, &starter, &id).await;
    let refused = h
        .services
        .script_monitor(
            h.ws.clone(),
            other,
            id.clone(),
            json!({"ttlMs": 60000, "runId": run}),
        )
        .await
        .unwrap();
    assert_eq!(refused["reason"], "already-monitored", "{refused}");
    let active = monitors_for(&h, &id).await;
    assert_eq!(active.len(), 1);
    assert_eq!(json!(active[0].monitor_id), armed["monitor"]["monitorId"]);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn start_monitor_skips_service_mode_scripts() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "dev server", "sleep 30", ScriptMode::Service).await;
    let (_, armed) = agent_start_armed(&h, &agent, &id).await;
    assert_eq!(armed, Value::Null);
    assert!(monitors_for(&h, &id).await.is_empty());
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn start_monitor_reports_another_owners_monitor_without_duplicating() {
    let h = harness().await;
    let first = monitor_owner(&h, "first").await;
    let second = monitor_owner(&h, "second").await;
    let id = create_simple(&h, "shared build", "sleep 30", ScriptMode::Command).await;
    let (run, armed) = agent_start_armed(&h, &first, &id).await;
    assert_eq!(armed["ok"], true, "{armed}");
    // A second agent "starting" the live run is a no-op start of the same run.
    let again = h
        .services
        .script_start_monitor(h.ws.clone(), second.clone(), id.clone(), run)
        .await
        .unwrap();
    assert_eq!(again["reason"], "already-monitored", "{again}");
    assert_eq!(again["ownerAgentId"], json!(first));
    assert_eq!(monitors_for(&h, &id).await.len(), 1);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}
