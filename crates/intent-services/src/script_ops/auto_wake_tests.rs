// Turn-end script wake guard (script_wake_guard.rs): an agent that ends its
// turn while a command script it started is still running, with nothing
// watching that script, gets an auto-armed script monitor.

use crate::script_wake_guard::{WakeDecision, AUTO_SCRIPT_MONITOR_TTL_MS};

/// Start `id` as `agent` through the same trait calls the MCP binding makes.
async fn agent_start(h: &Harness, agent: &intent_core::AgentId, id: &str) -> String {
    let reply = h.services.script_start(h.ws.clone(), id.into()).await.unwrap();
    let run = reply["runId"].as_str().expect("start reply runId").to_owned();
    h.services
        .script_note_agent_start(h.ws.clone(), agent.clone(), id.into(), run.clone())
        .await
        .unwrap();
    run
}

fn gate_command(gate: &std::path::Path, code: i32) -> String {
    format!(
        "while [ ! -e '{}' ]; do sleep 0.05; done; exit {code}",
        gate.display()
    )
}

async fn active_monitors(h: &Harness, id: &str) -> Vec<intent_core::ScriptMonitor> {
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
async fn auto_wake_arms_monitor_and_wakes_owner_with_exit_status() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let gate = tempfile::tempdir().unwrap();
    let gate_path = gate.path().join("go");
    let id = create_simple(&h, "long build", &gate_command(&gate_path, 3), ScriptMode::Command).await;
    let run = agent_start(&h, &agent, &id).await;

    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    let WakeDecision::Armed(monitor_id) = &decisions[0].1 else {
        panic!("expected an armed monitor: {decisions:?}");
    };
    let armed = h.services.store.script_monitor(&h.ws, monitor_id).await.unwrap();
    assert_eq!(armed.agent_id, agent);
    assert_eq!(armed.run_id, run);
    assert_eq!(armed.state, "active");
    let ttl = chrono::DateTime::parse_from_rfc3339(&armed.expires_at).unwrap()
        - chrono::DateTime::parse_from_rfc3339(&armed.created_at).unwrap();
    assert_eq!(ttl.num_milliseconds(), AUTO_SCRIPT_MONITOR_TTL_MS);

    std::fs::write(&gate_path, b"").unwrap();
    let done = monitor_row(&h, monitor_id, "completed").await;
    assert_eq!(done.result.as_ref().and_then(|r| r.exit_code), Some(3));
    assert!(
        intent_core::script_monitor::script_monitor_wake_text(&done).contains("\"exitCode\":3"),
        "the wake carries the run's exit status"
    );

    // One-shot: a second turn end does not re-arm the finished run.
    assert!(h.services.arm_idle_script_wakes(&agent).await.is_empty());
}

#[intent_test_macros::daemon_test]
async fn auto_wake_skips_script_already_monitored() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "watched build", "sleep 30", ScriptMode::Command).await;
    let run = agent_start(&h, &agent, &id).await;
    let own = h
        .services
        .script_monitor(h.ws.clone(), agent.clone(), id.clone(), json!({"ttlMs":60000,"runId":run}))
        .await
        .unwrap();
    assert_eq!(own["ok"], true, "{own}");

    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].1, WakeDecision::Monitored);
    assert_eq!(active_monitors(&h, &id).await.len(), 1, "no duplicate monitor");
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn auto_wake_skips_script_named_by_active_hook() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "hooked build", "sleep 30", ScriptMode::Command).await;
    agent_start(&h, &agent, &id).await;
    h.services
        .store
        .insert_hook(&intent_core::Hook {
            hook_id: intent_core::HookId::new(),
            workspace_id: h.ws.clone(),
            agent_id: agent.clone(),
            name: "wait for build".into(),
            code: format!("const s = await ws.script.status({id:?}); return {{dispatch: s.status === 'exited'}};"),
            delay_ms: 60_000,
            cron: None,
            run_at: None,
            state: intent_core::HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: None,
            perpetual: false,
            dispatch_count: 0,
        })
        .await
        .unwrap();

    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions[0].1, WakeDecision::HookCovers);
    assert!(active_monitors(&h, &id).await.is_empty());
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn auto_wake_ignores_service_mode_scripts() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "dev server", "sleep 30", ScriptMode::Service).await;
    agent_start(&h, &agent, &id).await;

    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions[0].1, WakeDecision::NotCommand);
    assert!(active_monitors(&h, &id).await.is_empty());
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn auto_wake_defers_while_messages_are_queued() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "queued build", "sleep 30", ScriptMode::Command).await;
    agent_start(&h, &agent, &id).await;

    h.services
        .agent_queues
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![crate::human_attribution_tests::imported_pending("q1")]);
    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions[0].1, WakeDecision::Deferred);
    assert!(active_monitors(&h, &id).await.is_empty());

    h.services.agent_queues.lock().unwrap().remove(&agent);
    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert!(matches!(decisions[0].1, WakeDecision::Armed(_)), "{decisions:?}");
    assert_eq!(active_monitors(&h, &id).await.len(), 1);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn auto_wake_skips_runs_that_already_finished() {
    let h = harness().await;
    let agent = monitor_owner(&h, "builder").await;
    let id = create_simple(&h, "quick", "true", ScriptMode::Command).await;
    let run = agent_start(&h, &agent, &id).await;
    let mgr = h.services.script_manager();
    tokio::time::timeout(LIVENESS, async {
        while mgr.run_is_live(h.ws.as_str(), &id, &run) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("quick run settles");

    let decisions = h.services.arm_idle_script_wakes(&agent).await;
    assert_eq!(decisions[0].1, WakeDecision::NotLive);
    assert!(active_monitors(&h, &id).await.is_empty());
}
