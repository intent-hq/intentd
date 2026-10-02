async fn monitor_owner(h: &Harness, name: &str) -> intent_core::AgentId {
    let value = h
        .services
        .agent_create(
            h.ws.clone(),
            Some(name.into()),
            Some("sonnet4.5".into()),
            None,
            None,
            None,
            intent_core::AgentCreateExtra {
                provider: Some("auggie".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    intent_core::AgentId::from(value["agent"]["id"].as_str().unwrap())
}

async fn monitor_row(h: &Harness, id: &str, state: &str) -> intent_core::ScriptMonitor {
    tokio::time::timeout(LIVENESS, async {
        loop {
            let row = h.services.store.script_monitor(&h.ws, id).await.unwrap();
            if row.state == state {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("monitor terminal deadline")
}

#[intent_test_macros::daemon_test]
async fn monitor_registration_owner_retry_cancel_and_run_token_are_atomic() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let other = monitor_owner(&h, "other").await;
    let id = create_simple(&h, "blocked", "read value", ScriptMode::Command).await;
    let start = h
        .services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        h.services.script_monitor(
            h.ws.clone(),
            owner.clone(),
            id.clone(),
            json!({"ttlMs":60000})
        ),
        h.services.script_monitor(
            h.ws.clone(),
            other.clone(),
            id.clone(),
            json!({"ttlMs":60000})
        )
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a["ok"], b["ok"]);
    let winner = if a["ok"] == true { a } else { b };
    let owner = intent_core::AgentId::from(winner["monitor"]["agentId"].as_str().unwrap());
    let mid = winner["monitor"]["monitorId"].as_str().unwrap().to_owned();
    assert_eq!(winner["monitor"]["runId"], start["runId"]);
    assert_eq!(
        h.services
            .script_monitor(
                h.ws.clone(),
                owner.clone(),
                id.clone(),
                json!({"ttlMs":1,"lineCount":2})
            )
            .await
            .unwrap(),
        winner
    );
    assert!(h
        .services
        .script_monitor(
            h.ws.clone(),
            owner.clone(),
            id.clone(),
            json!({"ttlMs":null})
        )
        .await
        .is_err());
    assert_eq!(
        h.services
            .active_script_monitors_for_agent(&owner)
            .await
            .len(),
        1
    );
    let stopped = h
        .services
        .script_monitor_cancel(h.ws.clone(), mid.clone(), None, true)
        .await
        .unwrap();
    assert_eq!(stopped["runStopped"], true);
    assert_eq!(stopped["monitor"]["result"]["outcome"], "cancelled");
    assert!(h
        .services
        .active_script_monitors_for_agent(&owner)
        .await
        .is_empty());
    let next = h
        .services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    assert_ne!(next["runId"], start["runId"]);
    assert_eq!(
        h.services
            .script_monitor_cancel(h.ws.clone(), mid, None, true)
            .await
            .unwrap()["runStopped"],
        false
    );
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn monitor_fast_completion_retries_have_one_durable_message() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let id = one_off(&h, "printf hidden-output").await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let final_run = retired(&h, &id).await;
    assert!(final_run.last_run.unwrap().run_id.is_some());
    // Terminal registration must survive explicit PTY/output eviction.
    h.services.script_manager().pty.kill_all().await;
    let first = h
        .services
        .script_monitor(
            h.ws.clone(),
            owner.clone(),
            id.clone(),
            json!({"ttlMs":60000}),
        )
        .await
        .unwrap();
    assert_eq!(first["monitor"]["state"], "completed");
    assert!(!first.to_string().contains("hidden-output"));
    let mid = first["monitor"]["monitorId"].as_str().unwrap();
    let row = h.services.store.script_monitor(&h.ws, mid).await.unwrap();
    h.services.dispatch_script_monitor(&row).await;
    h.services.dispatch_script_monitor(&row).await;
    assert_eq!(
        h.services
            .script_monitor(h.ws.clone(), owner.clone(), id, json!({"ttlMs":100}))
            .await
            .unwrap(),
        first
    );
    assert!(h
        .services
        .store
        .get_agent_message_by_id_with_pruned(&owner, &format!("script-monitor:{mid}"))
        .await
        .unwrap()
        .is_some());
    assert!(!h
        .services
        .store
        .script_monitor_wake_pending(mid)
        .await
        .unwrap());
}

#[intent_test_macros::daemon_test]
async fn monitor_output_fresh_window_regex_priority_rearm_and_eof() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let mgr = h.services.script_manager();
    let pty = tokio::time::timeout(LIVENESS, async {
        loop {
            if let Some(p) = mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .and_then(|m| m.pty_id)
            {
                break p;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let value = h
        .services
        .script_monitor(
            h.ws.clone(),
            owner.clone(),
            id.clone(),
            json!({"ttlMs":60000,"outputPattern":"^ready$","lineCount":1}),
        )
        .await
        .unwrap();
    let mid = value["monitor"]["monitorId"].as_str().unwrap();
    mgr.monitor_output(
        &h.ws,
        &id,
        pty,
        Some(&intent_pty::OutputChunk {
            bytes: b"rea".to_vec(),
            start_offset: 0,
            end_offset: 3,
        }),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        h.services
            .store
            .script_monitor(&h.ws, mid)
            .await
            .unwrap()
            .state,
        "active"
    );
    mgr.monitor_output(
        &h.ws,
        &id,
        pty,
        Some(&intent_pty::OutputChunk {
            bytes: b"dy\r\n".to_vec(),
            start_offset: 3,
            end_offset: 7,
        }),
        false,
    )
    .await
    .unwrap();
    let row = monitor_row(&h, mid, "triggered").await;
    assert_eq!(row.reason.as_deref(), Some("output-match"));
    assert_eq!(row.trigger.unwrap().matched_line.as_deref(), Some("ready"));
    let next = h
        .services
        .script_monitor(
            h.ws.clone(),
            owner,
            id.clone(),
            json!({"ttlMs":60000,"lineCount":1}),
        )
        .await
        .unwrap();
    assert_ne!(next["monitor"]["monitorId"], value["monitor"]["monitorId"]);
    mgr.monitor_output(
        &h.ws,
        &id,
        pty,
        Some(&intent_pty::OutputChunk {
            bytes: b"tail".to_vec(),
            start_offset: 0,
            end_offset: 4,
        }),
        false,
    )
    .await
    .unwrap();
    mgr.monitor_output(&h.ws, &id, pty, None, true)
        .await
        .unwrap();
    let row = monitor_row(
        &h,
        next["monitor"]["monitorId"].as_str().unwrap(),
        "triggered",
    )
    .await;
    assert_eq!(row.reason.as_deref(), Some("line-count"));
    assert_eq!(row.trigger.unwrap().observed_line_count, 1);
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn monitor_cleanup_suppresses_pending_outbox_and_never_revives() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let value = h
        .services
        .script_monitor(
            h.ws.clone(),
            owner.clone(),
            id.clone(),
            json!({"ttlMs":60000}),
        )
        .await
        .unwrap();
    let mid = value["monitor"]["monitorId"].as_str().unwrap();
    h.services
        .cancel_script_monitors(&h.ws, Some(&owner), "owner-retired")
        .await
        .unwrap();
    let row = h.services.store.script_monitor(&h.ws, mid).await.unwrap();
    assert_eq!(row.state, "cancelled");
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
    h.services.dispatch_script_monitor(&row).await;
    h.services
        .script_manager()
        .recover_monitors()
        .await
        .unwrap();
    assert!(h
        .services
        .store
        .get_agent_message_by_id_with_pruned(&owner, &format!("script-monitor:{mid}"))
        .await
        .unwrap()
        .is_none());
    assert!(h
        .services
        .active_script_monitors_for_agent(&owner)
        .await
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn monitor_exact_deadline_beats_new_cancel_without_stopping_run() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let now = chrono::Utc::now().timestamp_millis() + 60000;
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(now));
    let mut mgr = h.services.script_manager();
    mgr.parks.monitor_clock = Some(clock.clone());
    let row = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":1000}))
        .await
        .unwrap()["monitor"]
        .clone();
    clock.store(now + 1000, Ordering::SeqCst);
    let result = mgr
        .cancel_monitor(&h.ws, row["monitorId"].as_str().unwrap(), None, true)
        .await
        .unwrap();
    assert_eq!(result["monitor"]["state"], "expired");
    assert_eq!(result["runStopped"], false);
    assert!(mgr.is_live(&h.ws, &id).unwrap());
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn monitor_cancel_before_spawn_reserves_outcome_against_deadline() {
    let h = harness().await;
    let owner = monitor_owner(&h, "owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    let now = chrono::Utc::now().timestamp_millis() + 60000;
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(now));
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.before_spawn = Some(park.clone());
    mgr.parks.monitor_clock = Some(clock.clone());
    mgr.start(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let row = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":1000}))
        .await
        .unwrap()["monitor"]
        .clone();
    let mid = row["monitorId"].as_str().unwrap().to_owned();
    let stop_mgr = mgr.clone();
    let ws = h.ws.clone();
    let stop_mid = mid.clone();
    let stopped =
        tokio::spawn(async move { stop_mgr.cancel_monitor(&ws, &stop_mid, None, true).await });
    tokio::time::timeout(LIVENESS, async {
        loop {
            if h.services
                .store
                .pending_script_monitors()
                .await
                .unwrap()
                .iter()
                .any(|(row, reserved)| row.monitor_id == mid && *reserved)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.store(now + 1000, Ordering::SeqCst);
    mgr.reconcile_monitor(&h.ws, &mid).await.unwrap();
    assert_eq!(
        h.services
            .store
            .script_monitor(&h.ws, &mid)
            .await
            .unwrap()
            .state,
        "active"
    );
    park.release.notify_one();
    let result = tokio::time::timeout(LIVENESS, stopped)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result["runStopped"], true);
    assert_eq!(result["monitor"]["result"]["outcome"], "cancelled");
    assert!(result["monitor"]["result"].get("startedAt").is_none());
    assert!(result["monitor"]["result"].get("exitCode").is_none());
}

#[intent_test_macros::daemon_test]
async fn monitor_recovery_prior_result_deadline_interruption_and_cancel_intent() {
    for scenario in ["result", "deadline", "interrupted", "cancel-intent"] {
        let h = harness().await;
        let owner = monitor_owner(&h, "recovered owner").await;
        let id = create_simple(&h, "prior boot", "true", ScriptMode::Command).await;
        h.services
            .store
            .admit_script_run(&h.ws, &id, "prior-boot-token")
            .await
            .unwrap();
        let now = chrono::Utc::now().timestamp_millis() + 60000;
        let clock = Arc::new(std::sync::atomic::AtomicI64::new(now));
        let mut mgr = h.services.script_manager();
        mgr.parks.monitor_clock = Some(clock.clone());
        let registered = mgr
            .monitor(
                &h.ws,
                &owner,
                &id,
                json!({"ttlMs":1000,"outputPattern":"ready"}),
            )
            .await
            .unwrap();
        let mid = registered["monitor"]["monitorId"].as_str().unwrap();
        if scenario == "result" {
            let result = intent_core::ScriptLastRun {
                run_id: Some("prior-boot-token".into()),
                outcome: intent_core::ScriptRunOutcome::Succeeded,
                exit_code: Some(0),
                started_at: None,
                stopped_at: now_iso(),
                error: None,
            };
            h.services
                .store
                .settle_script_run(&h.ws, &id, "prior-boot-token", &result, false)
                .await
                .unwrap();
        }
        if scenario == "cancel-intent" {
            h.services
                .store
                .script_monitor_cancel_intent(&h.ws, mid, true)
                .await
                .unwrap();
        }
        if scenario != "interrupted" {
            clock.store(now + 1000, Ordering::SeqCst);
        }
        // New runtime locks model a daemon boot; the same isolated database is
        // the durable boundary. No process from the original boot is attached.
        mgr.locks = ScriptLocks::default();
        mgr.recover_monitors().await.unwrap();
        let row = h.services.store.script_monitor(&h.ws, mid).await.unwrap();
        if scenario == "deadline" {
            assert_eq!(row.state, "expired");
            assert!(row.result.is_none());
        } else {
            assert_eq!(row.state, "completed");
            assert_eq!(
                row.result.as_ref().unwrap().outcome,
                match scenario {
                    "result" => intent_core::ScriptRunOutcome::Succeeded,
                    "cancel-intent" => intent_core::ScriptRunOutcome::Cancelled,
                    _ => intent_core::ScriptRunOutcome::Interrupted,
                }
            );
        }
        h.services.dispatch_script_monitor(&row).await;
        mgr.recover_monitors().await.unwrap();
        assert!(h
            .services
            .store
            .get_agent_message_by_id_with_pruned(&owner, &format!("script-monitor:{mid}"))
            .await
            .unwrap()
            .is_some());
    }
}

#[intent_test_macros::daemon_test]
async fn monitor_active_cap_retry_and_failed_registration_leave_no_memory_watch() {
    let h = harness().await;
    let owner = monitor_owner(&h, "bounded owner").await;
    let mut ids = Vec::new();
    for index in 0..6 {
        let id = create_simple(&h, &format!("run {index}"), "true", ScriptMode::Command).await;
        h.services
            .store
            .admit_script_run(&h.ws, &id, &format!("token-{index}"))
            .await
            .unwrap();
        ids.push(id);
    }
    for id in &ids[..5] {
        h.services
            .script_monitor(
                h.ws.clone(),
                owner.clone(),
                id.clone(),
                json!({"ttlMs":60000}),
            )
            .await
            .unwrap();
    }
    assert!(h
        .services
        .script_monitor(
            h.ws.clone(),
            owner.clone(),
            ids[5].clone(),
            json!({"ttlMs":60000})
        )
        .await
        .is_err());
    assert_eq!(
        h.services
            .script_monitor(
                h.ws.clone(),
                owner.clone(),
                ids[0].clone(),
                json!({"ttlMs":1})
            )
            .await
            .unwrap()["ok"],
        true
    );
    assert_eq!(
        h.services
            .script_locks
            .monitor_windows
            .lock()
            .unwrap()
            .len(),
        5
    );
    h.services
        .cancel_script_monitors(&h.ws, Some(&owner), "owner-retired")
        .await
        .unwrap();
    assert!(h
        .services
        .script_locks
        .monitor_windows
        .lock()
        .unwrap()
        .is_empty());
    // Retiring in durable storage prevents admission even if the runtime is
    // still present; no acknowledged memory-only watcher may survive.
    h.services
        .store
        .set_agent_session_retired_at(&h.ws, &owner, Some(&now_iso()), &now_iso())
        .await
        .unwrap();
    assert!(h
        .services
        .script_monitor(h.ws.clone(), owner, ids[5].clone(), json!({"ttlMs":60000}))
        .await
        .is_err());
    assert!(h
        .services
        .script_locks
        .monitor_windows
        .lock()
        .unwrap()
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn monitor_service_backoff_keeps_token_and_manual_restart_settles_predecessor() {
    let h = harness().await;
    let owner = monitor_owner(&h, "service owner").await;
    let id = create_simple(&h, "retrying service", "true", ScriptMode::Service).await;
    let mut mgr = h.services.script_manager();
    mgr.too_fast_ms = 0;
    let started = mgr.start(&h.ws, &id).await.unwrap();
    let watch = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":60000}))
        .await
        .unwrap();
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr.status(&h.ws, &id).unwrap()["status"] == "restarting" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(mgr.status(&h.ws, &id).unwrap()["runId"], started["runId"]);
    assert_eq!(
        h.services
            .store
            .script_monitor(&h.ws, watch["monitor"]["monitorId"].as_str().unwrap())
            .await
            .unwrap()
            .state,
        "active"
    );
    let restarted = mgr.restart(&h.ws, &id).await.unwrap();
    assert_ne!(restarted["runId"], started["runId"]);
    let predecessor = h
        .services
        .store
        .script_monitor(&h.ws, watch["monitor"]["monitorId"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(predecessor.state, "completed");
    assert_eq!(
        predecessor.result.unwrap().outcome,
        intent_core::ScriptRunOutcome::Cancelled
    );
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap()
        .last_run
        .is_none());
    mgr.stop(&h.ws, &id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn monitor_storage_failure_retries_reserved_trigger_without_duplicate_or_ttl_substitution() {
    let h = harness().await;
    let owner = monitor_owner(&h, "fault owner").await;
    let id = create_simple(&h, "controlled", "read value", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let now = chrono::Utc::now().timestamp_millis() + 60000;
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(now));
    let mut mgr = h.services.script_manager();
    mgr.parks.monitor_clock = Some(clock.clone());
    let pty = tokio::time::timeout(LIVENESS, async {
        loop {
            if let Some(p) = mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .and_then(|m| m.pty_id)
            {
                break p;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER refuse_monitor_insert BEFORE INSERT ON script_monitor BEGIN SELECT RAISE(FAIL,'injected'); END").execute(h.services.store.write_pool()).await.unwrap();
    assert!(mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":1000,"lineCount":1}))
        .await
        .is_err());
    assert!(mgr.locks.monitor_windows.lock().unwrap().is_empty());
    sqlx::query("DROP TRIGGER refuse_monitor_insert")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    let watch = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":1000,"lineCount":1}))
        .await
        .unwrap();
    let mid = watch["monitor"]["monitorId"].as_str().unwrap();
    sqlx::query("CREATE TRIGGER refuse_monitor_result BEFORE UPDATE OF state ON script_monitor BEGIN SELECT RAISE(FAIL,'injected'); END").execute(h.services.store.write_pool()).await.unwrap();
    assert!(mgr
        .monitor_output(
            &h.ws,
            &id,
            pty,
            Some(&intent_pty::OutputChunk {
                bytes: b"line\n".to_vec(),
                start_offset: 0,
                end_offset: 5
            }),
            false
        )
        .await
        .is_err());
    clock.store(now + 1000, Ordering::SeqCst);
    sqlx::query("DROP TRIGGER refuse_monitor_result")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    mgr.reconcile_monitor(&h.ws, mid).await.unwrap();
    let row = h.services.store.script_monitor(&h.ws, mid).await.unwrap();
    assert_eq!(row.state, "triggered");
    assert_eq!(row.trigger.unwrap().observed_line_count, 1);
    mgr.stop(&h.ws, &id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn monitor_restore_failure_keeps_legacy_marker_and_compact_failure() {
    let h = harness_with_worktree(true).await;
    let owner = monitor_owner(&h, "restore owner").await;
    let id = create(
        &h,
        ScriptCreateParams {
            name: "restore failure".into(),
            command: "true".into(),
            mode: ScriptMode::Service,
            cwd: Some("../escape".into()),
            ..Default::default()
        },
    )
    .await;
    let mgr = h.services.script_manager();
    mgr.scripts
        .lock()
        .unwrap()
        .get_mut(&(h.ws.clone(), id.clone()))
        .unwrap()
        .state
        .previously_running = Some(true);
    h.services
        .store
        .set_script_was_running(h.ws.as_str(), &id, true)
        .await
        .unwrap();
    mgr.start_with_restore(&h.ws, &id, true).await.unwrap();
    let watch = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":60000}))
        .await
        .unwrap();
    let row = monitor_row(
        &h,
        watch["monitor"]["monitorId"].as_str().unwrap(),
        "completed",
    )
    .await;
    let result = row.result.unwrap();
    assert_eq!(result.outcome, intent_core::ScriptRunOutcome::Failed);
    assert_eq!(result.exit_code, Some(-1));
    assert!(result.error.is_some());
    assert!(result.started_at.is_none());
    assert_eq!(mgr.status(&h.ws, &id).unwrap()["previouslyRunning"], true);
    assert_eq!(
        h.services
            .store
            .list_was_running_script_ids()
            .await
            .unwrap(),
        vec![(h.ws.to_string(), id)]
    );
}

#[intent_test_macros::daemon_test]
async fn monitor_cancel_terminal_write_failure_reconciles_reserved_result() {
    let h = harness().await;
    let owner = monitor_owner(&h, "cancel write failure").await;
    let id = create_simple(
        &h,
        "cancel write failure",
        "read value",
        ScriptMode::Command,
    )
    .await;
    let mgr = h.services.script_manager();
    mgr.start(&h.ws, &id).await.unwrap();
    let watch = mgr
        .monitor(&h.ws, &owner, &id, json!({"ttlMs":60000}))
        .await
        .unwrap();
    let mid = watch["monitor"]["monitorId"].as_str().unwrap();
    sqlx::query("CREATE TRIGGER refuse_cancel_result BEFORE UPDATE OF state ON script_monitor BEGIN SELECT RAISE(FAIL,'injected'); END").execute(h.services.store.write_pool()).await.unwrap();
    assert!(mgr.cancel_monitor(&h.ws, mid, None, true).await.is_err());
    sqlx::query("DROP TRIGGER refuse_cancel_result")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    mgr.reconcile_monitor(&h.ws, mid).await.unwrap();
    let row = h.services.store.script_monitor(&h.ws, mid).await.unwrap();
    assert_eq!(row.state, "completed");
    assert_eq!(
        row.result.unwrap().outcome,
        intent_core::ScriptRunOutcome::Cancelled
    );
    assert_eq!(
        mgr.cancel_monitor(&h.ws, mid, None, true).await.unwrap()["runStopped"],
        false
    );
}
