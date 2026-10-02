async fn one_off(h: &Harness, command: &str) -> String {
    create(
        h,
        ScriptCreateParams {
            name: "one-off".into(),
            command: command.into(),
            mode: ScriptMode::Command,
            purpose: Some(intent_core::ScriptPurpose::OneOff),
            ..Default::default()
        },
    )
    .await
}

async fn retired(h: &Harness, id: &str) -> intent_core::Script {
    tokio::time::timeout(LIVENESS, async {
        loop {
            let def = h
                .services
                .store
                .get_script_in_workspace(&h.ws, id)
                .await
                .unwrap()
                .unwrap();
            if def.archived_at.is_some() {
                return def;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("command retires")
}

#[intent_test_macros::daemon_test]
async fn retirement_start_output_events_and_rerun_preserve_previous_result() {
    let h = harness().await;
    let id = one_off(&h, "printf retained").await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let first = retired(&h, &id).await;
    assert_eq!(
        first.last_run.as_ref().unwrap().outcome,
        intent_core::ScriptRunOutcome::Succeeded
    );
    assert_eq!(
        h.services
            .script_status(h.ws.clone(), id.clone())
            .await
            .unwrap()["status"],
        "exited"
    );
    assert!(h
        .services
        .script_output(h.ws.clone(), id.clone(), None, None, None)
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .contains("retained"));
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.supervise = Some(park.clone());
    let old_generation = mgr
        .scripts
        .lock()
        .unwrap()
        .get(&(h.ws.clone(), id.clone()))
        .unwrap()
        .generation;
    mgr.start(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    {
        let lock = mgr.locks.definition_lock(&id);
        let _guard = lock.lock().await;
        mgr.finish_run_locked(&h.ws, &id, old_generation).await;
    }
    let live = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(live.archived_at.is_none());
    assert_eq!(live.last_run, first.last_run);
    assert_eq!(
        mgr.archive(&h.ws, vec![id.clone()], true).await.unwrap()["skipped"][0]["reason"],
        "live"
    );
    park.release.notify_one();
    let next = retired(&h, &id).await;
    assert!(next.last_run.unwrap().started_at.is_some());
}

#[intent_test_macros::daemon_test]
async fn retirement_startup_failure_stop_and_timeout_have_explicit_outcomes() {
    let h = harness_with_worktree(true).await;
    for use_run in [true, false] {
        let id = create(
            &h,
            ScriptCreateParams {
                name: "bad cwd".into(),
                command: "true".into(),
                cwd: Some("../../escape".into()),
                mode: ScriptMode::Command,
                purpose: Some(intent_core::ScriptPurpose::OneOff),
                ..Default::default()
            },
        )
        .await;
        if use_run {
            assert!(h
                .services
                .script_run(h.ws.clone(), id.clone(), None, None)
                .await
                .is_err());
        } else {
            h.services
                .script_start(h.ws.clone(), id.clone())
                .await
                .unwrap();
        }
        let last = retired(&h, &id).await.last_run.unwrap();
        assert_eq!(last.outcome, intent_core::ScriptRunOutcome::Failed);
        assert!(last.started_at.is_none());
        assert_eq!(last.exit_code, Some(-1));
        assert!(last.error.is_some());
    }
    let never = one_off(&h, "true").await;
    h.services
        .script_stop(h.ws.clone(), never.clone())
        .await
        .unwrap();
    let def = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &never)
        .await
        .unwrap()
        .unwrap();
    assert!(def.last_run.is_none() && def.archived_at.is_none());
    let timed = one_off(&h, "cat").await;
    let result = h
        .services
        .script_run(h.ws.clone(), timed.clone(), None, Some(1))
        .await
        .unwrap();
    assert_eq!(result["timedOut"], true);
    let def = retired(&h, &timed).await;
    let last = def.last_run.as_ref().unwrap();
    assert_eq!(last.outcome, intent_core::ScriptRunOutcome::Cancelled);
    assert!(last.error.as_ref().unwrap().contains("timed out"));
    h.services
        .script_stop(h.ws.clone(), timed.clone())
        .await
        .unwrap();
    let stopped = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &timed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.last_run, def.last_run);
    assert_eq!(stopped.archived_at, def.archived_at);
}

#[intent_test_macros::daemon_test]
async fn retirement_stop_pending_start_and_restart_gap_do_not_deadlock() {
    let h = harness().await;
    let id = one_off(&h, "cat").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.supervise = Some(park.clone());
    mgr.start(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let stopping = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.stop(&ws, &id).await })
    };
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .stopped_by_user
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, stopping)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let cancelled = retired(&h, &id).await.last_run.unwrap();
    assert_eq!(cancelled.outcome, intent_core::ScriptRunOutcome::Cancelled);
    assert!(cancelled.started_at.is_none() && cancelled.exit_code.is_none());
    assert_eq!(mgr.status(&h.ws, &id).unwrap()["status"], "idle");
    mgr.restart(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let row = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.archived_at.is_none());
    assert_eq!(row.last_run, Some(cancelled));
    let pending = h.services.store.pending_script_runs().await.unwrap();
    assert!(pending.iter().any(|(_, sid, _, _)| sid == &id));
    let stopping = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.stop(&ws, &id).await })
    };
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, stopping)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    retired(&h, &id).await;
}

#[intent_test_macros::daemon_test]
async fn retirement_pending_recovery_is_idempotent_and_overrides_previous_success() {
    for purpose in [
        intent_core::ScriptPurpose::OneOff,
        intent_core::ScriptPurpose::Saved,
    ] {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "recovery".into(),
                command: "true".into(),
                mode: ScriptMode::Command,
                purpose: Some(purpose),
                ..Default::default()
            },
        )
        .await;
        h.services
            .script_run(h.ws.clone(), id.clone(), None, Some(5))
            .await
            .unwrap();
        h.services
            .store
            .set_script_archived_at(&h.ws, &id, None)
            .await
            .unwrap();
        h.services
            .store
            .admit_script_run(&h.ws, &id, "pending-restart")
            .await
            .unwrap();
        let svc = Services::new(Store::open(&h.tmp.path).await.unwrap());
        svc.hydrate_scripts().await.unwrap();
        let recovered = svc
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.last_run.as_ref().unwrap().outcome,
            intent_core::ScriptRunOutcome::Interrupted
        );
        assert!(recovered.last_run.as_ref().unwrap().started_at.is_none());
        assert_eq!(
            recovered.archived_at.is_some(),
            purpose == intent_core::ScriptPurpose::OneOff
        );
        assert_eq!(
            svc.script_status(h.ws.clone(), id.clone()).await.unwrap()["exitCode"],
            -1
        );
        assert_eq!(svc.hydrate_scripts().await.unwrap(), 0);
        let again = Services::new(Store::open(&h.tmp.path).await.unwrap());
        again.hydrate_scripts().await.unwrap();
        let next = again
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.last_run, recovered.last_run);
        assert_eq!(next.archived_at, recovered.archived_at);
        again.script_stop(h.ws.clone(), id.clone()).await.unwrap();
        assert_eq!(
            again
                .store
                .get_script_in_workspace(&h.ws, &id)
                .await
                .unwrap()
                .unwrap()
                .last_run,
            recovered.last_run
        );
    }
}

#[intent_test_macros::daemon_test]
async fn retirement_durable_token_fences_rerun_replacement_removal_and_scope() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let store = &h.services.store;
    let result = intent_core::ScriptLastRun {
        outcome: intent_core::ScriptRunOutcome::Succeeded,
        exit_code: Some(0),
        started_at: None,
        stopped_at: now_iso(),
        error: None,
    };
    store.admit_script_run(&h.ws, &id, "old").await.unwrap();
    store.admit_script_run(&h.ws, &id, "new").await.unwrap();
    assert!(!store
        .settle_script_run(&h.ws, &id, "old", &result, false)
        .await
        .unwrap());
    assert!(!store
        .settle_script_run(&WorkspaceId::from("foreign"), &id, "new", &result, false)
        .await
        .unwrap());
    assert!(store
        .settle_script_run(&h.ws, &id, "new", &result, false)
        .await
        .unwrap());
    assert!(!store
        .settle_script_run(&h.ws, &id, "new", &result, false)
        .await
        .unwrap());
    let mut replacement = store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    replacement.last_run = None;
    replacement.archived_at = None;
    store
        .admit_script_run(&h.ws, &id, "replaced")
        .await
        .unwrap();
    store.upsert_script(&replacement).await.unwrap();
    assert!(!store
        .settle_script_run(&h.ws, &id, "replaced", &result, false)
        .await
        .unwrap());
    store.admit_script_run(&h.ws, &id, "removed").await.unwrap();
    store.remove_script(&id).await.unwrap();
    store.upsert_script(&replacement).await.unwrap();
    assert!(!store
        .settle_script_run(&h.ws, &id, "removed", &result, false)
        .await
        .unwrap());
    assert!(store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap()
        .last_run
        .is_none());
}

#[intent_test_macros::daemon_test]
async fn retirement_database_failure_keeps_real_result_and_output_active() {
    let h = harness().await;
    let id = one_off(&h, "printf retained").await;
    sqlx::query("CREATE TRIGGER refuse_result BEFORE UPDATE OF last_run ON script BEGIN SELECT RAISE(FAIL,'result refused'); END")
        .execute(h.services.store.write_pool()).await.unwrap();
    let result = h
        .services
        .script_run(h.ws.clone(), id.clone(), None, Some(5))
        .await
        .unwrap();
    assert_eq!(result["exitCode"], 0);
    assert!(result["output"].as_str().unwrap().contains("retained"));
    let def = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(def.archived_at.is_none() && def.last_run.is_none());
    assert_eq!(
        h.services
            .script_status(h.ws.clone(), id.clone())
            .await
            .unwrap()["exitCode"],
        0
    );
    let recovered = Services::new(Store::open(&h.tmp.path).await.unwrap());
    assert_eq!(recovered.hydrate_scripts().await.unwrap(), 1);
    let row = recovered
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.archived_at.is_none() && row.last_run.is_none());
    assert_eq!(
        recovered.script_status(h.ws.clone(), id).await.unwrap()["error"],
        LOST_AT_DAEMON_STOP_ERROR
    );
}

#[intent_test_macros::daemon_test]
async fn retirement_dropped_run_waiter_finishes_and_shutdown_recovers_pending_start() {
    let h = harness().await;
    let id = one_off(&h, "cat").await;
    let mut sub = subscribe(&h);
    let run = {
        let svc = h.services.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { svc.script_run(ws, id, None, None).await })
    };
    await_state(&mut sub, LIVENESS, |v| {
        v["data"]["scriptId"] == id && v["data"]["status"] == "running"
    })
    .await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    h.services
        .script_stop(h.ws.clone(), id.clone())
        .await
        .unwrap();
    assert_eq!(
        retired(&h, &id).await.last_run.unwrap().outcome,
        intent_core::ScriptRunOutcome::Cancelled
    );
    let pending = one_off(&h, "cat").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.supervise = Some(park.clone());
    mgr.start(&h.ws, &pending).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let shutdown = {
        let mgr = mgr.clone();
        intent_core::spawn_daemon(async move { mgr.stop_all().await })
    };
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), pending.clone()))
                .unwrap()
                .running_at_shutdown
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, shutdown)
        .await
        .unwrap()
        .unwrap();
    let svc = Services::new(Store::open(&h.tmp.path).await.unwrap());
    svc.hydrate_scripts().await.unwrap();
    let def = svc
        .store
        .get_script_in_workspace(&h.ws, &pending)
        .await
        .unwrap()
        .unwrap();
    assert!(def.archived_at.is_some());
    assert_eq!(
        def.last_run.unwrap().outcome,
        intent_core::ScriptRunOutcome::Interrupted
    );
}

#[intent_test_macros::daemon_test]
async fn retirement_unobservable_exit_and_stale_finalizer_are_fenced() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let mgr = h.services.script_manager();
    let lock = mgr.locks.definition_lock(&id);
    let guard = lock.lock().await;
    mgr.prepare_launch(&h.ws, &id).await.unwrap();
    let generation = mgr
        .scripts
        .lock()
        .unwrap()
        .get(&(h.ws.clone(), id.clone()))
        .unwrap()
        .generation;
    mgr.mark_exited(&h.ws, &id, generation, None, false)
        .await
        .unwrap();
    mgr.record_result(&h.ws, &id, generation, false, false);
    mgr.finish_run_locked(&h.ws, &id, generation).await;
    let def = retired(&h, &id).await;
    assert_eq!(
        def.last_run.as_ref().unwrap().outcome,
        intent_core::ScriptRunOutcome::Interrupted
    );
    assert_eq!(
        def.last_run.unwrap().error.as_deref(),
        Some(EXIT_UNOBSERVABLE_ERROR)
    );
    drop(guard);
    h.services
        .script_create(
            h.ws.clone(),
            ScriptCreateParams {
                script_id: Some(id.clone()),
                name: "replacement".into(),
                command: "cat".into(),
                mode: ScriptMode::Command,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _guard = lock.lock().await;
    mgr.finish_run_locked(&h.ws, &id, generation).await;
    let replaced = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(replaced.last_run.is_none() && replaced.archived_at.is_none());
}

#[intent_test_macros::daemon_test]
async fn retirement_cancelled_waiter_cannot_split_committed_result_and_registry() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.settlement_committed = Some(park.clone());
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, Some(5)).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap()
        .archived_at
        .is_some());
    park.release.notify_one();
    let lock = mgr.locks.definition_lock(&id);
    let _guard = tokio::time::timeout(LIVENESS, lock.lock()).await.unwrap();
    let def = h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    let scripts = mgr.scripts.lock().unwrap();
    let entry = scripts.get(&(h.ws.clone(), id)).unwrap();
    assert_eq!(entry.def.last_run, def.last_run);
    assert_eq!(entry.def.archived_at, def.archived_at);
}

#[intent_test_macros::daemon_test]
async fn retirement_shutdown_in_restart_teardown_recovers_successor_admission() {
    let h = harness().await;
    let id = one_off(&h, "cat").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.supervise = Some(park.clone());
    mgr.start(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let old = h.services.store.pending_script_runs().await.unwrap()[0]
        .2
        .clone();
    let restart = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.restart(&ws, &id).await })
    };
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .stopped_by_user
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pending = h.services.store.pending_script_runs().await.unwrap();
    assert_ne!(pending[0].2, old, "restart admission precedes teardown");
    mgr.stop_all().await;
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, restart)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let lock = mgr.locks.definition_lock(&id);
    let guard = lock.lock().await;
    let handle = mgr
        .scripts
        .lock()
        .unwrap()
        .get_mut(&(h.ws.clone(), id.clone()))
        .unwrap()
        .supervisor
        .take();
    if let Some(handle) = handle {
        tokio::time::timeout(LIVENESS, handle)
            .await
            .unwrap()
            .unwrap();
    }
    drop(guard);
    let svc = Services::new(Store::open(&h.tmp.path).await.unwrap());
    svc.hydrate_scripts().await.unwrap();
    let def = svc
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        def.last_run.unwrap().outcome,
        intent_core::ScriptRunOutcome::Interrupted
    );
    assert!(def.archived_at.is_some());
}

#[intent_test_macros::daemon_test]
async fn retirement_shutdown_drains_known_result_before_returning() {
    let h = harness().await;
    let id = one_off(&h, "printf final").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.settlement_ready = Some(park.clone());
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, Some(5)).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let shutdown = {
        let mgr = mgr.clone();
        intent_core::spawn_daemon(async move { mgr.stop_all().await })
    };
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .stopped_by_user
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !shutdown.is_finished(),
        "shutdown drains pending known results"
    );
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, shutdown)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.await.unwrap().unwrap()["exitCode"], 0);
    let settled = retired(&h, &id).await;
    assert_eq!(
        settled.last_run.as_ref().unwrap().outcome,
        intent_core::ScriptRunOutcome::Succeeded
    );
    let fresh = Services::new(Store::open(&h.tmp.path).await.unwrap());
    fresh.hydrate_scripts().await.unwrap();
    assert_eq!(
        fresh
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap()
            .last_run,
        settled.last_run
    );
}

#[intent_test_macros::daemon_test]
async fn retirement_recovery_fences_a_pending_finalizer() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.settlement_ready = Some(park.clone());
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, Some(5)).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    // A new daemon has independent admission locks. Its CAS consumes the same
    // durable token; a delayed old finalizer cannot overwrite the recovery.
    let fresh = Services::new(Store::open(&h.tmp.path).await.unwrap());
    fresh.hydrate_scripts().await.unwrap();
    let recovered = fresh
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered.last_run.as_ref().unwrap().outcome,
        intent_core::ScriptRunOutcome::Interrupted
    );
    park.release.notify_one();
    // The waiter also queues an owned finalizer; release its test-only park.
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    park.release.notify_one();
    run.await.unwrap().unwrap();
    let after = fresh
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.last_run, recovered.last_run);
    assert_eq!(after.archived_at, recovered.archived_at);
}

#[intent_test_macros::daemon_test]
async fn retirement_shutdown_preserves_terminal_result_before_marker_persistence() {
    use intent_core::ScriptRunOutcome::{Cancelled, Failed, Succeeded};
    for (command, use_run, bad_cwd, expected) in [
        ("printf observed", false, false, Succeeded),
        ("exit 7", false, false, Failed),
        ("true", false, true, Failed),
        ("printf observed", true, false, Succeeded),
        ("sleep 30", true, false, Cancelled),
    ] {
        let h = harness_with_worktree(true).await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "early terminal".into(),
                command: command.into(),
                cwd: bad_cwd.then(|| "../../escape".into()),
                mode: ScriptMode::Command,
                purpose: Some(intent_core::ScriptPurpose::OneOff),
                ..Default::default()
            },
        )
        .await;
        let mut sub = subscribe(&h);
        let park = Arc::new(SupervisePark::default());
        let mut mgr = h.services.script_manager();
        mgr.parks.terminal_persist = Some(park.clone());
        let run = if use_run {
            let mgr = mgr.clone();
            let ws = h.ws.clone();
            let id = id.clone();
            Some(intent_core::spawn_daemon(async move {
                mgr.run(&ws, &id, None, Some(1)).await
            }))
        } else {
            mgr.start(&h.ws, &id).await.unwrap();
            None
        };
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .unwrap();
        let shutdown = {
            let mgr = mgr.clone();
            intent_core::spawn_daemon(async move { mgr.stop_all().await })
        };
        tokio::time::timeout(LIVENESS, async {
            loop {
                if mgr
                    .scripts
                    .lock()
                    .unwrap()
                    .get(&(h.ws.clone(), id.clone()))
                    .unwrap()
                    .stopped_by_user
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let captured = {
            let entries = mgr.scripts.lock().unwrap();
            let m = entries.get(&(h.ws.clone(), id.clone())).unwrap();
            (m.pending_result.clone(), m.running_at_shutdown)
        };
        park.release.notify_one();
        tokio::time::timeout(LIVENESS, shutdown)
            .await
            .unwrap()
            .unwrap();
        if let Some(run) = run {
            run.await.unwrap().unwrap();
        }
        assert!(
            !captured.1,
            "already observed terminal outcome must survive shutdown: {command}"
        );
        assert_eq!(captured.0.unwrap().outcome, expected);
        let before = h
            .services
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            before.archived_at.is_some(),
            "shutdown drains terminal owner before returning"
        );
        assert_eq!(before.last_run.as_ref().unwrap().outcome, expected);
        tokio::time::timeout(LIVENESS, async {
            let mut terminal_seen = false;
            loop {
                for event in sub.recv().await.unwrap() {
                    let event = serde_json::to_value(event).unwrap();
                    if event["type"] == "script:state" && event["data"]["status"] == "exited" {
                        terminal_seen = true;
                    }
                    if event["type"] == "script:changed" {
                        assert!(
                            terminal_seen,
                            "terminal state precedes automatic archive during shutdown"
                        );
                        return;
                    }
                }
            }
        })
        .await
        .unwrap();
        let fresh = Services::new(Store::open(&h.tmp.path).await.unwrap());
        fresh.hydrate_scripts().await.unwrap();
        let after = fresh
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.last_run, before.last_run);
        assert_eq!(after.archived_at, before.archived_at);
    }
}

#[intent_test_macros::daemon_test]
async fn retirement_shutdown_does_not_attach_predecessor_result_to_restart_admission() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.terminal_persist = Some(park.clone());
    mgr.start(&h.ws, &id).await.unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let lock = mgr.locks.definition_lock(&id);
    let guard = lock.lock().await;
    // Commit successor admission while the predecessor still owns terminal
    // publication. This is the durable restart-before-teardown boundary.
    mgr.prepare_restart(&h.ws, &id).await.unwrap();
    let token = h.services.store.pending_script_runs().await.unwrap()[0]
        .2
        .clone();
    let shutdown = {
        let mgr = mgr.clone();
        intent_core::spawn_daemon(async move { mgr.stop_all().await })
    };
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .stopped_by_user
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, shutdown)
        .await
        .unwrap()
        .unwrap();
    drop(guard);
    assert_eq!(
        h.services.store.pending_script_runs().await.unwrap()[0].2,
        token
    );
    let fresh = Services::new(Store::open(&h.tmp.path).await.unwrap());
    fresh.hydrate_scripts().await.unwrap();
    let def = fresh
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        def.last_run.unwrap().outcome,
        intent_core::ScriptRunOutcome::Interrupted
    );
    assert!(def.archived_at.is_some());
}

async fn cancelled_observed_failure_retires(bad_cwd: bool) {
    let h = harness_with_worktree(true).await;
    let id = create(
        &h,
        ScriptCreateParams {
            name: "cancelled observed failure".into(),
            command: "true".into(),
            cwd: bad_cwd.then(|| "../../escape".into()),
            mode: ScriptMode::Command,
            purpose: Some(intent_core::ScriptPurpose::OneOff),
            ..Default::default()
        },
    )
    .await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.terminal_persist = Some(park.clone());
    if !bad_cwd {
        mgr.pty.kill_all().await;
    }
    let mut sub = subscribe(&h);
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, None).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let observed = mgr
        .scripts
        .lock()
        .unwrap()
        .get(&(h.ws.clone(), id.clone()))
        .unwrap()
        .pending_result
        .clone()
        .expect("failure already observed before cancellation");
    assert_eq!(observed.outcome, intent_core::ScriptRunOutcome::Failed);
    assert_eq!(observed.exit_code, Some(-1));
    assert!(observed.started_at.is_none());
    assert!(observed.error.is_some());
    assert_eq!(
        h.services.store.pending_script_runs().await.unwrap().len(),
        1
    );
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    park.release.notify_one();
    // No later stop, shutdown or rerun may be needed to flush this failure.
    let settled = retired(&h, &id).await;
    assert_eq!(settled.last_run, Some(observed));
    assert!(h
        .services
        .store
        .pending_script_runs()
        .await
        .unwrap()
        .is_empty());
    tokio::time::timeout(LIVENESS, async {
        let mut terminal_seen = false;
        loop {
            for event in sub.recv().await.unwrap() {
                let event = serde_json::to_value(event).unwrap();
                if event["type"] == "script:state" && event["data"]["status"] == "exited" {
                    assert_eq!(
                        event["data"]["error"],
                        settled
                            .last_run
                            .as_ref()
                            .unwrap()
                            .error
                            .as_ref()
                            .unwrap()
                            .as_str()
                    );
                    terminal_seen = true;
                }
                if event["type"] == "script:changed" {
                    assert!(
                        terminal_seen,
                        "failure publication must precede automatic archive"
                    );
                    return;
                }
            }
        }
    })
    .await
    .unwrap();
}

#[intent_test_macros::daemon_test]
async fn retirement_cancelled_run_waiter_after_cwd_failure_still_retires() {
    cancelled_observed_failure_retires(true).await;
}

#[intent_test_macros::daemon_test]
async fn retirement_cancelled_run_waiter_after_spawn_failure_still_retires() {
    cancelled_observed_failure_retires(false).await;
}

#[intent_test_macros::daemon_test]
async fn retirement_shutdown_joins_finalizer_past_supervisor_grace() {
    let h = harness().await;
    let id = one_off(&h, "printf final").await;
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.settlement_ready = Some(park.clone());
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, None).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified()).await.unwrap();
    // Exercise the existing grace path with final durable settlement held.
    tokio::time::timeout(LIVENESS, mgr.stop_all()).await.unwrap();
    let drain = h.services.shutdown_store_writers();
    tokio::pin!(drain);
    let escaped = tokio::select! {
        biased;
        () = &mut drain => true,
        () = std::future::ready(()) => false,
    };
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, run).await.unwrap().unwrap().unwrap();
    if !escaped { tokio::time::timeout(LIVENESS, drain).await.unwrap(); }
    let settled = retired(&h, &id).await;
    assert_eq!(settled.last_run.unwrap().outcome, intent_core::ScriptRunOutcome::Succeeded);
    assert!(!escaped, "store barrier returned while a known script result was still unpersisted");
}
