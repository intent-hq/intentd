#[intent_test_macros::daemon_test]
async fn script_snapshots_cover_committed_results_and_restore() {
    for (command, purpose, expected) in [
        (
            "printf retained",
            intent_core::ScriptPurpose::OneOff,
            "succeeded",
        ),
        ("exit 7", intent_core::ScriptPurpose::OneOff, "failed"),
        ("cat", intent_core::ScriptPurpose::OneOff, "cancelled"),
        ("true", intent_core::ScriptPurpose::Saved, "succeeded"),
        ("exit 7", intent_core::ScriptPurpose::Saved, "failed"),
        ("cat", intent_core::ScriptPurpose::Saved, "cancelled"),
    ] {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "snapshot".into(),
                command: command.into(),
                mode: ScriptMode::Command,
                purpose: Some(purpose),
                ..Default::default()
            },
        )
        .await;
        let mgr = h.services.script_manager();
        let mut sub = subscribe(&h);
        if expected == "cancelled" {
            mgr.start(&h.ws, &id).await.unwrap();
            await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
            mgr.stop(&h.ws, &id).await.unwrap();
        } else {
            mgr.run(&h.ws, &id, None, Some(5)).await.unwrap();
        }
        let event = await_script_change(&mut sub, "updated").await;
        let row = &event["data"]["script"];
        assert_eq!(row["lastRun"]["outcome"], expected);
        assert_eq!(*row, mgr.list(&h.ws).await.unwrap()["scripts"][0]);
        assert_eq!(
            row["archivedAt"].is_string(),
            purpose == intent_core::ScriptPurpose::OneOff
        );
        let durable = h
            .services
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row["lastRun"],
            serde_json::to_value(durable.last_run).unwrap()
        );
        if purpose == intent_core::ScriptPurpose::OneOff {
            mgr.archive(&h.ws, vec![id.clone()], false).await.unwrap();
            let restored = await_script_change(&mut sub, "updated").await;
            assert!(restored["data"]["script"].get("archivedAt").is_none());
            assert_eq!(restored["data"]["script"]["lastRun"], row["lastRun"]);
            assert_eq!(
                restored["data"]["script"],
                mgr.list(&h.ws).await.unwrap()["scripts"][0]
            );
        }
    }
}

#[intent_test_macros::daemon_test]
async fn script_snapshot_recovery_installs_matching_runtime_before_publication() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    h.services
        .store
        .admit_script_run(&h.ws, &id, "crashed")
        .await
        .unwrap();
    let store = Store::open(&h.tmp.path).await.unwrap();
    let bus = EventBus::new(store.clone());
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![SCRIPT_CHANGED.to_string()],
        workspace_id: Some(h.ws.to_string()),
        ..Default::default()
    });
    let fresh = Services::new(store).with_event_bus(bus);
    assert_eq!(fresh.hydrate_scripts().await.unwrap(), 1);
    let event = await_script_change(&mut sub, "updated").await;
    let row = &event["data"]["script"];
    assert_eq!(row["lastRun"]["outcome"], "interrupted");
    assert_eq!(row["runtime"]["status"], "exited");
    assert_eq!(
        *row,
        fresh.script_manager().list(&h.ws).await.unwrap()["scripts"][0]
    );
    assert_eq!(fresh.hydrate_scripts().await.unwrap(), 0);
}

#[intent_test_macros::daemon_test]
async fn script_snapshot_replacement_fences_captured_terminal_publication() {
    for remove in [false, true] {
        for failure in [false, true] {
            let h = harness().await;
            let id = one_off(&h, "true").await;
            let mut mgr = h.services.script_manager();
            let park = Arc::new(SupervisePark::default());
            mgr.parks.terminal_persist = Some(park.clone());
            let generation = mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .generation;
            let terminal = {
                let mgr = mgr.clone();
                let ws = h.ws.clone();
                let id = id.clone();
                intent_core::spawn_daemon(async move {
                    if failure {
                        mgr.fail(&ws, &id, generation, "old failure", false).await;
                    } else {
                        mgr.mark_exited(&ws, &id, generation, None, false).await;
                    }
                })
            };
            tokio::time::timeout(LIVENESS, park.entered.notified())
                .await
                .unwrap();
            let mut sub = subscribe(&h);
            if remove {
                mgr.remove(&h.ws, &id).await.unwrap();
            } else {
                create(
                    &h,
                    ScriptCreateParams {
                        script_id: Some(id.clone()),
                        name: "successor".into(),
                        command: "cat".into(),
                        mode: ScriptMode::Command,
                        ..Default::default()
                    },
                )
                .await;
            }
            let change =
                await_script_change(&mut sub, if remove { "removed" } else { "updated" }).await;
            park.release.notify_one();
            tokio::time::timeout(LIVENESS, terminal)
                .await
                .unwrap()
                .unwrap();
            let events = h
                .services
                .store
                .query_events(&EventQuery {
                    workspace_id: Some(h.ws.clone()),
                    event_types: vec![SCRIPT_STATE.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(
                events.is_empty(),
                "captured predecessor published after successor: {change:?} {events:?}"
            );
        }
    }
}

#[intent_test_macros::daemon_test]
async fn script_snapshot_detached_completion_cannot_clear_successor_marker() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let mut mgr = h.services.script_manager();
    let park = Arc::new(SupervisePark::default());
    mgr.parks.terminal_persist = Some(park.clone());
    let run = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.run(&ws, &id, None, Some(5)).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    create(
        &h,
        ScriptCreateParams {
            script_id: Some(id.clone()),
            name: "successor service".into(),
            command: "cat".into(),
            mode: ScriptMode::Service,
            purpose: Some(intent_core::ScriptPurpose::Saved),
            ..Default::default()
        },
    )
    .await;
    let mut sub = subscribe(&h);
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        h.services
            .store
            .list_was_running_script_ids()
            .await
            .unwrap(),
        vec![(h.ws.to_string(), id.clone())]
    );
    let events = h
        .services
        .store
        .query_events(&EventQuery {
            workspace_id: Some(h.ws.clone()),
            event_types: vec![SCRIPT_STATE.to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        !events.iter().any(|e| e.data["status"] == "exited"),
        "old completion escaped generation fence"
    );
    assert_eq!(
        h.services.script_manager().status(&h.ws, &id).unwrap()["status"],
        "running"
    );
    h.services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn script_snapshot_old_url_and_state_cannot_mutate_replacement_or_rerun() {
    let h = harness().await;
    let id = one_off(&h, "true").await;
    let mgr = h.services.script_manager();
    let old = mgr
        .scripts
        .lock()
        .unwrap()
        .get(&(h.ws.clone(), id.clone()))
        .unwrap()
        .generation;
    create(
        &h,
        ScriptCreateParams {
            script_id: Some(id.clone()),
            name: "replacement".into(),
            command: "cat".into(),
            mode: ScriptMode::Command,
            ..Default::default()
        },
    )
    .await;
    let mut sub = subscribe(&h);
    mgr.start(&h.ws, &id).await.unwrap();
    await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
    assert!(
        mgr.try_detect_url(&h.ws, &id, old, b"http://localhost:3333")
            .await
    );
    mgr.emit_state_for(&h.ws, &id, old).await;
    let row = mgr.list(&h.ws).await.unwrap()["scripts"][0].clone();
    assert!(row["runtime"].get("detectedUrl").is_none());
    mgr.stop(&h.ws, &id).await.unwrap();
    let old = mgr
        .scripts
        .lock()
        .unwrap()
        .get(&(h.ws.clone(), id.clone()))
        .unwrap()
        .generation;
    mgr.start(&h.ws, &id).await.unwrap();
    await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
    assert!(mgr
        .mark_exited(&h.ws, &id, old, None, false)
        .await
        .is_none());
    mgr.fail(&h.ws, &id, old, "old failure", false).await;
    mgr.try_detect_url(&h.ws, &id, old, b"http://localhost:4444")
        .await;
    let runtime = mgr.status(&h.ws, &id).unwrap();
    assert_eq!(runtime["status"], "running");
    assert!(runtime.get("detectedUrl").is_none() && runtime.get("error").is_none());
    mgr.stop(&h.ws, &id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn script_snapshot_finished_stop_publishes_idle_without_changing_history() {
    for purpose in [
        intent_core::ScriptPurpose::OneOff,
        intent_core::ScriptPurpose::Saved,
    ] {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "finished".into(),
                command: "true".into(),
                mode: ScriptMode::Command,
                purpose: Some(purpose),
                ..Default::default()
            },
        )
        .await;
        let mgr = h.services.script_manager();
        mgr.run(&h.ws, &id, None, Some(5)).await.unwrap();
        let before = mgr.list(&h.ws).await.unwrap()["scripts"][0].clone();
        mgr.stop(&h.ws, &id).await.unwrap();
        let events = h
            .services
            .store
            .query_events(&EventQuery {
                workspace_id: Some(h.ws.clone()),
                event_types: vec![SCRIPT_STATE.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            events.iter().any(|e| e.data["status"] == "idle"),
            "finished stop must publish its runtime change"
        );
        let after = mgr.list(&h.ws).await.unwrap()["scripts"][0].clone();
        assert_eq!(after["runtime"]["status"], "idle");
        assert_eq!(after["archivedAt"], before["archivedAt"]);
        assert_eq!(after["lastRun"], before["lastRun"]);
        mgr.stop(&h.ws, &id).await.unwrap();
        let after_noop = h
            .services
            .store
            .query_events(&EventQuery {
                workspace_id: Some(h.ws.clone()),
                event_types: vec![SCRIPT_STATE.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            after_noop.len(),
            events.len(),
            "an already-idle stop is a true no-op"
        );
        assert_eq!(mgr.list(&h.ws).await.unwrap()["scripts"][0], after);
    }
}
