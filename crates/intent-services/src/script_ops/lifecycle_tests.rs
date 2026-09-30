#[intent_test_macros::daemon_test]
async fn archive_round_trip_preserves_output_scope_and_hydrates() {
    use intent_core::ScriptArchiveFilter;
    let h = harness().await;
    let id = create_simple(&h, "legacy", "printf retained", ScriptMode::Command).await;
    h.services
        .script_run(h.ws.clone(), id.clone(), None, Some(5))
        .await
        .unwrap();
    let before = h
        .services
        .script_status(h.ws.clone(), id.clone())
        .await
        .unwrap();
    let output = h
        .services
        .script_output(h.ws.clone(), id.clone(), None, None, None)
        .await
        .unwrap();
    let service = create_simple(&h, "service", "true", ScriptMode::Service).await;
    let result = h
        .services
        .script_archive(
            h.ws.clone(),
            vec![id.clone(), "foreign".into(), id.clone(), service.clone()],
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        json!({"archived":[id], "skipped":[{"scriptId":"foreign","reason":"notFound"},{"scriptId":service,"reason":"service"}]})
    );
    assert_eq!(
        h.services
            .script_status(h.ws.clone(), id.clone())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        h.services
            .script_output(h.ws.clone(), id.clone(), None, None, None)
            .await
            .unwrap(),
        output
    );
    let history = h
        .services
        .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Archived)
        .await
        .unwrap();
    assert_eq!(history["scripts"].as_array().unwrap().len(), 1);
    assert_eq!(history["scripts"][0]["purpose"], "saved");
    assert_eq!(history["scripts"][0]["lastRun"]["outcome"], "succeeded");
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    assert_eq!(
        h.services
            .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Archived)
            .await
            .unwrap(),
        history
    );
    assert_eq!(
        h.services.script_list(h.ws.clone()).await.unwrap()["scripts"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let store = Store::open(&h.tmp.path).await.unwrap();
    let restarted = Services::new(store);
    restarted.script_manager().hydrate().await.unwrap();
    let hydrated = restarted
        .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Archived)
        .await
        .unwrap();
    assert_eq!(
        hydrated["scripts"][0]["archivedAt"],
        history["scripts"][0]["archivedAt"]
    );
    assert_eq!(hydrated["scripts"][0]["runtime"]["status"], "idle");
    assert_eq!(
        restarted
            .script_restore(h.ws.clone(), vec![id.clone(), service.clone()])
            .await
            .unwrap(),
        json!({"restored":[id,service],"skipped":[]})
    );
    assert!(restarted
        .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Archived)
        .await
        .unwrap()["scripts"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[intent_test_macros::daemon_test]
async fn archive_validates_whole_selection_and_purpose_before_mutation() {
    let h = harness().await;
    let id = create(
        &h,
        ScriptCreateParams {
            name: "one".into(),
            command: "true".into(),
            mode: ScriptMode::Command,
            purpose: Some(intent_core::ScriptPurpose::OneOff),
            ..Default::default()
        },
    )
    .await;
    for ids in [
        vec![],
        vec![id.clone(), String::new()],
        vec![id.clone(); 1001],
    ] {
        assert!(matches!(
            h.services.script_archive(h.ws.clone(), ids).await,
            Err(Error::InvalidParams(_))
        ));
    }
    let invalid = ScriptCreateParams {
        name: "invalid".into(),
        command: "true".into(),
        mode: ScriptMode::Service,
        script_id: Some(id.clone()),
        ..Default::default()
    };
    assert!(matches!(
        h.services.script_create(h.ws.clone(), invalid).await,
        Err(Error::InvalidParams(_))
    ));
    let rows = h.services.script_list(h.ws.clone()).await.unwrap();
    assert_eq!(rows["scripts"][0]["purpose"], "oneOff");
    assert!(rows["scripts"][0].get("archivedAt").is_none());
    let updated = ScriptCreateParams {
        name: "saved".into(),
        command: "true".into(),
        mode: ScriptMode::Service,
        purpose: Some(intent_core::ScriptPurpose::Saved),
        script_id: Some(id),
        ..Default::default()
    };
    assert_eq!(
        h.services
            .script_create(h.ws.clone(), updated)
            .await
            .unwrap()["purpose"],
        "saved"
    );
}

#[intent_test_macros::daemon_test]
async fn archive_rejects_pending_launch_and_start_restores_first() {
    let h = harness().await;
    let park = Arc::new(SupervisePark::default());
    let services = h.services.clone().with_script_supervise_park(park.clone());
    let id = create_simple(&h, "pending", "cat", ScriptMode::Command).await;
    services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        services
            .script_archive(h.ws.clone(), vec![id.clone()])
            .await
            .unwrap(),
        json!({"archived":[],"skipped":[{"scriptId":id,"reason":"live"}]})
    );
    let rows = services.script_list(h.ws.clone()).await.unwrap();
    assert!(rows["scripts"][0].get("archivedAt").is_none());
    assert_eq!(
        services.store.list_was_running_script_ids().await.unwrap(),
        vec![(h.ws.to_string(), id.clone())]
    );
    park.release.notify_one();
    services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn archive_write_failure_cannot_hide_or_start_a_process() {
    let h = harness().await;
    let id = create_simple(&h, "durable", "cat", ScriptMode::Command).await;
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER refuse_restore BEFORE UPDATE OF archived_at ON script BEGIN SELECT RAISE(FAIL, 'injected archive write failure'); END")
        .execute(h.services.store.write_pool()).await.unwrap();
    assert!(h
        .services
        .script_start(h.ws.clone(), id.clone())
        .await
        .is_err());
    assert_eq!(
        h.services
            .script_status(h.ws.clone(), id.clone())
            .await
            .unwrap()["status"],
        "idle"
    );
    assert!(
        h.services.script_list(h.ws.clone()).await.unwrap()["scripts"][0]
            .get("archivedAt")
            .is_some()
    );
}

#[intent_test_macros::daemon_test]
async fn archive_commit_and_cancelled_waiter_serialize_with_start() {
    let h = harness().await;
    let id = create_simple(&h, "race", "cat", ScriptMode::Command).await;
    let park = Arc::new(SupervisePark::default());
    let launch = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.archive_persist = Some(park.clone());
    mgr.parks.supervise = Some(launch.clone());
    let archive = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.archive(&ws, vec![id], true).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    assert!(mgr.locks.definition_lock(&id).try_lock().is_err());
    archive.abort(); // the owned archive still finishes its durable transition
    let start = {
        let mgr = mgr.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { mgr.start(&ws, &id).await })
    };
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, start)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(LIVENESS, launch.entered.notified())
        .await
        .unwrap();
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap()
        .archived_at
        .is_none());
    assert_eq!(
        mgr.archive(&h.ws, vec![id.clone()], true).await.unwrap()["skipped"][0]["reason"],
        "live"
    );
    launch.release.notify_one();
    mgr.stop(&h.ws, &id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn archive_batch_failure_commits_only_successful_predecessors() {
    let h = harness().await;
    let a = create_simple(&h, "a", "true", ScriptMode::Command).await;
    let b = create_simple(&h, "b", "true", ScriptMode::Command).await;
    sqlx::query("CREATE TRIGGER refuse_second_archive BEFORE UPDATE OF archived_at ON script WHEN NEW.name='b' BEGIN SELECT RAISE(FAIL,'injected'); END")
        .execute(h.services.store.write_pool()).await.unwrap();
    assert!(matches!(
        h.services
            .script_archive(h.ws.clone(), vec![a.clone(), b.clone()])
            .await,
        Err(Error::Internal(_))
    ));
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &a)
        .await
        .unwrap()
        .unwrap()
        .archived_at
        .is_some());
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &b)
        .await
        .unwrap()
        .unwrap()
        .archived_at
        .is_none());
    sqlx::query("DROP TRIGGER refuse_second_archive")
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        h.services
            .script_archive(h.ws.clone(), vec![a.clone(), b.clone()])
            .await
            .unwrap()["archived"],
        json!([a, b])
    );
}

#[intent_test_macros::daemon_test]
async fn archive_cannot_enter_restart_teardown_gap() {
    let h = harness().await;
    let id = create_simple(&h, "restart", "cat", ScriptMode::Command).await;
    let park = Arc::new(SupervisePark::default());
    let services = h.services.clone().with_script_supervise_park(park.clone());
    services
        .script_start(h.ws.clone(), id.clone())
        .await
        .unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let restart = {
        let svc = services.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { svc.script_restart(ws, id).await })
    };
    let mgr = services.script_manager();
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
        mgr.locks.definition_lock(&id).try_lock().is_err(),
        "restart fences the complete stop/start gap"
    );
    let archive = {
        let svc = services.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { svc.script_archive(ws, vec![id]).await })
    };
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, restart)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    let result = tokio::time::timeout(LIVENESS, archive)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result["skipped"][0]["reason"], "live");
    assert!(
        services.script_list(h.ws.clone()).await.unwrap()["scripts"][0]
            .get("archivedAt")
            .is_none()
    );
    park.release.notify_one();
    services.script_stop(h.ws.clone(), id).await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn archive_cannot_hide_run_reservation_and_run_restores_history() {
    let h = harness().await;
    let id = create_simple(&h, "run", "printf retained", ScriptMode::Command).await;
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    let park = Arc::new(SupervisePark::default());
    let services = h
        .services
        .clone()
        .with_script_mark_running_park(park.clone());
    let run = {
        let svc = services.clone();
        let ws = h.ws.clone();
        let id = id.clone();
        intent_core::spawn_daemon(async move { svc.script_run(ws, id, None, Some(5)).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        services
            .script_archive(h.ws.clone(), vec![id.clone()])
            .await
            .unwrap()["skipped"][0]["reason"],
        "live"
    );
    assert!(
        services.script_list(h.ws.clone()).await.unwrap()["scripts"][0]
            .get("archivedAt")
            .is_none()
    );
    park.release.notify_one();
    assert_eq!(
        tokio::time::timeout(LIVENESS, run)
            .await
            .unwrap()
            .unwrap()
            .unwrap()["exitCode"],
        0
    );
    assert_eq!(
        services
            .script_archive(h.ws.clone(), vec![id.clone()])
            .await
            .unwrap()["archived"],
        json!([id])
    );
}

#[intent_test_macros::daemon_test]
async fn archive_run_cancelled_after_restore_commit_keeps_registry_consistent() {
    let h = harness().await;
    let id = one_off(&h, "cat").await;
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    let park = Arc::new(SupervisePark::default());
    let mut mgr = h.services.script_manager();
    mgr.parks.archive_committed = Some(park.clone());
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
    park.release.notify_one();
    tokio::time::timeout(LIVENESS, async {
        loop {
            if mgr
                .scripts
                .lock()
                .unwrap()
                .get(&(h.ws.clone(), id.clone()))
                .unwrap()
                .run_reserved
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled admission released its reservation");
    let lock = mgr.locks.definition_lock(&id);
    let _settled = tokio::time::timeout(LIVENESS, lock.lock()).await.unwrap();
    assert!(h
        .services
        .store
        .get_script_in_workspace(&h.ws, &id)
        .await
        .unwrap()
        .unwrap()
        .archived_at
        .is_none());
    let scripts = mgr.scripts.lock().unwrap();
    let entry = scripts.get(&(h.ws.clone(), id)).unwrap();
    assert!(
        entry.def.archived_at.is_none(),
        "cancelled restore must match its durable commit"
    );
    assert_eq!(entry.state.status, ScriptStatus::Idle);
    assert!(entry.run_reserved.is_none());
    assert!(
        entry.run_id.is_none(),
        "cancelled pre-spawn waiter cannot leave a run for a later stop to settle"
    );
    assert!(entry.def.last_run.is_none());
    assert_eq!(mgr.pty.count(), 0, "cancelled admission spawned nothing");
}

#[intent_test_macros::daemon_test]
async fn archive_empty_active_view_never_rebootstraps_repository_scripts() {
    use intent_core::ScriptArchiveFilter;
    let h = harness().await;
    let repo = WorktreeDir::new();
    std::fs::create_dir_all(repo.0.join(".intent")).unwrap();
    std::fs::write(
        repo.0.join(".intent/config.json"),
        r#"{"scripts":[{"name":"check","command":"true","mode":"command"}]}"#,
    )
    .unwrap();
    sqlx::query("UPDATE workspace SET repository_path = ? WHERE id = ?")
        .bind(repo.0.to_str().unwrap())
        .bind(h.ws.as_str())
        .execute(h.services.store.write_pool())
        .await
        .unwrap();
    let active = h
        .services
        .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Active)
        .await
        .unwrap();
    let rows = active["scripts"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["purpose"], "saved");
    let id = rows[0]["id"].as_str().unwrap().to_owned();
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            h.services
                .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Active)
                .await
                .unwrap(),
            json!({"scripts":[]})
        );
    }
    let history = h
        .services
        .script_list_filtered(h.ws.clone(), ScriptArchiveFilter::Archived)
        .await
        .unwrap();
    assert_eq!(history["scripts"].as_array().unwrap().len(), 1);
    assert_eq!(history["scripts"][0]["id"], id);
    assert_eq!(h.services.store.list_all_scripts().await.unwrap().len(), 1);
}

#[intent_test_macros::daemon_test]
async fn retirement_run_records_success_failure_and_saved_summary() {
    for (purpose, command, outcome, code) in [
        (
            intent_core::ScriptPurpose::OneOff,
            "printf retained",
            "succeeded",
            0,
        ),
        (intent_core::ScriptPurpose::OneOff, "exit 7", "failed", 7),
        (intent_core::ScriptPurpose::Saved, "true", "succeeded", 0),
    ] {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "retirement".into(),
                command: command.into(),
                mode: ScriptMode::Command,
                purpose: Some(purpose),
                ..Default::default()
            },
        )
        .await;
        let result = h
            .services
            .script_run(h.ws.clone(), id.clone(), None, Some(5))
            .await
            .unwrap();
        assert_eq!(result["exitCode"], code);
        let def = h
            .services
            .store
            .get_script_in_workspace(&h.ws, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&def).unwrap()["lastRun"]["outcome"],
            outcome
        );
        assert_eq!(
            def.archived_at.is_some(),
            purpose == intent_core::ScriptPurpose::OneOff
        );
        assert_eq!(
            h.services
                .script_status(h.ws.clone(), id.clone())
                .await
                .unwrap()["status"],
            "exited"
        );
        if code == 0 && purpose == intent_core::ScriptPurpose::OneOff {
            assert!(h
                .services
                .script_output(h.ws.clone(), id, None, None, None)
                .await
                .unwrap()
                .as_str()
                .unwrap()
                .contains("retained"));
        }
    }
}
