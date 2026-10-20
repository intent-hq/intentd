/// On-disk script output is deleted when it expires, when its script is
/// removed, when its workspace is deleted, and when its workspace no longer
/// exists at startup.
fn backdate_log(services: &Services, ws: &str, id: &str, run: &str, age: Duration) {
    let path = services
        .script_output_store()
        .run_path(ws, id, run)
        .expect("run path");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    f.set_modified(std::time::SystemTime::now() - age)
        .expect("backdate log");
}

fn log_exists(services: &Services, ws: &str, id: &str, run: &str) -> bool {
    services
        .script_output_store()
        .run_path(ws, id, run)
        .expect("run path")
        .is_file()
}

fn current_run(services: &Services, ws: &WorkspaceId, id: &str) -> String {
    services
        .script_manager()
        .scripts
        .lock()
        .unwrap()
        .get(&(ws.clone(), id.to_string()))
        .and_then(|m| m.state.run_id.clone())
        .expect("script has a current run")
}

async fn wait_for_log(services: &Services, ws: &WorkspaceId, id: &str) -> String {
    tokio::time::timeout(LIVENESS, async {
        loop {
            let run = services
                .script_manager()
                .scripts
                .lock()
                .unwrap()
                .get(&(ws.clone(), id.to_string()))
                .and_then(|m| m.state.run_id.clone());
            if let Some(run) = run {
                if log_exists(services, ws.as_str(), id, &run) {
                    return run;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the run's log appears on disk")
}

fn write_log(services: &Services, ws: &str, id: &str, run: &str) {
    services
        .script_output_store()
        .open_writer(ws, id, run)
        .append(b"line\n");
    assert!(log_exists(services, ws, id, run));
}

async fn output_harness() -> (WorktreeDir, Harness) {
    let data = WorktreeDir::new();
    let mut h = harness().await;
    h.services = h.services.clone().with_script_output_dir(&data.0);
    (data, h)
}

#[intent_test_macros::daemon_test]
async fn expiry_sweep_deletes_expired_logs_and_keeps_recent_and_live() {
    let (_data, h) = output_harness().await;
    let ws = h.ws.as_str().to_string();
    let old = Duration::from_secs(10 * 3600);
    let retention = Duration::from_secs(3600);

    // An exited command run with an expired log is deleted.
    let done = create_simple(&h, "done", "echo done-out", ScriptMode::Command).await;
    h.services
        .script_run(
            h.ws.clone(),
            done.clone(),
            None,
            Some(LIVENESS.as_secs().cast_signed()),
        )
        .await
        .expect("run");
    let done_run = current_run(&h.services, &h.ws, &done);
    backdate_log(&h.services, &ws, &done, &done_run, old);

    // A recent log of an exited run is kept.
    let recent = create_simple(&h, "recent", "echo recent-out", ScriptMode::Command).await;
    h.services
        .script_run(
            h.ws.clone(),
            recent.clone(),
            None,
            Some(LIVENESS.as_secs().cast_signed()),
        )
        .await
        .expect("run");
    let recent_run = current_run(&h.services, &h.ws, &recent);

    // A running service whose current log is old (an auto-restart appends to
    // the same run's log) is kept; an older run of the same script is not.
    let svc = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), svc.clone())
        .await
        .expect("start");
    let svc_run = wait_for_log(&h.services, &h.ws, &svc).await;
    backdate_log(&h.services, &ws, &svc, &svc_run, old);
    write_log(&h.services, &ws, &svc, "previous-run");
    backdate_log(&h.services, &ws, &svc, "previous-run", old);

    let report = h.services.sweep_script_output_older_than(retention).await;
    assert_eq!(report.deleted, 2, "{report:?}");
    assert_eq!(report.skipped_live, 1, "{report:?}");
    assert!(
        !log_exists(&h.services, &ws, &done, &done_run),
        "expired log deleted"
    );
    assert!(
        !log_exists(&h.services, &ws, &svc, "previous-run"),
        "old run of a live script deleted"
    );
    assert!(
        log_exists(&h.services, &ws, &recent, &recent_run),
        "recent log kept"
    );
    assert!(
        log_exists(&h.services, &ws, &svc, &svc_run),
        "live run's log kept"
    );

    // Once stopped, the service's expired log goes on the next sweep.
    h.services
        .script_stop(h.ws.clone(), svc.clone())
        .await
        .expect("stop");
    backdate_log(&h.services, &ws, &svc, &svc_run, old);
    let report = h.services.sweep_script_output_older_than(retention).await;
    assert_eq!(report.deleted, 1, "{report:?}");
    assert!(!log_exists(&h.services, &ws, &svc, &svc_run));

    // Zero retention never deletes anything.
    backdate_log(&h.services, &ws, &recent, &recent_run, old);
    let report = h
        .services
        .sweep_script_output_older_than(Duration::ZERO)
        .await;
    assert_eq!(report.deleted, 0, "{report:?}");
    assert!(log_exists(&h.services, &ws, &recent, &recent_run));
}

#[intent_test_macros::daemon_test]
async fn expiry_sweep_reads_retention_hours_live_from_settings() {
    let (data, mut h) = output_harness().await;
    let config_path = data.0.join("config.toml");
    std::fs::write(&config_path, "[scripts]\noutputRetentionHours = 0\n").expect("config");
    let registry = Arc::new(crate::SettingsRegistry::load(&config_path).expect("registry"));
    h.services = h.services.clone().with_settings_registry(registry.clone());
    let ws = h.ws.as_str().to_string();
    write_log(&h.services, &ws, "s", "r");
    backdate_log(&h.services, &ws, "s", "r", Duration::from_secs(48 * 3600));

    let report = h.services.sweep_expired_script_output().await;
    assert_eq!(report.deleted, 0, "0 disables the sweep: {report:?}");
    assert!(log_exists(&h.services, &ws, "s", "r"));

    registry
        .apply(&[("scripts.outputRetentionHours".to_string(), json!(24))])
        .expect("scripts.outputRetentionHours is a known setting");
    let report = h.services.sweep_expired_script_output().await;
    assert_eq!(report.deleted, 1, "{report:?}");
    assert!(!log_exists(&h.services, &ws, "s", "r"));
}

#[intent_test_macros::daemon_test]
async fn script_remove_deletes_its_output_and_archive_keeps_it() {
    let (_data, h) = output_harness().await;
    let ws = h.ws.as_str().to_string();
    let removed = create_simple(&h, "gone", "echo gone-out", ScriptMode::Command).await;
    let archived = create_simple(&h, "kept", "echo kept-out", ScriptMode::Command).await;
    for id in [&removed, &archived] {
        h.services
            .script_run(
                h.ws.clone(),
                id.clone(),
                None,
                Some(LIVENESS.as_secs().cast_signed()),
            )
            .await
            .expect("run");
    }
    let archived_run = current_run(&h.services, &h.ws, &archived);
    h.services
        .script_archive(h.ws.clone(), vec![archived.clone()])
        .await
        .expect("archive");
    h.services
        .script_remove(h.ws.clone(), removed.clone())
        .await
        .expect("remove");
    let removed_dir = h
        .services
        .script_output_store()
        .script_dir(&ws, &removed)
        .expect("dir");
    assert!(
        !removed_dir.exists(),
        "removed script's output folder is deleted"
    );
    assert!(
        log_exists(&h.services, &ws, &archived, &archived_run),
        "archived script keeps its output"
    );
}

#[intent_test_macros::daemon_test]
async fn workspace_delete_removes_its_script_output_after_scripts_stop() {
    let (data, mut h) = output_harness().await;
    h.services = h
        .services
        .clone()
        .with_workspaces_root(data.0.join("workspaces"));
    let ws = h.ws.as_str().to_string();
    let cmd = create_simple(&h, "cmd", "echo cmd-out", ScriptMode::Command).await;
    h.services
        .script_run(
            h.ws.clone(),
            cmd.clone(),
            None,
            Some(LIVENESS.as_secs().cast_signed()),
        )
        .await
        .expect("run");
    let svc = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), svc.clone())
        .await
        .expect("start");
    wait_for_log(&h.services, &h.ws, &svc).await;
    let other = WorkspaceId::new();
    write_log(&h.services, other.as_str(), "s", "r");

    h.services
        .delete_workspace(h.ws.clone())
        .await
        .expect("delete workspace");

    let mgr = h.services.script_manager();
    assert!(
        mgr.scripts
            .lock()
            .unwrap()
            .get(&(h.ws.clone(), svc.clone()))
            .is_none_or(|m| m.state.status != ScriptStatus::Running),
        "the workspace's service is stopped"
    );
    let ws_dir = h
        .services
        .script_output_store()
        .workspace_dir(&ws)
        .expect("dir");
    assert!(!ws_dir.exists(), "workspace output folder is deleted");
    assert!(
        log_exists(&h.services, other.as_str(), "s", "r"),
        "other workspaces keep output"
    );
}

#[intent_test_macros::daemon_test]
async fn startup_sweep_deletes_output_of_workspaces_that_no_longer_exist() {
    let (_data, h) = output_harness().await;
    let ws = h.ws.as_str().to_string();
    let orphan = WorkspaceId::new();
    let chief = WorkspaceId::chief();
    write_log(&h.services, &ws, "s", "r");
    write_log(&h.services, orphan.as_str(), "s", "r");
    write_log(&h.services, chief.as_str(), "s", "r");

    let removed = h.services.sweep_orphaned_script_output().await;
    assert_eq!(removed, 1);
    let orphan_dir = h
        .services
        .script_output_store()
        .workspace_dir(orphan.as_str())
        .expect("dir");
    assert!(!orphan_dir.exists(), "orphaned workspace folder deleted");
    assert!(
        log_exists(&h.services, &ws, "s", "r"),
        "existing workspace kept"
    );
    assert!(
        log_exists(&h.services, chief.as_str(), "s", "r"),
        "chief kept"
    );
}
