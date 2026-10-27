use crate::script_output_cleanup::ScriptOutputCleanupExt;
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

/// Wait until a run's log holds `line` terminated by a newline. A script that
/// prints that one line and then sleeps or exits writes nothing more, so a
/// log backdated after this keeps its mtime.
async fn wait_for_log_line(services: &Services, ws: &str, id: &str, run: &str, line: &str) {
    let path = services
        .script_output_store()
        .run_path(ws, id, run)
        .expect("run path");
    let (lf, crlf) = (format!("{line}\n"), format!("{line}\r\n"));
    tokio::time::timeout(LIVENESS, async {
        loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if text.contains(&lf) || text.contains(&crlf) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} gets the line {line:?}", path.display()));
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
    wait_for_log_line(&h.services, &ws, &done, &done_run, "done-out").await;
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
    // The log exists from the run's start; backdating before the service's
    // one line lands would let that write refresh the mtime.
    wait_for_log_line(&h.services, &ws, &svc, &svc_run, "svc-out").await;
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

/// Every log file below one workspace's output folder.
fn workspace_logs(services: &Services, ws: &str) -> Vec<PathBuf> {
    let dir = services
        .script_output_store()
        .workspace_dir(ws)
        .expect("dir");
    let mut out = Vec::new();
    for script in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        for run in std::fs::read_dir(script.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            out.push(run.path());
        }
    }
    out.sort();
    out
}

#[intent_test_macros::daemon_test]
async fn workspace_delete_refuses_launches_after_scripts_stop_and_recreates_no_log() {
    let (data, mut h) = output_harness().await;
    let park = Arc::new(SupervisePark::default());
    h.services = h
        .services
        .clone()
        .with_workspaces_root(data.0.join("workspaces"))
        .with_script_workspace_closed_park(park.clone());
    let ws = h.ws.as_str().to_string();
    let idle = create_simple(&h, "idle", "echo idle-out; sleep 60", ScriptMode::Service).await;
    let svc = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    let cmd = create_simple(&h, "cmd", "echo cmd-out", ScriptMode::Command).await;
    h.services
        .script_start(h.ws.clone(), svc.clone())
        .await
        .expect("start");
    wait_for_log(&h.services, &h.ws, &svc).await;

    let deleting = {
        let services = h.services.clone();
        let ws = h.ws.clone();
        intent_core::spawn_daemon(async move { services.delete_workspace(ws).await })
    };
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .expect("deletion parks after its scripts stop");
    let mgr = h.services.script_manager();
    assert!(mgr.locks.is_closing(&h.ws));
    assert!(
        !mgr.is_live(&h.ws, &svc).unwrap(),
        "running service stopped"
    );
    let before = workspace_logs(&h.services, &ws);

    let start = h.services.script_start(h.ws.clone(), idle.clone()).await;
    assert!(start.is_err(), "idle start is refused: {start:?}");
    let restart = h.services.script_restart(h.ws.clone(), svc.clone()).await;
    assert!(restart.is_err(), "restart is refused: {restart:?}");
    let run = h
        .services
        .script_run(
            h.ws.clone(),
            cmd.clone(),
            None,
            Some(LIVENESS.as_secs().cast_signed()),
        )
        .await;
    assert!(run.is_err(), "run is refused: {run:?}");
    for id in [&idle, &svc, &cmd] {
        assert!(!mgr.is_live(&h.ws, id).unwrap(), "{id} did not launch");
    }
    assert_eq!(
        workspace_logs(&h.services, &ws),
        before,
        "no new log opened"
    );

    park.release.notify_one();
    tokio::time::timeout(LIVENESS, deleting)
        .await
        .expect("deletion finishes")
        .expect("join")
        .expect("delete workspace");
    let ws_dir = h
        .services
        .script_output_store()
        .workspace_dir(&ws)
        .expect("dir");
    assert!(!ws_dir.exists(), "workspace output folder is deleted");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !ws_dir.exists(),
        "no log is recreated after the folder delete"
    );
    assert!(
        !mgr.locks.is_closing(&h.ws),
        "the mark is cleared once the delete returns, so a recreated workspace can launch"
    );
}

fn no_live_pty(services: &Services, ws: &WorkspaceId) -> bool {
    let pty = services.pty();
    services
        .script_manager()
        .scripts
        .lock()
        .unwrap()
        .iter()
        .filter(|((w, _), _)| w == ws)
        .all(|(_, m)| m.pty_id.is_none_or(|id| !pty.is_alive(id)))
}

#[intent_test_macros::daemon_test]
async fn workspace_delete_drains_launches_admitted_before_it_and_a_cancelled_caller() {
    let (data, mut h) = output_harness().await;
    let closed = Arc::new(SupervisePark::default());
    h.services = h
        .services
        .clone()
        .with_workspaces_root(data.0.join("workspaces"))
        .with_script_workspace_closed_park(closed.clone());
    let ws = h.ws.as_str().to_string();
    let svc = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    let cmd = create_simple(&h, "cmd", "echo cmd-out; sleep 60", ScriptMode::Command).await;

    // A service launch admitted before deletion, parked in its detached
    // supervisor before the PTY spawns.
    let svc_park = Arc::new(SupervisePark::default());
    let mut svc_mgr = h.services.script_manager();
    svc_mgr.parks.before_spawn = Some(svc_park.clone());
    svc_mgr.start(&h.ws, &svc).await.expect("start admitted");
    tokio::time::timeout(LIVENESS, svc_park.entered.notified())
        .await
        .expect("service launch parks before spawn");

    // A command run admitted before deletion whose caller is then cancelled.
    let run_park = Arc::new(SupervisePark::default());
    let mut run_mgr = h.services.script_manager();
    run_mgr.parks.before_spawn = Some(run_park.clone());
    let caller = {
        let mgr = run_mgr.clone();
        let ws = h.ws.clone();
        let id = cmd.clone();
        intent_core::spawn_daemon(async move {
            mgr.run(&ws, &id, None, Some(LIVENESS.as_secs().cast_signed()))
                .await
        })
    };
    tokio::time::timeout(LIVENESS, run_park.entered.notified())
        .await
        .expect("run parks before spawn");
    caller.abort();
    let _ = caller.await;

    let deleting = {
        let services = h.services.clone();
        let ws = h.ws.clone();
        intent_core::spawn_daemon(async move { services.delete_workspace(ws).await })
    };
    svc_park.release.notify_one();
    run_park.release.notify_one();
    tokio::time::timeout(LIVENESS, closed.entered.notified())
        .await
        .expect("deletion parks after its scripts stop");
    let mgr = h.services.script_manager();
    for id in [&svc, &cmd] {
        assert!(
            !mgr.is_live(&h.ws, id).unwrap(),
            "{id} admitted launch was drained"
        );
    }
    assert!(no_live_pty(&h.services, &h.ws), "no PTY survives the stop");

    closed.release.notify_one();
    tokio::time::timeout(LIVENESS, deleting)
        .await
        .expect("deletion finishes")
        .expect("join")
        .expect("delete workspace");
    let ws_dir = h
        .services
        .script_output_store()
        .workspace_dir(&ws)
        .expect("dir");
    assert!(!ws_dir.exists(), "workspace output folder is deleted");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!ws_dir.exists(), "no drained launch recreates a log");
    assert!(no_live_pty(&h.services, &h.ws));
}

#[intent_test_macros::daemon_test]
async fn cancelled_workspace_delete_reopens_scripts_and_retry_deletes_output() {
    let (data, mut h) = output_harness().await;
    let closed = Arc::new(SupervisePark::default());
    h.services = h
        .services
        .clone()
        .with_workspaces_root(data.0.join("workspaces"))
        .with_script_workspace_closed_park(closed.clone());
    let ws = h.ws.as_str().to_string();
    let svc = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), svc.clone())
        .await
        .expect("start");
    wait_for_log(&h.services, &h.ws, &svc).await;

    let deleting = {
        let services = h.services.clone();
        let ws = h.ws.clone();
        intent_core::spawn_daemon(async move { services.delete_workspace(ws).await })
    };
    tokio::time::timeout(LIVENESS, closed.entered.notified())
        .await
        .expect("deletion parks during script teardown");
    deleting.abort();
    assert!(deleting.await.is_err_and(|e| e.is_cancelled()));

    let mgr = h.services.script_manager();
    assert!(
        !mgr.locks.is_closing(&h.ws),
        "a cancelled delete reopens the workspace"
    );
    assert!(
        h.services.store().get_workspace(&h.ws).await.is_ok(),
        "workspace survives"
    );
    assert!(
        !mgr.is_live(&h.ws, &svc).unwrap(),
        "its service stays stopped"
    );
    assert!(no_live_pty(&h.services, &h.ws), "no orphan PTY");

    // Retry: the park lets the next deletion through.
    closed.release.notify_one();
    tokio::time::timeout(LIVENESS, h.services.delete_workspace(h.ws.clone()))
        .await
        .expect("retry does not deadlock")
        .expect("retry deletes the workspace");
    let ws_dir = h
        .services
        .script_output_store()
        .workspace_dir(&ws)
        .expect("dir");
    assert!(!ws_dir.exists(), "output folder is gone after the retry");
    assert!(no_live_pty(&h.services, &h.ws));
}

#[intent_test_macros::daemon_test]
async fn orphan_sweep_keeps_every_folder_when_the_store_cannot_be_read() {
    let (_data, h) = output_harness().await;
    let ws = h.ws.as_str().to_string();
    let orphan = WorkspaceId::new();
    write_log(&h.services, &ws, "s", "r");
    write_log(&h.services, orphan.as_str(), "s", "r");
    h.services.store().close().await;
    assert!(
        h.services
            .store()
            .get_workspace(&h.ws)
            .await
            .is_err_and(|e| !matches!(e, intent_core::Error::NotFound(_))),
        "the closed store fails reads with a non-NotFound error"
    );

    assert_eq!(h.services.sweep_orphaned_script_output().await, 0);
    assert!(
        log_exists(&h.services, &ws, "s", "r"),
        "existing workspace kept"
    );
    assert!(
        log_exists(&h.services, orphan.as_str(), "s", "r"),
        "an unverifiable folder is kept for the next startup"
    );
}

#[intent_test_macros::daemon_test]
async fn expiry_sweep_keeps_the_current_run_through_a_service_auto_restart() {
    let (_data, mut h) = output_harness().await;
    let park = Arc::new(SupervisePark::default());
    h.services = h
        .services
        .clone()
        .with_script_too_fast_ms(0)
        .with_script_before_spawn_park(park.clone());
    let ws = h.ws.as_str().to_string();
    let svc = create_simple(
        &h,
        "flappy",
        "echo attempt-out; sleep 0.2; exit 1",
        ScriptMode::Service,
    )
    .await;
    h.services
        .script_start(h.ws.clone(), svc.clone())
        .await
        .expect("start");
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .expect("first attempt spawns");
    park.release.notify_one();
    let run = wait_for_log(&h.services, &h.ws, &svc).await;

    // The first attempt exits; the supervisor backs off and respawns under
    // the same run, parking before the new attempt spawns or writes.
    tokio::time::timeout(LIVENESS, park.entered.notified())
        .await
        .expect("the service auto-restarts");
    let mgr = h.services.script_manager();
    assert_eq!(
        current_run(&h.services, &h.ws, &svc),
        run,
        "a restart keeps its run"
    );
    assert!(
        mgr.run_is_live(&ws, &svc, &run),
        "the restarting run is live"
    );
    let old = Duration::from_secs(10 * 3600);
    backdate_log(&h.services, &ws, &svc, &run, old);
    write_log(&h.services, &ws, &svc, "older-run");
    backdate_log(&h.services, &ws, &svc, "older-run", old);

    let report = h
        .services
        .sweep_script_output_older_than(Duration::from_secs(3600))
        .await;
    assert_eq!((report.deleted, report.skipped_live), (1, 1), "{report:?}");
    assert!(
        log_exists(&h.services, &ws, &svc, &run),
        "current run's log kept"
    );
    assert!(
        !log_exists(&h.services, &ws, &svc, "older-run"),
        "older run deleted"
    );

    // The respawn appends to the kept log: both attempts' output remains.
    park.release.notify_one();
    let path = h
        .services
        .script_output_store()
        .run_path(&ws, &svc, &run)
        .expect("path");
    tokio::time::timeout(LIVENESS, async {
        loop {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            if text.matches("attempt-out").count() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the second attempt appends to the same log");
    park.release.notify_one();
    h.services
        .script_stop(h.ws.clone(), svc.clone())
        .await
        .expect("stop");
}
