/// Script output goes to `<data_dir>/script-output/...` and `script.output`
/// reads it back from disk, so exited PTY sessions can be released.
async fn run_to_exit(services: &Services, ws: &WorkspaceId, id: &str) -> Value {
    services
        .script_run(
            ws.clone(),
            id.to_string(),
            None,
            Some(LIVENESS.as_secs().cast_signed()),
        )
        .await
        .expect("run")
}

fn latest_pty(services: &Services, ws: &WorkspaceId, id: &str) -> PtyId {
    services
        .script_manager()
        .scripts
        .lock()
        .unwrap()
        .get(&(ws.clone(), id.to_string()))
        .and_then(|m| m.pty_id)
        .expect("run recorded its pty")
}

async fn output_text(services: &Services, ws: &WorkspaceId, id: &str, n: Option<i64>) -> String {
    services
        .script_output(ws.clone(), id.to_string(), n, None, None)
        .await
        .expect("output")
        .as_str()
        .expect("bare string")
        .to_string()
}

#[intent_test_macros::daemon_test]
async fn script_output_survives_pty_release() {
    let data = WorktreeDir::new();
    let mut h = harness().await;
    h.services = h.services.clone().with_script_output_dir(&data.0);
    let id = create_simple(
        &h,
        "echo",
        "echo disk-alpha; echo disk-beta",
        ScriptMode::Command,
    )
    .await;
    let out = run_to_exit(&h.services, &h.ws, &id).await;
    assert!(
        out["output"].as_str().unwrap_or("").contains("disk-beta"),
        "script.run result still carries the output: {out:?}"
    );
    let pty = latest_pty(&h.services, &h.ws, &id);
    assert!(
        h.services.pty().scrollback(pty).is_err(),
        "the exited script PTY session is released"
    );
    let text = output_text(&h.services, &h.ws, &id, None).await;
    assert!(text.starts_with('['), "line-count header: {text:?}");
    assert!(
        text.contains("disk-alpha") && text.contains("disk-beta"),
        "{text:?}"
    );
    let page = h
        .services
        .script_output(h.ws.clone(), id.clone(), Some(50), Some(true), None)
        .await
        .expect("page");
    assert!(
        page["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|line| line.as_str().is_some_and(|l| l.contains("disk-beta"))),
        "pagination reads the disk log: {page:?}"
    );

    // Archive keeps the log readable.
    h.services
        .script_archive(h.ws.clone(), vec![id.clone()])
        .await
        .expect("archive");
    let text = output_text(&h.services, &h.ws, &id, None).await;
    assert!(text.contains("disk-beta"), "after archive: {text:?}");
}

#[intent_test_macros::daemon_test]
async fn service_pty_is_released_after_its_supervisor_settles() {
    let data = WorktreeDir::new();
    let mut h = harness().await;
    h.services = h.services.clone().with_script_output_dir(&data.0);
    let id = create_simple(&h, "svc", "echo svc-out; sleep 60", ScriptMode::Service).await;
    h.services
        .script_start(h.ws.clone(), id.clone())
        .await
        .expect("start");
    let mgr = h.services.script_manager();
    tokio::time::timeout(LIVENESS, async {
        loop {
            if output_text(&h.services, &h.ws, &id, None)
                .await
                .contains("svc-out")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("live output reaches the disk log while running");
    let pty = latest_pty(&h.services, &h.ws, &id);
    h.services
        .script_stop(h.ws.clone(), id.clone())
        .await
        .expect("stop");
    tokio::time::timeout(LIVENESS, async {
        while h.services.pty().scrollback(pty).is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("stopped service PTY session is released");
    assert!(!mgr.is_live(&h.ws, &id).unwrap());
    let text = output_text(&h.services, &h.ws, &id, None).await;
    assert!(text.contains("svc-out"), "{text:?}");
}

#[intent_test_macros::daemon_test]
async fn script_output_is_read_from_disk_after_restart() {
    let data = WorktreeDir::new();
    let mut h = harness().await;
    h.services = h.services.clone().with_script_output_dir(&data.0);
    let id = create_simple(
        &h,
        "echo",
        "echo restart-one; echo restart-two",
        ScriptMode::Command,
    )
    .await;
    run_to_exit(&h.services, &h.ws, &id).await;

    // Simulated daemon restart: a fresh Services over the same store and data dir.
    let svc2 = crate::test_runtime::with_runtime(Services::new(h.services.store().clone())).with_script_output_dir(&data.0);
    svc2.hydrate_scripts().await.expect("hydrate");
    let text = output_text(&svc2, &h.ws, &id, None).await;
    assert!(
        text.contains("restart-one") && text.contains("restart-two"),
        "{text:?}"
    );
    // Lazily loaded (known-id) access works too, without hydration.
    let svc3 = crate::test_runtime::with_runtime(Services::new(h.services.store().clone())).with_script_output_dir(&data.0);
    let text = output_text(&svc3, &h.ws, &id, Some(2)).await;
    assert!(text.contains("restart-two"), "{text:?}");
}

#[intent_test_macros::daemon_test]
async fn script_output_keeps_more_than_the_pty_scrollback() {
    let data = WorktreeDir::new();
    let mut h = harness().await;
    h.services = h.services.clone().with_script_output_dir(&data.0);
    let id = create_simple(&h, "seq", "seq 1 60000", ScriptMode::Command).await;
    run_to_exit(&h.services, &h.ws, &id).await;
    let text = output_text(&h.services, &h.ws, &id, Some(2)).await;
    // `seq` prints ~350 KB, far past the script PTY scrollback; the line count
    // covers every line (plus the trailing empty split segment).
    assert!(
        text.starts_with("[showing last 2 of 60001 lines]"),
        "{text:?}"
    );
    let page = h
        .services
        .script_output(h.ws.clone(), id.clone(), Some(1), Some(true), None)
        .await
        .expect("page");
    let mut token = page["nextToken"].as_str().map(str::to_string);
    let mut oldest = None;
    let mut pages = 0;
    while let Some(t) = token.take() {
        let next = h
            .services
            .script_output(h.ws.clone(), id.clone(), Some(200), Some(true), Some(t))
            .await
            .expect("page");
        oldest = next["items"]
            .as_array()
            .and_then(|items| items.last())
            .and_then(Value::as_str)
            .map(str::to_string);
        token = next["nextToken"].as_str().map(str::to_string);
        pages += 1;
        assert!(pages < 400, "pagination terminates");
    }
    assert_eq!(oldest.as_deref().map(str::trim), Some("1"), "oldest line is on disk");
}

#[intent_test_macros::daemon_test]
async fn script_output_cap_is_read_live_at_run_start() {
    let data = WorktreeDir::new();
    let config = data.0.join("config.toml");
    std::fs::write(&config, "").unwrap();
    let registry = Arc::new(intent_services::feature_api::SettingsRegistry::load(&config).expect("settings"));
    let mut h = harness().await;
    h.services = h
        .services
        .clone()
        .with_script_output_dir(&data.0)
        .with_settings_registry(registry.clone());
    // Lower the cap after the services exist: the run must read it live.
    registry
        .reload("[scripts]\noutputMaxBytesPerRun = 65536\n")
        .expect("lower the cap");
    let id = create_simple(&h, "seq", "seq 1 60000", ScriptMode::Command).await;
    run_to_exit(&h.services, &h.ws, &id).await;
    let run = h.services.script_status(h.ws.clone(), id.clone()).await.unwrap()["runId"]
        .as_str()
        .expect("run id")
        .to_string();
    let path = h
        .services
        .script_output_store()
        .run_path(h.ws.as_str(), &id, &run)
        .unwrap();
    let len = std::fs::metadata(&path).expect("log exists").len();
    assert!(len <= 65_536, "log capped at the live setting: {len}");
    let text = output_text(&h.services, &h.ws, &id, Some(2)).await;
    assert!(text.contains("60000"), "newest bytes kept: {text:?}");
}
