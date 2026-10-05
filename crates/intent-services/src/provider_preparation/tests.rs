use super::*;

#[test]
fn eligibility_uses_registry_pins_and_ignores_legacy_overrides() {
    let mut settings = SettingsFile::default();
    for id in PROVIDERS {
        let provider = find_provider(id).unwrap();
        assert!(eligible(provider, &settings));
        settings
            .providers
            .paths
            .insert(id.into(), "/obsolete/adapter".into());
        assert!(eligible(provider, &settings));
        assert!(provider.npx_only_package.unwrap().contains('@'));
    }
    assert!(!eligible(find_provider("auggie").unwrap(), &settings));
    settings.providers.enabled = Some(BTreeMap::from([("pi".into(), false)]));
    assert!(!eligible(find_provider("pi").unwrap(), &settings));
    settings.providers.enabled = None;
    let mut custom = *find_provider("pi").unwrap();
    custom.npx_only_honors_path_override = true;
    settings
        .providers
        .paths
        .insert("pi".into(), "/bin/sh".into());
    if cfg!(unix) {
        assert!(!eligible(&custom, &settings));
    }
    assert!(
        eligible(find_provider("pi").unwrap(), &settings),
        "ignored legacy override must not suppress preparation"
    );
    let mut gated = *find_provider("pi").unwrap();
    gated.requires_env_var = Some("INTENT_TEST_PREPARATION_MISSING_GATE_1F825");
    assert!(!eligible(&gated, &settings));
}

fn fixture_receipt(dir: &Path) -> PathBuf {
    let root = dir.join("npm/_npx/prepared");
    let package = root.join("node_modules/pi-acp");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("package.json"),
        r#"{"name":"pi-acp","version":"0.0.34","bin":"entry.js"}"#,
    )
    .unwrap();
    std::fs::write(
        package.join("entry.js"),
        "throw Error('adapter must not execute');",
    )
    .unwrap();
    std::fs::write(
        root.join("node_modules/.package-lock.json"),
        r#"{"packages":{"node_modules/pi-acp":{}}}"#,
    )
    .unwrap();
    std::fs::create_dir_all(root.join("node_modules/.bin")).unwrap();
    std::fs::write(root.join("node_modules/.bin/pi-acp"), "fixture-bin").unwrap();
    let receipt = dir.join("fixture-receipt.json");
    std::fs::write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({"root":root,"name":"pi-acp","version":"0.0.34"}))
            .unwrap(),
    )
    .unwrap();
    receipt
}

#[test]
fn success_receipt_invalidates_on_cache_deletion_and_partial_damage() {
    let dir = crate::test_support::test_tempdir("preparation-receipt");
    let path = fixture_receipt(dir.path());
    let receipt = Receipt::capture(&path).unwrap();
    assert!(receipt.usable());
    let entry = dir
        .path()
        .join("npm/_npx/prepared/node_modules/pi-acp/entry.js");
    std::fs::remove_file(entry).unwrap();
    assert!(!receipt.usable());
    assert!(Receipt::capture(&path).is_none());
    fixture_receipt(dir.path());
    let receipt = Receipt::capture(&path).unwrap();
    std::fs::remove_dir_all(dir.path().join("npm")).unwrap();
    assert!(!receipt.usable());
}

#[cfg(unix)]
fn fake_npx(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let binary = dir.join("npx");
    std::fs::write(
        &binary,
        r#"#!/bin/sh
printf '%s\n' "$@" > "$REPORT"
printf '%s' "$$" > "$STARTED"
for last do :; done
if [ -n "$RELEASE" ]; then
  while [ ! -f "$RELEASE" ]; do /bin/sleep 0.01; done
fi
if [ "$FAIL" = 1 ]; then exit 1; fi
/bin/cp "$FIXTURE_RECEIPT" "$(dirname "$last")/receipt.json"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    binary
}

#[cfg(unix)]
fn fake_job(dir: &Path, id: &'static str, release: Option<&Path>, fail: bool) -> Job {
    let root = dir.join(id);
    std::fs::create_dir_all(&root).unwrap();
    let receipt = if root.join("fixture-receipt.json").exists() {
        root.join("fixture-receipt.json")
    } else {
        fixture_receipt(&root)
    };
    let binary = if root.join("npx").exists() {
        root.join("npx")
    } else {
        fake_npx(&root)
    };
    let mut env = BTreeMap::from([
        (
            "REPORT".into(),
            root.join("report").to_string_lossy().into_owned(),
        ),
        (
            "STARTED".into(),
            root.join("started").to_string_lossy().into_owned(),
        ),
        (
            "FIXTURE_RECEIPT".into(),
            receipt.to_string_lossy().into_owned(),
        ),
        ("FAIL".into(), if fail { "1" } else { "0" }.into()),
    ]);
    if let Some(release) = release {
        env.insert("RELEASE".into(), release.to_string_lossy().into_owned());
    }
    build_job(find_provider(id).unwrap(), &binary, None, env).unwrap()
}

#[cfg(unix)]
async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("observable condition");
}

#[cfg(unix)]
#[tokio::test]
async fn preparation_runs_metadata_only_with_launch_isolation() {
    let dir = crate::test_support::test_tempdir("preparation-command");
    for id in PROVIDERS {
        let job = fake_job(dir.path(), id, None, false);
        let cwd = job
            .prepared
            .command
            .as_std()
            .get_current_dir()
            .unwrap()
            .to_owned();
        assert!(cwd.join("package.json").is_file());
        let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
        assert!(
            run(job, Duration::from_secs(5), std::future::pending(), gate)
                .await
                .is_some()
        );
        assert!(!cwd.exists());
        let args = std::fs::read_to_string(dir.path().join(id).join("report")).unwrap();
        let expected = format!(
            "--workspaces=false\n--yes\n--ignore-scripts\n--package={}\n--\nnode\n",
            find_provider(id).unwrap().npx_only_package.unwrap()
        );
        assert!(args.starts_with(&expected), "{args}");
        assert!(args.trim_end().ends_with("prepare.cjs"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn queue_is_bounded_deduplicates_and_runs_two_providers_concurrently() {
    let dir = crate::test_support::test_tempdir("preparation-concurrency");
    let root = dir.path().to_owned();
    let release = root.join("release");
    let selector: Arc<Selector> =
        Arc::new(move |id, _| Some(fake_job(&root, id, Some(&release), false)));
    let queue = Preparation::default();
    for _ in 0..50 {
        queue.enqueue_with(
            PROVIDERS.map(str::to_owned).to_vec(),
            SettingsFile::default(),
            &selector,
        );
    }
    wait_for(|| {
        PROVIDERS
            .iter()
            .filter(|id| dir.path().join(id).join("started").exists())
            .count()
            == 2
    })
    .await;
    assert_eq!(queue.state.lock().unwrap().pending.len(), 3);
    assert_eq!(queue.slots.available_permits(), 0);
    std::fs::write(dir.path().join("release"), "go").unwrap();
    wait_for(|| queue.state.lock().unwrap().pending.is_empty()).await;
    assert_eq!(queue.state.lock().unwrap().outcomes.len(), 3);
    assert!(queue
        .state
        .lock()
        .unwrap()
        .outcomes
        .values()
        .all(|result| result.receipt.is_some()));
    queue.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn failure_backoff_and_changed_context_allow_retry() {
    let dir = crate::test_support::test_tempdir("preparation-backoff");
    let root = dir.path().to_owned();
    let selector: Arc<Selector> = Arc::new(move |id, _| Some(fake_job(&root, id, None, true)));
    let queue = Preparation::default();
    let enqueue = || queue.enqueue_with(vec!["pi".into()], SettingsFile::default(), &selector);
    enqueue();
    wait_for(|| queue.state.lock().unwrap().outcomes.contains_key("pi")).await;
    let first = queue.state.lock().unwrap().outcomes["pi"].completed;
    enqueue();
    wait_for(|| queue.state.lock().unwrap().pending.is_empty()).await;
    assert_eq!(queue.state.lock().unwrap().outcomes["pi"].completed, first);
    queue
        .state
        .lock()
        .unwrap()
        .outcomes
        .get_mut("pi")
        .unwrap()
        .context = [0; 32];
    enqueue();
    wait_for(|| queue.state.lock().unwrap().outcomes["pi"].completed > first).await;
    let second = queue.state.lock().unwrap().outcomes["pi"].completed;
    queue
        .state
        .lock()
        .unwrap()
        .outcomes
        .get_mut("pi")
        .unwrap()
        .completed = Instant::now().checked_sub(BACKOFF).unwrap();
    enqueue();
    wait_for(|| queue.state.lock().unwrap().outcomes["pi"].completed > second).await;
    queue.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn timeout_and_shutdown_reap_children_and_release_admission() {
    let dir = crate::test_support::test_tempdir("preparation-timeout");
    let job = fake_job(dir.path(), "pi", Some(&dir.path().join("never")), false);
    let cwd = job
        .prepared
        .command
        .as_std()
        .get_current_dir()
        .unwrap()
        .to_owned();
    let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
    assert!(
        run(job, Duration::from_secs(1), std::future::pending(), gate)
            .await
            .is_none()
    );
    assert!(!cwd.exists());
    let pid: i32 = std::fs::read_to_string(dir.path().join("pi/started"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err());
    let queue = Preparation::default();
    let root = dir.path().to_owned();
    let selector: Arc<Selector> =
        Arc::new(move |id, _| Some(fake_job(&root, id, Some(&root.join("never")), false)));
    std::fs::remove_file(dir.path().join("pi/started")).unwrap();
    queue.enqueue_with(vec!["pi".into()], SettingsFile::default(), &selector);
    wait_for(|| dir.path().join("pi/started").exists()).await;
    queue.shutdown().await;
    assert!(queue.state.lock().unwrap().pending.is_empty());
    let pid: i32 = std::fs::read_to_string(dir.path().join("pi/started"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn real_launch_cancels_preparation_before_taking_cache_lock() {
    let dir = crate::test_support::test_tempdir("preparation-launch-race");
    let job = fake_job(dir.path(), "codex", Some(&dir.path().join("never")), false);
    let coordination = COORDINATION["codex"].clone();
    let gate = coordination.gate.clone().lock_owned().await;
    let worker = tokio::spawn(async move {
        run(job, DOWNLOAD_TIMEOUT, coordination.cancel.notified(), gate).await
    });
    wait_for(|| dir.path().join("codex/started").exists()).await;
    let launch = tokio::time::timeout(Duration::from_secs(5), before_launch("codex"))
        .await
        .unwrap()
        .unwrap();
    assert!(worker.await.unwrap().is_none());
    let pid: i32 = std::fs::read_to_string(dir.path().join("codex/started"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err());
    drop(launch);
}

/// Opt-in network proof. Downloads the actual registry pins into a fresh npm
/// cache, then resolves the real positional launch invocation offline. A
/// daemon-owned script-shell intercepts execution, so no adapter or CLI runs.
#[cfg(unix)]
#[tokio::test]
#[ignore = "downloads pinned npm packages; run explicitly with an isolated cache"]
async fn real_npm_pins_reuse_prepared_cache_offline_without_adapter_execution() {
    use std::os::unix::fs::PermissionsExt;
    let dir = crate::test_support::test_tempdir("preparation-real-npm");
    let npx = intent_providers::find_npx().expect("npx");
    let user_config = dir.path().join("user.npmrc");
    let global_config = dir.path().join("global.npmrc");
    std::fs::write(&user_config, "").unwrap();
    std::fs::write(&global_config, "").unwrap();
    let env = BTreeMap::from([
        (
            "npm_config_cache".into(),
            dir.path().join("cache").to_string_lossy().into_owned(),
        ),
        (
            "npm_config_userconfig".into(),
            user_config.to_string_lossy().into_owned(),
        ),
        (
            "npm_config_globalconfig".into(),
            global_config.to_string_lossy().into_owned(),
        ),
        (
            "npm_config_registry".into(),
            "https://registry.npmjs.org/".into(),
        ),
        ("npm_config_update_notifier".into(), "false".into()),
        ("npm_config_audit".into(), "false".into()),
        ("npm_config_fund".into(), "false".into()),
    ]);
    for id in PROVIDERS {
        let provider = find_provider(id).unwrap();
        let job = build_job(provider, &npx, None, env.clone()).unwrap();
        let gate = Arc::new(tokio::sync::Mutex::new(())).lock_owned().await;
        let receipt = run(job, DOWNLOAD_TIMEOUT, std::future::pending(), gate)
            .await
            .expect("actual pin prepared");
        assert!(receipt.usable());
        let probe = dir.path().join(format!("{id}-launch.json"));
        let shell = dir.path().join("metadata-shell");
        std::fs::write(&shell, format!(r"#!/usr/bin/env node
require('node:fs').writeFileSync({}, JSON.stringify({{argv:process.argv.slice(2), path:process.env.PATH}}));
", serde_json::to_string(&probe).unwrap())).unwrap();
        std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o700)).unwrap();
        let opts = SpawnOptions {
            provider,
            model: None,
            reasoning_effort: None,
            cwd: None,
            npx_launch_root: None,
            rules_file: None,
            mcp_config_file: None,
            env_mcp_config: None,
            unsloth_endpoint: None,
            quiet: false,
            provider_binary: None,
            extra_env: env.clone(),
            tools_to_remove: vec![],
            npx_fallback_binary: Some(&npx),
            npx_fallback_package: provider.npx_only_package,
            node_max_old_space_mb: None,
        };
        let mut launch = intent_acp::spawn::prepare_provider(&opts).unwrap();
        launch
            .command
            .env("npm_config_offline", "true")
            .env("npm_config_script_shell", &shell);
        let output = tokio::time::timeout(Duration::from_secs(30), launch.command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "{id}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let observed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&probe).unwrap()).unwrap();
        let root = receipt
            .files
            .iter()
            .find(|(path, _, _)| path.ends_with("node_modules/.package-lock.json"))
            .unwrap()
            .0
            .parent()
            .unwrap();
        assert!(std::env::split_paths(observed["path"].as_str().unwrap())
            .any(|path| path == root.join(".bin")));
        assert!(
            receipt.usable(),
            "offline launch must reuse the prepared tree unchanged"
        );
        eprintln!("{id}: prepared {} and resolved positional launch offline from {}; intercepted command {}", provider.npx_only_package.unwrap(), root.display(), observed["argv"]);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn no_op_and_success_dedup_then_cache_deletion_reprepare() {
    let queue = Preparation::default();
    let rejected: Arc<Selector> = Arc::new(|_, _| panic!("unknown or empty IDs must not probe"));
    queue.enqueue_with(vec![], SettingsFile::default(), &rejected);
    queue.enqueue_with(
        vec!["unknown".into(), "auggie".into()],
        SettingsFile::default(),
        &rejected,
    );
    assert!(queue.state.lock().unwrap().pending.is_empty());
    let dir = crate::test_support::test_tempdir("preparation-success-dedup");
    let root = dir.path().to_owned();
    let selector: Arc<Selector> = Arc::new(move |id, _| Some(fake_job(&root, id, None, false)));
    let enqueue = || {
        queue.enqueue_with(
            vec!["pi".into(), "pi".into()],
            SettingsFile::default(),
            &selector,
        );
    };
    enqueue();
    wait_for(|| queue.state.lock().unwrap().outcomes.contains_key("pi")).await;
    let first = queue.state.lock().unwrap().outcomes["pi"].completed;
    enqueue();
    wait_for(|| queue.state.lock().unwrap().pending.is_empty()).await;
    assert_eq!(queue.state.lock().unwrap().outcomes["pi"].completed, first);
    std::fs::remove_dir_all(dir.path().join("pi/npm")).unwrap();
    fixture_receipt(&dir.path().join("pi"));
    enqueue();
    wait_for(|| queue.state.lock().unwrap().outcomes["pi"].completed > first).await;
    assert!(queue.state.lock().unwrap().outcomes["pi"].receipt.is_some());
    queue.shutdown().await;
}

#[test]
fn pi_requires_detected_cli_and_rejects_known_old_versions_but_not_unknown() {
    use intent_providers::{pi_cli_gate, PiCliProbe};
    for (version, resolved, expected) in [
        ("0.81.0", true, true),
        ("0.80.0", true, false),
        ("unrecognized", true, true),
        ("0.81.0", false, false),
    ] {
        let status = crate::pi_cli::PiCliStatus {
            command: "pi".into(),
            resolved_path: resolved.then(|| PathBuf::from("/fixture/pi")),
            version_output: Some(version.into()),
            gate: pi_cli_gate(&PiCliProbe::Output(version.into())),
        };
        assert_eq!(pi_eligible(&status), expected, "{version}/{resolved}");
    }
}
