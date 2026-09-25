use super::*;

#[test]
fn version_output_never_becomes_an_error_or_account_payload() {
    for (output, expected) in [
        ("@agentclientprotocol/codex-acp 1.13.1\n", Some("1.13.1")),
        ("codex-acp v2.4.6", Some("2.4.6")),
        ("1.2.3-alpha.7", Some("1.2.3-alpha.7")),
        ("1.2.3 secret-token", None),
        ("1.2.3-account-secret", None),
        ("1.2.3\naccount=user@example.com", None),
        ("{\"access_token\":\"secret\"}", None),
        ("\u{1b}[31m1.2.3", None),
    ] {
        assert_eq!(
            parse_version(output.as_bytes(), VersionKind::Adapter).as_deref(),
            expected
        );
    }
    assert_eq!(
        parse_version(b"codex-cli 0.222.3\n", VersionKind::Runtime).as_deref(),
        Some("0.222.3")
    );
    assert_eq!(
        parse_version(b"codex-acp 1.2.3", VersionKind::Runtime),
        None
    );
    assert_eq!(safe_text("/adapter\n\u{1b}\u{202e}path"), "/adapter???path");
    assert_eq!(safe_text(&"x".repeat(900)).len(), 512);
}

#[cfg(unix)]
mod unix {
    use super::*;
    use intent_providers::discover::ProviderBinary;
    use std::os::unix::fs::{symlink, PermissionsExt};

    fn executable(path: &Path, body: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(target_os = "linux")]
    struct Cleanup(i32);

    #[cfg(target_os = "linux")]
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(self.0),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }

    struct Fixture {
        root: tempfile::TempDir,
        path: OsString,
        adapter: PathBuf,
        path_marker: PathBuf,
        npx_marker: PathBuf,
    }

    impl Fixture {
        fn new(adapter_version: &str, runtime_version: &str) -> Self {
            let root = crate::test_support::test_tempdir("codex-diagnostic-fixture");
            let bin = root.path().join("bin");
            std::fs::create_dir(&bin).unwrap();
            #[cfg(target_os = "linux")]
            {
                let node = intent_providers::find_node().expect("development host requires Node");
                symlink(std::fs::canonicalize(node).unwrap(), bin.join("node")).unwrap();
            }
            #[cfg(target_os = "macos")]
            executable(&bin.join("node"), "#!/bin/sh\nexit 93\n");
            let package_root = root
                .path()
                .join("node_modules/@agentclientprotocol/codex-acp");
            let adapter = package_root.join("dist/index.js");
            executable(&adapter, &format!("#!/usr/bin/env node\nif (process.argv[2] !== '--version') process.exit(90);\nconsole.log('@agentclientprotocol/codex-acp {adapter_version}');\n"));
            std::fs::write(
                package_root.join("package.json"),
                serde_json::to_vec(&serde_json::json!({
                    "name": "@agentclientprotocol/codex-acp", "version": adapter_version,
                    "bin": {"codex-acp": "dist/index.js"}
                }))
                .unwrap(),
            )
            .unwrap();
            let runtime = root.path().join("node_modules/@openai/codex/bin/codex.js");
            executable(&runtime, &format!("#!/usr/bin/env node\nif (process.argv[2] !== '--version') process.exit(91);\nconsole.log('codex-cli {runtime_version}');\n"));
            std::fs::write(root.path().join("node_modules/@openai/codex/package.json"),
                serde_json::to_vec(&serde_json::json!({"name":"@openai/codex", "version":runtime_version, "bin":{"codex":"bin/codex.js"}})).unwrap()).unwrap();
            let path_marker = root.path().join("unrelated-path-codex-ran");
            executable(
                &bin.join("codex"),
                &format!(
                    "#!/bin/sh\nprintf ran > '{}'\nprintf 'codex-cli 9.99.9\\n'\n",
                    path_marker.display()
                ),
            );
            // A managed local inspection must not execute npx at all, even
            // when it is available and would return a plausible version.
            let npx_marker = root.path().join("npx-ran");
            executable(
                &bin.join("npx"),
                &format!(
                    "#!/bin/sh\nprintf ran > '{}'\nexit 89\n",
                    npx_marker.display()
                ),
            );
            Self {
                root,
                path: bin.into_os_string(),
                adapter,
                path_marker,
                npx_marker,
            }
        }

        fn local(&self, source: ProviderBinarySource) -> CodexLaunch {
            CodexLaunch {
                selection: ProviderLaunch::Local(ProviderBinary {
                    path: self.adapter.clone(),
                    source,
                }),
                path: self.path.clone(),
                codex_path: None,
            }
        }

        fn managed(&self) -> CodexLaunch {
            CodexLaunch {
                selection: ProviderLaunch::Managed {
                    npx: self.root.path().join("bin/npx"),
                    package: CODEX_ACP_NPX_PACKAGE,
                },
                path: self.path.clone(),
                codex_path: None,
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn local_and_managed_report_distinct_measured_versions_never_path_codex() {
        let local = Fixture::new("2.4.6", "0.222.3");
        let launch = local.local(ProviderBinarySource::SettingsOverride);
        let result = launch.inspect_local().await;
        assert_eq!(result.report.launch_source, LaunchSource::SettingsOverride);
        assert_eq!(result.report.configured_package, CODEX_ACP_NPX_PACKAGE);
        assert_eq!(
            result.report.adapter_version,
            VersionMeasurement::Measured("2.4.6".into())
        );
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Measured("0.222.3".into())
        );
        assert_eq!(
            result.report.runtime_source,
            RuntimeSource::AdapterDependency
        );
        assert!(!result.report.removes_codex_overrides);
        assert!(!local.path_marker.exists());

        let pin = CODEX_ACP_NPX_PACKAGE.rsplit_once('@').unwrap().1;
        let managed = Fixture::new(pin, "0.333.4");
        let launch = managed.managed();
        let cold = launch.inspect_local().await;
        assert_eq!(
            cold.report.adapter_version,
            VersionMeasurement::Unknown(UnknownReason::ManagedPackageNotInspected)
        );
        assert_eq!(cold.report.runtime_source, RuntimeSource::Unknown);
        assert!(cold.runtime.is_none());
        assert_eq!(
            cold.report.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::ManagedPackageNotInspected)
        );
        assert!(!managed.npx_marker.exists());
        let resolved = launch.inspect_materialized(&managed.adapter).await;
        assert_eq!(resolved.report.launch_source, LaunchSource::ManagedNpm);
        assert!(resolved.report.removes_codex_overrides);
        assert_eq!(
            resolved.report.adapter_version,
            VersionMeasurement::Measured(pin.into())
        );
        assert_eq!(
            resolved.report.runtime_version,
            VersionMeasurement::Measured("0.333.4".into())
        );
        assert!(!managed.path_marker.exists());
        assert!(!managed.npx_marker.exists());
        assert!(resolved.runtime.unwrap().args[0]
            .to_string_lossy()
            .contains("@openai/codex/bin/codex.js"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn package_metadata_is_not_a_measured_version() {
        let pin = CODEX_ACP_NPX_PACKAGE.rsplit_once('@').unwrap().1;
        let fixture = Fixture::new(pin, "0.333.4");
        executable(
            &fixture.adapter,
            "#!/usr/bin/env node\nconsole.log('@agentclientprotocol/codex-acp 7.8.9');\n",
        );
        std::fs::write(
            fixture
                .root
                .path()
                .join("node_modules/@openai/codex/package.json"),
            r#"{"name":"@openai/codex","version":"0.1.2","bin":{"codex":"bin/codex.js"}}"#,
        )
        .unwrap();
        let result = fixture
            .managed()
            .inspect_materialized(&fixture.adapter)
            .await;
        assert_eq!(result.report.configured_package, CODEX_ACP_NPX_PACKAGE);
        assert_eq!(
            result.report.adapter_version,
            VersionMeasurement::Measured("7.8.9".into())
        );
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Measured("0.333.4".into())
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn recognized_local_adapter_retains_actual_runtime_override() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        let custom = Fixture::new("8.8.8", "0.444.5");
        let runtime = custom
            .root
            .path()
            .join("node_modules/@openai/codex/bin/codex.js");
        let mut launch = fixture.local(ProviderBinarySource::LocalDiscovery);
        launch.codex_path = Some(runtime.into_os_string());
        let result = launch.inspect_local().await;
        assert_eq!(result.report.launch_source, LaunchSource::LocalDiscovery);
        assert_eq!(
            result.report.runtime_source,
            RuntimeSource::EnvironmentOverride
        );
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Measured("0.444.5".into())
        );
        assert!(!fixture.path_marker.exists());
        launch.codex_path = Some("relative/runtime".into());
        let result = launch.inspect_local().await;
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::RelativeRuntimeOverride)
        );
        launch.codex_path = Some(fixture.root.path().join("missing").into_os_string());
        let result = launch.inspect_local().await;
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::RuntimeNotFound)
        );
    }

    #[tokio::test]
    async fn opaque_and_native_adapters_do_not_claim_a_nearby_dependency_or_override() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        for name in ["opaque-wrapper", "native-adapter"] {
            let adapter = fixture.root.path().join(name);
            executable(&adapter, "#!/bin/sh\nprintf 'codex-acp 3.5.7\\n'\n");
            if name == "native-adapter" {
                std::fs::copy("/bin/true", &adapter).unwrap();
            }
            let launch = CodexLaunch {
                selection: ProviderLaunch::Local(ProviderBinary {
                    path: adapter,
                    source: ProviderBinarySource::SettingsOverride,
                }),
                path: fixture.path.clone(),
                codex_path: Some(fixture.root.path().join("bin/codex").into_os_string()),
            };
            let result = launch.inspect_local().await;
            assert_eq!(
                result.report.adapter_version,
                VersionMeasurement::Unknown(if cfg!(target_os = "macos") {
                    UnknownReason::UnsupportedPlatform
                } else {
                    UnknownReason::OpaqueAdapter
                })
            );
            assert_eq!(
                result.report.runtime_version,
                VersionMeasurement::Unknown(if cfg!(target_os = "macos") {
                    UnknownReason::UnsupportedPlatform
                } else {
                    UnknownReason::OpaqueAdapter
                })
            );
            assert!(result.runtime.is_none());
        }
        // A JS wrapper adjacent to a real package is still not that package's
        // declared executable, and cannot borrow its runtime provenance.
        let wrapper = fixture.adapter.parent().unwrap().join("wrapper.js");
        executable(
            &wrapper,
            "#!/usr/bin/env node\nconsole.log('codex-acp 3.5.7');\n",
        );
        let mut launch = fixture.local(ProviderBinarySource::LocalDiscovery);
        launch.selection = ProviderLaunch::Local(ProviderBinary {
            path: wrapper,
            source: ProviderBinarySource::LocalDiscovery,
        });
        assert_eq!(
            launch.inspect_local().await.report.runtime_version,
            VersionMeasurement::Unknown(if cfg!(target_os = "macos") {
                UnknownReason::UnsupportedPlatform
            } else {
                UnknownReason::OpaqueAdapter
            })
        );
        assert!(!fixture.path_marker.exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn missing_dependency_and_wrong_managed_pin_stay_unknown() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        let result = fixture
            .managed()
            .inspect_materialized(&fixture.adapter)
            .await;
        assert_eq!(
            result.report.adapter_version,
            VersionMeasurement::Unknown(UnknownReason::PackageMismatch)
        );
        assert!(result.runtime.is_none());
        std::fs::remove_file(
            fixture
                .root
                .path()
                .join("node_modules/@openai/codex/bin/codex.js"),
        )
        .unwrap();
        let result = fixture
            .local(ProviderBinarySource::LocalDiscovery)
            .inspect_local()
            .await;
        assert_eq!(
            result.report.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::RuntimeNotFound)
        );
        assert!(!fixture.path_marker.exists());
    }

    #[tokio::test]
    async fn symlink_is_inspected_from_the_actual_adapter_package() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        let link = fixture.root.path().join("bin/codex-acp");
        symlink(&fixture.adapter, &link).unwrap();
        let launch = CodexLaunch {
            selection: ProviderLaunch::Local(ProviderBinary {
                path: link,
                source: ProviderBinarySource::LocalDiscovery,
            }),
            path: fixture.path.clone(),
            codex_path: None,
        };
        assert_eq!(
            launch.inspect_local().await.report.runtime_version,
            if cfg!(target_os = "macos") {
                VersionMeasurement::Unknown(UnknownReason::UnsupportedPlatform)
            } else {
                VersionMeasurement::Measured("0.222.3".into())
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn child_failures_are_sanitized_and_checks_are_isolated_and_bounded() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        let probe = fixture.root.path().join("probe");
        executable(&probe, "#!/bin/sh\nprintf 'account=user@example.com token=secret\\n'\nprintf 'secret failure\\n' >&2\nexit 1\n");
        let result = local_output(Command::new(&probe), &fixture.path, LOCAL_TIMEOUT).await;
        assert_eq!(result, Err(UnknownReason::UnsuccessfulExit));
        executable(
            &probe,
            "#!/usr/bin/env node\nprocess.stdout.write('x'.repeat(20000));\n",
        );
        assert_eq!(
            local_output(Command::new(&probe), &fixture.path, LOCAL_TIMEOUT).await,
            Err(UnknownReason::OutputLimit)
        );
        executable(&probe, "#!/bin/sh\n[ \"$HOME\" = \"$PWD\" ] || exit 1\n[ \"$CODEX_HOME\" = \"$PWD\" ] || exit 2\n[ -z \"$CODEX_CONFIG$OPENAI_API_KEY$NODE_OPTIONS\" ] || exit 3\nprintf 'isolated'\n");
        let mut command = Command::new(&probe);
        command
            .env("CODEX_CONFIG", "user MCP config")
            .env("OPENAI_API_KEY", "secret")
            .env("NODE_OPTIONS", "secret");
        assert_eq!(
            local_output(command, &fixture.path, LOCAL_TIMEOUT)
                .await
                .unwrap(),
            b"isolated"
        );
        executable(
            &probe,
            "#!/usr/bin/env node\nsetInterval(() => {}, 1000);\n",
        );
        assert_eq!(
            local_output(
                Command::new(&probe),
                &fixture.path,
                Duration::from_millis(50)
            )
            .await,
            Err(UnknownReason::TimedOut)
        );
    }

    #[tokio::test]
    async fn offline_inspection_does_not_execute_opaque_adapter_wrappers() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        for (name, body) in [
            ("wrapper", "#!/bin/sh\nexec npx --yes codex-acp \"$@\"\n"),
            ("wrapper.js", "#!/usr/bin/env node\nrequire('node:child_process').spawnSync('npx', ['--yes', 'codex-acp', ...process.argv.slice(2)]);\n"),
        ] {
            let wrapper = fixture.adapter.parent().unwrap().join(name);
            executable(&wrapper, body);
            let mut launch = fixture.local(ProviderBinarySource::SettingsOverride);
            launch.selection = ProviderLaunch::Local(ProviderBinary {
                path: wrapper,
                source: ProviderBinarySource::SettingsOverride,
            });
            let result = launch.inspect_local().await;
            assert!(!fixture.npx_marker.exists(), "default inspection executed npm through {name}");
            assert!(matches!(result.report.adapter_version, VersionMeasurement::Unknown(_)));
            assert!(result.runtime.is_none());
        }
    }

    #[tokio::test]
    async fn offline_inspection_does_not_execute_opaque_runtime_overrides() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        for (name, body) in [
            ("runtime", "#!/bin/sh\nexec npx --yes codex \"$@\"\n"),
            ("runtime.js", "#!/usr/bin/env node\nrequire('node:child_process').spawnSync('npx', ['--yes', 'codex', ...process.argv.slice(2)]);\n"),
        ] {
            let wrapper = fixture.root.path().join(name);
            executable(&wrapper, body);
            let mut launch = fixture.local(ProviderBinarySource::SettingsOverride);
            launch.codex_path = Some(wrapper.into_os_string());
            let result = launch.inspect_local().await;
            assert!(!fixture.npx_marker.exists(), "default inspection executed npm through {name}");
            assert!(matches!(result.report.runtime_version, VersionMeasurement::Unknown(_)));
            assert!(result.runtime.is_none());
        }
    }

    #[tokio::test]
    async fn version_errors_do_not_leak_into_serialized_report() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        executable(&fixture.adapter, "#!/usr/bin/env node\nconsole.log('account=user@example.com token=secret');\nconsole.error('secret error');\nprocess.exit(1);\n");
        let result = fixture
            .local(ProviderBinarySource::SettingsOverride)
            .inspect_local()
            .await;
        let json = serde_json::to_string(&result.report).unwrap();
        assert_eq!(
            result.report.adapter_version,
            VersionMeasurement::Unknown(if cfg!(target_os = "macos") {
                UnknownReason::UnsupportedPlatform
            } else {
                UnknownReason::UnsuccessfulExit
            })
        );
        assert!(!json.contains("secret"));
        assert!(!json.contains("user@example.com"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn probe_process_preserves_stdio_and_keeps_home_until_cleanup() {
        use tokio::io::AsyncWriteExt;

        let fixture = Fixture::new("2.4.6", "0.222.3");
        let home = crate::test_support::test_tempdir("codex-stdio-probe");
        let home_path = home.path().to_path_buf();
        let mut command = Command::new(fixture.root.path().join("bin/node"));
        command.env_clear().env("PATH", &fixture.path)
            .env("HOME", &home_path).current_dir(&home_path)
            // Host-wide instrumentation must not add output to this stdio fixture.
            .env("DD_TRACE_ENABLED", "false").env("DD_TRACE_STARTUP_LOGS", "false")
            .args(["-e", "const rl = require('node:readline').createInterface({input:process.stdin}); rl.once('line', line => { console.log(line); console.error('private stderr'); rl.close(); process.exitCode = 7; });"]);
        let mut probe = process::ProbeProcess::spawn(command, home).await.unwrap();
        let mut stdin = probe.stdin.take().unwrap();
        let mut stdout = probe.stdout.take().unwrap();
        let mut stderr = probe.stderr.take().unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            stdin
                .write_all(b"{\"request\":\"fixture\"}\n")
                .await
                .unwrap();
            drop(stdin);
            let mut out = String::new();
            let mut err = String::new();
            tokio::try_join!(
                stdout.read_to_string(&mut out),
                stderr.read_to_string(&mut err)
            )
            .unwrap();
            let status = probe.wait().await.unwrap();
            (out, err, status)
        })
        .await
        .unwrap();
        assert_eq!(result.0, "{\"request\":\"fixture\"}\n");
        assert_eq!(result.1, "private stderr\n");
        assert_eq!(result.2.code(), Some(7));
        assert!(home_path.is_dir(), "leader exit must not release its home");
        probe.cleanup().await.unwrap();
        assert!(!home_path.exists());
    }

    #[tokio::test]
    async fn production_command_scrubs_managed_overrides_and_preserves_local_ones() {
        let fixture = Fixture::new("2.4.6", "0.222.3");
        for (launch, managed) in [
            (fixture.managed(), true),
            (fixture.local(ProviderBinarySource::SettingsOverride), false),
        ] {
            let mut options = launch.spawn_options();
            options
                .extra_env
                .insert("CODEX_PATH".into(), "custom-runtime".into());
            options
                .extra_env
                .insert("CODEX_CONFIG".into(), "private config".into());
            let command = build_command(&options);
            for key in ["CODEX_PATH", "CODEX_CONFIG"] {
                let value = effective_env(&command, key);
                assert_eq!(value.is_none(), managed);
            }
        }
    }

    /// The child reports its process tree over a local socket before the test
    /// cancels it. No timing sleep is used to guess whether spawning finished.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancellation_reaps_the_process_tree_and_removes_temporary_home() {
        cancellation_case(false).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancellation_during_teardown_keeps_the_escaped_descendant_snapshot() {
        cancellation_case(true).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn early_exit_reaps_detached_child_before_removing_home() {
        early_exit_case(false).await;
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn early_exit_with_inherited_stdout_reaps_detached_child_on_timeout() {
        early_exit_case(true).await;
    }

    #[cfg(target_os = "linux")]
    async fn early_exit_case(inherit_stdout: bool) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        struct ProbeCleanup {
            task: tokio::task::AbortHandle,
            observation: PathBuf,
        }

        impl Drop for ProbeCleanup {
            fn drop(&mut self) {
                self.task.abort();
                if let Ok(text) = std::fs::read_to_string(&self.observation) {
                    for pid in text.lines().filter_map(|line| line.parse().ok()) {
                        drop(Cleanup(pid));
                    }
                }
            }
        }

        let fixture = Fixture::new("2.4.6", "0.222.3");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let observation = fixture.root.path().join("child-pid");
        let mut command = Command::new(fixture.root.path().join("bin/node"));
        command.args(["-e", r"
const fs = require('node:fs');
const child = require('node:child_process').spawn(process.execPath,
  ['-e', String.raw`
    process.on('SIGTERM', () => {
      const socket = require('node:net').connect(Number(process.argv[1]), '127.0.0.1');
      socket.on('connect', () => socket.end(JSON.stringify({homeExists: require('node:fs').existsSync(process.cwd())}) + '\n', () => process.exit(0)));
    });
    process.send('ready');
    process.disconnect();
    setInterval(() => {}, 1000);
  `, process.argv[1]],
  {detached: true, stdio: ['ignore', process.argv[3] === 'true' ? 'inherit' : 'ignore', 'ignore', 'ipc']});
fs.writeFileSync(process.argv[2], child.pid + '\n' + process.pid);
child.unref();
child.once('message', () => {
  const socket = require('node:net').connect(Number(process.argv[1]), '127.0.0.1');
  socket.on('connect', () => socket.write(JSON.stringify({pid: child.pid, leader: process.pid, home: process.cwd()}) + '\n'));
  socket.once('data', () => { console.log('codex-acp 5.6.7'); process.exit(0); });
});
"])
            .arg(port.to_string())
            .arg(&observation)
            .arg(inherit_stdout.to_string());
        let path = fixture.path.clone();
        let task =
            tokio::spawn(async move { local_output(command, &path, Duration::from_secs(2)).await });
        let _cleanup = ProbeCleanup {
            task: task.abort_handle(),
            observation,
        };
        let (socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = BufReader::new(socket);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), socket.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let observed: serde_json::Value = serde_json::from_str(&line).unwrap();
        let pid = i32::try_from(observed["pid"].as_i64().unwrap()).unwrap();
        let leader = i32::try_from(observed["leader"].as_i64().unwrap()).unwrap();
        let home = PathBuf::from(observed["home"].as_str().unwrap());
        assert_ne!(
            nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(pid))).unwrap(),
            nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(leader))).unwrap(),
            "fixture must actually escape the leader's process group"
        );
        assert!(home.is_dir());
        socket.get_mut().write_all(b"exit").await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        if inherit_stdout {
            assert_eq!(result, Err(UnknownReason::TimedOut));
        } else {
            assert_eq!(result.unwrap(), b"codex-acp 5.6.7\n");
        }
        assert!(
            !home.exists(),
            "completed probe retained its temporary home"
        );
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH),
            "detached child {pid} was not reaped before its temporary home disappeared"
        );
        let (mut notice, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), notice.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let notice: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            notice["homeExists"], true,
            "probe home disappeared before its descendant stopped"
        );
    }

    #[cfg(target_os = "linux")]
    async fn cancellation_case(during_teardown: bool) {
        use tokio::io::AsyncReadExt;
        let fixture = Fixture::new("2.4.6", "0.222.3");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut command = Command::new(fixture.root.path().join("bin/node"));
        command
            .args([
                "-e",
                r"
const child = require('node:child_process').spawn(process.execPath,
  ['-e', 'setInterval(() => {}, 1000)'], {stdio: 'inherit', detached: true});
const socket = require('node:net').connect(Number(process.argv[1]), '127.0.0.1');
socket.on('connect', () => socket.end(JSON.stringify({pid: child.pid, home: process.cwd()})));
process.on('SIGTERM', () => {
  const notice = require('node:net').connect(Number(process.argv[1]), '127.0.0.1');
  notice.on('connect', () => notice.end('terminating', () => process.exit(0)));
});
setInterval(() => {}, 1000);
",
            ])
            .arg(port.to_string());
        let path = fixture.path.clone();
        let timeout = Duration::from_secs(if during_teardown { 1 } else { 10 });
        let task = tokio::spawn(async move { local_output(command, &path, timeout).await });
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let observation: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let pid = i32::try_from(observation["pid"].as_i64().unwrap()).unwrap();
        let home = PathBuf::from(observation["home"].as_str().unwrap());
        let _cleanup = Cleanup(pid);
        assert!(home.is_dir());
        if during_teardown {
            let (mut notice, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut text = String::new();
            tokio::time::timeout(Duration::from_secs(5), notice.read_to_string(&mut text))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(text, "terminating");
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            while home.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled local check must finish reaping and remove its home");
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH),
            "cancelled probe must reap descendants before removing its home"
        );
    }
}
