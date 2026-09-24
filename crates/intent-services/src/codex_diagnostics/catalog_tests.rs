use super::*;

#[test]
fn catalog_ids_are_untrusted_and_keep_original_sources() {
    let mut auth = Authentication::default();
    auth.secrets.insert("credential-canary".into());
    let catalog = parse_acp(
        &json!({"models":{"availableModels":[
        {"modelId":"fixture-high"},{"modelId":"credential-canary"},
        {"modelId":"user@example.invalid"},{"modelId":"model\nforged"},
        {"modelId":"model\u{202e}evil"},{"modelId":"sk-secret"}]},
        "configOptions":[{"id":"model","options":[{"options":[{"value":"fixture"}]}]}]}),
        &auth,
    )
    .unwrap();
    assert_eq!(catalog.withheld_model_count, 5);
    assert_eq!(catalog.models[0].id, "fixture-high");
    assert_eq!(catalog.models[0].source, CatalogSource::AcpAvailableModels);
    assert_eq!(catalog.models[1].source, CatalogSource::AcpConfigOptions);
    let text = serde_json::to_string(&catalog).unwrap();
    assert!(!text.contains("credential-canary"));
    assert!(!text.contains("example.invalid"));
    assert!(!text.contains("forged"));
}

#[test]
fn malformed_empty_and_absent_catalogs_are_distinct() {
    let auth = Authentication::default();
    assert!(!parse_acp(&json!({}), &auth).unwrap().advertised);
    let empty = parse_acp(&json!({"models":{"availableModels":[]}}), &auth).unwrap();
    assert!(empty.advertised);
    assert!(empty.models.is_empty());
    for value in [
        json!({"models":"bad"}),
        json!({"models":{"availableModels":"bad"}}),
        json!({"models":{"availableModels":[{"name":"id missing"}]}}),
        json!({"configOptions":[{"id":"model","options":null}]}),
    ] {
        assert_eq!(
            parse_acp(&value, &auth),
            Err(CatalogFailure::InvalidResponse)
        );
    }
    let too_many = json!({"availableModels":vec![json!({"id":"fixture"});MODEL_LIMIT+1]});
    assert_eq!(
        parse_acp(&too_many, &auth),
        Err(CatalogFailure::OutputLimit)
    );
}

#[cfg(unix)]
mod unix {
    use super::super::super::{LaunchSource, RuntimeSource, VersionMeasurement};
    use super::*;
    use intent_providers::discover::{ProviderBinary, ProviderBinarySource};
    use std::ffi::OsString;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};

    struct Fixture {
        root: tempfile::TempDir,
        adapter: PathBuf,
        runtime: PathBuf,
        path: OsString,
    }

    fn executable(path: &Path, script: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, script).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    impl Fixture {
        fn new(config: &Value) -> Self {
            let root = crate::test_support::test_tempdir("codex-catalog-fixture");
            let bin = root.path().join("bin");
            std::fs::create_dir(&bin).unwrap();
            let node = std::fs::canonicalize(intent_providers::find_node().expect("Node required"))
                .unwrap();
            symlink(node, bin.join("node")).unwrap();
            let adapter = root
                .path()
                .join("node_modules/@agentclientprotocol/codex-acp/dist/index.js");
            let runtime = root.path().join("node_modules/@openai/codex/bin/codex.js");
            for (path, role) in [(&adapter, "acp"), (&runtime, "raw")] {
                executable(
                    path,
                    &format!(
                        "#!/usr/bin/env node\nconst role={};const fixture={};\n{}",
                        json!(role),
                        json!(root.path()),
                        include_str!("catalog_fixture.cjs")
                    ),
                );
            }
            let pin = intent_providers::config::CODEX_ACP_NPX_PACKAGE
                .rsplit_once('@')
                .unwrap()
                .1;
            std::fs::write(adapter.parent().unwrap().parent().unwrap().join("package.json"),
                json!({"name":"@agentclientprotocol/codex-acp","version":pin,"bin":{"codex-acp":"dist/index.js"}}).to_string()).unwrap();
            std::fs::write(
                runtime
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("package.json"),
                json!({"name":"@openai/codex","version":"0.333.4","bin":{"codex":"bin/codex.js"}})
                    .to_string(),
            )
            .unwrap();
            executable(&bin.join("codex"),&format!("#!/usr/bin/env node\nrequire('fs').writeFileSync({},'wrong');process.exit(92);",json!(root.path().join("path-codex-ran"))));
            executable(
                &bin.join("npx"),
                &format!(
                    r"#!/usr/bin/env node
const fs=require('fs'),path=require('path');
const fixture={};
fs.appendFileSync(path.join(fixture,'events.jsonl'),JSON.stringify({{role:'npx',args:process.argv.slice(2),home:process.env.HOME}})+'\n');
const target=path.join(process.env.HOME,'npm-cache','node_modules');
fs.cpSync(path.join(fixture,'node_modules'),target,{{recursive:true}});
const adapter=path.join(target,'@agentclientprotocol/codex-acp/dist/index.js');
const child=require('child_process').spawn(process.execPath,[adapter],{{stdio:'inherit',env:process.env}});
child.on('exit',code=>process.exit(code||0));
",
                    json!(root.path())
                ),
            );
            std::fs::write(root.path().join("fixture.json"), config.to_string()).unwrap();
            std::fs::write(root.path().join("events.jsonl"), "").unwrap();
            Self {
                root,
                adapter,
                runtime,
                path: bin.into_os_string(),
            }
        }

        fn launch(&self, managed: bool) -> CodexLaunch {
            CodexLaunch {
                selection: if managed {
                    ProviderLaunch::Managed {
                        npx: self.root.path().join("bin/npx"),
                        package: intent_providers::config::CODEX_ACP_NPX_PACKAGE,
                    }
                } else {
                    ProviderLaunch::Local(ProviderBinary {
                        path: self.adapter.clone(),
                        source: ProviderBinarySource::SettingsOverride,
                    })
                },
                path: self.path.clone(),
                codex_path: None,
            }
        }

        async fn auth(&self) -> Authentication {
            let user = self.root.path().join("user");
            std::fs::create_dir_all(&user).unwrap();
            std::fs::write(
                user.join("auth.json"),
                r#"{"tokens":{"access_token":"credential-canary","account_id":"account-canary"}}"#,
            )
            .unwrap();
            std::fs::write(
                user.join("config.toml"),
                "model = 'cached-model'\n[mcp_servers.bad]\ncommand = 'launch-mcp'\n",
            )
            .unwrap();
            std::fs::write(user.join("models_cache.json"), "user-cache-sentinel").unwrap();
            Authentication::read(
                Some(&user),
                vec![("OPENAI_API_KEY", "sk-fixture-key".into())],
            )
            .await
            .unwrap()
        }

        async fn run(&self, managed: bool) -> CodexCatalogReport {
            self.launch(managed)
                .catalogs_with_auth(Ok(self.auth().await), Limits::default())
                .await
        }

        fn events(&self) -> Vec<Value> {
            std::fs::read_to_string(self.root.path().join("events.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }

        fn assert_clean(&self) {
            assert!(!self.root.path().join("path-codex-ran").exists());
            assert!(!self.root.path().join("mcp-launched").exists());
            for event in self.events() {
                assert!(event.get("isolationFailure").is_none());
                assert!(event.get("unexpectedMethod").is_none());
                if let Some(home) = event.get("home").and_then(Value::as_str) {
                    assert!(
                        !Path::new(home).exists(),
                        "temporary probe home must be removed"
                    );
                }
                if let Some(pid) = event.get("pid").and_then(Value::as_i64) {
                    assert!(
                        nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(i32::try_from(pid).unwrap()),
                            None
                        )
                        .is_err(),
                        "probe must be reaped"
                    );
                    assert_eq!(event["private"], true);
                }
            }
            assert_eq!(
                std::fs::read_to_string(self.root.path().join("user/models_cache.json")).unwrap(),
                "user-cache-sentinel"
            );
            assert!(
                std::fs::read_to_string(self.root.path().join("user/config.toml"))
                    .unwrap()
                    .contains("mcp_servers")
            );
        }
    }

    fn catalog(outcome: &CatalogOutcome) -> &Catalog {
        let CatalogOutcome::Success(catalog) = outcome else {
            panic!("expected successful catalog, got {outcome:?}")
        };
        catalog
    }

    #[tokio::test]
    async fn command_isolation_replaces_inherited_configuration_and_preloads() {
        let fixture = Fixture::new(&json!({}));
        let auth = fixture.auth().await;
        let home = auth.home().await.unwrap();
        let mut command = Command::new(&fixture.adapter);
        for key in [
            "NODE_OPTIONS",
            "CODEX_CONFIG",
            "CODEX_PATH",
            "npm_config_workspace",
            "UNRELATED_SECRET",
        ] {
            command.env(key, "intentd-inherited-preload-canary");
        }
        auth.isolate(&mut command, &fixture.launch(true), home.path());
        let keys: Vec<_> = command
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| value.map(|_| key))
            .collect();
        for key in [
            "NODE_OPTIONS",
            "CODEX_CONFIG",
            "CODEX_PATH",
            "npm_config_workspace",
            "UNRELATED_SECRET",
        ] {
            assert!(!keys.contains(&std::ffi::OsStr::new(key)));
        }
        assert!(keys.contains(&std::ffi::OsStr::new("OPENAI_API_KEY")));
        assert_eq!(command.as_std().get_current_dir(), Some(home.path()));
    }

    #[tokio::test]
    async fn local_catalogs_are_isolated_prompt_free_paginated_and_observational() {
        let fixture = Fixture::new(&json!({}));
        let report = fixture.run(false).await;
        assert!(
            matches!(report.acp, CatalogOutcome::Success(_)),
            "{report:?}; events: {:?}",
            fixture.events()
        );
        assert_eq!(report.runtime.launch_source, LaunchSource::SettingsOverride);
        assert_eq!(
            report.runtime.runtime_source,
            RuntimeSource::AdapterDependency
        );
        assert_eq!(catalog(&report.acp).models.len(), 3);
        assert_eq!(catalog(&report.raw).models.len(), 2);
        assert_eq!(catalog(&report.raw).models[1].hidden, Some(true));
        assert_eq!(
            report.observe("fixture-model"),
            ModelObservation::PresentInBoth
        );
        assert_eq!(
            report.observe("fixture-model-high"),
            ModelObservation::PresentInAcpOnly
        );
        assert_eq!(
            report.observe("fixture-alias"),
            ModelObservation::PresentInRawOnly
        );
        assert_eq!(
            report.observe("missing-model"),
            ModelObservation::AbsentFromBothObservedCatalogs
        );
        assert!(report
            .observe("missing-model")
            .message()
            .contains("does not establish"));
        let text = serde_json::to_string(&report).unwrap();
        for canary in [
            "credential-canary",
            "account-canary",
            "example.invalid",
            "session-canary",
            "private-cursor",
            "sk-fixture-key",
        ] {
            assert!(!text.contains(canary));
        }
        for role in ["acp", "raw"] {
            let methods: Vec<_> = fixture
                .events()
                .iter()
                .filter(|v| v["role"] == role)
                .filter_map(|v| v["method"].as_str().map(String::from))
                .collect();
            assert_eq!(
                methods,
                if role == "acp" {
                    vec!["initialize", "session/new"]
                } else {
                    vec![
                        "initialize",
                        "initialized",
                        "account/read",
                        "model/list",
                        "model/list",
                    ]
                }
            );
        }
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn managed_catalogs_use_actual_launched_package_until_raw_finishes() {
        let fixture = Fixture::new(&json!({}));
        let launch = fixture.launch(true);
        let cold = launch.inspect_local().await;
        assert!(fixture.events().is_empty());
        assert_eq!(
            cold.report.adapter_version,
            VersionMeasurement::Unknown(UnknownReason::ManagedPackageNotInspected)
        );
        let report = fixture.run(true).await;
        assert_eq!(report.runtime.launch_source, LaunchSource::ManagedNpm);
        assert!(report.runtime.removes_codex_overrides);
        assert!(report
            .runtime
            .runtime_path
            .as_ref()
            .unwrap()
            .contains("npm-cache/node_modules/@openai/codex/bin/codex.js"));
        assert_eq!(
            report.runtime.runtime_version,
            VersionMeasurement::Measured("0.333.4".into())
        );
        assert_eq!(
            report.observe("fixture-model"),
            ModelObservation::PresentInBoth
        );
        let events = fixture.events();
        let npx = events.iter().find(|v| v["role"] == "npx").unwrap();
        assert_eq!(
            npx["args"],
            json!([
                "--workspaces=false",
                "-y",
                intent_providers::config::CODEX_ACP_NPX_PACKAGE
            ])
        );
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn independent_failures_never_count_as_empty_success() {
        for (role, mode, reason) in [
            ("acp", "auth", CatalogFailure::AuthenticationUnavailable),
            ("raw", "auth", CatalogFailure::AuthenticationUnavailable),
            ("acp", "unsupported", CatalogFailure::UnsupportedCapability),
            ("raw", "rpcError", CatalogFailure::RequestFailed),
            (
                "acp",
                "notificationError",
                CatalogFailure::AuthenticationUnavailable,
            ),
            ("acp", "peerRequest", CatalogFailure::UnsupportedCapability),
            ("raw", "exit", CatalogFailure::ProcessExited),
        ] {
            let fixture = Fixture::new(&json!({role:mode}));
            let report = fixture.run(false).await;
            let (failed, success) = if role == "acp" {
                (&report.acp, &report.raw)
            } else {
                (&report.raw, &report.acp)
            };
            assert_eq!(*failed, CatalogOutcome::Failed(reason));
            assert!(!catalog(success).models.is_empty());
            assert_eq!(
                report.observe("missing-model"),
                ModelObservation::Inconclusive
            );
            let text = serde_json::to_string(&report).unwrap();
            assert!(!text.contains("credential-canary"));
            assert!(!text.contains("account-canary"));
            fixture.assert_clean();
        }
    }

    #[tokio::test]
    async fn absent_and_empty_success_are_not_authentication_failures() {
        for session in [
            json!({"sessionId":"fixture"}),
            json!({"sessionId":"fixture","models":{"availableModels":[]}}),
        ] {
            let fixture =
                Fixture::new(&json!({"session":session,"pages":[{"data":[],"nextCursor":null}]}));
            let report = fixture.run(false).await;
            assert!(catalog(&report.acp).models.is_empty());
            assert_eq!(
                catalog(&report.acp).advertised,
                session.get("models").is_some()
            );
            assert!(catalog(&report.raw).models.is_empty());
            assert_eq!(
                report.observe("missing-model"),
                ModelObservation::AbsentFromBothObservedCatalogs
            );
            fixture.assert_clean();
        }
    }

    #[tokio::test]
    async fn fresh_probes_ignore_and_preserve_user_cache() {
        let fixture = Fixture::new(&json!({"model":"first-model"}));
        assert_eq!(
            fixture.run(false).await.observe("first-model"),
            ModelObservation::PresentInBoth
        );
        std::fs::write(
            fixture.root.path().join("fixture.json"),
            json!({"model":"second-model"}).to_string(),
        )
        .unwrap();
        let report = fixture.run(false).await;
        assert_eq!(
            report.observe("first-model"),
            ModelObservation::AbsentFromBothObservedCatalogs
        );
        assert_eq!(
            report.observe("second-model"),
            ModelObservation::PresentInBoth
        );
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn timeout_output_and_pagination_limits_preserve_other_phase_and_cleanup() {
        for (role, mode, reason) in [
            ("acp", "timeout", CatalogFailure::TimedOut),
            ("raw", "timeout", CatalogFailure::TimedOut),
            ("acp", "stdoutFlood", CatalogFailure::OutputLimit),
            ("raw", "stderrFlood", CatalogFailure::OutputLimit),
            ("acp", "malformed", CatalogFailure::InvalidResponse),
            ("raw", "repeatCursor", CatalogFailure::PaginationLimit),
            ("raw", "pages", CatalogFailure::PaginationLimit),
        ] {
            let fixture = Fixture::new(&json!({role:mode}));
            let start = tokio::time::Instant::now();
            let report = fixture
                .launch(false)
                .catalogs_with_auth(
                    Ok(fixture.auth().await),
                    Limits {
                        timeout: Duration::from_secs(2),
                        pages: 2,
                    },
                )
                .await;
            assert!(start.elapsed() < Duration::from_secs(20));
            let (failed, success) = if role == "acp" {
                (&report.acp, &report.raw)
            } else {
                (&report.raw, &report.acp)
            };
            assert_eq!(*failed, CatalogOutcome::Failed(reason), "{role} {mode}");
            assert!(!catalog(success).models.is_empty());
            fixture.assert_clean();
        }
    }

    #[tokio::test]
    async fn missing_runtime_does_not_discard_acp_catalog() {
        let fixture = Fixture::new(&json!({}));
        std::fs::remove_file(&fixture.runtime).unwrap();
        let report = fixture.run(false).await;
        assert!(!catalog(&report.acp).models.is_empty());
        assert_eq!(
            report.raw,
            CatalogOutcome::Failed(CatalogFailure::RuntimeUnverified)
        );
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn unsafe_ids_and_late_account_metadata_are_withheld() {
        let fixture = Fixture::new(
            &json!({"session":{"sessionId":"fixture","models":{"availableModels":[
            {"modelId":"account-canary"},{"modelId":"credential-canary"},{"modelId":"user@example.invalid"}]}},
            "pages":[{"data":[{"id":"valid-model","model":"account-canary"}],"nextCursor":null}]}),
        );
        let mut auth = fixture.auth().await;
        auth.secrets.remove("account-canary");
        let report = fixture
            .launch(false)
            .catalogs_with_auth(Ok(auth), Limits::default())
            .await;
        assert_eq!(catalog(&report.acp).withheld_model_count, 3);
        assert_eq!(catalog(&report.raw).withheld_model_count, 1);
        assert_eq!(report.observe("missing"), ModelObservation::Inconclusive);
        let text = serde_json::to_string(&report).unwrap();
        assert!(!text.contains("account-canary"));
        assert!(!text.contains("credential-canary"));
        assert!(!text.contains("example.invalid"));
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn managed_acp_failure_keeps_raw_result_and_pin_mismatch_is_unverified() {
        let fixture = Fixture::new(&json!({"acp":"auth"}));
        let report = fixture.run(true).await;
        assert_eq!(
            report.acp,
            CatalogOutcome::Failed(CatalogFailure::AuthenticationUnavailable)
        );
        assert!(!catalog(&report.raw).models.is_empty());
        fixture.assert_clean();

        let fixture = Fixture::new(&json!({}));
        std::fs::write(fixture.adapter.parent().unwrap().parent().unwrap().join("package.json"),
            json!({"name":"@agentclientprotocol/codex-acp","version":"0.0.0","bin":{"codex-acp":"dist/index.js"}}).to_string()).unwrap();
        let report = fixture.run(true).await;
        assert!(!catalog(&report.acp).models.is_empty());
        assert_eq!(
            report.runtime.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::PackageMismatch)
        );
        assert_eq!(
            report.raw,
            CatalogOutcome::Failed(CatalogFailure::RuntimeUnverified)
        );
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn managed_launch_without_an_entrypoint_never_guesses_a_cached_runtime() {
        let fixture = Fixture::new(&json!({}));
        executable(
            &fixture.root.path().join("bin/npx"),
            "#!/usr/bin/env node\nprocess.exit(7);\n",
        );
        let report = fixture.run(true).await;
        assert_eq!(
            report.acp,
            CatalogOutcome::Failed(CatalogFailure::ProcessExited)
        );
        assert_eq!(
            report.raw,
            CatalogOutcome::Failed(CatalogFailure::RuntimeUnverified)
        );
        assert_eq!(
            report.runtime.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::PackageUnreadable)
        );
        assert!(report.runtime.runtime_path.is_none());
        assert!(fixture.events().is_empty());
    }

    #[tokio::test]
    async fn cancelling_raw_probe_cleans_both_private_homes() {
        let fixture = Fixture::new(&json!({"raw":"timeout"}));
        let launch = fixture.launch(true);
        let auth = fixture.auth().await;
        let task =
            tokio::spawn(
                async move { launch.catalogs_with_auth(Ok(auth), Limits::default()).await },
            );
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                poll.tick().await;
                if fixture
                    .events()
                    .iter()
                    .any(|event| event["role"] == "raw" && event["method"] == "initialize")
                {
                    break;
                }
            }
        })
        .await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        ready.expect("raw fixture reached its pending request");
        tokio::time::timeout(Duration::from_secs(8), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                poll.tick().await;
                if fixture
                    .events()
                    .iter()
                    .filter_map(|event| event["home"].as_str())
                    .all(|home| !Path::new(home).exists())
                {
                    break;
                }
            }
        })
        .await
        .expect("cancelled catalog guards complete cleanup");
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn notification_catalog_requires_successful_session() {
        let fixture = Fixture::new(&json!({"acp":"notification"}));
        let report = fixture.run(false).await;
        assert_eq!(catalog(&report.acp).models[0].id, "notification-model");
        assert_eq!(catalog(&report.acp).models.len(), 1);
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn local_runtime_override_is_used_by_both_phases() {
        let fixture = Fixture::new(&json!({}));
        let custom = Fixture::new(&json!({"model":"custom-model"}));
        let mut launch = fixture.launch(false);
        launch.codex_path = Some(custom.runtime.clone().into_os_string());
        let report = launch
            .catalogs_with_auth(Ok(fixture.auth().await), Limits::default())
            .await;
        assert_eq!(
            report.runtime.runtime_source,
            RuntimeSource::EnvironmentOverride
        );
        assert_eq!(catalog(&report.raw).models[0].id, "custom-model");
        let boot = fixture
            .events()
            .into_iter()
            .find(|v| v["started"] == true)
            .unwrap();
        assert_eq!(boot["codexPath"], json!(custom.runtime));
        fixture.assert_clean();
    }

    #[tokio::test]
    async fn invalid_auth_material_is_not_silently_a_logged_out_probe() {
        let root = crate::test_support::test_tempdir("codex-catalog-auth");
        for text in [
            "not json".to_owned(),
            "null".to_owned(),
            "x".repeat(catalog_io::FILE_LIMIT + 1),
        ] {
            std::fs::write(root.path().join("auth.json"), text).unwrap();
            assert!(matches!(
                Authentication::read(Some(root.path()), vec![]).await,
                Err(CatalogFailure::AuthenticationUnavailable)
            ));
        }
        let auth = Authentication::read(None, vec![]).await.unwrap();
        let home = auth.home().await.unwrap();
        assert!(!home.path().join("auth.json").exists());
    }
}
