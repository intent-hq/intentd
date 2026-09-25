//! Exercise the real doctor CLI with isolated, prompt-free ACP/npm fixtures.
//! Linux execution is evidence for Linux only; native ownership has its own tests.

#![cfg(any(target_os = "linux", target_os = "macos"))]

mod common;

use std::fmt::Write;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use intent_core::settings_file::SettingsFile;
use intent_providers::discover::ProviderLaunch;
use intent_services::codex_diagnostics::CodexLaunch;
use intentd_test_support::GuardedChild;
use serde_json::{json, Value};

const SELECTION: &str = "INTENTD_DOCTOR_FIXTURE_SELECTION";
const CANARIES: &[&str] = &[
    "credential-canary",
    "account-canary",
    "user@example.invalid",
    "sk-fixture-key",
    "session-canary",
    "private-cursor",
];

fn executable(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

struct Fixture {
    root: tempfile::TempDir,
    adapter: PathBuf,
    runtime: PathBuf,
    selection: &'static str,
}

impl Fixture {
    fn new(selection: &'static str, config: &Value) -> Self {
        let root = common::test_tempdir("doctor-codex");
        let bin = root.path().join("bin");
        fs::create_dir(&bin).unwrap();
        #[cfg(target_os = "linux")]
        {
            let node =
                fs::canonicalize(intent_providers::find_node().expect("Node required")).unwrap();
            // A symlink would make production pair this Node with the host's real npx.
            fs::copy(&node, bin.join("node")).unwrap();
        }
        #[cfg(target_os = "macos")]
        executable(&bin.join("node"), &format!("#!/bin/sh\nprintf invoked > '{}'\nprintf 'credential-canary'\nprintf 'account-canary' >&2\nexit 93\n", root.path().join("node-ran").display()));
        let adapter = root
            .path()
            .join("node_modules/@agentclientprotocol/codex-acp/dist/index.js");
        let runtime = root.path().join("node_modules/@openai/codex/bin/codex.js");
        for (path, role) in [(&adapter, "acp"), (&runtime, "raw")] {
            executable(
                path,
                &format!(
                    "#!/usr/bin/env node\nconst role={};const fixture={};\n{}\n{}",
                    json!(role),
                    json!(root.path()),
                    include_str!("fixtures/codex-doctor-version.cjs"),
                    include_str!("../../intent-services/src/codex_diagnostics/catalog_fixture.cjs")
                ),
            );
        }
        let pin = intent_providers::config::CODEX_ACP_NPX_PACKAGE
            .rsplit_once('@')
            .unwrap()
            .1;
        for (path, name, version, bin_name, entry) in [
            (
                &adapter,
                "@agentclientprotocol/codex-acp",
                pin,
                "codex-acp",
                "dist/index.js",
            ),
            (
                &runtime,
                "@openai/codex",
                "0.333.4",
                "codex",
                "bin/codex.js",
            ),
        ] {
            fs::write(
                path.parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("package.json"),
                json!({"name":name,"version":version,"bin":{bin_name:entry}}).to_string(),
            )
            .unwrap();
        }
        executable(
            &bin.join("npx"),
            &format!(
                "#!/usr/bin/env node\nconst fixture={};\n{}",
                json!(root.path()),
                include_str!("fixtures/codex-doctor-npx.cjs")
            ),
        );
        executable(
            &bin.join("codex"),
            &format!(
                "#!/usr/bin/env node\nrequire('fs').writeFileSync({},'wrong');console.log('codex-cli 99.99.99');",
                json!(root.path().join("path-codex-ran"))
            ),
        );
        let mut settings = SettingsFile::default();
        // Keep the pre-existing non-Codex provider probes off installed accounts.
        for provider in intent_providers::ACP_PROVIDERS {
            // The mock provider's command is Node itself; it is gated off by
            // env_clear and must not replace the fixture's Node executable.
            if provider.id != "codex" && provider.id != "mock" {
                let stub = bin.join(provider.command);
                executable(&stub, "#!/bin/sh\nexit 1\n");
                settings
                    .providers
                    .paths
                    .insert(provider.id.into(), stub.display().to_string());
            }
        }
        executable(&bin.join("pi"), "#!/bin/sh\nexit 1\n");
        executable(&bin.join("gh"), "#!/bin/sh\nexit 1\n");
        match selection {
            "override" => {
                settings
                    .providers
                    .paths
                    .insert("codex".into(), adapter.display().to_string());
            }
            "discovered" => symlink(&adapter, bin.join("codex-acp")).unwrap(),
            "managed" => {}
            _ => panic!("unknown fixture selection"),
        }
        let mut config_text = String::from("[providers.paths]\n");
        for (key, value) in &settings.providers.paths {
            writeln!(config_text, "{} = {}", json!(key), json!(value)).unwrap();
        }
        fs::write(root.path().join("config.toml"), config_text).unwrap();
        fs::create_dir(root.path().join("user")).unwrap();
        fs::write(
            root.path().join("user/auth.json"),
            r#"{"tokens":{"access_token":"credential-canary","account_id":"account-canary"}}"#,
        )
        .unwrap();
        fs::write(
            root.path().join("user/config.toml"),
            "[mcp_servers.bad]\ncommand = 'must-not-run'\n",
        )
        .unwrap();
        fs::write(
            root.path().join("user/models_cache.json"),
            "unchanged-cache",
        )
        .unwrap();
        fs::write(root.path().join("fixture.json"), config.to_string()).unwrap();
        fs::write(root.path().join("events.jsonl"), "").unwrap();
        Self {
            root,
            adapter,
            runtime,
            selection,
        }
    }

    fn command(&self, program: &Path) -> Command {
        let mut command = Command::new(program);
        command
            .env_clear()
            .env("PATH", self.root.path().join("bin"))
            .env("HOME", self.root.path())
            .env("SHELL", "/nonexistent-doctor-fixture-shell")
            .env("CODEX_HOME", self.root.path().join("user"))
            .env("OPENAI_API_KEY", "sk-fixture-key")
            .env("INTENTD_CONFIG", self.root.path().join("config.toml"))
            .env("INTENTD_DATA_DIR", self.root.path().join("data"))
            .env(
                "INTENTD_WORKSPACES_DIR",
                self.root.path().join("workspaces"),
            )
            .env(
                "INTENTD_SECRETS_FILE",
                self.root.path().join("secrets.json"),
            )
            .env("INTENTD_TCP_PORT", "0")
            .env("NODE_DISABLE_COMPILE_CACHE", "1")
            .env("TMPDIR", self.root.path())
            .current_dir(self.root.path());
        command
    }

    fn run_command(&self, mut command: Command) -> (bool, String) {
        let stdout_path = self.root.path().join("stdout.log");
        let stderr_path = self.root.path().join("stderr.log");
        command
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout_path).unwrap())
            .stderr(fs::File::create(&stderr_path).unwrap());
        let mut child = GuardedChild::spawn(&mut command).unwrap();
        let status = child
            .wait_with_timeout(common::test_timeout(Duration::from_secs(150)))
            .unwrap()
            .expect("doctor command exceeded its functional-test deadline");
        let stdout = fs::read_to_string(stdout_path).unwrap();
        let stderr = fs::read_to_string(stderr_path).unwrap();
        for canary in CANARIES {
            assert!(
                !stdout.contains(canary),
                "stdout leaked a private fixture value"
            );
            assert!(
                !stderr.contains(canary),
                "stderr leaked a private fixture value"
            );
        }
        assert!(!stdout.contains('\u{1b}'));
        assert!(!stderr.contains('\u{1b}'));
        (status.success(), stdout)
    }

    fn doctor_command(&self, live: bool) -> Command {
        // Check production discovery before any adapter could execute. If a host
        // install outranks a fixture, fail rather than touching that installation.
        let mut preflight = self.command(&std::env::current_exe().unwrap());
        preflight
            .args(["--exact", "fixture_launch_is_selected", "--nocapture"])
            .env(SELECTION, self.selection);
        assert!(
            self.run_command(preflight).0,
            "fixture selection preflight failed"
        );
        let mut command = self.command(Path::new(env!("CARGO_BIN_EXE_intentd")));
        command.arg("doctor");
        if live {
            command.arg("--codex-models");
        }
        command
    }

    fn run(&self, live: bool) -> String {
        let (success, stdout) = self.run_command(self.doctor_command(live));
        assert!(
            success,
            "provider diagnostics must remain advisory: {stdout}"
        );
        self.assert_clean();
        println!("{stdout}");
        stdout
    }

    fn events(&self) -> Vec<Value> {
        fs::read_to_string(self.root.path().join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn assert_clean(&self) {
        assert!(!self.root.path().join("path-codex-ran").exists());
        assert!(!self.root.path().join("mcp-launched").exists());
        assert!(!self.root.path().join("opaque-adapter-ran").exists());
        assert!(!self.root.path().join("node-ran").exists());
        assert!(fs::read_dir(self.root.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("intentd-codex-")));
        for event in self.events() {
            assert!(event.get("isolationFailure").is_none());
            assert!(event.get("unexpectedMethod").is_none());
            if let Some(home) = event.get("home").and_then(Value::as_str) {
                assert!(!Path::new(home).exists(), "probe home must be removed");
            }
            if let Some(method) = event.get("method").and_then(Value::as_str) {
                assert!([
                    "initialize",
                    "session/new",
                    "initialized",
                    "account/read",
                    "model/list"
                ]
                .contains(&method));
            }
        }
        assert_eq!(
            fs::read_to_string(self.root.path().join("user/models_cache.json")).unwrap(),
            "unchanged-cache"
        );
        assert!(
            fs::read_to_string(self.root.path().join("user/config.toml"))
                .unwrap()
                .contains("mcp_servers")
        );
    }
}

#[test]
fn fixture_launch_is_selected() {
    let Ok(expected) = std::env::var(SELECTION) else {
        return;
    };
    let config = PathBuf::from(std::env::var_os("INTENTD_CONFIG").unwrap());
    let settings = SettingsFile::load_or_init(&config).unwrap();
    let launch = CodexLaunch::discover(&settings);
    // macOS temporary roots can be aliases such as /var -> /private/var.
    // Compare file identity after resolution, while still requiring the exact
    // fixture executable before doctor can run any provider check.
    let root = fs::canonicalize(config.parent().unwrap()).unwrap();
    match (expected.as_str(), launch.selection()) {
        ("managed", ProviderLaunch::Managed { npx, .. }) => {
            assert_eq!(fs::canonicalize(npx).unwrap(), root.join("bin/npx"));
        }
        ("override" | "discovered", ProviderLaunch::Local(binary)) => {
            assert_eq!(
                fs::canonicalize(&binary.path).unwrap(),
                root.join("node_modules/@agentclientprotocol/codex-acp/dist/index.js")
            );
        }
        _ => panic!("host provider resolution escaped the doctor fixture"),
    }
}

#[test]
fn default_managed_reports_configuration_without_materializing_or_querying() {
    let fixture = Fixture::new("managed", &json!({}));
    let stdout = fixture.run(false);
    assert!(stdout.contains("selected adapter: managed npm package"));
    assert!(stdout.contains(intent_providers::config::CODEX_ACP_NPX_PACKAGE));
    assert!(stdout.contains("configured managed package (not a measured version)"));
    assert!(stdout.contains("no package was installed"));
    if cfg!(target_os = "macos") {
        assert!(stdout.contains("version and fresh catalog probes are unsupported"));
    } else {
        assert!(stdout.contains("--codex-models"));
    }
    assert!(fixture.events().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn default_local_measures_selected_dependency_and_ignores_path_codex() {
    let fixture = Fixture::new("override", &json!({}));
    let stdout = fixture.run(false);
    assert!(stdout.contains("selected adapter: providers.paths override"));
    assert!(stdout.contains("measured adapter version: 1.13.1"));
    assert!(stdout.contains("measured runtime version: 0.333.4"));
    assert!(stdout.contains("runtime source: selected adapter dependency"));
    assert!(!stdout.contains("99.99.99"));
    let events = fixture.events();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event["version"] == true));
}

#[test]
fn default_local_discovery_is_distinct_from_an_override() {
    let fixture = Fixture::new("discovered", &json!({}));
    assert!(fixture
        .run(false)
        .contains("selected adapter: local discovery"));
}

#[cfg(target_os = "linux")]
#[test]
fn local_runtime_override_follows_production_selection() {
    let fixture = Fixture::new("override", &json!({}));
    let selected = fixture.root.path().join("override/bin/codex.js");
    executable(
        &selected,
        "#!/usr/bin/env node\nconsole.log('codex-cli 0.444.5');\n",
    );
    fs::write(
        fixture.root.path().join("override/package.json"),
        r#"{"name":"@openai/codex","version":"0.444.5","bin":{"codex":"bin/codex.js"}}"#,
    )
    .unwrap();
    let mut command = fixture.doctor_command(false);
    command.env("CODEX_PATH", &selected);
    let (success, stdout) = fixture.run_command(command);
    assert!(success);
    assert!(stdout.contains("runtime source: effective CODEX_PATH override"));
    assert!(stdout.contains("measured runtime version: 0.444.5"));
    assert!(!stdout.contains("measured runtime version: 0.333.4"));
    fixture.assert_clean();
}

#[test]
fn default_opaque_adapter_is_unknown_without_execution() {
    let fixture = Fixture::new("override", &json!({}));
    let marker = fixture.root.path().join("opaque-adapter-ran");
    executable(
        &fixture.adapter,
        &format!(
            "#!/bin/sh\nprintf invoked > '{}'\nexit 93\n",
            marker.to_str().unwrap().replace('\'', "'\\''")
        ),
    );
    let stdout = fixture.run(false);
    assert!(stdout.contains(if cfg!(target_os = "macos") {
        "process probes are unsupported on macOS"
    } else {
        "wrapper or native adapter cannot be inspected"
    }));
    assert!(fixture.events().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn live_managed_uses_resolved_package_and_reports_original_catalog_fields() {
    let fixture = Fixture::new("managed", &json!({}));
    let mut command = fixture.doctor_command(true);
    command.env("CODEX_PATH", fixture.root.path().join("bin/codex"));
    let (success, stdout) = fixture.run_command(command);
    assert!(success);
    fixture.assert_clean();
    assert!(stdout.contains("measured runtime version: 0.333.4"));
    assert!(stdout.contains("ACP catalog: advertised"));
    assert!(stdout.contains("raw runtime catalog: advertised"));
    assert!(stdout.contains("fixture-model-high"));
    assert!(stdout.contains("model alias: fixture-alias"));
    assert!(stdout.contains("hidden: true"));
    assert!(stdout.contains("may be synthesized by the adapter"));
    assert!(stdout.contains("not an entitlement check"));
    assert!(stdout.contains("fixture-model: ID observed in both catalogs"));
    assert!(stdout.contains("fixture-model-high: ID observed only in ACP"));
    assert!(stdout.contains("hidden-model: ID observed only in the selected runtime catalog"));
    assert!(fixture.events().iter().any(|event| event["role"] == "npx"));
    println!("{stdout}");
}

#[cfg(target_os = "linux")]
#[test]
fn missing_runtime_keeps_acp_success_and_an_advisory_raw_failure() {
    let fixture = Fixture::new("override", &json!({}));
    fs::remove_file(&fixture.runtime).unwrap();
    let stdout = fixture.run(true);
    assert!(stdout
        .contains("runtime version: unknown (selected adapter's runtime could not be resolved)"));
    assert!(stdout.contains("ACP catalog: advertised"));
    assert!(stdout.contains(
        "raw runtime catalog: unavailable (selected adapter's runtime could not be verified)"
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn raw_error_preserves_acp_observations_and_withholds_child_errors() {
    let fixture = Fixture::new("override", &json!({"raw":"rpcError"}));
    let stdout = fixture.run(true);
    assert!(stdout.contains("ACP catalog: advertised"));
    assert!(stdout
        .contains("raw runtime catalog: unavailable (provider rejected the diagnostic request)"));
    assert!(stdout.contains("comparison unavailable"));
}

#[cfg(target_os = "linux")]
#[test]
fn acp_error_preserves_raw_observations() {
    let fixture = Fixture::new("override", &json!({"acp":"unsupported"}));
    let stdout = fixture.run(true);
    assert!(stdout.contains(
        "ACP catalog: unavailable (provider does not support this diagnostic conversation)"
    ));
    assert!(stdout.contains("raw runtime catalog: advertised"));
}

#[cfg(target_os = "linux")]
#[test]
fn unavailable_authentication_is_separate_from_an_empty_catalog() {
    let fixture = Fixture::new("override", &json!({"acp":"auth","raw":"auth"}));
    let stdout = fixture.run(true);
    assert_eq!(
        stdout
            .matches("authentication is unavailable for this probe")
            .count(),
        2
    );
    assert!(!stdout.contains("catalog: advertised"));
}

#[cfg(target_os = "linux")]
#[test]
fn unreadable_authentication_does_not_start_live_probes() {
    let fixture = Fixture::new("managed", &json!({}));
    fs::write(
        fixture.root.path().join("user/auth.json"),
        "credential-canary",
    )
    .unwrap();
    let stdout = fixture.run(true);
    assert_eq!(
        stdout
            .matches("authentication is unavailable for this probe")
            .count(),
        2
    );
    assert!(fixture.events().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn advertised_empty_catalogs_are_observations_not_account_restrictions() {
    let fixture = Fixture::new(
        "override",
        &json!({
            "session":{"sessionId":"session-canary","models":{"availableModels":[]}},
            "pages":[{"data":[],"nextCursor":null}]
        }),
    );
    let stdout = fixture.run(true);
    assert_eq!(
        stdout.matches("catalog: advertised, 0 model rows").count(),
        2
    );
    assert!(stdout.contains("Absence does not establish an account restriction"));
    assert!(!stdout.contains("authentication is unavailable"));
}

#[cfg(target_os = "linux")]
#[test]
fn missing_acp_advertisement_cannot_claim_raw_only_membership() {
    let fixture = Fixture::new(
        "override",
        &json!({"session":{"sessionId":"session-canary"}}),
    );
    let stdout = fixture.run(true);
    assert!(stdout.contains("ACP catalog: not advertised"));
    assert!(stdout.contains("comparison unavailable"));
    assert!(!stdout.contains("ID observed only in the selected runtime catalog"));
}

#[cfg(target_os = "linux")]
#[test]
fn sensitive_ids_and_metadata_are_withheld_from_both_output_streams() {
    let fixture = Fixture::new(
        "override",
        &json!({
            "session":{"sessionId":"session-canary","models":{"availableModels":[
                {"modelId":"credential-canary"},{"modelId":"user@example.invalid"},
                {"modelId":"fixture-model","name":"account-canary"}]}},
            "pages":[{"data":[{"id":"account-canary"},{"id":"fixture-model","model":"sk-fixture-key"}],"nextCursor":null}]
        }),
    );
    let stdout = fixture.run(true);
    assert!(stdout.contains("withheld model IDs"));
    assert!(stdout.contains("comparison unavailable"));
}

#[cfg(target_os = "linux")]
#[test]
fn local_version_timeout_and_invalid_output_are_safe_unknowns() {
    let fixture = Fixture::new(
        "override",
        &json!({"versionRaw":"timeout","versionAcp":"invalid"}),
    );
    let stdout = fixture.run(false);
    assert!(stdout.contains("adapter version: unknown (local output was not a recognized version)"));
    assert!(stdout.contains("runtime version: unknown (local check exceeded its deadline)"));
    assert!(fixture
        .events()
        .iter()
        .all(|event| event["version"] == true));
}

#[cfg(target_os = "linux")]
#[test]
fn live_catalog_deadline_is_advisory_and_preserves_the_other_catalog() {
    let fixture = Fixture::new("override", &json!({"raw":"timeout"}));
    let stdout = fixture.run(true);
    assert!(stdout.contains("ACP catalog: advertised"));
    assert!(
        stdout.contains("raw runtime catalog: unavailable (catalog probe exceeded its deadline)")
    );
}

#[test]
fn daemon_health_failure_still_fails_with_catalog_diagnostics_enabled() {
    let fixture = Fixture::new("managed", &json!({}));
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = fixture.doctor_command(true);
    command.env(
        "INTENTD_TCP_PORT",
        port.local_addr().unwrap().port().to_string(),
    );
    let (success, stdout) = fixture.run_command(command);
    assert!(!success);
    assert!(stdout.contains("not bindable"));
    fixture.assert_clean();
}

#[test]
fn doctor_help_discloses_opt_in_and_its_limits() {
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["doctor", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "--codex-models",
        "download",
        "30",
        "authentication",
        "prompts",
        "advisory",
        "macOS",
        "metadata",
        "unsupported",
    ] {
        assert!(stdout.contains(expected), "missing help text: {expected}");
    }
}

#[cfg(target_os = "macos")]
#[test]
fn macos_local_selection_and_metadata_never_execute_provider_or_path_codex() {
    for selection in ["override", "discovered"] {
        let fixture = Fixture::new(selection, &json!({}));
        let stdout = fixture.run(false);
        let pin = intent_providers::config::CODEX_ACP_NPX_PACKAGE
            .rsplit_once('@')
            .unwrap()
            .1;
        assert!(stdout.contains(&format!(
            "adapter package version (metadata, not measured): {pin}"
        )));
        assert!(stdout.contains(if selection == "override" {
            "providers.paths override"
        } else {
            "local discovery"
        }));
        assert!(stdout.contains("process probes are unsupported on macOS"));
        assert!(stdout.contains("runtime source: unknown"));
        assert!(!stdout.contains("[ok] measured"));
        assert!(!stdout.contains("99.99.99"));
        assert!(fixture.events().is_empty());
    }
}

#[cfg(target_os = "macos")]
#[test]
fn macos_catalog_rejection_precedes_npm_authentication_and_probe_state() {
    for selection in ["managed", "override"] {
        let fixture = Fixture::new(selection, &json!({}));
        // A FIFO is invalid auth material. Capability rejection must take
        // precedence over authentication failure without provider/home setup.
        let auth = fixture.root.path().join("user/auth.json");
        fs::remove_file(&auth).unwrap();
        nix::unistd::mkfifo(
            &auth,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .unwrap();
        let mut command = fixture.doctor_command(true);
        command.env("CODEX_PATH", &fixture.runtime);
        let (success, stdout) = fixture.run_command(command);
        assert!(success);
        assert_eq!(
            stdout
                .matches("catalog: unavailable (process probes are unsupported on macOS")
                .count(),
            2
        );
        assert!(stdout.contains("Catalog comparison is inconclusive."));
        assert!(!stdout.contains("catalog: advertised"));
        assert!(!stdout.contains("authentication is unavailable"));
        assert!(!stdout.contains("fresh catalogs: checking"));
        assert!(fixture.events().is_empty());
        fixture.assert_clean();
        println!("{stdout}");
    }
}

#[cfg(target_os = "macos")]
#[test]
fn macos_malformed_metadata_is_not_reported_as_a_version_or_an_error_payload() {
    let fixture = Fixture::new("override", &json!({}));
    fs::write(fixture.adapter.parent().unwrap().parent().unwrap().join("package.json"),
        json!({"name":"@agentclientprotocol/codex-acp", "version":"credential-canary", "bin":{"codex-acp":"dist/index.js"}}).to_string()).unwrap();
    let stdout = fixture.run(false);
    assert!(!stdout.contains("adapter package version"));
    assert!(!stdout.contains("[ok] measured"));
    assert!(stdout.contains("process probes are unsupported on macOS"));
    assert!(fixture.events().is_empty());
}
