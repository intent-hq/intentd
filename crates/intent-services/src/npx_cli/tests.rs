//! Public service regressions with an isolated, offline node/npx toolchain.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_store::Store;
use intentd_test_support::GuardedChild;
use serde_json::{json, Value};

use crate::{Services, SettingsRegistry};

const ADAPTER: &str = r"import readline from 'node:readline';
const send = (o) => process.stdout.write(JSON.stringify(o) + '\n');
const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', (line) => {
  const msg = JSON.parse(line);
  if (msg.method === 'initialize') return send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: 1 } });
  if (msg.method === 'session/new') return send({ jsonrpc: '2.0', id: msg.id, result: {
    sessionId: 'npm-guard-fixture',
    configOptions: [{ id: 'model', category: 'model', type: 'select', currentValue: 'fixture-model', options: [{ value: 'fixture-model', name: 'Fixture model' }] }]
  }});
  if (msg.method === 'session/set_config_option') return send({ jsonrpc: '2.0', id: msg.id, result: {} });
  if (msg.method === 'session/prompt') {
    send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId: 'npm-guard-fixture', update: {
      sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'fixture reply' }
    }}});
    send({ jsonrpc: '2.0', id: msg.id, result: { stopReason: 'end_turn' } });
  }
});
";

const NPX: &str = r#"#!/bin/sh
if [ "$1" = --version ]; then
  printf 'probe\n' >> "$INTENT_TEST_NPX_ROOT/probes"
  if [ "$INTENT_TEST_NPX_VERSION" = failed ]; then exit 9; fi
  printf '%s\n' "$INTENT_TEST_NPX_VERSION"
  exit 0
fi
printf '%s\n' "$*" >> "$INTENT_TEST_NPX_ROOT/packages"
exec "$INTENT_TEST_REAL_NODE" "$INTENT_TEST_NPX_ROOT/adapter.mjs" "$@"
"#;

fn executable(path: &Path, script: &str) {
    std::fs::write(path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Only the child receives the fixture PATH and private home; parallel tests
/// and the daemon host retain their own environment and discovery caches.
fn in_subprocess(test: &str, version: &str) -> bool {
    let name = format!("npx_cli::tests::{test}");
    if std::env::var("INTENT_TEST_NPX_CASE").as_deref() == Ok(&name) {
        return false;
    }
    let node = intent_providers::resolve_on_path("node").expect("test host has Node.js");
    let tmp = crate::test_support::test_tempdir("intent-npx-prerequisite-");
    let root = tmp.path();
    let bin = root.join("bin");
    std::fs::create_dir(&bin).unwrap();
    std::fs::create_dir(root.join("codex-home")).unwrap();
    std::fs::write(root.join("adapter.mjs"), ADAPTER).unwrap();
    executable(
        &bin.join("node"),
        "#!/bin/sh\nexec \"$INTENT_TEST_REAL_NODE\" \"$@\"\n",
    );
    executable(&bin.join("npx"), NPX);
    let forbidden = "#!/bin/sh\nprintf 'native\\n' >> \"$INTENT_TEST_NPX_ROOT/native\"\nexit 99\n";
    executable(&bin.join("codex-acp"), forbidden);
    executable(&root.join("custom-codex-acp"), forbidden);
    executable(
        &root.join("direct-adapter"),
        "#!/bin/sh\nprintf 'direct\\n' >> \"$INTENT_TEST_NPX_ROOT/direct\"\nexec \"$INTENT_TEST_REAL_NODE\" \"$INTENT_TEST_NPX_ROOT/adapter.mjs\" \"$@\"\n",
    );
    let log_path = root.join("test.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env("INTENT_TEST_NPX_CASE", &name)
        .env("INTENT_TEST_NPX_VERSION", version)
        .env("INTENT_TEST_NPX_ROOT", root)
        .env("INTENT_TEST_REAL_NODE", node)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("CODEX_HOME", root.join("codex-home"))
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    let mut child = GuardedChild::spawn(&mut cmd).expect("start isolated service test");
    let status = child
        .wait_with_timeout(Duration::from_secs(120))
        .expect("wait for isolated service test")
        .expect("isolated service test timed out");
    assert!(
        status.success(),
        "{name}: {}",
        std::fs::read_to_string(log_path).unwrap()
    );
    true
}

fn root() -> PathBuf {
    PathBuf::from(std::env::var_os("INTENT_TEST_NPX_ROOT").unwrap())
}

fn lines(name: &str) -> Vec<String> {
    std::fs::read_to_string(root().join(name))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn paths(provider: &str) -> HashMap<String, String> {
    let adapter = if provider == "codex" {
        "custom-codex-acp"
    } else {
        "direct-adapter"
    };
    HashMap::from([(
        provider.to_string(),
        root().join(adapter).to_string_lossy().into_owned(),
    )])
}

async fn services(provider: &str, adapter_paths: &HashMap<String, String>) -> Services {
    let registry = Arc::new(SettingsRegistry::load(root().join("settings.toml")).unwrap());
    registry
        .apply(&[
            ("model.defaultProvider".to_string(), json!(provider)),
            ("providers.paths".to_string(), json!(adapter_paths)),
        ])
        .unwrap();
    Services::new(Store::open(&root().join("store.db")).await.unwrap())
        .with_settings_registry(registry)
}

async fn completion(services: &Services) -> intent_core::Result<Value> {
    services
        .agent_complete_once("hello".to_string(), None, None, None, None, Some(5_000))
        .await
}

fn assert_stale(reason: &str) {
    assert!(reason.contains("6.14.18"), "{reason}");
    assert!(
        reason.contains(intent_providers::NPX_NPM_REQUIREMENT),
        "{reason}"
    );
    assert!(
        reason.contains(&root().join("bin/npx").to_string_lossy().to_string()),
        "{reason}"
    );
    assert!(
        reason.contains(&root().join("bin/node").to_string_lossy().to_string()),
        "{reason}"
    );
    assert!(reason.contains("PATH"), "{reason}");
    assert_eq!(lines("probes").len(), 1);
    assert!(
        lines("packages").is_empty(),
        "package launch tripwire: {:?}",
        lines("packages")
    );
    assert!(
        lines("native").is_empty(),
        "native/custom adapter must not run"
    );
}

#[tokio::test]
async fn stale_npx_completion_is_unavailable_without_package_launch() {
    if in_subprocess(
        "stale_npx_completion_is_unavailable_without_package_launch",
        "6.14.18",
    ) {
        return;
    }
    let result = completion(&services("claude-code", &HashMap::new()).await)
        .await
        .unwrap();
    eprintln!(
        "completion={result}; package launches={:?}",
        lines("packages")
    );
    assert_eq!(result["available"], false);
    assert_stale(result["reason"].as_str().unwrap());
}

#[tokio::test]
async fn stale_npx_models_keep_static_warning_without_package_launch() {
    if in_subprocess(
        "stale_npx_models_keep_static_warning_without_package_launch",
        "6.14.18",
    ) {
        return;
    }
    let result = services("claude-code", &HashMap::new())
        .await
        .models_list(Some("claude-code".to_string()), true)
        .await
        .unwrap();
    eprintln!("models={result}; package launches={:?}", lines("packages"));
    assert_eq!(result["providerId"], "claude-code");
    assert_eq!(result["source"], "static");
    assert_eq!(result["models"], json!([]));
    assert_stale(result["warning"].as_str().unwrap());
}

#[tokio::test]
async fn stale_npx_test_prompt_is_not_installed_without_package_launch() {
    if in_subprocess(
        "stale_npx_test_prompt_is_not_installed_without_package_launch",
        "6.14.18",
    ) {
        return;
    }
    let result = crate::provider_test_prompt::provider_test_prompt(
        "claude-code",
        None,
        &HashMap::new(),
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "test prompt={result}; package launches={:?}",
        lines("packages")
    );
    assert_eq!(result["ok"], false);
    assert_eq!(result["reason"], "not-installed");
    assert_stale(result["message"].as_str().unwrap());
}

async fn assert_public_launches_succeed() {
    let services = services("claude-code", &HashMap::new()).await;
    assert_eq!(
        completion(&services).await.unwrap()["text"],
        "fixture reply"
    );
    let models = services
        .models_list(Some("claude-code".to_string()), true)
        .await
        .unwrap();
    assert_eq!(models["source"], "claude-code", "{models}");
    assert!(
        models["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == "fixture-model"),
        "{models}"
    );
    assert_eq!(
        crate::provider_test_prompt::provider_test_prompt(
            "claude-code",
            None,
            &HashMap::new(),
            None
        )
        .await
        .unwrap(),
        json!({"ok": true})
    );
    assert_eq!(
        completion(&services).await.unwrap()["text"],
        "fixture reply"
    );
    assert_eq!(
        lines("probes").len(),
        1,
        "all entrypoints share the cached verdict"
    );
    assert_eq!(
        lines("packages"),
        vec![
            format!(
                "--workspaces=false -y {}",
                intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE
            );
            4
        ]
    );
    assert!(lines("native").is_empty());
}

#[tokio::test]
async fn minimum_npx_version_runs_all_public_paths_with_one_probe() {
    if in_subprocess(
        "minimum_npx_version_runs_all_public_paths_with_one_probe",
        "7.0.0",
    ) {
        return;
    }
    assert_public_launches_succeed().await;
}

#[tokio::test]
async fn modern_npx_runs_all_public_paths_with_one_probe() {
    if in_subprocess("modern_npx_runs_all_public_paths_with_one_probe", "11.13.0") {
        return;
    }
    assert_public_launches_succeed().await;
}

#[tokio::test]
async fn failed_npx_probe_remains_permissive_on_all_public_paths() {
    if in_subprocess(
        "failed_npx_probe_remains_permissive_on_all_public_paths",
        "failed",
    ) {
        return;
    }
    assert_public_launches_succeed().await;
}

#[tokio::test]
async fn unparseable_npx_probe_remains_permissive_on_all_public_paths() {
    if in_subprocess(
        "unparseable_npx_probe_remains_permissive_on_all_public_paths",
        "unrecognized",
    ) {
        return;
    }
    assert_public_launches_succeed().await;
}

#[tokio::test]
async fn direct_adapter_skips_stale_npx_probe_and_package_launch() {
    if in_subprocess(
        "direct_adapter_skips_stale_npx_probe_and_package_launch",
        "6.14.18",
    ) {
        return;
    }
    let services = services("claude-code", &paths("claude-code")).await;
    assert_eq!(
        completion(&services).await.unwrap()["text"],
        "fixture reply"
    );
    assert_eq!(
        crate::provider_test_prompt::provider_test_prompt(
            "claude-code",
            None,
            &paths("claude-code"),
            None
        )
        .await
        .unwrap(),
        json!({"ok": true})
    );
    assert_eq!(lines("direct").len(), 2);
    assert!(
        lines("probes").is_empty(),
        "direct adapters must not run npx --version"
    );
    assert!(lines("packages").is_empty());
    assert!(lines("native").is_empty());
}
