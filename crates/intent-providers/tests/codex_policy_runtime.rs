//! Opt-in compatibility check against real Codex and the pinned ACP adapter.
//! No credentials, global config, or remote model requests are needed.
use intent_providers::CODEX_SUBAGENT_POLICY_CONFIG;
use serde_json::{json, Value};

#[test]
fn codex_policy_denies_agents_as_well_as_feature_flags() {
    let policy: Value = serde_json::from_str(CODEX_SUBAGENT_POLICY_CONFIG).unwrap();
    // Supported runtimes prefer model metadata over feature flags. This
    // explicit denial is required; the minimum gate excludes old parsers.
    assert_eq!(policy["agents"]["enabled"], json!(false));
    for feature in ["multi_agent", "multi_agent_v2"] {
        assert_eq!(policy["features"][feature], json!(false), "{feature}");
    }
}

#[test]
#[ignore = "requires INTENT_CODEX_TEST_BINARY and INTENT_CODEX_TEST_ADAPTER (pinned adapter JS entrypoint)"]
fn codex_policy_real_runtime() {
    let binary = std::env::var("INTENT_CODEX_TEST_BINARY").expect("set the Codex binary path");
    let adapter = std::env::var("INTENT_CODEX_TEST_ADAPTER").expect("set the adapter JS path");
    for metadata in ["v1", "v2", "none"] {
        let mut command = std::process::Command::new("python3");
        command
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/codex_policy_runtime.py"
            ))
            .args([
                binary.as_str(),
                adapter.as_str(),
                CODEX_SUBAGENT_POLICY_CONFIG,
                metadata,
            ])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap());
        if let Some(daemon) = std::env::var_os("INTENT_CODEX_TEST_DAEMON") {
            command.env("INTENT_CODEX_TEST_DAEMON", daemon);
        }
        let output = command.output().expect("run isolated runtime regression");
        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }
}
