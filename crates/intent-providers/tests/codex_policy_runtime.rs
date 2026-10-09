//! Opt-in compatibility check against real Codex and the pinned ACP adapter.
//! No credentials, global config, or remote model requests are needed.
use intent_providers::CODEX_SUBAGENT_POLICY_CONFIG;
use serde_json::{json, Value};

#[test]
fn codex_policy_uses_compatible_feature_flags() {
    let policy: Value = serde_json::from_str(CODEX_SUBAGENT_POLICY_CONFIG).unwrap();
    // Older Codex treats unknown agents keys as role declarations, so a scalar
    // agents.enabled causes AgentRoleToml deserialization to fail (#6982).
    assert!(policy.get("agents").is_none());
    for feature in ["multi_agent", "multi_agent_v2"] {
        assert_eq!(policy["features"][feature], json!(false), "{feature}");
    }
}

#[test]
#[ignore = "requires INTENT_CODEX_TEST_BINARY and INTENT_CODEX_TEST_ADAPTER (pinned adapter JS entrypoint)"]
fn codex_policy_real_runtime() {
    let binary = std::env::var("INTENT_CODEX_TEST_BINARY").expect("set the Codex binary path");
    let adapter = std::env::var("INTENT_CODEX_TEST_ADAPTER").expect("set the adapter JS path");
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/codex_policy_runtime.py"
        ))
        .args([
            binary.as_str(),
            adapter.as_str(),
            CODEX_SUBAGENT_POLICY_CONFIG,
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .output()
        .expect("run isolated runtime regression");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
}
