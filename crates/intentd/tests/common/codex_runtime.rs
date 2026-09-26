//! Process-local Node fixtures for Codex tests. No npm or model access.

use std::path::Path;

/// Route the bundled adapter launch through the deterministic ACP fixture.
pub fn install(data_dir: &Path, script: &str) -> Vec<(String, String)> {
    use std::os::unix::fs::PermissionsExt;

    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let bin = data_dir.join("codex-toolchain");
    std::fs::create_dir_all(&bin).expect("create fake toolchain");
    for (name, body) in [
        (
            "node",
            "#!/bin/sh\ncase \"$1\" in\n*/codex-acp.mjs) exec \"$MOCK_AGENT_NODE\" \"$MOCK_AGENT_SCRIPT_PATH\" \"$@\";;\n*) exec \"$MOCK_AGENT_NODE\" \"$@\";;\nesac\n",
        ),
        ("codex", "#!/bin/sh\nexit 99\n"),
        ("npx", "#!/bin/sh\nexit 99\n"),
    ] {
        let path = bin.join(name);
        std::fs::write(&path, body).expect("write fake toolchain executable");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake toolchain executable");
    }
    vec![
        ("PATH".into(), format!("{}:/usr/bin:/bin", bin.display())),
        (
            "MOCK_AGENT_NODE".into(),
            node.to_string_lossy().into_owned(),
        ),
        ("MOCK_AGENT_SCRIPT_PATH".into(), script.into()),
    ]
}

/// Run an in-process WSS server test in its own test process before
/// installing the fake toolchain environment. Parallel tests keep their
/// original PATH and provider discovery caches.
pub fn in_subprocess(test_name: &str) -> bool {
    use intentd_test_support::GuardedChild;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    if std::env::var("INTENTD_CODEX_NPX_TEST").as_deref() == Ok(test_name) {
        return false;
    }
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test_name}: node not on PATH");
        return true;
    }
    let dir = super::test_tempdir("itd-codex-npx-process-");
    let env = install(dir.path(), "unused-by-selectable-node");
    std::fs::write(
        dir.path().join("codex-toolchain/node"),
        "#!/bin/sh\ncase \"$1\" in\n*/codex-acp.mjs) IFS= read -r adapter < \"$MOCK_CODEX_ADAPTER_FILE\"; exec \"$adapter\" \"$@\";;\n*) exec \"$MOCK_AGENT_NODE\" \"$@\";;\nesac\n",
    )
    .expect("write selectable fake Node");
    let log_path = dir.path().join("test.log");
    let log = std::fs::File::create(&log_path).expect("create isolated test log");
    let mut cmd = Command::new(std::env::current_exe().expect("test executable"));
    cmd.args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .envs(env)
        .env("INTENTD_CODEX_NPX_TEST", test_name)
        .env("MOCK_CODEX_ADAPTER_FILE", dir.path().join("adapter-path"))
        .stdout(Stdio::from(log.try_clone().expect("clone log")))
        .stderr(Stdio::from(log));
    let mut child = GuardedChild::spawn(&mut cmd).expect("spawn isolated WSS test");
    let status = child
        .wait_with_timeout(Duration::from_secs(180))
        .expect("wait for isolated WSS test")
        .expect("isolated WSS test timed out");
    assert!(
        status.success(),
        "{test_name} failed: {}",
        std::fs::read_to_string(log_path).unwrap_or_default()
    );
    true
}

/// Change only this isolated test process's mock Node target between calls.
pub fn select_adapter(adapter: &Path) {
    let path = std::env::var_os("MOCK_CODEX_ADAPTER_FILE").expect("isolated Node test");
    std::fs::write(path, format!("{}\n", adapter.display())).expect("select mock adapter");
}
