//! Unit-test scratch and local CLI apparatus.

mod scratch;
pub(crate) use scratch::test_tempdir;

/// Supply the canonical installed CLI required even by a mock ACP adapter.
/// Keep the guard alive with the fixture; it restores PATH under the test env lock.
#[cfg(unix)]
pub(crate) fn installed_cli_env(
    dir: &std::path::Path,
    command: &str,
) -> crate::agent_manager::tests::EnvGuard {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("installed-cli");
    std::fs::create_dir_all(&bin).unwrap();
    let cli = bin.join(command);
    std::fs::write(&cli, "#!/bin/sh\n[ \"$#\" = 1 ] && [ \"$1\" = --version ] || exit 91\nprintf 'fixture-cli 1.0.0\\n'\n").unwrap();
    std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).unwrap();
    crate::agent_manager::tests::EnvGuard::set_all(&[("PATH", path.to_str().unwrap())])
}

/// Mock ACP adapter for quick-action effort tests, with an exact request log.
#[cfg(unix)]
pub(crate) fn quick_action_effort_adapter(
    behavior: &serde_json::Value,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    crate::agent_manager::tests::EnvGuard,
) {
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir("quick-action-effort-");
    let log = dir.path().join("requests.jsonl");
    let config = dir.path().join("behavior.json");
    std::fs::write(&config, behavior.to_string()).unwrap();
    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../intentd/tests/fixtures/mock-quick-action-effort.mjs");
    let bin = dir.path().join("claude-agent-acp");
    std::fs::write(&bin, format!("#!/bin/sh\nMOCK_EFFORT_BEHAVIOR=\"$(cat {config:?})\" MOCK_EFFORT_LOG={log:?} exec node {fixture:?}\n")).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let env = installed_cli_env(dir.path(), "claude");
    (dir, bin, log, env)
}
