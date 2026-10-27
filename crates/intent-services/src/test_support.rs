//! Unit-test scratch and local CLI apparatus.
//!
//! Compiled for this crate's tests and, under the `test-hooks` feature, for
//! the tests of feature crates. The parts that need dev-only dependencies
//! (provider CLI stubs, fixture timeouts) stay `cfg(test)`.

pub mod capability_fixtures;
#[cfg(test)]
pub(crate) mod fake_runtime;
pub mod fixtures;
pub mod goldens;
#[cfg(test)]
pub(crate) mod provider_installed;
#[cfg(test)]
pub(crate) mod quota_script;
pub mod retire_workspace;
mod scratch;
pub mod setup_lifecycle;
pub mod submodule_fixture;
pub use scratch::{test_tempdir, test_tempdir_in};

/// Supply the canonical installed CLI required even by a mock ACP adapter:
/// [`intent_providers::test_support::current_cli_script`], so the launch
/// never selects a bundled copy. Keep the guard alive with the fixture; it
/// restores PATH under the test env lock.
#[cfg(test)]
#[cfg(unix)]
pub(crate) fn installed_cli_env(
    dir: &std::path::Path,
    cli: intent_providers::installed_cli::InstalledCli,
) -> crate::agent_manager::tests::EnvGuard {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("installed-cli");
    std::fs::create_dir_all(&bin).unwrap();
    let path = bin.join(cli.command());
    std::fs::write(
        &path,
        intent_providers::test_support::current_cli_script(cli),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).unwrap();
    crate::agent_manager::tests::EnvGuard::set_all(&[("PATH", path.to_str().unwrap())])
}

/// Mock ACP adapter for quick-action effort tests, with an exact request log.
#[cfg(test)]
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
    let env = installed_cli_env(
        dir.path(),
        intent_providers::installed_cli::InstalledCli::Claude,
    );
    (dir, bin, log, env)
}

/// Bound fixture I/O without shortening normal runs under coverage settings
/// (delegates to [`intentd_test_support::test_timeout`]).
#[cfg(test)]
pub(crate) fn fixture_timeout(base: std::time::Duration) -> std::time::Duration {
    intentd_test_support::test_timeout(base)
}
