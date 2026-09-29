//! Command-only regressions: inspect fixture inputs before any daemon, CLI,
//! credential read or identity request can run.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

fn assert_isolated(command: &Command, data_dir: &Path) {
    let environment: HashMap<_, _> = command.get_envs().collect();
    for key in [
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "GH_HOST",
        "GH_ENTERPRISE_TOKEN",
        "GITHUB_ENTERPRISE_TOKEN",
    ] {
        assert!(
            environment.get(OsStr::new(key)) == Some(&None),
            "wake fixture must remove {key}, including inherited values"
        );
    }
    for (key, path) in [
        ("INTENTD_DATA_DIR", data_dir.to_path_buf()),
        ("INTENTD_CONFIG", data_dir.join("config.toml")),
        ("INTENTD_SECRETS_FILE", data_dir.join("secrets.json")),
        ("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces")),
        ("GH_CONFIG_DIR", data_dir.join("gh-config")),
    ] {
        assert!(
            environment.get(OsStr::new(key)) == Some(&Some(path.as_os_str())),
            "wake fixture must own {key}"
        );
    }
    assert!(
        environment.get(OsStr::new("INTENTD_ASSERT_HERMETIC_ROOT"))
            == Some(&Some(OsStr::new("1"))),
        "wake fixture must keep the hermetic-root guard enabled"
    );
}

#[test]
fn ordinary_command_isolates_identity_and_roots() {
    let data_dir = super::common::test_tempdir("itd-woc-command-");
    let command = super::serve_command(data_dir.path(), "both", &[]);
    assert_isolated(&command, data_dir.path());
}

#[test]
fn host_canaries_cannot_override_fixture_isolation() {
    let data_dir = super::common::test_tempdir("itd-woc-command-");
    // Synthetic values only. Supplying these through the command builder
    // avoids process-global environment changes and never opens these paths.
    let canaries = [
        ("GITHUB_TOKEN", "synthetic-github-token"),
        ("GH_TOKEN", "synthetic-gh-token"),
        ("GH_HOST", "synthetic-enterprise.invalid"),
        ("GH_ENTERPRISE_TOKEN", "synthetic-gh-enterprise-token"),
        ("GITHUB_ENTERPRISE_TOKEN", "synthetic-github-enterprise-token"),
        ("INTENTD_DATA_DIR", "synthetic-host/data"),
        ("INTENTD_CONFIG", "synthetic-host/config.toml"),
        ("INTENTD_SECRETS_FILE", "synthetic-host/secrets.json"),
        ("INTENTD_WORKSPACES_DIR", "synthetic-host/workspaces"),
        ("GH_CONFIG_DIR", "synthetic-host/gh"),
        ("INTENTD_ASSERT_HERMETIC_ROOT", "0"),
    ];
    let command = super::serve_command(data_dir.path(), "both", &canaries);
    assert_isolated(&command, data_dir.path());
}

#[test]
fn fixture_command_preserves_mock_provider_inputs() {
    let data_dir = super::common::test_tempdir("itd-woc-command-");
    let inputs = [
        ("INTENTD_AUTH_TOKEN", super::TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", "synthetic-provider.mjs"),
        ("MOCK_AGENT_BEHAVIOR", r#"{"promptRpcError":{"code":-32603}}"#),
    ];
    let command = super::serve_command(data_dir.path(), "both", &inputs);
    let environment: HashMap<_, _> = command.get_envs().collect();
    for (key, value) in inputs {
        assert!(
            environment.get(OsStr::new(key)) == Some(&Some(OsStr::new(value))),
            "wake fixture must preserve {key}"
        );
    }
    assert_eq!(
        command.get_program(),
        OsStr::new(env!("CARGO_BIN_EXE_intentd"))
    );
    assert_eq!(command.get_args().collect::<Vec<_>>(), [OsStr::new("serve")]);
    assert!(
        environment.get(OsStr::new("INTENTD_TCP_PORT")) == Some(&Some(OsStr::new("0"))),
        "wake fixture must retain the ephemeral WSS port"
    );
}
