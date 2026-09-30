//! Inspect fixture inputs before spawning, then exercise saved-credential
//! isolation using only a private synthetic host and a local CLI stub.

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
        environment.get(OsStr::new("INTENTD_ASSERT_HERMETIC_ROOT")) == Some(&Some(OsStr::new("1"))),
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
        (
            "GITHUB_ENTERPRISE_TOKEN",
            "synthetic-github-enterprise-token",
        ),
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
        (
            "MOCK_AGENT_BEHAVIOR",
            r#"{"promptRpcError":{"code":-32603}}"#,
        ),
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
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        [OsStr::new("serve")]
    );
    assert!(
        environment.get(OsStr::new("INTENTD_TCP_PORT")) == Some(&Some(OsStr::new("0"))),
        "wake fixture must retain the ephemeral WSS port"
    );
}

#[intent_test_macros::daemon_test]
async fn saved_host_credentials_are_not_used_by_daemon() {
    use std::os::unix::fs::PermissionsExt;

    let host = super::common::test_tempdir("itd-woc-synthetic-host-");
    let gh_config = host.path().join("gh");
    let bin = host.path().join("bin");
    std::fs::create_dir(&gh_config).unwrap();
    std::fs::create_dir(&bin).unwrap();
    let saved_login = "github.com:\n    user: synthetic-host\n    oauth_token: synthetic-token\n";
    let saved_secret = r#"{"sourceControl.github.token":"synthetic-saved-token"}"#;
    std::fs::write(gh_config.join("hosts.yml"), saved_login).unwrap();
    let secrets = host.path().join("secrets.json");
    std::fs::write(&secrets, saved_secret).unwrap();
    let observations = host.path().join("gh-config-observations");
    let gh = bin.join("gh");
    // Never emits credentials or makes a network request. Record the actual
    // child boundary; an unsafe fallback cannot hide behind an invalid token.
    std::fs::write(
        &gh,
        "#!/bin/sh\nprintf '%s\\n' \"$GH_CONFIG_DIR\" >> \"$WOC_GH_OBSERVATIONS\"\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    )))
    .unwrap();
    let inputs = [
        ("GH_TOKEN", "synthetic-env-token"),
        ("GITHUB_TOKEN", "synthetic-env-token"),
        ("GH_HOST", "synthetic-enterprise.invalid"),
        ("GH_ENTERPRISE_TOKEN", "synthetic-enterprise-token"),
        ("GITHUB_ENTERPRISE_TOKEN", "synthetic-enterprise-token"),
        ("GH_CONFIG_DIR", gh_config.to_str().unwrap()),
        ("INTENTD_SECRETS_FILE", secrets.to_str().unwrap()),
        ("PATH", path.to_str().unwrap()),
        ("WOC_GH_OBSERVATIONS", observations.to_str().unwrap()),
    ];
    // A broken command policy fails before a synthetic token can be offered to
    // a forge. The real daemon below uses exactly the same builder and inputs.
    let command_root = super::common::test_tempdir("itd-woc-command-");
    assert_isolated(
        &super::serve_command(command_root.path(), "both", &inputs),
        command_root.path(),
    );
    let (daemon, _, _, port, fingerprint) =
        super::boot_daemon_with_task_env("Isolated Identity", &inputs).await;
    let mut rpc = super::connect_ws(port, super::client_config(&fingerprint)).await;
    let status = super::wss_rpc(&mut rpc, 1, "github.authStatus", serde_json::json!({})).await;
    assert_eq!(status["isConfigured"], false, "{status}");
    let expected = daemon.data_dir.path().join("gh-config");
    let reads =
        std::fs::read_to_string(&observations).expect("the CLI fallback was actually exercised");
    assert!(!reads.is_empty());
    assert!(reads.lines().all(|line| Path::new(line) == expected));
    assert!(std::fs::read_dir(&expected).unwrap().next().is_none());
    drop(rpc);
    drop(daemon);
    assert_eq!(
        std::fs::read_to_string(gh_config.join("hosts.yml")).unwrap(),
        saved_login
    );
    assert_eq!(std::fs::read_to_string(&secrets).unwrap(), saved_secret);
}
