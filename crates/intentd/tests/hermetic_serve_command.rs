//! Command-only identity contract: never spawn a daemon or inspect host credentials.
mod common;

use std::ffi::OsStr;
use std::path::Path;
use std::process::Command;

#[derive(Debug, PartialEq, Eq)]
enum EnvSetting<'a> {
    Inherited,
    Removed,
    Set(&'a OsStr),
}

fn explicit_env<'a>(cmd: &'a Command, name: &str) -> EnvSetting<'a> {
    match cmd.get_envs().find(|(key, _)| *key == OsStr::new(name)) {
        None => EnvSetting::Inherited,
        Some((_, None)) => EnvSetting::Removed,
        Some((_, Some(value))) => EnvSetting::Set(value),
    }
}

fn fixture_command(data_dir: &Path, fixed: bool) -> Command {
    if fixed {
        common::hermetic_serve_command_fixed_port(data_dir)
    } else {
        common::hermetic_serve_command(data_dir)
    }
}

fn identity_contract(cmd: &Command, data_dir: &Path) -> Result<(), &'static str> {
    for name in ["GITHUB_TOKEN", "GH_TOKEN"] {
        if explicit_env(cmd, name) != EnvSetting::Removed {
            return Err(name);
        }
    }
    for (name, path) in [
        ("GH_CONFIG_DIR", data_dir.join("gh-config")),
        ("INTENTD_SECRETS_FILE", data_dir.join("secrets.json")),
    ] {
        if explicit_env(cmd, name) != EnvSetting::Set(path.as_os_str()) {
            return Err(name);
        }
    }
    let gh = data_dir.join("gh-config");
    if !gh.is_dir() || std::fs::read_dir(gh).unwrap().next().is_some() {
        return Err("GH_CONFIG_DIR must be empty");
    }
    Ok(())
}

#[test]
fn environment_probe_distinguishes_inherited_removed_and_explicit_values() {
    let mut cmd = Command::new("never-executed");
    assert_eq!(explicit_env(&cmd, "GH_TOKEN"), EnvSetting::Inherited);
    cmd.env_remove("GH_TOKEN");
    assert_eq!(explicit_env(&cmd, "GH_TOKEN"), EnvSetting::Removed);
    cmd.env("GH_TOKEN", "synthetic-token");
    assert_eq!(
        explicit_env(&cmd, "GH_TOKEN"),
        EnvSetting::Set(OsStr::new("synthetic-token"))
    );
    cmd.env("GH_TOKEN", "");
    assert_eq!(
        explicit_env(&cmd, "GH_TOKEN"),
        EnvSetting::Set(OsStr::new(""))
    );
}

#[test]
fn both_constructors_supply_complete_identity_and_preserve_port_behavior() {
    for fixed in [false, true] {
        let dir = common::test_tempdir("serve-contract-");
        let mut cmd = fixture_command(dir.path(), fixed);
        assert_eq!(identity_contract(&cmd, dir.path()), Ok(()));
        assert_eq!(cmd.get_program(), OsStr::new(env!("CARGO_BIN_EXE_intentd")));
        assert_eq!(cmd.get_args().collect::<Vec<_>>(), [OsStr::new("serve")]);
        assert_eq!(
            explicit_env(&cmd, "INTENTD_DATA_DIR"),
            EnvSetting::Set(dir.path().as_os_str())
        );
        assert_eq!(
            explicit_env(&cmd, "INTENTD_TCP_PORT"),
            if fixed {
                EnvSetting::Inherited
            } else {
                EnvSetting::Set(OsStr::new("0"))
            }
        );
        cmd.env("INTENTD_TCP_PORT", "54321");
        assert_eq!(
            explicit_env(&cmd, "INTENTD_TCP_PORT"),
            EnvSetting::Set(OsStr::new("54321"))
        );
    }
}

#[test]
fn every_missing_identity_setting_is_detected_before_spawn() {
    for fixed in [false, true] {
        for missing in [
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "GH_CONFIG_DIR",
            "INTENTD_SECRETS_FILE",
        ] {
            let dir = common::test_tempdir("serve-contract-mutation-");
            let cmd = fixture_command(dir.path(), fixed);
            assert_eq!(identity_contract(&cmd, dir.path()), Ok(()));
            // Rebuild explicit command settings while omitting exactly one setting.
            // No env_clear: an absent entry must model inheritance, not removal.
            let mut mutant = Command::new(cmd.get_program());
            mutant.args(cmd.get_args());
            for (key, value) in cmd.get_envs() {
                if key == OsStr::new(missing) {
                    continue;
                }
                match value {
                    Some(value) => {
                        mutant.env(key, value);
                    }
                    None => {
                        mutant.env_remove(key);
                    }
                }
            }
            assert_eq!(explicit_env(&mutant, missing), EnvSetting::Inherited);
            assert_eq!(identity_contract(&mutant, dir.path()), Err(missing));
            mutant.env(missing, "synthetic-host-value");
            assert_eq!(identity_contract(&mutant, dir.path()), Err(missing));
        }
    }
}

#[test]
fn empty_config_reuse_preserves_private_secrets_and_fixture_state() {
    let dir = common::test_tempdir("serve-contract-reuse-");
    std::fs::create_dir(dir.path().join("gh-config")).unwrap();
    let secrets = dir.path().join("secrets.json");
    std::fs::write(&secrets, "synthetic private fixture state").unwrap();
    for fixed in [false, true] {
        let cmd = fixture_command(dir.path(), fixed);
        assert_eq!(identity_contract(&cmd, dir.path()), Ok(()));
        assert_eq!(
            std::fs::read_to_string(&secrets).unwrap(),
            "synthetic private fixture state"
        );
    }
}

#[test]
fn populated_config_is_rejected_without_erasing_fixture_state() {
    for fixed in [false, true] {
        let dir = common::test_tempdir("serve-contract-populated-");
        let gh = dir.path().join("gh-config");
        std::fs::create_dir(&gh).unwrap();
        let hosts = gh.join("hosts.yml");
        std::fs::write(&hosts, "synthetic private identity").unwrap();
        let rejected = {
            let _suppress = common::suppress_failure_retention();
            std::panic::catch_unwind(|| fixture_command(dir.path(), fixed)).is_err()
        };
        assert!(rejected);
        assert_eq!(
            std::fs::read_to_string(hosts).unwrap(),
            "synthetic private identity"
        );
    }
}

#[cfg(unix)]
#[test]
fn config_symlink_is_rejected_even_when_target_is_empty() {
    for fixed in [false, true] {
        let dir = common::test_tempdir("serve-contract-symlink-");
        let other = common::test_tempdir("serve-contract-other-");
        std::os::unix::fs::symlink(other.path(), dir.path().join("gh-config")).unwrap();
        let rejected = {
            let _suppress = common::suppress_failure_retention();
            std::panic::catch_unwind(|| fixture_command(dir.path(), fixed)).is_err()
        };
        assert!(rejected);
        assert_eq!(std::fs::read_dir(other.path()).unwrap().count(), 0);
    }
}

#[cfg(unix)]
#[test]
fn secrets_symlink_is_rejected_without_reading_its_target() {
    for fixed in [false, true] {
        let dir = common::test_tempdir("serve-contract-secrets-link-");
        let other = common::test_tempdir("serve-contract-secrets-target-");
        let target = other.path().join("secrets.json");
        std::fs::write(&target, "synthetic outside secret").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join("secrets.json")).unwrap();
        let rejected = {
            let _suppress = common::suppress_failure_retention();
            std::panic::catch_unwind(|| fixture_command(dir.path(), fixed)).is_err()
        };
        assert!(rejected);
        assert_eq!(
            std::fs::read_to_string(target).unwrap(),
            "synthetic outside secret"
        );
    }
}

#[test]
fn reapplying_identity_after_overrides_restores_all_four_settings() {
    let dir = common::test_tempdir("serve-contract-reapply-");
    let mut cmd = fixture_command(dir.path(), false);
    for key in [
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "GH_CONFIG_DIR",
        "INTENTD_SECRETS_FILE",
    ] {
        cmd.env(key, "synthetic-host-value");
    }
    cmd.env("MOCK_AGENT_VALUE", "keep-me");
    common::hermetic_fixture_identity(&mut cmd, dir.path());
    assert_eq!(identity_contract(&cmd, dir.path()), Ok(()));
    assert_eq!(
        explicit_env(&cmd, "MOCK_AGENT_VALUE"),
        EnvSetting::Set(OsStr::new("keep-me"))
    );
}
