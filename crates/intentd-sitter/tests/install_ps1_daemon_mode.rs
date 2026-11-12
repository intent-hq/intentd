//! The "How will you use this computer?" half of `scripts/install.ps1`: the
//! `daemon-mode-choice` block (flag, env var, validation) and the
//! `daemon-mode-apply` block (node setup, switching a node back), run
//! verbatim under PowerShell against a fake `intentd` that records its calls
//! — the Windows counterpart of `install_sh_daemon_mode.rs`. Like
//! `install_ps1_owner.rs`, the tests skip without `pwsh` and fail instead
//! when CI sets `INTENTD_CI_EXPECT_PWSH`.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

#[track_caller]
fn ps_region(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.ps1");
    let script = fs::read_to_string(&path).unwrap();
    let begin = format!(">>> BEGIN {name}");
    let end = format!("<<< END {name}");
    let start = script
        .find(&begin)
        .unwrap_or_else(|| panic!("missing `{begin}`"));
    let after = start + script[start..].find('\n').unwrap() + 1;
    let stop = after
        + script[after..]
            .find(&end)
            .unwrap_or_else(|| panic!("missing `{end}`"));
    let region = script[after..stop].trim_end();
    region
        .strip_suffix('#')
        .unwrap_or(region)
        .trim_end()
        .to_string()
}

#[track_caller]
fn pwsh() -> Option<PathBuf> {
    for name in ["pwsh", "powershell"] {
        let found = Command::new("sh")
            .arg("-c")
            .arg(format!("command -v {name}"))
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|p| !p.is_empty());
        if let Some(path) = found {
            return Some(PathBuf::from(path));
        }
    }
    assert!(
        std::env::var_os("INTENTD_CI_EXPECT_PWSH").is_none_or(|v| v.is_empty()),
        "INTENTD_CI_EXPECT_PWSH is set but neither `pwsh` nor `powershell` is on PATH"
    );
    None
}

/// A fake `intentd`: records each call, answers `settings daemon.mode` from
/// `$FAKE_STATE/mode`, and exits with `$FAKE_FAIL_ON` when the call matches it.
const FAKE_INTENTD: &str = r#"#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_STATE/calls"
[ "$*" = "${FAKE_FAIL_ON:-}" ] && exit 1
case "$*" in
  "settings daemon.mode") printf 'daemon.mode = %s\n' "$(cat "$FAKE_STATE/mode" 2>/dev/null || echo regular)" ;;
  "pair --node --tunnel --yes") echo 'URL:         intent://node?v=1&host=&port=5181&fp=FP&invite=ni-1&secret=s&tc=tc-fake' ;;
  "pair --yes --tunnel") echo 'URL:         intent://pair?v=1&host=&port=5180&token=T&fp=FP&tc=tc-fake' ;;
esac
exit 0
"#;

struct Outcome {
    output: String,
    calls: Vec<String>,
}

/// Run `body` under PowerShell with `$dest` pointing at the fake daemon.
/// Everything the script writes (output, host, warnings) is captured.
fn run(pwsh: &Path, body: &str, envs: &[(&str, &str)], mode: Option<&str>) -> Outcome {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    fs::create_dir_all(&state).unwrap();
    if let Some(mode) = mode {
        fs::write(state.join("mode"), mode).unwrap();
    }
    let dest = dir.path().join("intentd");
    fs::write(&dest, FAKE_INTENTD).unwrap();
    fs::set_permissions(&dest, fs::Permissions::from_mode(0o755)).unwrap();
    let driver = format!(
        "$ErrorActionPreference = 'Stop'\n$dest = '{}'\n& {{\n{body}\n}} *>&1 | ForEach-Object {{ \"$_\" }}\n",
        dest.display()
    );
    let script = dir.path().join("driver.ps1");
    fs::write(&script, driver).unwrap();
    let cache = dir.path().join("xdg-cache");
    fs::create_dir_all(&cache).unwrap();
    let mut command = Command::new(pwsh);
    command
        .args(["-NoProfile", "-NonInteractive", "-File"])
        .arg(&script)
        .env_remove("INTENTD_DAEMON_MODE")
        .env("XDG_CACHE_HOME", &cache)
        .env("FAKE_STATE", &state);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .replace('\r', "");
    Outcome {
        output: text,
        calls: fs::read_to_string(state.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect(),
    }
}

fn choice(pwsh: &Path, param: &str, env: Option<&str>) -> String {
    let body = format!(
        "$DaemonMode = '{param}'\ntry {{\n{}\n\"MODE=$installMode EXPLICIT=$installModeExplicit\"\n}} catch {{ \"ERROR=$($_.Exception.Message)\" }}",
        ps_region("daemon-mode-choice")
    );
    let envs: Vec<(&str, &str)> = env
        .map(|v| ("INTENTD_DAEMON_MODE", v))
        .into_iter()
        .collect();
    run(pwsh, &body, &envs, None).output
}

fn apply(pwsh: &Path, mode: &str, explicit: bool, verdict: &str, saved: Option<&str>) -> Outcome {
    apply_with_pair(pwsh, mode, explicit, verdict, saved, false)
}

fn apply_with_pair(
    pwsh: &Path,
    mode: &str,
    explicit: bool,
    verdict: &str,
    saved: Option<&str>,
    pair: bool,
) -> Outcome {
    let body = format!(
        "$installMode = '{mode}'\n$installModeExplicit = ${explicit}\n$verdict = '{verdict}'\n$interactive = $false\n$pair = ${pair}\n{}",
        ps_region("daemon-mode-apply")
    );
    run(pwsh, &body, &[], saved)
}

#[test]
fn the_choice_folds_spellings_and_the_parameter_beats_the_env_var() {
    let Some(pwsh) = pwsh() else { return };
    for (param, env, want) in [
        ("Node", None, "MODE=node EXPLICIT=True"),
        ("", Some("regular"), "MODE=full EXPLICIT=True"),
        ("2", Some("full"), "MODE=node EXPLICIT=True"),
        ("", None, "MODE= EXPLICIT=False"),
    ] {
        let out = choice(&pwsh, param, env);
        assert!(
            out.contains(want),
            "-DaemonMode {param:?} env {env:?}: {out}"
        );
    }
    let bad = choice(&pwsh, "", Some("server"));
    assert!(
        bad.contains("invalid INTENTD_DAEMON_MODE value 'server' (expected full or node)"),
        "{bad}"
    );
}

#[test]
fn a_node_install_sets_the_mode_and_tunnel_restarts_then_pairs_unattended() {
    let Some(pwsh) = pwsh() else { return };
    let outcome = apply(&pwsh, "node", true, "up", None);
    assert_eq!(
        outcome.calls,
        [
            "settings daemon.mode node",
            "settings server.tunnel.enabled true",
            "restart",
            "pair --node --tunnel --yes",
        ],
        "{}",
        outcome.output
    );
    assert!(
        outcome.output.contains("intent://node?") && !outcome.output.contains("token="),
        "{}",
        outcome.output
    );
}

#[test]
fn a_failed_restart_names_the_commands_and_skips_pairing() {
    let Some(pwsh) = pwsh() else { return };
    let dir_env = [("FAKE_FAIL_ON", "restart")];
    let body = format!(
        "$installMode = 'node'\n$installModeExplicit = $true\n$verdict = 'up'\n$interactive = $false\n{}",
        ps_region("daemon-mode-apply")
    );
    let outcome = run(&pwsh, &body, &dir_env, None);
    assert!(
        !outcome.calls.iter().any(|c| c.starts_with("pair")),
        "{:?}",
        outcome.calls
    );
    assert!(
        outcome.output.contains("did not come back in node mode")
            && outcome.output.contains("intentd pair --node --tunnel"),
        "{}",
        outcome.output
    );
}

#[test]
fn node_setup_waits_for_a_daemon_that_is_up() {
    let Some(pwsh) = pwsh() else { return };
    let outcome = apply(&pwsh, "node", true, "undecided", None);
    assert!(outcome.calls.is_empty(), "{:?}", outcome.calls);
    assert!(
        outcome.output.contains("node setup did not run"),
        "{}",
        outcome.output
    );
}

#[test]
fn an_explicit_full_answer_switches_a_node_back_and_the_default_does_not() {
    let Some(pwsh) = pwsh() else { return };
    let explicit = apply(&pwsh, "full", true, "up", Some("node"));
    assert_eq!(
        explicit.calls,
        [
            "settings daemon.mode",
            "settings daemon.mode regular",
            "restart"
        ],
        "{}",
        explicit.output
    );
    let default = apply(&pwsh, "full", false, "up", Some("node"));
    assert!(default.calls.is_empty(), "{:?}", default.calls);
    let already_full = apply(&pwsh, "full", true, "up", None);
    assert_eq!(
        already_full.calls,
        ["settings daemon.mode"],
        "{}",
        already_full.output
    );
}

#[test]
fn the_pair_option_pairs_a_full_install_once_the_daemon_is_up() {
    let Some(pwsh) = pwsh() else { return };
    for mode in ["", "full"] {
        let outcome = apply_with_pair(&pwsh, mode, !mode.is_empty(), "up", None, true);
        assert_eq!(
            outcome.calls.last().map(String::as_str),
            Some("pair --yes --tunnel"),
            "{}",
            outcome.output
        );
        assert!(
            outcome.output.contains("intent://pair?"),
            "{}",
            outcome.output
        );
    }
    let waiting = apply_with_pair(&pwsh, "", false, "undecided", None, true);
    assert!(waiting.calls.is_empty(), "{:?}", waiting.calls);
    let node = apply_with_pair(&pwsh, "node", true, "up", None, true);
    assert_eq!(
        node.calls.iter().filter(|c| c.starts_with("pair")).count(),
        1,
        "a node pairs once, as a node: {:?}",
        node.calls
    );
    let off = apply(&pwsh, "", false, "up", None);
    assert!(off.calls.is_empty(), "{:?}", off.calls);
}
