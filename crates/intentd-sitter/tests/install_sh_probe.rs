//! Guard tests for `scripts/install.sh`'s `report_installed_version`: the
//! `--sitter-version` probe must surface a probe FAILURE (broken binary,
//! bad env) instead of silently reporting "no version" — the silent
//! swallowing is exactly how a case-mangled `INTENTD_CHANNEL` breaking
//! every child `intentd` call went unnoticed. The function is extracted
//! from the real script, so these tests pin the shipped code, not a copy.

#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// Extract the body of `report_installed_version()` from install.sh
/// (function header through the first column-0 `}`).
fn probe_function() -> String {
    let script_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.sh");
    let script = fs::read_to_string(&script_path).unwrap();
    let start = script
        .find("report_installed_version() {")
        .expect("install.sh must define report_installed_version()");
    let rest = &script[start..];
    let end = rest
        .find("\n}")
        .expect("report_installed_version() must close with a column-0 brace");
    rest[..end + 2].to_string()
}

/// Run the extracted function against a fake `intentd` with the given body,
/// under install.sh's own `info`/`warn` helpers and `set -eu`.
fn run_probe(fake_intentd_body: &str) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let fake = dir.path().join("intentd");
    fs::write(&fake, format!("#!/bin/sh\n{fake_intentd_body}\n")).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let driver = format!(
        "set -eu\n\
         info() {{ printf '%s\\n' \"install.sh: $*\"; }}\n\
         warn() {{ printf '%s\\n' \"install.sh: warning: $*\" >&2; }}\n\
         install_dir={install_dir}\n\
         {function}\n\
         report_installed_version\n",
        install_dir = shell_quote(dir.path().to_str().unwrap()),
        function = probe_function(),
    );
    Command::new("/bin/sh")
        .arg("-c")
        .arg(driver)
        .output()
        .unwrap()
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn successful_probe_reports_the_version() {
    let output = run_probe("printf '%s\\n' 'intentd-sitter 9.9.9'");
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed intentd-sitter 9.9.9 to"),
        "stdout: {stdout}"
    );
    assert_eq!(stderr_of(&output), "", "no warning on a successful probe");
}

#[test]
fn successful_but_empty_probe_is_the_quiet_no_version_report() {
    let output = run_probe("exit 0");
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_of(&output);
    assert!(stdout.contains("installed intentd to"), "stdout: {stdout}");
    assert_eq!(
        stderr_of(&output),
        "",
        "genuine absence of a version is not a failure"
    );
}

#[test]
fn failed_probe_is_a_warning_quoting_the_probe_output_not_a_silent_no_version() {
    let output = run_probe("echo 'intentd-sitter: invalid channel \"BeTa\"' >&2; exit 2");
    // Non-fatal by design: the binary is installed either way.
    assert_eq!(output.status.code(), Some(0));
    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("warning:") && stderr.contains("--sitter-version failed"),
        "a failed probe must surface as a warning, got stderr: {stderr}"
    );
    assert!(
        stderr.contains("invalid channel \"BeTa\""),
        "the warning must quote the probe's own output, got stderr: {stderr}"
    );
    let stdout = stdout_of(&output);
    assert!(
        !stdout.contains("installed intentd to"),
        "a failed probe must not masquerade as the quiet no-version report, got stdout: {stdout}"
    );
}
