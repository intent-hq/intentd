//! The opt-in saved-command wrapper retains OS evidence independently of a PTY.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use intentd_test_support::GuardedChild;
use nix::sys::signal::Signal;
use serde_json::Value;

fn run(root: &Path, id: &str, shell: &str, timeout: &str) -> GuardedChild {
    GuardedChild::spawn(
        Command::new(env!("CARGO_BIN_EXE_intentd"))
            .args(["command-run", "--record-dir"])
            .arg(root)
            .args([
                "--invocation",
                id,
                "--timeout-seconds",
                timeout,
                "--",
                "/bin/sh",
                "-c",
                shell,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .unwrap()
}

fn result(root: &Path, id: &str) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["command-result", "--record-dir"])
        .arg(root)
        .args(["--invocation", id])
        .output()
        .unwrap();
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&output.stderr)))
}

fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(Instant::now() < deadline, "missing {}", path.display());
        // timing-guard: polling an explicit child/receipt barrier
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn durable_command_records_real_zero_nonzero_and_signal() {
    let root = common::test_tempdir("durable-exit");
    for (id, shell, code, signal) in [
        ("zero", "printf actual-output; exit 0", Some(0), None),
        ("failure", "exit 23", Some(23), None),
        ("interrupted", "kill -TERM $$", None, Some(15)),
    ] {
        let mut child = run(root.path(), id, shell, "10");
        let status = child
            .wait_with_timeout(Duration::from_secs(15))
            .unwrap()
            .expect("wrapper exit");
        assert_eq!(status.success(), code == Some(0));
        let evidence = result(root.path(), id);
        assert_eq!(evidence["outcome"], "exited");
        assert_eq!(evidence["exitCode"], serde_json::json!(code));
        assert_eq!(evidence["signal"], serde_json::json!(signal));
        assert_eq!(evidence["invocation"]["id"], id);
    }
    assert_eq!(
        std::fs::read_to_string(root.path().join("zero/stdout.log")).unwrap(),
        "actual-output"
    );
}

#[test]
fn durable_command_survives_owner_loss_and_refuses_reuse() {
    let root = common::test_tempdir("durable-owner-loss");
    let ready = root.path().join("ready");
    let release = root.path().join("release");
    let shell = format!(
        // timing-guard: poll the release-file barrier under the command timeout
        "touch '{}'; while [ ! -f '{}' ]; do sleep 0.02; done; exit 37",
        ready.display(),
        release.display()
    ); // timing-guard: release-file barrier
    let mut owner = run(root.path(), "run-one", &shell, "20");
    wait_file(&ready);
    assert_eq!(result(root.path(), "run-one")["outcome"], "unknown");
    owner.signal_group(Signal::SIGKILL).unwrap();
    owner.wait().unwrap();
    std::fs::write(&release, "go").unwrap();
    wait_file(&root.path().join("run-one/result.json"));
    let evidence = result(root.path(), "run-one");
    assert_eq!(evidence["exitCode"], 37);
    let mut duplicate = run(root.path(), "run-one", "exit 0", "10");
    assert!(!duplicate
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
        .success());
    assert_eq!(result(root.path(), "run-one"), evidence);
}

#[test]
fn durable_command_timeout_and_untrustworthy_records_never_pass() {
    let root = common::test_tempdir("durable-invalid");
    let mut child = run(root.path(), "timeout", "while :; do :; done", "1");
    assert!(!child
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
        .success());
    assert_eq!(result(root.path(), "timeout")["outcome"], "timedOut");
    assert_eq!(result(root.path(), "absent")["outcome"], "unknown");
    let receipt = root.path().join("timeout/result.json");
    let mut evidence: Value = serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    evidence["invocation"]["id"] = "older-run".into();
    std::fs::write(&receipt, serde_json::to_vec(&evidence).unwrap()).unwrap();
    assert_eq!(result(root.path(), "timeout")["outcome"], "unknown");
    std::fs::write(&receipt, "{\"exitCode\":0").unwrap();
    assert_eq!(result(root.path(), "timeout")["outcome"], "unknown");
}

#[test]
fn durable_command_rejects_stale_same_id_and_reports_spawn_failure() {
    let root = common::test_tempdir("durable-stale");
    let mut child = run(root.path(), "original", "exit 0", "10");
    assert!(child
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
        .success());
    let invocation_path = root.path().join("original/invocation.json");
    let mut invocation: Value =
        serde_json::from_slice(&std::fs::read(&invocation_path).unwrap()).unwrap();
    invocation["startedAt"] = "different-invocation-same-name".into();
    std::fs::write(&invocation_path, serde_json::to_vec(&invocation).unwrap()).unwrap();
    assert_eq!(result(root.path(), "original")["outcome"], "unknown");
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["command-run", "--record-dir"])
        .arg(root.path())
        .args([
            "--invocation",
            "missing-program",
            "--timeout-seconds",
            "10",
            "--",
        ])
        .arg(root.path().join("does-not-exist"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let evidence = result(root.path(), "missing-program");
    assert_eq!(evidence["outcome"], "spawnFailed");
    assert_eq!(evidence["exitCode"], Value::Null);
    assert_eq!(evidence["signal"], Value::Null);
}

fn control(root: &Path, id: &str, action: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args([action, "--record-dir"])
        .arg(root)
        .args(["--invocation", id])
        .output()
        .unwrap()
}

#[test]
fn durable_command_explicit_stop_settles_child_and_cleanup_preserves_tombstone() {
    let root = common::test_tempdir("durable-stop");
    let ready = root.path().join("ready");
    let shell = format!("touch '{}'; while :; do :; done", ready.display());
    let mut owner = run(root.path(), "stop-me", &shell, "20");
    wait_file(&ready);
    assert!(!control(root.path(), "stop-me", "command-clean")
        .status
        .success());
    let stop = control(root.path(), "stop-me", "command-stop");
    assert_eq!(
        stop.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&stop.stderr)
    );
    let evidence = result(root.path(), "stop-me");
    assert_eq!(evidence["outcome"], "stopped");
    assert_eq!(evidence["signal"], 9);
    assert!(!owner
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
        .success());
    assert!(control(root.path(), "stop-me", "command-clean")
        .status
        .success());
    assert!(!root.path().join("stop-me/stdout.log").exists());
    assert_eq!(result(root.path(), "stop-me")["outcome"], "unknown");
    let mut duplicate = run(root.path(), "stop-me", "exit 0", "10");
    assert!(!duplicate
        .wait_with_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap()
        .success());
}

#[test]
fn durable_command_output_is_capped_without_changing_child_exit() {
    let root = common::test_tempdir("durable-output-cap");
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["command-run", "--record-dir"])
        .arg(root.path())
        .args([
            "--invocation",
            "large-output",
            "--timeout-seconds",
            "10",
            "--max-output-bytes",
            "1024",
            "--",
            "/bin/sh",
            "-c",
            "head -c 32768 /dev/zero; head -c 32768 /dev/zero >&2; exit 23",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let evidence = result(root.path(), "large-output");
    assert_eq!(evidence["exitCode"], 23);
    assert_eq!(evidence["stdoutTruncated"], true);
    assert_eq!(evidence["stderrTruncated"], true);
    for name in ["stdout.log", "stderr.log"] {
        assert_eq!(
            std::fs::metadata(root.path().join("large-output").join(name))
                .unwrap()
                .len(),
            1024
        );
    }
}
