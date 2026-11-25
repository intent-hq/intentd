//! The orchestrator binary's process-wide startup, before any tenant opens:
//! it raises the soft descriptor limit and logs it, sets up workload memory
//! containment, and runs the PTY workload shim that contained terminals
//! exec through (`/proc/self/exe cgroup-exec …`).
//!
//! No Postgres needed: the binary stops at the missing URL, after this
//! setup.
#![cfg(all(feature = "postgres", unix))]

use std::process::{Command, Output, Stdio};

/// Run the binary without a Postgres URL: it does its process setup, then
/// exits 1.
fn start_without_postgres(dir: &std::path::Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"))
        .env_remove("INTENTD_POSTGRES_URL")
        .env("INTENTD_DATA_DIR", dir)
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .output()
        .expect("run the orchestrator")
}

/// The `key=value` field of the first log line containing `message`.
fn field(log: &str, message: &str, key: &str) -> Option<String> {
    let line = log.lines().find(|line| line.contains(message))?;
    let at = line.find(&format!(" {key}="))? + key.len() + 2;
    Some(line[at..].split_whitespace().next()?.to_owned())
}

#[test]
fn logs_the_raised_fd_limit_and_containment_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let out = start_without_postgres(dir.path());
    let log =
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "no URL exits 1: {log}");
    let soft: u64 = field(&log, "fd limit", "soft")
        .unwrap_or_else(|| panic!("no fd limit line: {log}"))
        .parse()
        .expect("soft is a number");
    let hard = field(&log, "fd limit", "hard").expect("hard is logged");
    // The soft limit is raised to the hard one (macOS caps it at OPEN_MAX).
    let target = if cfg!(target_os = "macos") {
        hard.parse::<u64>().map_or(10240, |hard| hard.min(10240))
    } else {
        hard.parse::<u64>().expect("Linux has a finite hard limit")
    };
    assert!(
        soft >= target,
        "soft {soft} is not raised to {target}: {log}"
    );
    assert!(
        log.contains("workload memory containment"),
        "containment is set up and logged: {log}"
    );
}

#[test]
fn a_bad_workload_memory_ceiling_fails_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"))
        .env("INTENTD_POSTGRES_URL", "postgres://127.0.0.1:1/never")
        .env("INTENTD_WORKLOAD_MEMORY_MAX_MB", "lots")
        .env("INTENTD_DATA_DIR", dir.path())
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .output()
        .expect("run the orchestrator");
    let log =
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{log}");
    assert!(log.contains("INTENTD_WORKLOAD_MEMORY_MAX_MB"), "{log}");
    assert!(!log.contains("fd limit"), "nothing is set up: {log}");
}

#[cfg(target_os = "linux")]
#[test]
fn runs_the_pty_workload_shim() {
    let out = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"))
        .args([
            "cgroup-exec",
            "--oom-score-adj",
            "0",
            "--",
            "/bin/sh",
            "-c",
            "exit 7",
        ])
        .env_remove("INTENTD_POSTGRES_URL")
        .stdin(Stdio::null())
        .output()
        .expect("run the shim");
    assert_eq!(
        out.status.code(),
        Some(7),
        "the shim execs the workload: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
