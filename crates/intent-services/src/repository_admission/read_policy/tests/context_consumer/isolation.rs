//! Re-exec only these fixtures: production Git reads must still inspect local
//! configuration, while unrelated machine/user config must not enter the test.
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;

const WORKER: &str = "INTENT_CONTEXT_BOUNDARY_WORKER";
const PROOF: &str = "INTENT_CONTEXT_BOUNDARY_PROOF";
const CONTROLLER: &str = "INTENT_CONTEXT_BOUNDARY_CONTROLLER";
const START: &str = "INTENT_CONTEXT_BOUNDARY_START";

pub(super) struct Completion(PathBuf, String);

impl Completion {
    pub(super) fn finish(self) {
        // Only after all assertions and fixture teardown. create_new prevents
        // stale or duplicate proofs from turning an early return into success.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.0)
            .unwrap();
        file.write_all(self.1.as_bytes()).unwrap();
    }
}

fn await_start() {
    if std::env::var_os(START).is_some() {
        use std::io::Read;
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
        assert_eq!(
            byte,
            [1],
            "child must be contained before starting fixtures"
        );
    }
}

pub(super) fn enter(test: &str) -> Option<Completion> {
    await_start();
    if let Some(worker) = std::env::var_os(WORKER) {
        assert_eq!(worker, test, "wrong exact-test worker");
        let proof = PathBuf::from(std::env::var_os(PROOF).expect("worker proof path"));
        assert!(!proof.exists(), "proof must be fresh");
        return Some(Completion(proof, test.to_owned()));
    }
    let root = crate::test_support::test_tempdir("context-boundary-worker-");
    let proof = root.path().join("completed");
    let mut command = exact_test(test);
    isolate_git(&mut command, root.path());
    command
        .env(WORKER, test)
        .env(PROOF, &proof)
        .env_remove(CONTROLLER);
    // Retain HOME, PATH, caller enforcement, fixture and coverage variables.
    // All child scratch data remains parent-owned even on watchdog expiry.
    command
        .env("TMPDIR", root.path())
        .env("TMP", root.path())
        .env("TEMP", root.path());
    let nested = std::env::var_os(CONTROLLER).is_some();
    run(command, root.path(), Duration::from_secs(60), nested);
    assert_eq!(std::fs::read_to_string(proof).unwrap(), test);
    None
}

fn isolate_git(command: &mut Command, root: &Path) {
    for name in ["global", "system"] {
        let path = root.join(name);
        std::fs::write(&path, "").unwrap();
        command.env(format!("GIT_CONFIG_{}", name.to_uppercase()), path);
    }
    // Include explicit overrides as well as inherited indexed config entries.
    let keys: Vec<_> = std::env::vars_os()
        .map(|(key, _)| key)
        .chain(command.get_envs().map(|(key, _)| key.to_owned()))
        .filter(|key| {
            // Windows environment names are case-insensitive.
            let key = key.to_string_lossy().to_ascii_uppercase();
            key == "GIT_CONFIG_PARAMETERS"
                || key == "GIT_CONFIG_COUNT"
                || key.starts_with("GIT_CONFIG_KEY_")
                || key.starts_with("GIT_CONFIG_VALUE_")
        })
        .collect();
    for key in keys {
        command.env_remove(key);
    }
}

fn exact_test(test: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", test, "--nocapture", "--test-threads=1"]);
    // Each launch receives its own stdin handshake, not its parent's.
    command.env(START, "1");
    // Preserve instrumentation, adding PID separation only if a caller supplied
    // a fixed output file (libtest children otherwise overwrite one another).
    if let Some(mut profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        if !profile.to_string_lossy().contains("%p") {
            profile.push(".context-%p.profraw");
            command.env("LLVM_PROFILE_FILE", profile);
        }
    }
    command
}

pub(super) fn ambient_controller(test: &str) {
    let root = crate::test_support::test_tempdir("context-boundary-ambient-");
    for channel in ["global", "system"] {
        std::fs::write(
            root.path().join(channel),
            format!(
                "[remote \"canary-{channel}\"]\nurl = https://{channel}.test/fixture/repo.git\n"
            ),
        )
        .unwrap();
    }
    let mut command = exact_test(test);
    command
        .env_remove(WORKER)
        .env_remove(PROOF)
        .env(CONTROLLER, "1")
        .env_remove("GIT_CONFIG_NOSYSTEM")
        .env("GIT_CONFIG_GLOBAL", root.path().join("global"))
        .env("GIT_CONFIG_SYSTEM", root.path().join("system"))
        .env(
            "GIT_CONFIG_PARAMETERS",
            "'remote.canary-parameters.url=https://parameters.test/fixture/repo.git'",
        )
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "remote.canary-count.url")
        .env("GIT_CONFIG_VALUE_0", "https://count.test/fixture/repo.git");
    run(command, root.path(), Duration::from_secs(90), false);
}

fn run(mut command: Command, root: &Path, timeout: Duration, nested: bool) {
    let log_path = root.join("child.log");
    let log = File::create(&log_path).unwrap();
    command
        .stdin(Stdio::piped())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let mut child = platform::Child::spawn(&mut command, nested);
    child.start();
    let status = child.wait(timeout);
    #[cfg(windows)]
    let cleaned = child.cleanup();
    drop(child);
    #[cfg(windows)]
    assert!(cleaned, "boundary child job did not finish cleanup");
    let output = std::fs::read_to_string(log_path).unwrap();
    assert!(
        status.is_some_and(|s| s.success()),
        "boundary child failed: {status:?}\n{output}"
    );
    assert!(
        output.contains("1 passed"),
        "exact test did not execute: {output}"
    );
}

#[cfg(unix)]
mod platform {
    use super::*;
    use intentd_test_support::GuardedChild;
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::{getpgrp, getpid};

    pub(super) enum Child {
        Group(GuardedChild),
        // raw-child: allow — nested worker shares the controller's guarded group
        Nested(std::process::Child),
    }

    impl Child {
        pub(super) fn spawn(command: &mut Command, nested: bool) -> Self {
            if nested {
                // Only the ambient controller may launch a nested worker. Do
                // not create a second group that escapes its parent's guard.
                assert_eq!(getpgrp(), getpid(), "controller must own its group");
                Self::Nested(command.spawn().unwrap())
            } else {
                Self::Group(GuardedChild::spawn(command).unwrap())
            }
        }

        pub(super) fn start(&mut self) {
            match self {
                Self::Group(child) => child.stdin.take(),
                Self::Nested(child) => child.stdin.take(),
            }
            .unwrap()
            .write_all(&[1])
            .unwrap();
        }

        pub(super) fn wait(&mut self, timeout: Duration) -> Option<ExitStatus> {
            match self {
                Self::Group(child) => child.wait_with_timeout(timeout).unwrap(),
                Self::Nested(child) => {
                    let deadline = std::time::Instant::now() + timeout;
                    loop {
                        if let Some(status) = child.try_wait().unwrap() {
                            return Some(status);
                        }
                        if std::time::Instant::now() >= deadline {
                            return None;
                        }
                        // timing-guard: bounded process exit polling
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        }
    }

    impl Drop for Child {
        fn drop(&mut self) {
            if let Self::Nested(child) = self {
                if !matches!(child.try_wait(), Ok(Some(_))) {
                    // The outer GuardedChild owns/reaps the controller. On a
                    // nested timeout or panic kill this entire group, including
                    // ourselves, so neither worker nor Git can outlive it.
                    let _ = killpg(getpgrp(), Signal::SIGKILL);
                }
            }
        }
    }
}

#[cfg(windows)]
#[path = "isolation_windows.rs"]
mod platform;

#[test]
fn child_config_isolation_preserves_local_config_caller_and_coverage_overrides() {
    let root = crate::test_support::test_tempdir("context-boundary-env-");
    let mut command = Command::new("git");
    for (key, value) in [
        ("HOME", "keep-home"),
        ("PATH", "keep-path"),
        ("INTENTD_ASSERT_BOUND_CALLER", "1"),
        ("LLVM_PROFILE_FILE", "keep-%p-%m.profraw"),
        (
            "GIT_CONFIG_PARAMETERS",
            "'remote.canary.url=https://canary.test/r'",
        ),
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_KEY_0", "remote.canary.url"),
        ("GIT_CONFIG_VALUE_0", "https://canary.test/r"),
        ("GIT_CONFIG_KEY_27", "remote.stale.url"),
        ("GIT_CONFIG_VALUE_27", "stale"),
        ("git_config_key_28", "remote.lower.url"),
        ("git_config_value_28", "lower"),
    ] {
        command.env(key, value);
    }
    isolate_git(&mut command, root.path());
    let env: std::collections::BTreeMap<_, _> = command.get_envs().collect();
    for (key, value) in [
        ("HOME", "keep-home"),
        ("PATH", "keep-path"),
        ("INTENTD_ASSERT_BOUND_CALLER", "1"),
        ("LLVM_PROFILE_FILE", "keep-%p-%m.profraw"),
    ] {
        assert_eq!(
            env[std::ffi::OsStr::new(key)],
            Some(std::ffi::OsStr::new(value))
        );
    }
    for key in [
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_CONFIG_KEY_27",
        "GIT_CONFIG_VALUE_27",
        "git_config_key_28",
        "git_config_value_28",
    ] {
        assert_eq!(env[std::ffi::OsStr::new(key)], None);
    }
    for (key, name) in [
        ("GIT_CONFIG_GLOBAL", "global"),
        ("GIT_CONFIG_SYSTEM", "system"),
    ] {
        assert_eq!(
            env[std::ffi::OsStr::new(key)],
            Some(root.path().join(name).as_os_str())
        );
        assert_eq!(std::fs::read(root.path().join(name)).unwrap(), b"");
    }
}
