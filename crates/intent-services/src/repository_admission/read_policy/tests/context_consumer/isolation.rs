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

// Diagnostic-only owner telemetry is outside cleanup-owned scratch. The outer
// supervisor never signals an unowned/reaped worker group by a numeric PID.
fn diagnostic_owner(event: &str, root: &Path, cleaned: Option<bool>) {
    let Some(path) = std::env::var_os("CONTEXT_BOUNDARY_OWNER_LOG") else {
        return;
    };
    assert!(Path::new(&path).is_absolute());
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({"event":event,"pid":std::process::id(),
        "root":root,"cleaned":cleaned,"scratch_exists":root.exists()})
    )
    .unwrap();
}

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
    enter_with_timeout(test, Duration::from_secs(60))
}

fn enter_with_timeout(test: &str, timeout: Duration) -> Option<Completion> {
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
    run(
        command,
        root,
        timeout,
        nested,
        Some((proof.as_path(), test)),
    );
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
    // The outer owner must retain every nested directory if the controller
    // exits without running destructors (including a watchdog group kill).
    command
        .env("TMPDIR", root.path())
        .env("TMP", root.path())
        .env("TEMP", root.path());
    run(command, root, Duration::from_secs(90), false, None);
}

fn run(
    mut command: Command,
    root: tempfile::TempDir,
    timeout: Duration,
    nested: bool,
    proof: Option<(&Path, &str)>,
) {
    let root_path = root.path().to_owned();
    // On Unix, nested cleanup delegates descendants to the outer group owner.
    // Its TempDir transitively owns this directory, so only that owner may
    // remove it after the group stops (or retain it on uncertain cleanup).
    let root = if cfg!(unix) && nested {
        let _ = root.keep();
        None
    } else {
        Some(root)
    };
    let log_path = root_path.join("child.log");
    let log = File::create(&log_path).unwrap();
    command
        .stdin(Stdio::piped())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    diagnostic_owner("owner_start", &root_path, None);
    let mut child = platform::Child::spawn(&mut command, nested);
    // Even a broken startup pipe or an injected controller panic must finish
    // cleanup before the owning directory can be removed.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        child.start();
        cleanup_tests::after_start(nested);
        child.wait(timeout)
    }));
    let cleaned = child.cleanup();
    drop(child);
    if !cleaned {
        diagnostic_owner("owner_cleanup", &root_path, Some(false));
        if let Some(root) = root {
            let _ = root.keep();
        }
        panic!(
            "boundary child cleanup unconfirmed; retained {}",
            root_path.display()
        );
    }
    let output = std::fs::read_to_string(log_path).unwrap();
    let completed = proof
        .as_ref()
        .map(|(path, _)| std::fs::read_to_string(path));
    drop(root);
    diagnostic_owner("owner_cleanup", &root_path, Some(true));
    #[cfg(unix)]
    cleanup_tests::after_scratch_release(nested);
    let status = outcome.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(
        status.is_some_and(|s| s.success()),
        "boundary child failed: {status:?}\n{output}"
    );
    assert!(
        output.contains("1 passed"),
        "exact test did not execute: {output}"
    );
    if let Some((_, expected)) = proof {
        assert_eq!(completed.unwrap().unwrap(), expected);
    }
}

#[cfg(unix)]
#[path = "isolation_unix.rs"]
mod platform;

#[path = "cleanup_tests.rs"]
mod cleanup_tests;

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
