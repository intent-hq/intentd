//! Faults run only in disposable exact-test children. The descendant has its
//! own final watchdog so even a regression in the owner cannot leak it forever.
use super::*;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

const PROBE: &str =
    "repository_read_policy::tests::context_consumer::isolation::cleanup_tests::cleanup_probe";
const RECORD: &str = "INTENT_BOUNDARY_CLEANUP_RECORD";
const MODE: &str = "INTENT_BOUNDARY_CLEANUP_MODE";
const NESTED: &str = "INTENT_BOUNDARY_CLEANUP_NESTED";
const DESCENDANT: &str = "INTENT_BOUNDARY_CLEANUP_DESCENDANT";

fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "fixture readiness: {}",
            path.display()
        );
        // timing-guard: readiness handshake before injecting a failure
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn await_self_expiry() {
    let deadline = Instant::now() + Duration::from_secs(10);
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        std::thread::park_timeout(remaining);
    }
}

#[cfg(unix)]
pub(super) fn after_scratch_release(nested: bool) {
    if !nested || std::env::var(MODE).as_deref() != Ok("scratch-order") {
        return;
    }
    let record = PathBuf::from(std::env::var_os(RECORD).unwrap());
    // Hold the controller at the ownership handoff until its live descendant
    // has observed scratch. No elapsed-time race with the outer group sweep.
    std::fs::write(record.with_extension("inspect"), "inspect").unwrap();
    wait_file(&record.with_extension("ordering"));
}

pub(super) fn after_start(nested: bool) {
    let Some(record) = std::env::var_os(RECORD) else {
        return;
    };
    let mode = std::env::var(MODE).unwrap();
    if nested && mode.starts_with("controller-") {
        wait_file(Path::new(&record));
        if mode == "controller-crash" {
            std::process::exit(24);
        }
        panic!("intentional controller panic after worker readiness");
    }
}

#[test]
fn cleanup_probe() {
    let Some(record) = std::env::var_os(RECORD) else {
        return;
    };
    let record = PathBuf::from(record);
    let ready = record.with_extension("descendant-ready");
    if std::env::var_os(DESCENDANT).is_some() {
        std::fs::write(ready, "ready").unwrap();
        if std::env::var(MODE).as_deref() == Ok("scratch-order") {
            wait_file(&record.with_extension("inspect"));
            std::fs::write(
                record.with_extension("ordering"),
                std::env::temp_dir().exists().to_string(),
            )
            .unwrap();
        }
        // A bounded last-resort exit, not the expected cleanup mechanism.
        await_self_expiry();
        std::process::exit(0);
    }
    if std::env::var_os(NESTED).is_some()
        && std::env::var_os(CONTROLLER).is_none()
        && std::env::var_os(WORKER).is_none()
    {
        await_start();
        ambient_controller(PROBE);
        panic!("fault controller unexpectedly succeeded");
    }
    let Some(_completion) = enter_with_timeout(PROBE, Duration::from_secs(2)) else {
        panic!("fault worker unexpectedly succeeded");
    };
    let scratch = crate::test_support::test_tempdir("failure-scratch-");
    std::fs::write(scratch.path().join("fixture-data"), "owned scratch").unwrap();
    // This deliberately models a Git child that outlives its parent. Its group
    // owner (not this local handle) must stop it, even on process::exit/panic.
    #[expect(clippy::zombie_processes)] // Intentional orphan; the owner under test must stop it.
    let descendant = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", PROBE, "--nocapture", "--test-threads=1"])
        .env(DESCENDANT, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_file(&ready);
    let value = serde_json::json!({
        "worker":std::process::id(), "descendant":descendant.id(),
        "root":std::env::temp_dir(), "scratch":scratch.path(),
    });
    let staging = record.with_extension("pending");
    std::fs::write(&staging, value.to_string()).unwrap();
    std::fs::rename(staging, record).unwrap();
    match std::env::var(MODE).unwrap().as_str() {
        "crash" | "scratch-order" => std::process::exit(23),
        "panic" => panic!("intentional worker panic with live descendant"),
        "timeout" | "controller-crash" | "controller-panic" => {
            await_self_expiry();
            panic!("fixture owner failed to enforce its watchdog");
        }
        _ => unreachable!(),
    }
}

fn live(pids: &[u32]) -> Vec<u32> {
    let mut system = System::new();
    let ids: Vec<_> = pids.iter().copied().map(Pid::from_u32).collect();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&ids),
        true,
        ProcessRefreshKind::nothing(),
    );
    pids.iter()
        .copied()
        .filter(|pid| {
            system
                .process(Pid::from_u32(*pid))
                .is_some_and(|p| p.status() != ProcessStatus::Zombie)
        })
        .collect()
}

#[test]
fn failure_cleanup_stops_descendants_and_removes_owned_scratch() {
    for (nested, mode) in [
        (false, "crash"),
        (true, "crash"),
        (false, "panic"),
        (true, "panic"),
        (false, "timeout"),
        (true, "timeout"),
        (true, "controller-crash"),
        (true, "controller-panic"),
        #[cfg(unix)]
        (true, "scratch-order"),
    ] {
        let observer = crate::test_support::test_tempdir("context-cleanup-observer-");
        let record = observer.path().join("worker.json");
        let log_path = observer.path().join("observer.log");
        let log = File::create(&log_path).unwrap();
        let mut command = exact_test(PROBE);
        command
            .env(RECORD, &record)
            .env(MODE, mode)
            .env_remove(WORKER)
            .env_remove(PROOF)
            .env_remove(CONTROLLER)
            .env_remove(DESCENDANT)
            .env_remove(NESTED)
            .env("TMPDIR", observer.path())
            .env("TMP", observer.path())
            .env("TEMP", observer.path())
            .stdin(Stdio::piped())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if nested {
            command.env(NESTED, "1");
        }
        let mut entry = platform::Child::spawn(&mut command, false);
        entry.start();
        let status = entry.wait(Duration::from_secs(15));
        // The entry has a different group/job from the internal fixture owner.
        // Inspect internal descendant/scratch state BEFORE this observer drops.
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
        let pids = [
            value["worker"].as_u64().unwrap().try_into().unwrap(),
            value["descendant"].as_u64().unwrap().try_into().unwrap(),
        ];
        let survivors = live(&pids);
        let leaked = Path::new(value["root"].as_str().unwrap()).exists()
            || Path::new(value["scratch"].as_str().unwrap()).exists();
        let cleaned = entry.cleanup();
        drop(entry);
        if !survivors.is_empty() {
            // Failing regression hygiene: acknowledge the fixture's independent
            // self-expiry without signaling possibly reaped/reused numeric PIDs.
            let deadline = Instant::now() + Duration::from_secs(12);
            while !live(&pids).is_empty() && Instant::now() < deadline {
                // timing-guard: bounded cleanup of the intentionally failing fixture
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let output = std::fs::read_to_string(log_path).unwrap();
        assert!(
            status.is_some_and(|s| !s.success()),
            "{nested}/{mode}: {status:?}\n{output}"
        );
        assert!(cleaned, "observer cleanup {nested}/{mode}");
        if mode == "scratch-order" {
            assert_eq!(
                std::fs::read_to_string(record.with_extension("ordering")).unwrap(),
                "true",
                "scratch removed while descendant was alive"
            );
        }
        assert!(
            survivors.is_empty(),
            "live descendants {nested}/{mode}: {survivors:?}\n{output}"
        );
        assert!(
            !leaked,
            "leftover scratch {nested}/{mode}: {value}\n{output}"
        );
    }
}
