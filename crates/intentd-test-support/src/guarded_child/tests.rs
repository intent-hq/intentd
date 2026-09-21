use std::io::{self, BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::panic::{self, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::sys::signal::{kill, killpg};
use nix::unistd::{getpgid, getpgrp, setpgid, Pid};

use super::GuardedChild;
use crate::Barrier;

const BOUND: Duration = Duration::from_secs(5);

/// Whether `pid` still names a process (signal 0 probe), zombies included.
fn alive(pid: u32) -> bool {
    match kill(Pid::from_raw(pid.cast_signed()), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(e) => panic!("kill({pid}, 0): {e}"),
    }
}

/// Poll `cond` up to [`BOUND`]; panic with `what` when it never holds.
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        // timing-guard: poll interval
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out after {BOUND:?} waiting for {what}");
}

/// Every child detaches its stdio so nextest's leak detector never sees an
/// inherited capture pipe (intent-hq/intent#4284).
fn detached(program: &str) -> Command {
    let mut cmd = Command::new(program);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

fn sh(script: &str) -> Command {
    let mut cmd = detached("sh");
    cmd.arg("-c").arg(script);
    cmd
}

/// A shell that parks a `sleep 60` grandchild and reports its pid on stdout.
fn spawn_with_grandchild() -> (GuardedChild, u32) {
    let mut cmd = sh("sleep 60 & echo $! ; wait");
    cmd.stdout(Stdio::piped());
    let mut guard = GuardedChild::spawn(&mut cmd).unwrap();
    let stdout = guard.stdout.take().unwrap();
    let mut line = String::new();
    BufReader::new(stdout).read_line(&mut line).unwrap();
    let grandchild: u32 = line.trim().parse().expect("grandchild pid on stdout");
    (guard, grandchild)
}

#[test]
fn drop_kills_child_and_grandchild() {
    let (guard, grandchild) = spawn_with_grandchild();
    let child = guard.id();
    assert!(alive(child) && alive(grandchild));

    drop(guard);

    wait_until("child and grandchild to be gone", || {
        !alive(child) && !alive(grandchild)
    });
}

#[test]
fn panic_after_spawn_still_reaps() {
    let pids = Mutex::new(None);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        let (guard, grandchild) = spawn_with_grandchild();
        *pids.lock().unwrap() = Some((guard.id(), grandchild));
        panic!("injected mid-test panic while the guard is live");
    }));
    assert!(outcome.is_err(), "the injected panic must unwind");
    let (child, grandchild) = pids
        .into_inner()
        .unwrap()
        .expect("spawned before panicking");

    wait_until("child and grandchild to be gone after unwinding", || {
        !alive(child) && !alive(grandchild)
    });
}

#[test]
fn disarm_leaves_child_running() {
    let guard = GuardedChild::spawn(detached("sleep").arg("60")).unwrap();
    let pid = guard.id();

    let mut child = guard.disarm();
    assert!(alive(pid), "disarmed child must still be running");

    child.kill().unwrap();
    child.wait().unwrap();
    wait_until("manually killed child to be gone", || !alive(pid));
}

#[test]
fn drop_kills_child_that_left_its_group() {
    // The child joins the test's own group after `spawn` made it a leader
    // (`process_group(0)` runs before `pre_exec`), so its original group is
    // empty and `killpg(pid, SIGKILL)` fails with ESRCH — the reviewer's
    // repro for intentd#1928, where Drop then blocked in `wait()`.
    let target = getpgrp();
    let mut cmd = detached("sleep");
    cmd.arg("60");
    // SAFETY: `setpgid` is async-signal-safe and touches no locks or heap
    // state, so it is safe to call between fork and exec.
    unsafe {
        cmd.pre_exec(move || setpgid(Pid::from_raw(0), target).map_err(io::Error::from));
    }
    let guard = GuardedChild::spawn(&mut cmd).unwrap();
    let pid = guard.id();
    let nix_pid = Pid::from_raw(pid.cast_signed());
    assert_eq!(
        getpgid(Some(nix_pid)).unwrap(),
        target,
        "child must have joined our group"
    );
    assert_eq!(
        killpg(nix_pid, None),
        Err(Errno::ESRCH),
        "the child's original group must be empty"
    );

    let dropping = thread::spawn(move || drop(guard));
    wait_until("drop to return after the group kill failed", || {
        dropping.is_finished()
    });
    dropping.join().unwrap();
    assert!(!alive(pid), "child must be dead once drop returned");
}

#[test]
fn drop_after_reap_is_noop() {
    let mut guard = GuardedChild::spawn(&mut detached("true")).unwrap();
    let status = guard.wait().unwrap();
    assert!(status.success());

    // A reaped pid may already belong to someone else: Drop must neither
    // signal nor fail.
    drop(guard);
}

#[test]
fn barrier_release_and_entered() {
    let dir = tempfile::Builder::new()
        .prefix("intentd-test-support-")
        .tempdir()
        .unwrap();
    let barrier = Barrier::new(dir.path(), "hold");
    assert_eq!(barrier.path(), dir.path().join("barrier-hold"));
    assert!(!barrier.entered());

    let script = format!("{}; {}; exit 0", barrier.sh_arrive(), barrier.sh_wait());
    let mut guard = GuardedChild::spawn(&mut sh(&script)).unwrap();

    wait_until("script to arrive at the barrier", || barrier.entered());
    assert!(
        matches!(guard.try_wait(), Ok(None)),
        "script must hold at the barrier until released"
    );

    barrier.release();
    let status = guard
        .wait_with_timeout(BOUND)
        .unwrap()
        .expect("released script exits within the bound");
    assert_eq!(status.code(), Some(0));
}
