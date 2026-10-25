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

fn bound() -> Duration {
    crate::test_timeout(Duration::from_secs(5))
}

/// Whether `pid` still names a process (signal 0 probe), zombies included.
fn alive(pid: u32) -> bool {
    match kill(Pid::from_raw(pid.cast_signed()), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(e) => panic!("kill({pid}, 0): {e}"),
    }
}

/// Poll `cond` up to [`bound()`]; panic with `what` when it never holds.
fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let bound = bound();
    let deadline = Instant::now() + bound;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        // timing-guard: poll interval
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out after {bound:?} waiting for {what}");
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

/// Regression for the leaked sitter fakes: a test that gave up on its child
/// with `kill()` + `wait()` killed only the leader and reaped it, so `Drop`
/// could no longer reach the grandchild. `kill_tree` takes the tree down
/// before reaping.
#[test]
fn kill_tree_kills_child_and_grandchild_before_reaping() {
    use std::os::unix::process::ExitStatusExt as _;

    let (mut guard, grandchild) = spawn_with_grandchild();
    let child = guard.id();
    assert!(alive(child) && alive(grandchild));

    let status = guard.kill_tree().unwrap();
    assert_eq!(
        status.signal(),
        Some(nix::sys::signal::Signal::SIGKILL as i32)
    );
    wait_until("the grandchild to be gone", || !alive(grandchild));
    assert!(!alive(child), "kill_tree reaps the child");

    // Already reaped: neither a second call nor Drop signals anything.
    assert_eq!(guard.kill_tree().unwrap(), status);
    drop(guard);
}

/// After a reap the pid may belong to someone else: `kill_tree` signals
/// nothing and hands back the status the reap recorded.
#[test]
fn kill_tree_after_reap_returns_the_exit_status() {
    let mut guard = GuardedChild::spawn(&mut sh("exit 3")).unwrap();
    let status = guard
        .wait_with_timeout(BOUND)
        .unwrap()
        .expect("`exit 3` exits within the bound");
    assert_eq!(guard.kill_tree().unwrap(), status);
    assert_eq!(status.code(), Some(3));
}

/// SIGKILL only the leader, leaving it an unreaped zombie with its
/// `sleep 60` grandchild still running in the group.
fn spawn_with_grandchild_then_kill_the_leader() -> (GuardedChild, u32) {
    let (guard, grandchild) = spawn_with_grandchild();
    guard.signal(nix::sys::signal::Signal::SIGKILL).unwrap();
    let leader = Pid::from_raw(guard.id().cast_signed());
    // Wait for the exit without reaping, as the guard's own peek does.
    wait_until("the leader to exit (unreaped)", || {
        let mut info = std::mem::MaybeUninit::<nix::libc::siginfo_t>::zeroed();
        // SAFETY: writable zeroed storage; WNOWAIT never reaps our child.
        let ok = unsafe {
            nix::libc::waitid(
                nix::libc::P_PID,
                leader.as_raw().cast_unsigned(),
                info.as_mut_ptr(),
                nix::libc::WEXITED | nix::libc::WNOHANG | nix::libc::WNOWAIT,
            )
        } == 0;
        // SAFETY: zeroed storage; si_pid stays 0 until an exit is reported.
        ok && unsafe { info.assume_init().si_pid() } != 0
    });
    assert!(alive(grandchild), "the grandchild outlives its leader");
    (guard, grandchild)
}

/// Regression for the leaked sitter fakes: a test that sent `SIGKILL` only to its
/// supervisor (to prove the daemon survives it) relied on the guard to clean
/// the daemon up, but Drop reaped the dead leader first and then skipped the
/// sweep — one immortal fake per run piled up under `systemd --user`.
#[test]
fn drop_sweeps_the_group_of_a_killed_but_unreaped_leader() {
    let (guard, grandchild) = spawn_with_grandchild_then_kill_the_leader();
    drop(guard);
    wait_until("the orphaned grandchild to be gone", || !alive(grandchild));
}

#[test]
fn kill_tree_sweeps_the_group_of_a_killed_but_unreaped_leader() {
    use std::os::unix::process::ExitStatusExt as _;

    let (mut guard, grandchild) = spawn_with_grandchild_then_kill_the_leader();
    let status = guard.kill_tree().unwrap();
    assert_eq!(
        status.signal(),
        Some(nix::sys::signal::Signal::SIGKILL as i32)
    );
    wait_until("the orphaned grandchild to be gone", || !alive(grandchild));
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
        .wait_with_timeout(bound())
        .unwrap()
        .expect("released script exits within the bound");
    assert_eq!(status.code(), Some(0));
}

/// Wait for a guarded script to exit within [`BOUND`]; panic naming `what`.
fn exit_code_within_bound(guard: &mut GuardedChild, what: &str) -> Option<i32> {
    guard
        .wait_with_timeout(BOUND)
        .unwrap()
        .unwrap_or_else(|| panic!("{what} did not exit within {BOUND:?}"))
        .code()
}

/// Regression: a test that drops its `TempDir` without releasing used to
/// leave the script polling for a release file that can never appear.
#[test]
fn barrier_wait_gives_up_once_its_directory_is_gone() {
    let dir = tempfile::Builder::new()
        .prefix("intentd-test-support-")
        .tempdir()
        .unwrap();
    let barrier = Barrier::new(dir.path(), "hold");
    let script = format!("{}; {}; exit 0", barrier.sh_arrive(), barrier.sh_wait());
    let mut guard = GuardedChild::spawn(&mut sh(&script)).unwrap();
    wait_until("script to arrive at the barrier", || barrier.entered());

    dir.close().unwrap();
    assert_eq!(
        exit_code_within_bound(&mut guard, "a hold whose directory is gone"),
        Some(Barrier::ABANDONED_EXIT_CODE)
    );
}

/// Regression for the leaked sitter fakes: the hold runs inside a USR2 trap
/// (as `idle_restart_script` does), where the shell defers its TERM trap, so
/// the barrier's own exit is the only way out once the test is gone.
#[test]
fn barrier_wait_inside_a_trap_handler_gives_up_once_its_directory_is_gone() {
    let dir = tempfile::Builder::new()
        .prefix("intentd-test-support-")
        .tempdir()
        .unwrap();
    let ready = Barrier::new(dir.path(), "ready");
    let hold = Barrier::new(dir.path(), "hold");
    let script = format!(
        "trap 'exit 0' TERM; trap '{arrive}; {wait}; exit 75' USR2; {ready}; \
         while :; do sleep 0.05; done",
        arrive = hold.sh_arrive(),
        wait = hold.sh_wait(),
        ready = ready.sh_arrive(),
    );
    let mut guard = GuardedChild::spawn(&mut sh(&script)).unwrap();
    wait_until("traps to be installed", || ready.entered());
    guard.signal(nix::sys::signal::Signal::SIGUSR2).unwrap();
    wait_until("the USR2 handler to reach the hold", || hold.entered());

    dir.close().unwrap();
    assert_eq!(
        exit_code_within_bound(&mut guard, "a trapped hold whose directory is gone"),
        Some(Barrier::ABANDONED_EXIT_CODE)
    );
}

/// A hold whose directory survives (the test process got `SIGKILL`, so its
/// `TempDir` never dropped) still gives up after its deadline.
#[test]
fn barrier_wait_gives_up_after_its_deadline() {
    let dir = tempfile::Builder::new()
        .prefix("intentd-test-support-")
        .tempdir()
        .unwrap();
    for deadline in [Duration::ZERO, Duration::from_millis(200)] {
        let barrier = Barrier::new(dir.path(), "hold").with_deadline(deadline);
        let script = format!("{}; exit 0", barrier.sh_wait());
        let mut guard = GuardedChild::spawn(&mut sh(&script)).unwrap();
        assert_eq!(
            exit_code_within_bound(&mut guard, "a hold past its deadline"),
            Some(Barrier::ABANDONED_EXIT_CODE),
            "{deadline:?}"
        );
        assert!(dir.path().is_dir() && !barrier.path().exists());
    }
}

/// The default hold deadline is the base stretched by the test multiplier,
/// so coverage runs never abandon a hold a normal run would keep.
#[test]
fn barrier_default_deadline_scales_with_the_test_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let polls = |barrier: &Barrier| {
        let wait = barrier.sh_wait();
        let (_, rest) = wait.split_once("-ge ").unwrap();
        rest.split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let expected = crate::test_timeout(crate::DEFAULT_HOLD_DEADLINE)
        .as_nanos()
        .div_ceil(Duration::from_millis(50).as_nanos());
    assert_eq!(
        u128::from(polls(&Barrier::new(dir.path(), "hold"))),
        expected
    );
    assert_eq!(
        polls(&Barrier::new(dir.path(), "hold").with_deadline(Duration::ZERO)),
        1
    );
}
