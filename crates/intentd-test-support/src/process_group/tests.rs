use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use super::{is_running, ProcessGroup};

fn bound() -> Duration {
    crate::test_timeout(Duration::from_secs(5))
}

/// Poll `cond` up to [`bound()`]; panic with `what` when it never holds.
#[track_caller]
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

/// Start `script` as a detached background job in `group`, the way a sandbox
/// bootstrap starts its daemon: the shell that started it exits at once, so
/// nobody in this process parents it. Returns the job's pid.
fn detached_member(group: &ProcessGroup, script: &str) -> u32 {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("( {script} ) >/dev/null 2>&1 </dev/null & echo $!"))
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    group.join(&mut cmd);
    let out = cmd.output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("job pid on stdout")
}

/// Start a member that parents a `sleep` in a process group of its own, the
/// way a sandbox daemon starts each agent (`process_group(0)`), and keeps
/// running as `member_script` says. Returns the member's and the escaped
/// child's pids.
fn member_with_escaped_child(group: &ProcessGroup, member_script: &str) -> (u32, u32) {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("escaped.pid");
    let member = detached_member(
        group,
        &format!(
            "perl -e 'setpgrp(0, 0); exec @ARGV' sleep 600 & echo $! >'{}'; {member_script}",
            pid_file.display()
        ),
    );
    let mut escaped = None;
    wait_until("the escaped child to report its pid", || {
        escaped = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok());
        escaped.is_some()
    });
    let escaped = escaped.unwrap();
    wait_until("the escaped child to leave the group", || {
        !group.members().contains(&escaped)
    });
    (member, escaped)
}

#[test]
fn drop_ends_a_member_nobody_parents() {
    let group = ProcessGroup::new().unwrap();
    let member = detached_member(&group, "exec sleep 600");
    assert!(is_running(member));
    assert!(group.members().contains(&member));

    drop(group);

    wait_until("the detached member to exit", || !is_running(member));
}

#[test]
fn terminate_returns_once_members_exit_on_sigterm_even_when_stopped() {
    let group = ProcessGroup::new().unwrap();
    let running = detached_member(&group, "exec sleep 600");
    let paused = detached_member(&group, "exec sleep 600");
    kill(Pid::from_raw(paused.cast_signed()), Signal::SIGSTOP).unwrap();
    let group = Arc::new(group);
    let (done, finished) = mpsc::channel();
    let terminating = Arc::clone(&group);
    // A grace far beyond the hang guard: returning within the guard means
    // terminate stopped waiting once the members had exited.
    thread::spawn(move || {
        terminating.terminate(Duration::from_secs(3600));
        let _ = done.send(());
    });

    finished
        .recv_timeout(bound())
        .expect("terminate returns once its members exit on SIGTERM");
    assert!(!is_running(running) && !is_running(paused));
    assert!(group.members().is_empty());
}

#[test]
fn terminate_kills_a_member_that_ignores_sigterm_after_the_grace() {
    let group = ProcessGroup::new().unwrap();
    let stubborn = detached_member(&group, "trap '' TERM; while :; do sleep 1; done");
    let grace = Duration::from_millis(500);

    let started = Instant::now();
    group.terminate(grace);

    assert!(started.elapsed() >= grace);
    wait_until("the member that ignores SIGTERM to die", || {
        !is_running(stubborn)
    });
    // Terminated once, the group is done: a second call signals nothing.
    group.terminate(grace);
    group.kill();
}

#[test]
fn kill_ends_children_that_left_the_group() {
    let group = ProcessGroup::new().unwrap();
    let (member, escaped) = member_with_escaped_child(&group, "wait");

    group.kill();

    wait_until("the member and its escaped child to die", || {
        !is_running(member) && !is_running(escaped)
    });
}

#[test]
fn terminate_ends_children_that_left_the_group_once_the_grace_runs_out() {
    let group = ProcessGroup::new().unwrap();
    let (member, escaped) =
        member_with_escaped_child(&group, "trap '' TERM; while :; do sleep 1; done");

    group.terminate(Duration::from_millis(500));

    wait_until("the member and its escaped child to die", || {
        !is_running(member) && !is_running(escaped)
    });
}
