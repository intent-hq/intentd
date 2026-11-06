use std::io;
use std::ops::{Deref, DerefMut};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;

/// A `Child` spawned as the leader of its own process group and torn down
/// with that whole group on drop. A test that parks its fake daemon on a
/// [`Barrier`](crate::Barrier) and panics before releasing it would otherwise
/// drop a plain `Child` (no kill on drop) and the `TempDir` holding the
/// release file, leaving the process and its parked children behind forever.
/// Derefs to `Child` for the existing helpers (`id()`, `try_wait()`, `kill()`,
/// the stdio handles, ...).
///
/// Drop sweeps the group for as long as the kernel still owns the child:
/// while it runs, and also once it has exited but nobody has reaped it yet —
/// a test that SIGKILLs only the leader on purpose (to orphan its daemon)
/// still gets that daemon cleaned up; Drop used to reap such a leader first
/// and then skip the sweep, leaking one immortal fake per run. After the test
/// reaps the child itself, Drop signals nothing; see the crate docs for the
/// reaped-pid caveat. It kills the group and then the child pid itself, so a
/// child that left its group (`setpgid`) still dies instead of parking
/// `Drop` in `wait()` behind a failed `killpg`.
///
/// `Drop` never runs when the test process itself dies — a nextest timeout
/// (which signals only the test's own process group, not this one), a
/// SIGKILL, ctrl-c. For that, every guarded group also holds a watchdog: a
/// tiny `sh` that joins the group and blocks reading a pipe whose only write
/// end this process holds. When the process dies, however it dies, the
/// kernel closes that end, the read hits EOF, and the watchdog SIGKILLs its
/// group — itself included. Its membership also keeps the group id from
/// being reused before it fires. `Drop` and [`GuardedChild::disarm`] stop the
/// watchdog, so a finished test leaves nothing behind either way.
pub struct GuardedChild {
    /// `None` only once [`GuardedChild::disarm`] has moved the child out,
    /// which leaves `Drop` nothing to tear down; every other path sees `Some`.
    child: Option<Child>,
    /// `None` when the watchdog could not join the group (the child had
    /// already left it or exited) or once it has been stopped.
    watchdog: Option<Watchdog>,
}

/// Kills its process group once the test process dies (see [`GuardedChild`]).
struct Watchdog {
    process: Child,
    /// The write end the watchdog reads EOF from once this process is gone.
    /// Never written; `std` pipes are close-on-exec, so no other child
    /// inherits it and keeps the read end from seeing EOF.
    _lifeline: io::PipeWriter,
}

impl Watchdog {
    /// Spawn a watchdog into process group `group`; `None` if it cannot join.
    fn spawn(group: u32) -> Option<Self> {
        let (reader, lifeline) = io::pipe().ok()?;
        let process = Command::new("/bin/sh")
            .args(["-c", "read -r _; kill -KILL 0"])
            .stdin(Stdio::from(reader))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(i32::try_from(group).ok()?)
            .spawn()
            .ok()?;
        Some(Self {
            process,
            _lifeline: lifeline,
        })
    }

    /// Kill and reap the watchdog: our unreaped child, so its pid is ours.
    fn stop(mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// The `Some` invariant [`GuardedChild::child`] documents.
const CHILD_TAKEN: &str = "GuardedChild holds its child until `disarm` consumes the guard";

impl GuardedChild {
    /// Spawn `cmd` in its own process group (`setpgid(0, 0)` in the child)
    /// and guard it, with a watchdog in that group for when the test process
    /// dies before `Drop` runs (see [`GuardedChild`]).
    ///
    /// # Errors
    ///
    /// Any error from [`Command::spawn`].
    pub fn spawn(cmd: &mut Command) -> io::Result<Self> {
        let child = cmd.process_group(0).spawn()?;
        // Best effort: a child that already left its group (or exited and
        // took the group with it) has nothing for a watchdog to join.
        let watchdog = Watchdog::spawn(child.id());
        Ok(Self {
            child: Some(child),
            watchdog,
        })
    }

    /// Poll `try_wait()` until the child exits or `timeout` elapses; `None`
    /// on timeout (the child keeps running and stays guarded).
    ///
    /// # Errors
    ///
    /// Any error from [`Child::try_wait`].
    pub fn wait_with_timeout(&mut self, timeout: Duration) -> io::Result<Option<ExitStatus>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            // timing-guard: poll interval
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Send `signal` to the child process itself.
    ///
    /// # Errors
    ///
    /// Any error from `kill(2)`, e.g. `ESRCH` once the child is gone.
    pub fn signal(&self, signal: Signal) -> nix::Result<()> {
        kill(self.pid(), signal)
    }

    /// Send `signal` to the child's whole process group.
    ///
    /// # Errors
    ///
    /// Any error from `killpg(2)`, e.g. `ESRCH` once the group is gone.
    pub fn signal_group(&self, signal: Signal) -> nix::Result<()> {
        killpg(self.pid(), signal)
    }

    /// Force-kill the whole process tree, then reap the child: what `Drop`
    /// does, available while the guard is still in use. A test that gives up
    /// on its child must use this instead of `child.kill()` + `child.wait()`:
    /// that pair kills only the leader and then reaps it, after which `Drop`
    /// can no longer signal the group (the reaped-pid caveat), so a sitter's
    /// fake daemon outlived every timed-out sitter test this way. Like
    /// `Drop`, it also sweeps the group of a leader that exited but is not
    /// reaped yet, and signals nothing once the child has been reaped (see
    /// the crate docs for the reaped-pid caveat), just returning its status.
    ///
    /// # Errors
    ///
    /// Any error from [`Child::wait`].
    #[expect(
        clippy::missing_panics_doc,
        reason = "the child is only taken by `disarm`, which consumes the guard"
    )]
    pub fn kill_tree(&mut self) -> io::Result<ExitStatus> {
        let child = self.child.as_mut().expect(CHILD_TAKEN);
        if kernel_owns(child) {
            kill_group_and_leader(child);
        }
        // Reaps it, or returns the status an earlier reap recorded.
        child.wait()
    }

    /// Hand the `Child` back and skip the kill on drop: the caller now owns
    /// its teardown (the watchdog is stopped too).
    #[must_use]
    #[expect(
        clippy::missing_panics_doc,
        reason = "the child is only ever taken here, on the consuming call"
    )]
    pub fn disarm(mut self) -> Child {
        // Taking the child leaves the guard's `Drop` (the kill) nothing to do.
        self.child.take().expect(CHILD_TAKEN)
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(self.id().cast_signed())
    }
}

impl Deref for GuardedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        self.child.as_ref().expect(CHILD_TAKEN)
    }
}

impl DerefMut for GuardedChild {
    fn deref_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect(CHILD_TAKEN)
    }
}

impl Drop for GuardedChild {
    fn drop(&mut self) {
        // Only while the kernel still owns the child (running, or exited and
        // unreaped) does its pid still name the group and cannot have been
        // reused; a reaped child is the test's own business. `disarm` took
        // the child: nothing left to tear down but the watchdog.
        if let Some(child) = self.child.as_mut() {
            if kernel_owns(child) {
                kill_group_and_leader(child);
                let _ = child.wait();
            }
        }
        // Last, so it pins the group id through the sweep above.
        if let Some(watchdog) = self.watchdog.take() {
            watchdog.stop();
        }
    }
}

/// Whether the kernel still holds `child` for us: running, or exited but not
/// yet reaped. See [`peek_unreaped`].
fn kernel_owns(child: &Child) -> bool {
    peek_unreaped(child.id()).is_some()
}

/// Peek at our direct child `pid` with `waitid(WNOWAIT)`, which never reaps,
/// so the pid — and with it the process-group id — stays reserved until we
/// `wait()`: `None` once nobody owns it any more (`std` reaped it with
/// `try_wait()` / `wait()`, so the peek fails with `ECHILD`), otherwise
/// `Some(exited)`. The same peek guards the production sitter's session
/// cleanup (`child_exited` in intentd-sitter's `startup.rs`); `nix`'s
/// `waitid` wrapper is not available on macOS.
fn peek_unreaped(pid: u32) -> Option<bool> {
    use nix::libc;
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: `info` points to writable, zeroed siginfo_t storage. P_PID
    // selects our own direct child, and WNOWAIT leaves it unreaped.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result != 0 {
        return None;
    }
    // SAFETY: the storage was zeroed and waitid succeeded. WNOHANG leaves
    // si_pid zero while the child runs; an exit event fills it on Linux and
    // macOS.
    Some(unsafe { info.assume_init().si_pid() } != 0)
}

/// `SIGKILL` the unreaped `child`'s process group, then the child itself.
fn kill_group_and_leader(child: &Child) {
    let pid = Pid::from_raw(child.id().cast_signed());
    // A leader may have joined another group (`setpgid`), leaving its
    // own group empty: `killpg` then fails (ESRCH) and `wait()` would
    // block on a live child. The unreaped pid is still ours to signal,
    // so kill it directly as well; the errors carry nothing to act on.
    let _ = killpg(pid, Signal::SIGKILL);
    let _ = kill(pid, Signal::SIGKILL);
}

#[cfg(test)]
mod tests;
