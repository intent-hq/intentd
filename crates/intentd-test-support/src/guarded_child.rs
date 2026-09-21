use std::io;
use std::ops::{Deref, DerefMut};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;

/// A `Child` spawned as the leader of its own process group and torn down
/// with that whole group if dropped while still running. A test that parks
/// its fake daemon on a [`Barrier`](crate::Barrier) and panics before
/// releasing it would otherwise drop a plain `Child` (no kill on drop) and
/// the `TempDir` holding the release file, leaving the process and its
/// parked children behind forever. Derefs to `Child` for the existing
/// helpers (`id()`, `try_wait()`, `kill()`, the stdio handles, ...).
///
/// Drop signals only while `try_wait()` still reports the child as running;
/// see the crate docs for the reaped-pid caveat. It kills the group and then
/// the child pid itself, so a child that left its group (`setpgid`) still
/// dies instead of parking `Drop` in `wait()` behind a failed `killpg`.
pub struct GuardedChild {
    /// `None` only once [`GuardedChild::disarm`] has moved the child out,
    /// which leaves `Drop` nothing to tear down; every other path sees `Some`.
    child: Option<Child>,
}

/// The `Some` invariant [`GuardedChild::child`] documents.
const CHILD_TAKEN: &str = "GuardedChild holds its child until `disarm` consumes the guard";

impl GuardedChild {
    /// Spawn `cmd` in its own process group (`setpgid(0, 0)` in the child)
    /// and guard it.
    ///
    /// # Errors
    ///
    /// Any error from [`Command::spawn`].
    pub fn spawn(cmd: &mut Command) -> io::Result<Self> {
        let child = cmd.process_group(0).spawn()?;
        Ok(Self { child: Some(child) })
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

    /// Hand the `Child` back and skip the kill on drop: the caller now owns
    /// its teardown.
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
        // `disarm` took the child: nothing left to tear down.
        let Some(child) = self.child.as_mut() else {
            return;
        };
        // Only while the child is still alive does its pid still name the
        // group (and cannot have been reused); a reaped child is the test's
        // own business.
        if matches!(child.try_wait(), Ok(None)) {
            let pid = Pid::from_raw(child.id().cast_signed());
            // A leader may have joined another group (`setpgid`), leaving its
            // own group empty: `killpg` then fails (ESRCH) and `wait()` would
            // block on a live child. The unreaped pid is still ours to signal,
            // so kill it directly as well; the errors carry nothing to act on.
            let _ = killpg(pid, Signal::SIGKILL);
            let _ = kill(pid, Signal::SIGKILL);
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests;
