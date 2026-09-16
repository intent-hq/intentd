use std::io;
use std::mem::ManuallyDrop;
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
/// see the crate docs for the reaped-pid caveat.
pub struct GuardedChild {
    /// `ManuallyDrop` only so [`GuardedChild::disarm`] can move the child
    /// out; `Drop` for the guard is what tears it down.
    child: ManuallyDrop<Child>,
}

impl GuardedChild {
    /// Spawn `cmd` in its own process group (`setpgid(0, 0)` in the child)
    /// and guard it.
    ///
    /// # Errors
    ///
    /// Any error from [`Command::spawn`].
    pub fn spawn(cmd: &mut Command) -> io::Result<Self> {
        let child = cmd.process_group(0).spawn()?;
        Ok(Self {
            child: ManuallyDrop::new(child),
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

    /// Hand the `Child` back and skip the kill on drop: the caller now owns
    /// its teardown.
    #[must_use]
    pub fn disarm(self) -> Child {
        // Forget the guard so its `Drop` (the kill) never runs, then move the
        // child out of the forgotten shell.
        let mut this = ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped and never used again after this
        // line, so the child is moved out exactly once.
        unsafe { ManuallyDrop::take(&mut this.child) }
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(self.id().cast_signed())
    }
}

impl Deref for GuardedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}

impl DerefMut for GuardedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for GuardedChild {
    fn drop(&mut self) {
        // Only while the child is still alive does its pid still name the
        // group (and cannot have been reused); a reaped child is the test's
        // own business.
        if matches!(self.child.try_wait(), Ok(None)) {
            let pgid = Pid::from_raw(self.child.id().cast_signed());
            let _ = killpg(pgid, Signal::SIGKILL);
            let _ = self.child.wait();
        }
        // SAFETY: `drop` runs at most once and nothing reads `self.child`
        // afterwards; `disarm` forgets the guard, so it never reaches here.
        unsafe { ManuallyDrop::drop(&mut self.child) };
    }
}

#[cfg(test)]
mod tests;
