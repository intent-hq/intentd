//! Keep the group leader unreaped until all group members have stopped. The
//! shared `GuardedChild` deliberately relinquishes group ownership after reap,
//! which is too early for this fixture's leader-exits-before-Git failure path.
use super::*;
use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::{getpgrp, getpid, Pid};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::time::Instant;
use sysinfo::{ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

pub(super) struct Child {
    // raw-child: allow — waitid keeps the group leader owned until bounded cleanup
    process: std::process::Child,
    nested: bool,
    cleaned: bool,
}

impl Child {
    pub(super) fn spawn(command: &mut Command, nested: bool) -> Self {
        if nested {
            assert_eq!(getpgrp(), getpid(), "controller must own its group");
        } else {
            command.process_group(0);
        }
        Self {
            process: command.spawn().unwrap(),
            nested,
            cleaned: false,
        }
    }

    pub(super) fn start(&mut self) {
        self.process.stdin.take().unwrap().write_all(&[1]).unwrap();
    }

    fn observe(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if self.nested {
            // The controller, not this nested worker, anchors the group.
            return self.process.try_wait();
        }
        // SAFETY: zero initializes the no-event case; waitid writes a valid
        // siginfo for our specific child. WNOWAIT reserves its PID even after
        // exit, so group cleanup cannot signal a reaped/reused leader identity.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                self.process.id(),
                &raw mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: waitid initialized these fields (zero means no event).
        if unsafe { info.si_pid() } == 0 {
            return Ok(None);
        }
        let code = unsafe { info.si_status() };
        let status = match info.si_code {
            libc::CLD_EXITED => code << 8,
            libc::CLD_KILLED => code,
            libc::CLD_DUMPED => code | 0x80,
            _ => return Err(std::io::Error::other("unexpected waitid event")),
        };
        Ok(Some(ExitStatus::from_raw(status)))
    }

    pub(super) fn wait(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.observe().unwrap() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            // timing-guard: bounded exit observation, without reaping the leader
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub(super) fn cleanup(&mut self) -> bool {
        if self.cleaned {
            return true;
        }
        if self.nested {
            if matches!(self.process.try_wait(), Ok(Some(_))) {
                // The outer owner will sweep the entire group before reaping
                // this still-live controller, on both success and failure.
                self.cleaned = true;
                return true;
            }
            // This controller is itself the live group anchor. The outer
            // owner will observe our exit without reaping and finish cleanup.
            let _ = killpg(getpgrp(), Signal::SIGKILL);
            return false;
        }
        let pid = Pid::from_raw(self.process.id().cast_signed());
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut system = System::new();
        loop {
            // ECHILD means ownership was lost: never signal a numeric PID then.
            let Ok(exited) = self.observe() else {
                return false;
            };
            let _ = killpg(pid, Signal::SIGKILL);
            let _ = kill(pid, Signal::SIGKILL);
            system.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing(),
            );
            let live = system.processes().iter().any(|(id, process)| {
                // SAFETY: read-only identity lookup while the leader PID is
                // reserved. D-state/unknown processes are still considered live.
                (unsafe { libc::getpgid(id.as_u32().cast_signed()) }) == pid.as_raw()
                    && process.status() != ProcessStatus::Zombie
            });
            if exited.is_some() && !live {
                // Only now may wait release the leader's PID/group identity.
                if self.process.wait().is_ok() {
                    self.cleaned = true;
                    return true;
                }
                return false;
            }
            if Instant::now() >= deadline {
                return false;
            }
            // timing-guard: acknowledge group termination before reap/scratch removal
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}
