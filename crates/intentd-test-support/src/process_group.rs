use std::io;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;

/// A process group for processes the test does not parent, such as a daemon
/// a script starts with `nohup ... &` and then leaves: the test spawns those
/// scripts into the group ([`ProcessGroup::join`]), and everything they leave
/// behind stays in it.
///
/// The group is led by an anchor, a tiny `sh` that ignores `SIGTERM` and
/// blocks reading a pipe whose only write end this process holds. When this
/// process dies, however it dies (a nextest timeout, SIGKILL, ctrl-c), the
/// read hits EOF and the anchor sweeps the group (see below), itself
/// included. The anchor is this process's unreaped child until the group is
/// torn down, so the group id cannot be reused while it can still be
/// signalled.
///
/// [`ProcessGroup::terminate`] (and `Drop`) asks the members to stop
/// (`SIGCONT`, then `SIGTERM`), waits until they have, and sweeps whatever is
/// left. A sweep freezes (`SIGSTOP`) the members and every descendant of
/// theirs, until no new one appears, then SIGKILLs them all: a member's
/// child in a process group of its own (a daemon's agents) dies with it,
/// while its parent still names it. A process whose parent already exited
/// and that left the group (a script's `setsid ... &`) is out of reach:
/// give such scripts a `setsid` that stays in the group.
pub struct ProcessGroup {
    pgid: i32,
    /// `None` once the group has been torn down.
    anchor: Mutex<Option<Anchor>>,
}

struct Anchor {
    process: Child,
    /// Never written; `std` pipes are close-on-exec, so no member inherits
    /// it and keeps the anchor's read from seeing EOF.
    _lifeline: io::PipeWriter,
}

/// Freeze, then SIGKILL, the members of process group `$1` (not its leader,
/// nor the leader's own children such as this sweep's tools) and every
/// descendant of theirs, in any group. Repeats the freeze until the set
/// stops growing, so nothing forks a child the sweep has not seen.
const SWEEP_SCRIPT: &str = r#"g=$1
tree() {
  ps -A -o pid= -o ppid= -o pgid= | awk -v g="$g" '
    { pid[NR] = $1; ppid[NR] = $2; pgid[NR] = $3 }
    END {
      own[g] = 1
      do {
        grown = 0
        for (i = 1; i <= NR; i++)
          if (!(pid[i] in own) && (ppid[i] in own)) { own[pid[i]] = 1; grown = 1 }
      } while (grown)
      for (i = 1; i <= NR; i++)
        if (pgid[i] == g && !(pid[i] in own)) keep[pid[i]] = 1
      do {
        grown = 0
        for (i = 1; i <= NR; i++)
          if (!(pid[i] in keep) && !(pid[i] in own) && (ppid[i] in keep)) { keep[pid[i]] = 1; grown = 1 }
      } while (grown)
      for (p in keep) print p
    }' | sort -n | tr '\n' ' '
}
prev=-
cur=
i=0
while [ "$i" -lt 50 ]; do
  cur=$(tree)
  [ -n "$cur" ] && kill -STOP $cur 2>/dev/null
  [ "$cur" = "$prev" ] && break
  prev=$cur
  i=$((i + 1))
done
[ -n "$cur" ] && kill -KILL $cur 2>/dev/null
:
"#;

/// The anchor: ignores the `SIGTERM` (and terminal signals) that
/// [`ProcessGroup::terminate`] sends the group, so the group id stays pinned
/// while members shut down; sweeps the group once this process is gone.
fn anchor_script() -> String {
    format!("trap '' TERM HUP INT\nread -r _\nset -- $$\n{SWEEP_SCRIPT}\nkill -KILL 0\n")
}

/// Run [`SWEEP_SCRIPT`] over group `pgid` and wait for it.
fn sweep(pgid: i32) {
    let _ = Command::new("/bin/sh")
        .args(["-c", SWEEP_SCRIPT, "sh", &pgid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

impl ProcessGroup {
    /// A new, empty group (its anchor only).
    ///
    /// # Errors
    ///
    /// Any error from creating the pipe or spawning the anchor.
    pub fn new() -> io::Result<Self> {
        let (reader, lifeline) = io::pipe()?;
        let process = Command::new("/bin/sh")
            .args(["-c", &anchor_script()])
            .stdin(Stdio::from(reader))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()?;
        let pgid = i32::try_from(process.id()).map_err(io::Error::other)?;
        Ok(Self {
            pgid,
            anchor: Mutex::new(Some(Anchor {
                process,
                _lifeline: lifeline,
            })),
        })
    }

    /// The process group id, for `process_group` on a `tokio` command.
    #[must_use]
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// Spawn `cmd` into this group.
    pub fn join<'a>(&self, cmd: &'a mut Command) -> &'a mut Command {
        cmd.process_group(self.pgid)
    }

    /// The live members (not the anchor, not zombies).
    #[must_use]
    pub fn members(&self) -> Vec<u32> {
        let anchor = u32::try_from(self.pgid).unwrap_or_default();
        group_members(self.pgid)
            .into_iter()
            .filter(|pid| *pid != anchor)
            .collect()
    }

    /// Ask every member to stop (`SIGCONT`, then `SIGTERM`), wait up to
    /// `grace` until none is left, then sweep the group and SIGKILL it,
    /// anchor included.
    /// Once torn down (here or by [`ProcessGroup::kill`]) the group is done
    /// and later calls signal nothing.
    pub fn terminate(&self, grace: Duration) {
        let Some(mut anchor) = self.take_anchor() else {
            return;
        };
        let group = Pid::from_raw(self.pgid);
        let _ = killpg(group, Signal::SIGCONT);
        let _ = killpg(group, Signal::SIGTERM);
        let deadline = Instant::now() + grace;
        while !self.members().is_empty() && Instant::now() < deadline {
            // timing-guard: poll interval while members shut down
            thread::sleep(Duration::from_millis(20));
        }
        sweep(self.pgid);
        let _ = killpg(group, Signal::SIGKILL);
        let _ = anchor.process.wait();
    }

    /// Sweep and SIGKILL the whole group at once, as when its machine
    /// disappears.
    pub fn kill(&self) {
        if let Some(mut anchor) = self.take_anchor() {
            sweep(self.pgid);
            let _ = killpg(Pid::from_raw(self.pgid), Signal::SIGKILL);
            let _ = anchor.process.wait();
        }
    }

    fn take_anchor(&self) -> Option<Anchor> {
        self.anchor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.terminate(crate::test_timeout(Duration::from_secs(5)));
    }
}

/// Whether `pid` names a live process: running or stopped, not a zombie
/// waiting for its parent to reap it.
#[must_use]
pub fn is_running(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| stat_fields(&stat).map(|(state, _)| state != "Z"))
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        i32::try_from(pid).is_ok_and(|pid| nix::sys::signal::kill(Pid::from_raw(pid), None).is_ok())
    }
}

/// `(state, pgrp)` from a `/proc/<pid>/stat` line; the command name may hold
/// spaces and parentheses, so fields are counted after its last `)`.
#[cfg(target_os = "linux")]
fn stat_fields(stat: &str) -> Option<(&str, i32)> {
    let mut fields = stat.rsplit_once(')')?.1.split_whitespace();
    let state = fields.next()?;
    let pgrp = fields.nth(1)?.parse().ok()?;
    Some((state, pgrp))
}

#[cfg(target_os = "linux")]
fn group_members(pgid: i32) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| {
                    stat_fields(&stat).map(|(state, pgrp)| state != "Z" && pgrp == pgid)
                })
                .unwrap_or(false)
        })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn group_members(pgid: i32) -> Vec<u32> {
    Command::new("pgrep")
        .args(["-g", &pgid.to_string()])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .filter_map(|pid| pid.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
