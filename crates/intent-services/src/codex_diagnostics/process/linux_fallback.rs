//! Private synchronous subreaper for kernels built without `CONFIG_PROC_CHILDREN`.
//!
//! Scanning procfs only finds candidates to signal. Only waitpid returning
//! ECHILD confirms cleanup; a missing file or an empty scan never does.

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

pub(crate) const OWNER_ARGUMENT: &str = "--intent-internal-probe-owner";

/// Dispatch before constructing a runtime: the subreaper must stay single-threaded.
pub(crate) fn dispatch() -> Option<ExitCode> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new(OWNER_ARGUMENT)) {
        return None;
    }
    Some(if run(&args.collect::<Vec<_>>()).is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn descriptor(value: Option<&OsString>) -> io::Result<RawFd> {
    value
        .and_then(|text| text.to_str())
        .and_then(|text| text.parse::<RawFd>().ok())
        .filter(|fd| *fd >= 5)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}

fn run(args: &[OsString]) -> io::Result<()> {
    let control = descriptor(args.first())?;
    let status = descriptor(args.get(1))?;
    if control == status || args.len() < 3 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // SAFETY: this private, single-threaded entry point owns inherited fds.
    // CLOEXEC keeps lifetime descriptors out of the provider and descendants.
    unsafe {
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) < 0
            || libc::fcntl(control, libc::F_SETFD, libc::FD_CLOEXEC) < 0
            || libc::fcntl(status, libc::F_SETFD, libc::FD_CLOEXEC) < 0
        {
            return Err(io::Error::last_os_error());
        }
        let flags = libc::fcntl(control, libc::F_GETFL);
        if flags < 0 || libc::fcntl(control, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // SAFETY: each valid descriptor is uniquely owned by this helper.
    let mut control = unsafe { std::fs::File::from_raw_fd(control) };
    let mut status = unsafe { std::fs::File::from_raw_fd(status) };
    let child = Command::new(&args[2])
        .args(&args[3..])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let leader = child.id();
    // std::process::Child does not reap on drop. This process is the sole reaper.
    drop(child);
    for fd in 0..3 {
        // SAFETY: the provider inherited these descriptors; only our copies close.
        unsafe { libc::close(fd) };
    }
    let mut reported = false;
    let mut cleanup_started = None;
    loop {
        if cleanup_started.is_none() {
            match control.read(&mut [0; 16]) {
                Ok(0) => cleanup_started = Some(Instant::now()),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        if let Some(started) = cleanup_started {
            if started.elapsed() >= Duration::from_secs(4) {
                return Err(io::ErrorKind::TimedOut.into());
            }
            signal_children(std::process::id())?;
        }
        let no_children = reap(leader, &mut reported, &mut status)?;
        if cleanup_started.is_some() && no_children {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn reap(leader: u32, reported: &mut bool, status_pipe: &mut impl Write) -> io::Result<bool> {
    loop {
        let mut status = 0;
        // SAFETY: this single-threaded helper owns and reaps only its children.
        let pid = unsafe { libc::waitpid(-1, &raw mut status, libc::WNOHANG) };
        match pid.cmp(&0) {
            std::cmp::Ordering::Greater => {
                if u32::try_from(pid).ok() == Some(leader) {
                    let code = if libc::WIFEXITED(status) {
                        libc::WEXITSTATUS(status)
                    } else {
                        128 + libc::WTERMSIG(status)
                    };
                    if let Err(error) = writeln!(status_pipe, "{code:03}") {
                        // Cancellation may close the status reader before the
                        // control EOF arrives. It must not abandon descendants.
                        if error.kind() != io::ErrorKind::BrokenPipe {
                            return Err(error);
                        }
                    }
                    *reported = true;
                }
            }
            std::cmp::Ordering::Equal => {
                return Ok(false);
            }
            std::cmp::Ordering::Less => {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::ECHILD) if *reported => return Ok(true),
                    Some(libc::EINTR) => {}
                    _ => return Err(error),
                }
            }
        }
    }
}

fn parent_pid(stat: &str) -> Option<u32> {
    stat.rsplit_once(") ")?
        .1
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn signal_children(owner: u32) -> io::Result<()> {
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        if pid <= 1 {
            continue;
        }
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) =>
            {
                continue
            }
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => continue,
            Err(error) => return Err(error),
        };
        if parent_pid(&stat) == Some(owner) {
            // No waitpid runs between reading ownership and sending the signal.
            // Even if the child exits, its unreaped zombie pins the PID, so this
            // cannot signal an unrelated process after PID reuse. Adopted children
            // missed by this scan remain waitable and prevent an ECHILD verdict.
            if unsafe { libc::kill(pid, libc::SIGKILL) } < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_parent_with_spaces_and_parentheses_in_name() {
        assert_eq!(parent_pid("20 (name ) with spaces) S 12 4 5"), Some(12));
        assert_eq!(parent_pid("20 (name) Z 12 4 5"), Some(12));
        assert_eq!(parent_pid("malformed"), None);
        assert_eq!(parent_pid("20 (name) S nope"), None);
    }

    #[test]
    fn rejects_standard_and_invalid_descriptors() {
        for fd in ["0", "1", "2", "3", "4", "-1", "bad"] {
            assert!(descriptor(Some(&OsString::from(fd))).is_err());
        }
        assert_eq!(descriptor(Some(&OsString::from("11"))).unwrap(), 11);
    }
}
