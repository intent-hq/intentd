//! Detached startup of the ordinary supervisor. Startup callers serialize on a
//! separate lock; the serve lifetime lock remains the authority for ownership.
//! Only the child created by this invocation may be stopped on failure.

use crate::cli::SitterArgs;
use crate::paths::SitterPaths;

/// Internal marker: fail an initial launch rather than entering crash recovery
/// before the first readiness response. Removed from the daemon environment.
pub const STARTING_ENV: &str = "INTENTD_SITTER_STARTING";

/// Start a supervised daemon and return only after it answers system.status.
#[must_use]
pub fn run(args: &SitterArgs, paths: &SitterPaths) -> i32 {
    match start(args, paths) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!(
                "intentd-sitter: {error}; inspect {}",
                paths.sitter_dir.join("start.log").display()
            );
            1
        }
    }
}

fn start(args: &SitterArgs, paths: &SitterPaths) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    use crate::{readiness, state, supervisor};

    let mut serve_args = args.passthrough.clone();
    serve_args[0] = "serve".into();
    // Help is a foreground one-shot, even when a daemon is already running.
    if serve_args
        .iter()
        .skip(1)
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "--help" || arg == "-h")
    {
        let version = state::load(&paths.state_path)
            .current_version
            .ok_or_else(|| {
                io::Error::other("no daemon installed; run intentd serve to install it first")
            })?;
        let status = Command::new(paths.daemon_binary(&version))
            .args(&serve_args)
            .status()?;
        return if status.success() {
            Ok(())
        } else {
            Err(io::Error::other("invalid start options"))
        };
    }

    let deadline = Instant::now() + readiness::timeout_from_env();
    std::fs::create_dir_all(&paths.sitter_dir)?;
    let _lock = lock_start(paths, deadline)?;
    if probe(paths, deadline) {
        println!("intentd is already running; launch options apply on the next start");
        return Ok(());
    }
    // An existing serve may still be booting or restarting. Never stop it and
    // never launch a second supervisor merely because readiness is delayed.
    if supervisor::read_live_pid(&paths.pid_path).is_some() {
        loop {
            if probe(paths, deadline) {
                println!("intentd is running");
                return Ok(());
            }
            if Instant::now() >= deadline || supervisor::read_live_pid(&paths.pid_path).is_none() {
                return Err(io::Error::other("existing supervisor did not become ready"));
            }
            pause(deadline);
        }
    }
    if Instant::now() >= deadline {
        return Err(io::Error::other("startup readiness timed out"));
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);
    let log = options.open(paths.sitter_dir.join("start.log"))?;
    let mut command = Command::new(std::env::current_exe()?);
    // Put sitter flags before passthrough, including a possible bare `--`.
    // Config/default selections must keep following live channel changes.
    if let Some(channel) = args.channel {
        command.arg(format!("--sitter-channel={}", channel.channel));
    }
    command
        .args(&serve_args)
        .env(STARTING_ENV, "1")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    detach(&mut command);
    let mut child = command.spawn()?;
    let outcome = (|| loop {
        if child_exited(&child)? {
            break Err(io::Error::other("supervisor exited before readiness"));
        }
        if probe(paths, deadline) {
            // Avoid accepting a response from a child that has already exited.
            if !child_exited(&child)?
                && supervisor::read_live_pid(&paths.pid_path)
                    .is_some_and(|pid| pid.as_raw() == child.id().cast_signed())
            {
                println!(
                    "intentd started; logs: {}",
                    paths.sitter_dir.join("start.log").display()
                );
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            break Err(io::Error::other("startup readiness timed out"));
        }
        pause(deadline);
    })();
    if outcome.is_err() {
        let owned_pid = child.id().cast_signed();
        stop_owned_child(&mut child, &paths.pid_path);
        // Only another published lifetime owner can turn a failed launch into
        // a no-op. Probe after cleaning our group so an owned orphan cannot
        // supply that response. Never signal the other owner's PID.
        if supervisor::read_live_pid(&paths.pid_path).is_some_and(|pid| pid.as_raw() != owned_pid)
            && probe(paths, deadline)
        {
            println!("intentd is already running under another supervisor");
            return Ok(());
        }
    }
    outcome
}

#[cfg(unix)]
fn acquire_start_lock(
    mut file: std::fs::File,
    deadline: std::time::Instant,
) -> std::io::Result<nix::fcntl::Flock<std::fs::File>> {
    loop {
        match nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock) {
            Ok(lock) => return Ok(lock),
            Err((returned, nix::errno::Errno::EWOULDBLOCK))
                if std::time::Instant::now() < deadline =>
            {
                file = returned;
                pause(deadline);
            }
            Err((_, error)) => {
                return Err(std::io::Error::other(format!(
                    "cannot acquire startup lock: {error}"
                )))
            }
        }
    }
}

fn probe(paths: &SitterPaths, deadline: std::time::Instant) -> bool {
    crate::state::load(&paths.state_path)
        .current_version
        .is_some_and(|version| {
            crate::readiness::probe_daemon_version_with_timeout(
                &paths.daemon_binary(&version),
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .min(crate::readiness::PROBE_TIMEOUT),
            )
            .is_some()
        })
}

fn pause(deadline: std::time::Instant) {
    std::thread::sleep(
        deadline
            .saturating_duration_since(std::time::Instant::now())
            .min(std::time::Duration::from_millis(50)),
    );
}

#[cfg(unix)]
fn detach(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    // SAFETY: setsid is async-signal-safe; no allocation or locks in the forked child.
    unsafe {
        command.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::from)
        });
    }
}

/// Observe exit without reaping: the kernel must keep the child's PID reserved
/// until its session has been cleaned. `Child::try_wait` would release that PID
/// before killpg, risking a signal to an unrelated reused process group.
#[cfg(unix)]
fn child_exited(child: &std::process::Child) -> std::io::Result<bool> {
    use nix::libc;
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: info points to writable siginfo_t storage. P_PID selects our
    // unreaped direct child; WNOWAIT leaves its ownership intact on every poll.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            child.id(),
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: storage was zeroed and waitid succeeded. No event leaves si_pid
    // zero; an exit event fills its child-status fields on Linux and macOS.
    Ok(unsafe { info.assume_init().si_pid() } != 0)
}

#[cfg(unix)]
fn stop_owned_child(child: &mut std::process::Child, pid_path: &std::path::Path) {
    use nix::sys::signal::{kill, killpg, Signal};
    use nix::unistd::Pid;
    use std::time::{Duration, Instant};

    let Ok(exited) = child_exited(child) else {
        // ECHILD means we no longer have kernel ownership. Do not act on a
        // numeric PID that could have been reaped/reused by another owner.
        return;
    };
    let pid = Pid::from_raw(child.id().cast_signed());
    if !exited {
        let _ = kill(pid, Signal::SIGTERM);
        let deadline = Instant::now()
            + crate::supervisor::SupervisorConfig::from_env().kill_timeout
            + Duration::from_secs(1);
        while matches!(child_exited(child), Ok(false)) && Instant::now() < deadline {
            pause(deadline);
        }
    }
    // Even an exited supervisor can leave descendants behind. Its unreaped
    // leader reserves the PGID, so this cannot target a reused process group.
    let _ = killpg(pid, Signal::SIGKILL);
    // Remove only this child's stale record, before reaping releases its PID.
    // A foreground contender cannot overwrite a live/unreaped owner's record.
    if std::fs::read_to_string(pid_path).is_ok_and(|value| value.trim() == pid.to_string()) {
        let _ = std::fs::remove_file(pid_path);
    }
    let _ = child.wait();
}

/// The same lock serializes Windows restart requests and all detached launches.
#[cfg(unix)]
fn lock_start(
    paths: &SitterPaths,
    deadline: std::time::Instant,
) -> std::io::Result<nix::fcntl::Flock<std::fs::File>> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(paths.sitter_dir.join("start.lock"))?;
    acquire_start_lock(file, deadline)
}

#[cfg(windows)]
pub(crate) fn lock_start(
    paths: &SitterPaths,
    deadline: std::time::Instant,
) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::create_dir_all(&paths.sitter_dir)?;
    loop {
        match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .share_mode(0)
            .open(paths.sitter_dir.join("start.lock"))
        {
            Ok(file) => return Ok(file),
            Err(error)
                if error.raw_os_error() == Some(32) && std::time::Instant::now() < deadline =>
            {
                pause(deadline);
            }
            Err(error) => return Err(error),
        }
    }
}
#[cfg(windows)]
fn detach(command: &mut std::process::Command) {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NEW_PROCESS_GROUP, DETACHED_PROCESS};
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}
#[cfg(windows)]
fn child_exited(child: &std::process::Child) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::{
        Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
        System::Threading::WaitForSingleObject,
    };
    // SAFETY: std::process::Child retains the process handle even after exit.
    match unsafe { WaitForSingleObject(child.as_raw_handle(), 0) } {
        WAIT_OBJECT_0 => Ok(true),
        WAIT_TIMEOUT => Ok(false),
        _ => Err(std::io::Error::last_os_error()),
    }
}
#[cfg(windows)]
fn stop_owned_child(child: &mut std::process::Child, pid_path: &std::path::Path) {
    // The supervisor's job closes on death and kills every owned descendant.
    let _ = child.kill();
    let _ = child.wait();
    if std::fs::read_to_string(pid_path).is_ok_and(|value| value.trim() == child.id().to_string()) {
        let _ = std::fs::remove_file(pid_path);
    }
}

/// Request a Windows restart and wait for acknowledgment from the replacement,
/// not a status response that may still come from the old healthy child.
#[cfg(windows)]
#[must_use]
pub fn restart(args: &SitterArgs, paths: &SitterPaths) -> i32 {
    let result = (|| -> std::io::Result<bool> {
        let deadline = std::time::Instant::now() + crate::readiness::timeout_from_env();
        let _lock = lock_start(paths, deadline)?;
        let Some(pid) = crate::supervisor::read_live_pid(&paths.pid_path) else {
            return Ok(false);
        };
        let process = crate::windows::Process::open(pid.as_raw().cast_unsigned(), false)?;
        let control = crate::windows::RestartControl::open(
            pid.as_raw().cast_unsigned(),
            &process,
            &paths.sitter_dir,
        )?;
        let nonce = control.request()?;
        loop {
            if process.exited()? {
                return Err(std::io::Error::other("supervisor exited during restart"));
            }
            if control.completed(&nonce) {
                println!("intentd restarted and ready");
                return Ok(true);
            }
            if std::time::Instant::now() >= deadline {
                return Err(std::io::Error::other(
                    "replacement daemon did not become ready",
                ));
            }
            pause(deadline);
        }
    })();
    match result {
        Ok(true) => 0,
        Ok(false) => run(args, paths),
        Err(error) => {
            eprintln!("intentd-sitter: restart failed: {error}");
            1
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Runs on macOS as well as Linux: retain the dead supervisor with waitid,
    /// then kill its private group before reaping. The descendant owns a pipe;
    /// EOF proves cleanup without relying on platform-specific zombie status.
    #[test]
    fn dead_supervisor_cleanup_closes_descendant_pipe() {
        use std::io::Read;
        use std::process::{Command, Stdio};
        struct OwnedSession(std::process::Child, std::path::PathBuf);
        impl Drop for OwnedSession {
            fn drop(&mut self) {
                stop_owned_child(&mut self.0, &self.1);
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut command = Command::new("sh");
        command
            .args(["-c", "sleep 60 & echo ready; wait"])
            .stdout(Stdio::piped());
        detach(&mut command);
        let mut child = OwnedSession(command.spawn().unwrap(), dir.path().join("sitter.pid"));
        let pid = nix::unistd::Pid::from_raw(child.0.id().cast_signed());
        let pipe = child.0.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(pipe);
            let mut line = String::new();
            std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
            tx.send(line).unwrap();
            let mut tail = String::new();
            reader.read_to_string(&mut tail).unwrap();
            let _ = tx.send("eof".to_string());
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), "ready\n");
        nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !child_exited(&child.0).unwrap() {
            assert!(Instant::now() < deadline);
            pause(deadline);
        }
        // The PID is still reserved: observing exit a second time must work.
        assert!(child_exited(&child.0).unwrap());
        stop_owned_child(&mut child.0, &dir.path().join("sitter.pid"));
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), "eof");
        reader.join().unwrap();
    }
}

/// Stop the Windows supervisor even during startup/backoff. None means no
/// supervisor exists, so the CLI should forward stop to the installed daemon.
#[cfg(windows)]
#[must_use]
pub fn stop(paths: &SitterPaths) -> Option<i32> {
    let pid = crate::supervisor::read_live_pid(&paths.pid_path)?;
    let result = (|| -> std::io::Result<()> {
        let process = crate::windows::Process::open(pid.as_raw().cast_unsigned(), true)?;
        let control = crate::windows::RestartControl::open(
            pid.as_raw().cast_unsigned(),
            &process,
            &paths.sitter_dir,
        )?;
        let daemon_pid_path = paths.data_dir.join("intentd.pid");
        let daemon = std::fs::read_to_string(&daemon_pid_path)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
            .and_then(|pid| {
                crate::windows::Process::open(pid, false)
                    .ok()
                    .filter(|process| process.matches_record(&daemon_pid_path, pid))
            });
        control.request_stop()?;
        let deadline = std::time::Instant::now()
            + crate::supervisor::SupervisorConfig::from_env().kill_timeout * 2
            + std::time::Duration::from_secs(2);
        while !process.exited()? {
            if std::time::Instant::now() >= deadline {
                if !process.matches_record(&paths.pid_path, pid.as_raw().cast_unsigned()) {
                    return Err(std::io::Error::other(
                        "refusing to terminate unverified supervisor",
                    ));
                }
                process.terminate()?;
                let kill_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                while !process.exited()? {
                    if std::time::Instant::now() >= kill_deadline {
                        return Err(std::io::Error::other("supervisor did not stop"));
                    }
                    pause(kill_deadline);
                }
                break;
            }
            pause(deadline);
        }
        if let Some(daemon) = daemon {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while !daemon.exited()? {
                if std::time::Instant::now() >= deadline {
                    return Err(std::io::Error::other(
                        "daemon still running after supervisor exit",
                    ));
                }
                pause(deadline);
            }
        }
        Ok(())
    })();
    Some(match result {
        Ok(()) => {
            println!("intentd: stopped");
            0
        }
        Err(error) => {
            eprintln!("intentd-sitter: stop failed: {error}");
            1
        }
    })
}
