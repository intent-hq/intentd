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

#[cfg(not(unix))]
fn start(_args: &SitterArgs, _paths: &SitterPaths) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "background start is not supported on this platform yet",
    ))
}

#[cfg(unix)]
fn start(args: &SitterArgs, paths: &SitterPaths) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io;
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
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(paths.sitter_dir.join("start.lock"))?;
    let _lock = acquire_start_lock(file, deadline)?;
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
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(paths.sitter_dir.join("start.log"))?;
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

#[cfg(unix)]
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

#[cfg(unix)]
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
