use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Stdio};

use nix::sys::signal::{kill, killpg, Signal};
use nix::unistd::Pid;
use tokio::io::AsyncReadExt;
use tokio::net::unix::pipe;
use tokio::process::{Child, Command};

pub(super) struct Ownership {
    child: Child,
    pid: u32,
    status: pipe::Receiver,
    status_bytes: [u8; 4],
    status_length: usize,
    control: Option<pipe::Sender>,
    descendants: Vec<i32>,
    finished: bool,
}

#[expect(clippy::unused_async)] // Common platform API also permits asynchronous startup.
pub(super) async fn spawn(command: Command) -> io::Result<super::Started> {
    let (status_writer, status) = pipe::pipe()?;
    let (control, control_reader) = pipe::pipe()?;
    let status_writer = above_stdio(status_writer.into_blocking_fd()?)?;
    let control_reader = above_stdio(control_reader.into_blocking_fd()?)?;
    let mut owner = supervisor(&command, &control_reader, &status_writer);
    drop(command);
    // SAFETY: child-only prctl/fcntl are syscalls with no allocation or locks.
    // Owned descriptors remain alive in this closure through the fork/exec.
    unsafe {
        owner.pre_exec(move || {
            nix::sys::prctl::set_child_subreaper(true).map_err(io::Error::from)?;
            for fd in [&control_reader, &status_writer] {
                if libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = owner.spawn()?;
    let pid = child.id().expect("new supervisor has a pid");
    Ok(super::Started {
        stdin: child.stdin.take(),
        stdout: child.stdout.take(),
        stderr: child.stderr.take(),
        ownership: Ownership {
            child,
            pid,
            status,
            status_bytes: [0; 4],
            status_length: 0,
            control: Some(control),
            descendants: Vec::new(),
            finished: false,
        },
    })
}

fn above_stdio(fd: OwnedFd) -> io::Result<OwnedFd> {
    // Reserve shell descriptors 3/4 without colliding with either inherited
    // source descriptor. The copy remains close-on-exec until our pre_exec.
    // SAFETY: fcntl duplicates a valid owned descriptor, returning new ownership.
    let duplicate = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 5) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    drop(fd);
    // SAFETY: this successful fcntl returned a new descriptor owned only here.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

fn supervisor(command: &Command, control: &OwnedFd, status: &OwnedFd) -> Command {
    let command = command.as_std();
    let mut owner = Command::new("/bin/sh");
    owner
        .args([
            "-c",
            include_str!("supervise.sh"),
            "intentd-codex-diagnostic",
        ])
        .arg(control.as_raw_fd().to_string())
        .arg(status.as_raw_fd().to_string())
        .arg(command.get_program())
        .args(command.get_args())
        .env_clear()
        .envs(
            command
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value))),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    if let Some(directory) = command.get_current_dir() {
        owner.current_dir(directory);
    }
    owner
}

impl Ownership {
    pub(super) async fn wait(&mut self) -> io::Result<ExitStatus> {
        // Keep partial data in the owner so cancelling a wait never discards
        // part of the private status frame. Provider stdio remains untouched.
        while self.status_length < self.status_bytes.len() {
            let read = self
                .status
                .read(&mut self.status_bytes[self.status_length..])
                .await?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.status_length += read;
        }
        let code = std::str::from_utf8(&self.status_bytes[..3])
            .ok()
            .and_then(|text| text.parse::<u8>().ok())
            .filter(|_| self.status_bytes[3] == b'\n')
            .ok_or(io::ErrorKind::InvalidData)?;
        Ok(ExitStatus::from_raw(i32::from(code) << 8))
    }

    pub(super) async fn cleanup(mut self) -> io::Result<()> {
        // The subreaper stays alive throughout the sweep, so a child's natural
        // exit cannot hide its detached descendants from the next snapshot.
        loop {
            self.descendants = owned_children(self.pid)?;
            if self
                .descendants
                .iter()
                .map(|pid| terminated(*pid))
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .all(|stopped| stopped)
            {
                break;
            }
            intent_acp::sweep_escaped_descendants(&self.descendants).await;
        }
        // The shell may defer reaping adopted zombies until it leaves read.
        // First establish that none can execute, then release the supervisor
        // and confirm that every remaining identity has actually been reaped.
        drop(self.control.take());
        self.child.wait().await?;
        self.finished = true;
        for pid in &self.descendants {
            while std::fs::exists(format!("/proc/{pid}"))? {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}

fn terminated(pid: i32) -> io::Result<bool> {
    match read_proc(&format!("/proc/{pid}/stat")) {
        Ok(stat) => stat
            .rsplit_once(") ")
            .map(|(_, rest)| rest.starts_with('Z'))
            .ok_or_else(|| io::ErrorKind::InvalidData.into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn owned_children(owner: u32) -> io::Result<Vec<i32>> {
    // The owner is our single-threaded shell. Killing each generation causes
    // its orphans to be adopted by this same stable owner for the next pass.
    // Unlike the generic best-effort ps helper, read failures are not proof
    // of an empty tree and must preserve the temporary home.
    let children = read_proc(&format!("/proc/{owner}/task/{owner}/children"))?;
    // Include zombies so cleanup can verify they were reaped before it returns.
    children
        .split_whitespace()
        .map(|pid| {
            let pid = pid.parse::<i32>().map_err(|_| io::ErrorKind::InvalidData)?;
            if pid <= 1 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(pid)
        })
        .collect()
}

fn read_proc(path: &str) -> io::Result<String> {
    use std::io::Read;
    let mut text = String::new();
    std::fs::File::open(path)?
        .take(65537)
        .read_to_string(&mut text)?;
    if text.len() > 65536 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(text)
}

impl Drop for Ownership {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Runtime teardown can drop the cleanup future. Kill all identities it
        // already owns; the shared home guard retains unconfirmed directories.
        for pid in &self.descendants {
            let _ = kill(Pid::from_raw(*pid), Signal::SIGKILL);
        }
        let _ = killpg(Pid::from_raw(self.pid.cast_signed()), Signal::SIGKILL);
        let _ = self.child.start_kill();
    }
}
