//! Opt-in, bounded command execution whose direct-child wait survives daemon loss.
//! Receipts are local evidence, not authentication against another same-user process.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Args;
use nix::sys::signal::{killpg, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Args)]
pub(crate) struct RunArgs {
    /// Parent directory for retained invocation.json, result.json and output logs.
    #[arg(long)]
    record_dir: PathBuf,
    /// Unique invocation ID (letters, digits, hyphens, underscores). Never reuse it.
    #[arg(long)]
    invocation: String,
    /// Required execution bound, in seconds (1..86400).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=86400))]
    timeout_seconds: u64,
    /// Maximum retained bytes per output stream; excess is drained and discarded.
    #[arg(long, default_value_t = 8_388_608, value_parser = clap::value_parser!(u64).range(1..=67_108_864))]
    max_output_bytes: u64,
    /// Executable and arguments; use -- /bin/sh -c '...' for shell syntax.
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ResultArgs {
    #[arg(long)]
    record_dir: PathBuf,
    #[arg(long)]
    invocation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Invocation {
    version: u32,
    id: String,
    command: Vec<String>,
    cwd: PathBuf,
    started_at: String,
    timeout_seconds: u64,
    max_output_bytes: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Outcome {
    Exited,
    TimedOut,
    Stopped,
    SpawnFailed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Receipt {
    invocation: Invocation,
    outcome: Outcome,
    exit_code: Option<i32>,
    signal: Option<i32>,
    finished_at: String,
    error: Option<String>,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

impl Receipt {
    fn success(&self) -> bool {
        self.outcome == Outcome::Exited && self.exit_code == Some(0) && self.signal.is_none()
    }

    fn valid(&self) -> bool {
        let observed = matches!(
            (self.exit_code, self.signal),
            (Some(0..=255), None) | (None, Some(1..=127))
        );
        !self.finished_at.is_empty()
            && match self.outcome {
                Outcome::Exited | Outcome::TimedOut | Outcome::Stopped => {
                    observed && self.error.is_none()
                }
                Outcome::SpawnFailed => {
                    self.exit_code.is_none() && self.signal.is_none() && self.error.is_some()
                }
            }
    }
}

fn invocation_dir(root: &Path, id: &str) -> Result<PathBuf> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        bail!("invocation must contain 1..128 letters, digits, hyphens or underscores");
    }
    Ok(root.join(id))
}

/// Publish only a complete, synced JSON document. The directory is reserved once;
/// neither a later invocation nor a second worker may overwrite these files.
fn publish(dir: &Path, name: &str, value: &impl Serialize) -> Result<()> {
    let temporary = dir.join(format!("{name}.partial"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temporary, dir.join(name))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn read_receipt(root: &Path, id: &str) -> Result<Receipt> {
    let dir = invocation_dir(root, id)?;
    let invocation: Invocation = serde_json::from_reader(File::open(dir.join("invocation.json"))?)?;
    let receipt: Receipt = serde_json::from_reader(File::open(dir.join("result.json"))?)?;
    if invocation.version != 1
        || invocation.id != id
        || receipt.invocation != invocation
        || !receipt.valid()
    {
        bail!("incomplete or mismatched invocation evidence");
    }
    Ok(receipt)
}

pub(crate) fn result(args: &ResultArgs) -> ExitCode {
    match read_receipt(&args.record_dir, &args.invocation) {
        Ok(receipt) => {
            println!(
                "{}",
                serde_json::to_string(&receipt).expect("receipt serialization")
            );
            if receipt.success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({"outcome":"unknown", "invocationId":args.invocation, "error":error.to_string()})
            );
            ExitCode::from(2)
        }
    }
}

pub(crate) fn run(args: RunArgs) -> ExitCode {
    match launch(&args) {
        Ok(()) => result(&ResultArgs {
            record_dir: args.record_dir,
            invocation: args.invocation,
        }),
        Err(error) => {
            eprintln!("command evidence unavailable: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn launch(args: &RunArgs) -> Result<()> {
    let dir = invocation_dir(&args.record_dir, &args.invocation)?;
    fs::create_dir_all(&args.record_dir)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&dir)
        .context("reserve invocation directory (an existing ID cannot be reused)")?;
    let dir = fs::canonicalize(dir)?;
    let invocation = Invocation {
        version: 1,
        id: args.invocation.clone(),
        command: args.command.clone(),
        cwd: std::env::current_dir()?,
        started_at: intent_core::now_iso(),
        timeout_seconds: args.timeout_seconds,
        max_output_bytes: args.max_output_bytes,
    };
    publish(&dir, "invocation.json", &invocation)?;
    // The worker has no terminal descriptors and is not in the saved command's
    // process group. Killing that group, or losing its PTY, cannot kill the waiter.
    let mut worker = Command::new(std::env::current_exe()?);
    worker
        .arg("command-worker")
        .arg(&dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(File::create(dir.join("worker.log"))?);
    // SAFETY: setsid is async-signal-safe; this closure allocates nothing.
    unsafe {
        worker.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::from)
        });
    }
    let mut child = worker.spawn().context("spawn independent command waiter")?;
    eprintln!(
        "Command evidence: {} (worker continues until exit or timeout if this caller stops)",
        dir.display()
    );
    child.wait().context("wait for command evidence worker")?;
    Ok(())
}

pub(crate) fn worker(dir: &Path) -> ExitCode {
    match collect(dir) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("command evidence unavailable: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn collect(dir: &Path) -> Result<()> {
    // Even direct invocation of the internal helper cannot execute an ID twice.
    let _claim = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join("worker.claim"))?;
    let invocation: Invocation = serde_json::from_reader(File::open(dir.join("invocation.json"))?)?;
    if invocation.version != 1
        || invocation.command.is_empty()
        || !(1..=86400).contains(&invocation.timeout_seconds)
        || !(1..=67_108_864).contains(&invocation.max_output_bytes)
    {
        bail!("invalid command invocation");
    }
    let mut command = Command::new(&invocation.command[0]);
    command
        .args(&invocation.command[1..])
        .current_dir(&invocation.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut stdout = Capture::new(dir, "stdout.log", invocation.max_output_bytes)?;
    let mut stderr = Capture::new(dir, "stderr.log", invocation.max_output_bytes)?;
    let deadline = Instant::now() + Duration::from_secs(invocation.timeout_seconds);
    let mut receipt = Receipt {
        invocation,
        outcome: Outcome::Exited,
        exit_code: None,
        signal: None,
        finished_at: String::new(),
        error: None,
        stdout_truncated: false,
        stderr_truncated: false,
    };
    match command.spawn() {
        Err(error) => {
            receipt.outcome = Outcome::SpawnFailed;
            receipt.error = Some(error.to_string());
        }
        Ok(mut child) => {
            // Any capture error must kill/reap the owned child, never abandon it.
            let observed = (|| -> Result<_> {
                let mut out = child.stdout.take().context("child stdout pipe missing")?;
                let mut err = child.stderr.take().context("child stderr pipe missing")?;
                nonblocking(&out)?;
                nonblocking(&err)?;
                let status = loop {
                    stdout.drain(&mut out)?;
                    stderr.drain(&mut err)?;
                    let stopped = stop_requested(dir, &receipt.invocation);
                    match child.try_wait() {
                        Ok(Some(status)) => break status,
                        Ok(None) if !stopped && Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        Ok(None) => {
                            receipt.outcome = if stopped {
                                Outcome::Stopped
                            } else {
                                Outcome::TimedOut
                            };
                            // The unreaped direct child pins this process-group identity.
                            let _ =
                                killpg(Pid::from_raw(child.id().cast_signed()), Signal::SIGKILL);
                            let _ = child.kill();
                            break child.wait().context("wait after timeout")?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                Ok((status, out, err))
            })();
            let (status, mut out, mut err) = match observed {
                Ok(status) => status,
                Err(error) => {
                    if matches!(child.try_wait(), Ok(None)) {
                        let _ = killpg(Pid::from_raw(child.id().cast_signed()), Signal::SIGKILL);
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                    return Err(error);
                }
            };
            // After reaping, errors must never signal the old PID/group: it can
            // be reused. Drain available bytes without waiting on descendants.
            stdout.finish(&mut out)?;
            stderr.finish(&mut err)?;
            receipt.exit_code = status.code();
            receipt.signal = status.signal();
        }
    }
    stdout.file.sync_all()?;
    stderr.file.sync_all()?;
    receipt.stdout_truncated = stdout.truncated;
    receipt.stderr_truncated = stderr.truncated;
    receipt.finished_at = intent_core::now_iso();
    publish(dir, "result.json", &receipt)
}

/// Read pipes without waiting for EOF from descendants outside our lifetime.
fn nonblocking(stream: &impl AsFd) -> Result<()> {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};
    let flags = OFlag::from_bits_truncate(fcntl(stream, FcntlArg::F_GETFL)?);
    fcntl(stream, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

struct Capture {
    file: File,
    remaining: u64,
    truncated: bool,
}

impl Capture {
    fn new(dir: &Path, name: &str, limit: u64) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(name))?,
            remaining: limit,
            truncated: false,
        })
    }

    fn finish(&mut self, stream: &mut impl Read) -> Result<()> {
        self.drain(stream)?;
        // A child can enlarge its pipe or a descendant can keep writing. If
        // bytes remain after the final bounded drain, disclose incomplete output.
        let mut extra = [0];
        match stream.read(&mut extra) {
            Ok(0) => (),
            Ok(_) => self.truncated = true,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => self.truncated = true,
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    fn drain(&mut self, stream: &mut impl Read) -> Result<()> {
        let mut bytes = [0; 8192];
        // Bound work per tick even when a child writes continuously.
        for _ in 0..16 {
            let n = match stream.read(&mut bytes) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            let retained = n.min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
            self.file.write_all(&bytes[..retained])?;
            self.remaining -= retained as u64;
            self.truncated |= retained != n;
        }
        Ok(())
    }
}

fn stop_requested(dir: &Path, expected: &Invocation) -> bool {
    File::open(dir.join("stop.json"))
        .ok()
        .and_then(|file| serde_json::from_reader::<_, Invocation>(file).ok())
        .is_some_and(|invocation| invocation == *expected)
}

/// The waiter consumes this identity-bound request; no recovered PID is signalled.
pub(crate) fn stop(args: &ResultArgs) -> ExitCode {
    let request = (|| -> Result<()> {
        if read_receipt(&args.record_dir, &args.invocation).is_ok() {
            return Ok(());
        }
        let dir = invocation_dir(&args.record_dir, &args.invocation)?;
        let invocation: Invocation =
            serde_json::from_reader(File::open(dir.join("invocation.json"))?)?;
        if invocation.id != args.invocation || invocation.version != 1 {
            bail!("mismatched command invocation");
        }
        if !stop_requested(&dir, &invocation) {
            publish(&dir, "stop.json", &invocation)?;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while read_receipt(&args.record_dir, &args.invocation).is_err() && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    })();
    if let Err(error) = request {
        eprintln!("command stop evidence unavailable: {error:#}");
    }
    result(args)
}

/// Explicit retention cleanup accepts only settled, identity-checked evidence.
/// Keep a tombstone directory so cleanup can never enable invocation ID reuse.
pub(crate) fn clean(args: &ResultArgs) -> ExitCode {
    let cleanup = (|| -> Result<()> {
        read_receipt(&args.record_dir, &args.invocation)?;
        let dir = invocation_dir(&args.record_dir, &args.invocation)?;
        for name in [
            "stdout.log",
            "stderr.log",
            "worker.log",
            "stop.json",
            "worker.claim",
            "invocation.json",
            "result.json",
        ] {
            match fs::remove_file(dir.join(name)) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
        }
        File::open(&dir)?.sync_all()?;
        Ok(())
    })();
    match cleanup {
        Ok(()) => {
            println!(
                "{}",
                serde_json::json!({"cleaned":true,"invocationId":args.invocation})
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("refusing to clean unsettled command evidence: {error:#}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> Receipt {
        Receipt {
            invocation: Invocation {
                version: 1,
                id: "one".into(),
                command: vec!["true".into()],
                cwd: PathBuf::from("/"),
                started_at: "2026-09-30T12:00:00Z".into(),
                timeout_seconds: 10,
                max_output_bytes: 1024,
            },
            outcome: Outcome::Exited,
            exit_code: Some(0),
            signal: None,
            finished_at: "2026-09-30T12:00:01Z".into(),
            error: None,
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    #[test]
    fn contradictory_and_unobserved_status_is_invalid() {
        let mut r = receipt();
        assert!(r.valid() && r.success());
        r.signal = Some(15);
        assert!(!r.valid());
        r.exit_code = None;
        assert!(r.valid() && !r.success());
        r.signal = None;
        assert!(!r.valid());
        r.exit_code = Some(-1);
        assert!(!r.valid());
        r.exit_code = Some(256);
        assert!(!r.valid());
        r.exit_code = Some(0);
        r.outcome = Outcome::TimedOut;
        assert!(r.valid() && !r.success());
        r.finished_at.clear();
        assert!(!r.valid());
    }

    #[test]
    fn invocation_cannot_escape_its_directory() {
        for id in ["", ".", "..", "../other", "/absolute", "a/b", "a\\b"] {
            assert!(invocation_dir(Path::new("/records"), id).is_err());
        }
        assert_eq!(
            invocation_dir(Path::new("/records"), "gate_01-abc").unwrap(),
            Path::new("/records/gate_01-abc")
        );
    }
}
