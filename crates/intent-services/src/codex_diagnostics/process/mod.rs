//! Diagnostic-local process ownership. Standard streams remain available for
//! version checks and ACP/app-server conversations; lifecycle control is private.

use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};

use super::UnknownReason;

#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "macos.rs"]
mod platform;
#[cfg(windows)]
#[path = "windows.rs"]
mod platform;

struct Started {
    ownership: platform::Ownership,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
}

pub(crate) struct ProbeProcess {
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
    ownership: Option<platform::Ownership>,
    home: Option<ProbeHome>,
    cleanup_task: Option<tokio::task::JoinHandle<Result<(), UnknownReason>>>,
    cleanup_result: Option<Result<(), UnknownReason>>,
}

impl ProbeProcess {
    /// The caller supplies isolated env/cwd. All three standard streams are
    /// piped; drop unused stdin and drain/discard unused output streams.
    pub(crate) async fn spawn(
        mut command: Command,
        home: tempfile::TempDir,
    ) -> Result<Self, UnknownReason> {
        // On cancellation during platform startup, keep the directory until
        // ownership can confirm cleanup instead of dropping it prematurely.
        let home = ProbeHome(Some(home));
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Ok(started) = platform::spawn(command).await {
            Ok(Self {
                stdin: started.stdin,
                stdout: started.stdout,
                stderr: started.stderr,
                ownership: Some(started.ownership),
                home: Some(home),
                cleanup_task: None,
                cleanup_result: None,
            })
        } else {
            // A bounded platform startup error may follow process creation
            // and failed teardown. Preserve the home unless cleanup is
            // positively confirmed; never infer that from a generic error.
            drop(home);
            Err(UnknownReason::SpawnFailed)
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus, UnknownReason> {
        self.ownership
            .as_mut()
            .ok_or(UnknownReason::InspectionFailed)?
            .wait()
            .await
            .map_err(|_| UnknownReason::InspectionFailed)
    }

    pub(crate) async fn cleanup(&mut self) -> Result<(), UnknownReason> {
        if let Some(result) = self.cleanup_result {
            return result;
        }
        self.start_cleanup()?;
        // Keep the handle in the guard: cancelling this await and then retrying
        // cleanup must await the same task, not report premature success.
        let result = self
            .cleanup_task
            .as_mut()
            .expect("cleanup started")
            .await
            .unwrap_or(Err(UnknownReason::CleanupFailed));
        self.cleanup_task = None;
        self.cleanup_result = Some(result);
        result
    }

    fn start_cleanup(&mut self) -> Result<(), UnknownReason> {
        if self.cleanup_task.is_some() || self.cleanup_result.is_some() {
            return Ok(());
        }
        let handle =
            tokio::runtime::Handle::try_current().map_err(|_| UnknownReason::CleanupFailed)?;
        let ownership = self.ownership.take().ok_or(UnknownReason::CleanupFailed)?;
        let home = self.home.take();
        drop(self.stdin.take());
        drop(self.stdout.take());
        drop(self.stderr.take());
        self.cleanup_task = Some(handle.spawn(async move {
            match tokio::time::timeout(Duration::from_secs(5), ownership.cleanup()).await {
                Ok(Ok(())) => {
                    if let Some(home) = home {
                        home.remove().map_err(|_| UnknownReason::CleanupFailed)?;
                    }
                    Ok(())
                }
                _ => Err(UnknownReason::CleanupFailed),
            }
        }));
        Ok(())
    }
}

impl Drop for ProbeProcess {
    fn drop(&mut self) {
        // Cancellation of the awaiting caller must not cancel descendant
        // teardown or release its directory. The task owns both until settled.
        let _ = self.start_cleanup();
    }
}

struct ProbeHome(Option<tempfile::TempDir>);

impl ProbeHome {
    fn remove(mut self) -> std::io::Result<()> {
        match self.0.take() {
            Some(home) => home.close(),
            None => Ok(()),
        }
    }
}

impl Drop for ProbeHome {
    fn drop(&mut self) {
        if let Some(home) = self.0.take() {
            // Runtime shutdown or unconfirmed cleanup is not proof that the
            // process tree stopped. Never remove a still-used configuration.
            let _ = home.keep();
        }
    }
}
