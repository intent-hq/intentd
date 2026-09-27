//! Diagnostic-local process ownership. Standard streams remain available for
//! version checks and ACP/app-server conversations; lifecycle control is private.

use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};

use super::UnknownReason;

mod resources;
pub(crate) use resources::ProbeDependency;
use resources::ProbeHome;

// The read-only implementation is also built and tested on other hosts. Only
// macOS selects its uninhabited process owner; Linux/Windows keep real owners.
mod macos;
pub(super) use macos::inspect_metadata;

pub(super) fn ensure_supported() -> Result<(), UnknownReason> {
    if cfg!(target_os = "macos") {
        Err(UnknownReason::UnsupportedPlatform)
    } else {
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(super) mod test_control;

#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
use macos as platform;
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
    dependency: Option<ProbeHome>,
    cleanup_task: Option<tokio::task::JoinHandle<Result<(), UnknownReason>>>,
    cleanup_result: Option<Result<(), UnknownReason>>,
    #[cfg(all(test, target_os = "linux"))]
    cleanup_hook: Option<test_control::CleanupHook>,
}

impl ProbeProcess {
    /// The caller supplies isolated env/cwd. All three standard streams are
    /// piped; drop unused stdin and drain/discard unused output streams.
    pub(crate) async fn spawn(
        command: Command,
        home: tempfile::TempDir,
    ) -> Result<Self, UnknownReason> {
        Self::spawn_with_dependency(command, home, None).await
    }

    /// A dependent executable may live in another probe's temporary HOME.
    /// Acquire its lease before platform startup and keep it through cleanup.
    pub(crate) async fn spawn_with_dependency(
        mut command: Command,
        home: tempfile::TempDir,
        dependency: Option<ProbeDependency>,
    ) -> Result<Self, UnknownReason> {
        // Reject before acquiring a lease: no child can use these directories,
        // so an unsupported probe must not retain them as unconfirmed cleanup.
        ensure_supported()?;
        #[cfg(all(test, target_os = "linux"))]
        let cleanup_hook = test_control::capture(&command, home.path());
        // On cancellation during platform startup, keep the directory until
        // ownership can confirm cleanup instead of dropping it prematurely.
        let home = ProbeHome::new(home);
        let dependency = dependency.map(ProbeHome::from);
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
                dependency,
                cleanup_task: None,
                cleanup_result: None,
                #[cfg(all(test, target_os = "linux"))]
                cleanup_hook,
            })
        } else {
            // A bounded platform startup error may follow process creation
            // and failed teardown. Preserve the home unless cleanup is
            // positively confirmed; never infer that from a generic error.
            drop(home);
            Err(UnknownReason::SpawnFailed)
        }
    }

    pub(crate) fn dependency(&self) -> Option<ProbeDependency> {
        self.home.as_ref().map(ProbeHome::dependency)
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
        let dependency = self.dependency.take();
        #[cfg(all(test, target_os = "linux"))]
        let hook = self.cleanup_hook.take();
        drop(self.stdin.take());
        drop(self.stdout.take());
        drop(self.stderr.take());
        self.cleanup_task = Some(handle.spawn(async move {
            #[cfg(all(test, target_os = "linux"))]
            if let Some(hook) = &hook {
                hook.before().await;
            }
            let cleaned = tokio::time::timeout(Duration::from_secs(5), ownership.cleanup()).await;
            #[cfg(all(test, target_os = "linux"))]
            let cleaned = if hook.as_ref().is_some_and(test_control::CleanupHook::fail) {
                Ok(Err(std::io::Error::other(
                    "test cleanup confirmation unavailable",
                )))
            } else {
                cleaned
            };
            let result = match cleaned {
                Ok(Ok(())) => {
                    let own = home.map(ProbeHome::remove).transpose();
                    let shared = dependency.map(ProbeHome::remove).transpose();
                    own.and(shared)
                        .map(|_| ())
                        .map_err(|_| UnknownReason::CleanupFailed)
                }
                _ => Err(UnknownReason::CleanupFailed),
            };
            #[cfg(all(test, target_os = "linux"))]
            if let Some(hook) = &hook {
                hook.after();
            }
            result
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
