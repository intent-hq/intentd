//! Best-effort repair used only by the live provider setup test.

use std::time::Duration;

use tokio::process::Command;

/// Prevent npm settings from suppressing the install's cache manifest.
pub const NPX_SAVE_ARG: &str = "--save=true";

/// Includes admission, process startup, npm configuration and cache I/O.
pub const NPX_CACHE_REPAIR_BUDGET: Duration = Duration::from_secs(3);

/// Whether the actual adapter command inherits process variables.
#[derive(Clone, Copy)]
pub enum CommandEnvironment {
    Inherit,
    Complete,
}

/// Restore an absent manifest for a reviewed adapter pin during provider setup.
/// The caller returns within the budget. `keep_alive` stays with the owned job
/// until its process tree has exited, including after cancellation or timeout.
#[cfg_attr(
    not(unix),
    allow(
        clippy::unused_async,
        reason = "Keep the shared async API on targets without automatic cache repair"
    )
)]
pub async fn repair_npx_cache<T: Send + 'static>(
    command: &Command,
    package: &str,
    environment: CommandEnvironment,
    keep_alive: T,
) -> bool {
    #[cfg(unix)]
    {
        unix::repair(command, package, environment, keep_alive).await
    }
    #[cfg(not(unix))]
    {
        let _ = (command, package, environment, keep_alive);
        false
    }
}

#[cfg(unix)]
mod unix {
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::{Arc, LazyLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nix::errno::Errno;
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    use tokio::process::{Child, Command};
    use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

    use super::{CommandEnvironment, NPX_CACHE_REPAIR_BUDGET};

    static REPAIR_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
    const SCRIPT: &str = include_str!("../resources/npm-cache-repair.cjs");

    pub(super) async fn repair<T: Send + 'static>(
        command: &Command,
        package: &str,
        environment: CommandEnvironment,
        keep_alive: T,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + NPX_CACHE_REPAIR_BUDGET;
        if !intent_providers::ACP_PROVIDERS.iter().any(|provider| {
            provider.npx_only_package == Some(package)
                || provider.fallback_npx_package == Some(package)
        }) {
            return false;
        }
        let Some(snapshot) = LaunchSnapshot::capture(command) else {
            return false;
        };
        let package = package.to_owned();
        let (cancel, mut cancelled) = oneshot::channel::<()>();
        let job = intent_core::caller::spawn_with_current_caller(async move {
            let permit = tokio::select! {
                permit = REPAIR_SLOTS.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return false,
                },
                _ = &mut cancelled => return false,
            };
            if !matches!(
                cancelled.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ) {
                return false;
            }
            let started = tokio::task::spawn_blocking(move || {
                snapshot.start(package, environment, keep_alive, permit)
            })
            .await;
            let Ok(Some(mut running)) = started else {
                return false;
            };
            running.finish(&mut cancelled).await
        });
        let repaired = tokio::time::timeout_at(deadline, job)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false);
        drop(cancel);
        repaired
    }

    struct LaunchSnapshot {
        npm: PathBuf,
        cwd: Option<PathBuf>,
        env: Vec<(OsString, Option<OsString>)>,
        expires: u128,
    }

    impl LaunchSnapshot {
        fn capture(command: &Command) -> Option<Self> {
            let command = command.as_std();
            let npx = Path::new(command.get_program());
            let npm_name = match npx.file_name()?.to_str()? {
                "npx" => "npm",
                "npx-cli.js" => "npm-cli.js",
                _ => return None,
            };
            Some(Self {
                npm: npx.with_file_name(npm_name),
                cwd: command.get_current_dir().map(Path::to_owned),
                env: command
                    .get_envs()
                    .map(|(key, value)| (key.to_owned(), value.map(std::ffi::OsStr::to_owned)))
                    .collect(),
                expires: (SystemTime::now().duration_since(UNIX_EPOCH).ok()?
                    + NPX_CACHE_REPAIR_BUDGET)
                    .as_millis(),
            })
        }

        fn start<T: Send + 'static>(
            self,
            package: String,
            environment: CommandEnvironment,
            keep_alive: T,
            permit: OwnedSemaphorePermit,
        ) -> Option<RunningRepair<T>> {
            let mut command = Command::new("node");
            command
                .args(["-e", SCRIPT, "--"])
                .arg(package)
                .arg(self.npm)
                .arg(self.expires.to_string())
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .process_group(0);
            if let Some(cwd) = self.cwd {
                command.current_dir(cwd);
            }
            if matches!(environment, CommandEnvironment::Inherit) {
                command.envs(std::env::vars_os());
            }
            for (key, value) in self.env {
                if let Some(value) = value {
                    command.env(key, value);
                } else {
                    command.env_remove(key);
                }
            }
            crate::AgentPriority::from_env().apply(&mut command);
            let child = command.spawn().ok()?;
            let pid = child.id()?;
            Some(RunningRepair {
                child,
                pid,
                keep_alive: Some(keep_alive),
                permit: Some(permit),
                cleaned: false,
            })
        }
    }

    struct RunningRepair<T> {
        child: Child,
        pid: u32,
        keep_alive: Option<T>,
        permit: Option<OwnedSemaphorePermit>,
        cleaned: bool,
    }

    impl<T> RunningRepair<T> {
        async fn finish(&mut self, cancelled: &mut oneshot::Receiver<()>) -> bool {
            let status = tokio::select! {
                status = self.child.wait() => status.ok(),
                _ = cancelled => None,
            };
            let descendants = if self.child.id().is_some() {
                crate::descendant_pids(self.pid).await
            } else {
                Vec::new()
            };
            let group = Pid::from_raw(self.pid.cast_signed());
            // The process group can outlive its already-waited leader.
            let _ = killpg(group, Signal::SIGKILL);
            let _ = self.child.start_kill();
            let _ = self.child.wait().await;
            crate::sweep_escaped_descendants(&descendants).await;
            while !matches!(killpg(group, None), Err(Errno::ESRCH)) {
                if group_has_live_members(self.pid).await == Some(false) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            self.cleaned = true;
            status.is_some_and(|status| status.success())
        }
    }

    async fn group_has_live_members(group: u32) -> Option<bool> {
        let mut ps = Command::new("ps");
        ps.args(["-axo", "pgid=,stat="])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(2), ps.output())
            .await
            .ok()?
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let mut observed = false;
        for line in String::from_utf8(output.stdout).ok()?.lines() {
            let mut fields = line.split_whitespace();
            let pid: u32 = fields.next()?.parse().ok()?;
            let state = fields.next()?.as_bytes().first().copied()?;
            observed = true;
            if pid == group && !matches!(state, b'Z' | b'X') {
                return Some(true);
            }
        }
        observed.then_some(false)
    }

    impl<T> Drop for RunningRepair<T> {
        fn drop(&mut self) {
            if self.cleaned {
                return;
            }
            if self.child.id().is_some() {
                let _ = killpg(Pid::from_raw(self.pid.cast_signed()), Signal::SIGKILL);
                let _ = self.child.start_kill();
            }
            // An interrupted reaper must not remove a possibly live tree's cwd.
            std::mem::forget(self.keep_alive.take());
            std::mem::forget(self.permit.take());
        }
    }
}
