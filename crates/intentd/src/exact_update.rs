//! Fixed-release updates stay off the connection read loop. Only a verified
//! install triggers the sitter's restart-only SIGHUP path.

use std::path::Path;
use std::sync::{Arc, Mutex};

use intentd_sitter::paths::SitterPaths;
use intentd_sitter::updater::{validate_exact_version, Updater};
use serde_json::{json, Value};

static SITTER_EXACT_UPDATE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Capture before starting threads and scrub so daemon children cannot inherit it.
pub(crate) fn capture_sitter_handshake() {
    let name = intentd_sitter::supervisor::EXACT_UPDATE_ENV;
    SITTER_EXACT_UPDATE.store(
        std::env::var(name).as_deref() == Ok("1"),
        std::sync::atomic::Ordering::Relaxed,
    );
    std::env::remove_var(name);
}

pub(crate) fn supported(pid_path: &Path) -> bool {
    SITTER_EXACT_UPDATE.load(std::sync::atomic::Ordering::Relaxed)
        && crate::sitter_update_supported(pid_path)
}

#[derive(Clone, Default)]
pub(crate) struct ExactUpdate {
    status: Arc<Mutex<Option<Value>>>,
}

impl ExactUpdate {
    pub(crate) fn status(&self) -> Option<Value> {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn active(&self) -> bool {
        self.status().is_some_and(|s| s["state"] != "failed")
    }

    pub(crate) fn start(&self, pid_path: &Path, target: &str) -> Result<(), String> {
        let version = validate_exact_version(target).map_err(|e| e.to_string())?;
        // Cargo validates this version; build metadata does not affect precedence.
        let current = env!("CARGO_PKG_VERSION")
            .split_once('+')
            .map_or(env!("CARGO_PKG_VERSION"), |(version, _)| version);
        let current = validate_exact_version(current).map_err(|e| e.to_string())?;
        if !version.cmp_precedence(&current).is_gt() {
            return Err("target version must be newer than the running daemon".into());
        }
        if !supported(pid_path) {
            return Err("daemon requires a sitter with exact-version update support".into());
        }
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if status.as_ref().is_some_and(|s| s["state"] != "failed") {
            return Err("an exact-version update is already in progress".into());
        }
        *status = Some(json!({ "targetVersion": target, "state": "installing" }));
        drop(status);
        let operation = self.clone();
        let target = target.to_string();
        let pid_path = pid_path.to_path_buf();
        tokio::spawn(async move {
            let install_target = target.clone();
            let install_path = pid_path.clone();
            let result =
                tokio::task::spawn_blocking(move || install(&install_path, &install_target))
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(std::convert::identity);
            let result = result.and_then(|()| {
                operation.set(json!({ "targetVersion": target, "state": "restarting" }));
                restart(&pid_path)
            });
            if let Err(message) = result {
                tracing::warn!(target_version = %target, error = %message, "exact-version update failed");
                operation
                    .set(json!({ "targetVersion": target, "state": "failed", "message": message }));
            }
        });
        Ok(())
    }

    fn set(&self, value: Value) {
        *self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(value);
    }
}

fn install(pid_path: &Path, target: &str) -> Result<(), String> {
    let data_dir = pid_path
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid sitter data directory")?;
    let paths = SitterPaths::from_data_dir(data_dir);
    let updater = Updater::new(paths.clone()).map_err(|e| e.to_string())?;
    updater
        .install_exact(target, env!("CARGO_PKG_VERSION"))
        .map_err(|e| e.to_string())?;
    // A concurrent channel update must never be reported as installing our target.
    if intentd_sitter::state::load(&paths.state_path)
        .current_version
        .as_deref()
        != Some(target)
    {
        return Err("installed version changed during the update; retry after reconnecting".into());
    }
    Ok(())
}

#[cfg(unix)]
fn restart(pid_path: &Path) -> Result<(), String> {
    let pid = crate::supervising_sitter_pid(pid_path, std::os::unix::process::parent_id())
        .ok_or("supervising sitter is no longer available")?;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid.cast_signed()),
        nix::sys::signal::Signal::SIGHUP,
    )
    .map_err(|e| format!("failed to restart intentd-sitter: {e}"))
}

#[cfg(not(unix))]
fn restart(_pid_path: &Path) -> Result<(), String> {
    Err("exact-version updates require unix supervision".into())
}
