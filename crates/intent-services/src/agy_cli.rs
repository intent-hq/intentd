//! Keep the user's `agy` CLI new enough for `agy acp`.
//!
//! Like the Codex/Claude minimum (`bundled_cli`), only a launch or a catalog
//! probe acts, never discovery. agy has no package to install beside the
//! user's copy, so a confirmed-old `agy` runs its own signed `agy update`.
//! An unparseable version is permissive. A failed update is remembered for a
//! while so every turn does not retry it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use intent_providers::antigravity::{MIN_VERSION, MIN_VERSION_TRIPLE};
use intent_providers::version_gate::parse_cli_version;
use tokio::process::Command;

const VERSION_TIMEOUT: Duration = Duration::from_secs(10);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(300);
const FAILURE_TTL: Duration = Duration::from_secs(600);
const OUTPUT_TAIL: usize = 400;

/// Ensure `binary` is at [`MIN_VERSION`] or newer, running `agy update` once
/// when it is confirmed older.
///
/// # Errors
/// Returns an actionable message when the CLI stays too old.
pub(crate) async fn ensure_current(binary: &Path) -> Result<(), String> {
    ensure_current_with(binary, UPDATE_TIMEOUT).await
}

async fn ensure_current_with(binary: &Path, update_timeout: Duration) -> Result<(), String> {
    let Some(found) = version(binary).await else {
        return Ok(());
    };
    if found >= MIN_VERSION_TRIPLE {
        return Ok(());
    }
    // A detached task owns the update, so a caller cancelled by its own
    // budget (a background catalog refresh) cannot kill it or skip recording
    // a failure. Dropping the handle does not abort the task.
    let binary = binary.to_path_buf();
    tokio::spawn(async move { update_single_flight(&binary, update_timeout).await })
        .await
        .unwrap_or_else(|err| Err(format!("agy update task failed: {err}")))
}

async fn update_single_flight(binary: &Path, update_timeout: Duration) -> Result<(), String> {
    let lock = update_lock(binary);
    let _guard = lock.lock().await;
    // A concurrent caller may have finished the update while this one waited.
    let Some(found) = version(binary).await else {
        return Ok(());
    };
    if found >= MIN_VERSION_TRIPLE {
        return Ok(());
    }
    if let Some(reason) = recent_failure(binary) {
        return Err(reason);
    }
    let found = format_version(found);
    tracing::info!(path = %binary.display(), version = %found, minimum = MIN_VERSION, "updating agy for agy acp");
    let detail = match update(binary, update_timeout).await {
        Ok(()) => match version(binary).await {
            Some(now) if now >= MIN_VERSION_TRIPLE => {
                failures().lock().unwrap().remove(binary);
                tracing::info!(path = %binary.display(), version = %format_version(now), "agy updated");
                return Ok(());
            }
            Some(now) => format!(
                "agy update finished, but the version is still {}",
                format_version(now)
            ),
            None => return Ok(()),
        },
        Err(detail) => detail,
    };
    let reason = format!(
        "Antigravity needs agy {MIN_VERSION} or newer for `agy acp`; found {found} at {}. \
         Automatic update failed ({detail}). Run `agy update` on the daemon host, then retry.",
        binary.display()
    );
    remember_failure(binary, &reason);
    Err(reason)
}

async fn version(binary: &Path) -> Option<(u64, u64, u64)> {
    let output = tokio::time::timeout(
        VERSION_TIMEOUT,
        command(binary)
            .arg("--version")
            .stderr(Stdio::null())
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_cli_version(&String::from_utf8_lossy(&output.stdout))
}

async fn update(binary: &Path, timeout: Duration) -> Result<(), String> {
    let child = command(binary)
        .arg("update")
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("could not start agy update: {err}"))?;
    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(result) => result.map_err(|err| format!("agy update failed: {err}"))?,
        Err(_) => return Err(format!("agy update timed out after {}s", timeout.as_secs())),
    };
    if output.status.success() {
        return Ok(());
    }
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    let text = text.trim();
    let tail = text
        .char_indices()
        .rev()
        .nth(OUTPUT_TAIL)
        .map_or(text, |(index, _)| &text[index..]);
    Err(format!("agy update exited with {}: {tail}", output.status))
}

fn command(binary: &Path) -> Command {
    let absolute = std::path::absolute(binary).unwrap_or_else(|_| binary.to_path_buf());
    let mut command = Command::new(binary);
    command
        .env("PATH", intent_providers::enhanced_path(Some(&absolute)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command
}

fn format_version((major, minor, patch): (u64, u64, u64)) -> String {
    format!("{major}.{minor}.{patch}")
}

fn update_lock(binary: &Path) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, std::sync::Arc<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap()
        .entry(binary.to_path_buf())
        .or_default()
        .clone()
}

fn failures() -> &'static Mutex<HashMap<PathBuf, (Instant, String)>> {
    static FAILURES: OnceLock<Mutex<HashMap<PathBuf, (Instant, String)>>> = OnceLock::new();
    FAILURES.get_or_init(Mutex::default)
}

fn recent_failure(binary: &Path) -> Option<String> {
    let mut map = failures().lock().unwrap();
    match map.get(binary) {
        Some((at, reason)) if at.elapsed() < FAILURE_TTL => Some(reason.clone()),
        Some(_) => {
            map.remove(binary);
            None
        }
        None => None,
    }
}

/// The newest automatic update failure still inside its retry pause, for
/// discovery to show. Discovery itself never updates.
pub(crate) fn last_update_failure() -> Option<String> {
    failures()
        .lock()
        .unwrap()
        .values()
        .filter(|(at, _)| at.elapsed() < FAILURE_TTL)
        .max_by_key(|(at, _)| *at)
        .map(|(_, reason)| reason.clone())
}

fn remember_failure(binary: &Path, reason: &str) {
    failures()
        .lock()
        .unwrap()
        .insert(binary.to_path_buf(), (Instant::now(), reason.to_owned()));
}

#[cfg(all(test, unix))]
mod tests;
