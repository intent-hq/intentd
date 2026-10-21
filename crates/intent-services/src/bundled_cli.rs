//! Intent-managed copies of the Codex/Claude CLIs, installed on demand.
//!
//! Only the installed-CLI launch selection calls this, and only after the
//! host's own CLI was confirmed below the adapter-derived minimum. The exact
//! package comes from offline adapter metadata (`adapter_cli::BundledCli`);
//! discovery never installs. Each version is installed with the host's npm
//! (the sibling of the npx that launches the adapter) into a private staging
//! directory, marked complete, then renamed into place, so a crash or timeout
//! never leaves a partial runtime that later launches would trust. A complete
//! install is reused without running npm again.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use intent_providers::adapter_cli::BundledCli;
use intent_providers::installed_cli::{InstalledCli, InstalledCliRuntime};
use tokio::sync::Mutex;

const INSTALL_TIMEOUT: Duration = Duration::from_secs(600);
const READY: &str = ".intent-bundled-ready";
const STDERR_TAIL: usize = 600;

/// `<home>/.local/share/intent/providers/bundled-cli`, beside Intent's other
/// managed provider runtimes. `home` is the launch environment's home.
#[must_use]
pub fn root_in(home: &Path) -> Option<PathBuf> {
    home.is_absolute()
        .then(|| home.join(".local/share/intent/providers/bundled-cli"))
}

/// The installation directory for one exact package version.
#[must_use]
pub fn version_dir(root: &Path, bundled: &BundledCli) -> PathBuf {
    let package: String = bundled
        .package
        .trim_start_matches('@')
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    root.join(package).join(bundled.version.to_string())
}

fn bin_dir(dir: &Path) -> PathBuf {
    dir.join("node_modules").join(".bin")
}

/// A complete install's validated entry point, without running npm.
#[must_use]
pub fn existing(
    root: &Path,
    cli: InstalledCli,
    bundled: &BundledCli,
) -> Option<InstalledCliRuntime> {
    complete(&version_dir(root, bundled), cli, bundled)
}

fn complete(dir: &Path, cli: InstalledCli, bundled: &BundledCli) -> Option<InstalledCliRuntime> {
    let marker = std::fs::read_to_string(dir.join(READY)).ok()?;
    if marker.trim() != bundled.spec() {
        return None;
    }
    cli.resolve_in_dirs(&[bin_dir(dir)], cfg!(windows)).ok()
}

fn install_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// npm from the same toolchain as the adapter's npx.
fn find_npm() -> Option<PathBuf> {
    let npx = intent_providers::find_npx()?;
    let dir = npx.parent()?;
    let names: &[&str] = if cfg!(windows) {
        &["npm.cmd", "npm.exe"]
    } else {
        &["npm"]
    };
    names
        .iter()
        .map(|name| dir.join(name))
        .find(|path| intent_core::path_utils::is_executable_file(path))
}

/// Return the managed CLI, installing it first when no complete copy exists.
///
/// # Errors
/// Returns an actionable reason when npm is unavailable or the install fails.
pub async fn ensure(
    root: &Path,
    cli: InstalledCli,
    bundled: &BundledCli,
) -> Result<InstalledCliRuntime, String> {
    if let Some(runtime) = existing(root, cli, bundled) {
        return Ok(runtime);
    }
    // Unit tests seed complete installs or call `ensure_in` with a fake npm;
    // they must never reach the registry through the host's npm.
    if cfg!(test) {
        return Err(format!("unit tests do not install {}", bundled.spec()));
    }
    let (npm, path) = tokio::task::spawn_blocking(|| {
        let npm = find_npm();
        let path = intent_providers::enhanced_path(npm.as_deref());
        (npm, path)
    })
    .await
    .map_err(|_| "bundled CLI lookup task failed".to_owned())?;
    let npm = npm.ok_or("npm was not found next to npx on the execution host")?;
    ensure_in(root, &npm, &path, cli, bundled, INSTALL_TIMEOUT).await
}

/// [`ensure`] with an explicit root, npm and PATH (test seam).
///
/// # Errors
/// Returns an actionable reason when the install fails or times out.
pub async fn ensure_in(
    root: &Path,
    npm: &Path,
    path: &str,
    cli: InstalledCli,
    bundled: &BundledCli,
    timeout: Duration,
) -> Result<InstalledCliRuntime, String> {
    let _lock = install_lock().lock().await;
    let destination = version_dir(root, bundled);
    if let Some(runtime) = complete(&destination, cli, bundled) {
        return Ok(runtime);
    }
    let spec = bundled.spec();
    let parent = destination
        .parent()
        .ok_or("invalid bundled CLI directory")?
        .to_owned();
    std::fs::create_dir_all(&parent)
        .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    let staging = tempfile::Builder::new()
        .prefix(".install-")
        .tempdir_in(&parent)
        .map_err(|e| format!("cannot create a staging directory for {spec}: {e}"))?;
    tracing::info!(%spec, "installing Intent's bundled CLI");
    let mut command = tokio::process::Command::new(npm);
    command
        .args([
            "install",
            "--no-save",
            "--no-package-lock",
            "--no-audit",
            "--no-fund",
            "--loglevel=error",
            "--prefix",
        ])
        .arg(staging.path())
        .arg(&spec)
        .current_dir(staging.path())
        .env("PATH", path)
        .env("NPM_CONFIG_UPDATE_NOTIFIER", "false")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(timeout, command.output()).await {
        Err(_) => {
            return Err(format!(
                "npm install {spec} timed out after {}s",
                timeout.as_secs()
            ))
        }
        Ok(Err(e)) => return Err(format!("npm install {spec} could not start: {e}")),
        Ok(Ok(output)) => output,
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        let tail = stderr
            .char_indices()
            .rev()
            .nth(STDERR_TAIL)
            .map_or(stderr, |(i, _)| &stderr[i..]);
        return Err(format!(
            "npm install {spec} failed ({}): {tail}",
            output.status
        ));
    }
    if cli
        .resolve_in_dirs(&[bin_dir(staging.path())], cfg!(windows))
        .is_err()
    {
        return Err(format!(
            "npm install {spec} did not provide a {} executable",
            cli.command()
        ));
    }
    std::fs::write(staging.path().join(READY), &spec)
        .map_err(|e| format!("cannot mark {spec} complete: {e}"))?;
    if std::fs::symlink_metadata(&destination).is_ok() {
        // An incomplete or foreign earlier copy is never trusted.
        std::fs::remove_dir_all(&destination)
            .map_err(|e| format!("cannot replace {}: {e}", destination.display()))?;
    }
    let staged = staging.keep();
    if let Err(e) = std::fs::rename(&staged, &destination) {
        let _ = std::fs::remove_dir_all(&staged);
        return Err(format!("cannot activate {spec}: {e}"));
    }
    prune(&parent, &destination);
    complete(&destination, cli, bundled)
        .ok_or_else(|| format!("{spec} is not usable after install"))
}

/// Remove superseded versions and abandoned staging directories of this
/// package (installs are serialized); failures, e.g. a running executable on
/// Windows, only defer cleanup to a later install.
fn prune(parent: &Path, keep: &Path) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path != keep && entry.file_type().is_ok_and(|t| t.is_dir()) {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

#[cfg(test)]
#[path = "bundled_cli/tests.rs"]
mod tests;
