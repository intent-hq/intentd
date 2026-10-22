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
    let (lookup_root, lookup) = (root.to_owned(), bundled.clone());
    if let Some(runtime) = blocking(move || Ok(existing(&lookup_root, cli, &lookup))).await? {
        return Ok(runtime);
    }
    // Unit tests seed complete installs or call `ensure_in` with a fake npm;
    // they must never reach the registry through the host's npm.
    if cfg!(test) {
        return Err(format!("unit tests do not install {}", bundled.spec()));
    }
    let (npm, path) = blocking(|| {
        let npm = find_npm();
        let path = intent_providers::enhanced_path(npm.as_deref());
        Ok((npm, path))
    })
    .await?;
    let npm = npm.ok_or("npm was not found next to npx on the execution host")?;
    ensure_in(root, &npm, &path, cli, bundled, INSTALL_TIMEOUT).await
}

/// Filesystem work (marker reads, tree removal, renames) stays off the executor.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| "bundled CLI filesystem task failed".to_owned())?
}

/// [`ensure`] with an explicit root, npm and PATH (test seam).
///
/// Installs are serialized within this daemon and, through an exclusive lock
/// on the package's `.install.lock` file, with every other daemon or doctor
/// process sharing the home: the completeness recheck, install, activation
/// and pruning all happen under it.
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
    let spec = bundled.spec();
    let parent = destination
        .parent()
        .ok_or("invalid bundled CLI directory")?
        .to_owned();
    let lock_parent = parent.clone();
    // Held until this function returns; dropping the file releases the lock.
    let _file_lock = blocking(move || {
        std::fs::create_dir_all(&lock_parent)
            .map_err(|e| format!("cannot create {}: {e}", lock_parent.display()))?;
        lock_install(&lock_parent.join(LOCK))
    })
    .await?;
    let (dest, pin) = (destination.clone(), bundled.clone());
    if let Some(runtime) = blocking(move || Ok(complete(&dest, cli, &pin))).await? {
        return Ok(runtime);
    }
    let (staging_parent, staging_spec) = (parent.clone(), spec.clone());
    let staging = blocking(move || {
        tempfile::Builder::new()
            .prefix(".install-")
            .tempdir_in(&staging_parent)
            .map_err(|e| format!("cannot create a staging directory for {staging_spec}: {e}"))
    })
    .await?;
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
        return Err(format!(
            "npm install {spec} failed ({}): {}",
            output.status,
            tail(&output.stderr)
        ));
    }
    let staged = cli
        .resolve_in_dirs(&[bin_dir(staging.path())], cfg!(windows))
        .map_err(|_| {
            format!(
                "npm install {spec} did not provide a {} executable",
                cli.command()
            )
        })?;
    // Packages that finish their binary in an install script (or a missing
    // platform package) can leave a placeholder: only a CLI that reports the
    // pinned version is ever marked complete.
    verify_version(staged.path(), staging.path(), path, bundled).await?;
    let pin = bundled.clone();
    blocking(move || activate(staging, &destination, cli, &pin)).await
}

const VERIFY_TIMEOUT: Duration = Duration::from_secs(60);

async fn verify_version(
    executable: &Path,
    cwd: &Path,
    path: &str,
    bundled: &BundledCli,
) -> Result<(), String> {
    let spec = bundled.spec();
    let mut command = tokio::process::Command::new(executable);
    command
        .arg("--version")
        .current_dir(cwd)
        .env("PATH", path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(VERIFY_TIMEOUT, command.output()).await {
        Err(_) => {
            return Err(format!(
                "installed {spec} did not report its version in time"
            ))
        }
        Ok(Err(e)) => return Err(format!("installed {spec} could not run: {e}")),
        Ok(Ok(output)) => output,
    };
    let version = bundled.version.to_string();
    let reported = String::from_utf8_lossy(&output.stdout);
    if output.status.success()
        && reported
            .split_whitespace()
            .any(|t| t.trim_start_matches('v') == version)
    {
        return Ok(());
    }
    Err(format!(
        "installed {spec} is not runnable (--version exited {}): {}",
        output.status,
        tail(if output.stderr.is_empty() {
            &output.stdout
        } else {
            &output.stderr
        })
    ))
}

fn tail(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    text.char_indices()
        .rev()
        .nth(STDERR_TAIL)
        .map_or(text, |(i, _)| &text[i..])
        .to_owned()
}

/// Mark the verified staging tree complete and move it into place (under the
/// install lock). A complete copy is adopted; an incomplete one is moved
/// aside first so the final rename never merges trees.
fn activate(
    staging: tempfile::TempDir,
    destination: &Path,
    cli: InstalledCli,
    bundled: &BundledCli,
) -> Result<InstalledCliRuntime, String> {
    let spec = bundled.spec();
    std::fs::write(staging.path().join(READY), &spec)
        .map_err(|e| format!("cannot mark {spec} complete: {e}"))?;
    if let Some(runtime) = complete(destination, cli, bundled) {
        return Ok(runtime);
    }
    let parent = destination
        .parent()
        .ok_or("invalid bundled CLI directory")?;
    if std::fs::symlink_metadata(destination).is_ok() {
        let stale = parent.join(format!(".stale-{}", uuid::Uuid::new_v4()));
        std::fs::rename(destination, &stale)
            .map_err(|e| format!("cannot replace {}: {e}", destination.display()))?;
        let _ = std::fs::remove_dir_all(stale);
    }
    let staged = staging.keep();
    if let Err(e) = std::fs::rename(&staged, destination) {
        let _ = std::fs::remove_dir_all(&staged);
        // Lost a race with another daemon installing the same version.
        return complete(destination, cli, bundled)
            .ok_or_else(|| format!("cannot activate {spec}: {e}"));
    }
    prune(parent, destination);
    complete(destination, cli, bundled).ok_or_else(|| format!("{spec} is not usable after install"))
}

/// Exclusive lock shared with every process using this home (daemons and
/// standalone doctor runs); released when dropped. Unix uses an advisory
/// `flock`; Windows holds the lock file open without sharing.
#[cfg(unix)]
type InstallLock = nix::fcntl::Flock<std::fs::File>;
#[cfg(not(unix))]
type InstallLock = std::fs::File;

fn lock_install(path: &Path) -> Result<InstallLock, String> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(false).write(true);
    #[cfg(unix)]
    {
        let file = options
            .open(path)
            .map_err(|e| format!("cannot open the bundled CLI install lock: {e}"))?;
        nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusive)
            .map_err(|(_, errno)| format!("cannot lock the bundled CLI install: {errno}"))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // ERROR_SHARING_VIOLATION: another process holds the lock. Wait as
        // long as one install may take, then give up rather than hang.
        const SHARING_VIOLATION: i32 = 32;
        options.share_mode(0);
        let deadline = std::time::Instant::now() + INSTALL_TIMEOUT + VERIFY_TIMEOUT;
        loop {
            match options.open(path) {
                Ok(file) => return Ok(file),
                Err(e)
                    if e.raw_os_error() == Some(SHARING_VIOLATION)
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(250));
                }
                Err(e) => return Err(format!("cannot lock the bundled CLI install: {e}")),
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        options
            .open(path)
            .map_err(|e| format!("cannot open the bundled CLI install lock: {e}"))
    }
}

/// Versions kept per package, so daemons with different adapter pins that
/// share a home do not delete each other's runtimes.
const KEEP_VERSIONS: usize = 3;
const LOCK: &str = ".install.lock";

/// Remove all but the newest [`KEEP_VERSIONS`] versions, plus staging and
/// moved-aside trees. Callers hold the install lock, so no other process is
/// staging: every hidden directory is abandoned. Failures, e.g. a running
/// executable on Windows, only defer cleanup.
fn prune(parent: &Path, keep: &Path) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let mut versions = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path == keep || !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            let _ = std::fs::remove_dir_all(path);
        } else if let Some(version) = BundledCli::new("", &name).map(|dir| dir.version) {
            versions.push((version, path));
        }
    }
    versions.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, path) in versions.into_iter().skip(KEEP_VERSIONS - 1) {
        let _ = std::fs::remove_dir_all(path);
    }
}

#[cfg(test)]
#[path = "bundled_cli/tests.rs"]
mod tests;
