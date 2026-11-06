//! Recover missing npm exec metadata without replacing a shared installation.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha512};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

/// Prevent user npm settings from suppressing the cache manifest on installation.
pub const NPX_SAVE_ARG: &str = "--save=true";

/// Whether a command inherits process variables or holds a complete snapshot.
#[derive(Clone, Copy)]
pub enum CommandEnvironment {
    Inherit,
    Complete,
}

/// Restore only an absent cache-root manifest for an installed, reviewed pin.
/// Config discovery uses the launch's npm installation, environment and cwd.
/// Failure is advisory: the original provider launch still supplies its error.
pub async fn repair_npx_cache(
    command: &Command,
    package: &str,
    environment: CommandEnvironment,
) -> bool {
    // npm.cmd cleanup cannot yet own its node.exe process tree.
    if !cfg!(unix) {
        return false;
    }
    if !intent_providers::ACP_PROVIDERS.iter().any(|provider| {
        provider.npx_only_package == Some(package) || provider.fallback_npx_package == Some(package)
    }) {
        return false;
    }
    let Some(cache) = cache_root(command, environment).await else {
        return false;
    };
    let package = package.to_owned();
    tokio::task::spawn_blocking(move || repair_manifest(&cache, &package))
        .await
        .unwrap_or(false)
}

async fn cache_root(launch: &Command, environment: CommandEnvironment) -> Option<PathBuf> {
    let launch = launch.as_std();
    let npx = Path::new(launch.get_program());
    let npm_name = match npx.file_name()?.to_str()? {
        "npx" => "npm",
        "npx.cmd" => "npm.cmd",
        "npx.exe" => "npm.exe",
        "npx-cli.js" => "npm-cli.js",
        _ => return None,
    };
    let npm = npx.with_file_name(npm_name);
    if !npm.is_file() {
        return None;
    }
    let mut command = Command::new(npm);
    command
        .args(["config", "get", "cache", "--workspaces=false"])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(cwd) = launch.get_current_dir() {
        command.current_dir(cwd);
    }
    if matches!(environment, CommandEnvironment::Inherit) {
        command.envs(std::env::vars_os());
    }
    for (key, value) in launch.get_envs() {
        if let Some(value) = value {
            command.env(key, value);
        } else {
            command.env_remove(key);
        }
    }
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn().ok()?;
    let mut probe = CacheProbe { child };
    let stdout = probe.child.stdout.take()?;
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        stdout.take(65_536).read_to_end(&mut bytes).await.ok()?;
        let status = probe.child.wait().await.ok()?;
        if !status.success() {
            return None;
        }
        let output = std::str::from_utf8(&bytes).ok()?;
        let output = output.trim_end_matches(['\r', '\n']);
        if output.contains(['\r', '\n']) {
            return None;
        }
        let cache = PathBuf::from(output);
        cache.is_absolute().then_some(cache)
    })
    .await;
    if result.is_err() {
        probe.stop();
        let _ = tokio::time::timeout(Duration::from_millis(500), probe.child.wait()).await;
    }
    result.ok().flatten()
}

struct CacheProbe {
    child: Child,
}

impl CacheProbe {
    fn stop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pid.cast_signed()),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        let _ = self.child.start_kill();
    }
}

impl Drop for CacheProbe {
    fn drop(&mut self) {
        if self.child.id().is_some() {
            self.stop();
        }
    }
}

fn real_directory(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_dir())
}

fn repair_manifest(cache: &Path, package: &str) -> bool {
    let Some((name, version)) = package.rsplit_once('@') else {
        return false;
    };
    let mut hash = String::with_capacity(16);
    for byte in Sha512::digest(package.as_bytes()).iter().take(8) {
        write!(hash, "{byte:02x}").expect("writing to a String cannot fail");
    }
    let npx = cache.join("_npx");
    let install = npx.join(&hash[..16]);
    let manifest = install.join("package.json");
    if !real_directory(cache) || !real_directory(&npx) || !real_directory(&install) {
        return false;
    }
    let Ok(identity) = std::fs::symlink_metadata(&install) else {
        return false;
    };
    if !matches!(std::fs::symlink_metadata(&manifest), Err(e) if e.kind() == std::io::ErrorKind::NotFound)
    {
        return false;
    }
    let mut dependency = install.join("node_modules");
    if !real_directory(&dependency) {
        return false;
    }
    for segment in name.split('/') {
        dependency.push(segment);
        if !real_directory(&dependency) {
            return false;
        }
    }
    let package_manifest = dependency.join("package.json");
    if !std::fs::symlink_metadata(&package_manifest).is_ok_and(|m| m.file_type().is_file()) {
        return false;
    }
    let read = || -> Option<Value> {
        let mut bytes = Vec::new();
        std::fs::File::open(&package_manifest)
            .ok()?
            .take(65_536)
            .read_to_end(&mut bytes)
            .ok()?;
        serde_json::from_slice(&bytes).ok()
    };
    let Some(installed) = read() else {
        return false;
    };
    if installed["name"].as_str() != Some(name) || installed["version"].as_str() != Some(version) {
        return false;
    }
    let publish = || -> std::io::Result<()> {
        let candidate_path = install.join(format!(".intent-manifest-{}.tmp", uuid::Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate_path)?;
        let candidate = ManifestCandidate(candidate_path);
        let dependencies = std::collections::BTreeMap::from([(name, version)]);
        serde_json::to_writer(
            &mut file,
            &json!({"private":true,"dependencies":dependencies}),
        )?;
        file.flush()?;
        drop(file);
        if !real_directory(&npx) || !real_directory(&install) {
            return Err(std::io::ErrorKind::NotFound.into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = std::fs::symlink_metadata(&install)?;
            if (identity.dev(), identity.ino()) != (current.dev(), current.ino()) {
                return Err(std::io::ErrorKind::NotFound.into());
            }
        }
        #[cfg(not(unix))]
        let _ = &identity;
        std::fs::hard_link(&candidate.0, &manifest)
    };
    let repaired = publish().is_ok();
    if repaired {
        tracing::info!(
            package,
            "restored missing npx cache manifest before adapter launch"
        );
    }
    repaired
}

struct ManifestCandidate(PathBuf);

impl Drop for ManifestCandidate {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
