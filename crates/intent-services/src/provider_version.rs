//! Display-only `cliVersion` for local-binary discovery rows that no gate
//! probes (auggie, opencode, unsloth, grok, …). Purely descriptive: a failed
//! probe omits the field and never changes `installed`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use intent_providers::ProviderAvailability;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_VERSION_CHARS: usize = 80;

type Fingerprint = (u64, Option<SystemTime>);

/// The binary whose `--version` describes the row: the `providers.paths`
/// override when valid, else auto-detection. Unsloth reports its own CLI
/// (the secondary), not the shared opencode primary. Rows that are gated,
/// not installed, npx-launched, or probed elsewhere (Cursor) are skipped.
fn probe_path<S: std::hash::BuildHasher>(
    p: &ProviderAvailability,
    provider_paths: &HashMap<String, String, S>,
) -> Option<PathBuf> {
    if p.gated_off.is_some()
        || !p.installed
        || p.npx_only_package.is_some()
        || matches!(p.id, "cursor" | "mock")
    {
        return None;
    }
    let explicit = provider_paths.get(p.id).map(String::as_str);
    match &p.secondary_binary {
        Some(secondary) => {
            intent_providers::find_provider_binary(secondary.command, secondary.command, explicit)
        }
        None => intent_providers::find_provider_binary(p.id, p.command, explicit),
    }
}

fn fingerprint(path: &Path) -> Option<Fingerprint> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()))
}

fn cache() -> &'static Mutex<HashMap<PathBuf, (Fingerprint, String)>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, (Fingerprint, String)>>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

/// `--version` of `path`, cached until the binary changes. Failures are
/// not cached, so a transient timeout is retried on the next discovery.
fn cached_version(path: &Path) -> Option<String> {
    let print = fingerprint(path)?;
    if let Some((cached, version)) = cache().lock().ok()?.get(path) {
        if *cached == print {
            return Some(version.clone());
        }
    }
    let version = run_version_probe(path)?;
    if let Ok(mut cache) = cache().lock() {
        cache.insert(path.to_owned(), (print, version.clone()));
    }
    Some(version)
}

/// Probe every eligible row in parallel; keyed by provider id.
pub(crate) fn probe_display_versions<S: std::hash::BuildHasher>(
    providers: &[ProviderAvailability],
    provider_paths: &HashMap<String, String, S>,
) -> HashMap<&'static str, String> {
    let targets: Vec<(&'static str, PathBuf)> = providers
        .iter()
        .filter_map(|p| probe_path(p, provider_paths).map(|path| (p.id, path)))
        .collect();
    std::thread::scope(|scope| {
        let handles: Vec<_> = targets
            .iter()
            .map(|(id, path)| scope.spawn(move || cached_version(path).map(|v| (*id, v))))
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().ok().flatten())
            .collect()
    })
}

fn version_line(stdout: &[u8]) -> Option<String> {
    let line = String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())?
        .to_owned();
    (line.chars().count() <= MAX_VERSION_CHARS && !line.chars().any(char::is_control))
        .then_some(line)
}

fn kill_group(pid: u32) {
    #[cfg(unix)]
    if let Ok(pgid) = i32::try_from(pid) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pgid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(not(unix))]
    let _ = pid;
}

/// `<path> --version` in its own process group with the provider spawn
/// PATH (npm shims need `node`), bounded by [`PROBE_TIMEOUT`].
fn run_version_probe(path: &Path) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let mut cmd = Command::new(path);
    cmd.arg("--version")
        .env("PATH", intent_providers::enhanced_path(Some(path)))
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = cmd.spawn().ok()?;
    let pid = child.id();
    let start = std::time::Instant::now();
    let result = loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                kill_group(pid);
                let mut out = Vec::new();
                child.stdout.take()?.read_to_end(&mut out).ok()?;
                break version_line(&out);
            }
            Ok(Some(_)) | Err(_) => break None,
            Ok(None) if start.elapsed() >= PROBE_TIMEOUT => {
                kill_group(pid);
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
        }
    };
    kill_group(pid);
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn version_line_takes_first_nonempty_plain_line() {
        assert_eq!(version_line(b"\n  1.18.18 \nmore"), Some("1.18.18".into()));
        assert_eq!(version_line(b"\x1b[1m1.0\x1b[0m"), None);
        assert_eq!(version_line("x".repeat(81).as_bytes()), None);
        assert_eq!(version_line(b"   \n"), None);
    }

    #[test]
    fn cached_version_reprobes_after_the_binary_changes() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tool");
        let write = |script: &str| {
            std::fs::write(&bin, script).unwrap();
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        };
        write("#!/bin/sh\necho 1.0.0\n");
        assert_eq!(cached_version(&bin), Some("1.0.0".into()));
        write("#!/bin/sh\necho 10.20.30\n");
        assert_eq!(cached_version(&bin), Some("10.20.30".into()));
        write("#!/bin/sh\nexit 1\n");
        assert_eq!(cached_version(&bin), None);
    }
}
