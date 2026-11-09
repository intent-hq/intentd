//! Display-only `cliVersion` for local-binary discovery rows that no gate
//! probes (auggie, opencode, unsloth, grok, …). Purely descriptive: a failed
//! probe omits the field and never changes `installed`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use intent_providers::{auggie_cli_gate, PiCliGate, PiCliProbe, ProviderAvailability};

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a failed probe is remembered, so a hanging CLI does not add
/// [`PROBE_TIMEOUT`] to every discovery call.
const FAILURE_TTL: Duration = Duration::from_secs(600);
const MAX_VERSION_CHARS: usize = 80;

type Fingerprint = (u64, Option<SystemTime>);

/// Where a row's version comes from.
enum Target {
    Binary(PathBuf),
    /// Auggie launches the first candidate that is not too old
    /// (`select_auggie_for_spawn`), so the row reports that one.
    AuggieCandidates(Vec<PathBuf>),
}

/// The binary whose `--version` describes the row: the `providers.paths`
/// override when valid, else auto-detection. Unsloth reports its own CLI
/// (the secondary), not the shared opencode primary. Rows that are gated,
/// not installed, npx-launched, or probed elsewhere (Cursor) are skipped.
fn probe_target<S: std::hash::BuildHasher>(
    p: &ProviderAvailability,
    provider_paths: &HashMap<String, String, S>,
) -> Option<Target> {
    if p.gated_off.is_some()
        || !p.installed
        || p.npx_only_package.is_some()
        || matches!(p.id, "cursor" | "mock")
    {
        return None;
    }
    let explicit = provider_paths.get(p.id).map(String::as_str);
    if p.id == "auggie" {
        return Some(Target::AuggieCandidates(
            intent_providers::find_auggie_candidates(explicit),
        ));
    }
    match &p.secondary_binary {
        Some(secondary) => {
            intent_providers::find_provider_binary(secondary.command, secondary.command, explicit)
        }
        None => intent_providers::find_provider_binary(p.id, p.command, explicit),
    }
    .map(Target::Binary)
}

fn target_version(target: &Target) -> Option<String> {
    match target {
        Target::Binary(path) => cached_version(path),
        Target::AuggieCandidates(paths) => paths.iter().find_map(|path| {
            let version = cached_version(path);
            let probe = version
                .clone()
                .map_or(PiCliProbe::Failed, PiCliProbe::Output);
            match auggie_cli_gate(&probe) {
                PiCliGate::TooOld(_) => None,
                _ => Some(version),
            }
        })?,
    }
}

fn fingerprint(path: &Path) -> Option<Fingerprint> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()))
}

struct Cached {
    print: Fingerprint,
    version: Option<String>,
    probed_at: Instant,
}

fn cache() -> &'static Mutex<HashMap<PathBuf, Cached>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, Cached>>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

/// `--version` of `path`, cached until the binary changes. A failure is
/// cached for [`FAILURE_TTL`] only, so a transient timeout recovers.
fn cached_version(path: &Path) -> Option<String> {
    cached_version_with(path, FAILURE_TTL)
}

fn cached_version_with(path: &Path, failure_ttl: Duration) -> Option<String> {
    let print = fingerprint(path)?;
    if let Some(hit) = cache().lock().ok()?.get(path) {
        if hit.print == print && (hit.version.is_some() || hit.probed_at.elapsed() < failure_ttl) {
            return hit.version.clone();
        }
    }
    let version = run_version_probe(path);
    if let Ok(mut cache) = cache().lock() {
        cache.insert(
            path.to_owned(),
            Cached {
                print,
                version: version.clone(),
                probed_at: Instant::now(),
            },
        );
    }
    version
}

/// Probe every eligible row in parallel; keyed by provider id.
pub(crate) fn probe_display_versions<S: std::hash::BuildHasher>(
    providers: &[ProviderAvailability],
    provider_paths: &HashMap<String, String, S>,
) -> HashMap<&'static str, String> {
    let targets: Vec<(&'static str, Target)> = providers
        .iter()
        .filter_map(|p| probe_target(p, provider_paths).map(|target| (p.id, target)))
        .collect();
    std::thread::scope(|scope| {
        let handles: Vec<_> = targets
            .iter()
            .map(|(id, target)| scope.spawn(move || target_version(target).map(|v| (*id, v))))
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
/// PATH (npm shims need `node`), bounded by [`PROBE_TIMEOUT`] including
/// the stdout read (a grandchild may hold the pipe open). The group is
/// killed only on timeout, before the child is reaped, so a recycled
/// process-group id is never signalled.
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
    let start = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    if let Some(mut stdout) = child.stdout.take() {
        std::thread::spawn(move || {
            let mut out = Vec::new();
            let _ = (&mut stdout).take(64 * 1024).read_to_end(&mut out);
            let _ = tx.send(out);
        });
    }
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(Some(_)) | Err(_) => return None,
            Ok(None) if start.elapsed() >= PROBE_TIMEOUT => {
                kill_group(pid);
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
        }
    }
    let out = rx
        .recv_timeout(PROBE_TIMEOUT.saturating_sub(start.elapsed()))
        .ok()?;
    version_line(&out)
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

    #[test]
    fn failed_probe_is_remembered_until_its_ttl_expires() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("tool");
        let marker = dir.path().join("runs");
        std::fs::write(
            &bin,
            format!("#!/bin/sh\necho x >> '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let runs = || {
            std::fs::read_to_string(&marker)
                .unwrap_or_default()
                .lines()
                .count()
        };
        assert_eq!(cached_version_with(&bin, Duration::from_secs(600)), None);
        assert_eq!(cached_version_with(&bin, Duration::from_secs(600)), None);
        assert_eq!(runs(), 1);
        assert_eq!(cached_version_with(&bin, Duration::ZERO), None);
        assert_eq!(runs(), 2);
    }
}
