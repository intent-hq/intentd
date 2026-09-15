//! Host loadability probe behind `system.capabilities.microvmSupported` and
//! the `sandbox.options` `microvm` row: runs `intentd-microvm-helper --probe`
//! (which dlopens libkrun exactly as boot would) and caches the outcome, so a
//! host where libkrun is installed but cannot be loaded — missing transitive
//! dylibs such as libepoxy / virglrenderer — or where the helper binary is
//! missing next to intentd reports microVM as unavailable with the dlopen
//! diagnostic instead of failing at the first agent spawn.
//!
//! Cost contract: a success is cached for the daemon's lifetime, a failure
//! for [`FAILURE_TTL`] (so a `brew install` is picked up without a restart
//! while read RPCs never spawn a process per call). Concurrent callers join
//! the one in-flight probe; the helper is bounded by [`PROBE_TIMEOUT`] and a
//! timeout counts as a failure. The probe is prewarmed at daemon start on
//! capable platforms and otherwise runs lazily on first request.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::orchestrator::resolve_helper_exe;
use super::MicrovmError;

/// Upper bound on one `--probe` run; a dlopen either succeeds or fails
/// immediately, so a hang is a broken helper and counts as unavailable.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a failed probe is served from cache before the next call
/// re-probes.
pub const FAILURE_TTL: Duration = Duration::from_secs(30);
/// Maximum bytes of helper stderr carried into the reason string.
const STDERR_TAIL_BYTES: usize = 512;
/// Helper exit codes (see `intentd-microvm-helper`): unavailable / API error.
const EXIT_UNAVAILABLE: i32 = 69;
const EXIT_KRUN_API: i32 = 70;

/// `Ok(())` when the helper loaded libkrun; `Err(reason)` otherwise, with a
/// user-facing reason that carries the helper's diagnostic.
pub type HostProbeResult = Result<(), String>;

struct Cached {
    result: HostProbeResult,
    /// `None` = lifetime (successes and test-seeded values).
    expires_at: Option<Instant>,
}

/// Process-wide probe cache. Held as an `Arc` on `Services` so every clone
/// shares one result and one in-flight probe.
pub struct HostProbeCache {
    slot: Mutex<Option<Cached>>,
    /// Single-flight gate: a miss probes while holding it, and waiters are
    /// then served from the fresh entry.
    gate: tokio::sync::Mutex<()>,
}

impl Default for HostProbeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl HostProbeCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            slot: Mutex::new(None),
            gate: tokio::sync::Mutex::new(()),
        }
    }

    /// Test seam: pin the outcome for the daemon's lifetime so services tests
    /// never spawn the real helper.
    pub(crate) fn seed(&self, result: HostProbeResult) {
        *self.slot.lock().unwrap() = Some(Cached {
            result,
            expires_at: None,
        });
    }

    fn cached(&self) -> Option<HostProbeResult> {
        let slot = self.slot.lock().unwrap();
        slot.as_ref()
            .filter(|c| c.expires_at.is_none_or(|t| Instant::now() < t))
            .map(|c| c.result.clone())
    }

    /// Cached-or-fresh probe outcome (see the module docs for the caching
    /// contract).
    pub(crate) async fn result(&self) -> HostProbeResult {
        if let Some(result) = self.cached() {
            return result;
        }
        let _gate = self.gate.lock().await;
        if let Some(result) = self.cached() {
            return result;
        }
        let result = run_probe().await;
        let expires_at = result.is_err().then(|| Instant::now() + FAILURE_TTL);
        *self.slot.lock().unwrap() = Some(Cached {
            result: result.clone(),
            expires_at,
        });
        result
    }
}

/// Spawn `intentd-microvm-helper --probe` and map its outcome onto a reason.
/// On success the helper's one-line JSON report (resolved dylib directory,
/// libkrun path and version) is logged at INFO — the startup prewarm is the
/// usual emitter, so a support bundle shows which libkrun the daemon loads.
async fn run_probe() -> HostProbeResult {
    let helper = match resolve_helper_exe() {
        Ok(p) => p,
        Err(MicrovmError::HelperMissing(detail)) => {
            let searched = std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(PathBuf::from))
                .map_or_else(String::new, |d| format!("; searched {}", d.display()));
            return Err(format!(
                "microVM helper binary not found: {detail}{searched}"
            ));
        }
        Err(e) => return Err(e.to_string()),
    };
    let mut cmd = tokio::process::Command::new(&helper);
    cmd.arg("--probe")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            return Err(format!(
                "microVM helper {} could not be started: {e}",
                helper.display()
            ))
        }
    };
    // Dropping the future on timeout kills the child (`kill_on_drop`).
    let output = match tokio::time::timeout(PROBE_TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Err(format!("microVM helper probe failed to run: {e}")),
        Err(_) => {
            return Err(format!(
                "microVM helper probe did not finish within {}s",
                PROBE_TIMEOUT.as_secs()
            ))
        }
    };
    let tail = stderr_tail(&output.stderr);
    match output.status.code() {
        Some(0) => {
            let report = String::from_utf8_lossy(&output.stdout);
            let report = report.trim();
            tracing::info!(
                helper = %helper.display(),
                report = %report,
                "microVM helper probe: libkrun loadable"
            );
            Ok(())
        }
        Some(EXIT_UNAVAILABLE) => Err(format!("libkrun cannot be loaded on this host: {tail}")),
        Some(EXIT_KRUN_API) => Err(format!("libkrun probe hit a libkrun API error: {tail}")),
        Some(code) => Err(format!("microVM helper probe failed (exit {code}): {tail}")),
        None => Err(format!(
            "microVM helper probe was killed by a signal: {tail}"
        )),
    }
}

/// Last non-empty stderr lines with the helper's `intentd-microvm-helper: `
/// prefix stripped, joined on `; ` and capped at [`STDERR_TAIL_BYTES`] (tail
/// kept — dyld puts the decisive `Library not loaded:` line first, but the
/// `Reason:` line is what names the missing path).
fn stderr_tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let lines: Vec<&str> = text
        .lines()
        .map(|l| {
            l.trim()
                .strip_prefix("intentd-microvm-helper:")
                .map_or(l.trim(), str::trim)
        })
        .filter(|l| !l.is_empty())
        .collect();
    if lines.is_empty() {
        return "helper stderr was empty".to_string();
    }
    let joined = lines.join("; ");
    if joined.len() <= STDERR_TAIL_BYTES {
        return joined;
    }
    let mut start = joined.len() - STDERR_TAIL_BYTES;
    while !joined.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &joined[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_tail_strips_prefix_and_joins_lines() {
        let raw = b"intentd-microvm-helper: failed to load libkrun.dylib: dlopen(...): \
                    Library not loaded: /opt/homebrew/opt/libepoxy/lib/libepoxy.0.dylib\n  \
                    Referenced from: libkrun.1.19.4.dylib\n\n  Reason: tried: (no such file)\n";
        let tail = stderr_tail(raw);
        assert!(tail.starts_with("failed to load libkrun.dylib"), "{tail}");
        assert!(
            tail.contains("libepoxy.0.dylib; Referenced from:"),
            "{tail}"
        );
        assert!(tail.ends_with("Reason: tried: (no such file)"), "{tail}");
        assert_eq!(stderr_tail(b"\n  \n"), "helper stderr was empty");
    }

    #[test]
    fn stderr_tail_keeps_the_end_when_over_budget() {
        let raw = format!("{}END", "x".repeat(STDERR_TAIL_BYTES * 2));
        let tail = stderr_tail(raw.as_bytes());
        assert!(tail.starts_with('…'), "{tail}");
        assert!(tail.ends_with("END"), "{tail}");
        assert!(tail.len() <= STDERR_TAIL_BYTES + '…'.len_utf8());
    }

    #[tokio::test]
    async fn seeded_result_is_served_without_spawning() {
        let cache = HostProbeCache::new();
        cache.seed(Err("libkrun cannot be loaded on this host: test".into()));
        assert_eq!(
            cache.result().await,
            Err("libkrun cannot be loaded on this host: test".to_string())
        );
        cache.seed(Ok(()));
        assert_eq!(cache.result().await, Ok(()));
    }

    /// Without a helper next to the test binary (and no env override) the
    /// live probe reports the missing helper with the directory it searched
    /// — and caches that failure with a TTL rather than for the lifetime.
    #[cfg(unix)]
    #[tokio::test]
    async fn missing_helper_reports_searched_path_and_expires() {
        if std::env::var_os(super::super::orchestrator::HELPER_EXE_ENV).is_some() {
            eprintln!("Skipping: helper override set in the environment");
            return;
        }
        let cache = HostProbeCache::new();
        let err = cache
            .result()
            .await
            .expect_err("no helper next to test binary");
        assert!(err.contains("microVM helper binary not found"), "{err}");
        assert!(err.contains("searched "), "{err}");
        let cached = cache.slot.lock().unwrap();
        let entry = cached.as_ref().expect("failure cached");
        assert!(entry.expires_at.is_some(), "failures carry a TTL");
    }
}
