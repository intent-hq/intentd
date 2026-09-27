//! Shared cached version prerequisite for npx-backed adapter launches.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use intent_core::{Error, Result};

/// Check the selected npx before spawning, keeping the bounded subprocess
/// probe off the async runtime. Persistent callers skip this for live children.
pub(crate) async fn check_npx_version(npx: &Path) -> Result<()> {
    let npx = npx.to_path_buf();
    tokio::task::spawn_blocking(move || {
        guard_npx_version(&npx, intent_providers::find_node().as_deref())
    })
    .await
    .map_err(|e| Error::Internal(format!("npx version probe task failed: {e}")))?
}

/// Spawn-time npx version guard (intent-hq/intent#5725): reject an `npx`
/// whose npm is older than [`intent_providers::NPX_MIN_NPM_VERSION`] with a
/// user-facing `InvalidInput` naming the stale npx, the detected `node`, and
/// the remedy — npm 6's npx rejects `npx -y <pkg>` outright, so the spawn
/// would otherwise retry three times and surface only "agent stdout closed".
/// Permissive when the probe fails or its output does not parse (same policy
/// as the pi/auggie gates). Blocking (subprocess), so async callers use
/// [`check_npx_version`] to run it off the runtime.
pub(crate) fn guard_npx_version(npx: &Path, node: Option<&Path>) -> Result<()> {
    let gate = intent_providers::npx_gate(&probe_npx_version_cached(npx));
    match intent_providers::stale_npx_reason(&gate, npx, node) {
        Some(reason) => {
            tracing::warn!(
                npx_path = ?npx,
                node_path = ?node,
                gate = ?gate,
                "rejecting stale npx before spawn"
            );
            Err(Error::InvalidInput(reason))
        }
        None => Ok(()),
    }
}

/// How long a memoized `npx --version` verdict stays valid without a
/// re-probe. A replacement that preserves every fingerprint field is
/// re-checked after this at the latest, so a repaired installation is never
/// rejected for the rest of the daemon's lifetime.
const NPX_PROBE_TTL: Duration = Duration::from_secs(5 * 60);

/// Identity of the file behind an npx path, for cache invalidation: the
/// symlink target (a repointed `/usr/local/bin/npx` changes it even when the
/// new target carries the same metadata — published npm 6/7/11 archives all
/// stamp `npx-cli.js` with the same mtime), size, mtime, and on Unix the
/// device + inode. `None` when the path cannot be read.
#[derive(Clone, Debug, PartialEq, Eq)]
struct NpxFingerprint {
    target: PathBuf,
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    dev_ino: (u64, u64),
}

fn npx_fingerprint(npx: &Path) -> Option<NpxFingerprint> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(npx).ok()?;
    Some(NpxFingerprint {
        target: std::fs::canonicalize(npx).unwrap_or_else(|_| npx.to_path_buf()),
        len: meta.len(),
        modified: meta.modified().ok(),
        #[cfg(unix)]
        dev_ino: (meta.dev(), meta.ino()),
    })
}

/// `npx --version` probe result memoized per npx path, keyed on the file's
/// [`NpxFingerprint`] and bounded by [`NPX_PROBE_TTL`], so the guard costs
/// one short subprocess per distinct npx binary per TTL window and a
/// repointed/upgraded npx is re-probed. A failed probe is cached too — that
/// outcome is permissive, so caching it only preserves the pre-guard
/// behaviour.
fn probe_npx_version_cached(npx: &Path) -> intent_providers::PiCliProbe {
    use intent_providers::PiCliProbe;
    struct CachedProbe {
        fingerprint: Option<NpxFingerprint>,
        probed_at: Instant,
        probe: PiCliProbe,
    }
    static CACHE: std::sync::OnceLock<Mutex<HashMap<PathBuf, CachedProbe>>> =
        std::sync::OnceLock::new();
    let fingerprint = npx_fingerprint(npx);
    let cache = CACHE.get_or_init(Mutex::default);
    if let Some(cached) = cache.lock().unwrap().get(npx) {
        if cached.fingerprint == fingerprint && cached.probed_at.elapsed() < NPX_PROBE_TTL {
            return cached.probe.clone();
        }
    }
    let probe = run_npx_version_probe(npx).map_or(PiCliProbe::Failed, PiCliProbe::Output);
    cache.lock().unwrap().insert(
        npx.to_path_buf(),
        CachedProbe {
            fingerprint,
            probed_at: Instant::now(),
            probe: probe.clone(),
        },
    );
    probe
}

/// Run `<npx> --version` with a 3s budget and return the trimmed first
/// stdout line, or `None` on spawn failure, nonzero exit, timeout, or empty
/// output (same shape as the `auggie_cli` / `pi_cli` probes). Probes with
/// the same enhanced PATH the real spawn uses so npx's `#!/usr/bin/env node`
/// shebang resolves the sibling `node`.
fn run_npx_version_probe(npx: &Path) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let mut child = Command::new(npx)
        .arg("--version")
        .env("PATH", intent_providers::enhanced_path(Some(npx)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let timeout = Duration::from_secs(3);
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                let mut output = Vec::new();
                child.stdout.take()?.read_to_end(&mut output).ok()?;
                let stdout = String::from_utf8_lossy(&output);
                let first_line = stdout.lines().next()?.trim();
                if first_line.is_empty() {
                    return None;
                }
                return Some(first_line.to_string());
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
