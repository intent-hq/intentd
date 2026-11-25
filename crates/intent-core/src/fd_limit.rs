//! Process descriptor limit (`RLIMIT_NOFILE`) policy for a daemon process,
//! `intentd serve` and `intentd-orchestrator` (intent-hq/intent#4390):
//! raise the soft limit as far as the OS allows at startup, log the result,
//! and keep the sampled soft limit readable for `system.status`. Unix only;
//! every entry point is a no-op elsewhere.

use std::sync::OnceLock;

/// macOS `setrlimit` rejects a soft `RLIMIT_NOFILE` above
/// `kern.maxfilesperproc` even when the hard limit is `RLIM_INFINITY`;
/// `OPEN_MAX` (10240) is the portable ceiling that always succeeds there.
/// Linux enforces the finite hard limit itself, so no extra cap applies.
#[cfg(target_os = "macos")]
pub const PLATFORM_CAP: Option<u64> = Some(10240);
#[cfg(not(target_os = "macos"))]
pub const PLATFORM_CAP: Option<u64> = None;

/// Soft limit in effect after the startup raise (or the untouched value
/// when no raise was needed/possible). Set once by `raise_at_startup`.
static SOFT_LIMIT: OnceLock<u64> = OnceLock::new();

/// The soft `RLIMIT_NOFILE` sampled at startup, for `system.status`
/// reporting. `None` before `raise_at_startup` ran or when the limit
/// could not be read (non-Unix, `getrlimit` failure).
pub fn soft_limit() -> Option<u64> {
    SOFT_LIMIT.get().copied()
}

/// Pure raise decision: given the current `soft` limit, the `hard` limit
/// (`None` = `RLIM_INFINITY`) and the platform cap, return the soft value
/// to set, or `None` to leave the limit untouched. The target is the hard
/// limit bounded by the cap; an unlimited hard limit with no cap has no
/// finite target. The soft limit is never lowered.
///
/// `(hard = None, cap = None)` is effectively unreachable on Linux: the
/// kernel refuses `RLIM_INFINITY` for `RLIMIT_NOFILE` (EPERM above
/// `fs.nr_open`), so the hard limit is always finite there. That branch
/// is macOS-without-cap territory only, and macOS always has a cap.
pub fn target_soft(soft: u64, hard: Option<u64>, cap: Option<u64>) -> Option<u64> {
    let target = match (hard, cap) {
        (Some(hard), Some(cap)) => hard.min(cap),
        (Some(hard), None) => hard,
        (None, Some(cap)) => cap,
        (None, None) => return None,
    };
    (target > soft).then_some(target)
}

/// Current `(soft, hard)` `RLIMIT_NOFILE`; `hard == None` means unlimited.
#[cfg(unix)]
pub fn read() -> std::io::Result<(u64, Option<u64>)> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `lim` is a valid, writable `rlimit` for the call's duration.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut lim) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((to_u64(lim.rlim_cur), finite(lim.rlim_max)))
}

#[cfg(not(unix))]
pub fn read() -> std::io::Result<(u64, Option<u64>)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "RLIMIT_NOFILE is not available on this platform",
    ))
}

/// Render a hard limit for logs/doctor output.
pub fn fmt_hard(hard: Option<u64>) -> String {
    hard.map_or_else(|| "unlimited".to_string(), |h| h.to_string())
}

/// Raise the soft limit per `target_soft` and log one INFO line with the
/// resulting limits. Failures are WARN-only; the limit is left untouched.
#[cfg(unix)]
pub fn raise_at_startup() {
    let (soft, hard) = match read() {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "cannot read fd limit (getrlimit)");
            return;
        }
    };
    let mut raised_from = None;
    if let Some(target) = target_soft(soft, hard, PLATFORM_CAP) {
        let lim = libc::rlimit {
            rlim_cur: target as libc::rlim_t,
            rlim_max: hard.map_or(libc::RLIM_INFINITY, |h| h as libc::rlim_t),
        };
        // SAFETY: `lim` is a valid, initialized `rlimit` for the call's duration.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const lim) } == 0 {
            raised_from = Some(soft);
        } else {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                soft,
                target,
                hard = %fmt_hard(hard),
                "cannot raise fd limit (setrlimit); leaving it unchanged"
            );
        }
    }
    // Re-read so the logged/stored value is what the kernel actually applied.
    let (soft, hard) = read().unwrap_or((soft, hard));
    let _ = SOFT_LIMIT.set(soft);
    tracing::info!(
        soft,
        hard = %fmt_hard(hard),
        raised_from = %raised_from.map_or_else(|| "none".to_string(), |s| s.to_string()),
        "fd limit"
    );
}

#[cfg(not(unix))]
pub fn raise_at_startup() {}

#[cfg(unix)]
fn finite(v: libc::rlim_t) -> Option<u64> {
    (v != libc::RLIM_INFINITY).then(|| to_u64(v))
}

/// `rlim_t` is `u64` on the tier-1 Unix targets but not universally.
#[cfg(unix)]
#[expect(clippy::unnecessary_cast)]
fn to_u64(v: libc::rlim_t) -> u64 {
    v as u64
}
