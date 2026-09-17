//! Global forge rate-limit pause gate for the background sweeps
//! (monorepo#2961).
//!
//! When the PR-refresh sweep, the git-root sweep or the PR-monitor sweep
//! hits a forge rate limit (REST 403/429 with an exhausted quota, surfaced as
//! [`intent_core::Error::RateLimited`]), continuing to call the forge for
//! every remaining root/workspace on every tick both spams WARN logs (one
//! per root per tick, masking real failures) and burns the freshly-reset
//! quota window. This gate is shared by every [`crate::Services`] clone: the
//! first rate-limited call pauses ALL forge-touching sweep work until the
//! quota window resets, and reporting coalesces to one WARN per pause window
//! (the trigger site logs exactly when [`RateLimitGate::pause_for`] reports a
//! fresh pause). Sweep-local work (submodule auto-detect, prune, commit-sha
//! backfill) never pauses — only forge calls do.

use std::time::{Duration, Instant, SystemTime};

/// Fallback pause when the forge cannot report its reset timestamp (host
/// without the signal, or the free `rate_limit` probe itself failed).
pub(crate) const RATE_LIMIT_FALLBACK_PAUSE: Duration = Duration::from_secs(5 * 60);

/// Safety margin added past the reported reset so the first post-pause sweep
/// lands after the window actually turned over (clock skew, coarse
/// second-granularity timestamps). Deterministic rather than random jitter:
/// one daemon process is the only client of its quota, so herd-avoidance
/// randomness buys nothing while making tests flaky.
pub(crate) const RATE_LIMIT_RESET_MARGIN: Duration = Duration::from_secs(30);

/// Lower bound on any pause: a reset timestamp in the past (already turned
/// over, or skewed) still backs off briefly instead of hammering the forge.
pub(crate) const RATE_LIMIT_MIN_PAUSE: Duration = Duration::from_secs(60);

/// Upper bound on any pause, defending against a nonsense reset timestamp
/// far in the future (GitHub's core window is hourly).
pub(crate) const RATE_LIMIT_MAX_PAUSE: Duration = Duration::from_secs(2 * 60 * 60);

/// How long to pause given the forge-reported reset (unix seconds) and the
/// current unix time: until the reset plus [`RATE_LIMIT_RESET_MARGIN`],
/// clamped into `[RATE_LIMIT_MIN_PAUSE, RATE_LIMIT_MAX_PAUSE]`; without a
/// reported reset, [`RATE_LIMIT_FALLBACK_PAUSE`].
pub(crate) fn pause_duration(reset_unix: Option<u64>, now_unix: u64) -> Duration {
    let base = match reset_unix {
        // Saturating: a nonsense reset near `u64::MAX` must clamp to
        // [`RATE_LIMIT_MAX_PAUSE`], not overflow `Duration` and panic.
        Some(reset) => Duration::from_secs(reset.saturating_sub(now_unix))
            .saturating_add(RATE_LIMIT_RESET_MARGIN),
        None => RATE_LIMIT_FALLBACK_PAUSE,
    };
    base.clamp(RATE_LIMIT_MIN_PAUSE, RATE_LIMIT_MAX_PAUSE)
}

/// The fixed prefix of the pause annotation carried in a PR monitor's
/// `lastError` while the gate is closed — the marker by which an earlier
/// annotation is found and replaced (a deadline extension, a re-stamp), in
/// Rust ([`annotate_pause_error`]) and in SQL
/// (`Store::annotate_active_pr_monitors_pause`) alike.
pub(crate) const PAUSE_ERROR_MARKER: &str = "rate limited; PR monitor polling paused";

/// Separator between a genuine fetch error and the pause annotation
/// appended to it.
pub(crate) const PAUSE_ERROR_SEPARATOR: &str = "; ";

/// The pause annotation naming the gate's RFC 3339 deadline (`None` only in
/// the window between the deadline elapsing and the gate re-opening).
pub(crate) fn pause_error(until: Option<&str>) -> String {
    match until {
        Some(until) => format!("{PAUSE_ERROR_MARKER} until {until}"),
        None => PAUSE_ERROR_MARKER.to_string(),
    }
}

/// The `lastError` a monitor carries while the gate is closed: `error` (a
/// genuine fetch error, or `None` after a successful poll) with the current
/// pause annotation appended. An annotation already present in `error` —
/// from an earlier stamp, or a fetch that itself hit the limit — is replaced,
/// never repeated, so the row always names the CURRENT deadline and a
/// genuine error survives any number of re-stamps.
pub(crate) fn annotate_pause_error(error: Option<&str>, pause: &str) -> String {
    let genuine = error
        .map(|e| match e.find(PAUSE_ERROR_MARKER) {
            Some(at) => e[..at]
                .strip_suffix(PAUSE_ERROR_SEPARATOR)
                .unwrap_or(&e[..at]),
            None => e,
        })
        .filter(|e| !e.is_empty());
    match genuine {
        Some(genuine) => format!("{genuine}{PAUSE_ERROR_SEPARATOR}{pause}"),
        None => pause.to_string(),
    }
}

/// The pause deadline on both clocks: the monotonic instant the gate
/// compares against, and the wall-clock time surfaced to users and agents
/// (`pausedUntil`, the pause `lastError`) — captured once at pause time so
/// every surface names the same second.
#[derive(Clone, Copy)]
struct PauseDeadline {
    at: Instant,
    wall: SystemTime,
}

/// The shared pause state. Interior-mutable so one instance can sit in an
/// `Arc` across [`crate::Services`] clones; the mutex is only ever held for
/// a read/compare/store, never across an await.
#[derive(Default)]
pub(crate) struct RateLimitGate {
    paused_until: std::sync::Mutex<Option<PauseDeadline>>,
}

impl RateLimitGate {
    fn active_deadline(&self) -> Option<PauseDeadline> {
        let deadline = (*self.paused_until.lock().expect("gate lock"))?;
        (deadline.at > Instant::now()).then_some(deadline)
    }

    /// Remaining pause, or `None` when the gate is open (never paused, or
    /// the window elapsed — the gate re-opens implicitly, no reset call).
    pub(crate) fn paused_remaining(&self) -> Option<Duration> {
        let deadline = self.active_deadline()?;
        let remaining = deadline.at.saturating_duration_since(Instant::now());
        (remaining > Duration::ZERO).then_some(remaining)
    }

    /// The wall-clock deadline of the active pause window, or `None` when
    /// the gate is open.
    pub(crate) fn paused_until(&self) -> Option<SystemTime> {
        self.active_deadline().map(|d| d.wall)
    }

    /// Pause forge-touching sweep work for `duration` from now. Returns
    /// `true` when this call opened a NEW pause window (the caller should
    /// log its one WARN); `false` when a pause was already active — the
    /// deadline is extended if the new one is later, but reporting stays
    /// coalesced to the window's first trigger.
    pub(crate) fn pause_for(&self, duration: Duration) -> bool {
        let now = Instant::now();
        let deadline = PauseDeadline {
            at: now + duration,
            wall: SystemTime::now() + duration,
        };
        let mut slot = self.paused_until.lock().expect("gate lock");
        match *slot {
            Some(existing) if existing.at > now => {
                if deadline.at > existing.at {
                    *slot = Some(deadline);
                }
                false
            }
            _ => {
                *slot = Some(deadline);
                true
            }
        }
    }

    /// Re-open the gate immediately (tests simulate the pause window
    /// elapsing without waiting out the minimum pause).
    #[cfg(test)]
    pub(crate) fn clear(&self) {
        *self.paused_until.lock().expect("gate lock") = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_duration_honors_reset_with_margin() {
        // Reset 10 minutes out → pause 10 min + margin.
        let d = pause_duration(Some(1_600), 1_000);
        assert_eq!(d, Duration::from_secs(600) + RATE_LIMIT_RESET_MARGIN);
    }

    #[test]
    fn pause_duration_clamps_past_and_far_resets() {
        // Reset already in the past still backs off the minimum.
        assert_eq!(pause_duration(Some(500), 1_000), RATE_LIMIT_MIN_PAUSE);
        // A nonsense far-future reset is capped.
        assert_eq!(
            pause_duration(Some(1_000 + 10 * 24 * 3600), 1_000),
            RATE_LIMIT_MAX_PAUSE
        );
    }

    #[test]
    fn pause_duration_falls_back_without_a_reset() {
        assert_eq!(pause_duration(None, 1_000), RATE_LIMIT_FALLBACK_PAUSE);
    }

    /// The annotation composes with a genuine error, replaces an earlier
    /// annotation (bare or appended) instead of stacking, and stands alone
    /// after a successful poll or a fetch that itself hit the limit.
    #[test]
    fn pause_annotation_composes_and_replaces_without_stacking() {
        let t1 = pause_error(Some("2026-09-17T02:39:15Z"));
        let t2 = pause_error(Some("2026-09-17T03:09:15Z"));
        assert_eq!(
            t1,
            "rate limited; PR monitor polling paused until 2026-09-17T02:39:15Z"
        );
        assert_eq!(pause_error(None), PAUSE_ERROR_MARKER);

        assert_eq!(annotate_pause_error(None, &t1), t1);
        assert_eq!(annotate_pause_error(Some(""), &t1), t1);
        assert_eq!(annotate_pause_error(Some(&t1), &t2), t2);
        assert_eq!(
            annotate_pause_error(Some("forge down"), &t1),
            format!("forge down; {t1}")
        );
        assert_eq!(
            annotate_pause_error(Some(&format!("forge down; {t1}")), &t2),
            format!("forge down; {t2}")
        );
    }

    #[test]
    fn pause_duration_saturates_on_a_near_max_reset() {
        // A forge-supplied reset near `u64::MAX` must clamp to the cap,
        // not overflow the margin addition and panic.
        assert_eq!(pause_duration(Some(u64::MAX), 0), RATE_LIMIT_MAX_PAUSE);
        // The margin addition itself must not overflow either: a reset one
        // second out near the top of the range clamps to the minimum.
        assert_eq!(
            pause_duration(Some(u64::MAX), u64::MAX - 1),
            RATE_LIMIT_MIN_PAUSE
        );
    }

    #[test]
    fn gate_opens_after_the_window_and_coalesces_triggers() {
        let gate = RateLimitGate::default();
        assert!(gate.paused_remaining().is_none());
        assert!(gate.paused_until().is_none());

        // First trigger opens the window (caller logs); a second trigger
        // while paused is coalesced (no second WARN).
        let before = SystemTime::now();
        assert!(gate.pause_for(Duration::from_secs(60)));
        assert!(gate.paused_remaining().is_some());
        let until = gate.paused_until().expect("wall-clock deadline");
        assert!(until >= before + Duration::from_secs(60));
        assert!(!gate.pause_for(Duration::from_secs(60)));

        // A later deadline extends silently, on both clocks.
        assert!(!gate.pause_for(Duration::from_secs(120)));
        assert!(gate.paused_remaining().unwrap() > Duration::from_secs(60));
        assert!(gate.paused_until().unwrap() > until);
    }

    #[test]
    fn gate_reopens_once_the_deadline_elapses() {
        let gate = RateLimitGate::default();
        assert!(gate.pause_for(Duration::from_millis(5)));
        std::thread::sleep(Duration::from_millis(10));
        assert!(gate.paused_remaining().is_none());
        assert!(gate.paused_until().is_none());
        // The next trigger is a NEW window and warns again.
        assert!(gate.pause_for(Duration::from_secs(60)));
    }
}
