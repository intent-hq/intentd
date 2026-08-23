//! Global forge rate-limit pause gate for the background sweeps
//! (monorepo#2961).
//!
//! When the PR-refresh sweep or the git-root sweep hits a forge rate limit
//! (REST 403/429 with an exhausted quota, surfaced as
//! [`intent_core::Error::RateLimited`]), continuing to call the forge for
//! every remaining root/workspace on every tick both spams WARN logs (one
//! per root per tick, masking real failures) and burns the freshly-reset
//! quota window. This gate is shared by every [`crate::Services`] clone: the
//! first rate-limited call pauses ALL forge-touching sweep work until the
//! quota window resets, and reporting coalesces to one WARN per pause window
//! (the trigger site logs exactly when [`RateLimitGate::pause_for`] reports a
//! fresh pause). Sweep-local work (submodule auto-detect, prune, commit-sha
//! backfill) never pauses — only forge calls do.

use std::time::{Duration, Instant};

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
        Some(reset) => {
            Duration::from_secs(reset.saturating_sub(now_unix)) + RATE_LIMIT_RESET_MARGIN
        }
        None => RATE_LIMIT_FALLBACK_PAUSE,
    };
    base.clamp(RATE_LIMIT_MIN_PAUSE, RATE_LIMIT_MAX_PAUSE)
}

/// The shared pause state. Interior-mutable so one instance can sit in an
/// `Arc` across [`crate::Services`] clones; the mutex is only ever held for
/// a read/compare/store, never across an await.
#[derive(Default)]
pub(crate) struct RateLimitGate {
    paused_until: std::sync::Mutex<Option<Instant>>,
}

impl RateLimitGate {
    /// Remaining pause, or `None` when the gate is open (never paused, or
    /// the window elapsed — the gate re-opens implicitly, no reset call).
    pub(crate) fn paused_remaining(&self) -> Option<Duration> {
        let deadline = (*self.paused_until.lock().expect("gate lock"))?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        (remaining > Duration::ZERO).then_some(remaining)
    }

    /// Pause forge-touching sweep work for `duration` from now. Returns
    /// `true` when this call opened a NEW pause window (the caller should
    /// log its one WARN); `false` when a pause was already active — the
    /// deadline is extended if the new one is later, but reporting stays
    /// coalesced to the window's first trigger.
    pub(crate) fn pause_for(&self, duration: Duration) -> bool {
        let deadline = Instant::now() + duration;
        let mut slot = self.paused_until.lock().expect("gate lock");
        match *slot {
            Some(existing) if existing > Instant::now() => {
                if deadline > existing {
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

    #[test]
    fn gate_opens_after_the_window_and_coalesces_triggers() {
        let gate = RateLimitGate::default();
        assert!(gate.paused_remaining().is_none());

        // First trigger opens the window (caller logs); a second trigger
        // while paused is coalesced (no second WARN).
        assert!(gate.pause_for(Duration::from_secs(60)));
        assert!(gate.paused_remaining().is_some());
        assert!(!gate.pause_for(Duration::from_secs(60)));

        // A later deadline extends silently.
        assert!(!gate.pause_for(Duration::from_secs(120)));
        assert!(gate.paused_remaining().unwrap() > Duration::from_secs(60));
    }

    #[test]
    fn gate_reopens_once_the_deadline_elapses() {
        let gate = RateLimitGate::default();
        assert!(gate.pause_for(Duration::from_millis(5)));
        std::thread::sleep(Duration::from_millis(10));
        assert!(gate.paused_remaining().is_none());
        // The next trigger is a NEW window and warns again.
        assert!(gate.pause_for(Duration::from_secs(60)));
    }
}
