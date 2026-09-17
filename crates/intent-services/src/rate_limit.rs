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

/// Absolute floor on the remaining quota below which a paused gate is not
/// lifted early ([`quota_recovered`]): enough headroom for a full sweep
/// tick of every forge-touching sweep, so an early lift cannot trip the
/// limit again within seconds.
pub(crate) const RATE_LIMIT_LIFT_MIN_REMAINING: u64 = 500;

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

/// The remaining quota at or above which a paused gate lifts early:
/// `max(RATE_LIMIT_LIFT_MIN_REMAINING, 10% of limit)` — the absolute floor
/// for hosts without a reported limit (or a tiny one), a tenth of the
/// window otherwise.
pub(crate) fn lift_floor(limit: Option<u64>) -> u64 {
    RATE_LIMIT_LIFT_MIN_REMAINING.max(limit.unwrap_or(0) / 10)
}

/// Whether the forge's quota-free probe reports the quota recovered enough
/// to lift the pause before its deadline: a REPORTED `remaining` at or
/// above [`lift_floor`]. A host without the signal (`remaining: None`)
/// never lifts early — the deadline stands, as before the probe existed.
pub(crate) fn quota_recovered(remaining: Option<u64>, limit: Option<u64>) -> bool {
    remaining.is_some_and(|remaining| remaining >= lift_floor(limit))
}

/// The fixed prefix of the pause annotation carried in a PR monitor's
/// `lastError` while the gate is closed — the marker by which an earlier
/// annotation is found and replaced (a deadline extension, a re-stamp) or
/// cut off (a lift). The store owns the shape and every write of it: the
/// bulk stamp and clear are the only statements that put a deadline on a
/// row, and its guarded write-backs only keep or drop the row's own
/// (`intent_store::PrMonitorPollUpdate::last_error`).
pub(crate) use intent_store::{
    pr_monitor_pause_error as pause_error, PR_MONITOR_PAUSE_MARKER as PAUSE_ERROR_MARKER,
};

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
/// `Arc` across [`crate::Services`] clones; the `paused_until` mutex is
/// only ever held for a read/compare/store, never across an await.
///
/// `reconcile` serializes a gate TRANSITION with the persisted-annotation
/// statement that reconciles the PR monitor rows to it — a pause opening
/// or extending with its bulk stamp, a lift or a boot check with its bulk
/// clear ([`crate::Services::pause_sweeps_for_rate_limit`],
/// [`crate::Services::maybe_lift_rate_limit_pause`],
/// [`crate::Services::rehydrate_pr_monitors`]). Without it the two are
/// separate operations, and `SQLite`'s single writer only orders the
/// statements: a lift's clear delayed past a fresh trigger's stamp erased
/// the new pause while the gate held it (intent-hq/intentd#1945). Held
/// across the statement's await, so it is the async mutex; never taken by
/// the per-row write-backs, which the store composes against the row.
#[derive(Default)]
pub(crate) struct RateLimitGate {
    paused_until: std::sync::Mutex<Option<PauseDeadline>>,
    reconcile: tokio::sync::Mutex<()>,
}

impl RateLimitGate {
    /// Hold the gate's transition-and-reconciliation critical section: a
    /// transition ([`Self::pause_for`], [`Self::lift`], or a read that
    /// decides a clear) and the statement reconciling the rows to it run
    /// under this guard, so no other transition can slip between them.
    /// Never held across a forge call — probe first, then lock.
    pub(crate) async fn reconcile(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.reconcile.lock().await
    }

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

    /// Re-open the gate now, before its deadline — the forge's quota-free
    /// probe reported the quota recovered ([`quota_recovered`]). Returns
    /// `true` when a pause was active and is now lifted (the caller logs
    /// its one INFO and reconciles the persisted pause annotations);
    /// `false` when the gate was already open — never paused, or the
    /// window elapsed on its own — so a lift racing the deadline reports
    /// nothing. The next trigger opens a NEW window (and warns again).
    pub(crate) fn lift(&self) -> bool {
        let mut slot = self.paused_until.lock().expect("gate lock");
        let was_paused = slot.is_some_and(|deadline| deadline.at > Instant::now());
        *slot = None;
        was_paused
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

    /// The pause annotation names the deadline behind the fixed marker, and
    /// is the bare marker without one.
    #[test]
    fn pause_annotation_names_the_deadline_behind_the_marker() {
        assert_eq!(
            pause_error(Some("2026-09-17T02:39:15Z")),
            "rate limited; PR monitor polling paused until 2026-09-17T02:39:15Z"
        );
        assert_eq!(pause_error(None), PAUSE_ERROR_MARKER);
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

    /// An explicit lift re-opens a paused gate before its deadline and
    /// reports that it did; lifting an open gate (never paused, or already
    /// lifted / elapsed) reports nothing; the next trigger after a lift is
    /// a NEW window that warns again.
    #[test]
    fn lift_reopens_a_paused_gate_early_and_reports_only_when_it_was_paused() {
        let gate = RateLimitGate::default();
        assert!(!gate.lift(), "an open gate has nothing to lift");

        assert!(gate.pause_for(Duration::from_secs(3600)));
        assert!(gate.paused_remaining().is_some());
        assert!(gate.lift(), "the active pause is lifted");
        assert!(gate.paused_remaining().is_none());
        assert!(gate.paused_until().is_none());
        assert!(!gate.lift(), "a second lift finds the gate open");

        assert!(
            gate.pause_for(Duration::from_secs(60)),
            "the next trigger after a lift opens a fresh window (and warns)"
        );

        // A lift racing the deadline: the window already elapsed on its
        // own, so there was nothing to lift — no INFO for the caller.
        let gate = RateLimitGate::default();
        assert!(gate.pause_for(Duration::from_millis(5)));
        std::thread::sleep(Duration::from_millis(10));
        assert!(!gate.lift());
    }

    /// The early-lift floor is `max(500, 10% of limit)` on a REPORTED
    /// `remaining`: a host without the signal never lifts early.
    #[test]
    fn quota_recovered_honors_the_lift_floor() {
        assert_eq!(lift_floor(None), RATE_LIMIT_LIFT_MIN_REMAINING);
        assert_eq!(lift_floor(Some(1_000)), RATE_LIMIT_LIFT_MIN_REMAINING);
        assert_eq!(lift_floor(Some(5_000)), 500);
        assert_eq!(lift_floor(Some(15_000)), 1_500);

        // The evidence case (monorepo#2961): a full 5000/5000 window while
        // the daemon still sat on its deadline.
        assert!(quota_recovered(Some(5_000), Some(5_000)));
        assert!(quota_recovered(Some(500), Some(5_000)));
        assert!(!quota_recovered(Some(499), Some(5_000)));
        // A large window raises the floor to a tenth of it.
        assert!(!quota_recovered(Some(1_000), Some(15_000)));
        assert!(quota_recovered(Some(1_500), Some(15_000)));
        // Without a limit the absolute floor applies.
        assert!(quota_recovered(Some(500), None));
        assert!(!quota_recovered(Some(0), None));
        // No signal: the deadline stands.
        assert!(!quota_recovered(None, Some(5_000)));
        assert!(!quota_recovered(None, None));
    }
}
