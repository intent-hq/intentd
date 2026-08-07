//! Host suspend/resume detection via clock skew.
//!
//! No OS integration is required: a ~1s sampler (modeled on
//! `spawn_proc_usage_sampler`) compares a monotonic clock ([`Instant`]) against
//! the wall clock ([`SystemTime`]) between ticks. While the host is awake the
//! two advance together; across a suspend the wall clock jumps forward far more
//! than the monotonic clock (which is paused or barely advances during sleep),
//! so `wall_delta - mono_delta` is the suspend duration.
//!
//! When that skew exceeds a configured threshold the detector records a suspend
//! interval on the shared [`SuspendTracker`] and broadcasts a [`ResumeEvent`].
//! Other components (resume/enrollment gates) query the tracker for whether a
//! suspend overlapped some monotonic window, or subscribe to resume events.
//!
//! The query surface is consumed by the service layer via the
//! [`intent_services::SuspendOverlapQuery`] impl below (Task C enrollment) and
//! the subscribe surface by the wake-triggered resume orchestrator in `main`
//! (Task D). A couple of the richer [`ResumeEvent`] fields are exercised only
//! by the unit tests, so a narrow `dead_code` allow rides on that type.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::broadcast;

/// Cap on retained suspend intervals (oldest evicted first).
const MAX_SUSPEND_INTERVALS: usize = 64;

/// Capacity of the resume-event broadcast channel. Resume events are rare
/// (one per host wake), so a small buffer is ample; lagging subscribers drop
/// the oldest events rather than blocking the detector.
const RESUME_CHANNEL_CAPACITY: usize = 16;

/// Source of the two clocks the detector samples. Injectable so tests can drive
/// arbitrary skew without actually suspending the host.
pub trait Clock: Send + Sync + 'static {
    /// Monotonic, non-decreasing clock. Paused (or nearly so) across suspend.
    fn now_mono(&self) -> Instant;
    /// Wall clock. Continues advancing across suspend.
    fn now_wall(&self) -> SystemTime;
}

/// Production clock backed by the OS.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_mono(&self) -> Instant {
        Instant::now()
    }
    fn now_wall(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl<T: Clock> Clock for Arc<T> {
    fn now_mono(&self) -> Instant {
        (**self).now_mono()
    }
    fn now_wall(&self) -> SystemTime {
        (**self).now_wall()
    }
}

/// Emitted once per detected resume (host wake following a suspend).
///
/// `before`/`after` bracket the gap in monotonic terms; the resume
/// orchestrator only reads `suspended_for`, so those two fields are currently
/// exercised only by the module tests (hence the field-level `dead_code` allow).
#[derive(Debug, Clone)]
pub struct ResumeEvent {
    /// Detected suspend duration (wall skew) preceding this resume.
    pub suspended_for: Duration,
    /// Monotonic instant of the last sample before the gap.
    #[allow(dead_code)]
    pub before: Instant,
    /// Monotonic instant of the first sample after the gap (the resume tick).
    #[allow(dead_code)]
    pub after: Instant,
}

/// A single detected suspend, expressed in monotonic terms.
///
/// `[mono_start, mono_end]` bracket the tick that observed the gap (typically
/// only ~1s wide, since the monotonic clock is paused during sleep), while
/// `suspend` is the wall-clock duration the host was actually asleep. Overlap
/// queries use the monotonic bracket to decide whether a caller's window
/// spanned the suspend, and report `suspend` as the amount of lost time.
#[derive(Debug, Clone)]
struct SuspendInterval {
    mono_start: Instant,
    mono_end: Instant,
    suspend: Duration,
}

struct Inner {
    intervals: VecDeque<SuspendInterval>,
}

/// Shared record of recent suspends plus a resume-event stream.
pub struct SuspendTracker {
    inner: Mutex<Inner>,
    resume_tx: broadcast::Sender<ResumeEvent>,
    max_intervals: usize,
}

impl SuspendTracker {
    /// Construct an empty tracker with default history/channel bounds.
    pub fn new() -> Arc<Self> {
        let (resume_tx, _) = broadcast::channel(RESUME_CHANNEL_CAPACITY);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                intervals: VecDeque::new(),
            }),
            resume_tx,
            max_intervals: MAX_SUSPEND_INTERVALS,
        })
    }

    /// Subscribe to resume events. Late subscribers only see events emitted
    /// after they subscribe.
    pub fn subscribe(&self) -> broadcast::Receiver<ResumeEvent> {
        self.resume_tx.subscribe()
    }

    /// Record a detected suspend and broadcast the corresponding resume event.
    /// Bounds retained history to `max_intervals`, evicting oldest first.
    pub fn record_suspend(&self, mono_start: Instant, mono_end: Instant, suspend: Duration) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.intervals.push_back(SuspendInterval {
                mono_start,
                mono_end,
                suspend,
            });
            while inner.intervals.len() > self.max_intervals {
                inner.intervals.pop_front();
            }
        }
        tracing::info!(
            suspended_for_secs = suspend.as_secs(),
            "host resume detected (clock skew)"
        );
        // A send error only means there are no live subscribers yet; the
        // interval is still recorded for later `did_suspend_overlap` queries.
        let _ = self.resume_tx.send(ResumeEvent {
            suspended_for: suspend,
            before: mono_start,
            after: mono_end,
        });
    }

    /// Total suspend duration whose monotonic bracket overlaps the window
    /// `[start, end]`. Returns `None` if no retained suspend overlaps it.
    pub fn did_suspend_overlap(&self, start: Instant, end: Instant) -> Option<Duration> {
        let inner = self.inner.lock().unwrap();
        let mut total = Duration::ZERO;
        let mut overlapped = false;
        for iv in &inner.intervals {
            if start <= iv.mono_end && iv.mono_start <= end {
                total += iv.suspend;
                overlapped = true;
            }
        }
        overlapped.then_some(total)
    }
}

/// Bridge the daemon's clock-skew tracker to the service layer's overlap query
/// (Task C): the service crate defines the trait but keeps the concrete
/// detector here in the binary crate, so [`Services::with_suspend_tracker`]
/// receives the tracker as `Arc<dyn SuspendOverlapQuery>`. Delegates to the
/// inherent method of the same name.
impl intent_services::SuspendOverlapQuery for SuspendTracker {
    fn did_suspend_overlap(&self, start: Instant, end: Instant) -> Option<Duration> {
        SuspendTracker::did_suspend_overlap(self, start, end)
    }
}

/// TEST-ONLY SEAM (§13.1 E2E): a [`SuspendOverlapQuery`] that reports a fixed
/// overlap for ANY queried window. Wired in place of the real tracker's overlap
/// query when `INTENTD_TEST_FORCE_SUSPEND_OVERLAP_SECS` is set, so the WSS e2e
/// can drive a suspend-shaped turn interruption deterministically (a transient
/// upstream disconnect is always classified as suspend-overlapping and enrolled
/// for wake-resume) without a real host sleep. The real clock-skew
/// detector/tracker still runs for the wake orchestrator; this only forces the
/// Task C enrollment classification. Because no real suspend is recorded, the
/// wake broadcast never fires — so the e2e exercises the enrollment-driven
/// self-heal resume path end to end.
pub struct ForcedSuspendOverlap(Duration);

impl ForcedSuspendOverlap {
    pub fn new(overlap: Duration) -> Self {
        Self(overlap)
    }
}

impl intent_services::SuspendOverlapQuery for ForcedSuspendOverlap {
    fn did_suspend_overlap(&self, _start: Instant, _end: Instant) -> Option<Duration> {
        Some(self.0)
    }
}

/// Sampling detector: on each tick it compares monotonic and wall deltas since
/// the previous tick and records a suspend when the skew crosses `threshold`.
struct SuspendDetector<C: Clock> {
    clock: C,
    tracker: Arc<SuspendTracker>,
    threshold: Duration,
    last_mono: Instant,
    last_wall: SystemTime,
}

impl<C: Clock> SuspendDetector<C> {
    fn new(clock: C, tracker: Arc<SuspendTracker>, threshold: Duration) -> Self {
        let last_mono = clock.now_mono();
        let last_wall = clock.now_wall();
        Self {
            clock,
            tracker,
            threshold,
            last_mono,
            last_wall,
        }
    }

    /// Sample both clocks and record a suspend if the wall clock outran the
    /// monotonic clock by at least `threshold`.
    fn tick(&mut self) {
        let now_mono = self.clock.now_mono();
        let now_wall = self.clock.now_wall();
        let mono_delta = now_mono.saturating_duration_since(self.last_mono);
        // A backwards wall step (NTP correction) yields a zero delta, so skew
        // stays zero and is ignored.
        let wall_delta = now_wall
            .duration_since(self.last_wall)
            .unwrap_or(Duration::ZERO);
        let skew = wall_delta.saturating_sub(mono_delta);
        if skew >= self.threshold {
            self.tracker.record_suspend(self.last_mono, now_mono, skew);
        }
        self.last_mono = now_mono;
        self.last_wall = now_wall;
    }
}

/// Spawn the ~1s suspend/resume detector task and return the shared tracker.
///
/// Modeled on `spawn_proc_usage_sampler`: the first tick fires one period out,
/// and `MissedTickBehavior::Delay` keeps ticks spaced (never bursting) after a
/// suspend delays the timer.
pub fn spawn_suspend_detector(threshold: Duration) -> Arc<SuspendTracker> {
    let tracker = SuspendTracker::new();
    // Defensive floor (mirrors `Config`'s clamp): the ~1s sampler flags a
    // suspend on `skew >= threshold`, and skew is non-negative, so a zero
    // threshold would classify every tick as a suspend. Never let the detector
    // run below a 1s divergence regardless of the caller-supplied duration.
    let threshold = threshold.max(Duration::from_secs(1));
    let mut detector = SuspendDetector::new(SystemClock, tracker.clone(), threshold);
    tracing::info!(
        threshold_secs = threshold.as_secs(),
        "suspend/wake detector started"
    );
    tokio::spawn(async move {
        let period = Duration::from_secs(1);
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            detector.tick();
        }
    });
    tracker
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test clock whose monotonic and wall components advance independently.
    struct MockClock {
        mono: Mutex<Instant>,
        wall: Mutex<SystemTime>,
    }

    impl MockClock {
        fn new() -> Self {
            Self {
                mono: Mutex::new(Instant::now()),
                wall: Mutex::new(SystemTime::now()),
            }
        }
        fn advance(&self, mono: Duration, wall: Duration) {
            *self.mono.lock().unwrap() += mono;
            *self.wall.lock().unwrap() += wall;
        }
    }

    impl Clock for MockClock {
        fn now_mono(&self) -> Instant {
            *self.mono.lock().unwrap()
        }
        fn now_wall(&self) -> SystemTime {
            *self.wall.lock().unwrap()
        }
    }

    fn detector(clock: Arc<MockClock>) -> (SuspendDetector<Arc<MockClock>>, Arc<SuspendTracker>) {
        let tracker = SuspendTracker::new();
        let det = SuspendDetector::new(clock, tracker.clone(), Duration::from_secs(10));
        (det, tracker)
    }

    #[test]
    fn clock_jump_records_suspend_and_resume() {
        let clock = Arc::new(MockClock::new());
        let (mut det, tracker) = detector(clock.clone());
        let mut rx = tracker.subscribe();

        // Wall jumps 120s while the monotonic clock advances 1s: a suspend.
        clock.advance(Duration::from_secs(1), Duration::from_secs(120));
        det.tick();

        let ev = rx.try_recv().expect("resume event emitted");
        // ~119s of lost wall time (allow a small window for rounding).
        assert!(
            (ev.suspended_for.as_secs_f64() - 119.0).abs() < 1.0,
            "suspended_for = {:?}",
            ev.suspended_for
        );
        assert_eq!(
            tracker
                .did_suspend_overlap(ev.before, ev.after)
                .map(|d| d.as_secs()),
            Some(119)
        );
    }

    #[test]
    fn normal_tick_produces_nothing() {
        let clock = Arc::new(MockClock::new());
        let (mut det, tracker) = detector(clock.clone());
        let mut rx = tracker.subscribe();

        clock.advance(Duration::from_secs(1), Duration::from_secs(1));
        det.tick();

        assert!(rx.try_recv().is_err(), "no resume event on a normal tick");
        let before = det.last_mono;
        assert_eq!(tracker.did_suspend_overlap(before, before), None);
    }

    #[test]
    fn sub_threshold_jump_is_ignored() {
        let clock = Arc::new(MockClock::new());
        let (mut det, tracker) = detector(clock.clone());
        let mut rx = tracker.subscribe();

        // 5s skew is below the 10s threshold.
        clock.advance(Duration::from_secs(1), Duration::from_secs(6));
        det.tick();

        assert!(rx.try_recv().is_err(), "sub-threshold jump emits no event");
    }

    #[test]
    fn did_suspend_overlap_windows() {
        let tracker = SuspendTracker::new();
        let base = Instant::now();
        let mono_start = base + Duration::from_secs(10);
        let mono_end = base + Duration::from_secs(11);
        tracker.record_suspend(mono_start, mono_end, Duration::from_secs(119));

        // Overlapping window straddles the suspend bracket.
        assert_eq!(
            tracker
                .did_suspend_overlap(
                    base + Duration::from_secs(5),
                    base + Duration::from_secs(20)
                )
                .map(|d| d.as_secs()),
            Some(119)
        );
        // Non-overlapping window sits entirely before the bracket.
        assert_eq!(
            tracker.did_suspend_overlap(base, base + Duration::from_secs(5)),
            None
        );
    }
}
