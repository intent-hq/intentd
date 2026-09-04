//! Retry policy for listener accept loops (intent-hq/intent#4390).
//!
//! When `accept(2)` fails because the process is out of descriptors (EMFILE /
//! ENFILE) or kernel buffers (ENOBUFS / ENOMEM), retrying immediately spins the
//! loop at hundreds of attempts per second and floods the log with one WARN per
//! attempt. [`AcceptBackoff`] classifies each accept error: transient resource
//! exhaustion yields a jittered exponential delay (50 ms doubling to a 500 ms
//! cap, reset by the next successful accept) plus a rate-limited WARN, while any
//! other error keeps today's immediate retry.

use std::io;
use std::time::{Duration, Instant};

/// Delay after the first resource-exhaustion failure of a streak.
const INITIAL_DELAY: Duration = Duration::from_millis(50);
/// Upper bound on the (pre-jitter) delay.
const MAX_DELAY: Duration = Duration::from_millis(500);
/// Minimum spacing between WARN lines while a streak persists.
const WARN_INTERVAL: Duration = Duration::from_secs(5);

/// What the accept loop should do after one failed `accept`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptFailure {
    /// Transient resource exhaustion: sleep `delay` before the next accept.
    /// `warn` is true when this failure should be logged (first of the streak,
    /// then at most once per [`WARN_INTERVAL`]); `streak` counts consecutive
    /// failures including this one.
    Backoff {
        delay: Duration,
        streak: u32,
        warn: bool,
    },
    /// Any other error: retry immediately, as before.
    Other,
}

/// Per-listener backoff state. One instance per accept loop.
#[derive(Debug, Default)]
pub(crate) struct AcceptBackoff {
    streak: u32,
    base: Duration,
    last_warn: Option<Instant>,
}

impl AcceptBackoff {
    /// Classify `err` and advance the streak; see [`AcceptFailure`].
    pub(crate) fn on_error(&mut self, err: &io::Error) -> AcceptFailure {
        self.on_error_at(err, Instant::now())
    }

    /// [`Self::on_error`] with an injectable clock for the WARN rate limit.
    pub(crate) fn on_error_at(&mut self, err: &io::Error, now: Instant) -> AcceptFailure {
        if !is_resource_exhaustion(err) {
            return AcceptFailure::Other;
        }
        self.streak = self.streak.saturating_add(1);
        self.base = if self.streak == 1 {
            INITIAL_DELAY
        } else {
            (self.base * 2).min(MAX_DELAY)
        };
        let warn = match self.last_warn {
            None => true,
            Some(at) => now.duration_since(at) >= WARN_INTERVAL,
        };
        if warn {
            self.last_warn = Some(now);
        }
        AcceptFailure::Backoff {
            delay: jitter(self.base),
            streak: self.streak,
            warn,
        }
    }

    /// Record a successful accept. Returns the length of the streak that just
    /// ended so the caller can log recovery once, or `None` when no streak was
    /// in progress.
    pub(crate) fn on_success(&mut self) -> Option<u32> {
        let ended = (self.streak > 0).then_some(self.streak);
        self.streak = 0;
        self.base = Duration::ZERO;
        self.last_warn = None;
        ended
    }
}

/// "Equal jitter": a uniform draw from `[base / 2, base]`, so concurrent
/// listeners do not retry in lockstep while the delay never exceeds `base`.
fn jitter(base: Duration) -> Duration {
    let half = base / 2;
    let span_ms = half.as_millis();
    if span_ms == 0 {
        return base;
    }
    let Ok(random) = getrandom::u64() else {
        return base;
    };
    let span = u64::try_from(span_ms).unwrap_or(u64::MAX);
    half + Duration::from_millis(random % (span + 1))
}

/// Whether `err` is a transient descriptor/buffer exhaustion error from
/// `accept(2)` that a short wait may clear.
#[cfg(unix)]
fn is_resource_exhaustion(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    )
}

/// Windows equivalents: `WSAEMFILE` (10024) and `WSAENOBUFS` (10055) from
/// Winsock, plus the portable out-of-memory kind.
#[cfg(windows)]
fn is_resource_exhaustion(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(10024 | 10055)) || err.kind() == io::ErrorKind::OutOfMemory
}

#[cfg(not(any(unix, windows)))]
fn is_resource_exhaustion(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::OutOfMemory
}

#[cfg(test)]
mod tests {
    use super::{jitter, AcceptBackoff, AcceptFailure, INITIAL_DELAY, MAX_DELAY, WARN_INTERVAL};
    use std::io;
    use std::time::{Duration, Instant};

    #[cfg(unix)]
    fn emfile() -> io::Error {
        io::Error::from_raw_os_error(libc::EMFILE)
    }

    #[cfg(not(unix))]
    fn emfile() -> io::Error {
        io::Error::from(io::ErrorKind::OutOfMemory)
    }

    fn backoff(failure: AcceptFailure) -> (Duration, u32, bool) {
        match failure {
            AcceptFailure::Backoff {
                delay,
                streak,
                warn,
            } => (delay, streak, warn),
            AcceptFailure::Other => panic!("expected Backoff, got Other"),
        }
    }

    #[test]
    fn resource_error_backs_off_exponentially_up_to_the_cap() {
        let mut b = AcceptBackoff::default();
        let mut expected_base = INITIAL_DELAY;
        for n in 1..=12u32 {
            let (delay, streak, _) = backoff(b.on_error(&emfile()));
            assert_eq!(streak, n);
            assert!(
                delay >= expected_base / 2 && delay <= expected_base,
                "attempt {n}: delay {delay:?} outside [{:?}, {expected_base:?}]",
                expected_base / 2
            );
            assert!(
                delay <= MAX_DELAY,
                "attempt {n}: delay {delay:?} exceeds cap"
            );
            expected_base = (expected_base * 2).min(MAX_DELAY);
        }
        assert_eq!(expected_base, MAX_DELAY);
    }

    #[cfg(unix)]
    #[test]
    fn every_resource_errno_is_classified_as_backoff() {
        for code in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            let mut b = AcceptBackoff::default();
            let err = io::Error::from_raw_os_error(code);
            assert!(
                matches!(b.on_error(&err), AcceptFailure::Backoff { .. }),
                "errno {code} should back off"
            );
        }
    }

    #[test]
    fn jitter_stays_within_half_to_full_base() {
        for base in [INITIAL_DELAY, Duration::from_millis(200), MAX_DELAY] {
            for _ in 0..500 {
                let d = jitter(base);
                assert!(d >= base / 2 && d <= base, "jitter {d:?} for base {base:?}");
            }
        }
        assert_eq!(jitter(Duration::from_millis(1)), Duration::from_millis(1));
    }

    #[test]
    fn unrelated_error_retries_immediately_and_leaves_the_streak_alone() {
        let mut b = AcceptBackoff::default();
        assert_eq!(
            b.on_error(&io::Error::from(io::ErrorKind::ConnectionAborted)),
            AcceptFailure::Other
        );
        #[cfg(unix)]
        assert_eq!(
            b.on_error(&io::Error::from_raw_os_error(libc::EINTR)),
            AcceptFailure::Other
        );
        assert_eq!(b.on_success(), None);
        let (_, streak, _) = backoff(b.on_error(&emfile()));
        assert_eq!(streak, 1);
    }

    #[test]
    fn success_resets_the_streak_and_delay() {
        let mut b = AcceptBackoff::default();
        for _ in 0..4 {
            backoff(b.on_error(&emfile()));
        }
        assert_eq!(b.on_success(), Some(4));
        assert_eq!(b.on_success(), None);
        let (delay, streak, warn) = backoff(b.on_error(&emfile()));
        assert_eq!(streak, 1);
        assert!(warn, "first failure after recovery warns again");
        assert!(delay <= INITIAL_DELAY, "delay {delay:?} did not reset");
    }

    #[test]
    fn warn_fires_on_first_failure_then_at_most_once_per_interval() {
        let mut b = AcceptBackoff::default();
        let t0 = Instant::now();
        let warns: Vec<bool> = [
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_millis(4999),
            WARN_INTERVAL,
            WARN_INTERVAL + Duration::from_secs(1),
            WARN_INTERVAL * 2,
        ]
        .into_iter()
        .map(|offset| backoff(b.on_error_at(&emfile(), t0 + offset)).2)
        .collect();
        assert_eq!(warns, [true, false, false, true, false, true]);
    }
}
