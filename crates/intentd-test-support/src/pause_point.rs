//! [`PausePoint`]: the one way a test parks an operation part-way through.
//!
//! A test that needs an operation held at a precise spot (a secret write in
//! flight, a lease mid-renewal) used to hand-roll a `Gate` / `Pause` / `Park`
//! struct: an "entered" signal the operation sends and a "release" it then
//! waits on. Every copy waited on both with no time limit. On 2026-11-27 a
//! new orchestrator test (`lifecycle_gates`, run as plain `cargo test`, so
//! nextest's slow-timeout never applied) awaited `park.entered` for an
//! operation that had already failed before reaching it, and hung for about
//! six hours.
//!
//! Every wait here is bounded by [`test_timeout`](crate::test_timeout) on the
//! wall clock (so a paused tokio clock neither skips nor stalls it), and
//! [`PausePoint::reached`] watches the operation itself: when the operation
//! finishes before it reaches the pause point, the test fails at once with
//! the operation's result (its error) instead of waiting. `pause_point_lint`
//! steers new test code here.
//!
//! ```ignore
//! let pause = PausePoint::new("secret store");
//! let gated = GatedSecrets::new(pause.clone()); // calls `pause.pause().await`
//! let mut op = tokio::spawn(async move { svc.create(...).await });
//! pause.reached(&mut op).await; // panics with op's error if it ended first
//! // ... assertions while the operation is held ...
//! pause.release();
//! op.await.unwrap().unwrap();
//! ```

use std::fmt::Debug;
use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, Notify};

/// Base budget for the test to see the operation reach the pause point,
/// stretched by [`test_timeout`](crate::test_timeout). The `make` test
/// targets and coverage CI stretch it 3x (60 s), still under nextest's
/// kill (90 s slow-timeout, terminated after 2 periods: 180 s), so the
/// helper's message, not a kill, explains a hang.
pub const DEFAULT_REACH_TIMEOUT: Duration = Duration::from_secs(20);

/// Base budget an operation stays held before it gives up (panics),
/// stretched by [`test_timeout`](crate::test_timeout). Legitimate holds
/// last seconds; this only frees an operation whose test forgot to release
/// it or died. 3x stretched (120 s) it still fires before nextest's 180 s
/// kill.
pub const DEFAULT_HOLD_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Debug, Default)]
struct State {
    /// Times the operation side has arrived ([`PausePoint::pause`]).
    arrivals: u64,
    /// Arrivals the test side has already observed ([`PausePoint::reached`]).
    observed: u64,
    /// Releases granted and not yet used by a held operation.
    permits: u64,
}

/// A pause point an operation under test stops at until the test releases
/// it. Cheap to share: hand an `Arc<PausePoint>` to the fake that the
/// operation calls, keep one in the test.
///
/// - Operation side: [`PausePoint::pause`] (async) or
///   [`PausePoint::pause_blocking`] (sync code such as a `SecretStore`)
///   records the arrival and blocks until a [`PausePoint::release`]; after
///   the hold timeout it panics instead of blocking forever.
/// - Test side: [`PausePoint::reached`] waits for the next arrival while
///   watching the operation (fails with its result if it ends first);
///   [`PausePoint::wait_reached`] / [`PausePoint::wait_reached_blocking`]
///   wait without one. Both fail after the reach timeout.
///
/// Arrivals and releases are counted, so one pause point can hold an
/// operation several times; each [`PausePoint::reached`] observes one
/// arrival and each [`PausePoint::release`] lets one held caller through
/// (a release given before the arrival is kept for it).
#[derive(Debug)]
pub struct PausePoint {
    name: String,
    state: Mutex<State>,
    changed: Condvar,
    notify: Notify,
    reach_timeout: Duration,
    hold_timeout: Duration,
}

impl PausePoint {
    /// A pause point named `name` (it appears in every failure message),
    /// with [`DEFAULT_REACH_TIMEOUT`] and [`DEFAULT_HOLD_TIMEOUT`] stretched
    /// by [`test_timeout`](crate::test_timeout).
    #[must_use]
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::unshared(name))
    }

    /// [`PausePoint::new`] without the `Arc`, for a caller that embeds it
    /// or builds it with [`PausePoint::with_reach_timeout`] first.
    #[must_use]
    pub fn unshared(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            notify: Notify::new(),
            reach_timeout: crate::test_timeout(DEFAULT_REACH_TIMEOUT),
            hold_timeout: crate::test_timeout(DEFAULT_HOLD_TIMEOUT),
        }
    }

    /// Replace the reach budget with `base` stretched by
    /// [`test_timeout`](crate::test_timeout). It can be shortened, never
    /// removed.
    #[must_use]
    pub fn with_reach_timeout(mut self, base: Duration) -> Self {
        self.reach_timeout = crate::test_timeout(base);
        self
    }

    /// Replace the hold budget with `base` stretched by
    /// [`test_timeout`](crate::test_timeout).
    #[must_use]
    pub fn with_hold_timeout(mut self, base: Duration) -> Self {
        self.hold_timeout = crate::test_timeout(base);
        self
    }

    /// The name given at construction.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// How many times an operation has arrived so far.
    #[must_use]
    pub fn arrivals(&self) -> u64 {
        self.lock().arrivals
    }

    /// Let one held (or the next arriving) operation through.
    pub fn release(&self) {
        self.lock().permits += 1;
        self.wake();
    }

    /// Operation side, async: record the arrival, then wait for a
    /// [`PausePoint::release`].
    ///
    /// # Panics
    ///
    /// When no release comes within the hold timeout, so a forgotten
    /// release fails the operation instead of parking it forever.
    pub async fn pause(&self) {
        self.arrive();
        if self.take_permit() {
            return;
        }
        let deadline = WallDeadline::after(self.hold_timeout);
        let mut deadline = pin!(deadline);
        loop {
            let notified = self.notify.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            if self.take_permit() {
                return;
            }
            tokio::select! {
                biased;
                () = &mut notified => {}
                () = &mut deadline => {
                    if self.take_permit() {
                        return;
                    }
                    panic!("{}", self.hold_expired());
                }
            }
        }
    }

    /// Operation side, blocking: [`PausePoint::pause`] for synchronous code.
    /// Do not call it on an async runtime thread the test also needs.
    ///
    /// # Panics
    ///
    /// When no release comes within the hold timeout.
    #[track_caller]
    pub fn pause_blocking(&self) {
        self.arrive();
        let guard = self.lock();
        let (mut guard, _) = self
            .changed
            .wait_timeout_while(guard, self.hold_timeout, |s| s.permits == 0)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.permits == 0 {
            drop(guard);
            panic!("{}", self.hold_expired());
        }
        guard.permits -= 1;
    }

    /// Test side: wait until the operation reaches the pause point, while
    /// polling `op` (a `JoinHandle`, or any pinned future driving the
    /// operation). Observes one arrival.
    ///
    /// Returns `None` while `op` is still running (held at the pause point,
    /// the usual case), or `Some(output)` when `op` arrived and then already
    /// finished (a release was waiting): the output is handed back so the
    /// test can assert it, and `op` must not be polled again (a completed
    /// `JoinHandle` panics when polled).
    ///
    /// # Panics
    ///
    /// - When `op` finishes first: the message carries its output, so an
    ///   operation that failed early reports its error at once.
    /// - When neither happens within the reach timeout.
    pub async fn reached<F>(&self, op: &mut F) -> Option<F::Output>
    where
        F: Future + Unpin,
        F::Output: Debug,
    {
        if self.observe() {
            return None;
        }
        let deadline = WallDeadline::after(self.reach_timeout);
        let mut deadline = pin!(deadline);
        loop {
            let notified = self.notify.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            if self.observe() {
                return None;
            }
            tokio::select! {
                biased;
                () = &mut notified => {}
                out = &mut *op => {
                    if self.observe() {
                        // Arrived and then finished (a release was already
                        // waiting): the pause point was reached.
                        return Some(out);
                    }
                    panic!(
                        "pause point `{}`: the operation finished before reaching it: {out:?}",
                        self.name
                    );
                }
                () = &mut deadline => {
                    if self.observe() {
                        return None;
                    }
                    panic!("{}", self.reach_expired());
                }
            }
        }
    }

    /// Test side: [`PausePoint::reached`] without an operation to watch.
    /// Prefer `reached`: only it reports an operation that failed early.
    ///
    /// # Panics
    ///
    /// When no arrival comes within the reach timeout.
    pub async fn wait_reached(&self) {
        let _ = self.reached(&mut std::future::pending::<()>()).await;
    }

    /// Test side, blocking: [`PausePoint::wait_reached`] for a test that is
    /// not async.
    ///
    /// # Panics
    ///
    /// When no arrival comes within the reach timeout.
    #[track_caller]
    pub fn wait_reached_blocking(&self) {
        let guard = self.lock();
        let (mut guard, _) = self
            .changed
            .wait_timeout_while(guard, self.reach_timeout, |s| s.arrivals <= s.observed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.arrivals <= guard.observed {
            drop(guard);
            panic!("{}", self.reach_expired());
        }
        guard.observed += 1;
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn wake(&self) {
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    fn arrive(&self) {
        self.lock().arrivals += 1;
        self.wake();
    }

    fn take_permit(&self) -> bool {
        let mut state = self.lock();
        if state.permits == 0 {
            return false;
        }
        state.permits -= 1;
        true
    }

    fn observe(&self) -> bool {
        let mut state = self.lock();
        if state.arrivals <= state.observed {
            return false;
        }
        state.observed += 1;
        true
    }

    fn reach_expired(&self) -> String {
        format!(
            "pause point `{}`: the operation did not reach it within {:?} \
             (it is still running, or never calls `pause`)",
            self.name, self.reach_timeout
        )
    }

    fn hold_expired(&self) -> String {
        format!(
            "pause point `{}`: held for {:?} without a `release()`; \
             the test forgot to release it or is gone",
            self.name, self.hold_timeout
        )
    }
}

/// A future that completes once `duration` of wall-clock time has passed,
/// whatever the tokio clock does: under `start_paused` a tokio timer would
/// auto-advance (and fire at once) while the runtime waits on a blocking
/// thread. The timer thread exits as soon as the future is dropped.
struct WallDeadline {
    fired: oneshot::Receiver<()>,
    cancel: Arc<(Mutex<bool>, Condvar)>,
}

impl WallDeadline {
    fn after(duration: Duration) -> Self {
        let (tx, fired) = oneshot::channel();
        let cancel = Arc::new((Mutex::new(false), Condvar::new()));
        let thread_cancel = cancel.clone();
        let start = Instant::now();
        std::thread::Builder::new()
            .name("pause-point-deadline".into())
            .spawn(move || {
                let (lock, cvar) = &*thread_cancel;
                let guard = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let remaining = duration.saturating_sub(start.elapsed());
                let (cancelled, _) = cvar
                    .wait_timeout_while(guard, remaining, |cancelled| !*cancelled)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !*cancelled {
                    let _ = tx.send(());
                }
            })
            .expect("spawn pause-point deadline thread");
        Self { fired, cancel }
    }
}

impl Future for WallDeadline {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // A dropped sender (the thread died) also ends the wait: never
        // turn a failed timer into an unbounded one.
        Pin::new(&mut self.fired).poll(cx).map(|_| ())
    }
}

impl Drop for WallDeadline {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.cancel;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        cvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHORT: Duration = Duration::from_millis(300);

    fn panic_text(err: tokio::task::JoinError) -> String {
        let payload = err.into_panic();
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn reached_then_release_lets_the_operation_finish() {
        let pause = PausePoint::new("happy");
        let held = pause.clone();
        let mut op = tokio::spawn(async move {
            held.pause().await;
            "done"
        });
        pause.reached(&mut op).await;
        assert_eq!(pause.arrivals(), 1);
        assert!(!op.is_finished());
        pause.release();
        assert_eq!(op.await.unwrap(), "done");
    }

    /// The 2026-11-27 hang: the operation fails before its pause point.
    #[tokio::test]
    async fn an_operation_that_fails_before_the_pause_point_reports_its_error() {
        let pause = PausePoint::new("lifecycle park");
        let held = pause.clone();
        let mut op = tokio::spawn(async move {
            let failed: Result<(), String> = Err("lease fence rejected".into());
            failed?;
            held.pause().await;
            Ok::<_, String>(())
        });
        let test_side = pause.clone();
        let err = tokio::spawn(async move { test_side.reached(&mut op).await })
            .await
            .expect_err("reached must fail when the operation ends first");
        let text = panic_text(err);
        assert!(
            text.contains("finished before reaching it") && text.contains("lease fence rejected"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_never_arriving_operation_fails_after_the_reach_timeout() {
        let pause = Arc::new(PausePoint::unshared("never").with_reach_timeout(SHORT));
        let test_side = pause.clone();
        let err = tokio::spawn(async move { test_side.wait_reached().await })
            .await
            .expect_err("wait_reached must give up");
        assert!(panic_text(err).contains("did not reach it"));
    }

    /// Plain `cargo test`, no runtime, no nextest: still bounded.
    #[test]
    fn a_blocking_wait_is_bounded_without_a_runtime() {
        let pause = PausePoint::unshared("blocking").with_reach_timeout(SHORT);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pause.wait_reached_blocking();
        }));
        assert!(caught.is_err());
    }

    #[test]
    fn a_blocking_pause_without_release_gives_up() {
        let pause = PausePoint::unshared("unreleased").with_hold_timeout(SHORT);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pause.pause_blocking();
        }));
        assert!(caught.is_err());
        assert_eq!(pause.arrivals(), 1);
    }

    #[tokio::test]
    async fn an_async_pause_without_release_gives_up() {
        let pause = Arc::new(PausePoint::unshared("unreleased").with_hold_timeout(SHORT));
        let held = pause.clone();
        let err = tokio::spawn(async move { held.pause().await })
            .await
            .expect_err("pause must give up");
        assert!(panic_text(err).contains("without a `release()`"));
    }

    #[tokio::test]
    async fn blocking_operation_side_and_async_test_side_meet() {
        let pause = PausePoint::new("secret store");
        let held = pause.clone();
        let mut op = tokio::task::spawn_blocking(move || {
            held.pause_blocking();
            7
        });
        pause.reached(&mut op).await;
        pause.release();
        assert_eq!(op.await.unwrap(), 7);
    }

    /// A paused tokio clock auto-advances as soon as the runtime is idle,
    /// so a tokio timer would expire the reach budget at once. The operation
    /// arrives from a plain OS thread (not `spawn_blocking`, which would
    /// inhibit auto-advance) after real time passes: only a wall-clock
    /// deadline waits for it.
    #[tokio::test(start_paused = true)]
    async fn a_paused_clock_does_not_expire_the_wait() {
        let pause = Arc::new(PausePoint::unshared("paused clock").with_reach_timeout(SHORT));
        let held = pause.clone();
        let op = std::thread::spawn(move || {
            // timing-guard: the arrival must come after the idle runtime auto-advanced
            std::thread::sleep(Duration::from_millis(100));
            held.pause_blocking();
        });
        pause.wait_reached().await;
        pause.release();
        op.join().unwrap();
    }

    #[tokio::test]
    async fn arrivals_and_releases_are_counted() {
        let pause = PausePoint::new("twice");
        pause.release();
        let held = pause.clone();
        let mut op = tokio::spawn(async move {
            held.pause().await;
            held.pause().await;
        });
        pause.reached(&mut op).await;
        pause.reached(&mut op).await;
        pause.release();
        op.await.unwrap();
        assert_eq!(pause.arrivals(), 2);
    }

    /// An unspawned operation only runs while `reached` polls it, so with a
    /// release already waiting it arrives and finishes inside `reached`,
    /// which hands its output back instead of dropping it.
    #[tokio::test]
    async fn reached_returns_the_output_of_an_operation_that_finished_after_arriving() {
        let pause = PausePoint::new("inline");
        pause.release();
        let held = pause.clone();
        let mut op = Box::pin(async move {
            held.pause().await;
            Err::<(), _>("failed after the pause point")
        });
        assert_eq!(
            pause.reached(&mut op).await,
            Some(Err("failed after the pause point"))
        );
    }

    #[tokio::test]
    async fn an_operation_that_passes_a_pre_released_point_counts_as_reached() {
        let pause = PausePoint::new("pre-released");
        pause.release();
        let held = pause.clone();
        let mut op = tokio::spawn(async move {
            held.pause().await;
            "passed"
        });
        // Either the output came back with the arrival, or `op` was never
        // polled to completion and can still be awaited.
        let out = match pause.reached(&mut op).await {
            Some(out) => out,
            None => op.await,
        };
        assert_eq!(out.unwrap(), "passed");
    }
}
