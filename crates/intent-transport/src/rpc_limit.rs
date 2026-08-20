//! Daemon-wide cap on outstanding slow-path RPCs (`server.maxOutstandingRpcs`).
//!
//! The three detached-spawn slow paths in [`crate::conn::process_frame`] (the
//! `host.*` arm, the `browser.*` arm, and the trailing JSON-RPC dispatcher)
//! spawn one tokio task per request, so a client — or a fleet of connections —
//! can otherwise queue unbounded concurrent work. One [`RpcLimiter`] is built
//! per daemon and shared by every listener (UDS + WSS), so the cap is global,
//! not per-connection. Fast paths that run inline on the read loop are already
//! serialized per connection and are not gated.
//!
//! Over-limit requests are rejected immediately (never queued): a request with
//! an `id` gets `-32011 "Server overloaded"` with the echoed id, a notification
//! is dropped without a response (PROTOCOL §9).
//!
//! Fairness tradeoff: the pool is global with no per-connection reservation,
//! so one flooding client can consume every slot and other connections —
//! including the FE's own UDS traffic — then see `-32011` until it drains. That
//! is the intended posture for a resource-exhaustion cap; a per-connection
//! sub-cap or reserved local headroom is tracked separately.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// JSON-RPC error code for a rejected over-limit request.
pub(crate) const OVERLOAD_ERROR_CODE: i32 = -32011;

/// JSON-RPC error message for a rejected over-limit request.
pub(crate) const OVERLOAD_ERROR_MESSAGE: &str = "Server overloaded";

/// Shared permit source for the slow-path spawn sites. Cheap to clone (an
/// `Arc` inside); [`RpcLimiter::unlimited`] disables the cap entirely by
/// carrying no semaphore at all.
///
/// There is deliberately no `Default` impl: an implicit default would be
/// *unlimited*, letting a composition root that forgets to wire the limiter
/// silently opt out of the cap with no compile error. Callers must choose
/// [`RpcLimiter::new`] or [`RpcLimiter::unlimited`] explicitly; only the
/// `intentd` composition root decides the cap, and test wrappers such as
/// `serve_uds` intentionally pass `unlimited()`.
#[derive(Clone)]
pub struct RpcLimiter {
    semaphore: Option<Arc<Semaphore>>,
    /// Whether the cap is currently saturated, so sustained overload logs one
    /// WARN per transition into saturation instead of one per rejected frame.
    saturated: Arc<AtomicBool>,
}

impl RpcLimiter {
    /// Build a limiter capping outstanding slow-path RPCs at
    /// `max_outstanding`; `0` means unlimited (`server.maxOutstandingRpcs`).
    pub fn new(max_outstanding: u32) -> Self {
        if max_outstanding == 0 {
            return Self::unlimited();
        }
        Self {
            semaphore: Some(Arc::new(Semaphore::new(max_outstanding as usize))),
            saturated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A limiter that never rejects — the explicit opt-out for lightweight
    /// wrappers (e.g. `serve_uds`) that deliberately run without a cap.
    pub fn unlimited() -> Self {
        Self {
            semaphore: None,
            saturated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Try to claim one slot. `Ok(None)` means the limiter is unlimited;
    /// `Ok(Some(permit))` hands the caller a permit that MUST be moved into
    /// the spawned task so the slot is released when that task ends (including
    /// panic unwinds); `Err(Overloaded)` means the cap is reached.
    pub(crate) fn try_acquire(&self) -> Result<Option<OwnedSemaphorePermit>, Overloaded> {
        let Some(semaphore) = self.semaphore.clone() else {
            return Ok(None);
        };
        match semaphore.try_acquire_owned() {
            Ok(permit) => {
                self.saturated.store(false, Ordering::Relaxed);
                Ok(Some(permit))
            }
            // A closed semaphore can only happen if something explicitly
            // closes it (nothing does); treat it as overloaded rather than
            // silently disabling the cap.
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => Err(Overloaded {
                newly_saturated: !self.saturated.swap(true, Ordering::Relaxed),
            }),
        }
    }

    /// Slots currently free, or `None` when the limiter is unlimited.
    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.semaphore.as_ref().map(|s| s.available_permits())
    }
}

impl std::fmt::Debug for RpcLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcLimiter")
            .field("available_permits", &self.available_permits())
            .finish()
    }
}

/// The cap is reached: the request must be rejected, never queued.
#[derive(Debug)]
pub(crate) struct Overloaded {
    /// First rejection since the limiter last handed out a permit, i.e. the
    /// transition into saturation — the one worth a WARN.
    pub(crate) newly_saturated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_unlimited() {
        let limiter = RpcLimiter::new(0);
        assert_eq!(limiter.available_permits(), None);
        let mut permits = Vec::new();
        for _ in 0..1000 {
            permits.push(limiter.try_acquire().expect("never rejects"));
        }
    }

    #[test]
    fn permits_are_capped_and_released_on_drop() {
        let limiter = RpcLimiter::new(2);
        assert_eq!(limiter.available_permits(), Some(2));
        let first = limiter.try_acquire().expect("slot 1").expect("permit");
        let second = limiter.try_acquire().expect("slot 2").expect("permit");
        assert_eq!(limiter.available_permits(), Some(0));
        assert!(limiter.try_acquire().is_err(), "third must be rejected");
        drop(first);
        assert_eq!(limiter.available_permits(), Some(1));
        assert!(limiter.try_acquire().is_ok(), "freed slot is reusable");
        drop(second);
    }

    /// Only the transition into saturation is flagged, so sustained overload
    /// logs once instead of once per rejected frame; draining re-arms it.
    #[test]
    fn only_the_transition_into_saturation_is_flagged() {
        let limiter = RpcLimiter::new(1);
        let held = limiter.try_acquire().expect("slot").expect("permit");
        let first = limiter.try_acquire().expect_err("rejected");
        assert!(first.newly_saturated, "first rejection is the transition");
        let second = limiter.try_acquire().expect_err("rejected");
        assert!(!second.newly_saturated, "sustained overload stays quiet");
        drop(held);
        let _regained = limiter.try_acquire().expect("slot").expect("permit");
        let after = limiter.try_acquire().expect_err("rejected");
        assert!(after.newly_saturated, "re-saturation is flagged again");
    }

    #[test]
    fn clones_share_one_pool() {
        let limiter = RpcLimiter::new(1);
        let clone = limiter.clone();
        let held = limiter.try_acquire().expect("slot").expect("permit");
        assert!(clone.try_acquire().is_err(), "clone shares the pool");
        drop(held);
        assert!(clone.try_acquire().is_ok());
    }

    /// The permit is moved into the spawned handler task, so it must be
    /// released when that task ends normally.
    #[tokio::test]
    async fn permit_is_released_when_the_spawned_task_completes() {
        let limiter = RpcLimiter::new(1);
        let permit = limiter.try_acquire().expect("slot").expect("permit");
        let task = tokio::spawn(async move {
            let _permit = permit;
        });
        task.await.expect("task completes");
        assert_eq!(limiter.available_permits(), Some(1));
        assert!(limiter.try_acquire().is_ok());
    }

    /// A panicking handler unwinds the spawned task; the permit drops with the
    /// unwind, so a panic can never leak a slot.
    ///
    /// The panic hook is process-global, so the swap is serialized with the
    /// `panic_guard` tests through the shared global-state lock — otherwise
    /// parallel hook swaps restore each other's hook. The lock is held across
    /// the `await` on purpose: the quiet hook must stay installed until the
    /// spawned task has unwound.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn permit_is_released_when_the_spawned_task_panics() {
        let limiter = RpcLimiter::new(1);
        let permit = limiter.try_acquire().expect("slot").expect("permit");
        let _global = crate::panic_guard::test_support::lock_global_state();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let task = tokio::spawn(async move {
            let _permit = permit;
            panic!("boom");
        });
        let panicked = task.await.is_err();
        std::panic::set_hook(prev);
        assert!(panicked, "task must have panicked");
        assert_eq!(limiter.available_permits(), Some(1));
        assert!(limiter.try_acquire().is_ok());
    }
}
