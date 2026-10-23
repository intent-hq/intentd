//! Poll-frame stack control for deep async call paths.
//!
//! In an unoptimized (debug / test) build rustc gives every MIR temporary its
//! own stack slot. `callee(args).await` therefore materializes the callee's
//! whole future as a temporary in the CALLER's poll frame, and again as the
//! `IntoFuture::into_future` result, before moving it into the caller's
//! state: each await site costs roughly twice the callee future's size of
//! stack, live for every poll of the caller. A call-site
//! `Box::pin(callee(args))` only removes one of the two copies, because the
//! future is still built in the caller's frame before it is boxed.
//!
//! [`boxed_await`] builds the future inside its own short-lived,
//! never-inlined frame and returns it already boxed, so the caller's poll
//! frame holds just the closure's captures plus a pointer. The agent-turn path
//! (`run_message_worker` → `retry_spawn_owned` → `ensure_started_owned` →
//! `start_session_opened`) nests several large poll frames on one stack, so
//! its await sites use it; the reduced-stack guard
//! `agent_manager::tests::turn_path_fits_reduced_stack` keeps the path within
//! budget.

use std::future::Future;
use std::pin::Pin;

/// Build the future returned by `make` off the caller's poll frame and box it.
///
/// Use as `boxed_await(|| callee(args)).await` at await sites of large or
/// cold futures on deep async paths. The cost is one heap allocation per call.
#[inline(never)]
pub(crate) fn boxed_await<Fut: Future>(make: impl FnOnce() -> Fut) -> Pin<Box<Fut>> {
    Box::pin(make())
}

#[cfg(test)]
mod tests {
    use super::boxed_await;

    async fn add(a: u32, b: &u32) -> u32 {
        tokio::task::yield_now().await;
        a + *b
    }

    #[tokio::test]
    async fn boxed_await_drives_the_future_to_completion() {
        let b = 2;
        assert_eq!(boxed_await(|| add(1, &b)).await, 3);
    }
}
