//! Poll-frame stack control for the transport's deep async paths.
//!
//! Same rule as `intent_services::stack_frame`: in a debug / test build
//! `callee(args).await` materializes the callee's whole future in the
//! CALLER's poll frame (about twice its size). [`boxed_await`] builds the
//! future in its own never-inlined frame and returns it boxed, so the caller
//! holds only a pointer. `conn::process_captured_frame` and `router::dispatch`
//! sit on every RPC's stack, so their large or growing callees (including
//! each per-namespace dispatch function) are awaited through this helper.

use std::future::Future;
use std::pin::Pin;

/// Build the future returned by `make` off the caller's poll frame and box it.
#[inline(never)]
pub(crate) fn boxed_await<Fut: Future>(make: impl FnOnce() -> Fut) -> Pin<Box<Fut>> {
    Box::pin(make())
}

#[cfg(test)]
mod tests {
    use super::boxed_await;

    #[tokio::test]
    async fn boxed_await_drives_the_future_to_completion() {
        let b = 2;
        assert_eq!(boxed_await(|| async { 1 + b }).await, 3);
    }
}
