//! Test-only tracing plumbing shared by every capture-style subscriber test
//! in this crate (regression: monorepo#3580).
//!
//! Tests that assert on captured `tracing` events install a thread-local
//! subscriber via `tracing::subscriber::set_default`. Without a global
//! default, `tracing-core`'s callsite interest cache can be poisoned to
//! `never` under parallel tests: when its dispatcher registry believes there
//! is at most one live dispatcher, a callsite registering on another thread
//! rebuilds its interest *without the registry lock* from that thread's
//! current default — `NoSubscriber` on a thread with no local subscriber,
//! yielding `Interest::never()`. That unlocked store races the locked rebuild
//! triggered by a test's `set_default` and can land after it (lost update). A
//! cached `never` short-circuits `enabled()` entirely, so the capture sees
//! zero events and nothing rebuilds the cache before the test asserts.
//!
//! [`set_capture_default`] closes the race by installing a process-global
//! [`InterestAnchor`] (once) before setting the thread-local capture. The
//! anchor replaces the `NoSubscriber` fallback: every rebuild path now
//! resolves interest to at least `sometimes`, which forces the per-event
//! `enabled()` check instead of dropping events at the callsite.

/// Install `capture` as the thread-local default, with the process-global
/// [`InterestAnchor`] in place first. Use this instead of calling
/// `tracing::subscriber::set_default` directly in any test that asserts on
/// captured events.
pub(crate) fn set_capture_default<S>(capture: S) -> tracing::subscriber::DefaultGuard
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    static ANCHOR: std::sync::Once = std::sync::Once::new();
    ANCHOR.call_once(|| {
        tracing::subscriber::set_global_default(InterestAnchor).expect(
            "intent-services tests own this process's global tracing default; \
             a competing set_global_default call would silently re-expose the \
             monorepo#3580 interest-cache race",
        );
    });
    tracing::subscriber::set_default(capture)
}

/// Process-global fallback dispatcher that pins every callsite's interest at
/// `sometimes` so it can never be cached as `never` (see the module docs). It
/// consumes nothing itself: `enabled()` is always `false`, so threads without
/// a thread-local capture drop events exactly as they did with no global
/// default.
struct InterestAnchor;

impl tracing::Subscriber for InterestAnchor {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
