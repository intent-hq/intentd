//! Generic TTL cache for expensive host discovery / binary-resolution probes
//! (`host.providerDiscovery`, `host.findBinary`, `host.toolAvailability`).
//!
//! Mirrors the daemon's existing `AuthStatusCache` pattern
//! (`intent-services/src/provider_auth.rs`) — TTL + single-flight, positives
//! only — but for SYNCHRONOUS filesystem/PATH probes rather than an async CLI
//! spawn. Callers of this cache run on the daemon's blocking-thread pool
//! (`spawn_blocking`), so single-flighting uses a per-key [`std::sync::Mutex`]
//! instead of an async `OnceCell`: the computing thread simply holds the lock
//! across the (synchronous, no `.await`) resolution, and every other caller
//! for the same key blocks on that same mutex until it either observes the
//! fresh result the leader just wrote, or repeats a genuinely stale probe
//! itself.
//!
//! Only a *positive* resolution (installed / binary found) is cached — a
//! negative result is never stored, so installing a tool, or an override
//! change that newly resolves a binary, is picked up on the very next call
//! instead of waiting out a stale not-found TTL entry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One cached key's compute-and-store lock. Concurrent callers for the same
/// key contend on `entry`'s mutex directly, so the loser of the race blocks
/// until the winner finishes (single-flight) and then observes the
/// now-fresh cached value instead of repeating the probe.
struct Slot<T> {
    entry: Mutex<Option<(Instant, T)>>,
}

/// A keyed TTL cache with per-key single-flighting, for cheap-to-clone
/// values. Positivity is caller-defined via `is_positive` in
/// [`DiscoveryCache::get_or_compute`]: only a positive result is stored, so a
/// miss always re-probes on the next call rather than serving a stale
/// negative.
pub struct DiscoveryCache<T> {
    slots: Mutex<HashMap<String, Arc<Slot<T>>>>,
    ttl: Duration,
}

impl<T: Clone> DiscoveryCache<T> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    fn slot(&self, key: &str) -> Arc<Slot<T>> {
        let mut slots = self.slots.lock().expect("discovery cache poisoned");
        slots
            .entry(key.to_string())
            .or_insert_with(|| {
                Arc::new(Slot {
                    entry: Mutex::new(None),
                })
            })
            .clone()
    }

    /// Resolve `key` through the cache. A fresh cached hit short-circuits
    /// `compute`; otherwise `compute` runs while holding the key's slot lock
    /// (serializing concurrent callers for the same key instead of racing
    /// the filesystem), and the result is cached only when `is_positive`
    /// accepts it — a negative result is returned but left uncached, so the
    /// next call (even within the TTL window) re-probes.
    pub fn get_or_compute(
        &self,
        key: &str,
        compute: impl FnOnce() -> T,
        is_positive: impl FnOnce(&T) -> bool,
    ) -> T {
        let slot = self.slot(key);
        let mut guard = slot.entry.lock().expect("discovery cache slot poisoned");
        if let Some((at, value)) = guard.as_ref() {
            if at.elapsed() < self.ttl {
                return value.clone();
            }
        }
        let value = compute();
        if is_positive(&value) {
            *guard = Some((Instant::now(), value.clone()));
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn caches_positive_result_within_ttl() {
        let cache: DiscoveryCache<i32> = DiscoveryCache::new(Duration::from_secs(60));
        let calls = AtomicUsize::new(0);
        let compute = || {
            calls.fetch_add(1, Ordering::SeqCst);
            42
        };
        assert_eq!(cache.get_or_compute("k", compute, |_| true), 42);
        assert_eq!(cache.get_or_compute("k", compute, |_| true), 42);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second call must hit cache"
        );
    }

    #[test]
    fn does_not_cache_negative_result() {
        let cache: DiscoveryCache<Option<i32>> = DiscoveryCache::new(Duration::from_secs(60));
        let calls = AtomicUsize::new(0);
        let compute = || {
            calls.fetch_add(1, Ordering::SeqCst);
            None
        };
        assert_eq!(cache.get_or_compute("k", compute, |v| v.is_some()), None);
        assert_eq!(cache.get_or_compute("k", compute, |v| v.is_some()), None);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a negative result must never be served from cache"
        );
    }

    #[test]
    fn expired_entry_is_recomputed() {
        let cache: DiscoveryCache<i32> = DiscoveryCache::new(Duration::from_millis(1));
        let calls = AtomicUsize::new(0);
        let compute = || {
            calls.fetch_add(1, Ordering::SeqCst);
            7
        };
        assert_eq!(cache.get_or_compute("k", compute, |_| true), 7);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cache.get_or_compute("k", compute, |_| true), 7);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "an expired entry must be recomputed"
        );
    }

    #[test]
    fn distinct_keys_do_not_share_entries() {
        let cache: DiscoveryCache<i32> = DiscoveryCache::new(Duration::from_secs(60));
        assert_eq!(cache.get_or_compute("a", || 1, |_| true), 1);
        assert_eq!(cache.get_or_compute("b", || 2, |_| true), 2);
        // Re-reading "a" must still return its own cached value, not "b"'s.
        assert_eq!(cache.get_or_compute("a", || 99, |_| true), 1);
    }

    #[test]
    fn concurrent_callers_for_same_key_single_flight() {
        use std::sync::Barrier;

        let cache: Arc<DiscoveryCache<i32>> =
            Arc::new(DiscoveryCache::new(Duration::from_secs(60)));
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = cache.clone();
            let calls = calls.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                cache.get_or_compute(
                    "shared",
                    || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(20));
                        5
                    },
                    |_| true,
                )
            }));
        }
        for h in handles {
            assert_eq!(h.join().unwrap(), 5);
        }
        // Every racer either computed (blocked on the same mutex) or observed
        // the fresh cached value — a well-behaved single-flight never needs
        // more computations than racers, but must never fully re-run for
        // every racer once the first has stored a fresh entry.
        assert!(
            calls.load(Ordering::SeqCst) < 4,
            "concurrent callers for one key must not all recompute independently"
        );
    }
}
