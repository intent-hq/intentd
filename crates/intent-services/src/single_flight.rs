//! Single-flight slots for detached background computations (e.g. the
//! `workspace.diskUsage` walks): a key is claimed by at most one caller at a
//! time and released on drop, including on panic or task cancellation.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// RAII guard for a single-flight key: removes the key on drop, including on
/// panic or task cancellation, so a failed computation can never wedge the
/// single-flight state for the daemon's lifetime.
pub(crate) struct InFlightGuard<K: Eq + Hash> {
    set: Arc<Mutex<HashSet<K>>>,
    key: K,
}

impl<K: Eq + Hash> Drop for InFlightGuard<K> {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.key);
    }
}

/// Claim the single-flight slot for `key`. Returns `None` when another caller
/// already holds it.
pub(crate) fn try_begin<K: Eq + Hash + Clone>(
    set: &Arc<Mutex<HashSet<K>>>,
    key: K,
) -> Option<InFlightGuard<K>> {
    set.lock()
        .unwrap()
        .insert(key.clone())
        .then(|| InFlightGuard {
            set: Arc::clone(set),
            key,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The single-flight guard releases its slot on drop (panic-safety path).
    #[test]
    fn in_flight_guard_releases_slot_on_drop() {
        let set: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let guard = try_begin(&set, "key".to_string()).expect("claims slot");
        // A second claim on the same key loses while the guard is held.
        assert!(try_begin(&set, "key".to_string()).is_none());
        drop(guard);
        assert!(!set.lock().unwrap().contains("key"));
        assert!(try_begin(&set, "key".to_string()).is_some());
    }
}
