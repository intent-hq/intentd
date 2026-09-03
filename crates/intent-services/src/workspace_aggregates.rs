//! Offloaded, cached computation of git-derived workspace aggregates
//! (`cowSupported` for list/get).
//!
//! `diffSummary` is no longer computed here: `workspace.list` / `workspace.get`
//! / live-state subscription re-reads used to embed a full `head_diff_rollup`
//! on every workspace, which pinned the blocking pool whenever list polling or
//! `lastActivity` events re-materialized workspace rows. Desktop FE deprecated
//! `diffSummary` on metadata payloads and fetches diffs on demand via
//! `git.diffs`, so the per-worktree diff cache (and its event-driven
//! invalidation subscriber) has been removed.
//!
//! ## `CoW` probe cache
//!
//! Machine capability of the workspaces root; lifetime cache with
//! single-flight + serialized live probes. Over-budget calls omit the field;
//! the detached probe keeps running and backfills the cache.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Wall-clock budget one list/get call spends waiting for a single aggregate
/// before degrading to the last known value / omission.
///
/// Kept below the `rpc_profile` duration budget for hot RPCs (1000 ms): a cold
/// `CoW` probe used to wait up to 1500 ms inside the `workspace.list` dispatch,
/// so the first list after daemon start was guaranteed to draw a
/// duration-budget WARN regardless of actual list cost
/// (intent-hq/monorepo#2994). An over-budget probe still completes detached
/// and backfills the cache for the next poll.
const AGGREGATE_BUDGET: Duration = Duration::from_millis(900);

/// Shared cache + offload gates for the git-derived card aggregates. Held as
/// an `Arc` field on `Services` so every clone (and thus every concurrent
/// list/get call) observes the same cache and single-flight state.
pub(crate) struct WorkspaceAggregateCache {
    /// `CoW` support per workspaces root. This is a second layer over
    /// `intent_git::cow_probe`'s own process-wide cache: a hit here skips the
    /// `tokio::spawn` + probe-gate + `spawn_blocking` round-trip entirely.
    cow: Mutex<HashMap<PathBuf, bool>>,
    /// Shared result receivers for roots with a probe currently in flight.
    cow_in_flight: Arc<Mutex<HashMap<PathBuf, tokio::sync::watch::Receiver<Option<bool>>>>>,
    /// Serializes live `CoW` probes (shared `.cow_probe_temp` collision guard).
    cow_probe_gate: tokio::sync::Mutex<()>,
    budget: Duration,
}

/// Removes a completed or cancelled `CoW` probe from the shared flight map.
struct CowInFlightGuard {
    flights: Arc<Mutex<HashMap<PathBuf, tokio::sync::watch::Receiver<Option<bool>>>>>,
    key: PathBuf,
}

impl Drop for CowInFlightGuard {
    fn drop(&mut self) {
        self.flights.lock().unwrap().remove(&self.key);
    }
}

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

impl WorkspaceAggregateCache {
    pub(crate) fn new() -> Self {
        Self {
            cow: Mutex::new(HashMap::new()),
            cow_in_flight: Arc::new(Mutex::new(HashMap::new())),
            cow_probe_gate: tokio::sync::Mutex::new(()),
            budget: AGGREGATE_BUDGET,
        }
    }

    /// Cache-only read used by workspace list/get paths. A miss omits
    /// immediately without starting or awaiting filesystem work.
    pub(crate) fn cached_cow_supported(&self, workspaces_root: &PathBuf) -> Option<bool> {
        self.cow.lock().unwrap().get(workspaces_root).copied()
    }

    /// Start a best-effort detached probe for a cache miss. Single-flight and
    /// the serialized probe gate remain enforced by [`Self::cow_supported`].
    pub(crate) fn prewarm_cow_supported(self: &Arc<Self>, workspaces_root: PathBuf) {
        if self.cached_cow_supported(&workspaces_root).is_some() {
            return;
        }
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            let _ = cache.cow_supported(workspaces_root).await;
        });
    }

    /// Compute (or serve from cache) the `cowSupported` aggregate for a
    /// workspaces root. The probe runs root→root, so it reports whether the
    /// root's filesystem supports `CoW` cloning — a machine capability,
    /// independent of any repository. Completed probes are cached for the
    /// daemon's lifetime (support is invariant per root); an over-budget
    /// probe yields `None` for this call but keeps running detached and
    /// backfills the cache for the next poll. Failed probes are not cached,
    /// so a later call retries.
    pub(crate) async fn cow_supported(self: &Arc<Self>, workspaces_root: PathBuf) -> Option<bool> {
        let key = workspaces_root;
        if let Some(v) = self.cow.lock().unwrap().get(&key) {
            return Some(*v);
        }
        // Share one detached probe per root. Unlike workspace list/get, the
        // on-demand system capability RPC joins an existing startup prewarm so
        // a cold request cannot race it and observe an empty capability object.
        let mut receiver = {
            let mut flights = self.cow_in_flight.lock().unwrap();
            if let Some(receiver) = flights.get(&key) {
                receiver.clone()
            } else {
                let (sender, receiver) = tokio::sync::watch::channel(None);
                flights.insert(key.clone(), receiver.clone());
                let cache = Arc::clone(self);
                let flight_key = key.clone();
                tokio::spawn(async move {
                    let _in_flight = CowInFlightGuard {
                        flights: Arc::clone(&cache.cow_in_flight),
                        key: flight_key,
                    };
                    // Serialize live probes: concurrent probes into the same
                    // workspaces_root collide on the shared `.cow_probe_temp` name.
                    let _gate = cache.cow_probe_gate.lock().await;
                    if let Some(v) = cache.cow.lock().unwrap().get(&key) {
                        sender.send_replace(Some(*v));
                        return;
                    }
                    let started = Instant::now();
                    let root = key.clone();
                    // A fresh configured `workspaces.root` may not exist yet; the
                    // probe needs the directory to write its temp file into.
                    let result = match tokio::task::spawn_blocking(move || {
                        std::fs::create_dir_all(&root).map_err(|e| {
                            intent_core::Error::Internal(format!("create workspaces root: {e}"))
                        })?;
                        intent_git::cow_probe(&root, &root)
                    })
                    .await
                    {
                        Ok(Ok(support)) => {
                            let supported = matches!(support, intent_git::CowSupport::Supported);
                            cache.cow.lock().unwrap().insert(key, supported);
                            tracing::debug!(
                                supported,
                                total_ms = u64::try_from(started.elapsed().as_millis())
                                    .unwrap_or(u64::MAX),
                                "workspace aggregates: cow probe"
                            );
                            Some(supported)
                        }
                        Ok(Err(e)) => {
                            tracing::debug!(
                                workspaces_root = %key.display(),
                                error = %e,
                                "workspace aggregates: cow probe failed; will retry on a later call"
                            );
                            None
                        }
                        Err(e) => {
                            tracing::warn!(
                                workspaces_root = %key.display(),
                                error = %e,
                                "workspace aggregates: cow probe blocking task failed"
                            );
                            None
                        }
                    };
                    sender.send_replace(result);
                });
                receiver
            }
        };
        if let Some(value) = *receiver.borrow() {
            return Some(value);
        }
        match tokio::time::timeout(self.budget, receiver.changed()).await {
            Ok(Ok(())) => *receiver.borrow_and_update(),
            Ok(Err(_)) => *receiver.borrow(),
            Err(_) => {
                // Over budget: the detached probe keeps running and backfills
                // the cache for the next poll.
                tracing::debug!(
                    budget_ms = u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
                    "workspace aggregates: cow probe over budget; omitting cowSupported"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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

    #[tokio::test]
    async fn cow_supported_caches_per_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("workspaces");
        fs::create_dir(&root).unwrap();
        let cache = Arc::new(WorkspaceAggregateCache::new());
        let first = cache.cow_supported(root.clone()).await;
        assert!(first.is_some(), "same-volume probe should succeed");
        assert_eq!(cache.cow.lock().unwrap().len(), 1);
        let second = cache.cow_supported(root.clone()).await;
        assert_eq!(first, second);
    }

    /// A fresh configured `workspaces.root` may not exist on disk yet; the
    /// probe must create it rather than omit `cowSupported`.
    #[tokio::test]
    async fn cow_supported_creates_missing_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("not-yet-created").join("workspaces");
        assert!(!root.exists());
        let cache = Arc::new(WorkspaceAggregateCache::new());
        let result = cache.cow_supported(root.clone()).await;
        assert!(result.is_some(), "probe should create the missing root");
        assert!(root.exists());
    }

    #[tokio::test]
    async fn cow_supported_joins_in_flight_prewarm() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("workspaces");
        let cache = Arc::new(WorkspaceAggregateCache::new());
        let gate = cache.cow_probe_gate.lock().await;
        cache.prewarm_cow_supported(root.clone());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cache.cow_in_flight.lock().unwrap().contains_key(&root) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("prewarm should enter shared flight");
        let waiter_cache = Arc::clone(&cache);
        let waiter = tokio::spawn(async move { waiter_cache.cow_supported(root).await });
        drop(gate);
        assert!(waiter.await.unwrap().is_some());
    }
}
