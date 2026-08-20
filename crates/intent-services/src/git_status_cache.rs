//! Event-invalidated per-worktree cache for working-tree status scans
//! (monorepo#1648, derived-fields ladder rung 2).
//!
//! [`crate::git_status_singleflight`] stops *concurrent* `git.status` callers
//! from re-walking one worktree, but a steady drip of sequential reads still
//! paid a full [`intent_git::status::status`] scan each. This cache puts the
//! scan off the read path entirely: a read serves the last scanned
//! [`GitStatus`] until something invalidates it, and only a miss runs a scan —
//! coalesced through the same single-flight, so a burst of misses is still one
//! walk.
//!
//! Freshness comes from invalidation, not from polling: every daemon-observed
//! change marks the entry stale — git mutations (`git.stage`, `git.commit`,
//! branch switches, accept-changes steps) invalidate inline as they publish
//! `changes:git-status`, and external edits reach
//! [`crate::GitStatusRefresher`] via the `file:*` watcher and the `.git`
//! metadata watch, which invalidates and then repopulates through this cache
//! rather than running a competing scan. [`STATUS_CACHE_TTL`] is the backstop
//! for changes no daemon signal covers.
//!
//! Invalidation races the scan it interrupts: the leader captures the entry's
//! generation before it starts and stores its result only if the generation is
//! unchanged, so a change landing mid-scan is never overwritten by the older
//! snapshot — the next read misses and rescans. Followers never store: they
//! joined a flight whose generation is the leader's, not theirs. Failed scans
//! are never stored.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{Error, GitStatus, Result};

use crate::git_status_singleflight::{self, Join, StatusKey, StatusSingleFlight};

/// Fallback staleness bound for changes no daemon signal reports — edits that
/// bypass the file watcher (a worktree on a filesystem FSEvents/inotify does
/// not cover, a watcher that failed to register) or git metadata the `.git`
/// watch does not match. Deliberately short: the scan it re-authorizes is the
/// same one reads used to pay every time, so the worst case is today's
/// behavior once every 5s per worktree, while daemon-observed changes still
/// invalidate immediately.
pub(crate) const STATUS_CACHE_TTL: Duration = Duration::from_secs(5);

/// Test seam: invoked on the blocking pool immediately before each underlying
/// scan (counting + parking for coalescing/caching tests).
pub(crate) type ScanProbe = Option<Arc<dyn Fn() + Send + Sync>>;

/// Per-worktree cache state. `generation` advances on every invalidation so a
/// scan that started against an older state cannot publish into the cache.
#[derive(Default)]
struct Slot {
    generation: u64,
    cached: Option<(Arc<GitStatus>, Instant)>,
}

/// The event-invalidated status cache plus the single-flight it scans through.
/// Shared across [`crate::Services`] clones and with
/// [`crate::GitStatusRefresher`], so reads, mutations, and the watcher-driven
/// refresh all observe one cache.
pub struct GitStatusCache {
    flights: Arc<StatusSingleFlight>,
    slots: Mutex<HashMap<StatusKey, Slot>>,
    ttl: Duration,
}

impl Default for GitStatusCache {
    fn default() -> Self {
        Self::new()
    }
}

impl GitStatusCache {
    pub fn new() -> Self {
        Self {
            flights: Arc::new(StatusSingleFlight::default()),
            slots: Mutex::new(HashMap::new()),
            ttl: STATUS_CACHE_TTL,
        }
    }

    /// Test-only: compress the fallback TTL so expiry coverage completes in
    /// milliseconds instead of [`STATUS_CACHE_TTL`].
    #[cfg(test)]
    pub(crate) fn with_ttl(ttl: Duration) -> Self {
        Self { ttl, ..Self::new() }
    }

    /// Mark the cached status for `worktree` stale: the next read rescans.
    /// Also advances the generation, so a scan already in flight cannot store
    /// the pre-change snapshot it is about to produce.
    pub(crate) fn invalidate(&self, worktree: &Path) {
        let key = git_status_singleflight::status_key(worktree);
        // `get_mut`, not `entry().or_default()`: with no slot there is nothing
        // cached and no in-flight store to guard against — `get` creates the
        // slot before a scan starts, so every leader already has one. Avoids
        // allocating for paths that are only ever mutated, never read (the
        // `git.pull` invalidation runs against arbitrary repo paths, including
        // ones with no workspace row).
        if let Some(slot) = self.slots.lock().unwrap().get_mut(&key) {
            slot.generation = slot.generation.wrapping_add(1);
            slot.cached = None;
        }
    }

    /// Drop the slot for `worktree` entirely — for a worktree that is going
    /// away (workspace deletion), where [`Self::invalidate`] would keep an
    /// empty slot around forever.
    pub fn evict(&self, worktree: &Path) {
        let key = git_status_singleflight::status_key(worktree);
        self.slots.lock().unwrap().remove(&key);
    }

    /// The current status for `worktree`: the cached value when one is live,
    /// otherwise a scan (coalesced per worktree) whose result is cached.
    pub(crate) async fn get(&self, worktree: &Path, probe: ScanProbe) -> Result<Arc<GitStatus>> {
        let key = git_status_singleflight::status_key(worktree);
        {
            let mut slots = self.slots.lock().unwrap();
            let slot = slots.entry(key.clone()).or_default();
            if let Some((status, at)) = &slot.cached {
                if at.elapsed() < self.ttl {
                    return Ok(Arc::clone(status));
                }
                // TTL-expired: drop it now so a failing rescan cannot leave a
                // stale value behind to be served again.
                slot.cached = None;
            }
        }
        self.scan(&key, worktree, probe, None).await
    }

    /// Discard the cached status and require a scan that starts after this
    /// invalidation. If an older scan is already in flight, await and discard
    /// its result before joining or leading the next flight.
    pub(crate) async fn get_fresh(
        &self,
        worktree: &Path,
        probe: ScanProbe,
    ) -> Result<Arc<GitStatus>> {
        let key = git_status_singleflight::status_key(worktree);
        let minimum_generation = {
            let mut slots = self.slots.lock().unwrap();
            let slot = slots.entry(key.clone()).or_default();
            slot.generation = slot.generation.wrapping_add(1);
            slot.cached = None;
            slot.generation
        };
        self.scan(&key, worktree, probe, Some(minimum_generation))
            .await
    }

    /// Refresh for watcher-driven recomputes, using the same authoritative
    /// semantics as a forced `git.status` read.
    pub async fn refresh(&self, worktree: &Path) -> Result<Arc<GitStatus>> {
        self.get_fresh(worktree, None).await
    }

    /// The entry's current generation, snapshotted before a scan starts.
    fn generation(&self, key: &StatusKey) -> u64 {
        self.slots
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .generation
    }

    /// Store `status` only if the entry has not been invalidated since the
    /// scan started (see the module note on invalidation racing a scan).
    fn store(&self, key: &StatusKey, generation: u64, status: &Arc<GitStatus>) -> bool {
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.entry(key.clone()).or_default();
        if slot.generation == generation {
            slot.cached = Some((Arc::clone(status), Instant::now()));
            true
        } else {
            false
        }
    }

    /// Run the working-tree scan for `worktree` under per-worktree
    /// single-flight coalescing: the first caller leads the scan on the
    /// blocking pool and publishes the shared result; concurrent callers for
    /// the same worktree await it instead of re-walking the tree. A leader
    /// that vanishes without publishing frees the flight, so the result of a
    /// failed or cancelled scan is never reused.
    ///
    /// Only the leader caches its result, and only against the generation
    /// registered on that exact flight. Forced followers compare that flight
    /// generation with their invalidation generation and wait for an older
    /// published flight to retire before joining again.
    async fn scan(
        &self,
        key: &StatusKey,
        worktree: &Path,
        probe: ScanProbe,
        minimum_generation: Option<u64>,
    ) -> Result<Arc<GitStatus>> {
        loop {
            let candidate_generation = self.generation(key);
            match self.flights.join(key, candidate_generation) {
                Join::Leader(flight) => {
                    let generation = flight.generation();
                    // A libgit2 working-tree scan is unbounded CPU on a big
                    // repo; never run it on a Tokio worker.
                    let scan_path = worktree.to_path_buf();
                    let probe = probe.clone();
                    let scanned = tokio::task::spawn_blocking(move || {
                        if let Some(probe) = &probe {
                            probe();
                        }
                        intent_git::status::status(&scan_path)
                    })
                    .await
                    .map_err(|e| Error::Internal(format!("git status scan task failed: {e}")))?;
                    return match scanned {
                        Ok(status) => {
                            let shared = Arc::new(status);
                            let stored = self.store(key, generation, &shared);
                            flight.finish(Ok(Arc::clone(&shared)));
                            if minimum_generation
                                .is_some_and(|minimum| generation < minimum || !stored)
                            {
                                continue;
                            }
                            Ok(shared)
                        }
                        Err(e) => {
                            // Publish the inner message: every scan error is
                            // `Error::Internal` (map_git_err), and the follower
                            // re-wraps as `Error::Internal`, so coalesced
                            // callers observe the same variant and message (no
                            // double "internal error:" prefix).
                            flight.finish(Err(match &e {
                                Error::Internal(msg) => msg.clone(),
                                other => other.to_string(),
                            }));
                            Err(e)
                        }
                    };
                }
                Join::Follower(mut follower) => {
                    tracing::debug!(
                        worktree = %key.display(),
                        "git status: coalesced into in-flight worktree scan"
                    );
                    match follower.result().await {
                        Some(published) => {
                            let result = match published {
                                Ok(shared) => Ok(shared),
                                Err(msg) => Err(Error::Internal(msg)),
                            };
                            let generation = follower.generation();
                            if minimum_generation.is_some_and(|minimum| {
                                generation < minimum || self.generation(key) != generation
                            }) {
                                follower.wait_for_retirement().await;
                                continue;
                            }
                            return result;
                        }
                        // The leader vanished without publishing (cancelled
                        // RPC / panicked scan): retry — the next join elects a
                        // new leader.
                        None => continue,
                    }
                }
            }
        }
    }

    /// Test seam: number of followers currently awaiting the in-flight scan
    /// for `worktree`.
    #[cfg(test)]
    pub(crate) fn waiters(&self, worktree: &Path) -> usize {
        self.flights
            .waiters(&git_status_singleflight::status_key(worktree))
    }

    #[cfg(test)]
    pub(crate) fn retirement_waiters(&self, worktree: &Path) -> usize {
        self.flights
            .retirement_waiters(&git_status_singleflight::status_key(worktree))
    }

    #[cfg(test)]
    pub(crate) fn set_after_publish_probe(&self, probe: Arc<dyn Fn() + Send + Sync>) {
        self.flights.set_after_publish_probe(probe);
    }

    /// Test seam: number of slots currently held.
    #[cfg(test)]
    fn slot_count(&self) -> usize {
        self.slots.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invalidating a path that was never read allocates nothing — mutation
    /// paths run against arbitrary repo paths (`git.pull` before a workspace
    /// row exists).
    #[test]
    fn invalidating_an_unread_path_allocates_no_slot() {
        let cache = GitStatusCache::new();
        cache.invalidate(Path::new("/tmp/intentd-never-read"));
        assert_eq!(cache.slot_count(), 0);
    }

    /// Eviction drops the slot outright, so a deleted worktree's last scan is
    /// not retained for the daemon's lifetime.
    #[test]
    fn evict_drops_the_slot() {
        let cache = GitStatusCache::new();
        let path = Path::new("/tmp/intentd-evict-me");
        let key = git_status_singleflight::status_key(path);
        cache.slots.lock().unwrap().entry(key).or_default();
        assert_eq!(cache.slot_count(), 1);
        cache.evict(path);
        assert_eq!(cache.slot_count(), 0);
    }
}
