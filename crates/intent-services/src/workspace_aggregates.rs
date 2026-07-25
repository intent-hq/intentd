//! Offloaded, cached computation of the git-derived workspace card aggregates
//! (`diffSummary`, `cowSupported`) for the `workspace.list` / `workspace.get`
//! emit path (§9.1).
//!
//! Both aggregates are blocking libgit2/filesystem work (`head_diff_rollup`
//! runs two full workdir diffs plus an untracked scan; `cow_probe` does a live
//! clone probe). Running them inline per workspace made `workspace.list`
//! O(workspaces × workdir-diff) on the async runtime and blew past FE RPC
//! timeouts. This module bounds that cost:
//!
//! - **Blocking pool**: all rollups/probes run under `spawn_blocking`, never
//!   inline on tokio worker threads.
//! - **Bounded concurrency**: a global semaphore caps concurrent rollups.
//! - **Single-flight + short TTL cache**: one rollup per worktree at a time;
//!   completed rollups are cached briefly so FE list polling doesn't redo the
//!   same diff every call.
//! - **Per-call budget**: a list/get call waits at most [`AGGREGATE_BUDGET`]
//!   for a rollup, then serves the last completed value (possibly stale) or
//!   omits the aggregate — the wire shape keeps both fields optional. The
//!   detached computation still finishes and fills the cache, so a subsequent
//!   poll picks the value up.
//! - **CoW probe cache**: `cowSupported` is invariant per workspaces root
//!   (it is a machine capability of the root's filesystem), so successful
//!   probes are cached for the daemon's lifetime (over-budget probes finish
//!   detached and backfill the cache; failed probes are not cached so a
//!   later call retries). Live probes are serialized because concurrent
//!   probes into the same `workspaces_root` would collide on the shared
//!   `.cow_probe_temp` file now that enrichment fans out in parallel.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{now_iso, WorkspaceDiffSummary};

/// How long a completed diff rollup stays fresh. Card aggregates are advisory
/// (workspace cards, not the Changes panel), so brief staleness is acceptable
/// in exchange for not re-diffing every FE list poll.
const DIFF_SUMMARY_TTL: Duration = Duration::from_secs(5);

/// Wall-clock budget one list/get call spends waiting for a single aggregate
/// before degrading to the last known value / omission.
const AGGREGATE_BUDGET: Duration = Duration::from_millis(1_500);

/// Cap on concurrent blocking diff rollups across all callers, so a burst of
/// list calls over many large repos cannot exhaust the blocking pool.
const MAX_CONCURRENT_ROLLUPS: usize = 4;

/// Cap on concurrent per-workspace enrichment tasks in `workspace.list`, so a
/// large workspace count doesn't burst-issue unbounded concurrent store reads
/// (the git/FS side is separately bounded by [`MAX_CONCURRENT_ROLLUPS`]).
pub(crate) const MAX_CONCURRENT_ENRICHMENTS: usize = 8;

/// One completed rollup for a worktree. `summary` is `None` for legitimate
/// "no summary" outcomes (clean tree, not a git repo) — cached like any other
/// result so those worktrees aren't re-scanned every call within the TTL.
struct DiffCacheEntry {
    computed_at: Instant,
    summary: Option<WorkspaceDiffSummary>,
}

/// Shared cache + offload gates for the git-derived card aggregates. Held as
/// an `Arc` field on `Services` so every clone (and thus every concurrent
/// list/get call) observes the same cache and single-flight state.
pub(crate) struct WorkspaceAggregateCache {
    /// Last completed diff rollup per worktree path.
    diff: Mutex<HashMap<String, DiffCacheEntry>>,
    /// Worktrees with a rollup currently in flight (single-flight guard).
    diff_in_flight: Arc<Mutex<HashSet<String>>>,
    /// Bounds concurrent blocking rollups.
    gate: tokio::sync::Semaphore,
    /// CoW support per workspaces root. This is a second layer over
    /// `intent_git::cow_probe`'s own process-wide cache: a hit here skips the
    /// `tokio::spawn` + probe-gate + `spawn_blocking` round-trip entirely.
    cow: Mutex<HashMap<PathBuf, bool>>,
    /// Roots with a probe currently in flight (single-flight guard).
    cow_in_flight: Arc<Mutex<HashSet<PathBuf>>>,
    /// Serializes live CoW probes (shared `.cow_probe_temp` collision guard).
    cow_probe_gate: tokio::sync::Mutex<()>,
    ttl: Duration,
    budget: Duration,
}

/// RAII guard for a single-flight key: removes the key on drop, including on
/// panic or task cancellation, so a failed computation can never wedge the
/// single-flight state for the daemon's lifetime.
struct InFlightGuard<K: Eq + Hash> {
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
fn try_begin<K: Eq + Hash + Clone>(
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
        Self::with_timing(DIFF_SUMMARY_TTL, AGGREGATE_BUDGET)
    }

    /// Construct with explicit TTL/budget (tests shrink both to exercise the
    /// recompute and over-budget degradation paths deterministically).
    pub(crate) fn with_timing(ttl: Duration, budget: Duration) -> Self {
        Self {
            diff: Mutex::new(HashMap::new()),
            diff_in_flight: Arc::new(Mutex::new(HashSet::new())),
            gate: tokio::sync::Semaphore::new(MAX_CONCURRENT_ROLLUPS),
            cow: Mutex::new(HashMap::new()),
            cow_in_flight: Arc::new(Mutex::new(HashSet::new())),
            cow_probe_gate: tokio::sync::Mutex::new(()),
            ttl,
            budget,
        }
    }

    /// Compute (or serve from cache) the `diffSummary` aggregate for a
    /// worktree. Never blocks the async runtime and never waits longer than
    /// the configured budget; on a miss that can't complete in time it returns
    /// the last completed rollup (possibly stale) or `None`.
    pub(crate) async fn diff_summary(
        self: &Arc<Self>,
        workspace_id: &str,
        worktree: PathBuf,
    ) -> Option<WorkspaceDiffSummary> {
        let key = worktree.to_string_lossy().into_owned();
        if let Some(hit) = self.lookup_diff(&key, true) {
            return hit;
        }
        // Single-flight: only the first caller spawns a rollup for this
        // worktree; concurrent callers serve the last completed value instead
        // of stacking duplicate diffs. The guard clears the key on drop even
        // if the rollup panics, so the slot can never be wedged.
        let Some(guard) = try_begin(&self.diff_in_flight, key.clone()) else {
            return self.lookup_diff(&key, false).flatten();
        };
        let cache = Arc::clone(self);
        let task_key = key.clone();
        let handle = tokio::spawn(async move {
            let _guard = guard;
            cache.rollup_and_store(&task_key, worktree).await
        });
        match tokio::time::timeout(self.budget, handle).await {
            Ok(Ok(summary)) => summary,
            Ok(Err(e)) => {
                tracing::warn!(
                    workspace_id,
                    worktree = %key,
                    error = %e,
                    "workspace aggregates: diff rollup task failed; serving last known value"
                );
                self.lookup_diff(&key, false).flatten()
            }
            Err(_) => {
                // Over budget: the detached task keeps running and will fill
                // the cache for the next poll.
                tracing::debug!(
                    workspace_id,
                    worktree = %key,
                    budget_ms = self.budget.as_millis() as u64,
                    "workspace aggregates: diff rollup over budget; serving last known value"
                );
                self.lookup_diff(&key, false).flatten()
            }
        }
    }

    /// Cache lookup. Outer `Option` distinguishes "entry present" from a miss;
    /// the inner value is the cached summary (which may itself be `None`).
    /// `fresh_only` enforces the TTL; stale entries are served on the
    /// degradation paths above.
    fn lookup_diff(&self, key: &str, fresh_only: bool) -> Option<Option<WorkspaceDiffSummary>> {
        let map = self.diff.lock().unwrap();
        map.get(key)
            .filter(|e| !fresh_only || e.computed_at.elapsed() < self.ttl)
            .map(|e| e.summary.clone())
    }

    /// Run one bounded, offloaded rollup and record the result. Failures
    /// (blocking-task panic) are not cached so the next call retries.
    async fn rollup_and_store(&self, key: &str, worktree: PathBuf) -> Option<WorkspaceDiffSummary> {
        let _permit = self.gate.acquire().await.ok()?;
        let started = Instant::now();
        let summary =
            match tokio::task::spawn_blocking(move || compute_diff_summary_blocking(&worktree))
                .await
            {
                Ok(summary) => summary,
                Err(e) => {
                    tracing::warn!(
                        worktree = %key,
                        error = %e,
                        "workspace aggregates: diff rollup blocking task failed"
                    );
                    return None;
                }
            };
        tracing::debug!(
            worktree = %key,
            files = summary.as_ref().map(|s| s.total_files).unwrap_or(0),
            total_ms = started.elapsed().as_millis() as u64,
            "workspace aggregates: head diff rollup"
        );
        self.diff.lock().unwrap().insert(
            key.to_string(),
            DiffCacheEntry {
                computed_at: Instant::now(),
                summary: summary.clone(),
            },
        );
        summary
    }

    /// Compute (or serve from cache) the `cowSupported` aggregate for a
    /// workspaces root. The probe runs root→root, so it reports whether the
    /// root's filesystem supports CoW cloning — a machine capability,
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
        // Single-flight per root: while a probe is in flight, concurrent
        // callers omit the field instead of queueing duplicate detached tasks
        // behind the probe gate.
        let guard = try_begin(&self.cow_in_flight, key.clone())?;
        let cache = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let _in_flight = guard;
            // Serialize live probes: concurrent probes into the same
            // workspaces_root collide on the shared `.cow_probe_temp` name.
            let _gate = cache.cow_probe_gate.lock().await;
            if let Some(v) = cache.cow.lock().unwrap().get(&key) {
                return Some(*v);
            }
            let started = Instant::now();
            let root = key.clone();
            match tokio::task::spawn_blocking(move || intent_git::cow_probe(&root, &root)).await {
                Ok(Ok(support)) => {
                    let supported = matches!(support, intent_git::CowSupport::Supported);
                    cache.cow.lock().unwrap().insert(key, supported);
                    tracing::debug!(
                        supported,
                        total_ms = started.elapsed().as_millis() as u64,
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
            }
        });
        match tokio::time::timeout(self.budget, handle).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e,
                    "workspace aggregates: cow probe task failed; omitting cowSupported"
                );
                None
            }
            Err(_) => {
                // Over budget: the detached probe keeps running and backfills
                // the cache for the next poll.
                tracing::debug!(
                    budget_ms = self.budget.as_millis() as u64,
                    "workspace aggregates: cow probe over budget; omitting cowSupported"
                );
                None
            }
        }
    }
}

/// The blocking `diffSummary` rollup body (runs on the blocking pool): ports
/// the on-demand TS `computeWorkspaceDiffSummary`. Returns `None` when the
/// worktree is not a git repo or there are no changes (matching the TS
/// `undefined` fallback).
fn compute_diff_summary_blocking(worktree: &Path) -> Option<WorkspaceDiffSummary> {
    if !worktree.join(".git").exists() {
        return None;
    }
    let (total_files, total_additions, total_deletions) =
        match intent_git::diff::head_diff_rollup(worktree) {
            Ok(rollup) => rollup,
            Err(e) => {
                tracing::debug!(
                    worktree = %worktree.display(),
                    error = %e,
                    "workspace aggregates: head diff rollup errored; omitting diffSummary"
                );
                return None;
            }
        };
    if total_files == 0 {
        return None;
    }
    Some(WorkspaceDiffSummary {
        schema_version: 1,
        updated_at: now_iso(),
        total_files,
        total_additions,
        total_deletions,
        files: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn init_repo(dir: &Path) {
        let repo = git2::Repository::init(dir).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
    }

    fn commit_all(dir: &Path, message: &str) {
        let repo = git2::Repository::open(dir).unwrap();
        let mut index = repo.index().unwrap();
        index
            .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
            .unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = repo.signature().unwrap();
        let parent = repo
            .head()
            .ok()
            .and_then(|h| h.target())
            .map(|oid| repo.find_commit(oid).unwrap());
        let parents: Vec<&git2::Commit> = parent.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
            .unwrap();
    }

    fn seeded_dirty_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        fs::write(dir.path().join("a.txt"), "one\ntwo\n").unwrap();
        commit_all(dir.path(), "seed");
        fs::write(dir.path().join("a.txt"), "one\nCHANGED\n").unwrap();
        dir
    }

    #[tokio::test]
    async fn diff_summary_serves_cached_value_within_ttl() {
        let dir = seeded_dirty_repo();
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::from_secs(60),
            Duration::from_secs(30),
        ));
        let first = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(first.total_files, 1);

        // A new untracked file is not picked up within the TTL: served from cache.
        fs::write(dir.path().join("b.txt"), "new\n").unwrap();
        let second = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(second.total_files, 1);
    }

    #[tokio::test]
    async fn diff_summary_recomputes_after_ttl_expiry() {
        let dir = seeded_dirty_repo();
        // Zero TTL: every call recomputes.
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::ZERO,
            Duration::from_secs(30),
        ));
        let first = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(first.total_files, 1);

        fs::write(dir.path().join("b.txt"), "new\n").unwrap();
        let second = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(second.total_files, 2);
    }

    /// Claim every rollup permit so an in-flight rollup cannot complete,
    /// forcing the over-budget path deterministically. A zero budget alone is
    /// not enough: `tokio::time::timeout` polls the rollup once per wakeup,
    /// so an ultra-fast diff can occasionally land within "zero" budget.
    async fn hold_all_rollup_permits(
        cache: &WorkspaceAggregateCache,
    ) -> tokio::sync::SemaphorePermit<'_> {
        cache
            .gate
            .acquire_many(MAX_CONCURRENT_ROLLUPS as u32)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn diff_summary_over_budget_omits_then_backfills_cache() {
        let dir = seeded_dirty_repo();
        // Zero budget: the first call times out and omits the aggregate,
        // while the detached rollup completes and fills the cache for later calls.
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::from_secs(60),
            Duration::ZERO,
        ));
        let permits = hold_all_rollup_permits(&cache).await;
        let first = cache.diff_summary("ws-1", dir.path().to_path_buf()).await;
        assert!(first.is_none());
        drop(permits);

        let mut result = None;
        for _ in 0..250 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            result = cache.diff_summary("ws-1", dir.path().to_path_buf()).await;
            if result.is_some() {
                break;
            }
        }
        assert_eq!(result.expect("rollup backfills cache").total_files, 1);
    }

    #[tokio::test]
    async fn diff_summary_over_budget_serves_stale_value() {
        let dir = seeded_dirty_repo();
        // Zero TTL + zero budget: after the cache is backfilled once, a call
        // that sees a stale entry and an over-budget rollup must serve the
        // stale value rather than omit.
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::ZERO,
            Duration::ZERO,
        ));
        let permits = hold_all_rollup_permits(&cache).await;
        assert!(cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .is_none());
        drop(permits);
        let mut backfilled = None;
        for _ in 0..250 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            backfilled = cache.diff_summary("ws-1", dir.path().to_path_buf()).await;
            if backfilled.is_some() {
                break;
            }
        }
        assert_eq!(backfilled.expect("cache backfilled").total_files, 1);

        // Cache primed and rollups blocked: the over-budget call must serve
        // the stale entry.
        let permits = hold_all_rollup_permits(&cache).await;
        let stale = cache.diff_summary("ws-1", dir.path().to_path_buf()).await;
        drop(permits);
        assert_eq!(stale.expect("stale value served").total_files, 1);
    }

    #[tokio::test]
    async fn diff_summary_single_flight_non_winner_serves_last_known() {
        let dir = seeded_dirty_repo();
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::from_secs(60),
            Duration::from_secs(30),
        ));
        let key = dir.path().to_string_lossy().into_owned();
        // Simulate an in-flight rollup: the non-winner must return the last
        // known value (none yet) without waiting on or duplicating the diff.
        let guard = try_begin(&cache.diff_in_flight, key.clone()).expect("claims slot");
        assert!(cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .is_none());
        // Dropping the guard releases the slot (panic-safety path), after
        // which a fresh call computes normally.
        drop(guard);
        assert!(!cache.diff_in_flight.lock().unwrap().contains(&key));
        let summary = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(summary.total_files, 1);
    }

    #[tokio::test]
    async fn diff_summary_non_repo_worktree_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(WorkspaceAggregateCache::new());
        let summary = cache.diff_summary("ws-1", dir.path().to_path_buf()).await;
        assert!(summary.is_none());
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
}
