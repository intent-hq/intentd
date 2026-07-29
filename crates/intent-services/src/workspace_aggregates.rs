//! Offloaded, cached computation of git-derived workspace aggregates
//! (`diffSummary` for on-demand callers, `cowSupported` for list/get).
//!
//! ## Diff summary is off the high-frequency read path
//!
//! `workspace.list` / `workspace.get` / live-state subscription re-reads used
//! to embed a full `head_diff_rollup` on every workspace. That pinned the
//! blocking pool whenever list polling or `lastActivity` events re-materialized
//! workspace rows. Desktop FE already deprecated `diffSummary` on metadata
//! payloads and fetches it on demand (`GET_DIFF_SUMMARY`). This module still
//! exposes the rollup for optional/on-demand callers, but list/get enrichment
//! no longer attaches it.
//!
//! ## Cache coherency
//!
//! - **Blocking pool + bounded concurrency + single-flight** for rollups.
//! - **Event-driven invalidation** (primary): `spawn_diff_cache_invalidation`
//!   drops cached entries when `file:created` / `file:changed` / `file:deleted`
//!   or `changes:git-status` fires for a workspace. Idle worktrees are never
//!   re-diffed just because the workspace row was re-read.
//! - **Long TTL backstop** only: retained so a missed watcher event eventually
//!   expires; not the primary refresh trigger.
//! - **Per-call budget**: over-budget calls serve last known / omit; detached
//!   work still fills the cache.
//! - **CoW probe cache**: machine capability of the workspaces root; lifetime
//!   cache with single-flight + serialized live probes.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{now_iso, WorkspaceDiffSummary};

/// Minimum freshness window for a completed diff rollup. Card aggregates are
/// advisory (workspace cards, not the Changes panel), so brief staleness is
/// acceptable in exchange for not re-diffing every FE list poll.
const DIFF_SUMMARY_TTL: Duration = Duration::from_secs(60);

/// Multiplier applied to the last observed rollup duration when deriving the
/// adaptive TTL: a rollup that took `d` stays fresh for `d × N`, so a worktree
/// spends at most ~1/N of wall-clock time recomputing its rollup.
const DIFF_TTL_COMPUTE_MULTIPLIER: u32 = 5;

/// Upper bound on the adaptive TTL, so even a pathologically slow rollup is
/// retried within a bounded window.
const DIFF_SUMMARY_TTL_MAX: Duration = Duration::from_secs(300);

/// Adaptive freshness window for a diff cache entry: the last compute duration
/// scaled by [`DIFF_TTL_COMPUTE_MULTIPLIER`], clamped to `base..=max` (a
/// `max` below `base` is treated as `base`). Cheap rollups keep the short
/// base TTL; expensive ones back off proportionally, e.g. a 60 s rollup is
/// re-run at most ~once per 5 min.
fn adaptive_ttl(base: Duration, max: Duration, compute_duration: Duration) -> Duration {
    compute_duration
        .saturating_mul(DIFF_TTL_COMPUTE_MULTIPLIER)
        .clamp(base, max.max(base))
}

/// Wall-clock budget one list/get call spends waiting for a single aggregate
/// before degrading to the last known value / omission.
const AGGREGATE_BUDGET: Duration = Duration::from_millis(1_500);

/// Threshold above which a cache-miss rollup compute logs one warn-level line
/// so slow aggregates on large repos are visible in production (#963).
const SLOW_ROLLUP_WARN_THRESHOLD: Duration = Duration::from_secs(5);

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
    /// How long the rollup took, used to scale this entry's freshness window
    /// (see [`adaptive_ttl`]).
    compute_duration: Duration,
    summary: Option<WorkspaceDiffSummary>,
}

/// Shared cache + offload gates for the git-derived card aggregates. Held as
/// an `Arc` field on `Services` so every clone (and thus every concurrent
/// list/get call) observes the same cache and single-flight state.
// On-demand rollup path is retained for tests and potential future RPC callers;
// list/get enrichment no longer attaches diffSummary (high-frequency re-read).
#[allow(dead_code)]
pub(crate) struct WorkspaceAggregateCache {
    /// Last completed diff rollup per worktree path.
    diff: Mutex<HashMap<String, DiffCacheEntry>>,
    /// workspace_id to worktree cache keys last computed for that workspace.
    /// Lets event-driven invalidation drop entries without knowing the path.
    by_workspace: Mutex<HashMap<String, HashSet<String>>>,
    /// Per-workspace generation bumped on every invalidate. In-flight rollups
    /// capture the epoch at start and only write the cache if it still matches,
    /// so a late completion cannot repopulate a pre-change summary after
    /// invalidation (review on PR 743).
    diff_epoch: Mutex<HashMap<String, u64>>,
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
    /// Minimum (base) diff TTL; entries never expire faster than this.
    ttl: Duration,
    /// Upper clamp on the adaptive diff TTL.
    max_ttl: Duration,
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
        Self::with_timing(DIFF_SUMMARY_TTL, DIFF_SUMMARY_TTL_MAX, AGGREGATE_BUDGET)
    }

    /// Construct with explicit TTL bounds/budget (tests shrink these to
    /// exercise the recompute and over-budget degradation paths
    /// deterministically; passing `max_ttl == ttl` pins a fixed TTL).
    pub(crate) fn with_timing(ttl: Duration, max_ttl: Duration, budget: Duration) -> Self {
        Self {
            diff: Mutex::new(HashMap::new()),
            by_workspace: Mutex::new(HashMap::new()),
            diff_epoch: Mutex::new(HashMap::new()),
            diff_in_flight: Arc::new(Mutex::new(HashSet::new())),
            gate: tokio::sync::Semaphore::new(MAX_CONCURRENT_ROLLUPS),
            cow: Mutex::new(HashMap::new()),
            cow_in_flight: Arc::new(Mutex::new(HashSet::new())),
            cow_probe_gate: tokio::sync::Mutex::new(()),
            ttl,
            max_ttl,
            budget,
        }
    }

    /// Compute (or serve from cache) the `diffSummary` aggregate for a
    /// worktree. Never blocks the async runtime and never waits longer than
    /// the configured budget; on a miss that can't complete in time it returns
    /// the last completed rollup (possibly stale) or `None`.
    #[allow(dead_code)]
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
        let task_workspace_id = workspace_id.to_owned();
        let handle = tokio::spawn(async move {
            let _guard = guard;
            cache
                .rollup_and_store(&task_workspace_id, &task_key, worktree)
                .await
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
    /// `fresh_only` enforces the entry's adaptive TTL; stale entries are
    /// served on the degradation paths above.
    #[allow(dead_code)]
    fn lookup_diff(&self, key: &str, fresh_only: bool) -> Option<Option<WorkspaceDiffSummary>> {
        let map = self.diff.lock().unwrap();
        map.get(key)
            .filter(|e| {
                !fresh_only
                    || e.computed_at.elapsed()
                        < adaptive_ttl(self.ttl, self.max_ttl, e.compute_duration)
            })
            .map(|e| e.summary.clone())
    }

    /// Run one bounded, offloaded rollup and record the result. Failures
    /// (blocking-task panic) are not cached so the next call retries.
    #[allow(dead_code)]
    async fn rollup_and_store(
        &self,
        workspace_id: &str,
        key: &str,
        worktree: PathBuf,
    ) -> Option<WorkspaceDiffSummary> {
        let _permit = self.gate.acquire().await.ok()?;
        // Snapshot the workspace epoch before the expensive work. If
        // invalidate_workspace bumps it while we are computing, the result is
        // returned to the waiter but not cached.
        let epoch = self
            .diff_epoch
            .lock()
            .unwrap()
            .get(workspace_id)
            .copied()
            .unwrap_or(0);
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
        let compute_duration = started.elapsed();
        let ttl = adaptive_ttl(self.ttl, self.max_ttl, compute_duration);
        tracing::debug!(
            worktree = %key,
            files = summary.as_ref().map(|s| s.total_files).unwrap_or(0),
            total_ms = compute_duration.as_millis() as u64,
            ttl_ms = ttl.as_millis() as u64,
            "workspace aggregates: head diff rollup"
        );
        if compute_duration >= SLOW_ROLLUP_WARN_THRESHOLD {
            tracing::warn!(
                workspace_id,
                worktree = %key,
                total_ms = compute_duration.as_millis() as u64,
                ttl_ms = ttl.as_millis() as u64,
                "workspace aggregates: slow diff rollup on cache miss"
            );
        }
        // Drop the write if the workspace was invalidated while we were in flight.
        let current_epoch = self
            .diff_epoch
            .lock()
            .unwrap()
            .get(workspace_id)
            .copied()
            .unwrap_or(0);
        if current_epoch != epoch {
            tracing::debug!(
                workspace_id,
                worktree = %key,
                started_epoch = epoch,
                current_epoch,
                "workspace aggregates: discarding rollup superseded by invalidation"
            );
            return summary;
        }
        self.diff.lock().unwrap().insert(
            key.to_string(),
            DiffCacheEntry {
                computed_at: Instant::now(),
                compute_duration,
                summary: summary.clone(),
            },
        );
        self.by_workspace
            .lock()
            .unwrap()
            .entry(workspace_id.to_string())
            .or_default()
            .insert(key.to_string());
        summary
    }

    /// Drop cached diff summaries for a workspace (event-driven invalidation).
    /// Next on-demand diff_summary call recomputes. No-op when nothing is cached.
    pub(crate) fn invalidate_workspace(&self, workspace_id: &str) {
        // Bump first so any in-flight rollup started at the old epoch will
        // refuse to cache its result after it finishes.
        {
            let mut epochs = self.diff_epoch.lock().unwrap();
            *epochs.entry(workspace_id.to_string()).or_insert(0) += 1;
        }
        let keys = self
            .by_workspace
            .lock()
            .unwrap()
            .remove(workspace_id)
            .unwrap_or_default();
        if !keys.is_empty() {
            let mut map = self.diff.lock().unwrap();
            for key in keys {
                map.remove(&key);
            }
        }
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
            // A fresh configured `workspaces.root` may not exist yet; the
            // probe needs the directory to write its temp file into.
            match tokio::task::spawn_blocking(move || {
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

/// Subscribe to worktree / git events and invalidate the on-demand
/// diffSummary cache. Spawns a background task; safe to call once per
/// Services wiring. No-op if the current thread is not inside a tokio runtime
/// (unit tests that construct Services without a runtime).
pub(crate) fn spawn_diff_cache_invalidation(
    bus: crate::events::EventBus,
    cache: Arc<WorkspaceAggregateCache>,
) {
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => {
            tracing::debug!(
                "workspace aggregates: no tokio runtime; skipping diff cache invalidation subscriber"
            );
            return;
        }
    };
    handle.spawn(async move {
        use crate::events::filter::SubscriptionFilter;
        use intent_core::events::{CHANGES_GIT_STATUS, FILE_CHANGED, FILE_CREATED, FILE_DELETED};

        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![
                FILE_CREATED.to_string(),
                FILE_CHANGED.to_string(),
                FILE_DELETED.to_string(),
                CHANGES_GIT_STATUS.to_string(),
            ],
            ..SubscriptionFilter::default()
        });
        while let Some(batch) = sub.recv().await {
            let mut seen = HashSet::new();
            for ev in batch {
                let id = ev.workspace_id.as_str().to_string();
                if id.is_empty() || !seen.insert(id.clone()) {
                    continue;
                }
                cache.invalidate_workspace(&id);
            }
        }
    });
}

/// The blocking `diffSummary` rollup body (runs on the blocking pool): ports
/// the on-demand TS `computeWorkspaceDiffSummary`. Returns `None` when the
/// worktree is not a git repo or there are no changes (matching the TS
/// `undefined` fallback).
#[allow(dead_code)]
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
        // Zero TTL (max_ttl == ttl pins it): every call recomputes.
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::ZERO,
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

    #[test]
    fn adaptive_ttl_scales_with_compute_duration_and_clamps() {
        let base = Duration::from_secs(5);
        let max = Duration::from_secs(300);
        // Cheap rollups keep the base TTL.
        assert_eq!(adaptive_ttl(base, max, Duration::ZERO), base);
        assert_eq!(adaptive_ttl(base, max, Duration::from_millis(200)), base);
        // Past the base threshold the TTL scales linearly with compute time.
        assert_eq!(
            adaptive_ttl(base, max, Duration::from_secs(2)),
            Duration::from_secs(10)
        );
        assert_eq!(
            adaptive_ttl(base, max, Duration::from_secs(30)),
            Duration::from_secs(150)
        );
        // A 60 s rollup hits the 5 min cap: re-run at most ~once per 5 min.
        assert_eq!(adaptive_ttl(base, max, Duration::from_secs(60)), max);
        assert_eq!(adaptive_ttl(base, max, Duration::from_secs(3_600)), max);
        // A max below base degenerates to a fixed base TTL instead of
        // panicking in `clamp`.
        assert_eq!(
            adaptive_ttl(base, Duration::ZERO, Duration::from_secs(60)),
            base
        );
    }

    /// Freshness is per-entry: with a zero base TTL, an entry whose rollup was
    /// slow stays fresh (scaled TTL) while a fast entry expires immediately.
    #[test]
    fn lookup_diff_freshness_scales_with_entry_compute_duration() {
        let cache = WorkspaceAggregateCache::with_timing(
            Duration::ZERO,
            Duration::from_secs(300),
            Duration::from_secs(30),
        );
        let entry = |compute_duration| DiffCacheEntry {
            computed_at: Instant::now(),
            compute_duration,
            summary: None,
        };
        {
            let mut map = cache.diff.lock().unwrap();
            map.insert("slow".into(), entry(Duration::from_secs(60)));
            map.insert("fast".into(), entry(Duration::ZERO));
        }
        assert!(cache.lookup_diff("slow", true).is_some());
        assert!(cache.lookup_diff("fast", true).is_none());
        // Stale lookups still serve both entries.
        assert!(cache.lookup_diff("fast", false).is_some());
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
    async fn invalidate_workspace_forces_recompute() {
        let dir = seeded_dirty_repo();
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ));
        let first = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(first.total_files, 1);

        fs::write(dir.path().join("b.txt"), "new\n").unwrap();
        let cached = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(cached.total_files, 1);

        cache.invalidate_workspace("ws-1");
        let second = cache
            .diff_summary("ws-1", dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(second.total_files, 2);
    }

    #[tokio::test]
    async fn in_flight_rollup_does_not_repopulate_after_invalidate() {
        let dir = seeded_dirty_repo();
        // Long TTL so a wrong cached write would be served on the next lookup.
        let cache = Arc::new(WorkspaceAggregateCache::with_timing(
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ));
        let worktree = dir.path().to_path_buf();

        // Hold all rollup permits so the background rollup blocks on the gate
        // before computing (and before it can observe b.txt).
        let permits = hold_all_rollup_permits(&cache).await;

        let cache2 = Arc::clone(&cache);
        let worktree2 = worktree.clone();
        let join = tokio::spawn(async move { cache2.diff_summary("ws-1", worktree2).await });
        // Let the task claim single-flight and block on the gate.
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Tree change + invalidation while the rollup is still gated.
        fs::write(dir.path().join("b.txt"), "new\n").unwrap();
        cache.invalidate_workspace("ws-1");

        drop(permits);
        let _from_inflight = join.await.unwrap();

        // A subsequent call must recompute. If the superseded in-flight write
        // had been accepted, long TTL would serve total_files=1 forever.
        let after = cache
            .diff_summary("ws-1", worktree)
            .await
            .expect("fresh compute after invalidate");
        assert_eq!(
            after.total_files, 2,
            "stale in-flight rollup must not repopulate cache"
        );
    }
}
