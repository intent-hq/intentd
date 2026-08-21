//! Offloaded, cached computation of a workspace directory's disk footprint
//! (the on-demand `workspace.diskUsage` method — never the list/get path).
//!
//! Scans the whole per-workspace folder (`<workspaces_root>/<workspaceId>`:
//! repo checkout, tool-outputs, agent sandboxes, everything) and reports
//! **physical (allocated) usage** — the sum of `st_blocks * 512` — not
//! apparent size, so sparse regions are excluded. Hard links are deduped by
//! `(st_dev, st_ino)` within a single walk, so a file linked from several
//! places (e.g. a git alternates-style layout) is counted once. Symbolic
//! links are never followed (only the link's own allocation counts) and
//! directory-inode allocation is excluded.
//!
//! On non-Unix targets (Windows) there is no `st_blocks`/`st_ino` in the
//! portable metadata surface, so the walk falls back to **logical size**
//! (`Metadata::len`) with no hard-link dedup — a documented best-effort
//! approximation on those platforms.
//!
//! ## CoW-clone limitation (best effort)
//!
//! Clone-shared extents (APFS `clonefile`, btrfs/XFS reflink) are counted at
//! full allocated size in every workspace that references them. Excluding
//! them would require a per-file extent enumeration — FIEMAP +
//! `FIEMAP_EXTENT_SHARED` on Linux, `fcntl(F_LOG2PHYS_EXT)` physical-extent
//! dedup on macOS (no public sharing flag exists) — which multiplies the walk
//! cost by a syscall per file extent and blows the refresh budget on real
//! checkouts. The number is therefore an upper bound for `CoW` workspaces;
//! client copy already presents it as approximate.
//!
//! ## Cache semantics
//!
//! Per-workspace-dir entries with a ~60s TTL. [`DiskUsageCache::poll`]
//! returns `(usage, refreshing)`: a fresh entry is returned as-is with
//! `refreshing: false`; an expired entry is returned immediately while a
//! background recompute refreshes it (stale-while-revalidate); the
//! first-ever computation returns `None` and backfills for a later poll —
//! in both non-fresh cases `refreshing` is `true` (a walk is in flight or
//! was just armed by the call). Refreshes are single-flight per directory
//! and run the walk on the blocking pool, with walks across directories
//! globally serialized; a failed walk keeps the last-good entry (retry on
//! the next poll) and a missing directory simply never produces an entry.

use std::collections::{HashMap, HashSet};
use std::fs::Metadata;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{DiskUsageBreakdownEntry, WorkspaceDiskUsage};
use tokio::sync::Semaphore;

use crate::workspace_aggregates::try_begin;

/// How long a computed entry is served without triggering a refresh.
const DISK_USAGE_TTL: Duration = Duration::from_secs(60);

/// Sequential walks are sufficient because disk usage is stale-while-revalidate
/// and first paint omits it; concurrent full-tree walks only create disk contention.
const MAX_CONCURRENT_DISK_USAGE_WALKS: usize = 1;

/// Name grouping loose top-level files (and unfollowed symlinks).
const OTHER_BUCKET: &str = "other";

struct CacheEntry {
    usage: WorkspaceDiskUsage,
    refreshed_at: Instant,
}

/// Shared cache + single-flight state for per-workspace disk usage. Held as
/// an `Arc` so every clone of the owning service observes the same entries.
pub(crate) struct DiskUsageCache {
    entries: Mutex<HashMap<PathBuf, CacheEntry>>,
    in_flight: Arc<Mutex<HashSet<PathBuf>>>,
    walk_permits: Arc<Semaphore>,
    ttl: Duration,
    #[cfg(test)]
    walk_probe: Option<Arc<WalkProbe>>,
}

impl DiskUsageCache {
    pub(crate) fn new() -> Self {
        Self::with_ttl(DISK_USAGE_TTL)
    }

    fn with_ttl(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            walk_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_DISK_USAGE_WALKS)),
            ttl,
            #[cfg(test)]
            walk_probe: None,
        }
    }

    #[cfg(test)]
    fn with_probe(ttl: Duration, walk_probe: Arc<WalkProbe>) -> Self {
        Self {
            walk_probe: Some(walk_probe),
            ..Self::with_ttl(ttl)
        }
    }

    /// Serve the cached usage for `workspace_dir` under the module's cache
    /// semantics, returning `(usage, refreshing)`: fresh → `(cached, false)`;
    /// stale → `(cached, true)` while a background walk revalidates; absent
    /// → `(None, true)` while the first walk backfills. `refreshing` is
    /// `true` iff a walk for this directory is in flight or was just armed
    /// by this call.
    pub(crate) async fn poll(
        self: &Arc<Self>,
        workspace_dir: PathBuf,
    ) -> (Option<WorkspaceDiskUsage>, bool) {
        let (cached, fresh) = {
            let entries = self.entries.lock().unwrap();
            match entries.get(&workspace_dir) {
                Some(e) => (Some(e.usage.clone()), e.refreshed_at.elapsed() < self.ttl),
                None => (None, false),
            }
        };
        if fresh {
            return (cached, false);
        }
        // Single-flight per directory: while a walk is in flight, concurrent
        // callers keep serving the stale value (or omission) without queueing
        // duplicate walks.
        if let Some(guard) = try_begin(&self.in_flight, workspace_dir.clone()) {
            let cache = Arc::clone(self);
            tokio::spawn(async move {
                let _in_flight = guard;
                let walk_permit = Arc::clone(&cache.walk_permits)
                    .acquire_owned()
                    .await
                    .expect("disk usage walk semaphore is never closed");
                let dir = workspace_dir.clone();
                let started = Instant::now();
                #[cfg(test)]
                let walk_probe = cache.walk_probe.clone();
                let result = tokio::task::spawn_blocking(move || {
                    #[cfg(test)]
                    let _probe_guard = walk_probe.as_ref().map(|probe| probe.enter());
                    compute_dir_usage(&dir)
                })
                .await;
                drop(walk_permit);
                match result {
                    Ok(Ok(usage)) => {
                        tracing::debug!(
                            workspace_dir = %workspace_dir.display(),
                            bytes = usage.bytes,
                            files = usage.file_count,
                            total_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                            "disk usage: walk completed"
                        );
                        cache.entries.lock().unwrap().insert(
                            workspace_dir,
                            CacheEntry {
                                usage,
                                refreshed_at: Instant::now(),
                            },
                        );
                    }
                    Ok(Err(e)) => {
                        // Last-good entry (if any) is retained; next poll retries.
                        tracing::debug!(
                            workspace_dir = %workspace_dir.display(),
                            error = %e,
                            "disk usage: walk failed; keeping last-good value"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            workspace_dir = %workspace_dir.display(),
                            error = %e,
                            "disk usage: blocking walk task failed"
                        );
                    }
                }
            });
        }
        // Not fresh ⇒ either this call just armed the walk above or one was
        // already in flight (`try_begin` refused), so a refresh is running.
        (cached, true)
    }
}

#[cfg(test)]
struct WalkProbe {
    current: std::sync::atomic::AtomicUsize,
    max: std::sync::atomic::AtomicUsize,
    delay: Duration,
}

#[cfg(test)]
impl WalkProbe {
    fn new(delay: Duration) -> Self {
        Self {
            current: std::sync::atomic::AtomicUsize::new(0),
            max: std::sync::atomic::AtomicUsize::new(0),
            delay,
        }
    }

    fn enter(&self) -> WalkProbeGuard<'_> {
        use std::sync::atomic::Ordering;

        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        WalkProbeGuard { probe: self }
    }
}

#[cfg(test)]
struct WalkProbeGuard<'a> {
    probe: &'a WalkProbe,
}

#[cfg(test)]
impl Drop for WalkProbeGuard<'_> {
    fn drop(&mut self) {
        self.probe
            .current
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Running totals for one breakdown bucket.
#[derive(Default)]
struct Tally {
    bytes: u64,
    file_count: u64,
}

/// Walk a workspace directory and total its physical usage. Fails only when
/// the root itself is unreadable (e.g. missing directory → the caller omits
/// the aggregate); errors below the root are skipped best-effort so a
/// permission-denied sandbox subtree can't kill the whole walk.
fn compute_dir_usage(root: &Path) -> io::Result<WorkspaceDiskUsage> {
    // Hard-link dedup across the whole walk: a multi-linked inode counts
    // toward whichever top-level bucket encounters it first.
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let mut buckets: Vec<(String, Tally)> = Vec::new();
    let mut other = Tally::default();
    for entry in std::fs::read_dir(root)? {
        let Ok(entry) = entry else { continue };
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            let mut tally = Tally::default();
            walk_dir(&entry.path(), &mut seen, &mut tally);
            buckets.push((entry.file_name().to_string_lossy().into_owned(), tally));
        } else {
            tally_entry(&meta, &mut seen, &mut other);
        }
    }
    if other.bytes > 0 || other.file_count > 0 {
        buckets.push((OTHER_BUCKET.to_string(), other));
    }
    buckets.sort_by(|(an, a), (bn, b)| b.bytes.cmp(&a.bytes).then_with(|| an.cmp(bn)));
    let bytes = buckets.iter().map(|(_, t)| t.bytes).sum();
    let file_count = buckets.iter().map(|(_, t)| t.file_count).sum();
    Ok(WorkspaceDiskUsage {
        bytes,
        file_count,
        breakdown: buckets
            .into_iter()
            .map(|(name, t)| DiskUsageBreakdownEntry {
                name,
                bytes: t.bytes,
                file_count: t.file_count,
            })
            .collect(),
        computed_at: intent_core::now_iso(),
    })
}

/// Recurse into `dir`, accumulating into `tally`. Unreadable entries are
/// skipped (best effort); symlinks are not followed.
fn walk_dir(dir: &Path, seen: &mut HashSet<(u64, u64)>, tally: &mut Tally) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let Ok(entry) = entry else { continue };
        // `DirEntry::metadata` does not traverse symlinks.
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            walk_dir(&entry.path(), seen, tally);
        } else {
            tally_entry(&meta, seen, tally);
        }
    }
}

/// Account one non-directory entry: allocated blocks for everything, file
/// count for regular files only, multi-linked inodes counted once.
#[cfg(unix)]
fn tally_entry(meta: &Metadata, seen: &mut HashSet<(u64, u64)>, tally: &mut Tally) {
    if meta.nlink() > 1 && !seen.insert((meta.dev(), meta.ino())) {
        return;
    }
    tally.bytes += meta.blocks() * 512;
    if meta.is_file() {
        tally.file_count += 1;
    }
}

/// Non-Unix fallback (see module docs): logical size, no hard-link dedup.
#[cfg(not(unix))]
fn tally_entry(meta: &Metadata, _seen: &mut HashSet<(u64, u64)>, tally: &mut Tally) {
    tally.bytes += meta.len();
    if meta.is_file() {
        tally.file_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn write_file(path: &Path, len: usize) {
        let mut f = fs::File::create(path).unwrap();
        f.write_all(&vec![0xA5u8; len]).unwrap();
    }

    #[cfg(unix)]
    fn physical(path: &Path) -> u64 {
        fs::metadata(path).unwrap().blocks() * 512
    }

    /// Poll until the cache backfills an entry for `dir`.
    async fn poll_until_some(cache: &Arc<DiskUsageCache>, dir: &Path) -> WorkspaceDiskUsage {
        for _ in 0..200 {
            if let (Some(u), _) = cache.poll(dir.to_path_buf()).await {
                return u;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("disk usage never backfilled for {}", dir.display());
    }

    /// Wait for any in-flight walk to finish so the cached value is settled.
    async fn drain_in_flight(cache: &Arc<DiskUsageCache>) {
        for _ in 0..200 {
            if cache.in_flight.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("in-flight walk never drained");
    }

    #[cfg(unix)]
    #[test]
    fn sums_physical_bytes_and_dedupes_hard_links() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("data.bin");
        write_file(&file, 1 << 20);
        fs::hard_link(&file, dir.path().join("link.bin")).unwrap();
        let usage = compute_dir_usage(dir.path()).unwrap();
        assert_eq!(usage.bytes, physical(&file), "hard link counted once");
        assert_eq!(usage.file_count, 1);
    }

    #[cfg(unix)]
    #[test]
    fn sparse_files_count_allocated_not_apparent_size() {
        const APPARENT: u64 = 16 << 20;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sparse.bin");
        fs::File::create(&file).unwrap().set_len(APPARENT).unwrap();
        let usage = compute_dir_usage(dir.path()).unwrap();
        assert!(
            usage.bytes < APPARENT,
            "allocated ({}) should be well under apparent size ({APPARENT})",
            usage.bytes
        );
    }

    #[test]
    fn breakdown_per_top_level_entry_sorted_desc_with_other_bucket() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("repo")).unwrap();
        write_file(&dir.path().join("repo").join("big.bin"), 3 << 20);
        fs::create_dir_all(dir.path().join("tool-outputs").join("nested")).unwrap();
        write_file(
            &dir.path()
                .join("tool-outputs")
                .join("nested")
                .join("small.bin"),
            1 << 20,
        );
        write_file(&dir.path().join("loose.txt"), 4096);
        let usage = compute_dir_usage(dir.path()).unwrap();
        let names: Vec<&str> = usage.breakdown.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["repo", "tool-outputs", "other"]);
        assert_eq!(
            usage.bytes,
            usage.breakdown.iter().map(|e| e.bytes).sum::<u64>()
        );
        assert_eq!(
            usage.file_count,
            usage.breakdown.iter().map(|e| e.file_count).sum::<u64>()
        );
        assert_eq!(usage.file_count, 3);
    }

    #[test]
    fn missing_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(compute_dir_usage(&dir.path().join("nope")).is_err());
    }

    /// First-ever computation omits with `refreshing: true`; the detached
    /// walk backfills the cache and a settled fresh entry reads
    /// `refreshing: false`.
    #[tokio::test]
    async fn first_call_omits_then_backfills() {
        let dir = tempfile::tempdir().unwrap();
        write_file(&dir.path().join("f.bin"), 8192);
        let cache = Arc::new(DiskUsageCache::new());
        let (usage, refreshing) = cache.poll(dir.path().to_path_buf()).await;
        assert!(usage.is_none());
        assert!(refreshing, "first call arms the walk");
        let usage = poll_until_some(&cache, dir.path()).await;
        assert!(usage.bytes > 0);
        assert_eq!(usage.file_count, 1);
        drain_in_flight(&cache).await;
        let (usage, refreshing) = cache.poll(dir.path().to_path_buf()).await;
        assert!(usage.is_some());
        assert!(!refreshing, "settled fresh entry is not refreshing");
    }

    /// A fresh entry is served as-is: no recompute inside the TTL.
    #[tokio::test]
    async fn fresh_entry_served_without_recompute() {
        let dir = tempfile::tempdir().unwrap();
        write_file(&dir.path().join("f.bin"), 8192);
        let cache = Arc::new(DiskUsageCache::with_ttl(Duration::from_secs(3600)));
        let first = poll_until_some(&cache, dir.path()).await;
        write_file(&dir.path().join("g.bin"), 1 << 20);
        let (served, refreshing) = cache.poll(dir.path().to_path_buf()).await;
        assert_eq!(
            served.as_ref(),
            Some(&first),
            "fresh cache ignores new file"
        );
        assert!(!refreshing, "fresh entry never arms a walk");
        assert!(
            cache.in_flight.lock().unwrap().is_empty(),
            "no refresh spawned"
        );
    }

    /// An expired entry is returned immediately (with `refreshing: true`)
    /// while the background walk refreshes it for later calls.
    #[tokio::test]
    async fn stale_entry_served_while_revalidating() {
        let dir = tempfile::tempdir().unwrap();
        write_file(&dir.path().join("f.bin"), 8192);
        let cache = Arc::new(DiskUsageCache::with_ttl(Duration::ZERO));
        let old = poll_until_some(&cache, dir.path()).await;
        drain_in_flight(&cache).await;
        write_file(&dir.path().join("g.bin"), 1 << 20);
        let (served, refreshing) = cache.poll(dir.path().to_path_buf()).await;
        assert_eq!(
            served.unwrap().bytes,
            old.bytes,
            "stale value served immediately"
        );
        assert!(refreshing, "stale entry reports the armed revalidation");
        for _ in 0..200 {
            if let (Some(u), _) = cache.poll(dir.path().to_path_buf()).await {
                if u.bytes > old.bytes {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("revalidation never picked up the new file");
    }

    /// While a walk is claimed for a directory, callers don't spawn another
    /// — but still observe `refreshing: true` for the in-flight walk.
    #[tokio::test]
    async fn single_flight_coalesces_concurrent_calls() {
        let dir = tempfile::tempdir().unwrap();
        write_file(&dir.path().join("f.bin"), 8192);
        let cache = Arc::new(DiskUsageCache::new());
        let guard = try_begin(&cache.in_flight, dir.path().to_path_buf()).unwrap();
        let (usage, refreshing) = cache.poll(dir.path().to_path_buf()).await;
        assert!(usage.is_none());
        assert!(refreshing, "in-flight walk reports refreshing");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            cache.entries.lock().unwrap().is_empty(),
            "no walk ran while the slot was held"
        );
        drop(guard);
        poll_until_some(&cache, dir.path()).await;
    }

    /// Cold misses for distinct directories share the global walk permit.
    #[tokio::test]
    async fn concurrent_cold_walks_are_globally_serialized() {
        const DIR_COUNT: usize = 8;

        let root = tempfile::tempdir().unwrap();
        let probe = Arc::new(WalkProbe::new(Duration::from_millis(50)));
        let cache = Arc::new(DiskUsageCache::with_probe(
            Duration::from_secs(3600),
            Arc::clone(&probe),
        ));
        for index in 0..DIR_COUNT {
            let dir = root.path().join(format!("ws-{index}"));
            fs::create_dir(&dir).unwrap();
            write_file(&dir.join("f.bin"), 8192);
            assert!(cache.poll(dir).await.0.is_none());
        }

        assert_eq!(cache.in_flight.lock().unwrap().len(), DIR_COUNT);
        drain_in_flight(&cache).await;
        assert_eq!(
            probe.max.load(std::sync::atomic::Ordering::SeqCst),
            MAX_CONCURRENT_DISK_USAGE_WALKS
        );
        assert_eq!(cache.entries.lock().unwrap().len(), DIR_COUNT);
    }

    /// A failed walk keeps the last-good entry (missing dir after compute).
    #[tokio::test]
    async fn failed_walk_keeps_last_good() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("ws");
        fs::create_dir(&ws).unwrap();
        write_file(&ws.join("f.bin"), 8192);
        let cache = Arc::new(DiskUsageCache::with_ttl(Duration::ZERO));
        poll_until_some(&cache, &ws).await;
        drain_in_flight(&cache).await;
        // Baseline from the settled entry: zero-TTL polling above may have
        // refreshed past the first returned value (later `computed_at`).
        let old = cache
            .entries
            .lock()
            .unwrap()
            .get(&ws)
            .unwrap()
            .usage
            .clone();
        fs::remove_dir_all(&ws).unwrap();
        let served = cache.poll(ws.clone()).await.0.unwrap();
        assert_eq!(served, old);
        drain_in_flight(&cache).await;
        let again = cache.poll(ws.clone()).await.0.unwrap();
        assert_eq!(again, old, "failed refresh retained last-good value");
    }

    /// A never-existing directory omits forever without caching anything.
    #[tokio::test]
    async fn missing_directory_omits_without_error() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path().join("never-created");
        let cache = Arc::new(DiskUsageCache::new());
        assert!(cache.poll(ws.clone()).await.0.is_none());
        drain_in_flight(&cache).await;
        assert!(cache.poll(ws.clone()).await.0.is_none());
        assert!(cache.entries.lock().unwrap().is_empty());
    }
}
