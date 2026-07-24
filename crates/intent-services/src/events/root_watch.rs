//! Shared root-watch machinery for the skills/specialists watchers (#612).
//!
//! When the intended root exists, a recursive watch is placed on it directly.
//! When it does not (the common case — most workspaces have no tier dirs),
//! the nearest existing ancestor is watched NON-recursively solely to detect
//! the root being created; once it appears the watch is promoted to a
//! recursive watch on the actual root and the ancestor watch is torn down.
//! This avoids parking recursive watches on broad ancestors (workspace root,
//! or even `$HOME`).
//!
//! Event filtering also lives here: an event is forwarded when any of its
//! paths falls under the canonical root and either matches the caller's
//! filename filter or is directory-level (the root itself, an existing
//! directory, or a deleted path), so tier-directory deletions (`rm -rf`) are
//! caught. Callers rely on their fingerprint checks to suppress no-op
//! flushes.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A watch on a single intended root that may not exist yet.
/// Dropping this tears down the watcher and any pending promotion task.
pub(super) struct RootWatch {
    inner: Arc<Mutex<Inner>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct Inner {
    watcher: Option<RecommendedWatcher>,
    watched_path: Option<PathBuf>,
    recursive: bool,
}

impl Drop for RootWatch {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.watcher = None;
        }
    }
}

impl RootWatch {
    /// The path currently being watched and whether the watch is recursive.
    #[cfg(test)]
    pub(super) fn watched(&self) -> Option<(PathBuf, bool)> {
        let inner = self.inner.lock().unwrap();
        inner.watched_path.clone().map(|p| (p, inner.recursive))
    }
}

/// Start watching `root`, invoking `on_change` for matching events.
/// `filename_matches` is the per-watcher file filter (e.g. `SKILL.md`,
/// `*.md`).
pub(super) fn watch_root(
    root: PathBuf,
    filename_matches: fn(&Path) -> bool,
    on_change: impl Fn() + Send + Sync + 'static,
) -> notify::Result<RootWatch> {
    let on_change: Arc<dyn Fn() + Send + Sync> = Arc::new(on_change);
    let inner = Arc::new(Mutex::new(Inner::default()));

    if root.exists() {
        let watcher = recursive_watcher(&root, filename_matches, on_change)?;
        store(&inner, watcher, root, true);
        return Ok(RootWatch { inner, task: None });
    }

    let task = tokio::spawn(promote_loop(
        root,
        filename_matches,
        on_change,
        Arc::clone(&inner),
    ));
    Ok(RootWatch {
        inner,
        task: Some(task),
    })
}

/// Build a recursive watcher on an existing `root` with the event filter.
fn recursive_watcher(
    root: &Path,
    filename_matches: fn(&Path) -> bool,
    on_change: Arc<dyn Fn() + Send + Sync>,
) -> notify::Result<RecommendedWatcher> {
    // Filter against the canonical root: OS watchers (FSEvents in particular)
    // report canonicalized paths, so a symlinked root (e.g. `/var` →
    // `/private/var` on macOS) would otherwise never match.
    let canonical = canonical_root(root, root);
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            if event_matches(&event, &canonical, filename_matches) {
                on_change();
            }
        }
    })?;
    watcher.watch(root, RecursiveMode::Recursive)?;
    Ok(watcher)
}

/// Supervise a missing root: watch the nearest existing ancestor
/// non-recursively until the root (or a nearer ancestor) appears, then
/// promote to a recursive watch on the actual root. Storing the promoted
/// watcher replaces — and thereby tears down — the ancestor watch.
async fn promote_loop(
    root: PathBuf,
    filename_matches: fn(&Path) -> bool,
    on_change: Arc<dyn Fn() + Send + Sync>,
    inner: Arc<Mutex<Inner>>,
) {
    let (wake_tx, mut wake_rx) = mpsc::unbounded_channel::<()>();
    loop {
        if root.exists() {
            if let Ok(watcher) = recursive_watcher(&root, filename_matches, Arc::clone(&on_change))
            {
                store(&inner, watcher, root.clone(), true);
            }
            // Files may have landed inside the root before the recursive
            // watch was established (mkdir -p + immediate writes, or a whole
            // directory renamed into place): flush once and let the caller's
            // fingerprint check decide whether anything actually changed.
            on_change();
            return;
        }

        let ancestor = find_existing_ancestor(&root);
        let tx = wake_tx.clone();
        let watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if res.is_ok() {
                let _ = tx.send(());
            }
        });
        let Ok(mut watcher) = watcher else { return };
        if watcher
            .watch(&ancestor, RecursiveMode::NonRecursive)
            .is_err()
        {
            return;
        }
        store(&inner, watcher, ancestor.clone(), false);

        // Wait until the root or a nearer ancestor appears. Re-check after
        // the watch is established to close the create-before-watch race.
        while !root.exists() && find_existing_ancestor(&root) == ancestor {
            if wake_rx.recv().await.is_none() {
                return;
            }
        }
    }
}

fn store(inner: &Arc<Mutex<Inner>>, watcher: RecommendedWatcher, path: PathBuf, recursive: bool) {
    if let Ok(mut guard) = inner.lock() {
        guard.watcher = Some(watcher);
        guard.watched_path = Some(path);
        guard.recursive = recursive;
    }
}

/// Whether a notify event should be forwarded for the given canonical root.
/// Filename matches under the root always pass; directory-level paths (the
/// root itself, an existing directory, or a deleted path) pass regardless of
/// filename so tier-directory deletions are caught (#612).
fn event_matches(event: &notify::Event, root: &Path, filename_matches: fn(&Path) -> bool) -> bool {
    event
        .paths
        .iter()
        .any(|p| path_within_root(p, root) && (filename_matches(p) || directory_level(p)))
}

/// Directory-level heuristic: an existing directory, or a path that no
/// longer exists (deletions cannot be stat'ed — `rm -rf` of a tier dir may
/// surface only directory paths). Existing non-matching files stay filtered
/// out.
fn directory_level(path: &Path) -> bool {
    match path.symlink_metadata() {
        Ok(meta) => meta.is_dir(),
        Err(_) => true,
    }
}

/// Find the nearest existing ancestor of a path (for non-existent roots).
pub(super) fn find_existing_ancestor(path: &Path) -> PathBuf {
    let mut current = path.to_path_buf();
    while !current.exists() && current.parent().is_some() {
        current = current.parent().unwrap().to_path_buf();
    }
    if current.exists() {
        current
    } else {
        path.to_path_buf()
    }
}

/// Rebase `root` onto the canonicalized form of its nearest existing
/// `ancestor`, so it can be compared against the canonical paths OS watchers
/// report.
pub(super) fn canonical_root(root: &Path, ancestor: &Path) -> PathBuf {
    let canonical_ancestor = ancestor
        .canonicalize()
        .unwrap_or_else(|_| ancestor.to_path_buf());
    match root.strip_prefix(ancestor) {
        Ok(rest) => canonical_ancestor.join(rest),
        Err(_) => root.to_path_buf(),
    }
}

/// Whether an event path falls under the canonical root. `notify` does not
/// guarantee canonical paths across backends, so a raw prefix check is tried
/// first and a best-effort canonicalization of the event path covers
/// symlinked forms. Deleted paths cannot be canonicalized directly; they are
/// rebased onto their nearest existing ancestor instead.
pub(super) fn path_within_root(path: &Path, root: &Path) -> bool {
    if path.starts_with(root) {
        return true;
    }
    let ancestor = find_existing_ancestor(path);
    canonical_root(path, &ancestor).starts_with(root)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    /// Self-cleaning temp directory.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("intentd-root-watch-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn md_only(path: &Path) -> bool {
        path.extension().and_then(|e| e.to_str()) == Some("md")
    }

    async fn wait_for(mut cond: impl FnMut() -> bool, overall: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + overall;
        loop {
            if cond() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[test]
    fn path_within_root_matches_canonical_and_foreign_paths() {
        let dir = TempDir::new("pwr");
        let root = dir.path.canonicalize().expect("canonicalize temp dir");
        std::fs::write(root.join("a.md"), "x").expect("write file");

        assert!(path_within_root(&root.join("a.md"), &root));
        // Deleted files cannot be canonicalized; the ancestor-rebase fallback
        // must still resolve them under the root.
        assert!(path_within_root(&root.join("gone.md"), &root));
        assert!(!path_within_root(Path::new("/elsewhere/a.md"), &root));
    }

    #[cfg(unix)]
    #[test]
    fn path_within_root_resolves_symlinked_event_paths() {
        let dir = TempDir::new("pwr-sym");
        let real = dir.path.join("real");
        std::fs::create_dir_all(&real).expect("mk real dir");
        let root = real.canonicalize().expect("canonicalize real dir");
        std::fs::write(root.join("a.md"), "x").expect("write file");
        let link = dir.path.join("link");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");

        // Non-canonical (symlink) event paths must match the canonical root,
        // whether the file still exists (canonicalize) or was deleted
        // (ancestor rebase).
        assert!(path_within_root(&link.join("a.md"), &root));
        assert!(path_within_root(&link.join("deleted.md"), &root));
    }

    #[tokio::test]
    async fn missing_root_watches_nearest_ancestor_non_recursively() {
        let dir = TempDir::new("miss");
        let root = dir.path.join(".intent").join("specialists");
        let watch = watch_root(root, md_only, || {}).expect("watch root");

        assert!(
            wait_for(|| watch.watched().is_some(), Duration::from_secs(5)).await,
            "ancestor watch must establish"
        );
        let (path, recursive) = watch.watched().expect("watched");
        assert_eq!(path, dir.path, "must watch the nearest existing ancestor");
        assert!(
            !recursive,
            "missing root must not create a recursive watch above the intended root"
        );
    }

    #[tokio::test]
    async fn root_created_later_promotes_and_detects_subsequent_changes() {
        let dir = TempDir::new("promote");
        let root = dir.path.join(".intent").join("specialists");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let watch = watch_root(root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        })
        .expect("watch root");

        assert!(
            wait_for(|| watch.watched().is_some(), Duration::from_secs(5)).await,
            "ancestor watch must establish"
        );

        std::fs::create_dir_all(&root).expect("create root");
        assert!(
            wait_for(
                || watch.watched() == Some((root.clone(), true)),
                Duration::from_secs(10)
            )
            .await,
            "watch must promote to a recursive watch on the created root, got {:?}",
            watch.watched()
        );
        // Promotion fires a catch-up notification for anything created
        // before the recursive watch was established.
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) >= 1, Duration::from_secs(5)).await,
            "promotion must fire a catch-up notification"
        );

        // Let the promoted watch settle, then verify file changes under the
        // new root are detected.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let before = hits.load(Ordering::SeqCst);
        std::fs::write(root.join("new.md"), "x").expect("write md");
        assert!(
            wait_for(
                || hits.load(Ordering::SeqCst) > before,
                Duration::from_secs(10)
            )
            .await,
            "file changes under the promoted root must be detected"
        );
    }

    #[tokio::test]
    async fn directory_only_deletion_is_forwarded() {
        let dir = TempDir::new("rmdir");
        let root = dir.path.join("specialists");
        std::fs::create_dir_all(root.join("nested")).expect("mk root + nested dir");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let _watch = watch_root(root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        })
        .expect("watch root");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // No `.md` file ever exists: `rm -rf` surfaces only directory-level
        // events, which the filter must still forward.
        std::fs::remove_dir_all(&root).expect("remove tier dir");
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > 0, Duration::from_secs(10)).await,
            "tier-directory deletion must forward an event"
        );
    }
}
