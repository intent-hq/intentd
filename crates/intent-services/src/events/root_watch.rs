//! Shared root-watch machinery for the skills/specialists user tiers (#612).
//!
//! When the intended root exists, a recursive watch is placed on it directly.
//! When it does not (the common case — most hosts have only some of the
//! user-tier dirs), the nearest existing ancestor is watched NON-recursively
//! solely to detect the root being created; once it appears the watch is
//! promoted to a recursive watch on the actual root and the ancestor watch is
//! released. This avoids parking recursive watches on broad ancestors (or even
//! `$HOME`).
//!
//! Both watches ride the daemon's [`SharedWatchHub`] stream rather than owning
//! a `notify` watcher each (intent-hq/intent#4953): on Linux every watcher is
//! one inotify instance against `fs.inotify.max_user_instances`, and the five
//! user-tier roots alone used to cost an idle daemon five of them. Riding the
//! hub also inherits its deferred, off-thread registration — the OS-level
//! `notify` call can block indefinitely (macOS `FSEvents`,
//! intent-hq/monorepo#1572), which once stalled daemon startup before the UDS
//! socket was bound. Failures are logged rather than returned, then retried
//! with capped backoff (see [`watch_loop`]).
//!
//! Event filtering also lives here: an event is forwarded when any of its
//! paths falls under the canonical root and either matches the caller's
//! filename filter or is directory-level (the root itself, an existing
//! directory, or a deleted path), so tier-directory deletions (`rm -rf`) are
//! caught. Callers rely on their fingerprint checks to suppress no-op
//! flushes.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use notify::RecursiveMode;
use tokio::task::JoinHandle;

use super::shared_watch::{
    os_watch_limits, SharedWatchHub, SubHandle, CREATE_RETRY_CAP, CREATE_RETRY_INITIAL,
};

/// A watch on a single intended root that may not exist yet.
/// Dropping this releases the subscription and any pending promotion task.
pub(super) struct RootWatch {
    inner: Arc<Mutex<Inner>>,
    task: Option<JoinHandle<()>>,
    #[cfg(test)]
    root: PathBuf,
}

#[derive(Default)]
struct Inner {
    sub: Option<SubHandle>,
    watched_path: Option<PathBuf>,
    recursive: bool,
}

impl Drop for RootWatch {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.sub = None;
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

    /// Await the deferred registration landing. Tests that mutate the
    /// filesystem must wait for this instead of a fixed warm-up sleep, since
    /// registration no longer completes before [`watch_root`] returns.
    ///
    /// "Established" means *some* watch is in place: for a missing root that
    /// is the ancestor watch (the correct sync point for creation detection),
    /// not the recursive watch on the intended root, which only exists after
    /// promotion. Panics on timeout so a wedged registration is diagnosed
    /// here rather than as a downstream "no event" failure — and immediately
    /// once the watch loop has ended without storing a watch (a registration
    /// thread that never reported), since nothing will establish it later.
    /// A registration that merely *failed* is retried by the loop
    /// (intent-hq/intent#4852), so that case waits, up to `timeout` — as
    /// does a live watch the loop lost and is re-registering, during which
    /// `watched()` reads `None` again.
    ///
    /// The loop only ends when the watch is dropped, so a finished task is
    /// only a failure if `watched()` is still `None` when observed after
    /// `is_finished()` — the store happens-before the task ends.
    #[cfg(test)]
    pub(super) async fn wait_established(&self, timeout: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.watched().is_none() {
            if self.task.as_ref().is_some_and(JoinHandle::is_finished) {
                assert!(
                    self.watched().is_some(),
                    "watch loop for {} ended without establishing a watch (registration failed; see WARN logs); {}",
                    self.root.display(),
                    os_watch_limits()
                );
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "watch registration for {} did not establish within {timeout:?}; {}",
                self.root.display(),
                os_watch_limits()
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}

/// Start watching `root` over `hub`, invoking `on_change` for matching
/// events. `filename_matches` is the per-watcher file filter (e.g. `SKILL.md`,
/// `*.md`).
///
/// Registration is always deferred to a spawned task (and, inside the hub, to
/// its registrar thread), so a `notify` backend that blocks on registration
/// (macOS `FSEvents`, intent-hq/monorepo#1572) cannot stall the caller. A
/// registration failure is logged and retried, not returned.
pub(super) fn watch_root(
    hub: &Arc<SharedWatchHub>,
    root: PathBuf,
    filename_matches: fn(&Path) -> bool,
    on_change: impl Fn() + Send + Sync + 'static,
) -> RootWatch {
    let inner = Arc::new(Mutex::new(Inner::default()));
    #[cfg(test)]
    let intended_root = root.clone();
    let task = tokio::spawn(watch_loop(
        Arc::clone(hub),
        root,
        filename_matches,
        on_change,
        Arc::clone(&inner),
    ));
    RootWatch {
        inner,
        task: Some(task),
        #[cfg(test)]
        root: intended_root,
    }
}

/// Establish the watch off the caller's thread and then forward its events.
///
/// An existing root gets its recursive subscription directly; a missing one
/// enters ancestor supervision: the nearest existing ancestor is subscribed
/// non-recursively until the root (or a nearer ancestor) appears, then the
/// loop comes back around and promotes to a recursive subscription on the
/// actual root. Storing the promoted subscription replaces — and thereby
/// releases — the ancestor one.
///
/// A failed registration (recursive or ancestor) is retried with the same
/// capped exponential backoff the shared hub uses for watcher creation
/// (intent-hq/intent#3708) rather than abandoning the root: the dominant
/// failure is transient — inotify instance exhaustion, `EMFILE` — and a
/// watch given up on there stays dead for the process lifetime with only a
/// WARN to show for it (intent-hq/intent#4852). The failed subscription is
/// dropped first so the next attempt registers the root afresh, and a stored
/// ancestor watch stays live across the retries. Dropping the [`RootWatch`]
/// aborts the loop, retries included.
///
/// Ancestor events are level-triggered wake hints — the loop re-reads the
/// filesystem after each — and the ancestor receiver lives only for the wait
/// it drives, so traffic while a recursive registration keeps failing is
/// dropped at the hub rather than queued for the watch's lifetime.
///
/// A live subscription can still be lost afterwards: the hub re-registers a
/// root when a recursive co-tenant above it retires (its unwatch strips the
/// nested descriptors) and closes the root's channels if that re-registration
/// fails. The loop treats a closed receiver — recursive or ancestor — as such
/// a loss: it forgets the dead subscription and starts over with the same
/// backoff, rather than parking on the closed channel with a watch that
/// looks established and delivers nothing.
async fn watch_loop(
    hub: Arc<SharedWatchHub>,
    root: PathBuf,
    filename_matches: fn(&Path) -> bool,
    on_change: impl Fn() + Send + Sync + 'static,
    inner: Arc<Mutex<Inner>>,
) {
    let mut backoff = CREATE_RETRY_INITIAL;
    loop {
        if root.exists() {
            let (sub, mut rx, canonical) = hub.subscribe(&root);
            if sub.wait_live().await {
                store(&inner, sub, root.clone(), true);
                backoff = CREATE_RETRY_INITIAL;
                // Registration is deferred, so changes can land between
                // `watch_root` returning and the watch existing — and callers
                // prime their fingerprint before that. Flush once so such a
                // change is not absorbed as pre-existing (the same catch-up
                // covers files that landed inside a just-promoted root); the
                // fingerprint check suppresses the no-op case.
                on_change();
                while let Some(event) = rx.recv().await {
                    if event_matches(&event, &canonical, filename_matches) {
                        on_change();
                    }
                }
                forget(&inner);
                tracing::warn!(
                    root = %root.display(),
                    retry_in = ?backoff,
                    os_watch_limits = %os_watch_limits(),
                    "recursive watch lost; re-registering"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CREATE_RETRY_CAP);
                continue;
            }
            // Includes the root being deleted between the `exists` check and
            // the registrar's `watch()`: the retry sees it missing and falls
            // through to ancestor supervision.
            drop(sub);
            tracing::warn!(
                root = %root.display(),
                retry_in = ?backoff,
                os_watch_limits = %os_watch_limits(),
                "recursive watch on existing root failed; retrying"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(CREATE_RETRY_CAP);
            continue;
        }

        let ancestor = find_existing_ancestor(&root);
        let (sub, mut rx, _) = hub.subscribe_with(&ancestor, RecursiveMode::NonRecursive);
        if !sub.wait_live().await {
            drop(sub);
            tracing::warn!(
                root = %root.display(),
                ancestor = %ancestor.display(),
                retry_in = ?backoff,
                os_watch_limits = %os_watch_limits(),
                "ancestor watch failed; retrying"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(CREATE_RETRY_CAP);
            continue;
        }
        store(&inner, sub, ancestor.clone(), false);
        backoff = CREATE_RETRY_INITIAL;

        // Wait until the root or a nearer ancestor appears. Re-check after
        // the watch is established to close the create-before-watch race.
        let lost = loop {
            if root.exists() || find_existing_ancestor(&root) != ancestor {
                break false;
            }
            if rx.recv().await.is_none() {
                break true;
            }
        };
        if lost {
            forget(&inner);
            tracing::warn!(
                root = %root.display(),
                ancestor = %ancestor.display(),
                retry_in = ?backoff,
                os_watch_limits = %os_watch_limits(),
                "ancestor watch lost; re-registering"
            );
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(CREATE_RETRY_CAP);
        }
    }
}

fn store(inner: &Arc<Mutex<Inner>>, sub: SubHandle, path: PathBuf, recursive: bool) {
    if let Ok(mut guard) = inner.lock() {
        guard.sub = Some(sub);
        guard.watched_path = Some(path);
        guard.recursive = recursive;
    }
}

/// Release a lost subscription so the next attempt registers afresh, and stop
/// reporting it as the watched path meanwhile.
fn forget(inner: &Arc<Mutex<Inner>>) {
    if let Ok(mut guard) = inner.lock() {
        guard.sub = None;
        guard.watched_path = None;
        guard.recursive = false;
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
pub(super) fn directory_level(path: &Path) -> bool {
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
    use crate::events::LIVENESS;

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
    #[expect(clippy::await_holding_lock)]
    async fn missing_root_watches_nearest_ancestor_non_recursively() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("miss");
        let root = dir.path.join(".intent").join("specialists");
        let hub = SharedWatchHub::new();
        let watch = watch_root(&hub, root, md_only, || {});

        assert!(
            wait_for(|| watch.watched().is_some(), LIVENESS).await,
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
    #[expect(clippy::await_holding_lock)]
    async fn root_created_later_promotes_and_detects_subsequent_changes() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("promote");
        let root = dir.path.join(".intent").join("specialists");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let hub = SharedWatchHub::new();
        let watch = watch_root(&hub, root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        });

        assert!(
            wait_for(|| watch.watched().is_some(), LIVENESS).await,
            "ancestor watch must establish"
        );

        std::fs::create_dir_all(&root).expect("create root");
        assert!(
            wait_for(|| watch.watched() == Some((root.clone(), true)), LIVENESS).await,
            "watch must promote to a recursive watch on the created root, got {:?}",
            watch.watched()
        );
        // Promotion fires a catch-up notification for anything created
        // before the recursive watch was established.
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) >= 1, LIVENESS).await,
            "promotion must fire a catch-up notification"
        );

        // Let the promoted watch settle, then verify file changes under the
        // new root are detected.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let before = hits.load(Ordering::SeqCst);
        std::fs::write(root.join("new.md"), "x").expect("write md");
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > before, LIVENESS).await,
            "file changes under the promoted root must be detected"
        );
    }

    /// A recursive registration that keeps failing after the root appears
    /// must neither abandon the root nor tear down the ancestor watch, and
    /// ancestor traffic during the retries must not break recovery: once
    /// the registration can succeed the watch promotes, catches up, and
    /// detects later changes. An unreadable root reproduces the failure
    /// deterministically: `exists()` needs only search permission on the
    /// parent, but `inotify_add_watch` needs read permission on the
    /// directory itself, so every attempt fails with `EACCES` until the mode
    /// is restored. Linux only: `FSEvents` on macOS has no such read check and
    /// registers the unreadable root successfully.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn failed_promotion_keeps_ancestor_watch_and_recovers() {
        use std::os::unix::fs::PermissionsExt;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("promote-fail");
        let root = dir.path.join(".intent").join("specialists");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let hub = SharedWatchHub::new();
        let watch = watch_root(&hub, root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            wait_for(|| watch.watched().is_some(), LIVENESS).await,
            "ancestor watch must establish"
        );
        let ancestor = watch.watched().expect("watched").0;

        std::fs::create_dir_all(&root).expect("create root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000))
            .expect("chmod root");
        let restore = || {
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
                .expect("restore root mode");
        };
        if std::fs::read_dir(&root).is_ok() {
            // A privileged user reads a mode-000 directory; the failure cannot
            // be provoked here.
            restore();
            return;
        }

        // Let the first attempt and at least one retry fail while the
        // ancestor keeps producing events (each is one wake hint; the loop's
        // wake channel must not grow with them).
        for i in 0..CREATE_RETRY_INITIAL.as_millis() / 10 {
            std::fs::write(ancestor.join(format!("noise-{i}")), "x").expect("write noise");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        assert_eq!(
            watch.watched(),
            Some((ancestor.clone(), false)),
            "a failing recursive registration must keep the ancestor watch, not drop it"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "no promotion may be reported yet"
        );

        restore();
        assert!(
            wait_for(|| watch.watched() == Some((root.clone(), true)), LIVENESS).await,
            "watch must promote once registration can succeed, got {:?}",
            watch.watched()
        );
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) >= 1, LIVENESS).await,
            "recovered promotion must fire a catch-up notification"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let before = hits.load(Ordering::SeqCst);
        std::fs::write(root.join("new.md"), "x").expect("write md");
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > before, LIVENESS).await,
            "file changes under the recovered root must be detected"
        );
        drop(watch);
    }

    /// Regression for the widened-ancestor promotion hole (PR #1876 review,
    /// the intent-hq/intent#4852 shape): while the loop parks on the missing
    /// root's parent, a recursive co-subscriber of that parent (a workspace
    /// root watch) joins and leaves, which leaves the parent's OS watch
    /// recursive. Promotion then subscribes the root and, on storing it,
    /// releases the parent watch — whose recursive unwatch strips the root's
    /// descriptors, so the hub re-registers the root behind the loop's
    /// completed `wait_live`. With that registration failing, the loop must
    /// not stay parked on a dead watch: it re-registers and the recovered
    /// watch reports later changes. Linux only, like the hub behaviour it
    /// exercises. The `watch()` calls the fault seam counts on `specialists`:
    /// the recursive parent's descriptor-tracking adds the directory the
    /// moment it is created (1st), promotion registers it (2nd), the
    /// parent's retirement re-registers it (3rd, injected failure), and the
    /// loop recovers (4th).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn promotion_off_a_widened_ancestor_survives_a_failed_re_registration() {
        use crate::events::shared_watch::WatchFault;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("widened");
        let root = dir.path.join("specialists");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let fault = WatchFault::nth("specialists", 3);
        let hub = SharedWatchHub::with_watch_fault(&fault);
        let watch = watch_root(&hub, root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            wait_for(
                || watch.watched() == Some((dir.path.clone(), false)),
                LIVENESS
            )
            .await,
            "ancestor watch must establish"
        );

        // A recursive co-subscriber widens the ancestor's OS watch and leaves;
        // the hub keeps the root recursive for the ancestor watch's lifetime.
        let (sub_wide, _rx_wide, _) = hub.subscribe(&dir.path);
        sub_wide.wait_established(LIVENESS).await;
        drop(sub_wide);

        std::fs::create_dir_all(&root).expect("create root");
        // The parent's descriptor-tracking watches the new directory (1st
        // call); promotion registers the root (2nd, live), stores it —
        // releasing the ancestor watch, whose retirement re-registers the
        // root (3rd, injected failure) — and the loop recovers (4th).
        assert!(
            wait_for(|| fault.attempts() >= 4, LIVENESS).await,
            "the lost promoted watch must be re-registered, saw {} watch() calls",
            fault.attempts()
        );
        assert!(
            wait_for(|| watch.watched() == Some((root.clone(), true)), LIVENESS).await,
            "watch must settle on a recursive watch of the root, got {:?}",
            watch.watched()
        );
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) >= 1, LIVENESS).await,
            "the recovered promotion must fire a catch-up notification"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        let before = hits.load(Ordering::SeqCst);
        std::fs::write(root.join("new.md"), "x").expect("write md");
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > before, LIVENESS).await,
            "file changes under the recovered root must be detected"
        );
        assert_eq!(
            fault.attempts(),
            4,
            "exactly one recovery registration must follow the injected failure"
        );
    }

    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn directory_only_deletion_is_forwarded() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("rmdir");
        let root = dir.path.join("specialists");
        std::fs::create_dir_all(root.join("nested")).expect("mk root + nested dir");
        let hits = Arc::new(AtomicUsize::new(0));
        let h = Arc::clone(&hits);
        let hub = SharedWatchHub::new();
        let watch = watch_root(&hub, root.clone(), md_only, move || {
            h.fetch_add(1, Ordering::SeqCst);
        });
        assert!(
            wait_for(|| watch.watched() == Some((root.clone(), true)), LIVENESS).await,
            "recursive watch must establish"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;

        // No `.md` file ever exists: `rm -rf` surfaces only directory-level
        // events, which the filter must still forward.
        std::fs::remove_dir_all(&root).expect("remove tier dir");
        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > 0, LIVENESS).await,
            "tier-directory deletion must forward an event"
        );
    }

    /// Regression (intent-hq/monorepo#1572): OS watch registration can park
    /// indefinitely (macOS `FSEvents`), so `watch_root` must return without
    /// performing it — the watch is established from a spawned task instead.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn existing_root_registration_does_not_block_the_caller() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("nonblocking");
        let root = dir.path.join("specialists");
        std::fs::create_dir_all(&root).expect("mk root");

        let hub = SharedWatchHub::new();
        let start = std::time::Instant::now();
        let watch = watch_root(&hub, root.clone(), md_only, || {});
        let elapsed = start.elapsed();
        // Deterministic only under the current-thread runtime `#[tokio::test]`
        // defaults to: the spawned `watch_loop` cannot be polled before this
        // test's first `.await`. Under `flavor = "multi_thread"` a worker
        // could land registration first and this would flake in the passing
        // direction of the bug — the `elapsed` bound below is the
        // flavor-independent part of the assertion.
        assert!(
            watch.watched().is_none(),
            "registration must be deferred, not performed on the caller's thread"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "watch_root must return immediately, took {elapsed:?}"
        );

        assert!(
            wait_for(|| watch.watched() == Some((root.clone(), true)), LIVENESS).await,
            "the watch must still establish in the background"
        );
    }

    /// Deferred registration opens a window between `watch_root` returning
    /// and the watch existing. Callers prime their fingerprint before that,
    /// so a change landing in the window must still be flushed once the
    /// watch is established — otherwise it is absorbed as pre-existing.
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn existing_root_flushes_changes_that_land_before_registration() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new("catchup");
        let root = dir.path.join("specialists");
        std::fs::create_dir_all(&root).expect("mk root");

        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hits);
        let hub = SharedWatchHub::new();
        let watch = watch_root(&hub, root.clone(), md_only, move || {
            seen.fetch_add(1, Ordering::SeqCst);
        });
        // Written while registration is still pending: no OS event for it can
        // ever be delivered, so only the catch-up flush can surface it.
        assert!(
            watch.watched().is_none(),
            "registration must still be pending"
        );
        std::fs::write(root.join("a.md"), "x").expect("write file");

        assert!(
            wait_for(|| hits.load(Ordering::SeqCst) > 0, LIVENESS).await,
            "a change landing before registration must still trigger a flush"
        );
    }
}
