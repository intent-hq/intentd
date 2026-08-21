//! Filesystem watcher → `file:*` events (§10).
//!
//! Ports the workspace file-watch slice from `~/src/intent/`: the recursive
//! watch (since consolidated into a stream shared across workspaces by
//! [`SharedWatchHub`] and demuxed by path prefix) + per-path debounce of
//! `workspace/main/change-detection/file-watcher.ts` (`fileWatcherDebounce`,
//! `handleFileEvent`) and the canonical event-type taxonomy of
//! `change-detection/change-processor.ts` (`getEventType`): a `create` becomes
//! `file:created`, a `delete` becomes `file:deleted`, and both `modify` and
//! `rename` stay `file:changed` (`file:renamed` is never emitted). The
//! `data.action` discriminant always carries the raw `create|modify|delete|
//! rename` verb regardless of the event type, matching the TS `FileChangedEvent`
//! wire shape. Raw FS callbacks (sync, off-runtime) feed a tokio debounce task
//! that coalesces rapid changes per path before publishing to the [`EventBus`].
//!
//! Git-ignored paths are suppressed at ingest time via the `ignore` crate
//! (ripgrep's gitignore engine): a [`GitignoreMatcher`] built at watcher start
//! (and rebuilt when ignore rules change) drops ignored paths before they
//! enter the debounce map, so per-file events, burst summaries, and the
//! shutdown flush are all clean. `file:*` rows persisted before this filter
//! existed are historical noise aged out by the existing retention sweep
//! (`delete_ephemeral_events_before` in intent-store's event repo); the query
//! path is unchanged.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use ignore::{Match, WalkBuilder};
use intent_core::{now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use notify::event::{EventKind, ModifyKind};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::shared_watch::{SharedWatchHub, SubHandle};

/// Per-path debounce window. Matches the TS `fileWatcherDebounce` (300 ms): an
/// event is published `DEBOUNCE` after the *last* raw change for that path.
const DEBOUNCE: Duration = Duration::from_millis(300);

/// Burst threshold: when pending events exceed this count during a flush
/// window, collapse into per-directory summary events rather than emitting
/// one row per file (finding F4: prevent 31,881 INSERTs from bulk churn).
const BURST_THRESHOLD: usize = 100;

/// After a burst flush, keep collapsing due paths into directory summaries for
/// this long. Bulk churn often reaches the loop in several waves (staggered OS
/// delivery, late modify re-notifications for the same writes), and each wave
/// on its own can sit below [`BURST_THRESHOLD`]; the cooldown makes trailing
/// waves of the same churn collapse too instead of flushing per-file
/// (STAB-121). Only refreshed when the backlog itself exceeds the threshold —
/// cooldown-only collapses consume the window rather than extend it, so
/// unrelated small activity after a churn returns to per-file events within
/// one cooldown instead of staying in summary mode indefinitely.
const BURST_COOLDOWN: Duration = Duration::from_millis(1000);

/// Upper bound on raw events ingested per [`drain_ready`] call. `ingest` is
/// cheap and never awaits, but the raw channel is unbounded; the cap keeps a
/// pathological backlog from pinning the loop in a single non-yielding drain.
/// Leftovers are picked up by the next `recv`/flush iteration.
const DRAIN_MAX_PER_CALL: usize = 10_000;

/// Directory names ignored at any depth, mirroring the `IGNORE_PATTERNS` of
/// `unified-workspace-watcher.ts` plus the `.workspace-notes` additions of
/// `tracking.config.ts`. A path is dropped if any component matches.
const IGNORED_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    "out",
    ".next",
    ".nuxt",
    ".cache",
    ".parcel-cache",
    ".vscode",
    ".idea",
    "coverage",
    ".nyc_output",
    ".pytest_cache",
    "__pycache__",
    "venv",
    "vendor",
    ".svn",
    ".hg",
    "CVS",
    ".sass-cache",
    "tmp",
    "temp",
    ".tmp",
    ".temp",
    ".augment",
    ".intent",
    ".workspace-notes",
    ".workspace-notes.backup",
    ".workspace",
];

/// Default ignore patterns applied below every gitignore source, ported from
/// the pre-port TS `GitignoreManager.DEFAULT_PATTERNS`. They hold even in
/// non-git workspaces (the TS fallback behavior), and a user `.gitignore`
/// negation (e.g. `!dist`) overrides them because gitignore files rank higher
/// in the matcher chain.
const DEFAULT_IGNORE_PATTERNS: &[&str] = &[
    "node_modules",
    ".git",
    ".DS_Store",
    "Thumbs.db",
    "dist",
    "build",
    ".next",
    ".svelte-kit",
    "coverage",
    ".cache",
    "*.log",
    ".env",
    ".env.local",
    ".augment/*",
];

/// The raw change verb carried in `data.action` of every `file:*` event
/// (`file:created`/`file:deleted`/`file:changed` alike, per the module docs).
/// Serializes to the lowercase TS values (`change.action.toLowerCase()`).
/// `pub(super)` so the flush tests can build pending maps directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Action {
    Modify,
    Rename,
    Create,
    Delete,
}

impl Action {
    fn as_str(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Modify => "modify",
            Action::Delete => "delete",
            Action::Rename => "rename",
        }
    }

    /// Canonical `file:*` event type for this action, mirroring the TS
    /// `change-processor.ts` `getEventType`: `create`/`delete` get their own
    /// types; `modify` and `rename` collapse onto `file:changed`.
    fn event_type(self) -> &'static str {
        use intent_core::events::{FILE_CHANGED, FILE_CREATED, FILE_DELETED};
        match self {
            Action::Create => FILE_CREATED,
            Action::Delete => FILE_DELETED,
            Action::Modify | Action::Rename => FILE_CHANGED,
        }
    }

    /// Coalescing precedence: when several raw events land on one path inside
    /// the debounce window, the highest-rank action wins (a create+modify reads
    /// as `create`; anything ending in removal reads as `delete`).
    fn rank(self) -> u8 {
        match self {
            Action::Modify => 0,
            Action::Rename => 1,
            Action::Create => 2,
            Action::Delete => 3,
        }
    }
}

/// Map a `notify` event kind to an [`Action`]; `None` for access/other kinds
/// that carry no mutation (they are dropped, matching the TS adapter which only
/// forwards add/change/unlink).
fn action_for(kind: EventKind) -> Option<Action> {
    match kind {
        EventKind::Create(_) => Some(Action::Create),
        EventKind::Remove(_) => Some(Action::Delete),
        EventKind::Modify(ModifyKind::Name(_)) => Some(Action::Rename),
        EventKind::Modify(_) => Some(Action::Modify),
        EventKind::Any => Some(Action::Modify),
        EventKind::Access(_) | EventKind::Other => None,
    }
}

/// True when `relative` lives under an ignored directory (component match).
fn should_ignore(relative: &Path) -> bool {
    relative.components().any(|c| match c {
        Component::Normal(name) => name.to_str().is_some_and(|n| IGNORED_DIRS.contains(&n)),
        _ => false,
    })
}

/// Outcome of consulting the gitignore matcher chain for one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IgnoreVerdict {
    /// Some gitignore source ignores the path — suppress the event.
    Ignore,
    /// A negation (`!pattern`) explicitly re-includes the path; also rescues
    /// paths the [`IGNORED_DIRS`] prefilter would drop.
    Whitelist,
    /// No gitignore source matched.
    None,
}

fn to_verdict<T>(m: Match<T>) -> IgnoreVerdict {
    if m.is_ignore() {
        IgnoreVerdict::Ignore
    } else if m.is_whitelist() {
        IgnoreVerdict::Whitelist
    } else {
        IgnoreVerdict::None
    }
}

/// Locate the repository git dir for `root`, handling both a `.git` directory
/// and a `.git` **file** (`gitdir: <path>`) as written by linked worktrees and
/// `CoW` checkouts. Returns `None` for non-git roots.
fn resolve_git_dir(root: &Path) -> Option<PathBuf> {
    let dot_git = root.join(".git");
    let meta = std::fs::metadata(&dot_git).ok()?;
    if meta.is_dir() {
        return Some(dot_git);
    }
    let contents = std::fs::read_to_string(&dot_git).ok()?;
    let target = Path::new(contents.strip_prefix("gitdir:")?.trim());
    Some(if target.is_absolute() {
        target.to_path_buf()
    } else {
        root.join(target)
    })
}

/// Locate `info/exclude` for `git_dir`. Linked worktrees keep it under the
/// *common* dir (the `commondir` file inside the worktree's git dir points
/// there); primary checkouts use the git dir itself.
fn resolve_exclude_file(git_dir: &Path) -> PathBuf {
    let base = match std::fs::read_to_string(git_dir.join("commondir")) {
        Ok(contents) => {
            let common = Path::new(contents.trim());
            if common.is_absolute() {
                common.to_path_buf()
            } else {
                git_dir.join(common)
            }
        }
        Err(_) => git_dir.to_path_buf(),
    };
    base.join("info").join("exclude")
}

/// Ingest-time gitignore evaluation, mirroring how the pre-port TS filtered
/// through `gitignore-manager.ts` before debouncing but with full Git
/// semantics via the `ignore` crate. Sources are consulted highest precedence
/// first: `.gitignore` files (deepest directory wins, per Git), then
/// `.git/info/exclude`, then global excludes (`core.excludesFile`), then the
/// [`DEFAULT_IGNORE_PATTERNS`]; within each source the usual last-match-wins
/// gitignore rule applies, so the first source with a definitive match
/// decides.
///
/// The matcher is rebuilt lazily (on next use) after a raw event touches a
/// `.gitignore` file at any depth or the repo's `info/exclude`, so rule edits
/// take effect without a daemon restart. External edits to *global* excludes
/// are outside the watched root and stay restart-scoped. Failures degrade
/// gracefully: a source that fails to load simply drops out of the chain, so
/// legitimate events are never suppressed by an error.
struct GitignoreMatcher {
    root: PathBuf,
    /// Resolved `<gitdir>/info/exclude` (worktree-aware), watched for edits.
    /// Detected on the absolute path before the [`IGNORED_DIRS`] prefilter,
    /// which would otherwise drop everything under `.git`. Both the resolved
    /// and canonicalized forms are kept (deduped): `notify` may report either
    /// shape (symlinked roots, `..` segments from a worktree `commondir`), so
    /// comparing against both keeps invalidation robust.
    exclude_paths: Vec<PathBuf>,
    /// `.gitignore` matchers paired with their containing directory, ordered
    /// deepest-first so nested files take precedence per Git.
    gitignores: Vec<(PathBuf, Gitignore)>,
    exclude: Option<Gitignore>,
    global: Option<Gitignore>,
    defaults: Option<Gitignore>,
    /// Whether any source carries a `!` negation. When false, nothing can
    /// rescue a prefiltered path, so ingest skips the match entirely for
    /// [`IGNORED_DIRS`] paths (the fast path).
    has_whitelists: bool,
    dirty: bool,
}

impl GitignoreMatcher {
    fn new(root: PathBuf) -> Self {
        let mut matcher = Self {
            root,
            exclude_paths: Vec::new(),
            gitignores: Vec::new(),
            exclude: None,
            global: None,
            defaults: None,
            has_whitelists: false,
            dirty: false,
        };
        matcher.rebuild();
        matcher
    }

    /// Mark the matcher dirty when a raw event touches an ignore-rule file:
    /// a `.gitignore` at any depth or the repo's `info/exclude`. Rebuilding is
    /// deferred to the next [`Self::verdict`] call.
    ///
    /// `.gitignore` files under [`IGNORED_DIRS`] (e.g. `target/.gitignore`
    /// written by cargo, or files shipped inside `vendor/`) are skipped by the
    /// discovery walk in [`Self::rebuild`], so a rebuild for them would be a
    /// no-op — don't mark dirty for those (checked via `rel`, the
    /// workspace-relative path). The `exclude_paths` comparison stays on the
    /// absolute path: `info/exclude` intentionally lives under `.git`. The
    /// incoming path is canonicalized only on the cheap file-name match, and
    /// both it and its canonical form are compared against both stored forms,
    /// since `notify` and `resolve_exclude_file` may disagree on symlinks.
    fn note_raw_change(&mut self, abs: &Path, rel: Option<&str>) {
        if !self.exclude_paths.is_empty() && abs.file_name().is_some_and(|n| n == "exclude") {
            let canon = std::fs::canonicalize(abs).ok();
            if self
                .exclude_paths
                .iter()
                .any(|p| p == abs || Some(p) == canon.as_ref())
            {
                self.dirty = true;
                return;
            }
        }
        if abs.file_name().is_some_and(|n| n == ".gitignore")
            && rel.is_some_and(|r| !should_ignore(Path::new(r)))
        {
            self.dirty = true;
        }
    }

    /// Whether any source carries a `!` negation. Rebuilds first when the
    /// matcher is dirty so the ingest fast-path never consults a stale
    /// answer: a stale `false` would let the [`IGNORED_DIRS`] prefilter drop
    /// a path that a freshly added negation rescues.
    fn has_whitelists(&mut self) -> bool {
        if self.dirty {
            self.rebuild();
        }
        self.has_whitelists
    }

    /// Evaluate `abs` (with `rel`, its workspace-relative form) against the
    /// source chain. Uses parent-aware matching so files under an ignored
    /// directory (e.g. `.svelte-kit/output/x.d.ts` with a `.svelte-kit/`
    /// rule) match even after deletion: with no stat available the path is
    /// treated as a file, but its ancestors are checked as directories.
    fn verdict(&mut self, abs: &Path, rel: &str) -> IgnoreVerdict {
        if self.dirty {
            self.rebuild();
        }
        let is_dir = abs.is_dir();
        for (dir, matcher) in &self.gitignores {
            if !abs.starts_with(dir) {
                continue;
            }
            let v = to_verdict(matcher.matched_path_or_any_parents(abs, is_dir));
            if v != IgnoreVerdict::None {
                return v;
            }
        }
        if let Some(matcher) = &self.exclude {
            let v = to_verdict(matcher.matched_path_or_any_parents(abs, is_dir));
            if v != IgnoreVerdict::None {
                return v;
            }
        }
        if let Some(matcher) = &self.global {
            // The global matcher is rooted at "" (per `Gitignore::global`), so
            // it must see the relative path, not the absolute one.
            let v = to_verdict(matcher.matched_path_or_any_parents(Path::new(rel), is_dir));
            if v != IgnoreVerdict::None {
                return v;
            }
        }
        if let Some(matcher) = &self.defaults {
            return to_verdict(matcher.matched_path_or_any_parents(abs, is_dir));
        }
        IgnoreVerdict::None
    }

    /// (Re)load every source. Runs at watcher start and after ignore-rule
    /// edits; the `.gitignore` discovery walk is itself gitignore-aware and
    /// skips [`IGNORED_DIRS`], so it stays cheap even on large trees.
    fn rebuild(&mut self) {
        self.dirty = false;
        self.gitignores.clear();
        self.exclude = None;
        self.global = None;
        self.exclude_paths.clear();

        let mut defaults = GitignoreBuilder::new(&self.root);
        for pattern in DEFAULT_IGNORE_PATTERNS {
            let _ = defaults.add_line(None, pattern);
        }
        self.defaults = match defaults.build() {
            Ok(matcher) => Some(matcher),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to build default ignore matcher; only IGNORED_DIRS filtering applies"
                );
                None
            }
        };

        if let Some(git_dir) = resolve_git_dir(&self.root) {
            let (global, err) = Gitignore::global();
            if let Some(e) = err {
                tracing::debug!(error = %e, "failed to load global git excludes");
            }
            if global.num_ignores() + global.num_whitelists() > 0 {
                self.global = Some(global);
            }

            let exclude_path = resolve_exclude_file(&git_dir);
            if exclude_path.is_file() {
                let mut builder = GitignoreBuilder::new(&self.root);
                if let Some(e) = builder.add(&exclude_path) {
                    tracing::debug!(error = %e, "failed to read .git/info/exclude");
                }
                match builder.build() {
                    Ok(matcher) => self.exclude = Some(matcher),
                    Err(e) => {
                        tracing::debug!(error = %e, "failed to build info/exclude matcher");
                    }
                }
            }
            if let Ok(canon) = std::fs::canonicalize(&exclude_path) {
                if canon != exclude_path {
                    self.exclude_paths.push(canon);
                }
            }
            self.exclude_paths.push(exclude_path);

            let mut files: Vec<PathBuf> = Vec::new();
            // Keep discovery deterministic for a given tree: don't consult
            // parent-of-root ignore rules, `.ignore` files, host global
            // excludes, or `.git/info/exclude` during the walk — the sources
            // we honor are added to the matcher chain explicitly above.
            // In-tree `.gitignore` awareness stays on so ignored subtrees are
            // pruned from the walk itself.
            let walker = WalkBuilder::new(&self.root)
                .hidden(false)
                .follow_links(false)
                .parents(false)
                .ignore(false)
                .git_global(false)
                .git_exclude(false)
                .filter_entry(|entry| {
                    !entry.file_type().is_some_and(|t| t.is_dir())
                        || !entry
                            .file_name()
                            .to_str()
                            .is_some_and(|n| IGNORED_DIRS.contains(&n))
                })
                .build();
            for entry in walker.flatten() {
                if entry.file_type().is_some_and(|t| t.is_file())
                    && entry.file_name() == ".gitignore"
                {
                    files.push(entry.into_path());
                }
            }
            files.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
            for file in files {
                let Some(dir) = file.parent().map(Path::to_path_buf) else {
                    continue;
                };
                let mut builder = GitignoreBuilder::new(&dir);
                if let Some(e) = builder.add(&file) {
                    tracing::debug!(error = %e, path = %file.display(), "failed to read .gitignore");
                }
                match builder.build() {
                    Ok(matcher) => self.gitignores.push((dir, matcher)),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            path = %file.display(),
                            "failed to build .gitignore matcher"
                        );
                    }
                }
            }
        }

        self.has_whitelists = self
            .gitignores
            .iter()
            .map(|(_, m)| m)
            .chain(self.exclude.iter())
            .chain(self.global.iter())
            .any(|m| m.num_whitelists() > 0);
    }
}

/// Workspace-relative, forward-slash path for the event payload, or `None` when
/// `abs` is outside `root` (defensive; `notify` only reports under `root`).
fn relative_path(root: &Path, abs: &Path) -> Option<String> {
    let rel = abs.strip_prefix(root).ok()?;
    let joined = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    Some(joined)
}

/// Extract the parent directory of a workspace-relative path. Root files return
/// an empty string (workspace root), nested paths return the parent directory.
fn parent_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(idx) => path[..idx].to_string(),
        None => String::new(),
    }
}

/// A live watch over one workspace path. The recursive OS stream is no longer
/// owned here: it is shared across workspaces by [`SharedWatchHub`] and demuxed
/// by path prefix, so this holds only the subscription handle and the debounce
/// task (aborted on drop). Dropping the [`FileWatcher`] still tears this
/// workspace's whole pipeline down and releases its share of the stream — the
/// clean-shutdown contract for `serve`.
pub struct FileWatcher {
    _sub: SubHandle,
    task: JoinHandle<()>,
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FileWatcher {
    /// Start watching `root`, publishing debounced `file:changed` events for
    /// `workspace_id` to `bus`. Raw events arrive from the shared stream `hub`
    /// owns for `root`'s group, demuxed to this workspace, and are fed to the
    /// same async debounce loop as before.
    pub(super) fn start(
        hub: &Arc<SharedWatchHub>,
        bus: EventBus,
        workspace_id: WorkspaceId,
        root: PathBuf,
    ) -> Self {
        // `subscribe` returns the canonical root it demuxes against, so the
        // relative-path strip works against the paths the OS reports (macOS
        // FSEvents resolves `/var/...` → `/private/var/...`).
        let (sub, raw_rx, root) = hub.subscribe(&root);
        let task = tokio::spawn(debounce_loop(bus, workspace_id, root, raw_rx));
        Self { _sub: sub, task }
    }

    /// Await the shared watch on this workspace's root actually being
    /// established. Registration is deferred off the caller's thread
    /// (monorepo#1572), so tests must wait for it before mutating the tree.
    #[cfg(test)]
    pub(super) async fn wait_established(&self, timeout: Duration) {
        self._sub.wait_established(timeout).await;
    }
}

/// Coalesce raw FS events per path within [`DEBOUNCE`], then publish one
/// `file:changed` per path. A path is flushed `DEBOUNCE` after its last raw
/// event (timer reset on each new event, as in the TS `handleFileEvent`).
async fn debounce_loop(
    bus: EventBus,
    workspace_id: WorkspaceId,
    root: PathBuf,
    mut raw_rx: mpsc::UnboundedReceiver<notify::Event>,
) {
    let mut matcher = GitignoreMatcher::new(root.clone());
    let mut pending: HashMap<String, (Action, tokio::time::Instant)> = HashMap::new();
    let mut burst_until: Option<tokio::time::Instant> = None;
    loop {
        let next_deadline = pending.values().map(|(_, at)| *at).min();
        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(event) => {
                    ingest(&root, &mut matcher, &event, &mut pending);
                    drain_ready(&root, &mut matcher, &mut raw_rx, &mut pending);
                }
                // Watcher dropped: flush whatever is pending, then stop.
                None => {
                    flush_all(&bus, &workspace_id, &mut pending).await;
                    return;
                }
            },
            () = sleep_until(next_deadline), if next_deadline.is_some() => {
                // Ingest everything already delivered before deciding what is
                // due, so the burst decision sees the full backlog even when
                // publishes are slow (STAB-121).
                drain_ready(&root, &mut matcher, &mut raw_rx, &mut pending);
                flush_due(&bus, &workspace_id, &mut pending, &mut burst_until).await;
            }
        }
    }
}

/// Ingest every raw event already sitting in the channel without awaiting.
/// `tokio::select!` only takes one branch per iteration, so slow publishes
/// would otherwise starve ingestion: each raw event would be ingested one
/// publish-latency apart, spreading per-path deadlines so far that no single
/// flush ever sees the whole churn and the burst collapse never engages
/// (STAB-121).
fn drain_ready(
    root: &Path,
    matcher: &mut GitignoreMatcher,
    raw_rx: &mut mpsc::UnboundedReceiver<notify::Event>,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    for _ in 0..DRAIN_MAX_PER_CALL {
        match raw_rx.try_recv() {
            Ok(event) => ingest(root, matcher, &event, pending),
            Err(_) => break,
        }
    }
}

/// Fold one raw event into `pending`, resetting each affected path's deadline.
/// Gitignored paths are dropped here — before they enter `pending` — so
/// ignored churn never inflates the burst threshold.
fn ingest(
    root: &Path,
    matcher: &mut GitignoreMatcher,
    event: &notify::Event,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    let Some(action) = action_for(event.kind) else {
        return;
    };
    let deadline = tokio::time::Instant::now() + DEBOUNCE;
    for abs in &event.paths {
        // Observe ignore-rule edits before any filtering: info/exclude lives
        // under `.git`, which the IGNORED_DIRS prefilter drops.
        let rel = relative_path(root, abs);
        matcher.note_raw_change(abs, rel.as_deref());
        let Some(rel) = rel else {
            continue;
        };
        if rel.is_empty() {
            continue;
        }
        let prefiltered = should_ignore(Path::new(&rel));
        // Fast path: without any `!` negation nothing can rescue a
        // prefiltered path, so skip the gitignore match entirely.
        if prefiltered && !matcher.has_whitelists() {
            continue;
        }
        match matcher.verdict(abs, &rel) {
            IgnoreVerdict::Ignore => continue,
            IgnoreVerdict::None if prefiltered => continue,
            IgnoreVerdict::Whitelist | IgnoreVerdict::None => {}
        }
        let merged = match pending.get(&rel) {
            Some((existing, _)) if existing.rank() >= action.rank() => *existing,
            _ => action,
        };
        pending.insert(rel, (merged, deadline));
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Publish + remove every path whose debounce deadline has elapsed. When the
/// in-flight backlog exceeds [`BURST_THRESHOLD`], collapse into per-directory
/// summaries.
///
/// The burst decision is based on the whole `pending` map, not just the paths
/// due at this instant: staggered OS delivery spreads per-path deadlines, so
/// a single bulk churn can come due across several flushes that individually
/// sit below the threshold (STAB-121). Once a flush collapses, `burst_until`
/// keeps subsequent flushes collapsed for [`BURST_COOLDOWN`] so trailing waves
/// of the same churn (e.g. late modify re-notifications) summarize too. The
/// cooldown is only refreshed while the backlog stays above the threshold;
/// cooldown-only collapses do not extend it, so unrelated small activity
/// cannot keep the watcher in summary mode indefinitely.
///
/// `pub(super)` so tests can exercise the burst decision deterministically
/// with hand-built pending maps (no OS watcher or sleeps).
pub(super) async fn flush_due(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
    burst_until: &mut Option<tokio::time::Instant>,
) {
    let now = tokio::time::Instant::now();
    let due: Vec<String> = pending
        .iter()
        .filter(|(_, (_, at))| *at <= now)
        .map(|(p, _)| p.clone())
        .collect();
    if due.is_empty() {
        return;
    }

    let over_threshold = pending.len() > BURST_THRESHOLD;
    let in_cooldown = burst_until.is_some_and(|until| now < until);
    if over_threshold || in_cooldown {
        if over_threshold {
            *burst_until = Some(now + BURST_COOLDOWN);
        }
        flush_burst(bus, workspace_id, pending, &due).await;
    } else {
        for path in due {
            if let Some((action, _)) = pending.remove(&path) {
                publish(bus, workspace_id, &path, action).await;
            }
        }
    }
}

/// Handle burst scenario: collapse >`BURST_THRESHOLD` events into bounded
/// per-directory summary events with metadata indicating the burst.
async fn flush_burst(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
    due: &[String],
) {
    // Group due paths by directory.
    let mut by_dir: HashMap<String, Vec<(String, Action)>> = HashMap::new();
    for path in due {
        if let Some((action, _)) = pending.remove(path) {
            let dir = parent_dir(path);
            by_dir.entry(dir).or_default().push((path.clone(), action));
        }
    }

    // Emit one summary event per directory containing the count and actions.
    for (dir, files) in by_dir {
        publish_burst(bus, workspace_id, &dir, &files).await;
    }
}

/// Drain every pending path unconditionally (shutdown flush).
async fn flush_all(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    for (path, (action, _)) in std::mem::take(pending) {
        publish(bus, workspace_id, &path, action).await;
    }
}

/// Emit one `file:*` event matching the TS `FileChangedEvent` wire shape:
/// `data.{path,relativePath,action}` (both paths workspace-relative) attributed
/// to the system actor. The event type follows [`Action::event_type`]
/// (`file:created`/`file:deleted`/`file:changed`) while `data.action` always
/// carries the raw verb.
async fn publish(bus: &EventBus, workspace_id: &WorkspaceId, relative: &str, action: Action) {
    let event = NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: action.event_type().to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            id: Some("system".to_string()),
            name: Some("System".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "path": relative,
            "relativePath": relative,
            "action": action.as_str(),
        }),
    };
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, path = relative, "failed to publish file:* event");
    }
}

/// Emit a burst summary event for a directory: a single `file:changed` event
/// with `data.burst = true` and `data.affectedCount` indicating the number of
/// files affected. Subscribers recognize the burst marker and treat it as a
/// directory-level change rather than expecting individual per-file events.
async fn publish_burst(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    dir: &str,
    files: &[(String, Action)],
) {
    let count = files.len();
    let display_path = if dir.is_empty() {
        ".".to_string()
    } else {
        dir.to_string()
    };

    // Count actions to provide summary metadata.
    let mut creates = 0;
    let mut deletes = 0;
    let mut modifies = 0;
    for (_, action) in files {
        match action {
            Action::Create => creates += 1,
            Action::Delete => deletes += 1,
            Action::Modify | Action::Rename => modifies += 1,
        }
    }

    let event = NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: intent_core::events::FILE_CHANGED.to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            id: Some("system".to_string()),
            name: Some("System".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "path": display_path,
            "relativePath": display_path,
            "action": "modify",
            "burst": true,
            "affectedCount": count,
            "creates": creates,
            "deletes": deletes,
            "modifies": modifies,
        }),
    };
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, dir = dir, count = count, "failed to publish burst event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};

    #[test]
    fn action_for_maps_notify_kinds() {
        assert_eq!(
            action_for(EventKind::Create(CreateKind::File)),
            Some(Action::Create)
        );
        assert_eq!(
            action_for(EventKind::Remove(RemoveKind::File)),
            Some(Action::Delete)
        );
        assert_eq!(
            action_for(EventKind::Modify(ModifyKind::Name(RenameMode::Both))),
            Some(Action::Rename)
        );
        assert_eq!(
            action_for(EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Content
            ))),
            Some(Action::Modify)
        );
        assert_eq!(action_for(EventKind::Other), None);
    }

    #[test]
    fn event_type_matches_ts_taxonomy() {
        use intent_core::events::{FILE_CHANGED, FILE_CREATED, FILE_DELETED};
        // change-processor.ts getEventType: create/delete get distinct types;
        // modify/rename collapse onto file:changed (file:renamed is never emitted).
        assert_eq!(Action::Create.event_type(), FILE_CREATED);
        assert_eq!(Action::Delete.event_type(), FILE_DELETED);
        assert_eq!(Action::Modify.event_type(), FILE_CHANGED);
        assert_eq!(Action::Rename.event_type(), FILE_CHANGED);
    }

    #[test]
    fn action_precedence_keeps_strongest() {
        // Delete > Create > Rename > Modify.
        assert!(Action::Delete.rank() > Action::Create.rank());
        assert!(Action::Create.rank() > Action::Rename.rank());
        assert!(Action::Rename.rank() > Action::Modify.rank());
    }

    #[test]
    fn should_ignore_matches_noise_dirs() {
        assert!(should_ignore(Path::new("node_modules/foo.js")));
        assert!(should_ignore(Path::new("src/.git/index")));
        assert!(should_ignore(Path::new("target/debug/x")));
        assert!(should_ignore(Path::new(".workspace-notes/n.md")));
        assert!(!should_ignore(Path::new("src/main.rs")));
        assert!(!should_ignore(Path::new("README.md")));
    }

    #[test]
    fn relative_path_strips_root_and_uses_forward_slashes() {
        let root = Path::new("/ws/root");
        assert_eq!(
            relative_path(root, Path::new("/ws/root/src/main.rs")).as_deref(),
            Some("src/main.rs")
        );
        assert_eq!(relative_path(root, Path::new("/other/x")), None);
    }

    #[test]
    fn parent_dir_extracts_directory() {
        assert_eq!(parent_dir("foo.txt"), "");
        assert_eq!(parent_dir("src/main.rs"), "src");
        assert_eq!(parent_dir("a/b/c.txt"), "a/b");
        assert_eq!(parent_dir(""), "");
    }

    #[test]
    fn resolve_git_dir_handles_dir_gitdir_file_and_non_git() {
        let tmp = std::env::temp_dir().join(format!("intentd-gitdir-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(tmp.join("a/.git")).unwrap();
        assert_eq!(resolve_git_dir(&tmp.join("a")), Some(tmp.join("a/.git")));

        // Linked worktree / CoW checkout: `.git` is a file with a relative
        // gitdir pointer, resolved against the root.
        std::fs::create_dir_all(tmp.join("b")).unwrap();
        std::fs::write(tmp.join("b/.git"), "gitdir: ../a/.git/worktrees/b\n").unwrap();
        assert_eq!(
            resolve_git_dir(&tmp.join("b")),
            Some(tmp.join("b").join("../a/.git/worktrees/b"))
        );

        std::fs::create_dir_all(tmp.join("c")).unwrap();
        assert_eq!(resolve_git_dir(&tmp.join("c")), None);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_exclude_file_honors_commondir() {
        let tmp = std::env::temp_dir().join(format!("intentd-excl-{}", uuid::Uuid::new_v4()));
        let git_dir = tmp.join("gd");
        std::fs::create_dir_all(&git_dir).unwrap();
        assert_eq!(
            resolve_exclude_file(&git_dir),
            git_dir.join("info").join("exclude")
        );

        // Worktree git dirs carry a `commondir` file pointing at the shared dir.
        std::fs::write(git_dir.join("commondir"), "../..\n").unwrap();
        assert_eq!(
            resolve_exclude_file(&git_dir),
            git_dir.join("../..").join("info").join("exclude")
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
