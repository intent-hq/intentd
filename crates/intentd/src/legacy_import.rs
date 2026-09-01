//! Legacy workspace import — migrate per-directory Intent workspaces
//! (`<root>/<id>/.workspace/workspace.json`) into intentd's `SQLite` store.
//!
//! Legacy roots scanned by default: `~/intent/workspaces`, `~/intent`,
//! `~/.workspaces`. Only directories carrying `.workspace/workspace.json` are
//! candidates; everything else is ignored. The importer is read-only toward
//! the source and idempotent: ids already present in the DB are skipped
//! (updated only with `--force`). Manifests written by intentd itself
//! (`workspace.create` / `workspace.duplicate` stamp
//! `"managedBy": "intentd"`) are also skipped so a fresh daemon sharing the
//! workspaces root never adopts another daemon's live workspaces; `--force`
//! imports them anyway (the recovery path for a wiped DB).
//!
//! Two entry points share this module:
//! - The first-boot hook in `intentd serve`: [`decide_first_boot_import`]
//!   makes the eligibility decision synchronously at startup (fresh DB or a
//!   [`LEGACY_IMPORT_PENDING_MARKER_KEY`] left by an interrupted run, and no
//!   [`LEGACY_IMPORT_MARKER_KEY`] completion marker), then
//!   [`run_first_boot_import`] runs in a spawned background task so startup
//!   never waits on it. It never fails the daemon; completion is best-effort —
//!   the completion marker is written even when individual workspaces failed
//!   (failures are logged and summarized under
//!   [`LEGACY_IMPORT_FAILURES_KEY`], with `intentd import-legacy --force` as
//!   the manual retry path) — and the pending marker persisted before the run
//!   starts makes a killed import resume on the next boot.
//! - `intentd import-legacy [--root <dir>] [--dry-run] [--force]`
//!   (`cmd_import_legacy` in `main.rs`).
//!
//! Later per-workspace importers (agent transcripts) plug into
//! [`import_workspace_extras`], which receives each imported workspace's
//! legacy directory. Notes import is implemented there: legacy
//! `.workspace/notes/{id}.md` files (YAML frontmatter + markdown body) become
//! `note` rows — `spec.md` lands as the well-known `spec` note, frontmatter
//! `task:` maps to task metadata (`task_json`), and `parent` to `parent_id`.
//! Comments import follows: `.workspace/notes/.meta/{noteId}.comments.json`
//! files (the legacy FE `NoteCommentsData` shape) become `comment` rows —
//! threads reconstruct via `threadId`/`parentId`, `markId`/`from`/`to` derive
//! the anchor, and legacy fields intentd does not model land in `extra_json`.
//! The rest of the `.meta/` sidecar (versions/CRDT/trash) is skipped entirely.
//! Agent transcripts land last: `.workspace/agents/{agentId}.json` files (the
//! legacy FE `AgentSession` shape) become `agent_session` rows plus ordered
//! `agent_message` rows — always as terminal `Completed` historical sessions
//! that the startup interrupted-agent heal sweep never touches.
//! Note assets follow: `.workspace/assets/{assetId}` files (plus their
//! `.meta.json` sidecars) are copied into intentd's asset root
//! (`<assets_root>/<workspaceId>/<assetId>`, the layout `note.readAsset`
//! resolves) so `workspace-asset://<wsId>/<assetId>` references in imported
//! notes keep working. Finally, app-level electron-store blobs import once
//! per run (non-dry-run only): `config.json` `changeHistory` →
//! `workspace.changeHistory` and `repo-registry.json` `knownRepos` →
//! `repos.known`, both write-only-when-absent (an existing non-empty setting
//! is never clobbered) and `changeHistory` filtered to workspace ids that
//! were imported or already exist in the DB.
//!
//! The run stays low-impact next to a live daemon: all blocking filesystem
//! work (root/directory scans, manifest/note/comment/transcript reads, asset
//! copies) runs on tokio's blocking pool via [`run_blocking`], never on the
//! async runtime workers, and [`run`] yields to the scheduler between
//! workspaces so concurrent RPC traffic is not starved. Store writes remain
//! short per-workspace transactions.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use intent_core::{
    now_epoch_ms, now_iso, parse_iso, AgentId, AgentSession, AgentStatus, AuthorType, Comment,
    CommentAnchor, CommentAnchorType, CommentStatus, CommentType, ContentType, Error, Note, NoteId,
    NoteMetadata, NoteVisibility, TaskMetadata, Workspace,
};
use intent_services::{publish_workspace_created, EventBus};
use intent_store::Store;
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// Settings-table marker written after a successful non-dry-run import so the
/// first-boot hook never re-runs. Value: a JSON string RFC-3339 timestamp.
pub const LEGACY_IMPORT_MARKER_KEY: &str = "import.legacyCompletedAt";

/// Settings-table marker written when a first-boot import run begins and
/// cleared by [`write_completion_marker`]. A boot that finds it (with no
/// completion marker) resumes the interrupted import — even when the DB file
/// already existed — relying on the importer's idempotency (rows already in
/// the DB are skipped, daemon-managed manifests are skipped). Value: a JSON
/// string RFC-3339 timestamp.
pub const LEGACY_IMPORT_PENDING_MARKER_KEY: &str = "import.legacyStartedAt";

/// Settings-table row holding a compact summary of the workspaces that failed
/// to import in the most recent completed run (written by
/// [`persist_failure_summary`]; cleared when a run finishes with no failures).
/// Value: JSON `{"at", "total", "failures": [{"id", "dir", "reason"}, …]}`,
/// capped at [`FAILURE_SUMMARY_CAP`] entries. The documented manual retry
/// path is `intentd import-legacy --force`.
pub const LEGACY_IMPORT_FAILURES_KEY: &str = "import.legacyFailures";

/// Cap on the entries embedded in [`LEGACY_IMPORT_FAILURES_KEY`]; `total`
/// always carries the uncapped count.
const FAILURE_SUMMARY_CAP: usize = 50;

/// Legacy-only `workspace.json` fields intentd does not model — dropped on
/// import (the FE `WorkspaceSchema` extras written next to the §9.1 fields).
const LEGACY_ONLY_FIELDS: &[&str] = &["changesets", "conversationInfo", "timeline"];

/// Manifest marker field written by intentd's `write_workspace_metadata_file`
/// (`workspace.create` / `workspace.duplicate`). A manifest carrying
/// `"managedBy": "intentd"` belongs to a live daemon-managed workspace, not a
/// legacy one, and is skipped unless `--force` is set.
const MANAGED_BY_FIELD: &str = "managedBy";

/// [`MANAGED_BY_FIELD`] value identifying intentd-written manifests.
const MANAGED_BY_INTENTD: &str = "intentd";

/// Skip reason for daemon-managed manifests — an operational skip that never
/// blocks the first-boot completion marker.
const MANAGED_SKIP_REASON: &str = "daemon-managed manifest";

/// Inputs for one import run.
#[derive(Clone, Default)]
pub struct Options {
    /// Legacy roots to scan, in priority order (first occurrence of an id wins).
    pub roots: Vec<PathBuf>,
    /// Report what would happen without writing anything.
    pub dry_run: bool,
    /// Update rows whose id already exists instead of skipping them.
    pub force: bool,
    /// Destination asset root (`<root>/<workspaceId>/<assetId>`, the layout
    /// `note.readAsset` resolves). `None` disables the asset copy.
    pub assets_root: Option<PathBuf>,
    /// Legacy Electron app-level dir holding `config.json` /
    /// `repo-registry.json`. `None` disables the app-level blob import.
    pub app_dir: Option<PathBuf>,
    /// Event bus for `workspace:created` publishes on freshly inserted rows,
    /// so live subscribers (FE clients, the `WatcherRegistry`) learn about
    /// workspaces the importer writes directly through `Store`. `None`
    /// (the CLI path, tests) disables event emission.
    pub event_bus: Option<EventBus>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("roots", &self.roots)
            .field("dry_run", &self.dry_run)
            .field("force", &self.force)
            .field("assets_root", &self.assets_root)
            .field("app_dir", &self.app_dir)
            .field("event_bus", &self.event_bus.is_some())
            .finish()
    }
}

/// Outcome for one candidate workspace directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Inserted (or, on dry-run, would be inserted).
    Imported,
    /// Existing row overwritten via `--force` (or would be, on dry-run).
    Updated,
    /// Not imported; carries the reason.
    Skipped(String),
}

/// Per-workspace note-import counters (part of [`WorkspaceReport`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoteCounts {
    /// Note rows inserted.
    pub imported: usize,
    /// Note ids already present for the workspace (idempotent skip).
    pub skipped: usize,
    /// Files that could not be read or inserted (logged, never fatal).
    pub failed: usize,
}

impl NoteCounts {
    fn total(&self) -> usize {
        self.imported + self.skipped + self.failed
    }
}

/// Per-workspace comment-import counters (part of [`WorkspaceReport`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommentCounts {
    /// Comment rows inserted.
    pub imported: usize,
    /// Comment ids already present (idempotent skip).
    pub skipped: usize,
    /// Malformed entries/files or failed inserts (logged, never fatal).
    pub failed: usize,
    /// Comments files whose note does not exist for the workspace; skipped
    /// whole with a log.
    pub orphaned: usize,
}

impl CommentCounts {
    fn total(&self) -> usize {
        self.imported + self.skipped + self.failed + self.orphaned
    }
}

/// Per-workspace agent-transcript-import counters (part of
/// [`WorkspaceReport`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentCounts {
    /// Agent-session rows inserted.
    pub sessions_imported: usize,
    /// Sessions already present (idempotent skip, matched by id or by the
    /// preserved legacy id).
    pub sessions_skipped: usize,
    /// Files that could not be read/parsed or sessions that failed to insert
    /// (logged, never fatal).
    pub sessions_failed: usize,
    /// Message rows inserted across all imported sessions.
    pub messages_imported: usize,
    /// Malformed message entries dropped before persistence (logged, never
    /// fatal; the rest of the transcript still lands atomically with the
    /// session).
    pub messages_failed: usize,
}

impl AgentCounts {
    fn total(&self) -> usize {
        self.sessions_imported + self.sessions_skipped + self.sessions_failed
    }
}

/// Per-workspace asset-copy counters (part of [`WorkspaceReport`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssetCounts {
    /// Asset files copied into the intentd asset root.
    pub imported: usize,
    /// Files already present at the destination (idempotent skip).
    pub skipped: usize,
    /// Files that could not be read or copied (logged, never fatal).
    pub failed: usize,
}

impl AssetCounts {
    fn total(&self) -> usize {
        self.imported + self.skipped + self.failed
    }
}

/// Once-per-run app-level blob import counters (part of [`Report`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppSettingCounts {
    /// `knownRepos` entries written to the `repos.known` setting.
    pub repos_imported: usize,
    /// An existing non-empty `repos.known` setting was left untouched.
    pub repos_preserved: bool,
    /// `changeHistory` entries written to `workspace.changeHistory`.
    pub history_imported: usize,
    /// `changeHistory` entries dropped because their workspace id is neither
    /// imported nor present in the DB.
    pub history_filtered: usize,
    /// An existing non-empty `workspace.changeHistory` setting was left
    /// untouched.
    pub history_preserved: bool,
    /// Unreadable/malformed source files or failed setting writes (logged,
    /// never fatal).
    pub failed: usize,
}

impl AppSettingCounts {
    fn any(&self) -> bool {
        *self != Self::default()
    }
}

/// Per-workspace line of the final report.
#[derive(Debug, Clone)]
pub struct WorkspaceReport {
    pub id: String,
    pub dir: PathBuf,
    pub outcome: Outcome,
    pub notes: NoteCounts,
    pub comments: CommentCounts,
    pub agents: AgentCounts,
    pub assets: AssetCounts,
}

impl WorkspaceReport {
    fn new(id: impl Into<String>, dir: &Path, outcome: Outcome) -> Self {
        Self {
            id: id.into(),
            dir: dir.to_path_buf(),
            outcome,
            notes: NoteCounts::default(),
            comments: CommentCounts::default(),
            agents: AgentCounts::default(),
            assets: AssetCounts::default(),
        }
    }

    fn skipped(id: impl Into<String>, dir: &Path, reason: impl Into<String>) -> Self {
        Self::new(id, dir, Outcome::Skipped(reason.into()))
    }
}

/// Full report of one run: one entry per candidate workspace directory, plus
/// the once-per-run app-level blob outcome (`None` when no app dir was
/// configured or the run was a dry-run).
#[derive(Debug, Default)]
pub struct Report {
    pub entries: Vec<WorkspaceReport>,
    pub dry_run: bool,
    pub app_settings: Option<AppSettingCounts>,
}

impl Report {
    pub fn imported(&self) -> usize {
        self.count(|o| matches!(o, Outcome::Imported))
    }

    pub fn updated(&self) -> usize {
        self.count(|o| matches!(o, Outcome::Updated))
    }

    pub fn skipped(&self) -> usize {
        self.count(|o| matches!(o, Outcome::Skipped(_)))
    }

    /// Whether any non-operational workspace skip occurred. The CLI and RPC
    /// entry points withhold their completion-marker rewrite on these; the
    /// first-boot hook writes its marker regardless (best-effort) and
    /// persists the failure summary instead.
    pub fn has_compatibility_failures(&self) -> bool {
        self.entries.iter().any(|entry| {
            matches!(
                &entry.outcome,
                Outcome::Skipped(reason) if !is_operational_skip(reason)
            )
        })
    }

    /// Entries whose skip represents an import failure (data did not land),
    /// as opposed to a benign/expected skip — the set logged and persisted to
    /// [`LEGACY_IMPORT_FAILURES_KEY`] by the first-boot hook. Unlike
    /// [`Report::has_compatibility_failures`], store-write failures count:
    /// the summary reports everything a manual retry could recover.
    pub fn failures(&self) -> Vec<(&WorkspaceReport, &str)> {
        self.entries
            .iter()
            .filter_map(|entry| match &entry.outcome {
                Outcome::Skipped(reason) if !is_benign_skip(reason) => {
                    Some((entry, reason.as_str()))
                }
                _ => None,
            })
            .collect()
    }

    /// Total note rows inserted across all workspaces.
    pub fn notes_imported(&self) -> usize {
        self.entries.iter().map(|e| e.notes.imported).sum()
    }

    /// Total comment rows inserted across all workspaces.
    pub fn comments_imported(&self) -> usize {
        self.entries.iter().map(|e| e.comments.imported).sum()
    }

    /// Total agent-session rows inserted across all workspaces.
    pub fn agent_sessions_imported(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.agents.sessions_imported)
            .sum()
    }

    /// Total agent-message rows inserted across all workspaces.
    pub fn agent_messages_imported(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.agents.messages_imported)
            .sum()
    }

    /// Total asset files copied across all workspaces.
    pub fn assets_imported(&self) -> usize {
        self.entries.iter().map(|e| e.assets.imported).sum()
    }

    fn count(&self, pred: impl Fn(&Outcome) -> bool) -> usize {
        self.entries.iter().filter(|e| pred(&e.outcome)).count()
    }

    fn skip(&mut self, id: impl Into<String>, dir: &Path, reason: impl Into<String>) {
        self.entries.push(WorkspaceReport::skipped(id, dir, reason));
    }
}

fn is_operational_skip(reason: &str) -> bool {
    reason == "already in DB"
        || reason == MANAGED_SKIP_REASON
        || reason.starts_with("update failed:")
        || reason.starts_with("insert failed:")
        || reason.starts_with("lookup failed:")
}

/// Skips that are an expected part of normal operation — nothing failed and a
/// retry would change nothing — and therefore stay out of the persisted
/// failure summary. Narrower than [`is_operational_skip`]: store-write
/// failures are operational (they never block the CLI/RPC completion-marker
/// rewrite) but still belong in the failure summary.
fn is_benign_skip(reason: &str) -> bool {
    reason == "already in DB"
        || reason == MANAGED_SKIP_REASON
        || reason == "duplicate id already found under an earlier root"
        || reason == "virtual workspace id"
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = if self.dry_run {
            "legacy import (dry-run):"
        } else {
            "legacy import:"
        };
        writeln!(f, "{label}")?;
        if self.entries.is_empty() {
            writeln!(f, "  (no legacy workspaces found)")?;
        }
        for entry in &self.entries {
            let outcome = match &entry.outcome {
                Outcome::Imported if self.dry_run => "would import".to_string(),
                Outcome::Imported => "imported".to_string(),
                Outcome::Updated if self.dry_run => "would update (force)".to_string(),
                Outcome::Updated => "updated (force)".to_string(),
                Outcome::Skipped(reason) => format!("skipped: {reason}"),
            };
            let notes = if entry.notes.total() > 0 {
                format!(
                    ", notes: {} imported, {} skipped, {} failed",
                    entry.notes.imported, entry.notes.skipped, entry.notes.failed
                )
            } else {
                String::new()
            };
            let comments = if entry.comments.total() > 0 {
                format!(
                    ", comments: {} imported, {} skipped, {} failed, {} orphaned",
                    entry.comments.imported,
                    entry.comments.skipped,
                    entry.comments.failed,
                    entry.comments.orphaned
                )
            } else {
                String::new()
            };
            let agents = if entry.agents.total() > 0 {
                format!(
                    ", agents: {} imported, {} skipped, {} failed, messages: {} imported, {} failed",
                    entry.agents.sessions_imported,
                    entry.agents.sessions_skipped,
                    entry.agents.sessions_failed,
                    entry.agents.messages_imported,
                    entry.agents.messages_failed
                )
            } else {
                String::new()
            };
            let assets = if entry.assets.total() > 0 {
                format!(
                    ", assets: {} imported, {} skipped, {} failed",
                    entry.assets.imported, entry.assets.skipped, entry.assets.failed
                )
            } else {
                String::new()
            };
            writeln!(
                f,
                "  {}  {}{notes}{comments}{agents}{assets} ({})",
                entry.id,
                outcome,
                entry.dir.display()
            )?;
        }
        if let Some(app) = &self.app_settings {
            if app.any() {
                let repos = if app.repos_preserved {
                    "preserved existing".to_string()
                } else {
                    format!("{} imported", app.repos_imported)
                };
                let history = if app.history_preserved {
                    "preserved existing".to_string()
                } else {
                    format!(
                        "{} imported, {} filtered",
                        app.history_imported, app.history_filtered
                    )
                };
                writeln!(
                    f,
                    "  app settings: repos.known {repos}; workspace.changeHistory {history}; {} failed",
                    app.failed
                )?;
            }
        }
        write!(
            f,
            "summary: {} imported, {} updated, {} skipped, {} notes imported, {} comments imported, {} agent sessions imported, {} agent messages imported, {} assets imported",
            self.imported(),
            self.updated(),
            self.skipped(),
            self.notes_imported(),
            self.comments_imported(),
            self.agent_sessions_imported(),
            self.agent_messages_imported(),
            self.assets_imported()
        )
    }
}

/// Default legacy roots. `INTENTD_LEGACY_IMPORT_ROOTS` (PATH-style list —
/// `:`-separated on Unix, `;` on Windows; empty disables the scan)
/// overrides; under a hermetic test harness
/// (`INTENTD_ASSERT_HERMETIC_ROOT`, see STAB-138) with no override the scan is
/// disabled so tests can never read the developer's real `~/intent`.
pub fn default_roots() -> Vec<PathBuf> {
    if let Some(spec) = std::env::var_os("INTENTD_LEGACY_IMPORT_ROOTS") {
        return std::env::split_paths(&spec)
            .filter(|p| !p.as_os_str().is_empty())
            .collect();
    }
    if std::env::var_os("INTENTD_ASSERT_HERMETIC_ROOT").is_some() {
        return Vec::new();
    }
    // `directories` resolves the home dir platform-natively (Known Folder API
    // on Windows), so discovery works even when `HOME` is unset.
    let Some(base) = directories::BaseDirs::new() else {
        return Vec::new();
    };
    let home = base.home_dir();
    vec![
        home.join("intent").join("workspaces"),
        home.join("intent"),
        home.join(".workspaces"),
    ]
}

/// Default legacy Electron app-level dir (the electron-store `userData` dir
/// holding `config.json` / `repo-registry.json`). `INTENTD_LEGACY_APP_DIR`
/// overrides (empty disables); under the hermetic test harness
/// (`INTENTD_ASSERT_HERMETIC_ROOT`) with no override the import is disabled so
/// tests can never read the developer's real app dir.
pub fn default_app_dir() -> Option<PathBuf> {
    if let Some(spec) = std::env::var_os("INTENTD_LEGACY_APP_DIR") {
        if spec.is_empty() {
            return None;
        }
        return Some(PathBuf::from(spec));
    }
    if std::env::var_os("INTENTD_ASSERT_HERMETIC_ROOT").is_some() {
        return None;
    }
    // `BaseDirs::config_dir()` matches Electron's `userData` parent on every
    // platform: `~/Library/Application Support` (macOS), `%APPDATA%`
    // (Windows Roaming), `$XDG_CONFIG_HOME`/`~/.config` (Linux) — and does
    // not require `HOME` on Windows.
    let base = directories::BaseDirs::new()?;
    Some(base.config_dir().join("intent"))
}

/// E2E test seam: while the file named by this env var exists, [`run`] pauses
/// before importing each workspace. Lets the in-flight responsiveness e2e
/// (`tests/uds_legacy_import.rs`) hold the first-boot import mid-run
/// deterministically while it drives RPCs over the live socket, then release
/// it by deleting the file. Unset (the normal case) is a no-op.
const TEST_IMPORT_HOLD_FILE_ENV: &str = "INTENTD_TEST_LEGACY_IMPORT_HOLD_FILE";

/// Scan `opts.roots` in order and import every legacy workspace found. Missing
/// or unreadable roots are skipped silently (the default roots may simply not
/// exist); per-workspace problems are soft and reported as [`Outcome::Skipped`].
/// Each workspace imports inside its own spawned task, so parse/IO/store
/// errors AND panics in one workspace's unit become a logged skip and the run
/// continues with the next workspace. The run is read-only toward the source
/// directories. App-level blobs import once at the end (non-dry-run only,
/// when `opts.app_dir` is set).
pub async fn run(store: &Store, opts: &Options) -> anyhow::Result<Report> {
    let mut report = Report {
        dry_run: opts.dry_run,
        ..Report::default()
    };
    let test_hold_file: Option<PathBuf> = std::env::var_os(TEST_IMPORT_HOLD_FILE_ENV)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from);
    let mut seen: HashSet<String> = HashSet::new();
    for root in &opts.roots {
        // Candidate discovery (directory scan + manifest stat) is blocking
        // filesystem work — run it off the async runtime.
        let candidates: Vec<(PathBuf, PathBuf)> = {
            let root = root.clone();
            run_blocking(move || {
                let Ok(entries) = std::fs::read_dir(&root) else {
                    return Vec::new();
                };
                // `DirEntry::file_type()` does not follow symlinks, so a
                // symlinked directory pointing outside the legacy root is
                // never a candidate.
                let mut dirs: Vec<PathBuf> = entries
                    .filter_map(std::result::Result::ok)
                    .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                    .map(|e| e.path())
                    .collect();
                dirs.sort();
                dirs.into_iter()
                    .filter_map(|dir| {
                        let manifest = dir.join(".workspace").join("workspace.json");
                        is_regular_file(&manifest).then_some((dir, manifest))
                    })
                    .collect()
            })
            .await
        };
        for (dir, manifest) in candidates {
            // Test seam (see [`TEST_IMPORT_HOLD_FILE_ENV`]): pause here while
            // the hold file exists, keeping the run in flight without
            // importing further workspaces.
            if let Some(hold) = &test_hold_file {
                while hold.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
            }
            // Task-join isolation: the whole per-workspace unit (manifest
            // handling, store writes, extras) runs in its own spawned task,
            // so even a panic is contained — it surfaces as a `JoinError`
            // and becomes a skip entry instead of aborting the run.
            let task = {
                let store = store.clone();
                let opts = opts.clone();
                let seen = seen.clone();
                let dir = dir.clone();
                let manifest = manifest.clone();
                tokio::spawn(async move { import_one(&store, &dir, &manifest, &opts, &seen).await })
            };
            match task.await {
                Ok((claimed, entry)) => {
                    if let Some(id) = claimed {
                        seen.insert(id);
                    }
                    report.entries.push(entry);
                }
                // A panicked unit never returns `claimed`, so its id is not
                // recorded as seen — a same-id dir under a later root gets a
                // fresh attempt instead of a duplicate skip (harmless: the
                // importer is idempotent, so it is effectively a free retry).
                Err(e) => {
                    let dir_id = dir
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let reason = if e.is_panic() {
                        format!("import panicked: {}", panic_message(&e.into_panic()))
                    } else {
                        "import task cancelled".to_string()
                    };
                    tracing::warn!(
                        id = %dir_id,
                        dir = %dir.display(),
                        %reason,
                        "legacy workspace import failed; continuing with the next workspace"
                    );
                    report.skip(dir_id, &dir, reason);
                }
            }
            // Yield between workspaces so a long import run never starves
            // live RPC traffic sharing the runtime.
            tokio::task::yield_now().await;
        }
    }
    if !opts.dry_run {
        if let Some(app_dir) = &opts.app_dir {
            let landed: HashSet<String> = report
                .entries
                .iter()
                .filter(|e| matches!(e.outcome, Outcome::Imported | Outcome::Updated))
                .map(|e| e.id.clone())
                .collect();
            report.app_settings = Some(import_app_settings(store, app_dir, &landed).await);
        }
    }
    Ok(report)
}

/// Best-effort human-readable panic payload (`panic!` carries `&str` or
/// `String` in practice).
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// True when `path` is a regular file WITHOUT following symlinks
/// (`symlink_metadata`). The per-workspace importers use this instead of
/// `Path::is_file()` so a hostile symlink under `.workspace/` can never pull
/// arbitrary files from outside the legacy tree into the DB or the asset
/// root.
fn is_regular_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
}

/// Run a blocking closure on tokio's blocking pool so filesystem work never
/// occupies an async runtime worker. Panics are re-raised on the awaiting
/// task, so the per-workspace task-join isolation in [`run`] still turns
/// them into logged skips.
async fn run_blocking<T, F>(f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("blocking import task cancelled: {e}"),
    }
}

/// `std::fs::read_to_string` off the async runtime.
async fn read_file_blocking(path: PathBuf) -> std::io::Result<String> {
    run_blocking(move || std::fs::read_to_string(&path)).await
}

/// Scan `dir` off the async runtime and return its sorted entries that pass
/// `keep`. `None` when the directory cannot be read (a missing dir is a soft
/// no-op for every importer).
async fn scan_dir_blocking<F>(dir: PathBuf, keep: F) -> Option<Vec<PathBuf>>
where
    F: Fn(&Path) -> bool + Send + 'static,
{
    run_blocking(move || {
        let entries = std::fs::read_dir(&dir).ok()?;
        let mut files: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| keep(p))
            .collect();
        files.sort();
        Some(files)
    })
    .await
}

/// True when `id` is exactly one normal path component: no `/` or `\`
/// separators, not `..`/`.`, not absolute, no prefix/root components. Ids
/// failing this are unsafe to join onto the asset root or embed in
/// `workspace-asset://<wsId>/<assetId>` URLs.
fn id_is_single_path_component(id: &str) -> bool {
    use std::path::Component;
    if id.contains('/') || id.contains('\\') {
        return false;
    }
    let mut components = Path::new(id).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

/// Test-only injected panic — lets tests prove the task-join isolation in
/// [`run`] turns a panicking per-workspace unit into a logged skip.
#[cfg(test)]
fn maybe_injected_test_panic(obj: &Map<String, Value>) {
    assert!(!obj.contains_key("__testPanic"), "injected test panic");
}

#[cfg(not(test))]
fn maybe_injected_test_panic(_obj: &Map<String, Value>) {}

/// Import one candidate workspace directory, returning the id to record as
/// seen (`Some` once the manifest id passed the duplicate check — later
/// roots then report duplicates instead of re-importing) plus the report
/// entry. Runs inside its own spawned task (see [`run`]) so a panic anywhere
/// in here is contained to this workspace.
async fn import_one(
    store: &Store,
    dir: &Path,
    manifest: &Path,
    opts: &Options,
    seen: &HashSet<String>,
) -> (Option<String>, WorkspaceReport) {
    // The legacy layout names the workspace dir after its id; used as the
    // report id when the manifest is unusable.
    let dir_id = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let text = match read_file_blocking(manifest.to_path_buf()).await {
        Ok(t) => t,
        Err(e) => {
            return (
                None,
                WorkspaceReport::skipped(dir_id, dir, format!("cannot read workspace.json: {e}")),
            );
        }
    };
    let mut obj = match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(o)) => o,
        Ok(_) => {
            return (
                None,
                WorkspaceReport::skipped(dir_id, dir, "workspace.json is not a JSON object"),
            );
        }
        Err(e) => {
            return (
                None,
                WorkspaceReport::skipped(
                    dir_id,
                    dir,
                    format!("invalid JSON in workspace.json: {e}"),
                ),
            );
        }
    };
    maybe_injected_test_panic(&obj);
    // Prefer the manifest id; fall back to the directory name.
    let id = match obj.get("id").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ if !dir_id.is_empty() => {
            obj.insert("id".to_string(), json!(dir_id.clone()));
            dir_id.clone()
        }
        _ => {
            return (
                None,
                WorkspaceReport::skipped(dir_id, dir, "workspace.json has no id"),
            );
        }
    };
    if id == intent_core::CHIEF_WORKSPACE_ID {
        return (
            None,
            WorkspaceReport::skipped(id, dir, "virtual workspace id"),
        );
    }
    // Reject ids that are not a single normal path component: the id is used
    // verbatim as a DB key, in `workspace-asset://<wsId>/<assetId>` URLs
    // (split on `/`), and joined onto the asset root for the copy — a hostile
    // `../…`/absolute/multi-segment id could otherwise write outside
    // `<data_dir>/assets/`.
    if !id_is_single_path_component(&id) {
        return (
            None,
            WorkspaceReport::skipped(id, dir, "workspace id is not a plain path segment"),
        );
    }
    if seen.contains(&id) {
        return (
            None,
            WorkspaceReport::skipped(id, dir, "duplicate id already found under an earlier root"),
        );
    }
    // Past the duplicate check the id counts as seen regardless of outcome.
    let claimed = Some(id.clone());
    // Manifests intentd wrote itself (`workspace.create` / `.duplicate`) mark
    // live daemon-managed workspaces — a fresh daemon sharing the workspaces
    // root must never adopt them. `--force` remains the wiped-DB recovery path.
    if !opts.force && obj.get(MANAGED_BY_FIELD).and_then(Value::as_str) == Some(MANAGED_BY_INTENTD)
    {
        return (
            claimed,
            WorkspaceReport::skipped(id, dir, MANAGED_SKIP_REASON),
        );
    }
    // `workspace_from_legacy_json` stats the recorded worktree path — run it
    // off the runtime with the rest of the filesystem work.
    let ws = match run_blocking(move || workspace_from_legacy_json(obj)).await {
        Ok(ws) => ws,
        Err(reason) => return (claimed, WorkspaceReport::skipped(id, dir, reason)),
    };
    let outcome = match store.get_workspace(&ws.id).await {
        Ok(_) if !opts.force => Outcome::Skipped("already in DB".to_string()),
        Ok(_) => {
            if opts.dry_run {
                Outcome::Updated
            } else {
                match store.update_workspace(&ws).await {
                    Ok(()) => Outcome::Updated,
                    Err(e) => Outcome::Skipped(format!("update failed: {e}")),
                }
            }
        }
        Err(Error::NotFound(_)) => {
            if opts.dry_run {
                Outcome::Imported
            } else {
                match store.insert_workspace(&ws).await {
                    Ok(()) => Outcome::Imported,
                    Err(e) => Outcome::Skipped(format!("insert failed: {e}")),
                }
            }
        }
        Err(e) => Outcome::Skipped(format!("lookup failed: {e}")),
    };
    let landed = matches!(outcome, Outcome::Imported | Outcome::Updated);
    // A freshly inserted row (not a `--force` overwrite of an existing one)
    // is a new workspace as far as live subscribers are concerned: publish
    // `workspace:created` so connected clients refresh their lists and the
    // `WatcherRegistry` registers the workspace's watch roots at runtime.
    if matches!(outcome, Outcome::Imported) && !opts.dry_run {
        if let Some(bus) = &opts.event_bus {
            publish_workspace_created(bus, &ws).await;
        }
    }
    let mut entry = WorkspaceReport::new(id, dir, outcome);
    if landed && !opts.dry_run {
        import_workspace_extras(store, &ws, dir, opts.assets_root.as_deref(), &mut entry).await;
    }
    (claimed, entry)
}

/// Extension seam for the per-workspace importers: called once per
/// imported/updated workspace with its legacy directory (`<root>/<id>`,
/// containing `.workspace/…`). Imports notes, then comments (after notes, so
/// comments can attach to just-imported note rows), then agent transcripts,
/// then assets (a plain file copy — no store dependency).
async fn import_workspace_extras(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
    assets_root: Option<&Path>,
    entry: &mut WorkspaceReport,
) {
    entry.notes = import_workspace_notes(store, workspace, legacy_dir).await;
    entry.comments = import_workspace_comments(store, workspace, legacy_dir).await;
    entry.agents = import_workspace_agents(store, workspace, legacy_dir).await;
    if let Some(root) = assets_root {
        // Pure filesystem work (scan + copy) — run the whole copy off the
        // async runtime.
        let ws = workspace.clone();
        let dir = legacy_dir.to_path_buf();
        let root = root.to_path_buf();
        entry.assets = run_blocking(move || import_workspace_assets(&ws, &dir, &root)).await;
    }
}

/// Copy legacy `.workspace/assets/{assetId}` files (plus their `.meta.json`
/// sidecars) into intentd's asset root at `<root>/<workspaceId>/<assetId>` —
/// the exact layout `note.readAsset`/`note.saveAsset` use — so
/// `workspace-asset://<wsId>/<assetId>` references in imported notes resolve.
/// Best-effort and idempotent: files already present at the destination are
/// skipped (never overwritten), unreadable/uncopyable files are logged and
/// counted as `failed`, and subdirectories/dotfiles are ignored.
fn import_workspace_assets(workspace: &Workspace, legacy_dir: &Path, root: &Path) -> AssetCounts {
    let mut counts = AssetCounts::default();
    let assets_dir = legacy_dir.join(".workspace").join("assets");
    let Ok(entries) = std::fs::read_dir(&assets_dir) else {
        return counts; // no assets dir — nothing to import
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            is_regular_file(p)
                && !p
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('.'))
        })
        .collect();
    files.sort();
    if files.is_empty() {
        return counts;
    }
    let dest_dir = root.join(workspace.id.as_str());
    if let Err(e) = std::fs::create_dir_all(&dest_dir) {
        tracing::warn!(dir = %dest_dir.display(), error = %e, "legacy assets destination unusable; skipping asset copy");
        counts.failed = files.len();
        return counts;
    }
    for path in files {
        let Some(name) = path.file_name() else {
            counts.failed += 1;
            continue;
        };
        let dest = dest_dir.join(name);
        if dest.exists() {
            counts.skipped += 1;
            continue;
        }
        match std::fs::copy(&path, &dest) {
            Ok(_) => counts.imported += 1,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy asset copy failed; skipping");
                counts.failed += 1;
            }
        }
    }
    counts
}

/// Import legacy `.workspace/notes/*.md` files as note rows. Best-effort and
/// idempotent: note ids already present for the workspace are skipped, every
/// per-file problem is logged and counted as `failed` without ever failing the
/// workspace. The `.meta/` sidecar (versions/CRDT/trash) and dotfiles are
/// skipped entirely.
async fn import_workspace_notes(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
) -> NoteCounts {
    let mut counts = NoteCounts::default();
    let notes_dir = legacy_dir.join(".workspace").join("notes");
    let Some(files) = scan_dir_blocking(notes_dir, |p: &Path| {
        is_regular_file(p)
            && p.extension().is_some_and(|ext| ext == "md")
            && !p
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
    })
    .await
    else {
        return counts; // no notes dir — nothing to import
    };
    for path in files {
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = match read_file_blocking(path.clone()).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy note unreadable; skipping");
                counts.failed += 1;
                continue;
            }
        };
        let note = note_from_legacy_file(workspace, &stem, &text, &path);
        match store.get_note(&workspace.id, &note.id).await {
            Ok(_) => counts.skipped += 1,
            Err(Error::NotFound(_)) => match store.insert_note(&note).await {
                Ok(()) => counts.imported += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), note_id = %note.id, error = %e, "legacy note insert failed");
                    counts.failed += 1;
                }
            },
            Err(e) => {
                tracing::warn!(path = %path.display(), note_id = %note.id, error = %e, "legacy note lookup failed");
                counts.failed += 1;
            }
        }
    }
    counts
}

/// Legacy note YAML frontmatter (the shape the Intent FE wrote to
/// `.workspace/notes/{id}.md`). Unknown keys are ignored; `task` is kept as a
/// raw YAML value so a malformed task block degrades to a plain note instead
/// of failing the whole file.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NoteFrontmatter {
    id: Option<String>,
    title: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    archived: bool,
    visibility: Option<NoteVisibility>,
    #[serde(alias = "parentId")]
    parent: Option<String>,
    created: Option<String>,
    task: Option<serde_yaml::Value>,
}

/// Split `---\n<yaml>\n---\n<body>` frontmatter. Returns `(yaml, body)` or
/// `None` when the text does not start with a frontmatter block.
fn split_frontmatter(text: &str) -> Option<(&str, &str)> {
    let rest = text
        .strip_prefix("---\r\n")
        .or_else(|| text.strip_prefix("---\n"))?;
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            let yaml = &rest[..offset];
            let body = &rest[offset + line.len()..];
            let body = body
                .strip_prefix("\r\n")
                .or_else(|| body.strip_prefix('\n'))
                .unwrap_or(body);
            return Some((yaml, body));
        }
        offset += line.len();
    }
    None
}

/// Build a [`Note`] from one legacy note file. Best-effort: malformed or
/// missing frontmatter degrades to importing the body (or the whole file) with
/// a filename-derived title and defaults; a malformed `task:` block degrades
/// to a plain note. `spec.md` (or frontmatter id `spec`) becomes the
/// workspace's default spec note.
fn note_from_legacy_file(workspace: &Workspace, stem: &str, text: &str, path: &Path) -> Note {
    let (fm, body) = match split_frontmatter(text) {
        Some((yaml, body)) => match serde_yaml::from_str::<NoteFrontmatter>(yaml) {
            Ok(fm) => (fm, body),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy note frontmatter malformed; importing body with filename-derived title");
                (NoteFrontmatter::default(), body)
            }
        },
        None => (NoteFrontmatter::default(), text),
    };
    let id = fm
        .id
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| stem.to_string());
    let task = fm.task.and_then(|v| {
        match serde_yaml::from_value::<TaskMetadata>(v) {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy note task frontmatter malformed; importing as plain note");
                None
            }
        }
    });
    let created_at = fm
        .created
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(now_iso);
    Note {
        id: NoteId::from(id.clone()),
        workspace_id: workspace.id.clone(),
        title: fm
            .title
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| stem.to_string()),
        content: body.to_string(),
        content_type: ContentType::Markdown,
        tags: fm.tags,
        is_pinned: fm.pinned,
        is_archived: fm.archived,
        is_default: id == "spec",
        parent_id: fm.parent.filter(|s| !s.is_empty()).map(NoteId::from),
        visibility: fm.visibility.unwrap_or_default(),
        metadata: NoteMetadata { task },
        created_at: created_at.clone(),
        rev: 0,
        updated_at: created_at,
    }
}

/// Suffix of the legacy per-note comments sidecar
/// (`.workspace/notes/.meta/{noteId}.comments.json`).
const LEGACY_COMMENTS_SUFFIX: &str = ".comments.json";

/// Import legacy `.workspace/notes/.meta/{noteId}.comments.json` files (the FE
/// `NoteCommentsData` shape) as comment rows. Best-effort and idempotent:
/// comment ids already present are skipped, malformed files/entries and failed
/// inserts are logged and counted as `failed`, and files whose note does not
/// exist for the workspace are counted `orphaned` and skipped whole — nothing
/// ever fails the workspace import.
async fn import_workspace_comments(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
) -> CommentCounts {
    let mut counts = CommentCounts::default();
    let meta_dir = legacy_dir.join(".workspace").join("notes").join(".meta");
    let Some(files) = scan_dir_blocking(meta_dir, |p: &Path| {
        is_regular_file(p)
            && p.file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(LEGACY_COMMENTS_SUFFIX))
    })
    .await
    else {
        return counts; // no .meta dir — nothing to import
    };
    for path in files {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let note_id = NoteId::from(name.trim_end_matches(LEGACY_COMMENTS_SUFFIX));
        match store.get_note(&workspace.id, &note_id).await {
            Ok(_) => {}
            Err(Error::NotFound(_)) => {
                tracing::warn!(path = %path.display(), note_id = %note_id, "legacy comments file references missing note; skipping");
                counts.orphaned += 1;
                continue;
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), note_id = %note_id, error = %e, "legacy comments note lookup failed");
                counts.failed += 1;
                continue;
            }
        }
        let text = match read_file_blocking(path.clone()).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy comments file unreadable; skipping");
                counts.failed += 1;
                continue;
            }
        };
        let parsed: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy comments file is not valid JSON; skipping");
                counts.failed += 1;
                continue;
            }
        };
        let Some(list) = parsed.get("comments").and_then(Value::as_array) else {
            tracing::warn!(path = %path.display(), "legacy comments file has no `comments` array; skipping");
            counts.failed += 1;
            continue;
        };
        for raw in list {
            let Some(obj) = raw.as_object() else {
                tracing::warn!(path = %path.display(), "legacy comment entry is not an object; skipping");
                counts.failed += 1;
                continue;
            };
            let (comment, extra) = match comment_from_legacy_json(&note_id, obj.clone()) {
                Ok(pair) => pair,
                Err(reason) => {
                    tracing::warn!(path = %path.display(), reason, "legacy comment entry malformed; skipping");
                    counts.failed += 1;
                    continue;
                }
            };
            match store.get_comment(&comment.id).await {
                Ok(_) => counts.skipped += 1,
                Err(Error::NotFound(_)) => {
                    match store
                        .insert_comment_with_extras(&workspace.id, &comment, &extra)
                        .await
                    {
                        Ok(()) => counts.imported += 1,
                        Err(e) => {
                            tracing::warn!(path = %path.display(), comment_id = %comment.id, error = %e, "legacy comment insert failed");
                            counts.failed += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), comment_id = %comment.id, error = %e, "legacy comment lookup failed");
                    counts.failed += 1;
                }
            }
        }
    }
    counts
}

/// Map one legacy `NoteComment` JSON object (FE `comment.types.ts`) to a
/// [`Comment`] plus the leftover legacy fields destined for `extra_json`.
/// Known fields are consumed (removed) as they are mapped; whatever remains —
/// `lineStart`/`lineEnd`, `from`/`to`, `tags`, `reactions`, unknown keys — is
/// preserved verbatim so no source data is dropped. Thread structure keeps
/// `threadId` (defaulting to the FE's `thread-{id}` convention) and
/// `parentId`; `markId` becomes a point anchor and `section` the anchor text.
fn comment_from_legacy_json(
    note_id: &NoteId,
    mut obj: Map<String, Value>,
) -> Result<(Comment, Map<String, Value>), String> {
    fn take_string(obj: &mut Map<String, Value>, key: &str) -> Option<String> {
        match obj.get(key) {
            Some(Value::String(_)) => match obj.remove(key) {
                Some(Value::String(s)) if !s.is_empty() => Some(s),
                _ => None,
            },
            Some(Value::Null) => {
                obj.remove(key);
                None
            }
            // Wrong-typed values stay behind so they land in `extra_json`.
            _ => None,
        }
    }
    fn take_enum<T: serde::de::DeserializeOwned + Default>(
        obj: &mut Map<String, Value>,
        key: &str,
    ) -> Result<T, String> {
        match obj.remove(key) {
            None | Some(Value::Null) => Ok(T::default()),
            Some(v) => serde_json::from_value(v).map_err(|e| format!("bad `{key}`: {e}")),
        }
    }

    let id = take_string(&mut obj, "id").ok_or("missing `id`")?;
    // FE convention: comments without an explicit thread root their own.
    let thread_id = take_string(&mut obj, "threadId").unwrap_or_else(|| format!("thread-{id}"));
    let kind: CommentType = take_enum(&mut obj, "type")?;
    let status: CommentStatus = take_enum(&mut obj, "status")?;
    let author_type: AuthorType = take_enum(&mut obj, "authorType")?;
    let content = take_string(&mut obj, "content").unwrap_or_default();
    let author = take_string(&mut obj, "author").unwrap_or_default();
    let parent_id = take_string(&mut obj, "parentId");
    let created_at = take_string(&mut obj, "createdAt").unwrap_or_else(now_iso);
    let updated_at = take_string(&mut obj, "updatedAt").unwrap_or_else(|| created_at.clone());
    let anchor_text = take_string(&mut obj, "section");
    let agent_id = take_string(&mut obj, "agentId").map(AgentId::from);
    let is_orphaned = match obj.remove("isOrphaned") {
        Some(Value::Bool(b)) => Some(b),
        None | Some(Value::Null) => None,
        // Wrong-typed values stay behind so they land in `extra_json`.
        Some(other) => {
            obj.insert("isOrphaned".to_string(), other);
            None
        }
    };
    // The sidecar duplicates the note id per entry; the file location is
    // authoritative, so drop it rather than persisting a stale copy.
    obj.remove("noteId");

    // `markId` is the legacy anchor-mark id → point anchor. Without one the
    // exact offsets (`from`/`to`) stay in `extra_json` and no anchor is
    // stored (the wire omits the field, monorepo#729).
    let anchor = take_string(&mut obj, "markId").map(|mark_id| CommentAnchor {
        kind: CommentAnchorType::Point,
        point_id: Some(mark_id),
        ..Default::default()
    });

    let (suggestion_original, suggestion_proposed) = match obj.remove("suggestionDiff") {
        Some(Value::Object(mut diff)) => {
            let original = match diff.remove("original") {
                Some(Value::String(s)) => Some(s),
                Some(other) => {
                    diff.insert("original".to_string(), other);
                    None
                }
                None => None,
            };
            let proposed = match diff.remove("proposed") {
                Some(Value::String(s)) => Some(s),
                Some(other) => {
                    diff.insert("proposed".to_string(), other);
                    None
                }
                None => None,
            };
            if !diff.is_empty() {
                // Keep the unmapped diff fields (lineStart/lineEnd, …).
                obj.insert("suggestionDiff".to_string(), Value::Object(diff));
            }
            (original, proposed)
        }
        Some(other) => {
            obj.insert("suggestionDiff".to_string(), other);
            (None, None)
        }
        None => (None, None),
    };

    let comment = Comment {
        id,
        thread_id,
        note_id: Some(note_id.clone()),
        kind,
        content,
        author,
        author_type,
        status,
        parent_id,
        anchor,
        anchor_text,
        anchor_before: None,
        anchor_after: None,
        suggestion_original,
        suggestion_proposed,
        agent_id,
        is_orphaned,
        created_at,
        updated_at,
    };
    Ok((comment, obj))
}

/// Content-block `type`s intentd understands (the FE `ContentBlock` union in
/// `content-block.ts`). Blocks with any other/missing `type` degrade to text
/// blocks on import so the transcript stays readable.
const KNOWN_BLOCK_TYPES: &[&str] = &[
    "text",
    "code",
    "tool_use",
    "tool_result",
    "thinking",
    "image",
    "audio",
    "file",
];

/// Validate an `agent-{uuid}` id (mirrors the TS `agentIdPattern` and the
/// service-layer check). Legacy files carrying any other id shape get a fresh
/// server-minted id, with the original preserved in the session metadata.
fn is_valid_agent_id(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("agent-") else {
        return false;
    };
    let groups = [8usize, 4, 4, 4, 12];
    let parts: Vec<&str> = rest.split('-').collect();
    parts.len() == groups.len()
        && parts
            .iter()
            .zip(groups.iter())
            .all(|(part, &len)| part.len() == len && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Import legacy `.workspace/agents/{agentId}.json` files (the FE
/// `AgentSession` shape) as `agent_session` + ordered `agent_message` rows.
/// Best-effort and idempotent: sessions already present (by id, or by the
/// legacy id preserved under `metadata.legacyImport.originalId` when a new id
/// was minted) are skipped whole; malformed files and failed inserts are
/// logged and counted without ever failing the workspace import. Each session
/// and its full transcript persist in ONE store transaction
/// ([`Store::insert_agent_session_with_messages`]) — all-or-nothing, so a
/// crash or insert failure mid-transcript leaves no session row behind and
/// the re-run imports it cleanly. Imported sessions are always terminal
/// (`Completed`, not active) so the startup interrupted-agent heal sweep
/// never resumes them.
async fn import_workspace_agents(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
) -> AgentCounts {
    let mut counts = AgentCounts::default();
    let agents_dir = legacy_dir.join(".workspace").join("agents");
    // Sidecar artifacts (`*.json.checksum`, `*.json.corrupted.{ts}`, backups,
    // `.health-check`) fail the extension/dotfile filter and are ignored.
    let Some(files) = scan_dir_blocking(agents_dir, |p: &Path| {
        is_regular_file(p)
            && p.extension().is_some_and(|ext| ext == "json")
            && !p
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
    })
    .await
    else {
        return counts; // no agents dir — nothing to import
    };
    if files.is_empty() {
        return counts;
    }
    // Existing sessions for the workspace: ids plus preserved legacy ids, so
    // re-runs skip sessions that landed under a minted id.
    let mut existing: HashSet<String> = HashSet::new();
    match store.list_agent_session_summaries(&workspace.id).await {
        Ok(sessions) => {
            for s in sessions {
                if let Some(orig) = s
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("legacyImport"))
                    .and_then(|l| l.get("originalId"))
                    .and_then(Value::as_str)
                {
                    existing.insert(orig.to_string());
                }
                existing.insert(s.id.0);
            }
        }
        Err(e) => {
            tracing::warn!(workspace_id = %workspace.id, error = %e, "legacy agents: listing existing sessions failed; skipping agent import");
            counts.sessions_failed += files.len();
            return counts;
        }
    }
    for path in files {
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = match read_file_blocking(path.clone()).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy agent file unreadable; skipping");
                counts.sessions_failed += 1;
                continue;
            }
        };
        let obj = match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(o)) => o,
            Ok(_) => {
                tracing::warn!(path = %path.display(), "legacy agent file is not a JSON object; skipping");
                counts.sessions_failed += 1;
                continue;
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "legacy agent file is not valid JSON; skipping");
                counts.sessions_failed += 1;
                continue;
            }
        };
        let (session, messages) = match session_from_legacy_json(workspace, &stem, obj) {
            Ok(pair) => pair,
            Err(reason) => {
                tracing::warn!(path = %path.display(), reason, "legacy agent session malformed; skipping");
                counts.sessions_failed += 1;
                continue;
            }
        };
        let original_id = session
            .metadata
            .as_ref()
            .and_then(|m| m.get("legacyImport"))
            .and_then(|l| l.get("originalId"))
            .and_then(Value::as_str)
            .unwrap_or(session.id.as_str())
            .to_string();
        if existing.contains(&original_id) || existing.contains(session.id.as_str()) {
            counts.sessions_skipped += 1;
            continue;
        }
        // Guard against the id living in another workspace (ids are global).
        match store.get_agent_session_status(&session.id).await {
            Err(Error::NotFound(_)) => {}
            Ok(_) => {
                counts.sessions_skipped += 1;
                continue;
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), agent_id = %session.id, error = %e, "legacy agent session lookup failed");
                counts.sessions_failed += 1;
                continue;
            }
        }
        if session.id.as_str() != original_id {
            tracing::info!(path = %path.display(), original_id, minted_id = %session.id, "legacy agent id invalid; minted a new id (original preserved in metadata)");
        }
        // Parse the whole transcript up front (malformed entries are dropped
        // and counted), then persist session + messages in ONE store
        // transaction: a partial transcript can never land, so the
        // session-id-presence idempotency check stays safe across re-runs.
        let mut parsed: Vec<(String, Value, Option<Value>, String)> = Vec::new();
        let mut malformed = 0usize;
        for raw in messages {
            match message_from_legacy_json(raw) {
                Ok(parts) => parsed.push(parts),
                Err(reason) => {
                    tracing::warn!(path = %path.display(), agent_id = %session.id, reason, "legacy agent message malformed; skipping");
                    malformed += 1;
                }
            }
        }
        let rows: Vec<intent_store::ReplaceMessage<'_>> = parsed
            .iter()
            .map(
                |(role, content, metadata, created_at)| intent_store::ReplaceMessage {
                    role,
                    content,
                    metadata: metadata.as_ref(),
                    created_at,
                },
            )
            .collect();
        if let Err(e) = store
            .insert_agent_session_with_messages(&session, &rows)
            .await
        {
            tracing::warn!(path = %path.display(), agent_id = %session.id, error = %e, "legacy agent session insert failed");
            counts.sessions_failed += 1;
            counts.messages_failed += malformed;
            continue;
        }
        existing.insert(original_id);
        existing.insert(session.id.0.clone());
        counts.sessions_imported += 1;
        counts.messages_imported += parsed.len();
        counts.messages_failed += malformed;
    }
    counts
}

/// Map one legacy `AgentSession` JSON object (FE `agent-session.ts`) to an
/// [`AgentSession`] plus its raw legacy `messages` array. The session is
/// always imported as a terminal historical record: status `Completed`, not
/// active, no ACP session handle — the startup heal sweep (which only touches
/// `Active`/`Processing`/`Waiting`) never sees it. Legacy fields intentd has
/// no column for (`lastActivity`, `startedAt`/`endedAt`, fork lineage,
/// `agentMetadata`, `digest`, …) are preserved under
/// `metadata.legacyImport`, next to the verbatim legacy `metadata` object.
fn session_from_legacy_json(
    workspace: &Workspace,
    stem: &str,
    mut obj: Map<String, Value>,
) -> Result<(AgentSession, Vec<Value>), String> {
    fn lift_string(m: &mut Map<String, Value>, key: &str) -> Option<String> {
        match m.get(key) {
            Some(Value::String(_)) => match m.remove(key) {
                Some(Value::String(s)) if !s.is_empty() => Some(s),
                _ => None,
            },
            _ => None,
        }
    }
    fn take_string(obj: &mut Map<String, Value>, key: &str) -> Option<String> {
        match obj.remove(key) {
            Some(Value::String(s)) if !s.is_empty() => Some(s),
            Some(other) if !other.is_null() => {
                obj.insert(key.to_string(), other); // wrong type → legacyImport
                None
            }
            _ => None,
        }
    }
    fn take_bool(obj: &mut Map<String, Value>, key: &str) -> bool {
        match obj.remove(key) {
            Some(Value::Bool(b)) => b,
            Some(other) if !other.is_null() => {
                obj.insert(key.to_string(), other);
                false
            }
            _ => false,
        }
    }

    let original_id = take_string(&mut obj, "id").unwrap_or_else(|| stem.to_string());
    if original_id.is_empty() {
        return Err("missing `id` and empty filename".to_string());
    }
    let id = if is_valid_agent_id(&original_id) {
        AgentId::from(original_id.clone())
    } else {
        AgentId::from(format!("agent-{}", AgentId::new()))
    };
    let name = take_string(&mut obj, "name").unwrap_or_else(|| stem.to_string());
    let name_explicitly_set = take_bool(&mut obj, "nameExplicitlySet");
    let model = take_string(&mut obj, "model");
    // 'acp' is the protocol name, not a provider id — treat it as unset
    // (mirrors the FE `getAgentProvider`).
    let provider = take_string(&mut obj, "provider").filter(|p| p != "acp");
    let system_prompt = take_string(&mut obj, "systemPrompt");
    let backend_session_id = take_string(&mut obj, "backendSessionId").map(AgentId::from);
    let parent_agent_id = take_string(&mut obj, "parentSessionId").map(AgentId::from);
    let is_background = take_bool(&mut obj, "isBackground");
    let now = now_iso();
    let created_at = take_string(&mut obj, "createdAt").unwrap_or_else(|| now.clone());
    let updated_at = take_string(&mut obj, "updatedAt").unwrap_or_else(|| created_at.clone());
    let messages = match obj.remove("messages") {
        Some(Value::Array(list)) => list,
        None | Some(Value::Null) => Vec::new(),
        Some(_) => return Err("`messages` is not an array".to_string()),
    };
    let original_status = obj.remove("status");
    // The verbatim legacy `metadata` object rides along at the top level of
    // the persisted metadata; behavior fields intentd models as columns are
    // lifted out of it.
    let mut metadata = match obj.remove("metadata") {
        Some(Value::Object(m)) => m,
        Some(other) if !other.is_null() => {
            let mut m = Map::new();
            m.insert("legacyMetadata".to_string(), other);
            m
        }
        _ => Map::new(),
    };
    let specialist = lift_string(&mut metadata, "specialist");
    let task_note_id = lift_string(&mut metadata, "taskNoteId").map(NoteId::from);
    let completion_report = lift_string(&mut metadata, "completionReport");
    let completion_report_timestamp = lift_string(&mut metadata, "completionReportTimestamp");
    let initial_message = lift_string(&mut metadata, "initialMessage");
    let delegation_depth = match metadata.get("delegationDepth") {
        Some(Value::Number(n)) if n.is_i64() => {
            let v = n.as_i64();
            metadata.remove("delegationDepth");
            v
        }
        _ => None,
    };
    // Everything left in the legacy object (lastActivity, startedAt/endedAt,
    // fork lineage, agentMetadata, digest, runtime/UI state, …) is preserved
    // verbatim under `legacyImport`, alongside the import provenance fields.
    let mut legacy_import = obj;
    legacy_import.insert("originalId".to_string(), json!(original_id));
    if let Some(status) = original_status {
        legacy_import.insert("originalStatus".to_string(), status);
    }
    legacy_import.insert("importedAt".to_string(), json!(now.clone()));
    metadata.insert("legacyImport".to_string(), Value::Object(legacy_import));

    let session = AgentSession {
        // Imported sessions predate harness stamping, so they get the same
        // backfill values migration 0096 gives pre-existing rows: literal
        // "1.0" (NOT the current constant — a future bump must not relabel
        // legacy imports) and a NULL features snapshot (projections overlay
        // current settings on read).
        harness_version: "1.0".to_string(),
        harness_features: None,
        id,
        workspace_id: workspace.id.clone(),
        parent_agent_id,
        backend_session_id,
        acp_session_id: None,
        name,
        name_explicitly_set,
        model,
        reasoning_effort: None,
        effort_levels: None,
        provider,
        system_prompt,
        specialist,
        status: AgentStatus::Completed,
        is_active: false,
        messages: Vec::new(),
        stats: None,
        task_note_id,
        skip_auto_commit: false,
        completion_report,
        completion_report_timestamp,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth,
        initial_message,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        is_background,
        metadata: Some(Value::Object(metadata)),
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        created_at,
        updated_at,
    };
    Ok((session, messages))
}

/// Map one legacy `AgentMessage` JSON entry (FE `agent-message.ts`) to the
/// `(role, contentBlocks, metadata, createdAt)` tuple intentd persists.
/// Legacy role `error` becomes `system` (recorded as `legacyRole`); content
/// blocks with an unknown shape degrade to text blocks; legacy-only fields
/// (`toolCalls`/`toolResults`/`turnNumber`/`error`/…) are folded into the row
/// metadata so nothing is dropped. Entries that are not objects or carry no
/// usable role are malformed and skipped by the caller.
#[allow(clippy::type_complexity)]
fn message_from_legacy_json(raw: Value) -> Result<(String, Value, Option<Value>, String), String> {
    let Value::Object(mut obj) = raw else {
        return Err("message entry is not an object".to_string());
    };
    let legacy_role = match obj.remove("role") {
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => return Err("missing `role`".to_string()),
    };
    let role = match legacy_role.as_str() {
        "user" | "assistant" | "system" | "tool" => legacy_role.clone(),
        "error" => "system".to_string(),
        other => return Err(format!("unknown role `{other}`")),
    };
    let content = match obj.remove("contentBlocks") {
        Some(Value::Array(blocks)) => Value::Array(
            blocks
                .into_iter()
                .map(|b| {
                    let known = b
                        .as_object()
                        .and_then(|o| o.get("type"))
                        .and_then(Value::as_str)
                        .is_some_and(|t| KNOWN_BLOCK_TYPES.contains(&t));
                    if known {
                        b
                    } else {
                        // Unknown block shape → best-effort text block.
                        let text = match &b {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        json!({"type": "text", "text": text})
                    }
                })
                .collect(),
        ),
        // No blocks: an error-only message still lands as readable text.
        _ => match obj.get("error").and_then(Value::as_str) {
            Some(err) => json!([{"type": "text", "text": err}]),
            None => json!([]),
        },
    };
    let created_at = match obj.remove("timestamp") {
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => now_iso(),
    };
    // Fold the legacy metadata object plus every remaining legacy-only field
    // (id, toolCalls, toolResults, turnNumber, error, errorCode, streaming
    // flags, …) into the persisted row metadata.
    let mut metadata = match obj.remove("metadata") {
        Some(Value::Object(m)) => m,
        Some(other) if !other.is_null() => {
            let mut m = Map::new();
            m.insert("legacyMetadata".to_string(), other);
            m
        }
        _ => Map::new(),
    };
    if legacy_role != role {
        metadata.insert("legacyRole".to_string(), json!(legacy_role));
    }
    if let Some(id) = obj.remove("id") {
        metadata.insert("legacyMessageId".to_string(), id);
    }
    if !obj.is_empty() {
        metadata.insert("legacyFields".to_string(), Value::Object(obj));
    }
    let metadata = if metadata.is_empty() {
        None
    } else {
        Some(Value::Object(metadata))
    };
    Ok((role, content, metadata, created_at))
}

/// Build a [`Workspace`] from a legacy `workspace.json` object: drop the
/// legacy-only FE fields and the [`MANAGED_BY_FIELD`] marker, default the
/// intentd-only required fields, and apply the worktree fallback (a
/// `worktreePath` that no longer exists on disk is cleared and the workspace
/// becomes `skipWorktree`; `branch` is kept as-is).
fn workspace_from_legacy_json(mut obj: Map<String, Value>) -> Result<Workspace, String> {
    for key in LEGACY_ONLY_FIELDS {
        obj.remove(*key);
    }
    obj.remove(MANAGED_BY_FIELD);
    let now = now_iso();
    for (key, default) in [
        ("title", json!("")),
        ("branch", json!("")),
        ("status", json!("Active")),
        ("activity", json!("idle")),
        ("attention", json!("none")),
        ("createdAt", json!(now.clone())),
        ("updatedAt", json!(now)),
        ("tags", json!([])),
        ("skipWorktree", json!(false)),
        ("isRemote", json!(false)),
        ("archived", json!(false)),
    ] {
        obj.entry(key).or_insert(default);
    }
    if let Some(Value::String(script)) = obj.get("setupScript").cloned() {
        let updated_at = obj
            .get("updatedAt")
            .and_then(|value| match value {
                Value::Number(n) => n.as_u64(),
                Value::String(s) => s.parse().ok().or_else(|| {
                    parse_iso(s)
                        .and_then(|dt| u64::try_from(dt.unix_timestamp_nanos() / 1_000_000).ok())
                }),
                _ => None,
            })
            .unwrap_or_else(now_epoch_ms);
        obj.insert(
            "setupScript".to_string(),
            json!({ "script": script, "updatedAt": updated_at }),
        );
    }
    let mut ws: Workspace = serde_json::from_value(Value::Object(obj))
        .map_err(|e| format!("workspace.json parse failed: {e}"))?;
    if let Some(path) = &ws.worktree_path {
        if !Path::new(path).exists() {
            ws.worktree_path = None;
            ws.skip_worktree = true;
        }
    }
    Ok(ws)
}

/// Settings key backing the FE repo registry (see
/// `intent-services::settings_registry`, default `[]`).
const REPOS_KNOWN_KEY: &str = "repos.known";

/// Settings key backing the FE persisted change history (default `{}`).
const CHANGE_HISTORY_KEY: &str = "workspace.changeHistory";

/// True when a raw settings value is absent or semantically empty (null /
/// empty array / empty object / empty string) — i.e. safe to overwrite
/// without clobbering user data. A present-but-unparseable value counts as
/// NON-empty: it may be user data in a shape we don't understand, so the
/// import preserves it.
fn setting_is_empty(raw: Option<&str>) -> bool {
    let Some(raw) = raw else {
        return true;
    };
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::Null) => true,
        Ok(Value::Array(a)) => a.is_empty(),
        Ok(Value::Object(o)) => o.is_empty(),
        Ok(Value::String(s)) => s.is_empty(),
        Ok(_) | Err(_) => false,
        // Unparseable → treat as PRESENT (preserve): the importer must stay
        // strictly non-clobbering, even toward malformed existing values.
    }
}

/// Read `<dir>/<name>` as a JSON object, distinguishing "file absent" (`None`,
/// silently fine — the legacy store may simply never have been created) from
/// "present but unreadable/malformed" (`Err`, counted as a failure).
async fn read_json_object(
    dir: &Path,
    name: &'static str,
) -> Result<Option<Map<String, Value>>, String> {
    let path = dir.join(name);
    run_blocking(move || {
        if !is_regular_file(&path) {
            return Ok(None);
        }
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("cannot read {name}: {e}"))?;
        match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(o)) => Ok(Some(o)),
            Ok(_) => Err(format!("{name} is not a JSON object")),
            Err(e) => Err(format!("invalid JSON in {name}: {e}")),
        }
    })
    .await
}

/// Import the app-level electron-store blobs once per run:
/// `repo-registry.json` `knownRepos` → the `repos.known` setting and
/// `config.json` `changeHistory` → `workspace.changeHistory`. Both writes are
/// **write-only-when-absent**: an existing non-empty setting is preserved
/// untouched. `changeHistory` entries are filtered to workspace ids that
/// landed this run (`landed`) or already exist in the DB, so the setting never
/// references unknown workspaces. Best-effort throughout: unreadable files and
/// failed writes are logged and counted, never fatal.
async fn import_app_settings(
    store: &Store,
    app_dir: &Path,
    landed: &HashSet<String>,
) -> AppSettingCounts {
    let mut counts = AppSettingCounts::default();

    // repos.known ← repo-registry.json `knownRepos`
    match read_json_object(app_dir, "repo-registry.json").await {
        Ok(Some(obj)) => {
            if let Some(Value::Array(repos)) = obj.get("knownRepos") {
                if !repos.is_empty() {
                    match store.get_setting(REPOS_KNOWN_KEY).await {
                        Ok(existing) if !setting_is_empty(existing.as_deref()) => {
                            counts.repos_preserved = true;
                        }
                        Ok(_) => match store
                            .set_setting(REPOS_KNOWN_KEY, &Value::Array(repos.clone()).to_string())
                            .await
                        {
                            Ok(()) => counts.repos_imported = repos.len(),
                            Err(e) => {
                                tracing::warn!(error = %e, "legacy repos.known write failed");
                                counts.failed += 1;
                            }
                        },
                        Err(e) => {
                            tracing::warn!(error = %e, "legacy repos.known read failed; skipping");
                            counts.failed += 1;
                        }
                    }
                }
            }
        }
        Ok(None) => {}
        Err(reason) => {
            tracing::warn!(dir = %app_dir.display(), %reason, "legacy repo-registry.json unusable");
            counts.failed += 1;
        }
    }

    // workspace.changeHistory ← config.json `changeHistory`
    match read_json_object(app_dir, "config.json").await {
        Ok(Some(obj)) => {
            if let Some(Value::Object(history)) = obj.get("changeHistory") {
                if !history.is_empty() {
                    let mut kept = Map::new();
                    for (ws_id, chunks) in history {
                        let known = landed.contains(ws_id)
                            || store
                                .get_workspace(&intent_core::WorkspaceId::from(ws_id.as_str()))
                                .await
                                .is_ok();
                        if known {
                            kept.insert(ws_id.clone(), chunks.clone());
                        } else {
                            counts.history_filtered += 1;
                        }
                    }
                    if !kept.is_empty() {
                        match store.get_setting(CHANGE_HISTORY_KEY).await {
                            Ok(existing) if !setting_is_empty(existing.as_deref()) => {
                                counts.history_preserved = true;
                            }
                            Ok(_) => match store
                                .set_setting(
                                    CHANGE_HISTORY_KEY,
                                    &Value::Object(kept.clone()).to_string(),
                                )
                                .await
                            {
                                Ok(()) => counts.history_imported = kept.len(),
                                Err(e) => {
                                    tracing::warn!(error = %e, "legacy workspace.changeHistory write failed");
                                    counts.failed += 1;
                                }
                            },
                            Err(e) => {
                                tracing::warn!(error = %e, "legacy workspace.changeHistory read failed; skipping");
                                counts.failed += 1;
                            }
                        }
                    }
                }
            }
        }
        Ok(None) => {}
        Err(reason) => {
            tracing::warn!(dir = %app_dir.display(), %reason, "legacy config.json unusable");
            counts.failed += 1;
        }
    }

    counts
}

/// Write the [`LEGACY_IMPORT_MARKER_KEY`] settings row (a JSON string
/// timestamp) recording that a full import completed successfully, and clear
/// any [`LEGACY_IMPORT_PENDING_MARKER_KEY`] left by a first-boot run.
/// Pending-marker cleanup is best-effort: the first-boot gating checks the
/// completion marker first, so a stale pending marker is harmless.
pub async fn write_completion_marker(store: &Store) -> anyhow::Result<()> {
    store
        .set_setting(LEGACY_IMPORT_MARKER_KEY, &json!(now_iso()).to_string())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if let Err(e) = store.delete_setting(LEGACY_IMPORT_PENDING_MARKER_KEY).await {
        tracing::warn!(error = %e, "legacy import pending marker cleanup failed");
    }
    Ok(())
}

/// Persist a compact summary of `report`'s failures (see
/// [`Report::failures`]) under [`LEGACY_IMPORT_FAILURES_KEY`] — or clear the
/// row when the run had none, so a clean re-run (e.g. `intentd import-legacy
/// --force`) erases stale failure records. Best-effort: a settings write
/// failure is logged and swallowed.
pub async fn persist_failure_summary(store: &Store, report: &Report) {
    let failures = report.failures();
    if failures.is_empty() {
        if let Err(e) = store.delete_setting(LEGACY_IMPORT_FAILURES_KEY).await {
            tracing::warn!(error = %e, "legacy import failure summary cleanup failed");
        }
        return;
    }
    let entries: Vec<Value> = failures
        .iter()
        .take(FAILURE_SUMMARY_CAP)
        .map(|(entry, reason)| {
            json!({
                "id": entry.id,
                "dir": entry.dir.display().to_string(),
                "reason": reason,
            })
        })
        .collect();
    let summary = json!({
        "at": now_iso(),
        "total": failures.len(),
        "failures": entries,
    });
    if let Err(e) = store
        .set_setting(LEGACY_IMPORT_FAILURES_KEY, &summary.to_string())
        .await
    {
        tracing::warn!(error = %e, "legacy import failure summary write failed");
    }
}

/// Outcome of [`decide_first_boot_import`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstBootDecision {
    /// Fresh DB with no markers: import (the pending marker was just written).
    Start,
    /// A pending marker from an interrupted run: resume the import.
    Resume,
    /// Nothing to do (completion marker present, pre-existing DB with no
    /// pending marker, empty roots, or a marker read failed).
    Skip,
}

/// Synchronous first-boot eligibility decision for `intentd serve`, made at
/// startup before any transport comes up. Cheap (two settings reads); the
/// import itself is [`run_first_boot_import`], which the caller runs in a
/// spawned background task so startup never waits on it.
///
/// Rules, in order:
/// - Empty `roots` (e.g. `INTENTD_LEGACY_IMPORT_ROOTS=""` or the hermetic
///   test harness) disable the hook entirely — nothing is read or written.
/// - A completion marker means a prior run finished: skip.
/// - A pending marker means a prior run was interrupted (crash, kill,
///   shutdown abort): resume, even when the DB file pre-existed this boot.
/// - Otherwise import only on a truly fresh DB, persisting the pending
///   marker BEFORE the run starts so a kill at any point resumes on the next
///   boot. A failed pending-marker write is logged and the import still runs
///   (only resumability is lost).
pub async fn decide_first_boot_import(
    store: &Store,
    db_existed: bool,
    roots: &[PathBuf],
) -> FirstBootDecision {
    if roots.is_empty() {
        return FirstBootDecision::Skip;
    }
    match store.get_setting(LEGACY_IMPORT_MARKER_KEY).await {
        Ok(None) => {}
        Ok(Some(_)) => return FirstBootDecision::Skip,
        Err(e) => {
            tracing::warn!(error = %e, "legacy import marker read failed; skipping import");
            return FirstBootDecision::Skip;
        }
    }
    match store.get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY).await {
        Ok(Some(_)) => return FirstBootDecision::Resume,
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(error = %e, "legacy import pending marker read failed; skipping import");
            return FirstBootDecision::Skip;
        }
    }
    if db_existed {
        return FirstBootDecision::Skip;
    }
    if let Err(e) = store
        .set_setting(
            LEGACY_IMPORT_PENDING_MARKER_KEY,
            &json!(now_iso()).to_string(),
        )
        .await
    {
        tracing::warn!(error = %e, "legacy import pending marker write failed; import will not resume if interrupted");
    }
    FirstBootDecision::Start
}

/// First-boot import body for `intentd serve` — the background half of the
/// hook gated by [`decide_first_boot_import`]. Runs in a spawned task
/// concurrently with the transports coming up, so a large legacy tree never
/// delays startup; an interrupt at any point (crash, kill, shutdown abort)
/// leaves the pending marker in place and the next boot resumes (idempotent:
/// rows already in the DB are skipped). Never fails the daemon — every
/// failure is logged and swallowed. Completion is best-effort: once the run
/// itself completes, the completion marker is written (and the pending
/// marker cleared) even when individual workspaces failed — a single bad
/// workspace must never wedge the hook into a permanent boot-time retry
/// loop. Failed entries are logged with reasons and summarized under
/// [`LEGACY_IMPORT_FAILURES_KEY`]; `intentd import-legacy --force` is the
/// documented manual retry path. Only a run-level error (the scan itself
/// failed) withholds the marker so the next boot retries.
pub async fn run_first_boot_import(
    store: &Store,
    roots: Vec<PathBuf>,
    assets_root: Option<PathBuf>,
    app_dir: Option<PathBuf>,
    event_bus: Option<EventBus>,
    resumed: bool,
) {
    tracing::info!(
        resumed,
        "first-boot legacy workspace import starting in the background"
    );
    let opts = Options {
        roots,
        dry_run: false,
        force: false,
        assets_root,
        app_dir,
        event_bus,
    };
    match run(store, &opts).await {
        Ok(report) => {
            tracing::info!(
                imported = report.imported(),
                skipped = report.skipped(),
                notes = report.notes_imported(),
                comments = report.comments_imported(),
                agent_sessions = report.agent_sessions_imported(),
                agent_messages = report.agent_messages_imported(),
                assets = report.assets_imported(),
                "first-boot legacy workspace import complete"
            );
            for entry in &report.entries {
                tracing::info!(id = %entry.id, outcome = ?entry.outcome, "legacy workspace");
            }
            let failures = report.failures();
            if !failures.is_empty() {
                for (entry, reason) in &failures {
                    tracing::warn!(
                        id = %entry.id,
                        dir = %entry.dir.display(),
                        %reason,
                        "legacy workspace failed to import"
                    );
                }
                tracing::warn!(
                    failed = failures.len(),
                    "first-boot legacy workspace import completed with failures; completion marker written anyway (best-effort) — retry manually with `intentd import-legacy --force`"
                );
            }
            persist_failure_summary(store, &report).await;
            if let Err(e) = write_completion_marker(store).await {
                tracing::warn!(error = %e, "legacy import marker write failed");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "first-boot legacy workspace import failed; daemon continues (the next boot retries)");
        }
    }
}

/// Convenience composition of [`decide_first_boot_import`] +
/// [`run_first_boot_import`], awaiting the import inline. Used by tests;
/// `intentd serve` calls the two halves separately so the decision is
/// synchronous at startup while the import runs in a spawned background
/// task.
#[cfg(test)]
pub async fn maybe_import_on_first_boot(
    store: &Store,
    db_existed: bool,
    roots: Vec<PathBuf>,
    assets_root: Option<PathBuf>,
    app_dir: Option<PathBuf>,
) {
    match decide_first_boot_import(store, db_existed, &roots).await {
        FirstBootDecision::Skip => {}
        decision => {
            run_first_boot_import(
                store,
                roots,
                assets_root,
                app_dir,
                None,
                decision == FirstBootDecision::Resume,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use intent_core::WorkspaceId;

    /// Fresh throwaway fixture root under the system temp dir (never `~/intent`
    /// — STAB-138: tests must not pollute the developer's real workspace dirs).
    /// Returns the path plus an RAII guard that removes the dir on drop
    /// (including on panic); set `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it
    /// around for debugging.
    fn temp_root(tag: &str) -> (PathBuf, tempfile::TempDir) {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intentd-legacy-{tag}-"))
            .tempdir()
            .expect("create test tempdir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        (dir.path().to_path_buf(), dir)
    }

    /// Write `<root>/<id>/.workspace/workspace.json` with `extra` fields merged
    /// over a minimal legacy manifest (including the FE-only legacy arrays).
    fn write_legacy_workspace(root: &Path, id: &str, extra: &Value) -> PathBuf {
        let dir = root.join(id);
        let ws_dir = dir.join(".workspace");
        std::fs::create_dir_all(&ws_dir).unwrap();
        let mut obj = json!({
            "id": id,
            "title": format!("Legacy {id}"),
            "branch": format!("branch-{id}"),
            "status": "Active",
            "createdAt": "2025-05-01T00:00:00Z",
            "updatedAt": "2025-05-02T00:00:00Z",
            "tags": ["legacy"],
            "changesets": [{"id": "cs-1"}],
            "timeline": [{"event": "created"}],
            "conversationInfo": []
        });
        if let (Some(base), Some(over)) = (obj.as_object_mut(), extra.as_object()) {
            for (k, v) in over {
                base.insert(k.clone(), v.clone());
            }
        }
        std::fs::write(
            ws_dir.join("workspace.json"),
            serde_json::to_string_pretty(&obj).unwrap(),
        )
        .unwrap();
        dir
    }

    /// Open a store backed by a guarded temp dir; the returned guard removes
    /// the db plus its `-wal`/`-shm` sidecars on drop.
    async fn open_store() -> (Store, tempfile::TempDir) {
        let (dir, guard) = temp_root("db");
        let db = dir.join("legacy.db");
        (Store::open(&db).await.expect("open store"), guard)
    }

    fn opts(roots: Vec<PathBuf>) -> Options {
        Options {
            roots,
            ..Options::default()
        }
    }

    #[test]
    fn compatibility_failures_include_manifest_failures() {
        for reason in [
            "cannot read workspace.json: permission denied",
            "workspace.json has no id",
        ] {
            let mut report = Report::default();
            report.skip("ws-bad", Path::new("."), reason);
            assert!(report.has_compatibility_failures(), "{reason}");
        }
    }

    #[test]
    fn compatibility_failures_exclude_operational_skips() {
        for reason in [
            "already in DB",
            "daemon-managed manifest",
            "update failed: database busy",
            "insert failed: database busy",
            "lookup failed: database busy",
        ] {
            let mut report = Report::default();
            report.skip("ws-existing", Path::new("."), reason);
            assert!(!report.has_compatibility_failures(), "{reason}");
        }
    }

    /// Write `<ws-dir>/.workspace/notes/<name>` with raw contents.
    fn write_legacy_note(ws_dir: &Path, name: &str, contents: &str) {
        let notes_dir = ws_dir.join(".workspace").join("notes");
        std::fs::create_dir_all(&notes_dir).unwrap();
        std::fs::write(notes_dir.join(name), contents).unwrap();
    }

    #[tokio::test]
    async fn imports_legacy_workspaces_and_drops_legacy_fields() {
        let (root, _root_g) = temp_root("import");
        write_legacy_workspace(&root, "ws-a", &json!({}));
        write_legacy_workspace(
            &root,
            "ws-b",
            &json!({"archived": true, "archivedAt": "2025-06-01T00:00:00Z"}),
        );
        // Entries without .workspace/workspace.json are ignored.
        std::fs::create_dir_all(root.join("not-a-workspace")).unwrap();
        std::fs::write(root.join("stray-file"), "x").unwrap();
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.imported(), 2, "{report}");
        assert_eq!(report.skipped(), 0, "{report}");

        let a = store
            .get_workspace(&WorkspaceId::from("ws-a"))
            .await
            .unwrap();
        assert_eq!(a.title, "Legacy ws-a");
        assert_eq!(a.branch, "branch-ws-a");
        assert_eq!(a.tags, vec!["legacy".to_string()]);
        assert_eq!(a.created_at, "2025-05-01T00:00:00Z");
        let b = store
            .get_workspace(&WorkspaceId::from("ws-b"))
            .await
            .unwrap();
        assert!(b.archived);
    }

    /// `workspace:created` is published once per freshly inserted row (and
    /// only then: a re-run that skips existing rows publishes nothing).
    #[tokio::test]
    async fn publishes_workspace_created_only_for_fresh_inserts() {
        let (root, _root_g) = temp_root("created-events");
        write_legacy_workspace(&root, "ws-evt", &json!({}));
        let (store, _db_g) = open_store().await;
        let bus = EventBus::new(store.clone());
        let mut sub = bus.subscribe(intent_services::SubscriptionFilter {
            event_types: vec!["workspace:created".to_string()],
            ..Default::default()
        });
        let mut options = opts(vec![root.clone()]);
        options.event_bus = Some(bus.clone());

        let report = run(&store, &options).await.unwrap();
        assert_eq!(report.imported(), 1, "{report}");
        let batch = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("workspace:created not published")
            .expect("subscription closed");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_type, "workspace:created");
        assert_eq!(batch[0].workspace_id.as_str(), "ws-evt");
        assert_eq!(batch[0].data["workspaceId"], "ws-evt");
        assert!(
            batch[0].data["workspace"].is_object(),
            "{:?}",
            batch[0].data
        );

        // Idempotent re-run: the row exists, so no second event.
        let second = run(&store, &options).await.unwrap();
        assert_eq!(second.skipped(), 1, "{second}");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(250), sub.recv())
                .await
                .is_err(),
            "skip must not publish workspace:created"
        );
    }

    #[tokio::test]
    async fn imports_legacy_string_setup_script_and_child_note() {
        let (root, _root_g) = temp_root("setup-script");
        let ws_dir = write_legacy_workspace(
            &root,
            "ws-setup-script",
            &json!({"setupScript": "#!/bin/bash\nset -e\nnpm install\n"}),
        );
        write_legacy_note(
            &ws_dir,
            "extra.md",
            "---\nid: extra\ntitle: Imported extra\n---\n\nChild note body\n",
        );
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.imported(), 1, "{report}");
        assert_eq!(report.notes_imported(), 1, "{report}");

        let ws_id = WorkspaceId::from("ws-setup-script");
        let ws = store.get_workspace(&ws_id).await.unwrap();
        let setup = ws.setup_script.expect("normalized setup script");
        assert_eq!(setup.script, "#!/bin/bash\nset -e\nnpm install\n");
        assert_eq!(setup.updated_at, 1_746_144_000_000);
        let note = store
            .get_note(&ws_id, &NoteId::from("extra"))
            .await
            .unwrap();
        assert_eq!(note.content, "Child note body\n");
    }

    /// Hostile manifest ids (path traversal, absolute paths, separators) are
    /// rejected before any row insert or asset copy can use them.
    #[tokio::test]
    async fn rejects_workspace_ids_that_are_not_plain_path_segments() {
        let (root, _root_g) = temp_root("hostile-id");
        let (assets_root, _assets_g) = temp_root("hostile-id-assets");
        for (dir_name, hostile_id) in [
            ("evil-a", "../../escape"),
            ("evil-b", "/abs/path"),
            ("evil-c", "a/b"),
            ("evil-d", "a\\b"),
            ("evil-e", ".."),
            ("evil-f", "."),
        ] {
            let dir = write_legacy_workspace(&root, dir_name, &json!({ "id": hostile_id }));
            // Give each one an asset so a missed guard would attempt a copy.
            let assets_dir = dir.join(".workspace").join("assets");
            std::fs::create_dir_all(&assets_dir).unwrap();
            std::fs::write(assets_dir.join("asset-1"), "payload").unwrap();
        }
        let (store, _db_g) = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                assets_root: Some(assets_root.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.imported(), 0, "{report}");
        assert_eq!(report.skipped(), 6, "{report}");
        for entry in &report.entries {
            assert!(
                matches!(&entry.outcome, Outcome::Skipped(r) if r.contains("plain path segment")),
                "{report}"
            );
        }
        assert!(store.list_workspaces(true).await.unwrap().is_empty());
        // Nothing escaped the assets root (nothing was written at all).
        assert!(std::fs::read_dir(&assets_root).unwrap().next().is_none());
    }

    #[test]
    fn single_path_component_probe() {
        assert!(id_is_single_path_component("ws-a"));
        assert!(id_is_single_path_component("workspace_1.bak"));
        assert!(!id_is_single_path_component(".."));
        assert!(!id_is_single_path_component("."));
        assert!(!id_is_single_path_component("a/b"));
        assert!(!id_is_single_path_component("a\\b"));
        assert!(!id_is_single_path_component("/abs"));
        assert!(!id_is_single_path_component("../up"));
        assert!(!id_is_single_path_component(""));
    }

    #[tokio::test]
    async fn dry_run_reports_plan_without_writing() {
        let (root, _root_g) = temp_root("dry");
        write_legacy_workspace(&root, "ws-dry", &json!({}));
        let (store, _db_g) = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                dry_run: true,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.imported(), 1);
        assert!(report.to_string().contains("would import"), "{report}");
        assert!(store.list_workspaces(true).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn existing_ids_are_skipped_unless_forced() {
        let (root, _root_g) = temp_root("idem");
        write_legacy_workspace(&root, "ws-x", &json!({"title": "Old title"}));
        let (store, _db_g) = open_store().await;
        run(&store, &opts(vec![root.clone()])).await.unwrap();

        // Second run: idempotent skip.
        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.imported(), 0);
        assert_eq!(report.skipped(), 1);
        assert!(report.to_string().contains("already in DB"), "{report}");

        // --force overwrites the existing row.
        write_legacy_workspace(&root, "ws-x", &json!({"title": "New title"}));
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.updated(), 1, "{report}");
        let ws = store
            .get_workspace(&WorkspaceId::from("ws-x"))
            .await
            .unwrap();
        assert_eq!(ws.title, "New title");
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_worktree_falls_back_to_skip_worktree() {
        let (root, _root_g) = temp_root("worktree");
        let (live_dir, _live_g) = temp_root("live-worktree");
        write_legacy_workspace(
            &root,
            "ws-live",
            &json!({"worktreePath": live_dir.to_string_lossy(), "skipWorktree": false}),
        );
        write_legacy_workspace(
            &root,
            "ws-gone",
            &json!({"worktreePath": "/nonexistent/legacy/worktree", "skipWorktree": false}),
        );
        let (store, _db_g) = open_store().await;
        run(&store, &opts(vec![root.clone()])).await.unwrap();

        let live = store
            .get_workspace(&WorkspaceId::from("ws-live"))
            .await
            .unwrap();
        assert_eq!(
            live.worktree_path,
            Some(live_dir.to_string_lossy().into_owned())
        );
        assert!(!live.skip_worktree);
        assert_eq!(live.branch, "branch-ws-live");
        let gone = store
            .get_workspace(&WorkspaceId::from("ws-gone"))
            .await
            .unwrap();
        assert_eq!(gone.worktree_path, None);
        assert!(gone.skip_worktree);
        assert_eq!(gone.branch, "branch-ws-gone");
    }

    #[tokio::test]
    async fn skips_chief_duplicates_and_malformed_manifests() {
        let (root_a, _root_a_g) = temp_root("roots-a");
        let (root_b, _root_b_g) = temp_root("roots-b");
        write_legacy_workspace(&root_a, "__chief__", &json!({}));
        write_legacy_workspace(&root_a, "ws-dup", &json!({"title": "From root A"}));
        write_legacy_workspace(&root_b, "ws-dup", &json!({"title": "From root B"}));
        let broken = root_a.join("ws-broken").join(".workspace");
        std::fs::create_dir_all(&broken).unwrap();
        std::fs::write(broken.join("workspace.json"), "{ nope").unwrap();
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root_a.clone(), root_b.clone()]))
            .await
            .unwrap();
        assert_eq!(report.imported(), 1, "{report}");
        assert_eq!(report.skipped(), 3, "{report}");
        let text = report.to_string();
        assert!(text.contains("virtual workspace id"), "{text}");
        assert!(text.contains("duplicate id"), "{text}");
        assert!(text.contains("invalid JSON"), "{text}");
        // First root wins for the duplicated id.
        let dup = store
            .get_workspace(&WorkspaceId::from("ws-dup"))
            .await
            .unwrap();
        assert_eq!(dup.title, "From root A");
    }

    #[tokio::test]
    async fn first_boot_hook_imports_once_and_writes_marker() {
        let (root, _root_g) = temp_root("boot");
        write_legacy_workspace(&root, "ws-boot", &json!({}));
        let (store, _db_g) = open_store().await;

        // Fresh DB, no marker → import runs and the completion marker is
        // written; the pending marker written at start is cleared.
        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
        let marker = store.get_setting(LEGACY_IMPORT_MARKER_KEY).await.unwrap();
        assert!(
            marker.is_some_and(|m| m.starts_with('"')),
            "JSON string marker"
        );
        assert!(store
            .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
            .await
            .unwrap()
            .is_none());

        // Marker present → the hook is a no-op even on a "fresh" DB signal.
        write_legacy_workspace(&root, "ws-later", &json!({}));
        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
    }

    /// Best-effort completion: a workspace whose manifest fails to parse
    /// does not withhold the marker — the good workspace lands, the failure
    /// is summarized under [`LEGACY_IMPORT_FAILURES_KEY`], and the pending
    /// marker is cleared so the hook never wedges into a boot-time retry
    /// loop.
    #[tokio::test]
    async fn first_boot_hook_writes_marker_despite_parse_failure() {
        let (root, _root_g) = temp_root("boot-parse-failure");
        write_legacy_workspace(&root, "ws-good", &json!({}));
        write_legacy_workspace(&root, "ws-bad", &json!({"setupScript": 42}));
        let (store, _db_g) = open_store().await;

        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;

        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
            .await
            .unwrap()
            .is_none());
        // The failure summary names the bad workspace with its reason.
        let summary: Value = serde_json::from_str(
            &store
                .get_setting(LEGACY_IMPORT_FAILURES_KEY)
                .await
                .unwrap()
                .expect("failure summary persisted"),
        )
        .unwrap();
        assert_eq!(summary["total"], 1, "{summary}");
        assert!(summary["at"].is_string(), "{summary}");
        let failures = summary["failures"].as_array().unwrap();
        assert_eq!(failures.len(), 1, "{summary}");
        assert_eq!(failures[0]["id"], "ws-bad", "{summary}");
        assert!(
            failures[0]["reason"]
                .as_str()
                .unwrap()
                .contains("setupScript")
                || !failures[0]["reason"].as_str().unwrap().is_empty(),
            "{summary}"
        );

        // Marker present → later boots are a no-op (no retry loop).
        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
    }

    /// Task-join isolation: a workspace whose import panics becomes a logged
    /// skip; later workspaces still import and the first-boot marker is
    /// still written.
    #[tokio::test]
    async fn panicking_workspace_does_not_abort_the_run() {
        let (root, _root_g) = temp_root("panic-isolation");
        write_legacy_workspace(&root, "ws-a-panics", &json!({"__testPanic": true}));
        write_legacy_workspace(&root, "ws-b-good", &json!({}));
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();

        assert_eq!(report.imported(), 1, "{report}");
        assert_eq!(report.skipped(), 1, "{report}");
        assert!(report.entries.iter().any(|entry| {
            entry.id == "ws-a-panics"
                && matches!(&entry.outcome, Outcome::Skipped(reason) if reason.contains("import panicked"))
        }), "{report}");
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.id == "ws-b-good" && entry.outcome == Outcome::Imported));
        assert!(store
            .get_workspace(&WorkspaceId::from("ws-b-good"))
            .await
            .is_ok());
    }

    /// The first-boot hook completes despite a panicking workspace: the
    /// marker is written and the panic lands in the failure summary.
    #[tokio::test]
    async fn first_boot_hook_survives_panicking_workspace_and_writes_marker() {
        let (root, _root_g) = temp_root("boot-panic");
        write_legacy_workspace(&root, "ws-panics", &json!({"__testPanic": true}));
        write_legacy_workspace(&root, "ws-survives", &json!({}));
        let (store, _db_g) = open_store().await;

        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;

        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
            .await
            .unwrap()
            .is_none());
        let summary: Value = serde_json::from_str(
            &store
                .get_setting(LEGACY_IMPORT_FAILURES_KEY)
                .await
                .unwrap()
                .expect("failure summary persisted"),
        )
        .unwrap();
        assert_eq!(summary["total"], 1, "{summary}");
        assert_eq!(summary["failures"][0]["id"], "ws-panics", "{summary}");
        assert!(
            summary["failures"][0]["reason"]
                .as_str()
                .unwrap()
                .contains("import panicked"),
            "{summary}"
        );
    }

    /// A clean run clears a stale failure summary (the documented
    /// `import-legacy --force` retry path ends with an empty row).
    #[tokio::test]
    async fn clean_run_clears_stale_failure_summary() {
        let (root, _root_g) = temp_root("summary-clear");
        write_legacy_workspace(&root, "ws-flaky", &json!({"setupScript": 42}));
        let (store, _db_g) = open_store().await;

        let first = run(&store, &opts(vec![root.clone()])).await.unwrap();
        persist_failure_summary(&store, &first).await;
        assert!(store
            .get_setting(LEGACY_IMPORT_FAILURES_KEY)
            .await
            .unwrap()
            .is_some());

        write_legacy_workspace(&root, "ws-flaky", &json!({}));
        let second = run(&store, &opts(vec![root.clone()])).await.unwrap();
        persist_failure_summary(&store, &second).await;
        assert!(store
            .get_setting(LEGACY_IMPORT_FAILURES_KEY)
            .await
            .unwrap()
            .is_none());
    }

    /// Benign skips (already in DB, managed manifests, duplicates, virtual
    /// id) stay out of the failure summary; store-write failures would be
    /// included (see [`Report::failures`]).
    #[tokio::test]
    async fn failure_summary_excludes_benign_skips() {
        let (root, _root_g) = temp_root("summary-benign");
        write_legacy_workspace(&root, "ws-managed", &json!({"managedBy": "intentd"}));
        write_legacy_workspace(&root, "ws-existing", &json!({}));
        let (store, _db_g) = open_store().await;

        // Seed ws-existing so the run reports it "already in DB".
        run(&store, &opts(vec![root.clone()])).await.unwrap();
        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();

        assert!(report.failures().is_empty(), "{report}");
        persist_failure_summary(&store, &report).await;
        assert!(store
            .get_setting(LEGACY_IMPORT_FAILURES_KEY)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn second_run_recovers_parse_failure_and_skips_existing_workspace() {
        let (root, _root_g) = temp_root("parse-recovery");
        write_legacy_workspace(&root, "ws-existing", &json!({}));
        write_legacy_workspace(&root, "ws-recovered", &json!({"setupScript": 42}));
        let (store, _db_g) = open_store().await;

        let first = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(first.imported(), 1, "{first}");
        assert!(first.has_compatibility_failures(), "{first}");
        write_completion_marker(&store).await.unwrap();

        write_legacy_workspace(&root, "ws-recovered", &json!({}));
        let second = run(&store, &opts(vec![root.clone()])).await.unwrap();

        assert_eq!(second.imported(), 1, "{second}");
        assert_eq!(second.skipped(), 1, "{second}");
        assert!(!second.has_compatibility_failures(), "{second}");
        assert!(second.entries.iter().any(|entry| {
            entry.id == "ws-existing"
                && matches!(&entry.outcome, Outcome::Skipped(reason) if reason == "already in DB")
        }));
        assert!(second
            .entries
            .iter()
            .any(|entry| entry.id == "ws-recovered" && entry.outcome == Outcome::Imported));
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn skips_daemon_managed_manifest_without_force() {
        let (root, _root_g) = temp_root("managed");
        write_legacy_workspace(&root, "ws-managed", &json!({"managedBy": "intentd"}));
        write_legacy_workspace(&root, "ws-legacy", &json!({}));
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();

        assert_eq!(report.imported(), 1, "{report}");
        assert_eq!(report.skipped(), 1, "{report}");
        assert!(report.entries.iter().any(|entry| {
            entry.id == "ws-managed"
                && matches!(&entry.outcome, Outcome::Skipped(reason) if reason == MANAGED_SKIP_REASON)
        }));
        // Operational skip — must never block the first-boot marker.
        assert!(!report.has_compatibility_failures(), "{report}");
        assert!(store
            .get_workspace(&WorkspaceId::from("ws-managed"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn first_boot_hook_skips_daemon_managed_manifest_and_writes_marker() {
        let (root, _root_g) = temp_root("boot-managed");
        write_legacy_workspace(&root, "ws-managed", &json!({"managedBy": "intentd"}));
        let (store, _db_g) = open_store().await;

        maybe_import_on_first_boot(&store, false, vec![root.clone()], None, None).await;

        assert!(store.list_workspaces(true).await.unwrap().is_empty());
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn force_imports_daemon_managed_manifest() {
        let (root, _root_g) = temp_root("managed-force");
        write_legacy_workspace(&root, "ws-managed", &json!({"managedBy": "intentd"}));
        let (store, _db_g) = open_store().await;

        let force_opts = Options {
            roots: vec![root.clone()],
            force: true,
            ..Options::default()
        };
        let report = run(&store, &force_opts).await.unwrap();

        assert_eq!(report.imported(), 1, "{report}");
        // `managedBy` is dropped before deserialization — the import must not
        // fail on the marker field.
        let ws = store
            .get_workspace(&WorkspaceId::from("ws-managed"))
            .await
            .unwrap();
        assert_eq!(ws.title, "Legacy ws-managed");
    }

    #[tokio::test]
    async fn first_boot_hook_skips_preexisting_db() {
        let (root, _root_g) = temp_root("boot-existing");
        write_legacy_workspace(&root, "ws-pre", &json!({}));
        let (store, _db_g) = open_store().await;

        // Pre-existing DB, no pending marker → never imports, and no marker
        // (pending or completion) is written — across repeated boots.
        for _ in 0..2 {
            assert_eq!(
                decide_first_boot_import(&store, true, std::slice::from_ref(&root)).await,
                FirstBootDecision::Skip
            );
            maybe_import_on_first_boot(&store, true, vec![root.clone()], None, None).await;
            assert!(store.list_workspaces(true).await.unwrap().is_empty());
            assert!(store
                .get_setting(LEGACY_IMPORT_MARKER_KEY)
                .await
                .unwrap()
                .is_none());
            assert!(store
                .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
                .await
                .unwrap()
                .is_none());
        }
    }

    /// A daemon killed mid-import leaves the pending marker behind; the next
    /// boot resumes the run even though the DB file now pre-exists, skipping
    /// rows the interrupted run already imported.
    #[tokio::test]
    async fn killed_mid_import_resumes_on_next_boot() {
        let (root, _root_g) = temp_root("boot-resume");
        write_legacy_workspace(&root, "ws-a", &json!({}));
        let (store, _db_g) = open_store().await;

        // First boot: fresh DB → Start, and the pending marker is persisted
        // before the run begins.
        assert_eq!(
            decide_first_boot_import(&store, false, std::slice::from_ref(&root)).await,
            FirstBootDecision::Start
        );
        assert!(store
            .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
            .await
            .unwrap()
            .is_some());

        // Simulate a kill mid-import: only ws-a made it into the DB before
        // the task was aborted — no completion marker was written. ws-b is
        // written afterwards to stand in for the part of the legacy tree the
        // interrupted run never reached.
        run(&store, &opts(vec![root.clone()])).await.unwrap();
        write_legacy_workspace(&root, "ws-b", &json!({}));
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_none());

        // Second boot: DB pre-exists but the pending marker is set → Resume.
        assert_eq!(
            decide_first_boot_import(&store, true, std::slice::from_ref(&root)).await,
            FirstBootDecision::Resume
        );
        run_first_boot_import(&store, vec![root.clone()], None, None, None, true).await;

        // Both workspaces present (ws-a was skipped as already in DB), the
        // completion marker is written, and the pending marker is cleared.
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 2);
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .get_setting(LEGACY_IMPORT_PENDING_MARKER_KEY)
            .await
            .unwrap()
            .is_none());

        // Third boot: completion marker present → Skip, even with pending-era
        // history behind it.
        assert_eq!(
            decide_first_boot_import(&store, true, std::slice::from_ref(&root)).await,
            FirstBootDecision::Skip
        );
    }

    /// Empty roots fully disable the hook: no app-dir read, no marker write —
    /// so `INTENTD_LEGACY_IMPORT_ROOTS=""` really turns the feature off.
    #[tokio::test]
    async fn first_boot_hook_disabled_by_empty_roots() {
        let (app_dir, _app_g) = temp_root("boot-empty-app");
        std::fs::write(
            app_dir.join("repo-registry.json"),
            r#"{"knownRepos": [{"path": "/tmp/repo"}]}"#,
        )
        .unwrap();
        let (store, _db_g) = open_store().await;

        maybe_import_on_first_boot(&store, false, Vec::new(), None, Some(app_dir.clone())).await;
        assert!(store.list_workspaces(true).await.unwrap().is_empty());
        // App-level blobs were NOT imported and no marker was written.
        assert!(store.get_setting("repos.known").await.unwrap().is_none());
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn source_is_never_mutated() {
        let (root, _root_g) = temp_root("readonly");
        write_legacy_workspace(&root, "ws-ro", &json!({}));
        let manifest = root.join("ws-ro").join(".workspace").join("workspace.json");
        let before = std::fs::read(&manifest).unwrap();
        let (store, _db_g) = open_store().await;

        run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(std::fs::read(&manifest).unwrap(), before);
    }

    #[tokio::test]
    async fn imports_notes_with_frontmatter_spec_and_task() {
        let (root, _root_g) = temp_root("notes");
        let ws_dir = write_legacy_workspace(&root, "ws-notes", &json!({}));
        write_legacy_note(
            &ws_dir,
            "spec.md",
            "---\nid: spec\ntitle: Spec\ntags: [spec]\npinned: true\ncreated: \"2026-07-15T23:58:11.557Z\"\n---\n\n# The spec body\n",
        );
        write_legacy_note(
            &ws_dir,
            "task-1.md",
            "---\nid: task-1\ntitle: Do the thing\ntags: [task]\narchived: true\nvisibility: private\nparent: spec\ncreated: \"2026-07-16T00:00:00.000Z\"\ntask:\n  status: in_progress\n  assignedAgentIds: [agent-1]\n  acceptanceCriteria:\n    - it works\n  peerOrder: 100\n---\n\nTask body\n",
        );
        // No frontmatter at all: whole file is the body, filename-derived title.
        write_legacy_note(&ws_dir, "plain.md", "Just a body\n");
        // The .meta sidecar (versions/CRDT/trash) must be skipped entirely.
        let meta = ws_dir.join(".workspace").join("notes").join(".meta");
        std::fs::create_dir_all(meta.join("versions")).unwrap();
        std::fs::write(meta.join("versions").join("spec.jsonl"), "{}").unwrap();
        // Non-markdown files are ignored.
        write_legacy_note(&ws_dir, "scratch.txt", "not a note");
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.imported(), 1, "{report}");
        assert_eq!(report.notes_imported(), 3, "{report}");
        assert_eq!(report.entries[0].notes.imported, 3, "{report}");
        assert_eq!(report.entries[0].notes.failed, 0, "{report}");
        assert!(report.to_string().contains("notes: 3 imported"), "{report}");

        let ws_id = WorkspaceId::from("ws-notes");
        assert_eq!(store.list_notes(&ws_id).await.unwrap().len(), 3);

        let spec = store.get_note(&ws_id, &NoteId::from("spec")).await.unwrap();
        assert_eq!(spec.title, "Spec");
        assert!(spec.is_default);
        assert!(spec.is_pinned);
        assert_eq!(spec.tags, vec!["spec".to_string()]);
        assert_eq!(spec.created_at, "2026-07-15T23:58:11.557Z");
        assert_eq!(spec.content, "# The spec body\n");
        assert_eq!(spec.visibility, NoteVisibility::Workspace);
        assert!(spec.metadata.task.is_none());

        let task = store
            .get_note(&ws_id, &NoteId::from("task-1"))
            .await
            .unwrap();
        assert_eq!(task.title, "Do the thing");
        assert!(task.is_archived);
        assert!(!task.is_default);
        assert_eq!(task.visibility, NoteVisibility::Private);
        assert_eq!(task.parent_id, Some(NoteId::from("spec")));
        assert_eq!(task.content, "Task body\n");
        let meta = task.metadata.task.expect("task metadata");
        assert_eq!(meta.status, intent_core::TaskStatus::InProgress);
        assert_eq!(meta.assigned_agent_ids, vec!["agent-1".into()]);
        assert_eq!(meta.acceptance_criteria, vec!["it works".to_string()]);
        assert_eq!(meta.peer_order, Some(100));

        let plain = store
            .get_note(&ws_id, &NoteId::from("plain"))
            .await
            .unwrap();
        assert_eq!(plain.title, "plain");
        assert_eq!(plain.content, "Just a body\n");
        assert!(!plain.is_pinned);
    }

    #[tokio::test]
    async fn malformed_note_frontmatter_imports_body_best_effort() {
        let (root, _root_g) = temp_root("notes-malformed");
        let ws_dir = write_legacy_workspace(&root, "ws-bad-notes", &json!({}));
        // Unparseable YAML between valid delimiters: body still lands, with a
        // filename-derived title.
        write_legacy_note(
            &ws_dir,
            "broken.md",
            "---\ntitle: [unclosed\n  nope ::\n---\n\nSurviving body\n",
        );
        // Malformed task block inside otherwise-valid frontmatter: imports as
        // a plain note, keeping the rest of the metadata.
        write_legacy_note(
            &ws_dir,
            "bad-task.md",
            "---\nid: bad-task\ntitle: Bad task\ntask: \"not a mapping\"\n---\n\nBody here\n",
        );
        // Empty `title:` / `created:` degrade to filename-derived title and
        // a fresh timestamp rather than importing empty strings.
        write_legacy_note(
            &ws_dir,
            "empties.md",
            "---\nid: empties\ntitle: \"\"\ncreated: \"\"\n---\n\nEmpty meta body\n",
        );
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.entries[0].notes.imported, 3, "{report}");
        assert_eq!(report.entries[0].notes.failed, 0, "{report}");

        let ws_id = WorkspaceId::from("ws-bad-notes");
        let broken = store
            .get_note(&ws_id, &NoteId::from("broken"))
            .await
            .unwrap();
        assert_eq!(broken.title, "broken");
        assert_eq!(broken.content, "Surviving body\n");

        let bad_task = store
            .get_note(&ws_id, &NoteId::from("bad-task"))
            .await
            .unwrap();
        assert_eq!(bad_task.title, "Bad task");
        assert_eq!(bad_task.content, "Body here\n");
        assert!(bad_task.metadata.task.is_none());

        let empties = store
            .get_note(&ws_id, &NoteId::from("empties"))
            .await
            .unwrap();
        assert_eq!(empties.title, "empties");
        assert!(!empties.created_at.is_empty());
        assert_eq!(empties.created_at, empties.updated_at);
    }

    #[tokio::test]
    async fn notes_import_is_idempotent_per_note_id() {
        let (root, _root_g) = temp_root("notes-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-note-idem", &json!({}));
        write_legacy_note(
            &ws_dir,
            "spec.md",
            "---\nid: spec\ntitle: Spec\n---\n\nOriginal spec\n",
        );
        let (store, _db_g) = open_store().await;
        run(&store, &opts(vec![root.clone()])).await.unwrap();

        // Re-run with --force (workspace row updates, extras re-run): the
        // existing note id is skipped, its content untouched.
        write_legacy_note(
            &ws_dir,
            "spec.md",
            "---\nid: spec\ntitle: Spec\n---\n\nRewritten spec\n",
        );
        write_legacy_note(&ws_dir, "extra.md", "---\nid: extra\n---\n\nNew note\n");
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].notes.imported, 1, "{report}");
        assert_eq!(report.entries[0].notes.skipped, 1, "{report}");

        let ws_id = WorkspaceId::from("ws-note-idem");
        let spec = store.get_note(&ws_id, &NoteId::from("spec")).await.unwrap();
        assert_eq!(spec.content, "Original spec\n");
        assert_eq!(store.list_notes(&ws_id).await.unwrap().len(), 2);
    }

    /// Write `<ws-dir>/.workspace/notes/.meta/<name>` with raw contents.
    fn write_legacy_meta(ws_dir: &Path, name: &str, contents: &str) {
        let meta_dir = ws_dir.join(".workspace").join("notes").join(".meta");
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::write(meta_dir.join(name), contents).unwrap();
    }

    /// Fetch the raw `extra_json` column for one comment row.
    async fn comment_extra_json(store: &Store, id: &str) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT extra_json FROM comment WHERE id = ?")
            .bind(id)
            .fetch_one(store.read_pool())
            .await
            .expect("read extra_json")
    }

    #[tokio::test]
    async fn imports_comments_with_threads_anchors_and_extras() {
        let (root, _root_g) = temp_root("comments");
        let ws_dir = write_legacy_workspace(&root, "ws-comments", &json!({}));
        write_legacy_note(&ws_dir, "spec.md", "---\nid: spec\n---\n\nSpec body\n");
        // Realistic legacy sidecar: a root comment with anchor/extras, a reply
        // in the same thread, a suggestion, and one malformed entry (no id).
        let data = json!({
            "version": "1.0",
            "lastUpdated": "2025-06-02T00:00:00Z",
            "comments": [
                {
                    "id": "c-root",
                    "noteId": "spec",
                    "author": "clement",
                    "authorType": "user",
                    "type": "question",
                    "content": "Why this approach?",
                    "status": "open",
                    "createdAt": "2025-06-01T00:00:00Z",
                    "updatedAt": "2025-06-01T00:00:00Z",
                    "threadId": "thread-1",
                    "section": "## Design",
                    "markId": "mark-abc",
                    "from": 120,
                    "to": 148,
                    "lineStart": 10,
                    "lineEnd": 12,
                    "tags": ["design"],
                    "reactions": {"👍": "clement"}
                },
                {
                    "id": "c-reply",
                    "noteId": "spec",
                    "author": "agent-1",
                    "authorType": "agent",
                    "type": "comment",
                    "content": "Because of X.",
                    "status": "open",
                    "createdAt": "2025-06-01T01:00:00Z",
                    "updatedAt": "2025-06-01T01:00:00Z",
                    "threadId": "thread-1",
                    "parentId": "c-root",
                    "agentId": "agent-1",
                    "isOrphaned": false
                },
                {
                    "id": "c-sugg",
                    "noteId": "spec",
                    "author": "agent-1",
                    "authorType": "agent",
                    "type": "suggestion",
                    "content": "Rename it",
                    "status": "pending",
                    "createdAt": "2025-06-01T02:00:00Z",
                    "updatedAt": "2025-06-01T02:00:00Z",
                    "isOrphaned": "yes",
                    "suggestionDiff": {
                        "original": "foo",
                        "proposed": "bar",
                        "lineStart": 3
                    }
                },
                { "noteId": "spec", "content": "no id — malformed" }
            ]
        });
        write_legacy_meta(&ws_dir, "spec.comments.json", &data.to_string());
        // Orphaned sidecar: no matching note file → skipped whole.
        write_legacy_meta(
            &ws_dir,
            "ghost.comments.json",
            &json!({"version": "1.0", "comments": [{"id": "c-ghost"}]}).to_string(),
        );
        // Non-comments .meta files are ignored.
        write_legacy_meta(&ws_dir, "spec.versions.jsonl", "{}");
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        let counts = report.entries[0].comments;
        assert_eq!(counts.imported, 3, "{report}");
        assert_eq!(counts.failed, 1, "{report}");
        assert_eq!(counts.orphaned, 1, "{report}");
        assert_eq!(report.comments_imported(), 3, "{report}");
        assert!(
            report
                .to_string()
                .contains("comments: 3 imported, 0 skipped, 1 failed, 1 orphaned"),
            "{report}"
        );

        let root_c = store.get_comment("c-root").await.unwrap();
        assert_eq!(root_c.thread_id, "thread-1");
        assert_eq!(root_c.note_id, Some(NoteId::from("spec")));
        assert_eq!(root_c.kind, CommentType::Question);
        assert_eq!(root_c.status, CommentStatus::Open);
        assert_eq!(root_c.author, "clement");
        assert_eq!(root_c.author_type, AuthorType::User);
        let root_anchor = root_c
            .anchor
            .as_ref()
            .expect("root comment keeps its anchor");
        assert_eq!(root_anchor.kind, CommentAnchorType::Point);
        assert_eq!(root_anchor.point_id, Some("mark-abc".to_string()));
        assert_eq!(root_c.anchor_text, Some("## Design".to_string()));
        assert_eq!(root_c.created_at, "2025-06-01T00:00:00Z");
        // Unmapped legacy fields survive in extra_json.
        let extra: Value =
            serde_json::from_str(&comment_extra_json(&store, "c-root").await.unwrap()).unwrap();
        assert_eq!(extra["from"], json!(120));
        assert_eq!(extra["to"], json!(148));
        assert_eq!(extra["lineStart"], json!(10));
        assert_eq!(extra["lineEnd"], json!(12));
        assert_eq!(extra["tags"], json!(["design"]));
        assert_eq!(extra["reactions"], json!({"👍": "clement"}));

        // Thread structure survives: reply shares the thread and parent.
        let reply = store.get_comment("c-reply").await.unwrap();
        assert_eq!(reply.parent_id, Some("c-root".to_string()));
        assert_eq!(reply.author_type, AuthorType::Agent);
        assert_eq!(reply.agent_id, Some(AgentId::from("agent-1")));
        assert_eq!(reply.is_orphaned, Some(false));
        let thread = store.get_thread("thread-1").await.unwrap();
        assert_eq!(thread.comments.len(), 2);

        // Suggestion diff maps to suggestion_original/proposed; no threadId in
        // the source → FE default thread-{id}; leftover diff fields kept.
        let sugg = store.get_comment("c-sugg").await.unwrap();
        assert_eq!(sugg.kind, CommentType::Suggestion);
        assert_eq!(sugg.status, CommentStatus::Pending);
        assert_eq!(sugg.thread_id, "thread-c-sugg");
        assert_eq!(sugg.suggestion_original, Some("foo".to_string()));
        assert_eq!(sugg.suggestion_proposed, Some("bar".to_string()));
        let sugg_extra: Value =
            serde_json::from_str(&comment_extra_json(&store, "c-sugg").await.unwrap()).unwrap();
        assert_eq!(sugg_extra["suggestionDiff"], json!({"lineStart": 3}));
        // A wrong-typed `isOrphaned` is preserved verbatim, not dropped.
        assert_eq!(sugg_extra["isOrphaned"], json!("yes"));

        assert!(store.get_comment("c-ghost").await.is_err());
    }

    #[tokio::test]
    async fn comments_import_is_idempotent_and_survives_malformed_files() {
        let (root, _root_g) = temp_root("comments-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-c-idem", &json!({}));
        write_legacy_note(&ws_dir, "spec.md", "---\nid: spec\n---\n\nSpec\n");
        write_legacy_note(&ws_dir, "other.md", "---\nid: other\n---\n\nOther\n");
        write_legacy_meta(
            &ws_dir,
            "spec.comments.json",
            &json!({"version": "1.0", "comments": [{
                "id": "c-1",
                "author": "clement",
                "authorType": "user",
                "type": "comment",
                "content": "First",
                "status": "open",
                "createdAt": "2025-06-01T00:00:00Z",
                "updatedAt": "2025-06-01T00:00:00Z"
            }]})
            .to_string(),
        );
        // Whole-file garbage: counted failed, never fatal.
        write_legacy_meta(&ws_dir, "other.comments.json", "{ nope");
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.entries[0].comments.imported, 1, "{report}");
        assert_eq!(report.entries[0].comments.failed, 1, "{report}");

        // Force re-run: existing comment id skipped, content untouched.
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].comments.imported, 0, "{report}");
        assert_eq!(report.entries[0].comments.skipped, 1, "{report}");
        let c1 = store.get_comment("c-1").await.unwrap();
        assert_eq!(c1.content, "First");
        assert_eq!(
            store
                .list_comments(&NoteId::from("spec"))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// Write `<ws-dir>/.workspace/agents/<name>` with raw contents.
    fn write_legacy_agent(ws_dir: &Path, name: &str, contents: &str) {
        let agents_dir = ws_dir.join(".workspace").join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(agents_dir.join(name), contents).unwrap();
    }

    const LEGACY_AGENT_ID: &str = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";

    /// A realistic legacy `AgentSession` file: runtime/UI state, fork lineage,
    /// metadata behavior fields, and a transcript mixing known blocks, an
    /// unknown block shape, an error-only message, and one malformed entry.
    fn legacy_agent_fixture() -> Value {
        let messages = json!([
            {
                "id": "msg-1",
                "role": "user",
                "contentBlocks": [{"type": "text", "text": "Port the store crate"}],
                "timestamp": "2025-06-01T00:00:02Z",
                "turnNumber": 1
            },
            {
                "id": "msg-2",
                "role": "assistant",
                "contentBlocks": [
                    {"type": "thinking", "text": "Let me look."},
                    {"type": "tool_use", "id": "tu-1", "name": "view", "input": {"path": "src"}},
                    {"type": "tool_result", "tool_use_id": "tu-1", "output": "ok"},
                    {"type": "mystery", "payload": {"x": 1}},
                    {"type": "text", "text": "Done."}
                ],
                "timestamp": "2025-06-01T00:00:03Z",
                "toolCalls": [{"id": "tu-1", "name": "view", "arguments": {}}],
                "metadata": {"model": "sonnet4.5", "stopReason": "end_turn"}
            },
            {
                "id": "msg-3",
                "role": "error",
                "error": "stream disconnected",
                "timestamp": "2025-06-01T00:00:04Z"
            },
            "not an object — malformed",
            { "id": "msg-5", "contentBlocks": [] }
        ]);
        json!({
            "id": LEGACY_AGENT_ID,
            "backendSessionId": "auggie-session-1",
            "workspaceId": "ws-agents",
            "name": "Port the store",
            "nameExplicitlySet": true,
            "model": "sonnet4.5",
            "provider": "acp",
            "systemPrompt": "You are a porting agent.",
            "status": "Processing",
            "activationState": "active",
            "isStreaming": true,
            "queuedMessages": [],
            "createdAt": "2025-06-01T00:00:00Z",
            "updatedAt": "2025-06-02T00:00:00Z",
            "lastActivity": "2025-06-02T00:00:00Z",
            "startedAt": "2025-06-01T00:00:01Z",
            "endedAt": "2025-06-02T00:00:00Z",
            "isInitialAgent": true,
            "isBackground": true,
            "digest": "Ported the store crate",
            "parentSessionId": "agent-99999999-9999-4999-8999-999999999999",
            "forkedAt": "2025-06-01T12:00:00Z",
            "forkPoint": 2,
            "childSessionIds": ["agent-11111111-1111-4111-8111-111111111111"],
            "metadata": {
                "specialist": "implementor",
                "taskNoteId": "task-1",
                "completionReport": "Done.",
                "completionReportTimestamp": "2025-06-02T00:00:00Z",
                "delegationDepth": 1,
                "initialMessage": "Port the store crate",
                "createdByAgentId": "agent-99999999-9999-4999-8999-999999999999"
            },
            "messages": messages
        })
    }

    #[tokio::test]
    async fn imports_agent_transcripts_as_completed_sessions() {
        let (root, _root_g) = temp_root("agents");
        let ws_dir = write_legacy_workspace(&root, "ws-agents", &json!({}));
        write_legacy_agent(
            &ws_dir,
            &format!("{LEGACY_AGENT_ID}.json"),
            &legacy_agent_fixture().to_string(),
        );
        // Sidecar/stray files the legacy persistence wrote are ignored.
        write_legacy_agent(&ws_dir, &format!("{LEGACY_AGENT_ID}.json.checksum"), "abc");
        write_legacy_agent(&ws_dir, ".health-check", "test");
        write_legacy_agent(&ws_dir, "notes.txt", "not an agent");
        // Whole-file garbage: counted failed, never fatal.
        write_legacy_agent(&ws_dir, "agent-broken.json", "{ nope");
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        let counts = report.entries[0].agents;
        assert_eq!(counts.sessions_imported, 1, "{report}");
        assert_eq!(counts.sessions_failed, 1, "{report}");
        // 3 good messages; the non-object entry and the role-less entry skip.
        assert_eq!(counts.messages_imported, 3, "{report}");
        assert_eq!(counts.messages_failed, 2, "{report}");
        assert_eq!(report.agent_sessions_imported(), 1, "{report}");
        assert_eq!(report.agent_messages_imported(), 3, "{report}");
        assert!(
            report.to_string().contains(
                "agents: 1 imported, 0 skipped, 1 failed, messages: 3 imported, 2 failed"
            ),
            "{report}"
        );

        let session = store
            .get_agent_session(&AgentId::from(LEGACY_AGENT_ID))
            .await
            .unwrap();
        // Terminal historical record: never resumable.
        assert_eq!(session.status, AgentStatus::Completed);
        assert!(!session.is_active);
        assert_eq!(session.acp_session_id, None);
        assert_eq!(session.workspace_id, WorkspaceId::from("ws-agents"));
        assert_eq!(session.name, "Port the store");
        assert!(session.name_explicitly_set);
        assert_eq!(session.model, Some("sonnet4.5".to_string()));
        // 'acp' is the protocol name, not a provider — dropped.
        assert_eq!(session.provider, None);
        assert_eq!(
            session.system_prompt,
            Some("You are a porting agent.".to_string())
        );
        assert_eq!(
            session.backend_session_id,
            Some(AgentId::from("auggie-session-1"))
        );
        assert_eq!(
            session.parent_agent_id,
            Some(AgentId::from("agent-99999999-9999-4999-8999-999999999999"))
        );
        assert!(session.is_background);
        assert_eq!(session.created_at, "2025-06-01T00:00:00Z");
        assert_eq!(session.updated_at, "2025-06-02T00:00:00Z");
        // Imported sessions predate harness stamping: literal "1.0" (pinned
        // even if CURRENT_HARNESS_VERSION bumps) + NULL snapshot, exactly like
        // migration 0096's backfill of pre-existing rows.
        assert_eq!(session.harness_version, "1.0");
        assert_eq!(session.harness_features, None);
        // Metadata behavior fields lifted to columns.
        assert_eq!(session.specialist, Some("implementor".to_string()));
        assert_eq!(session.task_note_id, Some(NoteId::from("task-1")));
        assert_eq!(session.completion_report, Some("Done.".to_string()));
        assert_eq!(session.delegation_depth, Some(1));
        assert_eq!(
            session.initial_message,
            Some("Port the store crate".to_string())
        );
        // Unmapped legacy fields survive under metadata.legacyImport.
        let meta = session.metadata.as_ref().unwrap();
        assert_eq!(
            meta["createdByAgentId"],
            json!("agent-99999999-9999-4999-8999-999999999999")
        );
        let legacy = &meta["legacyImport"];
        assert_eq!(legacy["originalId"], json!(LEGACY_AGENT_ID));
        assert_eq!(legacy["originalStatus"], json!("Processing"));
        assert_eq!(legacy["lastActivity"], json!("2025-06-02T00:00:00Z"));
        assert_eq!(legacy["startedAt"], json!("2025-06-01T00:00:01Z"));
        assert_eq!(legacy["endedAt"], json!("2025-06-02T00:00:00Z"));
        assert_eq!(legacy["isInitialAgent"], json!(true));
        assert_eq!(legacy["forkPoint"], json!(2));
        assert_eq!(
            legacy["childSessionIds"],
            json!(["agent-11111111-1111-4111-8111-111111111111"])
        );
        assert_eq!(legacy["digest"], json!("Ported the store crate"));

        // Transcript: ordered, monotonic seq, roles mapped, blocks preserved.
        let msgs = &session.messages;
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].seq, 0);
        assert_eq!(msgs[1].seq, 1);
        assert_eq!(msgs[2].seq, 2);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(
            msgs[0].content,
            json!([{"type": "text", "text": "Port the store crate"}])
        );
        assert_eq!(msgs[0].created_at, "2025-06-01T00:00:02Z");
        assert_eq!(msgs[1].role, "assistant");
        let blocks = msgs[1].content.as_array().unwrap();
        assert_eq!(blocks.len(), 5);
        assert_eq!(blocks[0]["type"], json!("thinking"));
        assert_eq!(blocks[1]["type"], json!("tool_use"));
        assert_eq!(blocks[2]["type"], json!("tool_result"));
        // Unknown block shape degraded to a text block.
        assert_eq!(blocks[3]["type"], json!("text"));
        assert!(blocks[3]["text"].as_str().unwrap().contains("mystery"));
        assert_eq!(blocks[4], json!({"type": "text", "text": "Done."}));
        // Legacy-only message fields folded into row metadata.
        let m1 = msgs[1].metadata.as_ref().unwrap();
        assert_eq!(m1["model"], json!("sonnet4.5"));
        assert_eq!(m1["legacyMessageId"], json!("msg-2"));
        assert!(m1["legacyFields"]["toolCalls"].is_array());
        // `error` role → system, error text preserved as a text block.
        assert_eq!(msgs[2].role, "system");
        assert_eq!(
            msgs[2].content,
            json!([{"type": "text", "text": "stream disconnected"}])
        );
        let m2 = msgs[2].metadata.as_ref().unwrap();
        assert_eq!(m2["legacyRole"], json!("error"));
    }

    #[tokio::test]
    async fn agent_import_is_idempotent_and_mints_ids_for_invalid() {
        let (root, _root_g) = temp_root("agents-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-agent-idem", &json!({}));
        write_legacy_agent(
            &ws_dir,
            "not-a-uuid.json",
            &json!({
                "id": "not-a-uuid",
                "name": "Oddball",
                "messages": [
                    {"role": "user", "contentBlocks": [{"type": "text", "text": "hi"}],
                     "timestamp": "2025-06-01T00:00:00Z"}
                ]
            })
            .to_string(),
        );
        let (store, _db_g) = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.entries[0].agents.sessions_imported, 1, "{report}");

        let ws_id = WorkspaceId::from("ws-agent-idem");
        let sessions = store.list_agent_sessions(&ws_id).await.unwrap();
        assert_eq!(sessions.len(), 1);
        let session = &sessions[0];
        // Invalid legacy id → fresh `agent-{uuid}`, original preserved.
        assert!(is_valid_agent_id(session.id.as_str()), "{}", session.id);
        assert_eq!(
            session.metadata.as_ref().unwrap()["legacyImport"]["originalId"],
            json!("not-a-uuid")
        );
        assert_eq!(session.messages.len(), 1);

        // Force re-run: matched by preserved originalId, skipped whole —
        // no duplicate session, no duplicate messages.
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].agents.sessions_imported, 0, "{report}");
        assert_eq!(report.entries[0].agents.sessions_skipped, 1, "{report}");
        assert_eq!(report.entries[0].agents.messages_imported, 0, "{report}");
        let sessions = store.list_agent_sessions(&ws_id).await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].messages.len(), 1);
    }

    #[tokio::test]
    async fn imported_agent_sessions_are_never_swept_as_interrupted() {
        let (root, _root_g) = temp_root("agents-heal");
        let ws_dir = write_legacy_workspace(&root, "ws-agent-heal", &json!({}));
        // Legacy file frozen mid-flight ("Processing"): imports as Completed.
        write_legacy_agent(
            &ws_dir,
            &format!("{LEGACY_AGENT_ID}.json"),
            &legacy_agent_fixture().to_string(),
        );
        let (store, _db_g) = open_store().await;
        run(&store, &opts(vec![root.clone()])).await.unwrap();

        // The startup heal sweep must not touch the imported session.
        let services = intent_services::Services::new(store.clone());
        let healed = services.heal_stale_agent_sessions().await.unwrap();
        assert_eq!(healed, 0);
        assert!(store.list_interrupted_agents().await.unwrap().is_empty());
        let session = store
            .get_agent_session(&AgentId::from(LEGACY_AGENT_ID))
            .await
            .unwrap();
        assert_eq!(session.status, AgentStatus::Completed);
        assert!(!session.is_active);
    }

    /// Symlinks under `.workspace/` are never followed: a hostile link
    /// pointing outside the legacy tree must not be imported as an asset,
    /// note, or agent transcript.
    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_files_are_never_imported() {
        let (root, _root_g) = temp_root("symlink");
        let (outside, _outside_g) = temp_root("symlink-outside");
        std::fs::write(outside.join("secret.txt"), b"outside-data").unwrap();
        let ws_dir = write_legacy_workspace(&root, "ws-symlink", &json!({}));
        let assets_dir = ws_dir.join(".workspace").join("assets");
        let notes_dir = ws_dir.join(".workspace").join("notes");
        std::fs::create_dir_all(&assets_dir).unwrap();
        std::fs::create_dir_all(&notes_dir).unwrap();
        std::os::unix::fs::symlink(outside.join("secret.txt"), assets_dir.join("linked-asset"))
            .unwrap();
        std::os::unix::fs::symlink(outside.join("secret.txt"), notes_dir.join("linked.md"))
            .unwrap();
        // A real asset next to the symlink still imports.
        std::fs::write(assets_dir.join("real.bin"), b"real").unwrap();
        // A workspace whose manifest itself is a symlink is skipped whole.
        let evil_manifest = json!({ "id": "ws-evil-manifest", "title": "Evil" });
        std::fs::write(
            outside.join("workspace.json"),
            serde_json::to_string(&evil_manifest).unwrap(),
        )
        .unwrap();
        let evil_ws = root.join("ws-evil-manifest").join(".workspace");
        std::fs::create_dir_all(&evil_ws).unwrap();
        std::os::unix::fs::symlink(
            outside.join("workspace.json"),
            evil_ws.join("workspace.json"),
        )
        .unwrap();
        // A symlinked workspace DIRECTORY pointing outside the root is never
        // a discovery candidate either.
        let (outside_ws, _outside_ws_g) = temp_root("symlink-outside-ws");
        let outside_ws_dir = outside_ws.join("ws-evil-dir");
        let outside_meta = outside_ws_dir.join(".workspace");
        std::fs::create_dir_all(&outside_meta).unwrap();
        std::fs::write(
            outside_meta.join("workspace.json"),
            serde_json::to_string(&json!({ "id": "ws-evil-dir", "title": "Evil dir" })).unwrap(),
        )
        .unwrap();
        std::os::unix::fs::symlink(&outside_ws_dir, root.join("ws-evil-dir")).unwrap();
        let (assets_root, _assets_g) = temp_root("symlink-dest");
        let (store, _db_g) = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                assets_root: Some(assets_root.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        // The symlinked manifest/directory never become candidates: no report
        // entries and no workspace rows for them.
        assert_eq!(report.entries.len(), 1, "{report}");
        assert!(store
            .get_workspace(&WorkspaceId::from("ws-evil-dir"))
            .await
            .is_err());
        let entry = &report.entries[0];
        assert_eq!(entry.id, "ws-symlink", "{report}");
        assert_eq!(entry.assets.imported, 1, "{report}");
        assert_eq!(entry.notes.total(), 0, "{report}");
        assert!(store
            .get_workspace(&WorkspaceId::from("ws-evil-manifest"))
            .await
            .is_err());
        let dest = assets_root.join("ws-symlink");
        assert!(dest.join("real.bin").is_file());
        assert!(!dest.join("linked-asset").exists());
    }

    #[tokio::test]
    async fn imports_assets_into_workspace_scoped_root() {
        let (root, _root_g) = temp_root("assets");
        let (assets_root, _assets_g) = temp_root("assets-dest");
        let ws_dir = write_legacy_workspace(&root, "ws-assets", &json!({}));
        let src = ws_dir.join(".workspace").join("assets");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("img1.png"), b"png-bytes").unwrap();
        std::fs::write(
            src.join("img1.png.meta.json"),
            "{\"mimeType\":\"image/png\"}",
        )
        .unwrap();
        // Dotfiles and subdirectories are ignored.
        std::fs::write(src.join(".DS_Store"), "x").unwrap();
        std::fs::create_dir_all(src.join("subdir")).unwrap();
        let (store, _db_g) = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                assets_root: Some(assets_root.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].assets.imported, 2, "{report}");
        assert_eq!(report.entries[0].assets.failed, 0, "{report}");
        let dest = assets_root.join("ws-assets");
        assert_eq!(std::fs::read(dest.join("img1.png")).unwrap(), b"png-bytes");
        assert!(dest.join("img1.png.meta.json").is_file());
        assert!(!dest.join(".DS_Store").exists());
        assert!(!dest.join("subdir").exists());

        // Re-run with force: existing destination files are skipped, never
        // overwritten.
        std::fs::write(dest.join("img1.png"), b"already-migrated").unwrap();
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                assets_root: Some(assets_root.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].assets.imported, 0, "{report}");
        assert_eq!(report.entries[0].assets.skipped, 2, "{report}");
        assert_eq!(
            std::fs::read(dest.join("img1.png")).unwrap(),
            b"already-migrated"
        );
    }

    #[tokio::test]
    async fn no_assets_dir_or_root_is_a_noop() {
        let (root, _root_g) = temp_root("assets-none");
        write_legacy_workspace(&root, "ws-no-assets", &json!({}));
        let (store, _db_g) = open_store().await;
        // assets_root set but the legacy workspace has no assets dir.
        let (assets_root, _assets_g) = temp_root("assets-none-dest");
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                assets_root: Some(assets_root.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(report.entries[0].assets, AssetCounts::default(), "{report}");
        assert!(!assets_root.join("ws-no-assets").exists());
    }

    /// Write app-level `repo-registry.json` / `config.json` fixtures into a dir.
    fn write_app_file(dir: &Path, name: &str, value: &Value) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(name), value.to_string()).unwrap();
    }

    #[tokio::test]
    async fn imports_app_level_blobs_when_absent() {
        let (root, _root_g) = temp_root("app-blobs");
        let (app_dir, _app_g) = temp_root("app-dir");
        write_legacy_workspace(&root, "ws-hist", &json!({}));
        write_app_file(
            &app_dir,
            "repo-registry.json",
            &json!({"knownRepos": [
                {"path": "/tmp/repo-a", "name": "repo-a", "addedAt": "2025-01-01T00:00:00Z", "lastUsedAt": "2025-06-01T00:00:00Z"},
                {"path": "/tmp/repo-b", "name": "repo-b", "addedAt": "2025-02-01T00:00:00Z", "lastUsedAt": "2025-05-01T00:00:00Z"}
            ]}),
        );
        write_app_file(
            &app_dir,
            "config.json",
            &json!({"changeHistory": {
                "ws-hist": [{"file": "a.rs", "summary": "changed"}],
                "ws-unknown": [{"file": "b.rs", "summary": "dropped"}]
            }}),
        );
        let (store, _db_g) = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                app_dir: Some(app_dir.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let app = report.app_settings.expect("app settings ran");
        assert_eq!(app.repos_imported, 2, "{report}");
        assert_eq!(app.history_imported, 1, "{report}");
        assert_eq!(app.history_filtered, 1, "{report}");
        assert_eq!(app.failed, 0, "{report}");

        let repos: Value =
            serde_json::from_str(&store.get_setting("repos.known").await.unwrap().unwrap())
                .unwrap();
        assert_eq!(repos.as_array().unwrap().len(), 2);
        let history: Value = serde_json::from_str(
            &store
                .get_setting("workspace.changeHistory")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(history.get("ws-hist").is_some());
        assert!(history.get("ws-unknown").is_none());
    }

    #[tokio::test]
    async fn app_level_blobs_never_clobber_existing_settings() {
        let (root, _root_g) = temp_root("app-preserve");
        let (app_dir, _app_g) = temp_root("app-preserve-dir");
        write_legacy_workspace(&root, "ws-keep", &json!({}));
        write_app_file(
            &app_dir,
            "repo-registry.json",
            &json!({"knownRepos": [{"path": "/tmp/new", "name": "new"}]}),
        );
        write_app_file(
            &app_dir,
            "config.json",
            &json!({"changeHistory": {"ws-keep": [{"file": "x"}]}}),
        );
        let (store, _db_g) = open_store().await;
        store
            .set_setting("repos.known", &json!([{"path": "/tmp/mine"}]).to_string())
            .await
            .unwrap();
        store
            .set_setting(
                "workspace.changeHistory",
                &json!({"ws-mine": []}).to_string(),
            )
            .await
            .unwrap();

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                app_dir: Some(app_dir.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let app = report.app_settings.expect("app settings ran");
        assert!(app.repos_preserved, "{report}");
        assert!(app.history_preserved, "{report}");
        assert_eq!(app.repos_imported, 0, "{report}");
        assert_eq!(app.history_imported, 0, "{report}");

        let repos: Value =
            serde_json::from_str(&store.get_setting("repos.known").await.unwrap().unwrap())
                .unwrap();
        assert_eq!(repos[0]["path"], json!("/tmp/mine"));
        let history: Value = serde_json::from_str(
            &store
                .get_setting("workspace.changeHistory")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(history.get("ws-mine").is_some());
    }

    #[tokio::test]
    async fn app_dir_dry_run_missing_or_malformed_files() {
        let (root, _root_g) = temp_root("app-edge");
        write_legacy_workspace(&root, "ws-edge", &json!({}));
        let (store, _db_g) = open_store().await;

        // Dry-run never touches app settings even with an app_dir configured.
        let (app_dir, _app_g) = temp_root("app-edge-dir");
        write_app_file(
            &app_dir,
            "repo-registry.json",
            &json!({"knownRepos": [{"path": "/tmp/x"}]}),
        );
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                dry_run: true,
                app_dir: Some(app_dir.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        assert!(report.app_settings.is_none(), "{report}");
        assert!(store.get_setting("repos.known").await.unwrap().is_none());

        // Missing files: soft no-op. Malformed files: counted failed.
        std::fs::remove_dir_all(&app_dir).ok();
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("config.json"), "{ nope").unwrap();
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                force: true,
                app_dir: Some(app_dir.clone()),
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let app = report.app_settings.expect("app settings ran");
        assert_eq!(app.failed, 1, "{report}");
        assert_eq!(app.repos_imported, 0, "{report}");
        assert!(store.get_setting("repos.known").await.unwrap().is_none());
    }

    #[test]
    fn setting_emptiness_probe() {
        assert!(setting_is_empty(None));
        assert!(setting_is_empty(Some("null")));
        assert!(setting_is_empty(Some("[]")));
        assert!(setting_is_empty(Some("{}")));
        assert!(setting_is_empty(Some("\"\"")));
        // Unparseable → preserved (non-empty), never overwritten.
        assert!(!setting_is_empty(Some("not json")));
        assert!(!setting_is_empty(Some("[1]")));
        assert!(!setting_is_empty(Some("{\"a\":1}")));
        assert!(!setting_is_empty(Some("\"x\"")));
    }
}
