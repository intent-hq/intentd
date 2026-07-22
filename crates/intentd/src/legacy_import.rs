//! Legacy workspace import — migrate per-directory Intent workspaces
//! (`<root>/<id>/.workspace/workspace.json`) into intentd's SQLite store.
//!
//! Legacy roots scanned by default: `~/intent/workspaces`, `~/intent`,
//! `~/.workspaces`. Only directories carrying `.workspace/workspace.json` are
//! candidates; everything else is ignored. The importer is read-only toward
//! the source and idempotent: ids already present in the DB are skipped
//! (updated only with `--force`).
//!
//! Two entry points share this module:
//! - [`maybe_import_on_first_boot`] — fired by `intentd serve` only when the
//!   SQLite DB file did not exist before open AND no
//!   [`LEGACY_IMPORT_MARKER_KEY`] setting is present. It never fails startup;
//!   the marker is written only on successful completion.
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

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, AuthorType, Comment, CommentAnchor,
    CommentAnchorType, CommentStatus, CommentType, ContentType, Error, Note, NoteId, NoteMetadata,
    NoteVisibility, TaskMetadata, Workspace,
};
use intent_store::Store;
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// Settings-table marker written after a successful non-dry-run import so the
/// first-boot hook never re-runs. Value: a JSON string RFC-3339 timestamp.
pub const LEGACY_IMPORT_MARKER_KEY: &str = "import.legacyCompletedAt";

/// Legacy-only `workspace.json` fields intentd does not model — dropped on
/// import (the FE `WorkspaceSchema` extras written next to the §9.1 fields).
const LEGACY_ONLY_FIELDS: &[&str] = &["changesets", "conversationInfo", "timeline"];

/// Inputs for one import run.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Legacy roots to scan, in priority order (first occurrence of an id wins).
    pub roots: Vec<PathBuf>,
    /// Report what would happen without writing anything.
    pub dry_run: bool,
    /// Update rows whose id already exists instead of skipping them.
    pub force: bool,
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
    /// Malformed message entries or failed message inserts (logged, never
    /// fatal; the rest of the transcript still lands).
    pub messages_failed: usize,
}

impl AgentCounts {
    fn total(&self) -> usize {
        self.sessions_imported + self.sessions_skipped + self.sessions_failed
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
}

/// Full report of one run: one entry per candidate workspace directory.
#[derive(Debug, Default)]
pub struct Report {
    pub entries: Vec<WorkspaceReport>,
    pub dry_run: bool,
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

    fn count(&self, pred: impl Fn(&Outcome) -> bool) -> usize {
        self.entries.iter().filter(|e| pred(&e.outcome)).count()
    }

    fn skip(&mut self, id: impl Into<String>, dir: &Path, reason: impl Into<String>) {
        self.entries.push(WorkspaceReport {
            id: id.into(),
            dir: dir.to_path_buf(),
            outcome: Outcome::Skipped(reason.into()),
            notes: NoteCounts::default(),
            comments: CommentCounts::default(),
            agents: AgentCounts::default(),
        });
    }
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
            writeln!(
                f,
                "  {}  {}{notes}{comments}{agents} ({})",
                entry.id,
                outcome,
                entry.dir.display()
            )?;
        }
        write!(
            f,
            "summary: {} imported, {} updated, {} skipped, {} notes imported, {} comments imported, {} agent sessions imported, {} agent messages imported",
            self.imported(),
            self.updated(),
            self.skipped(),
            self.notes_imported(),
            self.comments_imported(),
            self.agent_sessions_imported(),
            self.agent_messages_imported()
        )
    }
}

/// Default legacy roots. `INTENTD_LEGACY_IMPORT_ROOTS` (colon-separated; empty
/// disables the scan) overrides; under a hermetic test harness
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
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return Vec::new();
    };
    vec![
        home.join("intent").join("workspaces"),
        home.join("intent"),
        home.join(".workspaces"),
    ]
}

/// Scan `opts.roots` in order and import every legacy workspace found. Missing
/// or unreadable roots are skipped silently (the default roots may simply not
/// exist); per-workspace problems are soft and reported as [`Outcome::Skipped`].
/// The run is read-only toward the source directories.
pub async fn run(store: &Store, opts: &Options) -> anyhow::Result<Report> {
    let mut report = Report {
        dry_run: opts.dry_run,
        ..Report::default()
    };
    let mut seen: HashSet<String> = HashSet::new();
    for root in &opts.roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        let mut dirs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        for dir in dirs {
            let manifest = dir.join(".workspace").join("workspace.json");
            if !manifest.is_file() {
                continue;
            }
            import_one(store, &dir, &manifest, opts, &mut seen, &mut report).await;
        }
    }
    Ok(report)
}

/// Import one candidate workspace directory, appending its outcome to `report`.
async fn import_one(
    store: &Store,
    dir: &Path,
    manifest: &Path,
    opts: &Options,
    seen: &mut HashSet<String>,
    report: &mut Report,
) {
    // The legacy layout names the workspace dir after its id; used as the
    // report id when the manifest is unusable.
    let dir_id = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let text = match std::fs::read_to_string(manifest) {
        Ok(t) => t,
        Err(e) => {
            report.skip(dir_id, dir, format!("cannot read workspace.json: {e}"));
            return;
        }
    };
    let mut obj = match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(o)) => o,
        Ok(_) => {
            report.skip(dir_id, dir, "workspace.json is not a JSON object");
            return;
        }
        Err(e) => {
            report.skip(dir_id, dir, format!("invalid JSON in workspace.json: {e}"));
            return;
        }
    };
    // Prefer the manifest id; fall back to the directory name.
    let id = match obj.get("id").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ if !dir_id.is_empty() => {
            obj.insert("id".to_string(), json!(dir_id.clone()));
            dir_id.clone()
        }
        _ => {
            report.skip(dir_id, dir, "workspace.json has no id");
            return;
        }
    };
    if id == intent_core::CHIEF_WORKSPACE_ID {
        report.skip(id, dir, "virtual workspace id");
        return;
    }
    if !seen.insert(id.clone()) {
        report.skip(id, dir, "duplicate id already found under an earlier root");
        return;
    }
    let ws = match workspace_from_legacy_json(obj) {
        Ok(ws) => ws,
        Err(reason) => {
            report.skip(id, dir, reason);
            return;
        }
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
    let mut entry = WorkspaceReport {
        id,
        dir: dir.to_path_buf(),
        outcome,
        notes: NoteCounts::default(),
        comments: CommentCounts::default(),
        agents: AgentCounts::default(),
    };
    if landed && !opts.dry_run {
        import_workspace_extras(store, &ws, dir, &mut entry).await;
    }
    report.entries.push(entry);
}

/// Extension seam for the per-workspace importers: called once per
/// imported/updated workspace with its legacy directory (`<root>/<id>`,
/// containing `.workspace/…`). Imports notes, then comments (after notes, so
/// comments can attach to just-imported note rows), then agent transcripts.
async fn import_workspace_extras(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
    entry: &mut WorkspaceReport,
) {
    entry.notes = import_workspace_notes(store, workspace, legacy_dir).await;
    entry.comments = import_workspace_comments(store, workspace, legacy_dir).await;
    entry.agents = import_workspace_agents(store, workspace, legacy_dir).await;
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
    let Ok(entries) = std::fs::read_dir(&notes_dir) else {
        return counts; // no notes dir — nothing to import
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_file()
                && p.extension().is_some_and(|ext| ext == "md")
                && !p
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('.'))
        })
        .collect();
    files.sort();
    for path in files {
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = match std::fs::read_to_string(&path) {
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
    let created_at = fm.created.unwrap_or_else(now_iso);
    Note {
        id: NoteId::from(id.clone()),
        workspace_id: workspace.id.clone(),
        title: fm.title.unwrap_or_else(|| stem.to_string()),
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
    let Ok(entries) = std::fs::read_dir(&meta_dir) else {
        return counts; // no .meta dir — nothing to import
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(LEGACY_COMMENTS_SUFFIX))
        })
        .collect();
    files.sort();
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
        let text = match std::fs::read_to_string(&path) {
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
        _ => None,
    };
    // The sidecar duplicates the note id per entry; the file location is
    // authoritative, so drop it rather than persisting a stale copy.
    obj.remove("noteId");

    // `markId` is the legacy anchor-mark id → point anchor. Without one the
    // exact offsets (`from`/`to`) stay in `extra_json` and the anchor is a
    // default (unanchored) range.
    let anchor = match take_string(&mut obj, "markId") {
        Some(mark_id) => CommentAnchor {
            kind: CommentAnchorType::Point,
            point_id: Some(mark_id),
            ..Default::default()
        },
        None => CommentAnchor::default(),
    };

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
    let rest = match s.strip_prefix("agent-") {
        Some(r) => r,
        None => return false,
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
/// logged and counted without ever failing the workspace import. Imported
/// sessions are always terminal (`Completed`, not active) so the startup
/// interrupted-agent heal sweep never resumes them.
async fn import_workspace_agents(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
) -> AgentCounts {
    let mut counts = AgentCounts::default();
    let agents_dir = legacy_dir.join(".workspace").join("agents");
    let Ok(entries) = std::fs::read_dir(&agents_dir) else {
        return counts; // no agents dir — nothing to import
    };
    // Sidecar artifacts (`*.json.checksum`, `*.json.corrupted.{ts}`, backups,
    // `.health-check`) fail the extension/dotfile filter and are ignored.
    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_file()
                && p.extension().is_some_and(|ext| ext == "json")
                && !p
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with('.'))
        })
        .collect();
    files.sort();
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
        let text = match std::fs::read_to_string(&path) {
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
        if let Err(e) = store.insert_agent_session(&session).await {
            tracing::warn!(path = %path.display(), agent_id = %session.id, error = %e, "legacy agent session insert failed");
            counts.sessions_failed += 1;
            continue;
        }
        existing.insert(original_id);
        existing.insert(session.id.0.clone());
        counts.sessions_imported += 1;
        for raw in messages {
            let (role, content, metadata, created_at) = match message_from_legacy_json(raw) {
                Ok(parts) => parts,
                Err(reason) => {
                    tracing::warn!(path = %path.display(), agent_id = %session.id, reason, "legacy agent message malformed; skipping");
                    counts.messages_failed += 1;
                    continue;
                }
            };
            match store
                .append_agent_message_with_metadata(
                    &session.id,
                    &role,
                    &content,
                    metadata.as_ref(),
                    &created_at,
                )
                .await
            {
                Ok(_) => counts.messages_imported += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), agent_id = %session.id, error = %e, "legacy agent message insert failed");
                    counts.messages_failed += 1;
                }
            }
        }
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
    fn lift_string(m: &mut Map<String, Value>, key: &str) -> Option<String> {
        match m.get(key) {
            Some(Value::String(_)) => match m.remove(key) {
                Some(Value::String(s)) if !s.is_empty() => Some(s),
                _ => None,
            },
            _ => None,
        }
    }
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
        id,
        workspace_id: workspace.id.clone(),
        parent_agent_id,
        backend_session_id,
        acp_session_id: None,
        name,
        name_explicitly_set,
        model,
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
        delegation_depth,
        initial_message,
        context_references: None,
        image_blocks: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        is_background,
        metadata: Some(Value::Object(metadata)),
        stop_reason: None,
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
/// legacy-only FE fields, default the intentd-only required fields, and apply
/// the worktree fallback (a `worktreePath` that no longer exists on disk is
/// cleared and the workspace becomes `skipWorktree`; `branch` is kept as-is).
fn workspace_from_legacy_json(mut obj: Map<String, Value>) -> Result<Workspace, String> {
    for key in LEGACY_ONLY_FIELDS {
        obj.remove(*key);
    }
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

/// Write the [`LEGACY_IMPORT_MARKER_KEY`] settings row (a JSON string
/// timestamp) recording that a full import completed successfully.
pub async fn write_completion_marker(store: &Store) -> anyhow::Result<()> {
    store
        .set_setting(LEGACY_IMPORT_MARKER_KEY, &json!(now_iso()).to_string())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))
}

/// First-boot hook for `intentd serve`: run the import only when the DB file
/// did not exist before `Store::open` AND no completion marker is set. Runs
/// after migrations (inside `Store::open`) and before any transport serves
/// RPCs. Never fails startup — every failure is logged and swallowed; the
/// marker is written only when the run completes.
pub async fn maybe_import_on_first_boot(store: &Store, db_existed: bool, roots: Vec<PathBuf>) {
    if db_existed {
        return;
    }
    match store.get_setting(LEGACY_IMPORT_MARKER_KEY).await {
        Ok(None) => {}
        Ok(Some(_)) => return,
        Err(e) => {
            tracing::warn!(error = %e, "legacy import marker read failed; skipping import");
            return;
        }
    }
    let opts = Options {
        roots,
        dry_run: false,
        force: false,
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
                "first-boot legacy workspace import complete"
            );
            for entry in &report.entries {
                tracing::info!(id = %entry.id, outcome = ?entry.outcome, "legacy workspace");
            }
            if let Err(e) = write_completion_marker(store).await {
                tracing::warn!(error = %e, "legacy import marker write failed");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "first-boot legacy workspace import failed; daemon continues");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use intent_core::WorkspaceId;

    /// Fresh throwaway fixture root under the system temp dir (never `~/intent`
    /// — STAB-138: tests must not pollute the developer's real workspace dirs).
    fn temp_root(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "intentd-legacy-{tag}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    /// Write `<root>/<id>/.workspace/workspace.json` with `extra` fields merged
    /// over a minimal legacy manifest (including the FE-only legacy arrays).
    fn write_legacy_workspace(root: &Path, id: &str, extra: Value) -> PathBuf {
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

    async fn open_store() -> Store {
        let db = std::env::temp_dir().join(format!("intentd-legacy-{}.db", uuid::Uuid::new_v4()));
        Store::open(&db).await.expect("open store")
    }

    fn opts(roots: Vec<PathBuf>) -> Options {
        Options {
            roots,
            dry_run: false,
            force: false,
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
        let root = temp_root("import");
        write_legacy_workspace(&root, "ws-a", json!({}));
        write_legacy_workspace(
            &root,
            "ws-b",
            json!({"archived": true, "archivedAt": "2025-06-01T00:00:00Z"}),
        );
        // Entries without .workspace/workspace.json are ignored.
        std::fs::create_dir_all(root.join("not-a-workspace")).unwrap();
        std::fs::write(root.join("stray-file"), "x").unwrap();
        let store = open_store().await;

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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn dry_run_reports_plan_without_writing() {
        let root = temp_root("dry");
        write_legacy_workspace(&root, "ws-dry", json!({}));
        let store = open_store().await;

        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                dry_run: true,
                force: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.imported(), 1);
        assert!(report.to_string().contains("would import"), "{report}");
        assert!(store.list_workspaces(true).await.unwrap().is_empty());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn existing_ids_are_skipped_unless_forced() {
        let root = temp_root("idem");
        write_legacy_workspace(&root, "ws-x", json!({"title": "Old title"}));
        let store = open_store().await;
        run(&store, &opts(vec![root.clone()])).await.unwrap();

        // Second run: idempotent skip.
        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.imported(), 0);
        assert_eq!(report.skipped(), 1);
        assert!(report.to_string().contains("already in DB"), "{report}");

        // --force overwrites the existing row.
        write_legacy_workspace(&root, "ws-x", json!({"title": "New title"}));
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                dry_run: false,
                force: true,
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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_worktree_falls_back_to_skip_worktree() {
        let root = temp_root("worktree");
        let live_dir = temp_root("live-worktree");
        write_legacy_workspace(
            &root,
            "ws-live",
            json!({"worktreePath": live_dir.to_string_lossy(), "skipWorktree": false}),
        );
        write_legacy_workspace(
            &root,
            "ws-gone",
            json!({"worktreePath": "/nonexistent/legacy/worktree", "skipWorktree": false}),
        );
        let store = open_store().await;
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

        std::fs::remove_dir_all(&root).ok();
        std::fs::remove_dir_all(&live_dir).ok();
    }

    #[tokio::test]
    async fn skips_chief_duplicates_and_malformed_manifests() {
        let root_a = temp_root("roots-a");
        let root_b = temp_root("roots-b");
        write_legacy_workspace(&root_a, "__chief__", json!({}));
        write_legacy_workspace(&root_a, "ws-dup", json!({"title": "From root A"}));
        write_legacy_workspace(&root_b, "ws-dup", json!({"title": "From root B"}));
        let broken = root_a.join("ws-broken").join(".workspace");
        std::fs::create_dir_all(&broken).unwrap();
        std::fs::write(broken.join("workspace.json"), "{ nope").unwrap();
        let store = open_store().await;

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

        std::fs::remove_dir_all(&root_a).ok();
        std::fs::remove_dir_all(&root_b).ok();
    }

    #[tokio::test]
    async fn first_boot_hook_imports_once_and_writes_marker() {
        let root = temp_root("boot");
        write_legacy_workspace(&root, "ws-boot", json!({}));
        let store = open_store().await;

        // Fresh DB, no marker → import runs and the marker is written.
        maybe_import_on_first_boot(&store, false, vec![root.clone()]).await;
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);
        let marker = store.get_setting(LEGACY_IMPORT_MARKER_KEY).await.unwrap();
        assert!(
            marker.is_some_and(|m| m.starts_with('"')),
            "JSON string marker"
        );

        // Marker present → the hook is a no-op even on a "fresh" DB signal.
        write_legacy_workspace(&root, "ws-later", json!({}));
        maybe_import_on_first_boot(&store, false, vec![root.clone()]).await;
        assert_eq!(store.list_workspaces(true).await.unwrap().len(), 1);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn first_boot_hook_skips_preexisting_db() {
        let root = temp_root("boot-existing");
        write_legacy_workspace(&root, "ws-pre", json!({}));
        let store = open_store().await;

        maybe_import_on_first_boot(&store, true, vec![root.clone()]).await;
        assert!(store.list_workspaces(true).await.unwrap().is_empty());
        assert!(store
            .get_setting(LEGACY_IMPORT_MARKER_KEY)
            .await
            .unwrap()
            .is_none());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn source_is_never_mutated() {
        let root = temp_root("readonly");
        write_legacy_workspace(&root, "ws-ro", json!({}));
        let manifest = root.join("ws-ro").join(".workspace").join("workspace.json");
        let before = std::fs::read(&manifest).unwrap();
        let store = open_store().await;

        run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(std::fs::read(&manifest).unwrap(), before);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn imports_notes_with_frontmatter_spec_and_task() {
        let root = temp_root("notes");
        let ws_dir = write_legacy_workspace(&root, "ws-notes", json!({}));
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
        let store = open_store().await;

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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn malformed_note_frontmatter_imports_body_best_effort() {
        let root = temp_root("notes-malformed");
        let ws_dir = write_legacy_workspace(&root, "ws-bad-notes", json!({}));
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
        let store = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.entries[0].notes.imported, 2, "{report}");
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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn notes_import_is_idempotent_per_note_id() {
        let root = temp_root("notes-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-note-idem", json!({}));
        write_legacy_note(
            &ws_dir,
            "spec.md",
            "---\nid: spec\ntitle: Spec\n---\n\nOriginal spec\n",
        );
        let store = open_store().await;
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
                dry_run: false,
                force: true,
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

        std::fs::remove_dir_all(&root).ok();
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
        let root = temp_root("comments");
        let ws_dir = write_legacy_workspace(&root, "ws-comments", json!({}));
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
        let store = open_store().await;

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
        assert_eq!(root_c.anchor.kind, CommentAnchorType::Point);
        assert_eq!(root_c.anchor.point_id, Some("mark-abc".to_string()));
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

        assert!(store.get_comment("c-ghost").await.is_err());

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn comments_import_is_idempotent_and_survives_malformed_files() {
        let root = temp_root("comments-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-c-idem", json!({}));
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
        let store = open_store().await;

        let report = run(&store, &opts(vec![root.clone()])).await.unwrap();
        assert_eq!(report.entries[0].comments.imported, 1, "{report}");
        assert_eq!(report.entries[0].comments.failed, 1, "{report}");

        // Force re-run: existing comment id skipped, content untouched.
        let report = run(
            &store,
            &Options {
                roots: vec![root.clone()],
                dry_run: false,
                force: true,
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

        std::fs::remove_dir_all(&root).ok();
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
        let root = temp_root("agents");
        let ws_dir = write_legacy_workspace(&root, "ws-agents", json!({}));
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
        let store = open_store().await;

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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn agent_import_is_idempotent_and_mints_ids_for_invalid() {
        let root = temp_root("agents-idem");
        let ws_dir = write_legacy_workspace(&root, "ws-agent-idem", json!({}));
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
        let store = open_store().await;

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
                dry_run: false,
                force: true,
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

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn imported_agent_sessions_are_never_swept_as_interrupted() {
        let root = temp_root("agents-heal");
        let ws_dir = write_legacy_workspace(&root, "ws-agent-heal", json!({}));
        // Legacy file frozen mid-flight ("Processing"): imports as Completed.
        write_legacy_agent(
            &ws_dir,
            &format!("{LEGACY_AGENT_ID}.json"),
            &legacy_agent_fixture().to_string(),
        );
        let store = open_store().await;
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

        std::fs::remove_dir_all(&root).ok();
    }
}
