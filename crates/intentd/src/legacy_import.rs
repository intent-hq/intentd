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
//! Later per-workspace importers (comments, agent transcripts) plug into
//! [`import_workspace_extras`], which receives each imported workspace's
//! legacy directory. Notes import is implemented there: legacy
//! `.workspace/notes/{id}.md` files (YAML frontmatter + markdown body) become
//! `note` rows — `spec.md` lands as the well-known `spec` note, frontmatter
//! `task:` maps to task metadata (`task_json`), and `parent` to `parent_id`.
//! The `.meta/` sidecar (versions/CRDT/trash) is skipped entirely.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use intent_core::{
    now_iso, ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, TaskMetadata,
    Workspace,
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

/// Per-workspace line of the final report.
#[derive(Debug, Clone)]
pub struct WorkspaceReport {
    pub id: String,
    pub dir: PathBuf,
    pub outcome: Outcome,
    pub notes: NoteCounts,
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

    fn count(&self, pred: impl Fn(&Outcome) -> bool) -> usize {
        self.entries.iter().filter(|e| pred(&e.outcome)).count()
    }

    fn skip(&mut self, id: impl Into<String>, dir: &Path, reason: impl Into<String>) {
        self.entries.push(WorkspaceReport {
            id: id.into(),
            dir: dir.to_path_buf(),
            outcome: Outcome::Skipped(reason.into()),
            notes: NoteCounts::default(),
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
            writeln!(
                f,
                "  {}  {}{notes} ({})",
                entry.id,
                outcome,
                entry.dir.display()
            )?;
        }
        write!(
            f,
            "summary: {} imported, {} updated, {} skipped, {} notes imported",
            self.imported(),
            self.updated(),
            self.skipped(),
            self.notes_imported()
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
    };
    if landed && !opts.dry_run {
        import_workspace_extras(store, &ws, dir, &mut entry).await;
    }
    report.entries.push(entry);
}

/// Extension seam for the per-workspace importers: called once per
/// imported/updated workspace with its legacy directory (`<root>/<id>`,
/// containing `.workspace/…`). Currently imports notes; follow-ups (comments,
/// agent transcripts) plug in here.
async fn import_workspace_extras(
    store: &Store,
    workspace: &Workspace,
    legacy_dir: &Path,
    entry: &mut WorkspaceReport,
) {
    entry.notes = import_workspace_notes(store, workspace, legacy_dir).await;
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
}
