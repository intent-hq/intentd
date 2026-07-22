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
//! Later per-workspace importers (notes, comments, agent transcripts) plug
//! into [`import_workspace_extras`], which receives each imported workspace's
//! legacy directory.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use intent_core::{now_iso, Error, Workspace};
use intent_store::Store;
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

/// Per-workspace line of the final report.
#[derive(Debug, Clone)]
pub struct WorkspaceReport {
    pub id: String,
    pub dir: PathBuf,
    pub outcome: Outcome,
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

    fn count(&self, pred: impl Fn(&Outcome) -> bool) -> usize {
        self.entries.iter().filter(|e| pred(&e.outcome)).count()
    }

    fn skip(&mut self, id: impl Into<String>, dir: &Path, reason: impl Into<String>) {
        self.entries.push(WorkspaceReport {
            id: id.into(),
            dir: dir.to_path_buf(),
            outcome: Outcome::Skipped(reason.into()),
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
            writeln!(f, "  {}  {} ({})", entry.id, outcome, entry.dir.display())?;
        }
        write!(
            f,
            "summary: {} imported, {} updated, {} skipped",
            self.imported(),
            self.updated(),
            self.skipped()
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
    report.entries.push(WorkspaceReport {
        id,
        dir: dir.to_path_buf(),
        outcome,
    });
    if landed && !opts.dry_run {
        import_workspace_extras(store, &ws, dir, report).await;
    }
}

/// Extension seam for the follow-up importers (notes, comments, agent
/// transcripts): called once per imported/updated workspace with its legacy
/// directory (`<root>/<id>`, containing `.workspace/…`). Currently a no-op.
async fn import_workspace_extras(
    _store: &Store,
    _workspace: &Workspace,
    _legacy_dir: &Path,
    _report: &mut Report,
) {
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
}
