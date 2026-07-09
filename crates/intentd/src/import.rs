//! `intentd import --from <dir>` — migrate a legacy Intent (Electron) install
//! into intentd's SQLite store (§9.7).
//!
//! The importer is **read-only** toward the source (it only ever reads files)
//! and **idempotent** (every entity is upserted by id, so re-running never
//! creates duplicates). It reuses the existing `Store` repositories directly
//! (§3.2) and adds no JSON-RPC wire surface.
//!
//! ## Assumed source layout
//!
//! ```text
//! <userData>/
//!   workspaces.json                         # JSON array of Workspace (§9.1, camelCase)
//!   workspaces/<workspace-id>/.workspace/
//!     notes/*.json                          # one Note per file
//!     agents/*.json                         # one AgentSession per file (with messages[])
//!     comments/*.json                       # one Comment per file
//! ```
//!
//! Entity JSON matches the §9.1 wire model 1:1 (the TS→Rust field mapping is 1:1
//! by design). intentd-derived/owned fields that a legacy install does not carry
//! (`activity`, `attention`, …) are defaulted in before deserialization.

use std::fmt;
use std::path::{Path, PathBuf};

use intent_core::{AgentSession, Comment, Error, Note, Workspace};
use intent_store::Store;
use serde_json::{json, Map, Value};

const WORKSPACE_SUBDIR: &str = ".workspace";

/// Per-domain counts plus soft (per-entity) errors from an import run (§9.7).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportSummary {
    pub workspaces_imported: usize,
    pub workspaces_updated: usize,
    pub notes_imported: usize,
    pub notes_updated: usize,
    pub agents_imported: usize,
    pub agents_updated: usize,
    pub messages_imported: usize,
    pub comments_imported: usize,
    pub comments_updated: usize,
    pub skipped: usize,
    pub errors: Vec<String>,
}

impl ImportSummary {
    /// Record a per-entity problem: count it as skipped and keep the reason for
    /// the final report. Soft errors never abort the run (partial import).
    fn skip(&mut self, reason: String) {
        self.skipped += 1;
        self.errors.push(reason);
    }
}

impl fmt::Display for ImportSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "import summary:")?;
        writeln!(
            f,
            "  workspaces: {} imported, {} updated",
            self.workspaces_imported, self.workspaces_updated
        )?;
        writeln!(
            f,
            "  notes:      {} imported, {} updated",
            self.notes_imported, self.notes_updated
        )?;
        writeln!(
            f,
            "  agents:     {} imported, {} updated ({} messages)",
            self.agents_imported, self.agents_updated, self.messages_imported
        )?;
        writeln!(
            f,
            "  comments:   {} imported, {} updated",
            self.comments_imported, self.comments_updated
        )?;
        write!(f, "  skipped:    {}", self.skipped)?;
        for reason in &self.errors {
            write!(f, "\n    - {reason}")?;
        }
        Ok(())
    }
}

/// Run the import: read `<source>/workspaces.json` and each workspace's
/// `.workspace/` entities, upserting all into `store`. Returns the summary, or a
/// hard error for an unusable source (missing dir, or a missing/malformed
/// `workspaces.json`). Per-entity problems are soft and counted as `skipped`.
pub async fn run(store: &Store, source: &Path) -> anyhow::Result<ImportSummary> {
    if !source.is_dir() {
        anyhow::bail!("import source is not a directory: {}", source.display());
    }
    let workspaces_path = source.join("workspaces.json");
    let text = std::fs::read_to_string(&workspaces_path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", workspaces_path.display()))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", workspaces_path.display()))?;
    let workspaces = value.as_array().ok_or_else(|| {
        anyhow::anyhow!(
            "{} must be a JSON array of workspaces",
            workspaces_path.display()
        )
    })?;

    let mut summary = ImportSummary::default();
    for entry in workspaces {
        import_workspace(store, source, entry, &mut summary).await;
    }
    Ok(summary)
}

/// Fill `obj` defaults for keys it does not already carry, so legacy entities
/// missing intentd-only required fields still deserialize.
fn fill_defaults(obj: &mut Map<String, Value>, defaults: &[(&str, Value)]) {
    for (key, default) in defaults {
        obj.entry(*key).or_insert_with(|| default.clone());
    }
}

/// Read every `*.json` file in `dir` (sorted) as a JSON object, recording any
/// unreadable/non-object/invalid file as a soft skip. A missing dir yields none.
fn load_objects(dir: &Path, summary: &mut ImportSummary) -> Vec<Map<String, Value>> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        match std::fs::read_to_string(&path) {
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(Value::Object(o)) => out.push(o),
                Ok(_) => summary.skip(format!("{} is not a JSON object", path.display())),
                Err(e) => summary.skip(format!("invalid JSON in {}: {e}", path.display())),
            },
            Err(e) => summary.skip(format!("cannot read {}: {e}", path.display())),
        }
    }
    out
}

/// Upsert one workspace and its `.workspace/` notes, agents, and comments.
async fn import_workspace(
    store: &Store,
    source: &Path,
    value: &Value,
    summary: &mut ImportSummary,
) {
    let Some(mut obj) = value.as_object().cloned() else {
        summary.skip("workspace entry is not a JSON object".to_string());
        return;
    };
    fill_defaults(
        &mut obj,
        &[
            ("status", json!("Active")),
            ("activity", json!("idle")),
            ("attention", json!("none")),
            ("tags", json!([])),
            ("skipWorktree", json!(false)),
            ("isRemote", json!(false)),
            ("archived", json!(false)),
        ],
    );
    let ws: Workspace = match serde_json::from_value(Value::Object(obj)) {
        Ok(w) => w,
        Err(e) => {
            summary.skip(format!("workspace parse failed: {e}"));
            return;
        }
    };
    let id = ws.id.clone();
    match upsert_workspace(store, &ws).await {
        Ok(true) => summary.workspaces_updated += 1,
        Ok(false) => summary.workspaces_imported += 1,
        Err(e) => {
            summary.skip(format!("workspace {id} upsert failed: {e}"));
            return;
        }
    }
    let ws_dir = source.join("workspaces").join(&id.0).join(WORKSPACE_SUBDIR);
    import_notes(store, &ws_dir.join("notes"), summary).await;
    import_agents(store, &ws_dir.join("agents"), summary).await;
    import_comments(store, &id, &ws_dir.join("comments"), summary).await;
}

/// Upsert a workspace by id; `Ok(true)` when it already existed (updated).
async fn upsert_workspace(store: &Store, ws: &Workspace) -> anyhow::Result<bool> {
    match store.get_workspace(&ws.id).await {
        Ok(_) => {
            store.update_workspace(ws).await?;
            Ok(true)
        }
        Err(Error::NotFound(_)) => {
            store.insert_workspace(ws).await?;
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// Import a workspace's notes. Done in two passes so a child never precedes its
/// parent through the `note.parent_id` self-FK: pass 1 upserts every note with
/// the parent stripped; pass 2 re-applies parents once all rows exist.
async fn import_notes(store: &Store, dir: &Path, summary: &mut ImportSummary) {
    let mut notes = Vec::new();
    for mut obj in load_objects(dir, summary) {
        fill_defaults(
            &mut obj,
            &[
                ("content", json!("")),
                ("contentType", json!("markdown")),
                ("tags", json!([])),
                ("isPinned", json!(false)),
                ("isArchived", json!(false)),
                ("isDefault", json!(false)),
                ("visibility", json!("workspace")),
                ("rev", json!(0)),
            ],
        );
        match serde_json::from_value::<Note>(Value::Object(obj)) {
            Ok(n) => notes.push(n),
            Err(e) => summary.skip(format!("note parse failed: {e}")),
        }
    }
    let mut applied = Vec::with_capacity(notes.len());
    for note in &notes {
        let mut flat = note.clone();
        flat.parent_id = None;
        let ok = match upsert_note(store, &flat).await {
            Ok(true) => {
                summary.notes_updated += 1;
                true
            }
            Ok(false) => {
                summary.notes_imported += 1;
                true
            }
            Err(e) => {
                summary.skip(format!("note {} upsert failed: {e}", note.id));
                false
            }
        };
        applied.push(ok);
    }
    for (note, &ok) in notes.iter().zip(&applied) {
        if ok && note.parent_id.is_some() {
            if let Err(e) = store.update_note(note).await {
                summary.skip(format!("note {} parent link failed: {e}", note.id));
            }
        }
    }
}

/// Upsert a note by id; `Ok(true)` when it already existed (updated).
async fn upsert_note(store: &Store, note: &Note) -> anyhow::Result<bool> {
    match store.get_note(&note.id).await {
        Ok(_) => {
            store.update_note(note).await?;
            Ok(true)
        }
        Err(Error::NotFound(_)) => {
            store.insert_note(note).await?;
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// Import a workspace's agent sessions and their persisted message logs. Only a
/// session's metadata + already-persisted messages are imported (no live
/// runtime, §9.7). Because the message log is append-only, messages are written
/// only when the session is newly inserted; a re-import leaves them untouched so
/// the run stays idempotent.
async fn import_agents(store: &Store, dir: &Path, summary: &mut ImportSummary) {
    for mut obj in load_objects(dir, summary) {
        fill_defaults(&mut obj, &[("status", json!("idle"))]);
        let session: AgentSession = match serde_json::from_value(Value::Object(obj)) {
            Ok(s) => s,
            Err(e) => {
                summary.skip(format!("agent parse failed: {e}"));
                continue;
            }
        };
        match store.get_agent_session(&session.id).await {
            Ok(_) => match store
                .update_agent_session(&session.workspace_id.clone(), &session)
                .await
            {
                Ok(()) => summary.agents_updated += 1,
                Err(e) => summary.skip(format!("agent {} update failed: {e}", session.id)),
            },
            Err(Error::NotFound(_)) => {
                if let Err(e) = store.insert_agent_session(&session).await {
                    summary.skip(format!("agent {} insert failed: {e}", session.id));
                    continue;
                }
                summary.agents_imported += 1;
                for msg in &session.messages {
                    match store
                        .append_agent_message(&session.id, &msg.role, &msg.content, &msg.created_at)
                        .await
                    {
                        Ok(_) => summary.messages_imported += 1,
                        Err(e) => summary.skip(format!("agent {} message failed: {e}", session.id)),
                    }
                }
            }
            Err(e) => summary.skip(format!("agent {} lookup failed: {e}", session.id)),
        }
    }
}

/// Import a workspace's comments, upserting each by id.
async fn import_comments(
    store: &Store,
    workspace_id: &intent_core::WorkspaceId,
    dir: &Path,
    summary: &mut ImportSummary,
) {
    for mut obj in load_objects(dir, summary) {
        fill_defaults(
            &mut obj,
            &[
                ("type", json!("comment")),
                ("status", json!("open")),
                ("authorType", json!("user")),
                ("anchor", json!({ "type": "range" })),
            ],
        );
        let comment: Comment = match serde_json::from_value(Value::Object(obj)) {
            Ok(c) => c,
            Err(e) => {
                summary.skip(format!("comment parse failed: {e}"));
                continue;
            }
        };
        match store.get_comment(&comment.id).await {
            Ok(_) => match store.update_comment(workspace_id, &comment).await {
                Ok(()) => summary.comments_updated += 1,
                Err(e) => summary.skip(format!("comment {} update failed: {e}", comment.id)),
            },
            Err(Error::NotFound(_)) => match store.insert_comment(workspace_id, &comment).await {
                Ok(()) => summary.comments_imported += 1,
                Err(e) => summary.skip(format!("comment {} insert failed: {e}", comment.id)),
            },
            Err(e) => summary.skip(format!("comment {} lookup failed: {e}", comment.id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use intent_core::{AgentId, NoteId, WorkspaceId};

    /// Build a self-contained fixture `userData` dir with one workspace holding
    /// two notes (one a child of the other), one agent session with two
    /// messages, and one comment. Returns the source dir path.
    fn write_fixture() -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("intentd-import-{}", uuid::Uuid::new_v4().simple()));
        let ws_dir = root.join("workspaces").join("ws-1").join(".workspace");
        for sub in ["notes", "agents", "comments"] {
            std::fs::create_dir_all(ws_dir.join(sub)).unwrap();
        }
        std::fs::write(
            root.join("workspaces.json"),
            serde_json::to_string_pretty(&json!([{
                "id": "ws-1",
                "title": "Imported WS",
                "branch": "main",
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
                "tags": ["seed"]
            }]))
            .unwrap(),
        )
        .unwrap();
        let notes = ws_dir.join("notes");
        std::fs::write(
            notes.join("spec.json"),
            json!({
                "id": "note-parent", "workspaceId": "ws-1", "title": "Spec",
                "content": "# Spec", "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            notes.join("child.json"),
            json!({
                "id": "note-child", "workspaceId": "ws-1", "title": "Child",
                "content": "body", "parentId": "note-parent",
                "createdAt": "2026-01-02T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            ws_dir.join("agents").join("a1.json"),
            json!({
                "id": "agent-1", "workspaceId": "ws-1", "name": "Worker",
                "status": "idle", "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
                "messages": [
                    {"id": "m1", "agentId": "agent-1", "seq": 0, "role": "user",
                     "contentBlocks": "hi", "timestamp": "2026-01-01T00:00:01Z"},
                    {"id": "m2", "agentId": "agent-1", "seq": 1, "role": "assistant",
                     "contentBlocks": "hello", "timestamp": "2026-01-01T00:00:02Z"}
                ]
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            ws_dir.join("comments").join("c1.json"),
            json!({
                "id": "c1", "threadId": "t1", "noteId": "note-parent",
                "type": "comment", "content": "nice", "author": "User",
                "authorType": "user", "status": "open",
                "anchor": {"type": "range"},
                "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        root
    }

    /// A sorted snapshot of every file under `dir` as `(relative path, bytes)`,
    /// used to assert the importer never mutates its source.
    fn snapshot(dir: &Path) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).unwrap().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let rel = path
                        .strip_prefix(dir)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned();
                    out.push((rel, std::fs::read(&path).unwrap()));
                }
            }
        }
        out.sort();
        out
    }

    async fn open_store() -> Store {
        let db = std::env::temp_dir().join(format!("intentd-import-{}.db", uuid::Uuid::new_v4()));
        Store::open(&db).await.expect("open store")
    }

    #[tokio::test]
    async fn imports_all_domains_with_correct_counts() {
        let source = write_fixture();
        let store = open_store().await;

        let summary = run(&store, &source).await.expect("import");
        assert_eq!(summary.workspaces_imported, 1);
        assert_eq!(summary.notes_imported, 2);
        assert_eq!(summary.agents_imported, 1);
        assert_eq!(summary.messages_imported, 2);
        assert_eq!(summary.comments_imported, 1);
        assert_eq!(summary.skipped, 0, "no soft errors: {:?}", summary.errors);

        // Rows actually landed, including the self-FK parent link.
        let ws = WorkspaceId::from("ws-1");
        assert_eq!(store.list_notes(&ws).await.unwrap().len(), 2);
        let child = store.get_note(&NoteId::from("note-child")).await.unwrap();
        assert_eq!(child.parent_id, Some(NoteId::from("note-parent")));
        let msgs = store
            .get_agent_messages(&AgentId::from("agent-1"), None)
            .await
            .unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            store
                .list_comments(&NoteId::from("note-parent"))
                .await
                .unwrap()
                .len(),
            1
        );

        std::fs::remove_dir_all(&source).ok();
    }

    #[tokio::test]
    async fn second_run_is_idempotent() {
        let source = write_fixture();
        let store = open_store().await;
        run(&store, &source).await.expect("first import");

        let summary = run(&store, &source).await.expect("second import");
        // Everything already exists → all updates, nothing newly imported.
        assert_eq!(summary.workspaces_imported, 0);
        assert_eq!(summary.workspaces_updated, 1);
        assert_eq!(summary.notes_imported, 0);
        assert_eq!(summary.notes_updated, 2);
        assert_eq!(summary.agents_imported, 0);
        assert_eq!(summary.agents_updated, 1);
        // Append-only message log is not re-written on re-import.
        assert_eq!(summary.messages_imported, 0);
        assert_eq!(summary.comments_updated, 1);

        // No duplicate rows after running twice.
        let ws = WorkspaceId::from("ws-1");
        assert_eq!(store.list_notes(&ws).await.unwrap().len(), 2);
        assert_eq!(store.list_agent_sessions(&ws).await.unwrap().len(), 1);
        assert_eq!(
            store
                .get_agent_messages(&AgentId::from("agent-1"), None)
                .await
                .unwrap()
                .len(),
            2,
        );

        std::fs::remove_dir_all(&source).ok();
    }

    #[tokio::test]
    async fn source_is_never_mutated() {
        let source = write_fixture();
        let store = open_store().await;

        let before = snapshot(&source);
        run(&store, &source).await.expect("import");
        let after = snapshot(&source);
        assert_eq!(before, after, "import must be read-only toward the source");

        std::fs::remove_dir_all(&source).ok();
    }

    #[tokio::test]
    async fn missing_source_dir_is_a_hard_error() {
        let store = open_store().await;
        let missing =
            std::env::temp_dir().join(format!("intentd-missing-{}", uuid::Uuid::new_v4()));
        let err = run(&store, &missing)
            .await
            .expect_err("missing dir must error");
        assert!(err.to_string().contains("not a directory"), "{err}");
    }

    #[tokio::test]
    async fn malformed_workspaces_json_is_a_hard_error() {
        let store = open_store().await;
        let root =
            std::env::temp_dir().join(format!("intentd-bad-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("workspaces.json"), "{ this is not json ]").unwrap();
        let err = run(&store, &root).await.expect_err("garbage must error");
        assert!(err.to_string().contains("invalid JSON"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn bad_entity_file_is_soft_skipped() {
        let source = write_fixture();
        // A malformed note file must not abort the run — it is counted skipped.
        let notes = source
            .join("workspaces")
            .join("ws-1")
            .join(".workspace")
            .join("notes");
        std::fs::write(notes.join("broken.json"), "{ nope").unwrap();
        let store = open_store().await;

        let summary = run(&store, &source).await.expect("import still succeeds");
        assert_eq!(summary.workspaces_imported, 1);
        assert_eq!(summary.notes_imported, 2);
        assert_eq!(summary.skipped, 1);
        assert!(
            summary.errors[0].contains("invalid JSON"),
            "{:?}",
            summary.errors
        );

        std::fs::remove_dir_all(&source).ok();
    }
}
