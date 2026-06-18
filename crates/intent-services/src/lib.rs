//! intent-services — the shared business-logic surface (§3.1).
//!
//! Depends on core, store, git, sourcecontrol, acp, context, providers, pty,
//! and search (§3.2). Sibling feature modules never import each other; they
//! communicate through the store and the event bus (§3.2 rule 4). This slice
//! implements the read-only `WorkspaceApi` surface (`workspace.list` /
//! `note.list`) over `intent-store`.

use std::path::PathBuf;

use base64::Engine as _;
use intent_core::{
    now_iso, BoxFuture, ContentType, Note, NoteAddInput, NoteAddResult, NoteCreate,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteId, NoteSetContentResult, NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult,
    NoteVisibility, ReadAssetResult, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceCreate, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use intent_store::Store;

pub use intent_core::{Error, Result, WorkspaceApi};

mod note_ops;

#[cfg(test)]
mod tests;

/// Aggregate service handle wired by the binary composition root. It implements
/// `WorkspaceApi` so it can be handed to `intent-acp` as `Arc<dyn WorkspaceApi>`
/// (§6.8) and dispatched to by the transport router.
#[derive(Clone)]
pub struct Services {
    store: Store,
    /// Root directory for note assets, laid out as `<root>/<workspaceId>/<assetId>`.
    /// `None` until configured by the composition root; `note.readAsset` errors
    /// when unset.
    assets_root: Option<PathBuf>,
}

impl Services {
    /// Wire the services surface over a persistence handle.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            assets_root: None,
        }
    }

    /// Configure the note-asset root directory (for `note.readAsset`).
    pub fn with_assets_root(mut self, root: PathBuf) -> Self {
        self.assets_root = Some(root);
        self
    }

    /// Borrow the underlying store (composition-root / diagnostics use).
    pub fn store(&self) -> &Store {
        &self.store
    }
}

/// Fetch a note scoped to `workspace_id`; `NotFound` if absent or in another
/// workspace. Used by the CRUD `note.get`/`note.update` paths.
async fn fetch_note(store: &Store, workspace_id: &WorkspaceId, note_id: &NoteId) -> Result<Note> {
    match store.get_note(note_id).await {
        Ok(n) if &n.workspace_id == workspace_id => Ok(n),
        Ok(_) | Err(Error::NotFound(_)) => Err(Error::NotFound(format!("note {note_id}"))),
        Err(e) => Err(e),
    }
}

/// Like [`fetch_note`] but surfaces the TS peer's `Note not found: <id>` message
/// as [`Error::Internal`] (→ `-32603`), matching the `ws.note.*` edit peers.
async fn fetch_note_peer(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
) -> Result<Note> {
    match store.get_note(note_id).await {
        Ok(n) if &n.workspace_id == workspace_id => Ok(n),
        Ok(_) | Err(Error::NotFound(_)) => {
            Err(Error::Internal(format!("Note not found: {note_id}")))
        }
        Err(e) => Err(e),
    }
}

impl WorkspaceApi for Services {
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let store = self.store.clone();
        Box::pin(async move { store.list_workspaces(include_archived).await })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move { store.get_workspace(&id).await })
    }

    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let now = now_iso();
            let id = WorkspaceId::new();
            // The branch defaults to the workspace id, mirroring the TS service.
            let branch = input.branch.unwrap_or_else(|| id.0.clone());
            let ws = Workspace {
                id,
                title: input.title.unwrap_or_default(),
                branch,
                base_ref: input.base_ref,
                base_commit_sha: input.base_commit_sha,
                status: WorkspaceStatus::Active,
                status_message: input.status_message,
                // Derived, read-only; never persisted (§9.9).
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now,
                last_activity: None,
                tags: input.tags.unwrap_or_default(),
                path: input.path,
                repository_owner: input.repository_owner,
                repository_name: input.repository_name,
                worktree_path: input.worktree_path,
                scope: input.scope,
                skip_worktree: input.skip_worktree.unwrap_or(false),
                setup_script: input.setup_script,
                is_remote: input.is_remote.unwrap_or(false),
                default_model: input.default_model,
                pr_number: None,
                pr_url: None,
                archived: false,
                archived_at: None,
            };
            store.insert_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            if let Some(v) = update.title {
                ws.title = v;
            }
            if let Some(v) = update.status_message {
                ws.status_message = Some(v);
            }
            if let Some(v) = update.branch {
                ws.branch = v;
            }
            if let Some(v) = update.base_ref {
                ws.base_ref = Some(v);
            }
            if let Some(v) = update.base_commit_sha {
                ws.base_commit_sha = Some(v);
            }
            if let Some(v) = update.status {
                ws.status = v;
            }
            if let Some(v) = update.tags {
                ws.tags = v;
            }
            if let Some(v) = update.path {
                ws.path = Some(v);
            }
            if let Some(v) = update.repository_owner {
                ws.repository_owner = Some(v);
            }
            if let Some(v) = update.repository_name {
                ws.repository_name = Some(v);
            }
            if let Some(v) = update.worktree_path {
                ws.worktree_path = Some(v);
            }
            if let Some(v) = update.scope {
                ws.scope = Some(v);
            }
            if let Some(v) = update.skip_worktree {
                ws.skip_worktree = v;
            }
            if let Some(v) = update.setup_script {
                ws.setup_script = Some(v);
            }
            if let Some(v) = update.is_remote {
                ws.is_remote = v;
            }
            if let Some(v) = update.default_model {
                ws.default_model = Some(v);
            }
            if let Some(v) = update.pr_number {
                ws.pr_number = Some(v);
            }
            if let Some(v) = update.pr_url {
                ws.pr_url = Some(v);
            }
            if let Some(v) = update.last_activity {
                ws.last_activity = Some(v);
            }
            if let Some(v) = update.attention {
                ws.attention = v;
            }
            if let Some(v) = update.archived {
                ws.archived = v;
            }
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        Box::pin(async move { store.delete_workspace(&id).await })
    }

    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            let now = now_iso();
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            ws.archived_at = Some(now.clone());
            ws.updated_at = now;
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            ws.status = WorkspaceStatus::Active;
            ws.archived = false;
            ws.archived_at = None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            ws.attention = WorkspaceAttention::None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            // "Seen" clears the unread flag; review-required attention persists.
            if ws.attention == WorkspaceAttention::Unread {
                ws.attention = WorkspaceAttention::None;
                ws.updated_at = now_iso();
                store.update_workspace(&ws).await?;
            }
            Ok(ws)
        })
    }

    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let store = self.store.clone();
        let id = workspace_id.clone();
        Box::pin(async move { store.list_notes(&id).await })
    }

    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        Box::pin(async move { fetch_note(&store, &workspace_id, &note_id).await })
    }

    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
    ) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        Box::pin(async move {
            let now = now_iso();
            let note = Note {
                id: NoteId::new(),
                workspace_id,
                title: input.title,
                content: input.content.unwrap_or_default(),
                content_type: ContentType::Markdown,
                tags: input.tags.unwrap_or_default(),
                is_pinned: false,
                is_archived: false,
                is_default: false,
                parent_id: input.parent_id.map(NoteId::from),
                visibility: NoteVisibility::Workspace,
                task: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_note(&note).await?;
            Ok(note)
        })
    }

    fn update_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteUpdateInput,
    ) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            // content present → raw full set; otherwise title/tags metadata.
            if let Some(content) = input.content {
                note.content = content;
            } else {
                if let Some(title) = input.title {
                    note.title = title;
                }
                if let Some(tags) = input.tags {
                    note.tags = tags;
                }
            }
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(note)
        })
    }

    fn add_to_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let (new_content, position) = note_ops::apply_add(
                &old_content,
                &input.content,
                input.heading.as_deref(),
                input.position.as_deref(),
            )?;
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(NoteAddResult {
                ok: true,
                note_id: note.id,
                added_length: input.content.chars().count(),
                total_length: new_content.chars().count(),
                position,
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn edit_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if input.old.is_empty() {
                return Err(Error::Internal(
                    "old is required and cannot be empty".to_string(),
                ));
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let (new_content, match_position, was_empty) =
                note_ops::apply_edit(&old_content, &input.old, &input.new)?;
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(NoteEditResult {
                ok: true,
                note_id: note.id,
                old_text_length: if was_empty {
                    0
                } else {
                    input.old.chars().count()
                },
                new_text_length: input.new.chars().count(),
                match_position,
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn edit_note_lines(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let new_content =
                note_ops::apply_edit_lines(&old_content, input.start, input.end, &input.content)?;
            let total_lines_before = old_content.split('\n').count();
            let total_lines_after = new_content.split('\n').count();
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(NoteEditLinesResult {
                ok: true,
                note_id: note.id,
                start_line: input.start,
                end_line: input.end,
                total_lines_before,
                total_lines_after,
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let previous_title = note.title.clone();
            if !old_content.is_empty() {
                let old_len = old_content.chars().count() as f64;
                let new_len = content.chars().count() as f64;
                let reduction = (old_len - new_len) / old_len * 100.0;
                if reduction > 50.0 && !confirm_replacement {
                    return Err(Error::Internal(format!(
                        "⚠️ CONTENT REDUCTION DETECTED: Your new content ({} chars) is {}% shorter than the existing content ({} chars).\n\nThis will REPLACE the entire note. If you intended to:\n- ADD content: Use note.add instead\n- EDIT a section: Use note.edit instead\n- PROCEED with replacement: Call note.setContent again with confirmReplacement=true",
                        content.chars().count(),
                        reduction.round() as i64,
                        old_content.chars().count()
                    )));
                }
            }
            let clean = note_ops::clean_set_content(&content)?;
            note.content = clean.clone();
            let now = now_iso();
            note.updated_at = now.clone();
            store.update_note(&note).await?;
            Ok(NoteSetContentResult {
                ok: true,
                title: note.title,
                note_id: note.id,
                previous_title: Some(previous_title),
                updated_at: now,
                old_content: Some(old_content),
                new_content: clean,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if title.is_none() && tags.is_none() {
                return Err(Error::Internal(
                    "At least one of title or tags must be provided".to_string(),
                ));
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let is_spec = note_id.as_str() == "spec";
            let mut changed = false;
            // Spec title cannot be modified (matches the TS peer).
            if let Some(t) = title {
                if !is_spec {
                    note.title = t;
                    changed = true;
                }
            }
            if let Some(t) = tags {
                note.tags = t;
                changed = true;
            }
            if !changed {
                return Ok(NoteUpdateMetadataResult {
                    ok: true,
                    note_id,
                    title: None,
                    tags: None,
                    updated_at: None,
                    skipped: Some(true),
                    reason: Some("spec title cannot be modified".to_string()),
                });
            }
            let now = now_iso();
            note.updated_at = now.clone();
            store.update_note(&note).await?;
            Ok(NoteUpdateMetadataResult {
                ok: true,
                note_id: note.id,
                title: Some(note.title),
                tags: Some(note.tags),
                updated_at: Some(now),
                skipped: None,
                reason: None,
            })
        })
    }

    fn delete_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Scope-check first so a foreign/absent note yields the peer message.
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            store.delete_note(&note_id).await?;
            Ok(NoteDeleteResult {
                ok: true,
                note_id,
                deleted: true,
            })
        })
    }

    fn list_note_tasks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            Ok(note_ops::parse_tasks(&note.content))
        })
    }

    fn read_asset(
        &self,
        workspace_id: WorkspaceId,
        asset: String,
    ) -> BoxFuture<'_, Result<ReadAssetResult>> {
        let assets_root = self.assets_root.clone();
        Box::pin(async move {
            let asset_id = note_ops::parse_asset_id(&asset)?;
            let root = assets_root
                .ok_or_else(|| Error::Internal("asset storage is not configured".to_string()))?;
            let path = root.join(&workspace_id.0).join(&asset_id);
            let bytes = std::fs::read(&path)
                .map_err(|e| Error::Internal(format!("Failed to read asset: {e}")))?;
            let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let size_kb = ((data.len() as f64) / 1024.0).round() as i64;
            let mime_type = note_ops::mime_from_extension(&asset_id);
            Ok(ReadAssetResult {
                asset_id,
                mime_type,
                data,
                size_kb,
            })
        })
    }
}

// Core domain service modules (§3.1).
pub mod notes {}
pub mod tasks {}
pub mod comments {}
pub mod workspace {}
pub mod agent {}
pub mod git {}
pub mod pr {}
pub mod script {}
pub mod file {}
pub mod event {}
pub mod drafts {} // §9.10

// Agent-Ecosystem modules (§18).
pub mod rules {}
pub mod specialists {}
pub mod mcp_servers {}
pub mod memories {}

// Code Changes Review modules (§17).
pub mod file_tracking {}
pub mod diffs {}
pub mod accept_changes {}
pub mod metrics {}

// Integrations & Ops modules (§19).
pub mod token_usage {}
pub mod session_stats {}
pub mod setup_scripts {}
