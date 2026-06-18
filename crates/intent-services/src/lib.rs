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
    now_iso, parse_iso, AgentId, AuthorType, BoxFuture, Comment, CommentAddResult, CommentAnchor,
    CommentAnchorType, CommentDeleteResult, CommentGetThreadResult, CommentListResult,
    CommentLocation, CommentRespondResult, CommentRespondThread, CommentStatus,
    CommentThreadSummary, CommentType, CommentWire, ContentType, Note, NoteAddInput, NoteAddResult,
    NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult,
    NoteEditResult, NoteId, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, TaskAssignAgentResult,
    TaskConvertBlocksResult, TaskCreatePrerequisiteResult, TaskGetMyTaskResult,
    TaskMarkAsTaskResult, TaskMetadata, TaskStatus, TaskSubtask, TaskUpdateNoteStatusResult,
    TaskUpdateResult, TaskUpdateStatusResult, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceCreate, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use intent_store::Store;

pub use intent_core::{Error, Result, WorkspaceApi};

pub mod events;
mod note_ops;

#[cfg(test)]
mod tests;

pub use events::{EventBus, Subscription, SubscriptionFilter};

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

/// The seven valid task-note statuses, in the order the TS validator lists them.
const TASK_STATUS_WORDS: [&str; 7] = [
    "not_started",
    "waiting",
    "discussion_needed",
    "in_progress",
    "review_required",
    "complete",
    "cancelled",
];

/// Parse a `TaskStatus`, validating against the canonical set with the TS
/// `updateNoteStatus` error message.
fn parse_task_status_strict(s: &str) -> Result<TaskStatus> {
    if !TASK_STATUS_WORDS.contains(&s) {
        return Err(Error::Internal(format!(
            "Invalid status: {s}. Must be one of: {}",
            TASK_STATUS_WORDS.join(", ")
        )));
    }
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| Error::Internal(format!("Invalid status: {s} ({e})")))
}

/// Parse a comment type word, defaulting to `comment` on absent/unknown values
/// (the TS handler passes the raw value straight through with that default).
fn parse_comment_type(opt: Option<&str>) -> CommentType {
    match opt {
        Some(s) => serde_json::from_value(serde_json::Value::String(s.to_string()))
            .unwrap_or(CommentType::Comment),
        None => CommentType::Comment,
    }
}

/// Validate an `agent-{uuid}` id, mirroring the TS `agentIdPattern`.
fn is_valid_agent_id(s: &str) -> bool {
    let rest = match s.strip_prefix("agent-") {
        Some(r) => r,
        None => return false,
    };
    let groups = [8usize, 4, 4, 4, 12];
    let parts: Vec<&str> = rest.split('-').collect();
    if parts.len() != groups.len() {
        return false;
    }
    parts
        .iter()
        .zip(groups.iter())
        .all(|(part, &len)| part.len() == len && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Reconstruct the TS `markId` for a comment anchor
/// (`"{startId}:start|{endId}:end"` for ranges, the `pointId` for points).
fn derive_mark_id(anchor: &CommentAnchor) -> Option<String> {
    match anchor.kind {
        CommentAnchorType::Range => match (&anchor.start_id, &anchor.end_id) {
            (Some(start), Some(end)) => Some(format!("{start}:start|{end}:end")),
            _ => None,
        },
        CommentAnchorType::Point => anchor.point_id.clone(),
    }
}

/// Apply the `updateTaskStatus` timestamp transitions in place.
fn apply_status_transition(task: &mut TaskMetadata, status: TaskStatus, now: &str) {
    task.status = status;
    if status == TaskStatus::InProgress && task.started_at.is_none() {
        task.started_at = Some(now.to_string());
    }
    if status == TaskStatus::Complete && task.completed_at.is_none() {
        task.completed_at = Some(now.to_string());
    }
    if status != TaskStatus::Complete && task.completed_at.is_some() {
        task.completed_at = None;
    }
}

/// Build a fresh task-note metadata with the markAsTask enrichment timestamps.
fn fresh_task_metadata(status: TaskStatus, now: &str, peer_order: Option<i64>) -> TaskMetadata {
    let mut task = TaskMetadata {
        status,
        peer_order,
        ..Default::default()
    };
    apply_status_transition(&mut task, status, now);
    task
}

impl Services {
    /// Create a child task note nested under `parent_id`, marking it a task with
    /// `status` (and optional `peer_order`). Shared by `createPrerequisite` and
    /// `convertBlocks`.
    async fn create_child_task_note(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &NoteId,
        title_raw: &str,
        content: String,
        status: TaskStatus,
        peer_order: Option<i64>,
    ) -> Result<Note> {
        let now = now_iso();
        let note = Note {
            id: NoteId::new(),
            workspace_id: workspace_id.clone(),
            title: note_ops::strip_markdown_formatting(title_raw),
            content,
            content_type: ContentType::Markdown,
            tags: Vec::new(),
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: Some(parent_id.clone()),
            visibility: NoteVisibility::Workspace,
            task: Some(fresh_task_metadata(status, &now, peer_order)),
            created_at: now.clone(),
            updated_at: now,
        };
        self.store.insert_note(&note).await?;
        Ok(note)
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

    fn task_update_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_text: String,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if task_text.is_empty() {
                return Err(Error::Internal(
                    "Task text is required to identify the task".to_string(),
                ));
            }
            let checkbox = note_ops::checkbox_for(&status).ok_or_else(|| {
                Error::Internal("Status must be 'done', 'todo', or 'in-progress'".to_string())
            })?;
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let normalized = task_text.trim().to_string();
            let updated = note_ops::apply_task_status(&note.content, &normalized, checkbox)?;
            note.content = updated;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(TaskUpdateStatusResult {
                ok: true,
                note_id: note.id,
                task_text: normalized,
                status,
            })
        })
    }

    fn task_update_note_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let new_status = parse_task_status_strict(&status)?;
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            let mut task = match note.task.clone() {
                Some(t) => t,
                None => {
                    return Err(Error::Internal(
                        "Note is not a task. Use markAsTask() first.".to_string(),
                    ))
                }
            };
            apply_status_transition(&mut task, new_status, &now_iso());
            note.task = Some(task);
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(TaskUpdateNoteStatusResult {
                ok: true,
                note_id: note.id.clone(),
                status: new_status,
                note,
            })
        })
    }

    fn task_update(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        line: i64,
        text: Option<String>,
        status: Option<String>,
        expected: Option<String>,
    ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if text.is_none() && status.is_none() {
                return Err(Error::Internal(
                    "Either text or status (or both) must be provided".to_string(),
                ));
            }
            if line < 1 {
                return Err(Error::Internal(
                    "Line number must be a positive integer".to_string(),
                ));
            }
            if let Some(s) = status.as_deref() {
                if note_ops::checkbox_for(s).is_none() {
                    return Err(Error::Internal(
                        "Status must be 'todo', 'in-progress', or 'done'".to_string(),
                    ));
                }
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let update = note_ops::apply_task_line_update(
                &note.content,
                line,
                text.as_deref(),
                status.as_deref(),
                expected.as_deref(),
            )?;
            note.content = update.content;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(TaskUpdateResult {
                ok: true,
                note_id: note.id,
                line_number: line,
                previous_text: update.previous_text,
                new_text: update.new_text,
                status: update.status_word,
            })
        })
    }

    fn get_my_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = match store.get_note(&task_note_id).await {
                Ok(n) if n.workspace_id == workspace_id => n,
                Ok(_) | Err(Error::NotFound(_)) => {
                    return Err(Error::Internal("Task note not found".to_string()))
                }
                Err(e) => return Err(e),
            };
            let task = match note.task.clone() {
                Some(t) => t,
                None => return Err(Error::Internal("Note is not a task".to_string())),
            };
            let all = store.list_notes(&workspace_id).await?;
            let subtasks = all
                .iter()
                .filter(|n| n.parent_id.as_ref() == Some(&note.id) && n.task.is_some())
                .map(|n| TaskSubtask {
                    id: n.id.clone(),
                    title: n.title.clone(),
                    status: n
                        .task
                        .as_ref()
                        .map(|t| status_word(t.status))
                        .unwrap_or("unknown")
                        .to_string(),
                })
                .collect();
            Ok(TaskGetMyTaskResult {
                note_id: note.id.clone(),
                title: note.title.clone(),
                content: note.content.clone(),
                status: task.status,
                parent_id: note.parent_id.clone(),
                assigned_agents: task.assigned_agent_ids.clone(),
                subtasks,
                task_metadata: task,
            })
        })
    }

    fn mark_as_task(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        acceptance_criteria: Vec<String>,
        effort: Option<String>,
    ) -> BoxFuture<'_, Result<TaskMarkAsTaskResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let new_status =
                serde_json::from_value::<TaskStatus>(serde_json::Value::String(status.clone()))
                    .map_err(|_| Error::Internal(format!("Invalid status: {status}")))?;
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let now = now_iso();
            match note.task.clone() {
                // Already a task with a changing status → preserve other fields.
                Some(mut existing) if existing.status != new_status => {
                    apply_status_transition(&mut existing, new_status, &now);
                    note.task = Some(existing);
                }
                // Fresh task (or same status) → set the markAsTask metadata.
                _ => {
                    let mut task = TaskMetadata {
                        status: new_status,
                        acceptance_criteria,
                        estimated_effort: effort,
                        ..Default::default()
                    };
                    apply_status_transition(&mut task, new_status, &now);
                    note.task = Some(task);
                }
            }
            note.updated_at = now;
            store.update_note(&note).await?;
            Ok(TaskMarkAsTaskResult {
                ok: true,
                note_id: note.id,
                status: new_status,
            })
        })
    }

    fn convert_task_blocks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
        let services = self.clone();
        Box::pin(async move {
            let store = &services.store;
            let mut note = fetch_note_peer(store, &workspace_id, &note_id).await?;
            if note.content.is_empty() {
                return Ok(TaskConvertBlocksResult {
                    ok: true,
                    converted_count: 0,
                    created_note_ids: Vec::new(),
                });
            }
            // Mirror the TS guard: only parse when a `@@@task` fence exists.
            let parsed = if note_ops::has_task_blocks(&note.content) {
                note_ops::extract_task_blocks(&note.content)
            } else {
                note_ops::TaskBlocksResult {
                    tasks: Vec::new(),
                    content_without_blocks: note.content.clone(),
                }
            };
            // Idempotency: map existing child note titles (normalized) → id.
            let all = store.list_notes(&workspace_id).await?;
            let mut existing_by_title: std::collections::HashMap<String, NoteId> = all
                .iter()
                .filter(|n| n.parent_id.as_ref() == Some(&note.id))
                .map(|n| (n.title.trim().to_lowercase(), n.id.clone()))
                .collect();

            // Start from the placeholder-substituted content; each valid block
            // is `<!-- task-block-placeholder-{i} -->` to be replaced below.
            let mut working = parsed.content_without_blocks.clone();
            let mut created_note_ids: Vec<String> = Vec::new();
            let mut peer_order = 100i64;
            for (i, task) in parsed.tasks.iter().enumerate() {
                let body = if task.content.is_empty() {
                    format!("# {}\n\nCreated as a prerequisite task.", task.title)
                } else {
                    format!("# {}\n\n{}", task.title, task.content)
                };
                let normalized = task.title.trim().to_lowercase();
                let task_note_id = match existing_by_title.get(&normalized) {
                    Some(existing_id) => existing_id.clone(),
                    None => {
                        let child = services
                            .create_child_task_note(
                                &workspace_id,
                                &note.id,
                                &task.title,
                                body,
                                TaskStatus::NotStarted,
                                Some(peer_order),
                            )
                            .await?;
                        existing_by_title.insert(normalized, child.id.clone());
                        created_note_ids.push(child.id.0.clone());
                        child.id
                    }
                };
                let placeholder = format!("<!-- task-block-placeholder-{i} -->");
                let linked = format!(
                    "- [ ] [{}](intent://local/task/{})",
                    task.title, task_note_id.0
                );
                working = working.replace(&placeholder, &linked);
                peer_order += 100;
            }

            let content_changed = working != note.content;
            if !content_changed && created_note_ids.is_empty() {
                return Ok(TaskConvertBlocksResult {
                    ok: true,
                    converted_count: 0,
                    created_note_ids: Vec::new(),
                });
            }
            note.content = working;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            Ok(TaskConvertBlocksResult {
                ok: true,
                converted_count: created_note_ids.len() as i64,
                created_note_ids,
            })
        })
    }

    fn create_prerequisite(
        &self,
        workspace_id: WorkspaceId,
        dependent_note_id: NoteId,
        title: String,
        content: Option<String>,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskCreatePrerequisiteResult>> {
        let services = self.clone();
        Box::pin(async move {
            let store = &services.store;
            // Verify the dependent note exists in this workspace.
            match store.get_note(&dependent_note_id).await {
                Ok(n) if n.workspace_id == workspace_id => {}
                Ok(_) | Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!(
                        "Dependent note not found: {dependent_note_id}"
                    )))
                }
                Err(e) => return Err(e),
            }
            let status_word = status.as_deref().unwrap_or("not_started");
            let task_status = parse_task_status_strict(status_word)?;
            let child = services
                .create_child_task_note(
                    &workspace_id,
                    &dependent_note_id,
                    &title,
                    content.unwrap_or_default(),
                    task_status,
                    None,
                )
                .await?;
            Ok(TaskCreatePrerequisiteResult {
                ok: true,
                prerequisite_note_id: child.id,
                dependent_note_id,
                title: child.title,
            })
        })
    }

    fn assign_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
    ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if !is_valid_agent_id(&agent_id) {
                return Err(Error::Internal(format!(
                    "Invalid agentId format: \"{agent_id}\". Agent IDs must be in format \"agent-{{uuid}}\" (e.g., \"agent-b0a8044a-5eac-4b52-8456-15d3b784decb\"). To create a new agent and assign it to this task, use create_agent with taskNoteId=\"{note_id}\" instead."
                )));
            }
            let mut note = match store.get_note(&note_id).await {
                Ok(n) if n.workspace_id == workspace_id => n,
                Ok(_) | Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!("Note not found: {note_id}")))
                }
                Err(e) => return Err(e),
            };
            let mut task = match note.task.clone() {
                Some(t) => t,
                None => return Err(Error::Internal(format!("Note {note_id} is not a task"))),
            };
            let agent = AgentId::from(agent_id.as_str());
            let already_assigned = task.assigned_agent_ids.contains(&agent);
            let should_update_status = task.status == TaskStatus::NotStarted;
            if already_assigned && !should_update_status {
                return Ok(TaskAssignAgentResult {
                    ok: true,
                    note_id,
                    agent_id: agent,
                });
            }
            let now = now_iso();
            if !already_assigned {
                task.assigned_agent_ids.push(agent.clone());
            }
            if should_update_status {
                apply_status_transition(&mut task, TaskStatus::InProgress, &now);
            }
            note.task = Some(task);
            note.updated_at = now;
            store.update_note(&note).await?;
            Ok(TaskAssignAgentResult {
                ok: true,
                note_id,
                agent_id: agent,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_add(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        search_context: String,
        comment_target: String,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if comment.trim().is_empty() {
                return Err(Error::Internal(
                    "Comment text is required and must be non-empty".to_string(),
                ));
            }
            if search_context.trim().is_empty() {
                return Err(Error::Internal(
                    "searchContext is required and must be non-empty".to_string(),
                ));
            }
            if comment_target.trim().is_empty() {
                return Err(Error::Internal(
                    "commentTarget is required and must be non-empty".to_string(),
                ));
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let (from, to, line) =
                note_ops::find_and_anchor_text(&note.content, &search_context, &comment_target)?;
            let anchored_text = note.content[from..to].to_string();
            let comment_id = uuid::Uuid::new_v4().to_string();
            note.content = format!(
                "{}<!--anchor:{id}:start-->{anchored}<!--anchor:{id}:end-->{}",
                &note.content[..from],
                &note.content[to..],
                id = comment_id,
                anchored = anchored_text,
            );
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            let now = now_iso();
            let new_comment = Comment {
                id: comment_id.clone(),
                thread_id: comment_id.clone(),
                note_id: Some(note_id),
                kind: parse_comment_type(kind.as_deref()),
                content: comment,
                author: author.unwrap_or_else(|| "Agent".to_string()),
                author_type: AuthorType::Agent,
                status: CommentStatus::Open,
                parent_id: None,
                anchor: CommentAnchor {
                    kind: CommentAnchorType::Range,
                    start_id: Some(comment_id.clone()),
                    end_id: Some(comment_id.clone()),
                    point_id: None,
                },
                anchor_text: Some(anchored_text.clone()),
                anchor_before: None,
                anchor_after: None,
                suggestion_original: None,
                suggestion_proposed: None,
                agent_id: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_comment(&new_comment).await?;
            Ok(CommentAddResult {
                success: true,
                message: format!("Comment successfully anchored to \"{anchored_text}\""),
                comment_id,
                anchored: true,
                location: CommentLocation {
                    line,
                    anchored_text,
                },
            })
        })
    }

    fn comment_list(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        since: Option<String>,
        author_type: Option<String>,
        status: Option<String>,
        include_comments: bool,
    ) -> BoxFuture<'_, Result<CommentListResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let since_dt = match &since {
                Some(s) => match parse_iso(s) {
                    Some(dt) => Some(dt),
                    None => {
                        return Err(Error::Internal(format!(
                            "Invalid 'since' timestamp: {s}. Must be ISO 8601 format."
                        )))
                    }
                },
                None => None,
            };
            if let Some(at) = author_type.as_deref() {
                if at != "user" && at != "agent" {
                    return Err(Error::Internal(format!(
                        "Invalid 'authorType': {at}. Must be 'user' or 'agent'."
                    )));
                }
            }
            if let Some(st) = status.as_deref() {
                if !["open", "resolved", "pending"].contains(&st) {
                    return Err(Error::Internal(format!(
                        "Invalid 'status': {st}. Must be 'open', 'resolved', or 'pending'."
                    )));
                }
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let comments = store.list_comments(&note_id).await?;

            // Group by thread id (roots carry thread_id == id).
            let mut order: Vec<String> = Vec::new();
            let mut groups: std::collections::HashMap<String, Vec<Comment>> =
                std::collections::HashMap::new();
            for c in comments {
                let tid = c.thread_id.clone();
                if !groups.contains_key(&tid) {
                    order.push(tid.clone());
                }
                groups.entry(tid).or_default().push(c);
            }

            let mut threads: Vec<CommentThreadSummary> = Vec::new();
            for tid in order {
                let mut group = groups.remove(&tid).unwrap_or_default();
                group.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                let root = group
                    .iter()
                    .find(|c| c.parent_id.is_none())
                    .cloned()
                    .unwrap_or_else(|| group[0].clone());
                let latest = group
                    .iter()
                    .max_by(|a, b| a.updated_at.cmp(&b.updated_at))
                    .cloned()
                    .unwrap_or_else(|| group[0].clone());
                let thread_status = if group.iter().any(|c| c.status == CommentStatus::Open) {
                    "open"
                } else if group.iter().all(|c| c.status == CommentStatus::Resolved) {
                    "resolved"
                } else {
                    "pending"
                };
                let last_activity = group
                    .iter()
                    .map(|c| c.updated_at.clone())
                    .max()
                    .unwrap_or_else(|| root.updated_at.clone());
                let comment_wires = if include_comments {
                    Some(group.iter().map(CommentWire::from_comment).collect())
                } else {
                    None
                };
                threads.push(CommentThreadSummary {
                    thread_id: tid,
                    note_id: note_id.clone(),
                    targeted_text: root.anchor_text.clone(),
                    anchor_id: derive_mark_id(&root.anchor),
                    status: thread_status.to_string(),
                    created_at: root.created_at.clone(),
                    last_activity,
                    latest_comment_author: latest.author.clone(),
                    latest_comment_author_type: latest.author_type,
                    latest_comment_at: latest.updated_at.clone(),
                    comment_count: group.len(),
                    comments: comment_wires,
                });
            }

            if let Some(dt) = since_dt {
                threads.retain(|t| match parse_iso(&t.last_activity) {
                    Some(la) => la > dt,
                    None => false,
                });
            }
            if let Some(at) = author_type.as_deref() {
                threads.retain(|t| status_word_author(t.latest_comment_author_type) == at);
            }
            if let Some(st) = status.as_deref() {
                threads.retain(|t| t.status == st);
            }
            threads.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

            let total_comments = threads.iter().map(|t| t.comment_count).sum();
            Ok(CommentListResult {
                total_threads: threads.len(),
                total_comments,
                threads,
            })
        })
    }

    fn comment_get_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentGetThreadResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if thread_id.is_none() && comment_id.is_none() {
                return Err(Error::Internal(
                    "Either threadId or commentId must be provided".to_string(),
                ));
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let all = store.list_comments(&note_id).await?;
            let target = match thread_id {
                Some(t) => t,
                None => {
                    let cid = comment_id.unwrap();
                    match all.iter().find(|c| c.id == cid) {
                        Some(c) => c.thread_id.clone(),
                        None => return Err(Error::Internal(format!("Comment not found: {cid}"))),
                    }
                }
            };
            let mut group: Vec<Comment> =
                all.into_iter().filter(|c| c.thread_id == target).collect();
            if group.is_empty() {
                return Err(Error::Internal(format!("Thread not found: {target}")));
            }
            group.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            let root = group
                .iter()
                .find(|c| c.parent_id.is_none())
                .cloned()
                .unwrap_or_else(|| group[0].clone());
            let replies: Vec<CommentWire> = group
                .iter()
                .filter(|c| c.parent_id.is_some())
                .map(CommentWire::from_comment)
                .collect();
            let resolved = group.iter().all(|c| {
                matches!(
                    c.status,
                    CommentStatus::Resolved | CommentStatus::Accepted | CommentStatus::Rejected
                )
            });
            Ok(CommentGetThreadResult {
                thread_id: target,
                note_id,
                root_comment: CommentWire::from_comment(&root),
                replies,
                total_comments: group.len(),
                status: if resolved { "resolved" } else { "open" }.to_string(),
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_respond(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
        suggestion_original: Option<String>,
        suggestion_proposed: Option<String>,
    ) -> BoxFuture<'_, Result<CommentRespondResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if thread_id.is_none() && comment_id.is_none() {
                return Err(Error::Internal(
                    "Either threadId or commentId must be provided".to_string(),
                ));
            }
            if comment.trim().is_empty() {
                return Err(Error::Internal(
                    "Comment text is required and must be non-empty".to_string(),
                ));
            }
            let kind_parsed = parse_comment_type(kind.as_deref());
            if kind_parsed == CommentType::Suggestion
                && (suggestion_original.is_none() || suggestion_proposed.is_none())
            {
                return Err(Error::Internal(
                    "For type='suggestion', both suggestionOriginal and suggestionProposed are required"
                        .to_string(),
                ));
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let all = store.list_comments(&note_id).await?;
            let (target, parent_from_id) = match thread_id {
                Some(t) => (t, None),
                None => {
                    let cid = comment_id.unwrap();
                    match all.iter().find(|c| c.id == cid).cloned() {
                        Some(c) => (c.thread_id.clone(), Some(c)),
                        None => return Err(Error::Internal(format!("Comment not found: {cid}"))),
                    }
                }
            };
            let mut group: Vec<Comment> = all
                .iter()
                .filter(|c| c.thread_id == target)
                .cloned()
                .collect();
            if group.is_empty() {
                return Err(Error::Internal(format!("Thread not found: {target}")));
            }
            // Newest first; the parent defaults to the most recent comment.
            group.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            let parent = parent_from_id.unwrap_or_else(|| group[0].clone());
            let now = now_iso();
            let reply = Comment {
                id: uuid::Uuid::new_v4().to_string(),
                thread_id: target.clone(),
                note_id: Some(note_id),
                kind: kind_parsed,
                content: comment,
                author: author.unwrap_or_else(|| "Agent".to_string()),
                author_type: AuthorType::Agent,
                status: CommentStatus::Open,
                parent_id: Some(parent.id.clone()),
                anchor: parent.anchor.clone(),
                anchor_text: parent.anchor_text.clone(),
                anchor_before: None,
                anchor_after: None,
                suggestion_original: if kind_parsed == CommentType::Suggestion {
                    suggestion_original
                } else {
                    None
                },
                suggestion_proposed: if kind_parsed == CommentType::Suggestion {
                    suggestion_proposed
                } else {
                    None
                },
                agent_id: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_comment(&reply).await?;
            Ok(CommentRespondResult {
                success: true,
                message: "Reply added successfully".to_string(),
                comment: CommentWire::from_comment(&reply),
                thread: CommentRespondThread {
                    thread_id: target,
                    total_comments: group.len() + 1,
                },
            })
        })
    }

    fn comment_delete(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        comment_id: String,
    ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            match store.delete_comment(&comment_id).await {
                Ok(()) => Ok(CommentDeleteResult {
                    success: true,
                    message: format!("Comment {comment_id} deleted from note {note_id}"),
                }),
                Err(Error::NotFound(_)) => {
                    Err(Error::Internal("Failed to delete comment".to_string()))
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Snake_case status word for a `TaskStatus` (matches the wire serialization).
fn status_word(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::NotStarted => "not_started",
        TaskStatus::Waiting => "waiting",
        TaskStatus::DiscussionNeeded => "discussion_needed",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::ReviewRequired => "review_required",
        TaskStatus::Complete => "complete",
        TaskStatus::Cancelled => "cancelled",
    }
}

/// Wire word for an `AuthorType` (`user` / `agent`).
fn status_word_author(author_type: AuthorType) -> &'static str {
    match author_type {
        AuthorType::User => "user",
        AuthorType::Agent => "agent",
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
