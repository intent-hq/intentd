//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::ids::{NoteId, WorkspaceId};
use crate::model::{
    Note, NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput,
    NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteSetContentResult, NoteTaskRow,
    NoteUpdateInput, NoteUpdateMetadataResult, ReadAssetResult, Workspace, WorkspaceCreate,
    WorkspaceUpdate,
};

/// Boxed, `Send` future — keeps [`WorkspaceApi`] object-safe so it can be held
/// as `Arc<dyn WorkspaceApi>` (the agent→BE callback handle, §6.8).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Business-logic read surface that `intent-acp` calls back into and the
/// transport router dispatches to. Defined here in the leaf crate; the real,
/// store-backed implementation lives in `intent-services` (§3.2 rule 3). The
/// default bodies return an internal error so downstream stubs compile until
/// they override these methods.
pub trait WorkspaceApi: Send + Sync {
    /// List workspaces, optionally including archived ones (PROTOCOL §5.1).
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let _ = include_archived;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_workspaces not implemented".to_string(),
            ))
        })
    }

    /// Fetch one workspace by id; `NotFound` if absent (PROTOCOL §5.1).
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_workspace not implemented".to_string(),
            ))
        })
    }

    /// Create a workspace from wire input, filling ids/defaults (PROTOCOL §5.1).
    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        let _ = input;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_workspace not implemented".to_string(),
            ))
        })
    }

    /// Apply a partial update to a workspace (PROTOCOL §5.1).
    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let _ = (id, update);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_workspace not implemented".to_string(),
            ))
        })
    }

    /// Delete a workspace by id; `NotFound` if absent (PROTOCOL §5.1).
    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::delete_workspace not implemented".to_string(),
            ))
        })
    }

    /// Archive a workspace (status→archived) (PROTOCOL §5.1).
    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::archive_workspace not implemented".to_string(),
            ))
        })
    }

    /// Unarchive a workspace (status→active) (PROTOCOL §5.1).
    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::unarchive_workspace not implemented".to_string(),
            ))
        })
    }

    /// Clear the dismissible `attention` flag to `none` (PROTOCOL §5.1).
    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::dismiss_attention not implemented".to_string(),
            ))
        })
    }

    /// Mark the workspace seen, clearing an `unread` `attention` flag (§5.1).
    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mark_seen not implemented".to_string(),
            ))
        })
    }

    /// List notes in a workspace (PROTOCOL §5.2).
    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_notes not implemented".to_string(),
            ))
        })
    }

    /// Fetch one note by id, scoped to the workspace (PROTOCOL §5.2).
    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_note not implemented".to_string(),
            ))
        })
    }

    /// Create a note from wire input (PROTOCOL §5.2).
    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
    ) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_note not implemented".to_string(),
            ))
        })
    }

    /// CRUD `note.update`: raw content set, or title/tags metadata (PROTOCOL §5.2).
    fn update_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteUpdateInput,
    ) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, note_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_note not implemented".to_string(),
            ))
        })
    }

    /// `note.add`: append/prepend/insert content (PROTOCOL §5.2).
    fn add_to_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let _ = (workspace_id, note_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::add_to_note not implemented".to_string(),
            ))
        })
    }

    /// `note.edit`: first exact-match replacement (PROTOCOL §5.2).
    fn edit_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let _ = (workspace_id, note_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::edit_note not implemented".to_string(),
            ))
        })
    }

    /// `note.editLines`: 1-based inclusive line replace/delete/insert (PROTOCOL §5.2).
    fn edit_note_lines(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let _ = (workspace_id, note_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::edit_note_lines not implemented".to_string(),
            ))
        })
    }

    /// `note.setContent`: full replace with the reduction guard (PROTOCOL §5.2).
    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let _ = (workspace_id, note_id, content, confirm_replacement);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::set_note_content not implemented".to_string(),
            ))
        })
    }

    /// `note.updateMetadata`: title/tags (spec title is skipped) (PROTOCOL §5.2).
    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        let _ = (workspace_id, note_id, title, tags);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_note_metadata not implemented".to_string(),
            ))
        })
    }

    /// `note.delete`: remove a note (PROTOCOL §5.2).
    fn delete_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::delete_note not implemented".to_string(),
            ))
        })
    }

    /// `note.listTasks`: parse checkbox rows from a note's content (PROTOCOL §5.2).
    fn list_note_tasks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_note_tasks not implemented".to_string(),
            ))
        })
    }

    /// `note.readAsset`: read an asset (id or `workspace-asset://` URL) (PROTOCOL §5.2).
    fn read_asset(
        &self,
        workspace_id: WorkspaceId,
        asset: String,
    ) -> BoxFuture<'_, Result<ReadAssetResult>> {
        let _ = (workspace_id, asset);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::read_asset not implemented".to_string(),
            ))
        })
    }
}

/// Context-engine abstraction implemented by `intent-context` (§3.1).
pub trait ContextEngine: Send + Sync {}
