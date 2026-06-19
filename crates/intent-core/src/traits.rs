//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::ids::{NoteId, WorkspaceId};
use crate::model::{
    AgentDelegateInput, CommentAddResult, CommentDeleteResult, CommentGetThreadResult,
    CommentListResult, CommentRespondResult, Event, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, Note, NoteAddInput, NoteAddResult, NoteCreate,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteSetContentResult, NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult, ReadAssetResult,
    TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskMarkAsTaskResult, TaskUpdateNoteStatusResult, TaskUpdateResult,
    TaskUpdateStatusResult, Workspace, WorkspaceCreate, WorkspaceEventSummary, WorkspaceUpdate,
};

/// Boxed, `Send` future — keeps [`WorkspaceApi`] object-safe so it can be held
/// as `Arc<dyn WorkspaceApi>` (the agent→BE callback handle, §6.8).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The single shared service surface (§6.8, "one impl, two front doors"): the
/// transport JSON-RPC router and the agent→BE MCP callback both dispatch to the
/// same `WorkspaceApi`, so an agent calling `note.*`/`task.*`/`comment.*`/
/// `agent.delegate`/event queries reuses the FE's service logic without a
/// dependency cycle. Defined here in the leaf crate so `intent-acp` can hold an
/// `Arc<dyn WorkspaceApi>`; the real, store-backed implementation lives in
/// `intent-services` (§3.2 rule 3). The default bodies return an internal error
/// so downstream stubs compile until they override these methods.
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

    /// `task.updateStatus`: flip a checkbox by exact task text (PROTOCOL §5.4).
    fn task_update_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_text: String,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
        let _ = (workspace_id, note_id, task_text, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update_status not implemented".to_string(),
            ))
        })
    }

    /// `task.updateNoteStatus`: set task-note metadata status (PROTOCOL §5.4).
    fn task_update_note_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let _ = (workspace_id, note_id, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update_note_status not implemented".to_string(),
            ))
        })
    }

    /// `task.update`: atomic single-line edit with `expected` conflict check (§5.4).
    fn task_update(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        line: i64,
        text: Option<String>,
        status: Option<String>,
        expected: Option<String>,
    ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
        let _ = (workspace_id, note_id, line, text, status, expected);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_update not implemented".to_string(),
            ))
        })
    }

    /// `task.getMyTask`: read a task note with subtasks + assignees (PROTOCOL §5.4).
    fn get_my_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
        let _ = (workspace_id, task_note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_my_task not implemented".to_string(),
            ))
        })
    }

    /// `task.markAsTask`: attach/replace task metadata on a note (PROTOCOL §5.4).
    fn mark_as_task(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        acceptance_criteria: Vec<String>,
        effort: Option<String>,
    ) -> BoxFuture<'_, Result<TaskMarkAsTaskResult>> {
        let _ = (workspace_id, note_id, status, acceptance_criteria, effort);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mark_as_task not implemented".to_string(),
            ))
        })
    }

    /// `task.convertBlocks`: `@@@task` blocks → linked child task notes (§5.4).
    fn convert_task_blocks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::convert_task_blocks not implemented".to_string(),
            ))
        })
    }

    /// `task.createPrerequisite`: create a child task note (PROTOCOL §5.4).
    fn create_prerequisite(
        &self,
        workspace_id: WorkspaceId,
        dependent_note_id: NoteId,
        title: String,
        content: Option<String>,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskCreatePrerequisiteResult>> {
        let _ = (workspace_id, dependent_note_id, title, content, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::create_prerequisite not implemented".to_string(),
            ))
        })
    }

    /// `task.assignAgent`: append an agent to a task's assignee list (§5.4).
    fn assign_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
    ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
        let _ = (workspace_id, note_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::assign_agent not implemented".to_string(),
            ))
        })
    }

    /// `agent.delegate`: delegate a task to a new agent (PROTOCOL §5.5). Part of
    /// the shared MCP surface agents call back into; the runtime wiring
    /// (spawn/ACP) lands in a later milestone, so the default returns an
    /// internal error and the result is the opaque service value.
    fn agent_delegate(
        &self,
        workspace_id: WorkspaceId,
        input: AgentDelegateInput,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, input);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_delegate not implemented".to_string(),
            ))
        })
    }

    /// `comment.add`: text-anchored comment via searchContext + commentTarget (§5.3).
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
        let _ = (
            workspace_id,
            note_id,
            search_context,
            comment_target,
            comment,
            kind,
            author,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_add not implemented".to_string(),
            ))
        })
    }

    /// `comment.list`: thread summaries with optional filters (PROTOCOL §5.3).
    fn comment_list(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        since: Option<String>,
        author_type: Option<String>,
        status: Option<String>,
        include_comments: bool,
    ) -> BoxFuture<'_, Result<CommentListResult>> {
        let _ = (
            workspace_id,
            note_id,
            since,
            author_type,
            status,
            include_comments,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_list not implemented".to_string(),
            ))
        })
    }

    /// `comment.getThread`: one thread by `threadId` or `commentId` (§5.3).
    fn comment_get_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentGetThreadResult>> {
        let _ = (workspace_id, note_id, thread_id, comment_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_get_thread not implemented".to_string(),
            ))
        })
    }

    /// `comment.respond`: add a reply to a thread (PROTOCOL §5.3).
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
        let _ = (
            workspace_id,
            note_id,
            thread_id,
            comment_id,
            comment,
            kind,
            author,
            suggestion_original,
            suggestion_proposed,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_respond not implemented".to_string(),
            ))
        })
    }

    /// `comment.delete`: remove a comment by id (PROTOCOL §5.3).
    fn comment_delete(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        comment_id: String,
    ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
        let _ = (workspace_id, note_id, comment_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_delete not implemented".to_string(),
            ))
        })
    }

    /// `event.recentFiles`: most-recently changed files (PROTOCOL §5.10).
    fn event_recent_files(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        let _ = (workspace_id, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_recent_files not implemented".to_string(),
            ))
        })
    }

    /// `event.agentActivity`: per-agent files (with `agentId`) or aggregated
    /// agent activity (without). The union result is returned as raw JSON
    /// (`FileActivity[]` or `AgentActivity[]`) (PROTOCOL §5.10).
    fn event_agent_activity(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<String>,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, minutes_ago);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_agent_activity not implemented".to_string(),
            ))
        })
    }

    /// `event.workspaceSummary`: aggregated activity summary (PROTOCOL §5.10).
    fn event_workspace_summary(
        &self,
        workspace_id: WorkspaceId,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<WorkspaceEventSummary>> {
        let _ = (workspace_id, minutes_ago);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_workspace_summary not implemented".to_string(),
            ))
        })
    }

    /// `event.directoryChanges`: recent `file:changed` under a prefix (§5.10).
    fn event_directory_changes(
        &self,
        workspace_id: WorkspaceId,
        dir: String,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        let _ = (workspace_id, dir, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_directory_changes not implemented".to_string(),
            ))
        })
    }

    /// `event.query`: filtered event query over the append-only log (§5.10).
    fn event_query(
        &self,
        workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<Vec<Event>>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_query not implemented".to_string(),
            ))
        })
    }

    /// `event.subscribe` (deprecated alias): service-style subscription result;
    /// does NOT wire WS streaming (use `events.subscribe`) (PROTOCOL §5.10/§6).
    fn event_subscribe(
        &self,
        workspace_id: WorkspaceId,
        event_types: Vec<String>,
    ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
        let _ = (workspace_id, event_types);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_subscribe not implemented".to_string(),
            ))
        })
    }

    /// `event.unsubscribe` (deprecated alias): service-style result (§5.10/§6).
    fn event_unsubscribe(
        &self,
        workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<EventUnsubscribeResult>> {
        let _ = (workspace_id, subscription_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_unsubscribe not implemented".to_string(),
            ))
        })
    }
}

/// Context-engine abstraction implemented by `intent-context` (§3.1).
pub trait ContextEngine: Send + Sync {}
