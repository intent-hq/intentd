//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::ids::{AgentId, ClientId, NoteId, WorkspaceId};
use crate::model::{
    AgentDelegateInput, AgentLite, CommentAddResult, CommentDeleteResult, CommentGetThreadResult,
    CommentListResult, CommentRespondResult, Draft, Event, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, GitAgentCommitResult, GitBranches, GitCommitResult,
    GitMergeConflicts, GitStatus, Note, NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult,
    NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteSetContentResult,
    NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult, ReadAssetResult, ScriptCreateParams,
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

    /// `agent.list`: workspace agents as the stripped [`AgentLite`] projection
    /// (PROTOCOL §5.5).
    fn agent_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_list not implemented".to_string(),
            ))
        })
    }

    /// `agent.get`: one agent as [`AgentLite`]; `NotFound` falls back to disk
    /// then surfaces `-32602` (PROTOCOL §5.5).
    fn agent_get(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentLite>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get not implemented".to_string(),
            ))
        })
    }

    /// `agent.getConversation`: `{ agentId, messages, truncated, totalMessages }`
    /// capped to the most-recent `limit` (PROTOCOL §5.5).
    fn agent_get_conversation(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, limit, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_conversation not implemented".to_string(),
            ))
        })
    }

    /// `agent.create`: persist a new agent session; returns `{ agent: { id, name } }`
    /// (the process spawns lazily on first turn) (PROTOCOL §5.5).
    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, name, model, specialist_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_create not implemented".to_string(),
            ))
        })
    }

    /// `agent.sendToTask`: follow up with the agent assigned to a task note
    /// (PROTOCOL §5.5).
    fn agent_send_to_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        priority: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, task_note_id, message, priority);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_send_to_task not implemented".to_string(),
            ))
        })
    }

    /// `agent.sendMessage`: deliver a user message, auto-queuing when the agent
    /// is mid-stream; `{ success, queued, messageId? }` (PROTOCOL §5.5).
    fn agent_send_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        content: String,
        message_id: Option<String>,
        image_blocks: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, content, message_id, image_blocks);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_send_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.forceMessage`: stop the current stream then deliver immediately
    /// (PROTOCOL §5.5).
    #[allow(clippy::too_many_arguments)]
    fn agent_force_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
        content: String,
        image_blocks: Option<serde_json::Value>,
        note_ids: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            agent_id,
            message_id,
            content,
            image_blocks,
            note_ids,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_force_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.queueMessage`: explicitly enqueue a message; `{ success, queuedMessage }`
    /// (PROTOCOL §5.5).
    fn agent_queue_message(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, content, image_blocks);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_queue_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.editQueuedMessage`: edit a queued message's content (PROTOCOL §5.5).
    fn agent_edit_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id, content);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_edit_queued_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.removeQueuedMessage`: remove a queued message (PROTOCOL §5.5).
    fn agent_remove_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_remove_queued_message not implemented".to_string(),
            ))
        })
    }

    /// `agent.getQueue`: the agent's pending message queue; `{ queue: [...] }`
    /// (PROTOCOL §5.5).
    fn agent_get_queue(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_queue not implemented".to_string(),
            ))
        })
    }

    /// `agent.stop`: cancel an in-flight stream; `{ success: true }` (PROTOCOL §5.5).
    fn agent_stop(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_stop not implemented".to_string(),
            ))
        })
    }

    /// `agent.setModel`: change an agent's model (PROTOCOL §5.5).
    fn agent_set_model(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        model_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id, model_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_set_model not implemented".to_string(),
            ))
        })
    }

    /// `agent.getModels`: `{ models: [{ id, name, provider, description? }] }`
    /// from the auggie CLI with a static tier fallback; no `workspaceId`
    /// (PROTOCOL §5.5).
    fn agent_get_models(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_models not implemented".to_string(),
            ))
        })
    }

    /// `agent.rename`: rename an agent; `{ success: true, name }` (PROTOCOL §5.5).
    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_rename not implemented".to_string(),
            ))
        })
    }

    /// `agent.delete`: delete an agent session; `{ success: true }` (PROTOCOL §5.5).
    fn agent_delete(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_delete not implemented".to_string(),
            ))
        })
    }

    /// `agent.wakeOrCreate`: resume/create the agent assigned to a task note
    /// (PROTOCOL §5.5).
    fn agent_wake_or_create(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        model: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, task_note_id, context_message, model);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_wake_or_create not implemented".to_string(),
            ))
        })
    }

    /// `agent.summary`: a quick summary of what the agent did (PROTOCOL §5.5).
    fn agent_summary(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_summary not implemented".to_string(),
            ))
        })
    }

    /// `agent.reportToParent`: child→parent report; `-32603` when the caller is
    /// not a delegated agent (PROTOCOL §5.5).
    fn agent_report_to_parent(
        &self,
        workspace_id: WorkspaceId,
        report: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, report);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_report_to_parent not implemented".to_string(),
            ))
        })
    }

    /// `agent.getSubscriptions`: `{ subscriptions, delegationGroups, agentStatuses }`
    /// (PROTOCOL §5.5).
    fn agent_get_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_subscriptions not implemented".to_string(),
            ))
        })
    }

    /// `agent.cancelSubscriptions`: cancel all of an agent's subscriptions;
    /// `{ success: true }` (PROTOCOL §5.5).
    fn agent_cancel_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, agent_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_cancel_subscriptions not implemented".to_string(),
            ))
        })
    }

    /// `agent.subscribe` (deprecated alias): service-style subscription result;
    /// not the WS streaming surface (use `events.subscribe`) (PROTOCOL §5.5/§6).
    fn agent_subscribe(
        &self,
        workspace_id: WorkspaceId,
        event_types: Vec<String>,
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, event_types, exclude_self, batch_window);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_subscribe not implemented".to_string(),
            ))
        })
    }

    /// `agent.unsubscribe` (deprecated alias): service-style result (PROTOCOL §5.5/§6).
    fn agent_unsubscribe(
        &self,
        workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, subscription_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_unsubscribe not implemented".to_string(),
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

    /// `git.status`: working-tree status for a workspace. Remote workspaces and
    /// non-repositories return the empty status (PROTOCOL §5.6).
    fn git_status(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<GitStatus>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_status not implemented".to_string(),
            ))
        })
    }

    /// `git.stage`: stage `paths` (CSV string or array). `.`/`*`/`--all` are
    /// rejected (`-32603`); returns the validated path list (PROTOCOL §5.6).
    fn git_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_stage not implemented".to_string(),
            ))
        })
    }

    /// `git.getBranches`: branches for a known `repo_path`; an unknown repo path
    /// is `-32602` (PROTOCOL §5.6).
    fn git_get_branches(
        &self,
        repo_path: String,
        include_remote: bool,
    ) -> BoxFuture<'_, Result<GitBranches>> {
        let _ = (repo_path, include_remote);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_get_branches not implemented".to_string(),
            ))
        })
    }

    /// `git.commit` (deprecated; prefer `git_agent_commit`): commit the already
    /// staged changes with `message`. Failures (incl. nothing to commit) are
    /// `-32603` (PROTOCOL §5.6).
    fn git_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
    ) -> BoxFuture<'_, Result<GitCommitResult>> {
        let _ = (workspace_id, message);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commit not implemented".to_string(),
            ))
        })
    }

    /// `git.agentCommit`: stage the agent's changes (or `files` when given) and
    /// commit them; `user_requested` bypasses the auto-commit gate (PROTOCOL
    /// §5.6).
    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        let _ = (workspace_id, message, files, user_requested);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_agent_commit not implemented".to_string(),
            ))
        })
    }

    /// `git.checkMergeConflicts`: whether merging the current branch into
    /// `target_branch` (or the detected default) would conflict (PROTOCOL §5.6).
    fn git_check_merge_conflicts(
        &self,
        workspace_id: WorkspaceId,
        target_branch: Option<String>,
    ) -> BoxFuture<'_, Result<GitMergeConflicts>> {
        let _ = (workspace_id, target_branch);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_check_merge_conflicts not implemented".to_string(),
            ))
        })
    }

    /// `pr.status`: the active PR's state, mergeability, and summary. Requires an
    /// active PR; otherwise `-32603` (PROTOCOL §5.7).
    fn pr_status(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_status not implemented".to_string(),
            ))
        })
    }

    /// `pr.listComments`: conversation-level comments on the active PR, clamped to
    /// `count` (default 20, max 100) (PROTOCOL §5.7).
    fn pr_list_comments(
        &self,
        workspace_id: WorkspaceId,
        count: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, count);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_list_comments not implemented".to_string(),
            ))
        })
    }

    /// `pr.listReviewComments`: line-anchored review threads on the active PR,
    /// filtered by `path` / `status` (PROTOCOL §5.7).
    fn pr_list_review_comments(
        &self,
        workspace_id: WorkspaceId,
        path: Option<String>,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_list_review_comments not implemented".to_string(),
            ))
        })
    }

    /// `pr.getReviews`: the review decision aggregate + reviews for the active PR
    /// (or `pr_number` when given) (PROTOCOL §5.7 extension).
    fn pr_get_reviews(
        &self,
        workspace_id: WorkspaceId,
        pr_number: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, pr_number);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_get_reviews not implemented".to_string(),
            ))
        })
    }

    /// `pr.listCheckRuns`: CI check-run tally + runs for `git_ref` (defaults to
    /// the PR head) (PROTOCOL §5.7 extension).
    fn pr_list_check_runs(
        &self,
        workspace_id: WorkspaceId,
        git_ref: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, git_ref);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_list_check_runs not implemented".to_string(),
            ))
        })
    }

    /// `pr.merge`: merge the active PR with `merge_method` (default `merge`) and
    /// optional commit overrides. Requires an active PR (PROTOCOL §5.7).
    fn pr_merge(
        &self,
        workspace_id: WorkspaceId,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, merge_method, commit_title, commit_message);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_merge not implemented".to_string(),
            ))
        })
    }

    /// `pr.updateBranch`: update the active PR branch from its base (PROTOCOL §5.7).
    fn pr_update_branch(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_update_branch not implemented".to_string(),
            ))
        })
    }

    /// `pr.postComment`: post a conversation comment on the active PR (PROTOCOL §5.7).
    fn pr_post_comment(
        &self,
        workspace_id: WorkspaceId,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, body);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_post_comment not implemented".to_string(),
            ))
        })
    }

    /// `pr.replyToReviewComment`: reply to a review comment on the active PR
    /// (PROTOCOL §5.7).
    fn pr_reply_to_review_comment(
        &self,
        workspace_id: WorkspaceId,
        comment_id: u64,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, comment_id, body);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_reply_to_review_comment not implemented".to_string(),
            ))
        })
    }

    /// `pr.resolveThread`: resolve/unresolve a review thread (default `resolve`)
    /// on the active PR (PROTOCOL §5.7).
    fn pr_resolve_thread(
        &self,
        workspace_id: WorkspaceId,
        thread_id: String,
        action: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, thread_id, action);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_resolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `pr.createReview`: submit a review (`approve` / `request-changes` /
    /// `comment`) on the active PR (PROTOCOL §5.7 extension).
    fn pr_create_review(
        &self,
        workspace_id: WorkspaceId,
        verdict: String,
        body: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, verdict, body);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_create_review not implemented".to_string(),
            ))
        })
    }

    /// `pr.waitForChanges`: poll the active PR's status + checks until a change
    /// is detected or the timeout elapses (PROTOCOL §5.7).
    fn pr_wait_for_changes(
        &self,
        workspace_id: WorkspaceId,
        timeout_seconds: Option<i64>,
        poll_interval_seconds: Option<i64>,
        watch: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, timeout_seconds, poll_interval_seconds, watch);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::pr_wait_for_changes not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.init`: initialize/attach the tracker for a workspace
    /// (`{ ok: true }`) (PROTOCOL §5.19).
    fn file_tracking_init(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_init not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.sync`: reconcile tracked changes against the live git
    /// worktree, preserving attribution (PROTOCOL §5.19).
    fn file_tracking_sync(
        &self,
        workspace_id: WorkspaceId,
        force: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, force);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_sync not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.load`: the tracked-change review list
    /// (`{ changes, truncated, totalCount }`) (PROTOCOL §5.19).
    fn file_tracking_load(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_load not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.getChanges`: the filtered tracked-change list
    /// (`{ changes, truncated, totalCount }`) (PROTOCOL §5.19).
    fn file_tracking_get_changes(
        &self,
        workspace_id: WorkspaceId,
        filter: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, filter);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_get_changes not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.loadCommits`: commit history with attribution
    /// (`{ commits: CommitWithAttribution[] }`) (PROTOCOL §5.19).
    fn file_tracking_load_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_load_commits not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.getLineStats`: real-time additions/deletions totals
    /// (`{ additions, deletions }`) (PROTOCOL §5.19).
    fn file_tracking_get_line_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_get_line_stats not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.stage`: stage `paths` and move their audit rows to the
    /// `staged` stage (`{ ok: true }`) (PROTOCOL §5.19).
    fn file_tracking_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_stage not implemented".to_string(),
            ))
        })
    }

    /// `file-tracking.unstage`: unstage `paths` and move their audit rows back to
    /// the `unstaged` stage (`{ ok: true }`) (PROTOCOL §5.19).
    fn file_tracking_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tracking_unstage not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getWorkspaceStats`: the workspace's line-change `Metrics`
    /// (`{ additions, deletions, filesChanged, byAgent }`) or `null` (PROTOCOL §5.20).
    fn metrics_get_workspace_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_workspace_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getAgentStats`: one agent's `Metrics` summed across workspaces
    /// (`{ additions, deletions, filesChanged }`, `byAgent` omitted) or `null` (§5.20).
    fn metrics_get_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_agent_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.getAllWorkspaceStats`: a `{ [workspaceId]: Metrics }` map across
    /// every workspace (PROTOCOL §5.20).
    fn metrics_get_all_workspace_stats(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_get_all_workspace_stats not implemented".to_string(),
            ))
        })
    }

    /// `metrics.clearAgentStats`: reset one agent's counters across workspaces,
    /// returning `{ success: boolean }` (PROTOCOL §5.20).
    fn metrics_clear_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::metrics_clear_agent_stats not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // accept-changes.* — commit→push→PR→merge orchestration (PROTOCOL §5.18).
    // ------------------------------------------------------------------------

    /// `accept-changes.getStatus`: the `WorkspaceGitStatus` for the accept-changes
    /// panel (branch, ahead/behind trunk, remote/push state, local commits with
    /// attribution, linked PR) (PROTOCOL §5.18).
    fn accept_changes_get_status(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_get_status not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.prepare`: validate an action and return a `PrepareResult`
    /// (warnings/errors, suggested commit message/PR fields, per-file stats)
    /// (PROTOCOL §5.18).
    fn accept_changes_prepare(
        &self,
        workspace_id: WorkspaceId,
        action: String,
        files: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, action, files);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_prepare not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.execute`: run the requested action (commit, optionally
    /// chaining push + create-PR via `options`) and return an `AcceptChangesResult`
    /// with per-step status (PROTOCOL §5.18). `params` is the full request object.
    fn accept_changes_execute(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_execute not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.mergePR`: merge the linked PR via the forge and return an
    /// `AcceptChangesResult` (PROTOCOL §5.18).
    fn accept_changes_merge_pr(
        &self,
        workspace_id: WorkspaceId,
        pr_number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            pr_number,
            merge_method,
            commit_title,
            commit_message,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_merge_pr not implemented".to_string(),
            ))
        })
    }

    /// `accept-changes.addRemote`: add (and, if needed, initialize) the `origin`
    /// remote, returning the refreshed `WorkspaceGitStatus` (PROTOCOL §5.18).
    fn accept_changes_add_remote(
        &self,
        workspace_id: WorkspaceId,
        remote_url: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, remote_url);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::accept_changes_add_remote not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // search.* — BE-owned file/path search (PROTOCOL §5.15, IMPLEMENTATION_SPEC §14).
    // ------------------------------------------------------------------------

    /// `search.inFiles`: gitignore-aware ripgrep content search over the
    /// workspace worktree. `opts` is the raw `{ caseSensitive?, regex?, globs?,
    /// maxResults? }` object; a malformed `opts.regex` surfaces as
    /// `InvalidParams` ("Invalid regex"). Returns `{ requestId, matches,
    /// truncated }`, minting `request_id` when absent (PROTOCOL §5.15).
    fn search_in_files(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        opts: Option<serde_json::Value>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, query, opts, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_in_files not implemented".to_string(),
            ))
        })
    }

    /// `search.fileNames`: gitignore-aware path/glob filename search over the
    /// worktree. Returns `{ requestId, files, truncated }`, minting `request_id`
    /// when absent (PROTOCOL §5.15).
    fn search_file_names(
        &self,
        workspace_id: WorkspaceId,
        pattern: String,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, pattern, limit, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_file_names not implemented".to_string(),
            ))
        })
    }

    /// `search.cancel`: abort an in-flight search by its `requestId`. A no-op
    /// success for an unknown/already-finished id (`{ ok: true }`) (PROTOCOL §5.15).
    fn search_cancel(&self, request_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_cancel not implemented".to_string(),
            ))
        })
    }

    /// `search.messages`: substring search over a workspace's persisted agent
    /// session messages. Returns `{ requestId, matches: MessageMatch[] }` inline,
    /// or `{ requestId, matches: [] }` (a prompt ack) when the result set is
    /// streamed via `search:result`/`search:done` (PROTOCOL §5.15 / §6.5).
    fn search_messages(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        agent_id: Option<String>,
        role: Option<String>,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, query, agent_id, role, limit, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_messages not implemented".to_string(),
            ))
        })
    }

    /// `search.events`: substring search over the BE event log. `workspaceId` is
    /// optional (absent → all workspaces). Returns `{ requestId, matches:
    /// EventMatch[] }` inline or a streamed prompt ack (PROTOCOL §5.15 / §6.5).
    fn search_events(
        &self,
        query: String,
        workspace_id: Option<WorkspaceId>,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, workspace_id, limit, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_events not implemented".to_string(),
            ))
        })
    }

    /// `search.memories`: substring search over the BE memories store. The
    /// `memories` table is not created until M9; until then this returns an
    /// empty match set (parity-safe, no error) (PROTOCOL §5.15).
    fn search_memories(
        &self,
        query: String,
        workspace_id: Option<WorkspaceId>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, workspace_id, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_memories not implemented".to_string(),
            ))
        })
    }

    /// `search.notes`: GLOBAL substring search over the BE notes store (no
    /// `workspaceId`). Returns `{ requestId, matches: NoteMatch[] }` (PROTOCOL §5.15).
    fn search_notes(
        &self,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_notes not implemented".to_string(),
            ))
        })
    }

    /// `search.codebase`: v1 ripgrep/symbol-backed content search over a
    /// workspace worktree (context-engine wiring deferred, §8). Returns
    /// `{ requestId, matches: CodebaseMatch[] }` inline or a streamed prompt ack
    /// (PROTOCOL §5.15 / §6.5).
    fn search_codebase(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, query, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::search_codebase not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // terminal.* — interactive PTYs on the unified host (PROTOCOL §5.13, §12).
    // ------------------------------------------------------------------------

    /// `terminal.create`: spawn a PTY (default shell when `command` is absent)
    /// scoped to the workspace and start fanning its output to subscribers as
    /// `terminal:data` events. Returns `{ terminalId }` (PROTOCOL §5.13).
    fn terminal_create(
        &self,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, cols, rows, cwd, command);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_create not implemented".to_string(),
            ))
        })
    }

    /// `terminal.write`: write base64-encoded input bytes to a PTY's stdin
    /// (writes are serialized into the single master). Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_write(
        &self,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, data);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_write not implemented".to_string(),
            ))
        })
    }

    /// `terminal.resize`: resize a PTY's visible area. Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_resize(
        &self,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, cols, rows);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_resize not implemented".to_string(),
            ))
        })
    }

    /// `terminal.kill`: signal/terminate a PTY's process group; the streamer
    /// emits `terminal:exit` when the process ends. Returns `{ ok: true }`
    /// (PROTOCOL §5.13).
    fn terminal_kill(&self, terminal_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = terminal_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_kill not implemented".to_string(),
            ))
        })
    }

    /// `terminal.getBuffer`: snapshot a PTY's server-side scrollback for replay,
    /// base64-encoded (optionally trailing `maxBytes`). Returns
    /// `{ terminalId, data }` (PROTOCOL §5.13).
    fn terminal_get_buffer(
        &self,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (terminal_id, max_bytes);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_get_buffer not implemented".to_string(),
            ))
        })
    }

    /// `terminal.list`: the workspace's live terminals as `{ terminals: [...] }`
    /// (PROTOCOL §5.9).
    fn terminal_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_list not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // script.* — named processes on the unified PTY host (PROTOCOL §5.8, §12.2).
    // ------------------------------------------------------------------------

    /// `script.list`: the workspace's scripts with runtime state as
    /// `{ scripts: [...] }` (PROTOCOL §5.8).
    fn script_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_list not implemented".to_string(),
            ))
        })
    }

    /// `script.create`: register a script definition; returns the created
    /// [`Script`](crate::model::Script) (PROTOCOL §5.8).
    fn script_create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_create not implemented".to_string(),
            ))
        })
    }

    /// `script.remove`: stop (if running) and forget a script; returns
    /// `{ ok, scriptId }` (PROTOCOL §5.8).
    fn script_remove(&self, script_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = script_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_remove not implemented".to_string(),
            ))
        })
    }

    /// `script.start`: spawn the script on the PTY host (service mode auto-
    /// restarts per policy); returns `{ ok, scriptId }` (PROTOCOL §5.8).
    fn script_start(&self, script_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = script_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_start not implemented".to_string(),
            ))
        })
    }

    /// `script.stop`: stop a running script (cancels pending auto-restart);
    /// returns `{ ok, scriptId }` (PROTOCOL §5.8).
    fn script_stop(&self, script_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = script_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_stop not implemented".to_string(),
            ))
        })
    }

    /// `script.restart`: stop then start, resetting the restart counter; returns
    /// `{ ok, scriptId }` (PROTOCOL §5.8).
    fn script_restart(&self, script_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = script_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_restart not implemented".to_string(),
            ))
        })
    }

    /// `script.output`: the script's current PTY scrollback as plaintext
    /// output-buffer text (optionally trailing `maxLines`, default 100); returns
    /// a bare string (`"No output yet."` when empty), not an object (§5.8).
    fn script_output(
        &self,
        script_id: String,
        max_lines: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (script_id, max_lines);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_output not implemented".to_string(),
            ))
        })
    }

    /// `script.status`: the script's [`ScriptRuntimeState`](crate::model::ScriptRuntimeState)
    /// (PROTOCOL §5.8).
    fn script_status(&self, script_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = script_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_status not implemented".to_string(),
            ))
        })
    }

    /// `script.run`: run a command-mode script to completion (optional
    /// `timeoutSeconds`), returning `{ exitCode?, output, timedOut?, warning? }`;
    /// service-mode scripts return a `warning` (PROTOCOL §5.8).
    fn script_run(
        &self,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (script_id, max_lines, timeout_seconds);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::script_run not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // client.hello + drafts.* — stable client identity & per-client drafts
    // (PROTOCOL §5.16/§5.17, IMPLEMENTATION_SPEC §15/§16). These back the
    // transport-level interceptors (§16); they are not routed through the
    // JSON-RPC dispatcher, but live on `WorkspaceApi` so the transport reaches
    // persistence through services without depending on `intent-store` (§3.2).
    // ------------------------------------------------------------------------

    /// `client.hello` persistence: upsert the logical `client` row, setting
    /// `first_seen` once and touching `last_seen`, and persisting `name` /
    /// `capabilities` (a JSON bag). The connection→client binding and the
    /// `server` capability block are transport concerns (§16) (PROTOCOL §5.17).
    fn upsert_client(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (client_id, name, capabilities);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::upsert_client not implemented".to_string(),
            ))
        })
    }

    /// `drafts.get`: the draft for the calling client (`client_id` resolved from
    /// the connection, never a param), or `None` when absent (PROTOCOL §5.16).
    fn draft_get(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<Option<Draft>>> {
        let _ = (workspace_id, agent_id, client_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_get not implemented".to_string(),
            ))
        })
    }

    /// `drafts.set`: upsert the calling client's draft. An empty `text` is a
    /// clear (the row is deleted). Returns `Some(updatedAt)` when a draft was
    /// stored or `None` when it was cleared, and emits `draft:changed` (carrying
    /// `hasDraft`, never the text) (PROTOCOL §5.16/§6.5).
    fn draft_set(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
        text: String,
    ) -> BoxFuture<'_, Result<Option<String>>> {
        let _ = (workspace_id, agent_id, client_id, text);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_set not implemented".to_string(),
            ))
        })
    }

    /// `drafts.clear`: delete the calling client's draft (idempotent if none),
    /// emitting `draft:changed` with `hasDraft: false` (PROTOCOL §5.16/§6.5).
    fn draft_clear(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<()>> {
        let _ = (workspace_id, agent_id, client_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::draft_clear not implemented".to_string(),
            ))
        })
    }
}

/// Context-engine abstraction implemented by `intent-context` (§3.1).
pub trait ContextEngine: Send + Sync {}
