//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::{AgentId, ClientId, NoteId, WorkspaceId};
use crate::model::{
    AgentDelegateInput, AgentLite, CommentAddResult, CommentDeleteResult, CommentGetThreadResult,
    CommentListResult, CommentResolveThreadResult, CommentRespondResult, Draft, EventQueryParams,
    EventSubscribeResult, EventUnsubscribeResult, FileActivity, GitAgentCommitResult,
    GitBranchStatus, GitBranches, GitCommitResult, GitMergeConflicts, GitPullResult, GitStatus,
    Note, NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput,
    NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteRestoreVersionResult,
    NoteSetContentResult, NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult, NoteVersion,
    NoteVersionSummary, ProjectType, ReadAssetResult, ScriptCreateParams, SetupScript,
    TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskListResult, TaskMarkAsTaskResult, TaskUpdateNoteStatusResult,
    TaskUpdateResult, TaskUpdateStatusResult, TokenUsage, Workspace, WorkspaceCreate,
    WorkspaceEventSummary, WorkspaceTask, WorkspaceUpdate,
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
    ///
    /// `idempotency_key` is the optional `params.idempotencyKey` (design note TB-0
    /// §5): when present and previously recorded, the original result is returned
    /// without re-executing; soft-launch when absent (warn + execute).
    fn create_workspace(
        &self,
        input: WorkspaceCreate,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let _ = (input, idempotency_key);
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

    /// Read the durable token/credit usage snapshot for a workspace (§5.23). The
    /// scan job itself is daemon-internal (no RPC); this is the wire **read** and
    /// returns a default (empty, `lastScanAt: null`) snapshot before the first
    /// scan. `NotFound` if the workspace is absent (router maps it to `-32602`).
    fn get_token_usage(&self, id: WorkspaceId) -> BoxFuture<'_, Result<TokenUsage>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_token_usage not implemented".to_string(),
            ))
        })
    }

    /// Read the durable worktree setup script for a workspace (§5.25). Returns a
    /// default (empty `script`, `updatedAt: 0`) record before the first save.
    /// `NotFound` if the workspace is absent (router maps it to `-32602`).
    fn get_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_setup_script not implemented".to_string(),
            ))
        })
    }

    /// Persist a hand-written setup-script body and return the stored record with
    /// `generatedBy: "user"` and a fresh `updatedAt` (§5.25). `NotFound` if the
    /// workspace is absent.
    fn save_setup_script(
        &self,
        id: WorkspaceId,
        script: String,
    ) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = (id, script);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::save_setup_script not implemented".to_string(),
            ))
        })
    }

    /// Classify the workspace's project from manifest files (§5.25); `None` when
    /// no known manifest is found. `NotFound` if the workspace is absent.
    fn detect_project_type(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Option<ProjectType>>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::detect_project_type not implemented".to_string(),
            ))
        })
    }

    /// Produce an AI-assisted draft setup script (§5.25), returned (not persisted)
    /// with `generatedBy: "agent"`. `NotFound` if the workspace is absent.
    fn generate_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::generate_setup_script not implemented".to_string(),
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
    ///
    /// `idempotency_key` is the optional `params.idempotencyKey` (design note TB-0
    /// §5): when present and previously recorded, the original result is returned
    /// without re-executing; soft-launch when absent (warn + execute).
    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<Note>> {
        let _ = (workspace_id, input, idempotency_key);
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
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let _ = (
            workspace_id,
            note_id,
            content,
            confirm_replacement,
            expected_version,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::set_note_content not implemented".to_string(),
            ))
        })
    }

    /// `note.updateMetadata`: title/tags (spec title is skipped) (PROTOCOL §5.2).
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        let _ = (workspace_id, note_id, title, tags, expected_version);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::update_note_metadata not implemented".to_string(),
            ))
        })
    }

    /// `note.delete`: remove a note (PROTOCOL §5.2). `expected_version` gates the
    /// delete on the current `rev` when `Some` (§5.6); on a stale value the
    /// conflict carries the current entity snapshot prior to deletion.
    fn delete_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let _ = (workspace_id, note_id, expected_version);
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

    /// `note.listVersions`: stored versions ascending by `v`, without content
    /// blobs (PROTOCOL §5.2 version-history extensions).
    fn list_note_versions(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteVersionSummary>>> {
        let _ = (workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_note_versions not implemented".to_string(),
            ))
        })
    }

    /// `note.getVersion`: one stored version with content (PROTOCOL §5.2
    /// version-history extensions).
    fn get_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
    ) -> BoxFuture<'_, Result<NoteVersion>> {
        let _ = (workspace_id, note_id, v);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::get_note_version not implemented".to_string(),
            ))
        })
    }

    /// `note.restoreVersion`: reset content to version `v` and append a new
    /// version capturing the restored state (PROTOCOL §5.2 version-history
    /// extensions).
    fn restore_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
    ) -> BoxFuture<'_, Result<NoteRestoreVersionResult>> {
        let _ = (workspace_id, note_id, v);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::restore_note_version not implemented".to_string(),
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
    /// `expected_version` gates the write on the current `rev` when `Some` (§5.6).
    fn task_update_note_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let _ = (workspace_id, note_id, status, expected_version);
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

    /// `task.list`: project a workspace's spec-linked task notes into the
    /// canonical `WorkspaceTask` list **plus** the workspace-wide `taskStats`
    /// aggregate (PROTOCOL §5.4). `status` optionally filters the task list to
    /// a single status; `stats` is always computed over the unfiltered
    /// spec-linked set so the FE can render the progress rollup verbatim
    /// (mirrors the canonical FE `computeTaskStats` in `task-stats.ts`).
    fn task_list(
        &self,
        workspace_id: WorkspaceId,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskListResult>> {
        let _ = (workspace_id, status);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_list not implemented".to_string(),
            ))
        })
    }

    /// `task.get`: project a single task note into a `WorkspaceTask` (PROTOCOL
    /// §5.4). Errors with `NotFound` when the note is absent/cross-workspace and
    /// `Internal` when the note carries no task metadata.
    fn task_get(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<WorkspaceTask>> {
        let _ = (workspace_id, task_note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::task_get not implemented".to_string(),
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
    ///
    /// `parent_agent_id` is the caller/spawning agent: the MCP front door passes
    /// `Some(caller)` to stamp the child's `parentAgentId`; the FE/RPC front door
    /// passes `None` (top-level creates stay parentless).
    fn agent_delegate(
        &self,
        workspace_id: WorkspaceId,
        input: AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, input, parent_agent_id);
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
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, limit, workspace_id, page_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_conversation not implemented".to_string(),
            ))
        })
    }

    /// The agent's in-flight ("live") turn, if a `session/prompt` is currently
    /// streaming (CS-0 D5): the partial assistant message as `{ messageId,
    /// contentBlocks }` so `chat.subscribe` can merge it into the seq-0 snapshot
    /// and a client arriving mid-turn sees a coherent in-flight message. A
    /// synchronous in-memory read (no I/O). The default returns `None` (no live
    /// turn surfaced) so non-agent `WorkspaceApi` impls need not implement it.
    fn agent_live_turn(&self, agent_id: AgentId) -> Option<serde_json::Value> {
        let _ = agent_id;
        None
    }

    /// Whether a turn loop is currently in flight for `agent_id` — the
    /// authoritative "active worker" signal backing the chat snapshot's live
    /// merge gate. `chat.subscribe` consults this before merging the in-memory
    /// `agent_live_turn` so a lingering live-turn slot with no real worker (or
    /// a session that never finalized across a crash) does not surface a
    /// phantom streaming message. Synchronous (no I/O); default `false` so
    /// non-agent `WorkspaceApi` impls need not implement it.
    fn agent_is_busy(&self, agent_id: AgentId) -> bool {
        let _ = agent_id;
        false
    }

    /// The daemon-owned runtime activity flags for `agent_id` as the object
    /// `{ isResponding, isWaitingOnTool, isWaitingForOtherAgents, waitingForAgentIds }`
    /// (PROTOCOL §5.5/§7.1): the BE-authoritative port of the FE agent-state
    /// selectors so `chat.subscribe`'s seq-0 snapshot carries the same liveness
    /// signal as the `AgentLite` projection. `isResponding` is the in-flight
    /// "active worker" signal ([`agent_is_busy`](WorkspaceApi::agent_is_busy));
    /// `isWaitingOnTool` is true when that turn has an unresolved `tool_use`;
    /// `isWaitingForOtherAgents` is true when the agent parents one or more
    /// pending completion watches; `waitingForAgentIds` is the distinct child
    /// agent-ids of those watches (non-empty iff `isWaitingForOtherAgents`,
    /// always emitted as `[]` otherwise — never null/omitted). Default returns
    /// all-`false`/`[]` so non-agent `WorkspaceApi` impls need not implement it.
    fn agent_activity_flags(&self, agent_id: AgentId) -> BoxFuture<'_, serde_json::Value> {
        let _ = agent_id;
        Box::pin(async {
            serde_json::json!({
                "isResponding": false,
                "isWaitingOnTool": false,
                "isWaitingForOtherAgents": false,
                "waitingForAgentIds": [],
            })
        })
    }

    /// `agent.create`: persist a new agent session; returns
    /// `{ agent: <AgentLite> }` (the full projection — a superset of the earlier
    /// `{ id, name }` shape, so existing readers stay green) (PROTOCOL §5.5).
    ///
    /// `parent_agent_id` is the caller/spawning agent: the MCP front door passes
    /// `Some(caller)` to stamp the child's `parentAgentId`; the FE/RPC front door
    /// passes `None` (top-level creates stay parentless).
    ///
    /// `requested_agent_id` is an optional well-formed `agent-{uuid}` id the
    /// client already minted (e.g. the FE's `UnifiedAgentFactory` uses it to
    /// key the pending session, then addresses `agent.sendMessage` at the same
    /// id). When `Some`, the service adopts it verbatim; otherwise a fresh id
    /// is minted. Malformed values are rejected as `-32602`.
    ///
    /// `extra` carries the widened FE-facing spawn hints
    /// (`provider`/`agentType`/`metadata`/`workspacePath`/`workspaceContext`).
    /// Only `provider` currently lands on the persisted session; the other
    /// fields are accepted so the FE seam can bind to the wire shape ahead of
    /// full persistence.
    #[allow(clippy::too_many_arguments)]
    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist_id: Option<String>,
        parent_agent_id: Option<AgentId>,
        idempotency_key: Option<String>,
        requested_agent_id: Option<AgentId>,
        extra: crate::model::AgentCreateExtra,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            name,
            model,
            specialist_id,
            parent_agent_id,
            idempotency_key,
            requested_agent_id,
            extra,
        );
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

    /// `agent.queueMessage`: explicitly enqueue a message; `{ success,
    /// queuedMessage }` where `queuedMessage` is `{ id, content, queuedAt,
    /// position, imageBlocks? }` (PROTOCOL §5.5).
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
    /// `editing` (optional) toggles the entry's under-edit state — when `Some(true)`
    /// the entry is excluded from the ready-to-send queue (drain skips it); when
    /// `Some(false)` it is re-included and self-drains; when `None` the editing
    /// flag is left unchanged (backwards-compatible with the original wire shape).
    fn agent_edit_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
        editing: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, message_id, content, editing);
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

    /// `agent.getQueue`: the agent's pending message queue; `{ success, queue:
    /// [{ id, content, queuedAt, position, imageBlocks? }] }` (PROTOCOL §5.5).
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

    /// `agent.respondPermission`: resolve an outstanding interactive permission
    /// prompt by `requestId`, unblocking the agent with the §8 `outcome`
    /// (`{ outcome: "selected", optionId }` / `{ outcome: "cancelled" }`).
    /// `{ resolved: bool }` (`false` when no such pending prompt) (PROTOCOL §8).
    fn agent_respond_permission(
        &self,
        request_id: String,
        outcome: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (request_id, outcome);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_respond_permission not implemented".to_string(),
            ))
        })
    }

    /// `agent.pendingPermissions`: the outstanding interactive permission prompts
    /// as `{ requests: [PermissionRequestData…] }`, optionally filtered to one
    /// agent by `agentId` (= `sessionId`) (PROTOCOL §8).
    fn agent_pending_permissions(
        &self,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = agent_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_pending_permissions not implemented".to_string(),
            ))
        })
    }

    /// `agent.rename`: rename an agent; `{ success: true, name }` (PROTOCOL §5.5).
    /// With `skip_if_explicitly_set = true`, an already-explicitly-named session
    /// is left untouched and the result carries `skipped: true` (P3-1.2b).
    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (agent_id, name, skip_if_explicitly_set);
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

    /// `agent.getSessionStats`: the per-session credit/message/tool rollup as
    /// `{ stats: SessionStats }` (PROTOCOL §5.24). `sessionId` is required; an
    /// unknown session surfaces `NotFound` which the router maps to `-32602`.
    fn agent_get_session_stats(
        &self,
        session_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = session_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_get_session_stats not implemented".to_string(),
            ))
        })
    }

    /// `agent.diagnostics`: a sanitized snapshot of agent statuses,
    /// subscriptions, delegation groups, and stuck-risk signals as
    /// `{ ok, diagnostics, text }` (PROTOCOL §5.5). Optional `agent_id` /
    /// `task_note_id` focus the snapshot; `stale_responding_after_ms` tunes the
    /// stale-responding threshold (default 600000).
    fn agent_diagnostics(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        stale_responding_after_ms: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            agent_id,
            task_note_id,
            stale_responding_after_ms,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::agent_diagnostics not implemented".to_string(),
            ))
        })
    }

    /// `agent.reportToParent`: child→parent report; `-32603` when the caller is
    /// not a delegated agent (PROTOCOL §5.5).
    ///
    /// `caller_agent_id` is the agent invoking the tool: the MCP front door
    /// passes `Some(caller)` so the impl can resolve the caller's
    /// `parentAgentId`; the FE/RPC front door passes `None`, which always
    /// surfaces `-32603` (there is no caller context to be a delegated agent).
    fn agent_report_to_parent(
        &self,
        workspace_id: WorkspaceId,
        report: serde_json::Value,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, report, caller_agent_id);
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

    /// `comment.resolveThread`: mark every comment in a thread resolved (or
    /// `resolved = false` to reopen), identified by `threadId` or `commentId`.
    fn comment_resolve_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        resolved: bool,
    ) -> BoxFuture<'_, Result<CommentResolveThreadResult>> {
        let _ = (workspace_id, note_id, thread_id, comment_id, resolved);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::comment_resolve_thread not implemented".to_string(),
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
    /// Returns the legacy bare array by default; when `params.paginate` (or a
    /// `params.page_token`) is set it returns the `{ items, nextToken }`
    /// pagination envelope (TA-2 / §5.5), newest→oldest with the limit clamped.
    fn event_query(
        &self,
        workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, params);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::event_query not implemented".to_string(),
            ))
        })
    }

    /// `event.subscribe` (deprecated alias): service-style subscription result;
    /// does NOT wire WS streaming (use `events.subscribe`) (PROTOCOL §5.10/§6).
    /// `exclude_self`/`batch_window` mirror the TS shim's forwarded options.
    fn event_subscribe(
        &self,
        workspace_id: WorkspaceId,
        event_types: Vec<String>,
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
        let _ = (workspace_id, event_types, exclude_self, batch_window);
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

    /// `git.unstage`: unstage `paths` (CSV string or array), the inverse of
    /// `git.stage`. `.`/`*`/`--all` are rejected (`-32603`); idempotent on
    /// already-unstaged paths. Returns the validated path list (PROTOCOL §5.6).
    fn git_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let _ = (workspace_id, paths);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_unstage not implemented".to_string(),
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

    /// `git.branchStatus`: ahead/behind vs `origin/<branch_name>` + working-tree
    /// uncommitted-changes flag for a known `repo_path`. Same known-repo gate as
    /// `git_get_branches`; an unknown repo path is `-32602` (PROTOCOL §5.6).
    fn git_branch_status(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<GitBranchStatus>> {
        let _ = (repo_path, branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_branch_status not implemented".to_string(),
            ))
        })
    }

    /// `git.pull`: fetch + rebase-pull `branch_name` from `origin` for the repo
    /// at `repo_path` (path-based like `git_get_branches`; an invalid repo path
    /// is `-32602`). Ordinary pull failures are a structured `{ ok: false,
    /// error }` result, not an `Err` (PROTOCOL §5.6).
    fn git_pull(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<GitPullResult>> {
        let _ = (repo_path, branch_name);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_pull not implemented".to_string(),
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
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<GitCommitResult>> {
        let _ = (workspace_id, message, idempotency_key);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commit not implemented".to_string(),
            ))
        })
    }

    /// `git.agentCommit`: stage the agent's changes (or `files` when given) and
    /// commit them; `user_requested` bypasses the auto-commit gate (PROTOCOL
    /// §5.6). When `agent_id` (and optionally `linked_note_id`) are present, the
    /// commit body carries `Agent-Id:` / `Linked-Note-Id:` attribution trailers;
    /// the FE/transport path passes `None` for both (no agent context).
    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        agent_id: Option<AgentId>,
        linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        let _ = (
            workspace_id,
            message,
            agent_id,
            linked_note_id,
            files,
            user_requested,
        );
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

    /// `git.changes`: the working-tree file list (`FileStatus[]`) for a
    /// workspace — the same `files` array as `git.status`. Remote workspaces and
    /// non-repositories return an empty array (wire §7.7).
    fn git_changes(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_changes not implemented".to_string(),
            ))
        })
    }

    /// `git.diffs`: per-file diff hunks. When `commit_hash` is set, returns
    /// the per-file hunks for `<commit_hash>^..<commit_hash>` (the commit's own
    /// changes vs its first parent; a root commit yields all-additions) and the
    /// `staged` flag is ignored. Otherwise `staged` selects the HEAD→index diff
    /// (`true`) or the index→workdir diff (`false`, default). `path` restricts
    /// the result to a single file. Returns `[{ path, hunks }]`; remote/non-repo
    /// workspaces and an unresolvable `commit_hash` return an empty array
    /// (wire §7.7).
    fn git_diffs(
        &self,
        workspace_id: WorkspaceId,
        path: Option<String>,
        staged: bool,
        commit_hash: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, staged, commit_hash);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_diffs not implemented".to_string(),
            ))
        })
    }

    /// `git.commitDetails`: metadata + per-file `(additions, deletions)` for a
    /// single commit, addressed by `commit_hash` (full SHA or short ref).
    /// Returns the wire shape `{ commitHash, author, authorEmail, date, message,
    /// files: string[], fileDetails: [{ path, additions, deletions }] }`.
    /// Remote/non-repo workspaces and an unresolvable hash return an empty
    /// envelope (`{ commitHash, fileDetails: [], files: [] }`) so the FE renders
    /// a friendly empty state instead of crashing (wire §7.7).
    fn git_commit_details(
        &self,
        workspace_id: WorkspaceId,
        commit_hash: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, commit_hash);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commit_details not implemented".to_string(),
            ))
        })
    }

    /// `git.commits`: paginated reverse-chronological commit history as the
    /// canonical §5.5 page envelope `{ items: CommitInfo[], nextToken }`. Remote
    /// workspaces and non-repositories return an empty page (wire §7.7).
    fn git_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, limit, page_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_commits not implemented".to_string(),
            ))
        })
    }

    /// `git.clone`: streaming clone of `url` into `<parent_dir>/<target_name>`
    /// (or the URL-derived basename when `target_name` is `None`). Returns
    /// `{ requestId, targetPath }` promptly and pushes `git:clone:progress`
    /// frames followed by a terminal `git:clone:done` on the event bus,
    /// correlated by `requestId` (PROTOCOL §5.6 / §6.5). Payloads never carry
    /// the source URL or credentials. `-32602` on missing/invalid params;
    /// `-32603` when the daemon cannot spawn `git`.
    fn git_clone(
        &self,
        url: String,
        parent_dir: String,
        target_name: Option<String>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (url, parent_dir, target_name, request_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::git_clone not implemented".to_string(),
            ))
        })
    }

    /// `repo.list`: the persistent known-repository registry, most-recently-used
    /// first, as `{ repos: KnownRepo[] }` (PROTOCOL §5.6). Populates the iOS
    /// Create-Workspace picker; the first invocation also lazily syncs repos from
    /// existing workspaces (never blocking/failing the response).
    fn repo_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::repo_list not implemented".to_string(),
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
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            merge_method,
            commit_title,
            commit_message,
            idempotency_key,
        );
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

    // ------------------------------------------------------------------------
    // `github.*` explicit-addressing surface (PROTOCOL §5.27). Unlike `pr.*`
    // (workspace/active-PR scoped), every data method takes `(owner, repo[,
    // number])` directly. Backed by the same `intent-sourcecontrol` engine.
    // ------------------------------------------------------------------------

    /// `github.pulls.create`: open a PR with `head` sent **verbatim** (no
    /// `owner:branch` login prefix) — `{ pull }` (PROTOCOL §5.27).
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_create(
        &self,
        owner: String,
        repo: String,
        title: String,
        body: String,
        head: String,
        base: String,
        draft: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, title, body, head, base, draft);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_create not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.get`: `GET /repos/{owner}/{repo}/pulls/{number}` → `{ pull }`.
    fn github_pulls_get(
        &self,
        owner: String,
        repo: String,
        number: u64,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_get not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.list`: `GET /repos/{owner}/{repo}/pulls` → `{ pulls, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        head: Option<String>,
        base: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, state, head, base, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_list not implemented".to_string(),
            ))
        })
    }

    // ========================================================================
    // github.* browse / auth / identity (PROTOCOL §5.27)
    //
    // Repo-addressed GitHub operations backed by the `SourceControl` engine,
    // distinct from the workspace/active-PR `pr.*` surface. The PAT comes from
    // the environment and is NEVER logged, echoed, or returned over the wire —
    // only derived identity / connection state crosses it.
    // ========================================================================

    /// `github.repos.list`: the authenticated user's repositories
    /// (`GET /user/repos`) → `{ repos: GithubRepo[], nextToken }`.
    fn github_repos_list(
        &self,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_list not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.search`: `GET /search/issues` (`is:pr` + `@me`
    /// involvement) → `{ pulls, nextToken }`.
    fn github_pulls_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, filter, state, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_search not implemented".to_string(),
            ))
        })
    }

    /// `github.repos.search`: search repositories (`GET /search/repositories`)
    /// → `{ repos: GithubRepo[], nextToken }`.
    fn github_repos_search(
        &self,
        query: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_search not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.merge`: `PUT /repos/{owner}/{repo}/pulls/{number}/merge`
    /// → `{ merged, message, sha? }`.
    #[allow(clippy::too_many_arguments)]
    fn github_pulls_merge(
        &self,
        owner: String,
        repo: String,
        number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            owner,
            repo,
            number,
            merge_method,
            commit_title,
            commit_message,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_merge not implemented".to_string(),
            ))
        })
    }

    /// `github.repos.get`: a single repository's metadata
    /// (`GET /repos/{owner}/{repo}`) → `{ repo: GithubRepo | null }`.
    fn github_repos_get(
        &self,
        owner: String,
        repo: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_repos_get not implemented".to_string(),
            ))
        })
    }

    /// `github.pulls.updateBranch`:
    /// `PUT /repos/{owner}/{repo}/pulls/{number}/update-branch` → `{ message, url? }`.
    fn github_pulls_update_branch(
        &self,
        owner: String,
        repo: String,
        number: u64,
        expected_head_sha: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, expected_head_sha);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_pulls_update_branch not implemented".to_string(),
            ))
        })
    }

    /// `github.issues.list`: `GET /repos/{owner}/{repo}/issues` (PRs filtered
    /// out) → `{ issues, nextToken }`.
    #[allow(clippy::too_many_arguments)]
    fn github_issues_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        labels: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, state, labels, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_issues_list not implemented".to_string(),
            ))
        })
    }

    /// `github.branches.list`: a repository's remote branch names
    /// (`GET /repos/{owner}/{repo}/branches`) → `{ branches: string[], nextToken }`.
    fn github_branches_list(
        &self,
        owner: String,
        repo: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_branches_list not implemented".to_string(),
            ))
        })
    }

    /// `github.issues.search`: `GET /search/issues` (`is:issue`) →
    /// `{ issues, nextToken }`.
    fn github_issues_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, filter, state, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_issues_search not implemented".to_string(),
            ))
        })
    }

    /// `github.authStatus`: validate the resolved env PAT via `GET /user` and
    /// report connection state. Never returns the token.
    fn github_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `github.listReviewComments`:
    /// `GET /repos/{owner}/{repo}/pulls/{number}/comments` → `{ comments, nextToken }`.
    fn github_list_review_comments(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_list_review_comments not implemented".to_string(),
            ))
        })
    }

    /// `github.connect`: no-op / guidance in the PAT-from-env model (no OAuth).
    fn github_connect(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_connect not implemented".to_string(),
            ))
        })
    }

    /// `github.replyReviewComment`: reply to a review comment
    /// (`inReplyToId = commentId`) → `{ comment }`.
    fn github_reply_review_comment(
        &self,
        owner: String,
        repo: String,
        number: u64,
        comment_id: u64,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, comment_id, body);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_reply_review_comment not implemented".to_string(),
            ))
        })
    }

    /// `github.revoke`: no-op / guidance; the token is environment-owned.
    fn github_revoke(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_revoke not implemented".to_string(),
            ))
        })
    }

    /// `github.getReviewThreads`: GraphQL `pullRequest.reviewThreads` →
    /// `{ threads, nextToken }`.
    fn github_get_review_threads(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (owner, repo, number, limit, next_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_get_review_threads not implemented".to_string(),
            ))
        })
    }

    /// `github.resolveThread`: GraphQL `resolveReviewThread` → `{ isResolved: true }`.
    fn github_resolve_thread(&self, thread_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = thread_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_resolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `github.unresolveThread`: GraphQL `unresolveReviewThread` →
    /// `{ isResolved: false }`.
    fn github_unresolve_thread(
        &self,
        thread_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = thread_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_unresolve_thread not implemented".to_string(),
            ))
        })
    }

    /// `github.getUser`: GitHub-derived identity (`GET /user`) →
    /// `{ user: GithubUser | null }`. Never includes the PAT.
    fn github_get_user(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::github_get_user not implemented".to_string(),
            ))
        })
    }

    /// `linear.authStatus`: validate the resolved Linear API key via the GraphQL
    /// `viewer` probe and report `{ authenticated, login?, scopes }`. The key is
    /// never returned. A missing/invalid key surfaces as `Internal` (PROTOCOL
    /// §5.28).
    fn linear_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `linear.listIssues`: the viewer's issues for the typed `filter`
    /// (`assigned`|`created`|`subscribed`|`team`|`all`, default `assigned`),
    /// returned as a bare `LinearIssueResult[]` (PROTOCOL §5.28).
    fn linear_list_issues(
        &self,
        filter: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (filter, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_issues not implemented".to_string(),
            ))
        })
    }

    /// `linear.searchIssues`: full-text issue search by `query`, returned as a
    /// bare `LinearIssueResult[]` (PROTOCOL §5.28).
    fn linear_search_issues(
        &self,
        query: String,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_search_issues not implemented".to_string(),
            ))
        })
    }

    /// `linear.getIssue`: a single flattened `LinearIssueResult` looked up by
    /// UUID `id` or `ENG-123` `identifier` (PROTOCOL §5.28).
    fn linear_get_issue(
        &self,
        id_or_identifier: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id_or_identifier;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_get_issue not implemented".to_string(),
            ))
        })
    }

    /// `linear.viewer`: the authenticated user as a bare `LinearUser`
    /// (PROTOCOL §5.28).
    fn linear_viewer(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_viewer not implemented".to_string(),
            ))
        })
    }

    /// `linear.listTeams`: teams as a bare `LinearTeam[]` (PROTOCOL §5.28).
    fn linear_list_teams(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_teams not implemented".to_string(),
            ))
        })
    }

    /// `linear.listWorkflowStates`: workflow states as a bare
    /// `LinearWorkflowState[]` (PROTOCOL §5.28).
    fn linear_list_workflow_states(
        &self,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_workflow_states not implemented".to_string(),
            ))
        })
    }

    /// `linear.listProjects`: projects as a bare `LinearProject[]`
    /// (PROTOCOL §5.28).
    fn linear_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_projects not implemented".to_string(),
            ))
        })
    }

    /// `linear.listLabels`: issue labels as a bare `LinearLabel[]`
    /// (PROTOCOL §5.28).
    fn linear_list_labels(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_list_labels not implemented".to_string(),
            ))
        })
    }

    /// `linear.createIssue`: create a Linear issue (`issueCreate` mutation).
    /// `request` is the bare `CreateIssueRequest` JSON: `title` and `teamId`
    /// are required; everything else is optional. Returns the flattened
    /// issue (`LinearIssueResult`); the API key never crosses the wire
    /// (PROTOCOL §5.28).
    fn linear_create_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_create_issue not implemented".to_string(),
            ))
        })
    }

    /// `linear.updateIssue`: update a Linear issue (`issueUpdate` mutation).
    /// `request` is the bare `UpdateIssueRequest` JSON: `issueId` is required;
    /// only the fields present are sent through `IssueUpdateInput`. Returns
    /// the flattened issue (`LinearIssueResult`); the API key never crosses
    /// the wire (PROTOCOL §5.28).
    fn linear_update_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = request;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::linear_update_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.authStatus`: probe the resolved Sentry credentials via
    /// `GET /organizations/{org}/` and report `{ authenticated, organization?,
    /// error? }`. The token is never returned. A missing pair surfaces as
    /// `Internal` (PROTOCOL §5.29).
    fn sentry_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_auth_status not implemented".to_string(),
            ))
        })
    }

    /// `sentry.listIssues`: issues matching the typed `status` filter
    /// (`unresolved`|`resolved`|`ignored`|`all`, default `unresolved`),
    /// optional `project` slug, optional free-text `query`, returned as a bare
    /// `SentryIssueResult[]` (PROTOCOL §5.29).
    fn sentry_list_issues(
        &self,
        project: Option<String>,
        status: Option<String>,
        query: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (project, status, query, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_list_issues not implemented".to_string(),
            ))
        })
    }

    /// `sentry.searchIssues`: full-text issue search by `query`, optional
    /// `project` slug, returned as a bare `SentryIssueResult[]` (PROTOCOL §5.29).
    fn sentry_search_issues(
        &self,
        query: String,
        project: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (query, project, limit);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_search_issues not implemented".to_string(),
            ))
        })
    }

    /// `sentry.listProjects`: projects for the configured organization as a
    /// bare `SentryProject[]` (PROTOCOL §5.29).
    fn sentry_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = limit;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_list_projects not implemented".to_string(),
            ))
        })
    }

    /// `sentry.getIssue`: a single flattened `SentryIssueResult` looked up by
    /// numeric/UUID id or shortId (e.g. `WEB-1`). The router enforces that at
    /// least one of `id`/`shortId` is supplied (PROTOCOL §5.29).
    fn sentry_get_issue(&self, id_or_short_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id_or_short_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_get_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.resolveIssue`: mutate the issue's status to `resolved` and
    /// return the updated flattened issue (PROTOCOL §5.29).
    fn sentry_resolve_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_resolve_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.ignoreIssue`: mutate the issue's status to `ignored` and return
    /// the updated flattened issue (PROTOCOL §5.29).
    fn sentry_ignore_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_ignore_issue not implemented".to_string(),
            ))
        })
    }

    /// `sentry.assignIssue`: assign the issue to `assignedTo` (an explicit
    /// `null`/absent unassigns) and return the updated flattened issue
    /// (PROTOCOL §5.29).
    fn sentry_assign_issue(
        &self,
        id: String,
        assigned_to: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, assigned_to);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::sentry_assign_issue not implemented".to_string(),
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
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, limit, page_token);
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
    // settings.* — BE-owned settings namespace (PROTOCOL §5.12, §9.8). Global
    // (no `workspaceId`); sensitive values are redacted in list/get and never
    // cross the wire in plaintext.
    // ------------------------------------------------------------------------

    /// `settings.list`: every setting definition with its (redacted) current
    /// value — `{ settings: SettingDefinitionWithValue[] }` (PROTOCOL §5.12).
    fn settings_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_list not implemented".to_string(),
            ))
        })
    }

    /// `settings.get`: one setting as `{ path, value, definition }`; the value is
    /// redacted when sensitive; unknown path → `-32602` (PROTOCOL §5.12).
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_get not implemented".to_string(),
            ))
        })
    }

    /// `settings.update`: atomic batch apply of `changes: [{ path, value }]`
    /// (unknown path / read-only / failed validation → `-32602`, nothing
    /// applied); returns `{ applied: [{ path, value }] }` redacted and emits
    /// `settings:changed` on success (PROTOCOL §5.12).
    fn settings_update(
        &self,
        changes: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = changes;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_update not implemented".to_string(),
            ))
        })
    }

    /// `settings.reset`: restore a setting's default, returning the redacted
    /// `{ path, value }` and emitting `settings:changed`; unknown path → `-32602`
    /// (PROTOCOL §5.12).
    fn settings_reset(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::settings_reset not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // rules.* — user-rule overrides + (internal) prompt-injection (§18.1,
    // PROTOCOL §5.21). `list`/`get` are reads; `update` upserts the user
    // override. Only user-override entries are editable; file-sourced entries
    // are read-only over the wire. The injection pipeline that assembles these
    // into an agent's prompt is internal (§6.8) — not a method here.
    // ------------------------------------------------------------------------

    /// `rules.list`: all rule sources as `{ rules: RuleSet }` — every
    /// user-override type with content plus, when `workspace_id` is given, the
    /// live workspace rule files (read-only) (PROTOCOL §5.21).
    fn rules_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_list not implemented".to_string(),
            ))
        })
    }

    /// `rules.get`: `{ enabled, content, updatedAt }` for one user-override type
    /// (PROTOCOL §5.21).
    fn rules_get(
        &self,
        workspace_id: WorkspaceId,
        rule_type: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, rule_type);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_get not implemented".to_string(),
            ))
        })
    }

    /// `rules.update`: upsert a user-override body (+ optional `enabled`),
    /// re-read the set, and emit `settings:changed`; returns `{ rules: RuleSet }`
    /// (PROTOCOL §5.21).
    fn rules_update(
        &self,
        workspace_id: WorkspaceId,
        rule_type: String,
        content: String,
        enabled: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, rule_type, content, enabled);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::rules_update not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // specialist.* — file-backed specialist definitions (PROTOCOL §5.11,
    // §18.2). Global (no `workspaceId`); resolved 3-tier project > user >
    // bundled. `list`/`get` read the resolved view; `create`/`edit`/`delete`
    // write user/project markdown-with-frontmatter files (`bundled` is
    // read-only). Nothing is persisted in SQLite.
    // ------------------------------------------------------------------------

    /// `specialist.list` → `{ specialists: SpecialistDef[] }` (user/project
    /// files override bundled). An optional `workspace_path` adds the project
    /// tier (PROTOCOL §5.11).
    fn specialist_list(
        &self,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_path;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_list not implemented".to_string(),
            ))
        })
    }

    /// `specialist.get` → `{ specialist: SpecialistDef }`, the resolved view;
    /// unknown id → `-32602` (PROTOCOL §5.11).
    fn specialist_get(
        &self,
        id: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_get not implemented".to_string(),
            ))
        })
    }

    /// `specialist.create` → write a new user/project file (default scope
    /// `user`); returns `{ specialist: SpecialistDef }` (PROTOCOL §5.11).
    fn specialist_create(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: Option<String>,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, spec, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_create not implemented".to_string(),
            ))
        })
    }

    /// `specialist.edit` → overwrite an existing user/project file; returns
    /// `{ specialist: SpecialistDef }`; missing file → `-32602` (PROTOCOL §5.11).
    fn specialist_edit(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, spec, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_edit not implemented".to_string(),
            ))
        })
    }

    /// `specialist.delete` → remove a user/project file; returns
    /// `{ success: true }`; missing/bundled id → `-32602` (PROTOCOL §5.11).
    fn specialist_delete(
        &self,
        id: String,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (id, scope, workspace_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::specialist_delete not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // mcp.servers.* — external MCP-server lifecycle/config (PROTOCOL §5.22,
    // §18.3). Config lives in the **sensitive** `mcp.servers` setting (`env`/
    // `headers` redacted over the wire); runtime status is not persisted and
    // transitions push `mcp.servers:status-changed`. Distinct from the §6.8
    // agent→BE callback.
    // ------------------------------------------------------------------------

    /// `mcp.servers.list` → `{ servers: McpServerConfig[] }` — configured
    /// external servers with sensitive `env`/`headers` redacted (PROTOCOL §5.22).
    fn mcp_servers_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_list not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.create` → add a server definition; returns
    /// `{ server: McpServerConfig }` (redacted) (PROTOCOL §5.22).
    fn mcp_servers_create(
        &self,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = config;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_create not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.update` → edit an existing definition; returns
    /// `{ server: McpServerConfig }` (redacted) (PROTOCOL §5.22).
    fn mcp_servers_update(
        &self,
        server_id: String,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, config);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_update not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.delete` → remove a definition (stopping it first); returns
    /// `{ success: true }` (PROTOCOL §5.22).
    fn mcp_servers_delete(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_delete not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.toggle` → enable (start) / disable (stop) a server; returns
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_toggle(
        &self,
        server_id: String,
        enabled: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (server_id, enabled);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_toggle not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.restart` → stop-then-start a server; returns
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_restart(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_restart not implemented".to_string(),
            ))
        })
    }

    /// `mcp.servers.getStatus` → point read of one server's live status as
    /// `{ status: McpServerStatus }` (PROTOCOL §5.22).
    fn mcp_servers_get_status(
        &self,
        server_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = server_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::mcp_servers_get_status not implemented".to_string(),
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

    /// `search.memories`: substring search over the BE memories store (§9.2).
    /// Returns `{ requestId, matches: MemoryMatch[] }`; an empty store yields an
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
    /// `terminal:data` events. `env` is an optional overlay layered over the
    /// daemon's inherited environment so callers can pass through per-terminal
    /// variables (e.g. `FORCE_COLOR`, `PATH` additions). Returns
    /// `{ terminalId }` (PROTOCOL §5.13).
    fn terminal_create(
        &self,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, cols, rows, cwd, command, env);
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

    /// `terminal.list`: the workspace's live terminals as a bare array
    /// `[{ id, name, cwd, isExecutingCommand }]` (PROTOCOL §5.9).
    fn terminal_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_list not implemented".to_string(),
            ))
        })
    }

    /// `terminal.readOutput`: a formatted, ANSI-stripped string view of a
    /// terminal's scrollback, keeping the trailing `max_lines` (default 200)
    /// (PROTOCOL §5.13).
    fn terminal_read_output(
        &self,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, terminal_id, max_lines, paginate, page_token);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::terminal_read_output not implemented".to_string(),
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
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (script_id, max_lines, paginate, page_token);
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

    // ------------------------------------------------------------------------
    // file.* — workspace-scoped filesystem access (PROTOCOL §5.10). Every path
    // is validated within the resolved workspace root; access-denied and other
    // filesystem failures surface as `Error::Internal` (→ `-32603`), matching
    // the TS handler which wraps the builder errors in `INTERNAL_ERROR`.
    // ------------------------------------------------------------------------

    /// `file.read`: the file's UTF-8 contents as a **bare JSON string** (not an
    /// object), per the TS `ws.file.read` builder (PROTOCOL §5.10).
    fn file_read(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_read not implemented".to_string(),
            ))
        })
    }

    /// `file.write`: create/overwrite a file (parent dirs created); returns
    /// `{ ok: true, path, size }` where `size` is the content byte/char length
    /// (PROTOCOL §5.10).
    fn file_write(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        content: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path, content);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_write not implemented".to_string(),
            ))
        })
    }

    /// `file.list`: directory entries as a **bare array** of
    /// `{ name, type }` (`type` = `"file"`/`"directory"`); `path` defaults to
    /// `"."` (PROTOCOL §5.10).
    fn file_list(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_list not implemented".to_string(),
            ))
        })
    }

    /// `file.delete`: remove a single file (rejects directories); returns
    /// `{ ok: true, path, deleted: true }` (PROTOCOL §5.10).
    fn file_delete(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_delete not implemented".to_string(),
            ))
        })
    }

    /// `file.mkdir`: create a directory (recursive); returns
    /// `{ ok: true, path, created: true }`, or `{ ok: true, path, existed: true }`
    /// when the directory already exists (PROTOCOL §5.10).
    fn file_mkdir(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_mkdir not implemented".to_string(),
            ))
        })
    }

    /// `file.rename`: move a file/directory (destination must not exist);
    /// returns `{ ok: true, oldPath, newPath, renamed: true, isDirectory }`
    /// (PROTOCOL §5.10).
    fn file_rename(
        &self,
        workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, old_path, new_path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_rename not implemented".to_string(),
            ))
        })
    }

    /// `file.tree`: directory entries directly under `path` (defaulting to the
    /// workspace root) as a **bare array** of `{ path, name, isDirectory }`
    /// (camelCase on the wire). The FE anchors the explorer on the root and
    /// lazy-lists children via `file.list`, so a shallow listing is sufficient.
    /// Shares the same within-workspace guard as the other file ops.
    fn file_tree(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, path);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::file_tree not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // primitive.* — append a fenced ```ws-block:<type>``` JSON primitive to a
    // note. Each returns `{ ok: true, primitiveId, noteId, content }` where
    // `content` is the full note text after the append, matching the TS
    // `appendPrimitiveBlock` builder (PROTOCOL §5.x). Every primitive carries
    // `version: 1` and `createdBy: "agent"`.
    // ------------------------------------------------------------------------

    /// `primitive.addReference`: append a `reference` primitive (`target.kind`
    /// = `symbol` for `#symbol:` semantic ids, else `file_range`; optional code
    /// `snapshot`).
    fn primitive_add_reference(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        semantic_id: String,
        description: String,
        snapshot: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, semantic_id, description, snapshot);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_reference not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addCli`: append a `cli` primitive (`cwd` defaults to `"./"`,
    /// `display.showCommandPrefix` = `"$"`).
    fn primitive_add_cli(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        command: String,
        description: String,
        working_directory: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (
            workspace_id,
            note_id,
            command,
            description,
            working_directory,
        );
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_cli not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addPatch`: append a `patch` primitive with a single-entry
    /// `patches: [{ filePath, diff }]` array.
    fn primitive_add_patch(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        file_path: String,
        diff: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, file_path, diff, description);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_patch not implemented".to_string(),
            ))
        })
    }

    /// `primitive.addAgentAction`: append an `agent_action` primitive with empty
    /// `inputs`.
    fn primitive_add_agent_action(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
        goal: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, note_id, agent_id, goal, description);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::primitive_add_agent_action not implemented".to_string(),
            ))
        })
    }

    // ------------------------------------------------------------------------
    // crossWorkspace.* — read-only access to sibling workspaces that share the
    // caller's `repositoryPath`. Sibling-scope and access-control failures
    // mirror the TS builder messages ("Current workspace is not associated with
    // a repository", "Access denied: ...", "Note not found: ...") and surface
    // as `Error::Internal` (→ `-32603`), matching the TS handler which wraps the
    // builder errors in `INTERNAL_ERROR` (PROTOCOL §5.x).
    // ------------------------------------------------------------------------

    /// `crossWorkspace.listSiblings`: workspaces sharing the caller's
    /// `repositoryPath`, excluding self. Bare array of
    /// `{ id, title, branch, status, createdAt, updatedAt }` (`title` defaults
    /// to `"Untitled"`; `status` is the PascalCase `WorkspaceStatus`).
    fn cross_workspace_list_siblings(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_list_siblings not implemented".to_string(),
            ))
        })
    }

    /// `crossWorkspace.listNotes`: notes in a sibling workspace as a bare array
    /// of `{ id, title, createdAt, updatedAt }`.
    fn cross_workspace_list_notes(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, target_workspace_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_list_notes not implemented".to_string(),
            ))
        })
    }

    /// `crossWorkspace.readNote`: one note in a sibling workspace as
    /// `{ id, title, content, numberedContent, sourceWorkspaceId,
    /// sourceWorkspaceTitle, branch, lineCount }`.
    fn cross_workspace_read_note(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let _ = (workspace_id, target_workspace_id, note_id);
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::cross_workspace_read_note not implemented".to_string(),
            ))
        })
    }
}

/// Whether a context engine is usable right now (§8.1). `Unavailable` is a
/// first-class, non-error state — never something that fails a request (§8.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineAvailability {
    /// A working engine is present; `version` is best-effort.
    Available {
        name: String,
        version: Option<String>,
    },
    /// No usable engine (not installed, not logged in, …) — not an error.
    Unavailable { reason: String },
}

/// Natural-language code/context retrieval request scoped to a workspace (§8.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveRequest {
    pub workspace_id: WorkspaceId,
    pub workspace_path: PathBuf,
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,
}

/// A single retrieved code/context hit (§8.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievedItem {
    /// Workspace-relative file path.
    pub file: String,
    /// Detected symbol/identifier, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// 1-based line, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    /// Snippet/preview of the matched content.
    pub preview: String,
    /// Relevance score, when provided by the engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// The result of a [`ContextEngine::retrieve`] call (§8.1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveResult {
    pub items: Vec<RetrievedItem>,
}

/// Errors raised by a [`ContextEngine`] (§8.1, §11.1). Construction never fails
/// the daemon and `availability()` never errors; these surface only from
/// `retrieve()`.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    /// No usable engine for this request (maps to `ContextUnavailable`, §11.1).
    #[error("context engine unavailable: {reason}")]
    Unavailable { reason: String },
    /// Spawning the engine process failed.
    #[error("failed to spawn context engine: {0}")]
    Spawn(String),
    /// The engine did not return within the timeout.
    #[error("context engine timed out")]
    Timeout,
    /// The engine ran but exited unsuccessfully.
    #[error("context engine failed: {0}")]
    CommandFailed(String),
    /// The engine's output could not be parsed.
    #[error("failed to parse context engine output: {0}")]
    Parse(String),
}

/// Context-engine abstraction implemented by `intent-context` (§3.1, §8.1).
///
/// Code retrieval is an **optional** capability: the daemon degrades gracefully
/// when no engine is available (§8.3). `availability()` is a non-error probe;
/// only `retrieve()` can fail with [`ContextError`].
#[async_trait::async_trait]
pub trait ContextEngine: Send + Sync {
    /// Is a working engine available right now?
    async fn availability(&self) -> EngineAvailability;

    /// Natural-language code/context retrieval scoped to a workspace.
    async fn retrieve(
        &self,
        req: RetrieveRequest,
    ) -> std::result::Result<RetrieveResult, ContextError>;
}
