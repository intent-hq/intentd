//! Tool-call dispatch: maps a workspace MCP tool name + JSON arguments to the
//! matching `WorkspaceApi` method, then serializes the typed result back to JSON
//! (the "two front doors" rule — this reuses the FE's service logic, §6.8).

use intent_core::model::AgentDelegateInput;
use intent_core::{
    Error, NoteAddInput, NoteCreate, NoteEditInput, NoteEditLinesInput, NoteId,
    NoteUpdateMetadataResult, Result,
};
use serde::Serialize;
use serde_json::Value;

use super::WorkspaceMcpServer;

fn req_str(args: &Value, key: &str) -> Result<String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::InvalidParams(format!("missing required parameter: {key}")))
}

fn opt_str(args: &Value, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

fn req_i64(args: &Value, key: &str) -> Result<i64> {
    args.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::InvalidParams(format!("missing required parameter: {key}")))
}

fn opt_bool(args: &Value, key: &str) -> Option<bool> {
    args.get(key).and_then(Value::as_bool)
}

fn opt_i64(args: &Value, key: &str) -> Option<i64> {
    args.get(key).and_then(Value::as_i64)
}

fn opt_vec_str(args: &Value, key: &str) -> Option<Vec<String>> {
    args.get(key).and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    })
}

fn note_id(args: &Value, key: &str) -> Result<NoteId> {
    Ok(NoteId::from_string(req_str(args, key)?))
}

fn val<T: Serialize>(result: Result<T>) -> Result<Value> {
    result.and_then(|v| serde_json::to_value(v).map_err(|e| Error::Internal(e.to_string())))
}

impl WorkspaceMcpServer {
    /// Dispatch a validated tool call to the shared `WorkspaceApi`.
    pub(super) async fn dispatch(&self, name: &str, args: &Value) -> Result<Value> {
        let ws = self.workspace_id.clone();
        let api = &self.api;
        match name {
            // ---- Read tools ----
            "list_notes_workspace-mcp" => val(api.list_notes(&ws).await),
            "get_note_workspace-mcp" => val(api.get_note(ws, note_id(args, "noteId")?).await),
            "list_note_tasks_workspace-mcp" => {
                val(api.list_note_tasks(ws, note_id(args, "noteId")?).await)
            }
            // ---- Note write tools ----
            "create_note_workspace-mcp" => val(api
                .create_note(
                    ws,
                    NoteCreate {
                        title: req_str(args, "title")?,
                        content: opt_str(args, "content"),
                        tags: opt_vec_str(args, "tags"),
                        parent_id: None,
                    },
                    opt_str(args, "idempotencyKey"),
                )
                .await),
            "add_to_note_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .add_to_note(
                        ws,
                        id,
                        NoteAddInput {
                            content: req_str(args, "content")?,
                            heading: opt_str(args, "heading"),
                            position: opt_str(args, "position"),
                        },
                    )
                    .await)
            }
            "set_note_content_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .set_note_content(
                        ws,
                        id,
                        req_str(args, "content")?,
                        opt_bool(args, "confirmReplacement").unwrap_or(false),
                        opt_i64(args, "expectedVersion"),
                    )
                    .await)
            }
            "edit_note_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .edit_note(
                        ws,
                        id,
                        NoteEditInput {
                            old: req_str(args, "old")?,
                            new: req_str(args, "new")?,
                        },
                    )
                    .await)
            }
            "edit_note_lines_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .edit_note_lines(
                        ws,
                        id,
                        NoteEditLinesInput {
                            start: req_i64(args, "start")?,
                            end: req_i64(args, "end")?,
                            content: req_str(args, "content")?,
                        },
                    )
                    .await)
            }
            "update_note_metadata_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                let r: Result<NoteUpdateMetadataResult> = api
                    .update_note_metadata(ws, id, opt_str(args, "title"), opt_vec_str(args, "tags"))
                    .await;
                val(r)
            }
            "delete_note_workspace-mcp" => val(api.delete_note(ws, note_id(args, "noteId")?).await),
            other => self.dispatch_more(other, args).await,
        }
    }

    /// Task, comment, and agent-creation tools (split to keep each match small).
    async fn dispatch_more(&self, name: &str, args: &Value) -> Result<Value> {
        let ws = self.workspace_id.clone();
        let api = &self.api;
        match name {
            // ---- Task write tools ----
            "update_task_status_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .task_update_status(
                        ws,
                        id,
                        req_str(args, "taskText")?,
                        req_str(args, "status")?,
                    )
                    .await)
            }
            "update_note_task_status_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .task_update_note_status(
                        ws,
                        id,
                        req_str(args, "status")?,
                        opt_i64(args, "expectedVersion"),
                    )
                    .await)
            }
            "update_task_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .task_update(
                        ws,
                        id,
                        req_i64(args, "line")?,
                        opt_str(args, "text"),
                        opt_str(args, "status"),
                        opt_str(args, "expected"),
                    )
                    .await)
            }
            "mark_as_task_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .mark_as_task(
                        ws,
                        id,
                        req_str(args, "status")?,
                        opt_vec_str(args, "acceptanceCriteria").unwrap_or_default(),
                        opt_str(args, "effort"),
                    )
                    .await)
            }
            "convert_task_blocks_workspace-mcp" => {
                val(api.convert_task_blocks(ws, note_id(args, "noteId")?).await)
            }
            "create_prerequisite_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .create_prerequisite(
                        ws,
                        id,
                        req_str(args, "title")?,
                        opt_str(args, "content"),
                        opt_str(args, "status"),
                    )
                    .await)
            }
            "assign_agent_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api.assign_agent(ws, id, req_str(args, "agentId")?).await)
            }
            // ---- Comment write tools ----
            "add_note_comment_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .comment_add(
                        ws,
                        id,
                        req_str(args, "searchContext")?,
                        req_str(args, "commentTarget")?,
                        req_str(args, "comment")?,
                        opt_str(args, "type"),
                        opt_str(args, "author"),
                    )
                    .await)
            }
            "respond_to_comment_thread_workspace-mcp" => {
                let id = note_id(args, "noteId")?;
                val(api
                    .comment_respond(
                        ws,
                        id,
                        opt_str(args, "threadId"),
                        opt_str(args, "commentId"),
                        req_str(args, "comment")?,
                        opt_str(args, "type"),
                        opt_str(args, "author"),
                        opt_str(args, "suggestionOriginal"),
                        opt_str(args, "suggestionProposed"),
                    )
                    .await)
            }
            // ---- Agent creation tools ----
            "delegate_task_workspace-mcp" => val(api
                .agent_delegate(
                    ws,
                    AgentDelegateInput {
                        task_note_id: opt_str(args, "taskNoteId").map(NoteId::from_string),
                        note_id: opt_str(args, "noteId").map(NoteId::from_string),
                        task_text: opt_str(args, "taskText"),
                        agent_instructions: opt_str(args, "agentInstructions"),
                        specialist: opt_str(args, "specialist"),
                        model: opt_str(args, "model"),
                        behavior_prompt: opt_str(args, "behaviorPrompt"),
                        wait_mode: opt_str(args, "waitMode"),
                        skip_auto_commit: opt_bool(args, "skipAutoCommit"),
                    },
                    self.caller_agent_id.clone(),
                )
                .await),
            "report_to_parent_workspace-mcp" => {
                let report = args.get("report").cloned().ok_or_else(|| {
                    Error::InvalidParams("missing required parameter: report".to_string())
                })?;
                val(api
                    .agent_report_to_parent(ws, report, self.caller_agent_id.clone())
                    .await)
            }
            other => Err(Error::InvalidParams(format!("Tool not found: {other}"))),
        }
    }
}
