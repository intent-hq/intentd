//! Tool-call dispatch: maps a workspace MCP tool name + JSON arguments to the
//! matching `WorkspaceApi` method, then serializes the typed result back to JSON
//! (the "two front doors" rule — this reuses the FE's service logic, §6.8).

use intent_core::model::AgentDelegateInput;
use intent_core::{
    AgentId, Error, NoteAddInput, NoteCreate, NoteEditInput, NoteEditLinesInput, NoteId,
    NoteUpdateMetadataResult, Result, WorkspaceUpdate, MAX_DELEGATION_DEPTH,
    WORKSPACE_STATUS_MESSAGE_MAX_LENGTH,
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
            // ---- Workspace metadata tools ----
            "get_workspace_details_workspace-mcp" => {
                let ws_row = api.get_workspace(ws).await?;
                let title = ws_row.title.trim();
                let has_title = !title.is_empty() && title != ws_row.id.as_str();
                let display_title = if title.is_empty() {
                    "(untitled)".to_string()
                } else {
                    title.to_string()
                };
                val::<serde_json::Value>(Ok(serde_json::json!({
                    "id": ws_row.id,
                    "title": display_title,
                    "hasTitle": has_title,
                    "status": ws_row.status,
                    "statusMessage": ws_row.status_message,
                    "branch": ws_row.branch,
                    "repositoryName": ws_row.repository_name,
                    "tags": ws_row.tags,
                })))
            }
            "set_workspace_title_workspace-mcp" => {
                // Skip-if-custom-titled: mirrors `ws-workspace-api.ts` — a
                // workspace whose stored `title` is non-empty and different
                // from its id already carries a human title, so the initial
                // agent's rename call is a no-op. Branch renaming when the
                // branch is still auto-generated is deferred: the daemon does
                // not yet own an equivalent branch-rename path (no
                // `intent_git::rename_branch`), so this tool is title-only.
                let title = req_str(args, "title")?;
                let trimmed = title.trim().to_string();
                if trimmed.is_empty() {
                    return Err(Error::InvalidParams("title must not be empty".to_string()));
                }
                let existing = api.get_workspace(ws.clone()).await?;
                let existing_title = existing.title.trim();
                if !existing_title.is_empty() && existing_title != existing.id.as_str() {
                    return val::<serde_json::Value>(Ok(serde_json::json!({
                        "ok": true,
                        "skipped": true,
                        "title": existing_title,
                        "branch": existing.branch,
                    })));
                }
                let update = WorkspaceUpdate {
                    title: Some(trimmed.clone()),
                    ..Default::default()
                };
                let updated = api.update_workspace(ws, update).await?;
                val::<serde_json::Value>(Ok(serde_json::json!({
                    "ok": true,
                    "title": updated.title,
                    "branch": updated.branch,
                })))
            }
            "set_workspace_status_message_workspace-mcp" => {
                // Optional `statusMessage`: empty string / whitespace clears
                // it, matching the reference `setStatusMessage(null)` clear
                // semantics. Over-length input surfaces InvalidParams per the
                // FE's `WORKSPACE_STATUS_MESSAGE_MAX_LENGTH` guard.
                let raw = opt_str(args, "statusMessage").unwrap_or_default();
                let trimmed = raw.trim();
                if trimmed.len() > WORKSPACE_STATUS_MESSAGE_MAX_LENGTH {
                    return Err(Error::InvalidParams(format!(
                        "statusMessage must be {WORKSPACE_STATUS_MESSAGE_MAX_LENGTH} characters or fewer"
                    )));
                }
                let update = WorkspaceUpdate {
                    status_message: Some(trimmed.to_string()),
                    ..Default::default()
                };
                let updated = api.update_workspace(ws, update).await?;
                let out = if trimmed.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(updated.status_message.unwrap_or_default())
                };
                val::<serde_json::Value>(Ok(serde_json::json!({
                    "ok": true,
                    "statusMessage": out,
                })))
            }
            // ---- Note write tools ----
            // `note.create` is an idempotent method (TB-0 §5): the daemon is the
            // caller here, so mint a fresh key when the tool arguments carry
            // none rather than tripping the services-layer soft-launch warn.
            "create_note_workspace-mcp" => val(api
                .create_note(
                    ws,
                    NoteCreate {
                        title: req_str(args, "title")?,
                        content: opt_str(args, "content"),
                        tags: opt_vec_str(args, "tags"),
                        parent_id: None,
                    },
                    opt_str(args, "idempotencyKey")
                        .or_else(|| Some(uuid::Uuid::new_v4().to_string())),
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
                    .update_note_metadata(
                        ws,
                        id,
                        opt_str(args, "title"),
                        opt_vec_str(args, "tags"),
                        None,
                    )
                    .await;
                val(r)
            }
            "delete_note_workspace-mcp" => {
                val(api.delete_note(ws, note_id(args, "noteId")?, None).await)
            }
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
            // No idempotency key here: `agent.delegate` reaches the create op
            // directly inside services (an explicit internal caller), so it
            // never crosses the `agent.create` soft-launch warn.
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
            "send_message_to_agent_workspace-mcp" => {
                let agent_id = AgentId::from(req_str(args, "agentId")?.as_str());
                val(api
                    .agent_send_message(
                        ws,
                        agent_id,
                        req_str(args, "message")?,
                        None,
                        None,
                        None,
                        opt_str(args, "priority"),
                        None,
                        None,
                        None,
                        None,
                    )
                    .await)
            }
            "send_message_to_task_agent_workspace-mcp" => val(api
                .agent_send_to_task(
                    ws,
                    note_id(args, "taskNoteId")?,
                    req_str(args, "message")?,
                    opt_str(args, "priority"),
                )
                .await),
            "wake_or_create_task_agent_workspace-mcp" => {
                // Reference `WakeOrCreateTaskAgentTool` guards against the caller
                // exceeding `MAX_DELEGATION_DEPTH` before the tool may create a
                // new agent for the task (the depth-check on the delegate path
                // lives inside `agent_delegate_op`). Fetch the caller's
                // projection to read its persisted depth.
                if let Some(caller) = self.caller_agent_id.clone() {
                    if let Ok(caller_lite) = api.agent_get(caller.clone(), Some(ws.clone())).await {
                        let depth = caller_lite.metadata.delegation_depth.unwrap_or(0);
                        if depth >= MAX_DELEGATION_DEPTH {
                            return Err(Error::InvalidParams(format!(
                                "Cannot delegate task: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {depth}. Please complete this task directly instead of delegating further."
                            )));
                        }
                    }
                }
                val(api
                    .agent_wake_or_create(
                        ws,
                        note_id(args, "taskNoteId")?,
                        req_str(args, "contextMessage")?,
                        intent_core::AgentWakeOrCreateInput {
                            model: opt_str(args, "model"),
                            ..Default::default()
                        },
                    )
                    .await)
            }
            // ---- Agent read tools (never restricted) ----
            "list_agents_workspace-mcp" => {
                // `includeCompleted` is accepted for wire parity with the
                // reference `ListAgentsTool` (§18.4). Filtering completed
                // agents lives in the services impl; the parameter is passed
                // through but not yet consulted by the daemon-side projection.
                let _include_completed = opt_bool(args, "includeCompleted").unwrap_or(false);
                val(api.agent_list(ws).await)
            }
            "get_agent_status_workspace-mcp" => {
                let agent_id = AgentId::from(req_str(args, "agentId")?.as_str());
                val(api.agent_get(agent_id, Some(ws)).await)
            }
            "read_agent_conversation_workspace-mcp" => {
                let agent_id = AgentId::from(req_str(args, "agentId")?.as_str());
                val(api
                    .agent_get_conversation(
                        agent_id,
                        opt_i64(args, "lastN"),
                        Some(ws),
                        opt_str(args, "pageToken"),
                    )
                    .await)
            }
            "get_agent_summary_workspace-mcp" => {
                let agent_id = AgentId::from(req_str(args, "agentId")?.as_str());
                val(api.agent_summary(ws, agent_id).await)
            }
            "get_agent_diagnostics_workspace-mcp" => {
                let agent_id = opt_str(args, "agentId").map(|s| AgentId::from(s.as_str()));
                let task_note_id = opt_str(args, "taskNoteId").map(NoteId::from_string);
                val(api
                    .agent_diagnostics(
                        ws,
                        agent_id,
                        task_note_id,
                        opt_i64(args, "staleRespondingAfterMs"),
                    )
                    .await)
            }
            // ---- Event subscription tools (deprecated aliases; the WSS
            // streaming surface lives on `events.subscribe`) ----
            "subscribe_to_events_workspace-mcp" => {
                let event_types = opt_vec_str(args, "eventTypes").ok_or_else(|| {
                    Error::InvalidParams("missing required parameter: eventTypes".to_string())
                })?;
                val(api
                    .agent_subscribe(
                        ws,
                        event_types,
                        opt_bool(args, "excludeSelf"),
                        opt_i64(args, "batchWindow"),
                    )
                    .await)
            }
            "unsubscribe_from_events_workspace-mcp" => val(api
                .agent_unsubscribe(ws, req_str(args, "subscriptionId")?)
                .await),
            // ---- Git write tools ----
            "git_commit_workspace-mcp" => {
                // Attribution is sourced from the MCP caller context (Option B,
                // matching the reference `ws-git-api.ts` agentCommit). Without an
                // agent context there is nothing to attribute the commit to.
                let agent_id = self.caller_agent_id.clone().ok_or_else(|| {
                    Error::InvalidParams(
                        "No agent context available. This tool must be called by an agent."
                            .to_string(),
                    )
                })?;
                let message = req_str(args, "message")?;
                let files = opt_vec_str(args, "files");
                let user_requested = opt_bool(args, "userRequested").unwrap_or(false);
                // `linked_note_id` stays `None`: its producer (auto-commit on task
                // completion) is not yet ported.
                val(api
                    .git_agent_commit(ws, message, Some(agent_id), None, files, user_requested)
                    .await)
            }
            other => Err(Error::InvalidParams(format!("Tool not found: {other}"))),
        }
    }
}
