//! Transport-agnostic JSON-RPC 2.0 router (PROTOCOL §3, §9).
//!
//! [`handle_message`] takes a single request string and returns the response
//! string, or `None` for notifications (a request without an `id` member).
//! Envelope validation, the notification-vs-request distinction, and the
//! `-32700/-32600/-32601/-32602/-32603` error matrix all live here so every
//! transport (UDS today, WS/TLS later) shares one code path.

use intent_core::{
    AgentDelegateInput, AgentId, Error, EventQueryParams, NoteAddInput, NoteCreate, NoteEditInput,
    NoteEditLinesInput, NoteId, NoteUpdateInput, WorkspaceApi, WorkspaceCreate, WorkspaceId,
    WorkspaceUpdate,
};
use serde_json::{json, Map, Value};

const PARSE_ERROR: i32 = -32700;
const INVALID_REQUEST: i32 = -32600;
const METHOD_NOT_FOUND: i32 = -32601;
const INVALID_PARAMS: i32 = -32602;

/// A JSON-RPC error to surface to the client.
struct RpcErr {
    code: i32,
    message: String,
    data: Option<Value>,
}

fn rpc(code: i32, message: impl Into<String>) -> RpcErr {
    RpcErr {
        code,
        message: message.into(),
        data: None,
    }
}

/// Map a domain [`Error`] to its JSON-RPC representation (§9). Internal errors
/// surface as `-32603 "Internal error"` carrying the original cause in `data`.
fn domain_to_rpc(e: Error) -> RpcErr {
    match e {
        Error::Internal(msg) => RpcErr {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(Value::String(msg)),
        },
        other => RpcErr {
            code: other.code(),
            message: other.to_string(),
            data: None,
        },
    }
}

/// Handle one JSON-RPC frame. Returns `Some(response)` for requests and `None`
/// for notifications (including unknown / failed ones, per §3.4).
pub async fn handle_message(api: &dyn WorkspaceApi, message: &str) -> Option<String> {
    let value: Value = match serde_json::from_str(message) {
        Ok(v) => v,
        // Parse errors are always answered with id null (§9), even for
        // would-be notifications — notification status is not yet known.
        Err(_) => return Some(error_string(Value::Null, PARSE_ERROR, "Parse error", None)),
    };

    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            return Some(error_string(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request: expected an object",
                None,
            ))
        }
    };

    let id_member = obj.get("id");
    let has_id = id_member.is_some();
    let id_type_ok = match id_member {
        None => true,
        Some(v) => v.is_string() || v.is_number() || v.is_null(),
    };
    let echo_id = match id_member {
        Some(v) if id_type_ok => v.clone(),
        _ => Value::Null,
    };

    // Envelope validation (-32600). Answered even for notification-shaped
    // frames: notification status is not trusted until the envelope is valid.
    let jsonrpc_ok = obj.get("jsonrpc").and_then(Value::as_str) == Some("2.0");
    let method = obj.get("method").and_then(Value::as_str);
    let method_ok = method.map(|m| !m.is_empty()).unwrap_or(false);
    if !jsonrpc_ok || !method_ok || !id_type_ok {
        let msg = if !jsonrpc_ok {
            "Invalid Request: jsonrpc must be \"2.0\""
        } else if !method_ok {
            "Invalid Request: method must be a non-empty string"
        } else {
            "Invalid Request: id must be a string, number, or null"
        };
        return Some(error_string(echo_id, INVALID_REQUEST, msg, None));
    }
    let method = method.unwrap();
    let is_notification = !has_id;

    // params: object kept as-is; positional array coerced to {}; absent/null
    // treated as empty; any other scalar is invalid (§3.1).
    let params: Map<String, Value> = match obj.get("params") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(Value::Array(_)) => Map::new(),
        Some(_) => {
            if is_notification {
                return None;
            }
            return Some(error_string(
                echo_id,
                INVALID_PARAMS,
                "Invalid params",
                None,
            ));
        }
    };

    let result = dispatch(api, method, &params).await;

    // Notifications never get a response, even on error / unknown method (§3.4).
    if is_notification {
        return None;
    }
    Some(match result {
        Ok(v) => success_string(echo_id, v),
        Err(e) => error_string(echo_id, e.code, &e.message, e.data),
    })
}

/// Dispatch a validated request to the injected [`WorkspaceApi`].
async fn dispatch(
    api: &dyn WorkspaceApi,
    method: &str,
    params: &Map<String, Value>,
) -> Result<Value, RpcErr> {
    match method {
        "workspace.list" => {
            let include_archived = params
                .get("includeArchived")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let workspaces = api
                .list_workspaces(include_archived)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "workspaces": workspaces }))
        }
        "workspace.get" => {
            let id = require_workspace_id(params)?;
            let ws = api.get_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.create" => {
            let input: WorkspaceCreate = serde_json::from_value(Value::Object(params.clone()))
                .map_err(|e| rpc(INVALID_PARAMS, format!("invalid params: {e}")))?;
            let ws = api.create_workspace(input).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.update" => {
            let id = require_workspace_id(params)?;
            let mut rest = params.clone();
            rest.remove("workspaceId");
            let update: WorkspaceUpdate = serde_json::from_value(Value::Object(rest))
                .map_err(|e| rpc(INVALID_PARAMS, format!("invalid params: {e}")))?;
            let ws = api
                .update_workspace(id, update)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.delete" => {
            let id = require_workspace_id(params)?;
            api.delete_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.archive" => {
            let id = require_workspace_id(params)?;
            api.archive_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.unarchive" => {
            let id = require_workspace_id(params)?;
            api.unarchive_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.dismissAttention" => {
            let id = require_workspace_id(params)?;
            let ws = api.dismiss_attention(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.markSeen" => {
            let id = require_workspace_id(params)?;
            let ws = api.mark_seen(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "note.list" => {
            let ws_id = match params.get("workspaceId").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => WorkspaceId::from(s),
                _ => return Err(rpc(INVALID_PARAMS, "workspaceId is required")),
            };
            let notes = api.list_notes(&ws_id).await.map_err(domain_to_rpc)?;
            Ok(json!({ "notes": notes }))
        }
        "note.get" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            match api.get_note(ws, note_id).await {
                Ok(note) => Ok(json!({ "note": note })),
                Err(Error::NotFound(_)) => Err(rpc(INVALID_PARAMS, "Note not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "note.create" => {
            let ws = require_ws_note(params)?;
            let title = require_str_param(params, "title")?;
            let input = NoteCreate {
                title,
                content: opt_str(params, "content"),
                tags: opt_tags(params, "tags"),
                parent_id: opt_str(params, "parentId"),
            };
            let note = api.create_note(ws, input).await.map_err(domain_to_rpc)?;
            Ok(json!({ "note": note }))
        }
        "note.update" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let input = NoteUpdateInput {
                content: opt_str(params, "content"),
                title: opt_str(params, "title"),
                tags: opt_tags(params, "tags"),
            };
            let note = api
                .update_note(ws, note_id, input)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "note": note }))
        }
        "note.add" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let content = require_str_param(params, "content")?;
            let input = NoteAddInput {
                content,
                heading: opt_str(params, "heading"),
                position: opt_str(params, "position"),
            };
            let result = api
                .add_to_note(ws, note_id, input)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.edit" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let old = require_str_param(params, "old")?;
            let new = require_str_param(params, "new")?;
            let input = NoteEditInput { old, new };
            let result = api
                .edit_note(ws, note_id, input)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.editLines" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            require_present(params, "start")?;
            require_present(params, "end")?;
            let content = require_str_param(params, "content")?;
            // Non-numeric/absent coerce to 0 so the service emits the TS
            // "must be a positive integer" message.
            let start = parse_int_loose(params.get("start")).unwrap_or(0);
            let end = parse_int_loose(params.get("end")).unwrap_or(0);
            let input = NoteEditLinesInput {
                start,
                end,
                content,
            };
            let result = api
                .edit_note_lines(ws, note_id, input)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.setContent" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let content = require_str_param(params, "content")?;
            let confirm = parse_confirm(params);
            let result = api
                .set_note_content(ws, note_id, content, confirm)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.updateMetadata" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let title = opt_str(params, "title");
            let tags = opt_tags(params, "tags");
            let result = api
                .update_note_metadata(ws, note_id, title, tags)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.delete" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let result = api.delete_note(ws, note_id).await.map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.listTasks" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let tasks = api
                .list_note_tasks(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            // The TS peer returns a bare array.
            to_result_value(&tasks)
        }
        "note.readAsset" => {
            let ws = require_ws_note(params)?;
            let asset = require_str_param(params, "asset")?;
            let result = api.read_asset(ws, asset).await.map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.updateStatus" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let task_text = require_str_param(params, "taskText")?;
            let status = require_str_param(params, "status")?;
            let result = api
                .task_update_status(ws, note_id, task_text, status)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.updateNoteStatus" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let status = require_str_param(params, "status")?;
            let result = api
                .task_update_note_status(ws, note_id, status)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.update" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            require_present(params, "line")?;
            // Non-numeric/absent coerce to 0 so the service emits the TS
            // "Line number must be a positive integer" message.
            let line = parse_int_loose(params.get("line")).unwrap_or(0);
            let text = opt_str(params, "text");
            let status = opt_str(params, "status");
            let expected = opt_str(params, "expected");
            let result = api
                .task_update(ws, note_id, line, text, status, expected)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.getMyTask" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let result = api
                .get_my_task(ws, task_note_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.markAsTask" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let status = require_str_param(params, "status")?;
            let acceptance_criteria = normalize_acceptance_criteria(params);
            let effort = opt_str(params, "effort");
            let result = api
                .mark_as_task(ws, note_id, status, acceptance_criteria, effort)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.convertBlocks" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let result = api
                .convert_task_blocks(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.createPrerequisite" => {
            let ws = require_ws_note(params)?;
            let dependent_note_id =
                require_str_param(params, "dependentNoteId").map(NoteId::from)?;
            let title = require_str_param(params, "title")?;
            let content = opt_str(params, "content");
            let status = opt_str(params, "status");
            let result = api
                .create_prerequisite(ws, dependent_note_id, title, content, status)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.assignAgent" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let agent_id = require_str_param(params, "agentId")?;
            let result = api
                .assign_agent(ws, note_id, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.add" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let search_context = require_str_param(params, "searchContext")?;
            let comment_target = require_str_param(params, "commentTarget")?;
            let comment = require_str_param(params, "comment")?;
            let kind = opt_str(params, "type");
            let author = opt_str(params, "author");
            let result = api
                .comment_add(
                    ws,
                    note_id,
                    search_context,
                    comment_target,
                    comment,
                    kind,
                    author,
                )
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.list" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let since = opt_str(params, "since");
            let author_type = opt_str(params, "authorType");
            let status = opt_str(params, "status");
            let include_comments = parse_bool(params, "includeComments");
            let result = api
                .comment_list(ws, note_id, since, author_type, status, include_comments)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.getThread" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let thread_id = opt_str(params, "threadId");
            let comment_id = opt_str(params, "commentId");
            let result = api
                .comment_get_thread(ws, note_id, thread_id, comment_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.respond" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let comment = require_str_param(params, "comment")?;
            let thread_id = opt_str(params, "threadId");
            let comment_id = opt_str(params, "commentId");
            let kind = opt_str(params, "type");
            let author = opt_str(params, "author");
            let suggestion_original = opt_str(params, "suggestionOriginal");
            let suggestion_proposed = opt_str(params, "suggestionProposed");
            let result = api
                .comment_respond(
                    ws,
                    note_id,
                    thread_id,
                    comment_id,
                    comment,
                    kind,
                    author,
                    suggestion_original,
                    suggestion_proposed,
                )
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.delete" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let comment_id = require_str_param(params, "commentId")?;
            let result = api
                .comment_delete(ws, note_id, comment_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.recentFiles" => {
            let ws = require_ws_note(params)?;
            let result = api
                .event_recent_files(ws, opt_int(params, "limit"))
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.agentActivity" => {
            let ws = require_ws_note(params)?;
            let agent_id = opt_str(params, "agentId");
            let minutes_ago = opt_int(params, "minutesAgo");
            let result = api
                .event_agent_activity(ws, agent_id, minutes_ago)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.workspaceSummary" => {
            let ws = require_ws_note(params)?;
            let result = api
                .event_workspace_summary(ws, opt_int(params, "minutesAgo"))
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.directoryChanges" => {
            let ws = require_ws_note(params)?;
            let dir = require_str_param(params, "dir")?;
            let result = api
                .event_directory_changes(ws, dir, opt_int(params, "limit"))
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.query" => {
            let ws = require_ws_note(params)?;
            let query = EventQueryParams {
                event_type: opt_str(params, "eventType"),
                actor_type: opt_str(params, "actorType"),
                actor_id: opt_str(params, "actorId"),
                path: opt_str(params, "path"),
                minutes_ago: opt_int(params, "minutesAgo"),
                limit: opt_int(params, "limit"),
            };
            let result = api.event_query(ws, query).await.map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.subscribe" => {
            let ws = require_ws_note(params)?;
            require_present(params, "eventTypes")?;
            let event_types = match params.get("eventTypes") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
                _ => return Err(rpc(INVALID_PARAMS, "eventTypes must be an array")),
            };
            let result = api
                .event_subscribe(ws, event_types)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.unsubscribe" => {
            let ws = require_ws_note(params)?;
            let subscription_id = require_str_param(params, "subscriptionId")?;
            let result = api
                .event_unsubscribe(ws, subscription_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "agent.list" => {
            let ws = require_ws_note(params)?;
            let agents = api.agent_list(ws).await.map_err(domain_to_rpc)?;
            Ok(json!({ "agents": agents }))
        }
        "agent.get" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            match api.agent_get(agent_id, ws).await {
                Ok(agent) => Ok(json!({ "agent": agent })),
                Err(Error::NotFound(_)) => Err(rpc(INVALID_PARAMS, "Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.getConversation" => {
            let agent_id = require_agent_id(params)?;
            let limit = opt_int(params, "limit");
            let ws = opt_workspace_id(params);
            match api.agent_get_conversation(agent_id, limit, ws).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(rpc(INVALID_PARAMS, "Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.create" => {
            let ws = require_ws_note(params)?;
            let name = opt_str(params, "name");
            let model = opt_str(params, "model");
            let specialist_id = opt_str(params, "specialistId");
            let result = api
                .agent_create(ws, name, model, specialist_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.delegate" => {
            let ws = require_ws_note(params)?;
            let mut rest = params.clone();
            rest.remove("workspaceId");
            let input: AgentDelegateInput = serde_json::from_value(Value::Object(rest))
                .map_err(|e| rpc(INVALID_PARAMS, format!("invalid params: {e}")))?;
            let result = api.agent_delegate(ws, input).await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.sendToTask" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let message = require_str_param(params, "message")?;
            let priority = opt_str(params, "priority");
            let result = api
                .agent_send_to_task(ws, task_note_id, message, priority)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.sendMessage" => {
            let agent_id = require_agent_id(params)?;
            let content = require_str_param(params, "content")?;
            let ws = require_ws_note(params)?;
            let message_id = opt_str(params, "messageId");
            let image_blocks = opt_value(params, "imageBlocks");
            let result = api
                .agent_send_message(ws, agent_id, content, message_id, image_blocks)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.forceMessage" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let content = require_str_param(params, "content")?;
            let ws = require_ws_note(params)?;
            let image_blocks = opt_value(params, "imageBlocks");
            let note_ids = opt_value(params, "noteIds");
            let result = api
                .agent_force_message(ws, agent_id, message_id, content, image_blocks, note_ids)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.queueMessage" => {
            let agent_id = require_agent_id(params)?;
            let content = require_str_param(params, "content")?;
            let image_blocks = opt_value(params, "imageBlocks");
            let result = api
                .agent_queue_message(agent_id, content, image_blocks)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.editQueuedMessage" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let content = require_str_param(params, "content")?;
            let result = api
                .agent_edit_queued_message(agent_id, message_id, content)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.removeQueuedMessage" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let result = api
                .agent_remove_queued_message(agent_id, message_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getQueue" => {
            let agent_id = require_agent_id(params)?;
            let result = api.agent_get_queue(agent_id).await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.stop" => {
            let agent_id = require_agent_id(params)?;
            let result = api.agent_stop(agent_id).await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.setModel" => {
            let agent_id = require_agent_id(params)?;
            let model_id = require_str_param(params, "modelId")?;
            let ws = require_ws_note(params)?;
            let result = api
                .agent_set_model(ws, agent_id, model_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getModels" => {
            let result = api.agent_get_models().await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.rename" => {
            let agent_id = require_agent_id(params)?;
            let name = require_str_param(params, "name")?;
            let trimmed = name.trim().to_string();
            if trimmed.is_empty() {
                return Err(rpc(INVALID_PARAMS, "Name cannot be empty"));
            }
            let result = api
                .agent_rename(agent_id, trimmed)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.delete" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let result = api
                .agent_delete(agent_id, ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.wakeOrCreate" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let context_message = require_str_param(params, "contextMessage")?;
            let model = opt_str(params, "model");
            let result = api
                .agent_wake_or_create(ws, task_note_id, context_message, model)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.summary" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            let result = api
                .agent_summary(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.reportToParent" => {
            let ws = require_ws_note(params)?;
            require_present(params, "report")?;
            let report = params.get("report").cloned().unwrap_or(Value::Null);
            let result = api
                .agent_report_to_parent(ws, report)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getSubscriptions" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            let result = api
                .agent_get_subscriptions(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.cancelSubscriptions" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            let result = api
                .agent_cancel_subscriptions(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.subscribe" => {
            let ws = require_ws_note(params)?;
            require_present(params, "eventTypes")?;
            let event_types = match params.get("eventTypes") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
                _ => return Err(rpc(INVALID_PARAMS, "eventTypes must be an array")),
            };
            let exclude_self = params.get("excludeSelf").and_then(Value::as_bool);
            let batch_window = opt_int(params, "batchWindow");
            let result = api
                .agent_subscribe(ws, event_types, exclude_self, batch_window)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.unsubscribe" => {
            let ws = require_ws_note(params)?;
            let subscription_id = require_str_param(params, "subscriptionId")?;
            let result = api
                .agent_unsubscribe(ws, subscription_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "git.status" => {
            let ws = require_ws_note(params)?;
            let status = api.git_status(ws).await.map_err(domain_to_rpc)?;
            to_result_value(&status)
        }
        "git.stage" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let staged = api.git_stage(ws, paths).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "paths": staged }))
        }
        "git.getBranches" => {
            let repo_path = require_str_param(params, "repoPath")?;
            let include_remote = parse_bool(params, "includeRemote");
            match api.git_get_branches(repo_path, include_remote).await {
                Ok(branches) => to_result_value(&branches),
                // Unknown/unauthorized repo path → -32602 with the TS message
                // verbatim (no `invalid params:` prefix from `domain_to_rpc`).
                Err(Error::InvalidParams(m)) => Err(rpc(INVALID_PARAMS, m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "git.commit" => {
            let ws = require_ws_note(params)?;
            let message = require_str_param(params, "message")?;
            let r = api.git_commit(ws, message).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "hash": r.hash, "files": r.files }))
        }
        "git.agentCommit" => {
            let ws = require_ws_note(params)?;
            let message = require_str_param(params, "message")?;
            let files = opt_str_array(params, "files");
            let user_requested = parse_bool(params, "userRequested");
            let r = api
                .git_agent_commit(ws, message, files, user_requested)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "hash": r.hash,
                "files": r.files,
                "fileCount": r.file_count,
            }))
        }
        "git.checkMergeConflicts" => {
            let ws = require_ws_note(params)?;
            let target = opt_str(params, "targetBranch");
            let r = api
                .git_check_merge_conflicts(ws, target)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&r)
        }
        _ => Err(rpc(METHOD_NOT_FOUND, "Method not found")),
    }
}

/// Extract a required `workspaceId` for `note.*` methods, matching the TS
/// message `workspaceId is required` (distinct from the `workspace.*` wording).
fn require_ws_note(params: &Map<String, Value>) -> Result<WorkspaceId, RpcErr> {
    match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(WorkspaceId::from(s)),
        _ => Err(rpc(INVALID_PARAMS, "workspaceId is required")),
    }
}

/// Require a `noteId` string param (`requireParam` parity: present & non-null).
fn require_note_id(params: &Map<String, Value>) -> Result<NoteId, RpcErr> {
    require_str_param(params, "noteId").map(NoteId::from)
}

/// Require an `agentId` string param (`requireParam` parity).
fn require_agent_id(params: &Map<String, Value>) -> Result<AgentId, RpcErr> {
    require_str_param(params, "agentId").map(|s| AgentId::from(s.as_str()))
}

/// Optional `workspaceId` for `agent.*` methods where it is a non-required
/// fallback (`agent.get`/`agent.getConversation`/`agent.delete`).
fn opt_workspace_id(params: &Map<String, Value>) -> Option<WorkspaceId> {
    params
        .get("workspaceId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(WorkspaceId::from)
}

/// Optional pass-through JSON param (absent/null → `None`); used for the opaque
/// `imageBlocks` / `noteIds` payloads.
fn opt_value(params: &Map<String, Value>, name: &str) -> Option<Value> {
    match params.get(name) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.clone()),
    }
}

/// Require a string param, mirroring TS `requireParam` (undefined/null → error).
fn require_str_param(params: &Map<String, Value>, name: &str) -> Result<String, RpcErr> {
    match params.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(rpc(
            INVALID_PARAMS,
            format!("Missing required parameter: {name}"),
        )),
    }
}

/// Require a param be present and non-null (used for numeric `start`/`end`).
fn require_present(params: &Map<String, Value>, name: &str) -> Result<(), RpcErr> {
    match params.get(name) {
        Some(Value::Null) | None => Err(rpc(
            INVALID_PARAMS,
            format!("Missing required parameter: {name}"),
        )),
        Some(_) => Ok(()),
    }
}

/// Optional string param (absent/null/non-string → `None`).
fn opt_str(params: &Map<String, Value>, name: &str) -> Option<String> {
    params.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Optional string-array param (absent/null/non-array → `None`); non-string
/// elements are skipped. Used for the `git.agentCommit` `files` list.
fn opt_str_array(params: &Map<String, Value>, name: &str) -> Option<Vec<String>> {
    params.get(name).and_then(Value::as_array).map(|items| {
        items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

/// Optional integer param from a JSON number (absent/non-number → `None`).
/// Used by the `event.*` `limit` / `minutesAgo` knobs, whose defaults are
/// applied in the service layer (`value || default`).
fn opt_int(params: &Map<String, Value>, name: &str) -> Option<i64> {
    match params.get(name) {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        _ => None,
    }
}

/// Normalize a `tags` param (array or comma string) into trimmed, non-empty
/// entries, mirroring the TS `normalizeTags`.
fn opt_tags(params: &Map<String, Value>, name: &str) -> Option<Vec<String>> {
    match params.get(name) {
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        ),
        Some(Value::String(s)) => Some(
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

/// `confirmReplacement` accepts a boolean or the string `"true"` (TS parity).
fn parse_confirm(params: &Map<String, Value>) -> bool {
    match params.get("confirmReplacement") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// Parse a boolean flag param: a real bool, or the string `"true"`.
fn parse_bool(params: &Map<String, Value>, name: &str) -> bool {
    match params.get(name) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// Normalize `task.markAsTask` `acceptanceCriteria`: a string array as-is, a
/// JSON-array string parsed, or any other string wrapped as a single entry
/// (mirrors the TS `Array.isArray ? … : JSON.parse(…) ?? [value]` branch).
fn normalize_acceptance_criteria(params: &Map<String, Value>) -> Vec<String> {
    match params.get("acceptanceCriteria") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        Some(Value::String(s)) => match serde_json::from_str::<Vec<String>>(s) {
            Ok(v) => v,
            Err(_) => vec![s.clone()],
        },
        _ => Vec::new(),
    }
}

/// Loosely parse an integer from a JSON number or leading-int string
/// (`parseInt`-like), returning `None` when no integer is present.
fn parse_int_loose(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Value::String(s)) => parse_leading_int(s),
        _ => None,
    }
}

fn parse_leading_int(s: &str) -> Option<i64> {
    let t = s.trim_start();
    let bytes = t.as_bytes();
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let digit_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == digit_start {
        return None;
    }
    t[..i].parse::<i64>().ok()
}

/// Serialize a typed result into a JSON-RPC `result` value.
fn to_result_value<T: serde::Serialize>(value: &T) -> Result<Value, RpcErr> {
    serde_json::to_value(value)
        .map_err(|e| domain_to_rpc(Error::Internal(format!("serialize result failed: {e}"))))
}

/// Extract a required `workspaceId` string param, or `-32602` with the exact
/// message the TS handler emits via `requireParam` (PROTOCOL §5.1).
fn require_workspace_id(params: &Map<String, Value>) -> Result<WorkspaceId, RpcErr> {
    match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(WorkspaceId::from(s)),
        _ => Err(rpc(
            INVALID_PARAMS,
            "Missing required parameter: workspaceId",
        )),
    }
}

/// Map a domain [`Error`] for `workspace.*` methods: a missing workspace surfaces
/// as `-32602 "Workspace not found"`, matching the TS handler (PROTOCOL §5.1).
fn workspace_err(e: Error) -> RpcErr {
    match e {
        Error::NotFound(_) => rpc(INVALID_PARAMS, "Workspace not found"),
        other => domain_to_rpc(other),
    }
}

/// Serialize a success envelope. `result` is always a JSON object (§3.2).
fn success_string(id: Value, result: Value) -> String {
    let resp = json!({ "jsonrpc": "2.0", "result": result, "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Serialize an error envelope, optionally carrying `data`.
fn error_string(id: Value, code: i32, message: &str, data: Option<Value>) -> String {
    let mut err = Map::new();
    err.insert("code".to_string(), json!(code));
    err.insert("message".to_string(), json!(message));
    if let Some(d) = data {
        err.insert("data".to_string(), d);
    }
    let resp = json!({ "jsonrpc": "2.0", "error": Value::Object(err), "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Last-resort response if serialization itself fails (should never happen).
fn internal_fallback() -> String {
    r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null}"#.to_string()
}

#[cfg(test)]
mod tests;
