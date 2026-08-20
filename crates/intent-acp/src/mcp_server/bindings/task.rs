//! `ws.task.*` bindings (WSAPI-3).
//!
//! Every entry point forwards to a matching [`WorkspaceApi`] method; the
//! typed results already carry the field names the reference `ws-task-api.ts`
//! peer returns, so shaping here is limited to peeling JS-side argument
//! objects into the trait's positional inputs.

use std::sync::Arc;

use intent_core::{AgentId, NoteId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_str, opt_vec_str, req_i64, req_str};

/// Canonical `TaskStatus` values accepted by the daemon. Kept in one place so
/// pre-flight validation stays consistent between `updateNoteStatus` and
/// `markAsTask` (both hit the same `parse_task_status_strict` in the service
/// layer, which reports internal-looking errors on unknown values).
const VALID_TASK_STATUSES: &[&str] = &[
    "not_started",
    "waiting",
    "discussion_needed",
    "blocked",
    "in_progress",
    "review_required",
    "complete",
    "cancelled",
];

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.task = {
        updateStatus: (noteId, taskText, status) =>
            host({ method: 'task.updateStatus', args: { noteId, taskText, status } }),
        updateNoteStatus: (noteId, status) =>
            host({ method: 'task.updateNoteStatus', args: { noteId, status } }),
        update: (noteId, line, options) =>
            host({ method: 'task.update', args: { noteId, line, ...(options || {}) } }),
        getMyTask: (taskNoteId) =>
            host({ method: 'task.getMyTask', args: { taskNoteId } }),
        markAsTask: (noteId, status, options) =>
            host({ method: 'task.markAsTask', args: { noteId, status, ...(options || {}) } }),
        setRelations: (noteId, options) =>
            host({ method: 'task.setRelations', args: { noteId, ...(options || {}) } }),
        convertBlocks: (noteId) =>
            host({ method: 'task.convertBlocks', args: { noteId } }),
        createPrerequisite: (dependentNoteId, title, options) =>
            host({
                method: 'task.createPrerequisite',
                args: { dependentNoteId, title, ...(options || {}) },
            }),
        assignAgent: (noteId, agentId, force) =>
            host({ method: 'task.assignAgent', args: { noteId, agentId, force } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "updateStatus" => update_status(api, ws, args).await,
        "updateNoteStatus" => update_note_status(api, ws, caller_agent_id, args).await,
        "update" => update(api, ws, args).await,
        "getMyTask" => get_my_task(api, ws, args).await,
        "markAsTask" => mark_as_task(api, ws, caller_agent_id, args).await,
        "setRelations" => set_relations(api, ws, args).await,
        "convertBlocks" => convert_blocks(api, ws, caller_agent_id, args).await,
        "createPrerequisite" => create_prerequisite(api, ws, caller_agent_id, args).await,
        "assignAgent" => assign_agent(api, ws, args).await,
        other => Err(format!("host: unknown method `task.{other}`")),
    }
}

async fn update_status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let task_text = req_str(args, "taskText")
        .map_err(|_| "Task text is required to identify the task".to_string())?;
    let status = req_str(args, "status")
        .map_err(|_| "Status must be 'done', 'todo', or 'in-progress'".to_string())?;
    if !matches!(status.as_str(), "done" | "todo" | "in-progress") {
        return Err("Status must be 'done', 'todo', or 'in-progress'".to_string());
    }
    let r = api
        .task_update_status(ws.clone(), NoteId::from_string(&note_id), task_text, status)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn update_note_status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let status = req_str(args, "status")?;
    if !VALID_TASK_STATUSES.contains(&status.as_str()) {
        return Err(format!(
            "Invalid status: {status}. Must be one of: {}",
            VALID_TASK_STATUSES.join(", ")
        ));
    }
    let r = api
        .task_update_note_status(
            ws.clone(),
            NoteId::from_string(&note_id),
            status,
            None,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn update(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let line = req_i64(args, "line").map_err(|_| "Line number is required".to_string())?;
    if line < 1 {
        return Err("Line number must be a positive integer".to_string());
    }
    let text = opt_str(args, "text");
    let status = opt_str(args, "status");
    let expected = opt_str(args, "expected");
    if text.is_none() && status.is_none() {
        return Err("Either text or status (or both) must be provided".to_string());
    }
    if let Some(s) = &status {
        if !matches!(s.as_str(), "todo" | "in-progress" | "done") {
            return Err("Status must be 'todo', 'in-progress', or 'done'".to_string());
        }
    }
    let r = api
        .task_update(
            ws.clone(),
            NoteId::from_string(&note_id),
            line,
            text,
            status,
            expected,
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn get_my_task(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let task_note_id = req_str(args, "taskNoteId")?;
    let r = api
        .get_my_task(ws.clone(), NoteId::from_string(&task_note_id))
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn mark_as_task(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let status = req_str(args, "status")?;
    if !VALID_TASK_STATUSES.contains(&status.as_str()) {
        return Err(format!(
            "Invalid status: {status}. Must be one of: {}",
            VALID_TASK_STATUSES.join(", ")
        ));
    }
    let acceptance_criteria = opt_vec_str(args, "acceptanceCriteria").unwrap_or_default();
    let effort = opt_str(args, "effort");
    let depends_on = opt_note_ids(args, "dependsOn");
    let conflicts_with = opt_note_ids(args, "conflictsWith");
    let r = api
        .mark_as_task(
            ws.clone(),
            NoteId::from_string(&note_id),
            status.clone(),
            acceptance_criteria,
            effort,
            depends_on,
            conflicts_with,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    // Reference peer returns only `{ ok, noteId, status }` — echo the daemon's
    // canonical `TaskMarkAsTaskResult` verbatim (which already carries those
    // three plus the parsed `TaskStatus`). Both shapes stay in sync via serde.
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn set_relations(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let r = api
        .task_set_relations(
            ws.clone(),
            NoteId::from_string(&note_id),
            opt_note_ids(args, "dependsOn"),
            opt_note_ids(args, "conflictsWith"),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

/// Optional note-id-array arg — `opt_vec_str` mapped into `NoteId`s.
fn opt_note_ids(args: &Value, key: &str) -> Option<Vec<NoteId>> {
    opt_vec_str(args, key).map(|ids| ids.into_iter().map(NoteId::from_string).collect())
}

async fn convert_blocks(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let r = api
        .convert_task_blocks(
            ws.clone(),
            NoteId::from_string(&note_id),
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn create_prerequisite(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let dependent_note_id = req_str(args, "dependentNoteId")?;
    let title = req_str(args, "title")?;
    let content = opt_str(args, "content");
    let status = opt_str(args, "status");
    let r = api
        .create_prerequisite(
            ws.clone(),
            NoteId::from_string(&dependent_note_id),
            title,
            content,
            status,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn assign_agent(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let agent_id = req_str(args, "agentId")?;
    // Format validation mirrors the reference: `agent-{uuid}`.
    if !is_agent_id(&agent_id) {
        return Err(format!(
            "Invalid agentId format: \"{agent_id}\". Agent IDs must be in format \"agent-{{uuid}}\" (e.g., \"agent-b0a8044a-5eac-4b52-8456-15d3b784decb\"). To create a new agent and assign it to this task, use create_agent with taskNoteId=\"{note_id}\" instead."
        ));
    }
    let r = api
        .assign_agent(
            ws.clone(),
            NoteId::from_string(&note_id),
            agent_id,
            opt_bool(args, "force"),
        )
        .await
        .map_err(map_err)?;
    // Return the daemon's canonical `TaskAssignAgentResult`; its serde
    // rename matches the reference peer's `{ ok, noteId, agentId }` shape.
    serde_json::to_value(r).map_err(|e| e.to_string())
}

fn is_agent_id(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("agent-") else {
        return false;
    };
    // 36-char uuid with 4 dashes at 8-4-4-4-12 positions; hex chars elsewhere.
    if rest.len() != 36 {
        return false;
    }
    let bytes = rest.as_bytes();
    let hyphen_positions = [8usize, 13, 18, 23];
    for (i, b) in bytes.iter().enumerate() {
        let is_hyphen = hyphen_positions.contains(&i);
        let ok = if is_hyphen {
            *b == b'-'
        } else {
            b.is_ascii_hexdigit()
        };
        if !ok {
            return false;
        }
    }
    true
}
