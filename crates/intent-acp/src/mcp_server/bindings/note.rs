//! `ws.note.*` bindings (WSAPI-3).
//!
//! Every JS entry point is a thin wrapper around `host({ method, args })`;
//! the Rust side calls the matching [`WorkspaceApi`] method and — for the
//! two shapes that diverge from the daemon's typed result (`note.read`,
//! `note.create`, `note.list`) — re-shapes the value to match the reference
//! `ws-note-api.ts` return objects agents already consume.

use std::sync::Arc;

use intent_core::{
    AgentId, NoteAddInput, NoteCreate, NoteEditInput, NoteEditLinesInput, NoteId, WorkspaceApi,
    WorkspaceId,
};
use serde_json::{json, Value};

use super::{map_err, opt_str, opt_vec_str, req_i64, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.note = {
        read: (id) => host({ method: 'note.read', args: { id } }),
        create: (title, content, tags) =>
            host({ method: 'note.create', args: { title, content, tags } }),
        list: (tag) => host({ method: 'note.list', args: { tag } }),
        listTasks: (id) => host({ method: 'note.listTasks', args: { id } }),
        readAsset: (asset) => host({ method: 'note.readAsset', args: { asset } }),
        setContent: (id, content, confirmReplacement) =>
            host({ method: 'note.setContent', args: { id, content, confirmReplacement } }),
        add: (id, options) => host({ method: 'note.add', args: { id, ...(options || {}) } }),
        edit: (id, options) => host({ method: 'note.edit', args: { id, ...(options || {}) } }),
        editLines: (id, options) =>
            host({ method: 'note.editLines', args: { id, ...(options || {}) } }),
        updateMetadata: (id, options) =>
            host({ method: 'note.updateMetadata', args: { id, ...(options || {}) } }),
        delete: (id) => host({ method: 'note.delete', args: { id } }),
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
        "read" => read(api, ws, args).await,
        "create" => create(api, ws, caller_agent_id, args).await,
        "list" => list(api, ws, args).await,
        "listTasks" => list_tasks(api, ws, args).await,
        "readAsset" => read_asset(api, ws, args).await,
        "setContent" => set_content(api, ws, caller_agent_id, args).await,
        "add" => add(api, ws, caller_agent_id, args).await,
        "edit" => edit(api, ws, caller_agent_id, args).await,
        "editLines" => edit_lines(api, ws, caller_agent_id, args).await,
        "updateMetadata" => update_metadata(api, ws, caller_agent_id, args).await,
        "delete" => delete(api, ws, args).await,
        other => Err(format!("host: unknown method `note.{other}`")),
    }
}

async fn read(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let note = api
        .get_note(ws.clone(), NoteId::from_string(&id))
        .await
        .map_err(map_err)?;
    let raw_content = note.content.clone();
    let total_lines = if raw_content.is_empty() {
        0
    } else {
        raw_content.split('\n').count()
    };
    let mut rendered = String::new();
    if !raw_content.is_empty() {
        for (idx, line) in raw_content.split('\n').enumerate() {
            if idx > 0 {
                rendered.push('\n');
            }
            rendered.push_str(&format!("{:>4} | {line}", idx + 1));
        }
    }
    if let Some(task) = note.metadata.task.as_ref() {
        rendered.push_str("\n\n--- Task Metadata ---\n");
        rendered.push_str(&format!(
            "Status: {}",
            serde_json::to_string(&task.status)
                .unwrap_or_default()
                .trim_matches('"')
        ));
        if !task.acceptance_criteria.is_empty() {
            rendered.push_str("\nAcceptance Criteria:\n");
            for c in &task.acceptance_criteria {
                rendered.push_str(&format!("  - {c}\n"));
            }
            rendered.pop();
        }
        if !task.assigned_agent_ids.is_empty() {
            let ids: Vec<String> = task
                .assigned_agent_ids
                .iter()
                .map(|a| a.as_str().to_string())
                .collect();
            rendered.push_str(&format!("\nAssigned Agents: {}", ids.join(", ")));
        }
        if let Some(e) = &task.estimated_effort {
            rendered.push_str(&format!("\nEstimated Effort: {e}"));
        }
        if let Some(r) = &task.blocked_reason {
            rendered.push_str(&format!("\nBlocked Reason: {r}"));
        }
    }
    let mut out = json!({
        "id": note.id.as_str(),
        "title": note.title,
        "tags": note.tags,
        "content": rendered,
        "rawContent": raw_content,
        "totalLines": total_lines,
        "imageCount": 0,
        "images": Vec::<Value>::new(),
    });
    if let Some(task) = note.metadata.task.as_ref() {
        let task_status = serde_json::to_value(task.status)
            .map_err(|e| format!("engine: serialize taskStatus failed: {e}"))?;
        let task_metadata = serde_json::to_value(task)
            .map_err(|e| format!("engine: serialize taskMetadata failed: {e}"))?;
        let obj = out.as_object_mut().unwrap();
        obj.insert("isTask".to_string(), Value::Bool(true));
        obj.insert("taskStatus".to_string(), task_status);
        obj.insert("taskMetadata".to_string(), task_metadata);
        obj.insert("dependencies".to_string(), json!([]));
    }
    Ok(out)
}

async fn create(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let title = req_str(args, "title").map_err(|_| "Title and content are required".to_string())?;
    let content =
        req_str(args, "content").map_err(|_| "Title and content are required".to_string())?;
    let tags = opt_vec_str(args, "tags");
    // Idempotency-wrapped in `intent-services`: honor a caller-supplied
    // `idempotencyKey` when present so retries of the same tool call dedupe,
    // otherwise mint a fresh UUID. Blank / whitespace-only keys are treated
    // as absent (parity with `comment.add`) so an accidental empty string
    // cannot collapse dedupe across unrelated requests.
    let idempotency_key = opt_str(args, "idempotencyKey")
        .filter(|k| !k.trim().is_empty())
        .or_else(|| Some(uuid::Uuid::new_v4().to_string()));
    let result = api
        .create_note(
            ws.clone(),
            NoteCreate {
                title,
                content: Some(content),
                tags,
                parent_id: None,
            },
            idempotency_key,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    let note = result.note;
    let link = format!("intent://local/{}/note/{}", ws.as_str(), note.id.as_str());
    let markdown_link = format!("[{}]({})", note.title, link);
    Ok(json!({
        "id": note.id.as_str(),
        "title": note.title,
        "tags": note.tags,
        "link": link,
        "markdownLink": markdown_link,
        "convertedCount": result.converted_count,
        "createdTaskNoteIds": result.created_task_note_ids,
        "createdTasks": result.created_tasks,
        "warnings": result.warnings,
    }))
}

async fn list(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let notes = api.list_notes(ws).await.map_err(map_err)?;
    let tag_filter = opt_str(args, "tag");
    let out: Vec<Value> = notes
        .into_iter()
        .filter(|n| match &tag_filter {
            Some(t) => n.tags.iter().any(|nt| nt == t),
            None => true,
        })
        .map(|n| {
            json!({
                "id": n.id.as_str(),
                "title": n.title,
                "tags": n.tags,
                "createdAt": n.created_at,
                "updatedAt": n.updated_at,
            })
        })
        .collect();
    Ok(Value::Array(out))
}

async fn list_tasks(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let rows = api
        .list_note_tasks(ws.clone(), NoteId::from_string(&id))
        .await
        .map_err(map_err)?;
    serde_json::to_value(rows).map_err(|e| e.to_string())
}

async fn read_asset(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let asset = req_str(args, "asset").map_err(|_| "Asset ID or URL is required".to_string())?;
    let r = api.read_asset(ws.clone(), asset).await.map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn set_content(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            "Content is required. Use updateMetadata to change only title/tags.".to_string()
        })?
        .to_string();
    let confirm = match args.get("confirmReplacement") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    let r = api
        .set_note_content(
            ws.clone(),
            NoteId::from_string(&id),
            content,
            confirm,
            None,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn add(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let content = req_str(args, "content").map_err(|_| "Content is required".to_string())?;
    let heading = opt_str(args, "heading");
    let position = opt_str(args, "position");
    let r = api
        .add_to_note(
            ws.clone(),
            NoteId::from_string(&id),
            NoteAddInput {
                content,
                heading,
                position,
            },
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn edit(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let old = args
        .get("old")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "old is required and cannot be empty".to_string())?
        .to_string();
    let new_text = args
        .get("new")
        .and_then(Value::as_str)
        .ok_or_else(|| "new is required".to_string())?
        .to_string();
    let r = api
        .edit_note(
            ws.clone(),
            NoteId::from_string(&id),
            NoteEditInput { old, new: new_text },
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn edit_lines(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let start =
        req_i64(args, "start").map_err(|_| "start must be a positive integer".to_string())?;
    let end = req_i64(args, "end").map_err(|_| "end must be a positive integer".to_string())?;
    if start < 1 {
        return Err("start must be a positive integer".to_string());
    }
    if end < 1 {
        return Err("end must be a positive integer".to_string());
    }
    if start > end {
        return Err("start cannot be greater than end".to_string());
    }
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "content is required".to_string())?
        .to_string();
    let r = api
        .edit_note_lines(
            ws.clone(),
            NoteId::from_string(&id),
            NoteEditLinesInput {
                start,
                end,
                content,
            },
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn update_metadata(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let title = opt_str(args, "title");
    let tags = opt_vec_str(args, "tags");
    if title.is_none() && tags.is_none() {
        return Err("At least one of title or tags must be provided".to_string());
    }
    let r = api
        .update_note_metadata(
            ws.clone(),
            NoteId::from_string(&id),
            title,
            tags,
            None,
            caller_agent_id.cloned(),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn delete(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let r = api
        .delete_note(ws.clone(), NoteId::from_string(&id), None)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}
