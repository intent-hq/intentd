//! `ws.note.*` bindings (WSAPI-3).
//!
//! Every JS entry point is a thin wrapper around `host({ method, args })`;
//! the Rust side calls the matching [`WorkspaceApi`] method and — for the
//! two shapes that diverge from the daemon's typed result (`note.read`,
//! `note.create`, `note.list`) — re-shapes the value to match the reference
//! `ws-note-api.ts` return objects agents already consume.

use std::fmt::Write as _;
use std::sync::Arc;

use intent_core::{
    asset_extension_from_mime, AgentId, NoteAddInput, NoteCreate, NoteEditInput,
    NoteEditLinesInput, NoteId, WorkspaceApi, WorkspaceId, SUPPORTED_ASSET_MIME_TYPES,
};
use serde_json::{json, Value};

use super::{map_err, opt_str, opt_vec_str, req_i64, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.note = {
        read: (id, options) => host({ method: 'note.read', args: { ...(options || {}), id } }),
        create: (title, content, tags) =>
            host({ method: 'note.create', args: { title, content, tags } }),
        list: (tag) => host({ method: 'note.list', args: { tag } }),
        listTasks: (id) => host({ method: 'note.listTasks', args: { id } }),
        readAsset: (asset) => host({ method: 'note.readAsset', args: { asset } }),
        saveAsset: (asset) => host({ method: 'note.saveAsset', args: asset || {} }),
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
";

/// Combined UTF-8 size of `oldContent` + `newContent` above which a
/// mutation result drops the full-note echo. The daemon has already
/// committed the write by the time the result is shaped; echoing two copies
/// of a multi-megabyte note into the script could exhaust its memory limit
/// and surface a committed write as an error (intent-hq/intent#7688).
pub(crate) const MAX_MUTATION_ECHO_BYTES: usize = 1024 * 1024;

/// Bound a committed mutation's result: past [`MAX_MUTATION_ECHO_BYTES`],
/// remove `oldContent` / `newContent` and mark `contentOmitted: true`; every
/// other field (ok, noteId, lengths, task conversion results) is kept.
fn bound_mutation_echo(mut result: Value) -> Value {
    if let Some(obj) = result.as_object_mut() {
        let echoed: usize = ["oldContent", "newContent"]
            .iter()
            .filter_map(|key| obj.get(*key).and_then(Value::as_str))
            .map(str::len)
            .sum();
        if echoed > MAX_MUTATION_ECHO_BYTES {
            obj.remove("oldContent");
            obj.remove("newContent");
            obj.insert("contentOmitted".to_string(), Value::Bool(true));
        }
    }
    result
}

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
        "saveAsset" => save_asset(api, ws, args).await,
        "setContent" => set_content(api, ws, caller_agent_id, args)
            .await
            .map(bound_mutation_echo),
        "add" => add(api, ws, caller_agent_id, args)
            .await
            .map(bound_mutation_echo),
        "edit" => edit(api, ws, caller_agent_id, args)
            .await
            .map(bound_mutation_echo),
        "editLines" => edit_lines(api, ws, caller_agent_id, args)
            .await
            .map(bound_mutation_echo),
        "updateMetadata" => update_metadata(api, ws, caller_agent_id, args).await,
        "delete" => delete(api, ws, args).await,
        other => Err(format!("host: unknown method `note.{other}`")),
    }
}

/// Requested line window of a bounded `ws.note.read` (intent-hq/intent#7688).
/// Lines are 1-based and split on `\n`, the numbering of the rendered
/// `content` and of `ws.note.editLines`.
struct LineWindow {
    start: usize,
    max: Option<usize>,
}

/// Parse an optional positive line option; `null` / absent is `None`.
/// Numeric strings are tolerated for parity with `editLines`' `req_i64`.
fn opt_positive_line(args: &Value, key: &str) -> Result<Option<usize>, String> {
    let invalid = || format!("{key} must be a positive integer");
    let parsed = match args.get(key) {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        Some(_) => None,
    };
    match parsed {
        Some(v) if v >= 1 => usize::try_from(v).map(Some).map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

/// `Some` iff the caller passed `startLine` or `maxLines`; with neither the
/// read keeps its unbounded result shape.
fn line_window(args: &Value) -> Result<Option<LineWindow>, String> {
    let start = opt_positive_line(args, "startLine")?;
    let max = opt_positive_line(args, "maxLines")?;
    if start.is_none() && max.is_none() {
        return Ok(None);
    }
    Ok(Some(LineWindow {
        start: start.unwrap_or(1),
        max,
    }))
}

/// Append `   N | text` display lines, newline-separated.
fn push_numbered<'a>(rendered: &mut String, lines: impl Iterator<Item = (usize, &'a str)>) {
    for (i, (number, line)) in lines.enumerate() {
        if i > 0 {
            rendered.push('\n');
        }
        let _ = write!(rendered, "{number:>4} | {line}");
    }
}

async fn read(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = req_str(args, "id").map_err(|_| "Note ID is required".to_string())?;
    let window = line_window(args)?;
    let note = api
        .get_note(ws.clone(), NoteId::from_string(&id))
        .await
        .map_err(map_err)?;
    let total_lines = if note.content.is_empty() {
        0
    } else {
        note.content.split('\n').count()
    };
    // `(startLine, endLine)` of a bounded read. Only the requested lines are
    // copied out of the note, so a huge note never reaches the script whole.
    let mut page: Option<(usize, usize)> = None;
    let mut rendered = String::new();
    let raw_content = if let Some(window) = &window {
        if window.start > total_lines.max(1) {
            return Err(format!(
                "startLine ({}) exceeds total lines in note ({total_lines})",
                window.start
            ));
        }
        let take = window.max.unwrap_or(usize::MAX);
        let numbered = || {
            note.content
                .split('\n')
                .enumerate()
                .skip(window.start - 1)
                .take(take)
                .map(|(idx, line)| (idx + 1, line))
        };
        let mut raw = String::new();
        let mut end = window.start - 1;
        if total_lines > 0 {
            for (number, line) in numbered() {
                if number > window.start {
                    raw.push('\n');
                }
                raw.push_str(line);
                end = number;
            }
            push_numbered(&mut rendered, numbered());
        }
        page = Some((window.start, end));
        raw
    } else {
        if total_lines > 0 {
            push_numbered(
                &mut rendered,
                note.content
                    .split('\n')
                    .enumerate()
                    .map(|(idx, line)| (idx + 1, line)),
            );
        }
        note.content.clone()
    };
    if let Some(task) = note.metadata.task.as_ref() {
        rendered.push_str("\n\n--- Task Metadata ---\n");
        let _ = write!(
            rendered,
            "Status: {}",
            serde_json::to_string(&task.status)
                .unwrap_or_default()
                .trim_matches('"')
        );
        if !task.acceptance_criteria.is_empty() {
            rendered.push_str("\nAcceptance Criteria:\n");
            for c in &task.acceptance_criteria {
                let _ = writeln!(rendered, "  - {c}");
            }
            rendered.pop();
        }
        if !task.assigned_agent_ids.is_empty() {
            let ids: Vec<String> = task
                .assigned_agent_ids
                .iter()
                .map(|a| a.as_str().to_string())
                .collect();
            let _ = write!(rendered, "\nAssigned Agents: {}", ids.join(", "));
        }
        if let Some(e) = &task.estimated_effort {
            let _ = write!(rendered, "\nEstimated Effort: {e}");
        }
        if let Some(r) = &task.blocked_reason {
            let _ = write!(rendered, "\nBlocked Reason: {r}");
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
    if let Some((start_line, end_line)) = page {
        let obj = out.as_object_mut().unwrap();
        obj.insert("startLine".to_string(), json!(start_line));
        obj.insert("endLine".to_string(), json!(end_line));
        obj.insert(
            "truncated".to_string(),
            Value::Bool(start_line > 1 || end_line < total_lines),
        );
        obj.insert("rev".to_string(), json!(note.rev));
        obj.insert("updatedAt".to_string(), json!(note.updated_at));
    }
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
    let markdown_link = super::markdown_link(&note.title, &link);
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

async fn save_asset(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let data = req_str(args, "data")?;
    let mime_type = req_str(args, "mimeType")?;
    if asset_extension_from_mime(&mime_type).is_none() {
        let supported = SUPPORTED_ASSET_MIME_TYPES
            .iter()
            .map(|(mime, _)| *mime)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!("mimeType must be one of: {supported}"));
    }
    let result = api
        .save_asset(ws.clone(), data, mime_type, opt_str(args, "originalName"))
        .await
        .map_err(map_err)?;
    serde_json::to_value(result).map_err(|e| e.to_string())
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
