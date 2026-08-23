//! `ws.comment.*` bindings (WSAPI-3).
//!
//! Every entry point forwards to a matching [`WorkspaceApi`] method; the
//! reference peer's JS-side thread bookkeeping already lives in the daemon
//! (`intent-services`), so the binding here is a thin argument peel.

use std::sync::Arc;

use intent_core::{NoteId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_str, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.comment = {
        add: (noteId, options) =>
            host({ method: 'comment.add', args: { noteId, ...(options || {}) } }),
        list: (noteId, options) =>
            host({ method: 'comment.list', args: { noteId, ...(options || {}) } }),
        getThread: (noteId, options) =>
            host({ method: 'comment.getThread', args: { noteId, ...(options || {}) } }),
        respond: (noteId, options) =>
            host({ method: 'comment.respond', args: { noteId, ...(options || {}) } }),
        delete: (noteId, commentId) =>
            host({ method: 'comment.delete', args: { noteId, commentId } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "add" => add(api, ws, args).await,
        "list" => list(api, ws, args).await,
        "getThread" => get_thread(api, ws, args).await,
        "respond" => respond(api, ws, args).await,
        "delete" => delete(api, ws, args).await,
        other => Err(format!("host: unknown method `comment.{other}`")),
    }
}

async fn add(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId, args: &Value) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let comment = req_str(args, "comment")
        .map_err(|_| "Comment text is required and must be non-empty".to_string())?;
    if comment.trim().is_empty() {
        return Err("Comment text is required and must be non-empty".to_string());
    }
    let search_context = req_str(args, "searchContext")
        .map_err(|_| "searchContext is required and must be non-empty".to_string())?;
    if search_context.trim().is_empty() {
        return Err("searchContext is required and must be non-empty".to_string());
    }
    let comment_target = req_str(args, "commentTarget")
        .map_err(|_| "commentTarget is required and must be non-empty".to_string())?;
    if comment_target.trim().is_empty() {
        return Err("commentTarget is required and must be non-empty".to_string());
    }
    let kind = opt_str(args, "type");
    let author = opt_str(args, "author");
    let author_type = opt_str(args, "authorType");
    let r = api
        .comment_add(
            ws.clone(),
            NoteId::from_string(&note_id),
            search_context,
            comment_target,
            comment,
            kind,
            author,
            author_type,
            opt_str(args, "idempotencyKey")
                .filter(|k| !k.trim().is_empty())
                .or_else(|| Some(uuid::Uuid::new_v4().to_string())),
            // MCP callers have no optimistic client-side anchors, so the
            // wire `commentId` param is not exposed here; the daemon mints.
            None,
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn list(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let since = opt_str(args, "since");
    let author_type = opt_str(args, "authorType");
    let status = opt_str(args, "status");
    let include_comments = opt_bool(args, "includeComments").unwrap_or(false);
    if let Some(t) = &author_type {
        if !matches!(t.as_str(), "user" | "agent") {
            return Err(format!(
                "Invalid 'authorType': {t}. Must be 'user' or 'agent'."
            ));
        }
    }
    if let Some(s) = &status {
        if !matches!(s.as_str(), "open" | "resolved" | "pending") {
            return Err(format!(
                "Invalid 'status': {s}. Must be 'open', 'resolved', or 'pending'."
            ));
        }
    }
    let r = api
        .comment_list(
            ws.clone(),
            NoteId::from_string(&note_id),
            since,
            author_type,
            status,
            include_comments,
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn get_thread(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let thread_id = opt_str(args, "threadId");
    let comment_id = opt_str(args, "commentId");
    if thread_id.is_none() && comment_id.is_none() {
        return Err("Either threadId or commentId must be provided".to_string());
    }
    let r = api
        .comment_get_thread(
            ws.clone(),
            NoteId::from_string(&note_id),
            thread_id,
            comment_id,
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn respond(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let thread_id = opt_str(args, "threadId");
    let comment_id = opt_str(args, "commentId");
    if thread_id.is_none() && comment_id.is_none() {
        return Err("Either threadId or commentId must be provided".to_string());
    }
    let comment = req_str(args, "comment")
        .map_err(|_| "Comment text is required and must be non-empty".to_string())?;
    if comment.trim().is_empty() {
        return Err("Comment text is required and must be non-empty".to_string());
    }
    let kind = opt_str(args, "type");
    let author = opt_str(args, "author");
    let author_type = opt_str(args, "authorType");
    let suggestion_original = opt_str(args, "suggestionOriginal");
    let suggestion_proposed = opt_str(args, "suggestionProposed");
    if kind.as_deref() == Some("suggestion")
        && (suggestion_original.is_none() || suggestion_proposed.is_none())
    {
        return Err(
            "For type='suggestion', both suggestionOriginal and suggestionProposed are required"
                .to_string(),
        );
    }
    let r = api
        .comment_respond(
            ws.clone(),
            NoteId::from_string(&note_id),
            thread_id,
            comment_id,
            comment,
            kind,
            author,
            author_type,
            suggestion_original,
            suggestion_proposed,
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
    let note_id = req_str(args, "noteId").map_err(|_| "Note ID is required".to_string())?;
    let comment_id =
        req_str(args, "commentId").map_err(|_| "Comment ID is required".to_string())?;
    let r = api
        .comment_delete(ws.clone(), NoteId::from_string(&note_id), comment_id)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}
